//! L8's logical move and L10's cutover: rows go from one source node to one or more targets
//! while the application keeps writing, and ownership changes in one short pause.
//!
//! The phases, each recorded in the step's detail before the next starts:
//!
//! 1. **prepared.** On each target: the tables (copied from the source's definition when
//!    missing), any stale rows of the slice deleted (a node that does not own rows may hold old
//!    copies), and the fence widened to its after-move ownership so replicated rows are accepted.
//!    On the source, a publication per target whose row filter is the slice: rows the source owns
//!    now AND the target will own after (every node is Postgres 17 or later, L1, so row filters
//!    are always there). Then a subscription per target. Each name made is
//!    `lepis_j<job>_s<step>_n<target>`, written down, and the only thing teardown drops.
//! 2. **synced.** Every table has finished its initial copy, and a marker row written on the
//!    source comes back on every target faster than a fraction of the pause the user allows.
//! 3. **cut, then sealed** (the cutover). On the source, one transaction takes ACCESS EXCLUSIVE on
//!    the tables. New writes queue behind it, which is the pause; transactions already in flight
//!    have `drain_timeout`, then are aborted (L10). A marker row is written and the job waits until
//!    every target has applied it, which proves every earlier write is there. Inside the same
//!    transaction the source's fence is narrowed and the tables leave the publications, so nothing
//!    the source does afterwards can reach a target. The catalog changes and the epoch bumps on the
//!    home node in one transaction that also records phase `cut`; the job waits for the routers
//!    to ack the epoch; then the source transaction commits and the queue drains (writes for the
//!    moved rows fail the source's fence, `23514`, and are retried on the new owner). A router that
//!    did not ack is named in `lepis.router` and its writes are refused by the fence (L7).
//! 4. **verified.** Count and checksum of the slice on the source and on each target, both read
//!    from snapshots taken inside the pause, must be equal. A move resumed after a crash in the
//!    cutover has no such snapshots: it says so and leaves the source's rows for a later cleanup.
//! 5. **torn_down**: subscriptions, slots, publications and marker rows, by exact name.
//! 6. **cleaned**: the source deletes the rows it gave away, in batches, then VACUUM.
//!
//! Cancel before `cut` rolls everything back, target rows included; after `cut` the move finishes.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use super::Kind;
use super::spec::TransferSpec;
use super::steps::{
	StepCtx, alter_briefly, cancelled, connect_node, delete_ordered, owns_expr, version,
};
use super::{OpError, Pg, load_catalog, qualified, same, schema, write_catalog};
use crate::catalog::{Catalog, NodeId, RelationKind, RelationName, quote_ident, quote_literal};

const MARK_SQL: &str = "create schema if not exists lepis_move; \
	create table if not exists lepis_move.mark (job_id bigint not null, target int not null, \
	token text not null, at timestamptz not null default now(), primary key (job_id, target, token))";

/// How long the routers' heartbeat may be old and still count as a live router.
const ROUTER_FRESH_SECS: u32 = 15;

/// What a table needs for one target, computed once at prepare and kept in the detail.
#[derive(Clone, Debug, Default)]
struct Plan {
	/// The slice, as the source evaluates it (its version), for the publication and verify.
	slice_source: String,
	/// The slice as the target evaluates it, for verify and rollback.
	slice_target: String,
	fence_target_after: Option<String>,
	fence_target_before: Option<String>,
	/// Whether the table on the target belongs to the cluster already (its stale rows may be
	/// deleted); otherwise a target table holding rows is refused.
	managed: bool,
}

fn names(cx: &StepCtx, target: NodeId) -> String {
	format!("lepis_j{}_s{}_n{}", cx.job, cx.n, target.0)
}

fn and(a: &str, b: &str) -> String {
	match (a, b) {
		("true", x) | (x, "true") => x.to_string(),
		_ => format!("({a}) and ({b})"),
	}
}

pub async fn run(cx: &mut StepCtx, h: &mut Pg, t: &TransferSpec) -> Result<(), OpError> {
	let mut held = None;
	loop {
		match cx.phase().to_string().as_str() {
			"new" => prepare(cx, h, t).await?,
			"prepared" => sync(cx, h, t).await?,
			"synced" => held = cutover(cx, h, t).await?,
			"cut" => seal(cx, h, t).await?,
			"sealed" => verify(cx, h, t, held.take()).await?,
			"verified" | "unverified" => teardown(cx, h, t, "torn_down").await?,
			"torn_down" => cleanup_source(cx, h, t).await?,
			"cleaned" | "noop" => return Ok(()),
			"rolling_back" => rollback(cx, h, t).await?,
			"rolled_back" => return Err(cancelled()),
			"verify_failed" => {
				return Err(OpError::refused(
					Kind::VerifyFailed,
					"verify found the copy differs from the source; nothing was cleaned up",
				));
			}
			other => return Err(OpError::new(format!("unknown phase {other}"))),
		}
	}
}

/// The per-target, per-table expressions, from the catalog before and after the change.
fn plans(
	before: &Catalog,
	after: &Catalog,
	t: &TransferSpec,
	source_version: u32,
	target_versions: &BTreeMap<NodeId, u32>,
) -> Result<BTreeMap<NodeId, BTreeMap<String, Plan>>, OpError> {
	let mut out = BTreeMap::new();
	for (&target, &tv) in target_versions {
		let mut m = BTreeMap::new();
		for rel in &t.tables {
			let plain = |c: &str| quote_ident(c);
			let s_before = owns_expr(before, rel, t.source, source_version, &plain)?;
			let p = Plan {
				slice_source: and(
					&s_before,
					&owns_expr(after, rel, target, source_version, &plain)?,
				),
				slice_target: and(
					&owns_expr(before, rel, t.source, tv, &plain)?,
					&owns_expr(after, rel, target, tv, &plain)?,
				),
				fence_target_after: super::steps::fence_for(after, rel, target, tv)?,
				fence_target_before: super::steps::fence_for(before, rel, target, tv)?,
				managed: matches!(
					before.relations.get(rel),
					Some(RelationKind::Sharded { .. } | RelationKind::Reference)
				),
			};
			m.insert(rel.to_string(), p);
		}
		out.insert(target, m);
	}
	Ok(out)
}

fn plan_json(p: &BTreeMap<NodeId, BTreeMap<String, Plan>>) -> Value {
	let mut o = serde_json::Map::new();
	for (n, tables) in p {
		let mut t = serde_json::Map::new();
		for (name, x) in tables {
			t.insert(
				name.clone(),
				json!({
					"slice_source": x.slice_source, "slice_target": x.slice_target,
					"fence_target_after": x.fence_target_after,
					"fence_target_before": x.fence_target_before, "managed": x.managed,
				}),
			);
		}
		o.insert(n.0.to_string(), Value::Object(t));
	}
	Value::Object(o)
}

fn plan_from(cx: &StepCtx, target: NodeId, table: &RelationName) -> Plan {
	let x = cx
		.get("plans")
		.and_then(|p| p.get(target.0.to_string()))
		.and_then(|p| p.get(table.to_string()))
		.cloned()
		.unwrap_or(Value::Null);
	let s = |k: &str| {
		x.get(k)
			.and_then(Value::as_str)
			.unwrap_or("false")
			.to_string()
	};
	let o = |k: &str| x.get(k).and_then(Value::as_str).map(str::to_string);
	Plan {
		slice_source: s("slice_source"),
		slice_target: s("slice_target"),
		fence_target_after: o("fence_target_after"),
		fence_target_before: o("fence_target_before"),
		managed: x.get("managed").and_then(Value::as_bool).unwrap_or(false),
	}
}

async fn prepare(cx: &mut StepCtx, h: &mut Pg, t: &TransferSpec) -> Result<(), OpError> {
	let started = Instant::now();
	let before = load_catalog(h).await?;
	let mut after = before.clone();
	t.change.apply(&mut after)?;
	if t.targets.is_empty() || (same(&before, &after) && t.change != super::Change::None) {
		return cx.save(h, json!({"phase": "noop"})).await;
	}
	let mut src = connect_node(&cx.app, &before, t.source).await?;
	let sv = version(&mut src).await?;
	let mut tvs = BTreeMap::new();
	let mut targets = BTreeMap::new();
	for &n in &t.targets {
		let mut pg = connect_node(&cx.app, &before, n).await?;
		tvs.insert(n, version(&mut pg).await?);
		targets.insert(n, pg);
	}
	let plans = plans(&before, &after, t, sv, &tvs)?;
	cx.save(
		h,
		json!({
			"source_version": sv,
			"target_versions": tvs.iter().map(|(k, v)| (k.0.to_string(), json!(v))).collect::<serde_json::Map<_, _>>(),
			"plans": plan_json(&plans),
		}),
	)
	.await?;

	for rel in &t.tables {
		schema::check_movable(&mut src, rel, None, &before).await?;
	}
	src.simple(MARK_SQL).await?;
	let ready = cx.get("tables_ready").and_then(Value::as_bool) == Some(true);
	for (&n, pg) in targets.iter_mut() {
		pg.simple(MARK_SQL).await?;
		let mut stale = Vec::new();
		for rel in &t.tables {
			let p = &plans[&n][&rel.to_string()];
			let made = schema::ensure_table(&mut src, pg, rel).await?;
			if !ready && !made {
				if p.managed {
					// Rows of the slice on a node that does not own them are old copies.
					stale.push((rel.clone(), p.slice_target.clone()));
				} else if schema::has_rows(pg, rel).await? {
					return Err(OpError::refused(
						Kind::TableHasRows,
						format!(
							"{} already holds rows in {rel}, a table the cluster does not manage; Lepis will not overwrite it",
							pg.label
						),
					));
				}
			}
			if let Some(f) = &p.fence_target_after {
				alter_briefly(pg, f).await?;
			}
		}
		delete_ordered(pg, stale).await?;
	}
	cx.save(
		h,
		json!({"tables_ready": true, "tables_ms": started.elapsed().as_millis() as u64}),
	)
	.await?;

	// The source as the targets reach it (a node label when it differs from the router's view).
	let peer = h
		.value(
			"select coalesce(labels->>'peer_host', host) from lepis.node where id = $1",
			&[&t.source.0.to_string()],
		)
		.await?
		.unwrap_or_default();
	let source_node = before
		.nodes
		.get(&t.source)
		.ok_or_else(|| OpError::new("no source node"))?;
	for (&n, pg) in targets.iter_mut() {
		let name = names(cx, n);
		let exists =
			src.value(
				"select count(*) from pg_publication where pubname = $1",
				&[&name],
			)
			.await?
			.as_deref() != Some("0");
		if !exists {
			let mut items = Vec::new();
			for rel in &t.tables {
				let p = &plans[&n][&rel.to_string()];
				if p.slice_source != "true" {
					items.push(format!("{} where ({})", qualified(rel), p.slice_source));
				} else {
					items.push(qualified(rel));
				}
			}
			items.push(format!(
				"lepis_move.mark where (job_id = {} and target = {})",
				cx.job, n.0
			));
			src.simple(&format!(
				"create publication {} for table {}",
				quote_ident(&name),
				items.join(", ")
			))
			.await?;
		}
		let has_sub =
			pg.value(
				"select count(*) from pg_subscription where subname = $1",
				&[&name],
			)
			.await?
			.as_deref() != Some("0");
		if !has_sub {
			let info = super::pg::conninfo(&cx.app, &peer, source_node);
			pg.simple(&format!(
				"create subscription {n} connection {} publication {n} \
				with (slot_name = {s}, create_slot = true, copy_data = true, enabled = true)",
				quote_literal(&info),
				n = quote_ident(&name),
				s = quote_literal(&name),
			))
			.await?;
		}
	}
	cx.save(
		h,
		json!({"phase": "prepared", "prepare_ms": started.elapsed().as_millis() as u64}),
	)
	.await
}

/// Waits for every table's initial copy, then for a marker row to round-trip fast enough.
async fn sync(cx: &mut StepCtx, h: &mut Pg, t: &TransferSpec) -> Result<(), OpError> {
	let c = load_catalog(h).await?;
	let mut src = connect_node(&cx.app, &c, t.source).await?;
	let mut targets = Vec::new();
	for &n in &t.targets {
		targets.push((n, connect_node(&cx.app, &c, n).await?));
	}
	let started = Instant::now();
	let mut last_save = Instant::now() - Duration::from_secs(60);
	loop {
		if cx.cancelling(h).await? {
			return rollback(cx, h, t).await;
		}
		let mut pending = Vec::new();
		let mut lag = serde_json::Map::new();
		for (n, pg) in targets.iter_mut() {
			let name = names(cx, *n);
			let r = pg
				.query(
					"select count(*) filter (where r.srsubstate <> 'r'), count(*) \
					from pg_subscription s left join pg_subscription_rel r on r.srsubid = s.oid \
					where s.subname = $1",
					&[&name],
				)
				.await?;
			let row = r.first().cloned().unwrap_or_default();
			let not_ready = row.first().cloned().flatten().unwrap_or_default();
			let total = row.get(1).cloned().flatten().unwrap_or_default();
			if not_ready != "0" || total == "0" {
				pending.push(n.0);
			}
			let bytes = src
				.value(
					"select pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint \
					from pg_replication_slots where slot_name = $1",
					&[&name],
				)
				.await?;
			lag.insert(n.0.to_string(), json!(bytes));
		}
		if last_save.elapsed() > Duration::from_secs(2) {
			cx.save(
				h,
				json!({"sync": {"pending": pending, "lag_bytes": lag, "seconds": started.elapsed().as_secs()}}),
			)
			.await?;
			last_save = Instant::now();
		}
		if pending.is_empty() {
			let rtt = mark_round_trip(cx, &mut src, &mut targets, Duration::from_secs(10)).await?;
			let budget = cx.settings.max_write_pause.mul_f32(0.4);
			if let Some(rtt) = rtt
				&& rtt <= budget
			{
				return cx
					.save(
						h,
						json!({"phase": "synced", "copy_seconds": started.elapsed().as_secs_f64(), "mark_rtt_ms": rtt.as_millis() as u64}),
					)
					.await;
			}
		}
		tokio::time::sleep(Duration::from_millis(200)).await;
	}
}

/// Writes a marker row on the source (in its own transaction) and waits until every target has
/// applied it. Logical replication applies commits in commit order, so a target holding the
/// marker holds every write the source committed before it.
async fn mark_round_trip(
	cx: &StepCtx,
	src: &mut Pg,
	targets: &mut [(NodeId, Pg)],
	limit: Duration,
) -> Result<Option<Duration>, OpError> {
	if limit.is_zero() {
		return Ok(None);
	}
	let token = super::token();
	let start = Instant::now();
	let values: Vec<String> = targets
		.iter()
		.map(|(n, _)| format!("({}, {}, '{token}')", cx.job, n.0))
		.collect();
	src.simple(&format!(
		"insert into lepis_move.mark (job_id, target, token) values {}",
		values.join(", ")
	))
	.await?;
	for (n, pg) in targets.iter_mut() {
		loop {
			let seen = pg
				.simple(&format!(
					"select exists (select from lepis_move.mark where job_id = {} and target = {} and token = '{token}')",
					cx.job, n.0
				))
				.await?;
			if seen
				.first()
				.and_then(|r| r.first().cloned().flatten())
				.as_deref() == Some("t")
			{
				break;
			}
			if start.elapsed() > limit {
				return Ok(None);
			}
			tokio::time::sleep(Duration::from_millis(2)).await;
		}
	}
	Ok(Some(start.elapsed()))
}

/// The snapshots verify reads, taken inside the pause.
pub struct Held {
	source: Pg,
	targets: Vec<(NodeId, Pg)>,
}

enum Attempt {
	Done(Held),
	Retry(String),
}

async fn cutover(cx: &mut StepCtx, h: &mut Pg, t: &TransferSpec) -> Result<Option<Held>, OpError> {
	let mut attempt = cx.get("attempts").and_then(Value::as_u64).unwrap_or(0);
	loop {
		attempt += 1;
		if cx.cancelling(h).await? {
			return rollback(cx, h, t).await.map(|()| None);
		}
		match try_cutover(cx, h, t, attempt).await? {
			Attempt::Done(held) => return Ok(Some(held)),
			Attempt::Retry(why) => {
				tracing::info!(
					job = cx.job,
					step = cx.n,
					attempt,
					"cutover postponed: {why}"
				);
				cx.save(h, json!({"attempts": attempt, "postponed": why}))
					.await?;
				let wait = Duration::from_millis(250 * (1u64 << attempt.min(7)));
				tokio::time::sleep(wait.min(Duration::from_secs(30))).await;
			}
		}
	}
}

async fn try_cutover(
	cx: &mut StepCtx,
	h: &mut Pg,
	t: &TransferSpec,
	attempt: u64,
) -> Result<Attempt, OpError> {
	let s = cx.settings;
	let c = load_catalog(h).await?;
	// Every connection is open before the pause starts.
	let mut lock = connect_node(&cx.app, &c, t.source).await?;
	let mut side = connect_node(&cx.app, &c, t.source).await?;
	let mut snap_source = connect_node(&cx.app, &c, t.source).await?;
	let mut watch = Vec::new();
	let mut snaps = Vec::new();
	for &n in &t.targets {
		watch.push((n, connect_node(&cx.app, &c, n).await?));
		snaps.push((n, connect_node(&cx.app, &c, n).await?));
	}
	let tables: Vec<String> = t.tables.iter().map(qualified).collect();
	let lock_pid = lock.pid();
	let canceller = lock.canceller();
	lock.simple("set statement_timeout = 0; set lock_timeout = 0; begin")
		.await?;

	let t0 = Instant::now();
	let mut aborted = 0u64;
	let lock_sql = format!("lock table {} in access exclusive mode", tables.join(", "));
	// The ceiling (L10) is checked at every point the attempt can still be given up: until the
	// catalog changes, letting go of the lock undoes everything. The lock itself is waited for
	// only as long as the pause allows; a transaction in flight that holds it past
	// `drain_timeout` (counted from its own start) is aborted, and the cutover tries again.
	let budget = s.max_write_pause.mul_f32(0.6);
	let granted = {
		let fut = lock.simple(&lock_sql);
		tokio::pin!(fut);
		match tokio::time::timeout(budget, &mut fut).await {
			Ok(r) => r.map(|_| true),
			Err(_) => {
				aborted = abort_blockers(&mut side, lock_pid, s.drain_timeout).await?;
				canceller.cancel().await;
				let _ = fut.await;
				Ok(false)
			}
		}
	};
	match granted {
		Ok(true) => {}
		Ok(false) => {
			let _ = lock.simple("rollback").await;
			return Ok(Attempt::Retry(format!(
				"transactions in flight held the tables past {} ms ({aborted} older than drain_timeout aborted)",
				budget.as_millis()
			)));
		}
		Err(e) => {
			let _ = lock.simple("rollback").await;
			return Ok(Attempt::Retry(format!("the lock failed: {e}")));
		}
	}
	let drained = t0.elapsed();
	if drained > budget {
		let _ = lock.simple("rollback").await;
		return Ok(Attempt::Retry(format!(
			"the drain took {} ms of the {} ms allowed",
			drained.as_millis(),
			s.max_write_pause.as_millis()
		)));
	}
	// Everything the source committed is now behind the marker.
	let left = budget.saturating_sub(t0.elapsed());
	let rtt = mark_round_trip(cx, &mut side, &mut watch, left).await?;
	if rtt.is_none() {
		let _ = lock.simple("rollback").await;
		return Ok(Attempt::Retry(format!(
			"the targets were not caught up within {} ms",
			budget.as_millis()
		)));
	}
	let caught_up = t0.elapsed();
	snap_source
		.simple("begin isolation level repeatable read; select 1")
		.await?;
	for (_, pg) in snaps.iter_mut() {
		pg.simple("begin isolation level repeatable read; select 1")
			.await?;
	}

	// The source gives the rows up: its fence, and the publications stop carrying them.
	let mut seal = String::new();
	let fences = source_fences(&c, t, &mut lock).await?;
	for f in &fences {
		seal.push_str(f);
		seal.push_str(";\n");
	}
	for &n in &t.targets {
		seal.push_str(&format!(
			"alter publication {} drop table {};\n",
			quote_ident(&names(cx, n)),
			tables.join(", ")
		));
	}
	if let Err(e) = lock.simple(&seal).await {
		let _ = lock.simple("rollback").await;
		return Ok(Attempt::Retry(format!("sealing the source failed: {e}")));
	}
	let sealed = t0.elapsed();

	if sealed > s.max_write_pause.mul_f32(0.8) {
		let _ = lock.simple("rollback").await;
		return Ok(Attempt::Retry(format!(
			"{} ms had gone before the catalog write; the ceiling is {} ms",
			sealed.as_millis(),
			s.max_write_pause.as_millis()
		)));
	}
	// The catalog, and phase `cut`, in one transaction on the home node.
	let before = load_catalog(h).await?;
	let mut after = before.clone();
	t.change.apply(&mut after)?;
	let mark_cut = cx.save_sql(&json!({"phase": "cut"}));
	let epoch = if same(&before, &after) {
		h.simple(&format!("begin; {mark_cut} commit")).await?;
		before.epoch
	} else {
		match write_catalog(h, &before, &after, &mark_cut).await {
			Ok(e) => e,
			Err(e) => {
				let _ = lock.simple("rollback").await;
				return Ok(Attempt::Retry(format!("the catalog write failed: {e}")));
			}
		}
	};
	if let Some(d) = cx.detail.as_object_mut() {
		d.insert("phase".into(), json!("cut"));
	}
	let catalogued = t0.elapsed();
	// The ceiling outranks the acks: a router that has not acked when it is reached is fenced out
	// (L7), never waited for past it.
	let ack_limit = s
		.ack_timeout
		.min(s.max_write_pause.saturating_sub(t0.elapsed()));
	let (acked, unacked) = wait_acks(h, epoch, ack_limit).await?;
	let acks = t0.elapsed();
	// The cut is durable; a failure from here on is finished by `seal` on the next run.
	lock.simple("commit").await?;
	let pause = t0.elapsed();
	// Named after the pause, so the write is not inside it.
	if !unacked.is_empty() {
		h.query(
			"update lepis.router set fenced_epoch = $1 where id = any (string_to_array($2, ','))",
			&[&epoch.to_string(), &unacked.join(",")],
		)
		.await?;
	}
	lock.close().await;
	side.close().await;
	for (_, pg) in watch {
		pg.close().await;
	}
	let ms = |d: Duration| d.as_secs_f64() * 1000.0;
	cx.save(
		h,
		json!({
			"phase": "sealed",
			"epoch": epoch,
			"attempts": attempt,
			"cutover": {
				"pause_ms": ms(pause), "drain_ms": ms(drained), "catch_up_ms": ms(caught_up - drained),
				"seal_ms": ms(sealed - caught_up), "catalog_ms": ms(catalogued - sealed),
				"ack_ms": ms(acks - catalogued), "commit_ms": ms(pause - acks),
				"aborted_transactions": aborted, "routers_acked": acked, "routers_fenced_out": unacked,
			},
		}),
	)
	.await?;
	Ok(Attempt::Done(Held {
		source: snap_source,
		targets: snaps,
	}))
}

/// The source's fences after the change, for the tables that are sharded after it.
async fn source_fences(
	c: &Catalog,
	t: &TransferSpec,
	src: &mut Pg,
) -> Result<Vec<String>, OpError> {
	let mut after = c.clone();
	t.change.apply(&mut after)?;
	let v = version(src).await?;
	let mut out = Vec::new();
	for rel in &t.tables {
		if let Some(f) = super::steps::fence_for(&after, rel, t.source, v)? {
			out.push(f);
		}
	}
	Ok(out)
}

/// Cancels (or, idle in a transaction, terminates) what holds locks the cutover is waiting for
/// and has been running longer than `drain` (L10's drain_timeout).
pub(super) async fn abort_blockers(
	side: &mut Pg,
	pid: i32,
	drain: Duration,
) -> Result<u64, OpError> {
	let rows = side
		.query(
			"select case when state like 'idle in transaction%' then pg_terminate_backend(pid) \
			else pg_cancel_backend(pid) end from pg_stat_activity where pid = any (pg_blocking_pids($1::int)) \
			and now() - xact_start > $2::int * interval '1 millisecond'",
			&[&pid.to_string(), &drain.as_millis().to_string()],
		)
		.await?;
	Ok(rows.len() as u64)
}

/// Waits until every live router has loaded `epoch`, up to `limit`. Returns the routers that
/// acked and those that did not.
pub(super) async fn wait_acks(
	h: &mut Pg,
	epoch: i64,
	limit: Duration,
) -> Result<(Vec<String>, Vec<String>), OpError> {
	let start = Instant::now();
	loop {
		let rows = h
			.query(
				&format!(
					"select id, epoch >= $1 from lepis.router \
					where seen_at > now() - interval '{ROUTER_FRESH_SECS} seconds' order by id"
				),
				&[&epoch.to_string()],
			)
			.await?;
		let (mut acked, mut waiting) = (Vec::new(), Vec::new());
		for r in rows {
			let id = r.first().cloned().flatten().unwrap_or_default();
			if r.get(1).cloned().flatten().as_deref() == Some("t") {
				acked.push(id);
			} else {
				waiting.push(id);
			}
		}
		if waiting.is_empty() {
			return Ok((acked, waiting));
		}
		if start.elapsed() >= limit {
			return Ok((acked, waiting));
		}
		tokio::time::sleep(Duration::from_millis(2)).await;
	}
}

/// A cutover that crashed after the catalog changed: the source's side again, without a catalog
/// write. Its verify is skipped (no snapshot from inside the pause), so cleanup is left for later.
async fn seal(cx: &mut StepCtx, h: &mut Pg, t: &TransferSpec) -> Result<(), OpError> {
	let c = load_catalog(h).await?;
	let mut lock = connect_node(&cx.app, &c, t.source).await?;
	let mut side = connect_node(&cx.app, &c, t.source).await?;
	let mut watch = Vec::new();
	for &n in &t.targets {
		watch.push((n, connect_node(&cx.app, &c, n).await?));
	}
	let tables: Vec<String> = t.tables.iter().map(qualified).collect();
	// The catalog already says `after`; the fences come from it as it is.
	let v = version(&mut lock).await?;
	let mut seal = format!(
		"set lock_timeout = '{}ms'; begin; lock table {} in access exclusive mode;\n",
		cx.settings.drain_timeout.as_millis(),
		tables.join(", ")
	);
	for rel in &t.tables {
		if let Some(f) = super::steps::fence_for(&c, rel, t.source, v)? {
			seal.push_str(&f);
			seal.push_str(";\n");
		}
	}
	lock.simple(&seal).await?;
	let _ = mark_round_trip(cx, &mut side, &mut watch, Duration::from_secs(30)).await?;
	let mut drop = String::new();
	for &n in &t.targets {
		let name = names(cx, n);
		for rel in &t.tables {
			let member = lock
				.value(
					"select count(*) from pg_publication_tables where pubname = $1 and schemaname = $2 and tablename = $3",
					&[&name, &rel.schema, &rel.table],
				)
				.await?;
			if member.as_deref() != Some("0") {
				drop.push_str(&format!(
					"alter publication {} drop table {};\n",
					quote_ident(&name),
					qualified(rel)
				));
			}
		}
	}
	if !drop.is_empty() {
		lock.simple(&drop).await?;
	}
	lock.simple("commit").await?;
	cx.save(
		h,
		json!({"phase": "sealed", "verify": "skipped: the cutover was finished after a restart, with no snapshot from inside the pause"}),
	)
	.await
}

async fn verify(
	cx: &mut StepCtx,
	h: &mut Pg,
	t: &TransferSpec,
	held: Option<Held>,
) -> Result<(), OpError> {
	let Some(mut held) = held else {
		return cx.save(h, json!({"phase": "unverified"})).await;
	};
	let mut results = Vec::new();
	let mut equal = true;
	for (n, pg) in held.targets.iter_mut() {
		for rel in &t.tables {
			let p = plan_from(cx, *n, rel);
			let sum = |slice: &str| {
				format!(
					"select count(*), coalesce(sum(hashtextextended(lepis_row::text, 0)::numeric), 0) \
					from {} lepis_row where {slice}",
					qualified(rel)
				)
			};
			let a = held.source.simple(&sum(&p.slice_source)).await?;
			let b = pg.simple(&sum(&p.slice_target)).await?;
			let same = a == b;
			equal &= same;
			let cell = |r: &[Vec<Option<String>>], i: usize| {
				r.first()
					.and_then(|x| x.get(i).cloned().flatten())
					.unwrap_or_default()
			};
			results.push(json!({
				"target": n.0, "table": rel.to_string(), "equal": same,
				"source_rows": cell(&a, 0), "target_rows": cell(&b, 0),
				"source_checksum": cell(&a, 1), "target_checksum": cell(&b, 1),
			}));
		}
		let _ = pg.simple("rollback").await;
	}
	let _ = held.source.simple("rollback").await;
	cx.save(
		h,
		json!({"phase": if equal { "verified" } else { "verify_failed" }, "verify": results}),
	)
	.await
}

/// Drops what this move made, by exact name: subscriptions (which drop their slots), any slot
/// left behind, publications, and this job's marker rows.
async fn teardown(
	cx: &mut StepCtx,
	h: &mut Pg,
	t: &TransferSpec,
	next: &str,
) -> Result<(), OpError> {
	let c = load_catalog(h).await?;
	let mut src = connect_node(&cx.app, &c, t.source).await?;
	for &n in &t.targets {
		let name = names(cx, n);
		let mut pg = connect_node(&cx.app, &c, n).await?;
		let has_sub =
			pg.value(
				"select count(*) from pg_subscription where subname = $1",
				&[&name],
			)
			.await?
			.as_deref() != Some("0");
		if has_sub
			&& pg
				.simple(&format!(
					"drop subscription if exists {}",
					quote_ident(&name)
				))
				.await
				.is_err()
		{
			// The source could not be reached to drop the slot: detach it, drop it below.
			pg.simple(&format!(
				"alter subscription {n} disable; alter subscription {n} set (slot_name = none); drop subscription {n}",
				n = quote_ident(&name)
			))
			.await?;
		}
		pg.simple(&format!(
			"delete from lepis_move.mark where job_id = {}",
			cx.job
		))
		.await
		.ok();
		for _ in 0..100 {
			let left = src
				.query(
					"select active from pg_replication_slots where slot_name = $1",
					&[&name],
				)
				.await?;
			match left
				.first()
				.and_then(|r| r.first().cloned().flatten())
				.as_deref()
			{
				None => break,
				Some("f") => {
					src.query("select pg_drop_replication_slot($1)", &[&name])
						.await?;
					break;
				}
				_ => tokio::time::sleep(Duration::from_millis(100)).await,
			}
		}
		src.simple(&format!(
			"drop publication if exists {}",
			quote_ident(&name)
		))
		.await?;
	}
	src.simple(&format!(
		"delete from lepis_move.mark where job_id = {}",
		cx.job
	))
	.await
	.ok();
	cx.save(h, json!({"phase": next})).await
}

/// The source deletes what it gave away. Only after a verified move.
async fn cleanup_source(cx: &mut StepCtx, h: &mut Pg, t: &TransferSpec) -> Result<(), OpError> {
	if !cx.get("verify").is_some_and(Value::is_array) {
		return cx
			.save(h, json!({"phase": "cleaned", "cleanup": "skipped: the move was not verified; run cleanup once it is"}))
			.await;
	}
	let c = load_catalog(h).await?;
	let mut src = connect_node(&cx.app, &c, t.source).await?;
	let v = version(&mut src).await?;
	let mut deleted = serde_json::Map::new();
	let mut items = Vec::new();
	for rel in &t.tables {
		// Only a table the source no longer holds in full: a copy to new nodes leaves it whole.
		if matches!(c.relations.get(rel), Some(RelationKind::Sharded { .. })) {
			let owns = owns_expr(&c, rel, t.source, v, &|col| quote_ident(col))?;
			items.push((rel.clone(), format!("not ({owns})")));
		}
	}
	for (rel, n) in delete_ordered(&mut src, items).await? {
		if n > 0 {
			src.simple(&format!("vacuum {}", qualified(&rel))).await?;
		}
		deleted.insert(rel.to_string(), json!(n));
	}
	cx.save(h, json!({"phase": "cleaned", "cleanup": deleted}))
		.await
}

/// Cancel before the cutover: everything made is dropped, the targets' copies deleted and their
/// fences put back.
async fn rollback(cx: &mut StepCtx, h: &mut Pg, t: &TransferSpec) -> Result<(), OpError> {
	teardown(cx, h, t, "rolling_back").await?;
	let c = load_catalog(h).await?;
	for &n in &t.targets {
		let mut pg = connect_node(&cx.app, &c, n).await?;
		let mut items = Vec::new();
		for rel in &t.tables {
			if !schema::exists(&mut pg, rel).await? {
				continue;
			}
			let p = plan_from(cx, n, rel);
			if let Some(f) = &p.fence_target_before {
				alter_briefly(&mut pg, f).await?;
			}
			items.push((rel.clone(), p.slice_target));
		}
		delete_ordered(&mut pg, items).await?;
	}
	cx.save(h, json!({"phase": "rolled_back"})).await?;
	Err(cancelled())
}
