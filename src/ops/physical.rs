//! L8's PHYSICAL move: the target is a streaming standby of the source, made by whoever runs
//! the nodes (in SnoutData Cloud, a pod restored from the source's pgBackRest stanza in S3; in a
//! test, `pg_basebackup`). Lepis never copies a row. It waits for the standby to be close
//! behind, then in one pause stops the moving tables on the source, waits for the standby to
//! replay the source's last commit, promotes it, fences both sides and changes the catalog. The
//! new node starts as a whole copy of the source and gives back what it does not own afterwards.
//!
//! Chosen by `select` for a transfer with ONE target whose `lepis.node.labels->>'standby_of'`
//! names the transfer's source (`node.attach` writes it). Anything else is the logical path
//! (`transfer.rs`); a standby asked to take rows from a node it does not follow is refused,
//! because a read-only node cannot be a logical target.
//!
//! The phases, each recorded in the step's detail before the next starts:
//!
//! 1. **standby.** The target is in recovery, has the source's system identifier, and the
//!    source has no logical subscriptions (a physical copy would start them a second time).
//! 2. **synced.** The standby replays the source's current WAL position faster than 0.4 of
//!    `max_write_pause`.
//! 3. **promoting, then cut, then sealed** (the cutover, L10). On the source one transaction
//!    takes ACCESS EXCLUSIVE on the moving tables (in-flight transactions get `drain_timeout`,
//!    then are aborted); a committed transaction on a side session flushes the WAL, and the job
//!    waits until the standby has replayed past it, which proves every write the source
//!    committed is there. Until here the attempt can be given up and retried. Then the standby
//!    is PROMOTED (phase `promoting` is written first): from now on there is no going back to a
//!    standby. Its copied pg_cron jobs are deleted (jobs run on the home node), its fences are
//!    narrowed to what it will own, the source's fences are narrowed inside the lock, the
//!    catalog changes with phase `cut` in one home transaction, the routers ack, and the source
//!    commits. A crash between the promotion and the catalog write leaves a writable copy whose
//!    slice may be stale: the job then finishes with the LOGICAL path, whose prepare deletes the
//!    target's stale copy of the slice and copies it again (`fallback`).
//! 4. **verified.** Count and checksum of the slice on the source (a snapshot from inside the
//!    pause) and on the target (a snapshot taken after its promotion, before anything routes to
//!    it) must be equal.
//! 5. **settled.** On the target, every sharded table's fence is rewritten from the catalog (it
//!    came with the source's), and the copied `lepis` and `lepis_move` schemas are dropped.
//! 6. **cleaned** (only after a verified move): the source deletes the rows it gave away; the
//!    target deletes every row it does not own (the source's other ranges, other keyspaces,
//!    registered global tables), in batches, then VACUUM.
//!
//! Cancel before `promoting` changes nothing: the standby stays a standby and the source keeps
//! its rows. After it, the move finishes.

use std::time::{Duration, Instant};

use serde_json::{Value, json};

use super::Kind;
use super::spec::TransferSpec;
use super::steps::{
	StepCtx, alter_briefly, cancelled, connect_node, delete_ordered, fence_for, owns_expr, version,
};
use super::{Change, OpError, Pg, load_catalog, qualified, same, write_catalog};
use crate::catalog::{Catalog, NodeId, NodeState, RelationKind, RelationName, quote_ident};

/// The label `node.attach` writes: the id of the node this one is a physical standby of.
pub const STANDBY_LABEL: &str = "standby_of";

/// Which path a transfer takes. Decided once, written into the step's detail, and read back on
/// every resume so a job never changes strategy half way (except by `fallback`).
pub async fn select(cx: &mut StepCtx, h: &mut Pg, t: &TransferSpec) -> Result<bool, OpError> {
	match cx.get("strategy").and_then(Value::as_str) {
		Some("physical") => return Ok(true),
		Some(_) => return Ok(false),
		None => {}
	}
	// A logical move that started before this code existed carries a phase and no strategy.
	if cx.get("phase").is_some() {
		return Ok(false);
	}
	let mut standbys = Vec::new();
	for &n in &t.targets {
		if let Some(of) = standby_of(h, n).await? {
			standbys.push((n, of));
		}
	}
	let physical = match standbys.as_slice() {
		[] => false,
		[(_, of)] if t.targets.len() == 1 && *of == t.source => true,
		[(n, of), ..] => {
			return Err(OpError::refused(Kind::NodeUnsuitable, format!(
				"{n} is a physical standby of {of}: until a move from {of} promotes it, it can only take rows from {of}, and alone"
			)));
		}
	};
	cx.save(h, json!({"strategy": if physical { "physical" } else { "logical" }}))
		.await?;
	Ok(physical)
}

/// The node `n` follows as a standby, from its catalog labels.
pub async fn standby_of(h: &mut Pg, n: NodeId) -> Result<Option<NodeId>, OpError> {
	Ok(h
		.value(
			"select labels->>'standby_of' from lepis.node where id = $1",
			&[&n.0.to_string()],
		)
		.await?
		.and_then(|v| v.parse::<i32>().ok())
		.map(NodeId))
}

pub async fn run(cx: &mut StepCtx, h: &mut Pg, t: &TransferSpec) -> Result<(), OpError> {
	let target = *t
		.targets
		.first()
		.ok_or_else(|| OpError::new("a physical move has one target"))?;
	let mut held = None;
	loop {
		match cx.phase().to_string().as_str() {
			"new" => check(cx, h, t, target).await?,
			"standby" => catch_up(cx, h, t, target).await?,
			"synced" => held = cutover(cx, h, t, target).await?,
			"promoting" => resume_promoting(cx, h, t, target).await?,
			"cut" => seal_source(cx, h, t).await?,
			"sealed" => verify(cx, h, t, target, held.take()).await?,
			"verified" | "unverified" => settle(cx, h, target).await?,
			"settled" => cleanup(cx, h, t, target).await?,
			"cleaned" | "noop" => return Ok(()),
			"rolled_back" => return Err(cancelled()),
			"fallback" => return fallback(cx, h, t).await,
			"verify_failed" => {
				return Err(OpError::refused(
					Kind::VerifyFailed,
					"verify found the promoted copy differs from the source; nothing was cleaned up",
				));
			}
			other => return Err(OpError::new(format!("unknown phase {other}"))),
		}
	}
}

/// The standby is what `node.attach` said it is, and the source can be copied physically.
async fn check(cx: &mut StepCtx, h: &mut Pg, t: &TransferSpec, target: NodeId) -> Result<(), OpError> {
	let c = load_catalog(h).await?;
	let mut after = c.clone();
	t.change.apply(&mut after)?;
	if same(&c, &after) && t.change != Change::None {
		return cx.save(h, json!({"phase": "noop"})).await;
	}
	let mut src = connect_node(&cx.app, &c, t.source).await?;
	let mut dst = connect_node(&cx.app, &c, target).await?;
	let report = standby_report(&mut src, &mut dst).await?;
	let subscriptions = src
		.value("select count(*) from pg_subscription", &[])
		.await?
		.unwrap_or_default();
	if subscriptions != "0" {
		return Err(OpError::refused(Kind::NodeUnsuitable, format!(
			"{} has {subscriptions} logical subscription(s); a physical copy would start them a second time on {}, so move these rows with the logical path (a node that is not a standby)",
			src.label, dst.label
		)));
	}
	if c.keyspaces.values().any(|k| {
		k.ranges.iter().any(|r| r.node == target) || k.pins.values().any(|n| *n == target)
	}) {
		return Err(OpError::refused(Kind::NodeNotEmpty, format!(
			"{target} already owns rows; a physical standby must own nothing before its first move"
		)));
	}
	let mut patch = report;
	patch["phase"] = json!("standby");
	cx.save(h, patch).await
}

/// What makes `dst` a usable physical copy of `src`, or a refusal naming what is not.
pub async fn standby_report(src: &mut Pg, dst: &mut Pg) -> Result<Value, OpError> {
	let in_recovery = dst.value("select pg_is_in_recovery()", &[]).await?;
	if in_recovery.as_deref() != Some("t") {
		return Err(OpError::refused(Kind::NodeUnsuitable, format!(
			"{} is not in recovery: a physical move needs it to be a standby of {}",
			dst.label, src.label
		)));
	}
	let sysid = "select system_identifier::text from pg_control_system()";
	let a = src.value(sysid, &[]).await?.unwrap_or_default();
	let b = dst.value(sysid, &[]).await?.unwrap_or_default();
	if a.is_empty() || a != b {
		return Err(OpError::refused(Kind::NodeUnsuitable, format!(
			"{} (system {b}) is not a copy of {} (system {a})",
			dst.label, src.label
		)));
	}
	let sv = src.value("select current_setting('server_version_num')", &[]).await?;
	let dv = dst.value("select current_setting('server_version_num')", &[]).await?;
	// A standby is always the primary's major; a minor difference is allowed by Postgres.
	let receiver = dst
		.value("select coalesce((select status from pg_stat_wal_receiver), 'none')", &[])
		.await?
		.unwrap_or_default();
	let mut warnings = Vec::new();
	if receiver != "streaming" {
		warnings.push(format!(
			"{} is not streaming from {} (wal receiver: {receiver}); it catches up from the archive, which makes the cutover wait longer",
			dst.label, src.label
		));
	}
	Ok(json!({
		"system_identifier": a,
		"source_version": sv, "target_version": dv,
		"wal_receiver": receiver,
		"warnings": warnings,
	}))
}

/// How long the standby takes to replay up to the source's current position.
async fn replay_gap(src: &mut Pg, dst: &mut Pg, limit: Duration) -> Result<Option<Duration>, OpError> {
	let start = Instant::now();
	let lsn = src
		.value("select pg_current_wal_lsn()::text", &[])
		.await?
		.ok_or_else(|| OpError::new("no WAL position"))?;
	wait_replay(dst, &lsn, limit, start).await
}

/// Waits until `dst` has replayed `lsn`, up to `limit` counted from `start`.
async fn wait_replay(
	dst: &mut Pg,
	lsn: &str,
	limit: Duration,
	start: Instant,
) -> Result<Option<Duration>, OpError> {
	loop {
		let done = dst
			.query(
				"select coalesce(pg_last_wal_replay_lsn() >= $1::pg_lsn, false)",
				&[lsn],
			)
			.await?;
		if done.first().and_then(|r| r.first().cloned().flatten()).as_deref() == Some("t") {
			return Ok(Some(start.elapsed()));
		}
		if start.elapsed() > limit {
			return Ok(None);
		}
		tokio::time::sleep(Duration::from_millis(2)).await;
	}
}

async fn catch_up(cx: &mut StepCtx, h: &mut Pg, t: &TransferSpec, target: NodeId) -> Result<(), OpError> {
	let c = load_catalog(h).await?;
	let mut src = connect_node(&cx.app, &c, t.source).await?;
	let mut dst = connect_node(&cx.app, &c, target).await?;
	let started = Instant::now();
	let mut last_save = Instant::now() - Duration::from_secs(60);
	loop {
		if cx.cancelling(h).await? {
			cx.save(h, json!({"phase": "rolled_back"})).await?;
			return Err(cancelled());
		}
		let budget = cx.settings.max_write_pause.mul_f32(0.4);
		let gap = replay_gap(&mut src, &mut dst, Duration::from_secs(10)).await?;
		if let Some(gap) = gap
			&& gap <= budget
		{
			return cx
				.save(
					h,
					json!({"phase": "synced", "catch_up_seconds": started.elapsed().as_secs_f64(), "replay_gap_ms": gap.as_millis() as u64}),
				)
				.await;
		}
		if last_save.elapsed() > Duration::from_secs(2) {
			let lag = src
				.value("select pg_current_wal_lsn()::text", &[])
				.await?
				.unwrap_or_default();
			let replayed = dst
				.value("select coalesce(pg_last_wal_replay_lsn()::text, '0/0')", &[])
				.await?
				.unwrap_or_default();
			let bytes = src
				.query("select pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)::bigint", &[&lag, &replayed])
				.await?
				.first()
				.and_then(|r| r.first().cloned().flatten());
			cx.save(
				h,
				json!({"catch_up": {"lag_bytes": bytes, "seconds": started.elapsed().as_secs(), "last_gap_ms": gap.map(|g| g.as_millis() as u64)}}),
			)
			.await?;
			last_save = Instant::now();
		}
		tokio::time::sleep(Duration::from_millis(200)).await;
	}
}

/// The snapshots verify reads: the source's from inside the pause, the target's from just after
/// its promotion.
pub struct Held {
	source: Pg,
	target: Pg,
}

enum Attempt {
	Done(Box<Held>),
	Retry(String),
}

async fn cutover(
	cx: &mut StepCtx,
	h: &mut Pg,
	t: &TransferSpec,
	target: NodeId,
) -> Result<Option<Held>, OpError> {
	let mut attempt = cx.get("attempts").and_then(Value::as_u64).unwrap_or(0);
	loop {
		attempt += 1;
		if cx.cancelling(h).await? {
			cx.save(h, json!({"phase": "rolled_back"})).await?;
			return Err(cancelled());
		}
		match try_cutover(cx, h, t, target, attempt).await? {
			Attempt::Done(held) => return Ok(Some(*held)),
			Attempt::Retry(why) => {
				tracing::info!(job = cx.job, step = cx.n, attempt, "physical cutover postponed: {why}");
				cx.save(h, json!({"attempts": attempt, "postponed": why})).await?;
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
	target: NodeId,
	attempt: u64,
) -> Result<Attempt, OpError> {
	let s = cx.settings;
	let c = load_catalog(h).await?;
	let mut after = c.clone();
	t.change.apply(&mut after)?;
	// Every connection is open before the pause starts.
	let mut lock = connect_node(&cx.app, &c, t.source).await?;
	let mut side = connect_node(&cx.app, &c, t.source).await?;
	let mut snap_source = connect_node(&cx.app, &c, t.source).await?;
	let mut dst = connect_node(&cx.app, &c, target).await?;
	let mut snap_target = connect_node(&cx.app, &c, target).await?;
	let tv = version(&mut dst).await?;
	// What the target must carry before anything routes to it, computed before the pause.
	let mut target_fences = Vec::new();
	for rel in &t.tables {
		if let Some(f) = fence_for(&after, rel, target, tv)? {
			target_fences.push(f);
		}
	}
	let sv = version(&mut lock).await?;
	let mut source_fences = String::new();
	for rel in &t.tables {
		if let Some(f) = fence_for(&after, rel, t.source, sv)? {
			source_fences.push_str(&f);
			source_fences.push_str(";\n");
		}
	}
	let tables: Vec<String> = t.tables.iter().map(qualified).collect();
	let lock_pid = lock.pid();
	let canceller = lock.canceller();
	side.simple("set synchronous_commit = on").await?;
	lock.simple("set statement_timeout = 0; set lock_timeout = 0; begin")
		.await?;

	let t0 = Instant::now();
	let budget = s.max_write_pause.mul_f32(0.6);
	let mut aborted = 0u64;
	let lock_sql = format!("lock table {} in access exclusive mode", tables.join(", "));
	let granted = {
		let fut = lock.simple(&lock_sql);
		tokio::pin!(fut);
		match tokio::time::timeout(budget, &mut fut).await {
			Ok(r) => r.map(|_| true),
			Err(_) => {
				aborted = super::transfer::abort_blockers(&mut side, lock_pid, s.drain_timeout).await?;
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
	// A committed transaction flushes the WAL up to its own commit record, which is after every
	// commit that came before the lock.
	let flushed = side
		.simple("begin; select txid_current(); commit; select pg_current_wal_flush_lsn()::text")
		.await?;
	let lsn = flushed
		.last()
		.and_then(|r| r.first().cloned().flatten())
		.ok_or_else(|| OpError::new("no flush position"))?;
	let replayed = wait_replay(&mut dst, &lsn, budget, t0).await?;
	if replayed.is_none() {
		let _ = lock.simple("rollback").await;
		return Ok(Attempt::Retry(format!(
			"the standby did not replay {lsn} within {} ms",
			budget.as_millis()
		)));
	}
	let caught_up = t0.elapsed();
	snap_source
		.simple("begin isolation level repeatable read; select 1")
		.await?;

	// The point of no return. Written down first, so a crash from here on is finished by
	// `resume_promoting` and never retried as if the target were still a standby.
	h.simple(&format!("begin; {} commit", cx.save_sql(&json!({"phase": "promoting", "promoting_lsn": lsn}))))
		.await?;
	if let Some(d) = cx.detail.as_object_mut() {
		d.insert("phase".into(), json!("promoting"));
	}
	let promoted = dst.value("select pg_promote(true, 60)", &[]).await?;
	if promoted.as_deref() != Some("t") {
		// Still a standby: nothing changed anywhere, so this attempt is given up like any other.
		let _ = lock.simple("rollback").await;
		h.simple(&format!("begin; {} commit", cx.save_sql(&json!({"phase": "synced"}))))
			.await?;
		if let Some(d) = cx.detail.as_object_mut() {
			d.insert("phase".into(), json!("synced"));
		}
		return Ok(Attempt::Retry("the standby did not finish promoting within 60 s".into()));
	}
	let promote_done = t0.elapsed();
	// Jobs run on the home node (Phase 7); a copy of them here would run twice.
	dst.simple(
		"do $$ begin if to_regclass('cron.job') is not null then delete from cron.job; end if; end $$",
	)
	.await?;
	for f in &target_fences {
		alter_briefly(&mut dst, f).await?;
	}
	snap_target
		.simple("begin isolation level repeatable read; select 1")
		.await?;
	// The source gives the rows up inside its lock.
	if !source_fences.is_empty() {
		lock.simple(&source_fences).await?;
	}
	let sealed = t0.elapsed();

	// The catalog, the target active and no longer a standby, and phase `cut`, in one home
	// transaction. From here on the job only ever finishes.
	let before = load_catalog(h).await?;
	let mut next = before.clone();
	Change::Many(vec![
		t.change.clone(),
		Change::NodeState {
			node: target,
			state: NodeState::Active,
		},
	])
	.apply(&mut next)?;
	let extra = format!(
		"update lepis.node set labels = labels - '{STANDBY_LABEL}' where id = {};\n{}",
		target.0,
		cx.save_sql(&json!({"phase": "cut"}))
	);
	let epoch = write_catalog(h, &before, &next, &extra).await?;
	if let Some(d) = cx.detail.as_object_mut() {
		d.insert("phase".into(), json!("cut"));
	}
	let catalogued = t0.elapsed();
	let ack_limit = s
		.ack_timeout
		.min(s.max_write_pause.saturating_sub(t0.elapsed()));
	let (acked, unacked) = super::transfer::wait_acks(h, epoch, ack_limit).await?;
	let acks = t0.elapsed();
	lock.simple("commit").await?;
	let pause = t0.elapsed();
	if !unacked.is_empty() {
		h.query(
			"update lepis.router set fenced_epoch = $1 where id = any (string_to_array($2, ','))",
			&[&epoch.to_string(), &unacked.join(",")],
		)
		.await?;
	}
	lock.close().await;
	side.close().await;
	dst.close().await;
	let ms = |d: Duration| d.as_secs_f64() * 1000.0;
	cx.save(
		h,
		json!({
			"phase": "sealed",
			"epoch": epoch,
			"attempts": attempt,
			"cutover": {
				"strategy": "physical",
				"pause_ms": ms(pause), "drain_ms": ms(drained), "catch_up_ms": ms(caught_up - drained),
				"promote_ms": ms(promote_done - caught_up), "fence_ms": ms(sealed - promote_done),
				"catalog_ms": ms(catalogued - sealed), "ack_ms": ms(acks - catalogued),
				"commit_ms": ms(pause - acks), "over_ceiling": pause > s.max_write_pause,
				"aborted_transactions": aborted, "routers_acked": acked, "routers_fenced_out": unacked,
			},
		}),
	)
	.await?;
	Ok(Attempt::Done(Box::new(Held {
		source: snap_source,
		target: snap_target,
	})))
}

/// A run that stopped after writing `promoting`. If the target is still a standby, the promotion
/// never happened and the cutover is tried again. If it is promoted, the source's lock died with
/// the run, so writes may have reached the slice on the source after the promotion: the copy is
/// finished by the logical path instead.
async fn resume_promoting(
	cx: &mut StepCtx,
	h: &mut Pg,
	t: &TransferSpec,
	target: NodeId,
) -> Result<(), OpError> {
	let c = load_catalog(h).await?;
	// The catalog write and phase `cut` are one transaction, so a catalog that already says the
	// target owns the slice means the cut happened and only the source's seal is left.
	let mut after = c.clone();
	t.change.apply(&mut after)?;
	if same(&c, &after) && t.change != Change::None {
		return cx.save(h, json!({"phase": "cut"})).await;
	}
	let mut dst = connect_node(&cx.app, &c, target).await?;
	if dst.value("select pg_is_in_recovery()", &[]).await?.as_deref() == Some("t") {
		return cx.save(h, json!({"phase": "synced"})).await;
	}
	cx.save(h, json!({"phase": "fallback"})).await
}

/// The target was promoted but the catalog never changed: it is an ordinary writable node now,
/// holding a copy of the source that may be behind for the moving slice. The logical path's
/// prepare deletes the target's stale copy of the slice and copies it again, so the move is
/// finished by that path, from a clean detail.
async fn fallback(cx: &mut StepCtx, h: &mut Pg, t: &TransferSpec) -> Result<(), OpError> {
	let target = t.targets[0];
	h.simple(&format!(
		"begin; update lepis.node set labels = labels - '{STANDBY_LABEL}' where id = {}; select lepis.bump(); commit",
		target.0
	))
	.await?;
	// The target owned nothing before this move (`check`), so every row it holds of the moving
	// tables is the source's copy as of the promotion. The logical prepare deletes stale rows
	// only of tables the cluster already manages, and refuses a table it does not that holds
	// rows; a table being distributed is the second kind, so the copy goes here.
	let c = load_catalog(h).await?;
	let mut dst = connect_node(&cx.app, &c, target).await?;
	let mut stale = Vec::new();
	for rel in &t.tables {
		if super::schema::exists(&mut dst, rel).await? {
			stale.push((rel.clone(), "true".to_string()));
		}
	}
	delete_ordered(&mut dst, stale).await?;
	dst.close().await;
	// Nothing of the physical attempt is needed by the logical one; a fresh detail is what
	// `transfer::run` expects at phase `new`.
	let restart = json!({
		"strategy": "logical",
		"phase": "new",
		"fallback": "the target was promoted, but the cutover did not reach the catalog; finished with the logical path",
	});
	h.query(
		"update lepis.job_step set detail = $1::jsonb where job_id = $2 and n = $3",
		&[&restart.to_string(), &cx.job.to_string(), &cx.n.to_string()],
	)
	.await?;
	cx.detail = restart;
	super::transfer::run(cx, h, t).await
}

/// A cutover that stopped after the catalog changed: the source's fences again, under a lock,
/// from the catalog as it now is. Verify is skipped: there is no snapshot from inside the pause.
async fn seal_source(cx: &mut StepCtx, h: &mut Pg, t: &TransferSpec) -> Result<(), OpError> {
	let c = load_catalog(h).await?;
	let mut lock = connect_node(&cx.app, &c, t.source).await?;
	let v = version(&mut lock).await?;
	let tables: Vec<String> = t.tables.iter().map(qualified).collect();
	let mut seal = format!(
		"set lock_timeout = '{}ms'; begin; lock table {} in access exclusive mode;\n",
		cx.settings.drain_timeout.as_millis(),
		tables.join(", ")
	);
	for rel in &t.tables {
		if let Some(f) = fence_for(&c, rel, t.source, v)? {
			seal.push_str(&f);
			seal.push_str(";\n");
		}
	}
	seal.push_str("commit");
	if let Err(e) = lock.simple(&seal).await {
		let _ = lock.simple("rollback").await;
		return Err(e);
	}
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
	target: NodeId,
	held: Option<Held>,
) -> Result<(), OpError> {
	let Some(mut held) = held else {
		return cx.save(h, json!({"phase": "unverified"})).await;
	};
	let c = load_catalog(h).await?;
	// The catalog already says `after`, and the target owned nothing before (`check`), so the
	// slice is exactly what the target owns now: on the source's snapshot those are the rows it
	// gave, and on the target's (a whole copy, before cleanup) the rows it received.
	let v = version(&mut held.target).await?;
	let mut results = Vec::new();
	let mut equal = true;
	for rel in &t.tables {
		let slice = owns_expr(&c, rel, target, v, &|col| quote_ident(col))?;
		let sum = format!(
			"select count(*), coalesce(sum(hashtextextended(lepis_row::text, 0)::numeric), 0) \
			from {} lepis_row where {slice}",
			qualified(rel)
		);
		let a = held.source.simple(&sum).await;
		let b = held.target.simple(&sum).await;
		let (a, b) = match (a, b) {
			(Ok(a), Ok(b)) => (a, b),
			(Err(e), _) | (_, Err(e)) => {
				let _ = held.source.simple("rollback").await;
				let _ = held.target.simple("rollback").await;
				return cx
					.save(h, json!({"phase": "unverified", "verify": format!("skipped: {e}")}))
					.await;
			}
		};
		let same = a == b;
		equal &= same;
		let cell = |r: &[Vec<Option<String>>], i: usize| {
			r.last()
				.and_then(|x| x.get(i).cloned().flatten())
				.unwrap_or_default()
		};
		results.push(json!({
			"target": target.0, "table": rel.to_string(), "equal": same,
			"source_rows": cell(&a, 0), "target_rows": cell(&b, 0),
			"source_checksum": cell(&a, 1), "target_checksum": cell(&b, 1),
		}));
	}
	let _ = held.source.simple("rollback").await;
	let _ = held.target.simple("rollback").await;
	cx.save(
		h,
		json!({"phase": if equal { "verified" } else { "verify_failed" }, "verify": results}),
	)
	.await
}

/// The promoted node stops looking like the source: every fence from the catalog, and the
/// source's copies of Lepis's own schemas gone.
async fn settle(cx: &mut StepCtx, h: &mut Pg, target: NodeId) -> Result<(), OpError> {
	let c = load_catalog(h).await?;
	let mut dst = connect_node(&cx.app, &c, target).await?;
	let v = version(&mut dst).await?;
	let mut fenced = 0;
	for rel in sharded(&c) {
		if super::schema::exists(&mut dst, &rel).await?
			&& let Some(f) = fence_for(&c, &rel, target, v)?
		{
			alter_briefly(&mut dst, &f).await?;
			fenced += 1;
		}
	}
	if c.home().map(|n| n.id) != Some(target) {
		dst.simple("drop schema if exists lepis_move cascade; drop schema if exists lepis cascade")
			.await?;
	}
	cx.save(h, json!({"phase": "settled", "target_fences": fenced})).await
}

fn sharded(c: &Catalog) -> Vec<RelationName> {
	let mut out: Vec<RelationName> = c
		.relations
		.iter()
		.filter(|(_, k)| matches!(k, RelationKind::Sharded { .. }))
		.map(|(r, _)| r.clone())
		.collect();
	out.sort();
	out
}

/// After a verified move: the source deletes what it gave away; the target deletes everything
/// it holds and does not own.
async fn cleanup(cx: &mut StepCtx, h: &mut Pg, t: &TransferSpec, target: NodeId) -> Result<(), OpError> {
	if !cx.get("verify").is_some_and(Value::is_array) {
		return cx
			.save(h, json!({"phase": "cleaned", "cleanup": "skipped: the move was not verified; run cleanup once it is"}))
			.await;
	}
	let c = load_catalog(h).await?;
	let mut deleted = serde_json::Map::new();

	let mut src = connect_node(&cx.app, &c, t.source).await?;
	let sv = version(&mut src).await?;
	let mut items = Vec::new();
	for rel in &t.tables {
		if matches!(c.relations.get(rel), Some(RelationKind::Sharded { .. })) {
			let owns = owns_expr(&c, rel, t.source, sv, &|col| quote_ident(col))?;
			items.push((rel.clone(), format!("not ({owns})")));
		}
	}
	for (rel, n) in delete_ordered(&mut src, items).await? {
		if n > 0 {
			src.simple(&format!("vacuum {}", qualified(&rel))).await?;
		}
		deleted.insert(format!("source:{rel}"), json!(n));
	}

	let mut dst = connect_node(&cx.app, &c, target).await?;
	let tv = version(&mut dst).await?;
	let mut items = Vec::new();
	let mut names: Vec<(&RelationName, &RelationKind)> = c.relations.iter().collect();
	names.sort_by(|a, b| a.0.cmp(b.0));
	for (rel, kind) in names {
		if matches!(kind, RelationKind::Reference) || !super::schema::exists(&mut dst, rel).await? {
			continue;
		}
		// Sharded: what it does not own. Global (registered): all of it, off the home node.
		let owns = owns_expr(&c, rel, target, tv, &|col| quote_ident(col))?;
		if owns != "true" {
			items.push((rel.clone(), format!("not ({owns})")));
		}
	}
	for (rel, n) in delete_ordered(&mut dst, items).await? {
		if n > 0 {
			dst.simple(&format!("vacuum {}", qualified(&rel))).await?;
		}
		deleted.insert(format!("target:{rel}"), json!(n));
	}
	cx.save(h, json!({"phase": "cleaned", "cleanup": deleted})).await
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_label_is_the_one_node_attach_writes() {
		assert_eq!(STANDBY_LABEL, "standby_of");
	}
}
