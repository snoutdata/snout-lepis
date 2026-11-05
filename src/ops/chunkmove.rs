//! Moving whole chunks: a transfer whose tables are chunked.
//!
//! Two shapes, one cutover (L10), the same phases as `transfer.rs`:
//!
//! - **Move.** The source holds the chunk tables already. Each target gets the shell (copied from
//!   the source's definition) and an empty copy of each chunk it will own; the source publishes
//!   the chunk tables themselves, no row filter, and each target subscribes. At cutover the
//!   source's copies are RETIRED inside the pause (out of the shell, renamed out of reach, fenced
//!   `CHECK (false)`, `chunk::retire_sql`), the targets' join their shells, the catalog moves.
//!   Cleanup is `DROP TABLE`: no DELETE, no VACUUM.
//! - **Convert** (a table distributed into chunks). The source, the home node, holds the table as
//!   one heap. It makes every chunk table beside it and fills each from the heap; a capture
//!   trigger logs the key of every row written meanwhile, and the log is replayed into the chunks
//!   in rounds (pg_repack's way, proven in `scripts/chunk-proof.sh`). The chunks other nodes will
//!   own go to them as in a move. At cutover, writes stop on the heap, the last of the log is
//!   replayed, the heap is emptied (`TRUNCATE ONLY`) and becomes the shell, the chunks it keeps
//!   join it, and the others are retired.
//!
//! Every statement that waited on the cutover's lock is cancelled before it commits (the router
//! tells its client 40001, since the catalog moved): a write planned against the old shape must
//! not run against the new one, where a DELETE through the shell would find nothing, silently.
//!
//! Verify compares count and checksum of every moved chunk on both sides from snapshots taken
//! inside the pause (for a conversion, against the heap's slice). A crash inside the cutover is
//! finished without them, as `transfer.rs` does, and says so.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use super::Kind;
use super::spec::TransferSpec;
use super::steps::{StepCtx, cancelled, connect_node, version};
use super::transfer::{
	COPY_STALL, MARK_SQL, abort_blockers, mark_round_trip, names, node_labels, wait_acks,
};
use super::{OpError, Pg, load_catalog, qualified, same, schema, write_catalog};
use crate::catalog::{
	Catalog, Chunk, NodeId, RelationKind, RelationName, quote_ident, quote_literal,
};
use crate::chunk;

/// Keys replayed from the capture log per round.
const REPLAY_BATCH: usize = 5_000;
/// A conversion cuts over once the capture log is this short (the rest is replayed in the pause).
const REPLAY_SETTLED: u64 = 2_000;

/// How long the preparation's DDL waits for a lock before the step is tried again.
const PREPARE_LOCK: &str = "1s";

/// Whether a transfer is this file's: any of its tables is chunked once the change is made.
pub fn applies(after: &Catalog, t: &TransferSpec) -> bool {
	t.tables.iter().any(|r| after.chunked.contains(r))
}

/// What a chunk transfer does, decided at prepare and kept in the step's detail.
#[derive(Clone, Debug, Default)]
struct Work {
	keyspace: String,
	tables: Vec<RelationName>,
	key_columns: Vec<String>,
	/// Each table's second key column, in a keyspace with a second key (Phase 5).
	second_columns: Vec<Option<String>>,
	/// The source holds the tables as heaps, to be chunked.
	convert: bool,
	/// Target → the chunk numbers it will own.
	moves: BTreeMap<NodeId, Vec<u32>>,
	/// Chunks the source keeps (a conversion's, which join its shell at cutover), or a split's
	/// two halves.
	keep: Vec<u32>,
	chunks: Vec<Chunk>,
	/// A chunk split in place on the source: the chunk the halves (`keep`) are made from.
	split_from: Option<u32>,
}

impl Work {
	/// Built on the source beside what it copies from, under a capture log (convert, split).
	fn local(&self) -> bool {
		self.convert || self.split_from.is_some()
	}

	/// What the source copies `rel`'s rows from: the heap, or the chunk being split.
	fn heap(&self, rel: &RelationName) -> RelationName {
		match self.split_from {
			Some(n) => chunk::chunk_relation(rel, n),
			None => rel.clone(),
		}
	}

	/// The chunk tables the source builds and fills.
	fn building(&self) -> Vec<u32> {
		if self.convert {
			self.chunks.iter().map(|c| c.number).collect()
		} else {
			self.keep.clone()
		}
	}

	fn moved(&self) -> Vec<u32> {
		let mut v: Vec<u32> = self.moves.values().flatten().copied().collect();
		v.sort_unstable();
		v
	}

	fn chunk(&self, n: u32) -> Chunk {
		*self
			.chunks
			.iter()
			.find(|c| c.number == n)
			.expect("a chunk the work names")
	}

	/// Table `i`'s key as SQL (both columns in a keyspace with a second key).
	fn key_sql(&self, i: usize, prefix: Option<&str>) -> crate::catalog::KeySql {
		let q = |col: &str| match prefix {
			Some(p) => format!("{p}.{}", quote_ident(col)),
			None => quote_ident(col),
		};
		crate::catalog::KeySql {
			key: q(&self.key_columns[i]),
			second: self.second_columns.get(i).cloned().flatten().map(|s| q(&s)),
		}
	}

	fn to_json(&self) -> Value {
		json!({
			"keyspace": self.keyspace,
			"tables": self.tables.iter().map(|t| t.to_string()).collect::<Vec<_>>(),
			"key_columns": self.key_columns,
			"second_columns": self.second_columns,
			"convert": self.convert,
			"moves": self.moves.iter().map(|(n, v)| (n.0.to_string(), json!(v))).collect::<serde_json::Map<_, _>>(),
			"keep": self.keep,
			"split_from": self.split_from,
			"chunks": self.chunks.iter().map(|c| json!([c.number, c.lo.to_string(), c.hi.to_string()])).collect::<Vec<_>>(),
		})
	}

	fn from_json(v: &Value) -> Result<Work, OpError> {
		let strs = |k: &str| -> Vec<String> {
			v.get(k)
				.and_then(Value::as_array)
				.map(|a| {
					a.iter()
						.filter_map(|x| x.as_str().map(str::to_string))
						.collect()
				})
				.unwrap_or_default()
		};
		let nums = |x: &Value| -> Vec<u32> {
			x.as_array()
				.map(|a| {
					a.iter()
						.filter_map(|n| n.as_u64().and_then(|n| u32::try_from(n).ok()))
						.collect()
				})
				.unwrap_or_default()
		};
		let tables = strs("tables")
			.iter()
			.map(|t| super::relation_name(t))
			.collect::<Result<Vec<_>, _>>()?;
		let mut moves = BTreeMap::new();
		if let Some(m) = v.get("moves").and_then(Value::as_object) {
			for (k, x) in m {
				let id: i32 = k.parse().map_err(|_| OpError::new("bad node in detail"))?;
				moves.insert(NodeId(id), nums(x));
			}
		}
		let chunks = v
			.get("chunks")
			.and_then(Value::as_array)
			.map(|a| {
				a.iter()
					.filter_map(|c| {
						Some(Chunk {
							number: u32::try_from(c.get(0)?.as_u64()?).ok()?,
							lo: super::i64_of(c.get(1)?)?,
							hi: super::i64_of(c.get(2)?)?,
						})
					})
					.collect()
			})
			.unwrap_or_default();
		Ok(Work {
			keyspace: v
				.get("keyspace")
				.and_then(Value::as_str)
				.unwrap_or_default()
				.to_string(),
			tables,
			key_columns: strs("key_columns"),
			second_columns: v
				.get("second_columns")
				.and_then(Value::as_array)
				.map(|a| a.iter().map(|x| x.as_str().map(str::to_string)).collect())
				.unwrap_or_default(),
			convert: v.get("convert").and_then(Value::as_bool).unwrap_or(false),
			moves,
			keep: v.get("keep").map(nums).unwrap_or_default(),
			chunks,
			split_from: v
				.get("split_from")
				.and_then(Value::as_u64)
				.and_then(|n| u32::try_from(n).ok()),
		})
	}
}

fn plan_work(before: &Catalog, after: &Catalog, t: &TransferSpec) -> Result<Work, OpError> {
	let mut keyspace = None;
	let mut key_columns = Vec::new();
	let mut second_columns = Vec::new();
	let mut tables = Vec::new();
	let mut convert = None;
	for rel in &t.tables {
		let Some(RelationKind::Sharded {
			keyspace: k,
			key_column,
		}) = after.relations.get(rel)
		else {
			return Err(OpError::refused(
				Kind::NotGlobal,
				format!("{rel} is not a sharded table once the change is made"),
			));
		};
		if !after.chunked.contains(rel) {
			return Err(OpError::refused(
				Kind::NotGlobal,
				format!(
					"{rel} is not chunked and moves with tables that are; chunk it first (table rechunk)"
				),
			));
		}
		if keyspace.get_or_insert(k.clone()) != k {
			return Err(OpError::new(
				"a chunk transfer moves the tables of one keyspace",
			));
		}
		let c = !before.chunked.contains(rel);
		if *convert.get_or_insert(c) != c {
			return Err(OpError::refused(
				Kind::NotGlobal,
				"a transfer cannot chunk some tables and move the chunks of others at once",
			));
		}
		key_columns.push(key_column.clone());
		second_columns.push(after.second_keys.get(rel).cloned());
		tables.push(rel.clone());
	}
	let keyspace = keyspace.ok_or_else(|| OpError::new("a chunk transfer with no table"))?;
	let convert = convert.unwrap_or(false);
	let ks_after = &after.keyspaces[&keyspace];
	let chunks = ks_after.chunks.clone();
	// A chunk split in place: one chunk of `before` replaced by two of `after`, nothing moving.
	let chunks_before = &before.keyspaces[&keyspace].chunks;
	if !convert && *chunks_before != chunks {
		let old: Vec<u32> = chunks_before
			.iter()
			.filter(|c| !chunks.contains(c))
			.map(|c| c.number)
			.collect();
		let halves: Vec<u32> = chunks
			.iter()
			.filter(|c| !chunks_before.contains(c))
			.map(|c| c.number)
			.collect();
		let [from] = old.as_slice() else {
			return Err(OpError::new("a chunk split replaces exactly one chunk"));
		};
		if halves.len() != 2 {
			return Err(OpError::new("a chunk split makes exactly two chunks"));
		}
		return Ok(Work {
			keyspace,
			tables,
			key_columns,
			second_columns,
			convert: false,
			moves: BTreeMap::new(),
			keep: halves,
			chunks,
			split_from: Some(*from),
		});
	}
	let mut moves: BTreeMap<NodeId, Vec<u32>> = BTreeMap::new();
	let mut keep = Vec::new();
	for c in &chunks {
		let to = ks_after.owner_of_hash(c.lo);
		let from = if convert {
			t.source
		} else {
			before.keyspaces[&keyspace].owner_of_hash(c.lo)
		};
		if from != t.source {
			continue;
		}
		if to == t.source {
			if convert {
				keep.push(c.number);
			}
		} else if t.targets.contains(&to) {
			moves.entry(to).or_default().push(c.number);
		} else {
			return Err(OpError::new(format!(
				"chunk {} goes to {to}, which is not one of the transfer's targets",
				c.number
			)));
		}
	}
	Ok(Work {
		keyspace,
		tables,
		key_columns,
		second_columns,
		convert,
		moves,
		keep,
		chunks,
		split_from: None,
	})
}

fn work(cx: &StepCtx) -> Result<Work, OpError> {
	Work::from_json(
		cx.get("chunk")
			.ok_or_else(|| OpError::new("the step lost its chunk plan"))?,
	)
}

/// The capture log, its trigger function per table, and the trigger's name.
fn cap_log(cx: &StepCtx) -> String {
	format!("lepis_move.cap_j{}_s{}", cx.job, cx.n)
}
fn cap_fn(cx: &StepCtx, i: usize) -> String {
	format!("lepis_move.cap_j{}_s{}_{i}", cx.job, cx.n)
}
fn cap_trigger(cx: &StepCtx) -> String {
	format!("lepis_cap_j{}_s{}", cx.job, cx.n)
}

pub async fn run(cx: &mut StepCtx, h: &mut Pg, t: &TransferSpec) -> Result<(), OpError> {
	let mut held = None;
	loop {
		match cx.phase().to_string().as_str() {
			"new" => prepare(cx, h, t).await?,
			"prepared" => sync(cx, h, t).await?,
			"synced" => held = cutover(cx, h, t).await?,
			"cut" => seal_after_crash(cx, h, t).await?,
			"sealed" => verify(cx, h, held.take()).await?,
			"verified" | "unverified" => teardown(cx, h, t, "torn_down").await?,
			"torn_down" => cleanup(cx, h, t).await?,
			"cleaned" | "noop" => return Ok(()),
			"rolling_back" => rollback(cx, h, t).await?,
			"rolled_back" => return Err(cancelled()),
			"verify_failed" => {
				return Err(OpError::refused(
					Kind::VerifyFailed,
					"verify found a chunk's copy differs from the source; nothing was cleaned up",
				));
			}
			other => return Err(OpError::new(format!("unknown phase {other}"))),
		}
	}
}

/// The columns that take values (not generated), in order.
async fn value_columns(pg: &mut Pg, rel: &RelationName) -> Result<Vec<String>, OpError> {
	Ok(pg
		.query(
			"select attname::text from pg_attribute where attrelid = $1::regclass and attnum > 0 \
			and not attisdropped and attgenerated = '' order by attnum",
			&[&qualified(rel)],
		)
		.await?
		.into_iter()
		.filter_map(|r| r.first().cloned().flatten())
		.collect())
}

async fn key_type(pg: &mut Pg, rel: &RelationName, key: &str) -> Result<String, OpError> {
	pg.value(
		"select format_type(atttypid, atttypmod) from pg_attribute \
		where attrelid = $1::regclass and attname = $2",
		&[&qualified(rel), key],
	)
	.await?
	.ok_or_else(|| OpError::new(format!("{rel} has no column {key}")))
}

async fn exists(pg: &mut Pg, schema_name: &str, table: &str) -> Result<bool, OpError> {
	Ok(pg
		.value(
			"select count(*) from pg_class k join pg_namespace n on n.oid = k.relnamespace \
			where n.nspname = $1 and k.relname = $2",
			&[schema_name, table],
		)
		.await?
		.as_deref()
		!= Some("0"))
}

/// Runs each table's `chunk::sync_sql` until all pass: a foreign key between chunks needs the
/// referenced table's chunks synced first, and the order is not known here.
async fn sync_all(pg: &mut Pg, tables: &[RelationName]) -> Result<(), OpError> {
	let mut left: Vec<&RelationName> = tables.iter().collect();
	let mut last = None;
	for _ in 0..=tables.len() {
		let mut failed = Vec::new();
		for t in left {
			if let Err(e) = pg.simple(&chunk::sync_sql(t)).await {
				last = Some(e);
				failed.push(t);
			}
		}
		if failed.is_empty() {
			return Ok(());
		}
		left = failed;
	}
	Err(last.unwrap_or_else(|| OpError::new("the chunks could not be synced")))
}

/// The shell's triggers and foreign keys on the target's shell, which `schema::ddl` does not copy
/// (the chunks' copies are `sync_sql`'s, from the shell).
async fn copy_extras(src: &mut Pg, dst: &mut Pg, rel: &RelationName) -> Result<(), OpError> {
	let t = qualified(rel);
	for r in src
		.query(
			"select tgname, pg_get_triggerdef(oid) from pg_trigger where tgrelid = $1::regclass and not tgisinternal \
			and tgname not like 'lepis\\_cap\\_%'",
			&[&t],
		)
		.await?
	{
		let name = r.first().cloned().flatten().unwrap_or_default();
		let def = r.get(1).cloned().flatten().unwrap_or_default();
		let have = dst
			.value(
				"select count(*) from pg_trigger where tgrelid = $1::regclass and tgname = $2",
				&[&t, &name],
			)
			.await?;
		if have.as_deref() == Some("0") {
			dst.simple(&def).await?;
		}
	}
	for r in src
		.query(
			"select conname, pg_get_constraintdef(oid) from pg_constraint where conrelid = $1::regclass and contype = 'f'",
			&[&t],
		)
		.await?
	{
		let name = r.first().cloned().flatten().unwrap_or_default();
		let def = r.get(1).cloned().flatten().unwrap_or_default();
		let have = dst
			.value(
				"select count(*) from pg_constraint where conrelid = $1::regclass and conname = $2",
				&[&t, &name],
			)
			.await?;
		if have.as_deref() == Some("0") {
			dst.simple(&format!(
				"alter table {t} add constraint {} {def}",
				quote_ident(&name)
			))
			.await?;
		}
	}
	Ok(())
}

/// The chunk joins the shell unless it already has.
async fn attach(pg: &mut Pg, shell: &RelationName, n: u32) -> Result<(), OpError> {
	let c = qualified(&chunk::chunk_relation(shell, n));
	let joined = pg
		.value(
			"select count(*) from pg_inherits where inhrelid = $1::regclass and inhparent = $2::regclass",
			&[&c, &qualified(shell)],
		)
		.await?;
	if joined.as_deref() == Some("0") {
		pg.simple(&chunk::attach_sql(shell, n)).await?;
	}
	Ok(())
}

async fn prepare(cx: &mut StepCtx, h: &mut Pg, t: &TransferSpec) -> Result<(), OpError> {
	let started = Instant::now();
	let before = load_catalog(h).await?;
	let mut after = before.clone();
	t.change.apply(&mut after)?;
	let w = match cx.get("chunk") {
		Some(_) => work(cx)?,
		None => {
			let w = plan_work(&before, &after, t)?;
			cx.save(h, json!({"chunk": w.to_json()})).await?;
			w
		}
	};
	if !w.local() && w.moves.is_empty() {
		return cx.save(h, json!({"phase": "noop"})).await;
	}
	let mut src = connect_node(&cx.app, &before, t.source).await?;
	let sv = version(&mut src).await?;
	let ks = after.keyspaces[&w.keyspace].clone();
	src.simple(MARK_SQL).await?;
	let ready = cx.get("tables_ready").and_then(Value::as_bool) == Some(true);

	// A conversion: the capture trigger first, so every write from here on is logged; then every
	// chunk table beside the heap, filled from it.
	// The preparation's DDL waits at most PREPARE_LOCK for a lock (a long COPY into a chunk would
	// otherwise queue it, and every client behind it); 55P03 is a passing error, so the engine runs
	// the step again later, and it is idempotent.
	if !ready {
		src.simple(&format!("set lock_timeout = '{PREPARE_LOCK}'"))
			.await?;
	}
	if w.local() && !ready {
		src.simple(&format!(
			"create table if not exists {log} (id bigserial primary key, rel text not null, key text not null); \
			alter table {log} add column if not exists second text",
			log = cap_log(cx)
		))
		.await?;
		for (i, rel) in w.tables.iter().enumerate() {
			let key = quote_ident(&w.key_columns[i]);
			// A second key is logged with the key: a row's chunk is the pair's.
			let (scol, sold, snew) = match w.second_columns.get(i).cloned().flatten() {
				Some(s) => {
					let s = quote_ident(&s);
					(
						", second".to_string(),
						format!(", old.{s}::text"),
						format!(", new.{s}::text"),
					)
				}
				None => (String::new(), String::new(), String::new()),
			};
			src.simple(&format!(
				"create or replace function {f}() returns trigger language plpgsql as $lepis$ begin \
				if tg_op <> 'INSERT' then insert into {log} (rel, key{scol}) values ({r}, old.{key}::text{sold}); end if; \
				if tg_op <> 'DELETE' then insert into {log} (rel, key{scol}) values ({r}, new.{key}::text{snew}); end if; \
				return null; end $lepis$; \
				drop trigger if exists {tg} on {t}; \
				create trigger {tg} after insert or update or delete on {t} for each row execute function {f}()",
				f = cap_fn(cx, i),
				log = cap_log(cx),
				r = quote_literal(&rel.to_string()),
				tg = quote_ident(&cap_trigger(cx)),
				t = qualified(&w.heap(rel)),
			))
			.await?;
			for c in w.building().into_iter().map(|n| w.chunk(n)) {
				let c = &c;
				let ct = chunk::chunk_table(&rel.table, c.number);
				if !exists(&mut src, &rel.schema, &ct).await? {
					src.simple(
						&chunk::create_sql(rel, &w.key_sql(i, None), &ks, *c, sv)
							.map_err(OpError::new)?,
					)
					.await?;
				}
			}
		}
		sync_all(&mut src, &w.tables).await?;
	}

	// Each target: the shell, its triggers and foreign keys, and an empty copy of each chunk.
	let mut targets = BTreeMap::new();
	for &n in w.moves.keys() {
		let mut pg = connect_node(&cx.app, &before, n).await?;
		pg.simple(MARK_SQL).await?;
		let tv = version(&mut pg).await?;
		if !ready {
			pg.simple(&format!("set lock_timeout = '{PREPARE_LOCK}'"))
				.await?;
			for (i, rel) in w.tables.iter().enumerate() {
				schema::ensure_table(&mut src, &mut pg, rel).await?;
				pg.simple(&chunk::ensure_shell_sql(rel)).await?;
				copy_extras(&mut src, &mut pg, rel).await?;
				for &num in &w.moves[&n] {
					let ct = chunk::chunk_table(&rel.table, num);
					if exists(&mut pg, &rel.schema, &ct).await? {
						pg.simple(&format!(
							"truncate {}",
							qualified(&chunk::chunk_relation(rel, num))
						))
						.await?;
					} else {
						pg.simple(
							&chunk::create_sql(rel, &w.key_sql(i, None), &ks, w.chunk(num), tv)
								.map_err(OpError::new)?,
						)
						.await?;
					}
				}
			}
			sync_all(&mut pg, &w.tables).await?;
			pg.simple("reset lock_timeout").await?;
		}
		targets.insert(n, pg);
	}
	if !ready {
		src.simple("reset lock_timeout").await?;
		cx.save(
			h,
			json!({"tables_ready": true, "tables_ms": started.elapsed().as_millis() as u64}),
		)
		.await?;
	}

	if w.local() {
		fill(cx, h, &mut src, &w, &ks, sv).await?;
		while replay(cx, &mut src, &w, &before).await? > REPLAY_SETTLED {
			if cx.cancelling(h).await? {
				return rollback(cx, h, t).await;
			}
		}
	}

	// Publications of the chunk tables themselves, a subscription per target.
	let labels = node_labels(h).await?;
	let source_node = before
		.nodes
		.get(&t.source)
		.ok_or_else(|| OpError::new("no source node"))?
		.clone();
	let none = json!({});
	for (&n, pg) in targets.iter_mut() {
		let (peer, peer_port) = super::spec::reach(
			&source_node.host,
			source_node.port,
			labels.get(&t.source).unwrap_or(&none),
			labels.get(&n).unwrap_or(&none),
		);
		let name = names(cx, n);
		let has_pub =
			src.value(
				"select count(*) from pg_publication where pubname = $1",
				&[&name],
			)
			.await?
			.as_deref() != Some("0");
		let mut items = Vec::new();
		for rel in &w.tables {
			for &num in &w.moves[&n] {
				items.push(qualified(&chunk::chunk_relation(rel, num)));
			}
		}
		items.push(format!(
			"lepis_move.mark where (job_id = {} and target = {})",
			cx.job, n.0
		));
		// A publication of this name already there (a run resumed, or a job id reused after the
		// catalog was made again) is set to exactly these tables.
		src.simple(&format!(
			"{} publication {} {} table {}",
			if has_pub { "alter" } else { "create" },
			quote_ident(&name),
			if has_pub { "set" } else { "for" },
			items.join(", ")
		))
		.await?;
		let has_sub =
			pg.value(
				"select count(*) from pg_subscription where subname = $1",
				&[&name],
			)
			.await?
			.as_deref() != Some("0");
		if has_sub {
			pg.simple(&format!(
				"alter subscription {} refresh publication with (copy_data = true)",
				quote_ident(&name)
			))
			.await?;
		} else {
			let info = super::pg::conninfo(&cx.app, &peer, peer_port, &source_node);
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

/// A conversion's chunks, each filled from the heap in one transaction (so a resumed fill
/// starts that chunk over), the ones done kept in the detail.
async fn fill(
	cx: &mut StepCtx,
	h: &mut Pg,
	src: &mut Pg,
	w: &Work,
	ks: &crate::catalog::Keyspace,
	sv: u32,
) -> Result<(), OpError> {
	let mut done: Vec<String> = cx
		.get("filled")
		.and_then(Value::as_array)
		.map(|a| {
			a.iter()
				.filter_map(|x| x.as_str().map(str::to_string))
				.collect()
		})
		.unwrap_or_default();
	let started = Instant::now();
	let mut rows: u64 = cx.get("filled_rows").and_then(Value::as_u64).unwrap_or(0);
	for (i, rel) in w.tables.iter().enumerate() {
		let cols = value_columns(src, rel)
			.await?
			.iter()
			.map(|c| quote_ident(c))
			.collect::<Vec<_>>()
			.join(", ");
		for c in w.building().into_iter().map(|n| w.chunk(n)) {
			let c = &c;
			let id = format!("{rel}#{}", c.number);
			if done.contains(&id) {
				continue;
			}
			if cx.cancelling(h).await? {
				return Err(cancelled());
			}
			let fence =
				chunk::chunk_fence(&w.key_sql(i, None), ks, *c, sv).map_err(OpError::new)?;
			let ct = qualified(&chunk::chunk_relation(rel, c.number));
			let r = src
				.simple(&format!(
					"begin; truncate {ct}; insert into {ct} ({cols}) select {cols} from only {} where {fence}; \
					select count(*) from {ct}; commit",
					qualified(&w.heap(rel))
				))
				.await?;
			rows += r
				.last()
				.and_then(|x| x.first().cloned().flatten())
				.and_then(|v| v.parse::<u64>().ok())
				.unwrap_or(0);
			done.push(id);
			cx.save(h, json!({"filled": done, "filled_rows": rows}))
				.await?;
		}
	}
	cx.save(
		h,
		json!({"fill_seconds": started.elapsed().as_secs_f64(), "filled_rows": rows}),
	)
	.await
}

/// One round of a conversion's capture log, in one transaction: the keys logged so far (up to
/// `REPLAY_BATCH`), each key's rows deleted from its chunk and copied again from the heap, the
/// log rows read deleted. Returns the keys still logged. `pg` may be inside a transaction of its
/// own (the cutover's), which `in_tx` says.
async fn replay(cx: &StepCtx, pg: &mut Pg, w: &Work, c: &Catalog) -> Result<u64, OpError> {
	replay_in(cx, pg, w, c, false).await
}

async fn replay_in(
	cx: &StepCtx,
	pg: &mut Pg,
	w: &Work,
	c: &Catalog,
	in_tx: bool,
) -> Result<u64, OpError> {
	let ks = &c.keyspaces[&w.keyspace];
	let log = cap_log(cx);
	if !in_tx {
		pg.simple("begin").await?;
	}
	let r = async {
		let rows = pg
			.simple(&format!(
				"select id, rel, key, second from {log} order by id limit {REPLAY_BATCH}"
			))
			.await?;
		let Some(max) = rows
			.last()
			.and_then(|r| r.first().cloned().flatten())
			.and_then(|v| v.parse::<i64>().ok())
		else {
			return Ok(0u64);
		};
		for (i, rel) in w.tables.iter().enumerate() {
			let name = rel.to_string();
			let second = w.second_columns.get(i).cloned().flatten();
			let mut by_chunk: BTreeMap<u32, Vec<(String, String)>> = BTreeMap::new();
			for r in &rows {
				if r.get(1).cloned().flatten().as_deref() != Some(name.as_str()) {
					continue;
				}
				let key = r.get(2).cloned().flatten().unwrap_or_default();
				let two = r.get(3).cloned().flatten().unwrap_or_default();
				let hash = match &second {
					Some(_) => ks
						.hash_pair(&key, &two)
						.map_err(|e| OpError::new(e.to_string()))?,
					None => ks
						.key_type
						.hash_text_value(&key, ks.seed)
						.map_err(|e| OpError::new(e.to_string()))?,
				};
				let n = crate::chunk::chunk_of(&w.chunks, hash).number;
				let keys = by_chunk.entry(n).or_default();
				let k = (key, two);
				if !keys.contains(&k) {
					keys.push(k);
				}
			}
			if by_chunk.is_empty() {
				continue;
			}
			let kt = key_type(pg, rel, &w.key_columns[i]).await?;
			let st = match &second {
				Some(col) => Some(key_type(pg, rel, col).await?),
				None => None,
			};
			let cols = value_columns(pg, rel)
				.await?
				.iter()
				.map(|c| quote_ident(c))
				.collect::<Vec<_>>()
				.join(", ");
			let key = quote_ident(&w.key_columns[i]);
			let mut sql = String::new();
			for (n, keys) in by_chunk {
				let arr = |ty: &str, f: &dyn Fn(&(String, String)) -> &String| {
					format!(
						"array[{}]::{ty}[]",
						keys.iter()
							.map(|k| quote_literal(f(k)))
							.collect::<Vec<_>>()
							.join(", ")
					)
				};
				// The rows of each logged key (each logged pair, with a second key).
				let which = match (&second, &st) {
					(Some(col), Some(st)) => format!(
						"({key}, {}) in (select * from unnest({}, {}))",
						quote_ident(col),
						arr(&kt, &|k| &k.0),
						arr(st, &|k| &k.1)
					),
					_ => format!("{key} = any ({})", arr(&kt, &|k| &k.0)),
				};
				let ct = qualified(&chunk::chunk_relation(rel, n));
				sql.push_str(&format!(
					"delete from {ct} where {which}; \
					insert into {ct} ({cols}) select {cols} from only {} where {which};\n",
					qualified(&w.heap(rel))
				));
			}
			pg.simple(&sql).await?;
		}
		pg.simple(&format!("delete from {log} where id <= {max}"))
			.await?;
		let left = pg
			.value(&format!("select count(*) from {log}"), &[])
			.await?
			.and_then(|v| v.parse().ok())
			.unwrap_or(0);
		Ok(left)
	}
	.await;
	if !in_tx {
		match &r {
			Ok(_) => {
				pg.simple("commit").await?;
			}
			Err(_) => {
				let _ = pg.simple("rollback").await;
			}
		}
	}
	r
}

async fn sync(cx: &mut StepCtx, h: &mut Pg, t: &TransferSpec) -> Result<(), OpError> {
	let w = work(cx)?;
	let c = load_catalog(h).await?;
	let mut src = connect_node(&cx.app, &c, t.source).await?;
	let mut targets = Vec::new();
	for &n in w.moves.keys() {
		targets.push((n, connect_node(&cx.app, &c, n).await?));
	}
	let started = Instant::now();
	let mut last_save = Instant::now() - Duration::from_secs(60);
	let mut stalled_since: Option<Instant> = None;
	loop {
		if cx.cancelling(h).await? {
			return rollback(cx, h, t).await;
		}
		let backlog = if w.local() {
			replay(cx, &mut src, &w, &c).await?
		} else {
			0
		};
		let mut pending = Vec::new();
		let mut stalled = false;
		for (n, pg) in targets.iter_mut() {
			let name = names(cx, *n);
			let r = pg
				.query(
					"select count(*) filter (where r.srsubstate <> 'r'), count(*), \
					(select count(*) from pg_stat_subscription w where w.subname = $1 and w.relid is not null) \
					from pg_subscription s left join pg_subscription_rel r on r.srsubid = s.oid \
					where s.subname = $1",
					&[&name],
				)
				.await?;
			let row = r.first().cloned().unwrap_or_default();
			let not_ready = row.first().cloned().flatten().unwrap_or_default();
			let total = row.get(1).cloned().flatten().unwrap_or_default();
			let copying = row.get(2).cloned().flatten().unwrap_or_default();
			if not_ready != "0" || total == "0" {
				pending.push(n.0);
			}
			if not_ready != "0" && copying == "0" {
				stalled = true;
			}
		}
		if last_save.elapsed() > Duration::from_secs(2) {
			cx.save(
				h,
				json!({"sync": {"pending": pending, "capture_backlog": backlog, "seconds": started.elapsed().as_secs()}}),
			)
			.await?;
			last_save = Instant::now();
		}
		stalled_since = if stalled {
			stalled_since.or(Some(Instant::now()))
		} else {
			None
		};
		if stalled_since.is_some_and(|at| at.elapsed() > COPY_STALL) {
			return Err(OpError::new(
				"a chunk's initial copy stopped with nothing left copying it",
			));
		}
		if pending.is_empty() && backlog <= REPLAY_SETTLED {
			let rtt = if targets.is_empty() {
				Some(Duration::ZERO)
			} else {
				mark_round_trip(cx, &mut src, &mut targets, Duration::from_secs(10)).await?
			};
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
				// A step that fails the same way every time is not waiting for a quiet moment.
				if attempt >= 12 && why.contains("failed") {
					return Err(OpError::new(format!(
						"the chunk cutover failed {attempt} times; the last: {why}"
					)));
				}
				tracing::info!(
					job = cx.job,
					step = cx.n,
					attempt,
					"chunk cutover postponed: {why}"
				);
				cx.save(h, json!({"attempts": attempt, "postponed": why}))
					.await?;
				let wait = Duration::from_millis(250 * (1u64 << attempt.min(7)));
				tokio::time::sleep(wait.min(Duration::from_secs(30))).await;
			}
		}
	}
}

/// The source's half of the seal, as SQL in the cutover's transaction: a conversion's heap
/// becomes the shell and keeps its chunks; every moved chunk leaves its publication and is
/// retired.
fn seal_sql(job: i64, step: i32, w: &Work) -> String {
	let pubname = |target: NodeId| format!("lepis_j{job}_s{step}_n{}", target.0);
	let trigger = format!("lepis_cap_j{job}_s{step}");
	let mut s = String::new();
	if let Some(old) = w.split_from {
		for rel in &w.tables {
			s.push_str(&format!(
				"drop trigger if exists {} on {};\n{};\n",
				quote_ident(&trigger),
				qualified(&chunk::chunk_relation(rel, old)),
				chunk::retire_sql(rel, old)
			));
			for &n in &w.keep {
				s.push_str(&chunk::attach_sql(rel, n));
				s.push_str(";\n");
			}
		}
	}
	if w.convert {
		let heaps: Vec<String> = w.tables.iter().map(qualified).collect();
		s.push_str(&format!(
			"lock table {} in access exclusive mode;\ntruncate only {};\n",
			heaps.join(", "),
			heaps.join(", ")
		));
		for rel in &w.tables {
			s.push_str(&chunk::shell_sql(rel));
			s.push_str(&format!(
				";\ndrop trigger if exists {} on {};\n",
				quote_ident(&trigger),
				qualified(rel)
			));
			for &n in &w.keep {
				s.push_str(&chunk::attach_sql(rel, n));
				s.push_str(";\n");
			}
		}
	}
	for (target, nums) in &w.moves {
		for rel in &w.tables {
			for &n in nums {
				s.push_str(&format!(
					"alter publication {} drop table {};\n{};\n",
					quote_ident(&pubname(*target)),
					qualified(&chunk::chunk_relation(rel, n)),
					if w.convert {
						chunk::retire_detached_sql(rel, n)
					} else {
						chunk::retire_sql(rel, n)
					}
				));
			}
		}
	}
	s
}

async fn try_cutover(
	cx: &mut StepCtx,
	h: &mut Pg,
	t: &TransferSpec,
	attempt: u64,
) -> Result<Attempt, OpError> {
	let s = cx.settings;
	let w = work(cx)?;
	let c = load_catalog(h).await?;
	let mut lock = connect_node(&cx.app, &c, t.source).await?;
	let mut side = connect_node(&cx.app, &c, t.source).await?;
	let mut snap_source = connect_node(&cx.app, &c, t.source).await?;
	let mut watch = Vec::new();
	let mut snaps = Vec::new();
	for &n in w.moves.keys() {
		watch.push((n, connect_node(&cx.app, &c, n).await?));
		snaps.push((n, connect_node(&cx.app, &c, n).await?));
	}
	// What stops writes: a conversion's heaps (EXCLUSIVE: the last replay still reads them), a
	// move's chunk tables on the source.
	let (locked, mode): (Vec<String>, &str) = if w.local() {
		(
			w.tables.iter().map(|r| qualified(&w.heap(r))).collect(),
			"exclusive",
		)
	} else {
		(
			w.tables
				.iter()
				.flat_map(|rel| {
					w.moved()
						.into_iter()
						.map(|n| qualified(&chunk::chunk_relation(rel, n)))
				})
				.collect(),
			"access exclusive",
		)
	};
	let lock_pid = lock.pid();
	let canceller = lock.canceller();
	lock.simple("set statement_timeout = 0; set lock_timeout = 0; begin")
		.await?;
	let t0 = Instant::now();
	let mut aborted = 0u64;
	let lock_sql = format!("lock table {} in {mode} mode", locked.join(", "));
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
	let give_up = async |lock: &mut Pg, why: String| -> Result<Attempt, OpError> {
		let _ = lock.simple("rollback").await;
		Ok(Attempt::Retry(why))
	};
	match granted {
		Ok(true) => {}
		Ok(false) => {
			return give_up(
				&mut lock,
				format!(
					"transactions in flight held the tables past {} ms ({aborted} older than drain_timeout aborted)",
					budget.as_millis()
				),
			)
			.await;
		}
		Err(e) => return give_up(&mut lock, format!("the lock failed: {e}")).await,
	}
	let drained = t0.elapsed();
	// The last of a conversion's log, committed on the side so it replicates before the mark.
	if w.local() {
		loop {
			match replay(cx, &mut side, &w, &c).await {
				Ok(0) => break,
				Ok(_) => {}
				Err(e) => return give_up(&mut lock, format!("the last replay failed: {e}")).await,
			}
			if t0.elapsed() > budget {
				return give_up(
					&mut lock,
					format!(
						"the capture log was not replayed within {} ms",
						budget.as_millis()
					),
				)
				.await;
			}
		}
	}
	let left = budget.saturating_sub(t0.elapsed());
	let rtt = if watch.is_empty() {
		Some(Duration::ZERO)
	} else {
		mark_round_trip(cx, &mut side, &mut watch, left).await?
	};
	if rtt.is_none() {
		return give_up(
			&mut lock,
			format!(
				"the targets were not caught up within {} ms",
				budget.as_millis()
			),
		)
		.await;
	}
	let caught_up = t0.elapsed();
	snap_source
		.simple("begin isolation level repeatable read; select 1")
		.await?;
	for (_, pg) in snaps.iter_mut() {
		pg.simple("begin isolation level repeatable read; select 1")
			.await?;
	}
	// The targets' chunks join their shells (nothing routes there until the catalog says so).
	for (n, pg) in watch.iter_mut() {
		for rel in &w.tables {
			for &num in &w.moves[n] {
				if let Err(e) = attach(pg, rel, num).await {
					return give_up(
						&mut lock,
						format!("{n}: a chunk could not join its shell: {e}"),
					)
					.await;
				}
			}
		}
	}
	let remaining = s.max_write_pause.saturating_sub(t0.elapsed());
	let seal = format!(
		"set local lock_timeout = '{}ms';\n{}",
		remaining.as_millis().max(1),
		seal_sql(cx.job, cx.n, &w)
	);
	if let Err(e) = lock.simple(&seal).await {
		return give_up(&mut lock, format!("sealing the source failed: {e}")).await;
	}
	let sealed = t0.elapsed();
	if sealed > s.max_write_pause.mul_f32(0.8) {
		return give_up(
			&mut lock,
			format!(
				"{} ms had gone before the catalog write; the ceiling is {} ms",
				sealed.as_millis(),
				s.max_write_pause.as_millis()
			),
		)
		.await;
	}
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
			Err(e) => return give_up(&mut lock, format!("the catalog write failed: {e}")).await,
		}
	};
	if let Some(d) = cx.detail.as_object_mut() {
		d.insert("phase".into(), json!("cut"));
	}
	let catalogued = t0.elapsed();
	let ack_limit = s
		.ack_timeout
		.min(s.max_write_pause.saturating_sub(t0.elapsed()));
	let (acked, unacked) = wait_acks(h, epoch, ack_limit).await?;
	let acks = t0.elapsed();
	// Whatever waited on the pause was planned against the old shape: it is cancelled, not run.
	let cancelled_waiters = side
		.query(
			"select pg_cancel_backend(pid) from pg_stat_activity where $1::int = any (pg_blocking_pids(pid))",
			&[&lock_pid.to_string()],
		)
		.await?
		.len();
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
				"aborted_transactions": aborted, "cancelled_waiters": cancelled_waiters,
				"routers_acked": acked, "routers_fenced_out": unacked,
			},
		}),
	)
	.await?;
	Ok(Attempt::Done(Held {
		source: snap_source,
		targets: snaps,
	}))
}

/// A cutover whose catalog write committed and whose source transaction did not (a crash in
/// between): the source's half again, then the targets'. No snapshot from inside the pause is
/// left, so the move finishes unverified.
async fn seal_after_crash(cx: &mut StepCtx, h: &mut Pg, t: &TransferSpec) -> Result<(), OpError> {
	let w = work(cx)?;
	let c = load_catalog(h).await?;
	let mut src = connect_node(&cx.app, &c, t.source).await?;
	let first = w
		.tables
		.first()
		.cloned()
		.ok_or_else(|| OpError::new("no table"))?;
	if let Some(old) = w.split_from {
		let ct = chunk::chunk_table(&first.table, old);
		if exists(&mut src, &first.schema, &ct).await? {
			src.simple("begin").await?;
			let r = async {
				let heaps: Vec<String> = w.tables.iter().map(|r| qualified(&w.heap(r))).collect();
				src.simple(&format!(
					"lock table {} in exclusive mode",
					heaps.join(", ")
				))
				.await?;
				while replay_in(cx, &mut src, &w, &c, true).await? > 0 {}
				src.simple(&seal_sql(cx.job, cx.n, &w)).await?;
				Ok::<(), OpError>(())
			}
			.await;
			finish(&mut src, r).await?;
		}
	} else if w.convert {
		let shell =
			src.value(
				"select count(*) from pg_constraint where conrelid = $1::regclass and conname = $2",
				&[&qualified(&first), chunk::SHELL_CONSTRAINT],
			)
			.await?
			.as_deref() != Some("0");
		if !shell {
			src.simple("begin").await?;
			let r = async {
				let heaps: Vec<String> = w.tables.iter().map(qualified).collect();
				src.simple(&format!(
					"lock table {} in exclusive mode",
					heaps.join(", ")
				))
				.await?;
				while replay_in(cx, &mut src, &w, &c, true).await? > 0 {}
				src.simple(&seal_sql(cx.job, cx.n, &w)).await?;
				Ok::<(), OpError>(())
			}
			.await;
			finish(&mut src, r).await?;
		}
	} else {
		// Only the chunks not retired yet.
		let mut sql = String::new();
		for (target, nums) in &w.moves {
			for rel in &w.tables {
				for &n in nums {
					let ct = chunk::chunk_table(&rel.table, n);
					if exists(&mut src, &rel.schema, &ct).await? {
						sql.push_str(&format!(
							"alter publication {} drop table if exists {};\n{};\n",
							quote_ident(&names(cx, *target)),
							qualified(&chunk::chunk_relation(rel, n)),
							chunk::retire_sql(rel, n)
						));
					}
				}
			}
		}
		if !sql.is_empty() {
			src.simple("begin").await?;
			let r = src.simple(&sql).await.map(|_| ());
			finish(&mut src, r).await?;
		}
	}
	for (&n, nums) in &w.moves {
		let mut pg = connect_node(&cx.app, &c, n).await?;
		for rel in &w.tables {
			for &num in nums {
				attach(&mut pg, rel, num).await?;
			}
		}
	}
	cx.save(
		h,
		json!({"phase": "sealed", "verify": "skipped: the cutover was finished after a restart, with no snapshot from inside the pause"}),
	)
	.await
}

/// Commits an open transaction when `r` is fine, else rolls it back and returns the error.
async fn finish(pg: &mut Pg, r: Result<(), OpError>) -> Result<(), OpError> {
	match r {
		Ok(()) => {
			pg.simple("commit").await?;
			Ok(())
		}
		Err(e) => {
			let _ = pg.simple("rollback").await;
			Err(e)
		}
	}
}

async fn verify(cx: &mut StepCtx, h: &mut Pg, held: Option<Held>) -> Result<(), OpError> {
	let Some(mut held) = held else {
		return cx.save(h, json!({"phase": "unverified"})).await;
	};
	let w = work(cx)?;
	let c = load_catalog(h).await?;
	let ks = c.keyspaces[&w.keyspace].clone();
	let sv = version(&mut held.source).await?;
	let sum = |from: &str| {
		format!(
			"select count(*), coalesce(sum(hashtextextended(lepis_row::text, 0)::numeric), 0) from {from}"
		)
	};
	// The source's side of chunk `n` of `rel`: its retired copy (a conversion's heap cannot be
	// read from the pause's snapshot: TRUNCATE is not MVCC-safe), or a split's old chunk's slice.
	let source_of = |i: usize, rel: &RelationName, n: u32| -> Result<String, OpError> {
		if let Some(old) = w.split_from {
			let fence = chunk::chunk_fence(&w.key_sql(i, None), &ks, w.chunk(n), sv)
				.map_err(OpError::new)?;
			let live = chunk::chunk_relation(rel, old);
			Ok(format!(
				"(select * from {}.{} where {fence}) lepis_row",
				quote_ident(&live.schema),
				quote_ident(&chunk::retired_name(&live.table))
			))
		} else {
			let live = chunk::chunk_relation(rel, n);
			Ok(format!(
				"{}.{} lepis_row",
				quote_ident(&live.schema),
				quote_ident(&chunk::retired_name(&live.table))
			))
		}
	};
	let cell = |r: &[Vec<Option<String>>], i: usize| {
		r.first()
			.and_then(|x| x.get(i).cloned().flatten())
			.unwrap_or_default()
	};
	let mut results = Vec::new();
	let mut equal = true;
	for (i, rel) in w.tables.iter().enumerate() {
		// A split's halves against the old chunk; a conversion's kept chunks have nothing left
		// to be compared with (their heap is emptied), and were filled and replayed from it.
		for &n in w.keep.iter().filter(|_| w.split_from.is_some()) {
			let a = held.source.simple(&sum(&source_of(i, rel, n)?)).await?;
			let b = held
				.source
				.simple(&sum(&format!(
					"{} lepis_row",
					qualified(&chunk::chunk_relation(rel, n))
				)))
				.await?;
			let same = a == b;
			equal &= same;
			results.push(
				json!({"chunk": n, "table": rel.to_string(), "kept": true, "equal": same,
				"source_rows": cell(&a, 0), "chunk_rows": cell(&b, 0)}),
			);
		}
		for (target, pg) in held.targets.iter_mut() {
			for &n in &w.moves[target] {
				let a = held.source.simple(&sum(&source_of(i, rel, n)?)).await?;
				let b = pg
					.simple(&sum(&format!(
						"{} lepis_row",
						qualified(&chunk::chunk_relation(rel, n))
					)))
					.await?;
				let same = a == b;
				equal &= same;
				results.push(
					json!({"chunk": n, "table": rel.to_string(), "target": target.0,
					"equal": same, "source_rows": cell(&a, 0), "target_rows": cell(&b, 0),
					"source_checksum": cell(&a, 1), "target_checksum": cell(&b, 1)}),
				);
			}
		}
	}
	for (_, pg) in held.targets.iter_mut() {
		let _ = pg.simple("rollback").await;
	}
	let _ = held.source.simple("rollback").await;
	cx.save(
		h,
		json!({"phase": if equal { "verified" } else { "verify_failed" }, "verify": results}),
	)
	.await
}

async fn teardown(
	cx: &mut StepCtx,
	h: &mut Pg,
	t: &TransferSpec,
	next: &str,
) -> Result<(), OpError> {
	let w = work(cx)?;
	let c = load_catalog(h).await?;
	let mut src = connect_node(&cx.app, &c, t.source).await?;
	for &n in w.moves.keys() {
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
	if w.local() {
		// The function goes with its trigger, wherever the trigger is (a split's old chunk is
		// renamed by now).
		let mut drop = String::new();
		for (i, _) in w.tables.iter().enumerate() {
			drop.push_str(&format!(
				"drop function if exists {}() cascade;\n",
				cap_fn(cx, i)
			));
		}
		drop.push_str(&format!("drop table if exists {}", cap_log(cx)));
		src.simple(&drop).await?;
	}
	src.simple(&format!(
		"delete from lepis_move.mark where job_id = {}",
		cx.job
	))
	.await
	.ok();
	cx.save(h, json!({"phase": next})).await
}

/// The source's retired chunks, dropped: no DELETE, no VACUUM.
async fn cleanup(cx: &mut StepCtx, h: &mut Pg, t: &TransferSpec) -> Result<(), OpError> {
	if !cx.get("verify").is_some_and(Value::is_array) {
		return cx
			.save(h, json!({"phase": "cleaned", "cleanup": "skipped: the move was not verified; drop the lepis_retired_ tables once it is"}))
			.await;
	}
	let w = work(cx)?;
	let c = load_catalog(h).await?;
	let mut src = connect_node(&cx.app, &c, t.source).await?;
	let mut gone = Vec::new();
	for rel in &w.tables {
		for n in w.moved().into_iter().chain(w.split_from) {
			let live = chunk::chunk_relation(rel, n);
			gone.push(format!(
				"{}.{}",
				quote_ident(&live.schema),
				quote_ident(&chunk::retired_name(&live.table))
			));
		}
	}
	if !gone.is_empty() {
		src.simple(&format!("drop table if exists {}", gone.join(", ")))
			.await?;
	}
	cx.save(
		h,
		json!({"phase": "cleaned", "cleanup": {"dropped": gone.len()}}),
	)
	.await
}

async fn rollback(cx: &mut StepCtx, h: &mut Pg, t: &TransferSpec) -> Result<(), OpError> {
	teardown(cx, h, t, "rolling_back").await?;
	let w = work(cx)?;
	let c = load_catalog(h).await?;
	for (&n, nums) in &w.moves {
		let mut pg = connect_node(&cx.app, &c, n).await?;
		let mut drop = Vec::new();
		for rel in &w.tables {
			for &num in nums {
				drop.push(qualified(&chunk::chunk_relation(rel, num)));
			}
		}
		if !drop.is_empty() {
			pg.simple(&format!("drop table if exists {}", drop.join(", ")))
				.await?;
		}
	}
	if w.local() {
		let mut src = connect_node(&cx.app, &c, t.source).await?;
		let mut drop = Vec::new();
		for rel in &w.tables {
			for n in w.building() {
				drop.push(qualified(&chunk::chunk_relation(rel, n)));
			}
		}
		if !drop.is_empty() {
			src.simple(&format!("drop table if exists {}", drop.join(", ")))
				.await?;
		}
	}
	cx.save(h, json!({"phase": "rolled_back"})).await?;
	Err(cancelled())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{Keyspace, Node, NodeState, Strategy};
	use crate::config::SslMode;
	use crate::hash::KeyType;
	use std::collections::HashMap;

	fn rel(t: &str) -> RelationName {
		RelationName {
			schema: "public".into(),
			table: t.into(),
		}
	}

	fn catalog(nodes: i32, chunks: u32) -> Catalog {
		let mut c = Catalog {
			epoch: 1,
			..Default::default()
		};
		for i in 1..=nodes {
			c.nodes.insert(
				NodeId(i),
				Node {
					id: NodeId(i),
					name: format!("n{i}"),
					host: "h".into(),
					port: 5432,
					dbname: "d".into(),
					sslmode: SslMode::Disable,
					home: i == 1,
					state: NodeState::Active,
					server_version_num: Some(180_000),
				},
			);
		}
		let ids: Vec<NodeId> = (1..=nodes).map(NodeId).collect();
		let mut ks = Keyspace {
			name: "k".into(),
			strategy: Strategy::Hash,
			key_type: KeyType::Int8,
			seed: 3,
			ranges: Keyspace::even_ranges(nodes as usize, &ids),
			pins: HashMap::new(),
			chunks: Vec::new(),
			second: None,
		};
		ks.chunks = chunk::chunks_for(chunks, &ks).unwrap();
		c.keyspaces.insert("k".into(), ks);
		c
	}

	#[test]
	fn a_conversion_keeps_the_sources_chunks_and_sends_the_rest() {
		let before = catalog(4, 16);
		let mut after = before.clone();
		after.relations.insert(
			rel("t"),
			RelationKind::Sharded {
				keyspace: "k".into(),
				key_column: "id".into(),
			},
		);
		after.chunked.insert(rel("t"));
		let t = TransferSpec {
			source: NodeId(1),
			targets: vec![NodeId(2), NodeId(3), NodeId(4)],
			tables: vec![rel("t")],
			change: super::super::Change::None,
		};
		let w = plan_work(&before, &after, &t).unwrap();
		assert!(w.convert);
		assert_eq!(w.keep.len(), 4);
		assert_eq!(w.moves.len(), 3);
		assert!(w.moves.values().all(|v| v.len() == 4));
		let back = Work::from_json(&w.to_json()).unwrap();
		assert_eq!(back.moves, w.moves);
		assert_eq!(back.keep, w.keep);
		assert_eq!(back.chunks, w.chunks);
		let s = seal_sql(7, 1, &w);
		assert!(s.contains("truncate only \"public\".\"t\""), "{s}");
		assert_eq!(s.matches(" inherit \"public\".\"t\";").count(), 4, "{s}");
		assert_eq!(s.matches("rename to").count(), 12);
	}

	#[test]
	fn a_move_sends_only_what_changes_owner() {
		let mut before = catalog(2, 8);
		before.relations.insert(
			rel("t"),
			RelationKind::Sharded {
				keyspace: "k".into(),
				key_column: "id".into(),
			},
		);
		before.chunked.insert(rel("t"));
		let mut after = before.clone();
		let r = after.keyspaces["k"].ranges[0];
		after.keyspaces.get_mut("k").unwrap().ranges[0].node = NodeId(2);
		let t = TransferSpec {
			source: r.node,
			targets: vec![NodeId(2)],
			tables: vec![rel("t")],
			change: super::super::Change::None,
		};
		let w = plan_work(&before, &after, &t).unwrap();
		assert!(!w.convert);
		assert_eq!(w.moves[&NodeId(2)].len(), 4);
		assert!(w.keep.is_empty());
	}
}
