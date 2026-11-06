//! Running one step of a job. Every step reads the catalog fresh, checks what is already done,
//! and does the rest, so running it twice is the same as running it once.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use super::Kind;
use super::spec::{NodeSpec, Step, sslmode_name};
use super::{
	Change, OpError, Pg, Settings, Target, change_catalog, load_catalog, qualified, schema,
	state_name, transfer,
};
use crate::catalog::{
	self, Catalog, NodeId, NodeState, RelationKind, RelationName, owns_check, quote_ident,
	quote_literal,
};
use crate::server::App;

/// Rows deleted per statement when a node gives up rows it no longer owns.
pub const CLEANUP_BATCH: usize = 10_000;

/// What a running step knows about itself, and its durable progress.
pub struct StepCtx {
	pub app: Arc<App>,
	pub job: i64,
	pub n: i32,
	pub settings: Settings,
	pub detail: Value,
}

impl StepCtx {
	/// Merges `patch` into the step's durable detail, on the home node and here.
	pub async fn save(&mut self, h: &mut Pg, patch: Value) -> Result<(), OpError> {
		if let (Some(d), Some(p)) = (self.detail.as_object_mut(), patch.as_object()) {
			for (k, v) in p {
				d.insert(k.clone(), v.clone());
			}
		}
		h.query(
			"update lepis.job_step set detail = detail || $1::jsonb where job_id = $2 and n = $3",
			&[
				&patch.to_string(),
				&self.job.to_string(),
				&self.n.to_string(),
			],
		)
		.await?;
		Ok(())
	}

	/// SQL that does the same as `save`, to run inside another transaction on the home node.
	pub fn save_sql(&self, patch: &Value) -> String {
		format!(
			"update lepis.job_step set detail = detail || {}::jsonb where job_id = {} and n = {};",
			quote_literal(&patch.to_string()),
			self.job,
			self.n
		)
	}

	pub fn get(&self, k: &str) -> Option<&Value> {
		self.detail.get(k)
	}

	pub fn phase(&self) -> &str {
		self.detail
			.get("phase")
			.and_then(Value::as_str)
			.unwrap_or("new")
	}

	/// Whether someone asked for this job to stop.
	pub async fn cancelling(&self, h: &mut Pg) -> Result<bool, OpError> {
		Ok(h.value(
			"select state from lepis.job where id = $1",
			&[&self.job.to_string()],
		)
		.await?
		.as_deref()
			== Some("cancelling"))
	}
}

/// Cancelled by the user: the step stopped cleanly.
pub fn cancelled() -> OpError {
	OpError {
		code: Some("57014".into()),
		message: "the job was cancelled".into(),
		kind: Some(super::Kind::Cancelled),
	}
}

pub async fn node_target(app: &App, c: &Catalog, id: NodeId) -> Result<Target, OpError> {
	let n = c
		.nodes
		.get(&id)
		.ok_or_else(|| OpError::refused(Kind::NoSuchNode, format!("there is no {id}")))?;
	Ok(Target::of(app, n))
}

pub async fn connect_node(app: &App, c: &Catalog, id: NodeId) -> Result<Pg, OpError> {
	Pg::connect(app, &node_target(app, c, id).await?).await
}

pub async fn version(pg: &mut Pg) -> Result<u32, OpError> {
	pg.value("select current_setting('server_version_num')", &[])
		.await?
		.and_then(|v| v.parse().ok())
		.ok_or_else(|| OpError::new("no server_version_num"))
}

/// Whether a node is a physical standby still replaying its source (`pg_is_in_recovery()`): a node
/// attached as `joining` from the home's backup (Cloud) until a range moves onto it and promotes it.
/// It cannot be written, and needs no writing: every fence, table and delete on its source reaches
/// it by replay. So the steps that write to every node pass it by, rather than failing the job on
/// `25006 read-only transaction` (found live on 2026-10-11: a table distributed while a new node was
/// joining failed at its fences).
pub async fn in_recovery(pg: &mut Pg) -> Result<bool, OpError> {
	Ok(pg
		.simple("select pg_is_in_recovery()")
		.await?
		.first()
		.and_then(|r| r.first().cloned().flatten())
		.as_deref()
		== Some("t"))
}

/// Runs a fence (or any short ALTER on a busy table) with a lock timeout, retrying, so it never
/// queues writers behind it for long.
pub async fn alter_briefly(pg: &mut Pg, sql: &str) -> Result<(), OpError> {
	let mut last = None;
	for attempt in 0..20u64 {
		match pg
			.simple(&format!(
				"begin; set local lock_timeout = '200ms'; {sql}; commit"
			))
			.await
		{
			Ok(_) => return Ok(()),
			Err(e) if e.is("55P03") || e.is("40P01") => {
				let _ = pg.simple("rollback").await;
				last = Some(e);
				tokio::time::sleep(Duration::from_millis(50 * (attempt + 1))).await;
			}
			Err(e) => {
				let _ = pg.simple("rollback").await;
				return Err(e);
			}
		}
	}
	Err(last.unwrap_or_else(|| OpError::new("lock timeout")))
}

/// The fence `node` should carry for a sharded relation under `c` (None when it is not sharded).
pub fn fence_for(
	c: &Catalog,
	rel: &RelationName,
	node: NodeId,
	version: u32,
) -> Result<Option<String>, OpError> {
	// A chunked table's fences are its chunks' own, fixed for each chunk's life (chunk.rs).
	if c.chunked.contains(rel) {
		return Ok(None);
	}
	match c.relations.get(rel) {
		Some(RelationKind::Sharded {
			keyspace,
			key_column,
		}) => {
			let ks = c
				.keyspaces
				.get(keyspace)
				.ok_or_else(|| OpError::new(format!("no keyspace {keyspace}")))?;
			catalog::fence_sql(rel, key_column, ks, node, version)
				.map(Some)
				.map_err(|e| OpError::refused(Kind::NodeUnsuitable, e.0))
		}
		_ => Ok(None),
	}
}

/// True for the rows `node` owns of `rel` under `c`: the fence's expression for a sharded
/// relation, everything for a reference table, and for a global (or unknown) table everything
/// on the home node and nothing elsewhere. `column` turns the key column's name into SQL.
pub fn owns_expr(
	c: &Catalog,
	rel: &RelationName,
	node: NodeId,
	version: u32,
	column: &dyn Fn(&str) -> String,
) -> Result<String, OpError> {
	match c.relations.get(rel) {
		Some(RelationKind::Sharded {
			keyspace,
			key_column,
		}) => {
			let ks = c
				.keyspaces
				.get(keyspace)
				.ok_or_else(|| OpError::new(format!("no keyspace {keyspace}")))?;
			let key = catalog::KeySql {
				key: column(key_column),
				second: c.second_keys.get(rel).map(|s| column(s)),
			};
			owns_check(key, ks, node, version)
				.map_err(|e| OpError::refused(Kind::NodeUnsuitable, e.0))
		}
		Some(RelationKind::Reference) => Ok("true".into()),
		_ => Ok(if c.home().map(|n| n.id) == Some(node) {
			"true".into()
		} else {
			"false".into()
		}),
	}
}

pub async fn run(cx: &mut StepCtx, h: &mut Pg, step: &Step) -> Result<(), OpError> {
	match step {
		Step::NodeCheck(spec) => node_check(cx, h, spec).await,
		Step::NodeInsert { id, spec } => node_insert(cx, h, *id, spec).await,
		Step::RolesSync { node } => {
			let c = load_catalog(h).await?;
			let mut home = Pg::connect(&cx.app, &Target::home(&cx.app)).await?;
			let mut dst = connect_node(&cx.app, &c, *node).await?;
			let n = schema::sync_roles(&mut home, &mut dst).await?;
			cx.save(h, json!({"roles": n})).await
		}
		Step::TableCreate { node, table } => table_create(cx, h, *node, table).await,
		// L8: physical when the one target is a standby of the source (`physical.rs`), else logical.
		Step::Transfer(t) => {
			// Chunked tables move as whole chunk tables.
			let mut after = load_catalog(h).await?;
			t.change.apply(&mut after)?;
			if super::physical::select(cx, h, t).await? {
				super::physical::run(cx, h, t).await
			} else if super::chunkmove::applies(&after, t) {
				super::chunkmove::run(cx, h, t).await
			} else {
				transfer::run(cx, h, t).await
			}
		}
		Step::Catalog(change) => {
			let epoch = change_catalog(h, change, "").await?;
			cx.save(h, json!({"epoch": epoch})).await
		}
		Step::Fences { keyspace } => fences(cx, h, keyspace).await,
		Step::TableDrop { node, table } => {
			let c = load_catalog(h).await?;
			let mut pg = connect_node(&cx.app, &c, *node).await?;
			pg.simple(&format!("drop table if exists {}", qualified(table)))
				.await?;
			Ok(())
		}
		Step::Verify { keyspace } => verify(cx, h, keyspace.as_deref()).await,
		Step::Cleanup { node } => cleanup(cx, h, *node).await,
		Step::NodeAttach {
			id,
			spec,
			standby_of,
		} => super::attach::run(cx, h, *id, spec, *standby_of).await,
		Step::NodeFailover { node, spec, fenced } => {
			super::failover::run(cx, h, *node, spec, *fenced).await
		}
		Step::NodeReaddress { node, spec } => super::readdress::run(cx, h, *node, spec).await,
		Step::RestorePoint { name } => super::restore_point::run(cx, h, name).await,
		Step::Dissolve => dissolve(cx, h).await,
		Step::Dechunk { table } => {
			let c = load_catalog(h).await?;
			if !c.chunked.contains(table) {
				return Ok(());
			}
			let change = Change::Many(vec![
				Change::Chunked {
					name: table.clone(),
					chunked: false,
				},
				Change::Relation {
					name: table.clone(),
					kind: RelationKind::Global,
				},
			]);
			let epoch = change_catalog(
				h,
				&change,
				&format!(
					"{};",
					crate::chunk::dechunk_sql(std::slice::from_ref(table))
				),
			)
			.await?;
			cx.save(h, json!({"epoch": epoch})).await
		}
	}
}

/// `dissolve`'s one step, on a cluster whose home is its only node (the plan refused otherwise;
/// asked again here, since a job may run long after it was planned). Nothing is left in doubt
/// first: a prepared two-phase transaction needs a router to finish it, and the router is what
/// goes next. Then each distributed table loses its fence, and the catalog forgets every keyspace
/// and relation in one write. Rows are untouched: on one node they were all there already.
/// Striped sequences keep their stride, so the values they hand out stay unique (with gaps).
async fn dissolve(cx: &mut StepCtx, h: &mut Pg) -> Result<(), OpError> {
	let c = load_catalog(h).await?;
	let home = c
		.home()
		.map(|n| n.id)
		.ok_or_else(|| OpError::refused(Kind::CatalogInvalid, "the catalog has no home node"))?;
	if let Some(n) = c
		.nodes
		.values()
		.find(|n| n.id != home && n.state != NodeState::Removed)
	{
		return Err(OpError::refused(
			Kind::NotSingleNode,
			format!(
				"{} joined the cluster after this was planned; drain and remove it first",
				n.name
			),
		));
	}
	let waiting = h
		.simple(
			"select (select count(*) from pg_prepared_xacts 				where gid like 'lepis:' || (select uid from lepis.cluster where id = 1) || ':%') 			+ (select count(*) from lepis.prepared where decision = 'commit')",
		)
		.await?
		.first()
		.and_then(|r| r.first().cloned().flatten())
		.and_then(|v| v.parse::<i64>().ok())
		.unwrap_or(0);
	if waiting > 0 {
		return Err(OpError::refused(
			Kind::InDoubt,
			format!(
				"{waiting} two-phase transaction(s) are not finished yet; resume this job in a minute, once the router has finished them"
			),
		));
	}
	let mut sharded: Vec<&RelationName> = c
		.relations
		.iter()
		.filter(|(_, k)| matches!(k, RelationKind::Sharded { .. }))
		.map(|(r, _)| r)
		.collect();
	sharded.sort();
	let mut pg = connect_node(&cx.app, &c, home).await?;
	let mut unfenced = Vec::new();
	for rel in sharded {
		if schema::exists(&mut pg, rel).await? {
			alter_briefly(
				&mut pg,
				&format!(
					"alter table {} drop constraint if exists lepis_owns",
					qualified(rel)
				),
			)
			.await?;
			unfenced.push(rel.to_string());
		}
	}
	let mut forgot: Vec<Change> = Vec::new();
	let mut tables: Vec<String> = c.relations.keys().map(|r| r.to_string()).collect();
	tables.sort();
	for r in c.relations.keys() {
		forgot.push(Change::DropRelation { name: r.clone() });
	}
	let mut keyspaces: Vec<String> = c.keyspaces.keys().cloned().collect();
	keyspaces.sort();
	for k in &keyspaces {
		forgot.push(Change::DropKeyspace { name: k.clone() });
	}
	// Chunked tables become one heap again, in the transaction that forgets them.
	let mut chunked: Vec<&RelationName> = c.chunked.iter().collect();
	chunked.sort();
	let owned: Vec<RelationName> = chunked.iter().map(|r| (*r).clone()).collect();
	let extra = if owned.is_empty() {
		String::new()
	} else {
		format!("{};\n", crate::chunk::dechunk_sql(&owned))
	};
	let epoch = change_catalog(h, &Change::Many(forgot), &extra).await?;
	cx.save(
		h,
		json!({"epoch": epoch, "unfenced": unfenced, "dechunked": chunked.iter().map(|r| r.to_string()).collect::<Vec<_>>(), "forgot": {"tables": tables, "keyspaces": keyspaces}}),
	)
	.await
}

/// L1's requirements for a node, each refused with the setting to change.
async fn node_check(cx: &mut StepCtx, h: &mut Pg, spec: &NodeSpec) -> Result<(), OpError> {
	let t = Target {
		label: format!("{} ({}:{})", spec.name, spec.host, spec.port),
		address: crate::config::NodeAddress {
			host: spec.host.clone(),
			port: spec.port,
			sslmode: spec.sslmode,
			ca_file: cx.app.config.home().ca_file.clone(),
		},
		dbname: spec.dbname.clone(),
	};
	let mut pg = Pg::connect(&cx.app, &t).await?;
	let report = check_node(&mut pg).await?;
	cx.save(h, report).await
}

/// What a node add checks, as a report; refuses with the fix when a requirement is not met.
pub async fn check_node(pg: &mut Pg) -> Result<Value, OpError> {
	let rows = pg
		.query(
			"select current_setting('server_version_num'), current_setting('wal_level'), \
			current_setting('max_prepared_transactions'), \
			(select rolsuper or rolreplication from pg_roles where rolname = current_user), \
			current_setting('max_replication_slots'), \
			current_setting('idle_replication_slot_timeout', true)",
			&[],
		)
		.await?;
	let r = rows.first().cloned().unwrap_or_default();
	let get = |i: usize| r.get(i).cloned().flatten().unwrap_or_default();
	let version: u32 = get(0).parse().unwrap_or(0);
	if let Some(m) = crate::catalog::version_refusal(&pg.label, version) {
		return Err(OpError::refused(Kind::NodeUnsuitable, m));
	}
	if get(1) != "logical" {
		return Err(OpError::refused(
			Kind::NodeUnsuitable,
			format!(
				"{} has wal_level = {}; moves need wal_level = logical (on RDS, rds.logical_replication = 1), then a restart",
				pg.label,
				get(1)
			),
		));
	}
	if get(2) == "0" {
		return Err(OpError::refused(
			Kind::NodeUnsuitable,
			format!(
				"{} has max_prepared_transactions = 0; two-phase commit needs it above 0, then a restart",
				pg.label
			),
		));
	}
	if get(3) != "t" {
		return Err(OpError::refused(
			Kind::NodeUnsuitable,
			format!(
				"{}: Lepis's service login needs SUPERUSER, or REPLICATION and the right to create subscriptions",
				pg.label
			),
		));
	}
	let mut warnings = Vec::new();
	let idle = get(5);
	if version >= 180_000 && (idle.is_empty() || idle == "0") {
		warnings.push(
			"idle_replication_slot_timeout is off: a slot a crashed job leaves behind keeps WAL until the job is resumed or cancelled; setting it is the safety net (Lepis does not change it, it applies to every slot on the server)".to_string(),
		);
	}
	Ok(json!({
		"server_version_num": version,
		"wal_level": get(1),
		"max_prepared_transactions": get(2),
		"max_replication_slots": get(4),
		"idle_replication_slot_timeout": if idle.is_empty() { Value::Null } else { json!(idle) },
		"warnings": warnings,
	}))
}

async fn node_insert(
	cx: &mut StepCtx,
	h: &mut Pg,
	id: NodeId,
	spec: &NodeSpec,
) -> Result<(), OpError> {
	let t = Target {
		label: spec.name.clone(),
		address: crate::config::NodeAddress {
			host: spec.host.clone(),
			port: spec.port,
			sslmode: spec.sslmode,
			ca_file: cx.app.config.home().ca_file.clone(),
		},
		dbname: spec.dbname.clone(),
	};
	let mut pg = Pg::connect(&cx.app, &t).await?;
	let v = version(&mut pg).await?;
	if let Some(m) = crate::catalog::version_refusal(&pg.label, v) {
		return Err(OpError::refused(Kind::NodeUnsuitable, m));
	}
	let labels = spec.address_labels();
	h.simple(&format!(
		"begin; insert into lepis.node (id, name, host, port, dbname, sslmode, kind, state, server_version_num, labels) \
		values ({}, {}, {}, {}, {}, '{}', 'data', 'joining', {v}, {}::jsonb) on conflict (id) do nothing; \
		select lepis.bump(); commit",
		id.0,
		quote_literal(&spec.name),
		quote_literal(&spec.host),
		spec.port,
		quote_literal(&spec.dbname),
		sslmode_name(spec.sslmode),
		quote_literal(&labels.to_string()),
	))
	.await?;
	Ok(())
}

/// Makes `table` on `node` from the definition on the home node (or the first node that has it),
/// fenced as the catalog says.
async fn table_create(
	cx: &mut StepCtx,
	h: &mut Pg,
	node: NodeId,
	table: &RelationName,
) -> Result<(), OpError> {
	let c = load_catalog(h).await?;
	let mut dst = connect_node(&cx.app, &c, node).await?;
	// A standby has the table, and its source's fence, by replay.
	if in_recovery(&mut dst).await? {
		return Ok(());
	}
	if !schema::exists(&mut dst, table).await? {
		let mut made = false;
		let mut sources: Vec<NodeId> = c.home().map(|n| n.id).into_iter().collect();
		sources.extend(
			c.nodes
				.values()
				.filter(|n| !n.home && n.id != node)
				.map(|n| n.id),
		);
		for s in sources {
			let mut src = connect_node(&cx.app, &c, s).await?;
			if schema::exists(&mut src, table).await? {
				schema::ensure_table(&mut src, &mut dst, table).await?;
				made = true;
				break;
			}
		}
		if !made {
			return Err(OpError::refused(
				Kind::NoSuchTable,
				format!("no node has {table}"),
			));
		}
	}
	let v = version(&mut dst).await?;
	if let Some(f) = fence_for(&c, table, node, v)? {
		alter_briefly(&mut dst, &f).await?;
	}
	Ok(())
}

/// Every fence of a keyspace, on every node that has its tables, from the catalog.
async fn fences(cx: &mut StepCtx, h: &mut Pg, keyspace: &str) -> Result<(), OpError> {
	let c = load_catalog(h).await?;
	let tables = tables_of(&c, keyspace);
	let mut done = 0;
	let mut standbys = Vec::new();
	for n in c.nodes.values().filter(|n| n.state != NodeState::Removed) {
		let mut pg = connect_node(&cx.app, &c, n.id).await?;
		if in_recovery(&mut pg).await? {
			standbys.push(n.id.0);
			continue;
		}
		let v = version(&mut pg).await?;
		for t in &tables {
			if schema::exists(&mut pg, t).await?
				&& let Some(f) = fence_for(&c, t, n.id, v)?
			{
				alter_briefly(&mut pg, &f).await?;
				done += 1;
			}
		}
	}
	cx.save(h, json!({"fences": done, "standbys_passed": standbys}))
		.await
}

/// The sharded relations of a keyspace, in name order.
pub fn tables_of(c: &Catalog, keyspace: &str) -> Vec<RelationName> {
	let mut out: Vec<RelationName> = c
		.relations
		.iter()
		.filter(|(_, k)| matches!(k, RelationKind::Sharded { keyspace: ks, .. } if ks == keyspace))
		.map(|(n, _)| n.clone())
		.collect();
	out.sort();
	out
}

/// L13's verify: on every node, for every sharded table, the rows it owns (count and an
/// order-independent checksum over the row text), the rows it holds but does not own (left for
/// cleanup), and whether its fence is there; reference tables are compared node by node.
async fn verify(cx: &mut StepCtx, h: &mut Pg, keyspace: Option<&str>) -> Result<(), OpError> {
	let c = load_catalog(h).await?;
	let mut report = Vec::new();
	let mut ok = true;
	let mut reference: Vec<(RelationName, Vec<(String, String)>)> = Vec::new();
	let mut names: Vec<(&RelationName, &RelationKind)> = c.relations.iter().collect();
	names.sort_by(|a, b| a.0.cmp(b.0));
	for n in c.nodes.values().filter(|n| n.state != NodeState::Removed) {
		let mut pg = connect_node(&cx.app, &c, n.id).await?;
		let v = version(&mut pg).await?;
		// A joining standby is a whole copy of its source until a move promotes it: every chunk
		// is there by replay, owned or not, and it gives back what it does not own afterwards.
		let whole_copy = super::physical::standby_of(h, n.id).await?.is_some();
		for (rel, kind) in &names {
			let in_scope = match (kind, keyspace) {
				(RelationKind::Sharded { keyspace: k, .. }, Some(want)) => k == want,
				(RelationKind::Sharded { .. }, None) => true,
				(RelationKind::Reference, None) => true,
				_ => false,
			};
			if !in_scope || !schema::exists(&mut pg, rel).await? {
				continue;
			}
			let owns = owns_expr(&c, rel, n.id, v, &|col| {
				format!("lepis_row.{}", quote_ident(col))
			})?;
			let rows = pg
				.simple(&format!(
					"select count(*) filter (where {owns}), count(*) filter (where not ({owns})), \
					coalesce(sum(hashtextextended(lepis_row::text, 0)::numeric) filter (where {owns}), 0), \
					(select count(*) from pg_constraint where conrelid = '{t}'::regclass and conname = 'lepis_owns') \
					from {t} lepis_row",
					t = qualified(rel)
				))
				.await?;
			let r = rows.first().cloned().unwrap_or_default();
			let get = |i: usize| r.get(i).cloned().flatten().unwrap_or_default();
			let sharded = matches!(kind, RelationKind::Sharded { .. });
			// A chunked table is fenced when its shell refuses rows and every chunk it reads
			// carries its own fence.
			let fenced = if c.chunked.contains(*rel) {
				pg.query(
					"select (select count(*) from pg_constraint where conrelid = $1::regclass and conname = $2) = 1 					and not exists (select from pg_inherits i where i.inhparent = $1::regclass 					and not exists (select from pg_constraint k where k.conrelid = i.inhrelid and k.conname = 'lepis_owns'))",
					&[&qualified(rel), crate::chunk::SHELL_CONSTRAINT],
				)
				.await?
				.first()
				.and_then(|r| r.first().cloned().flatten())
				.as_deref()
					== Some("t")
			} else {
				get(3) != "0"
			};
			if sharded && !fenced {
				ok = false;
			}
			// A chunked table: each chunk it reads, its rows and checksum, and whether the node
			// owns it.
			let mut chunks = Vec::new();
			if let (Some(all), RelationKind::Sharded { keyspace: k, .. }) = (c.chunks_of(rel), kind)
			{
				let ks = &c.keyspaces[k];
				let attached = pg
					.query(
						"select k.relname::text from pg_inherits i join pg_class k on k.oid = i.inhrelid \
						where i.inhparent = $1::regclass order by 1",
						&[&qualified(rel)],
					)
					.await?;
				for a in attached {
					let name = a.first().cloned().flatten().unwrap_or_default();
					let number = name
						.strip_prefix(&format!("{}_c", rel.table))
						.and_then(|d| d.parse::<u32>().ok());
					let owned = number
						.and_then(|n| all.iter().find(|x| x.number == n))
						.is_some_and(|x| ks.owner_of_hash(x.lo) == n.id);
					if !owned && !whole_copy {
						ok = false;
					}
					let r = pg
						.simple(&format!(
							"select count(*), coalesce(sum(hashtextextended(lepis_row::text, 0)::numeric), 0) \
							from {}.{} lepis_row",
							quote_ident(&rel.schema),
							quote_ident(&name)
						))
						.await?;
					let cell = |i: usize| {
						r.first()
							.and_then(|x| x.get(i).cloned().flatten())
							.unwrap_or_default()
					};
					chunks.push(
						json!({"chunk": number, "table": name, "owned": owned, "rows": cell(0), "checksum": cell(1)}),
					);
				}
			}
			if matches!(kind, RelationKind::Reference) {
				match reference.iter_mut().find(|(r, _)| r == *rel) {
					Some((_, v)) => v.push((get(0), get(2))),
					None => reference.push(((*rel).clone(), vec![(get(0), get(2))])),
				}
			}
			report.push(json!({
				"node": n.id.0, "table": rel.to_string(), "kind": if sharded { "sharded" } else { "reference" },
				"owned_rows": get(0), "foreign_rows": get(1), "checksum": get(2), "fenced": fenced,
				"chunks": chunks,
			}));
		}
	}
	let mut mismatched = Vec::new();
	for (rel, copies) in &reference {
		if copies.windows(2).any(|w| w[0] != w[1]) {
			ok = false;
			mismatched.push(rel.to_string());
		}
	}
	cx.save(
		h,
		json!({"ok": ok, "tables": report, "reference_mismatch": mismatched}),
	)
	.await?;
	if ok {
		Ok(())
	} else {
		Err(OpError::refused(
			Kind::VerifyFailed,
			format!(
				"verify found a problem (a missing fence, or reference copies that differ: {})",
				mismatched.join(", ")
			),
		))
	}
}

/// L13's cleanup: deletes the rows a node holds but does not own, in batches, then VACUUM.
/// Refused while a move's copy has not been verified: those rows may be the only good copy.
async fn cleanup(cx: &mut StepCtx, h: &mut Pg, node: Option<NodeId>) -> Result<(), OpError> {
	let unverified = h
		.simple(
			"select string_agg(distinct job_id::text, ', ') from lepis.job_step \
			where kind = 'transfer' and detail->>'phase' in ('cut', 'sealed', 'verify_failed')",
		)
		.await?
		.first()
		.and_then(|r| r.first().cloned().flatten());
	if let Some(jobs) = unverified {
		return Err(OpError::refused(
			Kind::UnverifiedMoves,
			format!(
				"job {jobs} moved rows that were never verified; resume or inspect it before cleaning up"
			),
		));
	}
	let c = load_catalog(h).await?;
	let mut deleted = serde_json::Map::new();
	for n in c
		.nodes
		.values()
		.filter(|n| n.state != NodeState::Removed && node.is_none_or(|x| x == n.id))
	{
		let mut pg = connect_node(&cx.app, &c, n.id).await?;
		// A standby owns nothing and holds its source's rows by replay: nothing to delete there.
		if in_recovery(&mut pg).await? {
			continue;
		}
		let v = version(&mut pg).await?;
		let mut sharded: Vec<&RelationName> = c
			.relations
			.iter()
			.filter(|(_, k)| matches!(k, RelationKind::Sharded { .. }))
			.map(|(r, _)| r)
			.collect();
		sharded.sort();
		let mut items = Vec::new();
		let mut chunked = Vec::new();
		for rel in sharded {
			if c.chunked.contains(rel) {
				chunked.push(rel.clone());
			} else if schema::exists(&mut pg, rel).await? {
				let owns = owns_expr(&c, rel, n.id, v, &|col| quote_ident(col))?;
				items.push((rel.clone(), format!("not ({owns})")));
			}
		}
		// A chunked table gives rows away as whole retired chunk tables: dropped, no DELETE.
		if !chunked.is_empty() {
			let gone = pg
				.simple(&crate::chunk::drop_retired_sql(&chunked))
				.await?
				.first()
				.and_then(|r| r.first().cloned().flatten())
				.unwrap_or_default();
			deleted.insert(format!("{}:retired chunk tables", n.name), json!(gone));
		}
		for (rel, count) in delete_ordered(&mut pg, items).await? {
			if count > 0 {
				pg.simple(&format!("vacuum {}", qualified(&rel))).await?;
			}
			deleted.insert(format!("{}:{rel}", n.name), json!(count));
		}
	}
	cx.save(h, json!({"deleted": deleted})).await
}

/// Deletes the rows of `rel` matching `filter`, `CLEANUP_BATCH` at a time.
pub async fn delete_batched(pg: &mut Pg, rel: &RelationName, filter: &str) -> Result<u64, OpError> {
	let t = qualified(rel);
	let mut total = 0u64;
	loop {
		let n: u64 = pg
			.simple(&format!(
				"with d as (delete from {t} where ctid = any (array (select ctid from {t} where {filter} limit {CLEANUP_BATCH})) returning 1) \
				select count(*) from d"
			))
			.await?
			.first()
			.and_then(|r| r.first().cloned().flatten())
			.and_then(|v| v.parse().ok())
			.unwrap_or(0);
		total += n;
		if n < CLEANUP_BATCH as u64 {
			return Ok(total);
		}
	}
}

/// For status: a node's state name.
pub fn node_json(c: &Catalog) -> Vec<Value> {
	c.nodes
		.values()
		.map(|n| {
			let ranges: Vec<Value> = c
				.keyspaces
				.values()
				.flat_map(|k| {
					k.ranges.iter().filter(|r| r.node == n.id).map(
						move |r| json!({"keyspace": k.name, "lo": r.lo.to_string(), "hi": r.hi.to_string()}),
					)
				})
				.collect();
			json!({
				"id": n.id.0, "name": n.name, "host": n.host, "port": n.port, "dbname": n.dbname,
				"home": n.home, "state": state_name(n.state), "server_version_num": n.server_version_num,
				"ranges": ranges,
			})
		})
		.collect()
}

/// Deletes from several tables, retrying a table whose delete a foreign key refused (`23503`)
/// after the others: tables colocated by a key reference each other (L9), and children go first.
pub async fn delete_ordered(
	pg: &mut Pg,
	items: Vec<(RelationName, String)>,
) -> Result<Vec<(RelationName, u64)>, OpError> {
	let mut pending = items;
	let mut done = Vec::new();
	while !pending.is_empty() {
		let before = pending.len();
		let mut left = Vec::new();
		let mut last = None;
		for (rel, filter) in pending {
			match delete_batched(pg, &rel, &filter).await {
				Ok(n) => done.push((rel, n)),
				Err(e) if e.is("23503") => {
					last = Some(e);
					left.push((rel, filter));
				}
				Err(e) => return Err(e),
			}
		}
		if left.len() == before {
			// No table could go: a key from outside these tables holds them.
			return Err(last.unwrap_or_else(|| OpError::new("no progress deleting rows")));
		}
		pending = left;
	}
	Ok(done)
}
