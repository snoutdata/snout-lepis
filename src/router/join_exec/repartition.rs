//! A repartition: two large tables joined on columns that are
//! not their distribution keys. Both are re-hashed on the join columns over the nodes that hold
//! them, so every pair of rows that can join lands on one node; each node joins its share and
//! the router merges, as for a scatter (the copies are sharded tables of a keyspace of the plan's
//! own, `join::spread_keyspace`). Everything else the read uses is broadcast.
//!
//! - **Through the router**, under `lepis.join_repartition_bytes` (the two tables' size on disk):
//!   each table is fetched and its rows sent, by their join column's hash, into a temporary
//!   table on the node that hash names.
//! - **Node to node** (4b), past that cap and under `lepis.join_shuffle_rows`: each node that
//!   holds rows of a table sends them straight to the node that will join them, by `postgres_fdw`
//!   (contrib, in every Postgres image, nothing more to run). The rows are read in the client's
//!   own session on the source node, `INSERT INTO <foreign table> SELECT … FROM <table>`, so its
//!   grants and row security decide what is read. The foreign table writes, as `lepis_shuffle`,
//!   into an UNLOGGED staging table on the receiving node: a login Lepis manages (never a
//!   superuser, no inherited rights) that may only INSERT into a shuffle's staging tables. The
//!   staging and foreign tables are made and dropped by the service login, the staging tables
//!   readable by the client's role only; dropped when the read ends, answered, refused or failed,
//!   and any left behind by a router that died are swept an hour later by the next shuffle.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::Instant;

use super::super::{Answer, Message, Router, WireError, qualified_name};
use super::{Fetched, error_text, reference};
use crate::catalog::{
	Keyspace, NodeId, RelationKind, RelationName, owns_check, quote_ident, quote_literal,
};
use crate::join::{self, Caps};
use crate::ops::{Pg, Target};
use crate::route;

/// The name of a repartition's keyspace in its plan's catalog.
const SPREAD: &str = "lepis_join_spread";

/// The login a node uses to write into another node's staging table.
const SHUFFLE_ROLE: &str = "lepis_shuffle";

/// Nodes (by address) this process has set up for shuffles, with the password it set.
static READY: Mutex<Option<HashSet<String>>> = Mutex::new(None);

/// One table's place in a repartition.
struct Side {
	rel: RelationName,
	column: String,
	/// Its columns: name, type as `format_type` gives it.
	columns: Vec<(String, String)>,
	/// The join column's type.
	oid: u32,
	/// Where its rows are now.
	sources: Vec<NodeId>,
}

impl Router {
	/// Some(answer or refusal) when the read is a repartition's to answer, None to go on.
	#[allow(clippy::too_many_arguments)]
	pub(super) async fn repartition(
		&mut self,
		sql: &str,
		batch: &[Message],
		reads: &join::Reads,
		est: &HashMap<RelationName, f64>,
		sizes: &HashMap<RelationName, f64>,
		caps: &Caps,
		made: &mut Vec<(NodeId, RelationName)>,
		next: &mut usize,
		deadline: Instant,
	) -> Result<Option<Option<route::Refusal>>, WireError> {
		if caps.repartition_bytes == 0 && caps.shuffle_rows == 0 {
			return Ok(None);
		}
		// The two largest groups, each one table: the two that move.
		let size = |g: &[RelationName]| {
			g.iter()
				.map(|r| est.get(r).copied().unwrap_or(0.0))
				.sum::<f64>()
		};
		let mut order: Vec<usize> = (0..reads.groups.len()).collect();
		order.sort_by(|x, y| size(&reads.groups[*y]).total_cmp(&size(&reads.groups[*x])));
		let [ga, gb, ..] = order.as_slice() else {
			return Ok(None);
		};
		let ([a], [b]) = (reads.groups[*ga].as_slice(), reads.groups[*gb].as_slice()) else {
			return Ok(None);
		};
		let (a, b) = (a.clone(), b.clone());
		let catalog = self.catalog.clone();
		let path = self.search_path.clone();
		let resolve = |sch: &str, t: &str| crate::analyze::resolve_name(&catalog, &path, sch, t);
		let Some((ca, cb)) = join::repartition_keys(sql, &a, &b, &resolve) else {
			return Ok(None);
		};
		// Everything else is broadcast, under its own cap.
		let others: Vec<RelationName> = order[2..]
			.iter()
			.flat_map(|g| reads.groups[*g].iter().cloned())
			.chain(reads.globals.iter().cloned())
			.collect();
		let small: f64 = others
			.iter()
			.map(|r| est.get(r).copied().unwrap_or(0.0))
			.sum();
		if !others.is_empty()
			&& (caps.broadcast_rows == 0 || small > caps.broadcast_rows as f64 * 2.0)
		{
			return Ok(None);
		}
		let mut sides = Vec::new();
		for (rel, column) in [(a, ca), (b, cb)] {
			let Some(RelationKind::Sharded { keyspace, .. }) = catalog.relations.get(&rel) else {
				return Ok(None);
			};
			let sources = catalog.nodes_of(keyspace);
			let Some(described) = self.columns_of(&rel, &sources).await? else {
				return Ok(None);
			};
			let Some(oid) = described.iter().find(|c| c.0 == column).map(|c| c.2) else {
				return Ok(None);
			};
			sides.push(Side {
				rel,
				column,
				columns: described.into_iter().map(|(n, t, _)| (n, t)).collect(),
				oid,
				sources,
			});
		}
		let Some(key_type) = join::join_key_type(sides[0].oid, sides[1].oid) else {
			return Ok(None);
		};
		let mut targets: Vec<NodeId> = sides
			.iter()
			.flat_map(|s| s.sources.iter().copied())
			.collect();
		targets.sort();
		targets.dedup();
		let spread = join::spread_keyspace(SPREAD, key_type, &targets);
		let bytes: f64 = sides
			.iter()
			.map(|s| sizes.get(&s.rel).copied().unwrap_or(0.0))
			.sum();
		let rows: f64 = sides
			.iter()
			.map(|s| est.get(&s.rel).copied().unwrap_or(0.0))
			.sum();
		let through_router = caps.repartition_bytes > 0 && bytes <= caps.repartition_bytes as f64;
		let node_to_node =
			!through_router && caps.shuffle_rows > 0 && rows <= caps.shuffle_rows as f64;
		if !through_router && !node_to_node {
			return Ok(Some(Some(if caps.shuffle_rows > 0 {
				join::over(
					"more rows moved between nodes",
					"join_shuffle_rows",
					rows as u64,
					caps.shuffle_rows,
				)
			} else {
				join::over(
					"more bytes through the router",
					"join_repartition_bytes",
					bytes as u64,
					caps.repartition_bytes,
				)
			})));
		}

		// The small tables, to every node that joins.
		let mut temps: HashMap<RelationName, RelationName> = HashMap::new();
		let mut copies: Vec<(RelationName, Fetched)> = Vec::new();
		let mut small_rows = 0u64;
		for r in &others {
			match self.fetch(r).await? {
				Ok(f) => {
					small_rows += f.rows.len() as u64;
					copies.push((r.clone(), f));
				}
				Err(e) => return Ok(Some(Some(could_not_read(r, &e)))),
			}
			if small_rows > caps.broadcast_rows {
				return Ok(None);
			}
		}
		for (r, f) in &copies {
			*next += 1;
			let t = join::temp_name(*next);
			for n in &targets {
				if let Some(e) = self.put(*n, &t, f, made).await? {
					return Ok(Some(Some(e)));
				}
			}
			temps.insert(r.clone(), t);
		}
		let mut kinds = reference(&temps);

		let mut shuffle = None;
		if through_router {
			for s in &sides {
				let f = match self.fetch(&s.rel).await? {
					Ok(f) => f,
					Err(e) => return Ok(Some(Some(could_not_read(&s.rel, &e)))),
				};
				if f.bytes > caps.repartition_bytes.saturating_mul(2) {
					return Ok(Some(Some(join::over(
						"more bytes through the router",
						"join_repartition_bytes",
						f.bytes,
						caps.repartition_bytes,
					))));
				}
				let Some(at) = f.fields.iter().position(|x| x.name == s.column) else {
					return Ok(None);
				};
				let mut parts: HashMap<NodeId, Vec<crate::merge::Row>> =
					targets.iter().map(|n| (*n, Vec::new())).collect();
				for row in f.rows {
					let v = row[at]
						.as_deref()
						.map(|v| String::from_utf8_lossy(v).into_owned());
					let Ok(n) = join::spread_node(&spread, v.as_deref()) else {
						return Ok(None);
					};
					parts.get_mut(&n).expect("a target").push(row);
				}
				*next += 1;
				let t = join::temp_name(*next);
				for n in &targets {
					let part = Fetched {
						fields: f.fields.clone(),
						rows: parts.remove(n).unwrap_or_default(),
						bytes: 0,
					};
					if let Some(e) = self.put(*n, &t, &part, made).await? {
						return Ok(Some(Some(e)));
					}
				}
				kinds.insert(
					t.clone(),
					RelationKind::Sharded {
						keyspace: SPREAD.into(),
						key_column: s.column.clone(),
					},
				);
				temps.insert(s.rel.clone(), t);
			}
		} else {
			let mut sh = Shuffle::new();
			let r = self
				.shuffle(&mut sh, &sides, &spread, &targets, &mut temps, &mut kinds)
				.await;
			shuffle = Some(sh);
			match r {
				Ok(None) => {}
				Ok(Some(refusal)) => {
					self.unshuffle(shuffle.take().expect("made")).await;
					return Ok(Some(Some(refusal)));
				}
				Err(e) => {
					self.unshuffle(shuffle.take().expect("made")).await;
					return Err(e);
				}
			}
		}
		if Instant::now() > deadline {
			if let Some(sh) = shuffle {
				self.unshuffle(sh).await;
			}
			return Ok(Some(Some(join::over(
				"more time than a join may take",
				"join_timeout_ms",
				caps.timeout_ms,
				caps.timeout_ms,
			))));
		}
		let r = self
			.run_rewritten(sql, batch, &temps, &kinds, Some(&spread), false)
			.await;
		if let Some(sh) = shuffle {
			self.unshuffle(sh).await;
		}
		r
	}

	/// The columns of `rel` (name, type, type oid), as the first of `nodes` has it; None if it
	/// has none.
	async fn columns_of(
		&mut self,
		rel: &RelationName,
		nodes: &[NodeId],
	) -> Result<Option<Vec<(String, String, u32)>>, WireError> {
		let Some(n) = nodes.first().copied() else {
			return Ok(None);
		};
		self.ensure_conn(n).await?;
		let q = format!(
			"select a.attname::text, format_type(a.atttypid, a.atttypmod), a.atttypid::text \
			from pg_attribute a where a.attrelid = to_regclass({}) and a.attnum > 0 \
			and not a.attisdropped order by a.attnum",
			quote_literal(&qualified_name(rel))
		);
		let answers = match self.run_worker(&[n], &q, &[], None, &[]).await? {
			Ok(a) => a,
			Err(_) => return Ok(None),
		};
		let text = |c: &Option<Vec<u8>>| {
			c.as_deref()
				.map(|v| String::from_utf8_lossy(v).into_owned())
				.unwrap_or_default()
		};
		let mut out = Vec::new();
		for a in &answers {
			for r in &a.rows {
				out.push((text(&r[0]), text(&r[1]), text(&r[2]).parse().unwrap_or(0)));
			}
		}
		Ok((!out.is_empty()).then_some(out))
	}

	/// The node-to-node shuffle of both sides into staging tables on `targets`. Some(refusal)
	/// when it could not; what it made is in `sh` either way.
	async fn shuffle(
		&mut self,
		sh: &mut Shuffle,
		sides: &[Side],
		spread: &Keyspace,
		targets: &[NodeId],
		temps: &mut HashMap<RelationName, RelationName>,
		kinds: &mut HashMap<RelationName, RelationKind>,
	) -> Result<Option<route::Refusal>, WireError> {
		let refuse = |why: String| {
			route::Refusal {
			code: route::NOT_ACROSS_NODES,
			message: format!("a join could not move rows between nodes: {why}"),
			hint: "Run the query again, or set lepis.join_shuffle_rows = 0 to keep the join on the router (under lepis.join_repartition_bytes).".into(),
		}
		};
		let catalog = self.catalog.clone();
		let mut nodes: Vec<NodeId> = targets.to_vec();
		for s in sides {
			nodes.extend(s.sources.iter().copied());
		}
		nodes.sort();
		nodes.dedup();
		// The client's role, as its statements run.
		let first = nodes[0];
		self.ensure_conn(first).await?;
		self.catch_up_sets(first).await?;
		let role = match self
			.run_worker(&[first], "select current_user::text", &[], None, &[])
			.await?
		{
			Ok(a) => a
				.first()
				.and_then(|a| a.rows.first())
				.and_then(|r| r[0].clone())
				.map(|v| String::from_utf8_lossy(&v).into_owned())
				.unwrap_or_default(),
			Err(e) => return Ok(Some(refuse(error_text(&e)))),
		};
		let password = shuffle_password(&self.app.config.service.password);
		for n in &nodes {
			let Some(node) = catalog.nodes.get(n) else {
				return Ok(Some(refuse(format!("{n} is not in the catalog"))));
			};
			let mut pg = match Pg::connect(&self.app, &Target::of(&self.app, node)).await {
				Ok(pg) => pg,
				Err(e) => return Ok(Some(refuse(e.to_string()))),
			};
			let peers: Vec<&crate::catalog::Node> = nodes
				.iter()
				.filter(|x| *x != n)
				.filter_map(|x| catalog.nodes.get(x))
				.collect();
			// Keyed by the peers as well: a node added, or one that moved, sets up again.
			let key = std::iter::once(node)
				.chain(peers.iter().copied())
				.map(|x| format!("{}={}:{}/{}", x.id.0, x.host, x.port, x.dbname))
				.collect::<Vec<_>>()
				.join(",");
			let ready = READY
				.lock()
				.unwrap_or_else(|e| e.into_inner())
				.get_or_insert_with(HashSet::new)
				.contains(&key);
			if !ready {
				if let Err(e) = pg.simple(&setup_sql(&password, &peers)).await {
					pg.close().await;
					return Ok(Some(refuse(format!(
						"{} could not be set up for it: {e}",
						node.name
					))));
				}
				READY
					.lock()
					.unwrap_or_else(|e| e.into_inner())
					.get_or_insert_with(HashSet::new)
					.insert(key);
			}
			// What a crashed router or a dropped query left, an hour on.
			let _ = pg.simple(&sweep_sql()).await;
			sh.pgs.insert(*n, pg);
		}
		let set = format!(
			"select pg_catalog.set_config('lepis.shuffle', {}, false)",
			quote_literal(&sh.token)
		);
		for n in &nodes {
			self.ensure_conn(*n).await?;
			self.catch_up_sets(*n).await?;
		}
		if let Err(e) = self
			.run_each(&nodes, &vec![set; nodes.len()], &[], None, &[])
			.await?
		{
			return Ok(Some(refuse(error_text(&e))));
		}
		let digest = sha256_hex(&sh.token);
		for (k, s) in sides.iter().enumerate() {
			let staging = format!("{}_{}", sh.id, k + 1);
			let cols: Vec<String> = s
				.columns
				.iter()
				.map(|(n, t)| format!("{} {t}", quote_ident(n)))
				.collect();
			let names: Vec<String> = s.columns.iter().map(|(n, _)| quote_ident(n)).collect();
			let st = format!("{}.{}", join::SHUFFLE_SCHEMA, quote_ident(&staging));
			for t in targets {
				let sql = format!(
					"create unlogged table {st} ({}) with (autovacuum_enabled = false); \
					alter table {st} enable row level security; alter table {st} force row level security; \
					create policy own on {st} to {r} using ({check}) with check ({check}); \
					create policy fill on {st} for insert to {SHUFFLE_ROLE} with check (true); \
					grant select, insert on {st} to {r}; grant insert on {st} to {SHUFFLE_ROLE}",
					cols.join(", "),
					r = quote_ident(&role),
					check = token_check(&digest),
				);
				sh.staging.push((*t, staging.clone()));
				if let Err(e) = sh.pg(*t).simple(&sql).await {
					return Ok(Some(refuse(e.to_string())));
				}
			}
			for src in &s.sources {
				for t in targets.iter().filter(|t| *t != src) {
					let ft = format!("{staging}_n{}", t.0);
					let f = format!("{}.{}", join::SHUFFLE_SCHEMA, quote_ident(&ft));
					let sql = format!(
						"create foreign table {f} ({}) server {} options (schema_name {}, table_name {}, batch_size '1000'); \
						grant insert on {f} to {}",
						cols.join(", "),
						quote_ident(&server_name(*t)),
						quote_literal(join::SHUFFLE_SCHEMA),
						quote_literal(&staging),
						quote_ident(&role)
					);
					sh.foreign.push((*src, ft));
					if let Err(e) = sh.pg(*src).simple(&sql).await {
						return Ok(Some(refuse(e.to_string())));
					}
				}
			}
			// Each source reads its rows once, in one statement (one snapshot), in the client's own
			// session, and writes every target's share.
			let RelationKind::Sharded {
				keyspace,
				key_column,
			} = catalog.relations[&s.rel].clone()
			else {
				return Ok(Some(refuse(format!("{} is not distributed", s.rel))));
			};
			let chunked = catalog.chunked.contains(&s.rel);
			let column = format!("lepis_row.{}", quote_ident(&s.column));
			let mut sqls = Vec::new();
			for src in &s.sources {
				let version = catalog
					.nodes
					.get(src)
					.and_then(|x| x.server_version_num)
					.unwrap_or(180_000);
				let owns = if chunked {
					"true".to_string()
				} else {
					owns_check(
						format!("lepis_row.{}", quote_ident(&key_column)),
						&catalog.keyspaces[&keyspace],
						*src,
						version,
					)
					.unwrap_or_else(|_| "false".into())
				};
				let sel = names
					.iter()
					.map(|n| format!("lepis_row.{n}"))
					.collect::<Vec<_>>()
					.join(", ");
				let mut parts = vec![format!(
					"lepis_rows as materialized (select {sel} from {} lepis_row where {owns})",
					qualified_name(&s.rel)
				)];
				for (i, t) in targets.iter().enumerate() {
					let dest = if src == t {
						st.clone()
					} else {
						format!(
							"{}.{}",
							join::SHUFFLE_SCHEMA,
							quote_ident(&format!("{staging}_n{}", t.0))
						)
					};
					let mut filter = owns_check(&column, spread, *t, version)
						.map_err(|e| e.0)
						.unwrap_or_else(|_| "false".into());
					if *t == spread.ranges[0].node {
						filter = format!("({filter}) or {column} is null");
					}
					parts.push(format!(
						"lepis_to{i} as (insert into {dest} ({cols}) select {sel} from lepis_rows lepis_row where {filter})",
						cols = names.join(", "),
					));
				}
				sqls.push(format!("with {} select 1", parts.join(", ")));
			}
			if let Err(e) = self.run_each(&s.sources, &sqls, &[], None, &[]).await? {
				return Ok(Some(could_not_read(&s.rel, &e)));
			}
			for t in targets {
				let _ = sh.pg(*t).simple(&format!("analyze {st}")).await;
			}
			let rel = RelationName {
				schema: join::SHUFFLE_SCHEMA.into(),
				table: staging,
			};
			kinds.insert(
				rel.clone(),
				RelationKind::Sharded {
					keyspace: spread.name.clone(),
					key_column: s.column.clone(),
				},
			);
			temps.insert(s.rel.clone(), rel);
		}
		Ok(None)
	}

	/// Drops what a shuffle made, on the service login's sessions, and closes them.
	async fn unshuffle(&mut self, mut sh: Shuffle) {
		let mut by: HashMap<NodeId, Vec<String>> = HashMap::new();
		for (n, f) in &sh.foreign {
			by.entry(*n).or_default().push(format!(
				"drop foreign table if exists {}.{}",
				join::SHUFFLE_SCHEMA,
				quote_ident(f)
			));
		}
		for (n, t) in &sh.staging {
			by.entry(*n).or_default().push(format!(
				"drop table if exists {}.{}",
				join::SHUFFLE_SCHEMA,
				quote_ident(t)
			));
		}
		for (n, stmts) in by {
			if let Some(pg) = sh.pgs.get_mut(&n) {
				let _ = pg.simple(&stmts.join("; ")).await;
			}
		}
		for (_, pg) in sh.pgs.drain() {
			pg.close().await;
		}
	}
}

/// What one shuffle made, to be dropped.
struct Shuffle {
	/// `j<ms>r<random>`: when it was made, for the sweep, and its own.
	id: String,
	/// Set on the client's sessions; the staging tables' policy reads only with it.
	token: String,
	pgs: HashMap<NodeId, Pg>,
	staging: Vec<(NodeId, String)>,
	foreign: Vec<(NodeId, String)>,
}

impl Shuffle {
	fn new() -> Shuffle {
		let ms = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map(|d| d.as_millis() as u64)
			.unwrap_or(0);
		let mut r = [0u8; 4];
		let _ = getrandom::fill(&mut r);
		let mut t = [0u8; 16];
		let _ = getrandom::fill(&mut t);
		Shuffle {
			token: t.iter().map(|b| format!("{b:02x}")).collect(),
			id: format!("j{ms:x}r{:08x}", u32::from_le_bytes(r)),
			pgs: HashMap::new(),
			staging: Vec::new(),
			foreign: Vec::new(),
		}
	}

	fn pg(&mut self, n: NodeId) -> &mut Pg {
		self.pgs.get_mut(&n).expect("connected for the shuffle")
	}
}

fn could_not_read(r: &RelationName, e: &Message) -> route::Refusal {
	route::Refusal {
		code: route::NOT_ACROSS_NODES,
		message: format!("a join could not read {r}: {}", error_text(e)),
		hint: "Run the query again.".into(),
	}
}

/// The foreign server on every node that names node `n`.
fn server_name(n: NodeId) -> String {
	format!("lepis_shuffle_n{}", n.0)
}

/// `lepis_shuffle`'s password: derived from the service login's, so every router sets the same
/// one and nothing more is stored.
pub fn shuffle_password(service_password: &str) -> String {
	use hmac::{Hmac, Mac};
	let mut m = <Hmac<sha2::Sha256> as Mac>::new_from_slice(service_password.as_bytes())
		.expect("any key length");
	m.update(b"lepis_shuffle");
	m.finalize()
		.into_bytes()
		.iter()
		.map(|b| format!("{b:02x}"))
		.collect()
}

/// What a node needs for shuffles, made once per router process (each statement idempotent and
/// safe against another router doing the same): `postgres_fdw`, the `lepis_shuffle` login, its
/// schema, and a foreign server and a mapping for each peer.
pub fn setup_sql(password: &str, peers: &[&crate::catalog::Node]) -> String {
	let schema = join::SHUFFLE_SCHEMA;
	let mut s = format!(
		"do $lepis$ begin \
		begin create extension if not exists postgres_fdw; exception when unique_violation or duplicate_object then null; end; \
		begin create role {SHUFFLE_ROLE} login nosuperuser noinherit nocreatedb nocreaterole noreplication nobypassrls; \
		exception when duplicate_object or unique_violation then null; end; \
		begin create schema if not exists {schema}; exception when unique_violation or duplicate_object then null; end; \
		end $lepis$; \
		alter role {SHUFFLE_ROLE} with login nosuperuser noinherit password {pw}; \
		revoke all on schema {schema} from public; \
		grant usage on schema {schema} to public; ",
		pw = quote_literal(password)
	);
	for p in peers {
		let srv = quote_ident(&server_name(p.id));
		let opts = |verb: &str| {
			format!(
				"{verb} host {}, {verb} port {}, {verb} dbname {}, {verb} sslmode {}",
				quote_literal(&p.host),
				quote_literal(&p.port.to_string()),
				quote_literal(&p.dbname),
				quote_literal(p.sslmode.as_str())
			)
		};
		let add = format!(
			"host {}, port {}, dbname {}, sslmode {}",
			quote_literal(&p.host),
			quote_literal(&p.port.to_string()),
			quote_literal(&p.dbname),
			quote_literal(p.sslmode.as_str())
		);
		s.push_str(&format!(
			"do $lepis$ begin \
			if not exists (select 1 from pg_foreign_server where srvname = {name}) then \
			begin create server {srv} foreign data wrapper postgres_fdw options ({add}); exception when duplicate_object then null; end; \
			end if; \
			execute {set}; \
			if not exists (select 1 from pg_user_mappings where srvname = {name} and usename = 'public') then \
			begin create user mapping for public server {srv} options (user {role}, password {pw}); exception when duplicate_object then null; end; \
			end if; \
			execute {map}; \
			end $lepis$; ",
			name = quote_literal(&server_name(p.id)),
			set = quote_literal(&format!("alter server {srv} options ({})", opts("set"))),
			map = quote_literal(&format!(
				"alter user mapping for public server {srv} options (set user {}, set password {})",
				quote_literal(SHUFFLE_ROLE),
				quote_literal(password)
			)),
			role = quote_literal(SHUFFLE_ROLE),
			pw = quote_literal(password),
		));
	}
	s
}

/// Drops a shuffle's leftovers an hour old: run by every shuffle.
fn sweep_sql() -> String {
	let schema = join::SHUFFLE_SCHEMA;
	format!(
		"do $lepis$ declare r record; begin \
		for r in select c.relname, c.relkind from pg_class c join pg_namespace n on n.oid = c.relnamespace \
		where n.nspname = '{schema}' and c.relkind in ('r', 'f') and c.relname ~ '^j[0-9a-f]+r' loop \
		if ('x' || lpad(substring(r.relname from '^j([0-9a-f]+)r'), 16, '0'))::bit(64)::bigint \
		< (extract(epoch from now()) * 1000)::bigint - 3600000 then \
		if r.relkind = 'f' then execute format('drop foreign table if exists {schema}.%I', r.relname); \
		else execute format('drop table if exists {schema}.%I', r.relname); end if; \
		end if; end loop; end $lepis$"
	)
}

/// The staging tables' policy: the session's `lepis.shuffle` hashes to the shuffle's digest,
/// evaluated once per scan. The token itself is never stored on a node.
fn token_check(digest: &str) -> String {
	format!(
		"(select encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(pg_catalog.current_setting('lepis.shuffle', true), ''), 'UTF8')), 'hex')) = {}",
		quote_literal(digest)
	)
}

fn sha256_hex(s: &str) -> String {
	use sha2::Digest;
	sha2::Sha256::digest(s.as_bytes())
		.iter()
		.map(|b| format!("{b:02x}"))
		.collect()
}

#[allow(dead_code)]
fn _uses(_: Answer) {}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn setup_makes_the_login_and_the_servers() {
		let peer = crate::catalog::Node {
			id: NodeId(3),
			name: "n3".into(),
			host: "10.0.0.3".into(),
			port: 5432,
			dbname: "app".into(),
			sslmode: crate::config::SslMode::Disable,
			home: false,
			state: crate::catalog::NodeState::Active,
			server_version_num: Some(180_000),
		};
		let s = setup_sql("pw", &[&peer]);
		assert!(s.contains("create role lepis_shuffle login nosuperuser noinherit"));
		assert!(s.contains(
			"create server \"lepis_shuffle_n3\" foreign data wrapper postgres_fdw options (host '10.0.0.3', port '5432', dbname 'app', sslmode 'disable')"
		));
		assert!(s.contains("create user mapping for public server \"lepis_shuffle_n3\" options (user 'lepis_shuffle', password 'pw')"));
		assert!(sweep_sql().contains("drop foreign table if exists lepis_shuffle.%I"));
		assert!(token_check(&sha256_hex("t")).contains(&sha256_hex("t")));
		assert_eq!(shuffle_password("a"), shuffle_password("a"));
		assert_ne!(shuffle_password("a"), shuffle_password("b"));
		assert_eq!(shuffle_password("a").len(), 64);
	}
}
