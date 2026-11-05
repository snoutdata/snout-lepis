//! The router's half of join.rs: fetching the rows a join needs, copying them where they are
//! needed as temporary tables, running the rewritten read through the paths the router already
//! has (a scatter with the copies as reference tables, or the home node alone), and dropping the
//! copies whatever happened.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::{Answer, Field, Message, Outcome, Part, Router, cstr, put_message, refusal_message};
use crate::catalog::{Keyspace, NodeId, RelationKind, RelationName, quote_ident};
use crate::join::{self, Caps, Slot};
use crate::merge::Row;
use crate::route::{self, Route};
use crate::wire::{self, WireError};

mod repartition;

/// One relation's rows as fetched, with what they look like.
pub(super) struct Fetched {
	fields: Vec<Field>,
	rows: Vec<Row>,
	bytes: u64,
}

impl Router {
	/// The SQL of a batch that is one SELECT a join may answer instead of `r`, outside a
	/// transaction; None otherwise.
	pub(super) fn joinable(&self, batch: &[Message], r: &route::Refusal) -> Option<String> {
		if self.in_join || self.in_tx() || !join::answerable(r) {
			return None;
		}
		let stmts = self.batch_statements(batch)?;
		let [sql] = stmts.as_slice() else {
			return None;
		};
		match &*self.analyze(sql) {
			Ok(v)
				if v.len() == 1
					&& v[0].facts.kind == route::Kind::Select
					&& !v[0].facts.tables.iter().any(|t| t.written)
					&& v[0].tx.is_none()
					&& v[0].session.is_none() =>
			{
				Some(sql.clone())
			}
			_ => None,
		}
	}

	pub(super) async fn answer_join(
		&mut self,
		sql: String,
		batch: Vec<Message>,
		why: route::Refusal,
	) -> Result<(), WireError> {
		let simple = batch.first().map(|m| m.tag) == Some(b'Q');
		let caps = Caps::read(&self.settings.join, &join::session_caps(&self.sets));
		let Some(_slot) = Slot::take(&caps) else {
			let r = join::over(
				"a join slot on this router",
				"join_concurrency",
				caps.concurrency,
				caps.concurrency,
			);
			self.refuse_answer(&batch, simple, r);
			return Ok(());
		};
		let mut made: Vec<(NodeId, RelationName)> = Vec::new();
		let r = self.join_inner(&sql, &batch, &why, &caps, &mut made).await;
		// The copies go, whatever happened (a lost node took its own with its session).
		let mut by_node: HashMap<NodeId, Vec<String>> = HashMap::new();
		for (n, t) in &made {
			by_node.entry(*n).or_default().push(format!(
				"{}.{}",
				quote_ident(&t.schema),
				quote_ident(&t.table)
			));
		}
		for (n, tables) in by_node {
			if let Some(conn) = self.conns.get_mut(&n) {
				let _ = Part { node: n, conn }
					.execute_sql(&format!("drop table if exists {}", tables.join(", ")))
					.await;
			}
		}
		match r {
			Ok(None) => Ok(()),
			Ok(Some(refusal)) => {
				self.refuse_answer(&batch, simple, refusal);
				Ok(())
			}
			Err(e) => Err(e),
		}
	}

	fn refuse_answer(&mut self, batch: &[Message], simple: bool, r: route::Refusal) {
		self.answer(
			batch,
			simple,
			None,
			Outcome::Failed {
				at_parse: false,
				error: refusal_message(r.code, &r.message, &r.hint),
			},
		);
	}

	/// Runs the join; Some(refusal) when it cannot, the client not yet answered.
	async fn join_inner(
		&mut self,
		sql: &str,
		batch: &[Message],
		why: &route::Refusal,
		caps: &Caps,
		made: &mut Vec<(NodeId, RelationName)>,
	) -> Result<Option<route::Refusal>, WireError> {
		let deadline = Instant::now() + Duration::from_millis(caps.timeout_ms.max(1));
		let late = || {
			join::over(
				"more time than a join may take",
				"join_timeout_ms",
				caps.timeout_ms,
				caps.timeout_ms,
			)
		};
		let facts = match &*self.analyze(sql) {
			Ok(v) if v.len() == 1 => v[0].facts.clone(),
			_ => return Ok(Some(why.clone())),
		};
		let reads = join::reads(&facts, &self.catalog);
		if reads.groups.is_empty() {
			return Ok(Some(why.clone()));
		}
		let home = self.home();
		let mut temps: HashMap<RelationName, RelationName> = HashMap::new();
		let mut next = 0usize;

		// 1. Broadcast: the largest group stays, the rest is copied to its nodes.
		let (est, sizes) = self.estimate(&reads).await?;
		if caps.broadcast_rows > 0 {
			let linear = |g: &[RelationName]| {
				let catalog = self.catalog.clone();
				let path = self.search_path.clone();
				let resolve =
					move |sch: &str, t: &str| crate::analyze::resolve_name(&catalog, &path, sch, t);
				join::stays_linear(sql, g, &resolve)
			};
			if let Some(a) = join::anchor(&reads, &est).filter(|a| linear(&reads.groups[*a])) {
				let anchor = reads.groups[a].clone();
				let others: Vec<RelationName> = reads
					.groups
					.iter()
					.enumerate()
					.filter(|(i, _)| *i != a)
					.flat_map(|(_, g)| g.iter().cloned())
					.chain(reads.globals.iter().cloned())
					.collect();
				let estimated: f64 = others
					.iter()
					.map(|r| est.get(r).copied().unwrap_or(0.0))
					.sum();
				if !others.is_empty() && estimated <= caps.broadcast_rows as f64 * 2.0 {
					let mut fetched = Vec::new();
					let mut rows = 0u64;
					let mut fits = true;
					for r in &others {
						match self.fetch(r).await? {
							Ok(f) => {
								rows += f.rows.len() as u64;
								fetched.push((r.clone(), f));
							}
							Err(e) => {
								return Ok(Some(route::Refusal {
									code: route::NOT_ACROSS_NODES,
									message: format!(
										"a join could not read {r}: {}",
										error_text(&e)
									),
									hint: "Run the query again.".into(),
								}));
							}
						}
						if rows > caps.broadcast_rows {
							fits = false;
							break;
						}
					}
					if fits {
						let keyspace = match self.catalog.relations.get(&anchor[0]) {
							Some(RelationKind::Sharded { keyspace, .. }) => keyspace.clone(),
							_ => return Ok(Some(why.clone())),
						};
						let mut targets = self.catalog.nodes_of(&keyspace);
						if !targets.contains(&home) {
							targets.push(home);
						}
						for (r, f) in &fetched {
							next += 1;
							let t = join::temp_name(next);
							for n in &targets {
								if let Some(e) = self.put(*n, &t, f, made).await? {
									return Ok(Some(e));
								}
							}
							temps.insert(r.clone(), t);
						}
						if Instant::now() > deadline {
							return Ok(Some(late()));
						}
						if let Some(done) = self
							.run_rewritten(sql, batch, &temps, &reference(&temps), None, false)
							.await?
						{
							return Ok(done);
						}
					}
				}
			}
		}

		// 2. Repartition: two big tables joined on other columns, both re-hashed on those.
		if let Some(done) = self
			.repartition(
				sql, batch, &reads, &est, &sizes, caps, made, &mut next, deadline,
			)
			.await?
		{
			return Ok(done);
		}

		// 3. Gather: every distributed table it reads, copied to the home node.
		if caps.gather_rows == 0 {
			return Ok(Some(why.clone()));
		}
		let mut rows = 0u64;
		let mut bytes = 0u64;
		let mut all: Vec<RelationName> = Vec::new();
		for g in &reads.groups {
			for r in g {
				if !all.contains(r) {
					all.push(r.clone());
				}
			}
		}
		for r in &all {
			if let Some(t) = temps.get(r)
				&& made.iter().any(|(n, x)| *n == home && x == t)
			{
				continue;
			}
			let f = match self.fetch(r).await? {
				Ok(f) => f,
				Err(e) => {
					return Ok(Some(route::Refusal {
						code: route::NOT_ACROSS_NODES,
						message: format!("a join could not read {r}: {}", error_text(&e)),
						hint: "Run the query again.".into(),
					}));
				}
			};
			rows += f.rows.len() as u64;
			bytes += f.bytes;
			if rows > caps.gather_rows {
				return Ok(Some(join::over(
					"more rows on one node",
					"join_gather_rows",
					rows,
					caps.gather_rows,
				)));
			}
			if bytes > caps.gather_bytes {
				return Ok(Some(join::over(
					"more bytes on one node",
					"join_gather_bytes",
					bytes,
					caps.gather_bytes,
				)));
			}
			if Instant::now() > deadline {
				return Ok(Some(late()));
			}
			next += 1;
			let t = join::temp_name(next);
			if let Some(e) = self.put(home, &t, &f, made).await? {
				return Ok(Some(e));
			}
			temps.insert(r.clone(), t);
		}
		match self
			.run_rewritten(sql, batch, &temps, &reference(&temps), None, true)
			.await?
		{
			Some(done) => Ok(done),
			None => Ok(Some(why.clone())),
		}
	}

	/// The statement with each relation in `temps` renamed to its copy, run as a scatter (copies
	/// as reference tables) or, `on_home`, on the home node alone. Some(None) when it answered the
	/// client, Some(Some(r)) for a refusal, None when this shape cannot run it.
	async fn run_rewritten(
		&mut self,
		sql: &str,
		batch: &[Message],
		temps: &HashMap<RelationName, RelationName>,
		kinds: &HashMap<RelationName, RelationKind>,
		spread: Option<&Keyspace>,
		on_home: bool,
	) -> Result<Option<Option<route::Refusal>>, WireError> {
		let catalog = self.catalog.clone();
		let path = self.search_path.clone();
		let resolve = |sch: &str, t: &str| crate::analyze::resolve_name(&catalog, &path, sch, t);
		let rewritten = match crate::chunk::rename(sql, temps, &resolve) {
			Ok(s) => s,
			Err(e) => {
				return Ok(Some(Some(route::Refusal {
					code: route::NOT_ACROSS_NODES,
					message: format!("a join could not rewrite this read: {e}"),
					hint: "Pin the query to one distribution key value.".into(),
				})));
			}
		};
		// The copies are reference tables to the planner (a repartition's, sharded tables of its
		// own keyspace); a gathered statement reads nothing else that is distributed.
		let mut c = (*catalog).clone();
		for (t, k) in kinds {
			c.relations.insert(t.clone(), k.clone());
		}
		if let Some(ks) = spread {
			c.keyspaces.insert(ks.name.clone(), ks.clone());
		}
		let home = self.home();
		let nodes = if on_home {
			vec![home]
		} else {
			let stmts = crate::analyze::analyze(&rewritten, &c, &path);
			let Ok(v) = stmts else {
				return Ok(None);
			};
			let [st] = v.as_slice() else {
				return Ok(None);
			};
			match route::route(&st.facts, &c, &[]) {
				// A scatter that would still be refused (a set operation, a window): gathered.
				Route::Scatter(_) if crate::scatter::check(&rewritten, &c, &path).is_err() => {
					return Ok(None);
				}
				Route::Scatter(nodes) => nodes,
				Route::Node(n) => vec![n],
				Route::Home => vec![home],
				_ => return Ok(None),
			}
		};
		let saved = std::mem::replace(&mut self.catalog, Arc::new(c));
		self.in_join = true;
		// Boxed: gather may come back here (join.rs answers what its scatter refuses).
		let r = Box::pin(self.gather(nodes, rewritten, Vec::new(), batch.to_vec())).await;
		self.in_join = false;
		self.catalog = saved;
		r.map(|()| Some(None))
	}

	/// Estimated rows of each distributed relation `reads` names, from each node's statistics
	/// (a chunked table's chunks counted with it; a table never analyzed, by its size at 100 bytes a
	/// row), and their size on disk.
	async fn estimate(
		&mut self,
		reads: &join::Reads,
	) -> Result<(HashMap<RelationName, f64>, HashMap<RelationName, f64>), WireError> {
		let mut out = HashMap::new();
		let mut sizes = HashMap::new();
		for g in &reads.groups {
			for r in g {
				let Some(RelationKind::Sharded { keyspace, .. }) = self.catalog.relations.get(r)
				else {
					continue;
				};
				let nodes = self.catalog.nodes_of(keyspace);
				for n in &nodes {
					self.ensure_conn(*n).await?;
				}
				let q = format!(
					"select coalesce(sum(case when c.reltuples >= 0 then c.reltuples else pg_table_size(c.oid) / 100 end), 0)::text, \
					coalesce(sum(pg_table_size(c.oid)), 0)::text from pg_class c \
					where c.oid = to_regclass({l}) or c.oid in (select inhrelid from pg_inherits where inhparent = to_regclass({l}))",
					l = crate::catalog::quote_literal(&super::qualified_name(r))
				);
				let (mut total, mut bytes) = (0.0, 0.0);
				if let Ok(answers) = self.run_worker(&nodes, &q, &[], None, &[]).await? {
					let cell = |a: &super::Answer, i: usize| {
						a.rows
							.first()
							.and_then(|r| r.get(i).cloned().flatten())
							.and_then(|v| String::from_utf8(v).ok())
							.and_then(|v| v.parse::<f64>().ok())
							.unwrap_or(0.0)
					};
					for a in answers {
						total += cell(&a, 0);
						bytes += cell(&a, 1);
					}
				}
				out.insert(r.clone(), total);
				sizes.insert(r.clone(), bytes);
			}
		}
		for g in &reads.globals {
			out.insert(g.clone(), 0.0);
		}
		Ok((out, sizes))
	}

	/// Every row of `rel` from the nodes that own it (each node's own rows only), or from the home
	/// node for a global table, in text format.
	async fn fetch(&mut self, rel: &RelationName) -> Result<Result<Fetched, Message>, WireError> {
		let home = self.home();
		let name = super::qualified_name(rel);
		let (nodes, sqls): (Vec<NodeId>, Vec<String>) = match self.catalog.relations.get(rel) {
			Some(RelationKind::Sharded {
				keyspace,
				key_column,
			}) => {
				let nodes = self.catalog.nodes_of(keyspace);
				let chunked = self.catalog.chunked.contains(rel);
				let mut sqls = Vec::new();
				for n in &nodes {
					if chunked {
						// A node's shell reads exactly the chunks it owns.
						sqls.push(format!("select * from {name}"));
						continue;
					}
					let version = self
						.catalog
						.nodes
						.get(n)
						.and_then(|x| x.server_version_num)
						.unwrap_or(180_000);
					let ks = &self.catalog.keyspaces[keyspace];
					let owns = crate::catalog::owns_check(
						format!("lepis_row.{}", quote_ident(key_column)),
						ks,
						*n,
						version,
					)
					.unwrap_or_else(|_| "false".into());
					sqls.push(format!(
						"select lepis_row.* from {name} lepis_row where {owns}"
					));
				}
				(nodes, sqls)
			}
			_ => (vec![home], vec![format!("select * from {name}")]),
		};
		for n in &nodes {
			self.ensure_conn(*n).await?;
			self.catch_up_sets(*n).await?;
		}
		let answers = match self.run_each(&nodes, &sqls, &[], None, &[]).await? {
			Ok(a) => a,
			Err(e) => return Ok(Err(e)),
		};
		let fields = answers
			.first()
			.map(|a| a.fields.clone())
			.unwrap_or_default();
		let mut rows = Vec::new();
		let mut bytes = 0u64;
		for a in answers {
			for r in a.rows {
				bytes += r
					.iter()
					.map(|c| c.as_ref().map_or(1, Vec::len) as u64 + 1)
					.sum::<u64>();
				rows.push(r);
			}
		}
		Ok(Ok(Fetched {
			fields,
			rows,
			bytes,
		}))
	}

	/// `f` as temporary table `t` on `node`: made with the fetched columns' types, filled by COPY,
	/// analyzed. Some(refusal) when the node would not take it.
	async fn put(
		&mut self,
		node: NodeId,
		t: &RelationName,
		f: &Fetched,
		made: &mut Vec<(NodeId, RelationName)>,
	) -> Result<Option<route::Refusal>, WireError> {
		self.ensure_conn(node).await?;
		self.catch_up_sets(node).await?;
		let oids: Vec<String> = f.fields.iter().map(|x| x.oid.to_string()).collect();
		let mods: Vec<String> = f.fields.iter().map(|x| x.typmod.to_string()).collect();
		let types = self
			.home_query(
				"select format_type(o::oid, m::int) from unnest(string_to_array($1, ',')::int8[], \
				string_to_array($2, ',')::int[]) with ordinality as x(o, m, i) order by i",
				&[&oids.join(","), &mods.join(",")],
			)
			.await?;
		let cols: Vec<String> = f
			.fields
			.iter()
			.zip(&types)
			.map(|(x, ty)| {
				format!(
					"{} {}",
					quote_ident(&x.name),
					ty.first()
						.cloned()
						.flatten()
						.unwrap_or_else(|| "text".into())
				)
			})
			.collect();
		let table = format!("pg_temp.{}", quote_ident(&t.table));
		let conn = self.conns.get_mut(&node).expect("connected");
		if let Err(e) = (Part { node, conn })
			.execute_sql(&format!(
				"drop table if exists {table}; create temp table {} ({})",
				quote_ident(&t.table),
				cols.join(", ")
			))
			.await
		{
			return Ok(Some(route::Refusal {
				code: route::NOT_ACROSS_NODES,
				message: format!("{node} could not hold a join's copy: {e}"),
				hint: "Run the query again.".into(),
			}));
		}
		made.push((node, t.clone()));
		// COPY … FROM STDIN, the rows in text format, in messages of about 256 kB.
		self.send(node, &[wire::query(&format!("copy {table} from stdin"))])
			.await?;
		let conn = self.conns.get_mut(&node).expect("connected");
		conn.prepared.remove("");
		let mut error = None;
		loop {
			let m = conn.reader.next().await?;
			match m.tag {
				b'G' => break,
				b'E' => error = Some(m),
				b'Z' => break,
				_ => {}
			}
		}
		if let Some(e) = error {
			return Ok(Some(route::Refusal {
				code: route::NOT_ACROSS_NODES,
				message: format!("{node} refused a join's copy: {}", error_text(&e)),
				hint: "Run the query again.".into(),
			}));
		}
		let mut buf = Vec::with_capacity(1 << 18);
		let mut out = Vec::new();
		for r in &f.rows {
			for (i, c) in r.iter().enumerate() {
				if i > 0 {
					buf.push(b'\t');
				}
				join::copy_cell(&mut buf, c.as_deref());
			}
			buf.push(b'\n');
			if buf.len() >= 1 << 18 {
				put_message(&mut out, &Message::new(b'd', std::mem::take(&mut buf)));
			}
		}
		if !buf.is_empty() {
			put_message(&mut out, &Message::new(b'd', buf));
		}
		put_message(&mut out, &Message::new(b'c', Vec::new()));
		self.send_raw(node, &out).await?;
		let conn = self.conns.get_mut(&node).expect("connected");
		let mut error = None;
		loop {
			let m = conn.reader.next().await?;
			match m.tag {
				b'E' => error = Some(m),
				b'Z' => break,
				_ => {}
			}
		}
		if let Some(e) = error {
			return Ok(Some(route::Refusal {
				code: route::NOT_ACROSS_NODES,
				message: format!("{node} refused a join's copy: {}", error_text(&e)),
				hint: "Run the query again.".into(),
			}));
		}
		let conn = self.conns.get_mut(&node).expect("connected");
		let _ = Part { node, conn }
			.execute_sql(&format!("analyze {table}"))
			.await;
		Ok(None)
	}
}

/// Each copy in `temps` as a reference table.
fn reference(temps: &HashMap<RelationName, RelationName>) -> HashMap<RelationName, RelationKind> {
	temps
		.values()
		.map(|t| (t.clone(), RelationKind::Reference))
		.collect()
}

/// An error message's sentence.
fn error_text(m: &Message) -> String {
	wire::parse_error_fields(&m.body)
		.into_iter()
		.find(|(k, _)| *k == b'M')
		.map(|(_, v)| v)
		.unwrap_or_else(|| cstr(&m.body))
}

#[allow(dead_code)]
fn _uses(_: Answer) {}
