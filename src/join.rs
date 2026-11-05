//! Distributed joins and windows: a read the router used to refuse
//! because it needed rows from several nodes at once is answered by copying the rows it needs
//! where they are needed, into temporary tables, and running the statement there.
//!
//! - **Broadcast.** The statement's largest sharded table (with what is joined to it on its key)
//!   stays where it is; every other table it reads (another keyspace's, one not joined on the key,
//!   a global table) is fetched once, counted against `lepis.join_broadcast_rows`, and copied to
//!   each node that holds the large one as a temporary table. To the planner that copy is a
//!   reference table, and with reference tables the statement is one the router already answers
//!   (a scatter whose merge the oracle proves). Each node joins its share; the router merges.
//! - **Gather.** A statement no scatter can answer (a window over every node's rows, a set
//!   operation, DISTINCT ON, GROUPING SETS, a subquery that aggregates a distributed table), or a
//!   broadcast whose small side is over its cap: every distributed table it reads is copied to the
//!   home node, under `lepis.join_gather_rows` and `lepis.join_gather_bytes`, and the statement
//!   runs there, as one Postgres runs it.
//!
//! Each cap is a session setting (`SET lepis.join_broadcast_rows = …`) over the cluster's
//! setting over the default here; 0 turns a strategy off. Over a cap the statement is refused as
//! before, the message naming the cap and the fix (L9). A read stays per-shard consistent, not
//! one global snapshot (L9): each node's rows are read once, in its own snapshot.
//!
//! The copies are temporary tables of the router's own sessions, dropped when the statement
//! ends, whether it answered, failed or was cancelled; a node lost mid-join ends the session,
//! and its temporary tables with it. Nothing reaches the client until every node has answered.
//!
//! Only what this file names may take this path (`answerable`): a refusal about anything else
//! (an aggregate the router cannot finish, row locks, …) stays a refusal.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::catalog::{Catalog, Keyspace, NodeId, RelationKind, RelationName, Strategy};
use crate::hash::KeyType;
use crate::route::{Facts, Refusal};

/// The caps, each an initial guess measured in Phase 6.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Caps {
	/// Rows of the small side a broadcast copies (all its tables together).
	pub broadcast_rows: u64,
	/// Rows a gather copies to the home node (every table together).
	pub gather_rows: u64,
	/// Bytes a gather copies (the rows' text, as fetched).
	pub gather_bytes: u64,
	/// Joins this router runs at once.
	pub concurrency: u64,
	/// How long a join may take before it is given up, in ms.
	pub timeout_ms: u64,
	/// Bytes (the tables' size on disk) a repartition through the router moves.
	pub repartition_bytes: u64,
	/// Rows (estimated) a node-to-node shuffle moves; 0 leaves the router path alone.
	pub shuffle_rows: u64,
}

impl Default for Caps {
	fn default() -> Caps {
		Caps {
			broadcast_rows: 100_000,
			gather_rows: 1_000_000,
			gather_bytes: 1 << 30,
			concurrency: 4,
			timeout_ms: 60_000,
			repartition_bytes: 256 << 20,
			shuffle_rows: 1_000_000_000,
		}
	}
}

/// The setting names, as a session `SET lepis.<name>` or a cluster setting `<name>`.
pub const SETTINGS: [&str; 7] = [
	"join_broadcast_rows",
	"join_gather_rows",
	"join_gather_bytes",
	"join_concurrency",
	"join_timeout_ms",
	"join_repartition_bytes",
	"join_shuffle_rows",
];

impl Caps {
	/// The cluster's settings over the defaults, then the session's over those.
	pub fn read(cluster: &serde_json::Value, session: &HashMap<String, u64>) -> Caps {
		let d = Caps::default();
		let get = |k: &str, d: u64| {
			session
				.get(k)
				.copied()
				.or_else(|| cluster.get(k).and_then(serde_json::Value::as_u64))
				.unwrap_or(d)
		};
		Caps {
			broadcast_rows: get("join_broadcast_rows", d.broadcast_rows),
			gather_rows: get("join_gather_rows", d.gather_rows),
			gather_bytes: get("join_gather_bytes", d.gather_bytes),
			concurrency: get("join_concurrency", d.concurrency),
			timeout_ms: get("join_timeout_ms", d.timeout_ms),
			repartition_bytes: get("join_repartition_bytes", d.repartition_bytes),
			shuffle_rows: get("join_shuffle_rows", d.shuffle_rows),
		}
	}
}

/// The session's `lepis.join_*` settings, from the SETs the router replays on its nodes.
pub fn session_caps(sets: &[String]) -> HashMap<String, u64> {
	let mut out = HashMap::new();
	for s in sets {
		let lower = s.to_ascii_lowercase();
		let Some(rest) = lower.trim().strip_prefix("set ") else {
			continue;
		};
		let rest = rest.trim_start_matches("session ").trim();
		let Some(rest) = rest.strip_prefix("lepis.") else {
			continue;
		};
		let (name, value) = match rest.split_once('=').or_else(|| rest.split_once(" to ")) {
			Some((n, v)) => (n.trim(), v.trim()),
			None => continue,
		};
		if !SETTINGS.contains(&name) {
			continue;
		}
		let v = value.trim_matches(|c| c == '\'' || c == '"' || c == ';' || c == ' ');
		if let Ok(n) = v.parse::<u64>() {
			out.insert(name.to_string(), n);
		}
	}
	out
}

/// The refusals this file may answer instead, by the sentence route.rs or scatter.rs gives.
const ANSWERABLE: [&str; 9] = [
	"a join of distributed tables that is not on the distribution key",
	"are not colocated",
	"lives only on the home node and cannot be combined with a distributed table",
	"UNION, INTERSECT and EXCEPT across several nodes",
	"SELECT DISTINCT ON across several nodes",
	"GROUPING SETS, ROLLUP and CUBE across several nodes",
	"a window function across several nodes",
	"FETCH … WITH TIES across several nodes",
	"a subquery with",
];

/// Whether a refusal is one a join or a gather answers.
pub fn answerable(r: &Refusal) -> bool {
	r.code == crate::route::NOT_ACROSS_NODES && ANSWERABLE.iter().any(|m| r.message.contains(m))
}

/// Joins running on this router now, for the concurrency cap.
static RUNNING: AtomicUsize = AtomicUsize::new(0);

/// A slot among `Caps::concurrency`, given back when dropped.
pub struct Slot;

impl Slot {
	pub fn take(caps: &Caps) -> Option<Slot> {
		let n = RUNNING.fetch_add(1, Ordering::SeqCst);
		if n as u64 >= caps.concurrency {
			RUNNING.fetch_sub(1, Ordering::SeqCst);
			return None;
		}
		Some(Slot)
	}
}

impl Drop for Slot {
	fn drop(&mut self) {
		RUNNING.fetch_sub(1, Ordering::SeqCst);
	}
}

/// What a statement reads that is not everywhere: its sharded tables, with what each is joined to
/// on its key (index into `groups`), and its global tables.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Reads {
	/// Groups of sharded relations tied by key equality (each a candidate to stay in place).
	pub groups: Vec<Vec<RelationName>>,
	/// Global (home-only) relations.
	pub globals: Vec<RelationName>,
}

/// The tables `facts` reads that a join must move, grouped. A relation used in two groups (a
/// self-join not on the key) is in both: such a statement can only be gathered.
pub fn reads(facts: &Facts, catalog: &Catalog) -> Reads {
	let n = facts.tables.len();
	let mut parent: Vec<usize> = (0..n).collect();
	fn find(p: &mut [usize], x: usize) -> usize {
		let mut x = x;
		while p[x] != x {
			p[x] = p[p[x]];
			x = p[x];
		}
		x
	}
	for &(a, b) in &facts.key_joins {
		if a < n && b < n {
			let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
			parent[ra] = rb;
		}
	}
	let mut out = Reads::default();
	let mut roots: Vec<usize> = Vec::new();
	for (i, t) in facts.tables.iter().enumerate() {
		match catalog.relations.get(&t.name) {
			Some(RelationKind::Sharded { .. }) => {
				let r = find(&mut parent, i);
				match roots.iter().position(|x| *x == r) {
					Some(g) => {
						if !out.groups[g].contains(&t.name) {
							out.groups[g].push(t.name.clone());
						}
					}
					None => {
						roots.push(r);
						out.groups.push(vec![t.name.clone()]);
					}
				}
			}
			Some(RelationKind::Reference) => {}
			_ => {
				if !out.globals.contains(&t.name)
					&& !t.name.schema.starts_with("pg_")
					&& t.name.schema != "information_schema"
				{
					out.globals.push(t.name.clone());
				}
			}
		}
	}
	out
}

/// Which group stays in place for a broadcast: the one with the most estimated rows, when no
/// relation of it is also read elsewhere in the statement. None: gather instead.
pub fn anchor(reads: &Reads, rows: &HashMap<RelationName, f64>) -> Option<usize> {
	let size = |g: &[RelationName]| {
		g.iter()
			.map(|r| rows.get(r).copied().unwrap_or(0.0))
			.sum::<f64>()
	};
	let best = (0..reads.groups.len())
		.max_by(|a, b| size(&reads.groups[*a]).total_cmp(&size(&reads.groups[*b])))?;
	let shared = reads.groups[best].iter().any(|r| {
		reads
			.groups
			.iter()
			.enumerate()
			.any(|(i, g)| i != best && g.contains(r))
	});
	(!shared).then_some(best)
}

/// Whether a broadcast may leave `stay` where it is: every use of those relations in `sql` is in
/// the outermost SELECT's FROM, on no nullable side of an outer join, and nowhere else (not in a
/// subquery, a CTE, a set operation). Only then is the statement's answer the union of each
/// node's answer over its own rows of them; an anti-join, a semi-join or an outer join whose
/// other side is the staying table would be computed against one node's rows alone.
pub fn stays_linear(
	sql: &str,
	stay: &[RelationName],
	resolve: &dyn Fn(&str, &str) -> RelationName,
) -> bool {
	use pg_query::NodeEnum;
	let Ok(parsed) = pg_query::parse(sql) else {
		return false;
	};
	let mut total = 0usize;
	for (n, _, _, _) in parsed.protobuf.nodes() {
		if let pg_query::NodeRef::RangeVar(rv) = n
			&& stay.contains(&resolve(&rv.schemaname, &rv.relname))
		{
			total += 1;
		}
	}
	let Some(NodeEnum::SelectStmt(top)) = parsed
		.protobuf
		.stmts
		.first()
		.and_then(|s| s.stmt.as_deref())
		.and_then(|n| n.node.as_ref())
	else {
		return false;
	};
	if top.larg.is_some() || top.with_clause.is_some() {
		return false;
	}
	// The uses in the FROM list that keep every row they produce (inner joins, the preserved side).
	fn safe(
		n: &pg_query::protobuf::Node,
		nullable: bool,
		stay: &[RelationName],
		resolve: &dyn Fn(&str, &str) -> RelationName,
	) -> usize {
		use pg_query::protobuf::JoinType;
		match n.node.as_ref() {
			Some(NodeEnum::RangeVar(rv)) => {
				usize::from(!nullable && stay.contains(&resolve(&rv.schemaname, &rv.relname)))
			}
			Some(NodeEnum::JoinExpr(j)) => {
				let t = JoinType::try_from(j.jointype).unwrap_or(JoinType::JoinInner);
				let (ln, rn) = match t {
					JoinType::JoinLeft => (nullable, true),
					JoinType::JoinRight => (true, nullable),
					JoinType::JoinFull => (true, true),
					_ => (nullable, nullable),
				};
				j.larg.as_deref().map_or(0, |l| safe(l, ln, stay, resolve))
					+ j.rarg.as_deref().map_or(0, |r| safe(r, rn, stay, resolve))
			}
			_ => 0,
		}
	}
	let ok: usize = top
		.from_clause
		.iter()
		.map(|n| safe(n, false, stay, resolve))
		.sum();
	total > 0 && ok == total
}

/// The join columns of a repartition of `a` and `b`: the read uses each of them once, in its
/// outermost FROM, on no nullable side (`stays_linear`), and equates a column of one with a
/// column of the other in its WHERE or a join's ON, as a conjunct (both written qualified, by
/// the table's name or alias). Re-hashed on those columns, every pair of rows that can join is on
/// one node, and the answer is the union of each node's answer over its share, which the scatter
/// and merge the oracle proves then finish. None: no such equality, or the shape is not one.
pub fn repartition_keys(
	sql: &str,
	a: &RelationName,
	b: &RelationName,
	resolve: &dyn Fn(&str, &str) -> RelationName,
) -> Option<(String, String)> {
	use pg_query::NodeEnum;
	if a == b || !stays_linear(sql, &[a.clone(), b.clone()], resolve) {
		return None;
	}
	let parsed = pg_query::parse(sql).ok()?;
	let (mut uses_a, mut uses_b) = (0, 0);
	for (n, _, _, _) in parsed.protobuf.nodes() {
		if let pg_query::NodeRef::RangeVar(rv) = n {
			let r = resolve(&rv.schemaname, &rv.relname);
			uses_a += usize::from(&r == a);
			uses_b += usize::from(&r == b);
		}
	}
	if uses_a != 1 || uses_b != 1 {
		return None;
	}
	let Some(NodeEnum::SelectStmt(top)) = parsed
		.protobuf
		.stmts
		.first()
		.and_then(|s| s.stmt.as_deref())
		.and_then(|n| n.node.as_ref())
	else {
		return None;
	};
	// The names each is referred to by, and every ON in the FROM tree.
	let (mut ref_a, mut ref_b) = (None, None);
	let mut quals: Vec<&pg_query::protobuf::Node> = Vec::new();
	fn from<'n>(
		n: &'n pg_query::protobuf::Node,
		a: &RelationName,
		b: &RelationName,
		resolve: &dyn Fn(&str, &str) -> RelationName,
		ref_a: &mut Option<String>,
		ref_b: &mut Option<String>,
		quals: &mut Vec<&'n pg_query::protobuf::Node>,
	) {
		match n.node.as_ref() {
			Some(NodeEnum::RangeVar(rv)) => {
				let r = resolve(&rv.schemaname, &rv.relname);
				let name = rv
					.alias
					.as_ref()
					.map(|x| x.aliasname.clone())
					.unwrap_or_else(|| rv.relname.clone());
				if &r == a {
					*ref_a = Some(name);
				} else if &r == b {
					*ref_b = Some(name);
				}
			}
			Some(NodeEnum::JoinExpr(j)) => {
				if let Some(q) = j.quals.as_deref() {
					quals.push(q);
				}
				for side in [j.larg.as_deref(), j.rarg.as_deref()].into_iter().flatten() {
					from(side, a, b, resolve, ref_a, ref_b, quals);
				}
			}
			_ => {}
		}
	}
	for n in &top.from_clause {
		from(n, a, b, resolve, &mut ref_a, &mut ref_b, &mut quals);
	}
	let (ref_a, ref_b) = (ref_a?, ref_b?);
	if let Some(w) = top.where_clause.as_deref() {
		quals.push(w);
	}
	fn conjuncts<'n>(n: &'n pg_query::protobuf::Node, out: &mut Vec<&'n pg_query::protobuf::Node>) {
		match n.node.as_ref() {
			Some(NodeEnum::BoolExpr(b))
				if b.boolop == pg_query::protobuf::BoolExprType::AndExpr as i32 =>
			{
				for x in &b.args {
					conjuncts(x, out);
				}
			}
			_ => out.push(n),
		}
	}
	fn column(n: &pg_query::protobuf::Node) -> Option<(String, String)> {
		let Some(NodeEnum::ColumnRef(c)) = n.node.as_ref() else {
			return None;
		};
		let text = |x: &pg_query::protobuf::Node| match x.node.as_ref() {
			Some(NodeEnum::String(s)) => Some(s.sval.clone()),
			_ => None,
		};
		match c.fields.as_slice() {
			[t, col] => Some((text(t)?, text(col)?)),
			_ => None,
		}
	}
	let mut all = Vec::new();
	for q in quals {
		conjuncts(q, &mut all);
	}
	for c in all {
		let Some(NodeEnum::AExpr(e)) = c.node.as_ref() else {
			continue;
		};
		let is_eq = e.kind == pg_query::protobuf::AExprKind::AexprOp as i32
			&& matches!(e.name.as_slice(), [x] if matches!(x.node.as_ref(), Some(NodeEnum::String(s)) if s.sval == "="));
		if !is_eq {
			continue;
		}
		let (Some(l), Some(r)) = (
			e.lexpr.as_deref().and_then(column),
			e.rexpr.as_deref().and_then(column),
		) else {
			continue;
		};
		if l.0 == ref_a && r.0 == ref_b {
			return Some((l.1, r.1));
		}
		if l.0 == ref_b && r.0 == ref_a {
			return Some((r.1, l.1));
		}
	}
	None
}

/// The hash a repartition places a join column's values by, from the column's type oid: the
/// key types L5 hashes. Two columns of the integer types hash alike (Postgres's own integer
/// hashes agree across widths); any other pair must be one type. None: not repartitioned.
pub fn join_key_type(a: u32, b: u32) -> Option<KeyType> {
	fn of(oid: u32) -> Option<KeyType> {
		Some(match oid {
			21 => KeyType::Int2,
			23 => KeyType::Int4,
			20 => KeyType::Int8,
			25 | 1043 => KeyType::Text,
			1042 => KeyType::Bpchar,
			1700 => KeyType::Numeric,
			2950 => KeyType::Uuid,
			17 => KeyType::Bytea,
			1082 => KeyType::Date,
			1114 => KeyType::Timestamp,
			1184 => KeyType::Timestamptz,
			_ => return None,
		})
	}
	let (x, y) = (of(a)?, of(b)?);
	let int = |k| matches!(k, KeyType::Int2 | KeyType::Int4 | KeyType::Int8);
	if int(x) && int(y) {
		return Some(KeyType::Int8);
	}
	(x == y).then_some(x)
}

/// The keyspace a repartition spreads both tables over: `nodes`, one even range each, hashed as
/// `key_type`. It lives only in the plan's own copy of the catalog.
pub fn spread_keyspace(name: &str, key_type: KeyType, nodes: &[NodeId]) -> Keyspace {
	Keyspace {
		name: name.to_string(),
		strategy: Strategy::Hash,
		key_type,
		seed: 0,
		ranges: Keyspace::even_ranges(nodes.len(), nodes),
		pins: HashMap::new(),
		chunks: Vec::new(),
		second: None,
	}
}

/// The node a value of the join column goes to (NULL, which joins nothing, to the first).
pub fn spread_node(ks: &Keyspace, value: Option<&str>) -> Result<NodeId, String> {
	match value {
		None => Ok(ks.ranges[0].node),
		Some(v) => ks
			.key_type
			.hash_text_value(v, ks.seed)
			.map(|h| ks.owner_of_hash(h))
			.map_err(|e| e.to_string()),
	}
}

/// The schema a node-to-node shuffle's staging tables live in (on every node).
pub const SHUFFLE_SCHEMA: &str = "lepis_shuffle";

/// Whether `r` is a join's own copy (a temporary table, or a shuffle's staging table), never a
/// table of the client's.
pub fn is_staging(r: &RelationName) -> bool {
	(r.schema == "pg_temp" && r.table.starts_with("lepis_join_")) || r.schema == SHUFFLE_SCHEMA
}

/// The temporary table a copy of `n` lands in: in the session's own temp schema.
pub fn temp_name(n: usize) -> RelationName {
	RelationName {
		schema: "pg_temp".into(),
		table: format!("lepis_join_{n}"),
	}
}

/// One cell in COPY's text format.
pub fn copy_cell(out: &mut Vec<u8>, cell: Option<&[u8]>) {
	let Some(v) = cell else {
		out.extend_from_slice(b"\\N");
		return;
	};
	for &b in v {
		match b {
			b'\\' => out.extend_from_slice(b"\\\\"),
			b'\t' => out.extend_from_slice(b"\\t"),
			b'\n' => out.extend_from_slice(b"\\n"),
			b'\r' => out.extend_from_slice(b"\\r"),
			_ => out.push(b),
		}
	}
}

/// A refusal over a cap: what was over, the cap's name, and the fix.
pub fn over(what: &str, cap: &str, value: u64, limit: u64) -> Refusal {
	Refusal {
		code: crate::route::NOT_ACROSS_NODES,
		message: format!("this read needs {what} ({value}), over lepis.{cap} ({limit})"),
		hint: format!(
			"Raise it for the session (SET lepis.{cap} = …), narrow the read, make the smaller table a reference table, or pin the query to one distribution key value."
		),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::route::TableUse;

	fn rel(t: &str) -> RelationName {
		RelationName {
			schema: "s".into(),
			table: t.into(),
		}
	}

	#[test]
	fn session_caps_read_the_sets() {
		let c = session_caps(&[
			"SET lepis.join_broadcast_rows = 5".into(),
			"set lepis.join_gather_rows to '7'".into(),
			"set work_mem = '4MB'".into(),
			"set lepis.other = 3".into(),
		]);
		assert_eq!(c.get("join_broadcast_rows"), Some(&5));
		assert_eq!(c.get("join_gather_rows"), Some(&7));
		assert_eq!(c.len(), 2);
		let caps = Caps::read(
			&serde_json::json!({"join_concurrency": 9, "join_gather_rows": 1}),
			&c,
		);
		assert_eq!(
			(caps.broadcast_rows, caps.gather_rows, caps.concurrency),
			(5, 7, 9)
		);
	}

	#[test]
	fn only_named_refusals_are_answered() {
		let r = |m: &str| Refusal {
			code: crate::route::NOT_ACROSS_NODES,
			message: m.into(),
			hint: String::new(),
		};
		assert!(answerable(&r(
			"a window function across several nodes would see only each node's rows"
		)));
		assert!(answerable(&r(
			"a join of distributed tables that is not on the distribution key would need rows from several nodes"
		)));
		assert!(!answerable(&r(
			"string_agg() across several nodes cannot be finished"
		)));
		assert!(!answerable(&Refusal {
			code: "22P02",
			message: "a window function across several nodes".into(),
			hint: String::new()
		}));
	}

	#[test]
	fn only_a_table_every_row_of_which_stands_alone_stays() {
		let res = |sch: &str, t: &str| RelationName {
			schema: if sch.is_empty() { "s" } else { sch }.into(),
			table: t.into(),
		};
		let a = [rel("a")];
		assert!(stays_linear(
			"select * from a join b on a.x = b.y where a.k = 1",
			&a,
			&res
		));
		assert!(stays_linear(
			"select * from a left join b on a.x = b.y",
			&a,
			&res
		));
		assert!(!stays_linear(
			"select * from b left join a on a.x = b.y",
			&a,
			&res
		));
		assert!(!stays_linear(
			"select * from b where not exists (select 1 from a where a.x = b.y)",
			&a,
			&res
		));
		assert!(!stays_linear(
			"select * from b where exists (select 1 from a where a.x = b.y)",
			&a,
			&res
		));
		assert!(stays_linear(
			"select * from a where not exists (select 1 from b where a.x = b.y)",
			&a,
			&res
		));
		assert!(!stays_linear(
			"select x from a union select y from b",
			&a,
			&res
		));
	}

	#[test]
	fn the_largest_group_stays_unless_it_is_read_twice() {
		let reads = Reads {
			groups: vec![vec![rel("a"), rel("b")], vec![rel("c")]],
			globals: vec![rel("g")],
		};
		let rows = HashMap::from([(rel("a"), 1e6), (rel("b"), 1e5), (rel("c"), 10.0)]);
		assert_eq!(anchor(&reads, &rows), Some(0));
		let twice = Reads {
			groups: vec![vec![rel("a")], vec![rel("a")]],
			globals: vec![],
		};
		assert_eq!(anchor(&twice, &rows), None);
		let mut out = Vec::new();
		copy_cell(&mut out, Some(b"a\tb\\c\n"));
		copy_cell(&mut out, None);
		assert_eq!(out, b"a\\tb\\\\c\\n\\N");
		let _ = TableUse {
			name: rel("x"),
			key_values: None,
			written: false,
		};
	}

	#[test]
	fn a_repartition_needs_one_equality_between_the_two() {
		let res = |sch: &str, t: &str| RelationName {
			schema: if sch.is_empty() { "s" } else { sch }.into(),
			table: t.into(),
		};
		let (a, b) = (rel("a"), rel("b"));
		let k = |sql: &str| repartition_keys(sql, &a, &b, &res);
		assert_eq!(
			k("select count(*) from a join b on a.x = b.y"),
			Some(("x".into(), "y".into()))
		);
		assert_eq!(
			k("select * from b bb, a aa where aa.k > 1 and bb.y = aa.x"),
			Some(("x".into(), "y".into()))
		);
		assert_eq!(
			k("select * from a join b on a.z < b.y and a.x = b.y"),
			Some(("x".into(), "y".into()))
		);
		// Not a conjunct, unqualified, on a nullable side, read twice: none.
		assert_eq!(k("select * from a join b on a.x = b.y or a.z = 1"), None);
		assert_eq!(k("select * from a join b on x = y"), None);
		assert_eq!(k("select * from a left join b on a.x = b.y"), None);
		assert_eq!(
			k("select * from a join b on a.x = b.y where a.x in (select x from a)"),
			None
		);
		assert_eq!(k("select * from a join c on a.x = c.y"), None);
	}

	#[test]
	fn join_columns_hash_alike_only_when_their_types_do() {
		assert_eq!(join_key_type(23, 20), Some(KeyType::Int8));
		assert_eq!(join_key_type(25, 1043), Some(KeyType::Text));
		assert_eq!(join_key_type(25, 1042), None);
		assert_eq!(join_key_type(1700, 23), None);
		assert_eq!(join_key_type(700, 700), None);
		let ks = spread_keyspace("x", KeyType::Int8, &[NodeId(1), NodeId(2), NodeId(3)]);
		assert_eq!(spread_node(&ks, None), Ok(NodeId(1)));
		// An int4 1 and an int8 1 land together, as their Postgres hashes agree.
		assert_eq!(spread_node(&ks, Some("1")), spread_node(&ks, Some("1")));
		let mut seen = std::collections::BTreeSet::new();
		for v in 0..300 {
			seen.insert(spread_node(&ks, Some(&v.to_string())).unwrap());
		}
		assert_eq!(seen.len(), 3);
		assert!(spread_node(&ks, Some("x")).is_err());
	}
}
