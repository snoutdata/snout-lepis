//! Phase 2: sending one statement to several nodes and collecting the answers.
//!
//! This file decides WHAT each node runs (the worker query) and HOW merge.rs combines the
//! answers (a `MergePlan`); router.rs does the sending. The worker is the client's SELECT,
//! rewritten through Postgres's own parse tree and deparsed (L4), never by editing text:
//!
//! - **Plain rows** run unchanged and are concatenated.
//! - **ORDER BY** stays on every node, so each answers in order and the router merges (k-way).
//!   A sort key that is not a column of the answer is added as a hidden column after the
//!   client's, and stripped before the client sees the rows.
//! - **LIMIT n OFFSET m** becomes `LIMIT n+m` on every node; the router skips m and keeps n.
//! - **Aggregates** become partial aggregates per node: count, sum, min, max and bool_and/or as
//!   themselves, avg as sum and count; GROUP BY stays, and HAVING, ORDER BY, LIMIT and DISTINCT
//!   move to the router, which finishes the aggregates (merge.rs). `count(DISTINCT x)` (and the
//!   sum / avg forms) adds x to each node's GROUP BY and counts the distinct values here.
//! - **Values a node must rank** (merge.rs) carry a hidden `pg_collation_for` column, so the
//!   home node orders them under the collation the expression really has.
//!
//! What cannot be computed this way is refused with a sentence naming the fix (L9): a window
//! function, a set operation, DISTINCT ON, locking clauses, aggregates the router cannot finish
//! (string_agg, array_agg, …), and a subquery that would be computed on each node's rows alone
//! (an aggregate, DISTINCT or LIMIT over a sharded table inside it, unless it groups by the
//! shard key, which keeps each group on one node).

use std::collections::{BTreeSet, HashMap};

use pg_query::NodeEnum;
use pg_query::protobuf::{
	AConst, AExprKind, BoolExprType, ColumnRef, ExplainStmt, FuncCall, LimitOption, Node,
	NullTestType, ParseResult, RawStmt, ResTarget, SelectStmt, SetOperation, SortBy, SortByDir,
	SortByNulls, a_const,
};

use crate::analyze;
use crate::catalog::{Catalog, RelationKind, RelationName};
use crate::merge::{
	self, Agg, AggKind, CmpOp, Expr, Groups, MergeKind, MergePlan, SortKey, is_native,
};
use crate::route::{NOT_ACROSS_NODES, ParamValue, Refusal};

/// What one node said the original statement returns (Describe), when the router has asked.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Described {
	pub names: Vec<String>,
	pub types: Vec<u32>,
}

pub struct Context<'a> {
	pub catalog: &'a Catalog,
	pub search_path: &'a [String],
	/// None when only checking (tests/routes.rs): names and types are unknown then.
	pub described: Option<&'a Described>,
	/// Function names the home node says are aggregates (user-defined ones included).
	pub aggregates: &'a [String],
	pub params: &'a [ParamValue],
	pub param_types: &'a [u32],
	/// The client's result formats as Bind sent them (none, one for all, or one per column).
	pub result_formats: &'a [i16],
	/// The worker's column types, once a node has described it (see `Planned::needs_types`).
	pub col_types: Option<&'a [u32]>,
	/// Whether types Lepis does not know by number (user types, domains) are collatable.
	pub collatable: &'a HashMap<u32, bool>,
}

impl Context<'_> {
	/// The format the client wants output column `i` in.
	pub fn format(&self, i: usize) -> i16 {
		match self.result_formats {
			[] => 0,
			[f] => *f,
			fs => fs.get(i).copied().unwrap_or(0),
		}
	}
}

/// What every node runs and how the answers combine.
#[derive(Clone, Debug)]
pub struct Planned {
	pub worker_sql: String,
	/// The format each worker column is fetched in.
	pub formats: Vec<i16>,
	pub merge: MergePlan,
	/// EXPLAIN of the statement (text format): each node explains the worker.
	pub explain: bool,
	/// One line for EXPLAIN: how the answers are combined.
	pub summary: String,
	/// A compared column's type is not known yet: describe `worker_sql` on a node and plan
	/// again with `Context::col_types`, so its collation is asked for only if it has one
	/// (`pg_collation_for` is an error on a type without collations).
	pub needs_types: bool,
	/// The worker's SELECT (without the EXPLAIN around it), for EXPLAIN's routing line.
	pub worker_select: String,
}

fn no(message: impl Into<String>, hint: impl Into<String>) -> Refusal {
	Refusal {
		code: NOT_ACROSS_NODES,
		message: message.into(),
		hint: hint.into(),
	}
}

const PIN: &str = "Pin the query to one shard key value, so one node answers it.";

/// Whether `sql` (one statement) is an EXPLAIN Lepis answers itself: text format, of a SELECT.
pub fn is_explain(sql: &str) -> bool {
	let Ok(parsed) = pg_query::parse(sql) else {
		return false;
	};
	let Some(NodeEnum::ExplainStmt(e)) = parsed
		.protobuf
		.stmts
		.first()
		.and_then(|s| s.stmt.as_deref())
		.and_then(|n| n.node.as_ref())
	else {
		return false;
	};
	matches!(
		e.query.as_deref().and_then(|n| n.node.as_ref()),
		Some(NodeEnum::SelectStmt(_))
	) && explain_format_is_text(e)
}

fn explain_format_is_text(e: &ExplainStmt) -> bool {
	e.options.iter().all(|o| match &o.node {
		Some(NodeEnum::DefElem(d)) if d.defname == "format" => matches!(
			d.arg.as_deref().and_then(|a| a.node.as_ref()),
			Some(NodeEnum::String(s)) if s.sval.eq_ignore_ascii_case("text")
		),
		_ => true,
	})
}

/// Every function name the statement calls (lower case, unqualified), for asking the home node
/// which of them are aggregates.
pub fn functions(sql: &str) -> Vec<String> {
	let Ok(parsed) = pg_query::parse(sql) else {
		return Vec::new();
	};
	let mut out = BTreeSet::new();
	for raw in &parsed.protobuf.stmts {
		if let Some(n) = raw.stmt.as_deref() {
			walk(n, &mut |s| {
				if let Seen::Expr(NodeEnum::FuncCall(f)) = s
					&& let Some(name) = func_name(f)
				{
					out.insert(name.1);
				}
				true
			});
		}
	}
	out.into_iter().collect()
}

/// Whether the statement reads the clock through SQL's own syntax (CURRENT_TIMESTAMP,
/// CURRENT_DATE, LOCALTIME, …), which each node answers with its own transaction's time. A
/// statement Lepis cannot read is taken to.
pub fn calls_the_clock(sql: &str) -> bool {
	use pg_query::protobuf::SqlValueFunctionOp as Op;
	let Ok(parsed) = pg_query::parse(sql) else {
		return true;
	};
	let mut found = false;
	for raw in &parsed.protobuf.stmts {
		if let Some(n) = raw.stmt.as_deref() {
			walk(n, &mut |s| {
				if let Seen::Expr(NodeEnum::SqlvalueFunction(f)) = s
					&& !matches!(
						Op::try_from(f.op),
						Ok(Op::SvfopCurrentRole
							| Op::SvfopCurrentUser
							| Op::SvfopUser | Op::SvfopSessionUser
							| Op::SvfopCurrentCatalog
							| Op::SvfopCurrentSchema)
					) {
					found = true;
				}
				true
			});
		}
	}
	found
}

/// An INSERT … VALUES as one statement per node, each with only the rows (by number) that node
/// owns. RETURNING is kept; with ON CONFLICT it is refused, since a row the conflict skips would
/// leave no way to put the returned rows back in order.
pub fn split_insert(sql: &str, rows: &[Vec<usize>]) -> Result<Vec<String>, Refusal> {
	let parsed =
		pg_query::parse(sql).map_err(|e| no(format!("Lepis cannot read this INSERT: {e}"), PIN))?;
	let version = parsed.protobuf.version;
	let Some(NodeEnum::InsertStmt(ins)) = parsed
		.protobuf
		.stmts
		.first()
		.and_then(|s| s.stmt.as_deref())
		.and_then(|n| n.node.as_ref())
	else {
		return Err(no(
			"Lepis can split only an INSERT … VALUES by its rows",
			PIN,
		));
	};
	if ins.on_conflict_clause.is_some() && !ins.returning_list.is_empty() {
		return Err(no(
			"INSERT … ON CONFLICT … RETURNING with rows for several nodes is not supported",
			"Insert the rows of each shard key value in a statement of their own, or leave out RETURNING.",
		));
	}
	let Some(NodeEnum::SelectStmt(values)) =
		ins.select_stmt.as_deref().and_then(|n| n.node.as_ref())
	else {
		return Err(no(
			"Lepis can split only an INSERT … VALUES by its rows",
			PIN,
		));
	};
	let mut out = Vec::with_capacity(rows.len());
	for mine in rows {
		let mut v = values.as_ref().clone();
		v.values_lists = mine
			.iter()
			.map(|i| values.values_lists.get(*i).cloned())
			.collect::<Option<Vec<Node>>>()
			.ok_or_else(|| no("Lepis lost a row of this INSERT", PIN))?;
		let mut i = ins.as_ref().clone();
		i.select_stmt = Some(Box::new(node(NodeEnum::SelectStmt(Box::new(v)))));
		out.push(deparse_stmt(NodeEnum::InsertStmt(Box::new(i)), version)?);
	}
	Ok(out)
}

/// The plan for one statement Lepis scatters, or the refusal that says why it cannot be.
pub fn plan(sql: &str, ctx: &Context) -> Result<Planned, Refusal> {
	let parsed = pg_query::parse(sql).map_err(|e| {
		no(
			format!("Lepis cannot read this statement: {e}"),
			"Rewrite the statement without it, or pin it to one shard key value.",
		)
	})?;
	let version = parsed.protobuf.version;
	let [raw] = parsed.protobuf.stmts.as_slice() else {
		return Err(no(
			"a query that reads several nodes must be sent on its own",
			"Send it in a query string of its own, not with other statements.",
		));
	};
	let (select, explain) = match raw.stmt.as_deref().and_then(|n| n.node.as_ref()) {
		Some(NodeEnum::SelectStmt(s)) => (s.as_ref(), None),
		Some(NodeEnum::ExplainStmt(e)) => match e.query.as_deref().and_then(|n| n.node.as_ref()) {
			Some(NodeEnum::SelectStmt(s)) => {
				if !explain_format_is_text(e) {
					return Err(no(
						"EXPLAIN of a query across nodes is given in text format only",
						"Use EXPLAIN without FORMAT, or pin the query to one shard key value.",
					));
				}
				(s.as_ref(), Some(e.options.clone()))
			}
			_ => return Err(no("only a SELECT can read several nodes", PIN)),
		},
		_ => return Err(no("only a SELECT can read several nodes", PIN)),
	};

	let mut b = Builder::new(ctx, select, version);
	b.check_top()?;
	b.check_nested()?;
	let (mut merge, summary) = if b.aggregate()? {
		b.groups()?
	} else {
		b.rows()?
	};
	let needs_types = b.finish_companions(explain.is_some())?;
	merge.collation_of = b.collation_of.clone();
	let changed = b.changed;
	let formats = b.formats.clone();
	let worker = b.into_worker();
	let worker_select = deparse_stmt(NodeEnum::SelectStmt(Box::new(worker.clone())), version)?;
	let worker_sql = match explain {
		None if !changed => sql.trim().trim_end_matches(';').to_string(),
		None => worker_select.clone(),
		Some(options) => deparse_stmt(
			NodeEnum::ExplainStmt(Box::new(ExplainStmt {
				query: Some(Box::new(Node {
					node: Some(NodeEnum::SelectStmt(Box::new(worker))),
				})),
				options,
			})),
			version,
		)?,
	};
	Ok(Planned {
		worker_sql,
		formats,
		merge,
		explain: explain_is(raw),
		summary,
		needs_types,
		worker_select,
	})
}

/// The SELECT an EXPLAIN explains, as SQL: what the router describes to plan it.
pub fn explained(sql: &str) -> Option<String> {
	let parsed = pg_query::parse(sql).ok()?;
	let version = parsed.protobuf.version;
	match parsed
		.protobuf
		.stmts
		.first()?
		.stmt
		.as_deref()?
		.node
		.as_ref()?
	{
		NodeEnum::ExplainStmt(e) => match e.query.as_deref()?.node.as_ref()? {
			s @ NodeEnum::SelectStmt(_) => deparse_stmt(s.clone(), version).ok(),
			_ => None,
		},
		_ => None,
	}
}

fn explain_is(raw: &RawStmt) -> bool {
	matches!(
		raw.stmt.as_deref().and_then(|n| n.node.as_ref()),
		Some(NodeEnum::ExplainStmt(_))
	)
}

/// The decision without a database (tests/routes.rs): Ok when a scatter would be attempted.
pub fn check(sql: &str, catalog: &Catalog, search_path: &[String]) -> Result<(), Refusal> {
	let ctx = Context {
		catalog,
		search_path,
		described: None,
		aggregates: &[],
		params: &[],
		param_types: &[],
		result_formats: &[],
		col_types: None,
		collatable: &HashMap::new(),
	};
	plan(sql, &ctx).map(|_| ())
}

fn deparse_stmt(n: NodeEnum, version: i32) -> Result<String, Refusal> {
	pg_query::deparse(&ParseResult {
		version,
		stmts: vec![RawStmt {
			stmt: Some(Box::new(Node { node: Some(n) })),
			stmt_location: 0,
			stmt_len: 0,
		}],
	})
	.map_err(|e| {
		no(
			format!("Lepis could not write the query each node runs: {e}"),
			PIN,
		)
	})
}

// ---------------------------------------------------------------------------------------------
// Walking the tree.

enum Seen<'a> {
	Expr(&'a NodeEnum),
	Select(&'a SelectStmt),
}

/// Visits `n` and everything under it; `f` returning false stops the descent below that node.
fn walk<'a>(n: &'a Node, f: &mut dyn FnMut(Seen<'a>) -> bool) {
	let Some(e) = &n.node else { return };
	if let NodeEnum::SelectStmt(s) = e {
		if f(Seen::Select(s)) {
			walk_select(s, f);
		}
		return;
	}
	if !f(Seen::Expr(e)) {
		return;
	}
	for c in children(e) {
		walk(c, f);
	}
}

/// Visits the clauses of one SELECT (and its set operation's arms, as SELECTs).
fn walk_select<'a>(s: &'a SelectStmt, f: &mut dyn FnMut(Seen<'a>) -> bool) {
	if let Some(w) = &s.with_clause {
		for c in &w.ctes {
			walk(c, f);
		}
	}
	for n in s
		.target_list
		.iter()
		.chain(&s.from_clause)
		.chain(s.where_clause.as_deref())
		.chain(&s.group_clause)
		.chain(s.having_clause.as_deref())
		.chain(&s.window_clause)
		.chain(&s.distinct_clause)
		.chain(&s.values_lists)
		.chain(&s.sort_clause)
		.chain(s.limit_offset.as_deref())
		.chain(s.limit_count.as_deref())
	{
		walk(n, f);
	}
	for arm in [&s.larg, &s.rarg].into_iter().flatten() {
		if f(Seen::Select(arm)) {
			walk_select(arm, f);
		}
	}
}

/// The child nodes of an expression or FROM item (the node kinds analyze.rs reads).
fn children(e: &NodeEnum) -> Vec<&Node> {
	use NodeEnum as N;
	fn o(n: &Option<Box<Node>>) -> Option<&Node> {
		n.as_deref()
	}
	match e {
		N::ResTarget(r) => o(&r.val).into_iter().chain(&r.indirection).collect(),
		N::AExpr(a) => o(&a.lexpr).into_iter().chain(o(&a.rexpr)).collect(),
		N::BoolExpr(b) => b.args.iter().collect(),
		N::NullTest(t) => o(&t.arg).into_iter().collect(),
		N::BooleanTest(t) => o(&t.arg).into_iter().collect(),
		N::TypeCast(t) => o(&t.arg).into_iter().collect(),
		N::CollateClause(c) => o(&c.arg).into_iter().collect(),
		N::NamedArgExpr(a) => o(&a.arg).into_iter().collect(),
		N::CaseExpr(c) => o(&c.arg)
			.into_iter()
			.chain(&c.args)
			.chain(o(&c.defresult))
			.collect(),
		N::CaseWhen(w) => o(&w.expr).into_iter().chain(o(&w.result)).collect(),
		N::CoalesceExpr(c) => c.args.iter().collect(),
		N::MinMaxExpr(m) => m.args.iter().collect(),
		N::RowExpr(r) => r.args.iter().collect(),
		N::AArrayExpr(a) => a.elements.iter().collect(),
		N::List(l) => l.items.iter().collect(),
		N::AIndirection(a) => o(&a.arg).into_iter().chain(&a.indirection).collect(),
		N::AIndices(a) => o(&a.lidx).into_iter().chain(o(&a.uidx)).collect(),
		N::MultiAssignRef(m) => o(&m.source).into_iter().collect(),
		N::SortBy(s) => o(&s.node).into_iter().collect(),
		N::WindowDef(w) => w
			.partition_clause
			.iter()
			.chain(&w.order_clause)
			.chain(o(&w.start_offset))
			.chain(o(&w.end_offset))
			.collect(),
		N::GroupingSet(g) => g.content.iter().collect(),
		N::FuncCall(f) => {
			let mut v: Vec<&Node> = f
				.args
				.iter()
				.chain(&f.agg_order)
				.chain(o(&f.agg_filter))
				.collect();
			if let Some(w) = &f.over {
				v.extend(
					w.partition_clause
						.iter()
						.chain(&w.order_clause)
						.chain(o(&w.start_offset))
						.chain(o(&w.end_offset)),
				);
			}
			v
		}
		N::SubLink(s) => o(&s.testexpr).into_iter().chain(o(&s.subselect)).collect(),
		N::JoinExpr(j) => o(&j.larg)
			.into_iter()
			.chain(o(&j.rarg))
			.chain(o(&j.quals))
			.collect(),
		N::RangeSubselect(r) => o(&r.subquery).into_iter().collect(),
		N::RangeFunction(r) => r.functions.iter().collect(),
		N::RangeTableSample(t) => o(&t.relation)
			.into_iter()
			.chain(&t.args)
			.chain(o(&t.repeatable))
			.collect(),
		N::RangeTableFunc(t) => o(&t.docexpr)
			.into_iter()
			.chain(o(&t.rowexpr))
			.chain(&t.columns)
			.collect(),
		N::RangeTableFuncCol(c) => o(&c.colexpr).into_iter().chain(o(&c.coldefexpr)).collect(),
		N::CommonTableExpr(c) => o(&c.ctequery).into_iter().collect(),
		// The writes, for what a statement calls (`functions`, `calls_the_clock`).
		N::InsertStmt(i) => {
			let mut v: Vec<&Node> = i.with_clause.iter().flat_map(|w| &w.ctes).collect();
			v.extend(o(&i.select_stmt));
			v.extend(&i.returning_list);
			if let Some(c) = &i.on_conflict_clause {
				v.extend(&c.target_list);
				v.extend(o(&c.where_clause));
			}
			v
		}
		N::UpdateStmt(u) => u
			.with_clause
			.iter()
			.flat_map(|w| &w.ctes)
			.chain(&u.target_list)
			.chain(o(&u.where_clause))
			.chain(&u.from_clause)
			.chain(&u.returning_list)
			.collect(),
		N::DeleteStmt(d) => d
			.with_clause
			.iter()
			.flat_map(|w| &w.ctes)
			.chain(&d.using_clause)
			.chain(o(&d.where_clause))
			.chain(&d.returning_list)
			.collect(),
		N::XmlExpr(x) => x.named_args.iter().chain(&x.args).collect(),
		N::XmlSerialize(x) => o(&x.expr).into_iter().collect(),
		N::IndexElem(i) => o(&i.expr).into_iter().collect(),
		N::JsonIsPredicate(j) => o(&j.expr).into_iter().collect(),
		N::JsonValueExpr(j) => o(&j.raw_expr).into_iter().collect(),
		N::JsonScalarExpr(j) => o(&j.expr).into_iter().collect(),
		N::JsonObjectConstructor(j) => j.exprs.iter().collect(),
		N::JsonArrayConstructor(j) => j.exprs.iter().collect(),
		N::JsonArrayQueryConstructor(j) => o(&j.query).into_iter().collect(),
		_ => Vec::new(),
	}
}

/// Walks one level: every node of `n` except what is inside a nested SELECT, whose roots are
/// pushed to `nested`.
fn level<'a>(n: &'a Node, out: &mut Vec<&'a NodeEnum>, nested: &mut Vec<&'a SelectStmt>) {
	walk(n, &mut |s| match s {
		Seen::Expr(e) => {
			out.push(e);
			true
		}
		Seen::Select(sel) => {
			nested.push(sel);
			false
		}
	});
}

/// One SELECT's own nodes and the SELECTs nested directly in it.
fn select_level(s: &SelectStmt) -> (Vec<&NodeEnum>, Vec<&SelectStmt>) {
	let mut out = Vec::new();
	let mut nested = Vec::new();
	walk_select(s, &mut |seen| match seen {
		Seen::Expr(e) => {
			out.push(e);
			true
		}
		Seen::Select(sel) => {
			nested.push(sel);
			false
		}
	});
	(out, nested)
}

/// (schema, name) of a function, lower-cased as Postgres folds unquoted names.
fn func_name(f: &FuncCall) -> Option<(Option<String>, String)> {
	let parts: Vec<&str> = f
		.funcname
		.iter()
		.map(|n| match &n.node {
			Some(NodeEnum::String(s)) => Some(s.sval.as_str()),
			_ => None,
		})
		.collect::<Option<_>>()?;
	match parts.as_slice() {
		[name] => Some((None, name.to_string())),
		[schema, name] => Some((Some(schema.to_string()), name.to_string())),
		_ => None,
	}
}

fn column_name(n: &Node) -> Option<Vec<&str>> {
	match &n.node {
		Some(NodeEnum::ColumnRef(ColumnRef { fields, .. })) => fields
			.iter()
			.map(|f| match &f.node {
				Some(NodeEnum::String(s)) => Some(s.sval.as_str()),
				_ => None,
			})
			.collect(),
		_ => None,
	}
}

fn int_const(n: &Node) -> Option<i64> {
	match &n.node {
		Some(NodeEnum::AConst(AConst {
			val: Some(a_const::Val::Ival(i)),
			..
		})) => Some(i.ival as i64),
		_ => None,
	}
}

fn node(e: NodeEnum) -> Node {
	Node { node: Some(e) }
}

fn null_const() -> Node {
	node(NodeEnum::AConst(AConst {
		isnull: true,
		location: -1,
		val: None,
	}))
}

fn number_const(v: u64) -> Node {
	let val = match i32::try_from(v) {
		Ok(i) => a_const::Val::Ival(pg_query::protobuf::Integer { ival: i }),
		Err(_) => a_const::Val::Fval(pg_query::protobuf::Float {
			fval: v.to_string(),
		}),
	};
	node(NodeEnum::AConst(AConst {
		isnull: false,
		location: -1,
		val: Some(val),
	}))
}

fn string_node(s: &str) -> Node {
	node(NodeEnum::String(pg_query::protobuf::String {
		sval: s.into(),
	}))
}

fn call(schema_name: &[&str], args: Vec<Node>) -> Node {
	node(NodeEnum::FuncCall(Box::new(FuncCall {
		funcname: schema_name.iter().map(|s| string_node(s)).collect(),
		args,
		funcformat: pg_query::protobuf::CoercionForm::CoerceExplicitCall as i32,
		location: -1,
		..Default::default()
	})))
}

// ---------------------------------------------------------------------------------------------
// The builder.

struct Builder<'a> {
	ctx: &'a Context<'a>,
	top: &'a SelectStmt,
	version: i32,
	targets: Vec<Node>,
	group: Vec<Node>,
	/// The client's columns: the first `visible` of the worker's.
	visible: usize,
	formats: Vec<i16>,
	collation_of: Vec<Option<usize>>,
	/// The expression behind each worker column, when Lepis knows it.
	col_expr: Vec<Option<Node>>,
	/// Hidden columns by their deparsed text and format, so each is added once.
	hidden: HashMap<(String, i16), usize>,
	changed: bool,
	limit: Option<u64>,
	offset: u64,
	push_limit: bool,
	grouped: bool,
	/// Compared columns that may need a collation companion.
	compared: BTreeSet<usize>,
}

impl<'a> Builder<'a> {
	fn new(ctx: &'a Context<'a>, top: &'a SelectStmt, version: i32) -> Builder<'a> {
		let has_star = top.target_list.iter().any(|t| {
			matches!(&t.node, Some(NodeEnum::ResTarget(r))
				if matches!(r.val.as_deref().and_then(|v| v.node.as_ref()),
					Some(NodeEnum::ColumnRef(c)) if c.fields.iter().any(|f| matches!(f.node, Some(NodeEnum::AStar(_))))))
		});
		let visible = match ctx.described {
			Some(d) => d.names.len(),
			None => top.target_list.len(),
		};
		let col_expr = (0..visible)
			.map(|i| {
				if has_star {
					// `*` expands to columns Lepis cannot name from the tree: by the names Describe
					// gave, which is what they are (an ambiguous one is an error from the node).
					ctx.described.and_then(|d| d.names.get(i)).map(|n| {
						node(NodeEnum::ColumnRef(ColumnRef {
							fields: vec![string_node(n)],
							location: -1,
						}))
					})
				} else {
					match top.target_list.get(i).and_then(|t| t.node.as_ref()) {
						Some(NodeEnum::ResTarget(r)) => r.val.as_deref().cloned(),
						_ => None,
					}
				}
			})
			.collect();
		Builder {
			ctx,
			top,
			version,
			targets: top.target_list.clone(),
			group: top.group_clause.clone(),
			visible,
			formats: (0..visible).map(|i| ctx.format(i)).collect(),
			collation_of: vec![None; visible],
			col_expr,
			hidden: HashMap::new(),
			changed: false,
			limit: None,
			offset: 0,
			push_limit: false,
			grouped: false,
			compared: BTreeSet::new(),
		}
	}

	fn type_of(&self, col: usize) -> Option<u32> {
		if col < self.visible
			&& let Some(t) = self.ctx.described.and_then(|d| d.types.get(col))
		{
			return Some(*t);
		}
		self.ctx.col_types.and_then(|t| t.get(col)).copied()
	}

	fn deparse_expr(&self, n: &Node) -> Result<String, Refusal> {
		deparse_stmt(
			NodeEnum::SelectStmt(Box::new(SelectStmt {
				target_list: vec![node(NodeEnum::ResTarget(Box::new(ResTarget {
					val: Some(Box::new(n.clone())),
					..Default::default()
				})))],
				limit_option: LimitOption::Default as i32,
				op: SetOperation::SetopNone as i32,
				..Default::default()
			})),
			self.version,
		)
	}

	/// A hidden worker column computing `expr`, fetched in `format`.
	fn hidden(&mut self, expr: &Node, format: i16) -> Result<usize, Refusal> {
		let key = (self.deparse_expr(expr)?, format);
		if let Some(c) = self.hidden.get(&key) {
			return Ok(*c);
		}
		// After the client's columns and the hidden ones before it.
		let col = self.formats.len();
		self.targets
			.push(node(NodeEnum::ResTarget(Box::new(ResTarget {
				val: Some(Box::new(expr.clone())),
				..Default::default()
			}))));
		self.formats.push(format);
		self.collation_of.push(None);
		self.col_expr.push(Some(expr.clone()));
		self.hidden.insert(key, col);
		self.changed = true;
		Ok(col)
	}

	/// Asks each node for the collation of a compared column whose type Lepis does not order
	/// itself.
	/// (Recorded here, added by `finish_companions` after every other column, so the worker's
	/// other columns keep their places whichever companions it gets.)
	fn companion(&mut self, col: usize) -> Result<(), Refusal> {
		self.compared.insert(col);
		Ok(())
	}

	/// Adds the collation companions of the compared columns whose type has collations; returns
	/// whether some column's type is not known yet.
	fn finish_companions(&mut self, explain: bool) -> Result<bool, Refusal> {
		if explain {
			return Ok(false);
		}
		let mut unknown = false;
		for col in std::mem::take(&mut self.compared) {
			let collatable = match self.type_of(col) {
				Some(t) if is_native(t) => Some(false),
				// text, varchar, char(n), name
				Some(25 | 1043 | 1042 | 19) => Some(true),
				// Every other built-in type is without collations.
				Some(t) if t < 16384 => Some(false),
				Some(t) => self.ctx.collatable.get(&t).copied(),
				None => None,
			};
			match collatable {
				Some(false) => {}
				None => unknown = true,
				Some(true) => {
					let Some(expr) = self.col_expr.get(col).cloned().flatten() else {
						continue;
					};
					let c =
						self.hidden(&call(&["pg_catalog", "pg_collation_for"], vec![expr]), 0)?;
					self.collation_of[col] = Some(c);
				}
			}
		}
		Ok(unknown)
	}

	fn is_agg(&self, f: &FuncCall) -> bool {
		f.over.is_none()
			&& (analyze::is_aggregate(f)
				|| func_name(f).is_some_and(|(_, n)| self.ctx.aggregates.contains(&n)))
	}

	/// Whether `n` has an aggregate (or window function) of THIS level.
	fn has(&self, n: &Node, window: bool) -> bool {
		let mut found = false;
		let mut out = Vec::new();
		let mut nested = Vec::new();
		level(n, &mut out, &mut nested);
		for e in out {
			if let NodeEnum::FuncCall(f) = e
				&& ((window && f.over.is_some()) || (!window && self.is_agg(f)))
			{
				found = true;
			}
		}
		found
	}

	fn check_top(&self) -> Result<(), Refusal> {
		let s = self.top;
		if !matches!(
			SetOperation::try_from(s.op),
			Ok(SetOperation::SetopNone) | Err(_)
		) || s.larg.is_some()
		{
			return Err(no(
				"UNION, INTERSECT and EXCEPT across several nodes are not supported yet",
				"Run each side as a query of its own, or pin the query to one shard key value.",
			));
		}
		if !s.locking_clause.is_empty() {
			return Err(no(
				"row locks (FOR UPDATE, FOR SHARE) across several nodes are not supported yet",
				"Lock the rows of one shard key value at a time.",
			));
		}
		if s.distinct_clause.iter().any(|d| d.node.is_some()) {
			return Err(no(
				"SELECT DISTINCT ON across several nodes is not supported yet",
				"Use DISTINCT or GROUP BY, or pin the query to one shard key value.",
			));
		}
		if s.group_distinct
			|| s.group_clause
				.iter()
				.any(|g| matches!(g.node, Some(NodeEnum::GroupingSet(_))))
		{
			return Err(no(
				"GROUPING SETS, ROLLUP and CUBE across several nodes are not supported yet",
				"Run one GROUP BY per grouping, or pin the query to one shard key value.",
			));
		}
		if !s.window_clause.is_empty()
			|| s.target_list
				.iter()
				.chain(&s.sort_clause)
				.chain(s.having_clause.as_deref())
				.any(|n| self.has(n, true))
		{
			return Err(no(
				"a window function across several nodes would see only each node's rows",
				"Compute it over one shard key value (pin the query), or in the application over the rows Lepis returns.",
			));
		}
		if s.limit_option == LimitOption::WithTies as i32 {
			return Err(no(
				"FETCH … WITH TIES across several nodes is not supported yet",
				"Use LIMIT, or pin the query to one shard key value.",
			));
		}
		Ok(())
	}

	/// Every SELECT nested in the statement that reads a sharded table must give the same rows
	/// computed on each node's share as on the whole table.
	fn check_nested(&self) -> Result<(), Refusal> {
		let mut ctes = BTreeSet::new();
		let top = node(NodeEnum::SelectStmt(Box::new(self.top.clone())));
		walk(&top, &mut |s| {
			if let Seen::Expr(NodeEnum::CommonTableExpr(c)) = s {
				ctes.insert(c.ctename.clone());
			}
			true
		});
		let (_, nested) = select_level(self.top);
		for l in nested {
			self.check_level(l, &ctes)?;
		}
		Ok(())
	}

	fn check_level(&self, l: &SelectStmt, ctes: &BTreeSet<String>) -> Result<(), Refusal> {
		let (own, nested) = select_level(l);
		if self.reads_sharded(l, ctes) {
			let what = if l.larg.is_some() {
				Some("a UNION, INTERSECT or EXCEPT")
			} else if !l.distinct_clause.is_empty() {
				Some("DISTINCT")
			} else if l.limit_count.is_some() || l.limit_offset.is_some() {
				Some("LIMIT or OFFSET")
			} else if own
				.iter()
				.any(|e| matches!(e, NodeEnum::FuncCall(f) if f.over.is_some()))
				|| !l.window_clause.is_empty()
			{
				Some("a window function")
			} else if (own
				.iter()
				.any(|e| matches!(e, NodeEnum::FuncCall(f) if self.is_agg(f)))
				|| !l.group_clause.is_empty()
				|| l.having_clause.is_some())
				&& !self.groups_by_key(l)
			{
				Some("an aggregate or GROUP BY")
			} else {
				None
			};
			if let Some(what) = what {
				return Err(no(
					format!(
						"a subquery with {what} over a sharded table would be computed on each node's rows separately"
					),
					"Group the subquery by the shard key, make the table it reads a reference table, or pin the query to one shard key value.",
				));
			}
		}
		for n in nested {
			self.check_level(n, ctes)?;
		}
		Ok(())
	}

	fn resolve(&self, schema: &str, table: &str) -> RelationName {
		if !schema.is_empty() {
			return RelationName {
				schema: schema.into(),
				table: table.into(),
			};
		}
		for s in self.ctx.search_path {
			let n = RelationName {
				schema: s.clone(),
				table: table.into(),
			};
			if self.ctx.catalog.relations.contains_key(&n) {
				return n;
			}
		}
		RelationName {
			schema: self.ctx.search_path.first().cloned().unwrap_or_default(),
			table: table.into(),
		}
	}

	fn sharded_key(&self, schema: &str, table: &str) -> Option<String> {
		match self.ctx.catalog.relations.get(&self.resolve(schema, table)) {
			Some(RelationKind::Sharded { key_column, .. }) => Some(key_column.clone()),
			_ => None,
		}
	}

	/// Whether a SELECT reads a sharded table, or a CTE (which might), anywhere inside it.
	fn reads_sharded(&self, l: &SelectStmt, ctes: &BTreeSet<String>) -> bool {
		let mut found = false;
		let n = node(NodeEnum::SelectStmt(Box::new(l.clone())));
		walk(&n, &mut |s| {
			if let Seen::Expr(NodeEnum::RangeVar(rv)) = s
				&& ((rv.schemaname.is_empty() && ctes.contains(&rv.relname))
					|| self.sharded_key(&rv.schemaname, &rv.relname).is_some())
			{
				found = true;
			}
			!found
		});
		found
	}

	/// A SELECT whose GROUP BY names the shard key of every sharded table in its FROM: each of
	/// its groups lives on one node, so each node computes whole groups.
	fn groups_by_key(&self, l: &SelectStmt) -> bool {
		let mut tables: Vec<(String, String)> = Vec::new();
		let mut other = false;
		fn from<'n>(
			n: &'n Node,
			out: &mut Vec<&'n pg_query::protobuf::RangeVar>,
			other: &mut bool,
		) {
			match &n.node {
				Some(NodeEnum::RangeVar(rv)) => out.push(rv),
				Some(NodeEnum::JoinExpr(j)) => {
					for s in [&j.larg, &j.rarg].into_iter().flatten() {
						from(s, out, other);
					}
				}
				Some(NodeEnum::RangeSubselect(_)) => *other = true,
				_ => {}
			}
		}
		let mut rvs = Vec::new();
		for f in &l.from_clause {
			from(f, &mut rvs, &mut other);
		}
		for rv in rvs {
			if let Some(key) = self.sharded_key(&rv.schemaname, &rv.relname) {
				let refname = rv
					.alias
					.as_ref()
					.map(|a| a.aliasname.clone())
					.unwrap_or_else(|| rv.relname.clone());
				tables.push((refname, key));
			}
		}
		!tables.is_empty()
			&& !other && tables.iter().all(|(refname, key)| {
			l.group_clause.iter().any(|g| {
				matches!(column_name(g).as_deref(), Some([k]) if k == key)
					|| matches!(column_name(g).as_deref(), Some([r, k]) if r == refname && k == key)
			})
		})
	}

	fn aggregate(&self) -> Result<bool, Refusal> {
		let s = self.top;
		Ok(!s.group_clause.is_empty()
			|| s.having_clause.is_some()
			|| s.target_list
				.iter()
				.chain(&s.sort_clause)
				.any(|n| self.has(n, false)))
	}

	// -----------------------------------------------------------------------------------------
	// LIMIT and OFFSET.

	fn count_value(&self, n: Option<&Node>, what: &str) -> Result<Option<i64>, Refusal> {
		let Some(n) = n else { return Ok(None) };
		let bad = || {
			no(
				format!("{what} across several nodes must be a number or a parameter"),
				format!("Write {what} as a number, or bind it as a parameter."),
			)
		};
		let parse = |s: &str| -> Result<i64, Refusal> {
			match s.trim().parse::<i64>() {
				Ok(v) => Ok(v),
				Err(_) => merge::Dec::parse(s)
					.and_then(|d| d.to_i64())
					.ok_or_else(bad),
			}
		};
		match &n.node {
			Some(NodeEnum::AConst(c)) if c.isnull => Ok(None),
			Some(NodeEnum::AConst(c)) => match &c.val {
				Some(a_const::Val::Ival(i)) => Ok(Some(i.ival as i64)),
				Some(a_const::Val::Fval(f)) => parse(&f.fval).map(Some),
				Some(a_const::Val::Sval(s)) => parse(&s.sval).map(Some),
				_ => Err(bad()),
			},
			Some(NodeEnum::TypeCast(t)) => self.count_value(t.arg.as_deref(), what),
			Some(NodeEnum::AExpr(a))
				if a.lexpr.is_none()
					&& matches!(AExprKind::try_from(a.kind), Ok(AExprKind::AexprOp))
					&& op_name(&a.name).as_deref() == Some("-") =>
			{
				Ok(self.count_value(a.rexpr.as_deref(), what)?.map(|v| -v))
			}
			Some(NodeEnum::ParamRef(p)) => {
				if self.ctx.described.is_none() {
					return Ok(None);
				}
				let i = (p.number as usize).wrapping_sub(1);
				match self.ctx.params.get(i).map(ParamValue::value) {
					None | Some(ParamValue::Null | ParamValue::Declared(..)) => Ok(None),
					Some(ParamValue::Text(s)) => parse(s).map(Some),
					Some(ParamValue::Binary(b)) => match b.len() {
						8 => Ok(Some(i64::from_be_bytes(
							b[..].try_into().map_err(|_| bad())?,
						))),
						4 => Ok(Some(
							i32::from_be_bytes(b[..].try_into().map_err(|_| bad())?) as i64,
						)),
						2 => Ok(Some(
							i16::from_be_bytes(b[..].try_into().map_err(|_| bad())?) as i64,
						)),
						_ => Err(bad()),
					},
				}
			}
			_ => Err(bad()),
		}
	}

	fn limits(&mut self) -> Result<(), Refusal> {
		let offset = self.count_value(self.top.limit_offset.as_deref(), "OFFSET")?;
		let limit = self.count_value(self.top.limit_count.as_deref(), "LIMIT")?;
		if offset.is_some_and(|o| o < 0) {
			return Err(Refusal {
				code: "2201X",
				message: "OFFSET must not be negative".into(),
				hint: String::new(),
			});
		}
		if limit.is_some_and(|l| l < 0) {
			return Err(Refusal {
				code: "2201W",
				message: "LIMIT must not be negative".into(),
				hint: String::new(),
			});
		}
		self.offset = offset.unwrap_or(0) as u64;
		self.limit = limit.map(|l| l as u64);
		if self.top.limit_count.is_some() || self.top.limit_offset.is_some() {
			self.changed = true;
		}
		Ok(())
	}

	fn limit_summary(&self) -> String {
		match (self.limit, self.offset) {
			(None, 0) => String::new(),
			(None, o) => format!("; OFFSET {o} in Lepis"),
			(Some(l), o) if self.push_limit => {
				format!(
					"; LIMIT {l} OFFSET {o} in Lepis, each node LIMIT {}",
					l.saturating_add(o)
				)
			}
			(Some(l), o) => format!("; LIMIT {l} OFFSET {o} in Lepis"),
		}
	}

	// -----------------------------------------------------------------------------------------
	// Rows.

	/// The worker column an ORDER BY item names: a position, an output column's name, or else
	/// an expression over the input (a hidden column).
	fn sort_column(&mut self, n: &Node) -> Result<usize, Refusal> {
		if let Some(k) = int_const(n) {
			if k >= 1 && (k as usize) <= self.visible {
				return Ok(k as usize - 1);
			}
			return Err(no(
				format!("ORDER BY position {k} is not in the select list"),
				"",
			));
		}
		if let Some([name]) = column_name(n).as_deref()
			&& let Some(i) = self.output_named(name)
		{
			return Ok(i);
		}
		self.hidden(n, 0)
	}

	/// An output column called `name` (ORDER BY's SQL92 rule: an output name wins).
	fn output_named(&self, name: &str) -> Option<usize> {
		match self.ctx.described {
			Some(d) => d.names.iter().position(|n| n == name),
			None => self.top.target_list.iter().position(|t| {
				matches!(&t.node, Some(NodeEnum::ResTarget(r))
					if r.name == name
						|| (r.name.is_empty()
							&& r.val.as_deref().and_then(column_name).and_then(|c| c.last().copied()) == Some(name)))
			}),
		}
	}

	fn sort_by(n: &Node) -> Result<(&Node, bool, bool), Refusal> {
		let Some(NodeEnum::SortBy(s)) = &n.node else {
			return Err(no("Lepis cannot read this ORDER BY item", PIN));
		};
		let SortBy {
			node: Some(inner),
			sortby_dir,
			sortby_nulls,
			..
		} = s.as_ref()
		else {
			return Err(no("Lepis cannot read this ORDER BY item", PIN));
		};
		let desc = match SortByDir::try_from(*sortby_dir) {
			Ok(SortByDir::SortbyDesc) => true,
			Ok(SortByDir::SortbyUsing) => {
				return Err(no(
					"ORDER BY … USING an operator across several nodes is not supported yet",
					"Use ASC or DESC, or pin the query to one shard key value.",
				));
			}
			_ => false,
		};
		let nulls_first = match SortByNulls::try_from(*sortby_nulls) {
			Ok(SortByNulls::SortbyNullsFirst) => true,
			Ok(SortByNulls::SortbyNullsLast) => false,
			_ => desc,
		};
		Ok((inner, desc, nulls_first))
	}

	fn rows(&mut self) -> Result<(MergePlan, String), Refusal> {
		self.limits()?;
		let mut order = Vec::new();
		for item in self.top.sort_clause.clone() {
			let (n, desc, nulls_first) = Self::sort_by(&item)?;
			let col = self.sort_column(n)?;
			self.companion(col)?;
			order.push(SortKey {
				expr: Expr::Col(col),
				desc,
				nulls_first,
			});
		}
		let distinct = !self.top.distinct_clause.is_empty();
		if distinct {
			for c in 0..self.visible {
				self.companion(c)?;
			}
		}
		self.push_limit = self.limit.is_some();
		let sorted = !order.is_empty();
		let mut summary = String::from("rows from every node");
		if sorted {
			summary.push_str(&format!(", merged in order of {} key(s)", order.len()));
		}
		if distinct {
			summary.push_str(", duplicates removed in Lepis");
		}
		summary.push_str(&self.limit_summary());
		Ok((
			MergePlan {
				visible: self.visible,
				collation_of: self.collation_of.clone(),
				kind: MergeKind::Rows { sorted },
				distinct,
				order,
				offset: self.offset,
				limit: self.limit,
			},
			summary,
		))
	}

	// -----------------------------------------------------------------------------------------
	// Groups.

	fn groups(&mut self) -> Result<(MergePlan, String), Refusal> {
		self.limits()?;
		self.grouped = true;
		let top = self.top;
		if self
			.ctx
			.described
			.is_some_and(|d| d.names.len() != top.target_list.len())
		{
			return Err(no(
				"a * in the select list of an aggregate query across several nodes is not supported yet",
				"Name the columns, or pin the query to one shard key value.",
			));
		}
		let mut st = GroupState::default();
		// The client's columns keep their places, so GROUP BY 1 still means the first; one that
		// holds an aggregate is computed here, and the node sends NULL in its place.
		let mut outputs = Vec::new();
		for i in 0..top.target_list.len() {
			let Some(NodeEnum::ResTarget(r)) = top.target_list[i].node.as_ref() else {
				return Err(no("Lepis cannot read this select list", PIN));
			};
			let Some(val) = r.val.as_deref() else {
				return Err(no("Lepis cannot read this select list", PIN));
			};
			if self.has(val, false) {
				let mut placeholder = r.as_ref().clone();
				placeholder.val = Some(Box::new(null_const()));
				placeholder.indirection.clear();
				self.targets[i] = node(NodeEnum::ResTarget(Box::new(placeholder)));
				self.formats[i] = 0;
				self.changed = true;
				let format = self.ctx.format(i);
				outputs.push(self.compile(val, Some(format), &mut st)?);
			} else {
				outputs.push(Expr::Col(i));
			}
		}
		let mut keys = Vec::new();
		for g in top.group_clause.clone() {
			let col = if let Some(k) = int_const(&g) {
				if k < 1 || k as usize > self.visible {
					return Err(no(
						format!("GROUP BY position {k} is not in the select list"),
						"",
					));
				}
				k as usize - 1
			} else {
				let same = column_name(&g).and_then(|g| {
					top.target_list.iter().position(|t| {
						matches!(&t.node, Some(NodeEnum::ResTarget(r))
							if (r.name.is_empty() || Some(r.name.as_str()) == g.last().copied())
								&& r.val.as_deref().and_then(column_name) == Some(g.clone()))
					})
				});
				if let Some(i) = same.filter(|i| *i < self.visible) {
					self.companion(i)?;
					keys.push(i);
					continue;
				}
				if let Some([name]) = column_name(&g).as_deref() {
					let aliased = top.target_list.iter().any(|t| {
						matches!(&t.node, Some(NodeEnum::ResTarget(r))
							if r.name == *name
								&& r.val.as_deref().and_then(column_name).as_deref() != Some(&[*name][..]))
					});
					if aliased {
						return Err(no(
							format!(
								"GROUP BY {name} names an output column, which across several nodes is not supported yet"
							),
							"GROUP BY the expression itself, or its position (GROUP BY 1).",
						));
					}
				}
				self.hidden(&g, 0)?
			};
			self.companion(col)?;
			keys.push(col);
		}
		let having = match top.having_clause.as_deref() {
			Some(h) => Some(self.compile(h, None, &mut st)?),
			None => None,
		};
		let mut order = Vec::new();
		for item in top.sort_clause.clone() {
			let (n, desc, nulls_first) = Self::sort_by(&item)?;
			let expr = if let Some(k) = int_const(n) {
				if k < 1 || k as usize > outputs.len() {
					return Err(no(
						format!("ORDER BY position {k} is not in the select list"),
						"",
					));
				}
				outputs[k as usize - 1].clone()
			} else if let Some([name]) = column_name(n).as_deref()
				&& let Some(i) = self.output_named(name)
			{
				outputs[i].clone()
			} else {
				self.compile(n, None, &mut st)?
			};
			self.compare(&expr)?;
			order.push(SortKey {
				expr,
				desc,
				nulls_first,
			});
		}
		let distinct = !top.distinct_clause.is_empty();
		if distinct {
			for e in outputs.clone() {
				self.compare(&e)?;
			}
		}
		self.group.extend(st.distinct_args);
		self.changed = true;
		let summary = format!(
			"partial aggregates on every node ({} group key(s), {} aggregate(s)), finished in Lepis{}{}",
			keys.len(),
			st.aggs.len(),
			if having.is_some() { " with HAVING" } else { "" },
			self.limit_summary()
		);
		Ok((
			MergePlan {
				visible: self.visible,
				collation_of: self.collation_of.clone(),
				kind: MergeKind::Groups(Groups {
					keys,
					aggs: st.aggs,
					outputs,
					having,
					one_group: top.group_clause.is_empty(),
				}),
				distinct,
				order,
				offset: self.offset,
				limit: self.limit,
			},
			summary,
		))
	}

	/// The columns an expression compares get their collation companions.
	fn compare(&mut self, e: &Expr) -> Result<(), Refusal> {
		if let Expr::Col(c) = e {
			self.companion(*c)?;
		}
		Ok(())
	}

	/// An expression after aggregation, as merge.rs evaluates it. `raw` is the client's format
	/// when this is a whole output column (a bare min / max is passed on as the node sent it).
	fn compile(
		&mut self,
		n: &Node,
		raw: Option<i16>,
		st: &mut GroupState,
	) -> Result<Expr, Refusal> {
		let Some(e) = &n.node else {
			return Ok(Expr::Null);
		};
		if let NodeEnum::FuncCall(f) = e
			&& self.is_agg(f)
		{
			return Ok(Expr::Agg(self.agg(f, raw, st)?));
		}
		if !self.has(n, false) {
			return match e {
				NodeEnum::AConst(c) if c.isnull => Ok(Expr::Null),
				NodeEnum::AConst(AConst {
					val: Some(a_const::Val::Ival(i)),
					..
				}) => Ok(Expr::Int(i.ival as i128)),
				NodeEnum::AConst(AConst {
					val: Some(a_const::Val::Fval(f)),
					..
				}) => Ok(Expr::Num(f.fval.clone())),
				NodeEnum::ParamRef(p) => self.param(p.number),
				_ => Ok(Expr::Col(self.hidden(n, 0)?)),
			};
		}
		let unsupported = || {
			no(
				"Lepis cannot compute this expression over aggregates across several nodes yet",
				"Select the aggregates themselves and compute the rest in the application, or pin the query to one shard key value.",
			)
		};
		let mut sub = |x: Option<&Node>, me: &mut Self| -> Result<Box<Expr>, Refusal> {
			match x {
				Some(x) => Ok(Box::new(me.compile(x, None, st)?)),
				None => Err(unsupported()),
			}
		};
		match e {
			NodeEnum::AExpr(a) if matches!(AExprKind::try_from(a.kind), Ok(AExprKind::AexprOp)) => {
				let op = match op_name(&a.name).as_deref() {
					Some("=") => CmpOp::Eq,
					Some("<>" | "!=") => CmpOp::Ne,
					Some("<") => CmpOp::Lt,
					Some("<=") => CmpOp::Le,
					Some(">") => CmpOp::Gt,
					Some(">=") => CmpOp::Ge,
					_ => return Err(unsupported()),
				};
				let l = sub(a.lexpr.as_deref(), self)?;
				let r = sub(a.rexpr.as_deref(), self)?;
				Ok(Expr::Cmp(op, l, r))
			}
			NodeEnum::BoolExpr(b) => {
				let args = b
					.args
					.iter()
					.map(|x| self.compile(x, None, st))
					.collect::<Result<Vec<_>, _>>()?;
				match BoolExprType::try_from(b.boolop) {
					Ok(BoolExprType::AndExpr) => Ok(Expr::And(args)),
					Ok(BoolExprType::OrExpr) => Ok(Expr::Or(args)),
					Ok(BoolExprType::NotExpr) if args.len() == 1 => {
						Ok(Expr::Not(Box::new(args.into_iter().next().expect("one"))))
					}
					_ => Err(unsupported()),
				}
			}
			NodeEnum::NullTest(t) => {
				let negated = matches!(
					NullTestType::try_from(t.nulltesttype),
					Ok(NullTestType::IsNotNull)
				);
				Ok(Expr::IsNull(sub(t.arg.as_deref(), self)?, negated))
			}
			NodeEnum::TypeCast(t) => {
				let Some(ty) = &t.type_name else {
					return Err(unsupported());
				};
				let names: Vec<&str> = ty
					.names
					.iter()
					.filter_map(|n| match &n.node {
						Some(NodeEnum::String(s)) => Some(s.sval.as_str()),
						_ => None,
					})
					.collect();
				if !ty.typmods.is_empty() || !ty.array_bounds.is_empty() {
					return Err(unsupported());
				}
				let arg = sub(t.arg.as_deref(), self)?;
				match names.as_slice() {
					["pg_catalog", "numeric"] | ["numeric"] | ["decimal"] => Ok(Expr::Numeric(arg)),
					["pg_catalog", "int8"] | ["int8"] | ["bigint"] => Ok(Expr::Int8(arg)),
					_ => Err(unsupported()),
				}
			}
			NodeEnum::FuncCall(f)
				if matches!(func_name(f), Some((None | Some(_), ref n)) if n == "round")
					&& matches!(
						func_name(f).and_then(|(s, _)| s).as_deref(),
						None | Some("pg_catalog")
					) && !f.agg_star
					&& !f.agg_distinct
					&& f.agg_order.is_empty()
					&& f.agg_filter.is_none()
					&& f.over.is_none()
					&& (1..=2).contains(&f.args.len()) =>
			{
				let x = sub(f.args.first(), self)?;
				let places = match f.args.get(1) {
					Some(p) => Some(Box::new(self.compile(p, None, st)?)),
					None => None,
				};
				Ok(Expr::Round(x, places))
			}
			_ => Err(unsupported()),
		}
	}

	fn param(&self, number: i32) -> Result<Expr, Refusal> {
		let i = (number as usize).wrapping_sub(1);
		let bad = || {
			no(
				format!(
					"Lepis cannot read parameter ${number} to compute the aggregates across nodes"
				),
				"Bind it as a number, or pin the query to one shard key value.",
			)
		};
		if self.ctx.described.is_none() {
			return Ok(Expr::Null);
		}
		let ty = self.ctx.param_types.get(i).copied().unwrap_or(0);
		match self.ctx.params.get(i).map(ParamValue::value) {
			None | Some(ParamValue::Null | ParamValue::Declared(..)) => Ok(Expr::Null),
			Some(ParamValue::Text(s)) => match ty {
				merge::INT2 | merge::INT4 | merge::INT8 => {
					s.trim().parse::<i128>().map(Expr::Int).map_err(|_| bad())
				}
				merge::NUMERIC => merge::Dec::parse(s)
					.map(|d| Expr::Num(d.to_text()))
					.ok_or_else(bad),
				_ => Err(bad()),
			},
			Some(ParamValue::Binary(b)) => match (ty, b.len()) {
				(merge::INT8, 8) => Ok(Expr::Int(i64::from_be_bytes(
					b[..].try_into().map_err(|_| bad())?,
				) as i128)),
				(merge::INT4, 4) => Ok(Expr::Int(i32::from_be_bytes(
					b[..].try_into().map_err(|_| bad())?,
				) as i128)),
				(merge::INT2, 2) => Ok(Expr::Int(i16::from_be_bytes(
					b[..].try_into().map_err(|_| bad())?,
				) as i128)),
				(merge::NUMERIC, _) => merge::Dec::from_binary(b)
					.map(|d| Expr::Num(d.to_text()))
					.ok_or_else(bad),
				_ => Err(bad()),
			},
		}
	}

	/// An aggregate: its partial on every node and how it finishes.
	fn agg(
		&mut self,
		f: &FuncCall,
		raw: Option<i16>,
		st: &mut GroupState,
	) -> Result<usize, Refusal> {
		let Some((schema, name)) = func_name(f) else {
			return Err(no("Lepis cannot read this aggregate", PIN));
		};
		let cannot = |why: &str| {
			no(
				format!("{name}() {why}"),
				"Pin the query to one shard key value, or compute it in the application from rows Lepis returns.",
			)
		};
		if schema.as_deref().is_some_and(|s| s != "pg_catalog") {
			return Err(cannot(
				"is not one of the aggregates Lepis combines across nodes",
			));
		}
		if f.agg_within_group || !f.agg_order.is_empty() {
			return Err(cannot(
				"with ORDER BY or WITHIN GROUP cannot be combined from each node's part",
			));
		}
		let kind = match name.as_str() {
			"count" => AggKind::Count,
			"sum" => AggKind::Sum,
			"min" => AggKind::Min,
			"max" => AggKind::Max,
			"avg" => AggKind::Avg,
			"bool_and" | "every" => AggKind::BoolAnd,
			"bool_or" => AggKind::BoolOr,
			_ => return Err(cannot("cannot be combined from each node's part yet")),
		};
		let one_arg = || -> Result<&Node, Refusal> {
			match f.args.as_slice() {
				[a] if !f.agg_star => Ok(a),
				_ => Err(no(format!("{name}() takes one argument here"), PIN)),
			}
		};
		let a = if f.agg_distinct
			&& !matches!(
				kind,
				AggKind::Min | AggKind::Max | AggKind::BoolAnd | AggKind::BoolOr
			) {
			if f.agg_filter.is_some() {
				return Err(cannot(
					"with both DISTINCT and FILTER cannot be combined across nodes yet",
				));
			}
			let arg = one_arg()?.clone();
			let col = self.hidden(&arg, 0)?;
			self.companion(col)?;
			let key = self.deparse_expr(&arg)?;
			if !st.distinct_keys.contains(&key) {
				st.distinct_keys.insert(key);
				st.distinct_args.push(arg);
			}
			Agg {
				kind: match kind {
					AggKind::Count => AggKind::CountDistinct,
					AggKind::Sum => AggKind::SumDistinct,
					_ => AggKind::AvgDistinct,
				},
				col,
				count_col: None,
			}
		} else {
			if !f.agg_star && kind != AggKind::Count {
				one_arg()?;
			}
			let renamed = |to: &str| {
				let mut p = f.clone();
				p.agg_distinct = false;
				p.location = -1;
				if let Some(last) = p.funcname.last_mut() {
					*last = string_node(to);
				}
				node(NodeEnum::FuncCall(Box::new(p)))
			};
			let format = match kind {
				AggKind::Min | AggKind::Max => raw.unwrap_or(0),
				_ => 0,
			};
			let partial = match kind {
				AggKind::Avg => renamed("sum"),
				AggKind::BoolAnd => renamed("bool_and"),
				_ => renamed(&name),
			};
			let col = self.hidden(&partial, format)?;
			if matches!(kind, AggKind::Min | AggKind::Max) {
				self.companion(col)?;
			}
			let count_col = if kind == AggKind::Avg {
				Some(self.hidden(&renamed("count"), 0)?)
			} else {
				None
			};
			Agg {
				kind,
				col,
				count_col,
			}
		};
		if let Some(i) = st.aggs.iter().position(|x| *x == a) {
			return Ok(i);
		}
		st.aggs.push(a);
		Ok(st.aggs.len() - 1)
	}

	fn into_worker(self) -> SelectStmt {
		let mut w = self.top.clone();
		w.target_list = self.targets;
		if self.grouped {
			w.group_clause = self.group;
			w.having_clause = None;
			w.sort_clause.clear();
			w.distinct_clause.clear();
			w.limit_count = None;
			w.limit_offset = None;
		} else {
			w.limit_offset = None;
			w.limit_count = self
				.limit
				.filter(|_| self.push_limit)
				.map(|l| Box::new(number_const(l.saturating_add(self.offset))));
			w.limit_option = if w.limit_count.is_some() {
				LimitOption::Count as i32
			} else {
				LimitOption::Default as i32
			};
		}
		w
	}
}

#[derive(Default)]
struct GroupState {
	aggs: Vec<Agg>,
	distinct_args: Vec<Node>,
	distinct_keys: BTreeSet<String>,
}

fn op_name(name: &[Node]) -> Option<String> {
	match name {
		[n] => match &n.node {
			Some(NodeEnum::String(s)) => Some(s.sval.clone()),
			_ => None,
		},
		[schema, n] => match (&schema.node, &n.node) {
			(Some(NodeEnum::String(s)), Some(NodeEnum::String(o))) if s.sval == "pg_catalog" => {
				Some(o.sval.clone())
			}
			_ => None,
		},
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{Keyspace, Node as CatalogNode, NodeId, NodeState, Strategy};
	use crate::config::SslMode;
	use crate::hash::KeyType;

	fn cat() -> Catalog {
		let ids: Vec<NodeId> = (1..=3).map(NodeId).collect();
		let mut c = Catalog {
			epoch: 1,
			..Default::default()
		};
		for id in &ids {
			c.nodes.insert(
				*id,
				CatalogNode {
					id: *id,
					name: format!("n{}", id.0),
					host: "h".into(),
					port: 5432,
					dbname: "app".into(),
					sslmode: SslMode::Disable,
					home: id.0 == 1,
					state: NodeState::Active,
					server_version_num: Some(180_000),
				},
			);
		}
		c.keyspaces.insert(
			"tenant".into(),
			Keyspace {
				name: "tenant".into(),
				strategy: Strategy::Hash,
				key_type: KeyType::Int8,
				seed: 1,
				ranges: Keyspace::even_ranges(12, &ids),
				pins: Default::default(),
			},
		);
		for t in ["orders", "items"] {
			c.relations.insert(
				RelationName {
					schema: "app".into(),
					table: t.into(),
				},
				RelationKind::Sharded {
					keyspace: "tenant".into(),
					key_column: "tenant_id".into(),
				},
			);
		}
		c.relations.insert(
			RelationName {
				schema: "app".into(),
				table: "countries".into(),
			},
			RelationKind::Reference,
		);
		c
	}

	fn planned(sql: &str, names: &[&str], types: &[u32]) -> Result<Planned, Refusal> {
		planned_with(sql, names, types, None)
	}

	fn planned_with(
		sql: &str,
		names: &[&str],
		types: &[u32],
		col_types: Option<&[u32]>,
	) -> Result<Planned, Refusal> {
		let cat = cat();
		let path = vec!["app".to_string()];
		let d = Described {
			names: names.iter().map(|s| s.to_string()).collect(),
			types: types.to_vec(),
		};
		let ctx = Context {
			catalog: &cat,
			search_path: &path,
			described: Some(&d),
			aggregates: &[],
			params: &[],
			param_types: &[],
			result_formats: &[],
			col_types,
			collatable: &HashMap::new(),
		};
		plan(sql, &ctx)
	}

	#[test]
	fn plain_rows_run_unchanged() {
		let p = planned(
			"select tenant_id, status from orders where total > 5;",
			&["tenant_id", "status"],
			&[20, 25],
		)
		.unwrap();
		assert_eq!(
			p.worker_sql,
			"select tenant_id, status from orders where total > 5"
		);
		assert_eq!(p.merge.kind, MergeKind::Rows { sorted: false });
	}

	#[test]
	fn order_and_limit_push_down_with_hidden_keys() {
		let p = planned(
			"select tenant_id, order_id from orders order by placed_at, 1 desc limit 10 offset 25",
			&["tenant_id", "order_id"],
			&[20, 20],
		)
		.unwrap();
		// placed_at's type is unknown until a node describes the worker …
		assert!(p.needs_types);
		assert_eq!(
			p.worker_sql,
			"SELECT tenant_id, order_id, placed_at FROM orders ORDER BY placed_at, 1 DESC LIMIT 35"
		);
		// … a timestamp has no collation, a text does.
		let sql =
			"select tenant_id, order_id from orders order by placed_at, 1 desc limit 10 offset 25";
		let p = planned_with(
			sql,
			&["tenant_id", "order_id"],
			&[20, 20],
			Some(&[20, 20, 1114]),
		)
		.unwrap();
		assert!(!p.needs_types);
		assert_eq!(p.merge.collation_of[2], None);
		let p = planned_with(
			sql,
			&["tenant_id", "order_id"],
			&[20, 20],
			Some(&[20, 20, 25]),
		)
		.unwrap();
		assert_eq!(
			p.worker_sql,
			"SELECT tenant_id, order_id, placed_at, pg_catalog.pg_collation_for(placed_at) FROM orders ORDER BY placed_at, 1 DESC LIMIT 35"
		);
		assert_eq!(p.merge.offset, 25);
		assert_eq!(p.merge.limit, Some(10));
		assert_eq!(p.merge.order[0].expr, Expr::Col(2));
		assert_eq!(p.merge.collation_of[2], Some(3));
		assert!(p.merge.order[1].desc && p.merge.order[1].nulls_first);
	}

	#[test]
	fn aggregates_become_partials() {
		let p = planned(
			"select status, count(*), round(avg(total), 2) from orders group by status having sum(total) > 10 order by 2 desc",
			&["status", "count", "round"],
			&[25, 20, 1700],
		)
		.unwrap();
		assert_eq!(
			p.worker_sql,
			"SELECT status, NULL, NULL, count(*), sum(total), count(total), pg_catalog.pg_collation_for(status) FROM orders GROUP BY status"
		);
		let MergeKind::Groups(g) = &p.merge.kind else {
			panic!()
		};
		assert_eq!(g.keys, vec![0]);
		assert_eq!(g.aggs.len(), 3);
		assert!(g.having.is_some());
	}

	#[test]
	fn count_distinct_groups_each_node_by_the_argument() {
		let p = planned("select count(distinct sku) from items", &["count"], &[20]).unwrap();
		assert!(
			p.worker_sql.ends_with("FROM items GROUP BY sku"),
			"{}",
			p.worker_sql
		);
	}

	#[test]
	fn refusals_name_their_fix() {
		for sql in [
			"select tenant_id, rank() over (order by total) from orders",
			"select string_agg(status, ',') from orders",
			"select * from orders union select * from orders",
			"select distinct on (status) * from orders",
			"select * from orders for update",
			"select * from orders where total > (select avg(total) from orders)",
			"select * from (select status from orders limit 5) s",
			"select count(*) from (select distinct status from orders) s",
			"select upper(status) as status, count(*) from orders group by status",
			"select array_agg(total order by total) from orders",
		] {
			let r = planned(sql, &["a"], &[25]).unwrap_err();
			assert_eq!(r.code, "0A000", "{sql}");
			assert!(!r.hint.is_empty(), "{sql}");
		}
		// A subquery grouped by the shard key computes whole groups on each node.
		assert!(
			planned(
				"select avg(n) from (select tenant_id, count(*) n from orders group by tenant_id) s",
				&["avg"],
				&[1700]
			)
			.is_ok()
		);
		// Over a reference table, any subquery is the same on every node.
		assert!(
			planned(
				"select * from orders where country in (select code from countries limit 3)",
				&["a"],
				&[25]
			)
			.is_ok()
		);
		let r = planned("select * from orders limit -1", &["a"], &[25]).unwrap_err();
		assert_eq!(r.code, "2201W");
	}

	#[test]
	fn explain_explains_the_worker() {
		let p = planned(
			"explain (costs off) select * from orders order by total limit 3",
			&["tenant_id"],
			&[20],
		)
		.unwrap();
		assert!(p.explain);
		assert!(
			p.worker_sql.starts_with("EXPLAIN (COSTS OFF) SELECT"),
			"{}",
			p.worker_sql
		);
		assert!(is_explain("explain select 1"));
		assert!(!is_explain("explain (format json) select 1"));
	}
}
