//! The parser adapter (L4): what each statement of a query string says, read from Postgres's own
//! parse tree (libpg_query through `pg_query`), as the `Facts` route() decides on.
//!
//! route() trusts every fact here, so this file leans one way throughout: a fact that cannot be
//! proven from the tree is left out. A key left unpinned or a join not recorded can only become a
//! scatter or a refusal; a key pinned wrongly sends a statement to a node that answers differently
//! from one Postgres would (L9). Where the tree holds something this file cannot see inside, a
//! cluster with sharded tables gets a refusal naming it rather than a guess.
//!
//! Two words used below. A query LEVEL is one SELECT / UPDATE / DELETE / INSERT with its own FROM
//! list; a subquery is a level of its own. A PIN is what a level's WHERE says a table's key equals.
//! route() intersects the pins of tables joined on the key (they share one key value per row), so
//! a pin is only recorded where it really restricts every row the statement produces: from the
//! level's top-level conjunction, never from the ON of an outer join (which filters nothing on
//! the preserved side), and an `IS NULL` never for a table an outer join can null-extend.

use pg_query::NodeEnum;
use pg_query::protobuf::{
	AExprKind, BoolExprType, ColumnRef, DeleteStmt, DiscardMode, InsertStmt, JoinType, Node,
	NullTestType, ObjectType, RangeVar, SelectStmt, SetOperation, SubLinkType, TransactionStmtKind,
	UpdateStmt, VariableSetKind, WithClause, a_const,
};

use crate::catalog::{Catalog, RelationKind, RelationName};
use crate::route::{AnalyzeError, Facts, KeyValue, Kind, SessionVerb, Statement, TableUse, TxVerb};

type Result<T = ()> = std::result::Result<T, AnalyzeError>;

/// A statement's kind and, for transaction control and session state, what it does.
type Verbs = (Kind, Option<TxVerb>, Option<SessionVerb>);

fn plain(kind: Kind) -> Result<Verbs> {
	Ok((kind, None, None))
}

/// Every statement of `sql`, in order, with what route() needs to know about it. `search_path`
/// is the session's, as `current_schemas(false)` reports it: an unqualified table is the first
/// one it finds there.
pub fn analyze(
	sql: &str,
	catalog: &Catalog,
	search_path: &[String],
) -> std::result::Result<Vec<Statement>, AnalyzeError> {
	let parsed = pg_query::parse(sql).map_err(|e| {
		AnalyzeError::Syntax(match e {
			pg_query::Error::Parse(m) => m,
			e => e.to_string(),
		})
	})?;
	let sharded = catalog
		.relations
		.values()
		.any(|k| matches!(k, RelationKind::Sharded { .. }));
	let mut out = Vec::new();
	for raw in &parsed.protobuf.stmts {
		let Some(node) = raw.stmt.as_deref().and_then(|n| n.node.as_ref()) else {
			continue;
		};
		let mut a = Analyzer {
			catalog,
			search_path,
			sharded,
			tables: Vec::new(),
			key_joins: Vec::new(),
			needs_merge: Vec::new(),
			assigns_key: false,
			insert_keys: None,
			ctes: Vec::new(),
			levels: Vec::new(),
		};
		let (kind, tx, session) = a.statement(node)?;
		let text = text_of(sql, raw.stmt_location, raw.stmt_len);
		out.push(Statement {
			session_bound: session_state(node, &text),
			sql: text,
			facts: a.finish(kind)?,
			tx,
			session,
		});
	}
	Ok(out)
}

/// One statement's own text: libpg_query gives BYTE offsets into the original, and a length of
/// 0 for the last statement, meaning "to the end".
fn text_of(sql: &str, location: i32, len: i32) -> String {
	let start = usize::try_from(location).unwrap_or(0).min(sql.len());
	let end = match usize::try_from(len) {
		Ok(n) if n > 0 => start.saturating_add(n).min(sql.len()),
		_ => sql.len(),
	};
	sql.get(start..end).unwrap_or(sql).trim().to_string()
}

/// Where a qualifier of a level came from, which decides what it may prove.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Qual {
	/// The WHERE, or the ON of an inner join no outer join null-extends: it filters every row
	/// the level produces, so it may pin keys and its EXISTS / IN may restrict the level.
	Filter,
	/// The ON of an outer join (or of any join inside one's nullable side): it decides which rows
	/// MATCH, not which rows survive. Key equalities in it still prove colocation (every match of
	/// a row lives on that row's node), but it pins nothing.
	JoinOnly,
}

/// One entry of a level's FROM list, as column references see it.
struct Item {
	/// The alias, or the relation's own name.
	refname: String,
	/// The relation, for a table without an alias (`schema.table.column` names it so).
	relation: Option<RelationName>,
	/// Index into `tables`, for a table.
	table: Option<usize>,
	/// The shard key column of a sharded table whose columns keep their names.
	key: Option<String>,
	/// On the nullable side of an outer join.
	nullable: bool,
	/// Inside a join that has an alias, which hides the tables' own names.
	hidden: bool,
}

/// A FROM subtree: its items (a contiguous run of the level's) and its USING / NATURAL columns,
/// each with the sharded items keyed by that column it merges.
struct Tree {
	start: usize,
	end: usize,
	merged: Vec<(String, Vec<usize>)>,
}

struct Level {
	items: Vec<Item>,
	merged: Vec<(String, Vec<usize>)>,
	/// False while a subquery in this level's FROM that is not LATERAL is read: its siblings are
	/// out of its sight, so a name there must resolve further out.
	visible: bool,
	/// Every row this level produces filters a row of the level around it (an EXISTS or IN that
	/// is a conjunct of a Filter, or a subquery in FROM no outer join null-extends).
	positive: bool,
	has_agg: bool,
	grouped: bool,
	/// Key equalities with a table of an enclosing level: (the table here or deeper, the table
	/// there, that level). They become key joins only if every level they cross is positive.
	pending: Vec<(usize, usize, usize)>,
}

impl Level {
	fn new(positive: bool) -> Level {
		Level {
			items: Vec::new(),
			merged: Vec::new(),
			visible: true,
			positive,
			has_agg: false,
			grouped: false,
			pending: Vec::new(),
		}
	}
}

/// What the level around a subquery may learn from it.
#[derive(Clone, Copy, Default)]
struct Summary {
	/// For `x IN (SELECT key FROM t …)`: the use of `t`, when the set the subquery returns is
	/// exactly the keys of the rows of `t` its WHERE keeps (no LIMIT, aggregate, grouping …), so
	/// computing it on one node gives the members that node holds.
	key_target: Option<usize>,
}

/// A column reference that denotes a sharded table's key.
#[derive(Clone, Copy)]
struct Binding {
	level: usize,
	item: usize,
	/// Through a USING / NATURAL column, whose value may come from either side.
	merged: bool,
}

struct Analyzer<'c> {
	catalog: &'c Catalog,
	search_path: &'c [String],
	/// Whether the catalog has any sharded table. Without one every statement goes home, so
	/// nothing here needs refusing.
	sharded: bool,
	tables: Vec<TableUse>,
	key_joins: Vec<(usize, usize)>,
	needs_merge: Vec<&'static str>,
	assigns_key: bool,
	insert_keys: Option<Vec<KeyValue>>,
	/// CTE names in scope, outermost first.
	ctes: Vec<String>,
	/// The levels being read, outermost first.
	levels: Vec<Level>,
}

impl Analyzer<'_> {
	// -----------------------------------------------------------------------------------------
	// Statements.

	fn statement(&mut self, n: &NodeEnum) -> Result<Verbs> {
		use NodeEnum as N;
		match n {
			N::SelectStmt(s) => {
				self.select(s, false)?;
				match s.into_clause.as_ref().and_then(|i| i.rel.as_ref()) {
					// SELECT … INTO creates a table: DDL.
					Some(rel) => {
						self.add_table(rel, true);
						plain(Kind::Ddl)
					}
					None => plain(Kind::Select),
				}
			}
			N::InsertStmt(s) => {
				self.insert(s, true)?;
				plain(Kind::Insert)
			}
			N::UpdateStmt(s) => {
				self.update(s)?;
				plain(Kind::Update)
			}
			N::DeleteStmt(s) => {
				self.delete(s)?;
				plain(Kind::Delete)
			}
			N::CopyStmt(c) => {
				if let Some(rel) = &c.relation {
					self.add_table(rel, c.is_from);
				}
				if let Some(q) = opt(&c.query) {
					self.query(q, false)?;
				}
				plain(if c.is_from {
					Kind::CopyFrom
				} else {
					Kind::CopyTo
				})
			}
			// EXPLAIN is the statement it explains: EXPLAIN ANALYZE of a write runs the write.
			N::ExplainStmt(e) => match opt(&e.query) {
				Some(inner) => self.statement(inner),
				None => plain(Kind::Other),
			},
			N::TransactionStmt(t) => {
				use TransactionStmtKind as T;
				let verb = match TransactionStmtKind::try_from(t.kind) {
					Ok(T::TransStmtBegin | T::TransStmtStart) => TxVerb::Begin,
					Ok(T::TransStmtCommit) => TxVerb::Commit,
					Ok(T::TransStmtRollback) => TxVerb::Rollback,
					_ => TxVerb::Other,
				};
				Ok((Kind::Transaction, Some(verb), None))
			}
			N::VariableSetStmt(v) => {
				use VariableSetKind as V;
				let verb = match VariableSetKind::try_from(v.kind) {
					Ok(V::VarResetAll) => SessionVerb::ResetAll,
					// SET SESSION CHARACTERISTICS AS TRANSACTION sets the session's defaults, so it
					// is replayed like any SET; SET TRANSACTION [SNAPSHOT] is this transaction's.
					Ok(V::VarSetMulti) if v.name == "SESSION CHARACTERISTICS" => SessionVerb::Set {
						touches_search_path: false,
					},
					Ok(V::VarSetMulti) => SessionVerb::SetLocal,
					_ if v.is_local => SessionVerb::SetLocal,
					_ => SessionVerb::Set {
						touches_search_path: v.name == "search_path",
					},
				};
				Ok((Kind::Session, None, Some(verb)))
			}
			N::DiscardStmt(d) => {
				let verb = if d.target == DiscardMode::DiscardAll as i32 {
					SessionVerb::ResetAll
				} else {
					SessionVerb::Other
				};
				Ok((Kind::Session, None, Some(verb)))
			}
			N::ConstraintsSetStmt(_) => Ok((Kind::Session, None, Some(SessionVerb::SetLocal))),
			N::VariableShowStmt(_)
			| N::ListenStmt(_)
			| N::UnlistenStmt(_)
			| N::NotifyStmt(_)
			| N::LoadStmt(_) => Ok((Kind::Session, None, Some(SessionVerb::Other))),
			N::DoStmt(_) => self.opaque(
				"a DO block may touch any table, so Lepis cannot tell which node it belongs to",
			),
			N::CallStmt(_) => self.opaque(
				"a CALL may touch any table, so Lepis cannot tell which node it belongs to",
			),
			N::PrepareStmt(_) | N::ExecuteStmt(_) | N::DeallocateStmt(_) => self.opaque(
				"SQL-level PREPARE / EXECUTE hide the statement from Lepis, so it cannot tell which node it belongs to; use the protocol's prepared statements instead",
			),
			N::MergeStmt(_) => self.opaque(
				"MERGE is not supported on a sharded database yet: Lepis cannot tell which node each of its rows belongs to",
			),
			n if is_ddl(n) => {
				self.ddl(n)?;
				plain(Kind::Ddl)
			}
			n => self.opaque(&format!(
				"{} is not supported on a sharded database yet, so Lepis cannot tell which node it belongs to",
				node_name(n)
			)),
		}
	}

	/// A statement Lepis cannot see inside: harmless (home) on a cluster with nothing sharded,
	/// refused otherwise.
	fn opaque(&self, why: &str) -> Result<Verbs> {
		if self.sharded {
			Err(AnalyzeError::Unsupported(why.to_string()))
		} else {
			Ok((Kind::Other, None, None))
		}
	}

	fn finish(mut self, kind: Kind) -> Result<Facts> {
		match kind {
			Kind::Select | Kind::CopyTo => {
				// route() treats these as reads, so a write inside one (a data-modifying WITH,
				// COPY (DELETE … RETURNING) TO) would be scattered like a read.
				if let Some(t) = self
					.tables
					.iter()
					.find(|t| t.written && self.is_sharded(&t.name))
				{
					return Err(AnalyzeError::Unsupported(format!(
						"a query that reads may not also write the sharded table {} yet; run the write as a statement of its own",
						t.name
					)));
				}
			}
			Kind::Ddl => {
				for t in &mut self.tables {
					t.written = true;
					t.key_values = None;
				}
				self.key_joins.clear();
				self.assigns_key = false;
				self.insert_keys = None;
				self.needs_merge.clear();
			}
			_ => self.needs_merge.clear(),
		}
		Ok(Facts {
			kind,
			tables: self.tables,
			key_joins: self.key_joins,
			assigns_key: self.assigns_key,
			insert_keys: self.insert_keys,
			needs_merge: self.needs_merge,
		})
	}

	/// A statement nested in another: a subquery, a CTE body, COPY's query, a rule's action.
	fn query(&mut self, n: &NodeEnum, positive: bool) -> Result<Summary> {
		match n {
			NodeEnum::SelectStmt(s) => self.select(s, positive),
			NodeEnum::InsertStmt(s) => self.insert(s, false).map(|_| Summary::default()),
			NodeEnum::UpdateStmt(s) => self.update(s).map(|_| Summary::default()),
			NodeEnum::DeleteStmt(s) => self.delete(s).map(|_| Summary::default()),
			n => self.unknown(n).map(|_| Summary::default()),
		}
	}

	fn select(&mut self, s: &SelectStmt, positive: bool) -> Result<Summary> {
		self.merge_marks(s);
		let mark = self.with(s.with_clause.as_ref())?;
		let summary = if is_set_operation(s) {
			// Each arm is a level of its own, and neither filters anything around them alone.
			for arm in [&s.larg, &s.rarg].into_iter().flatten() {
				self.select(arm, false)?;
			}
			self.levels.push(Level::new(false));
			for n in s
				.sort_clause
				.iter()
				.chain(opt_node(&s.limit_offset))
				.chain(opt_node(&s.limit_count))
			{
				self.expr(n)?;
			}
			self.pop_level();
			Summary::default()
		} else {
			self.levels.push(Level::new(positive));
			let mut quals = Vec::new();
			let mut merged = Vec::new();
			for f in &s.from_clause {
				merged.extend(self.range_entry(f, false, &mut quals)?.merged);
			}
			self.cur().merged = merged;
			if let Some(w) = &s.where_clause {
				for c in conjuncts(w) {
					quals.push((c, Qual::Filter));
				}
			}
			for (q, kind) in quals {
				self.conjunct(q, kind)?;
			}
			for n in s
				.values_lists
				.iter()
				.chain(&s.target_list)
				.chain(&s.distinct_clause)
				.chain(&s.group_clause)
				.chain(opt_node(&s.having_clause))
				.chain(&s.window_clause)
				.chain(&s.sort_clause)
				.chain(opt_node(&s.limit_offset))
				.chain(opt_node(&s.limit_count))
			{
				self.expr(n)?;
			}
			if !s.group_clause.is_empty() || s.having_clause.is_some() {
				self.cur().grouped = true;
			}
			let key_target = self.key_target(s);
			let positive = self.pop_level();
			Summary {
				key_target: key_target.filter(|_| positive),
			}
		};
		self.ctes.truncate(mark);
		Ok(summary)
	}

	/// The table whose key a single-column subquery returns, if the set it returns is just the
	/// keys of the rows it keeps (see `Summary`). Read while its level is still current.
	fn key_target(&self, s: &SelectStmt) -> Option<usize> {
		let plain_distinct = s.distinct_clause.iter().all(|d| d.node.is_none());
		if s.target_list.len() != 1
			|| !plain_distinct
			|| s.limit_count.is_some()
			|| s.limit_offset.is_some()
		{
			return None;
		}
		let Some(NodeEnum::ResTarget(rt)) = s.target_list[0].node.as_ref() else {
			return None;
		};
		let NodeEnum::ColumnRef(c) = opt(&rt.val)? else {
			return None;
		};
		let here = self.levels.len() - 1;
		match self.bind(c).as_slice() {
			[b] if b.level == here => self.levels[here].items[b.item].table,
			_ => None,
		}
	}

	fn insert(&mut self, s: &InsertStmt, top: bool) -> Result {
		let mark = self.with(s.with_clause.as_ref())?;
		let Some(rel) = &s.relation else {
			return Ok(());
		};
		let target = self.add_table(rel, true);
		let key = self.key_of(target);
		if key.is_some() && s.cols.is_empty() {
			return Err(AnalyzeError::Unsupported(
				"name the columns of an INSERT into a sharded table, so Lepis can find the shard key"
					.into(),
			));
		}
		// INSERT … VALUES: the rows are read where they are; INSERT … SELECT: a query.
		let rows = match opt(&s.select_stmt) {
			Some(NodeEnum::SelectStmt(v)) if is_plain_values(v) => {
				self.levels.push(Level::new(false));
				for row in &v.values_lists {
					self.expr(row)?;
				}
				self.pop_level();
				Some(&v.values_lists)
			}
			Some(q) => {
				self.query(q, false)?;
				None
			}
			None => None,
		};
		let mut keys = None;
		if let (Some(key), Some(rows)) = (&key, rows) {
			let Some(pos) = s.cols.iter().position(|c| {
				matches!(&c.node, Some(NodeEnum::ResTarget(r)) if &r.name == key && r.indirection.is_empty())
			}) else {
				return Err(AnalyzeError::Unsupported(format!(
					"an INSERT into a sharded table must give its shard key column {key} a value, so Lepis can find each row's node"
				)));
			};
			let mut out = Vec::with_capacity(rows.len());
			for row in rows {
				let value = match &row.node {
					Some(NodeEnum::List(l)) => l.items.get(pos).and_then(key_value),
					_ => None,
				};
				out.push(value.ok_or_else(|| {
					AnalyzeError::Unsupported(
						"the shard key of an inserted row must be a literal or a parameter".into(),
					)
				})?);
			}
			keys = Some(out);
		}
		// ON CONFLICT and RETURNING see the target, and ON CONFLICT also EXCLUDED.
		self.levels.push(Level::new(false));
		let item = self.item(rel, Some(target), false);
		self.cur().items.push(item);
		self.cur().items.push(pseudo("excluded".into(), false));
		if let Some(oc) = &s.on_conflict_clause {
			if let Some(infer) = &oc.infer {
				for n in infer
					.index_elems
					.iter()
					.chain(opt_node(&infer.where_clause))
				{
					self.expr(n)?;
				}
			}
			self.assignments(&oc.target_list, target)?;
			if let Some(w) = &oc.where_clause {
				self.expr(w)?;
			}
		}
		for n in &s.returning_list {
			self.expr(n)?;
		}
		self.pop_level();
		self.ctes.truncate(mark);
		if let Some(keys) = keys {
			// Only a statement that is nothing but the rows can be split by them: a subquery
			// in a row, a CTE or a RETURNING subquery reads tables the split would not follow.
			// Elsewhere the keys still pin the target, which is exactly where the rows go.
			if top && self.tables.len() == 1 {
				self.insert_keys = Some(keys);
			} else {
				self.tables[target].key_values = Some(keys);
			}
		}
		Ok(())
	}

	fn update(&mut self, s: &UpdateStmt) -> Result {
		let mark = self.with(s.with_clause.as_ref())?;
		let Some(rel) = &s.relation else {
			return Ok(());
		};
		self.levels.push(Level::new(false));
		let target = self.add_table(rel, true);
		let item = self.item(rel, Some(target), false);
		self.cur().items.push(item);
		let mut quals = Vec::new();
		let mut merged = Vec::new();
		for f in &s.from_clause {
			merged.extend(self.range_entry(f, false, &mut quals)?.merged);
		}
		self.cur().merged = merged;
		if let Some(w) = &s.where_clause {
			for c in conjuncts(w) {
				quals.push((c, Qual::Filter));
			}
		}
		for (q, kind) in quals {
			self.conjunct(q, kind)?;
		}
		self.assignments(&s.target_list, target)?;
		for n in &s.returning_list {
			self.expr(n)?;
		}
		self.pop_level();
		self.ctes.truncate(mark);
		Ok(())
	}

	fn delete(&mut self, s: &DeleteStmt) -> Result {
		let mark = self.with(s.with_clause.as_ref())?;
		let Some(rel) = &s.relation else {
			return Ok(());
		};
		self.levels.push(Level::new(false));
		let target = self.add_table(rel, true);
		let item = self.item(rel, Some(target), false);
		self.cur().items.push(item);
		let mut quals = Vec::new();
		let mut merged = Vec::new();
		for f in &s.using_clause {
			merged.extend(self.range_entry(f, false, &mut quals)?.merged);
		}
		self.cur().merged = merged;
		if let Some(w) = &s.where_clause {
			for c in conjuncts(w) {
				quals.push((c, Qual::Filter));
			}
		}
		for (q, kind) in quals {
			self.conjunct(q, kind)?;
		}
		for n in &s.returning_list {
			self.expr(n)?;
		}
		self.pop_level();
		self.ctes.truncate(mark);
		Ok(())
	}

	/// A SET list (UPDATE, ON CONFLICT DO UPDATE): assigning the target's key would move the row.
	fn assignments(&mut self, targets: &[Node], target: usize) -> Result {
		let key = self.key_of(target);
		for t in targets {
			if let Some(NodeEnum::ResTarget(rt)) = &t.node
				&& key.as_deref() == Some(rt.name.as_str())
			{
				self.assigns_key = true;
			}
			self.expr(t)?;
		}
		Ok(())
	}

	/// Reads a WITH list and brings its names into scope; returns the scope mark to restore.
	/// A CTE body is a level of its own that filters nothing around it.
	fn with(&mut self, w: Option<&WithClause>) -> Result<usize> {
		let mark = self.ctes.len();
		let Some(w) = w else {
			return Ok(mark);
		};
		let ctes: Vec<_> = w
			.ctes
			.iter()
			.filter_map(|c| match &c.node {
				Some(NodeEnum::CommonTableExpr(c)) => Some(c),
				_ => None,
			})
			.collect();
		// WITH RECURSIVE: every name is visible in every body. Otherwise a body sees only the
		// CTEs before it, so `WITH orders AS (SELECT … FROM orders)` reads the TABLE orders.
		if w.recursive {
			self.ctes.extend(ctes.iter().map(|c| c.ctename.clone()));
		}
		for c in ctes {
			if let Some(q) = opt(&c.ctequery) {
				self.query(q, false)?;
			}
			if !w.recursive {
				self.ctes.push(c.ctename.clone());
			}
		}
		Ok(mark)
	}

	/// Leaves the current level; returns whether it is positive (filters the level around it),
	/// and hands its correlated key equalities outward if so. An aggregate or GROUP BY / HAVING
	/// can produce a row from no input (`count(*)` of nothing is 0), so such a level proves
	/// nothing about the rows around it.
	fn pop_level(&mut self) -> bool {
		let Some(level) = self.levels.pop() else {
			return false;
		};
		let positive = level.positive && !level.has_agg && !level.grouped;
		if positive && let Some(parent) = self.levels.len().checked_sub(1) {
			for (inner, outer, at) in level.pending {
				if at == parent {
					self.key_joins.push((outer, inner));
				} else {
					self.levels[parent].pending.push((inner, outer, at));
				}
			}
		}
		positive
	}

	fn cur(&mut self) -> &mut Level {
		self.levels.last_mut().expect("a level is open")
	}

	fn level(&self) -> &Level {
		self.levels.last().expect("a level is open")
	}

	// -----------------------------------------------------------------------------------------
	// FROM.

	fn range_entry<'n>(
		&mut self,
		n: &'n Node,
		nullable: bool,
		quals: &mut Vec<(&'n Node, Qual)>,
	) -> Result<Tree> {
		let start = self.cur().items.len();
		let mut merged = Vec::new();
		match &n.node {
			Some(NodeEnum::RangeVar(rv)) => {
				let item = if rv.schemaname.is_empty() && self.ctes.contains(&rv.relname) {
					pseudo(refname(rv), nullable)
				} else {
					let t = self.add_table(rv, false);
					self.item(rv, Some(t), nullable)
				};
				self.cur().items.push(item);
			}
			Some(NodeEnum::JoinExpr(j)) => {
				let (left_nullable, right_nullable, filter) = match JoinType::try_from(j.jointype) {
					Ok(JoinType::JoinInner) => (nullable, nullable, !nullable),
					Ok(JoinType::JoinLeft) => (nullable, true, false),
					Ok(JoinType::JoinRight) => (true, nullable, false),
					_ => (true, true, false),
				};
				let (Some(l), Some(r)) = (j.larg.as_deref(), j.rarg.as_deref()) else {
					return Ok(Tree {
						start,
						end: start,
						merged,
					});
				};
				let left = self.range_entry(l, left_nullable, quals)?;
				let right = self.range_entry(r, right_nullable, quals)?;
				let names: Vec<String> = if j.is_natural {
					self.natural_columns(&left, &right)
				} else {
					strings(&j.using_clause)
						.unwrap_or_default()
						.into_iter()
						.map(str::to_string)
						.collect()
				};
				for name in &names {
					let (lp, rp) = (self.providers(&left, name), self.providers(&right, name));
					for a in &lp {
						for b in &rp {
							let level = self.level();
							let (ta, tb) = (level.items[*a].table, level.items[*b].table);
							if let (Some(ta), Some(tb)) = (ta, tb) {
								self.key_joins.push((ta, tb));
							}
						}
					}
					merged.push((name.clone(), lp.into_iter().chain(rp).collect()));
				}
				for (name, items) in left.merged.into_iter().chain(right.merged) {
					if !names.contains(&name) {
						merged.push((name, items));
					}
				}
				if let Some(q) = &j.quals {
					let kind = if filter { Qual::Filter } else { Qual::JoinOnly };
					for c in conjuncts(q) {
						quals.push((c, kind));
					}
				}
				if let Some(alias) = &j.alias {
					let end = self.cur().items.len();
					for item in &mut self.cur().items[start..end] {
						item.hidden = true;
					}
					self.cur()
						.items
						.push(pseudo(alias.aliasname.clone(), nullable));
				}
				if let Some(alias) = &j.join_using_alias {
					self.cur()
						.items
						.push(pseudo(alias.aliasname.clone(), nullable));
				}
			}
			Some(NodeEnum::RangeSubselect(rs)) => {
				// A subquery that is not LATERAL cannot see its siblings.
				if !rs.lateral {
					self.cur().visible = false;
				}
				let read = match opt(&rs.subquery) {
					Some(q) => self.query(q, !nullable).map(|_| ()),
					None => Ok(()),
				};
				self.cur().visible = true;
				read?;
				let name = rs
					.alias
					.as_ref()
					.map(|a| a.aliasname.clone())
					.unwrap_or_default();
				self.cur().items.push(pseudo(name, nullable));
			}
			Some(NodeEnum::RangeFunction(rf)) => {
				let mut name = String::new();
				for f in &rf.functions {
					// Each is a List of [the call, its column definitions].
					let call = match &f.node {
						Some(NodeEnum::List(l)) => l.items.first(),
						_ => Some(f),
					};
					if let Some(call) = call {
						if let Some(NodeEnum::FuncCall(fc)) = &call.node
							&& name.is_empty()
						{
							name = strings(&fc.funcname)
								.and_then(|s| s.last().map(|s| s.to_string()))
								.unwrap_or_default();
						}
						self.expr(call)?;
					}
				}
				if let Some(a) = &rf.alias {
					name = a.aliasname.clone();
				}
				self.cur().items.push(pseudo(name, nullable));
			}
			Some(NodeEnum::RangeTableSample(ts)) => {
				let Some(rel) = ts.relation.as_deref() else {
					return self.unknown_tree(n, start);
				};
				let tree = self.range_entry(rel, nullable, quals)?;
				let level = self.level();
				let sampled = (tree.start..tree.end)
					.filter_map(|i| level.items[i].table)
					.find(|t| self.is_sharded(&self.tables[*t].name));
				if let Some(t) = sampled {
					return Err(AnalyzeError::Unsupported(format!(
						"TABLESAMPLE of the sharded table {} would sample each node's rows, not the table's",
						self.tables[t].name
					)));
				}
				for n in ts.args.iter().chain(opt_node(&ts.repeatable)) {
					self.expr(n)?;
				}
				merged = tree.merged;
			}
			Some(NodeEnum::RangeTableFunc(tf)) => {
				for n in opt_node(&tf.docexpr).chain(opt_node(&tf.rowexpr)) {
					self.expr(n)?;
				}
				for c in &tf.columns {
					if let Some(NodeEnum::RangeTableFuncCol(c)) = &c.node {
						for n in opt_node(&c.colexpr).chain(opt_node(&c.coldefexpr)) {
							self.expr(n)?;
						}
					}
				}
				let name = tf
					.alias
					.as_ref()
					.map(|a| a.aliasname.clone())
					.unwrap_or_default();
				self.cur().items.push(pseudo(name, nullable));
			}
			_ => return self.unknown_tree(n, start),
		}
		Ok(Tree {
			start,
			end: self.cur().items.len(),
			merged,
		})
	}

	fn unknown_tree(&mut self, n: &Node, start: usize) -> Result<Tree> {
		if let Some(e) = &n.node {
			self.unknown(e)?;
		}
		self.cur().items.push(pseudo(String::new(), true));
		Ok(Tree {
			start,
			end: start + 1,
			merged: Vec::new(),
		})
	}

	/// The sharded items of a subtree a USING column `name` comes from: its own USING column of
	/// that name, else the one sharded item keyed by it (Postgres refuses a USING column that
	/// appears twice on one side).
	fn providers(&self, tree: &Tree, name: &str) -> Vec<usize> {
		if let Some((_, items)) = tree.merged.iter().find(|(n, _)| n == name) {
			return items.clone();
		}
		let level = self.level();
		let keyed: Vec<usize> = (tree.start..tree.end)
			.filter(|i| level.items[*i].key.as_deref() == Some(name))
			.collect();
		if keyed.len() == 1 { keyed } else { Vec::new() }
	}

	/// The columns a NATURAL join merges that Lepis can know: key columns present on both sides.
	/// (Other common columns are merged too, but nothing here depends on them.)
	fn natural_columns(&self, left: &Tree, right: &Tree) -> Vec<String> {
		let level = self.level();
		let mut names: Vec<String> = Vec::new();
		for tree in [left, right] {
			for i in tree.start..tree.end {
				if let Some(k) = &level.items[i].key
					&& !names.contains(k)
				{
					names.push(k.clone());
				}
			}
			for (n, _) in &tree.merged {
				if !names.contains(n) {
					names.push(n.clone());
				}
			}
		}
		names
			.into_iter()
			.filter(|n| !self.providers(left, n).is_empty() && !self.providers(right, n).is_empty())
			.collect()
	}

	// -----------------------------------------------------------------------------------------
	// Qualifiers: pins and key joins.

	fn conjunct(&mut self, n: &Node, q: Qual) -> Result {
		if let Some(NodeEnum::SubLink(sl)) = &n.node
			&& q == Qual::Filter
		{
			let ty = SubLinkType::try_from(sl.sub_link_type);
			if matches!(ty, Ok(SubLinkType::ExistsSublink | SubLinkType::AnySublink)) {
				if let Some(t) = &sl.testexpr {
					self.expr(t)?;
				}
				let summary = match opt(&sl.subselect) {
					Some(s) => self.query(s, true)?,
					None => Summary::default(),
				};
				// `key IN (SELECT key FROM …)`: the two are equal on every row that survives.
				if matches!(ty, Ok(SubLinkType::AnySublink))
					&& is_eq(&sl.oper_name)
					&& let Some(target) = summary.key_target
					&& let Some(NodeEnum::ColumnRef(c)) = opt(&sl.testexpr)
				{
					let here = self.levels.len() - 1;
					for b in self.bind(c) {
						if b.level == here
							&& let Some(t) = self.levels[here].items[b.item].table
						{
							self.key_joins.push((t, target));
						}
					}
				}
				return Ok(());
			}
		}
		self.expr(n)?;
		match &n.node {
			Some(NodeEnum::AExpr(a))
				if matches!(AExprKind::try_from(a.kind), Ok(AExprKind::AexprOp))
					&& is_eq(&a.name) =>
			{
				let (Some(l), Some(r)) = (a.lexpr.as_deref(), a.rexpr.as_deref()) else {
					return Ok(());
				};
				match (column(l), column(r)) {
					(Some(cl), Some(cr)) => {
						let (bl, br) = (self.bind(cl), self.bind(cr));
						self.equate(&bl, &br);
					}
					(Some(c), None) | (None, Some(c)) if q == Qual::Filter => {
						let other = if column(l).is_some() { r } else { l };
						if let Some(v) = key_value(other) {
							let b = self.bind(c);
							self.pin(&b, vec![v], false);
						}
					}
					_ => {}
				}
			}
			Some(NodeEnum::AExpr(a))
				if q == Qual::Filter
					&& matches!(AExprKind::try_from(a.kind), Ok(AExprKind::AexprIn))
					&& is_eq(&a.name) =>
			{
				let (Some(c), Some(NodeEnum::List(list))) =
					(a.lexpr.as_deref().and_then(column), opt(&a.rexpr))
				else {
					return Ok(());
				};
				let values: Option<Vec<KeyValue>> = list.items.iter().map(key_value).collect();
				if let Some(values) = values {
					let b = self.bind(c);
					self.pin(&b, values, false);
				}
			}
			Some(NodeEnum::NullTest(t))
				if q == Qual::Filter
					&& matches!(
						NullTestType::try_from(t.nulltesttype),
						Ok(NullTestType::IsNull)
					) =>
			{
				if let Some(c) = t.arg.as_deref().and_then(column) {
					let b = self.bind(c);
					self.pin(&b, vec![KeyValue::Null], true);
				}
			}
			_ => {}
		}
		Ok(())
	}

	/// Records `values` as what each bound table's key equals, keeping the narrower of two pins:
	/// both hold, so the true set is a subset of each.
	fn pin(&mut self, bound: &[Binding], values: Vec<KeyValue>, null_test: bool) {
		let here = self.levels.len() - 1;
		for b in bound {
			// A pin binds the level it is written in; a comparison with an outer table's key is
			// evaluated per outer row and restricts nothing at that table's own level.
			if b.level != here {
				continue;
			}
			let item = &self.levels[here].items[b.item];
			// `IS NULL` is true of a null-extended row: it says nothing about the table's rows.
			if null_test && (item.nullable || b.merged) {
				continue;
			}
			let Some(t) = item.table else { continue };
			match &self.tables[t].key_values {
				Some(old) if old.len() <= values.len() => {}
				_ => self.tables[t].key_values = Some(values.clone()),
			}
		}
	}

	/// `a.key = b.key`: colocation, at this level or (pending) with an enclosing one.
	fn equate(&mut self, left: &[Binding], right: &[Binding]) {
		let here = self.levels.len() - 1;
		for x in left {
			for y in right {
				let tx = self.levels[x.level].items[x.item].table;
				let ty = self.levels[y.level].items[y.item].table;
				let (Some(tx), Some(ty)) = (tx, ty) else {
					continue;
				};
				if tx == ty {
					continue;
				}
				match (x.level == here, y.level == here) {
					(true, true) => self.key_joins.push((tx, ty)),
					(true, false) => self.cur().pending.push((tx, ty, y.level)),
					(false, true) => self.cur().pending.push((ty, tx, x.level)),
					(false, false) => {}
				}
			}
		}
	}

	/// The sharded tables whose KEY a column reference denotes, following Postgres's scoping: an
	/// unqualified name binds at the innermost level (the one written in), a qualified one at
	/// the innermost level with an entry of that name. Anything ambiguous binds nothing.
	fn bind(&self, c: &ColumnRef) -> Vec<Binding> {
		let Some(fields) = strings(&c.fields) else {
			return Vec::new();
		};
		let Some(here) = self.levels.len().checked_sub(1) else {
			return Vec::new();
		};
		match fields.as_slice() {
			[col] => {
				let level = &self.levels[here];
				let merged: Vec<_> = level
					.merged
					.iter()
					.filter(|(n, _)| n.as_str() == *col)
					.collect();
				match merged.as_slice() {
					[(_, items)] => {
						return items
							.iter()
							.map(|i| Binding {
								level: here,
								item: *i,
								merged: true,
							})
							.collect();
					}
					[] => {}
					_ => return Vec::new(),
				}
				let keyed: Vec<usize> = (0..level.items.len())
					.filter(|i| level.items[*i].key.as_deref() == Some(*col))
					.collect();
				match keyed.as_slice() {
					[i] => vec![Binding {
						level: here,
						item: *i,
						merged: false,
					}],
					_ => Vec::new(),
				}
			}
			[qualifier, col] => self.bind_qualified(col, |item| item.refname == *qualifier),
			[schema, table, col] => {
				let name = self.resolve(schema, table);
				self.bind_qualified(col, |item| item.relation.as_ref() == Some(&name))
			}
			_ => Vec::new(),
		}
	}

	fn bind_qualified(&self, col: &str, names: impl Fn(&Item) -> bool) -> Vec<Binding> {
		for (level, l) in self.levels.iter().enumerate().rev() {
			if !l.visible {
				continue;
			}
			let found: Vec<usize> = (0..l.items.len())
				.filter(|i| !l.items[*i].hidden && names(&l.items[*i]))
				.collect();
			match found.as_slice() {
				[] => continue,
				[i] if l.items[*i].key.as_deref() == Some(col) => {
					return vec![Binding {
						level,
						item: *i,
						merged: false,
					}];
				}
				_ => return Vec::new(),
			}
		}
		Vec::new()
	}

	// -----------------------------------------------------------------------------------------
	// Expressions: every subquery in them is read (non-positive), and the constructs a merge
	// across nodes would get wrong are noted.

	fn expr(&mut self, n: &Node) -> Result {
		use NodeEnum as N;
		let Some(e) = &n.node else { return Ok(()) };
		match e {
			N::AConst(_)
			| N::ColumnRef(_)
			| N::ParamRef(_)
			| N::AStar(_)
			| N::SqlvalueFunction(_)
			| N::SetToDefault(_)
			| N::CurrentOfExpr(_)
			| N::String(_)
			| N::Integer(_)
			| N::Float(_)
			| N::Boolean(_)
			| N::BitString(_)
			| N::TypeName(_)
			| N::LockingClause(_)
			| N::GroupingFunc(_) => Ok(()),
			N::AExpr(a) => self.exprs(opt_node(&a.lexpr).chain(opt_node(&a.rexpr))),
			N::BoolExpr(b) => self.exprs(&b.args),
			N::NullTest(t) => self.exprs(opt_node(&t.arg)),
			N::BooleanTest(t) => self.exprs(opt_node(&t.arg)),
			N::TypeCast(t) => self.exprs(opt_node(&t.arg)),
			N::CollateClause(c) => self.exprs(opt_node(&c.arg)),
			N::NamedArgExpr(a) => self.exprs(opt_node(&a.arg)),
			N::CaseExpr(c) => self.exprs(
				opt_node(&c.arg)
					.chain(&c.args)
					.chain(opt_node(&c.defresult)),
			),
			N::CaseWhen(w) => self.exprs(opt_node(&w.expr).chain(opt_node(&w.result))),
			N::CoalesceExpr(c) => self.exprs(&c.args),
			N::MinMaxExpr(m) => self.exprs(&m.args),
			N::RowExpr(r) => self.exprs(&r.args),
			N::AArrayExpr(a) => self.exprs(&a.elements),
			N::List(l) => self.exprs(&l.items),
			N::AIndirection(a) => self.exprs(opt_node(&a.arg).chain(&a.indirection)),
			N::AIndices(a) => self.exprs(opt_node(&a.lidx).chain(opt_node(&a.uidx))),
			N::ResTarget(r) => self.exprs(opt_node(&r.val).chain(&r.indirection)),
			// `SET (a, b) = (SELECT …)`: every column shares one source; read it once.
			N::MultiAssignRef(m) if m.colno <= 1 => self.exprs(opt_node(&m.source)),
			N::MultiAssignRef(_) => Ok(()),
			N::SortBy(s) => self.exprs(opt_node(&s.node)),
			N::WindowDef(w) => self.exprs(
				w.partition_clause
					.iter()
					.chain(&w.order_clause)
					.chain(opt_node(&w.start_offset))
					.chain(opt_node(&w.end_offset)),
			),
			N::GroupingSet(g) => self.exprs(&g.content),
			N::IndexElem(i) => self.exprs(opt_node(&i.expr)),
			N::XmlExpr(x) => self.exprs(x.named_args.iter().chain(&x.args)),
			N::XmlSerialize(x) => self.exprs(opt_node(&x.expr)),
			N::FuncCall(f) => {
				if f.over.is_some() {
					self.mark("a window function");
				} else if is_aggregate(f) {
					self.mark("an aggregate");
					if let Some(l) = self.levels.last_mut() {
						l.has_agg = true;
					}
				}
				self.exprs(
					f.args
						.iter()
						.chain(&f.agg_order)
						.chain(opt_node(&f.agg_filter)),
				)?;
				if let Some(w) = &f.over {
					self.exprs(
						w.partition_clause
							.iter()
							.chain(&w.order_clause)
							.chain(opt_node(&w.start_offset))
							.chain(opt_node(&w.end_offset)),
					)?;
				}
				Ok(())
			}
			N::SubLink(s) => {
				self.exprs(opt_node(&s.testexpr))?;
				if let Some(q) = opt(&s.subselect) {
					self.query(q, false)?;
				}
				Ok(())
			}
			N::JsonIsPredicate(j) => self.exprs(opt_node(&j.expr)),
			N::JsonValueExpr(j) => self.exprs(opt_node(&j.raw_expr)),
			N::JsonKeyValue(j) => {
				self.exprs(opt_node(&j.key))?;
				match &j.value {
					Some(v) => self.exprs(opt_node(&v.raw_expr)),
					None => Ok(()),
				}
			}
			N::JsonObjectConstructor(j) => self.exprs(&j.exprs),
			N::JsonArrayConstructor(j) => self.exprs(&j.exprs),
			N::JsonArrayQueryConstructor(j) => match opt(&j.query) {
				Some(q) => self.query(q, false).map(|_| ()),
				None => Ok(()),
			},
			N::JsonScalarExpr(j) => self.exprs(opt_node(&j.expr)),
			N::JsonParseExpr(j) => match &j.expr {
				Some(v) => self.exprs(opt_node(&v.raw_expr)),
				None => Ok(()),
			},
			N::JsonSerializeExpr(j) => match &j.expr {
				Some(v) => self.exprs(opt_node(&v.raw_expr)),
				None => Ok(()),
			},
			N::JsonObjectAgg(j) => {
				self.json_agg(j.constructor.as_deref())?;
				match &j.arg {
					Some(kv) => {
						self.exprs(opt_node(&kv.key))?;
						match &kv.value {
							Some(v) => self.exprs(opt_node(&v.raw_expr)),
							None => Ok(()),
						}
					}
					None => Ok(()),
				}
			}
			N::JsonArrayAgg(j) => {
				self.json_agg(j.constructor.as_deref())?;
				match &j.arg {
					Some(v) => self.exprs(opt_node(&v.raw_expr)),
					None => Ok(()),
				}
			}
			e => self.unknown(e),
		}
	}

	fn exprs<'n>(&mut self, nodes: impl IntoIterator<Item = &'n Node>) -> Result {
		for n in nodes {
			self.expr(n)?;
		}
		Ok(())
	}

	fn json_agg(&mut self, c: Option<&pg_query::protobuf::JsonAggConstructor>) -> Result {
		let Some(c) = c else { return Ok(()) };
		if c.over.is_some() {
			self.mark("a window function");
		} else {
			self.mark("an aggregate");
			if let Some(l) = self.levels.last_mut() {
				l.has_agg = true;
			}
		}
		self.exprs(c.agg_order.iter().chain(opt_node(&c.agg_filter)))?;
		if let Some(w) = &c.over {
			self.exprs(w.partition_clause.iter().chain(&w.order_clause))?;
		}
		Ok(())
	}

	/// A node this file does not read: it could hide a subquery, so with sharded tables it is
	/// refused rather than skipped.
	fn unknown(&self, n: &NodeEnum) -> Result {
		if self.sharded {
			Err(AnalyzeError::Unsupported(format!(
				"Lepis cannot read {} yet, so it cannot tell which tables the statement touches",
				node_name(n)
			)))
		} else {
			Ok(())
		}
	}

	fn merge_marks(&mut self, s: &SelectStmt) {
		if !s.sort_clause.is_empty() {
			self.mark("ORDER BY");
		}
		if s.limit_count.is_some() {
			self.mark("LIMIT");
		}
		if s.limit_offset.is_some() {
			self.mark("OFFSET");
		}
		if !s.group_clause.is_empty() {
			self.mark("GROUP BY");
		}
		if s.having_clause.is_some() {
			self.mark("HAVING");
		}
		if !s.distinct_clause.is_empty() {
			self.mark("DISTINCT");
		}
		if is_set_operation(s) {
			self.mark("a set operation");
		}
	}

	fn mark(&mut self, what: &'static str) {
		if !self.needs_merge.contains(&what) {
			self.needs_merge.push(what);
		}
	}

	// -----------------------------------------------------------------------------------------
	// DDL: every relation the statement names.

	fn ddl(&mut self, n: &NodeEnum) -> Result {
		use NodeEnum as N;
		match n {
			N::CreateStmt(c) => self.create_stmt(c),
			N::CreateForeignTableStmt(c) => {
				if let Some(c) = &c.base_stmt {
					self.create_stmt(c);
				}
			}
			N::CreateTableAsStmt(c) => {
				if let Some(q) = opt(&c.query) {
					self.query(q, false)?;
				}
				if let Some(rel) = c.into.as_ref().and_then(|i| i.rel.as_ref()) {
					self.add_table(rel, true);
				}
			}
			N::ViewStmt(v) => {
				if let Some(rel) = &v.view {
					self.add_table(rel, true);
				}
				if let Some(q) = opt(&v.query) {
					self.query(q, false)?;
				}
			}
			N::RuleStmt(r) => {
				if let Some(rel) = &r.relation {
					self.add_table(rel, true);
				}
				for a in &r.actions {
					if let Some(q) = &a.node {
						self.query(q, false)?;
					}
				}
			}
			N::AlterTableStmt(a) => {
				if let Some(rel) = &a.relation {
					self.add_table(rel, true);
				}
				for cmd in &a.cmds {
					if let Some(N::AlterTableCmd(cmd)) = &cmd.node {
						match opt(&cmd.def) {
							Some(N::Constraint(c)) => self.constraint(c),
							Some(N::ColumnDef(d)) => self.column_def(d),
							Some(N::PartitionCmd(p)) => {
								if let Some(rel) = &p.name {
									self.add_table(rel, true);
								}
							}
							_ => {}
						}
					}
				}
			}
			N::DropStmt(d) => {
				let drop = match ObjectType::try_from(d.remove_type) {
					Ok(
						ObjectType::ObjectTable
						| ObjectType::ObjectView
						| ObjectType::ObjectMatview
						| ObjectType::ObjectForeignTable
						| ObjectType::ObjectSequence
						| ObjectType::ObjectIndex,
					) => 0,
					// `DROP TRIGGER t ON tbl` and friends: the list ends with the object's name.
					Ok(
						ObjectType::ObjectRule
						| ObjectType::ObjectTrigger
						| ObjectType::ObjectPolicy,
					) => 1,
					_ => return Ok(()),
				};
				for o in &d.objects {
					if let Some(N::List(l)) = &o.node {
						self.add_name_list(&l.items, drop);
					}
				}
			}
			N::CommentStmt(c) => {
				let drop = match ObjectType::try_from(c.objtype) {
					Ok(
						ObjectType::ObjectTable
						| ObjectType::ObjectView
						| ObjectType::ObjectMatview
						| ObjectType::ObjectForeignTable
						| ObjectType::ObjectSequence
						| ObjectType::ObjectIndex,
					) => 0,
					Ok(
						ObjectType::ObjectColumn
						| ObjectType::ObjectTabconstraint
						| ObjectType::ObjectRule
						| ObjectType::ObjectTrigger
						| ObjectType::ObjectPolicy,
					) => 1,
					_ => return Ok(()),
				};
				if let Some(N::List(l)) = opt(&c.object) {
					self.add_name_list(&l.items, drop);
				}
			}
			N::RenameStmt(r) => {
				if let Some(rel) = &r.relation {
					self.add_table(rel, true);
				}
			}
			N::AlterObjectSchemaStmt(a) => {
				if let Some(rel) = &a.relation {
					self.add_table(rel, true);
				}
			}
			N::AlterOwnerStmt(a) => {
				if let Some(rel) = &a.relation {
					self.add_table(rel, true);
				}
			}
			N::AlterSeqStmt(s) => {
				if let Some(rel) = &s.sequence {
					self.add_table(rel, true);
				}
			}
			N::CreateSeqStmt(s) => {
				if let Some(rel) = &s.sequence {
					self.add_table(rel, true);
				}
			}
			N::CreatePolicyStmt(p) => {
				if let Some(rel) = &p.table {
					self.add_table(rel, true);
				}
			}
			N::AlterPolicyStmt(p) => {
				if let Some(rel) = &p.table {
					self.add_table(rel, true);
				}
			}
			N::CreateTrigStmt(t) => {
				for rel in t.relation.iter().chain(&t.constrrel) {
					self.add_table(rel, true);
				}
			}
			N::IndexStmt(i) => {
				if let Some(rel) = &i.relation {
					self.add_table(rel, true);
				}
			}
			N::ClusterStmt(c) => {
				if let Some(rel) = &c.relation {
					self.add_table(rel, true);
				}
			}
			N::ReindexStmt(r) => {
				if let Some(rel) = &r.relation {
					self.add_table(rel, true);
				}
			}
			N::RefreshMatViewStmt(r) => {
				if let Some(rel) = &r.relation {
					self.add_table(rel, true);
				}
			}
			N::TruncateStmt(t) => self.add_range_vars(&t.relations),
			N::GrantStmt(g) => self.add_range_vars(&g.objects),
			N::LockStmt(l) => self.add_range_vars(&l.relations),
			N::CreateStatsStmt(s) => self.add_range_vars(&s.relations),
			N::VacuumStmt(v) => {
				for r in &v.rels {
					if let Some(N::VacuumRelation(r)) = &r.node
						&& let Some(rel) = &r.relation
					{
						self.add_table(rel, true);
					}
				}
			}
			_ => {}
		}
		Ok(())
	}

	fn create_stmt(&mut self, c: &pg_query::protobuf::CreateStmt) {
		if let Some(rel) = &c.relation {
			self.add_table(rel, true);
		}
		self.add_range_vars(&c.inh_relations);
		for e in c.table_elts.iter().chain(&c.constraints) {
			match &e.node {
				Some(NodeEnum::ColumnDef(d)) => self.column_def(d),
				Some(NodeEnum::Constraint(k)) => self.constraint(k),
				Some(NodeEnum::TableLikeClause(l)) => {
					if let Some(rel) = &l.relation {
						self.add_table(rel, true);
					}
				}
				_ => {}
			}
		}
	}

	fn column_def(&mut self, d: &pg_query::protobuf::ColumnDef) {
		for k in &d.constraints {
			if let Some(NodeEnum::Constraint(k)) = &k.node {
				self.constraint(k);
			}
		}
	}

	fn constraint(&mut self, k: &pg_query::protobuf::Constraint) {
		if let Some(rel) = &k.pktable {
			self.add_table(rel, true);
		}
	}

	fn add_range_vars(&mut self, nodes: &[Node]) {
		for n in nodes {
			if let Some(NodeEnum::RangeVar(rel)) = &n.node {
				self.add_table(rel, true);
			}
		}
	}

	/// A name given as a list of strings (`[table]`, `[schema, table]`, `[db, schema, table]`),
	/// less the last `drop` (a column, trigger or constraint name after the table's).
	fn add_name_list(&mut self, items: &[Node], drop: usize) {
		let Some(parts) = strings(items) else { return };
		let parts = &parts[..parts.len().saturating_sub(drop)];
		let name = match parts {
			[table] => self.resolve("", table),
			[schema, table] | [_, schema, table] => self.resolve(schema, table),
			_ => return,
		};
		self.tables.push(TableUse {
			name,
			key_values: None,
			written: true,
		});
	}

	// -----------------------------------------------------------------------------------------
	// Relations.

	/// Postgres's lookup: a qualified name is itself; an unqualified one is the first schema of
	/// the search path holding it. A table the catalog does not know is unknown to Lepis too,
	/// and route() treats it as global (home).
	fn resolve(&self, schema: &str, table: &str) -> RelationName {
		if !schema.is_empty() {
			return RelationName {
				schema: schema.to_string(),
				table: table.to_string(),
			};
		}
		for s in self.search_path {
			let name = RelationName {
				schema: s.clone(),
				table: table.to_string(),
			};
			if self.catalog.relations.contains_key(&name) {
				return name;
			}
		}
		RelationName {
			schema: self
				.search_path
				.first()
				.cloned()
				.unwrap_or_else(|| "public".to_string()),
			table: table.to_string(),
		}
	}

	fn add_table(&mut self, rv: &RangeVar, written: bool) -> usize {
		let name = self.resolve(&rv.schemaname, &rv.relname);
		self.tables.push(TableUse {
			name,
			key_values: None,
			written,
		});
		self.tables.len() - 1
	}

	fn is_sharded(&self, name: &RelationName) -> bool {
		matches!(
			self.catalog.relations.get(name),
			Some(RelationKind::Sharded { .. })
		)
	}

	fn key_of(&self, table: usize) -> Option<String> {
		match self.catalog.relations.get(&self.tables[table].name) {
			Some(RelationKind::Sharded { key_column, .. }) => Some(key_column.clone()),
			_ => None,
		}
	}

	fn item(&self, rv: &RangeVar, table: Option<usize>, nullable: bool) -> Item {
		// `orders AS o (a, b)` renames columns by position, which hides which one is the key.
		let renamed = rv.alias.as_ref().is_some_and(|a| !a.colnames.is_empty());
		Item {
			refname: refname(rv),
			relation: table
				.filter(|_| rv.alias.is_none())
				.map(|t| self.tables[t].name.clone()),
			table,
			key: table.and_then(|t| self.key_of(t)).filter(|_| !renamed),
			nullable,
			hidden: false,
		}
	}
}

/// An entry with no table behind it (a CTE, a subquery, a function): it holds a name, so it
/// shadows the same name further out, and binds nothing.
fn pseudo(refname: String, nullable: bool) -> Item {
	Item {
		refname,
		relation: None,
		table: None,
		key: None,
		nullable,
		hidden: false,
	}
}

fn refname(rv: &RangeVar) -> String {
	match &rv.alias {
		Some(a) => a.aliasname.clone(),
		None => rv.relname.clone(),
	}
}

/// The top-level AND terms of a qualifier.
fn conjuncts(n: &Node) -> Vec<&Node> {
	let mut out = Vec::new();
	fn walk<'n>(n: &'n Node, out: &mut Vec<&'n Node>) {
		match &n.node {
			Some(NodeEnum::BoolExpr(b)) if b.boolop == BoolExprType::AndExpr as i32 => {
				for a in &b.args {
					walk(a, out);
				}
			}
			_ => out.push(n),
		}
	}
	walk(n, &mut out);
	out
}

fn column(n: &Node) -> Option<&ColumnRef> {
	match &n.node {
		Some(NodeEnum::ColumnRef(c)) => Some(c),
		_ => None,
	}
}

/// A value a key can be compared with: a literal, `$n`, or either cast to a plain type. A cast
/// with a modifier (`::varchar(3)`) can change the value and is not followed.
fn key_value(n: &Node) -> Option<KeyValue> {
	match n.node.as_ref()? {
		NodeEnum::AConst(c) => constant(c),
		NodeEnum::ParamRef(p) => usize::try_from(p.number)
			.ok()
			.filter(|n| *n > 0)
			.map(KeyValue::Param),
		NodeEnum::TypeCast(t) => {
			let plain = t
				.type_name
				.as_ref()
				.is_some_and(|ty| ty.typmods.is_empty() && ty.array_bounds.is_empty());
			if !plain {
				return None;
			}
			// The cast's type travels with the value: route.rs lets only a type that hashes as
			// the key's pin it (a binary `$1::int8` is int8's bytes, whatever the key is).
			let name = t
				.type_name
				.as_ref()?
				.names
				.last()
				.and_then(|n| match &n.node {
					Some(NodeEnum::String(s)) => Some(s.sval.clone()),
					_ => None,
				})?;
			let inner = match opt(&t.arg)? {
				NodeEnum::AConst(c) => constant(c)?,
				NodeEnum::ParamRef(_) | NodeEnum::TypeCast(_) => key_value(t.arg.as_deref()?)?,
				_ => return None,
			};
			Some(match inner {
				KeyValue::Null => KeyValue::Null,
				inner => KeyValue::Cast(Box::new(inner), name),
			})
		}
		// `- 5` the grammar did not fold into the constant.
		NodeEnum::AExpr(a)
			if a.lexpr.is_none()
				&& matches!(AExprKind::try_from(a.kind), Ok(AExprKind::AexprOp))
				&& matches!(strings(&a.name).as_deref(), Some(["-"])) =>
		{
			match opt(&a.rexpr)? {
				NodeEnum::AConst(c) => match constant(c)? {
					KeyValue::Const(s) if !s.starts_with('-') => {
						Some(KeyValue::Const(format!("-{s}")))
					}
					_ => None,
				},
				_ => None,
			}
		}
		_ => None,
	}
}

fn constant(c: &pg_query::protobuf::AConst) -> Option<KeyValue> {
	if c.isnull {
		return Some(KeyValue::Null);
	}
	Some(KeyValue::Const(match c.val.as_ref()? {
		a_const::Val::Ival(i) => i.ival.to_string(),
		a_const::Val::Fval(f) => f.fval.clone(),
		a_const::Val::Sval(s) => s.sval.clone(),
		a_const::Val::Boolval(b) => b.boolval.to_string(),
		a_const::Val::Bsval(_) => return None,
	}))
}

/// `=` written plainly (`OPERATOR(myschema.=)` is someone else's operator); an IN sublink's
/// operator list is empty.
fn is_eq(name: &[Node]) -> bool {
	matches!(strings(name).as_deref(), Some([]) | Some(["="]))
}

fn strings(nodes: &[Node]) -> Option<Vec<&str>> {
	nodes
		.iter()
		.map(|n| match &n.node {
			Some(NodeEnum::String(s)) => Some(s.sval.as_str()),
			_ => None,
		})
		.collect()
}

fn opt(n: &Option<Box<Node>>) -> Option<&NodeEnum> {
	n.as_deref().and_then(|n| n.node.as_ref())
}

fn opt_node(n: &Option<Box<Node>>) -> impl Iterator<Item = &Node> {
	n.as_deref().into_iter()
}

fn is_set_operation(s: &SelectStmt) -> bool {
	matches!(
		SetOperation::try_from(s.op),
		Ok(SetOperation::SetopUnion | SetOperation::SetopIntersect | SetOperation::SetopExcept)
	)
}

fn is_plain_values(v: &SelectStmt) -> bool {
	!v.values_lists.is_empty()
		&& v.with_clause.is_none()
		&& v.sort_clause.is_empty()
		&& v.limit_count.is_none()
		&& v.limit_offset.is_none()
}

/// The aggregates a merge across nodes would get wrong: anything written as one (`*`, DISTINCT,
/// ORDER BY, FILTER, WITHIN GROUP) and the standard ones by name.
/// The JWT claims a statement sets for its transaction or session, as the data API sets them:
/// `select set_config('request.jwt.claims', <json>, <is_local>)` (among other set_config calls in
/// the same SELECT), or `SET [LOCAL] "request.jwt.claims" = '<json>'`. Returns the JSON text and
/// whether it is local to the transaction; None when the statement sets no claims, or sets them
/// from something other than a literal or a bound text parameter.
pub fn jwt_claims(sql: &str, params: &[crate::route::ParamValue]) -> Option<(String, bool)> {
	const NAME: &str = "request.jwt.claims";
	if !sql.contains(NAME) {
		return None;
	}
	let parsed = pg_query::parse(sql).ok()?;
	let node = parsed
		.protobuf
		.stmts
		.first()?
		.stmt
		.as_deref()?
		.node
		.as_ref()?;
	fn text(n: &NodeEnum, params: &[crate::route::ParamValue]) -> Option<String> {
		match n {
			NodeEnum::AConst(c) => match c.val.as_ref()? {
				a_const::Val::Sval(s) => Some(s.sval.clone()),
				a_const::Val::Boolval(b) => Some(b.boolval.to_string()),
				_ => None,
			},
			NodeEnum::ParamRef(p) => match params.get((p.number as usize).checked_sub(1)?)?.value()
			{
				crate::route::ParamValue::Text(t) => Some(t.clone()),
				// A text value in binary is its UTF-8 bytes.
				crate::route::ParamValue::Binary(b) => String::from_utf8(b.clone()).ok(),
				_ => None,
			},
			NodeEnum::TypeCast(t) => text(t.arg.as_deref()?.node.as_ref()?, params),
			_ => None,
		}
	}
	match node {
		NodeEnum::VariableSetStmt(v) if v.name == NAME => {
			let value = text(v.args.first()?.node.as_ref()?, params)?;
			Some((value, v.is_local))
		}
		NodeEnum::SelectStmt(s) => {
			for t in &s.target_list {
				let Some(NodeEnum::ResTarget(r)) = t.node.as_ref() else {
					continue;
				};
				let Some(NodeEnum::FuncCall(f)) = r.val.as_deref().and_then(|v| v.node.as_ref())
				else {
					continue;
				};
				let name = f.funcname.last().and_then(|n| match n.node.as_ref() {
					Some(NodeEnum::String(s)) => Some(s.sval.as_str()),
					_ => None,
				});
				if name != Some("set_config") || f.args.len() != 3 {
					continue;
				}
				let arg = |i: usize| f.args[i].node.as_ref().and_then(|n| text(n, params));
				if arg(0).as_deref() != Some(NAME) {
					continue;
				}
				let local = !matches!(
					arg(2).as_deref().map(str::to_ascii_lowercase).as_deref(),
					Some("false" | "f" | "off" | "no" | "0")
				);
				return Some((arg(1)?, local));
			}
			None
		}
		_ => None,
	}
}

/// What a statement leaves behind in the node's session that Lepis cannot carry to another
/// backend, for transaction pooling (where the next transaction may run on another backend):
/// a name for it, or None. Session SETs and prepared statements are not here: Lepis replays
/// those itself.
pub fn session_bound(sql: &str) -> Option<&'static str> {
	let parsed = pg_query::parse(sql).ok()?;
	parsed
		.protobuf
		.stmts
		.iter()
		.filter_map(|raw| raw.stmt.as_deref().and_then(|n| n.node.as_ref()))
		.find_map(|node| session_state(node, sql))
}

/// `session_bound` for one statement already parsed.
fn session_state(node: &NodeEnum, sql: &str) -> Option<&'static str> {
	{
		let temp = |r: &Option<pg_query::protobuf::RangeVar>| {
			r.as_ref().is_some_and(|r| r.relpersistence == "t")
		};
		let dropped_at_commit =
			|o: i32| o == pg_query::protobuf::OnCommitAction::OncommitDrop as i32;
		match node {
			NodeEnum::ListenStmt(_) => return Some("LISTEN"),
			NodeEnum::PrepareStmt(_) => return Some("PREPARE"),
			NodeEnum::LoadStmt(_) => return Some("LOAD"),
			NodeEnum::CreateStmt(c) if temp(&c.relation) && !dropped_at_commit(c.oncommit) => {
				return Some("a temporary table");
			}
			NodeEnum::CreateTableAsStmt(c)
				if c.into
					.as_ref()
					.is_some_and(|i| temp(&i.rel) && !dropped_at_commit(i.on_commit)) =>
			{
				return Some("a temporary table");
			}
			// CURSOR_OPT_HOLD
			NodeEnum::DeclareCursorStmt(d) if d.options & 0x0020 != 0 => {
				return Some("a cursor WITH HOLD");
			}
			_ => {}
		}
	}
	if !sql.contains("advisory_lock") {
		return None;
	}
	let session_locks = [
		"pg_advisory_lock",
		"pg_advisory_lock_shared",
		"pg_try_advisory_lock",
		"pg_try_advisory_lock_shared",
	];
	crate::scatter::functions(sql)
		.iter()
		.any(|f| session_locks.contains(&f.as_str()))
		.then_some("a session advisory lock")
}

pub(crate) fn is_aggregate(f: &pg_query::protobuf::FuncCall) -> bool {
	if f.agg_star
		|| f.agg_distinct
		|| f.agg_within_group
		|| !f.agg_order.is_empty()
		|| f.agg_filter.is_some()
	{
		return true;
	}
	let Some(name) = strings(&f.funcname).and_then(|s| s.last().map(|s| s.to_string())) else {
		return false;
	};
	const NAMES: &[&str] = &[
		"count",
		"sum",
		"min",
		"max",
		"avg",
		"array_agg",
		"string_agg",
		"bool_and",
		"bool_or",
		"every",
		"xmlagg",
		"variance",
		"corr",
		"mode",
		"bit_and",
		"bit_or",
		"bit_xor",
		"range_agg",
		"range_intersect_agg",
		"any_value",
	];
	// The json ones also come as `_strict` / `_unique` variants.
	const PREFIXES: &[&str] = &[
		"stddev",
		"var_",
		"covar_",
		"regr_",
		"percentile_",
		"json_agg",
		"jsonb_agg",
		"json_object_agg",
		"jsonb_object_agg",
	];
	NAMES.contains(&name.as_str()) || PREFIXES.iter().any(|p| name.starts_with(p))
}

/// The statements that change the schema (or maintain or lock tables): routed by the relations
/// they name, home when nobody distributed them and refused on a distributed one until DDL
/// reaches every node (Phase 3).
fn is_ddl(n: &NodeEnum) -> bool {
	use NodeEnum as N;
	matches!(
		n,
		N::CreateStmt(_)
			| N::CreateForeignTableStmt(_)
			| N::AlterTableStmt(_)
			| N::DropStmt(_)
			| N::IndexStmt(_)
			| N::RenameStmt(_)
			| N::TruncateStmt(_)
			| N::CreateTableAsStmt(_)
			| N::ViewStmt(_)
			| N::CommentStmt(_)
			| N::GrantStmt(_)
			| N::GrantRoleStmt(_)
			| N::AlterDefaultPrivilegesStmt(_)
			| N::AlterSeqStmt(_)
			| N::CreateSeqStmt(_)
			| N::CreateSchemaStmt(_)
			| N::CreateFunctionStmt(_)
			| N::AlterFunctionStmt(_)
			| N::CreateTrigStmt(_)
			| N::CreateEventTrigStmt(_)
			| N::AlterEventTrigStmt(_)
			| N::CreatePolicyStmt(_)
			| N::AlterPolicyStmt(_)
			| N::CreateExtensionStmt(_)
			| N::AlterExtensionStmt(_)
			| N::CreateDomainStmt(_)
			| N::AlterDomainStmt(_)
			| N::CompositeTypeStmt(_)
			| N::CreateEnumStmt(_)
			| N::CreateRangeStmt(_)
			| N::AlterEnumStmt(_)
			| N::AlterTypeStmt(_)
			| N::DefineStmt(_)
			| N::RuleStmt(_)
			| N::AlterObjectSchemaStmt(_)
			| N::AlterOwnerStmt(_)
			| N::AlterOperatorStmt(_)
			| N::CreateRoleStmt(_)
			| N::AlterRoleStmt(_)
			| N::AlterRoleSetStmt(_)
			| N::DropRoleStmt(_)
			| N::DropOwnedStmt(_)
			| N::ReassignOwnedStmt(_)
			| N::CreateStatsStmt(_)
			| N::AlterStatsStmt(_)
			| N::SecLabelStmt(_)
			| N::CreateCastStmt(_)
			| N::CreateConversionStmt(_)
			| N::CreateTransformStmt(_)
			| N::CreateOpClassStmt(_)
			| N::CreateOpFamilyStmt(_)
			| N::AlterOpFamilyStmt(_)
			| N::CreateAmStmt(_)
			| N::CreatePlangStmt(_)
			| N::AlterCollationStmt(_)
			| N::AlterTsdictionaryStmt(_)
			| N::AlterTsconfigurationStmt(_)
			| N::CreateFdwStmt(_)
			| N::AlterFdwStmt(_)
			| N::CreateForeignServerStmt(_)
			| N::AlterForeignServerStmt(_)
			| N::CreateUserMappingStmt(_)
			| N::AlterUserMappingStmt(_)
			| N::DropUserMappingStmt(_)
			| N::ImportForeignSchemaStmt(_)
			| N::CreatePublicationStmt(_)
			| N::AlterPublicationStmt(_)
			| N::CreateSubscriptionStmt(_)
			| N::AlterSubscriptionStmt(_)
			| N::DropSubscriptionStmt(_)
			| N::CreateTableSpaceStmt(_)
			| N::DropTableSpaceStmt(_)
			| N::AlterTableSpaceOptionsStmt(_)
			| N::AlterTableMoveAllStmt(_)
			| N::CreatedbStmt(_)
			| N::AlterDatabaseStmt(_)
			| N::AlterDatabaseSetStmt(_)
			| N::AlterDatabaseRefreshCollStmt(_)
			| N::DropdbStmt(_)
			| N::AlterSystemStmt(_)
			| N::LockStmt(_)
			| N::VacuumStmt(_)
			| N::ClusterStmt(_)
			| N::ReindexStmt(_)
			| N::RefreshMatViewStmt(_)
			| N::CheckPointStmt(_)
	)
}

/// The parse node's type name, for a refusal ("CreateTableSpaceStmt").
fn node_name(n: &NodeEnum) -> String {
	let s = format!("{n:?}");
	s.split(['(', ' ', '{'])
		.next()
		.unwrap_or_default()
		.to_string()
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use super::*;
	use crate::catalog::{Keyspace, Node as CatalogNode, NodeId, NodeState, Strategy};
	use crate::config::SslMode;
	use crate::hash::KeyType;
	use crate::route::{Route, route};

	fn rel(s: &str, t: &str) -> RelationName {
		RelationName {
			schema: s.into(),
			table: t.into(),
		}
	}

	/// `app`: orders and items sharded by tenant_id (int8), events by device (uuid), countries
	/// a reference table, plans global; four nodes.
	fn cat() -> Catalog {
		let ids: Vec<NodeId> = (1..=4).map(NodeId).collect();
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
		for (name, key_type, seed) in [("tenant", KeyType::Int8, 1), ("device", KeyType::Uuid, 2)] {
			c.keyspaces.insert(
				name.into(),
				Keyspace {
					name: name.into(),
					strategy: Strategy::Hash,
					key_type,
					seed,
					ranges: Keyspace::even_ranges(32, &ids),
					pins: HashMap::new(),
				},
			);
		}
		let sharded = |k: &str, col: &str| RelationKind::Sharded {
			keyspace: k.into(),
			key_column: col.into(),
		};
		c.relations
			.insert(rel("app", "orders"), sharded("tenant", "tenant_id"));
		c.relations
			.insert(rel("app", "items"), sharded("tenant", "tenant_id"));
		c.relations
			.insert(rel("app", "events"), sharded("device", "device"));
		c.relations
			.insert(rel("app", "countries"), RelationKind::Reference);
		c.relations
			.insert(rel("app", "plans"), RelationKind::Global);
		c
	}

	fn path() -> Vec<String> {
		vec!["app".into()]
	}

	fn all(sql: &str) -> Vec<Statement> {
		analyze(sql, &cat(), &path()).unwrap_or_else(|e| panic!("{sql}: {e:?}"))
	}

	fn one(sql: &str) -> Facts {
		let mut s = all(sql);
		assert_eq!(s.len(), 1, "{sql}");
		s.remove(0).facts
	}

	fn unsupported(sql: &str) -> String {
		match analyze(sql, &cat(), &path()) {
			Err(AnalyzeError::Unsupported(m)) => m,
			other => panic!("{sql}: expected Unsupported, got {other:?}"),
		}
	}

	fn c(s: &str) -> KeyValue {
		KeyValue::Const(s.into())
	}

	fn names(f: &Facts) -> Vec<String> {
		f.tables.iter().map(|t| t.name.to_string()).collect()
	}

	/// The pins of every use of `table`, in order.
	fn pins(f: &Facts, table: &str) -> Vec<Option<Vec<KeyValue>>> {
		f.tables
			.iter()
			.filter(|t| t.name.table == table)
			.map(|t| t.key_values.clone())
			.collect()
	}

	/// The pin of the one use of the key in `sql`'s WHERE, on orders.
	fn pin(sql: &str) -> Option<Vec<KeyValue>> {
		let f = one(sql);
		assert_eq!(f.tables.len(), 1, "{sql}");
		f.tables[0].key_values.clone()
	}

	fn joined(f: &Facts, a: usize, b: usize) -> bool {
		f.key_joins
			.iter()
			.any(|&(x, y)| (x, y) == (a, b) || (y, x) == (a, b))
	}

	fn routed(sql: &str) -> Route {
		let cat = cat();
		route(&one(sql), &cat, &[])
	}

	// Rule 1: statements and their text.

	#[test]
	fn a_parse_error_is_a_syntax_error() {
		assert!(matches!(
			analyze("selec 1", &cat(), &path()),
			Err(AnalyzeError::Syntax(_))
		));
	}

	#[test]
	fn each_statement_carries_its_own_text() {
		let s = all("BEGIN;\n  select * from orders where tenant_id = 1 ;\n\tCOMMIT");
		let sql: Vec<&str> = s.iter().map(|s| s.sql.as_str()).collect();
		assert_eq!(
			sql,
			[
				"BEGIN",
				"select * from orders where tenant_id = 1",
				"COMMIT"
			]
		);
		assert_eq!(s[0].tx, Some(TxVerb::Begin));
		assert_eq!(s[1].facts.kind, Kind::Select);
		assert_eq!(s[1].facts.tables[0].key_values, Some(vec![c("1")]));
		assert_eq!(s[2].tx, Some(TxVerb::Commit));
		// Offsets are bytes: multibyte text before a statement must not shift the next one.
		let s = all("select 'h\u{e9}llo \u{2014} \u{fc}'; select 2;");
		assert_eq!(s[0].sql, "select 'h\u{e9}llo \u{2014} \u{fc}'");
		assert_eq!(s[1].sql, "select 2");
		assert!(all("").is_empty());
	}

	// Rule 2: relation names.

	#[test]
	fn names_resolve_through_the_search_path() {
		let cat = cat();
		let f = |sql: &str, path: &[&str]| {
			let path: Vec<String> = path.iter().map(|s| s.to_string()).collect();
			analyze(sql, &cat, &path).unwrap().remove(0).facts
		};
		assert_eq!(names(&f("select * from app.orders", &[])), ["app.orders"]);
		// The first schema that HAS the table, not merely the first schema.
		assert_eq!(
			names(&f("select * from orders", &["mine", "app"])),
			["app.orders"]
		);
		// An unknown table lands in the first schema, or public with an empty path.
		assert_eq!(
			names(&f("select * from nowhere", &["mine", "app"])),
			["mine.nowhere"]
		);
		assert_eq!(names(&f("select * from nowhere", &[])), ["public.nowhere"]);
		// The same name in a schema the catalog does not know is a different, unknown table.
		assert_eq!(
			names(&f("select * from orders", &["public"])),
			["public.orders"]
		);
		// Unknown is not sharded: no column-list rule.
		assert_eq!(
			f("insert into nowhere values (1)", &["app"]).insert_keys,
			None
		);
	}

	#[test]
	fn a_cte_shadows_a_table_of_its_name() {
		let f =
			one("with orders as (select 1 as tenant_id) select * from orders where tenant_id = 5");
		assert!(f.tables.is_empty());
		// Not recursive: the body's `orders` is the table; only the outer reference is the CTE.
		let f =
			one("with orders as (select * from orders where tenant_id = 3) select * from orders");
		assert_eq!(names(&f), ["app.orders"]);
		assert_eq!(pins(&f, "orders"), [Some(vec![c("3")])]);
		// Recursive: the name is the CTE inside its own body too.
		let f = one(
			"with recursive orders(n) as (select 1 union all select n + 1 from orders where n < 3) select * from orders",
		);
		assert!(f.tables.is_empty());
		// An enclosing level's CTE shadows inside a subquery; a qualified name is never a CTE.
		let f = one(
			"with items as (select 1 as x) select * from orders o where exists (select 1 from items) and o.tenant_id in (select tenant_id from app.items)",
		);
		assert_eq!(names(&f), ["app.orders", "app.items"]);
		assert!(joined(&f, 0, 1));
		// The scope ends with its statement.
		let f = one(
			"select (with items as (select 1) select count(*) from items), (select count(*) from items)",
		);
		assert_eq!(names(&f), ["app.items"]);
	}

	// Rule 3: kinds.

	#[test]
	fn kinds_of_data_statements() {
		assert_eq!(one("values (1), (2)").kind, Kind::Select);
		assert_eq!(one("select 1 union select 2").kind, Kind::Select);
		assert_eq!(
			one("insert into orders (tenant_id) values (1)").kind,
			Kind::Insert
		);
		assert_eq!(
			one("update orders set status = 'x' where tenant_id = 1").kind,
			Kind::Update
		);
		assert_eq!(
			one("delete from orders where tenant_id = 1").kind,
			Kind::Delete
		);
		let f = one("copy orders from stdin");
		assert_eq!((f.kind, f.tables[0].written), (Kind::CopyFrom, true));
		let f = one("copy orders to stdout");
		assert_eq!((f.kind, f.tables[0].written), (Kind::CopyTo, false));
		let f = one("copy (select * from items where tenant_id = 4) to stdout");
		assert_eq!(f.kind, Kind::CopyTo);
		assert_eq!(pins(&f, "items"), [Some(vec![c("4")])]);
	}

	#[test]
	fn explain_is_the_statement_it_explains() {
		let f = one("explain select * from orders where tenant_id = 9 order by 1");
		assert_eq!(f.kind, Kind::Select);
		assert_eq!(pins(&f, "orders"), [Some(vec![c("9")])]);
		assert_eq!(f.needs_merge, ["ORDER BY"]);
		// EXPLAIN ANALYZE of a write runs the write.
		let f = one("explain (analyze) delete from orders where tenant_id = 9");
		assert_eq!(f.kind, Kind::Delete);
		assert!(f.tables[0].written);
	}

	#[test]
	fn transaction_control() {
		let tx = |sql: &str| {
			let s = all(sql).remove(0);
			assert_eq!(s.facts.kind, Kind::Transaction, "{sql}");
			s.tx.unwrap()
		};
		assert_eq!(tx("begin"), TxVerb::Begin);
		assert_eq!(
			tx("start transaction isolation level serializable"),
			TxVerb::Begin
		);
		assert_eq!(tx("commit"), TxVerb::Commit);
		assert_eq!(tx("end"), TxVerb::Commit);
		assert_eq!(tx("rollback"), TxVerb::Rollback);
		assert_eq!(tx("abort"), TxVerb::Rollback);
		for sql in [
			"rollback to savepoint a",
			"savepoint a",
			"release a",
			"prepare transaction 'g'",
			"commit prepared 'g'",
			"rollback prepared 'g'",
		] {
			assert_eq!(tx(sql), TxVerb::Other, "{sql}");
		}
	}

	#[test]
	fn session_state() {
		let verb = |sql: &str| {
			let s = all(sql).remove(0);
			assert_eq!(s.facts.kind, Kind::Session, "{sql}");
			s.session.unwrap()
		};
		let set = |p| SessionVerb::Set {
			touches_search_path: p,
		};
		assert_eq!(verb("set work_mem = '64MB'"), set(false));
		assert_eq!(verb("set search_path = app, public"), set(true));
		assert_eq!(verb("set schema 'app'"), set(true));
		assert_eq!(verb("reset search_path"), set(true));
		assert_eq!(verb("set work_mem to default"), set(false));
		assert_eq!(verb("set local work_mem = '1MB'"), SessionVerb::SetLocal);
		assert_eq!(verb("set local search_path = x"), SessionVerb::SetLocal);
		assert_eq!(
			verb("set transaction isolation level serializable"),
			SessionVerb::SetLocal
		);
		assert_eq!(
			verb("set session characteristics as transaction deferrable"),
			SessionVerb::Set {
				touches_search_path: false
			}
		);
		assert_eq!(verb("set constraints all deferred"), SessionVerb::SetLocal);
		assert_eq!(verb("reset all"), SessionVerb::ResetAll);
		assert_eq!(verb("discard all"), SessionVerb::ResetAll);
		assert_eq!(verb("discard plans"), SessionVerb::Other);
		for sql in [
			"show work_mem",
			"listen x",
			"unlisten *",
			"notify x",
			"load 'auto_explain'",
		] {
			assert_eq!(verb(sql), SessionVerb::Other, "{sql}");
		}
	}

	#[test]
	fn ddl_names_every_relation() {
		let ddl = |sql: &str| {
			let f = one(sql);
			assert_eq!(f.kind, Kind::Ddl, "{sql}");
			assert!(
				f.tables.iter().all(|t| t.written && t.key_values.is_none()),
				"{sql}"
			);
			assert!(f.key_joins.is_empty() && f.needs_merge.is_empty(), "{sql}");
			names(&f)
		};
		assert_eq!(
			ddl("create table app.t (id int references orders (order_id), like items)"),
			["app.t", "app.orders", "app.items"]
		);
		assert_eq!(ddl("alter table orders add column x int"), ["app.orders"]);
		assert_eq!(
			ddl("drop table orders, app.plans"),
			["app.orders", "app.plans"]
		);
		assert_eq!(ddl("drop trigger tr on items"), ["app.items"]);
		assert_eq!(ddl("create index on orders (status)"), ["app.orders"]);
		assert_eq!(ddl("truncate orders, items"), ["app.orders", "app.items"]);
		assert_eq!(
			ddl("create table t2 as select * from orders where tenant_id = 1 order by 1"),
			["app.orders", "app.t2"]
		);
		assert_eq!(ddl("select * into t3 from items"), ["app.items", "app.t3"]);
		assert_eq!(
			ddl("create view v as select * from orders join items using (tenant_id)"),
			["app.v", "app.orders", "app.items"]
		);
		assert_eq!(
			ddl("comment on column orders.status is 'x'"),
			["app.orders"]
		);
		assert_eq!(ddl("grant select on orders to someone"), ["app.orders"]);
		assert_eq!(ddl("alter sequence s restart"), ["app.s"]);
		assert_eq!(ddl("create sequence s2"), ["app.s2"]);
		assert_eq!(ddl("alter table orders rename to orders2"), ["app.orders"]);
		assert!(ddl("create function f() returns int language sql as 'select 1'").is_empty());
	}

	#[test]
	fn opaque_statements_are_refused_only_when_something_is_sharded() {
		assert!(unsupported("do $$ begin perform 1; end $$").contains("DO block"));
		assert!(unsupported("call p()").contains("CALL"));
		for sql in ["prepare q as select 1", "execute q", "deallocate q"] {
			assert!(unsupported(sql).contains("PREPARE"), "{sql}");
		}
		assert!(
			unsupported(
				"merge into orders o using items i on o.tenant_id = i.tenant_id when matched then delete"
			)
			.contains("MERGE")
		);
		assert!(unsupported("declare c cursor for select 1").contains("DeclareCursorStmt"));
		// With nothing sharded they cannot go wrong: home.
		let mut cat = cat();
		cat.relations
			.retain(|_, k| !matches!(k, RelationKind::Sharded { .. }));
		for sql in [
			"do $$ begin end $$",
			"call p()",
			"execute q",
			"merge into plans p using plans q on p.id = q.id when matched then delete",
		] {
			let s = analyze(sql, &cat, &path()).unwrap().remove(0);
			assert_eq!(s.facts.kind, Kind::Other, "{sql}");
		}
	}

	// Rule 4: tables at every depth.

	#[test]
	fn tables_at_every_depth_with_what_is_written() {
		let f = one(
			"with x as (select * from events) select (select 1 from countries limit 1) from orders o \
			 join items i on i.tenant_id = o.tenant_id \
			 where exists (select 1 from plans p where p.id = 1) and o.order_id in (select order_id from items)",
		);
		assert_eq!(
			names(&f),
			[
				"app.events",
				"app.orders",
				"app.items",
				"app.plans",
				"app.items",
				"app.countries"
			]
		);
		assert!(f.tables.iter().all(|t| !t.written));
		let f =
			one("insert into items (tenant_id, order_id) select tenant_id, order_id from orders");
		assert_eq!(names(&f), ["app.items", "app.orders"]);
		assert_eq!(
			f.tables.iter().map(|t| t.written).collect::<Vec<_>>(),
			[true, false]
		);
		let f = one(
			"update orders o set status = i.sku from items i where i.tenant_id = o.tenant_id and o.tenant_id = 2",
		);
		assert_eq!(names(&f), ["app.orders", "app.items"]);
		assert!(f.tables[0].written && !f.tables[1].written);
		assert!(joined(&f, 0, 1));
		assert_eq!(pins(&f, "orders"), [Some(vec![c("2")])]);
		let f = one(
			"delete from orders o using items i where i.tenant_id = o.tenant_id and i.tenant_id = 2",
		);
		assert!(f.tables[0].written && !f.tables[1].written);
		assert_eq!(pins(&f, "items"), [Some(vec![c("2")])]);
		// A data-modifying CTE writes. One that writes a sharded table inside a read is
		// refused: route() would scatter it like a read.
		let f = one("with gone as (delete from plans where id = 1 returning *) select * from gone");
		assert!(f.tables[0].written);
		assert!(
			unsupported(
				"with gone as (delete from orders where tenant_id = 1 returning *) select * from gone"
			)
			.contains("app.orders")
		);
		assert!(
			unsupported("copy (delete from orders where tenant_id = 1 returning *) to stdout")
				.contains("app.orders")
		);
		// Under a write it is a write, so route() treats it as one.
		let f = one(
			"with gone as (delete from orders where tenant_id = 1 returning *) insert into items (tenant_id) select tenant_id from gone",
		);
		assert_eq!(f.kind, Kind::Insert);
		assert!(f.tables[0].written);
	}

	// Rule 5: what the key equals.

	#[test]
	fn constants_parameters_and_lists_pin() {
		let p = |sql: &str| pin(&format!("select * from orders where {sql}"));
		assert_eq!(p("tenant_id = 42"), Some(vec![c("42")]));
		assert_eq!(p("42 = tenant_id"), Some(vec![c("42")]));
		assert_eq!(p("tenant_id = $1"), Some(vec![KeyValue::Param(1)]));
		assert_eq!(
			p("tenant_id = $2::int8"),
			Some(vec![KeyValue::Cast(
				Box::new(KeyValue::Param(2)),
				"int8".into()
			)])
		);
		assert_eq!(
			p("tenant_id in (1, 2, $3)"),
			Some(vec![c("1"), c("2"), KeyValue::Param(3)])
		);
		assert_eq!(p("tenant_id is null"), Some(vec![KeyValue::Null]));
		assert_eq!(p("tenant_id = null"), Some(vec![KeyValue::Null]));
		assert_eq!(
			p("tenant_id = '7'::bigint"),
			Some(vec![KeyValue::Cast(Box::new(c("7")), "int8".into())])
		);
		assert_eq!(
			p("tenant_id = cast('8' as int8)"),
			Some(vec![KeyValue::Cast(Box::new(c("8")), "int8".into())])
		);
		assert_eq!(p("tenant_id = -5"), Some(vec![c("-5")]));
		assert_eq!(
			p("tenant_id = -9223372036854775808"),
			Some(vec![c("-9223372036854775808")])
		);
		assert_eq!(p("tenant_id = 1.5"), Some(vec![c("1.5")]));
		assert_eq!(p("tenant_id = 'abc'"), Some(vec![c("abc")]));
		assert_eq!(p("tenant_id = 1 and status = 'x'"), Some(vec![c("1")]));
		assert_eq!(p("(status = 'x' and (tenant_id = 1))"), Some(vec![c("1")]));
		// The fewest values of several ANDed pins, in either order.
		assert_eq!(
			p("tenant_id in (1, 2) and tenant_id = 2"),
			Some(vec![c("2")])
		);
		assert_eq!(
			p("tenant_id = 2 and tenant_id in (1, 2)"),
			Some(vec![c("2")])
		);
	}

	#[test]
	fn anything_else_leaves_the_key_unpinned() {
		let p = |sql: &str| pin(&format!("select * from orders where {sql}"));
		for sql in [
			"tenant_id = 1 or tenant_id = 2",
			"tenant_id = 1 or status = 'x'",
			"not (tenant_id = 1)",
			"tenant_id = abs(-3)",
			"tenant_id = any(array[1, 2])",
			"tenant_id between 1 and 2",
			"tenant_id > 1",
			"tenant_id <> 1",
			"tenant_id not in (1)",
			"tenant_id in (1, abs(2))",
			"tenant_id is not null",
			"tenant_id::text = '1'",
			"tenant_id = order_id",
			// A cast with a modifier can change the value.
			"tenant_id = '123'::varchar(2)",
			"tenant_id operator(pg_catalog.=) 1",
		] {
			assert_eq!(p(sql), None, "{sql}");
		}
	}

	#[test]
	fn qualified_references_bind_by_alias_or_name() {
		assert_eq!(
			pin("select * from orders o where o.tenant_id = 1"),
			Some(vec![c("1")])
		);
		assert_eq!(
			pin("select * from orders where orders.tenant_id = 1"),
			Some(vec![c("1")])
		);
		assert_eq!(
			pin("select * from app.orders where app.orders.tenant_id = 1"),
			Some(vec![c("1")])
		);
		// An aliased table is known only by its alias (Postgres refuses the name).
		assert_eq!(
			pin("select * from orders o where orders.tenant_id = 1"),
			None
		);
		// Column aliases rename by position: the key is no longer known by its name.
		assert_eq!(
			pin("select * from orders o(a, b) where o.tenant_id = 1"),
			None
		);
		// Another level's alias of the same name shadows.
		let f = one(
			"select * from orders o where exists (select 1 from items o where o.tenant_id = 1)",
		);
		assert_eq!(pins(&f, "orders"), [None]);
		assert_eq!(pins(&f, "items"), [Some(vec![c("1")])]);
	}

	#[test]
	fn an_ambiguous_unqualified_key_pins_nothing_unless_it_is_a_join_column() {
		let f = one("select * from orders o, items i where tenant_id = 1");
		assert_eq!(
			(pins(&f, "orders"), pins(&f, "items")),
			(vec![None], vec![None])
		);
		let f = one("select * from orders o join items i using (order_id) where tenant_id = 1");
		assert_eq!(
			(pins(&f, "orders"), pins(&f, "items")),
			(vec![None], vec![None])
		);
		for sql in [
			"select * from orders o join items i using (tenant_id) where tenant_id = 1",
			"select * from orders natural join items where tenant_id = 1",
			"select * from orders o full join items i using (tenant_id) where tenant_id = 1",
			"select * from orders o join items i using (tenant_id) join items j using (tenant_id) where tenant_id = 1",
		] {
			let f = one(sql);
			assert!(
				f.tables.iter().all(|t| t.key_values == Some(vec![c("1")])),
				"{sql}"
			);
			assert!(joined(&f, 0, 1), "{sql}");
		}
		// A USING column can be null-extended: IS NULL on it says nothing about either side.
		let f = one(
			"select * from orders o full join items i using (tenant_id) where tenant_id is null",
		);
		assert!(f.tables.iter().all(|t| t.key_values.is_none()));
	}

	#[test]
	fn outer_joins_pin_only_what_their_where_filters() {
		// The ON of an outer join decides matches, not survivors.
		let f = one(
			"select * from orders o left join items i on i.tenant_id = o.tenant_id and i.tenant_id = 5 and o.tenant_id = 5",
		);
		assert_eq!(
			(pins(&f, "orders"), pins(&f, "items")),
			(vec![None], vec![None])
		);
		assert!(joined(&f, 0, 1));
		// A strict WHERE on the nullable side filters; IS NULL is true of a null-extended row.
		let f = one(
			"select * from orders o left join items i on i.tenant_id = o.tenant_id where i.tenant_id = 5",
		);
		assert_eq!(pins(&f, "items"), [Some(vec![c("5")])]);
		let f = one(
			"select * from orders o left join items i on i.tenant_id = o.tenant_id where i.tenant_id is null",
		);
		assert_eq!(pins(&f, "items"), [None]);
		let f = one(
			"select * from orders o left join items i on i.tenant_id = o.tenant_id where o.tenant_id is null",
		);
		assert_eq!(pins(&f, "orders"), [Some(vec![KeyValue::Null])]);
		// An inner join's ON filters, unless it sits inside an outer join's nullable side.
		let f = one(
			"select * from orders o join items i on i.tenant_id = o.tenant_id and o.tenant_id = 5",
		);
		assert_eq!(pins(&f, "orders"), [Some(vec![c("5")])]);
		let f = one(
			"select * from countries c left join (orders o join items i on o.tenant_id = i.tenant_id and o.tenant_id = 5) on true",
		);
		assert_eq!(pins(&f, "orders"), [None]);
		assert!(joined(&f, 1, 2));
	}

	#[test]
	fn a_pin_belongs_to_the_level_it_is_written_in() {
		let f = one(
			"select * from items where order_id in (select order_id from orders where tenant_id = 5)",
		);
		assert_eq!(
			(pins(&f, "items"), pins(&f, "orders")),
			(vec![None], vec![Some(vec![c("5")])])
		);
		// A comparison with an OUTER table's key, inside a subquery, restricts nothing there.
		let f = one(
			"select * from orders o where exists (select 1 from items i where o.tenant_id = 5)",
		);
		assert_eq!(
			(pins(&f, "orders"), pins(&f, "items")),
			(vec![None], vec![None])
		);
	}

	// Rule 6: key joins.

	#[test]
	fn key_joins_at_one_level() {
		assert!(joined(
			&one("select * from orders o, items i where o.tenant_id = i.tenant_id"),
			0,
			1
		));
		assert!(joined(
			&one("select * from orders o join items i on i.tenant_id = o.tenant_id"),
			0,
			1
		));
		assert!(joined(
			&one("select * from orders o join items i using (tenant_id)"),
			0,
			1
		));
		assert!(!joined(
			&one("select * from orders o join items i on i.order_id = o.order_id"),
			0,
			1
		));
		assert!(!joined(
			&one("select * from orders o join items i on i.tenant_id = o.tenant_id or true"),
			0,
			1
		));
	}

	#[test]
	fn key_joins_through_subqueries_only_where_they_filter() {
		let j = |sql: &str| joined(&one(sql), 0, 1);
		assert!(j(
			"select * from orders o where o.tenant_id in (select tenant_id from items)"
		));
		assert!(j(
			"select * from orders o where o.tenant_id in (select distinct i.tenant_id from items i where i.sku = 'a')"
		));
		assert!(j(
			"select * from orders o where exists (select 1 from items i where i.tenant_id = o.tenant_id)"
		));
		// Unqualified, the inner level's own key.
		assert!(j(
			"select * from orders o where exists (select 1 from items i where tenant_id = o.tenant_id)"
		));
		assert!(j(
			"select * from orders o, lateral (select * from items i where i.tenant_id = o.tenant_id limit 1) x"
		));
		// A LIMIT, an aggregate or a grouping changes what the subquery returns per node.
		assert!(!j(
			"select * from orders o where o.tenant_id in (select tenant_id from items limit 3)"
		));
		assert!(!j(
			"select * from orders o where o.tenant_id in (select tenant_id from items group by tenant_id)"
		));
		assert!(!j(
			"select * from orders o where exists (select count(*) from items i where i.tenant_id = o.tenant_id)"
		));
		assert!(!j(
			"select * from orders o where exists (select 1 from items i where i.tenant_id = o.tenant_id having count(*) = 0)"
		));
		// Not a filter of the outer rows.
		assert!(!j(
			"select * from orders o where not exists (select 1 from items i where i.tenant_id = o.tenant_id)"
		));
		assert!(!j(
			"select * from orders o where o.status = 'x' or exists (select 1 from items i where i.tenant_id = o.tenant_id)"
		));
		assert!(!j(
			"select o.*, (select count(*) from items i where i.tenant_id = o.tenant_id) from orders o"
		));
		assert!(!j(
			"select * from orders o left join lateral (select * from items i where i.tenant_id = o.tenant_id) x on true"
		));
		assert!(!j(
			"select * from orders o where exists (select 1 from items i where i.tenant_id = o.tenant_id union select 1)"
		));
		// Two levels down, through a positive level and through a negative one.
		let f = one(
			"select * from orders o where exists (select 1 from items i where exists (select 1 from items j where j.tenant_id = o.tenant_id))",
		);
		assert!(joined(&f, 0, 2) && !joined(&f, 0, 1));
		let f = one(
			"select * from orders o where not exists (select 1 from items i where exists (select 1 from items j where j.tenant_id = o.tenant_id))",
		);
		assert!(!joined(&f, 0, 2));
		// A subquery in FROM that is not LATERAL cannot see its siblings.
		let f = one(
			"select * from orders o where exists (select 1 from items o2, (select 1 from items x where x.tenant_id = o2.tenant_id) s)",
		);
		assert!(!joined(&f, 1, 2));
		let f = one(
			"select * from orders o where exists (select 1 from items o2, lateral (select 1 from items x where x.tenant_id = o2.tenant_id) s)",
		);
		assert!(joined(&f, 1, 2));
	}

	// Rule 7: assigning the key.

	#[test]
	fn assigning_the_key() {
		assert!(one("update orders set tenant_id = 2 where tenant_id = 1").assigns_key);
		assert!(
			one("update orders set (status, tenant_id) = ('x', 2) where tenant_id = 1").assigns_key
		);
		assert!(!one("update orders set status = 'x' where tenant_id = 1").assigns_key);
		assert!(!one("update plans set id = 2").assigns_key);
		assert!(
			one("insert into orders (tenant_id, order_id) values (1, 2) on conflict (tenant_id, order_id) do update set tenant_id = 3")
				.assigns_key
		);
		assert!(
			!one("insert into orders (tenant_id, order_id) values (1, 2) on conflict (tenant_id, order_id) do update set status = excluded.status")
				.assigns_key
		);
	}

	// Rule 8: inserted keys.

	#[test]
	fn inserted_rows_carry_their_keys() {
		let f = one(
			"insert into orders (order_id, tenant_id) values (1, 10), (2, $1), (3, '12'::int8), (4, -7), (5, null) returning *",
		);
		assert_eq!(
			f.insert_keys,
			Some(vec![
				c("10"),
				KeyValue::Param(1),
				KeyValue::Cast(Box::new(c("12")), "int8".into()),
				c("-7"),
				KeyValue::Null
			])
		);
		assert_eq!(f.tables[0].key_values, None);
		assert_eq!(
			one("insert into orders (tenant_id) select tenant_id from items").insert_keys,
			None
		);
		assert_eq!(
			one("insert into plans values (1, 'x', 3)").insert_keys,
			None
		);
		// A row that reads a table cannot be split by its key; the keys pin the target instead.
		let f = one(
			"insert into orders (tenant_id, total_cents) values (5, (select max(price_cents) from items))",
		);
		assert_eq!(f.insert_keys, None);
		assert_eq!(pins(&f, "orders"), [Some(vec![c("5")])]);
	}

	#[test]
	fn an_insert_lepis_cannot_place_is_refused() {
		let literal = "the shard key of an inserted row must be a literal or a parameter";
		assert_eq!(
			unsupported("insert into orders (tenant_id) values (default)"),
			literal
		);
		assert_eq!(
			unsupported("insert into orders (tenant_id) values (1), (1 + 1)"),
			literal
		);
		let columns =
			"name the columns of an INSERT into a sharded table, so Lepis can find the shard key";
		assert_eq!(unsupported("insert into orders values (1)"), columns);
		assert_eq!(
			unsupported("insert into orders select * from orders"),
			columns
		);
		assert!(unsupported("insert into orders (order_id) values (1)").contains("tenant_id"));
	}

	// Rule 9: what a merge across nodes would get wrong.

	#[test]
	fn constructs_that_need_a_merge() {
		let m = |sql: &str| one(sql).needs_merge;
		assert!(m("select * from orders where tenant_id > 3").is_empty());
		assert_eq!(
			m(
				"select tenant_id, count(*) from orders group by tenant_id having count(*) > 1 order by 1 limit 5 offset 2"
			),
			[
				"ORDER BY",
				"LIMIT",
				"OFFSET",
				"GROUP BY",
				"HAVING",
				"an aggregate"
			]
		);
		assert_eq!(m("select distinct status from orders"), ["DISTINCT"]);
		assert_eq!(m("select distinct on (status) * from orders"), ["DISTINCT"]);
		assert_eq!(
			m("select rank() over (order by total_cents) from orders"),
			["a window function"]
		);
		assert_eq!(
			m("select sum(total_cents) over () from orders"),
			["a window function"]
		);
		assert_eq!(
			m("select count(*) filter (where value is null) from events"),
			["an aggregate"]
		);
		assert_eq!(
			m("select percentile_cont(0.5) within group (order by value) from events"),
			["an aggregate"]
		);
		assert_eq!(
			m("select stddev_pop(value), regr_slope(value, seq), json_agg(kind) from events"),
			["an aggregate"]
		);
		assert_eq!(
			m("select json_arrayagg(kind) from events"),
			["an aggregate"]
		);
		assert_eq!(
			m("select * from orders union all select * from orders"),
			["a set operation"]
		);
		// At any depth.
		assert_eq!(
			m("select * from orders where total_cents > (select avg(total_cents) from orders)"),
			["an aggregate"]
		);
		assert_eq!(
			m("select * from (select * from orders order by 1 limit 1) x"),
			["ORDER BY", "LIMIT"]
		);
		// Only a read is merged.
		assert!(
			m("update orders set status = (select max(sku) from items) where tenant_id = 1")
				.is_empty()
		);
	}

	#[test]
	fn what_cannot_be_read_is_refused_with_sharded_tables() {
		assert!(unsupported("select * from orders tablesample system (10)").contains("app.orders"));
		one("select * from plans tablesample system (10)");
		assert!(
			unsupported(
				"select * from json_table('[]'::jsonb, '$[*]' columns (a int path '$.a')) jt"
			)
			.contains("JsonTable")
		);
	}

	// Through route(): the traps, end to end.

	#[test]
	fn the_traps_never_reach_a_single_node() {
		let cat = cat();
		let five = Route::Node(cat.owner("tenant", "5").unwrap());
		// A real filter on a key join: one node.
		assert_eq!(
			routed(
				"select * from orders o where exists (select 1 from items i where i.tenant_id = o.tenant_id and i.tenant_id = 5)"
			),
			five
		);
		assert_eq!(
			routed(
				"select * from orders o left join items i on i.tenant_id = o.tenant_id where o.tenant_id = 5"
			),
			five
		);
		for sql in [
			// Orders on every node satisfy these.
			"select * from orders o where not exists (select 1 from items i where i.tenant_id = o.tenant_id and i.tenant_id = 5)",
			"select * from orders o where exists (select 1 from items i where i.tenant_id = o.tenant_id and i.tenant_id = 5 having count(*) = 0)",
			"select * from orders o left join items i on i.tenant_id = o.tenant_id and i.tenant_id = 5",
			"select * from orders o left join items i on i.tenant_id = o.tenant_id where i.tenant_id is null",
			"select * from orders o, items i where tenant_id = 5",
			"select * from orders where tenant_id = 5 or tenant_id = 6",
			"with orders as (select 5 as tenant_id) select * from app.orders o, orders where orders.tenant_id = 5",
		] {
			let r = routed(sql);
			assert!(
				matches!(r, Route::Scatter(_) | Route::Refuse(_)),
				"{sql}: {r:?}"
			);
		}
	}
}
