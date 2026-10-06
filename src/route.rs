//! Where a statement goes: a pure function of what the statement says (its `Facts`, which the
//! parser adapter extracts) and the catalog snapshot.
//!
//! The rule that shapes everything here (L9): a statement either goes somewhere Lepis can prove
//! gives exactly the answer one Postgres would, or it is REFUSED with a sentence naming the fix.
//! It is never run somewhere it might be wrong. The oracle (tests/oracle.rs) is what holds this
//! file to that.

use std::collections::BTreeSet;

use crate::catalog::{Catalog, NodeId, RelationKind, RelationName};

/// A value the key is compared with: a literal in the SQL, or a parameter bound later.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyValue {
	/// Postgres's text form of the literal.
	Const(String),
	/// `$n`, 1-based.
	Param(usize),
	/// `NULL`: matches nothing under `=`, so it constrains to no node.
	Null,
	/// A value cast to a type (its name as the parser gives it, unqualified): the value is
	/// that type's, and only a type whose values hash as the key's may pin it.
	Cast(Box<KeyValue>, String),
}

/// One relation a statement reads or writes, with what the WHERE clause pins its key to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableUse {
	pub name: RelationName,
	/// Every value the key is equal to under the statement's top-level conjunction
	/// (`key = v`, `key IN (…)`); None when the key is not pinned.
	pub key_values: Option<Vec<KeyValue>>,
	pub written: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
	Select,
	Insert,
	Update,
	Delete,
	CopyFrom,
	CopyTo,
	/// CREATE / ALTER / DROP and friends.
	Ddl,
	/// BEGIN, COMMIT, ROLLBACK, SAVEPOINT …
	Transaction,
	/// SET, SHOW, RESET, DISCARD, LISTEN, … : session state.
	Session,
	Other,
}

/// What the parser adapter found in one statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Facts {
	pub kind: Kind,
	pub tables: Vec<TableUse>,
	/// Pairs of tables (indexes into `tables`) joined by equality of their shard keys.
	pub key_joins: Vec<(usize, usize)>,
	/// An UPDATE that assigns the shard key: it would move the row to another node.
	pub assigns_key: bool,
	/// For INSERT … VALUES: the key of each row, in order.
	pub insert_keys: Option<Vec<KeyValue>>,
	/// Constructs that are correct on one node and need merging across nodes: window functions,
	/// ORDER BY, LIMIT, aggregates, DISTINCT, … (scatter.rs plans the merge from the tree).
	pub needs_merge: Vec<&'static str>,
}

/// Transaction control, which the router handles itself (a BEGIN binds to a node lazily).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxVerb {
	/// BEGIN / START TRANSACTION.
	Begin,
	/// COMMIT / END.
	Commit,
	/// ROLLBACK / ABORT (not ROLLBACK TO SAVEPOINT).
	Rollback,
	/// SAVEPOINT, RELEASE, ROLLBACK TO, PREPARE TRANSACTION, COMMIT/ROLLBACK PREPARED.
	Other,
}

/// Session state, which every node of the session must agree on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionVerb {
	/// `SET` (not `SET LOCAL`) or `SET … TO DEFAULT` / `RESET x`: replayed on every node.
	Set { touches_search_path: bool },
	/// `SET LOCAL`, `SET CONSTRAINTS`, `SET TRANSACTION`: this transaction's node only.
	SetLocal,
	/// `RESET ALL`, `DISCARD ALL`: clears what is replayed.
	ResetAll,
	/// `SHOW`, `LISTEN`, `UNLISTEN`, `NOTIFY`, `LOAD`, … : home (or the transaction's node).
	Other,
}

/// One statement of a query string, as the parser adapter (analyze.rs) describes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Statement {
	/// The statement's own text, without the others of a multi-statement string.
	pub sql: String,
	pub facts: Facts,
	pub tx: Option<TxVerb>,
	pub session: Option<SessionVerb>,
	/// What it leaves in the node's session that transaction pooling cannot carry
	/// (`analyze::session_bound`).
	pub session_bound: Option<&'static str>,
}

/// Why a statement could not be analysed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AnalyzeError {
	/// Lepis's parser rejected it. The home node decides whether it really is invalid.
	Syntax(String),
	/// Valid, but uses something Lepis cannot route correctly yet; the sentence says what.
	Unsupported(String),
}

/// The decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Route {
	/// One node, the statement unchanged.
	Node(NodeId),
	/// The home node: global tables, session commands, anything with no sharded table.
	Home,
	/// The statement split by node: INSERT rows grouped by owner (row indexes per node).
	SplitInsert(Vec<(NodeId, Vec<usize>)>),
	/// A read across these nodes whose results are merged (scatter.rs, merge.rs).
	Scatter(Vec<NodeId>),
	/// COPY … FROM STDIN into a sharded table: each row to its owner (`copy`).
	SplitCopy(Vec<NodeId>),
	/// A write every one of these nodes runs as it is, committed together (two-phase): an
	/// UPDATE or DELETE of a sharded table each node applies to its own rows, or a write to a
	/// reference table, whose copies must then agree (`same_count`: every node's row count).
	Fanout {
		nodes: Vec<NodeId>,
		same_count: bool,
	},
	Refuse(Refusal),
}

/// A refusal: SQLSTATE, the sentence, and the fix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal {
	pub code: &'static str,
	pub message: String,
	pub hint: String,
}

fn refuse(code: &'static str, message: impl Into<String>, hint: impl Into<String>) -> Route {
	Route::Refuse(Refusal {
		code,
		message: message.into(),
		hint: hint.into(),
	})
}

/// SQLSTATE for "Lepis cannot run this across nodes": feature_not_supported, as Postgres uses
/// for a valid statement it will not run.
pub const NOT_ACROSS_NODES: &str = "0A000";

/// A bound parameter as the client sent it (Bind's per-parameter format).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParamValue {
	Text(String),
	Binary(Vec<u8>),
	Null,
	/// A value whose type the statement's Parse declared (a non-zero type oid): a binary value
	/// is that type's bytes, whatever the key's type is.
	Declared(u32, Box<ParamValue>),
}

impl ParamValue {
	/// The value, without the type its Parse declared.
	pub fn value(&self) -> &ParamValue {
		match self {
			ParamValue::Declared(_, v) => v.value(),
			v => v,
		}
	}
}

/// Bound parameter values, for `KeyValue::Param`.
pub type Params<'a> = &'a [ParamValue];

pub fn route(facts: &Facts, catalog: &Catalog, params: Params) -> Route {
	match facts.kind {
		Kind::Transaction | Kind::Session | Kind::Other => return Route::Home,
		Kind::Ddl => {
			// A table nobody has distributed is global: its DDL is the home node's alone. The router
			// runs DDL on a distributed table across nodes itself (ddl.rs) before it gets here; what
			// reaches this is DDL ddl.rs does not know.
			let distributed = facts.tables.iter().find(|t| {
				matches!(
					catalog.relations.get(&t.name),
					Some(RelationKind::Sharded { .. } | RelationKind::Reference)
				)
			});
			return match distributed {
				None => Route::Home,
				Some(_) if catalog.nodes.len() <= 1 => Route::Home,
				Some(t) => refuse(
					NOT_ACROSS_NODES,
					format!(
						"Lepis cannot run this DDL on the distributed table {} across nodes",
						t.name
					),
					"Change the table on every node directly, with the same statement, until Lepis runs schema changes itself.",
				),
			};
		}
		_ => {}
	}

	let mut sharded: Vec<(usize, &str, &str)> = Vec::new();
	let mut has_global = false;
	let mut writes_reference = false;
	for (i, t) in facts.tables.iter().enumerate() {
		match catalog.relations.get(&t.name) {
			Some(RelationKind::Sharded {
				keyspace,
				key_column,
			}) => sharded.push((i, keyspace, key_column)),
			Some(RelationKind::Reference) => writes_reference |= t.written,
			Some(RelationKind::Global) | None => has_global = true,
		}
	}

	if writes_reference {
		if catalog.nodes.len() <= 1 {
			return Route::Home;
		}
		// Every copy, in one two-phase commit; the statement must read nothing but reference
		// tables (and the catalogs), or each copy would be written from different rows.
		if !sharded.is_empty() || has_global {
			return refuse(
				NOT_ACROSS_NODES,
				"a write to a reference table may read only reference tables",
				"Read the other tables first, and write the reference table with the values.",
			);
		}
		if !matches!(facts.kind, Kind::Insert | Kind::Update | Kind::Delete) {
			return refuse(
				NOT_ACROSS_NODES,
				"only INSERT, UPDATE and DELETE write a reference table on every node",
				"Write the reference table with INSERT, UPDATE or DELETE.",
			);
		}
		return Route::Fanout {
			nodes: crate::twopc::writable_nodes(catalog),
			same_count: true,
		};
	}
	if sharded.is_empty() {
		return Route::Home;
	}
	if has_global {
		let g = facts
			.tables
			.iter()
			.find(|t| {
				!catalog.relations.contains_key(&t.name)
					|| matches!(catalog.relations.get(&t.name), Some(RelationKind::Global))
			})
			.map(|t| t.name.to_string())
			.unwrap_or_default();
		return refuse(
			NOT_ACROSS_NODES,
			format!("{g} lives only on the home node and cannot be combined with a sharded table"),
			format!("Make {g} a reference table so every node has it, or query it separately."),
		);
	}

	// Every sharded table must share one keyspace (colocation) …
	let keyspace = sharded[0].1;
	if let Some((i, other, _)) = sharded.iter().find(|(_, k, _)| *k != keyspace) {
		return refuse(
			NOT_ACROSS_NODES,
			format!(
				"{} (keyspace {other}) and {} (keyspace {keyspace}) are not colocated",
				facts.tables[*i].name, facts.tables[sharded[0].0].name
			),
			"Join sharded tables only within one keyspace, on the shard key.",
		);
	}
	// The common case first: one sharded table pinned to one value is that value's node.
	if let [(i, _, _)] = sharded.as_slice()
		&& !facts.assigns_key
		&& facts.kind != Kind::Insert
		&& let Some([v]) = facts.tables[*i].key_values.as_deref()
	{
		return match owner(catalog, keyspace, v, params) {
			Ok(Some(n)) => Route::Node(n),
			Ok(None) => Route::Node(catalog.nodes_of(keyspace)[0]),
			Err(r) => r,
		};
	}
	// … and be tied to each other by key equality, or each group of them pinned to keys that all
	// live on one node (checked below, once the key values are known).
	let components = key_components(&sharded, facts);
	let not_on_key = || {
		refuse(
			NOT_ACROSS_NODES,
			"a join of sharded tables that is not on the shard key would need rows from several nodes",
			"Join colocated tables on their shard key, or make the smaller table a reference table.",
		)
	};

	if facts.assigns_key {
		return refuse(
			NOT_ACROSS_NODES,
			"an UPDATE may not change a row's shard key",
			"Delete the row and insert it with the new key.",
		);
	}

	// INSERT … VALUES: each row to its owner.
	if facts.kind == Kind::Insert
		&& let Some(keys) = &facts.insert_keys
	{
		let mut by_node: Vec<(NodeId, Vec<usize>)> = Vec::new();
		for (row, k) in keys.iter().enumerate() {
			let node = match owner(catalog, keyspace, k, params) {
				Ok(Some(n)) => n,
				Ok(None) => {
					return refuse(
						"23502",
						"a row's shard key is NULL",
						"Every row of a sharded table needs a shard key.",
					);
				}
				Err(r) => return r,
			};
			match by_node.iter_mut().find(|(n, _)| *n == node) {
				Some((_, rows)) => rows.push(row),
				None => by_node.push((node, vec![row])),
			}
		}
		return match by_node.as_slice() {
			[(n, _)] => Route::Node(*n),
			_ => Route::SplitInsert(by_node),
		};
	}

	// Within a key-joined group the key is one value, so the group's nodes are the
	// INTERSECTION of what its pinned tables allow. Across groups (no key join between them)
	// rows from every group are needed, so the statement's nodes are the UNION.
	let mut union: BTreeSet<NodeId> = BTreeSet::new();
	let mut any_unpinned = false;
	for group in &components {
		let mut nodes: Option<BTreeSet<NodeId>> = None;
		for i in group {
			let Some(values) = &facts.tables[*i].key_values else {
				continue;
			};
			let mut here = BTreeSet::new();
			for v in values {
				match owner(catalog, keyspace, v, params) {
					Ok(Some(n)) => {
						here.insert(n);
					}
					Ok(None) => {}
					Err(r) => return r,
				}
			}
			nodes = Some(match nodes {
				None => here,
				Some(prev) => prev.intersection(&here).copied().collect(),
			});
		}
		match nodes {
			Some(set) => union.extend(set),
			None => any_unpinned = true,
		}
	}
	if components.len() > 1 && (any_unpinned || union.len() > 1) {
		return not_on_key();
	}

	let nodes: Vec<NodeId> = if any_unpinned {
		catalog.nodes_of(keyspace)
	} else {
		union.into_iter().collect()
	};
	match nodes.as_slice() {
		// Pinned to nothing (`key = NULL`, or two pins that disagree): any one node answers the
		// empty result correctly, with the right columns.
		[] => Route::Node(catalog.nodes_of(keyspace)[0]),
		[n] => Route::Node(*n),
		_ => {
			// Any statement that writes, whatever its kind: a SELECT with a writing CTE too.
			// An UPDATE or DELETE whose rows are on several nodes: each node applies it to its
			// own rows, and the nodes commit together. Any other write across nodes (INSERT …
			// SELECT, COPY, a write inside a query) would have to move rows between nodes.
			if matches!(facts.kind, Kind::Update | Kind::Delete) {
				return Route::Fanout {
					nodes,
					same_count: false,
				};
			}
			if facts.kind == Kind::CopyFrom && facts.tables.len() == 1 {
				return Route::SplitCopy(nodes);
			}
			if facts.tables.iter().any(|t| t.written)
				|| matches!(facts.kind, Kind::CopyFrom | Kind::Insert)
			{
				return refuse(
					NOT_ACROSS_NODES,
					"a write whose rows belong to several nodes is supported for INSERT … VALUES, COPY … FROM STDIN, UPDATE and DELETE",
					"Pin the write to one shard key value, or write the rows with INSERT … VALUES.",
				);
			}
			// A read: what its ORDER BY, aggregates and the rest need from a merge, and whether
			// they can have it, is scatter.rs's to decide (it refuses what it cannot merge).
			Route::Scatter(nodes)
		}
	}
}

/// The sharded tables grouped by key-equality joins (union-find), each group a list of indexes
/// into `facts.tables`.
fn key_components(sharded: &[(usize, &str, &str)], facts: &Facts) -> Vec<Vec<usize>> {
	let mut parent: Vec<usize> = (0..facts.tables.len()).collect();
	fn find(p: &mut [usize], x: usize) -> usize {
		let mut x = x;
		while p[x] != x {
			p[x] = p[p[x]];
			x = p[x];
		}
		x
	}
	for &(a, b) in &facts.key_joins {
		if a < parent.len() && b < parent.len() {
			let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
			parent[ra] = rb;
		}
	}
	let mut groups: Vec<(usize, Vec<usize>)> = Vec::new();
	for (i, _, _) in sharded {
		let root = find(&mut parent, *i);
		match groups.iter_mut().find(|(r, _)| *r == root) {
			Some((_, g)) => g.push(*i),
			None => groups.push((root, vec![*i])),
		}
	}
	groups.into_iter().map(|(_, g)| g).collect()
}

/// The node owning one key value; Ok(None) for NULL.
fn owner(
	catalog: &Catalog,
	keyspace: &str,
	v: &KeyValue,
	params: Params,
) -> Result<Option<NodeId>, Route> {
	let bad = |e: String| {
		refuse(
			"22P02",
			e,
			"Pass the shard key in its type's standard form.",
		)
	};
	let Some(key) = catalog.keyspaces.get(keyspace).map(|k| k.key_type) else {
		return Err(bad(format!("no keyspace {keyspace}")));
	};
	// The types a value passes through on its way to the comparison: each must hash as the key.
	let mut types: Vec<u32> = Vec::new();
	let mut v = v;
	while let KeyValue::Cast(inner, name) = v {
		match type_oid(name) {
			Some(oid) => types.push(oid),
			None => return Err(other_type(key, name)),
		}
		v = inner;
	}
	match v {
		KeyValue::Null => Ok(None),
		KeyValue::Cast(..) => unreachable!("unwrapped above"),
		KeyValue::Const(s) => {
			let mut to_numeric = false;
			for t in &types {
				match fits(key, *t) {
					Some(w) => to_numeric |= w,
					None => return Err(other_type(key, &t.to_string())),
				}
			}
			// A literal cast to an integer type is rounded by the cast: it must be one already.
			if types.iter().any(|t| matches!(t, 20 | 21 | 23)) && s.trim().parse::<i128>().is_err()
			{
				return Err(bad(format!("{s} is not an integer")));
			}
			let _ = to_numeric;
			catalog.owner(keyspace, s).map(Some).map_err(|e| bad(e.0))
		}
		KeyValue::Param(n) => {
			let Some(p) = params.get(n.wrapping_sub(1)) else {
				return Err(refuse(
					"08P01",
					format!("parameter ${n} was not bound"),
					"Bind every parameter the statement uses.",
				));
			};
			let (declared, p) = match p {
				ParamValue::Declared(oid, inner) => (Some(*oid), inner.as_ref()),
				p => (None, p),
			};
			let mut to_numeric = false;
			for t in declared.iter().chain(&types) {
				match fits(key, *t) {
					Some(w) => to_numeric |= w,
					None => return Err(other_type(key, &t.to_string())),
				}
			}
			// The bytes are the declared type's, else the first cast's, else the key's own.
			let bytes_type = declared.or_else(|| types.last().copied());
			match p {
				ParamValue::Null => Ok(None),
				ParamValue::Text(s) => catalog.owner(keyspace, s).map(Some).map_err(|e| bad(e.0)),
				ParamValue::Binary(b) if to_numeric && matches!(bytes_type, Some(20 | 21 | 23)) => {
					let int = match b.len() {
						2 => i16::from_be_bytes([b[0], b[1]]) as i64,
						4 => i32::from_be_bytes([b[0], b[1], b[2], b[3]]) as i64,
						8 => i64::from_be_bytes(b[..].try_into().expect("8 bytes")),
						_ => return Err(bad("not a binary integer".into())),
					};
					catalog
						.owner(keyspace, &int.to_string())
						.map(Some)
						.map_err(|e| bad(e.0))
				}
				ParamValue::Binary(b) => catalog
					.owner_binary(keyspace, b)
					.map(Some)
					.map_err(|e| bad(e.0)),
				ParamValue::Declared(..) => Err(bad("a parameter declared twice".into())),
			}
		}
	}
}

/// The oid of a type as a cast names it (`::bigint` is `int8` to the parser).
fn type_oid(name: &str) -> Option<u32> {
	Some(match name {
		"int2" | "smallint" => 21,
		"int4" | "integer" | "int" => 23,
		"int8" | "bigint" => 20,
		"numeric" | "decimal" => 1700,
		"text" => 25,
		"varchar" => 1043,
		"bpchar" => 1042,
		"uuid" => 2950,
		"bytea" => 17,
		"date" => 1082,
		"timestamp" => 1114,
		"timestamptz" => 1184,
		_ => return None,
	})
}

/// Whether a value of type `oid` compares with the key as the same value, hashed the same:
/// Some(true) when it is an integer compared with a numeric key (hashed through its text),
/// None when it may not pin the key at all.
fn fits(key: crate::hash::KeyType, oid: u32) -> Option<bool> {
	use crate::hash::KeyType as K;
	match (key, oid) {
		(K::Int2 | K::Int4 | K::Int8, 20 | 21 | 23) => Some(false),
		(K::Numeric, 1700) => Some(false),
		(K::Numeric, 20 | 21 | 23) => Some(true),
		(K::Text, 25 | 1043) => Some(false),
		(K::Bpchar, 1042) => Some(false),
		(K::Uuid, 2950) | (K::Bytea, 17) | (K::Date, 1082) => Some(false),
		(K::Timestamp, 1114) | (K::Timestamptz, 1184) => Some(false),
		_ => None,
	}
}

fn other_type(key: crate::hash::KeyType, what: &str) -> Route {
	refuse(
		NOT_ACROSS_NODES,
		format!(
			"the shard key is compared with a value of another type ({what}), so Lepis cannot tell which node it belongs to"
		),
		format!(
			"Compare the key with a {} value, or bind it in text format.",
			key.sql_name()
		),
	)
}

/// COPY … FROM STDIN into a sharded table, split by row: each row goes to the node that owns its
/// shard key. Pure: the router feeds the client's CopyData through a `Splitter` and sends each
/// piece where it says.
///
/// The three formats are read as Postgres reads them, as far as finding where a row ends and
/// what its key column holds: text (backslash escapes, `\N`, the `\.` end marker), CSV (quotes,
/// the escape character, quoted newlines, a header line) and binary (the signature, then
/// length-prefixed tuples, then the trailer). A row is forwarded byte for byte, so each node
/// parses exactly what one Postgres would have.
pub mod copy {
	use pg_query::NodeEnum;

	use super::{NOT_ACROSS_NODES, Refusal};

	#[derive(Clone, Copy, Debug, PartialEq, Eq)]
	pub enum Format {
		Text,
		Csv,
		Binary,
	}

	/// How the client's COPY data is written.
	#[derive(Clone, Debug, PartialEq, Eq)]
	pub struct Options {
		pub format: Format,
		pub delimiter: u8,
		pub quote: u8,
		pub escape: u8,
		/// The text that means NULL.
		pub null: Vec<u8>,
		/// The first line is a header (CSV, or text from Postgres 15).
		pub header: bool,
	}

	/// The statement's columns (None: every column of the table, in order) and options.
	#[derive(Clone, Debug, PartialEq, Eq)]
	pub struct CopyIn {
		pub columns: Option<Vec<String>>,
		pub options: Options,
	}

	fn no(message: impl Into<String>, hint: &str) -> Refusal {
		Refusal {
			code: NOT_ACROSS_NODES,
			message: message.into(),
			hint: hint.into(),
		}
	}

	fn string_arg(n: &pg_query::protobuf::Node) -> Option<String> {
		match n.node.as_ref()? {
			NodeEnum::String(s) => Some(s.sval.clone()),
			NodeEnum::Boolean(b) => Some(if b.boolval { "true" } else { "false" }.into()),
			NodeEnum::Integer(i) => Some(i.ival.to_string()),
			NodeEnum::AConst(c) => match c.val.as_ref()? {
				pg_query::protobuf::a_const::Val::Sval(s) => Some(s.sval.clone()),
				pg_query::protobuf::a_const::Val::Ival(i) => Some(i.ival.to_string()),
				pg_query::protobuf::a_const::Val::Boolval(b) => {
					Some(if b.boolval { "true" } else { "false" }.into())
				}
				_ => None,
			},
			_ => None,
		}
	}

	fn one_byte(name: &str, v: &str) -> Result<u8, Refusal> {
		match v.as_bytes() {
			[b] if b.is_ascii() => Ok(*b),
			_ => Err(no(
				format!("COPY {name} must be one single-byte character for a COPY across nodes"),
				"Use a single ASCII character.",
			)),
		}
	}

	/// Reads a `COPY table [(columns)] FROM STDIN [WITH (…)]`.
	pub fn read(sql: &str) -> Result<CopyIn, Refusal> {
		let parsed = pg_query::parse(sql).map_err(|e| {
			no(
				format!("Lepis cannot read this COPY: {e}"),
				"Check the statement.",
			)
		})?;
		let Some(NodeEnum::CopyStmt(c)) = parsed
			.protobuf
			.stmts
			.first()
			.and_then(|s| s.stmt.as_deref())
			.and_then(|n| n.node.as_ref())
		else {
			return Err(no("this is not a COPY", "Check the statement."));
		};
		if !c.is_from || c.is_program || !c.filename.is_empty() || c.relation.is_none() {
			return Err(no(
				"only COPY … FROM STDIN can be split across nodes: a file or a program is on one node",
				"Send the data from the client: COPY table FROM STDIN.",
			));
		}
		let columns = if c.attlist.is_empty() {
			None
		} else {
			Some(
				c.attlist
					.iter()
					.filter_map(string_arg)
					.collect::<Vec<String>>(),
			)
		};
		let mut o = Options {
			format: Format::Text,
			delimiter: b'\t',
			quote: b'"',
			escape: b'"',
			null: b"\\N".to_vec(),
			header: false,
		};
		let mut delimiter = None;
		let mut null = None;
		let mut quote = None;
		let mut escape = None;
		for opt in &c.options {
			let Some(NodeEnum::DefElem(d)) = opt.node.as_ref() else {
				continue;
			};
			let arg = d.arg.as_deref().and_then(string_arg);
			let v = arg.clone().unwrap_or_default();
			match d.defname.as_str() {
				"format" => {
					o.format = match v.to_ascii_lowercase().as_str() {
						"text" => Format::Text,
						"csv" => Format::Csv,
						"binary" => Format::Binary,
						other => {
							return Err(no(
								format!("COPY format {other} is not known"),
								"Use text, csv or binary.",
							));
						}
					}
				}
				"delimiter" => delimiter = Some(one_byte("DELIMITER", &v)?),
				"quote" => quote = Some(one_byte("QUOTE", &v)?),
				"escape" => escape = Some(one_byte("ESCAPE", &v)?),
				"null" => null = Some(v.into_bytes()),
				"header" => {
					o.header = !matches!(
						v.to_ascii_lowercase().as_str(),
						"false" | "off" | "0" | "no"
					)
				}
				"encoding" => {
					let e = v.to_ascii_lowercase().replace(['-', '_'], "");
					if e != "utf8" {
						return Err(no(
							format!("COPY ENCODING {v} is not supported across nodes"),
							"Send the data as UTF8.",
						));
					}
				}
				"freeze" | "on_error" | "log_verbosity" | "reject_limit" => {}
				other => {
					return Err(no(
						format!("COPY option {other} is not supported across nodes"),
						"Leave the option out, or COPY the rows of each shard key value on their own.",
					));
				}
			}
		}
		if o.format == Format::Csv {
			o.delimiter = b',';
			o.null = Vec::new();
		}
		if let Some(d) = delimiter {
			o.delimiter = d;
		}
		if let Some(n) = null {
			o.null = n;
		}
		if let Some(q) = quote {
			o.quote = q;
			o.escape = q;
		}
		if let Some(e) = escape {
			o.escape = e;
		}
		Ok(CopyIn {
			columns,
			options: o,
		})
	}

	/// The key of one row: its text form (text and CSV), or its binary form.
	#[derive(Clone, Debug, PartialEq, Eq)]
	pub enum Key {
		Null,
		Text(String),
		Binary(Vec<u8>),
	}

	/// One piece of the client's data.
	#[derive(Clone, Debug, PartialEq, Eq)]
	pub enum Piece {
		/// Every node gets it: a header line, or binary COPY's signature and header.
		Everyone(Vec<u8>),
		/// One row, with its key.
		Row { key: Key, bytes: Vec<u8> },
		/// Binary COPY's trailer, or text's `\.` line: the end of the data.
		End(Vec<u8>),
	}

	pub const BINARY_SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";

	/// Splits a COPY stream into rows, a CopyData at a time.
	pub struct Splitter {
		o: Options,
		key_col: usize,
		buf: Vec<u8>,
		header_pending: bool,
		ended: bool,
	}

	impl Splitter {
		/// `key_col` is the shard key's position among the COPY's columns.
		pub fn new(options: Options, key_col: usize) -> Splitter {
			let header_pending = options.header || options.format == Format::Binary;
			Splitter {
				o: options,
				key_col,
				buf: Vec::new(),
				header_pending,
				ended: false,
			}
		}

		/// The complete pieces `data` finishes; what is left waits for the next call.
		pub fn push(&mut self, data: &[u8]) -> Result<Vec<Piece>, String> {
			if self.ended {
				return Ok(Vec::new());
			}
			self.buf.extend_from_slice(data);
			let mut out = Vec::new();
			let mut at = 0;
			while !self.ended {
				let piece = match self.o.format {
					Format::Binary => self.binary(at)?,
					_ => self.line(at, false)?,
				};
				let Some((piece, next)) = piece else { break };
				at = next;
				out.push(piece);
			}
			self.buf.drain(..at);
			Ok(out)
		}

		/// At CopyDone: what is left must be one last row without its line end, or nothing.
		pub fn finish(&mut self) -> Result<Vec<Piece>, String> {
			if self.ended || self.buf.is_empty() {
				return Ok(Vec::new());
			}
			if self.o.format == Format::Binary {
				return Err("the binary COPY data ends in the middle of a row".into());
			}
			let mut out = Vec::new();
			if let Some((p, _)) = self.line(0, true)? {
				out.push(p);
			}
			self.buf.clear();
			Ok(out)
		}

		/// The line starting at `at`, if it is complete (or `last`).
		fn line(&mut self, at: usize, last: bool) -> Result<Option<(Piece, usize)>, String> {
			let b = &self.buf;
			let csv = self.o.format == Format::Csv;
			let mut i = at;
			let mut quoted = false;
			let end = loop {
				let Some(&c) = b.get(i) else {
					if last && i > at {
						if quoted {
							return Err("the CSV data ends inside a quoted field".into());
						}
						break i;
					}
					return Ok(None);
				};
				if csv {
					if quoted {
						if c == self.o.escape && self.o.escape != self.o.quote {
							i += 2;
							continue;
						}
						if c == self.o.quote {
							if self.o.escape == self.o.quote && b.get(i + 1) == Some(&self.o.quote)
							{
								i += 2;
								continue;
							}
							quoted = false;
						}
						i += 1;
						continue;
					}
					if c == self.o.quote {
						quoted = true;
						i += 1;
						continue;
					}
				} else if c == b'\\' {
					if i + 1 >= b.len() && !last {
						return Ok(None);
					}
					i += 2;
					continue;
				}
				if c == b'\n' {
					break i + 1;
				}
				if c == b'\r' {
					match b.get(i + 1) {
						Some(b'\n') => break i + 2,
						None if !last => return Ok(None),
						_ => {
							return Err(
								"rows ending in a bare carriage return are not supported across nodes"
									.into(),
							);
						}
					}
				}
				i += 1;
			};
			let end = end.min(b.len());
			let bytes = b[at..end].to_vec();
			let mut content = &bytes[..];
			while let Some((last_byte, rest)) = content.split_last() {
				if *last_byte == b'\n' || *last_byte == b'\r' {
					content = rest;
				} else {
					break;
				}
			}
			if content == b"\\." {
				self.ended = true;
				return Ok(Some((Piece::End(bytes), end)));
			}
			if self.header_pending {
				self.header_pending = false;
				return Ok(Some((Piece::Everyone(bytes), end)));
			}
			let key = if csv {
				self.csv_key(content)?
			} else {
				self.text_key(content)?
			};
			Ok(Some((Piece::Row { key, bytes }, end)))
		}

		fn text_key(&self, row: &[u8]) -> Result<Key, String> {
			let mut field = 0;
			let mut start = 0;
			let mut i = 0;
			let mut raw: Option<&[u8]> = None;
			while i <= row.len() {
				if i == row.len() || row[i] == self.o.delimiter {
					if field == self.key_col {
						raw = Some(&row[start..i]);
						break;
					}
					field += 1;
					start = i + 1;
					i += 1;
					continue;
				}
				if row[i] == b'\\' {
					i += 2;
					continue;
				}
				i += 1;
			}
			let Some(raw) = raw else {
				return Err("a row has fewer columns than the COPY names".into());
			};
			if raw == self.o.null.as_slice() {
				return Ok(Key::Null);
			}
			let mut out = Vec::with_capacity(raw.len());
			let mut i = 0;
			while i < raw.len() {
				let c = raw[i];
				if c != b'\\' || i + 1 >= raw.len() {
					out.push(c);
					i += 1;
					continue;
				}
				let n = raw[i + 1];
				i += 2;
				match n {
					b'b' => out.push(8),
					b'f' => out.push(12),
					b'n' => out.push(b'\n'),
					b'r' => out.push(b'\r'),
					b't' => out.push(b'\t'),
					b'v' => out.push(11),
					b'0'..=b'7' => {
						let mut v = u32::from(n - b'0');
						for _ in 0..2 {
							match raw.get(i) {
								Some(d @ b'0'..=b'7') => {
									v = v * 8 + u32::from(d - b'0');
									i += 1;
								}
								_ => break,
							}
						}
						out.push((v & 0xff) as u8);
					}
					b'x' if raw.get(i).is_some_and(u8::is_ascii_hexdigit) => {
						let mut v = 0u32;
						for _ in 0..2 {
							match raw.get(i) {
								Some(d) if d.is_ascii_hexdigit() => {
									v = v * 16 + (*d as char).to_digit(16).unwrap_or(0);
									i += 1;
								}
								_ => break,
							}
						}
						out.push(v as u8);
					}
					other => out.push(other),
				}
			}
			String::from_utf8(out)
				.map(Key::Text)
				.map_err(|_| "a shard key in the COPY data is not valid UTF-8".into())
		}

		fn csv_key(&self, row: &[u8]) -> Result<Key, String> {
			let (q, e, d) = (self.o.quote, self.o.escape, self.o.delimiter);
			let mut field = 0;
			let mut i = 0;
			loop {
				// One field from `i`.
				let mut value = Vec::new();
				let mut was_quoted = false;
				let mut quoted = false;
				while i < row.len() {
					let c = row[i];
					if quoted {
						if c == e
							&& e != q && matches!(row.get(i + 1), Some(x) if *x == q || *x == e)
						{
							value.push(row[i + 1]);
							i += 2;
							continue;
						}
						if c == q {
							if e == q && row.get(i + 1) == Some(&q) {
								value.push(q);
								i += 2;
								continue;
							}
							quoted = false;
							i += 1;
							continue;
						}
						value.push(c);
						i += 1;
						continue;
					}
					if c == d {
						break;
					}
					if c == q {
						quoted = true;
						was_quoted = true;
						i += 1;
						continue;
					}
					value.push(c);
					i += 1;
				}
				if field == self.key_col {
					if !was_quoted && value == self.o.null {
						return Ok(Key::Null);
					}
					return String::from_utf8(value)
						.map(Key::Text)
						.map_err(|_| "a shard key in the COPY data is not valid UTF-8".into());
				}
				if i >= row.len() {
					return Err("a row has fewer columns than the COPY names".into());
				}
				field += 1;
				i += 1;
			}
		}

		/// The binary piece starting at `at`, if it is complete.
		fn binary(&mut self, at: usize) -> Result<Option<(Piece, usize)>, String> {
			let b = &self.buf[at..];
			if self.header_pending {
				if b.len() < BINARY_SIGNATURE.len() + 8 {
					return Ok(None);
				}
				if &b[..BINARY_SIGNATURE.len()] != BINARY_SIGNATURE {
					return Err("the binary COPY data does not start with its signature".into());
				}
				let ext = i32::from_be_bytes(b[15..19].try_into().expect("4 bytes"));
				if ext < 0 {
					return Err("the binary COPY header is malformed".into());
				}
				let len = 19 + ext as usize;
				if b.len() < len {
					return Ok(None);
				}
				self.header_pending = false;
				return Ok(Some((Piece::Everyone(b[..len].to_vec()), at + len)));
			}
			if b.len() < 2 {
				return Ok(None);
			}
			let fields = i16::from_be_bytes([b[0], b[1]]);
			if fields == -1 {
				self.ended = true;
				return Ok(Some((Piece::End(b[..2].to_vec()), at + 2)));
			}
			if fields < 0 {
				return Err("the binary COPY data has a row with a negative column count".into());
			}
			let mut i = 2;
			let mut key = None;
			for f in 0..fields as usize {
				if b.len() < i + 4 {
					return Ok(None);
				}
				let len = i32::from_be_bytes(b[i..i + 4].try_into().expect("4 bytes"));
				i += 4;
				let value = if len < 0 {
					if len != -1 {
						return Err("the binary COPY data has a malformed field length".into());
					}
					Key::Null
				} else {
					let len = len as usize;
					if b.len() < i + len {
						return Ok(None);
					}
					let v = Key::Binary(b[i..i + len].to_vec());
					i += len;
					v
				};
				if f == self.key_col {
					key = Some(value);
				}
			}
			let Some(key) = key else {
				return Err("a row has fewer columns than the COPY names".into());
			};
			Ok(Some((
				Piece::Row {
					key,
					bytes: b[..i].to_vec(),
				},
				at + i,
			)))
		}
	}

	#[cfg(test)]
	mod tests {
		use super::*;

		fn split(sql: &str, key: usize, chunks: &[&[u8]]) -> Vec<Piece> {
			let c = read(sql).unwrap();
			let mut s = Splitter::new(c.options, key);
			let mut out = Vec::new();
			for ch in chunks {
				out.extend(s.push(ch).unwrap());
			}
			out.extend(s.finish().unwrap());
			out
		}

		fn keys(p: &[Piece]) -> Vec<Key> {
			p.iter()
				.filter_map(|p| match p {
					Piece::Row { key, .. } => Some(key.clone()),
					_ => None,
				})
				.collect()
		}

		fn t(s: &str) -> Key {
			Key::Text(s.into())
		}

		#[test]
		fn text_rows_split_wherever_the_chunks_fall() {
			let data = b"1\ta\\tb\n2\t\\N\n\\N\tx\n4\\\n2\ty\r\n";
			let want = vec![t("1"), t("2"), Key::Null, t("4\n2")];
			for cut in 0..data.len() {
				let p = split("copy t from stdin", 0, &[&data[..cut], &data[cut..]]);
				assert_eq!(keys(&p), want, "cut at {cut}");
				let whole: Vec<u8> = p
					.iter()
					.flat_map(|p| match p {
						Piece::Row { bytes, .. } | Piece::Everyone(bytes) | Piece::End(bytes) => {
							bytes.clone()
						}
					})
					.collect();
				assert_eq!(whole, data);
			}
			let p = split(
				"copy t from stdin",
				1,
				&[b"1\ta\\x41\\101\n2\t\\N\n\\.\n3\tz\n"],
			);
			assert_eq!(keys(&p), vec![t("aAA"), Key::Null]);
			assert!(matches!(p.last(), Some(Piece::End(_))));
		}

		#[test]
		fn csv_quotes_newlines_and_a_header() {
			let data = b"id,name\n\"1\",\"a,\"\"b\"\"\nc\"\n,x\n\"\",y\n7,last";
			for cut in 0..data.len() {
				let p = split(
					"copy t (id, name) from stdin with (format csv, header true)",
					0,
					&[&data[..cut], &data[cut..]],
				);
				assert!(matches!(&p[0], Piece::Everyone(h) if h == b"id,name\n"));
				assert_eq!(
					keys(&p),
					vec![t("1"), Key::Null, t(""), t("7")],
					"cut {cut}"
				);
			}
			let p = split(
				"copy t from stdin with csv delimiter ';' quote '''' escape '\\'",
				1,
				&[b"1;'a\\'b';x\n"],
			);
			assert_eq!(keys(&p), vec![t("a'b")]);
		}

		#[test]
		fn binary_rows_and_the_trailer() {
			let mut data = BINARY_SIGNATURE.to_vec();
			data.extend_from_slice(&0i32.to_be_bytes());
			data.extend_from_slice(&0i32.to_be_bytes());
			for (k, v) in [(Some(7i64), b"ab".as_slice()), (None, b"c")] {
				data.extend_from_slice(&2i16.to_be_bytes());
				match k {
					Some(k) => {
						data.extend_from_slice(&8i32.to_be_bytes());
						data.extend_from_slice(&k.to_be_bytes());
					}
					None => data.extend_from_slice(&(-1i32).to_be_bytes()),
				}
				data.extend_from_slice(&(v.len() as i32).to_be_bytes());
				data.extend_from_slice(v);
			}
			data.extend_from_slice(&(-1i16).to_be_bytes());
			for cut in 0..data.len() {
				let p = split(
					"copy t from stdin (format binary)",
					0,
					&[&data[..cut], &data[cut..]],
				);
				assert_eq!(
					keys(&p),
					vec![Key::Binary(7i64.to_be_bytes().to_vec()), Key::Null]
				);
				assert!(matches!(p.first(), Some(Piece::Everyone(_))));
				assert!(matches!(p.last(), Some(Piece::End(_))));
			}
		}

		#[test]
		fn every_option_form_reads() {
			for sql in [
				"copy t from stdin with (format csv, delimiter ';', quote '''', escape '\\')",
				"copy t from stdin with (format text, delimiter '|', null 'NULL')",
				"copy t from stdin with (format text, header true)",
				"copy t from stdin with (format binary)",
			] {
				if let Err(e) = read(sql) {
					panic!("{sql}: {e:?}");
				}
			}
		}

		#[test]
		fn what_cannot_be_split_is_refused() {
			for sql in [
				"copy t from '/tmp/x'",
				"copy t from program 'cat'",
				"copy t to stdout",
				"copy t from stdin with (format csv, force_null (a))",
				"copy t from stdin with (encoding 'latin1')",
				"copy t from stdin with (delimiter '||')",
			] {
				assert!(read(sql).is_err(), "{sql}");
			}
			let mut s = Splitter::new(read("copy t from stdin").unwrap().options, 0);
			assert!(s.push(b"1\tx\r2\ty\n").is_err());
		}
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use super::*;
	use crate::catalog::{Keyspace, Node, NodeState, Strategy};
	use crate::config::SslMode;
	use crate::hash::KeyType;

	fn rel(t: &str) -> RelationName {
		RelationName {
			schema: "app".into(),
			table: t.into(),
		}
	}

	fn cat(n: i32) -> Catalog {
		let ids: Vec<NodeId> = (1..=n).map(NodeId).collect();
		let mut c = Catalog {
			epoch: 1,
			..Default::default()
		};
		for id in &ids {
			c.nodes.insert(
				*id,
				Node {
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
				ranges: Keyspace::even_ranges(n as usize * 8, &ids),
				pins: HashMap::new(),
			},
		);
		c.keyspaces.insert(
			"device".into(),
			Keyspace {
				name: "device".into(),
				strategy: Strategy::Hash,
				key_type: KeyType::Uuid,
				seed: 2,
				ranges: Keyspace::even_ranges(n as usize * 8, &ids),
				pins: HashMap::new(),
			},
		);
		let sharded = |k: &str, c: &str| RelationKind::Sharded {
			keyspace: k.into(),
			key_column: c.into(),
		};
		c.relations
			.insert(rel("orders"), sharded("tenant", "tenant_id"));
		c.relations
			.insert(rel("items"), sharded("tenant", "tenant_id"));
		c.relations
			.insert(rel("events"), sharded("device", "device"));
		c.relations
			.insert(rel("countries"), RelationKind::Reference);
		c.relations.insert(rel("plans"), RelationKind::Global);
		c.keyspaces.insert(
			"amount".into(),
			Keyspace {
				name: "amount".into(),
				strategy: Strategy::Hash,
				key_type: KeyType::Numeric,
				seed: 3,
				ranges: Keyspace::even_ranges(n as usize * 8, &ids),
				pins: HashMap::new(),
			},
		);
		c.relations
			.insert(rel("ledger"), sharded("amount", "amount"));
		c
	}

	fn select(tables: Vec<TableUse>) -> Facts {
		Facts {
			kind: Kind::Select,
			tables,
			key_joins: vec![],
			assigns_key: false,
			insert_keys: None,
			needs_merge: vec![],
		}
	}

	fn t(name: &str, keys: Option<Vec<KeyValue>>) -> TableUse {
		TableUse {
			name: rel(name),
			key_values: keys,
			written: false,
		}
	}

	fn c(s: &str) -> KeyValue {
		KeyValue::Const(s.into())
	}

	#[test]
	fn pinned_key_goes_to_its_owner() {
		let cat = cat(4);
		let r = route(&select(vec![t("orders", Some(vec![c("42")]))]), &cat, &[]);
		assert_eq!(r, Route::Node(cat.owner("tenant", "42").unwrap()));
		let p = route(
			&select(vec![t("orders", Some(vec![KeyValue::Param(1)]))]),
			&cat,
			&[ParamValue::Text("42".into())],
		);
		assert_eq!(p, r);
	}

	#[test]
	fn unpinned_reads_scatter_whatever_they_need_merged() {
		let cat = cat(4);
		assert!(matches!(
			route(&select(vec![t("orders", None)]), &cat, &[]),
			Route::Scatter(n) if n.len() == 4
		));
		let mut f = select(vec![t("orders", None)]);
		f.needs_merge = vec!["ORDER BY"];
		assert!(matches!(route(&f, &cat, &[]), Route::Scatter(_)));
	}

	#[test]
	fn colocated_join_needs_the_key() {
		let cat = cat(4);
		let mut f = select(vec![t("orders", Some(vec![c("7")])), t("items", None)]);
		assert!(matches!(route(&f, &cat, &[]), Route::Refuse(_)));
		f.key_joins = vec![(0, 1)];
		assert_eq!(
			route(&f, &cat, &[]),
			Route::Node(cat.owner("tenant", "7").unwrap())
		);
		// No join, but both pinned to the same tenant: one node.
		let same = select(vec![
			t("orders", Some(vec![c("7")])),
			t("items", Some(vec![c("7")])),
		]);
		assert_eq!(
			route(&same, &cat, &[]),
			Route::Node(cat.owner("tenant", "7").unwrap())
		);
		// No join, pinned to tenants on different nodes: the rows of both are needed.
		let (a, b) = (0..1000)
			.map(|i| i.to_string())
			.find_map(|x| {
				(cat.owner("tenant", &x).unwrap() != cat.owner("tenant", "7").unwrap()).then_some(x)
			})
			.map(|x| ("7".to_string(), x))
			.unwrap();
		let apart = select(vec![
			t("orders", Some(vec![c(&a)])),
			t("items", Some(vec![c(&b)])),
		]);
		assert!(matches!(route(&apart, &cat, &[]), Route::Refuse(_)));
	}

	#[test]
	fn reference_joins_freely_and_global_does_not() {
		let cat = cat(4);
		let ok = select(vec![t("orders", Some(vec![c("7")])), t("countries", None)]);
		assert!(matches!(route(&ok, &cat, &[]), Route::Node(_)));
		let bad = select(vec![t("orders", Some(vec![c("7")])), t("plans", None)]);
		assert!(matches!(route(&bad, &cat, &[]), Route::Refuse(_)));
		assert_eq!(
			route(&select(vec![t("plans", None)]), &cat, &[]),
			Route::Home
		);
	}

	#[test]
	fn different_keyspaces_are_refused() {
		let cat = cat(2);
		let mut f = select(vec![t("orders", Some(vec![c("1")])), t("events", None)]);
		f.key_joins = vec![(0, 1)];
		assert!(matches!(route(&f, &cat, &[]), Route::Refuse(_)));
	}

	#[test]
	fn inserts_split_by_owner() {
		let cat = cat(4);
		let keys: Vec<KeyValue> = (0..200).map(|i| c(&i.to_string())).collect();
		let f = Facts {
			kind: Kind::Insert,
			tables: vec![TableUse {
				written: true,
				..t("orders", None)
			}],
			insert_keys: Some(keys),
			..select(vec![])
		};
		let Route::SplitInsert(groups) = route(&f, &cat, &[]) else {
			panic!("not split");
		};
		assert_eq!(groups.len(), 4);
		assert_eq!(groups.iter().map(|(_, r)| r.len()).sum::<usize>(), 200);
		for (n, rows) in &groups {
			for r in rows {
				assert_eq!(cat.owner("tenant", &r.to_string()).unwrap(), *n);
			}
		}
	}

	#[test]
	fn a_joining_node_is_neither_written_nor_read() {
		// Node 4 is a physical split's standby: in the catalog, joining, owning no range yet.
		let mut cat = cat(3);
		cat.nodes.insert(
			NodeId(4),
			Node {
				id: NodeId(4),
				name: "n4".into(),
				state: NodeState::Joining,
				home: false,
				..cat.nodes[&NodeId(2)].clone()
			},
		);
		let reference = Facts {
			kind: Kind::Update,
			tables: vec![TableUse {
				written: true,
				..t("countries", None)
			}],
			..select(vec![])
		};
		let all = vec![NodeId(1), NodeId(2), NodeId(3)];
		assert_eq!(
			route(&reference, &cat, &[]),
			Route::Fanout {
				nodes: all.clone(),
				same_count: true
			}
		);
		assert_eq!(
			route(&select(vec![t("orders", None)]), &cat, &[]),
			Route::Scatter(all.clone())
		);
		let update = Facts {
			kind: Kind::Delete,
			tables: vec![TableUse {
				written: true,
				..t("orders", None)
			}],
			..select(vec![])
		};
		assert_eq!(
			route(&update, &cat, &[]),
			Route::Fanout {
				nodes: all,
				same_count: false
			}
		);
	}

	#[test]
	fn writes_across_nodes_fan_out_and_key_changes_are_refused() {
		let cat = cat(4);
		let f = Facts {
			kind: Kind::Update,
			tables: vec![TableUse {
				written: true,
				..t("orders", None)
			}],
			..select(vec![])
		};
		assert!(matches!(
			route(&f, &cat, &[]),
			Route::Fanout {
				same_count: false,
				..
			}
		));
		let f = Facts {
			kind: Kind::Update,
			tables: vec![TableUse {
				written: true,
				..t("orders", Some(vec![c("3")]))
			}],
			assigns_key: true,
			..select(vec![])
		};
		assert!(matches!(route(&f, &cat, &[]), Route::Refuse(_)));
	}

	/// `amount = $1::int8` on a numeric key, the parameter bound as int8's 8 bytes: read as
	/// numeric bytes it would hash some other number and land on the wrong node.
	#[test]
	fn a_parameter_is_read_as_its_own_type() {
		let cat = cat(4);
		let key = |v: KeyValue| select(vec![t("ledger", Some(vec![v]))]);
		let want = Route::Node(cat.owner("amount", "7").unwrap());
		let cast = |inner: KeyValue, ty: &str| KeyValue::Cast(Box::new(inner), ty.into());
		let int8 = ParamValue::Binary(7i64.to_be_bytes().to_vec());
		assert_eq!(
			route(
				&key(cast(KeyValue::Param(1), "int8")),
				&cat,
				std::slice::from_ref(&int8)
			),
			want
		);
		// The same bytes, typed by the Parse instead of a cast.
		let declared = ParamValue::Declared(20, Box::new(int8.clone()));
		assert_eq!(route(&key(KeyValue::Param(1)), &cat, &[declared]), want);
		// Without either, the bytes are the key's own type (numeric): 7 as numeric is right.
		let numeric = ParamValue::Binary(crate::merge::Dec::parse("7").unwrap().to_binary());
		assert_eq!(route(&key(KeyValue::Param(1)), &cat, &[numeric]), want);
		// A type that does not hash as the key is refused, never guessed.
		for (v, p) in [
			(cast(KeyValue::Param(1), "float8"), int8.clone()),
			(
				cast(KeyValue::Param(1), "text"),
				ParamValue::Text("7".into()),
			),
			(
				KeyValue::Param(1),
				ParamValue::Declared(701, Box::new(int8.clone())),
			),
		] {
			assert!(
				matches!(route(&key(v.clone()), &cat, &[p]), Route::Refuse(_)),
				"{v:?}"
			);
		}
		// A literal the cast would round is refused; one it keeps routes.
		assert!(matches!(
			route(&key(cast(c("1.5"), "int8")), &cat, &[]),
			Route::Refuse(_)
		));
		assert_eq!(route(&key(cast(c("7"), "int8")), &cat, &[]), want);
		// An integer key takes any integer width.
		let int4 = ParamValue::Binary(42i32.to_be_bytes().to_vec());
		let orders = select(vec![t(
			"orders",
			Some(vec![cast(KeyValue::Param(1), "int4")]),
		)]);
		assert_eq!(
			route(&orders, &cat, &[int4]),
			Route::Node(cat.owner("tenant", "42").unwrap())
		);
	}

	#[test]
	fn a_bad_key_literal_is_refused_not_guessed() {
		let cat = cat(2);
		let r = route(
			&select(vec![t("orders", Some(vec![c("forty-two")]))]),
			&cat,
			&[],
		);
		assert!(matches!(r, Route::Refuse(Refusal { code: "22P02", .. })));
	}

	#[test]
	fn single_node_clusters_route_everything_home_or_to_it() {
		let cat = cat(1);
		assert_eq!(
			route(&select(vec![t("orders", None)]), &cat, &[]),
			Route::Node(NodeId(1))
		);
	}
}
