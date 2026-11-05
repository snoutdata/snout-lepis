//! The design check (L24): what the schema says about the cluster's keys, read from the home
//! node's catalog and Lepis's, deterministic, and nothing else. It CHECKS and nothing else: it
//! writes no job and runs nothing; a finding that has an operation carries the request body
//! (`{"op": "table.distribute", ...}`), and SQL the person runs themselves (an index, a column)
//! is given as text.
//!
//! The distribution key is what the whole cluster turns on: a row's node is its key's, a foreign
//! key holds only between rows on one node (L9), a unique key only over what one node holds (L15),
//! and a join or a lookup without the key is a scatter. So the findings are about keys:
//!
//! - **Where a table belongs.** A table on the home node that references a distributed table, or
//!   one already known to follow it, belongs with that table's rows: by the column its foreign key
//!   pairs with the parent's key, else by a column of the parent's key's name. One with neither
//!   cannot follow it until it has the column (`missing_key_column`); one with the column whose
//!   foreign key leaves it out cannot either (`fk_without_key`). The walk follows foreign keys
//!   down any number of tables, so a chain of twelve is read like a chain of one.
//! - **What would refuse it.** A table that belongs somewhere but cannot be distributed there says
//!   why, in the words `table distribute` would use (`blocked`): no primary key, a unique key
//!   without the distribution key, a sequence.
//! - **What a sharded table references off its key** must be a copy on every node
//!   (`should_be_reference`), or say why it cannot be one.
//! - **Indexes.** A distributed table whose key leads no index finds its own rows by a scan
//!   (`key_not_indexed`); a foreign key whose columns lead no index makes every join on it and
//!   every delete of a parent scan the child (`fk_not_indexed`). Postgres creates neither on its
//!   own.
//! - **Size.** A big table still on the home node (`big_on_home`), a big reference table, which is
//!   stored and written on every node (`reference_large`), a table that cannot be moved at all
//!   (`no_primary_key`).
//!
//! `check` is pure and unit-tested; `read` gathers the facts from the home node (`auto::read_facts`
//! and the indexes), leaving out chunk tables and retired ones, which are the shells' own.
//! Every threshold is CHOSEN, not measured.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::{Value, json};

use super::auto::{self, ForeignKey, TableFacts};
use super::spec::Op;
use super::steps::connect_node;
use super::{Kind, OpError, Pg, load_catalog, qualified};
use crate::catalog::{Catalog, NodeState, RelationKind, RelationName, quote_ident};
use crate::server::App;

/// A table on the home node at or above this size is worth distributing. Chosen: a gigabyte is
/// minutes of copy at the plan's assumed rate, and past it one node's cache stops holding it.
pub const BIG_BYTES: u64 = 1 << 30;

/// A reference table at or above this size costs its size on every node, and every write to it
/// is a write on every node. Chosen.
pub const REFERENCE_BYTES: u64 = 1 << 30;

/// One index: the table, and its leading plain columns in order (an expression ends the list).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Index {
	pub table: RelationName,
	pub columns: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct Facts {
	pub tables: Vec<TableFacts>,
	pub fks: Vec<ForeignKey>,
	pub indexes: Vec<Index>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
	/// The cluster refuses it, or will.
	Error,
	/// It works, slowly, or it blocks a move later.
	Warn,
	/// A suggestion.
	Info,
}

impl Severity {
	pub fn name(self) -> &'static str {
		match self {
			Severity::Error => "error",
			Severity::Warn => "warn",
			Severity::Info => "info",
		}
	}
}

#[derive(Clone, Debug, PartialEq)]
pub struct Finding {
	pub severity: Severity,
	pub code: &'static str,
	pub table: RelationName,
	/// The other tables it is about (the parent of a foreign key), for scoping a plan's check.
	pub related: Vec<RelationName>,
	pub message: String,
	pub fix: String,
	/// SQL the person runs themselves, through the router.
	pub sql: Option<String>,
	/// An admin API request body that does the fix.
	pub operation: Option<Value>,
}

impl Finding {
	pub fn to_json(&self) -> Value {
		json!({
			"severity": self.severity.name(),
			"code": self.code,
			"table": self.table.to_string(),
			"related": self.related.iter().map(|r| r.to_string()).collect::<Vec<_>>(),
			"message": self.message,
			"fix": self.fix,
			"sql": self.sql,
			"operation": self.operation,
		})
	}

	fn touches(&self, tables: &BTreeSet<RelationName>) -> bool {
		tables.contains(&self.table) || self.related.iter().any(|r| tables.contains(r))
	}
}

/// Where a table's rows are, or should be: by `column` in `keyspace`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Target {
	keyspace: String,
	column: String,
	/// Already distributed so (else: where the foreign keys say it belongs).
	now: bool,
	/// The table whose rows it follows, when it does not yet.
	via: Option<RelationName>,
}

fn kind_of(c: &Catalog, t: &RelationName) -> RelationKind {
	c.relations.get(t).cloned().unwrap_or(RelationKind::Global)
}

fn table<'a>(f: &'a Facts, name: &RelationName) -> Option<&'a TableFacts> {
	f.tables.iter().find(|t| &t.name == name)
}

fn has_column(t: &TableFacts, name: &str) -> bool {
	t.columns.iter().any(|c| c.name == name)
}

/// Whether an index of `table` begins with exactly `cols`, in any order.
fn indexed(f: &Facts, table: &RelationName, cols: &[String]) -> bool {
	let want: BTreeSet<&str> = cols.iter().map(String::as_str).collect();
	f.indexes.iter().any(|i| {
		&i.table == table
			&& i.columns.len() >= cols.len()
			&& i.columns[..cols.len()]
				.iter()
				.map(String::as_str)
				.collect::<BTreeSet<_>>()
				== want
	})
}

fn list(cols: &[String]) -> String {
	cols.iter()
		.map(|c| quote_ident(c))
		.collect::<Vec<_>>()
		.join(", ")
}

fn gb(bytes: u64) -> String {
	format!("{:.1} GB", bytes as f64 / (1u64 << 30) as f64)
}

/// Every table's target: the distributed ones as they are, then, following foreign keys down
/// from them until nothing changes, every home table that references one with a target.
fn targets(c: &Catalog, f: &Facts) -> BTreeMap<RelationName, Target> {
	let mut out: BTreeMap<RelationName, Target> = BTreeMap::new();
	// Every distributed table, in the schemas checked or not: a table of one schema can follow
	// a table of another.
	for (name, kind) in &c.relations {
		if let RelationKind::Sharded {
			keyspace,
			key_column,
		} = kind.clone()
		{
			out.insert(
				name.clone(),
				Target {
					keyspace,
					column: key_column,
					now: true,
					via: None,
				},
			);
		}
	}
	// Before anything is distributed (or for a tenant nothing distributed reaches): the tenant
	// `table.auto` would choose in each keyspace, the table whose primary key the most tables
	// reference, keyed by that key, so a schema is checked while it is designed. The walk below
	// takes it from there; table.auto itself leaves a whole linked group home when one table in
	// it cannot follow, which is the very table the check is here to name.
	let mut names: Vec<&String> = c.keyspaces.keys().collect();
	names.sort();
	for ks in names {
		if let Some((table, pk, _)) = auto::tenant(&f.tables, &f.fks, c.keyspaces[ks].key_type)
			&& kind_of(c, &table) == RelationKind::Global
		{
			out.entry(table).or_insert(Target {
				keyspace: ks.clone(),
				column: pk,
				now: false,
				via: None,
			});
		}
	}
	let mut fks: Vec<&ForeignKey> = f.fks.iter().collect();
	fks.sort_by(|a, b| a.from.cmp(&b.from).then(a.name.cmp(&b.name)));
	loop {
		let mut changed = false;
		for fk in &fks {
			if fk.from == fk.to
				|| out.contains_key(&fk.from)
				|| kind_of(c, &fk.from) != RelationKind::Global
			{
				continue;
			}
			let (Some(parent), Some(child)) = (out.get(&fk.to).cloned(), table(f, &fk.from)) else {
				continue;
			};
			let column = fk
				.to_columns
				.iter()
				.position(|c| *c == parent.column)
				.map(|i| fk.columns[i].clone())
				.or_else(|| has_column(child, &parent.column).then(|| parent.column.clone()));
			if let Some(column) = column {
				out.insert(
					fk.from.clone(),
					Target {
						keyspace: parent.keyspace,
						column,
						now: false,
						via: Some(fk.to.clone()),
					},
				);
				changed = true;
			}
		}
		if !changed {
			return out;
		}
	}
}

/// The findings, most severe first, then by table and code.
pub fn check(c: &Catalog, f: &Facts) -> Vec<Finding> {
	let mut out: Vec<Finding> = Vec::new();
	let spread = c
		.nodes
		.values()
		.any(|n| !n.home && n.state != NodeState::Removed);
	let target = targets(c, f);
	let key_type = |ks: &str| c.keyspaces.get(ks).map(|k| k.key_type.sql_name());

	for t in &f.tables {
		let kind = kind_of(c, &t.name);
		// Indexes on the key.
		if let RelationKind::Sharded { key_column, .. } = &kind
			&& !indexed(f, &t.name, std::slice::from_ref(key_column))
		{
			out.push(Finding {
				severity: Severity::Warn,
				code: "key_not_indexed",
				table: t.name.clone(),
				related: vec![],
				message: format!(
					"{} is distributed by {key_column}, and no index begins with it: a query for one key finds its rows by reading the whole table on its node",
					t.name
				),
				fix: format!("Index {key_column}, through the router (every chunk gets it)."),
				sql: Some(format!(
					"create index on {} ({});",
					qualified(&t.name),
					quote_ident(key_column)
				)),
				operation: None,
			});
		}
		match &kind {
			RelationKind::Reference if t.bytes >= REFERENCE_BYTES => out.push(Finding {
				severity: Severity::Warn,
				code: "reference_large",
				table: t.name.clone(),
				related: vec![],
				message: format!(
					"{} is a reference table of {}: every node stores all of it, and every write to it is a write on every node",
					t.name,
					gb(t.bytes)
				),
				fix: "Keep reference tables to lookups that are read far more than written; distribute a big one by the key the tables joining it use.".into(),
				sql: None,
				operation: None,
			}),
			RelationKind::Global => {
				if spread && !t.replayable {
					out.push(Finding {
						severity: Severity::Warn,
						code: "no_primary_key",
						table: t.name.clone(),
						related: vec![],
						message: format!(
							"{} has no primary key: it can never be distributed, copied to every node, or moved off this node",
							t.name
						),
						fix: "Add a primary key (it must include the distribution key once the table is distributed).".into(),
						sql: None,
						operation: None,
					});
				}
				if let Some(g) = target.get(&t.name) {
					// Whose rows it follows, or (via None) table auto's own choice for it.
					let (via, belongs) = match &g.via {
						Some(v) => (
							v.clone(),
							format!("belongs with {v}'s rows (by {} in keyspace {})", g.column, g.keyspace),
						),
						None => (
							t.name.clone(),
							format!("is the tenant: the table whose key the most tables reference, distributed by {} in keyspace {}", g.column, g.keyspace),
						),
					};
					let op = json!({"op": "table.distribute", "table": t.name.to_string(), "column": g.column, "keyspace": g.keyspace});
					match auto::shard_refusal(t, &g.column) {
						Some(why) => out.push(Finding {
							severity: Severity::Warn,
							code: "blocked",
							table: t.name.clone(),
							related: vec![via.clone()],
							message: format!("{} {belongs}, and cannot be distributed: {why}", t.name),
							fix: "Change what the reason names, then distribute it into the same keyspace.".into(),
							sql: None,
							operation: None,
						}),
						None => out.push(Finding {
							severity: if spread && t.bytes >= BIG_BYTES { Severity::Warn } else { Severity::Info },
							code: if spread && t.bytes >= BIG_BYTES { "big_on_home" } else { "can_follow" },
							table: t.name.clone(),
							related: vec![via.clone()],
							message: format!(
								"{} ({}) is on the home node and {belongs}: every foreign key it has to the tables keyed the same way would hold on one node",
								t.name,
								gb(t.bytes),
							),
							fix: format!("Distribute it by {} into {}.", g.column, g.keyspace),
							sql: None,
							operation: Some(op),
						}),
					}
				} else if spread && t.bytes >= BIG_BYTES {
					out.push(Finding {
						severity: Severity::Warn,
						code: "big_on_home",
						table: t.name.clone(),
						related: vec![],
						message: format!(
							"{} is {} and all of it is on the home node, and no foreign key says which key it follows",
							t.name,
							gb(t.bytes)
						),
						fix: "Choose its distribution key: the column most of its queries filter and join by (table auto proposes one).".into(),
						sql: None,
						operation: None,
					});
				}
			}
			_ => {}
		}
	}

	// Foreign keys.
	let mut should_reference: BTreeMap<RelationName, Vec<String>> = BTreeMap::new();
	for fk in &f.fks {
		if table(f, &fk.from).is_none() {
			continue;
		}
		if !indexed(f, &fk.from, &fk.columns) {
			out.push(Finding {
				severity: Severity::Warn,
				code: "fk_not_indexed",
				table: fk.from.clone(),
				related: vec![fk.to.clone()],
				message: format!(
					"foreign key {} ({}) has no index on {}: every join on it reads the whole table, and every delete or key change in {} scans it",
					fk.name,
					fk.columns.join(", "),
					fk.from,
					fk.to
				),
				fix: "Index the foreign key's columns, through the router.".into(),
				sql: Some(format!(
					"create index on {} ({});",
					qualified(&fk.from),
					list(&fk.columns)
				)),
				operation: None,
			});
		}
		if fk.from == fk.to {
			continue;
		}
		let parent = target.get(&fk.to);
		let child = target.get(&fk.from);
		let to_kind = kind_of(c, &fk.to);
		match (child, parent) {
			// Both placed by keys of one keyspace: the key must pair them.
			(Some(ch), Some(p)) if ch.keyspace == p.keyspace => {
				let paired = fk
					.columns
					.iter()
					.zip(&fk.to_columns)
					.any(|(a, b)| *a == ch.column && *b == p.column);
				if !paired && !(ch.now && p.now) {
					let mut from_cols = vec![ch.column.clone()];
					from_cols.extend(fk.columns.iter().filter(|c| **c != ch.column).cloned());
					let mut to_cols = vec![p.column.clone()];
					to_cols.extend(fk.to_columns.iter().filter(|c| **c != p.column).cloned());
					out.push(Finding {
						severity: Severity::Error,
						code: "fk_without_key",
						table: fk.from.clone(),
						related: vec![fk.to.clone()],
						message: format!(
							"foreign key {} links {} to {} by ({}), not by the distribution key {}: once distributed, a row and the row it references could be on different nodes, and the key is refused",
							fk.name,
							fk.from,
							fk.to,
							fk.columns.join(", "),
							ch.column
						),
						fix: format!(
							"Make the foreign key ({}) references {} ({}); {} needs a unique key on those columns.",
							from_cols.join(", "),
							fk.to,
							to_cols.join(", "),
							fk.to
						),
						sql: None,
						operation: None,
					});
				}
			}
			// The parent is placed by a key and the child is not: it lacks the key's column.
			(None, Some(p))
				if kind_of(c, &fk.from) == RelationKind::Global
					&& to_kind != RelationKind::Reference =>
			{
				let ty = key_type(&p.keyspace).unwrap_or("<the key's type>");
				let on = fk
					.columns
					.iter()
					.zip(&fk.to_columns)
					.map(|(a, b)| format!("c.{} = p.{}", quote_ident(a), quote_ident(b)))
					.collect::<Vec<_>>()
					.join(" and ");
				out.push(Finding {
					severity: Severity::Warn,
					code: "missing_key_column",
					table: fk.from.clone(),
					related: vec![fk.to.clone()],
					message: format!(
						"{} references {} (whose rows are placed by {}) and has no {} column: its rows cannot be kept with the rows they reference, so it cannot be distributed while foreign key {} holds",
						fk.from, fk.to, p.column, p.column, fk.name
					),
					fix: format!(
						"Add {} to {}, fill it from {}, and put it first in foreign key {}; then every table below {} can follow too.",
						p.column, fk.from, fk.to, fk.name, fk.from
					),
					sql: Some(format!(
						"alter table {from} add column {col} {ty};\nupdate {from} c set {col} = p.{col} from {to} p where {on};",
						from = qualified(&fk.from),
						col = quote_ident(&p.column),
						to = qualified(&fk.to),
					)),
					operation: None,
				});
			}
			_ => {}
		}
		// A distributed table referencing a home table off its key: that table must be everywhere.
		if let Some(ch) = child
			&& ch.now && to_kind == RelationKind::Global
			&& parent.is_none()
		{
			should_reference
				.entry(fk.to.clone())
				.or_default()
				.push(format!("{} ({})", fk.from, fk.name));
		}
	}
	for (name, by) in should_reference {
		let Some(t) = table(f, &name) else { continue };
		let by = by.join(", ");
		match auto::reference_refusal(t) {
			None => out.push(Finding {
				severity: Severity::Error,
				code: "should_be_reference",
				table: name.clone(),
				related: vec![],
				message: format!(
					"{name} is on the home node and referenced by distributed tables off their key ({by}): those foreign keys cannot hold on their nodes"
				),
				fix: "Make it a reference table, a copy on every node.".into(),
				sql: None,
				operation: Some(json!({"op": "table.reference", "table": name.to_string()})),
			}),
			Some(why) => out.push(Finding {
				severity: Severity::Error,
				code: "should_be_reference",
				table: name.clone(),
				related: vec![],
				message: format!(
					"{name} is referenced by distributed tables off their key ({by}), so it must be a copy on every node, and it cannot be one: {why}"
				),
				fix: "Change what the reason names (ids from uuidv7() instead of a sequence), then make it a reference table.".into(),
				sql: None,
				operation: None,
			}),
		}
	}
	out.sort_by(|a, b| {
		a.severity
			.cmp(&b.severity)
			.then(a.table.cmp(&b.table))
			.then(a.code.cmp(b.code))
			.then(a.message.cmp(&b.message))
	});
	out
}

/// The findings as the API says them, with counts.
pub fn to_json(findings: &[Finding], f: &Facts) -> Value {
	let count = |s: Severity| findings.iter().filter(|x| x.severity == s).count();
	json!({
		"findings": findings.iter().map(Finding::to_json).collect::<Vec<_>>(),
		"summary": {
			"errors": count(Severity::Error),
			"warnings": count(Severity::Warn),
			"info": count(Severity::Info),
			"tables": f.tables.len(),
			"foreign_keys": f.fks.len(),
		},
	})
}

/// The findings that touch `tables`, for a plan.
pub fn scoped(findings: &[Finding], tables: &BTreeSet<RelationName>) -> Vec<Finding> {
	findings
		.iter()
		.filter(|x| x.touches(tables))
		.cloned()
		.collect()
}

/// Whether `name` is a chunk table (`<shell>_cNNN`) or a retired one, which the shell speaks for.
fn is_chunk_table(c: &Catalog, name: &RelationName) -> bool {
	if name.table.starts_with("lepis_retired_") {
		return true;
	}
	let Some((base, digits)) = name.table.rsplit_once("_c") else {
		return false;
	};
	digits.len() >= 3
		&& digits.bytes().all(|b| b.is_ascii_digit())
		&& c.chunked.contains(&RelationName {
			schema: name.schema.clone(),
			table: base.to_string(),
		})
}

/// The user schemas of the home node: all but Postgres's own, Lepis's and `exclude`.
async fn user_schemas(pg: &mut Pg, exclude: &[String]) -> Result<Vec<String>, OpError> {
	let rows = pg
		.query(
			"select nspname from pg_namespace where nspname not like 'pg\\_%' \
			and nspname not in ('information_schema', 'lepis') order by nspname",
			&[],
		)
		.await?;
	Ok(rows
		.iter()
		.filter_map(|r| r.first().cloned().flatten())
		.filter(|s| !exclude.contains(s))
		.collect())
}

/// The facts of `schemas` (every user schema when empty, less `exclude`) on the home node.
pub async fn read(
	pg: &mut Pg,
	c: &Catalog,
	schemas: &[String],
	exclude: &[String],
) -> Result<Facts, OpError> {
	let schemas = if schemas.is_empty() {
		user_schemas(pg, exclude).await?
	} else {
		schemas.to_vec()
	};
	if schemas.is_empty() {
		return Ok(Facts::default());
	}
	let (mut tables, mut fks) = auto::read_facts(pg, &schemas).await?;
	tables.retain(|t| !is_chunk_table(c, &t.name));
	fks.retain(|f| !is_chunk_table(c, &f.from) && !is_chunk_table(c, &f.to));
	let mut indexes = Vec::new();
	for r in pg
		.query(
			"select n.nspname, c.relname, \
			(select string_agg(a.attname, chr(31) order by u.ord) from unnest(i.indkey::int2[]) with ordinality u(attnum, ord) \
			join pg_attribute a on a.attrelid = i.indrelid and a.attnum = u.attnum \
			where u.ord <= coalesce((select min(z.ord) - 1 from unnest(i.indkey::int2[]) with ordinality z(attnum, ord) where z.attnum = 0), i.indnkeyatts)) \
			from pg_index i join pg_class c on c.oid = i.indrelid join pg_namespace n on n.oid = c.relnamespace \
			where n.nspname = any (string_to_array($1, ',')) and i.indpred is null",
			&[&schemas.join(",")],
		)
		.await?
	{
		let text = |i: usize| r.get(i).cloned().flatten().unwrap_or_default();
		let name = RelationName {
			schema: text(0),
			table: text(1),
		};
		if is_chunk_table(c, &name) {
			continue;
		}
		indexes.push(Index {
			table: name,
			columns: r
				.get(2)
				.cloned()
				.flatten()
				.map(|s| s.split('\u{1f}').map(str::to_string).collect())
				.unwrap_or_default(),
		});
	}
	Ok(Facts {
		tables,
		fks,
		indexes,
	})
}

/// `GET /v1/check`: the whole check of `schemas` (every user schema when empty, less `exclude`).
pub async fn run(
	app: &Arc<App>,
	h: &mut Pg,
	schemas: &[String],
	exclude: &[String],
) -> Result<Value, OpError> {
	let c = load_catalog(h).await?;
	let home = c
		.home()
		.map(|n| n.id)
		.ok_or_else(|| OpError::refused(Kind::CatalogInvalid, "the catalog has no home node"))?;
	let mut pg = connect_node(app, &c, home).await?;
	let facts = read(&mut pg, &c, schemas, exclude).await;
	pg.close().await;
	let facts = facts?;
	Ok(to_json(&check(&c, &facts), &facts))
}

/// The tables a table operation changes, or None for an operation on ranges and nodes, whose
/// plans the design check has nothing to say about.
pub fn tables_of(op: &Op) -> Option<(Vec<String>, BTreeSet<RelationName>)> {
	match op {
		Op::TableDistribute { table, .. }
		| Op::TableReference { table }
		| Op::TableGlobal { table } => {
			Some((vec![table.schema.clone()], BTreeSet::from([table.clone()])))
		}
		Op::TableAuto { schemas, .. } => Some((schemas.clone(), BTreeSet::new())),
		_ => None,
	}
}

/// The check of the tables `op` changes, as a plan carries it: the findings about them (every
/// finding of the schemas for table.auto). Read against the catalog as it is now, so a plan
/// says what the person is about to change, before they change it.
pub async fn for_plan(app: &Arc<App>, c: &Catalog, op: &Op) -> Result<Option<Value>, OpError> {
	let Some((schemas, tables)) = tables_of(op) else {
		return Ok(None);
	};
	let Some(home) = c.home().map(|n| n.id) else {
		return Ok(None);
	};
	let mut pg = connect_node(app, c, home).await?;
	let facts = read(&mut pg, c, &schemas, &[]).await;
	pg.close().await;
	let facts = facts?;
	let all = check(c, &facts);
	let found = if tables.is_empty() {
		all
	} else {
		scoped(&all, &tables)
	};
	Ok(Some(to_json(&found, &facts)))
}

/// The schema list of a query string's `name=a,b`, empty when absent.
pub fn query_list(path: &str, name: &str) -> Vec<String> {
	let Some((_, q)) = path.split_once('?') else {
		return Vec::new();
	};
	q.split('&')
		.filter_map(|p| p.split_once('='))
		.filter(|(k, _)| *k == name)
		.flat_map(|(_, v)| v.split(','))
		.map(str::trim)
		.filter(|s| !s.is_empty())
		.map(str::to_string)
		.collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::hash::KeyType;
	use crate::ops::auto::Column;
	use crate::ops::tests::cat;

	fn rel(t: &str) -> RelationName {
		RelationName {
			schema: "public".into(),
			table: t.into(),
		}
	}

	fn col(name: &str, seq: bool) -> Column {
		Column {
			name: name.into(),
			key_type: Some(KeyType::Int8),
			sequence: seq,
		}
	}

	fn t(name: &str, cols: &[&str], pk: &[&str], bytes: u64) -> TableFacts {
		TableFacts {
			name: rel(name),
			bytes,
			columns: cols.iter().map(|c| col(c, false)).collect(),
			primary: pk.iter().map(|s| s.to_string()).collect(),
			uniques: vec![(
				format!("{name}_pkey"),
				pk.iter().map(|s| s.to_string()).collect(),
			)],
			replayable: !pk.is_empty(),
			partitioned: false,
		}
	}

	fn fk(name: &str, from: &str, cols: &[&str], to: &str, to_cols: &[&str]) -> ForeignKey {
		ForeignKey {
			name: name.into(),
			from: rel(from),
			columns: cols.iter().map(|s| s.to_string()).collect(),
			to: rel(to),
			to_columns: to_cols.iter().map(|s| s.to_string()).collect(),
		}
	}

	fn ix(table: &str, cols: &[&str]) -> Index {
		Index {
			table: rel(table),
			columns: cols.iter().map(|s| s.to_string()).collect(),
		}
	}

	/// The test catalog (`cat`: keyspace `k`, int8, a home node and two data nodes) with
	/// `customer` distributed by its id.
	fn catalog() -> Catalog {
		let mut c = cat(3, 3);
		c.relations.clear();
		c.relations.insert(
			rel("customer"),
			RelationKind::Sharded {
				keyspace: "k".into(),
				key_column: "id".into(),
			},
		);
		c
	}

	fn codes(found: &[Finding]) -> Vec<(&'static str, String)> {
		found
			.iter()
			.map(|f| (f.code, f.table.table.clone()))
			.collect()
	}

	#[test]
	fn a_chain_follows_its_root_through_every_table_that_carries_the_key() {
		// customer -> package (by customer_id) -> payment (carries customer_id) -> line (does not).
		let c = catalog();
		let f = Facts {
			tables: vec![
				t("customer", &["id"], &["id"], 0),
				t("package", &["customer_id", "id"], &["customer_id", "id"], 0),
				t(
					"payment",
					&["customer_id", "id", "package_id"],
					&["customer_id", "id"],
					0,
				),
				t("line", &["id", "payment_id"], &["id"], 0),
			],
			fks: vec![
				fk(
					"package_customer",
					"package",
					&["customer_id"],
					"customer",
					&["id"],
				),
				fk(
					"payment_package",
					"payment",
					&["customer_id", "package_id"],
					"package",
					&["customer_id", "id"],
				),
				fk("line_payment", "line", &["payment_id"], "payment", &["id"]),
			],
			indexes: vec![
				ix("customer", &["id"]),
				ix("package", &["customer_id", "id"]),
				ix("payment", &["customer_id", "package_id"]),
				ix("line", &["payment_id"]),
			],
		};
		let found = check(&c, &f);
		assert_eq!(
			codes(&found),
			vec![
				("missing_key_column", "line".into()),
				("can_follow", "package".into()),
				("can_follow", "payment".into()),
			],
			"{found:#?}"
		);
		let follow = found.iter().find(|x| x.table == rel("payment")).unwrap();
		assert_eq!(
			follow.operation,
			Some(
				json!({"op": "table.distribute", "table": "public.payment", "column": "customer_id", "keyspace": "k"})
			)
		);
		let line = &found[0];
		assert!(
			line.message.contains("has no customer_id column"),
			"{line:?}"
		);
		assert_eq!(
			line.sql.as_deref(),
			Some(
				"alter table \"public\".\"line\" add column \"customer_id\" bigint;\nupdate \"public\".\"line\" c set \"customer_id\" = p.\"customer_id\" from \"public\".\"payment\" p where c.\"payment_id\" = p.\"id\";"
			)
		);
	}

	#[test]
	fn a_foreign_key_that_leaves_the_key_out_is_named_with_the_key_it_needs() {
		let c = catalog();
		let f = Facts {
			tables: vec![
				t("customer", &["id"], &["id"], 0),
				t("package", &["customer_id", "id"], &["customer_id", "id"], 0),
				t(
					"payment",
					&["customer_id", "id", "package_id"],
					&["customer_id", "id"],
					0,
				),
			],
			fks: vec![
				fk(
					"package_customer",
					"package",
					&["customer_id"],
					"customer",
					&["id"],
				),
				fk(
					"payment_package",
					"payment",
					&["package_id"],
					"package",
					&["id"],
				),
			],
			indexes: vec![
				ix("customer", &["id"]),
				ix("package", &["customer_id"]),
				ix("payment", &["package_id"]),
			],
		};
		let found = check(&c, &f);
		let bad = found.iter().find(|x| x.code == "fk_without_key").unwrap();
		assert_eq!(bad.severity, Severity::Error);
		assert_eq!(bad.table, rel("payment"));
		assert!(
			bad.fix
				.contains("(customer_id, package_id) references public.package (customer_id, id)"),
			"{bad:?}"
		);
	}

	#[test]
	fn a_lookup_a_distributed_table_references_off_its_key_must_be_everywhere() {
		let mut c = catalog();
		c.relations.insert(
			rel("package"),
			RelationKind::Sharded {
				keyspace: "k".into(),
				key_column: "customer_id".into(),
			},
		);
		let mut status = t("status", &["id", "name"], &["id"], 0);
		let mut staff = t("staff", &["id"], &["id"], 0);
		staff.columns[0].sequence = true;
		status.columns[0].sequence = false;
		let f = Facts {
			tables: vec![
				t("customer", &["id"], &["id"], 0),
				t(
					"package",
					&["customer_id", "id", "status_id", "seller_id"],
					&["customer_id", "id"],
					0,
				),
				status,
				staff,
			],
			fks: vec![
				fk(
					"package_customer",
					"package",
					&["customer_id"],
					"customer",
					&["id"],
				),
				fk(
					"package_status",
					"package",
					&["status_id"],
					"status",
					&["id"],
				),
				fk(
					"package_seller",
					"package",
					&["seller_id"],
					"staff",
					&["id"],
				),
			],
			indexes: vec![
				ix("customer", &["id"]),
				ix("package", &["customer_id", "id"]),
				ix("package", &["status_id"]),
			],
		};
		let found = check(&c, &f);
		let status = found
			.iter()
			.find(|x| x.code == "should_be_reference" && x.table == rel("status"))
			.unwrap();
		assert_eq!(
			status.operation,
			Some(json!({"op": "table.reference", "table": "public.status"}))
		);
		let staff = found
			.iter()
			.find(|x| x.code == "should_be_reference" && x.table == rel("staff"))
			.unwrap();
		assert!(staff.operation.is_none());
		assert!(staff.message.contains("filled by a sequence"), "{staff:?}");
		// package_seller has no index.
		let unindexed: Vec<_> = found
			.iter()
			.filter(|x| x.code == "fk_not_indexed")
			.collect();
		assert_eq!(unindexed.len(), 1, "{found:#?}");
		assert_eq!(
			unindexed[0].sql.as_deref(),
			Some("create index on \"public\".\"package\" (\"seller_id\");")
		);
		// Errors come first.
		assert_eq!(found[0].severity, Severity::Error);
	}

	#[test]
	fn indexes_on_the_key_and_size_are_checked() {
		let mut c = catalog();
		c.relations.insert(rel("lookup"), RelationKind::Reference);
		let f = Facts {
			tables: vec![
				t("customer", &["id", "name"], &["id"], 0),
				t("lookup", &["id"], &["id"], 3 << 30),
				t("events", &["id"], &["id"], 5 << 30),
				t("nopk", &["a"], &[], 0),
			],
			fks: vec![],
			// customer's only index begins with name, not its key.
			indexes: vec![
				ix("customer", &["name", "id"]),
				ix("lookup", &["id"]),
				ix("events", &["id"]),
			],
		};
		let found = check(&c, &f);
		assert_eq!(
			codes(&found),
			vec![
				("key_not_indexed", "customer".into()),
				("big_on_home", "events".into()),
				("reference_large", "lookup".into()),
				("no_primary_key", "nopk".into()),
			],
			"{found:#?}"
		);
	}

	#[test]
	fn a_table_that_belongs_but_is_refused_says_why() {
		let c = catalog();
		let mut package = t("package", &["customer_id", "id"], &["id"], 0);
		package.columns[1].sequence = true;
		let f = Facts {
			tables: vec![t("customer", &["id"], &["id"], 0), package],
			fks: vec![fk(
				"package_customer",
				"package",
				&["customer_id"],
				"customer",
				&["id"],
			)],
			indexes: vec![ix("customer", &["id"]), ix("package", &["customer_id"])],
		};
		let found = check(&c, &f);
		assert_eq!(codes(&found), vec![("blocked", "package".into())]);
		assert!(
			found[0].message.contains("does not include customer_id"),
			"{found:?}"
		);
	}

	#[test]
	fn a_plan_sees_the_findings_about_its_tables() {
		let c = catalog();
		let f = Facts {
			tables: vec![
				t("customer", &["id"], &["id"], 0),
				t("a", &["id", "customer_id"], &["id"], 0),
				t("b", &["id"], &["id"], 0),
			],
			fks: vec![
				fk("a_customer", "a", &["customer_id"], "customer", &["id"]),
				fk("b_a", "b", &["a_id"], "a", &["id"]),
			],
			indexes: vec![ix("customer", &["id"])],
		};
		let all = check(&c, &f);
		let only_b = scoped(&all, &BTreeSet::from([rel("b")]));
		assert!(!only_b.is_empty());
		assert!(
			only_b
				.iter()
				.all(|x| x.table == rel("b") || x.related.contains(&rel("b")))
		);
		assert!(only_b.len() < all.len());
	}

	#[test]
	fn before_anything_is_distributed_the_check_follows_table_autos_choice() {
		// Nothing distributed yet: customer is the tenant table auto would choose, so package
		// follows it, and line, with no customer_id, is told to add it.
		let mut c = cat(3, 3);
		c.relations.clear();
		let f = Facts {
			tables: vec![
				t("customer", &["id"], &["id"], 0),
				t("package", &["customer_id", "id"], &["customer_id", "id"], 0),
				t("invoice", &["customer_id", "id"], &["customer_id", "id"], 0),
				t(
					"line",
					&["id", "package_customer", "package_id"],
					&["package_customer", "id"],
					0,
				),
				t("note", &["id", "invoice_id"], &["id"], 0),
			],
			fks: vec![
				fk(
					"package_customer",
					"package",
					&["customer_id"],
					"customer",
					&["id"],
				),
				fk(
					"invoice_customer",
					"invoice",
					&["customer_id"],
					"customer",
					&["id"],
				),
				fk(
					"line_package",
					"line",
					&["package_customer", "package_id"],
					"package",
					&["customer_id", "id"],
				),
				fk("note_invoice", "note", &["invoice_id"], "invoice", &["id"]),
			],
			indexes: vec![
				ix("customer", &["id"]),
				ix("package", &["customer_id", "id"]),
				ix("invoice", &["customer_id", "id"]),
				ix("line", &["package_customer", "package_id"]),
				ix("note", &["invoice_id"]),
			],
		};
		let found = check(&c, &f);
		let at = |code: &str, table: &str| {
			found
				.iter()
				.find(|x| x.code == code && x.table == rel(table))
		};
		assert!(at("can_follow", "customer").is_some(), "{found:#?}");
		assert!(at("can_follow", "package").is_some(), "{found:#?}");
		// line follows package by the column its foreign key pairs with package's key.
		let line = at("can_follow", "line").unwrap();
		assert_eq!(
			line.operation.as_ref().unwrap()["column"],
			"package_customer"
		);
		assert!(at("missing_key_column", "note").is_some(), "{found:#?}");
	}

	#[test]
	fn chunk_tables_are_the_shells_own() {
		let mut c = catalog();
		c.chunked.insert(rel("customer"));
		assert!(is_chunk_table(&c, &rel("customer_c007")));
		assert!(is_chunk_table(&c, &rel("lepis_retired_customer_c007")));
		assert!(!is_chunk_table(&c, &rel("customer_c07")));
		assert!(!is_chunk_table(&c, &rel("other_c007")));
	}

	#[test]
	fn query_lists() {
		assert_eq!(
			query_list("/v1/check?schemas=a,b&exclude=auth", "schemas"),
			vec!["a", "b"]
		);
		assert!(query_list("/v1/check", "schemas").is_empty());
	}
}
