//! `table.auto`: a distribution key for every table, from the schema alone, so a
//! person need not answer the specialist's question table by table.
//!
//! The key is the column the foreign-key graph agrees on: the TENANT is the table whose primary key
//! the most other tables reference by one column, and every table that references it, or carries
//! the column those tables reference it by, is keyed by that column, so a tenant's rows stay
//! together and a join on the tenant goes to one node. A table with none of that is keyed by its
//! primary key's first column. A small table keyed only by its own primary key is not worth
//! spreading: it becomes a reference table when a distributed table references it, and otherwise
//! stays on the home node. Whatever L15 or L9 would refuse is left on the home node, and the plan
//! says why for every table, chosen or not.
//!
//! The choice is pure (`choose`, tested here); `plan` reads the facts from the home node, checks
//! every choice with `schema::check_movable` against the catalog as it will be after the job (a
//! linked group can only be checked together: each of two tables joined by a foreign key is
//! refused alone, since its partner is still on the home node), and moves the reference tables in
//! one transfer and the sharded ones in another, each one cutover (`Change::Many`).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use serde_json::{Value, json};

use super::spec::{Op, Step, TransferSpec};
use super::steps::connect_node;
use super::{Change, Kind, OpError, Pg, schema};
use crate::catalog::{Catalog, NodeId, NodeState, RelationKind, RelationName};
use crate::hash::KeyType;
use crate::server::App;

/// A table at or above this many bytes is worth spreading by its own primary key. Chosen: smaller
/// tables cost more in cross-node work than they save in room.
pub const MIN_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
	pub name: String,
	/// The column's type as a key type, when it is one (`KeyType::from_sql_name` of its base).
	pub key_type: Option<KeyType>,
	/// Filled by a sequence (an identity, or a `nextval` default).
	pub sequence: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableFacts {
	pub name: RelationName,
	pub bytes: u64,
	pub columns: Vec<Column>,
	/// The primary key's columns, in its order.
	pub primary: Vec<String>,
	/// Every primary key and unique constraint, by name, with its columns.
	pub uniques: Vec<(String, Vec<String>)>,
	/// Updates and deletes can be replayed by key (a primary key, or a replica identity).
	pub replayable: bool,
	pub partitioned: bool,
}

/// A foreign key: `from`'s `columns` reference `to`'s `to_columns`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForeignKey {
	pub name: String,
	pub from: RelationName,
	pub columns: Vec<String>,
	pub to: RelationName,
	pub to_columns: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Choice {
	Shard { column: String },
	Reference,
	Leave,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chosen {
	pub table: RelationName,
	pub choice: Choice,
	pub why: String,
}

impl Chosen {
	pub fn to_json(&self) -> Value {
		let (choice, column) = match &self.choice {
			Choice::Shard { column } => ("shard", Some(column.as_str())),
			Choice::Reference => ("reference", None),
			Choice::Leave => ("leave", None),
		};
		json!({"table": self.table.to_string(), "choice": choice, "column": column, "why": self.why})
	}
}

/// The tenant: the table whose one-column primary key, of the keyspace's type, the most other
/// tables reference by one column (ties to the first by name), with the name most of them use for
/// that column. None when no table is referenced so.
pub(crate) fn tenant(
	tables: &[TableFacts],
	fks: &[ForeignKey],
	key_type: KeyType,
) -> Option<(RelationName, String, String)> {
	let mut best: Option<(usize, &TableFacts, String)> = None;
	for t in tables {
		let [pk] = t.primary.as_slice() else {
			continue;
		};
		if column(t, pk).and_then(|c| c.key_type) != Some(key_type) {
			continue;
		}
		let mut referrers: BTreeMap<&RelationName, &str> = BTreeMap::new();
		for f in fks {
			if f.to == t.name
				&& f.from != t.name
				&& f.to_columns == [pk.clone()]
				&& f.columns.len() == 1
			{
				referrers.entry(&f.from).or_insert(f.columns[0].as_str());
			}
		}
		if referrers.is_empty() {
			continue;
		}
		let mut names: BTreeMap<&str, usize> = BTreeMap::new();
		for c in referrers.values() {
			*names.entry(c).or_default() += 1;
		}
		let common = names
			.iter()
			.max_by(|a, b| a.1.cmp(b.1).then(b.0.cmp(a.0)))
			.map(|(n, _)| n.to_string())
			.unwrap_or_default();
		if best.as_ref().is_none_or(|(n, b, _)| {
			referrers.len() > *n || (referrers.len() == *n && t.name < b.name)
		}) {
			best = Some((referrers.len(), t, common));
		}
	}
	best.map(|(_, t, common)| (t.name.clone(), t.primary[0].clone(), common))
}

fn column<'a>(t: &'a TableFacts, name: &str) -> Option<&'a Column> {
	t.columns.iter().find(|c| c.name == name)
}

/// What L15 refuses for `t` sharded by `key`, in the words `check_movable` uses.
pub(crate) fn shard_refusal(t: &TableFacts, key: &str) -> Option<String> {
	if !t.replayable {
		return Some(
			"it has no primary key, so a move cannot replay its updates and deletes".into(),
		);
	}
	if let Some((name, _)) = t
		.uniques
		.iter()
		.find(|(_, cols)| !cols.iter().any(|c| c == key))
	{
		return Some(format!(
			"constraint {name} does not include {key}: a node can only check uniqueness over what it holds"
		));
	}
	if let Some(c) = t.columns.iter().find(|c| c.sequence) {
		return Some(format!(
			"{} is filled by a sequence, which is not global across nodes: use uuidv7(), or a per-node sequence striped by node",
			c.name
		));
	}
	None
}

/// What keeps `t` from being a reference table: a reference table's copies are written separately
/// and must compute the same row, which a sequence would not.
pub(crate) fn reference_refusal(t: &TableFacts) -> Option<String> {
	if !t.replayable {
		return Some(
			"it has no primary key, so a copy cannot replay its updates and deletes".into(),
		);
	}
	t.columns.iter().find(|c| c.sequence).map(|c| {
		format!(
			"{} is filled by a sequence, and each copy of a reference table would fill it differently",
			c.name
		)
	})
}

/// One choice per table in `tables`, by name. `kinds` is the catalog's: a table already sharded or
/// made a reference table is left as it is. `forced` leaves a table on the home node with a reason
/// found outside the facts (a refusal of `check_movable`).
pub fn choose(
	tables: &[TableFacts],
	fks: &[ForeignKey],
	kinds: &HashMap<RelationName, RelationKind>,
	keyspace: &str,
	key_type: KeyType,
	min_bytes: u64,
	forced: &BTreeMap<RelationName, String>,
) -> Vec<Chosen> {
	let tenant = tenant(tables, fks, key_type);
	let mut out: BTreeMap<RelationName, Chosen> = BTreeMap::new();
	let mut small: BTreeSet<RelationName> = BTreeSet::new();
	// Keyed only by their own primary key: better as reference tables when referenced off-key.
	let mut by_pk: BTreeSet<RelationName> = BTreeSet::new();
	let leave = |t: &TableFacts, why: String| Chosen {
		table: t.name.clone(),
		choice: Choice::Leave,
		why,
	};
	for t in tables {
		let chosen = match kinds.get(&t.name) {
			Some(RelationKind::Sharded {
				keyspace,
				key_column,
			}) => leave(
				t,
				format!("already sharded by {key_column} in keyspace {keyspace}"),
			),
			Some(RelationKind::Reference) => leave(t, "already a reference table".into()),
			_ if forced.contains_key(&t.name) => leave(t, forced[&t.name].clone()),
			_ if t.partitioned => leave(t, "a partitioned table: distribute it by hand".into()),
			_ => {
				let typed = |c: &str| column(t, c).and_then(|c| c.key_type) == Some(key_type);
				let by_tenant = tenant.as_ref().and_then(|(tn, pk, common)| {
					if &t.name == tn {
						let n = count_referrers(fks, tn, pk);
						let tables = if n == 1 {
							"1 table references"
						} else {
							&format!("{n} tables reference")
						};
						return Some((pk.clone(), format!("the tenant: {tables} its {pk}")));
					}
					if let Some(f) = fks.iter().find(|f| {
						f.from == t.name
							&& &f.to == tn && f.to_columns == [pk.clone()]
							&& f.columns.len() == 1
							&& typed(&f.columns[0])
					}) {
						return Some((
							f.columns[0].clone(),
							format!("references the tenant {tn} by it"),
						));
					}
					typed(common).then(|| {
						(
							common.clone(),
							format!(
								"carries {common}, as the tables that reference the tenant {tn} do"
							),
						)
					})
				});
				let key = by_tenant.map(|(c, why)| (c, why, false)).or_else(|| {
					t.primary.first().filter(|c| typed(c)).map(|c| {
						(
							c.clone(),
							"its primary key's first column".to_string(),
							true,
						)
					})
				});
				match key {
					Some((_, _, true)) if t.bytes < min_bytes => {
						small.insert(t.name.clone());
						leave(t, "small, and no distributed table references it".into())
					}
					Some((c, why, pk)) => match shard_refusal(t, &c) {
						Some(r) => leave(t, format!("sharded by {c} ({why}), {r}")),
						None => {
							if pk {
								by_pk.insert(t.name.clone());
							}
							Chosen {
								table: t.name.clone(),
								choice: Choice::Shard { column: c },
								why,
							}
						}
					},
					None if t.bytes < min_bytes => {
						small.insert(t.name.clone());
						leave(t, "small, and no distributed table references it".into())
					}
					None => leave(
						t,
						format!(
							"no column of type {} to key it by: neither the tenant's nor a primary key",
							key_type.sql_name()
						),
					),
				}
			}
		};
		out.insert(t.name.clone(), chosen);
	}
	// A small table a candidate references becomes a reference table, when it can be one.
	for name in &small {
		let referenced = fks.iter().any(|f| {
			&f.to == name
				&& &f.from != name
				&& matches!(
					out.get(&f.from).map(|c| &c.choice),
					Some(Choice::Shard { .. })
				)
		});
		let t = tables
			.iter()
			.find(|t| &t.name == name)
			.expect("a table of the facts");
		if referenced {
			out.insert(
				name.clone(),
				match reference_refusal(t) {
					Some(r) => leave(t, format!("small and referenced, but {r}")),
					None => Chosen {
						table: name.clone(),
						choice: Choice::Reference,
						why: "small, and a distributed table references it".into(),
					},
				},
			);
		}
	}
	// The key a table will be sharded by in this keyspace, by this job or already.
	let key_after = |out: &BTreeMap<RelationName, Chosen>, r: &RelationName| -> Option<String> {
		match out.get(r).map(|c| &c.choice) {
			Some(Choice::Shard { column }) => Some(column.clone()),
			Some(_) => None,
			None => match kinds.get(r) {
				Some(RelationKind::Sharded {
					keyspace: k,
					key_column,
				}) if k == keyspace => Some(key_column.clone()),
				_ => None,
			},
		}
	};
	let sharded_after =
		|out: &BTreeMap<RelationName, Chosen>, r: &RelationName| key_after(out, r).is_some();
	// Whether `f` pairs its two tables' keys, so a row and the row it references share a node.
	let colocated = |out: &BTreeMap<RelationName, Chosen>, f: &ForeignKey| match (
		key_after(out, &f.from),
		key_after(out, &f.to),
	) {
		(Some(a), Some(b)) => f
			.columns
			.iter()
			.zip(&f.to_columns)
			.any(|(x, y)| *x == a && *y == b),
		_ => false,
	};
	// A table keyed by its own primary key and referenced by a sharded table off that key (a
	// plan, by `plan_id`) would have its rows elsewhere than the rows referencing them: a copy on
	// every node instead, whatever its size.
	let off_key: Vec<RelationName> = by_pk
		.iter()
		.filter(|p| {
			fks.iter().any(|f| {
				&f.to == *p && &f.from != *p && sharded_after(&out, &f.from) && !colocated(&out, f)
			})
		})
		.cloned()
		.collect();
	for name in off_key {
		let t = tables
			.iter()
			.find(|t| t.name == name)
			.expect("a table of the facts");
		let via =
			fks.iter()
				.find(|f| {
					f.to == name
						&& f.from != name && sharded_after(&out, &f.from)
						&& !colocated(&out, f)
				})
				.map(|f| format!("{} references it by {}", f.from, f.columns.join(", ")))
				.unwrap_or_default();
		out.insert(
			name.clone(),
			match reference_refusal(t) {
				Some(r) => leave(
					t,
					format!("{via}, not by its key, and it cannot be a reference table: {r}"),
				),
				None => Chosen {
					table: name.clone(),
					choice: Choice::Reference,
					why: format!("{via}, not by its key: a copy on every node"),
				},
			},
		);
	}
	// L9: a foreign key holds only between tables on the same node, so until nothing changes, a
	// sharded table linked to one that will be neither colocated on the key's columns nor (on its
	// referencing side) a reference table stays home, and so does a reference table referencing a
	// sharded one.
	let reference_after = |out: &BTreeMap<RelationName, Chosen>, r: &RelationName| {
		matches!(out.get(r).map(|c| &c.choice), Some(Choice::Reference))
			|| kinds.get(r) == Some(&RelationKind::Reference)
	};
	loop {
		let mut demote: Vec<(RelationName, String)> = Vec::new();
		for (name, c) in &out {
			let broken = match &c.choice {
				Choice::Shard { .. } => fks.iter().find_map(|f| {
					if &f.from == name && &f.to == name {
						(!colocated(&out, f)).then(|| {
							format!("foreign key {} links rows that may have different keys, so they would be on different nodes", f.name)
						})
					} else if &f.from == name && !sharded_after(&out, &f.to) && !reference_after(&out, &f.to) {
						Some(format!("foreign key {} references {}, which stays on the home node", f.name, f.to))
					} else if &f.from == name && sharded_after(&out, &f.to) && !colocated(&out, f) {
						Some(format!("foreign key {} references {} by columns that are not both distribution keys", f.name, f.to))
					} else if &f.to == name && &f.from != name && !sharded_after(&out, &f.from) {
						Some(format!("foreign key {} from {}, which is not sharded with it, references it", f.name, f.from))
					} else {
						None
					}
				}),
				Choice::Reference => fks.iter().find_map(|f| {
					(&f.from == name && &f.to != name && sharded_after(&out, &f.to))
						.then(|| format!("foreign key {} references {}, which is sharded", f.name, f.to))
				}),
				Choice::Leave => None,
			};
			if let Some(why) = broken {
				demote.push((name.clone(), why));
			}
		}
		if demote.is_empty() {
			break;
		}
		for (name, why) in demote {
			let c = out.get_mut(&name).expect("chosen");
			c.why = match &c.choice {
				Choice::Shard { column } => format!("would be sharded by {column}, but {why}"),
				_ => format!("would be a reference table, but {why}"),
			};
			c.choice = Choice::Leave;
		}
	}
	// A reference table nothing sharded references any more has no reason to be one.
	let unreferenced: Vec<RelationName> = out
		.iter()
		.filter(|(name, c)| {
			c.choice == Choice::Reference
				&& !fks
					.iter()
					.any(|f| &f.to == *name && &f.from != *name && sharded_after(&out, &f.from))
		})
		.map(|(n, _)| n.clone())
		.collect();
	for name in unreferenced {
		let c = out.get_mut(&name).expect("chosen");
		c.choice = Choice::Leave;
		c.why = "small, and no distributed table references it".into();
	}
	out.into_values().collect()
}

fn count_referrers(fks: &[ForeignKey], to: &RelationName, pk: &str) -> usize {
	fks.iter()
		.filter(|f| {
			&f.to == to && &f.from != to && f.to_columns == [pk.to_string()] && f.columns.len() == 1
		})
		.map(|f| &f.from)
		.collect::<BTreeSet<_>>()
		.len()
}

/// A column list as the queries below return it, joined by U+001F.
fn columns_of(v: Option<&Option<String>>) -> Vec<String> {
	v.cloned()
		.flatten()
		.map(|s| s.split('\u{1f}').map(str::to_string).collect())
		.unwrap_or_default()
}

/// The ordinary and partitioned tables of `schemas` on the home node (no partition, nothing an
/// extension owns), and every foreign key with one of them at either end.
pub async fn read_facts(
	pg: &mut Pg,
	schemas: &[String],
) -> Result<(Vec<TableFacts>, Vec<ForeignKey>), OpError> {
	let list = schemas.join(",");
	let tables_sql = "select c.oid::text, n.nspname, c.relname, pg_total_relation_size(c.oid)::text, \
		(c.relreplident in ('f', 'i') or (c.relreplident = 'd' and exists (select from pg_index i where i.indrelid = c.oid and i.indisprimary)))::text, \
		(c.relkind = 'p')::text \
		from pg_class c join pg_namespace n on n.oid = c.relnamespace \
		where c.relkind in ('r', 'p') and not c.relispartition and n.nspname = any (string_to_array($1, ',')) \
		and not exists (select from pg_depend d where d.classid = 'pg_class'::regclass and d.objid = c.oid and d.deptype = 'e') \
		order by n.nspname, c.relname";
	let mut tables: Vec<TableFacts> = Vec::new();
	let mut by_oid: HashMap<String, usize> = HashMap::new();
	for r in pg.query(tables_sql, &[&list]).await? {
		let text = |i: usize| r.get(i).cloned().flatten().unwrap_or_default();
		by_oid.insert(text(0), tables.len());
		tables.push(TableFacts {
			name: RelationName {
				schema: text(1),
				table: text(2),
			},
			bytes: text(3).parse().unwrap_or(0),
			columns: Vec::new(),
			primary: Vec::new(),
			uniques: Vec::new(),
			replayable: text(4) == "true",
			partitioned: text(5) == "true",
		});
	}
	let scope = "c.relkind in ('r', 'p') and not c.relispartition and n.nspname = any (string_to_array($1, ','))";
	for r in pg
		.query(
			&format!(
				"select c.oid::text, a.attname, format_type(a.atttypid, a.atttypmod), \
				(a.attidentity <> '' or coalesce(pg_get_expr(d.adbin, d.adrelid), '') like 'nextval(%')::text \
				from pg_attribute a join pg_class c on c.oid = a.attrelid join pg_namespace n on n.oid = c.relnamespace \
				left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum \
				where {scope} and a.attnum > 0 and not a.attisdropped order by c.oid, a.attnum"
			),
			&[&list],
		)
		.await?
	{
		let text = |i: usize| r.get(i).cloned().flatten().unwrap_or_default();
		if let Some(&i) = by_oid.get(&text(0)) {
			let ty = text(2);
			let base = ty.split('(').next().unwrap_or_default().trim().to_string();
			tables[i].columns.push(Column {
				name: text(1),
				key_type: KeyType::from_sql_name(&base),
				sequence: text(3) == "true",
			});
		}
	}
	let cols = |rel: &str, key: &str| {
		format!(
			"(select string_agg(a.attname, chr(31) order by u.ord) from unnest({key}) with ordinality u(attnum, ord) \
			join pg_attribute a on a.attrelid = {rel} and a.attnum = u.attnum)"
		)
	};
	for r in pg
		.query(
			&format!(
				"select k.conrelid::text, k.conname, k.contype::text, {} from pg_constraint k \
				join pg_class c on c.oid = k.conrelid join pg_namespace n on n.oid = c.relnamespace \
				where {scope} and k.contype in ('p', 'u') order by k.conrelid, k.conname",
				cols("k.conrelid", "k.conkey")
			),
			&[&list],
		)
		.await?
	{
		let text = |i: usize| r.get(i).cloned().flatten().unwrap_or_default();
		if let Some(&i) = by_oid.get(&text(0)) {
			let columns = columns_of(r.get(3));
			if text(2) == "p" {
				tables[i].primary = columns.clone();
			}
			tables[i].uniques.push((text(1), columns));
		}
	}
	let mut fks = Vec::new();
	for r in pg
		.query(
			&format!(
				"select k.conname, rn.nspname, r.relname, {}, fn.nspname, f.relname, {} from pg_constraint k \
				join pg_class r on r.oid = k.conrelid join pg_namespace rn on rn.oid = r.relnamespace \
				join pg_class f on f.oid = k.confrelid join pg_namespace fn on fn.oid = f.relnamespace \
				where k.contype = 'f' and (rn.nspname = any (string_to_array($1, ',')) or fn.nspname = any (string_to_array($1, ','))) \
				order by k.conname",
				cols("k.conrelid", "k.conkey"),
				cols("k.confrelid", "k.confkey")
			),
			&[&list],
		)
		.await?
	{
		let text = |i: usize| r.get(i).cloned().flatten().unwrap_or_default();
		fks.push(ForeignKey {
			name: text(0),
			from: RelationName {
				schema: text(1),
				table: text(2),
			},
			columns: columns_of(r.get(3)),
			to: RelationName {
				schema: text(4),
				table: text(5),
			},
			to_columns: columns_of(r.get(6)),
		});
	}
	Ok((tables, fks))
}

pub struct AutoPlan {
	pub steps: Vec<Step>,
	pub choices: Vec<Chosen>,
}

/// `table.auto` planned: the choices, each checked by `check_movable` against the catalog as the
/// job will leave it (a refusal leaves that table home and chooses again, until none is refused),
/// then the reference tables' transfer and the sharded tables', each one cutover.
pub async fn plan(app: &Arc<App>, c: &Catalog, home: NodeId, op: &Op) -> Result<AutoPlan, OpError> {
	let Op::TableAuto {
		keyspace,
		schemas,
		min_bytes,
	} = op
	else {
		unreachable!("auto::plan is table.auto's");
	};
	let k = c.keyspaces.get(keyspace).ok_or_else(|| {
		OpError::refused(
			Kind::NoSuchKeyspace,
			format!("there is no keyspace {keyspace}"),
		)
	})?;
	let mut h = connect_node(app, c, home).await?;
	let (tables, fks) = read_facts(&mut h, schemas).await?;
	let mut forced: BTreeMap<RelationName, String> = BTreeMap::new();
	let choices = loop {
		let choices = choose(
			&tables,
			&fks,
			&c.relations,
			keyspace,
			k.key_type,
			*min_bytes,
			&forced,
		);
		let mut after = c.clone();
		for ch in &choices {
			match &ch.choice {
				Choice::Shard { column } => {
					after.relations.insert(
						ch.table.clone(),
						RelationKind::Sharded {
							keyspace: keyspace.clone(),
							key_column: column.clone(),
						},
					);
				}
				Choice::Reference => {
					after
						.relations
						.insert(ch.table.clone(), RelationKind::Reference);
				}
				Choice::Leave => {}
			}
		}
		let mut refused = false;
		for ch in &choices {
			let shard = match &ch.choice {
				Choice::Shard { column } => Some((column.as_str(), k)),
				Choice::Reference => None,
				Choice::Leave => continue,
			};
			match schema::check_movable(&mut h, &ch.table, shard, &after).await {
				Ok(()) => {}
				// A refusal leaves the table home; anything else (the node gone) stops the plan.
				Err(e) if e.kind.is_some() => {
					forced.insert(ch.table.clone(), e.message);
					refused = true;
				}
				Err(e) => return Err(e),
			}
		}
		if !refused {
			break choices;
		}
	};
	let mut steps = Vec::new();
	let change = |kind: &dyn Fn(&Chosen) -> Option<RelationKind>| -> (Vec<RelationName>, Change) {
		let picked: Vec<(RelationName, RelationKind)> = choices
			.iter()
			.filter_map(|ch| kind(ch).map(|k| (ch.table.clone(), k)))
			.collect();
		(
			picked.iter().map(|(n, _)| n.clone()).collect(),
			Change::Many(
				picked
					.into_iter()
					.map(|(name, kind)| Change::Relation { name, kind })
					.collect(),
			),
		)
	};
	let (refs, ref_change) =
		change(&|ch| (ch.choice == Choice::Reference).then_some(RelationKind::Reference));
	if !refs.is_empty() {
		let targets: Vec<NodeId> = c
			.nodes
			.values()
			.filter(|n| !n.home && n.state != NodeState::Removed)
			.map(|n| n.id)
			.collect();
		steps.push(if targets.is_empty() {
			Step::Catalog(ref_change)
		} else {
			Step::Transfer(TransferSpec {
				source: home,
				targets,
				tables: refs,
				change: ref_change,
			})
		});
	}
	let (sharded, shard_change) = change(&|ch| match &ch.choice {
		Choice::Shard { column } => Some(RelationKind::Sharded {
			keyspace: keyspace.clone(),
			key_column: column.clone(),
		}),
		_ => None,
	});
	if !sharded.is_empty() {
		let targets: Vec<NodeId> = c
			.nodes_of(keyspace)
			.into_iter()
			.filter(|n| *n != home)
			.collect();
		if targets.is_empty() {
			steps.push(Step::Catalog(shard_change));
			steps.push(Step::Fences {
				keyspace: keyspace.clone(),
			});
		} else {
			steps.push(Step::Transfer(TransferSpec {
				source: home,
				targets,
				tables: sharded,
				change: shard_change,
			}));
		}
	}
	Ok(AutoPlan { steps, choices })
}

#[cfg(test)]
mod tests {
	use super::*;

	fn rel(t: &str) -> RelationName {
		RelationName {
			schema: "public".into(),
			table: t.into(),
		}
	}

	fn col(name: &str, ty: Option<KeyType>) -> Column {
		Column {
			name: name.into(),
			key_type: ty,
			sequence: false,
		}
	}

	const BIG: u64 = 1 << 30;
	const UUID: Option<KeyType> = Some(KeyType::Uuid);

	/// A table keyed by `primary`, with uuid columns `cols` (and `name`, text).
	fn table(name: &str, bytes: u64, cols: &[&str], primary: &[&str]) -> TableFacts {
		let mut columns: Vec<Column> = cols.iter().map(|c| col(c, UUID)).collect();
		columns.push(col("name", Some(KeyType::Text)));
		TableFacts {
			name: rel(name),
			bytes,
			columns,
			primary: primary.iter().map(|c| c.to_string()).collect(),
			uniques: vec![(
				format!("{name}_pkey"),
				primary.iter().map(|c| c.to_string()).collect(),
			)],
			replayable: !primary.is_empty(),
			partitioned: false,
		}
	}

	fn fk(from: &str, cols: &[&str], to: &str, to_cols: &[&str]) -> ForeignKey {
		ForeignKey {
			name: format!("{from}_{}_fkey", cols.join("_")),
			from: rel(from),
			columns: cols.iter().map(|c| c.to_string()).collect(),
			to: rel(to),
			to_columns: to_cols.iter().map(|c| c.to_string()).collect(),
		}
	}

	fn run(tables: &[TableFacts], fks: &[ForeignKey]) -> BTreeMap<String, (Choice, String)> {
		choose(
			tables,
			fks,
			&HashMap::new(),
			"app",
			KeyType::Uuid,
			MIN_BYTES,
			&BTreeMap::new(),
		)
		.into_iter()
		.map(|c| (c.table.table.clone(), (c.choice, c.why)))
		.collect()
	}

	fn shard(c: &str) -> Choice {
		Choice::Shard { column: c.into() }
	}

	/// A multi-tenant schema: accounts, the tenant; projects and invoices reference it; tasks
	/// carries account_id with a composite key to projects; plans is a small lookup table the
	/// accounts reference; audit is big with nothing to do with the tenant; notes is small alone.
	fn saas() -> (Vec<TableFacts>, Vec<ForeignKey>) {
		(
			vec![
				table("accounts", BIG, &["id", "plan_id"], &["id"]),
				table(
					"projects",
					BIG,
					&["account_id", "id"],
					&["account_id", "id"],
				),
				table(
					"invoices",
					1024,
					&["account_id", "id"],
					&["account_id", "id"],
				),
				table(
					"tasks",
					BIG,
					&["account_id", "project_id", "id"],
					&["account_id", "id"],
				),
				table("plans", 1024, &["id"], &["id"]),
				table("audit", BIG, &["id"], &["id"]),
				table("notes", 1024, &["id"], &["id"]),
			],
			vec![
				fk("projects", &["account_id"], "accounts", &["id"]),
				fk("invoices", &["account_id"], "accounts", &["id"]),
				fk(
					"tasks",
					&["account_id", "project_id"],
					"projects",
					&["account_id", "id"],
				),
				fk("accounts", &["plan_id"], "plans", &["id"]),
			],
		)
	}

	#[test]
	fn the_tenant_keys_every_table_that_references_or_carries_it() {
		let (tables, fks) = saas();
		let got = run(&tables, &fks);
		assert_eq!(got["accounts"].0, shard("id"));
		assert!(
			got["accounts"]
				.1
				.contains("the tenant: 2 tables reference its id"),
			"{}",
			got["accounts"].1
		);
		assert_eq!(got["projects"].0, shard("account_id"));
		// Small, and still sharded: its tenant column keeps it with its account.
		assert_eq!(got["invoices"].0, shard("account_id"));
		assert_eq!(got["tasks"].0, shard("account_id"));
		assert!(
			got["tasks"].1.contains("carries account_id"),
			"{}",
			got["tasks"].1
		);
		// Small and referenced by a sharded table: a copy on every node.
		assert_eq!(got["plans"].0, Choice::Reference);
		// Big and unrelated: its own primary key.
		assert_eq!(got["audit"].0, shard("id"));
		assert_eq!(got["audit"].1, "its primary key's first column");
		// Small and alone: home.
		assert_eq!(got["notes"].0, Choice::Leave);
	}

	#[test]
	fn what_l15_refuses_stays_home_and_says_why() {
		let (mut tables, fks) = saas();
		// audit keyed by a bigserial, events with a name unique on its own: neither is linked to
		// anything, so the tenant's tables are unaffected.
		tables[5].columns[0].sequence = true;
		let mut events = table("events", BIG, &["id"], &["id"]);
		events
			.uniques
			.push(("events_name_key".into(), vec!["name".into()]));
		tables.push(events);
		let got = run(&tables, &fks);
		assert_eq!(got["audit"].0, Choice::Leave);
		assert!(
			got["audit"].1.contains("id is filled by a sequence"),
			"{}",
			got["audit"].1
		);
		assert_eq!(got["events"].0, Choice::Leave);
		assert!(
			got["events"]
				.1
				.contains("constraint events_name_key does not include id"),
			"{}",
			got["events"].1
		);
		assert_eq!(got["accounts"].0, shard("id"));
		assert_eq!(got["tasks"].0, shard("account_id"));
		assert_eq!(got["plans"].0, Choice::Reference);
	}

	#[test]
	fn a_table_left_home_takes_what_it_links_to_home_with_it() {
		let (mut tables, fks) = saas();
		// projects with a name unique across accounts: it stays home, so tasks (which references
		// it) cannot go, nor accounts (which it references), nor what references accounts. L9: a
		// key between a home table and a sharded one is refused in either direction.
		tables[1]
			.uniques
			.push(("projects_name_key".into(), vec!["name".into()]));
		let got = run(&tables, &fks);
		assert!(
			got["projects"]
				.1
				.contains("constraint projects_name_key does not include account_id")
		);
		assert!(
			got["tasks"]
				.1
				.contains("references public.projects, which stays on the home node"),
			"{}",
			got["tasks"].1
		);
		assert!(
			got["accounts"]
				.1
				.contains("from public.projects, which is not sharded with it"),
			"{}",
			got["accounts"].1
		);
		for t in ["projects", "tasks", "accounts", "invoices", "plans"] {
			assert_eq!(got[t].0, Choice::Leave, "{t}: {}", got[t].1);
		}
		assert_eq!(got["audit"].0, shard("id"));
	}

	#[test]
	fn a_home_table_referencing_a_candidate_keeps_it_home() {
		let (mut tables, mut fks) = saas();
		// A global table (no uuid key at all) references accounts: accounts cannot leave the home
		// node without breaking that key, and neither can anything that needs accounts.
		let mut logs = table("logs", BIG, &[], &[]);
		logs.columns.push(col("account_id", UUID));
		// No primary key, a replica identity instead.
		logs.uniques.clear();
		logs.replayable = true;
		tables.push(logs);
		fks.push(fk("logs", &["account_id"], "accounts", &["id"]));
		let got = run(&tables, &fks);
		// logs carries account_id, so it is sharded with the tenant and nothing breaks.
		assert_eq!(got["logs"].0, shard("account_id"));
		// Without the column's type matching, it cannot be, and the tenant stays home with it.
		tables
			.last_mut()
			.unwrap()
			.columns
			.last_mut()
			.unwrap()
			.key_type = Some(KeyType::Int8);
		let got = run(&tables, &fks);
		assert_eq!(got["logs"].0, Choice::Leave);
		assert_eq!(got["accounts"].0, Choice::Leave);
		assert!(
			got["accounts"]
				.1
				.contains("from public.logs, which is not sharded with it"),
			"{}",
			got["accounts"].1
		);
		// The tenant's dependents follow it home; plans, referenced by nothing sharded, is left too.
		for t in ["projects", "invoices", "tasks"] {
			assert_eq!(got[t].0, Choice::Leave, "{t}: {}", got[t].1);
		}
		assert_eq!(got["plans"].0, Choice::Leave);
		// audit has no link to any of it.
		assert_eq!(got["audit"].0, shard("id"));
	}

	#[test]
	fn a_reference_table_filled_by_a_sequence_is_refused_and_takes_its_referrer_home() {
		let (mut tables, fks) = saas();
		tables[4].columns[0].sequence = true;
		let got = run(&tables, &fks);
		assert_eq!(got["plans"].0, Choice::Leave);
		assert!(
			got["plans"]
				.1
				.contains("each copy of a reference table would fill it differently")
		);
		// accounts references plans, which stays home: the tenant cannot go, nor what follows it.
		assert_eq!(got["accounts"].0, Choice::Leave);
		assert_eq!(got["projects"].0, Choice::Leave);
	}

	#[test]
	fn what_the_catalog_already_placed_and_what_was_forced_are_left_as_they_are() {
		let (tables, fks) = saas();
		let mut kinds = HashMap::new();
		kinds.insert(
			rel("audit"),
			RelationKind::Sharded {
				keyspace: "app".into(),
				key_column: "id".into(),
			},
		);
		let mut forced = BTreeMap::new();
		forced.insert(rel("notes"), "refused by the node".to_string());
		let got: BTreeMap<String, (Choice, String)> = choose(
			&tables,
			&fks,
			&kinds,
			"app",
			KeyType::Uuid,
			MIN_BYTES,
			&forced,
		)
		.into_iter()
		.map(|c| (c.table.table.clone(), (c.choice, c.why)))
		.collect();
		assert_eq!(
			got["audit"],
			(
				Choice::Leave,
				"already sharded by id in keyspace app".into()
			)
		);
		assert_eq!(got["notes"], (Choice::Leave, "refused by the node".into()));
		assert_eq!(got["accounts"].0, shard("id"));
	}

	#[test]
	fn a_table_referenced_off_its_key_is_copied_and_a_self_key_must_stay_on_the_key() {
		let (tables, fks) = saas();
		// plans as big as anything: still referenced by plan_id, not by the key accounts has.
		let mut big = tables.clone();
		big[4].bytes = BIG;
		let got = run(&big, &fks);
		assert_eq!(got["plans"].0, Choice::Reference);
		assert!(
			got["plans"]
				.1
				.contains("public.accounts references it by plan_id, not by its key"),
			"{}",
			got["plans"].1
		);
		assert_eq!(got["accounts"].0, shard("id"));
		// tasks with a parent task by id alone: rows of two accounts could be linked.
		let mut selfish = tables.clone();
		selfish[3].columns.push(col("parent_id", UUID));
		let mut fks2 = fks.clone();
		fks2.push(fk("tasks", &["parent_id"], "tasks", &["id"]));
		let got = run(&selfish, &fks2);
		assert_eq!(got["tasks"].0, Choice::Leave);
		assert!(
			got["tasks"]
				.1
				.contains("links rows that may have different keys"),
			"{}",
			got["tasks"].1
		);
		// With the account in the key on both sides, it holds on each node.
		let mut fks3 = fks.clone();
		fks3.push(fk(
			"tasks",
			&["account_id", "parent_id"],
			"tasks",
			&["account_id", "id"],
		));
		assert_eq!(run(&selfish, &fks3)["tasks"].0, shard("account_id"));
	}

	#[test]
	fn with_no_tenant_the_primary_key_keys_big_tables_and_the_wrong_type_is_named() {
		let tables = vec![
			table("events", BIG, &["id"], &["id"]),
			table("tiny", 10, &["id"], &["id"]),
		];
		let got = run(&tables, &[]);
		assert_eq!(got["events"].0, shard("id"));
		assert_eq!(got["tiny"].0, Choice::Leave);
		let got: Vec<Chosen> = choose(
			&tables,
			&[],
			&HashMap::new(),
			"app",
			KeyType::Int8,
			MIN_BYTES,
			&BTreeMap::new(),
		);
		assert_eq!(got[0].choice, Choice::Leave);
		assert!(
			got[0].why.contains("no column of type bigint"),
			"{}",
			got[0].why
		);
	}
}
