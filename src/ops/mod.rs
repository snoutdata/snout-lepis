//! Phase 4: the operations engine (L8, L10, L13): durable jobs that add, drain and remove nodes,
//! split, merge and move ranges, pin tenants, distribute tables, verify and clean up, all online.
//!
//! The shape, top to bottom:
//!
//! - **An operation** (`spec::Op`) is what a person, the CLI or the dashboard asks for. `plan`
//!   expands it into **steps** (`spec::Step`) against the current catalog and says what will
//!   move, how much, how long the copy should take and what pause to expect. Nothing runs.
//! - **A job** is a planned operation written to `lepis.job` + `lepis.job_step` on the home
//!   node. Its steps are fixed when it is written, so whichever router runs it, and however often
//!   it is resumed, it is the same plan. One router at a time runs jobs: the one holding the
//!   cluster's advisory lock on the home node (`engine.rs`). Jobs run one after another.
//! - **A step is idempotent.** It records its progress in `job_step.detail` as it goes (the exact
//!   names of what it made, the phase it reached), and running it again after a crash reads that
//!   and carries on.
//! - **The move** (`transfer.rs`) is L8's logical path: a publication on the source whose row
//!   filter is the slice that moves (the source's fence AND the target's fence after the move), a
//!   subscription on the target, the lag watched until a marker row round-trips quickly, then
//!   L10's cutover.
//! - **Every catalog change** is computed by applying a `Change` to an in-memory copy of the
//!   catalog and writing the difference, with `lepis.bump()`, in one transaction (`write_catalog`).

pub mod advisor;
pub mod attach;
pub mod engine;
pub mod pg;
pub mod physical;
pub mod plan;
pub mod restore_point;
pub mod schema;
pub mod spec;
pub mod steps;
pub mod transfer;

use std::time::Duration;

use serde_json::{Value, json};

use crate::catalog::{
	self, Catalog, Keyspace, NodeId, NodeState, Range, RelationKind, RelationName, Strategy,
	quote_ident, quote_literal,
};
use crate::hash::KeyType;
pub use pg::{Kind, OpError, Pg, Rows, Target};

/// The catalog DDL this module adds (also part of `catalog::CATALOG_SQL`); the engine applies it
/// to a catalog made before Phase 4.
pub const JOBS_SQL: &str = include_str!("../catalog_jobs.sql");

/// The choices L10 leaves to the user, from `lepis.cluster.settings`, overridable per job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Settings {
	pub max_write_pause: Duration,
	pub drain_timeout: Duration,
	pub ack_timeout: Duration,
	/// The copy rate a plan assumes. Chosen, not measured: a plan's time is an estimate.
	pub copy_mb_per_s: u64,
}

impl Default for Settings {
	fn default() -> Self {
		Settings {
			max_write_pause: Duration::from_millis(2000),
			drain_timeout: Duration::from_millis(5000),
			ack_timeout: Duration::from_millis(2000),
			copy_mb_per_s: 50,
		}
	}
}

impl Settings {
	/// The cluster's settings, then any of the same names in `over` (a job's arguments).
	pub fn from_json(cluster: &Value, over: &Value) -> Settings {
		let d = Settings::default();
		let ms = |k: &str, d: Duration| -> Duration {
			over.get(k)
				.and_then(Value::as_u64)
				.or_else(|| cluster.get(k).and_then(Value::as_u64))
				.map_or(d, Duration::from_millis)
		};
		Settings {
			max_write_pause: ms("max_write_pause_ms", d.max_write_pause),
			drain_timeout: ms("drain_timeout_ms", d.drain_timeout),
			ack_timeout: ms("ack_timeout_ms", d.ack_timeout),
			copy_mb_per_s: over
				.get("copy_mb_per_s")
				.and_then(Value::as_u64)
				.or_else(|| cluster.get("copy_mb_per_s").and_then(Value::as_u64))
				.unwrap_or(d.copy_mb_per_s)
				.max(1),
		}
	}

	pub fn to_json(self) -> Value {
		json!({
			"max_write_pause_ms": self.max_write_pause.as_millis() as u64,
			"drain_timeout_ms": self.drain_timeout.as_millis() as u64,
			"ack_timeout_ms": self.ack_timeout.as_millis() as u64,
			"copy_mb_per_s": self.copy_mb_per_s,
		})
	}
}

/// One change to the catalog. Applying it is idempotent: applied twice, the second time finds
/// it done and changes nothing, which is what lets a step that crashed after committing it run
/// again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
	None,
	RangeOwner {
		keyspace: String,
		lo: i64,
		hi: i64,
		to: NodeId,
	},
	PinOwner {
		keyspace: String,
		value: String,
		to: NodeId,
	},
	Pin {
		keyspace: String,
		value: String,
		node: NodeId,
	},
	Split {
		keyspace: String,
		lo: i64,
		at: i64,
	},
	Merge {
		keyspace: String,
		a: i64,
		b: i64,
	},
	Relation {
		name: RelationName,
		kind: RelationKind,
	},
	Keyspace {
		name: String,
		key_type: KeyType,
		seed: u64,
		ranges: Vec<Range>,
	},
	NodeState {
		node: NodeId,
		state: NodeState,
	},
	/// Several changes applied together (one cutover moving several ranges).
	Many(Vec<Change>),
}

fn ks_mut<'a>(c: &'a mut Catalog, name: &str) -> Result<&'a mut Keyspace, OpError> {
	c.keyspaces.get_mut(name).ok_or_else(|| {
		OpError::refused(Kind::NoSuchKeyspace, format!("there is no keyspace {name}"))
	})
}

impl Change {
	pub fn apply(&self, c: &mut Catalog) -> Result<(), OpError> {
		match self {
			Change::None => {}
			Change::RangeOwner {
				keyspace,
				lo,
				hi,
				to,
			} => {
				let ks = ks_mut(c, keyspace)?;
				let r = ks
					.ranges
					.iter_mut()
					.find(|r| r.lo == *lo && r.hi == *hi)
					.ok_or_else(|| {
						OpError::refused(
							Kind::NoSuchRange,
							format!("keyspace {keyspace} has no range [{lo}, {hi}] any more"),
						)
					})?;
				r.node = *to;
			}
			Change::PinOwner {
				keyspace,
				value,
				to,
			} => {
				let ks = ks_mut(c, keyspace)?;
				let p = ks.pins.get_mut(value).ok_or_else(|| {
					OpError::refused(
						Kind::NoSuchPin,
						format!("keyspace {keyspace} has no pin {value}"),
					)
				})?;
				*p = *to;
			}
			Change::Pin {
				keyspace,
				value,
				node,
			} => {
				let ks = ks_mut(c, keyspace)?;
				ks.pins.entry(value.clone()).or_insert(*node);
			}
			Change::Split { keyspace, lo, at } => {
				let ks = ks_mut(c, keyspace)?;
				if ks.ranges.iter().any(|r| r.lo == *at) && ks.ranges.iter().any(|r| r.lo == *lo) {
					return Ok(());
				}
				let i = ks
					.ranges
					.iter()
					.position(|r| r.lo == *lo && r.hi >= *at && *at > *lo)
					.ok_or_else(|| {
						OpError::refused(
							Kind::NoSuchRange,
							format!(
								"keyspace {keyspace} has no range starting at {lo} that {at} falls inside"
							),
						)
					})?;
				let r = ks.ranges[i];
				ks.ranges[i].hi = at - 1;
				ks.ranges.insert(
					i + 1,
					Range {
						lo: *at,
						hi: r.hi,
						node: r.node,
					},
				);
			}
			Change::Merge { keyspace, a, b } => {
				let ks = ks_mut(c, keyspace)?;
				let ia = ks.ranges.iter().position(|r| r.lo == *a).ok_or_else(|| {
					OpError::refused(
						Kind::NoSuchRange,
						format!("keyspace {keyspace} has no range starting at {a}"),
					)
				})?;
				let Some(ib) = ks.ranges.iter().position(|r| r.lo == *b) else {
					// Already merged when a's range reaches past b.
					if ks.ranges[ia].hi >= *b {
						return Ok(());
					}
					return Err(OpError::refused(
						Kind::NoSuchRange,
						format!("keyspace {keyspace} has no range starting at {b}"),
					));
				};
				let (ra, rb) = (ks.ranges[ia], ks.ranges[ib]);
				if ra.hi.checked_add(1) != Some(rb.lo) {
					return Err(OpError::refused(
						Kind::NotAdjacent,
						format!("ranges {a} and {b} of {keyspace} are not adjacent"),
					));
				}
				if ra.node != rb.node {
					return Err(OpError::refused(
						Kind::RangesOnDifferentNodes,
						format!(
							"ranges {a} and {b} of {keyspace} are on different nodes; move one first"
						),
					));
				}
				ks.ranges[ia].hi = rb.hi;
				ks.ranges.remove(ib);
			}
			Change::Relation { name, kind } => {
				c.relations.insert(name.clone(), kind.clone());
			}
			Change::Keyspace {
				name,
				key_type,
				seed,
				ranges,
			} => match c.keyspaces.get(name) {
				Some(k) if k.key_type == *key_type && k.seed == *seed => {}
				Some(_) => {
					return Err(OpError::refused(
						Kind::KeyspaceExists,
						format!("keyspace {name} exists with another key type or seed"),
					));
				}
				None => {
					c.keyspaces.insert(
						name.clone(),
						Keyspace {
							name: name.clone(),
							strategy: Strategy::Hash,
							key_type: *key_type,
							seed: *seed,
							ranges: ranges.clone(),
							pins: Default::default(),
						},
					);
				}
			},
			Change::NodeState { node, state } => {
				c.nodes
					.get_mut(node)
					.ok_or_else(|| {
						OpError::refused(Kind::NoSuchNode, format!("there is no {node}"))
					})?
					.state = *state;
			}
			Change::Many(all) => {
				for ch in all {
					ch.apply(c)?;
				}
			}
		}
		Ok(())
	}

	pub fn to_json(&self) -> Value {
		match self {
			Change::None => json!({"none": {}}),
			Change::RangeOwner {
				keyspace,
				lo,
				hi,
				to,
			} => {
				json!({"range_owner": {"keyspace": keyspace, "lo": lo.to_string(), "hi": hi.to_string(), "to": to.0}})
			}
			Change::PinOwner {
				keyspace,
				value,
				to,
			} => json!({"pin_owner": {"keyspace": keyspace, "value": value, "to": to.0}}),
			Change::Pin {
				keyspace,
				value,
				node,
			} => json!({"pin": {"keyspace": keyspace, "value": value, "node": node.0}}),
			Change::Split { keyspace, lo, at } => {
				json!({"split": {"keyspace": keyspace, "lo": lo.to_string(), "at": at.to_string()}})
			}
			Change::Merge { keyspace, a, b } => {
				json!({"merge": {"keyspace": keyspace, "a": a.to_string(), "b": b.to_string()}})
			}
			Change::Relation { name, kind } => {
				let (k, ks, col) = kind_parts(kind);
				json!({"relation": {"table": name.to_string(), "kind": k, "keyspace": ks, "column": col}})
			}
			Change::Keyspace {
				name,
				key_type,
				seed,
				ranges,
			} => json!({"keyspace": {
				"name": name, "key_type": key_type.sql_name(), "seed": seed.to_string(),
				"ranges": ranges.iter().map(|r| json!([r.lo.to_string(), r.hi.to_string(), r.node.0])).collect::<Vec<_>>(),
			}}),
			Change::NodeState { node, state } => {
				json!({"node_state": {"node": node.0, "state": state_name(*state)}})
			}
			Change::Many(all) => {
				json!({"many": all.iter().map(Change::to_json).collect::<Vec<_>>()})
			}
		}
	}

	pub fn from_json(v: &Value) -> Result<Change, OpError> {
		let (k, a) = v
			.as_object()
			.and_then(|o| o.iter().next())
			.ok_or_else(|| OpError::new("a change is an object with one key"))?;
		Ok(match k.as_str() {
			"none" => Change::None,
			"range_owner" => Change::RangeOwner {
				keyspace: s(a, "keyspace")?,
				lo: i(a, "lo")?,
				hi: i(a, "hi")?,
				to: NodeId(i(a, "to")? as i32),
			},
			"pin_owner" => Change::PinOwner {
				keyspace: s(a, "keyspace")?,
				value: s(a, "value")?,
				to: NodeId(i(a, "to")? as i32),
			},
			"pin" => Change::Pin {
				keyspace: s(a, "keyspace")?,
				value: s(a, "value")?,
				node: NodeId(i(a, "node")? as i32),
			},
			"split" => Change::Split {
				keyspace: s(a, "keyspace")?,
				lo: i(a, "lo")?,
				at: i(a, "at")?,
			},
			"merge" => Change::Merge {
				keyspace: s(a, "keyspace")?,
				a: i(a, "a")?,
				b: i(a, "b")?,
			},
			"relation" => Change::Relation {
				name: relation_name(&s(a, "table")?)?,
				kind: match s(a, "kind")?.as_str() {
					"sharded" => RelationKind::Sharded {
						keyspace: s(a, "keyspace")?,
						key_column: s(a, "column")?,
					},
					"reference" => RelationKind::Reference,
					_ => RelationKind::Global,
				},
			},
			"keyspace" => Change::Keyspace {
				name: s(a, "name")?,
				key_type: KeyType::from_sql_name(&s(a, "key_type")?)
					.ok_or_else(|| OpError::new("unknown key type"))?,
				seed: u64_of(a.get("seed")).ok_or_else(|| OpError::new("seed"))?,
				ranges: a
					.get("ranges")
					.and_then(Value::as_array)
					.map(|rs| {
						rs.iter()
							.filter_map(|r| {
								Some(Range {
									lo: i64_of(r.get(0)?)?,
									hi: i64_of(r.get(1)?)?,
									node: NodeId(r.get(2)?.as_i64()? as i32),
								})
							})
							.collect()
					})
					.unwrap_or_default(),
			},
			"node_state" => Change::NodeState {
				node: NodeId(i(a, "node")? as i32),
				state: parse_state(&s(a, "state")?)?,
			},
			"many" => Change::Many(
				a.as_array()
					.map(|x| {
						x.iter()
							.map(Change::from_json)
							.collect::<Result<Vec<_>, _>>()
					})
					.transpose()?
					.unwrap_or_default(),
			),
			other => return Err(OpError::new(format!("unknown change {other}"))),
		})
	}
}

pub fn kind_parts(kind: &RelationKind) -> (&'static str, Option<String>, Option<String>) {
	match kind {
		RelationKind::Sharded {
			keyspace,
			key_column,
		} => ("sharded", Some(keyspace.clone()), Some(key_column.clone())),
		RelationKind::Reference => ("reference", None, None),
		RelationKind::Global => ("global", None, None),
	}
}

pub fn state_name(s: NodeState) -> &'static str {
	match s {
		NodeState::Joining => "joining",
		NodeState::Active => "active",
		NodeState::Draining => "draining",
		NodeState::Removed => "removed",
	}
}

pub fn parse_state(s: &str) -> Result<NodeState, OpError> {
	Ok(match s {
		"joining" => NodeState::Joining,
		"active" => NodeState::Active,
		"draining" => NodeState::Draining,
		"removed" => NodeState::Removed,
		_ => {
			return Err(OpError::refused(
				Kind::BadRequest,
				format!("unknown node state {s}"),
			));
		}
	})
}

/// `schema.table`, or a bare `table` in `public`. Quoted names are not accepted: the catalog
/// stores names as Postgres folds them.
pub fn relation_name(s: &str) -> Result<RelationName, OpError> {
	let (schema, table) = s.split_once('.').unwrap_or(("public", s));
	if schema.is_empty() || table.is_empty() || table.contains('.') {
		return Err(OpError::refused(
			Kind::BadRequest,
			format!("{s}: name a table as schema.table"),
		));
	}
	Ok(RelationName {
		schema: schema.to_string(),
		table: table.to_string(),
	})
}

pub fn qualified(r: &RelationName) -> String {
	format!("{}.{}", quote_ident(&r.schema), quote_ident(&r.table))
}

/// A 64-bit number from JSON, as a string (how Lepis sends them: JavaScript loses precision
/// past 2^53) or as a number.
pub fn i64_of(v: &Value) -> Option<i64> {
	v.as_i64()
		.or_else(|| v.as_str().and_then(|t| t.parse().ok()))
}

pub fn u64_of(v: Option<&Value>) -> Option<u64> {
	v.and_then(|v| {
		v.as_u64()
			.or_else(|| v.as_str().and_then(|t| t.parse().ok()))
	})
}

pub(crate) fn s(v: &Value, k: &str) -> Result<String, OpError> {
	v.get(k)
		.and_then(Value::as_str)
		.map(str::to_string)
		.ok_or_else(|| OpError::refused(Kind::BadRequest, format!("{k} is required (a string)")))
}

pub(crate) fn i(v: &Value, k: &str) -> Result<i64, OpError> {
	v.get(k)
		.and_then(|x| {
			x.as_i64()
				.or_else(|| x.as_str().and_then(|t| t.parse().ok()))
		})
		.ok_or_else(|| OpError::refused(Kind::BadRequest, format!("{k} is required (a number)")))
}

/// The whole catalog, from one snapshot.
pub async fn load_catalog(h: &mut Pg) -> Result<Catalog, OpError> {
	h.simple("begin isolation level repeatable read read only")
		.await?;
	let r = async {
		let epoch = h
			.simple(catalog::LOAD_EPOCH)
			.await?
			.first()
			.and_then(|r| r.first().cloned().flatten())
			.and_then(|v| v.parse().ok())
			.ok_or_else(|| OpError::new("the catalog has no epoch"))?;
		let nodes = h.simple(catalog::LOAD_NODES).await?;
		let keyspaces = h.simple(catalog::LOAD_KEYSPACES).await?;
		let ranges = h.simple(catalog::LOAD_RANGES).await?;
		let pins = h.simple(catalog::LOAD_PINS).await?;
		let relations = h.simple(catalog::LOAD_RELATIONS).await?;
		Catalog::from_rows(epoch, &nodes, &keyspaces, &ranges, &pins, &relations)
			.map_err(|e| OpError::new(format!("the catalog is not usable: {e}")))
	}
	.await;
	let _ = h.simple("rollback").await;
	r
}

/// Writes what differs between `before` and `after` (keyspaces, ranges, pins, relations, node
/// states), runs `extra` in the same transaction, bumps the epoch, and returns the new epoch.
/// Refuses if the catalog moved since `before` was read, so two writers never interleave.
pub async fn write_catalog(
	h: &mut Pg,
	before: &Catalog,
	after: &Catalog,
	extra: &str,
) -> Result<i64, OpError> {
	after.validate().map_err(|e| {
		OpError::refused(
			Kind::CatalogInvalid,
			format!("the change would break the catalog: {e}"),
		)
	})?;
	let mut sql = String::from("begin;\n");
	sql.push_str(&format!(
		"do $$ declare e bigint; begin \
		select epoch into e from lepis.cluster where id = 1 for update; \
		if e <> {} then \
		raise exception 'the catalog moved while this change was planned' using errcode = '40001'; \
		end if; end $$;\n",
		before.epoch
	));
	for (id, n) in &after.nodes {
		if before.nodes.get(id).map(|b| b.state) != Some(n.state) {
			sql.push_str(&format!(
				"update lepis.node set state = '{}' where id = {};\n",
				state_name(n.state),
				id.0
			));
		}
	}
	let mut names: Vec<&String> = after.keyspaces.keys().collect();
	names.sort();
	for name in names {
		let k = &after.keyspaces[name];
		let old = before.keyspaces.get(name);
		let n = quote_literal(name);
		if old.is_none() {
			sql.push_str(&format!(
				"insert into lepis.keyspace (name, strategy, key_type, seed) values ({n}, 'hash', {}, {});\n",
				quote_literal(k.key_type.sql_name()),
				k.seed as i64
			));
		}
		if old.map(|o| &o.ranges) != Some(&k.ranges) {
			sql.push_str(&format!("delete from lepis.range where keyspace = {n};\n"));
			for r in &k.ranges {
				sql.push_str(&format!(
					"insert into lepis.range (keyspace, lo, hi, node_id) values ({n}, {}, {}, {});\n",
					r.lo, r.hi, r.node.0
				));
			}
		}
		if old.map(|o| &o.pins) != Some(&k.pins) {
			sql.push_str(&format!("delete from lepis.pin where keyspace = {n};\n"));
			let mut pins: Vec<_> = k.pins.iter().collect();
			pins.sort();
			for (v, node) in pins {
				sql.push_str(&format!(
					"insert into lepis.pin (keyspace, value, node_id) values ({n}, {}, {});\n",
					quote_literal(v),
					node.0
				));
			}
		}
	}
	for (name, kind) in &after.relations {
		if before.relations.get(name) != Some(kind) {
			let (k, ks, col) = kind_parts(kind);
			let opt = |v: Option<String>| v.map_or("null".to_string(), |v| quote_literal(&v));
			sql.push_str(&format!(
				"insert into lepis.relation (schema_name, table_name, kind, keyspace, key_column) \
				values ({}, {}, '{k}', {}, {}) on conflict (schema_name, table_name) do update \
				set kind = excluded.kind, keyspace = excluded.keyspace, key_column = excluded.key_column;\n",
				quote_literal(&name.schema),
				quote_literal(&name.table),
				opt(ks),
				opt(col)
			));
		}
	}
	sql.push_str(extra);
	sql.push_str("\nselect lepis.bump();\ncommit;");
	match h.simple(&sql).await {
		Ok(rows) => rows
			.last()
			.and_then(|r| r.first().cloned().flatten())
			.and_then(|v| v.parse().ok())
			.ok_or_else(|| OpError::new("lepis.bump() returned nothing")),
		Err(e) => {
			let _ = h.simple("rollback").await;
			Err(e)
		}
	}
}

/// Loads the catalog, applies `change`, writes it. Returns the new epoch, or the current one
/// when the change was already there.
pub async fn change_catalog(h: &mut Pg, change: &Change, extra: &str) -> Result<i64, OpError> {
	let before = load_catalog(h).await?;
	let mut after = before.clone();
	change.apply(&mut after)?;
	if same(&before, &after) && extra.is_empty() {
		return Ok(before.epoch);
	}
	write_catalog(h, &before, &after, extra).await
}

/// Whether two catalogs say the same thing (the epoch aside).
pub fn same(a: &Catalog, b: &Catalog) -> bool {
	a.nodes == b.nodes && a.keyspaces == b.keyspaces && a.relations == b.relations
}

/// A random hex token.
pub fn token() -> String {
	let mut b = [0u8; 8];
	getrandom::fill(&mut b).expect("the operating system has randomness");
	b.iter().map(|x| format!("{x:02x}")).collect()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::Node;
	use crate::config::SslMode;

	pub(crate) fn cat(n: i32, ranges: usize) -> Catalog {
		let ids: Vec<NodeId> = (1..=n).map(NodeId).collect();
		let mut c = Catalog {
			epoch: 7,
			..Default::default()
		};
		for id in &ids {
			c.nodes.insert(
				*id,
				Node {
					id: *id,
					name: format!("n{}", id.0),
					host: format!("h{}", id.0),
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
			"k".into(),
			Keyspace {
				name: "k".into(),
				strategy: Strategy::Hash,
				key_type: KeyType::Int8,
				seed: 1,
				ranges: Keyspace::even_ranges(ranges, &ids),
				pins: Default::default(),
			},
		);
		c
	}

	#[test]
	fn changes_are_idempotent_and_round_trip() {
		let c = cat(2, 2);
		let r = c.keyspaces["k"].ranges[0];
		let at = r.lo + (r.hi - r.lo) / 2 + 1;
		let changes = [
			Change::Split {
				keyspace: "k".into(),
				lo: r.lo,
				at,
			},
			Change::RangeOwner {
				keyspace: "k".into(),
				lo: r.lo,
				hi: r.hi,
				to: NodeId(2),
			},
			Change::Pin {
				keyspace: "k".into(),
				value: "42".into(),
				node: NodeId(2),
			},
			Change::Relation {
				name: relation_name("app.t").unwrap(),
				kind: RelationKind::Reference,
			},
		];
		for ch in changes {
			assert_eq!(Change::from_json(&ch.to_json()).unwrap(), ch);
			let mut once = c.clone();
			ch.apply(&mut once).unwrap();
			let mut twice = once.clone();
			ch.apply(&mut twice).unwrap();
			assert!(same(&once, &twice), "{ch:?}");
			once.validate().unwrap();
		}
	}

	#[test]
	fn split_then_merge_is_where_it_started() {
		let c = cat(2, 2);
		let r = c.keyspaces["k"].ranges[1];
		let at = r.lo + 1000;
		let mut x = c.clone();
		Change::Split {
			keyspace: "k".into(),
			lo: r.lo,
			at,
		}
		.apply(&mut x)
		.unwrap();
		assert_eq!(x.keyspaces["k"].ranges.len(), 3);
		x.validate().unwrap();
		Change::Merge {
			keyspace: "k".into(),
			a: r.lo,
			b: at,
		}
		.apply(&mut x)
		.unwrap();
		assert!(same(&x, &c));
		// Ranges on different nodes are not merged by metadata.
		let r0 = c.keyspaces["k"].ranges[0];
		assert!(
			Change::Merge {
				keyspace: "k".into(),
				a: r0.lo,
				b: r.lo
			}
			.apply(&mut x.clone())
			.is_err()
		);
	}

	#[test]
	fn settings_take_the_job_over_the_cluster() {
		let s = Settings::from_json(
			&json!({"max_write_pause_ms": 3000, "drain_timeout_ms": 100}),
			&json!({"max_write_pause_ms": 500}),
		);
		assert_eq!(s.max_write_pause, Duration::from_millis(500));
		assert_eq!(s.drain_timeout, Duration::from_millis(100));
		assert_eq!(s.ack_timeout, Settings::default().ack_timeout);
	}
}
