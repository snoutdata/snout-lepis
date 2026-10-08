//! `plan`: an operation turned into the steps a job will run, against the catalog as it is now,
//! with what will move, how much, how long the copy should take and what pause to expect. A plan
//! runs nothing; `run` writes the same plan as a job.
//!
//! How much is read from each source node's own statistics (`pg_total_relation_size`,
//! `reltuples`) scaled by the share of the hash space that moves, which is exact in expectation
//! because the hash is uniform. The copy time assumes `copy_mb_per_s` (chosen, 50 MB/s by
//! default); the pause is an estimate, and the job records the one it measured.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use serde_json::{Value, json};

use super::Kind;
use super::spec::{NodeSpec, Op, Step, TransferSpec};
use super::steps::{check_node, connect_node, tables_of};
use super::{Change, OpError, Pg, Settings, Target, load_catalog, qualified, schema};
use crate::catalog::{Catalog, Keyspace, NodeId, NodeState, Range, RelationKind, RelationName};
use crate::hash::KeyType;
use crate::server::App;

#[derive(Debug)]
pub struct Planned {
	pub steps: Vec<Step>,
	pub summary: Value,
}

fn width(r: &Range) -> u128 {
	(r.hi as i128 - r.lo as i128 + 1) as u128
}

/// Nodes that can take rows: active, not being drained.
fn active(c: &Catalog) -> Vec<NodeId> {
	c.nodes
		.values()
		.filter(|n| n.state == NodeState::Active)
		.map(|n| n.id)
		.collect()
}

fn ks<'a>(c: &'a Catalog, name: &str) -> Result<&'a Keyspace, OpError> {
	c.keyspaces.get(name).ok_or_else(|| {
		OpError::refused(Kind::NoSuchKeyspace, format!("there is no keyspace {name}"))
	})
}

/// Moves that even out a keyspace's share of the hash space over `members`, splitting a range
/// when none fits. Ranges on nodes that are not members (being drained) all move. Returns the
/// splits, in order, as (lo, at), and the final layout.
pub fn rebalance(ks: &Keyspace, members: &[NodeId]) -> (Vec<(i64, i64)>, Vec<Range>) {
	let mut ranges = ks.ranges.clone();
	let mut splits = Vec::new();
	if members.is_empty() {
		return (splits, ranges);
	}
	let total: u128 = ranges.iter().map(width).sum();
	let fair = total / members.len() as u128;
	let tolerance = (fair / 20).max(1);
	for _ in 0..512 {
		let mut load: BTreeMap<NodeId, u128> = members.iter().map(|n| (*n, 0)).collect();
		let mut outsider = None;
		for r in &ranges {
			match load.get_mut(&r.node) {
				Some(l) => *l += width(r),
				None => outsider = Some(r.node),
			}
		}
		let (&b, &lb) = load
			.iter()
			.min_by_key(|(n, l)| (**l, **n))
			.expect("members");
		if let Some(a) = outsider {
			let i = ranges
				.iter()
				.position(|r| r.node == a)
				.expect("outsider range");
			ranges[i].node = b;
			continue;
		}
		let (&a, &la) = load
			.iter()
			.max_by_key(|(n, l)| (**l, std::cmp::Reverse(**n)))
			.expect("members");
		let diff = la - lb;
		if diff <= tolerance {
			break;
		}
		let want = diff / 2;
		// The range on `a` whose width is nearest `want` without reaching `diff`.
		let pick = ranges
			.iter()
			.enumerate()
			.filter(|(_, r)| r.node == a && width(r) < diff)
			.min_by_key(|(_, r)| width(r).abs_diff(want))
			.map(|(i, _)| i);
		match pick {
			Some(i) => ranges[i].node = b,
			None => {
				let Some(i) = ranges
					.iter()
					.enumerate()
					.filter(|(_, r)| r.node == a && width(r) > want)
					.min_by_key(|(_, r)| width(r))
					.map(|(i, _)| i)
				else {
					break;
				};
				let r = ranges[i];
				let at = (r.lo as i128 + want as i128) as i64;
				splits.push((r.lo, at));
				ranges[i].hi = at - 1;
				ranges[i].node = b;
				ranges.insert(
					i + 1,
					Range {
						lo: at,
						hi: r.hi,
						node: r.node,
					},
				);
			}
		}
	}
	(splits, ranges)
}

/// The steps that take a keyspace from its layout to `ranges` (after `splits`): the splits, then
/// one transfer per (source, target) pair carrying every range between them.
fn moves_to(
	c: &Catalog,
	name: &str,
	splits: &[(i64, i64)],
	ranges: &[Range],
) -> Result<Vec<Step>, OpError> {
	let k = ks(c, name)?;
	let mut steps: Vec<Step> = splits
		.iter()
		.map(|(lo, at)| {
			Step::Catalog(Change::Split {
				keyspace: name.into(),
				lo: *lo,
				at: *at,
			})
		})
		.collect();
	let mut by_pair: BTreeMap<(NodeId, NodeId), Vec<Change>> = BTreeMap::new();
	for r in ranges {
		let from = k.owner_of_hash(r.lo);
		if from != r.node {
			by_pair
				.entry((from, r.node))
				.or_default()
				.push(Change::RangeOwner {
					keyspace: name.into(),
					lo: r.lo,
					hi: r.hi,
					to: r.node,
				});
		}
	}
	let tables = tables_of(c, name);
	for ((from, to), changes) in by_pair {
		steps.push(Step::Transfer(TransferSpec {
			source: from,
			targets: vec![to],
			tables: tables.clone(),
			change: if changes.len() == 1 {
				changes.into_iter().next().expect("one")
			} else {
				Change::Many(changes)
			},
		}));
	}
	Ok(steps)
}

fn least_loaded(k: &Keyspace, candidates: &[NodeId], not: NodeId) -> Option<NodeId> {
	let mut load: HashMap<NodeId, u128> = candidates.iter().map(|n| (*n, 0)).collect();
	for r in &k.ranges {
		if let Some(l) = load.get_mut(&r.node) {
			*l += width(r);
		}
	}
	candidates
		.iter()
		.copied()
		.filter(|n| *n != not)
		.min_by_key(|n| (load[n], *n))
}

fn random_seed() -> u64 {
	let mut b = [0u8; 4];
	getrandom::fill(&mut b).expect("the operating system has randomness");
	u32::from_le_bytes(b) as u64
}

/// Expands `op` against the catalog `c`.
pub async fn expand(app: &Arc<App>, c: &Catalog, op: &Op) -> Result<Vec<Step>, OpError> {
	let home = c
		.home()
		.map(|n| n.id)
		.ok_or_else(|| OpError::refused(Kind::CatalogInvalid, "the catalog has no home node"))?;
	Ok(match op {
		Op::NodeAdd(spec) => {
			node_add(
				app,
				c,
				spec,
				c.nodes.keys().map(|n| n.0).max().unwrap_or(0) + 1,
			)
			.await?
		}
		Op::NodeDrain { node, to } => {
			let node = node.resolve(c)?;
			let mut targets: Vec<NodeId> = if to.is_empty() {
				active(c)
			} else {
				to.iter().map(|t| t.resolve(c)).collect::<Result<_, _>>()?
			};
			targets.retain(|n| *n != node);
			if targets.is_empty() {
				return Err(OpError::refused(
					Kind::NoTargetNode,
					format!("there is no other active node to drain {node} to"),
				));
			}
			let mut steps = vec![Step::Catalog(Change::NodeState {
				node,
				state: NodeState::Draining,
			})];
			steps.extend(drain_moves(c, node, &targets)?);
			steps
		}
		Op::NodeRemove { node } => {
			let node = node.resolve(c)?;
			if node == home {
				return Err(OpError::refused(
					Kind::HomeNode,
					"the home node holds the catalog and cannot be removed",
				));
			}
			for k in c.keyspaces.values() {
				if k.ranges.iter().any(|r| r.node == node) || k.pins.values().any(|n| *n == node) {
					return Err(OpError::refused(
						Kind::NodeNotEmpty,
						format!(
							"{node} still owns rows of keyspace {}; drain it first",
							k.name
						),
					));
				}
			}
			vec![Step::Catalog(Change::NodeState {
				node,
				state: NodeState::Removed,
			})]
		}
		Op::KeyspaceCreate {
			name,
			key_type,
			seed,
			ranges,
			nodes,
		} => {
			let kt = KeyType::from_sql_name(key_type).ok_or_else(|| {
				OpError::refused(
					Kind::BadKeyType,
					format!(
						"key type {key_type}: one of {}",
						KeyType::ALL.map(|k| k.sql_name()).join(", ")
					),
				)
			})?;
			let owners: Vec<NodeId> = if nodes.is_empty() {
				active(c)
			} else {
				nodes
					.iter()
					.map(|n| n.resolve(c))
					.collect::<Result<_, _>>()?
			};
			if owners.is_empty() {
				return Err(OpError::refused(
					Kind::NoTargetNode,
					"no active node to own the ranges",
				));
			}
			let n = ranges.unwrap_or(owners.len()).max(1);
			vec![Step::Catalog(Change::Keyspace {
				name: name.clone(),
				key_type: kt,
				seed: seed.unwrap_or_else(random_seed),
				ranges: Keyspace::even_ranges(n, &owners),
			})]
		}
		Op::TableDistribute {
			table,
			column,
			keyspace,
		} => {
			if matches!(
				c.relations.get(table),
				Some(RelationKind::Sharded { .. } | RelationKind::Reference)
			) {
				return Err(OpError::refused(
					Kind::AlreadyDistributed,
					format!("{table} is already distributed; it must be global to be distributed"),
				));
			}
			let k = ks(c, keyspace)?;
			let mut h = connect_node(app, c, home).await?;
			schema::check_movable(&mut h, table, Some((column, k)), c).await?;
			let change = Change::Relation {
				name: table.clone(),
				kind: RelationKind::Sharded {
					keyspace: keyspace.clone(),
					key_column: column.clone(),
				},
			};
			let targets: Vec<NodeId> = c
				.nodes_of(keyspace)
				.into_iter()
				.filter(|n| *n != home)
				.collect();
			if targets.is_empty() {
				vec![
					Step::Catalog(change),
					Step::Fences {
						keyspace: keyspace.clone(),
					},
				]
			} else {
				vec![Step::Transfer(TransferSpec {
					source: home,
					targets,
					tables: vec![table.clone()],
					change,
				})]
			}
		}
		Op::TableReference { table } => {
			if c.relations
				.get(table)
				.is_some_and(|k| *k != RelationKind::Global)
			{
				return Err(OpError::refused(
					Kind::NotGlobal,
					format!("{table} is not a global table"),
				));
			}
			let mut h = connect_node(app, c, home).await?;
			schema::check_movable(&mut h, table, None, c).await?;
			let change = Change::Relation {
				name: table.clone(),
				kind: RelationKind::Reference,
			};
			let targets: Vec<NodeId> = c
				.nodes
				.values()
				.filter(|n| !n.home && n.state != NodeState::Removed)
				.map(|n| n.id)
				.collect();
			if targets.is_empty() {
				vec![Step::Catalog(change)]
			} else {
				vec![Step::Transfer(TransferSpec {
					source: home,
					targets,
					tables: vec![table.clone()],
					change,
				})]
			}
		}
		Op::TableGlobal { table } => table_global(c, table, home)?,
		Op::RangeSplit {
			keyspace,
			range,
			at,
			to,
		} => {
			let k = ks(c, keyspace)?;
			let r = *k.ranges.iter().find(|r| r.lo == *range).ok_or_else(|| {
				OpError::refused(
					Kind::NoSuchRange,
					format!("keyspace {keyspace} has no range starting at {range}"),
				)
			})?;
			if r.lo == r.hi {
				return Err(OpError::refused(
					Kind::BadBound,
					"a range of one value cannot be split",
				));
			}
			let at = at.unwrap_or((r.lo as i128 + (r.hi as i128 - r.lo as i128) / 2 + 1) as i64);
			if at <= r.lo || at > r.hi {
				return Err(OpError::refused(
					Kind::BadBound,
					format!(
						"{at} is not inside the range [{}, {}] (the upper half starts at it)",
						r.lo, r.hi
					),
				));
			}
			let target = match to {
				Some(t) => Some(t.resolve(c)?),
				None => least_loaded(k, &active(c), r.node),
			};
			let mut steps = vec![Step::Catalog(Change::Split {
				keyspace: keyspace.clone(),
				lo: r.lo,
				at,
			})];
			if let Some(t) = target.filter(|t| *t != r.node) {
				steps.push(Step::Transfer(TransferSpec {
					source: r.node,
					targets: vec![t],
					tables: tables_of(c, keyspace),
					change: Change::RangeOwner {
						keyspace: keyspace.clone(),
						lo: at,
						hi: r.hi,
						to: t,
					},
				}));
			}
			steps
		}
		Op::RangeMerge { keyspace, a, b } => {
			let k = ks(c, keyspace)?;
			let (a, b) = if a < b { (*a, *b) } else { (*b, *a) };
			let ra = *k.ranges.iter().find(|r| r.lo == a).ok_or_else(|| {
				OpError::refused(
					Kind::NoSuchRange,
					format!("keyspace {keyspace} has no range starting at {a}"),
				)
			})?;
			let rb = *k.ranges.iter().find(|r| r.lo == b).ok_or_else(|| {
				OpError::refused(
					Kind::NoSuchRange,
					format!("keyspace {keyspace} has no range starting at {b}"),
				)
			})?;
			if ra.hi.checked_add(1) != Some(rb.lo) {
				return Err(OpError::refused(
					Kind::NotAdjacent,
					format!("ranges {a} and {b} are not adjacent"),
				));
			}
			let mut steps = Vec::new();
			if ra.node != rb.node {
				steps.push(Step::Transfer(TransferSpec {
					source: rb.node,
					targets: vec![ra.node],
					tables: tables_of(c, keyspace),
					change: Change::RangeOwner {
						keyspace: keyspace.clone(),
						lo: rb.lo,
						hi: rb.hi,
						to: ra.node,
					},
				}));
			}
			steps.push(Step::Catalog(Change::Merge {
				keyspace: keyspace.clone(),
				a,
				b,
			}));
			steps
		}
		Op::RangeMove {
			keyspace,
			range,
			to,
		} => {
			let k = ks(c, keyspace)?;
			let r = *k.ranges.iter().find(|r| r.lo == *range).ok_or_else(|| {
				OpError::refused(
					Kind::NoSuchRange,
					format!("keyspace {keyspace} has no range starting at {range}"),
				)
			})?;
			let to = to.resolve(c)?;
			if to == r.node {
				return Err(OpError::refused(
					Kind::AlreadyDone,
					format!("{to} already owns that range"),
				));
			}
			vec![Step::Transfer(TransferSpec {
				source: r.node,
				targets: vec![to],
				tables: tables_of(c, keyspace),
				change: Change::RangeOwner {
					keyspace: keyspace.clone(),
					lo: r.lo,
					hi: r.hi,
					to,
				},
			})]
		}
		Op::TenantPin {
			keyspace,
			value,
			node,
		} => {
			let k = ks(c, keyspace)?;
			let owner = c
				.owner(keyspace, value)
				.map_err(|e| OpError::refused(Kind::BadKey, e.0))?;
			let mut steps = Vec::new();
			if !k.pins.contains_key(value) {
				steps.push(Step::Catalog(Change::Pin {
					keyspace: keyspace.clone(),
					value: value.clone(),
					node: owner,
				}));
				steps.push(Step::Fences {
					keyspace: keyspace.clone(),
				});
			}
			if let Some(n) = node {
				let n = n.resolve(c)?;
				if n != owner {
					steps.push(Step::Transfer(TransferSpec {
						source: owner,
						targets: vec![n],
						tables: tables_of(c, keyspace),
						change: Change::PinOwner {
							keyspace: keyspace.clone(),
							value: value.clone(),
							to: n,
						},
					}));
				}
			}
			if steps.is_empty() {
				return Err(OpError::refused(
					Kind::AlreadyDone,
					format!("{value} is already pinned there"),
				));
			}
			steps
		}
		Op::Rebalance { keyspace } => {
			let members = active(c);
			let mut names: Vec<&String> = c.keyspaces.keys().collect();
			names.sort();
			let mut steps = Vec::new();
			for name in names {
				if keyspace.as_ref().is_some_and(|k| k != name) {
					continue;
				}
				let (splits, ranges) = rebalance(&c.keyspaces[name], &members);
				steps.extend(moves_to(c, name, &splits, &ranges)?);
			}
			steps
		}
		Op::Scale { add, remove } => scale(app, c, add, *remove, home).await?,
		Op::Verify { keyspace } => vec![Step::Verify {
			keyspace: keyspace.clone(),
		}],
		Op::Cleanup { node } => vec![Step::Cleanup {
			node: node.as_ref().map(|n| n.resolve(c)).transpose()?,
		}],
		Op::NodeAttach { spec, standby_of } => {
			let standby_of = standby_of.resolve(c)?;
			super::attach::precheck(app, c, spec, standby_of).await?;
			vec![Step::NodeAttach {
				id: NodeId(c.nodes.keys().map(|n| n.0).max().unwrap_or(0) + 1),
				spec: spec.clone(),
				standby_of,
			}]
		}
		Op::RestorePoint { name } => {
			let name = name
				.clone()
				.unwrap_or_else(super::restore_point::default_name);
			super::restore_point::check_name(&name)?;
			vec![Step::RestorePoint { name }]
		}
	})
}

async fn node_add(
	app: &Arc<App>,
	c: &Catalog,
	spec: &NodeSpec,
	id: i32,
) -> Result<Vec<Step>, OpError> {
	if c.nodes.values().any(|n| n.name == spec.name) {
		return Err(OpError::refused(
			Kind::NodeNameTaken,
			format!("a node named {} is already in the cluster", spec.name),
		));
	}
	// Refused now, with the setting to change, rather than when the job reaches it.
	let t = Target {
		label: spec.name.clone(),
		address: crate::config::NodeAddress {
			host: spec.host.clone(),
			port: spec.port,
			sslmode: spec.sslmode,
			ca_file: app.config.home.ca_file.clone(),
		},
		dbname: spec.dbname.clone(),
	};
	let mut pg = Pg::connect(app, &t).await?;
	check_node(&mut pg).await?;
	let id = NodeId(id);
	let mut steps = vec![
		Step::NodeCheck(spec.clone()),
		Step::NodeInsert {
			id,
			spec: spec.clone(),
		},
		Step::RolesSync { node: id },
	];
	let mut sharded: Vec<&RelationName> = c
		.relations
		.iter()
		.filter(|(_, k)| matches!(k, RelationKind::Sharded { .. }))
		.map(|(r, _)| r)
		.collect();
	sharded.sort();
	for r in sharded {
		steps.push(Step::TableCreate {
			node: id,
			table: r.clone(),
		});
	}
	let mut reference: Vec<RelationName> = c
		.relations
		.iter()
		.filter(|(_, k)| **k == RelationKind::Reference)
		.map(|(r, _)| r.clone())
		.collect();
	reference.sort();
	if !reference.is_empty() {
		steps.push(Step::Transfer(TransferSpec {
			source: c.home().map(|n| n.id).expect("home"),
			targets: vec![id],
			tables: reference,
			change: Change::None,
		}));
	}
	steps.push(Step::Catalog(Change::NodeState {
		node: id,
		state: NodeState::Active,
	}));
	Ok(steps)
}

/// Every range and pin of `node`, moved to the least loaded of `targets`.
fn drain_moves(c: &Catalog, node: NodeId, targets: &[NodeId]) -> Result<Vec<Step>, OpError> {
	let mut steps = Vec::new();
	let mut names: Vec<&String> = c.keyspaces.keys().collect();
	names.sort();
	for name in names {
		let k = &c.keyspaces[name];
		// Only what leaves `node` moves, each range to the least loaded target: a drain is not
		// a rebalance of everyone else.
		let mut load: BTreeMap<NodeId, u128> = targets.iter().map(|n| (*n, 0)).collect();
		for r in &k.ranges {
			if let Some(l) = load.get_mut(&r.node) {
				*l += width(r);
			}
		}
		let mut final_ranges = k.ranges.clone();
		for r in final_ranges.iter_mut().filter(|r| r.node == node) {
			let (&t, _) = load
				.iter()
				.min_by_key(|(n, l)| (**l, **n))
				.expect("targets");
			r.node = t;
			*load.get_mut(&t).expect("target") += width(r);
		}
		steps.extend(moves_to(c, name, &[], &final_ranges)?);
		let mut pins: Vec<(&String, &NodeId)> =
			k.pins.iter().filter(|(_, n)| **n == node).collect();
		pins.sort();
		for (value, _) in pins {
			let h = k
				.key_type
				.hash_text_value(value, k.seed)
				.map_err(|e| OpError::refused(Kind::BadKey, e.to_string()))?;
			let to = final_ranges
				.iter()
				.find(|r| r.lo <= h && h <= r.hi)
				.map_or(targets[0], |r| r.node);
			steps.push(Step::Transfer(TransferSpec {
				source: node,
				targets: vec![to],
				tables: tables_of(c, name),
				change: Change::PinOwner {
					keyspace: name.clone(),
					value: value.clone(),
					to,
				},
			}));
		}
	}
	Ok(steps)
}

fn table_global(c: &Catalog, table: &RelationName, home: NodeId) -> Result<Vec<Step>, OpError> {
	let change = Step::Catalog(Change::Relation {
		name: table.clone(),
		kind: RelationKind::Global,
	});
	match c.relations.get(table) {
		Some(RelationKind::Reference) => {
			let mut steps = vec![change];
			for n in c
				.nodes
				.values()
				.filter(|n| !n.home && n.state != NodeState::Removed)
			{
				steps.push(Step::TableDrop {
					node: n.id,
					table: table.clone(),
				});
			}
			Ok(steps)
		}
		Some(RelationKind::Sharded { keyspace, .. }) => {
			let others: Vec<RelationName> = tables_of(c, keyspace)
				.into_iter()
				.filter(|t| t != table)
				.collect();
			if !others.is_empty() {
				return Err(OpError::refused(
					Kind::TableNotShardable,
					format!(
						"{table} shares keyspace {keyspace} with {}; its rows cannot go home alone",
						others
							.iter()
							.map(|t| t.to_string())
							.collect::<Vec<_>>()
							.join(", ")
					),
				));
			}
			let k = ks(c, keyspace)?;
			let all_home: Vec<Range> = k
				.ranges
				.iter()
				.map(|r| Range { node: home, ..*r })
				.collect();
			let mut steps = moves_to(c, keyspace, &[], &all_home)?;
			let mut pins: Vec<&String> = k
				.pins
				.iter()
				.filter(|(_, n)| **n != home)
				.map(|(v, _)| v)
				.collect();
			pins.sort();
			for v in pins {
				steps.push(Step::Transfer(TransferSpec {
					source: k.pins[v],
					targets: vec![home],
					tables: vec![table.clone()],
					change: Change::PinOwner {
						keyspace: keyspace.clone(),
						value: v.clone(),
						to: home,
					},
				}));
			}
			steps.push(change);
			Ok(steps)
		}
		_ => Err(OpError::refused(
			Kind::AlreadyDone,
			format!("{table} is already global"),
		)),
	}
}

async fn scale(
	app: &Arc<App>,
	c: &Catalog,
	add: &[NodeSpec],
	remove: usize,
	home: NodeId,
) -> Result<Vec<Step>, OpError> {
	if !add.is_empty() && remove > 0 {
		return Err(OpError::refused(
			Kind::BadRequest,
			"scale either adds nodes or removes them, not both at once",
		));
	}
	let mut steps = Vec::new();
	if !add.is_empty() {
		let first = c.nodes.keys().map(|n| n.0).max().unwrap_or(0) + 1;
		let mut after = c.clone();
		for (next, spec) in (first..).zip(add) {
			steps.extend(node_add(app, c, spec, next).await?);
			after.nodes.insert(
				NodeId(next),
				crate::catalog::Node {
					id: NodeId(next),
					name: spec.name.clone(),
					host: spec.host.clone(),
					port: spec.port,
					dbname: spec.dbname.clone(),
					sslmode: spec.sslmode,
					home: false,
					state: NodeState::Active,
					server_version_num: None,
				},
			);
		}
		let members = active(&after);
		let mut names: Vec<&String> = c.keyspaces.keys().collect();
		names.sort();
		for name in names {
			let (splits, ranges) = rebalance(&c.keyspaces[name], &members);
			steps.extend(moves_to(c, name, &splits, &ranges)?);
		}
		return Ok(steps);
	}
	// Down: the emptiest data nodes are drained and removed.
	let mut load: Vec<(u128, NodeId)> = active(c)
		.into_iter()
		.filter(|n| *n != home)
		.map(|n| {
			let w: u128 = c
				.keyspaces
				.values()
				.flat_map(|k| k.ranges.iter())
				.filter(|r| r.node == n)
				.map(width)
				.sum();
			(w, n)
		})
		.collect();
	load.sort();
	if remove > load.len() {
		return Err(OpError::refused(
			Kind::NoTargetNode,
			format!(
				"only {} data nodes can be removed (the home node stays)",
				load.len()
			),
		));
	}
	let going: Vec<NodeId> = load.iter().take(remove).map(|(_, n)| *n).collect();
	let staying: Vec<NodeId> = active(c)
		.into_iter()
		.filter(|n| !going.contains(n))
		.collect();
	let mut layout = c.clone();
	for &n in &going {
		steps.push(Step::Catalog(Change::NodeState {
			node: n,
			state: NodeState::Draining,
		}));
		steps.extend(drain_moves(&layout, n, &staying)?);
		// Later drains plan against the layout this one leaves.
		for st in &steps {
			if let Step::Transfer(t) = st {
				let _ = t.change.apply(&mut layout);
			} else if let Step::Catalog(ch) = st {
				let _ = ch.apply(&mut layout);
			}
		}
	}
	for n in going {
		steps.push(Step::Catalog(Change::NodeState {
			node: n,
			state: NodeState::Removed,
		}));
	}
	Ok(steps)
}

/// The plan's numbers for the steps: per move, how many rows and bytes, the copy time at the
/// assumed rate, and the pause expected at its cutover.
pub async fn summarize(
	app: &Arc<App>,
	c: &Catalog,
	op: &Op,
	steps: &[Step],
	settings: Settings,
) -> Result<Value, OpError> {
	let mut moves = Vec::new();
	let (mut rows_total, mut bytes_total, mut seconds_total) = (0f64, 0f64, 0f64);
	let mut layout = c.clone();
	let mut warnings: Vec<String> = Vec::new();
	let mut sizes: HashMap<(NodeId, RelationName), (f64, f64)> = HashMap::new();
	for st in steps {
		match st {
			Step::Transfer(t) => {
				let mut after = layout.clone();
				t.change.apply(&mut after)?;
				let mut rows = 0f64;
				let mut bytes = 0f64;
				for rel in &t.tables {
					let key = (t.source, rel.clone());
					if !sizes.contains_key(&key) {
						let mut pg = connect_node(app, &layout, t.source).await?;
						let r = pg
							.query(
								"select coalesce(pg_total_relation_size(to_regclass($1)), 0), \
								coalesce((select reltuples from pg_class where oid = to_regclass($1)), 0)",
								&[&qualified(rel)],
							)
							.await?;
						let row = r.first().cloned().unwrap_or_default();
						let num = |i: usize| {
							row.get(i)
								.cloned()
								.flatten()
								.and_then(|v| v.parse::<f64>().ok())
								.unwrap_or(0.0)
						};
						let mut tuples = num(1);
						if tuples <= 0.0 && num(0) > 0.0 && num(0) < 1e9 {
							// Never analyzed (-1): count it, when that is cheap.
							tuples = pg
								.value(&format!("select count(*) from {}", qualified(rel)), &[])
								.await?
								.and_then(|v| v.parse().ok())
								.unwrap_or(0.0);
						}
						sizes.insert(key.clone(), (tuples.max(0.0), num(0)));
					}
					let (r, b) = sizes[&key];
					let share = share_moving(&layout, &after, rel, t.source, &t.targets);
					let pinned = match (&t.change, layout.relations.get(rel)) {
						(
							Change::PinOwner { value, .. },
							Some(RelationKind::Sharded { key_column, .. }),
						) => {
							let mut pg = connect_node(app, &layout, t.source).await?;
							pg.value(
								&format!(
									"select count(*) from {} where {} = $1",
									qualified(rel),
									crate::catalog::quote_ident(key_column)
								),
								&[value],
							)
							.await?
							.and_then(|v| v.parse::<f64>().ok())
						}
						_ => None,
					};
					let moving = pinned.unwrap_or(r * share);
					rows += moving;
					bytes += if r > 0.0 { b * moving / r } else { b * share };
				}
				let seconds = bytes / (settings.copy_mb_per_s as f64 * 1e6) + 1.0;
				rows_total += rows;
				bytes_total += bytes;
				seconds_total += seconds;
				moves.push(json!({
					"source": t.source.0,
					"targets": t.targets.iter().map(|n| n.0).collect::<Vec<_>>(),
					"tables": t.tables.iter().map(|r| r.to_string()).collect::<Vec<_>>(),
					"change": t.change.to_json(),
					"estimated_rows": rows.round() as u64,
					"estimated_bytes": bytes.round() as u64,
					"estimated_copy_seconds": (seconds * 10.0).round() / 10.0,
					"expected_pause_ms": expected_pause_ms(t.tables.len()),
				}));
				layout = after;
			}
			Step::Catalog(ch) => {
				let _ = ch.apply(&mut layout);
			}
			_ => {}
		}
	}
	if let Op::TenantPin { .. } = op
		&& !moves.is_empty()
	{
		warnings.push("a keyspace with pins routes binary-format keys only in text format (bind the key as text)".into());
	}
	Ok(json!({
		"op": op.name(),
		"steps": steps.iter().map(|s| json!({"kind": s.kind(), "args": s.args()})).collect::<Vec<_>>(),
		"moves": moves,
		"cutovers": moves.len(),
		"estimated_rows": rows_total.round() as u64,
		"estimated_bytes": bytes_total.round() as u64,
		"estimated_copy_seconds": (seconds_total * 10.0).round() / 10.0,
		"expected_pause_ms": moves.iter().filter_map(|m| m["expected_pause_ms"].as_u64()).max().unwrap_or(0),
		"max_write_pause_ms": settings.max_write_pause.as_millis() as u64,
		"assumptions": format!(
			"sizes from the source's statistics scaled by the share of the hash space that moves; copy at {} MB/s (chosen); pause estimated, and never above max_write_pause_ms: a cutover that would exceed it is postponed and retried",
			settings.copy_mb_per_s
		),
		"warnings": warnings,
	}))
}

/// An estimate, not a measurement: a lock, a marker round trip, one ALTER per table, a catalog
/// write and the routers' acks.
fn expected_pause_ms(tables: usize) -> u64 {
	50 + 15 * tables as u64
}

/// The fraction of the source's rows of `rel` that move to the targets.
fn share_moving(
	before: &Catalog,
	after: &Catalog,
	rel: &RelationName,
	source: NodeId,
	targets: &[NodeId],
) -> f64 {
	let held = |c: &Catalog, n: NodeId| -> Option<u128> {
		match c.relations.get(rel) {
			Some(RelationKind::Sharded { keyspace, .. }) => c
				.keyspaces
				.get(keyspace)
				.map(|k| k.ranges.iter().filter(|r| r.node == n).map(width).sum()),
			_ => None,
		}
	};
	match (held(before, source), held(after, source)) {
		(Some(b), Some(a)) if b > 0 => (b.saturating_sub(a)) as f64 / b as f64,
		(None, Some(a)) => {
			// Distributed from the home node: the share the home no longer owns.
			let total: f64 = 2f64.powi(64);
			1.0 - a as f64 / total
		}
		(None, None) if targets.is_empty() => 0.0,
		_ => 1.0,
	}
}

/// Plans `op` against the catalog as it is now.
pub async fn plan(
	app: &Arc<App>,
	h: &mut Pg,
	op: &Op,
	settings: Settings,
) -> Result<Planned, OpError> {
	let c = load_catalog(h).await?;
	let steps = expand(app, &c, op).await?;
	let summary = summarize(app, &c, op, &steps, settings).await?;
	Ok(Planned { steps, summary })
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ops::tests::cat;

	fn loads(ranges: &[Range]) -> BTreeMap<NodeId, u128> {
		let mut m = BTreeMap::new();
		for r in ranges {
			*m.entry(r.node).or_default() += width(r);
		}
		m
	}

	#[test]
	fn rebalance_spreads_onto_a_new_node() {
		let c = cat(3, 6);
		let mut k = c.keyspaces["k"].clone();
		// Two nodes own everything; node 3 joins empty.
		for r in k.ranges.iter_mut() {
			if r.node == NodeId(3) {
				r.node = NodeId(1);
			}
		}
		let (_, out) = rebalance(&k, &[NodeId(1), NodeId(2), NodeId(3)]);
		let mut check = k.clone();
		check.ranges = out.clone();
		let l = loads(&out);
		let fair = u64::MAX as u128 / 3;
		for (_, v) in l {
			assert!(v.abs_diff(fair) < fair / 10, "{v} vs {fair}");
		}
		// Still covers the space once.
		let mut cat2 = c.clone();
		cat2.keyspaces.insert("k".into(), check);
		cat2.validate().unwrap();
	}

	#[test]
	fn rebalance_splits_when_nothing_fits() {
		let c = cat(2, 1);
		let k = c.keyspaces["k"].clone();
		let (splits, out) = rebalance(&k, &[NodeId(1), NodeId(2)]);
		assert_eq!(splits.len(), 1);
		assert_eq!(out.len(), 2);
		assert_ne!(out[0].node, out[1].node);
	}

	#[test]
	fn drain_moves_only_the_drained_node() {
		let c = cat(3, 6);
		let steps = drain_moves(&c, NodeId(3), &[NodeId(1), NodeId(2)]).unwrap();
		let mut after = c.clone();
		for st in &steps {
			match st {
				Step::Transfer(t) => {
					assert_eq!(t.source, NodeId(3));
					t.change.apply(&mut after).unwrap();
				}
				Step::Catalog(ch) => ch.apply(&mut after).unwrap(),
				_ => {}
			}
		}
		after.validate().unwrap();
		assert!(
			after.keyspaces["k"]
				.ranges
				.iter()
				.all(|r| r.node != NodeId(3))
		);
		let moved: usize = steps
			.iter()
			.filter(|s| matches!(s, Step::Transfer(_)))
			.count();
		assert!((1..=2).contains(&moved), "{steps:?}");
	}
}
