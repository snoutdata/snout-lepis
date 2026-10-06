//! The catalog as the router holds it: one immutable snapshot per epoch (L6). Everything that
//! decides where a row lives reads a snapshot, never the database, so a routing decision is a
//! pure function a test can drive.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use crate::config::SslMode;
use crate::hash::KeyType;

/// The DDL that creates the catalog on the home node; idempotent.
pub const CATALOG_SQL: &str = concat!(
	include_str!("catalog.sql"),
	include_str!("catalog_2pc.sql"),
	include_str!("catalog_jobs.sql"),
	include_str!("catalog_cron.sql"),
);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub i32);

impl fmt::Display for NodeId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "node {}", self.0)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeState {
	Joining,
	Active,
	Draining,
	Removed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Node {
	pub id: NodeId,
	pub name: String,
	pub host: String,
	pub port: u16,
	pub dbname: String,
	pub sslmode: SslMode,
	pub home: bool,
	pub state: NodeState,
	pub server_version_num: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Strategy {
	Hash,
	Range,
	List,
	Schema,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Keyspace {
	pub name: String,
	pub strategy: Strategy,
	pub key_type: KeyType,
	pub seed: u64,
	/// Ownership, sorted by `lo`, covering the whole signed 64-bit space for a hash keyspace.
	pub ranges: Vec<Range>,
	/// Key values given their own node (L12), by the value's text form.
	pub pins: HashMap<String, NodeId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Range {
	pub lo: i64,
	pub hi: i64,
	pub node: NodeId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RelationKind {
	/// Rows placed by `key_column` in `keyspace`.
	Sharded {
		keyspace: String,
		key_column: String,
	},
	/// A full copy on every node.
	Reference,
	/// Only on the home node.
	Global,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelationName {
	pub schema: String,
	pub table: String,
}

impl fmt::Display for RelationName {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}.{}", self.schema, self.table)
	}
}

#[derive(Clone, Debug, Default)]
pub struct Catalog {
	pub epoch: i64,
	pub nodes: BTreeMap<NodeId, Node>,
	pub keyspaces: HashMap<String, Keyspace>,
	pub relations: HashMap<RelationName, RelationKind>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogError(pub String);

impl fmt::Display for CatalogError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(&self.0)
	}
}

impl std::error::Error for CatalogError {}

/// The oldest Postgres a node may run, as `server_version_num` (L1): Lepis supports 17 and 18.
pub const MIN_SERVER_VERSION: u32 = 170_000;

/// The sentence a node below [`MIN_SERVER_VERSION`] is refused with, wherever it would enter the
/// cluster; `None` when its version is fine.
pub fn version_refusal(node: &str, server_version_num: u32) -> Option<String> {
	(server_version_num < MIN_SERVER_VERSION).then(|| {
		format!(
			"{node} runs Postgres {}.{}; Lepis needs Postgres 17 or later on every node (L1)",
			server_version_num / 10_000,
			server_version_num % 10_000
		)
	})
}

fn bad<T>(m: impl Into<String>) -> Result<T, CatalogError> {
	Err(CatalogError(m.into()))
}

impl Catalog {
	pub fn home(&self) -> Option<&Node> {
		self.nodes.values().find(|n| n.home)
	}

	/// The node that owns `value` (in Postgres's text form) of `keyspace`.
	pub fn owner(&self, keyspace: &str, value: &str) -> Result<NodeId, CatalogError> {
		let ks = self
			.keyspaces
			.get(keyspace)
			.ok_or_else(|| CatalogError(format!("no keyspace {keyspace}")))?;
		if !ks.pins.is_empty() {
			let canon = |v: &str| {
				ks.key_type
					.canonical_text(v)
					.map_err(|e| CatalogError(e.to_string()))
			};
			let want = canon(value)?;
			for (pinned, n) in &ks.pins {
				if canon(pinned)? == want {
					return Ok(*n);
				}
			}
		}
		let h = ks
			.key_type
			.hash_text_value(value, ks.seed)
			.map_err(|e| CatalogError(e.to_string()))?;
		Ok(ks.owner_of_hash(h))
	}

	/// The node that owns a key given in binary format. A pin is matched on the value's text
	/// form, so a keyspace with pins needs the text form: binary keys there are refused.
	pub fn owner_binary(&self, keyspace: &str, value: &[u8]) -> Result<NodeId, CatalogError> {
		let ks = self
			.keyspaces
			.get(keyspace)
			.ok_or_else(|| CatalogError(format!("no keyspace {keyspace}")))?;
		if !ks.pins.is_empty() {
			return bad(format!(
				"keyspace {keyspace} has pinned values; bind its key in text format"
			));
		}
		let h = ks
			.key_type
			.hash_binary_value(value, ks.seed)
			.map_err(|e| CatalogError(e.to_string()))?;
		Ok(ks.owner_of_hash(h))
	}

	/// Checks the invariants a router relies on, so a broken catalog is refused at load rather
	/// than discovered as a misplaced row.
	pub fn validate(&self) -> Result<(), CatalogError> {
		if self.nodes.values().filter(|n| n.home).count() != 1 {
			return bad("the catalog must name exactly one home node");
		}
		for n in self.nodes.values() {
			if let Some(m) = n
				.server_version_num
				.and_then(|v| version_refusal(&format!("{} ({})", n.id, n.name), v))
			{
				return bad(m);
			}
		}
		for ks in self.keyspaces.values() {
			if ks.strategy == Strategy::Hash {
				let mut next = i64::MIN;
				let mut done = false;
				for r in &ks.ranges {
					if done || r.lo != next {
						return bad(format!(
							"keyspace {}: hash ranges must cover every value once (gap or overlap at {})",
							ks.name, r.lo
						));
					}
					if r.hi == i64::MAX {
						done = true;
					} else {
						next = r.hi + 1;
					}
				}
				if !done {
					return bad(format!(
						"keyspace {}: hash ranges stop short of the end",
						ks.name
					));
				}
			}
			for r in &ks.ranges {
				self.usable_node(r.node, &ks.name)?;
			}
			for n in ks.pins.values() {
				self.usable_node(*n, &ks.name)?;
			}
		}
		for (name, kind) in &self.relations {
			if let RelationKind::Sharded { keyspace, .. } = kind
				&& !self.keyspaces.contains_key(keyspace)
			{
				return bad(format!("{name} names an unknown keyspace {keyspace}"));
			}
		}
		Ok(())
	}

	fn usable_node(&self, id: NodeId, keyspace: &str) -> Result<(), CatalogError> {
		match self.nodes.get(&id) {
			None => bad(format!("keyspace {keyspace} names an unknown {id}")),
			Some(n) if n.state == NodeState::Removed => {
				bad(format!("keyspace {keyspace} still names removed {id}"))
			}
			Some(_) => Ok(()),
		}
	}

	/// Every node that holds any range or pin of a keyspace (where a scatter goes).
	pub fn nodes_of(&self, keyspace: &str) -> Vec<NodeId> {
		let mut out: Vec<NodeId> = self
			.keyspaces
			.get(keyspace)
			.map(|ks| {
				ks.ranges
					.iter()
					.map(|r| r.node)
					.chain(ks.pins.values().copied())
					.collect()
			})
			.unwrap_or_default();
		out.sort();
		out.dedup();
		out
	}

	/// Builds a catalog from the text rows of the five `LOAD_*` queries.
	pub fn from_rows(
		epoch: i64,
		nodes: &[Vec<Option<String>>],
		keyspaces: &[Vec<Option<String>>],
		ranges: &[Vec<Option<String>>],
		pins: &[Vec<Option<String>>],
		relations: &[Vec<Option<String>>],
	) -> Result<Catalog, CatalogError> {
		let col = |r: &[Option<String>], i: usize| -> Result<String, CatalogError> {
			r.get(i)
				.cloned()
				.flatten()
				.ok_or_else(|| CatalogError(format!("catalog row missing column {i}")))
		};
		let num = |s: String| -> Result<i64, CatalogError> {
			s.parse()
				.map_err(|_| CatalogError(format!("not a number in the catalog: {s}")))
		};
		let mut c = Catalog {
			epoch,
			..Catalog::default()
		};
		for r in nodes {
			let id = NodeId(num(col(r, 0)?)? as i32);
			let sslmode = match col(r, 5)?.as_str() {
				"disable" => SslMode::Disable,
				"require" => SslMode::Require,
				_ => SslMode::VerifyFull,
			};
			let state = match col(r, 7)?.as_str() {
				"joining" => NodeState::Joining,
				"active" => NodeState::Active,
				"draining" => NodeState::Draining,
				_ => NodeState::Removed,
			};
			c.nodes.insert(
				id,
				Node {
					id,
					name: col(r, 1)?,
					host: col(r, 2)?,
					port: u16::try_from(num(col(r, 3)?)?)
						.map_err(|_| CatalogError("bad port".into()))?,
					dbname: col(r, 4)?,
					sslmode,
					home: col(r, 6)? == "home",
					state,
					server_version_num: r.get(8).cloned().flatten().and_then(|v| v.parse().ok()),
				},
			);
		}
		for r in keyspaces {
			let name = col(r, 0)?;
			let strategy = match col(r, 1)?.as_str() {
				"hash" => Strategy::Hash,
				"range" => Strategy::Range,
				"list" => Strategy::List,
				_ => Strategy::Schema,
			};
			let key_type = KeyType::from_sql_name(&col(r, 2)?)
				.ok_or_else(|| CatalogError(format!("keyspace {name}: unsupported key type")))?;
			c.keyspaces.insert(
				name.clone(),
				Keyspace {
					name,
					strategy,
					key_type,
					seed: num(col(r, 3)?)? as u64,
					ranges: Vec::new(),
					pins: HashMap::new(),
				},
			);
		}
		for r in ranges {
			let ks = col(r, 0)?;
			let range = Range {
				lo: num(col(r, 1)?)?,
				hi: num(col(r, 2)?)?,
				node: NodeId(num(col(r, 3)?)? as i32),
			};
			c.keyspaces
				.get_mut(&ks)
				.ok_or_else(|| CatalogError(format!("range for unknown keyspace {ks}")))?
				.ranges
				.push(range);
		}
		for ks in c.keyspaces.values_mut() {
			ks.ranges.sort_by_key(|r| r.lo);
		}
		for r in pins {
			let ks = col(r, 0)?;
			let node = NodeId(num(col(r, 2)?)? as i32);
			c.keyspaces
				.get_mut(&ks)
				.ok_or_else(|| CatalogError(format!("pin for unknown keyspace {ks}")))?
				.pins
				.insert(col(r, 1)?, node);
		}
		for r in relations {
			let name = RelationName {
				schema: col(r, 0)?,
				table: col(r, 1)?,
			};
			let kind = match col(r, 2)?.as_str() {
				"sharded" => RelationKind::Sharded {
					keyspace: col(r, 3)?,
					key_column: col(r, 4)?,
				},
				"reference" => RelationKind::Reference,
				_ => RelationKind::Global,
			};
			c.relations.insert(name, kind);
		}
		c.validate()?;
		Ok(c)
	}
}

pub const LOAD_EPOCH: &str = "select epoch from lepis.cluster where id = 1";
pub const LOAD_NODES: &str =
	"select id, name, host, port, dbname, sslmode, kind, state, server_version_num from lepis.node";
pub const LOAD_KEYSPACES: &str = "select name, strategy, key_type, seed from lepis.keyspace";
pub const LOAD_RANGES: &str = "select keyspace, lo, hi, node_id from lepis.range";
pub const LOAD_PINS: &str = "select keyspace, value, node_id from lepis.pin";
pub const LOAD_RELATIONS: &str =
	"select schema_name, table_name, kind, keyspace, key_column from lepis.relation";

impl Keyspace {
	pub fn owner_of_hash(&self, h: i64) -> NodeId {
		// Ranges are sorted and, once validated, cover the space; the last range whose lo <= h.
		let i = self.ranges.partition_point(|r| r.lo <= h);
		self.ranges[i.saturating_sub(1)].node
	}

	/// The ranges one node owns, for its fence (L7).
	pub fn ranges_of(&self, node: NodeId) -> Vec<(i64, i64)> {
		self.ranges
			.iter()
			.filter(|r| r.node == node)
			.map(|r| (r.lo, r.hi))
			.collect()
	}

	/// `n` equal ranges over the signed 64-bit space, assigned round-robin to `nodes`.
	pub fn even_ranges(n: usize, nodes: &[NodeId]) -> Vec<Range> {
		assert!(n > 0 && !nodes.is_empty());
		let width = (u64::MAX / n as u64) as i128;
		(0..n)
			.map(|i| {
				let lo = i64::MIN as i128 + width * i as i128;
				let hi = if i + 1 == n {
					i64::MAX as i128
				} else {
					lo + width - 1
				};
				Range {
					lo: lo as i64,
					hi: hi as i64,
					node: nodes[i % nodes.len()],
				}
			})
			.collect()
	}
}

/// The fence for one sharded table on one node (L7): a `NOT VALID` CHECK, so adding it is
/// instant and every new row is checked, over the same hash a router computes.
pub fn fence_sql(
	relation: &RelationName,
	key_column: &str,
	ks: &Keyspace,
	node: NodeId,
	server_version_num: u32,
) -> Result<String, CatalogError> {
	let check = owns_check(&quote_ident(key_column), ks, node, server_version_num)?;
	let table = format!(
		"{}.{}",
		quote_ident(&relation.schema),
		quote_ident(&relation.table)
	);
	Ok(format!(
		"alter table {table} drop constraint if exists lepis_owns; \
		alter table {table} add constraint lepis_owns check ({check}) not valid"
	))
}

/// The boolean expression that is true for exactly the keys `node` owns in `ks`: the body of its
/// fence, and the row filter of a move (Phase 4). `column` is any SQL expression for the key,
/// already quoted, such as `"tenant_id"`. Pins are listed in value
/// order, so the same catalog always gives the same text.
pub fn owns_check(
	column: &str,
	ks: &Keyspace,
	node: NodeId,
	server_version_num: u32,
) -> Result<String, CatalogError> {
	let expr = ks
		.key_type
		.sql_expression(column, ks.seed, server_version_num)
		.map_err(|e| CatalogError(e.to_string()))?;
	let owned = ks.ranges_of(node);
	let mut terms: Vec<String> = owned
		.iter()
		.map(|(lo, hi)| format!("{expr} between {lo} and {hi}"))
		.collect();
	let mut pins: Vec<(&String, &NodeId)> = ks.pins.iter().collect();
	pins.sort();
	for (value, n) in &pins {
		if **n == node {
			terms.push(format!("{column} = {}", quote_literal(value)));
		}
	}
	let check = if terms.is_empty() {
		"false".to_string()
	} else {
		terms.join(" or ")
	};
	// Pins owned elsewhere are carved out of this node's ranges.
	let carve: Vec<String> = pins
		.iter()
		.filter(|(_, n)| **n != node)
		.map(|(v, _)| format!("{column} <> {}", quote_literal(v)))
		.collect();
	Ok(if carve.is_empty() {
		check
	} else {
		format!("({check}) and {}", carve.join(" and "))
	})
}

pub fn quote_ident(s: &str) -> String {
	format!("\"{}\"", s.replace('"', "\"\""))
}

pub fn quote_literal(s: &str) -> String {
	format!("'{}'", s.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn node(id: i32, home: bool) -> Node {
		Node {
			id: NodeId(id),
			name: format!("n{id}"),
			host: format!("h{id}"),
			port: 5432,
			dbname: "app".into(),
			sslmode: SslMode::Disable,
			home,
			state: NodeState::Active,
			server_version_num: Some(180_000),
		}
	}

	fn catalog(n: usize) -> Catalog {
		let ids: Vec<NodeId> = (1..=n as i32).map(NodeId).collect();
		let mut c = Catalog {
			epoch: 1,
			..Default::default()
		};
		for (i, id) in ids.iter().enumerate() {
			c.nodes.insert(*id, node(id.0, i == 0));
		}
		c.keyspaces.insert(
			"tenant".into(),
			Keyspace {
				name: "tenant".into(),
				strategy: Strategy::Hash,
				key_type: KeyType::Int8,
				seed: 0x5eed,
				ranges: Keyspace::even_ranges(n * 4, &ids),
				pins: HashMap::new(),
			},
		);
		c
	}

	#[test]
	fn a_node_below_17_is_refused_at_load() {
		let mut c = catalog(3);
		c.validate().unwrap();
		c.nodes.get_mut(&NodeId(2)).unwrap().server_version_num = Some(170_000);
		c.validate().unwrap();
		c.nodes.get_mut(&NodeId(2)).unwrap().server_version_num = None;
		c.validate().unwrap();
		c.nodes.get_mut(&NodeId(2)).unwrap().server_version_num = Some(160_004);
		assert_eq!(
			c.validate().unwrap_err().0,
			"node 2 (n2) runs Postgres 16.4; Lepis needs Postgres 17 or later on every node (L1)"
		);
		assert!(version_refusal("x", 130_022).is_some());
		assert!(version_refusal("x", 169_999).is_some());
		assert!(version_refusal("x", 170_000).is_none());
		assert!(version_refusal("x", 180_001).is_none());
	}

	#[test]
	fn even_ranges_cover_the_space() {
		for n in [1, 2, 3, 7, 64, 1024] {
			let ids = [NodeId(1), NodeId(2)];
			let mut c = catalog(2);
			c.keyspaces.get_mut("tenant").unwrap().ranges = Keyspace::even_ranges(n, &ids);
			c.validate().unwrap();
		}
	}

	#[test]
	fn gaps_and_overlaps_are_refused() {
		let mut c = catalog(2);
		c.keyspaces.get_mut("tenant").unwrap().ranges[1].lo += 1;
		assert!(c.validate().is_err());
		let mut c = catalog(2);
		c.keyspaces.get_mut("tenant").unwrap().ranges.pop();
		assert!(c.validate().is_err());
	}

	#[test]
	fn owners_spread_and_pins_win() {
		let mut c = catalog(4);
		let mut counts: HashMap<NodeId, usize> = HashMap::new();
		for t in 0..100_000 {
			*counts
				.entry(c.owner("tenant", &t.to_string()).unwrap())
				.or_default() += 1;
		}
		assert_eq!(counts.len(), 4);
		for n in counts.values() {
			assert!((20_000..30_000).contains(n), "{counts:?}");
		}
		c.keyspaces
			.get_mut("tenant")
			.unwrap()
			.pins
			.insert("42".into(), NodeId(3));
		assert_eq!(c.owner("tenant", "42").unwrap(), NodeId(3));
		// A pin matches the value, not its spelling.
		assert_eq!(c.owner("tenant", "+042").unwrap(), NodeId(3));
		assert_eq!(c.owner("tenant", " 42").unwrap(), NodeId(3));
	}

	#[test]
	fn canonical_text_ignores_spelling() {
		let same = |t: KeyType, a: &str, b: &str| {
			assert_eq!(
				t.canonical_text(a).unwrap(),
				t.canonical_text(b).unwrap(),
				"{a} vs {b}"
			)
		};
		let differ = |t: KeyType, a: &str, b: &str| {
			assert_ne!(
				t.canonical_text(a).unwrap(),
				t.canonical_text(b).unwrap(),
				"{a} vs {b}"
			)
		};
		same(KeyType::Numeric, "1.0", "1.00");
		same(KeyType::Numeric, "0", "-0.0");
		differ(KeyType::Numeric, "1", "-1");
		differ(KeyType::Numeric, "NaN", "Infinity");
		same(KeyType::Numeric, "inf", "+Infinity");
		same(KeyType::Bpchar, "CA", "CA  ");
		differ(KeyType::Text, "CA", "CA ");
		same(
			KeyType::Uuid,
			"{A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11}",
			"a0eebc999c0b4ef8bb6d6bb9bd380a11",
		);
	}

	#[test]
	fn fence_names_only_owned_ranges() {
		let c = catalog(2);
		let ks = &c.keyspaces["tenant"];
		let sql = fence_sql(
			&RelationName {
				schema: "app".into(),
				table: "orders".into(),
			},
			"tenant_id",
			ks,
			NodeId(2),
			180_000,
		)
		.unwrap();
		assert_eq!(
			sql.matches(" between ").count(),
			ks.ranges_of(NodeId(2)).len()
		);
		assert!(sql.contains("hashint8extended(\"tenant_id\", 24301)"));
		assert!(sql.ends_with("not valid"));
	}
}
