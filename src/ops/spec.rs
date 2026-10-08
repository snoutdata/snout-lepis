//! What can be asked (`Op`, the L13 operations) and what a job is made of (`Step`), with their
//! JSON forms: an operation is what the admin API receives, a step is what `lepis.job_step`
//! stores.

use serde_json::{Value, json};

use super::Kind;
use super::{Change, OpError, i, relation_name, s};
use crate::catalog::{Catalog, NodeId, RelationName};
use crate::config::SslMode;

/// A node as `node add` names it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeSpec {
	pub name: String,
	pub host: String,
	pub port: u16,
	pub dbname: String,
	pub sslmode: SslMode,
	/// The host OTHER nodes reach this one by, when it differs from the router's view (a
	/// subscription connects node to node). Kept in `lepis.node.labels`.
	pub peer_host: Option<String>,
}

impl NodeSpec {
	pub fn from_json(v: &Value) -> Result<NodeSpec, OpError> {
		let port = v.get("port").and_then(Value::as_u64).unwrap_or(5432);
		Ok(NodeSpec {
			name: s(v, "name")?,
			host: s(v, "host")?,
			port: u16::try_from(port)
				.map_err(|_| OpError::refused(Kind::BadRequest, "port: not a port"))?,
			dbname: v
				.get("dbname")
				.and_then(Value::as_str)
				.unwrap_or("postgres")
				.to_string(),
			sslmode: match v
				.get("sslmode")
				.and_then(Value::as_str)
				.unwrap_or("verify-full")
			{
				"disable" => SslMode::Disable,
				"require" => SslMode::Require,
				"verify-full" => SslMode::VerifyFull,
				other => {
					return Err(OpError::refused(
						Kind::BadRequest,
						format!("sslmode {other}: one of disable, require, verify-full"),
					));
				}
			},
			peer_host: v
				.get("peer_host")
				.and_then(Value::as_str)
				.map(str::to_string),
		})
	}

	pub fn to_json(&self) -> Value {
		json!({
			"name": self.name, "host": self.host, "port": self.port, "dbname": self.dbname,
			"sslmode": sslmode_name(self.sslmode), "peer_host": self.peer_host,
		})
	}
}

pub fn sslmode_name(m: SslMode) -> &'static str {
	match m {
		SslMode::Disable => "disable",
		SslMode::Require => "require",
		SslMode::VerifyFull => "verify-full",
	}
}

/// A node named by id (`3`) or by name (`"n3"`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeRef {
	Id(i32),
	Name(String),
}

impl NodeRef {
	pub fn from_json(v: &Value) -> Option<NodeRef> {
		match v {
			Value::Number(n) => n.as_i64().map(|n| NodeRef::Id(n as i32)),
			Value::String(t) => Some(match t.parse::<i32>() {
				Ok(n) => NodeRef::Id(n),
				Err(_) => NodeRef::Name(t.clone()),
			}),
			_ => None,
		}
	}

	pub fn resolve(&self, c: &Catalog) -> Result<NodeId, OpError> {
		let found = match self {
			NodeRef::Id(n) => c.nodes.get(&NodeId(*n)).map(|n| n.id),
			NodeRef::Name(name) => c.nodes.values().find(|n| &n.name == name).map(|n| n.id),
		};
		found
			.ok_or_else(|| OpError::refused(Kind::NoSuchNode, format!("there is no node {self:?}")))
	}
}

/// The operations of L13. Each one is planned, then run as a job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
	NodeAdd(NodeSpec),
	NodeDrain {
		node: NodeRef,
		to: Vec<NodeRef>,
	},
	NodeRemove {
		node: NodeRef,
	},
	KeyspaceCreate {
		name: String,
		key_type: String,
		seed: Option<u64>,
		ranges: Option<usize>,
		nodes: Vec<NodeRef>,
	},
	TableDistribute {
		table: RelationName,
		column: String,
		keyspace: String,
	},
	TableReference {
		table: RelationName,
	},
	TableGlobal {
		table: RelationName,
	},
	RangeSplit {
		keyspace: String,
		range: i64,
		at: Option<i64>,
		to: Option<NodeRef>,
	},
	RangeMerge {
		keyspace: String,
		a: i64,
		b: i64,
	},
	RangeMove {
		keyspace: String,
		range: i64,
		to: NodeRef,
	},
	TenantPin {
		keyspace: String,
		value: String,
		node: Option<NodeRef>,
	},
	Rebalance {
		keyspace: Option<String>,
	},
	Scale {
		add: Vec<NodeSpec>,
		remove: usize,
	},
	Verify {
		keyspace: Option<String>,
	},
	Cleanup {
		node: Option<NodeRef>,
	},
	/// A physical standby of `standby_of` joins (Phase 6, `attach.rs`).
	NodeAttach {
		spec: NodeSpec,
		standby_of: NodeRef,
	},
	/// One named marker in every node's WAL with no two-phase decision in between (`restore_point.rs`).
	RestorePoint {
		name: Option<String>,
	},
}

/// Every operation's name, for the API's own description of itself.
pub const OPS: [&str; 17] = [
	"node.add",
	"node.drain",
	"node.remove",
	"keyspace.create",
	"table.distribute",
	"table.reference",
	"table.global",
	"range.split",
	"range.merge",
	"range.move",
	"tenant.pin",
	"rebalance",
	"scale",
	"verify",
	"cleanup",
	"node.attach",
	"restore_point",
];

fn node_ref(v: &Value, k: &str) -> Result<NodeRef, OpError> {
	v.get(k).and_then(NodeRef::from_json).ok_or_else(|| {
		OpError::refused(
			Kind::BadRequest,
			format!("{k} is required (a node id or name)"),
		)
	})
}

fn opt_node(v: &Value, k: &str) -> Option<NodeRef> {
	v.get(k).and_then(NodeRef::from_json)
}

fn node_list(v: &Value, k: &str) -> Vec<NodeRef> {
	v.get(k)
		.and_then(Value::as_array)
		.map(|a| a.iter().filter_map(NodeRef::from_json).collect())
		.unwrap_or_default()
}

fn opt_str(v: &Value, k: &str) -> Option<String> {
	v.get(k).and_then(Value::as_str).map(str::to_string)
}

impl Op {
	pub fn from_json(v: &Value) -> Result<Op, OpError> {
		let name = s(v, "op")?;
		Ok(match name.as_str() {
			"node.add" => Op::NodeAdd(NodeSpec::from_json(v)?),
			"node.drain" => Op::NodeDrain {
				node: node_ref(v, "node")?,
				to: node_list(v, "to"),
			},
			"node.remove" => Op::NodeRemove {
				node: node_ref(v, "node")?,
			},
			"keyspace.create" => Op::KeyspaceCreate {
				name: s(v, "name")?,
				key_type: s(v, "key_type")?,
				seed: super::u64_of(v.get("seed")),
				ranges: v.get("ranges").and_then(Value::as_u64).map(|n| n as usize),
				nodes: node_list(v, "nodes"),
			},
			"table.distribute" => Op::TableDistribute {
				table: relation_name(&s(v, "table")?)?,
				column: s(v, "column")?,
				keyspace: s(v, "keyspace")?,
			},
			"table.reference" => Op::TableReference {
				table: relation_name(&s(v, "table")?)?,
			},
			"table.global" => Op::TableGlobal {
				table: relation_name(&s(v, "table")?)?,
			},
			"range.split" => Op::RangeSplit {
				keyspace: s(v, "keyspace")?,
				range: i(v, "range")?,
				at: v.get("at").and_then(|_| i(v, "at").ok()),
				to: opt_node(v, "to"),
			},
			"range.merge" => Op::RangeMerge {
				keyspace: s(v, "keyspace")?,
				a: i(v, "a")?,
				b: i(v, "b")?,
			},
			"range.move" => Op::RangeMove {
				keyspace: s(v, "keyspace")?,
				range: i(v, "range")?,
				to: node_ref(v, "to")?,
			},
			"tenant.pin" => Op::TenantPin {
				keyspace: s(v, "keyspace")?,
				value: v
					.get("value")
					.map(|x| match x {
						Value::String(t) => t.clone(),
						other => other.to_string(),
					})
					.ok_or_else(|| OpError::refused(Kind::BadRequest, "value is required"))?,
				node: opt_node(v, "node"),
			},
			"rebalance" => Op::Rebalance {
				keyspace: opt_str(v, "keyspace"),
			},
			"scale" => Op::Scale {
				add: v
					.get("add")
					.and_then(Value::as_array)
					.map(|a| a.iter().map(NodeSpec::from_json).collect())
					.transpose()?
					.unwrap_or_default(),
				remove: v.get("remove").and_then(Value::as_u64).unwrap_or(0) as usize,
			},
			"verify" => Op::Verify {
				keyspace: opt_str(v, "keyspace"),
			},
			"cleanup" => Op::Cleanup {
				node: opt_node(v, "node"),
			},
			"node.attach" => Op::NodeAttach {
				spec: NodeSpec::from_json(v)?,
				standby_of: node_ref(v, "standby_of")?,
			},
			"restore_point" => Op::RestorePoint {
				name: opt_str(v, "name"),
			},
			other => {
				return Err(OpError::refused(
					Kind::UnknownOperation,
					format!("unknown operation {other}; one of {}", OPS.join(", ")),
				));
			}
		})
	}

	pub fn name(&self) -> &'static str {
		match self {
			Op::NodeAdd(_) => "node.add",
			Op::NodeDrain { .. } => "node.drain",
			Op::NodeRemove { .. } => "node.remove",
			Op::KeyspaceCreate { .. } => "keyspace.create",
			Op::TableDistribute { .. } => "table.distribute",
			Op::TableReference { .. } => "table.reference",
			Op::TableGlobal { .. } => "table.global",
			Op::RangeSplit { .. } => "range.split",
			Op::RangeMerge { .. } => "range.merge",
			Op::RangeMove { .. } => "range.move",
			Op::TenantPin { .. } => "tenant.pin",
			Op::Rebalance { .. } => "rebalance",
			Op::Scale { .. } => "scale",
			Op::Verify { .. } => "verify",
			Op::Cleanup { .. } => "cleanup",
			Op::NodeAttach { .. } => "node.attach",
			Op::RestorePoint { .. } => "restore_point",
		}
	}
}

/// One logical move (L8) from one source to one or more targets, ending in one cutover (L10)
/// that applies `change` to the catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferSpec {
	pub source: NodeId,
	pub targets: Vec<NodeId>,
	pub tables: Vec<RelationName>,
	pub change: Change,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
	/// Version and settings of a node about to join (L1).
	NodeCheck(NodeSpec),
	NodeInsert {
		id: NodeId,
		spec: NodeSpec,
	},
	/// The login roles of the home node, with the same SCRAM verifiers (L11), on `node`.
	RolesSync {
		node: NodeId,
	},
	/// The table's definition copied to `node` (empty), fenced as the catalog says.
	TableCreate {
		node: NodeId,
		table: RelationName,
	},
	Transfer(TransferSpec),
	/// A metadata-only change, then the fences it implies.
	Catalog(Change),
	/// Every fence of a keyspace rewritten on every node from the catalog.
	Fences {
		keyspace: String,
	},
	TableDrop {
		node: NodeId,
		table: RelationName,
	},
	Verify {
		keyspace: Option<String>,
	},
	Cleanup {
		node: Option<NodeId>,
	},
	/// A physical standby written into the catalog as `joining` (`attach.rs`).
	NodeAttach {
		id: NodeId,
		spec: NodeSpec,
		standby_of: NodeId,
	},
	RestorePoint {
		name: String,
	},
}

fn tables_json(t: &[RelationName]) -> Value {
	Value::Array(t.iter().map(|r| json!(r.to_string())).collect())
}

impl Step {
	pub fn kind(&self) -> &'static str {
		match self {
			Step::NodeCheck(_) => "node.check",
			Step::NodeInsert { .. } => "node.insert",
			Step::RolesSync { .. } => "roles.sync",
			Step::TableCreate { .. } => "table.create",
			Step::Transfer(_) => "transfer",
			Step::Catalog(_) => "catalog",
			Step::Fences { .. } => "fences",
			Step::TableDrop { .. } => "table.drop",
			Step::Verify { .. } => "verify",
			Step::Cleanup { .. } => "cleanup",
			Step::NodeAttach { .. } => "node.attach",
			Step::RestorePoint { .. } => "restore_point",
		}
	}

	pub fn args(&self) -> Value {
		match self {
			Step::NodeCheck(n) => n.to_json(),
			Step::NodeInsert { id, spec } => json!({"id": id.0, "spec": spec.to_json()}),
			Step::RolesSync { node } => json!({"node": node.0}),
			Step::TableCreate { node, table } | Step::TableDrop { node, table } => {
				json!({"node": node.0, "table": table.to_string()})
			}
			Step::Transfer(t) => json!({
				"source": t.source.0,
				"targets": t.targets.iter().map(|n| n.0).collect::<Vec<_>>(),
				"tables": tables_json(&t.tables),
				"change": t.change.to_json(),
			}),
			Step::Catalog(c) => json!({"change": c.to_json()}),
			Step::Fences { keyspace } => json!({"keyspace": keyspace}),
			Step::Verify { keyspace } => json!({"keyspace": keyspace}),
			Step::Cleanup { node } => json!({"node": node.map(|n| n.0)}),
			Step::NodeAttach {
				id,
				spec,
				standby_of,
			} => {
				json!({"id": id.0, "spec": spec.to_json(), "standby_of": standby_of.0})
			}
			Step::RestorePoint { name } => json!({"name": name}),
		}
	}

	pub fn from_json(kind: &str, a: &Value) -> Result<Step, OpError> {
		let node = |k: &str| -> Result<NodeId, OpError> { Ok(NodeId(i(a, k)? as i32)) };
		let tables = |v: Option<&Value>| -> Result<Vec<RelationName>, OpError> {
			v.and_then(Value::as_array)
				.map(|x| {
					x.iter()
						.map(|t| relation_name(t.as_str().unwrap_or_default()))
						.collect()
				})
				.unwrap_or_else(|| Ok(Vec::new()))
		};
		Ok(match kind {
			"node.check" => Step::NodeCheck(NodeSpec::from_json(a)?),
			"node.insert" => Step::NodeInsert {
				id: node("id")?,
				spec: NodeSpec::from_json(a.get("spec").unwrap_or(&Value::Null))?,
			},
			"roles.sync" => Step::RolesSync {
				node: node("node")?,
			},
			"table.create" => Step::TableCreate {
				node: node("node")?,
				table: relation_name(&s(a, "table")?)?,
			},
			"table.drop" => Step::TableDrop {
				node: node("node")?,
				table: relation_name(&s(a, "table")?)?,
			},
			"transfer" => Step::Transfer(TransferSpec {
				source: node("source")?,
				targets: a
					.get("targets")
					.and_then(Value::as_array)
					.map(|x| {
						x.iter()
							.filter_map(Value::as_i64)
							.map(|n| NodeId(n as i32))
							.collect()
					})
					.unwrap_or_default(),
				tables: tables(a.get("tables"))?,
				change: Change::from_json(a.get("change").unwrap_or(&Value::Null))?,
			}),
			"catalog" => Step::Catalog(Change::from_json(a.get("change").unwrap_or(&Value::Null))?),
			"fences" => Step::Fences {
				keyspace: s(a, "keyspace")?,
			},
			"verify" => Step::Verify {
				keyspace: opt_str(a, "keyspace"),
			},
			"cleanup" => Step::Cleanup {
				node: a
					.get("node")
					.and_then(Value::as_i64)
					.map(|n| NodeId(n as i32)),
			},
			"node.attach" => Step::NodeAttach {
				id: node("id")?,
				spec: NodeSpec::from_json(a.get("spec").unwrap_or(&Value::Null))?,
				standby_of: node("standby_of")?,
			},
			"restore_point" => Step::RestorePoint {
				name: s(a, "name")?,
			},
			other => return Err(OpError::new(format!("unknown step {other}"))),
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn steps_round_trip() {
		let steps = [
			Step::Transfer(TransferSpec {
				source: NodeId(1),
				targets: vec![NodeId(2), NodeId(3)],
				tables: vec![relation_name("app.t").unwrap()],
				change: Change::RangeOwner {
					keyspace: "k".into(),
					lo: -5,
					hi: 9,
					to: NodeId(2),
				},
			}),
			Step::Cleanup { node: None },
			Step::Verify {
				keyspace: Some("k".into()),
			},
			Step::NodeInsert {
				id: NodeId(4),
				spec: NodeSpec::from_json(
					&json!({"name": "n4", "host": "db4", "sslmode": "disable"}),
				)
				.unwrap(),
			},
		];
		for st in steps {
			assert_eq!(Step::from_json(st.kind(), &st.args()).unwrap(), st);
		}
	}

	#[test]
	fn ops_parse_and_refuse_with_a_sentence() {
		let op = Op::from_json(
			&json!({"op": "range.split", "keyspace": "k", "range": "-9223372036854775808", "to": "n3"}),
		)
		.unwrap();
		assert_eq!(
			op,
			Op::RangeSplit {
				keyspace: "k".into(),
				range: i64::MIN,
				at: None,
				to: Some(NodeRef::Name("n3".into())),
			}
		);
		let e = Op::from_json(&json!({"op": "range.teleport"})).unwrap_err();
		assert!(e.message.contains("range.move"), "{e}");
		let e = Op::from_json(&json!({"op": "range.move", "keyspace": "k"})).unwrap_err();
		assert!(e.message.contains("range is required"), "{e}");
	}
}
