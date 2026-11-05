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
	/// The port with `peer_host`, when it is not `port`. Kept in `lepis.node.labels`.
	pub peer_port: Option<u16>,
	/// Where the node runs, for a cluster whose nodes are on more than one machine: a node of
	/// the same site reaches this one at `site_host`:`site_port`, a node of another site at
	/// `peer_host`:`peer_port` (`reach`). Kept in `lepis.node.labels`.
	pub site: Option<String>,
	pub site_host: Option<String>,
	pub site_port: Option<u16>,
}

/// The labels that say how other nodes reach a node (`NodeSpec`'s addressing). A node's row
/// keeps the ones its spec names; a re-address or a failover replaces all of them at once.
pub const ADDRESS_LABELS: [&str; 5] = ["peer_host", "peer_port", "site", "site_host", "site_port"];

fn opt_port(v: &Value, k: &str) -> Result<Option<u16>, OpError> {
	match v.get(k) {
		None | Some(Value::Null) => Ok(None),
		Some(p) => p
			.as_u64()
			.and_then(|p| u16::try_from(p).ok())
			.filter(|p| *p > 0)
			.map(Some)
			.ok_or_else(|| OpError::refused(Kind::BadRequest, format!("{k}: not a port"))),
	}
}

/// How a node reaches `source` for a subscription: the same site's address when both name the
/// same site, else the source's `peer_host` (with `peer_port`), else the catalog's own host and
/// port. `labels` are each node's `lepis.node.labels`.
pub fn reach(host: &str, port: u16, source: &Value, target: &Value) -> (String, u16) {
	let text = |v: &Value, k: &str| v.get(k).and_then(Value::as_str).map(str::to_string);
	let num = |v: &Value, k: &str| {
		v.get(k)
			.and_then(Value::as_u64)
			.and_then(|p| u16::try_from(p).ok())
	};
	let same_site =
		matches!((text(source, "site"), text(target, "site")), (Some(a), Some(b)) if a == b);
	if same_site && let Some(h) = text(source, "site_host") {
		return (h, num(source, "site_port").unwrap_or(port));
	}
	match text(source, "peer_host") {
		Some(h) => (h, num(source, "peer_port").unwrap_or(port)),
		None => (host.to_string(), port),
	}
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
			peer_port: opt_port(v, "peer_port")?,
			site: opt_str(v, "site"),
			site_host: opt_str(v, "site_host"),
			site_port: opt_port(v, "site_port")?,
		})
	}

	pub fn to_json(&self) -> Value {
		let mut v = json!({
			"name": self.name, "host": self.host, "port": self.port, "dbname": self.dbname,
			"sslmode": sslmode_name(self.sslmode), "peer_host": self.peer_host,
		});
		// Only when set, so a spec stored before these existed reads back the same.
		for (k, x) in [
			("peer_port", self.peer_port.map(|p| json!(p))),
			("site", self.site.as_ref().map(|s| json!(s))),
			("site_host", self.site_host.as_ref().map(|s| json!(s))),
			("site_port", self.site_port.map(|p| json!(p))),
		] {
			if let Some(x) = x {
				v[k] = x;
			}
		}
		v
	}

	/// The addressing labels this spec names (`ADDRESS_LABELS`), as a JSON object.
	pub fn address_labels(&self) -> Value {
		let mut v = json!({});
		if let Some(p) = &self.peer_host {
			v["peer_host"] = json!(p);
		}
		if let Some(p) = self.peer_port {
			v["peer_port"] = json!(p);
		}
		if let Some(s) = &self.site {
			v["site"] = json!(s);
		}
		if let Some(s) = &self.site_host {
			v["site_host"] = json!(s);
		}
		if let Some(p) = self.site_port {
			v["site_port"] = json!(p);
		}
		v
	}
}

pub fn sslmode_name(m: SslMode) -> &'static str {
	m.as_str()
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
		/// A second key's type: its tables are distributed by two
		/// columns, so one value of the first spreads over every node.
		second_key_type: Option<String>,
	},
	TableDistribute {
		table: RelationName,
		column: String,
		keyspace: String,
		/// One heap per node, as before chunked storage; else chunk tables.
		heap: bool,
		/// The keyspace's chunk count when this is its first chunked table (else it has one).
		chunks: Option<u32>,
		/// The size the table is expected to reach, for the chunk count when none is given.
		expected_bytes: Option<u64>,
		/// The second key column, in a keyspace with a second key (Phase 5).
		second: Option<String>,
	},
	TableReference {
		table: RelationName,
	},
	TableGlobal {
		table: RelationName,
	},
	/// A distribution key chosen for every table of `schemas` from the schema alone (`auto.rs`).
	TableAuto {
		keyspace: String,
		schemas: Vec<String>,
		min_bytes: u64,
	},
	/// One chunk of a chunked keyspace split in two in place, on its node, online.
	/// The halves get new numbers, every table of the keyspace splits it.
	ChunkSplit {
		keyspace: String,
		chunk: u32,
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
	/// A node's replica takes its place (L16-L17, `failover.rs`). `fenced`: the caller stopped it.
	NodeFailover {
		node: NodeRef,
		spec: NodeSpec,
		fenced: bool,
	},
	/// A node is reached somewhere else now (`readdress.rs`): its host, port and addressing labels.
	/// The spec's name, dbname and sslmode are not used.
	NodeReaddress {
		node: NodeRef,
		spec: NodeSpec,
	},
	/// One named marker in every node's WAL with no two-phase decision in between (`restore_point.rs`).
	RestorePoint {
		name: Option<String>,
	},
	/// A keyspace no table uses any more, forgotten with its ranges and pins.
	KeyspaceDrop {
		name: String,
	},
	/// The cluster back to one plain database: allowed once the home is the only node left. Its
	/// data is checked, the fences come off, and the catalog forgets every keyspace and relation;
	/// the job log stays. What runs the router then stops it (the Cloud: `cloud_project_unshard`).
	Dissolve,
}

/// Every operation's name, for the API's own description of itself.
pub const OPS: [&str; 23] = [
	"node.add",
	"node.drain",
	"node.remove",
	"keyspace.create",
	"table.distribute",
	"table.reference",
	"table.global",
	"table.auto",
	"range.split",
	"chunk.split",
	"range.merge",
	"range.move",
	"tenant.pin",
	"rebalance",
	"scale",
	"verify",
	"cleanup",
	"node.attach",
	"node.failover",
	"node.readdress",
	"restore_point",
	"keyspace.drop",
	"dissolve",
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

/// `node.readdress`'s address: a `NodeSpec` read from the body with no name of its own (the node
/// keeps its name, database and TLS mode, so only host, port and the addressing labels are used).
fn readdress_spec(v: &Value) -> Result<NodeSpec, OpError> {
	let mut w = v.clone();
	if w.get("port").is_none() {
		return Err(OpError::refused(
			Kind::BadRequest,
			"port is required (a number)",
		));
	}
	w["name"] = json!("");
	w["sslmode"] = json!("disable");
	NodeSpec::from_json(&w)
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
				second_key_type: v
					.get("second_key_type")
					.and_then(Value::as_str)
					.map(str::to_string),
			},
			"table.distribute" => Op::TableDistribute {
				table: relation_name(&s(v, "table")?)?,
				// `"by": ["tenant_id", "seq"]` names both keys at once (Phase 5).
				column: match v.get("by").and_then(Value::as_array) {
					Some(by) => by
						.first()
						.and_then(Value::as_str)
						.map(str::to_string)
						.ok_or_else(|| OpError::new("by: the key columns, as strings"))?,
					None => s(v, "column")?,
				},
				second: match v.get("by").and_then(Value::as_array) {
					Some(by) if by.len() > 2 => {
						return Err(OpError::new(
							"by: at most two columns, the key and a second key",
						));
					}
					Some(by) => by.get(1).and_then(Value::as_str).map(str::to_string),
					None => v.get("second").and_then(Value::as_str).map(str::to_string),
				},
				keyspace: s(v, "keyspace")?,
				heap: v.get("heap").and_then(Value::as_bool).unwrap_or(false),
				chunks: v
					.get("chunks")
					.and_then(Value::as_u64)
					.and_then(|n| u32::try_from(n).ok()),
				expected_bytes: super::u64_of(v.get("expected_bytes")),
			},
			"table.reference" => Op::TableReference {
				table: relation_name(&s(v, "table")?)?,
			},
			"table.global" => Op::TableGlobal {
				table: relation_name(&s(v, "table")?)?,
			},
			"table.auto" => {
				let schemas = match v.get("schemas") {
					None | Some(Value::Null) => vec!["public".to_string()],
					Some(Value::Array(a)) => a
						.iter()
						.map(|x| {
							x.as_str()
								.filter(|t| !t.is_empty() && !t.contains(','))
								.map(str::to_string)
								.ok_or_else(|| {
									OpError::refused(
										Kind::BadRequest,
										"schemas: a list of schema names, none with a comma",
									)
								})
						})
						.collect::<Result<Vec<_>, _>>()?,
					Some(_) => {
						return Err(OpError::refused(
							Kind::BadRequest,
							"schemas: a list of schema names",
						));
					}
				};
				if schemas.is_empty() {
					return Err(OpError::refused(
						Kind::BadRequest,
						"schemas: name at least one",
					));
				}
				Op::TableAuto {
					keyspace: s(v, "keyspace")?,
					schemas,
					min_bytes: v
						.get("min_bytes")
						.and_then(Value::as_u64)
						.unwrap_or(super::auto::MIN_BYTES),
				}
			}
			"range.split" => Op::RangeSplit {
				keyspace: s(v, "keyspace")?,
				range: i(v, "range")?,
				at: v.get("at").and_then(|_| i(v, "at").ok()),
				to: opt_node(v, "to"),
			},
			"chunk.split" => Op::ChunkSplit {
				keyspace: s(v, "keyspace")?,
				chunk: u32::try_from(i(v, "chunk")?)
					.map_err(|_| OpError::refused(Kind::BadRequest, "chunk is a chunk number"))?,
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
			"node.failover" => Op::NodeFailover {
				node: node_ref(v, "node")?,
				spec: NodeSpec::from_json(v)?,
				fenced: v.get("fenced").and_then(Value::as_bool).unwrap_or(false),
			},
			"node.readdress" => Op::NodeReaddress {
				node: node_ref(v, "node")?,
				spec: readdress_spec(v)?,
			},
			"restore_point" => Op::RestorePoint {
				name: opt_str(v, "name"),
			},
			"keyspace.drop" => Op::KeyspaceDrop {
				name: s(v, "name")?,
			},
			"dissolve" => Op::Dissolve,
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
			Op::TableAuto { .. } => "table.auto",
			Op::RangeSplit { .. } => "range.split",
			Op::ChunkSplit { .. } => "chunk.split",
			Op::RangeMerge { .. } => "range.merge",
			Op::RangeMove { .. } => "range.move",
			Op::TenantPin { .. } => "tenant.pin",
			Op::Rebalance { .. } => "rebalance",
			Op::Scale { .. } => "scale",
			Op::Verify { .. } => "verify",
			Op::Cleanup { .. } => "cleanup",
			Op::NodeAttach { .. } => "node.attach",
			Op::NodeFailover { .. } => "node.failover",
			Op::NodeReaddress { .. } => "node.readdress",
			Op::RestorePoint { .. } => "restore_point",
			Op::KeyspaceDrop { .. } => "keyspace.drop",
			Op::Dissolve => "dissolve",
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
	/// A node's catalog row moved to its promoted replica (`failover.rs`).
	NodeFailover {
		node: NodeId,
		spec: NodeSpec,
		fenced: bool,
	},
	/// A node's catalog row given its new address (`readdress.rs`).
	NodeReaddress {
		node: NodeId,
		spec: NodeSpec,
	},
	RestorePoint {
		name: String,
	},
	/// Fences off and the catalog's shape forgotten, on a cluster of the home alone (`Op::Dissolve`).
	Dissolve,
	/// A chunked table whose chunks are all on the home node becomes one heap there again, and
	/// global, in one home transaction (`table.global`, `dissolve`).
	Dechunk {
		table: RelationName,
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
			Step::NodeFailover { .. } => "node.failover",
			Step::NodeReaddress { .. } => "node.readdress",
			Step::RestorePoint { .. } => "restore_point",
			Step::Dissolve => "dissolve",
			Step::Dechunk { .. } => "dechunk",
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
			Step::NodeFailover { node, spec, fenced } => {
				json!({"node": node.0, "spec": spec.to_json(), "fenced": fenced})
			}
			Step::NodeReaddress { node, spec } => {
				json!({"node": node.0, "spec": spec.to_json()})
			}
			Step::RestorePoint { name } => json!({"name": name}),
			Step::Dissolve => json!({}),
			Step::Dechunk { table } => json!({"table": table.to_string()}),
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
			"node.failover" => Step::NodeFailover {
				node: node("node")?,
				spec: NodeSpec::from_json(a.get("spec").unwrap_or(&Value::Null))?,
				fenced: a.get("fenced").and_then(Value::as_bool).unwrap_or(false),
			},
			"node.readdress" => Step::NodeReaddress {
				node: node("node")?,
				spec: NodeSpec::from_json(a.get("spec").unwrap_or(&Value::Null))?,
			},
			"restore_point" => Step::RestorePoint {
				name: s(a, "name")?,
			},
			"dissolve" => Step::Dissolve,
			"dechunk" => Step::Dechunk {
				table: relation_name(&s(a, "table")?)?,
			},
			other => return Err(OpError::new(format!("unknown step {other}"))),
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn reach_picks_the_address_by_site() {
		let none = json!({});
		// No labels: the catalog's own address, as before.
		assert_eq!(reach("h2", 5432, &none, &none), ("h2".into(), 5432));
		// The home's peer_host alone (a router beside it on loopback): that host, the catalog port.
		assert_eq!(
			reach("127.0.0.1", 5432, &json!({"peer_host": "pod-a"}), &none),
			("pod-a".into(), 5432)
		);
		let a = json!({"site": "10.0.0.5", "site_host": "pod-a", "site_port": 5432, "peer_host": "10.0.0.5", "peer_port": 15433});
		// The same site: by the pod's name there.
		assert_eq!(
			reach("pod-a", 5432, &a, &json!({"site": "10.0.0.5"})),
			("pod-a".into(), 5432)
		);
		// Another site, or a target that names none: by the machine's address and the published port.
		assert_eq!(
			reach("pod-a", 5432, &a, &json!({"site": "10.0.0.6"})),
			("10.0.0.5".into(), 15433)
		);
		assert_eq!(reach("pod-a", 5432, &a, &none), ("10.0.0.5".into(), 15433));
		// A site with no site_host: its peer address.
		assert_eq!(
			reach(
				"x",
				7,
				&json!({"site": "s", "peer_host": "p"}),
				&json!({"site": "s"})
			),
			("p".into(), 7)
		);
	}

	#[test]
	fn addressing_round_trips_and_is_left_out_when_unset() {
		let plain = NodeSpec::from_json(&json!({"name": "n2", "host": "h2", "sslmode": "disable"}))
			.unwrap();
		assert_eq!(plain.address_labels(), json!({}));
		assert!(plain.to_json().get("site").is_none());
		let full = NodeSpec::from_json(&json!({
			"name": "n2", "host": "10.0.0.6", "port": 15434, "sslmode": "disable",
			"peer_host": "10.0.0.6", "peer_port": 15434, "site": "10.0.0.6", "site_host": "pod-b", "site_port": 5432,
		}))
		.unwrap();
		assert_eq!(NodeSpec::from_json(&full.to_json()).unwrap(), full);
		assert_eq!(
			full.address_labels(),
			json!({"peer_host": "10.0.0.6", "peer_port": 15434, "site": "10.0.0.6", "site_host": "pod-b", "site_port": 5432})
		);
		assert!(NodeSpec::from_json(&json!({"name": "n", "host": "h", "peer_port": 0})).is_err());
		assert!(
			NodeSpec::from_json(&json!({"name": "n", "host": "h", "site_port": 70000})).is_err()
		);
	}

	#[test]
	fn readdress_reads_its_body() {
		let op = Op::from_json(&json!({"op": "node.readdress", "node": "n2", "host": "10.0.0.6", "port": 15434, "site": "10.0.0.6"})).unwrap();
		match &op {
			Op::NodeReaddress { node, spec } => {
				assert_eq!(node, &NodeRef::Name("n2".into()));
				assert_eq!(
					(spec.host.as_str(), spec.port, spec.site.as_deref()),
					("10.0.0.6", 15434, Some("10.0.0.6"))
				);
			}
			other => panic!("{other:?}"),
		}
		assert_eq!(op.name(), "node.readdress");
		assert!(OPS.contains(&"node.readdress"));
		assert!(Op::from_json(&json!({"op": "node.readdress", "node": 2, "host": "h"})).is_err());
		assert!(
			Op::from_json(&json!({"op": "node.readdress", "host": "h", "port": 5432})).is_err()
		);
		let st = Step::NodeReaddress {
			node: NodeId(2),
			spec: NodeSpec::from_json(
				&json!({"name": "", "host": "h", "port": 15434, "sslmode": "disable", "site": "s"}),
			)
			.unwrap(),
		};
		assert_eq!(Step::from_json(st.kind(), &st.args()).unwrap(), st);
	}

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
			Step::Dissolve,
			Step::Dechunk {
				table: RelationName {
					schema: "public".into(),
					table: "t".into(),
				},
			},
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
		assert_eq!(
			Op::from_json(&json!({"op": "dissolve"})).unwrap(),
			Op::Dissolve
		);
		assert_eq!(
			Op::from_json(&json!({"op": "keyspace.drop", "name": "k"})).unwrap(),
			Op::KeyspaceDrop { name: "k".into() }
		);
		let e = Op::from_json(&json!({"op": "range.teleport"})).unwrap_err();
		assert!(e.message.contains("range.move"), "{e}");
		let e = Op::from_json(&json!({"op": "range.move", "keyspace": "k"})).unwrap_err();
		assert!(e.message.contains("range is required"), "{e}");
	}
}
