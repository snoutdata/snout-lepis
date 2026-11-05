//! `node.readdress`: a node is reached somewhere else now, and its catalog row says so.
//!
//! Whoever runs the nodes may move one: in SnoutData Cloud a node, or the home with the router
//! beside it, can move to another machine, after which the router dials it at another host and
//! port, and the other nodes reach it (for a subscription) by other labels (`spec::reach`). Its id,
//! name, ranges, pins and fences stay what they are; only `host`, `port` and the addressing labels
//! (`spec::ADDRESS_LABELS`) change, in one home transaction with the epoch, so every router reads
//! the new address.
//!
//! What is checked first, at the plan and again when the step runs: the node is in the catalog and
//! not removed, no other node is at that address, and the node answers there in the state its
//! catalog row implies (a joining node is a standby, in recovery; an active or draining one is
//! not). A row that already says all of it is refused as already done at the plan, and left as it
//! is by the step, so running it twice changes nothing.

use serde_json::{Value, json};

use super::Kind;
use super::attach::target_of;
use super::spec::{ADDRESS_LABELS, NodeSpec};
use super::steps::{StepCtx, in_recovery};
use super::{OpError, Pg, Target, load_catalog, state_name};
use crate::catalog::{Catalog, NodeId, NodeState, quote_literal};
use crate::server::App;

/// The node's addressing labels as its row holds them (only `ADDRESS_LABELS`).
async fn address_labels_of(h: &mut Pg, node: NodeId) -> Result<Value, OpError> {
	let text = h
		.value(
			"select coalesce(labels, '{}')::text from lepis.node where id = $1",
			&[&node.0.to_string()],
		)
		.await?
		.unwrap_or_else(|| "{}".into());
	let all: Value = serde_json::from_str(&text).unwrap_or_else(|_| json!({}));
	Ok(only_address(&all))
}

/// The addressing labels of a label object, and nothing else.
pub fn only_address(labels: &Value) -> Value {
	let mut v = json!({});
	for k in ADDRESS_LABELS {
		if let Some(x) = labels.get(k).filter(|x| !x.is_null()) {
			v[k] = x.clone();
		}
	}
	v
}

/// Why this address may not be written on this node, or None. Pure, so the rules are tested.
pub fn refusal(c: &Catalog, node: NodeId, spec: &NodeSpec) -> Option<OpError> {
	let Some(n) = c.nodes.get(&node) else {
		return Some(OpError::refused(
			Kind::NoSuchNode,
			format!("there is no {node}"),
		));
	};
	if n.state == NodeState::Removed {
		return Some(OpError::refused(
			Kind::NodeUnsuitable,
			format!(
				"{} is removed; only a node of the cluster is re-addressed",
				n.name
			),
		));
	}
	if spec.host.is_empty() || spec.port == 0 {
		return Some(OpError::refused(
			Kind::BadRequest,
			"host and port are required",
		));
	}
	if let Some(o) = c.nodes.values().find(|o| {
		o.id != node && o.state != NodeState::Removed && o.host == spec.host && o.port == spec.port
	}) {
		return Some(OpError::refused(
			Kind::NodeNameTaken,
			format!("{}:{} is {}'s address", spec.host, spec.port, o.name),
		));
	}
	None
}

/// Whether the row already says this: the same host and port, and the same addressing labels.
pub fn already(c: &Catalog, node: NodeId, spec: &NodeSpec, labels: &Value) -> bool {
	c.nodes.get(&node).is_some_and(|n| {
		n.host == spec.host && n.port == spec.port && only_address(labels) == spec.address_labels()
	})
}

/// The node answers at the new address, in the state its row implies.
async fn reachable(app: &App, c: &Catalog, node: NodeId, spec: &NodeSpec) -> Result<(), OpError> {
	let n = &c.nodes[&node];
	let at = NodeSpec {
		name: n.name.clone(),
		dbname: n.dbname.clone(),
		sslmode: n.sslmode,
		..spec.clone()
	};
	let target: Target = target_of(app, &at);
	let mut pg = Pg::connect(app, &target).await?;
	let standby = in_recovery(&mut pg).await?;
	pg.close().await;
	let wanted = n.state == NodeState::Joining;
	if standby != wanted {
		return Err(OpError::refused(
			Kind::NodeUnsuitable,
			format!(
				"{} is {}, and what answers at {}:{} {}",
				n.name,
				state_name(n.state),
				spec.host,
				spec.port,
				if standby {
					"is a standby"
				} else {
					"is not a standby"
				}
			),
		));
	}
	Ok(())
}

/// What `plan` refuses on, so a re-address that cannot be right is refused before it is a job.
pub async fn precheck(
	app: &App,
	c: &Catalog,
	node: NodeId,
	spec: &NodeSpec,
) -> Result<(), OpError> {
	if let Some(e) = refusal(c, node, spec) {
		return Err(e);
	}
	let mut h = Pg::connect(app, &Target::home(app)).await?;
	let labels = address_labels_of(&mut h, node).await?;
	h.close().await;
	if already(c, node, spec, &labels) {
		return Err(OpError::refused(
			Kind::AlreadyDone,
			format!(
				"{} is already at {}:{}",
				c.nodes[&node].name, spec.host, spec.port
			),
		));
	}
	reachable(app, c, node, spec).await
}

/// The step: checked again, then the row written with the epoch, unless it already says it.
pub async fn run(
	cx: &mut StepCtx,
	h: &mut Pg,
	node: NodeId,
	spec: &NodeSpec,
) -> Result<(), OpError> {
	let c = load_catalog(h).await?;
	if let Some(e) = refusal(&c, node, spec) {
		return Err(e);
	}
	let labels = address_labels_of(h, node).await?;
	if already(&c, node, spec, &labels) {
		return cx.save(h, json!({"readdressed": false})).await;
	}
	reachable(&cx.app, &c, node, spec).await?;
	let drop = ADDRESS_LABELS
		.iter()
		.map(|k| format!(" - '{k}'"))
		.collect::<String>();
	h.simple(&format!(
		"begin; update lepis.node set host = {}, port = {}, \
		labels = (coalesce(labels, '{{}}'::jsonb){drop}) || {}::jsonb where id = {}; \
		select lepis.bump(); {} commit",
		quote_literal(&spec.host),
		spec.port,
		quote_literal(&spec.address_labels().to_string()),
		node.0,
		cx.save_sql(&json!({"readdressed": true, "host": spec.host, "port": spec.port})),
	))
	.await?;
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ops::tests::cat;

	fn spec(v: Value) -> NodeSpec {
		let mut v = v;
		v["name"] = json!("");
		NodeSpec::from_json(&v).unwrap()
	}

	#[test]
	fn refuses_what_cannot_be_right() {
		let mut c = cat(2, 2);
		let n2 = c.nodes[&NodeId(2)].clone();
		let s = spec(json!({"host": "10.0.0.7", "port": 15433}));
		assert!(refusal(&c, NodeId(2), &s).is_none());
		assert_eq!(
			refusal(&c, NodeId(99), &s).unwrap().kind,
			Some(Kind::NoSuchNode)
		);
		let taken = spec(json!({"host": n2.host, "port": n2.port}));
		assert_eq!(
			refusal(&c, NodeId(1), &taken).unwrap().kind,
			Some(Kind::NodeNameTaken)
		);
		// Its own address is not another node's.
		assert!(refusal(&c, NodeId(2), &taken).is_none());
		c.nodes.get_mut(&NodeId(2)).unwrap().state = NodeState::Removed;
		assert_eq!(
			refusal(&c, NodeId(2), &s).unwrap().kind,
			Some(Kind::NodeUnsuitable)
		);
		// A removed node's old address is free.
		assert!(refusal(&c, NodeId(1), &taken).is_none());
	}

	#[test]
	fn already_compares_the_address_and_its_labels_only() {
		let c = cat(2, 2);
		let n2 = c.nodes[&NodeId(2)].clone();
		let same = spec(json!({"host": n2.host, "port": n2.port}));
		assert!(already(&c, NodeId(2), &same, &json!({"standby_of": 1})));
		assert!(!already(&c, NodeId(2), &same, &json!({"peer_host": "x"})));
		let site = spec(
			json!({"host": n2.host, "port": n2.port, "site": "10.0.0.5", "site_host": "pod", "site_port": 5432, "peer_host": "10.0.0.5", "peer_port": 15433}),
		);
		assert!(!already(&c, NodeId(2), &site, &json!({})));
		assert!(already(
			&c,
			NodeId(2),
			&site,
			&json!({"site": "10.0.0.5", "site_host": "pod", "site_port": 5432, "peer_host": "10.0.0.5", "peer_port": 15433, "failed_over_from": "n9"})
		));
		let moved = spec(json!({"host": "10.0.0.6", "port": n2.port}));
		assert!(!already(&c, NodeId(2), &moved, &json!({})));
	}
}
