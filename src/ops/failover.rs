//! `node.failover`: a node is lost or corrupt, and its REPLICA takes its place (L16-L17).
//!
//! The replica is a streaming standby of the node, made by whoever runs the nodes (in SnoutData
//! Cloud, a pod restored from the node's own pgBackRest stanza; in a test, `pg_basebackup -R`),
//! and it is not in the catalog. The node keeps its id: its ranges, pins, fences and sequences
//! stay what they are, and only its catalog row's name and address change to the replica's.
//!
//! The phases, each recorded in the step's detail before the next starts:
//!
//! 1. **checked.** The replica is in recovery and streams from THIS node (its `primary_conninfo`
//!    names the node's host), which works with the node gone. The node is fenced: either the
//!    caller says it stopped it (`fenced: true`; the Cloud stops the pod first), or Lepis reaches
//!    it, makes it read-only, ends its sessions and waits for the replica to replay its last
//!    position. A node that answers and was not fenced is never failed over around: two
//!    writable copies of one node is the one outcome worse than a node down.
//! 2. **promoted.** `pg_promote`, then `synchronous_standby_names` reset on the promoted copy:
//!    it came with the node's settings, which name the replica itself, and a primary waiting for
//!    a standby that does not exist hangs every commit. Its host sets it again once it has a
//!    replica of its own.
//! 3. **cut.** The node's row names the replica, in one home transaction with the epoch, then the
//!    routers ack; one that does not in time is fenced out, as a cutover does. Routers dial by
//!    address, so their pools for the old node are never used again.
//!
//! What a commit acknowledged by the node but not yet on the replica becomes depends on how the
//! node replicated: with the replica synchronous (the Cloud, L16) there is none. A prepared
//! two-phase transaction on the node is on the replica too, and in-doubt recovery finishes it.

use std::time::Duration;

use serde_json::json;

use super::Kind;
use super::attach::target_of;
use super::physical::STANDBY_LABEL;
use super::spec::{NodeSpec, sslmode_name};
use super::steps::{StepCtx, connect_node, in_recovery};
use super::{OpError, Pg, load_catalog};
use crate::catalog::{Catalog, NodeId, NodeState, quote_literal};
use crate::server::App;

/// How long Lepis tries to reach the failing node before treating it as gone.
const REACH_NODE: Duration = Duration::from_secs(5);
/// How long a reachable, fenced node's last position may take to reach the replica.
const DRAIN_REPLICA: Duration = Duration::from_secs(60);

/// What `plan` refuses on, so a failover that cannot be right is refused before it is a job.
pub async fn precheck(
	app: &App,
	c: &Catalog,
	node: NodeId,
	spec: &NodeSpec,
) -> Result<serde_json::Value, OpError> {
	let n = c
		.nodes
		.get(&node)
		.ok_or_else(|| OpError::refused(Kind::NoSuchNode, format!("there is no {node}")))?;
	if n.home {
		return Err(OpError::refused(
			Kind::HomeNode,
			// L19: the catalog, the commit log and the router come back with the home's data, so a
			// home is failed over beneath this job (its replica's data promoted in its place), never by it.
			"the home node holds the catalog and runs beside the router, so it is failed over by putting its replica's data in its place, not by node.failover",
		));
	}
	if !matches!(n.state, NodeState::Active | NodeState::Draining) {
		return Err(OpError::refused(
			Kind::NodeUnsuitable,
			format!(
				"{node} is {}; only a node that owns rows is failed over (a joining node is a standby, a removed one is gone)",
				super::state_name(n.state)
			),
		));
	}
	if c.nodes.values().any(|o| {
		o.id != node && (o.name == spec.name || (o.host == spec.host && o.port == spec.port))
	}) {
		return Err(OpError::refused(
			Kind::NodeNameTaken,
			format!(
				"{} is another node of the cluster, not a replica",
				spec.name
			),
		));
	}
	let mut r = Pg::connect(app, &target_of(app, spec)).await?;
	replica_report(&mut r, &n.host, spec).await
}

/// The replica is a standby of `node_host`, or a refusal naming what it is instead. Read on the
/// replica alone, so it holds with the node gone.
async fn replica_report(
	r: &mut Pg,
	node_host: &str,
	spec: &NodeSpec,
) -> Result<serde_json::Value, OpError> {
	if !in_recovery(r).await? {
		return Err(OpError::refused(
			Kind::NodeUnsuitable,
			format!(
				"{} is not in recovery: a replica is a standby of the node it replaces",
				spec.name
			),
		));
	}
	let conninfo = r
		.value("select current_setting('primary_conninfo')", &[])
		.await?
		.unwrap_or_default();
	if !streams_from(&conninfo, node_host) {
		return Err(OpError::refused(
			Kind::NodeUnsuitable,
			format!(
				"{} is not a replica of {node_host} (it streams from: {})",
				spec.name,
				conninfo_host(&conninfo).unwrap_or("nowhere")
			),
		));
	}
	let replayed = r
		.value(
			"select coalesce(pg_last_wal_replay_lsn()::text, '0/0')",
			&[],
		)
		.await?
		.unwrap_or_default();
	Ok(json!({"replica": spec.name, "streams_from": node_host, "replayed_lsn": replayed}))
}

/// The `host=` of a libpq conninfo string, quoted or not.
fn conninfo_host(conninfo: &str) -> Option<&str> {
	conninfo.split_whitespace().find_map(|kv| {
		let (k, v) = kv.split_once('=')?;
		(k == "host").then(|| v.trim_matches('\''))
	})
}

/// Whether a standby with this `primary_conninfo` streams from `host`.
fn streams_from(conninfo: &str, host: &str) -> bool {
	conninfo_host(conninfo) == Some(host)
}

pub async fn run(
	cx: &mut StepCtx,
	h: &mut Pg,
	node: NodeId,
	spec: &NodeSpec,
	fenced: bool,
) -> Result<(), OpError> {
	loop {
		match cx.phase().to_string().as_str() {
			"new" => check(cx, h, node, spec, fenced).await?,
			"checked" => {
				promote(cx, spec).await?;
				cx.save(h, json!({"phase": "promoted"})).await?;
			}
			"promoted" => cut(cx, h, node, spec).await?,
			"cut" => return Ok(()),
			other => return Err(OpError::new(format!("unknown phase {other}"))),
		}
	}
}

async fn check(
	cx: &mut StepCtx,
	h: &mut Pg,
	node: NodeId,
	spec: &NodeSpec,
	fenced: bool,
) -> Result<(), OpError> {
	let c = load_catalog(h).await?;
	let n = c
		.nodes
		.get(&node)
		.ok_or_else(|| OpError::refused(Kind::NoSuchNode, format!("there is no {node}")))?
		.clone();
	if n.name == spec.name {
		// A resume after the catalog already names the replica: nothing left but the routers.
		return cx.save(h, json!({"phase": "promoted"})).await;
	}
	let mut r = Pg::connect(&cx.app, &target_of(&cx.app, spec)).await?;
	if !in_recovery(&mut r).await? {
		// Promoted by an earlier run of this step that stopped before saving it.
		return cx
			.save(
				h,
				json!({"phase": "checked", "resumed": "already promoted"}),
			)
			.await;
	}
	let mut report = replica_report(&mut r, &n.host, spec).await?;
	let reached = tokio::time::timeout(REACH_NODE, connect_node(&cx.app, &c, node)).await;
	match reached {
		Ok(Ok(mut old)) => {
			// The node answers: it is made read-only and its sessions end before anything is
			// promoted, and the replica must have everything it wrote.
			old.simple("alter system set default_transaction_read_only = on")
				.await?;
			old.simple("select pg_reload_conf()").await?;
			old.simple(
				"select pg_terminate_backend(pid) from pg_stat_activity \
				where backend_type = 'client backend' and pid <> pg_backend_pid()",
			)
			.await?;
			let lsn = old
				.value("select pg_current_wal_lsn()::text", &[])
				.await?
				.ok_or_else(|| OpError::new("no WAL position on the node"))?;
			old.close().await;
			let start = std::time::Instant::now();
			loop {
				let done = r
					.query(
						"select coalesce(pg_last_wal_replay_lsn() >= $1::pg_lsn, false)",
						&[&lsn],
					)
					.await?;
				if done
					.first()
					.and_then(|x| x.first().cloned().flatten())
					.as_deref() == Some("t")
				{
					break;
				}
				if start.elapsed() > DRAIN_REPLICA {
					return Err(OpError::refused(
						Kind::NodeUnsuitable,
						format!(
							"{} did not replay {}'s last position ({lsn}) in {} s; the node is read-only and still the owner",
							spec.name,
							n.name,
							DRAIN_REPLICA.as_secs()
						),
					));
				}
				tokio::time::sleep(Duration::from_millis(20)).await;
			}
			report["fence"] = json!({"how": "made read-only by lepis", "last_lsn": lsn});
		}
		_ if fenced => {
			report["fence"] = json!({"how": "stopped by the caller"});
		}
		Ok(Err(e)) => {
			return Err(OpError::refused(
				Kind::NodeUnsuitable,
				format!(
					"{} could not be reached ({}) and the caller did not say it is stopped; a node that may still be writing is never failed over around",
					n.name, e.message
				),
			));
		}
		Err(_) => {
			return Err(OpError::refused(
				Kind::NodeUnsuitable,
				format!(
					"{} did not answer in {} s and the caller did not say it is stopped; a node that may still be writing is never failed over around",
					n.name,
					REACH_NODE.as_secs()
				),
			));
		}
	}
	report["phase"] = json!("checked");
	report["failed_node"] = json!(n.name);
	cx.save(h, report).await
}

/// Promotes the replica, if it is not already, and clears the wait for a standby it came with.
async fn promote(cx: &mut StepCtx, spec: &NodeSpec) -> Result<(), OpError> {
	let mut r = Pg::connect(&cx.app, &target_of(&cx.app, spec)).await?;
	if in_recovery(&mut r).await? {
		let ok = r
			.value("select pg_promote(true, 120)", &[])
			.await?
			.unwrap_or_default();
		if ok != "t" {
			return Err(OpError::new(format!(
				"{} did not finish promoting in 120 s",
				spec.name
			)));
		}
	}
	clear_sync_wait(&mut r).await
}

/// A promoted copy waits on no standby until its host gives it one: the settings it came with
/// may name its source's replica, and a primary waiting for a standby that is not there hangs
/// every commit. ALTER SYSTEM cannot run inside a transaction block, so these are separate.
pub async fn clear_sync_wait(pg: &mut Pg) -> Result<(), OpError> {
	let names = pg
		.value("select current_setting('synchronous_standby_names')", &[])
		.await?
		.unwrap_or_default();
	if names.is_empty() {
		return Ok(());
	}
	pg.simple("alter system reset synchronous_standby_names")
		.await?;
	pg.simple("select pg_reload_conf()").await?;
	Ok(())
}

async fn cut(cx: &mut StepCtx, h: &mut Pg, node: NodeId, spec: &NodeSpec) -> Result<(), OpError> {
	let failed = cx.detail["failed_node"]
		.as_str()
		.unwrap_or_default()
		.to_string();
	let mut labels = json!({ "failed_over_from": failed });
	if let Some(p) = &spec.peer_host {
		labels["peer_host"] = json!(p);
	}
	let rows = h
		.simple(&format!(
			"begin; update lepis.node set name = {}, host = {}, port = {}, dbname = {}, sslmode = '{}', \
			labels = (labels - '{STANDBY_LABEL}' - 'peer_host') || {}::jsonb where id = {}; \
			select lepis.bump(); {} commit",
			quote_literal(&spec.name),
			quote_literal(&spec.host),
			spec.port,
			quote_literal(&spec.dbname),
			sslmode_name(spec.sslmode),
			quote_literal(&labels.to_string()),
			node.0,
			cx.save_sql(&json!({"phase": "cut"})),
		))
		.await?;
	if let Some(d) = cx.detail.as_object_mut() {
		d.insert("phase".into(), json!("cut"));
	}
	let epoch: i64 = rows
		.iter()
		.find_map(|r| r.first().cloned().flatten().and_then(|v| v.parse().ok()))
		.ok_or_else(|| OpError::new("the catalog did not return an epoch"))?;
	let (acked, unacked) = super::transfer::wait_acks(h, epoch, cx.settings.ack_timeout).await?;
	if !unacked.is_empty() {
		h.query(
			"update lepis.router set fenced_epoch = $1 where id = any (string_to_array($2, ','))",
			&[&epoch.to_string(), &unacked.join(",")],
		)
		.await?;
	}
	cx.save(
		h,
		json!({"epoch": epoch, "routers_acked": acked, "routers_fenced_out": unacked}),
	)
	.await
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn reads_the_host_a_standby_streams_from() {
		let c = "user=snoutpod_admin passfile=/x host=snoutpod-abc port=5432 application_name=r1";
		assert_eq!(conninfo_host(c), Some("snoutpod-abc"));
		assert!(streams_from(c, "snoutpod-abc"));
		assert!(!streams_from(c, "snoutpod-ab"));
		assert_eq!(conninfo_host("host='h1' port=1"), Some("h1"));
		assert_eq!(conninfo_host(""), None);
	}
}
