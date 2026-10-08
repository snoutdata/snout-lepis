//! `node.attach`: a node that is a PHYSICAL STANDBY of a node already in the cluster joins it,
//! so the next move from that node is L8's physical path (`physical.rs`).
//!
//! Who made the standby is not Lepis's business. In SnoutData Cloud the host agent restores a new
//! pod from the source's pgBackRest stanza in S3 and lets it stream from the source; standalone it
//! can be `pg_basebackup -R`. What Lepis checks is what the physical move relies on: the node is in
//! recovery, it is a copy of the node it names (the same system identifier), and its settings are
//! the ones a node needs (L1). It joins as `joining`, owning nothing, with `labels.standby_of`
//! naming its source; the first physical move from that source promotes it and makes it `active`.
//!
//! No role sync and no table copies, unlike `node.add`: a physical copy already has the source's
//! roles, verifiers and tables, and a standby cannot be written to anyway.

use serde_json::json;

use super::Kind;
use super::physical::{STANDBY_LABEL, standby_report};
use super::spec::{NodeSpec, sslmode_name};
use super::steps::{StepCtx, check_node, connect_node};
use super::{OpError, Pg, Target, load_catalog};
use crate::catalog::{Catalog, NodeId, quote_literal};
use crate::server::App;

pub fn target_of(app: &App, spec: &NodeSpec) -> Target {
	Target {
		label: format!("{} ({}:{})", spec.name, spec.host, spec.port),
		address: crate::config::NodeAddress {
			host: spec.host.clone(),
			port: spec.port,
			sslmode: spec.sslmode,
			ca_file: app.config.home.ca_file.clone(),
		},
		dbname: spec.dbname.clone(),
	}
}

/// The checks `plan` refuses on, so a bad attach is refused before it is a job.
pub async fn precheck(
	app: &App,
	c: &Catalog,
	spec: &NodeSpec,
	standby_of: NodeId,
) -> Result<serde_json::Value, OpError> {
	if c.nodes.values().any(|n| n.name == spec.name) {
		return Err(OpError::refused(
			Kind::NodeNameTaken,
			format!("a node named {} is already in the cluster", spec.name),
		));
	}
	if !c.nodes.contains_key(&standby_of) {
		return Err(OpError::refused(
			Kind::NoSuchNode,
			format!("there is no {standby_of}"),
		));
	}
	let mut dst = Pg::connect(app, &target_of(app, spec)).await?;
	let mut src = connect_node(app, c, standby_of).await?;
	let mut report = standby_report(&mut src, &mut dst).await?;
	let settings = check_node(&mut dst).await?;
	report["settings"] = settings;
	Ok(report)
}

/// The step: check again (the standby may have been rebuilt since the plan), then write the
/// node into the catalog, idempotently.
pub async fn run(
	cx: &mut StepCtx,
	h: &mut Pg,
	id: NodeId,
	spec: &NodeSpec,
	standby_of: NodeId,
) -> Result<(), OpError> {
	let c = load_catalog(h).await?;
	if !c.nodes.contains_key(&id) {
		let report = precheck(&cx.app, &c, spec, standby_of).await?;
		let version = report["settings"]["server_version_num"]
			.as_u64()
			.unwrap_or(0);
		let mut labels = json!({ STANDBY_LABEL: standby_of.0 });
		if let Some(p) = &spec.peer_host {
			labels["peer_host"] = json!(p);
		}
		h.simple(&format!(
			"begin; insert into lepis.node (id, name, host, port, dbname, sslmode, kind, state, server_version_num, labels) \
			values ({}, {}, {}, {}, {}, '{}', 'data', 'joining', {version}, {}::jsonb) on conflict (id) do nothing; \
			select lepis.bump(); commit",
			id.0,
			quote_literal(&spec.name),
			quote_literal(&spec.host),
			spec.port,
			quote_literal(&spec.dbname),
			sslmode_name(spec.sslmode),
			quote_literal(&labels.to_string()),
		))
		.await?;
		cx.save(h, json!({"attached": report})).await?;
	}
	Ok(())
}
