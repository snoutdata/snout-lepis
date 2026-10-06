//! A CLUSTER restore point: one named marker in every node's WAL, taken while no two-phase
//! commit can be decided, so a point-in-time restore of every node to that name lands the
//! cluster on one consistent instant (Phase 6, "Backups").
//!
//! Why the decision log is the thing to hold still. A transaction that spans nodes is PREPARED on
//! each of them, then decided by one row in `lepis.prepared` on the home node (the commit point,
//! `twopc.rs`), then COMMIT PREPARED on each. Restored to markers taken at arbitrary moments, one
//! node could hold the commit while another still holds it prepared, and the home node no decision
//! at all, so recovery would roll back the half that is prepared: half a transfer. With
//! `lepis.prepared` locked against writes for the instant the markers are taken, every node's
//! marker sees the same set of decisions: a transaction prepared at its marker either has its
//! decision row at home's marker (the in-doubt recovery commits it) or does not (it rolls back),
//! and one already committed somewhere was decided before the lock, so its row is there. Single-node
//! transactions need nothing: any instant on one node is consistent for them.
//!
//! The lock is held for as long as it takes to write one marker per node (milliseconds); a
//! coordinator deciding during it waits, nothing is refused. The names and the LSN each node
//! reported are kept in `lepis.restore_point` on the home node, which is backed up with it.

use serde_json::{Map, Value, json};

use super::Kind;
use super::steps::{StepCtx, connect_node};
use super::{OpError, Pg, load_catalog};
use crate::catalog::{NodeState, quote_literal};

pub const RESTORE_POINT_SQL: &str = "create table if not exists lepis.restore_point (
	name text primary key,
	epoch bigint not null,
	lsns jsonb not null,
	created_at timestamptz not null default now()
)";

/// A restore point's name: what `recovery_target_name` (and pgBackRest's `--target`) are given.
pub fn check_name(name: &str) -> Result<(), OpError> {
	let ok = !name.is_empty()
		&& name.len() <= 60
		&& name
			.bytes()
			.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-');
	if ok {
		Ok(())
	} else {
		Err(OpError::refused(
			Kind::BadRequest,
			format!("{name:?}: a restore point is named with 1-60 of a-z, 0-9, _ and -"),
		))
	}
}

/// The name a restore point gets when none is asked for, fixed when the job is planned.
pub fn default_name() -> String {
	let secs = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map_or(0, |d| d.as_secs());
	format!("lepis-{secs}")
}

pub async fn run(cx: &mut StepCtx, h: &mut Pg, name: &str) -> Result<(), OpError> {
	check_name(name)?;
	if cx.get("lsns").is_some() {
		return Ok(());
	}
	h.simple(RESTORE_POINT_SQL).await?;
	if h
		.value("select count(*) from lepis.restore_point where name = $1", &[name])
		.await?
		.as_deref()
		!= Some("0")
	{
		return Err(OpError::refused(Kind::AlreadyDone, format!(
			"the cluster already has a restore point named {name}"
		)));
	}
	let c = load_catalog(h).await?;
	let home = c
		.home()
		.map(|n| n.id)
		.ok_or_else(|| OpError::new("the catalog has no home node"))?;
	// Every connection is open before the lock is taken.
	let mut nodes = Vec::new();
	for n in c.nodes.values().filter(|n| n.state != NodeState::Removed && n.id != home) {
		nodes.push((n.id, n.name.clone(), connect_node(&cx.app, &c, n.id).await?));
	}
	let lit = quote_literal(name);
	h.simple("begin; set local lock_timeout = '5s'; lock table lepis.prepared in exclusive mode")
		.await?;
	let mut lsns = Map::new();
	let marked = async {
		for (id, label, pg) in nodes.iter_mut() {
			let lsn = pg
				.value(&format!("select pg_create_restore_point({lit})::text"), &[])
				.await?
				.unwrap_or_default();
			lsns.insert(id.0.to_string(), json!({"node": label, "lsn": lsn}));
		}
		let lsn = h
			.value(&format!("select pg_create_restore_point({lit})::text"), &[])
			.await?
			.unwrap_or_default();
		lsns.insert(home.0.to_string(), json!({"node": "home", "lsn": lsn}));
		h.simple(&format!(
			"insert into lepis.restore_point (name, epoch, lsns) values ({lit}, {}, {}::jsonb); {} commit",
			c.epoch,
			quote_literal(&Value::Object(lsns.clone()).to_string()),
			cx.save_sql(&json!({"restore_point": name, "lsns": Value::Object(lsns.clone())})),
		))
		.await?;
		Ok::<(), OpError>(())
	}
	.await;
	if let Err(e) = marked {
		let _ = h.simple("rollback").await;
		return Err(e);
	}
	if let Some(d) = cx.detail.as_object_mut() {
		d.insert("restore_point".into(), json!(name));
		d.insert("lsns".into(), Value::Object(lsns));
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn names_are_what_recovery_target_name_takes() {
		assert!(check_name("lepis-1791234567").is_ok());
		assert!(check_name(&default_name()).is_ok());
		for bad in ["", "Has Caps", "a;b", "quote'", &"x".repeat(61)] {
			assert!(check_name(bad).is_err(), "{bad:?}");
		}
	}
}
