//! pg_cron on a sharded cluster (Phase 7): a job runs on the HOME node, where
//! pg_cron's own table puts it, unless `lepis.cron_job` marks it to run on EVERY node, and then
//! each other writable node runs a copy of it.
//!
//! A copy is named `lepis:<jobname>`, which is what tells the runner's jobs from a job a node has
//! of its own (never touched). `sync` is run by the in-doubt recovery leader (twopc.rs) beside
//! role reconcile, over Lepis's service login: pg_cron schedules a job as another role only for a
//! superuser. A node made by a physical split starts with none of its source's jobs (ops/physical.rs
//! deletes the `cron.job` rows it copied), and gets its copies on the first pass after it is
//! active. A node without pg_cron is skipped and named.

use crate::backend::{Backend, BackendError};
use crate::catalog::{Catalog, NodeState};
use crate::server::App;
use crate::twopc;

/// The prefix a copy is named with on another node.
pub const COPY_PREFIX: &str = "lepis:";

/// Whether there is anything to read: the marking table and pg_cron on home.
const PRESENT: &str =
	"select to_regclass('lepis.cron_job') is not null and to_regclass('cron.job') is not null";

/// Home's jobs marked to run everywhere, with what pg_cron holds for each.
const WANTED: &str = "select j.jobname, j.schedule, j.command, j.database, j.username, j.active \
	from cron.job j join lepis.cron_job m on m.jobname = j.jobname \
	where m.scope = 'every_node' order by j.jobname, j.username";

/// The copies already on a node.
const COPIES: &str = "select jobid, jobname, schedule, command, database, username, active \
	from cron.job where jobname like 'lepis:%' order by jobid";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
	pub name: String,
	pub schedule: String,
	pub command: String,
	pub database: String,
	pub username: String,
	pub active: bool,
}

/// What a node needs: jobs to schedule (or reschedule; pg_cron updates a job of the same name and
/// role in place) and the ids of copies to remove.
pub fn plan(wanted: &[Job], copies: &[(i64, Job)]) -> (Vec<Job>, Vec<i64>) {
	let mut schedule = Vec::new();
	for w in wanted {
		let copy = Job {
			name: format!("{COPY_PREFIX}{}", w.name),
			..w.clone()
		};
		if !copies.iter().any(|(_, c)| *c == copy) {
			schedule.push(copy);
		}
	}
	let mut seen = std::collections::HashSet::new();
	let unschedule = copies
		.iter()
		.filter(|(_, c)| {
			let wanted_here = wanted
				.iter()
				.any(|w| c.name == format!("{COPY_PREFIX}{}", w.name) && c.username == w.username);
			// One copy per name and role; a second (left by a role that was renamed) goes.
			!wanted_here || !seen.insert((c.name.clone(), c.username.clone()))
		})
		.map(|(id, _)| *id)
		.collect();
	(schedule, unschedule)
}

fn job(row: &[Option<String>], from: usize) -> Job {
	let get = |i: usize| row.get(from + i).cloned().flatten().unwrap_or_default();
	Job {
		name: get(0),
		schedule: get(1),
		command: get(2),
		database: get(3),
		username: get(4),
		active: get(5) == "t",
	}
}

fn yes(rows: &[Vec<Option<String>>]) -> bool {
	rows.first()
		.and_then(|r| r.first().cloned().flatten())
		.as_deref()
		== Some("t")
}

/// Makes every node in `nodes` run a copy of each job home marks `every_node`, and nothing it no
/// longer marks. Reads the marking only when the catalog has it. Returns what it did, one line
/// per statement. Each connection must be allowed to schedule a job as the job's own role.
pub async fn sync(
	home: &mut Backend,
	nodes: &mut [(String, Backend)],
) -> Result<Vec<String>, BackendError> {
	if !yes(&home.query(PRESENT, &[]).await?) {
		return Ok(Vec::new());
	}
	let wanted: Vec<Job> = home
		.query(WANTED, &[])
		.await?
		.iter()
		.map(|r| job(r, 0))
		.collect();
	let mut done = Vec::new();
	for (name, node) in nodes.iter_mut() {
		if !yes(&node
			.query("select to_regclass('cron.job') is not null", &[])
			.await?)
		{
			if !wanted.is_empty() {
				done.push(format!(
					"{name}: no pg_cron, {} job(s) not copied",
					wanted.len()
				));
			}
			continue;
		}
		let copies: Vec<(i64, Job)> = node
			.query(COPIES, &[])
			.await?
			.iter()
			.map(|r| {
				let id = r
					.first()
					.cloned()
					.flatten()
					.and_then(|v| v.parse().ok())
					.unwrap_or(0);
				(id, job(r, 1))
			})
			.collect();
		let (schedule, unschedule) = plan(&wanted, &copies);
		for id in unschedule {
			node.query("select cron.unschedule($1::bigint)", &[&id.to_string()])
				.await?;
			done.push(format!("{name}: unschedule {id}"));
		}
		for j in schedule {
			node.query(
				"select cron.schedule_in_database($1, $2, $3, $4, $5, $6::boolean)",
				&[
					&j.name,
					&j.schedule,
					&j.command,
					&j.database,
					&j.username,
					if j.active { "t" } else { "f" },
				],
			)
			.await?;
			done.push(format!("{name}: schedule {}", j.name));
		}
	}
	Ok(done)
}

/// `sync` over the cluster: home, and every other writable node that is not a standby.
pub async fn sync_cluster(app: &App, catalog: &Catalog) -> Result<Vec<String>, String> {
	let mut home = twopc::home_service(app, "cron sync")
		.await
		.map_err(|e| format!("home node: {e}"))?;
	if !yes(&home.query(PRESENT, &[]).await.map_err(|e| e.to_string())?) {
		home.close().await;
		return Ok(Vec::new());
	}
	let writable = twopc::writable_nodes(catalog);
	let mut nodes = Vec::new();
	let mut skipped = Vec::new();
	for n in catalog.nodes.values() {
		if n.home || n.state == NodeState::Removed || !writable.contains(&n.id) {
			continue;
		}
		match twopc::node_service(app, n, "cron sync").await {
			Ok(mut b) => {
				if twopc::is_standby(&mut b).await.unwrap_or(true) {
					b.close().await;
				} else {
					nodes.push((n.id.to_string(), b));
				}
			}
			Err(e) => skipped.push(format!("{}: {e}", n.id)),
		}
	}
	let result = sync(&mut home, &mut nodes).await.map_err(|e| e.to_string());
	home.close().await;
	for (_, b) in nodes {
		b.close().await;
	}
	let mut done = result?;
	done.extend(skipped);
	Ok(done)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_plan_copies_what_is_marked_and_nothing_else() {
		let tick = Job {
			name: "tick".into(),
			schedule: "1 seconds".into(),
			command: "select 1".into(),
			database: "app".into(),
			username: "app_owner".into(),
			active: true,
		};
		let copy = Job {
			name: "lepis:tick".into(),
			..tick.clone()
		};
		// Nothing there: schedule it.
		assert_eq!(
			plan(std::slice::from_ref(&tick), &[]),
			(vec![copy.clone()], vec![])
		);
		// There and the same: nothing.
		assert_eq!(
			plan(std::slice::from_ref(&tick), &[(7, copy.clone())]),
			(vec![], vec![])
		);
		// Changed on home: scheduled again (pg_cron updates it in place).
		let moved = Job {
			schedule: "5 seconds".into(),
			..tick.clone()
		};
		assert_eq!(
			plan(std::slice::from_ref(&moved), &[(7, copy.clone())]).0[0].schedule,
			"5 seconds"
		);
		// Unmarked: the copy goes; so does a second copy of one name and role.
		assert_eq!(plan(&[], &[(7, copy.clone())]), (vec![], vec![7]));
		assert_eq!(
			plan(std::slice::from_ref(&tick), &[(7, copy.clone()), (9, copy)]),
			(vec![], vec![9])
		);
	}

	#[test]
	fn rows_read_as_jobs() {
		let row: Vec<Option<String>> = ["7", "lepis:t", "1 seconds", "select 1", "app", "u", "f"]
			.iter()
			.map(|s| Some(s.to_string()))
			.collect();
		let j = job(&row, 1);
		assert_eq!(j.name, "lepis:t");
		assert!(!j.active);
		assert!(yes(&[vec![Some("t".into())]]));
		assert!(!yes(&[]));
	}
}
