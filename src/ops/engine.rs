//! The job runner (L13). Every router runs this loop; the one holding the cluster's advisory lock
//! on the home node is the leader and runs jobs, one after another in the order they were made.
//! The lock is a session lock: when the leader's process or connection dies, Postgres releases
//! it and another router takes over, resuming the job at the step it was on. Steps are
//! idempotent (`steps.rs`), so resuming is running the step again.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::sync::Notify;

use super::Kind;
use super::spec::{Op, Step};
use super::steps::{self, StepCtx};
use super::{JOBS_SQL, OpError, Pg, Settings, Target, plan};
use crate::catalog::quote_literal;
use crate::server::App;

/// The advisory lock key of the job leader (a constant; one cluster per database).
pub const LEADER_KEY: i64 = 0x6c65_7069_735f_6f70; // "lepis_op"
const IDLE: Duration = Duration::from_secs(2);
/// A step that fails this many times in a row on errors that look passing (a node unreachable)
/// fails the job.
const MAX_ATTEMPTS: u64 = 30;

/// A name for this process among the routers: host, process id and a random suffix.
pub fn runner_id() -> String {
	let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "lepis".into());
	format!("{host}-{}-{}", std::process::id(), &super::token()[..6])
}

/// Runs jobs for as long as the router runs. `kick` wakes it when the admin API made a job.
pub async fn run(app: Arc<App>, kick: Arc<Notify>) {
	let me = runner_id();
	loop {
		if let Err(e) = lead(&app, &kick, &me).await {
			tracing::warn!("job runner: {e}");
		}
		tokio::select! {
			_ = tokio::time::sleep(IDLE) => {}
			_ = kick.notified() => {}
		}
	}
}

/// Makes the catalog's Phase 4 tables when a catalog from before them is found.
pub async fn ensure_jobs_schema(h: &mut Pg) -> Result<bool, OpError> {
	let rows = h
		.simple("select to_regclass('lepis.cluster') is not null, to_regclass('lepis.job') is not null")
		.await?;
	let r = rows.first().cloned().unwrap_or_default();
	if r.first().cloned().flatten().as_deref() != Some("t") {
		return Ok(false);
	}
	if r.get(1).cloned().flatten().as_deref() != Some("t") {
		h.simple(JOBS_SQL).await?;
	}
	Ok(true)
}

async fn lead(app: &Arc<App>, kick: &Arc<Notify>, me: &str) -> Result<(), OpError> {
	let mut h = Pg::connect(app, &Target::home(app)).await?;
	loop {
		if ensure_jobs_schema(&mut h).await? {
			let got = h
				.value(
					"select pg_try_advisory_lock($1::bigint)",
					&[&LEADER_KEY.to_string()],
				)
				.await?;
			if got.as_deref() == Some("t") {
				break;
			}
		}
		tokio::select! {
			_ = tokio::time::sleep(IDLE) => {}
			_ = kick.notified() => {}
		}
	}
	tracing::info!(runner = me, "this router runs the cluster's jobs");
	loop {
		let next = h
			.simple(
				"select id from lepis.job where state in ('pending', 'running', 'cancelling') order by id limit 1",
			)
			.await?;
		match next
			.first()
			.and_then(|r| r.first().cloned().flatten())
			.and_then(|v| v.parse::<i64>().ok())
		{
			Some(id) => run_job(app, &mut h, id, me).await?,
			None => {
				tokio::select! {
					_ = tokio::time::sleep(IDLE) => {}
					_ = kick.notified() => {}
				}
			}
		}
	}
}

/// Whether an error should stop the job, or is worth trying again (a node that did not answer,
/// a lock or serialization conflict).
fn passing(e: &OpError) -> bool {
	match e.code.as_deref() {
		None => true,
		Some(c) => matches!(c, "40001" | "40P01" | "55P03" | "57P01" | "57P03" | "08006" | "08001"),
	}
}

async fn run_job(app: &Arc<App>, h: &mut Pg, id: i64, me: &str) -> Result<(), OpError> {
	let job = h
		.query(
			"select state, args, (select settings from lepis.cluster where id = 1) from lepis.job where id = $1",
			&[&id.to_string()],
		)
		.await?;
	let row = job.first().cloned().unwrap_or_default();
	let state = row.first().cloned().flatten().unwrap_or_default();
	let args: Value = row
		.get(1)
		.cloned()
		.flatten()
		.and_then(|v| serde_json::from_str(&v).ok())
		.unwrap_or(Value::Null);
	let cluster: Value = row
		.get(2)
		.cloned()
		.flatten()
		.and_then(|v| serde_json::from_str(&v).ok())
		.unwrap_or(Value::Null);
	let settings = Settings::from_json(&cluster, &args);
	h.query(
		"update lepis.job set state = case when state = 'pending' then 'running' else state end, \
		runner = $2, updated_at = now() where id = $1",
		&[&id.to_string(), me],
	)
	.await?;
	let steps = h
		.query(
			"select n, kind, args::text, state, detail::text from lepis.job_step where job_id = $1 order by n",
			&[&id.to_string()],
		)
		.await?;
	let mut cancelling = state == "cancelling";
	for r in steps {
		let get = |i: usize| r.get(i).cloned().flatten().unwrap_or_default();
		let n: i32 = get(0).parse().unwrap_or(0);
		if matches!(get(3).as_str(), "done" | "skipped") {
			continue;
		}
		let step_args: Value = serde_json::from_str(&get(2)).unwrap_or(Value::Null);
		let step = Step::from_json(&get(1), &step_args)?;
		let detail: Value = serde_json::from_str(&get(4)).unwrap_or_else(|_| json!({}));
		let started = detail.get("phase").is_some();
		cancelling = cancelling || state_of(h, id).await? == "cancelling";
		// A cancel skips what has not started; a move already started rolls back or finishes.
		if cancelling && !(started && matches!(step, Step::Transfer(_))) {
			h.query(
				"update lepis.job_step set state = 'skipped', finished_at = now() where job_id = $1 and n = $2",
				&[&id.to_string(), &n.to_string()],
			)
			.await?;
			continue;
		}
		h.query(
			"update lepis.job_step set state = 'running', started_at = coalesce(started_at, now()) \
			where job_id = $1 and n = $2",
			&[&id.to_string(), &n.to_string()],
		)
		.await?;
		let mut cx = StepCtx {
			app: app.clone(),
			job: id,
			n,
			settings,
			detail,
		};
		let mut step_home = Pg::connect(app, &Target::home(app)).await?;
		let result = steps::run(&mut cx, &mut step_home, &step).await;
		step_home.close().await;
		match result {
			Ok(()) => {
				h.query(
					"update lepis.job_step set state = 'done', finished_at = now() where job_id = $1 and n = $2",
					&[&id.to_string(), &n.to_string()],
				)
				.await?;
			}
			Err(e) if e.is("57014") && e.message == steps::cancelled().message => {
				cancelling = true;
				h.query(
					"update lepis.job_step set state = 'skipped', finished_at = now() where job_id = $1 and n = $2",
					&[&id.to_string(), &n.to_string()],
				)
				.await?;
			}
			Err(e) if passing(&e) => {
				let attempts = cx.get("step_attempts").and_then(Value::as_u64).unwrap_or(0) + 1;
				h.query(
					"update lepis.job_step set detail = detail || $3::jsonb where job_id = $1 and n = $2",
					&[
						&id.to_string(),
						&n.to_string(),
						&json!({"step_attempts": attempts, "last_error": e.to_string()}).to_string(),
					],
				)
				.await?;
				if attempts < MAX_ATTEMPTS {
					tracing::warn!(job = id, step = n, attempts, "step will be retried: {e}");
					tokio::time::sleep(Duration::from_millis(500 * attempts.min(10))).await;
					return Ok(());
				}
				return fail(h, id, n, &e).await;
			}
			Err(e) => return fail(h, id, n, &e).await,
		}
	}
	// A cancel that arrived after the last step began still ends the job as cancelled.
	cancelling = cancelling || state_of(h, id).await? == "cancelling";
	h.query(
		"update lepis.job set state = $2, updated_at = now(), finished_at = now() where id = $1",
		&[&id.to_string(), if cancelling { "cancelled" } else { "done" }],
	)
	.await?;
	tracing::info!(job = id, "job finished");
	Ok(())
}

async fn state_of(h: &mut Pg, id: i64) -> Result<String, OpError> {
	Ok(h
		.value("select state from lepis.job where id = $1", &[&id.to_string()])
		.await?
		.unwrap_or_default())
}

async fn fail(h: &mut Pg, id: i64, n: i32, e: &OpError) -> Result<(), OpError> {
	tracing::warn!(job = id, step = n, "job failed: {e}");
	h.query(
		"update lepis.job_step set state = 'failed', finished_at = now(), \
		detail = detail || jsonb_build_object('error', $3::text) where job_id = $1 and n = $2",
		&[&id.to_string(), &n.to_string(), &e.to_string()],
	)
	.await?;
	h.query(
		"update lepis.job set state = 'failed', error = $2, updated_at = now(), finished_at = now() where id = $1",
		&[&id.to_string(), &e.to_string()],
	)
	.await?;
	Ok(())
}

/// Plans `op` and writes it as a job. Returns the job id and the plan.
pub async fn submit(
	app: &Arc<App>,
	h: &mut Pg,
	op: &Op,
	args: &Value,
) -> Result<(i64, Value), OpError> {
	if !ensure_jobs_schema(h).await? {
		return Err(OpError::refused(
			Kind::NoCatalog, "this database has no Lepis catalog (schema lepis)",
		));
	}
	let cluster: Value = h
		.value("select settings::text from lepis.cluster where id = 1", &[])
		.await?
		.and_then(|v| serde_json::from_str(&v).ok())
		.unwrap_or(Value::Null);
	let settings = Settings::from_json(&cluster, args);
	let planned = plan::plan(app, h, op, settings).await?;
	if planned.steps.is_empty() {
		return Err(OpError::refused(Kind::NothingToDo, "there is nothing to do"));
	}
	let mut sql = format!(
		"begin; insert into lepis.job (op, args, plan) values ({}, {}::jsonb, {}::jsonb) returning id;",
		quote_literal(op.name()),
		quote_literal(&args.to_string()),
		quote_literal(&planned.summary.to_string()),
	);
	for (n, st) in planned.steps.iter().enumerate() {
		sql.push_str(&format!(
			"insert into lepis.job_step (job_id, n, kind, args) values (currval(pg_get_serial_sequence('lepis.job', 'id')), {n}, {}, {}::jsonb);",
			quote_literal(st.kind()),
			quote_literal(&st.args().to_string()),
		));
	}
	sql.push_str("commit;");
	let rows = match h.simple(&sql).await {
		Ok(r) => r,
		Err(e) => {
			let _ = h.simple("rollback").await;
			return Err(e);
		}
	};
	let id = rows
		.first()
		.and_then(|r| r.first().cloned().flatten())
		.and_then(|v| v.parse().ok())
		.ok_or_else(|| OpError::new("the job was not written"))?;
	Ok((id, planned.summary))
}

/// Asks a job to stop: a pending one is cancelled at once, a running one at its next step
/// boundary (a move before its cutover rolls back; after it, it finishes).
pub async fn cancel(h: &mut Pg, id: i64) -> Result<String, OpError> {
	let rows = h
		.query(
			"update lepis.job set state = case state when 'pending' then 'cancelled' \
			when 'running' then 'cancelling' else state end, updated_at = now(), \
			finished_at = case when state = 'pending' then now() else finished_at end \
			where id = $1 returning state",
			&[&id.to_string()],
		)
		.await?;
	rows.first()
		.and_then(|r| r.first().cloned().flatten())
		.ok_or_else(|| OpError::refused(Kind::NoSuchJob, format!("there is no job {id}")))
}

/// Runs a failed job again from the step that failed.
pub async fn resume(h: &mut Pg, id: i64) -> Result<String, OpError> {
	if h
		.value("select state from lepis.job where id = $1", &[&id.to_string()])
		.await?
		.is_none()
	{
		return Err(OpError::refused(Kind::NoSuchJob, format!("there is no job {id}")));
	}
	let rows = h
		.query(
			"with j as (update lepis.job set state = 'running', error = null, finished_at = null, \
			updated_at = now() where id = $1 and state = 'failed' returning id) \
			update lepis.job_step s set state = 'pending', finished_at = null, \
			detail = s.detail - 'error' - 'step_attempts' from j \
			where s.job_id = j.id and s.state = 'failed' returning s.n",
			&[&id.to_string()],
		)
		.await?;
	if rows.is_empty() {
		return Err(OpError::refused(Kind::JobNotFailed, format!(
			"job {id} is not a failed job; only a failed job is resumed (a running one resumes by itself)"
		)));
	}
	Ok("running".into())
}

/// A job and its steps, as the admin API shows them.
pub async fn show(h: &mut Pg, id: i64) -> Result<Value, OpError> {
	let job = h
		.query(
			"select row_to_json(j)::text from (select id, op, args, plan, state, error, runner, \
			created_at, updated_at, finished_at from lepis.job where id = $1) j",
			&[&id.to_string()],
		)
		.await?;
	let Some(text) = job.first().and_then(|r| r.first().cloned().flatten()) else {
		return Err(OpError::refused(Kind::NoSuchJob, format!("there is no job {id}")));
	};
	let mut v: Value = serde_json::from_str(&text).map_err(|e| OpError::new(e.to_string()))?;
	let steps = h
		.query(
			"select coalesce(json_agg(s order by n), '[]')::text from (select n, kind, args, state, detail, \
			started_at, finished_at from lepis.job_step where job_id = $1) s",
			&[&id.to_string()],
		)
		.await?;
	let steps: Value = steps
		.first()
		.and_then(|r| r.first().cloned().flatten())
		.and_then(|t| serde_json::from_str(&t).ok())
		.unwrap_or(json!([]));
	v["steps"] = steps;
	Ok(v)
}

pub async fn list(h: &mut Pg, limit: i64) -> Result<Value, OpError> {
	let rows = h
		.query(
			"select coalesce(json_agg(j order by id desc), '[]')::text from (select id, op, state, error, \
			created_at, finished_at, (select count(*) from lepis.job_step s where s.job_id = job.id) steps, \
			(select count(*) from lepis.job_step s where s.job_id = job.id and s.state in ('done', 'skipped')) steps_done \
			from lepis.job order by id desc limit $1) j",
			&[&limit.to_string()],
		)
		.await?;
	Ok(rows
		.first()
		.and_then(|r| r.first().cloned().flatten())
		.and_then(|t| serde_json::from_str(&t).ok())
		.unwrap_or(json!([])))
}
