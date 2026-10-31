//! The autopilot: the job leader asks the advisor on an interval and, when nothing
//! else is running, either records its top recommendation (`advise`) or submits it as an ordinary
//! job (`auto`), one at a time. **Off unless a person turns it on**: `autopilot` in the cluster's
//! settings is `off`, `advise` or `auto`, and anything else, a missing one included, is off.
//!
//! A job it starts is an ordinary job whose arguments carry `"actor": "lepis"` and the advisor's
//! reason, so the job log says which jobs it ran and why. It never undoes a person: a keyspace a
//! person placed by hand (a range moved, a range split onto a node, a tenant pinned) is left to
//! them, an advice that needs something only a person can give (a new node's address) is never
//! taken, and after a job of its own fails it waits until a person has looked at it.
//!
//! What it decided last is kept in `lepis.autopilot` (one row, made by the autopilot itself) and
//! answered by `GET /v1/autopilot`.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use super::spec::Op;
use super::{OpError, Pg, advisor, engine};
use crate::catalog::quote_literal;
use crate::server::App;

/// How often the autopilot asks the advisor, unless `autopilot_interval_ms` says otherwise.
/// Chosen: each ask samples the write counters for `advice_sample_ms` (5 s).
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(60);
const MIN_INTERVAL: Duration = Duration::from_secs(5);

/// The operations that place data by hand: after one of a person's, its keyspace is theirs.
const HAND: [&str; 3] = ["range.move", "range.split", "tenant.pin"];

/// The one row of what it decided last, made by the autopilot on its first run.
pub const AUTOPILOT_SQL: &str = "create table if not exists lepis.autopilot (\
	id int primary key default 1 check (id = 1), \
	mode text not null, \
	decided_at timestamptz not null default now(), \
	decision jsonb not null default '{}')";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
	Off,
	Advise,
	Auto,
}

impl Mode {
	/// The cluster's `autopilot` setting; anything but `advise` or `auto` is off.
	pub fn from_settings(cluster: &Value) -> Mode {
		match cluster.get("autopilot").and_then(Value::as_str) {
			Some("advise") => Mode::Advise,
			Some("auto") => Mode::Auto,
			_ => Mode::Off,
		}
	}

	pub fn name(self) -> &'static str {
		match self {
			Mode::Off => "off",
			Mode::Advise => "advise",
			Mode::Auto => "auto",
		}
	}
}

/// `autopilot_interval_ms`, never under five seconds.
pub fn interval(cluster: &Value) -> Duration {
	cluster
		.get("autopilot_interval_ms")
		.and_then(Value::as_u64)
		.map_or(DEFAULT_INTERVAL, Duration::from_millis)
		.max(MIN_INTERVAL)
}

/// A job as the autopilot reads the log, newest first.
#[derive(Clone, Debug, PartialEq)]
pub struct JobSeen {
	pub id: i64,
	pub op: String,
	pub args: Value,
	pub state: String,
	pub error: Option<String>,
}

impl JobSeen {
	fn by_lepis(&self) -> bool {
		self.args.get("actor").and_then(Value::as_str) == Some("lepis")
	}
}

#[derive(Clone, Debug, PartialEq)]
pub enum Next {
	Off,
	/// A job is pending or running: one at a time.
	Busy(String),
	/// A job of its own failed: it waits for a person.
	Held(String),
	/// Nothing to take, and why.
	Nothing(String),
	/// `advise`: what it would run.
	Propose {
		request: Value,
		reason: String,
	},
	/// `auto`: what it runs.
	Submit {
		request: Value,
		reason: String,
	},
}

impl Next {
	pub fn to_json(&self) -> Value {
		match self {
			Next::Off => json!({"kind": "off"}),
			Next::Busy(w) => json!({"kind": "busy", "why": w}),
			Next::Held(w) => json!({"kind": "held", "why": w}),
			Next::Nothing(w) => json!({"kind": "nothing", "why": w}),
			Next::Propose { request, reason } => {
				json!({"kind": "propose", "request": request, "why": reason})
			}
			Next::Submit { request, reason } => {
				json!({"kind": "submit", "request": request, "why": reason})
			}
		}
	}
}

/// A request without what the autopilot adds to it, for comparing one with another.
fn bare(v: &Value) -> Value {
	let mut v = v.clone();
	if let Some(o) = v.as_object_mut() {
		o.remove("actor");
		o.remove("reason");
	}
	v
}

/// The keyspaces a person placed by hand, from the log (a cancelled job placed nothing).
pub fn hand_placed(jobs: &[JobSeen]) -> BTreeSet<String> {
	jobs.iter()
		.filter(|j| !j.by_lepis() && j.state != "cancelled" && HAND.contains(&j.op.as_str()))
		.filter(|j| j.op != "range.split" || j.args.get("to").is_some_and(|t| !t.is_null()))
		.filter_map(|j| {
			j.args
				.get("keyspace")
				.and_then(Value::as_str)
				.map(str::to_string)
		})
		.collect()
}

/// What the autopilot does next, from its mode, the advisor's answer (`advisor::advise`) and the
/// job log, newest first. Pure.
pub fn next(mode: Mode, advice: &Value, jobs: &[JobSeen]) -> Next {
	if mode == Mode::Off {
		return Next::Off;
	}
	if let Some(j) = jobs
		.iter()
		.find(|j| matches!(j.state.as_str(), "pending" | "running" | "cancelling"))
	{
		return Next::Busy(format!(
			"job {} ({}) is {}: one job at a time",
			j.id, j.op, j.state
		));
	}
	if let Some(j) = jobs.first().filter(|j| j.by_lepis() && j.state == "failed") {
		return Next::Held(format!(
			"job {} ({}), which it started, failed: {}. It waits until a person resumes, cancels or follows it with a job of their own",
			j.id,
			j.op,
			j.error.as_deref().unwrap_or("no error recorded")
		));
	}
	let hand = hand_placed(jobs);
	let failed: Vec<Value> = jobs
		.iter()
		.filter(|j| j.by_lepis() && j.state == "failed")
		.map(|j| bare(&j.args))
		.collect();
	let mut passed: Vec<String> = Vec::new();
	for a in advice
		.get("advice")
		.and_then(Value::as_array)
		.into_iter()
		.flatten()
	{
		let request = &a["request"];
		let op = request["op"].as_str().unwrap_or("?");
		let needs = a["needs"].as_array().map_or(0, Vec::len);
		let keyspace = request.get("keyspace").and_then(Value::as_str);
		let why = if needs > 0 {
			Some(format!("{op} needs what only a person can give"))
		} else if a.get("plan").is_none_or(Value::is_null) {
			Some(format!("{op} could not be planned"))
		} else if let Some(k) = keyspace.filter(|k| hand.contains(*k)) {
			Some(format!(
				"{op} in keyspace {k}, which a person placed by hand"
			))
		} else if failed.contains(&bare(request)) {
			Some(format!("{op} failed when it ran it before"))
		} else {
			None
		};
		if let Some(w) = why {
			passed.push(w);
			continue;
		}
		let reason = a["reason"].as_str().unwrap_or_default().to_string();
		let request = request.clone();
		return match mode {
			Mode::Advise => Next::Propose { request, reason },
			_ => Next::Submit { request, reason },
		};
	}
	if passed.is_empty() {
		Next::Nothing(
			advice["summary"]
				.as_str()
				.unwrap_or("the advisor recommends nothing")
				.to_string(),
		)
	} else {
		Next::Nothing(format!(
			"every recommendation was passed over: {}",
			passed.join("; ")
		))
	}
}

/// The job log as `next` reads it: the most recent jobs, newest first.
async fn jobs(h: &mut Pg) -> Result<Vec<JobSeen>, OpError> {
	Ok(
		h.simple(
			"select id, op, args::text, state, error from lepis.job order by id desc limit 50",
		)
		.await?
		.into_iter()
		.map(|r| {
			let text = |i: usize| r.get(i).cloned().flatten();
			JobSeen {
				id: text(0).and_then(|v| v.parse().ok()).unwrap_or(0),
				op: text(1).unwrap_or_default(),
				args: text(2)
					.and_then(|v| serde_json::from_str(&v).ok())
					.unwrap_or(Value::Null),
				state: text(3).unwrap_or_default(),
				error: text(4),
			}
		})
		.collect(),
	)
}

async fn record(h: &mut Pg, mode: Mode, decision: &Value) -> Result<(), OpError> {
	h.simple(&format!(
		"insert into lepis.autopilot (id, mode, decided_at, decision) values (1, {m}, now(), {d}::jsonb) \
		on conflict (id) do update set mode = excluded.mode, decided_at = now(), decision = excluded.decision",
		m = quote_literal(mode.name()),
		d = quote_literal(&decision.to_string()),
	))
	.await
	.map(|_| ())
}

/// One pass, by the job leader while it has no job to run. Off, it does nothing at all.
pub async fn tick(app: &Arc<App>, h: &mut Pg, cluster: &Value) -> Result<Next, OpError> {
	let mode = Mode::from_settings(cluster);
	if mode == Mode::Off {
		return Ok(Next::Off);
	}
	h.simple(AUTOPILOT_SQL).await?;
	let log = jobs(h).await?;
	// Busy or held needs no advice: asking would sample the write counters for nothing.
	let pre = next(mode, &json!({"advice": []}), &log);
	let decided = match pre {
		Next::Busy(_) | Next::Held(_) => pre,
		_ => {
			let advice = advisor::advise(app, h, None).await?;
			next(mode, &advice, &log)
		}
	};
	let mut decision = decided.to_json();
	if let Next::Submit { request, reason } = &decided {
		let mut args = request.clone();
		args["actor"] = json!("lepis");
		args["reason"] = json!(reason);
		match Op::from_json(&args) {
			Ok(op) => match engine::submit(app, h, &op, &args).await {
				Ok((id, _)) => {
					tracing::info!(job = id, op = op.name(), %reason, "autopilot started a job");
					decision["job"] = json!(id);
				}
				Err(e) => decision["error"] = json!(e.message),
			},
			Err(e) => decision["error"] = json!(e.message),
		}
	}
	record(h, mode, &decision).await?;
	Ok(decided)
}

/// `GET /v1/autopilot`: the mode, and what it decided last (null before its first pass).
pub async fn show(h: &mut Pg) -> Result<Value, OpError> {
	let cluster: Value = h
		.value("select settings::text from lepis.cluster where id = 1", &[])
		.await?
		.and_then(|v| serde_json::from_str(&v).ok())
		.unwrap_or(Value::Null);
	let last = if h
		.value("select to_regclass('lepis.autopilot') is not null", &[])
		.await?
		.as_deref()
		== Some("t")
	{
		h.value(
			"select json_build_object('mode', mode, 'at', decided_at, 'decision', decision)::text from lepis.autopilot where id = 1",
			&[],
		)
		.await?
		.and_then(|v| serde_json::from_str(&v).ok())
		.unwrap_or(Value::Null)
	} else {
		Value::Null
	};
	Ok(json!({
		"mode": Mode::from_settings(&cluster).name(),
		"interval_ms": interval(&cluster).as_millis() as u64,
		"last": last,
	}))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn job(id: i64, op: &str, args: Value, state: &str) -> JobSeen {
		JobSeen {
			id,
			op: op.into(),
			args,
			state: state.into(),
			error: (state == "failed").then(|| "node 3 did not answer".into()),
		}
	}

	fn advice(items: Value) -> Value {
		json!({"summary": "1 recommendation.", "advice": items})
	}

	fn mv(ks: &str) -> Value {
		json!({"request": {"op": "range.move", "keyspace": ks, "range": "0", "to": 3}, "reason": "node 1 holds 80%", "needs": [], "plan": {"cutovers": 1}})
	}

	#[test]
	fn off_unless_set_and_anything_else_is_off() {
		for s in [
			json!({}),
			json!(null),
			json!({"autopilot": "on"}),
			json!({"autopilot": true}),
			json!({"autopilot": "AUTO"}),
		] {
			assert_eq!(Mode::from_settings(&s), Mode::Off, "{s}");
		}
		assert_eq!(
			Mode::from_settings(&json!({"autopilot": "advise"})),
			Mode::Advise
		);
		assert_eq!(
			Mode::from_settings(&json!({"autopilot": "auto"})),
			Mode::Auto
		);
		assert_eq!(
			next(Mode::Off, &advice(json!([mv("acct")])), &[]),
			Next::Off
		);
		assert_eq!(interval(&json!({})), DEFAULT_INTERVAL);
		assert_eq!(
			interval(&json!({"autopilot_interval_ms": 10})),
			MIN_INTERVAL
		);
	}

	#[test]
	fn it_takes_the_first_recommendation_it_may_run_and_says_so_in_advise() {
		let a = advice(json!([mv("acct")]));
		assert_eq!(
			next(Mode::Advise, &a, &[]),
			Next::Propose {
				request: mv("acct")["request"].clone(),
				reason: "node 1 holds 80%".into()
			}
		);
		assert!(matches!(next(Mode::Auto, &a, &[]), Next::Submit { .. }));
	}

	#[test]
	fn one_job_at_a_time_and_a_failure_of_its_own_waits_for_a_person() {
		let a = advice(json!([mv("acct")]));
		let running = [job(7, "rebalance", json!({"op": "rebalance"}), "running")];
		assert!(matches!(next(Mode::Auto, &a, &running), Next::Busy(w) if w.contains("job 7")));
		let failed = [job(
			8,
			"range.move",
			json!({"op": "range.move", "keyspace": "acct", "range": "0", "to": 3, "actor": "lepis"}),
			"failed",
		)];
		assert!(
			matches!(next(Mode::Auto, &a, &failed), Next::Held(w) if w.contains("node 3 did not answer"))
		);
		// A person followed it with a job of their own: it carries on, but never runs what failed again.
		let after = [
			job(9, "verify", json!({"op": "verify"}), "done"),
			failed[0].clone(),
		];
		assert!(
			matches!(next(Mode::Auto, &a, &after), Next::Nothing(w) if w.contains("failed when it ran it before"))
		);
	}

	#[test]
	fn a_keyspace_placed_by_hand_is_left_to_the_person() {
		let a = advice(json!([mv("acct"), mv("device")]));
		let log = [
			job(
				3,
				"range.move",
				json!({"op": "range.move", "keyspace": "acct", "range": "0", "to": 2}),
				"done",
			),
			// A split that names no node places nothing; a cancelled job placed nothing.
			job(
				2,
				"range.split",
				json!({"op": "range.split", "keyspace": "device", "range": "0"}),
				"done",
			),
			job(
				1,
				"tenant.pin",
				json!({"op": "tenant.pin", "keyspace": "device", "value": "4"}),
				"cancelled",
			),
		];
		assert_eq!(hand_placed(&log), BTreeSet::from(["acct".to_string()]));
		match next(Mode::Auto, &a, &log) {
			Next::Submit { request, .. } => assert_eq!(request["keyspace"], "device"),
			other => panic!("{other:?}"),
		}
		// Its own moves are not a person's.
		let own = [job(
			4,
			"range.move",
			json!({"op": "range.move", "keyspace": "device", "range": "0", "to": 2, "actor": "lepis"}),
			"done",
		)];
		assert!(hand_placed(&own).is_empty());
	}

	#[test]
	fn what_needs_a_person_or_has_no_plan_is_passed_over_with_the_reason() {
		let a = advice(json!([
			{"request": {"op": "node.add", "name": "n4", "host": null}, "reason": "the nodes are full", "needs": ["host"], "plan": null},
			{"request": {"op": "range.split", "keyspace": "acct", "range": "0", "at": "5", "to": 2}, "reason": "big", "needs": [], "plan": null, "plan_error": {"kind": "failed"}},
		]));
		match next(Mode::Auto, &a, &[]) {
			Next::Nothing(w) => {
				assert!(
					w.contains("node.add needs what only a person can give"),
					"{w}"
				);
				assert!(w.contains("range.split could not be planned"), "{w}");
			}
			other => panic!("{other:?}"),
		}
		assert_eq!(
			next(Mode::Auto, &advice(json!([])), &[]),
			Next::Nothing("1 recommendation.".into())
		);
	}
}
