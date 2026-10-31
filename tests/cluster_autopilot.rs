//! The autopilot against a real three-node cluster: off unless a person turns it
//! on, `advise` records what it would run and runs nothing, `auto` runs the advisor's
//! recommendation as an ordinary job marked as its own, and a keyspace a person placed by hand is
//! left to them.
//!
//! Needs `LEPIS_IT_HOME`, `LEPIS_IT_DATA_NODES` and `LEPIS_ORACLE_REFERENCE` (`scripts/it.sh`;
//! `LEPIS_IT_ONLY=cluster_autopilot` runs just this file).

#[macro_use]
mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use lepis::config::Config;
use lepis::server::{self, App};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TOKEN: &str = "an-admin-token-for-the-autopilot-test";

struct Admin {
	addr: String,
}

impl Admin {
	async fn call(&self, method: &str, path: &str, body: Option<&Value>) -> (u16, Value) {
		let mut s = tokio::net::TcpStream::connect(&self.addr).await.unwrap();
		let body = body.map(|b| b.to_string()).unwrap_or_default();
		let req = format!(
			"{method} {path} HTTP/1.1\r\nhost: lepis\r\nauthorization: Bearer {TOKEN}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
			body.len()
		);
		s.write_all(req.as_bytes()).await.unwrap();
		let mut out = Vec::new();
		s.read_to_end(&mut out).await.unwrap();
		let text = String::from_utf8_lossy(&out);
		let status: u16 = text[9..12].parse().unwrap();
		let json = text.split_once("\r\n\r\n").map_or("", |(_, b)| b);
		(status, serde_json::from_str(json).unwrap_or(Value::Null))
	}

	async fn ok(&self, method: &str, path: &str, body: Option<&Value>) -> Value {
		let (status, v) = self.call(method, path, body).await;
		assert!(status < 300, "{method} {path} {body:?}: {status} {v}");
		v
	}

	async fn run(&self, op: Value) -> Value {
		let id = self.ok("POST", "/v1/jobs", Some(&op)).await["job"]
			.as_i64()
			.unwrap();
		self.wait(id).await
	}

	async fn wait(&self, id: i64) -> Value {
		let deadline = Instant::now() + Duration::from_secs(300);
		loop {
			let j = self.ok("GET", &format!("/v1/jobs/{id}"), None).await;
			match j["state"].as_str() {
				Some("done") => return j,
				Some("failed" | "cancelled") => panic!("job {id}: {j:#}"),
				_ if Instant::now() > deadline => panic!("job {id} did not finish: {j}"),
				_ => tokio::time::sleep(Duration::from_millis(200)).await,
			}
		}
	}

	async fn jobs(&self) -> Vec<Value> {
		self.ok("GET", "/v1/jobs", None).await["jobs"]
			.as_array()
			.cloned()
			.unwrap_or_default()
	}

	/// Waits for the autopilot's record to say `kind`, at most `secs`.
	async fn decision(&self, kind: &str, secs: u64) -> Value {
		let deadline = Instant::now() + Duration::from_secs(secs);
		loop {
			let a = self.ok("GET", "/v1/autopilot", None).await;
			if a["last"]["decision"]["kind"] == kind {
				return a;
			}
			assert!(Instant::now() < deadline, "no {kind} decision: {a:#}");
			tokio::time::sleep(Duration::from_millis(500)).await;
		}
	}
}

#[tokio::test]
async fn the_autopilot_runs_only_when_asked_and_leaves_hand_placements_alone() {
	let b = need_bed!();
	let nodes = node_ids(&b);
	for (_, a) in &nodes {
		connect(a, "postgres", "x")
			.await
			.batch_execute("drop schema if exists apx cascade; drop schema if exists apy cascade")
			.await
			.unwrap();
	}
	let home = connect(&b.home, "postgres", "x").await;
	home.batch_execute(
		"create schema apx; create schema apy;
		create table apx.t (k bigint primary key, v text);
		create table apy.t (k bigint primary key, v text);
		insert into apx.t select g, repeat('x', 200) from generate_series(1, 20000) g;
		insert into apy.t select g, repeat('y', 200) from generate_series(1, 20000) g;
		analyze apx.t, apy.t;",
	)
	.await
	.unwrap();

	let config: HashMap<String, String> = [
		("LEPIS_HOME", b.home.clone()),
		("LEPIS_HOME_SSLMODE", "disable".into()),
		("LEPIS_SERVICE_USER", "postgres".into()),
		("LEPIS_SERVICE_PASSWORD", "x".into()),
	]
	.into_iter()
	.map(|(k, v)| (k.to_string(), v))
	.collect();
	let app: Arc<App> = App::new(Config::from_map(&config).unwrap()).unwrap();
	let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
	tokio::spawn(server::serve(app.clone(), l));
	let al = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
	let admin = Admin {
		addr: al.local_addr().unwrap().to_string(),
	};
	// The admin server starts the job runner, which runs the autopilot.
	tokio::spawn(lepis::admin::serve_admin(
		app.clone(),
		al,
		TOKEN.into(),
		Arc::new(tokio::sync::Notify::new()),
	));
	tokio::time::sleep(Duration::from_millis(500)).await;

	// Two keyspaces with every range on the home node, each with a table: lopsided on purpose.
	for ks in ["apx", "apy"] {
		admin
			.run(
				json!({"op": "keyspace.create", "name": ks, "key_type": "bigint", "ranges": 4, "nodes": [1]}),
			)
			.await;
		admin
			.run(
				json!({"op": "table.distribute", "table": format!("{ks}.t"), "column": "k", "keyspace": ks}),
			)
			.await;
	}
	// The advisor sees any skew at once: no size floor, no write sampling, a pass every 5 s.
	admin
		.ok(
			"POST",
			"/v1/settings",
			Some(
				&json!({"advice_min_bytes": 0, "advice_sample_ms": 0, "autopilot_interval_ms": 5000}),
			),
		)
		.await;
	assert!(
		!admin.ok("GET", "/v1/advice", None).await["advice"]
			.as_array()
			.unwrap()
			.is_empty(),
		"the advisor should recommend something for two lopsided keyspaces"
	);

	// The router says it has an autopilot, off by default: what a client checks before offering it.
	let ops = admin.ok("GET", "/v1/ops", None).await;
	assert_eq!(ops["autopilot_settings"]["autopilot"], "off", "{ops}");

	// Off unless asked: several intervals pass and it neither runs nor records anything.
	let before = admin.jobs().await.len();
	tokio::time::sleep(Duration::from_secs(12)).await;
	let a = admin.ok("GET", "/v1/autopilot", None).await;
	assert_eq!(
		(a["mode"].as_str(), &a["last"]),
		(Some("off"), &Value::Null),
		"{a}"
	);
	assert_eq!(admin.jobs().await.len(), before);
	let (code, e) = admin
		.call("POST", "/v1/settings", Some(&json!({"autopilot": "yes"})))
		.await;
	assert_eq!(
		(code, e["error"]["kind"].as_str()),
		(400, Some("bad_setting")),
		"{e}"
	);

	// Advise: what it would run, recorded; nothing runs.
	admin
		.ok(
			"POST",
			"/v1/settings",
			Some(&json!({"autopilot": "advise"})),
		)
		.await;
	let a = admin.decision("propose", 30).await;
	assert_eq!(
		a["last"]["decision"]["request"]["op"], "range.move",
		"{a:#}"
	);
	assert_eq!(admin.jobs().await.len(), before);

	// A person moves one of apx's ranges by hand: apx is theirs from now on.
	let st = admin.ok("GET", "/v1/status", None).await;
	let lo = st["cluster"]["keyspaces"]
		.as_array()
		.and_then(|ks| ks.iter().find(|k| k["name"] == "apx"))
		.and_then(|k| k["ranges"][0]["lo"].as_str().map(str::to_string))
		.or_else(|| {
			st["keyspaces"]
				.as_array()
				.and_then(|ks| ks.iter().find(|k| k["name"] == "apx"))
				.and_then(|k| k["ranges"][0]["lo"].as_str().map(str::to_string))
		})
		.unwrap_or_else(|| panic!("apx's first range in {st:#}"));
	admin
		.run(json!({"op": "range.move", "keyspace": "apx", "range": lo, "to": 2}))
		.await;

	// Auto: it runs apy's recommendation as its own job, never apx's.
	admin
		.ok("POST", "/v1/settings", Some(&json!({"autopilot": "auto"})))
		.await;
	let deadline = Instant::now() + Duration::from_secs(120);
	let own = loop {
		let jobs = admin.jobs().await;
		if let Some(j) = jobs.iter().find(|j| j["actor"] == "lepis") {
			break j.clone();
		}
		assert!(
			Instant::now() < deadline,
			"the autopilot ran nothing: {:#}",
			admin.ok("GET", "/v1/autopilot", None).await
		);
		tokio::time::sleep(Duration::from_millis(500)).await;
	};
	let done = admin.wait(own["id"].as_i64().unwrap()).await;
	assert_eq!(done["args"]["keyspace"], "apy", "{done:#}");
	assert!(
		done["args"]["reason"]
			.as_str()
			.is_some_and(|r| !r.is_empty()),
		"{done:#}"
	);
	// Every job it ran is apy's, and apx's ranges are where the person left them.
	tokio::time::sleep(Duration::from_secs(12)).await;
	admin
		.ok("POST", "/v1/settings", Some(&json!({"autopilot": "off"})))
		.await;
	for j in admin.jobs().await.iter().filter(|j| j["actor"] == "lepis") {
		let id = j["id"].as_i64().unwrap();
		let full = admin.ok("GET", &format!("/v1/jobs/{id}"), None).await;
		assert_ne!(
			full["args"]["keyspace"], "apx",
			"the autopilot moved a hand-placed keyspace: {full:#}"
		);
	}
	admin.run(json!({"op": "verify"})).await;
}
