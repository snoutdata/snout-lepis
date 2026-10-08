//! Phase 10's advisor against the real three-node cluster: a keyspace whose rows pile onto one
//! range gets a recommendation to split that range, as a request the admin API takes, with the
//! bound inside the range, the half going to another node, and its plan. Nothing runs.
//!
//! Needs `LEPIS_IT_HOME`, `LEPIS_IT_DATA_NODES` and `LEPIS_ORACLE_REFERENCE` (`scripts/it.sh`;
//! `LEPIS_IT_ONLY=cluster_advice` runs just this file).

#[macro_use]
mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use lepis::config::Config;
use lepis::server::{self, App};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TOKEN: &str = "an-admin-token-for-the-advice-test";
const SEED: u64 = 777;

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

	/// Runs one operation as a job and waits for it to finish.
	async fn op(&self, op: Value) {
		let (status, v) = self.call("POST", "/v1/jobs", Some(&op)).await;
		assert_eq!(status, 201, "run {op}: {v}");
		let id = v["job"].as_i64().unwrap();
		let deadline = Instant::now() + Duration::from_secs(300);
		loop {
			let (_, j) = self.call("GET", &format!("/v1/jobs/{id}"), None).await;
			match j["state"].as_str() {
				Some("done") => return,
				Some("failed" | "cancelled") => panic!("{op}: {j:#}"),
				_ if Instant::now() > deadline => panic!("job {id} did not finish: {j}"),
				_ => tokio::time::sleep(Duration::from_millis(200)).await,
			}
		}
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_range_holding_most_of_the_rows_gets_a_split() {
	let b = need_bed!();
	let nodes = node_ids(&b);
	for (_, a) in &nodes {
		let admin = connect(a, "postgres", "x").await;
		admin
			.batch_execute("drop schema if exists advx cascade")
			.await
			.unwrap();
	}
	let home = connect(&b.home, "postgres", "x").await;
	home.batch_execute(
		"create schema advx;
		create table advx.t (k bigint not null, id bigint not null, pad text, primary key (k, id));
		insert into advx.t select (g % 300) + 1, g, 'seed' from generate_series(1, 3000) g;",
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
	tokio::spawn(lepis::admin::serve_admin(
		app.clone(),
		al,
		TOKEN.into(),
		Arc::new(tokio::sync::Notify::new()),
	));
	tokio::time::sleep(Duration::from_millis(500)).await;

	admin
		.op(json!({"op": "keyspace.create", "name": "advk", "key_type": "bigint", "seed": SEED, "ranges": 3, "nodes": [1, 2, 3]}))
		.await;
	admin
		.op(json!({"op": "table.distribute", "table": "advx.t", "column": "k", "keyspace": "advk"}))
		.await;

	// The skew: 400,000 candidate keys, and only the third of them that hash into node 2's range
	// is written, straight to node 2 (its fence admits exactly those).
	let (_, st) = admin.call("GET", "/v1/status", None).await;
	let ks = st["keyspaces"]
		.as_array()
		.unwrap()
		.iter()
		.find(|k| k["name"] == "advk")
		.unwrap()
		.clone();
	let range = ks["ranges"]
		.as_array()
		.unwrap()
		.iter()
		.find(|r| r["node"] == 2)
		.unwrap()
		.clone();
	let lo: i64 = range["lo"].as_str().unwrap().parse().unwrap();
	let hi: i64 = range["hi"].as_str().unwrap().parse().unwrap();
	let n2 = connect(&nodes[1].1, "postgres", "x").await;
	let version: i32 = n2
		.query_one("select current_setting('server_version_num')::int", &[])
		.await
		.unwrap()
		.get(0);
	let expr = KeyType::Int8
		.sql_expression("g", SEED, version as u32)
		.unwrap();
	n2.batch_execute(&format!(
		"insert into advx.t select g, 0, repeat('x', 200) from generate_series(1000000, 1400000) g \
		where {expr} between {lo} and {hi}"
	))
	.await
	.unwrap();
	for (_, a) in &nodes {
		connect(a, "postgres", "x")
			.await
			.batch_execute("analyze advx.t")
			.await
			.unwrap();
	}

	// The defaults leave a test-sized keyspace alone; the threshold is a setting.
	let (code, v) = admin
		.call(
			"POST",
			"/v1/settings",
			Some(&json!({"advice_min_bytes": 1})),
		)
		.await;
	assert_eq!(code, 200, "{v}");
	let (code, e) = admin.call("GET", "/v1/advice?sample_ms=soon", None).await;
	assert_eq!(
		(code, e["error"]["kind"].as_str()),
		(400, Some("bad_setting")),
		"{e}"
	);

	let t = Instant::now();
	let (code, advice) = admin.call("GET", "/v1/advice?sample_ms=0", None).await;
	assert_eq!(code, 200, "{advice}");
	eprintln!(
		"advice in {} ms: {}",
		t.elapsed().as_millis(),
		advice["summary"]
	);
	for a in advice["advice"].as_array().unwrap() {
		eprintln!("  {}: {}", a["op"], a["reason"]);
	}
	let facts: Vec<&Value> = advice["facts"]["ranges"]
		.as_array()
		.unwrap()
		.iter()
		.filter(|r| r["keyspace"] == "advk")
		.collect();
	let heavy = facts
		.iter()
		.find(|r| r["lo"] == json!(lo.to_string()))
		.unwrap();
	let others: u64 = facts
		.iter()
		.filter(|r| r["node"] != 2)
		.map(|r| r["bytes"].as_u64().unwrap())
		.sum();
	assert!(heavy["bytes"].as_u64().unwrap() > 10 * others, "{facts:?}");
	assert!(heavy["sampled_rows"].as_u64().unwrap() > 1000, "{heavy}");

	let split = advice["advice"]
		.as_array()
		.unwrap()
		.iter()
		.find(|a| a["request"]["keyspace"] == "advk")
		.unwrap_or_else(|| panic!("no advice for advk: {advice:#}"));
	let req = &split["request"];
	assert_eq!(req["op"], "range.split", "{split:#}");
	assert_eq!(req["range"], json!(lo.to_string()));
	assert_ne!(req["to"], 2);
	let at: i64 = req["at"].as_str().unwrap().parse().unwrap();
	assert!(at > lo && at <= hi, "{at} outside [{lo}, {hi}]");
	assert_eq!(split["metric"], "size");
	assert!(split["needs"].as_array().unwrap().is_empty());
	// Its plan is the one POST /v1/plan gives: half of node 2's rows go to the other node.
	let plan = &split["plan"];
	assert_eq!(plan["op"], "range.split", "{split:#}");
	assert_eq!(plan["moves"][0]["source"], 2, "{plan}");
	let moving = plan["estimated_rows"].as_u64().unwrap();
	let rows = heavy["rows"].as_u64().unwrap();
	assert!(
		moving > rows / 5 && moving < rows * 4 / 5,
		"moving {moving} of {rows}"
	);
	// And nothing ran.
	let (_, jobs) = admin.call("GET", "/v1/jobs", None).await;
	assert!(
		jobs["jobs"]
			.as_array()
			.unwrap()
			.iter()
			.all(|j| j["op"] != "range.split"),
		"{jobs}"
	);

	let (code, _) = admin
		.call(
			"POST",
			"/v1/settings",
			Some(&json!({"advice_min_bytes": 268_435_456})),
		)
		.await;
	assert_eq!(code, 200);
}
