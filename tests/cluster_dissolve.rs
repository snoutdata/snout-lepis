//! Undoing a cluster on a real three-node one (0.1.5): a table dropped through the router leaves the
//! catalog, and one made again under its name is a plain table; a keyspace in use is not dropped and
//! a spare one is; `dissolve` is refused while there are nodes besides the home, and once they are
//! drained and removed it takes the fences off and the catalog forgets every keyspace and table,
//! with every row of every table still there, compared table by table with the plain reference.
//!
//! Needs `LEPIS_IT_HOME`, `LEPIS_IT_DATA_NODES` and `LEPIS_ORACLE_REFERENCE` (`scripts/it.sh`,
//! `LEPIS_IT_ONLY=cluster_dissolve`). Skips without. It drains and removes the data nodes, which
//! no other file minds: each one distributes the bed again from scratch (`common::distribute`).

#[macro_use]
mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use lepis::config::Config;
use lepis::server::{self, App};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TOKEN: &str = "a-token-for-the-dissolve-test";

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

	/// The plan's refusal: (status, kind, message).
	async fn refused(&self, op: Value) -> (u16, String, String) {
		let (status, v) = self.call("POST", "/v1/plan", Some(&op)).await;
		assert!(status >= 400, "{op} was planned: {v}");
		(
			status,
			v["error"]["kind"].as_str().unwrap_or_default().to_string(),
			v["error"]["message"]
				.as_str()
				.unwrap_or_default()
				.to_string(),
		)
	}

	/// Runs one operation to the end; it must finish.
	async fn op(&self, op: Value) -> Value {
		let (status, v) = self.call("POST", "/v1/jobs", Some(&op)).await;
		assert_eq!(status, 201, "run {op}: {v}");
		let id = v["job"].as_i64().unwrap();
		let deadline = Instant::now() + Duration::from_secs(300);
		loop {
			let (_, j) = self.call("GET", &format!("/v1/jobs/{id}"), None).await;
			match j["state"].as_str() {
				Some("done") => return j,
				Some("failed" | "cancelled") => panic!("{op} did not finish: {j:#}"),
				_ if Instant::now() > deadline => panic!("job {id} did not finish: {j}"),
				_ => tokio::time::sleep(Duration::from_millis(200)).await,
			}
		}
	}

	async fn status(&self) -> Value {
		let (s, v) = self.call("GET", "/v1/status", None).await;
		assert_eq!(s, 200, "{v}");
		v
	}
}

fn names(status: &Value, list: &str, key: &str) -> Vec<String> {
	let mut out: Vec<String> = status[list]
		.as_array()
		.map(|a| {
			a.iter()
				.filter_map(|x| x[key].as_str().map(str::to_string))
				.collect()
		})
		.unwrap_or_default();
	out.sort();
	out
}

async fn one(c: &Client, sql: &str) -> String {
	match c
		.simple_query(sql)
		.await
		.unwrap()
		.into_iter()
		.find_map(|m| match m {
			SimpleQueryMessage::Row(r) => Some(r.get(0).map(str::to_string)),
			_ => None,
		}) {
		Some(Some(v)) => v,
		_ => String::new(),
	}
}

/// Every row of a table as one digest, independent of physical order.
fn digest(table: &str) -> String {
	format!(
		"select count(*)::text || ':' || coalesce(md5(string_agg(t::text, '|' order by t::text)), '') from oracle.{table} t"
	)
}

#[tokio::test]
async fn a_cluster_is_taken_apart_with_every_row_kept() {
	let b = need_bed!();
	let nodes = node_ids(&b);
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
	let lepis = l.local_addr().unwrap().to_string();
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
	let home = connect(&b.home, "postgres", "x").await;
	let reference = connect(&b.reference, "postgres", "x").await;
	let app_conn = connect(&lepis, "lepis_app", "app-pw").await;

	// 1. A distributed table dropped through the router leaves the catalog in the same
	// transaction (a two-phase one: it is on every node), and the router has the new epoch within
	// its heartbeat. Made again under the same name, it is a plain table on the home node alone.
	for (_, a) in &nodes {
		connect(a, "postgres", "x")
			.await
			.batch_execute(
				"drop table if exists oracle.scratch; grant create on schema oracle to lepis_app",
			)
			.await
			.unwrap();
	}
	app_conn
		.batch_execute(
			"create table oracle.scratch (tenant_id bigint not null, n int not null, primary key (tenant_id, n));
			insert into oracle.scratch select g, g from generate_series(1, 200) g;",
		)
		.await
		.unwrap();
	admin
		.op(json!({"op": "table.distribute", "table": "oracle.scratch", "column": "tenant_id", "keyspace": "tenant"}))
		.await;
	assert!(
		names(&admin.status().await, "tables", "table").contains(&"oracle.scratch".to_string())
	);
	// The copies Lepis made on the data nodes are its service login's; the application drops it.
	for (_, a) in nodes.iter().skip(1) {
		connect(a, "postgres", "x")
			.await
			.batch_execute("alter table oracle.scratch owner to lepis_app")
			.await
			.unwrap();
	}
	let before: i64 = one(&home, "select epoch from lepis.cluster")
		.await
		.parse()
		.unwrap();
	app_conn
		.batch_execute("drop table oracle.scratch")
		.await
		.unwrap();
	assert_eq!(
		one(
			&home,
			"select count(*) from lepis.relation where table_name = 'scratch'"
		)
		.await,
		"0",
		"the dropped table is still in the catalog"
	);
	let after: i64 = one(&home, "select epoch from lepis.cluster")
		.await
		.parse()
		.unwrap();
	assert!(
		after > before,
		"the epoch did not move ({before} -> {after})"
	);
	let deadline = Instant::now() + Duration::from_secs(12);
	while app.catalog().map_or(0, |c| c.epoch) < after {
		assert!(
			Instant::now() < deadline,
			"the router never loaded epoch {after}"
		);
		tokio::time::sleep(Duration::from_millis(200)).await;
	}
	app_conn
		.batch_execute("create table oracle.scratch (tenant_id bigint not null primary key); insert into oracle.scratch values (1), (2), (3)")
		.await
		.unwrap();
	for (id, a) in &nodes {
		let has = one(
			&connect(a, "postgres", "x").await,
			"select to_regclass('oracle.scratch') is not null",
		)
		.await;
		assert_eq!(
			has,
			if id.0 == 1 { "t" } else { "f" },
			"node {}: the new scratch is not a plain table",
			id.0
		);
	}
	assert_eq!(
		one(&app_conn, "select count(*) from oracle.scratch").await,
		"3"
	);
	app_conn
		.batch_execute("drop table oracle.scratch")
		.await
		.unwrap();
	eprintln!("dropped table: forgotten at epoch {after}, the router caught up, the name is free");

	// 2. Keyspaces: one in use is refused with the tables named; a spare one is dropped.
	let (status, kind, message) = admin
		.refused(json!({"op": "keyspace.drop", "name": "tenant"}))
		.await;
	assert_eq!(
		(status, kind.as_str()),
		(409, "keyspace_in_use"),
		"{message}"
	);
	assert!(message.contains("oracle.orders"), "{message}");
	admin
		.op(json!({"op": "keyspace.create", "name": "spare", "key_type": "bigint", "ranges": 3}))
		.await;
	admin
		.op(json!({"op": "keyspace.drop", "name": "spare"}))
		.await;
	assert_eq!(
		names(&admin.status().await, "keyspaces", "name"),
		["device", "tenant"]
	);

	// 3. Dissolve waits for the home to be alone.
	let (status, kind, message) = admin.refused(json!({"op": "dissolve"})).await;
	assert_eq!(
		(status, kind.as_str()),
		(409, "not_single_node"),
		"{message}"
	);
	assert!(message.contains("n2, n3"), "{message}");

	// 4. Every range comes home, then the data nodes leave.
	admin
		.op(json!({"op": "node.drain", "node": "n3", "to": ["n1"]}))
		.await;
	admin
		.op(json!({"op": "node.drain", "node": "n2", "to": ["n1"]}))
		.await;
	admin.op(json!({"op": "node.remove", "node": "n3"})).await;
	admin.op(json!({"op": "node.remove", "node": "n2"})).await;

	// 5. Dissolve: verified first, then the fences come off and the catalog forgets its shape.
	// Counted in `oracle` only: an earlier file (cluster_advice) leaves a fenced table of its own
	// on the home node, which the catalog this file rebuilt never named.
	let fences_before = one(
		&home,
		"select count(*) from pg_constraint where conname = 'lepis_owns' and connamespace = 'oracle'::regnamespace",
	)
	.await;
	assert_ne!(
		fences_before, "0",
		"the home node had no fences to take off"
	);
	let jobs_before = one(&home, "select count(*) from lepis.job").await;
	let j = admin.op(json!({"op": "dissolve"})).await;
	let steps = j["steps"].as_array().unwrap();
	assert_eq!(
		steps
			.iter()
			.map(|s| s["kind"].as_str().unwrap())
			.collect::<Vec<_>>(),
		["verify", "dissolve"]
	);
	assert_eq!(steps[0]["detail"]["ok"], true, "{:#}", steps[0]["detail"]);
	let forgot = &steps[1]["detail"]["forgot"];
	assert_eq!(forgot["keyspaces"], json!(["device", "tenant"]), "{forgot}");
	eprintln!(
		"dissolve: job {}, {} fences off, forgot {}",
		j["id"],
		steps[1]["detail"]["unfenced"]
			.as_array()
			.map_or(0, Vec::len),
		forgot
	);
	assert_eq!(
		one(
			&home,
			"select count(*) from pg_constraint where conname = 'lepis_owns' and connamespace = 'oracle'::regnamespace"
		)
		.await,
		"0"
	);
	assert_eq!(
		one(&home, "select (select count(*) from lepis.keyspace) + (select count(*) from lepis.range) + (select count(*) from lepis.relation)").await,
		"0"
	);
	let jobs_after: i64 = one(&home, "select count(*) from lepis.job")
		.await
		.parse()
		.unwrap();
	assert_eq!(
		jobs_after,
		jobs_before.parse::<i64>().unwrap() + 1,
		"the job log was not kept"
	);
	let st = admin.status().await;
	assert!(
		names(&st, "keyspaces", "name").is_empty() && names(&st, "tables", "table").is_empty(),
		"{st:#}"
	);

	// Every row of every table is on the home node, the same as the reference, and reads the same
	// through the router, which now passes everything to the home node.
	for table in ["countries", "plans", "tenants", "orders", "items", "events"] {
		let want = one(&reference, &digest(table)).await;
		assert_eq!(
			one(&home, &digest(table)).await,
			want,
			"oracle.{table} on the home node"
		);
		assert_eq!(
			one(&app_conn, &digest(table)).await,
			want,
			"oracle.{table} through the router"
		);
		eprintln!("  oracle.{table}: {want}");
	}

	// Dissolving what is already one database changes nothing and still finishes.
	let again = admin.op(json!({"op": "dissolve"})).await;
	assert_eq!(
		again["steps"][1]["detail"]["forgot"]["keyspaces"],
		json!([])
	);
}
