//! `table.auto` against a real three-node cluster: a schema whose tables are linked
//! by foreign keys is distributed in one call, the linked group in one cutover (each of its tables
//! alone is refused, since its partner is still on the home node), and afterwards every row is on
//! the node that owns its key, a reference table is whole on every node, and nothing is lost.
//!
//! Needs `LEPIS_IT_HOME`, `LEPIS_IT_DATA_NODES` and `LEPIS_ORACLE_REFERENCE` (`scripts/it.sh`;
//! `LEPIS_IT_ONLY=cluster_auto` runs just this file).

#[macro_use]
mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use lepis::config::Config;
use lepis::server::{self, App};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TOKEN: &str = "an-admin-token-for-the-auto-test";

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

	/// Runs `op` and waits for its job, which must finish.
	async fn run(&self, op: &Value) -> Value {
		let (status, v) = self.call("POST", "/v1/jobs", Some(op)).await;
		assert_eq!(status, 201, "run {op}: {v}");
		let id = v["job"].as_i64().unwrap();
		let deadline = Instant::now() + Duration::from_secs(300);
		loop {
			let (_, j) = self.call("GET", &format!("/v1/jobs/{id}"), None).await;
			match j["state"].as_str() {
				Some("done") => return j,
				Some("failed" | "cancelled") => panic!("{op}: {j:#}"),
				_ if Instant::now() > deadline => panic!("job {id} did not finish: {j}"),
				_ => tokio::time::sleep(Duration::from_millis(200)).await,
			}
		}
	}
}

/// The admin API on a router of the bed, for a test that only plans.
async fn admin_for(b: &Bed) -> (Arc<App>, Admin) {
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
	(app, admin)
}

async fn count(c: &tokio_postgres::Client, table: &str) -> i64 {
	c.query_one(&format!("select count(*) from {table}"), &[])
		.await
		.unwrap()
		.get(0)
}

/// A view made before the table is distributed would read only the home node's rows afterwards,
/// so distributing under one is refused, naming every view however deep (a view on a view, a
/// materialized view), and `table.auto` leaves that table home. Plans only: nothing moves.
#[tokio::test]
async fn a_table_read_by_a_view_is_not_distributed() {
	let b = need_bed!();
	let home = connect(&b.home, "postgres", "x").await;
	home.batch_execute(
		"drop schema if exists viewx cascade;
		create schema viewx;
		create table viewx.t (id bigint primary key, v text);
		create table viewx.free (id bigint primary key, v text);
		insert into viewx.t select g, 'v' from generate_series(1, 20) g;
		insert into viewx.free select g, 'v' from generate_series(1, 20) g;
		create view viewx.v1 as select id, v from viewx.t where id > 5;
		create view viewx.v2 as select count(*) from viewx.v1;
		create materialized view viewx.m1 as select id from viewx.t;
		analyze viewx.t, viewx.free;",
	)
	.await
	.unwrap();
	let (_app, admin) = admin_for(&b).await;
	let distribute = |table: &str| json!({"op": "table.distribute", "table": table, "column": "id", "keyspace": "tenant"});

	let (code, e) = admin
		.call("POST", "/v1/plan", Some(&distribute("viewx.t")))
		.await;
	assert_eq!(
		(code, e["error"]["kind"].as_str()),
		(409, Some("table_not_shardable")),
		"{e}"
	);
	let message = e["error"]["message"].as_str().unwrap_or_default();
	for view in ["viewx.v1", "viewx.v2", "viewx.m1"] {
		assert!(message.contains(view), "{view} not named: {message}");
	}
	assert!(
		!message.contains('\u{2014}'),
		"no em-dash in what an owner reads: {message}"
	);

	// table.auto leaves it home, saying why, and still places the table no view reads.
	let (code, plan) = admin
		.call(
			"POST",
			"/v1/plan",
			Some(
				&json!({"op": "table.auto", "keyspace": "tenant", "schemas": ["viewx"], "min_bytes": 0}),
			),
		)
		.await;
	assert_eq!(code, 200, "{plan}");
	let choice = |table: &str| {
		plan["choices"]
			.as_array()
			.unwrap()
			.iter()
			.find(|c| c["table"] == table)
			.cloned()
			.unwrap_or_else(|| panic!("no choice for {table}: {plan:#}"))
	};
	let t = choice("viewx.t");
	assert_eq!(t["choice"], "leave", "{t}");
	assert!(
		t["why"].as_str().unwrap_or_default().contains("viewx.v1"),
		"{t}"
	);
	assert_ne!(choice("viewx.free")["choice"], "leave", "{plan:#}");

	// The materialized view gone, the view on a view still holds it; both views gone, it plans.
	home.batch_execute("drop materialized view viewx.m1")
		.await
		.unwrap();
	let (code, e) = admin
		.call("POST", "/v1/plan", Some(&distribute("viewx.t")))
		.await;
	assert_eq!(code, 409, "{e}");
	let message = e["error"]["message"].as_str().unwrap_or_default();
	assert!(
		message.contains("viewx.v2") && !message.contains("viewx.m1"),
		"{message}"
	);
	home.batch_execute("drop view viewx.v2; drop view viewx.v1")
		.await
		.unwrap();
	let (code, p) = admin
		.call("POST", "/v1/plan", Some(&distribute("viewx.t")))
		.await;
	assert_eq!(code, 200, "{p}");
	home.batch_execute("drop schema viewx cascade")
		.await
		.unwrap();
}

#[tokio::test]
async fn table_auto_distributes_a_linked_schema_in_one_call() {
	let b = need_bed!();
	let nodes = node_ids(&b);
	for (_, a) in &nodes {
		connect(a, "postgres", "x")
			.await
			.batch_execute("drop schema if exists autox cascade")
			.await
			.unwrap();
	}
	let home = connect(&b.home, "postgres", "x").await;
	// accounts is the tenant (projects references it); tasks carries account_id with a key to
	// projects on both columns; plans is referenced by accounts by plan_id, which is not the key
	// accounts is sharded by; audit is filled by a sequence.
	home.batch_execute(
		"create schema autox;
		create table autox.plans (id bigint primary key, name text not null);
		create table autox.accounts (id bigint primary key, plan_id bigint not null references autox.plans (id), name text);
		create table autox.projects (account_id bigint not null references autox.accounts (id), id bigint not null,
			name text, primary key (account_id, id));
		create table autox.tasks (account_id bigint not null, project_id bigint not null, id bigint not null, name text,
			primary key (account_id, id), foreign key (account_id, project_id) references autox.projects (account_id, id));
		create table autox.notes (id bigint primary key, body text);
		create table autox.audit (id bigserial primary key, what text);
		insert into autox.plans select g, 'plan ' || g from generate_series(1, 3) g;
		insert into autox.accounts select g, g % 3 + 1, 'account ' || g from generate_series(1, 300) g;
		insert into autox.projects select a, a * 10 + p, 'p' from generate_series(1, 300) a, generate_series(1, 2) p;
		insert into autox.tasks select a, a * 10 + p, (a * 10 + p) * 10 + t, 't'
			from generate_series(1, 300) a, generate_series(1, 2) p, generate_series(1, 3) t;
		insert into autox.notes select g, 'n' from generate_series(1, 50) g;
		insert into autox.audit (what) select 'a' from generate_series(1, 20);
		analyze autox.plans, autox.accounts, autox.projects, autox.tasks, autox.notes, autox.audit;",
	)
	.await
	.unwrap();
	let before: BTreeMap<&str, i64> = {
		let mut m = BTreeMap::new();
		for t in ["plans", "accounts", "projects", "tasks", "notes", "audit"] {
			m.insert(t, count(&home, &format!("autox.{t}")).await);
		}
		m
	};

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

	// Each linked table alone is refused: its partner is still on the home node.
	let (code, e) = admin
		.call(
			"POST",
			"/v1/plan",
			Some(&json!({"op": "table.distribute", "table": "autox.accounts", "column": "id", "keyspace": "tenant"})),
		)
		.await;
	assert_eq!(
		(code, e["error"]["kind"].as_str()),
		(409, Some("table_not_shardable")),
		"{e}"
	);

	// The plan says what it chose for every table, and why. min_bytes 0: no table is too small.
	let op =
		json!({"op": "table.auto", "keyspace": "tenant", "schemas": ["autox"], "min_bytes": 0});
	let (code, plan) = admin.call("POST", "/v1/plan", Some(&op)).await;
	assert_eq!(code, 200, "{plan}");
	let choices: BTreeMap<String, (String, Value, String)> = plan["choices"]
		.as_array()
		.unwrap()
		.iter()
		.map(|c| {
			(
				c["table"]
					.as_str()
					.unwrap()
					.trim_start_matches("autox.")
					.to_string(),
				(
					c["choice"].as_str().unwrap().to_string(),
					c["column"].clone(),
					c["why"].as_str().unwrap().to_string(),
				),
			)
		})
		.collect();
	eprintln!("{choices:#?}");
	let is = |t: &str, choice: &str, column: Value| {
		let (c, col, why) = &choices[t];
		assert_eq!((c.as_str(), col), (choice, &column), "{t}: {why}");
	};
	is("accounts", "shard", json!("id"));
	is("projects", "shard", json!("account_id"));
	is("tasks", "shard", json!("account_id"));
	is("notes", "shard", json!("id"));
	is("plans", "reference", Value::Null);
	is("audit", "leave", Value::Null);
	assert!(
		choices["audit"].2.contains("filled by a sequence"),
		"{:?}",
		choices["audit"]
	);
	assert!(
		choices["plans"].2.contains("by plan_id, not by its key"),
		"{:?}",
		choices["plans"]
	);
	// Two cutovers: the reference table, then the sharded group together.
	assert_eq!(plan["cutovers"], 2, "{plan:#}");

	admin.run(&op).await;
	admin
		.run(&json!({"op": "verify", "keyspace": "tenant"}))
		.await;

	// Every row is somewhere exactly once; the reference table is whole on every node.
	let mut on: BTreeMap<&str, i64> = BTreeMap::new();
	for (id, a) in &nodes {
		let c = connect(a, "postgres", "x").await;
		for t in ["accounts", "projects", "tasks", "notes"] {
			*on.entry(t).or_default() += count(&c, &format!("autox.{t}")).await;
		}
		assert_eq!(
			count(&c, "autox.plans").await,
			before["plans"],
			"plans on node {id:?}"
		);
	}
	for t in ["accounts", "projects", "tasks", "notes"] {
		assert_eq!(on[t], before[t], "{t}: {on:?}");
	}
	assert_eq!(count(&home, "autox.audit").await, before["audit"]);
	// A tenant's rows are on one node: its account, projects and tasks together.
	for (_, a) in &nodes {
		let c = connect(a, "postgres", "x").await;
		let orphans: i64 = c
			.query_one(
				"select (select count(*) from autox.projects p where not exists (select from autox.accounts a where a.id = p.account_id))
					+ (select count(*) from autox.tasks t where not exists (select from autox.projects p where (p.account_id, p.id) = (t.account_id, t.project_id)))",
				&[],
			)
			.await
			.unwrap()
			.get(0);
		assert_eq!(orphans, 0, "{a}: rows away from the rows they reference");
	}

	// Again: everything is placed, so there is nothing to do, and the plan says so per table.
	let (code, again) = admin.call("POST", "/v1/plan", Some(&op)).await;
	assert_eq!(code, 200, "{again}");
	assert!(
		again["choices"]
			.as_array()
			.unwrap()
			.iter()
			.all(|c| c["choice"] == "leave"),
		"{again:#}"
	);
	let (code, e) = admin.call("POST", "/v1/jobs", Some(&op)).await;
	assert_eq!(
		(code, e["error"]["kind"].as_str()),
		(409, Some("nothing_to_do")),
		"{e}"
	);
}
