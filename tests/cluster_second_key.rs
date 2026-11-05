//! A second key on a real three-node cluster: a keyspace with a second key, so one
//! tenant's rows spread over every node. One big tenant's rows land on each node; reads by the
//! tenant alone are scatters and answer as one Postgres; INSERT (several rows, parameters), COPY,
//! UPDATE and DELETE place and find rows by both keys; assigning a key is refused; a range moves
//! with the rows kept; verify passes. Every chunk's fence checks the pair's hash in SQL, so a
//! row the router placed by a different hash than Postgres computes would be refused.
//!
//! Needs `LEPIS_IT_HOME`, `LEPIS_IT_DATA_NODES` and `LEPIS_ORACLE_REFERENCE` (`scripts/it.sh`,
//! `LEPIS_IT_ONLY=cluster_second_key`). Skips without.

#[macro_use]
mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use lepis::config::Config;
use lepis::server::{self, App};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TOKEN: &str = "a-token-for-the-second-key-test";

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

	async fn refused(&self, op: Value) -> String {
		let (status, v) = self.call("POST", "/v1/plan", Some(&op)).await;
		assert!(status >= 400, "{op} was planned: {v}");
		v["error"]["message"]
			.as_str()
			.unwrap_or_default()
			.to_string()
	}

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
}

async fn one(c: &Client, sql: &str) -> String {
	match c
		.simple_query(sql)
		.await
		.unwrap_or_else(|e| panic!("{sql}: {e}"))
		.into_iter()
		.find_map(|m| match m {
			SimpleQueryMessage::Row(r) => Some(r.get(0).map(str::to_string)),
			_ => None,
		}) {
		Some(Some(v)) => v,
		_ => String::new(),
	}
}

const TABLE: &str = "create table oracle.visits (tenant_id bigint not null, seq bigint not null, \
	note text, primary key (tenant_id, seq));
	insert into oracle.visits select 1, g, 'big ' || g from generate_series(1, 2000) g;
	insert into oracle.visits select t, g, 'small' from generate_series(2, 11) t, generate_series(1, 100) g;";

/// The reads compared with one Postgres, rows sorted.
const READS: [&str; 7] = [
	"select count(*), sum(seq) from oracle.visits where tenant_id = 1",
	"select tenant_id, count(*), max(seq) from oracle.visits group by tenant_id order by 1",
	"select tenant_id, seq, note from oracle.visits where tenant_id = 1 and seq between 990 and 1010 order by seq",
	"select tenant_id, seq from oracle.visits where tenant_id = 3 and seq = 50",
	"select note, count(*) from oracle.visits group by note having count(*) > 1 order by 2 desc, 1 limit 5",
	"select t.country, count(*) from oracle.visits v join oracle.tenants t on t.tenant_id = v.tenant_id group by t.country order by 1",
	"select count(distinct tenant_id) from oracle.visits",
];

async fn same(cluster: &Client, reference: &Client, when: &str) {
	for sql in READS {
		assert_eq!(
			answer(cluster, sql, true).await,
			answer(reference, sql, true).await,
			"{sql} ({when})"
		);
	}
}

#[tokio::test]
async fn one_tenant_spreads_over_every_node() {
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
	for c in [&home, &reference] {
		c.batch_execute(&format!(
			"drop table if exists oracle.visits; {TABLE}; grant all on oracle.visits to lepis_app"
		))
		.await
		.unwrap();
	}
	for (_, a) in nodes.iter().skip(1) {
		connect(a, "postgres", "x")
			.await
			.batch_execute("drop table if exists oracle.visits cascade")
			.await
			.unwrap();
	}

	admin
		.op(json!({"op": "keyspace.create", "name": "visits", "key_type": "int8", "second_key_type": "int8", "ranges": 4}))
		.await;
	// What a second key needs, refused with what to do.
	let m = admin
		.refused(json!({"op": "table.distribute", "table": "oracle.visits", "column": "tenant_id", "keyspace": "visits"}))
		.await;
	assert!(m.contains("name the second"), "{m}");
	let m = admin
		.refused(json!({"op": "table.distribute", "table": "oracle.visits", "by": ["tenant_id", "seq"], "keyspace": "visits", "heap": true}))
		.await;
	assert!(m.contains("chunk tables"), "{m}");
	let m = admin
		.refused(json!({"op": "table.distribute", "table": "oracle.visits", "by": ["tenant_id", "note"], "keyspace": "visits"}))
		.await;
	assert!(m.contains("second key is bigint"), "{m}");
	let m = admin
		.refused(json!({"op": "table.distribute", "table": "oracle.visits", "by": ["tenant_id", "seq"], "keyspace": "tenant"}))
		.await;
	assert!(m.contains("has one key"), "{m}");
	// A plain unique index counts, not only a constraint.
	home.batch_execute(
		"create unique index visits_note on oracle.visits (tenant_id, note) where false",
	)
	.await
	.unwrap();
	let m = admin
		.refused(json!({"op": "table.distribute", "table": "oracle.visits", "by": ["tenant_id", "seq"], "keyspace": "visits", "chunks": 32}))
		.await;
	assert!(
		m.contains("visits_note") && m.contains("does not include the second key"),
		"{m}"
	);
	home.batch_execute("drop index oracle.visits_note")
		.await
		.unwrap();

	admin
		.op(json!({"op": "table.distribute", "table": "oracle.visits", "by": ["tenant_id", "seq"], "keyspace": "visits", "chunks": 32}))
		.await;
	for (_, a) in &nodes {
		connect(a, "postgres", "x")
			.await
			.batch_execute("grant all on all tables in schema oracle to lepis_app")
			.await
			.unwrap();
	}
	// The big tenant is on every node, each row once.
	let mut total = 0;
	for (id, a) in &nodes {
		let n: i64 = one(
			&connect(a, "postgres", "x").await,
			"select count(*) from oracle.visits where tenant_id = 1",
		)
		.await
		.parse()
		.unwrap();
		assert!(n > 0, "{id} holds none of tenant 1");
		total += n;
	}
	assert_eq!(total, 2000);

	let c = connect(&lepis, "lepis_app", "app-pw").await;
	same(&c, &reference, "distributed").await;

	// Writes by both keys: rows of one statement go to several nodes (two-phase commit).
	let writes = [
		"insert into oracle.visits (tenant_id, seq, note) values (1, 5001, 'w'), (1, 5002, 'w'), (1, 5003, 'w'), (2, 5001, 'w')",
		"update oracle.visits set note = 'updated' where tenant_id = 1 and seq < 300",
		"delete from oracle.visits where tenant_id = 4 and seq > 50",
		"insert into oracle.visits (tenant_id, seq, note) values (12, 1, 'new') on conflict (tenant_id, seq) do nothing",
	];
	for w in writes {
		c.batch_execute(w)
			.await
			.unwrap_or_else(|e| panic!("{w}: {e}"));
		reference.batch_execute(w).await.unwrap();
	}
	let ins = "insert into oracle.visits (tenant_id, seq, note) values ($1, $2, $3)";
	for seq in 6000..6020i64 {
		let n = format!("p{seq}");
		c.execute(ins, &[&1i64, &seq, &n]).await.unwrap();
		reference.execute(ins, &[&1i64, &seq, &n]).await.unwrap();
	}
	// COPY, split by the pair.
	let data: String = (7000..7300)
		.map(|s| format!("{}\t{s}\tcopy\n", s % 3 + 1))
		.collect();
	for client in [&c, &reference] {
		let sink = client
			.copy_in("copy oracle.visits (tenant_id, seq, note) from stdin")
			.await
			.unwrap();
		futures_util::pin_mut!(sink);
		futures_util::SinkExt::send(&mut sink, bytes::Bytes::from(data.clone()))
			.await
			.unwrap();
		sink.finish().await.unwrap();
	}
	same(&c, &reference, "after the writes").await;
	// A key is not assigned: the row would change node.
	for sql in [
		"update oracle.visits set seq = seq + 100000 where tenant_id = 5",
		"update oracle.visits set tenant_id = 99 where seq = 1",
	] {
		let e = c.simple_query(sql).await.unwrap_err();
		assert_eq!(
			e.as_db_error().map(|d| d.code().code()),
			Some("0A000"),
			"{sql}: {e}"
		);
	}

	// A range moves (the chunk move's capture log replays the pairs), under a write.
	let status = admin.call("GET", "/v1/status", None).await.1;
	let ks = status["keyspaces"]
		.as_array()
		.unwrap()
		.iter()
		.find(|k| k["name"] == "visits")
		.unwrap()
		.clone();
	let r = &ks["ranges"][0];
	let from = r["node"].as_i64().unwrap();
	let to = nodes
		.iter()
		.map(|(id, _)| id.0 as i64)
		.find(|n| *n != from)
		.unwrap();
	let w = {
		let lepis = lepis.clone();
		tokio::spawn(async move {
			let c = connect(&lepis, "lepis_app", "app-pw").await;
			let mut done = Vec::new();
			for seq in 8000..8200i64 {
				for _ in 0..50 {
					match c
						.execute(
							"insert into oracle.visits (tenant_id, seq, note) values (1, $1, 'during')",
							&[&seq],
						)
						.await
					{
						Ok(_) => {
							done.push(seq);
							break;
						}
						Err(e)
							if e.as_db_error().is_some_and(|d| {
								d.code().code() == "40001" || d.code().code() == "57014"
							}) =>
						{
							tokio::time::sleep(Duration::from_millis(20)).await;
						}
						Err(e) => panic!("{e}"),
					}
				}
			}
			done
		})
	};
	admin
		.op(json!({"op": "range.move", "keyspace": "visits", "range": r["lo"].as_str().map(|s| s.parse::<i64>().unwrap()).unwrap_or_else(|| r["lo"].as_i64().unwrap()), "to": to}))
		.await;
	let written = w.await.unwrap();
	for seq in &written {
		reference
			.execute(
				"insert into oracle.visits (tenant_id, seq, note) values (1, $1, 'during')",
				&[seq],
			)
			.await
			.unwrap();
	}
	same(&c, &reference, "after the move").await;
	let v = admin
		.op(json!({"op": "verify", "keyspace": "visits"}))
		.await;
	assert_eq!(v["state"], "done", "{v:#}");
}
