//! Phase 4's gate against a real three-node cluster: the operations engine moves rows while a
//! write load runs through Lepis, and afterwards no row is lost or duplicated.
//!
//! A table is distributed from the home node, then a range is split (half moving), a range moves,
//! two ranges merge, a tenant is pinned to its own node, a node is drained and removed, the
//! cluster scales back out (node add, then a rebalance), and a move is cancelled; then verify. Eight writers insert and update
//! through the router the whole time, retrying what the cutovers refuse (`23514` from a fence,
//! the `57014` of a drain abort, a broken session). The write latency each one saw, retries
//! included, is the pause the application felt; the jobs record the pause they measured.
//!
//! Needs `LEPIS_IT_HOME`, `LEPIS_IT_DATA_NODES` and `LEPIS_ORACLE_REFERENCE` (`scripts/it.sh`).

#[macro_use]
mod common;

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use common::*;
use lepis::config::Config;
use lepis::server::{self, App};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TOKEN: &str = "an-admin-token-for-the-tests";
const WORKERS: usize = 8;
const SEED_ROWS: i64 = 20_000;
static T0: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

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

	async fn plan(&self, op: Value) -> Value {
		let (status, plan) = self.call("POST", "/v1/plan", Some(&op)).await;
		assert_eq!(status, 200, "plan {op}: {plan}");
		for k in [
			"steps",
			"moves",
			"estimated_rows",
			"estimated_copy_seconds",
			"expected_pause_ms",
			"max_write_pause_ms",
		] {
			assert!(plan.get(k).is_some(), "plan {op} has no {k}: {plan}");
		}
		plan
	}

	async fn start(&self, op: Value) -> i64 {
		let (status, v) = self.call("POST", "/v1/jobs", Some(&op)).await;
		assert_eq!(status, 201, "run {op}: {v}");
		v["job"].as_i64().unwrap()
	}

	async fn wait(&self, id: i64) -> Value {
		let deadline = Instant::now() + Duration::from_secs(300);
		loop {
			let (_, j) = self.call("GET", &format!("/v1/jobs/{id}"), None).await;
			match j["state"].as_str() {
				Some("done" | "failed" | "cancelled") => return j,
				_ if Instant::now() > deadline => panic!("job {id} did not finish: {j}"),
				_ => tokio::time::sleep(Duration::from_millis(200)).await,
			}
		}
	}

	/// Plans, runs and waits for one operation; it must finish.
	async fn op(&self, op: Value) -> Value {
		let plan = self.plan(op.clone()).await;
		let id = self.start(op.clone()).await;
		let j = self.wait(id).await;
		assert_eq!(j["state"], "done", "{op} failed: {j:#}");
		eprintln!(
			"  (finished at {:.1}s)",
			T0.get().map_or(0.0, |t| t.elapsed().as_secs_f64())
		);
		for s in j["steps"].as_array().unwrap() {
			let d = &s["detail"];
			if d.get("prepare_ms").is_some() {
				eprintln!(
					"    move: tables {} ms, prepared {} ms, copy {} s, mark {} ms, cutover {}",
					d["tables_ms"],
					d["prepare_ms"],
					d["copy_seconds"],
					d["mark_rtt_ms"],
					d["cutover"]
				);
			}
		}
		eprintln!(
			"{:<18} job {id}: {} cutover(s), estimated {} rows, {:.1}s copy; measured pauses {:?} ms",
			op["op"].as_str().unwrap(),
			plan["cutovers"],
			plan["estimated_rows"],
			plan["estimated_copy_seconds"].as_f64().unwrap_or(0.0),
			pauses(&j)
				.iter()
				.map(|p| p.round() as i64)
				.collect::<Vec<_>>(),
		);
		j
	}

	async fn status(&self) -> Value {
		let (s, v) = self.call("GET", "/v1/status", None).await;
		assert_eq!(s, 200, "{v}");
		v
	}
}

/// The ranges of a keyspace, as (lo, hi, node).
fn ranges(status: &Value, keyspace: &str) -> Vec<(i64, i64, i64)> {
	status["keyspaces"]
		.as_array()
		.unwrap()
		.iter()
		.find(|k| k["name"] == keyspace)
		.unwrap()["ranges"]
		.as_array()
		.unwrap()
		.iter()
		.map(|r| {
			(
				r["lo"].as_str().unwrap().parse().unwrap(),
				r["hi"].as_str().unwrap().parse().unwrap(),
				r["node"].as_i64().unwrap(),
			)
		})
		.collect()
}

/// Every cutover a job made: its measured pause.
fn pauses(job: &Value) -> Vec<f64> {
	job["steps"]
		.as_array()
		.unwrap()
		.iter()
		.filter_map(|s| s["detail"]["cutover"]["pause_ms"].as_f64())
		.collect()
}

async fn try_connect(address: &str) -> Option<Client> {
	let (host, port) = address.rsplit_once(':').unwrap();
	let (client, conn) = tokio_postgres::Config::new()
		.host(host)
		.port(port.parse().unwrap())
		.user("lepis_app")
		.password("app-pw")
		.dbname("postgres")
		.connect_timeout(Duration::from_secs(5))
		.connect(NoTls)
		.await
		.ok()?;
	tokio::spawn(conn);
	Some(client)
}

#[derive(Default)]
struct Stats {
	/// When each write started (from the test's start) and how long it took, retries included.
	latency: Vec<(Duration, Duration)>,
	inserted: Vec<(i64, i64)>,
	/// Per row: updates acknowledged, and updates whose outcome is unknown (the session broke).
	updated: BTreeMap<(i64, i64), (i32, i32)>,
	errors: BTreeMap<String, usize>,
	stale_updates: usize,
}

struct Rng(u64);
impl Rng {
	fn next(&mut self) -> u64 {
		self.0 ^= self.0 << 13;
		self.0 ^= self.0 >> 7;
		self.0 ^= self.0 << 17;
		self.0
	}
}

enum Outcome {
	Done,
	/// The session broke after the statement was sent: it may or may not have committed.
	Unknown,
}

/// One write, retried until it lands. An insert that meets its own key on a retry landed the
/// first time; an update that matches nothing went to a node that no longer owns the row (a
/// session that has not reloaded the catalog) and is retried on a fresh session.
async fn write(
	lepis: &str,
	client: &mut Option<Client>,
	sql: &str,
	insert: bool,
	stats: &StdMutex<Stats>,
) -> Outcome {
	let begun = Instant::now();
	let mut trail: Vec<String> = Vec::new();
	let mut attempt = 0;
	loop {
		attempt += 1;
		assert!(attempt < 2000, "{sql} never landed");
		if client.is_none() {
			*client = try_connect(lepis).await;
			if client.is_none() {
				tokio::time::sleep(Duration::from_millis(20)).await;
				continue;
			}
		}
		let c = client.as_ref().unwrap();
		match c.simple_query(sql).await {
			Ok(messages) => {
				let n = messages.iter().find_map(|m| match m {
					SimpleQueryMessage::CommandComplete(n) => Some(*n),
					_ => None,
				});
				if !insert && n == Some(0) {
					stats.lock().unwrap().stale_updates += 1;
					trail.push(format!("{} stale", begun.elapsed().as_millis()));
					*client = None;
					continue;
				}
				if begun.elapsed() > Duration::from_secs(2) {
					eprint!(
						"t={:.1}s ",
						T0.get().map_or(0.0, |t| t.elapsed().as_secs_f64())
					);
					eprintln!(
						"slow write: {} ms, {attempt} attempts {trail:?}: {sql}",
						begun.elapsed().as_millis()
					);
				}
				return Outcome::Done;
			}
			Err(e) => {
				let code = e.as_db_error().map(|d| d.code().code().to_string());
				trail.push(format!(
					"{} {}",
					begun.elapsed().as_millis(),
					code.clone().unwrap_or_else(|| e.to_string())
				));
				if insert && attempt > 1 && code.as_deref() == Some("23505") {
					return Outcome::Done;
				}
				*stats
					.lock()
					.unwrap()
					.errors
					.entry(code.clone().unwrap_or_else(|| "session".into()))
					.or_default() += 1;
				*client = None;
				if code.is_none() && !insert {
					return Outcome::Unknown;
				}
				tokio::time::sleep(Duration::from_millis(5)).await;
			}
		}
	}
}

async fn load(
	lepis: String,
	w: usize,
	stop: Arc<AtomicBool>,
	next: Arc<AtomicI64>,
	stats: Arc<StdMutex<Stats>>,
	t0: Instant,
) {
	let mut rng = Rng(0x9e37_79b9_7f4a_7c15 ^ (w as u64 + 1).wrapping_mul(0x2545_f491_4f6c_dd1d));
	let mut client = try_connect(&lepis).await;
	let mut mine: Vec<(i64, i64)> = Vec::new();
	while !stop.load(Ordering::Relaxed) {
		let started = t0.elapsed();
		let begun = Instant::now();
		if mine.is_empty() || rng.next() % 10 < 6 {
			let acct = (rng.next() % 1000) as i64 + 1;
			let id = next.fetch_add(1, Ordering::Relaxed);
			let sql = format!(
				"insert into opsx.ledger (acct, id, n, note) values ({acct}, {id}, 0, 'w{w}')"
			);
			write(&lepis, &mut client, &sql, true, &stats).await;
			mine.push((acct, id));
			let mut s = stats.lock().unwrap();
			s.inserted.push((acct, id));
			s.latency.push((started, begun.elapsed()));
		} else {
			let (acct, id) = mine[(rng.next() % mine.len() as u64) as usize];
			let sql = format!("update opsx.ledger set n = n + 1 where acct = {acct} and id = {id}");
			let outcome = write(&lepis, &mut client, &sql, false, &stats).await;
			let mut s = stats.lock().unwrap();
			let e = s.updated.entry((acct, id)).or_default();
			match outcome {
				Outcome::Done => e.0 += 1,
				Outcome::Unknown => e.1 += 1,
			}
			s.latency.push((started, begun.elapsed()));
		}
	}
}

fn percentile(v: &mut [f64], p: f64) -> f64 {
	if v.is_empty() {
		return 0.0;
	}
	v.sort_by(|a, b| a.partial_cmp(b).unwrap());
	v[((v.len() as f64 - 1.0) * p).round() as usize]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn operations_under_write_load_lose_nothing() {
	let b = need_bed!();
	let nodes = node_ids(&b);
	let home = connect(&b.home, "postgres", "x").await;
	for (_, a) in &nodes {
		let admin = connect(a, "postgres", "x").await;
		admin
			.batch_execute("drop schema if exists opsx cascade")
			.await
			.unwrap();
	}
	home.batch_execute(&format!(
		"create schema opsx;
		create table opsx.ledger (acct bigint not null, id bigint not null, n int not null default 0,
			note text, primary key (acct, id));
		grant usage on schema opsx to lepis_app;
		grant select, insert, update, delete on opsx.ledger to lepis_app;
		insert into opsx.ledger select (g % 500) + 1, g, 0, 'seed' from generate_series(1, {SEED_ROWS}) g;"
	))
	.await
	.unwrap();

	// One router, with the admin API on a port of its own.
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

	// The API refuses without the token, and names what it cannot do.
	let mut s = tokio::net::TcpStream::connect(&admin.addr).await.unwrap();
	s.write_all(b"GET /v1/status HTTP/1.1\r\n\r\n")
		.await
		.unwrap();
	let mut out = String::new();
	s.read_to_string(&mut out).await.unwrap();
	assert!(out.starts_with("HTTP/1.1 401"), "{out}");
	let (code, e) = admin
		.call(
			"POST",
			"/v1/plan",
			Some(&json!({"op": "range.move", "keyspace": "nope", "range": 0, "to": 2})),
		)
		.await;
	assert_eq!(
		(code, e["error"]["kind"].as_str()),
		(404, Some("no_such_keyspace")),
		"{e}"
	);
	assert!(
		e["error"]["message"]
			.as_str()
			.unwrap()
			.contains("no keyspace nope"),
		"{e}"
	);
	// 404 for what does not exist, 400 for input that cannot be right, 409 for what the
	// cluster's state refuses; `kind` is the machine-readable reason.
	for (method, path, body, status, kind) in [
		("GET", "/v1/jobs/99999", None, 404, "no_such_job"),
		("POST", "/v1/jobs/99999/cancel", None, 404, "no_such_job"),
		("POST", "/v1/jobs/99999/resume", None, 404, "no_such_job"),
		(
			"POST",
			"/v1/plan",
			Some(json!({"op": "range.teleport"})),
			400,
			"unknown_operation",
		),
		(
			"POST",
			"/v1/plan",
			Some(json!({"op": "range.move", "keyspace": "tenant"})),
			400,
			"bad_request",
		),
		(
			"POST",
			"/v1/plan",
			Some(json!({"op": "node.remove", "node": 1})),
			409,
			"home_node",
		),
		(
			"POST",
			"/v1/plan",
			Some(json!({"op": "node.remove", "node": 2})),
			409,
			"node_not_empty",
		),
		("POST", "/v1/nothing", None, 404, "no_such_route"),
	] {
		let (code, e) = admin.call(method, path, body.as_ref()).await;
		assert_eq!(
			(code, e["error"]["kind"].as_str()),
			(status, Some(kind)),
			"{method} {path}: {e}"
		);
		assert!(
			e["error"]["message"]
				.as_str()
				.is_some_and(|m| !m.is_empty()),
			"{e}"
		);
	}
	// A plan's 64-bit bounds are strings.
	let first = ranges(&admin.status().await, "tenant")[0].0;
	let p = admin
		.plan(json!({"op": "range.split", "keyspace": "tenant", "range": first.to_string()}))
		.await;
	let split = &p["steps"][0]["args"]["change"]["split"];
	assert_eq!(split["lo"], json!(first.to_string()), "{p}");
	assert!(
		p["moves"][0]["change"]["range_owner"]["hi"].is_string(),
		"{p}"
	);

	admin
		.op(json!({"op": "keyspace.create", "name": "acct", "key_type": "bigint", "seed": 4242, "ranges": 2, "nodes": [1, 2]}))
		.await;

	// The load, through Lepis, for the rest of the test.
	let stop = Arc::new(AtomicBool::new(false));
	let next = Arc::new(AtomicI64::new(1_000_000));
	let stats = Arc::new(StdMutex::new(Stats::default()));
	let t0 = Instant::now();
	T0.set(t0).ok();
	let sampler = tokio::spawn(sample_stalls(nodes.clone(), stop.clone(), t0));
	let mut workers = Vec::new();
	for w in 0..WORKERS {
		workers.push(tokio::spawn(load(
			lepis.clone(),
			w,
			stop.clone(),
			next.clone(),
			stats.clone(),
			t0,
		)));
	}
	tokio::time::sleep(Duration::from_secs(2)).await;

	let mut jobs = Vec::new();
	jobs.push(
		admin
			.op(
				json!({"op": "table.distribute", "table": "opsx.ledger", "column": "acct", "keyspace": "acct"}),
			)
			.await,
	);

	// Split node 2's range and move the upper half to node 3.
	let st = admin.status().await;
	let (lo2, hi2, _) = *ranges(&st, "acct").iter().find(|r| r.2 == 2).unwrap();
	jobs.push(
		admin
			.op(json!({"op": "range.split", "keyspace": "acct", "range": lo2.to_string(), "to": 3}))
			.await,
	);
	let st = admin.status().await;
	let r = ranges(&st, "acct");
	assert!(
		r.iter().any(|x| x.0 == lo2 && x.2 == 2) && r.iter().any(|x| x.1 == hi2 && x.2 == 3),
		"{r:?}"
	);
	let upper = r.iter().find(|x| x.1 == hi2).unwrap().0;

	// Node 1's range moves to node 3.
	let (lo1, _, _) = *r.iter().find(|x| x.2 == 1).unwrap();
	jobs.push(
		admin
			.op(
				json!({"op": "range.move", "keyspace": "acct", "range": lo1.to_string(), "to": "n3"}),
			)
			.await,
	);

	// The two halves of the split come back together (the upper half moves to node 2 first).
	jobs.push(
		admin
			.op(
				json!({"op": "range.merge", "keyspace": "acct", "a": lo2.to_string(), "b": upper.to_string()}),
			)
			.await,
	);
	let st = admin.status().await;
	assert!(
		ranges(&st, "acct").contains(&(lo2, hi2, 2)),
		"{:?}",
		ranges(&st, "acct")
	);

	// A tenant on a node of its own.
	jobs.push(
		admin
			.op(json!({"op": "tenant.pin", "keyspace": "acct", "value": "7", "node": 3}))
			.await,
	);
	let st = admin.status().await;
	let pins = &st["keyspaces"]
		.as_array()
		.unwrap()
		.iter()
		.find(|k| k["name"] == "acct")
		.unwrap()["pins"];
	assert_eq!(pins, &json!([{"value": "7", "node": 3}]));

	// Node 3 gives everything back (every keyspace, the pin included) and leaves.
	jobs.push(admin.op(json!({"op": "node.drain", "node": 3})).await);
	jobs.push(admin.op(json!({"op": "node.remove", "node": 3})).await);
	// The same server joins again as a new node, and the keyspaces spread onto it.
	let (h3, p3) = b.data[1].rsplit_once(':').unwrap();
	jobs.push(
		admin
			.op(json!({"op": "scale", "add": [{"name": "n4", "host": h3, "port": p3.parse::<u16>().unwrap(), "dbname": "postgres", "sslmode": "disable"}]}))
			.await,
	);
	let st = admin.status().await;
	assert!(
		ranges(&st, "acct").iter().any(|r| r.2 == 4),
		"{:?}",
		ranges(&st, "acct")
	);

	// A move cancelled at once: rolled back before its cutover, or finished after it.
	let st = admin.status().await;
	let (lo, _, owner) = ranges(&st, "acct")[0];
	let to = if owner == 1 { 2 } else { 1 };
	let id = admin
		.start(json!({"op": "range.move", "keyspace": "acct", "range": lo.to_string(), "to": to}))
		.await;
	// Cancelled once its copy is under way, so the rollback is what is tested.
	let deadline = Instant::now() + Duration::from_secs(60);
	while Instant::now() < deadline {
		let (_, j) = admin.call("GET", &format!("/v1/jobs/{id}"), None).await;
		if j["steps"][0]["detail"]["phase"].is_string() {
			break;
		}
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	let (code, c) = admin
		.call("POST", &format!("/v1/jobs/{id}/cancel"), None)
		.await;
	assert_eq!(code, 200, "{c}");
	let j = admin.wait(id).await;
	assert_eq!(j["state"], "cancelled", "{j:#}");
	let st = admin.status().await;
	if j["steps"][0]["detail"]["phase"] == "rolled_back" {
		assert!(
			ranges(&st, "acct").contains(&(lo, ranges(&st, "acct")[0].1, owner)),
			"a rolled back move changed the owner"
		);
	}
	eprintln!(
		"cancel             job {id}: phase {}",
		j["steps"][0]["detail"]["phase"]
	);
	jobs.push(j);

	tokio::time::sleep(Duration::from_secs(2)).await;
	stop.store(true, Ordering::Relaxed);
	for w in workers {
		w.await.unwrap();
	}
	sampler.await.unwrap();
	let v = admin.op(json!({"op": "verify"})).await;
	let report = &v["steps"][0]["detail"];
	assert_eq!(report["ok"], true, "{report:#}");
	for t in report["tables"].as_array().unwrap() {
		assert_eq!(t["foreign_rows"], "0", "left for cleanup: {t}");
	}

	// Every row exactly once across the nodes, every acknowledged write in it.
	let mut seen: BTreeMap<(i64, i64), i32> = BTreeMap::new();
	let mut dup = 0;
	for (id, a) in &nodes {
		let c = connect(a, "postgres", "x").await;
		let rows = c
			.query("select acct, id, n from opsx.ledger", &[])
			.await
			.unwrap();
		eprintln!("node {}: {} ledger rows", id.0, rows.len());
		for r in rows {
			if seen.insert((r.get(0), r.get(1)), r.get(2)).is_some() {
				dup += 1;
			}
		}
	}
	let stats = std::mem::take(&mut *stats.lock().unwrap());
	let expected: HashSet<(i64, i64)> = (1..=SEED_ROWS)
		.map(|g| ((g % 500) + 1, g))
		.chain(stats.inserted.iter().copied())
		.collect();
	let lost = expected.iter().filter(|k| !seen.contains_key(k)).count();
	let extra = seen.keys().filter(|k| !expected.contains(k)).count();
	let mut wrong_updates = 0;
	for (k, (acked, unknown)) in &stats.updated {
		let n = seen.get(k).copied().unwrap_or(-1);
		if n < *acked || n > acked + unknown {
			wrong_updates += 1;
			eprintln!("row {k:?}: n = {n}, {acked} updates acknowledged, {unknown} unknown");
		}
	}
	let mut slow: Vec<(Duration, Duration)> = stats.latency.clone();
	slow.sort_by_key(|(_, d)| std::cmp::Reverse(*d));
	eprintln!(
		"slowest writes (started at, took): {:?}",
		slow.iter()
			.take(5)
			.map(|(s, d)| (s.as_secs_f64(), d.as_millis()))
			.collect::<Vec<_>>()
	);
	let mut all: Vec<f64> = stats
		.latency
		.iter()
		.map(|(_, d)| d.as_secs_f64() * 1000.0)
		.collect();
	let writes = all.len();
	let p50 = percentile(&mut all, 0.50);
	let p99 = percentile(&mut all, 0.99);
	let p999 = percentile(&mut all, 0.999);
	let max = all.last().copied().unwrap_or(0.0);
	let mut cut: Vec<f64> = jobs.iter().flat_map(pauses).collect();
	let cutovers = cut.len();
	let cut_p99 = percentile(&mut cut, 0.99);
	let cut_max = cut.last().copied().unwrap_or(0.0);
	let version = home
		.query_one("select current_setting('server_version_num')", &[])
		.await
		.unwrap()
		.get::<_, String>(0);
	eprintln!(
		"GATE pg {version}: {writes} writes ({} inserts, {} rows updated) in {:.1}s; lost {lost}, duplicated {dup}, unexpected {extra}, wrong updates {wrong_updates}",
		stats.inserted.len(),
		stats.updated.len(),
		t0.elapsed().as_secs_f64()
	);
	eprintln!(
		"GATE pg {version}: client write latency p50 {p50:.1} ms, p99 {p99:.1} ms, p99.9 {p999:.1} ms, max {max:.1} ms"
	);
	eprintln!(
		"GATE pg {version}: {cutovers} cutovers, measured pause p99 {cut_p99:.1} ms, max {cut_max:.1} ms; retried errors {:?}, stale updates {}",
		stats.errors, stats.stale_updates
	);
	assert_eq!((lost, dup, extra, wrong_updates), (0, 0, 0, 0));
	assert!(
		cut_max <= 2000.0,
		"a cutover paused writes for {cut_max} ms"
	);
	assert!(p99 <= 2000.0, "p99 write latency {p99} ms");

	// Nothing a job made is left on any node.
	for (_, a) in &nodes {
		let c = connect(a, "postgres", "x").await;
		let left: i64 = c
			.query_one(
				"select (select count(*) from pg_replication_slots where slot_name like 'lepis_j%') \
				+ (select count(*) from pg_publication where pubname like 'lepis_j%') \
				+ (select count(*) from pg_subscription where subname like 'lepis_j%')",
				&[],
			)
			.await
			.unwrap()
			.get(0);
		assert_eq!(
			left, 0,
			"{a} still has a slot, publication or subscription a job made"
		);
	}
	let st = admin.status().await;
	assert!(
		st["routers"]
			.as_array()
			.unwrap()
			.iter()
			.any(|r| r["current"] == true),
		"{st:#}"
	);
}

/// Every 200 ms, on every node: client statements running for over a second, what they wait on,
/// and what holds the lock they want. Printed once per statement, when first seen, so a stalled
/// write can be matched to its blocker (or shown to have none).
async fn sample_stalls(nodes: Vec<(NodeId, String)>, stop: Arc<AtomicBool>, t0: Instant) {
	let mut conns = Vec::new();
	for (id, a) in &nodes {
		conns.push((id.0, connect(a, "postgres", "x").await));
	}
	let mut seen = HashSet::new();
	while !stop.load(Ordering::Relaxed) {
		for (n, c) in &conns {
			let Ok(rows) = c
				.simple_query(
					"select a.pid, a.query_start::text, coalesce(a.wait_event_type, '-'), coalesce(a.wait_event, '-'), \
					round(extract(epoch from now() - a.query_start)::numeric, 1)::text, \
					left(regexp_replace(a.query, '[[:space:]]+', ' ', 'g'), 70), pg_blocking_pids(a.pid)::text, \
					coalesce((select string_agg(b.pid || ' ' || coalesce(b.application_name, '') || ' [' || b.state || '] ' \
						|| left(regexp_replace(b.query, '[[:space:]]+', ' ', 'g'), 90), ' | ') \
						from pg_stat_activity b where b.pid = any (pg_blocking_pids(a.pid))), '-') \
					from pg_stat_activity a where a.state = 'active' and a.pid <> pg_backend_pid() \
					and a.backend_type = 'client backend' and now() - a.query_start > interval '1 second' \
					and a.application_name not like 'snout-lepis%'",
				)
				.await
			else {
				continue;
			};
			for m in rows {
				if let SimpleQueryMessage::Row(r) = m {
					let key = (
						*n,
						r.get(0).unwrap_or("").to_string(),
						r.get(1).unwrap_or("").to_string(),
					);
					if seen.insert(key) {
						eprintln!(
							"STALL t={:.1}s node {n} pid {} waiting {}s on {}/{}: {} | blocked by {} = {}",
							t0.elapsed().as_secs_f64(),
							r.get(0).unwrap_or(""),
							r.get(4).unwrap_or(""),
							r.get(2).unwrap_or(""),
							r.get(3).unwrap_or(""),
							r.get(5).unwrap_or(""),
							r.get(6).unwrap_or(""),
							r.get(7).unwrap_or(""),
						);
					}
				}
			}
		}
		tokio::time::sleep(Duration::from_millis(200)).await;
	}
}
