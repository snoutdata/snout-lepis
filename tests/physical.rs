//! L8's PHYSICAL move (Phase 6) against a real primary and a real streaming standby of it, under a
//! write load through Lepis: `node.attach` joins the standby, `range.split` moves half the
//! keyspace onto it by promoting it, and afterwards no row is lost, duplicated or left on a node
//! that does not own it. Then the promoted node is an ordinary node: a merge moves its range back
//! with the logical path.
//!
//! How the standby was made is not under test (in the Cloud it is a pgBackRest restore from S3,
//! `packages/snoutpod`); here it is `pg_basebackup -R`. Needs `LEPIS_PHYS_PRIMARY` and
//! `LEPIS_PHYS_STANDBY` (`scripts/it-physical.sh`), and skips without them.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use lepis::config::Config;
use lepis::hash::KeyType;
use lepis::server::{self, App};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

const TOKEN: &str = "a-physical-admin-token-for-tests";
const SEED: u64 = 4242;
const SEED_ROWS: i64 = 20_000;
const WORKERS: usize = 4;

fn env(k: &str) -> Option<String> {
	std::env::var(k).ok().filter(|v| !v.is_empty())
}

async fn connect(address: &str, user: &str, password: &str) -> Option<Client> {
	let (host, port) = address.rsplit_once(':')?;
	let (client, conn) = tokio_postgres::Config::new()
		.host(host)
		.port(port.parse().ok()?)
		.user(user)
		.password(password)
		.dbname("postgres")
		.connect_timeout(Duration::from_secs(5))
		.connect(NoTls)
		.await
		.ok()?;
	tokio::spawn(conn);
	Some(client)
}

async fn admin_of(address: &str) -> Client {
	connect(address, "postgres", "x")
		.await
		.unwrap_or_else(|| panic!("postgres@{address}"))
}

async fn one(c: &Client, sql: &str) -> String {
	c.simple_query(sql)
		.await
		.unwrap_or_else(|e| panic!("{sql}: {e}"))
		.into_iter()
		.find_map(|m| match m {
			SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap_or_default().to_string()),
			_ => None,
		})
		.unwrap_or_default()
}

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

	async fn op(&self, op: Value) -> Value {
		let (status, plan) = self.call("POST", "/v1/plan", Some(&op)).await;
		assert_eq!(status, 200, "plan {op}: {plan}");
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

/// (lo, hi, node) of the keyspace's ranges.
fn ranges(status: &Value, keyspace: &str) -> Vec<(i64, i64, i64)> {
	let ks = status["keyspaces"]
		.as_array()
		.unwrap()
		.iter()
		.find(|k| k["name"] == keyspace)
		.unwrap_or_else(|| panic!("no keyspace {keyspace} in {status}"));
	ks["ranges"]
		.as_array()
		.unwrap()
		.iter()
		.map(|r| {
			let n = |v: &Value| {
				v.as_i64()
					.or_else(|| v.as_str().and_then(|s| s.parse().ok()))
					.unwrap()
			};
			(n(&r["lo"]), n(&r["hi"]), n(&r["node"]))
		})
		.collect()
}

#[derive(Default)]
struct Stats {
	inserted: Vec<(i64, i64)>,
	errors: BTreeMap<String, usize>,
}

/// Inserts through Lepis until stopped, retrying what a cutover refuses; an insert that meets
/// its own key on a retry had landed the first time.
async fn load(
	lepis: String,
	w: u64,
	stop: Arc<AtomicBool>,
	next: Arc<AtomicI64>,
	stats: Arc<Mutex<Stats>>,
) {
	let mut x = 0x9e37_79b9_7f4a_7c15u64 ^ (w + 1).wrapping_mul(0x2545_f491_4f6c_dd1d);
	let mut client = connect(&lepis, "phys_app", "app-pw").await;
	while !stop.load(Ordering::Relaxed) {
		x ^= x << 13;
		x ^= x >> 7;
		x ^= x << 17;
		let acct = (x % 1000) as i64 + 1;
		let id = next.fetch_add(1, Ordering::Relaxed);
		let sql =
			format!("insert into physx.ledger (acct, id, note) values ({acct}, {id}, 'w{w}')");
		let mut attempt = 0;
		loop {
			attempt += 1;
			assert!(attempt < 2000, "{sql} never landed");
			if client.is_none() {
				client = connect(&lepis, "phys_app", "app-pw").await;
				if client.is_none() {
					tokio::time::sleep(Duration::from_millis(20)).await;
					continue;
				}
			}
			match client.as_ref().unwrap().simple_query(&sql).await {
				Ok(_) => break,
				Err(e) => {
					let code = e.as_db_error().map(|d| d.code().code().to_string());
					if attempt > 1 && code.as_deref() == Some("23505") {
						break;
					}
					*stats
						.lock()
						.unwrap()
						.errors
						.entry(code.unwrap_or_else(|| "session".into()))
						.or_default() += 1;
					client = None;
					tokio::time::sleep(Duration::from_millis(5)).await;
				}
			}
		}
		stats.lock().unwrap().inserted.push((acct, id));
	}
}

async fn rows(c: &Client) -> Vec<(i64, i64)> {
	c.query("select acct, id from physx.ledger", &[])
		.await
		.unwrap()
		.iter()
		.map(|r| (r.get::<_, i64>(0), r.get::<_, i64>(1)))
		.collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_standby_takes_half_a_keyspace_by_promotion() {
	let (Some(primary), Some(standby)) = (env("LEPIS_PHYS_PRIMARY"), env("LEPIS_PHYS_STANDBY"))
	else {
		eprintln!("LEPIS_PHYS_PRIMARY / LEPIS_PHYS_STANDBY not set; skipped");
		return;
	};
	let home = admin_of(&primary).await;
	let (phost, pport) = primary.rsplit_once(':').unwrap();
	home.batch_execute(&format!(
		"drop schema if exists physx cascade; drop schema if exists lepis cascade; drop schema if exists lepis_move cascade;
		do $$ begin
			if not exists (select from pg_roles where rolname = 'phys_app') then
				create role phys_app login password 'app-pw';
			end if;
		end $$;
		create schema physx;
		create table physx.ledger (acct bigint not null, id bigint not null, note text, primary key (acct, id));
		create table physx.plans (id int primary key, name text not null);
		insert into physx.plans values (1, 'free'), (2, 'pro');
		grant usage on schema physx to phys_app;
		grant select, insert, update, delete on all tables in schema physx to phys_app;
		insert into physx.ledger select (g % 500) + 1, g, 'seed' from generate_series(1, {SEED_ROWS}) g;
		{catalog}
		insert into lepis.node (id, name, host, port, dbname, sslmode, kind, state)
			values (1, 'n1', '{phost}', {pport}, 'postgres', 'disable', 'home', 'active');
		insert into lepis.relation values ('physx', 'plans', 'global', null, null);
		select lepis.bump();",
		catalog = lepis::catalog::CATALOG_SQL,
	))
	.await
	.expect("setup");
	// The standby has the setup before anything reads it there.
	let sb = admin_of(&standby).await;
	assert_eq!(
		one(&sb, "select pg_is_in_recovery()").await,
		"t",
		"{standby} is not a standby"
	);
	let deadline = Instant::now() + Duration::from_secs(60);
	while one(
		&sb,
		"select count(*) from pg_namespace where nspname = 'lepis'",
	)
	.await != "1"
		|| one(
			&sb,
			"select coalesce((select count(*) from physx.ledger), 0)",
		)
		.await != SEED_ROWS.to_string()
	{
		assert!(
			Instant::now() < deadline,
			"the standby did not replay the setup"
		);
		tokio::time::sleep(Duration::from_millis(200)).await;
	}

	let config: HashMap<String, String> = [
		("LEPIS_HOME", primary.clone()),
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

	admin
		.op(json!({"op": "keyspace.create", "name": "acct", "key_type": "bigint", "seed": SEED, "ranges": 2, "nodes": [1]}))
		.await;
	admin
		.op(
			json!({"op": "table.distribute", "table": "physx.ledger", "column": "acct", "keyspace": "acct"}),
		)
		.await;

	// A standby of a node it is not attached as is refused, and so is a name already taken.
	let (sh, sp) = standby.rsplit_once(':').unwrap();
	let attach = json!({"op": "node.attach", "name": "n2", "host": sh, "port": sp.parse::<u16>().unwrap(),
		"dbname": "postgres", "sslmode": "disable", "standby_of": 1});
	let (code, e) = admin
		.call("POST", "/v1/plan", Some(&json!({"op": "node.attach", "name": "n1", "host": sh, "port": 5432, "sslmode": "disable", "standby_of": 1})))
		.await;
	assert_eq!(
		(code, e["error"]["kind"].as_str()),
		(409, Some("node_name_taken")),
		"{e}"
	);
	let (code, e) = admin
		.call("POST", "/v1/plan", Some(&json!({"op": "node.attach", "name": "n9", "host": phost, "port": pport.parse::<u16>().unwrap(), "sslmode": "disable", "standby_of": 1})))
		.await;
	assert_eq!(code, 409, "the primary itself is not a standby: {e}");
	assert!(
		e["error"]["message"]
			.as_str()
			.unwrap()
			.contains("not in recovery"),
		"{e}"
	);

	// The load, through Lepis, for the rest of the test.
	let stop = Arc::new(AtomicBool::new(false));
	let next = Arc::new(AtomicI64::new(1_000_000));
	let stats = Arc::new(Mutex::new(Stats::default()));
	let mut workers = Vec::new();
	for w in 0..WORKERS {
		workers.push(tokio::spawn(load(
			lepis.clone(),
			w as u64,
			stop.clone(),
			next.clone(),
			stats.clone(),
		)));
	}
	tokio::time::sleep(Duration::from_secs(2)).await;

	let j = admin.op(attach.clone()).await;
	assert_eq!(
		j["steps"][0]["detail"]["attached"]["wal_receiver"], "streaming",
		"{j:#}"
	);
	let st = admin.status().await;
	let n2 = st["nodes"]
		.as_array()
		.unwrap()
		.iter()
		.find(|n| n["name"] == "n2")
		.unwrap();
	assert_eq!(n2["state"], "joining", "{st}");

	// The upper range moves to the standby, which is promoted to take it.
	let r = ranges(&st, "acct");
	let upper = *r.iter().max_by_key(|x| x.0).unwrap();
	let j = admin
		.op(
			json!({"op": "range.move", "keyspace": "acct", "range": upper.0.to_string(), "to": "n2"}),
		)
		.await;
	let step = j["steps"]
		.as_array()
		.unwrap()
		.iter()
		.find(|s| s["kind"] == "transfer")
		.unwrap();
	let d = &step["detail"];
	assert_eq!(d["strategy"], "physical", "{j:#}");
	assert_eq!(d["phase"], "cleaned", "{j:#}");
	for v in d["verify"].as_array().unwrap() {
		assert_eq!(v["equal"], true, "{v}");
	}
	eprintln!("physical cutover: {}", d["cutover"]);
	eprintln!("cleanup: {}", d["cleanup"]);
	assert_eq!(
		one(&sb, "select pg_is_in_recovery()").await,
		"f",
		"the standby was not promoted"
	);

	tokio::time::sleep(Duration::from_secs(2)).await;
	stop.store(true, Ordering::Relaxed);
	for w in workers {
		w.await.unwrap();
	}
	let stats = std::mem::take(&mut *stats.lock().unwrap());
	eprintln!(
		"{} inserts through Lepis, refusals retried: {:?}",
		stats.inserted.len(),
		stats.errors
	);

	// Catalog: n2 is active, owns the upper range, is no longer labelled a standby.
	let st = admin.status().await;
	assert!(ranges(&st, "acct").contains(&(upper.0, upper.1, 2)), "{st}");
	let n2 = st["nodes"]
		.as_array()
		.unwrap()
		.iter()
		.find(|n| n["name"] == "n2")
		.unwrap();
	assert_eq!(n2["state"], "active", "{st}");
	assert_eq!(
		one(
			&home,
			"select coalesce(labels->>'standby_of', 'none') from lepis.node where id = 2"
		)
		.await,
		"none"
	);

	// Every row exactly once, each on its owner; nothing left on a node that does not own it.
	let owner = |acct: i64| {
		let h = KeyType::Int8
			.hash_text_value(&acct.to_string(), SEED)
			.unwrap();
		if (upper.0..=upper.1).contains(&h) {
			2
		} else {
			1
		}
	};
	let on1 = rows(&home).await;
	let on2 = rows(&sb).await;
	assert!(
		on1.iter().all(|(a, _)| owner(*a) == 1),
		"node 1 holds rows it does not own"
	);
	assert!(
		on2.iter().all(|(a, _)| owner(*a) == 2),
		"node 2 holds rows it does not own"
	);
	assert!(!on2.is_empty() && !on1.is_empty());
	let all: BTreeSet<(i64, i64)> = on1.iter().chain(on2.iter()).copied().collect();
	assert_eq!(all.len(), on1.len() + on2.len(), "a row is on both nodes");
	let mut want: BTreeSet<(i64, i64)> = (1..=SEED_ROWS).map(|g| ((g % 500) + 1, g)).collect();
	want.extend(stats.inserted.iter().copied());
	let lost: Vec<_> = want.difference(&all).take(5).collect();
	assert!(
		lost.is_empty(),
		"{} committed rows are missing, e.g. {lost:?}",
		want.difference(&all).count()
	);

	// The promoted node: its copy of the global table and of Lepis's catalog are gone, its
	// fence refuses a key it does not own, and the old owner's fence refuses the moved keys.
	assert_eq!(one(&sb, "select count(*) from physx.plans").await, "0");
	assert_eq!(one(&home, "select count(*) from physx.plans").await, "2");
	assert_eq!(
		one(
			&sb,
			"select count(*) from pg_namespace where nspname = 'lepis'"
		)
		.await,
		"0"
	);
	let moved = (1..10_000).find(|a| owner(*a) == 2).unwrap();
	let kept = (1..10_000).find(|a| owner(*a) == 1).unwrap();
	let e = home
		.simple_query(&format!(
			"insert into physx.ledger values ({moved}, -1, 'direct')"
		))
		.await
		.unwrap_err();
	assert_eq!(e.as_db_error().unwrap().code().code(), "23514", "{e}");
	let e = sb
		.simple_query(&format!(
			"insert into physx.ledger values ({kept}, -1, 'direct')"
		))
		.await
		.unwrap_err();
	assert_eq!(e.as_db_error().unwrap().code().code(), "23514", "{e}");

	// The promoted node is an ordinary node now: the range comes back by the logical path.
	let lower = *ranges(&st, "acct").iter().min_by_key(|x| x.0).unwrap();
	let j = admin
		.op(json!({"op": "range.merge", "keyspace": "acct", "a": lower.0.to_string(), "b": upper.0.to_string()}))
		.await;
	let step = j["steps"]
		.as_array()
		.unwrap()
		.iter()
		.find(|s| s["kind"] == "transfer")
		.unwrap();
	assert_eq!(step["detail"]["strategy"], "logical", "{j:#}");
	let st = admin.status().await;
	assert_eq!(ranges(&st, "acct"), vec![(i64::MIN, i64::MAX, 1)], "{st}");
	admin.op(json!({"op": "verify", "keyspace": "acct"})).await;

	// And a cluster restore point lands on every node, with the LSN each one wrote it at.
	let j = admin
		.op(json!({"op": "restore_point", "name": "phys-test"}))
		.await;
	let lsns = &j["steps"][0]["detail"]["lsns"];
	assert!(
		lsns["1"]["lsn"].as_str().is_some_and(|s| s.contains('/')),
		"{j:#}"
	);
	assert!(
		lsns["2"]["lsn"].as_str().is_some_and(|s| s.contains('/')),
		"{j:#}"
	);
	assert_eq!(
		one(
			&home,
			"select count(*) from lepis.restore_point where name = 'phys-test'"
		)
		.await,
		"1"
	);
	let (code, e) = admin
		.call(
			"POST",
			"/v1/plan",
			Some(&json!({"op": "restore_point", "name": "Not Valid"})),
		)
		.await;
	assert_eq!(code, 400, "{e}");
}
