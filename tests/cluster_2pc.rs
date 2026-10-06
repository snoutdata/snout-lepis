//! Phase 3 against a real three-node cluster: two-phase commit, in-doubt recovery after a crash
//! at every step, a bank-transfer test under coordinator and node kills, DDL fan-out, per-node
//! jobs and role sync.
//!
//! Needs the same bed as `tests/cluster.rs` (`scripts/it.sh` provides it), with
//! `max_prepared_transactions > 0` on every node. Skips without.

#[macro_use]
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common::*;
use lepis::backend;
use lepis::config::{NodeAddress, SslMode};
use lepis::ddl;
use lepis::live;
use lepis::roles;
use lepis::scram::ClientCredential;
use lepis::server::App;
use lepis::twopc::{
	self, Coordinator, NodeSession, Outcome, Participant, Settings, Stop, TwoPcError,
};

/// Recovery that acts at once: what a test runs after a crash it staged.
const NOW: Settings = Settings {
	abort_grace: Duration::ZERO,
	commit_grace: Duration::ZERO,
	abort_retain: Duration::from_secs(3600),
	interval: Duration::from_millis(100),
};

async fn app(b: &Bed) -> Arc<App> {
	let config: HashMap<String, String> = [
		("LEPIS_HOME", b.home.clone()),
		("LEPIS_HOME_SSLMODE", "disable".into()),
		("LEPIS_SERVICE_USER", "postgres".into()),
		("LEPIS_SERVICE_PASSWORD", "x".into()),
	]
	.into_iter()
	.map(|(k, v)| (k.to_string(), v))
	.collect();
	let app = App::new(lepis::config::Config::from_map(&config).unwrap()).unwrap();
	live::load(&app).await.unwrap();
	app
}

/// A session on one node as `user`, as the router would hold for a client.
async fn session(b: &Bed, node: NodeId, user: &str, password: &str) -> NodeSession {
	let address = &node_ids(b)[node.0 as usize - 1].1;
	let (host, port) = address.rsplit_once(':').unwrap();
	let a = NodeAddress {
		host: host.into(),
		port: port.parse().unwrap(),
		sslmode: SslMode::Disable,
		ca_file: None,
	};
	let params = vec![
		("user".to_string(), user.to_string()),
		("database".to_string(), "postgres".to_string()),
	];
	let backend = backend::connect(
		&a,
		None,
		&params,
		ClientCredential::Password(password.into()),
	)
	.await
	.unwrap_or_else(|e| panic!("{user}@{address}: {e}"));
	NodeSession { node, backend }
}

async fn sessions(b: &Bed, user: &str, password: &str) -> Vec<NodeSession> {
	let mut out = Vec::new();
	for (id, _) in node_ids(b) {
		out.push(session(b, id, user, password).await);
	}
	out
}

async fn on_every_node(b: &Bed, sql: &str) {
	for (_, a) in node_ids(b) {
		connect(&a, "postgres", "x")
			.await
			.batch_execute(sql)
			.await
			.unwrap_or_else(|e| panic!("{a}: {e}"));
	}
}

/// One query's single value on every node, in node order.
async fn each_node(b: &Bed, sql: &str) -> Vec<String> {
	let mut out = Vec::new();
	for (_, a) in node_ids(b) {
		let rows = connect(&a, "postgres", "x")
			.await
			.simple_query(sql)
			.await
			.unwrap();
		out.push(
			rows.iter()
				.find_map(|m| match m {
					SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap_or("").to_string()),
					_ => None,
				})
				.unwrap_or_default(),
		);
	}
	out
}

/// One query's single value on the home node (where `lepis.*` lives).
async fn on_home(b: &Bed, sql: &str) -> String {
	connect(&b.home, "postgres", "x")
		.await
		.query_one(&format!("select ({sql})::text"), &[])
		.await
		.unwrap()
		.get(0)
}

async fn prepared_left(b: &Bed) -> usize {
	each_node(
		b,
		"select count(*) from pg_prepared_xacts where gid like 'lepis:%'",
	)
	.await
	.iter()
	.map(|v| v.parse::<usize>().unwrap())
	.sum()
}

/// Runs recovery until nothing of this cluster is left prepared.
async fn recover_all(app: &App, settings: &Settings) {
	let catalog = app.catalog().unwrap();
	let mut log = twopc::home_service(app, "test").await.unwrap();
	for _ in 0..50 {
		let r = twopc::recover_once(app, &catalog, &mut log, settings)
			.await
			.unwrap();
		if r.waiting == 0 {
			return;
		}
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
	panic!("recovery did not converge");
}

async fn setup_marks(b: &Bed) {
	on_every_node(
		b,
		"create schema if not exists twopc;
		drop table if exists twopc.marks, twopc.uniq;
		create table twopc.marks (tag text);
		create table twopc.uniq (k int unique deferrable initially deferred);
		grant usage on schema twopc to lepis_app;
		grant select, insert, update, delete on all tables in schema twopc to lepis_app;",
	)
	.await;
}

async fn marks(b: &Bed, tag: &str) -> Vec<usize> {
	each_node(
		b,
		&format!(
			"select count(*) from twopc.marks where tag = {}",
			catalog::quote_literal(tag)
		),
	)
	.await
	.iter()
	.map(|v| v.parse().unwrap())
	.collect()
}

#[tokio::test]
async fn a_write_on_three_nodes_commits_on_all_or_none() {
	let b = need_bed!();
	setup_marks(&b).await;
	let app = app(&b).await;
	let c = Coordinator::new(&app, Settings::default());

	// All three commit.
	let mut ps = sessions(&b, "lepis_app", "app-pw").await;
	for p in &mut ps {
		p.execute("begin; insert into twopc.marks values ('ok')")
			.await
			.unwrap();
	}
	let o: Outcome = c.commit(&mut ps).await.unwrap();
	assert!(o.txid.is_some() && o.unresolved.is_empty(), "{o:?}");
	assert_eq!(marks(&b, "ok").await, vec![1, 1, 1]);
	assert_eq!(prepared_left(&b).await, 0);
	assert_eq!(
		on_home(&b, "select count(*) from lepis.prepared").await,
		"0",
		"a finished transaction leaves no log row"
	);

	// One node's part failed earlier: PREPARE answers ROLLBACK there, and nobody commits.
	for p in &mut ps {
		p.execute("begin; insert into twopc.marks values ('failed')")
			.await
			.unwrap();
	}
	assert!(ps[1].execute("select 1/0").await.is_err());
	let e = c.commit(&mut ps).await.unwrap_err();
	assert!(matches!(e, TwoPcError::Aborted { .. }), "{e}");
	assert_eq!(marks(&b, "failed").await, vec![0, 0, 0]);

	// A deferred unique check fails at PREPARE: the node's own error reaches the client.
	on_every_node(&b, "insert into twopc.uniq values (7)").await;
	for (i, p) in ps.iter_mut().enumerate() {
		let k = if i == 2 { 7 } else { 8 + i };
		p.execute(&format!("begin; insert into twopc.uniq values ({k})"))
			.await
			.unwrap();
	}
	let e = c.commit(&mut ps).await.unwrap_err();
	let fields = lepis::wire::parse_error_fields(&e.to_message().body);
	assert!(fields.contains(&(b'C', "23505".into())), "{fields:?}");
	assert_eq!(
		each_node(&b, "select count(*) from twopc.uniq").await,
		vec!["1", "1", "1"]
	);
	assert_eq!(prepared_left(&b).await, 0);

	// Every session is usable afterwards.
	for p in &mut ps {
		assert_eq!(p.execute("select 1").await.unwrap(), "SELECT 1");
	}
}

#[tokio::test]
async fn recovery_converges_after_a_crash_at_every_step() {
	let b = need_bed!();
	setup_marks(&b).await;
	let app = app(&b).await;
	let c = Coordinator::new(&app, Settings::default());
	let n = node_ids(&b).len();
	let mut stops = Vec::new();
	for k in 0..=n {
		stops.push(Stop::AfterPrepared(k));
	}
	stops.push(Stop::AfterDecision);
	for k in 0..=n {
		stops.push(Stop::AfterCommitted(k));
	}
	for (i, stop) in stops.iter().enumerate() {
		let tag = format!("crash-{i}");
		let mut ps = sessions(&b, "lepis_app", "app-pw").await;
		for p in &mut ps {
			p.execute(&format!("begin; insert into twopc.marks values ('{tag}')"))
				.await
				.unwrap();
		}
		assert!(matches!(
			c.commit_until(&mut ps, *stop).await,
			Err(TwoPcError::Stopped)
		));
		// The router dies: its sessions close. An open transaction goes with its session; a
		// prepared one stays until recovery decides it.
		drop(ps);
		tokio::time::sleep(Duration::from_millis(200)).await;
		recover_all(&app, &NOW).await;
		let decided = matches!(stop, Stop::AfterDecision | Stop::AfterCommitted(_));
		let want = if decided { vec![1; n] } else { vec![0; n] };
		assert_eq!(marks(&b, &tag).await, want, "{stop:?}");
		assert_eq!(prepared_left(&b).await, 0, "{stop:?}");
		eprintln!(
			"{stop:?}: {}",
			if decided { "committed" } else { "rolled back" }
		);
	}
	// Commit decisions are forgotten once finished; abort decisions are kept.
	assert_eq!(
		on_home(
			&b,
			"select count(*) from lepis.prepared where decision = 'commit'"
		)
		.await,
		"0"
	);
}

#[tokio::test]
async fn recovery_leaves_young_transactions_alone_and_one_router_leads() {
	let b = need_bed!();
	setup_marks(&b).await;
	let app = app(&b).await;
	let c = Coordinator::new(&app, Settings::default());
	let mut ps = sessions(&b, "lepis_app", "app-pw").await;
	for p in &mut ps {
		p.execute("begin; insert into twopc.marks values ('young')")
			.await
			.unwrap();
	}
	let _ = c.commit_until(&mut ps, Stop::AfterPrepared(3)).await;
	drop(ps);
	let patient = Settings::default();
	let catalog = app.catalog().unwrap();
	let mut log = twopc::home_service(&app, "test").await.unwrap();
	let r = twopc::recover_once(&app, &catalog, &mut log, &patient)
		.await
		.unwrap();
	assert_eq!((r.waiting, r.rolled_back), (3, 0), "{r:?}");
	assert_eq!(prepared_left(&b).await, 3);
	recover_all(&app, &NOW).await;
	assert_eq!(marks(&b, "young").await, vec![0, 0, 0]);

	// The register is written once: the first decision holds.
	let mut log2 = twopc::home_service(&app, "test").await.unwrap();
	let t = 4242;
	assert_eq!(
		twopc::decide(&mut log, t, &[NodeId(1)], twopc::Decision::Commit)
			.await
			.unwrap(),
		twopc::Decision::Commit
	);
	assert_eq!(
		twopc::decide(&mut log2, t, &[], twopc::Decision::Abort)
			.await
			.unwrap(),
		twopc::Decision::Commit
	);
	log.query("delete from lepis.prepared where txid = 4242", &[])
		.await
		.unwrap();

	// One leader: the lock is the session's, and passes on when that session ends.
	assert!(twopc::try_lead(&mut log).await.unwrap());
	assert!(!twopc::try_lead(&mut log2).await.unwrap());
	log.close().await;
	let mut led = false;
	for _ in 0..20 {
		if twopc::try_lead(&mut log2).await.unwrap() {
			led = true;
			break;
		}
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
	assert!(led);
}

// ---------------------------------------------------------------------------------------------
// The bank.

const ACCOUNTS: i64 = 30;
const OPENING: i64 = 1000;
const WORKERS: u64 = 6;
const TRANSFERS: u64 = 40;

fn home_of(account: i64, nodes: usize) -> NodeId {
	NodeId((account % nodes as i64) as i32 + 1)
}

struct Rng(u64);
impl Rng {
	fn next(&mut self) -> u64 {
		self.0 ^= self.0 << 13;
		self.0 ^= self.0 >> 7;
		self.0 ^= self.0 << 17;
		self.0
	}
	fn below(&mut self, n: u64) -> u64 {
		self.next() % n
	}
}

/// What the client was told, and so what must be true afterwards.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Told {
	/// Committed (or the coordinator died after deciding to): applied, all of it.
	Applied,
	/// Failed or died before deciding: none of it.
	NotApplied,
	/// In doubt: all or none.
	Either,
}

async fn transfer(b: &Bed, c: &Coordinator, rng: &mut Rng, txn: i64) -> Told {
	let n = node_ids(b).len();
	let from = rng.below(ACCOUNTS as u64) as i64;
	let mut to = rng.below(ACCOUNTS as u64) as i64;
	if to == from {
		to = (to + 1) % ACCOUNTS;
	}
	let amount = 1 + rng.below(50) as i64;
	// Lock in account order everywhere, so two transfers never wait on each other across nodes.
	let mut legs = [(from, -amount), (to, amount)];
	legs.sort();
	let mut nodes: Vec<NodeId> = legs.iter().map(|(a, _)| home_of(*a, n)).collect();
	nodes.dedup();
	let mut ps = Vec::new();
	for node in &nodes {
		let mut s = session(b, *node, "lepis_app", "app-pw").await;
		if s.execute("set lock_timeout = '10s'; begin").await.is_err() {
			return Told::NotApplied;
		}
		ps.push(s);
	}
	for (account, delta) in legs {
		let i = nodes
			.iter()
			.position(|x| *x == home_of(account, n))
			.unwrap();
		let sql = format!(
			"update bank.accounts set balance = balance + ({delta}) where id = {account};
			insert into bank.ledger values ({txn}, {account}, {delta})"
		);
		if ps[i].execute(&sql).await.is_err() {
			return Told::NotApplied;
		}
	}
	let fault = rng.below(100);
	let stop = match fault {
		0..60 => None,
		60..68 => Some(Stop::AfterPrepared(1)),
		68..74 => Some(Stop::AfterPrepared(ps.len())),
		74..82 => Some(Stop::AfterDecision),
		82..90 => Some(Stop::AfterCommitted(1)),
		_ => {
			// A node drops the session before the commit.
			let (node, pid) = ps.last().map(|v| (v.node, v.backend.pid)).unwrap();
			let address = &node_ids(b)[node.0 as usize - 1].1;
			connect(address, "postgres", "x")
				.await
				.execute("select pg_terminate_backend($1)", &[&pid])
				.await
				.unwrap();
			tokio::time::sleep(Duration::from_millis(50)).await;
			None
		}
	};
	let result = match stop {
		Some(s) => c.commit_until(&mut ps, s).await,
		None => c.commit(&mut ps).await,
	};
	match (result, stop) {
		(Ok(_), _) => Told::Applied,
		(Err(TwoPcError::Stopped), Some(Stop::AfterDecision | Stop::AfterCommitted(_))) => {
			Told::Applied
		}
		(Err(TwoPcError::Stopped), _) => Told::NotApplied,
		(Err(TwoPcError::Aborted { .. }), _) => Told::NotApplied,
		(Err(TwoPcError::InDoubt { .. }), _) => Told::Either,
	}
}

#[tokio::test]
async fn bank_transfers_under_coordinator_and_node_kills_lose_nothing() {
	let b = need_bed!();
	let n = node_ids(&b).len();
	on_every_node(
		&b,
		"create schema if not exists bank;
		drop table if exists bank.accounts, bank.ledger;
		create table bank.accounts (id bigint primary key, balance bigint not null);
		create table bank.ledger (txn bigint not null, account bigint not null, delta bigint not null);
		grant usage on schema bank to lepis_app;
		grant select, insert, update on all tables in schema bank to lepis_app;",
	)
	.await;
	for (id, a) in node_ids(&b) {
		let rows: Vec<String> = (0..ACCOUNTS)
			.filter(|acc| home_of(*acc, n) == id)
			.map(|acc| format!("({acc}, {OPENING})"))
			.collect();
		connect(&a, "postgres", "x")
			.await
			.batch_execute(&format!(
				"insert into bank.accounts values {}",
				rows.join(",")
			))
			.await
			.unwrap();
	}

	let app = app(&b).await;
	let settings = Settings {
		abort_grace: Duration::from_secs(2),
		commit_grace: Duration::from_millis(500),
		abort_retain: Duration::from_secs(3600),
		interval: Duration::from_millis(200),
	};
	let c = Arc::new(Coordinator::new(&app, settings));

	// Recovery runs throughout, beside the live coordinators.
	let done = Arc::new(AtomicBool::new(false));
	let recovery = {
		let app = app.clone();
		let done = done.clone();
		tokio::spawn(async move {
			let catalog = app.catalog().unwrap();
			let mut log = twopc::home_service(&app, "test").await.unwrap();
			let mut passes = 0;
			while !done.load(Ordering::Relaxed) {
				twopc::recover_once(&app, &catalog, &mut log, &settings)
					.await
					.unwrap();
				passes += 1;
				tokio::time::sleep(settings.interval).await;
			}
			passes
		})
	};

	let bed = Arc::new(b);
	let mut workers = Vec::new();
	for w in 0..WORKERS {
		let c = c.clone();
		let bed = bed.clone();
		workers.push(tokio::spawn(async move {
			let mut rng = Rng(0x9e37_79b9_7f4a_7c15 ^ (w + 1).wrapping_mul(0x2545_f491_4f6c_dd1d));
			let mut told = Vec::new();
			for i in 0..TRANSFERS {
				let txn = (w * 1000 + i) as i64;
				told.push((txn, transfer(&bed, &c, &mut rng, txn).await));
			}
			told
		}));
	}
	let mut told = HashMap::new();
	for w in workers {
		told.extend(w.await.unwrap());
	}
	done.store(true, Ordering::Relaxed);
	let passes = recovery.await.unwrap();
	let b = Arc::try_unwrap(bed).ok().unwrap();
	recover_all(&app, &NOW).await;
	assert_eq!(prepared_left(&b).await, 0);

	// Read every node's books.
	let mut balance: HashMap<i64, i64> = HashMap::new();
	let mut legs: HashMap<i64, Vec<(i64, i64)>> = HashMap::new();
	for (_, a) in node_ids(&b) {
		let admin = connect(&a, "postgres", "x").await;
		for r in admin
			.query("select id, balance from bank.accounts", &[])
			.await
			.unwrap()
		{
			balance.insert(r.get(0), r.get(1));
		}
		for r in admin
			.query("select txn, account, delta from bank.ledger", &[])
			.await
			.unwrap()
		{
			legs.entry(r.get(0)).or_default().push((r.get(1), r.get(2)));
		}
	}
	let total: i64 = balance.values().sum();
	assert_eq!(total, ACCOUNTS * OPENING, "money was created or lost");
	for (acc, bal) in &balance {
		let moved: i64 = legs
			.values()
			.flatten()
			.filter(|(a, _)| a == acc)
			.map(|(_, d)| d)
			.sum();
		assert_eq!(
			*bal,
			OPENING + moved,
			"account {acc} disagrees with its ledger"
		);
	}
	let mut counts: HashMap<String, usize> = HashMap::new();
	for (txn, t) in &told {
		let l = legs.get(txn).map_or(0, Vec::len);
		assert!(l == 0 || l == 2, "transfer {txn} is half applied: {l} legs");
		match t {
			Told::Applied => assert_eq!(l, 2, "transfer {txn} was committed but is missing"),
			Told::NotApplied => assert_eq!(l, 0, "transfer {txn} failed but was applied"),
			Told::Either => {}
		}
		*counts.entry(format!("{t:?}")).or_default() += 1;
	}
	assert_eq!(told.len(), (WORKERS * TRANSFERS) as usize);
	eprintln!("{counts:?}, {passes} recovery passes during the run");
	assert!(
		counts.get("Applied").copied().unwrap_or(0) > 100,
		"{counts:?}"
	);
}

// ---------------------------------------------------------------------------------------------
// DDL and roles.

#[tokio::test]
async fn ddl_reaches_every_node_or_none_and_jobs_report_per_node() {
	let b = need_bed!();
	on_every_node(
		&b,
		"drop schema if exists ddl2pc cascade; create schema ddl2pc;
		create table ddl2pc.ref (k int primary key, v text);
		insert into ddl2pc.ref values (1, 'a'), (2, 'b');",
	)
	.await;
	let app = app(&b).await;
	let c = Coordinator::new(&app, Settings::default());
	let mut ps = sessions(&b, "postgres", "x").await;

	// Transactional DDL: everywhere.
	c.run_everywhere(&mut ps, "create table ddl2pc.a (id int)", false)
		.await
		.unwrap();
	assert_eq!(
		each_node(&b, "select to_regclass('ddl2pc.a') is not null").await,
		vec!["t", "t", "t"]
	);
	// …or nowhere: node 3 refuses, and nodes 1 and 2 are left as they were.
	connect(&node_ids(&b)[2].1, "postgres", "x")
		.await
		.batch_execute("create table ddl2pc.b (id int)")
		.await
		.unwrap();
	let e = c
		.run_everywhere(&mut ps, "create table ddl2pc.b (id int, extra int)", false)
		.await
		.unwrap_err();
	assert!(
		matches!(
			e,
			TwoPcError::Aborted {
				node: Some(NodeId(3)),
				..
			}
		),
		"{e}"
	);
	assert_eq!(
		each_node(
			&b,
			"select count(*) from pg_attribute where attrelid = to_regclass('ddl2pc.b') and attnum > 0"
		)
		.await,
		vec!["0", "0", "1"]
	);

	// A reference-table write: every copy, with the same count.
	c.run_everywhere(&mut ps, "update ddl2pc.ref set v = 'x' where k = 1", true)
		.await
		.unwrap();
	assert_eq!(
		each_node(&b, "select v from ddl2pc.ref where k = 1").await,
		vec!["x", "x", "x"]
	);
	connect(&node_ids(&b)[1].1, "postgres", "x")
		.await
		.batch_execute("delete from ddl2pc.ref where k = 2")
		.await
		.unwrap();
	let e = c
		.run_everywhere(&mut ps, "update ddl2pc.ref set v = 'y' where k = 2", true)
		.await
		.unwrap_err();
	assert!(
		matches!(e, TwoPcError::Aborted { code: "XX001", .. }),
		"{e}"
	);
	assert_eq!(
		each_node(
			&b,
			"select coalesce(max(v), '-') from ddl2pc.ref where k = 2"
		)
		.await,
		vec!["b", "-", "b"]
	);

	// Per-node DDL: a job, with each node's state.
	let mut log = twopc::home_service(&app, "test").await.unwrap();
	let ids: Vec<NodeId> = ps.iter().map(|p| p.node).collect();
	let sql = "create index concurrently a_id on ddl2pc.a (id)";
	let job = ddl::start_job(&mut log, sql, &ids).await.unwrap();
	let status = ddl::run_job(&mut log, job, sql, &mut ps).await.unwrap();
	assert!(status.iter().all(|s| s.state == "done"), "{status:?}");
	assert_eq!(
		each_node(
			&b,
			"select indisvalid from pg_index where indexrelid = to_regclass('ddl2pc.a_id')"
		)
		.await,
		vec!["t", "t", "t"]
	);
	// One node already has it: that node fails, the others are done, and the job says so.
	connect(&node_ids(&b)[1].1, "postgres", "x")
		.await
		.batch_execute("create index a_v on ddl2pc.ref (v)")
		.await
		.unwrap();
	let sql = "create index concurrently a_v on ddl2pc.ref (v)";
	let job = ddl::start_job(&mut log, sql, &ids).await.unwrap();
	let status = ddl::run_job(&mut log, job, sql, &mut ps).await.unwrap();
	let states: Vec<&str> = status.iter().map(|s| s.state.as_str()).collect();
	assert_eq!(states, vec!["done", "failed", "done"], "{status:?}");
	assert!(
		status[1]
			.error
			.as_deref()
			.unwrap_or("")
			.contains("already exists")
	);
	for sql in [
		"vacuum ddl2pc.a",
		"alter system set work_mem = '5MB'",
		"alter system reset work_mem",
	] {
		let job = ddl::start_job(&mut log, sql, &ids).await.unwrap();
		let status = ddl::run_job(&mut log, job, sql, &mut ps).await.unwrap();
		assert!(
			status.iter().all(|s| s.state == "done"),
			"{sql}: {status:?}"
		);
	}

	// The plan for the oracle's own tables, from the live catalog.
	let catalog = app.catalog().unwrap();
	let orders = RelationName {
		schema: "oracle".into(),
		table: "orders".into(),
	};
	assert_eq!(
		ddl::plan(
			"alter table oracle.orders add column note text",
			std::slice::from_ref(&orders),
			&catalog,
			&HashMap::new()
		),
		ddl::DdlPlan::Transactional(ddl::Target::Everywhere)
	);
	// L15 at distribute time, from a real table's facts.
	let home = connect(&b.home, "postgres", "x").await;
	home.batch_execute(
		"create table ddl2pc.s (id serial primary key, k bigint not null, u uuid default gen_random_uuid())",
	)
	.await
	.unwrap();
	let facts = |sql: &'static str| {
		let home = &home;
		async move {
			home.simple_query(&sql.replace("$1", "'ddl2pc.s'"))
				.await
				.unwrap()
				.into_iter()
				.filter_map(|m| match m {
					SimpleQueryMessage::Row(r) => {
						Some((0..r.len()).map(|i| r.get(i).map(str::to_string)).collect())
					}
					_ => None,
				})
				.collect::<Vec<Vec<Option<String>>>>()
		}
	};
	let columns: Vec<ddl::ColumnFacts> = facts(ddl::DISTRIBUTE_COLUMNS_SQL)
		.await
		.into_iter()
		.map(|r| ddl::ColumnFacts {
			name: r[0].clone().unwrap(),
			default: r[1].clone(),
			identity: r[2].as_deref() == Some("t"),
			increment: r[3].as_deref().and_then(|v| v.parse().ok()),
		})
		.collect();
	let unique: Vec<Vec<String>> = facts(ddl::DISTRIBUTE_UNIQUE_SQL)
		.await
		.into_iter()
		.map(|r| {
			r[0].clone()
				.unwrap()
				.split(',')
				.map(str::to_string)
				.collect()
		})
		.collect();
	let s = RelationName {
		schema: "ddl2pc".into(),
		table: "s".into(),
	};
	assert_eq!(columns.len(), 3);
	assert_eq!(columns[0].increment, Some(1));
	assert_eq!(unique, vec![vec!["id".to_string()]]);
	assert!(ddl::distribute_refusal(&s, "k", &columns, &unique).is_some());
	let striped = vec![ddl::ColumnFacts {
		increment: Some(ddl::STRIDE),
		..columns[0].clone()
	}];
	assert!(ddl::distribute_refusal(&s, "id", &striped, &unique).is_none());
	on_every_node(&b, "drop schema ddl2pc cascade").await;
}

async fn verifiers(b: &Bed, role: &str) -> Vec<String> {
	each_node(
		b,
		&format!(
			"select coalesce(rolpassword, '-') from pg_authid where rolname = {}",
			catalog::quote_literal(role)
		),
	)
	.await
}

async fn replicate(app: &App, home: &Client, sql: &str) {
	home.batch_execute(sql).await.unwrap();
	let change = roles::change_of(sql).expect("a role statement");
	let catalog = app.catalog().unwrap();
	for (node, r) in roles::replicate(app, &catalog, &change).await.unwrap() {
		r.unwrap_or_else(|e| panic!("{node}: {e}"));
	}
}

#[tokio::test]
async fn roles_reach_every_node_with_one_verifier() {
	let b = need_bed!();
	on_every_node(
		&b,
		"drop role if exists r2pc; drop role if exists r2pc_b; drop role if exists r2pc_readers;",
	)
	.await;
	let app = app(&b).await;
	let home = connect(&b.home, "postgres", "x").await;

	replicate(&app, &home, "create role r2pc login password 'pw-one'").await;
	let v = verifiers(&b, "r2pc").await;
	assert!(v[0].starts_with("SCRAM-SHA-256$"), "{v:?}");
	assert!(
		v.iter().all(|x| *x == v[0]),
		"one verifier everywhere: {v:?}"
	);
	for (_, a) in node_ids(&b) {
		connect(&a, "r2pc", "pw-one").await;
	}

	replicate(&app, &home, "alter role r2pc set statement_timeout = '7s'").await;
	replicate(
		&app,
		&home,
		"alter role r2pc set search_path = \"$user\", oracle",
	)
	.await;
	let settings = each_node(
		&b,
		"select array_to_string(setconfig, '|') from pg_db_role_setting s join pg_roles r on r.oid = s.setrole where r.rolname = 'r2pc' and s.setdatabase = 0",
	)
	.await;
	assert!(settings.iter().all(|s| *s == settings[0]), "{settings:?}");
	assert!(settings[0].contains("statement_timeout=7s"), "{settings:?}");

	home.batch_execute("create role r2pc_readers")
		.await
		.unwrap();
	replicate(&app, &home, "grant r2pc_readers to r2pc").await;
	assert_eq!(
		each_node(&b, "select pg_has_role('r2pc', 'r2pc_readers', 'member')").await,
		vec!["t", "t", "t"]
	);

	replicate(&app, &home, "alter role r2pc password 'pw-two'").await;
	let v2 = verifiers(&b, "r2pc").await;
	assert_ne!(v2[0], v[0]);
	assert!(v2.iter().all(|x| *x == v2[0]), "{v2:?}");
	for (_, a) in node_ids(&b) {
		connect(&a, "r2pc", "pw-two").await;
	}

	replicate(&app, &home, "alter role r2pc rename to r2pc_b").await;
	assert_eq!(
		each_node(&b, "select count(*) from pg_roles where rolname = 'r2pc'").await,
		vec!["0", "0", "0"]
	);
	assert_eq!(
		each_node(&b, "select pg_has_role('r2pc_b', 'r2pc_readers', 'member')").await,
		vec!["t", "t", "t"]
	);

	// A node that drifted is brought back by reconcile.
	connect(&node_ids(&b)[2].1, "postgres", "x")
		.await
		.batch_execute("alter role r2pc_b nologin; revoke r2pc_readers from r2pc_b")
		.await
		.unwrap();
	let catalog = app.catalog().unwrap();
	for (node, r) in roles::reconcile(&app, &catalog).await.unwrap() {
		r.unwrap_or_else(|e| panic!("{node}: {e}"));
	}
	assert_eq!(
		each_node(&b, "select rolcanlogin::text || pg_has_role('r2pc_b', 'r2pc_readers', 'member')::text from pg_roles where rolname = 'r2pc_b'").await,
		vec!["truetrue", "truetrue", "truetrue"]
	);

	replicate(&app, &home, "drop role r2pc_b").await;
	replicate(&app, &home, "drop role r2pc_readers").await;
	assert_eq!(
		each_node(
			&b,
			"select count(*) from pg_roles where rolname like 'r2pc%'"
		)
		.await,
		vec!["0", "0", "0"]
	);
}
