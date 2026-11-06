//! A router's pool: how many connections it holds on a node, and the line for them (`gate.rs`).
//! Real Postgres, real clients. Each test has a role of its own, so it counts only its own backends,
//! and they run one at a time, since the pool mode is the cluster's.
//!
//! Needs what `scripts/it.sh` provides (`LEPIS_IT_ONLY=cluster_cap`). Skips without.

#[macro_use]
mod common;

use std::time::{Duration, Instant};

use common::*;
use tokio_postgres::{Client, NoTls};

static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn role(b: &Bed, name: &str) {
	connect(&b.home, "postgres", "x")
		.await
		.batch_execute(&format!(
			"drop role if exists {name}; create role {name} login password 'cap-pw'"
		))
		.await
		.unwrap();
}

async fn clear_settings(b: &Bed) {
	connect(&b.home, "postgres", "x")
		.await
		.batch_execute(
			"update lepis.cluster set settings = settings - 'pool_mode'; select lepis.bump()",
		)
		.await
		.unwrap();
	tokio::time::sleep(Duration::from_millis(2100)).await;
}

async fn client(address: &str, name: &str) -> Result<Client, tokio_postgres::Error> {
	let (host, port) = address.rsplit_once(':').unwrap();
	let (c, conn) = tokio_postgres::Config::new()
		.host(host)
		.port(port.parse().unwrap())
		.user(name)
		.password("cap-pw")
		.dbname("postgres")
		.connect(NoTls)
		.await?;
	tokio::spawn(conn);
	Ok(c)
}

/// Backends of this role on the home node, in any state.
async fn backends(b: &Bed, name: &str) -> i64 {
	connect(&b.home, "postgres", "x")
		.await
		.query_one(
			"select count(*) from pg_stat_activity where usename = $1",
			&[&name],
		)
		.await
		.unwrap()
		.get(0)
}

#[tokio::test]
async fn clients_past_the_size_wait_their_turn_and_all_finish() {
	let _one = ONE_AT_A_TIME.lock().await;
	let b = need_bed!();
	clear_settings(&b).await;
	role(&b, "cap_line").await;
	let lepis = start_lepis_with(
		&b,
		&[("LEPIS_POOL_SIZE", "3"), ("LEPIS_QUEUE_WAIT_MS", "20000")],
	)
	.await;
	let mut clients = Vec::new();
	for _ in 0..9 {
		clients.push(client(&lepis, "cap_line").await.unwrap());
	}
	let started = Instant::now();
	let running: Vec<_> = clients
		.iter()
		.map(|c| c.simple_query("select pg_sleep(0.6)"))
		.collect();
	let all = futures_util::future::join_all(running);
	tokio::pin!(all);
	let mut peak = 0;
	let results = loop {
		tokio::select! {
			r = &mut all => break r,
			() = tokio::time::sleep(Duration::from_millis(100)) => {
				peak = peak.max(backends(&b, "cap_line").await);
			}
		}
	};
	for r in results {
		r.expect("a client past the size waits and finishes");
	}
	assert_eq!(peak, 3, "the node saw {peak} connections");
	assert!(
		started.elapsed() >= Duration::from_millis(1800),
		"nine of 0.6 s on three connections took {:?}",
		started.elapsed()
	);
}

#[tokio::test]
async fn idle_clients_hold_nothing_past_the_size() {
	let _one = ONE_AT_A_TIME.lock().await;
	let b = need_bed!();
	clear_settings(&b).await;
	role(&b, "cap_idle").await;
	let lepis = start_lepis_with(
		&b,
		&[("LEPIS_POOL_SIZE", "4"), ("LEPIS_QUEUE_WAIT_MS", "20000")],
	)
	.await;
	let mut clients = Vec::new();
	for _ in 0..40 {
		clients.push(
			client(&lepis, "cap_idle")
				.await
				.expect("forty clients sign in on four connections"),
		);
	}
	tokio::time::sleep(Duration::from_millis(200)).await;
	let held = backends(&b, "cap_idle").await;
	assert!(held <= 4, "forty idle clients hold {held} connections");
	for c in &clients {
		c.simple_query("select 1")
			.await
			.expect("each takes its turn");
	}
	assert!(backends(&b, "cap_idle").await <= 4);
}

#[tokio::test]
async fn a_client_that_waits_too_long_is_told_why() {
	let _one = ONE_AT_A_TIME.lock().await;
	let b = need_bed!();
	clear_settings(&b).await;
	role(&b, "cap_full").await;
	let lepis = start_lepis_with(
		&b,
		&[("LEPIS_POOL_SIZE", "1"), ("LEPIS_QUEUE_WAIT_MS", "1000")],
	)
	.await;
	let waiting = client(&lepis, "cap_full").await.unwrap();
	let holder = client(&lepis, "cap_full").await.unwrap();
	tokio::time::sleep(Duration::from_millis(100)).await;
	holder.batch_execute("begin; select 1").await.unwrap();

	// One already signed in, at its next statement.
	let refused = waiting.simple_query("select 1").await.unwrap_err();
	let db = refused
		.as_db_error()
		.expect("an answer, not a dropped connection");
	assert_eq!(db.code().code(), "53300", "{db:?}");
	assert!(db.message().contains("came free within"), "{db:?}");

	// One signing in.
	let at_door = client(&lepis, "cap_full")
		.await
		.err()
		.expect("no room to sign in");
	assert_eq!(
		at_door.as_db_error().map(|d| d.code().code()),
		Some("53300")
	);

	// The holder is untouched, and its connection comes back to whoever is next.
	holder.batch_execute("commit").await.unwrap();
	tokio::time::sleep(Duration::from_millis(100)).await;
	client(&lepis, "cap_full")
		.await
		.unwrap()
		.simple_query("select 1")
		.await
		.unwrap();
}

#[tokio::test]
async fn an_idle_connection_of_another_role_gives_way() {
	let _one = ONE_AT_A_TIME.lock().await;
	let b = need_bed!();
	clear_settings(&b).await;
	role(&b, "cap_left").await;
	role(&b, "cap_next").await;
	let lepis = start_lepis_with(
		&b,
		&[("LEPIS_POOL_SIZE", "1"), ("LEPIS_QUEUE_WAIT_MS", "20000")],
	)
	.await;
	let left = client(&lepis, "cap_left").await.unwrap();
	left.simple_query("select 1").await.unwrap();
	tokio::time::sleep(Duration::from_millis(150)).await;
	assert_eq!(backends(&b, "cap_left").await, 1);

	let started = Instant::now();
	let next = client(&lepis, "cap_next").await.unwrap();
	next.simple_query("select 1").await.unwrap();
	assert!(
		started.elapsed() < Duration::from_secs(3),
		"it waited {:?}",
		started.elapsed()
	);
	tokio::time::sleep(Duration::from_millis(150)).await;
	assert_eq!(
		backends(&b, "cap_left").await,
		0,
		"the idle connection stays closed"
	);
}

#[tokio::test]
async fn session_mode_is_not_counted() {
	let _one = ONE_AT_A_TIME.lock().await;
	let b = need_bed!();
	clear_settings(&b).await;
	role(&b, "cap_sess").await;
	connect(&b.home, "postgres", "x")
		.await
		.batch_execute("update lepis.cluster set settings = settings || '{\"pool_mode\": \"session\"}'::jsonb; select lepis.bump()")
		.await
		.unwrap();
	tokio::time::sleep(Duration::from_millis(2100)).await;
	let lepis = start_lepis_with(
		&b,
		&[("LEPIS_POOL_SIZE", "1"), ("LEPIS_QUEUE_WAIT_MS", "500")],
	)
	.await;
	let one = client(&lepis, "cap_sess").await.unwrap();
	let two = client(&lepis, "cap_sess").await.unwrap();
	one.simple_query("select 1").await.unwrap();
	two.simple_query("select 1").await.unwrap();
	assert_eq!(backends(&b, "cap_sess").await, 2);
	clear_settings(&b).await;
}
