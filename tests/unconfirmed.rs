//! A commit whose wait for its synchronous standby is cancelled (L21's open caveat, closed in the
//! router): Postgres commits it on the primary and tells the client `COMMIT` with a WARNING, so a
//! client whose driver cancels on a deadline would be told yes for a commit the standby lacks.
//! Through Lepis it gets FATAL 08007 instead, and the session ends. (`statement_timeout` cannot do
//! this: Postgres turns it off before the commit runs.)
//!
//! Its own test binary, because it names a standby that never connects in the node's
//! `synchronous_standby_names` while it runs, and cargo runs test binaries one at a time. Needs
//! `LEPIS_IT_HOME` (a Postgres whose superuser is postgres/x); `scripts/it.sh` provides one.

use std::collections::HashMap;
use std::time::Duration;

use lepis::config::Config;
use lepis::server::{self, App};
use tokio_postgres::{Client, NoTls};

fn home() -> Option<String> {
	std::env::var("LEPIS_IT_HOME")
		.ok()
		.filter(|v| !v.is_empty())
}

async fn connect(host: &str, port: u16) -> Client {
	let (client, conn) = tokio_postgres::Config::new()
		.host(host)
		.port(port)
		.user("postgres")
		.password("x")
		.dbname("postgres")
		.connect(NoTls)
		.await
		.expect("connection");
	tokio::spawn(conn);
	client
}

async fn direct() -> Client {
	let home = home().expect("checked");
	let (host, port) = home.rsplit_once(':').expect("host:port");
	connect(host, port.parse().expect("port")).await
}

async fn names(admin: &Client, value: &str) {
	// One statement each: ALTER SYSTEM refuses a transaction block, and a query string is one.
	admin
		.batch_execute(&format!(
			"alter system set synchronous_standby_names = '{value}'"
		))
		.await
		.expect("alter system");
	admin
		.batch_execute("select pg_reload_conf()")
		.await
		.expect("reload");
	// The checkpointer applies it a moment after a new session sees it.
	for _ in 0..50 {
		let c = direct().await;
		let v: String = c
			.query_one("show synchronous_standby_names", &[])
			.await
			.unwrap()
			.get(0);
		if v == value {
			tokio::time::sleep(Duration::from_millis(500)).await;
			return;
		}
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
	panic!("synchronous_standby_names never became {value:?}");
}

#[tokio::test]
async fn a_commit_cancelled_while_it_waits_for_the_standby_is_never_reported_committed() {
	if home().is_none() {
		eprintln!("LEPIS_IT_HOME not set; skipped");
		return;
	}
	let admin = direct().await;
	admin
		.batch_execute(
			"create table if not exists lepis_unconfirmed (n int primary key); truncate lepis_unconfirmed",
		)
		.await
		.expect("setup");

	let env: HashMap<String, String> = [
		("LEPIS_HOME", home().expect("checked")),
		("LEPIS_HOME_SSLMODE", "disable".into()),
		("LEPIS_SERVICE_USER", "postgres".into()),
		("LEPIS_SERVICE_PASSWORD", "x".into()),
	]
	.into_iter()
	.map(|(k, v)| (k.to_string(), v))
	.collect();
	let app = App::new(Config::from_map(&env).expect("config")).expect("app");
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
		.await
		.expect("bind");
	let addr = listener.local_addr().expect("address");
	tokio::spawn(server::serve(app, listener));
	// Lepis reads its catalog on the home before it is a router; open its first session now,
	// while commits still go through.
	let lepis = connect(&addr.ip().to_string(), addr.port()).await;

	names(&admin, "lepis_test_absent").await;
	// Each commit waits for a standby that never comes, until its client cancels it.
	async fn cancelled(c: &Client, n: i32) -> Result<(), tokio_postgres::Error> {
		let token = c.cancel_token();
		tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(500)).await;
			let _ = token.cancel_query(NoTls).await;
		});
		let insert = format!("insert into lepis_unconfirmed values ({n})");
		tokio::time::timeout(Duration::from_secs(10), c.batch_execute(&insert))
			.await
			.expect("the cancel did not end the wait")
	}
	let plain = cancelled(&direct().await, 1).await;
	let routed = cancelled(&lepis, 2).await;
	names(&admin, "").await;

	// Postgres itself says yes: what the router must not pass on.
	assert!(
		plain.is_ok(),
		"a cancelled wait is reported committed by Postgres: {plain:?}"
	);
	let e = routed.expect_err("Lepis reported a commit its standby never confirmed");
	if let Some(db) = e.as_db_error() {
		assert_eq!(db.code().code(), "08007", "{db:?}");
	}
	assert!(lepis.is_closed() || lepis.simple_query("select 1").await.is_err());
	// Both are committed on the primary all the same: the client is told it does not know.
	let rows: Vec<i32> = admin
		.query("select n from lepis_unconfirmed order by n", &[])
		.await
		.unwrap()
		.iter()
		.map(|r| r.get(0))
		.collect();
	assert_eq!(rows, [1, 2]);
}
