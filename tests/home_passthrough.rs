//! The home pass-through (`LEPIS_PASSTHROUGH_PORT`, passthrough.rs): a
//! session on it reaches the copy that takes writes as the client sent it, a cancel reaches that
//! session, and a session opened after a failover reaches the new primary while one opened before
//! is left where it is. Two Postgres from `scripts/it.sh` stand in for the copies (LEPIS_IT_HOME
//! and LEPIS_ORACLE_REFERENCE), fenced the way follow.rs's test fences them,
//! `default_transaction_read_only = on`. Its own test binary, because it changes both servers'
//! settings while it runs (and puts them back).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use lepis::config::Config;
use lepis::server::{self, App};
use tokio_postgres::{Client, NoTls};

fn env(key: &str) -> Option<String> {
	std::env::var(key).ok().filter(|v| !v.is_empty())
}

async fn connect(host: &str, port: u16) -> Result<Client, tokio_postgres::Error> {
	let (client, conn) = tokio_postgres::Config::new()
		.host(host)
		.port(port)
		.user("postgres")
		.password("x")
		.dbname("postgres")
		.connect_timeout(Duration::from_secs(3))
		.connect(NoTls)
		.await?;
	tokio::spawn(conn);
	Ok(client)
}

async fn direct(address: &str) -> Client {
	let (host, port) = address.rsplit_once(':').expect("host:port");
	connect(host, port.parse().expect("port"))
		.await
		.expect("direct connection")
}

async fn through(at: &SocketAddr) -> Result<Client, tokio_postgres::Error> {
	connect(&at.ip().to_string(), at.port()).await
}

async fn read_only(address: &str, on: bool) {
	let c = direct(address).await;
	let sql = if on {
		"alter system set default_transaction_read_only = on"
	} else {
		"alter system reset default_transaction_read_only"
	};
	c.batch_execute(sql).await.expect("alter system");
	c.batch_execute("select pg_reload_conf()")
		.await
		.expect("reload");
}

async fn server_addr(c: &Client) -> String {
	c.query_one("select inet_server_addr()::text", &[])
		.await
		.expect("query")
		.get(0)
}

/// Which server a fresh session through the pass-through lands on, once it answers.
async fn lands_on(at: &SocketAddr) -> Option<String> {
	match tokio::time::timeout(Duration::from_secs(3), through(at)).await {
		Ok(Ok(c)) => Some(server_addr(&c).await),
		_ => None,
	}
}

const PROBE: &str = "lepis_pt_probe";
/// A body with a comment and odd spacing, which `prosrc` keeps only if nothing rewrote it.
const BODY: &str = "\n  select 'as sent'::text  /* untouched by Lepis */\n";

#[tokio::test]
async fn the_pass_through_reaches_the_primary_as_sent_and_follows_it() {
	let (Some(a), Some(b)) = (env("LEPIS_IT_HOME"), env("LEPIS_ORACLE_REFERENCE")) else {
		eprintln!("LEPIS_IT_HOME and LEPIS_ORACLE_REFERENCE not set; skipped");
		return;
	};
	let a_addr = server_addr(&direct(&a).await).await;
	let b_addr = server_addr(&direct(&b).await).await;

	// b is fenced (read-only), a takes writes; the homes are listed b first.
	read_only(&b, true).await;
	let config: HashMap<String, String> = [
		("LEPIS_HOMES", format!("{b},{a}")),
		("LEPIS_HOME_SSLMODE", "disable".into()),
		("LEPIS_SERVICE_USER", "postgres".into()),
		("LEPIS_SERVICE_PASSWORD", "x".into()),
		("LEPIS_PASSTHROUGH_PORT", "7434".into()),
	]
	.into_iter()
	.map(|(k, v)| (k.to_string(), v))
	.collect();
	let config = Config::from_map(&config).expect("config");
	assert_eq!(config.passthrough_port, Some(7434));
	let app = App::new(config).expect("app");
	let routing = tokio::net::TcpListener::bind("127.0.0.1:0")
		.await
		.expect("bind");
	// Port 0 rather than 7434, so a run never collides with anything on the machine.
	let passthrough = tokio::net::TcpListener::bind("127.0.0.1:0")
		.await
		.expect("bind");
	let at = passthrough.local_addr().expect("address");
	tokio::spawn(server::serve_with(app, routing, Some(passthrough)));

	let mut first = None;
	let started = Instant::now();
	while first.is_none() && started.elapsed() < Duration::from_secs(15) {
		first = lands_on(&at).await;
		if first.is_none() {
			tokio::time::sleep(Duration::from_millis(200)).await;
		}
	}
	let first = first.expect("the pass-through never answered");

	// A function as auth's and storage's migrations write them, through the pass-through: it is on
	// the primary byte for byte, and nowhere else.
	let before = through(&at).await.expect("a session on the pass-through");
	let on_before = server_addr(&before).await;
	before
		.batch_execute(&format!(
			"create or replace function {PROBE}() returns text language sql as $lepis${BODY}$lepis$"
		))
		.await
		.expect("create function through the pass-through");
	let prosrc = |c: Client| async move {
		c.query_opt("select prosrc from pg_proc where proname = $1", &[&PROBE])
			.await
			.expect("pg_proc")
			.map(|r| r.get::<_, String>(0))
	};
	let on_a = prosrc(direct(&a).await).await;
	let on_b = prosrc(direct(&b).await).await;
	let answered: String = before
		.query_one(&format!("select {PROBE}()"), &[])
		.await
		.expect("the function runs")
		.get(0);

	// A cancel on the pass-through reaches the session it was given for.
	let sleeping = {
		let (host, port) = (at.ip().to_string(), at.port());
		tokio::spawn(async move {
			let (c, conn) = tokio_postgres::Config::new()
				.host(&host)
				.port(port)
				.user("postgres")
				.password("x")
				.dbname("postgres")
				.connect(NoTls)
				.await
				.expect("a second session");
			tokio::spawn(conn);
			let token = c.cancel_token();
			let query = tokio::spawn(async move { c.batch_execute("select pg_sleep(30)").await });
			tokio::time::sleep(Duration::from_millis(500)).await;
			token.cancel_query(NoTls).await.expect("cancel sent");
			tokio::time::timeout(Duration::from_secs(10), query)
				.await
				.expect("the query was cancelled, not run out")
				.expect("task")
		})
	};
	let cancelled = sleeping.await.expect("task");
	before
		.batch_execute(&format!("drop function if exists {PROBE}()"))
		.await
		.expect("drop function");

	// The failover: a fenced, b the primary. A NEW session lands on b within a few seconds; the
	// one opened before stays on a (in production a's guard fence ends it).
	read_only(&a, true).await;
	read_only(&b, false).await;
	let mut moved = None;
	for _ in 0..30 {
		tokio::time::sleep(Duration::from_millis(250)).await;
		moved = lands_on(&at).await;
		if moved.as_deref() == Some(b_addr.as_str()) {
			break;
		}
	}
	let still = server_addr(&before).await;
	let read_only_after = match through(&at).await {
		Ok(c) => c
			.query_one("show transaction_read_only", &[])
			.await
			.map(|r| r.get::<_, String>(0))
			.map_err(|e| e.to_string()),
		Err(e) => Err(e.to_string()),
	};
	read_only(&a, false).await;

	assert_eq!(
		first, a_addr,
		"the pass-through started on a copy that does not take writes"
	);
	assert_eq!(on_before, a_addr);
	assert_eq!(
		on_a.as_deref(),
		Some(BODY),
		"the function did not arrive as sent"
	);
	assert_eq!(on_b, None, "the function reached a copy it was not sent to");
	assert_eq!(answered, "as sent");
	let code = cancelled
		.expect_err("pg_sleep(30) finished")
		.code()
		.cloned();
	assert_eq!(code, Some(tokio_postgres::error::SqlState::QUERY_CANCELED));
	assert_eq!(
		moved.as_deref(),
		Some(b_addr.as_str()),
		"the pass-through did not follow the primary"
	);
	assert_eq!(still, a_addr, "an open session was moved");
	assert_eq!(
		read_only_after,
		Ok("off".to_string()),
		"a new session is on a copy that does not take writes"
	);
}
