//! A home that is the copies of one database (`LEPIS_HOMES`, follow.rs): the router uses the copy
//! that takes writes, and follows when another one does. Two Postgres from `scripts/it.sh` stand in
//! for the copies (LEPIS_IT_HOME and LEPIS_ORACLE_REFERENCE); a copy is made "not the primary" the
//! way a guard's fence makes it, `default_transaction_read_only = on`. Its own test binary, because
//! it changes both servers' settings while it runs (and puts them back).

use std::collections::HashMap;
use std::time::Duration;

use lepis::config::Config;
use lepis::server::{self, App};
use tokio_postgres::{Client, NoTls};

fn env(key: &str) -> Option<String> {
	std::env::var(key).ok().filter(|v| !v.is_empty())
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

async fn direct(address: &str) -> Client {
	let (host, port) = address.rsplit_once(':').expect("host:port");
	connect(host, port.parse().expect("port")).await
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

/// Which server a fresh session through the router lands on, by its own address.
async fn lands_on(router: &std::net::SocketAddr) -> String {
	let c = connect(&router.ip().to_string(), router.port()).await;
	c.query_one("select inet_server_addr()::text", &[])
		.await
		.expect("query")
		.get(0)
}

#[tokio::test]
async fn the_router_uses_the_copy_that_takes_writes_and_follows_when_it_moves() {
	let (Some(a), Some(b)) = (env("LEPIS_IT_HOME"), env("LEPIS_ORACLE_REFERENCE")) else {
		eprintln!("LEPIS_IT_HOME and LEPIS_ORACLE_REFERENCE not set; skipped");
		return;
	};
	let a_addr: String = direct(&a)
		.await
		.query_one("select inet_server_addr()::text", &[])
		.await
		.unwrap()
		.get(0);
	let b_addr: String = direct(&b)
		.await
		.query_one("select inet_server_addr()::text", &[])
		.await
		.unwrap()
		.get(0);

	// Physical copies share every role's verifier byte for byte; two separate servers do not, so
	// a's is given to b, as a copy would have it.
	let verifier: String = direct(&a)
		.await
		.query_one(
			"select rolpassword from pg_authid where rolname = 'postgres'",
			&[],
		)
		.await
		.unwrap()
		.get(0);
	direct(&b)
		.await
		.batch_execute(&format!("alter role postgres password '{verifier}'"))
		.await
		.expect("the same verifier on b");

	// b is fenced (read-only), a takes writes; the homes are listed b first.
	read_only(&b, true).await;
	let env: HashMap<String, String> = [
		("LEPIS_HOMES", format!("{b},{a}")),
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
	let router = listener.local_addr().expect("address");
	tokio::spawn(server::serve(app, listener));

	let mut first = String::new();
	for _ in 0..50 {
		tokio::time::sleep(Duration::from_millis(200)).await;
		if let Ok(Ok(on)) = tokio::time::timeout(
			Duration::from_secs(3),
			tokio::spawn(async move { lands_on(&router).await }),
		)
		.await
		{
			first = on;
			break;
		}
	}
	assert_eq!(
		first, a_addr,
		"the router started on a copy that does not take writes"
	);

	// The failover: a fenced, b the primary. New sessions land on b within a few seconds.
	read_only(&a, true).await;
	read_only(&b, false).await;
	let mut moved = String::new();
	for _ in 0..30 {
		tokio::time::sleep(Duration::from_millis(250)).await;
		moved = lands_on(&router).await;
		if moved == b_addr {
			break;
		}
	}
	read_only(&a, false).await;
	assert_eq!(moved, b_addr, "the router did not follow the primary");
}
