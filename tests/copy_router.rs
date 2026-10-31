//! A copy's router (`LEPIS_CATALOG=off`, Metis Phase 10 step 5) against a home that HAS the Lepis
//! catalog, signed in as a login with no rights on schema `lepis`, the way the agent's
//! `routerLoginSql` makes `snoutpod_router`. With the catalog on, that router stops at its first
//! read ("permission denied for schema lepis", fleet drill 6). With it off it relays a client's
//! session to the home, writes no `lepis.router` heartbeat, and holds no advisory lock: in-doubt
//! recovery and the job runner, whose locks belong to the home's own router, never start.
//! Runs against LEPIS_IT_HOME from `scripts/it.sh`, in a database of its own.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use lepis::config::Config;
use lepis::server::{self, App};
use tokio_postgres::{Client, NoTls};

const DB: &str = "lepis_copy_it";
const ROUTER: &str = "lepis_copy_it_router";
const USER: &str = "lepis_copy_it_user";

fn env(key: &str) -> Option<String> {
	std::env::var(key).ok().filter(|v| !v.is_empty())
}

async fn connect(
	host: &str,
	port: u16,
	user: &str,
	password: &str,
	db: &str,
) -> Result<Client, tokio_postgres::Error> {
	let (client, conn) = tokio_postgres::Config::new()
		.host(host)
		.port(port)
		.user(user)
		.password(password)
		.dbname(db)
		.connect_timeout(Duration::from_secs(3))
		.connect(NoTls)
		.await?;
	tokio::spawn(conn);
	Ok(client)
}

async fn admin(home: &str, db: &str) -> Client {
	let (host, port) = home.rsplit_once(':').expect("host:port");
	connect(host, port.parse().expect("port"), "postgres", "x", db)
		.await
		.expect("direct connection")
}

async fn router(
	home: &str,
	catalog: &str,
) -> (tokio::task::JoinHandle<std::io::Result<()>>, SocketAddr) {
	let config: HashMap<String, String> = [
		("LEPIS_HOME", home.to_string()),
		("LEPIS_HOME_SSLMODE", "disable".into()),
		("LEPIS_SERVICE_USER", ROUTER.into()),
		("LEPIS_SERVICE_PASSWORD", "r".into()),
		("LEPIS_SERVICE_DATABASE", DB.into()),
		(
			"LEPIS_VERIFIER_QUERY",
			format!("select {ROUTER}.verifier($1)"),
		),
		("LEPIS_CATALOG", catalog.into()),
	]
	.into_iter()
	.map(|(k, v)| (k.to_string(), v))
	.collect();
	let app = App::new(Config::from_map(&config).expect("config")).expect("app");
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
		.await
		.expect("bind");
	let at = listener.local_addr().expect("address");
	(tokio::spawn(server::serve_with(app, listener, None)), at)
}

#[tokio::test]
async fn a_copys_router_relays_without_reading_the_catalog() {
	let Some(home) = env("LEPIS_IT_HOME") else {
		eprintln!("LEPIS_IT_HOME not set; skipped");
		return;
	};
	let root = admin(&home, "postgres").await;
	// One statement each: DROP and CREATE DATABASE refuse a transaction block.
	for sql in [
		format!("drop database if exists {DB} with (force)"),
		format!("drop role if exists {ROUTER}"),
		format!("drop role if exists {USER}"),
		format!("create database {DB}"),
		format!("create role {ROUTER} login noinherit password 'r'"),
		format!("create role {USER} login password 'u'"),
		format!("grant connect on database {DB} to {ROUTER}, {USER}"),
	] {
		root.batch_execute(&sql).await.expect(&sql);
	}

	// The home's catalog, as lepisBootstrapSql leaves it for a separated layout: its home row
	// names a host only the home's own pod reaches.
	let db = admin(&home, DB).await;
	db.batch_execute(lepis::catalog::CATALOG_SQL)
		.await
		.expect("catalog");
	db.batch_execute(&format!(
		"insert into lepis.node (id, name, host, port, dbname, sslmode, kind, state)
			values (1, 'home', 'home-pod.invalid', 5432, '{DB}', 'disable', 'home', 'active');
		create schema {ROUTER} authorization postgres;
		create function {ROUTER}.verifier(role_name text) returns text
			language sql stable security definer set search_path = pg_catalog as $f$
			select a.rolpassword from pg_catalog.pg_authid a
			where a.rolname = role_name and a.rolcanlogin and not a.rolsuper
		$f$;
		revoke all on schema {ROUTER} from public;
		revoke all on function {ROUTER}.verifier(text) from public;
		grant usage on schema {ROUTER} to {ROUTER};
		grant execute on function {ROUTER}.verifier(text) to {ROUTER};"
	))
	.await
	.expect("router login");
	let usage: bool = db
		.query_one(
			&format!("select has_schema_privilege('{ROUTER}', 'lepis', 'usage')"),
			&[],
		)
		.await
		.expect("privilege")
		.get(0);
	assert!(!usage, "the router's login was given schema lepis");

	// The catalog on: the router stops at its first read, as drill 6's did.
	let (on, _) = router(&home, "on").await;
	let stopped = tokio::time::timeout(Duration::from_secs(10), on)
		.await
		.expect("a router with the catalog on kept running")
		.expect("join");
	let e = stopped
		.expect_err("a router with the catalog on started")
		.to_string();
	assert!(e.contains("permission denied for schema lepis"), "{e}");

	// Off: a client's session reaches the home as that client.
	let (off, at) = router(&home, "off").await;
	let mut client = None;
	for _ in 0..50 {
		if let Ok(c) = connect(&at.ip().to_string(), at.port(), USER, "u", DB).await {
			client = Some(c);
			break;
		}
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
	let client = client.expect("the copy's router never answered");
	let row = client
		.query_one("select current_user::text, current_database()::text", &[])
		.await
		.expect("query through the router");
	assert_eq!(
		(row.get::<_, String>(0), row.get::<_, String>(1)),
		(USER.to_string(), DB.to_string())
	);

	// Past recovery's and the runner's first pass, had either started.
	tokio::time::sleep(Duration::from_secs(3)).await;
	let locks: i64 = db
		.query_one(
			&format!(
				"select count(*) from pg_locks l join pg_stat_activity a on a.pid = l.pid
				where l.locktype = 'advisory' and a.usename = '{ROUTER}'"
			),
			&[],
		)
		.await
		.expect("locks")
		.get(0);
	assert_eq!(locks, 0, "the copy's router took an advisory lock");
	let heartbeats: i64 = db
		.query_one("select count(*) from lepis.router", &[])
		.await
		.expect("router rows")
		.get(0);
	assert_eq!(heartbeats, 0, "the copy's router wrote a heartbeat");
	assert!(!off.is_finished(), "the copy's router stopped");

	off.abort();
	drop(client);
	drop(db);
	for sql in [
		format!("drop database if exists {DB} with (force)"),
		format!("drop role if exists {ROUTER}"),
		format!("drop role if exists {USER}"),
	] {
		root.batch_execute(&sql).await.expect(&sql);
	}
}
