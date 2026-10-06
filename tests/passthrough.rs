//! Phase 0's pass-through, against a real Postgres: a client logs into Lepis with its own role
//! and password, Lepis logs into the node as that role with the key the client's proof
//! revealed, and everything after that behaves as if the client had connected directly.
//!
//! Needs `LEPIS_IT_HOME` (host:port of a Postgres whose superuser is postgres/x);
//! `scripts/it.sh` provides one. Skips without it.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use lepis::config::Config;
use lepis::server::{self, App};
use tokio_postgres::{Client, NoTls};

fn home() -> Option<String> {
	std::env::var("LEPIS_IT_HOME")
		.ok()
		.filter(|v| !v.is_empty())
}

async fn direct(user: &str, password: &str) -> Client {
	let home = home().expect("checked");
	let (host, port) = home.rsplit_once(':').expect("host:port");
	let (client, conn) = tokio_postgres::Config::new()
		.host(host)
		.port(port.parse().expect("port"))
		.user(user)
		.password(password)
		.dbname("postgres")
		.connect(NoTls)
		.await
		.expect("direct connection");
	tokio::spawn(conn);
	client
}

/// Roles and a table, idempotent so tests may run in any order and in parallel.
async fn setup() {
	let admin = direct("postgres", "x").await;
	admin
		.batch_execute(
			"begin;
			select pg_advisory_xact_lock(4242);
			do $$ begin
				if not exists (select from pg_roles where rolname = 'lepis_app') then
					create role lepis_app login password 'app-pw';
				end if;
				if not exists (select from pg_roles where rolname = 'lepis_nologin') then
					create role lepis_nologin nologin password 'x';
				end if;
			end $$;
			create table if not exists lepis_it (id bigint primary key, note text);
			grant all on lepis_it to lepis_app;
			commit;",
		)
		.await
		.expect("setup");
}

async fn start_lepis() -> SocketAddr {
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
	addr
}

async fn through(
	addr: SocketAddr,
	user: &str,
	password: &str,
) -> Result<Client, tokio_postgres::Error> {
	let (client, conn) = tokio_postgres::Config::new()
		.host(addr.ip().to_string())
		.port(addr.port())
		.user(user)
		.password(password)
		.dbname("postgres")
		.application_name("lepis-it")
		.connect(NoTls)
		.await?;
	tokio::spawn(conn);
	Ok(client)
}

macro_rules! need_home {
	() => {
		if home().is_none() {
			eprintln!("LEPIS_IT_HOME not set; skipped");
			return;
		}
		setup().await;
	};
}

#[tokio::test]
async fn logs_in_as_the_clients_own_role() {
	need_home!();
	let addr = start_lepis().await;
	let c = through(addr, "lepis_app", "app-pw").await.expect("login");
	let row = c
		.query_one(
			"select current_user::text, session_user::text, current_setting('application_name')",
			&[],
		)
		.await
		.unwrap();
	assert_eq!(row.get::<_, String>(0), "lepis_app");
	assert_eq!(row.get::<_, String>(1), "lepis_app");
	// Startup parameters reach the node.
	assert_eq!(row.get::<_, String>(2), "lepis-it");
}

#[tokio::test]
async fn wrong_password_and_unknown_role_look_the_same() {
	need_home!();
	let addr = start_lepis().await;
	for (user, pw) in [
		("lepis_app", "wrong"),
		("no_such_role", "x"),
		("lepis_nologin", "x"),
	] {
		let Err(e) = through(addr, user, pw).await else {
			panic!("{user} logged in");
		};
		let db = e.as_db_error().expect("a server error");
		assert_eq!(db.code().code(), "28P01", "{user}");
		assert_eq!(
			db.message(),
			format!("password authentication failed for user \"{user}\"")
		);
	}
}

#[tokio::test]
async fn queries_prepared_statements_and_transactions() {
	need_home!();
	let addr = start_lepis().await;
	let c = through(addr, "lepis_app", "app-pw").await.unwrap();
	let base: i64 = 1_000_000 + i64::from(std::process::id() % 1000) * 1000;
	c.execute(
		"delete from lepis_it where id between $1 and $1 + 999",
		&[&base],
	)
	.await
	.unwrap();
	let insert = c
		.prepare("insert into lepis_it (id, note) values ($1, $2)")
		.await
		.unwrap();
	for i in 0..100i64 {
		c.execute(&insert, &[&(base + i), &format!("n{i}")])
			.await
			.unwrap();
	}
	let n: i64 = c
		.query_one(
			"select count(*) from lepis_it where id between $1 and $1 + 999",
			&[&base],
		)
		.await
		.unwrap()
		.get(0);
	assert_eq!(n, 100);

	// A rolled-back transaction leaves nothing behind.
	c.batch_execute(&format!(
		"begin; insert into lepis_it values ({}, 'gone'); rollback;",
		base + 500
	))
	.await
	.unwrap();
	let gone = c
		.query_opt("select 1 from lepis_it where id = $1", &[&(base + 500)])
		.await
		.unwrap();
	assert!(gone.is_none());

	// An error does not end the session.
	let e = c.query_one("select 1 / 0", &[]).await.unwrap_err();
	assert_eq!(e.as_db_error().unwrap().code().code(), "22012");
	let one: i32 = c.query_one("select 1", &[]).await.unwrap().get(0);
	assert_eq!(one, 1);
}

#[tokio::test]
async fn copy_both_ways() {
	need_home!();
	use futures_util::{SinkExt, TryStreamExt};
	let addr = start_lepis().await;
	let c = through(addr, "lepis_app", "app-pw").await.unwrap();
	c.batch_execute("create temp table t (a int, b text)")
		.await
		.unwrap();
	let sink = c.copy_in("copy t from stdin").await.unwrap();
	let mut sink = std::pin::pin!(sink);
	let mut data = String::new();
	for i in 0..10_000 {
		data.push_str(&format!("{i}\trow {i}\n"));
	}
	sink.send(bytes::Bytes::from(data)).await.unwrap();
	let n = sink.finish().await.unwrap();
	assert_eq!(n, 10_000);
	let out: Vec<bytes::Bytes> = c
		.copy_out("copy (select * from t order by a) to stdout")
		.await
		.unwrap()
		.try_collect()
		.await
		.unwrap();
	let text: Vec<u8> = out.concat();
	assert_eq!(text.iter().filter(|&&b| b == b'\n').count(), 10_000);
}

#[tokio::test]
async fn cancel_reaches_the_node() {
	need_home!();
	let addr = start_lepis().await;
	let c = through(addr, "lepis_app", "app-pw").await.unwrap();
	let token = c.cancel_token();
	let started = Instant::now();
	let sleeper = tokio::spawn(async move { c.query_one("select pg_sleep(30)", &[]).await });
	tokio::time::sleep(Duration::from_millis(300)).await;
	token.cancel_query(NoTls).await.unwrap();
	let e = sleeper.await.unwrap().unwrap_err();
	assert_eq!(e.as_db_error().unwrap().code().code(), "57014");
	assert!(started.elapsed() < Duration::from_secs(10));
}

#[tokio::test]
async fn many_sessions_at_once() {
	need_home!();
	let addr = start_lepis().await;
	let mut tasks = Vec::new();
	for i in 0..50i32 {
		tasks.push(tokio::spawn(async move {
			let c = through(addr, "lepis_app", "app-pw").await.unwrap();
			for _ in 0..20 {
				let v: i32 = c
					.query_one("select $1::int + 1", &[&i])
					.await
					.unwrap()
					.get(0);
				assert_eq!(v, i + 1);
			}
		}));
	}
	for t in tasks {
		t.await.unwrap();
	}
}

#[tokio::test]
async fn a_missing_database_is_the_nodes_own_error() {
	need_home!();
	let addr = start_lepis().await;
	let (client, conn) = tokio_postgres::Config::new()
		.host(addr.ip().to_string())
		.port(addr.port())
		.user("lepis_app")
		.password("app-pw")
		.dbname("no_such_db")
		.connect(NoTls)
		.await
		.map(|(c, conn)| (Some(c), Some(conn)))
		.unwrap_or_else(|e| {
			assert_eq!(e.as_db_error().unwrap().code().code(), "3D000");
			(None, None)
		});
	assert!(client.is_none() && conn.is_none());
}
