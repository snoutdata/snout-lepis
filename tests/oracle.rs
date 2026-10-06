//! The oracle (Phase 0): the same schema and data in a plain reference
//! Postgres and in the cluster behind Lepis; every query in `oracle/queries.sql` runs on both,
//! and the answers must be equal (or, from Phase 1, Lepis must refuse with an L9 error where
//! the corpus says `route=refuse`). This is the correctness gate every phase passes.
//!
//! Needs `LEPIS_IT_HOME` (the cluster's home node) and `LEPIS_ORACLE_REFERENCE` (a separate
//! Postgres), both with superuser postgres/x; `scripts/it.sh` provides them. Skips without.

use std::collections::HashMap;

use lepis::config::Config;
use lepis::server::{self, App};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

const SCHEMA: &str = include_str!("../oracle/schema.sql");
const DATA: &str = include_str!("../oracle/data.sql");
const QUERIES: &str = include_str!("../oracle/queries.sql");

fn env(k: &str) -> Option<String> {
	std::env::var(k).ok().filter(|v| !v.is_empty())
}

async fn connect(host_port: &str, user: &str, password: &str) -> Client {
	let (host, port) = host_port.rsplit_once(':').expect("host:port");
	let (client, conn) = tokio_postgres::Config::new()
		.host(host)
		.port(port.parse().expect("port"))
		.user(user)
		.password(password)
		.dbname("postgres")
		.application_name("lepis-oracle")
		.connect(NoTls)
		.await
		.unwrap_or_else(|e| panic!("{user}@{host_port}: {e}"));
	tokio::spawn(conn);
	client
}

/// Loads the schema and data and makes the app role, the same way on either target.
async fn load(host_port: &str) {
	let admin = connect(host_port, "postgres", "x").await;
	admin
		.batch_execute(
			"begin;
			select pg_advisory_xact_lock(4242);
			do $$ begin
				if not exists (select from pg_roles where rolname = 'lepis_app') then
					create role lepis_app login password 'app-pw';
				end if;
			end $$;
			commit;",
		)
		.await
		.expect("role");
	admin.batch_execute(SCHEMA).await.expect("schema");
	admin.batch_execute(DATA).await.expect("data");
	admin
		.batch_execute(
			"grant usage on schema oracle to lepis_app;
			grant select, insert, update, delete on all tables in schema oracle to lepis_app;",
		)
		.await
		.expect("grants");
}

#[derive(Debug)]
struct Query {
	name: String,
	ordered: bool,
	sql: String,
}

fn corpus() -> Vec<Query> {
	let mut out: Vec<Query> = Vec::new();
	for line in QUERIES.lines() {
		if let Some(header) = line.strip_prefix("-- name: ") {
			let mut words = header.split_whitespace();
			let name = words.next().expect("a name").to_string();
			let ordered = words.any(|w| w == "ordered");
			out.push(Query {
				name,
				ordered,
				sql: String::new(),
			});
		} else if let Some(q) = out.last_mut()
			&& !line.starts_with("--")
		{
			q.sql.push_str(line);
			q.sql.push('\n');
		}
	}
	out
}

type Answer = Result<Vec<Vec<Option<String>>>, String>;

async fn answer(c: &Client, sql: &str, ordered: bool) -> Answer {
	match c.simple_query(sql).await {
		Ok(messages) => {
			let mut rows: Vec<Vec<Option<String>>> = messages
				.into_iter()
				.filter_map(|m| match m {
					SimpleQueryMessage::Row(r) => {
						Some((0..r.len()).map(|i| r.get(i).map(str::to_string)).collect())
					}
					_ => None,
				})
				.collect();
			if !ordered {
				rows.sort();
			}
			Ok(rows)
		}
		Err(e) => Err(e
			.as_db_error()
			.map(|d| format!("{}: {}", d.code().code(), d.message()))
			.unwrap_or_else(|| e.to_string())),
	}
}

#[tokio::test]
async fn cluster_answers_like_one_postgres() {
	let (Some(home), Some(reference)) = (env("LEPIS_IT_HOME"), env("LEPIS_ORACLE_REFERENCE"))
	else {
		eprintln!("LEPIS_IT_HOME / LEPIS_ORACLE_REFERENCE not set; skipped");
		return;
	};
	load(&reference).await;
	load(&home).await;

	let config: HashMap<String, String> = [
		("LEPIS_HOME", home.clone()),
		("LEPIS_HOME_SSLMODE", "disable".into()),
		("LEPIS_SERVICE_USER", "postgres".into()),
		("LEPIS_SERVICE_PASSWORD", "x".into()),
	]
	.into_iter()
	.map(|(k, v)| (k.to_string(), v))
	.collect();
	let app = App::new(Config::from_map(&config).expect("config")).expect("app");
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
	let lepis = listener.local_addr().unwrap().to_string();
	tokio::spawn(server::serve(app, listener));

	let reference = connect(&reference, "lepis_app", "app-pw").await;
	let cluster = connect(&lepis, "lepis_app", "app-pw").await;

	let queries = corpus();
	assert!(queries.len() >= 30, "corpus has {} queries", queries.len());
	let mut differences = Vec::new();
	for q in &queries {
		let want = answer(&reference, &q.sql, q.ordered).await;
		let got = answer(&cluster, &q.sql, q.ordered).await;
		if want.is_err() {
			differences.push(format!("{}: the reference itself failed: {want:?}", q.name));
		} else if got != want {
			differences.push(format!(
				"{}:\n  reference {want:?}\n  cluster   {got:?}",
				q.name
			));
		}
	}
	assert!(
		differences.is_empty(),
		"{} of {} queries differ:\n{}",
		differences.len(),
		queries.len(),
		differences.join("\n")
	);
}
