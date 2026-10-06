//! pg_cron on a sharded cluster (Phase 7), against three real Postgres 18
//! servers with pg_cron: `lepis::cron::sync` copies the jobs home marks `every_node` to the other
//! nodes, and nothing else. The pure planning is unit-tested in `src/cron.rs`.
//!
//! Skips without LEPIS_CRON_NODES (`home:port,node2:port,…`); `bash tests/cron_sync.sh` starts
//! three Postgres 18 servers with pg_cron and runs it.

use lepis::backend::{self, Backend};
use lepis::catalog::CATALOG_SQL;
use lepis::config::{NodeAddress, SslMode};
use lepis::cron;
use lepis::scram::ClientCredential;
use tokio_postgres::{Client, NoTls};

fn nodes() -> Option<Vec<(String, u16)>> {
	let list = std::env::var("LEPIS_CRON_NODES").ok()?;
	Some(
		list.split(',')
			.map(|n| {
				let (h, p) = n.rsplit_once(':').expect("host:port");
				(h.to_string(), p.parse().expect("port"))
			})
			.collect(),
	)
}

async fn connect(host: &str, port: u16) -> Client {
	let (client, conn) = tokio_postgres::Config::new()
		.host(host)
		.port(port)
		.user("postgres")
		.password("test")
		.dbname("app")
		.connect(NoTls)
		.await
		.unwrap_or_else(|e| panic!("{host}:{port}: {e}"));
	tokio::spawn(conn);
	client
}

/// Lepis's own login, as the recovery leader holds it.
async fn service(host: &str, port: u16) -> Backend {
	let a = NodeAddress {
		host: host.into(),
		port,
		sslmode: SslMode::Disable,
		ca_file: None,
	};
	let params = vec![
		("user".to_string(), "postgres".to_string()),
		("database".to_string(), "app".to_string()),
	];
	backend::connect(&a, None, &params, ClientCredential::Password("test".into()))
		.await
		.unwrap_or_else(|e| panic!("{host}:{port}: {e}"))
}

async fn count(c: &Client, sql: &str) -> i64 {
	c.query_one(sql, &[]).await.unwrap().get(0)
}

/// One pass, as the leader runs it: fresh service logins to home and the other nodes.
async fn sync(list: &[(String, u16)]) -> Vec<String> {
	let mut home = service(&list[0].0, list[0].1).await;
	let mut others = Vec::new();
	for (i, (h, p)) in list[1..].iter().enumerate() {
		others.push((["node2", "node3"][i].to_string(), service(h, *p).await));
	}
	cron::sync(&mut home, &mut others).await.unwrap()
}

#[tokio::test]
async fn every_node_jobs_run_on_every_node_and_the_rest_on_home() {
	let Some(list) = nodes() else {
		eprintln!("skipped: LEPIS_CRON_NODES is not set (tests/cron_sync.sh sets it)");
		return;
	};
	let mut clients = Vec::new();
	for (h, p) in &list {
		let c = connect(h, *p).await;
		c.batch_execute(
			"drop extension if exists pg_cron cascade; create extension pg_cron;
			 drop table if exists ticks, home_ticks;
			 create table ticks (at timestamptz default now());
			 create table home_ticks (at timestamptz default now());",
		)
		.await
		.unwrap();
		clients.push(c);
	}
	let home = &clients[0];
	let others: Vec<(&str, &Client)> = clients[1..]
		.iter()
		.enumerate()
		.map(|(i, c)| (["node2", "node3"][i], c))
		.collect();

	// No marking table yet: there is nothing to read, and nothing is done.
	home.batch_execute("drop schema if exists lepis cascade; create schema lepis")
		.await
		.unwrap();
	assert!(sync(&list).await.is_empty());

	home.batch_execute(CATALOG_SQL).await.unwrap();
	home.batch_execute(
		"select cron.schedule('tick', '1 seconds', 'insert into ticks default values');
		 select cron.schedule('home-only', '1 seconds', 'insert into home_ticks default values');
		 insert into lepis.cron_job (jobname) values ('tick');",
	)
	.await
	.unwrap();
	// A job a node has of its own is never the runner's.
	others[0]
		.1
		.batch_execute("select cron.schedule('local', '0 0 1 1 *', 'select 1')")
		.await
		.unwrap();

	let done = sync(&list).await;
	assert_eq!(
		done,
		vec!["node2: schedule lepis:tick", "node3: schedule lepis:tick"]
	);
	assert!(
		sync(&list).await.is_empty(),
		"a second pass changes nothing"
	);

	// The copies run on their nodes; the unmarked job runs on home alone.
	tokio::time::sleep(std::time::Duration::from_secs(4)).await;
	for c in &clients {
		assert!(count(c, "select count(*) from ticks").await > 0);
	}
	assert!(count(home, "select count(*) from home_ticks").await > 0);
	for (_, c) in &others {
		assert_eq!(count(c, "select count(*) from home_ticks").await, 0);
	}

	// Changed on home: each copy follows.
	home.batch_execute(
		"select cron.schedule('tick', '2 seconds', 'insert into ticks default values')",
	)
	.await
	.unwrap();
	assert_eq!(sync(&list).await.len(), 2);
	for (_, c) in &others {
		assert_eq!(
			count(
				c,
				"select count(*) from cron.job where jobname = 'lepis:tick' and schedule = '2 seconds'"
			)
			.await,
			1
		);
	}

	// Unmarked: the copies go, and the node's own job stays.
	home.batch_execute("update lepis.cron_job set scope = 'home' where jobname = 'tick'")
		.await
		.unwrap();
	assert_eq!(sync(&list).await.len(), 2);
	for (_, c) in &others {
		assert_eq!(
			count(
				c,
				"select count(*) from cron.job where jobname like 'lepis:%'"
			)
			.await,
			0
		);
	}
	assert_eq!(
		count(
			others[0].1,
			"select count(*) from cron.job where jobname = 'local'"
		)
		.await,
		1
	);
	assert_eq!(
		count(home, "select count(*) from cron.job where jobname = 'tick'").await,
		1
	);
}
