//! Read replicas against a real cluster: the home node's
//! streaming standby, named in the settings, serves the reads of a session that asks for it, and
//! nothing else. A read that can write goes to the primary (nextval, FOR UPDATE, a writable CTE,
//! a user function); one that writes anyway (a view calling nextval) is refused by the replica
//! and runs again on the primary, the client never seeing it; a session that wrote reads from the
//! primary; a replica that falls behind or does not answer is passed over, and one that dies
//! mid-read has the read run on the primary with the session kept.
//!
//! Needs `LEPIS_IT_HOME`, `LEPIS_IT_DATA_NODES`, `LEPIS_ORACLE_REFERENCE` and
//! `LEPIS_IT_HOME_REPLICA` (the home's standby; `scripts/it.sh` makes one,
//! `LEPIS_IT_ONLY=cluster_replica` runs this file alone). Skips without.

#[macro_use]
mod common;

use std::time::{Duration, Instant};

use common::*;

/// Long enough for the router's settings and a replica's reading to be taken again.
const SETTLE: Duration = Duration::from_millis(3_500);

/// A session through the router that asked for replicas.
async fn opted(lepis: &str) -> Client {
	let c = connect(lepis, "lepis_app", "app-pw").await;
	c.batch_execute("set lepis.read_from = 'replica'")
		.await
		.unwrap();
	c
}

/// A session that asked in its connection string (`options=-c lepis.read_from=replica`), no SET.
async fn opted_by_options(lepis: &str) -> Client {
	let (host, port) = lepis.rsplit_once(':').unwrap();
	let (client, conn) = tokio_postgres::Config::new()
		.host(host)
		.port(port.parse().unwrap())
		.user("lepis_app")
		.password("app-pw")
		.dbname("postgres")
		.options("-c lepis.read_from=replica")
		.connect(NoTls)
		.await
		.unwrap();
	tokio::spawn(conn);
	client
}

/// Whether `sql`'s first column, `pg_is_in_recovery()`, says the replica answered.
async fn on_replica(c: &Client, sql: &str) -> bool {
	c.query_one(sql, &[])
		.await
		.unwrap_or_else(|e| panic!("{sql}: {e}"))
		.get(0)
}

/// The same through the simple protocol.
async fn on_replica_simple(c: &Client, sql: &str) -> bool {
	let rows = c
		.simple_query(sql)
		.await
		.unwrap_or_else(|e| panic!("{sql}: {e}"));
	let row = rows
		.iter()
		.find_map(|m| match m {
			SimpleQueryMessage::Row(r) => Some(r),
			_ => None,
		})
		.expect("a row");
	row.get(0) == Some("t")
}

/// Waits until the replica has replayed everything the home has written so far.
async fn caught_up(home: &Client, replica: &Client) {
	let lsn: String = home
		.query_one("select pg_current_wal_lsn()::text", &[])
		.await
		.unwrap()
		.get(0);
	let deadline = Instant::now() + Duration::from_secs(30);
	loop {
		let done: bool = replica
			.query_one(
				"select pg_last_wal_replay_lsn() >= $1::text::pg_lsn",
				&[&lsn],
			)
			.await
			.unwrap()
			.get(0);
		if done {
			return;
		}
		assert!(Instant::now() < deadline, "the replica never reached {lsn}");
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
}

async fn set_replica(home: &Client, host: &str, port: &str) {
	home.execute(
		"update lepis.cluster set settings = settings || jsonb_build_object('read_replicas', \
		jsonb_build_object('n1', jsonb_build_object('host', $1::text, 'port', $2::text::int, 'sslmode', 'disable'))) \
		where id = 1",
		&[&host, &port],
	)
	.await
	.unwrap();
}

#[tokio::test]
async fn a_session_that_asks_reads_from_the_home_nodes_replica() {
	let b = need_bed!();
	let Some(replica_at) = env("LEPIS_IT_HOME_REPLICA") else {
		eprintln!("LEPIS_IT_HOME_REPLICA not set; skipped");
		return;
	};
	let (host, port) = replica_at.rsplit_once(':').unwrap();
	let home = connect(&b.home, "postgres", "x").await;
	let replica = connect(&replica_at, "postgres", "x").await;
	home.batch_execute(
		"drop schema if exists rr cascade;
		create schema rr;
		create sequence rr.s;
		create table rr.g (id int primary key, v text);
		create table rr.big (id int, v text);
		insert into rr.g values (1, 'a'), (2, 'b');
		create view rr.w as select nextval('rr.s') as n;
		create function rr.f() returns int language sql volatile as 'select 1';
		grant usage on schema rr to lepis_app;
		grant all on all tables in schema rr to lepis_app;
		grant all on sequence rr.s to lepis_app;
		grant execute on function rr.f() to lepis_app;",
	)
	.await
	.unwrap();
	set_replica(&home, host, port).await;
	caught_up(&home, &replica).await;
	let lepis = start_lepis(&b).await;

	// Asked for, a read the home answers comes from its replica; not asked for, from the home.
	let plain = connect(&lepis, "lepis_app", "app-pw").await;
	assert!(!on_replica(&plain, "select pg_is_in_recovery()").await);
	let a = opted(&lepis).await;
	assert!(on_replica(&a, "select pg_is_in_recovery()").await);
	assert!(on_replica_simple(&a, "select pg_is_in_recovery()").await);
	assert!(on_replica(&a, "select pg_is_in_recovery(), count(*) from rr.g").await);
	// Asked for in the connection string, the same; a SET to the primary takes it back, and RESET
	// returns to what the connection asked for, as in Postgres.
	let o = opted_by_options(&lepis).await;
	assert!(on_replica(&o, "select pg_is_in_recovery()").await);
	o.batch_execute("set lepis.read_from = 'primary'")
		.await
		.unwrap();
	assert!(!on_replica(&o, "select pg_is_in_recovery()").await);
	o.batch_execute("reset lepis.read_from").await.unwrap();
	assert!(on_replica(&o, "select pg_is_in_recovery()").await);
	// A read across nodes takes the home's share from the replica and the rest from the nodes,
	// and counts what one Postgres would.
	let all: i64 = plain
		.query_one("select count(*) from oracle.items", &[])
		.await
		.unwrap()
		.get(0);
	let across: i64 = a
		.query_one("select count(*) from oracle.items", &[])
		.await
		.unwrap()
		.get(0);
	assert_eq!(across, all);
	assert!(
		on_replica(&a, "select pg_is_in_recovery()").await,
		"a read keeps the session on the replica"
	);

	// A read that can write goes to the primary, and the session stays there.
	for sql in [
		"select pg_is_in_recovery(), nextval('rr.s')",
		"select pg_is_in_recovery() from rr.g where id = 1 for update",
		"select pg_is_in_recovery() from rr.g where id = 1 for share",
		"with x as (update rr.g set v = 'u' where id = 1 returning id) select pg_is_in_recovery() from x",
		"select pg_is_in_recovery(), rr.f()",
	] {
		let c = opted(&lepis).await;
		assert!(!on_replica(&c, sql).await, "{sql} ran on the replica");
		assert!(
			!on_replica(&c, "select pg_is_in_recovery()").await,
			"after {sql} the session read from the replica"
		);
	}
	let c = opted(&lepis).await;
	c.execute("insert into rr.g values (3, 'c')", &[])
		.await
		.unwrap();
	assert!(
		!on_replica(
			&c,
			"select pg_is_in_recovery(), count(*) from rr.g where id = 3"
		)
		.await,
		"a session reads its own writes"
	);

	// A read that writes where Lepis cannot see it (a view calling nextval) is refused by the
	// replica (25006) and runs again on the primary: the client gets its row, never the error.
	let before: i64 = home
		.query_one("select last_value from rr.s", &[])
		.await
		.unwrap()
		.get(0);
	let c = opted(&lepis).await;
	let row = c
		.query_one("select pg_is_in_recovery(), n from rr.w", &[])
		.await
		.expect("the replica's refusal reached the client");
	assert!(!row.get::<_, bool>(0));
	assert!(row.get::<_, i64>(1) > before);
	let c = opted(&lepis).await;
	assert!(!on_replica_simple(&c, "select pg_is_in_recovery(), n from rr.w").await);

	// A replica further behind than its max_lag_bytes is passed over, and taken again once it
	// keeps up.
	replica
		.batch_execute("select pg_wal_replay_pause()")
		.await
		.unwrap();
	home.batch_execute(
		"insert into rr.big select g, repeat('x', 1000) from generate_series(1, 4000) g",
	)
	.await
	.unwrap();
	tokio::time::sleep(SETTLE).await;
	let c = opted(&lepis).await;
	assert!(
		!on_replica(&c, "select pg_is_in_recovery()").await,
		"a replica 4 MB behind served a read"
	);
	replica
		.batch_execute("select pg_wal_replay_resume()")
		.await
		.unwrap();
	caught_up(&home, &replica).await;
	tokio::time::sleep(SETTLE).await;
	let c = opted(&lepis).await;
	assert!(
		on_replica(&c, "select pg_is_in_recovery()").await,
		"the replica was not taken again"
	);

	// A replica that does not answer is passed over without an error.
	set_replica(&home, host, "1").await;
	tokio::time::sleep(SETTLE).await;
	let c = opted(&lepis).await;
	assert!(!on_replica(&c, "select pg_is_in_recovery()").await);

	// A replica that dies mid-read, before the client has anything, costs the session nothing:
	// the read runs on the primary, and the dead replica serves nothing after it. Its postmaster
	// is killed (SIGKILL), so the backend reading exits as the standby goes.
	set_replica(&home, host, port).await;
	tokio::time::sleep(SETTLE).await;
	let c = opted(&lepis).await;
	assert!(
		on_replica(&c, "select pg_is_in_recovery()").await,
		"the replica was not taken back"
	);
	let postmaster: String = replica
		.query_one(
			"select split_part(pg_read_file('postmaster.pid'), E'\\n', 1)",
			&[],
		)
		.await
		.unwrap()
		.get(0);
	let slow = tokio::spawn(async move {
		let r = c
			.query_one("select pg_is_in_recovery(), pg_sleep(3)::text", &[])
			.await;
		(c, r)
	});
	tokio::time::sleep(Duration::from_millis(1_000)).await;
	let _ = replica
		.batch_execute(&format!(
			"copy (select 1) to program 'kill -KILL {postmaster}'"
		))
		.await;
	let (c, r) = slow.await.unwrap();
	let row = r.expect("the session ended with its replica");
	assert!(
		!row.get::<_, bool>(0),
		"the read was not run again on the primary"
	);
	assert!(
		!on_replica(&c, "select pg_is_in_recovery()").await,
		"a dead replica served a read"
	);
	let other = opted(&lepis).await;
	assert!(!on_replica_simple(&other, "select pg_is_in_recovery()").await);

	home.batch_execute(
		"update lepis.cluster set settings = settings - 'read_replicas' where id = 1;
		drop schema rr cascade;",
	)
	.await
	.unwrap();
}
