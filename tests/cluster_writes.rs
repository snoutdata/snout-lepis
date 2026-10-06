//! Writes across nodes through the router, on a real three-node cluster: transactions that take
//! in several nodes and commit through two-phase commit, UPDATE / DELETE that every node applies
//! to its own rows, an INSERT whose rows go to their owners (RETURNING in the order of VALUES),
//! reference-table writes on every copy, and what is refused (SERIALIZABLE, savepoints, values a
//! copy would compute differently, sequences that are not global).
//!
//! Needs `LEPIS_IT_HOME`, `LEPIS_IT_DATA_NODES` and `LEPIS_ORACLE_REFERENCE`; `scripts/it.sh`
//! provides them (`LEPIS_IT_ONLY=cluster_writes`). Skips without.

#[macro_use]
mod common;

use common::*;

fn owners(b: &Bed) -> impl Fn(i64) -> NodeId {
	let ids: Vec<NodeId> = node_ids(b).iter().map(|(i, _)| *i).collect();
	let ks = keyspace("tenant", KeyType::Int8, TENANT_SEED, &ids);
	move |t: i64| {
		ks.owner_of_hash(
			KeyType::Int8
				.hash_text_value(&t.to_string(), ks.seed)
				.unwrap(),
		)
	}
}

/// A tenant on each node, the home node's first.
fn one_per_node(b: &Bed) -> Vec<i64> {
	let owner = owners(b);
	let mut out: Vec<i64> = Vec::new();
	for (id, _) in node_ids(b) {
		out.push((1..500i64).find(|t| owner(*t) == id).unwrap());
	}
	out
}

async fn code(c: &Client, sql: &str) -> String {
	let e = c.simple_query(sql).await.unwrap_err();
	e.as_db_error()
		.map(|d| d.code().code().to_string())
		.unwrap_or_else(|| e.to_string())
}

async fn status_of(c: &Client, t: i64) -> String {
	c.query_one(
		"select status from oracle.tenants t join oracle.orders o using (tenant_id) where tenant_id = $1 and order_id = 1",
		&[&t],
	)
	.await
	.unwrap()
	.get(0)
}

async fn prepared_left(b: &Bed) -> i64 {
	let mut n = 0;
	for (_, a) in node_ids(b) {
		let c = connect(&a, "postgres", "x").await;
		let v: i64 = c
			.query_one("select count(*) from pg_prepared_xacts", &[])
			.await
			.unwrap()
			.get(0);
		n += v;
	}
	n
}

#[tokio::test]
async fn a_transaction_takes_in_every_node_and_commits_on_all() {
	let b = need_bed!();
	let lepis = start_lepis(&b).await;
	let c = connect(&lepis, "lepis_app", "app-pw").await;
	let t = one_per_node(&b);

	// COMMIT: every node's change, two-phase.
	c.batch_execute("begin").await.unwrap();
	for x in &t {
		c.execute(
			"update oracle.orders set status = 'tx-commit' where tenant_id = $1 and order_id = 1",
			&[x],
		)
		.await
		.unwrap();
	}
	c.batch_execute("commit").await.unwrap();
	for x in &t {
		assert_eq!(status_of(&c, *x).await, "tx-commit", "tenant {x}");
	}
	assert_eq!(prepared_left(&b).await, 0);

	// ROLLBACK: none of them.
	c.batch_execute("begin isolation level repeatable read")
		.await
		.unwrap();
	for x in &t {
		c.execute(
			"update oracle.orders set status = 'tx-rollback' where tenant_id = $1 and order_id = 1",
			&[x],
		)
		.await
		.unwrap();
	}
	c.batch_execute("rollback").await.unwrap();
	for x in &t {
		assert_eq!(status_of(&c, *x).await, "tx-commit", "tenant {x}");
	}

	// SET LOCAL reaches the nodes the transaction joins later.
	c.batch_execute("begin; set local timezone = 'Asia/Tokyo'")
		.await
		.unwrap();
	for x in &t {
		let tz: String = c
			.query_one(
				"select current_setting('TimeZone') from oracle.orders where tenant_id = $1 and order_id = 1",
				&[x],
			)
			.await
			.unwrap()
			.get(0);
		assert_eq!(tz, "Asia/Tokyo", "tenant {x}");
	}
	c.batch_execute("commit").await.unwrap();

	// A failure on one node aborts the whole transaction, as one Postgres aborts it.
	c.batch_execute("begin").await.unwrap();
	c.execute(
		"update oracle.orders set status = 'failed' where tenant_id = $1 and order_id = 1",
		&[&t[0]],
	)
	.await
	.unwrap();
	let e = c
		.execute(
			"update oracle.orders set total_cents = 1 / (order_id - 1) where tenant_id = $1 and order_id = 1",
			&[&t[1]],
		)
		.await
		.unwrap_err();
	assert_eq!(e.as_db_error().unwrap().code().code(), "22012");
	assert_eq!(
		code(
			&c,
			&format!("select 1 from oracle.orders where tenant_id = {}", t[2])
		)
		.await,
		"25P02"
	);
	let tag = c.simple_query("commit").await.unwrap();
	assert!(matches!(&tag[0], SimpleQueryMessage::CommandComplete(_)));
	assert_eq!(status_of(&c, t[0]).await, "tx-commit");
	assert_eq!(prepared_left(&b).await, 0);
}

#[tokio::test]
async fn what_a_transaction_across_nodes_cannot_keep_is_refused() {
	let b = need_bed!();
	let lepis = start_lepis(&b).await;
	let c = connect(&lepis, "lepis_app", "app-pw").await;
	let t = one_per_node(&b);
	let read = |x: i64| format!("select count(*) from oracle.orders where tenant_id = {x}");

	// SERIALIZABLE: each node would check only its own part.
	c.batch_execute("begin isolation level serializable")
		.await
		.unwrap();
	c.simple_query(&read(t[0])).await.unwrap();
	assert_eq!(code(&c, &read(t[1])).await, "0A000");
	c.batch_execute("rollback").await.unwrap();

	// Savepoints: ROLLBACK TO would undo one node's part only.
	c.batch_execute("begin").await.unwrap();
	c.simple_query(&read(t[0])).await.unwrap();
	c.batch_execute("savepoint a").await.unwrap();
	assert_eq!(code(&c, &read(t[1])).await, "0A000");
	c.batch_execute("rollback").await.unwrap();

	// A session is usable after each.
	c.simple_query(&read(t[1])).await.unwrap();
}

#[tokio::test]
async fn writes_across_nodes_happen_everywhere_or_nowhere() {
	let b = need_bed!();
	let lepis = start_lepis(&b).await;
	let reference = connect(&b.reference, "lepis_app", "app-pw").await;
	let c = connect(&lepis, "lepis_app", "app-pw").await;

	// UPDATE and DELETE: every node its own rows, the count is the sum.
	let sql = "update oracle.orders set status = status where total_cents > 45000";
	let want = reference.execute(sql, &[]).await.unwrap();
	assert!(want > 10);
	assert_eq!(c.execute(sql, &[]).await.unwrap(), want);
	let sql =
		"delete from oracle.items where qty = 5 and line = 4 returning tenant_id, order_id, line";
	let mut want: Vec<(i64, i64, i32)> = reference
		.query(sql, &[])
		.await
		.unwrap()
		.iter()
		.map(|r| (r.get(0), r.get(1), r.get(2)))
		.collect();
	let mut got: Vec<(i64, i64, i32)> = c
		.query(sql, &[])
		.await
		.unwrap()
		.iter()
		.map(|r| (r.get(0), r.get(1), r.get(2)))
		.collect();
	want.sort();
	got.sort();
	assert!(!want.is_empty());
	assert_eq!(got, want);
	assert_eq!(prepared_left(&b).await, 0);

	// An error on one node: nothing changes on any.
	let before: i64 = c
		.query_one(
			"select count(*) from oracle.orders where status = 'nowhere'",
			&[],
		)
		.await
		.unwrap()
		.get(0);
	let e = c
		.execute(
			"update oracle.orders set status = 'nowhere', total_cents = 1 / (tenant_id - 77) where total_cents > 100",
			&[],
		)
		.await
		.unwrap_err();
	assert_eq!(e.as_db_error().unwrap().code().code(), "22012");
	let after: i64 = c
		.query_one(
			"select count(*) from oracle.orders where status = 'nowhere'",
			&[],
		)
		.await
		.unwrap()
		.get(0);
	assert_eq!((before, after), (0, 0));

	// INSERT … VALUES with rows for every node: each row on its owner, RETURNING in order.
	let owner = owners(&b);
	let ids: Vec<i64> = (9301..9321).collect();
	let values: Vec<String> = ids
		.iter()
		.map(|t| format!("({t}, 'new {t}', 'CA', 1, now())"))
		.collect();
	let rows = c
		.query(
			&format!(
				"insert into oracle.tenants (tenant_id, name, country, plan_id, created_at) values {} returning tenant_id",
				values.join(", ")
			),
			&[],
		)
		.await
		.unwrap();
	let returned: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
	assert_eq!(returned, ids);
	for (id, address) in node_ids(&b) {
		let admin = connect(&address, "postgres", "x").await;
		let here: Vec<i64> = admin
			.query(
				"select tenant_id from oracle.tenants where tenant_id between 9301 and 9320",
				&[],
			)
			.await
			.unwrap()
			.iter()
			.map(|r| r.get(0))
			.collect();
		assert!(!here.is_empty(), "{address} got none");
		for t in here {
			assert_eq!(owner(t), id, "tenant {t} is on {address}");
		}
	}
	// … and in a transaction, then rolled back: none of them stays.
	c.batch_execute("begin").await.unwrap();
	c.batch_execute(
		"insert into oracle.tenants (tenant_id, name, country, plan_id, created_at) values (9401, 'a', 'CA', 1, now()), (9402, 'b', 'CA', 1, now()), (9403, 'c', 'CA', 1, now()), (9404, 'd', 'CA', 1, now())",
	)
	.await
	.unwrap_or_else(|e| panic!("{e:?}"));
	c.batch_execute("rollback").await.unwrap();
	let n: i64 = c
		.query_one(
			"select count(*) from oracle.tenants where tenant_id between 9401 and 9404",
			&[],
		)
		.await
		.unwrap()
		.get(0);
	assert_eq!(n, 0);
}

#[tokio::test]
async fn a_reference_table_is_written_on_every_copy() {
	let b = need_bed!();
	let lepis = start_lepis(&b).await;
	let c = connect(&lepis, "lepis_app", "app-pw").await;
	let n = c
		.execute(
			"update oracle.countries set name = 'Kanada' where code = 'CA'",
			&[],
		)
		.await
		.unwrap();
	assert_eq!(n, 1);
	c.execute(
		"insert into oracle.countries (code, name, region) values ('XX', 'Nowhere', 'None')",
		&[],
	)
	.await
	.unwrap();
	for (_, address) in node_ids(&b) {
		let admin = connect(&address, "postgres", "x").await;
		let names: Vec<String> = admin
			.query(
				"select name from oracle.countries where code in ('CA', 'XX') order by code",
				&[],
			)
			.await
			.unwrap()
			.iter()
			.map(|r| r.get(0))
			.collect();
		assert_eq!(names, ["Kanada", "Nowhere"], "{address}");
	}
	// Values a copy would compute for itself are refused.
	for sql in [
		"update oracle.countries set name = now()::text where code = 'XX'",
		"update oracle.countries set name = current_timestamp::text where code = 'XX'",
		"update oracle.countries set name = random()::text where code = 'XX'",
		"update oracle.countries set region = (select max(status) from oracle.orders) where code = 'XX'",
	] {
		assert_eq!(code(&c, sql).await, "0A000", "{sql}");
	}
	// Copies that already differ are not written over.
	let (_, first) = node_ids(&b).into_iter().nth(1).unwrap();
	connect(&first, "postgres", "x")
		.await
		.batch_execute("delete from oracle.countries where code = 'XX'")
		.await
		.unwrap();
	assert_eq!(
		code(
			&c,
			"update oracle.countries set name = 'Somewhere' where code = 'XX'"
		)
		.await,
		"XX001"
	);
	assert_eq!(prepared_left(&b).await, 0);
}

#[tokio::test]
async fn sequences_that_are_not_global_stay_on_the_home_node() {
	let b = need_bed!();
	for (_, address) in node_ids(&b) {
		connect(&address, "postgres", "x")
			.await
			.batch_execute(
				"create sequence if not exists oracle.plain_ids; grant usage on sequence oracle.plain_ids to lepis_app;",
			)
			.await
			.unwrap();
	}
	let lepis = start_lepis(&b).await;
	let c = connect(&lepis, "lepis_app", "app-pw").await;
	let t = one_per_node(&b);
	// On the home node a sequence is the one Postgres has; anywhere else it is refused (L15).
	c.simple_query(&format!(
		"select nextval('oracle.plain_ids') from oracle.tenants where tenant_id = {}",
		t[0]
	))
	.await
	.unwrap();
	assert_eq!(
		code(
			&c,
			&format!(
				"select nextval('oracle.plain_ids') from oracle.tenants where tenant_id = {}",
				t[1]
			)
		)
		.await,
		"0A000"
	);
}

/// An owner that logs in through Lepis: one verifier on every node, as L11 needs.
async fn owner_login(b: &Bed) {
	let home = connect(&b.home, "postgres", "x").await;
	home.batch_execute(
		"do $$ begin
			if not exists (select from pg_roles where rolname = 'lepis_owner') then
				create role lepis_owner login superuser password 'owner-pw';
			end if;
		end $$;",
	)
	.await
	.unwrap();
	let v: String = home
		.query_one(
			"select rolpassword from pg_authid where rolname = 'lepis_owner'",
			&[],
		)
		.await
		.unwrap()
		.get(0);
	for (_, address) in node_ids(b) {
		connect(&address, "postgres", "x")
			.await
			.batch_execute(&format!(
				"do $$ begin
					if exists (select from pg_roles where rolname = 'lepis_owner') then
						alter role lepis_owner superuser password {v};
					else
						create role lepis_owner login superuser password {v};
					end if;
				end $$;",
				v = catalog::quote_literal(&v)
			))
			.await
			.unwrap();
	}
}

async fn on_each(b: &Bed, sql: &str) -> Vec<String> {
	let mut out = Vec::new();
	for (_, address) in node_ids(b) {
		let c = connect(&address, "postgres", "x").await;
		let v: Option<String> = c.query_one(sql, &[]).await.unwrap().get(0);
		out.push(v.unwrap_or_default());
	}
	out
}

#[tokio::test]
async fn schema_changes_reach_every_node_or_none() {
	let b = need_bed!();
	owner_login(&b).await;
	let lepis = start_lepis(&b).await;
	let c = connect(&lepis, "lepis_owner", "owner-pw").await;
	let has_note = "select count(*)::text from information_schema.columns where table_schema = 'oracle' and table_name = 'orders' and column_name = 'note'";

	// Autocommit: every node, through two-phase commit.
	c.batch_execute("alter table oracle.orders add column note text")
		.await
		.unwrap();
	assert_eq!(on_each(&b, has_note).await, ["1", "1", "1"]);
	assert_eq!(prepared_left(&b).await, 0);

	// In a transaction: on every node when it commits, on none when it rolls back.
	c.batch_execute("begin").await.unwrap();
	c.batch_execute("alter table oracle.orders drop column note")
		.await
		.unwrap();
	c.batch_execute("rollback").await.unwrap();
	assert_eq!(on_each(&b, has_note).await, ["1", "1", "1"]);
	c.batch_execute("begin").await.unwrap();
	c.batch_execute("alter table oracle.orders drop column note")
		.await
		.unwrap();
	c.batch_execute("commit").await.unwrap();
	assert_eq!(on_each(&b, has_note).await, ["0", "0", "0"]);

	// A node that refuses rolls everything back.
	let (_, second) = node_ids(&b).into_iter().nth(1).unwrap();
	connect(&second, "postgres", "x")
		.await
		.batch_execute("alter table oracle.orders add column note int")
		.await
		.unwrap();
	let e = c
		.batch_execute("alter table oracle.orders add column note text")
		.await
		.unwrap_err();
	assert_eq!(e.as_db_error().unwrap().code().code(), "42701");
	assert_eq!(on_each(&b, has_note).await, ["0", "1", "0"]);
	connect(&second, "postgres", "x")
		.await
		.batch_execute("alter table oracle.orders drop column note")
		.await
		.unwrap();

	// What no transaction block may hold runs node by node, as a job.
	let has_index = "select count(*)::text from pg_indexes where schemaname = 'oracle' and indexname = 'orders_status_idx'";
	c.batch_execute("create index concurrently orders_status_idx on oracle.orders (status)")
		.await
		.unwrap();
	assert_eq!(on_each(&b, has_index).await, ["1", "1", "1"]);
	let jobs: i64 = connect(&b.home, "postgres", "x")
		.await
		.query_one(
			"select count(*) from lepis.ddl_job_node n join lepis.ddl_job j on j.id = n.job_id where j.sql like '%orders_status_idx%' and n.state = 'done'",
			&[],
		)
		.await
		.unwrap()
		.get(0);
	assert_eq!(jobs, 3);
	c.batch_execute("begin").await.unwrap();
	assert_eq!(
		code(
			&c,
			"create index concurrently orders_status_idx2 on oracle.orders (status)"
		)
		.await,
		"25001"
	);
	c.batch_execute("rollback").await.unwrap();

	// An index is found by its table: DROP INDEX reaches every node.
	c.batch_execute("drop index oracle.orders_status_idx")
		.await
		.unwrap();
	assert_eq!(on_each(&b, has_index).await, ["0", "0", "0"]);

	// What would break the catalog is refused.
	assert_eq!(
		code(&c, "alter table oracle.orders rename to orders2").await,
		"0A000"
	);
}

#[tokio::test]
async fn a_role_reaches_every_node_with_its_verifier() {
	let b = need_bed!();
	owner_login(&b).await;
	let lepis = start_lepis(&b).await;
	let c = connect(&lepis, "lepis_owner", "owner-pw").await;
	let _ = c.batch_execute("drop role if exists lepis_new").await;
	c.batch_execute("create role lepis_new login password 'new-pw'")
		.await
		.unwrap();
	let verifiers = on_each(
		&b,
		"select rolpassword from pg_authid where rolname = 'lepis_new'",
	)
	.await;
	assert!(verifiers[0].starts_with("SCRAM-SHA-256$"), "{verifiers:?}");
	assert!(
		verifiers.iter().all(|v| *v == verifiers[0]),
		"{verifiers:?}"
	);
	// The new role logs in through Lepis and reaches every node.
	c.batch_execute("grant usage on schema oracle to lepis_new; ")
		.await
		.unwrap();
	c.batch_execute("grant select on all tables in schema oracle to lepis_new")
		.await
		.unwrap();
	let n = connect(&lepis, "lepis_new", "new-pw").await;
	n.simple_query("select count(*) from oracle.orders")
		.await
		.unwrap();
	c.batch_execute("drop owned by lepis_new").await.unwrap();
	c.batch_execute("drop role lepis_new").await.unwrap();
	assert_eq!(
		on_each(
			&b,
			"select count(*)::text from pg_roles where rolname = 'lepis_new'"
		)
		.await,
		["0", "0", "0"]
	);
}

async fn copy(c: &Client, sql: &str, data: &[u8]) -> Result<u64, tokio_postgres::Error> {
	use futures_util::SinkExt;
	let sink = c.copy_in(sql).await?;
	futures_util::pin_mut!(sink);
	for chunk in data.chunks(7) {
		sink.send(bytes::Bytes::copy_from_slice(chunk)).await?;
	}
	sink.finish().await
}

async fn tenants_between(b: &Bed, lo: i64, hi: i64) -> Vec<(i64, NodeId)> {
	let mut out = Vec::new();
	for (id, address) in node_ids(b) {
		let admin = connect(&address, "postgres", "x").await;
		for r in admin
			.query(
				"select tenant_id from oracle.tenants where tenant_id between $1 and $2",
				&[&lo, &hi],
			)
			.await
			.unwrap()
		{
			out.push((r.get::<_, i64>(0), id));
		}
	}
	out.sort();
	out
}

#[tokio::test]
async fn copy_sends_each_row_to_its_owner() {
	let b = need_bed!();
	let lepis = start_lepis(&b).await;
	let c = connect(&lepis, "lepis_app", "app-pw").await;
	let owner = owners(&b);

	// Text, split across CopyData messages at every 7 bytes.
	let mut text = String::new();
	for t in 9501..9541i64 {
		text.push_str(&format!(
			"{t}\tname\\\\t{t}\tCA\t1\t2024-01-01 00:00:00+00\n"
		));
	}
	let n = copy(
		&c,
		"copy oracle.tenants (tenant_id, name, country, plan_id, created_at) from stdin",
		text.as_bytes(),
	)
	.await
	.unwrap();
	assert_eq!(n, 40);
	let rows = tenants_between(&b, 9501, 9540).await;
	assert_eq!(rows.len(), 40);
	assert!(rows.iter().all(|(t, n)| owner(*t) == *n), "{rows:?}");
	assert!(
		rows.iter()
			.map(|(_, n)| *n)
			.collect::<std::collections::BTreeSet<_>>()
			.len() > 1
	);

	// CSV with a header, quotes and the key not first.
	let mut csv = String::from("name,tenant_id,country,plan_id,created_at\n");
	for t in 9601..9621i64 {
		csv.push_str(&format!(
			"\"a, \"\"quoted\"\"\nname\",{t},CA,1,2024-01-01\n"
		));
	}
	let n = copy(
		&c,
		"copy oracle.tenants (name, tenant_id, country, plan_id, created_at) from stdin with (format csv, header true)",
		csv.as_bytes(),
	)
	.await
	.unwrap();
	assert_eq!(n, 20);
	let rows = tenants_between(&b, 9601, 9620).await;
	assert_eq!(rows.len(), 20);
	assert!(rows.iter().all(|(t, n)| owner(*t) == *n));

	// A row that fails on its node fails the whole COPY, on every node.
	let bad =
		"9701\tx\tCA\t1\t2024-01-01\n9702\tx\tZZ\t1\t2024-01-01\n9703\tx\tCA\t1\t2024-01-01\n";
	let e = copy(&c, "copy oracle.tenants from stdin", bad.as_bytes())
		.await
		.unwrap_err();
	assert_eq!(e.as_db_error().unwrap().code().code(), "23503");
	assert!(tenants_between(&b, 9701, 9703).await.is_empty());
	// A row without its key is refused, and nothing is written.
	let e = copy(
		&c,
		"copy oracle.tenants from stdin",
		b"9801\tx\tCA\t1\t2024-01-01\n\\N\tx\tCA\t1\t2024-01-01\n",
	)
	.await
	.unwrap_err();
	assert_eq!(e.as_db_error().unwrap().code().code(), "0A000");
	assert!(tenants_between(&b, 9801, 9801).await.is_empty());

	// Inside a transaction: undone by its ROLLBACK, kept by its COMMIT.
	c.batch_execute("begin").await.unwrap();
	copy(
		&c,
		"copy oracle.tenants from stdin",
		b"9901\tx\tCA\t1\t2024-01-01\n9902\tx\tCA\t1\t2024-01-01\n9903\tx\tCA\t1\t2024-01-01\n",
	)
	.await
	.unwrap();
	c.batch_execute("rollback").await.unwrap();
	assert!(tenants_between(&b, 9901, 9903).await.is_empty());
	c.batch_execute("begin").await.unwrap();
	copy(
		&c,
		"copy oracle.tenants from stdin",
		b"9901\tx\tCA\t1\t2024-01-01\n9902\tx\tCA\t1\t2024-01-01\n9903\tx\tCA\t1\t2024-01-01\n",
	)
	.await
	.unwrap();
	c.batch_execute("commit").await.unwrap();
	assert_eq!(tenants_between(&b, 9901, 9903).await.len(), 3);

	// Binary, with the key's own type.
	{
		use tokio_postgres::binary_copy::BinaryCopyInWriter;
		use tokio_postgres::types::Type;
		let sink = c
			.copy_in("copy oracle.tenants (tenant_id, name, country, plan_id, created_at) from stdin (format binary)")
			.await
			.unwrap();
		let w = BinaryCopyInWriter::new(
			sink,
			&[
				Type::INT8,
				Type::TEXT,
				Type::TEXT,
				Type::INT4,
				Type::TIMESTAMPTZ,
			],
		);
		futures_util::pin_mut!(w);
		let at = std::time::SystemTime::UNIX_EPOCH;
		for t in 9951..9961i64 {
			w.as_mut()
				.write(&[&t, &"bin", &"CA", &1i32, &at])
				.await
				.unwrap();
		}
		assert_eq!(w.finish().await.unwrap(), 10);
	}
	let rows = tenants_between(&b, 9951, 9960).await;
	assert_eq!(rows.len(), 10);
	assert!(rows.iter().all(|(t, n)| owner(*t) == *n));
	assert_eq!(prepared_left(&b).await, 0);

	// The session goes on.
	c.simple_query("select count(*) from oracle.tenants")
		.await
		.unwrap();
}

/// A node a physical split is adding: in the catalog, `joining`, owning nothing, and (here) not
/// even reachable. Nothing the router does may wait on it.
#[tokio::test]
async fn a_joining_node_is_left_out_of_every_fan_out() {
	let b = need_bed!();
	let home = connect(&b.home, "postgres", "x").await;
	home.batch_execute(
		"insert into lepis.node (id, name, host, port, dbname, sslmode, kind, state) \
		values (9, 'n9', 'joining.invalid', 5432, 'postgres', 'disable', 'data', 'joining'); \
		select lepis.bump();",
	)
	.await
	.unwrap();
	let lepis = start_lepis(&b).await;
	let c = connect(&lepis, "lepis_app", "app-pw").await;
	let t = one_per_node(&b);
	let result = async {
		// A reference-table write, a write across nodes, a read across nodes, and a transaction
		// over every node.
		c.execute(
			"update oracle.countries set name = name where code = 'CA'",
			&[],
		)
		.await?;
		c.execute(
			"update oracle.orders set status = status where total_cents > 45000",
			&[],
		)
		.await?;
		c.simple_query("select count(*) from oracle.orders").await?;
		c.batch_execute("begin").await?;
		for x in &t {
			c.execute(
				"update oracle.orders set status = status where tenant_id = $1",
				&[x],
			)
			.await?;
		}
		c.batch_execute("commit").await?;
		Ok::<(), tokio_postgres::Error>(())
	}
	.await;
	let owner = connect(&b.home, "postgres", "x").await;
	owner
		.batch_execute("delete from lepis.node where id = 9; select lepis.bump();")
		.await
		.unwrap();
	result.unwrap();
}

/// A client that keeps the NoticeResponses it is sent.
async fn noticing(
	address: &str,
	user: &str,
	password: &str,
) -> (Client, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
	let (host, port) = address.rsplit_once(':').unwrap();
	let (c, mut conn) = tokio_postgres::Config::new()
		.host(host)
		.port(port.parse().unwrap())
		.user(user)
		.password(password)
		.dbname("postgres")
		.connect(NoTls)
		.await
		.unwrap();
	let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
	let keep = seen.clone();
	tokio::spawn(async move {
		while let Some(m) = std::future::poll_fn(|cx| conn.poll_message(cx)).await {
			if let Ok(tokio_postgres::AsyncMessage::Notice(n)) = m {
				keep.lock()
					.unwrap()
					.push(format!("{} {}", n.code().code(), n.message()));
			}
		}
	});
	(c, seen)
}

#[tokio::test]
async fn a_role_change_a_node_missed_is_a_warning() {
	let b = need_bed!();
	owner_login(&b).await;
	let lepis = start_lepis(&b).await;
	let (c, notices) = noticing(&lepis, "lepis_owner", "owner-pw").await;
	let _ = c.batch_execute("drop role if exists lepis_gone").await;
	notices.lock().unwrap().clear();
	c.batch_execute("create role lepis_gone").await.unwrap();
	assert!(
		notices.lock().unwrap().is_empty(),
		"{:?}",
		notices.lock().unwrap()
	);
	// On one data node the role owns something, so dropping it there fails.
	let (_, second) = node_ids(&b).into_iter().nth(1).unwrap();
	let n2 = connect(&second, "postgres", "x").await;
	n2.batch_execute("create table public.lepis_gone_owns (x int); alter table public.lepis_gone_owns owner to lepis_gone;")
		.await
		.unwrap();
	c.batch_execute("drop role lepis_gone").await.unwrap();
	let seen = notices.lock().unwrap().clone();
	assert!(
		seen.iter()
			.any(|n| n.starts_with("01000 ") && n.contains("not copied")),
		"{seen:?}"
	);
	n2.batch_execute("drop table public.lepis_gone_owns; drop role if exists lepis_gone;")
		.await
		.unwrap();
}

/// The analysis of a statement is cached by its text; a change of search_path (or of the
/// catalog) must not reuse one made under the old one.
#[tokio::test]
async fn a_cached_analysis_follows_the_search_path() {
	let b = need_bed!();
	let lepis = start_lepis(&b).await;
	let reference = connect(&b.reference, "lepis_app", "app-pw").await;
	let c = connect(&lepis, "lepis_app", "app-pw").await;
	let sql = "select count(*) from orders";
	// Under public, there is no such table: the home node says so.
	assert!(c.simple_query(sql).await.is_err());
	c.batch_execute("set search_path = oracle, public")
		.await
		.unwrap();
	reference
		.batch_execute("set search_path = oracle, public")
		.await
		.unwrap();
	// Under oracle, the same text is the sharded table, read across every node.
	let want = answer(&reference, sql, false).await;
	assert_eq!(answer(&c, sql, false).await, want);
	assert_eq!(answer(&c, sql, false).await, want);
	let stmt = c
		.prepare("select count(*) from orders where tenant_id = $1")
		.await
		.unwrap();
	for t in 1..20i64 {
		let got: i64 = c.query_one(&stmt, &[&t]).await.unwrap().get(0);
		let want: i64 = reference
			.query_one("select count(*) from orders where tenant_id = $1", &[&t])
			.await
			.unwrap()
			.get(0);
		assert_eq!(got, want, "tenant {t}");
	}
}
