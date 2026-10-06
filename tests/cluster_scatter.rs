//! Phase 2 against a real three-node cluster: reads across nodes through the extended protocol
//! (binary results, compared byte for byte with one Postgres), parameters, a user type, EXPLAIN,
//! and every refusal with its SQLSTATE and its fix. The simple-protocol half is the oracle in
//! tests/cluster.rs.
//!
//! Needs `LEPIS_IT_HOME`, `LEPIS_IT_DATA_NODES` and `LEPIS_ORACLE_REFERENCE`; `scripts/it.sh`
//! provides them (`LEPIS_IT_ONLY=cluster_scatter` runs this file alone). Skips without.

#[macro_use]
mod common;

use common::*;
use tokio_postgres::types::{FromSql, ToSql, Type};

/// A value exactly as the server sent it, whatever its type.
struct Raw(Option<Vec<u8>>);

impl<'a> FromSql<'a> for Raw {
	fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		Ok(Raw(Some(raw.to_vec())))
	}

	fn from_sql_null(_: &Type) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		Ok(Raw(None))
	}

	fn accepts(_: &Type) -> bool {
		true
	}
}

type Binary = Result<Vec<Vec<Option<Vec<u8>>>>, (String, String)>;

/// A query, its parameters, and whether its rows are compared in order.
type Case = (&'static str, Vec<Box<dyn ToSql + Sync>>, bool);

fn rows_of(rows: Vec<tokio_postgres::Row>, ordered: bool) -> Vec<Vec<Option<Vec<u8>>>> {
	let mut out: Vec<Vec<Option<Vec<u8>>>> = rows
		.iter()
		.map(|r| (0..r.len()).map(|i| r.get::<_, Raw>(i).0).collect())
		.collect();
	if !ordered {
		out.sort();
	}
	out
}

fn error_of(e: tokio_postgres::Error) -> (String, String) {
	e.as_db_error()
		.map(|d| (d.code().code().to_string(), d.message().to_string()))
		.unwrap_or_else(|| (String::new(), e.to_string()))
}

/// The query through the extended protocol, results in binary.
async fn binary(c: &Client, sql: &str, params: &[&(dyn ToSql + Sync)], ordered: bool) -> Binary {
	c.query(sql, params)
		.await
		.map(|r| rows_of(r, ordered))
		.map_err(error_of)
}

/// The corpus's reads across nodes: (name, ordered, sql).
fn scatter_queries() -> Vec<(String, bool, String)> {
	let mut out: Vec<(String, bool, String, String)> = Vec::new();
	for line in QUERIES.lines() {
		if let Some(h) = line.strip_prefix("-- name: ") {
			let mut w = h.split_whitespace();
			let name = w.next().unwrap().to_string();
			let rest: Vec<&str> = w.collect();
			let route = rest
				.iter()
				.find_map(|x| x.strip_prefix("route="))
				.unwrap_or("single")
				.to_string();
			out.push((name, rest.contains(&"ordered"), route, String::new()));
		} else if let Some(q) = out.last_mut()
			&& !line.starts_with("--")
		{
			q.3.push_str(line);
			q.3.push('\n');
		}
	}
	out.into_iter()
		.filter(|q| q.2 == "scatter")
		.map(|(n, o, _, s)| (n, o, s.trim().trim_end_matches(';').to_string()))
		.collect()
}

#[tokio::test]
async fn binary_answers_match_one_postgres_byte_for_byte() {
	let b = need_bed!();
	let lepis = start_lepis(&b).await;
	let reference = connect(&b.reference, "lepis_app", "app-pw").await;
	let cluster = connect(&lepis, "lepis_app", "app-pw").await;
	let queries = scatter_queries();
	assert!(queries.len() >= 30, "{} scatter queries", queries.len());
	let mut wrong = Vec::new();
	for (name, ordered, sql) in &queries {
		let want = binary(&reference, sql, &[], *ordered).await;
		let got = binary(&cluster, sql, &[], *ordered).await;
		assert!(want.is_ok(), "{name}: the reference failed: {want:?}");
		if got != want {
			wrong.push(format!(
				"{name}:\n  reference {want:?}\n  cluster   {got:?}"
			));
		}
	}
	assert!(wrong.is_empty(), "{}", wrong.join("\n"));

	// The whole table in order, no LIMIT: a k-way merge of every row.
	let all = "select tenant_id, order_id, total_cents, placed_at from oracle.orders order by total_cents desc, placed_at, tenant_id, order_id";
	let want = binary(&reference, all, &[], true).await.unwrap();
	assert!(want.len() > 5000);
	assert_eq!(binary(&cluster, all, &[], true).await.unwrap(), want);
}

#[tokio::test]
async fn parameters_reach_every_node_and_the_merge() {
	let b = need_bed!();
	let lepis = start_lepis(&b).await;
	let reference = connect(&b.reference, "lepis_app", "app-pw").await;
	let cluster = connect(&lepis, "lepis_app", "app-pw").await;
	let cases: Vec<Case> = vec![
		(
			"select count(*), sum(total_cents) from oracle.orders where total_cents > $1",
			vec![Box::new(40_000i64)],
			true,
		),
		(
			"select tenant_id, order_id from oracle.orders order by placed_at, tenant_id, order_id limit $1 offset $2",
			vec![Box::new(7i64), Box::new(30i64)],
			true,
		),
		(
			"select status, count(*) from oracle.orders where total_cents < $1 group by status having count(*) > $2 order by 2 desc",
			vec![Box::new(30_000i64), Box::new(100i64)],
			true,
		),
		(
			"select round(avg(total_cents), $1) from oracle.orders where status = $2",
			vec![Box::new(3i32), Box::new("paid".to_string())],
			true,
		),
	];
	for (sql, params, ordered) in &cases {
		let p: Vec<&(dyn ToSql + Sync)> = params.iter().map(|b| b.as_ref()).collect();
		let want = binary(&reference, sql, &p, *ordered).await;
		assert!(want.is_ok(), "{sql}: {want:?}");
		assert_eq!(binary(&cluster, sql, &p, *ordered).await, want, "{sql}");
	}

	// Parse, Bind, Describe and Execute in one batch (the unnamed statement).
	let want = typed(&reference).await;
	assert!(want.is_ok());
	assert_eq!(typed(&cluster).await, want);
}

async fn typed(c: &Client) -> Binary {
	c.query_typed(
		"select count(*), max(total_cents) from oracle.orders where status = $1",
		&[(&"refunded", Type::TEXT)],
	)
	.await
	.map(|r| rows_of(r, true))
	.map_err(error_of)
}

async fn lines(c: &Client, sql: &str) -> Vec<String> {
	c.simple_query(sql)
		.await
		.unwrap()
		.into_iter()
		.filter_map(|m| match m {
			SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
			_ => None,
		})
		.collect()
}

#[tokio::test]
async fn a_user_type_is_ordered_by_its_own_rules() {
	let b = need_bed!();
	// An enum orders by its declared order, not alphabetically: a type Lepis knows nothing
	// about, ranked by the home node.
	for target in std::iter::once(&b.reference).chain(node_ids(&b).iter().map(|(_, a)| a)) {
		connect(target, "postgres", "x")
			.await
			.batch_execute(
				"do $$ begin
					if not exists (select from pg_type where typname = 'st') then
						create type oracle.st as enum ('shipped', 'new', 'refunded', 'paid');
					end if;
				end $$;
				grant usage on type oracle.st to lepis_app;",
			)
			.await
			.unwrap();
	}
	let lepis = start_lepis(&b).await;
	let reference = connect(&b.reference, "lepis_app", "app-pw").await;
	let cluster = connect(&lepis, "lepis_app", "app-pw").await;
	for (sql, ordered) in [
		(
			"select tenant_id, order_id, status::oracle.st from oracle.orders order by 3, 1, 2 limit 40",
			true,
		),
		(
			"select status::oracle.st, count(*) from oracle.orders group by 1 order by 1 desc",
			true,
		),
		(
			"select min(status::oracle.st), max(status::oracle.st) from oracle.orders",
			true,
		),
		(
			"select distinct status::oracle.st from oracle.orders order by 1",
			true,
		),
	] {
		let want = answer(&reference, sql, ordered).await;
		assert!(want.is_ok(), "{sql}: {want:?}");
		assert_eq!(answer(&cluster, sql, ordered).await, want, "{sql}");
		let want = binary(&reference, sql, &[], ordered).await;
		assert_eq!(
			binary(&cluster, sql, &[], ordered).await,
			want,
			"{sql} (binary)"
		);
	}
}

#[tokio::test]
async fn every_refusal_has_its_code_and_its_fix() {
	let b = need_bed!();
	// A user-defined aggregate Lepis cannot combine, on every node.
	for (_, address) in node_ids(&b) {
		connect(&address, "postgres", "x")
			.await
			.batch_execute(
				"do $$ begin
					if not exists (select from pg_proc where proname = 'mysum') then
						create aggregate oracle.mysum(bigint) (sfunc = int8pl, stype = bigint);
					end if;
				end $$;",
			)
			.await
			.unwrap();
	}
	let lepis = start_lepis(&b).await;
	let reference = connect(&b.reference, "lepis_app", "app-pw").await;
	let c = connect(&lepis, "lepis_app", "app-pw").await;
	let refused = [
		"select tenant_id, rank() over (order by total_cents) from oracle.orders",
		"select string_agg(name, ',') from oracle.tenants",
		"select sum(value) from oracle.events",
		"select avg(value) from oracle.events",
		"select array_agg(status order by status) from oracle.orders",
		"select oracle.mysum(total_cents) from oracle.orders",
		"select tenant_id from oracle.orders union all select tenant_id from oracle.items",
		"select distinct on (status) status from oracle.orders",
		"select tenant_id from oracle.orders for update",
		"select count(*) from oracle.orders where total_cents > (select avg(total_cents) from oracle.orders)",
		"select count(*) from (select distinct status from oracle.orders) s",
		"select upper(status) as status, count(*) from oracle.orders group by status",
		"select tenant_id from oracle.orders order by total_cents using <",
		"select tenant_id from oracle.orders order by total_cents fetch first 3 rows with ties",
		"select status, count(*) from oracle.orders group by rollup (status)",
		"select count(*) || 'x' from oracle.orders",
		"select count(*) from oracle.orders having count(*)::text = '1'",
		"explain (format json) select count(*) from oracle.orders",
		"select count(*) from oracle.orders; select 1",
	];
	for sql in refused {
		let e = c.simple_query(sql).await.unwrap_err();
		let d = e.as_db_error().unwrap_or_else(|| panic!("{sql}: {e}"));
		assert_eq!(d.code().code(), "0A000", "{sql}: {}", d.message());
		let hint = d.hint().unwrap_or_else(|| panic!("{sql}: no hint"));
		assert!(!hint.is_empty(), "{sql}");
		assert!(
			!d.message().contains('\u{2014}') && !hint.contains('\u{2014}'),
			"{sql}: an em-dash in a message"
		);
		// The session is usable after every refusal.
		c.simple_query("select 1").await.unwrap();
	}
	// The unqualified name, found by asking the home node which functions are aggregates.
	c.batch_execute("set search_path = oracle, public")
		.await
		.unwrap();
	let e = c
		.simple_query("select mysum(total_cents) from orders")
		.await
		.unwrap_err();
	assert_eq!(e.as_db_error().unwrap().code().code(), "0A000");
	c.batch_execute("reset search_path").await.unwrap();

	// Postgres's own errors stay Postgres's own.
	for sql in [
		"select tenant_id from oracle.orders limit -1",
		"select tenant_id from oracle.orders offset -1",
		"select no_such_column from oracle.orders order by 1",
	] {
		let want = reference.simple_query(sql).await.unwrap_err();
		let got = c.simple_query(sql).await.unwrap_err();
		assert_eq!(
			got.as_db_error().unwrap().code(),
			want.as_db_error().unwrap().code(),
			"{sql}"
		);
	}

	// Inside a transaction block a read across nodes runs in the transaction on every node, and
	// sees its writes …
	let count = |rows: Vec<SimpleQueryMessage>| -> String {
		rows.iter()
			.find_map(|m| match m {
				SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
				_ => None,
			})
			.unwrap()
	};
	let mine = count(
		c.simple_query("select count(*) from oracle.orders where tenant_id = 1")
			.await
			.unwrap(),
	);
	c.batch_execute("begin").await.unwrap();
	c.simple_query("update oracle.orders set status = 'in-tx' where tenant_id = 1")
		.await
		.unwrap();
	let seen = count(
		c.simple_query("select count(*) from oracle.orders where status = 'in-tx'")
			.await
			.unwrap(),
	);
	assert_eq!(seen, mine);
	c.batch_execute("rollback").await.unwrap();
	let after = count(
		c.simple_query("select count(*) from oracle.orders where status = 'in-tx'")
			.await
			.unwrap(),
	);
	assert_eq!(after, "0");
	// … except where a savepoint is open, which no other node could follow.
	c.batch_execute("begin").await.unwrap();
	c.simple_query("select count(*) from oracle.orders where tenant_id = 1")
		.await
		.unwrap();
	c.batch_execute("savepoint a").await.unwrap();
	let e = c
		.simple_query("select count(*) from oracle.orders")
		.await
		.unwrap_err();
	assert_eq!(e.as_db_error().unwrap().code().code(), "0A000");
	c.batch_execute("rollback").await.unwrap();
	c.simple_query("select count(*) from oracle.orders")
		.await
		.unwrap();
}

#[tokio::test]
async fn explain_shows_every_node_and_the_routing() {
	let b = need_bed!();
	let lepis = start_lepis(&b).await;
	let c = connect(&lepis, "lepis_app", "app-pw").await;
	let plan = lines(
		&c,
		"explain select status, count(*) from oracle.orders group by status order by 2 desc limit 2",
	)
	.await;
	assert!(
		plan[0].starts_with("Lepis: 3 nodes (n1, n2, n3); partial aggregates"),
		"{plan:?}"
	);
	assert!(
		plan[1].starts_with("Lepis: each node runs: SELECT"),
		"{plan:?}"
	);
	for n in ["n1", "n2", "n3"] {
		assert!(plan.contains(&format!("Node {n}:")), "{plan:?}");
	}
	assert!(plan.iter().any(|l| l.contains("Aggregate")), "{plan:?}");

	let plan = lines(
		&c,
		"explain (analyze, costs off) select tenant_id from oracle.orders order by total_cents limit 3",
	)
	.await;
	assert!(plan[0].contains("merged in order"), "{plan:?}");
	assert!(plan.iter().any(|l| l.contains("actual")), "{plan:?}");

	let plan = lines(
		&c,
		"explain select * from oracle.orders where tenant_id = 7",
	)
	.await;
	assert!(plan[0].starts_with("Lepis: one node, n"), "{plan:?}");
	assert!(plan.iter().any(|l| l.starts_with("  ")), "{plan:?}");
}

/// Whether any node is still running a statement whose text holds `marker`.
async fn still_running(b: &Bed, marker: &str) -> Vec<String> {
	let mut out = Vec::new();
	for (_, address) in node_ids(b) {
		let admin = connect(&address, "postgres", "x").await;
		let n: i64 = admin
			.query_one(
				"select count(*) from pg_stat_activity where state = 'active' and pid <> pg_backend_pid() and query like '%' || $1 || '%'",
				&[&marker],
			)
			.await
			.unwrap()
			.get(0);
		if n > 0 {
			out.push(address);
		}
	}
	out
}

#[tokio::test]
async fn a_cancel_reaches_every_node_a_statement_runs_on() {
	let b = need_bed!();
	let lepis = start_lepis(&b).await;
	let reference = connect(&b.reference, "lepis_app", "app-pw").await;
	let _ = reference;
	for (marker, sql) in [
		(
			"cancel_read_marker",
			"select tenant_id, 'cancel_read_marker' from oracle.tenants where pg_sleep(2) is not null",
		),
		(
			"cancel_write_marker",
			"update oracle.orders set status = status where pg_sleep(2) is not null and 'cancel_write_marker' <> ''",
		),
	] {
		let c = connect(&lepis, "lepis_app", "app-pw").await;
		let token = c.cancel_token();
		let run = tokio::spawn(async move { c.simple_query(sql).await.map(|_| ()) });
		tokio::time::sleep(std::time::Duration::from_millis(700)).await;
		assert_eq!(
			still_running(&b, marker).await.len(),
			3,
			"{marker} runs everywhere"
		);
		token.cancel_query(NoTls).await.unwrap();
		let e = run.await.unwrap().unwrap_err();
		assert_eq!(e.as_db_error().unwrap().code().code(), "57014", "{marker}");
		let mut left = Vec::new();
		for _ in 0..20 {
			left = still_running(&b, marker).await;
			if left.is_empty() {
				break;
			}
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		}
		assert!(left.is_empty(), "{marker} still running on {left:?}");
	}
}

#[tokio::test]
async fn a_large_read_streams_through_the_merge() {
	let b = need_bed!();
	let lepis = start_lepis(&b).await;
	let reference = connect(&b.reference, "lepis_app", "app-pw").await;
	let c = connect(&lepis, "lepis_app", "app-pw").await;
	// Many windows per node, merged by a text key the home node ranks window by window.
	let sql = "select o.status, o.tenant_id, o.order_id, g from oracle.orders o, generate_series(1, 40) g order by o.status, o.tenant_id, o.order_id, g";
	let want = answer(&reference, sql, true).await;
	let got = answer(&c, sql, true).await;
	assert!(
		matches!(&want, Ok(r) if r.len() > 3000),
		"{:?}",
		want.as_ref().map(|r| r.len())
	);
	assert_eq!(got, want);
	// A LIMIT stops the read and leaves every node's session usable.
	let sql = "select o.status, o.tenant_id, o.order_id, g from oracle.orders o, generate_series(1, 40) g order by o.status desc, o.tenant_id, o.order_id, g offset 7 limit 11";
	assert_eq!(
		answer(&c, sql, true).await,
		answer(&reference, sql, true).await
	);
	// Unordered: every row once.
	let sql = "select o.tenant_id, o.order_id, g from oracle.orders o, generate_series(1, 40) g";
	assert_eq!(
		answer(&c, sql, false).await,
		answer(&reference, sql, false).await
	);
	// And inside a transaction, the portals close with it.
	c.batch_execute("begin").await.unwrap();
	let sql = "select o.status, o.order_id from oracle.orders o order by 1, 2 limit 3";
	assert_eq!(
		answer(&c, sql, true).await,
		answer(&reference, sql, true).await
	);
	assert_eq!(
		answer(&c, sql, true).await,
		answer(&reference, sql, true).await
	);
	c.batch_execute("commit").await.unwrap();
}
