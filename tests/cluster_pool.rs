//! Transaction pooling (`pool_mode` = `transaction`) and routing by a JWT claim
//! (`route_claim`), through the router on a real three-node cluster.
//!
//! Needs `LEPIS_IT_HOME`, `LEPIS_IT_DATA_NODES` and `LEPIS_ORACLE_REFERENCE`; `scripts/it.sh`
//! provides them (`LEPIS_IT_ONLY=cluster_pool`). Skips without.

#[macro_use]
mod common;

use common::*;

/// Sets one cluster setting (a JSON value) and waits until a router would have read it.
async fn setting(b: &Bed, name: &str, value: &str) {
	connect(&b.home, "postgres", "x")
		.await
		.execute(
			"update lepis.cluster set settings = settings || jsonb_build_object($1::text, ($2::text)::jsonb) where id = 1",
			&[&name, &value],
		)
		.await
		.unwrap();
	tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
}

async fn clear_settings(b: &Bed) {
	connect(&b.home, "postgres", "x")
		.await
		.batch_execute("update lepis.cluster set settings = '{}' where id = 1")
		.await
		.unwrap();
	tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
}

async fn one(c: &Client, sql: &str) -> String {
	let rows = c.simple_query(sql).await.unwrap();
	rows.iter()
		.find_map(|m| match m {
			SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
			_ => None,
		})
		.unwrap()
}

async fn code(c: &Client, sql: &str) -> String {
	let e = c.simple_query(sql).await.unwrap_err();
	e.as_db_error()
		.map(|d| d.code().code().to_string())
		.unwrap_or_else(|| e.to_string())
}

async fn connect_as(address: &str, app: &str) -> Client {
	let (host, port) = address.rsplit_once(':').unwrap();
	let (c, conn) = tokio_postgres::Config::new()
		.host(host)
		.port(port.parse().unwrap())
		.user("lepis_app")
		.password("app-pw")
		.dbname("postgres")
		.application_name(app)
		.connect(NoTls)
		.await
		.unwrap();
	tokio::spawn(conn);
	c
}

#[tokio::test]
async fn transaction_pooling_shares_backends_and_keeps_each_session_its_own() {
	let b = need_bed!();
	setting(&b, "pool_mode", "\"transaction\"").await;
	let lepis = start_lepis(&b).await;

	// A tenant on a node other than home, which neither client has a session to yet.
	let ids: Vec<NodeId> = node_ids(&b).iter().map(|(i, _)| *i).collect();
	let ks = keyspace("tenant", KeyType::Int8, TENANT_SEED, &ids);
	let far = (1..500i64)
		.find(|t| {
			ks.owner_of_hash(
				KeyType::Int8
					.hash_text_value(&t.to_string(), ks.seed)
					.unwrap(),
			) != ids[0]
		})
		.unwrap();
	let there = format!(
		"select pg_backend_pid()::text || ' ' || current_setting('TimeZone') || ' ' || current_setting('application_name') from oracle.tenants where tenant_id = {far}"
	);
	let a = connect_as(&lepis, "app-a").await;
	let bee = connect_as(&lepis, "app-b").await;
	a.batch_execute("set timezone = 'Asia/Tokyo'")
		.await
		.unwrap();
	let a_there = one(&a, &there).await;
	// B is handed the backend A gave back (once A has been idle a moment): it is B's session
	// there, not A's.
	tokio::time::sleep(std::time::Duration::from_millis(200)).await;
	let b_there = one(&bee, &there).await;
	let (a_pid, b_pid) = (
		a_there.split(' ').next().unwrap(),
		b_there.split(' ').next().unwrap(),
	);
	assert_eq!(
		b_pid, a_pid,
		"the backend went back to the pool and was reused"
	);
	assert!(a_there.ends_with("Asia/Tokyo app-a"), "{a_there}");
	assert!(b_there.ends_with("Etc/UTC app-b"), "{b_there}");
	// A's own settings follow it to whatever backend it is given.
	assert!(one(&a, &there).await.ends_with("Asia/Tokyo app-a"));
	assert_eq!(one(&a, "show timezone").await, "Asia/Tokyo");

	// A prepared statement is parsed again where it is missing.
	let stmt = a
		.prepare("select count(*) from oracle.orders where tenant_id = $1")
		.await
		.unwrap();
	for _ in 0..3 {
		let n: i64 = a.query_one(&stmt, &[&1i64]).await.unwrap().get(0);
		assert!(n > 0);
		one(&bee, "select 1").await;
	}
	// A transaction keeps its backend to the end.
	a.batch_execute("begin").await.unwrap();
	let p1 = one(&a, "select pg_backend_pid()").await;
	one(&bee, "select pg_backend_pid()").await;
	let p2 = one(&a, "select pg_backend_pid()").await;
	a.batch_execute("commit").await.unwrap();
	assert_eq!(p1, p2);

	// What would outlive the transaction is refused, with the setting named.
	for sql in [
		"listen somewhere",
		"create temp table t (x int)",
		"prepare p as select 1",
		"select pg_advisory_lock(42)",
	] {
		assert_eq!(code(&a, sql).await, "0A000", "{sql}");
	}
	// … but not what ends with it.
	a.batch_execute(
		"begin; select pg_advisory_xact_lock(42); create temp table t (x int) on commit drop; commit",
	)
	.await
	.unwrap();

	// The oracle corpus through one pooled session answers as one Postgres.
	a.batch_execute("reset timezone").await.unwrap();
	let reference = connect(&b.reference, "lepis_app", "app-pw").await;
	let mut wrong = Vec::new();
	let mut name = String::new();
	let mut ordered = false;
	let mut sql = String::new();
	let mut queries = Vec::new();
	for line in QUERIES.lines().chain(std::iter::once("-- name: end")) {
		if let Some(h) = line.strip_prefix("-- name: ") {
			if !name.is_empty() {
				queries.push((name.clone(), ordered, sql.clone()));
			}
			let mut w = h.split_whitespace();
			name = w.next().unwrap().to_string();
			ordered = w.any(|x| x == "ordered");
			sql.clear();
		} else if !line.starts_with("--") {
			sql.push_str(line);
			sql.push('\n');
		}
	}
	for (name, ordered, sql) in &queries {
		let want = answer(&reference, sql, *ordered).await;
		let got = answer(&a, sql, *ordered).await;
		if got != want && !matches!(&got, Err((c, _)) if c == "0A000") {
			wrong.push(format!("{name}: {want:?} / {got:?}"));
		}
	}
	assert!(wrong.is_empty(), "{wrong:#?}");
	clear_settings(&b).await;
}

#[tokio::test]
async fn a_jwt_claim_routes_the_transaction_to_its_tenants_node() {
	let b = need_bed!();
	setting(&b, "route_claim", "\"tenant_id\"").await;
	setting(&b, "route_claim_keyspace", "\"tenant\"").await;
	let lepis = start_lepis(&b).await;
	let c = connect(&lepis, "lepis_app", "app-pw").await;
	let nodes = node_ids(&b);
	let ids: Vec<NodeId> = nodes.iter().map(|(i, _)| *i).collect();
	let ks = keyspace("tenant", KeyType::Int8, TENANT_SEED, &ids);
	let owner = |t: i64| {
		ks.owner_of_hash(
			KeyType::Int8
				.hash_text_value(&t.to_string(), ks.seed)
				.unwrap(),
		)
	};
	let far = (1..500i64).find(|t| owner(*t) != ids[0]).unwrap();
	let other = (1..500i64).find(|t| owner(*t) != owner(far)).unwrap();
	let (_, address) = nodes.iter().find(|(i, _)| *i == owner(far)).unwrap();
	let on_node: i64 = connect(address, "postgres", "x")
		.await
		.query_one("select count(*) from oracle.orders", &[])
		.await
		.unwrap()
		.get(0);
	let claims = format!("{{\"tenant_id\": {far}, \"role\": \"authenticated\"}}");

	for prepared in [false, true] {
		c.batch_execute("begin").await.unwrap();
		if prepared {
			// As the data API sends it: a parameter, among other set_config calls.
			c.query(
				"select set_config('search_path', 'public', true), set_config('request.jwt.claims', $1, true)",
				&[&claims],
			)
			.await
			.unwrap();
		} else {
			c.batch_execute(&format!(
				"select set_config('request.jwt.claims', '{claims}', true)"
			))
			.await
			.unwrap();
		}
		// No key: the tenant's node alone answers, and it has the claims.
		let row = c
			.query_one(
				"select count(*), current_setting('request.jwt.claims', true) from oracle.orders",
				&[],
			)
			.await
			.unwrap();
		assert_eq!(row.get::<_, i64>(0), on_node, "prepared {prepared}");
		assert_eq!(
			row.get::<_, Option<String>>(1).as_deref(),
			Some(claims.as_str())
		);
		c.query("select * from oracle.orders where tenant_id = $1", &[&far])
			.await
			.unwrap();
		// Another tenant's key is refused.
		let e = c
			.query(
				"select * from oracle.orders where tenant_id = $1",
				&[&other],
			)
			.await
			.unwrap_err();
		assert_eq!(e.as_db_error().unwrap().code().code(), "42501");
		c.batch_execute("rollback").await.unwrap();
	}
	// Without the claim, the same read is across every node again.
	let all: i64 = c
		.query_one("select count(*) from oracle.orders", &[])
		.await
		.unwrap()
		.get(0);
	assert!(all > on_node);
	clear_settings(&b).await;
}
