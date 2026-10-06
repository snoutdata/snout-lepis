//! Phases 1 to 3 against a real three-node cluster: the oracle's data distributed by tenant
//! and by device, each node holding only its own rows behind its fence (L7), and Lepis in front.
//!
//! Needs `LEPIS_IT_HOME`, `LEPIS_IT_DATA_NODES` (comma-separated host:port, two of them) and
//! `LEPIS_ORACLE_REFERENCE`, all with superuser postgres/x; `scripts/it.sh` provides them.
//! Skips without.

#[macro_use]
mod common;

use common::*;

#[tokio::test]
async fn the_oracle_on_three_nodes() {
	let b = need_bed!();
	let lepis = start_lepis(&b).await;
	let reference = connect(&b.reference, "lepis_app", "app-pw").await;

	let mut failures = Vec::new();
	let mut counts: HashMap<&str, usize> = HashMap::new();
	let mut name = String::new();
	let mut ordered = false;
	let mut expect = String::new();
	let mut sql = String::new();
	let mut queries: Vec<(String, bool, String, String)> = Vec::new();
	for line in QUERIES.lines().chain(std::iter::once("-- name: end")) {
		if let Some(h) = line.strip_prefix("-- name: ") {
			if !name.is_empty() {
				queries.push((name.clone(), ordered, expect.clone(), sql.clone()));
			}
			let mut w = h.split_whitespace();
			name = w.next().unwrap().to_string();
			let rest: Vec<&str> = w.collect();
			ordered = rest.contains(&"ordered");
			expect = rest
				.iter()
				.find_map(|x| x.strip_prefix("route="))
				.unwrap_or("single")
				.to_string();
			sql.clear();
		} else if !line.starts_with("--") {
			sql.push_str(line);
			sql.push('\n');
		}
	}

	for (name, ordered, expect, sql) in &queries {
		// A fresh session each, so one refusal inside a transaction cannot leak into the next.
		let cluster = connect(&lepis, "lepis_app", "app-pw").await;
		let want = answer(&reference, sql, *ordered).await;
		let got = answer(&cluster, sql, *ordered).await;
		let verdict = match (expect.as_str(), &got) {
			// A read across nodes answers (Phase 2); only what the corpus marks is refused, with
			// the L9 code. A scatter that is refused is as wrong as a wrong answer.
			("refuse", Err((code, _))) if code == "0A000" => "refused",
			(_, g) if *g == want => "equal",
			_ => "WRONG",
		};
		*counts.entry(verdict).or_default() += 1;
		if verdict == "WRONG" {
			failures.push(format!(
				"{name} ({expect}):\n  reference {want:?}\n  cluster   {got:?}"
			));
		}
	}
	eprintln!("{counts:?}");
	assert!(
		failures.is_empty(),
		"{} wrong:\n{}",
		failures.len(),
		failures.join("\n")
	);
	assert!(
		counts.get("equal").copied().unwrap_or(0) >= 50,
		"{counts:?}"
	);
}

#[tokio::test]
async fn rows_land_on_their_owner_and_fences_hold() {
	let b = need_bed!();
	let lepis = start_lepis(&b).await;
	let c = connect(&lepis, "lepis_app", "app-pw").await;
	let nodes = node_ids(&b);
	let ids: Vec<NodeId> = nodes.iter().map(|(i, _)| *i).collect();
	let ks = keyspace("tenant", KeyType::Int8, TENANT_SEED, &ids);

	for t in 9001..9031i64 {
		c.execute(
			"insert into oracle.tenants (tenant_id, name, country, plan_id, created_at) values ($1, 'new', 'CA', 1, now())",
			&[&t],
		)
		.await
		.unwrap();
	}
	for (id, address) in &nodes {
		let admin = connect(address, "postgres", "x").await;
		let here: Vec<i64> = admin
			.query(
				"select tenant_id from oracle.tenants where tenant_id between 9001 and 9030",
				&[],
			)
			.await
			.unwrap()
			.iter()
			.map(|r| r.get(0))
			.collect();
		for t in &here {
			let h = KeyType::Int8
				.hash_text_value(&t.to_string(), ks.seed)
				.unwrap();
			assert_eq!(ks.owner_of_hash(h), *id, "tenant {t} is on {address}");
		}
		// A row written straight to the wrong node is refused by that node's fence.
		let foreign = (9100..9200i64)
			.find(|t| {
				ks.owner_of_hash(
					KeyType::Int8
						.hash_text_value(&t.to_string(), ks.seed)
						.unwrap(),
				) != *id
			})
			.unwrap();
		let e = admin
			.execute(
				"insert into oracle.tenants values ($1, 'stray', 'CA', 1, now())",
				&[&foreign],
			)
			.await
			.unwrap_err();
		assert_eq!(e.as_db_error().unwrap().code().code(), "23514", "{address}");
		admin
			.execute(
				"delete from oracle.tenants where tenant_id between 9001 and 9030",
				&[],
			)
			.await
			.unwrap();
	}
}

#[tokio::test]
async fn a_transaction_starts_on_its_node_and_takes_in_others() {
	let b = need_bed!();
	let lepis = start_lepis(&b).await;
	let c = connect(&lepis, "lepis_app", "app-pw").await;
	let ids: Vec<NodeId> = node_ids(&b).iter().map(|(i, _)| *i).collect();
	let ks = keyspace("tenant", KeyType::Int8, TENANT_SEED, &ids);
	let owner = |t: i64| {
		ks.owner_of_hash(
			KeyType::Int8
				.hash_text_value(&t.to_string(), ks.seed)
				.unwrap(),
		)
	};
	let a = 1i64;
	let other = (2..500i64).find(|t| owner(*t) != owner(a)).unwrap();
	let same = (2..500i64).find(|t| owner(*t) == owner(a)).unwrap();

	// BEGIN alone is answered by Lepis; the first statement binds the transaction.
	c.batch_execute("begin").await.unwrap();
	let n: i64 = c
		.query_one(
			"select count(*) from oracle.orders where tenant_id = $1",
			&[&a],
		)
		.await
		.unwrap()
		.get(0);
	assert!(n > 0);
	c.query_one(
		"select count(*) from oracle.orders where tenant_id = $1",
		&[&same],
	)
	.await
	.unwrap();
	// Another node joins the transaction (Phase 3), and COMMIT ends it on both.
	c.query_one(
		"select count(*) from oracle.orders where tenant_id = $1",
		&[&other],
	)
	.await
	.unwrap();
	c.batch_execute("commit").await.unwrap();
	// After it, the session is free again.
	c.query_one(
		"select count(*) from oracle.orders where tenant_id = $1",
		&[&other],
	)
	.await
	.unwrap();

	// A write inside a transaction is undone by ROLLBACK on its node.
	c.batch_execute(&format!(
		"begin; update oracle.orders set status = 'zzz' where tenant_id = {a}; rollback;"
	))
	.await
	.unwrap();
	let z: i64 = c
		.query_one(
			"select count(*) from oracle.orders where tenant_id = $1 and status = 'zzz'",
			&[&a],
		)
		.await
		.unwrap()
		.get(0);
	assert_eq!(z, 0);
}

#[tokio::test]
async fn settings_and_prepared_statements_follow_the_session() {
	let b = need_bed!();
	let lepis = start_lepis(&b).await;
	let c = connect(&lepis, "lepis_app", "app-pw").await;
	let ids: Vec<NodeId> = node_ids(&b).iter().map(|(i, _)| *i).collect();
	let ks = keyspace("tenant", KeyType::Int8, TENANT_SEED, &ids);
	let owner = |t: i64| {
		ks.owner_of_hash(
			KeyType::Int8
				.hash_text_value(&t.to_string(), ks.seed)
				.unwrap(),
		)
	};
	// One tenant per node.
	let mut per_node: Vec<i64> = Vec::new();
	for t in 1..500i64 {
		if !per_node.iter().any(|p| owner(*p) == owner(t)) {
			per_node.push(t);
		}
	}
	assert_eq!(per_node.len(), ids.len());

	// A session SET reaches every node, including ones the session opens later.
	c.batch_execute("set timezone = 'Asia/Kolkata'")
		.await
		.unwrap();
	let stmt = c
		.prepare(
			"select current_setting('TimeZone'), count(*) from oracle.orders where tenant_id = $1",
		)
		.await
		.unwrap();
	for t in &per_node {
		let row = c.query_one(&stmt, &[t]).await.unwrap();
		assert_eq!(row.get::<_, String>(0), "Asia/Kolkata", "tenant {t}");
	}
	// A SET undone by ROLLBACK is undone everywhere.
	c.batch_execute("begin; set timezone = 'UTC'; rollback;")
		.await
		.unwrap();
	for t in &per_node {
		let row = c.query_one(&stmt, &[t]).await.unwrap();
		assert_eq!(
			row.get::<_, String>(0),
			"Asia/Kolkata",
			"tenant {t} after rollback"
		);
	}
}
