//! Shared by every `tests/cluster*.rs` file: the three-node bed `scripts/it.sh` starts, the
//! oracle data distributed over it with fences and a catalog, and an in-process Lepis in front.
//! Each test binary distributes once (it drops and recreates `oracle` and `lepis`), so files
//! never depend on each other's state.
#![allow(dead_code)]

pub use std::collections::HashMap;
use std::sync::OnceLock;

pub use lepis::catalog::{self, Keyspace, NodeId, RelationKind, RelationName};
use lepis::config::Config;
pub use lepis::hash::KeyType;
use lepis::server::{self, App};
use tokio::sync::Mutex;
pub use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

pub const SCHEMA: &str = include_str!("../../oracle/schema.sql");
pub const DATA: &str = include_str!("../../oracle/data.sql");
pub const QUERIES: &str = include_str!("../../oracle/queries.sql");
pub const RANGES_PER_NODE: usize = 4;
pub const TENANT_SEED: u64 = 0x07e4_a4e7;
pub const DEVICE_SEED: u64 = 0x00de_71ce;

pub fn env(k: &str) -> Option<String> {
	std::env::var(k).ok().filter(|v| !v.is_empty())
}

pub struct Bed {
	pub home: String,
	pub data: Vec<String>,
	pub reference: String,
}

pub fn bed() -> Option<Bed> {
	Some(Bed {
		home: env("LEPIS_IT_HOME")?,
		data: env("LEPIS_IT_DATA_NODES")?
			.split(',')
			.map(str::to_string)
			.collect(),
		reference: env("LEPIS_ORACLE_REFERENCE")?,
	})
}

pub async fn connect(host_port: &str, user: &str, password: &str) -> Client {
	let (host, port) = host_port.rsplit_once(':').expect("host:port");
	let (client, conn) = tokio_postgres::Config::new()
		.host(host)
		.port(port.parse().expect("port"))
		.user(user)
		.password(password)
		.dbname("postgres")
		.application_name("lepis-cluster")
		.connect(NoTls)
		.await
		.unwrap_or_else(|e| panic!("{user}@{host_port}: {e}"));
	tokio::spawn(conn);
	client
}

pub fn node_ids(b: &Bed) -> Vec<(NodeId, String)> {
	std::iter::once(&b.home)
		.chain(b.data.iter())
		.enumerate()
		.map(|(i, a)| (NodeId(i as i32 + 1), a.clone()))
		.collect()
}

pub fn keyspace(name: &str, key_type: KeyType, seed: u64, nodes: &[NodeId]) -> Keyspace {
	Keyspace {
		name: name.into(),
		strategy: catalog::Strategy::Hash,
		key_type,
		seed,
		ranges: Keyspace::even_ranges(nodes.len() * RANGES_PER_NODE, nodes),
		pins: HashMap::new(),
		chunks: Vec::new(),
		second: None,
	}
}

/// The sharded tables, children first (the order rows are deleted in).
pub const SHARDED: [(&str, &str, &str); 4] = [
	("items", "tenant", "tenant_id"),
	("orders", "tenant", "tenant_id"),
	("tenants", "tenant", "tenant_id"),
	("events", "device", "device"),
];

/// Distributes the oracle data once per test run: the same rows everywhere, then each node keeps
/// only what it owns and is fenced; the reference gets everything.
pub async fn distribute(b: &Bed) {
	static DONE: OnceLock<Mutex<bool>> = OnceLock::new();
	let mut done = DONE.get_or_init(|| Mutex::new(false)).lock().await;
	if *done {
		return;
	}
	let nodes = node_ids(b);
	let ids: Vec<NodeId> = nodes.iter().map(|(i, _)| *i).collect();
	let mut keyspaces = [
		keyspace("tenant", KeyType::Int8, TENANT_SEED, &ids),
		keyspace("device", KeyType::Uuid, DEVICE_SEED, &ids),
	];
	// LEPIS_IT_CHUNKS=<n>: the sharded tables as shells and chunk tables, n
	// chunks per keyspace, so the same corpus runs through chunked storage.
	let chunks: Option<u32> = std::env::var("LEPIS_IT_CHUNKS")
		.ok()
		.and_then(|v| v.parse().ok());
	if let Some(n) = chunks {
		for k in keyspaces.iter_mut() {
			k.chunks = lepis::chunk::chunks_for(n, k).expect("chunks");
		}
	}

	// The application role, with ONE verifier copied everywhere (L11; role sync is Phase 3).
	let home_admin = connect(&b.home, "postgres", "x").await;
	home_admin
		.batch_execute("drop schema if exists oracle cascade; drop schema if exists lepis cascade;")
		.await
		.unwrap();
	home_admin
		.batch_execute(
			"do $$ begin
				if not exists (select from pg_roles where rolname = 'lepis_app') then
					create role lepis_app login password 'app-pw';
				end if;
			end $$;",
		)
		.await
		.unwrap();
	let verifier: String = home_admin
		.query_one(
			"select rolpassword from pg_authid where rolname = 'lepis_app'",
			&[],
		)
		.await
		.unwrap()
		.get(0);

	for target in std::iter::once(&b.reference).chain(nodes.iter().map(|(_, a)| a)) {
		let admin = connect(target, "postgres", "x").await;
		admin
			.batch_execute(&format!(
				"do $$ begin
					if exists (select from pg_roles where rolname = 'lepis_app') then
						alter role lepis_app password {v};
					else
						create role lepis_app login password {v};
					end if;
				end $$;",
				v = catalog::quote_literal(&verifier)
			))
			.await
			.unwrap();
		admin.batch_execute(SCHEMA).await.expect("schema");
		admin.batch_execute(DATA).await.expect("data");
		admin
			.batch_execute(
				"grant usage on schema oracle to lepis_app;
				grant select, insert, update, delete on all tables in schema oracle to lepis_app;",
			)
			.await
			.unwrap();
	}

	// Each node keeps its own rows, deleted with the same expression its fence checks.
	for (id, address) in &nodes {
		let admin = connect(address, "postgres", "x").await;
		let version: i32 = admin
			.query_one("select current_setting('server_version_num')::int", &[])
			.await
			.unwrap()
			.get(0);
		if chunks.is_some() {
			// Chunked: each owned chunk a table filled from the heap, then the heaps emptied
			// together (their foreign keys), made shells, the chunks attached, synced.
			let mut sql = String::from("begin;\n");
			let mut rels = Vec::new();
			for (table, ks, column) in SHARDED.iter().rev() {
				let k = keyspaces.iter().find(|k| k.name == *ks).unwrap();
				let rel = RelationName {
					schema: "oracle".into(),
					table: table.to_string(),
				};
				for c in &k.chunks {
					if k.owner_of_hash(c.lo) != *id {
						continue;
					}
					sql.push_str(
						&lepis::chunk::create_sql(
							&rel,
							&lepis::catalog::KeySql::from(lepis::catalog::quote_ident(column)),
							k,
							*c,
							version as u32,
						)
						.unwrap(),
					);
					sql.push_str(";\n");
				}
				rels.push(rel);
			}
			for rel in &rels {
				sql.push_str(&lepis::chunk::sync_sql(rel));
				sql.push_str(";\n");
			}
			for (table, ks, column) in SHARDED.iter().rev() {
				let k = keyspaces.iter().find(|k| k.name == *ks).unwrap();
				for c in &k.chunks {
					if k.owner_of_hash(c.lo) != *id {
						continue;
					}
					let fence = lepis::chunk::chunk_fence(
						&lepis::catalog::KeySql::from(lepis::catalog::quote_ident(column)),
						k,
						*c,
						version as u32,
					)
					.unwrap();
					sql.push_str(&format!(
						"insert into oracle.{t} select * from only oracle.{table} where {fence};\n",
						t = lepis::chunk::chunk_table(table, c.number)
					));
				}
			}
			sql.push_str(&format!(
				"truncate only {};\n",
				rels.iter()
					.map(|r| format!("oracle.{}", r.table))
					.collect::<Vec<_>>()
					.join(", ")
			));
			for (table, ks, _) in SHARDED.iter().rev() {
				let k = keyspaces.iter().find(|k| k.name == *ks).unwrap();
				let rel = RelationName {
					schema: "oracle".into(),
					table: table.to_string(),
				};
				sql.push_str(&lepis::chunk::shell_sql(&rel));
				sql.push_str(";\n");
				for c in &k.chunks {
					if k.owner_of_hash(c.lo) == *id {
						sql.push_str(&lepis::chunk::attach_sql(&rel, c.number));
						sql.push_str(";\n");
					}
				}
			}
			sql.push_str(
				"grant select, insert, update, delete on all tables in schema oracle to lepis_app;\ncommit;",
			);
			admin
				.batch_execute(&sql)
				.await
				.unwrap_or_else(|e| panic!("chunking on {address}: {e}"));
			continue;
		}
		for (table, ks, column) in SHARDED {
			let k = keyspaces.iter().find(|k| k.name == ks).unwrap();
			let rel = RelationName {
				schema: "oracle".into(),
				table: table.into(),
			};
			let fence = catalog::fence_sql(&rel, column, k, *id, version as u32).unwrap();
			let check = fence
				.split(" check (")
				.nth(1)
				.and_then(|s| s.strip_suffix(") not valid"))
				.unwrap()
				.to_string();
			admin
				.batch_execute(&format!(
					"delete from oracle.{table} where not ({check}); {fence};"
				))
				.await
				.unwrap_or_else(|e| panic!("{table} on {address}: {e}"));
		}
	}

	// The catalog, on the home node.
	home_admin
		.batch_execute(catalog::CATALOG_SQL)
		.await
		.unwrap();
	let mut sql = String::new();
	for (id, address) in &nodes {
		let (host, port) = address.rsplit_once(':').unwrap();
		sql.push_str(&format!(
			"insert into lepis.node (id, name, host, port, dbname, sslmode, kind, state) \
			values ({}, 'n{}', {}, {port}, 'postgres', 'disable', '{}', 'active');\n",
			id.0,
			id.0,
			catalog::quote_literal(host),
			if id.0 == 1 { "home" } else { "data" }
		));
	}
	for k in &keyspaces {
		sql.push_str(&format!(
			"insert into lepis.keyspace values ('{}', 'hash', '{}', {});\n",
			k.name,
			k.key_type.sql_name(),
			k.seed as i64
		));
		for r in &k.ranges {
			sql.push_str(&format!(
				"insert into lepis.range values ('{}', {}, {}, {});\n",
				k.name, r.lo, r.hi, r.node.0
			));
		}
		for c in &k.chunks {
			sql.push_str(&format!(
				"insert into lepis.chunk values ('{}', {}, {}, {});\n",
				k.name, c.number, c.lo, c.hi
			));
		}
	}
	for (table, ks, column) in SHARDED {
		sql.push_str(&format!(
			"insert into lepis.relation values ('oracle', '{table}', 'sharded', '{ks}', '{column}', {});\n",
			chunks.is_some()
		));
	}
	sql.push_str(
		"insert into lepis.relation values ('oracle', 'countries', 'reference', null, null);\n",
	);
	sql.push_str("insert into lepis.relation values ('oracle', 'plans', 'global', null, null);\n");
	sql.push_str("select lepis.bump();\n");
	home_admin.batch_execute(&sql).await.expect("catalog");
	*done = true;
	let _ = RelationKind::Global;
}

pub async fn start_lepis(b: &Bed) -> String {
	start_lepis_with(b, &[]).await
}

/// A router with more of its settings named.
#[allow(dead_code)]
pub async fn start_lepis_with(b: &Bed, more: &[(&str, &str)]) -> String {
	let mut config: HashMap<String, String> = [
		("LEPIS_HOME", b.home.clone()),
		("LEPIS_HOME_SSLMODE", "disable".into()),
		("LEPIS_SERVICE_USER", "postgres".into()),
		("LEPIS_SERVICE_PASSWORD", "x".into()),
	]
	.into_iter()
	.map(|(k, v)| (k.to_string(), v))
	.collect();
	for (k, v) in more {
		config.insert(k.to_string(), v.to_string());
	}
	let app = App::new(Config::from_map(&config).expect("config")).expect("app");
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
	let address = listener.local_addr().unwrap().to_string();
	tokio::spawn(server::serve(app, listener));
	// serve() loads the catalog before accepting; give it a moment.
	tokio::time::sleep(std::time::Duration::from_millis(300)).await;
	address
}

#[allow(unused_macros)]
macro_rules! need_bed {
	() => {{
		let Some(b) = bed() else {
			eprintln!("cluster env not set; skipped");
			return;
		};
		distribute(&b).await;
		b
	}};
}

pub type Answer = Result<Vec<Vec<Option<String>>>, (String, String)>;

pub async fn answer(c: &Client, sql: &str, ordered: bool) -> Answer {
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
			.map(|d| (d.code().code().to_string(), d.message().to_string()))
			.unwrap_or_else(|| (String::new(), e.to_string()))),
	}
}
