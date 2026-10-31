//! Read replicas per node, one per node.
//!
//! A node may have one read replica: a streaming standby of it, named in the cluster's settings
//! (`read_replicas`, keyed by node name), never in the catalog. A session that asks for it
//! (`SET lepis.read_from = 'replica'`, or the same in its connection string: `options=-c
//! lepis.read_from=replica`, or a startup parameter of that name) has its reads served there
//! instead of on the node:
//!
//! - **Only a read that cannot write.** A SELECT can: `nextval`, a function that writes, `FOR
//!   UPDATE`, a writable CTE, `SELECT INTO`. `read_only` is an allowlist over the whole parse
//!   tree, so a function Lepis does not know (any user function) keeps the read on the primary.
//!   What gets past it anyway (a view that calls `nextval`) is refused by the replica with
//!   25006, and the router runs the read again on the primary without the client seeing it.
//! - **Only outside a transaction, and only until the session writes.** A session that has
//!   sent anything that may write reads from the primary from then on, so it reads its writes.
//! - **Only a replica that keeps up.** Its replay is compared with the node's WAL position at
//!   most every [`READ_EVERY`] by whichever session asks first (`usable`); one further behind
//!   than its `max_lag_bytes`, one out of recovery (promoted), or one that does not answer is
//!   passed over, and the read goes to the node.
//!
//! The router keeps a replica's session as it keeps a node's, under [`replica_id`]: a NodeId no
//! catalog uses, so prepared statements, SETs and the pool work for it unchanged.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::catalog::{Node, NodeId};
use crate::config::SslMode;
use crate::server::App;

/// How far behind its node a replica may be and still serve reads, by default.
pub const DEFAULT_MAX_LAG_BYTES: u64 = 1 << 20;

/// How old a replica's reading may be before the next session to ask takes it again.
pub const READ_EVERY: Duration = Duration::from_secs(2);

/// How long a reading may wait for the replica or its node.
const ASK_FOR: Duration = Duration::from_secs(2);

/// The SQLSTATE a standby refuses a write with (read_only_sql_transaction).
pub const READ_ONLY: &str = "25006";

/// A replica's NodeId: below every catalog's (they are positive), one per node.
const BASE: i32 = i32::MIN / 2;

pub fn replica_id(primary: NodeId) -> NodeId {
	NodeId(BASE + primary.0)
}

/// The node whose replica `id` is, or None for a node's own id.
pub fn primary_of(id: NodeId) -> Option<NodeId> {
	(id.0 < 0 && id.0 - BASE > 0).then(|| NodeId(id.0 - BASE))
}

/// One node's replica, as the settings name it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Replica {
	pub host: String,
	pub port: u16,
	pub sslmode: SslMode,
	/// The node's own database when not given.
	pub dbname: Option<String>,
	pub max_lag_bytes: u64,
}

impl Replica {
	/// `of`, at the replica's address, under its replica id.
	pub fn node(&self, of: &Node) -> Node {
		Node {
			id: replica_id(of.id),
			name: format!("{}-replica", of.name),
			host: self.host.clone(),
			port: self.port,
			dbname: self.dbname.clone().unwrap_or_else(|| of.dbname.clone()),
			sslmode: self.sslmode,
			home: false,
			state: of.state,
			server_version_num: of.server_version_num,
		}
	}
}

/// The replicas the settings name, by node name; an entry that does not parse is left out
/// (`problem` refuses one at the admin API).
pub fn from_settings(v: Option<&Value>) -> BTreeMap<String, Replica> {
	let Some(o) = v.and_then(Value::as_object) else {
		return BTreeMap::new();
	};
	o.iter()
		.filter_map(|(name, r)| parse(r).ok().map(|r| (name.clone(), r)))
		.collect()
}

fn parse(r: &Value) -> Result<Replica, String> {
	let o = r.as_object().ok_or(
		"a replica is an object: host, port, and optionally sslmode, dbname, max_lag_bytes",
	)?;
	for k in o.keys() {
		if !matches!(
			k.as_str(),
			"host" | "port" | "sslmode" | "dbname" | "max_lag_bytes"
		) {
			return Err(format!("a replica has no setting {k}"));
		}
	}
	let host = o
		.get("host")
		.and_then(Value::as_str)
		.filter(|h| !h.is_empty() && h.len() <= 253 && !h.chars().any(char::is_control))
		.ok_or("a replica's host is a name or address")?;
	let port = o
		.get("port")
		.and_then(Value::as_u64)
		.and_then(|p| u16::try_from(p).ok())
		.filter(|p| *p > 0)
		.ok_or("a replica's port is a number from 1 to 65535")?;
	let sslmode = match o.get("sslmode") {
		None | Some(Value::Null) => SslMode::VerifyFull,
		Some(v) => v
			.as_str()
			.and_then(SslMode::parse)
			.ok_or("a replica's sslmode is disable, require, verify-ca or verify-full")?,
	};
	let dbname = match o.get("dbname") {
		None | Some(Value::Null) => None,
		Some(v) => Some(
			v.as_str()
				.filter(|d| !d.is_empty() && d.len() <= 63)
				.ok_or("a replica's dbname is a database name")?
				.to_string(),
		),
	};
	let max_lag_bytes = match o.get("max_lag_bytes") {
		None | Some(Value::Null) => DEFAULT_MAX_LAG_BYTES,
		// Below 2^53, so the admin API's JSON reads it exactly in JavaScript too.
		Some(v) => v
			.as_u64()
			.filter(|n| *n < 1 << 53)
			.ok_or("a replica's max_lag_bytes is a number of bytes")?,
	};
	Ok(Replica {
		host: host.to_string(),
		port,
		sslmode,
		dbname,
		max_lag_bytes,
	})
}

/// Why `read_replicas` cannot be written, as a sentence; None when it can (null clears it).
pub fn problem(v: &Value) -> Option<String> {
	if v.is_null() {
		return None;
	}
	let Some(o) = v.as_object() else {
		return Some("read_replicas is an object keyed by node name, or null to clear".into());
	};
	for (name, r) in o {
		if name.is_empty() || name.len() > 63 {
			return Some("read_replicas is keyed by node name".into());
		}
		if let Err(e) = parse(r) {
			return Some(format!("{name}: {e}"));
		}
	}
	None
}

/// The functions a read may call and still be served by a replica: built-ins that write
/// nothing, change no session state and take no lock. Anything else, a user function included,
/// keeps the read on the primary.
const READ_ONLY_FUNCTIONS: &[&str] = &[
	// Aggregates.
	"count",
	"sum",
	"avg",
	"min",
	"max",
	"bool_and",
	"bool_or",
	"every",
	"string_agg",
	"array_agg",
	"json_agg",
	"jsonb_agg",
	"json_object_agg",
	"jsonb_object_agg",
	"stddev",
	"stddev_pop",
	"stddev_samp",
	"variance",
	"var_pop",
	"var_samp",
	"percentile_cont",
	"percentile_disc",
	"mode",
	// Window functions.
	"row_number",
	"rank",
	"dense_rank",
	"percent_rank",
	"cume_dist",
	"ntile",
	"lag",
	"lead",
	"first_value",
	"last_value",
	"nth_value",
	// Text.
	"lower",
	"upper",
	"initcap",
	"length",
	"char_length",
	"character_length",
	"octet_length",
	"substr",
	"substring",
	"left",
	"right",
	"trim",
	"btrim",
	"ltrim",
	"rtrim",
	"lpad",
	"rpad",
	"concat",
	"concat_ws",
	"replace",
	"split_part",
	"strpos",
	"position",
	"starts_with",
	"reverse",
	"repeat",
	"format",
	"md5",
	"encode",
	"decode",
	"regexp_replace",
	"regexp_match",
	"regexp_matches",
	"regexp_like",
	"regexp_split_to_array",
	"string_to_array",
	"array_to_string",
	"translate",
	"ascii",
	"chr",
	"quote_ident",
	"quote_literal",
	"to_hex",
	// Numbers.
	"abs",
	"round",
	"trunc",
	"floor",
	"ceil",
	"ceiling",
	"mod",
	"power",
	"sqrt",
	"exp",
	"ln",
	"log",
	"sign",
	"div",
	"width_bucket",
	"random",
	// Time.
	"now",
	"statement_timestamp",
	"transaction_timestamp",
	"clock_timestamp",
	"date_trunc",
	"date_part",
	"extract",
	"date_bin",
	"age",
	"make_date",
	"make_time",
	"make_timestamp",
	"make_timestamptz",
	"make_interval",
	"to_char",
	"to_date",
	"to_timestamp",
	"to_number",
	"timezone",
	"isfinite",
	// JSON.
	"to_json",
	"to_jsonb",
	"json_build_object",
	"jsonb_build_object",
	"json_build_array",
	"jsonb_build_array",
	"jsonb_extract_path",
	"jsonb_extract_path_text",
	"json_extract_path",
	"json_extract_path_text",
	"jsonb_array_length",
	"json_array_length",
	"jsonb_array_elements",
	"jsonb_array_elements_text",
	"json_array_elements",
	"json_array_elements_text",
	"jsonb_each",
	"jsonb_each_text",
	"json_each",
	"json_each_text",
	"jsonb_object_keys",
	"json_object_keys",
	"jsonb_typeof",
	"json_typeof",
	"jsonb_strip_nulls",
	"jsonb_set",
	"jsonb_insert",
	"jsonb_pretty",
	"jsonb_path_query",
	"jsonb_path_query_first",
	"jsonb_path_exists",
	"row_to_json",
	"array_to_json",
	// Arrays and sets.
	"array_length",
	"array_upper",
	"array_lower",
	"cardinality",
	"array_position",
	"array_positions",
	"array_append",
	"array_prepend",
	"array_cat",
	"array_remove",
	"unnest",
	"generate_series",
	"generate_subscripts",
	// Other.
	"coalesce",
	"nullif",
	"greatest",
	"least",
	"pg_is_in_recovery",
	"current_setting",
	"version",
	"gen_random_uuid",
	"pg_sleep",
];

/// Whether `sql` is one read a replica may answer: SELECTs (or VALUES / TABLE) only, nothing
/// that writes or locks anywhere in them, and no function but `READ_ONLY_FUNCTIONS` (bare, or
/// in pg_catalog). The whole parse tree is walked, generically, so a construct added to the
/// parser later is seen too.
pub fn read_only(sql: &str) -> bool {
	let Ok(parsed) = pg_query::parse(sql) else {
		return false;
	};
	if parsed.protobuf.stmts.is_empty() {
		return false;
	}
	for s in &parsed.protobuf.stmts {
		match s.stmt.as_deref().and_then(|n| n.node.as_ref()) {
			Some(pg_query::NodeEnum::SelectStmt(_)) => {}
			_ => return false,
		}
	}
	let Ok(tree) = serde_json::to_value(&parsed.protobuf) else {
		return false;
	};
	walk(&tree)
}

fn walk(v: &Value) -> bool {
	match v {
		Value::Array(a) => a.iter().all(walk),
		Value::Object(o) => o.iter().all(|(k, v)| match k.as_str() {
			"InsertStmt" | "UpdateStmt" | "DeleteStmt" | "MergeStmt" => false,
			"locking_clause" => v.as_array().is_none_or(Vec::is_empty) && walk(v),
			"into_clause" => v.is_null(),
			"FuncCall" => function_ok(v) && walk(v),
			_ => walk(v),
		}),
		_ => true,
	}
}

fn function_ok(call: &Value) -> bool {
	let names: Vec<&str> = call
		.get("funcname")
		.and_then(Value::as_array)
		.map(|parts| {
			parts
				.iter()
				.filter_map(|p| p.pointer("/node/String/sval").and_then(Value::as_str))
				.collect()
		})
		.unwrap_or_default();
	match names.as_slice() {
		[f] | ["pg_catalog", f] => READ_ONLY_FUNCTIONS.contains(f),
		_ => false,
	}
}

/// What the connection asked for before any SET: a startup parameter `lepis.read_from`, or `-c
/// lepis.read_from=<value>` (or `--lepis.read_from=<value>`) in its `options`, the last one winning,
/// as Postgres applies them. None when it asked nothing.
pub fn startup_read_from(startup: &[(String, String)]) -> Option<String> {
	let mut got = None;
	for (k, v) in startup {
		if k == "lepis.read_from" {
			got = Some(v.clone());
		} else if k == "options" {
			// libpq's word splitting: whitespace separates, a backslash keeps the next character.
			let mut words: Vec<String> = Vec::new();
			let mut word = String::new();
			let mut chars = v.chars();
			while let Some(c) = chars.next() {
				if c == '\\' {
					if let Some(n) = chars.next() {
						word.push(n);
					}
				} else if c.is_whitespace() {
					if !word.is_empty() {
						words.push(std::mem::take(&mut word));
					}
				} else {
					word.push(c);
				}
			}
			if !word.is_empty() {
				words.push(word);
			}
			let mut i = 0;
			while i < words.len() {
				let w = &words[i];
				let set = if w == "-c" {
					i += 1;
					words.get(i).map(String::as_str)
				} else {
					w.strip_prefix("-c").or_else(|| w.strip_prefix("--"))
				};
				if let Some((name, value)) = set.and_then(|s| s.split_once('=')) {
					if name == "lepis.read_from" {
						got = Some(value.to_string());
					}
				}
				i += 1;
			}
		}
	}
	got
}

/// Whether the session asks for its reads on replicas: what its connection asked for, then its
/// SETs, the last `lepis.read_from` winning; `RESET`, `DEFAULT` and `RESET ALL` go back to the
/// connection's, as they do in Postgres.
pub fn wants_replica(startup: &[(String, String)], sets: &[String]) -> bool {
	use pg_query::protobuf::VariableSetKind as K;
	let initial = startup_read_from(startup).as_deref() == Some("replica");
	let mut want = initial;
	for sql in sets {
		let Ok(parsed) = pg_query::parse(sql) else {
			continue;
		};
		for s in &parsed.protobuf.stmts {
			let Some(pg_query::NodeEnum::VariableSetStmt(v)) =
				s.stmt.as_deref().and_then(|n| n.node.as_ref())
			else {
				continue;
			};
			let kind = K::try_from(v.kind).unwrap_or(K::Undefined);
			if kind == K::VarResetAll {
				want = initial;
				continue;
			}
			if v.name != "lepis.read_from" {
				continue;
			}
			if matches!(kind, K::VarSetDefault | K::VarReset) {
				want = initial;
				continue;
			}
			let value = v.args.first().and_then(|a| match a.node.as_ref() {
				Some(pg_query::NodeEnum::AConst(c)) => match &c.val {
					Some(pg_query::protobuf::a_const::Val::Sval(s)) => Some(s.sval.clone()),
					_ => None,
				},
				_ => None,
			});
			want = value.as_deref() == Some("replica");
		}
	}
	want
}

// ---------------------------------------------------------------------------------------------
// Whether a replica keeps up.

struct Entry {
	usable: bool,
	read_at: Instant,
	reading: bool,
}

fn cache() -> &'static Mutex<HashMap<String, Entry>> {
	static CACHE: OnceLock<Mutex<HashMap<String, Entry>>> = OnceLock::new();
	CACHE.get_or_init(Default::default)
}

fn key(primary: &Node, replica: &Node) -> String {
	format!(
		"{}:{}/{} <- {}:{}/{}",
		primary.host, primary.port, primary.dbname, replica.host, replica.port, replica.dbname
	)
}

/// Whether `replica` may serve `primary`'s reads now. A reading is shared by every session of
/// the process for [`READ_EVERY`]; then the first to ask takes it again while the others go on
/// with the one they have. Never read yet and being read: not usable.
pub async fn usable(app: &Arc<App>, primary: &Node, replica: &Node, max_lag_bytes: u64) -> bool {
	let k = key(primary, replica);
	{
		let mut c = cache().lock().expect("replicas");
		match c.get_mut(&k) {
			Some(e)
				if e.read_at.elapsed() < READ_EVERY
					|| (e.reading && e.read_at.elapsed() < Duration::from_secs(10)) =>
			{
				return e.usable;
			}
			Some(e) => e.reading = true,
			None => {
				c.insert(
					k.clone(),
					Entry {
						usable: false,
						read_at: Instant::now(),
						reading: true,
					},
				);
			}
		}
	}
	let ok = tokio::time::timeout(ASK_FOR * 2, keeps_up(app, primary, replica, max_lag_bytes))
		.await
		.unwrap_or(false);
	cache().lock().expect("replicas").insert(
		k,
		Entry {
			usable: ok,
			read_at: Instant::now(),
			reading: false,
		},
	);
	ok
}

/// A replica a session could not reach is passed over until the next reading.
pub fn mark_down(primary: &Node, replica: &Node) {
	cache().lock().expect("replicas").insert(
		key(primary, replica),
		Entry {
			usable: false,
			read_at: Instant::now(),
			reading: false,
		},
	);
}

async fn keeps_up(app: &Arc<App>, primary: &Node, replica: &Node, max_lag_bytes: u64) -> bool {
	let Ok(Ok(mut r)) = tokio::time::timeout(
		ASK_FOR,
		crate::twopc::node_service(app, replica, "read replica"),
	)
	.await
	else {
		return false;
	};
	let at = r
		.query(
			"select pg_is_in_recovery()::text, pg_last_wal_replay_lsn()::text",
			&[],
		)
		.await;
	r.close().await;
	let lsn = match at.ok().and_then(|rows| rows.into_iter().next()) {
		Some(row) if row.first().cloned().flatten().as_deref() == Some("true") => {
			match row.get(1).cloned().flatten() {
				Some(lsn) => lsn,
				None => return false,
			}
		}
		_ => return false,
	};
	let Ok(Ok(mut p)) = tokio::time::timeout(
		ASK_FOR,
		crate::twopc::node_service(app, primary, "read replica lag"),
	)
	.await
	else {
		return false;
	};
	let behind = p
		.query(
			"select greatest(pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn), 0)::bigint::text",
			&[&lsn],
		)
		.await;
	p.close().await;
	let behind: Option<u64> = behind
		.ok()
		.and_then(|rows| rows.into_iter().next())
		.and_then(|row| row.into_iter().next().flatten())
		.and_then(|b| b.parse().ok());
	let ok = behind.is_some_and(|b| b <= max_lag_bytes);
	if !ok {
		tracing::info!(replica = %replica.host, behind = ?behind, "a read replica is passed over");
	}
	ok
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn ids_round_trip_and_never_meet_a_catalog_id() {
		for n in [1, 2, 3, 99, 1_000_000] {
			let r = replica_id(NodeId(n));
			assert!(r.0 < 0);
			assert_eq!(primary_of(r), Some(NodeId(n)));
			assert_eq!(primary_of(NodeId(n)), None);
		}
	}

	#[test]
	fn reads_that_cannot_write_and_the_ones_that_can() {
		for ok in [
			"select 1",
			"select pg_is_in_recovery()",
			"select count(*), max(v) from t where id = 3 group by g order by 1 limit 5",
			"select * from a join b using (id) where a.x in (select x from c)",
			"with x as (select * from t) select lower(name) from x",
			"select case when a > 1 then coalesce(b, 0) else abs(c) end from t",
			"select row_number() over (order by id) from t",
			"values (1), (2)",
			"table t",
			"select pg_catalog.lower('A')",
			"select * from generate_series(1, 3)",
		] {
			assert!(read_only(ok), "{ok}");
		}
		for no in [
			"select nextval('s')",
			"select setval('s', 3)",
			"select my_function(id) from t",
			"select public.lower('A')",
			"select * from t for update",
			"select * from t for share",
			"select * from t where id in (select id from u for update)",
			"with x as (update t set v = 1 returning *) select * from x",
			"with x as (insert into t values (1) returning *) select * from x",
			"with x as (delete from t returning *) select * from x",
			"select * into t2 from t",
			"select set_config('a.b', 'c', false)",
			"select pg_advisory_lock(1)",
			"select txid_current()",
			"select array[nextval('s')]",
			"select (select nextval('s'))",
			"select count(*) filter (where nextval('s') > 0) from t",
			"insert into t values (1)",
			"update t set v = 1",
			"explain select 1",
			"show work_mem",
			"select 1; insert into t values (1)",
			"",
			"not sql",
		] {
			assert!(!read_only(no), "{no}");
		}
	}

	#[test]
	fn the_last_read_from_wins() {
		let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
		let wants_replica = |sets: &[String]| wants_replica(&[], sets);
		assert!(!wants_replica(&[]));
		assert!(wants_replica(&s(&["set lepis.read_from = 'replica'"])));
		assert!(wants_replica(&s(&["SET lepis.read_from TO replica"])));
		assert!(!wants_replica(&s(&[
			"set lepis.read_from = 'replica'",
			"set lepis.read_from = 'primary'"
		])));
		assert!(!wants_replica(&s(&[
			"set lepis.read_from = 'replica'",
			"reset lepis.read_from"
		])));
		assert!(!wants_replica(&s(&[
			"set lepis.read_from = 'replica'",
			"set lepis.read_from to default"
		])));
		assert!(wants_replica(&s(&[
			"set work_mem = '4MB'",
			"set lepis.read_from = 'replica'"
		])));
	}

	#[test]
	fn the_connection_string_may_ask() {
		let p = |v: &[(&str, &str)]| {
			v.iter()
				.map(|(k, v)| (k.to_string(), v.to_string()))
				.collect::<Vec<_>>()
		};
		let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
		let asked = p(&[("user", "u"), ("options", "-c lepis.read_from=replica")]);
		assert!(wants_replica(&asked, &[]));
		assert!(wants_replica(
			&p(&[("options", "-clepis.read_from=replica")]),
			&[]
		));
		assert!(wants_replica(
			&p(&[("options", "--lepis.read_from=replica")]),
			&[]
		));
		assert!(wants_replica(
			&p(&[("options", "-c work_mem=4MB  -c lepis.read_from=replica")]),
			&[]
		));
		assert!(wants_replica(&p(&[("lepis.read_from", "replica")]), &[]));
		assert!(!wants_replica(
			&p(&[(
				"options",
				"-c lepis.read_from=replica -c lepis.read_from=primary"
			)]),
			&[]
		));
		assert!(!wants_replica(
			&p(&[("options", "-c lepis.read_fromx=replica")]),
			&[]
		));
		assert!(!wants_replica(
			&p(&[("application_name", "lepis.read_from=replica")]),
			&[]
		));
		// A SET overrides it; RESET, DEFAULT and RESET ALL go back to it, as Postgres does.
		assert!(!wants_replica(
			&asked,
			&s(&["set lepis.read_from = 'primary'"])
		));
		assert!(wants_replica(
			&asked,
			&s(&["set lepis.read_from = 'primary'", "reset lepis.read_from"])
		));
		assert!(wants_replica(
			&asked,
			&s(&[
				"set lepis.read_from = 'primary'",
				"set lepis.read_from to default"
			])
		));
		assert!(wants_replica(
			&asked,
			&s(&["set lepis.read_from = 'primary'", "reset all"])
		));
		assert!(!wants_replica(
			&[],
			&s(&["set lepis.read_from = 'replica'", "reset all"])
		));
	}

	#[test]
	fn settings_parse_and_refuse() {
		let v: Value =
			serde_json::json!({"n2": {"host": "10.0.0.5", "port": 5432, "sslmode": "disable"}});
		let r = from_settings(Some(&v));
		assert_eq!(
			r["n2"],
			Replica {
				host: "10.0.0.5".into(),
				port: 5432,
				sslmode: SslMode::Disable,
				dbname: None,
				max_lag_bytes: DEFAULT_MAX_LAG_BYTES
			}
		);
		assert_eq!(problem(&v), None);
		assert_eq!(problem(&Value::Null), None);
		for bad in [
			serde_json::json!("n2"),
			serde_json::json!({"n2": {"port": 5432}}),
			serde_json::json!({"n2": {"host": "h", "port": 0}}),
			serde_json::json!({"n2": {"host": "h", "port": 5432, "sslmode": "maybe"}}),
			serde_json::json!({"n2": {"host": "h", "port": 5432, "extra": 1}}),
			serde_json::json!({"n2": {"host": "h", "port": 5432, "max_lag_bytes": -1}}),
		] {
			assert!(problem(&bad).is_some(), "{bad}");
		}
	}
}
