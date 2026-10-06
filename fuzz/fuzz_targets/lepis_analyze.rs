//! snout-lepis: key extraction (lepis/src/analyze.rs) and the routing decision made from it
//! (lepis/src/route.rs), over any SQL a client sends, against a fixed three-node catalog with
//! colocated, uuid-keyed, reference and global tables and a pinned tenant. Every statement is
//! analysed or refused, and every analysed one is routed or refused, without a panic.
//!
//! It runs on a thread with a 2 MiB stack, the size of a tokio worker's, so a statement nested
//! deeply enough to overflow the router's stack overflows here too.
//!
//! An input that starts with 0xff is not SQL: each following byte picks a token from a list of
//! the words the analyser looks for, which reaches its deeper branches far sooner than random
//! text does.
#![no_main]
use std::sync::OnceLock;

use lepis::analyze::analyze;
use lepis::catalog::Catalog;
use lepis::route::{ParamValue, route};
use libfuzzer_sys::fuzz_target;

fn row(cells: &[&str]) -> Vec<Option<String>> {
	cells
		.iter()
		.map(|c| (*c != "NULL").then(|| (*c).to_string()))
		.collect()
}

fn catalog() -> &'static Catalog {
	static C: OnceLock<Catalog> = OnceLock::new();
	C.get_or_init(|| {
		let nodes = [
			row(&["1", "n1", "home", "5432", "app", "disable", "home", "active", "180000"]),
			row(&["2", "n2", "node2", "5432", "app", "disable", "data", "active", "180000"]),
			row(&["3", "n3", "node3", "5432", "app", "disable", "data", "active", "130000"]),
		];
		let keyspaces = [
			row(&["tenant", "hash", "bigint", "132424935"]),
			row(&["device", "hash", "uuid", "14578126"]),
			row(&["name", "hash", "text", "0"]),
		];
		let mut ranges = Vec::new();
		for ks in ["tenant", "device", "name"] {
			let bounds = [
				(i64::MIN, -3_074_457_345_618_258_603),
				(-3_074_457_345_618_258_602, 3_074_457_345_618_258_601),
				(3_074_457_345_618_258_602, i64::MAX),
			];
			for (i, (lo, hi)) in bounds.iter().enumerate() {
				ranges.push(row(&[ks, &lo.to_string(), &hi.to_string(), &(i + 1).to_string()]));
			}
		}
		let pins = [row(&["tenant", "42", "3"])];
		let mut relations = Vec::new();
		for schema in ["public", "oracle"] {
			for (table, ks, column) in [
				("tenants", "tenant", "tenant_id"),
				("orders", "tenant", "tenant_id"),
				("items", "tenant", "tenant_id"),
				("events", "device", "device"),
				("users", "name", "name"),
			] {
				relations.push(row(&[schema, table, "sharded", ks, column]));
			}
			relations.push(row(&[schema, "countries", "reference", "NULL", "NULL"]));
			relations.push(row(&[schema, "plans", "global", "NULL", "NULL"]));
		}
		Catalog::from_rows(7, &nodes, &keyspaces, &ranges, &pins, &relations)
			.expect("the fuzz catalog is valid")
	})
}

/// What the analyser looks for: statement shapes, the tables and key columns above, joins,
/// predicates, literals and parameters of every key type.
const TOKENS: &[&str] = &[
	"select", "insert into", "update", "delete from", "with", "as", "from", "where", "and", "or",
	"not", "join", "left join", "right join", "full join", "cross join", "lateral", "on", "using",
	"(", ")", ",", ";", "=", "<>", "<", "in", "is null", "is not null", "between", "exists",
	"any", "all", "union", "union all", "intersect", "except", "values", "set", "returning",
	"order by", "group by", "having", "limit", "offset", "distinct", "for update", "*", "count(*)",
	"sum(", "begin", "commit", "rollback", "savepoint s", "prepare", "execute", "set local",
	"set search_path to", "reset all", "discard all", "copy", "to stdout", "from stdin",
	"create table", "alter table", "drop table", "truncate", "explain", "listen x", "notify x",
	"tenants", "orders", "items", "events", "users", "countries", "plans", "public.orders",
	"oracle.tenants", "pg_class", "t", "o", "i", "t.tenant_id", "o.tenant_id", "i.tenant_id",
	"tenant_id", "device", "name", "order_id", "code", "id", "1", "42", "-9223372036854775808",
	"9223372036854775807", "99999999999999999999", "'42'", "'x'", "null", "true", "$1", "$2",
	"$1::bigint", "::int2", "::text", "'7d8e2a3c-6f0b-4b7e-9a51-1c2d3e4f5a6b'", "::uuid",
	"array[1, 2]", "row(1, 2)", "case when", "then", "else", "end", "coalesce(", "now()",
	"current_setting('x')", "nextval('s')", "default", "on conflict do nothing",
	"on conflict (tenant_id) do update set", "excluded.tenant_id", "into", "select 1",
	"recursive", "materialized", "tablesample system (1)", "only", "where current of c",
];

fn source(data: &[u8]) -> Option<String> {
	match data.split_first() {
		Some((0xff, rest)) => Some(
			rest.iter()
				.map(|b| TOKENS[*b as usize % TOKENS.len()])
				.collect::<Vec<_>>()
				.join(" "),
		),
		_ => std::str::from_utf8(data).ok().map(str::to_string),
	}
}

fuzz_target!(|data: &[u8]| {
	let Some(sql) = source(data) else { return };
	let worker = std::thread::Builder::new()
		.stack_size(2 * 1024 * 1024)
		.spawn(move || {
			let catalog = catalog();
			let search_path = ["public".to_string()];
			let Ok(statements) = analyze(&sql, catalog, &search_path) else {
				return;
			};
			let params = [
				ParamValue::Text("42".into()),
				ParamValue::Binary(7i64.to_be_bytes().to_vec()),
			];
			for s in &statements {
				let _ = route(&s.facts, catalog, &[]);
				let _ = route(&s.facts, catalog, &params);
			}
		})
		.expect("a thread");
	if let Err(panic) = worker.join() {
		std::panic::resume_unwind(panic);
	}
});
