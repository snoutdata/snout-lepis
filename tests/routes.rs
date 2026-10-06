//! The oracle corpus through the parser adapter and route(), with no database: every query in
//! `oracle/queries.sql` against a three-node catalog of the oracle schema must get the decision
//! its `route=` annotation names. tests/oracle.rs checks the ANSWERS against a real cluster; this
//! checks the DECISIONS, so a routing change shows up here first, in milliseconds. A scatter is
//! judged by what scatter.rs makes of it: planned (`scatter`) or refused (`refuse`).

use std::collections::HashMap;

use lepis::analyze::analyze;
use lepis::catalog::{
	Catalog, Keyspace, Node, NodeId, NodeState, RelationKind, RelationName, Strategy,
};
use lepis::config::SslMode;
use lepis::hash::KeyType;
use lepis::route::{Kind, Route, route};
use lepis::scatter;

const QUERIES: &str = include_str!("../oracle/queries.sql");

/// Annotations the decision without a database does not meet, each with why; the test asserts they STILL do not, so the
/// list cannot go stale. These are reported, not fixed here: the corpus is the spec.
const KNOWN: &[(&str, &str)] = &[(
	"float_sum",
	"refused once the nodes say the argument is double precision (merge.rs); a check without a database cannot know the type",
)];

fn catalog() -> Catalog {
	let ids: Vec<NodeId> = (1..=3).map(NodeId).collect();
	let mut c = Catalog {
		epoch: 1,
		..Default::default()
	};
	for id in &ids {
		c.nodes.insert(
			*id,
			Node {
				id: *id,
				name: format!("n{}", id.0),
				host: "h".into(),
				port: 5432,
				dbname: "postgres".into(),
				sslmode: SslMode::Disable,
				home: id.0 == 1,
				state: NodeState::Active,
				server_version_num: Some(180_000),
			},
		);
	}
	for (name, key_type, seed) in [("tenant", KeyType::Int8, 1), ("device", KeyType::Uuid, 2)] {
		c.keyspaces.insert(
			name.into(),
			Keyspace {
				name: name.into(),
				strategy: Strategy::Hash,
				key_type,
				seed,
				ranges: Keyspace::even_ranges(24, &ids),
				pins: HashMap::new(),
			},
		);
	}
	let rel = |t: &str| RelationName {
		schema: "oracle".into(),
		table: t.into(),
	};
	let sharded = |k: &str, col: &str| RelationKind::Sharded {
		keyspace: k.into(),
		key_column: col.into(),
	};
	for t in ["tenants", "orders", "items"] {
		c.relations.insert(rel(t), sharded("tenant", "tenant_id"));
	}
	c.relations
		.insert(rel("events"), sharded("device", "device"));
	c.relations
		.insert(rel("countries"), RelationKind::Reference);
	c.relations.insert(rel("plans"), RelationKind::Global);
	c.validate().expect("a valid catalog");
	c
}

struct Query {
	name: String,
	route: String,
	sql: String,
}

/// The corpus, in the format its header describes: `-- name: <id> [ordered] [route=<what>]`,
/// then the query's lines up to the next header.
fn corpus() -> Vec<Query> {
	let mut out: Vec<Query> = Vec::new();
	for line in QUERIES.lines() {
		if let Some(header) = line.strip_prefix("-- name: ") {
			let mut words = header.split_whitespace();
			let name = words.next().expect("a name").to_string();
			let route = words
				.find_map(|w| w.strip_prefix("route="))
				.unwrap_or_else(|| panic!("{name}: no route= annotation"))
				.to_string();
			out.push(Query {
				name,
				route,
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

/// The decision for a query, as a word the annotations use (or why it does not fit one).
fn decide(q: &Query, cat: &Catalog) -> Result<String, String> {
	let stmts = match analyze(&q.sql, cat, &["public".to_string()]) {
		Ok(s) => s,
		// Lepis refusing to analyse is a refusal of the statement.
		Err(_) if q.route == "refuse" => return Ok("refuse".into()),
		Err(e) => return Ok(format!("{e:?}")),
	};
	// A transaction (BEGIN; …; ROLLBACK;) is judged by its data statement.
	let data: Vec<_> = stmts
		.iter()
		.filter(|s| !matches!(s.facts.kind, Kind::Transaction | Kind::Session))
		.collect();
	let [s] = data.as_slice() else {
		return Err(format!("{} data statements", data.len()));
	};
	Ok(String::from(match route(&s.facts, cat, &[]) {
		Route::Node(_) => "single",
		Route::Home => "home",
		Route::Scatter(_) => match scatter::check(&s.sql, cat, &["public".to_string()]) {
			Ok(()) => "scatter",
			Err(_) => "refuse",
		},
		Route::Refuse(_) => "refuse",
		Route::SplitInsert(_) => "split",
		Route::Fanout { .. } => "all",
		Route::SplitCopy(_) => "split",
	}))
}

#[test]
fn the_corpus_routes_as_annotated() {
	let cat = catalog();
	let corpus = corpus();
	assert!(
		corpus.len() >= 30,
		"the corpus has {} queries",
		corpus.len()
	);
	let mut wrong = Vec::new();
	for q in &corpus {
		let got = decide(q, &cat).unwrap_or_else(|e| panic!("{}: {e}", q.name));
		let known = KNOWN.iter().find(|(n, _)| *n == q.name);
		match known {
			None if got != q.route => {
				wrong.push(format!("{}: route={} but got {got}", q.name, q.route))
			}
			Some((_, why)) if got == q.route => wrong.push(format!(
				"{}: listed as known ({why}) but now routes as annotated; take it off the list",
				q.name
			)),
			_ => {}
		}
	}
	for (name, _) in KNOWN {
		assert!(
			corpus.iter().any(|q| q.name == *name),
			"{name} is not in the corpus"
		);
	}
	assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
