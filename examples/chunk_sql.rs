//! Prints the chunk SQL Lepis writes for one table, for trying it on a node by hand.
//! `cargo run -p snout-lepis --example chunk_sql -- public cust id 4`
use lepis::catalog::{Keyspace, NodeId, RelationName, Strategy};
use lepis::hash::KeyType;

fn main() {
	let a: Vec<String> = std::env::args().collect();
	let shell = RelationName {
		schema: a[1].clone(),
		table: a[2].clone(),
	};
	let count: u32 = a[4].parse().expect("a chunk count");
	let mut ks = Keyspace {
		name: "k".into(),
		strategy: Strategy::Hash,
		key_type: KeyType::Int8,
		seed: 0,
		ranges: Keyspace::even_ranges(1, &[NodeId(1)]),
		pins: Default::default(),
		chunks: Vec::new(),
		second: None,
	};
	ks.chunks = lepis::chunk::even_chunks(count).expect("chunks");
	println!("{};", lepis::chunk::shell_sql(&shell));
	for c in &ks.chunks {
		println!(
			"{};",
			lepis::chunk::create_sql(
				&shell,
				&lepis::catalog::KeySql::from(lepis::catalog::quote_ident(&a[3])),
				&ks,
				*c,
				180_000,
			)
			.expect("sql")
		);
		println!("{};", lepis::chunk::attach_sql(&shell, c.number));
	}
	println!("{};", lepis::chunk::sync_sql(&shell));
}
