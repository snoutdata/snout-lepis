//! The shard hash against values a real Postgres returned (`tests/fixtures/hash.tsv`, written
//! by `scripts/hash-check.sh … --fixture`), so `cargo test` checks the port with no database.
//! The script itself is the large check; the line forms are its (`examples/hashcheck.rs`).

use lepis::hash::{KeyType, hash_text};

fn hex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}

#[test]
fn matches_postgres() {
	let fixture = include_str!("fixtures/hash.tsv");
	let mut checked = 0usize;
	let mut types = std::collections::BTreeSet::new();
	for line in fixture
		.lines()
		.filter(|l| !l.is_empty() && !l.starts_with('#'))
	{
		let f: Vec<&str> = line.split('\t').collect();
		assert_eq!(f.len(), 4, "{line}");
		let seed = f[2].parse::<i64>().unwrap() as u64;
		let expected: i64 = f[3].parse().unwrap();
		let got = match f[0] {
			"texthex" => hash_text(&String::from_utf8(hex(f[1])).unwrap(), seed),
			"bpcharhex" => {
				let bytes = hex(f[1]);
				let text = String::from_utf8(bytes.clone()).unwrap();
				let t = KeyType::Bpchar.hash_text_value(&text, seed).unwrap();
				let b = KeyType::Bpchar.hash_binary_value(&bytes, seed).unwrap();
				assert_eq!(t, b, "{line}");
				t
			}
			"numericbin" => KeyType::Numeric
				.hash_binary_value(&hex(f[1]), seed)
				.unwrap_or_else(|e| panic!("{e}")),
			ty => KeyType::from_sql_name(ty)
				.unwrap()
				.hash_text_value(f[1], seed)
				.unwrap_or_else(|e| panic!("{e}")),
		};
		assert_eq!(got, expected, "{line}");
		types.insert(f[0]);
		checked += 1;
	}
	assert!(checked > 1000, "fixture too small: {checked}");
	for t in ["numeric", "numericbin", "bpcharhex"] {
		assert!(types.contains(t), "the fixture has no {t} lines");
	}
}
