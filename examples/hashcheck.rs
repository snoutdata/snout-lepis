//! The differential check for the shard hash (L5): reads lines a real Postgres wrote, each
//! `type<TAB>value<TAB>seed<TAB>hash`, recomputes the hash here, and reports every difference.
//! `scripts/hash-check.sh` drives it; run alone it reads stdin.
//!
//! Some types arrive in a form other than their text output, because the transport is lines
//! or because the binary form is what is being checked:
//!
//! - `texthex`: a `text` value's UTF-8 bytes in hex (a random string may hold a tab or a
//!   newline);
//! - `bpcharhex`: a `character` value's bytes as `bpcharsend` gives them, padding included,
//!   checked in both the text and the binary form (they are the same bytes);
//! - `numericbin`: `numeric_send` in hex, checked as a binary Bind parameter. A plain `numeric`
//!   line carries the spelling the value was typed in, not only Postgres's output.

use std::collections::BTreeMap;
use std::io::{self, BufRead, Write};

use lepis::hash::{KeyType, hash_text};

fn main() {
	let stdin = io::stdin();
	let mut seen: BTreeMap<String, (u64, u64)> = BTreeMap::new();
	let mut shown = 0usize;
	let mut bad_lines = 0u64;
	for line in stdin.lock().lines() {
		let line = line.expect("stdin");
		if line.is_empty() {
			continue;
		}
		let mut f = line.split('\t');
		let (Some(ty), Some(value), Some(seed), Some(expected), None) =
			(f.next(), f.next(), f.next(), f.next(), f.next())
		else {
			bad_lines += 1;
			continue;
		};
		let seed = seed.parse::<i64>().expect("seed") as u64;
		let expected: i64 = expected.parse().expect("hash");
		let got = hash(ty, value, seed);
		let entry = seen.entry(ty.to_string()).or_default();
		entry.0 += 1;
		let ok = matches!(got, Ok(h) if h == expected);
		if !ok {
			entry.1 += 1;
			if shown < 20 {
				shown += 1;
				eprintln!("MISMATCH {ty} {value:?} seed={seed} postgres={expected} lepis={got:?}");
			}
		}
	}
	let mut out = io::stdout().lock();
	let mut failed = bad_lines > 0;
	for (ty, (n, bad)) in &seen {
		writeln!(out, "{ty:>12}  {n:>10} values  {bad} different").unwrap();
		failed |= *bad > 0;
	}
	if bad_lines > 0 {
		writeln!(out, "{bad_lines} unreadable lines").unwrap();
	}
	if seen.is_empty() {
		writeln!(out, "no input").unwrap();
		failed = true;
	}
	if failed {
		std::process::exit(1);
	}
}

/// The hash of one line's value, or why it could not be read. The two forms of a `bpcharhex`
/// value must agree, or the line counts as different.
fn hash(ty: &str, value: &str, seed: u64) -> Result<i64, String> {
	match ty {
		"texthex" => {
			let text = String::from_utf8(decode_hex(value)).expect("utf-8 from Postgres");
			Ok(hash_text(&text, seed))
		}
		"bpcharhex" => {
			let bytes = decode_hex(value);
			let text = String::from_utf8(bytes.clone()).expect("utf-8 from Postgres");
			let t = KeyType::Bpchar
				.hash_text_value(&text, seed)
				.map_err(|e| e.to_string())?;
			let b = KeyType::Bpchar
				.hash_binary_value(&bytes, seed)
				.map_err(|e| e.to_string())?;
			if t == b {
				Ok(t)
			} else {
				Err(format!("text {t} but binary {b}"))
			}
		}
		"numericbin" => KeyType::Numeric
			.hash_binary_value(&decode_hex(value), seed)
			.map_err(|e| e.to_string()),
		_ => KeyType::from_sql_name(ty)
			.unwrap_or_else(|| panic!("type {ty}"))
			.hash_text_value(value, seed)
			.map_err(|e| e.to_string()),
	}
}

fn decode_hex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
		.collect()
}
