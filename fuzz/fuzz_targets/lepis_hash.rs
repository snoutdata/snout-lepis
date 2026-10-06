//! snout-lepis: reading a shard key value (lepis/src/hash.rs). A key arrives as text (a literal
//! in a statement, a text-format Bind parameter, a COPY field) or as binary (a binary Bind
//! parameter); either is hashed or refused with a KeyError, never a panic. Integer keys hash the
//! same at every width, so a text value one integer type accepts hashes the same as the wider
//! types read it; a text key hashes the same from its text and from its bytes.
#![no_main]
use lepis::hash::KeyType;
use libfuzzer_sys::fuzz_target;

const SEEDS: [u64; 3] = [0, 0x07e4_a4e7, u64::MAX];

fuzz_target!(|data: &[u8]| {
	for seed in SEEDS {
		for t in KeyType::ALL {
			let _ = t.hash_binary_value(data, seed);
		}
	}
	let Ok(text) = std::str::from_utf8(data) else { return };
	let _ = KeyType::from_sql_name(text);
	for seed in SEEDS {
		for t in KeyType::ALL {
			let _ = t.hash_text_value(text, seed);
		}
		let widths = [KeyType::Int2, KeyType::Int4, KeyType::Int8];
		for (i, narrow) in widths.iter().enumerate() {
			if let Ok(h) = narrow.hash_text_value(text, seed) {
				for wide in &widths[i..] {
					assert_eq!(wide.hash_text_value(text, seed), Ok(h), "{text:?} as {wide:?}");
				}
			}
		}
		assert_eq!(
			KeyType::Text.hash_text_value(text, seed),
			KeyType::Text.hash_binary_value(data, seed)
		);
	}
});
