//! snout-lepis: splitting COPY … FROM STDIN data by row (lepis/src/route.rs, `copy`), in each of
//! the three formats, over any bytes a client sends, cut into CopyData messages anywhere. Every
//! input is split or refused without a panic, and what is split is the input itself: the pieces,
//! put back together, are the bytes the splitter consumed, in order, so no node is sent a byte
//! the client did not send or misses one it did.
//!
//! The first byte picks the format and options, the second the key's column, the third where
//! the data is cut; the rest is the data.
#![no_main]
use lepis::route::copy::{Piece, Splitter, read};
use libfuzzer_sys::fuzz_target;

const STATEMENTS: &[&str] = &[
	"copy t from stdin",
	"copy t from stdin with (format csv)",
	"copy t from stdin with (format csv, header true)",
	"copy t from stdin with (format csv, delimiter ';', quote '''', escape '\\')",
	"copy t from stdin with (format text, delimiter '|', null 'NULL')",
	"copy t from stdin with (format text, header true)",
	"copy t from stdin with (format binary)",
];

fuzz_target!(|data: &[u8]| {
	let [s, k, cut, rest @ ..] = data else { return };
	let sql = STATEMENTS[*s as usize % STATEMENTS.len()];
	let options = read(sql).expect("the fuzz statements are valid").options;
	let mut splitter = Splitter::new(options, (*k % 6) as usize);
	let step = (*cut as usize % 61) + 1;
	let mut sent = Vec::new();
	let mut ok = true;
	for chunk in rest.chunks(step) {
		match splitter.push(chunk) {
			Ok(pieces) => sent.extend(pieces),
			Err(_) => {
				ok = false;
				break;
			}
		}
	}
	if ok {
		match splitter.finish() {
			Ok(pieces) => sent.extend(pieces),
			Err(_) => ok = false,
		}
	}
	let joined: Vec<u8> = sent
		.iter()
		.flat_map(|p| match p {
			Piece::Everyone(b) | Piece::End(b) | Piece::Row { bytes: b, .. } => b.clone(),
		})
		.collect();
	assert!(rest.starts_with(&joined), "the pieces are not the input");
	if ok && !sent.iter().any(|p| matches!(p, Piece::End(_))) {
		assert_eq!(
			joined.len(),
			rest.len(),
			"input was left over with no error"
		);
	}
});
