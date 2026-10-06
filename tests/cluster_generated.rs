//! The generated corpus (`tests/common/gen.rs`) on the three-node cluster: each query runs
//! through an in-process Lepis and against the reference Postgres, and the answers must be
//! equal, or Lepis must refuse with `0A000` (L9). Anything else fails, printed with its seed,
//! its index and its SQL, which is all it takes to run it again.
//!
//!   LEPIS_GEN_N      how many queries (default 500)
//!   LEPIS_GEN_SEED   the seed (default 0x5eed; decimal or 0x hex)
//!   LEPIS_GEN_ONLY   one index alone
//!
//! Needs the cluster `scripts/it.sh` starts (`LEPIS_IT_ONLY=cluster_generated`); skips without.
//! A query the REFERENCE rejects is the generator's bug, not the router's, and fails too.

#[macro_use]
mod common;
#[path = "common/gen.rs"]
mod generated;

use std::time::Duration;

use common::*;
use generated::Generator;

fn number(v: &str) -> u64 {
	match v.strip_prefix("0x") {
		Some(h) => u64::from_str_radix(h, 16),
		None => v.parse(),
	}
	.unwrap_or_else(|_| panic!("not a number: {v}"))
}

async fn timed(c: &Client, sql: &str, ordered: bool) -> Answer {
	tokio::time::timeout(Duration::from_secs(60), answer(c, sql, ordered))
		.await
		.unwrap_or_else(|_| Err(("timeout".into(), "no answer in 60 s".into())))
}

#[tokio::test]
async fn generated_queries_answer_like_one_postgres() {
	let b = need_bed!();
	let n = env("LEPIS_GEN_N").map_or(500, |v| number(&v));
	let seed = env("LEPIS_GEN_SEED").map_or(0x5eed, |v| number(&v));
	let indices: Vec<u64> = match env("LEPIS_GEN_ONLY") {
		Some(i) => vec![number(&i)],
		None => (0..n).collect(),
	};

	let lepis = start_lepis(&b).await;
	let reference = connect(&b.reference, "lepis_app", "app-pw").await;
	let devices: Vec<String> = reference
		.query(
			"select distinct device::text from oracle.events order by 1",
			&[],
		)
		.await
		.unwrap()
		.iter()
		.map(|r| r.get(0))
		.collect();
	let generator = Generator::new(devices);

	let mut counts: HashMap<&str, usize> = HashMap::new();
	let mut by_family: HashMap<(&str, &str), usize> = HashMap::new();
	let mut failures = Vec::new();
	for i in indices {
		let q = generator.query(seed, i);
		// A fresh session each, so one refusal inside a transaction cannot leak into the next.
		let cluster = connect(&lepis, "lepis_app", "app-pw").await;
		let want = timed(&reference, &q.sql, q.ordered).await;
		let got = timed(&cluster, &q.sql, q.ordered).await;
		let verdict = match (&want, &got) {
			(Err(_), _) => "invalid",
			(_, Err((code, _))) if code == "0A000" => "refused",
			(w, g) if w == g => "equal",
			_ => "wrong",
		};
		*counts.entry(verdict).or_default() += 1;
		*by_family.entry((q.family, verdict)).or_default() += 1;
		if matches!(verdict, "wrong" | "invalid") {
			failures.push(format!(
				"{verdict}: seed={seed:#x} index={i} ({}, {})\n  {}\n  {}",
				q.family,
				if q.ordered { "ordered" } else { "as a multiset" },
				q.sql,
				difference(&want, &got)
			));
		}
	}

	let mut families: Vec<_> = by_family.into_iter().collect();
	families.sort();
	eprintln!("generated corpus, seed {seed:#x}: {counts:?}");
	for ((family, verdict), k) in families {
		eprintln!("  {family:>14} {verdict:>8} {k}");
	}
	assert!(
		failures.is_empty(),
		"{} of the generated queries failed:\n{}",
		failures.len(),
		failures.join("\n")
	);
}

/// Where two answers part, short enough to read in a failure: the errors, or the row counts and
/// the first row that differs.
fn difference(want: &Answer, got: &Answer) -> String {
	match (want, got) {
		(Ok(w), Ok(g)) => {
			let at = w.iter().zip(g.iter()).position(|(a, b)| a != b);
			let row = |rows: &Vec<Vec<Option<String>>>, i: usize| {
				rows.get(i).map_or("(none)".to_string(), |r| format!("{r:?}"))
			};
			let i = at.unwrap_or(w.len().min(g.len()));
			format!(
				"want {} rows, got {}; first difference at row {i}:\n    want {}\n    got  {}",
				w.len(),
				g.len(),
				row(w, i),
				row(g, i)
			)
		}
		_ => format!("want {}\n  got  {}", one(want), one(got)),
	}
}

fn one(a: &Answer) -> String {
	match a {
		Err((code, message)) => format!("error {code}: {message}"),
		Ok(rows) => format!("{} rows, the first {:?}", rows.len(), rows.first()),
	}
}
