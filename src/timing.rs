//! What this router measured of each node: how long each node took to
//! answer a client's batch, and how much of the time it was answering. Postgres exposes no CPU
//! reading without an extension (L1), so a node's busy share is the router's own view of it:
//! the time its batches were in flight, over the wall time. One router sees its own traffic only.

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::catalog::NodeId;

/// Latencies kept per node for its percentiles.
const KEEP: usize = 4096;

struct Node {
	latencies: VecDeque<f32>,
	busy: Duration,
	since: Instant,
}

fn all() -> &'static Mutex<HashMap<NodeId, Node>> {
	static ALL: OnceLock<Mutex<HashMap<NodeId, Node>>> = OnceLock::new();
	ALL.get_or_init(|| Mutex::new(HashMap::new()))
}

/// One batch answered by `node` in `took`.
pub fn record(node: NodeId, took: Duration) {
	let Ok(mut m) = all().lock() else {
		return;
	};
	let n = m.entry(node).or_insert_with(|| Node {
		latencies: VecDeque::with_capacity(KEEP),
		busy: Duration::ZERO,
		since: Instant::now(),
	});
	if n.latencies.len() == KEEP {
		n.latencies.pop_front();
	}
	n.latencies.push_back(took.as_secs_f32() * 1000.0);
	n.busy += took;
}

/// A node's p50 and p99 in ms over its last answers, and its busy share since the last reading
/// (which starts the next window).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Reading {
	pub p50_ms: f64,
	pub p99_ms: f64,
	pub busy_pct: f64,
	pub answers: usize,
}

pub fn read(node: NodeId) -> Option<Reading> {
	let mut m = all().lock().ok()?;
	let n = m.get_mut(&node)?;
	let mut v: Vec<f32> = n.latencies.iter().copied().collect();
	if v.is_empty() {
		return None;
	}
	v.sort_by(f32::total_cmp);
	let at = |q: f64| f64::from(v[((v.len() - 1) as f64 * q).round() as usize]);
	let wall = n.since.elapsed().as_secs_f64().max(0.001);
	let r = Reading {
		p50_ms: at(0.5),
		p99_ms: at(0.99),
		busy_pct: (n.busy.as_secs_f64() / wall * 100.0).min(100.0 * 64.0),
		answers: v.len(),
	};
	n.busy = Duration::ZERO;
	n.since = Instant::now();
	Some(r)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn percentiles_of_what_was_recorded() {
		let n = NodeId(9_999);
		for i in 1..=100 {
			record(n, Duration::from_millis(i));
		}
		let r = read(n).unwrap();
		assert_eq!(r.answers, 100);
		assert!((r.p50_ms - 51.0).abs() < 1.5, "{r:?}");
		assert!((r.p99_ms - 99.0).abs() < 1.5, "{r:?}");
		assert!(r.busy_pct > 0.0);
		assert!(read(NodeId(9_998)).is_none());
	}
}
