//! Phase 10's advisor: when to split, move or add, said as the operations the admin API already
//! takes. It ADVISES and nothing else: nothing here writes a job, and nothing runs on its own.
//! A recommendation is a request body (`{"op": "range.split", ...}`) with the reason in a
//! sentence and the same `plan` that `POST /v1/plan` would give; running it is a person's (or
//! their agent's) call, through `POST /v1/jobs` like any other operation. Autoscale is a later
//! decision (TBD-3) and is not built here.
//!
//! Two halves:
//!
//! - **Facts** (`gather`, impure): per node, the database's size, its client connections against
//!   `max_connections`, and its write rate; per range, rows, bytes and write rate. A range is a
//!   slice of the hash space, not a relation, so its share of a table is measured by SAMPLING:
//!   each node's sharded tables are read down to about `SAMPLE_ROWS` rows (`TABLESAMPLE SYSTEM`
//!   when larger), each sampled row's key is hashed with the keyspace's own L5 expression, and
//!   the table's `pg_total_relation_size` and `reltuples` are divided among the node's ranges in
//!   proportion. Write rates are two readings of `pg_stat_user_tables` (inserts, updates and
//!   deletes) `sample_ms` apart; Postgres keeps them per table, so a table's rate is attributed
//!   to the node's ranges in proportion to their rows. The sampled keys also give each range the
//!   bound that splits it where the data is, not where the hash space is.
//! - **Decisions** (`decide`, pure and unit-tested): a node holding more than `skew_pct` of an
//!   even share of a keyspace's bytes (or `hot_pct` of its writes) gives a range to the least
//!   loaded node, whole when one fits the gap, otherwise split at the sampled bound; one key that
//!   is most of a range is pinned instead, since no bound splits a single key; a range over
//!   `split_bytes` is split whatever the balance; and a cluster whose data is over `disk_pct` of
//!   its nodes' disk, or whose every node is over `conn_pct` of its connections, needs a node.
//!
//! Every threshold is a cluster setting (`POST /v1/settings`, the `advice_*` names) and every
//! default is CHOSEN, not measured.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use super::engine;
use super::spec::Op;
use super::steps::{connect_node, tables_of};
use super::{Kind, OpError, Pg, Settings, load_catalog, plan, qualified};
use crate::catalog::{Catalog, NodeId, NodeState, Range, RelationName, quote_ident};
use crate::server::App;

/// Rows read from one node's table to tell its ranges apart. Chosen: enough that a range holding
/// a tenth of a table is seen in about two thousand rows, cheap enough to do on every call.
pub const SAMPLE_ROWS: f64 = 20_000.0;

/// The longest a call may spend between its two readings of the write counters.
pub const MAX_SAMPLE_MS: u64 = 30_000;

/// The advisor's thresholds, from `lepis.cluster.settings`. Every default is chosen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdviceSettings {
	/// How long between the two readings of the write counters (0: no write rates).
	pub sample_ms: u64,
	/// A keyspace smaller than this gets no balancing advice: moving it is not worth a pause.
	pub min_bytes: u64,
	/// A range larger than this is split whatever the balance.
	pub split_bytes: u64,
	/// A node holding more than this percent of an even share of a keyspace's bytes is too big.
	pub skew_pct: u64,
	/// The same for writes per second.
	pub hot_pct: u64,
	/// A keyspace taking fewer writes per second than this gets no write advice.
	pub min_writes_per_s: u64,
	/// One key holding this percent of a range is pinned rather than split.
	pub pin_pct: u64,
	/// Each node's disk, when the user says it (Postgres cannot see it). 0: unknown.
	pub node_disk_bytes: u64,
	/// The data over this percent of the nodes' disk needs a node.
	pub disk_pct: u64,
	/// Every node over this percent of `max_connections` needs a node.
	pub conn_pct: u64,
}

/// The settings' names, as `POST /v1/settings` takes them.
pub const SETTING_NAMES: [&str; 10] = [
	"advice_sample_ms",
	"advice_min_bytes",
	"advice_split_bytes",
	"advice_skew_pct",
	"advice_hot_pct",
	"advice_min_writes_per_s",
	"advice_pin_pct",
	"advice_node_disk_bytes",
	"advice_disk_pct",
	"advice_conn_pct",
];

impl Default for AdviceSettings {
	fn default() -> Self {
		AdviceSettings {
			sample_ms: 5_000,
			min_bytes: 256 << 20,
			// About eleven minutes of copy at the plan's assumed 50 MB/s.
			split_bytes: 32 << 30,
			skew_pct: 150,
			hot_pct: 200,
			min_writes_per_s: 100,
			pin_pct: 50,
			node_disk_bytes: 0,
			disk_pct: 80,
			conn_pct: 80,
		}
	}
}

impl AdviceSettings {
	pub fn from_json(cluster: &Value) -> AdviceSettings {
		let d = AdviceSettings::default();
		let get = |k: &str, d: u64| cluster.get(k).and_then(Value::as_u64).unwrap_or(d);
		AdviceSettings {
			sample_ms: get("advice_sample_ms", d.sample_ms).min(MAX_SAMPLE_MS),
			min_bytes: get("advice_min_bytes", d.min_bytes),
			split_bytes: get("advice_split_bytes", d.split_bytes).max(1),
			skew_pct: get("advice_skew_pct", d.skew_pct).max(101),
			hot_pct: get("advice_hot_pct", d.hot_pct).max(101),
			min_writes_per_s: get("advice_min_writes_per_s", d.min_writes_per_s),
			pin_pct: get("advice_pin_pct", d.pin_pct).clamp(1, 100),
			node_disk_bytes: get("advice_node_disk_bytes", d.node_disk_bytes),
			disk_pct: get("advice_disk_pct", d.disk_pct).clamp(1, 100),
			conn_pct: get("advice_conn_pct", d.conn_pct).clamp(1, 100),
		}
	}

	pub fn to_json(self) -> Value {
		json!({
			"advice_sample_ms": self.sample_ms,
			"advice_min_bytes": self.min_bytes,
			"advice_split_bytes": self.split_bytes,
			"advice_skew_pct": self.skew_pct,
			"advice_hot_pct": self.hot_pct,
			"advice_min_writes_per_s": self.min_writes_per_s,
			"advice_pin_pct": self.pin_pct,
			"advice_node_disk_bytes": self.node_disk_bytes,
			"advice_disk_pct": self.disk_pct,
			"advice_conn_pct": self.conn_pct,
		})
	}
}

/// What one node said about itself.
#[derive(Clone, Debug)]
pub struct NodeFacts {
	pub id: NodeId,
	pub name: String,
	/// Active and answering: a node that can be given rows.
	pub active: bool,
	pub db_bytes: u64,
	pub connections: u64,
	pub max_connections: u64,
	pub writes_per_s: f64,
	/// Why the node could not be read, when it could not.
	pub error: Option<String>,
}

/// One sampled row: its key's hash, its key as text, and how much of the node's bytes and writes
/// it stands for.
#[derive(Clone, Debug, PartialEq)]
pub struct Sample {
	pub hash: i64,
	pub key: String,
	pub bytes: f64,
	pub writes: f64,
}

/// One range, as measured on its owner.
#[derive(Clone, Debug)]
pub struct RangeFacts {
	pub keyspace: String,
	pub lo: i64,
	pub hi: i64,
	pub node: NodeId,
	pub rows: f64,
	pub bytes: f64,
	pub writes_per_s: f64,
	/// The sampled rows that fell in it, sorted by hash.
	pub samples: Vec<Sample>,
}

#[derive(Clone, Debug)]
pub struct Facts {
	pub nodes: Vec<NodeFacts>,
	pub ranges: Vec<RangeFacts>,
	/// (keyspace, value) already pinned.
	pub pins: HashSet<(String, String)>,
	/// The id the next node would get, for the name a node.add suggests.
	pub next_node: i32,
	pub sampled_ms: u64,
}

/// One recommendation: a body the admin API takes, and why.
#[derive(Clone, Debug, PartialEq)]
pub struct Advice {
	pub request: Value,
	pub reason: String,
	/// What it is about: `size`, `writes`, `disk` or `connections`.
	pub metric: &'static str,
	/// Fields the request still needs from a person (a new node's address), so it cannot be
	/// planned yet.
	pub needs: Vec<&'static str>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Metric {
	Bytes,
	Writes,
}

impl Metric {
	fn of(self, r: &RangeFacts) -> f64 {
		match self {
			Metric::Bytes => r.bytes,
			Metric::Writes => r.writes_per_s,
		}
	}

	fn weight(self, s: &Sample) -> f64 {
		match self {
			Metric::Bytes => s.bytes,
			Metric::Writes => s.writes,
		}
	}

	fn name(self) -> &'static str {
		match self {
			Metric::Bytes => "size",
			Metric::Writes => "writes",
		}
	}

	fn floor(self, s: &AdviceSettings) -> f64 {
		match self {
			Metric::Bytes => s.min_bytes as f64,
			Metric::Writes => s.min_writes_per_s as f64,
		}
	}

	fn pct(self, s: &AdviceSettings) -> u64 {
		match self {
			Metric::Bytes => s.skew_pct,
			Metric::Writes => s.hot_pct,
		}
	}

	fn show(self, v: f64) -> String {
		match self {
			Metric::Bytes => bytes(v),
			Metric::Writes => format!("{v:.1} writes/s"),
		}
	}
}

/// Bytes in the unit a person would say.
pub fn bytes(v: f64) -> String {
	let units = ["B", "kB", "MB", "GB", "TB", "PB"];
	let mut v = v.max(0.0);
	let mut u = 0;
	while v >= 1000.0 && u + 1 < units.len() {
		v /= 1000.0;
		u += 1;
	}
	if u == 0 {
		format!("{v:.0} B")
	} else {
		format!("{v:.1} {}", units[u])
	}
}

/// The recommendations for these facts. Pure: the whole policy is here.
pub fn decide(f: &Facts, s: &AdviceSettings) -> Vec<Advice> {
	let members: Vec<NodeId> = f
		.nodes
		.iter()
		.filter(|n| n.active && n.error.is_none())
		.map(|n| n.id)
		.collect();
	let mut out = Vec::new();
	let mut touched: HashSet<(String, i64)> = HashSet::new();
	let mut add_reasons: Vec<(String, &'static str)> = cluster_headroom(f, s, &members);

	let mut names: Vec<&str> = f.ranges.iter().map(|r| r.keyspace.as_str()).collect();
	names.sort_unstable();
	names.dedup();
	for k in names {
		let rs: Vec<&RangeFacts> = f.ranges.iter().filter(|r| r.keyspace == k).collect();
		for m in [Metric::Bytes, Metric::Writes] {
			if let Some((lo, a)) = balance(f, s, k, &rs, &members, m)
				&& touched.insert((k.to_string(), lo))
			{
				out.push(a);
			}
		}
		// A range too big to be one unit, however even the nodes are.
		let mut big: Vec<&&RangeFacts> = rs
			.iter()
			.filter(|r| r.bytes > s.split_bytes as f64 && r.bytes >= s.min_bytes as f64)
			.collect();
		big.sort_by(|a, b| b.bytes.total_cmp(&a.bytes));
		for r in big {
			if touched.contains(&(k.to_string(), r.lo)) {
				continue;
			}
			let why = format!(
				"range {k}:{} holds {}, over the {} a range may hold (advice_split_bytes)",
				r.lo,
				bytes(r.bytes),
				bytes(s.split_bytes as f64)
			);
			match least_loaded(&rs, &members, r.node, Metric::Bytes) {
				Some(to) => {
					touched.insert((k.to_string(), r.lo));
					out.push(split_or_pin(f, s, r, r.bytes / 2.0, to, Metric::Bytes, &why));
				}
				None => add_reasons.push((format!("{why}, and there is no other node to take half"), "size")),
			}
		}
	}
	out.extend(node_disk(f, s, &members, &mut touched));
	if !add_reasons.is_empty() {
		let reason = add_reasons
			.iter()
			.map(|(r, _)| r.as_str())
			.collect::<Vec<_>>()
			.join("; and ");
		out.insert(
			0,
			Advice {
				request: json!({"op": "node.add", "name": format!("n{}", f.next_node), "host": null}),
				reason: format!("{reason}: add a node (it needs an address), then rebalance onto it"),
				metric: add_reasons[0].1,
				needs: vec!["host"],
			},
		);
	}
	out
}

/// The cluster as a whole out of room: the data against the nodes' disk, and connections.
fn cluster_headroom(f: &Facts, s: &AdviceSettings, members: &[NodeId]) -> Vec<(String, &'static str)> {
	let live: Vec<&NodeFacts> = f.nodes.iter().filter(|n| members.contains(&n.id)).collect();
	let mut why = Vec::new();
	if live.is_empty() {
		return why;
	}
	if s.node_disk_bytes > 0 {
		let used: f64 = live.iter().map(|n| n.db_bytes as f64).sum();
		let cap = live.len() as f64 * s.node_disk_bytes as f64;
		if used * 100.0 > s.disk_pct as f64 * cap {
			why.push((
				format!(
					"the data ({}) is {:.0}% of the {} nodes' disk ({} each, advice_node_disk_bytes), over the {}% set (advice_disk_pct)",
					bytes(used),
					used * 100.0 / cap,
					live.len(),
					bytes(s.node_disk_bytes as f64),
					s.disk_pct
				),
				"disk",
			));
		}
	}
	if live
		.iter()
		.all(|n| n.max_connections > 0 && n.connections * 100 > s.conn_pct * n.max_connections)
	{
		why.push((
			format!(
				"every node is using more than {}% of its max_connections (advice_conn_pct)",
				s.conn_pct
			),
			"connections",
		));
	}
	why
}

/// The member other than `not` holding the least of this keyspace.
fn least_loaded(rs: &[&RangeFacts], members: &[NodeId], not: NodeId, m: Metric) -> Option<NodeId> {
	let mut load: BTreeMap<NodeId, f64> = members.iter().map(|n| (*n, 0.0)).collect();
	for r in rs {
		if let Some(l) = load.get_mut(&r.node) {
			*l += m.of(r);
		}
	}
	load.into_iter()
		.filter(|(n, _)| *n != not)
		.min_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)))
		.map(|(n, _)| n)
}

/// One keyspace, one metric: when its most loaded node is over the threshold, the one step that
/// narrows the gap to its least loaded. Returns the range it is about.
fn balance(
	f: &Facts,
	s: &AdviceSettings,
	k: &str,
	rs: &[&RangeFacts],
	members: &[NodeId],
	m: Metric,
) -> Option<(i64, Advice)> {
	if members.len() < 2 {
		return None;
	}
	let mut load: BTreeMap<NodeId, f64> = members.iter().map(|n| (*n, 0.0)).collect();
	for r in rs {
		if let Some(l) = load.get_mut(&r.node) {
			*l += m.of(r);
		}
	}
	let total: f64 = load.values().sum();
	if total <= 0.0 || total < m.floor(s) {
		return None;
	}
	let fair = total / members.len() as f64;
	let (a, la) = load
		.iter()
		.max_by(|x, y| x.1.total_cmp(y.1).then(y.0.cmp(x.0)))
		.map(|(n, l)| (*n, *l))?;
	let (b, lb) = load
		.iter()
		.min_by(|x, y| x.1.total_cmp(y.1).then(x.0.cmp(y.0)))
		.map(|(n, l)| (*n, *l))?;
	if a == b || la * 100.0 <= m.pct(s) as f64 * fair {
		return None;
	}
	let gap = la - lb;
	let want = gap / 2.0;
	let why = format!(
		"{a} holds {} of keyspace {k}'s {}, {:.0}% of an even share ({}; the threshold is {}%), and {b} holds the least ({})",
		m.show(la),
		m.name(),
		la * 100.0 / fair,
		m.show(fair),
		m.pct(s),
		m.show(lb)
	);
	let on_a: Vec<&RangeFacts> = rs.iter().copied().filter(|r| r.node == a).collect();
	// A whole range that narrows the gap without overshooting it.
	if let Some(r) = on_a
		.iter()
		.filter(|r| m.of(r) > 0.0 && m.of(r) < gap)
		.min_by(|x, y| (m.of(x) - want).abs().total_cmp(&(m.of(y) - want).abs()))
	{
		return Some((
			r.lo,
			Advice {
				request: json!({"op": "range.move", "keyspace": k, "range": r.lo.to_string(), "to": b.0}),
				reason: format!(
					"{why}: moving range {k}:{} ({}) to {b} evens them out",
					r.lo,
					m.show(m.of(r))
				),
				metric: m.name(),
				needs: vec![],
			},
		));
	}
	// Otherwise the biggest range on it gives part of itself.
	let r = on_a.iter().max_by(|x, y| m.of(x).total_cmp(&m.of(y)))?;
	if m.of(r) <= 0.0 {
		return None;
	}
	Some((r.lo, split_or_pin(f, s, r, want, b, m, &why)))
}

/// Gives about `want` (in `m`'s units) of range `r` to node `to`: a split at the sampled bound,
/// or, when one key is most of the range, a pin, since no bound splits a single key.
fn split_or_pin(
	f: &Facts,
	s: &AdviceSettings,
	r: &RangeFacts,
	want: f64,
	to: NodeId,
	m: Metric,
	why: &str,
) -> Advice {
	let k = &r.keyspace;
	if let Some((key, share)) = dominant(r, m)
		&& share * 100.0 >= s.pin_pct as f64
		&& !f.pins.contains(&(k.clone(), key.clone()))
	{
		return Advice {
			request: json!({"op": "tenant.pin", "keyspace": k, "value": key, "node": to.0}),
			reason: format!(
				"{why}: one key, {key}, is {:.0}% of range {k}:{}'s {} (advice_pin_pct is {}%), and no bound splits a single key; pinning it gives it its own range on {to}",
				share * 100.0,
				r.lo,
				m.name(),
				s.pin_pct
			),
			metric: m.name(),
			needs: vec![],
		};
	}
	let (at, sampled) = match split_bound(r, m, want) {
		Some(at) => (at, true),
		None => (midpoint(r), false),
	};
	Advice {
		request: json!({
			"op": "range.split", "keyspace": k, "range": r.lo.to_string(), "at": at.to_string(), "to": to.0,
		}),
		reason: format!(
			"{why}: splitting range {k}:{} at {at} sends about {} to {to}{}",
			r.lo,
			m.show(want.min(m.of(r))),
			if sampled {
				" (the bound is where the sampled rows put it)"
			} else {
				" (no rows were sampled, so the bound is the middle of the hash space)"
			}
		),
		metric: m.name(),
		needs: vec![],
	}
}

/// The key holding most of a range's `m`, with its share.
fn dominant(r: &RangeFacts, m: Metric) -> Option<(String, f64)> {
	let mut by: HashMap<&str, f64> = HashMap::new();
	let mut total = 0.0;
	for x in &r.samples {
		*by.entry(x.key.as_str()).or_default() += m.weight(x);
		total += m.weight(x);
	}
	if total <= 0.0 || r.samples.len() < 2 {
		return None;
	}
	by.into_iter()
		.max_by(|a, b| a.1.total_cmp(&b.1).then(b.0.cmp(a.0)))
		.map(|(k, w)| (k.to_string(), w / total))
}

/// The bound above which about `want` of the range's `m` lies, by its samples (sorted by hash).
/// Always inside `(lo, hi]`; None when the samples cannot place one.
pub fn split_bound_of(samples: &[Sample], lo: i64, hi: i64, weight: impl Fn(&Sample) -> f64, fraction: f64) -> Option<i64> {
	let total: f64 = samples.iter().map(&weight).sum();
	if total <= 0.0 || lo >= hi {
		return None;
	}
	let target = (fraction.clamp(0.0, 1.0)) * total;
	let mut acc = 0.0;
	let mut at = None;
	for x in samples.iter().rev() {
		acc += weight(x);
		at = Some(x.hash);
		if acc >= target {
			break;
		}
	}
	let mut at = at?;
	if at <= lo {
		// The bound fell on the range's first hash: the next distinct hash above it, if any.
		at = samples.iter().map(|x| x.hash).find(|h| *h > lo)?;
	}
	(at > lo && at <= hi).then_some(at)
}

fn split_bound(r: &RangeFacts, m: Metric, want: f64) -> Option<i64> {
	let v = m.of(r);
	if v <= 0.0 {
		return None;
	}
	split_bound_of(&r.samples, r.lo, r.hi, |x| m.weight(x), want / v)
}

fn midpoint(r: &RangeFacts) -> i64 {
	(r.lo as i128 + (r.hi as i128 - r.lo as i128) / 2 + 1) as i64
}

/// A node over its share of its own disk, when other nodes are not: the range that fits the
/// emptiest node's room moves there, or the biggest is split to it.
fn node_disk(f: &Facts, s: &AdviceSettings, members: &[NodeId], touched: &mut HashSet<(String, i64)>) -> Vec<Advice> {
	let mut out = Vec::new();
	if s.node_disk_bytes == 0 {
		return out;
	}
	let limit = s.node_disk_bytes as f64 * s.disk_pct as f64 / 100.0;
	let live: Vec<&NodeFacts> = f.nodes.iter().filter(|n| members.contains(&n.id)).collect();
	let Some(roomiest) = live.iter().min_by(|a, b| a.db_bytes.cmp(&b.db_bytes).then(a.id.cmp(&b.id))) else {
		return out;
	};
	let room = limit - roomiest.db_bytes as f64;
	for n in &live {
		if (n.db_bytes as f64) <= limit || n.id == roomiest.id || room <= 0.0 {
			continue;
		}
		let why = format!(
			"{} uses {} of its {} disk, over the {}% set (advice_disk_pct), and {} has {} of room",
			n.id,
			bytes(n.db_bytes as f64),
			bytes(s.node_disk_bytes as f64),
			s.disk_pct,
			roomiest.id,
			bytes(room)
		);
		let mut mine: Vec<&RangeFacts> = f
			.ranges
			.iter()
			.filter(|r| r.node == n.id && !touched.contains(&(r.keyspace.clone(), r.lo)) && r.bytes > 0.0)
			.collect();
		mine.sort_by(|a, b| b.bytes.total_cmp(&a.bytes));
		let Some(biggest) = mine.first() else {
			continue;
		};
		let a = match mine.iter().find(|r| r.bytes < room) {
			Some(r) => Advice {
				request: json!({"op": "range.move", "keyspace": r.keyspace, "range": r.lo.to_string(), "to": roomiest.id.0}),
				reason: format!("{why}: moving range {}:{} ({}) there", r.keyspace, r.lo, bytes(r.bytes)),
				metric: "disk",
				needs: vec![],
			},
			None => {
				let mut a = split_or_pin(f, s, biggest, room / 2.0, roomiest.id, Metric::Bytes, &why);
				a.metric = "disk";
				a
			}
		};
		let r = a.request.get("range").and_then(Value::as_str).and_then(|t| t.parse().ok()).unwrap_or(biggest.lo);
		let k = a.request.get("keyspace").and_then(Value::as_str).unwrap_or_default().to_string();
		touched.insert((k, r));
		out.push(a);
	}
	out
}

/// One node's sharded table, as read for the facts.
struct TableRead {
	node: NodeId,
	keyspace: String,
	size: f64,
	tuples: f64,
	writes_per_s: f64,
	/// (hash, key) of every sampled row, owned or not.
	sample: Vec<(i64, String)>,
}

async fn write_counters(pg: &mut Pg) -> Result<HashMap<String, f64>, OpError> {
	let _ = pg.simple("select pg_stat_clear_snapshot()").await;
	let rows = pg
		.simple(
			"select schemaname || '.' || relname, n_tup_ins + n_tup_upd + n_tup_del from pg_stat_user_tables",
		)
		.await?;
	Ok(rows
		.into_iter()
		.filter_map(|r| {
			let name = r.first().cloned().flatten()?;
			let n = r.get(1).cloned().flatten()?.parse().ok()?;
			Some((name, n))
		})
		.collect())
}

fn num(row: &[Option<String>], i: usize) -> f64 {
	row.get(i)
		.cloned()
		.flatten()
		.and_then(|v| v.parse::<f64>().ok())
		.unwrap_or(0.0)
}

/// Reads the facts: every node's size, connections and write rate, and every range's share of
/// its node's tables. `sample_ms` apart, the two readings of the write counters.
pub async fn gather(app: &Arc<App>, c: &Catalog, sample_ms: u64) -> Result<Facts, OpError> {
	let t0 = Instant::now();
	let mut nodes: Vec<NodeFacts> = Vec::new();
	let mut conns: BTreeMap<NodeId, (Pg, u32)> = BTreeMap::new();
	let mut before: HashMap<NodeId, HashMap<String, f64>> = HashMap::new();
	for n in c.nodes.values().filter(|n| n.state != NodeState::Removed) {
		let mut facts = NodeFacts {
			id: n.id,
			name: n.name.clone(),
			active: n.state == NodeState::Active,
			db_bytes: 0,
			connections: 0,
			max_connections: 0,
			writes_per_s: 0.0,
			error: None,
		};
		let read = async {
			let mut pg = connect_node(app, c, n.id).await?;
			let counters = write_counters(&mut pg).await?;
			let row = pg
				.simple(
					"select pg_database_size(current_database()), \
					(select count(*) from pg_stat_activity where backend_type = 'client backend'), \
					current_setting('max_connections'), current_setting('server_version_num')",
				)
				.await?
				.into_iter()
				.next()
				.unwrap_or_default();
			Ok::<_, OpError>((pg, counters, row))
		};
		match read.await {
			Ok((pg, counters, row)) => {
				facts.db_bytes = num(&row, 0) as u64;
				facts.connections = num(&row, 1) as u64;
				facts.max_connections = num(&row, 2) as u64;
				before.insert(n.id, counters);
				conns.insert(n.id, (pg, num(&row, 3) as u32));
			}
			Err(e) => facts.error = Some(e.message),
		}
		nodes.push(facts);
	}

	let mut reads: BTreeMap<(NodeId, RelationName), TableRead> = BTreeMap::new();
	let mut names: Vec<&String> = c.keyspaces.keys().collect();
	names.sort();
	for name in names {
		let ks = &c.keyspaces[name];
		for rel in tables_of(c, name) {
			let Some(crate::catalog::RelationKind::Sharded { key_column, .. }) = c.relations.get(&rel) else {
				continue;
			};
			for (id, (pg, version)) in conns.iter_mut() {
				if !ks.ranges.iter().any(|r| r.node == *id) {
					continue;
				}
				let row = pg
					.query(
						"select coalesce(pg_total_relation_size(to_regclass($1)), 0), \
						coalesce((select reltuples from pg_class where oid = to_regclass($1)), 0), \
						to_regclass($1) is not null",
						&[&qualified(&rel)],
					)
					.await?
					.into_iter()
					.next()
					.unwrap_or_default();
				if row.get(2).cloned().flatten().as_deref() != Some("t") {
					continue;
				}
				let size = num(&row, 0);
				let mut tuples = num(&row, 1);
				if tuples <= 0.0 && size > 0.0 && size < 1e9 {
					// Never analyzed: count it, when that is cheap.
					tuples = pg
						.value(&format!("select count(*) from {}", qualified(&rel)), &[])
						.await?
						.and_then(|v| v.parse().ok())
						.unwrap_or(0.0);
				}
				let Ok(expr) = ks.key_type.sql_expression(&quote_ident(key_column), ks.seed, *version) else {
					continue;
				};
				let sample_clause = if tuples > SAMPLE_ROWS {
					format!(" tablesample system ({:.6})", (SAMPLE_ROWS * 100.0 / tuples).max(0.000_001))
				} else {
					String::new()
				};
				let rows = pg
					.simple(&format!(
						"select ({expr})::text, ({})::text from {}{sample_clause}",
						quote_ident(key_column),
						qualified(&rel)
					))
					.await?;
				let sample = rows
					.into_iter()
					.filter_map(|r| {
						let h = r.first().cloned().flatten()?.parse().ok()?;
						Some((h, r.get(1).cloned().flatten().unwrap_or_default()))
					})
					.collect();
				reads.insert(
					(*id, rel.clone()),
					TableRead {
						node: *id,
						keyspace: name.clone(),
						size,
						tuples: tuples.max(0.0),
						writes_per_s: 0.0,
						sample,
					},
				);
			}
		}
	}

	// The second reading of the write counters, `sample_ms` after the first.
	let mut sampled_ms = 0;
	if sample_ms > 0 {
		let wait = Duration::from_millis(sample_ms.min(MAX_SAMPLE_MS)).saturating_sub(t0.elapsed());
		tokio::time::sleep(wait).await;
		let seconds = t0.elapsed().as_secs_f64().max(0.001);
		sampled_ms = (seconds * 1000.0) as u64;
		for (id, (pg, _)) in conns.iter_mut() {
			let after = write_counters(pg).await?;
			let was = before.get(id).cloned().unwrap_or_default();
			let rate = |name: &str| (after.get(name).copied().unwrap_or(0.0) - was.get(name).copied().unwrap_or(0.0)).max(0.0) / seconds;
			if let Some(n) = nodes.iter_mut().find(|n| n.id == *id) {
				n.writes_per_s = after.keys().map(|k| rate(k)).sum();
			}
			for ((node, rel), t) in reads.iter_mut() {
				if node == id {
					t.writes_per_s = rate(&rel.to_string());
				}
			}
		}
	}
	for (_, (pg, _)) in conns {
		pg.close().await;
	}

	let mut ranges = Vec::new();
	let mut pins = HashSet::new();
	let mut names: Vec<&String> = c.keyspaces.keys().collect();
	names.sort();
	for name in names {
		let ks = &c.keyspaces[name];
		for v in ks.pins.keys() {
			pins.insert((name.clone(), v.clone()));
		}
		let tables: Vec<&TableRead> = reads.values().filter(|t| &t.keyspace == name).collect();
		ranges.extend(attribute(name, &ks.ranges, &tables));
	}
	Ok(Facts {
		nodes,
		ranges,
		pins,
		next_node: c.nodes.keys().map(|n| n.0).max().unwrap_or(0) + 1,
		sampled_ms,
	})
}

fn width(r: &Range) -> f64 {
	(r.hi as i128 - r.lo as i128 + 1) as f64
}

/// Divides each node's tables among the ranges it owns, by where their sampled rows' hashes fall
/// (by width when nothing was sampled).
fn attribute(keyspace: &str, ranges: &[Range], tables: &[&TableRead]) -> Vec<RangeFacts> {
	let mut out: Vec<RangeFacts> = ranges
		.iter()
		.map(|r| RangeFacts {
			keyspace: keyspace.to_string(),
			lo: r.lo,
			hi: r.hi,
			node: r.node,
			rows: 0.0,
			bytes: 0.0,
			writes_per_s: 0.0,
			samples: Vec::new(),
		})
		.collect();
	for t in tables {
		let owned: Vec<usize> = (0..ranges.len()).filter(|i| ranges[*i].node == t.node).collect();
		if owned.is_empty() {
			continue;
		}
		let n = t.sample.len() as f64;
		if n == 0.0 {
			let total: f64 = owned.iter().map(|i| width(&ranges[*i])).sum();
			for i in owned {
				let share = width(&ranges[i]) / total;
				out[i].rows += t.tuples * share;
				out[i].bytes += t.size * share;
				out[i].writes_per_s += t.writes_per_s * share;
			}
			continue;
		}
		for (h, key) in &t.sample {
			let Some(&i) = owned.iter().find(|i| ranges[**i].lo <= *h && *h <= ranges[**i].hi) else {
				continue;
			};
			out[i].rows += t.tuples / n;
			out[i].bytes += t.size / n;
			out[i].writes_per_s += t.writes_per_s / n;
			out[i].samples.push(Sample {
				hash: *h,
				key: key.clone(),
				bytes: t.size / n,
				writes: t.writes_per_s / n,
			});
		}
	}
	for r in &mut out {
		r.samples.sort_by_key(|s| s.hash);
	}
	out
}

fn facts_json(f: &Facts) -> Value {
	json!({
		"sampled_ms": f.sampled_ms,
		"nodes": f.nodes.iter().map(|n| json!({
			"id": n.id.0, "name": n.name, "active": n.active, "db_bytes": n.db_bytes,
			"connections": n.connections, "max_connections": n.max_connections,
			"writes_per_s": (n.writes_per_s * 10.0).round() / 10.0, "error": n.error,
		})).collect::<Vec<_>>(),
		"ranges": f.ranges.iter().map(|r| json!({
			"keyspace": r.keyspace, "lo": r.lo.to_string(), "hi": r.hi.to_string(), "node": r.node.0,
			"rows": r.rows.round() as u64, "bytes": r.bytes.round() as u64,
			"writes_per_s": (r.writes_per_s * 10.0).round() / 10.0, "sampled_rows": r.samples.len(),
		})).collect::<Vec<_>>(),
	})
}

/// `GET /v1/advice`: the facts, the recommendations, and each one's plan. Runs nothing.
pub async fn advise(app: &Arc<App>, h: &mut Pg, sample_ms: Option<u64>) -> Result<Value, OpError> {
	if !engine::ensure_jobs_schema(h).await? {
		return Err(OpError::refused(Kind::NoCatalog, "the home node has no Lepis catalog (schema lepis)"));
	}
	let cluster: Value = h
		.value("select settings::text from lepis.cluster where id = 1", &[])
		.await?
		.and_then(|v| serde_json::from_str(&v).ok())
		.unwrap_or(Value::Null);
	let s = AdviceSettings::from_json(&cluster);
	let op_settings = Settings::from_json(&cluster, &Value::Null);
	let c = load_catalog(h).await?;
	let f = gather(app, &c, sample_ms.unwrap_or(s.sample_ms).min(MAX_SAMPLE_MS)).await?;
	let decided = decide(&f, &s);
	let mut advice = Vec::new();
	for a in decided {
		let mut v = json!({
			"op": a.request["op"], "request": a.request, "reason": a.reason, "metric": a.metric,
			"needs": a.needs, "plan": null,
		});
		if a.needs.is_empty() {
			let planned = match Op::from_json(&a.request) {
				Ok(op) => plan::plan(app, h, &op, op_settings).await.map(|p| p.summary),
				Err(e) => Err(e),
			};
			match planned {
				Ok(p) => v["plan"] = p,
				Err(e) => {
					v["plan_error"] = json!({"kind": e.kind.map_or("failed", Kind::name), "message": e.message});
				}
			}
		}
		advice.push(v);
	}
	let summary = if advice.is_empty() {
		format!(
			"Nothing to do: no node holds over {}% of an even share of a keyspace's size or {}% of its writes, no range is over {}, and the nodes have room.",
			s.skew_pct,
			s.hot_pct,
			bytes(s.split_bytes as f64)
		)
	} else {
		format!(
			"{} recommendation{}. Each is a request POST /v1/jobs takes as it is; nothing has run. Run one, then ask again: the next answer is from the cluster it leaves.",
			advice.len(),
			if advice.len() == 1 { "" } else { "s" }
		)
	};
	Ok(json!({
		"summary": summary,
		"advice": advice,
		"facts": facts_json(&f),
		"settings": s.to_json(),
		"assumptions": format!(
			"thresholds are settings and their defaults are chosen, not measured; a range's rows and bytes are its node's table statistics divided by where up to {} sampled rows per table hash; write rates are pg_stat_user_tables inserts+updates+deletes over {} ms, attributed to a node's ranges by their rows, and Postgres reports them with up to a second's delay; disk is known only when advice_node_disk_bytes is set",
			SAMPLE_ROWS as u64,
			f.sampled_ms
		),
	}))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn node(id: i32) -> NodeFacts {
		NodeFacts {
			id: NodeId(id),
			name: format!("n{id}"),
			active: true,
			db_bytes: 0,
			connections: 5,
			max_connections: 100,
			writes_per_s: 0.0,
			error: None,
		}
	}

	/// `n` equal ranges, round-robin over `nodes`, with `bytes[i]` in range i spread evenly over
	/// 100 samples of distinct keys.
	fn ranges(n: usize, nodes: &[i32], bytes: &[f64]) -> Vec<RangeFacts> {
		let ids: Vec<NodeId> = nodes.iter().map(|n| NodeId(*n)).collect();
		crate::catalog::Keyspace::even_ranges(n, &ids)
			.into_iter()
			.enumerate()
			.map(|(i, r)| {
				let step = ((r.hi as i128 - r.lo as i128) / 101) as i64;
				let samples = (1..=100)
					.map(|j| Sample {
						hash: (r.lo as i128 + step as i128 * j as i128) as i64,
						key: format!("{i}-{j}"),
						bytes: bytes[i] / 100.0,
						writes: 0.0,
					})
					.collect();
				RangeFacts {
					keyspace: "k".into(),
					lo: r.lo,
					hi: r.hi,
					node: r.node,
					rows: bytes[i] / 100.0,
					bytes: bytes[i],
					writes_per_s: 0.0,
					samples,
				}
			})
			.collect()
	}

	fn facts(nodes: &[i32], rs: Vec<RangeFacts>) -> Facts {
		Facts {
			nodes: nodes.iter().map(|n| node(*n)).collect(),
			ranges: rs,
			pins: HashSet::new(),
			next_node: nodes.iter().max().unwrap() + 1,
			sampled_ms: 0,
		}
	}

	const GB: f64 = 1e9;

	#[test]
	fn an_even_cluster_gets_no_advice() {
		let f = facts(&[1, 2, 3], ranges(6, &[1, 2, 3], &[GB; 6]));
		assert_eq!(decide(&f, &AdviceSettings::default()), vec![]);
	}

	#[test]
	fn a_small_keyspace_is_left_alone_however_skewed() {
		let f = facts(&[1, 2, 3], ranges(3, &[1, 2, 3], &[100e6, 1e6, 1e6]));
		assert_eq!(decide(&f, &AdviceSettings::default()), vec![]);
		let s = AdviceSettings { min_bytes: 1, ..Default::default() };
		assert_eq!(decide(&f, &s).len(), 1);
	}

	#[test]
	fn one_heavy_range_is_split_where_its_rows_are_toward_the_emptiest_node() {
		let mut rs = ranges(3, &[1, 2, 3], &[GB, 9.0 * GB, 2.0 * GB]);
		// Range 1's rows sit in its lower part: the bound must follow them, not the hash middle.
		let r = &mut rs[1];
		let step = ((r.hi as i128 - r.lo as i128) / 1000) as i64;
		for (j, s) in r.samples.iter_mut().enumerate() {
			s.hash = r.lo + step * (j as i64 + 1);
		}
		let (lo, hi) = (r.lo, r.hi);
		let f = facts(&[1, 2, 3], rs);
		let a = decide(&f, &AdviceSettings::default());
		assert_eq!(a.len(), 1, "{a:?}");
		let req = &a[0].request;
		assert_eq!(req["op"], "range.split");
		assert_eq!(req["range"], lo.to_string());
		assert_eq!(req["to"], 1, "node 1 holds the least");
		let at: i64 = req["at"].as_str().unwrap().parse().unwrap();
		assert!(at > lo && at <= hi);
		// The gap is 8 GB; half of it, 4 GB, is 44 of the 100 sampled rows, all in the bottom tenth.
		assert!(at < lo + (hi - lo) / 10, "{at} should be near the rows, low in the range");
		assert_eq!(a[0].metric, "size");
		assert!(a[0].reason.contains("node 2 holds 9.0 GB"), "{}", a[0].reason);
		// The request is one the admin API takes.
		Op::from_json(req).unwrap();
	}

	#[test]
	fn a_whole_range_moves_when_one_fits_the_gap() {
		// Node 1 holds two ranges (3 GB + 3 GB), node 2 one (0.5 GB): a 3 GB range fits the 5.5 GB gap.
		let f = facts(&[1, 2], ranges(3, &[1, 1, 2], &[3.0 * GB, 3.0 * GB, 0.5 * GB]).into_iter().enumerate().map(|(i, mut r)| {
			r.node = NodeId(if i < 2 { 1 } else { 2 });
			r
		}).collect());
		let a = decide(&f, &AdviceSettings::default());
		assert_eq!(a.len(), 1, "{a:?}");
		assert_eq!(a[0].request["op"], "range.move");
		assert_eq!(a[0].request["to"], 2);
		Op::from_json(&a[0].request).unwrap();
	}

	#[test]
	fn one_key_that_is_most_of_a_range_is_pinned_not_split() {
		let mut rs = ranges(2, &[1, 2], &[10.0 * GB, GB]);
		let lo = rs[0].lo;
		for s in rs[0].samples.iter_mut().take(70) {
			s.key = "4242".into();
			s.hash = lo + 77;
		}
		rs[0].samples.sort_by_key(|s| s.hash);
		let f = facts(&[1, 2], rs.clone());
		let a = decide(&f, &AdviceSettings::default());
		assert_eq!(a[0].request["op"], "tenant.pin");
		assert_eq!(a[0].request["value"], "4242");
		assert_eq!(a[0].request["node"], 2);
		Op::from_json(&a[0].request).unwrap();
		// Already pinned: a split instead.
		let mut f = facts(&[1, 2], rs);
		f.pins.insert(("k".into(), "4242".into()));
		assert_eq!(decide(&f, &AdviceSettings::default())[0].request["op"], "range.split");
	}

	#[test]
	fn hot_writes_count_like_size() {
		let mut rs = ranges(3, &[1, 2, 3], &[GB, GB, GB]);
		rs[0].writes_per_s = 900.0;
		rs[1].writes_per_s = 50.0;
		rs[2].writes_per_s = 50.0;
		for s in rs[0].samples.iter_mut() {
			s.writes = 9.0;
		}
		let f = facts(&[1, 2, 3], rs);
		let a = decide(&f, &AdviceSettings::default());
		assert_eq!(a.len(), 1, "{a:?}");
		assert_eq!(a[0].metric, "writes");
		assert_eq!(a[0].request["op"], "range.split");
		// Below the floor, nothing.
		let s = AdviceSettings { min_writes_per_s: 5_000, ..Default::default() };
		assert_eq!(decide(&f, &s), vec![]);
	}

	#[test]
	fn a_range_over_the_size_limit_is_split_even_when_balanced() {
		let f = facts(&[1, 2], ranges(2, &[1, 2], &[40.0 * GB, 40.0 * GB]));
		let a = decide(&f, &AdviceSettings::default());
		assert_eq!(a.len(), 2, "{a:?}");
		assert!(a.iter().all(|x| x.request["op"] == "range.split"));
		assert_eq!(a[0].request["to"], 2);
		assert_eq!(a[1].request["to"], 1);
		// One node alone: nowhere to send half, so a node.
		let f = facts(&[1], ranges(1, &[1], &[40.0 * GB]));
		let a = decide(&f, &AdviceSettings::default());
		assert_eq!(a.len(), 1);
		assert_eq!(a[0].request["op"], "node.add");
		assert_eq!(a[0].request["name"], "n2");
		assert_eq!(a[0].needs, vec!["host"]);
	}

	#[test]
	fn a_full_cluster_needs_a_node_and_a_full_node_gives_a_range_away() {
		let s = AdviceSettings {
			node_disk_bytes: 100_000_000_000,
			..Default::default()
		};
		let mut f = facts(&[1, 2], ranges(4, &[1, 2], &[GB, GB, GB, GB]));
		f.nodes[0].db_bytes = 90_000_000_000;
		f.nodes[1].db_bytes = 85_000_000_000;
		let a = decide(&f, &s);
		assert_eq!(a[0].request["op"], "node.add", "{a:?}");
		assert_eq!(a[0].metric, "disk");
		// One node full, the other with room: a range moves to it.
		f.nodes[1].db_bytes = 20_000_000_000;
		let a = decide(&f, &s);
		assert_eq!(a.len(), 1, "{a:?}");
		assert_eq!(a[0].request["op"], "range.move");
		assert_eq!(a[0].request["to"], 2);
		assert_eq!(a[0].metric, "disk");
		// Every node short of connections.
		let mut f = facts(&[1, 2], ranges(2, &[1, 2], &[GB, GB]));
		for n in &mut f.nodes {
			n.connections = 95;
		}
		let a = decide(&f, &AdviceSettings::default());
		assert_eq!((a.len(), a[0].metric), (1, "connections"));
	}

	#[test]
	fn unreachable_and_draining_nodes_are_not_targets() {
		let mut f = facts(&[1, 2, 3], ranges(3, &[1, 2, 3], &[9.0 * GB, 2.0 * GB, 0.0]));
		f.nodes[2].error = Some("refused".into());
		let a = decide(&f, &AdviceSettings::default());
		assert_eq!(a[0].request["to"], 2, "{a:?}");
		f.nodes[2].error = None;
		f.nodes[2].active = false;
		assert_eq!(decide(&f, &AdviceSettings::default())[0].request["to"], 2);
	}

	#[test]
	fn the_bound_is_always_inside_the_range() {
		let s = |h: i64| Sample { hash: h, key: h.to_string(), bytes: 1.0, writes: 0.0 };
		let all_low = vec![s(10), s(10), s(10), s(20)];
		assert_eq!(split_bound_of(&all_low, 10, 100, |x| x.bytes, 0.9), Some(20));
		assert_eq!(split_bound_of(&[s(10), s(10)], 10, 100, |x| x.bytes, 0.5), None);
		assert_eq!(split_bound_of(&[], 10, 100, |x| x.bytes, 0.5), None);
		let spread: Vec<Sample> = (1..=10).map(|i| s(i * 10)).collect();
		assert_eq!(split_bound_of(&spread, 0, 1000, |x| x.bytes, 0.3), Some(80));
		assert_eq!(split_bound_of(&spread, i64::MIN, i64::MAX, |x| x.bytes, 1.0), Some(10));
	}

	#[test]
	fn settings_read_back_and_stay_sane() {
		let d = AdviceSettings::default();
		assert_eq!(AdviceSettings::from_json(&d.to_json()), d);
		let s = AdviceSettings::from_json(&json!({"advice_skew_pct": 50, "advice_sample_ms": 999_999, "advice_min_bytes": 1}));
		assert_eq!((s.skew_pct, s.sample_ms, s.min_bytes), (101, MAX_SAMPLE_MS, 1));
		let mut names: Vec<String> = d.to_json().as_object().unwrap().keys().cloned().collect();
		names.sort();
		let mut want: Vec<String> = SETTING_NAMES.iter().map(|s| s.to_string()).collect();
		want.sort();
		assert_eq!(names, want);
	}

	#[test]
	fn attribution_follows_the_sampled_hashes() {
		let r = vec![
			Range { lo: i64::MIN, hi: -1, node: NodeId(1) },
			Range { lo: 0, hi: i64::MAX, node: NodeId(1) },
		];
		let t = TableRead {
			node: NodeId(1),
			keyspace: "k".into(),
			size: 1000.0,
			tuples: 100.0,
			writes_per_s: 10.0,
			sample: vec![(-5, "a".into()), (5, "b".into()), (6, "c".into()), (7, "d".into())],
		};
		let out = attribute("k", &r, &[&t]);
		assert_eq!(out[0].bytes, 250.0);
		assert_eq!(out[1].bytes, 750.0);
		assert_eq!(out[1].rows, 75.0);
		assert_eq!(out[1].samples.len(), 3);
		// Nothing sampled: by width.
		let t = TableRead { sample: vec![], ..t };
		let out = attribute("k", &r, &[&t]);
		assert!((out[0].bytes - 500.0).abs() < 1e-6);
	}
}
