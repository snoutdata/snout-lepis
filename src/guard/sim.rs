//! The simulator (L21, Phase 14a): copies of one Postgres, their guards and a witness under random
//! crashes and cut links, run against a model of Postgres's synchronous replication. Every tick a
//! client writes to every copy that takes writes, and the model acknowledges a commit as Postgres
//! would: at once with `synchronous_standby_names` empty, once every standby named has flushed it,
//! never while fenced; emptying the names releases the commits waiting on it. Then everything heals
//! and the checks run:
//!
//! - every acknowledged commit is on the copy the witness ends with as primary;
//! - a commit is only ever acknowledged by the witness's primary of that moment;
//! - whenever the witness holds a standby eligible, that standby has every acknowledged commit, so
//!   a grant can never lose one (checked every tick, and at each handover and replacement);
//! - with nothing broken, a write is acknowledged again (no copy is left fenced for good).
//!
//! Three copies (`a`, `b`, `c`): two in the pair, the third a spare a replacement brings in. Two
//! witnesses: a pair may be moved from one to the other. An operator stands in for the control
//! plane and the host agents: it rebuilds a replaced copy from the primary (once the witness names
//! it the standby while it is down, a primary, or not streaming from the primary), makes a
//! replacement's copy, removes a copy the pair no longer holds, repoints the guards' definitions
//! (restarting them, as a changed definition does), and, in the random runs, asks for handovers and
//! replacements and moves the pair between witnesses.

// The loops index the copies and their names together, and borrow `self` mutably inside.
#![allow(clippy::needless_range_loop)]

use super::decide::{Action, Change, Memory, Postgres, StandbySeen, SyncNames, decide};
use super::old;
use super::run::{GuardState, in_tick_order};
use super::witness::{Answer, Call, Cluster, Moving, Request, RequestKind};
use serde_json::{Value, json};
use std::collections::HashSet;

const TICK: u64 = 100;
const T: u64 = 10_000;
const NAMES: [&str; 3] = ["a", "b", "c"];
const WITNESSES: [&str; 2] = ["w0", "w1"];
const N: usize = 3;
/// Nodes for links: the copies, then the witnesses.
const NODES: usize = N + 2;
const STREAM_PER_TICK: usize = 3;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Role {
	Primary,
	Standby,
}

/// `synchronous_standby_names`, by copy.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Sync {
	Off,
	Fenced,
	Named(Vec<usize>),
}

struct Copy {
	/// Part of the pair's life: a removed spare is not.
	present: bool,
	pg_up: bool,
	guard_up: bool,
	role: Role,
	wal: Vec<u64>,
	/// The ids in `wal`, to check quickly that a copy holds every acknowledged commit.
	set: HashSet<u64>,
	/// Bumped each time it is rebuilt.
	generation: u64,
	/// How far it has replayed what it holds (a standby); records replayed a tick.
	replay: usize,
	replay_per_tick: usize,
	sync: Sync,
	/// Commits written and waiting for the standbys: (id, the LSN it needs flushed).
	pending: Vec<(u64, usize)>,
	mem: Memory,
	/// Standby: the copy it streams from, and the last message from it (`None`: no stream).
	follows: Option<usize>,
	last_receive: Option<u64>,
	/// Primary: each standby's last reply, the flush and the replay it reported.
	replies: [Option<(u64, usize, usize)>; N],
	/// Primary: a demoted copy ended its standby's stream; it reconnects after this.
	ended_until: u64,
	archive_fenced: bool,
	/// What its definition says: its peer and its witness (the operator's).
	cfg_peer: usize,
	cfg_witness: usize,
	/// The witness it asks now (a moved witness's new address).
	witness: usize,
	/// On disk: a step sent and unanswered, and the standby the witness last named.
	sent: Option<Change>,
	kept_standby: Option<String>,
	/// The next answer to a handover, a replacement or a giving up is lost after the witness acted
	/// on it. (A lost grant is the failover's own concern, where it ends when the stream does.)
	lose_answer: bool,
	/// Its guard runs the image from before moves (`old`, afde9ba1f), with that guard's memory.
	old: bool,
	old_mem: old::decide::Memory,
	/// The guard's state file, as whichever version last wrote it.
	state: Value,
}

struct Witness {
	up: bool,
	/// Runs the witness from before moves.
	old: bool,
	/// Could not read its state file (one a newer witness wrote it cannot take): it stays down.
	broken: bool,
	/// The new witness's state, or for an old one the same state read as the new one reads it.
	cluster: Option<Cluster>,
	old_cluster: Option<old::witness::Cluster>,
	/// The state file, as written, and as the new witness reads it.
	file: Option<Value>,
	file_view: Option<Cluster>,
}

impl Witness {
	fn holding(cluster: Option<Cluster>) -> Witness {
		let file = cluster.as_ref().map(Cluster::to_json);
		Witness {
			up: true,
			old: false,
			broken: false,
			file_view: cluster.clone(),
			cluster,
			old_cluster: None,
			file,
		}
	}

	/// The state file written from what it holds now, by its own version.
	fn save(&mut self) {
		self.file = if self.old {
			self.old_cluster
				.as_ref()
				.map(old::witness::Cluster::to_json)
		} else {
			self.cluster.as_ref().map(Cluster::to_json)
		};
		self.file_view = self.file.as_ref().and_then(|f| Cluster::from_json(f, 0));
		if self.old {
			self.cluster = self.file_view.clone();
		}
	}

	/// Started (again) as `old` or not, from its state file: a file it cannot read keeps it down.
	fn start(&mut self, old: bool, now: u64) {
		self.old = old;
		self.broken = false;
		self.up = true;
		let Some(file) = self.file.clone() else {
			self.cluster = None;
			self.old_cluster = None;
			return;
		};
		if old {
			self.old_cluster = old::witness::Cluster::from_json(&file, now);
			self.broken = self.old_cluster.is_none();
			if !self.broken {
				self.cluster = self
					.old_cluster
					.as_ref()
					.and_then(|c| Cluster::from_json(&c.to_json(), now));
			}
		} else {
			self.cluster = Cluster::from_json(&file, now);
			self.old_cluster = None;
			self.broken = self.cluster.is_none();
		}
		if self.broken {
			self.up = false;
		}
	}

	/// One call, as JSON in and out, as on the wire. `None`: not a call it reads (a 400).
	fn handle(&mut self, call: &Call, now: u64) -> Option<Value> {
		let json = call.to_json();
		if self.old {
			let c = old::witness::Call::from_json(&json)?;
			let (answer, changed) = self.old_cluster.as_mut()?.handle(&c, now);
			if changed {
				self.save();
			}
			return Some(answer.to_json());
		}
		let (answer, changed) = self.cluster.as_mut()?.handle(&Call::from_json(&json)?, now);
		if changed {
			self.save();
		}
		Some(answer.to_json())
	}
}

fn old_action(a: old::decide::Action) -> Action {
	use old::decide::Action as A;
	match a {
		A::Renew => Action::Renew,
		A::Eligible(e) => Action::Eligible(e),
		A::GoAsync => Action::GoAsync,
		A::GoSync => Action::GoSync,
		A::Fence => Action::Fence,
		A::Unfence => Action::Unfence,
		A::Ask => Action::Ask,
		A::Promote(e) => Action::Promote(e),
		A::FenceArchive => Action::FenceArchive,
		A::UnfenceArchive => Action::UnfenceArchive,
	}
}

/// What the old guard's runner reads from the same Postgres: two names it did not write are the
/// standby alone and gone (its `names_of`), and it knows no replay.
fn old_facts(pg: Postgres) -> old::decide::Postgres {
	use old::decide::{Postgres as P, StandbySeen as S, SyncNames as N};
	match pg {
		Postgres::Down => P::Down,
		Postgres::Standby { receive_age_ms } => P::Standby { receive_age_ms },
		Postgres::Primary {
			names,
			standby,
			lsn,
			archive_fenced,
			..
		} => P::Primary {
			names: match names {
				SyncNames::Standby | SyncNames::Both => N::Standby,
				SyncNames::Off => N::Off,
				SyncNames::Fenced => N::Fenced,
			},
			standby: standby.filter(|_| names != SyncNames::Both).map(|s| S {
				reply_age_ms: s.reply_age_ms,
				sync: s.sync,
				flush_lsn: s.flush_lsn,
			}),
			lsn,
			archive_fenced,
		},
	}
}

/// xorshift64*: the simulator needs no more than this, and a seed replays a run exactly.
struct Rng(u64);

impl Rng {
	fn next(&mut self) -> u64 {
		self.0 ^= self.0 >> 12;
		self.0 ^= self.0 << 25;
		self.0 ^= self.0 >> 27;
		self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
	}

	fn chance(&mut self, per_million: u64) -> bool {
		self.next() % 1_000_000 < per_million
	}
}

/// A move of the pair between witnesses, as the control plane steps it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MoveStep {
	Freeze,
	Adopt,
	Redirect,
	Repoint,
	Release,
}

struct World {
	now: u64,
	copies: [Copy; N],
	witnesses: [Witness; 2],
	/// links[x][y]: the link between nodes x and y works (copies 0..N, witnesses N..).
	links: [[bool; NODES]; NODES],
	next_id: u64,
	acked: Vec<u64>,
	grants: u32,
	/// Grants while the pair's guards and its witness were not all on one image.
	mixed_grants: u32,
	handovers: u32,
	replaces: u32,
	moves: u32,
	requests: u64,
	moving: Option<(usize, usize, MoveStep)>,
	/// The eligible standby last checked (its name, the epoch, its generation) and how many
	/// acknowledged commits it was checked for.
	checked: Option<(String, u64, u64)>,
	checked_upto: usize,
	trace: Vec<String>,
}

fn index(name: &str) -> usize {
	NAMES
		.iter()
		.position(|n| *n == name)
		.expect("a copy's name")
}

impl World {
	fn new() -> World {
		let copy = |role, present, peer| Copy {
			present,
			pg_up: present,
			guard_up: true,
			role,
			wal: Vec::new(),
			set: HashSet::new(),
			generation: 0,
			replay: 0,
			replay_per_tick: usize::MAX,
			sync: if role == Role::Primary {
				Sync::Named(vec![1])
			} else {
				Sync::Off
			},
			pending: Vec::new(),
			mem: Memory {
				epoch: 1,
				sync_from_lsn: Some(0),
				..Memory::default()
			},
			follows: (role == Role::Standby).then_some(0),
			last_receive: None,
			replies: [None; N],
			ended_until: 0,
			archive_fenced: false,
			cfg_peer: peer,
			cfg_witness: 0,
			witness: 0,
			sent: None,
			kept_standby: None,
			lose_answer: false,
			old: false,
			old_mem: old::decide::Memory {
				epoch: 1,
				sync_from_lsn: Some(0),
				..old::decide::Memory::default()
			},
			state: json!({"epoch": 1}),
		};
		let cluster = Cluster::new("a", Some("b"), T, 0);
		World {
			now: 0,
			copies: [
				copy(Role::Primary, true, 1),
				copy(Role::Standby, true, 0),
				copy(Role::Standby, false, 0),
			],
			witnesses: [Witness::holding(Some(cluster)), Witness::holding(None)],
			links: [[true; NODES]; NODES],
			next_id: 0,
			acked: Vec::new(),
			grants: 0,
			mixed_grants: 0,
			handovers: 0,
			replaces: 0,
			moves: 0,
			requests: 0,
			moving: None,
			checked: None,
			checked_upto: 0,
			trace: Vec::new(),
		}
	}

	fn link(&self, x: usize, y: usize) -> bool {
		self.links[x][y]
	}

	fn cut(&mut self, x: usize, y: usize, up: bool) {
		self.links[x][y] = up;
		self.links[y][x] = up;
	}

	/// The witness whose word stands: the one holding the pair and not pointing elsewhere.
	fn authority(&self) -> usize {
		match self.witnesses[0].file_view.as_ref().map(|c| &c.moving) {
			Some(Some(Moving::Moved(_))) | None => 1,
			_ => 0,
		}
	}

	/// The pair as the witness of record holds it (its state file while it is down).
	fn pair(&self) -> &Cluster {
		let w = &self.witnesses[self.authority()];
		w.cluster
			.as_ref()
			.filter(|_| w.up)
			.or(w.file_view.as_ref())
			.expect("the witness of record holds the pair")
	}

	/// The witness `w`, started again as `old` or not.
	fn start_witness(&mut self, w: usize, old: bool) {
		let now = self.now;
		self.witnesses[w].start(old, now);
		if self.witnesses[w].broken {
			self.log(format!(
				"{} {} cannot read its state file and stays down",
				WITNESSES[w],
				if old { "(old)" } else { "(new)" }
			));
		}
	}

	fn log(&mut self, s: String) {
		self.trace.push(format!("{:>7} {s}", self.now));
		if self.trace.len() > 80 {
			self.trace.remove(0);
		}
	}

	fn fail(&self, what: String) -> ! {
		panic!("{what}\n{}", self.trace.join("\n"))
	}

	/// Every acknowledged commit is on `i`.
	fn holds_every_ack(&self, i: usize) -> bool {
		let set = &self.copies[i].set;
		self.acked.iter().all(|id| set.contains(id))
	}

	/// The guard started (again), its version reading the state file: one it cannot read keeps it
	/// down (an old guard on a copy a new one demoted, or left mid-step: it refuses to start).
	fn restart_guard(&mut self, i: usize) {
		let c = &mut self.copies[i];
		let sync_from = matches!(c.sync, Sync::Named(_)).then_some(c.wal.len() as u64);
		// A redirect is followed in memory only: the definition's witness again.
		c.witness = c.cfg_witness;
		if c.old {
			let Some(epoch) = old::read_epoch(&c.state) else {
				c.guard_up = false;
				self.log(format!(
					"{}'s old guard cannot read its state file and does not start",
					NAMES[i]
				));
				return;
			};
			c.guard_up = true;
			c.old_mem = old::decide::Memory {
				epoch,
				sync_from_lsn: sync_from,
				..old::decide::Memory::default()
			};
			return;
		}
		let Some(state) = GuardState::from_json(&c.state) else {
			c.guard_up = false;
			return;
		};
		c.guard_up = true;
		// What is on disk; calls numbered from 1 again, so any lease since tells how a step went.
		let change = state.change().map(|sent| match sent {
			Change::HandoverSent { id, .. } => Change::HandoverSent { id, from: 1 },
			Change::ReplaceSent { id, to, .. } => Change::ReplaceSent { id, to, from: 1 },
			other => other,
		});
		c.sent = change.clone();
		c.kept_standby = state.standby.clone();
		c.mem = Memory {
			epoch: state.epoch,
			demoted: state.demoted,
			standby: state.standby,
			change,
			sync_from_lsn: sync_from,
			..Memory::default()
		};
	}

	/// What a new guard keeps on disk, from its memory (the runner writes the same on each change).
	fn persist(&mut self, i: usize) {
		let c = &mut self.copies[i];
		if c.old {
			return;
		}
		let sent = c.sent.as_ref().and_then(|s| match s {
			Change::HandoverSent { id, .. } => {
				Some((RequestKind::Handover, id.clone(), String::new()))
			}
			Change::ReplaceSent { id, to, .. } => {
				Some((RequestKind::Replace, id.clone(), to.clone()))
			}
			_ => None,
		});
		c.state = GuardState {
			epoch: c.mem.epoch,
			demoted: c.mem.demoted,
			standby: c.kept_standby.clone(),
			sent,
			peer: Some(NAMES[c.cfg_peer].to_string()),
		}
		.to_json();
	}

	/// The epoch the guard holds, whichever version runs.
	fn epoch_of(&self, i: usize) -> u64 {
		let c = &self.copies[i];
		if c.old { c.old_mem.epoch } else { c.mem.epoch }
	}

	fn chaos(&mut self, r: &mut Rng) {
		for x in 0..NODES {
			for y in (x + 1)..NODES {
				if x >= N && y >= N {
					continue;
				}
				if r.chance(700) {
					let up = !self.links[x][y];
					self.cut(x, y, up);
					let name = |n: usize| if n < N { NAMES[n] } else { WITNESSES[n - N] };
					self.log(format!(
						"link {}-{} {}",
						name(x),
						name(y),
						if up { "up" } else { "cut" }
					));
				}
			}
		}
		for i in 0..N {
			if !self.copies[i].present {
				continue;
			}
			let c = &mut self.copies[i];
			if c.pg_up && r.chance(800) {
				c.pg_up = false;
				c.pending.clear();
				c.last_receive = None;
				c.replies = [None; N];
				self.log(format!("{} postgres crashed", NAMES[i]));
			} else if !c.pg_up && r.chance(5_000) {
				c.pg_up = true;
				self.log(format!("{} postgres back", NAMES[i]));
			}
			let c = &mut self.copies[i];
			if c.guard_up && r.chance(800) {
				c.guard_up = false;
				self.log(format!("{} guard crashed", NAMES[i]));
			} else if !c.guard_up && r.chance(5_000) {
				// Sometimes back on the other image: an upgrade, or a roll back.
				if r.chance(300_000) {
					c.old = !c.old;
				}
				let old = c.old;
				self.restart_guard(i);
				self.log(format!(
					"{} guard back{}",
					NAMES[i],
					if old { " (old)" } else { "" }
				));
			}
			let c = &mut self.copies[i];
			if r.chance(300) {
				c.replay_per_tick = if c.replay_per_tick == 0 {
					usize::MAX
				} else {
					0
				};
			}
			if r.chance(300) {
				c.lose_answer = true;
			}
		}
		for w in 0..2 {
			let wit = &mut self.witnesses[w];
			if wit.up && r.chance(400) {
				wit.up = false;
				self.log(format!("{} crashed", WITNESSES[w]));
			} else if !wit.up && r.chance(3_000) {
				let old = if r.chance(300_000) { !wit.old } else { wit.old };
				self.start_witness(w, old);
				if self.witnesses[w].up {
					self.log(format!(
						"{} back{}",
						WITNESSES[w],
						if old { " (old)" } else { "" }
					));
				}
			}
		}
	}

	fn replicate(&mut self) {
		for s in 0..N {
			let Some(p) = self.copies[s].follows else {
				continue;
			};
			let (src, dst) = (&self.copies[p], &self.copies[s]);
			let ok = dst.present
				&& self.link(p, s)
				&& src.pg_up && dst.pg_up
				&& src.role == Role::Primary
				&& dst.role == Role::Standby
				&& self.now >= src.ended_until
				&& follows(&src.wal, &dst.wal);
			if ok {
				// A few records a tick, so a standby that was away is behind for a while.
				let have = dst.wal.len();
				let upto = src.wal.len().min(have + STREAM_PER_TICK);
				let missing = src.wal[have..upto].to_vec();
				let now = self.now;
				let d = &mut self.copies[s];
				d.set.extend(missing.iter().copied());
				d.wal.extend(missing);
				d.replay = d.wal.len().min(d.replay.saturating_add(d.replay_per_tick));
				d.last_receive = Some(now);
				let (flushed, replayed) = (d.wal.len(), d.replay);
				self.copies[p].replies[s] = Some((now, flushed, replayed));
			}
		}
	}

	fn write_and_ack(&mut self) {
		for i in 0..N {
			let c = &self.copies[i];
			if c.pg_up && c.role == Role::Primary && c.sync != Sync::Fenced {
				self.next_id += 1;
				let id = self.next_id;
				let c = &mut self.copies[i];
				c.wal.push(id);
				c.set.insert(id);
				let lsn = c.wal.len();
				c.pending.push((id, lsn));
			}
			let c = &mut self.copies[i];
			let replies = c.replies;
			let mut done = Vec::new();
			c.pending.retain(|&(id, lsn)| {
				let ack = match &c.sync {
					Sync::Off => true,
					Sync::Named(v) => v
						.iter()
						.all(|s| replies[*s].is_some_and(|(_, f, _)| f >= lsn)),
					Sync::Fenced => false,
				};
				if ack {
					done.push(id);
				}
				!ack
			});
			if !done.is_empty() && self.pair().primary != NAMES[i] {
				self.fail(format!(
					"{} acknowledged commit {} while the witness's primary is {}",
					NAMES[i],
					done[0],
					self.pair().primary
				));
			}
			self.acked.extend(done);
		}
	}

	/// The copy `i`'s commits wait for: the witness's standby as it last said, else its definition's.
	fn peer(&self, i: usize) -> usize {
		if self.copies[i].old {
			return self.copies[i].cfg_peer;
		}
		self.copies[i]
			.mem
			.standby
			.as_deref()
			.map_or(self.copies[i].cfg_peer, index)
	}

	fn candidate(&self, i: usize) -> Option<usize> {
		if self.copies[i].old {
			return None;
		}
		let m = &self.copies[i].mem;
		match &m.change {
			Some(Change::Both { to, .. }) | Some(Change::ReplaceSent { to, .. }) => Some(index(to)),
			_ => m
				.request
				.as_ref()
				.filter(|r| r.kind == RequestKind::Replace)
				.map(|r| index(&r.to)),
		}
	}

	fn seen(&self, i: usize, s: usize) -> Option<StandbySeen> {
		let (c, other) = (&self.copies[i], &self.copies[s]);
		let streams = self.link(i, s)
			&& other.present
			&& other.pg_up
			&& other.role == Role::Standby
			&& other.follows == Some(i)
			&& self.now >= c.ended_until;
		let named = matches!(&c.sync, Sync::Named(v) if v.contains(&s));
		c.replies[s]
			.filter(|_| streams)
			.map(|(t, flush, replay)| StandbySeen {
				reply_age_ms: self.now - t,
				sync: named,
				flush_lsn: flush as u64,
				replay_lsn: replay as u64,
			})
	}

	fn facts(&self, i: usize) -> Postgres {
		let c = &self.copies[i];
		if !c.pg_up {
			return Postgres::Down;
		}
		match c.role {
			Role::Standby => Postgres::Standby {
				receive_age_ms: c.last_receive.map(|t| self.now - t),
			},
			Role::Primary => {
				let peer = self.peer(i);
				// As the runner reads the setting: names it did not write are the standby alone, gone.
				let (names, known) = match &c.sync {
					Sync::Off => (SyncNames::Off, true),
					Sync::Fenced => (SyncNames::Fenced, true),
					Sync::Named(v) if *v == [peer] => (SyncNames::Standby, true),
					Sync::Named(v) if v.len() == 2 && v.contains(&peer) => (SyncNames::Both, true),
					Sync::Named(_) => (SyncNames::Standby, false),
				};
				Postgres::Primary {
					names,
					standby: if known { self.seen(i, peer) } else { None },
					candidate: self
						.candidate(i)
						.filter(|x| *x != peer && *x != i)
						.and_then(|x| self.seen(i, x)),
					lsn: c.wal.len() as u64,
					archive_fenced: Some(c.archive_fenced),
				}
			}
		}
	}

	/// One call from copy `i`'s guard to the witness it asks, followed once to where it moved.
	fn call(&mut self, i: usize, call: Call) -> Option<Answer> {
		let primary = self.copies[i].role == Role::Primary;
		let now = self.now;
		if self.copies[i].old {
			// The old guard reads the answer as it did: one it does not know is no answer at all.
			let json = self.exchange(i, &call)?;
			let answer = old::witness::Answer::from_json(&json)?;
			let c = &mut self.copies[i];
			c.old_mem.heard(now, primary, &answer);
			// It keeps an epoch it is granted, or learns as a standby, in the file it writes.
			match &answer {
				old::witness::Answer::Granted { epoch } => c.state = json!({ "epoch": epoch }),
				old::witness::Answer::Demoted { epoch, .. } if !primary => {
					c.state = json!({ "epoch": epoch })
				}
				_ => {}
			}
			return Answer::from_json(&json);
		}
		self.copies[i].mem.next_call();
		let mut answer = Answer::from_json(&self.exchange(i, &call)?)?;
		if let Answer::Moved { witness } = &answer {
			let to = WITNESSES.iter().position(|w| w == witness)?;
			self.copies[i].witness = to;
			self.log(format!("{} follows the pair to {witness}", NAMES[i]));
			answer = Answer::from_json(&self.exchange(i, &call)?)?;
			if matches!(answer, Answer::Moved { .. }) {
				return None;
			}
		}
		let c = &mut self.copies[i];
		c.mem.heard(now, primary, &answer);
		if primary && c.mem.standby.is_some() {
			c.kept_standby = c.mem.standby.clone();
		}
		if let Answer::Granted { .. } = answer {
			c.kept_standby = None;
		}
		self.persist(i);
		Some(answer)
	}

	fn exchange(&mut self, i: usize, call: &Call) -> Option<Value> {
		let w = self.copies[i].witness;
		if !self.witnesses[w].up || !self.link(i, N + w) {
			return None;
		}
		let now = self.now;
		let answer = self.witnesses[w].handle(call, now)?;
		let changes = matches!(
			call,
			Call::Handover { .. } | Call::Replace { .. } | Call::Abandon { .. }
		);
		if changes && std::mem::take(&mut self.copies[i].lose_answer) {
			return None;
		}
		Some(answer)
	}

	fn guards(&mut self) {
		for i in 0..N {
			if !self.copies[i].guard_up || !self.copies[i].present {
				continue;
			}
			let pg = self.facts(i);
			// A copy rebuilt as a standby: what it kept as a primary is spent (the runner clears it,
			// so an old guard can start on it again).
			if !self.copies[i].old && matches!(pg, Postgres::Standby { .. }) {
				let c = &mut self.copies[i];
				c.mem.demoted = false;
				if matches!(
					c.mem.change,
					Some(Change::HandoverSent { .. } | Change::ReplaceSent { .. })
				) {
					c.mem.change = None;
				}
			}
			// In the runner's order: what acts on the copy first, then the calls.
			let actions = if self.copies[i].old {
				old::decide::decide(self.now, T, old_facts(pg), &self.copies[i].old_mem)
					.into_iter()
					.map(old_action)
					.collect()
			} else {
				decide(self.now, T, pg, &self.copies[i].mem)
			};
			for action in in_tick_order(actions) {
				self.act(i, action, pg);
			}
			if !self.copies[i].mem.change.as_ref().is_some_and(|c| {
				matches!(c, Change::HandoverSent { .. } | Change::ReplaceSent { .. })
			}) {
				self.copies[i].sent = None;
			}
			self.persist(i);
			// The archive is guarded in the tick that fences, never a tick later.
			let c = &self.copies[i];
			if c.role == Role::Primary && c.sync == Sync::Fenced && !c.archive_fenced {
				self.fail(format!("{} fenced with its archive open", NAMES[i]));
			}
		}
	}

	fn fence(&mut self, i: usize) {
		let c = &mut self.copies[i];
		c.sync = Sync::Fenced;
		c.pending.clear();
	}

	fn act(&mut self, i: usize, action: Action, pg: Postgres) {
		let (copy, epoch) = (NAMES[i].to_string(), self.epoch_of(i));
		match action {
			Action::Renew => {
				self.call(i, Call::Renew { copy, epoch });
			}
			Action::Eligible(eligible) => {
				// A new guard names whom it vouches for; an old one never did.
				let standby =
					(eligible && !self.copies[i].old).then(|| NAMES[self.peer(i)].to_string());
				self.call(
					i,
					Call::Eligible {
						copy,
						epoch,
						eligible,
						standby,
					},
				);
			}
			Action::Ask => {
				let lost = match pg {
					Postgres::Standby { receive_age_ms } => receive_age_ms.unwrap_or(u64::MAX),
					_ => 0,
				};
				let a = self.call(
					i,
					Call::Ask {
						copy,
						epoch,
						stream_lost_ms: lost,
					},
				);
				if let Some(Answer::Granted { epoch }) = a
					&& self.copies[i].role == Role::Standby
				{
					// Every other primary still up can acknowledge nothing more: fenced, or waiting for
					// the copy just granted, whose stream ends with its promotion (a primary that lost an
					// answer keeps both of a replacement's standbys named, the granted one among them).
					for o in 0..N {
						let old = &self.copies[o];
						let can_ack = match &old.sync {
							Sync::Fenced => false,
							Sync::Off => true,
							Sync::Named(v) => !v.contains(&i),
						};
						if o != i
							&& old.present && old.pg_up
							&& old.guard_up && old.role == Role::Primary
							&& can_ack
						{
							self.fail(format!(
								"{} granted while {} still serves",
								NAMES[i], NAMES[o]
							));
						}
					}
					self.grants += 1;
					let olds = self.copies.iter().filter(|c| c.present && c.old).count()
						+ self.witnesses.iter().filter(|w| w.up && w.old).count();
					let ends = self.copies.iter().filter(|c| c.present).count()
						+ self.witnesses.iter().filter(|w| w.up).count();
					if olds > 0 && olds < ends {
						self.mixed_grants += 1;
					}
					self.log(format!("{} granted", NAMES[i]));
					// Acted on in the same tick, as the runner does.
					self.act(i, Action::Promote(epoch), pg);
				}
			}
			Action::GoAsync => {
				let c = &self.copies[i];
				let eligible = if c.old {
					c.old_mem.eligible
				} else {
					c.mem.eligible
				};
				if eligible != Some(false) {
					self.fail(format!("{} went async with eligible not cleared", NAMES[i]));
				}
				self.copies[i].sync = Sync::Off;
				self.log(format!("{} async", NAMES[i]));
			}
			Action::GoSync => {
				let peer = self.peer(i);
				let c = &mut self.copies[i];
				c.sync = Sync::Named(vec![peer]);
				c.mem.sync_from_lsn = Some(c.wal.len() as u64);
				c.old_mem.sync_from_lsn = Some(c.wal.len() as u64);
				self.log(format!("{} sync with {}", NAMES[i], NAMES[peer]));
			}
			Action::Fence => {
				self.fence(i);
				self.log(format!("{} fenced", NAMES[i]));
			}
			Action::Unfence => {
				let peer = self.peer(i);
				let c = &mut self.copies[i];
				c.sync = match &c.mem.change {
					Some(Change::ReplaceSent { to, .. }) if !c.old => {
						Sync::Named(vec![peer, index(to)])
					}
					_ => Sync::Named(vec![peer]),
				};
				c.mem.sync_from_lsn = Some(c.wal.len() as u64);
				c.old_mem.sync_from_lsn = Some(c.wal.len() as u64);
				self.log(format!("{} unfenced", NAMES[i]));
			}
			Action::FenceArchive => self.copies[i].archive_fenced = true,
			Action::UnfenceArchive => self.copies[i].archive_fenced = false,
			Action::Promote(epoch) => {
				let c = &mut self.copies[i];
				if c.role == Role::Standby {
					// Replay finishes everything received first.
					c.replay = c.wal.len();
				}
				c.sync = Sync::Off;
				c.archive_fenced = false;
				c.role = Role::Primary;
				c.follows = None;
				c.last_receive = None;
				c.replies = [None; N];
				c.sent = None;
				c.kept_standby = None;
				c.mem = Memory::promoted(epoch, self.now);
				c.old_mem = old::decide::Memory::promoted(epoch, self.now);
				self.log(format!("{} promoted at epoch {epoch}", NAMES[i]));
			}
			Action::HandoverFence => {
				let Some(id) = self.copies[i].mem.request.as_ref().map(|r| r.id.clone()) else {
					return;
				};
				self.fence(i);
				self.copies[i].mem.change = Some(Change::Fencing {
					id,
					since: self.now,
				});
				self.log(format!("{} fenced for a handover", NAMES[i]));
			}
			Action::Handover => {
				let id = match &self.copies[i].mem.change {
					Some(Change::Fencing { id, .. }) | Some(Change::HandoverSent { id, .. }) => {
						id.clone()
					}
					_ => return,
				};
				let c = &mut self.copies[i];
				let change = Change::HandoverSent {
					id: id.clone(),
					from: c.mem.calls + 2,
				};
				c.sent = Some(change.clone());
				c.mem.change = Some(change);
				let before = self.pair().epoch;
				let a = self.call(i, Call::Handover { copy, epoch, id });
				if self.pair().epoch > before {
					let to = index(&self.pair().primary);
					if !self.holds_every_ack(to) {
						self.fail(format!(
							"handed over to {} without every acknowledged commit",
							NAMES[to]
						));
					}
					self.handovers += 1;
					self.log(format!("{} handed over to {}", NAMES[i], NAMES[to]));
				}
				if let Some(Answer::Refused { .. }) = a {
					self.copies[i].mem.change = None;
				}
			}
			Action::GoBoth => {
				let Some(r) = self.copies[i].mem.request.clone() else {
					return;
				};
				let peer = self.peer(i);
				let to = index(&r.to);
				let c = &mut self.copies[i];
				c.sync = Sync::Named(vec![peer, to]);
				c.mem.candidate_from_lsn = Some(c.wal.len() as u64);
				c.mem.change = Some(Change::Both {
					id: r.id,
					to: r.to,
					since: self.now,
				});
				self.log(format!(
					"{} waits for {} and {}",
					NAMES[i], NAMES[peer], NAMES[to]
				));
			}
			Action::Replace => {
				let (id, to) = match &self.copies[i].mem.change {
					Some(Change::Both { id, to, .. })
					| Some(Change::ReplaceSent { id, to, .. }) => (id.clone(), to.clone()),
					_ => return,
				};
				let c = &mut self.copies[i];
				let change = Change::ReplaceSent {
					id: id.clone(),
					to: to.clone(),
					from: c.mem.calls + 1,
				};
				c.sent = Some(change.clone());
				c.mem.change = Some(change);
				let before = self.pair().standby.clone();
				let a = self.call(i, Call::Replace { copy, epoch, id });
				if self.pair().standby != before
					&& self.pair().standby.as_deref() == Some(to.as_str())
				{
					if !self.holds_every_ack(index(&to)) {
						self.fail(format!(
							"{to} made the standby without every acknowledged commit"
						));
					}
					self.replaces += 1;
					self.log(format!("{} replaced its standby with {to}", NAMES[i]));
				}
				if let Some(Answer::Refused { .. }) = a {
					self.copies[i].mem.change = None;
				}
			}
			Action::Abandon(reason) => {
				let Some(id) = self.copies[i].mem.request.as_ref().map(|r| r.id.clone()) else {
					return;
				};
				self.copies[i].mem.change = Some(Change::GivingUp {
					id: id.clone(),
					reason,
				});
				self.log(format!("{} gives up {id}: {}", NAMES[i], reason.text()));
				self.call(
					i,
					Call::Abandon {
						copy,
						epoch,
						id,
						reason: reason.text().into(),
					},
				);
			}
			Action::Resolved => self.copies[i].mem.change = None,
			Action::EndStreams => {
				let now = self.now;
				self.copies[i].ended_until = now + 5_000;
				for s in 0..N {
					if self.copies[s].follows == Some(i) && self.copies[s].role == Role::Standby {
						self.copies[s].last_receive = None;
					}
				}
				self.log(format!("{} ends its standby's stream", NAMES[i]));
			}
		}
	}

	/// Makes `s` a standby of `p` from `p`'s data.
	fn rebuild(&mut self, s: usize, p: usize) {
		let (wal, set, w) = (
			self.copies[p].wal.clone(),
			self.copies[p].set.clone(),
			self.copies[p].cfg_witness,
		);
		for c in &mut self.copies {
			c.replies[s] = None;
		}
		let t = &mut self.copies[s];
		t.present = true;
		t.pg_up = true;
		t.guard_up = true;
		t.role = Role::Standby;
		t.replay = wal.len();
		t.wal = wal;
		t.set = set;
		t.generation += 1;
		t.sync = Sync::Off;
		t.pending.clear();
		t.follows = Some(p);
		t.last_receive = None;
		t.ended_until = 0;
		t.archive_fenced = false;
		t.cfg_peer = p;
		t.cfg_witness = w;
		// Its guard is made again with the pod, reading the state file its directory kept.
		self.restart_guard(s);
		self.log(format!("{} rebuilt from {}", NAMES[s], NAMES[p]));
	}

	fn operator(&mut self, r: Option<&mut Rng>) {
		let a = self.authority();
		if !self.witnesses[a].up {
			return;
		}
		let pair = self.pair().clone();
		let p = index(&pair.primary);
		let replacing = pair
			.request
			.as_ref()
			.filter(|r| r.kind == RequestKind::Replace)
			.map(|r| index(&r.to));
		// The standby, made again when it is gone, a primary, or not streaming from the primary.
		if let Some(s) = pair.standby.as_deref().map(index) {
			let (src, dst) = (&self.copies[p], &self.copies[s]);
			let replaced = !dst.present
				|| !dst.pg_up
				|| dst.role == Role::Primary
				|| dst.follows != Some(p)
				|| !follows(&src.wal, &dst.wal);
			if replaced && src.pg_up && src.role == Role::Primary && self.link(p, s) {
				self.rebuild(s, p);
			}
		}
		// A replacement's copy, made when it is asked for.
		if let Some(x) = replacing
			&& !self.copies[x].present
			&& self.copies[p].pg_up
			&& self.copies[p].role == Role::Primary
		{
			self.rebuild(x, p);
		}
		// A copy the pair no longer holds, removed.
		for x in 0..N {
			let held = NAMES[x] == pair.primary
				|| pair.standby.as_deref() == Some(NAMES[x])
				|| replacing == Some(x);
			if !held && self.copies[x].present {
				let c = &mut self.copies[x];
				c.present = false;
				c.pg_up = false;
				c.pending.clear();
				c.follows = None;
				self.log(format!("{} removed", NAMES[x]));
			}
		}
		// The primary's definition names its standby (a changed definition restarts the guard).
		if let Some(s) = pair.standby.as_deref().map(index)
			&& self.copies[p].cfg_peer != s
			&& self.copies[p].present
		{
			self.copies[p].cfg_peer = s;
			if self.copies[p].guard_up {
				self.restart_guard(p);
			}
			self.log(format!(
				"{}'s guard defined with peer {}",
				NAMES[p], NAMES[s]
			));
		}
		self.step_move();
		let Some(r) = r else {
			return;
		};
		if self.moving.is_none() && pair.request.is_none() && pair.moving.is_none() {
			if r.chance(150_000) {
				self.ask(RequestKind::Handover, pair.standby.as_deref().map(index));
			} else if r.chance(150_000) {
				let spare = (0..N).find(|x| {
					NAMES[*x] != pair.primary && pair.standby.as_deref() != Some(NAMES[*x])
				});
				self.ask(RequestKind::Replace, spare);
			} else if r.chance(60_000)
				&& self.witnesses[1 - a].up
				&& !self.witnesses[a].old
				&& self.copies.iter().all(|c| !c.present || !c.old)
			{
				// The control plane's gate: both guards say the witness they ask (new ones do), and the
				// witness says `move`. A target that turns out old is caught at the adopt.
				self.moving = Some((a, 1 - a, MoveStep::Freeze));
				self.log(format!(
					"moving the pair from {} to {}",
					WITNESSES[a],
					WITNESSES[1 - a]
				));
			}
		}
	}

	/// Asks the witness of record for a change; refused ones are ignored, as the control plane's are.
	fn ask(&mut self, kind: RequestKind, to: Option<usize>) -> bool {
		let Some(to) = to else {
			return false;
		};
		self.requests += 1;
		let id = format!("r{}", self.requests);
		let (a, now) = (self.authority(), self.now);
		let w = &mut self.witnesses[a];
		// A witness from before requests has no such verb (a 404): nothing is asked.
		if !w.up || w.old {
			return false;
		}
		let Some(c) = w.cluster.as_mut() else {
			return false;
		};
		let asked = c.ask_for(
			Request {
				id: id.clone(),
				kind,
				to: NAMES[to].into(),
			},
			now,
		);
		if asked == Ok(true) {
			w.save();
			self.log(format!("asked {id}: {} to {}", kind.as_str(), NAMES[to]));
			return true;
		}
		false
	}

	/// One step of a move between witnesses, as each witness's host would take it.
	fn step_move(&mut self) {
		let Some((from, to, step)) = self.moving else {
			return;
		};
		if !self.witnesses[from].up || !self.witnesses[to].up {
			return;
		}
		let next =
			match step {
				MoveStep::Freeze => {
					// A source from before moves has no `freeze` (a 404): the move never starts.
					if self.witnesses[from].old {
						self.moving = None;
						self.log("move given up: the witness cannot be frozen (old)".into());
						return;
					}
					let w = &mut self.witnesses[from];
					let c = w.cluster.as_mut().expect("the pair is there");
					c.freeze();
					w.save();
					MoveStep::Adopt
				}
				MoveStep::Adopt => {
					// A target that does not say `move` (its health's features) is never handed the pair:
					// the source is thawed and the move given up.
					if self.witnesses[to].old || self.witnesses[from].old {
						if !self.witnesses[from].old {
							let w = &mut self.witnesses[from];
							w.cluster
								.as_mut()
								.expect("frozen there")
								.thaw()
								.expect("frozen, not moved");
							w.save();
						}
						self.moving = None;
						self.log("move given up: the new witness does not take a pair over (old); thawed".into());
						return;
					}
					let frozen = self.witnesses[from].cluster.clone().expect("frozen there");
					let w = &mut self.witnesses[to];
					w.cluster = Some(Cluster::adopt(&frozen, T));
					w.save();
					MoveStep::Redirect
				}
				MoveStep::Redirect => {
					let w = &mut self.witnesses[from];
					let c = w.cluster.as_mut().expect("frozen there");
					c.moved(WITNESSES[to]).expect("frozen first");
					w.save();
					MoveStep::Repoint
				}
				MoveStep::Repoint => {
					let mut all = true;
					for i in 0..N {
						let c = &self.copies[i];
						if !c.present {
							continue;
						}
						if c.cfg_witness != to {
							self.copies[i].cfg_witness = to;
							if self.copies[i].guard_up {
								self.restart_guard(i);
							}
						}
						all &= self.copies[i].witness == to || !self.copies[i].guard_up;
					}
					if all {
						MoveStep::Release
					} else {
						MoveStep::Repoint
					}
				}
				MoveStep::Release => {
					let w = &mut self.witnesses[from];
					w.cluster = None;
					w.file = None;
					w.file_view = None;
					self.moving = None;
					self.moves += 1;
					self.log(format!("the pair moved to {}", WITNESSES[to]));
					return;
				}
			};
		self.log(format!("move: {step:?} done"));
		self.moving = Some((from, to, next));
	}

	/// The witness's eligible standby has every acknowledged commit, at every moment: checked in
	/// full when it changes, and for each new acknowledgement after.
	fn check_eligible(&mut self) {
		let pair = self.pair();
		let Some(s) = pair
			.standby
			.clone()
			.filter(|_| pair.eligible && !pair.accept_loss)
		else {
			self.checked = None;
			return;
		};
		let i = index(&s);
		let key = (s.clone(), pair.epoch, self.copies[i].generation);
		let from = if self.checked.as_ref() == Some(&key) {
			self.checked_upto
		} else {
			0
		};
		let set = &self.copies[i].set;
		if let Some(id) = self.acked[from..].iter().find(|id| !set.contains(id)) {
			self.fail(format!(
				"{s} is the eligible standby without acknowledged commit {id}"
			));
		}
		self.checked = Some(key);
		self.checked_upto = self.acked.len();
	}

	fn tick(&mut self, mut r: Option<&mut Rng>) {
		self.now += TICK;
		if let Some(r) = r.as_deref_mut() {
			self.chaos(r);
		}
		self.replicate();
		self.write_and_ack();
		self.guards();
		self.check_eligible();
		if self.now.is_multiple_of(5_000) {
			self.operator(r);
		}
	}

	fn heal(&mut self) {
		self.links = [[true; NODES]; NODES];
		// A witness that could not read its file is rolled forward, as an operator would.
		for w in 0..2 {
			if !self.witnesses[w].up {
				let old = self.witnesses[w].old && !self.witnesses[w].broken;
				self.start_witness(w, old);
				if !self.witnesses[w].up {
					self.start_witness(w, false);
				}
			}
		}
		for i in 0..N {
			let c = &mut self.copies[i];
			c.replay_per_tick = usize::MAX;
			c.lose_answer = false;
			if c.present {
				c.pg_up = true;
				if !c.guard_up {
					self.restart_guard(i);
				}
				// A guard that would not start on what a newer one left is rolled forward too.
				if !self.copies[i].guard_up {
					self.copies[i].old = false;
					self.restart_guard(i);
				}
			}
		}
		self.log("healed".into());
	}

	/// Ticks until `done` holds, failing past `limit` ms.
	fn until(&mut self, limit: u64, what: &str, done: impl Fn(&World) -> bool) {
		let start = self.now;
		while !done(self) {
			self.tick(None);
			if self.now - start > limit {
				self.fail(format!("not {what} within {limit} ms"));
			}
		}
	}

	fn run_for(&mut self, ms: u64) {
		for _ in 0..(ms / TICK) {
			self.tick(None);
		}
	}

	/// The final checks: every acknowledged commit on the witness's primary, and writes going on.
	fn settle(&mut self, what: &str) {
		self.heal();
		let before = self.acked.len();
		self.run_for(6 * T);
		// A standby that was away long streams back at two records a tick net of the writes, and
		// commits wait for it: given the time it needs, never forever.
		for _ in 0..(30 * T / TICK) {
			if self.acked.len() > before {
				break;
			}
			self.tick(None);
		}
		let p = index(&self.pair().primary);
		if !self.holds_every_ack(p) {
			self.fail(format!(
				"{what}: an acknowledged commit is not on the primary {}",
				NAMES[p]
			));
		}
		if self.acked.len() <= before {
			self.fail(format!("{what}: nothing commits once everything is back"));
		}
	}
}

/// Whether `standby` is a prefix of `primary`. Every id is written once, on one copy, and travels
/// only as part of a prefix, so two histories that agree at the standby's last position agree up
/// to it.
fn follows(primary: &[u64], standby: &[u64]) -> bool {
	standby.len() <= primary.len()
		&& standby
			.last()
			.is_none_or(|l| primary[standby.len() - 1] == *l)
}

fn run(seed: u64, ticks: u64) -> World {
	let mut r = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
	let mut w = World::new();
	// Each guard and each witness on the old image or the new, as a pinned image rolls out; chaos
	// flips them as they restart.
	for i in 0..N {
		w.copies[i].old = r.chance(400_000);
	}
	for k in 0..2 {
		let old = r.chance(400_000);
		w.start_witness(k, old);
	}
	for _ in 0..ticks {
		w.tick(Some(&mut r));
	}
	w.settle(&format!("seed {seed}"));
	w
}

#[test]
fn no_acknowledged_commit_is_lost_and_only_the_primary_acknowledges() {
	let (mut grants, mut handovers, mut replaces, mut moves, mut mixed_grants) = (0, 0, 0, 0, 0);
	for seed in 0..300 {
		let w = run(seed, 30_000);
		mixed_grants += w.mixed_grants;
		grants += w.grants;
		handovers += w.handovers;
		replaces += w.replaces;
		moves += w.moves;
	}
	// The runs must exercise the failover and each change, not only survive by never trying them.
	assert!(grants > 300, "only {grants} grants in 300 runs");
	assert!(handovers > 100, "only {handovers} handovers in 300 runs");
	assert!(replaces > 100, "only {replaces} replacements in 300 runs");
	assert!(moves > 50, "only {moves} witness moves in 300 runs");
	// And failovers with old and new ends in one pair, not only pairs of one image.
	assert!(
		mixed_grants > 100,
		"only {mixed_grants} grants in a mixed pair in 300 runs"
	);
}

/// A pair at rest: synchronous, vouched for, writing.
fn warm() -> World {
	let mut w = World::new();
	w.run_for(5_000);
	assert!(
		w.pair().eligible,
		"the standby is vouched for once synchronous"
	);
	w
}

#[test]
fn a_dead_primary_is_replaced_within_two_leases() {
	let mut w = warm();
	w.copies[0].pg_up = false;
	w.until(2 * T + 1_000, "promoted", |w| {
		w.copies[1].role == Role::Primary
	});
}

#[test]
fn a_cut_primary_fences_itself_before_the_standby_is_promoted() {
	let mut w = warm();
	w.cut(0, 1, false);
	w.cut(0, N, false);
	w.until(3 * T, "promoted", |w| w.copies[1].role == Role::Primary);
	assert_eq!(w.copies[0].sync, Sync::Fenced);
}

#[test]
fn with_the_standby_and_the_witness_gone_commits_wait() {
	let mut w = warm();
	w.copies[1].pg_up = false;
	w.witnesses[0].up = false;
	let before = w.acked.len();
	w.run_for(3 * T);
	assert_eq!(w.acked.len(), before);
	assert_eq!(w.copies[1].role, Role::Standby);
}

#[test]
fn the_witness_alone_gone_changes_nothing() {
	let mut w = warm();
	w.witnesses[0].up = false;
	let before = w.acked.len();
	w.run_for(3 * T);
	assert!(w.acked.len() > before + 200);
	assert_eq!(w.copies[0].sync, Sync::Named(vec![1]));
}

fn ended(w: &World) -> Option<(bool, String)> {
	w.pair().ended.as_ref().map(|e| (e.done, e.reason.clone()))
}

#[test]
fn a_switchover_swaps_the_roles_in_seconds_and_the_pair_is_whole_again() {
	let mut w = warm();
	assert!(w.ask(RequestKind::Handover, Some(1)));
	let asked = w.now;
	w.until(5_000, "handed over and promoted", |w| {
		w.copies[1].role == Role::Primary && w.pair().epoch == 2
	});
	assert!(w.now - asked < 5_000);
	assert_eq!(ended(&w), Some((true, "handed over".into())));
	assert!(w.copies[0].mem.demoted && w.copies[0].sync == Sync::Fenced);
	assert_eq!(w.handovers, 1);
	assert_eq!(w.grants, 1, "the new primary learned it from the witness");
	// The old primary rejoins as the new one's standby, and is vouched for again.
	w.until(4 * T, "whole again", |w| {
		w.pair().eligible && w.copies[0].role == Role::Standby && w.copies[0].follows == Some(1)
	});
	w.settle("switchover");
	assert_eq!(w.pair().primary, "b");
}

#[test]
fn a_switchover_whose_standby_falls_behind_stops_and_the_primary_keeps_its_place() {
	let mut w = warm();
	w.copies[1].replay_per_tick = 0;
	w.run_for(1_000);
	assert!(w.ask(RequestKind::Handover, Some(1)));
	w.until(3 * T, "given up", |w| w.pair().request.is_none());
	assert_eq!(
		ended(&w),
		Some((false, "the standby did not catch up in time".into()))
	);
	w.run_for(1_000);
	assert_eq!((w.pair().primary.as_str(), w.pair().epoch), ("a", 1));
	assert_eq!(w.copies[0].sync, Sync::Named(vec![1]));
	let before = w.acked.len();
	w.run_for(2_000);
	assert!(w.acked.len() > before, "writes go on");
	w.settle("behind");
}

#[test]
fn a_switchover_whose_standby_drops_mid_way_stops_and_commits_go_on() {
	let mut w = warm();
	assert!(w.ask(RequestKind::Handover, Some(1)));
	w.copies[1].replay_per_tick = 0;
	w.until(2_000, "fencing", |w| {
		matches!(w.copies[0].mem.change, Some(Change::Fencing { .. }))
	});
	w.cut(0, 1, false);
	w.until(2 * T, "given up", |w| w.pair().request.is_none());
	assert_eq!(
		ended(&w),
		Some((false, "the standby stopped streaming synchronously".into()))
	);
	let before = w.acked.len();
	w.run_for(T);
	assert!(
		w.acked.len() > before,
		"the primary goes on alone, its witness agreeing"
	);
	assert_eq!((w.pair().primary.as_str(), w.pair().epoch), ("a", 1));
	w.settle("standby dropped");
}

#[test]
fn a_switchover_that_loses_the_witness_before_the_step_stops_and_unfences() {
	let mut w = warm();
	assert!(w.ask(RequestKind::Handover, Some(1)));
	w.copies[1].replay_per_tick = 0;
	w.until(2_000, "fencing", |w| {
		matches!(w.copies[0].mem.change, Some(Change::Fencing { .. }))
	});
	w.cut(0, N, false);
	w.until(T, "unfenced", |w| w.copies[0].sync != Sync::Fenced);
	assert!(matches!(
		w.copies[0].mem.change,
		Some(Change::GivingUp { .. })
	));
	w.copies[1].replay_per_tick = usize::MAX;
	w.cut(0, N, true);
	w.until(T, "told", |w| w.pair().request.is_none());
	assert_eq!(
		ended(&w),
		Some((false, "the primary lost the witness".into()))
	);
	assert_eq!(w.pair().epoch, 1);
	w.settle("witness lost before the step");
}

#[test]
fn a_handover_whose_answer_is_lost_stays_fenced_until_the_witness_answers_even_across_a_restart() {
	let mut w = warm();
	assert!(w.ask(RequestKind::Handover, Some(1)));
	w.until(2_000, "fencing", |w| {
		matches!(w.copies[0].mem.change, Some(Change::Fencing { .. }))
	});
	// The witness takes the handover and the answer never arrives; then the witness is out of reach.
	w.copies[0].lose_answer = true;
	w.until(2_000, "sent", |w| w.pair().epoch == 2);
	w.cut(0, N, false);
	w.copies[0].guard_up = false;
	w.tick(None);
	w.restart_guard(0);
	assert!(matches!(
		w.copies[0].mem.change,
		Some(Change::HandoverSent { .. })
	));
	// Its standby streams and the lease would allow it: still fenced, nothing acknowledged by it.
	w.run_for(2 * T);
	assert_eq!(w.copies[0].sync, Sync::Fenced);
	assert_eq!(w.copies[1].role, Role::Standby, "nobody told it");
	w.cut(0, N, true);
	w.until(2 * T, "promoted", |w| w.copies[1].role == Role::Primary);
	assert!(w.copies[0].mem.demoted);
	w.settle("lost answer");
}

#[test]
fn a_new_primarys_guard_restarting_after_the_handover_still_promotes_it() {
	let mut w = warm();
	assert!(w.ask(RequestKind::Handover, Some(1)));
	w.copies[1].guard_up = false;
	w.until(5_000, "handed over", |w| w.pair().epoch == 2);
	w.run_for(1_000);
	assert_eq!(w.copies[1].role, Role::Standby);
	w.restart_guard(1);
	w.until(7_000, "promoted", |w| w.copies[1].role == Role::Primary);
	w.settle("standby's guard restarted");
}

#[test]
fn a_primary_dying_mid_switchover_fails_over_and_the_failover_ends_the_request() {
	let mut w = warm();
	assert!(w.ask(RequestKind::Handover, Some(1)));
	w.copies[1].replay_per_tick = 0;
	w.until(2_000, "fencing", |w| {
		matches!(w.copies[0].mem.change, Some(Change::Fencing { .. }))
	});
	w.copies[0].pg_up = false;
	w.copies[1].replay_per_tick = usize::MAX;
	w.until(2 * T + 1_000, "failed over", |w| {
		w.copies[1].role == Role::Primary
	});
	assert_eq!(ended(&w), Some((false, "the primary failed over".into())));
	w.settle("primary died mid-way");
}

#[test]
fn a_standby_dying_mid_switchover_stops_it_and_the_primary_goes_on_alone() {
	let mut w = warm();
	assert!(w.ask(RequestKind::Handover, Some(1)));
	w.copies[1].replay_per_tick = 0;
	w.until(2_000, "fencing", |w| {
		matches!(w.copies[0].mem.change, Some(Change::Fencing { .. }))
	});
	w.copies[1].pg_up = false;
	w.until(2 * T, "async", |w| w.copies[0].sync == Sync::Off);
	assert!(!w.pair().eligible && w.pair().epoch == 1);
	w.settle("standby died mid-way");
}

#[test]
fn a_replacement_makes_the_new_standby_synchronous_before_the_old_one_goes() {
	let mut w = warm();
	assert!(w.ask(RequestKind::Replace, Some(2)));
	w.until(4 * T, "replaced", |w| {
		w.pair().standby.as_deref() == Some("c")
	});
	assert!(
		w.pair().eligible,
		"eligible from the moment it is the standby"
	);
	assert_eq!(w.replaces, 1);
	w.until(2 * T, "the old standby removed", |w| !w.copies[1].present);
	w.until(T, "waiting for the new standby alone", |w| {
		w.copies[0].sync == Sync::Named(vec![2])
	});
	// And it is the one a failover goes to.
	w.copies[0].pg_up = false;
	w.until(2 * T + 1_000, "promoted", |w| {
		w.copies[2].role == Role::Primary
	});
	w.settle("replaced");
}

#[test]
fn a_replacement_whose_new_copy_drops_before_the_step_goes_back_to_the_old_standby() {
	let mut w = warm();
	assert!(w.ask(RequestKind::Replace, Some(2)));
	w.until(4 * T, "both named", |w| {
		matches!(w.copies[0].mem.change, Some(Change::Both { .. }))
	});
	w.cut(0, 2, false);
	w.until(2 * T, "given up", |w| w.pair().request.is_none());
	assert_eq!(
		ended(&w),
		Some((
			false,
			"the new standby did not stream and catch up in time".into()
		))
	);
	assert_eq!(w.pair().standby.as_deref(), Some("b"));
	assert!(w.pair().eligible);
	w.until(T, "the old standby alone", |w| {
		w.copies[0].sync == Sync::Named(vec![1])
	});
	w.settle("candidate dropped");
}

#[test]
fn a_replacement_whose_answer_is_lost_keeps_both_named_across_a_restart_until_it_is_known() {
	let mut w = warm();
	assert!(w.ask(RequestKind::Replace, Some(2)));
	w.until(4 * T, "both named", |w| {
		matches!(w.copies[0].mem.change, Some(Change::Both { .. }))
	});
	w.copies[0].lose_answer = true;
	w.until(4 * T, "taken", |w| w.pair().standby.as_deref() == Some("c"));
	// The new standby cut off at once: had the primary gone back to the old one alone, the old one
	// would acknowledge what the witness's standby never gets (`check_eligible`, every tick).
	w.cut(0, 2, false);
	w.copies[0].guard_up = false;
	w.tick(None);
	w.restart_guard(0);
	assert!(matches!(
		w.copies[0].mem.change,
		Some(Change::ReplaceSent { .. })
	));
	w.until(2 * T, "settled on the witness's standby", |w| {
		w.copies[0].mem.change.is_none() && w.copies[0].sync != Sync::Named(vec![1, 2])
	});
	assert_ne!(w.copies[0].sync, Sync::Named(vec![1]));
	w.cut(0, 2, true);
	w.until(2 * T, "the new standby alone", |w| {
		w.copies[0].sync == Sync::Named(vec![2])
	});
	w.settle("replacement answer lost");
}

#[test]
fn a_witness_moves_with_the_epoch_and_the_pair_never_without_one() {
	let mut w = warm();
	assert!(w.ask(RequestKind::Handover, Some(1)));
	w.until(5_000, "handed over", |w| w.pair().epoch == 2);
	w.run_for(4 * T);
	w.moving = Some((0, 1, MoveStep::Freeze));
	let before = w.acked.len();
	w.until(6 * T, "moved", |w| w.moving.is_none());
	assert!(
		w.acked.len() > before + 100,
		"writes go on through the move"
	);
	assert!(w.witnesses[0].cluster.is_none());
	assert_eq!((w.pair().epoch, w.pair().primary.as_str()), (2, "b"));
	assert!(
		w.copies
			.iter()
			.filter(|c| c.present)
			.all(|c| c.witness == 1)
	);
	// The moved witness decides the next failover.
	w.copies[1].pg_up = false;
	w.until(2 * T + 1_000, "failed over", |w| {
		w.copies[0].role == Role::Primary
	});
	assert_eq!(w.pair().epoch, 3);
	w.settle("moved");
}

#[test]
fn a_primary_dying_while_its_witness_moves_fails_over_at_the_new_witness() {
	let mut w = warm();
	w.moving = Some((0, 1, MoveStep::Freeze));
	w.until(4 * T, "redirected", |w| {
		matches!(w.moving, Some((_, _, MoveStep::Repoint)))
	});
	w.copies[0].pg_up = false;
	w.until(3 * T, "failed over", |w| w.copies[1].role == Role::Primary);
	assert_eq!(w.authority(), 1);
	w.settle("primary died in the move");
}

// Mixed versions (a pinned image rolling out): the old guard and witness (afde9ba1f) beside the new,
// every call, answer and state file crossing between them as JSON. The random runs above mix them
// and flip them on restarts; these pin each rule down.

#[test]
fn an_old_guard_reads_every_answer_a_new_witness_gives_or_takes_it_for_none() {
	use old::witness::Answer as Old;
	let lease = Answer::Lease {
		epoch: 3,
		lease_ms: 10_000,
		eligible: true,
		standby: Some("b".into()),
		request: Some(Request {
			id: "x".into(),
			kind: RequestKind::Handover,
			to: "b".into(),
		}),
	};
	assert_eq!(
		Old::from_json(&lease.to_json()),
		Some(Old::Lease {
			epoch: 3,
			lease_ms: 10_000,
			eligible: true
		}),
		"the new fields are ignored, the lease read as it always was"
	);
	for a in [
		Answer::Granted { epoch: 4 },
		Answer::Held { reason: "x".into() },
		Answer::Demoted {
			epoch: 4,
			primary: "b".into(),
		},
		Answer::Refused { reason: "x".into() },
	] {
		assert_eq!(
			Old::from_json(&a.to_json()).map(|o| o.to_json()),
			Some(a.to_json())
		);
	}
	// A redirect it cannot follow is no answer at all, as a silent witness is: it decides nothing on it.
	assert_eq!(
		Old::from_json(
			&Answer::Moved {
				witness: "10.0.0.9:7445".into()
			}
			.to_json()
		),
		None
	);
}

#[test]
fn an_old_witness_refuses_the_new_calls_and_reads_a_named_vouch_as_it_always_did() {
	use old::witness::Call as Old;
	for c in [
		Call::Handover {
			copy: "a".into(),
			epoch: 1,
			id: "x".into(),
		},
		Call::Replace {
			copy: "a".into(),
			epoch: 1,
			id: "x".into(),
		},
		Call::Abandon {
			copy: "a".into(),
			epoch: 1,
			id: "x".into(),
			reason: "r".into(),
		},
	] {
		assert_eq!(Old::from_json(&c.to_json()), None, "{c:?} is a 400 there");
	}
	let vouch = Call::Eligible {
		copy: "a".into(),
		epoch: 1,
		eligible: true,
		standby: Some("b".into()),
	};
	let mut cluster = old::witness::Cluster::new("a", Some("b"), T, 0);
	let read = Old::from_json(&vouch.to_json()).expect("read");
	assert!(matches!(
		cluster.handle(&read, 0).0,
		old::witness::Answer::Lease { eligible: true, .. }
	));
}

#[test]
fn a_new_witness_takes_a_vouch_only_for_its_standby_and_after_a_replacement_only_a_named_one() {
	let mut c = Cluster::new("a", Some("b"), T, 0);
	let vouch = |standby: Option<&str>| Call::Eligible {
		copy: "a".into(),
		epoch: 1,
		eligible: true,
		standby: standby.map(str::to_string),
	};
	assert!(matches!(
		c.handle(&vouch(Some("c")), 0).0,
		Answer::Refused { .. }
	));
	// An old guard names none: taken, as it always was.
	assert!(matches!(
		c.handle(&vouch(None), 0).0,
		Answer::Lease { eligible: true, .. }
	));
	c.ask_for(
		Request {
			id: "r".into(),
			kind: RequestKind::Replace,
			to: "c".into(),
		},
		0,
	)
	.unwrap();
	c.handle(
		&Call::Replace {
			copy: "a".into(),
			epoch: 1,
			id: "r".into(),
		},
		1,
	);
	c.handle(
		&Call::Eligible {
			copy: "a".into(),
			epoch: 1,
			eligible: false,
			standby: None,
		},
		2,
	);
	// An old guard's definition may still name b: it cannot say whom it vouches for, so it is refused.
	assert!(matches!(
		c.handle(&vouch(None), 3).0,
		Answer::Refused { .. }
	));
	assert!(matches!(
		c.handle(&vouch(Some("b")), 3).0,
		Answer::Refused { .. }
	));
	assert!(matches!(
		c.handle(&vouch(Some("c")), 3).0,
		Answer::Lease { eligible: true, .. }
	));
	assert!(c.replaced);
}

#[test]
fn state_files_cross_versions_only_where_the_older_one_cannot_misread_them() {
	// The witness's: an old witness reads a plain pair as the new one wrote it, and refuses to start
	// on one being moved or whose standby a replacement named.
	let mut c = Cluster::new("a", Some("b"), T, 0);
	c.eligible = true;
	c.ask_for(
		Request {
			id: "x".into(),
			kind: RequestKind::Handover,
			to: "b".into(),
		},
		0,
	)
	.unwrap();
	let back = old::witness::Cluster::from_json(&c.to_json(), 0).expect("a plain pair reads");
	assert_eq!(
		(
			back.epoch,
			back.primary.as_str(),
			back.standby.as_deref(),
			back.eligible
		),
		(1, "a", Some("b"), true)
	);
	for change in 0..3 {
		let mut held = c.clone();
		match change {
			0 => {
				held.freeze();
			}
			1 => {
				held.freeze();
				held.moved("10.0.0.9:7445").unwrap();
			}
			_ => held.replaced = true,
		}
		assert!(old::witness::Cluster::from_json(&held.to_json(), 0).is_none());
		assert_eq!(
			Cluster::from_json(&held.to_json(), 0).map(|h| h.epoch),
			Some(1)
		);
	}
	// An old witness's file, read by the new one.
	let older = old::witness::Cluster::new("a", Some("b"), T, 0);
	assert_eq!(
		Cluster::from_json(&older.to_json(), 0).map(|c| c.epoch),
		Some(1)
	);

	// The guard's: an old guard starts on a plain one (a standby the witness named, which its
	// definition names too, included), and refuses where it would misread.
	let state = |demoted, sent: bool, standby: &str| GuardState {
		epoch: 5,
		demoted,
		standby: Some(standby.into()),
		sent: sent.then(|| (RequestKind::Handover, "x".into(), String::new())),
		peer: Some("b".into()),
	};
	assert_eq!(
		old::read_epoch(&state(false, false, "b").to_json()),
		Some(5)
	);
	for refused in [
		state(true, false, "b"),
		state(false, true, "b"),
		state(false, false, "c"),
	] {
		assert_eq!(old::read_epoch(&refused.to_json()), None, "{refused:?}");
		assert_eq!(
			GuardState::from_json(&refused.to_json()).map(|s| s.epoch),
			Some(5)
		);
	}
	assert_eq!(
		GuardState::from_json(&json!({"epoch": 7})).map(|s| s.epoch),
		Some(7)
	);
}

/// A pair at rest on the given images: guards a, b and witness w0.
fn warm_on(a_old: bool, b_old: bool, witness_old: bool) -> World {
	let mut w = World::new();
	w.copies[0].old = a_old;
	w.copies[1].old = b_old;
	w.start_witness(0, witness_old);
	w.run_for(5_000);
	assert!(w.pair().eligible, "vouched for on any mix");
	w
}

#[test]
fn every_mix_of_old_and_new_ends_fails_over_and_keeps_its_epochs() {
	for mix in 0..8 {
		let (a, b, wit) = (mix & 1 == 1, mix & 2 == 2, mix & 4 == 4);
		let mut w = warm_on(a, b, wit);
		w.copies[0].pg_up = false;
		w.until(2 * T + 1_000, "promoted", |w| {
			w.copies[1].role == Role::Primary
		});
		assert_eq!(w.pair().epoch, 2, "mix {mix}");
		w.settle(&format!("mix {mix}"));
	}
}

#[test]
fn a_change_asked_of_a_pair_with_an_old_primary_guard_is_ignored_and_ends_at_the_witness() {
	// The control plane never asks one (both guards must say the witness they ask); if it did, the
	// old guard never reads the request, and the witness ends it itself.
	let mut w = warm_on(true, false, false);
	assert!(w.ask(RequestKind::Handover, Some(1)));
	w.until(8 * T, "ended", |w| w.pair().request.is_none());
	assert_eq!(
		ended(&w),
		Some((
			false,
			"the primary's guard did not carry it out in time".into()
		))
	);
	assert_eq!((w.pair().primary.as_str(), w.pair().epoch), ("a", 1));
	assert_eq!(w.copies[0].sync, Sync::Named(vec![1]));
	w.settle("old primary guard");
}

#[test]
fn a_handover_to_a_standby_on_the_old_guard_completes() {
	// The new primary's guard and the witness carry it out; the old standby's guard only asks once
	// its stream ends, as it always did, and is granted.
	let mut w = warm_on(false, true, false);
	assert!(w.ask(RequestKind::Handover, Some(1)));
	w.until(10_000, "promoted", |w| {
		w.copies[1].role == Role::Primary && w.pair().epoch == 2
	});
	w.settle("old standby guard");
}

#[test]
fn an_old_witness_takes_no_change_and_no_move() {
	let mut w = warm_on(false, false, true);
	assert!(
		!w.ask(RequestKind::Handover, Some(1)),
		"no request verb there"
	);
	w.moving = Some((0, 1, MoveStep::Freeze));
	w.run_for(6_000);
	assert!(w.moving.is_none() && w.witnesses[1].cluster.is_none());
	assert_eq!(w.authority(), 0);
	w.settle("old witness");
}

#[test]
fn a_move_to_an_old_witness_is_given_up_and_the_pair_thawed() {
	let mut w = warm_on(false, false, false);
	w.start_witness(1, true);
	w.moving = Some((0, 1, MoveStep::Freeze));
	w.run_for(11_000);
	assert!(w.moving.is_none());
	assert_eq!(
		w.witnesses[0].cluster.as_ref().map(|c| c.moving.clone()),
		Some(None)
	);
	// Thawed: a failover goes through the old witness again.
	w.copies[0].pg_up = false;
	w.until(2 * T + 1_000, "promoted", |w| {
		w.copies[1].role == Role::Primary
	});
	w.settle("move to an old witness");
}

#[test]
fn an_old_guard_rolled_back_onto_a_copy_a_new_one_left_mid_step_refuses_to_start() {
	let mut w = warm();
	assert!(w.ask(RequestKind::Handover, Some(1)));
	w.until(2_000, "fencing", |w| {
		matches!(w.copies[0].mem.change, Some(Change::Fencing { .. }))
	});
	w.copies[0].lose_answer = true;
	w.until(2_000, "sent", |w| w.pair().epoch == 2);
	w.cut(0, N, false);
	w.copies[0].guard_up = false;
	w.tick(None);
	w.copies[0].old = true;
	w.restart_guard(0);
	assert!(
		!w.copies[0].guard_up,
		"it would end the fence on its standby's stream"
	);
	assert_eq!(w.copies[0].sync, Sync::Fenced);
	w.run_for(T);
	assert_eq!(w.copies[0].sync, Sync::Fenced);
	w.settle("old guard on a handover sent");
}
