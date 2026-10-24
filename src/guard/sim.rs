//! The simulator (L21, Phase 14a): two copies, their guards and a witness under random crashes and
//! cut links, run against a model of Postgres's synchronous replication. Every tick a client writes
//! to every copy that takes writes, and the model acknowledges a commit as Postgres would: at once
//! with `synchronous_standby_names` empty, once the standby has flushed it with the standby named,
//! never while fenced; emptying the names releases the commits waiting on it. Then everything heals
//! and the checks run:
//!
//! - every acknowledged commit is on the copy the witness ends with as primary;
//! - a commit is only ever acknowledged by the witness's primary of that moment;
//! - with nothing broken, a write is acknowledged again (no copy is left fenced for good).
//!
//! An operator stands in for whoever rebuilds a replaced copy (the host agent, a person): once the
//! witness names a copy the standby while it is down or still a primary, it is restored from the
//! primary and streams from it.

// The loops index the two copies and their names together, and borrow `self` mutably inside.
#![allow(clippy::needless_range_loop)]

use super::decide::{Action, Memory, Postgres, StandbySeen, SyncNames, decide};
use super::witness::{Answer, Call, Cluster};

const TICK: u64 = 100;
const T: u64 = 10_000;
const NAMES: [&str; 2] = ["a", "b"];
const STREAM_PER_TICK: usize = 3;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Role {
	Primary,
	Standby,
}

struct Copy {
	pg_up: bool,
	guard_up: bool,
	role: Role,
	wal: Vec<u64>,
	names: SyncNames,
	/// Commits written and waiting for the standby: (id, the LSN it needs flushed).
	pending: Vec<(u64, usize)>,
	mem: Memory,
	/// Standby: the last message from the primary.
	last_receive: Option<u64>,
	/// Primary: the standby's last reply and the flush it reported.
	last_reply: Option<(u64, usize)>,
}

struct Witness {
	up: bool,
	cluster: Cluster,
	/// The state file.
	file: Cluster,
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

struct World {
	now: u64,
	copies: [Copy; 2],
	witness: Witness,
	/// Links: a-b, a-witness, b-witness.
	ab: bool,
	aw: bool,
	bw: bool,
	next_id: u64,
	acked: Vec<u64>,
	grants: u32,
	trace: Vec<String>,
}

impl World {
	fn new() -> World {
		let copy = |role| Copy {
			pg_up: true,
			guard_up: true,
			role,
			wal: Vec::new(),
			names: SyncNames::Standby,
			pending: Vec::new(),
			mem: Memory {
				epoch: 1,
				sync_from_lsn: Some(0),
				..Memory::default()
			},
			last_receive: None,
			last_reply: None,
		};
		let cluster = Cluster::new("a", Some("b"), T, 0);
		World {
			now: 0,
			copies: [copy(Role::Primary), copy(Role::Standby)],
			witness: Witness {
				up: true,
				cluster: cluster.clone(),
				file: cluster,
			},
			ab: true,
			aw: true,
			bw: true,
			next_id: 0,
			acked: Vec::new(),
			grants: 0,
			trace: Vec::new(),
		}
	}

	fn to_witness(&self, i: usize) -> bool {
		self.witness.up && if i == 0 { self.aw } else { self.bw }
	}

	fn log(&mut self, s: String) {
		self.trace.push(format!("{:>7} {s}", self.now));
		if self.trace.len() > 60 {
			self.trace.remove(0);
		}
	}

	fn chaos(&mut self, r: &mut Rng) {
		for link in 0..3 {
			if r.chance(2_000) {
				let l = match link {
					0 => &mut self.ab,
					1 => &mut self.aw,
					_ => &mut self.bw,
				};
				*l = !*l;
				let (name, state) = (["ab", "aw", "bw"][link], *l);
				self.log(format!("link {name} {}", if state { "up" } else { "cut" }));
			}
		}
		for i in 0..2 {
			let c = &mut self.copies[i];
			if c.pg_up && r.chance(800) {
				c.pg_up = false;
				c.pending.clear();
				c.last_receive = None;
				c.last_reply = None;
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
				// Only the epoch survives a restart; the runner reads where the names stand.
				c.guard_up = true;
				c.mem = Memory {
					epoch: c.mem.epoch,
					sync_from_lsn: (c.names == SyncNames::Standby).then_some(c.wal.len() as u64),
					..Memory::default()
				};
				self.log(format!("{} guard back", NAMES[i]));
			}
		}
		if self.witness.up && r.chance(500) {
			self.witness.up = false;
			self.log("witness crashed".into());
		} else if !self.witness.up && r.chance(3_000) {
			self.witness.up = true;
			self.witness.cluster = self.witness.file.clone().restored(self.now);
			self.log("witness back".into());
		}
	}

	fn replicate(&mut self) {
		for (p, s) in [(0, 1), (1, 0)] {
			let ok = self.ab
				&& self.copies[p].pg_up
				&& self.copies[s].pg_up
				&& self.copies[p].role == Role::Primary
				&& self.copies[s].role == Role::Standby
				&& follows(&self.copies[p].wal, &self.copies[s].wal);
			if ok {
				// A few records a tick, so a standby that was away is behind for a while.
				let have = self.copies[s].wal.len();
				let upto = self.copies[p].wal.len().min(have + STREAM_PER_TICK);
				let missing = self.copies[p].wal[have..upto].to_vec();
				self.copies[s].wal.extend(missing);
				let flushed = self.copies[s].wal.len();
				self.copies[s].last_receive = Some(self.now);
				self.copies[p].last_reply = Some((self.now, flushed));
			}
		}
	}

	fn write_and_ack(&mut self) {
		for i in 0..2 {
			let c = &self.copies[i];
			if c.pg_up && c.role == Role::Primary && c.names != SyncNames::Fenced {
				self.next_id += 1;
				let id = self.next_id;
				let c = &mut self.copies[i];
				c.wal.push(id);
				let lsn = c.wal.len();
				c.pending.push((id, lsn));
			}
			let c = &mut self.copies[i];
			let flushed = c.last_reply.map_or(0, |(_, f)| f);
			let mut done = Vec::new();
			c.pending.retain(|&(id, lsn)| {
				let ack = match c.names {
					SyncNames::Off => true,
					SyncNames::Standby => flushed >= lsn,
					SyncNames::Fenced => false,
				};
				if ack {
					done.push(id);
				}
				!ack
			});
			for id in done {
				assert_eq!(
					self.witness.cluster.primary,
					NAMES[i],
					"{} acknowledged commit {id} while the witness's primary is {}\n{}",
					NAMES[i],
					self.witness.cluster.primary,
					self.trace.join("\n")
				);
				self.acked.push(id);
			}
		}
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
				let other = &self.copies[1 - i];
				let streams = self.ab && other.pg_up && other.role == Role::Standby;
				let standby = c
					.last_reply
					.filter(|_| streams)
					.map(|(t, flush)| StandbySeen {
						reply_age_ms: self.now - t,
						sync: c.names == SyncNames::Standby,
						flush_lsn: flush as u64,
					});
				Postgres::Primary {
					names: c.names,
					standby,
					lsn: c.wal.len() as u64,
				}
			}
		}
	}

	fn call(&mut self, i: usize, call: Call) -> Option<Answer> {
		if !self.to_witness(i) {
			return None;
		}
		let (answer, changed) = self.witness.cluster.handle(&call, self.now);
		if changed {
			self.witness.file = self.witness.cluster.clone();
		}
		Some(answer)
	}

	/// What the runner does with an answer to any call.
	fn heard(&mut self, i: usize, answer: Option<Answer>) {
		let (now, c) = (self.now, &mut self.copies[i]);
		if let Some(a) = answer {
			c.mem.heard(now, c.role == Role::Primary, &a);
		}
	}

	fn guards(&mut self) {
		for i in 0..2 {
			if !self.copies[i].guard_up {
				continue;
			}
			let pg = self.facts(i);
			for action in decide(self.now, T, pg, &self.copies[i].mem) {
				let (copy, epoch) = (NAMES[i].to_string(), self.copies[i].mem.epoch);
				match action {
					Action::Renew => {
						let a = self.call(i, Call::Renew { copy, epoch });
						self.heard(i, a);
					}
					Action::Eligible(eligible) => {
						let a = self.call(
							i,
							Call::Eligible {
								copy,
								epoch,
								eligible,
							},
						);
						self.heard(i, a);
					}
					Action::Ask => {
						let lost = match pg {
							Postgres::Standby { receive_age_ms } => {
								receive_age_ms.unwrap_or(u64::MAX)
							}
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
						if matches!(a, Some(Answer::Granted { .. }))
							&& self.copies[i].role == Role::Standby
						{
							let old = &self.copies[1 - i];
							if old.pg_up && old.guard_up && old.role == Role::Primary {
								assert_eq!(
									old.names,
									SyncNames::Fenced,
									"{} granted while {} still serves\n{}",
									NAMES[i],
									NAMES[1 - i],
									self.trace.join("\n")
								);
							}
							self.grants += 1;
							self.log(format!("{} granted", NAMES[i]));
						}
						self.heard(i, a);
					}
					Action::GoAsync => {
						assert_eq!(self.copies[i].mem.eligible, Some(false));
						self.copies[i].names = SyncNames::Off;
						self.log(format!("{} async", NAMES[i]));
					}
					Action::GoSync => {
						let c = &mut self.copies[i];
						c.names = SyncNames::Standby;
						c.mem.sync_from_lsn = Some(c.wal.len() as u64);
						self.log(format!("{} sync", NAMES[i]));
					}
					Action::Fence => {
						let c = &mut self.copies[i];
						c.names = SyncNames::Fenced;
						c.pending.clear();
						self.log(format!("{} fenced", NAMES[i]));
					}
					Action::Unfence => {
						let c = &mut self.copies[i];
						c.names = SyncNames::Standby;
						c.mem.sync_from_lsn = Some(c.wal.len() as u64);
						self.log(format!("{} unfenced", NAMES[i]));
					}
					Action::Promote(epoch) => {
						let c = &mut self.copies[i];
						c.names = SyncNames::Off;
						c.role = Role::Primary;
						c.last_receive = None;
						c.last_reply = None;
						c.mem = Memory::promoted(epoch, self.now);
						self.log(format!("{} promoted at epoch {epoch}", NAMES[i]));
					}
				}
			}
		}
	}

	fn operator(&mut self) {
		if !self.witness.up || !self.ab {
			return;
		}
		let w = &self.witness.cluster;
		let Some(standby) = w.standby.as_deref() else {
			return;
		};
		let s = NAMES.iter().position(|n| *n == standby).unwrap_or(0);
		let p = 1 - s;
		let source = &self.copies[p];
		let target = &self.copies[s];
		let replaced =
			!target.pg_up || target.role == Role::Primary || !follows(&source.wal, &target.wal);
		if replaced && source.pg_up && source.role == Role::Primary && w.primary == NAMES[p] {
			let wal = source.wal.clone();
			let epoch = w.epoch;
			let t = &mut self.copies[s];
			t.pg_up = true;
			t.role = Role::Standby;
			t.wal = wal;
			t.pending.clear();
			t.last_receive = None;
			t.last_reply = None;
			t.mem = Memory {
				epoch,
				..Memory::default()
			};
			self.log(format!("{standby} rebuilt from {}", NAMES[p]));
		}
	}

	fn tick(&mut self, r: Option<&mut Rng>) {
		self.now += TICK;
		if let Some(r) = r {
			self.chaos(r);
		}
		self.replicate();
		self.write_and_ack();
		self.guards();
		if self.now.is_multiple_of(5_000) {
			self.operator();
		}
	}

	fn heal(&mut self) {
		self.ab = true;
		self.aw = true;
		self.bw = true;
		self.witness.up = true;
		self.witness.cluster = self.witness.file.clone().restored(self.now);
		for c in &mut self.copies {
			c.pg_up = true;
			if !c.guard_up {
				c.guard_up = true;
				c.mem = Memory {
					epoch: c.mem.epoch,
					sync_from_lsn: (c.names == SyncNames::Standby).then_some(c.wal.len() as u64),
					..Memory::default()
				};
			}
		}
		self.log("healed".into());
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
	for _ in 0..ticks {
		w.tick(Some(&mut r));
	}
	w.heal();
	let before = w.acked.len();
	for _ in 0..(6 * T / TICK) {
		w.tick(None);
	}
	let p = NAMES
		.iter()
		.position(|n| *n == w.witness.cluster.primary)
		.unwrap_or(0);
	let wal: std::collections::HashSet<u64> = w.copies[p].wal.iter().copied().collect();
	for id in &w.acked {
		assert!(
			wal.contains(id),
			"seed {seed}: acknowledged commit {id} is not on the primary {}\n{}",
			NAMES[p],
			w.trace.join("\n")
		);
	}
	assert!(
		w.acked.len() > before,
		"seed {seed}: nothing commits once everything is back\n{}",
		w.trace.join("\n")
	);
	w
}

#[test]
fn no_acknowledged_commit_is_lost_and_only_the_primary_acknowledges() {
	let mut grants = 0;
	for seed in 0..300 {
		grants += run(seed, 30_000).grants;
	}
	// The runs must exercise the failover, not only survive by never trying it.
	assert!(grants > 300, "only {grants} grants in 300 runs");
}

#[test]
fn a_dead_primary_is_replaced_within_two_leases() {
	let mut w = World::new();
	for _ in 0..50 {
		w.tick(None);
	}
	assert!(
		w.witness.cluster.eligible,
		"the standby is vouched for once synchronous"
	);
	w.copies[0].pg_up = false;
	let died = w.now;
	while w.copies[1].role == Role::Standby {
		w.tick(None);
		assert!(
			w.now - died < 2 * T + 1_000,
			"not promoted within two leases"
		);
	}
}

#[test]
fn a_cut_primary_fences_itself_before_the_standby_is_promoted() {
	let mut w = World::new();
	for _ in 0..50 {
		w.tick(None);
	}
	w.ab = false;
	w.aw = false;
	while w.copies[1].role == Role::Standby {
		w.tick(None);
	}
	assert_eq!(w.copies[0].names, SyncNames::Fenced);
}

#[test]
fn with_the_standby_and_the_witness_gone_commits_wait() {
	let mut w = World::new();
	for _ in 0..50 {
		w.tick(None);
	}
	w.copies[1].pg_up = false;
	w.witness.up = false;
	let before = w.acked.len();
	for _ in 0..(3 * T / TICK) {
		w.tick(None);
	}
	assert_eq!(w.acked.len(), before);
	assert_eq!(w.copies[1].role, Role::Standby);
}

#[test]
fn the_witness_alone_gone_changes_nothing() {
	let mut w = World::new();
	for _ in 0..50 {
		w.tick(None);
	}
	w.witness.up = false;
	let before = w.acked.len();
	for _ in 0..(3 * T / TICK) {
		w.tick(None);
	}
	assert!(w.acked.len() > before + 200);
	assert_eq!(w.copies[0].names, SyncNames::Standby);
}
