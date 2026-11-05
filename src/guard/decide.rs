//! The guard's decision (L21): what one copy's guard does this tick, from what its Postgres shows
//! and what the witness last said. Pure; `run.rs` reads the facts and carries the actions out, and
//! the simulator (`sim.rs`) does the same against a model.
//!
//! The rule everything else serves: a commit is acknowledged without the standby only after the
//! witness has recorded that the standby is not `eligible` (`Eligible(false)` before `GoAsync`), and
//! the standby is vouched for only once it is synchronous and has flushed past the LSN of the moment
//! it became so (`GoSync`, then `Eligible(true)`). The lease is renewed every tick and decides only
//! when a cut primary stops serving (`Fence`).
//!
//! Two changes the control plane may ask of a whole pair through the witness (`Request`), carried
//! out here by the primary's guard, each in steps that can stop at any point with nothing lost:
//!
//! - **A handover** (a planned switchover): fence this copy for it (`HandoverFence`: nothing
//!   commits, sessions ended), wait until the standby has replayed everything it wrote, then
//!   `Handover`: the witness records the standby as the primary at the next epoch and this copy as
//!   demoted, and the demoted copy ends its standby's stream so the standby asks and is granted at
//!   once. Until the witness answers, the copy stays fenced (`HandoverSent`, kept on disk): a lost
//!   answer is never read as "not done". Anything wrong before the handover is sent (the standby
//!   gone, behind, the witness silent) unfences and gives the request up (`Abandon`).
//! - **A replacement standby**, made before the old one goes: once the new copy streams and has
//!   caught up, commits wait for both (`GoBoth`, `FIRST 2`), and once it has flushed past the moment
//!   that took effect it has every acknowledged commit, so `Replace` makes it the witness's standby,
//!   eligible, and commits then wait for it alone. While the witness has not answered, both stay
//!   named (`ReplaceSent`): either copy may be the one the witness holds, and each has everything.

use super::witness::{Answer, Request, RequestKind};

/// What `synchronous_standby_names` says on a primary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncNames {
	/// The standby alone: no commit is acknowledged before it has flushed it.
	Standby,
	/// Empty: commits are acknowledged by this copy alone.
	Off,
	/// A name no standby has (`FENCED_NAME`), with the copy read-only: nothing commits.
	Fenced,
	/// The standby and one more copy (`FIRST 2`), during a replacement: a commit waits for both.
	Both,
}

/// The standby as the primary sees it (`pg_stat_replication`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StandbySeen {
	/// Since the standby's last reply (`reply_time`).
	pub reply_age_ms: u64,
	/// `sync_state = 'sync'`.
	pub sync: bool,
	pub flush_lsn: u64,
	pub replay_lsn: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Postgres {
	Down,
	Primary {
		names: SyncNames,
		/// The witness's standby, streaming from this copy.
		standby: Option<StandbySeen>,
		/// The copy a replacement asks for, streaming from this copy.
		candidate: Option<StandbySeen>,
		lsn: u64,
		/// Whether `archive_command` carries the fence's guard (`fence_archive_command`):
		/// `Some(true)` it archives only segments the standby had flushed, `Some(false)` everything,
		/// `None` no archive_command at all, so nothing to guard.
		archive_fenced: Option<bool>,
	},
	/// In recovery; `receive_age_ms` since the last message from the primary (`None`: no stream).
	Standby {
		receive_age_ms: Option<u64>,
	},
}

/// Why the primary's guard gave a request up: said to the witness, which keeps it for the control
/// plane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
	/// The standby was not caught up in time.
	Behind,
	/// The standby stopped streaming, or stopped being synchronous.
	StandbyGone,
	/// The witness stopped answering before the step that needs it.
	WitnessSilent,
	/// The pair was not whole (its standby synchronous and vouched for) in time.
	NotWhole,
	/// The new standby did not stream, or did not catch up, in time.
	CandidateNotReady,
	/// This guard restarted in the middle of a replacement, and starts it again only when asked again.
	Restarted,
}

impl Reason {
	pub fn text(self) -> &'static str {
		match self {
			Reason::Behind => "the standby did not catch up in time",
			Reason::StandbyGone => "the standby stopped streaming synchronously",
			Reason::WitnessSilent => "the primary lost the witness",
			Reason::NotWhole => {
				"the pair was not whole: its standby was not synchronous and vouched for"
			}
			Reason::CandidateNotReady => "the new standby did not stream and catch up in time",
			Reason::Restarted => "the primary's guard restarted in the middle of it",
		}
	}
}

/// This primary's part in a request, step by step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
	/// Handover: fenced for it at `since`, waiting for the standby to replay all this copy wrote.
	Fencing { id: String, since: u64 },
	/// Handover sent; kept on disk. Fenced until the witness says how it went: a lease answered to
	/// call number `from` or later tells it.
	HandoverSent { id: String, from: u64 },
	/// Replacement: `to` named beside the standby (`FIRST 2`) at `since`.
	Both { id: String, to: String, since: u64 },
	/// Replacement sent; kept on disk. Both stay named until a lease answered to call number `from`
	/// or later (the replacement's own answer among them) says which copy is the standby.
	ReplaceSent { id: String, to: String, from: u64 },
	/// Given up; said to the witness until it answers.
	GivingUp { id: String, reason: Reason },
}

impl Change {
	pub fn id(&self) -> &str {
		match self {
			Change::Fencing { id, .. }
			| Change::HandoverSent { id, .. }
			| Change::Both { id, .. }
			| Change::ReplaceSent { id, .. }
			| Change::GivingUp { id, .. } => id,
		}
	}
}

/// What the guard remembers between ticks. Only `epoch`, `demoted`, the witness's `standby`, and
/// a handover or a replacement sent but not answered, are kept across a restart (the guard's state
/// file); the rest starts empty, which is always the cautious reading.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Memory {
	pub epoch: u64,
	/// When the last renewal the witness accepted was SENT (the witness's lease began later).
	pub renewed_at: Option<u64>,
	/// `eligible` as the witness last answered.
	pub eligible: Option<bool>,
	/// The witness said a newer epoch has another primary: fenced until rebuilt.
	pub demoted: bool,
	/// The witness granted this copy `epoch`; promote.
	pub granted: Option<u64>,
	/// The primary's LSN when the standby was last put into `synchronous_standby_names`; the
	/// standby is vouched for once it has flushed past it. `None`: not known (after a restart the
	/// runner sets it to the current LSN).
	pub sync_from_lsn: Option<u64>,
	/// The witness's standby as last answered (kept on disk): the copy this primary's commits wait
	/// for. `None`: never heard (the configured peer stands in).
	pub standby: Option<String>,
	/// What the witness last said is asked of the pair.
	pub request: Option<Request>,
	/// When this guard first heard of `request`.
	pub request_heard_at: Option<u64>,
	/// This copy's part in `request`.
	pub change: Option<Change>,
	/// A replacement: the LSN its new standby must flush past (read after `GoBoth` took effect).
	pub candidate_from_lsn: Option<u64>,
	/// Calls made to the witness, and the number of the last one answered with a lease.
	pub calls: u64,
	pub lease_call: u64,
	/// The witness has answered this guard since it started.
	pub heard: bool,
}

impl Memory {
	/// The number of the call about to go out.
	pub fn next_call(&mut self) -> u64 {
		self.calls += 1;
		self.calls
	}

	/// What an answer from the witness teaches, to any call (the last numbered by `next_call`).
	/// `sent_at`: when the call went out (a lease is counted from then); `primary`: this copy's
	/// Postgres is one.
	pub fn heard(&mut self, sent_at: u64, primary: bool, answer: &Answer) {
		self.heard = !matches!(answer, Answer::Moved { .. });
		match answer {
			Answer::Lease {
				eligible,
				standby,
				request,
				..
			} => {
				self.renewed_at = Some(sent_at);
				self.eligible = Some(*eligible);
				self.lease_call = self.calls;
				// A witness from before replacements says no standby: the last one known stands.
				if standby.is_some() {
					self.standby = standby.clone();
				}
				let same = self.request.as_ref().map(|r| &r.id) == request.as_ref().map(|r| &r.id);
				if !same {
					self.request_heard_at = request.as_ref().map(|_| sent_at);
				}
				self.request = request.clone();
			}
			Answer::Granted { epoch } => {
				self.epoch = *epoch;
				self.granted = Some(*epoch);
				// A copy granted the primary's place is no longer demoted. A demoted primary that rejoined
				// as the standby, then was handed the primary back, kept `demoted` here, and the runner
				// wrote it to the state file again after the grant had cleared it there; restarted (its
				// definition changed with a replacement), the primary read itself demoted and stayed
				// fenced for good (the local fleet drill, 2026-10-31: every write refused).
				self.demoted = false;
			}
			// A primary at an older epoch has been replaced; a standby learns whose it is.
			Answer::Demoted { epoch, .. } => {
				if primary {
					self.demoted = true;
					self.change = None;
				} else {
					self.epoch = *epoch;
				}
			}
			Answer::Held { .. } | Answer::Refused { .. } | Answer::Moved { .. } => {}
		}
	}

	/// Just promoted at `epoch`: the grant renewed the lease (asked at `asked_at`) and cleared
	/// `eligible`, and `synchronous_standby_names` is empty.
	pub fn promoted(epoch: u64, asked_at: u64) -> Memory {
		Memory {
			epoch,
			renewed_at: Some(asked_at),
			eligible: Some(false),
			..Memory::default()
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
	Renew,
	/// Tell the witness the standby does (true) or may not (false) have every acknowledged commit.
	Eligible(bool),
	/// `synchronous_standby_names` to empty. Only after the witness answered `eligible: false`.
	GoAsync,
	/// `synchronous_standby_names` to the standby alone (the witness's), and remember the LSN.
	GoSync,
	/// Read-only, sessions ended, `synchronous_standby_names` to `FENCED_NAME`.
	Fence,
	/// Out of the fence, into `SyncNames::Standby` (never straight to `Off`); into `Both` while a
	/// replacement's answer is awaited.
	Unfence,
	/// Ask the witness for the primary's place.
	Ask,
	/// Granted: clear `synchronous_standby_names`, `pg_promote()`, hold `epoch`.
	Promote(u64),
	/// `archive_command` guarded: only segments before the one holding the standby's last flush
	/// are archived, so a fenced copy never feeds an archive-fed standby a branch. Always after
	/// `Fence` (or with the copy already fenced), never instead of it: a failed reload here leaves
	/// the fence as it is and is tried again on the next tick.
	FenceArchive,
	/// The guard taken off `archive_command` again, once the copy is out of the fence.
	UnfenceArchive,
	/// A handover's fence: `Fence`, and the handover remembered (`Change::Fencing`).
	HandoverFence,
	/// Tell the witness the standby is the primary now (the handover asked). Kept on disk first.
	Handover,
	/// `synchronous_standby_names` to the standby and the replacement's copy (`FIRST 2`).
	GoBoth,
	/// Tell the witness the replacement's copy has every acknowledged commit. Kept on disk first.
	Replace,
	/// Give the request up, with the reason, until the witness has heard it.
	Abandon(Reason),
	/// The request's step is settled (done, or never taken): forget it.
	Resolved,
	/// End the stream of a standby reading from this demoted copy, so it asks the witness.
	EndStreams,
}

/// The name a fenced copy waits for. No standby ever connects with it.
pub const FENCED_NAME: &str = "lepis_fenced";

/// How close (in bytes of WAL) a standby must be before a handover fences for it, or a new standby
/// before commits wait for it: one segment, so the fence lasts a moment and a commit's wait no
/// longer than it already does.
pub const CATCH_UP_SLACK: u64 = 16 << 20;

pub fn decide(now: u64, lease_ms: u64, pg: Postgres, mem: &Memory) -> Vec<Action> {
	match pg {
		Postgres::Down => Vec::new(),
		Postgres::Standby { receive_age_ms } => {
			if let Some(epoch) = mem.granted {
				return vec![Action::Promote(epoch)];
			}
			// Asked once on starting whatever the stream says: a guard stopped between a grant and
			// its promotion (or a handover's new primary) learns it, though its old primary streams
			// to it again and it would otherwise never ask.
			if !mem.heard || receive_age_ms.is_none_or(|age| age >= lease_ms) {
				vec![Action::Ask]
			} else {
				Vec::new()
			}
		}
		Postgres::Primary {
			names,
			standby,
			candidate,
			lsn,
			archive_fenced,
		} => {
			if mem.demoted {
				let mut out = if names == SyncNames::Fenced {
					Vec::new()
				} else {
					vec![Action::Fence]
				};
				// Its standby still streaming from it (a handover's new primary, not promoted yet):
				// ended, so it learns from the witness that it is the primary.
				if standby.is_some() {
					out.push(Action::EndStreams);
				}
				if archive_fenced == Some(false) {
					out.push(Action::FenceArchive);
				}
				return out;
			}
			let mut out = vec![Action::Renew];
			// Still holding a grant out of recovery: the promotion outlasted `pg_promote`'s wait
			// and finished since, so it is recorded first (the runner, which does not promote
			// again). The rest is decided as it stands, so a lapsed lease still fences.
			if let Some(epoch) = mem.granted {
				out.insert(0, Action::Promote(epoch));
			}
			// Half the lease, measured from when the renewal was sent: the witness's lease began
			// after that, so this copy stops before the witness could hand its place to another.
			let lease = mem
				.renewed_at
				.is_some_and(|t| now.saturating_sub(t) < lease_ms / 2);
			let up = standby.filter(|s| s.reply_age_ms < lease_ms / 2);
			let candidate = candidate.filter(|s| s.reply_age_ms < lease_ms / 2);
			let facts = Facts {
				now,
				lease_ms,
				names,
				up,
				candidate,
				lsn,
				lease,
			};
			out.extend(change(&facts, mem).unwrap_or_else(|| ordinary(&facts, mem)));
			// The archive follows the fence: guarded from the tick that fences (after the fence,
			// so a failure here cannot keep it from happening) until the tick that unfences.
			let fenced = out.contains(&Action::Fence)
				|| out.contains(&Action::HandoverFence)
				|| (names == SyncNames::Fenced && !out.contains(&Action::Unfence));
			match archive_fenced {
				Some(false) if fenced => out.push(Action::FenceArchive),
				Some(true) if !fenced => out.push(Action::UnfenceArchive),
				_ => {}
			}
			out
		}
	}
}

/// A primary's facts for this tick, as `decide` reads them.
struct Facts {
	now: u64,
	lease_ms: u64,
	names: SyncNames,
	/// The standby, heard within half a lease.
	up: Option<StandbySeen>,
	/// The replacement's copy, heard within half a lease.
	candidate: Option<StandbySeen>,
	lsn: u64,
	/// A renewal accepted within half a lease.
	lease: bool,
}

/// The rules of a pair with nothing asked of it.
fn ordinary(f: &Facts, mem: &Memory) -> Vec<Action> {
	match f.names {
		// A standby streaming from this copy cannot have been promoted, so either one ends the
		// fence; commits wait for the standby again until the witness agrees otherwise.
		SyncNames::Fenced => {
			if f.lease || f.up.is_some() {
				vec![Action::Unfence]
			} else {
				Vec::new()
			}
		}
		// Not confirmed by the witness since a restart, with commits acknowledged alone: an
		// operator may have promoted the standby with `accept_loss` meanwhile.
		SyncNames::Off if mem.renewed_at.is_none() => vec![Action::Fence],
		_ if !f.lease && f.up.is_none() => vec![Action::Fence],
		SyncNames::Standby => match f.up {
			None => match mem.eligible {
				Some(false) => vec![Action::GoAsync],
				_ => vec![Action::Eligible(false)],
			},
			Some(s) => {
				let past = mem.sync_from_lsn.is_some_and(|from| s.flush_lsn >= from);
				if s.sync && past && mem.eligible == Some(false) {
					vec![Action::Eligible(true)]
				} else {
					Vec::new()
				}
			}
		},
		SyncNames::Off => {
			if f.up.is_some() {
				vec![Action::GoSync]
			} else {
				Vec::new()
			}
		}
		// Two named and nothing remembered (a restart in a replacement): either may be the standby
		// the witness holds, and each has every acknowledged commit, so both stay named until a
		// renewal says which it is and that nothing is still asked; a request still asked is ended
		// first, since this guard cannot know whether it sent the step.
		SyncNames::Both => {
			if mem.renewed_at.is_none() {
				Vec::new()
			} else if mem.request.is_some() {
				vec![Action::Abandon(Reason::Restarted)]
			} else {
				vec![Action::GoSync]
			}
		}
	}
}

/// The rules of a request, when one is asked of the pair or this copy is part way through one.
/// `None`: the ordinary rules decide this tick.
fn change(f: &Facts, mem: &Memory) -> Option<Vec<Action>> {
	let asked = |id: &str, kind: RequestKind| {
		mem.request
			.as_ref()
			.is_some_and(|r| r.id == id && r.kind == kind)
	};
	let unfence = |out: &mut Vec<Action>| {
		if f.names == SyncNames::Fenced && (f.lease || f.up.is_some()) {
			out.push(Action::Unfence);
		}
	};
	match &mem.change {
		Some(Change::HandoverSent { id, from }) => {
			// Fenced whatever else, until the witness says: a renewal answered after the handover
			// went out tells whether it was taken (it would have been answered `Demoted`).
			let mut out = Vec::new();
			if f.names != SyncNames::Fenced {
				out.push(Action::Fence);
			}
			if mem.lease_call >= *from {
				if asked(id, RequestKind::Handover) {
					// Still asked: sent again, which the witness answers as it did the first.
					out.push(Action::Handover);
				} else {
					// Ended without it: a late copy of the call is refused, so nothing moved.
					out.push(Action::Resolved);
					unfence(&mut out);
				}
			}
			Some(out)
		}
		Some(Change::Fencing { id, since }) => {
			let reason = if !asked(id, RequestKind::Handover) {
				None
			// Fenced, the standby reads as asynchronous (no standby is named); streaming is what counts,
			// and `eligible` says it has every acknowledged commit.
			} else if f.up.is_none() {
				Some(Reason::StandbyGone)
			} else if !f.lease {
				Some(Reason::WitnessSilent)
			} else if f.now.saturating_sub(*since) > f.lease_ms {
				Some(Reason::Behind)
			} else {
				let s = f.up.expect("checked above");
				return Some(if f.names != SyncNames::Fenced {
					vec![Action::HandoverFence]
				} else if s.replay_lsn >= f.lsn {
					vec![Action::Handover]
				} else {
					Vec::new()
				});
			};
			// Nothing was sent, so the witness still holds this copy the primary: out of the fence.
			let mut out = Vec::new();
			unfence(&mut out);
			out.push(match reason {
				Some(r) => Action::Abandon(r),
				None => Action::Resolved,
			});
			Some(out)
		}
		Some(Change::Both { id, since, .. }) => {
			if f.names != SyncNames::Both {
				let mut out = vec![Action::Resolved];
				out.extend(ordinary(f, mem));
				return Some(out);
			}
			if !asked(id, RequestKind::Replace) {
				return Some(vec![Action::Resolved, Action::GoSync]);
			}
			let reason = if f.up.is_none() {
				Some(Reason::StandbyGone)
			} else if f.candidate.is_none() || f.now.saturating_sub(*since) > 2 * f.lease_ms {
				Some(Reason::CandidateNotReady)
			} else {
				None
			};
			// Nothing was sent: back to the standby alone (the witness's, which has everything).
			if let Some(r) = reason {
				let narrow = if !f.lease && f.up.is_none() {
					Action::Fence
				} else {
					Action::GoSync
				};
				return Some(vec![narrow, Action::Abandon(r)]);
			}
			let (s, c) = (f.up.expect("checked"), f.candidate.expect("checked"));
			let past = mem
				.candidate_from_lsn
				.is_some_and(|from| c.flush_lsn >= from);
			Some(if s.sync && c.sync && past {
				vec![Action::Replace]
			} else {
				Vec::new()
			})
		}
		Some(Change::ReplaceSent { id, to, from }) => {
			if mem.lease_call >= *from {
				if mem.standby.as_deref() == Some(to.as_str()) {
					// Taken: the new copy is the standby, and commits wait for it alone.
					let mut out = vec![Action::Resolved];
					out.extend(match f.names {
						SyncNames::Both => vec![Action::GoSync],
						_ => ordinary(f, mem),
					});
					return Some(out);
				}
				if !asked(id, RequestKind::Replace) {
					let mut out = vec![Action::Resolved];
					out.extend(match f.names {
						SyncNames::Both => vec![Action::GoSync],
						_ => ordinary(f, mem),
					});
					return Some(out);
				}
				let ready = f.up.is_some_and(|s| s.sync)
					&& f.candidate.is_some_and(|c| {
						c.sync
							&& mem
								.candidate_from_lsn
								.is_some_and(|from| c.flush_lsn >= from)
					});
				return Some(if ready {
					vec![Action::Replace]
				} else {
					vec![Action::Abandon(Reason::CandidateNotReady)]
				});
			}
			// Unanswered: both stay named (or fenced, which commits nothing either).
			Some(match f.names {
				SyncNames::Fenced => {
					let mut out = Vec::new();
					unfence(&mut out);
					out
				}
				_ if !f.lease && f.up.is_none() => vec![Action::Fence],
				_ => Vec::new(),
			})
		}
		Some(Change::GivingUp { id, reason }) => {
			let still = mem.request.as_ref().is_some_and(|r| r.id == *id);
			if still {
				let mut out = match f.names {
					// Both named while the witness has not heard: stay so (it may still take it).
					SyncNames::Both => Vec::new(),
					_ => ordinary(f, mem),
				};
				out.push(Action::Abandon(*reason));
				return Some(out);
			}
			let mut out = vec![Action::Resolved];
			out.extend(ordinary(f, mem));
			Some(out)
		}
		None => {
			let r = mem.request.as_ref()?;
			if f.names == SyncNames::Both || f.names == SyncNames::Fenced {
				return None;
			}
			let whole = f.names == SyncNames::Standby
				&& f.lease && f.up.is_some_and(|s| s.sync)
				&& mem.eligible == Some(true);
			let waited = mem
				.request_heard_at
				.is_some_and(|t| f.now.saturating_sub(t) > 2 * f.lease_ms);
			let give_up = |reason: Reason| {
				let mut out = ordinary(f, mem);
				out.push(Action::Abandon(reason));
				Some(out)
			};
			match r.kind {
				RequestKind::Handover => {
					let caught_up =
						f.up.is_some_and(|s| s.replay_lsn.saturating_add(CATCH_UP_SLACK) >= f.lsn);
					if whole && caught_up {
						Some(vec![Action::HandoverFence])
					} else if waited {
						give_up(if whole {
							Reason::Behind
						} else {
							Reason::NotWhole
						})
					} else {
						None
					}
				}
				RequestKind::Replace => {
					let ready = f
						.candidate
						.is_some_and(|c| c.flush_lsn.saturating_add(CATCH_UP_SLACK) >= f.lsn);
					if whole && ready {
						Some(vec![Action::GoBoth])
					} else if waited {
						give_up(if whole {
							Reason::CandidateNotReady
						} else {
							Reason::NotWhole
						})
					} else {
						None
					}
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	const T: u64 = 10_000;

	fn primary(names: SyncNames, standby: Option<StandbySeen>) -> Postgres {
		Postgres::Primary {
			names,
			standby,
			candidate: None,
			lsn: 100,
			archive_fenced: None,
		}
	}

	fn archiving(names: SyncNames, standby: Option<StandbySeen>, fenced: bool) -> Postgres {
		Postgres::Primary {
			names,
			standby,
			candidate: None,
			lsn: 100,
			archive_fenced: Some(fenced),
		}
	}

	fn seen(age: u64, sync: bool, flush: u64) -> Option<StandbySeen> {
		Some(StandbySeen {
			reply_age_ms: age,
			sync,
			flush_lsn: flush,
			replay_lsn: flush,
		})
	}

	fn renewed(now: u64, eligible: bool) -> Memory {
		Memory {
			epoch: 1,
			renewed_at: Some(now),
			eligible: Some(eligible),
			sync_from_lsn: Some(50),
			..Memory::default()
		}
	}

	#[test]
	fn the_standby_is_cleared_at_the_witness_before_commits_go_on_without_it() {
		let gone = primary(SyncNames::Standby, None);
		assert_eq!(
			decide(1_000, T, gone, &renewed(1_000, true)),
			[Action::Renew, Action::Eligible(false)]
		);
		assert_eq!(
			decide(1_000, T, gone, &renewed(1_000, false)),
			[Action::Renew, Action::GoAsync]
		);
		let unknown = Memory {
			eligible: None,
			..renewed(1_000, true)
		};
		assert_eq!(
			decide(1_000, T, gone, &unknown),
			[Action::Renew, Action::Eligible(false)]
		);
	}

	#[test]
	fn the_standby_is_vouched_for_only_once_synchronous_and_past_the_switch() {
		let m = renewed(1_000, false);
		assert_eq!(
			decide(1_000, T, primary(SyncNames::Off, seen(100, false, 0)), &m),
			[Action::Renew, Action::GoSync]
		);
		assert_eq!(
			decide(
				1_000,
				T,
				primary(SyncNames::Standby, seen(100, false, 60)),
				&m
			),
			[Action::Renew]
		);
		assert_eq!(
			decide(
				1_000,
				T,
				primary(SyncNames::Standby, seen(100, true, 40)),
				&m
			),
			[Action::Renew]
		);
		assert_eq!(
			decide(
				1_000,
				T,
				primary(SyncNames::Standby, seen(100, true, 50)),
				&m
			),
			[Action::Renew, Action::Eligible(true)]
		);
		let unknown = Memory {
			sync_from_lsn: None,
			..m
		};
		assert_eq!(
			decide(
				1_000,
				T,
				primary(SyncNames::Standby, seen(100, true, 999)),
				&unknown
			),
			[Action::Renew]
		);
	}

	#[test]
	fn a_primary_with_neither_lease_nor_standby_fences_itself() {
		let m = renewed(0, true);
		assert_eq!(
			decide(T / 2, T, primary(SyncNames::Standby, None), &m),
			[Action::Renew, Action::Fence]
		);
		assert_eq!(
			decide(
				T / 2,
				T,
				primary(SyncNames::Standby, seen(100, true, 60)),
				&m
			),
			[Action::Renew]
		);
		assert_eq!(
			decide(
				T / 2 - 1,
				T,
				primary(SyncNames::Off, None),
				&renewed(0, false)
			),
			[Action::Renew]
		);
		assert_eq!(
			decide(T / 2, T, primary(SyncNames::Off, None), &renewed(0, false)),
			[Action::Renew, Action::Fence]
		);
	}

	#[test]
	fn a_fence_ends_with_the_lease_or_a_streaming_standby_and_goes_back_to_synchronous() {
		let stale = renewed(0, true);
		assert_eq!(
			decide(T, T, primary(SyncNames::Fenced, None), &stale),
			[Action::Renew]
		);
		assert_eq!(
			decide(T, T, primary(SyncNames::Fenced, None), &renewed(T, true)),
			[Action::Renew, Action::Unfence]
		);
		assert_eq!(
			decide(
				T,
				T,
				primary(SyncNames::Fenced, seen(100, true, 60)),
				&stale
			),
			[Action::Renew, Action::Unfence]
		);
	}

	#[test]
	fn a_restart_that_finds_commits_unguarded_fences_until_the_witness_answers() {
		let fresh = Memory {
			epoch: 1,
			..Memory::default()
		};
		assert_eq!(
			decide(0, T, primary(SyncNames::Off, seen(10, false, 0)), &fresh),
			[Action::Renew, Action::Fence]
		);
		assert_eq!(
			decide(0, T, primary(SyncNames::Standby, seen(10, true, 0)), &fresh),
			[Action::Renew]
		);
	}

	#[test]
	fn a_grant_clears_a_demotion_learned_before_it() {
		// The old primary, demoted by a handover, rejoined as the standby and was handed the primary
		// back: the grant must clear the demotion, or a restart reads it fenced for good.
		let mut mem = Memory {
			epoch: 3,
			demoted: true,
			..Memory::default()
		};
		mem.next_call();
		mem.heard(T, false, &Answer::Granted { epoch: 4 });
		assert!(!mem.demoted);
		assert_eq!(mem.granted, Some(4));
		assert_eq!(mem.epoch, 4);
	}

	#[test]
	fn a_demoted_copy_stays_fenced_and_stops_renewing() {
		let m = Memory {
			demoted: true,
			..renewed(0, true)
		};
		assert_eq!(
			decide(0, T, primary(SyncNames::Standby, seen(1, true, 1)), &m),
			[Action::Fence, Action::EndStreams]
		);
		assert!(decide(0, T, primary(SyncNames::Fenced, None), &m).is_empty());
	}

	#[test]
	fn the_archive_is_guarded_after_the_fence_and_tried_again_while_fenced() {
		// Cut: no lease, no standby. The fence first, so a failure guarding the archive cannot keep
		// commits from stopping.
		let lapsed = renewed(0, true);
		assert_eq!(
			decide(T, T, archiving(SyncNames::Standby, None, false), &lapsed),
			[Action::Renew, Action::Fence, Action::FenceArchive]
		);
		// Already fenced with the archive still open (the last reload failed): still fenced, and
		// the archive tried again. Nothing unfences it for that.
		assert_eq!(
			decide(T, T, archiving(SyncNames::Fenced, None, false), &lapsed),
			[Action::Renew, Action::FenceArchive]
		);
		assert_eq!(
			decide(T, T, archiving(SyncNames::Fenced, None, true), &lapsed),
			[Action::Renew]
		);
	}

	#[test]
	fn the_archive_opens_with_the_fence_and_not_before() {
		// The lease is back: out of the fence, and the archive with it.
		assert_eq!(
			decide(
				1_000,
				T,
				archiving(SyncNames::Fenced, None, true),
				&renewed(1_000, true)
			),
			[Action::Renew, Action::Unfence, Action::UnfenceArchive]
		);
		// A copy found guarded while not fenced (an unfence whose archive half failed) opens it.
		assert_eq!(
			decide(
				1_000,
				T,
				archiving(SyncNames::Standby, seen(100, true, 60), true),
				&renewed(1_000, true)
			),
			[Action::Renew, Action::UnfenceArchive]
		);
		// Healthy and open: nothing to do. No archive_command: nothing to guard, ever.
		assert_eq!(
			decide(
				1_000,
				T,
				archiving(SyncNames::Standby, seen(100, true, 60), false),
				&renewed(1_000, true)
			),
			[Action::Renew]
		);
		assert_eq!(
			decide(T, T, primary(SyncNames::Standby, None), &renewed(0, true)),
			[Action::Renew, Action::Fence]
		);
	}

	#[test]
	fn a_demoted_copy_guards_its_archive_too() {
		let m = Memory {
			demoted: true,
			..renewed(0, true)
		};
		assert_eq!(
			decide(
				0,
				T,
				archiving(SyncNames::Standby, seen(1, true, 1), false),
				&m
			),
			[Action::Fence, Action::EndStreams, Action::FenceArchive]
		);
		assert_eq!(
			decide(0, T, archiving(SyncNames::Fenced, None, false), &m),
			[Action::FenceArchive]
		);
		assert!(decide(0, T, archiving(SyncNames::Fenced, None, true), &m).is_empty());
	}

	#[test]
	fn the_standby_asks_once_the_stream_is_gone_and_promotes_when_granted() {
		let starting = Memory {
			epoch: 1,
			..Memory::default()
		};
		assert_eq!(
			decide(
				0,
				T,
				Postgres::Standby {
					receive_age_ms: Some(1)
				},
				&starting
			),
			[Action::Ask],
			"once, on starting"
		);
		let m = Memory {
			heard: true,
			..starting
		};
		assert!(
			decide(
				0,
				T,
				Postgres::Standby {
					receive_age_ms: Some(T - 1)
				},
				&m
			)
			.is_empty()
		);
		assert_eq!(
			decide(
				0,
				T,
				Postgres::Standby {
					receive_age_ms: Some(T)
				},
				&m
			),
			[Action::Ask]
		);
		assert_eq!(
			decide(
				0,
				T,
				Postgres::Standby {
					receive_age_ms: None
				},
				&m
			),
			[Action::Ask]
		);
		let granted = Memory {
			granted: Some(2),
			..m
		};
		assert_eq!(
			decide(
				0,
				T,
				Postgres::Standby {
					receive_age_ms: None
				},
				&granted
			),
			[Action::Promote(2)]
		);
	}

	#[test]
	fn a_promotion_that_outlasted_its_wait_is_recorded_and_still_fenced_without_a_lease() {
		let late = Memory {
			epoch: 2,
			granted: Some(2),
			..Memory::default()
		};
		let primary = Postgres::Primary {
			names: SyncNames::Off,
			standby: None,
			candidate: None,
			lsn: 0,
			archive_fenced: None,
		};
		assert_eq!(
			decide(10_000, T, primary, &late),
			[Action::Promote(2), Action::Renew, Action::Fence]
		);
		let recorded = Memory::promoted(2, 10_000);
		assert_eq!(recorded.granted, None);
		assert_eq!(decide(10_000, T, primary, &recorded), [Action::Renew]);
	}

	fn asked(kind: RequestKind, to: &str) -> Memory {
		Memory {
			request: Some(Request {
				id: "x".into(),
				kind,
				to: to.into(),
			}),
			request_heard_at: Some(0),
			standby: Some("b".into()),
			..renewed(1_000, true)
		}
	}

	fn two(
		names: SyncNames,
		standby: Option<StandbySeen>,
		candidate: Option<StandbySeen>,
	) -> Postgres {
		Postgres::Primary {
			names,
			standby,
			candidate,
			lsn: 100,
			archive_fenced: None,
		}
	}

	#[test]
	fn a_handover_fences_only_a_whole_caught_up_pair_and_gives_up_on_one_that_is_not() {
		let m = asked(RequestKind::Handover, "b");
		assert_eq!(
			decide(
				1_000,
				T,
				primary(SyncNames::Standby, seen(100, true, 90)),
				&m
			),
			[Action::Renew, Action::HandoverFence]
		);
		// Not vouched for yet: the ordinary rules, until two leases have passed.
		let unvouched = Memory {
			eligible: Some(false),
			..m.clone()
		};
		assert_eq!(
			decide(
				1_000,
				T,
				primary(SyncNames::Standby, seen(100, true, 40)),
				&unvouched
			),
			[Action::Renew]
		);
		let late = Memory {
			renewed_at: Some(2 * T + 1),
			..unvouched
		};
		assert_eq!(
			decide(
				2 * T + 1,
				T,
				primary(SyncNames::Standby, seen(100, true, 40)),
				&late
			),
			[Action::Renew, Action::Abandon(Reason::NotWhole)]
		);
	}

	#[test]
	fn a_handover_is_sent_once_the_standby_replayed_everything_and_stops_on_anything_wrong() {
		let fencing = Memory {
			change: Some(Change::Fencing {
				id: "x".into(),
				since: 1_000,
			}),
			..asked(RequestKind::Handover, "b")
		};
		let behind = StandbySeen {
			reply_age_ms: 100,
			sync: true,
			flush_lsn: 100,
			replay_lsn: 99,
		};
		let caught = StandbySeen {
			replay_lsn: 100,
			..behind
		};
		assert_eq!(
			decide(1_500, T, primary(SyncNames::Fenced, Some(behind)), &fencing),
			[Action::Renew]
		);
		assert_eq!(
			decide(1_500, T, primary(SyncNames::Fenced, Some(caught)), &fencing),
			[Action::Renew, Action::Handover]
		);
		// The standby gone: out of the fence (the lease holds), and the request given up.
		assert_eq!(
			decide(1_500, T, primary(SyncNames::Fenced, None), &fencing),
			[
				Action::Renew,
				Action::Unfence,
				Action::Abandon(Reason::StandbyGone)
			]
		);
		// The witness silent for half a lease: out of the fence on the standby's word.
		let silent = Memory {
			renewed_at: Some(0),
			..fencing.clone()
		};
		assert_eq!(
			decide(T / 2, T, primary(SyncNames::Fenced, Some(behind)), &silent),
			[
				Action::Renew,
				Action::Unfence,
				Action::Abandon(Reason::WitnessSilent)
			]
		);
		// Fenced a lease and still behind.
		let long = Memory {
			renewed_at: Some(1_000 + T + 1),
			..fencing.clone()
		};
		assert_eq!(
			decide(
				1_000 + T + 1,
				T,
				primary(SyncNames::Fenced, Some(behind)),
				&long
			),
			[
				Action::Renew,
				Action::Unfence,
				Action::Abandon(Reason::Behind)
			]
		);
		// No longer asked (the witness ended it): out of the fence, nothing to say.
		let ended = Memory {
			request: None,
			..fencing
		};
		assert_eq!(
			decide(1_500, T, primary(SyncNames::Fenced, Some(behind)), &ended),
			[Action::Renew, Action::Unfence, Action::Resolved]
		);
	}

	#[test]
	fn a_handover_sent_stays_fenced_until_a_later_renewal_says_how_it_went() {
		let sent = Memory {
			change: Some(Change::HandoverSent {
				id: "x".into(),
				from: 6,
			}),
			calls: 5,
			lease_call: 4,
			..asked(RequestKind::Handover, "b")
		};
		// A lease and a streaming standby would each end an ordinary fence; not this one.
		assert_eq!(
			decide(
				1_000,
				T,
				primary(SyncNames::Fenced, seen(100, true, 100)),
				&sent
			),
			[Action::Renew]
		);
		let answered = Memory {
			lease_call: 6,
			calls: 6,
			..sent.clone()
		};
		assert_eq!(
			decide(
				1_000,
				T,
				primary(SyncNames::Fenced, seen(100, true, 100)),
				&answered
			),
			[Action::Renew, Action::Handover]
		);
		let not_taken = Memory {
			request: None,
			..answered
		};
		assert_eq!(
			decide(
				1_000,
				T,
				primary(SyncNames::Fenced, seen(100, true, 100)),
				&not_taken
			),
			[Action::Renew, Action::Resolved, Action::Unfence]
		);
	}

	#[test]
	fn a_demoted_copy_ends_the_stream_of_its_standby_so_it_asks() {
		let m = Memory {
			demoted: true,
			..renewed(0, true)
		};
		assert_eq!(
			decide(0, T, primary(SyncNames::Fenced, seen(1, true, 100)), &m),
			[Action::EndStreams]
		);
		assert!(decide(0, T, primary(SyncNames::Fenced, None), &m).is_empty());
	}

	#[test]
	fn a_replacement_names_both_and_switches_only_once_the_new_copy_has_flushed_past_it() {
		let m = asked(RequestKind::Replace, "c");
		assert_eq!(
			decide(
				1_000,
				T,
				two(SyncNames::Standby, seen(100, true, 100), None),
				&m
			),
			[Action::Renew]
		);
		assert_eq!(
			decide(
				1_000,
				T,
				two(
					SyncNames::Standby,
					seen(100, true, 100),
					seen(100, false, 90)
				),
				&m
			),
			[Action::Renew, Action::GoBoth]
		);
		let both = Memory {
			change: Some(Change::Both {
				id: "x".into(),
				to: "c".into(),
				since: 1_000,
			}),
			candidate_from_lsn: Some(95),
			..m.clone()
		};
		assert_eq!(
			decide(
				1_000,
				T,
				two(SyncNames::Both, seen(100, true, 100), seen(100, true, 94)),
				&both
			),
			[Action::Renew]
		);
		assert_eq!(
			decide(
				1_000,
				T,
				two(SyncNames::Both, seen(100, true, 100), seen(100, true, 95)),
				&both
			),
			[Action::Renew, Action::Replace]
		);
		// The new copy gone before the step: back to the standby alone, which has everything.
		assert_eq!(
			decide(
				1_000,
				T,
				two(SyncNames::Both, seen(100, true, 100), None),
				&both
			),
			[
				Action::Renew,
				Action::GoSync,
				Action::Abandon(Reason::CandidateNotReady)
			]
		);
	}

	#[test]
	fn a_replacement_sent_keeps_both_named_until_the_witness_says_which_is_the_standby() {
		let sent = Memory {
			change: Some(Change::ReplaceSent {
				id: "x".into(),
				to: "c".into(),
				from: 5,
			}),
			calls: 5,
			lease_call: 4,
			candidate_from_lsn: Some(95),
			..asked(RequestKind::Replace, "c")
		};
		// Even with the new copy gone: either may be the standby the witness holds.
		assert_eq!(
			decide(
				1_000,
				T,
				two(SyncNames::Both, seen(100, true, 100), None),
				&sent
			),
			[Action::Renew]
		);
		let taken = Memory {
			lease_call: 5,
			standby: Some("c".into()),
			request: None,
			..sent.clone()
		};
		assert_eq!(
			decide(
				1_000,
				T,
				two(SyncNames::Both, seen(100, true, 100), None),
				&taken
			),
			[Action::Renew, Action::Resolved, Action::GoSync]
		);
		let refused = Memory {
			lease_call: 6,
			calls: 6,
			request: None,
			..sent
		};
		assert_eq!(
			decide(
				1_000,
				T,
				two(SyncNames::Both, seen(100, true, 100), None),
				&refused
			),
			[Action::Renew, Action::Resolved, Action::GoSync]
		);
	}

	#[test]
	fn two_named_after_a_restart_stay_so_until_the_witness_has_said_who_is_the_standby() {
		let fresh = Memory {
			epoch: 1,
			..Memory::default()
		};
		let both = two(SyncNames::Both, seen(100, true, 100), seen(100, true, 100));
		assert_eq!(decide(0, T, both, &fresh), [Action::Renew]);
		let still_asked = asked(RequestKind::Replace, "c");
		assert_eq!(
			decide(1_000, T, both, &still_asked),
			[Action::Renew, Action::Abandon(Reason::Restarted)]
		);
		let settled = Memory {
			request: None,
			standby: Some("c".into()),
			..renewed(1_000, true)
		};
		assert_eq!(
			decide(1_000, T, both, &settled),
			[Action::Renew, Action::GoSync]
		);
	}
}
