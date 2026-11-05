//! The guard's decision (L21): what one copy's guard does this tick, from what its Postgres shows
//! and what the witness last said. Pure; `run.rs` reads the facts and carries the actions out, and
//! the simulator (`sim.rs`) does the same against a model.
//!
//! The rule everything else serves: a commit is acknowledged without the standby only after the
//! witness has recorded that the standby is not `eligible` (`Eligible(false)` before `GoAsync`), and
//! the standby is vouched for only once it is synchronous and has flushed past the LSN of the moment
//! it became so (`GoSync`, then `Eligible(true)`). The lease is renewed every tick and decides only
//! when a cut primary stops serving (`Fence`).

use super::witness::Answer;

/// What `synchronous_standby_names` says on a primary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncNames {
	/// The standby alone: no commit is acknowledged before it has flushed it.
	Standby,
	/// Empty: commits are acknowledged by this copy alone.
	Off,
	/// A name no standby has (`FENCED_NAME`), with the copy read-only: nothing commits.
	Fenced,
}

/// The standby as the primary sees it (`pg_stat_replication`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StandbySeen {
	/// Since the standby's last reply (`reply_time`).
	pub reply_age_ms: u64,
	/// `sync_state = 'sync'`.
	pub sync: bool,
	pub flush_lsn: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Postgres {
	Down,
	Primary {
		names: SyncNames,
		standby: Option<StandbySeen>,
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

/// What the guard remembers between ticks. Only `epoch` is kept across a restart (the guard's
/// state file); the rest starts empty, which is always the cautious reading.
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
}

impl Memory {
	/// What an answer from the witness teaches, to any call. `sent_at`: when the call went out (a
	/// lease is counted from then); `primary`: this copy's Postgres is one.
	pub fn heard(&mut self, sent_at: u64, primary: bool, answer: &Answer) {
		match answer {
			Answer::Lease { eligible, .. } => {
				self.renewed_at = Some(sent_at);
				self.eligible = Some(*eligible);
			}
			Answer::Granted { epoch } => {
				self.epoch = *epoch;
				self.granted = Some(*epoch);
			}
			// A primary at an older epoch has been replaced; a standby learns whose it is.
			Answer::Demoted { epoch, .. } => {
				if primary {
					self.demoted = true;
				} else {
					self.epoch = *epoch;
				}
			}
			Answer::Held { .. } | Answer::Refused { .. } => {}
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
	/// `synchronous_standby_names` to the standby, and remember the LSN.
	GoSync,
	/// Read-only, sessions ended, `synchronous_standby_names` to `FENCED_NAME`.
	Fence,
	/// Out of the fence, into `SyncNames::Standby` (never straight to `Off`).
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
}

/// The name a fenced copy waits for. No standby ever connects with it.
pub const FENCED_NAME: &str = "lepis_fenced";

pub fn decide(now: u64, lease_ms: u64, pg: Postgres, mem: &Memory) -> Vec<Action> {
	match pg {
		Postgres::Down => Vec::new(),
		Postgres::Standby { receive_age_ms } => {
			if let Some(epoch) = mem.granted {
				return vec![Action::Promote(epoch)];
			}
			if receive_age_ms.is_none_or(|age| age >= lease_ms) {
				vec![Action::Ask]
			} else {
				Vec::new()
			}
		}
		Postgres::Primary {
			names,
			standby,
			archive_fenced,
			..
		} => {
			if mem.demoted {
				let mut out = if names == SyncNames::Fenced {
					Vec::new()
				} else {
					vec![Action::Fence]
				};
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
			match names {
				// A standby streaming from this copy cannot have been promoted, so either one ends the
				// fence; commits wait for the standby again until the witness agrees otherwise.
				SyncNames::Fenced => {
					if lease || up.is_some() {
						out.push(Action::Unfence);
					}
				}
				// Not confirmed by the witness since a restart, with commits acknowledged alone: an
				// operator may have promoted the standby with `accept_loss` meanwhile.
				SyncNames::Off if mem.renewed_at.is_none() => out.push(Action::Fence),
				_ if !lease && up.is_none() => out.push(Action::Fence),
				SyncNames::Standby => match up {
					None => match mem.eligible {
						Some(false) => out.push(Action::GoAsync),
						_ => out.push(Action::Eligible(false)),
					},
					Some(s) => {
						let past = mem.sync_from_lsn.is_some_and(|from| s.flush_lsn >= from);
						if s.sync && past && mem.eligible == Some(false) {
							out.push(Action::Eligible(true));
						}
					}
				},
				SyncNames::Off => {
					if up.is_some() {
						out.push(Action::GoSync);
					}
				}
			}
			// The archive follows the fence: guarded from the tick that fences (after the fence,
			// so a failure here cannot keep it from happening) until the tick that unfences.
			let fenced = out.contains(&Action::Fence)
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
