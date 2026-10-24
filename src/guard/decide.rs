//! The guard's decision (L21): what one copy's guard does this tick, from what its Postgres shows
//! and what the witness last said. Pure; `run.rs` reads the facts and carries the actions out, and
//! the simulator (`sim.rs`) does the same against a model.
//!
//! The rule everything else serves: a commit is acknowledged without the standby only after the
//! witness has recorded that the standby is not `eligible` (`Eligible(false)` before `GoAsync`), and
//! the standby is vouched for only once it is synchronous and has flushed past the LSN of the moment
//! it became so (`GoSync`, then `Eligible(true)`). The lease is renewed every tick and decides only
//! when a cut primary stops serving (`Fence`).

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
		Postgres::Primary { names, standby, .. } => {
			if mem.demoted {
				return if names == SyncNames::Fenced {
					Vec::new()
				} else {
					vec![Action::Fence]
				};
			}
			let mut out = vec![Action::Renew];
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
			out
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
			lsn: 100,
		}
	}

	fn seen(age: u64, sync: bool, flush: u64) -> Option<StandbySeen> {
		Some(StandbySeen {
			reply_age_ms: age,
			sync,
			flush_lsn: flush,
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
	fn a_demoted_copy_stays_fenced_and_stops_renewing() {
		let m = Memory {
			demoted: true,
			..renewed(0, true)
		};
		assert_eq!(
			decide(0, T, primary(SyncNames::Standby, seen(1, true, 1)), &m),
			[Action::Fence]
		);
		assert!(decide(0, T, primary(SyncNames::Fenced, None), &m).is_empty());
	}

	#[test]
	fn the_standby_asks_once_the_stream_is_gone_and_promotes_when_granted() {
		let m = Memory {
			epoch: 1,
			..Memory::default()
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
}
