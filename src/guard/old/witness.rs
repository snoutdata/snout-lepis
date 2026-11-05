//! The witness's state machine (L21): the third vote, pure, no I/O. `serve.rs` puts it behind HTTP
//! and a state file; the simulator (`sim.rs`) drives it directly.
//!
//! Per cluster it holds who the primary is, at which epoch, who its standby is, the primary's lease
//! and `eligible`: whether every commit the primary has acknowledged is on the standby. The
//! primary's guard is the only one that sets `eligible`, and it clears it BEFORE it lets a commit
//! through without the standby (decide.rs), so a grant made here never loses an acknowledged commit.
//! The lease only decides when: a grant waits for it to run out.
//!
//! Time is the witness's own clock in milliseconds; the lease is never persisted. A witness that
//! restarts holds every lease for one more `lease_ms` (`Cluster::restored`), so a primary that
//! renewed just before the restart is never replaced early.

use serde_json::{Value, json};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cluster {
	pub epoch: u64,
	pub primary: String,
	pub standby: Option<String>,
	pub eligible: bool,
	pub lease_ms: u64,
	/// An operator allowed the next grant to skip `eligible` (`accept_loss`); used once.
	pub accept_loss: bool,
	/// On the witness's clock; not persisted.
	pub lease_until: u64,
}

/// What a guard asks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Call {
	/// The primary, every tick.
	Renew { copy: String, epoch: u64 },
	/// The primary: its standby does (true) or may not (false) have every acknowledged commit.
	Eligible {
		copy: String,
		epoch: u64,
		eligible: bool,
	},
	/// The standby: it has heard nothing from the primary for `stream_lost_ms`.
	Ask {
		copy: String,
		epoch: u64,
		stream_lost_ms: u64,
	},
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Answer {
	/// The lease is renewed (or `eligible` set), until `lease_ms` from now.
	Lease {
		epoch: u64,
		lease_ms: u64,
		eligible: bool,
	},
	/// The asker is the primary now, at `epoch`.
	Granted { epoch: u64 },
	/// Not now; `reason` says what is missing.
	Held { reason: String },
	/// The caller's epoch is over: `primary` holds `epoch`. It never writes again until rebuilt.
	Demoted { epoch: u64, primary: String },
	/// The call does not fit the caller (the wrong role, an epoch never granted).
	Refused { reason: String },
}

impl Cluster {
	pub fn new(primary: &str, standby: Option<&str>, lease_ms: u64, now: u64) -> Cluster {
		Cluster {
			epoch: 1,
			primary: primary.to_string(),
			standby: standby.map(str::to_string),
			eligible: false,
			lease_ms,
			accept_loss: false,
			lease_until: now + lease_ms,
		}
	}

	/// A cluster read back from the state file after a restart: every lease held for one more term.
	pub fn restored(mut self, now: u64) -> Cluster {
		self.lease_until = now + self.lease_ms;
		self
	}

	/// Answers one call. The bool is whether the state changed in a way the file must hold before
	/// the answer goes out (a renewal only moves the lease, which is never persisted).
	pub fn handle(&mut self, call: &Call, now: u64) -> (Answer, bool) {
		let (copy, epoch) = match call {
			Call::Renew { copy, epoch }
			| Call::Eligible { copy, epoch, .. }
			| Call::Ask { copy, epoch, .. } => (copy, *epoch),
		};
		if epoch < self.epoch {
			return (
				Answer::Demoted {
					epoch: self.epoch,
					primary: self.primary.clone(),
				},
				false,
			);
		}
		if epoch > self.epoch {
			return (
				Answer::Refused {
					reason: "an epoch this witness never granted".into(),
				},
				false,
			);
		}
		match call {
			Call::Renew { .. } => {
				if *copy != self.primary {
					return (
						Answer::Refused {
							reason: "only the primary renews".into(),
						},
						false,
					);
				}
				self.lease_until = now + self.lease_ms;
				(self.lease(), false)
			}
			Call::Eligible { eligible, .. } => {
				if *copy != self.primary {
					return (
						Answer::Refused {
							reason: "only the primary vouches for its standby".into(),
						},
						false,
					);
				}
				if *eligible && self.standby.is_none() {
					return (
						Answer::Refused {
							reason: "the cluster has no standby".into(),
						},
						false,
					);
				}
				let changed = self.eligible != *eligible;
				self.eligible = *eligible;
				self.lease_until = now + self.lease_ms;
				(self.lease(), changed)
			}
			Call::Ask { stream_lost_ms, .. } => {
				// Granted already, and its guard restarted before it promoted: tell it again.
				if *copy == self.primary {
					return (Answer::Granted { epoch: self.epoch }, false);
				}
				if self.standby.as_deref() != Some(copy.as_str()) {
					return (
						Answer::Refused {
							reason: "only the standby asks".into(),
						},
						false,
					);
				}
				if *stream_lost_ms < self.lease_ms {
					return (
						Answer::Held {
							reason: "the standby still hears the primary".into(),
						},
						false,
					);
				}
				if now < self.lease_until {
					return (
						Answer::Held {
							reason: "the primary's lease holds".into(),
						},
						false,
					);
				}
				if !self.eligible && !self.accept_loss {
					return (
						Answer::Held {
							reason: "the standby may be missing acknowledged commits".into(),
						},
						false,
					);
				}
				let old = std::mem::replace(&mut self.primary, copy.clone());
				self.epoch += 1;
				self.standby = Some(old);
				self.eligible = false;
				self.accept_loss = false;
				self.lease_until = now + self.lease_ms;
				(Answer::Granted { epoch: self.epoch }, true)
			}
		}
	}

	fn lease(&self) -> Answer {
		Answer::Lease {
			epoch: self.epoch,
			lease_ms: self.lease_ms,
			eligible: self.eligible,
		}
	}

	/// The state file's form (no lease: see the module comment).
	pub fn to_json(&self) -> Value {
		json!({
			"epoch": self.epoch,
			"primary": self.primary,
			"standby": self.standby,
			"eligible": self.eligible,
			"lease_ms": self.lease_ms,
			"accept_loss": self.accept_loss,
		})
	}

	pub fn from_json(v: &Value, now: u64) -> Option<Cluster> {
		let lease_ms = v.get("lease_ms")?.as_u64()?;
		Some(
			Cluster {
				epoch: v.get("epoch")?.as_u64()?,
				primary: v.get("primary")?.as_str()?.to_string(),
				standby: v.get("standby").and_then(Value::as_str).map(str::to_string),
				eligible: v.get("eligible")?.as_bool()?,
				lease_ms,
				accept_loss: v
					.get("accept_loss")
					.and_then(Value::as_bool)
					.unwrap_or(false),
				lease_until: 0,
			}
			.restored(now),
		)
	}
}

impl Call {
	pub fn to_json(&self) -> Value {
		match self {
			Call::Renew { copy, epoch } => json!({"call": "renew", "copy": copy, "epoch": epoch}),
			Call::Eligible {
				copy,
				epoch,
				eligible,
			} => {
				json!({"call": "eligible", "copy": copy, "epoch": epoch, "eligible": eligible})
			}
			Call::Ask {
				copy,
				epoch,
				stream_lost_ms,
			} => {
				json!({"call": "ask", "copy": copy, "epoch": epoch, "stream_lost_ms": stream_lost_ms})
			}
		}
	}

	pub fn from_json(v: &Value) -> Option<Call> {
		let copy = v.get("copy")?.as_str()?.to_string();
		let epoch = v.get("epoch")?.as_u64()?;
		match v.get("call")?.as_str()? {
			"renew" => Some(Call::Renew { copy, epoch }),
			"eligible" => Some(Call::Eligible {
				copy,
				epoch,
				eligible: v.get("eligible")?.as_bool()?,
			}),
			"ask" => Some(Call::Ask {
				copy,
				epoch,
				stream_lost_ms: v.get("stream_lost_ms")?.as_u64()?,
			}),
			_ => None,
		}
	}
}

impl Answer {
	pub fn to_json(&self) -> Value {
		match self {
			Answer::Lease {
				epoch,
				lease_ms,
				eligible,
			} => {
				json!({"answer": "lease", "epoch": epoch, "lease_ms": lease_ms, "eligible": eligible})
			}
			Answer::Granted { epoch } => json!({"answer": "granted", "epoch": epoch}),
			Answer::Held { reason } => json!({"answer": "held", "reason": reason}),
			Answer::Demoted { epoch, primary } => {
				json!({"answer": "demoted", "epoch": epoch, "primary": primary})
			}
			Answer::Refused { reason } => json!({"answer": "refused", "reason": reason}),
		}
	}

	/// The guard's side.
	pub fn from_json(v: &Value) -> Option<Answer> {
		let reason = || {
			v.get("reason")
				.and_then(Value::as_str)
				.unwrap_or("")
				.to_string()
		};
		match v.get("answer")?.as_str()? {
			"lease" => Some(Answer::Lease {
				epoch: v.get("epoch")?.as_u64()?,
				lease_ms: v.get("lease_ms")?.as_u64()?,
				eligible: v.get("eligible")?.as_bool()?,
			}),
			"granted" => Some(Answer::Granted {
				epoch: v.get("epoch")?.as_u64()?,
			}),
			"held" => Some(Answer::Held { reason: reason() }),
			"demoted" => Some(Answer::Demoted {
				epoch: v.get("epoch")?.as_u64()?,
				primary: v.get("primary")?.as_str()?.to_string(),
			}),
			"refused" => Some(Answer::Refused { reason: reason() }),
			_ => None,
		}
	}
}
