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
//!
//! The control plane may ask a pair for one change at a time (`Request`, through the operator):
//! a planned handover to the standby, or a new standby in place of the old one. The primary hears
//! it on its renewal and carries it out (decide.rs); the witness takes the step that moves who is
//! what (`Handover`, `Replace`) only from the primary at its epoch, for the request still asked, so
//! a copy of the call that arrives after the request ended is refused. A failover ends whatever is
//! asked. A pair moving to another witness is frozen first (leases renewed, nothing changed),
//! taken over there with the same state (`adopt`), and then every call here is answered `Moved`.

use serde_json::{Value, json};

/// How long a change the control plane asked for may take, in leases, before the witness ends it
/// itself: the primary's guard gives up well before (two leases waiting for its standby, one fenced).
pub const REQUEST_LEASES: u64 = 6;

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
	/// A change asked of the pair, which the primary's guard carries out; persisted.
	pub request: Option<Request>,
	/// How the last request ended, for the witness's host to report; persisted.
	pub ended: Option<Ended>,
	/// The pair moving to another witness (`freeze`, then `moved`); persisted.
	pub moving: Option<Moving>,
	/// When `request` was made, on the witness's clock; not persisted (a restart starts it again).
	pub request_at: u64,
	/// Taken over from another witness and not yet called: the lease starts at the first call, so
	/// a primary still renewing at the old witness is never replaced here early. Not persisted.
	pub adopted: bool,
	/// The standby was named by a replacement since the last grant, so a primary's guard whose
	/// definition may still name the old one could vouch for the wrong copy: a vouch must name the
	/// standby (a guard from before replacements cannot, and is refused until the next grant), and a
	/// witness from before replacements, which could not tell, refuses to start on the file.
	/// Persisted.
	pub replaced: bool,
}

/// What may be asked of a pair, by the control plane through the witness's operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestKind {
	/// The primary hands its place to its standby: a planned switchover, no acknowledged commit lost.
	Handover,
	/// The standby is replaced by `to`, which streams already: made before the old one goes.
	Replace,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
	/// The caller's id, so the end it reads is the end of what it asked.
	pub id: String,
	pub kind: RequestKind,
	/// Handover: the standby that becomes the primary. Replace: the new standby's copy name.
	pub to: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ended {
	pub id: String,
	pub kind: RequestKind,
	/// Carried out (true), or given up (`reason` says why).
	pub done: bool,
	pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Moving {
	/// Being moved: the state is final (it is being copied to the new witness), so nothing changes
	/// it; leases are still renewed, so the primary keeps serving.
	Frozen,
	/// Moved to this `host:port`: every call is answered with it, and nothing here changes again.
	Moved(String),
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
		/// The standby vouched for (a guard from before replacements names none).
		standby: Option<String>,
	},
	/// The standby: it has heard nothing from the primary for `stream_lost_ms`.
	Ask {
		copy: String,
		epoch: u64,
		stream_lost_ms: u64,
	},
	/// The primary, fenced, its standby having replayed all it wrote: the standby is the primary
	/// now (the request `id`, a handover).
	Handover {
		copy: String,
		epoch: u64,
		id: String,
	},
	/// The primary: the request's new standby has every acknowledged commit (it was named beside the
	/// old one and has flushed past the moment it was), so it is the standby now, eligible.
	Replace {
		copy: String,
		epoch: u64,
		id: String,
	},
	/// The primary gives the request up, saying why.
	Abandon {
		copy: String,
		epoch: u64,
		id: String,
		reason: String,
	},
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Answer {
	/// The lease is renewed (or `eligible` set), until `lease_ms` from now. `standby` is whom the
	/// witness holds the standby, and `request` what is asked of the pair, if anything.
	Lease {
		epoch: u64,
		lease_ms: u64,
		eligible: bool,
		standby: Option<String>,
		request: Option<Request>,
	},
	/// The asker is the primary now, at `epoch`.
	Granted { epoch: u64 },
	/// Not now; `reason` says what is missing.
	Held { reason: String },
	/// The caller's epoch is over: `primary` holds `epoch`. It never writes again until rebuilt.
	Demoted { epoch: u64, primary: String },
	/// The call does not fit the caller (the wrong role, an epoch never granted).
	Refused { reason: String },
	/// The pair is kept by the witness at `witness` (`host:port`) now: ask there.
	Moved { witness: String },
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
			request: None,
			ended: None,
			moving: None,
			request_at: now,
			adopted: false,
			replaced: false,
		}
	}

	/// A cluster read back from the state file after a restart: every lease held for one more term,
	/// and a request's time counted again from now.
	pub fn restored(mut self, now: u64) -> Cluster {
		self.lease_until = now + self.lease_ms;
		self.request_at = now;
		self
	}

	/// The state another witness froze, taken over here (`moving`): the same epoch, primary,
	/// standby, `eligible` and `accept_loss`, so the pair's guards carry on as they were. No
	/// request (freezing ended any) and no lease until the first call.
	pub fn adopt(frozen: &Cluster, lease_ms: u64) -> Cluster {
		Cluster {
			epoch: frozen.epoch,
			primary: frozen.primary.clone(),
			standby: frozen.standby.clone(),
			eligible: frozen.eligible,
			lease_ms,
			accept_loss: frozen.accept_loss,
			lease_until: 0,
			request: None,
			ended: None,
			moving: None,
			request_at: 0,
			adopted: true,
			replaced: frozen.replaced,
		}
	}

	fn end(&mut self, done: bool, reason: &str) {
		if let Some(r) = self.request.take() {
			self.ended = Some(Ended {
				id: r.id,
				kind: r.kind,
				done,
				reason: reason.to_string(),
			});
		}
	}

	/// Ends a request the primary's guard has not carried out in time. True when it did.
	pub fn expire(&mut self, now: u64) -> bool {
		if self.request.is_some() && now >= self.request_at + REQUEST_LEASES * self.lease_ms {
			self.end(false, "the primary's guard did not carry it out in time");
			return true;
		}
		false
	}

	/// The operator's: asks the pair for `request`. `Ok(true)` when it is newly asked, `Ok(false)`
	/// when it was asked already (the same id, pending or ended), `Err` with the reason it cannot be.
	pub fn ask_for(&mut self, request: Request, now: u64) -> Result<bool, String> {
		self.expire(now);
		if self.moving.is_some() {
			return Err("the pair is moving to another witness".into());
		}
		if let Some(r) = &self.request {
			if r.id == request.id {
				return Ok(false);
			}
			return Err(format!("another change is in flight: {}", r.id));
		}
		if self.ended.as_ref().is_some_and(|e| e.id == request.id) {
			return Ok(false);
		}
		match request.kind {
			RequestKind::Handover => {
				if self.standby.as_deref() != Some(request.to.as_str()) {
					return Err("a handover goes to the pair's standby".into());
				}
				if !self.eligible {
					return Err("the standby may be missing acknowledged commits".into());
				}
			}
			RequestKind::Replace => {
				if request.to == self.primary
					|| self.standby.as_deref() == Some(request.to.as_str())
				{
					return Err("the new standby is a third copy".into());
				}
			}
		}
		self.request = Some(request);
		self.request_at = now;
		Ok(true)
	}

	/// The operator's, before the pair moves to another witness: any request ended, and nothing
	/// changes the state from here. True when it changed.
	pub fn freeze(&mut self) -> bool {
		if self.moving.is_some() {
			return false;
		}
		self.end(false, "the witness is moving this pair");
		self.moving = Some(Moving::Frozen);
		true
	}

	/// The operator's, when a move is given up before the new witness took the pair over (it could
	/// not, or its version does not take an `adopt`): the freeze ends and nothing else changed.
	pub fn thaw(&mut self) -> Result<bool, String> {
		match &self.moving {
			None => Ok(false),
			Some(Moving::Frozen) => {
				self.moving = None;
				Ok(true)
			}
			Some(Moving::Moved(to)) => {
				Err(format!("moved to {to} already: the pair is held there now"))
			}
		}
	}

	/// The operator's, once the new witness holds the frozen state: every call is sent there.
	pub fn moved(&mut self, to: &str) -> Result<bool, String> {
		match &self.moving {
			Some(Moving::Frozen) => {
				self.moving = Some(Moving::Moved(to.to_string()));
				Ok(true)
			}
			Some(Moving::Moved(at)) if at == to => Ok(false),
			Some(Moving::Moved(at)) => Err(format!("moved to {at} already")),
			None => Err("freeze it first: the new witness must hold its final state".into()),
		}
	}

	/// Answers one call. The bool is whether the state changed in a way the file must hold before
	/// the answer goes out (a renewal only moves the lease, which is never persisted).
	pub fn handle(&mut self, call: &Call, now: u64) -> (Answer, bool) {
		if let Some(Moving::Moved(to)) = &self.moving {
			return (
				Answer::Moved {
					witness: to.clone(),
				},
				false,
			);
		}
		if self.adopted {
			self.adopted = false;
			self.lease_until = now + self.lease_ms;
		}
		let expired = self.expire(now);
		let (answer, changed) = self.answer(call, now);
		(answer, changed || expired)
	}

	fn answer(&mut self, call: &Call, now: u64) -> (Answer, bool) {
		let (copy, epoch) = match call {
			Call::Renew { copy, epoch }
			| Call::Eligible { copy, epoch, .. }
			| Call::Ask { copy, epoch, .. }
			| Call::Handover { copy, epoch, .. }
			| Call::Replace { copy, epoch, .. }
			| Call::Abandon { copy, epoch, .. } => (copy, *epoch),
		};
		// The primary asking: granted, and its guard restarted before it promoted, or a handover's
		// new primary, which may still hold the epoch before it. Told again, at the epoch it holds.
		if let Call::Ask { .. } = call
			&& *copy == self.primary
		{
			return (Answer::Granted { epoch: self.epoch }, false);
		}
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
		let frozen = self.moving == Some(Moving::Frozen);
		let primary_only = |what: &str| {
			(
				Answer::Refused {
					reason: format!("only the primary {what}"),
				},
				false,
			)
		};
		match call {
			Call::Renew { .. } => {
				if *copy != self.primary {
					return primary_only("renews");
				}
				self.lease_until = now + self.lease_ms;
				(self.lease(), false)
			}
			Call::Eligible {
				eligible, standby, ..
			} => {
				if *copy != self.primary {
					return primary_only("vouches for its standby");
				}
				if *eligible && standby.is_some() && *standby != self.standby {
					return (
						Answer::Refused {
							reason: "the standby is another copy than the one vouched for".into(),
						},
						false,
					);
				}
				if *eligible && standby.is_none() && self.replaced {
					return (
						Answer::Refused {
							reason: "since the standby was replaced, a vouch names it".into(),
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
				if changed && frozen {
					return (
						Answer::Held {
							reason: "the witness is moving this pair".into(),
						},
						false,
					);
				}
				self.eligible = *eligible;
				self.lease_until = now + self.lease_ms;
				(self.lease(), changed)
			}
			Call::Ask { stream_lost_ms, .. } => {
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
				if frozen {
					return (
						Answer::Held {
							reason: "the witness is moving this pair".into(),
						},
						false,
					);
				}
				// A failover outranks whatever was asked of the pair.
				self.end(false, "the primary failed over");
				let old = std::mem::replace(&mut self.primary, copy.clone());
				self.epoch += 1;
				self.standby = Some(old);
				self.eligible = false;
				self.accept_loss = false;
				self.replaced = false;
				self.lease_until = now + self.lease_ms;
				(Answer::Granted { epoch: self.epoch }, true)
			}
			Call::Handover { id, .. } | Call::Replace { id, .. } => {
				if *copy != self.primary {
					return primary_only("carries out a change");
				}
				let kind = if matches!(call, Call::Handover { .. }) {
					RequestKind::Handover
				} else {
					RequestKind::Replace
				};
				let Some(r) = self
					.request
					.clone()
					.filter(|r| r.id == *id && r.kind == kind)
				else {
					// Never asked, or ended: a copy of the call that arrives late is refused too.
					return (
						Answer::Refused {
							reason: "no such change is asked of this pair".into(),
						},
						false,
					);
				};
				match kind {
					RequestKind::Handover => {
						let why = if self.standby.as_deref() != Some(r.to.as_str()) {
							Some("the standby is another copy now")
						} else if !self.eligible {
							Some("the standby may be missing acknowledged commits")
						} else {
							None
						};
						if let Some(why) = why {
							self.end(false, why);
							return (Answer::Refused { reason: why.into() }, true);
						}
						self.end(true, "handed over");
						let old = std::mem::replace(&mut self.primary, r.to);
						self.epoch += 1;
						self.standby = Some(old);
						self.eligible = false;
						self.accept_loss = false;
						self.replaced = false;
						self.lease_until = now + self.lease_ms;
						(
							Answer::Demoted {
								epoch: self.epoch,
								primary: self.primary.clone(),
							},
							true,
						)
					}
					RequestKind::Replace => {
						self.end(true, "replaced");
						self.standby = Some(r.to);
						self.eligible = true;
						self.replaced = true;
						self.lease_until = now + self.lease_ms;
						(self.lease(), true)
					}
				}
			}
			Call::Abandon { id, reason, .. } => {
				if *copy != self.primary {
					return primary_only("gives a change up");
				}
				let pending = self.request.as_ref().is_some_and(|r| r.id == *id);
				if pending {
					self.end(false, reason);
				}
				self.lease_until = now + self.lease_ms;
				(self.lease(), pending)
			}
		}
	}

	fn lease(&self) -> Answer {
		Answer::Lease {
			epoch: self.epoch,
			lease_ms: self.lease_ms,
			eligible: self.eligible,
			standby: self.standby.clone(),
			request: self.request.clone(),
		}
	}

	/// The state file's form (no lease: see the module comment). A pair being moved, or whose
	/// standby a replacement named, keeps its lease term under another name (`lease_ms_held`), so a
	/// witness from before moves and replacements, which would forget the freeze or the redirect or
	/// take a vouch for the wrong copy, refuses to start on it instead.
	pub fn to_json(&self) -> Value {
		let lease_key = if self.moving.is_some() || self.replaced {
			"lease_ms_held"
		} else {
			"lease_ms"
		};
		let mut v = json!({
			"epoch": self.epoch,
			"primary": self.primary,
			"standby": self.standby,
			"eligible": self.eligible,
			"accept_loss": self.accept_loss,
			"request": self.request.as_ref().map(Request::to_json),
			"ended": self.ended.as_ref().map(|e| json!({
				"id": e.id, "kind": e.kind.as_str(), "done": e.done, "reason": e.reason,
			})),
			"moving": match &self.moving {
				None => Value::Null,
				Some(Moving::Frozen) => json!("frozen"),
				Some(Moving::Moved(to)) => json!({"moved": to}),
			},
			"replaced": self.replaced,
		});
		v[lease_key] = json!(self.lease_ms);
		v
	}

	pub fn from_json(v: &Value, now: u64) -> Option<Cluster> {
		let lease_ms = v
			.get("lease_ms")
			.or_else(|| v.get("lease_ms_held"))?
			.as_u64()?;
		let ended = v.get("ended").filter(|e| !e.is_null()).and_then(|e| {
			Some(Ended {
				id: e.get("id")?.as_str()?.to_string(),
				kind: RequestKind::parse(e.get("kind")?.as_str()?)?,
				done: e.get("done")?.as_bool()?,
				reason: e.get("reason")?.as_str()?.to_string(),
			})
		});
		let moving = match v.get("moving") {
			Some(Value::String(s)) if s == "frozen" => Some(Moving::Frozen),
			Some(m) if m.get("moved").is_some() => {
				Some(Moving::Moved(m.get("moved")?.as_str()?.to_string()))
			}
			_ => None,
		};
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
				request: v.get("request").and_then(Request::from_json),
				ended,
				moving,
				request_at: 0,
				adopted: false,
				replaced: v.get("replaced").and_then(Value::as_bool).unwrap_or(false),
			}
			.restored(now),
		)
	}
}

impl RequestKind {
	pub fn as_str(self) -> &'static str {
		match self {
			RequestKind::Handover => "handover",
			RequestKind::Replace => "replace",
		}
	}

	pub fn parse(s: &str) -> Option<RequestKind> {
		match s {
			"handover" => Some(RequestKind::Handover),
			"replace" => Some(RequestKind::Replace),
			_ => None,
		}
	}
}

impl Request {
	pub fn to_json(&self) -> Value {
		json!({"id": self.id, "kind": self.kind.as_str(), "to": self.to})
	}

	pub fn from_json(v: &Value) -> Option<Request> {
		Some(Request {
			id: v.get("id")?.as_str()?.to_string(),
			kind: RequestKind::parse(v.get("kind")?.as_str()?)?,
			to: v.get("to")?.as_str()?.to_string(),
		})
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
				standby,
			} => {
				let mut v =
					json!({"call": "eligible", "copy": copy, "epoch": epoch, "eligible": eligible});
				// Only when named, so a witness from before reads the same call.
				if let Some(s) = standby {
					v["standby"] = json!(s);
				}
				v
			}
			Call::Ask {
				copy,
				epoch,
				stream_lost_ms,
			} => {
				json!({"call": "ask", "copy": copy, "epoch": epoch, "stream_lost_ms": stream_lost_ms})
			}
			Call::Handover { copy, epoch, id } => {
				json!({"call": "handover", "copy": copy, "epoch": epoch, "id": id})
			}
			Call::Replace { copy, epoch, id } => {
				json!({"call": "replace", "copy": copy, "epoch": epoch, "id": id})
			}
			Call::Abandon {
				copy,
				epoch,
				id,
				reason,
			} => {
				json!({"call": "abandon", "copy": copy, "epoch": epoch, "id": id, "reason": reason})
			}
		}
	}

	pub fn from_json(v: &Value) -> Option<Call> {
		let copy = v.get("copy")?.as_str()?.to_string();
		let epoch = v.get("epoch")?.as_u64()?;
		let id = || v.get("id").and_then(Value::as_str).map(str::to_string);
		match v.get("call")?.as_str()? {
			"renew" => Some(Call::Renew { copy, epoch }),
			"eligible" => Some(Call::Eligible {
				copy,
				epoch,
				eligible: v.get("eligible")?.as_bool()?,
				standby: v.get("standby").and_then(Value::as_str).map(str::to_string),
			}),
			"ask" => Some(Call::Ask {
				copy,
				epoch,
				stream_lost_ms: v.get("stream_lost_ms")?.as_u64()?,
			}),
			"handover" => Some(Call::Handover {
				copy,
				epoch,
				id: id()?,
			}),
			"replace" => Some(Call::Replace {
				copy,
				epoch,
				id: id()?,
			}),
			"abandon" => Some(Call::Abandon {
				copy,
				epoch,
				id: id()?,
				reason: v.get("reason")?.as_str()?.to_string(),
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
				standby,
				request,
			} => {
				let mut v = json!({"answer": "lease", "epoch": epoch, "lease_ms": lease_ms, "eligible": eligible});
				// Only when there is something to say, so a guard from before reads the same answer.
				if let Some(s) = standby {
					v["standby"] = json!(s);
				}
				if let Some(r) = request {
					v["request"] = r.to_json();
				}
				v
			}
			Answer::Granted { epoch } => json!({"answer": "granted", "epoch": epoch}),
			Answer::Held { reason } => json!({"answer": "held", "reason": reason}),
			Answer::Demoted { epoch, primary } => {
				json!({"answer": "demoted", "epoch": epoch, "primary": primary})
			}
			Answer::Refused { reason } => json!({"answer": "refused", "reason": reason}),
			Answer::Moved { witness } => json!({"answer": "moved", "witness": witness}),
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
				standby: v.get("standby").and_then(Value::as_str).map(str::to_string),
				request: v.get("request").and_then(Request::from_json),
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
			"moved" => Some(Answer::Moved {
				witness: v.get("witness")?.as_str()?.to_string(),
			}),
			_ => None,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn cluster() -> Cluster {
		Cluster::new("a", Some("b"), 10_000, 0)
	}

	fn ask(epoch: u64, lost: u64) -> Call {
		Call::Ask {
			copy: "b".into(),
			epoch,
			stream_lost_ms: lost,
		}
	}

	#[test]
	fn a_grant_waits_for_the_lease_the_stream_and_eligible() {
		let mut c = cluster();
		assert_eq!(
			c.handle(&ask(1, 20_000), 5_000).0,
			Answer::Held {
				reason: "the primary's lease holds".into()
			}
		);
		assert_eq!(
			c.handle(&ask(1, 20_000), 11_000).0,
			Answer::Held {
				reason: "the standby may be missing acknowledged commits".into()
			}
		);
		c.handle(
			&Call::Eligible {
				copy: "a".into(),
				epoch: 1,
				eligible: true,
				standby: None,
			},
			11_000,
		);
		assert_eq!(
			c.handle(&ask(1, 5_000), 30_000).0,
			Answer::Held {
				reason: "the standby still hears the primary".into()
			}
		);
		assert_eq!(
			c.handle(&ask(1, 20_000), 30_000),
			(Answer::Granted { epoch: 2 }, true)
		);
		assert_eq!(
			(c.primary.as_str(), c.standby.as_deref(), c.eligible),
			("b", Some("a"), false)
		);
	}

	#[test]
	fn the_old_primary_is_told_it_is_over() {
		let mut c = cluster();
		c.handle(
			&Call::Eligible {
				copy: "a".into(),
				epoch: 1,
				eligible: true,
				standby: None,
			},
			0,
		);
		c.handle(&ask(1, 20_000), 20_000);
		assert_eq!(
			c.handle(
				&Call::Renew {
					copy: "a".into(),
					epoch: 1
				},
				20_001
			)
			.0,
			Answer::Demoted {
				epoch: 2,
				primary: "b".into()
			}
		);
		assert_eq!(
			c.handle(
				&Call::Eligible {
					copy: "a".into(),
					epoch: 1,
					eligible: false,
					standby: None,
				},
				20_001
			)
			.0,
			Answer::Demoted {
				epoch: 2,
				primary: "b".into()
			}
		);
	}

	#[test]
	fn roles_and_epochs_are_checked() {
		let mut c = cluster();
		assert!(matches!(
			c.handle(
				&Call::Renew {
					copy: "b".into(),
					epoch: 1
				},
				0
			)
			.0,
			Answer::Refused { .. }
		));
		assert!(matches!(
			c.handle(
				&Call::Renew {
					copy: "a".into(),
					epoch: 7
				},
				0
			)
			.0,
			Answer::Refused { .. }
		));
		assert!(matches!(
			c.handle(
				&Call::Ask {
					copy: "c".into(),
					epoch: 1,
					stream_lost_ms: 1 << 40
				},
				99_999
			)
			.0,
			Answer::Refused { .. }
		));
		assert_eq!(
			c.handle(
				&Call::Ask {
					copy: "a".into(),
					epoch: 1,
					stream_lost_ms: 0
				},
				0
			)
			.0,
			Answer::Granted { epoch: 1 }
		);
		let mut lone = Cluster::new("a", None, 10_000, 0);
		assert!(matches!(
			lone.handle(
				&Call::Eligible {
					copy: "a".into(),
					epoch: 1,
					eligible: true,
					standby: None,
				},
				0
			)
			.0,
			Answer::Refused { .. }
		));
	}

	#[test]
	fn accept_loss_skips_eligible_once() {
		let mut c = cluster();
		c.accept_loss = true;
		assert_eq!(
			c.handle(&ask(1, 20_000), 20_000).0,
			Answer::Granted { epoch: 2 }
		);
		assert!(!c.accept_loss);
	}

	#[test]
	fn a_restart_holds_the_lease_one_more_term() {
		let mut c = cluster();
		c.handle(
			&Call::Eligible {
				copy: "a".into(),
				epoch: 1,
				eligible: true,
				standby: None,
			},
			0,
		);
		let mut back = Cluster::from_json(&c.to_json(), 50_000).unwrap();
		assert_eq!(
			back.handle(&ask(1, 1 << 40), 55_000).0,
			Answer::Held {
				reason: "the primary's lease holds".into()
			}
		);
		assert_eq!(
			back.handle(&ask(1, 1 << 40), 60_000).0,
			Answer::Granted { epoch: 2 }
		);
	}

	#[test]
	fn the_wire_forms_round_trip() {
		for call in [
			Call::Renew {
				copy: "a".into(),
				epoch: 3,
			},
			Call::Eligible {
				copy: "a".into(),
				epoch: 3,
				eligible: true,
				standby: None,
			},
			ask(3, 12),
			Call::Handover {
				copy: "a".into(),
				epoch: 3,
				id: "x".into(),
			},
			Call::Replace {
				copy: "a".into(),
				epoch: 3,
				id: "x".into(),
			},
			Call::Abandon {
				copy: "a".into(),
				epoch: 3,
				id: "x".into(),
				reason: "behind".into(),
			},
		] {
			assert_eq!(Call::from_json(&call.to_json()), Some(call));
		}
		for answer in [
			Answer::Lease {
				epoch: 2,
				lease_ms: 10,
				eligible: true,
				standby: None,
				request: None,
			},
			Answer::Lease {
				epoch: 2,
				lease_ms: 10,
				eligible: true,
				standby: Some("b".into()),
				request: Some(handover("x")),
			},
			Answer::Granted { epoch: 4 },
			Answer::Demoted {
				epoch: 4,
				primary: "b".into(),
			},
			Answer::Moved {
				witness: "10.0.0.9:7445".into(),
			},
		] {
			assert_eq!(Answer::from_json(&answer.to_json()), Some(answer));
		}
		// A lease with nothing asked reads as it did before the fields existed.
		let plain = Answer::Lease {
			epoch: 2,
			lease_ms: 10,
			eligible: false,
			standby: None,
			request: None,
		};
		assert_eq!(
			plain.to_json(),
			json!({"answer": "lease", "epoch": 2, "lease_ms": 10, "eligible": false})
		);
	}

	fn handover(id: &str) -> Request {
		Request {
			id: id.into(),
			kind: RequestKind::Handover,
			to: "b".into(),
		}
	}

	fn replace(id: &str, to: &str) -> Request {
		Request {
			id: id.into(),
			kind: RequestKind::Replace,
			to: to.into(),
		}
	}

	fn eligible(c: &mut Cluster, now: u64) {
		c.handle(
			&Call::Eligible {
				copy: "a".into(),
				epoch: c.epoch,
				eligible: true,
				standby: None,
			},
			now,
		);
	}

	fn hand(c: &mut Cluster, id: &str, now: u64) -> (Answer, bool) {
		c.handle(
			&Call::Handover {
				copy: "a".into(),
				epoch: 1,
				id: id.into(),
			},
			now,
		)
	}

	#[test]
	fn a_handover_is_asked_for_an_eligible_standby_and_moves_the_epoch() {
		let mut c = cluster();
		assert!(c.ask_for(handover("x"), 0).is_err(), "not eligible yet");
		eligible(&mut c, 0);
		assert!(
			c.ask_for(
				Request {
					to: "c".into(),
					..handover("x")
				},
				0
			)
			.is_err(),
			"only to the standby"
		);
		assert_eq!(c.ask_for(handover("x"), 0), Ok(true));
		assert_eq!(c.ask_for(handover("x"), 0), Ok(false), "the same ask again");
		assert!(c.ask_for(handover("y"), 0).is_err(), "one at a time");
		// The primary hears it on its renewal.
		let (lease, _) = c.handle(
			&Call::Renew {
				copy: "a".into(),
				epoch: 1,
			},
			100,
		);
		assert!(matches!(lease, Answer::Lease { request: Some(r), .. } if r.id == "x"));
		assert_eq!(
			hand(&mut c, "x", 200),
			(
				Answer::Demoted {
					epoch: 2,
					primary: "b".into()
				},
				true
			)
		);
		assert_eq!(
			(
				c.primary.as_str(),
				c.standby.as_deref(),
				c.eligible,
				c.epoch
			),
			("b", Some("a"), false, 2)
		);
		assert_eq!(
			c.ended.as_ref().map(|e| (e.id.as_str(), e.done)),
			Some(("x", true))
		);
		// A late copy of the call is told the epoch is over; the new primary is granted on asking.
		assert!(matches!(hand(&mut c, "x", 300).0, Answer::Demoted { .. }));
		assert_eq!(
			c.handle(
				&Call::Ask {
					copy: "b".into(),
					epoch: 1,
					stream_lost_ms: 0
				},
				300
			)
			.0,
			Answer::Granted { epoch: 2 }
		);
	}

	#[test]
	fn a_handover_not_asked_or_ended_is_refused_and_a_late_copy_too() {
		let mut c = cluster();
		eligible(&mut c, 0);
		assert!(matches!(hand(&mut c, "x", 0).0, Answer::Refused { .. }));
		c.ask_for(handover("x"), 0).unwrap();
		// Given up by the primary's guard: ended, and a copy of a handover sent before arrives late.
		c.handle(
			&Call::Abandon {
				copy: "a".into(),
				epoch: 1,
				id: "x".into(),
				reason: "the standby fell behind".into(),
			},
			100,
		);
		assert_eq!(
			c.ended.as_ref().map(|e| e.reason.as_str()),
			Some("the standby fell behind")
		);
		assert_eq!(
			hand(&mut c, "x", 200),
			(
				Answer::Refused {
					reason: "no such change is asked of this pair".into()
				},
				false
			)
		);
		assert_eq!((c.primary.as_str(), c.epoch), ("a", 1));
		// Not carried out in time: the witness ends it itself.
		c.ask_for(handover("y"), 1_000).unwrap();
		let (_, changed) = c.handle(
			&Call::Renew {
				copy: "a".into(),
				epoch: 1,
			},
			1_000 + REQUEST_LEASES * 10_000,
		);
		assert!(changed && c.request.is_none());
		assert_eq!(
			c.ended.as_ref().map(|e| (e.id.as_str(), e.done)),
			Some(("y", false))
		);
		// The standby no longer eligible when the handover arrives: ended, nothing moved.
		c.ask_for(handover("z"), 100_000).unwrap();
		c.handle(
			&Call::Eligible {
				copy: "a".into(),
				epoch: 1,
				eligible: false,
				standby: None,
			},
			100_000,
		);
		assert!(matches!(
			hand(&mut c, "z", 100_001),
			(Answer::Refused { .. }, true)
		));
		assert_eq!(
			(c.primary.as_str(), c.epoch, c.request.is_none()),
			("a", 1, true)
		);
	}

	#[test]
	fn a_failover_ends_a_change_in_flight() {
		let mut c = cluster();
		eligible(&mut c, 0);
		c.ask_for(handover("x"), 0).unwrap();
		assert_eq!(
			c.handle(&ask(1, 20_000), 20_000).0,
			Answer::Granted { epoch: 2 }
		);
		assert_eq!(
			c.ended.as_ref().map(|e| (e.id.as_str(), e.done)),
			Some(("x", false))
		);
		assert!(matches!(
			hand(&mut c, "x", 20_001).0,
			Answer::Demoted { .. }
		));
	}

	#[test]
	fn a_replacement_makes_the_new_copy_the_eligible_standby() {
		let mut c = cluster();
		assert!(c.ask_for(replace("r", "a"), 0).is_err());
		assert!(c.ask_for(replace("r", "b"), 0).is_err());
		assert_eq!(c.ask_for(replace("r", "c"), 0), Ok(true));
		let (answer, changed) = c.handle(
			&Call::Replace {
				copy: "a".into(),
				epoch: 1,
				id: "r".into(),
			},
			10,
		);
		assert!(changed);
		assert!(matches!(
			answer,
			Answer::Lease { standby: Some(ref s), eligible: true, request: None, .. } if s == "c"
		));
		// The old standby asks nothing of it any more; the new one is the one granted on a loss.
		assert!(matches!(
			c.handle(
				&Call::Ask {
					copy: "b".into(),
					epoch: 1,
					stream_lost_ms: 1 << 40
				},
				99_999
			)
			.0,
			Answer::Refused { .. }
		));
		assert_eq!(
			c.handle(
				&Call::Ask {
					copy: "c".into(),
					epoch: 1,
					stream_lost_ms: 1 << 40
				},
				99_999
			)
			.0,
			Answer::Granted { epoch: 2 }
		);
	}

	#[test]
	fn a_frozen_pair_keeps_its_lease_changes_nothing_and_then_points_elsewhere() {
		let mut c = cluster();
		eligible(&mut c, 0);
		c.ask_for(handover("x"), 0).unwrap();
		assert!(c.freeze());
		assert_eq!(
			c.ended.as_ref().map(|e| (e.id.as_str(), e.done)),
			Some(("x", false))
		);
		assert!(c.ask_for(handover("y"), 0).is_err());
		assert!(matches!(
			c.handle(
				&Call::Renew {
					copy: "a".into(),
					epoch: 1
				},
				1_000
			),
			(Answer::Lease { .. }, false)
		));
		// Clearing eligible would change the state being copied: held.
		assert!(matches!(
			c.handle(
				&Call::Eligible {
					copy: "a".into(),
					epoch: 1,
					eligible: false,
					standby: None,
				},
				1_000
			),
			(Answer::Held { .. }, false)
		));
		// Nor is the standby granted, even with the lease out.
		assert!(matches!(
			c.handle(&ask(1, 1 << 40), 1 << 30).0,
			Answer::Held { .. }
		));
		let state = c.clone();
		assert!(c.moved("10.0.0.9:7445").unwrap());
		assert_eq!(
			c.handle(&ask(1, 1 << 40), 1 << 30),
			(
				Answer::Moved {
					witness: "10.0.0.9:7445".into()
				},
				false
			)
		);
		// Taken over elsewhere: the same epoch and roles, and the lease starts at the first call.
		let mut there = Cluster::adopt(&state, 10_000);
		assert_eq!(
			(there.epoch, there.primary.as_str(), there.eligible),
			(1, "a", true)
		);
		assert_eq!(
			there.handle(&ask(1, 1 << 40), 5_000_000).0,
			Answer::Held {
				reason: "the primary's lease holds".into()
			}
		);
		assert_eq!(
			there.handle(&ask(1, 1 << 40), 5_010_000).0,
			Answer::Granted { epoch: 2 }
		);
		let back = Cluster::from_json(&c.to_json(), 0).unwrap();
		assert_eq!(back.moving, Some(Moving::Moved("10.0.0.9:7445".into())));
		assert_eq!(back.ended, c.ended);
	}
}
