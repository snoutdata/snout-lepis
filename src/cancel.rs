//! Cancel requests. Every client gets a process id and key minted by Lepis, never a node's, so a
//! client cannot cancel a session it does not own, and a 3.2 client's long key works in front of
//! a node that only knows 3.0's four bytes. A cancel is forwarded to whatever node the client's
//! session is using.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::backend;
use crate::config::NodeAddress;
use crate::scram::constant_time_eq;
use crate::wire;

/// Where a client's cancel goes.
#[derive(Clone)]
pub struct Target {
	pub node: NodeAddress,
	pub tls: Option<Arc<rustls::ClientConfig>>,
	pub pid: i32,
	pub key: Vec<u8>,
}

#[derive(Default)]
pub struct Registry {
	sessions: Mutex<HashMap<i32, (Vec<u8>, Target)>>,
}

impl Registry {
	/// Mints a process id and a key of `key_len` bytes and records where they cancel.
	pub fn register(&self, key_len: usize, target: Target) -> (i32, Vec<u8>) {
		let mut sessions = self.sessions.lock().expect("cancel registry");
		loop {
			let mut pid_bytes = [0u8; 4];
			getrandom::fill(&mut pid_bytes).expect("the operating system has randomness");
			// Positive and non-zero, like a real backend's.
			let pid = (i32::from_be_bytes(pid_bytes) & 0x7fff_ffff).max(1);
			if sessions.contains_key(&pid) {
				continue;
			}
			let mut key = vec![0u8; key_len];
			getrandom::fill(&mut key).expect("the operating system has randomness");
			sessions.insert(pid, (key.clone(), target));
			return (pid, key);
		}
	}

	/// Points a session's cancel at another node session (the router moved the client there).
	pub fn retarget(&self, pid: i32, target: Target) {
		if let Some((_, t)) = self.sessions.lock().expect("cancel registry").get_mut(&pid) {
			*t = target;
		}
	}

	pub fn forget(&self, pid: i32) {
		self.sessions.lock().expect("cancel registry").remove(&pid);
	}

	fn lookup(&self, pid: i32, key: &[u8]) -> Option<Target> {
		let sessions = self.sessions.lock().expect("cancel registry");
		let (k, t) = sessions.get(&pid)?;
		constant_time_eq(k, key).then(|| t.clone())
	}

	/// Forwards a client's cancel. Like Postgres, it answers nothing either way.
	pub async fn cancel(&self, pid: i32, key: &[u8]) {
		let Some(t) = self.lookup(pid, key) else {
			tracing::debug!(pid, "cancel for an unknown session ignored");
			return;
		};
		match backend::open(&t.node, t.tls.as_ref()).await {
			Ok(mut s) => {
				let _ = wire::write_all(&mut s, &wire::cancel_request(t.pid, &t.key)).await;
			}
			Err(e) => tracing::warn!(node = %t.node, "cancel not delivered: {e}"),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::config::SslMode;

	fn target() -> Target {
		Target {
			node: NodeAddress {
				host: "n".into(),
				port: 1,
				sslmode: SslMode::Disable,
				ca_file: None,
			},
			tls: None,
			pid: 7,
			key: vec![1, 2, 3, 4],
		}
	}

	#[test]
	fn keys_are_checked() {
		let r = Registry::default();
		let (pid, key) = r.register(32, target());
		assert_eq!(key.len(), 32);
		assert!(pid > 0);
		assert!(r.lookup(pid, &key).is_some());
		let mut wrong = key.clone();
		wrong[0] ^= 1;
		assert!(r.lookup(pid, &wrong).is_none());
		r.forget(pid);
		assert!(r.lookup(pid, &key).is_none());
	}
}
