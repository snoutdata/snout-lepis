//! How many connections this router may hold on one node, and the line for them.
//!
//! In transaction mode each backend (in a client's hands or idle) holds a slot of its node. A client
//! that wants one while none is free waits its turn, in order, until `LEPIS_QUEUE_WAIT_MS`. A slot is
//! given back when its backend closes, and an idle backend is closed to make room for a client of
//! another role (`router.rs`, `slot_for`). A session that keeps its own connection is not counted
//! here: session mode and `LEPIS_SESSION_ROLES` hold what they connect with, up to the node's limit.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

/// One node's slots and its line.
pub struct Gate {
	slots: Arc<Semaphore>,
	line: Mutex<VecDeque<(u64, Arc<Notify>)>>,
	next: AtomicU64,
}

/// A slot of a node, held for as long as its backend is open.
pub struct Slot {
	permit: Option<OwnedSemaphorePermit>,
	gate: Arc<Gate>,
}

impl Drop for Slot {
	fn drop(&mut self) {
		// The permit first: whoever this wakes finds it free.
		self.permit.take();
		self.gate.wake();
	}
}

/// A place in a node's line. Leaving it, however that happens, wakes whoever is next.
pub struct Ticket {
	id: u64,
	notify: Arc<Notify>,
	gate: Arc<Gate>,
}

impl Drop for Ticket {
	fn drop(&mut self) {
		let id = self.id;
		self.gate
			.line
			.lock()
			.expect("line")
			.retain(|(one, _)| *one != id);
		self.gate.wake();
	}
}

impl Ticket {
	/// Whether it is this client's turn.
	pub fn first(&self) -> bool {
		self.gate
			.line
			.lock()
			.expect("line")
			.front()
			.is_some_and(|(one, _)| *one == self.id)
	}

	/// Waits to be woken. False once the deadline has passed.
	pub async fn wait(&self, deadline: Instant) -> bool {
		tokio::time::timeout_at(deadline, self.notify.notified())
			.await
			.is_ok()
	}
}

impl Gate {
	fn new(size: usize) -> Arc<Gate> {
		Arc::new(Gate {
			slots: Arc::new(Semaphore::new(size)),
			line: Mutex::new(VecDeque::new()),
			next: AtomicU64::new(0),
		})
	}

	/// A free slot, if there is one.
	pub fn try_slot(self: &Arc<Self>) -> Option<Slot> {
		let permit = self.slots.clone().try_acquire_owned().ok()?;
		Some(Slot {
			permit: Some(permit),
			gate: self.clone(),
		})
	}

	/// Takes the last place in the line.
	pub fn join(self: &Arc<Self>) -> Ticket {
		let id = self.next.fetch_add(1, Ordering::Relaxed);
		let notify = Arc::new(Notify::new());
		self.line
			.lock()
			.expect("line")
			.push_back((id, notify.clone()));
		Ticket {
			id,
			notify,
			gate: self.clone(),
		}
	}

	/// Something came free: the client at the front looks.
	pub fn wake(&self) {
		if let Some((_, notify)) = self.line.lock().expect("line").front() {
			notify.notify_one();
		}
	}

	/// Slots free now (tests, and what a refusal is measured against).
	pub fn free(&self) -> usize {
		self.slots.available_permits()
	}
}

/// Every node's gate, by the pool's node key (`host:port/database`). No gate at size 0.
pub struct Gates {
	size: usize,
	pub wait: Duration,
	gates: Mutex<HashMap<String, Arc<Gate>>>,
}

impl Gates {
	pub fn new(size: usize, wait: Duration) -> Gates {
		Gates {
			size,
			wait,
			gates: Mutex::new(HashMap::new()),
		}
	}

	pub fn of(&self, node: &str) -> Option<Arc<Gate>> {
		if self.size == 0 {
			return None;
		}
		Some(
			self.gates
				.lock()
				.expect("gates")
				.entry(node.to_string())
				.or_insert_with(|| Gate::new(self.size))
				.clone(),
		)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn soon() -> Instant {
		Instant::now() + Duration::from_millis(50)
	}

	#[test]
	fn no_size_is_no_gate() {
		assert!(Gates::new(0, Duration::ZERO).of("a:1/d").is_none());
	}

	#[test]
	fn a_node_has_its_own_slots_and_a_closed_one_comes_back() {
		let gates = Gates::new(1, Duration::ZERO);
		let a = gates.of("a:1/d").unwrap();
		let held = a.try_slot().unwrap();
		assert!(a.try_slot().is_none());
		assert!(gates.of("b:1/d").unwrap().try_slot().is_some());
		drop(held);
		assert_eq!(a.free(), 1);
	}

	#[tokio::test]
	async fn the_line_goes_in_order_and_a_freed_slot_wakes_its_front() {
		let gate = Gates::new(1, Duration::ZERO).of("a:1/d").unwrap();
		let held = gate.try_slot().unwrap();
		let first = gate.join();
		let second = gate.join();
		assert!(first.first());
		assert!(!second.first());
		assert!(!first.wait(soon()).await, "nothing came free yet");
		drop(held);
		assert!(first.wait(soon()).await, "the freed slot wakes the front");
		assert!(!second.wait(soon()).await, "and only the front is woken");
		drop(first);
		assert!(second.first());
		assert!(second.wait(soon()).await, "leaving wakes the next");
	}

	#[tokio::test]
	async fn a_wake_before_the_wait_is_not_lost() {
		let gate = Gates::new(1, Duration::ZERO).of("a:1/d").unwrap();
		let ticket = gate.join();
		gate.wake();
		assert!(ticket.wait(soon()).await);
	}
}
