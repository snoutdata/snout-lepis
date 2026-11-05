//! The guard and the witness as they were before a pair could be moved (afde9ba1f, the 0.1.19
//! behaviour, which the control plane's own pair runs and is never upgraded in our order), kept
//! whole for the mixed-version tests only: `sim.rs` runs them beside the new ones, every call and
//! answer and state file crossing between them as JSON, as on the wire and on disk.

#![allow(dead_code)]

pub mod decide;
pub mod witness;

use serde_json::Value;

/// The old guard's state file, as its `read_epoch` read it: the epoch, required, or the guard does
/// not start.
pub fn read_epoch(v: &Value) -> Option<u64> {
	v.get("epoch").and_then(Value::as_u64)
}
