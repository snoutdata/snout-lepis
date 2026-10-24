//! The home's own failover (L21, Phase 14): a guard beside each copy of a Postgres and a witness
//! as the third vote. `witness` and `decide` are the whole of the logic and are pure; the
//! simulator in `sim.rs` runs them against a model of synchronous replication under random
//! crashes and cut links.

pub mod decide;
pub mod run;
pub mod serve;
pub mod witness;

#[cfg(test)]
mod sim;
