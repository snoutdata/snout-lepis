//! The home's own failover (L21, Phase 14): a guard beside each copy of a Postgres and a witness
//! as the third vote. `witness` and `decide` are the whole of the logic and are pure; the
//! simulator in `sim.rs` runs them against a model of synchronous replication under random
//! crashes and cut links.

/// The build this binary is: the image's version, which the publish script
/// passes into the image build (`LEPIS_BUILD`), else the crate's (a build from a working tree). The
/// guard writes it into its status file and the witness gives it on `/v1/health`, so the control plane
/// asks a pair for a change only when its guards and its witness carry that change
/// (`cloud_guard_required_build`). A binary from before this says none, which reads as older.
pub const BUILD: &str = match option_env!("LEPIS_BUILD") {
	Some(b) if !b.is_empty() => b,
	_ => env!("CARGO_PKG_VERSION"),
};

pub mod decide;
pub mod run;
pub mod serve;
pub mod witness;

#[cfg(test)]
mod old;
#[cfg(test)]
mod sim;
