//! snout-lepis: one Postgres database spread over many plain Postgres nodes.
//!
//! The comments name design decisions as L1…L15 and build phases as Phase 0…10; the README
//! says what each part does.

pub mod admin;
pub mod analyze;
pub mod auth;
pub mod backend;
pub mod cancel;
pub mod catalog;
pub mod config;
pub mod cron;
pub mod ddl;
pub mod frame;
pub mod hash;
pub mod live;
pub mod merge;
pub mod ops;
pub mod roles;
pub mod route;
pub mod router;
pub mod scatter;
pub mod scram;
pub mod server;
pub mod session;
pub mod tls;
pub mod twopc;
pub mod wire;
