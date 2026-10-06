//! Where a role's SCRAM verifier comes from: the home node, read over Lepis's own service login
//! and cached briefly. A role that does not exist, cannot log in, or has no SCRAM verifier gets a
//! mock verifier, so the exchange runs to the end and fails the same way a wrong password does:
//! a client cannot tell "no such role" from "wrong password" (what Postgres does too).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::backend::{self, Backend};
use crate::config::Config;
use crate::scram::{ClientCredential, Verifier};

const CACHE_FOR: Duration = Duration::from_secs(30);

pub struct VerifierSource {
	config: Arc<Config>,
	tls: Option<Arc<rustls::ClientConfig>>,
	conn: Mutex<Option<Backend>>,
	cache: std::sync::Mutex<HashMap<String, (Instant, Option<Verifier>)>>,
	mock_secret: [u8; 32],
}

impl VerifierSource {
	pub fn new(config: Arc<Config>, tls: Option<Arc<rustls::ClientConfig>>) -> Self {
		let mut mock_secret = [0u8; 32];
		getrandom::fill(&mut mock_secret).expect("the operating system has randomness");
		VerifierSource {
			config,
			tls,
			conn: Mutex::new(None),
			cache: Default::default(),
			mock_secret,
		}
	}

	/// The role's verifier, or a mock one that no proof can satisfy. The bool says which, for
	/// the log only.
	pub async fn verifier(&self, role: &str) -> Result<(Verifier, bool), String> {
		if let Some((at, v)) = self.cache.lock().expect("verifier cache").get(role)
			&& at.elapsed() < CACHE_FOR
		{
			return Ok(match v {
				Some(v) => (v.clone(), true),
				None => (self.mock(role), false),
			});
		}
		let found = self.lookup(role).await?;
		self.cache
			.lock()
			.expect("verifier cache")
			.insert(role.to_string(), (Instant::now(), found.clone()));
		Ok(match found {
			Some(v) => (v, true),
			None => (self.mock(role), false),
		})
	}

	/// After a failed login the cached verifier may be stale (the password was just changed).
	pub fn forget(&self, role: &str) {
		self.cache.lock().expect("verifier cache").remove(role);
	}

	async fn lookup(&self, role: &str) -> Result<Option<Verifier>, String> {
		let mut conn = self.conn.lock().await;
		for attempt in 0..2 {
			if conn.is_none() {
				*conn = Some(self.connect().await?);
			}
			let c = conn.as_mut().expect("just connected");
			match c.query(&self.config.verifier_query, &[role]).await {
				Ok(rows) => {
					let value = rows
						.into_iter()
						.next()
						.and_then(|r| r.into_iter().next().flatten());
					return Ok(value.and_then(|v| Verifier::parse(&v).ok()));
				}
				Err(e @ backend::BackendError::Refused(_)) => {
					return Err(format!("the verifier query failed: {e}"));
				}
				Err(e) => {
					*conn = None;
					if attempt == 1 {
						return Err(format!("the home node's service connection broke: {e}"));
					}
				}
			}
		}
		unreachable!("the loop returns")
	}

	async fn connect(&self) -> Result<Backend, String> {
		let s = &self.config.service;
		let params = vec![
			("user".to_string(), s.user.clone()),
			("database".to_string(), s.database.clone()),
			("application_name".to_string(), "snout-lepis".to_string()),
		];
		backend::connect(
			&self.config.home,
			self.tls.as_ref(),
			&params,
			ClientCredential::Password(s.password.clone()),
		)
		.await
		.map_err(|e| format!("service login to {}: {e}", self.config.home))
	}

	fn mock(&self, role: &str) -> Verifier {
		let mut h = Sha256::new();
		h.update(self.mock_secret);
		h.update(role.as_bytes());
		let d: [u8; 32] = h.finalize().into();
		let mut stored_key = [0u8; 32];
		let mut server_key = [0u8; 32];
		getrandom::fill(&mut stored_key).expect("the operating system has randomness");
		getrandom::fill(&mut server_key).expect("the operating system has randomness");
		Verifier {
			iterations: 4096,
			salt: d[..16].to_vec(),
			stored_key,
			server_key,
		}
	}
}
