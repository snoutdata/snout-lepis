//! Configuration, from the environment like every component in the stack.
//!
//! The only node named here is the HOME node (L6): the catalog on it says where everything else
//! is. Phase 0 routes every connection to it.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

/// How Lepis reaches a node, in libpq's `sslmode` words for the three that mean something.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SslMode {
	/// Plain TCP. Only for a node on the same host or a private network the operator trusts.
	Disable,
	/// Encrypted, certificate not checked.
	Require,
	/// Encrypted, certificate chain and host name checked. The default.
	VerifyFull,
}

impl SslMode {
	fn parse(s: &str) -> Option<SslMode> {
		Some(match s {
			"disable" => SslMode::Disable,
			"require" => SslMode::Require,
			"verify-full" => SslMode::VerifyFull,
			_ => return None,
		})
	}
}

#[derive(Clone, Debug)]
pub struct NodeAddress {
	pub host: String,
	pub port: u16,
	pub sslmode: SslMode,
	/// A PEM bundle to trust for `verify-full` instead of the public web roots.
	pub ca_file: Option<PathBuf>,
}

impl fmt::Display for NodeAddress {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}:{}", self.host, self.port)
	}
}

/// Lepis's own login to the home node, used to read role verifiers (and, from Phase 1, the
/// catalog). Never a client's credential.
#[derive(Clone)]
pub struct ServiceLogin {
	pub user: String,
	pub password: String,
	pub database: String,
}

impl fmt::Debug for ServiceLogin {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ServiceLogin")
			.field("user", &self.user)
			.field("database", &self.database)
			.finish_non_exhaustive()
	}
}

#[derive(Clone, Debug)]
pub struct Config {
	pub host: String,
	pub port: u16,
	pub tls_cert: Option<PathBuf>,
	pub tls_key: Option<PathBuf>,
	pub home: NodeAddress,
	pub service: ServiceLogin,
	/// Returns one row with one column, the role's `rolpassword`, for `$1` = the role name.
	pub verifier_query: String,
	pub max_clients: usize,
	/// How long the router routes by its catalog after it last heard from the home node; zero
	/// for no limit (live.rs `Freshness`).
	pub catalog_max_age: Duration,
}

pub const DEFAULT_VERIFIER_QUERY: &str = "select rolpassword from pg_catalog.pg_authid \
	where rolname = $1 and rolcanlogin and (rolvaliduntil is null or rolvaliduntil > now())";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(&self.0)
	}
}

impl std::error::Error for ConfigError {}

impl Config {
	pub fn from_env() -> Result<Config, ConfigError> {
		Config::from_map(&std::env::vars().collect())
	}

	pub fn from_map(env: &HashMap<String, String>) -> Result<Config, ConfigError> {
		let get = |k: &str| env.get(k).filter(|v| !v.is_empty()).cloned();
		let need = |k: &str| get(k).ok_or_else(|| ConfigError(format!("{k} is required")));
		let port = |k: &str, default: u16| -> Result<u16, ConfigError> {
			match get(k) {
				None => Ok(default),
				Some(v) => v
					.parse()
					.map_err(|_| ConfigError(format!("{k}: not a port: {v}"))),
			}
		};

		let (home_host, home_port) = {
			let v = need("LEPIS_HOME")?;
			match v.rsplit_once(':') {
				Some((h, p)) => (
					h.trim_start_matches('[').trim_end_matches(']').to_string(),
					p.parse()
						.map_err(|_| ConfigError(format!("LEPIS_HOME: not host:port: {v}")))?,
				),
				None => (v, 5432),
			}
		};
		let sslmode = match get("LEPIS_HOME_SSLMODE") {
			None => SslMode::VerifyFull,
			Some(v) => SslMode::parse(&v).ok_or_else(|| {
				ConfigError(format!(
					"LEPIS_HOME_SSLMODE: {v} (one of disable, require, verify-full)"
				))
			})?,
		};
		let tls_cert = get("LEPIS_TLS_CERT").map(PathBuf::from);
		let tls_key = get("LEPIS_TLS_KEY").map(PathBuf::from);
		if tls_cert.is_some() != tls_key.is_some() {
			return Err(ConfigError(
				"LEPIS_TLS_CERT and LEPIS_TLS_KEY go together".into(),
			));
		}
		let max_clients = match get("LEPIS_MAX_CLIENTS") {
			None => 1000,
			Some(v) => v
				.parse()
				.map_err(|_| ConfigError(format!("LEPIS_MAX_CLIENTS: {v}")))?,
		};
		let catalog_max_age = match get("LEPIS_CATALOG_MAX_AGE_MS") {
			None => crate::live::DEFAULT_CATALOG_MAX_AGE,
			Some(v) => {
				let ms: u64 = v
					.parse()
					.map_err(|_| ConfigError(format!("LEPIS_CATALOG_MAX_AGE_MS: {v}")))?;
				let age = Duration::from_millis(ms);
				// A router a cutover does not wait for must already have stopped routing.
				if age >= crate::live::ROUTER_FRESH {
					return Err(ConfigError(format!(
						"LEPIS_CATALOG_MAX_AGE_MS: {v} must be under {}, the age at which a cutover stops waiting for a router's heartbeat (0 turns the limit off)",
						crate::live::ROUTER_FRESH.as_millis()
					)));
				}
				age
			}
		};
		Ok(Config {
			host: get("LEPIS_HOST").unwrap_or_else(|| "0.0.0.0".into()),
			port: port("LEPIS_PORT", 5432)?,
			tls_cert,
			tls_key,
			home: NodeAddress {
				host: home_host,
				port: home_port,
				sslmode,
				ca_file: get("LEPIS_HOME_CA").map(PathBuf::from),
			},
			service: ServiceLogin {
				user: need("LEPIS_SERVICE_USER")?,
				password: need("LEPIS_SERVICE_PASSWORD")?,
				database: get("LEPIS_SERVICE_DATABASE").unwrap_or_else(|| "postgres".into()),
			},
			verifier_query: get("LEPIS_VERIFIER_QUERY")
				.unwrap_or_else(|| DEFAULT_VERIFIER_QUERY.into()),
			max_clients,
			catalog_max_age,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
		pairs
			.iter()
			.map(|(k, v)| (k.to_string(), v.to_string()))
			.collect()
	}

	#[test]
	fn defaults_are_strict() {
		let c = Config::from_map(&env(&[
			("LEPIS_HOME", "db.internal:6543"),
			("LEPIS_SERVICE_USER", "lepis"),
			("LEPIS_SERVICE_PASSWORD", "pw"),
		]))
		.unwrap();
		assert_eq!(c.home.host, "db.internal");
		assert_eq!(c.home.port, 6543);
		assert_eq!(c.home.sslmode, SslMode::VerifyFull);
		assert_eq!(c.port, 5432);
		assert_eq!(c.catalog_max_age, crate::live::DEFAULT_CATALOG_MAX_AGE);
		assert!(!format!("{:?}", c.service).contains("pw"));
	}

	#[test]
	fn the_catalog_age_stays_under_the_cutovers_wait() {
		let base = [
			("LEPIS_HOME", "h"),
			("LEPIS_SERVICE_USER", "u"),
			("LEPIS_SERVICE_PASSWORD", "p"),
		];
		let with = |v: &str| {
			let mut e = env(&base);
			e.insert("LEPIS_CATALOG_MAX_AGE_MS".into(), v.into());
			Config::from_map(&e)
		};
		assert_eq!(with("0").unwrap().catalog_max_age, Duration::ZERO);
		assert_eq!(
			with("14999").unwrap().catalog_max_age,
			Duration::from_millis(14_999)
		);
		assert!(with("15000").unwrap_err().0.contains("must be under 15000"));
		assert!(with("ten").is_err());
	}

	#[test]
	fn refuses_half_a_certificate() {
		let e = Config::from_map(&env(&[
			("LEPIS_HOME", "h"),
			("LEPIS_SERVICE_USER", "u"),
			("LEPIS_SERVICE_PASSWORD", "p"),
			("LEPIS_TLS_CERT", "/c.pem"),
		]))
		.unwrap_err();
		assert!(e.0.contains("go together"));
	}
}
