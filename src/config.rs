//! Configuration, from the environment like every component in the stack.
//!
//! The only node named here is the HOME node (L6): the catalog on it says where everything else
//! is. Phase 0 routes every connection to it.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// How Lepis reaches a node, in libpq's `sslmode` words for the four that mean something.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SslMode {
	/// Plain TCP. Only for a node on the same host or a private network the operator trusts.
	Disable,
	/// Encrypted, certificate not checked.
	Require,
	/// Encrypted, and the certificate is one of `LEPIS_HOME_CA`'s, or chains to one; its name is
	/// not checked (libpq's `verify-ca`). For a home whose own self-signed certificate is pinned,
	/// as a copy's router pins its home's, the way a standby pins its primary's (Metis Phase 10).
	VerifyCa,
	/// Encrypted, certificate chain and host name checked. The default.
	VerifyFull,
}

impl SslMode {
	pub(crate) fn parse(s: &str) -> Option<SslMode> {
		Some(match s {
			"disable" => SslMode::Disable,
			"require" => SslMode::Require,
			"verify-ca" => SslMode::VerifyCa,
			"verify-full" => SslMode::VerifyFull,
			_ => return None,
		})
	}

	pub(crate) fn as_str(self) -> &'static str {
		match self {
			SslMode::Disable => "disable",
			SslMode::Require => "require",
			SslMode::VerifyCa => "verify-ca",
			SslMode::VerifyFull => "verify-full",
		}
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

/**
 * The home nodes Lepis may log into: one (`LEPIS_HOME`), or the copies of one database
 * (`LEPIS_HOMES`), of which the router uses whichever is the primary now (`follow.rs`): out of
 * recovery and not `default_transaction_read_only`, which a guard's fence turns on. Shared, so the
 * follower's choice is what every session, verifier lookup and catalog read sees next.
 */
#[derive(Clone)]
pub struct Homes {
	list: Vec<NodeAddress>,
	current: Arc<RwLock<usize>>,
}

impl Homes {
	pub fn new(list: Vec<NodeAddress>) -> Homes {
		assert!(!list.is_empty(), "a home is required");
		Homes {
			list,
			current: Arc::new(RwLock::new(0)),
		}
	}

	/// The home in use now.
	pub fn current(&self) -> NodeAddress {
		let i = *self.current.read().unwrap_or_else(|e| e.into_inner());
		self.list[i].clone()
	}

	pub fn all(&self) -> &[NodeAddress] {
		&self.list
	}

	/// Uses `list[i]` from now on.
	pub fn set(&self, i: usize) {
		if i < self.list.len() {
			*self.current.write().unwrap_or_else(|e| e.into_inner()) = i;
		}
	}

	pub fn index(&self) -> usize {
		*self.current.read().unwrap_or_else(|e| e.into_inner())
	}
}

impl fmt::Debug for Homes {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Homes")
			.field("list", &self.list)
			.field("current", &self.index())
			.finish()
	}
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
	/// The home pass-through's port on `host` (`LEPIS_PASSTHROUGH_PORT`; None, the default, for
	/// none): a session there is relayed to the current home as it is (`passthrough.rs`).
	pub passthrough_port: Option<u16>,
	pub tls_cert: Option<PathBuf>,
	pub tls_key: Option<PathBuf>,
	pub homes: Homes,
	pub service: ServiceLogin,
	/// Returns one row with one column, the role's `rolpassword`, for `$1` = the role name.
	pub verifier_query: String,
	pub max_clients: usize,
	/// How long the router routes by its catalog after it last heard from the home node; zero
	/// for no limit (live.rs `Freshness`).
	pub catalog_max_age: Duration,
	/// Roles whose clients keep their sessions whatever `pool_mode` says (`LEPIS_SESSION_ROLES`,
	/// comma separated; the data API's `authenticator` by default): PostgREST pools for itself and
	/// LISTENs for its schema reloads.
	pub session_roles: Vec<String>,
	/// Whether the router reads the home's catalog and does what goes with it: routing by it,
	/// in-doubt recovery and the job runner (`LEPIS_CATALOG`, `on` by default). `off` is a copy's
	/// router (Metis Phase 10): every session relayed to the home as it is, nothing of `lepis` read,
	/// and no advisory lock taken from the home's own router.
	pub catalog: bool,
}

impl Config {
	/// The home node in use now: the only one, or the copy that is the primary (`Homes`).
	pub fn home(&self) -> NodeAddress {
		self.homes.current()
	}
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

		let addresses = match (get("LEPIS_HOME"), get("LEPIS_HOMES")) {
			(Some(_), Some(_)) => {
				return Err(ConfigError(
					"LEPIS_HOME and LEPIS_HOMES: one home, or the copies of one, not both".into(),
				));
			}
			(Some(one), None) => vec![(one, "LEPIS_HOME")],
			(None, Some(list)) => list
				.split(',')
				.map(|one| (one.trim().to_string(), "LEPIS_HOMES"))
				.filter(|(one, _)| !one.is_empty())
				.collect(),
			(None, None) => {
				return Err(ConfigError(
					"LEPIS_HOME (or LEPIS_HOMES) is required".into(),
				));
			}
		};
		if addresses.is_empty() {
			return Err(ConfigError("LEPIS_HOMES names no home".into()));
		}
		let mut homes_at = Vec::new();
		for (v, key) in addresses {
			homes_at.push(match v.rsplit_once(':') {
				Some((h, p)) => (
					h.trim_start_matches('[').trim_end_matches(']').to_string(),
					p.parse()
						.map_err(|_| ConfigError(format!("{key}: not host:port: {v}")))?,
				),
				None => (v, 5432),
			});
		}
		let sslmode = match get("LEPIS_HOME_SSLMODE") {
			None => SslMode::VerifyFull,
			Some(v) => SslMode::parse(&v).ok_or_else(|| {
				ConfigError(format!(
					"LEPIS_HOME_SSLMODE: {v} (one of disable, require, verify-ca, verify-full)"
				))
			})?,
		};
		if sslmode == SslMode::VerifyCa && get("LEPIS_HOME_CA").is_none() {
			return Err(ConfigError(
				"LEPIS_HOME_SSLMODE=verify-ca needs LEPIS_HOME_CA, the certificate to trust".into(),
			));
		}
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
		let session_roles = get("LEPIS_SESSION_ROLES")
			.map_or_else(|| "authenticator".to_string(), |v| v.to_string())
			.split(',')
			.map(str::trim)
			.filter(|role| !role.is_empty())
			.map(str::to_string)
			.collect();
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
		let catalog = match get("LEPIS_CATALOG").as_deref() {
			None | Some("on") => true,
			Some("off") => false,
			Some(v) => return Err(ConfigError(format!("LEPIS_CATALOG: {v} (on or off)"))),
		};
		// The admin API's jobs act on the catalog, which a router without one never reads.
		if !catalog && get("LEPIS_ADMIN_ADDR").is_some() {
			return Err(ConfigError(
				"LEPIS_CATALOG=off and LEPIS_ADMIN_ADDR: a router without the catalog runs no jobs"
					.into(),
			));
		}
		let listen_port = port("LEPIS_PORT", 5432)?;
		let passthrough_port = match get("LEPIS_PASSTHROUGH_PORT") {
			None => None,
			Some(_) => {
				let p = port("LEPIS_PASSTHROUGH_PORT", 0)?;
				if p == 0 || p == listen_port {
					return Err(ConfigError(format!(
						"LEPIS_PASSTHROUGH_PORT: {p} (a port of its own, not LEPIS_PORT's {listen_port})"
					)));
				}
				Some(p)
			}
		};
		Ok(Config {
			host: get("LEPIS_HOST").unwrap_or_else(|| "0.0.0.0".into()),
			port: listen_port,
			passthrough_port,
			tls_cert,
			tls_key,
			homes: Homes::new(
				homes_at
					.into_iter()
					.map(|(host, port)| NodeAddress {
						host,
						port,
						sslmode,
						ca_file: get("LEPIS_HOME_CA").map(PathBuf::from),
					})
					.collect(),
			),
			service: ServiceLogin {
				user: need("LEPIS_SERVICE_USER")?,
				password: need("LEPIS_SERVICE_PASSWORD")?,
				database: get("LEPIS_SERVICE_DATABASE").unwrap_or_else(|| "postgres".into()),
			},
			verifier_query: get("LEPIS_VERIFIER_QUERY")
				.unwrap_or_else(|| DEFAULT_VERIFIER_QUERY.into()),
			max_clients,
			catalog_max_age,
			session_roles,
			catalog,
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
		assert_eq!(c.home().host, "db.internal");
		assert_eq!(c.home().port, 6543);
		assert_eq!(c.home().sslmode, SslMode::VerifyFull);
		assert_eq!(c.port, 5432);
		assert_eq!(c.catalog_max_age, crate::live::DEFAULT_CATALOG_MAX_AGE);
		assert!(!format!("{:?}", c.service).contains("pw"));
	}

	#[test]
	fn verify_ca_names_the_certificate_it_trusts() {
		let base = [
			("LEPIS_HOME", "10.0.1.20:15432"),
			("LEPIS_SERVICE_USER", "snoutpod_router"),
			("LEPIS_SERVICE_PASSWORD", "pw"),
			("LEPIS_HOME_SSLMODE", "verify-ca"),
		];
		assert!(Config::from_map(&env(&base)).is_err());
		let mut with = base.to_vec();
		with.push(("LEPIS_HOME_CA", "/tls/home-ca.pem"));
		let c = Config::from_map(&env(&with)).unwrap();
		assert_eq!(c.home().sslmode, SslMode::VerifyCa);
		assert_eq!(
			c.home().ca_file.as_deref(),
			Some(std::path::Path::new("/tls/home-ca.pem"))
		);
		assert_eq!(
			SslMode::parse("verify-ca").map(SslMode::as_str),
			Some("verify-ca")
		);
	}

	#[test]
	fn a_copys_router_reads_no_catalog() {
		let base = [
			("LEPIS_HOME", "10.0.1.20:15432"),
			("LEPIS_SERVICE_USER", "snoutpod_router"),
			("LEPIS_SERVICE_PASSWORD", "pw"),
		];
		assert!(Config::from_map(&env(&base)).unwrap().catalog);
		let with = |v: &str| {
			let mut e = base.to_vec();
			e.push(("LEPIS_CATALOG", v));
			Config::from_map(&env(&e))
		};
		assert!(with("on").unwrap().catalog);
		assert!(!with("off").unwrap().catalog);
		assert!(with("no").is_err());
		let mut admin = base.to_vec();
		admin.extend([
			("LEPIS_CATALOG", "off"),
			("LEPIS_ADMIN_ADDR", "127.0.0.1:7444"),
		]);
		assert!(Config::from_map(&env(&admin)).is_err());
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
	fn the_home_pass_through_is_off_unless_given_a_port_of_its_own() {
		let base = [
			("LEPIS_HOME", "h"),
			("LEPIS_SERVICE_USER", "u"),
			("LEPIS_SERVICE_PASSWORD", "p"),
		];
		let with = |pairs: &[(&str, &str)]| {
			let mut e = env(&base);
			for (k, v) in pairs {
				e.insert(k.to_string(), v.to_string());
			}
			Config::from_map(&e)
		};
		assert_eq!(with(&[]).unwrap().passthrough_port, None);
		assert_eq!(
			with(&[("LEPIS_PASSTHROUGH_PORT", "")])
				.unwrap()
				.passthrough_port,
			None
		);
		assert_eq!(
			with(&[("LEPIS_PASSTHROUGH_PORT", "7434")])
				.unwrap()
				.passthrough_port,
			Some(7434)
		);
		assert!(
			with(&[("LEPIS_PASSTHROUGH_PORT", "seven")])
				.unwrap_err()
				.0
				.contains("not a port")
		);
		assert!(
			with(&[("LEPIS_PASSTHROUGH_PORT", "65536")])
				.unwrap_err()
				.0
				.contains("not a port")
		);
		assert!(with(&[("LEPIS_PASSTHROUGH_PORT", "0")]).is_err());
		// The routing listener's port, whether given or the default.
		assert!(
			with(&[("LEPIS_PASSTHROUGH_PORT", "5432")])
				.unwrap_err()
				.0
				.contains("its own")
		);
		assert!(with(&[("LEPIS_PORT", "7432"), ("LEPIS_PASSTHROUGH_PORT", "7432")]).is_err());
		assert_eq!(
			with(&[("LEPIS_PORT", "7432"), ("LEPIS_PASSTHROUGH_PORT", "5432")])
				.unwrap()
				.passthrough_port,
			Some(5432)
		);
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

	#[test]
	fn a_home_may_be_the_copies_of_one_database() {
		let env = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
			pairs
				.iter()
				.map(|(k, v)| (k.to_string(), v.to_string()))
				.collect()
		};
		let base = [("LEPIS_SERVICE_USER", "u"), ("LEPIS_SERVICE_PASSWORD", "p")];
		let mut both = env(&base);
		both.insert(
			"LEPIS_HOMES".into(),
			"10.40.1.115:25432, 10.40.1.79:25432".into(),
		);
		let c = Config::from_map(&both).expect("homes");
		let all: Vec<String> = c.homes.all().iter().map(|h| h.to_string()).collect();
		assert_eq!(all, ["10.40.1.115:25432", "10.40.1.79:25432"]);
		assert_eq!(c.home().to_string(), "10.40.1.115:25432");
		c.homes.set(1);
		assert_eq!(c.home().to_string(), "10.40.1.79:25432");
		// A clone shares the choice: every session sees where the follower moved it.
		let shared = c.clone();
		c.homes.set(0);
		assert_eq!(shared.home().to_string(), "10.40.1.115:25432");

		let mut one_and_many = both.clone();
		one_and_many.insert("LEPIS_HOME".into(), "h:1".into());
		assert!(Config::from_map(&one_and_many).is_err());
		let mut none = env(&base);
		none.insert("LEPIS_HOMES".into(), " , ".into());
		assert!(Config::from_map(&none).is_err());
		assert!(Config::from_map(&env(&base)).is_err());
	}
}
