//! `snout-lepis witness` (L21, Phase 14b): `witness.rs`'s state machine behind HTTP and a state
//! file per cluster. HTTP/1.1 and JSON, one request per connection, like the admin API:
//!
//! | Method and path | Who | What |
//! | --- | --- | --- |
//! | `GET /v1/health` | anyone | `{"ok": true}` |
//! | `POST /v1/clusters/<id>/call` | the cluster's guards (its token) | a `Call` in, an `Answer` out |
//! | `PUT /v1/clusters/<id>` | the operator | `{"primary", "standby", "lease_ms", "token"}`: a new cluster at epoch 1; 409 if it exists |
//! | `GET /v1/clusters/<id>` | the operator | its state and `lease_left_ms` |
//! | `POST /v1/clusters/<id>/standby` | the operator | `{"standby": name or null}`: a rebuilt or new standby; `eligible` goes false |
//! | `POST /v1/clusters/<id>/accept-loss` | the operator | the next grant may skip `eligible` (the standby may lack acknowledged commits) |
//!
//! A cluster's file is written and fsynced, then renamed into place and its directory fsynced,
//! before any answer that depends on it goes out; a write that fails leaves the state as it was and
//! answers 500. A cluster exists only once the operator has created it, so a witness that lost its
//! disk grants nothing until a person says what is true.
//!
//! Configuration: `LEPIS_WITNESS_ADDR` (e.g. `10.99.0.3:7445`), `LEPIS_WITNESS_DIR` (the state
//! files), `LEPIS_WITNESS_TOKEN` (the operator's), and either a certificate
//! (`LEPIS_WITNESS_TLS_CERT` and `_KEY`) or, off loopback, `LEPIS_WITNESS_CLEARTEXT=wireguard`: the operator's word that the
//! address is on a network that encrypts already. Otherwise a token would cross it in the clear,
//! and the witness refuses to start.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use super::witness::{Answer, Call, Cluster};
use crate::admin::{read_request, respond};
use crate::scram::constant_time_eq;

const READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Connections served at once. A request may carry up to the admin API's 1 MiB body, so this bounds
/// what a flood can make the witness hold to tens of MiB, inside the unit's memory cap; the rest
/// wait in the kernel's backlog. Two guards a cluster need a handful.
const MAX_CONNECTIONS: usize = 32;

pub struct WitnessConfig {
	pub addr: String,
	pub dir: PathBuf,
	pub operator_token: String,
	pub tls: Option<(PathBuf, PathBuf)>,
	pub cleartext_ok: bool,
}

impl WitnessConfig {
	pub fn from_env() -> Result<WitnessConfig, String> {
		let get = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
		let need = |k: &str| get(k).ok_or_else(|| format!("{k} is required"));
		let tls = match (get("LEPIS_WITNESS_TLS_CERT"), get("LEPIS_WITNESS_TLS_KEY")) {
			(Some(c), Some(k)) => Some((PathBuf::from(c), PathBuf::from(k))),
			(None, None) => None,
			_ => return Err("LEPIS_WITNESS_TLS_CERT and LEPIS_WITNESS_TLS_KEY go together".into()),
		};
		let cleartext_ok = match get("LEPIS_WITNESS_CLEARTEXT").as_deref() {
			None => false,
			Some("wireguard") => true,
			Some(v) => {
				return Err(format!(
					"LEPIS_WITNESS_CLEARTEXT: {v} (the one value is wireguard)"
				));
			}
		};
		Ok(WitnessConfig {
			addr: need("LEPIS_WITNESS_ADDR")?,
			dir: PathBuf::from(need("LEPIS_WITNESS_DIR")?),
			operator_token: need("LEPIS_WITNESS_TOKEN")?,
			tls,
			cleartext_ok,
		})
	}
}

struct Entry {
	cluster: Cluster,
	token_sha256: String,
}

pub struct Witness {
	dir: PathBuf,
	operator_token: String,
	started: Instant,
	clusters: Mutex<HashMap<String, Entry>>,
}

impl Witness {
	/// Reads every cluster's file; each lease is held one more term from now.
	pub fn open(dir: &Path, operator_token: &str) -> Result<Witness, String> {
		std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
		let mut clusters = HashMap::new();
		let listing = std::fs::read_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
		for f in listing.flatten() {
			let path = f.path();
			let Some(id) = path
				.file_name()
				.and_then(|n| n.to_str())
				.and_then(|n| n.strip_suffix(".json"))
			else {
				continue;
			};
			let text =
				std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
			let v: Value =
				serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
			let cluster = v.get("cluster").and_then(|c| Cluster::from_json(c, 0));
			let token = v.get("token_sha256").and_then(Value::as_str);
			let (Some(cluster), Some(token)) = (cluster, token) else {
				return Err(format!("{}: not a witness state file", path.display()));
			};
			clusters.insert(
				id.to_string(),
				Entry {
					cluster,
					token_sha256: token.to_string(),
				},
			);
		}
		tracing::info!(dir = %dir.display(), clusters = clusters.len(), "witness state read");
		Ok(Witness {
			dir: dir.to_path_buf(),
			operator_token: operator_token.to_string(),
			started: Instant::now(),
			clusters: Mutex::new(clusters),
		})
	}

	fn now(&self) -> u64 {
		self.started.elapsed().as_millis() as u64
	}

	fn persist(&self, id: &str, e: &Entry) -> Result<(), String> {
		let body = json!({"cluster": e.cluster.to_json(), "token_sha256": e.token_sha256});
		let path = self.dir.join(format!("{id}.json"));
		let tmp = self.dir.join(format!(".{id}.json.tmp"));
		let write = || -> std::io::Result<()> {
			use std::io::Write;
			let mut f = std::fs::File::create(&tmp)?;
			f.write_all(body.to_string().as_bytes())?;
			f.sync_all()?;
			std::fs::rename(&tmp, &path)?;
			std::fs::File::open(&self.dir)?.sync_all()
		};
		write().map_err(|e| format!("{}: {e}", path.display()))
	}

	/// One request, already read. Returns the status and the body.
	pub fn handle(
		&self,
		method: &str,
		path: &str,
		authorization: Option<&str>,
		body: &[u8],
	) -> (u16, Value) {
		let path = path
			.split('?')
			.next()
			.unwrap_or_default()
			.trim_end_matches('/');
		if method == "GET" && path == "/v1/health" {
			return (200, json!({"ok": true}));
		}
		let Some(rest) = path.strip_prefix("/v1/clusters/") else {
			return error(404, "no such path");
		};
		let (id, verb) = rest.split_once('/').unwrap_or((rest, ""));
		if id.is_empty()
			|| id.len() > 64
			|| !id
				.bytes()
				.all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
		{
			return error(400, "a cluster id is 1-64 letters, digits, - or _");
		}
		let presented = authorization
			.and_then(|a| a.strip_prefix("Bearer "))
			.unwrap_or_default();
		let body: Value = if body.is_empty() {
			Value::Null
		} else {
			match serde_json::from_slice(body) {
				Ok(v) => v,
				Err(e) => return error(400, &format!("not JSON: {e}")),
			}
		};
		let now = self.now();
		let mut clusters = self.clusters.lock().unwrap_or_else(|p| p.into_inner());
		if (method, verb) == ("POST", "call") {
			let Some(e) = clusters.get_mut(id) else {
				return error(404, "no such cluster on this witness");
			};
			if !constant_time_eq(sha256_hex(presented).as_bytes(), e.token_sha256.as_bytes()) {
				return error(401, "send Authorization: Bearer <the cluster's token>");
			}
			let Some(call) = Call::from_json(&body) else {
				return error(
					400,
					"not a call: {\"call\": \"renew\"|\"eligible\"|\"ask\", \"copy\", \"epoch\", ...}",
				);
			};
			let before = e.cluster.clone();
			let (answer, changed) = e.cluster.handle(&call, now);
			if changed {
				let saved = self.persist(id, e);
				if let Err(m) = saved {
					e.cluster = before;
					tracing::error!(cluster = id, "{m}");
					return error(
						500,
						"the witness could not write its state; nothing changed",
					);
				}
				log_change(id, &call, &answer, &e.cluster);
			}
			return (200, answer.to_json());
		}
		if !constant_time_eq(presented.as_bytes(), self.operator_token.as_bytes()) {
			return error(401, "send Authorization: Bearer <LEPIS_WITNESS_TOKEN>");
		}
		match (method, verb) {
			("PUT", "") => {
				if clusters.contains_key(id) {
					return error(
						409,
						"the cluster exists; a witness never forgets an epoch it granted",
					);
				}
				let primary = body.get("primary").and_then(Value::as_str);
				let standby = body.get("standby").and_then(Value::as_str);
				let lease_ms = body
					.get("lease_ms")
					.and_then(Value::as_u64)
					.unwrap_or(10_000);
				let token = body
					.get("token")
					.and_then(Value::as_str)
					.filter(|t| t.len() >= 32);
				let (Some(primary), Some(token)) = (primary, token) else {
					return error(
						400,
						"send {\"primary\", \"standby\", \"lease_ms\", \"token\" (32 characters or more)}",
					);
				};
				if !(2_000..=600_000).contains(&lease_ms) || standby == Some(primary) {
					return error(
						400,
						"lease_ms is 2000-600000, and the standby is another copy",
					);
				}
				let e = Entry {
					cluster: Cluster::new(primary, standby, lease_ms, now),
					token_sha256: sha256_hex(token),
				};
				if let Err(m) = self.persist(id, &e) {
					return error(500, &m);
				}
				tracing::info!(
					cluster = id,
					primary,
					?standby,
					lease_ms,
					"cluster created at epoch 1"
				);
				let state = state_json(&e.cluster, now);
				clusters.insert(id.to_string(), e);
				(201, state)
			}
			("GET", "") => match clusters.get(id) {
				Some(e) => (200, state_json(&e.cluster, now)),
				None => error(404, "no such cluster on this witness"),
			},
			("POST", "standby" | "accept-loss") => {
				let Some(e) = clusters.get_mut(id) else {
					return error(404, "no such cluster on this witness");
				};
				let before = e.cluster.clone();
				if verb == "standby" {
					let standby = body.get("standby").and_then(Value::as_str);
					if standby == Some(e.cluster.primary.as_str()) {
						return error(400, "the standby is another copy than the primary");
					}
					e.cluster.standby = standby.map(str::to_string);
					e.cluster.eligible = false;
				} else {
					e.cluster.accept_loss = true;
				}
				if let Err(m) = self.persist(id, e) {
					e.cluster = before;
					return error(500, &m);
				}
				tracing::warn!(cluster = id, verb, "set by the operator");
				(200, state_json(&e.cluster, now))
			}
			_ => error(404, "no such path"),
		}
	}
}

fn log_change(id: &str, call: &Call, answer: &Answer, c: &Cluster) {
	match answer {
		Answer::Granted { epoch } => {
			tracing::warn!(cluster = id, primary = %c.primary, epoch, "granted: a new primary");
		}
		_ => {
			tracing::info!(cluster = id, call = %call.to_json(), eligible = c.eligible, "state changed")
		}
	}
}

fn state_json(c: &Cluster, now: u64) -> Value {
	let mut v = c.to_json();
	v["lease_left_ms"] = json!(c.lease_until.saturating_sub(now));
	v
}

fn error(status: u16, message: &str) -> (u16, Value) {
	(status, json!({"error": {"message": message}}))
}

fn sha256_hex(s: &str) -> String {
	Sha256::digest(s.as_bytes())
		.iter()
		.map(|b| format!("{b:02x}"))
		.collect()
}

/// Serves until the listener fails.
pub async fn serve_witness(config: WitnessConfig) -> Result<(), String> {
	let witness = Arc::new(Witness::open(&config.dir, &config.operator_token)?);
	let listener = TcpListener::bind(&config.addr)
		.await
		.map_err(|e| format!("{}: {e}", config.addr))?;
	let local = listener.local_addr().map_err(|e| e.to_string())?;
	let tls = match &config.tls {
		Some((cert, key)) => {
			let (server, _) = crate::tls::server_config(cert, key)?;
			let mut c = (*server).clone();
			c.alpn_protocols = vec![b"http/1.1".to_vec()];
			Some(TlsAcceptor::from(Arc::new(c)))
		}
		None => None,
	};
	if tls.is_none() && !local.ip().is_loopback() && !config.cleartext_ok {
		return Err(format!(
			"the witness on {local} would take tokens in the clear: give it a certificate (LEPIS_WITNESS_TLS_CERT), bind it to loopback, or say the network encrypts (LEPIS_WITNESS_CLEARTEXT=wireguard)"
		));
	}
	tracing::info!(%local, tls = tls.is_some(), "snout-lepis witness listening");
	let slots = Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS));
	loop {
		let slot = slots
			.clone()
			.acquire_owned()
			.await
			.map_err(|e| e.to_string())?;
		let (tcp, _) = listener.accept().await.map_err(|e| e.to_string())?;
		let (witness, tls) = (witness.clone(), tls.clone());
		tokio::spawn(async move {
			let _slot = slot;
			let _ = tcp.set_nodelay(true);
			match tls {
				Some(acceptor) => {
					if let Ok(Ok(s)) =
						tokio::time::timeout(READ_TIMEOUT, acceptor.accept(tcp)).await
					{
						connection(s, &witness).await;
					}
				}
				None => connection(tcp, &witness).await,
			}
		});
	}
}

async fn connection<S: AsyncRead + AsyncWrite + Unpin>(mut s: S, witness: &Witness) {
	let (status, body) = match tokio::time::timeout(READ_TIMEOUT, read_request(&mut s)).await {
		Err(_) => error(408, "the request took too long to arrive"),
		Ok(Err((code, m))) => error(code, &m),
		Ok(Ok(req)) => {
			// The state file is written under the lock: off the async threads.
			tokio::task::block_in_place(|| {
				witness.handle(
					&req.method,
					&req.path,
					req.authorization.as_deref(),
					&req.body,
				)
			})
		}
	};
	respond(&mut s, status, &body).await;
}

#[cfg(test)]
mod tests {
	use super::*;

	const OP: &str = "operator-token";
	const TOKEN: &str = "a-cluster-token-that-is-long-enough";

	fn dir(name: &str) -> PathBuf {
		let d = std::env::temp_dir().join(format!("lepis-witness-{name}-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&d);
		d
	}

	fn create(w: &Witness) -> (u16, Value) {
		let body =
			json!({"primary": "a", "standby": "b", "lease_ms": 2_000, "token": TOKEN}).to_string();
		w.handle(
			"PUT",
			"/v1/clusters/cp",
			Some(&format!("Bearer {OP}")),
			body.as_bytes(),
		)
	}

	fn call(w: &Witness, c: Call) -> (u16, Value) {
		w.handle(
			"POST",
			"/v1/clusters/cp/call",
			Some(&format!("Bearer {TOKEN}")),
			c.to_json().to_string().as_bytes(),
		)
	}

	#[test]
	fn a_cluster_is_created_once_and_its_changes_survive_a_restart() {
		let d = dir("restart");
		let w = Witness::open(&d, OP).unwrap();
		assert_eq!(create(&w).0, 201);
		assert_eq!(create(&w).0, 409);
		let (status, answer) = call(
			&w,
			Call::Eligible {
				copy: "a".into(),
				epoch: 1,
				eligible: true,
			},
		);
		assert_eq!((status, answer["eligible"].as_bool()), (200, Some(true)));
		drop(w);
		let w = Witness::open(&d, OP).unwrap();
		let (_, state) = w.handle("GET", "/v1/clusters/cp", Some(&format!("Bearer {OP}")), b"");
		assert_eq!(
			(state["eligible"].as_bool(), state["epoch"].as_u64()),
			(Some(true), Some(1))
		);
		assert!(
			state["lease_left_ms"].as_u64().unwrap() > 1_000,
			"a restart holds the lease a term"
		);
		std::fs::remove_dir_all(&d).unwrap();
	}

	#[test]
	fn tokens_are_checked_and_unknown_clusters_grant_nothing() {
		let d = dir("tokens");
		let w = Witness::open(&d, OP).unwrap();
		let ask = Call::Ask {
			copy: "b".into(),
			epoch: 1,
			stream_lost_ms: u64::MAX,
		};
		assert_eq!(call(&w, ask.clone()).0, 404);
		assert_eq!(create(&w).0, 201);
		let wrong = w.handle(
			"POST",
			"/v1/clusters/cp/call",
			Some("Bearer nope"),
			ask.to_json().to_string().as_bytes(),
		);
		assert_eq!(wrong.0, 401);
		assert_eq!(
			w.handle(
				"GET",
				"/v1/clusters/cp",
				Some(&format!("Bearer {TOKEN}")),
				b""
			)
			.0,
			401
		);
		assert_eq!(
			w.handle(
				"GET",
				"/v1/clusters/../x",
				Some(&format!("Bearer {OP}")),
				b""
			)
			.0,
			400
		);
		assert_eq!(call(&w, ask).1["answer"], "held");
		std::fs::remove_dir_all(&d).unwrap();
	}

	#[test]
	fn the_operator_sets_the_standby_and_may_accept_a_loss() {
		let d = dir("operator");
		let w = Witness::open(&d, OP).unwrap();
		create(&w);
		call(
			&w,
			Call::Eligible {
				copy: "a".into(),
				epoch: 1,
				eligible: true,
			},
		);
		let auth = format!("Bearer {OP}");
		let (_, s) = w.handle(
			"POST",
			"/v1/clusters/cp/standby",
			Some(&auth),
			br#"{"standby": "c"}"#,
		);
		assert_eq!(
			(s["standby"].as_str(), s["eligible"].as_bool()),
			(Some("c"), Some(false))
		);
		let (_, s) = w.handle("POST", "/v1/clusters/cp/accept-loss", Some(&auth), b"");
		assert_eq!(s["accept_loss"].as_bool(), Some(true));
		std::fs::remove_dir_all(&d).unwrap();
	}
}
