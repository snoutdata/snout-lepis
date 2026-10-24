//! `snout-lepis guard` (L21, Phase 14c): beside one copy of a Postgres, every tick it reads what
//! that Postgres shows, asks `decide` what to do, and does it, to the Postgres and to the witness.
//! Everything that decides is in `decide.rs`; this file is the reading and the doing.
//!
//! It owns its copy's `synchronous_standby_names`, `default_transaction_read_only` and the
//! replication timeouts (`ALTER SYSTEM`, then `pg_reload_conf()`), so nothing else may set them
//! (the host agent leaves a guarded pod's to it, Phase 14e). It logs in as a superuser of its own
//! copy: it promotes, ends sessions and changes the server's settings.
//!
//! Three facts the reading rests on, each a Postgres behaviour rather than a choice:
//! - The standby's liveness on the primary is when its `reply_time` last CHANGED, timed on this
//!   guard's clock, never the value itself (the standby's clock). An idle walsender sends a
//!   keepalive once half of `wal_sender_timeout` passes without a reply, so with
//!   `wal_sender_timeout` at half the lease and the standby's periodic reply slower than that,
//!   both sides hear each other every quarter lease on an idle cluster.
//! - `synchronous_standby_names` takes effect for commits when the CHECKPOINTER applies the
//!   reload, not when this session sees it. So the LSN the standby must flush past before it is
//!   vouched for is read after a barrier (`sync_barrier`): a new session that shows the value
//!   (the postmaster has applied it and signalled every child), then a `CHECKPOINT`, which the
//!   checkpointer starts only after handling that signal. It runs on its own session, off the
//!   tick, since a checkpoint can take a while and the lease must keep being renewed.
//! - A backend waiting in synchronous replication that is terminated never tells its client the
//!   commit succeeded, so ending sessions after the fenced name is in place loses no promise.
//!
//! Configuration (`GuardConfig::from_map`): the witness (`LEPIS_GUARD_WITNESS` host:port, its
//! `_SSLMODE` and `_CA`), the cluster and its token (`LEPIS_GUARD_CLUSTER`, `LEPIS_GUARD_TOKEN`),
//! this copy's name and its peer's (`LEPIS_GUARD_COPY`, `LEPIS_GUARD_PEER`: each copy's
//! `application_name` when it streams as the standby), the local Postgres (`LEPIS_GUARD_PG`,
//! `_PG_SSLMODE`, `_PG_CA`, `_PG_USER`, `_PG_PASSWORD`, `_PG_DATABASE`), the epoch's state file
//! (`LEPIS_GUARD_STATE`), an optional status file (`LEPIS_GUARD_STATUS`), the lease and the tick
//! (`LEPIS_GUARD_LEASE_MS`, the witness's; `LEPIS_GUARD_TICK_MS`), and three hooks run with
//! `sh -c` (`LEPIS_GUARD_ON_PROMOTE`, `_ON_FENCE`, `_ON_DEMOTED`; `LEPIS_GUARD_EVENT`,
//! `LEPIS_GUARD_EPOCH` and `LEPIS_GUARD_COPY` in their environment).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_rustls::TlsConnector;

use super::decide::{Action, FENCED_NAME, Memory, Postgres, StandbySeen, SyncNames, decide};
use super::witness::{Answer, Call};
use crate::backend::{self, Backend};
use crate::config::{NodeAddress, SslMode};
use crate::scram::ClientCredential;

const MAX_ANSWER: u64 = 1024 * 1024;

type Rows = Vec<Vec<Option<String>>>;

#[derive(Clone, Debug)]
pub struct GuardConfig {
	pub witness: NodeAddress,
	pub cluster: String,
	pub token: String,
	pub copy: String,
	pub peer: String,
	pub pg: NodeAddress,
	pub pg_user: String,
	pub pg_password: String,
	pub pg_database: String,
	pub state_file: PathBuf,
	pub status_file: Option<PathBuf>,
	pub lease_ms: u64,
	pub tick_ms: u64,
	pub on_promote: Option<String>,
	pub on_fence: Option<String>,
	pub on_demoted: Option<String>,
}

fn address(
	get: &dyn Fn(&str) -> Option<String>,
	key: &str,
	default: Option<&str>,
	default_port: u16,
) -> Result<NodeAddress, String> {
	let v = get(key)
		.or(default.map(str::to_string))
		.ok_or_else(|| format!("{key} is required"))?;
	let (host, port) = match v.rsplit_once(':') {
		Some((h, p)) => (
			h.trim_start_matches('[').trim_end_matches(']').to_string(),
			p.parse()
				.map_err(|_| format!("{key}: not host:port: {v}"))?,
		),
		None => (v, default_port),
	};
	let loopback = host == "localhost"
		|| host
			.parse::<std::net::IpAddr>()
			.is_ok_and(|ip| ip.is_loopback());
	let mode_key = format!("{key}_SSLMODE");
	let sslmode = match get(&mode_key) {
		// Its own copy on this machine needs no TLS; anything across a network checks the name.
		None if loopback => SslMode::Disable,
		None => SslMode::VerifyFull,
		Some(m) => SslMode::parse(&m)
			.ok_or_else(|| format!("{mode_key}: {m} (one of disable, require, verify-full)"))?,
	};
	Ok(NodeAddress {
		host,
		port,
		sslmode,
		ca_file: get(&format!("{key}_CA")).map(PathBuf::from),
	})
}

impl GuardConfig {
	pub fn from_env() -> Result<GuardConfig, String> {
		GuardConfig::from_map(&std::env::vars().collect())
	}

	pub fn from_map(env: &HashMap<String, String>) -> Result<GuardConfig, String> {
		let get = |k: &str| env.get(k).filter(|v| !v.is_empty()).cloned();
		let need = |k: &str| get(k).ok_or_else(|| format!("{k} is required"));
		let ms = |k: &str, default: u64| -> Result<u64, String> {
			get(k).map_or(Ok(default), |v| v.parse().map_err(|_| format!("{k}: {v}")))
		};
		let lease_ms = ms("LEPIS_GUARD_LEASE_MS", 10_000)?;
		let tick_ms = ms("LEPIS_GUARD_TICK_MS", 1_000)?;
		if !(2_000..=600_000).contains(&lease_ms) {
			return Err(format!(
				"LEPIS_GUARD_LEASE_MS: {lease_ms} (2000 to 600000, the witness's lease)"
			));
		}
		// A renewal is good for half the lease, and two ticks must fit in it.
		if tick_ms == 0 || tick_ms > lease_ms / 5 {
			return Err(format!(
				"LEPIS_GUARD_TICK_MS: {tick_ms} (1 to {}, a fifth of the lease)",
				lease_ms / 5
			));
		}
		let (copy, peer) = (need("LEPIS_GUARD_COPY")?, need("LEPIS_GUARD_PEER")?);
		if copy == peer || copy == FENCED_NAME || peer == FENCED_NAME {
			return Err(format!(
				"LEPIS_GUARD_COPY and LEPIS_GUARD_PEER must differ, and neither may be {FENCED_NAME}"
			));
		}
		Ok(GuardConfig {
			witness: address(&get, "LEPIS_GUARD_WITNESS", None, 7445)?,
			cluster: need("LEPIS_GUARD_CLUSTER")?,
			token: need("LEPIS_GUARD_TOKEN")?,
			copy,
			peer,
			pg: address(&get, "LEPIS_GUARD_PG", Some("127.0.0.1:5432"), 5432)?,
			pg_user: get("LEPIS_GUARD_PG_USER").unwrap_or_else(|| "postgres".into()),
			pg_password: need("LEPIS_GUARD_PG_PASSWORD")?,
			pg_database: get("LEPIS_GUARD_PG_DATABASE").unwrap_or_else(|| "postgres".into()),
			state_file: PathBuf::from(need("LEPIS_GUARD_STATE")?),
			status_file: get("LEPIS_GUARD_STATUS").map(PathBuf::from),
			lease_ms,
			tick_ms,
			on_promote: get("LEPIS_GUARD_ON_PROMOTE"),
			on_fence: get("LEPIS_GUARD_ON_FENCE"),
			on_demoted: get("LEPIS_GUARD_ON_DEMOTED"),
		})
	}
}

/// A standby name as `synchronous_standby_names` takes it: always double-quoted, so a name that
/// is a keyword (`FIRST`, `ANY`) or has a dash is still one name.
pub fn quoted(name: &str) -> String {
	format!("\"{}\"", name.replace('"', "\"\""))
}

/// The value read back, in `decide`'s terms. `None`: not a value this guard writes (someone else
/// set it), which `facts` reads as the standby alone and gone, so `decide` clears `eligible` at the
/// witness before commits can go on without it.
pub fn names_of(value: &str, peer: &str) -> Option<SyncNames> {
	let v = value.trim();
	if v.is_empty() {
		Some(SyncNames::Off)
	} else if v == FENCED_NAME || v == quoted(FENCED_NAME) {
		Some(SyncNames::Fenced)
	} else if v == quoted(peer) || v == peer {
		Some(SyncNames::Standby)
	} else {
		None
	}
}

/// The replication timeouts the reading above relies on, as `ALTER SYSTEM` statements: written on
/// both copies, since either may be the primary tomorrow.
pub fn timeout_settings(lease_ms: u64) -> [(&'static str, String); 3] {
	[
		("wal_sender_timeout", format!("{}ms", lease_ms / 2)),
		("wal_receiver_timeout", format!("{}ms", lease_ms / 2)),
		// Slower than half of wal_sender_timeout, so an idle primary's keepalives (and the
		// standby's answers to them) are what keep both sides hearing each other.
		(
			"wal_receiver_status_interval",
			format!("{}s", lease_ms.div_ceil(1000)),
		),
	]
}

fn sql_literal(s: &str) -> String {
	format!("'{}'", s.replace('\'', "''"))
}

/// The state file: the epoch, the only memory a guard keeps across a restart.
pub fn read_epoch(path: &Path) -> Result<u64, String> {
	match std::fs::read_to_string(path) {
		Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(1),
		Err(e) => Err(format!("{}: {e}", path.display())),
		Ok(text) => serde_json::from_str::<Value>(&text)
			.ok()
			.and_then(|v| v.get("epoch").and_then(Value::as_u64))
			.ok_or_else(|| format!("{}: not a guard state file", path.display())),
	}
}

/// Written, fsynced, renamed into place and the directory fsynced: a promotion happens only after
/// the epoch it was granted is on disk.
pub fn write_epoch(path: &Path, epoch: u64) -> Result<(), String> {
	let tmp = path.with_extension("tmp");
	let fail = |e: std::io::Error| format!("{}: {e}", path.display());
	{
		let mut f = std::fs::File::create(&tmp).map_err(fail)?;
		std::io::Write::write_all(&mut f, json!({ "epoch": epoch }).to_string().as_bytes())
			.map_err(fail)?;
		f.sync_all().map_err(fail)?;
	}
	std::fs::rename(&tmp, path).map_err(fail)?;
	if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
		std::fs::File::open(dir)
			.and_then(|d| d.sync_all())
			.map_err(fail)?;
	}
	Ok(())
}

fn pg_error(e: backend::BackendError) -> String {
	e.to_string()
}

async fn pg_connect(cfg: &GuardConfig) -> Result<Backend, String> {
	let tls = crate::tls::client_config(&cfg.pg)?;
	let params = vec![
		("user".to_string(), cfg.pg_user.clone()),
		("database".to_string(), cfg.pg_database.clone()),
		(
			"application_name".to_string(),
			"snout-lepis guard".to_string(),
		),
	];
	let mut b = backend::connect(
		&cfg.pg,
		tls.as_ref(),
		&params,
		ClientCredential::Password(cfg.pg_password.clone()),
	)
	.await
	.map_err(|e| format!("{}: {e}", cfg.pg))?;
	// A fenced copy defaults to read-only; the guard's own session is never what is fenced.
	b.simple("SET default_transaction_read_only = off")
		.await
		.map_err(pg_error)?;
	Ok(b)
}

fn lsn(v: Option<&Option<String>>) -> u64 {
	v.and_then(|c| c.as_deref())
		.and_then(|s| s.split('.').next())
		.and_then(|s| s.parse().ok())
		.unwrap_or(0)
}

/// The barrier described at the top: the LSN read after the new `synchronous_standby_names` holds
/// for every commit.
async fn sync_barrier(cfg: GuardConfig, expect: String) -> Result<u64, String> {
	let mut b = pg_connect(&cfg).await?;
	let shown = b
		.query("SELECT current_setting('synchronous_standby_names')", &[])
		.await
		.map_err(pg_error)?;
	let now = shown.first().and_then(|r| r[0].clone()).unwrap_or_default();
	if now.trim() != expect {
		b.close().await;
		return Err(format!(
			"synchronous_standby_names is {now:?} in a new session, not {expect:?} yet"
		));
	}
	b.simple("CHECKPOINT").await.map_err(pg_error)?;
	let rows = b
		.query(
			"SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::text",
			&[],
		)
		.await
		.map_err(pg_error)?;
	b.close().await;
	Ok(lsn(rows.first().map(|r| &r[0])))
}

/// One HTTP/1.1 request to the witness, read to the close.
async fn http(
	at: &NodeAddress,
	tls: Option<&Arc<rustls::ClientConfig>>,
	request: String,
) -> Result<(u16, Value), String> {
	let tcp = TcpStream::connect((at.host.as_str(), at.port))
		.await
		.map_err(|e| format!("{at}: {e}"))?;
	tcp.set_nodelay(true).ok();
	let mut raw = Vec::new();
	match tls {
		Some(config) => {
			let name = crate::tls::server_name(&at.host)?;
			let mut s = TlsConnector::from(config.clone())
				.connect(name, tcp)
				.await
				.map_err(|e| format!("{at}: TLS: {e}"))?;
			s.write_all(request.as_bytes())
				.await
				.map_err(|e| format!("{at}: {e}"))?;
			(&mut s)
				.take(MAX_ANSWER)
				.read_to_end(&mut raw)
				.await
				.map_err(|e| format!("{at}: {e}"))?;
		}
		None => {
			let mut s = tcp;
			s.write_all(request.as_bytes())
				.await
				.map_err(|e| format!("{at}: {e}"))?;
			(&mut s)
				.take(MAX_ANSWER)
				.read_to_end(&mut raw)
				.await
				.map_err(|e| format!("{at}: {e}"))?;
		}
	}
	let split = raw
		.windows(4)
		.position(|w| w == b"\r\n\r\n")
		.ok_or_else(|| format!("{at}: not an HTTP answer"))?;
	let head = String::from_utf8_lossy(&raw[..split]);
	let status = head
		.split_whitespace()
		.nth(1)
		.and_then(|s| s.parse().ok())
		.ok_or_else(|| format!("{at}: not an HTTP answer"))?;
	let body = serde_json::from_slice(&raw[split + 4..]).unwrap_or(Value::Null);
	Ok((status, body))
}

/// What the status file says, for the host agent and the CLI.
fn status_json(g: &Guard, pg: &Postgres, now: u64) -> Value {
	let (role, names, standby) = match pg {
		Postgres::Down => ("down", None, None),
		Postgres::Standby { .. } => ("standby", None, None),
		Postgres::Primary { names, standby, .. } => (
			"primary",
			Some(match names {
				SyncNames::Standby => "standby",
				SyncNames::Off => "off",
				SyncNames::Fenced => "fenced",
			}),
			Some(standby.is_some_and(|s| s.reply_age_ms < g.lease_ms / 2)),
		),
	};
	json!({
		"copy": g.cfg.copy,
		"cluster": g.cfg.cluster,
		"role": role,
		"synchronous": names,
		"standby_up": standby,
		"epoch": g.mem.epoch,
		"eligible": g.mem.eligible,
		"demoted": g.mem.demoted,
		"lease_left_ms": g.mem.renewed_at.map(|t| (t + g.lease_ms / 2).saturating_sub(now)),
		"last_error": g.last_error,
	})
}

struct Guard {
	cfg: GuardConfig,
	start: Instant,
	lease_ms: u64,
	mem: Memory,
	pg: Option<Backend>,
	witness_tls: Option<Arc<rustls::ClientConfig>>,
	/// The standby's `reply_time` as last read, and when this guard first saw that value.
	reply: Option<(String, u64)>,
	/// Since when the standby has heard nothing (its stream gone), on this guard's clock.
	stream_gone_since: Option<u64>,
	/// Bumped by every change to `synchronous_standby_names`, so a barrier that finishes after a
	/// later change is thrown away.
	names_generation: u64,
	barrier: Option<(u64, JoinHandle<Result<u64, String>>)>,
	/// When the Ask that was granted went out (the new lease is counted from it).
	asked_at: u64,
	timeouts_set: bool,
	last_error: Option<String>,
}

impl Guard {
	fn now(&self) -> u64 {
		self.start.elapsed().as_millis() as u64
	}

	fn trouble(&mut self, m: String) {
		if self.last_error.as_ref() != Some(&m) {
			tracing::warn!("{m}");
		}
		self.last_error = Some(m);
	}

	async fn session(&mut self) -> Result<&mut Backend, String> {
		if self.pg.is_none() {
			self.pg = Some(pg_connect(&self.cfg).await?);
		}
		Ok(self.pg.as_mut().expect("just connected"))
	}

	async fn run_sql(&mut self, sql: &str) -> Result<(), String> {
		let r = self.session().await?.simple(sql).await;
		if r.is_err() {
			self.pg = None;
		}
		r.map(|_| ()).map_err(pg_error)
	}

	async fn rows(&mut self, sql: &str, params: &[&str]) -> Result<Rows, String> {
		let r = self.session().await?.query(sql, params).await;
		if r.is_err() {
			self.pg = None;
		}
		r.map_err(pg_error)
	}

	/// What the local Postgres shows, in `decide`'s terms.
	async fn facts(&mut self, now: u64) -> Postgres {
		match self.read(now).await {
			Ok(pg) => pg,
			Err(m) => {
				self.pg = None;
				self.trouble(format!("reading {}: {m}", self.cfg.pg));
				Postgres::Down
			}
		}
	}

	async fn read(&mut self, now: u64) -> Result<Postgres, String> {
		let head = self
			.rows(
				"SELECT pg_is_in_recovery()::text, current_setting('synchronous_standby_names'), \
				 CASE WHEN pg_is_in_recovery() THEN NULL \
				 ELSE pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::text END",
				&[],
			)
			.await?;
		let row = head.first().ok_or("no row")?;
		if row[0].as_deref() == Some("true") {
			self.reply = None;
			let rx = self
				.rows(
					"SELECT (extract(epoch FROM clock_timestamp() - last_msg_receipt_time) * 1000)::bigint::text \
					 FROM pg_stat_wal_receiver WHERE status = 'streaming'",
					&[],
				)
				.await?;
			let age = rx
				.first()
				.and_then(|r| r[0].as_deref())
				.and_then(|s| s.parse::<i64>().ok())
				.map(|ms| ms.max(0) as u64);
			self.stream_gone_since = match age {
				Some(_) => None,
				None => Some(self.stream_gone_since.unwrap_or(now)),
			};
			return Ok(Postgres::Standby {
				receive_age_ms: age,
			});
		}
		let value = row[1].clone().unwrap_or_default();
		let names = names_of(&value, &self.cfg.peer);
		if names.is_none() {
			self.trouble(format!(
				"synchronous_standby_names is {value:?}, which this guard did not set; read as the standby alone and gone"
			));
		}
		let current = lsn(row.get(2));
		let peer = self.cfg.peer.clone();
		let rep = self
			.rows(
				"SELECT sync_state, pg_wal_lsn_diff(flush_lsn, '0/0')::text, reply_time::text \
				 FROM pg_stat_replication WHERE application_name = $1 AND state = 'streaming' \
				 ORDER BY reply_time DESC NULLS LAST LIMIT 1",
				&[&peer],
			)
			.await?;
		let standby = match rep.first() {
			Some(r) if names.is_some() => {
				let reply = r[2].clone().unwrap_or_default();
				let since = match &self.reply {
					Some((last, since)) if *last == reply => *since,
					// Not seen moving yet: counted as half a lease old until it does.
					None => now.saturating_sub(self.lease_ms / 2),
					Some(_) => now,
				};
				self.reply = Some((reply, since));
				Some(StandbySeen {
					reply_age_ms: now - since,
					sync: r[0].as_deref() == Some("sync"),
					flush_lsn: lsn(r.get(1)),
				})
			}
			_ => {
				self.reply = None;
				None
			}
		};
		Ok(Postgres::Primary {
			names: names.unwrap_or(SyncNames::Standby),
			standby,
			lsn: current,
		})
	}

	async fn call(&mut self, call: Call, primary: bool) -> Option<Answer> {
		let sent = self.now();
		let body = call.to_json().to_string();
		let request = format!(
			"POST /v1/clusters/{}/call HTTP/1.1\r\nhost: {}\r\nauthorization: Bearer {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
			self.cfg.cluster,
			self.cfg.witness.host,
			self.cfg.token,
			body.len()
		);
		let wait = Duration::from_millis(self.lease_ms / 4);
		let got = tokio::time::timeout(
			wait,
			http(&self.cfg.witness, self.witness_tls.as_ref(), request),
		)
		.await
		.unwrap_or_else(|_| Err(format!("{}: no answer in {wait:?}", self.cfg.witness)));
		let answer = match got {
			Ok((200, v)) => Answer::from_json(&v),
			Ok((status, v)) => {
				let m = v
					.pointer("/error/message")
					.and_then(Value::as_str)
					.unwrap_or("");
				self.trouble(format!("the witness answered {status}: {m}"));
				return None;
			}
			Err(m) => {
				self.trouble(m);
				return None;
			}
		};
		let Some(answer) = answer else {
			self.trouble("the witness's answer is not an answer".into());
			return None;
		};
		match &answer {
			Answer::Lease { lease_ms, .. } if *lease_ms != self.lease_ms => {
				tracing::warn!(
					witness = lease_ms,
					configured = self.lease_ms,
					"the witness's lease differs from LEPIS_GUARD_LEASE_MS; using the witness's"
				);
				self.lease_ms = *lease_ms;
				self.timeouts_set = false;
			}
			Answer::Granted { epoch } => {
				// On disk before this copy acts on it: a restart must not forget an epoch it holds.
				if let Err(m) = write_epoch(&self.cfg.state_file, *epoch) {
					self.trouble(format!("granted epoch {epoch} but could not keep it: {m}"));
					return None;
				}
				self.asked_at = sent;
				tracing::info!(epoch, "granted the primary's place");
			}
			Answer::Demoted { epoch, primary: p } if primary && !self.mem.demoted => {
				tracing::warn!(epoch, primary = %p, "demoted: another copy is the primary");
				self.hook(&self.cfg.on_demoted.clone(), "demoted");
			}
			Answer::Refused { reason } => self.trouble(format!("the witness refused: {reason}")),
			_ => {}
		}
		self.mem.heard(sent, primary, &answer);
		if !primary && let Answer::Demoted { epoch, .. } = answer {
			// A standby that learned a newer epoch keeps it.
			let _ = write_epoch(&self.cfg.state_file, epoch);
		}
		Some(answer)
	}

	fn hook(&self, command: &Option<String>, event: &str) {
		let Some(command) = command.clone() else {
			return;
		};
		let (event, epoch, copy) = (event.to_string(), self.mem.epoch, self.cfg.copy.clone());
		tokio::spawn(async move {
			let run = tokio::process::Command::new("sh")
				.arg("-c")
				.arg(&command)
				.env("LEPIS_GUARD_EVENT", &event)
				.env("LEPIS_GUARD_EPOCH", epoch.to_string())
				.env("LEPIS_GUARD_COPY", &copy)
				.kill_on_drop(true)
				.status();
			match tokio::time::timeout(Duration::from_secs(300), run).await {
				Ok(Ok(s)) if s.success() => tracing::info!(%event, "hook ran"),
				Ok(Ok(s)) => tracing::warn!(%event, %s, "hook failed"),
				Ok(Err(e)) => tracing::warn!(%event, "hook did not start: {e}"),
				Err(_) => tracing::warn!(%event, "hook took over 300 s and was stopped"),
			}
		});
	}

	async fn set(&mut self, settings: &[(&str, Option<String>)]) -> Result<(), String> {
		// One statement at a time: ALTER SYSTEM refuses to run inside a transaction block, and a
		// multi-statement query string is one.
		for (name, value) in settings {
			let sql = match value {
				Some(v) => format!("ALTER SYSTEM SET {name} = {}", sql_literal(v)),
				None => format!("ALTER SYSTEM RESET {name}"),
			};
			self.run_sql(&sql).await?;
		}
		self.run_sql("SELECT pg_reload_conf()").await
	}

	async fn act(&mut self, action: Action, pg: &Postgres) -> Result<(), String> {
		let (copy, epoch) = (self.cfg.copy.clone(), self.mem.epoch);
		let primary = matches!(pg, Postgres::Primary { .. });
		let peer = quoted(&self.cfg.peer);
		match action {
			Action::Renew => {
				self.call(Call::Renew { copy, epoch }, primary).await;
			}
			Action::Eligible(eligible) => {
				self.call(
					Call::Eligible {
						copy,
						epoch,
						eligible,
					},
					primary,
				)
				.await;
			}
			Action::Ask => {
				let now = self.now();
				let lost = match pg {
					Postgres::Standby {
						receive_age_ms: Some(age),
					} => *age,
					_ => now.saturating_sub(self.stream_gone_since.unwrap_or(now)),
				};
				self.call(
					Call::Ask {
						copy,
						epoch,
						stream_lost_ms: lost,
					},
					primary,
				)
				.await;
			}
			Action::GoAsync => {
				self.names_generation += 1;
				self.set(&[("synchronous_standby_names", Some(String::new()))])
					.await?;
				tracing::warn!(
					"the standby is gone and the witness agrees: commits go on without it"
				);
			}
			Action::GoSync => {
				self.names_generation += 1;
				self.mem.sync_from_lsn = None;
				self.set(&[("synchronous_standby_names", Some(peer))])
					.await?;
				tracing::info!("the standby is back: commits wait for it again");
			}
			Action::Fence => {
				self.names_generation += 1;
				self.set(&[
					("synchronous_standby_names", Some(FENCED_NAME.into())),
					("default_transaction_read_only", Some("on".into())),
				])
				.await?;
				self.run_sql(
					"SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
					 WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()",
				)
				.await?;
				tracing::warn!("fenced: no lease and no standby");
				self.hook(&self.cfg.on_fence.clone(), "fence");
			}
			Action::Unfence => {
				self.names_generation += 1;
				self.mem.sync_from_lsn = None;
				self.set(&[
					("synchronous_standby_names", Some(peer)),
					("default_transaction_read_only", None),
				])
				.await?;
				tracing::info!("unfenced: commits wait for the standby");
			}
			Action::Promote(epoch) => {
				self.set(&[
					("synchronous_standby_names", Some(String::new())),
					("default_transaction_read_only", None),
				])
				.await?;
				let wait = (self.lease_ms / 2000).max(1).to_string();
				let done = self
					.rows("SELECT pg_promote(true, $1::int)::text", &[&wait])
					.await;
				match done {
					// A promotion already under way refuses a second one; the next tick sees which.
					Ok(r) if r.first().and_then(|r| r[0].as_deref()) == Some("true") => {
						self.mem = Memory::promoted(epoch, self.asked_at);
						self.reply = None;
						self.names_generation += 1;
						tracing::warn!(epoch, "promoted: this copy is the primary");
						self.hook(&self.cfg.on_promote.clone(), "promote");
					}
					Ok(_) => return Err("pg_promote did not finish in time".into()),
					Err(m) => return Err(format!("pg_promote: {m}")),
				}
			}
		}
		Ok(())
	}

	/// Starts the barrier when the standby is named and its starting LSN is unknown, and takes the
	/// result of one that finished for the current names.
	async fn barrier(&mut self, pg: &Postgres) {
		let named = matches!(
			pg,
			Postgres::Primary {
				names: SyncNames::Standby,
				..
			}
		);
		if self.barrier.as_ref().is_some_and(|(_, h)| h.is_finished()) {
			let (generation, handle) = self.barrier.take().expect("just seen");
			let result = handle.await.unwrap_or_else(|e| Err(e.to_string()));
			match result {
				Ok(lsn) if generation == self.names_generation && named => {
					self.mem.sync_from_lsn = Some(lsn);
					tracing::info!(
						lsn,
						"the standby is vouched for once it has flushed past this"
					);
				}
				Ok(_) => {}
				Err(m) => self.trouble(format!("sync barrier: {m}")),
			}
		}
		if named && self.mem.sync_from_lsn.is_none() && self.barrier.is_none() {
			let handle = tokio::spawn(sync_barrier(self.cfg.clone(), quoted(&self.cfg.peer)));
			self.barrier = Some((self.names_generation, handle));
		}
	}

	async fn timeouts(&mut self) {
		if self.timeouts_set {
			return;
		}
		let wanted = timeout_settings(self.lease_ms);
		let names: Vec<String> = wanted.iter().map(|(n, _)| sql_literal(n)).collect();
		let sql = format!(
			"SELECT name, setting || coalesce(unit, '') FROM pg_settings WHERE name IN ({})",
			names.join(", ")
		);
		let Ok(rows) = self.rows(&sql, &[]).await else {
			return;
		};
		let have: HashMap<String, String> = rows
			.into_iter()
			.filter_map(|r| Some((r[0].clone()?, r[1].clone()?)))
			.collect();
		// pg_settings speaks in each setting's own unit (ms or s): compare in milliseconds.
		let ms = |v: &str| -> Option<u64> {
			let digits: String = v.chars().take_while(char::is_ascii_digit).collect();
			let n: u64 = digits.parse().ok()?;
			Some(if v.ends_with("ms") { n } else { n * 1000 })
		};
		let change: Vec<(&str, Option<String>)> = wanted
			.iter()
			.filter(|(n, v)| have.get(*n).and_then(|h| ms(h)) != ms(v))
			.map(|(n, v)| (*n, Some(v.clone())))
			.collect();
		if change.is_empty() || self.set(&change).await.is_ok() {
			self.timeouts_set = true;
		}
	}

	fn write_status(&self, pg: &Postgres, now: u64) {
		let Some(path) = &self.cfg.status_file else {
			return;
		};
		let tmp = path.with_extension("tmp");
		let text = status_json(self, pg, now).to_string();
		if std::fs::write(&tmp, text).is_ok() {
			let _ = std::fs::rename(&tmp, path);
		}
	}

	async fn tick(&mut self) {
		let now = self.now();
		let pg = self.facts(now).await;
		if !matches!(pg, Postgres::Down) {
			self.timeouts().await;
		}
		self.barrier(&pg).await;
		let mut failed = false;
		for action in decide(now, self.lease_ms, pg, &self.mem) {
			if let Err(m) = self.act(action, &pg).await {
				self.trouble(format!("{action:?}: {m}"));
				failed = true;
				break;
			}
		}
		if !failed && self.last_error.is_some() && !matches!(pg, Postgres::Down) {
			self.last_error = None;
		}
		self.write_status(&pg, now);
	}
}

/// Runs until stopped. A primary renews once before its first decision, so a restart does not
/// fence a healthy copy for want of a lease it simply has not asked for yet.
pub async fn run_guard(cfg: GuardConfig) -> Result<(), String> {
	let epoch = read_epoch(&cfg.state_file)?;
	// The client config speaks Postgres's ALPN; the witness's listener offers HTTP/1.1.
	let tls_for = |a: &NodeAddress, alpn: &[u8]| -> Result<_, String> {
		Ok(crate::tls::client_config(a)?.map(|c| {
			let mut c = (*c).clone();
			c.alpn_protocols = vec![alpn.to_vec()];
			Arc::new(c)
		}))
	};
	let mut g = Guard {
		witness_tls: tls_for(&cfg.witness, b"http/1.1")?,
		lease_ms: cfg.lease_ms,
		cfg,
		start: Instant::now(),
		mem: Memory {
			epoch,
			..Memory::default()
		},
		pg: None,
		reply: None,
		stream_gone_since: None,
		names_generation: 0,
		barrier: None,
		asked_at: 0,
		timeouts_set: false,
		last_error: None,
	};
	tracing::info!(copy = %g.cfg.copy, cluster = %g.cfg.cluster, epoch, witness = %g.cfg.witness, "snout-lepis guard starting");
	let now = g.now();
	if let Postgres::Primary { .. } = g.facts(now).await {
		let copy = g.cfg.copy.clone();
		g.call(Call::Renew { copy, epoch }, true).await;
	}
	let mut every = tokio::time::interval(Duration::from_millis(g.cfg.tick_ms));
	every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
	loop {
		every.tick().await;
		g.tick().await;
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
		let mut m: HashMap<String, String> = [
			("LEPIS_GUARD_WITNESS", "10.99.0.3:7445"),
			("LEPIS_GUARD_WITNESS_SSLMODE", "disable"),
			("LEPIS_GUARD_CLUSTER", "control-plane"),
			("LEPIS_GUARD_TOKEN", "t"),
			("LEPIS_GUARD_COPY", "oregon"),
			("LEPIS_GUARD_PEER", "toronto"),
			("LEPIS_GUARD_PG_PASSWORD", "p"),
			("LEPIS_GUARD_STATE", "/var/lib/lepis/guard.json"),
		]
		.iter()
		.map(|(k, v)| (k.to_string(), v.to_string()))
		.collect();
		for (k, v) in pairs {
			m.insert(k.to_string(), v.to_string());
		}
		m
	}

	#[test]
	fn the_configuration_has_safe_defaults_and_refuses_what_cannot_work() {
		let c = GuardConfig::from_map(&env(&[])).unwrap();
		assert_eq!((c.pg.host.as_str(), c.pg.port), ("127.0.0.1", 5432));
		assert_eq!(c.pg.sslmode, SslMode::Disable);
		assert_eq!(c.witness.sslmode, SslMode::Disable);
		assert_eq!((c.lease_ms, c.tick_ms), (10_000, 1_000));
		let remote =
			GuardConfig::from_map(&env(&[("LEPIS_GUARD_PG", "db.internal:6432")])).unwrap();
		assert_eq!(remote.pg.sslmode, SslMode::VerifyFull);
		for bad in [
			("LEPIS_GUARD_TICK_MS", "2001"),
			("LEPIS_GUARD_LEASE_MS", "1000"),
			("LEPIS_GUARD_PEER", "oregon"),
			("LEPIS_GUARD_COPY", FENCED_NAME),
			("LEPIS_GUARD_PG_SSLMODE", "prefer"),
		] {
			assert!(GuardConfig::from_map(&env(&[bad])).is_err(), "{bad:?}");
		}
		let mut missing = env(&[]);
		missing.remove("LEPIS_GUARD_TOKEN");
		assert!(GuardConfig::from_map(&missing).is_err());
	}

	#[test]
	fn names_read_back_as_this_guard_writes_them_and_anything_else_is_unknown() {
		assert_eq!(quoted("toronto"), "\"toronto\"");
		assert_eq!(quoted("a\"b"), "\"a\"\"b\"");
		assert_eq!(names_of("", "toronto"), Some(SyncNames::Off));
		assert_eq!(names_of("lepis_fenced", "toronto"), Some(SyncNames::Fenced));
		assert_eq!(names_of("\"toronto\"", "toronto"), Some(SyncNames::Standby));
		assert_eq!(names_of("toronto", "toronto"), Some(SyncNames::Standby));
		// A keeper could stand in for the standby: never read as the standby alone.
		assert_eq!(names_of("FIRST 1 (toronto, keeper)", "toronto"), None);
		assert_eq!(names_of("*", "toronto"), None);
	}

	#[test]
	fn the_timeouts_let_an_idle_cluster_hear_itself_every_quarter_lease() {
		let t = timeout_settings(10_000);
		assert_eq!(t[0], ("wal_sender_timeout", "5000ms".to_string()));
		assert_eq!(t[1], ("wal_receiver_timeout", "5000ms".to_string()));
		assert_eq!(t[2], ("wal_receiver_status_interval", "10s".to_string()));
	}

	#[test]
	fn the_epoch_survives_on_disk_and_starts_at_one() {
		let dir = std::env::temp_dir().join(format!("lepis-guard-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&dir);
		std::fs::create_dir_all(&dir).unwrap();
		let f = dir.join("guard.json");
		assert_eq!(read_epoch(&f).unwrap(), 1);
		write_epoch(&f, 7).unwrap();
		assert_eq!(read_epoch(&f).unwrap(), 7);
		std::fs::write(&f, "{}").unwrap();
		assert!(read_epoch(&f).is_err());
		let _ = std::fs::remove_dir_all(&dir);
	}
}
