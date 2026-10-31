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

/// Whether `action` is a call to the witness (the rest act on this copy alone).
pub fn calls_witness(action: Action) -> bool {
	matches!(action, Action::Renew | Action::Eligible(_) | Action::Ask)
}

/// A tick's actions as the runner takes them: what acts on this copy first, then the calls to the
/// witness, each kind in `decide`'s order. A call can take a quarter lease to give up on a silent
/// witness, and a fence decided in the same tick must not wait for it (the 2026-10-26 network cut
/// fenced at +9 s behind two such calls); and after one unanswered call the tick makes no more.
pub fn in_tick_order(actions: Vec<Action>) -> Vec<Action> {
	let (calls, local): (Vec<Action>, Vec<Action>) =
		actions.into_iter().partition(|a| calls_witness(*a));
	local.into_iter().chain(calls).collect()
}

/// How long a renewal may wait for the witness: a quarter lease, but never past the moment this
/// copy's own lease runs out (half a lease after the last renewal accepted, `decide`), less a
/// floor of a twentieth, so the decision that fences comes then and not a call's timeout later.
pub fn renew_wait_ms(now: u64, renewed_at: Option<u64>, lease_ms: u64) -> u64 {
	let quarter = lease_ms / 4;
	match renewed_at {
		Some(t) => (t + lease_ms / 2)
			.saturating_sub(now)
			.clamp(lease_ms / 20, quarter.max(lease_ms / 20)),
		None => quarter,
	}
}

/// The standby's stream, from one reading of its receiver's `age` (since the last message, `None`
/// with no receiver streaming): since when it has been gone, and when the last message came. Gone
/// counts from that last message, not from when the receiver gave up (`wal_receiver_timeout`, half
/// a lease later): a silent cut lost the stream at the cut, and was granted at +15.7 s for it.
pub fn stream_gone(
	gone_since: Option<u64>,
	last_receipt: Option<u64>,
	age: Option<u64>,
	now: u64,
) -> (Option<u64>, Option<u64>) {
	match age {
		Some(a) => (None, Some(now.saturating_sub(a))),
		None => (
			Some(gone_since.or(last_receipt).unwrap_or(now)),
			last_receipt,
		),
	}
}

fn sql_literal(s: &str) -> String {
	format!("'{}'", s.replace('\'', "''"))
}

/// `archive_command` as the guard reads it: empty when `archive_mode` is off, where Postgres shows
/// it as `(disabled)` whatever it holds, so a copy that archives nothing is never "guarded".
macro_rules! archive_command_sql {
	() => {
		"CASE WHEN current_setting('archive_mode') = 'off' THEN '' \
		 ELSE current_setting('archive_command') END"
	};
}

/// The guard a fenced primary's `archive_command` carries, before the command it had: the shell
/// archives a file only when its name sorts before the segment holding the standby's last flush.
/// Every segment before that one the standby has whole, so nothing past the fork reaches the
/// archive, where an archive-fed standby would replay it and never reach the new timeline (the
/// 2026-10-26 network cut). The `x` keeps `expr` from reading two all-digit names
/// as numbers, and `LC_ALL=C` makes the order bytewise (WAL names are fixed-width hex).
const ARCHIVE_GUARD_HEAD: &str = "LC_ALL=C expr \"x%f\" \"<\" \"x";
const ARCHIVE_GUARD_TAIL: &str = "\" >/dev/null && ";

/// The bound a fence gives the archive: the WAL file name holding the standby's last flush, or,
/// with no flush ever seen or a name that is not one, `0`, which every file name sorts after, so
/// nothing more is archived. A stale flush is an earlier segment: it archives less, never more.
pub fn archive_bound(walfile: Option<&str>) -> String {
	match walfile {
		Some(n)
			if n.len() == 24
				&& n.bytes()
					.all(|b| b.is_ascii_digit() || (b'A'..=b'F').contains(&b)) =>
		{
			n.to_string()
		}
		_ => "0".to_string(),
	}
}

/// `command` with the fence's guard at `bound` in front (any guard it had replaced).
pub fn fence_archive_command(command: &str, bound: &str) -> String {
	format!(
		"{ARCHIVE_GUARD_HEAD}{bound}{ARCHIVE_GUARD_TAIL}{}",
		unfence_archive_command(command)
	)
}

/// `command` without the fence's guard: the command it had before the fence.
pub fn unfence_archive_command(command: &str) -> &str {
	let Some(rest) = command.strip_prefix(ARCHIVE_GUARD_HEAD) else {
		return command;
	};
	let bound = rest.bytes().take_while(u8::is_ascii_alphanumeric).count();
	rest[bound..]
		.strip_prefix(ARCHIVE_GUARD_TAIL)
		.unwrap_or(command)
}

pub fn archive_is_fenced(command: &str) -> bool {
	unfence_archive_command(command) != command
}

/// What a promotion's `restore_command` carries in front of the command it had, from the reload
/// before `pg_promote` until it returns. Ending recovery asks the archive for a file five times
/// (the WAL segment past the stream twice, the next timeline's history twice, the current one's
/// once), and with pgBackRest reading S3 each miss is most of a second: the 4.1 s promotion of the
/// 2026-10-26 drill, where 0.16 s is Postgres's own. A segment is refused at once: the stream had
/// every acknowledged commit, and a fenced primary archives nothing past the standby's flush. A
/// history file `pg_wal` has is refused too, since Postgres then reads its own copy, which is what
/// it does when the archive misses. A history file `pg_wal` lacks still goes to the archive, so a
/// timeline another copy took there is never chosen again.
const RESTORE_GUARD: &str =
	"case \"%f\" in *.history) [ -f \"pg_wal/%f\" ] && exit 1;; *) exit 1;; esac; ";

/// `command` with the promotion's guard in front (any guard it had replaced).
pub fn promote_restore_command(command: &str) -> String {
	format!("{RESTORE_GUARD}{}", plain_restore_command(command))
}

/// `command` without the promotion's guard: the command it had.
pub fn plain_restore_command(command: &str) -> &str {
	command.strip_prefix(RESTORE_GUARD).unwrap_or(command)
}

/// The completed segments of the timeline just left, among `pg_wal`'s file names, for a copy now
/// on `timeline` (hex, as in a WAL file name). A promoted standby archives none of what it
/// received (`archive_mode = on`), and a fenced primary held back whatever it had not archived
/// yet, so these may be missing from the archive. Pushing one already there is harmless: the
/// archiver refuses only a different file under the same name, which is a branch.
pub fn left_timeline_segments<'a>(timeline: &str, names: &[&'a str]) -> Vec<&'a str> {
	let Ok(now) = u32::from_str_radix(timeline, 16) else {
		return Vec::new();
	};
	// Timelines start at 1: the first has none before it.
	let Some(left) = now.checked_sub(1).filter(|t| *t >= 1) else {
		return Vec::new();
	};
	let prefix = format!("{left:08X}");
	let mut out: Vec<&str> = names
		.iter()
		.copied()
		.filter(|n| {
			n.len() == 24 && n.starts_with(&prefix) && n.bytes().all(|b| b.is_ascii_hexdigit())
		})
		.collect();
	out.sort_unstable();
	out
}

/// One shell script running `command` (an `archive_command`, `%p`, `%f` and `%%` as Postgres
/// expands them, from the data directory) for each of `names`, printing `pushed <name>` or
/// `failed <name>` for each.
pub fn push_script(command: &str, names: &[&str]) -> String {
	let mut cmd = String::new();
	let mut chars = command.chars();
	while let Some(c) = chars.next() {
		match (c, c == '%') {
			(_, true) => match chars.next() {
				Some('p') => cmd.push_str("\"pg_wal/$f\""),
				Some('f') => cmd.push_str("\"$f\""),
				Some('%') => cmd.push('%'),
				Some(other) => {
					cmd.push('%');
					cmd.push(other);
				}
				None => cmd.push('%'),
			},
			_ => cmd.push(c),
		}
	}
	format!(
		"for f in {}; do if ( {cmd} ) >/dev/null 2>&1; then echo \"pushed $f\"; else echo \"failed $f\"; fi; done",
		names.join(" ")
	)
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

/// After a promotion: the timeline just left, pushed to the archive with the copy's own
/// `archive_command`, from its own data directory (`COPY ... FROM PROGRAM`, as the guard's
/// superuser), so a segment the old primary never archived (a fence holds back everything from
/// the segment its standby was in) is not missing from the archive. Returns how many were pushed
/// and the names that failed.
async fn push_left_timeline(
	cfg: GuardConfig,
	command: String,
) -> Result<(usize, Vec<String>), String> {
	let mut b = pg_connect(&cfg).await?;
	let found = b
		.query(
			"SELECT substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8), \
			 coalesce((SELECT string_agg(name, ' ') FROM pg_ls_waldir()), '')",
			&[],
		)
		.await
		.map_err(pg_error)?;
	let row = found.first().ok_or("no row")?;
	let timeline = row[0].clone().unwrap_or_default();
	let all = row[1].clone().unwrap_or_default();
	let names: Vec<&str> = all.split_whitespace().collect();
	let left = left_timeline_segments(&timeline, &names);
	if left.is_empty() {
		b.close().await;
		return Ok((0, Vec::new()));
	}
	b.simple("CREATE TEMP TABLE lepis_pushed (line text)")
		.await
		.map_err(pg_error)?;
	b.simple(&format!(
		"COPY lepis_pushed FROM PROGRAM {}",
		sql_literal(&push_script(&command, &left))
	))
	.await
	.map_err(pg_error)?;
	let lines = b
		.query("SELECT line FROM lepis_pushed", &[])
		.await
		.map_err(pg_error)?;
	b.close().await;
	let lines: Vec<String> = lines.iter().filter_map(|r| r[0].clone()).collect();
	let pushed = lines.iter().filter(|l| l.starts_with("pushed ")).count();
	let failed = lines
		.iter()
		.filter_map(|l| l.strip_prefix("failed ").map(str::to_string))
		.collect();
	Ok((pushed, failed))
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

/// What the status file says, for the host agent and the CLI. A contract: the host agent
/// (`snoutpod/src/host/guard.ts`, `parseGuardStatus`) and snout-metis (which takes the primary's
/// region from it) read these field names and values, so a change here is a change there.
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
	let archive = match pg {
		Postgres::Primary {
			archive_fenced: Some(fenced),
			..
		} => Some(if *fenced { "fenced" } else { "open" }),
		_ => None,
	};
	json!({
		"copy": g.cfg.copy,
		"cluster": g.cfg.cluster,
		"role": role,
		"synchronous": names,
		"standby_up": standby,
		"archive": archive,
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
	/// Standby: when the last message from the primary arrived, as last read while it streamed.
	last_receipt_at: Option<u64>,
	/// This tick: a call to the witness went unanswered, so no more are made until the next.
	witness_silent: bool,
	/// Primary: the highest flush its standby has reported (`pg_stat_replication`), kept after the
	/// standby's row is gone. What `FenceArchive` lets the archive have.
	last_flush: Option<u64>,
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
				concat!(
					"SELECT pg_is_in_recovery()::text, current_setting('synchronous_standby_names'), \
					 CASE WHEN pg_is_in_recovery() THEN NULL \
					 ELSE pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::text END, ",
					archive_command_sql!()
				),
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
			(self.stream_gone_since, self.last_receipt_at) =
				stream_gone(self.stream_gone_since, self.last_receipt_at, age, now);
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
		// The standby's flush as last read, kept when its row goes: what a fence lets the archive have.
		// Flush only moves forward, so an old reading is a lower bound, never past the fork.
		if let Some(r) = rep.first() {
			let flush = lsn(r.get(1));
			self.last_flush = Some(self.last_flush.map_or(flush, |f| f.max(flush)));
		}
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
		let archive = row.get(3).cloned().flatten().unwrap_or_default();
		Ok(Postgres::Primary {
			names: names.unwrap_or(SyncNames::Standby),
			standby,
			lsn: current,
			archive_fenced: (!archive.trim().is_empty()).then(|| archive_is_fenced(&archive)),
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
		let wait = Duration::from_millis(match &call {
			Call::Renew { .. } => renew_wait_ms(sent, self.mem.renewed_at, self.lease_ms),
			_ => self.lease_ms / 4,
		});
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
				self.witness_silent = true;
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

	/// `archive_command` now, and its `pg_settings.context`.
	async fn archive_command(&mut self) -> Result<(String, Option<String>), String> {
		let r = self
			.rows(
				concat!(
					"SELECT ",
					archive_command_sql!(),
					", (SELECT context FROM pg_settings WHERE name = 'archive_command')"
				),
				&[],
			)
			.await?;
		let row = r.first().ok_or("no row")?;
		Ok((row[0].clone().unwrap_or_default(), row[1].clone()))
	}

	/// `restore_command` now, and its `pg_settings.context` (`sighup` since Postgres 14).
	async fn restore_command(&mut self) -> Result<(String, Option<String>), String> {
		let r = self
			.rows(
				"SELECT current_setting('restore_command'), \
				 (SELECT context FROM pg_settings WHERE name = 'restore_command')",
				&[],
			)
			.await?;
		let row = r.first().ok_or("no row")?;
		Ok((row[0].clone().unwrap_or_default(), row[1].clone()))
	}

	/// `restore_command` back as it was, wherever a promotion's guard is left on it (a promotion
	/// that ended, or a guard that stopped in the middle of one). Best effort: a failure is logged,
	/// and the guard only refuses archive reads, never a write.
	async fn restore_plain(&mut self) {
		match self.restore_command().await {
			Ok((command, context)) if plain_restore_command(&command) != command => {
				let plain = plain_restore_command(&command).to_string();
				if context.as_deref() != Some("sighup") {
					self.trouble(format!(
						"restore_command takes {context:?} here, not a reload: the promotion's guard is left on it"
					));
				} else if let Err(m) = self.set(&[("restore_command", Some(plain))]).await {
					self.trouble(format!("restore_command could not be put back: {m}"));
				}
			}
			Ok(_) => {}
			Err(m) => self.trouble(format!("restore_command could not be read: {m}")),
		}
	}

	/// `archive_command` set by a reload, never a restart: refused unless Postgres says a reload
	/// applies it (`sighup`, as it has since 8.3). An error leaves everything else as it was, the
	/// fence included, and the next tick reads the command again and tries again.
	async fn set_archive(&mut self, command: &str, context: Option<&str>) -> Result<(), String> {
		// Nothing archives (or archiving went off since the reading): nothing to guard or open.
		if unfence_archive_command(command).trim().is_empty() {
			return Ok(());
		}
		if context != Some("sighup") {
			return Err(format!(
				"archive_command takes {context:?} here, not a reload: left as it is"
			));
		}
		self.set(&[("archive_command", Some(command.to_string()))])
			.await
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
			Action::FenceArchive => {
				let flush = self
					.last_flush
					.map(|l| format!("{:X}/{:X}", l >> 32, l & 0xffff_ffff))
					.unwrap_or_default();
				let r = self
					.rows(
						concat!(
							"SELECT ",
							archive_command_sql!(),
							", (SELECT context FROM pg_settings WHERE name = 'archive_command'), \
							 CASE WHEN $1 = '' THEN NULL ELSE pg_walfile_name($1::pg_lsn) END"
						),
						&[&flush],
					)
					.await?;
				let row = r.first().ok_or("no row")?;
				let (command, context) = (row[0].clone().unwrap_or_default(), row[1].clone());
				let bound = archive_bound(row[2].as_deref());
				self.set_archive(&fence_archive_command(&command, &bound), context.as_deref())
					.await?;
				tracing::warn!(
					%bound,
					"the archive fenced too: only segments before the standby's last flush"
				);
			}
			Action::UnfenceArchive => {
				let (command, context) = self.archive_command().await?;
				self.set_archive(unfence_archive_command(&command), context.as_deref())
					.await?;
				tracing::info!("the archive is open again");
			}
			// Out of recovery while still holding the grant: the promotion outlasted
			// `pg_promote`'s wait (half a lease) below and finished since. Recorded now, as an
			// answer in time would have been (`decide`).
			Action::Promote(epoch) if matches!(pg, Postgres::Primary { .. }) => {
				let (command, _) = self.archive_command().await?;
				tracing::warn!(epoch, "the promotion finished after pg_promote's wait");
				self.promoted(epoch, unfence_archive_command(&command).to_string());
			}
			Action::Promote(epoch) => {
				// The archive opened in the same reload: a copy fenced as a primary once, and a
				// standby since, may still carry the guard, and the new timeline sorts past its bound.
				let (command, context) = self.archive_command().await?;
				let open = unfence_archive_command(&command).to_string();
				let mut settings = vec![
					("synchronous_standby_names", Some(String::new())),
					("default_transaction_read_only", None),
				];
				if open != command && context.as_deref() == Some("sighup") {
					settings.push(("archive_command", Some(open.clone())));
				}
				// And the archive asked only for what a promotion cannot do without (RESTORE_GUARD).
				let (restore, restore_context) = self.restore_command().await?;
				if !plain_restore_command(&restore).trim().is_empty()
					&& restore_context.as_deref() == Some("sighup")
				{
					settings.push(("restore_command", Some(promote_restore_command(&restore))));
				}
				self.set(&settings).await?;
				let wait = (self.lease_ms / 2000).max(1).to_string();
				let done = self
					.rows("SELECT pg_promote(true, $1::int)::text", &[&wait])
					.await;
				// Whatever it answered: a promotion still under way reads the archive as before.
				self.restore_plain().await;
				match done {
					// A promotion already under way refuses a second one; the next tick sees which.
					Ok(r) if r.first().and_then(|r| r[0].as_deref()) == Some("true") => {
						self.promoted(epoch, open);
					}
					// Still under way: the grant is kept, and the tick that finds the copy out of
					// recovery records it (the arm above).
					Ok(_) => return Err("pg_promote did not finish in time".into()),
					Err(m) => return Err(format!("pg_promote: {m}")),
				}
			}
		}
		Ok(())
	}

	/// The promotion at `epoch` is done: remember it, run `on_promote`, and push what the archive
	/// is missing of the timeline left (`open` is the unguarded `archive_command`).
	fn promoted(&mut self, epoch: u64, open: String) {
		self.mem = Memory::promoted(epoch, self.asked_at);
		self.reply = None;
		self.names_generation += 1;
		tracing::warn!(epoch, "promoted: this copy is the primary");
		self.hook(&self.cfg.on_promote.clone(), "promote");
		if !open.trim().is_empty() {
			let cfg = self.cfg.clone();
			tokio::spawn(async move {
				match push_left_timeline(cfg, open).await {
					Ok((pushed, failed)) if failed.is_empty() => {
						tracing::info!(pushed, "the timeline left is in the archive")
					}
					Ok((pushed, failed)) => tracing::warn!(
						pushed,
						?failed,
						"segments of the timeline left could not be archived"
					),
					Err(m) => tracing::warn!("archiving the timeline left: {m}"),
				}
			});
		}
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
		self.witness_silent = false;
		let granted_before = self.mem.granted;
		for action in in_tick_order(decide(now, self.lease_ms, pg, &self.mem)) {
			if self.witness_silent && calls_witness(action) {
				continue;
			}
			if let Err(m) = self.act(action, &pg).await {
				self.trouble(format!("{action:?}: {m}"));
				failed = true;
				break;
			}
		}
		// A grant that came in this tick is acted on in it, as the next tick's `decide` would (a
		// standby holding a grant is promoted, whatever else it reads), a tick sooner: a second of
		// the live drill's 4.1 s from the grant to the promotion.
		if !failed
			&& granted_before.is_none()
			&& matches!(pg, Postgres::Standby { .. })
			&& let Some(epoch) = self.mem.granted
		{
			let action = Action::Promote(epoch);
			if let Err(m) = self.act(action, &pg).await {
				self.trouble(format!("{action:?}: {m}"));
				failed = true;
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
		last_receipt_at: None,
		witness_silent: false,
		last_flush: None,
		names_generation: 0,
		barrier: None,
		asked_at: 0,
		timeouts_set: false,
		last_error: None,
	};
	tracing::info!(copy = %g.cfg.copy, cluster = %g.cfg.cluster, epoch, witness = %g.cfg.witness, "snout-lepis guard starting");
	let now = g.now();
	let pg = g.facts(now).await;
	// A guard stopped between a promotion's reload and its end left RESTORE_GUARD on: off it comes.
	if !matches!(pg, Postgres::Down) {
		g.restore_plain().await;
	}
	if let Postgres::Primary { .. } = pg {
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

	const PUSH: &str = "snoutpod-pgbackrest archive-push %p";

	/// What the archiver runs for `name`: `command` with `%p` and `%f` expanded as Postgres does,
	/// run by `sh` in `dir`; its output.
	fn archiver(command: &str, name: &str, dir: &Path) -> String {
		let cmd = command
			.replace("%p", &format!("pg_wal/{name}"))
			.replace("%f", name);
		let out = std::process::Command::new("sh")
			.arg("-c")
			.arg(&cmd)
			.current_dir(dir)
			.output()
			.expect("sh runs");
		String::from_utf8_lossy(&out.stdout).trim().to_string()
	}

	fn archived<'a>(command: &str, names: &[&'a str]) -> Vec<&'a str> {
		let dir = std::env::temp_dir();
		names
			.iter()
			.copied()
			.filter(|n| archiver(command, n, &dir) == format!("archived {n}"))
			.collect()
	}

	const GET: &str = "snoutpod-pgbackrest archive-get %f \"%p\"";

	/// What a promotion's `restore_command` does for `name` in a data directory whose `pg_wal`
	/// holds `local`: whether it found the file, and what the command it had was asked.
	fn restored(command: &str, name: &str, local: &[&str]) -> (bool, String) {
		let dir = std::env::temp_dir().join(format!("lepis-restore-{}-{name}", std::process::id()));
		std::fs::create_dir_all(dir.join("pg_wal")).unwrap();
		for f in local {
			std::fs::write(dir.join("pg_wal").join(f), "").unwrap();
		}
		let out = std::process::Command::new("sh")
			.arg("-c")
			.arg(
				command
					.replace("%p", "pg_wal/RECOVERYXLOG")
					.replace("%f", name),
			)
			.current_dir(&dir)
			.output()
			.expect("sh runs");
		std::fs::remove_dir_all(&dir).unwrap();
		(
			out.status.success(),
			String::from_utf8_lossy(&out.stdout).trim().to_string(),
		)
	}

	#[test]
	fn a_promotion_asks_the_archive_only_for_a_timeline_it_has_not_seen() {
		// The 2026-10-26 drill's promoted copy: on timeline 3, its history in pg_wal.
		let guarded = promote_restore_command("echo asked %f");
		let local = ["00000003.history", "000000030000001B000000D2"];
		// Segments: never asked, whether pg_wal has them or not.
		for seg in ["000000030000001B000000D3", "000000030000001B000000D2"] {
			assert_eq!(
				restored(&guarded, seg, &local),
				(false, String::new()),
				"{seg}"
			);
		}
		// Its own timeline's history: Postgres reads pg_wal's copy.
		assert_eq!(
			restored(&guarded, "00000003.history", &local),
			(false, String::new())
		);
		// The next timeline's: still asked, so one another copy took is never chosen again.
		assert_eq!(
			restored(&guarded, "00000004.history", &local),
			(true, "asked 00000004.history".to_string())
		);
		// Unguarded, everything is asked, as before.
		assert_eq!(
			restored("echo asked %f", "000000030000001B000000D3", &local).1,
			"asked 000000030000001B000000D3"
		);
		// On and off, never stacked, and nothing else is touched by the strip.
		assert_eq!(plain_restore_command(&promote_restore_command(GET)), GET);
		let twice = promote_restore_command(&promote_restore_command(GET));
		assert_eq!(twice.matches("case").count(), 1);
		assert_eq!(plain_restore_command(GET), GET);
		assert_eq!(plain_restore_command(""), "");
	}

	#[test]
	fn the_fence_guard_goes_on_and_comes_off_the_command_it_had() {
		let fenced = fence_archive_command(PUSH, "000000030000001B000000D2");
		assert!(archive_is_fenced(&fenced));
		assert_eq!(unfence_archive_command(&fenced), PUSH);
		// A second fence moves the bound; it never stacks a second guard.
		let again = fence_archive_command(&fenced, "000000030000001B000000D4");
		assert_eq!(again.matches("expr").count(), 1);
		assert!(again.contains("x000000030000001B000000D4"));
		assert_eq!(unfence_archive_command(&again), PUSH);
		// The command as it was is never touched by the strip, nor anything that only looks close.
		assert!(!archive_is_fenced(PUSH));
		assert_eq!(unfence_archive_command(PUSH), PUSH);
		let near = "LC_ALL=C expr \"x%f\" \"<\" \"x0\" && something";
		assert_eq!(unfence_archive_command(near), near);
		assert_eq!(
			archive_bound(Some("000000030000001B000000D2")),
			"000000030000001B000000D2"
		);
		for odd in [
			None,
			Some(""),
			Some("00000003.history"),
			Some("000000030000001b000000d2"),
		] {
			assert_eq!(archive_bound(odd), "0");
		}
	}

	#[test]
	fn a_fenced_primary_archives_nothing_past_the_fork() {
		// The 2026-10-26 cut: the standby's last flush in D2, the fenced copy writing on into D4.
		let fenced = fence_archive_command("echo archived %f", "000000030000001B000000D2");
		let pg_wal = [
			"000000030000001B000000D0",
			"000000030000001B000000D1",
			"000000030000001B000000D2",
			"000000030000001B000000D3",
			"000000030000001B000000D4",
		];
		assert_eq!(
			archived(&fenced, &pg_wal),
			["000000030000001B000000D0", "000000030000001B000000D1"]
		);
		// All-digit names are compared as strings, not numbers; a later timeline sorts after.
		let digits = fence_archive_command("echo archived %f", "000000030000002000000010");
		assert_eq!(
			archived(
				&digits,
				&[
					"000000030000002000000009",
					"000000030000002000000011",
					"000000040000002000000001"
				]
			),
			["000000030000002000000009"]
		);
		// No flush ever seen: nothing more at all.
		let blind = fence_archive_command("echo archived %f", &archive_bound(None));
		assert!(archived(&blind, &pg_wal).is_empty());
		assert!(archived(&blind, &["00000003.history"]).is_empty());
		// Open again, the command as it was archives everything.
		assert_eq!(archived(unfence_archive_command(&fenced), &pg_wal), pg_wal);
	}

	#[test]
	fn a_stale_flush_archives_less_and_the_promotion_pushes_what_it_held_back() {
		// The last reading said D1; the standby had really flushed into D2. Whole and before the
		// fork, D1 is then held back too: less, never more.
		let pg_wal = [
			"000000030000001B000000D0",
			"000000030000001B000000D1",
			"000000030000001B000000D2",
			"000000030000001B000000D3",
		];
		let stale = archived(
			&fence_archive_command("echo archived %f", "000000030000001B000000D1"),
			&pg_wal,
		);
		let fresh = archived(
			&fence_archive_command("echo archived %f", "000000030000001B000000D2"),
			&pg_wal,
		);
		assert_eq!(stale, ["000000030000001B000000D0"]);
		assert!(stale.iter().all(|n| fresh.contains(n)));
		assert!(
			!stale.contains(&"000000030000001B000000D1")
				&& fresh.contains(&"000000030000001B000000D1")
		);
		// The promoted standby holds D1 whole (and the fork's segment only as its new timeline's
		// copy and a .partial): its push after the promotion takes D1, and nothing past the fork.
		let promoted = [
			"000000030000001B000000D0",
			"000000030000001B000000D1",
			"000000030000001B000000D2.partial",
			"00000004.history",
			"000000040000001B000000D2",
			"000000040000001B000000D3",
			"000000020000001A000000EB",
		];
		assert_eq!(
			left_timeline_segments("00000004", &promoted),
			["000000030000001B000000D0", "000000030000001B000000D1"]
		);
		assert!(left_timeline_segments("00000001", &["000000000000000000000001"]).is_empty());
		assert!(left_timeline_segments("zz", &promoted).is_empty());
	}

	#[test]
	fn the_promotion_push_runs_the_archive_command_from_the_data_directory() {
		let dir = std::env::temp_dir().join(format!("lepis-push-{}", std::process::id()));
		std::fs::create_dir_all(dir.join("pg_wal")).expect("dir");
		std::fs::write(dir.join("pg_wal/000000030000001B000000D0"), "wal").expect("file");
		let command = "test -f %p && echo %f 100%% >> pushed.txt";
		let script = push_script(
			command,
			&["000000030000001B000000D0", "000000030000001B000000D1"],
		);
		let out = std::process::Command::new("sh")
			.arg("-c")
			.arg(&script)
			.current_dir(&dir)
			.output()
			.expect("sh runs");
		let lines = String::from_utf8_lossy(&out.stdout).to_string();
		assert_eq!(
			lines.lines().collect::<Vec<_>>(),
			[
				"pushed 000000030000001B000000D0",
				"failed 000000030000001B000000D1"
			]
		);
		assert_eq!(
			std::fs::read_to_string(dir.join("pushed.txt"))
				.expect("pushed")
				.trim(),
			"000000030000001B000000D0 100%"
		);
		let _ = std::fs::remove_dir_all(&dir);
	}

	#[test]
	fn a_fence_never_waits_behind_a_call_to_the_witness() {
		assert_eq!(
			in_tick_order(vec![Action::Renew, Action::Fence, Action::FenceArchive]),
			[Action::Fence, Action::FenceArchive, Action::Renew]
		);
		assert_eq!(
			in_tick_order(vec![Action::Renew, Action::Eligible(false)]),
			[Action::Renew, Action::Eligible(false)]
		);
		assert_eq!(
			in_tick_order(vec![Action::Renew, Action::Unfence, Action::UnfenceArchive]),
			[Action::Unfence, Action::UnfenceArchive, Action::Renew]
		);
		assert!(calls_witness(Action::Ask) && !calls_witness(Action::Promote(2)));
	}

	#[test]
	fn a_renewal_waits_no_longer_than_the_lease_it_would_keep() {
		const L: u64 = 10_000;
		// Just renewed: a quarter lease, as before.
		assert_eq!(renew_wait_ms(100, Some(0), L), 2_500);
		// A second of the lease left: a second, so the next decision comes when it runs out.
		assert_eq!(renew_wait_ms(4_000, Some(0), L), 1_000);
		// Run out already: the floor, a twentieth.
		assert_eq!(renew_wait_ms(6_000, Some(0), L), 500);
		// Never renewed: a quarter lease.
		assert_eq!(renew_wait_ms(6_000, None, L), 2_500);
	}

	#[test]
	fn a_lost_stream_counts_from_the_last_message_not_from_the_receiver_giving_up() {
		// Streaming, the last message 300 ms ago.
		assert_eq!(stream_gone(None, None, Some(300), 1_000), (None, Some(700)));
		// The receiver gave up at wal_receiver_timeout: gone since the message at 700.
		assert_eq!(
			stream_gone(None, Some(700), None, 6_000),
			(Some(700), Some(700))
		);
		// And it stays that, reading after reading.
		assert_eq!(
			stream_gone(Some(700), Some(700), None, 9_000),
			(Some(700), Some(700))
		);
		// Back, then gone again: from the newer message.
		assert_eq!(
			stream_gone(Some(700), Some(700), Some(0), 20_000),
			(None, Some(20_000))
		);
		assert_eq!(
			stream_gone(None, Some(20_000), None, 26_000),
			(Some(20_000), Some(20_000))
		);
		// A guard that never saw it stream: from its first reading, as before.
		assert_eq!(stream_gone(None, None, None, 5_000), (Some(5_000), None));
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
