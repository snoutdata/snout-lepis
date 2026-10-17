//! The admin API (L13): the one interface every management surface calls. `snoutdata shards
//! --admin <url>` talks to it for a standalone router; the Cloud's Snout Function, the dashboard
//! and Studio reach the same operations through it. Nothing here decides anything: it plans,
//! writes jobs and reads their state, and the job engine (`ops::engine`) does the work.
//!
//! HTTP/1.1 and JSON on its own port, one request per connection:
//!
//! | Method and path | What |
//! | --- | --- |
//! | `GET /v1/health` | `{"ok": true}`; the only call without a token |
//! | `GET /v1/status` | the cluster: epoch, nodes and their ranges, keyspaces, tables, routers and their acks, the jobs not finished |
//! | `GET /v1/ops` | the operations and the settings they take |
//! | `POST /v1/plan` | `{"op": "range.split", ...}` → the plan: steps, what moves, rows, bytes, copy time, expected pause |
//! | `POST /v1/jobs` | the same body → plans it and writes it as a job: `{"job": id, "plan": {...}}` |
//! | `GET /v1/jobs` | the latest jobs |
//! | `GET /v1/jobs/<id>` | one job with its steps and their progress |
//! | `POST /v1/jobs/<id>/cancel` | stop it (a move before its cutover rolls back) |
//! | `POST /v1/jobs/<id>/resume` | run a failed job again from the step that failed |
//! | `POST /v1/settings` | `{"max_write_pause_ms": 2000, ...}`: the cluster's L10 settings, the advisor's `advice_*` thresholds, and the router's `pool_mode`, `route_claim`, `route_claim_keyspace` (strings; null clears) |
//! | `GET /v1/advice[?sample_ms=N]` | Phase 10's advisor: the facts it read, and each recommendation as a request `POST /v1/jobs` takes, with its reason and its plan. Runs nothing |
//!
//! Configuration, from the environment like everything else: `LEPIS_ADMIN_ADDR` (unset: no admin
//! listener; e.g. `127.0.0.1:7432`) and `LEPIS_ADMIN_TOKEN` (required with it; every call but
//! health sends `Authorization: Bearer <token>`). When the router has a certificate
//! (`LEPIS_TLS_CERT`), the admin port is HTTPS with it; a non-loopback address without one is
//! refused, so the token never crosses a network in the clear.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio_rustls::TlsAcceptor;

use crate::catalog::quote_literal;
use crate::ops::Kind;
use crate::ops::advisor::{self, AdviceSettings};
use crate::ops::engine;
use crate::ops::spec::{OPS, Op};
use crate::ops::steps::node_json;
use crate::ops::{OpError, Pg, Settings, Target, kind_parts, load_catalog, plan};
use crate::scram::constant_time_eq;
use crate::server::App;

const MAX_HEAD: usize = 16 * 1024;
const MAX_BODY: usize = 1024 * 1024;
const READ_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct AdminSettings {
	pub addr: SocketAddr,
	pub token: String,
}

impl AdminSettings {
	/// From `LEPIS_ADMIN_ADDR` and `LEPIS_ADMIN_TOKEN`; None when no address is set.
	pub fn from_env() -> Result<Option<AdminSettings>, String> {
		let get = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
		let Some(addr) = get("LEPIS_ADMIN_ADDR") else {
			return Ok(None);
		};
		let addr: SocketAddr = addr
			.parse()
			.map_err(|_| format!("LEPIS_ADMIN_ADDR: not host:port: {addr}"))?;
		let token = get("LEPIS_ADMIN_TOKEN")
			.ok_or("LEPIS_ADMIN_TOKEN is required with LEPIS_ADMIN_ADDR")?;
		if token.len() < 16 {
			return Err("LEPIS_ADMIN_TOKEN must be at least 16 characters".into());
		}
		Ok(Some(AdminSettings { addr, token }))
	}
}

/// Starts what Phase 4 adds to a router: the job runner, always, and the admin listener when
/// `LEPIS_ADMIN_ADDR` is set. Called once by `server::serve`.
pub fn spawn(app: &Arc<App>) {
	let kick = Arc::new(Notify::new());
	tokio::spawn(engine::run(app.clone(), kick.clone()));
	match AdminSettings::from_env() {
		Ok(None) => {}
		Ok(Some(s)) => {
			let app = app.clone();
			tokio::spawn(async move {
				match TcpListener::bind(s.addr).await {
					Ok(l) => {
						if let Err(e) = serve_admin(app, l, s.token, kick).await {
							tracing::error!("admin API: {e}");
						}
					}
					Err(e) => tracing::error!("admin API: {}: {e}", s.addr),
				}
			});
		}
		Err(e) => tracing::error!("admin API not started: {e}"),
	}
}

/// Serves the admin API on `listener` until it fails. `kick` wakes this router's job runner.
pub async fn serve_admin(
	app: Arc<App>,
	listener: TcpListener,
	token: String,
	kick: Arc<Notify>,
) -> std::io::Result<()> {
	let local = listener.local_addr()?;
	let tls = match (&app.config.tls_cert, &app.config.tls_key) {
		(Some(c), Some(k)) => {
			let (server, _) = crate::tls::server_config(c, k).map_err(std::io::Error::other)?;
			let mut config = (*server).clone();
			config.alpn_protocols = vec![b"http/1.1".to_vec()];
			Some(TlsAcceptor::from(Arc::new(config)))
		}
		_ => None,
	};
	if tls.is_none() && !local.ip().is_loopback() {
		return Err(std::io::Error::other(format!(
			"the admin API on {local} would send its token in the clear: bind it to a loopback address, or give the router a certificate (LEPIS_TLS_CERT)"
		)));
	}
	tracing::info!(%local, tls = tls.is_some(), "snout-lepis admin API listening");
	let token = Arc::new(token);
	loop {
		let (tcp, _) = listener.accept().await?;
		let (app, token, kick, tls) = (app.clone(), token.clone(), kick.clone(), tls.clone());
		tokio::spawn(async move {
			let _ = tcp.set_nodelay(true);
			match tls {
				Some(acceptor) => {
					if let Ok(Ok(s)) =
						tokio::time::timeout(READ_TIMEOUT, acceptor.accept(tcp)).await
					{
						connection(s, &app, &token, &kick).await;
					}
				}
				None => connection(tcp, &app, &token, &kick).await,
			}
		});
	}
}

struct Request {
	method: String,
	path: String,
	authorization: Option<String>,
	body: Vec<u8>,
}

async fn read_request<S: AsyncRead + Unpin>(s: &mut S) -> Result<Request, (u16, String)> {
	let mut buf = Vec::with_capacity(4096);
	let head_end = loop {
		if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
			break i;
		}
		if buf.len() > MAX_HEAD {
			return Err((431, "the request head is too large".into()));
		}
		let mut chunk = [0u8; 4096];
		let n = s.read(&mut chunk).await.map_err(|e| (400, e.to_string()))?;
		if n == 0 {
			return Err((400, "the connection closed mid-request".into()));
		}
		buf.extend_from_slice(&chunk[..n]);
	};
	let head = std::str::from_utf8(&buf[..head_end]).map_err(|_| (400, "not UTF-8".to_string()))?;
	let mut lines = head.split("\r\n");
	let first = lines.next().unwrap_or_default();
	let mut parts = first.split(' ');
	let method = parts.next().unwrap_or_default().to_string();
	let path = parts.next().unwrap_or_default().to_string();
	let mut length = 0usize;
	let mut authorization = None;
	for l in lines {
		let Some((k, v)) = l.split_once(':') else {
			continue;
		};
		let v = v.trim();
		match k.trim().to_ascii_lowercase().as_str() {
			"content-length" => {
				length = v.parse().map_err(|_| (400, "content-length".to_string()))?;
			}
			"authorization" => authorization = Some(v.to_string()),
			"transfer-encoding" => {
				return Err((411, "send a content-length, not a chunked body".into()));
			}
			_ => {}
		}
	}
	if length > MAX_BODY {
		return Err((413, "the body is over 1 MB".into()));
	}
	let mut body = buf[head_end + 4..].to_vec();
	while body.len() < length {
		let mut chunk = vec![0u8; (length - body.len()).min(64 * 1024)];
		let n = s.read(&mut chunk).await.map_err(|e| (400, e.to_string()))?;
		if n == 0 {
			return Err((400, "the connection closed mid-body".into()));
		}
		body.extend_from_slice(&chunk[..n]);
	}
	body.truncate(length);
	Ok(Request {
		method,
		path,
		authorization,
		body,
	})
}

async fn connection<S: AsyncRead + AsyncWrite + Unpin>(
	mut s: S,
	app: &Arc<App>,
	token: &str,
	kick: &Arc<Notify>,
) {
	let (status, body) = match tokio::time::timeout(READ_TIMEOUT, read_request(&mut s)).await {
		Err(_) => (
			408,
			error_body("timeout", "timeout", "the request took too long to arrive"),
		),
		Ok(Err((code, m))) => (code, error_body("bad_request", "bad_http", &m)),
		Ok(Ok(req)) => handle(req, app, token, kick).await,
	};
	let reason = match status {
		200 => "OK",
		201 => "Created",
		400 => "Bad Request",
		401 => "Unauthorized",
		404 => "Not Found",
		409 => "Conflict",
		_ => "Error",
	};
	let text = body.to_string();
	let response = format!(
		"HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\ncache-control: no-store\r\n\r\n{text}",
		text.len()
	);
	let _ = s.write_all(response.as_bytes()).await;
	let _ = s.shutdown().await;
}

/// Every error has the same shape: `code` is the class (`bad_request`, `unauthorized`,
/// `not_found`, `conflict`, `failed`, `timeout`), `kind` the stable reason (`no_such_job`,
/// `not_adjacent`, ...: `ops::Kind`), `message` the sentence for a person.
fn error_body(code: &str, kind: &str, message: &str) -> Value {
	json!({"error": {"code": code, "kind": kind, "message": message}})
}

fn op_error(e: OpError) -> (u16, Value) {
	let (status, code, kind) = match e.kind {
		Some(k) => {
			let s = k.http_status();
			let code = match s {
				404 => "not_found",
				400 => "bad_request",
				_ => "conflict",
			};
			(s, code, k.name())
		}
		None => (500, "failed", "failed"),
	};
	let mut body = error_body(code, kind, &e.message);
	if let Some(c) = &e.code {
		body["error"]["sqlstate"] = json!(c);
	}
	(status, body)
}

async fn handle(req: Request, app: &Arc<App>, token: &str, kick: &Arc<Notify>) -> (u16, Value) {
	let path = req
		.path
		.split('?')
		.next()
		.unwrap_or_default()
		.trim_end_matches('/');
	if req.method == "GET" && path == "/v1/health" {
		return (200, json!({"ok": true}));
	}
	let presented = req
		.authorization
		.as_deref()
		.and_then(|a| a.strip_prefix("Bearer "))
		.unwrap_or_default();
	if !constant_time_eq(presented.as_bytes(), token.as_bytes()) {
		return (
			401,
			error_body(
				"unauthorized",
				"unauthorized",
				"send Authorization: Bearer <LEPIS_ADMIN_TOKEN>",
			),
		);
	}
	let body: Value = if req.body.is_empty() {
		json!({})
	} else {
		match serde_json::from_slice(&req.body) {
			Ok(v) => v,
			Err(e) => {
				return (
					400,
					error_body(
						"bad_request",
						"bad_json",
						&format!("the body is not JSON: {e}"),
					),
				);
			}
		}
	};
	let mut h = match Pg::connect(app, &Target::home(app)).await {
		Ok(h) => h,
		Err(e) => return op_error(e),
	};
	let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
	let result = match (req.method.as_str(), segments.as_slice()) {
		("GET", ["v1", "status"]) => status(&mut h).await.map(|v| (200, v)),
		("GET", ["v1", "ops"]) => Ok((
			200,
			json!({
				"ops": OPS,
				"settings": Settings::default().to_json(),
				"advice_settings": AdviceSettings::default().to_json(),
				"refusals": Kind::ALL.iter().map(|k| json!({"kind": k.name(), "status": k.http_status()})).collect::<Vec<_>>(),
			}),
		)),
		("POST", ["v1", "plan"]) => plan_op(app, &mut h, &body).await.map(|v| (200, v)),
		("POST", ["v1", "jobs"]) => match Op::from_json(&body) {
			Ok(op) => engine::submit(app, &mut h, &op, &body)
				.await
				.map(|(id, plan)| {
					kick.notify_one();
					(201, json!({"job": id, "plan": plan}))
				}),
			Err(e) => Err(e),
		},
		("GET", ["v1", "advice"]) => match query_u64(&req.path, "sample_ms") {
			Ok(sample_ms) => advisor::advise(app, &mut h, sample_ms)
				.await
				.map(|v| (200, v)),
			Err(e) => Err(e),
		},
		("POST", ["v1", "settings"]) => match settings_problem(&body) {
			None => set_settings(&mut h, &body)
				.await
				.map(|()| (200, json!({"settings": body}))),
			Some(why) => Err(OpError::refused(Kind::BadSetting, why)),
		},
		("GET", ["v1", "jobs"]) => engine::list(&mut h, 50)
			.await
			.map(|v| (200, json!({"jobs": v}))),
		("GET", ["v1", "jobs", id]) => match id.parse() {
			Ok(id) => engine::show(&mut h, id).await.map(|v| (200, v)),
			Err(_) => {
				return (
					404,
					error_body("not_found", "no_such_job", "job ids are numbers"),
				);
			}
		},
		("POST", ["v1", "jobs", id, action]) => match (id.parse::<i64>(), *action) {
			(Ok(id), "cancel") => engine::cancel(&mut h, id)
				.await
				.map(|s| (200, json!({"job": id, "state": s}))),
			(Ok(id), "resume") => engine::resume(&mut h, id).await.map(|s| {
				kick.notify_one();
				(200, json!({"job": id, "state": s}))
			}),
			_ => {
				return (
					404,
					error_body(
						"not_found",
						"no_such_route",
						"the actions are cancel and resume",
					),
				);
			}
		},
		_ => {
			return (
				404,
				error_body(
					"not_found",
					"no_such_route",
					&format!("{} {path}", req.method),
				),
			);
		}
	};
	h.close().await;
	result.unwrap_or_else(op_error)
}

/// A whole number from the query string, or None when it is not there.
fn query_u64(path: &str, name: &str) -> Result<Option<u64>, OpError> {
	let Some((_, q)) = path.split_once('?') else {
		return Ok(None);
	};
	for pair in q.split('&') {
		let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
		if k == name {
			return v.parse().map(Some).map_err(|_| {
				OpError::refused(
					Kind::BadSetting,
					format!("{name} is a whole number of milliseconds, not {v:?}"),
				)
			});
		}
	}
	Ok(None)
}

async fn plan_op(app: &Arc<App>, h: &mut Pg, body: &Value) -> Result<Value, OpError> {
	let op = Op::from_json(body)?;
	let cluster: Value = h
		.value("select settings::text from lepis.cluster where id = 1", &[])
		.await?
		.and_then(|v| serde_json::from_str(&v).ok())
		.unwrap_or(Value::Null);
	let settings = Settings::from_json(&cluster, body);
	Ok(plan::plan(app, h, &op, settings).await?.summary)
}

/// The cluster as the catalog and the job log say it is.
pub async fn status(h: &mut Pg) -> Result<Value, OpError> {
	if !engine::ensure_jobs_schema(h).await? {
		return Ok(json!({"catalog": false}));
	}
	let c = load_catalog(h).await?;
	let mut keyspaces: Vec<Value> = c
		.keyspaces
		.values()
		.map(|k| {
			let mut pins: Vec<Value> = k
				.pins
				.iter()
				.map(|(v, n)| json!({"value": v, "node": n.0}))
				.collect();
			pins.sort_by_key(|p| p["value"].to_string());
			json!({
				"name": k.name, "key_type": k.key_type.sql_name(), "seed": k.seed.to_string(),
				"ranges": k.ranges.iter().map(|r| json!({"lo": r.lo.to_string(), "hi": r.hi.to_string(), "node": r.node.0})).collect::<Vec<_>>(),
				"pins": pins,
			})
		})
		.collect();
	keyspaces.sort_by_key(|k| k["name"].to_string());
	let mut tables: Vec<Value> = c
		.relations
		.iter()
		.map(|(n, k)| {
			let (kind, ks, col) = kind_parts(k);
			json!({"table": n.to_string(), "kind": kind, "keyspace": ks, "column": col})
		})
		.collect();
	tables.sort_by_key(|t| t["table"].to_string());
	let routers = h
		.simple(&format!(
			"select coalesce(json_agg(r order by id), '[]')::text from (select id, epoch, epoch >= {} as current, \
			seen_at, now() - seen_at < interval '15 seconds' as live, fenced_epoch from lepis.router) r",
			c.epoch
		))
		.await?;
	let routers: Value = routers
		.first()
		.and_then(|r| r.first().cloned().flatten())
		.and_then(|t| serde_json::from_str(&t).ok())
		.unwrap_or(json!([]));
	let settings: Value = h
		.value("select settings::text from lepis.cluster where id = 1", &[])
		.await?
		.and_then(|v| serde_json::from_str(&v).ok())
		.unwrap_or(json!({}));
	let jobs = h
		.simple(
			"select coalesce(json_agg(j order by id), '[]')::text from (select id, op, state, created_at \
			from lepis.job where state in ('pending', 'running', 'cancelling')) j",
		)
		.await?;
	let jobs: Value = jobs
		.first()
		.and_then(|r| r.first().cloned().flatten())
		.and_then(|t| serde_json::from_str(&t).ok())
		.unwrap_or(json!([]));
	Ok(json!({
		"catalog": true,
		"epoch": c.epoch,
		"settings": Settings::from_json(&settings, &Value::Null).to_json(),
		"advice_settings": AdviceSettings::from_json(&settings).to_json(),
		"router_settings": router_settings_json(&settings),
		"nodes": node_json(&c),
		"keyspaces": keyspaces,
		"tables": tables,
		"routers": routers,
		"jobs": jobs,
	}))
}

/// The router's settings (router.rs `Settings`), as the API shows them.
fn router_settings_json(settings: &Value) -> Value {
	let s = crate::router::Settings::from_json(&settings.to_string());
	json!({
		"pool_mode": if s.transaction_pool { "transaction" } else { "session" },
		"route_claim": s.route_claim,
		"route_claim_keyspace": s.route_claim_keyspace,
	})
}

/// Why a settings patch is refused, as a sentence; None when it may be written. Numbers for the
/// L10 and advisor settings; of the router's, `pool_mode` is `session` or `transaction` and the
/// claim settings are names; null clears any of those three.
fn settings_problem(body: &Value) -> Option<String> {
	let mut numbers = vec![
		"max_write_pause_ms",
		"drain_timeout_ms",
		"ack_timeout_ms",
		"copy_mb_per_s",
	];
	numbers.extend(advisor::SETTING_NAMES);
	let strings = ["pool_mode", "route_claim", "route_claim_keyspace"];
	let usage = || {
		format!(
			"settings are numbers named {}, or {} (strings, or null to clear)",
			numbers.join(", "),
			strings.join(", ")
		)
	};
	let Some(o) = body.as_object().filter(|o| !o.is_empty()) else {
		return Some(usage());
	};
	for (k, v) in o {
		let k = k.as_str();
		if numbers.contains(&k) {
			if !v.is_u64() {
				return Some(format!("{k} is a number"));
			}
			continue;
		}
		if !strings.contains(&k) {
			return Some(usage());
		}
		if v.is_null() {
			continue;
		}
		let Some(t) = v.as_str() else {
			return Some(format!("{k} is a string"));
		};
		match k {
			"pool_mode" if !matches!(t, "session" | "transaction") => {
				return Some("pool_mode is session or transaction".into());
			}
			"route_claim" | "route_claim_keyspace"
				if t.is_empty() || t.len() > 63 || t.chars().any(char::is_control) =>
			{
				return Some(format!("{k} is a name of 1 to 63 characters"));
			}
			_ => {}
		}
	}
	None
}

/// Changes the cluster's L10 settings (`max_write_pause_ms`, `drain_timeout_ms`,
/// `ack_timeout_ms`, `copy_mb_per_s`) and the advisor's (`advisor::SETTING_NAMES`).
pub async fn set_settings(h: &mut Pg, patch: &Value) -> Result<(), OpError> {
	h.simple(&format!(
		"update lepis.cluster set settings = settings || {}::jsonb where id = 1",
		quote_literal(&patch.to_string())
	))
	.await?;
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn router_settings_are_strings_and_the_rest_numbers() {
		for ok in [
			json!({"max_write_pause_ms": 2000}),
			json!({"pool_mode": "transaction"}),
			json!({"pool_mode": "session", "route_claim": "tenant_id", "route_claim_keyspace": "tenant"}),
			json!({"route_claim": null}),
			json!({"drain_timeout_ms": 5, "pool_mode": null}),
		] {
			assert_eq!(settings_problem(&ok), None, "{ok}");
		}
		for bad in [
			json!({}),
			json!([]),
			json!({"pool_mode": "statement"}),
			json!({"pool_mode": 1}),
			json!({"route_claim": ""}),
			json!({"route_claim": "x".repeat(64)}),
			json!({"max_write_pause_ms": "2000"}),
			json!({"no_such": 1}),
		] {
			assert!(settings_problem(&bad).is_some(), "{bad}");
		}
		let shown =
			router_settings_json(&json!({"pool_mode": "transaction", "route_claim": "tenant_id"}));
		assert_eq!(
			shown,
			json!({"pool_mode": "transaction", "route_claim": "tenant_id", "route_claim_keyspace": null})
		);
	}
	use crate::ops::spec::{Step, TransferSpec};
	use crate::ops::{Change, relation_name};

	/// No JSON number the API sends may need more than 53 bits: JavaScript reads them as doubles.
	fn no_wide_numbers(v: &Value, path: &str) {
		match v {
			Value::Number(n) => {
				let ok = n.as_i64().is_some_and(|x| x.unsigned_abs() < 1 << 53)
					|| n.as_u64().is_some_and(|x| x < 1 << 53)
					|| n.as_f64()
						.is_some_and(|x| x.fract() != 0.0 || x.abs() < 9.0e15);
				assert!(ok, "{path} = {n} loses precision in JavaScript");
			}
			Value::Array(a) => a
				.iter()
				.enumerate()
				.for_each(|(i, x)| no_wide_numbers(x, &format!("{path}[{i}]"))),
			Value::Object(o) => o
				.iter()
				.for_each(|(k, x)| no_wide_numbers(x, &format!("{path}.{k}"))),
			_ => {}
		}
	}

	#[test]
	fn sixty_four_bit_values_go_out_as_strings() {
		let changes = [
			Change::RangeOwner {
				keyspace: "k".into(),
				lo: i64::MIN,
				hi: i64::MAX,
				to: crate::catalog::NodeId(2),
			},
			Change::Split {
				keyspace: "k".into(),
				lo: i64::MIN,
				at: -4_611_686_018_427_387_904,
			},
			Change::Merge {
				keyspace: "k".into(),
				a: i64::MIN,
				b: 7_000_000_000_000_000_001,
			},
			Change::Keyspace {
				name: "k".into(),
				key_type: crate::hash::KeyType::Int8,
				seed: u64::MAX / 3,
				ranges: crate::catalog::Keyspace::even_ranges(3, &[crate::catalog::NodeId(1)]),
			},
		];
		for ch in changes {
			let step = Step::Transfer(TransferSpec {
				source: crate::catalog::NodeId(1),
				targets: vec![crate::catalog::NodeId(2)],
				tables: vec![relation_name("app.t").unwrap()],
				change: ch.clone(),
			});
			no_wide_numbers(&step.args(), "args");
			assert_eq!(Step::from_json(step.kind(), &step.args()).unwrap(), step);
		}
	}

	#[test]
	fn refusals_map_to_status_and_kind() {
		let cases = [
			(Kind::NoSuchJob, 404, "not_found", "no_such_job"),
			(Kind::BadRequest, 400, "bad_request", "bad_request"),
			(
				Kind::UnknownOperation,
				400,
				"bad_request",
				"unknown_operation",
			),
			(Kind::NotAdjacent, 409, "conflict", "not_adjacent"),
			(Kind::JobNotFailed, 409, "conflict", "job_not_failed"),
		];
		for (kind, status, code, name) in cases {
			let (s, body) = op_error(OpError::refused(kind, "a sentence"));
			assert_eq!(s, status);
			assert_eq!(body["error"]["code"], code);
			assert_eq!(body["error"]["kind"], name);
			assert_eq!(body["error"]["message"], "a sentence");
		}
		let (s, body) = op_error(OpError::new("the node went away"));
		assert_eq!((s, body["error"]["kind"].as_str()), (500, Some("failed")));
		// Kinds are stable snake_case and unique.
		let mut names: Vec<&str> = Kind::ALL.iter().map(|k| k.name()).collect();
		assert!(
			names
				.iter()
				.all(|n| n.chars().all(|c| c.is_ascii_lowercase() || c == '_'))
		);
		names.sort();
		names.dedup();
		assert_eq!(names.len(), Kind::ALL.len());
	}

	#[test]
	fn the_query_string_is_read_and_checked() {
		assert_eq!(query_u64("/v1/advice", "sample_ms").unwrap(), None);
		assert_eq!(
			query_u64("/v1/advice?sample_ms=250", "sample_ms").unwrap(),
			Some(250)
		);
		assert_eq!(
			query_u64("/v1/advice?x=1&sample_ms=0", "sample_ms").unwrap(),
			Some(0)
		);
		let e = query_u64("/v1/advice?sample_ms=soon", "sample_ms").unwrap_err();
		assert_eq!(op_error(e).0, 400);
	}

	#[test]
	fn bad_operations_are_bad_input() {
		let e = Op::from_json(&json!({"op": "range.teleport"})).unwrap_err();
		assert_eq!(e.kind, Some(Kind::UnknownOperation));
		let e = Op::from_json(&json!({"op": "range.move", "keyspace": "k"})).unwrap_err();
		assert_eq!(e.kind, Some(Kind::BadRequest));
		assert_eq!(op_error(e).0, 400);
	}
}
