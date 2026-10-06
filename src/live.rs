//! The catalog, kept current (L6): read from the home node at start, then again whenever the
//! epoch moves. A router learns of a move by `LISTEN lepis_epoch` and, in case a notification is
//! lost with a broken connection, by reading the epoch every `POLL`.
//!
//! Each router also keeps a heartbeat row in `lepis.router` with the epoch it has loaded: its ack.
//! A cutover (L10, `ops::transfer`) waits for every live router to ack the new epoch before the
//! source lets writes through again; one that does not is named there, and the node fences keep
//! its writes off the old owner (L7).
//!
//! The catalog lives in the database Lepis's service login uses (`LEPIS_SERVICE_DATABASE`), and
//! that database is the cluster. A cluster without a `lepis` schema is empty: every session is
//! the Phase 0 pass-through.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;

use crate::backend::{self, BoxStream};
use crate::catalog::{self, Catalog};
use crate::frame::FrameReader;
use crate::scram::ClientCredential;
use crate::server::App;
use crate::wire;

type Conn = FrameReader<BoxStream>;

const POLL: Duration = Duration::from_secs(30);
/// How often the heartbeat is written even when nothing changes. A cutover counts a router as
/// live while its heartbeat is under 15 s old.
const HEARTBEAT: Duration = Duration::from_secs(5);
const RETRY: Duration = Duration::from_secs(2);

async fn service_conn(app: &App) -> Result<Conn, String> {
	let s = &app.config.service;
	let params = vec![
		("user".to_string(), s.user.clone()),
		("database".to_string(), s.database.clone()),
		(
			"application_name".to_string(),
			"snout-lepis catalog".to_string(),
		),
	];
	backend::connect(
		&app.config.home,
		app.node_tls.as_ref(),
		&params,
		ClientCredential::Password(s.password.clone()),
	)
	.await
	.map(|b| FrameReader::new(b.stream))
	.map_err(|e| format!("service login to {}: {e}", app.config.home))
}

/// One simple-protocol statement and its rows as text. Notifications that arrive meanwhile are
/// dropped: the caller reads the epoch itself right after.
async fn one(b: &mut Conn, sql: &str) -> Result<Vec<Vec<Option<String>>>, String> {
	let mut notified = false;
	exchange(b, sql, &mut notified).await
}

/// `one`, noting whether a notification arrived while it ran.
async fn exchange(
	b: &mut Conn,
	sql: &str,
	notified: &mut bool,
) -> Result<Vec<Vec<Option<String>>>, String> {
	let w = b.get_mut();
	w.write_all(&wire::query(sql).encode())
		.await
		.map_err(|e| e.to_string())?;
	w.flush().await.map_err(|e| e.to_string())?;
	let mut rows = Vec::new();
	let mut error = None;
	loop {
		let m = b.next().await.map_err(|e| e.to_string())?;
		match m.tag {
			b'D' => rows.push(
				wire::parse_data_row(&m.body)
					.map_err(|e| e.to_string())?
					.into_iter()
					.map(|c| c.map(|v| String::from_utf8_lossy(&v).into_owned()))
					.collect(),
			),
			b'E' => {
				let f = wire::parse_error_fields(&m.body);
				error = f.into_iter().find(|(k, _)| *k == b'M').map(|(_, v)| v);
			}
			b'A' => *notified = true,
			b'Z' => break,
			_ => {}
		}
	}
	match error {
		Some(e) => Err(format!("{sql}: {e}")),
		None => Ok(rows),
	}
}

/// Reads the whole catalog, or None when the cluster has none.
pub async fn read(b: &mut Conn) -> Result<Option<Catalog>, String> {
	let exists = one(b, "select to_regclass('lepis.cluster') is not null").await?;
	if exists
		.first()
		.and_then(|r| r.first().cloned().flatten())
		.as_deref()
		!= Some("t")
	{
		return Ok(None);
	}
	// One snapshot: the six reads see the same catalog.
	one(b, "begin isolation level repeatable read read only").await?;
	let result = async {
		let epoch = one(b, catalog::LOAD_EPOCH)
			.await?
			.first()
			.and_then(|r| r.first().cloned().flatten())
			.and_then(|v| v.parse().ok())
			.ok_or("the catalog has no epoch")?;
		let nodes = one(b, catalog::LOAD_NODES).await?;
		let keyspaces = one(b, catalog::LOAD_KEYSPACES).await?;
		let ranges = one(b, catalog::LOAD_RANGES).await?;
		let pins = one(b, catalog::LOAD_PINS).await?;
		let relations = one(b, catalog::LOAD_RELATIONS).await?;
		Catalog::from_rows(epoch, &nodes, &keyspaces, &ranges, &pins, &relations)
			.map_err(|e| format!("the catalog is not usable: {e}"))
	}
	.await;
	let _ = one(b, "rollback").await;
	result.map(Some)
}

/// Loads the catalog once, before the first client is accepted. A catalog that cannot be read
/// stops the router: serving without it would send rows to the wrong node.
pub async fn load(app: &Arc<App>) -> Result<(), String> {
	let mut b = service_conn(app).await?;
	let c = read(&mut b).await?;
	let _ = b.get_mut().write_all(&wire::terminate().encode()).await;
	app.set_catalog(c);
	Ok(())
}

/// Keeps the catalog current for as long as the router runs.
pub async fn watch(app: Arc<App>) {
	let id = crate::ops::engine::runner_id();
	loop {
		if let Err(e) = watch_once(&app, &id).await {
			tracing::warn!("catalog watch: {e}; reconnecting");
		}
		tokio::time::sleep(RETRY).await;
	}
}

async fn watch_once(app: &Arc<App>, id: &str) -> Result<(), String> {
	let mut b = service_conn(app).await?;
	one(&mut b, "listen lepis_epoch").await?;
	let mut last_read = tokio::time::Instant::now();
	let mut notified = true;
	loop {
		if !notified && last_read.elapsed() < POLL {
			let epoch = app.catalog().map_or(0, |c| c.epoch);
			notified = heartbeat(&mut b, id, epoch).await || wait(&mut b).await?;
			continue;
		}
		last_read = tokio::time::Instant::now();
		let current = app.catalog().map_or(0, |c| c.epoch);
		let fresh = read(&mut b).await?;
		let moved = fresh.as_ref().map_or(0, |c| c.epoch) != current
			|| fresh.is_some() != app.catalog().is_some();
		if moved {
			tracing::info!(
				epoch = fresh.as_ref().map_or(0, |c| c.epoch),
				"catalog reloaded"
			);
			app.set_catalog(fresh);
		}
		// The ack: this router now routes with this epoch.
		if heartbeat(&mut b, id, app.catalog().map_or(0, |c| c.epoch)).await {
			continue;
		}
		notified = wait(&mut b).await?;
	}
}

/// Waits for a notification (true) or the heartbeat interval (false).
async fn wait(b: &mut Conn) -> Result<bool, String> {
	match tokio::time::timeout(HEARTBEAT, b.next()).await {
		Ok(Ok(m)) if m.tag == b'A' => Ok(true),
		Ok(Ok(m)) if matches!(m.tag, b'N' | b'S') => Ok(false),
		Ok(Ok(m)) => Err(format!(
			"unexpected '{}' on the catalog connection",
			char::from(m.tag)
		)),
		Ok(Err(e)) => Err(e.to_string()),
		Err(_) => Ok(false),
	}
}

/// Writes this router's heartbeat with the epoch it routes by. Returns whether a notification
/// arrived meanwhile, so the caller reads the catalog again instead of waiting. A cluster with no
/// catalog has nothing to ack.
async fn heartbeat(b: &mut Conn, id: &str, epoch: i64) -> bool {
	if epoch == 0 {
		return false;
	}
	let sql = format!(
		"insert into lepis.router (id, epoch, seen_at) values ({}, {epoch}, now()) \
		on conflict (id) do update set epoch = excluded.epoch, seen_at = now()",
		catalog::quote_literal(id)
	);
	let mut notified = false;
	if let Err(e) = exchange(b, &sql, &mut notified).await {
		tracing::debug!("router heartbeat: {e}");
	}
	notified
}
