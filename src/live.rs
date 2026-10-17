//! The catalog, kept current (L6): read from the home node at start, then again whenever the
//! epoch moves. A router learns of a move by `LISTEN lepis_epoch`; by its heartbeat, which reads
//! the epoch back, within `HEARTBEAT` when a move was made without a notification (a dropped table
//! leaving the catalog inside a two-phase transaction, catalog.sql); and, in case the connection
//! itself broke, by reading the whole catalog every `POLL`.
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;

use crate::backend::{self, BoxStream};
use crate::catalog::{self, Catalog};
use crate::frame::FrameReader;
use crate::scram::ClientCredential;
use crate::server::App;
use crate::wire;

type Conn = FrameReader<BoxStream>;

const POLL: Duration = Duration::from_secs(30);
/// How often the heartbeat is written even when nothing changes: well under the catalog's
/// maximum age, so a router that can reach the home node never comes near it.
const HEARTBEAT: Duration = Duration::from_secs(5);
const RETRY: Duration = Duration::from_secs(2);

/// How old a router's heartbeat may be, by the home node's clock, and still count as a live
/// router: a cutover waits for the routers under it to ack its epoch and for no other
/// (`ops::transfer::wait_acks`).
pub const ROUTER_FRESH: Duration = Duration::from_secs(15);

/// How long a router goes on routing by its catalog after it last heard from the home node,
/// unless `LEPIS_CATALOG_MAX_AGE_MS` says otherwise (`Freshness`).
///
/// The invariant both numbers keep: **any router a cutover does not wait for has already stopped
/// routing.** A heartbeat counts from the moment it was SENT, which is before the home node dates
/// it (`seen_at`), so a router whose heartbeat is older than ROUTER_FRESH last heard from the home
/// more than ROUTER_FRESH ago, and stopped routing ROUTER_FRESH minus the maximum age before the
/// cutover looked. The maximum age is therefore refused at or above ROUTER_FRESH (config.rs).
pub const DEFAULT_CATALOG_MAX_AGE: Duration = Duration::from_secs(10);

/// When this router last heard from the home node, and whether that is too long ago to route by
/// its catalog. A router cut off from the home cannot learn that a range moved: its writes there
/// are refused by the old owner's fence (L7), but its reads would be answered from the old owner's
/// copy, missing what was written on the new owner since. So past the maximum age it refuses
/// statements (router.rs `plan_once`) until the home answers again and its catalog is current.
pub struct Freshness {
	max_age: Duration,
	origin: Instant,
	/// Milliseconds after `origin` of the last word from the home node, plus one; 0 for never.
	heard: AtomicU64,
}

impl Freshness {
	/// `max_age` zero turns the bound off.
	pub fn new(max_age: Duration, origin: Instant) -> Freshness {
		Freshness {
			max_age,
			origin,
			heard: AtomicU64::new(0),
		}
	}

	/// The home node answered something sent at `sent`: the catalog this router holds was current
	/// then. Never moves back.
	pub fn heard(&self, sent: Instant) {
		let ms = sent.saturating_duration_since(self.origin).as_millis() as u64 + 1;
		self.heard.fetch_max(ms, Ordering::Relaxed);
	}

	/// How long ago the home was last heard from, when that is longer than the maximum age (or
	/// it never was); None while the catalog may be routed by.
	pub fn stale_at(&self, now: Instant) -> Option<Duration> {
		if self.max_age.is_zero() {
			return None;
		}
		let heard = self.heard.load(Ordering::Relaxed);
		let since = now.saturating_duration_since(self.origin);
		let age = match heard {
			0 => since,
			ms => since.saturating_sub(Duration::from_millis(ms - 1)),
		};
		(heard == 0 || age > self.max_age).then_some(age)
	}
}

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
	let sent = Instant::now();
	let c = read(&mut b).await?;
	let _ = b.get_mut().write_all(&wire::terminate().encode()).await;
	app.set_catalog(c);
	app.freshness.heard(sent);
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
			notified = heartbeat(app, &mut b, id, epoch).await || wait(&mut b).await?;
			continue;
		}
		last_read = tokio::time::Instant::now();
		let current = app.catalog().map_or(0, |c| c.epoch);
		let sent = Instant::now();
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
		app.freshness.heard(sent);
		// The ack: this router now routes with this epoch.
		if heartbeat(app, &mut b, id, app.catalog().map_or(0, |c| c.epoch)).await {
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

/// Writes this router's heartbeat with the epoch it routes by. Returns whether the catalog moved
/// meanwhile, by a notification or by the epoch the heartbeat reads back being another, so the
/// caller reads the catalog again instead of waiting. A cluster with no catalog has nothing to ack.
///
/// A heartbeat that reads back this router's own epoch is the home node saying the catalog is
/// still current (`Freshness::heard`). One that reads back another is how a router a cutover
/// fenced out (`lepis.router.fenced_epoch`, which is always the epoch the cutover wrote) learns of
/// it: the caller reads the catalog at once, within HEARTBEAT of the cutover.
async fn heartbeat(app: &App, b: &mut Conn, id: &str, epoch: i64) -> bool {
	if epoch == 0 {
		return false;
	}
	let sent = Instant::now();
	let sql = format!(
		"insert into lepis.router (id, epoch, seen_at) values ({}, {epoch}, now()) \
		on conflict (id) do update set epoch = excluded.epoch, seen_at = now() \
		returning (select epoch from lepis.cluster where id = 1)",
		catalog::quote_literal(id)
	);
	let mut notified = false;
	match exchange(b, &sql, &mut notified).await {
		Ok(rows) => {
			let now = rows
				.first()
				.and_then(|r| r.first().cloned().flatten())
				.and_then(|v| v.parse::<i64>().ok());
			let moved = notified || now.is_some_and(|e| e != epoch);
			if !moved && now.is_some() {
				app.freshness.heard(sent);
			}
			moved
		}
		Err(e) => {
			tracing::debug!("router heartbeat: {e}");
			notified
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_catalog_is_routed_by_until_it_is_older_than_the_maximum_age() {
		let t0 = Instant::now();
		let f = Freshness::new(Duration::from_secs(10), t0);
		assert!(
			f.stale_at(t0).is_some(),
			"never heard from the home: not routed by"
		);
		f.heard(t0);
		assert_eq!(f.stale_at(t0 + Duration::from_secs(10)), None);
		assert_eq!(
			f.stale_at(t0 + Duration::from_millis(10_500)),
			Some(Duration::from_millis(10_500))
		);
		// The home answers again: routed by at once.
		f.heard(t0 + Duration::from_secs(12));
		assert_eq!(f.stale_at(t0 + Duration::from_secs(13)), None);
		// An answer to something sent earlier never makes the catalog look older.
		f.heard(t0 + Duration::from_secs(1));
		assert_eq!(f.stale_at(t0 + Duration::from_secs(21)), None);
		assert!(f.stale_at(t0 + Duration::from_secs(23)).is_some());
	}

	#[test]
	fn a_zero_maximum_age_turns_the_bound_off() {
		let t0 = Instant::now();
		let f = Freshness::new(Duration::ZERO, t0);
		assert_eq!(f.stale_at(t0 + Duration::from_secs(3600)), None);
	}

	#[test]
	fn a_router_the_cutover_does_not_wait_for_has_stopped_routing() {
		// The heartbeat is the router's last word to the home; the cutover stops waiting for it
		// ROUTER_FRESH after the home dated it, which is after it was sent.
		assert!(DEFAULT_CATALOG_MAX_AGE < ROUTER_FRESH);
		assert!(HEARTBEAT < DEFAULT_CATALOG_MAX_AGE);
		let sent = Instant::now();
		let f = Freshness::new(DEFAULT_CATALOG_MAX_AGE, sent);
		f.heard(sent);
		assert!(f.stale_at(sent + ROUTER_FRESH).is_some());
	}
}
