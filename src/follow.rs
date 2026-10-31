//! A router whose home is a set of copies of one database (`LEPIS_HOMES`): it uses the one that is
//! the primary now, and follows a failover with nothing told to it (one router per host, so every
//! service copy reaches the current primary on loopback).
//!
//! The primary is the copy that takes writes: out of recovery and not
//! `default_transaction_read_only`. The second half matters: a guard's fence leaves an old
//! primary out of recovery but read-only (L21), and a router that took "not in recovery" alone
//! would send writes to the one copy that must refuse them. Every copy is asked each
//! `FOLLOW_EVERY`; exactly one taking writes becomes the home, none keeps the last (a failover in
//! flight, or the witness's lease running out), and two keep the current one if it is one of them.
//! A session opened meanwhile on a copy that stopped taking writes is ended by the guard's fence
//! (which terminates every client session) and reconnects to the new home.

use std::sync::Arc;
use std::time::Duration;

use crate::server::App;

/// How often every copy is asked: well inside the guards' lease (10 s), so a promotion is
/// followed within about a second of the new primary taking writes.
pub const FOLLOW_EVERY: Duration = Duration::from_secs(1);

/// How long one copy has to answer: a dead one must not hold up asking the next.
pub const ASK_TIMEOUT: Duration = Duration::from_millis(700);

/// The copies that take writes, by index into the homes, from one round of asking.
async fn writable(app: &App) -> Vec<usize> {
	let mut found = Vec::new();
	for (i, node) in app.config.homes.all().iter().enumerate() {
		if let Ok(Ok(true)) =
			tokio::time::timeout(ASK_TIMEOUT, crate::live::takes_writes(app, node)).await
		{
			found.push(i);
		}
	}
	found
}

/// Which home to use, given the copies that take writes and the one in use: see the module comment.
pub fn choose(writable: &[usize], current: usize) -> usize {
	match writable {
		[] => current,
		[only] => *only,
		many if many.contains(&current) => current,
		many => many[0],
	}
}

/// One round: asks every copy and moves the home if the primary moved. True when it moved.
pub async fn follow_once(app: &App) -> bool {
	let current = app.config.homes.index();
	let found = writable(app).await;
	if found.len() > 1 {
		tracing::warn!(
			?found,
			"more than one copy takes writes; keeping the one in use"
		);
	}
	let next = choose(&found, current);
	if next == current {
		return false;
	}
	let all = app.config.homes.all();
	tracing::warn!(from = %all[current], to = %all[next], "the primary moved; following it");
	app.config.homes.set(next);
	true
}

/// Before the router serves: the home is the copy that takes writes, waiting up to `wait` for
/// one (a router started during a failover). After that it starts on whatever it has.
pub async fn settle(app: &App, wait: Duration) {
	if app.config.homes.all().len() < 2 {
		return;
	}
	let started = tokio::time::Instant::now();
	loop {
		follow_once(app).await;
		if !writable(app).await.is_empty() || started.elapsed() >= wait {
			tracing::info!(home = %app.config.home(), "home chosen among the copies");
			return;
		}
		tokio::time::sleep(FOLLOW_EVERY).await;
	}
}

/// The follower, for as long as the router runs. Nothing for a single home.
pub async fn run(app: Arc<App>) {
	if app.config.homes.all().len() < 2 {
		return;
	}
	let mut every = tokio::time::interval(FOLLOW_EVERY);
	every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
	loop {
		every.tick().await;
		follow_once(&app).await;
	}
}

#[cfg(test)]
mod tests {
	use super::choose;

	#[test]
	fn the_home_is_the_one_copy_that_takes_writes() {
		// Promoted: the standby takes writes, the old primary (down, or fenced read-only) does not.
		assert_eq!(choose(&[1], 0), 1);
		// Nobody takes writes (a failover in flight): stay where it is.
		assert_eq!(choose(&[], 1), 1);
		// Two at once (should never be): keep the one in use if it is one of them.
		assert_eq!(choose(&[0, 1], 1), 1);
		assert_eq!(choose(&[1, 2], 0), 1);
	}
}
