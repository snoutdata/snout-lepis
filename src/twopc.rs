//! Two-phase commit across nodes (L9): a write that touched several nodes commits on all of them
//! or on none, with the decision logged on the home node.
//!
//! A transaction with more than one participant commits in four steps:
//!
//! 1. `PREPARE TRANSACTION 'lepis:<cluster>:<txid>:<node>'` on every participant, in parallel, on
//!    the participant's own session. Any refusal, and the transaction is rolled back everywhere.
//! 2. The DECISION: one row in `lepis.prepared` on the home node, written by Lepis's service
//!    login. That row is the commit point. Before it, a crash means rollback; after it, commit.
//! 3. `COMMIT PREPARED` on every participant, again on its own session (the same role that
//!    prepared, so no privilege beyond the client's is needed).
//! 4. The row is deleted.
//!
//! The decision row is a register written once (its primary key): the coordinator writes
//! `commit`, in-doubt recovery writes `abort` for a prepared transaction that has waited past its
//! grace with no decision, and whichever insert lands first is what happens; the other side reads
//! it back and obeys. So a coordinator that stalls and a recovery that loses patience cannot
//! decide differently, whatever the clocks say. An `abort` row is kept for a day so the register
//! is never found empty again by a coordinator that was stalled past the grace.
//!
//! In-doubt recovery (`spawn_recovery`) runs in every router and does work in one at a time: the
//! one holding a session advisory lock on the home node. It reads the log FIRST and the nodes'
//! `pg_prepared_xacts` second, which is what makes deleting a finished `commit` row safe: every
//! row it read was written after all of that transaction's PREPAREs, so any of them still
//! pending is in the scan that follows.
//!
//! One participant needs none of this: a plain COMMIT is atomic already.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::task::Poll;
use std::time::{Duration, Instant};

use crate::backend::{self, Backend, BackendError};
use crate::catalog::{Catalog, Node, NodeId, NodeState, quote_literal};
use crate::config::{NodeAddress, ServiceLogin};
use crate::scram::ClientCredential;
use crate::server::App;
use crate::wire::{ErrorFields, Message};

/// The timings two-phase commit and its recovery work to.
#[derive(Clone, Copy, Debug)]
pub struct Settings {
	/// How long a prepared transaction may wait for a decision before recovery rolls it back. A
	/// coordinator that has not decided by half of this rolls back itself.
	pub abort_grace: Duration,
	/// How long recovery leaves a decided transaction to its coordinator before finishing it.
	pub commit_grace: Duration,
	/// How long an `abort` decision is kept.
	pub abort_retain: Duration,
	/// How often the recovery loop looks.
	pub interval: Duration,
}

impl Default for Settings {
	fn default() -> Self {
		Settings {
			abort_grace: Duration::from_secs(30),
			commit_grace: Duration::from_secs(5),
			abort_retain: Duration::from_secs(24 * 3600),
			interval: Duration::from_secs(5),
		}
	}
}

/// The session advisory lock (on the home node's cluster database) whose holder runs recovery:
/// "lepis2pc" in ASCII.
pub const LEADER_LOCK: i64 = 0x6c65_7069_7332_7063;

/// The leader syncs pg_cron copies every this many passes (cron.rs).
const CRON_EVERY: u64 = 6;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
	Commit,
	Abort,
}

impl Decision {
	fn as_str(self) -> &'static str {
		match self {
			Decision::Commit => "commit",
			Decision::Abort => "abort",
		}
	}
}

/// One node's side of a distributed transaction: a session with a transaction open on it.
/// `NodeSession` is one over a `Backend`; the router implements it over its own per-node
/// connections.
pub trait Participant: Send {
	fn node(&self) -> NodeId;
	/// Runs one statement Lepis wrote (BEGIN, PREPARE TRANSACTION, COMMIT PREPARED, …) and returns
	/// its command tag, or the node's refusal.
	fn execute(&mut self, sql: &str) -> impl Future<Output = Result<String, BackendError>> + Send;
}

/// A participant over a plain `Backend`.
pub struct NodeSession {
	pub node: NodeId,
	pub backend: Backend,
}

impl Participant for NodeSession {
	fn node(&self) -> NodeId {
		self.node
	}

	async fn execute(&mut self, sql: &str) -> Result<String, BackendError> {
		let mut r = self.backend.simple(sql).await?;
		Ok(r.tags.pop().unwrap_or_default())
	}
}

/// What a commit did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outcome {
	/// The two-phase transaction id; None when one participant committed plainly.
	pub txid: Option<i64>,
	/// Participants whose COMMIT PREPARED did not go through. The transaction IS committed (the
	/// decision is logged); recovery finishes these.
	pub unresolved: Vec<NodeId>,
}

#[derive(Debug)]
pub enum TwoPcError {
	/// Rolled back everywhere (anything left prepared is rolled back by recovery). `error` is a
	/// node's own refusal when there was one, which the client should see as it is.
	Aborted {
		code: &'static str,
		reason: String,
		node: Option<NodeId>,
		error: Option<Message>,
	},
	/// The decision could not be written or read back: recovery will commit or roll back, and
	/// nobody can say which yet.
	InDoubt { txid: i64, reason: String },
	/// A test stopped the coordinator here (`Coordinator::commit_until`), as a crash would.
	#[doc(hidden)]
	Stopped,
}

impl std::fmt::Display for TwoPcError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			TwoPcError::Aborted { reason, .. } => write!(f, "rolled back: {reason}"),
			TwoPcError::InDoubt { txid, reason } => {
				write!(f, "transaction {txid} is in doubt: {reason}")
			}
			TwoPcError::Stopped => f.write_str("stopped"),
		}
	}
}

impl TwoPcError {
	/// The ErrorResponse the client is sent.
	pub fn to_message(&self) -> Message {
		match self {
			TwoPcError::Aborted {
				error: Some(m), ..
			} => m.clone(),
			TwoPcError::Aborted { code, reason, .. } => {
				ErrorFields::error(code, format!("the transaction was rolled back: {reason}"))
					.message()
			}
			TwoPcError::InDoubt { txid, reason } => ErrorFields::error(
				"08007",
				format!("the outcome of the transaction across nodes is not known yet: {reason}"),
			)
			.with_hint(format!(
				"Lepis's in-doubt recovery commits or rolls back two-phase transaction {txid} within a minute; check before retrying."
			))
			.message(),
			TwoPcError::Stopped => ErrorFields::error("XX000", "stopped").message(),
		}
	}
}

/// Where a test may stop the coordinator, as a crash would.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stop {
	/// After the first `n` participants prepared; the rest were never asked.
	AfterPrepared(usize),
	/// After the decision was logged.
	AfterDecision,
	/// After the first `n` participants committed.
	AfterCommitted(usize),
}

/// The gid of one participant's prepared transaction.
pub fn gid(cluster: &str, txid: i64, node: NodeId) -> String {
	format!("lepis:{cluster}:{txid}:{}", node.0)
}

/// The txid and node of one of `cluster`'s gids; None for anyone else's.
pub fn parse_gid(cluster: &str, gid: &str) -> Option<(i64, i32)> {
	let rest = gid
		.strip_prefix("lepis:")?
		.strip_prefix(cluster)?
		.strip_prefix(':')?;
	let (txid, node) = rest.split_once(':')?;
	Some((txid.parse().ok()?, node.parse().ok()?))
}

/// A fresh transaction id: 63 random bits, positive. Routers never coordinate ids, so a
/// collision is possible in principle; it can only abort (the register is taken), never commit
/// the wrong thing.
fn new_txid() -> i64 {
	let mut b = [0u8; 8];
	getrandom::fill(&mut b).expect("the OS random source");
	(u64::from_be_bytes(b) >> 1).max(1) as i64
}

/// Polls every future to completion, together, on this task.
pub(crate) async fn join_all<F: Future>(futures: Vec<F>) -> Vec<F::Output> {
	let mut futures: Vec<Pin<Box<F>>> = futures.into_iter().map(Box::pin).collect();
	let mut out: Vec<Option<F::Output>> = futures.iter().map(|_| None).collect();
	std::future::poll_fn(|cx| {
		let mut pending = false;
		for (f, o) in futures.iter_mut().zip(out.iter_mut()) {
			if o.is_none() {
				match f.as_mut().poll(cx) {
					Poll::Ready(v) => *o = Some(v),
					Poll::Pending => pending = true,
				}
			}
		}
		if pending {
			Poll::Pending
		} else {
			Poll::Ready(())
		}
	})
	.await;
	out.into_iter()
		.map(|o| o.expect("polled to completion"))
		.collect()
}

/// The nodes a write may reach: `active` and `draining` ones. A `joining` node is not written
/// through the router (a node add fills it itself, and a physical standby replays its source and
/// would refuse), so 2PC participants, DDL fan-out and role sync leave it out.
pub fn writable_nodes(catalog: &Catalog) -> Vec<NodeId> {
	catalog
		.nodes
		.values()
		.filter(|n| matches!(n.state, NodeState::Active | NodeState::Draining))
		.map(|n| n.id)
		.collect()
}

/// Whether a node is a standby still in recovery (`pg_is_in_recovery()`), which takes no writes.
pub async fn is_standby(b: &mut Backend) -> Result<bool, BackendError> {
	Ok(b.query("select pg_is_in_recovery()", &[])
		.await?
		.first()
		.and_then(|r| r.first().cloned().flatten())
		.as_deref()
		== Some("t"))
}

// ---------------------------------------------------------------------------------------------
// Lepis's own connections.

/// How Lepis reaches a node in the catalog.
pub fn node_address(app: &App, node: &Node) -> NodeAddress {
	NodeAddress {
		host: node.host.clone(),
		port: node.port,
		sslmode: node.sslmode,
		ca_file: app.config.home.ca_file.clone(),
	}
}

async fn service_connect(
	address: &NodeAddress,
	tls: Option<&Arc<rustls::ClientConfig>>,
	service: &ServiceLogin,
	database: &str,
	purpose: &str,
) -> Result<Backend, BackendError> {
	let params = vec![
		("user".to_string(), service.user.clone()),
		("database".to_string(), database.to_string()),
		(
			"application_name".to_string(),
			format!("snout-lepis {purpose}"),
		),
		// The decision must be on disk before anything acts on it, whatever the database says.
		("synchronous_commit".to_string(), "on".to_string()),
	];
	backend::connect(
		address,
		tls,
		&params,
		ClientCredential::Password(service.password.clone()),
	)
	.await
}

/// Lepis's service login on the home node's cluster database (where `lepis.*` lives).
pub async fn home_service(app: &App, purpose: &str) -> Result<Backend, BackendError> {
	service_connect(
		&app.config.home,
		app.node_tls.as_ref(),
		&app.config.service,
		&app.config.service.database,
		purpose,
	)
	.await
}

/// Lepis's service login on one node of the catalog, in the node's database. The service role
/// must exist with the same password on every node (it is a superuser, so it can finish any
/// role's prepared transaction and sync roles).
pub async fn node_service(app: &App, node: &Node, purpose: &str) -> Result<Backend, BackendError> {
	let address = node_address(app, node);
	let tls = crate::tls::client_config(&address).map_err(BackendError::Unsupported)?;
	service_connect(
		&address,
		tls.as_ref(),
		&app.config.service,
		&node.dbname,
		purpose,
	)
	.await
}

// ---------------------------------------------------------------------------------------------
// The decision log.

async fn read_uid(log: &mut Backend) -> Result<String, BackendError> {
	log.query("select uid from lepis.cluster where id = 1", &[])
		.await?
		.into_iter()
		.next()
		.and_then(|r| r.into_iter().next().flatten())
		.ok_or_else(|| BackendError::Unsupported("lepis.cluster has no row".into()))
}

/// Writes `decision` for `txid` unless one is there already, and returns the one that holds.
pub async fn decide(
	log: &mut Backend,
	txid: i64,
	nodes: &[NodeId],
	decision: Decision,
) -> Result<Decision, BackendError> {
	let id = txid.to_string();
	let nodes = format!(
		"{{{}}}",
		nodes
			.iter()
			.map(|n| n.0.to_string())
			.collect::<Vec<_>>()
			.join(",")
	);
	let rows = log
		.query(
			"with ins as (insert into lepis.prepared (txid, decision, nodes) \
			values ($1::bigint, $2, $3::int[]) on conflict (txid) do nothing returning decision) \
			select decision from ins union all \
			select decision from lepis.prepared where txid = $1::bigint",
			&[&id, decision.as_str(), &nodes],
		)
		.await?;
	let mut held = rows
		.into_iter()
		.next()
		.and_then(|r| r.into_iter().next().flatten());
	if held.is_none() {
		// Another writer's row committed after this statement's snapshot: read it now.
		held = log
			.query(
				"select decision from lepis.prepared where txid = $1::bigint",
				&[&id],
			)
			.await?
			.into_iter()
			.next()
			.and_then(|r| r.into_iter().next().flatten());
	}
	match held.as_deref() {
		Some("commit") => Ok(Decision::Commit),
		Some("abort") => Ok(Decision::Abort),
		_ => Err(BackendError::Unsupported(format!(
			"no decision for transaction {txid} after writing one"
		))),
	}
}

// ---------------------------------------------------------------------------------------------
// The coordinator.

/// Commits transactions across nodes. One per cluster (`for_app`); it holds a few idle service
/// connections to the home node for the log.
pub struct Coordinator {
	home: NodeAddress,
	tls: Option<Arc<rustls::ClientConfig>>,
	service: ServiceLogin,
	settings: Settings,
	uid: tokio::sync::OnceCell<String>,
	idle: tokio::sync::Mutex<Vec<Backend>>,
}

const IDLE_LOG_CONNECTIONS: usize = 4;

impl Coordinator {
	pub fn new(app: &App, settings: Settings) -> Coordinator {
		Coordinator {
			home: app.config.home.clone(),
			tls: app.node_tls.clone(),
			service: app.config.service.clone(),
			settings,
			uid: tokio::sync::OnceCell::new(),
			idle: tokio::sync::Mutex::new(Vec::new()),
		}
	}

	/// The cluster's coordinator, made once per home node and service login in this process.
	pub fn for_app(app: &App) -> Arc<Coordinator> {
		static ALL: OnceLock<StdMutex<HashMap<String, Arc<Coordinator>>>> = OnceLock::new();
		let key = format!(
			"{}/{}/{}",
			app.config.home, app.config.service.database, app.config.service.user
		);
		let mut all = ALL
			.get_or_init(Default::default)
			.lock()
			.expect("coordinator registry");
		all.entry(key)
			.or_insert_with(|| Arc::new(Coordinator::new(app, Settings::default())))
			.clone()
	}

	pub fn settings(&self) -> &Settings {
		&self.settings
	}

	async fn log(&self) -> Result<Backend, BackendError> {
		if let Some(b) = self.idle.lock().await.pop() {
			return Ok(b);
		}
		service_connect(
			&self.home,
			self.tls.as_ref(),
			&self.service,
			&self.service.database,
			"2pc",
		)
		.await
	}

	async fn give_back(&self, b: Backend) {
		let mut idle = self.idle.lock().await;
		if idle.len() < IDLE_LOG_CONNECTIONS {
			idle.push(b);
		}
	}

	/// The cluster's uid, read once.
	pub async fn uid(&self) -> Result<String, BackendError> {
		self.uid
			.get_or_try_init(|| async {
				let mut b = self.log().await?;
				let uid = read_uid(&mut b).await?;
				self.give_back(b).await;
				Ok(uid)
			})
			.await
			.cloned()
	}

	/// Writes a decision through a pooled log connection.
	async fn log_decide(
		&self,
		txid: i64,
		nodes: &[NodeId],
		d: Decision,
	) -> Result<Decision, BackendError> {
		let mut b = self.log().await?;
		let r = decide(&mut b, txid, nodes, d).await;
		if r.is_ok() {
			self.give_back(b).await;
		}
		r
	}

	async fn forget(&self, txid: i64) {
		if let Ok(mut b) = self.log().await
			&& b.query(
				"delete from lepis.prepared where txid = $1::bigint and decision = 'commit'",
				&[&txid.to_string()],
			)
			.await
			.is_ok()
		{
			self.give_back(b).await;
		}
	}

	/// Commits the open transaction of every participant, all or none. On error the
	/// participants' sessions are outside any transaction (or broken).
	pub async fn commit<P: Participant>(&self, ps: &mut [P]) -> Result<Outcome, TwoPcError> {
		self.run(ps, None).await
	}

	/// `commit`, stopped where a crash would stop it. Sessions are left as they are; a test drops
	/// them, as a dead router's would be.
	#[doc(hidden)]
	pub async fn commit_until<P: Participant>(
		&self,
		ps: &mut [P],
		stop: Stop,
	) -> Result<Outcome, TwoPcError> {
		self.run(ps, Some(stop)).await
	}

	async fn run<P: Participant>(
		&self,
		ps: &mut [P],
		stop: Option<Stop>,
	) -> Result<Outcome, TwoPcError> {
		if ps.is_empty() {
			return Ok(Outcome {
				txid: None,
				unresolved: Vec::new(),
			});
		}
		if ps.len() == 1 && stop.is_none() {
			return commit_one(&mut ps[0]).await;
		}
		let uid = match self.uid().await {
			Ok(u) => u,
			Err(e) => {
				rollback_all(ps).await;
				return Err(TwoPcError::Aborted {
					code: "58000",
					reason: format!("the two-phase commit log on the home node is not usable: {e}"),
					node: None,
					error: None,
				});
			}
		};
		let txid = new_txid();
		let started = Instant::now();
		let nodes: Vec<NodeId> = ps.iter().map(|p| p.node()).collect();
		let gids: Vec<String> = nodes.iter().map(|n| gid(&uid, txid, *n)).collect();

		// 1. Prepare.
		let upto = match stop {
			Some(Stop::AfterPrepared(k)) => k.min(ps.len()),
			_ => ps.len(),
		};
		let results = join_all(
			ps[..upto]
				.iter_mut()
				.zip(&gids)
				.map(|(p, g)| {
					let sql = format!("prepare transaction {}", quote_literal(g));
					async move { p.execute(&sql).await }
				})
				.collect(),
		)
		.await;
		if matches!(stop, Some(Stop::AfterPrepared(_))) {
			return Err(TwoPcError::Stopped);
		}
		let mut prepared = vec![false; ps.len()];
		let mut failure: Option<(NodeId, BackendError)> = None;
		let mut unknown = false;
		for (i, r) in results.into_iter().enumerate() {
			match r {
				Ok(tag) if tag == "PREPARE TRANSACTION" => prepared[i] = true,
				Ok(tag) => {
					// An aborted transaction answers PREPARE with ROLLBACK.
					failure.get_or_insert((
						nodes[i],
						BackendError::Unsupported(format!(
							"{} answered {tag}: its part of the transaction had already failed",
							nodes[i]
						)),
					));
				}
				Err(e) => {
					unknown |= matches!(e, BackendError::Unreachable(_));
					failure.get_or_insert((nodes[i], e));
				}
			}
		}
		if failure.is_none() && started.elapsed() > self.settings.abort_grace / 2 {
			failure = Some((
				nodes[0],
				BackendError::Unsupported(
					"preparing took longer than half the in-doubt grace".into(),
				),
			));
		}
		if let Some((node, e)) = failure {
			if unknown {
				// A PREPARE whose answer was lost may have happened: decide now so recovery
				// rolls it back without waiting out the grace.
				let _ = self.log_decide(txid, &nodes, Decision::Abort).await;
			}
			self.abort(ps, &gids, &prepared).await;
			return Err(match e {
				BackendError::Refused(m) => TwoPcError::Aborted {
					code: "40001",
					reason: format!("{node} refused to prepare"),
					node: Some(node),
					error: Some(m),
				},
				e => TwoPcError::Aborted {
					code: "40001",
					reason: format!("{node}: {e}"),
					node: Some(node),
					error: None,
				},
			});
		}

		// 2. Decide.
		match self.log_decide(txid, &nodes, Decision::Commit).await {
			Ok(Decision::Commit) => {}
			Ok(Decision::Abort) => {
				self.abort(ps, &gids, &prepared).await;
				return Err(TwoPcError::Aborted {
					code: "40001",
					reason: "in-doubt recovery rolled it back first; retry it".into(),
					node: None,
					error: None,
				});
			}
			Err(e) => {
				return Err(TwoPcError::InDoubt {
					txid,
					reason: format!("writing the decision: {e}"),
				});
			}
		}
		if stop == Some(Stop::AfterDecision) {
			return Err(TwoPcError::Stopped);
		}

		// 3. Commit.
		let upto = match stop {
			Some(Stop::AfterCommitted(k)) => k.min(ps.len()),
			_ => ps.len(),
		};
		let results = join_all(
			ps[..upto]
				.iter_mut()
				.zip(&gids)
				.map(|(p, g)| {
					let sql = format!("commit prepared {}", quote_literal(g));
					async move { p.execute(&sql).await }
				})
				.collect(),
		)
		.await;
		if matches!(stop, Some(Stop::AfterCommitted(_))) {
			return Err(TwoPcError::Stopped);
		}
		let unresolved: Vec<NodeId> = results
			.iter()
			.zip(&nodes)
			.filter(|(r, _)| r.is_err())
			.map(|(_, n)| *n)
			.collect();

		// 4. Forget.
		if unresolved.is_empty() {
			self.forget(txid).await;
		}
		Ok(Outcome {
			txid: Some(txid),
			unresolved,
		})
	}

	/// Rolls back every participant: prepared ones by gid, the rest plainly. Failures are left to
	/// recovery (a prepared transaction outlives its session; an open one does not).
	async fn abort<P: Participant>(&self, ps: &mut [P], gids: &[String], prepared: &[bool]) {
		join_all(
			ps.iter_mut()
				.zip(gids)
				.zip(prepared)
				.map(|((p, g), was)| {
					let sql = if *was {
						format!("rollback prepared {}", quote_literal(g))
					} else {
						"rollback".to_string()
					};
					async move {
						let _ = p.execute(&sql).await;
					}
				})
				.collect(),
		)
		.await;
	}

	/// Runs `sql` in one transaction on every participant and commits them together. The
	/// participants must be outside a transaction. With `same_count`, the statement's command
	/// tag (its row count) must be the same on every node, as it must be for a write to a
	/// reference table, whose copies are identical; a difference rolls everything back.
	pub async fn run_everywhere<P: Participant>(
		&self,
		ps: &mut [P],
		sql: &str,
		same_count: bool,
	) -> Result<Outcome, TwoPcError> {
		let batch = format!("begin;\n{sql}");
		let results = join_all(
			ps.iter_mut()
				.map(|p| {
					let batch = batch.clone();
					async move { p.execute(&batch).await }
				})
				.collect(),
		)
		.await;
		let nodes: Vec<NodeId> = ps.iter().map(|p| p.node()).collect();
		let mut failure = None;
		let mut tags = Vec::new();
		for (r, n) in results.into_iter().zip(&nodes) {
			match r {
				Ok(t) => tags.push((*n, t)),
				Err(e) => {
					failure.get_or_insert((*n, e));
				}
			}
		}
		if failure.is_none() && same_count && tags.windows(2).any(|w| w[0].1 != w[1].1) {
			rollback_all(ps).await;
			return Err(TwoPcError::Aborted {
				code: "XX001",
				reason: format!(
					"the copies of a reference table differ: {}",
					tags.iter()
						.map(|(n, t)| format!("{n} answered {t}"))
						.collect::<Vec<_>>()
						.join(", ")
				),
				node: None,
				error: None,
			});
		}
		if let Some((node, e)) = failure {
			rollback_all(ps).await;
			return Err(match e {
				BackendError::Refused(m) => TwoPcError::Aborted {
					code: "XX000",
					reason: format!("{node} refused"),
					node: Some(node),
					error: Some(m),
				},
				e => TwoPcError::Aborted {
					code: "08006",
					reason: format!("{node}: {e}"),
					node: Some(node),
					error: None,
				},
			});
		}
		self.commit(ps).await
	}
}

async fn rollback_all<P: Participant>(ps: &mut [P]) {
	join_all(
		ps.iter_mut()
			.map(|p| async move {
				let _ = p.execute("rollback").await;
			})
			.collect(),
	)
	.await;
}

async fn commit_one<P: Participant>(p: &mut P) -> Result<Outcome, TwoPcError> {
	let node = p.node();
	match p.execute("commit").await {
		Ok(tag) if tag == "COMMIT" => Ok(Outcome {
			txid: None,
			unresolved: Vec::new(),
		}),
		Ok(tag) => Err(TwoPcError::Aborted {
			code: "40001",
			reason: format!("{node} answered {tag}: the transaction had already failed"),
			node: Some(node),
			error: None,
		}),
		Err(BackendError::Refused(m)) => Err(TwoPcError::Aborted {
			code: "XX000",
			reason: format!("{node} refused to commit"),
			node: Some(node),
			error: Some(m),
		}),
		Err(e) => Err(TwoPcError::Aborted {
			code: "08006",
			reason: format!("{node}: {e}; whether the commit happened is the node's to say"),
			node: Some(node),
			error: None,
		}),
	}
}

// ---------------------------------------------------------------------------------------------
// In-doubt recovery.

/// What one recovery pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Report {
	pub committed: usize,
	pub rolled_back: usize,
	/// Prepared transactions left alone this pass: inside their grace, or busy.
	pub waiting: usize,
	/// Log rows deleted.
	pub forgotten: usize,
	pub unreachable: Vec<NodeId>,
}

/// Starts the in-doubt recovery loop for the cluster; it runs as long as the process does. The
/// hook in `server::serve`: `tokio::spawn(twopc::recovery(app.clone()));` beside `live::watch`.
pub async fn recovery(app: Arc<App>) {
	recovery_with(app, Settings::default()).await;
}

pub async fn recovery_with(app: Arc<App>, settings: Settings) {
	loop {
		if let Err(e) = lead(&app, &settings).await {
			tracing::warn!("in-doubt recovery: {e}; retrying");
		}
		tokio::time::sleep(settings.interval).await;
	}
}

/// Takes the leader lock on `log` if no other router holds it.
pub async fn try_lead(log: &mut Backend) -> Result<bool, BackendError> {
	let r = log
		.query(
			"select pg_try_advisory_lock($1::bigint)",
			&[&LEADER_LOCK.to_string()],
		)
		.await?;
	Ok(r.first()
		.and_then(|r| r.first().cloned().flatten())
		.as_deref()
		== Some("t"))
}

async fn lead(app: &Arc<App>, settings: &Settings) -> Result<(), String> {
	let mut log = home_service(app, "2pc recovery")
		.await
		.map_err(|e| format!("home node: {e}"))?;
	loop {
		if try_lead(&mut log).await.map_err(|e| e.to_string())? {
			break;
		}
		tokio::time::sleep(settings.interval).await;
	}
	tracing::info!("in-doubt recovery: this router leads");
	let mut active: Option<BTreeSet<NodeId>> = None;
	let mut passes: u64 = 0;
	loop {
		if let Some(catalog) = app.catalog() {
			// A node that has just become writable (a promoted standby, a finished join) is
			// brought in line with home's roles, as is every node when this router starts leading.
			let now = writable_nodes(&catalog)
				.into_iter()
				.collect::<BTreeSet<_>>();
			let grew = active.as_ref().is_none_or(|a| !now.is_subset(a));
			if grew {
				match crate::roles::reconcile(app, &catalog).await {
					Ok(r) if r.iter().all(|(_, x)| x.is_ok()) => active = Some(now),
					Ok(r) => tracing::warn!("role reconcile: {r:?}"),
					Err(e) => tracing::warn!("role reconcile: {e}"),
				}
			}
			// pg_cron copies (cron.rs): at once for a new node, and every few passes for a job
			// marked, changed or unmarked on home.
			if grew || passes.is_multiple_of(CRON_EVERY) {
				match crate::cron::sync_cluster(app, &catalog).await {
					Ok(done) if !done.is_empty() => tracing::info!(?done, "cron sync"),
					Ok(_) => {}
					Err(e) => tracing::warn!("cron sync: {e}"),
				}
			}
			passes += 1;
			match recover_once(app, &catalog, &mut log, settings).await {
				Ok(r) if r.committed + r.rolled_back > 0 => tracing::info!(
					committed = r.committed,
					rolled_back = r.rolled_back,
					"in-doubt transactions resolved"
				),
				Ok(_) => {}
				// The log connection itself failing loses the lock: start again.
				Err(Pass::Log(e)) => return Err(e),
				Err(Pass::Other(e)) => tracing::warn!("in-doubt recovery: {e}"),
			}
		}
		tokio::time::sleep(settings.interval).await;
	}
}

/// Why a pass stopped.
#[derive(Debug)]
pub enum Pass {
	/// The log connection broke (and with it, the leader lock).
	Log(String),
	Other(String),
}

impl std::fmt::Display for Pass {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Pass::Log(m) | Pass::Other(m) => f.write_str(m),
		}
	}
}

fn log_err(e: BackendError) -> Pass {
	match e {
		BackendError::Refused(_) => Pass::Other(e.to_string()),
		e => Pass::Log(e.to_string()),
	}
}

struct Logged {
	decision: Decision,
	nodes: Vec<i32>,
	age: f64,
}

/// One recovery pass over every node of `catalog`, with `log` a service connection to the home
/// node. Safe to run beside live coordinators and beside other passes; the leader lock only
/// keeps routers from doing the same work twice.
pub async fn recover_once(
	app: &App,
	catalog: &Catalog,
	log: &mut Backend,
	settings: &Settings,
) -> Result<Report, Pass> {
	let mut report = Report::default();
	let uid = read_uid(log).await.map_err(log_err)?;

	// The log first (see the header for why the order matters).
	let mut logged: HashMap<i64, Logged> = HashMap::new();
	for r in log
		.query(
			"select txid, decision, array_to_string(nodes, ','), \
			extract(epoch from now() - decided_at)::float8 from lepis.prepared",
			&[],
		)
		.await
		.map_err(log_err)?
	{
		let get = |i: usize| r.get(i).cloned().flatten().unwrap_or_default();
		let Ok(txid) = get(0).parse() else { continue };
		logged.insert(
			txid,
			Logged {
				decision: if get(1) == "commit" {
					Decision::Commit
				} else {
					Decision::Abort
				},
				nodes: get(2).split(',').filter_map(|n| n.parse().ok()).collect(),
				age: get(3).parse().unwrap_or(0.0),
			},
		);
	}

	// Then every node's prepared transactions of this cluster.
	let pattern = format!("lepis:{uid}:%");
	let mut scanned: HashSet<i32> = HashSet::new();
	// txid → nodes still holding it after this pass.
	let mut remaining: HashMap<i64, usize> = HashMap::new();
	for node in catalog.nodes.values() {
		if node.state == NodeState::Removed {
			continue;
		}
		let mut b = match node_service(app, node, "2pc recovery").await {
			Ok(b) => b,
			Err(e) => {
				tracing::warn!("in-doubt recovery: {}: {e}", node.id);
				report.unreachable.push(node.id);
				continue;
			}
		};
		// A physical standby replays its source's prepared transactions and can finish none of
		// them; they are the source's to resolve, and it was never a participant.
		if is_standby(&mut b).await.unwrap_or(false) {
			b.close().await;
			continue;
		}
		let rows = match b
			.query(
				"select gid, extract(epoch from now() - prepared)::float8 from pg_prepared_xacts \
				where database = current_database() and gid like $1",
				&[&pattern],
			)
			.await
		{
			Ok(r) => r,
			Err(e) => {
				tracing::warn!("in-doubt recovery: {}: {e}", node.id);
				report.unreachable.push(node.id);
				continue;
			}
		};
		scanned.insert(node.id.0);
		for r in rows {
			let gid_text = r.first().cloned().flatten().unwrap_or_default();
			let age: f64 = r
				.get(1)
				.cloned()
				.flatten()
				.and_then(|v| v.parse().ok())
				.unwrap_or(0.0);
			let Some((txid, _)) = parse_gid(&uid, &gid_text) else {
				continue;
			};
			let decision = match logged.get(&txid) {
				Some(l) if l.decision == Decision::Abort => Some(Decision::Abort),
				Some(l) if l.age >= settings.commit_grace.as_secs_f64() => Some(Decision::Commit),
				Some(_) => None,
				None if age >= settings.abort_grace.as_secs_f64() => {
					// No decision past the grace: take the register for abort. If the coordinator
					// got there first, its commit is what comes back, and is obeyed.
					Some(
						decide(log, txid, &[], Decision::Abort)
							.await
							.map_err(log_err)?,
					)
				}
				None => None,
			};
			let done = match decision {
				None => false,
				Some(d) => {
					let verb = match d {
						Decision::Commit => "commit prepared",
						Decision::Abort => "rollback prepared",
					};
					match b
						.simple(&format!("{verb} {}", quote_literal(&gid_text)))
						.await
					{
						Ok(_) => {
							match d {
								Decision::Commit => report.committed += 1,
								Decision::Abort => report.rolled_back += 1,
							}
							true
						}
						// Finished meanwhile by its coordinator.
						Err(e) if e.sqlstate().as_deref() == Some("42704") => true,
						Err(e) => {
							tracing::debug!("in-doubt recovery: {gid_text}: {e}");
							false
						}
					}
				}
			};
			if !done {
				report.waiting += 1;
				*remaining.entry(txid).or_default() += 1;
			}
		}
		b.close().await;
	}

	// Forget what is finished: a commit whose nodes all answered and hold nothing for it, an
	// abort the same once it is older than its retention.
	let forget: Vec<String> = logged
		.iter()
		.filter(|(txid, l)| {
			let finished = !remaining.contains_key(txid)
				&& l.nodes.iter().all(|n| {
					scanned.contains(n)
						|| !catalog
							.nodes
							.get(&NodeId(*n))
							.is_some_and(|x| x.state != NodeState::Removed)
				});
			let old = match l.decision {
				Decision::Commit => l.age >= settings.commit_grace.as_secs_f64(),
				Decision::Abort => {
					l.age >= settings.abort_retain.as_secs_f64() && report.unreachable.is_empty()
				}
			};
			finished && old
		})
		.map(|(txid, _)| txid.to_string())
		.collect();
	if !forget.is_empty() {
		log.query(
			"delete from lepis.prepared where txid = any($1::bigint[])",
			&[&format!("{{{}}}", forget.join(","))],
		)
		.await
		.map_err(log_err)?;
		report.forgotten = forget.len();
	}
	Ok(report)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn gids_round_trip_and_ignore_other_clusters() {
		let g = gid("a1b2", 1234567890123, NodeId(3));
		assert_eq!(g, "lepis:a1b2:1234567890123:3");
		assert_eq!(parse_gid("a1b2", &g), Some((1234567890123, 3)));
		assert_eq!(parse_gid("a1b", &g), None);
		assert_eq!(parse_gid("a1b2", "lepis:a1b2:x:3"), None);
		assert_eq!(parse_gid("a1b2", "other:1:2"), None);
		// Postgres caps a gid at 200 bytes; the longest Lepis makes is far under it.
		assert!(gid("0123456789abcdef", i64::MAX, NodeId(i32::MAX)).len() < 200);
	}

	#[test]
	fn txids_are_positive_and_differ() {
		let ids: HashSet<i64> = (0..1000).map(|_| new_txid()).collect();
		assert_eq!(ids.len(), 1000);
		assert!(ids.iter().all(|i| *i > 0));
	}

	#[tokio::test]
	async fn join_all_keeps_order() {
		let futures: Vec<_> = (0..5u64)
			.map(|i| async move {
				tokio::time::sleep(Duration::from_millis(50 - i * 10)).await;
				i
			})
			.collect();
		assert_eq!(join_all(futures).await, vec![0, 1, 2, 3, 4]);
	}

	#[test]
	fn errors_tell_the_client_what_happened() {
		let in_doubt = TwoPcError::InDoubt {
			txid: 7,
			reason: "x".into(),
		}
		.to_message();
		let f = crate::wire::parse_error_fields(&in_doubt.body);
		assert!(f.contains(&(b'C', "08007".into())));
		let aborted = TwoPcError::Aborted {
			code: "40001",
			reason: "y".into(),
			node: None,
			error: None,
		}
		.to_message();
		assert!(crate::wire::parse_error_fields(&aborted.body).contains(&(b'C', "40001".into())));
	}
}
