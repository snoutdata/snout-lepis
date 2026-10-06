//! The session engine: one client, any number of nodes.
//!
//! The client speaks to Lepis as if it were one Postgres. Lepis reads each batch of messages (up
//! to a Sync, a Flush, or one simple Query), decides where it goes (route.rs), and relays the
//! answers back in order. The rules that keep that indistinguishable from one Postgres:
//!
//! - **One node per batch; a transaction takes in nodes as it goes.** A transaction starts on the
//!   first node it touches; a statement for another node opens the same transaction there (its
//!   BEGIN and SET LOCALs replayed), and COMMIT of a transaction on several nodes is two-phase
//!   (twopc.rs). SERIALIZABLE and savepoints do not cross nodes and are refused. A batch whose
//!   statements disagree is refused before anything is sent.
//! - **Statements for several nodes are answered by Lepis.** A read is scattered and merged
//!   (scatter.rs, merge.rs); an UPDATE or DELETE runs on every node that holds rows, an INSERT
//!   sends each row to its owner, a reference-table write and a schema change run on every
//!   node, all committed together; a schema change no transaction may hold runs node by node as
//!   a job (ddl.rs); a role statement runs on home and is then copied (roles.rs).
//! - **BEGIN is lazy.** A batch that only opens a transaction is answered by Lepis; the BEGIN is
//!   sent with the first statement that names a node.
//! - **Session state is replayed.** A session-level SET reaches every node the session uses,
//!   in order, before that node runs anything else; one undone by ROLLBACK is dropped.
//! - **Prepared statements follow the batch.** A statement prepared on one node is parsed again,
//!   silently, on any other node a later Bind sends it to.
//! - **Answers come back in the order the client asked.** Batches for the same node are
//!   pipelined; a batch for a different node waits until the earlier ones have finished.
//! - **The catalog is the one of the transaction's start.** Between transactions a session takes
//!   the router's newest catalog (a split or a move bumped the epoch), so no statement is routed
//!   by an epoch older than its transaction. Inside one it keeps its node; a statement the new
//!   catalog sends elsewhere fails with `40001` (retry the transaction), as the cutover's own
//!   abort of in-flight transactions does.
//! - **A refusal looks like any other error.** Outside a transaction Lepis answers with the
//!   ErrorResponse itself; inside one, the transaction's node raises it, so the node's
//!   transaction is aborted exactly as the client is told it is.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use tokio::io::AsyncWriteExt;

use crate::analyze;
use crate::backend::{self, Backend, BackendError, BoxStream};
use crate::catalog::{Catalog, NodeId};
use crate::config::NodeAddress;
use crate::ddl::{self, DdlPlan};
use crate::frame::FrameReader;
use crate::merge::{self, Gathered, MergeError, RankRequest, Ranks, Row};
use crate::roles::{self, RoleChange};
use crate::route::{self, AnalyzeError, ParamValue, Route, SessionVerb, Statement, TxVerb};
use crate::scatter;
use crate::scram::{ClientCredential, ClientSecret};
use crate::server::App;
use crate::twopc;
use crate::wire::{self, Body, ErrorFields, Message, WireError};

/// A connection, read through its frame buffer and written through `get_mut`. Not split: the
/// router reads and writes each one from its own task, never both at once, so the lock a split
/// takes on every read and write would buy nothing.
type Reader = FrameReader<BoxStream>;

/// One node's session for this client.
struct Conn {
	reader: Reader,
	/// The node's own process id and cancel key for this session.
	pid: i32,
	key: Vec<u8>,
	/// Statement name → the generation of the Parse this node holds.
	prepared: HashMap<String, u64>,
	/// How many of the session's SETs this node has run.
	sets_applied: usize,
	/// Statements closed by the client that this node still holds.
	closes_owed: Vec<String>,
	/// How many of the open transaction's SETs this node has run.
	tx_applied: usize,
}

/// A statement the client prepared (Parse), kept so it can be parsed again on another node.
struct Prepared {
	generation: u64,
	body: Vec<u8>,
	sql: String,
	/// The parameter types its Parse declared (0: unspecified).
	types: Vec<u32>,
}

/// Which of a node's answers reach the client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Owner {
	/// The client's batch: everything, less the swallowed completions.
	Client,
	/// A refusal raised on the transaction's node: only the error and what follows it, so the
	/// client sees what Postgres would have shown had its first message failed.
	ErrorOnly,
}

/// A Sync (or simple Query) sent to a node whose ReadyForQuery has not come back.
struct InFlight {
	node: NodeId,
	owner: Owner,
	/// ParseComplete / CloseComplete answers to drop, in the order they will arrive.
	swallow: VecDeque<bool>,
	/// The batch ROLLed BACK (for the SET bookkeeping).
	rolls_back: bool,
}

pub struct Router {
	app: Arc<App>,
	catalog: Arc<Catalog>,
	secret: ClientSecret,
	startup: Vec<(String, String)>,
	client_r: Reader,
	out: Vec<u8>,
	conns: HashMap<NodeId, Conn>,
	/// The node the open transaction is on.
	pinned: Option<NodeId>,
	/// A BEGIN the client sent that no node has seen yet.
	unbound_begin: Option<String>,
	tx_status: u8,
	statements: HashMap<String, Prepared>,
	generation: u64,
	portals: HashMap<String, NodeId>,
	sets: Vec<String>,
	pending_sets: Vec<String>,
	search_path: Vec<String>,
	/// Statement text → its analysis, for the catalog epoch and search_path it was made under
	/// (`analysed_for`): a prepared statement's Bind or a repeated query never parses again.
	analysed: std::cell::RefCell<HashMap<String, Arc<Analysed>>>,
	analysed_for: std::cell::RefCell<(i64, Vec<String>)>,
	/// Each node address's TLS client configuration, built once (cancel targets need it).
	tls: std::cell::RefCell<HashMap<String, Option<Arc<rustls::ClientConfig>>>>,
	search_path_dirty: bool,
	inflight: VecDeque<InFlight>,
	/// Complete batches waiting for their turn.
	queued: VecDeque<Vec<Message>>,
	/// The batch being collected.
	batch: Vec<Message>,
	/// After an error the client is told about, discard until its Sync (protocol rule).
	skip_until_sync: bool,
	/// A COPY FROM STDIN is running on this node.
	copy_in: Option<NodeId>,
	/// A COPY Lepis is splitting across nodes.
	copy_split: Option<CopySplit>,
	/// What is waiting in `out` must reach the client before Lepis waits again.
	flush_due: bool,
	cancel_pid: i32,
	/// The client's cancel key (Lepis's own), for `spread`.
	cancel_key: Vec<u8>,
	/// The node a cancel from the client currently reaches.
	cancel_node: NodeId,
	/// Array types of types the home node was asked about (ranking, merge.rs).
	array_types: HashMap<u32, u32>,
	/// Whether user types have collations, as the home node said (scatter.rs).
	collatable: HashMap<u32, bool>,
	/// The catalog moved while the open transaction ran.
	moved_in_tx: bool,
	/// The nodes the open transaction runs on, in the order it reached them.
	participants: Vec<NodeId>,
	/// The open transaction's BEGIN, as the client wrote it (replayed on each node it joins).
	tx_begin: Option<String>,
	/// The open transaction's SETs and SET LOCALs, replayed on each node it joins.
	tx_replay: Vec<String>,
	/// The open transaction used a savepoint.
	tx_savepoint: bool,
	coordinator: Option<Arc<twopc::Coordinator>>,
	/// Index → its table, as the home node said (ddl.rs `NeedIndexOwners`).
	index_owners: HashMap<crate::catalog::RelationName, crate::catalog::RelationName>,
	/// Role statements sent to the home node and not yet copied to the others.
	roles_owed: Vec<RoleChange>,
	/// The role the client logged in as (the pool's key).
	role: String,
	settings: Arc<Settings>,
	settings_read: std::time::Instant,
	/// The catalog's home node (looked up once per catalog).
	home_id: NodeId,
	/// Node sessions set aside between transactions (transaction pooling), and when they go to
	/// the pool.
	parked: Vec<(NodeId, Conn)>,
	parked_until: Option<tokio::time::Instant>,
	/// The JWT claim the transaction (or session) is routed by.
	claim: Option<Claim>,
}

impl Router {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		app: Arc<App>,
		catalog: Arc<Catalog>,
		secret: ClientSecret,
		startup: Vec<(String, String)>,
		client: BoxStream,
		home: Backend,
		home_id: NodeId,
		cancel_pid: i32,
		cancel_key: Vec<u8>,
	) -> Router {
		let role = startup
			.iter()
			.find(|(k, _)| k == "user")
			.map(|(_, v)| v.clone())
			.unwrap_or_default();
		let mut conns = HashMap::new();
		conns.insert(
			home_id,
			Conn {
				reader: FrameReader::new(home.stream),
				pid: home.pid,
				key: home.key,
				prepared: HashMap::new(),
				sets_applied: 0,
				closes_owed: Vec::new(),
				tx_applied: 0,
			},
		);
		Router {
			app,
			catalog,
			secret,
			startup,
			client_r: FrameReader::new(client),
			out: Vec::new(),
			conns,
			pinned: None,
			unbound_begin: None,
			tx_status: b'I',
			statements: HashMap::new(),
			generation: 0,
			portals: HashMap::new(),
			sets: Vec::new(),
			pending_sets: Vec::new(),
			search_path: vec!["public".into()],
			analysed: Default::default(),
			analysed_for: Default::default(),
			tls: Default::default(),
			search_path_dirty: true,
			inflight: VecDeque::new(),
			queued: VecDeque::new(),
			batch: Vec::new(),
			skip_until_sync: false,
			copy_in: None,
			copy_split: None,
			flush_due: false,
			cancel_pid,
			cancel_key,
			cancel_node: home_id,
			array_types: HashMap::new(),
			collatable: HashMap::new(),
			moved_in_tx: false,
			participants: Vec::new(),
			tx_begin: None,
			tx_replay: Vec::new(),
			tx_savepoint: false,
			coordinator: None,
			index_owners: HashMap::new(),
			roles_owed: Vec::new(),
			role,
			settings: Arc::new(Settings::default()),
			home_id,
			parked: Vec::new(),
			parked_until: None,
			claim: None,
			settings_read: std::time::Instant::now()
				.checked_sub(settings::FRESH * 2)
				.unwrap_or_else(std::time::Instant::now),
		}
	}

	fn home(&self) -> NodeId {
		self.home_id
	}

	/// Runs until the client leaves.
	pub async fn run(mut self) -> Result<(), WireError> {
		self.refresh_settings().await;
		loop {
			// One write per answer, not per message: the client is sent what is waiting when
			// nothing more is in flight, when a batch has ended (its ReadyForQuery) or a COPY
			// waits for the client, or when a lot is waiting.
			if self.inflight.is_empty() || self.flush_due || self.out.len() >= FLUSH_AT {
				self.flush_client().await?;
				self.flush_due = false;
			}
			if self.can_release() {
				self.release().await?;
			}
			// Responses come from the node at the front of the queue; with nothing in flight, the
			// home node may still send notifications.
			let front = self.inflight.front().map(|f| f.node);
			let listen_node = if self.copy_split.is_some() {
				None
			} else {
				front.or(Some(self.home()))
			};
			let parked_until = self.parked_until;
			let Router {
				client_r, conns, ..
			} = &mut self;
			let node_reader = listen_node
				.and_then(|n| conns.get_mut(&n))
				.map(|c| &mut c.reader);
			let event = match (node_reader, parked_until) {
				(Some(r), _) => tokio::select! {
					m = client_r.next() => Event::Client(m?),
					m = r.next() => Event::Node(listen_node.expect("set"), m?),
				},
				(None, Some(until)) => tokio::select! {
					m = client_r.next() => Event::Client(m?),
					() = tokio::time::sleep_until(until) => Event::Idle,
				},
				(None, None) => Event::Client(client_r.next().await?),
			};
			match event {
				Event::Client(m) => {
					if !self.on_client(m).await? {
						return Ok(());
					}
					// The rest of what the client has already sent (a batch arrives in one read).
					while let Some(m) = self.client_r.take()? {
						if !self.on_client(m).await? {
							return Ok(());
						}
					}
				}
				Event::Node(n, m) => {
					self.on_node(n, m).await?;
					// The rest of what this node has already sent, without another wait: a
					// node's answer usually arrives in one read.
					while self.inflight.front().is_some_and(|f| f.node == n)
						&& let Some(m) = self
							.conns
							.get_mut(&n)
							.map(|c| c.reader.take())
							.transpose()?
							.flatten()
					{
						self.on_node(n, m).await?;
					}
					self.dispatch().await?;
				}
				Event::Idle => self.publish(),
			}
		}
	}

	async fn on_client(&mut self, m: Message) -> Result<bool, WireError> {
		if self.copy_split.is_some() && m.tag != b'X' {
			self.copy_message(m).await?;
			return Ok(true);
		}
		if let Some(node) = self.copy_in
			&& matches!(m.tag, b'd' | b'c' | b'f' | b'H' | b'S')
		{
			if matches!(m.tag, b'c' | b'f') {
				self.copy_in = None;
			}
			self.send(node, &[m]).await?;
			return Ok(true);
		}
		if self.skip_until_sync {
			if m.tag == b'S' {
				self.skip_until_sync = false;
				self.local(&[], self.tx_status_now())?;
				return Ok(true);
			}
			if m.tag != b'X' {
				return Ok(true);
			}
		}
		match m.tag {
			b'X' => {
				for c in self.conns.values_mut() {
					let _ = c
						.reader
						.get_mut()
						.write_all(&wire::terminate().encode())
						.await;
				}
				// Idle sessions set aside are healthy: another client can have them.
				self.publish();
				return Ok(false);
			}
			b'Q' => {
				self.queued.push_back(vec![m]);
			}
			b'S' | b'H' => {
				self.batch.push(m);
				let b = std::mem::take(&mut self.batch);
				self.queued.push_back(b);
			}
			b'P' | b'B' | b'D' | b'E' | b'C' => self.batch.push(m),
			b'F' => self.queued.push_back(vec![m]),
			b'd' | b'c' | b'f' => {} // COPY messages with no COPY running: ignored, as Postgres does
			other => {
				return Err(WireError::Protocol(format!(
					"unexpected message '{}'",
					char::from(other)
				)));
			}
		}
		self.dispatch().await?;
		Ok(true)
	}

	async fn on_node(&mut self, n: NodeId, m: Message) -> Result<(), WireError> {
		let Some(front) = self.inflight.front_mut() else {
			// Asynchronous messages while idle: notifications, notices, parameter changes.
			if matches!(m.tag, b'A' | b'N' | b'S') {
				put_message(&mut self.out, &m);
			}
			return Ok(());
		};
		debug_assert_eq!(front.node, n);
		let mut forward = match front.owner {
			Owner::Client => true,
			Owner::ErrorOnly => matches!(m.tag, b'E' | b'N' | b'Z'),
		};
		if matches!(m.tag, b'Z' | b'G' | b'W' | b's') {
			self.flush_due = true;
		}
		match m.tag {
			b'1' | b'3' => {
				if front.swallow.pop_front() == Some(true) {
					forward = false;
				}
			}
			b'G' => self.copy_in = Some(n),
			b'Z' => {
				let done = self.inflight.pop_front().expect("front exists");
				let status = m.body.first().copied().unwrap_or(b'I');
				self.after_ready(done.node, status, done.rolls_back);
				self.copy_in = None;
				if forward {
					put_message(&mut self.out, &m);
				}
				return Ok(());
			}
			_ => {}
		}
		if forward {
			put_message(&mut self.out, &m);
		}
		Ok(())
	}

	fn after_ready(&mut self, node: NodeId, status: u8, rolled_back: bool) {
		let failed = self.tx_status == b'E';
		self.tx_status = status;
		match status {
			b'I' => {
				// The transaction is over: its SETs stand if it committed, and the node that ran
				// them already has them.
				if rolled_back || failed {
					self.pending_sets.clear();
				} else if !self.pending_sets.is_empty() {
					let up_to_date = self
						.conns
						.get(&node)
						.is_some_and(|c| c.sets_applied == self.sets.len());
					self.sets.append(&mut self.pending_sets);
					if up_to_date && let Some(c) = self.conns.get_mut(&node) {
						c.sets_applied = self.sets.len();
					}
				}
				self.end_tx_state();
			}
			_ => {
				self.pinned = Some(node);
				if !self.participants.contains(&node) {
					self.participants.push(node);
				}
			}
		}
	}

	fn tx_status_now(&self) -> u8 {
		if self.unbound_begin.is_some() && self.pinned.is_none() {
			b'T'
		} else {
			self.tx_status
		}
	}

	async fn flush_client(&mut self) -> Result<(), WireError> {
		if !self.out.is_empty() {
			self.client_r.get_mut().write_all(&self.out).await?;
			self.client_r.get_mut().flush().await?;
			self.out.clear();
		}
		Ok(())
	}

	// -----------------------------------------------------------------------------------------
	// Planning and dispatch.

	/// Sends queued batches while order allows: a batch goes out when nothing is in flight, or
	/// when everything in flight is on the same node.
	async fn dispatch(&mut self) -> Result<(), WireError> {
		while !self.queued.is_empty() {
			// Planning may need the home node idle (search_path), so plan only when idle or
			// when the plan can be made without it.
			if !self.inflight.is_empty() && self.search_path_dirty {
				return Ok(());
			}
			let batch = self.queued.pop_front().expect("front exists");
			let plan = match self.plan(&batch).await {
				Ok(p) => p,
				Err(e) => {
					self.queued.push_front(batch);
					return Err(e);
				}
			};
			let target = match &plan {
				Plan::Node { node, .. } => Some(*node),
				Plan::Local { .. }
				| Plan::Refuse(_)
				| Plan::Gather { .. }
				| Plan::EndTx { .. }
				| Plan::Fanout(_)
				| Plan::Job(_)
				| Plan::Copy { .. }
				| Plan::NeedIndexOwners(_) => None,
			};
			if self.busy_elsewhere(target) {
				self.queued.push_front(batch);
				return Ok(());
			}
			self.execute(plan, batch).await?;
		}
		Ok(())
	}

	fn busy_elsewhere(&self, target: Option<NodeId>) -> bool {
		match target {
			None => !self.inflight.is_empty(),
			Some(n) => self.inflight.iter().any(|f| f.node != n),
		}
	}

	/// Folds one query's statements into what the batch needs; a refusal stops the batch.
	fn consider(
		&self,
		acc: &mut Acc,
		stmts: &[Statement],
		params: &[ParamValue],
	) -> Option<route::Refusal> {
		for s in stmts {
			if self.settings.transaction_pool
				&& let Some(what) = s.session_bound
			{
				return Some(route::Refusal {
					code: route::NOT_ACROSS_NODES,
					message: format!("{what} outlives the transaction, and with transaction pooling the next transaction may run on another backend"),
					hint: "Set the cluster's pool_mode to session for this, or keep the state inside one transaction.".into(),
				});
			}
			// The JWT claims a data API request sets: the transaction goes to that key's node.
			if self.settings.route_claim.is_some()
				&& let Some((json, local)) = analyze::jwt_claims(&s.sql, params)
			{
				match self.claim_from(&json, local) {
					Some(Ok(c)) => {
						acc.targets.push(c.node);
						acc.claim = Some(c);
					}
					Some(Err(r)) => return Some(r),
					None => {}
				}
			}
			if let Some(v) = s.tx {
				acc.tx_verbs.push(v);
				continue;
			}
			if let Some(v) = s.session {
				acc.sessions.push((v, s.sql.clone()));
				continue;
			}
			acc.statements += 1;
			// Schema changes and role statements: where and how (ddl.rs, roles.rs).
			if matches!(s.facts.kind, route::Kind::Ddl | route::Kind::Other) {
				let names: Vec<crate::catalog::RelationName> =
					s.facts.tables.iter().map(|t| t.name.clone()).collect();
				match ddl::plan(&s.sql, &names, &self.catalog, &self.index_owners) {
					DdlPlan::NotDdl
					| DdlPlan::Transactional(ddl::Target::Home)
					| DdlPlan::PerNode(ddl::Target::Home) => {}
					DdlPlan::Transactional(ddl::Target::Everywhere) => {
						acc.data = true;
						acc.fanouts.push(Fanout {
							nodes: ddl::nodes(ddl::Target::Everywhere, &self.catalog),
							sql: s.sql.clone(),
							same_count: false,
							insert: false,
							written: Vec::new(),
							per_node: None,
							rows: None,
							ddl: true,
						});
						continue;
					}
					DdlPlan::PerNode(ddl::Target::Everywhere) => {
						acc.data = true;
						acc.jobs.push(s.sql.clone());
						continue;
					}
					DdlPlan::Role(change) => acc.roles.push(change),
					DdlPlan::NeedIndexOwners(names) => {
						acc.index_owners.extend(names);
						continue;
					}
					DdlPlan::Refuse(r) => return Some(r),
				}
			}
			// No relation at all, or only the system catalogs (identical on every node): it
			// runs wherever the batch or the transaction already is.
			if s.facts.tables.iter().all(|t| {
				t.name.schema == "pg_catalog"
					|| t.name.schema == "information_schema"
					|| t.name.table.starts_with("pg_")
			}) && !s.facts.tables.iter().any(|t| t.written)
			{
				continue;
			}
			acc.data = true;
			let r = route::route(&s.facts, &self.catalog, params);
			let r = match acc.claim.as_ref().or(self.claim.as_ref()) {
				Some(c) => match self.claimed(r, &s.facts, params, c) {
					Ok(r) => r,
					Err(e) => return Some(e),
				},
				None => r,
			};
			match r {
				Route::Node(n) => {
					// EXPLAIN of a read on one node: Lepis adds its own routing line.
					if s.facts.kind == route::Kind::Select
						&& s.sql
							.trim_start()
							.get(..7)
							.is_some_and(|w| w.eq_ignore_ascii_case("explain"))
						&& scatter::is_explain(&s.sql)
					{
						acc.explains.push((n, s.sql.clone()));
					}
					if n != self.home() {
						let lower = s.sql.to_ascii_lowercase();
						if lower.contains("nextval") || lower.contains("setval") {
							acc.off_home.push(s.sql.clone());
						}
					}
					acc.targets.push(n);
				}
				Route::Home => acc.targets.push(self.home()),
				Route::Refuse(r) => return Some(r),
				Route::Scatter(nodes) => acc.gathers.push((nodes, s.sql.clone())),
				Route::SplitCopy(nodes) => acc.copies.push((nodes, s.sql.clone())),
				Route::Fanout { nodes, same_count } => {
					let written = s
						.facts
						.tables
						.iter()
						.filter(|t| t.written)
						.map(|t| t.name.clone())
						.collect();
					acc.fanouts.push(Fanout {
						nodes,
						sql: s.sql.clone(),
						same_count,
						insert: s.facts.kind == route::Kind::Insert,
						written,
						per_node: None,
						rows: None,
						ddl: false,
					});
				}
				Route::SplitInsert(groups) => {
					// Each node its own rows, in one two-phase commit.
					let rows: Vec<Vec<usize>> = groups.iter().map(|(_, r)| r.clone()).collect();
					let per_node = match scatter::split_insert(&s.sql, &rows) {
						Ok(p) => p,
						Err(r) => return Some(r),
					};
					acc.fanouts.push(Fanout {
						nodes: groups.iter().map(|(n, _)| *n).collect(),
						sql: s.sql.clone(),
						same_count: false,
						insert: true,
						written: Vec::new(),
						per_node: Some(per_node),
						rows: Some(rows),
						ddl: false,
					});
				}
			}
		}
		None
	}

	/// Takes the router's newest catalog: between transactions at once, inside one noting that
	/// it moved. Only with nothing in flight, so the transaction state it reads is current.
	fn refresh_catalog(&mut self) {
		if !self.inflight.is_empty() {
			return;
		}
		let Some(fresh) = self.app.catalog() else {
			return;
		};
		if fresh.epoch == self.catalog.epoch || fresh.home().is_none() {
			return;
		}
		// A BEGIN no node has seen yet has read nothing: its transaction starts on the new one.
		if self.pinned.is_some() || self.tx_status != b'I' {
			self.moved_in_tx = true;
		}
		self.home_id = fresh.home().map(|n| n.id).unwrap_or(self.home_id);
		self.catalog = fresh;
	}

	/// Plans a batch. Anything that reads or changes the transaction's state waits for the
	/// batches still on their way back first: their ReadyForQuery is what says whether a
	/// transaction is open, and on which nodes.
	async fn plan(&mut self, batch: &[Message]) -> Result<Plan, WireError> {
		let mut p = self.plan_once(batch).await?;
		if let Plan::NeedIndexOwners(names) = &p {
			self.learn_index_owners(names.clone()).await?;
			p = self.plan_once(batch).await?;
			if let Plan::NeedIndexOwners(names) = &p {
				// Indexes the home node does not have: the statement fails there as it should.
				for n in names.clone() {
					self.index_owners.insert(n.clone(), n);
				}
				p = self.plan_once(batch).await?;
			}
		}
		if self.inflight.is_empty() || !self.depends_on_tx(&p, batch) {
			return Ok(p);
		}
		self.wait_idle().await?;
		self.plan_once(batch).await
	}

	fn depends_on_tx(&self, p: &Plan, batch: &[Message]) -> bool {
		let plain = matches!(p, Plan::Node { join: false, begin: None, sessions, .. } if sessions.is_empty());
		if !plain
			|| self.pinned.is_some()
			|| self.tx_status != b'I'
			|| !self.participants.is_empty()
		{
			return true;
		}
		batch.iter().any(|m| {
			let sql = match m.tag {
				b'Q' => cstr(&m.body),
				b'P' => parse_parse(&m.body).map(|(_, s)| s).unwrap_or_default(),
				_ => return false,
			};
			self.analyze(&sql)
				.as_ref()
				.as_ref()
				.map_or(true, |v| v.iter().any(|s| s.tx.is_some()))
		})
	}

	async fn plan_once(&mut self, batch: &[Message]) -> Result<Plan, WireError> {
		self.refresh_settings().await;
		self.refresh_catalog();
		if self.search_path_dirty && self.inflight.is_empty() {
			self.refresh_search_path().await?;
		}
		let mut acc = Acc::default();
		// Statements this batch parses, for its own Binds (they reach self.statements only when
		// the batch is sent).
		let mut parsed_here: HashMap<String, (String, Vec<u8>)> = HashMap::new();
		let mut bound_here: Vec<String> = Vec::new();

		for m in batch {
			let analysed: Option<(Arc<Analysed>, Vec<ParamValue>)> = match m.tag {
				b'Q' => {
					let sql = cstr(&m.body);
					Some((self.analyze(&sql), Vec::new()))
				}
				b'P' => {
					let (name, sql) = parse_parse(&m.body)?;
					parsed_here.insert(name, (sql, m.body.clone()));
					None
				}
				b'B' => {
					let (portal, stmt, params) = parse_bind(&m.body)?;
					bound_here.push(portal);
					// An unknown statement: the node will say it does not exist.
					let known: Option<(&str, std::borrow::Cow<[u32]>)> = parsed_here
						.get(&stmt)
						.map(|(s, b)| {
							(
								s.as_str(),
								std::borrow::Cow::Owned(parse_param_types(b).unwrap_or_default()),
							)
						})
						.or_else(|| {
							self.statements.get(&stmt).map(|p| {
								(
									p.sql.as_str(),
									std::borrow::Cow::Borrowed(p.types.as_slice()),
								)
							})
						});
					match known {
						Some((sql, declared)) => {
							// A parameter whose type the Parse declared is that type's value.
							let params = params
								.into_iter()
								.enumerate()
								.map(|(i, p)| match declared.get(i) {
									Some(&oid) if oid != 0 && p != ParamValue::Null => {
										ParamValue::Declared(oid, Box::new(p))
									}
									_ => p,
								})
								.collect();
							Some((self.analyze(sql), params))
						}
						None => None,
					}
				}
				b'E' | b'D' | b'C' => {
					// A portal from an earlier batch: its node is a target. One this batch binds
					// is the Bind's, wherever the old one of that name was.
					if let Some(name) = portal_ref(m)
						&& !bound_here.contains(&name)
						&& let Some(n) = self.portals.get(&name)
					{
						acc.targets.push(*n);
					}
					None
				}
				_ => None,
			};
			let Some((result, params)) = analysed else {
				continue;
			};
			match &*result {
				Ok(stmts) => {
					if let Some(r) = self.consider(&mut acc, stmts, &params) {
						return Ok(Plan::Refuse(r));
					}
				}
				Err(AnalyzeError::Unsupported(msg)) => {
					return Ok(Plan::Refuse(route::Refusal {
						code: route::NOT_ACROSS_NODES,
						message: msg.clone(),
						hint: "Rewrite the statement without it, or pin it to one shard key value."
							.into(),
					}));
				}
				Err(AnalyzeError::Syntax(_)) => {
					// The home node decides: a real syntax error is its error to give; a statement
					// it accepts and Lepis cannot read is refused rather than guessed at.
					if self.catalog.nodes.len() > 1 {
						let sql = match m.tag {
							b'Q' => cstr(&m.body),
							_ => {
								let (_, stmt, _) = parse_bind(&m.body)?;
								parsed_here
									.get(&stmt)
									.map(|(sql, _)| sql.clone())
									.or_else(|| self.statements.get(&stmt).map(|p| p.sql.clone()))
									.unwrap_or_default()
							}
						};
						if let Some(r) = self.unreadable(&sql).await? {
							return Ok(Plan::Refuse(r));
						}
					}
					acc.targets.push(self.pinned.unwrap_or(self.home()));
				}
			}
		}

		let Acc {
			mut targets,
			tx_verbs,
			sessions,
			data,
			statements,
			gathers,
			explains,
			fanouts,
			off_home,
			jobs,
			roles,
			index_owners,
			copies,
			claim,
		} = acc;
		if let Some(c) = claim {
			// Set on every node the transaction (or session) reaches later as well.
			let replay = Self::claim_replay(&c);
			if c.local {
				self.tx_replay.push(replay);
			} else {
				self.sets.push(replay);
			}
			self.claim = Some(c);
		}
		if let Some((nodes, sql)) = copies.first() {
			if copies.len() > 1 || statements > 1 || !sessions.is_empty() || !tx_verbs.is_empty() {
				return Ok(Plan::Refuse(route::Refusal {
					code: route::NOT_ACROSS_NODES,
					message: "a COPY across nodes must be sent on its own".into(),
					hint: "Send the COPY as a statement of its own.".into(),
				}));
			}
			if let Some(r) = self.gather_refusal(batch) {
				return Ok(Plan::Refuse(r));
			}
			// A Parse or Describe of the COPY alone is the home node's to answer; the COPY runs
			// at its Execute.
			if batch.iter().any(|m| matches!(m.tag, b'Q' | b'E')) {
				return Ok(Plan::Copy {
					nodes: nodes.clone(),
					sql: sql.clone(),
				});
			}
			targets.push(self.home());
		}
		if !index_owners.is_empty() {
			return Ok(Plan::NeedIndexOwners(index_owners));
		}
		if let Some(sql) = jobs.first() {
			if jobs.len() > 1 || statements > 1 || !sessions.is_empty() || !tx_verbs.is_empty() {
				return Ok(Plan::Refuse(route::Refusal {
					code: route::NOT_ACROSS_NODES,
					message: "a schema change that runs node by node must be sent on its own"
						.into(),
					hint:
						"Send it as a statement of its own, not in one string or batch with others."
							.into(),
				}));
			}
			if self.in_tx() {
				return Ok(Plan::Refuse(route::Refusal {
					code: "25001",
					message: "this statement cannot run inside a transaction block".into(),
					hint: "Run it outside the transaction block.".into(),
				}));
			}
			if let Some(r) = self.shape_refusal(batch) {
				return Ok(Plan::Refuse(r));
			}
			return Ok(Plan::Job(sql.clone()));
		}
		self.roles_owed.extend(roles);
		for sql in &off_home {
			if let Some(r) = self.sequence_refusal(sql).await? {
				return Ok(Plan::Refuse(r));
			}
		}
		if let Some(f) = fanouts.first() {
			if fanouts.len() > 1 || statements > 1 || !sessions.is_empty() || !tx_verbs.is_empty() {
				return Ok(Plan::Refuse(route::Refusal {
					code: route::NOT_ACROSS_NODES,
					message: "a write to several nodes must be sent on its own".into(),
					hint:
						"Send it as a statement of its own, not in one string or batch with others."
							.into(),
				}));
			}
			if let Some(r) = self.shape_refusal(batch) {
				return Ok(Plan::Refuse(r));
			}
			if self.participants.len() > 1 && self.tx_status == b'E' {
				return Ok(Plan::Refuse(route::Refusal {
					code: "25P02",
					message: "this transaction has already failed on one of its nodes; nothing more runs in it".into(),
					hint: "End it with ROLLBACK.".into(),
				}));
			}
			if self.pinned.is_some() && self.moved_in_tx {
				return Ok(Plan::Refuse(route::Refusal {
					code: "40001",
					message: "the rows this statement needs moved to another node while the transaction ran".into(),
					hint: "Retry the transaction.".into(),
				}));
			}
			if self.pinned.is_some()
				&& f.nodes.iter().any(|n| !self.participants.contains(n))
				&& let Some(r) = self.join_refusal()
			{
				return Ok(Plan::Refuse(r));
			}
			return Ok(Plan::Fanout(f.clone()));
		}
		// A read across nodes is answered by Lepis from every node's answer (scatter.rs). It runs
		// alone, outside a transaction; an EXPLAIN on one node takes the same path when it can.
		if let Some((nodes, sql)) = gathers.first() {
			if gathers.len() > 1 || statements > 1 || !sessions.is_empty() || !tx_verbs.is_empty() {
				return Ok(Plan::Refuse(route::Refusal {
					code: route::NOT_ACROSS_NODES,
					message: "a query that reads several nodes must be sent on its own".into(),
					hint: "Send it as a query of its own, not in one string or batch with other statements.".into(),
				}));
			}
			if let Some(r) = self.gather_refusal(batch) {
				return Ok(Plan::Refuse(r));
			}
			return Ok(Plan::Gather {
				nodes: nodes.clone(),
				sql: sql.clone(),
			});
		}
		if let [(n, sql)] = explains.as_slice()
			&& statements == 1
			&& sessions.is_empty()
			&& tx_verbs.is_empty()
			&& self.gather_refusal(batch).is_none()
		{
			return Ok(Plan::Gather {
				nodes: vec![*n],
				sql: sql.clone(),
			});
		}
		targets.sort();
		targets.dedup();
		let node = match targets.as_slice() {
			[] => None,
			[n] => Some(*n),
			_ => {
				return Ok(Plan::Refuse(route::Refusal {
					code: route::NOT_ACROSS_NODES,
					message:
						"these statements belong to different nodes and cannot run together yet"
							.into(),
					hint: "Send the statements for each shard key value in a batch of their own."
						.into(),
				}));
			}
		};
		// A transaction on several nodes ends through Lepis (two-phase commit), alone in its batch.
		let ends = tx_verbs
			.iter()
			.find(|v| matches!(v, TxVerb::Commit | TxVerb::Rollback))
			.copied();
		let multi = self.participants.len() > 1;
		if multi && tx_verbs.contains(&TxVerb::Other) {
			return Ok(Plan::Refuse(route::Refusal {
				code: route::NOT_ACROSS_NODES,
				message: "savepoints and PREPARE TRANSACTION in a transaction across several nodes are not supported".into(),
				hint: "Keep a transaction with savepoints to the shard key values of one node.".into(),
			}));
		}
		if (multi || self.tx_status == b'E')
			&& let Some(v) = ends
		{
			if data || !sessions.is_empty() || tx_verbs.len() > 1 {
				return Ok(Plan::Refuse(route::Refusal {
					code: route::NOT_ACROSS_NODES,
					message: "COMMIT or ROLLBACK of a transaction across several nodes must be sent on its own".into(),
					hint: "Send COMMIT (or ROLLBACK) as a statement of its own.".into(),
				}));
			}
			let chain = batch.iter().any(|m| {
				let sql = match m.tag {
					b'Q' => cstr(&m.body),
					b'P' => parse_parse(&m.body).map(|(_, s)| s).unwrap_or_default(),
					_ => String::new(),
				}
				.to_ascii_lowercase();
				sql.contains("and chain") && !sql.contains("and no chain")
			});
			if chain {
				return Ok(Plan::Refuse(route::Refusal {
					code: route::NOT_ACROSS_NODES,
					message:
						"COMMIT AND CHAIN of a transaction across several nodes is not supported"
							.into(),
					hint: "COMMIT, then BEGIN again.".into(),
				}));
			}
			return Ok(Plan::EndTx {
				commit: v == TxVerb::Commit,
			});
		}
		// In a transaction across nodes that failed, everything but its end fails as Postgres
		// fails it.
		if self.tx_status == b'E' && (multi || !tx_verbs.contains(&TxVerb::Other)) {
			return Ok(Plan::Refuse(route::Refusal {
				code: "25P02",
				message: "this transaction has already failed on one of its nodes; nothing more runs in it".into(),
				hint: "End it with ROLLBACK.".into(),
			}));
		}
		let mut join = false;
		if let (Some(p), Some(n)) = (self.pinned, node)
			&& p != n && !self.participants.contains(&n)
			&& !self.moved_in_tx
			&& self.tx_status == b'T'
		{
			if let Some(r) = self.join_refusal() {
				return Ok(Plan::Refuse(r));
			}
			join = true;
		}
		// After the catalog moved, a statement for any node but the last one is refused: the
		// transaction's other nodes hold snapshots taken under the old ownership.
		if let (Some(p), Some(n)) = (self.pinned, node)
			&& p != n && !join
			&& (self.moved_in_tx || !self.participants.contains(&n))
		{
			if self.moved_in_tx {
				return Ok(Plan::Refuse(route::Refusal {
					code: "40001",
					message: "the rows this statement needs moved to another node while the transaction ran".into(),
					hint: "Retry the transaction.".into(),
				}));
			}
			return Ok(Plan::Refuse(route::Refusal {
				code: route::NOT_ACROSS_NODES,
				message: "this transaction already uses another node".into(),
				hint: "Keep a transaction to the shard key values of one node, or run the other statements outside it.".into(),
			}));
		}

		let rolls_back = tx_verbs.contains(&TxVerb::Rollback);
		match (node, self.pinned) {
			(Some(n), _) => Ok(Plan::Node {
				node: n,
				sessions,
				rolls_back,
				begin: self.unbound_begin.clone(),
				join,
			}),
			(None, Some(p)) => Ok(Plan::Node {
				node: p,
				sessions,
				rolls_back,
				begin: None,
				join: false,
			}),
			(None, None) if !data && !tx_verbs.is_empty() && sessions.is_empty() => {
				// Only transaction control, with no node yet: Lepis answers.
				Ok(Plan::Local { tx_verbs })
			}
			(None, None) => {
				let home = self.home();
				Ok(Plan::Node {
					node: home,
					sessions,
					rolls_back,
					begin: self.unbound_begin.clone(),
					join: false,
				})
			}
		}
	}

	async fn execute(&mut self, plan: Plan, batch: Vec<Message>) -> Result<(), WireError> {
		match plan {
			Plan::Refuse(r) => self.refuse(r, &batch).await,
			Plan::Gather { nodes, sql } => self.gather(nodes, sql, batch).await,
			Plan::EndTx { commit } => self.end_tx(commit, &batch).await,
			Plan::Fanout(f) => self.fanout(f, batch).await,
			Plan::Job(sql) => self.job(sql, batch).await,
			Plan::Copy { nodes, sql } => self.copy_start(nodes, sql, batch).await,
			Plan::NeedIndexOwners(_) => unreachable!("plan() answers it"),
			Plan::Local { tx_verbs } => {
				for v in &tx_verbs {
					match v {
						TxVerb::Begin => {
							if self.unbound_begin.is_none() {
								self.unbound_begin = Some("BEGIN".into());
							}
						}
						TxVerb::Commit | TxVerb::Rollback => self.unbound_begin = None,
						TxVerb::Other => {}
					}
				}
				// Re-read the BEGIN's own text, so its isolation level and modes reach the node.
				for m in &batch {
					let sql = match m.tag {
						b'Q' => Some(cstr(&m.body)),
						b'P' => parse_parse(&m.body).ok().map(|(_, s)| s),
						_ => None,
					};
					if let Some(sql) = sql
						&& let Ok(stmts) = &*self.analyze(&sql)
					{
						for s in stmts {
							if s.tx == Some(TxVerb::Begin) && self.unbound_begin.is_some() {
								self.unbound_begin = Some(s.sql.clone());
							}
						}
					}
				}
				let status = self.tx_status_now();
				self.local(&batch, status)
			}
			Plan::Node {
				node,
				sessions,
				rolls_back,
				begin,
				join,
			} => {
				if join {
					if let Some(r) = self.join(node).await? {
						return self.refuse(r, &batch).await;
					}
				} else {
					self.ensure_conn(node).await?;
					self.catch_up_sets(node).await?;
					self.catch_up_tx(node).await?;
				}
				if let Some(b) = begin {
					self.unbound_begin = None;
					self.tx_begin = Some(b.clone());
					self.internal(node, &b).await?;
				}
				let opens_tx = batch.iter().any(|m| {
					let sql = match m.tag {
						b'Q' => Some(cstr(&m.body)),
						b'P' => parse_parse(&m.body).ok().map(|(_, s)| s),
						_ => None,
					};
					sql.is_some_and(|sql| {
						self.analyze(&sql)
							.as_ref()
							.as_ref()
							.is_ok_and(|v| v.iter().any(|s| s.tx == Some(TxVerb::Begin)))
					})
				});
				let in_tx = self.pinned.is_some() || self.tx_status != b'I' || opens_tx;
				// What the transaction needs replayed on a node it joins later: its BEGIN, its
				// SETs; and whether it used a savepoint (which no join could follow).
				for m in &batch {
					let sql = match m.tag {
						b'Q' => cstr(&m.body),
						b'P' => parse_parse(&m.body).map(|(_, s)| s).unwrap_or_default(),
						_ => continue,
					};
					let a = self.analyze(&sql);
					for s in a.as_ref().as_ref().map(Vec::as_slice).unwrap_or(&[]) {
						match s.tx {
							Some(TxVerb::Begin) if self.tx_begin.is_none() => {
								self.tx_begin = Some(s.sql.clone());
							}
							Some(TxVerb::Other) if in_tx => self.tx_savepoint = true,
							_ => {}
						}
					}
				}
				for (verb, sql) in &sessions {
					if in_tx && matches!(verb, SessionVerb::Set { .. } | SessionVerb::SetLocal) {
						// This node runs it in the batch; the other participants before their next.
						let n = self.tx_replay.len();
						self.tx_replay.push(sql.clone());
						if let Some(c) = self.conns.get_mut(&node)
							&& c.tx_applied == n
						{
							c.tx_applied = n + 1;
						}
					}
					match verb {
						SessionVerb::Set {
							touches_search_path,
						} => {
							if in_tx {
								self.pending_sets.push(sql.clone());
							} else {
								// The node runs it in this batch, after catching up.
								self.sets.push(sql.clone());
								let n = self.sets.len();
								if let Some(c) = self.conns.get_mut(&node)
									&& c.sets_applied + 1 == n
								{
									c.sets_applied = n;
								}
							}
							if *touches_search_path {
								self.search_path_dirty = true;
							}
						}
						SessionVerb::ResetAll => {
							self.sets.clear();
							self.pending_sets.clear();
							self.search_path_dirty = true;
							for c in self.conns.values_mut() {
								c.sets_applied = 0;
							}
						}
						SessionVerb::SetLocal | SessionVerb::Other => {}
					}
				}
				self.forward(node, batch, rolls_back).await?;
				self.copy_roles().await
			}
		}
	}

	/// Sends a client batch to its node, re-parsing statements the node does not hold.
	async fn forward(
		&mut self,
		node: NodeId,
		batch: Vec<Message>,
		rolls_back: bool,
	) -> Result<(), WireError> {
		self.retarget_cancel(node);
		let mut out: Vec<Message> = Vec::with_capacity(batch.len());
		let mut swallow = VecDeque::new();
		let mut syncs = 0usize;
		let owed: Vec<String> =
			std::mem::take(&mut self.conns.get_mut(&node).expect("ensured").closes_owed);
		for name in owed {
			out.push(Body::new().byte(b'S').cstr(&name).message(b'C'));
			swallow.push_back(true);
		}
		for m in batch {
			match m.tag {
				b'P' => {
					let (name, sql) = parse_parse(&m.body)?;
					self.generation += 1;
					self.statements.insert(
						name.clone(),
						Prepared {
							generation: self.generation,
							types: parse_param_types(&m.body).unwrap_or_default(),
							body: m.body.clone(),
							sql,
						},
					);
					let g = self.generation;
					self.conns
						.get_mut(&node)
						.expect("ensured")
						.prepared
						.insert(name, g);
					swallow.push_back(false);
					out.push(m);
				}
				b'B' | b'D' => {
					let (portal, stmt) = if m.tag == b'B' {
						let (portal, stmt) = bind_names(&m.body)?;
						(Some(portal), Some(stmt))
					} else if m.body.first() == Some(&b'S') {
						(None, Some(cstr(&m.body[1..])))
					} else {
						(None, None)
					};
					if let Some(stmt) = stmt
						&& let Some(p) = self.statements.get(&stmt)
					{
						let conn = self.conns.get_mut(&node).expect("ensured");
						if conn.prepared.get(&stmt) != Some(&p.generation) {
							out.push(Message::new(b'P', p.body.clone()));
							swallow.push_back(true);
							conn.prepared.insert(stmt, p.generation);
						}
					}
					if let Some(portal) = portal {
						self.portals.insert(portal, node);
					}
					out.push(m);
				}
				b'C' => {
					if m.body.first() == Some(&b'S') {
						let name = cstr(&m.body[1..]);
						self.statements.remove(&name);
						for (id, c) in self.conns.iter_mut() {
							if c.prepared.remove(&name).is_some() && *id != node {
								c.closes_owed.push(name.clone());
							}
						}
					}
					swallow.push_back(false);
					out.push(m);
				}
				b'S' | b'Q' | b'F' => {
					if m.tag == b'Q' {
						// A simple query replaces the node's unnamed statement.
						self.conns
							.get_mut(&node)
							.expect("ensured")
							.prepared
							.remove("");
					}
					syncs += 1;
					out.push(m);
				}
				_ => out.push(m),
			}
		}
		self.send(node, &out).await?;
		// One InFlight per Sync / Query; the swallow list belongs to the first.
		for i in 0..syncs {
			self.inflight.push_back(InFlight {
				node,
				owner: Owner::Client,
				swallow: if i == 0 {
					std::mem::take(&mut swallow)
				} else {
					VecDeque::new()
				},
				rolls_back,
			});
		}
		Ok(())
	}

	/// Lepis's own statement on a node, its answers dropped. Waits for it.
	async fn internal(&mut self, node: NodeId, sql: &str) -> Result<(), WireError> {
		self.wait_idle().await?;
		self.send(node, &[wire::query(sql)]).await?;
		let conn = self.conns.get_mut(&node).expect("ensured");
		conn.prepared.remove("");
		loop {
			let m = conn.reader.next().await?;
			match m.tag {
				b'Z' => {
					let status = m.body.first().copied().unwrap_or(b'I');
					if status != b'I' {
						self.pinned = Some(node);
						self.tx_status = status;
					}
					return Ok(());
				}
				// A ParameterStatus is the client's to see (SET TimeZone replayed on a node).
				b'S' => put_message(&mut self.out, &m),
				_ => {}
			}
		}
	}

	/// Waits until every in-flight batch has been answered, relaying as usual.
	async fn wait_idle(&mut self) -> Result<(), WireError> {
		while let Some(f) = self.inflight.front() {
			let n = f.node;
			let m = self
				.conns
				.get_mut(&n)
				.expect("in flight")
				.reader
				.next()
				.await?;
			Box::pin(self.on_node(n, m)).await?;
		}
		Ok(())
	}

	async fn catch_up_sets(&mut self, node: NodeId) -> Result<(), WireError> {
		let applied = self.conns.get(&node).map_or(0, |c| c.sets_applied);
		if applied < self.sets.len() {
			let sql = self.sets[applied..].join(";\n");
			self.internal(node, &sql).await?;
			let n = self.sets.len();
			self.conns.get_mut(&node).expect("ensured").sets_applied = n;
		}
		Ok(())
	}

	async fn ensure_conn(&mut self, node: NodeId) -> Result<(), WireError> {
		if self.conns.contains_key(&node) {
			return Ok(());
		}
		if let Some(i) = self.parked.iter().position(|(n, _)| *n == node) {
			let (_, c) = self.parked.swap_remove(i);
			self.conns.insert(node, c);
			return Ok(());
		}
		if self.take_pooled(node).await? {
			return Ok(());
		}
		let n = self.catalog.nodes.get(&node).cloned();
		let Some(n) = n else {
			return Err(WireError::Protocol(format!("{node} is not in the catalog")));
		};
		let address = NodeAddress {
			host: n.host.clone(),
			port: n.port,
			sslmode: n.sslmode,
			ca_file: self.app.config.home.ca_file.clone(),
		};
		let tls = crate::tls::client_config(&address).map_err(WireError::Protocol)?;
		let mut params: Vec<(String, String)> = self
			.startup
			.iter()
			.filter(|(k, _)| k != "database")
			.cloned()
			.collect();
		params.push(("database".into(), n.dbname.clone()));
		let b = backend::connect(
			&address,
			tls.as_ref(),
			&params,
			ClientCredential::Key(self.secret.clone()),
		)
		.await
		.map_err(|e| WireError::Protocol(format!("{node}: {e}")))?;
		self.conns.insert(
			node,
			Conn {
				reader: FrameReader::new(b.stream),
				pid: b.pid,
				key: b.key,
				prepared: HashMap::new(),
				sets_applied: 0,
				closes_owed: Vec::new(),
				tx_applied: 0,
			},
		);
		Ok(())
	}

	/// A cancel from the client reaches the node this batch runs on.
	fn retarget_cancel(&mut self, node: NodeId) {
		if node == self.cancel_node {
			return;
		}
		let (Some(conn), Some(n)) = (self.conns.get(&node), self.catalog.nodes.get(&node)) else {
			return;
		};
		let address = NodeAddress {
			host: n.host.clone(),
			port: n.port,
			sslmode: n.sslmode,
			ca_file: self.app.config.home.ca_file.clone(),
		};
		let tls = self.tls_of(&address);
		self.app.cancels.retarget(
			self.cancel_pid,
			crate::cancel::Target {
				node: address,
				tls,
				pid: conn.pid,
				key: conn.key.clone(),
			},
		);
		self.cancel_node = node;
	}

	async fn send(&mut self, node: NodeId, msgs: &[Message]) -> Result<(), WireError> {
		let mut bytes = Vec::with_capacity(msgs.iter().map(|m| m.body.len() + 5).sum());
		for m in msgs {
			put_message(&mut bytes, m);
		}
		if !self.conns.contains_key(&node) {
			// A session given back to the pool between transactions: take one again.
			Box::pin(self.ensure_conn(node)).await?;
			Box::pin(self.catch_up_sets(node)).await?;
		}
		let c = self.conns.get_mut(&node).expect("ensured");
		c.reader.get_mut().write_all(&bytes).await?;
		c.reader.get_mut().flush().await?;
		Ok(())
	}

	// -----------------------------------------------------------------------------------------
	// Answers Lepis gives itself.

	/// Answers a batch of transaction control as Postgres would have.
	fn local(&mut self, batch: &[Message], status: u8) -> Result<(), WireError> {
		let mut tag_for_execute = Vec::new();
		for m in batch {
			match m.tag {
				b'Q' => {
					let sql = cstr(&m.body);
					if let Ok(stmts) = &*self.analyze(&sql) {
						for s in stmts {
							let tag = command_tag(s.tx);
							self.out
								.extend_from_slice(&Body::new().cstr(tag).message(b'C').encode());
						}
					}
					self.out
						.extend_from_slice(&Body::new().byte(status).message(b'Z').encode());
				}
				b'P' => {
					let (name, sql) = parse_parse(&m.body)?;
					if let Ok(stmts) = &*self.analyze(&sql) {
						tag_for_execute.push((name, stmts.first().and_then(|s| s.tx)));
					}
					self.out
						.extend_from_slice(&Message::new(b'1', vec![]).encode());
				}
				b'B' => self
					.out
					.extend_from_slice(&Message::new(b'2', vec![]).encode()),
				b'D' => {
					if m.body.first() == Some(&b'S') {
						self.out.extend_from_slice(
							&Body::new()
								.bytes(&0u16.to_be_bytes())
								.message(b't')
								.encode(),
						);
					}
					self.out
						.extend_from_slice(&Message::new(b'n', vec![]).encode());
				}
				b'E' => {
					let verb = tag_for_execute.first().and_then(|(_, v)| *v);
					self.out.extend_from_slice(
						&Body::new().cstr(command_tag(verb)).message(b'C').encode(),
					);
				}
				b'C' => self
					.out
					.extend_from_slice(&Message::new(b'3', vec![]).encode()),
				b'S' => self
					.out
					.extend_from_slice(&Body::new().byte(status).message(b'Z').encode()),
				_ => {}
			}
		}
		if batch.is_empty() {
			self.out
				.extend_from_slice(&Body::new().byte(status).message(b'Z').encode());
		}
		Ok(())
	}

	async fn refuse(&mut self, r: route::Refusal, batch: &[Message]) -> Result<(), WireError> {
		let ends_with_sync = batch.last().is_some_and(|m| matches!(m.tag, b'S' | b'Q'));
		if let Some(node) = self.pinned {
			// The transaction's node raises it, so its transaction is aborted as the client
			// is told.
			let sql = format!(
				"do $lepis$ begin raise exception using errcode = {}, message = {}, hint = {}; end $lepis$",
				crate::catalog::quote_literal(r.code),
				crate::catalog::quote_literal(&r.message),
				crate::catalog::quote_literal(&r.hint),
			);
			let msgs = if batch.first().map(|m| m.tag) == Some(b'Q') {
				vec![wire::query(&sql)]
			} else {
				let mut v = vec![
					Body::new()
						.cstr("")
						.cstr(&sql)
						.bytes(&0u16.to_be_bytes())
						.message(b'P'),
					Body::new()
						.cstr("")
						.cstr("")
						.bytes(&0u16.to_be_bytes())
						.bytes(&0u16.to_be_bytes())
						.bytes(&0u16.to_be_bytes())
						.message(b'B'),
					Body::new().cstr("").i32(0).message(b'E'),
				];
				if ends_with_sync {
					v.push(Message::new(b'S', vec![]));
				} else {
					v.push(Message::new(b'H', vec![]));
				}
				v
			};
			self.send(node, &msgs).await?;
			self.conns
				.get_mut(&node)
				.expect("pinned")
				.prepared
				.remove("");
			if ends_with_sync {
				self.inflight.push_back(InFlight {
					node,
					owner: Owner::ErrorOnly,
					swallow: VecDeque::new(),
					rolls_back: false,
				});
			}
			return Ok(());
		}
		let e = ErrorFields::error(r.code, r.message).with_hint(r.hint);
		self.out.extend_from_slice(&e.message().encode());
		if ends_with_sync {
			let status = self.tx_status_now();
			let status = if status == b'T' { b'E' } else { status };
			self.out
				.extend_from_slice(&Body::new().byte(status).message(b'Z').encode());
		} else {
			self.skip_until_sync = true;
		}
		Ok(())
	}

	// -----------------------------------------------------------------------------------------
	// Statements.

	/// A node address's TLS client configuration (none when it is not built: a cancel then goes
	/// in the clear, as it would have failed).
	fn tls_of(&self, address: &NodeAddress) -> Option<Arc<rustls::ClientConfig>> {
		let key = format!("{address}/{:?}", address.sslmode);
		if let Some(t) = self.tls.borrow().get(&key) {
			return t.clone();
		}
		let t = crate::tls::client_config(address).ok().flatten();
		self.tls.borrow_mut().insert(key, t.clone());
		t
	}

	/// The analysis of `sql` under the session's catalog and search_path, from the cache when it
	/// was made under the same ones.
	fn analyze(&self, sql: &str) -> Arc<Analysed> {
		/// Entries kept; past this the cache starts over (a client making every query unique).
		const MAX: usize = 4096;
		{
			let mut f = self.analysed_for.borrow_mut();
			if f.0 != self.catalog.epoch || f.1 != self.search_path {
				*f = (self.catalog.epoch, self.search_path.clone());
				self.analysed.borrow_mut().clear();
			}
		}
		if let Some(a) = self.analysed.borrow().get(sql) {
			return a.clone();
		}
		let a = Arc::new(analyze::analyze(sql, &self.catalog, &self.search_path));
		let mut cache = self.analysed.borrow_mut();
		if cache.len() >= MAX {
			cache.clear();
		}
		cache.insert(sql.to_string(), a.clone());
		a
	}

	/// For SQL Lepis's parser rejected: asks the home node. Its error, if it has one, is the
	/// statement's error (forwarded as a refusal with the node's own code); if it accepts the
	/// statement, Lepis refuses, since it cannot tell where it belongs.
	async fn unreadable(&mut self, sql: &str) -> Result<Option<route::Refusal>, WireError> {
		self.wait_idle().await?;
		let home = self.home();
		let name = "lepis_syntax_check";
		let msgs = [
			Body::new()
				.cstr(name)
				.cstr(sql)
				.bytes(&0u16.to_be_bytes())
				.message(b'P'),
			Body::new().byte(b'S').cstr(name).message(b'C'),
			Message::new(b'S', vec![]),
		];
		self.send(home, &msgs).await?;
		let mut error: Option<Vec<(u8, String)>> = None;
		let conn = self.conns.get_mut(&home).expect("home");
		loop {
			let m = conn.reader.next().await?;
			match m.tag {
				b'E' => error = Some(wire::parse_error_fields(&m.body)),
				b'Z' => break,
				_ => {}
			}
		}
		Ok(Some(match error {
			Some(fields) => {
				let get = |k: u8| fields.iter().find(|(f, _)| *f == k).map(|(_, v)| v.clone()).unwrap_or_default();
				route::Refusal {
					code: Box::leak(get(b'C').into_boxed_str()),
					message: get(b'M'),
					hint: get(b'H'),
				}
			}
			None => route::Refusal {
				code: route::NOT_ACROSS_NODES,
				message: "Lepis cannot read this statement, so it cannot tell which node it belongs to".into(),
				hint: "Lepis's SQL parser is older than the node's; rewrite the statement without the newest syntax.".into(),
			},
		}))
	}

	async fn refresh_search_path(&mut self) -> Result<(), WireError> {
		self.search_path_dirty = false;
		let node = self.pinned.unwrap_or(self.home());
		let msgs = [wire::query(
			"select array_to_string(current_schemas(false), ',')",
		)];
		self.send(node, &msgs).await?;
		let conn = self.conns.get_mut(&node).expect("ensured");
		conn.prepared.remove("");
		let mut value = None;
		loop {
			let m = conn.reader.next().await?;
			match m.tag {
				b'D' => {
					value = wire::parse_data_row(&m.body)?
						.into_iter()
						.next()
						.flatten()
						.map(|b| String::from_utf8_lossy(&b).into_owned());
				}
				b'Z' => break,
				_ => {}
			}
		}
		if let Some(v) = value {
			self.search_path = v
				.split(',')
				.filter(|s| !s.is_empty())
				.map(str::to_string)
				.collect();
		}
		Ok(())
	}
}

// ---------------------------------------------------------------------------------------------
// Reads across nodes (Phase 2): one statement, every node it needs, one answer.
//
// The statement runs synchronously, with nothing else in flight: Lepis describes it on the home
// node (the client's columns, its parameters' types, and which of its functions are aggregates),
// plans the worker query (scatter.rs), sends the worker to every node at once and reads their
// answers, asks the home node to rank the values only it can order, and combines (merge.rs).
// Nothing reaches the client until the answer is whole, so an error is never preceded by part
// of an answer.

impl Router {
	/// Why a read across nodes cannot run in this batch, if it cannot: it must be alone (a simple
	/// Query, or one Bind and its Execute ending in Sync) and outside a transaction.
	fn gather_refusal(&self, batch: &[Message]) -> Option<route::Refusal> {
		if self.tx_status == b'E' {
			return Some(route::Refusal {
				code: "25P02",
				message: "this transaction has already failed on one of its nodes; nothing more runs in it".into(),
				hint: "End it with ROLLBACK.".into(),
			});
		}
		if self.pinned.is_some() && self.moved_in_tx {
			return Some(route::Refusal {
				code: "40001",
				message:
					"the rows this statement needs moved to another node while the transaction ran"
						.into(),
				hint: "Retry the transaction.".into(),
			});
		}
		if self.pinned.is_some()
			&& let Some(r) = self.join_refusal()
		{
			return Some(r);
		}
		self.shape_refusal(batch)
	}

	/// Why this batch is not one statement up to its Sync (a simple Query, or one Bind and its
	/// Execute), which is what Lepis answers itself from several nodes.
	fn shape_refusal(&self, batch: &[Message]) -> Option<route::Refusal> {
		let alone = || {
			route::Refusal {
			code: route::NOT_ACROSS_NODES,
			message: "a statement for several nodes must be the only one up to its Sync".into(),
			hint: "Send its Parse, Bind, Describe and Execute followed by Sync, with nothing else in between.".into(),
		}
		};
		match batch {
			[m] if m.tag == b'Q' => return None,
			[.., last] if last.tag == b'S' => {}
			_ => return Some(alone()),
		}
		let count = |t: u8| batch.iter().filter(|m| m.tag == t).count();
		if count(b'B') != 1
			|| count(b'E') != 1
			|| count(b'P') > 1
			|| count(b'S') != 1
			|| batch
				.iter()
				.any(|m| !matches!(m.tag, b'P' | b'B' | b'D' | b'E' | b'S'))
		{
			return Some(alone());
		}
		let execute = batch.iter().find(|m| m.tag == b'E')?;
		let (_, rest) = take_cstr(&execute.body).ok()?;
		if rest.get(..4).is_some_and(|b| b != [0, 0, 0, 0]) {
			return Some(route::Refusal {
				code: route::NOT_ACROSS_NODES,
				message:
					"fetching a read across several nodes a few rows at a time is not supported yet"
						.into(),
				hint: "Execute it without a row limit, or pin it to one shard key value.".into(),
			});
		}
		None
	}

	async fn gather(
		&mut self,
		nodes: Vec<NodeId>,
		sql: String,
		batch: Vec<Message>,
	) -> Result<(), WireError> {
		let home = self.home();
		let simple = batch.first().map(|m| m.tag) == Some(b'Q');
		let parse = batch.iter().find(|m| m.tag == b'P').cloned();
		let bind = match batch.iter().find(|m| m.tag == b'B') {
			Some(m) => Some(BindMsg::parse(&m.body)?),
			None => None,
		};
		// The parameter types the client declared: on its Parse, here or earlier.
		let declared = match (&parse, &bind) {
			(Some(p), _) => parse_param_types(&p.body)?,
			(None, Some(b)) => match self.statements.get(&b.stmt) {
				Some(p) => parse_param_types(&p.body)?,
				None => Vec::new(),
			},
			_ => Vec::new(),
		};
		if let Some(p) = &parse {
			// Known to Lepis, held by no node yet: a later Bind parses it where it goes.
			let (name, sql) = parse_parse(&p.body)?;
			self.generation += 1;
			self.statements.insert(
				name.clone(),
				Prepared {
					generation: self.generation,
					types: parse_param_types(&p.body).unwrap_or_default(),
					body: p.body.clone(),
					sql,
				},
			);
			for c in self.conns.values_mut() {
				c.prepared.remove(&name);
			}
		}
		let params = bind.as_ref().map(BindMsg::values).unwrap_or_default();
		let result_formats = bind
			.as_ref()
			.map(|b| b.result_formats.clone())
			.unwrap_or_default();

		let mut all = nodes.clone();
		if !all.contains(&home) {
			all.push(home);
		}
		for n in &all {
			self.ensure_conn(*n).await?;
			self.catch_up_sets(*n).await?;
		}
		// Inside a transaction block the read runs in it, on every node it reads.
		if self.in_tx()
			&& let Some(r) = self.enter_tx(&nodes).await?
		{
			let o = Outcome::Failed {
				at_parse: false,
				error: refusal_message(r.code, &r.message, &r.hint),
			};
			self.answer(&batch, simple, None, o);
			return Ok(());
		}

		// 1. What the statement returns, from the home node.
		let explained = scatter::explained(&sql);
		let functions = if nodes.len() > 1 {
			scatter::functions(&sql)
		} else {
			Vec::new()
		};
		let shown = explained.as_deref().unwrap_or(&sql).to_string();
		let d = match self.describe(home, &shown, &declared, &functions).await? {
			Ok(d) => d,
			Err(e) => {
				self.answer(
					&batch,
					simple,
					None,
					Outcome::Failed {
						at_parse: true,
						error: e,
					},
				);
				return Ok(());
			}
		};
		let described = scatter::Described {
			names: d.fields.iter().map(|f| f.name.clone()).collect(),
			types: d.fields.iter().map(|f| f.oid).collect(),
		};
		let explain = explained.is_some();
		let shown_fields = if explain {
			vec![Field::text("QUERY PLAN")]
		} else {
			d.fields.clone()
		};

		// 2. The worker (unchanged on one node: that is only EXPLAIN), on every node.
		let (worker, formats, planned) = if nodes.len() == 1 {
			(sql.clone(), vec![0i16], None)
		} else {
			// Planned once; if a compared column's type is unknown, the worker is described and
			// planned again, so a collation is asked for only where the type has one.
			let mut col_types: Option<Vec<u32>> = None;
			let mut pass = 0;
			let planned = loop {
				pass += 1;
				let ctx = scatter::Context {
					catalog: &self.catalog,
					search_path: &self.search_path,
					described: Some(&described),
					aggregates: &d.aggregates,
					params: &params,
					param_types: &d.params,
					result_formats: &result_formats,
					col_types: col_types.as_deref(),
					collatable: &self.collatable,
				};
				let p = scatter::plan(&sql, &ctx);
				match p {
					Ok(p) if p.needs_types && pass > 2 => {
						break Err(route::Refusal {
							code: route::NOT_ACROSS_NODES,
							message: "Lepis could not learn the types of the values it would compare across nodes".into(),
							hint: "Pin the query to one shard key value.".into(),
						});
					}
					Ok(p) if p.needs_types => {
						if col_types.is_none() {
							match self.describe(home, &p.worker_sql, &d.params, &[]).await? {
								Ok(w) => col_types = Some(w.fields.iter().map(|f| f.oid).collect()),
								Err(e) => {
									self.answer(
										&batch,
										simple,
										Some((&d, &shown_fields)),
										Outcome::Failed {
											at_parse: false,
											error: e,
										},
									);
									return Ok(());
								}
							}
						}
						let unknown: Vec<u32> = col_types
							.iter()
							.flatten()
							.copied()
							.filter(|t| *t >= 16384 && !self.collatable.contains_key(t))
							.collect();
						if !unknown.is_empty() {
							self.learn_collatable(home, &unknown).await?;
						}
					}
					other => break other,
				}
			};
			match planned {
				Ok(p) => {
					let formats = if p.explain {
						vec![0]
					} else {
						p.formats.clone()
					};
					(p.worker_sql.clone(), formats, Some(p))
				}
				Err(r) => {
					let e = refusal_message(r.code, &r.message, &r.hint);
					self.answer(
						&batch,
						simple,
						Some((&d, &shown_fields)),
						Outcome::Failed {
							at_parse: false,
							error: e,
						},
					);
					return Ok(());
				}
			}
		};
		if let Some(p) = &planned
			&& !explain
		{
			return self
				.gather_stream(
					&nodes,
					p,
					&d,
					&shown_fields,
					bind.as_ref(),
					&result_formats,
					&batch,
					simple,
				)
				.await;
		}
		let answers = match self
			.run_worker(&nodes, &worker, &d.params, bind.as_ref(), &formats)
			.await?
		{
			Ok(a) => a,
			Err(e) => {
				self.answer(
					&batch,
					simple,
					Some((&d, &shown_fields)),
					Outcome::Failed {
						at_parse: false,
						error: e,
					},
				);
				return Ok(());
			}
		};

		// 3. One answer.
		let rows = match &planned {
			_ if explain => Ok(self.explain_rows(&nodes, planned.as_ref(), &answers)),
			Some(p) => self.combine(home, p, &d, &result_formats, &answers).await?,
			None => Ok(answers.into_iter().flat_map(|a| a.rows).collect()),
		};
		let outcome = match rows {
			Ok(rows) => Outcome::Rows(rows, None),
			Err(e) => Outcome::Failed {
				at_parse: false,
				error: refusal_message(e.code, &e.message, e.hint.as_deref().unwrap_or("")),
			},
		};
		self.answer(&batch, simple, Some((&d, &shown_fields)), outcome);
		Ok(())
	}

	/// Whether each of these types (user types, domains) has collations, from the home node.
	async fn learn_collatable(&mut self, home: NodeId, types: &[u32]) -> Result<(), WireError> {
		let list: Vec<String> = types.iter().map(|o| o.to_string()).collect();
		let list = format!("{{{}}}", list.join(","));
		let msgs = [
			parse_message(
				"select oid::int8, (typcollation <> 0)::text from pg_catalog.pg_type where oid = any($1::oid[])",
				&[],
			),
			bind_message(&[0], &[Some(list.into_bytes())], &[]),
			execute_message(),
			Message::new(b'S', vec![]),
		];
		self.send(home, &msgs).await?;
		let conn = self.conns.get_mut(&home).expect("connected");
		conn.prepared.remove("");
		loop {
			let m = conn.reader.next().await?;
			match m.tag {
				b'D' => {
					let r = wire::parse_data_row(&m.body)?;
					let text = |i: usize| {
						r.get(i)
							.cloned()
							.flatten()
							.map(|b| String::from_utf8_lossy(&b).into_owned())
					};
					if let (Some(t), Some(c)) =
						(text(0).and_then(|t| t.parse::<u32>().ok()), text(1))
					{
						self.collatable.insert(t, c == "true");
					}
				}
				b'Z' => return Ok(()),
				_ => {}
			}
		}
	}

	/// Parse + Describe on a node, and (with `functions`) which of them are aggregates.
	async fn describe(
		&mut self,
		node: NodeId,
		sql: &str,
		declared: &[u32],
		functions: &[String],
	) -> Result<Result<DescribeResult, Message>, WireError> {
		let mut msgs = vec![
			parse_message(sql, declared),
			Body::new().byte(b'S').cstr("").message(b'D'),
			Message::new(b'S', vec![]),
		];
		if !functions.is_empty() {
			msgs.push(parse_message(
				"select coalesce(string_agg(distinct proname::text, ','), '') from pg_catalog.pg_proc \
				where prokind = 'a' and proname = any($1::text[])",
				&[],
			));
			msgs.push(bind_message(
				&[0],
				&[Some(text_array(functions.iter().map(|f| f.as_bytes())))],
				&[],
			));
			msgs.push(execute_message());
			msgs.push(Message::new(b'S', vec![]));
		}
		let syncs = msgs.iter().filter(|m| m.tag == b'S').count();
		self.send(node, &msgs).await?;
		let conn = self.conns.get_mut(&node).expect("connected");
		conn.prepared.remove("");
		let mut out = DescribeResult::default();
		let mut error = None;
		let mut seen = 0;
		while seen < syncs {
			let m = conn.reader.next().await?;
			match m.tag {
				b't' => out.params = parse_parameter_description(&m.body)?,
				b'T' => out.fields = Field::parse_all(&m.body)?,
				b'D' => {
					if let Some(Some(v)) = wire::parse_data_row(&m.body)?.into_iter().next() {
						out.aggregates = String::from_utf8_lossy(&v)
							.split(',')
							.filter(|s| !s.is_empty())
							.map(str::to_string)
							.collect();
					}
				}
				b'E' if seen == 0 => error = Some(m),
				b'E' => {
					// The aggregate check failing leaves Lepis unsure what the statement calls.
					error = Some(refusal_message(
						route::NOT_ACROSS_NODES,
						"Lepis could not check which functions of this query are aggregates",
						"Pin the query to one shard key value.",
					));
				}
				b'Z' => seen += 1,
				_ => {}
			}
		}
		Ok(match error {
			Some(e) => Err(e),
			None => Ok(out),
		})
	}

	/// Sends the worker to every node, then reads each answer.
	async fn run_worker(
		&mut self,
		nodes: &[NodeId],
		sql: &str,
		param_types: &[u32],
		bind: Option<&BindMsg>,
		formats: &[i16],
	) -> Result<Result<Vec<Answer>, Message>, WireError> {
		let sqls = vec![sql.to_string(); nodes.len()];
		self.run_each(nodes, &sqls, param_types, bind, formats)
			.await
	}

	/// `run_worker` with each node's own statement.
	async fn run_each(
		&mut self,
		nodes: &[NodeId],
		sqls: &[String],
		param_types: &[u32],
		bind: Option<&BindMsg>,
		formats: &[i16],
	) -> Result<Result<Vec<Answer>, Message>, WireError> {
		let (pformats, values) = match bind {
			Some(b) => (b.param_formats.clone(), b.values.clone()),
			None => (Vec::new(), Vec::new()),
		};
		self.spread_cancel(nodes);
		for (n, sql) in nodes.iter().zip(sqls) {
			let msgs = vec![
				parse_message(sql, param_types),
				bind_message(&pformats, &values, formats),
				Body::new().byte(b'P').cstr("").message(b'D'),
				execute_message(),
				Message::new(b'S', vec![]),
			];
			self.send(*n, &msgs).await?;
			self.conns
				.get_mut(n)
				.expect("connected")
				.prepared
				.remove("");
		}
		// Every node is already working; reading them one after another costs nothing more.
		let mut answers = Vec::with_capacity(nodes.len());
		let mut error = None;
		for n in nodes {
			let conn = self.conns.get_mut(n).expect("connected");
			let mut a = Answer::default();
			loop {
				let m = conn.reader.next().await?;
				match m.tag {
					b'T' => a.fields = Field::parse_all(&m.body)?,
					b'D' => a.rows.push(wire::parse_data_row(&m.body)?),
					b'C' => a.tag = cstr(&m.body),
					b'E' => {
						if error.is_none() {
							error = Some(m);
						}
					}
					b'N' | b'A' | b'S' => put_message(&mut self.out, &m),
					b'Z' => break,
					_ => {}
				}
			}
			answers.push(a);
		}
		self.unspread_cancel();
		Ok(match error {
			Some(e) => Err(e),
			None => Ok(answers),
		})
	}

	/// The client's rows from every node's, ranking on the home node what only it can order.
	async fn combine(
		&mut self,
		home: NodeId,
		planned: &scatter::Planned,
		d: &DescribeResult,
		result_formats: &[i16],
		answers: &[Answer],
	) -> Result<Result<Vec<Row>, MergeError>, WireError> {
		let types: Vec<u32> = answers
			.first()
			.map(|a| a.fields.iter().map(|f| f.oid).collect())
			.unwrap_or_default();
		if answers.iter().any(|a| a.fields.len() != types.len()) {
			return Ok(Err(MergeError {
				code: "XX000",
				message: "the nodes answered with different columns (is the schema the same on every node?)".into(),
				hint: None,
			}));
		}
		let nodes: Vec<Vec<Row>> = answers.iter().map(|a| a.rows.clone()).collect();
		let g = Gathered {
			types: &types,
			formats: &planned.formats,
			nodes: &nodes,
		};
		let requests = match planned.merge.needs_ranks(&g) {
			Ok(r) => r,
			Err(e) => return Ok(Err(e)),
		};
		let ranks = if requests.is_empty() {
			Ranks::new()
		} else {
			match self.rank(home, &requests, true).await? {
				Ok(r) => r,
				Err(e) => return Ok(Err(e)),
			}
		};
		let fmt = |i: usize| match result_formats {
			[] => 0,
			[f] => *f,
			fs => fs.get(i).copied().unwrap_or(0),
		};
		let out: Vec<(u32, i16)> = d
			.fields
			.iter()
			.enumerate()
			.map(|(i, f)| (f.oid, fmt(i)))
			.collect();
		Ok(planned.merge.combine(&g, &ranks, &out))
	}

	/// The home node's `dense_rank()` of each column's values, under the column's collation.
	/// With `sync` false the requests end in Flush, not Sync, so a portal open on the home
	/// session (a streaming read) survives them.
	async fn rank(
		&mut self,
		home: NodeId,
		requests: &[RankRequest],
		sync: bool,
	) -> Result<Result<Ranks, MergeError>, WireError> {
		let end = || Message::new(if sync { b'S' } else { b'H' }, vec![]);
		let unknown: Vec<u32> = requests
			.iter()
			.map(|r| r.oid)
			.filter(|o| array_type(*o).is_none() && !self.array_types.contains_key(o))
			.collect();
		if !unknown.is_empty() {
			let list: Vec<String> = unknown.iter().map(|o| o.to_string()).collect();
			let list = format!("{{{}}}", list.join(","));
			let msgs = [
				parse_message(
					"select oid::int8, typarray::int8 from pg_catalog.pg_type where oid = any($1::oid[])",
					&[],
				),
				bind_message(&[0], &[Some(list.into_bytes())], &[]),
				execute_message(),
				end(),
			];
			self.send(home, &msgs).await?;
			let conn = self.conns.get_mut(&home).expect("connected");
			conn.prepared.remove("");
			loop {
				let m = conn.reader.next().await?;
				match m.tag {
					b'D' => {
						let r = wire::parse_data_row(&m.body)?;
						let num = |i: usize| {
							r.get(i)
								.cloned()
								.flatten()
								.and_then(|b| String::from_utf8_lossy(&b).parse::<u32>().ok())
						};
						if let (Some(t), Some(a)) = (num(0), num(1)) {
							self.array_types.insert(t, a);
						}
					}
					b'C' | b'E' if !sync => break,
					b'Z' => break,
					_ => {}
				}
			}
		}
		let mut msgs = Vec::new();
		for r in requests {
			let Some(array) = array_type(r.oid)
				.or_else(|| self.array_types.get(&r.oid).copied())
				.filter(|a| *a != 0)
			else {
				return Ok(Err(MergeError {
					code: route::NOT_ACROSS_NODES,
					message: format!("Lepis cannot order values of type {} across nodes", r.oid),
					hint: Some("Pin the query to one shard key value.".into()),
				}));
			};
			let collate = r
				.collation
				.as_ref()
				.map(|c| format!(" collate {c}"))
				.unwrap_or_default();
			msgs.push(parse_message(
				&format!(
					"select o, dense_rank() over (order by v{collate}) from unnest($1) with ordinality as t(v, o)"
				),
				&[array],
			));
			let value = if r.format == 1 {
				binary_array(r.oid, &r.values)
			} else {
				text_array(r.values.iter().map(Vec::as_slice))
			};
			msgs.push(bind_message(&[r.format], &[Some(value)], &[]));
			msgs.push(execute_message());
		}
		msgs.push(end());
		self.send(home, &msgs).await?;
		let conn = self.conns.get_mut(&home).expect("connected");
		conn.prepared.remove("");
		let mut ranks = Ranks::new();
		let mut at = 0usize;
		let mut error = None;
		loop {
			let m = conn.reader.next().await?;
			match m.tag {
				b'D' => {
					let r = wire::parse_data_row(&m.body)?;
					let num = |i: usize| {
						r.get(i)
							.cloned()
							.flatten()
							.and_then(|b| String::from_utf8_lossy(&b).parse::<i64>().ok())
					};
					if let (Some(req), Some(o), Some(rank)) = (requests.get(at), num(0), num(1))
						&& let Some(v) = req.values.get((o - 1) as usize)
					{
						ranks.entry(req.col).or_default().insert(v.clone(), rank);
					}
				}
				b'C' => {
					at += 1;
					if !sync && at == requests.len() {
						break;
					}
				}
				b'E' => {
					let fields = wire::parse_error_fields(&m.body);
					let text = fields
						.iter()
						.find(|(k, _)| *k == b'M')
						.map(|(_, v)| v.clone())
						.unwrap_or_default();
					error = Some(MergeError {
						code: route::NOT_ACROSS_NODES,
						message: format!(
							"the home node could not order the values Lepis merges: {text}"
						),
						hint: Some("Pin the query to one shard key value.".into()),
					});
					if !sync {
						break;
					}
				}
				b'Z' => break,
				_ => {}
			}
		}
		Ok(match error {
			Some(e) => Err(e),
			None => Ok(ranks),
		})
	}

	/// EXPLAIN's answer: Lepis's routing line, then each node's plan under its name.
	fn explain_rows(
		&self,
		nodes: &[NodeId],
		planned: Option<&scatter::Planned>,
		answers: &[Answer],
	) -> Vec<Row> {
		let name = |n: &NodeId| {
			self.catalog
				.nodes
				.get(n)
				.map(|x| x.name.clone())
				.unwrap_or_else(|| n.to_string())
		};
		let mut lines = vec![match planned {
			None => format!(
				"Lepis: one node, {} (the shard key picks it); the statement runs there unchanged",
				nodes.first().map(name).unwrap_or_default()
			),
			Some(p) => format!(
				"Lepis: {} nodes ({}); {}",
				nodes.len(),
				nodes.iter().map(name).collect::<Vec<_>>().join(", "),
				p.summary
			),
		}];
		if let Some(p) = planned {
			lines.push(format!("Lepis: each node runs: {}", p.worker_select));
		}
		for (n, a) in nodes.iter().zip(answers) {
			lines.push(format!("Node {}:", name(n)));
			for r in &a.rows {
				if let Some(Some(b)) = r.first() {
					lines.push(format!("  {}", String::from_utf8_lossy(b)));
				}
			}
		}
		lines
			.into_iter()
			.map(|l| vec![Some(l.into_bytes())])
			.collect()
	}

	/// Answers the batch as Postgres would have, message by message.
	fn answer(
		&mut self,
		batch: &[Message],
		simple: bool,
		described: Option<(&DescribeResult, &[Field])>,
		outcome: Outcome,
	) {
		if matches!(outcome, Outcome::Failed { .. })
			&& (self.pinned.is_some() || !self.participants.is_empty())
		{
			// The client's transaction is aborted, as one Postgres would abort it.
			self.tx_status = b'E';
		}
		let ready = Body::new().byte(self.tx_status_now()).message(b'Z');
		if simple {
			match outcome {
				Outcome::Failed { error, .. } => self.out.extend_from_slice(&error.encode()),
				Outcome::Rows(rows, tag) => {
					if let Some((_, fields)) = described
						&& !fields.is_empty()
					{
						self.out
							.extend_from_slice(&Field::message(fields, |_| 0).encode());
					}
					self.rows_out(&rows, tag.as_deref());
				}
			}
			self.out.extend_from_slice(&ready.encode());
			return;
		}
		let formats = batch
			.iter()
			.find(|m| m.tag == b'B')
			.and_then(|m| BindMsg::parse(&m.body).ok())
			.map(|b| b.result_formats)
			.unwrap_or_default();
		let (failed_at_parse, error, rows) = match outcome {
			Outcome::Failed { at_parse, error } => (at_parse, Some(error), None),
			Outcome::Rows(r, tag) => (false, None, Some((r, tag))),
		};
		let mut failed = false;
		for m in batch {
			if failed && m.tag != b'S' {
				continue;
			}
			match m.tag {
				b'P' | b'B' if failed_at_parse => {
					if let Some(e) = &error {
						self.out.extend_from_slice(&e.encode());
					}
					failed = true;
				}
				b'P' => self
					.out
					.extend_from_slice(&Message::new(b'1', vec![]).encode()),
				b'B' => self
					.out
					.extend_from_slice(&Message::new(b'2', vec![]).encode()),
				b'D' => {
					let Some((d, fields)) = described else {
						continue;
					};
					if m.body.first() == Some(&b'S') {
						self.out
							.extend_from_slice(&parameter_description(&d.params).encode());
					}
					if fields.is_empty() {
						self.out
							.extend_from_slice(&Message::new(b'n', vec![]).encode());
					} else if m.body.first() == Some(&b'S') {
						self.out
							.extend_from_slice(&Field::message(fields, |_| 0).encode());
					} else {
						let fmt = |i: usize| match formats.as_slice() {
							[] => 0,
							[f] => *f,
							fs => fs.get(i).copied().unwrap_or(0),
						};
						self.out
							.extend_from_slice(&Field::message(fields, fmt).encode());
					}
				}
				b'E' => match (&error, &rows) {
					(Some(e), _) => {
						self.out.extend_from_slice(&e.encode());
						failed = true;
					}
					(None, Some((r, tag))) => {
						let (r, tag) = (r.clone(), tag.clone());
						self.rows_out(&r, tag.as_deref());
					}
					_ => {}
				},
				b'S' => self.out.extend_from_slice(&ready.encode()),
				_ => {}
			}
		}
	}

	/// The rows and their CommandComplete (`SELECT n` unless `tag` says otherwise).
	fn rows_out(&mut self, rows: &[Row], tag: Option<&str>) {
		for r in rows {
			self.out.extend_from_slice(&data_row(r).encode());
		}
		let tag = match tag {
			Some(t) => t.to_string(),
			None => format!("SELECT {}", rows.len()),
		};
		self.out
			.extend_from_slice(&Body::new().cstr(&tag).message(b'C').encode());
	}
}

/// What the home node said about a statement.
#[derive(Default)]
struct DescribeResult {
	params: Vec<u32>,
	fields: Vec<Field>,
	aggregates: Vec<String>,
}

/// One node's answer to the worker.
#[derive(Default)]
struct Answer {
	fields: Vec<Field>,
	rows: Vec<Row>,
	/// Its CommandComplete.
	tag: String,
}

enum Outcome {
	/// The rows, and the command tag when it is not `SELECT n`.
	Rows(Vec<Row>, Option<String>),
	/// An ErrorResponse to give; `at_parse` when the statement itself is invalid.
	Failed { at_parse: bool, error: Message },
}

fn refusal_message(code: &'static str, message: &str, hint: &str) -> Message {
	let mut e = ErrorFields::error(code, message);
	if !hint.is_empty() {
		e = e.with_hint(hint);
	}
	e.message()
}

/// One column of a RowDescription.
#[derive(Clone, Debug)]
struct Field {
	name: String,
	table: i32,
	attnum: i16,
	oid: u32,
	typlen: i16,
	typmod: i32,
}

impl Field {
	fn text(name: &str) -> Field {
		Field {
			name: name.into(),
			table: 0,
			attnum: 0,
			oid: merge::TEXT,
			typlen: -1,
			typmod: -1,
		}
	}

	fn parse_all(body: &[u8]) -> Result<Vec<Field>, WireError> {
		let bad = || WireError::Protocol("malformed RowDescription".into());
		let n = body
			.get(..2)
			.map(|b| u16::from_be_bytes([b[0], b[1]]))
			.ok_or_else(bad)? as usize;
		let mut r = &body[2..];
		let mut out = Vec::with_capacity(n);
		for _ in 0..n {
			let (name, rest) = take_cstr(r)?;
			let f = rest.get(..18).ok_or_else(bad)?;
			out.push(Field {
				name,
				table: i32::from_be_bytes([f[0], f[1], f[2], f[3]]),
				attnum: i16::from_be_bytes([f[4], f[5]]),
				oid: u32::from_be_bytes([f[6], f[7], f[8], f[9]]),
				typlen: i16::from_be_bytes([f[10], f[11]]),
				typmod: i32::from_be_bytes([f[12], f[13], f[14], f[15]]),
			});
			r = &rest[18..];
		}
		Ok(out)
	}

	fn message(fields: &[Field], format: impl Fn(usize) -> i16) -> Message {
		let mut b = Body::new().bytes(&(fields.len() as u16).to_be_bytes());
		for (i, f) in fields.iter().enumerate() {
			b = b
				.cstr(&f.name)
				.i32(f.table)
				.bytes(&f.attnum.to_be_bytes())
				.u32(f.oid)
				.bytes(&f.typlen.to_be_bytes())
				.i32(f.typmod)
				.bytes(&format(i).to_be_bytes());
		}
		b.message(b'T')
	}
}

/// A Bind as the client sent it.
#[derive(Clone)]
struct BindMsg {
	stmt: String,
	param_formats: Vec<i16>,
	values: Vec<Option<Vec<u8>>>,
	result_formats: Vec<i16>,
}

impl BindMsg {
	fn parse(body: &[u8]) -> Result<BindMsg, WireError> {
		let bad = || WireError::Protocol("malformed Bind".into());
		let (_, r) = take_cstr(body)?;
		let (stmt, mut r) = take_cstr(r)?;
		let i16s = |r: &mut &[u8]| -> Result<Vec<i16>, WireError> {
			let n = r
				.get(..2)
				.map(|b| u16::from_be_bytes([b[0], b[1]]))
				.ok_or_else(bad)? as usize;
			let mut v = Vec::with_capacity(n);
			for i in 0..n {
				let b = r.get(2 + i * 2..4 + i * 2).ok_or_else(bad)?;
				v.push(i16::from_be_bytes([b[0], b[1]]));
			}
			*r = &r[2 + n * 2..];
			Ok(v)
		};
		let param_formats = i16s(&mut r)?;
		let n = r
			.get(..2)
			.map(|b| u16::from_be_bytes([b[0], b[1]]))
			.ok_or_else(bad)? as usize;
		r = &r[2..];
		let mut values = Vec::with_capacity(n);
		for _ in 0..n {
			let len = r
				.get(..4)
				.map(|b| i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
				.ok_or_else(bad)?;
			r = &r[4..];
			if len < 0 {
				values.push(None);
			} else {
				values.push(Some(r.get(..len as usize).ok_or_else(bad)?.to_vec()));
				r = &r[len as usize..];
			}
		}
		let result_formats = i16s(&mut r)?;
		Ok(BindMsg {
			stmt,
			param_formats,
			values,
			result_formats,
		})
	}

	fn values(&self) -> Vec<ParamValue> {
		self.values
			.iter()
			.enumerate()
			.map(|(i, v)| {
				let format = match self.param_formats.as_slice() {
					[] => 0,
					[f] => *f,
					fs => fs.get(i).copied().unwrap_or(0),
				};
				match v {
					None => ParamValue::Null,
					Some(b) if format == 1 => ParamValue::Binary(b.clone()),
					Some(b) => ParamValue::Text(String::from_utf8_lossy(b).into_owned()),
				}
			})
			.collect()
	}
}

/// The parameter types a Parse declares.
fn parse_param_types(body: &[u8]) -> Result<Vec<u32>, WireError> {
	let (_, r) = take_cstr(body)?;
	let (_, r) = take_cstr(r)?;
	let Some(n) = r
		.get(..2)
		.map(|b| u16::from_be_bytes([b[0], b[1]]) as usize)
	else {
		return Ok(Vec::new());
	};
	(0..n)
		.map(|i| {
			r.get(2 + i * 4..6 + i * 4)
				.map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
				.ok_or_else(|| WireError::Protocol("malformed Parse".into()))
		})
		.collect()
}

fn parse_parameter_description(body: &[u8]) -> Result<Vec<u32>, WireError> {
	let bad = || WireError::Protocol("malformed ParameterDescription".into());
	let n = body
		.get(..2)
		.map(|b| u16::from_be_bytes([b[0], b[1]]))
		.ok_or_else(bad)? as usize;
	(0..n)
		.map(|i| {
			body.get(2 + i * 4..6 + i * 4)
				.map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
				.ok_or_else(bad)
		})
		.collect()
}

fn parameter_description(types: &[u32]) -> Message {
	let mut b = Body::new().bytes(&(types.len() as u16).to_be_bytes());
	for t in types {
		b = b.u32(*t);
	}
	b.message(b't')
}

fn parse_message(sql: &str, types: &[u32]) -> Message {
	let mut b = Body::new()
		.cstr("")
		.cstr(sql)
		.bytes(&(types.len() as u16).to_be_bytes());
	for t in types {
		b = b.u32(*t);
	}
	b.message(b'P')
}

fn bind_message(
	param_formats: &[i16],
	values: &[Option<Vec<u8>>],
	result_formats: &[i16],
) -> Message {
	let mut b = Body::new()
		.cstr("")
		.cstr("")
		.bytes(&(param_formats.len() as u16).to_be_bytes());
	for f in param_formats {
		b = b.bytes(&f.to_be_bytes());
	}
	b = b.bytes(&(values.len() as u16).to_be_bytes());
	for v in values {
		b = match v {
			None => b.i32(-1),
			Some(v) => b.i32(v.len() as i32).bytes(v),
		};
	}
	b = b.bytes(&(result_formats.len() as u16).to_be_bytes());
	for f in result_formats {
		b = b.bytes(&f.to_be_bytes());
	}
	b.message(b'B')
}

fn execute_message() -> Message {
	Body::new().cstr("").i32(0).message(b'E')
}

fn data_row(cells: &[Option<Vec<u8>>]) -> Message {
	let mut b = Body::new().bytes(&(cells.len() as u16).to_be_bytes());
	for c in cells {
		b = match c {
			None => b.i32(-1),
			Some(v) => b.i32(v.len() as i32).bytes(v),
		};
	}
	b.message(b'D')
}

/// An array's text form, every element quoted.
fn text_array<'a>(values: impl Iterator<Item = &'a [u8]>) -> Vec<u8> {
	let mut out = vec![b'{'];
	for (i, v) in values.enumerate() {
		if i > 0 {
			out.push(b',');
		}
		out.push(b'"');
		for &c in v {
			if c == b'"' || c == b'\\' {
				out.push(b'\\');
			}
			out.push(c);
		}
		out.push(b'"');
	}
	out.push(b'}');
	out
}

/// A one-dimensional array's binary form, of values already in their binary form.
fn binary_array(element: u32, values: &[Vec<u8>]) -> Vec<u8> {
	let mut b = Body::new()
		.i32(1)
		.i32(0)
		.u32(element)
		.i32(values.len() as i32)
		.i32(1);
	for v in values {
		b = b.i32(v.len() as i32).bytes(v);
	}
	b.finish()
}

/// The array types of the built-in types Lepis ranks most (fixed OIDs since Postgres 8).
fn array_type(oid: u32) -> Option<u32> {
	Some(match oid {
		16 => 1000,
		17 => 1001,
		18 => 1002,
		19 => 1003,
		20 => 1016,
		21 => 1005,
		23 => 1007,
		25 => 1009,
		26 => 1028,
		700 => 1021,
		701 => 1022,
		790 => 791,
		869 => 1041,
		1042 => 1014,
		1043 => 1015,
		1082 => 1182,
		1083 => 1183,
		1114 => 1115,
		1184 => 1185,
		1186 => 1187,
		1266 => 1270,
		1700 => 1231,
		2950 => 2951,
		3802 => 3807,
		_ => return None,
	})
}

// ---------------------------------------------------------------------------------------------
// Transactions across nodes (Phase 3).
//
// A transaction starts on the node of its first statement, as in Phase 1. A later statement for
// another node JOINS that node: the transaction's BEGIN (with its isolation level and modes) and
// its SET LOCALs are replayed there first, and the node becomes a participant. COMMIT of a
// transaction with several participants is two-phase (twopc.rs): every participant prepares, the
// decision is logged on the home node, then each commits; ROLLBACK rolls back each. What cannot
// be made correct is refused before anything is sent: a SERIALIZABLE transaction across nodes
// (each node would check only its own part), savepoints across nodes (ROLLBACK TO would undo one
// node's part only), and COMMIT AND CHAIN.

/// One participant: a node's session of this client, as twopc.rs drives it.
struct Part<'a> {
	node: NodeId,
	conn: &'a mut Conn,
}

impl twopc::Participant for Part<'_> {
	fn node(&self) -> NodeId {
		self.node
	}

	async fn execute(&mut self, sql: &str) -> Result<String, BackendError> {
		self.conn
			.reader
			.get_mut()
			.write_all(&wire::query(sql).encode())
			.await
			.map_err(|e| BackendError::Unreachable(e.to_string()))?;
		self.conn
			.reader
			.get_mut()
			.flush()
			.await
			.map_err(|e| BackendError::Unreachable(e.to_string()))?;
		self.conn.prepared.remove("");
		let mut tag = String::new();
		let mut error = None;
		loop {
			let m = self
				.conn
				.reader
				.next()
				.await
				.map_err(|e| BackendError::Unreachable(e.to_string()))?;
			match m.tag {
				b'C' => tag = cstr(&m.body),
				b'E' => error = Some(m),
				b'Z' => break,
				_ => {}
			}
		}
		match error {
			Some(e) => Err(BackendError::Refused(e)),
			None => Ok(tag),
		}
	}
}

impl Router {
	/// This session's two-phase coordinator (its log connections live as long as the session).
	fn coordinator(&mut self) -> Arc<twopc::Coordinator> {
		let app = self.app.clone();
		self.coordinator
			.get_or_insert_with(|| {
				Arc::new(twopc::Coordinator::new(&app, twopc::Settings::default()))
			})
			.clone()
	}

	/// Brings every one of `nodes` into the client's open transaction (or the one its BEGIN
	/// opened and no node has seen yet).
	async fn enter_tx(&mut self, nodes: &[NodeId]) -> Result<Option<route::Refusal>, WireError> {
		if let Some(b) = self.unbound_begin.take() {
			self.tx_begin = Some(b);
		}
		for n in nodes {
			if self.participants.contains(n) {
				self.catch_up_tx(*n).await?;
			} else if let Some(r) = self.join(*n).await? {
				return Ok(Some(r));
			}
		}
		if self.pinned.is_none() {
			self.pinned = nodes.first().copied();
			self.tx_status = b'T';
		}
		Ok(None)
	}

	/// Whether the client is in a transaction block (one a node has seen or not).
	fn in_tx(&self) -> bool {
		self.pinned.is_some() || self.unbound_begin.is_some() || self.tx_status != b'I'
	}

	/// Why the open transaction may not take in another node, if it may not.
	fn join_refusal(&self) -> Option<route::Refusal> {
		if self.tx_savepoint {
			return Some(route::Refusal {
				code: route::NOT_ACROSS_NODES,
				message: "a transaction with savepoints cannot take in another node".into(),
				hint: "Use savepoints only in a transaction that stays on one node, or keep the transaction to one shard key value.".into(),
			});
		}
		None
	}

	/// Opens the transaction on another node: its BEGIN and SET LOCALs, then checks that the
	/// isolation level is one a transaction across nodes keeps.
	async fn join(&mut self, node: NodeId) -> Result<Option<route::Refusal>, WireError> {
		self.ensure_conn(node).await?;
		self.catch_up_sets(node).await?;
		let begin = self.tx_begin.clone().unwrap_or_else(|| "BEGIN".into());
		let mut sql = begin;
		for s in &self.tx_replay {
			sql.push_str(";\n");
			sql.push_str(s);
		}
		sql.push_str(";\nselect current_setting('transaction_isolation')");
		self.send(node, &[wire::query(&sql)]).await?;
		let conn = self.conns.get_mut(&node).expect("ensured");
		conn.prepared.remove("");
		let mut isolation = None;
		let mut error = None;
		let status;
		loop {
			let m = conn.reader.next().await?;
			match m.tag {
				b'D' => {
					isolation = wire::parse_data_row(&m.body)?
						.into_iter()
						.next()
						.flatten()
						.map(|b| String::from_utf8_lossy(&b).into_owned());
				}
				b'E' => error = Some(wire::parse_error_fields(&m.body)),
				b'Z' => {
					status = m.body.first().copied().unwrap_or(b'I');
					break;
				}
				_ => {}
			}
		}
		let refusal = if let Some(f) = error {
			let get = |k: u8| {
				f.iter()
					.find(|(x, _)| *x == k)
					.map(|(_, v)| v.clone())
					.unwrap_or_default()
			};
			Some(route::Refusal {
				code: route::NOT_ACROSS_NODES,
				message: format!(
					"the transaction could not be opened on another node: {}",
					get(b'M')
				),
				hint: "Keep the transaction to the shard key values of one node.".into(),
			})
		} else if isolation.as_deref() == Some("serializable") {
			Some(route::Refusal {
				code: route::NOT_ACROSS_NODES,
				message: "a SERIALIZABLE transaction cannot span several nodes: each node would check only its own part".into(),
				hint: "Use REPEATABLE READ or READ COMMITTED for a transaction across nodes, or keep it to the shard key values of one node.".into(),
			})
		} else {
			None
		};
		if refusal.is_some() {
			if status != b'I' {
				let _ = Part {
					node,
					conn: self.conns.get_mut(&node).expect("ensured"),
				}
				.execute_rollback()
				.await;
			}
			return Ok(refusal);
		}
		let n = self.tx_replay.len();
		self.conns.get_mut(&node).expect("ensured").tx_applied = n;
		if !self.participants.contains(&node) {
			self.participants.push(node);
		}
		Ok(None)
	}

	/// Replays the transaction's SETs a participant has not run yet.
	async fn catch_up_tx(&mut self, node: NodeId) -> Result<(), WireError> {
		let applied = self.conns.get(&node).map_or(0, |c| c.tx_applied);
		if self.participants.len() < 2 || applied >= self.tx_replay.len() {
			return Ok(());
		}
		let sql = self.tx_replay[applied..].join(";\n");
		self.internal(node, &sql).await?;
		let n = self.tx_replay.len();
		self.conns.get_mut(&node).expect("ensured").tx_applied = n;
		Ok(())
	}

	/// COMMIT or ROLLBACK of a transaction on several nodes, answered by Lepis.
	async fn end_tx(&mut self, commit: bool, batch: &[Message]) -> Result<(), WireError> {
		let failed = self.tx_status == b'E';
		let participants = self.participants.clone();
		let coordinator = self.coordinator();
		let mut parts: Vec<Part> = self
			.conns
			.iter_mut()
			.filter(|(id, _)| participants.contains(id))
			.map(|(id, conn)| Part { node: *id, conn })
			.collect();
		let (tag, error) = if commit && !failed {
			match coordinator.commit(&mut parts).await {
				Ok(_) => ("COMMIT", None),
				Err(e) => ("ROLLBACK", Some(e.to_message())),
			}
		} else {
			for p in parts.iter_mut() {
				let _ = p.execute_rollback().await;
			}
			("ROLLBACK", None)
		};
		let committed = tag == "COMMIT";
		// The transaction is over everywhere: its SETs stand if it committed (each node is
		// brought up to date before it runs anything else).
		if committed {
			self.sets.append(&mut self.pending_sets);
		} else {
			self.pending_sets.clear();
		}
		self.end_tx_state();
		self.tx_status = b'I';
		self.copy_roles().await?;
		// The answer, as Postgres gives it to a COMMIT or ROLLBACK.
		let ready = Body::new().byte(b'I').message(b'Z');
		let complete = Body::new().cstr(tag).message(b'C');
		for m in batch {
			match m.tag {
				b'Q' => {
					match &error {
						Some(e) => self.out.extend_from_slice(&e.encode()),
						None => self.out.extend_from_slice(&complete.encode()),
					}
					self.out.extend_from_slice(&ready.encode());
				}
				b'P' => self
					.out
					.extend_from_slice(&Message::new(b'1', vec![]).encode()),
				b'B' => self
					.out
					.extend_from_slice(&Message::new(b'2', vec![]).encode()),
				b'D' => self
					.out
					.extend_from_slice(&Message::new(b'n', vec![]).encode()),
				b'E' => match &error {
					Some(e) => self.out.extend_from_slice(&e.encode()),
					None => self.out.extend_from_slice(&complete.encode()),
				},
				b'S' => self.out.extend_from_slice(&ready.encode()),
				_ => {}
			}
		}
		Ok(())
	}

	/// Forgets everything that belonged to the transaction that just ended.
	fn end_tx_state(&mut self) {
		if self.claim.as_ref().is_some_and(|c| c.local) {
			self.claim = None;
		}
		self.participants.clear();
		self.tx_begin = None;
		self.tx_replay.clear();
		self.tx_savepoint = false;
		self.pinned = None;
		self.unbound_begin = None;
		self.moved_in_tx = false;
		self.portals.clear();
		for c in self.conns.values_mut() {
			c.tx_applied = 0;
		}
	}
}

impl Part<'_> {
	async fn execute_rollback(&mut self) -> Result<String, BackendError> {
		use crate::twopc::Participant;
		self.execute("rollback").await
	}
}

// ---------------------------------------------------------------------------------------------
// Writes several nodes run (Phase 3): an UPDATE or DELETE of a sharded table, each node on its
// own rows, and a write to a reference table, the same on every copy. Inside the client's
// transaction each node joins it; outside one, each node runs it in a transaction of its own and
// the nodes commit through two-phase commit, so the write happens on every node or on none.

impl Router {
	async fn fanout(&mut self, f: Fanout, batch: Vec<Message>) -> Result<(), WireError> {
		let home = self.home();
		let simple = batch.first().map(|m| m.tag) == Some(b'Q');
		let parse = batch.iter().find(|m| m.tag == b'P').cloned();
		let bind = match batch.iter().find(|m| m.tag == b'B') {
			Some(m) => Some(BindMsg::parse(&m.body)?),
			None => None,
		};
		let declared = match (&parse, &bind) {
			(Some(p), _) => parse_param_types(&p.body)?,
			(None, Some(b)) => match self.statements.get(&b.stmt) {
				Some(p) => parse_param_types(&p.body)?,
				None => Vec::new(),
			},
			_ => Vec::new(),
		};
		if let Some(p) = &parse {
			let (name, sql) = parse_parse(&p.body)?;
			self.generation += 1;
			self.statements.insert(
				name.clone(),
				Prepared {
					generation: self.generation,
					types: parse_param_types(&p.body).unwrap_or_default(),
					body: p.body.clone(),
					sql,
				},
			);
			for c in self.conns.values_mut() {
				c.prepared.remove(&name);
			}
		}
		let result_formats = bind
			.as_ref()
			.map(|b| b.result_formats.clone())
			.unwrap_or_default();
		let mut all = f.nodes.clone();
		if !all.contains(&home) {
			all.push(home);
		}
		for n in &all {
			self.ensure_conn(*n).await?;
			self.catch_up_sets(*n).await?;
		}
		let refuse = |r: route::Refusal| Outcome::Failed {
			at_parse: false,
			error: refusal_message(r.code, &r.message, &r.hint),
		};
		if !f.ddl
			&& let Some(r) = self.sequence_refusal(&f.sql).await?
		{
			self.answer(&batch, simple, None, refuse(r));
			return Ok(());
		}
		if f.same_count
			&& let Some(r) = self.copies_refusal(&f).await?
		{
			self.answer(&batch, simple, None, refuse(r));
			return Ok(());
		}
		let d = match self.describe(home, &f.sql, &declared, &[]).await? {
			Ok(d) => d,
			Err(e) => {
				self.answer(
					&batch,
					simple,
					None,
					Outcome::Failed {
						at_parse: true,
						error: e,
					},
				);
				return Ok(());
			}
		};
		let fields = d.fields.clone();

		// Every node in a transaction: the client's, or one of the write's own.
		let implicit = self.pinned.is_none() && self.unbound_begin.is_none();
		if implicit {
			for n in &f.nodes {
				let conn = self.conns.get_mut(n).expect("connected");
				let r = Part { node: *n, conn }.execute_sql("begin").await;
				if let Err(e) = r {
					self.rollback_nodes(&f.nodes).await;
					let error = match e {
						BackendError::Refused(m) => m,
						e => refusal_message("08006", &format!("{n}: {e}"), ""),
					};
					self.answer(
						&batch,
						simple,
						Some((&d, &fields)),
						Outcome::Failed {
							at_parse: false,
							error,
						},
					);
					return Ok(());
				}
			}
		} else if let Some(r) = self.enter_tx(&f.nodes).await? {
			self.answer(&batch, simple, Some((&d, &fields)), refuse(r));
			return Ok(());
		}

		let sqls = f
			.per_node
			.clone()
			.unwrap_or_else(|| vec![f.sql.clone(); f.nodes.len()]);
		let answers = self
			.run_each(&f.nodes, &sqls, &d.params, bind.as_ref(), &result_formats)
			.await?;
		let fail = |this: &mut Self, error: Message| Outcome::Failed {
			at_parse: false,
			error: {
				if !implicit {
					// The client's transaction is aborted, as one Postgres would abort it.
					this.tx_status = b'E';
				}
				error
			},
		};
		let answers = match answers {
			Ok(a) => a,
			Err(e) => {
				if implicit {
					self.rollback_nodes(&f.nodes).await;
				}
				let o = fail(self, e);
				self.answer(&batch, simple, Some((&d, &fields)), o);
				return Ok(());
			}
		};
		if f.same_count && answers.windows(2).any(|w| w[0].tag != w[1].tag) {
			if implicit {
				self.rollback_nodes(&f.nodes).await;
			}
			let tags: Vec<String> = f
				.nodes
				.iter()
				.zip(&answers)
				.map(|(n, a)| format!("{n} answered {}", a.tag))
				.collect();
			let e = refusal_message(
				"XX001",
				&format!(
					"the copies of a reference table differ, so the write was not made: {}",
					tags.join(", ")
				),
				"Bring the copies back in line (they must hold the same rows), then write again.",
			);
			let o = fail(self, e);
			self.answer(&batch, simple, Some((&d, &fields)), o);
			return Ok(());
		}
		let tag = if f.same_count {
			answers.first().map(|a| a.tag.clone()).unwrap_or_default()
		} else {
			sum_tags(answers.iter().map(|a| a.tag.as_str()))
		};
		// A split INSERT's RETURNING rows go back to the order of its VALUES: node by node, a
		// node's rows are its row numbers in order. A node that returned another number of rows
		// (a trigger skipped one) leaves no order to restore, and the insert is undone.
		let reordered = match (&f.rows, fields.is_empty()) {
			(Some(order), false) => {
				let fits = answers
					.iter()
					.zip(order)
					.all(|(a, o)| a.rows.len() == o.len());
				if !fits {
					if implicit {
						self.rollback_nodes(&f.nodes).await;
					}
					let e = refusal_message(
						route::NOT_ACROSS_NODES,
						"an INSERT across nodes returned a different number of rows than it inserted, so RETURNING cannot be put in order",
						"Insert the rows of each shard key value in a statement of their own.",
					);
					let o = fail(self, e);
					self.answer(&batch, simple, Some((&d, &fields)), o);
					return Ok(());
				}
				let mut slots: Vec<(usize, Row)> = Vec::new();
				for (a, o) in answers.iter().zip(order) {
					slots.extend(o.iter().copied().zip(a.rows.iter().cloned()));
				}
				slots.sort_by_key(|(i, _)| *i);
				Some(slots.into_iter().map(|(_, r)| r).collect::<Vec<Row>>())
			}
			_ => None,
		};
		let rows: Vec<Row> = if let Some(r) = reordered {
			r
		} else if f.same_count {
			answers
				.into_iter()
				.next()
				.map(|a| a.rows)
				.unwrap_or_default()
		} else {
			answers.into_iter().flat_map(|a| a.rows).collect()
		};
		if implicit {
			let coordinator = self.coordinator();
			let nodes = f.nodes.clone();
			let mut parts: Vec<Part> = self
				.conns
				.iter_mut()
				.filter(|(id, _)| nodes.contains(id))
				.map(|(id, conn)| Part { node: *id, conn })
				.collect();
			if let Err(e) = coordinator.commit(&mut parts).await {
				self.answer(
					&batch,
					simple,
					Some((&d, &fields)),
					Outcome::Failed {
						at_parse: false,
						error: e.to_message(),
					},
				);
				return Ok(());
			}
		}
		self.answer(
			&batch,
			simple,
			Some((&d, &fields)),
			Outcome::Rows(rows, Some(tag)),
		);
		Ok(())
	}

	async fn rollback_nodes(&mut self, nodes: &[NodeId]) {
		for n in nodes {
			if let Some(conn) = self.conns.get_mut(n) {
				let _ = Part { node: *n, conn }.execute_sql("rollback").await;
			}
		}
	}

	/// L15 for a statement routed off the home node: `nextval` of a sequence that is not
	/// striped by node would hand out values another node hands out too.
	async fn sequence_refusal(&mut self, sql: &str) -> Result<Option<route::Refusal>, WireError> {
		if crate::ddl::sequence_calls(sql).is_empty() {
			return Ok(None);
		}
		let striped: Vec<(String, String)> = match twopc::home_service(&self.app, "sequences").await
		{
			Ok(mut b) => {
				let rows = b
					.query(crate::ddl::LOAD_STRIPED_SQL, &[])
					.await
					.unwrap_or_default();
				b.close().await;
				rows.into_iter()
					.filter_map(|r| Some((r.first().cloned()??, r.get(1).cloned()??)))
					.collect()
			}
			Err(_) => Vec::new(),
		};
		let path = self.search_path.clone();
		Ok(crate::ddl::sequence_refusal(sql, |name| {
			let name = name.trim_matches('"');
			match name.split_once('.') {
				Some((s, n)) => striped.iter().any(|(a, b)| a == s && b == n),
				None => striped
					.iter()
					.any(|(a, b)| b == name && path.iter().any(|p| p == a)),
			}
		}))
	}

	/// A reference table's copies are written separately, so the write must compute the same
	/// values on every one: no function that is not immutable (now(), random(), nextval), no
	/// CURRENT_TIMESTAMP and friends, and for an INSERT no column default that calls one.
	async fn copies_refusal(&mut self, f: &Fanout) -> Result<Option<route::Refusal>, WireError> {
		let differ = |what: String| route::Refusal {
			code: route::NOT_ACROSS_NODES,
			message: format!(
				"{what} would be computed separately on each copy of the reference table, and the copies would differ"
			),
			hint: "Compute the value first and write it as a literal or a parameter.".into(),
		};
		if scatter::calls_the_clock(&f.sql) {
			return Ok(Some(differ("the current time".into())));
		}
		let functions = scatter::functions(&f.sql);
		if !functions.is_empty() {
			let names =
				String::from_utf8_lossy(&text_array(functions.iter().map(|s| s.as_bytes())))
					.into_owned();
			let rows = self
				.home_query(
					"select coalesce(string_agg(distinct proname::text, ', '), '') from pg_catalog.pg_proc \
					where proname = any($1::text[]) and provolatile <> 'i'",
					&[&names],
				)
				.await?;
			if let Some(v) = rows.first().and_then(|r| r.first().cloned().flatten())
				&& !v.is_empty()
			{
				return Ok(Some(differ(format!("{v}()"))));
			}
		}
		if f.insert {
			for t in &f.written {
				let name = format!(
					"{}.{}",
					crate::catalog::quote_ident(&t.schema),
					crate::catalog::quote_ident(&t.table)
				);
				let rows = self
					.home_query(
						"select (select count(*) from pg_catalog.pg_attrdef d \
							where d.adrelid = $1::regclass and pg_catalog.pg_get_expr(d.adbin, d.adrelid) ~ '\\(') \
						+ (select count(*) from pg_catalog.pg_attribute a \
							where a.attrelid = $1::regclass and a.attidentity <> '')",
						&[&name],
					)
					.await?;
				if rows
					.first()
					.and_then(|r| r.first().cloned().flatten())
					.is_some_and(|v| v != "0")
				{
					return Ok(Some(differ(format!("a column default of {t}"))));
				}
			}
		}
		Ok(None)
	}

	/// A query of Lepis's own on the client's home session, its rows as text (none on error).
	async fn home_query(
		&mut self,
		sql: &str,
		params: &[&str],
	) -> Result<Vec<Vec<Option<String>>>, WireError> {
		let home = self.home();
		let values: Vec<Option<Vec<u8>>> =
			params.iter().map(|p| Some(p.as_bytes().to_vec())).collect();
		let msgs = [
			parse_message(sql, &[]),
			bind_message(&[0], &values, &[]),
			execute_message(),
			Message::new(b'S', vec![]),
		];
		self.send(home, &msgs).await?;
		let conn = self.conns.get_mut(&home).expect("connected");
		conn.prepared.remove("");
		let mut rows = Vec::new();
		loop {
			let m = conn.reader.next().await?;
			match m.tag {
				b'D' => rows.push(
					wire::parse_data_row(&m.body)?
						.into_iter()
						.map(|c| c.map(|b| String::from_utf8_lossy(&b).into_owned()))
						.collect(),
				),
				b'Z' => return Ok(rows),
				_ => {}
			}
		}
	}
}

impl Part<'_> {
	async fn execute_sql(&mut self, sql: &str) -> Result<String, BackendError> {
		use crate::twopc::Participant;
		self.execute(sql).await
	}
}

/// The command tags of several nodes' parts as one: `UPDATE 3` and `UPDATE 4` are `UPDATE 7`.
fn sum_tags<'a>(tags: impl Iterator<Item = &'a str>) -> String {
	let tags: Vec<&str> = tags.collect();
	// `CREATE TABLE`, `ALTER TABLE`: no count to add up.
	if tags.iter().any(|t| {
		t.rsplit(' ')
			.next()
			.is_none_or(|w| w.parse::<u64>().is_err())
	}) {
		return tags.first().map(|t| t.to_string()).unwrap_or_default();
	}
	let tags = tags.into_iter();
	let mut verb: Option<Vec<String>> = None;
	let mut total: u64 = 0;
	for t in tags {
		let mut words: Vec<String> = t.split(' ').map(str::to_string).collect();
		let n = words.pop().and_then(|w| w.parse::<u64>().ok()).unwrap_or(0);
		total += n;
		verb.get_or_insert(words);
	}
	let mut words = verb.unwrap_or_default();
	words.push(total.to_string());
	words.join(" ")
}

// ---------------------------------------------------------------------------------------------
// Schema changes that cannot be atomic, and role changes (Phase 3).

/// A participant that keeps what each statement answered, for a job's reply. (In a mutex
/// only so that it is Sync, as ddl.rs's job runner needs; it is never locked.)
struct Recording<'a> {
	part: tokio::sync::Mutex<Part<'a>>,
	node: NodeId,
	tag: Option<String>,
	error: Option<Message>,
}

impl twopc::Participant for Recording<'_> {
	fn node(&self) -> NodeId {
		self.node
	}

	async fn execute(&mut self, sql: &str) -> Result<String, BackendError> {
		let r = self.part.get_mut().execute_sql(sql).await;
		match &r {
			Ok(t) => self.tag = Some(t.clone()),
			Err(BackendError::Refused(m)) => self.error = Some(m.clone()),
			Err(_) => {}
		}
		r
	}
}

impl Router {
	/// A statement no transaction block may hold (CREATE INDEX CONCURRENTLY, VACUUM, …) on a
	/// distributed table: each node runs it on its own, and `lepis.ddl_job_node` records where
	/// it ran. It cannot be all or nothing; a node that failed is named, with the nodes it did
	/// run on.
	async fn job(&mut self, sql: String, batch: Vec<Message>) -> Result<(), WireError> {
		let simple = batch.first().map(|m| m.tag) == Some(b'Q');
		let nodes: Vec<NodeId> = ddl::nodes(ddl::Target::Everywhere, &self.catalog);
		for n in &nodes {
			self.ensure_conn(*n).await?;
			self.catch_up_sets(*n).await?;
		}
		let unrecorded = |e: String| Outcome::Failed {
			at_parse: false,
			error: refusal_message(
				"08006",
				&format!("the job could not be recorded on the home node: {e}"),
				"Try again once the home node answers.",
			),
		};
		let mut log = match twopc::home_service(&self.app, "ddl job").await {
			Ok(b) => b,
			Err(e) => {
				self.answer(&batch, simple, None, unrecorded(e.to_string()));
				return Ok(());
			}
		};
		let job = match ddl::start_job(&mut log, &sql, &nodes).await {
			Ok(j) => j,
			Err(e) => {
				log.close().await;
				self.answer(&batch, simple, None, unrecorded(e.to_string()));
				return Ok(());
			}
		};
		self.spread_cancel(&nodes);
		let mut parts: Vec<Recording> = self
			.conns
			.iter_mut()
			.filter(|(id, _)| nodes.contains(id))
			.map(|(id, conn)| Recording {
				part: tokio::sync::Mutex::new(Part { node: *id, conn }),
				node: *id,
				tag: None,
				error: None,
			})
			.collect();
		let states = ddl::run_job(&mut log, job, &sql, &mut parts).await;
		spread::clear(self.cancel_pid);
		let tag = parts.iter().find_map(|p| p.tag.clone());
		let failed: Vec<(NodeId, Option<Message>)> = parts
			.iter()
			.filter(|p| p.tag.is_none())
			.map(|p| (p.node, p.error.clone()))
			.collect();
		drop(parts);
		log.close().await;
		let outcome = match (states, failed.first()) {
			(Ok(_), None) => Outcome::Rows(Vec::new(), Some(tag.unwrap_or_default())),
			(Err(e), None) => Outcome::Failed {
				at_parse: false,
				error: refusal_message(
					"08006",
					&format!(
						"every node ran the statement, but the job's record could not be updated: {e}"
					),
					"Nothing needs running again.",
				),
			},
			(_, Some((node, error))) => {
				let done: Vec<String> = nodes
					.iter()
					.filter(|n| !failed.iter().any(|(f, _)| f == *n))
					.map(|n| n.to_string())
					.collect();
				let (code, message) = match error {
					Some(m) => {
						let f = wire::parse_error_fields(&m.body);
						let get = |k: u8| {
							f.iter()
								.find(|(x, _)| *x == k)
								.map(|(_, v)| v.clone())
								.unwrap_or_default()
						};
						(get(b'C'), get(b'M'))
					}
					None => (
						"08006".to_string(),
						"the node could not be reached".to_string(),
					),
				};
				let hint = if done.is_empty() {
					"It ran on no node; fix the cause and run it again.".to_string()
				} else {
					format!(
						"It ran on {} and cannot be undone there; fix the cause and run it again where it failed (lepis.ddl_job_node lists each node).",
						done.join(", ")
					)
				};
				Outcome::Failed {
					at_parse: false,
					error: owned_error(&code, &format!("{message} (on {node}; job {job})"), &hint),
				}
			}
		};
		self.answer(&batch, simple, None, outcome);
		Ok(())
	}

	/// Copies the role statements home has run to every other node, once no transaction holds
	/// them. A statement that failed or was rolled back changed nothing on home, and copying
	/// home's roles again changes nothing either.
	async fn copy_roles(&mut self) -> Result<(), WireError> {
		if self.roles_owed.is_empty() {
			return Ok(());
		}
		self.wait_idle().await?;
		if self.in_tx() {
			return Ok(());
		}
		let changes = std::mem::take(&mut self.roles_owed);
		let mut missed: Vec<String> = Vec::new();
		for c in &changes {
			// The client's statement stands either way (home has it); a node that missed it is
			// named in a WARNING, and the role reconcile brings it in line.
			match roles::replicate(&self.app, &self.catalog, c).await {
				Ok(results) => {
					for (node, r) in results {
						if let Err(e) = r {
							missed.push(format!("{node}: {e}"));
						}
					}
				}
				Err(e) => missed.push(e),
			}
		}
		if !missed.is_empty() {
			let notice = Body::new()
				.byte(b'S')
				.cstr("WARNING")
				.byte(b'V')
				.cstr("WARNING")
				.byte(b'C')
				.cstr("01000")
				.byte(b'M')
				.cstr(&format!(
					"the role change was made on the home node but not copied to every node ({})",
					missed.join("; ")
				))
				.byte(b'H')
				.cstr("Lepis's role reconcile copies it once the node can take it; until then, logging in through that node may fail.")
				.byte(0)
				.message(b'N');
			// Before the ReadyForQuery that ends the statement, when it has not been sent yet.
			let at = last_ready(&self.out).unwrap_or(self.out.len());
			let bytes = notice.encode();
			self.out.splice(at..at, bytes);
		}
		Ok(())
	}

	/// Asks the home node which tables these indexes are on.
	async fn learn_index_owners(
		&mut self,
		names: Vec<crate::catalog::RelationName>,
	) -> Result<(), WireError> {
		self.wait_idle().await?;
		let home = self.home();
		self.ensure_conn(home).await?;
		self.catch_up_sets(home).await?;
		let refs: Vec<String> = names.iter().map(ddl::index_ref).collect();
		let refs =
			String::from_utf8_lossy(&text_array(refs.iter().map(|s| s.as_bytes()))).into_owned();
		let rows = self.home_query(ddl::INDEX_OWNERS_SQL, &[&refs]).await?;
		for r in rows {
			let get = |i: usize| r.get(i).cloned().flatten().unwrap_or_default();
			self.index_owners.insert(
				crate::catalog::RelationName {
					schema: get(0),
					table: get(1),
				},
				crate::catalog::RelationName {
					schema: get(2),
					table: get(3),
				},
			);
		}
		Ok(())
	}
}

/// Where the last ReadyForQuery in `out` starts, if there is one.
fn last_ready(out: &[u8]) -> Option<usize> {
	let mut i = 0;
	let mut last = None;
	while i + 5 <= out.len() {
		if out[i] == b'Z' {
			last = Some(i);
		}
		let len = u32::from_be_bytes([out[i + 1], out[i + 2], out[i + 3], out[i + 4]]) as usize;
		i += 1 + len;
	}
	last
}

/// An ErrorResponse with a SQLSTATE known only at run time (a node's own).
fn owned_error(code: &str, message: &str, hint: &str) -> Message {
	let mut b = Body::new()
		.byte(b'S')
		.cstr("ERROR")
		.byte(b'V')
		.cstr("ERROR")
		.byte(b'C')
		.cstr(code)
		.byte(b'M')
		.cstr(message);
	if !hint.is_empty() {
		b = b.byte(b'H').cstr(hint);
	}
	b.byte(0).message(b'E')
}

// ---------------------------------------------------------------------------------------------
// Streaming a read across nodes.
//
// Each node runs the worker as a named portal and hands it over a window at a time (Execute with
// a row count, then Flush): the merge pulls a node's next window only when that node's buffered
// rows are used up, so memory holds a window per node whatever the result's size, and a LIMIT
// that is reached closes every portal without reading the rest. Values only the home node can
// order are ranked again on each refill, on the home session itself (Flush, never Sync, so the
// portal survives).

/// Rows a node hands over per Execute.
const WINDOW: i32 = 1000;
/// The worker's portal on every node.
const PORTAL: &str = "lepis_w";
/// Rows are sent to the client whenever this much is waiting.
const FLUSH_AT: usize = 64 * 1024;
/// How long a client in transaction pooling keeps its node sessions to itself after a
/// transaction, before they go to the pool for any client.
const PARK: std::time::Duration = std::time::Duration::from_millis(50);

/// What one window brought.
enum Pulled {
	/// The portal has more rows.
	More,
	Finished,
	Failed(Message),
}

/// The cancel keys of the node sessions a statement runs on, beyond the one the cancel registry
/// points at, so that a client's cancel stops every node working for it.
pub(crate) mod spread {
	use std::collections::HashMap;
	use std::sync::{Mutex, OnceLock};

	use crate::cancel::Target;

	type Map = HashMap<i32, (Vec<u8>, Vec<Target>)>;

	fn map() -> &'static Mutex<Map> {
		static MAP: OnceLock<Mutex<Map>> = OnceLock::new();
		MAP.get_or_init(Default::default)
	}

	pub fn set(pid: i32, key: &[u8], targets: Vec<Target>) {
		map()
			.lock()
			.expect("cancel spread")
			.insert(pid, (key.to_vec(), targets));
	}

	pub fn clear(pid: i32) {
		map().lock().expect("cancel spread").remove(&pid);
	}

	/// Forwards a client's cancel to every node session of the statement it is running.
	pub async fn cancel(pid: i32, key: &[u8]) {
		let targets = {
			let m = map().lock().expect("cancel spread");
			match m.get(&pid) {
				Some((k, t)) if crate::scram::constant_time_eq(k, key) => t.clone(),
				_ => return,
			}
		};
		crate::twopc::join_all(
			targets
				.into_iter()
				.map(|t| async move {
					match crate::backend::open(&t.node, t.tls.as_ref()).await {
						Ok(mut s) => {
							let _ = crate::wire::write_all(
								&mut s,
								&crate::wire::cancel_request(t.pid, &t.key),
							)
							.await;
						}
						Err(e) => tracing::warn!(node = %t.node, "cancel not delivered: {e}"),
					}
				})
				.collect(),
		)
		.await;
	}
}

impl Router {
	/// Where a cancel for this session's connection to `node` goes.
	fn cancel_target(&self, node: NodeId) -> Option<crate::cancel::Target> {
		let (conn, n) = (self.conns.get(&node)?, self.catalog.nodes.get(&node)?);
		let address = NodeAddress {
			host: n.host.clone(),
			port: n.port,
			sslmode: n.sslmode,
			ca_file: self.app.config.home.ca_file.clone(),
		};
		let tls = self.tls_of(&address);
		Some(crate::cancel::Target {
			node: address,
			tls,
			pid: conn.pid,
			key: conn.key.clone(),
		})
	}

	/// A cancel from the client now reaches every one of `nodes`.
	fn spread_cancel(&self, nodes: &[NodeId]) {
		let targets: Vec<_> = nodes
			.iter()
			.filter(|n| **n != self.cancel_node)
			.filter_map(|n| self.cancel_target(*n))
			.collect();
		if !targets.is_empty() {
			spread::set(self.cancel_pid, &self.cancel_key, targets);
		}
	}

	fn unspread_cancel(&self) {
		spread::clear(self.cancel_pid);
	}

	/// Reads one window of a node's portal into `buf`; `fields` gets its RowDescription.
	async fn pull(
		&mut self,
		node: NodeId,
		first: bool,
		buf: &mut VecDeque<Row>,
		fields: &mut Vec<Field>,
	) -> Result<Pulled, WireError> {
		if !first {
			self.send(
				node,
				&[
					Body::new().cstr(PORTAL).i32(WINDOW).message(b'E'),
					Message::new(b'H', vec![]),
				],
			)
			.await?;
		}
		let conn = self.conns.get_mut(&node).expect("connected");
		loop {
			let m = conn.reader.next().await?;
			match m.tag {
				b'T' => *fields = Field::parse_all(&m.body)?,
				b'D' => buf.push_back(wire::parse_data_row(&m.body)?),
				b's' => return Ok(Pulled::More),
				b'C' => return Ok(Pulled::Finished),
				b'E' => return Ok(Pulled::Failed(m)),
				b'N' | b'A' | b'S' => put_message(&mut self.out, &m),
				_ => {}
			}
		}
	}

	/// Closes every node's portal and ends its extended-protocol run; returns whether a node's
	/// transaction is now failed.
	async fn end_portals(&mut self, nodes: &[NodeId]) -> Result<bool, WireError> {
		let close = [
			Body::new().byte(b'P').cstr(PORTAL).message(b'C'),
			Message::new(b'S', vec![]),
		];
		for n in nodes {
			self.send(*n, &close).await?;
		}
		let mut failed = false;
		for n in nodes {
			let conn = self.conns.get_mut(n).expect("connected");
			loop {
				let m = conn.reader.next().await?;
				match m.tag {
					b'N' | b'A' | b'S' => put_message(&mut self.out, &m),
					b'Z' => {
						failed |= m.body.first() == Some(&b'E');
						break;
					}
					_ => {}
				}
			}
		}
		Ok(failed)
	}

	/// A read across nodes, merged as the rows arrive (see the section's header).
	#[allow(clippy::too_many_arguments)]
	async fn gather_stream(
		&mut self,
		nodes: &[NodeId],
		p: &scatter::Planned,
		d: &DescribeResult,
		shown_fields: &[Field],
		bind: Option<&BindMsg>,
		result_formats: &[i16],
		batch: &[Message],
		simple: bool,
	) -> Result<(), WireError> {
		let home = self.home();
		let (pformats, values) = match bind {
			Some(b) => (b.param_formats.clone(), b.values.clone()),
			None => (Vec::new(), Vec::new()),
		};
		let open = [
			parse_message(&p.worker_sql, &d.params),
			bind_portal(PORTAL, &pformats, &values, &p.formats),
			Body::new().byte(b'P').cstr(PORTAL).message(b'D'),
			Body::new().cstr(PORTAL).i32(WINDOW).message(b'E'),
			Message::new(b'H', vec![]),
		];
		self.spread_cancel(nodes);
		for n in nodes {
			self.send(*n, &open).await?;
			self.conns
				.get_mut(n)
				.expect("connected")
				.prepared
				.remove("");
		}
		let k = nodes.len();
		let mut bufs: Vec<VecDeque<Row>> = vec![VecDeque::new(); k];
		let mut finished = vec![false; k];
		let mut types: Vec<u32> = Vec::new();
		let mut error: Option<Message> = None;
		for i in 0..k {
			let mut fields = Vec::new();
			let mut buf = std::mem::take(&mut bufs[i]);
			match self.pull(nodes[i], true, &mut buf, &mut fields).await? {
				Pulled::More => {}
				Pulled::Finished => finished[i] = true,
				Pulled::Failed(e) => {
					finished[i] = true;
					error.get_or_insert(e);
				}
			}
			bufs[i] = buf;
			// A user type's oid is each node's own: the types are the home node's (it ranks
			// them), and the nodes need only agree on the shape.
			let t: Vec<u32> = fields.iter().map(|f| f.oid).collect();
			if i == 0 || nodes[i] == home {
				if i > 0 && t.len() != types.len() && error.is_none() {
					error = Some(refusal_message(
						"XX000",
						"the nodes answered with different columns (is the schema the same on every node?)",
						"",
					));
				}
				types = t;
			} else if error.is_none() && t.len() != types.len() {
				error = Some(refusal_message(
					"XX000",
					"the nodes answered with different columns (is the schema the same on every node?)",
					"",
				));
			}
		}
		let ranks_sync = !nodes.contains(&home);

		let mut emitted: u64 = 0;
		let mut head = false;
		let result: Result<(), Message> = 'run: {
			if let Some(e) = error.take() {
				break 'run Err(e);
			}
			if !p.merge.streams(&types) {
				// Groups (one row per group and node) or a DISTINCT Lepis cannot order itself:
				// every window, then the buffered merge.
				for i in 0..k {
					while !finished[i] {
						let mut buf = std::mem::take(&mut bufs[i]);
						let mut f = Vec::new();
						match self.pull(nodes[i], false, &mut buf, &mut f).await? {
							Pulled::More => {}
							Pulled::Finished => finished[i] = true,
							Pulled::Failed(e) => break 'run Err(e),
						}
						bufs[i] = buf;
					}
				}
				let failed = self.end_portals(nodes).await?;
				self.unspread_cancel();
				let all: Vec<Vec<Row>> = bufs.into_iter().map(Vec::from).collect();
				let answers: Vec<Answer> = all
					.into_iter()
					.map(|rows| Answer {
						fields: types
							.iter()
							.map(|t| Field {
								oid: *t,
								..Field::text("")
							})
							.collect(),
						rows,
						tag: String::new(),
					})
					.collect();
				if failed && self.in_tx() {
					self.tx_status = b'E';
				}
				let rows = self.combine(home, p, d, result_formats, &answers).await?;
				let outcome = match rows {
					Ok(rows) => Outcome::Rows(rows, None),
					Err(e) => Outcome::Failed {
						at_parse: false,
						error: refusal_message(e.code, &e.message, e.hint.as_deref().unwrap_or("")),
					},
				};
				self.answer(batch, simple, Some((d, shown_fields)), outcome);
				return Ok(());
			}
			let mut merge = match p.merge.row_merge(k, &types, &p.formats) {
				Ok(m) => m,
				Err(e) => break 'run Err(merge_error(&e)),
			};
			// Ranked again after every refill: a rank is good for the window it was asked for.
			loop {
				let requests = match merge.needs_ranks(&bufs) {
					Ok(r) => r,
					Err(e) => break 'run Err(merge_error(&e)),
				};
				let ranks = if requests.is_empty() {
					Ranks::new()
				} else {
					match self.rank(home, &requests, ranks_sync).await? {
						Ok(r) => r,
						Err(e) => break 'run Err(merge_error(&e)),
					}
				};
				let mut rows = Vec::new();
				let step = merge.step(&mut bufs, &finished, &ranks, &mut rows);
				if !rows.is_empty() {
					if !head {
						self.stream_head(batch, simple, d, shown_fields);
						head = true;
					}
					for r in &rows {
						self.out.extend_from_slice(&data_row(r).encode());
					}
					emitted += rows.len() as u64;
					if self.out.len() >= FLUSH_AT {
						self.flush_client().await?;
					}
				}
				match step {
					Err(e) => break 'run Err(merge_error(&e)),
					Ok(merge::Step::Done) => break 'run Ok(()),
					Ok(merge::Step::Need(i)) => {
						let mut buf = std::mem::take(&mut bufs[i]);
						let mut f = Vec::new();
						match self.pull(nodes[i], false, &mut buf, &mut f).await? {
							Pulled::More => {}
							Pulled::Finished => finished[i] = true,
							Pulled::Failed(e) => break 'run Err(e),
						}
						bufs[i] = buf;
					}
				}
			}
		};
		let failed = self.end_portals(nodes).await?;
		self.unspread_cancel();
		if (failed || result.is_err()) && self.in_tx() {
			self.tx_status = b'E';
		}
		match result {
			Err(e) if !head => {
				self.answer(
					batch,
					simple,
					Some((d, shown_fields)),
					Outcome::Failed {
						at_parse: false,
						error: e,
					},
				);
			}
			result => {
				if !head {
					self.stream_head(batch, simple, d, shown_fields);
				}
				self.stream_tail(batch, simple, result.map(|_| emitted));
			}
		}
		Ok(())
	}

	/// What the client is sent before the rows: as `answer` sends it.
	fn stream_head(
		&mut self,
		batch: &[Message],
		simple: bool,
		d: &DescribeResult,
		fields: &[Field],
	) {
		if simple {
			if !fields.is_empty() {
				self.out
					.extend_from_slice(&Field::message(fields, |_| 0).encode());
			}
			return;
		}
		let formats = batch
			.iter()
			.find(|m| m.tag == b'B')
			.and_then(|m| BindMsg::parse(&m.body).ok())
			.map(|b| b.result_formats)
			.unwrap_or_default();
		for m in batch {
			match m.tag {
				b'P' => self
					.out
					.extend_from_slice(&Message::new(b'1', vec![]).encode()),
				b'B' => self
					.out
					.extend_from_slice(&Message::new(b'2', vec![]).encode()),
				b'D' => {
					if m.body.first() == Some(&b'S') {
						self.out
							.extend_from_slice(&parameter_description(&d.params).encode());
					}
					if fields.is_empty() {
						self.out
							.extend_from_slice(&Message::new(b'n', vec![]).encode());
					} else if m.body.first() == Some(&b'S') {
						self.out
							.extend_from_slice(&Field::message(fields, |_| 0).encode());
					} else {
						let fmt = |i: usize| match formats.as_slice() {
							[] => 0,
							[f] => *f,
							fs => fs.get(i).copied().unwrap_or(0),
						};
						self.out
							.extend_from_slice(&Field::message(fields, fmt).encode());
					}
				}
				b'E' => return,
				_ => {}
			}
		}
	}

	/// What the client is sent after the rows: the count, or the error that stopped them.
	fn stream_tail(&mut self, batch: &[Message], simple: bool, result: Result<u64, Message>) {
		let ready = Body::new().byte(self.tx_status_now()).message(b'Z');
		let end = match &result {
			Ok(n) => Body::new().cstr(&format!("SELECT {n}")).message(b'C'),
			Err(e) => e.clone(),
		};
		if simple {
			self.out.extend_from_slice(&end.encode());
			self.out.extend_from_slice(&ready.encode());
			return;
		}
		for m in batch {
			match m.tag {
				b'E' => self.out.extend_from_slice(&end.encode()),
				b'S' => self.out.extend_from_slice(&ready.encode()),
				_ => {}
			}
		}
	}
}

/// An ErrorResponse for a merge that could not be made.
fn merge_error(e: &MergeError) -> Message {
	refusal_message(e.code, &e.message, e.hint.as_deref().unwrap_or(""))
}

fn bind_portal(
	portal: &str,
	param_formats: &[i16],
	values: &[Option<Vec<u8>>],
	result_formats: &[i16],
) -> Message {
	let mut b = Body::new()
		.cstr(portal)
		.cstr("")
		.bytes(&(param_formats.len() as u16).to_be_bytes());
	for f in param_formats {
		b = b.bytes(&f.to_be_bytes());
	}
	b = b.bytes(&(values.len() as u16).to_be_bytes());
	for v in values {
		b = match v {
			None => b.i32(-1),
			Some(v) => b.i32(v.len() as i32).bytes(v),
		};
	}
	b = b.bytes(&(result_formats.len() as u16).to_be_bytes());
	for f in result_formats {
		b = b.bytes(&f.to_be_bytes());
	}
	b.message(b'B')
}

// ---------------------------------------------------------------------------------------------
// Transaction pooling, and the cluster's settings.
//
// In session mode (the default) a client keeps its node sessions for as long as it is connected,
// as with one Postgres. With the cluster setting `pool_mode` = `transaction`, a client holds node
// sessions only while a transaction (or an unfinished batch) needs them: whenever it is idle
// they go back to a pool per (node, role), and the next statement takes one from there (the
// one this client left, when it is still there), or connects. A backend another session left is
// reset first (DISCARD ALL). What made the session the client's is put back on whatever
// backend it lands on: its startup parameters, its SETs (the same replay a new node gets) and its
// prepared statements (parsed again where they are missing, as on any node). What cannot be put
// back (LISTEN, a temporary table, a cursor WITH HOLD, SQL PREPARE, a session advisory lock,
// LOAD) is refused in transaction mode, with the setting named.

/// The cluster's settings the router reads (`lepis.cluster.settings`, a JSON object).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Settings {
	/// `pool_mode` = `transaction`.
	pub transaction_pool: bool,
	/// `route_claim`: the JWT claim (in `request.jwt.claims`) that holds a transaction's shard key.
	pub route_claim: Option<String>,
	/// `route_claim_keyspace`: the keyspace that claim is a value of (optional with one keyspace).
	pub route_claim_keyspace: Option<String>,
}

impl Settings {
	pub fn from_json(text: &str) -> Settings {
		let v: serde_json::Value = serde_json::from_str(text).unwrap_or_default();
		let s = |k: &str| {
			v.get(k)
				.and_then(|x| x.as_str())
				.filter(|x| !x.is_empty())
				.map(str::to_string)
		};
		Settings {
			transaction_pool: s("pool_mode").as_deref() == Some("transaction"),
			route_claim: s("route_claim"),
			route_claim_keyspace: s("route_claim_keyspace"),
		}
	}
}

pub(crate) mod settings {
	use std::collections::HashMap;
	use std::sync::{Arc, Mutex, OnceLock};
	use std::time::Duration;

	use super::Settings;
	use crate::server::App;

	/// How often a session looks at the cluster's settings again.
	pub const FRESH: Duration = Duration::from_secs(2);
	/// How often the one reader per cluster reads them from the home node.
	const READ_EVERY: Duration = Duration::from_secs(1);

	type Cache = HashMap<String, Arc<Settings>>;

	fn cache() -> &'static Mutex<Cache> {
		static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();
		CACHE.get_or_init(Default::default)
	}

	/// One reading over `b`, or None (no column yet, the node said no, or it went away).
	async fn read(b: &mut crate::backend::Backend) -> Option<Settings> {
		let rows = b
			.query("select settings::text from lepis.cluster where id = 1", &[])
			.await
			.ok()?;
		Some(
			rows.first()
				.and_then(|r| r.first().cloned().flatten())
				.map(|t| Settings::from_json(&t))
				.unwrap_or_default(),
		)
	}

	/// The cluster's settings. The first session of a cluster in this process reads them and
	/// starts the one task that reads them again every second over one connection, so no
	/// session waits on, or logs in for, a reading after that. A cluster without the column,
	/// or a home node that does not answer, has the defaults.
	pub async fn get(app: &Arc<App>) -> Arc<Settings> {
		let key = format!("{}/{}", app.config.home, app.config.service.database);
		if let Some(s) = cache().lock().expect("settings").get(&key) {
			return s.clone();
		}
		let mut conn = crate::twopc::home_service(app, "settings").await.ok();
		let first = match conn.as_mut() {
			Some(b) => read(b).await.unwrap_or_default(),
			None => Settings::default(),
		};
		let first = Arc::new(first);
		{
			let mut c = cache().lock().expect("settings");
			if let Some(s) = c.get(&key) {
				// Another session got there first, and its reader runs.
				return s.clone();
			}
			c.insert(key.clone(), first.clone());
		}
		let app = app.clone();
		tokio::spawn(async move {
			loop {
				tokio::time::sleep(READ_EVERY).await;
				if conn.is_none() {
					conn = crate::twopc::home_service(&app, "settings").await.ok();
				}
				let Some(b) = conn.as_mut() else { continue };
				match read(b).await {
					Some(s) => {
						let mut c = cache().lock().expect("settings");
						if c.get(&key).is_none_or(|old| **old != s) {
							c.insert(key.clone(), Arc::new(s));
						}
					}
					None => conn = None,
				}
			}
		});
		first
	}
}

mod pool {
	use std::collections::HashMap;
	use std::sync::{Mutex, OnceLock};
	use std::time::{Duration, Instant};

	use super::Conn;

	/// (node address and database, role).
	pub type Key = (String, String);

	/// Idle backends kept per key; more are closed.
	const MAX_IDLE: usize = 64;
	/// An idle backend older than this is closed rather than handed out.
	const IDLE_FOR: Duration = Duration::from_secs(300);

	/// (when it came back, the session that left it, the backend).
	type Pools = HashMap<Key, Vec<(Instant, i32, Conn)>>;

	fn pools() -> &'static Mutex<Pools> {
		static POOLS: OnceLock<Mutex<Pools>> = OnceLock::new();
		POOLS.get_or_init(Default::default)
	}

	/// A backend for session `me`: the one it left itself if it is still there (nothing to reset),
	/// else the most recent; the owner says which.
	pub(super) fn take(key: &Key, me: i32) -> Option<(i32, Conn)> {
		let mut p = pools().lock().expect("pool");
		let idle = p.get_mut(key)?;
		idle.retain(|(at, _, _)| at.elapsed() < IDLE_FOR);
		let i = idle
			.iter()
			.rposition(|(_, owner, _)| *owner == me)
			.or_else(|| idle.len().checked_sub(1))?;
		let (_, owner, c) = idle.remove(i);
		Some((owner, c))
	}

	pub(super) fn put(key: Key, owner: i32, conn: Conn) {
		let mut p = pools().lock().expect("pool");
		let idle = p.entry(key).or_default();
		if idle.len() < MAX_IDLE {
			idle.push((Instant::now(), owner, conn));
		}
	}

	/// How many idle backends the pool holds for `key` (tests).
	pub fn idle(key: &Key) -> usize {
		pools().lock().expect("pool").get(key).map_or(0, Vec::len)
	}
}

/// Idle backends in the pool for a node address (`host:port/database`) and role, for tests.
pub fn pooled(node: &str, role: &str) -> usize {
	pool::idle(&(node.to_string(), role.to_string()))
}

impl Router {
	fn pool_key(&self, node: NodeId) -> Option<pool::Key> {
		let n = self.catalog.nodes.get(&node)?;
		Some((
			format!("{}:{}/{}", n.host, n.port, n.dbname),
			self.role.clone(),
		))
	}

	/// Whether every node session can go back to the pool now: nothing in flight or waiting, no
	/// transaction, no COPY.
	fn can_release(&self) -> bool {
		self.settings.transaction_pool
			&& !self.conns.is_empty()
			&& self.inflight.is_empty()
			&& self.queued.is_empty()
			&& self.batch.is_empty()
			&& self.tx_status == b'I'
			&& self.pinned.is_none()
			&& self.unbound_begin.is_none()
			&& self.participants.is_empty()
			&& self.copy_in.is_none()
			&& self.copy_split.is_none()
			&& !self.skip_until_sync
			&& self.roles_owed.is_empty()
	}

	/// Sets the node sessions aside at the end of a transaction. They go to the pool only when
	/// the client stays idle for `PARK` (`publish`): a busy client takes its own back without
	/// touching the pool, and its cancel key keeps pointing at them meanwhile, since nobody else
	/// can have them.
	async fn release(&mut self) -> Result<(), WireError> {
		self.parked.extend(self.conns.drain());
		self.parked_until = Some(tokio::time::Instant::now() + PARK);
		Ok(())
	}

	/// Gives the sessions set aside back to the pool as they are: one is reset only if another
	/// session takes it (`take_pooled`).
	fn publish(&mut self) {
		self.parked_until = None;
		if self.parked.is_empty() {
			return;
		}
		for (node, c) in std::mem::take(&mut self.parked) {
			if let Some(key) = self.pool_key(node) {
				pool::put(key, self.cancel_pid, c);
			}
		}
		// A cancel now reaches no backend (pid 0 is nobody's).
		if let Some(home) = self.catalog.home() {
			let address = NodeAddress {
				host: home.host.clone(),
				port: home.port,
				sslmode: home.sslmode,
				ca_file: self.app.config.home.ca_file.clone(),
			};
			let tls = self.tls_of(&address);
			self.app.cancels.retarget(
				self.cancel_pid,
				crate::cancel::Target {
					node: address,
					tls,
					pid: 0,
					key: vec![0; 4],
				},
			);
		}
		self.cancel_node = NodeId(i32::MIN);
	}

	/// A backend from the pool, set up as this client's: its startup parameters first (DISCARD
	/// ALL put the pooled backend's own back).
	async fn take_pooled(&mut self, node: NodeId) -> Result<bool, WireError> {
		if !self.settings.transaction_pool {
			return Ok(false);
		}
		let Some(key) = self.pool_key(node) else {
			return Ok(false);
		};
		let Some((owner, mut c)) = pool::take(&key, self.cancel_pid) else {
			return Ok(false);
		};
		if owner == self.cancel_pid {
			// The backend this session left: still exactly as it was.
			self.conns.insert(node, c);
			return Ok(true);
		}
		// Another session's: DISCARD ALL, then this client's own startup parameters.
		let reset = async {
			c.reader
				.get_mut()
				.write_all(&wire::query("DISCARD ALL").encode())
				.await?;
			c.reader.get_mut().flush().await?;
			let mut ok = true;
			loop {
				let m = c.reader.next().await?;
				match m.tag {
					b'E' => ok = false,
					b'Z' => return Ok::<bool, WireError>(ok && m.body.first() == Some(&b'I')),
					_ => {}
				}
			}
		};
		if !matches!(reset.await, Ok(true)) {
			return Ok(false);
		}
		c.prepared.clear();
		c.sets_applied = 0;
		c.closes_owed.clear();
		c.tx_applied = 0;
		let sets: Vec<String> = self
			.startup
			.iter()
			.filter(|(k, _)| {
				!matches!(k.as_str(), "user" | "database" | "replication" | "options")
					&& k.bytes()
						.all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.')
			})
			.map(|(k, v)| {
				format!(
					"select pg_catalog.set_config({}, {}, false)",
					crate::catalog::quote_literal(k),
					crate::catalog::quote_literal(v)
				)
			})
			.collect();
		if !sets.is_empty() {
			let ok = async {
				c.reader
					.get_mut()
					.write_all(&wire::query(&sets.join(";\n")).encode())
					.await?;
				c.reader.get_mut().flush().await?;
				let mut ok = true;
				loop {
					let m = c.reader.next().await?;
					match m.tag {
						b'E' => ok = false,
						b'Z' => return Ok::<bool, WireError>(ok),
						_ => {}
					}
				}
			}
			.await;
			if !matches!(ok, Ok(true)) {
				return Ok(false);
			}
		}
		self.conns.insert(node, c);
		Ok(true)
	}

	/// Reads the cluster's settings again when the last reading is old.
	async fn refresh_settings(&mut self) {
		if self.settings_read.elapsed() >= settings::FRESH {
			self.settings = settings::get(&self.app).await;
			self.settings_read = std::time::Instant::now();
		}
	}
}

// ---------------------------------------------------------------------------------------------
// COPY … FROM STDIN split by row (route.rs `copy`).
//
// Lepis answers the client's COPY itself: every node of the keyspace runs the same COPY, the
// client's data is cut into rows, and each row goes to the node that owns its shard key. Outside
// a transaction block the nodes commit together (two-phase); inside one, each node joins it.

/// Bytes of rows buffered for one node before they are sent.
const COPY_CHUNK: usize = 64 * 1024;

/// A COPY the router is splitting.
struct CopySplit {
	nodes: Vec<NodeId>,
	splitter: route::copy::Splitter,
	keyspace: String,
	/// Per node: CopyData bytes waiting to be sent.
	pending: HashMap<NodeId, Vec<u8>>,
	/// The client used the simple protocol (ReadyForQuery follows CopyDone).
	simple: bool,
	/// Each node's COPY is in a transaction of its own (committed together at the end).
	implicit: bool,
	/// The first problem with the data; the COPY fails at its end.
	error: Option<Message>,
}

impl Router {
	async fn copy_start(
		&mut self,
		nodes: Vec<NodeId>,
		sql: String,
		batch: Vec<Message>,
	) -> Result<(), WireError> {
		let simple = batch.first().map(|m| m.tag) == Some(b'Q');
		let home = self.home();
		let fail = |this: &mut Self, code: &'static str, message: &str, hint: &str| {
			let o = Outcome::Failed {
				at_parse: false,
				error: refusal_message(code, message, hint),
			};
			this.answer(&batch, simple, None, o);
		};
		let c = match route::copy::read(&sql) {
			Ok(c) => c,
			Err(r) => {
				fail(self, r.code, &r.message, &r.hint);
				return Ok(());
			}
		};
		// The table, its shard key and the key's place among the COPY's columns.
		let facts = match &*self.analyze(&sql) {
			Ok(v) => v.first().map(|s| s.facts.clone()),
			Err(_) => None,
		};
		let Some(table) = facts
			.and_then(|f| f.tables.into_iter().next())
			.map(|t| t.name)
		else {
			fail(
				self,
				route::NOT_ACROSS_NODES,
				"Lepis could not tell which table this COPY writes",
				"Name the table with its schema.",
			);
			return Ok(());
		};
		let Some(crate::catalog::RelationKind::Sharded {
			keyspace,
			key_column,
		}) = self.catalog.relations.get(&table).cloned()
		else {
			fail(
				self,
				route::NOT_ACROSS_NODES,
				"Lepis can split a COPY only into a sharded table",
				"COPY into the table on its node.",
			);
			return Ok(());
		};
		for n in nodes.iter().chain(std::iter::once(&home)) {
			self.ensure_conn(*n).await?;
			self.catch_up_sets(*n).await?;
		}
		let name = format!(
			"{}.{}",
			crate::catalog::quote_ident(&table.schema),
			crate::catalog::quote_ident(&table.table)
		);
		let cols = self
			.home_query(
				"select attname::text, pg_catalog.format_type(atttypid, null) from pg_catalog.pg_attribute \
				where attrelid = $1::regclass and attnum > 0 and not attisdropped and attgenerated = '' \
				order by attnum",
				&[&name],
			)
			.await?;
		let all: Vec<(String, String)> = cols
			.into_iter()
			.map(|r| {
				(
					r.first().cloned().flatten().unwrap_or_default(),
					r.get(1).cloned().flatten().unwrap_or_default(),
				)
			})
			.collect();
		let named: Vec<String> = match &c.columns {
			Some(v) => v.clone(),
			None => all.iter().map(|(n, _)| n.clone()).collect(),
		};
		let Some(key_col) = named.iter().position(|n| *n == key_column) else {
			fail(
				self,
				route::NOT_ACROSS_NODES,
				&format!(
					"a COPY into {table} across nodes must include its shard key column {key_column}"
				),
				"Add the shard key to the COPY's column list.",
			);
			return Ok(());
		};
		let binary = c.options.format == route::copy::Format::Binary;
		if binary {
			let key_type = all
				.iter()
				.find(|(n, _)| *n == key_column)
				.map(|(_, t)| t.clone())
				.unwrap_or_default();
			let ks_type = self
				.catalog
				.keyspaces
				.get(&keyspace)
				.map(|k| k.key_type.sql_name().to_string())
				.unwrap_or_default();
			if key_type != ks_type {
				fail(
					self,
					route::NOT_ACROSS_NODES,
					&format!(
						"a binary COPY across nodes needs {key_column} to be of the keyspace's type {ks_type}, not {key_type}"
					),
					"Use COPY's text or csv format.",
				);
				return Ok(());
			}
		}

		// Every node in a transaction, then in COPY.
		let implicit = !self.in_tx();
		if implicit {
			for n in &nodes {
				let conn = self.conns.get_mut(n).expect("connected");
				if let Err(e) = (Part { node: *n, conn }).execute_sql("begin").await {
					self.rollback_nodes(&nodes).await;
					let msg = e.to_string();
					fail(self, "08006", &msg, "");
					return Ok(());
				}
			}
		} else if let Some(r) = self.enter_tx(&nodes).await? {
			fail(self, r.code, &r.message, &r.hint);
			return Ok(());
		}
		self.spread_cancel(&nodes);
		let mut copy_in: Option<Message> = None;
		let mut error: Option<Message> = None;
		let mut copying: Vec<NodeId> = Vec::new();
		for n in &nodes {
			self.send(*n, &[wire::query(&sql)]).await?;
			let conn = self.conns.get_mut(n).expect("connected");
			conn.prepared.remove("");
			loop {
				let m = conn.reader.next().await?;
				match m.tag {
					b'G' => {
						copy_in.get_or_insert(m);
						copying.push(*n);
						break;
					}
					b'E' => {
						error.get_or_insert(m);
					}
					b'Z' => break,
					b'N' => put_message(&mut self.out, &m),
					_ => {}
				}
			}
		}
		if error.is_some() || copy_in.is_none() {
			// A node refused the COPY: end it on the others and answer with the refusal.
			self.copy_abort(&copying, "a node refused the COPY").await?;
			if implicit {
				self.rollback_nodes(&nodes).await;
			} else {
				self.tx_status = b'E';
			}
			self.unspread_cancel();
			let e = error
				.unwrap_or_else(|| refusal_message("XX000", "a node did not start the COPY", ""));
			self.answer(
				&batch,
				simple,
				None,
				Outcome::Failed {
					at_parse: true,
					error: e,
				},
			);
			return Ok(());
		}
		// The client is told to send its data.
		if !simple {
			for m in &batch {
				match m.tag {
					b'P' => self
						.out
						.extend_from_slice(&Message::new(b'1', vec![]).encode()),
					b'B' => self
						.out
						.extend_from_slice(&Message::new(b'2', vec![]).encode()),
					b'D' => self
						.out
						.extend_from_slice(&Message::new(b'n', vec![]).encode()),
					_ => {}
				}
			}
		}
		self.out
			.extend_from_slice(&copy_in.expect("checked").encode());
		self.copy_split = Some(CopySplit {
			nodes,
			splitter: route::copy::Splitter::new(c.options, key_col),
			keyspace,
			pending: HashMap::new(),
			simple,
			implicit,
			error: None,
		});
		Ok(())
	}

	/// Ends every node's COPY with CopyFail and reads each answer to its ReadyForQuery.
	async fn copy_abort(&mut self, nodes: &[NodeId], why: &str) -> Result<(), WireError> {
		for n in nodes {
			let fail = Body::new().cstr(why).message(b'f');
			self.send(*n, &[fail]).await?;
		}
		for n in nodes {
			let conn = self.conns.get_mut(n).expect("connected");
			loop {
				let m = conn.reader.next().await?;
				if m.tag == b'Z' {
					break;
				}
			}
		}
		Ok(())
	}

	/// One message from the client while a split COPY runs.
	async fn copy_message(&mut self, m: Message) -> Result<(), WireError> {
		let Some(mut cs) = self.copy_split.take() else {
			return Ok(());
		};
		match m.tag {
			b'd' => {
				if cs.error.is_none() {
					match cs.splitter.push(&m.body) {
						Ok(pieces) => self.copy_route(&mut cs, pieces),
						Err(e) => cs.error = Some(refusal_message("22P04", &e, "")),
					}
					for n in cs.nodes.clone() {
						if cs.pending.get(&n).is_some_and(|p| p.len() >= COPY_CHUNK) {
							let bytes = cs.pending.remove(&n).unwrap_or_default();
							self.send_raw(n, &bytes).await?;
						}
					}
				}
				self.copy_split = Some(cs);
			}
			b'c' => {
				if cs.error.is_none() {
					match cs.splitter.finish() {
						Ok(pieces) => self.copy_route(&mut cs, pieces),
						Err(e) => cs.error = Some(refusal_message("22P04", &e, "")),
					}
				}
				self.copy_end(cs, None).await?;
			}
			b'f' => {
				let why = cstr(&m.body);
				self.copy_end(cs, Some(why)).await?;
			}
			// Postgres ignores these during COPY: a client may send them without noticing the
			// statement was a COPY.
			b'H' | b'S' => self.copy_split = Some(cs),
			other => {
				return Err(WireError::Protocol(format!(
					"unexpected message '{}' during COPY",
					char::from(other)
				)));
			}
		}
		Ok(())
	}

	/// Each piece to the node buffers it belongs in.
	fn copy_route(&self, cs: &mut CopySplit, pieces: Vec<route::copy::Piece>) {
		use route::copy::{Key, Piece};
		let frame = |bytes: &[u8]| {
			let mut out = Vec::with_capacity(bytes.len() + 5);
			out.push(b'd');
			out.extend_from_slice(&((bytes.len() + 4) as u32).to_be_bytes());
			out.extend_from_slice(bytes);
			out
		};
		for p in pieces {
			match p {
				Piece::Everyone(bytes) | Piece::End(bytes) => {
					for n in &cs.nodes {
						cs.pending.entry(*n).or_default().extend(frame(&bytes));
					}
				}
				Piece::Row { key, bytes } => {
					let owner = match &key {
						Key::Null => {
							Err("a row's shard key is NULL, so it has no node".to_string())
						}
						Key::Text(v) => self
							.catalog
							.owner(&cs.keyspace, v)
							.map_err(|e| e.to_string()),
						Key::Binary(v) => self
							.catalog
							.owner_binary(&cs.keyspace, v)
							.map_err(|e| e.to_string()),
					};
					match owner {
						Ok(n) if cs.nodes.contains(&n) => {
							cs.pending.entry(n).or_default().extend(frame(&bytes));
						}
						Ok(n) => {
							cs.error = Some(refusal_message(
								route::NOT_ACROSS_NODES,
								&format!("a row belongs to {n}, which this COPY does not write"),
								"Try again: the cluster changed while the COPY ran.",
							));
							return;
						}
						Err(e) => {
							cs.error = Some(refusal_message(
								route::NOT_ACROSS_NODES,
								&format!("COPY across nodes: {e}"),
								"Give every row its shard key.",
							));
							return;
						}
					}
				}
			}
		}
	}

	/// CopyDone (`fail` None) or CopyFail from the client: each node finishes, the nodes commit
	/// together or none does, and the client gets one answer.
	async fn copy_end(&mut self, cs: CopySplit, fail: Option<String>) -> Result<(), WireError> {
		let failing = fail.is_some() || cs.error.is_some();
		let mut pending = cs.pending;
		for n in &cs.nodes {
			let mut bytes = pending.remove(n).unwrap_or_default();
			let end = if failing {
				Body::new()
					.cstr("the COPY across nodes failed")
					.message(b'f')
			} else {
				Message::new(b'c', vec![])
			};
			bytes.extend_from_slice(&end.encode());
			self.send_raw(*n, &bytes).await?;
		}
		let mut total: u64 = 0;
		let mut error: Option<Message> = None;
		for n in &cs.nodes {
			let conn = self.conns.get_mut(n).expect("connected");
			loop {
				let m = conn.reader.next().await?;
				match m.tag {
					b'C' => {
						total += cstr(&m.body)
							.rsplit(' ')
							.next()
							.and_then(|v| v.parse::<u64>().ok())
							.unwrap_or(0);
					}
					b'E' => {
						error.get_or_insert(m);
					}
					b'N' => put_message(&mut self.out, &m),
					b'Z' => break,
					_ => {}
				}
			}
		}
		self.unspread_cancel();
		let error = match (fail, cs.error) {
			(Some(why), _) => Some(refusal_message(
				"57014",
				&format!("COPY from stdin failed: {why}"),
				"",
			)),
			(None, Some(e)) => Some(e),
			(None, None) => error,
		};
		let error = match error {
			Some(e) => {
				if cs.implicit {
					self.rollback_nodes(&cs.nodes).await;
				} else {
					self.tx_status = b'E';
				}
				Some(e)
			}
			None if cs.implicit => {
				let coordinator = self.coordinator();
				let nodes = cs.nodes.clone();
				let mut parts: Vec<Part> = self
					.conns
					.iter_mut()
					.filter(|(id, _)| nodes.contains(id))
					.map(|(id, conn)| Part { node: *id, conn })
					.collect();
				coordinator
					.commit(&mut parts)
					.await
					.err()
					.map(|e| e.to_message())
			}
			None => None,
		};
		let sent_error = error.is_some();
		match error {
			Some(e) => self.out.extend_from_slice(&e.encode()),
			None => self.out.extend_from_slice(
				&Body::new()
					.cstr(&format!("COPY {total}"))
					.message(b'C')
					.encode(),
			),
		}
		if cs.simple {
			let z = Body::new().byte(self.tx_status_now()).message(b'Z');
			self.out.extend_from_slice(&z.encode());
		} else if sent_error {
			// After an error in the extended protocol, everything up to the next Sync is
			// skipped, as Postgres skips it.
			self.skip_until_sync = true;
		}
		Ok(())
	}

	async fn send_raw(&mut self, node: NodeId, bytes: &[u8]) -> Result<(), WireError> {
		let c = self.conns.get_mut(&node).expect("connected");
		c.reader.get_mut().write_all(bytes).await?;
		c.reader.get_mut().flush().await?;
		Ok(())
	}
}

// ---------------------------------------------------------------------------------------------
// Routing by a JWT claim (the cluster settings `route_claim`, `route_claim_keyspace`).
//
// The data API opens each request's transaction with `set_config('request.jwt.claims', …, true)`
// and lets row-level security keep a tenant to its rows. With `route_claim` naming the claim
// that holds the shard key (`tenant_id`, say), the router reads it there and sends the whole
// transaction to that tenant's node: a statement on that keyspace's tables that names no key, or
// would be read across nodes, runs on the tenant's node alone, and one that names another key is
// refused. The claims are set on every node the transaction reaches.

/// The shard key a transaction's JWT claim names.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Claim {
	keyspace: String,
	value: String,
	node: NodeId,
	/// Set for the transaction (`set_config(…, true)`, SET LOCAL), not the session.
	local: bool,
	/// The claims, as set.
	json: String,
}

impl Router {
	/// The claim a statement's `request.jwt.claims` names, when the cluster routes by one.
	fn claim_from(&self, json: &str, local: bool) -> Option<Result<Claim, route::Refusal>> {
		let name = self.settings.route_claim.as_ref()?;
		let v: serde_json::Value = serde_json::from_str(json).ok()?;
		let value = match v.get(name)? {
			serde_json::Value::String(s) => s.clone(),
			serde_json::Value::Number(n) => n.to_string(),
			_ => return None,
		};
		let keyspace = match &self.settings.route_claim_keyspace {
			Some(k) => k.clone(),
			None if self.catalog.keyspaces.len() == 1 => self
				.catalog
				.keyspaces
				.keys()
				.next()
				.cloned()
				.unwrap_or_default(),
			None => {
				return Some(Err(route::Refusal {
					code: "F0000",
					message: format!(
						"the cluster routes by the JWT claim {name} but has several keyspaces"
					),
					hint: "Name the claim's keyspace in the cluster setting route_claim_keyspace."
						.into(),
				}));
			}
		};
		Some(
			self.catalog
				.owner(&keyspace, &value)
				.map(|node| Claim {
					keyspace: keyspace.clone(),
					value: value.clone(),
					node,
					local,
					json: json.to_string(),
				})
				.map_err(|e| route::Refusal {
					code: "22023",
					message: format!(
						"the JWT claim {name} = {value} is not a key of {keyspace}: {e}"
					),
					hint: "Issue the token with a valid key.".into(),
				}),
		)
	}

	/// A statement's route inside a transaction routed by a claim.
	fn claimed(
		&self,
		r: Route,
		facts: &route::Facts,
		params: &[ParamValue],
		c: &Claim,
	) -> Result<Route, route::Refusal> {
		let mut mine = false;
		let mut other = false;
		for t in &facts.tables {
			match self.catalog.relations.get(&t.name) {
				Some(crate::catalog::RelationKind::Sharded { keyspace, .. })
					if *keyspace == c.keyspace =>
				{
					mine = true;
					for v in t.key_values.iter().flatten() {
						if !self.claim_matches(v, params, c) {
							return Err(route::Refusal {
								code: "42501",
								message: format!(
									"this statement names another key of {} than the transaction's JWT claim ({})",
									c.keyspace, c.value
								),
								hint: "A transaction routed by its JWT claim may name only that claim's key."
									.into(),
							});
						}
					}
				}
				Some(crate::catalog::RelationKind::Sharded { .. }) => other = true,
				_ => {}
			}
		}
		if !mine || other {
			return Ok(r);
		}
		Ok(match r {
			Route::Node(_)
			| Route::Scatter(_)
			| Route::SplitCopy(_)
			| Route::Fanout {
				same_count: false, ..
			} => Route::Node(c.node),
			r => r,
		})
	}

	fn claim_matches(&self, v: &route::KeyValue, params: &[ParamValue], c: &Claim) -> bool {
		use crate::hash::KeyType;
		let key_type = self.catalog.keyspaces.get(&c.keyspace).map(|k| k.key_type);
		let same = |s: &str| match key_type {
			Some(KeyType::Int2 | KeyType::Int4 | KeyType::Int8) => {
				match (s.trim().parse::<i128>(), c.value.trim().parse::<i128>()) {
					(Ok(a), Ok(b)) => a == b,
					_ => false,
				}
			}
			Some(KeyType::Numeric) => match (
				merge::Dec::parse(s.trim()),
				merge::Dec::parse(c.value.trim()),
			) {
				(Some(a), Some(b)) => a.order(&b) == std::cmp::Ordering::Equal,
				_ => false,
			},
			Some(KeyType::Uuid) => {
				let n = |x: &str| {
					x.chars()
						.filter(|ch| ch.is_ascii_hexdigit())
						.collect::<String>()
						.to_ascii_lowercase()
				};
				n(s) == n(&c.value)
			}
			Some(KeyType::Bpchar) => s.trim_end_matches(' ') == c.value.trim_end_matches(' '),
			_ => s == c.value,
		};
		match v {
			route::KeyValue::Const(s) => same(s),
			route::KeyValue::Cast(inner, _) => self.claim_matches(inner, params, c),
			route::KeyValue::Param(i) => match i
				.checked_sub(1)
				.and_then(|i| params.get(i))
				.map(ParamValue::value)
			{
				Some(ParamValue::Text(t)) => same(t),
				// A binary value: its node must be the claim's (its exact value is not read here).
				Some(ParamValue::Binary(b)) => {
					self.catalog.owner_binary(&c.keyspace, b).ok() == Some(c.node)
				}
				_ => false,
			},
			route::KeyValue::Null => false,
		}
	}

	/// What sets the claims on a node the transaction (or session) reaches later.
	fn claim_replay(c: &Claim) -> String {
		format!(
			"select pg_catalog.set_config('request.jwt.claims', {}, {})",
			crate::catalog::quote_literal(&c.json),
			c.local
		)
	}
}

type Analysed = Result<Vec<Statement>, AnalyzeError>;

#[derive(Default)]
struct Acc {
	targets: Vec<NodeId>,
	tx_verbs: Vec<TxVerb>,
	sessions: Vec<(SessionVerb, String)>,
	data: bool,
	/// Statements other than transaction control and session state.
	statements: usize,
	/// Reads across nodes: the nodes and the statement.
	gathers: Vec<(Vec<NodeId>, String)>,
	/// EXPLAINs of a read on one node.
	explains: Vec<(NodeId, String)>,
	/// Writes every one of several nodes runs.
	fanouts: Vec<Fanout>,
	/// Statements for a node other than home that call a sequence (L15).
	off_home: Vec<String>,
	/// Schema changes that cannot run in a transaction block, for every node (a job each).
	jobs: Vec<String>,
	/// Role changes, to copy to every node once home has them.
	roles: Vec<RoleChange>,
	/// Indexes whose tables decide where a statement goes (`INDEX_OWNERS_SQL`).
	index_owners: Vec<crate::catalog::RelationName>,
	/// COPY … FROM STDIN into a sharded table, for its nodes.
	copies: Vec<(Vec<NodeId>, String)>,
	/// The JWT claim this batch set (`route_claim`).
	claim: Option<Claim>,
}

/// A write several nodes run as it is (route.rs `Route::Fanout`).
#[derive(Clone, Debug)]
struct Fanout {
	nodes: Vec<NodeId>,
	sql: String,
	/// A reference table's copies: every node must change the same number of rows.
	same_count: bool,
	insert: bool,
	written: Vec<crate::catalog::RelationName>,
	/// Each node's own statement (an INSERT split by its rows), else `sql` everywhere.
	per_node: Option<Vec<String>>,
	/// For a split INSERT: the original row numbers each node got, to put RETURNING back in
	/// the order of the VALUES.
	rows: Option<Vec<Vec<usize>>>,
	/// A schema change (ddl.rs): every node, one transaction each, committed together.
	ddl: bool,
}

enum Event {
	Client(Message),
	Node(NodeId, Message),
	/// The client stayed idle past `PARK`: its node sessions go to the pool.
	Idle,
}

enum Plan {
	Node {
		node: NodeId,
		sessions: Vec<(SessionVerb, String)>,
		rolls_back: bool,
		begin: Option<String>,
		/// The open transaction takes in this node first.
		join: bool,
	},
	/// COMMIT (two-phase) or ROLLBACK of a transaction on several nodes.
	EndTx {
		commit: bool,
	},
	/// A write several nodes run, committed together.
	Fanout(Fanout),
	/// A schema change each node runs on its own, outside a transaction (ddl.rs jobs).
	Job(String),
	/// Lepis must learn which tables these indexes are on (home node), then plan again.
	NeedIndexOwners(Vec<crate::catalog::RelationName>),
	/// COPY … FROM STDIN split by row.
	Copy {
		nodes: Vec<NodeId>,
		sql: String,
	},
	Local {
		tx_verbs: Vec<TxVerb>,
	},
	/// A read Lepis answers from several nodes' answers (or an EXPLAIN it annotates).
	Gather {
		nodes: Vec<NodeId>,
		sql: String,
	},
	Refuse(route::Refusal),
}

fn command_tag(v: Option<TxVerb>) -> &'static str {
	match v {
		Some(TxVerb::Begin) => "BEGIN",
		Some(TxVerb::Commit) => "COMMIT",
		Some(TxVerb::Rollback) => "ROLLBACK",
		_ => "SET",
	}
}

fn cstr(b: &[u8]) -> String {
	let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
	utf8(&b[..end])
}

/// Bytes as a String: as they are when they are UTF-8 (nearly always), else lossily.
fn utf8(b: &[u8]) -> String {
	match std::str::from_utf8(b) {
		Ok(s) => s.to_owned(),
		Err(_) => String::from_utf8_lossy(b).into_owned(),
	}
}

/// Appends a message to an output buffer as it goes on the wire (no copy of its own first).
fn put_message(out: &mut Vec<u8>, m: &Message) {
	out.push(m.tag);
	out.extend_from_slice(&((m.body.len() + 4) as u32).to_be_bytes());
	out.extend_from_slice(&m.body);
}

/// A Bind's portal and statement names, without reading its parameters.
fn bind_names(body: &[u8]) -> Result<(String, String), WireError> {
	let (portal, rest) = take_cstr(body)?;
	let (stmt, _) = take_cstr(rest)?;
	Ok((portal, stmt))
}

fn take_cstr(b: &[u8]) -> Result<(String, &[u8]), WireError> {
	let end = b
		.iter()
		.position(|&c| c == 0)
		.ok_or_else(|| WireError::Protocol("unterminated string".into()))?;
	Ok((utf8(&b[..end]), &b[end + 1..]))
}

/// Parse: (statement name, SQL).
fn parse_parse(body: &[u8]) -> Result<(String, String), WireError> {
	let (name, rest) = take_cstr(body)?;
	let (sql, _) = take_cstr(rest)?;
	Ok((name, sql))
}

/// Bind: (portal, statement, parameters as the client sent them).
fn parse_bind(body: &[u8]) -> Result<(String, String, Vec<ParamValue>), WireError> {
	let bad = || WireError::Protocol("malformed Bind".into());
	let (portal, r) = take_cstr(body)?;
	let (stmt, r) = take_cstr(r)?;
	let u16_at = |r: &[u8], i: usize| -> Result<u16, WireError> {
		r.get(i..i + 2)
			.map(|b| u16::from_be_bytes([b[0], b[1]]))
			.ok_or_else(bad)
	};
	let nformats = u16_at(r, 0)? as usize;
	let mut formats = Vec::with_capacity(nformats);
	for i in 0..nformats {
		formats.push(u16_at(r, 2 + i * 2)?);
	}
	let mut r = &r[2 + nformats * 2..];
	let nparams = u16_at(r, 0)? as usize;
	// Zero format codes, one for all, or exactly one per parameter: Postgres refuses anything else.
	if nformats > 1 && nformats != nparams {
		return Err(bad());
	}
	r = &r[2..];
	let mut params = Vec::with_capacity(nparams);
	for i in 0..nparams {
		let len = r
			.get(0..4)
			.map(|b| i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
			.ok_or_else(bad)?;
		r = &r[4..];
		if len < 0 {
			params.push(ParamValue::Null);
			continue;
		}
		let len = len as usize;
		let v = r.get(..len).ok_or_else(bad)?.to_vec();
		r = &r[len..];
		let format = match formats.len() {
			0 => 0,
			1 => formats[0],
			_ => *formats.get(i).ok_or_else(bad)?,
		};
		params.push(if format == 1 {
			ParamValue::Binary(v)
		} else {
			ParamValue::Text(String::from_utf8_lossy(&v).into_owned())
		});
	}
	Ok((portal, stmt, params))
}

/// The portal an Execute, a Describe 'P' or a Close 'P' names.
fn portal_ref(m: &Message) -> Option<String> {
	match m.tag {
		b'E' => Some(cstr(&m.body)),
		b'D' | b'C' if m.body.first() == Some(&b'P') => Some(cstr(&m.body[1..])),
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// The router's own cost per transaction, without a network: a client and a node made of
	/// in-memory pipes, the node answering a Bind/Execute/Sync at once, as pgbench -S -M prepared
	/// sends it. `cargo test --release -p snout-lepis --lib -- --ignored --nocapture
	/// router_cost` prints microseconds per transaction.
	#[tokio::test(flavor = "current_thread")]
	#[ignore]
	async fn router_cost_per_transaction() {
		use crate::catalog::{Keyspace, Node, NodeState, RelationKind, RelationName, Strategy};
		use crate::config::{Config, SslMode};
		use tokio::io::AsyncWriteExt;

		let config: HashMap<String, String> = [
			("LEPIS_HOME", "127.0.0.1:1"),
			("LEPIS_HOME_SSLMODE", "disable"),
			("LEPIS_SERVICE_USER", "postgres"),
			("LEPIS_SERVICE_PASSWORD", "x"),
		]
		.into_iter()
		.map(|(k, v)| (k.to_string(), v.to_string()))
		.collect();
		let app = App::new(Config::from_map(&config).unwrap()).unwrap();
		let mut c = Catalog {
			epoch: 1,
			..Default::default()
		};
		c.nodes.insert(
			NodeId(1),
			Node {
				id: NodeId(1),
				name: "n1".into(),
				host: "127.0.0.1".into(),
				port: 1,
				dbname: "bench".into(),
				sslmode: SslMode::Disable,
				home: true,
				state: NodeState::Active,
				server_version_num: Some(180_000),
			},
		);
		c.keyspaces.insert(
			"acct".into(),
			Keyspace {
				name: "acct".into(),
				strategy: Strategy::Hash,
				key_type: crate::hash::KeyType::Int4,
				seed: 0,
				ranges: Keyspace::even_ranges(1, &[NodeId(1)]),
				pins: HashMap::new(),
			},
		);
		c.relations.insert(
			RelationName {
				schema: "public".into(),
				table: "pgbench_accounts".into(),
			},
			RelationKind::Sharded {
				keyspace: "acct".into(),
				key_column: "aid".into(),
			},
		);
		let catalog = Arc::new(c);
		app.set_catalog(Some((*catalog).clone()));

		let (client, router_side) = tokio::io::duplex(1 << 16);
		let (node_side, node) = tokio::io::duplex(1 << 16);
		let floor = std::env::var("LEPIS_BENCH_FLOOR").is_ok();
		let (router_side, router_side_floor) = if floor {
			(tokio::io::duplex(16).0, router_side)
		} else {
			let d = tokio::io::duplex(16).0;
			(router_side, d)
		};
		let (node_side, node_side_floor) = if floor {
			(tokio::io::duplex(16).0, node_side)
		} else {
			let d = tokio::io::duplex(16).0;
			(node_side, d)
		};
		let home = Backend {
			stream: Box::new(node_side),
			pid: 7,
			key: vec![0; 4],
			minor: 0,
			opening: Vec::new(),
			ready: Body::new().byte(b'I').message(b'Z'),
		};
		let secret = crate::scram::ClientSecret::from_password("x", b"salt", 4096);
		let router = Router::new(
			app.clone(),
			catalog,
			secret,
			vec![
				("user".into(), "bench".into()),
				("database".into(), "postgres".into()),
			],
			Box::new(router_side),
			home,
			NodeId(1),
			1,
			vec![0; 4],
		);
		// LEPIS_BENCH_FLOOR=1: the same pipes with nothing in between but a byte copy, for what
		// the harness itself costs.
		if std::env::var("LEPIS_BENCH_FLOOR").is_ok() {
			drop(router);
			let (mut a, mut b) = (router_side_floor, node_side_floor);
			tokio::spawn(async move { tokio::io::copy_bidirectional(&mut a, &mut b).await });
		} else {
			tokio::spawn(router.run());
		}
		// The node: answers each Sync with what a prepared single-row SELECT returns; the
		// search_path query with one row.
		tokio::spawn(async move {
			let mut r = FrameReader::new(node);
			let mut pending: Vec<u8> = Vec::new();
			loop {
				let Ok(m) = r.next().await else { return };
				match m.tag {
					b'P' => put_message(&mut pending, &Message::new(b'1', vec![])),
					b'B' => put_message(&mut pending, &Message::new(b'2', vec![])),
					b'D' => put_message(&mut pending, &Message::new(b'n', vec![])),
					b'E' => {
						put_message(&mut pending, &data_row(&[Some(b"42".to_vec())]));
						put_message(&mut pending, &Body::new().cstr("SELECT 1").message(b'C'));
					}
					b'Q' => {
						put_message(&mut pending, &data_row(&[Some(b"public".to_vec())]));
						put_message(&mut pending, &Body::new().cstr("SELECT 1").message(b'C'));
						put_message(&mut pending, &Body::new().byte(b'I').message(b'Z'));
					}
					b'S' => put_message(&mut pending, &Body::new().byte(b'I').message(b'Z')),
					_ => {}
				}
				if matches!(m.tag, b'S' | b'Q') {
					if r.get_mut().write_all(&pending).await.is_err() {
						return;
					}
					pending.clear();
				}
			}
		});
		let mut client = FrameReader::new(client);
		let sql = "SELECT abalance FROM pgbench_accounts WHERE aid = $1;";
		let mut prepare = Vec::new();
		put_message(&mut prepare, &parse_message(sql, &[23]));
		put_message(&mut prepare, &Message::new(b'S', vec![]));
		let mut prepare_named = Vec::new();
		put_message(
			&mut prepare_named,
			&Body::new()
				.cstr("P0_1")
				.cstr(sql)
				.bytes(&1u16.to_be_bytes())
				.u32(23)
				.message(b'P'),
		);
		put_message(&mut prepare_named, &Message::new(b'S', vec![]));
		client.get_mut().write_all(&prepare_named).await.unwrap();
		while client.next().await.unwrap().tag != b'Z' {}
		let txn = |aid: i32| {
			let mut b = Vec::new();
			put_message(
				&mut b,
				&Body::new()
					.cstr("")
					.cstr("P0_1")
					.bytes(&1u16.to_be_bytes())
					.bytes(&1u16.to_be_bytes())
					.bytes(&1u16.to_be_bytes())
					.i32(4)
					.bytes(&aid.to_be_bytes())
					.bytes(&0u16.to_be_bytes())
					.message(b'B'),
			);
			put_message(&mut b, &execute_message());
			put_message(&mut b, &Message::new(b'S', vec![]));
			b
		};
		let _ = prepare;
		for i in 0..2000 {
			client.get_mut().write_all(&txn(i)).await.unwrap();
			while client.next().await.unwrap().tag != b'Z' {}
		}
		let n = 50_000;
		let t = std::time::Instant::now();
		for i in 0..n {
			client.get_mut().write_all(&txn(i)).await.unwrap();
			while client.next().await.unwrap().tag != b'Z' {}
		}
		let per = t.elapsed().as_secs_f64() * 1e6 / f64::from(n);
		eprintln!(
			"router: {per:.2} µs per Bind/Execute/Sync (client, router and node on one thread)"
		);
	}

	#[test]
	fn bind_parameters_in_both_formats() {
		let body = Body::new()
			.cstr("p")
			.cstr("s")
			.bytes(&2u16.to_be_bytes())
			.bytes(&0u16.to_be_bytes())
			.bytes(&1u16.to_be_bytes())
			.bytes(&3u16.to_be_bytes())
			.i32(2)
			.bytes(b"42")
			.i32(8)
			.bytes(&7i64.to_be_bytes())
			.i32(-1)
			.bytes(&0u16.to_be_bytes())
			.finish();
		// Two format codes for three parameters is a client error Postgres also rejects.
		assert!(parse_bind(&body).is_err());
		let body = Body::new()
			.cstr("")
			.cstr("s")
			.bytes(&1u16.to_be_bytes())
			.bytes(&1u16.to_be_bytes())
			.bytes(&2u16.to_be_bytes())
			.i32(8)
			.bytes(&7i64.to_be_bytes())
			.i32(-1)
			.bytes(&0u16.to_be_bytes())
			.finish();
		let (portal, stmt, params) = parse_bind(&body).unwrap();
		assert_eq!((portal.as_str(), stmt.as_str()), ("", "s"));
		assert_eq!(
			params,
			vec![
				ParamValue::Binary(7i64.to_be_bytes().to_vec()),
				ParamValue::Null
			]
		);
	}
}
