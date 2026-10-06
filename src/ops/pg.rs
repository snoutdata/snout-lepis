//! The operations engine as a client of the nodes: Lepis's service login (the same one live.rs
//! reads the catalog with) to any node the catalog names, simple-protocol statements, rows as
//! text, and errors that keep their SQLSTATE so a step can tell "try again" from "stop".

use std::fmt;
use std::sync::Arc;

use crate::backend::{self, Backend};
use crate::catalog::Node;
use crate::config::NodeAddress;
use crate::scram::ClientCredential;
use crate::server::App;
use crate::wire;

pub type Rows = Vec<Vec<Option<String>>>;

/// What went wrong, with the node's SQLSTATE when a node said it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpError {
	pub code: Option<String>,
	pub message: String,
	/// For a refusal: what kind, stable and machine-readable (the admin API sends it).
	pub kind: Option<Kind>,
}

impl OpError {
	pub fn new(m: impl Into<String>) -> OpError {
		OpError {
			code: None,
			message: m.into(),
			kind: None,
		}
	}

	/// A refusal the caller can fix: wrong arguments, a precondition that does not hold.
	pub fn refused(kind: Kind, m: impl Into<String>) -> OpError {
		OpError {
			code: Some("0A000".into()),
			message: m.into(),
			kind: Some(kind),
		}
	}

	pub fn is(&self, code: &str) -> bool {
		self.code.as_deref() == Some(code)
	}
}

impl fmt::Display for OpError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match &self.code {
			Some(c) => write!(f, "{} ({c})", self.message),
			None => f.write_str(&self.message),
		}
	}
}

impl std::error::Error for OpError {}

impl From<wire::WireError> for OpError {
	fn from(e: wire::WireError) -> Self {
		OpError::new(e.to_string())
	}
}

fn node_error(m: &wire::Message, context: &str) -> OpError {
	let fields = wire::parse_error_fields(&m.body);
	let get = |k: u8| fields.iter().find(|(f, _)| *f == k).map(|(_, v)| v.clone());
	OpError {
		code: get(b'C'),
		message: format!("{context}: {}", get(b'M').unwrap_or_default()),
		kind: None,
	}
}

/// How the engine reaches one node: its address as the router sees it.
#[derive(Clone, Debug)]
pub struct Target {
	pub label: String,
	pub address: NodeAddress,
	pub dbname: String,
}

impl Target {
	pub fn of(app: &App, n: &Node) -> Target {
		Target {
			label: format!("{} ({}:{})", n.name, n.host, n.port),
			address: NodeAddress {
				host: n.host.clone(),
				port: n.port,
				sslmode: n.sslmode,
				ca_file: app.config.home.ca_file.clone(),
			},
			dbname: n.dbname.clone(),
		}
	}

	pub fn home(app: &App) -> Target {
		Target {
			label: format!("home ({})", app.config.home),
			address: app.config.home.clone(),
			dbname: app.config.service.database.clone(),
		}
	}
}

/// One session on one node as the service login.
pub struct Pg {
	b: Backend,
	pub label: String,
	address: NodeAddress,
}

impl Pg {
	pub async fn connect(app: &App, t: &Target) -> Result<Pg, OpError> {
		let tls = crate::tls::client_config(&t.address).map_err(OpError::new)?;
		let s = &app.config.service;
		let params = vec![
			("user".to_string(), s.user.clone()),
			("database".to_string(), t.dbname.clone()),
			("application_name".to_string(), "snout-lepis ops".to_string()),
		];
		let b = backend::connect(
			&t.address,
			tls.as_ref(),
			&params,
			ClientCredential::Password(s.password.clone()),
		)
		.await
		.map_err(|e| match e {
			backend::BackendError::Refused(m) => node_error(&m, &t.label),
			other => OpError::new(format!("{}: {other}", t.label)),
		})?;
		Ok(Pg {
			b,
			label: t.label.clone(),
			address: t.address.clone(),
		})
	}

	/// The node's process id for this session (what `pg_blocking_pids` names).
	pub fn pid(&self) -> i32 {
		self.b.pid
	}

	/// One or more statements in the simple protocol; the rows of all of them, as text.
	pub async fn simple(&mut self, sql: &str) -> Result<Rows, OpError> {
		wire::write_all(&mut self.b.stream, &wire::query(sql).encode()).await?;
		let mut rows = Vec::new();
		let mut error = None;
		loop {
			let m = wire::read_message(&mut self.b.stream, 64 * 1024 * 1024).await?;
			match m.tag {
				b'D' => rows.push(
					wire::parse_data_row(&m.body)?
						.into_iter()
						.map(|c| c.map(|v| String::from_utf8_lossy(&v).into_owned()))
						.collect(),
				),
				b'E' => error = Some(node_error(&m, &self.label)),
				b'Z' => break,
				_ => {}
			}
		}
		match error {
			Some(e) => Err(e),
			None => Ok(rows),
		}
	}

	/// One statement with text parameters (extended protocol).
	pub async fn query(&mut self, sql: &str, params: &[&str]) -> Result<Rows, OpError> {
		self.b.query(sql, params).await.map_err(|e| match e {
			backend::BackendError::Refused(m) => node_error(&m, &self.label),
			other => OpError::new(format!("{}: {other}", self.label)),
		})
	}

	/// The first column of the first row, if any.
	pub async fn value(&mut self, sql: &str, params: &[&str]) -> Result<Option<String>, OpError> {
		Ok(self
			.query(sql, params)
			.await?
			.into_iter()
			.next()
			.and_then(|r| r.into_iter().next().flatten()))
	}

	/// What it takes to cancel this session's statement from elsewhere, while the session itself
	/// is busy waiting for it.
	pub fn canceller(&self) -> Canceller {
		Canceller {
			address: self.address.clone(),
			pid: self.b.pid,
			key: self.b.key.clone(),
		}
	}

	pub async fn close(self) {
		self.b.close().await;
	}
}

/// The service login's credential for a connection string a node uses to reach another node (a
/// subscription). Only ever written into the node's own catalog, never logged.
pub fn conninfo(app: &Arc<App>, peer_host: &str, n: &Node) -> String {
	let q = |v: &str| format!("'{}'", v.replace('\\', "\\\\").replace('\'', "\\'"));
	let sslmode = match n.sslmode {
		crate::config::SslMode::Disable => "disable",
		crate::config::SslMode::Require => "require",
		crate::config::SslMode::VerifyFull => "verify-full",
	};
	format!(
		"host={} port={} dbname={} user={} password={} sslmode={sslmode} application_name={}",
		q(peer_host),
		n.port,
		q(&n.dbname),
		q(&app.config.service.user),
		q(&app.config.service.password),
		q("snout-lepis move"),
	)
}

/// A session's cancel key, held apart from the session.
pub struct Canceller {
	address: NodeAddress,
	pid: i32,
	key: Vec<u8>,
}

impl Canceller {
	pub async fn cancel(&self) {
		let tls = crate::tls::client_config(&self.address).ok().flatten();
		if let Ok(mut s) = backend::open(&self.address, tls.as_ref()).await {
			let _ = wire::write_all(&mut s, &wire::cancel_request(self.pid, &self.key)).await;
		}
	}
}

/// Why an operation was refused, as a stable snake_case name the admin API sends beside the
/// sentence, and the HTTP status it maps to: 404 for something that does not exist, 400 for input
/// that cannot be right, 409 for a request the cluster's current state does not allow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
	NoSuchJob,
	NoSuchKeyspace,
	NoSuchNode,
	NoSuchRange,
	NoSuchPin,
	NoSuchTable,
	NoSuchColumn,
	BadRequest,
	UnknownOperation,
	BadKeyType,
	BadKey,
	BadBound,
	BadSetting,
	NotAdjacent,
	RangesOnDifferentNodes,
	AlreadyDone,
	AlreadyDistributed,
	NotGlobal,
	NodeNotEmpty,
	HomeNode,
	NodeNameTaken,
	NoTargetNode,
	NothingToDo,
	JobNotFailed,
	TableNotMovable,
	TableNotShardable,
	TableHasRows,
	NodeUnsuitable,
	CatalogInvalid,
	KeyspaceExists,
	UnverifiedMoves,
	NoCatalog,
	RangeMoved,
	VerifyFailed,
	Cancelled,
}

impl Kind {
	pub const ALL: [Kind; 35] = [
		Kind::NoSuchJob,
		Kind::NoSuchKeyspace,
		Kind::NoSuchNode,
		Kind::NoSuchRange,
		Kind::NoSuchPin,
		Kind::NoSuchTable,
		Kind::NoSuchColumn,
		Kind::BadRequest,
		Kind::UnknownOperation,
		Kind::BadKeyType,
		Kind::BadKey,
		Kind::BadBound,
		Kind::BadSetting,
		Kind::NotAdjacent,
		Kind::RangesOnDifferentNodes,
		Kind::AlreadyDone,
		Kind::AlreadyDistributed,
		Kind::NotGlobal,
		Kind::NodeNotEmpty,
		Kind::HomeNode,
		Kind::NodeNameTaken,
		Kind::NoTargetNode,
		Kind::NothingToDo,
		Kind::JobNotFailed,
		Kind::TableNotMovable,
		Kind::TableNotShardable,
		Kind::TableHasRows,
		Kind::NodeUnsuitable,
		Kind::CatalogInvalid,
		Kind::KeyspaceExists,
		Kind::UnverifiedMoves,
		Kind::NoCatalog,
		Kind::RangeMoved,
		Kind::VerifyFailed,
		Kind::Cancelled,
	];

	pub fn name(self) -> &'static str {
		match self {
			Kind::NoSuchJob => "no_such_job",
			Kind::NoSuchKeyspace => "no_such_keyspace",
			Kind::NoSuchNode => "no_such_node",
			Kind::NoSuchRange => "no_such_range",
			Kind::NoSuchPin => "no_such_pin",
			Kind::NoSuchTable => "no_such_table",
			Kind::NoSuchColumn => "no_such_column",
			Kind::BadRequest => "bad_request",
			Kind::UnknownOperation => "unknown_operation",
			Kind::BadKeyType => "bad_key_type",
			Kind::BadKey => "bad_key",
			Kind::BadBound => "bad_bound",
			Kind::BadSetting => "bad_setting",
			Kind::NotAdjacent => "not_adjacent",
			Kind::RangesOnDifferentNodes => "ranges_on_different_nodes",
			Kind::AlreadyDone => "already_done",
			Kind::AlreadyDistributed => "already_distributed",
			Kind::NotGlobal => "not_global",
			Kind::NodeNotEmpty => "node_not_empty",
			Kind::HomeNode => "home_node",
			Kind::NodeNameTaken => "node_name_taken",
			Kind::NoTargetNode => "no_target_node",
			Kind::NothingToDo => "nothing_to_do",
			Kind::JobNotFailed => "job_not_failed",
			Kind::TableNotMovable => "table_not_movable",
			Kind::TableNotShardable => "table_not_shardable",
			Kind::TableHasRows => "table_has_rows",
			Kind::NodeUnsuitable => "node_unsuitable",
			Kind::CatalogInvalid => "catalog_invalid",
			Kind::KeyspaceExists => "keyspace_exists",
			Kind::UnverifiedMoves => "unverified_moves",
			Kind::NoCatalog => "no_catalog",
			Kind::RangeMoved => "range_moved",
			Kind::VerifyFailed => "verify_failed",
			Kind::Cancelled => "cancelled",
		}
	}

	pub fn http_status(self) -> u16 {
		match self {
			Kind::NoSuchJob
			| Kind::NoSuchKeyspace
			| Kind::NoSuchNode
			| Kind::NoSuchRange
			| Kind::NoSuchPin
			| Kind::NoSuchTable
			| Kind::NoSuchColumn => 404,
			Kind::BadRequest
			| Kind::UnknownOperation
			| Kind::BadKeyType
			| Kind::BadKey
			| Kind::BadBound
			| Kind::BadSetting => 400,
			_ => 409,
		}
	}
}
