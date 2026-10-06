//! Lepis as a client of a node: connect, encrypt, authenticate, and read the session's opening
//! messages, which the router hands on to its own client.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::config::{NodeAddress, SslMode};
use crate::scram::{self, ClientCredential, ClientExchange};
use crate::tls;
use crate::wire::{self, MAX_CANCEL_KEY_LEN, MAX_SETUP_MESSAGE_LEN, Message, PROTOCOL_3_2};

/// Anything a session can run over: plain TCP or TLS, either side.
pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncStream for T {}
pub type BoxStream = Box<dyn AsyncStream>;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub enum BackendError {
	/// The node could not be reached, or the connection broke.
	Unreachable(String),
	/// The node said no, in its own words (forwarded to the client as it is).
	Refused(Message),
	/// Something Lepis cannot do with this node, as a sentence.
	Unsupported(String),
}

impl std::fmt::Display for BackendError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			BackendError::Unreachable(m) | BackendError::Unsupported(m) => f.write_str(m),
			BackendError::Refused(m) => {
				let fields = wire::parse_error_fields(&m.body);
				let msg = fields
					.iter()
					.find(|(k, _)| *k == b'M')
					.map_or("", |(_, v)| v.as_str());
				write!(f, "the node refused: {msg}")
			}
		}
	}
}

impl From<wire::WireError> for BackendError {
	fn from(e: wire::WireError) -> Self {
		BackendError::Unreachable(e.to_string())
	}
}

/// A connected, authenticated node session.
pub struct Backend {
	pub stream: BoxStream,
	pub pid: i32,
	pub key: Vec<u8>,
	/// The protocol minor version the node agreed to (0 or 2).
	pub minor: u32,
	/// `ParameterStatus` and `NoticeResponse` messages from the opening, in order.
	pub opening: Vec<Message>,
	/// The `ReadyForQuery` that ended the opening.
	pub ready: Message,
}

/// What a simple-protocol query string answered (`Backend::simple`).
#[derive(Debug, Default)]
pub struct Simple {
	/// Every row of every statement, as text.
	pub rows: Vec<Vec<Option<String>>>,
	/// The command tag of each statement that completed, in order.
	pub tags: Vec<String>,
	/// The transaction status ReadyForQuery reported: `I`, `T` or `E`.
	pub status: u8,
}

impl BackendError {
	/// The SQLSTATE of a node's refusal.
	pub fn sqlstate(&self) -> Option<String> {
		match self {
			BackendError::Refused(m) => wire::parse_error_fields(&m.body)
				.into_iter()
				.find(|(k, _)| *k == b'C')
				.map(|(_, v)| v),
			_ => None,
		}
	}
}

/// Opens a raw stream to a node, encrypted as its `sslmode` says.
pub async fn open(
	node: &NodeAddress,
	tls_config: Option<&Arc<rustls::ClientConfig>>,
) -> Result<BoxStream, BackendError> {
	let address = format!("{}:{}", node.host, node.port);
	let tcp = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(&address))
		.await
		.map_err(|_| BackendError::Unreachable(format!("{address}: timed out")))?
		.map_err(|e| BackendError::Unreachable(format!("{address}: {e}")))?;
	tcp.set_nodelay(true).ok();
	let Some(config) = tls_config else {
		return Ok(Box::new(tcp));
	};
	let mut tcp = tcp;
	tcp.write_all(&wire::ssl_request())
		.await
		.map_err(|e| BackendError::Unreachable(format!("{address}: {e}")))?;
	let answer = tcp
		.read_u8()
		.await
		.map_err(|e| BackendError::Unreachable(format!("{address}: {e}")))?;
	if answer != b'S' {
		return Err(BackendError::Unsupported(format!(
			"{address} does not accept TLS (sslmode={})",
			if node.sslmode == SslMode::Require {
				"require"
			} else {
				"verify-full"
			}
		)));
	}
	let name = tls::server_name(&node.host).map_err(BackendError::Unsupported)?;
	let stream = TlsConnector::from(config.clone())
		.connect(name, tcp)
		.await
		.map_err(|e| BackendError::Unreachable(format!("{address}: TLS: {e}")))?;
	Ok(Box::new(stream))
}

/// Connects and logs in as `params`' `user`, with `credential`, and reads up to the first
/// `ReadyForQuery`. Protocol 3.2 is asked for; a node that only speaks 3.0 says so and is fine.
pub async fn connect(
	node: &NodeAddress,
	tls_config: Option<&Arc<rustls::ClientConfig>>,
	params: &[(String, String)],
	credential: ClientCredential,
) -> Result<Backend, BackendError> {
	let mut stream = open(node, tls_config).await?;
	wire::write_all(&mut stream, &wire::startup_message(PROTOCOL_3_2, params)).await?;

	let mut minor = 2;
	let mut opening = Vec::new();
	let mut key_data: Option<(i32, Vec<u8>)> = None;
	let mut exchange: Option<ClientExchange> = None;
	let mut credential = Some(credential);
	loop {
		let m = wire::read_message(&mut stream, MAX_SETUP_MESSAGE_LEN).await?;
		match m.tag {
			b'R' => {
				let code = m
					.body
					.get(0..4)
					.map(|b| i32::from_be_bytes(b.try_into().expect("four bytes")))
					.unwrap_or(-1);
				let data = &m.body[4.min(m.body.len())..];
				match code {
					0 => {}
					10 => {
						let mechanisms = wire::parse_cstr_list(data);
						if !mechanisms.iter().any(|x| x == scram::MECHANISM) {
							return Err(BackendError::Unsupported(format!(
								"{node} offers {mechanisms:?}; Lepis needs SCRAM-SHA-256"
							)));
						}
						let Some(c) = credential.take() else {
							return Err(BackendError::Unsupported(format!(
								"{node} asked for SCRAM twice"
							)));
						};
						let (ex, first) = ClientExchange::new(c);
						wire::write_all(
							&mut stream,
							&wire::sasl_initial_response(scram::MECHANISM, first.as_bytes())
								.encode(),
						)
						.await?;
						exchange = Some(ex);
					}
					11 => {
						let Some(ex) = exchange.as_mut() else {
							return Err(BackendError::Unsupported(format!(
								"{node} sent SASLContinue out of turn"
							)));
						};
						let reply = ex
							.respond(data)
							.map_err(|e| BackendError::Unsupported(format!("{node}: {e}")))?;
						wire::write_all(
							&mut stream,
							&wire::sasl_response(reply.as_bytes()).encode(),
						)
						.await?;
					}
					12 => {
						let Some(ex) = exchange.as_ref() else {
							return Err(BackendError::Unsupported(format!(
								"{node} sent SASLFinal out of turn"
							)));
						};
						ex.verify(data)
							.map_err(|e| BackendError::Unsupported(format!("{node}: {e}")))?;
					}
					3 => {
						// A cleartext password: only for Lepis's own service login, and only
						// over an encrypted connection.
						match (&credential, node.sslmode) {
							(
								Some(ClientCredential::Password(p)),
								SslMode::Require | SslMode::VerifyFull,
							) => {
								let p = p.clone();
								wire::write_all(&mut stream, &wire::password_message(&p).encode())
									.await?;
								credential = None;
							}
							_ => {
								return Err(BackendError::Unsupported(format!(
									"{node} asks for a cleartext password; set the role's authentication to scram-sha-256 in pg_hba.conf"
								)));
							}
						}
					}
					5 => {
						return Err(BackendError::Unsupported(format!(
							"{node} uses md5 authentication; Lepis needs scram-sha-256 (password_encryption and pg_hba.conf)"
						)));
					}
					other => {
						return Err(BackendError::Unsupported(format!(
							"{node} asks for authentication method {other}; Lepis needs scram-sha-256"
						)));
					}
				}
			}
			b'v' => {
				// NegotiateProtocolVersion: the newest minor the node speaks.
				if m.body.len() >= 4 {
					let v = u32::from_be_bytes(m.body[0..4].try_into().expect("four bytes"));
					minor = v & 0xffff;
				}
			}
			b'K' => {
				if m.body.len() < 8 || m.body.len() > 4 + MAX_CANCEL_KEY_LEN {
					return Err(BackendError::Unsupported(format!(
						"{node} sent a malformed BackendKeyData"
					)));
				}
				let pid = i32::from_be_bytes(m.body[0..4].try_into().expect("four bytes"));
				key_data = Some((pid, m.body[4..].to_vec()));
			}
			b'S' | b'N' => opening.push(m),
			b'E' => return Err(BackendError::Refused(m)),
			b'Z' => {
				let (pid, key) = key_data.unwrap_or((0, Vec::new()));
				return Ok(Backend {
					stream,
					pid,
					key,
					minor,
					opening,
					ready: m,
				});
			}
			other => {
				return Err(BackendError::Unsupported(format!(
					"{node} sent '{}' during startup",
					char::from(other)
				)));
			}
		}
	}
}

impl Backend {
	/// Runs one statement with text parameters through the extended protocol and returns its
	/// rows as text. For Lepis's own service queries, never a client's.
	pub async fn query(
		&mut self,
		sql: &str,
		params: &[&str],
	) -> Result<Vec<Vec<Option<String>>>, BackendError> {
		let parse = wire::Body::new()
			.cstr("")
			.cstr(sql)
			.bytes(&0u16.to_be_bytes())
			.message(b'P');
		let mut bind = wire::Body::new()
			.cstr("")
			.cstr("")
			.bytes(&0u16.to_be_bytes())
			.bytes(&(params.len() as u16).to_be_bytes());
		for p in params {
			bind = bind.i32(p.len() as i32).bytes(p.as_bytes());
		}
		let bind = bind.bytes(&0u16.to_be_bytes()).message(b'B');
		let execute = wire::Body::new().cstr("").i32(0).message(b'E');
		let sync = wire::Message::new(b'S', Vec::new());
		let mut out = Vec::new();
		for m in [parse, bind, execute, sync] {
			out.extend_from_slice(&m.encode());
		}
		wire::write_all(&mut self.stream, &out).await?;

		let mut rows = Vec::new();
		let mut error = None;
		loop {
			let m = wire::read_message(&mut self.stream, 16 * 1024 * 1024).await?;
			match m.tag {
				b'D' => {
					let row = wire::parse_data_row(&m.body)?;
					rows.push(
						row.into_iter()
							.map(|c| c.map(|b| String::from_utf8_lossy(&b).into_owned()))
							.collect(),
					);
				}
				b'E' => error = Some(m),
				b'Z' => break,
				_ => {}
			}
		}
		match error {
			Some(e) => Err(BackendError::Refused(e)),
			None => Ok(rows),
		}
	}

	/// Runs a query string through the simple protocol (several statements allowed) and reads to
	/// its ReadyForQuery. For Lepis's own statements (two-phase commit, role sync, DDL fan-out),
	/// never a client's batch. An error comes back as the node's own message, with the session
	/// ready again (inside a failed transaction, if one was open).
	pub async fn simple(&mut self, sql: &str) -> Result<Simple, BackendError> {
		wire::write_all(&mut self.stream, &wire::query(sql).encode()).await?;
		let mut out = Simple::default();
		let mut error = None;
		loop {
			let m = wire::read_message(&mut self.stream, 16 * 1024 * 1024).await?;
			match m.tag {
				b'D' => {
					let row = wire::parse_data_row(&m.body)?;
					out.rows.push(
						row.into_iter()
							.map(|c| c.map(|b| String::from_utf8_lossy(&b).into_owned()))
							.collect(),
					);
				}
				b'C' => {
					let tag = m.body.split(|b| *b == 0).next().unwrap_or_default();
					out.tags.push(String::from_utf8_lossy(tag).into_owned());
				}
				b'E' => error = Some(m),
				b'Z' => {
					out.status = m.body.first().copied().unwrap_or(b'I');
					break;
				}
				_ => {}
			}
		}
		match error {
			Some(e) => Err(BackendError::Refused(e)),
			None => Ok(out),
		}
	}

	pub async fn close(mut self) {
		let _ = wire::write_all(&mut self.stream, &wire::terminate().encode()).await;
	}
}
