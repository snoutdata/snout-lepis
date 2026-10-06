//! The Postgres wire protocol, the parts the router reads and writes itself.
//!
//! Lepis sits on both sides of every connection: it is the server to the client and a client to
//! each node. The startup packet, authentication and the session's opening messages are handled
//! message by message here; everything a node sends after that is relayed.
//!
//! Protocol 3.0 and 3.2 are spoken on both sides, independently: the only difference between
//! them is the length of the cancel key, and Lepis hands every client its own key (cancel.rs),
//! so a 3.2 client in front of a Postgres 13 node is fine.

use std::fmt;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Protocol 3.0, as the startup packet spells it.
pub const PROTOCOL_3_0: u32 = 196_608;
/// Protocol 3.2 (Postgres 18): longer cancel keys.
pub const PROTOCOL_3_2: u32 = 196_610;
const SSL_REQUEST: u32 = 80_877_103;
const GSSENC_REQUEST: u32 = 80_877_104;
const CANCEL_REQUEST: u32 = 80_877_102;

/// What Postgres itself allows for a startup packet.
const MAX_STARTUP_LEN: usize = 10_000;
/// The longest message read while a connection is being set up (authentication, the opening
/// parameter reports). After that the stream is relayed and never parsed here.
pub const MAX_SETUP_MESSAGE_LEN: usize = 64 * 1024;
/// The longest cancel key protocol 3.2 allows.
pub const MAX_CANCEL_KEY_LEN: usize = 256;

#[derive(Debug)]
pub enum WireError {
	Io(std::io::Error),
	Protocol(String),
}

impl fmt::Display for WireError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			WireError::Io(e) => write!(f, "{e}"),
			WireError::Protocol(m) => write!(f, "protocol violation: {m}"),
		}
	}
}

impl std::error::Error for WireError {}

impl From<std::io::Error> for WireError {
	fn from(e: std::io::Error) -> Self {
		WireError::Io(e)
	}
}

fn violation(m: impl Into<String>) -> WireError {
	WireError::Protocol(m.into())
}

/// The first packet a client sends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Startup {
	SslRequest,
	GssEncRequest,
	Cancel {
		pid: i32,
		key: Vec<u8>,
	},
	Startup {
		/// The version exactly as requested (major << 16 | minor).
		version: u32,
		params: Vec<(String, String)>,
	},
}

impl Startup {
	pub fn param<'a>(params: &'a [(String, String)], name: &str) -> Option<&'a str> {
		params
			.iter()
			.find(|(k, _)| k == name)
			.map(|(_, v)| v.as_str())
	}
}

pub async fn read_startup<S: AsyncRead + Unpin>(s: &mut S) -> Result<Startup, WireError> {
	let len = s.read_u32().await? as usize;
	if !(8..=MAX_STARTUP_LEN).contains(&len) {
		return Err(violation(format!("startup packet of {len} bytes")));
	}
	let mut body = vec![0u8; len - 4];
	s.read_exact(&mut body).await?;
	parse_startup(&body)
}

pub fn parse_startup(body: &[u8]) -> Result<Startup, WireError> {
	// read_startup never hands on fewer than 4 bytes, but this is public and fuzzed on its own.
	let Some((code, rest)) = body.split_first_chunk::<4>() else {
		return Err(violation("startup packet without a protocol code"));
	};
	let code = u32::from_be_bytes(*code);
	match code {
		SSL_REQUEST if rest.is_empty() => Ok(Startup::SslRequest),
		GSSENC_REQUEST if rest.is_empty() => Ok(Startup::GssEncRequest),
		CANCEL_REQUEST => {
			if rest.len() < 8 || rest.len() > 4 + MAX_CANCEL_KEY_LEN {
				return Err(violation("cancel request of the wrong length"));
			}
			let pid = i32::from_be_bytes(rest[0..4].try_into().expect("checked length"));
			Ok(Startup::Cancel {
				pid,
				key: rest[4..].to_vec(),
			})
		}
		version if version >> 16 == 3 => {
			let mut params = Vec::new();
			let mut r = rest;
			loop {
				let (name, after) = take_cstr(r)?;
				if name.is_empty() {
					if !after.is_empty() {
						return Err(violation("bytes after the startup parameters"));
					}
					break;
				}
				let (value, after) = take_cstr(after)?;
				params.push((name, value));
				r = after;
			}
			Ok(Startup::Startup { version, params })
		}
		other => Err(violation(format!(
			"unsupported protocol {}.{}",
			other >> 16,
			other & 0xffff
		))),
	}
}

fn take_cstr(b: &[u8]) -> Result<(String, &[u8]), WireError> {
	let end = b
		.iter()
		.position(|&c| c == 0)
		.ok_or_else(|| violation("unterminated string"))?;
	let s = std::str::from_utf8(&b[..end])
		.map_err(|_| violation("a string that is not UTF-8"))?
		.to_string();
	Ok((s, &b[end + 1..]))
}

/// One regular message: its tag and its body (without the length).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
	pub tag: u8,
	pub body: Vec<u8>,
}

impl Message {
	pub fn new(tag: u8, body: Vec<u8>) -> Self {
		Message { tag, body }
	}

	pub fn encode(&self) -> Vec<u8> {
		let mut out = Vec::with_capacity(self.body.len() + 5);
		out.push(self.tag);
		out.extend_from_slice(&((self.body.len() + 4) as u32).to_be_bytes());
		out.extend_from_slice(&self.body);
		out
	}
}

pub async fn read_message<S: AsyncRead + Unpin>(
	s: &mut S,
	max_len: usize,
) -> Result<Message, WireError> {
	let tag = s.read_u8().await?;
	let len = s.read_u32().await? as usize;
	if len < 4 || len - 4 > max_len {
		return Err(violation(format!(
			"message '{}' of {len} bytes",
			char::from(tag)
		)));
	}
	let mut body = vec![0u8; len - 4];
	s.read_exact(&mut body).await?;
	Ok(Message { tag, body })
}

pub async fn write_all<S: AsyncWrite + Unpin>(s: &mut S, bytes: &[u8]) -> Result<(), WireError> {
	s.write_all(bytes).await?;
	s.flush().await?;
	Ok(())
}

/// A small builder for message bodies.
#[derive(Default)]
pub struct Body(Vec<u8>);

impl Body {
	pub fn new() -> Self {
		Body(Vec::new())
	}
	pub fn i32(mut self, v: i32) -> Self {
		self.0.extend_from_slice(&v.to_be_bytes());
		self
	}
	pub fn u32(mut self, v: u32) -> Self {
		self.0.extend_from_slice(&v.to_be_bytes());
		self
	}
	pub fn cstr(mut self, s: &str) -> Self {
		self.0.extend_from_slice(s.as_bytes());
		self.0.push(0);
		self
	}
	pub fn bytes(mut self, b: &[u8]) -> Self {
		self.0.extend_from_slice(b);
		self
	}
	pub fn byte(mut self, b: u8) -> Self {
		self.0.push(b);
		self
	}
	pub fn finish(self) -> Vec<u8> {
		self.0
	}
	pub fn message(self, tag: u8) -> Message {
		Message::new(tag, self.0)
	}
}

// ---------------------------------------------------------------------------------------------
// What the router sends as a server.

/// An `ErrorResponse` (or, with severity "WARNING"/"NOTICE", the body of a `NoticeResponse`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorFields {
	pub severity: &'static str,
	pub code: &'static str,
	pub message: String,
	pub detail: Option<String>,
	pub hint: Option<String>,
}

impl ErrorFields {
	pub fn fatal(code: &'static str, message: impl Into<String>) -> Self {
		ErrorFields {
			severity: "FATAL",
			code,
			message: message.into(),
			detail: None,
			hint: None,
		}
	}

	pub fn error(code: &'static str, message: impl Into<String>) -> Self {
		ErrorFields {
			severity: "ERROR",
			..ErrorFields::fatal(code, message)
		}
	}

	pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
		self.hint = Some(hint.into());
		self
	}

	pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
		self.detail = Some(detail.into());
		self
	}

	pub fn message(&self) -> Message {
		let mut b = Body::new()
			.byte(b'S')
			.cstr(self.severity)
			.byte(b'V')
			.cstr(self.severity)
			.byte(b'C')
			.cstr(self.code)
			.byte(b'M')
			.cstr(&self.message);
		if let Some(d) = &self.detail {
			b = b.byte(b'D').cstr(d);
		}
		if let Some(h) = &self.hint {
			b = b.byte(b'H').cstr(h);
		}
		b.byte(0).message(b'E')
	}
}

/// The fields of an `ErrorResponse` or `NoticeResponse` a node sent.
pub fn parse_error_fields(body: &[u8]) -> Vec<(u8, String)> {
	let mut out = Vec::new();
	let mut r = body;
	while let Some((&field, rest)) = r.split_first() {
		if field == 0 {
			break;
		}
		match take_cstr(rest) {
			Ok((value, after)) => {
				out.push((field, value));
				r = after;
			}
			Err(_) => break,
		}
	}
	out
}

pub fn authentication_ok() -> Message {
	Body::new().i32(0).message(b'R')
}

pub fn authentication_sasl(mechanisms: &[&str]) -> Message {
	let mut b = Body::new().i32(10);
	for m in mechanisms {
		b = b.cstr(m);
	}
	b.byte(0).message(b'R')
}

pub fn authentication_sasl_continue(data: &[u8]) -> Message {
	Body::new().i32(11).bytes(data).message(b'R')
}

pub fn authentication_sasl_final(data: &[u8]) -> Message {
	Body::new().i32(12).bytes(data).message(b'R')
}

pub fn backend_key_data(pid: i32, key: &[u8]) -> Message {
	Body::new().i32(pid).bytes(key).message(b'K')
}

pub fn negotiate_protocol_version(newest_minor: u32, unsupported: &[String]) -> Message {
	let mut b = Body::new()
		.u32((3 << 16) | newest_minor)
		.u32(unsupported.len() as u32);
	for o in unsupported {
		b = b.cstr(o);
	}
	b.message(b'v')
}

/// The `SASLInitialResponse` a client sends, read by the router as a server.
pub fn parse_sasl_initial(body: &[u8]) -> Result<(String, Vec<u8>), WireError> {
	let (mechanism, rest) = take_cstr(body)?;
	if rest.len() < 4 {
		return Err(violation("short SASLInitialResponse"));
	}
	let n = i32::from_be_bytes(rest[0..4].try_into().expect("checked length"));
	let data = &rest[4..];
	if n < 0 || n as usize != data.len() {
		return Err(violation("SASLInitialResponse length mismatch"));
	}
	Ok((mechanism, data.to_vec()))
}

// ---------------------------------------------------------------------------------------------
// What the router sends as a client.

pub fn startup_message(version: u32, params: &[(String, String)]) -> Vec<u8> {
	let mut b = Body::new().u32(version);
	for (k, v) in params {
		b = b.cstr(k).cstr(v);
	}
	let body = b.byte(0).finish();
	let mut out = Vec::with_capacity(body.len() + 4);
	out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
	out.extend_from_slice(&body);
	out
}

pub fn ssl_request() -> Vec<u8> {
	let mut out = Vec::with_capacity(8);
	out.extend_from_slice(&8u32.to_be_bytes());
	out.extend_from_slice(&SSL_REQUEST.to_be_bytes());
	out
}

pub fn cancel_request(pid: i32, key: &[u8]) -> Vec<u8> {
	let mut out = Vec::with_capacity(12 + key.len());
	out.extend_from_slice(&((12 + key.len()) as u32).to_be_bytes());
	out.extend_from_slice(&CANCEL_REQUEST.to_be_bytes());
	out.extend_from_slice(&pid.to_be_bytes());
	out.extend_from_slice(key);
	out
}

pub fn sasl_initial_response(mechanism: &str, data: &[u8]) -> Message {
	Body::new()
		.cstr(mechanism)
		.i32(data.len() as i32)
		.bytes(data)
		.message(b'p')
}

pub fn sasl_response(data: &[u8]) -> Message {
	Message::new(b'p', data.to_vec())
}

pub fn password_message(password: &str) -> Message {
	Body::new().cstr(password).message(b'p')
}

pub fn query(sql: &str) -> Message {
	Body::new().cstr(sql).message(b'Q')
}

pub fn terminate() -> Message {
	Message::new(b'X', Vec::new())
}

/// The cstring list in an `AuthenticationSASL` body after its code.
pub fn parse_cstr_list(mut b: &[u8]) -> Vec<String> {
	let mut out = Vec::new();
	while let Ok((s, rest)) = take_cstr(b) {
		if s.is_empty() {
			break;
		}
		out.push(s);
		b = rest;
	}
	out
}

/// A `DataRow` as text values (None for NULL).
pub fn parse_data_row(body: &[u8]) -> Result<Vec<Option<Vec<u8>>>, WireError> {
	if body.len() < 2 {
		return Err(violation("short DataRow"));
	}
	let n = u16::from_be_bytes([body[0], body[1]]) as usize;
	let mut r = &body[2..];
	let mut out = Vec::with_capacity(n);
	for _ in 0..n {
		if r.len() < 4 {
			return Err(violation("short DataRow"));
		}
		let len = i32::from_be_bytes(r[0..4].try_into().expect("checked length"));
		r = &r[4..];
		if len < 0 {
			out.push(None);
		} else {
			let len = len as usize;
			if r.len() < len {
				return Err(violation("short DataRow"));
			}
			out.push(Some(r[..len].to_vec()));
			r = &r[len..];
		}
	}
	Ok(out)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn startup_round_trip() {
		let params = vec![
			("user".to_string(), "alice".to_string()),
			("database".to_string(), "app".to_string()),
		];
		let bytes = startup_message(PROTOCOL_3_2, &params);
		let parsed = parse_startup(&bytes[4..]).unwrap();
		assert_eq!(
			parsed,
			Startup::Startup {
				version: PROTOCOL_3_2,
				params
			}
		);
	}

	#[test]
	fn special_requests() {
		assert_eq!(
			parse_startup(&ssl_request()[4..]).unwrap(),
			Startup::SslRequest
		);
		let key = [7u8; 32];
		assert_eq!(
			parse_startup(&cancel_request(42, &key)[4..]).unwrap(),
			Startup::Cancel {
				pid: 42,
				key: key.to_vec()
			}
		);
		assert!(parse_startup(&cancel_request(42, &[1, 2])[4..]).is_err());
		assert!(parse_startup(&[0, 2, 0, 0]).is_err());
		assert!(parse_startup(&[0, 3]).is_err());
	}

	#[test]
	fn error_fields_round_trip() {
		let m = ErrorFields::fatal("28P01", "nope")
			.with_hint("try again")
			.message();
		let f = parse_error_fields(&m.body);
		assert!(f.contains(&(b'C', "28P01".to_string())));
		assert!(f.contains(&(b'H', "try again".to_string())));
	}
}
