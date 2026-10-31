//! The home pass-through (`LEPIS_PASSTHROUGH_PORT`):
//! a second listener on which a session is relayed to the CURRENT home as it is. No catalog, no
//! routing, no DDL fan-out, no SCRAM of Lepis's own: the startup packet goes to the home as the
//! client sent it, the home authenticates the client, and from there it is bytes both ways. It is
//! for auth and storage, whose migrations create functions, triggers and roles that must not fan
//! out to the data nodes.
//!
//! What it shares with the routing listener: the preamble (`session::preamble`, so TLS by
//! SSLRequest or direct, GSSAPI declined and a cleartext client served exactly as there, with the
//! same `LEPIS_TLS_CERT`), the hop (`backend::open`, encrypted as `LEPIS_HOME_SSLMODE` says, so a
//! TLS client over a TLS hop is decrypted and encrypted again), the relay (`session::relay`, which
//! also ends a session before a commit its synchronous standby did not confirm) and the home, as
//! `follow.rs` keeps it: a session is opened on whichever copy takes writes when it starts, and is
//! left there; after a failover the old primary's guard fence ends it and the client reconnects.
//!
//! Two things follow from not taking part in the login:
//! - The client is given the home's own process id and cancel key (the home's BackendKeyData passes
//!   untouched), so a cancel request on this port is forwarded as it is to the current home. The
//!   home moves only once another copy takes writes, by which time the old one's sessions have
//!   been fenced, so the current home is where a live session's cancel belongs; a stale one is
//!   ignored there, as Postgres ignores any key that does not match.
//! - SCRAM channel binding cannot survive the splice: a client on TLS binds to Lepis's certificate,
//!   and the home checks the binding against its own. A client that reaches this port over TLS
//!   while the hop is TLS too must say `channel_binding=disable`; with either leg in the clear
//!   there is no binding to break.

use std::sync::Arc;

use tokio::net::TcpStream;

use crate::backend::{self, BoxStream};
use crate::server::App;
use crate::session::{self, AUTHENTICATION_TIMEOUT, Preamble};
use crate::wire::{self, ErrorFields, WireError};

/// One client on the pass-through, from the first byte to the last.
pub async fn run(app: Arc<App>, tcp: TcpStream) {
	let peer = tcp.peer_addr().ok();
	// As on the routing listener, a client has AUTHENTICATION_TIMEOUT to finish its preamble;
	// from the startup packet on, the home's own authentication_timeout applies.
	let opened = match tokio::time::timeout(AUTHENTICATION_TIMEOUT, open(&app, tcp)).await {
		Ok(Ok(opened)) => opened,
		Ok(Err(e)) => {
			quiet_or_warn(&peer, &e);
			return;
		}
		Err(_) => {
			tracing::debug!(?peer, "pass-through: no startup packet in time");
			return;
		}
	};
	let Some((client, node)) = opened else {
		return;
	};
	if let Err(e) = session::relay(client, node).await {
		quiet_or_warn(&peer, &e);
	}
}

/// A client that simply hung up is not worth a line; anything else on the relay is.
fn quiet_or_warn(peer: &Option<std::net::SocketAddr>, e: &WireError) {
	match e {
		WireError::Io(io) if io.kind() == std::io::ErrorKind::UnexpectedEof => {}
		_ => tracing::warn!(?peer, "pass-through relay: {e}"),
	}
}

/// The preamble, then the startup packet sent on to the home. The client's stream and the home's,
/// ready to relay; None when the connection ended there (a cancel, a refusal).
async fn open(app: &App, tcp: TcpStream) -> Result<Option<(BoxStream, BoxStream)>, WireError> {
	let (mut client, version, params) = match session::preamble(app, tcp).await? {
		Preamble::Closed => return Ok(None),
		Preamble::Cancel { pid, key } => {
			cancel(app, pid, &key).await;
			return Ok(None);
		}
		Preamble::Startup {
			client,
			version,
			params,
			..
		} => (client, version, params),
	};
	let node = app.config.home();
	let mut stream = match backend::open(&node, app.node_tls.as_ref()).await {
		Ok(s) => s,
		Err(e) => {
			tracing::warn!(%node, "pass-through: could not reach the home: {e}");
			session::refuse(
				&mut client,
				ErrorFields::fatal("08006", format!("could not open a session on {node}"))
					.with_detail(e.to_string()),
			)
			.await?;
			return Ok(None);
		}
	};
	// The packet as the client sent it: `wire::parse_startup` takes nothing after the last
	// parameter, so encoding what it read gives back the same bytes.
	wire::write_all(&mut stream, &wire::startup_message(version, &params)).await?;
	Ok(Some((client, stream)))
}

/// Forwards a cancel request to the current home as it is. Like Postgres, nothing is answered.
async fn cancel(app: &App, pid: i32, key: &[u8]) {
	let node = app.config.home();
	match backend::open(&node, app.node_tls.as_ref()).await {
		Ok(mut s) => {
			let _ = wire::write_all(&mut s, &wire::cancel_request(pid, key)).await;
		}
		Err(e) => tracing::warn!(%node, "pass-through: cancel not delivered: {e}"),
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;
	use std::time::Duration;

	use tokio::io::{AsyncReadExt, AsyncWriteExt};
	use tokio::net::TcpListener;

	use super::*;
	use crate::config::Config;

	/// A home that records the first packet it is sent and answers with a few bytes of its own.
	async fn fake_home() -> (String, tokio::task::JoinHandle<Vec<u8>>) {
		let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let address = listener.local_addr().unwrap().to_string();
		let task = tokio::spawn(async move {
			let (mut tcp, _) = listener.accept().await.unwrap();
			let len = tcp.read_u32().await.unwrap();
			let mut packet = len.to_be_bytes().to_vec();
			packet.resize(len as usize, 0);
			tcp.read_exact(&mut packet[4..]).await.unwrap();
			tcp.write_all(&wire::Message::new(b'R', vec![0, 0, 0, 0]).encode())
				.await
				.unwrap();
			let mut more = [0u8; 3];
			tcp.read_exact(&mut more).await.unwrap();
			packet.extend_from_slice(&more);
			packet
		});
		(address, task)
	}

	fn app(home: &str) -> Arc<App> {
		let config: HashMap<String, String> = [
			("LEPIS_HOME", home),
			("LEPIS_HOME_SSLMODE", "disable"),
			("LEPIS_SERVICE_USER", "lepis"),
			("LEPIS_SERVICE_PASSWORD", "x"),
		]
		.into_iter()
		.map(|(k, v)| (k.to_string(), v.to_string()))
		.collect();
		App::new(Config::from_map(&config).expect("config")).expect("app")
	}

	#[tokio::test]
	async fn the_startup_packet_and_everything_after_reach_the_home_as_sent() {
		let (home, recorded) = fake_home().await;
		let app = app(&home);
		let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let address = listener.local_addr().unwrap();
		tokio::spawn(async move {
			let (tcp, _) = listener.accept().await.unwrap();
			run(app, tcp).await;
		});
		let params = vec![
			("user".to_string(), "auth_admin".to_string()),
			("database".to_string(), "postgres".to_string()),
			("_pq_.unknown".to_string(), "1".to_string()),
		];
		let startup = wire::startup_message(wire::PROTOCOL_3_2, &params);
		let mut c = TcpStream::connect(address).await.unwrap();
		// GSSAPI is declined first, as on the routing listener, and never reaches the home.
		let mut gss = 8u32.to_be_bytes().to_vec();
		gss.extend_from_slice(&80_877_104u32.to_be_bytes());
		c.write_all(&gss).await.unwrap();
		assert_eq!(c.read_u8().await.unwrap(), b'N');
		c.write_all(&startup).await.unwrap();
		let mut answer = [0u8; 9];
		tokio::time::timeout(Duration::from_secs(5), c.read_exact(&mut answer))
			.await
			.expect("the home's answer is relayed")
			.unwrap();
		assert_eq!(answer[0], b'R');
		c.write_all(b"abc").await.unwrap();
		let mut want = startup.clone();
		want.extend_from_slice(b"abc");
		assert_eq!(recorded.await.unwrap(), want);
	}

	#[tokio::test]
	async fn a_cancel_goes_to_the_home_as_it_is() {
		let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let home = listener.local_addr().unwrap().to_string();
		let got = tokio::spawn(async move {
			let (mut tcp, _) = listener.accept().await.unwrap();
			let mut all = Vec::new();
			tcp.read_to_end(&mut all).await.unwrap();
			all
		});
		let app = app(&home);
		let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let address = listener.local_addr().unwrap();
		tokio::spawn(async move {
			let (tcp, _) = listener.accept().await.unwrap();
			run(app, tcp).await;
		});
		let request = wire::cancel_request(4242, &[9; 32]);
		let mut c = TcpStream::connect(address).await.unwrap();
		c.write_all(&request).await.unwrap();
		let forwarded = tokio::time::timeout(Duration::from_secs(5), got)
			.await
			.expect("the cancel reached the home")
			.unwrap();
		assert_eq!(forwarded, request);
	}

	#[tokio::test]
	async fn an_unreachable_home_is_said_in_postgres_words() {
		let app = app("127.0.0.1:1");
		let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let address = listener.local_addr().unwrap();
		tokio::spawn(async move {
			let (tcp, _) = listener.accept().await.unwrap();
			run(app, tcp).await;
		});
		let mut c = TcpStream::connect(address).await.unwrap();
		c.write_all(&wire::startup_message(
			wire::PROTOCOL_3_0,
			&[("user".to_string(), "u".to_string())],
		))
		.await
		.unwrap();
		let mut all = Vec::new();
		tokio::time::timeout(Duration::from_secs(15), c.read_to_end(&mut all))
			.await
			.expect("answered and closed")
			.unwrap();
		assert_eq!(all[0], b'E');
		let fields = wire::parse_error_fields(&all[5..]);
		assert!(fields.contains(&(b'C', "08006".to_string())), "{fields:?}");
	}
}
