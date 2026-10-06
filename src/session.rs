//! One client connection, from the first byte to the last.
//!
//! Phase 0 is a pass-through: the client is authenticated by Lepis, a session is opened on the
//! home node as the same role with the same startup parameters, and from the first
//! `ReadyForQuery` on, bytes are relayed both ways. Phase 1 replaces the relay with routing.
//! A client has `AUTHENTICATION_TIMEOUT` to get that far, as Postgres gives it
//! `authentication_timeout`; one that stalls is let go, so it cannot hold a slot.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;

use crate::backend::{self, BackendError, BoxStream};
use crate::cancel;
use crate::router::Router;
use crate::scram::{self, Binding, ClientCredential, ClientSecret, ServerExchange};
use crate::server::App;
use crate::tls::ALPN_POSTGRESQL;
use crate::wire::{self, ErrorFields, MAX_SETUP_MESSAGE_LEN, Message, Startup, WireError};

/// The newest protocol minor version Lepis speaks to clients.
const NEWEST_MINOR: u32 = 2;

/// How long a client has to log in, as Postgres's `authentication_timeout` defaults to.
pub const AUTHENTICATION_TIMEOUT: Duration = Duration::from_secs(60);

pub async fn run(app: Arc<App>, tcp: TcpStream) {
	run_with_timeout(app, tcp, AUTHENTICATION_TIMEOUT).await
}

/// `run`, with the time a client has to log in.
pub async fn run_with_timeout(app: Arc<App>, tcp: TcpStream, limit: Duration) {
	let peer = tcp.peer_addr().ok();
	if let Err(e) = serve(app, tcp, limit).await {
		match e {
			WireError::Io(ref io) if io.kind() == std::io::ErrorKind::UnexpectedEof => {}
			_ => tracing::debug!(?peer, "session ended: {e}"),
		}
	}
}

/// A client that has logged in, with its session on the home node.
struct Opened {
	client: BoxStream,
	params: Vec<(String, String)>,
	user: String,
	client_key: ClientSecret,
	backend: backend::Backend,
	minor: u32,
}

/// Everything before the session is relayed: TLS, the startup packet, SCRAM and the home node's
/// session. None when the connection ended there (a cancel request, a refusal).
async fn open(app: &Arc<App>, tcp: TcpStream) -> Result<Option<Opened>, WireError> {
	tcp.set_nodelay(true).ok();

	// A Postgres 17+ client may skip SSLRequest and start TLS at once (sslnegotiation=direct);
	// its first byte is then a TLS handshake record.
	let mut first = [0u8; 1];
	let direct_tls = tcp.peek(&mut first).await? == 1 && first[0] == 0x16;
	let mut client: BoxStream;
	let mut encrypted = false;
	if direct_tls {
		let Some(acceptor) = &app.tls_acceptor else {
			return Ok(None);
		};
		let tls = acceptor.accept(tcp).await?;
		if tls.get_ref().1.alpn_protocol() != Some(ALPN_POSTGRESQL) {
			return Ok(None);
		}
		client = Box::new(tls);
		encrypted = true;
	} else {
		client = Box::new(tcp);
	}

	let (version, params) = loop {
		match wire::read_startup(&mut client).await? {
			Startup::SslRequest if !encrypted => match &app.tls_acceptor {
				Some(acceptor) => {
					client.write_all(b"S").await?;
					client = Box::new(accept(acceptor, client).await?);
					encrypted = true;
				}
				None => client.write_all(b"N").await?,
			},
			Startup::GssEncRequest if !encrypted => client.write_all(b"N").await?,
			Startup::SslRequest | Startup::GssEncRequest => {
				return refuse(
					&mut client,
					ErrorFields::fatal("08P01", "encryption requested twice"),
				)
				.await
				.map(|_| None);
			}
			Startup::Cancel { pid, key } => {
				app.cancels.cancel(pid, &key).await;
				crate::router::spread::cancel(pid, &key).await;
				return Ok(None);
			}
			Startup::Startup { version, params } => break (version, params),
		}
	};

	// Protocol version and `_pq_.` options: Lepis knows no options, and speaks up to 3.2.
	let requested_minor = version & 0xffff;
	let unknown_options: Vec<String> = params
		.iter()
		.filter(|(k, _)| k.starts_with("_pq_."))
		.map(|(k, _)| k.clone())
		.collect();
	let minor = requested_minor.min(NEWEST_MINOR);
	if requested_minor > NEWEST_MINOR || !unknown_options.is_empty() {
		wire::write_all(
			&mut client,
			&wire::negotiate_protocol_version(minor, &unknown_options).encode(),
		)
		.await?;
	}
	let params: Vec<(String, String)> = params
		.into_iter()
		.filter(|(k, _)| !k.starts_with("_pq_."))
		.collect();

	let Some(user) = Startup::param(&params, "user").map(str::to_string) else {
		return refuse(
			&mut client,
			ErrorFields::fatal(
				"28000",
				"no PostgreSQL user name specified in startup packet",
			),
		)
		.await
		.map(|_| None);
	};
	if let Some(r) = Startup::param(&params, "replication")
		&& !matches!(r, "false" | "off" | "no" | "0")
	{
		return refuse(
			&mut client,
			ErrorFields::fatal("08P01", "Lepis does not route replication connections")
				.with_hint("Connect to a node directly for replication."),
		)
		.await
		.map(|_| None);
	}

	// SCRAM with the client.
	// Channel binding ties the proof to THIS TLS session, so it exists only on an encrypted one.
	let binding = if encrypted {
		app.channel_binding.as_deref()
	} else {
		None
	};
	let client_key = match authenticate(app, &mut client, &user, binding).await? {
		Ok(secret) => secret,
		Err(fields) => return refuse(&mut client, fields).await.map(|_| None),
	};

	// The same session on the home node, as the same role.
	let node = &app.config.home;
	let backend = match backend::connect(
		node,
		app.node_tls.as_ref(),
		&params,
		ClientCredential::Key(client_key.clone()),
	)
	.await
	{
		Ok(b) => b,
		Err(BackendError::Refused(m)) => {
			wire::write_all(&mut client, &m.encode()).await?;
			return Ok(None);
		}
		Err(e) => {
			tracing::warn!(%node, %user, "node session: {e}");
			return refuse(
				&mut client,
				ErrorFields::fatal("08006", format!("could not open a session on {node}"))
					.with_detail(e.to_string()),
			)
			.await
			.map(|_| None);
		}
	};

	Ok(Some(Opened {
		client,
		params,
		user,
		client_key,
		backend,
		minor,
	}))
}

async fn serve(app: Arc<App>, tcp: TcpStream, limit: Duration) -> Result<(), WireError> {
	// Postgres's authentication_timeout: a client that never finishes logging in gives its slot
	// back, and the connection is closed without a word, as Postgres closes it.
	let opened = match tokio::time::timeout(limit, open(&app, tcp)).await {
		Ok(opened) => opened?,
		Err(_) => {
			tracing::debug!("authentication timed out");
			return Ok(());
		}
	};
	let Some(Opened {
		mut client,
		params,
		user,
		client_key,
		mut backend,
		minor,
	}) = opened
	else {
		return Ok(());
	};
	let node = &app.config.home;
	let key_len = if minor >= 2 { 32 } else { 4 };
	let (pid, key) = app.cancels.register(
		key_len,
		cancel::Target {
			node: node.clone(),
			tls: app.node_tls.clone(),
			pid: backend.pid,
			key: backend.key.clone(),
		},
	);

	let mut opening = wire::authentication_ok().encode();
	for m in &backend.opening {
		opening.extend_from_slice(&m.encode());
	}
	opening.extend_from_slice(&wire::backend_key_data(pid, &key).encode());
	opening.extend_from_slice(&backend.ready.encode());
	// The cluster's database with a catalog that distributes something goes through the
	// router (Phase 1); anything else is relayed as it is (Phase 0).
	let database = Startup::param(&params, "database")
		.unwrap_or(&user)
		.to_string();
	let routed = app.catalog().filter(|c| {
		!c.relations.is_empty() && c.home().is_some() && database == app.config.service.database
	});
	let relayed = async {
		wire::write_all(&mut client, &opening).await?;
		match routed {
			Some(catalog) => {
				let home = catalog.home().map(|n| n.id).expect("checked");
				Router::new(
					app.clone(),
					catalog,
					client_key,
					params,
					client,
					backend,
					home,
					pid,
					key.clone(),
				)
				.run()
				.await
			}
			None => {
				tokio::io::copy_bidirectional(&mut client, &mut backend.stream).await?;
				Ok::<(), WireError>(())
			}
		}
	}
	.await;
	app.cancels.forget(pid);
	crate::router::spread::clear(pid);
	relayed
}

async fn accept(
	acceptor: &TlsAcceptor,
	stream: BoxStream,
) -> Result<tokio_rustls::server::TlsStream<BoxStream>, WireError> {
	Ok(acceptor.accept(stream).await?)
}

async fn refuse(client: &mut BoxStream, fields: ErrorFields) -> Result<(), WireError> {
	wire::write_all(client, &fields.message().encode()).await
}

/// The SCRAM exchange with the client. The outer Result is the connection; the inner one is
/// the verdict, as the error to send. `binding` is this connection's `tls-server-end-point`
/// data when SCRAM-SHA-256-PLUS can be offered.
async fn authenticate(
	app: &App,
	client: &mut BoxStream,
	user: &str,
	binding: Option<&[u8]>,
) -> Result<Result<ClientSecret, ErrorFields>, WireError> {
	let failed = || {
		ErrorFields::fatal(
			"28P01",
			format!("password authentication failed for user \"{user}\""),
		)
	};
	let (verifier, real) = match app.verifiers.verifier(user).await {
		Ok(v) => v,
		Err(e) => {
			tracing::error!("{e}");
			return Ok(Err(ErrorFields::fatal(
				"08006",
				"Lepis cannot read roles from the home node",
			)));
		}
	};
	// PLUS first, as Postgres lists them: libpq takes the first it can use.
	let mechanisms: &[&str] = match binding {
		Some(_) => &[scram::MECHANISM_PLUS, scram::MECHANISM],
		None => &[scram::MECHANISM],
	};
	wire::write_all(client, &wire::authentication_sasl(mechanisms).encode()).await?;

	let m = read_password_message(client).await?;
	let (mechanism, data) = wire::parse_sasl_initial(&m.body)?;
	let chosen = match (mechanism.as_str(), binding) {
		(scram::MECHANISM_PLUS, Some(b)) => Binding::Chosen(b),
		(scram::MECHANISM, Some(_)) => Binding::Declined,
		(scram::MECHANISM, None) => Binding::NotOffered,
		_ => {
			return Ok(Err(ErrorFields::fatal(
				"28000",
				format!("SASL mechanism {mechanism} is not offered"),
			)));
		}
	};
	let (exchange, server_first) = match ServerExchange::start(verifier.clone(), &data, chosen) {
		Ok(x) => x,
		Err(e) => return Ok(Err(ErrorFields::fatal("28000", e.0))),
	};
	wire::write_all(
		client,
		&wire::authentication_sasl_continue(server_first.as_bytes()).encode(),
	)
	.await?;

	let m = read_password_message(client).await?;
	match exchange.finish(&m.body) {
		Ok((server_final, client_key)) if real => {
			wire::write_all(
				client,
				&wire::authentication_sasl_final(server_final.as_bytes()).encode(),
			)
			.await?;
			Ok(Ok(ClientSecret {
				client_key,
				verifier,
			}))
		}
		// Said as it is: the binding is checked before the proof and depends only on the
		// certificate, so naming it reveals nothing about the role, and a client being relayed
		// should be told so rather than that its password is wrong. Postgres's code too.
		Err(e) if e.0 == scram::BINDING_CHECK_FAILED => {
			tracing::warn!(%user, "SCRAM channel binding check failed");
			Ok(Err(ErrorFields::fatal("08P01", e.0)))
		}
		_ => {
			app.verifiers.forget(user);
			tracing::info!(%user, "authentication failed");
			Ok(Err(failed()))
		}
	}
}

async fn read_password_message(client: &mut BoxStream) -> Result<Message, WireError> {
	let m = wire::read_message(client, MAX_SETUP_MESSAGE_LEN).await?;
	if m.tag != b'p' {
		return Err(WireError::Protocol(format!(
			"expected a password message, got '{}'",
			char::from(m.tag)
		)));
	}
	Ok(m)
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use tokio::io::{AsyncReadExt, AsyncWriteExt};
	use tokio::net::{TcpListener, TcpStream};

	use super::*;
	use crate::config::Config;

	#[tokio::test]
	async fn a_client_that_never_logs_in_is_let_go() {
		let config: HashMap<String, String> = [
			("LEPIS_HOME", "127.0.0.1:1"),
			("LEPIS_HOME_SSLMODE", "disable"),
			("LEPIS_SERVICE_USER", "lepis"),
			("LEPIS_SERVICE_PASSWORD", "x"),
		]
		.into_iter()
		.map(|(k, v)| (k.to_string(), v.to_string()))
		.collect();
		let app = App::new(Config::from_map(&config).expect("config")).expect("app");
		let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let address = listener.local_addr().unwrap();
		let server = tokio::spawn(async move {
			let (tcp, _) = listener.accept().await.unwrap();
			run_with_timeout(app, tcp, Duration::from_millis(200)).await;
		});
		let mut c = TcpStream::connect(address).await.unwrap();
		// The first bytes of a startup packet, then nothing.
		c.write_all(&[0, 0, 0, 40]).await.unwrap();
		let mut buf = [0u8; 16];
		let n = tokio::time::timeout(Duration::from_secs(5), c.read(&mut buf))
			.await
			.expect("the connection is closed when the time is up")
			.unwrap_or(0);
		assert_eq!(n, 0, "nothing is said, the connection is closed");
		server.await.unwrap();
	}
}
