//! The listeners and what every session shares: the routing listener (`LEPIS_PORT`) and, when
//! `LEPIS_PASSTHROUGH_PORT` is set, the home pass-through (`passthrough.rs`).

use std::future::Future;
use std::sync::{Arc, RwLock};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio_rustls::TlsAcceptor;

use crate::auth::VerifierSource;
use crate::cancel;
use crate::catalog::Catalog;
use crate::config::Config;
use crate::live;
use crate::session;
use crate::tls;

pub struct App {
	pub config: Arc<Config>,
	pub tls_acceptor: Option<TlsAcceptor>,
	/// The `tls-server-end-point` binding data of the certificate `tls_acceptor` serves, when
	/// its signature names a hash; SCRAM-SHA-256-PLUS is offered only when this is Some.
	pub channel_binding: Option<Vec<u8>>,
	pub node_tls: Option<Arc<rustls::ClientConfig>>,
	pub verifiers: VerifierSource,
	pub cancels: cancel::Registry,
	pub clients: Arc<Semaphore>,
	/// The current catalog snapshot; None when the cluster has no `lepis` schema (live.rs).
	catalog: RwLock<Option<Arc<Catalog>>>,
	/// When the home node last answered, and whether the catalog may still be routed by.
	pub freshness: live::Freshness,
}

impl App {
	pub fn new(config: Config) -> Result<Arc<App>, String> {
		let config = Arc::new(config);
		let (tls_acceptor, channel_binding) = match (&config.tls_cert, &config.tls_key) {
			(Some(c), Some(k)) => {
				let (server, binding) = tls::server_config(c, k)?;
				(Some(TlsAcceptor::from(server)), binding)
			}
			_ => (None, None),
		};
		let node_tls = tls::client_config(&config.home())?;
		let freshness = live::Freshness::new(config.catalog_max_age, std::time::Instant::now());
		Ok(Arc::new(App {
			verifiers: VerifierSource::new(config.clone(), node_tls.clone()),
			clients: Arc::new(Semaphore::new(config.max_clients)),
			config,
			tls_acceptor,
			channel_binding,
			node_tls,
			cancels: cancel::Registry::default(),
			catalog: RwLock::new(None),
			freshness,
		}))
	}

	/// The catalog a new statement is routed with. A session takes one snapshot per batch, so a
	/// reload never changes the rules in the middle of one.
	pub fn catalog(&self) -> Option<Arc<Catalog>> {
		self.catalog.read().expect("catalog lock").clone()
	}

	pub fn set_catalog(&self, c: Option<Catalog>) {
		*self.catalog.write().expect("catalog lock") = c.map(Arc::new);
	}
}

/// Accepts clients until the listener fails. Each client is its own task; a client over
/// `max_clients` is closed at once rather than queued.
pub async fn serve(app: Arc<App>, listener: TcpListener) -> std::io::Result<()> {
	serve_with(app, listener, None).await
}

/// `serve`, and the home pass-through on `passthrough` when there is one (`passthrough.rs`). It
/// opens once the home is chosen, as the routing listener does, follows the same home, and counts
/// against the same `max_clients`. Either listener failing stops both.
pub async fn serve_with(
	app: Arc<App>,
	listener: TcpListener,
	passthrough: Option<TcpListener>,
) -> std::io::Result<()> {
	// A home that is a set of copies starts on the one that takes writes, and follows it.
	crate::follow::settle(&app, std::time::Duration::from_secs(30)).await;
	tokio::spawn(crate::follow::run(app.clone()));
	// Not after the catalog: the pass-through never reads it, so a home whose catalog cannot be
	// read is no reason to keep auth and storage waiting.
	let passthrough = async {
		let Some(passthrough) = passthrough else {
			return std::future::pending().await;
		};
		tracing::info!(
			address = %passthrough.local_addr()?,
			home = %app.config.home(),
			"home pass-through listening"
		);
		accept(&app, passthrough, crate::passthrough::run).await
	};
	let routing = async {
		// A copy's router: no catalog, no recovery, no job runner, every session relayed.
		if !app.config.catalog {
			tracing::info!("LEPIS_CATALOG=off: every session is relayed to the home as it is");
			return accept(&app, listener, session::run).await;
		}
		live::load(&app)
			.await
			.map_err(|e| std::io::Error::other(format!("the catalog: {e}")))?;
		tokio::spawn(live::watch(app.clone()));
		// Phase 3: resolves prepared transactions a crashed coordinator left in doubt.
		tokio::spawn(crate::twopc::recovery(app.clone()));
		// Phase 4: the job runner, and the admin API when LEPIS_ADMIN_ADDR is set.
		crate::admin::spawn(&app);
		accept(&app, listener, session::run).await
	};
	tokio::select! {
		r = routing => r,
		r = passthrough => r,
	}
}

/// One listener's accept loop: `session` runs each client in its own task, which holds one of the
/// `max_clients` permits while it lasts.
async fn accept<F, Fut>(app: &Arc<App>, listener: TcpListener, session: F) -> std::io::Result<()>
where
	F: Fn(Arc<App>, TcpStream) -> Fut,
	Fut: Future<Output = ()> + Send + 'static,
{
	loop {
		let (tcp, peer) = listener.accept().await?;
		let Ok(permit) = app.clients.clone().try_acquire_owned() else {
			tracing::warn!(%peer, "too many clients, connection closed");
			drop(tcp);
			continue;
		};
		let run = session(app.clone(), tcp);
		tokio::spawn(async move {
			run.await;
			drop(permit);
		});
	}
}
