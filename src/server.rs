//! The listener and what every session shares.

use std::sync::{Arc, RwLock};

use tokio::net::TcpListener;
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
		let node_tls = tls::client_config(&config.home)?;
		Ok(Arc::new(App {
			verifiers: VerifierSource::new(config.clone(), node_tls.clone()),
			clients: Arc::new(Semaphore::new(config.max_clients)),
			config,
			tls_acceptor,
			channel_binding,
			node_tls,
			cancels: cancel::Registry::default(),
			catalog: RwLock::new(None),
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
	live::load(&app)
		.await
		.map_err(|e| std::io::Error::other(format!("the catalog: {e}")))?;
	tokio::spawn(live::watch(app.clone()));
	// Phase 3: resolves prepared transactions a crashed coordinator left in doubt.
	tokio::spawn(crate::twopc::recovery(app.clone()));
	// Phase 4: the job runner, and the admin API when LEPIS_ADMIN_ADDR is set.
	crate::admin::spawn(&app);
	loop {
		let (tcp, peer) = listener.accept().await?;
		let Ok(permit) = app.clients.clone().try_acquire_owned() else {
			tracing::warn!(%peer, "too many clients, connection closed");
			drop(tcp);
			continue;
		};
		let app = app.clone();
		tokio::spawn(async move {
			session::run(app, tcp).await;
			drop(permit);
		});
	}
}
