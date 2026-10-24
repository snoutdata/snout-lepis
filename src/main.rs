//! The snout-lepis binary. Wiring only; everything it runs is in the library.
//!
//! `snout-lepis` alone is the router. `snout-lepis witness` and `snout-lepis guard` are L21's
//! failover: the third vote (`guard/serve.rs`, `LEPIS_WITNESS_*`) and the process beside each copy
//! of a Postgres (`guard/run.rs`, `LEPIS_GUARD_*`).

use lepis::config::Config;
use lepis::guard::run::{GuardConfig, run_guard};
use lepis::guard::serve::{WitnessConfig, serve_witness};
use lepis::server::{self, App};

async fn run() -> Result<(), String> {
	let args: Vec<String> = std::env::args().skip(1).collect();
	match args
		.iter()
		.map(String::as_str)
		.collect::<Vec<_>>()
		.as_slice()
	{
		[] => router().await,
		["witness"] => {
			let config = WitnessConfig::from_env()?;
			tokio::select! {
				result = serve_witness(config) => result,
				_ = stopped() => Ok(()),
			}
		}
		["guard"] => {
			let config = GuardConfig::from_env()?;
			tokio::select! {
				result = run_guard(config) => result,
				_ = stopped() => Ok(()),
			}
		}
		_ => Err(format!(
			"usage: snout-lepis [witness | guard] (got: {})",
			args.join(" ")
		)),
	}
}

async fn router() -> Result<(), String> {
	let config = Config::from_env().map_err(|e| e.to_string())?;
	let address = format!("{}:{}", config.host, config.port);
	let app = App::new(config)?;
	let listener = tokio::net::TcpListener::bind(&address)
		.await
		.map_err(|e| format!("{address}: {e}"))?;
	tracing::info!(%address, home = %app.config.home, "snout-lepis listening");
	tokio::select! {
		result = server::serve(app, listener) => result.map_err(|e| e.to_string()),
		_ = stopped() => Ok(()),
	}
}

/// Ctrl-C, or SIGTERM: as PID 1 in a container the kernel gives SIGTERM no default action.
async fn stopped() {
	#[cfg(unix)]
	{
		let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
			.expect("a SIGTERM handler");
		tokio::select! {
			_ = term.recv() => {}
			_ = tokio::signal::ctrl_c() => {}
		}
	}
	#[cfg(not(unix))]
	{
		let _ = tokio::signal::ctrl_c().await;
	}
}

#[tokio::main]
async fn main() {
	tracing_subscriber::fmt()
		.with_env_filter(
			tracing_subscriber::EnvFilter::try_from_env("LEPIS_LOG")
				.unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
		)
		// Colour only on a terminal; container logs get plain text.
		.with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
		.init();
	if let Err(e) = run().await {
		tracing::error!("{e}");
		std::process::exit(1);
	}
}
