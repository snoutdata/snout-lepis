//! snout-lepis: what a client or a node sends before the stream is relayed (lepis/src/wire.rs:
//! the startup packet, the setup messages, the bodies the router reads), and the framing every
//! message goes through after that (lepis/src/frame.rs). Any bytes are read or refused, never a
//! panic, and what is read encodes back to exactly the bytes it came from.
#![no_main]
use std::sync::OnceLock;

use lepis::frame::FrameReader;
use lepis::wire;
use libfuzzer_sys::fuzz_target;

fn runtime() -> &'static tokio::runtime::Runtime {
	static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
	RT.get_or_init(|| {
		tokio::runtime::Builder::new_current_thread()
			.build()
			.expect("a runtime")
	})
}

fuzz_target!(|data: &[u8]| {
	// A startup packet's body (what read_startup hands on), and a whole packet off a stream.
	if let Ok(wire::Startup::Startup { version, params }) = wire::parse_startup(data) {
		assert_eq!(&wire::startup_message(version, &params)[4..], data);
	}
	runtime().block_on(async {
		let _ = wire::read_startup(&mut &data[..]).await;
		// The setup phase: one message after another, as authentication reads them.
		let mut s = data;
		while wire::read_message(&mut s, wire::MAX_SETUP_MESSAGE_LEN)
			.await
			.is_ok()
		{}
		// After setup: the framing the router relays with, which must never tear a message.
		let mut frames = FrameReader::new(data);
		let mut at = 0;
		while let Ok(m) = frames.next().await {
			let encoded = m.encode();
			assert_eq!(&data[at..at + encoded.len()], &encoded[..]);
			at += encoded.len();
		}
	});
	let _ = wire::parse_error_fields(data);
	let _ = wire::parse_cstr_list(data);
	let _ = wire::parse_data_row(data);
	if let Ok((mechanism, payload)) = wire::parse_sasl_initial(data) {
		assert_eq!(
			wire::sasl_initial_response(&mechanism, &payload).body,
			data
		);
	}
});
