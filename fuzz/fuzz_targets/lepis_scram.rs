//! snout-lepis: SCRAM-SHA-256 on both sides of the router (lepis/src/scram.rs). A role's stored
//! verifier as the home node returns it, a client's first and final messages to Lepis, and a
//! node's first and final messages to Lepis. Each is read or refused without a panic; a verifier
//! that parses encodes back to itself; and no bytes the fuzzer makes ever log in, since a proof
//! needs a password it does not know.
//!
//! The input is up to four messages separated by NUL bytes. Each exchange's nonce is random, so
//! `@@` in the second and third message is replaced by the nonce of that exchange, which lets the
//! fuzzer reach the proof and signature checks instead of stopping at "the nonce does not match".
#![no_main]
use std::sync::OnceLock;

use lepis::scram::{Binding, ClientCredential, ClientExchange, ClientSecret, ServerExchange, Verifier};
use libfuzzer_sys::fuzz_target;

// A low iteration count keeps an input fast; the parsers do not care how it was made.
const SALT: &[u8] = b"lepis-fuzz-salt!";
const ITERATIONS: u32 = 16;

fn verifier() -> &'static Verifier {
	static V: OnceLock<Verifier> = OnceLock::new();
	V.get_or_init(|| Verifier::from_password("pencil", SALT, ITERATIONS))
}

fn secret() -> &'static ClientSecret {
	static S: OnceLock<ClientSecret> = OnceLock::new();
	S.get_or_init(|| ClientSecret::from_password("pencil", SALT, ITERATIONS))
}

fn with_nonce(message: &[u8], nonce: &str) -> Vec<u8> {
	let mut out = Vec::with_capacity(message.len() + nonce.len());
	let mut rest = message;
	while let Some(i) = rest.windows(2).position(|w| w == b"@@") {
		out.extend_from_slice(&rest[..i]);
		out.extend_from_slice(nonce.as_bytes());
		rest = &rest[i + 2..];
	}
	out.extend_from_slice(rest);
	out
}

fuzz_target!(|data: &[u8]| {
	if let Ok(text) = std::str::from_utf8(data)
		&& let Ok(v) = Verifier::parse(text)
	{
		assert_eq!(Verifier::parse(&v.encode()), Ok(v));
	}

	let mut parts = data.splitn(4, |&b| b == 0);
	let client_first = parts.next().unwrap_or_default();
	let client_final = parts.next().unwrap_or_default();
	let server_first = parts.next().unwrap_or_default();
	let server_final = parts.next().unwrap_or_default();

	// Lepis as the server, under each of the three channel binding states.
	let binding = match data.first().map(|b| b % 3) {
		Some(0) => Binding::NotOffered,
		Some(1) => Binding::Declined,
		_ => Binding::Chosen(b"0123456789abcdef0123456789abcdef"),
	};
	if let Ok((exchange, first)) = ServerExchange::start(verifier().clone(), client_first, binding) {
		let nonce = first
			.split(',')
			.find_map(|a| a.strip_prefix("r="))
			.expect("Lepis's server-first-message carries the nonce");
		let finished = exchange.finish(&with_nonce(client_final, nonce));
		assert!(finished.is_err(), "a proof without the password was accepted");
	}

	// Lepis as a client of a node, logging in with a client's key (pass-through).
	let (mut exchange, first) = ClientExchange::new(ClientCredential::Key(secret().clone()));
	let ours = first.rsplit_once("r=").expect("our nonce").1.to_string();
	if exchange.respond(&with_nonce(server_first, &ours)).is_ok() {
		assert!(
			exchange.verify(server_final).is_err(),
			"a node's signature without the verifier was accepted"
		);
	}
});
