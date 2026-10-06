//! SCRAM-SHA-256 on both sides of the router, and the pass-through that joins them (L11).
//!
//! A client proves itself to Lepis against the role's stored verifier
//! (`SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>`). Verifying the proof recovers
//! the ClientKey, which is the one secret a SCRAM client needs, and Lepis then logs into each
//! node as that role with it. Lepis never sees or stores a password.
//!
//! That works only when every node holds the SAME verifier for the role (same salt, same
//! iterations), because the ClientKey is derived from the salted password. Lepis checks the
//! salt and iteration count each node offers and refuses with a sentence when they differ;
//! role sync (Phase 3) is what keeps them equal.
//!
//! RFC 5802 and RFC 7677, as Postgres implements them: the username in the SCRAM messages is
//! ignored (the startup packet's `user` is the role).
//!
//! Channel binding (SCRAM-SHA-256-PLUS, `tls-server-end-point`, RFC 5929) is offered to a client
//! connected over TLS, bound to the certificate LEPIS serves: the client's TLS ends here, so that
//! is the channel its proof must be tied to. Toward the nodes Lepis stays plain SCRAM-SHA-256
//! (`n,,`); that leg is Lepis's own connection, protected by the node's `sslmode`.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

pub const MECHANISM: &str = "SCRAM-SHA-256";
pub const MECHANISM_PLUS: &str = "SCRAM-SHA-256-PLUS";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramError(pub String);

impl std::fmt::Display for ScramError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str(&self.0)
	}
}

impl std::error::Error for ScramError {}

fn err<T>(m: impl Into<String>) -> Result<T, ScramError> {
	Err(ScramError(m.into()))
}

fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
	let mut m = HmacSha256::new_from_slice(key).expect("HMAC takes any key length");
	m.update(data);
	m.finalize().into_bytes().into()
}

fn sha256(data: &[u8]) -> [u8; 32] {
	Sha256::digest(data).into()
}

fn xor32(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
	let mut out = [0u8; 32];
	for i in 0..32 {
		out[i] = a[i] ^ b[i];
	}
	out
}

/// Equality that takes the same time wherever the first difference is.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
	if a.len() != b.len() {
		return false;
	}
	let mut d = 0u8;
	for (x, y) in a.iter().zip(b) {
		d |= x ^ y;
	}
	d == 0
}

fn random_nonce() -> String {
	let mut bytes = [0u8; 18];
	getrandom::fill(&mut bytes).expect("the operating system has randomness");
	B64.encode(bytes)
}

/// `Hi()` from RFC 5802: PBKDF2-HMAC-SHA-256 with one output block.
fn salted_password(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
	let mut block = salt.to_vec();
	block.extend_from_slice(&1u32.to_be_bytes());
	let mut u = hmac(password, &block);
	let mut out = u;
	for _ in 1..iterations {
		u = hmac(password, &u);
		for i in 0..32 {
			out[i] ^= u[i];
		}
	}
	out
}

/// A role's stored SCRAM verifier, as `pg_authid.rolpassword` holds it.
#[derive(Clone, PartialEq, Eq)]
pub struct Verifier {
	pub iterations: u32,
	pub salt: Vec<u8>,
	pub stored_key: [u8; 32],
	pub server_key: [u8; 32],
}

impl std::fmt::Debug for Verifier {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		// The keys are as good as the password for logging in through SCRAM; never print them.
		f.debug_struct("Verifier")
			.field("iterations", &self.iterations)
			.finish_non_exhaustive()
	}
}

impl Verifier {
	pub fn parse(s: &str) -> Result<Verifier, ScramError> {
		let rest = match s.strip_prefix("SCRAM-SHA-256$") {
			Some(r) => r,
			None => return err("the role's password is not a SCRAM-SHA-256 verifier"),
		};
		let (params, keys) = match rest.split_once('$') {
			Some(p) => p,
			None => return err("malformed verifier"),
		};
		let (iterations, salt) = match params.split_once(':') {
			Some(p) => p,
			None => return err("malformed verifier"),
		};
		let (stored, server) = match keys.split_once(':') {
			Some(p) => p,
			None => return err("malformed verifier"),
		};
		let decode32 = |k: &str| -> Result<[u8; 32], ScramError> {
			let v = B64.decode(k).or_else(|_| err("malformed verifier"))?;
			v.try_into().or_else(|_| err("malformed verifier"))
		};
		Ok(Verifier {
			iterations: iterations.parse().or_else(|_| err("malformed verifier"))?,
			salt: B64.decode(salt).or_else(|_| err("malformed verifier"))?,
			stored_key: decode32(stored)?,
			server_key: decode32(server)?,
		})
	}

	/// The verifier Postgres would store for this password and salt (for tests and for a
	/// configured service credential; a role's own password never passes through Lepis).
	pub fn from_password(password: &str, salt: &[u8], iterations: u32) -> Verifier {
		let salted = salted_password(password.as_bytes(), salt, iterations);
		let client_key = hmac(&salted, b"Client Key");
		Verifier {
			iterations,
			salt: salt.to_vec(),
			stored_key: sha256(&client_key),
			server_key: hmac(&salted, b"Server Key"),
		}
	}

	pub fn encode(&self) -> String {
		format!(
			"SCRAM-SHA-256${}:{}${}:{}",
			self.iterations,
			B64.encode(&self.salt),
			B64.encode(self.stored_key),
			B64.encode(self.server_key)
		)
	}
}

/// What the client side of an exchange holds: enough to log in as the role and to check the
/// node really has the verifier.
#[derive(Clone)]
pub struct ClientSecret {
	pub client_key: [u8; 32],
	pub verifier: Verifier,
}

impl ClientSecret {
	pub fn from_password(password: &str, salt: &[u8], iterations: u32) -> ClientSecret {
		let salted = salted_password(password.as_bytes(), salt, iterations);
		ClientSecret {
			client_key: hmac(&salted, b"Client Key"),
			verifier: Verifier::from_password(password, salt, iterations),
		}
	}
}

// ---------------------------------------------------------------------------------------------
// The server side: a client logging into Lepis.

/// The channel-binding name Lepis supports (RFC 5929 §4), the one every libpq uses.
pub const TLS_SERVER_END_POINT: &str = "tls-server-end-point";

/// What `ServerExchange::finish` says when the client's binding data is not this channel's,
/// which is either a relayed TLS session or a client bound to some other certificate.
pub const BINDING_CHECK_FAILED: &str = "SCRAM channel binding check failed";

/// What was offered and what the client chose, which decides the gs2 headers Lepis accepts.
#[derive(Debug, Clone, Copy)]
pub enum Binding<'a> {
	/// Only SCRAM-SHA-256 was offered (no TLS, or a certificate whose hash cannot be named).
	NotOffered,
	/// PLUS was offered and the client chose SCRAM-SHA-256.
	Declined,
	/// The client chose SCRAM-SHA-256-PLUS: the `tls-server-end-point` data of this connection.
	Chosen(&'a [u8]),
}

/// One client's exchange with Lepis acting as the server.
pub struct ServerExchange {
	verifier: Verifier,
	client_first_bare: String,
	server_first: String,
	/// What `c=` must decode to: the gs2 header, then the binding data when PLUS was chosen.
	channel: Vec<u8>,
	nonce: String,
}

impl ServerExchange {
	/// Reads the client-first-message and returns the exchange plus the server-first-message.
	pub fn start(
		verifier: Verifier,
		client_first: &[u8],
		binding: Binding<'_>,
	) -> Result<(Self, String), ScramError> {
		let msg = match std::str::from_utf8(client_first) {
			Ok(m) => m,
			Err(_) => return err("client-first-message is not UTF-8"),
		};
		// gs2-header: "n,," / "y,," / "p=<binding>,," and an authzid we do not accept.
		let mut parts = msg.splitn(3, ',');
		let cbind = parts.next().unwrap_or("");
		let authzid = parts.next().unwrap_or("");
		let bare = match parts.next() {
			Some(b) => b,
			None => return err("malformed client-first-message"),
		};
		// The flag must agree with the mechanism, and `y` must not meet a server that offered
		// PLUS: `y` means "I can bind but you did not offer it", so seeing it after an offer
		// means something between us removed PLUS from the list (RFC 5802 §6). The sentences
		// are Postgres's own (auth-scram.c), so a client sees the same refusal either way.
		let binding_data: &[u8] = match (cbind, binding) {
			("n", Binding::NotOffered | Binding::Declined) | ("y", Binding::NotOffered) => &[],
			("n" | "y", Binding::Chosen(_)) => {
				return err(
					"The client selected SCRAM-SHA-256-PLUS, but the SCRAM message does not include channel binding data.",
				);
			}
			("y", Binding::Declined) => {
				return err(
					"SCRAM channel binding negotiation error: the client supports channel binding and thinks the server does not, but this server does support it.",
				);
			}
			(c, b) if c.starts_with("p=") => {
				let Binding::Chosen(data) = b else {
					return err(
						"The client selected SCRAM-SHA-256 without channel binding, but the SCRAM message includes channel binding data.",
					);
				};
				if &c[2..] != TLS_SERVER_END_POINT {
					return err(format!(
						"unsupported SCRAM channel-binding type \"{}\"",
						&c[2..]
					));
				}
				data
			}
			_ => return err("malformed client-first-message"),
		};
		if !authzid.is_empty() {
			return err("an authorization identity is not supported");
		}
		let client_nonce = match bare.split(',').find_map(|a| a.strip_prefix("r=")) {
			Some(n)
				if !n.is_empty() && n.bytes().all(|c| (0x21..=0x7e).contains(&c) && c != b',') =>
			{
				n
			}
			_ => return err("client-first-message has no nonce"),
		};
		if bare.starts_with("m=") {
			return err("SCRAM extensions are not supported");
		}
		let nonce = format!("{client_nonce}{}", random_nonce());
		let server_first = format!(
			"r={nonce},s={},i={}",
			B64.encode(&verifier.salt),
			verifier.iterations
		);
		let mut channel = format!("{cbind},{authzid},").into_bytes();
		channel.extend_from_slice(binding_data);
		Ok((
			ServerExchange {
				verifier,
				client_first_bare: bare.to_string(),
				server_first: server_first.clone(),
				channel,
				nonce,
			},
			server_first,
		))
	}

	/// Reads the client-final-message. On success returns the server-final-message to send and
	/// the ClientKey the proof revealed.
	pub fn finish(self, client_final: &[u8]) -> Result<(String, [u8; 32]), ScramError> {
		let msg = match std::str::from_utf8(client_final) {
			Ok(m) => m,
			Err(_) => return err("client-final-message is not UTF-8"),
		};
		let (without_proof, proof) = match msg.rsplit_once(",p=") {
			Some(p) => p,
			None => return err("client-final-message has no proof"),
		};
		let mut attrs = without_proof.split(',');
		let channel = attrs.next().and_then(|a| a.strip_prefix("c="));
		let nonce = attrs.next().and_then(|a| a.strip_prefix("r="));
		// Compared decoded, as Postgres does. The data is public (a hash of the certificate),
		// so this needs no constant-time comparison.
		if channel.and_then(|c| B64.decode(c).ok()).as_deref() != Some(self.channel.as_slice()) {
			return err(BINDING_CHECK_FAILED);
		}
		if nonce != Some(self.nonce.as_str()) {
			return err("nonce does not match");
		}
		let proof: [u8; 32] = match B64.decode(proof).ok().and_then(|p| p.try_into().ok()) {
			Some(p) => p,
			None => return err("malformed proof"),
		};
		let auth_message = format!(
			"{},{},{}",
			self.client_first_bare, self.server_first, without_proof
		);
		let client_signature = hmac(&self.verifier.stored_key, auth_message.as_bytes());
		let client_key = xor32(&proof, &client_signature);
		if !constant_time_eq(&sha256(&client_key), &self.verifier.stored_key) {
			return err("password authentication failed");
		}
		let server_signature = hmac(&self.verifier.server_key, auth_message.as_bytes());
		Ok((format!("v={}", B64.encode(server_signature)), client_key))
	}
}

// ---------------------------------------------------------------------------------------------
// The client side: Lepis logging into a node.

/// What Lepis logs into a node with: the ClientKey a client revealed (pass-through), or, for
/// its own service connections only, a configured password.
#[derive(Clone)]
pub enum ClientCredential {
	Key(ClientSecret),
	Password(String),
}

pub struct ClientExchange {
	credential: ClientCredential,
	secret: Option<ClientSecret>,
	client_first_bare: String,
	nonce: String,
	auth_message: Option<String>,
}

impl ClientExchange {
	pub fn new(credential: ClientCredential) -> (Self, String) {
		let nonce = random_nonce();
		let bare = format!("n=,r={nonce}");
		let first = format!("n,,{bare}");
		(
			ClientExchange {
				credential,
				secret: None,
				client_first_bare: bare,
				nonce,
				auth_message: None,
			},
			first,
		)
	}

	/// Reads the node's server-first-message and returns the client-final-message.
	pub fn respond(&mut self, server_first: &[u8]) -> Result<String, ScramError> {
		let msg = match std::str::from_utf8(server_first) {
			Ok(m) => m,
			Err(_) => return err("server-first-message is not UTF-8"),
		};
		let mut nonce = None;
		let mut salt = None;
		let mut iterations = None;
		for a in msg.split(',') {
			if let Some(v) = a.strip_prefix("r=") {
				nonce = Some(v);
			} else if let Some(v) = a.strip_prefix("s=") {
				salt = B64.decode(v).ok();
			} else if let Some(v) = a.strip_prefix("i=") {
				iterations = v.parse::<u32>().ok();
			}
		}
		let (Some(nonce), Some(salt), Some(iterations)) = (nonce, salt, iterations) else {
			return err("malformed server-first-message");
		};
		if !nonce.starts_with(&self.nonce) || nonce.len() == self.nonce.len() {
			return err("the node's nonce does not extend ours");
		}
		let secret = match &self.credential {
			ClientCredential::Key(secret) => {
				if salt != secret.verifier.salt || iterations != secret.verifier.iterations {
					return err(
						"the role's password verifier on this node differs from the one Lepis authenticated against",
					);
				}
				secret.clone()
			}
			ClientCredential::Password(password) => {
				ClientSecret::from_password(password, &salt, iterations)
			}
		};
		let without_proof = format!("c=biws,r={nonce}");
		let auth_message = format!("{},{msg},{without_proof}", self.client_first_bare);
		let client_signature = hmac(&secret.verifier.stored_key, auth_message.as_bytes());
		let proof = xor32(&secret.client_key, &client_signature);
		self.auth_message = Some(auth_message);
		self.secret = Some(secret);
		Ok(format!("{without_proof},p={}", B64.encode(proof)))
	}

	/// Checks the node's server-final-message: the node proves it holds the verifier too.
	pub fn verify(&self, server_final: &[u8]) -> Result<(), ScramError> {
		let msg = String::from_utf8_lossy(server_final);
		if let Some(e) = msg.strip_prefix("e=") {
			return err(format!("the node refused: {e}"));
		}
		let Some(sig) = msg
			.strip_prefix("v=")
			.and_then(|v| B64.decode(v.trim()).ok())
		else {
			return err("malformed server-final-message");
		};
		let (Some(auth_message), Some(secret)) = (&self.auth_message, &self.secret) else {
			return err("server-final before server-first");
		};
		let expected = hmac(&secret.verifier.server_key, auth_message.as_bytes());
		if !constant_time_eq(&sig, &expected) {
			return err("the node's signature is wrong: it does not hold this role's verifier");
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// RFC 7677 §3's example, which fixes the salted password, both keys and the exchange.
	const RFC_PASSWORD: &str = "pencil";
	const RFC_SALT: &str = "W22ZaJ0SNY7soEsUEjb6gQ==";

	#[test]
	fn rfc7677_keys() {
		let salt = B64.decode(RFC_SALT).unwrap();
		let v = Verifier::from_password(RFC_PASSWORD, &salt, 4096);
		// ServerSignature for the RFC's AuthMessage, which pins StoredKey and ServerKey.
		let auth = "n=user,r=rOprNGfwEbeRWgbNEkqO,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096,c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
		let server_signature = hmac(&v.server_key, auth.as_bytes());
		assert_eq!(
			B64.encode(server_signature),
			"6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
		);
		let client_key = ClientSecret::from_password(RFC_PASSWORD, &salt, 4096).client_key;
		let proof = xor32(&client_key, &hmac(&v.stored_key, auth.as_bytes()));
		assert_eq!(
			B64.encode(proof),
			"dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
		);
	}

	#[test]
	fn verifier_round_trip() {
		let v = Verifier::from_password("secret", b"0123456789abcdef", 4096);
		assert_eq!(Verifier::parse(&v.encode()).unwrap(), v);
		assert!(Verifier::parse("md5abc").is_err());
	}

	/// Lepis as the server, then Lepis as a client with the recovered key against a second
	/// "node" holding the same verifier: the pass-through end to end.
	#[test]
	fn pass_through() {
		let v = Verifier::from_password("hunter2", b"saltsaltsaltsalt", 4096);
		let user = ClientSecret::from_password("hunter2", b"saltsaltsaltsalt", 4096);

		// The application logs into Lepis.
		let (mut app, first) = ClientExchange::new(ClientCredential::Key(user));
		let (server, server_first) =
			ServerExchange::start(v.clone(), first.as_bytes(), Binding::NotOffered).unwrap();
		let app_final = app.respond(server_first.as_bytes()).unwrap();
		let (server_final, recovered) = server.finish(app_final.as_bytes()).unwrap();
		app.verify(server_final.as_bytes()).unwrap();

		// Lepis logs into a node with what it recovered.
		let (mut lepis, first) = ClientExchange::new(ClientCredential::Key(ClientSecret {
			client_key: recovered,
			verifier: v.clone(),
		}));
		let (node, node_first) =
			ServerExchange::start(v.clone(), first.as_bytes(), Binding::NotOffered).unwrap();
		let lepis_final = lepis.respond(node_first.as_bytes()).unwrap();
		let (node_final, _) = node.finish(lepis_final.as_bytes()).unwrap();
		lepis.verify(node_final.as_bytes()).unwrap();
	}

	#[test]
	fn wrong_password_fails() {
		let v = Verifier::from_password("right", b"saltsaltsaltsalt", 4096);
		let (mut app, first) = ClientExchange::new(ClientCredential::Password("wrong".into()));
		let (server, server_first) =
			ServerExchange::start(v.clone(), first.as_bytes(), Binding::NotOffered).unwrap();
		let app_final = app.respond(server_first.as_bytes()).unwrap();
		assert!(server.finish(app_final.as_bytes()).is_err());
	}

	#[test]
	fn different_verifier_on_node_is_named() {
		let v = Verifier::from_password("pw", b"saltsaltsaltsalt", 4096);
		let other = Verifier::from_password("pw", b"othersaltothersa", 4096);
		let (mut lepis, first) = ClientExchange::new(ClientCredential::Key(ClientSecret {
			client_key: ClientSecret::from_password("pw", b"saltsaltsaltsalt", 4096).client_key,
			verifier: v,
		}));
		let (_, node_first) =
			ServerExchange::start(other, first.as_bytes(), Binding::NotOffered).unwrap();
		let e = lepis.respond(node_first.as_bytes()).unwrap_err();
		assert!(e.0.contains("differs"), "{e}");
	}

	/// A SCRAM-SHA-256-PLUS client, as libpq is one: the first message carries `gs2` and the
	/// final message's `c=` is base64(gs2 header + `cbind_data`). `ClientExchange` only ever
	/// speaks `n,,` (toward nodes), so the client side of PLUS lives here.
	fn plus_login(
		binding: Binding<'_>,
		gs2: &str,
		cbind_data: &[u8],
	) -> Result<[u8; 32], ScramError> {
		let secret = ClientSecret::from_password("pw", b"saltsaltsaltsalt", 4096);
		let bare = "n=,r=clientnonce";
		let (server, server_first) = ServerExchange::start(
			secret.verifier.clone(),
			format!("{gs2}{bare}").as_bytes(),
			binding,
		)?;
		let nonce = server_first
			.split(',')
			.find_map(|a| a.strip_prefix("r="))
			.unwrap();
		let mut channel = gs2.as_bytes().to_vec();
		channel.extend_from_slice(cbind_data);
		let without_proof = format!("c={},r={nonce}", B64.encode(&channel));
		let auth_message = format!("{bare},{server_first},{without_proof}");
		let proof = xor32(
			&secret.client_key,
			&hmac(&secret.verifier.stored_key, auth_message.as_bytes()),
		);
		let client_final = format!("{without_proof},p={}", B64.encode(proof));
		server.finish(client_final.as_bytes()).map(|(_, key)| key)
	}

	const CERT_HASH: &[u8] = &[7u8; 32];
	const PLUS_GS2: &str = "p=tls-server-end-point,,";

	#[test]
	fn plus_with_this_channels_binding_logs_in() {
		let key = plus_login(Binding::Chosen(CERT_HASH), PLUS_GS2, CERT_HASH).unwrap();
		assert_eq!(
			key,
			ClientSecret::from_password("pw", b"saltsaltsaltsalt", 4096).client_key
		);
	}

	#[test]
	fn plus_with_another_channels_binding_is_refused() {
		let e = plus_login(Binding::Chosen(CERT_HASH), PLUS_GS2, &[8u8; 32]).unwrap_err();
		assert_eq!(e.0, BINDING_CHECK_FAILED);
		// Binding data left out entirely (only the gs2 header in `c=`) is the same refusal.
		let e = plus_login(Binding::Chosen(CERT_HASH), PLUS_GS2, &[]).unwrap_err();
		assert_eq!(e.0, BINDING_CHECK_FAILED);
	}

	/// `y` says "the server did not offer PLUS"; after an offer, that is a downgrade.
	#[test]
	fn y_after_plus_was_offered_is_refused() {
		let e = plus_login(Binding::Declined, "y,,", &[]).unwrap_err();
		assert!(e.0.contains("negotiation error"), "{e}");
		// Without an offer, `y` is an honest client and logs in.
		plus_login(Binding::NotOffered, "y,,", &[]).unwrap();
		// So does `n` when the client declined an offer (libpq's channel_binding=disable).
		plus_login(Binding::Declined, "n,,", &[]).unwrap();
	}

	#[test]
	fn mechanism_and_gs2_header_must_agree() {
		// SCRAM-SHA-256 chosen, binding data sent anyway.
		for b in [Binding::NotOffered, Binding::Declined] {
			let e = plus_login(b, PLUS_GS2, CERT_HASH).unwrap_err();
			assert!(e.0.contains("without channel binding"), "{e}");
		}
		// SCRAM-SHA-256-PLUS chosen, no binding.
		for gs2 in ["n,,", "y,,"] {
			let e = plus_login(Binding::Chosen(CERT_HASH), gs2, &[]).unwrap_err();
			assert!(e.0.contains("does not include channel binding"), "{e}");
		}
	}

	#[test]
	fn unknown_binding_types_are_refused() {
		for gs2 in ["p=tls-unique,,", "p=tls-exporter,,", "p=,,"] {
			let e = plus_login(Binding::Chosen(CERT_HASH), gs2, CERT_HASH).unwrap_err();
			assert!(
				e.0.contains("unsupported SCRAM channel-binding type"),
				"{e}"
			);
		}
	}
}
