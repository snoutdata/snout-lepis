//! TLS on both sides: the certificate Lepis serves to clients, and how it reaches nodes. Also the
//! `tls-server-end-point` channel binding (RFC 5929) of the certificate Lepis serves, which is
//! what lets a client's SCRAM-SHA-256-PLUS exchange prove nobody is relaying its TLS.

use std::path::Path;
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme};
use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};

use crate::config::{NodeAddress, SslMode};

/// The ALPN identifier Postgres 17+ uses for a direct TLS connection.
pub const ALPN_POSTGRESQL: &[u8] = b"postgresql";

fn provider() -> Arc<CryptoProvider> {
	Arc::new(rustls::crypto::ring::default_provider())
}

/// The certificate Lepis serves to clients, plus its `tls-server-end-point` binding data, or
/// None when the certificate's signature names no hash (Ed25519, Ed448) and SCRAM-SHA-256-PLUS
/// therefore cannot be offered. Computed once here: the certificate never changes while the
/// process runs.
pub fn server_config(
	cert: &Path,
	key: &Path,
) -> Result<(Arc<ServerConfig>, Option<Vec<u8>>), String> {
	let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert)
		.map_err(|e| format!("{}: {e}", cert.display()))?
		.collect::<Result<_, _>>()
		.map_err(|e| format!("{}: {e}", cert.display()))?;
	let Some(end_entity) = certs.first() else {
		return Err(format!("{}: no certificate in the file", cert.display()));
	};
	let binding = match tls_server_end_point(end_entity) {
		Ok(b) => Some(b),
		Err(e) => {
			// Not fatal: the certificate still serves TLS and only the PLUS mechanism is
			// withheld, which libpq's default `channel_binding=prefer` accepts.
			tracing::warn!(
				"{}: SCRAM channel binding is not offered: {e}",
				cert.display()
			);
			None
		}
	};
	let key = PrivateKeyDer::from_pem_file(key).map_err(|e| format!("{}: {e}", key.display()))?;
	let mut config = ServerConfig::builder_with_provider(provider())
		.with_safe_default_protocol_versions()
		.map_err(|e| e.to_string())?
		.with_no_client_auth()
		.with_single_cert(certs, key)
		.map_err(|e| e.to_string())?;
	config.alpn_protocols = vec![ALPN_POSTGRESQL.to_vec()];
	Ok((Arc::new(config), binding))
}

/// The client configuration for one node, or None for `sslmode=disable`.
pub fn client_config(node: &NodeAddress) -> Result<Option<Arc<ClientConfig>>, String> {
	let builder = ClientConfig::builder_with_provider(provider())
		.with_safe_default_protocol_versions()
		.map_err(|e| e.to_string())?;
	let mut config = match node.sslmode {
		SslMode::Disable => return Ok(None),
		SslMode::Require => builder
			.dangerous()
			.with_custom_certificate_verifier(Arc::new(EncryptOnly(provider())))
			.with_no_client_auth(),
		SslMode::VerifyFull => {
			let mut roots = RootCertStore::empty();
			match &node.ca_file {
				Some(path) => {
					for cert in CertificateDer::pem_file_iter(path)
						.map_err(|e| format!("{}: {e}", path.display()))?
					{
						let cert = cert.map_err(|e| format!("{}: {e}", path.display()))?;
						roots
							.add(cert)
							.map_err(|e| format!("{}: {e}", path.display()))?;
					}
				}
				None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
			}
			builder.with_root_certificates(roots).with_no_client_auth()
		}
	};
	config.alpn_protocols = vec![ALPN_POSTGRESQL.to_vec()];
	Ok(Some(Arc::new(config)))
}

pub fn server_name(host: &str) -> Result<ServerName<'static>, String> {
	ServerName::try_from(host.to_string()).map_err(|e| format!("{host}: {e}"))
}

/// `sslmode=require`: the channel is encrypted and the handshake's signatures are checked, but
/// the certificate itself is not, exactly as libpq's `require` behaves without a root file.
#[derive(Debug)]
struct EncryptOnly(Arc<CryptoProvider>);

impl ServerCertVerifier for EncryptOnly {
	fn verify_server_cert(
		&self,
		_end_entity: &CertificateDer<'_>,
		_intermediates: &[CertificateDer<'_>],
		_server_name: &ServerName<'_>,
		_ocsp_response: &[u8],
		_now: UnixTime,
	) -> Result<ServerCertVerified, rustls::Error> {
		Ok(ServerCertVerified::assertion())
	}

	fn verify_tls12_signature(
		&self,
		message: &[u8],
		cert: &CertificateDer<'_>,
		dss: &DigitallySignedStruct,
	) -> Result<HandshakeSignatureValid, rustls::Error> {
		verify_tls12_signature(
			message,
			cert,
			dss,
			&self.0.signature_verification_algorithms,
		)
	}

	fn verify_tls13_signature(
		&self,
		message: &[u8],
		cert: &CertificateDer<'_>,
		dss: &DigitallySignedStruct,
	) -> Result<HandshakeSignatureValid, rustls::Error> {
		verify_tls13_signature(
			message,
			cert,
			dss,
			&self.0.signature_verification_algorithms,
		)
	}

	fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
		self.0.signature_verification_algorithms.supported_schemes()
	}
}

// ---------------------------------------------------------------------------------------------
// tls-server-end-point (RFC 5929 §4.1): a hash of the server's end-entity certificate, with the
// hash named by the certificate's own signature algorithm, except that MD5 and SHA-1 become
// SHA-256. Postgres computes it the same way (be_tls_get_certificate_hash, through OpenSSL's
// X509_get_signature_info), so a client sees the same binding through Lepis as from a node
// serving this certificate. Where Postgres errors (a signature with no hash, such as Ed25519),
// Lepis returns an error and does not offer PLUS at all, rather than offering it and failing
// every client that takes it.

/// The hash `tls-server-end-point` uses for a certificate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndPointHash {
	Sha224,
	Sha256,
	Sha384,
	Sha512,
}

/// The binding data for a DER certificate.
pub fn tls_server_end_point(cert_der: &[u8]) -> Result<Vec<u8>, String> {
	Ok(match end_point_hash(cert_der)? {
		EndPointHash::Sha224 => Sha224::digest(cert_der).to_vec(),
		EndPointHash::Sha256 => Sha256::digest(cert_der).to_vec(),
		EndPointHash::Sha384 => Sha384::digest(cert_der).to_vec(),
		EndPointHash::Sha512 => Sha512::digest(cert_der).to_vec(),
	})
}

// Object identifiers as their DER content bytes, compared as bytes so nothing is decoded.
const PKCS1: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01]; // 1.2.840.113549.1.1
const ECDSA_SHA1: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x01]; // 1.2.840.10045.4.1
const ECDSA_SHA2: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03]; // 1.2.840.10045.4.3
const NIST_HASH: &[u8] = &[0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02]; // 2.16.840.1.101.3.4.2
const SHA1: &[u8] = &[0x2b, 0x0e, 0x03, 0x02, 0x1a]; // 1.3.14.3.2.26
const MD5: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x02, 0x05]; // 1.2.840.113549.2.5
const ED25519: &[u8] = &[0x2b, 0x65, 0x70]; // 1.3.101.112
const ED448: &[u8] = &[0x2b, 0x65, 0x71]; // 1.3.101.113

const TAG_SEQUENCE: u8 = 0x30;
const TAG_OID: u8 = 0x06;
const TAG_CONTEXT_0: u8 = 0xa0;

/// When `oid` is `arc` plus exactly one more single-byte arc, that arc.
fn last_arc(oid: &[u8], arc: &[u8]) -> Option<u8> {
	match oid.strip_prefix(arc) {
		Some(&[n]) => Some(n),
		_ => None,
	}
}

/// Which hash `tls-server-end-point` uses for this certificate, read from its outer
/// signatureAlgorithm (the one X509_get_signature_nid reads, not the copy inside the TBS).
pub fn end_point_hash(cert_der: &[u8]) -> Result<EndPointHash, String> {
	let bad = || "the certificate is not well-formed DER".to_string();
	// Certificate ::= SEQUENCE { tbsCertificate, signatureAlgorithm, signatureValue }
	let (cert, rest) = der_take(cert_der, TAG_SEQUENCE).ok_or_else(bad)?;
	if !rest.is_empty() {
		return Err(bad());
	}
	let (_tbs, after_tbs) = der_take(cert, TAG_SEQUENCE).ok_or_else(bad)?;
	let (algorithm, _signature) = der_take(after_tbs, TAG_SEQUENCE).ok_or_else(bad)?;
	// AlgorithmIdentifier ::= SEQUENCE { algorithm OBJECT IDENTIFIER, parameters ANY OPTIONAL }
	let (oid, params) = der_take(algorithm, TAG_OID).ok_or_else(bad)?;

	if let Some(n) = last_arc(oid, PKCS1) {
		return match n {
			// md5WithRSAEncryption and sha1WithRSAEncryption: RFC 5929 says SHA-256.
			4 | 5 | 11 => Ok(EndPointHash::Sha256),
			12 => Ok(EndPointHash::Sha384),
			13 => Ok(EndPointHash::Sha512),
			14 => Ok(EndPointHash::Sha224),
			10 => pss_hash(params).ok_or_else(|| "malformed RSASSA-PSS parameters".to_string()),
			_ => Err(format!(
				"unknown RSA signature algorithm 1.2.840.113549.1.1.{n}"
			)),
		};
	}
	if oid == ECDSA_SHA1 {
		return Ok(EndPointHash::Sha256);
	}
	if let Some(n) = last_arc(oid, ECDSA_SHA2) {
		return match n {
			1 => Ok(EndPointHash::Sha224),
			2 => Ok(EndPointHash::Sha256),
			3 => Ok(EndPointHash::Sha384),
			4 => Ok(EndPointHash::Sha512),
			_ => Err(format!(
				"unknown ECDSA signature algorithm 1.2.840.10045.4.3.{n}"
			)),
		};
	}
	if oid == ED25519 || oid == ED448 {
		return Err(
			"the certificate is signed with EdDSA, which names no hash for tls-server-end-point"
				.to_string(),
		);
	}
	Err("the certificate's signature algorithm is not one Lepis knows".to_string())
}

/// The hash in RSASSA-PSS-params (RFC 4055 §3.1). OpenSSL's X509_get_signature_info reads the
/// same field, so this matches what Postgres 16+ does with a PSS certificate.
///
/// ```text
/// RSASSA-PSS-params ::= SEQUENCE {
///     hashAlgorithm     [0] HashAlgorithm DEFAULT sha1,
///     maskGenAlgorithm  [1] ...,  saltLength [2] ...,  trailerField [3] ... }
/// ```
///
/// An absent hashAlgorithm is SHA-1, which becomes SHA-256 like any SHA-1 signature.
fn pss_hash(params: &[u8]) -> Option<EndPointHash> {
	let (seq, rest) = der_take(params, TAG_SEQUENCE)?;
	if !rest.is_empty() {
		return None;
	}
	if seq.first() != Some(&TAG_CONTEXT_0) {
		return Some(EndPointHash::Sha256);
	}
	let (explicit, _) = der_take(seq, TAG_CONTEXT_0)?;
	let (hash_alg, rest) = der_take(explicit, TAG_SEQUENCE)?;
	if !rest.is_empty() {
		return None;
	}
	let (oid, _) = der_take(hash_alg, TAG_OID)?;
	if oid == SHA1 || oid == MD5 {
		return Some(EndPointHash::Sha256);
	}
	match last_arc(oid, NIST_HASH)? {
		1 => Some(EndPointHash::Sha256),
		2 => Some(EndPointHash::Sha384),
		3 => Some(EndPointHash::Sha512),
		4 => Some(EndPointHash::Sha224),
		_ => None,
	}
}

/// One DER element with the expected tag: its contents and what follows it. Only the forms a
/// certificate uses are accepted (a single-byte tag, a definite length of at most four length
/// bytes), and every length is checked against what is actually there, so a truncated or
/// hostile input is None rather than a panic.
fn der_take(input: &[u8], tag: u8) -> Option<(&[u8], &[u8])> {
	let (&t, rest) = input.split_first()?;
	if t != tag {
		return None;
	}
	let (&first, rest) = rest.split_first()?;
	let (len, rest) = if first < 0x80 {
		(usize::from(first), rest)
	} else {
		// 0x80 alone is the indefinite length, which DER forbids.
		let n = usize::from(first & 0x7f);
		if n == 0 || n > 4 || rest.len() < n {
			return None;
		}
		let (bytes, rest) = rest.split_at(n);
		let len = bytes
			.iter()
			.fold(0usize, |acc, &b| (acc << 8) | usize::from(b));
		(len, rest)
	};
	if rest.len() < len {
		return None;
	}
	Some(rest.split_at(len))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn fixture(name: &str) -> CertificateDer<'static> {
		let path = Path::new(env!("CARGO_MANIFEST_DIR"))
			.join("tests/fixtures/certs")
			.join(name);
		CertificateDer::from_pem_file(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
	}

	fn hex(b: &[u8]) -> String {
		b.iter().map(|x| format!("{x:02x}")).collect()
	}

	/// The fixtures are `openssl req -x509` certificates (PEM, no keys; `.crt` so nothing mistakes
	/// them for a key file), one per signature algorithm; the
	/// expected hash is RFC 5929's rule applied to what `openssl x509 -text` names.
	#[test]
	fn the_hash_follows_the_signature_algorithm() {
		use EndPointHash::*;
		for (name, want) in [
			("rsa-sha1.crt", Some(Sha256)),
			("rsa-sha256.crt", Some(Sha256)),
			("rsa-sha384.crt", Some(Sha384)),
			("rsa-sha512.crt", Some(Sha512)),
			("rsa-pss-sha384.crt", Some(Sha384)),
			// PSS with every parameter at its default: the hash is SHA-1, so SHA-256.
			("rsa-pss-default.crt", Some(Sha256)),
			("ecdsa-sha256.crt", Some(Sha256)),
			("ecdsa-sha384.crt", Some(Sha384)),
			("ecdsa-sha512.crt", Some(Sha512)),
			("ed25519.crt", None),
		] {
			assert_eq!(end_point_hash(&fixture(name)).ok(), want, "{name}");
		}
	}

	/// The binding data against `openssl x509 -outform der | openssl dgst -<hash>`, so the hash
	/// is taken over exactly the certificate's DER bytes.
	#[test]
	fn binding_data_matches_openssl() {
		for (name, want) in [
			(
				"rsa-pss-sha384.crt",
				"4c381622cfa30e7f583e2241e1e2f6c09802c66da9ca0a47520e6887e8dda4bb844b7fab0af969570a22da7b01ccf568",
			),
			(
				"rsa-sha1.crt",
				"e24ab90596604a9a8794c8a4768c863f84aeb81ad94bf7e23f795898294d9b3c",
			),
			(
				"ecdsa-sha512.crt",
				"64ade68720c2830d5ca6a33e50f1b6e0e093569554111e9cab1b53485c8dd2c88958f32bfcab2995e001214607eb5e6aca157d6d93cf0637766143a49571daea",
			),
		] {
			assert_eq!(
				hex(&tls_server_end_point(&fixture(name)).unwrap()),
				want,
				"{name}"
			);
		}
		assert!(tls_server_end_point(&fixture("ed25519.crt")).is_err());
	}

	#[test]
	fn truncated_or_garbage_der_is_an_error_not_a_panic() {
		let der = fixture("ecdsa-sha256.crt").to_vec();
		for cut in 0..der.len() {
			assert!(end_point_hash(&der[..cut]).is_err(), "cut at {cut}");
		}
		let mut trailing = der.clone();
		trailing.push(0);
		assert!(end_point_hash(&trailing).is_err());
		assert!(end_point_hash(&[0x30, 0x84, 0xff, 0xff, 0xff, 0xff]).is_err());
		assert!(end_point_hash(&[0x30, 0x80, 0x00, 0x00]).is_err());
	}
}
