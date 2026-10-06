//! The shard hash, which is Postgres's own extended hash for the key's type (L5).
//!
//! Every node computes ownership in plain SQL, with no extension: the fence
//! (`CHECK (hashint8extended(id, seed) BETWEEN lo AND hi)`), the cleanup `DELETE`, the
//! publication row filter and the verify checksum are all ordinary expressions over Postgres's
//! built-in immutable hash functions. So the router must compute exactly the same number for the
//! same value, and this module is a port of those functions:
//!
//! - `hash_bytes_extended` and `hash_bytes_uint32_extended` from `src/common/hashfn.c`, which
//!   are Bob Jenkins's lookup3 (public domain) as Postgres adapted it, with the seed folded in;
//! - the per-type wrappers from `src/backend/access/hash/hashfunc.c`, `uuid.c`, `date.c`,
//!   `timestamp.c` and `varchar.c` (`bpchar`);
//! - `hash_numeric_extended` from `numeric.c`, with the parts of `numeric_in` and `numeric_recv`
//!   that decide which digits a value is stored as, since the hash is over those digits.
//!
//! Ported from PostgreSQL under the PostgreSQL Licence (THIRD-PARTY-NOTICES.md). Only the
//! little-endian path exists here; it is the one every supported platform takes, and the hash is
//! defined by what a little-endian server returns, which is what the differential test compares
//! against (`scripts/hash-check.sh`).
//!
//! Every function returns the value the SQL function returns: a signed `bigint`. Ranges in the
//! catalog are over that signed space, so a fence is a plain `BETWEEN` with no casts.

use std::fmt;

/// The key types a keyspace may use. Anything else is refused when the keyspace is made, with
/// this list in the message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum KeyType {
	Int2,
	Int4,
	Int8,
	/// `text` and `varchar`, which share one hash.
	Text,
	/// `character(n)` and `bpchar`, whose hash ignores trailing spaces (the padding).
	Bpchar,
	/// Unconstrained `numeric`. `numeric(p,s)` is refused: that column rounds a value to `s`
	/// places on the way in, so the row would be stored, and hashed by its fence, as a number
	/// the statement never wrote.
	Numeric,
	Uuid,
	Bytea,
	Date,
	Timestamp,
	Timestamptz,
}

impl KeyType {
	pub const ALL: [KeyType; 11] = [
		KeyType::Int2,
		KeyType::Int4,
		KeyType::Int8,
		KeyType::Text,
		KeyType::Bpchar,
		KeyType::Numeric,
		KeyType::Uuid,
		KeyType::Bytea,
		KeyType::Date,
		KeyType::Timestamp,
		KeyType::Timestamptz,
	];

	/// The Postgres type name, as `format_type` prints it for the canonical spelling.
	pub fn sql_name(self) -> &'static str {
		match self {
			KeyType::Int2 => "smallint",
			KeyType::Int4 => "integer",
			KeyType::Int8 => "bigint",
			KeyType::Text => "text",
			KeyType::Bpchar => "character",
			KeyType::Numeric => "numeric",
			KeyType::Uuid => "uuid",
			KeyType::Bytea => "bytea",
			KeyType::Date => "date",
			KeyType::Timestamp => "timestamp without time zone",
			KeyType::Timestamptz => "timestamp with time zone",
		}
	}

	/// The SQL expression a node evaluates to get the same hash, for a node of the given
	/// `server_version_num`. `column` must already be a quoted identifier.
	///
	/// Postgres 18 added `hashbyteaextended`, `hashdateextended` and
	/// `timestamptz_hash_extended`; on 17 (the oldest node Lepis accepts, L1) the same numbers
	/// come from portable forms: a date is its day count (which `-` refuses for ±infinity, so an
	/// infinite date key is refused on a 17 node), and a `timestamptz` read `AT TIME ZONE 'UTC'`
	/// is a `timestamp` with the same internal microseconds. `bytea` has no SQL-callable hash
	/// before 18 and is refused while any node runs 17.
	pub fn sql_expression(
		self,
		column: &str,
		seed: u64,
		server_version_num: u32,
	) -> Result<String, &'static str> {
		let pg18 = server_version_num >= 180_000;
		let seed = seed as i64;
		Ok(match self {
			KeyType::Int2 => format!("hashint2extended({column}, {seed})"),
			KeyType::Int4 => format!("hashint4extended({column}, {seed})"),
			KeyType::Int8 => format!("hashint8extended({column}, {seed})"),
			KeyType::Text => format!("hashtextextended({column}, {seed})"),
			KeyType::Bpchar => format!("hashbpcharextended({column}, {seed})"),
			KeyType::Numeric => format!("hash_numeric_extended({column}, {seed})"),
			KeyType::Uuid => format!("uuid_hash_extended({column}, {seed})"),
			KeyType::Bytea if pg18 => format!("hashbyteaextended({column}, {seed})"),
			KeyType::Bytea => return Err("a bytea shard key needs Postgres 18 on every node"),
			KeyType::Date if pg18 => format!("hashdateextended({column}, {seed})"),
			KeyType::Date => format!("hashint4extended({column} - date '2000-01-01', {seed})"),
			KeyType::Timestamp => format!("timestamp_hash_extended({column}, {seed})"),
			KeyType::Timestamptz if pg18 => format!("timestamptz_hash_extended({column}, {seed})"),
			KeyType::Timestamptz => {
				format!("timestamp_hash_extended({column} at time zone 'UTC', {seed})")
			}
		})
	}

	/// Accepts the names and aliases Postgres accepts for these types. A length on `character`
	/// is accepted, since padding does not change the hash; a precision on `numeric` is not (see
	/// [`KeyType::Numeric`]).
	pub fn from_sql_name(name: &str) -> Option<KeyType> {
		let n = name.trim().to_ascii_lowercase();
		if let Some((base, modifier)) = n.split_once('(') {
			let length = modifier.strip_suffix(')')?.trim();
			let is_char = matches!(base.trim_end(), "character" | "char" | "bpchar");
			let is_length = !length.is_empty() && length.bytes().all(|c| c.is_ascii_digit());
			return (is_char && is_length).then_some(KeyType::Bpchar);
		}
		Some(match n.as_str() {
			"smallint" | "int2" => KeyType::Int2,
			"integer" | "int" | "int4" => KeyType::Int4,
			"bigint" | "int8" => KeyType::Int8,
			"text" | "varchar" | "character varying" => KeyType::Text,
			"character" | "char" | "bpchar" => KeyType::Bpchar,
			"numeric" | "decimal" => KeyType::Numeric,
			"uuid" => KeyType::Uuid,
			"bytea" => KeyType::Bytea,
			"date" => KeyType::Date,
			"timestamp" | "timestamp without time zone" => KeyType::Timestamp,
			"timestamptz" | "timestamp with time zone" => KeyType::Timestamptz,
			_ => return None,
		})
	}

	/// The hash of a value given in Postgres's text output form (what `COPY … TO` and a text-mode
	/// result print with `DateStyle = ISO` and, for `timestamptz`, any offset).
	pub fn hash_text_value(self, value: &str, seed: u64) -> Result<i64, KeyError> {
		Ok(match self {
			KeyType::Int2 => hash_int2(parse_int::<i16>(value, self)?, seed),
			KeyType::Int4 => hash_int4(parse_int::<i32>(value, self)?, seed),
			KeyType::Int8 => hash_int8(parse_int::<i64>(value, self)?, seed),
			KeyType::Text => hash_text(value, seed),
			KeyType::Bpchar => hash_bpchar(value.as_bytes(), seed),
			KeyType::Numeric => hash_numeric(&parse_numeric(value)?, seed),
			KeyType::Uuid => hash_uuid(&parse_uuid(value)?, seed),
			KeyType::Bytea => hash_bytea(&parse_bytea(value)?, seed),
			KeyType::Date => hash_date(parse_date(value)?, seed),
			KeyType::Timestamp => hash_timestamp(parse_timestamp(value, false)?, seed),
			KeyType::Timestamptz => hash_timestamp(parse_timestamp(value, true)?, seed),
		})
	}

	/// The value in one spelling per value, so two texts Postgres reads as the same key compare
	/// equal: `007` and `7`, `'CA '` and `'CA'` as bpchar, `1.0` and `1.00`, a uuid in any case
	/// or with braces. A pin is matched on this, never on the raw text, since the hash and the
	/// fence both see the value and not its spelling.
	pub fn canonical_text(self, value: &str) -> Result<String, KeyError> {
		Ok(match self {
			KeyType::Int2 => parse_int::<i16>(value, self)?.to_string(),
			KeyType::Int4 => parse_int::<i32>(value, self)?.to_string(),
			KeyType::Int8 => parse_int::<i64>(value, self)?.to_string(),
			KeyType::Text => value.to_string(),
			KeyType::Bpchar => value.trim_end_matches(' ').to_string(),
			KeyType::Numeric => match parse_numeric(value)? {
				// NaN and the infinities all parse as Special; the text tells them apart.
				Numeric::Special => {
					let t = value.trim().to_ascii_lowercase();
					let t = t.strip_prefix('+').unwrap_or(&t);
					match t {
						"inf" => "infinity".to_string(),
						"-inf" => "-infinity".to_string(),
						t => t.to_string(),
					}
				}
				Numeric::Finite { weight, digits } => {
					// The parsed form drops the sign (the hash ignores it); zero has no sign.
					let negative = value.trim_start().starts_with('-') && !digits.is_empty();
					format!("{}{weight}:{digits:?}", if negative { "-" } else { "" })
				}
			},
			KeyType::Uuid => parse_uuid(value)?
				.iter()
				.map(|b| format!("{b:02x}"))
				.collect(),
			KeyType::Bytea => parse_bytea(value)?
				.iter()
				.map(|b| format!("{b:02x}"))
				.collect(),
			KeyType::Date => parse_date(value)?.to_string(),
			KeyType::Timestamp => parse_timestamp(value, false)?.to_string(),
			KeyType::Timestamptz => parse_timestamp(value, true)?.to_string(),
		})
	}

	/// The hash of a value given in Postgres's BINARY format (a Bind parameter with format 1).
	/// An integer key accepts any integer width: the integer hashes agree across widths, so a
	/// client that binds an int8 key as int4 still lands on the right node.
	pub fn hash_binary_value(self, value: &[u8], seed: u64) -> Result<i64, KeyError> {
		let err = |reason| KeyError {
			key_type: self,
			value: format!("<{} binary bytes>", value.len()),
			reason,
		};
		let int = |v: &[u8]| -> Option<i64> {
			Some(match v.len() {
				2 => i64::from(i16::from_be_bytes(v.try_into().ok()?)),
				4 => i64::from(i32::from_be_bytes(v.try_into().ok()?)),
				8 => i64::from_be_bytes(v.try_into().ok()?),
				_ => return None,
			})
		};
		Ok(match self {
			KeyType::Int2 | KeyType::Int4 | KeyType::Int8 => {
				hash_int8(int(value).ok_or_else(|| err("not a binary integer"))?, seed)
			}
			KeyType::Text => hash_text(
				std::str::from_utf8(value).map_err(|_| err("not UTF-8"))?,
				seed,
			),
			KeyType::Bpchar => {
				std::str::from_utf8(value).map_err(|_| err("not UTF-8"))?;
				hash_bpchar(value, seed)
			}
			KeyType::Numeric => hash_numeric(&recv_numeric(value).map_err(err)?, seed),
			KeyType::Uuid => hash_uuid(
				&value
					.try_into()
					.map_err(|_| err("a binary uuid is 16 bytes"))?,
				seed,
			),
			KeyType::Bytea => hash_bytea(value, seed),
			KeyType::Date => hash_date(
				i32::from_be_bytes(
					value
						.try_into()
						.map_err(|_| err("a binary date is 4 bytes"))?,
				),
				seed,
			),
			KeyType::Timestamp | KeyType::Timestamptz => hash_timestamp(
				i64::from_be_bytes(
					value
						.try_into()
						.map_err(|_| err("a binary timestamp is 8 bytes"))?,
				),
				seed,
			),
		})
	}
}

/// A shard key value the router could not read. It is a refusal, never a guess: a key read
/// wrongly sends a row to a node the fence then rejects, or worse, a read to the wrong node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyError {
	pub key_type: KeyType,
	pub value: String,
	pub reason: &'static str,
}

impl fmt::Display for KeyError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"cannot read {:?} as a {} shard key: {}",
			self.value,
			self.key_type.sql_name(),
			self.reason
		)
	}
}

impl std::error::Error for KeyError {}

fn key_error(key_type: KeyType, value: &str, reason: &'static str) -> KeyError {
	KeyError {
		key_type,
		value: value.chars().take(64).collect(),
		reason,
	}
}

// ---------------------------------------------------------------------------------------------
// lookup3, as Postgres has it (src/common/hashfn.c)

#[inline(always)]
fn mix(a: &mut u32, b: &mut u32, c: &mut u32) {
	*a = a.wrapping_sub(*c);
	*a ^= c.rotate_left(4);
	*c = c.wrapping_add(*b);
	*b = b.wrapping_sub(*a);
	*b ^= a.rotate_left(6);
	*a = a.wrapping_add(*c);
	*c = c.wrapping_sub(*b);
	*c ^= b.rotate_left(8);
	*b = b.wrapping_add(*a);
	*a = a.wrapping_sub(*c);
	*a ^= c.rotate_left(16);
	*c = c.wrapping_add(*b);
	*b = b.wrapping_sub(*a);
	*b ^= a.rotate_left(19);
	*a = a.wrapping_add(*c);
	*c = c.wrapping_sub(*b);
	*c ^= b.rotate_left(4);
	*b = b.wrapping_add(*a);
}

#[inline(always)]
fn final_mix(a: &mut u32, b: &mut u32, c: &mut u32) {
	*c ^= *b;
	*c = c.wrapping_sub(b.rotate_left(14));
	*a ^= *c;
	*a = a.wrapping_sub(c.rotate_left(11));
	*b ^= *a;
	*b = b.wrapping_sub(a.rotate_left(25));
	*c ^= *b;
	*c = c.wrapping_sub(b.rotate_left(16));
	*a ^= *c;
	*a = a.wrapping_sub(c.rotate_left(4));
	*b ^= *a;
	*b = b.wrapping_sub(a.rotate_left(14));
	*c ^= *b;
	*c = c.wrapping_sub(b.rotate_left(24));
}

#[inline(always)]
fn seed_state(a: &mut u32, b: &mut u32, c: &mut u32, seed: u64) {
	// The seed is treated as a 12-byte chunk of data padded with four zero bytes.
	if seed != 0 {
		*a = a.wrapping_add((seed >> 32) as u32);
		*b = b.wrapping_add(seed as u32);
		mix(a, b, c);
	}
}

#[inline(always)]
fn le32(k: &[u8]) -> u32 {
	u32::from_le_bytes([k[0], k[1], k[2], k[3]])
}

/// `hash_bytes_extended`: the hash of an arbitrary byte string.
pub fn hash_bytes_extended(key: &[u8], seed: u64) -> u64 {
	// Postgres passes the length as an int and stores it in a uint32.
	let mut len = key.len() as u32;
	let init = 0x9e37_79b9u32.wrapping_add(len).wrapping_add(3_923_095);
	let (mut a, mut b, mut c) = (init, init, init);
	seed_state(&mut a, &mut b, &mut c, seed);

	let mut k = key;
	while len >= 12 {
		a = a.wrapping_add(le32(&k[0..4]));
		b = b.wrapping_add(le32(&k[4..8]));
		c = c.wrapping_add(le32(&k[8..12]));
		mix(&mut a, &mut b, &mut c);
		k = &k[12..];
		len -= 12;
	}

	// The last 0-11 bytes. The lowest byte of c is reserved for the length, so c takes bytes
	// 8-10 shifted up by one byte.
	if len >= 11 {
		c = c.wrapping_add(u32::from(k[10]) << 24);
	}
	if len >= 10 {
		c = c.wrapping_add(u32::from(k[9]) << 16);
	}
	if len >= 9 {
		c = c.wrapping_add(u32::from(k[8]) << 8);
	}
	if len >= 8 {
		b = b.wrapping_add(u32::from(k[7]) << 24);
	}
	if len >= 7 {
		b = b.wrapping_add(u32::from(k[6]) << 16);
	}
	if len >= 6 {
		b = b.wrapping_add(u32::from(k[5]) << 8);
	}
	if len >= 5 {
		b = b.wrapping_add(u32::from(k[4]));
	}
	if len >= 4 {
		a = a.wrapping_add(u32::from(k[3]) << 24);
	}
	if len >= 3 {
		a = a.wrapping_add(u32::from(k[2]) << 16);
	}
	if len >= 2 {
		a = a.wrapping_add(u32::from(k[1]) << 8);
	}
	if len >= 1 {
		a = a.wrapping_add(u32::from(k[0]));
	}

	final_mix(&mut a, &mut b, &mut c);
	(u64::from(b) << 32) | u64::from(c)
}

/// `hash_bytes_uint32_extended`: the hash of one 32-bit word, used by every integer type.
pub fn hash_uint32_extended(k: u32, seed: u64) -> u64 {
	let init = 0x9e37_79b9u32.wrapping_add(4).wrapping_add(3_923_095);
	let (mut a, mut b, mut c) = (init, init, init);
	seed_state(&mut a, &mut b, &mut c, seed);
	a = a.wrapping_add(k);
	final_mix(&mut a, &mut b, &mut c);
	(u64::from(b) << 32) | u64::from(c)
}

// ---------------------------------------------------------------------------------------------
// The per-type functions, each returning what its SQL namesake returns.

/// `hashint2extended`: the value is widened to int4 first, so it agrees with `hashint4extended`.
pub fn hash_int2(v: i16, seed: u64) -> i64 {
	hash_int4(i32::from(v), seed)
}

/// `hashint4extended`.
pub fn hash_int4(v: i32, seed: u64) -> i64 {
	hash_uint32_extended(v as u32, seed) as i64
}

/// `hashint8extended`. The two halves are folded so that a value that fits in an int4 hashes
/// the same as that int4 does, which is what lets Postgres compare across integer widths.
pub fn hash_int8(v: i64, seed: u64) -> i64 {
	let hi = (v >> 32) as u32;
	let lo = (v as u32) ^ if v >= 0 { hi } else { !hi };
	hash_uint32_extended(lo, seed) as i64
}

/// `hashtextextended` under a deterministic collation: the hash of the string's bytes (the
/// database encoding, which for every node Lepis accepts is UTF-8).
pub fn hash_text(v: &str, seed: u64) -> i64 {
	hash_bytes_extended(v.as_bytes(), seed) as i64
}

/// `uuid_hash_extended`: the 16 bytes in network order.
pub fn hash_uuid(v: &[u8; 16], seed: u64) -> i64 {
	hash_bytes_extended(v, seed) as i64
}

/// `hashvarlenaextended`, which is what `bytea` uses.
pub fn hash_bytea(v: &[u8], seed: u64) -> i64 {
	hash_bytes_extended(v, seed) as i64
}

/// A `date` is days since 2000-01-01 and hashes as that int4.
pub fn hash_date(days: i32, seed: u64) -> i64 {
	hash_int4(days, seed)
}

/// `timestamp` and `timestamptz` are microseconds since 2000-01-01 00:00 UTC and hash as that
/// int8 (`timestamp_hash_extended` is `hashint8extended`).
pub fn hash_timestamp(micros: i64, seed: u64) -> i64 {
	hash_int8(micros, seed)
}

/// `hashbpcharextended` under a deterministic collation: the bytes without their trailing
/// spaces (`bcTruelen`), so `'CA'` and `'CA  '` are one key. Only the space is trimmed; a tab
/// or any other whitespace is part of the value.
pub fn hash_bpchar(v: &[u8], seed: u64) -> i64 {
	let len = v.iter().rposition(|&c| c != b' ').map_or(0, |i| i + 1);
	hash_bytes_extended(&v[..len], seed) as i64
}

/// A `numeric` as the hash sees it: Postgres stores a finite value as base-10000 digits plus
/// the weight (the power of 10000) of the first, and the hash covers only those, with leading
/// and trailing zero digits stripped. The sign and the display scale are not hashed, so 1.0,
/// 1.00 and -1 all hash alike; NaN and both infinities hash to the seed itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Numeric {
	/// NaN, `Infinity` or `-Infinity`.
	Special,
	/// `digits` are base-10000, most significant first, with no leading or trailing zero;
	/// zero has none.
	Finite { weight: i32, digits: Vec<i16> },
}

const NBASE: i32 = 10_000;
const DEC_DIGITS: i32 = 4;
/// `NUMERIC_DSCALE_MASK`: a scale wider than 14 bits overflows the stored header.
const NUMERIC_DSCALE_MAX: i32 = 0x3fff;

impl Numeric {
	/// `strip_var` plus `make_result`'s range check: zero digits off both ends, a weight that
	/// fits the stored int16, and a display scale that fits its 14 bits.
	fn stored(mut weight: i32, mut digits: Vec<i16>, dscale: i32) -> Option<Numeric> {
		let lead = digits.iter().take_while(|d| **d == 0).count();
		digits.drain(..lead);
		weight -= lead as i32;
		let keep = digits.iter().rposition(|d| *d != 0).map_or(0, |i| i + 1);
		digits.truncate(keep);
		if digits.is_empty() {
			weight = 0;
		}
		if i16::try_from(weight).is_err() || !(0..=NUMERIC_DSCALE_MAX).contains(&dscale) {
			return None;
		}
		Some(Numeric::Finite { weight, digits })
	}
}

/// `hash_numeric_extended`.
pub fn hash_numeric(v: &Numeric, seed: u64) -> i64 {
	let Numeric::Finite { weight, digits } = v else {
		return seed as i64;
	};
	// The hash strips zero digits itself; a stored value has none, but a caller's might.
	let start = digits.iter().take_while(|d| **d == 0).count();
	if start == digits.len() {
		return seed.wrapping_sub(1) as i64;
	}
	let end = digits.iter().rposition(|d| *d != 0).map_or(0, |i| i + 1);
	let bytes: Vec<u8> = digits[start..end]
		.iter()
		.flat_map(|d| d.to_le_bytes())
		.collect();
	// `digit_hash ^ weight`: the int weight is widened to 64 bits with its sign.
	let weight = i64::from(*weight - start as i32) as u64;
	(hash_bytes_extended(&bytes, seed) ^ weight) as i64
}

// ---------------------------------------------------------------------------------------------
// Reading values in Postgres's text output form.

fn parse_int<T: std::str::FromStr>(value: &str, key_type: KeyType) -> Result<T, KeyError> {
	// Postgres accepts surrounding whitespace and a leading '+'.
	let t = value.trim();
	let t = t.strip_prefix('+').unwrap_or(t);
	t.parse::<T>()
		.map_err(|_| key_error(key_type, value, "not an integer of that width"))
}

fn hex_val(c: u8) -> Option<u8> {
	match c {
		b'0'..=b'9' => Some(c - b'0'),
		b'a'..=b'f' => Some(c - b'a' + 10),
		b'A'..=b'F' => Some(c - b'A' + 10),
		_ => None,
	}
}

/// `uuid` input: 32 hex digits, optionally in braces, with a hyphen allowed after any group of
/// four digits (which is what Postgres's `string_to_uuid` accepts).
fn parse_uuid(value: &str) -> Result<[u8; 16], KeyError> {
	let err = || key_error(KeyType::Uuid, value, "not a uuid");
	let t = value.trim().as_bytes();
	let t = if t.first() == Some(&b'{') {
		if t.last() != Some(&b'}') || t.len() < 2 {
			return Err(err());
		}
		&t[1..t.len() - 1]
	} else {
		t
	};
	let mut out = [0u8; 16];
	let mut i = 0usize;
	let mut digits = 0usize;
	while digits < 32 {
		let hi = hex_val(*t.get(i).ok_or_else(err)?).ok_or_else(err)?;
		let lo = hex_val(*t.get(i + 1).ok_or_else(err)?).ok_or_else(err)?;
		out[digits / 2] = (hi << 4) | lo;
		digits += 2;
		i += 2;
		if digits < 32 && digits.is_multiple_of(4) && t.get(i) == Some(&b'-') {
			i += 1;
		}
	}
	if i != t.len() {
		return Err(err());
	}
	Ok(out)
}

/// `bytea` input in the hex format (`\x…`), the only form Postgres outputs by default. The
/// legacy escape format is refused rather than half-read.
fn parse_bytea(value: &str) -> Result<Vec<u8>, KeyError> {
	let err = |r| key_error(KeyType::Bytea, value, r);
	let hex = value
		.strip_prefix("\\x")
		.ok_or_else(|| err("only the hex format (\\x…) is read"))?;
	let digits: Vec<u8> = hex.bytes().filter(|c| !c.is_ascii_whitespace()).collect();
	if !digits.len().is_multiple_of(2) {
		return Err(err("odd number of hex digits"));
	}
	digits
		.chunks(2)
		.map(|p| match (hex_val(p[0]), hex_val(p[1])) {
			(Some(h), Some(l)) => Ok((h << 4) | l),
			_ => Err(err("not a hex digit")),
		})
		.collect()
}

/// C's `isspace` in the C locale, which is what `numeric_in` skips (it includes `\v`, which
/// Rust's `is_ascii_whitespace` does not).
fn c_space(c: u8) -> bool {
	matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// `numeric_in` for an unconstrained column: the grammar of Postgres 17 and 18, which is the
/// same (`_` between digits, `0x`/`0o`/`0b` integers and the infinities all predate 17).
fn parse_numeric(value: &str) -> Result<Numeric, KeyError> {
	let syntax = || key_error(KeyType::Numeric, value, "not a number");
	let range = || key_error(KeyType::Numeric, value, "out of numeric's range");
	let s = value.as_bytes();
	let at = |i: usize| s.get(i).copied().unwrap_or(0);
	let mut i = 0;
	while c_space(at(i)) {
		i += 1;
	}
	let start = i;
	if matches!(at(i), b'+' | b'-') {
		i += 1;
	}

	// Everything else starts with a digit or a point after the sign.
	if !at(i).is_ascii_digit() && at(i) != b'.' {
		let word = |from: usize, w: &str| {
			s.len() >= from + w.len() && s[from..from + w.len()].eq_ignore_ascii_case(w.as_bytes())
		};
		// NaN takes no sign, so it is matched from before one.
		let end = if word(start, "nan") {
			start + 3
		} else if word(i, "infinity") {
			i + 8
		} else if word(i, "inf") {
			i + 3
		} else {
			return Err(syntax());
		};
		return if s[end..].iter().all(|c| c_space(*c)) {
			Ok(Numeric::Special)
		} else {
			Err(syntax())
		};
	}

	let base = match (at(i), at(i + 1)) {
		(b'0', b'x' | b'X') => 16,
		(b'0', b'o' | b'O') => 8,
		(b'0', b'b' | b'B') => 2,
		_ => 10,
	};
	let (n, end) = if base == 10 {
		decimal_numeric(s, i).ok_or_else(syntax)?
	} else {
		integer_numeric(s, i + 2, base).ok_or_else(syntax)?
	};
	if !s[end..].iter().all(|c| c_space(*c)) {
		return Err(syntax());
	}
	n.ok_or_else(range)
}

/// `set_var_from_str`: decimal digits, an optional point, `_` between digits, an optional
/// exponent. Returns the value (None when it overflows the stored format) and where it ended,
/// or None for a syntax error.
fn decimal_numeric(s: &[u8], mut i: usize) -> Option<(Option<Numeric>, usize)> {
	let at = |i: usize| s.get(i).copied().unwrap_or(0);
	let mut have_dp = false;
	if at(i) == b'.' {
		have_dp = true;
		i += 1;
	}
	if !at(i).is_ascii_digit() {
		return None;
	}
	// Four zeros of padding in front and three behind, for aligning to base-10000 digits.
	let mut dec: Vec<u8> = vec![0; DEC_DIGITS as usize];
	let mut dweight: i64 = -1;
	let mut dscale: i64 = 0;
	loop {
		let c = at(i);
		if c.is_ascii_digit() {
			dec.push(c - b'0');
			if have_dp {
				dscale += 1;
			} else {
				dweight += 1;
			}
			i += 1;
		} else if c == b'.' {
			if have_dp {
				return None;
			}
			have_dp = true;
			i += 1;
			if at(i) == b'_' {
				return None;
			}
		} else if c == b'_' {
			i += 1;
			if !at(i).is_ascii_digit() {
				return None;
			}
		} else {
			break;
		}
	}
	let ddigits = dec.len() as i64 - i64::from(DEC_DIGITS);
	dec.extend([0; DEC_DIGITS as usize - 1]);

	if matches!(at(i), b'e' | b'E') {
		i += 1;
		let neg = match at(i) {
			b'+' => {
				i += 1;
				false
			}
			b'-' => {
				i += 1;
				true
			}
			_ => false,
		};
		if !at(i).is_ascii_digit() {
			return None;
		}
		let mut exponent: i64 = 0;
		loop {
			let c = at(i);
			if c.is_ascii_digit() {
				exponent = exponent * 10 + i64::from(c - b'0');
				i += 1;
				if exponent > i64::from(i32::MAX / 2) {
					return Some((None, s.len()));
				}
			} else if c == b'_' {
				i += 1;
				if !at(i).is_ascii_digit() {
					return None;
				}
			} else {
				break;
			}
		}
		let exponent = if neg { -exponent } else { exponent };
		dweight += exponent;
		dscale = (dscale - exponent).max(0);
	}

	// Postgres's conversion to base 10000, with C's truncating division (the operands here are
	// never negative where it matters).
	let dd = i64::from(DEC_DIGITS);
	let weight = if dweight >= 0 {
		(dweight + 1 + dd - 1) / dd - 1
	} else {
		-((-dweight - 1) / dd + 1)
	};
	let offset = (weight + 1) * dd - (dweight + 1);
	let ndigits = (ddigits + offset + dd - 1) / dd;
	let mut k = (dd - offset) as usize;
	let mut digits = Vec::with_capacity(ndigits as usize);
	for _ in 0..ndigits {
		let d = dec[k..k + 4]
			.iter()
			.fold(0i16, |acc, x| acc * 10 + i16::from(*x));
		digits.push(d);
		k += 4;
	}
	let n = i32::try_from(weight)
		.ok()
		.zip(i32::try_from(dscale).ok())
		.and_then(|(w, d)| Numeric::stored(w, digits, d));
	Some((n, i))
}

/// `set_var_from_non_decimal_integer_str`: hex, octal or binary digits after the prefix, with
/// `_` allowed before any digit (so `0x_ff` is accepted, as Postgres accepts it).
fn integer_numeric(s: &[u8], mut i: usize, base: u32) -> Option<(Option<Numeric>, usize)> {
	let first = i;
	let digit = |c: u8| (c as char).to_digit(base);
	// The value in base 10000, least significant first.
	let mut acc: Vec<u32> = Vec::new();
	loop {
		let c = s.get(i).copied().unwrap_or(0);
		if let Some(d) = digit(c) {
			let mut carry = d;
			for x in acc.iter_mut() {
				let v = *x * base + carry;
				*x = v % NBASE as u32;
				carry = v / NBASE as u32;
			}
			while carry > 0 {
				acc.push(carry % NBASE as u32);
				carry /= NBASE as u32;
			}
			i += 1;
			// Postgres stops as soon as the weight passes int16; so does this.
			if acc.len() > i16::MAX as usize + 1 {
				return Some((None, s.len()));
			}
		} else if c == b'_' {
			i += 1;
			// An underscore must be followed by a digit.
			digit(s.get(i).copied().unwrap_or(0))?;
		} else {
			break;
		}
	}
	if i == first {
		return None;
	}
	let weight = acc.len() as i32 - 1;
	let digits = acc.iter().rev().map(|d| *d as i16).collect();
	Some((Numeric::stored(weight, digits, 0), i))
}

/// `numeric_recv`: the binary form (`int16 ndigits, int16 weight, uint16 sign, uint16 dscale`,
/// then the digits), with digits the scale hides truncated away exactly as Postgres does.
fn recv_numeric(v: &[u8]) -> Result<Numeric, &'static str> {
	let word = |i: usize| -> Option<u16> { Some(u16::from_be_bytes([*v.get(i)?, *v.get(i + 1)?])) };
	let header = || -> Option<(u16, i16, u16, u16)> {
		Some((word(0)?, word(2)? as i16, word(4)?, word(6)?))
	};
	let (len, weight, sign, dscale) = header().ok_or("a binary numeric has an 8-byte header")?;
	if v.len() != 8 + 2 * usize::from(len) {
		return Err("a binary numeric's length does not match its digit count");
	}
	match sign {
		0x0000 | 0x4000 => {}
		0xc000 | 0xd000 | 0xf000 => return Ok(Numeric::Special),
		_ => return Err("not a numeric sign"),
	}
	if i32::from(dscale) > NUMERIC_DSCALE_MAX {
		return Err("not a numeric scale");
	}
	let mut digits = Vec::with_capacity(usize::from(len));
	for k in 0..usize::from(len) {
		let d = word(8 + 2 * k).ok_or("short")? as i16;
		if !(0..NBASE as i16).contains(&d) {
			return Err("not a numeric digit");
		}
		digits.push(d);
	}
	let mut weight = i32::from(weight);
	let dscale = i32::from(dscale);

	// `trunc_var(value, dscale)`.
	let di = (weight + 1) * DEC_DIGITS + dscale;
	if di <= 0 {
		digits.clear();
		weight = 0;
	} else {
		let wanted = ((di + DEC_DIGITS - 1) / DEC_DIGITS) as usize;
		if wanted <= digits.len() {
			digits.truncate(wanted);
			let keep = di % DEC_DIGITS;
			if keep > 0 {
				let pow10 = [0i16, 1000, 100, 10][keep as usize];
				let last = digits.last_mut().expect("wanted >= 1");
				*last -= *last % pow10;
			}
		}
	}
	Numeric::stored(weight, digits, dscale).ok_or("out of numeric's range")
}

/// Days from 1970-01-01 to a proleptic Gregorian date (Howard Hinnant's `days_from_civil`).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
	let y = if m <= 2 { y - 1 } else { y };
	let era = if y >= 0 { y } else { y - 399 } / 400;
	let yoe = y - era * 400;
	let m = i64::from(m);
	let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + i64::from(d) - 1;
	let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
	era * 146_097 + doe - 719_468
}

/// Days from 1970-01-01 to 2000-01-01, Postgres's epoch.
const PG_EPOCH_DAYS: i64 = 10_957;
const MICROS_PER_DAY: i64 = 86_400_000_000;

fn is_leap(y: i64) -> bool {
	(y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i64, m: u32) -> u32 {
	match m {
		1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
		4 | 6 | 9 | 11 => 30,
		_ => {
			if is_leap(y) {
				29
			} else {
				28
			}
		}
	}
}

/// `YYYY-MM-DD` (four or more year digits) into days since 2000-01-01, with the date validated.
fn parse_ymd(s: &str) -> Option<i64> {
	let mut it = s.splitn(3, '-');
	let y = it.next()?;
	let m = it.next()?;
	let d = it.next()?;
	if y.len() < 4 || m.len() != 2 || d.len() != 2 {
		return None;
	}
	if !(y.bytes().all(|c| c.is_ascii_digit())
		&& m.bytes().all(|c| c.is_ascii_digit())
		&& d.bytes().all(|c| c.is_ascii_digit()))
	{
		return None;
	}
	let y: i64 = y.parse().ok()?;
	let m: u32 = m.parse().ok()?;
	let d: u32 = d.parse().ok()?;
	if !(1..=12).contains(&m) || d == 0 || d > days_in_month(y, m) {
		return None;
	}
	Some(days_from_civil(y, m, d) - PG_EPOCH_DAYS)
}

/// A `date` in ISO form, or `infinity` / `-infinity`. BC dates are refused (`DateStyle = ISO`
/// prints them with a ` BC` suffix, and a shard key in the first millennium is not a real case).
fn parse_date(value: &str) -> Result<i32, KeyError> {
	let err = || key_error(KeyType::Date, value, "not an ISO date (YYYY-MM-DD)");
	match value.trim() {
		"infinity" => return Ok(i32::MAX),
		"-infinity" => return Ok(i32::MIN),
		_ => {}
	}
	let days = parse_ymd(value.trim()).ok_or_else(err)?;
	i32::try_from(days).map_err(|_| err())
}

/// A `timestamp` or `timestamptz` in ISO form: `YYYY-MM-DD HH:MM:SS[.ffffff]`, with a `T`
/// accepted for the space, and for `timestamptz` a required offset `±HH[:MM[:SS]]` (Postgres
/// prints one; reading one without it would mean guessing the session's `TimeZone`).
fn parse_timestamp(value: &str, with_zone: bool) -> Result<i64, KeyError> {
	let key_type = if with_zone {
		KeyType::Timestamptz
	} else {
		KeyType::Timestamp
	};
	let err = |r| key_error(key_type, value, r);
	let t = value.trim();
	match t {
		"infinity" => return Ok(i64::MAX),
		"-infinity" => return Ok(i64::MIN),
		_ => {}
	}
	let (date, rest) = t
		.split_once([' ', 'T'])
		.ok_or_else(|| err("not an ISO timestamp"))?;
	let days = parse_ymd(date).ok_or_else(|| err("not an ISO date"))?;

	// Split off the zone: the last '+' or '-' after the time's start.
	let (time, zone) = match rest.rfind(['+', '-']) {
		Some(i) => (&rest[..i], Some(&rest[i..])),
		None => (rest, None),
	};
	if with_zone != zone.is_some() {
		return Err(err(if with_zone {
			"a timestamptz key needs its offset"
		} else {
			"a timestamp key has no offset"
		}));
	}

	let (hms, frac) = match time.split_once('.') {
		Some((h, f)) => (h, Some(f)),
		None => (time, None),
	};
	let mut parts = hms.split(':');
	let h: i64 = num2(parts.next()).ok_or_else(|| err("bad hour"))?;
	let mi: i64 = num2(parts.next()).ok_or_else(|| err("bad minute"))?;
	let s: i64 = num2(parts.next()).ok_or_else(|| err("bad second"))?;
	if parts.next().is_some() || h > 24 || mi > 59 || s > 60 || (h == 24 && (mi > 0 || s > 0)) {
		return Err(err("bad time of day"));
	}
	let micros_frac = match frac {
		None => 0,
		Some(f) if !f.is_empty() && f.len() <= 6 && f.bytes().all(|c| c.is_ascii_digit()) => {
			let n: i64 = f.parse().map_err(|_| err("bad fraction"))?;
			n * 10i64.pow(6 - f.len() as u32)
		}
		Some(_) => {
			return Err(err(
				"more than microsecond precision is rounded by Postgres",
			));
		}
	};
	let mut micros = ((h * 60 + mi) * 60 + s) * 1_000_000 + micros_frac;
	if h == 24 && micros_frac > 0 {
		return Err(err("bad time of day"));
	}

	if let Some(z) = zone {
		let sign: i64 = if z.starts_with('-') { -1 } else { 1 };
		let mut zp = z[1..].split(':');
		let zh = num2(zp.next()).ok_or_else(|| err("bad offset"))?;
		let zm = zp
			.next()
			.map_or(Some(0), |p| num2(Some(p)))
			.ok_or_else(|| err("bad offset"))?;
		let zs = zp
			.next()
			.map_or(Some(0), |p| num2(Some(p)))
			.ok_or_else(|| err("bad offset"))?;
		if zp.next().is_some() || zh > 15 || zm > 59 || zs > 59 {
			return Err(err("bad offset"));
		}
		micros -= sign * ((zh * 60 + zm) * 60 + zs) * 1_000_000;
	}

	days.checked_mul(MICROS_PER_DAY)
		.and_then(|d| d.checked_add(micros))
		.ok_or_else(|| err("out of range"))
}

fn num2(p: Option<&str>) -> Option<i64> {
	let p = p?;
	if p.len() != 2 || !p.bytes().all(|c| c.is_ascii_digit()) {
		return None;
	}
	p.parse().ok()
}

#[cfg(test)]
mod tests {
	use super::*;

	// The real check is `scripts/hash-check.sh`, over millions of values against a running
	// Postgres; `tests/fixtures/hash.tsv` holds a sample of its output so `cargo test` checks the
	// port without one (tests/hash_fixture.rs). These are the properties around it.
	#[test]
	fn integer_widths_agree() {
		for v in [-5i64, 0, 1, 42, i64::from(i32::MAX), i64::from(i32::MIN)] {
			let as4 = i32::try_from(v).unwrap();
			assert_eq!(hash_int8(v, 0), hash_int4(as4, 0), "{v}");
			assert_eq!(hash_int8(v, 7), hash_int4(as4, 7), "{v}");
		}
		assert_eq!(hash_int2(-3, 9), hash_int4(-3, 9));
	}

	#[test]
	fn binary_agrees_with_text() {
		let s = 99;
		for (t, text, bin) in [
			(KeyType::Int8, "-7", (-7i64).to_be_bytes().to_vec()),
			(KeyType::Int8, "-7", (-7i32).to_be_bytes().to_vec()),
			(KeyType::Int4, "70000", 70000i64.to_be_bytes().to_vec()),
			(KeyType::Text, "héllo", "héllo".as_bytes().to_vec()),
			(KeyType::Date, "2000-01-02", 1i32.to_be_bytes().to_vec()),
			(
				KeyType::Timestamptz,
				"2000-01-01 00:00:01+00",
				1_000_000i64.to_be_bytes().to_vec(),
			),
			(
				KeyType::Uuid,
				"00010203-0405-0607-0809-0a0b0c0d0e0f",
				(0u8..16).collect(),
			),
		] {
			assert_eq!(
				t.hash_binary_value(&bin, s).unwrap(),
				t.hash_text_value(text, s).unwrap(),
				"{t:?} {text}"
			);
		}
		assert!(KeyType::Uuid.hash_binary_value(&[1, 2, 3], s).is_err());
	}

	#[test]
	fn numeric_normalises_as_postgres_stores() {
		let h = |v: &str| KeyType::Numeric.hash_text_value(v, 5).unwrap();
		let one = h("1");
		for v in ["1.0", "1.00", "+1", " 1e0 ", "0.1e1", "10e-1", "0001.000", "-1", "1_0e-1"] {
			assert_eq!(h(v), one, "{v}");
		}
		// Digits are grouped in fours from the point, so these differ only by weight.
		assert_ne!(h("1"), h("10000"));
		assert_eq!(
			parse_numeric("12345.678").unwrap(),
			Numeric::Finite {
				weight: 1,
				digits: vec![1, 2345, 6780]
			}
		);
		assert_eq!(
			parse_numeric("0x10000").unwrap(),
			parse_numeric("65536").unwrap()
		);
		assert_eq!(h("0"), h("-0.000e5"));
		assert_eq!(h("0"), (5u64.wrapping_sub(1)) as i64);
		for v in ["NaN", "nan ", "Infinity", "-inf"] {
			assert_eq!(h(v), 5, "{v}");
		}
		for v in ["", ".", "1..2", "1._2", "+-1", "-NaN", "0x", "1e", "1 2", "1__0", "1e-20000"] {
			assert!(parse_numeric(v).is_err(), "{v}");
		}
	}

	#[test]
	fn numeric_binary_agrees_with_text() {
		let send = |weight: i16, sign: u16, dscale: u16, digits: &[i16]| {
			let mut b = Vec::new();
			b.extend((digits.len() as u16).to_be_bytes());
			b.extend(weight.to_be_bytes());
			b.extend(sign.to_be_bytes());
			b.extend(dscale.to_be_bytes());
			for d in digits {
				b.extend(d.to_be_bytes());
			}
			b
		};
		let bin = |b: Vec<u8>| KeyType::Numeric.hash_binary_value(&b, 9).unwrap();
		let txt = |v: &str| KeyType::Numeric.hash_text_value(v, 9).unwrap();
		assert_eq!(bin(send(0, 0, 1, &[1, 5000])), txt("1.5"));
		assert_eq!(bin(send(1, 0x4000, 0, &[12, 3456])), txt("-123456"));
		// A scale that hides digits truncates them, as numeric_recv does.
		assert_eq!(bin(send(0, 0, 1, &[1, 5678])), txt("1.5"));
		assert_eq!(bin(send(-1, 0, 2, &[99])), txt("0"));
		assert_eq!(bin(send(0, 0xc000, 0, &[])), txt("NaN"));
		assert!(KeyType::Numeric.hash_binary_value(&send(0, 0, 0, &[10000]), 9).is_err());
		assert!(KeyType::Numeric.hash_binary_value(&[0, 1, 0, 0], 9).is_err());
	}

	#[test]
	fn bpchar_ignores_padding_only() {
		let h = |v: &str| KeyType::Bpchar.hash_text_value(v, 3).unwrap();
		assert_eq!(h("CA"), h("CA   "));
		assert_eq!(h("CA"), hash_text("CA", 3));
		assert_eq!(h("   "), hash_text("", 3));
		assert_ne!(h("CA"), h(" CA"));
		assert_ne!(h("CA"), h("CA\t"));
		assert_eq!(KeyType::Bpchar.hash_binary_value(b"CA  ", 3).unwrap(), h("CA"));
		assert_eq!(KeyType::from_sql_name("character(3)"), Some(KeyType::Bpchar));
		assert_eq!(KeyType::from_sql_name("bpchar"), Some(KeyType::Bpchar));
		assert_eq!(KeyType::from_sql_name("numeric"), Some(KeyType::Numeric));
		assert_eq!(KeyType::from_sql_name("numeric(10,2)"), None);
		for t in KeyType::ALL {
			assert_eq!(KeyType::from_sql_name(t.sql_name()), Some(t));
		}
	}

	#[test]
	fn seed_zero_differs_from_seeded() {
		assert_ne!(hash_text("tenant-1", 0), hash_text("tenant-1", 1));
	}

	#[test]
	fn reads_text_forms() {
		let u = parse_uuid("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").unwrap();
		assert_eq!(u, parse_uuid("{a0eebc999c0b4ef8bb6d6bb9bd380a11}").unwrap());
		assert_eq!(
			u,
			parse_uuid("A0EE-BC99-9C0B-4EF8-BB6D-6BB9-BD38-0A11").unwrap()
		);
		assert!(parse_uuid("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1").is_err());

		assert_eq!(parse_date("2000-01-01").unwrap(), 0);
		assert_eq!(parse_date("1999-12-31").unwrap(), -1);
		assert_eq!(parse_date("2024-02-29").unwrap(), 8825);
		assert!(parse_date("2023-02-29").is_err());

		assert_eq!(
			parse_timestamp("2000-01-01 00:00:01", false).unwrap(),
			1_000_000
		);
		assert_eq!(
			parse_timestamp("2000-01-01 00:00:00.5", false).unwrap(),
			500_000
		);
		assert_eq!(
			parse_timestamp("2000-01-01 01:00:00+01", true).unwrap(),
			parse_timestamp("2000-01-01 00:00:00+00", true).unwrap()
		);
		assert_eq!(
			parse_timestamp("1999-12-31 23:30:00-00:30", true).unwrap(),
			0
		);
		assert!(parse_timestamp("2000-01-01 00:00:00", true).is_err());

		assert_eq!(
			parse_bytea("\\xdeadBEEF").unwrap(),
			vec![0xde, 0xad, 0xbe, 0xef]
		);
		assert_eq!(parse_int::<i64>(" +12 ", KeyType::Int8).unwrap(), 12);
	}
}
