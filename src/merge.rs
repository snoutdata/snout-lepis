//! Phase 2: combining several nodes' answers into the one a single Postgres would give.
//!
//! Everything here is pure: the router gathers each node's rows (scatter.rs says what each node
//! ran), and this file turns them into the client's rows. The rule it keeps is the one route.rs
//! keeps (L9): an answer is either exactly what one Postgres would have said, or an error saying
//! why not. So it never compares values by guesswork:
//!
//! - **Values compare by a TOKEN.** A few types have an order Lepis can compute exactly from the
//!   value alone (integers, numeric, floats, bool, uuid); every other type (text under any
//!   collation, dates, timestamps, intervals, anything a user defined) is ranked by the HOME NODE
//!   itself: the router sends it the distinct values and reads back their `dense_rank()` under the
//!   column's own collation (`needs_ranks`), so ties, case and accents fall exactly where the node
//!   puts them.
//! - **A k-way merge checks what it assumes.** Each node returns its rows sorted; if a node's rows
//!   are found out of the order the tokens give (a node with another default collation, say),
//!   the merge stops with an error instead of interleaving them wrongly.
//! - **Arithmetic is Postgres's.** count adds integers; sum adds exact decimals (`Dec`), and a
//!   `bigint` result that leaves its range is the error Postgres raises; avg is `numeric_div` of
//!   the summed partials, with Postgres's own result scale and rounding. Floating-point sums
//!   depend on the order values are added, so they are refused rather than approximated.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

// ---------------------------------------------------------------------------------------------
// Exact decimals: Postgres's numeric, as far as combining needs it.

/// An exact decimal: `mag` × 10^-`scale`, or one of numeric's special values.
#[derive(Clone, Debug)]
pub enum Dec {
	NaN,
	Inf {
		neg: bool,
	},
	Fin {
		neg: bool,
		/// Decimal digits, most significant first, with no leading zeros; empty is zero.
		mag: Vec<u8>,
		/// Digits after the decimal point (Postgres's display scale).
		scale: u32,
	},
}

/// Postgres's limits on a division's result scale (numeric.c).
const MIN_SIG_DIGITS: i64 = 16;
const MAX_DISPLAY_SCALE: i64 = 1000;
const MAX_RESULT_SCALE: i64 = 2000;

impl Dec {
	pub fn zero() -> Dec {
		Dec::Fin {
			neg: false,
			mag: Vec::new(),
			scale: 0,
		}
	}

	pub fn from_i128(v: i128) -> Dec {
		let mag = if v == 0 {
			Vec::new()
		} else {
			v.unsigned_abs()
				.to_string()
				.bytes()
				.map(|b| b - b'0')
				.collect()
		};
		Dec::Fin {
			neg: v < 0,
			mag,
			scale: 0,
		}
	}

	/// Postgres's text form of a numeric (no exponent), or of an integer.
	pub fn parse(s: &str) -> Option<Dec> {
		let s = s.trim();
		match s.to_ascii_lowercase().as_str() {
			"nan" => return Some(Dec::NaN),
			"infinity" | "+infinity" | "inf" | "+inf" => return Some(Dec::Inf { neg: false }),
			"-infinity" | "-inf" => return Some(Dec::Inf { neg: true }),
			_ => {}
		}
		let (neg, body) = match s.as_bytes().first()? {
			b'-' => (true, &s[1..]),
			b'+' => (false, &s[1..]),
			_ => (false, s),
		};
		let (int, frac) = match body.split_once('.') {
			Some((i, f)) => (i, f),
			None => (body, ""),
		};
		if int.is_empty() && frac.is_empty() {
			return None;
		}
		if !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()) {
			return None;
		}
		let digits: Vec<u8> = int.bytes().chain(frac.bytes()).map(|b| b - b'0').collect();
		Some(Dec::fin(neg, digits, frac.len() as u32))
	}

	fn fin(neg: bool, mut mag: Vec<u8>, scale: u32) -> Dec {
		let lead = mag.iter().take_while(|d| **d == 0).count();
		mag.drain(..lead);
		Dec::Fin {
			neg: neg && !mag.is_empty(),
			mag,
			scale,
		}
	}

	pub fn to_text(&self) -> String {
		match self {
			Dec::NaN => "NaN".into(),
			Dec::Inf { neg: false } => "Infinity".into(),
			Dec::Inf { neg: true } => "-Infinity".into(),
			Dec::Fin { neg, mag, scale } => {
				let scale = *scale as usize;
				let mut digits: String = mag.iter().map(|d| char::from(b'0' + d)).collect();
				if digits.len() <= scale {
					digits = "0".repeat(scale + 1 - digits.len()) + &digits;
				}
				let (int, frac) = digits.split_at(digits.len() - scale);
				let mut out = String::new();
				if *neg {
					out.push('-');
				}
				out.push_str(int);
				if scale > 0 {
					out.push('.');
					out.push_str(frac);
				}
				out
			}
		}
	}

	/// The magnitude with `to` digits after the point (`to` ≥ the scale).
	fn aligned(mag: &[u8], scale: u32, to: u32) -> Vec<u8> {
		if mag.is_empty() {
			return Vec::new();
		}
		let mut v = mag.to_vec();
		v.extend(std::iter::repeat_n(0, (to - scale) as usize));
		v
	}

	pub fn add(&self, other: &Dec) -> Dec {
		match (self, other) {
			(Dec::NaN, _) | (_, Dec::NaN) => Dec::NaN,
			(Dec::Inf { neg: a }, Dec::Inf { neg: b }) if a != b => Dec::NaN,
			(Dec::Inf { neg }, _) | (_, Dec::Inf { neg }) => Dec::Inf { neg: *neg },
			(
				Dec::Fin {
					neg: na,
					mag: ma,
					scale: sa,
				},
				Dec::Fin {
					neg: nb,
					mag: mb,
					scale: sb,
				},
			) => {
				let s = (*sa).max(*sb);
				let a = Dec::aligned(ma, *sa, s);
				let b = Dec::aligned(mb, *sb, s);
				if na == nb {
					return Dec::fin(*na, mag_add(&a, &b), s);
				}
				match mag_cmp(&a, &b) {
					Ordering::Equal => Dec::Fin {
						neg: false,
						mag: Vec::new(),
						scale: s,
					},
					Ordering::Greater => Dec::fin(*na, mag_sub(&a, &b), s),
					Ordering::Less => Dec::fin(*nb, mag_sub(&b, &a), s),
				}
			}
		}
	}

	/// numeric's order: -Infinity < every number < Infinity < NaN, and NaN equals NaN.
	pub fn order(&self, other: &Dec) -> Ordering {
		fn class(d: &Dec) -> i8 {
			match d {
				Dec::Inf { neg: true } => 0,
				Dec::Fin { .. } => 1,
				Dec::Inf { neg: false } => 2,
				Dec::NaN => 3,
			}
		}
		match (self, other) {
			(
				Dec::Fin {
					neg: na,
					mag: ma,
					scale: sa,
				},
				Dec::Fin {
					neg: nb,
					mag: mb,
					scale: sb,
				},
			) => {
				if na != nb {
					return if *na {
						Ordering::Less
					} else {
						Ordering::Greater
					};
				}
				let s = (*sa).max(*sb);
				let o = mag_cmp(&Dec::aligned(ma, *sa, s), &Dec::aligned(mb, *sb, s));
				if *na { o.reverse() } else { o }
			}
			_ => class(self).cmp(&class(other)),
		}
	}

	/// The weight and first digit of the value in base 10000, as numeric stores it (0, 0 for
	/// zero): what `select_div_scale` reads.
	fn nbase_lead(&self) -> (i64, u32) {
		let Dec::Fin { mag, scale, .. } = self else {
			return (0, 0);
		};
		if mag.is_empty() {
			return (0, 0);
		}
		let int_len = mag.len() as i64 - *scale as i64;
		if int_len > 0 {
			let first_len = match int_len % 4 {
				0 => 4,
				r => r,
			} as usize;
			let groups = (int_len + 3) / 4;
			(groups - 1, digits_value(&mag[..first_len]))
		} else {
			// A pure fraction: its first non-zero digit is at position p after the point.
			let lead_zeros = (-int_len) as usize;
			let mut frac: Vec<u8> = vec![0; lead_zeros];
			frac.extend_from_slice(mag);
			while !frac.len().is_multiple_of(4) {
				frac.push(0);
			}
			let g = lead_zeros / 4;
			(-(g as i64) - 1, digits_value(&frac[g * 4..g * 4 + 4]))
		}
	}

	/// Postgres's `numeric_div(self, n)` for a positive integer `n`: the result scale
	/// `select_div_scale` picks, and the exact quotient rounded half away from zero to it.
	pub fn div_count(&self, n: u64) -> Dec {
		let Dec::Fin { neg, mag, scale } = self else {
			return self.clone();
		};
		let (w1, f1) = self.nbase_lead();
		let (w2, f2) = Dec::from_i128(n as i128).nbase_lead();
		let mut qweight = w1 - w2;
		if f1 <= f2 {
			qweight -= 1;
		}
		let rscale = (MIN_SIG_DIGITS - qweight * 4)
			.max(*scale as i64)
			.clamp(0, MAX_DISPLAY_SCALE) as u32;
		let dividend = Dec::aligned(mag, *scale, rscale);
		let (mut q, r) = mag_div_small(&dividend, n);
		if r as u128 * 2 >= n as u128 {
			q = mag_add(&q, &[1]);
		}
		Dec::fin(*neg, q, rscale)
	}

	/// Postgres's `round(numeric, int)`: half away from zero, to `places` digits after the point
	/// (negative rounds to the left of it); the result shows `max(places, 0)` of them.
	pub fn round(&self, places: i64) -> Dec {
		let Dec::Fin { neg, mag, scale } = self else {
			return self.clone();
		};
		let places = places.clamp(-MAX_RESULT_SCALE, MAX_RESULT_SCALE);
		let scale = *scale as i64;
		if places >= scale {
			return Dec::fin(
				*neg,
				Dec::aligned(mag, scale as u32, places as u32),
				places as u32,
			);
		}
		// Drop k digits, rounding on the first one dropped.
		let k = (scale - places) as usize;
		let (kept, first_dropped) = if k > mag.len() {
			(Vec::new(), 0)
		} else {
			(mag[..mag.len() - k].to_vec(), mag[mag.len() - k])
		};
		let mut q = if kept.is_empty() { Vec::new() } else { kept };
		if first_dropped >= 5 {
			q = mag_add(&q, &[1]);
		}
		if places >= 0 {
			Dec::fin(*neg, q, places as u32)
		} else {
			let mut q = q;
			if !q.is_empty() {
				q.extend(std::iter::repeat_n(0, (-places) as usize));
			}
			Dec::fin(*neg, q, 0)
		}
	}

	/// numeric to bigint, as the cast does: rounded half away from zero, then range checked.
	pub fn to_i64(&self) -> Option<i64> {
		let Dec::Fin { neg, mag, .. } = self.round(0) else {
			return None;
		};
		if mag.len() > 19 {
			return None;
		}
		let v: i128 = mag.iter().fold(0i128, |a, d| a * 10 + *d as i128);
		let v = if neg { -v } else { v };
		i64::try_from(v).ok()
	}

	/// numeric's binary form (`numeric_send`).
	pub fn to_binary(&self) -> Vec<u8> {
		let header = |ndigits: i16, weight: i16, sign: u16, dscale: u16| {
			let mut b = Vec::new();
			b.extend_from_slice(&ndigits.to_be_bytes());
			b.extend_from_slice(&weight.to_be_bytes());
			b.extend_from_slice(&sign.to_be_bytes());
			b.extend_from_slice(&dscale.to_be_bytes());
			b
		};
		let (neg, mag, scale) = match self {
			Dec::NaN => return header(0, 0, 0xC000, 0),
			Dec::Inf { neg: false } => return header(0, 0, 0xD000, 0),
			Dec::Inf { neg: true } => return header(0, 0, 0xF000, 0),
			Dec::Fin { neg, mag, scale } => (*neg, mag, *scale as usize),
		};
		let int_len = mag.len().saturating_sub(scale);
		let mut int: Vec<u8> = mag[..int_len].to_vec();
		while !int.len().is_multiple_of(4) {
			int.insert(0, 0);
		}
		let mut frac: Vec<u8> = vec![0; scale.saturating_sub(mag.len())];
		frac.extend_from_slice(&mag[int_len..]);
		while !frac.len().is_multiple_of(4) {
			frac.push(0);
		}
		let mut groups: Vec<u32> = int
			.chunks(4)
			.chain(frac.chunks(4))
			.map(digits_value)
			.collect();
		let mut weight = (int.len() / 4) as i64 - 1;
		while groups.first() == Some(&0) {
			groups.remove(0);
			weight -= 1;
		}
		while groups.last() == Some(&0) {
			groups.pop();
		}
		if groups.is_empty() {
			return header(0, 0, 0, scale as u16);
		}
		let mut b = header(
			groups.len() as i16,
			weight as i16,
			if neg { 0x4000 } else { 0 },
			scale as u16,
		);
		for g in groups {
			b.extend_from_slice(&(g as i16).to_be_bytes());
		}
		b
	}

	/// numeric's binary form read back (`numeric_recv`).
	pub fn from_binary(b: &[u8]) -> Option<Dec> {
		let word = |i: usize| b.get(i..i + 2).map(|w| u16::from_be_bytes([w[0], w[1]]));
		let ndigits = word(0)? as usize;
		let weight = word(2)? as i16 as i64;
		let sign = word(4)?;
		let dscale = word(6)? as usize;
		match sign {
			0xC000 => return Some(Dec::NaN),
			0xD000 => return Some(Dec::Inf { neg: false }),
			0xF000 => return Some(Dec::Inf { neg: true }),
			0 | 0x4000 => {}
			_ => return None,
		}
		// Every group from weight down to the scale's last one, as four decimal digits.
		let mut int = String::new();
		let mut frac = String::new();
		let lowest = (-(dscale as i64 + 3) / 4).min(weight - ndigits as i64 + 1);
		for w in (lowest..=weight.max(0)).rev() {
			let i = weight - w;
			let g = if i >= 0 && (i as usize) < ndigits {
				word(8 + 2 * i as usize)?
			} else {
				0
			};
			if g > 9999 {
				return None;
			}
			if w >= 0 {
				int.push_str(&format!("{g:04}"));
			} else {
				frac.push_str(&format!("{g:04}"));
			}
		}
		frac.truncate(dscale);
		while frac.len() < dscale {
			frac.push('0');
		}
		let text = format!(
			"{}{}{}{}",
			if sign == 0x4000 { "-" } else { "" },
			if int.is_empty() { "0" } else { &int },
			if dscale > 0 { "." } else { "" },
			frac
		);
		Dec::parse(&text)
	}
}

fn digits_value(d: &[u8]) -> u32 {
	d.iter().fold(0u32, |a, x| a * 10 + *x as u32)
}

fn mag_cmp(a: &[u8], b: &[u8]) -> Ordering {
	a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}

fn mag_add(a: &[u8], b: &[u8]) -> Vec<u8> {
	let mut out = Vec::with_capacity(a.len().max(b.len()) + 1);
	let (mut i, mut j, mut carry) = (a.len(), b.len(), 0u8);
	while i > 0 || j > 0 || carry > 0 {
		let mut s = carry;
		if i > 0 {
			i -= 1;
			s += a[i];
		}
		if j > 0 {
			j -= 1;
			s += b[j];
		}
		out.push(s % 10);
		carry = s / 10;
	}
	out.reverse();
	let lead = out.iter().take_while(|d| **d == 0).count();
	out.drain(..lead);
	out
}

/// a - b for a ≥ b.
fn mag_sub(a: &[u8], b: &[u8]) -> Vec<u8> {
	let mut out = Vec::with_capacity(a.len());
	let (mut i, mut j, mut borrow) = (a.len(), b.len(), 0i8);
	while i > 0 {
		i -= 1;
		let mut d = a[i] as i8 - borrow;
		if j > 0 {
			j -= 1;
			d -= b[j] as i8;
		}
		if d < 0 {
			d += 10;
			borrow = 1;
		} else {
			borrow = 0;
		}
		out.push(d as u8);
	}
	out.reverse();
	let lead = out.iter().take_while(|d| **d == 0).count();
	out.drain(..lead);
	out
}

fn mag_div_small(a: &[u8], n: u64) -> (Vec<u8>, u64) {
	let mut q = Vec::with_capacity(a.len());
	let mut r: u128 = 0;
	for d in a {
		r = r * 10 + *d as u128;
		q.push((r / n as u128) as u8);
		r %= n as u128;
	}
	let lead = q.iter().take_while(|d| **d == 0).count();
	q.drain(..lead);
	(q, r as u64)
}

// ---------------------------------------------------------------------------------------------
// Types and tokens.

pub const BOOL: u32 = 16;
pub const INT8: u32 = 20;
pub const INT2: u32 = 21;
pub const INT4: u32 = 23;
pub const TEXT: u32 = 25;
pub const OID: u32 = 26;
pub const FLOAT4: u32 = 700;
pub const FLOAT8: u32 = 701;
pub const NUMERIC: u32 = 1700;
pub const UUID: u32 = 2950;

/// The types whose order Lepis computes from the value alone. Every other type is ranked by a
/// node.
pub fn is_native(oid: u32) -> bool {
	matches!(
		oid,
		BOOL | INT8 | INT2 | INT4 | OID | FLOAT4 | FLOAT8 | NUMERIC | UUID
	)
}

pub fn type_name(oid: u32) -> String {
	match oid {
		BOOL => "boolean".into(),
		INT8 => "bigint".into(),
		INT2 => "smallint".into(),
		INT4 => "integer".into(),
		TEXT => "text".into(),
		FLOAT4 => "real".into(),
		FLOAT8 => "double precision".into(),
		NUMERIC => "numeric".into(),
		790 => "money".into(),
		1186 => "interval".into(),
		other => format!("type {other}"),
	}
}

/// What two values are compared by. Within one column every token has the same variant.
#[derive(Clone, Debug)]
pub enum Token {
	Int(i128),
	Float(f64),
	Num(Dec),
	Bool(bool),
	Bytes(Vec<u8>),
	/// The value's place in the home node's own order of the column's values.
	Rank(i64),
}

impl Token {
	fn class(&self) -> u8 {
		match self {
			Token::Int(_) | Token::Num(_) => 0,
			Token::Float(_) => 1,
			Token::Bool(_) => 2,
			Token::Bytes(_) => 3,
			Token::Rank(_) => 4,
		}
	}
}

/// float8's order: NaN above everything (and equal to NaN), -0 equal to 0.
fn float_cmp(a: f64, b: f64) -> Ordering {
	match (a.is_nan(), b.is_nan()) {
		(true, true) => Ordering::Equal,
		(true, false) => Ordering::Greater,
		(false, true) => Ordering::Less,
		_ => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
	}
}

impl Ord for Token {
	fn cmp(&self, other: &Self) -> Ordering {
		match (self, other) {
			(Token::Int(a), Token::Int(b)) => a.cmp(b),
			(Token::Int(a), Token::Num(b)) => Dec::from_i128(*a).order(b),
			(Token::Num(a), Token::Int(b)) => a.order(&Dec::from_i128(*b)),
			(Token::Num(a), Token::Num(b)) => a.order(b),
			(Token::Float(a), Token::Float(b)) => float_cmp(*a, *b),
			(Token::Bool(a), Token::Bool(b)) => a.cmp(b),
			(Token::Bytes(a), Token::Bytes(b)) => a.cmp(b),
			(Token::Rank(a), Token::Rank(b)) => a.cmp(b),
			_ => self.class().cmp(&other.class()),
		}
	}
}

impl PartialOrd for Token {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl PartialEq for Token {
	fn eq(&self, other: &Self) -> bool {
		self.cmp(other) == Ordering::Equal
	}
}

impl Eq for Token {}

/// An error to give the client instead of an answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeError {
	pub code: &'static str,
	pub message: String,
	pub hint: Option<String>,
}

fn err(code: &'static str, message: impl Into<String>) -> MergeError {
	MergeError {
		code,
		message: message.into(),
		hint: None,
	}
}

fn refusal(message: impl Into<String>, hint: impl Into<String>) -> MergeError {
	MergeError {
		code: crate::route::NOT_ACROSS_NODES,
		message: message.into(),
		hint: Some(hint.into()),
	}
}

fn malformed(oid: u32) -> MergeError {
	err(
		"XX000",
		format!(
			"a node sent a {} value Lepis could not read",
			type_name(oid)
		),
	)
}

fn utf8(b: &[u8], oid: u32) -> Result<&str, MergeError> {
	std::str::from_utf8(b).map_err(|_| malformed(oid))
}

/// The token of a value of a native type, in the format it was sent in.
fn native_token(b: &[u8], oid: u32, format: i16) -> Result<Token, MergeError> {
	let bad = || malformed(oid);
	if format == 1 {
		return match oid {
			BOOL => Ok(Token::Bool(*b.first().ok_or_else(bad)? != 0)),
			INT2 => Ok(Token::Int(
				i16::from_be_bytes(b.try_into().map_err(|_| bad())?) as i128,
			)),
			INT4 => Ok(Token::Int(
				i32::from_be_bytes(b.try_into().map_err(|_| bad())?) as i128,
			)),
			OID => Ok(Token::Int(
				u32::from_be_bytes(b.try_into().map_err(|_| bad())?) as i128,
			)),
			INT8 => Ok(Token::Int(
				i64::from_be_bytes(b.try_into().map_err(|_| bad())?) as i128,
			)),
			FLOAT4 => Ok(Token::Float(
				f32::from_be_bytes(b.try_into().map_err(|_| bad())?) as f64,
			)),
			FLOAT8 => Ok(Token::Float(f64::from_be_bytes(
				b.try_into().map_err(|_| bad())?,
			))),
			NUMERIC => Dec::from_binary(b).map(Token::Num).ok_or_else(bad),
			UUID if b.len() == 16 => Ok(Token::Bytes(b.to_vec())),
			_ => Err(bad()),
		};
	}
	let s = utf8(b, oid)?;
	match oid {
		BOOL => match s {
			"t" => Ok(Token::Bool(true)),
			"f" => Ok(Token::Bool(false)),
			_ => Err(bad()),
		},
		INT2 | INT4 | INT8 | OID => s.parse::<i128>().map(Token::Int).map_err(|_| bad()),
		FLOAT4 | FLOAT8 => parse_float(s).map(Token::Float).ok_or_else(bad),
		NUMERIC => Dec::parse(s).map(Token::Num).ok_or_else(bad),
		UUID => {
			let hex: String = s.chars().filter(|c| *c != '-').collect();
			if hex.len() != 32 {
				return Err(bad());
			}
			(0..16)
				.map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16))
				.collect::<Result<Vec<u8>, _>>()
				.map(Token::Bytes)
				.map_err(|_| bad())
		}
		_ => Err(bad()),
	}
}

fn parse_float(s: &str) -> Option<f64> {
	match s {
		"NaN" => Some(f64::NAN),
		"Infinity" => Some(f64::INFINITY),
		"-Infinity" => Some(f64::NEG_INFINITY),
		_ => s.parse().ok(),
	}
}

fn int_of(b: &[u8], oid: u32, format: i16) -> Result<i128, MergeError> {
	match native_token(b, oid, format)? {
		Token::Int(v) => Ok(v),
		_ => Err(malformed(oid)),
	}
}

fn dec_of(b: &[u8], oid: u32, format: i16) -> Result<Dec, MergeError> {
	match native_token(b, oid, format)? {
		Token::Int(v) => Ok(Dec::from_i128(v)),
		Token::Num(d) => Ok(d),
		_ => Err(malformed(oid)),
	}
}

// ---------------------------------------------------------------------------------------------
// The plan scatter.rs makes.

pub type Cell = Option<Vec<u8>>;
pub type Row = Vec<Cell>;

/// How the nodes' rows become the client's.
#[derive(Clone, Debug, PartialEq)]
pub struct MergePlan {
	/// The client's columns are the first `visible` of the worker's (for `Rows`), or `outputs`.
	pub visible: usize,
	/// Per worker column, the worker column holding `pg_collation_for` of it, if any.
	pub collation_of: Vec<Option<usize>>,
	pub kind: MergeKind,
	pub distinct: bool,
	pub order: Vec<SortKey>,
	pub offset: u64,
	pub limit: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MergeKind {
	/// Rows pass through; with `sorted`, each node's rows are in `order` and are merged.
	Rows { sorted: bool },
	/// Partial aggregates per node, finished here.
	Groups(Groups),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Groups {
	/// The worker columns holding the GROUP BY values.
	pub keys: Vec<usize>,
	pub aggs: Vec<Agg>,
	/// One per client column.
	pub outputs: Vec<Expr>,
	pub having: Option<Expr>,
	/// No GROUP BY: exactly one group, even over no rows.
	pub one_group: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Agg {
	pub kind: AggKind,
	/// The partial's worker column (the argument's, for the DISTINCT forms).
	pub col: usize,
	/// avg: the column of its partial count.
	pub count_col: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggKind {
	Count,
	Sum,
	Min,
	Max,
	Avg,
	BoolAnd,
	BoolOr,
	CountDistinct,
	SumDistinct,
	AvgDistinct,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpOp {
	Eq,
	Ne,
	Lt,
	Le,
	Gt,
	Ge,
}

/// What the router computes after aggregation: HAVING, and expressions over aggregates.
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
	/// A worker column's value for the group (a key, or something the keys determine).
	Col(usize),
	Agg(usize),
	Int(i128),
	Num(String),
	Null,
	Cmp(CmpOp, Box<Expr>, Box<Expr>),
	And(Vec<Expr>),
	Or(Vec<Expr>),
	Not(Box<Expr>),
	IsNull(Box<Expr>, bool),
	Round(Box<Expr>, Option<Box<Expr>>),
	/// `::numeric` (no type modifier).
	Numeric(Box<Expr>),
	/// `::bigint`.
	Int8(Box<Expr>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct SortKey {
	pub expr: Expr,
	pub desc: bool,
	pub nulls_first: bool,
}

/// What every node returned: the worker's column types and formats, and each node's rows in
/// node order.
pub struct Gathered<'a> {
	pub types: &'a [u32],
	pub formats: &'a [i16],
	pub nodes: &'a [Vec<Row>],
}

/// The values of one column a node must rank (`dense_rank()` under `collation`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RankRequest {
	pub col: usize,
	pub oid: u32,
	pub format: i16,
	pub collation: Option<String>,
	pub values: Vec<Vec<u8>>,
}

pub type Ranks = HashMap<usize, HashMap<Vec<u8>, i64>>;

impl MergePlan {
	/// The worker columns whose values are compared.
	fn compared(&self) -> BTreeSet<usize> {
		let mut out = BTreeSet::new();
		fn cols(e: &Expr, out: &mut BTreeSet<usize>) {
			match e {
				Expr::Col(c) => {
					out.insert(*c);
				}
				Expr::Cmp(_, a, b) => {
					cols(a, out);
					cols(b, out);
				}
				Expr::And(v) | Expr::Or(v) => v.iter().for_each(|e| cols(e, out)),
				Expr::Not(a) | Expr::IsNull(a, _) | Expr::Numeric(a) | Expr::Int8(a) => {
					cols(a, out)
				}
				Expr::Round(a, b) => {
					cols(a, out);
					if let Some(b) = b {
						cols(b, out);
					}
				}
				_ => {}
			}
		}
		for k in &self.order {
			cols(&k.expr, &mut out);
		}
		match &self.kind {
			MergeKind::Rows { .. } => {
				if self.distinct {
					out.extend(0..self.visible);
				}
			}
			MergeKind::Groups(g) => {
				out.extend(g.keys.iter().copied());
				for a in &g.aggs {
					if matches!(
						a.kind,
						AggKind::Min
							| AggKind::Max | AggKind::CountDistinct
							| AggKind::SumDistinct
							| AggKind::AvgDistinct
					) {
						out.insert(a.col);
					}
				}
				if let Some(h) = &g.having {
					cols(h, &mut out);
				}
				if self.distinct {
					for e in &g.outputs {
						cols(e, &mut out);
					}
				}
			}
		}
		out
	}

	/// The values a node must rank before `combine` can run: every compared column of a type
	/// Lepis does not order itself.
	pub fn needs_ranks(&self, g: &Gathered) -> Result<Vec<RankRequest>, MergeError> {
		self.ranks_for(g.types, g.formats, g.nodes.iter().flatten())
	}

	/// `needs_ranks` over any rows.
	fn ranks_for<'r>(
		&self,
		types: &[u32],
		formats: &[i16],
		rows: impl Iterator<Item = &'r Row> + Clone,
	) -> Result<Vec<RankRequest>, MergeError> {
		let mut out = Vec::new();
		for col in self.compared() {
			let Some(&oid) = types.get(col) else {
				return Err(err(
					"XX000",
					"the nodes returned fewer columns than Lepis asked for",
				));
			};
			if is_native(oid) {
				continue;
			}
			let mut values: BTreeSet<&[u8]> = BTreeSet::new();
			let mut collation: Option<Option<String>> = None;
			for r in rows.clone() {
				{
					let Some(Some(v)) = r.get(col) else { continue };
					values.insert(v);
					let c = match self.collation_of.get(col).copied().flatten() {
						Some(cc) => match r.get(cc) {
							Some(Some(b)) => Some(utf8(b, TEXT)?.to_string()),
							_ => None,
						},
						None => None,
					};
					match &collation {
						None => collation = Some(c),
						Some(prev) if *prev != c => {
							return Err(refusal(
								"the rows of one column carry different collations, so Lepis cannot order them as one",
								"Give the expression one collation with COLLATE.",
							));
						}
						_ => {}
					}
				}
			}
			if values.is_empty() {
				continue;
			}
			let collation = collation.flatten();
			if let Some(c) = &collation
				&& !c
					.bytes()
					.all(|b| b.is_ascii_alphanumeric() || b"_-.\"@ ".contains(&b))
			{
				return Err(refusal(
					format!("Lepis cannot order values under the collation {c}"),
					"Use a collation whose name has only letters, digits, '_', '-', '.' and '@'.",
				));
			}
			out.push(RankRequest {
				col,
				oid,
				format: formats.get(col).copied().unwrap_or(0),
				collation,
				values: values.into_iter().map(<[u8]>::to_vec).collect(),
			});
		}
		Ok(out)
	}

	/// The client's rows, each column in the format `out` names for it (type oid, format).
	pub fn combine(
		&self,
		g: &Gathered,
		ranks: &Ranks,
		out: &[(u32, i16)],
	) -> Result<Vec<Row>, MergeError> {
		let tok = Tokens { g, ranks };
		match &self.kind {
			MergeKind::Rows { sorted } => self.rows(&tok, *sorted),
			MergeKind::Groups(groups) => self.groups(&tok, groups, out),
		}
	}

	fn rows(&self, tok: &Tokens, sorted: bool) -> Result<Vec<Row>, MergeError> {
		let g = tok.g;
		let key_cols: Vec<usize> = self
			.order
			.iter()
			.map(|k| match k.expr {
				Expr::Col(c) => Ok(c),
				_ => Err(err("XX000", "a sort key over rows must be a column")),
			})
			.collect::<Result<_, _>>()?;
		let keys_of = |r: &Row| -> Result<Vec<Option<Token>>, MergeError> {
			key_cols.iter().map(|c| tok.cell(r, *c)).collect()
		};
		let mut merged: Vec<&Row> = Vec::new();
		if sorted && !key_cols.is_empty() {
			// The k-way merge: the least head among the nodes, ties to the earlier node.
			let mut keys: Vec<Vec<Vec<Option<Token>>>> = Vec::with_capacity(g.nodes.len());
			for rows in g.nodes {
				keys.push(rows.iter().map(&keys_of).collect::<Result<_, _>>()?);
			}
			let mut at = vec![0usize; g.nodes.len()];
			loop {
				let mut best: Option<usize> = None;
				for (n, rows) in g.nodes.iter().enumerate() {
					if at[n] >= rows.len() {
						continue;
					}
					best = match best {
						Some(b)
							if self.cmp_keys(&keys[b][at[b]], &keys[n][at[n]])
								!= Ordering::Greater =>
						{
							Some(b)
						}
						_ => Some(n),
					};
				}
				let Some(n) = best else { break };
				merged.push(&g.nodes[n][at[n]]);
				at[n] += 1;
				if at[n] < g.nodes[n].len()
					&& self.cmp_keys(&keys[n][at[n] - 1], &keys[n][at[n]]) == Ordering::Greater
				{
					return Err(err(
						"XX000",
						"a node returned its rows out of the order Lepis merges them in (do the nodes have the same collation and settings?)",
					));
				}
			}
		} else {
			merged = g.nodes.iter().flatten().collect();
		}
		let mut seen: BTreeSet<Vec<Option<Token>>> = BTreeSet::new();
		let mut out = Vec::new();
		let mut skipped = 0u64;
		for r in merged {
			if self.distinct {
				let k: Vec<Option<Token>> = (0..self.visible)
					.map(|c| tok.cell(r, c))
					.collect::<Result<_, _>>()?;
				if !seen.insert(k) {
					continue;
				}
			}
			if skipped < self.offset {
				skipped += 1;
				continue;
			}
			if self.limit.is_some_and(|l| out.len() as u64 >= l) {
				break;
			}
			out.push(r[..self.visible.min(r.len())].to_vec());
		}
		Ok(out)
	}

	fn cmp_keys(&self, a: &[Option<Token>], b: &[Option<Token>]) -> Ordering {
		for (i, k) in self.order.iter().enumerate() {
			let o = match (&a[i], &b[i]) {
				(None, None) => Ordering::Equal,
				(None, Some(_)) => {
					if k.nulls_first {
						Ordering::Less
					} else {
						Ordering::Greater
					}
				}
				(Some(_), None) => {
					if k.nulls_first {
						Ordering::Greater
					} else {
						Ordering::Less
					}
				}
				(Some(x), Some(y)) => {
					if k.desc {
						y.cmp(x)
					} else {
						x.cmp(y)
					}
				}
			};
			if o != Ordering::Equal {
				return o;
			}
		}
		Ordering::Equal
	}

	fn groups(
		&self,
		tok: &Tokens,
		plan: &Groups,
		out: &[(u32, i16)],
	) -> Result<Vec<Row>, MergeError> {
		let g = tok.g;
		let mut index: BTreeMap<Vec<Option<Token>>, usize> = BTreeMap::new();
		let mut groups: Vec<Group> = Vec::new();
		if plan.one_group {
			index.insert(Vec::new(), 0);
			groups.push(Group::new(None, plan));
		}
		for r in g.nodes.iter().flatten() {
			let key: Vec<Option<Token>> = plan
				.keys
				.iter()
				.map(|c| tok.cell(r, *c))
				.collect::<Result<_, _>>()?;
			let i = match index.get(&key) {
				Some(i) => *i,
				None => {
					index.insert(key, groups.len());
					groups.push(Group::new(Some(r), plan));
					groups.len() - 1
				}
			};
			let grp = &mut groups[i];
			if grp.rep.is_none() {
				grp.rep = Some(r);
			}
			for (a, acc) in plan.aggs.iter().zip(grp.accs.iter_mut()) {
				acc.add(a, r, tok)?;
			}
		}

		struct Done {
			outputs: Vec<Val>,
			keys: Vec<Option<Token>>,
		}
		let mut done: Vec<Done> = Vec::new();
		let mut seen: BTreeSet<Vec<Option<Token>>> = BTreeSet::new();
		for grp in &groups {
			let finals: Vec<Val> = plan
				.aggs
				.iter()
				.zip(&grp.accs)
				.map(|(a, acc)| acc.finish(a))
				.collect::<Result<_, _>>()?;
			let ev = Eval {
				rep: grp.rep,
				finals: &finals,
				tok,
			};
			if let Some(h) = &plan.having
				&& !matches!(ev.eval(h)?, Val::Bool(true))
			{
				continue;
			}
			let outputs: Vec<Val> = plan
				.outputs
				.iter()
				.map(|e| ev.eval(e))
				.collect::<Result<_, _>>()?;
			if self.distinct {
				let k: Vec<Option<Token>> =
					outputs.iter().map(Val::token).collect::<Result<_, _>>()?;
				if !seen.insert(k) {
					continue;
				}
			}
			let keys: Vec<Option<Token>> = self
				.order
				.iter()
				.map(|k| ev.eval(&k.expr).and_then(|v| v.token()))
				.collect::<Result<_, _>>()?;
			done.push(Done { outputs, keys });
		}
		if !self.order.is_empty() {
			done.sort_by(|a, b| self.cmp_keys(&a.keys, &b.keys));
		}
		let mut rows = Vec::new();
		for d in done
			.into_iter()
			.skip(self.offset.min(usize::MAX as u64) as usize)
		{
			if self.limit.is_some_and(|l| rows.len() as u64 >= l) {
				break;
			}
			let mut row = Vec::with_capacity(d.outputs.len());
			for (i, v) in d.outputs.into_iter().enumerate() {
				let (oid, format) = out.get(i).copied().unwrap_or((TEXT, 0));
				row.push(v.encode(oid, format)?);
			}
			rows.push(row);
		}
		Ok(rows)
	}
}

// ---------------------------------------------------------------------------------------------
// Streaming: rows merged as they arrive, a window at a time.

/// What a streaming merge needs next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
	/// Node `n` (by position) has no row buffered and has not finished: fetch its next window.
	Need(usize),
	/// Every row the client gets has been emitted (all nodes finished, or LIMIT reached).
	Done,
}

/// A merge of plain rows (`MergeKind::Rows`) that never holds more than the windows the router
/// fetched: each node's rows arrive a window at a time, and a row is emitted as soon as it is the
/// least of every node's head. Values only the home node can order are ranked again for every
/// window (`needs_ranks` covers what is buffered, plus each node's last row for the order check),
/// so a rank never outlives the window it was asked for.
pub struct RowMerge<'a> {
	plan: &'a MergePlan,
	types: Vec<u32>,
	formats: Vec<i16>,
	key_cols: Vec<usize>,
	sorted: bool,
	last: Vec<Option<Row>>,
	seen: BTreeSet<Vec<Option<Token>>>,
	skipped: u64,
	emitted: u64,
}

impl MergePlan {
	/// Whether the rows can be merged as they arrive: plain rows, sort keys that are columns,
	/// and a DISTINCT only over values Lepis orders itself (a rank is good for one window only).
	pub fn streams(&self, types: &[u32]) -> bool {
		let MergeKind::Rows { .. } = self.kind else {
			return false;
		};
		self.order.iter().all(|k| matches!(k.expr, Expr::Col(_)))
			&& (!self.distinct
				|| (0..self.visible).all(|c| types.get(c).is_some_and(|t| is_native(*t))))
	}

	pub fn row_merge(
		&self,
		nodes: usize,
		types: &[u32],
		formats: &[i16],
	) -> Result<RowMerge<'_>, MergeError> {
		let MergeKind::Rows { sorted } = self.kind else {
			return Err(err("XX000", "only plain rows are merged as they arrive"));
		};
		let key_cols = self
			.order
			.iter()
			.map(|k| match k.expr {
				Expr::Col(c) => Ok(c),
				_ => Err(err("XX000", "a sort key over rows must be a column")),
			})
			.collect::<Result<_, _>>()?;
		Ok(RowMerge {
			plan: self,
			types: types.to_vec(),
			formats: formats.to_vec(),
			key_cols,
			sorted,
			last: vec![None; nodes],
			seen: BTreeSet::new(),
			skipped: 0,
			emitted: 0,
		})
	}
}

impl RowMerge<'_> {
	/// LIMIT is reached: nothing more is wanted from any node.
	pub fn full(&self) -> bool {
		self.plan.limit.is_some_and(|l| self.emitted >= l)
	}

	/// The values the home node must rank before `step` can compare what is buffered.
	pub fn needs_ranks(&self, bufs: &[VecDeque<Row>]) -> Result<Vec<RankRequest>, MergeError> {
		let rows: Vec<&Row> = bufs
			.iter()
			.flatten()
			.chain(self.last.iter().flatten())
			.collect();
		self.plan
			.ranks_for(&self.types, &self.formats, rows.iter().copied())
	}

	/// Emits into `out` every row that can be emitted now.
	pub fn step(
		&mut self,
		bufs: &mut [VecDeque<Row>],
		finished: &[bool],
		ranks: &Ranks,
		out: &mut Vec<Row>,
	) -> Result<Step, MergeError> {
		let g = Gathered {
			types: &self.types,
			formats: &self.formats,
			nodes: &[],
		};
		let tok = Tokens { g: &g, ranks };
		let keys_of = |r: &Row| -> Result<Vec<Option<Token>>, MergeError> {
			self.key_cols.iter().map(|c| tok.cell(r, *c)).collect()
		};
		let merging = self.sorted && !self.key_cols.is_empty();
		loop {
			if self.full() {
				return Ok(Step::Done);
			}
			let n = if merging {
				if let Some(n) = (0..bufs.len()).find(|n| bufs[*n].is_empty() && !finished[*n]) {
					return Ok(Step::Need(n));
				}
				let mut best: Option<(usize, Vec<Option<Token>>)> = None;
				for (n, b) in bufs.iter().enumerate() {
					let Some(head) = b.front() else { continue };
					let k = keys_of(head)?;
					best = match best {
						Some((bn, bk)) if self.plan.cmp_keys(&bk, &k) != Ordering::Greater => {
							Some((bn, bk))
						}
						_ => Some((n, k)),
					};
				}
				match best {
					Some((n, _)) => n,
					None => return Ok(Step::Done),
				}
			} else {
				match (0..bufs.len()).find(|n| !bufs[*n].is_empty()) {
					Some(n) => n,
					None => {
						return Ok(match (0..bufs.len()).find(|n| !finished[*n]) {
							Some(n) => Step::Need(n),
							None => Step::Done,
						});
					}
				}
			};
			let r = bufs[n].pop_front().expect("a head");
			if merging
				&& let Some(prev) = &self.last[n]
				&& self.plan.cmp_keys(&keys_of(prev)?, &keys_of(&r)?) == Ordering::Greater
			{
				return Err(err(
					"XX000",
					"a node returned its rows out of the order Lepis merges them in (do the nodes have the same collation and settings?)",
				));
			}
			if self.plan.distinct {
				let k: Vec<Option<Token>> = (0..self.plan.visible)
					.map(|c| tok.cell(&r, c))
					.collect::<Result<_, _>>()?;
				if !self.seen.insert(k) {
					if merging {
						self.last[n] = Some(r);
					}
					continue;
				}
			}
			let visible = r[..self.plan.visible.min(r.len())].to_vec();
			if merging {
				self.last[n] = Some(r);
			}
			if self.skipped < self.plan.offset {
				self.skipped += 1;
				continue;
			}
			self.emitted += 1;
			out.push(visible);
		}
	}
}

/// Tokens of cells, from the value itself or from the node's ranks.
struct Tokens<'a> {
	g: &'a Gathered<'a>,
	ranks: &'a Ranks,
}

impl Tokens<'_> {
	fn cell(&self, r: &Row, col: usize) -> Result<Option<Token>, MergeError> {
		match r.get(col) {
			Some(Some(b)) => self.value(b, col).map(Some),
			Some(None) => Ok(None),
			None => Err(err("XX000", "a node returned a short row")),
		}
	}

	fn value(&self, b: &[u8], col: usize) -> Result<Token, MergeError> {
		let oid = self.g.types.get(col).copied().unwrap_or(TEXT);
		if is_native(oid) {
			let format = self.g.formats.get(col).copied().unwrap_or(0);
			return native_token(b, oid, format);
		}
		self.ranks
			.get(&col)
			.and_then(|m| m.get(b))
			.map(|r| Token::Rank(*r))
			.ok_or_else(|| err("XX000", "a value was not ranked"))
	}

	fn oid(&self, col: usize) -> u32 {
		self.g.types.get(col).copied().unwrap_or(TEXT)
	}

	fn format(&self, col: usize) -> i16 {
		self.g.formats.get(col).copied().unwrap_or(0)
	}
}

/// A value after aggregation.
#[derive(Clone, Debug)]
enum Val {
	Null,
	Int(i128),
	Num(Dec),
	Bool(bool),
	/// A node's value as it was sent, with its token when the column is compared.
	Raw(Vec<u8>, Option<Token>),
}

impl Val {
	fn token(&self) -> Result<Option<Token>, MergeError> {
		Ok(match self {
			Val::Null => None,
			Val::Int(v) => Some(Token::Int(*v)),
			Val::Num(d) => Some(Token::Num(d.clone())),
			Val::Bool(b) => Some(Token::Bool(*b)),
			Val::Raw(_, Some(t)) => Some(t.clone()),
			Val::Raw(_, None) => return Err(err("XX000", "a value Lepis compares had no token")),
		})
	}

	fn dec(&self) -> Option<Dec> {
		match self {
			Val::Int(v) => Some(Dec::from_i128(*v)),
			Val::Num(d) => Some(d.clone()),
			Val::Raw(_, Some(Token::Int(v))) => Some(Dec::from_i128(*v)),
			Val::Raw(_, Some(Token::Num(d))) => Some(d.clone()),
			_ => None,
		}
	}

	fn encode(self, oid: u32, format: i16) -> Result<Cell, MergeError> {
		let range = || err("22003", format!("{} out of range", type_name(oid)));
		let wrong = || {
			err(
				"XX000",
				format!(
					"Lepis computed a value that does not fit the column's type ({})",
					type_name(oid)
				),
			)
		};
		Ok(Some(match self {
			Val::Null => return Ok(None),
			Val::Raw(b, _) => b,
			Val::Bool(v) => match (oid, format) {
				(BOOL, 1) => vec![v as u8],
				(BOOL, _) => {
					if v {
						b"t".to_vec()
					} else {
						b"f".to_vec()
					}
				}
				_ => return Err(wrong()),
			},
			Val::Int(v) => match oid {
				INT8 | INT4 | INT2 => {
					let v = match oid {
						INT8 => i64::try_from(v).map_err(|_| range())?,
						INT4 => i32::try_from(v).map_err(|_| range())? as i64,
						_ => i16::try_from(v).map_err(|_| range())? as i64,
					};
					if format == 1 {
						match oid {
							INT8 => v.to_be_bytes().to_vec(),
							INT4 => (v as i32).to_be_bytes().to_vec(),
							_ => (v as i16).to_be_bytes().to_vec(),
						}
					} else {
						v.to_string().into_bytes()
					}
				}
				NUMERIC => {
					let d = Dec::from_i128(v);
					if format == 1 {
						d.to_binary()
					} else {
						d.to_text().into_bytes()
					}
				}
				_ => return Err(wrong()),
			},
			Val::Num(d) => match oid {
				NUMERIC if format == 1 => d.to_binary(),
				NUMERIC => d.to_text().into_bytes(),
				_ => return Err(wrong()),
			},
		}))
	}
}

/// One group's state.
struct Group<'r> {
	rep: Option<&'r Row>,
	accs: Vec<Acc>,
}

impl<'r> Group<'r> {
	fn new(rep: Option<&'r Row>, plan: &Groups) -> Group<'r> {
		Group {
			rep,
			accs: plan.aggs.iter().map(|_| Acc::default()).collect(),
		}
	}
}

#[derive(Default)]
struct Acc {
	count: i128,
	int: Option<i128>,
	num: Option<Dec>,
	best: Option<(Token, Vec<u8>)>,
	bool: Option<bool>,
	distinct: BTreeMap<Token, Dec>,
	/// The sum came as bigint partials (from smallint / integer arguments).
	ints: bool,
}

impl Acc {
	fn add(&mut self, a: &Agg, r: &Row, tok: &Tokens) -> Result<(), MergeError> {
		let cell = r
			.get(a.col)
			.ok_or_else(|| err("XX000", "a node returned a short row"))?;
		let (oid, format) = (tok.oid(a.col), tok.format(a.col));
		match a.kind {
			AggKind::Count => {
				if let Some(b) = cell {
					self.count += int_of(b, oid, format)?;
				}
			}
			AggKind::Sum | AggKind::Avg => {
				if a.kind == AggKind::Avg
					&& let Some(c) = a.count_col
					&& let Some(Some(b)) = r.get(c)
				{
					self.count += int_of(b, tok.oid(c), tok.format(c))?;
				}
				match oid {
					INT8 => {
						self.ints = true;
						if let Some(b) = cell {
							let v = int_of(b, oid, format)?;
							self.int = Some(self.int.unwrap_or(0) + v);
						}
					}
					NUMERIC => {
						if let Some(b) = cell {
							let v = dec_of(b, oid, format)?;
							self.num = Some(match &self.num {
								Some(s) => s.add(&v),
								None => v,
							});
						}
					}
					FLOAT4 | FLOAT8 => {
						let what = if a.kind == AggKind::Sum { "sum" } else { "avg" };
						return Err(refusal(
							format!(
								"{what} of {} values across nodes depends on the order the values are added, so Lepis does not compute it",
								type_name(oid)
							),
							format!(
								"Cast the argument to numeric ({what}(x::numeric)), or pin the query to one shard key value."
							),
						));
					}
					other => {
						return Err(refusal(
							format!(
								"Lepis does not combine a sum of {} across nodes yet",
								type_name(other)
							),
							"Pin the query to one shard key value.",
						));
					}
				}
			}
			AggKind::Min | AggKind::Max => {
				if let Some(b) = cell {
					let t = tok.value(b, a.col)?;
					let better = match &self.best {
						None => true,
						Some((cur, _)) => {
							let o = t.cmp(cur);
							(a.kind == AggKind::Min && o == Ordering::Less)
								|| (a.kind == AggKind::Max && o == Ordering::Greater)
						}
					};
					if better {
						self.best = Some((t, b.clone()));
					}
				}
			}
			AggKind::BoolAnd | AggKind::BoolOr => {
				if let Some(b) = cell {
					let Token::Bool(v) = native_token(b, oid, format)? else {
						return Err(malformed(oid));
					};
					self.bool = Some(match (self.bool, a.kind) {
						(None, _) => v,
						(Some(p), AggKind::BoolAnd) => p && v,
						(Some(p), _) => p || v,
					});
				}
			}
			AggKind::CountDistinct | AggKind::SumDistinct | AggKind::AvgDistinct => {
				if let Some(b) = cell {
					let t = tok.value(b, a.col)?;
					if a.kind != AggKind::CountDistinct && !self.distinct.contains_key(&t) {
						match oid {
							INT2 | INT4 => self.ints = true,
							INT8 | NUMERIC => {}
							other => {
								return Err(refusal(
									format!(
										"Lepis does not combine a sum of distinct {} values across nodes",
										type_name(other)
									),
									"Cast the argument to numeric, or pin the query to one shard key value.",
								));
							}
						}
						let v = dec_of(b, oid, format)?;
						self.distinct.insert(t, v);
					} else {
						self.distinct.entry(t).or_insert_with(Dec::zero);
					}
				}
			}
		}
		Ok(())
	}

	fn finish(&self, a: &Agg) -> Result<Val, MergeError> {
		let sum = |ints: bool, int: Option<i128>, num: &Option<Dec>| -> Result<Val, MergeError> {
			if ints {
				return match int {
					None => Ok(Val::Null),
					Some(v) if i64::try_from(v).is_ok() => Ok(Val::Int(v)),
					Some(_) => Err(err("22003", "bigint out of range")),
				};
			}
			Ok(num.clone().map_or(Val::Null, Val::Num))
		};
		Ok(match a.kind {
			AggKind::Count => Val::Int(self.count),
			AggKind::Sum => sum(self.ints, self.int, &self.num)?,
			AggKind::Min | AggKind::Max => match &self.best {
				Some((t, b)) => Val::Raw(b.clone(), Some(t.clone())),
				None => Val::Null,
			},
			AggKind::Avg => {
				if self.count == 0 {
					return Ok(Val::Null);
				}
				let total = if self.ints {
					self.int.map(Dec::from_i128)
				} else {
					self.num.clone()
				};
				let Some(total) = total else {
					return Ok(Val::Null);
				};
				Val::Num(total.div_count(self.count as u64))
			}
			AggKind::BoolAnd | AggKind::BoolOr => self.bool.map_or(Val::Null, Val::Bool),
			AggKind::CountDistinct => Val::Int(self.distinct.len() as i128),
			AggKind::SumDistinct | AggKind::AvgDistinct => {
				if self.distinct.is_empty() {
					return Ok(Val::Null);
				}
				let mut total = Dec::zero();
				for v in self.distinct.values() {
					total = total.add(v);
				}
				if a.kind == AggKind::AvgDistinct {
					Val::Num(total.div_count(self.distinct.len() as u64))
				} else if self.ints {
					match total.to_i64() {
						Some(v) => Val::Int(v as i128),
						None => return Err(err("22003", "bigint out of range")),
					}
				} else {
					Val::Num(total)
				}
			}
		})
	}
}

/// Evaluates an `Expr` for one group.
struct Eval<'a> {
	rep: Option<&'a Row>,
	finals: &'a [Val],
	tok: &'a Tokens<'a>,
}

impl Eval<'_> {
	fn eval(&self, e: &Expr) -> Result<Val, MergeError> {
		let unsupported = |what: &str| {
			refusal(
				format!("Lepis cannot compute {what} after combining the nodes' aggregates"),
				"Select the aggregates themselves and compute the rest in the application, or pin the query to one shard key value.",
			)
		};
		Ok(match e {
			Expr::Col(c) => match self.rep.and_then(|r| r.get(*c)) {
				Some(Some(b)) => {
					let t = self.tok.value(b, *c).ok();
					Val::Raw(b.clone(), t)
				}
				_ => Val::Null,
			},
			Expr::Agg(i) => self.finals[*i].clone(),
			Expr::Int(v) => Val::Int(*v),
			Expr::Num(s) => Val::Num(Dec::parse(s).ok_or_else(|| unsupported("this number"))?),
			Expr::Null => Val::Null,
			Expr::Cmp(op, a, b) => {
				let (a, b) = (self.eval(a)?, self.eval(b)?);
				if matches!(a, Val::Null) || matches!(b, Val::Null) {
					return Ok(Val::Null);
				}
				let o = match (&a, &b) {
					(Val::Bool(x), Val::Bool(y)) => x.cmp(y),
					_ => match (a.dec(), b.dec()) {
						(Some(x), Some(y)) => x.order(&y),
						_ => {
							return Err(unsupported("a comparison of values that are not numbers"));
						}
					},
				};
				Val::Bool(match op {
					CmpOp::Eq => o == Ordering::Equal,
					CmpOp::Ne => o != Ordering::Equal,
					CmpOp::Lt => o == Ordering::Less,
					CmpOp::Le => o != Ordering::Greater,
					CmpOp::Gt => o == Ordering::Greater,
					CmpOp::Ge => o != Ordering::Less,
				})
			}
			Expr::And(v) | Expr::Or(v) => {
				let and = matches!(e, Expr::And(_));
				let mut null = false;
				for x in v {
					match self.eval(x)? {
						Val::Bool(b) if b != and => return Ok(Val::Bool(b)),
						Val::Bool(_) => {}
						Val::Null => null = true,
						_ => return Err(unsupported("AND / OR of values that are not booleans")),
					}
				}
				if null { Val::Null } else { Val::Bool(and) }
			}
			Expr::Not(a) => match self.eval(a)? {
				Val::Bool(b) => Val::Bool(!b),
				Val::Null => Val::Null,
				_ => return Err(unsupported("NOT of a value that is not a boolean")),
			},
			Expr::IsNull(a, negated) => {
				let null = matches!(self.eval(a)?, Val::Null);
				Val::Bool(null != *negated)
			}
			Expr::Round(a, places) => {
				let a = self.eval(a)?;
				let places = match places {
					None => Val::Int(0),
					Some(p) => self.eval(p)?,
				};
				match (a, places) {
					(Val::Null, _) | (_, Val::Null) => Val::Null,
					(x, Val::Int(p)) => match x.dec() {
						Some(d) => {
							Val::Num(d.round(p.clamp(i32::MIN as i128, i32::MAX as i128) as i64))
						}
						None => return Err(unsupported("round() of a value that is not numeric")),
					},
					_ => {
						return Err(unsupported(
							"round() to a number of places that is not an integer",
						));
					}
				}
			}
			Expr::Numeric(a) => match self.eval(a)? {
				Val::Null => Val::Null,
				x => match x.dec() {
					Some(d) => Val::Num(d),
					None => {
						return Err(unsupported(
							"a cast to numeric of a value that is not a number",
						));
					}
				},
			},
			Expr::Int8(a) => match self.eval(a)? {
				Val::Null => Val::Null,
				x => match x.dec().and_then(|d| d.to_i64()) {
					Some(v) => Val::Int(v as i128),
					None if x.dec().is_some() => return Err(err("22003", "bigint out of range")),
					None => {
						return Err(unsupported(
							"a cast to bigint of a value that is not a number",
						));
					}
				},
			},
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn d(s: &str) -> Dec {
		Dec::parse(s).unwrap()
	}

	#[test]
	fn decimals_print_as_postgres_does() {
		for s in [
			"0",
			"1",
			"-1",
			"0.5",
			"-0.05",
			"123.4500",
			"0.000",
			"NaN",
			"Infinity",
			"-Infinity",
			"10000",
		] {
			assert_eq!(d(s).to_text(), s);
		}
		assert_eq!(d("-0").to_text(), "0");
		assert_eq!(d("007.10").to_text(), "7.10");
		assert!(Dec::parse("1e5").is_none());
		assert!(Dec::parse("").is_none());
	}

	#[test]
	fn sums_keep_the_largest_scale_and_specials() {
		assert_eq!(d("1.5").add(&d("2.25")).to_text(), "3.75");
		assert_eq!(d("1.50").add(&d("-1.5")).to_text(), "0.00");
		assert_eq!(d("-10").add(&d("3.1")).to_text(), "-6.9");
		assert_eq!(d("999").add(&d("1")).to_text(), "1000");
		assert_eq!(d("Infinity").add(&d("-Infinity")).to_text(), "NaN");
		assert_eq!(d("Infinity").add(&d("5")).to_text(), "Infinity");
		assert_eq!(d("NaN").add(&d("5")).to_text(), "NaN");
	}

	#[test]
	fn division_matches_numeric_div() {
		// Values checked against Postgres 18: select avg(x) …
		assert_eq!(d("3").div_count(2).to_text(), "1.5000000000000000");
		assert_eq!(d("7").div_count(3).to_text(), "2.3333333333333333");
		assert_eq!(d("0").div_count(1).to_text(), "0.00000000000000000000");
		assert_eq!(d("30000").div_count(2).to_text(), "15000.000000000000");
		assert_eq!(d("3.75").div_count(2).to_text(), "1.8750000000000000");
		assert_eq!(d("-5").div_count(3).to_text(), "-1.6666666666666667");
		assert_eq!(d("2").div_count(3).to_text(), "0.66666666666666666667");
		assert_eq!(
			d("12345678901234567890").div_count(7).to_text(),
			"1763668414462081127"
		);
		assert_eq!(
			d("0.0001").div_count(3).to_text(),
			"0.000033333333333333333333"
		);
	}

	#[test]
	fn rounding_is_half_away_from_zero() {
		assert_eq!(d("2.5").round(0).to_text(), "3");
		assert_eq!(d("-2.5").round(0).to_text(), "-3");
		assert_eq!(d("1.2345").round(2).to_text(), "1.23");
		assert_eq!(d("1.235").round(2).to_text(), "1.24");
		assert_eq!(d("1.5").round(4).to_text(), "1.5000");
		assert_eq!(d("1250").round(-2).to_text(), "1300");
		assert_eq!(d("49").round(-2).to_text(), "0");
		assert_eq!(d("0.0004").round(3).to_text(), "0.000");
		assert_eq!(d("9.99").round(1).to_text(), "10.0");
		assert_eq!(d("2.5").to_i64(), Some(3));
		assert_eq!(d("9223372036854775808").to_i64(), None);
	}

	#[test]
	fn order_is_numerics() {
		assert_eq!(d("1.0").order(&d("1")), Ordering::Equal);
		assert_eq!(d("-2").order(&d("1")), Ordering::Less);
		assert_eq!(d("-2").order(&d("-10")), Ordering::Greater);
		assert_eq!(d("NaN").order(&d("Infinity")), Ordering::Greater);
		assert_eq!(d("-Infinity").order(&d("-99999")), Ordering::Less);
		assert_eq!(
			Token::Float(f64::NAN).cmp(&Token::Float(f64::INFINITY)),
			Ordering::Greater
		);
		assert_eq!(Token::Float(-0.0).cmp(&Token::Float(0.0)), Ordering::Equal);
		assert_eq!(Token::Int(3).cmp(&Token::Num(d("2.5"))), Ordering::Greater);
	}

	#[test]
	fn numeric_binary_round_trips() {
		for s in [
			"0",
			"1",
			"-1",
			"12345.678",
			"0.0001",
			"10000",
			"99999999.99999999",
			"-0.5",
			"0.000",
			"NaN",
			"Infinity",
			"-Infinity",
			"1.5000000000000000",
		] {
			let b = d(s).to_binary();
			assert_eq!(Dec::from_binary(&b).unwrap().to_text(), s, "{s}");
		}
		// 12345.678: weight 1, digits 1 2345 6780, dscale 3.
		let b = d("12345.678").to_binary();
		assert_eq!(&b[..8], &[0, 3, 0, 1, 0, 0, 0, 3]);
		assert_eq!(&b[8..], &[0, 1, 0x09, 0x29, 0x1a, 0x7c]);
	}

	fn rows(v: &[&[Option<&str>]]) -> Vec<Row> {
		v.iter()
			.map(|r| r.iter().map(|c| c.map(|s| s.as_bytes().to_vec())).collect())
			.collect()
	}

	#[test]
	fn a_k_way_merge_with_limit_and_offset() {
		let plan = MergePlan {
			visible: 1,
			collation_of: vec![None],
			kind: MergeKind::Rows { sorted: true },
			distinct: false,
			order: vec![SortKey {
				expr: Expr::Col(0),
				desc: true,
				nulls_first: true,
			}],
			offset: 1,
			limit: Some(3),
		};
		let nodes = vec![
			rows(&[&[None], &[Some("9")], &[Some("4")]]),
			rows(&[&[Some("8")], &[Some("5")], &[Some("1")]]),
		];
		let g = Gathered {
			types: &[INT4],
			formats: &[0],
			nodes: &nodes,
		};
		let out = plan.combine(&g, &Ranks::new(), &[(INT4, 0)]).unwrap();
		assert_eq!(out, rows(&[&[Some("9")], &[Some("8")], &[Some("5")]]));
		// A node out of order is an error, never a wrong answer.
		let bad = vec![rows(&[&[Some("1")], &[Some("9")]])];
		let g = Gathered {
			types: &[INT4],
			formats: &[0],
			nodes: &bad,
		};
		assert!(plan.combine(&g, &Ranks::new(), &[(INT4, 0)]).is_err());
	}

	/// The streaming merge gives what the buffered one gives, whatever the window size.
	#[test]
	fn a_streaming_merge_matches_the_buffered_one() {
		for (sorted, distinct, offset, limit) in [
			(true, false, 1, Some(3)),
			(true, false, 0, None),
			(true, true, 0, None),
			(false, false, 2, Some(4)),
			(false, true, 0, None),
		] {
			let plan = MergePlan {
				visible: 1,
				collation_of: vec![None],
				kind: MergeKind::Rows { sorted },
				distinct,
				order: if sorted {
					vec![SortKey {
						expr: Expr::Col(0),
						desc: true,
						nulls_first: true,
					}]
				} else {
					Vec::new()
				},
				offset,
				limit,
			};
			let nodes = vec![
				rows(&[
					&[None],
					&[Some("9")],
					&[Some("9")],
					&[Some("4")],
					&[Some("2")],
				]),
				rows(&[&[Some("8")], &[Some("5")], &[Some("4")], &[Some("1")]]),
				rows(&[]),
				rows(&[&[Some("7")]]),
			];
			let g = Gathered {
				types: &[INT4],
				formats: &[0],
				nodes: &nodes,
			};
			let want = plan.combine(&g, &Ranks::new(), &[(INT4, 0)]).unwrap();
			for window in 1..4 {
				let mut m = plan.row_merge(nodes.len(), &[INT4], &[0]).unwrap();
				let mut at = vec![0usize; nodes.len()];
				let mut bufs: Vec<VecDeque<Row>> = vec![VecDeque::new(); nodes.len()];
				let mut finished = vec![false; nodes.len()];
				let mut got = Vec::new();
				loop {
					match m
						.step(&mut bufs, &finished, &Ranks::new(), &mut got)
						.unwrap()
					{
						Step::Done => break,
						Step::Need(n) => {
							let end = (at[n] + window).min(nodes[n].len());
							bufs[n].extend(nodes[n][at[n]..end].iter().cloned());
							at[n] = end;
							finished[n] = end == nodes[n].len();
						}
					}
				}
				assert_eq!(
					got, want,
					"sorted {sorted} distinct {distinct} window {window}"
				);
			}
		}
	}

	#[test]
	fn groups_sum_count_avg_and_having() {
		// worker: status (text, ranked), count(*), sum(int4 col) as int8, count(col).
		let plan = MergePlan {
			visible: 2,
			collation_of: vec![None; 4],
			kind: MergeKind::Groups(Groups {
				keys: vec![0],
				aggs: vec![
					Agg {
						kind: AggKind::Count,
						col: 1,
						count_col: None,
					},
					Agg {
						kind: AggKind::Avg,
						col: 2,
						count_col: Some(3),
					},
				],
				outputs: vec![
					Expr::Col(0),
					Expr::Round(Box::new(Expr::Agg(1)), Some(Box::new(Expr::Int(2)))),
				],
				having: Some(Expr::Cmp(
					CmpOp::Gt,
					Box::new(Expr::Agg(0)),
					Box::new(Expr::Int(2)),
				)),
				one_group: false,
			}),
			distinct: false,
			order: vec![SortKey {
				expr: Expr::Col(0),
				desc: false,
				nulls_first: false,
			}],
			offset: 0,
			limit: None,
		};
		let nodes = vec![
			rows(&[
				&[Some("b"), Some("2"), Some("10"), Some("2")],
				&[Some("a"), Some("1"), Some("1"), Some("1")],
			]),
			rows(&[
				&[Some("b"), Some("1"), Some("5"), Some("1")],
				&[Some("a"), Some("1"), Some("2"), Some("1")],
			]),
		];
		let g = Gathered {
			types: &[TEXT, INT8, INT8, INT8],
			formats: &[0; 4],
			nodes: &nodes,
		};
		let req = plan.needs_ranks(&g).unwrap();
		assert_eq!(req.len(), 1);
		assert_eq!(req[0].values, vec![b"a".to_vec(), b"b".to_vec()]);
		let mut ranks = Ranks::new();
		ranks.insert(
			0,
			[(b"a".to_vec(), 1), (b"b".to_vec(), 2)]
				.into_iter()
				.collect(),
		);
		let out = plan
			.combine(&g, &ranks, &[(TEXT, 0), (NUMERIC, 0)])
			.unwrap();
		assert_eq!(out, rows(&[&[Some("b"), Some("5.00")]]));
	}

	#[test]
	fn float_sums_are_refused_and_bigint_overflow_is_postgres_error() {
		let plan = MergePlan {
			visible: 1,
			collation_of: vec![None],
			kind: MergeKind::Groups(Groups {
				keys: vec![],
				aggs: vec![Agg {
					kind: AggKind::Sum,
					col: 0,
					count_col: None,
				}],
				outputs: vec![Expr::Agg(0)],
				having: None,
				one_group: true,
			}),
			distinct: false,
			order: vec![],
			offset: 0,
			limit: None,
		};
		let nodes = vec![rows(&[&[Some("1.5")]])];
		let g = Gathered {
			types: &[FLOAT8],
			formats: &[0],
			nodes: &nodes,
		};
		assert_eq!(
			plan.combine(&g, &Ranks::new(), &[(FLOAT8, 0)])
				.unwrap_err()
				.code,
			"0A000"
		);
		let nodes = vec![
			rows(&[&[Some("9223372036854775807")]]),
			rows(&[&[Some("1")]]),
		];
		let g = Gathered {
			types: &[INT8],
			formats: &[0],
			nodes: &nodes,
		};
		assert_eq!(
			plan.combine(&g, &Ranks::new(), &[(INT8, 0)])
				.unwrap_err()
				.code,
			"22003"
		);
		// No rows anywhere: one group, sum NULL.
		let nodes = vec![rows(&[&[None]]), vec![]];
		let g = Gathered {
			types: &[INT8],
			formats: &[0],
			nodes: &nodes,
		};
		assert_eq!(
			plan.combine(&g, &Ranks::new(), &[(INT8, 0)]).unwrap(),
			vec![vec![None]]
		);
	}
}
