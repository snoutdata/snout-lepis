//! The generated half of the oracle's corpus (Phase 0): SELECTs over
//! `oracle/schema.sql`, in the manner of SQLsmith but shaped to the schema, so nearly all of
//! them run and every one has exactly one right answer. `tests/cluster_generated.rs` runs them
//! through Lepis and against the reference Postgres.
//!
//! Seeded and reproducible: query `i` of seed `s` is the same text on every machine, and each
//! query draws from its own stream, so one can be rerun alone (`LEPIS_GEN_ONLY=i`).
//!
//! What keeps an answer single: an ORDER BY is always total (it ends in the primary key of
//! every table in the FROM, or in every grouping column), and rows are compared in order only
//! then; LIMIT and OFFSET appear only under such an ORDER BY; a floating sum or average is
//! rounded to six places (the values are tenths, so the order the nodes add them in cannot
//! reach the sixth place); an aggregate that builds a string orders its input. Everything else,
//! including the text form of an exact `avg` and the collation a text ORDER BY sorts by, is
//! the router's to reproduce.
#![allow(dead_code)]

use std::fmt::Write as _;

/// SplitMix64: small, fast, and the same on every platform, which is all a corpus needs.
pub struct Rng(u64);

impl Rng {
	pub fn new(seed: u64, index: u64) -> Rng {
		let mut r = Rng(seed ^ index.wrapping_mul(0xd1b5_4a32_d192_ed03));
		r.next();
		r
	}

	pub fn next(&mut self) -> u64 {
		self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
		let mut z = self.0;
		z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
		z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
		z ^ (z >> 31)
	}

	pub fn below(&mut self, n: usize) -> usize {
		(self.next() % n as u64) as usize
	}

	/// A whole number in `lo..=hi`.
	pub fn int(&mut self, lo: i64, hi: i64) -> i64 {
		lo + (self.next() % (hi - lo + 1) as u64) as i64
	}

	pub fn chance(&mut self, p: f64) -> bool {
		((self.next() >> 11) as f64) / ((1u64 << 53) as f64) < p
	}

	pub fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
		&xs[self.below(xs.len())]
	}
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
	Int,
	Text,
	Date,
	Ts,
	Tstz,
	Uuid,
	Float,
	Json,
	Bytea,
}

impl Kind {
	/// Has a btree ordering, so can be in ORDER BY, min/max and DISTINCT.
	fn orderable(self) -> bool {
		!matches!(self, Kind::Json | Kind::Bytea)
	}

	/// Has `min` and `max`: `uuid` sorts but has no aggregate for either.
	fn has_min_max(self) -> bool {
		self.orderable() && self != Kind::Uuid
	}
}

#[derive(Clone, Copy)]
struct Col {
	name: &'static str,
	kind: Kind,
	nullable: bool,
	/// Few distinct values: worth grouping by.
	group: bool,
}

const fn col(name: &'static str, kind: Kind) -> Col {
	Col {
		name,
		kind,
		nullable: false,
		group: false,
	}
}

const fn grp(name: &'static str, kind: Kind) -> Col {
	Col {
		name,
		kind,
		nullable: false,
		group: true,
	}
}

const fn null(name: &'static str, kind: Kind) -> Col {
	Col {
		name,
		kind,
		nullable: true,
		group: false,
	}
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Placement {
	Tenant,
	Device,
	Reference,
	Global,
}

struct Table {
	name: &'static str,
	alias: &'static str,
	placement: Placement,
	pk: &'static [&'static str],
	cols: &'static [Col],
}

const TENANTS: Table = Table {
	name: "tenants",
	alias: "t",
	placement: Placement::Tenant,
	pk: &["tenant_id"],
	cols: &[
		col("tenant_id", Kind::Int),
		col("name", Kind::Text),
		grp("country", Kind::Text),
		grp("plan_id", Kind::Int),
		col("created_at", Kind::Tstz),
	],
};

const ORDERS: Table = Table {
	name: "orders",
	alias: "o",
	placement: Placement::Tenant,
	pk: &["tenant_id", "order_id"],
	cols: &[
		col("tenant_id", Kind::Int),
		col("order_id", Kind::Int),
		col("placed_on", Kind::Date),
		col("placed_at", Kind::Ts),
		col("total_cents", Kind::Int),
		grp("status", Kind::Text),
		col("ref", Kind::Uuid),
	],
};

const ITEMS: Table = Table {
	name: "items",
	alias: "i",
	placement: Placement::Tenant,
	pk: &["tenant_id", "order_id", "line"],
	cols: &[
		col("tenant_id", Kind::Int),
		col("order_id", Kind::Int),
		grp("line", Kind::Int),
		col("sku", Kind::Text),
		grp("qty", Kind::Int),
		col("price_cents", Kind::Int),
		null("attrs", Kind::Json),
		null("blob", Kind::Bytea),
	],
};

const EVENTS: Table = Table {
	name: "events",
	alias: "e",
	placement: Placement::Device,
	pk: &["device", "seq"],
	cols: &[
		col("device", Kind::Uuid),
		col("seq", Kind::Int),
		col("at", Kind::Tstz),
		grp("kind", Kind::Text),
		null("value", Kind::Float),
	],
};

const COUNTRIES: Table = Table {
	name: "countries",
	alias: "c",
	placement: Placement::Reference,
	pk: &["code"],
	cols: &[
		col("code", Kind::Text),
		col("name", Kind::Text),
		grp("region", Kind::Text),
	],
};

const PLANS: Table = Table {
	name: "plans",
	alias: "p",
	placement: Placement::Global,
	pk: &["id"],
	cols: &[
		col("id", Kind::Int),
		col("name", Kind::Text),
		col("monthly_cents", Kind::Int),
	],
};

const SHARDED: [&Table; 4] = [&TENANTS, &ORDERS, &ITEMS, &EVENTS];
const STATUSES: [&str; 4] = ["new", "paid", "shipped", "refunded"];
const COUNTRY_CODES: [&str; 9] = ["CA", "US", "BR", "DE", "FR", "PT", "JP", "IN", "NG"];
const REGIONS: [&str; 4] = ["Americas", "Europe", "Asia", "Africa"];
const KINDS: [&str; 3] = ["boot", "reading", "alarm"];

/// One generated statement and how its answer is compared.
pub struct Query {
	pub family: &'static str,
	pub sql: String,
	/// Compared row by row in order (the ORDER BY is total); otherwise as a multiset.
	pub ordered: bool,
}

/// The values the generator cannot derive on its own: the oracle's device keys, which are md5
/// digests, read once from the reference.
pub struct Generator {
	devices: Vec<String>,
}

impl Generator {
	pub fn new(devices: Vec<String>) -> Generator {
		assert!(!devices.is_empty(), "no devices in oracle.events");
		Generator { devices }
	}

	pub fn query(&self, seed: u64, index: u64) -> Query {
		let mut g = Gen {
			r: Rng::new(seed, index),
			devices: &self.devices,
		};
		let w = g.r.below(100);
		match w {
			0..=19 => g.rows(),
			20..=39 => g.aggregate(),
			40..=56 => g.join(),
			57..=71 => g.subquery(),
			72..=77 => g.distinct(),
			78..=83 => g.set_operation(),
			84..=89 => g.window(),
			90..=94 => g.unsharded(),
			_ => g.across(),
		}
	}
}

struct Gen<'a> {
	r: Rng,
	devices: &'a [String],
}

/// A filter on a table's shard key, and whether it names exactly one key (one node).
struct KeyFilter {
	sql: Option<String>,
	single: bool,
}

impl Gen<'_> {
	fn tenant(&mut self) -> i64 {
		// Mostly real tenants; now and then one that does not exist.
		if self.r.chance(0.05) {
			*self.r.pick(&[0, 501, -3, 100_000])
		} else {
			self.r.int(1, 500)
		}
	}

	fn device(&mut self) -> String {
		let d = self.r.pick(self.devices).clone();
		match self.r.below(10) {
			0 => d.to_uppercase(),
			1 => format!("{{{d}}}"),
			2 => d.replace('-', ""),
			_ => d,
		}
	}

	fn sharded(&mut self) -> &'static Table {
		SHARDED[self.r.below(SHARDED.len())]
	}

	/// A filter on `alias.key`, of every shape the analyzer reads and some it should not.
	fn key_filter(&mut self, t: &Table, alias: &str, none_ok: bool) -> KeyFilter {
		let single = |sql: String| KeyFilter {
			sql: Some(sql),
			single: true,
		};
		let many = |sql: String| KeyFilter {
			sql: Some(sql),
			single: false,
		};
		let roll = self.r.below(100);
		if none_ok && roll < 30 {
			return KeyFilter {
				sql: None,
				single: false,
			};
		}
		match t.placement {
			Placement::Tenant => {
				let c = format!("{alias}.tenant_id");
				let k = self.tenant();
				match self.r.below(12) {
					0..=4 => single(format!("{c} = {k}")),
					5 => single(format!("{c} = '{k}'")),
					6 => single(format!("{c} = {k}::bigint")),
					7 => single(format!("{k} = {c}")),
					8 => {
						let ks: Vec<String> = (0..self.r.int(2, 5))
							.map(|_| self.tenant().to_string())
							.collect();
						many(format!("{c} in ({})", ks.join(", ")))
					}
					9 => {
						let lo = self.r.int(1, 480);
						many(format!("{c} between {lo} and {}", lo + self.r.int(0, 20)))
					}
					10 => {
						let k2 = self.tenant();
						many(format!("({c} = {k} or {c} = {k2})"))
					}
					_ => {
						let k2 = self.tenant();
						many(format!("{c} = any(array[{k}, {k2}]::bigint[])"))
					}
				}
			}
			Placement::Device => {
				let c = format!("{alias}.device");
				let d = self.device();
				match self.r.below(6) {
					0..=3 => single(format!("{c} = '{d}'")),
					4 => single(format!("{c} = '{d}'::uuid")),
					_ => {
						let d2 = self.device();
						many(format!("{c} in ('{d}', '{d2}')"))
					}
				}
			}
			Placement::Reference | Placement::Global => KeyFilter {
				sql: None,
				single: false,
			},
		}
	}

	/// A predicate on one non-key column.
	fn predicate(&mut self, t: &Table, alias: &str) -> String {
		let c = |n: &str| format!("{alias}.{n}");
		let r = &mut self.r;
		match t.name {
			"tenants" => match r.below(5) {
				0 => format!("{} = '{}'", c("country"), r.pick(&COUNTRY_CODES)),
				1 => format!(
					"{} in ('{}', '{}')",
					c("country"),
					r.pick(&COUNTRY_CODES),
					r.pick(&COUNTRY_CODES)
				),
				2 => format!("{} = {}", c("plan_id"), r.int(1, 4)),
				3 => format!("{} like 'tenant {}%'", c("name"), r.int(1, 9)),
				_ => format!(
					"{} >= timestamptz '2024-01-01 00:00+00' + interval '{} hours'",
					c("created_at"),
					r.int(0, 18_500)
				),
			},
			"orders" => match r.below(7) {
				0 => format!("{} > {}", c("total_cents"), r.int(100, 50_000)),
				1 => format!(
					"{} between {} and {}",
					c("total_cents"),
					r.int(100, 20_000),
					r.int(20_000, 50_100)
				),
				2 => format!("{} = '{}'", c("status"), r.pick(&STATUSES)),
				3 => format!(
					"{} in ('{}', '{}')",
					c("status"),
					r.pick(&STATUSES),
					r.pick(&STATUSES)
				),
				4 => format!("{} <= {}", c("order_id"), r.int(1, 40)),
				5 => format!(
					"{} >= date '2024-01-01' + {}",
					c("placed_on"),
					r.int(0, 700)
				),
				_ => format!(
					"{} < timestamp '2024-01-01' + interval '{} days'",
					c("placed_at"),
					r.int(0, 700)
				),
			},
			"items" => match r.below(7) {
				0 => format!("{} >= {}", c("qty"), r.int(1, 5)),
				1 => format!("{} = 'SKU-{}'", c("sku"), r.int(0, 249)),
				2 => format!("{} like 'SKU-{}%'", c("sku"), r.int(1, 9)),
				3 => format!("{} < {}", c("price_cents"), r.int(100, 9_100)),
				4 => format!(
					"{}->>'color' = '{}'",
					c("attrs"),
					r.pick(&["red", "green", "blue"])
				),
				5 => format!(
					"{} is {}null",
					c(r.pick(&["attrs", "blob"])),
					if r.chance(0.5) { "not " } else { "" }
				),
				_ => format!("{} = {}", c("line"), r.int(1, 4)),
			},
			"events" => match r.below(6) {
				0 => format!("{} = '{}'", c("kind"), r.pick(&KINDS)),
				1 => format!("{} > {}.{}", c("value"), r.int(0, 99), r.int(0, 9)),
				2 => format!(
					"{} is {}null",
					c("value"),
					if r.chance(0.5) { "not " } else { "" }
				),
				3 => {
					let lo = r.int(1, 45);
					format!("{} between {lo} and {}", c("seq"), lo + r.int(0, 10))
				}
				_ => format!(
					"{} >= timestamptz '2025-06-01 00:00+00' + interval '{} minutes'",
					c("at"),
					r.int(0, 200_050)
				),
			},
			"countries" => match r.below(2) {
				0 => format!("{} = '{}'", c("region"), r.pick(&REGIONS)),
				_ => format!("{} <> '{}'", c("code"), r.pick(&COUNTRY_CODES)),
			},
			_ => format!("{} >= {}", c("monthly_cents"), r.pick(&[0, 1500, 4900])),
		}
	}

	fn predicates(&mut self, t: &Table, alias: &str, max: usize) -> Vec<String> {
		let n = self.r.below(max + 1);
		(0..n).map(|_| self.predicate(t, alias)).collect()
	}

	/// An output expression over a column: the column itself, mostly.
	fn expression(&mut self, alias: &str, c: &Col) -> String {
		let q = format!("{alias}.{}", c.name);
		if !self.r.chance(0.2) {
			return q;
		}
		match c.kind {
			Kind::Int => format!("{q} * 2 + 1"),
			Kind::Text => format!("upper({q})"),
			Kind::Date => format!("{q} + 1"),
			Kind::Ts | Kind::Tstz => format!("date_trunc('day', {q})"),
			Kind::Uuid => format!("{q}::text"),
			Kind::Float => format!("coalesce({q}, -1)"),
			Kind::Json => format!("{q}->>'size'"),
			Kind::Bytea => format!("length({q})"),
		}
	}

	/// Some columns of `t`, never none.
	fn columns(&mut self, t: &'static Table) -> Vec<&'static Col> {
		let mut out: Vec<&Col> = t.cols.iter().filter(|_| self.r.chance(0.45)).collect();
		if out.is_empty() {
			out.push(self.r.pick(t.cols));
		}
		out
	}

	/// A total ORDER BY over `tables` (alias, table): perhaps a few ordinary columns first, then
	/// every primary-key column, each in a random direction.
	fn order_by(&mut self, tables: &[(&str, &'static Table)]) -> String {
		let mut keys = Vec::new();
		for _ in 0..self.r.below(3) {
			let (a, t) = *self.r.pick(tables);
			let c = self.r.pick(t.cols);
			if !c.kind.orderable() {
				continue;
			}
			let mut k = format!("{a}.{}{}", c.name, self.direction());
			if c.nullable {
				k.push_str(self.r.pick(&[" nulls first", " nulls last", ""]));
			}
			keys.push(k);
		}
		for (a, t) in tables {
			for pk in t.pk {
				keys.push(format!("{a}.{pk}{}", self.direction()));
			}
		}
		format!(" order by {}", keys.join(", "))
	}

	fn direction(&mut self) -> &'static str {
		if self.r.chance(0.3) { " desc" } else { "" }
	}

	fn limit(&mut self, always: bool) -> String {
		if !always && self.r.chance(0.5) {
			return String::new();
		}
		let mut s = format!(" limit {}", self.r.int(0, 40));
		if self.r.chance(0.4) {
			let _ = write!(s, " offset {}", self.r.int(0, 60));
		}
		s
	}

	fn where_clause(parts: &[String]) -> String {
		if parts.is_empty() {
			String::new()
		} else {
			format!(" where {}", parts.join(" and "))
		}
	}

	// ---------------------------------------------------------------------------------------
	// The families.

	/// Rows from one table: a key lookup, or a scan the router must scatter and merge.
	fn rows(&mut self) -> Query {
		let t = if self.r.chance(0.85) {
			self.sharded()
		} else {
			*self.r.pick(&[&COUNTRIES, &PLANS])
		};
		let a = t.alias;
		let key = self.key_filter(t, a, true);
		let mut parts: Vec<String> = key.sql.into_iter().collect();
		parts.extend(self.predicates(t, a, 2));
		let cols: Vec<String> = self
			.columns(t)
			.into_iter()
			.map(|c| self.expression(a, c))
			.collect();
		let mut sql = format!(
			"select {} from oracle.{} {a}{}",
			cols.join(", "),
			t.name,
			Self::where_clause(&parts)
		);
		// A scan over many keys is ordered and limited, so the answer stays small.
		let small = key.single || matches!(t.placement, Placement::Reference | Placement::Global);
		let ordered = !small || self.r.chance(0.6);
		if ordered {
			sql.push_str(&self.order_by(&[(a, t)]));
			sql.push_str(&self.limit(!small));
		}
		Query {
			family: "rows",
			sql,
			ordered,
		}
	}

	/// Aggregate expressions over `alias`'s columns.
	fn aggregates(&mut self, t: &'static Table, alias: &str) -> Vec<String> {
		let mut out = Vec::new();
		for _ in 0..self.r.int(1, 4) {
			let c = self.r.pick(t.cols);
			let q = format!("{alias}.{}", c.name);
			let a = match (c.kind, self.r.below(6)) {
				(_, 0) => "count(*)".to_string(),
				(Kind::Int, 1) => format!("sum({q})"),
				(Kind::Int, 2) => format!("avg({q})"),
				(Kind::Int, 3) => format!("round(avg({q}), 4)"),
				(Kind::Float, 1 | 2) => format!("round(sum({q})::numeric, 6)"),
				(Kind::Float, 3) => format!("round(avg({q})::numeric, 6)"),
				(k, 4) if k.has_min_max() => format!("min({q})"),
				(k, 5) if k.has_min_max() => format!("max({q})"),
				(k, _) if k.orderable() && self.r.chance(0.5) => format!("count(distinct {q})"),
				_ => format!("count({q})"),
			};
			out.push(a);
		}
		if self.r.chance(0.1) {
			let c = self.r.pick(t.cols);
			if c.kind == Kind::Int {
				out.push(format!(
					"count(*) filter (where {alias}.{} > {})",
					c.name,
					self.r.int(1, 1000)
				));
			}
		}
		if self.r.chance(0.04)
			&& let Some(c) = t.cols.iter().find(|c| c.group && c.kind == Kind::Text)
		{
			out.push(format!(
				"string_agg(distinct {alias}.{}, ',' order by {alias}.{})",
				c.name, c.name
			));
		}
		out
	}

	/// `count`/`sum`/`min`/`max`/`avg`, with and without GROUP BY and HAVING.
	fn aggregate(&mut self) -> Query {
		let t = if self.r.chance(0.9) {
			self.sharded()
		} else {
			&COUNTRIES
		};
		let a = t.alias;
		let key = self.key_filter(t, a, true);
		let mut parts: Vec<String> = key.sql.into_iter().collect();
		parts.extend(self.predicates(t, a, 2));
		let aggs = self.aggregates(t, a);
		let groups: Vec<String> = if self.r.chance(0.55) {
			let mut g: Vec<String> = t
				.cols
				.iter()
				.filter(|c| c.group && self.r.chance(0.6))
				.map(|c| format!("{a}.{}", c.name))
				.collect();
			if g.is_empty() || self.r.chance(0.2) {
				g.push(format!("{a}.{}", t.cols[0].name));
			}
			g.dedup();
			g
		} else {
			Vec::new()
		};
		let mut sql = "select ".to_string();
		if !groups.is_empty() {
			sql.push_str(&groups.join(", "));
			sql.push_str(", ");
		}
		let _ = write!(
			sql,
			"{} from oracle.{} {a}{}",
			aggs.join(", "),
			t.name,
			Self::where_clause(&parts)
		);
		let mut ordered = groups.is_empty();
		if !groups.is_empty() {
			let _ = write!(sql, " group by {}", groups.join(", "));
			if self.r.chance(0.35) {
				let _ = write!(sql, " having count(*) > {}", self.r.int(0, 40));
			}
			if self.r.chance(0.6) {
				let keys: Vec<String> = groups
					.iter()
					.map(|g| format!("{g}{}", self.direction()))
					.collect();
				let _ = write!(sql, " order by {}", keys.join(", "));
				sql.push_str(&self.limit(false));
				ordered = true;
			}
		}
		Query {
			family: "aggregate",
			sql,
			ordered,
		}
	}

	/// Joins: colocated on the key, with the reference table, with the global one, LEFT and
	/// inner, as rows or as aggregates.
	fn join(&mut self) -> Query {
		let (left, right, on): (&'static Table, &'static Table, &str) = match self.r.below(6) {
			0 | 1 => (
				&ORDERS,
				&ITEMS,
				"i.tenant_id = o.tenant_id and i.order_id = o.order_id",
			),
			2 => (&TENANTS, &ORDERS, "o.tenant_id = t.tenant_id"),
			3 => (&TENANTS, &COUNTRIES, "c.code = t.country"),
			4 => (&TENANTS, &PLANS, "p.id = t.plan_id"),
			_ => (&ORDERS, &ITEMS, "using"),
		};
		let (la, ra) = (left.alias, right.alias);
		let kind = if right.placement == Placement::Tenant && self.r.chance(0.25) {
			"left join"
		} else {
			"join"
		};
		let cond = if on == "using" {
			" using (tenant_id, order_id)".to_string()
		} else {
			format!(" on {on}")
		};
		let key = self.key_filter(left, la, true);
		let mut parts: Vec<String> = key.sql.into_iter().collect();
		parts.extend(self.predicates(left, la, 1));
		if kind == "join" {
			parts.extend(self.predicates(right, ra, 1));
		}
		let from = format!(
			"oracle.{} {la} {kind} oracle.{} {ra}{cond}",
			left.name, right.name
		);
		let (sql, ordered) = if self.r.chance(0.5) {
			// Rows: every table's key in the ORDER BY makes it total.
			let mut cols: Vec<String> = Vec::new();
			for c in self.columns(left) {
				cols.push(self.expression(la, c));
			}
			for c in self.columns(right) {
				if on == "using" && matches!(c.name, "tenant_id" | "order_id") {
					continue;
				}
				cols.push(self.expression(ra, c));
			}
			if cols.is_empty() {
				cols.push(format!("{la}.{}", left.pk[0]));
			}
			let mut sql = format!(
				"select {} from {from}{}",
				cols.join(", "),
				Self::where_clause(&parts)
			);
			let ordered = !key.single || self.r.chance(0.5);
			if ordered {
				let tables: Vec<(&str, &'static Table)> = if on == "using" {
					// USING merges the key columns; the right side's own key is `line`.
					vec![(la, left)]
				} else {
					vec![(la, left), (ra, right)]
				};
				sql.push_str(&self.order_by(&tables));
				if on == "using" {
					let _ = write!(sql, ", {ra}.line{}", self.direction());
				}
				sql.push_str(&self.limit(!key.single));
			}
			(sql, ordered)
		} else {
			let g = right
				.cols
				.iter()
				.chain(left.cols.iter())
				.find(|c| c.group)
				.copied();
			let group_on = |alias_of: &str| {
				if right.cols.iter().any(|c| c.name == alias_of) {
					ra
				} else {
					la
				}
			};
			let mut aggs = self.aggregates(left, la);
			aggs.extend(self.aggregates(right, ra));
			match g {
				Some(gc) if self.r.chance(0.6) => {
					let ga = format!("{}.{}", group_on(gc.name), gc.name);
					(
						format!(
							"select {ga}, {} from {from}{} group by {ga} order by {ga}",
							aggs.join(", "),
							Self::where_clause(&parts)
						),
						true,
					)
				}
				_ => (
					format!(
						"select {} from {from}{}",
						aggs.join(", "),
						Self::where_clause(&parts)
					),
					true,
				),
			}
		};
		Query {
			family: "join",
			sql,
			ordered,
		}
	}

	/// IN and EXISTS subqueries, correlated scalar ones, derived tables, CTEs, and a scalar
	/// over every shard used as a filter.
	fn subquery(&mut self) -> Query {
		let key = self.key_filter(&ORDERS, "o", true);
		let mut parts: Vec<String> = key.sql.clone().into_iter().collect();
		let mut select = "o.tenant_id, o.order_id, o.total_cents".to_string();
		let family_sql = match self.r.below(8) {
			0 => {
				parts.push(format!(
					"o.tenant_id in (select t.tenant_id from oracle.tenants t where {})",
					self.predicate(&TENANTS, "t")
				));
				None
			}
			1 => {
				parts.push(format!(
					"exists (select 1 from oracle.items i where i.tenant_id = o.tenant_id and i.order_id = o.order_id and {})",
					self.predicate(&ITEMS, "i")
				));
				None
			}
			2 => {
				select.push_str(
					", (select count(*) from oracle.items i where i.tenant_id = o.tenant_id and i.order_id = o.order_id) as lines",
				);
				None
			}
			3 => {
				parts.push(format!(
					"o.tenant_id in (select t.tenant_id from oracle.tenants t join oracle.countries c on c.code = t.country where c.region = '{}')",
					self.r.pick(&REGIONS)
				));
				None
			}
			4 => {
				// The average over every node, as a filter: per-node averages would be wrong.
				parts.push(format!(
					"o.total_cents > (select {}(o2.total_cents) from oracle.orders o2)",
					self.r.pick(&["avg", "max", "min"])
				));
				None
			}
			5 => {
				let inner = self.predicate(&ORDERS, "o");
				let grp = *self.r.pick(&["status", "tenant_id"]);
				let w = Self::where_clause(&parts);
				Some((
					format!(
						"select s.{grp}, count(*), sum(s.total_cents) from (select o.* from oracle.orders o{w}{}{inner}) s group by s.{grp} order by s.{grp}",
						if w.is_empty() { " where " } else { " and " }
					),
					true,
				))
			}
			6 => {
				let w = Self::where_clause(&parts);
				let p = self.predicate(&ORDERS, "o");
				Some((
					format!(
						"with big as (select o.tenant_id, o.order_id, o.total_cents from oracle.orders o{w}{}{p}) select count(*), coalesce(sum(total_cents), 0), min(order_id) from big",
						if w.is_empty() { " where " } else { " and " }
					),
					true,
				))
			}
			_ => {
				parts.push(format!(
					"not exists (select 1 from oracle.items i where i.tenant_id = o.tenant_id and i.order_id = o.order_id and i.line > {})",
					self.r.int(1, 3)
				));
				None
			}
		};
		let (sql, ordered) = match family_sql {
			Some(x) => x,
			None => {
				let mut sql = format!(
					"select {select} from oracle.orders o{}",
					Self::where_clause(&parts)
				);
				let ordered = !key.single || self.r.chance(0.5);
				if ordered {
					sql.push_str(&self.order_by(&[("o", &ORDERS)]));
					sql.push_str(&self.limit(!key.single));
				}
				(sql, ordered)
			}
		};
		Query {
			family: "subquery",
			sql,
			ordered,
		}
	}

	/// DISTINCT over low-cardinality columns, ordered by all of them, or counted.
	fn distinct(&mut self) -> Query {
		let t = self.sharded();
		let a = t.alias;
		let key = self.key_filter(t, a, true);
		let mut parts: Vec<String> = key.sql.into_iter().collect();
		parts.extend(self.predicates(t, a, 1));
		let mut cols: Vec<String> = t
			.cols
			.iter()
			.filter(|c| c.group && self.r.chance(0.7))
			.map(|c| format!("{a}.{}", c.name))
			.collect();
		if cols.is_empty() {
			let c = t.cols.iter().find(|c| c.group).unwrap_or(&t.cols[0]);
			cols.push(format!("{a}.{}", c.name));
		}
		let w = Self::where_clause(&parts);
		if self.r.chance(0.3) {
			return Query {
				family: "distinct",
				sql: format!(
					"select count(distinct ({})) from oracle.{} {a}{w}",
					cols.join(", "),
					t.name
				),
				ordered: true,
			};
		}
		let order: Vec<String> = (1..=cols.len())
			.map(|n| format!("{n}{}", self.direction()))
			.collect();
		let mut sql = format!(
			"select distinct {} from oracle.{} {a}{w} order by {}",
			cols.join(", "),
			t.name,
			order.join(", ")
		);
		sql.push_str(&self.limit(false));
		Query {
			family: "distinct",
			sql,
			ordered: true,
		}
	}

	/// UNION, UNION ALL, INTERSECT and EXCEPT of two filtered reads of one table.
	fn set_operation(&mut self) -> Query {
		let t = if self.r.chance(0.8) {
			&ORDERS
		} else {
			&TENANTS
		};
		let a = t.alias;
		let pk = t.pk.iter().map(|p| format!("{a}.{p}")).collect::<Vec<_>>();
		let side = |g: &mut Self| {
			let key = g.key_filter(t, a, true);
			let mut parts: Vec<String> = key.sql.into_iter().collect();
			parts.extend(g.predicates(t, a, 1));
			format!(
				"select {} from oracle.{} {a}{}",
				pk.join(", "),
				t.name,
				Self::where_clause(&parts)
			)
		};
		let l = side(self);
		let r = side(self);
		let op = *self.r.pick(&["union", "union all", "intersect", "except"]);
		let order: Vec<String> = (1..=t.pk.len())
			.map(|n| format!("{n}{}", self.direction()))
			.collect();
		let mut sql = format!("{l} {op} {r} order by {}", order.join(", "));
		sql.push_str(&self.limit(true));
		Query {
			family: "set operation",
			sql,
			ordered: true,
		}
	}

	/// Windows: partitioned by the key (each partition on one node), and not.
	fn window(&mut self) -> Query {
		let key = self.key_filter(&ORDERS, "o", true);
		let parts: Vec<String> = key.sql.into_iter().collect();
		let partition = if self.r.chance(0.7) {
			"partition by o.tenant_id "
		} else {
			""
		};
		let f = *self.r.pick(&[
			"rank()",
			"row_number()",
			"sum(o.total_cents)",
			"count(*)",
			"lag(o.total_cents)",
		]);
		let mut sql = format!(
			"select o.tenant_id, o.order_id, {f} over ({partition}order by o.total_cents desc, o.tenant_id, o.order_id) from oracle.orders o{}",
			Self::where_clause(&parts)
		);
		sql.push_str(&self.order_by(&[("o", &ORDERS)]));
		sql.push_str(&self.limit(!key.single));
		Query {
			family: "window",
			sql,
			ordered: true,
		}
	}

	/// Reference and global tables alone: one copy answers.
	fn unsharded(&mut self) -> Query {
		let t = *self.r.pick(&[&COUNTRIES, &PLANS]);
		let a = t.alias;
		let parts = self.predicates(t, a, 1);
		let cols: Vec<String> = self
			.columns(t)
			.into_iter()
			.map(|c| self.expression(a, c))
			.collect();
		let mut sql = format!(
			"select {} from oracle.{} {a}{}",
			cols.join(", "),
			t.name,
			Self::where_clause(&parts)
		);
		let ordered = self.r.chance(0.6);
		if ordered {
			sql.push_str(&self.order_by(&[(a, t)]));
			sql.push_str(&self.limit(false));
		}
		Query {
			family: "unsharded",
			sql,
			ordered,
		}
	}

	/// What L9 refuses: joins across shards on something other than the key, and a key
	/// compared with another table's non-key column.
	fn across(&mut self) -> Query {
		let sql = match self.r.below(3) {
			0 => "select a.tenant_id, a.order_id, b.tenant_id, b.order_id from oracle.orders a join oracle.orders b on a.ref = b.ref and a.tenant_id <> b.tenant_id order by 1, 2, 3, 4 limit 5".to_string(),
			1 => format!(
				"select t.tenant_id, o.tenant_id, o.order_id from oracle.tenants t join oracle.orders o on o.order_id = t.plan_id where t.tenant_id = {} order by 1, 2, 3 limit 20",
				self.tenant()
			),
			_ => format!(
				"select e.kind, count(*) from oracle.events e join oracle.orders o on o.order_id = e.seq where o.tenant_id = {} group by e.kind order by e.kind",
				self.tenant()
			),
		};
		Query {
			family: "across",
			sql,
			ordered: true,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_same_seed_writes_the_same_corpus() {
		let g = Generator::new(vec!["242df4cf-cef9-291f-2da4-ae33926355a5".into()]);
		for i in 0..200 {
			assert_eq!(g.query(7, i).sql, g.query(7, i).sql);
		}
		assert_ne!(g.query(7, 1).sql, g.query(8, 1).sql);
	}
}
