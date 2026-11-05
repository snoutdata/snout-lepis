//! Chunked storage: a sharded table is, on each node, one plain table
//! per chunk it owns, `<t>_cNNN`, under an empty SHELL `<t>` that every chunk `INHERITS`.
//!
//! - A keyspace is cut into chunks, each a slice of the 64-bit L5 hash space. A range (L12) is a
//!   set of whole chunks; a split or a merge moves a boundary between chunks, never inside one.
//! - Each chunk carries its own fence `lepis_owns` over its slice (L7, per chunk), so a stray
//!   write fails `23514` as it does today.
//! - The shell carries `lepis_shell_insert CHECK (false) NO INHERIT`: a node-local INSERT into it
//!   fails loudly, while SELECT/UPDATE/DELETE through it reach that node's chunks natively.
//! - Routed statements name the chunk (`rewrite`); a scatter runs once per node against the shell.
//!
//! The Docker proof of every point above is `scripts/chunk-proof.sh`.

use std::collections::{HashMap, HashSet};

use pg_query::NodeEnum;
use pg_query::protobuf::{Node, RangeVar, SelectStmt, WithClause};

use crate::catalog::{KeySql, Keyspace, RelationName, quote_ident};

/// The shell's constraint, named so the error a node-local INSERT gets says why.
pub const SHELL_CONSTRAINT: &str = "lepis_shell_insert";
/// Chunk count when nothing says the table will be big.
pub const DEFAULT_CHUNKS: u32 = 64;
/// The most chunks a keyspace may start with: each is a table plus its indexes on its node.
/// An initial guess, measured in Phase 6.
pub const MAX_CHUNKS: u32 = 4096;
/// The size a chunk is planned for when an expected size is given (initial guess, Phase 6).
pub const TARGET_CHUNK_BYTES: u64 = 8 << 30;

/// One chunk of a keyspace: its number (stable for its life, never reused) and its hash slice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Chunk {
	pub number: u32,
	pub lo: i64,
	pub hi: i64,
}

/// The chunk count for a table expected to hold `expected_bytes`: a power of two, at least the
/// default, enough for chunks of about `TARGET_CHUNK_BYTES`, at most `MAX_CHUNKS`.
pub fn chunk_count_for(expected_bytes: Option<u64>) -> u32 {
	let want = expected_bytes.map_or(0, |b| b.div_ceil(TARGET_CHUNK_BYTES));
	let want = u32::try_from(want)
		.unwrap_or(MAX_CHUNKS)
		.max(DEFAULT_CHUNKS);
	want.checked_next_power_of_two()
		.unwrap_or(MAX_CHUNKS)
		.min(MAX_CHUNKS)
}

/// `count` equal chunks over the signed 64-bit space, numbered from 0 in hash order.
pub fn even_chunks(count: u32) -> Result<Vec<Chunk>, String> {
	if count == 0 || !count.is_power_of_two() || count > MAX_CHUNKS {
		return Err(format!(
			"a chunk count is a power of two from 1 to {MAX_CHUNKS}, not {count}"
		));
	}
	let width = (1i128 << 64) / i128::from(count);
	Ok((0..count)
		.map(|i| {
			let lo = i128::from(i64::MIN) + width * i128::from(i);
			Chunk {
				number: i,
				lo: lo as i64,
				hi: (lo + width - 1) as i64,
			}
		})
		.collect())
}

/// `count` chunks whose boundaries include every range boundary of `ks`: equal chunks, each
/// range boundary then snapped onto the nearest chunk boundary (ranges made by
/// `Keyspace::even_ranges` sit a few hash values off an exact power-of-two cut). So every range,
/// existing or new, is a set of whole chunks.
pub fn chunks_for(count: u32, ks: &Keyspace) -> Result<Vec<Chunk>, String> {
	let mut chunks = even_chunks(count)?;
	for r in &ks.ranges {
		if r.lo == i64::MIN {
			continue;
		}
		let i = chunks.partition_point(|c| c.lo <= r.lo);
		// The chunk boundary nearest r.lo: chunks[i - 1].lo or chunks[i].lo.
		let below = chunks[i - 1].lo;
		let j = match chunks.get(i) {
			Some(c)
				if (i128::from(c.lo) - i128::from(r.lo))
					< (i128::from(r.lo) - i128::from(below)) =>
			{
				i
			}
			_ => i - 1,
		};
		if j == 0 {
			return Err(format!(
				"keyspace {}: range at {} is narrower than a chunk",
				ks.name, r.lo
			));
		}
		chunks[j].lo = r.lo;
		chunks[j - 1].hi = r.lo - 1;
		if chunks[j].lo > chunks[j].hi || chunks[j - 1].lo > chunks[j - 1].hi {
			return Err(format!(
				"keyspace {}: range at {} is narrower than a chunk",
				ks.name, r.lo
			));
		}
	}
	validate(&chunks, ks)?;
	Ok(chunks)
}

/// Splits `c` in two at the middle of its slice; the halves take `c`'s number and `next`.
/// A one-value chunk cannot split (a hot single key is a pin, L12).
pub fn split(c: Chunk, next: u32) -> Option<(Chunk, Chunk)> {
	if c.lo == c.hi {
		return None;
	}
	let mid = ((i128::from(c.lo) + i128::from(c.hi) + 1).div_euclid(2)) as i64;
	Some((
		Chunk {
			number: c.number,
			lo: c.lo,
			hi: mid - 1,
		},
		Chunk {
			number: next,
			lo: mid,
			hi: c.hi,
		},
	))
}

/// The chunk holding hash `h`; `chunks` sorted by `lo` and covering the space.
pub fn chunk_of(chunks: &[Chunk], h: i64) -> Chunk {
	let i = chunks.partition_point(|c| c.lo <= h);
	chunks[i.saturating_sub(1)]
}

/// Chunks sorted by `lo`, contiguous, covering the whole space, numbers unique; and every range
/// of `ks` made of whole chunks.
pub fn validate(chunks: &[Chunk], ks: &Keyspace) -> Result<(), String> {
	let mut seen = HashSet::new();
	let mut at = i128::from(i64::MIN);
	for c in chunks {
		if i128::from(c.lo) != at || c.hi < c.lo {
			return Err(format!(
				"keyspace {}: chunk {} does not start where the last ended",
				ks.name, c.number
			));
		}
		if !seen.insert(c.number) {
			return Err(format!("keyspace {}: chunk {} twice", ks.name, c.number));
		}
		at = i128::from(c.hi) + 1;
	}
	if at != i128::from(i64::MAX) + 1 {
		return Err(format!(
			"keyspace {}: chunks do not cover the space",
			ks.name
		));
	}
	let starts: HashSet<i64> = chunks.iter().map(|c| c.lo).collect();
	for r in &ks.ranges {
		let next_ok = r.hi == i64::MAX || starts.contains(&(r.hi + 1));
		if !starts.contains(&r.lo) || !next_ok {
			return Err(format!(
				"keyspace {}: range [{}, {}] is not made of whole chunks",
				ks.name, r.lo, r.hi
			));
		}
	}
	Ok(())
}

/// The chunks a range `[lo, hi]` is made of.
pub fn chunks_in(chunks: &[Chunk], lo: i64, hi: i64) -> Vec<Chunk> {
	chunks
		.iter()
		.filter(|c| c.lo >= lo && c.hi <= hi)
		.copied()
		.collect()
}

/// `<t>_cNNN`: three digits at least, more for a chunk number past 999.
pub fn chunk_table(table: &str, number: u32) -> String {
	format!("{table}{}", suffix(number))
}

/// `_cNNN`, what a chunk's table and every object copied onto it end with.
pub fn suffix(number: u32) -> String {
	format!("_c{number:03}")
}

/// Postgres's longest identifier, in bytes (NAMEDATALEN - 1).
pub const MAX_IDENT: usize = 63;

/// The name of the chunk's copy of the shell's index or constraint `base`: `<base>_cNNN`, the
/// base cut (at a character boundary) so the whole fits in 63 bytes. `sync_sql` computes the
/// same in SQL.
pub fn object_name(base: &str, number: u32) -> String {
	let sfx = suffix(number);
	let room = MAX_IDENT.saturating_sub(sfx.len());
	if base.len() <= room {
		return format!("{base}{sfx}");
	}
	let mut end = room;
	while !base.is_char_boundary(end) {
		end -= 1;
	}
	format!("{}{sfx}", &base[..end])
}

/// Why `table` cannot be chunked by name: its chunk tables' names would not fit.
pub fn name_refusal(table: &str, chunks: &[Chunk]) -> Option<String> {
	let widest = chunks.iter().map(|c| c.number).max().unwrap_or(0);
	(chunk_table(table, widest).len() > MAX_IDENT).then(|| {
		format!(
			"{table} is too long a name to be chunked: {} is past Postgres's 63-byte limit",
			chunk_table(table, widest)
		)
	})
}

/// The chunk table's relation, in the shell's schema.
pub fn chunk_relation(shell: &RelationName, number: u32) -> RelationName {
	RelationName {
		schema: shell.schema.clone(),
		table: chunk_table(&shell.table, number),
	}
}

fn qualified(r: &RelationName) -> String {
	format!("{}.{}", quote_ident(&r.schema), quote_ident(&r.table))
}

/// Makes a table the shell: refuses every row of its own, the definition kept.
pub fn shell_sql(shell: &RelationName) -> String {
	format!(
		"alter table {t} drop constraint if exists {SHELL_CONSTRAINT}, \
		add constraint {SHELL_CONSTRAINT} check (false) no inherit",
		t = qualified(shell)
	)
}

/// `shell_sql` only when the shell lacks the constraint: the ALTER takes an ACCESS EXCLUSIVE lock
/// that queues behind any long write into a chunk (a COPY) and every client behind it.
pub fn ensure_shell_sql(shell: &RelationName) -> String {
	format!(
		"do $lepis_shell$ begin if not exists (select from pg_constraint where conrelid = {r}::regclass \
		and conname = {c}) then execute {sql}; end if; end $lepis_shell$",
		r = crate::catalog::quote_literal(&qualified(shell)),
		c = crate::catalog::quote_literal(SHELL_CONSTRAINT),
		sql = crate::catalog::quote_literal(&shell_sql(shell)),
	)
}

/// The fence of one chunk: the L5 hash of the key (and a second key, Phase 5) within the chunk's
/// slice. `key` is SQL, already quoted (`KeySql::of`).
pub fn chunk_fence(
	key: &KeySql,
	ks: &Keyspace,
	c: Chunk,
	server_version_num: u32,
) -> Result<String, String> {
	let expr = ks.hash_sql(key, server_version_num).map_err(|e| e.0)?;
	Ok(format!("{expr} between {} and {}", c.lo, c.hi))
}

/// Creates one chunk table of `shell`, not yet inheriting (`attach_sql` does that at cutover, or
/// at once for a new table). Columns, defaults, generated columns, NOT NULL and CHECKs come from
/// the shell through INHERITS. Indexes, the PK, unique keys, foreign keys, triggers, grants and
/// RLS are `sync_sql`'s, which names each chunk's copy `<name>_cNNN` (`object_name`), so a DROP
/// INDEX or an `ON CONFLICT ON CONSTRAINT` on the shell's name finds the chunk's. (`LIKE …
/// INCLUDING INDEXES`, the proof's form, names them after the chunk table instead.)
pub fn create_sql(
	shell: &RelationName,
	key: &KeySql,
	ks: &Keyspace,
	c: Chunk,
	server_version_num: u32,
) -> Result<String, String> {
	let fence = chunk_fence(key, ks, c, server_version_num)?;
	let t = qualified(&chunk_relation(shell, c.number));
	let s = qualified(shell);
	Ok(format!(
		"create table {t} (like {s} including storage including compression \
		including statistics including comments, constraint lepis_owns check ({fence})) \
		inherits ({s}); alter table {t} no inherit {s}"
	))
}

/// The chunk joins the shell: from here node-local SQL on the shell sees its rows.
pub fn attach_sql(shell: &RelationName, number: u32) -> String {
	format!(
		"alter table {} inherit {}",
		qualified(&chunk_relation(shell, number)),
		qualified(shell)
	)
}

/// At cutover on the side that gives a chunk up: it leaves the shell (no row counted twice),
/// is renamed out of reach (a stale router's DELETE gets `42P01`, which a CHECK cannot give) and
/// fenced (`lepis_owns` false, so an INSERT or UPDATE that finds it by the new name fails too).
/// Dropped afterwards by cleanup: no DELETE, no VACUUM.
pub fn retire_sql(shell: &RelationName, number: u32) -> String {
	let live = chunk_relation(shell, number);
	let gone = retired_name(&live.table);
	let t = qualified(&live);
	let g = format!("{}.{}", quote_ident(&live.schema), quote_ident(&gone));
	format!(
		"alter table {t} no inherit {s}; alter table {t} rename to {gq}; \
		alter table {g} drop constraint if exists lepis_owns, \
		add constraint lepis_owns check (false) not valid",
		s = qualified(shell),
		gq = quote_ident(&gone),
	)
}

/// `retire_sql`, only if the chunk table is there under its live name (a physical copy holds the
/// chunks its source had, a resumed cutover may have retired some already).
pub fn retire_if_there_sql(shell: &RelationName, number: u32) -> String {
	let live = qualified(&chunk_relation(shell, number));
	format!(
		"do $lepis_retire$ begin if to_regclass({}) is not null then {}; end if; end $lepis_retire$",
		crate::catalog::quote_literal(&live),
		retire_sql(shell, number)
			.split("; ")
			.map(|q| format!("execute {}", crate::catalog::quote_literal(q)))
			.collect::<Vec<_>>()
			.join("; ")
	)
}

/// What `node` must retire of the chunked tables in `tables` once `after` is the catalog: every
/// chunk it does not own then (`retire_if_there_sql`, so chunks it never had pass).
pub fn retire_unowned_sql(
	after: &crate::catalog::Catalog,
	tables: &[RelationName],
	node: crate::catalog::NodeId,
) -> String {
	let mut out = String::new();
	for rel in tables {
		let Some(chunks) = after.chunks_of(rel) else {
			continue;
		};
		let Some(crate::catalog::RelationKind::Sharded { keyspace, .. }) = after.relations.get(rel)
		else {
			continue;
		};
		let ks = &after.keyspaces[keyspace];
		for c in chunks {
			if ks.owner_of_hash(c.lo) != node {
				out.push_str(&retire_if_there_sql(rel, c.number));
				out.push_str(";\n");
			}
		}
	}
	out
}

/// `retire_sql` for a chunk that never joined the shell (a conversion's chunk built for another
/// node): renamed out of reach and fenced, nothing to leave.
pub fn retire_detached_sql(shell: &RelationName, number: u32) -> String {
	let live = chunk_relation(shell, number);
	let gone = retired_name(&live.table);
	format!(
		"alter table {} rename to {}; alter table {}.{} drop constraint if exists lepis_owns, \
		add constraint lepis_owns check (false) not valid",
		qualified(&live),
		quote_ident(&gone),
		quote_ident(&live.schema),
		quote_ident(&gone)
	)
}

/// The name a retired chunk waits under until cleanup drops it.
pub fn retired_name(chunk_table: &str) -> String {
	format!("lepis_retired_{chunk_table}")
}

/// Makes every chunk table of `shell` on this node match the shell, in plain SQL (one `DO`
/// block, run on the node): its owner, grants, row-level security and policies, indexes, the PK
/// and unique/exclusion constraints, foreign keys (to the referenced table's chunk of the same
/// number when that table is chunked, else to the table itself) and triggers, each copy named
/// `<name>_cNNN`; copies the shell no longer has are dropped. A chunk move's capture trigger
/// (`lepis_cap_…`, ops/chunkmove.rs) is never copied nor dropped: it belongs to the move. Idempotent: run after a chunk is
/// made and after every schema change to the shell. Columns, defaults, NOT NULL and CHECKs need
/// nothing: the chunks inherit them.
///
/// A chunk table is found by name (`<t>_c` and three or more digits, in the shell's schema), so
/// a chunk not yet attached (a move's target before cutover) is kept in step too.
pub fn sync_sql(shell: &RelationName) -> String {
	SYNC_SQL.replace("$SHELL$", &crate::catalog::quote_literal(&qualified(shell)))
}

const SYNC_SQL: &str = r#"do $lepis_sync$
declare
	s regclass := to_regclass($SHELL$);
	sn name;
	sns oid;
	sowner oid;
	c record;
	x record;
	sfx text;
	cname text;
	target text;
	def text;
	have text;
	ref oid;
	p int;
begin
	if s is null then
		return;
	end if;
	select relname, relnamespace, relowner into sn, sns, sowner from pg_class where oid = s;
	for c in
		select k.oid, k.oid::regclass::text as qname, k.relowner, k.relrowsecurity, k.relforcerowsecurity,
			substr(k.relname, length(sn) + 1) as sfx
		from pg_class k
		where k.relnamespace = sns and k.relkind = 'r'
			and left(k.relname, length(sn) + 2) = sn || '_c'
			and substr(k.relname, length(sn) + 3) ~ '^[0-9]{3,}$'
	loop
		sfx := c.sfx;
		-- Owner, grants, row-level security.
		if c.relowner <> sowner then
			execute format('alter table %s owner to %I', c.qname, pg_get_userbyid(sowner));
		end if;
		if exists (
			(select a.grantee, a.privilege_type, a.is_grantable
				from pg_class k, aclexplode(coalesce(k.relacl, acldefault('r', k.relowner))) a where k.oid = s
			except
			select a.grantee, a.privilege_type, a.is_grantable
				from pg_class k, aclexplode(coalesce(k.relacl, acldefault('r', k.relowner))) a where k.oid = c.oid)
			union all
			(select a.grantee, a.privilege_type, a.is_grantable
				from pg_class k, aclexplode(coalesce(k.relacl, acldefault('r', k.relowner))) a where k.oid = c.oid
			except
			select a.grantee, a.privilege_type, a.is_grantable
				from pg_class k, aclexplode(coalesce(k.relacl, acldefault('r', k.relowner))) a where k.oid = s)
		) then
			for x in
				select distinct a.grantee from pg_class k, aclexplode(coalesce(k.relacl, acldefault('r', k.relowner))) a
				where k.oid = c.oid
			loop
				execute format('revoke all on %s from %s', c.qname,
					case when x.grantee = 0 then 'public' else quote_ident(pg_get_userbyid(x.grantee)) end);
			end loop;
			for x in
				select a.grantee, a.privilege_type, a.is_grantable
				from pg_class k, aclexplode(coalesce(k.relacl, acldefault('r', k.relowner))) a where k.oid = s
			loop
				execute format('grant %s on %s to %s%s', x.privilege_type, c.qname,
					case when x.grantee = 0 then 'public' else quote_ident(pg_get_userbyid(x.grantee)) end,
					case when x.is_grantable then ' with grant option' else '' end);
			end loop;
		end if;
		select relrowsecurity, relforcerowsecurity into x from pg_class where oid = s;
		if x.relrowsecurity <> c.relrowsecurity then
			execute format('alter table %s %s row level security', c.qname,
				case when x.relrowsecurity then 'enable' else 'disable' end);
		end if;
		if x.relforcerowsecurity <> c.relforcerowsecurity then
			execute format('alter table %s %s row level security', c.qname,
				case when x.relforcerowsecurity then 'force' else 'no force' end);
		end if;
		-- Policies: the same name, command, kind, roles and expressions.
		for x in
			select cp.polname from pg_policy cp
			where cp.polrelid = c.oid and not exists (
				select from pg_policy sp where sp.polrelid = s and sp.polname = cp.polname
					and sp.polcmd = cp.polcmd and sp.polpermissive = cp.polpermissive
					and sp.polroles = cp.polroles
					and pg_get_expr(sp.polqual, s) is not distinct from pg_get_expr(cp.polqual, c.oid)
					and pg_get_expr(sp.polwithcheck, s) is not distinct from pg_get_expr(cp.polwithcheck, c.oid))
		loop
			execute format('drop policy %I on %s', x.polname, c.qname);
		end loop;
		for x in
			select sp.polname, sp.polcmd, sp.polpermissive, pg_get_expr(sp.polqual, s) as q,
				pg_get_expr(sp.polwithcheck, s) as w,
				(select string_agg(case when r = 0 then 'public' else quote_ident(pg_get_userbyid(r)) end, ', ')
					from unnest(sp.polroles) r) as roles
			from pg_policy sp
			where sp.polrelid = s and not exists (
				select from pg_policy cp where cp.polrelid = c.oid and cp.polname = sp.polname)
		loop
			execute format('create policy %I on %s as %s for %s to %s%s%s', x.polname, c.qname,
				case when x.polpermissive then 'permissive' else 'restrictive' end,
				case x.polcmd when 'r' then 'select' when 'a' then 'insert' when 'w' then 'update'
					when 'd' then 'delete' else 'all' end,
				x.roles,
				case when x.q is null then '' else ' using (' || x.q || ')' end,
				case when x.w is null then '' else ' with check (' || x.w || ')' end);
		end loop;
		-- Foreign keys first out (they may lean on an index that is about to change).
		for x in
			select cc.conname from pg_constraint cc
			where cc.conrelid = c.oid and cc.contype = 'f' and right(cc.conname, length(sfx)) = sfx
				and not exists (
					select from pg_constraint sc where sc.conrelid = s and sc.contype = 'f'
						and (case when octet_length(sc.conname || sfx) <= 63 then sc.conname || sfx
							else left(sc.conname, 63 - length(sfx)) || sfx end) = cc.conname)
		loop
			execute format('alter table %s drop constraint %I', c.qname, x.conname);
		end loop;
		-- Indexes, and the constraints an index carries (PK, UNIQUE, EXCLUDE).
		for x in
			select ti.relname, con.conname
			from pg_index i join pg_class ti on ti.oid = i.indexrelid
			left join pg_constraint con on con.conindid = i.indexrelid and con.conrelid = c.oid
				and con.contype in ('p', 'u', 'x')
			where i.indrelid = c.oid and right(ti.relname, length(sfx)) = sfx
				and not exists (
					select from pg_index si join pg_class sc on sc.oid = si.indexrelid
					where si.indrelid = s
						and (case when octet_length(sc.relname || sfx) <= 63 then sc.relname || sfx
							else left(sc.relname, 63 - length(sfx)) || sfx end) = ti.relname
						and si.indisunique = i.indisunique
						and substr(pg_get_indexdef(si.indexrelid), position(' USING ' in pg_get_indexdef(si.indexrelid)))
							= substr(pg_get_indexdef(i.indexrelid), position(' USING ' in pg_get_indexdef(i.indexrelid))))
		loop
			if x.conname is not null then
				execute format('alter table %s drop constraint %I', c.qname, x.conname);
			else
				execute format('drop index %I.%I', (select nspname from pg_namespace where oid = sns), x.relname);
			end if;
		end loop;
		for x in
			select sc.relname, si.indisunique, si.indexrelid, pg_get_indexdef(si.indexrelid) as idef, con.oid as conoid
			from pg_index si join pg_class sc on sc.oid = si.indexrelid
			left join pg_constraint con on con.conindid = si.indexrelid and con.conrelid = s
				and con.contype in ('p', 'u', 'x')
			where si.indrelid = s
		loop
			target := case when octet_length(x.relname || sfx) <= 63 then x.relname || sfx
				else left(x.relname, 63 - length(sfx)) || sfx end;
			if not exists (
				select from pg_index i join pg_class ti on ti.oid = i.indexrelid
				where i.indrelid = c.oid and ti.relname = target
			) then
				if x.conoid is not null then
					execute format('alter table %s add constraint %I %s', c.qname, target, pg_get_constraintdef(x.conoid));
				else
					execute format('create %sindex %I on %s%s', case when x.indisunique then 'unique ' else '' end,
						target, c.qname, substr(x.idef, position(' USING ' in x.idef)));
				end if;
			end if;
		end loop;
		-- Foreign keys: to the referenced table's chunk of this number, when it has one.
		for x in
			select sc.conname, sc.confrelid, pg_get_constraintdef(sc.oid) as cdef
			from pg_constraint sc where sc.conrelid = s and sc.contype = 'f'
		loop
			target := case when octet_length(x.conname || sfx) <= 63 then x.conname || sfx
				else left(x.conname, 63 - length(sfx)) || sfx end;
			def := x.cdef;
			select k.oid into ref from pg_class k
			where k.relnamespace = (select relnamespace from pg_class where oid = x.confrelid)
				and k.relname = (select relname from pg_class where oid = x.confrelid) || sfx
				and k.relkind = 'r';
			if ref is not null then
				def := replace(def, 'REFERENCES ' || x.confrelid::regclass::text || '(',
					'REFERENCES ' || ref::regclass::text || '(');
			end if;
			select pg_get_constraintdef(cc.oid) into have from pg_constraint cc
			where cc.conrelid = c.oid and cc.conname = target;
			if have is distinct from def then
				if have is not null then
					execute format('alter table %s drop constraint %I', c.qname, target);
				end if;
				execute format('alter table %s add constraint %I %s', c.qname, target, def);
			end if;
			ref := null;
			have := null;
		end loop;
		-- Triggers: the shell's, on the chunk, by the same name.
		for x in
			select ct.tgname from pg_trigger ct
			where ct.tgrelid = c.oid and not ct.tgisinternal and ct.tgname not like 'lepis\_cap\_%'
				and not exists (select from pg_trigger st where st.tgrelid = s and not st.tgisinternal
					and st.tgname = ct.tgname)
		loop
			execute format('drop trigger %I on %s', x.tgname, c.qname);
		end loop;
		for x in
			select st.tgname, st.tgenabled, pg_get_triggerdef(st.oid) as tdef
			from pg_trigger st where st.tgrelid = s and not st.tgisinternal
				and st.tgname not like 'lepis\_cap\_%' 
		loop
			-- pg_get_triggerdef names the table schema-qualified; s::text only when not visible.
			cname := ' ON ' || quote_ident((select nspname from pg_namespace where oid = sns)) || '.'
				|| quote_ident(sn) || ' ';
			p := position(cname in x.tdef);
			if p = 0 then
				cname := ' ON ' || s::text || ' ';
				p := position(cname in x.tdef);
			end if;
			if p = 0 then
				raise exception 'Lepis cannot copy trigger % of % to its chunks: %', x.tgname, s, x.tdef;
			end if;
			def := left(x.tdef, p - 1) || ' ON ' || c.qname || ' ' || substr(x.tdef, p + length(cname));
			select pg_get_triggerdef(ct.oid) into have from pg_trigger ct
			where ct.tgrelid = c.oid and ct.tgname = x.tgname and not ct.tgisinternal;
			if have is distinct from def then
				if have is not null then
					execute format('drop trigger %I on %s', x.tgname, c.qname);
				end if;
				execute def;
			end if;
			if (select tgenabled from pg_trigger where tgrelid = c.oid and tgname = x.tgname) <> x.tgenabled then
				execute format('alter table %s %s trigger %I', c.qname,
					case x.tgenabled when 'D' then 'disable' when 'A' then 'enable always'
						when 'R' then 'enable replica' else 'enable' end, x.tgname);
			end if;
			have := null;
		end loop;
	end loop;
end $lepis_sync$"#;

fn retired_match(shells: &[RelationName]) -> String {
	shells
		.iter()
		.map(|s| {
			let sch = crate::catalog::quote_literal(&s.schema);
			let t = crate::catalog::quote_literal(&s.table);
			format!(
				"(n.nspname = {sch} and left(k.relname, length({t}) + 16) = 'lepis_retired_' || {t} || '_c' \
				and substr(k.relname, length({t}) + 17) ~ '^[0-9]{{3,}}$')"
			)
		})
		.collect::<Vec<_>>()
		.join(" or ")
}

/// Drops the retired chunk tables of `shells` on a node (what a move's cleanup left, or a move
/// resumed without one), then says how many there were.
pub fn drop_retired_sql(shells: &[RelationName]) -> String {
	let m = retired_match(shells);
	format!(
		"create temp table if not exists lepis_retired_count (n bigint); truncate lepis_retired_count; \
		do $lepis_retired$ declare x text; c bigint; begin \
		select string_agg(k.oid::regclass::text, ', '), count(*) into x, c from pg_class k \
		join pg_namespace n on n.oid = k.relnamespace where k.relkind = 'r' and ({m}); \
		insert into lepis_retired_count values (c); \
		if x is not null then execute 'drop table ' || x; end if; end $lepis_retired$; \
		select n from lepis_retired_count"
	)
}

/// On one node holding every chunk of `shells`: the rows back in each shell's own heap, the chunk
/// tables (and retired ones) dropped, the shells' refusal gone. One `DO` block, in the caller's
/// transaction, the way back from chunked storage (`table.global`, `dissolve`). A table whose rows
/// would break a foreign key until its parent's are back waits for the parent (the block tries
/// again until none is left); the chunk tables go last, all in one DROP, since chunks refer to
/// each other's.
pub fn dechunk_sql(shells: &[RelationName]) -> String {
	let list = shells
		.iter()
		.map(|r| crate::catalog::quote_literal(&qualified(r)))
		.collect::<Vec<_>>()
		.join(", ");
	let m = retired_match(shells);
	format!(
		r#"do $lepis_dechunk$
declare
	pending text[] := array[{list}]::text[];
	all_shells text[] := pending;
	t text;
	c record;
	cols text;
	progressed bool;
	gone text;
begin
	foreach t in array all_shells loop
		execute format('lock table %s in access exclusive mode', t);
		execute format('alter table %s drop constraint if exists {SHELL_CONSTRAINT}', t);
	end loop;
	loop
		exit when cardinality(pending) = 0;
		progressed := false;
		foreach t in array pending loop
			begin
				select string_agg(quote_ident(attname), ', ' order by attnum) into cols from pg_attribute
				where attrelid = t::regclass and attnum > 0 and not attisdropped and attgenerated = '';
				for c in select i.inhrelid::regclass::text as name from pg_inherits i where i.inhparent = t::regclass loop
					execute format('insert into %s (%s) select %s from only %s', t, cols, cols, c.name);
				end loop;
				pending := array_remove(pending, t);
				progressed := true;
			exception when foreign_key_violation then
				null;
			end;
		end loop;
		if not progressed then
			raise exception 'Lepis cannot put the rows of % back: their foreign keys refer to each other', pending;
		end if;
	end loop;
	select string_agg(x.name, ', ') into gone from (
		select i.inhrelid::regclass::text as name from pg_inherits i
		where i.inhparent = any (select s::regclass from unnest(all_shells) s)
		union all
		select k.oid::regclass::text from pg_class k join pg_namespace n on n.oid = k.relnamespace
		where k.relkind = 'r' and ({m})) x;
	if gone is not null then
		execute 'drop table ' || gone;
	end if;
end $lepis_dechunk$"#
	)
}

/// Before a DROP TABLE of chunked tables, on each node: their chunk tables, live and retired, in
/// one DROP (so foreign keys among them never stand in the way), `CASCADE` when the client's
/// DROP said so. Run in the DROP's own transaction: if the DROP fails, they stay.
pub fn drop_chunks_sql(shells: &[RelationName], cascade: bool) -> String {
	let conds: Vec<String> = shells
		.iter()
		.map(|s| {
			let sch = crate::catalog::quote_literal(&s.schema);
			let t = crate::catalog::quote_literal(&s.table);
			format!(
				"(n.nspname = {sch} and ((left(k.relname, length({t}) + 2) = {t} || '_c' \
				and substr(k.relname, length({t}) + 3) ~ '^[0-9]{{3,}}$') \
				or (left(k.relname, length({t}) + 16) = 'lepis_retired_' || {t} || '_c' \
				and substr(k.relname, length({t}) + 17) ~ '^[0-9]{{3,}}$')))"
			)
		})
		.collect();
	format!(
		"do $lepis_drop$ declare x text; begin \
		select string_agg(k.oid::regclass::text, ', ') into x from pg_class k \
		join pg_namespace n on n.oid = k.relnamespace where k.relkind = 'r' and ({}); \
		if x is not null then execute 'drop table ' || x{}; end if; end $lepis_drop$",
		conds.join(" or "),
		if cascade { " || ' cascade'" } else { "" }
	)
}

// ---------------------------------------------------------------------------------------------
// Routed statements: `<t>` becomes `<t>_cNNN`.

/// Rewrites one statement so each sharded table in `map` names its chunk table instead, keeping
/// the name the statement used as the alias (so `t.col` still resolves). `resolve` maps a
/// RangeVar's schema and name to its relation (the analyzer's search-path rule).
///
/// Every RangeVar the parser reports for a mapped table must be rewritten; if one sits where the
/// walker does not reach, or a CTE shadows a mapped name, the statement is refused, never sent
/// half-rewritten. A name left as the shell's is never wrong, only slower: on the node, a read or
/// an UPDATE/DELETE through the shell reaches exactly that node's chunks, and an INSERT fails on
/// `lepis_shell_insert`.
pub fn rewrite(
	sql: &str,
	map: &HashMap<RelationName, u32>,
	resolve: &dyn Fn(&str, &str) -> RelationName,
) -> Result<String, String> {
	let to: HashMap<RelationName, To> = map
		.iter()
		.map(|(r, n)| (r.clone(), To::Chunk(*n)))
		.collect();
	rewrite_to(sql, &to, resolve)
}

/// `rewrite`, each relation in `map` named as another table (a join's temporary copy, join.rs),
/// the old name kept as its alias.
pub fn rename(
	sql: &str,
	map: &HashMap<RelationName, RelationName>,
	resolve: &dyn Fn(&str, &str) -> RelationName,
) -> Result<String, String> {
	let to: HashMap<RelationName, To> = map
		.iter()
		.map(|(r, t)| (r.clone(), To::Table(t.clone())))
		.collect();
	rewrite_to(sql, &to, resolve)
}

/// What a renamed relation becomes.
enum To {
	Chunk(u32),
	Table(RelationName),
}

fn rewrite_to(
	sql: &str,
	map: &HashMap<RelationName, To>,
	resolve: &dyn Fn(&str, &str) -> RelationName,
) -> Result<String, String> {
	let mut parsed = pg_query::parse(sql).map_err(|e| e.to_string())?;
	let mut expected = 0usize;
	let mut ctes: HashSet<String> = HashSet::new();
	for (n, _, _, _) in parsed.protobuf.nodes() {
		match n {
			pg_query::NodeRef::RangeVar(rv) => {
				if map.contains_key(&resolve(&rv.schemaname, &rv.relname)) {
					expected += 1;
				}
			}
			pg_query::NodeRef::CommonTableExpr(c) => {
				ctes.insert(c.ctename.clone());
			}
			_ => {}
		}
	}
	if map.keys().any(|r| ctes.contains(&r.table)) {
		return Err("a WITH query has the name of a chunked table".into());
	}
	if expected == 0 {
		return Ok(sql.to_string());
	}
	let mut w = Renamer {
		map,
		resolve,
		done: 0,
	};
	for s in &mut parsed.protobuf.stmts {
		if let Some(n) = s.stmt.as_deref_mut() {
			w.node(n);
		}
	}
	if w.done != expected {
		return Err(format!(
			"Lepis could not name the chunk for every use of a chunked table ({} of {expected})",
			w.done
		));
	}
	pg_query::deparse(&parsed.protobuf).map_err(|e| e.to_string())
}

struct Renamer<'a> {
	map: &'a HashMap<RelationName, To>,
	resolve: &'a dyn Fn(&str, &str) -> RelationName,
	done: usize,
}

impl Renamer<'_> {
	/// Renames `rv` when it is mapped; the chunk number it took.
	fn range_var(&mut self, rv: &mut RangeVar) -> Option<u32> {
		let to = self.map.get(&(self.resolve)(&rv.schemaname, &rv.relname))?;
		if rv.alias.is_none() {
			rv.alias = Some(pg_query::protobuf::Alias {
				aliasname: rv.relname.clone(),
				colnames: Vec::new(),
			});
		}
		self.done += 1;
		match to {
			To::Chunk(number) => {
				rv.relname = chunk_table(&rv.relname, *number);
				Some(*number)
			}
			To::Table(t) => {
				rv.schemaname = t.schema.clone();
				rv.relname = t.table.clone();
				None
			}
		}
	}

	fn opt(&mut self, n: &mut Option<Box<Node>>) {
		if let Some(n) = n.as_deref_mut() {
			self.node(n);
		}
	}

	fn list(&mut self, v: &mut [Node]) {
		for n in v {
			self.node(n);
		}
	}

	fn with(&mut self, w: &mut Option<WithClause>) {
		if let Some(w) = w {
			self.list(&mut w.ctes);
		}
	}

	fn select(&mut self, s: &mut SelectStmt) {
		self.with(&mut s.with_clause);
		self.list(&mut s.target_list);
		self.list(&mut s.from_clause);
		self.opt(&mut s.where_clause);
		self.list(&mut s.group_clause);
		self.opt(&mut s.having_clause);
		self.list(&mut s.window_clause);
		self.list(&mut s.values_lists);
		self.list(&mut s.sort_clause);
		self.list(&mut s.distinct_clause);
		self.opt(&mut s.limit_offset);
		self.opt(&mut s.limit_count);
		if let Some(l) = s.larg.as_deref_mut() {
			self.select(l);
		}
		if let Some(r) = s.rarg.as_deref_mut() {
			self.select(r);
		}
	}

	fn node(&mut self, n: &mut Node) {
		let Some(e) = n.node.as_mut() else {
			return;
		};
		match e {
			NodeEnum::RangeVar(rv) => {
				self.range_var(rv);
			}
			NodeEnum::SelectStmt(s) => self.select(s),
			NodeEnum::InsertStmt(i) => {
				self.with(&mut i.with_clause);
				let number = i.relation.as_mut().and_then(|rv| self.range_var(rv));
				self.opt(&mut i.select_stmt);
				if let Some(oc) = i.on_conflict_clause.as_deref_mut() {
					if let (Some(n), Some(infer)) = (number, oc.infer.as_deref_mut())
						&& !infer.conname.is_empty()
					{
						infer.conname = object_name(&infer.conname, n);
					}
					self.list(&mut oc.target_list);
					self.opt(&mut oc.where_clause);
				}
				self.list(&mut i.returning_list);
			}
			NodeEnum::UpdateStmt(u) => {
				self.with(&mut u.with_clause);
				if let Some(rv) = u.relation.as_mut() {
					self.range_var(rv);
				}
				self.list(&mut u.target_list);
				self.list(&mut u.from_clause);
				self.opt(&mut u.where_clause);
				self.list(&mut u.returning_list);
			}
			NodeEnum::DeleteStmt(d) => {
				self.with(&mut d.with_clause);
				if let Some(rv) = d.relation.as_mut() {
					self.range_var(rv);
				}
				self.list(&mut d.using_clause);
				self.opt(&mut d.where_clause);
				self.list(&mut d.returning_list);
			}
			NodeEnum::ExplainStmt(x) => self.opt(&mut x.query),
			NodeEnum::CommonTableExpr(c) => self.opt(&mut c.ctequery),
			NodeEnum::JoinExpr(j) => {
				self.opt(&mut j.larg);
				self.opt(&mut j.rarg);
				self.opt(&mut j.quals);
			}
			NodeEnum::RangeSubselect(r) => self.opt(&mut r.subquery),
			NodeEnum::SubLink(s) => {
				self.opt(&mut s.testexpr);
				self.opt(&mut s.subselect);
			}
			NodeEnum::ResTarget(r) => self.opt(&mut r.val),
			NodeEnum::AExpr(a) => {
				self.opt(&mut a.lexpr);
				self.opt(&mut a.rexpr);
			}
			NodeEnum::BoolExpr(b) => self.list(&mut b.args),
			NodeEnum::FuncCall(f) => {
				self.list(&mut f.args);
				self.opt(&mut f.agg_filter);
			}
			NodeEnum::TypeCast(t) => self.opt(&mut t.arg),
			NodeEnum::NullTest(t) => self.opt(&mut t.arg),
			NodeEnum::BooleanTest(t) => self.opt(&mut t.arg),
			NodeEnum::CaseExpr(c) => {
				self.opt(&mut c.arg);
				self.list(&mut c.args);
				self.opt(&mut c.defresult);
			}
			NodeEnum::CaseWhen(w) => {
				self.opt(&mut w.expr);
				self.opt(&mut w.result);
			}
			NodeEnum::CoalesceExpr(c) => self.list(&mut c.args),
			NodeEnum::MinMaxExpr(m) => self.list(&mut m.args),
			NodeEnum::RowExpr(r) => self.list(&mut r.args),
			NodeEnum::AArrayExpr(a) => self.list(&mut a.elements),
			NodeEnum::List(l) => self.list(&mut l.items),
			NodeEnum::SortBy(s) => self.opt(&mut s.node),
			_ => {}
		}
	}
}

/// The COPY … FROM as it runs on a node for one chunk: into that chunk table, its columns and
/// options kept.
pub fn copy_into_sql(sql: &str, into: &RelationName) -> Option<String> {
	let mut parsed = pg_query::parse(sql).ok()?;
	let stmt = parsed.protobuf.stmts.first_mut()?.stmt.as_deref_mut()?;
	let Some(NodeEnum::CopyStmt(copy)) = stmt.node.as_mut() else {
		return None;
	};
	if !copy.is_from {
		return None;
	}
	let rv = copy.relation.as_mut()?;
	rv.schemaname = into.schema.clone();
	rv.relname = into.table.clone();
	rv.alias = None;
	pg_query::deparse(&parsed.protobuf).ok()
}

/// A use of a chunked table Postgres would answer wrongly through the shell, as a sentence:
/// `ONLY <t>` (the shell holds no rows, its chunks are children). `chunked` decides by relation.
pub fn only_refusal(
	sql: &str,
	chunked: &dyn Fn(&RelationName) -> bool,
	resolve: &dyn Fn(&str, &str) -> RelationName,
) -> Option<String> {
	let parsed = pg_query::parse(sql).ok()?;
	for (n, _, _, _) in parsed.protobuf.nodes() {
		if let pg_query::NodeRef::RangeVar(rv) = n {
			let r = resolve(&rv.schemaname, &rv.relname);
			if !rv.inh && chunked(&r) {
				return Some(format!(
					"ONLY {r} would see none of its rows: they are in its chunk tables (Lepis keeps it chunked)"
				));
			}
		}
	}
	None
}

/// `COPY <t> [(cols)] TO …` of a chunked table as `COPY (SELECT cols FROM <t>) TO …`: COPY TO
/// of a table reads that table alone, never its children, so on the shell it would copy
/// nothing. None when `sql` is not such a COPY.
pub fn copy_to_sql(
	sql: &str,
	chunked: &dyn Fn(&RelationName) -> bool,
	resolve: &dyn Fn(&str, &str) -> RelationName,
) -> Option<String> {
	use pg_query::protobuf::{AStar, ColumnRef, ResTarget};
	let mut parsed = pg_query::parse(sql).ok()?;
	let stmt = parsed.protobuf.stmts.first_mut()?.stmt.as_deref_mut()?;
	let Some(NodeEnum::CopyStmt(copy)) = stmt.node.as_mut() else {
		return None;
	};
	if copy.is_from {
		return None;
	}
	let rv = copy.relation.take()?;
	if !chunked(&resolve(&rv.schemaname, &rv.relname)) {
		return None;
	}
	let col = |fields: Vec<Node>| Node {
		node: Some(NodeEnum::ResTarget(Box::new(ResTarget {
			val: Some(Box::new(Node {
				node: Some(NodeEnum::ColumnRef(ColumnRef {
					fields,
					location: -1,
				})),
			})),
			location: -1,
			..Default::default()
		}))),
	};
	let target_list: Vec<Node> = if copy.attlist.is_empty() {
		vec![col(vec![Node {
			node: Some(NodeEnum::AStar(AStar {})),
		}])]
	} else {
		copy.attlist.drain(..).map(|a| col(vec![a])).collect()
	};
	let select = SelectStmt {
		target_list,
		from_clause: vec![Node {
			node: Some(NodeEnum::RangeVar(rv)),
		}],
		limit_option: pg_query::protobuf::LimitOption::Default as i32,
		op: pg_query::protobuf::SetOperation::SetopNone as i32,
		..Default::default()
	};
	copy.query = Some(Box::new(Node {
		node: Some(NodeEnum::SelectStmt(Box::new(select))),
	}));
	pg_query::deparse(&parsed.protobuf).ok()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{NodeId, Range, Strategy};
	use crate::hash::KeyType;

	fn ks(ranges: Vec<Range>) -> Keyspace {
		Keyspace {
			name: "tenant".into(),
			strategy: Strategy::Hash,
			key_type: KeyType::Int8,
			seed: 0,
			ranges,
			pins: HashMap::new(),
			chunks: Vec::new(),
			second: None,
		}
	}

	fn rel(t: &str) -> RelationName {
		RelationName {
			schema: "public".into(),
			table: t.into(),
		}
	}

	fn public(s: &str, t: &str) -> RelationName {
		RelationName {
			schema: if s.is_empty() { "public" } else { s }.into(),
			table: t.into(),
		}
	}

	#[test]
	fn counts() {
		assert_eq!(chunk_count_for(None), 64);
		assert_eq!(chunk_count_for(Some(100 << 30)), 64);
		assert_eq!(chunk_count_for(Some(1 << 40)), 128);
		assert_eq!(chunk_count_for(Some(u64::MAX)), MAX_CHUNKS);
		assert!(even_chunks(48).is_err());
		assert!(even_chunks(0).is_err());
	}

	#[test]
	fn even_chunks_cover_the_space_and_match_the_proof() {
		let c = even_chunks(4).unwrap();
		assert_eq!(c[0].lo, i64::MIN);
		assert_eq!(c[0].hi, -4611686018427387905);
		assert_eq!(c[1].lo, -4611686018427387904);
		assert_eq!(c[1].hi, -1);
		assert_eq!(c[3].hi, i64::MAX);
		let k = ks(crate::catalog::Keyspace::even_ranges(
			2,
			&[NodeId(1), NodeId(2)],
		));
		// even_ranges' cut is two values off the exact half; chunks_for snaps onto it.
		assert!(validate(&c, &k).is_err());
		let a = chunks_for(4, &k).unwrap();
		assert_eq!(a[1].hi, k.ranges[0].hi);
		for n in [2, 3, 5, 7] {
			let k = ks(crate::catalog::Keyspace::even_ranges(
				n,
				&[NodeId(1), NodeId(2)],
			));
			let a = chunks_for(64, &k).unwrap();
			assert_eq!(a.len(), 64);
			for r in &k.ranges {
				assert!(!chunks_in(&a, r.lo, r.hi).is_empty());
			}
		}
		assert!(
			chunks_for(
				2,
				&ks(crate::catalog::Keyspace::even_ranges(5, &[NodeId(1)]))
			)
			.is_err()
		);
		assert_eq!(chunk_of(&c, -1).number, 1);
		assert_eq!(chunk_of(&c, 0).number, 2);
		assert_eq!(chunks_in(&c, i64::MIN, -1).len(), 2);
		// A range that cuts a chunk is refused.
		let bad = ks(crate::catalog::Keyspace::even_ranges(3, &[NodeId(1)]));
		assert!(validate(&c, &bad).is_err());
	}

	#[test]
	fn split_halves_a_chunk() {
		let c = even_chunks(4).unwrap()[0];
		let (a, b) = split(c, 4).unwrap();
		assert_eq!((a.lo, a.hi), (i64::MIN, -6917529027641081857));
		assert_eq!((b.number, b.lo, b.hi), (4, -6917529027641081856, c.hi));
		assert!(
			split(
				Chunk {
					number: 9,
					lo: 5,
					hi: 5
				},
				10
			)
			.is_none()
		);
		let (x, y) = split(
			Chunk {
				number: 1,
				lo: 4,
				hi: 5,
			},
			2,
		)
		.unwrap();
		assert_eq!((x.lo, x.hi, y.lo, y.hi), (4, 4, 5, 5));
	}

	#[test]
	fn ddl() {
		let k = ks(crate::catalog::Keyspace::even_ranges(1, &[NodeId(1)]));
		let c = even_chunks(4).unwrap()[1];
		let s = create_sql(&rel("cust"), &KeySql::from("\"id\""), &k, c, 180_000).unwrap();
		assert!(
			s.contains("create table \"public\".\"cust_c001\" (like \"public\".\"cust\""),
			"{s}"
		);
		assert!(
			s.contains("hashint8extended(\"id\", 0) between -4611686018427387904 and -1"),
			"{s}"
		);
		assert!(shell_sql(&rel("cust")).ends_with("lepis_shell_insert check (false) no inherit"));
		assert!(shell_sql(&rel("cust")).contains("drop constraint if exists lepis_shell_insert"));
		let r = retire_sql(&rel("cust"), 1);
		assert!(r.contains("rename to \"lepis_retired_cust_c001\""), "{r}");
		assert_eq!(chunk_table("t", 1234), "t_c1234");
	}

	fn map() -> HashMap<RelationName, u32> {
		HashMap::from([(rel("cust"), 7), (rel("ord"), 7)])
	}

	#[test]
	fn rewrites_every_use() {
		let m = map();
		let r = |sql: &str| rewrite(sql, &m, &public).unwrap();
		assert_eq!(
			r("select cust.name from cust where id = 5"),
			"SELECT cust.name FROM cust_c007 cust WHERE id = 5"
		);
		assert_eq!(
			r("select c.name from public.cust c join ord o on o.cust_id = c.id where c.id = $1"),
			"SELECT c.name FROM public.cust_c007 c JOIN ord_c007 o ON o.cust_id = c.id WHERE c.id = $1"
		);
		assert_eq!(
			r(
				"insert into cust values (5, 'a') on conflict (id) do update set name = excluded.name returning *"
			),
			"INSERT INTO cust_c007 AS cust VALUES (5, 'a') ON CONFLICT (id) DO UPDATE SET name = excluded.name RETURNING *"
		);
		assert_eq!(
			r(
				"update cust set name = 'x' where id = 5 and exists (select 1 from ord where cust_id = 5)"
			),
			"UPDATE cust_c007 cust SET name = 'x' WHERE id = 5 AND EXISTS (SELECT 1 FROM ord_c007 ord WHERE cust_id = 5)"
		);
		assert_eq!(
			r("delete from cust where id = 5"),
			"DELETE FROM cust_c007 cust WHERE id = 5"
		);
		assert_eq!(
			r("with x as (select * from ord where cust_id = 5) select * from x"),
			"WITH x AS (SELECT * FROM ord_c007 ord WHERE cust_id = 5) SELECT * FROM x"
		);
		assert_eq!(r("select 1 from other"), "select 1 from other");
		assert_eq!(
			r("select coalesce((select max(n) from ord where cust_id = 5), 0)"),
			"SELECT COALESCE((SELECT max(n) FROM ord_c007 ord WHERE cust_id = 5), 0)"
		);
	}

	#[test]
	fn on_conflict_on_constraint_names_the_chunks_copy() {
		let m = map();
		let r = rewrite(
			"insert into cust values (5, 'a') on conflict on constraint cust_pkey do nothing",
			&m,
			&public,
		)
		.unwrap();
		assert!(r.contains("ON CONSTRAINT cust_pkey_c007"), "{r}");
		assert_eq!(object_name("ab", 7), "ab_c007");
		let long = "x".repeat(70);
		assert_eq!(object_name(&long, 12).len(), 63);
		assert!(object_name(&long, 12).ends_with("_c012"));
	}

	#[test]
	fn only_and_copy_to_on_a_chunked_table() {
		let ch = |r: &RelationName| r.table == "cust";
		assert!(only_refusal("select * from only cust", &ch, &public).is_some());
		assert!(only_refusal("select * from cust", &ch, &public).is_none());
		assert!(only_refusal("select * from only other", &ch, &public).is_none());
		assert_eq!(
			copy_to_sql(
				"copy cust (id, name) to stdout with (format csv)",
				&ch,
				&public
			)
			.unwrap(),
			"COPY (SELECT id, name FROM cust) TO STDOUT CSV"
		);
		assert_eq!(
			copy_to_sql("copy public.cust to stdout", &ch, &public).unwrap(),
			"COPY (SELECT * FROM public.cust) TO STDOUT"
		);
		assert!(copy_to_sql("copy cust from stdin", &ch, &public).is_none());
		assert_eq!(
			copy_into_sql(
				"copy public.cust (id, name) from stdin with (format csv)",
				&rel("cust_c007")
			)
			.unwrap(),
			"COPY public.cust_c007(id, name) FROM STDIN CSV"
		);
		assert!(copy_to_sql("copy other to stdout", &ch, &public).is_none());
	}

	#[test]
	fn sync_and_drop_sql_name_the_shell() {
		let s = sync_sql(&rel("cust"));
		assert!(
			s.contains("to_regclass('\"public\".\"cust\"')"),
			"{}",
			&s[..200]
		);
		let d = drop_chunks_sql(&[rel("cust")], true);
		assert!(d.contains("|| ' cascade'"));
		assert!(d.contains("'lepis_retired_'"));
	}

	#[test]
	fn refuses_what_it_cannot_rewrite_whole() {
		let m = map();
		assert!(rewrite("with cust as (select 1) select * from cust", &m, &public).is_err());
		// Where neither the parser's node list nor the walker reaches, the name stays the shell's:
		// on the node that is today's semantics (a read sees the node's rows, an INSERT fails).
		let r = rewrite(
			"select * from xmltable('/a' passing (select 1 from cust where id = 5) columns a int)",
			&m,
			&public,
		);
		assert!(r.unwrap().contains("from cust where"));
	}
}
