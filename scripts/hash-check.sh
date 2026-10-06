#!/usr/bin/env bash
# The differential check for the shard hash (L5): a real Postgres computes
# hashint8extended & co. over random values of every key type, and examples/hashcheck.rs
# recomputes each one with the Rust port. Any difference fails.
#
#   bash scripts/hash-check.sh                     1,000,000 values per type on postgres:18
#   bash scripts/hash-check.sh 10000000 17         10M per type on postgres:17
#   bash scripts/hash-check.sh 300 18 --fixture    rewrite tests/fixtures/hash.tsv instead
#
# The SQL side uses exactly the expressions KeyType::sql_expression gives a node of that major
# (the portable forms on 17, the native functions on 18, both on 18), so what is checked is
# what a fence will run. Run from anywhere. The Postgres container is named for this run and
# removed after; it never touches any other container.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
stack="$(cd "$here/.." && pwd)"
engine="${STACK_ENGINE:-docker}"
n="${1:-1000000}"
major="${2:-18}"
mode="${3:-check}"
name="lepis-hashcheck-$$"

cleanup() { "$engine" rm -fv "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT

"$engine" run -d --name "$name" -e POSTGRES_PASSWORD=x "docker.io/library/postgres:$major" >/dev/null
# pg_isready answers during the init script's temporary server; wait for a query to work twice.
for _ in 1 2; do
	until "$engine" exec "$name" psql -U postgres -Atc 'select 1' >/dev/null 2>&1; do sleep 1; done
	sleep 2
done

# Seeds: a fifth are zero (Postgres skips the seed mix for 0, a separate path), the rest random.
sql="$(cat <<SQL
\set ON_ERROR_STOP on
select current_setting('server_version_num')::int >= 180000 as pg18 \gset
set timezone = 'America/St_Johns';
create function pg_temp.r8() returns int8 language sql as
	\$\$ select (floor(random() * 4294967296)::int8 << 32) | floor(random() * 4294967296)::int8 \$\$;
create function pg_temp.ri(lo int8, hi int8) returns int8 language sql as
	\$\$ select lo + floor(random() * (hi - lo + 1)::float8)::int8 \$\$;
create function pg_temp.seed() returns int8 language sql as
	\$\$ select case when random() < 0.2 then 0 else pg_temp.r8() end \$\$;
create function pg_temp.str() returns text language sql as \$\$
	select coalesce(string_agg(chr(case
		when random() < 0.7 then pg_temp.ri(32, 126)
		when random() < 0.5 then pg_temp.ri(128, 55295)
		else pg_temp.ri(57344, 1114111) end::int), ''), '')
	from generate_series(1, pg_temp.ri(0, 40)::int)
\$\$;
create function pg_temp.digits(n int, zeros float8) returns text language sql as \$\$
	select coalesce(string_agg(case when random() < zeros then '0'
		else floor(random() * 10)::int::text end, ''), '')
	from generate_series(1, n)
\$\$;
-- A numeric as somebody might TYPE it: signs, leading zeros, trailing zeros past the point,
-- exponents, runs of zero digits (which become whole zero base-10000 digits), underscores
-- and hex. Half are sent as typed, half as Postgres prints them.
create function pg_temp.num(fancy bool) returns text language plpgsql as \$\$
declare
	z float8 := case when random() < 0.3 then 0.7 else 0.1 end;
	ip text := pg_temp.digits(pg_temp.ri(0, 24)::int, z);
	fp text := pg_temp.digits(pg_temp.ri(0, 24)::int, z);
	s text;
begin
	if fancy and random() < 0.05 then
		s := '0x' || to_hex(pg_temp.r8());
		if random() < 0.5 then s := s || to_hex(pg_temp.r8()); end if;
		return case when random() < 0.5 then '-' else '' end || s;
	end if;
	if ip = '' and fp = '' then ip := '0'; end if;
	if fancy and random() < 0.1 and length(ip) >= 2 then
		ip := left(ip, 1) || '_' || substr(ip, 2);
	end if;
	s := ip;
	if fp <> '' or random() < 0.1 then
		s := s || '.' || fp || repeat('0', pg_temp.ri(0, 5)::int);
	end if;
	if random() < 0.1 then s := repeat('0', pg_temp.ri(1, 4)::int) || s; end if;
	if random() < 0.3 then
		s := s || (array['e', 'E'])[pg_temp.ri(1, 2)::int] || (array['', '+', '-'])[pg_temp.ri(1, 3)::int]
			|| pg_temp.ri(0, 300);
	end if;
	s := (array['', '', '', '-', '-', '+'])[pg_temp.ri(1, 6)::int] || s;
	if random() < 0.05 then s := ' ' || s; end if;
	if random() < 0.05 then s := s || '  '; end if;
	return case when random() < 0.5 then s else s::numeric::text end;
end
\$\$;
create temp table numeric_edge(v text);
insert into numeric_edge values ('0'), ('-0'), ('0.0000'), ('0e100'), ('1'), ('1.0'), ('1.00'),
	('-1'), ('10'), ('100'), ('1000'), ('9999'), ('10000'), ('10001'), ('0.1'), ('0.0001'),
	('0.00001'), ('0.00010'), ('-1.5'), ('123456789012345678901234567890.123456789'), ('1e-6'),
	('1e6'), ('.5'), ('5.'), ('  42  '), ('+7'), ('NaN'), ('nan'), ('1e131071'), ('1e-16383'),
	(repeat('9', 1000)), ('0.' || repeat('0', 16000) || '1'), ('99990000.00009999');
insert into numeric_edge values ('Infinity'), ('-Infinity'), ('inf'), ('-INF');
insert into numeric_edge values ('1_000'), ('0x1F'), ('0o17'), ('0b101'), ('0x_ff'),
	('1_000.000_1'), ('1e1_0'), ('-0X' || repeat('f', 300));
create temp table bpchar_edge(v text);
insert into bpchar_edge values (''), (' '), ('   '), ('a'), ('a '), ('abcdefghijk '),
	('abcdefghijkl'), ('abcdefghijkl '), ('abcdefghijklm   '), ('é  '), (E'a\t'), (E'\t '), (' a');

copy (
	with edge(v) as (values (0::int8), (1), (-1), (2147483647), (-2147483648), (2147483648),
		(-2147483649), (9223372036854775807), (-9223372036854775808), (4294967295), (4294967296))
	select 'int8', v::text, s, hashint8extended(v, s)
		from edge, (values (0::int8), (1), (-1), (42)) seeds(s)
	union all select 'int4', v::int4::text, s, hashint4extended(v::int4, s)
		from edge, (values (0::int8), (7)) seeds(s) where v between -2147483648 and 2147483647
	union all select 'int2', v::int2::text, s, hashint2extended(v::int2, s)
		from (values (0::int8), (1), (-1), (32767), (-32768)) e(v), (values (0::int8), (7)) seeds(s)
	union all select 'texthex', encode(convert_to(v, 'UTF8'), 'hex'), s, hashtextextended(v, s)
		from (values (''), ('a'), ('abcdefghijk'), ('abcdefghijkl'), ('abcdefghijklm'), ('é'))
			e(v), (values (0::int8), (7)) seeds(s)
	union all select 'timestamp', v::text, s, timestamp_hash_extended(v, s)
		from (values (timestamp 'infinity'), (timestamp '-infinity'), (timestamp '2000-01-01'))
			e(v), (values (0::int8), (7)) seeds(s)
) to stdout;

copy (select 'int8', v, s, hashint8extended(v, s) from
	(select pg_temp.r8() v, pg_temp.seed() s from generate_series(1, $n)) x) to stdout;
copy (select 'int4', v, s, hashint4extended(v, s) from
	(select pg_temp.ri(-2147483648, 2147483647)::int4 v, pg_temp.seed() s from generate_series(1, $n)) x) to stdout;
copy (select 'int2', v, s, hashint2extended(v, s) from
	(select pg_temp.ri(-32768, 32767)::int2 v, pg_temp.seed() s from generate_series(1, greatest($n / 10, 1))) x) to stdout;
copy (select 'texthex', encode(convert_to(v, 'UTF8'), 'hex'), s, hashtextextended(v, s) from
	(select pg_temp.str() v, pg_temp.seed() s from generate_series(1, $n)) x) to stdout;
copy (select 'uuid', v, s, uuid_hash_extended(v, s) from
	(select md5(random()::text || g)::uuid v, pg_temp.seed() s from generate_series(1, $n) g) x) to stdout;
copy (select 'date', v, s, hashint4extended(v - date '2000-01-01', s) from
	(select date '2000-01-01' + pg_temp.ri(-700000, 2000000)::int v, pg_temp.seed() s
		from generate_series(1, $n)) x) to stdout;
copy (select 'timestamp', v, s, timestamp_hash_extended(v, s) from
	(select timestamp '2000-01-01' + pg_temp.ri(-60000000000000000, 200000000000000000) * interval '1 microsecond' v,
		pg_temp.seed() s from generate_series(1, $n)) x) to stdout;
copy (select 'timestamptz', v, s, timestamp_hash_extended(v at time zone 'UTC', s) from
	(select timestamptz '2000-01-01 00:00+00' + pg_temp.ri(-60000000000000000, 200000000000000000) * interval '1 microsecond' v,
		pg_temp.seed() s from generate_series(1, $n)) x) to stdout;
copy (
	select 'numeric', v, s, hash_numeric_extended(v::numeric, s)
		from numeric_edge, (values (0::int8), (7), (-1)) seeds(s)
	union all select 'numericbin', encode(numeric_send(v::numeric), 'hex'), s, hash_numeric_extended(v::numeric, s)
		from numeric_edge, (values (0::int8), (7), (-1)) seeds(s)
	union all select 'bpcharhex', encode(bpcharsend(v::bpchar), 'hex'), s, hashbpcharextended(v::bpchar, s)
		from bpchar_edge, (values (0::int8), (7), (-1)) seeds(s)
) to stdout;
copy (select 'numeric', v, s, hash_numeric_extended(v::numeric, s) from
	(select pg_temp.num(true) v, pg_temp.seed() s from generate_series(1, $n)) x) to stdout;
copy (select 'numericbin', encode(numeric_send(v), 'hex'), s, hash_numeric_extended(v, s) from
	(select pg_temp.num(true)::numeric v, pg_temp.seed() s from generate_series(1, $n)) x) to stdout;
-- A character value padded as a char(n) column pads it, sometimes by a real char(20).
copy (select 'bpcharhex', encode(bpcharsend(v), 'hex'), s, hashbpcharextended(v, s) from
	(select case when random() < 0.2 and char_length(t) <= 20 then t::char(20)
		else rpad(t, char_length(t) + pg_temp.ri(0, 5)::int)::bpchar end v, pg_temp.seed() s
	from (select pg_temp.str() || repeat(' ', pg_temp.ri(0, 3)::int) t
		from generate_series(1, $n)) y) x) to stdout;
\if :pg18
copy (select 'bytea', encode(v, 'hex'), s, hashbyteaextended(v, s) from
	(select decode(substr(md5(random()::text) || md5(random()::text), 1, pg_temp.ri(0, 32)::int * 2), 'hex') v,
		pg_temp.seed() s from generate_series(1, $n) g) x) to stdout;
copy (select 'date', v, s, hashdateextended(v, s) from
	(select case when g % 1000 = 0 then date 'infinity' when g % 1000 = 1 then date '-infinity'
		else date '2000-01-01' + pg_temp.ri(-700000, 2000000)::int end v, pg_temp.seed() s
		from generate_series(1, $n) g) x) to stdout;
copy (select 'timestamptz', v, s, timestamptz_hash_extended(v, s) from
	(select timestamptz '2000-01-01 00:00+00' + pg_temp.ri(-60000000000000000, 200000000000000000) * interval '1 microsecond' v,
		pg_temp.seed() s from generate_series(1, $n)) x) to stdout;
\endif
SQL
)"

echo "postgres:$major, $n values per type" >&2
run_sql() {
	"$engine" exec -i "$name" psql -U postgres -q -X -v ON_ERROR_STOP=1 <<<"$sql" \
		| sed 's/^bytea\t/bytea\t\\x/'
}

if [ "$mode" = "--fixture" ]; then
	out="$here/../tests/fixtures/hash.tsv"
	{
		echo "# Written by scripts/hash-check.sh $n $major --fixture: type, value, seed, the hash Postgres returned."
		echo "# $("$engine" exec "$name" psql -U postgres -Atc 'select version()')"
		echo "# Lines over 512 bytes (the thousand-digit numerics) are checked by the run, not kept here."
		run_sql | awk 'length($0) <= 512'
	} >"$out"
	echo "wrote $(grep -vc '^#' "$out") lines to $out" >&2
	exit 0
fi

run_sql | STACK_DEV_ARGS="-i" bash "$stack/scripts/dev.sh" \
	cargo run -q --release -p snout-lepis --example hashcheck
