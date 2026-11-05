#!/usr/bin/env bash
# Proof: chunk tables under an inheritance shell, on two stock Postgres
# nodes in containers. Decides whether the design holds; each check fails the run.
#
#   bash scripts/chunk-proof.sh        postgres:18 (STACK_ENGINE=podman for podman)
#
# 1. local.sql: the shell's node-local semantics (reads, refused INSERTs, UPDATE/DELETE/RETURNING
#    through it, fences, FKs, triggers, views, DDL recursion).
# 2. A chunk split in place under 8 concurrent writers (pgbench through a "router" that reads a
#    chunk map and retries a fenced or vanished chunk): no lost or duplicate row or update.
# 3. A chunk move by logical replication of the chunk table itself, writes through the shell,
#    then the cutover: the source's copy leaves the shell and is renamed and fenced, the
#    target's joins it, and a stale writer on the source fails.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/chunk-proof"
engine="${STACK_ENGINE:-docker}"
major="${1:-18}"
run="lepis-chunk-$$"
a="$run-a"
b="$run-b"
cleanup() {
	"$engine" rm -fv "$a" "$b" >/dev/null 2>&1 || true
	"$engine" network rm "$run" >/dev/null 2>&1 || true
}
trap cleanup EXIT
"$engine" network create "$run" >/dev/null
for n in "$a" "$b"; do
	"$engine" run -d --name "$n" --network "$run" -e POSTGRES_PASSWORD=x \
		"docker.io/library/postgres:$major" -c wal_level=logical >/dev/null
done
for n in "$a" "$b"; do
	for _ in $(seq 60); do
		"$engine" exec "$n" pg_isready -U postgres -q 2>/dev/null && break
		sleep 1
	done
done
sleep 2
q() { "$engine" exec -i "$1" psql -U postgres -v ON_ERROR_STOP=1 -qtA "${@:2}"; }

echo "1. node-local semantics"
q "$a" -c "create database l" >/dev/null
q "$a" -d l <"$here/local.sql" | grep -x "LOCAL OK"

echo "2. split under concurrent writes"
q "$a" <"$here/split-setup.sql" >/dev/null
"$engine" cp "$here/writer.pgb" "$a:/tmp/writer.pgb"
"$engine" exec "$a" pgbench -U postgres -n -c 8 -T 15 -f /tmp/writer.pgb postgres >"/tmp/$run.pgb" 2>&1 &
writer=$!
sleep 3
q "$a" <"$here/split.sql" >/dev/null
wait "$writer"
grep -q "number of failed transactions: 0 " "/tmp/$run.pgb"
read -r total expect sumv upds dups retries shell map <<<"$(q "$a" -F ' ' -c "select
	(select count(*) from acct), (select count(*) from pool) + (select count(*) from ins_log),
	(select sum(v) from acct), (select count(*) from upd_log),
	(select count(*) from (select id from acct group by id having count(*) > 1) d),
	(select count(*) from retries), (select count(*) from only acct),
	(select string_agg(tab, ',' order by lo) from chunkmap)")"
rm -f "/tmp/$run.pgb"
echo "   rows $total/$expect, updates $sumv/$upds, duplicates $dups, stale retries $retries, map $map"
[ "$total" = "$expect" ] && [ "$sumv" = "$upds" ] && [ "$dups" = 0 ] && [ "$shell" = 0 ]
[ "$map" = "acct_c000a,acct_c000b" ]

echo "3. move of one chunk table"
q "$a" -c "create publication lm for table only acct_c000b" >/dev/null
sed "s/:SOURCE/$a/" "$here/move-target.sql" | q "$b" >/dev/null 2>&1
before="$(q "$a" -c "select count(*) from acct")"
q "$a" -c "update acct set v = v + 100 where id in (select id from acct_c000b limit 50)" >/dev/null
q "$a" -c "delete from acct where id in (select id from acct_c000b limit 7)" >/dev/null
src=""
for _ in $(seq 60); do
	src="$(q "$a" -c "select count(*), sum(v) from acct_c000b")"
	[ "$src" = "$(q "$b" -c "select count(*), sum(v) from acct_c000b")" ] && break
	src=""
	sleep 1
done
[ -n "$src" ]
q "$a" <"$here/move-cutover.sql" >/dev/null
q "$b" -c "drop subscription lm" -c "alter table acct_c000b inherit acct" >/dev/null 2>&1
left="$(q "$a" -c "select count(*) from acct")"
arrived="$(q "$b" -c "select count(*) from acct")"
echo "   source shell $before -> $left, target shell $arrived"
[ $((left + arrived)) = $((before - 7)) ]
echo "chunk proof: OK (postgres:$major)"
