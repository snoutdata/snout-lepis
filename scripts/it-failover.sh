#!/usr/bin/env bash
# L21's failover drill (Phase 14d) against real Postgres: two copies, a guard beside each, and a
# witness, under a writer that records every commit Postgres acknowledged. Each scenario starts
# from a fresh cluster, breaks one thing, and then checks the two promises:
#
#   - no acknowledged commit is missing from the copy the witness names as the primary;
#   - never were both copies able to acknowledge a commit at once.
#
#   bash scripts/it-failover.sh                     every scenario, postgres:18
#   bash scripts/it-failover.sh kill-primary cut-primary
#
# Scenarios: kill-primary (the primary's machine dies), cut-primary (the primary loses the
# standby and the witness, clients still reach it), cut-replication (the standby alone is cut
# off: commits go on without it, no failover), kill-witness (nothing changes), standby-and-witness
# (both gone: commits stop, none lost), async-then-cut (the primary has gone on without the
# standby, then loses the witness: it stops, and the standby is not promoted).
#
# Each copy is a "machine" on three networks: one to the other copy, one to the witness, one to
# the clients (where it has another name, client-a or client-b, so replication cannot go round a
# cut through it). A link is cut by disconnecting a network, and a guard shares its copy's network
# namespace, so it is cut with it. The guards and the witness run the snout-lepis built for Linux
# in the stack's dev container (scripts/dev.sh). Everything is named lepis-fo-<pid>-* and removed
# after, by name.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
stack="$(cd "$here/.." && pwd)"
engine="${STACK_ENGINE:-docker}"
major="${LEPIS_FO_MAJOR:-18}"
pg_image="docker.io/library/postgres:$major"
lease=4000
tick=500
token="drill-token-0123456789abcdef0123456789"
operator="drill-operator"

all=(kill-primary cut-primary cut-replication kill-witness standby-and-witness async-then-cut)
if [ "$#" -gt 0 ]; then scenarios=("$@"); else scenarios=("${all[@]}"); fi

echo "== building snout-lepis for Linux" >&2
bash "$stack/scripts/dev.sh" cargo build -q -p snout-lepis >&2
hash_of() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$@"; else shasum -a 256 "$@"; fi; }
dev_image="snout-stack-dev:$(hash_of "$stack/container/Containerfile" | cut -c1-12)"
bin=/cache/target/debug/snout-lepis

run=""
names() {
	A="$run-a" B="$run-b" W="$run-w" C="$run-c" GA="$run-ga" GB="$run-gb"
	NAB="$run-ab" NAW="$run-aw" NBW="$run-bw" NC="$run-net-c"
}
cleanup() {
	[ -n "$run" ] || return 0
	if [ -n "${LEPIS_FO_LOGS:-}" ]; then
		for c in "$GA" "$GB" "$W"; do
			echo "--- $c" >&2
			"$engine" logs "$c" 2>&1 | tail -n "$LEPIS_FO_LOGS" >&2 || true
		done
	fi
	"$engine" rm -fv "$GA" "$GB" "$C" "$A" "$B" "$W" >/dev/null 2>&1 || true
	for n in "$NAB" "$NAW" "$NBW" "$NC"; do "$engine" network rm "$n" >/dev/null 2>&1 || true; done
}
trap cleanup EXIT

# Whether a copy could acknowledge a commit now: a writable primary whose commits wait for nobody,
# or for a standby that is streaming from it.
probe="select not pg_is_in_recovery()
	and current_setting('default_transaction_read_only') = 'off'
	and (current_setting('synchronous_standby_names') = ''
		or exists (select from pg_stat_replication where state = 'streaming'
			and current_setting('synchronous_standby_names') = '\"' || application_name || '\"'))"

settings=(-c password_encryption=scram-sha-256 -c wal_level=replica -c max_wal_senders=10
	-c hot_standby=on)

psql_in() { # container, sql
	"$engine" exec -e PGPASSWORD=x "$1" psql -U postgres -h 127.0.0.1 -XAtqc "$2"
}

wait_for() { # what, seconds, shell condition (evaluated here, so it may call these functions)
	local what="$1" left="$2"
	until eval "$3" >/dev/null 2>&1; do
		left=$((left - 1))
		if [ "$left" -le 0 ]; then
			echo "timed out waiting for $what" >&2
			return 1
		fi
		sleep 1
	done
}

witness_port() { "$engine" port "$W" 7445 | head -1 | sed 's/.*://'; }
witness() { # method path [body]
	curl -fsS -X "$1" -H "authorization: Bearer $operator" "http://127.0.0.1:$(witness_port)$2" ${3:+-d "$3"}
}
witness_says() { # field
	witness GET /v1/clusters/drill | python3 -c "import json,sys; print(json.load(sys.stdin)['$1'])"
}

guard() { # name container copy peer
	"$engine" run -d --name "$1" --network "container:$2" \
		-v snout-stack-target:/cache/target:ro \
		-e LEPIS_LOG=info,lepis::guard=debug \
		-e LEPIS_GUARD_WITNESS=witness:7445 -e LEPIS_GUARD_WITNESS_SSLMODE=disable \
		-e LEPIS_GUARD_CLUSTER=drill -e LEPIS_GUARD_TOKEN="$token" \
		-e LEPIS_GUARD_COPY="$3" -e LEPIS_GUARD_PEER="$4" \
		-e LEPIS_GUARD_PG=127.0.0.1:5432 -e LEPIS_GUARD_PG_PASSWORD=x \
		-e LEPIS_GUARD_STATE=/tmp/guard.json -e LEPIS_GUARD_STATUS=/tmp/status.json \
		-e LEPIS_GUARD_LEASE_MS="$lease" -e LEPIS_GUARD_TICK_MS="$tick" \
		"$dev_image" "$bin" guard >/dev/null
}

setup() {
	run="lepis-fo-$$-$1"
	names
	for n in "$NAB" "$NAW" "$NBW" "$NC"; do "$engine" network create "$n" >/dev/null; done

	"$engine" run -d --name "$W" --network "$NAW" --network-alias witness -p 127.0.0.1::7445 \
		-v snout-stack-target:/cache/target:ro \
		-e LEPIS_WITNESS_ADDR=0.0.0.0:7445 -e LEPIS_WITNESS_DIR=/tmp/witness \
		-e LEPIS_WITNESS_TOKEN="$operator" -e LEPIS_WITNESS_CLEARTEXT=wireguard \
		"$dev_image" "$bin" witness >/dev/null
	"$engine" network connect --alias witness "$NBW" "$W"

	"$engine" run -d --name "$A" --network "$NAB" --network-alias a -e POSTGRES_PASSWORD=x \
		-e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 -e POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 \
		"$pg_image" "${settings[@]}" >/dev/null
	"$engine" network connect "$NAW" "$A"
	"$engine" network connect --alias client-a "$NC" "$A"
	# The image's entrypoint restarts the server once after initdb: wait for the second start.
	wait_for "copy a" 60 'psql_in "$A" "select 1"'
	sleep 2
	wait_for "copy a" 30 'psql_in "$A" "select 1"'
	"$engine" exec "$A" bash -c 'echo "host replication postgres all scram-sha-256" >> "$PGDATA/pg_hba.conf"'
	psql_in "$A" "select pg_reload_conf()" >/dev/null
	psql_in "$A" "create table drill (n bigint primary key)"

	local data=/var/lib/postgresql/standby
	"$engine" run -d --name "$B" --network "$NAB" --network-alias b -e PGPASSWORD=x --entrypoint bash \
		"$pg_image" -c "
		set -e
		mkdir -p $data && chown postgres:postgres $data && chmod 700 $data
		gosu postgres pg_basebackup -h a -U postgres -D $data -R -X stream -c fast
		echo \"primary_conninfo = 'host=a user=postgres password=x application_name=b'\" >> $data/postgresql.auto.conf
		echo 'host replication postgres all scram-sha-256' >> $data/pg_hba.conf
		exec gosu postgres postgres -D $data ${settings[*]}" >/dev/null
	"$engine" network connect "$NBW" "$B"
	"$engine" network connect --alias client-b "$NC" "$B"
	wait_for "copy b streaming" 60 '[ "$(psql_in "$B" "select status from pg_stat_wal_receiver")" = streaming ]'

	witness PUT /v1/clusters/drill "{\"primary\":\"a\",\"standby\":\"b\",\"lease_ms\":$lease,\"token\":\"$token\"}" >/dev/null
	guard "$GA" "$A" a b
	guard "$GB" "$B" b a
	wait_for "b vouched for" 60 '[ "$(witness_says eligible)" = True ]'
	echo "== $run: a primary, b synchronous and eligible" >&2

	# The writer, on the clients' network, numbers its commits and keeps the ones acknowledged;
	# a commit cut off by its 3 s timeout was never acknowledged. Beside it, every half second,
	# which copies could acknowledge a commit then.
	"$engine" run -d --name "$C" --network "$NC" -e PGPASSWORD=x -e PROBE="$probe" --entrypoint bash \
		"$pg_image" -c '
		acks() { timeout 1 psql -h "$1" -U postgres -XAtqc "$PROBE" 2>/dev/null; }
		( while :; do
			a=$(acks client-a); b=$(acks client-b)
			if [ "$a" = t ] && [ "$b" = t ]; then echo "$(date +%T.%N) both" >> /tmp/both; fi
			sleep 0.5
		done ) &
		n=0
		while :; do
			n=$((n + 1))
			got=$(timeout 3 psql "host=client-a,client-b user=postgres target_session_attrs=read-write connect_timeout=1" \
				-XAtqc "insert into drill values ($n) returning n" 2>/dev/null || true)
			if [ "$got" = "$n" ]; then echo "$n" >> /tmp/acked; fi
			sleep 0.05
		done' >/dev/null
	sleep 6
}

acked_count() { "$engine" exec "$C" bash -c 'wc -l < /tmp/acked 2>/dev/null || echo 0' | tr -d ' '; }

check() { # scenario, expected primary
	local name="$1" want="$2"
	"$engine" stop -t 1 "$C" >/dev/null
	local primary
	primary="$(witness_says primary 2>/dev/null || echo "$want")"
	if [ "$primary" != "$want" ]; then
		echo "FAIL $name: the witness names $primary as the primary, expected $want" >&2
		return 1
	fi
	local copy="$A"
	[ "$primary" = b ] && copy="$B"
	"$engine" cp "$C:/tmp/acked" "/tmp/$run.acked" 2>/dev/null || : >"/tmp/$run.acked"
	local both=0
	if "$engine" cp "$C:/tmp/both" "/tmp/$run.both" 2>/dev/null; then both="$(wc -l <"/tmp/$run.both" | tr -d ' ')"; fi
	psql_in "$copy" "select n from drill order by n" >"/tmp/$run.have"
	local acked lost
	acked="$(wc -l <"/tmp/$run.acked" | tr -d ' ')"
	# comm compares as strings, so both sides are sorted as strings.
	sort "/tmp/$run.acked" | comm -23 - <(sort "/tmp/$run.have") >"/tmp/$run.lost"
	lost="$(wc -l <"/tmp/$run.lost" | tr -d ' ')"
	if [ "$lost" != 0 ]; then echo "lost: $(sort -n "/tmp/$run.lost" | head -20 | tr '\n' ' ')" >&2; fi
	rm -f "/tmp/$run.acked" "/tmp/$run.have" "/tmp/$run.both" "/tmp/$run.lost"
	echo "== $name: $acked acknowledged, $lost lost, primary $primary, both writable at $both samples" >&2
	[ "$lost" = 0 ] && [ "$both" = 0 ]
}

progress() { # seconds: commits acknowledged over that time
	local before after
	before="$(acked_count)"
	sleep "$1"
	after="$(acked_count)"
	echo $((after - before))
}

failed=()
for s in "${scenarios[@]}"; do
	setup "$s"
	case "$s" in
	kill-primary)
		"$engine" kill "$GA" "$A" >/dev/null
		sleep $((lease * 3 / 1000))
		moved="$(progress 4)"
		[ "$moved" -gt 0 ] || { echo "FAIL $s: no commits on b after the failover" >&2; failed+=("$s"); }
		check "$s" b || failed+=("$s")
		;;
	cut-primary)
		"$engine" network disconnect "$NAB" "$A"
		"$engine" network disconnect "$NAW" "$A"
		sleep $((lease * 3 / 1000))
		[ "$(psql_in "$A" "show synchronous_standby_names")" = lepis_fenced ] ||
			{ echo "FAIL $s: the cut primary is not fenced" >&2; failed+=("$s"); }
		moved="$(progress 4)"
		[ "$moved" -gt 0 ] || { echo "FAIL $s: no commits on b after the failover" >&2; failed+=("$s"); }
		check "$s" b || failed+=("$s")
		;;
	cut-replication)
		"$engine" network disconnect "$NAB" "$B"
		sleep $((lease * 2 / 1000))
		moved="$(progress 4)"
		[ "$moved" -gt 0 ] || { echo "FAIL $s: commits stopped without the standby" >&2; failed+=("$s"); }
		check "$s" a || failed+=("$s")
		;;
	kill-witness)
		"$engine" stop -t 1 "$W" >/dev/null
		sleep $((lease * 2 / 1000))
		moved="$(progress 4)"
		[ "$moved" -gt 0 ] || { echo "FAIL $s: commits stopped without the witness" >&2; failed+=("$s"); }
		"$engine" start "$W" >/dev/null
		sleep 2
		check "$s" a || failed+=("$s")
		;;
	standby-and-witness)
		"$engine" kill "$GB" "$B" >/dev/null
		"$engine" stop -t 1 "$W" >/dev/null
		sleep $((lease * 2 / 1000))
		moved="$(progress 4)"
		[ "$moved" = 0 ] || { echo "FAIL $s: $moved commits acknowledged with no standby and no witness" >&2; failed+=("$s"); }
		"$engine" start "$W" >/dev/null
		sleep 2
		check "$s" a || failed+=("$s")
		;;
	async-then-cut)
		"$engine" network disconnect "$NAB" "$B"
		wait_for "a to go on without b" 30 '[ -z "$(psql_in "$A" "show synchronous_standby_names")" ]'
		sleep 2
		"$engine" network disconnect "$NAW" "$A"
		sleep $((lease * 3 / 1000))
		moved="$(progress 4)"
		[ "$moved" = 0 ] || { echo "FAIL $s: $moved commits acknowledged by a copy with no lease" >&2; failed+=("$s"); }
		[ "$(psql_in "$B" "select pg_is_in_recovery()")" = t ] ||
			{ echo "FAIL $s: b was promoted without every acknowledged commit" >&2; failed+=("$s"); }
		check "$s" a || failed+=("$s")
		;;
	*)
		echo "no such scenario: $s (${all[*]})" >&2
		exit 2
		;;
	esac
	cleanup
	run=""
done

if [ "${#failed[@]}" -gt 0 ]; then
	echo "FAILED: ${failed[*]}" >&2
	exit 1
fi
echo "== every scenario: 0 lost, never two writable" >&2
