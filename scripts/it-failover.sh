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
# standby, then loses the witness: it stops, and the standby is not promoted), silent-cut-archive
# (cut-primary as the 2026-10-26 live drill did it, packets dropped rather than the network gone,
# with both copies archiving into one directory every second: once fenced, the old primary
# archives nothing at or past the fork, and every segment before it reaches the archive),
# silent-cut-archive-gated (the same, with the pod image's snoutpod-archive-gate in front of
# archive_command: nothing past the fork reaches the archive at all, before the fence included),
# silent-cut-timing (the same cut at the live lease of 10 s and tick of 1 s: the primary fences
# about half a lease after its last renewal, and the standby is granted about a lease after the
# last WAL it received, which is why a silent cut is slower than a clean stop; its standby reads a slow,
# empty archive, as a pod's does from S3, and is promoted within 3.2 s of the grant with its
# restore_command as it was after).
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
# The live control plane's (guard.ts: the lease, and a tick of a fifth of it capped at 1 s).
live_lease=10000
live_tick=1000
token="drill-token-0123456789abcdef0123456789"
operator="drill-operator"

all=(kill-primary cut-primary cut-replication kill-witness standby-and-witness async-then-cut silent-cut-archive
	silent-cut-archive-gated silent-cut-timing)
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
		for c in "$GA" "$GB" "$W" "$B"; do
			echo "--- $c" >&2
			"$engine" logs "$c" 2>&1 | tail -n "$LEPIS_FO_LOGS" >&2 || true
		done
	fi
	"$engine" rm -fv "$GA" "$GB" "$C" "$A" "$B" "$W" >/dev/null 2>&1 || true
	for n in "$NAB" "$NAW" "$NBW" "$NC"; do "$engine" network rm "$n" >/dev/null 2>&1 || true; done
	"$engine" volume rm "$run-archive" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Whether a copy could acknowledge a commit now: a writable primary whose commits wait for nobody,
# or for a standby that is streaming from it.
probe="select not pg_is_in_recovery()
	and current_setting('default_transaction_read_only') = 'off'
	and (current_setting('synchronous_standby_names') = ''
		or exists (select from pg_stat_replication where state = 'streaming'
			and current_setting('synchronous_standby_names') = '\"' || application_name || '\"'))"

base_settings=(-c password_encryption=scram-sha-256 -c wal_level=replica -c max_wal_senders=10
	-c hot_standby=on)
# archive_command is set with ALTER SYSTEM (as the pods set it), never here: a command-line value
# outranks postgresql.auto.conf, and the guard changes it there.
archive_settings=(-c archive_mode=on -c archive_timeout=1)
# As pgBackRest does: the same file again is fine, a different one under the name is refused.
archive_command='if [ -f /archive/%f ]; then cmp -s %p /archive/%f; else cp %p /archive/%f; fi'
# The pod image's archive gate (snoutpod-archive-gate), in front of archive_command in
# silent-cut-archive-gated: a segment goes into the archive only once the synchronous standby has
# flushed past it, which closes the seconds between a silent cut and the fence.
archive_gate="$stack/../snoutpod/image/bin/snoutpod-archive-gate"

# A pod's standby reads its archive with pgBackRest from S3, where a file it lacks costs about 0.8 s
# to be told so; promoting asks five times. silent-cut-timing's standby is given one.
slow_restore='sleep 0.8; exit 1'

# A silent cut: packets dropped in a copy's network namespace (the live drill's nft table), so
# nothing fails fast the way a disconnected network does. Its own small image, made once.
nft_image="lepis-fo-nft:trixie"
silent_cut() { # container, nft rules
	if ! "$engine" image inspect "$nft_image" >/dev/null 2>&1; then
		printf 'FROM debian:trixie-slim\nRUN apt-get update && apt-get install -y --no-install-recommends nftables && rm -rf /var/lib/apt/lists/*\n' |
			"$engine" build -q -t "$nft_image" - >/dev/null
	fi
	# Prints when the rules took effect, on the engine's clock (the one the guards log by).
	"$engine" run --rm -i --network "container:$1" --cap-add NET_ADMIN --entrypoint sh "$nft_image" \
		-c 'nft -f - && date -u +%s.%N' <<<"$2"
}
cut_a() { # the live drill's cut, on a: b's stream and the witness, nothing else
	local b_ip w_ip
	b_ip="$(ip_on "$B" "$NAB")"
	w_ip="$(ip_on "$W" "$NAW")"
	silent_cut "$A" "table inet lepisdrill {
		chain drill_in { type filter hook input priority -300; policy accept; ip saddr $b_ip tcp dport 5432 drop; }
		chain drill_out { type filter hook output priority -300; policy accept; ip daddr $w_ip tcp dport 7445 drop; }
	}"
}
logged_at() { # container, message: the first line's time with it, as epoch seconds
	"$engine" logs "$1" 2>&1 | grep -F "$2" | head -1 | python3 -c '
import sys, datetime
line = sys.stdin.read().split()
print(datetime.datetime.fromisoformat(line[0].replace("Z", "+00:00")).timestamp() if line else "")'
}
ip_on() { # container, network
	"$engine" inspect -f "{{(index .NetworkSettings.Networks \"$2\").IPAddress}}" "$1"
}

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
	settings=("${base_settings[@]}")
	archive=()
	restore=""
	if [ "$1" = silent-cut-timing ]; then lease=$live_lease tick=$live_tick restore=$slow_restore; else lease=4000 tick=500; fi
	command=$archive_command
	if [ "$1" = silent-cut-archive ] || [ "$1" = silent-cut-archive-gated ]; then
		settings+=("${archive_settings[@]}")
		"$engine" volume create "$run-archive" >/dev/null
		"$engine" run --rm -v "$run-archive:/archive" --entrypoint chown "$pg_image" postgres:postgres /archive
		archive=(-v "$run-archive:/archive")
	fi
	if [ "$1" = silent-cut-archive-gated ]; then
		archive+=(-v "$archive_gate:/usr/local/bin/snoutpod-archive-gate:ro" -e SNOUTPOD_ADMIN_USER=postgres)
		command="snoutpod-archive-gate %f && $archive_command"
	fi

	"$engine" run -d --name "$W" --network "$NAW" --network-alias witness -p 127.0.0.1::7445 \
		-v snout-stack-target:/cache/target:ro \
		-e LEPIS_WITNESS_ADDR=0.0.0.0:7445 -e LEPIS_WITNESS_DIR=/tmp/witness \
		-e LEPIS_WITNESS_TOKEN="$operator" -e LEPIS_WITNESS_CLEARTEXT=wireguard \
		"$dev_image" "$bin" witness >/dev/null
	"$engine" network connect --alias witness "$NBW" "$W"

	"$engine" run -d --name "$A" --network "$NAB" --network-alias a -e POSTGRES_PASSWORD=x ${archive[@]+"${archive[@]}"} \
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
	if [ "${#archive[@]}" -gt 0 ]; then
		psql_in "$A" "alter system set archive_command = '$command'" >/dev/null
		psql_in "$A" "select pg_reload_conf()" >/dev/null
	fi

	local data=/var/lib/postgresql/standby
	"$engine" run -d --name "$B" --network "$NAB" --network-alias b -e PGPASSWORD=x ${archive[@]+"${archive[@]}"} --entrypoint bash \
		"$pg_image" -c "
		set -e
		mkdir -p $data && chown postgres:postgres $data && chmod 700 $data
		gosu postgres pg_basebackup -h a -U postgres -D $data -R -X stream -c fast
		echo \"primary_conninfo = 'host=a user=postgres password=x application_name=b'\" >> $data/postgresql.auto.conf
		[ -z \"$restore\" ] || echo \"restore_command = '$restore'\" >> $data/postgresql.auto.conf
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
		# A copy that archives nothing (archive_mode off, archive_command shown as "(disabled)") is
		# never given a guarded archive_command.
		[ "$(psql_in "$A" "select count(*) from pg_file_settings where name = 'archive_command'")" = 0 ] ||
			{ echo "FAIL $s: the guard wrote an archive_command on a copy that does not archive" >&2; failed+=("$s"); }
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
	silent-cut-archive | silent-cut-archive-gated)
		cut_a >/dev/null
		# WAL the standby never gets, in the seconds before the fence: commits that wait for nobody
		# (synchronous_commit = local) and segment switches, as the live drill's background writes
		# were. Ungated, these reach the archive past the fork; through the gate, none may.
		for _ in 1 2 3; do
			psql_in "$A" "set synchronous_commit = local; create table if not exists local_wal (n int); insert into local_wal select generate_series(1, 1000); select pg_switch_wal()" >/dev/null || true
			sleep 0.5
		done
		if ! wait_for "a fenced, its archive guarded" $((lease * 4 / 1000)) \
			'[ "$(psql_in "$A" "show synchronous_standby_names")" = lepis_fenced ] && psql_in "$A" "show archive_command" | grep -q "^LC_ALL=C expr"'; then
			echo "FAIL $s: the cut primary is not fenced with its archive guarded" >&2
			failed+=("$s")
		fi
		"$engine" exec "$B" ls /archive >"/tmp/$run.at-fence"
		wait_for "b promoted" $((lease * 6 / 1000)) '[ "$(psql_in "$B" "select pg_is_in_recovery()")" = f ]' ||
			{ echo "FAIL $s: b was not promoted" >&2; failed+=("$s"); }
		# archive_timeout is 1 s: an open archive on a would have taken several more segments by now.
		moved="$(progress 6)"
		[ "$moved" -gt 0 ] || { echo "FAIL $s: no commits on b after the failover" >&2; failed+=("$s"); }
		fork="$(psql_in "$B" "select convert_from(pg_read_binary_file('pg_wal/00000002.history'), 'UTF8')" | awk -F'\t' 'NF >= 2 { l = $2 } END { print l }')"
		fork_seg="$(psql_in "$B" "select '00000001' || substr(pg_walfile_name('$fork'::pg_lsn), 9)")"
		"$engine" exec "$B" ls /archive >"/tmp/$run.after"
		python3 - "/tmp/$run.at-fence" "/tmp/$run.after" "$fork_seg" "$s" <<'PY' || failed+=("$s")
import re, sys
at, after, fork, scenario = sys.argv[1:]
seg = re.compile(r"^00000001[0-9A-F]{16}$")
before = {n for n in open(at).read().split() if seg.match(n)}
now = {n for n in open(after).read().split() if seg.match(n)}
leaked_before = sorted(n for n in before if n >= fork)
leaked_after = sorted(n for n in now - before if n >= fork)
index = lambda n: int(n[8:16], 16) * 256 + int(n[16:], 16)
first = min(now, key=index, default=fork)
missing = [i for i in range(index(first), index(fork)) if not any(index(n) == i for n in now)]
print(f"== {scenario}: fork in {fork}; timeline 1 segments archived {len(now)}; at or past the fork "
      f"before the fence {len(leaked_before)} {leaked_before}, after it {len(leaked_after)} {leaked_after}; "
      f"missing before the fork {len(missing)}", file=sys.stderr)
# With the gate, nothing past the fork reaches the archive at all, before the fence included.
gated = scenario.endswith("-gated")
if leaked_after:
    print(f"FAIL {scenario}: the fenced primary archived past the fork", file=sys.stderr)
if gated and leaked_before:
    print(f"FAIL {scenario}: a segment past the fork reached the archive before the fence, through the gate", file=sys.stderr)
if missing:
    print(f"FAIL {scenario}: segments before the fork are missing from the archive", file=sys.stderr)
sys.exit(1 if leaked_after or missing or (gated and leaked_before) else 0)
PY
		rm -f "/tmp/$run.at-fence" "/tmp/$run.after"
		check "$s" b || failed+=("$s")
		;;
	silent-cut-timing)
		cut="$(cut_a)"
		wait_for "b promoted" $((lease * 4 / 1000)) '[ "$(psql_in "$B" "select pg_is_in_recovery()")" = f ]' ||
			{ echo "FAIL $s: b was not promoted" >&2; failed+=("$s"); }
		moved="$(progress 4)"
		[ "$moved" -gt 0 ] || { echo "FAIL $s: no commits on b after the failover" >&2; failed+=("$s"); }
		fenced="$(logged_at "$GA" "fenced: no lease and no standby")"
		granted="$(logged_at "$GB" "granted the primary's place")"
		promoted="$(logged_at "$GB" "promoted: this copy is the primary")"
		# The promotion's guard on restore_command comes off again once pg_promote has returned.
		[ "$(psql_in "$B" "show restore_command")" = "$slow_restore" ] ||
			{ echo "FAIL $s: b's restore_command is $(psql_in "$B" "show restore_command"), not the one it had" >&2; failed+=("$s"); }
		python3 - "$cut" "$fenced" "$granted" "$promoted" "$lease" <<'PY' || failed+=("$s")
import sys
cut, fenced, granted, promoted, lease = sys.argv[1:]
cut, lease = float(cut), int(lease) / 1000
at = lambda t: float(t) - cut if t else None
f, g, p = at(fenced), at(granted), at(promoted)
show = lambda v: "never" if v is None else f"+{v:.1f} s"
print(f"== silent-cut-timing: lease {lease:.0f} s; fenced {show(f)}, granted {show(g)}, promoted {show(p)}", file=sys.stderr)
# The fence: half a lease after the last renewal (at most a tick before the cut), plus a tick.
# The grant: a lease after the last WAL the standby received (about the cut), plus two ticks.
bad = []
if f is None or f > lease / 2 + 1.5:
    bad.append(f"fenced {show(f)}, not within {lease / 2 + 1.5:.1f} s")
if g is None or g > lease + 2.5:
    bad.append(f"granted {show(g)}, not within {lease + 2.5:.1f} s")
# The promotion: the archive reads it still makes at 0.8 s each, the next timeline's history twice
# and, on timeline 1 alone, which pg_wal has no history for, the current one's: 2.4 s here, 1.6 s on
# a pod past its first failover. Without the guard it made five, 4 s.
if g is not None and (p is None or p - g > 3.2):
    bad.append(f"promoted {show(p)}, more than 3.2 s after the grant")
for b in bad:
    print(f"FAIL silent-cut-timing: {b}", file=sys.stderr)
sys.exit(1 if bad else 0)
PY
		check "$s" b || failed+=("$s")
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
