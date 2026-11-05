#!/usr/bin/env bash
# A chunked table grown large through the router while the autopilot keeps it in shape: how it
# splits (ranges split and moved, then single chunks split in place), whether it settles after each
# round, what the clients see while it does, and that no row is lost or doubled.
#
#   bash bench/grow.sh <label> [rounds] [rows a round] [split bytes]
#   e.g. grow.sh branch 8 5000000 536870912
#
# Docker only: four Postgres 18 nodes on one network, one router (the release binary of THIS
# checkout) on the host. Everything it makes is named lepis-grow-* and removed at the end (KEEP=1
# keeps it).
#
# The table: `ev (k, id, v, pad)`, primary key (k, id), an index on v, distributed empty by k into
# keyspace `k` (4 ranges, one a node) as 16 chunks. Keys are spread over 10M values, and one key
# (42) takes 1 row in 33, a hot tenant no split can divide. Each round COPYs `rows` more through the
# router, then waits for the autopilot (every 10 s; `advice_split_bytes` lowered to `split bytes` so
# a laptop reaches "too big") until it has run nothing for a minute. Throughout, pgbench writes and
# point-reads through the router. Writes results/<date>-grow-<label>.md.
set -euo pipefail
label="${1:?a label}"
rounds="${2:-8}"
rows="${3:-5000000}"
split="${4:-536870912}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
stack="$(cd "$here/.." && pwd)"
run=lepis-grow
net="$run-net"
nodes=("$run-n1" "$run-n2" "$run-n3" "$run-n4")
token=grow-token-0123456789abcdef
out="$here/results/$(date -u +%Y-%m-%d)-grow-$label.md"
tmp="$(mktemp -d)"
pids=()

cleanup() {
	for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
	[ -n "${KEEP:-}" ] && return
	docker rm -fv "${nodes[@]}" "$run-pgbench" >/dev/null 2>&1 || true
	docker network rm "$net" >/dev/null 2>&1 || true
	rm -rf "$tmp"
}
trap cleanup EXIT
cleanup
mkdir -p "$tmp"

ip() { docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1"; }
sql() { docker exec -i "$1" psql -U postgres -XAtq -v ON_ERROR_STOP=1 "${@:2}"; }
routed() {
	docker run -i --rm --network host -e PGPASSWORD=app postgres:18 \
		psql -h 127.0.0.1 -p 6432 -U app -d postgres -XAtq -v ON_ERROR_STOP=1 "$@"
}
admin() {
	curl -sS --fail-with-body -X "$1" -H "authorization: Bearer $token" -H 'content-type: application/json' \
		"http://127.0.0.1:7445$2" ${3:+-d "$3"}
}
job() {
	local id state
	id="$(admin POST /v1/jobs "$1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["job"])')"
	while :; do
		state="$(admin GET "/v1/jobs/$id" | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])')"
		case "$state" in
		done) return 0 ;;
		failed | cancelled)
			admin GET "/v1/jobs/$id" >&2
			return 1
			;;
		esac
		sleep 1
	done
}

echo "== build ($label)" >&2
(cd "$stack" && cargo build -q --release -p snout-lepis)
bin="${CARGO_TARGET_DIR:-$stack/target}/release/snout-lepis"

echo "== nodes" >&2
docker network create "$net" >/dev/null
for c in "${nodes[@]}"; do
	docker run -d --name "$c" --network "$net" --shm-size=1g -e POSTGRES_PASSWORD=x \
		-e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 -e POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 \
		postgres:18 -c password_encryption=scram-sha-256 -c wal_level=logical \
		-c max_prepared_transactions=64 -c max_connections=300 -c shared_buffers=512MB \
		-c max_wal_size=512MB -c checkpoint_timeout=5min >/dev/null
done
for c in "${nodes[@]}"; do
	until sql "$c" -c 'select 1' >/dev/null 2>&1; do sleep 1; done
done
sleep 2
home="$(ip "${nodes[0]}")"
sql "${nodes[0]}" -c "create role app login password 'app';
	create table ev (k bigint not null, id bigint not null, v int not null, pad text,
		primary key (k, id));
	create index ev_v on ev (v);
	grant select, insert, update, delete on ev to app" >/dev/null
for f in catalog.sql catalog_2pc.sql catalog_jobs.sql catalog_cron.sql; do
	sql "${nodes[0]}" <"$here/../src/$f" >/dev/null
done
sql "${nodes[0]}" -c "insert into lepis.node (id, name, host, port, dbname, sslmode, kind, state, server_version_num)
	values (1, 'n1', '$home', 5432, 'postgres', 'disable', 'home', 'active', current_setting('server_version_num')::int);
	select lepis.bump();" >/dev/null

echo "== router" >&2
env LEPIS_HOME="$home:5432" LEPIS_HOME_SSLMODE=disable LEPIS_SERVICE_USER=postgres \
	LEPIS_SERVICE_PASSWORD=x LEPIS_PORT=6432 LEPIS_ADMIN_ADDR=127.0.0.1:7445 LEPIS_ADMIN_TOKEN=$token \
	RUST_LOG=warn "$bin" >"$tmp/router.log" 2>&1 &
pids+=($!)
until curl -fsS http://127.0.0.1:7445/v1/health >/dev/null 2>&1; do sleep 1; done
for i in 2 3 4; do
	job "{\"op\": \"node.add\", \"name\": \"n$i\", \"host\": \"$(ip "${nodes[$((i - 1))]}")\", \"port\": 5432, \"dbname\": \"postgres\", \"sslmode\": \"disable\"}" >/dev/null
done
job '{"op": "keyspace.create", "name": "k", "key_type": "int8", "ranges": 4}' >/dev/null
job '{"op": "table.distribute", "table": "public.ev", "column": "k", "keyspace": "k", "chunks": 16}' >/dev/null
admin POST /v1/settings "{\"autopilot\": \"auto\", \"autopilot_interval_ms\": 10000, \"advice_sample_ms\": 3000,
	\"advice_split_bytes\": $split, \"advice_min_bytes\": $((split / 8)), \"advice_hold_s\": 60}" >/dev/null

# Clients throughout: a write (negative ids, apart from the loaded rows) and a point read.
cat >"$tmp/probe.sql" <<'EOF'
\set k random(1, 10000000)
\set id random(1, 2000000000)
insert into ev (k, id, v, pad) values (:k, -:id, 0, 'probe') on conflict do nothing;
select count(*) from ev where k = :k;
EOF
docker run -d --name "$run-pgbench" --network host -v "$tmp:/b" -e PGPASSWORD=app postgres:18 \
	pgbench -h 127.0.0.1 -p 6432 -U app -n -c 4 -j 2 -T 86400 --max-tries=20 \
	-l --aggregate-interval=10 --log-prefix=/b/lat -f /b/probe.sql postgres >/dev/null

# Per round: what the catalog and the nodes hold, and what the autopilot did.
snapshot() {
	local chunks ranges bytes biggest
	chunks="$(sql "${nodes[0]}" -c "select count(*) from lepis.chunk where keyspace = 'k'")"
	ranges="$(sql "${nodes[0]}" -c "select string_agg(n, ' ' order by node_id) from
		(select node_id, count(*)::text n from lepis.range where keyspace = 'k' group by node_id) x")"
	bytes=0
	biggest=0
	local per=()
	for c in "${nodes[@]}"; do
		read -r b m < <(sql "$c" -F ' ' -c "select coalesce(sum(s), 0), coalesce(max(s), 0) from
			(select pg_total_relation_size(c.oid) s from pg_class c where c.relkind = 'r' and c.relname ~ '^ev_c[0-9]+$') x")
		bytes=$((bytes + b))
		[ "$m" -gt "$biggest" ] && biggest=$m
		per+=("$((b / 1048576))")
	done
	echo "$chunks|$ranges|$((bytes / 1048576))|${per[*]}|$((biggest / 1048576))"
}
jobs_since() {
	sql "${nodes[0]}" -c "select coalesce(string_agg(op || ' ' || n, ', ' order by op), '-') from
		(select op, count(*) n from lepis.job where id > $1 and args->>'actor' = 'lepis' and state = 'done' group by op) x"
}
failed_jobs() { sql "${nodes[0]}" -c "select count(*) from lepis.job where state = 'failed'"; }
last_job() { sql "${nodes[0]}" -c "select coalesce(max(id), 0) from lepis.job"; }
busy() { sql "${nodes[0]}" -c "select count(*) from lepis.job where state in ('pending', 'running', 'cancelling')"; }

rowsfile="$tmp/rounds"
: >"$rowsfile"
: >"$tmp/copies"
loaded=0
for r in $(seq 1 "$rounds"); do
	a=$((loaded + 1))
	b=$((loaded + rows))
	echo "== round $r: rows $a..$b" >&2
	j0="$(last_job)"
	t0=$(date +%s.%N)
	# A COPY a cutover aborts (40001, L10) is retried whole, as a client would; one that failed must
	# have left nothing.
	tries=0
	until sql "${nodes[0]}" -c "copy (select case when g % 33 = 0 then 42 else 1 + (g * 2654435761) % 10000000 end,
		g, (g * 7919) % 100000, repeat('x', 60) from generate_series($a::bigint, $b) g) to stdout" |
		routed -c 'copy ev from stdin' >/dev/null 2>"$tmp/copy.err"; do
		tries=$((tries + 1))
		echo "   COPY failed: $(head -c 300 "$tmp/copy.err")" >&2
		left_behind="$(routed -c "select count(*) from ev where id between $a and $b")"
		[ "$left_behind" = 0 ] || echo "PARTIAL COPY: $left_behind rows of round $r kept" | tee -a "$tmp/partial" >&2
		[ "$tries" -ge 5 ] && exit 1
		sleep 5
	done
	t1=$(date +%s.%N)
	echo "$r $t0 $t1" >>"$tmp/copies"
	loaded=$b
	# Settled: no job pending or running, and none started, for six checks 10 s apart.
	quiet=0
	last="$(last_job)"
	w0=$(date +%s)
	while [ "$quiet" -lt 6 ] && [ $(($(date +%s) - w0)) -lt 1800 ]; do
		sleep 10
		now="$(last_job)"
		if [ "$(busy)" = 0 ] && [ "$now" = "$last" ]; then quiet=$((quiet + 1)); else quiet=0; fi
		last="$now"
	done
	settle=$(($(date +%s) - w0 - 60))
	[ "$quiet" -lt 6 ] && settle="over 1800 (not settled)"
	pause="$(sql "${nodes[0]}" -c "select coalesce(round(max((detail->'cutover'->>'pause_ms')::numeric), 1)::text, '-')
		from lepis.job_step where job_id > $j0")"
	left="$(admin GET '/v1/advice?sample_ms=3000' | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["advice"]))')"
	echo "$r|$loaded|$(python3 -c "print(round($rows / ($t1 - $t0)))")|$(snapshot)|$(jobs_since "$j0")|$settle|$pause|$(failed_jobs)|$left|$tries" >>"$rowsfile"
	tail -1 "$rowsfile" >&2
done

echo "== check" >&2
docker rm -f "$run-pgbench" >/dev/null
admin POST /v1/settings '{"autopilot": "off"}' >/dev/null
expect_sum=$((loaded * (loaded + 1) / 2))
got="$(routed -F ' ' -c "select count(*), sum(id) from ev where id > 0")"
hot="$(routed -c "select count(*) from ev where k = 42 and id > 0")"
expect_hot="$(sql "${nodes[0]}" -c "select count(*) from generate_series(1::bigint, $loaded) g
	where case when g % 33 = 0 then 42 else 1 + (g * 2654435761) % 10000000 end = 42")"
# The aggregated log: interval start, transactions, latency sum, sum of squares, min, max, ... failures.
probe="$(cat "$tmp"/lat.* 2>/dev/null | awk '{n += $2; s += $3; if ($6 + 0 > m + 0) m = $6 + 0; a = ($2 > 0) ? $3 / $2 : 0; if (a > w) w = a; if ($2 == 0) z++}
	END {printf "%d tx, mean %.1f ms, worst 10 s mean %.1f ms, slowest %.0f ms, %d idle 10 s intervals", n, (n ? s / n : 0) / 1000, w / 1000, m / 1000, z}')"
# Each 10 s interval with no transaction or one over 5 s, and what was running then.
sql "${nodes[0]}" -F ' ' -c "select j.id, j.op, s.n, s.kind, extract(epoch from s.started_at), extract(epoch from coalesce(s.finished_at, now()))
	from lepis.job j join lepis.job_step s on s.job_id = j.id where s.started_at is not null" >"$tmp/steps"
stalls="$(cat "$tmp"/lat.* 2>/dev/null | python3 -c '
import sys
steps = [l.split() for l in open(sys.argv[1]) if l.strip()]
copies = [l.split() for l in open(sys.argv[2]) if l.strip()]
by = {}
for l in sys.stdin:
    f = l.split()
    t, n, mx = int(f[0]), int(f[1]), int(f[5]) / 1000
    o = by.setdefault(t, [0, 0])
    o[0] += n
    o[1] = max(o[1], mx)
out = []
for t, (n, mx) in sorted(by.items()):
    if n and mx < 5000:
        continue
    w = [f"job {s[0]} {s[1]} step {s[2]} {s[3]}" for s in steps if float(s[4]) < t + 10 and float(s[5]) > t]
    w += [f"COPY round {c[0]}" for c in copies if float(c[1]) < t + 10 and float(c[2]) > t]
    out.append(f"{t}: {n} tx, slowest {mx:.0f} ms; " + ("; ".join(w) or "nothing of the autopilot or the load"))
print("<br>".join(out) or "none")
' "$tmp/steps" "$tmp/copies")"
aborted="$(docker logs "$run-pgbench" 2>&1 | grep -c 'aborted' || true)"

mkdir -p "$here/results"
{
	echo "# Growing a chunked table, $label ($(git -C "$stack" rev-parse --short HEAD)), $(date -u +%Y-%m-%d)"
	echo
	echo "\`ev\` grown by $rows rows a round over $rounds rounds through the router (COPY), 4 nodes, Postgres 18, Docker,"
	echo "one box. Distributed empty: 4 ranges, 16 chunks. Keys spread over 10M values; key 42 holds 1 row in 33."
	echo "Autopilot on: \`advice_split_bytes\` $((split / 1048576)) MB, \`advice_min_bytes\` an eighth of it, \`advice_hold_s\` 60, every 10 s. pgbench writes and"
	echo "point-reads through the router throughout (4 clients)."
	echo
	echo "| round | rows | COPY rows/s | chunks | ranges per node | MB on nodes | MB per node | largest chunk MB | autopilot jobs | settle s | max pause ms | failed jobs | advice left | COPY retries |"
	echo "| ---: | ---: | ---: | ---: | --- | ---: | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |"
	awk -F'|' '{printf "|"; for (i = 1; i <= NF; i++) printf " %s |", $i; print ""}' "$rowsfile"
	echo
	echo "| check | value |"
	echo "| --- | --- |"
	echo "| loaded rows, sum(id): expected | $loaded, $expect_sum |"
	echo "| loaded rows, sum(id): through the router | $got |"
	echo "| hot key 42 rows: expected, got | $expect_hot, $hot |"
	echo "| probe clients | $probe; $aborted aborted |"
	echo "| stalled 10 s intervals (epoch s) | $stalls |"
	echo "| a failed COPY that left rows | $(cat "$tmp/partial" 2>/dev/null || echo none) |"
} >"$out"
cat "$out"
