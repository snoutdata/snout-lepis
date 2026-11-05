#!/usr/bin/env bash
# What chunked storage changes, measured on one box, the same way on the
# `main` sha and on the branch.
#
#   bash bench/chunks.sh <label> [rows]        e.g.  chunks.sh branch 10000000
#
# Docker only: four Postgres 18 nodes and a plain Postgres holding all the data (the "one
# Postgres" the joins are compared with), each a container on one network; two routers, the
# release binary of THIS checkout, on the host. Everything it makes is named lepis-bench-* and
# removed at the end (KEEP=1 keeps it).
#
# The data: two big tables of `rows` rows each, distributed by k with Zipf-skewed keys (s = 1,
# over 1,000,000 values) and two secondary indexes each, colocated in keyspace `k` (16 ranges, 4 a
# node); a 1,000,000-row reference-style table `dim`; and `small`, 50,000 rows in a keyspace of its
# own, for the broadcast join. The big tables are loaded on the home node and distributed by
# `table.distribute` (chunked on the branch, one heap a node on main).
#
# Measured: moving one of node 2's four ranges (25% of it) to node 4 while pgbench reads and
# writes through router 1: WAL the source wrote, disk the source got back, move time, the
# cutover's write pause, p99 of reads and writes during the move; a broadcast join of big_a with
# small and a gather of a window over big_a, against the plain Postgres; and (branch) whether the
# advisor moves a hot range and then stops. Writes results/<date>-chunks-<label>.md.
set -euo pipefail
label="${1:?a label}"
rows="${2:-10000000}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
stack="$(cd "$here/.." && pwd)"
run=lepis-bench
net="$run-net"
nodes=("$run-n1" "$run-n2" "$run-n3" "$run-n4")
plain="$run-plain"
token=bench-token-0123456789abcdef
out="$here/results/$(date -u +%Y-%m-%d)-chunks-$label.md"
tmp="$(mktemp -d)"
pids=()

cleanup() {
	for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
	[ -n "${KEEP:-}" ] && return
	docker rm -fv "${nodes[@]}" "$plain" "$run-pgbench" >/dev/null 2>&1 || true
	docker network rm "$net" >/dev/null 2>&1 || true
	rm -rf "$tmp"
}
trap cleanup EXIT

ip() { docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1"; }
sql() { docker exec -i "$1" psql -U postgres -XAtq -v ON_ERROR_STOP=1 "${@:2}"; }
admin() {
	curl -sS --fail-with-body -X "$1" -H "authorization: Bearer $token" -H 'content-type: application/json' \
		"http://127.0.0.1:7444$2" ${3:+-d "$3"}
}
job() {
	local id
	id="$(admin POST /v1/jobs "$1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["job"])')"
	while :; do
		local state
		state="$(admin GET "/v1/jobs/$id" | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])')"
		case "$state" in
		done) echo "$id"; return 0 ;;
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
for c in "${nodes[@]}" "$plain"; do
	docker run -d --name "$c" --network "$net" --shm-size=1g -e POSTGRES_PASSWORD=x \
		-e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 -e POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 \
		postgres:18 -c password_encryption=scram-sha-256 -c wal_level=logical \
		-c max_prepared_transactions=64 -c max_connections=300 -c shared_buffers=512MB \
		-c max_wal_size=1GB -c checkpoint_timeout=5min >/dev/null
done
for c in "${nodes[@]}" "$plain"; do
	until sql "$c" -c 'select 1' >/dev/null 2>&1; do sleep 1; done
done
sleep 2
home="$(ip "${nodes[0]}")"

# The data: generated identically on the home node and on the plain Postgres.
gen="create table big_a (k bigint not null, id bigint not null, v int not null, w int not null, pad text,
	primary key (k, id));
create index big_a_v on big_a (v);
create index big_a_w on big_a (w);
create table big_b (k bigint not null, id bigint not null, j int not null, x int not null, pad text,
	primary key (k, id));
create index big_b_j on big_b (j);
create index big_b_x on big_b (x);
create table dim (id int primary key, name text not null);
create table small (s bigint primary key, v int not null, label text not null);
insert into dim select g, 'dim ' || g from generate_series(1, 1000000) g;
insert into small select g, g % 100000, 'small ' || g from generate_series(1, 50000) g;
-- Zipf (s = 1: log-uniform) over 1,000,000 keys, from a deterministic uniform.
insert into big_a select greatest(1, floor(power(1000000, ((g * 2654435761::bigint) % 1000003) / 1000003.0)))::bigint,
	g, (g * 7919) % 100000, (g * 104729) % 1000000, repeat('a', 40) from generate_series(1::bigint, $rows) g;
insert into big_b select greatest(1, floor(power(1000000, ((g * 40503::bigint) % 1000033) / 1000033.0)))::bigint,
	g, (g * 6113) % 100000, (g * 15485863) % 1000000, repeat('b', 40) from generate_series(1::bigint, $rows) g;
analyze;"
echo "== load $rows rows x 2" >&2
sql "${nodes[0]}" -c "create role app login password 'app'" >/dev/null
printf '%s' "$gen" | sql "${nodes[0]}"
printf '%s' "$gen" | sql "$plain"
sql "${nodes[0]}" -c "grant select, insert, update, delete on all tables in schema public to app" >/dev/null

echo "== catalog" >&2
for f in catalog.sql catalog_2pc.sql catalog_jobs.sql catalog_cron.sql; do
	[ -f "$here/../src/$f" ] && sql "${nodes[0]}" <"$here/../src/$f" >/dev/null
done
sql "${nodes[0]}" -c "insert into lepis.node (id, name, host, port, dbname, sslmode, kind, state, server_version_num)
	values (1, 'n1', '$home', 5432, 'postgres', 'disable', 'home', 'active', current_setting('server_version_num')::int);
	select lepis.bump();" >/dev/null

echo "== routers" >&2
for i in 1 2; do
	env LEPIS_HOME="$home:5432" LEPIS_HOME_SSLMODE=disable LEPIS_SERVICE_USER=postgres \
		LEPIS_SERVICE_PASSWORD=x LEPIS_PORT=$((6431 + i)) \
		$([ "$i" = 1 ] && echo LEPIS_ADMIN_ADDR=127.0.0.1:7444 LEPIS_ADMIN_TOKEN=$token) \
		RUST_LOG=warn "$bin" >"$tmp/router$i.log" 2>&1 &
	pids+=($!)
done
until curl -fsS http://127.0.0.1:7444/v1/health >/dev/null 2>&1; do sleep 1; done

for i in 2 3 4; do
	job "{\"op\": \"node.add\", \"name\": \"n$i\", \"host\": \"$(ip "${nodes[$((i - 1))]}")\", \"port\": 5432, \"dbname\": \"postgres\", \"sslmode\": \"disable\"}" >/dev/null
done
job '{"op": "keyspace.create", "name": "k", "key_type": "int8", "ranges": 16}' >/dev/null
job '{"op": "keyspace.create", "name": "sk", "key_type": "int8", "ranges": 4}' >/dev/null
job '{"op": "table.reference", "table": "public.dim"}' >/dev/null
t0=$(date +%s)
job '{"op": "table.distribute", "table": "public.big_a", "column": "k", "keyspace": "k"}' >/dev/null
job '{"op": "table.distribute", "table": "public.big_b", "column": "k", "keyspace": "k"}' >/dev/null
job '{"op": "table.distribute", "table": "public.small", "column": "s", "keyspace": "sk"}' >/dev/null
distribute_s=$(($(date +%s) - t0))

# One of node 2's ranges (a quarter of it) to node 4, under load.
range="$(admin GET /v1/status | python3 -c '
import json, sys
c = json.load(sys.stdin)
ks = [k for k in c["keyspaces"] if k["name"] == "k"][0]
print([r for r in ks["ranges"] if r["node"] == 2][0]["lo"])')"
src="${nodes[1]}"
lsn0="$(sql "$src" -c 'select pg_current_wal_lsn()')"
size0="$(sql "$src" -c 'select pg_database_size(current_database())')"

cat >"$tmp/read.sql" <<'EOF'
\set k random_zipfian(1, 1000000, 1.1)
select count(*) from big_a where k = :k;
EOF
cat >"$tmp/write.sql" <<'EOF'
\set k random_zipfian(1, 1000000, 1.1)
\set id random(1, 2000000000)
insert into big_a (k, id, v, w, pad) values (:k, :id, 1, 1, 'bench') on conflict do nothing;
EOF
docker run -d --name "$run-pgbench" --network host -v "$tmp:/b" -e PGPASSWORD=app postgres:18 \
	bash -c "pgbench -h 127.0.0.1 -p 6432 -U app -n -c 8 -j 4 -T 600 -l --log-prefix=/b/read -f /b/read.sql postgres > /b/read.out 2>&1 &
	pgbench -h 127.0.0.1 -p 6433 -U app -n -c 4 -j 2 -T 600 -l --log-prefix=/b/write -f /b/write.sql postgres > /b/write.out 2>&1; wait" >/dev/null
sleep 10
m0=$(date +%s.%N)
mid="$(job "{\"op\": \"range.move\", \"keyspace\": \"k\", \"range\": \"$range\", \"to\": 4}")"
m1=$(date +%s.%N)
docker rm -f "$run-pgbench" >/dev/null
sql "$src" -c 'checkpoint' >/dev/null
lsn1="$(sql "$src" -c 'select pg_current_wal_lsn()')"
size1="$(sql "$src" -c 'select pg_database_size(current_database())')"
wal="$(sql "$src" -c "select pg_wal_lsn_diff('$lsn1', '$lsn0')::bigint")"
pause="$(admin GET "/v1/jobs/$mid" | python3 -c '
import json, sys
j = json.load(sys.stdin)
p = [s["detail"].get("cutover", {}).get("pause_ms") for s in j["steps"] if s["kind"] == "transfer"]
print(round(max([x for x in p if x is not None] or [0]), 1))')"
p99() { cat "$tmp"/$1.* 2>/dev/null | awk '{print $3}' | sort -n | awk '{a[NR]=$1} END {if (NR) printf "%.1f", a[int(NR*0.99)]/1000; else print "n/a"}'; }

# Joins: through router 1 and on the plain Postgres, three times each, the best.
best() {
	local target="$1" port="$2" q="$3" b=999999
	for _ in 1 2 3; do
		local conn ms
		if [ "$target" = router ]; then
			conn="host=$gw port=$port user=app password=app dbname=postgres"
		else
			conn="dbname=postgres user=postgres"
		fi
		ms="$(docker exec -i "$plain" psql "$conn" -XAq -c '\timing on' -c "$q" 2>&1 | sed -n 's/^Time: \([0-9.]*\) ms.*/\1/p' | tail -1)"
		ms="${ms%.*}"
		[ -n "$ms" ] && [ "$ms" -lt "$b" ] && b=$ms
	done
	echo "$b"
}
gw="$(docker network inspect -f '{{range .IPAM.Config}}{{.Gateway}}{{end}}' "$net")"
bq="select s.label, count(*) from big_a a join small s on s.v = a.v where a.w < 20000 group by s.label order by 2 desc limit 5"
wq="select s, v, rank() over (order by v desc, s) from small order by 3 limit 5"
broadcast_r="$(best router 6432 "$bq")"
broadcast_p="$(best plain 0 "$bq")"
window_r="$(best router 6432 "$wq")"
window_p="$(best plain 0 "$wq")"

# The advisor on a skewed load: the Zipf keys make a few ranges hot for reads. The autopilot runs
# what the advisor says, every 10 s, for 4 minutes of load: does it move the hot range, and does it
# stop moving (the hold window, gain over cost)?
advice="$(admin GET '/v1/advice?sample_ms=5000' | python3 -c '
import json, sys
a = json.load(sys.stdin)["advice"]
print("; ".join(x["request"]["op"] + " (" + x["metric"] + ")" for x in a) or "none")')"
# advice_min_reads_per_s is the branch's (Phase 3); main refuses it and runs without.
admin POST /v1/settings '{"autopilot": "auto", "autopilot_interval_ms": 10000, "advice_sample_ms": 3000, "advice_min_reads_per_s": 100}' >/dev/null 2>&1 ||
	admin POST /v1/settings '{"autopilot": "auto", "autopilot_interval_ms": 10000, "advice_sample_ms": 3000}' >/dev/null
docker run -d --name "$run-pgbench" --network host -v "$tmp:/b" -e PGPASSWORD=app postgres:18 \
	pgbench -h 127.0.0.1 -p 6432 -U app -n -c 8 -j 4 -T 240 -f /b/read.sql postgres >/dev/null
lepis_jobs() {
	admin GET /v1/jobs | python3 -c '
import json, sys
j = json.load(sys.stdin)
if isinstance(j, dict):
    j = next((v for v in j.values() if isinstance(v, list)), [])
print(sum(1 for x in j if isinstance(x, dict) and x.get("actor") == "lepis"))'
}
sleep 120
auto_first="$(lepis_jobs)"
sleep 120
auto_second="$(lepis_jobs)"
docker rm -f "$run-pgbench" >/dev/null
admin POST /v1/settings '{"autopilot": "off"}' >/dev/null

mkdir -p "$here/results"
{
	echo "# Chunked storage, $label ($(git -C "$stack" rev-parse --short HEAD)), $(date -u +%Y-%m-%d)"
	echo
	echo "$rows rows in each of big_a and big_b (Zipf s=1 keys, 2 secondary indexes each), 1M-row dim, 50k-row small; 4 nodes and a plain Postgres, Postgres 18, Docker, one 4-vCPU box."
	echo
	echo "| measure | value |"
	echo "| --- | ---: |"
	echo "| distribute 3 tables (s) | $distribute_s |"
	echo "| move 25% of node 2: time (s) | $(python3 -c "print(round($m1 - $m0, 1))") |"
	echo "| source WAL during the move (MB) | $((wal / 1000000)) |"
	echo "| source disk given back (MB) | $(((size0 - size1) / 1000000)) |"
	echo "| cutover write pause (ms) | $pause |"
	echo "| p99 read during the move (ms) | $(p99 read) |"
	echo "| p99 write during the move (ms) | $(p99 write) |"
	echo "| broadcast join, router vs plain (ms) | $broadcast_r vs $broadcast_p |"
	echo "| window over small (gather onto home), router vs plain (ms) | $window_r vs $window_p |"
	echo "| advice after the run | $advice |"
	echo "| autopilot jobs on skewed reads: first 2 min, next 2 min | $auto_first, $((auto_second - auto_first)) |"
} >"$out"
cat "$out"
