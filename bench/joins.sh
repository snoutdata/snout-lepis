#!/usr/bin/env bash
# Two big tables joined on a column that is not their
# key, through the router and node to node, against one plain Postgres. Same cluster as chunks.sh.
#
#   bash bench/joins.sh <label> [rows] [mid rows]        e.g.  joins.sh branch 10000000
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
# Measured: a join of mid_a and mid_b (1M rows each) on id, through the router and node to node,
# and of big_a and big_b node to node (the router path's memory holds every row it moves), each
# compared with the plain Postgres, answer and time. Writes results/<date>-joins-<label>.md.
set -euo pipefail
label="${1:?a label}"
rows="${2:-10000000}"
mid="${3:-1000000}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
stack="$(cd "$here/.." && pwd)"
run=lepis-benchj
net="$run-net"
nodes=("$run-n1" "$run-n2" "$run-n3" "$run-n4")
plain="$run-plain"
token=bench-token-0123456789abcdef
out="$here/results/$(date -u +%Y-%m-%d)-joins-$label.md"
tmp="$(mktemp -d)"
pids=()

cleanup() {
	for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
	[ -n "${KEEP:-}" ] && return
	docker rm -fv "${nodes[@]}" "$plain" >/dev/null 2>&1 || true
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
create table mid_a as select * from big_a where id <= $mid;
alter table mid_a add primary key (k, id);
create table mid_b as select * from big_b where id <= $mid;
alter table mid_b add primary key (k, id);
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
job '{"op": "table.distribute", "table": "public.mid_a", "column": "k", "keyspace": "k"}' >/dev/null
job '{"op": "table.distribute", "table": "public.mid_b", "column": "k", "keyspace": "k"}' >/dev/null
distribute_s=$(($(date +%s) - t0))


# Each join through router 1 (with the session's settings first) and on the plain Postgres: the
# best of `n` runs, and the answer, which must be the plain one's.
gw="$(docker network inspect -f '{{range .IPAM.Config}}{{.Gateway}}{{end}}' "$net")"
run_q() {
	local target="$1" sets="$2" q="$3" n="$4" b=999999999 got=""
	for _ in $(seq 1 "$n"); do
		local conn outp ms
		if [ "$target" = router ]; then
			conn="host=$gw port=6432 user=app password=app dbname=postgres"
		else
			conn="dbname=postgres user=postgres"
		fi
		local args=()
		IFS=';' read -ra parts <<<"$sets"
		for p in "${parts[@]}"; do
			[ -n "${p// /}" ] && args+=(-c "$p")
		done
		outp="$(docker exec -i "$plain" psql "$conn" -XAtq "${args[@]}" -c '\timing on' -c "$q" 2>&1)"
		ms="$(printf '%s\n' "$outp" | sed -n 's/^Time: \([0-9.]*\) ms.*/\1/p' | tail -1)"
		got="$(printf '%s\n' "$outp" | grep -v '^Time:' | tail -1)"
		ms="${ms%.*}"
		[ -n "$ms" ] && [ "$ms" -lt "$b" ] && b=$ms
	done
	echo "$b|$got"
}
off="set lepis.join_broadcast_rows = 0; set lepis.join_gather_rows = 0; set lepis.join_timeout_ms = 1800000"
router_path="$off; set lepis.join_shuffle_rows = 0; set lepis.join_repartition_bytes = 2000000000"
node_path="$off; set lepis.join_repartition_bytes = 0"
qm="select count(*), sum(a.w::bigint + b.x) from mid_a a join mid_b b on b.id = a.id"
qb="select count(*), sum(a.w::bigint + b.x) from big_a a join big_b b on b.id = a.id"
qg="select a.v % 10, count(*) from big_a a join big_b b on b.id = a.id where b.x < 500000 group by 1 order by 1 limit 3"
echo "== joins" >&2
mid_plain="$(run_q plain "select 1" "$qm" 3)"
mid_router="$(run_q router "$router_path" "$qm" 3)"
mid_node="$(run_q router "$node_path" "$qm" 3)"
big_plain="$(run_q plain "select 1" "$qb" 2)"
big_node="$(run_q router "$node_path" "$qb" 2)"
grp_plain="$(run_q plain "select 1" "$qg" 2)"
grp_node="$(run_q router "$node_path" "$qg" 2)"
left="$(for n in "${nodes[@]}"; do sql "$n" -c "select count(*) from pg_class c join pg_namespace s on s.oid = c.relnamespace where s.nspname = 'lepis_shuffle'"; done | awk "{s += \$1} END {print s + 0}")"
same() { [ "${1#*|}" = "${2#*|}" ] && echo yes || echo "NO (${1#*|} vs ${2#*|})"; }

mkdir -p "$here/results"
{
	echo "# Repartitioned joins, $label ($(git -C "$stack" rev-parse --short HEAD)), $(date -u +%Y-%m-%d)"
	echo
	echo "big_a, big_b: $rows rows each; mid_a, mid_b: $mid rows each; all distributed by k (chunked), joined on id (not the key). 4 nodes and a plain Postgres, Postgres 18, Docker, one 4-vCPU box. Best of 3 (mid) or 2 (big), ms."
	echo
	echo "| join | router | plain | same answer |"
	echo "| --- | ---: | ---: | --- |"
	echo "| mid ⋈ mid, through the router | ${mid_router%%|*} | ${mid_plain%%|*} | $(same "$mid_router" "$mid_plain") |"
	echo "| mid ⋈ mid, node to node | ${mid_node%%|*} | ${mid_plain%%|*} | $(same "$mid_node" "$mid_plain") |"
	echo "| big ⋈ big, node to node | ${big_node%%|*} | ${big_plain%%|*} | $(same "$big_node" "$big_plain") |"
	echo "| big ⋈ big filtered + group, node to node | ${grp_node%%|*} | ${grp_plain%%|*} | $(same "$grp_node" "$grp_plain") |"
	echo "| shuffle tables left on the nodes | $left | | |"
} >"$out"
cat "$out"
