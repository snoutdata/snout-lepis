#!/usr/bin/env bash
# Lepis's benchmark (Phase 0): pgbench straight at Postgres, through Lepis,
# and through PgBouncer, in the same run on the same box, so the three differ only in what sits
# in the middle.
#
#   bash bench/pgbench.sh [seconds] [clients] [label]
#
# Runs anywhere Docker runs; the numbers that count are from the AWS box the plan names, and a
# laptop run is labelled as one. Writes bench/results/<date>-<label>.md.
#
# Workloads, each against each target:
#   select   pgbench -S -M prepared   one indexed read per transaction: the router's per-message cost
#   tpcb     pgbench    -M prepared   the default TPC-B-like mix: five statements in a transaction
#   connect  pgbench -S -C            a new connection per transaction: the login path (two SCRAM
#                                     exchanges through Lepis, auth_query + one through PgBouncer)
#
# Lepis in Phase 0 is a session pass-through, so PgBouncer runs in session mode for a like-for-like
# reading. With LEPIS_BENCH_ROUTED=1 the bench database gets a one-node catalog, so Lepis runs its
# router (Phase 1 on), and Lepis is measured twice: pool_mode session and pool_mode transaction. Everything it starts is named lepis-bench-<pid>-* and removed after, by name.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
stack="$(cd "$here/.." && pwd)"
engine="${STACK_ENGINE:-docker}"
seconds="${1:-30}"
clients="${2:-16}"
label="${3:-local}"  # a laptop run stays "local": a smoke test of the rig, never a result (X9)
routed="${LEPIS_BENCH_ROUTED:-}"
workloads="${LEPIS_BENCH_WORKLOADS:-select tpcb connect}"  # a subset, e.g. "select" for a 64-client run
run="lepis-bench-$$"
net="$run-net"
pg="$run-pg"
lepis="$run-lepis"
bouncer="$run-bouncer"
client="$run-client"
pgbouncer_image="docker.io/edoburu/pgbouncer:v1.24.1-p1"

cleanup() {
	"$engine" rm -fv "$pg" "$lepis" "$bouncer" "$client" >/dev/null 2>&1 || true
	"$engine" network rm "$net" >/dev/null 2>&1 || true
}
trap cleanup EXIT

"$engine" network create "$net" >/dev/null
"$engine" run -d --name "$pg" --network "$net" -e POSTGRES_PASSWORD=x \
	-e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 -e POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 \
	docker.io/library/postgres:18 -c max_connections=300 -c shared_buffers=256MB >/dev/null
for _ in 1 2; do
	until "$engine" exec "$pg" psql -U postgres -Atc 'select 1' >/dev/null 2>&1; do sleep 1; done
	sleep 2
done
"$engine" exec "$pg" psql -U postgres -qc "create role bench login password 'bench-pw'"
"$engine" exec "$pg" psql -U postgres -qc "create database bench owner bench"
"$engine" exec -e PGPASSWORD=bench-pw "$pg" pgbench -h 127.0.0.1 -U bench -i -s 20 -q bench >/dev/null 2>&1
service_db=postgres
if [ -n "$routed" ]; then
	# One node, the home node, and a catalog that distributes something: the router runs.
	cat "$here/../src/catalog.sql" "$here/../src/catalog_2pc.sql" "$here/../src/catalog_jobs.sql" |
		"$engine" exec -i "$pg" psql -q -U postgres -d bench -v ON_ERROR_STOP=1 >/dev/null
	"$engine" exec "$pg" psql -q -U postgres -d bench -v ON_ERROR_STOP=1 -c "insert into lepis.node (id, name, host, port, dbname, sslmode, kind, state) values (1, 'n1', '$pg', 5432, 'bench', 'disable', 'home', 'active'); insert into lepis.keyspace values ('acct', 'hash', 'integer', 0); insert into lepis.range values ('acct', -9223372036854775808, 9223372036854775807, 1); insert into lepis.relation values ('public', 'pgbench_accounts', 'sharded', 'acct', 'aid'), ('public', 'pgbench_history', 'global', null, null); select lepis.bump();" >/dev/null
	# The same data in a database the catalog does not cover: Lepis's pass-through, measured in
	# the same run as the router.
	"$engine" exec "$pg" psql -q -U postgres -c 'create database benchpt owner bench template bench' >/dev/null
	service_db=bench
fi

STACK_DEV_ARGS="-d --name $lepis --network $net -e LEPIS_HOME=$pg:5432 -e LEPIS_HOME_SSLMODE=disable \
-e LEPIS_SERVICE_USER=postgres -e LEPIS_SERVICE_PASSWORD=x -e LEPIS_SERVICE_DATABASE=$service_db -e LEPIS_LOG=warn" bash "$stack/scripts/dev.sh" \
	bash -c 'cargo build -q --release -p snout-lepis && echo built && exec /cache/target/release/snout-lepis' >/dev/null
until "$engine" logs "$lepis" 2>&1 | grep -q built; do
	"$engine" ps -q -f "name=$lepis" | grep -q . || { "$engine" logs "$lepis" >&2; exit 1; }
	sleep 1
done
sleep 1

"$engine" run -d --name "$bouncer" --network "$net" \
	-e DB_HOST="$pg" -e DB_USER=bench -e DB_PASSWORD=bench-pw -e DB_NAME=bench \
	-e AUTH_TYPE=scram-sha-256 -e POOL_MODE=session -e MAX_CLIENT_CONN=500 -e DEFAULT_POOL_SIZE=100 \
	-e LISTEN_PORT=5432 "$pgbouncer_image" >/dev/null
sleep 3

"$engine" run -d --name "$client" --network "$net" --entrypoint sleep docker.io/library/postgres:18 infinity >/dev/null

bench() {
	local host="$1" args="$2" db="${3:-bench}"
	"$engine" exec "$client" sh -c 'rm -f /tmp/pgl.*'
	"$engine" exec -e PGPASSWORD=bench-pw "$client" \
		pgbench -h "$host" -p 5432 -U bench -n -T "$seconds" -c "$clients" -j 4 -l --sampling-rate=0.1 --log-prefix=/tmp/pgl $args "$db" 2>&1
}
# p50 and p99 in ms, from the 10% per-transaction sample bench() logged (field 3 is microseconds).
percentiles() {
	"$engine" exec "$client" sh -c "cat /tmp/pgl.* | awk '{print \$3}' | sort -n | awk '{a[NR]=\$1} END {printf \"%.3f %.3f\", a[int(NR*0.5)+1]/1000, a[int(NR*0.99)+1]/1000}'"
}

out="$here/results/$(date -u +%Y-%m-%d)-$label.md"
mkdir -p "$here/results"
raw="$(mktemp)"
{
	echo "# Lepis Phase 0: pgbench, $label"
	echo
	echo "\`bash bench/pgbench.sh $seconds $clients $label\`, $(date -u +%FT%TZ)."
	echo "Host: $(uname -srm), $(nproc 2>/dev/null || echo '?') CPUs. postgres:18, scale 20, $clients clients, $seconds s each."
	if [ -n "$routed" ]; then
		echo "Lepis: the router (a one-node catalog), pool_mode session and transaction. PgBouncer $pgbouncer_image in session mode."
	else
		echo "Lepis: session pass-through (Phase 0). PgBouncer $pgbouncer_image in session mode."
	fi
	echo
	echo "| workload | target | tps | latency avg (ms) | vs direct | p50 (ms) | p99 (ms) | p50 vs direct |"
	echo "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |"
} >"$out"

for w in $workloads; do
	case "$w" in
		select) args="-S -M prepared" ;;
		tpcb) args="-M prepared" ;;
		connect) args="-S -C" ;;
	esac
	direct_lat=""
	targets="direct lepis pgbouncer"
	[ -n "$routed" ] && targets="direct lepis-passthrough lepis-session lepis-transaction pgbouncer"
	# LEPIS_BENCH_TARGETS reorders or narrows them (direct first: the others are read against it).
	targets="${LEPIS_BENCH_TARGETS:-$targets}"
	for target in $targets; do
		case "$target" in
			direct) host="$pg" ;;
			lepis-passthrough) host="$lepis" ;;
			lepis) host="$lepis" ;;
			lepis-*)
				host="$lepis"
				"$engine" exec "$pg" psql -q -U postgres -d bench -c "update lepis.cluster set settings = settings || jsonb_build_object('pool_mode', '${target#lepis-}') where id = 1" >/dev/null
				sleep 3 # the router reads its settings every 2 s
				;;
			pgbouncer) host="$bouncer" ;;
		esac
		echo "== $w via $target" >&2
		db=bench
		[ "$target" = lepis-passthrough ] && db=benchpt
		result="$(bench "$host" "$args" "$db")"
		printf '## %s via %s\n\n```\n%s\n```\n\n' "$w" "$target" "$result" >>"$raw"
		tps="$(grep -oE 'tps = [0-9.]+' <<<"$result" | head -1 | awk '{print $3}')"
		lat="$(grep -oE 'latency average = [0-9.]+' <<<"$result" | awk '{print $4}')"
		read -r p50 p99 <<<"$(percentiles)"
		if [ "$target" = direct ]; then
			direct_lat="$lat"
			direct_p50="$p50"
			delta="-"
			delta50="-"
		else
			delta="$(awk -v a="$lat" -v b="$direct_lat" 'BEGIN { printf "%+.0f µs", (a - b) * 1000 }')"
			delta50="$(awk -v a="$p50" -v b="$direct_p50" 'BEGIN { printf "%+.0f µs", (a - b) * 1000 }')"
		fi
		echo "| $w | $target | ${tps%.*} | $lat | $delta | $p50 | $p99 | $delta50 |" >>"$out"
		echo "   tps=$tps latency=${lat}ms p50=${p50}ms p99=${p99}ms" >&2
	done
done

{
	echo
	echo "## Raw"
	echo
	cat "$raw"
} >>"$out"
rm -f "$raw"
echo "wrote $out" >&2
