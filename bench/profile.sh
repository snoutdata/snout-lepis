#!/usr/bin/env bash
# Where the router spends its time: perf over Lepis while pgbench drives one workload through it.
# Linux with perf installed, run as root (on a bench host, not a laptop):
#
#   bash bench/profile.sh [select|tpcb] [session|transaction|passthrough] [clients] [seconds]
#
# With PROF_SYSCALLS=1 it counts system calls per transaction instead of sampling.
#
# Builds an unstripped release binary (the release profile with symbols), starts a Postgres and
# Lepis on the host network with the bench's one-node catalog (pgbench_accounts sharded by aid),
# records perf for `seconds` while pgbench runs, and prints the hottest functions. Everything it
# starts is named lepis-prof-<pid>-* and removed after, by name.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
stack="$(cd "$here/.." && pwd)"
engine="${STACK_ENGINE:-podman}"
workload="${1:-select}"
mode="${2:-session}"
clients="${3:-16}"
seconds="${4:-15}"
run="lepis-prof-$$"
pg="$run-pg"
pgport=55432
lport=56432
lepis_pid=""
cleanup() {
	[ -n "$lepis_pid" ] && kill "$lepis_pid" 2>/dev/null || true
	"$engine" rm -fv "$pg" "$run-client" >/dev/null 2>&1 || true
}
trap cleanup EXIT

STACK_DEV_ARGS="-e CARGO_PROFILE_RELEASE_STRIP=false -e CARGO_PROFILE_RELEASE_DEBUG=line-tables-only" \
	bash "$stack/scripts/dev.sh" cargo build -q --release -p snout-lepis
bin="$("$engine" volume inspect snout-stack-target --format '{{.Mountpoint}}')/release/snout-lepis"

"$engine" run -d --name "$pg" --network host -e POSTGRES_PASSWORD=x \
	-e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 -e POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 \
	docker.io/library/postgres:18 -c port=$pgport -c max_connections=300 -c shared_buffers=256MB >/dev/null
for _ in 1 2; do
	until "$engine" exec "$pg" psql -p $pgport -U postgres -Atc 'select 1' >/dev/null 2>&1; do sleep 1; done
	sleep 2
done
psql_() { "$engine" exec -i "$pg" psql -p $pgport -q -U postgres -v ON_ERROR_STOP=1 "$@"; }
psql_ -c "create role bench login password 'bench-pw'"
psql_ -c "create database bench owner bench"
"$engine" exec -e PGPASSWORD=bench-pw "$pg" pgbench -h 127.0.0.1 -p $pgport -U bench -i -s 20 -q bench >/dev/null 2>&1
cat "$here/../src/catalog.sql" "$here/../src/catalog_2pc.sql" "$here/../src/catalog_jobs.sql" | psql_ -d bench >/dev/null
psql_ -d bench -c "insert into lepis.node (id, name, host, port, dbname, sslmode, kind, state) values (1, 'n1', '127.0.0.1', $pgport, 'bench', 'disable', 'home', 'active'); insert into lepis.keyspace values ('acct', 'hash', 'integer', 0); insert into lepis.range values ('acct', -9223372036854775808, 9223372036854775807, 1); insert into lepis.relation values ('public', 'pgbench_accounts', 'sharded', 'acct', 'aid'), ('public', 'pgbench_history', 'global', null, null); update lepis.cluster set settings = settings || jsonb_build_object('pool_mode', '${mode/passthrough/session}') where id = 1; select lepis.bump();" >/dev/null

psql_ -c "create database benchpt owner bench template bench" >/dev/null

LEPIS_HOME=127.0.0.1:$pgport LEPIS_HOME_SSLMODE=disable LEPIS_SERVICE_USER=postgres LEPIS_SERVICE_PASSWORD=x \
	LEPIS_SERVICE_DATABASE=bench LEPIS_PORT=$lport LEPIS_HOST=127.0.0.1 LEPIS_LOG=warn "$bin" &
lepis_pid=$!
sleep 2

db=bench
[ "$mode" = passthrough ] && db=benchpt
args="-S -M prepared"
[ "$workload" = tpcb ] && args="-M prepared"
"$engine" run -d --name "$run-client" --network host --entrypoint sleep docker.io/library/postgres:18 infinity >/dev/null
"$engine" exec -e PGPASSWORD=bench-pw "$run-client" pgbench -h 127.0.0.1 -p $lport -U bench -n \
	-T $((seconds + 6)) -c "$clients" -j 4 $args "$db" >"$here/prof-pgbench.txt" 2>&1 &
bench=$!
sleep 3
if [ -n "${PROF_SYSCALLS:-}" ]; then
	perf stat -x, -e 'task-clock,raw_syscalls:sys_enter,syscalls:sys_enter_read,syscalls:sys_enter_write,syscalls:sys_enter_recvfrom,syscalls:sys_enter_sendto,syscalls:sys_enter_epoll_pwait,syscalls:sys_enter_futex' -p "$lepis_pid" -o "$here/prof-stat.txt" -- sleep "$seconds" >/dev/null 2>&1
	wait "$bench" || true
	grep -E 'tps|latency' "$here/prof-pgbench.txt"
	tps="$(grep -oE 'tps = [0-9.]+' "$here/prof-pgbench.txt" | awk '{print $3}')"
	awk -F, -v tps="$tps" -v s="$seconds" '$1 ~ /^[0-9.]+$/ { printf "%-40s %10.3f per transaction (task-clock in ms)\n", $3, $1 / (tps * s) }' "$here/prof-stat.txt"
	rm -f "$here/prof-stat.txt" "$here/prof-pgbench.txt"
	exit 0
fi
perf record -F 999 -g -p "$lepis_pid" -o "$here/perf.data" -- sleep "$seconds" >/dev/null 2>&1
wait "$bench" || true
grep -E 'tps|latency' "$here/prof-pgbench.txt"
echo "== by object"
perf report -i "$here/perf.data" --no-children --stdio --sort dso -g none 2>/dev/null | grep -E '^ +[0-9]' | head -8 || true
echo "== kernel and user, by function"
perf report -i "$here/perf.data" --no-children --stdio --sort symbol -g none 2>/dev/null | grep -E '^ +[0-9]' | head -25 || true
echo "== user space only"
perf report -i "$here/perf.data" --no-children --stdio --sort symbol -g none 2>/dev/null | grep -F '[.]' | head -60 || true
rm -f "$here/perf.data" "$here/perf.data.old" "$here/prof-pgbench.txt"
