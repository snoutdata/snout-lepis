#!/usr/bin/env bash
# Lepis's scale-out benchmark: throughput through one router (and two) as the same data set is
# spread over 1, 2, 4 and 8 data nodes, each node a machine of its own. pgbench.sh measures what
# the router costs on one box; this measures what the nodes add when there are more of them, and
# where the router stops keeping up.
#
#   bash bench/scaleout.sh --routers sd-a[,sd-b] --client sd-c --nodes sd-1,...,sd-8 [label]
#
# Run from a workstation with the fleet account's credentials. The hosts are throwaways made with
# the fleet's `create-host.sh --throwaway` (the script refuses any host not tagged Throwaway=true,
# so it can never be pointed at a fleet host), reached through Session Manager by the fleet's
# `host-exec.sh` (SCALEOUT_HOST_EXEC names another runner with its interface); they talk to each other on their private addresses inside the fleet's security
# group, on ports it already opens between its members (15432-16431). Nothing is enrolled. The
# script does not create or delete hosts: the person running it does both, by exact instance id.
# Writes bench/results/<date>-<label>.md and the pgbench output to results/raw/.
#
# What runs where, all rootful Podman with the host's network (no NAT in the measured path):
#   nodes    postgres:18 on :15432, its data on the host's data volume; node 1 is the home node
#            and holds the catalog
#   routers  Lepis (LEPIS_IMAGE, the fleet's ECR build by default) on :15433, session pool mode
#   client   pgbench from postgres:18; it also loads the data and writes the catalog over the
#            network, so the per-N steps are psql from one place
#   every    a CPU probe on :15499 that answers each connection with /proc/stat, read by the
#            client a few seconds into each run and again just before it ends, so every row says
#            how busy the router, the nodes and the client were while it was measured
#
# The data set is the same total at every N: ACCOUNTS rows of pgbench_accounts distributed by aid
# (keyspace acct), with pgbench_history colocated by aid, and a TPC-B-shaped set distributed by
# branch (keyspace branch: tpcb_branches, tpcb_tellers, tpcb_accounts, tpcb_history, BRANCHES
# branches of 10 tellers and 1,000 accounts each). Each node is loaded with only the rows its
# range owns (generated on the node, filtered by the same hash its fence then checks), and fenced
# with `lepis_owns` like a real cluster. One equal hash range per node.
#
# Workloads, each `-M prepared`, every key uniform over the whole data set:
#   select   one point read by aid                                        (pgbench -S's statement)
#   write    BEGIN; UPDATE pgbench_accounts by aid; INSERT pgbench_history; END   (one node)
#   tpcb     the TPC-B mix (5 statements) with the account, teller and branch of ONE branch, so the
#            transaction stays on one node: routed by branch, not pgbench's own mix, whose three
#            independent keys would make nearly every transaction a two-phase one across nodes
#
# Targets, at each client count (the total over all endpoints, split evenly):
#   direct   N=1 only: pgbench straight at the one node, the same scripts: what the router costs
#   keyed    pgbench straight at every node at once, each process picking keys its node owns from
#            a per-node key table (one extra index probe per statement, a subselect; tpcb: one
#            extra round trip, a \gset before BEGIN): what the nodes do with no router at all
#   routed1  every client through router 1
#   routed2  half the clients through each of two routers sharing the one catalog (Lepis keeps no
#            state but its connections, so any number of routers run against one home node)
#
# Before each workload's runs at a node count, WARMUP seconds of it through router 1, unmeasured;
# after each load every table and index is read into memory (pg_prewarm). Both because a smoke
# run's first rows were the cold cache and the full-page images after the load's checkpoint, not
# the target. Session pool mode gives every client a backend on every node it touches, so a node
# sees all the clients at once: max_connections is 600, not the one-box rig's 300.
#
# Past about 300 clients through the routers the home node runs out of connections (600): it holds
# every client's session and, for each session whose first transaction committed in two phases,
# the router's connection to the two-phase commit log. Those runs say so in their error column.
#
# Environment: LEPIS_IMAGE, LEPIS_SCALEOUT_SECONDS (30), LEPIS_SCALEOUT_CLIENTS ("64 128 256"),
# LEPIS_SCALEOUT_CLIENTS_<n> (the counts at n nodes, e.g. LEPIS_SCALEOUT_CLIENTS_8="128 256 512"),
# LEPIS_SCALEOUT_WARMUP (20), LEPIS_SCALEOUT_COUNTS ("1 2 4 8"), LEPIS_SCALEOUT_WORKLOADS
# ("select write tpcb"), LEPIS_SCALEOUT_TWO_AT (node counts that also get routed2, "4 8"),
# LEPIS_SCALEOUT_ACCOUNTS (10000000: pgbench scale 100), LEPIS_SCALEOUT_BRANCHES (4000).
# Everything it starts on a host is named lepis-so-*, and stays until the host is terminated.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
host_exec="${SCALEOUT_HOST_EXEC:-$here/../../../snoutpod/aws/host-exec.sh}"
region="${AWS_REGION:-us-west-2}"
image="${LEPIS_IMAGE:-$(aws sts get-caller-identity --query Account --output text).dkr.ecr.$region.amazonaws.com/s-fleet/lepis:0.1.11}"
seconds="${LEPIS_SCALEOUT_SECONDS:-30}"
client_counts="${LEPIS_SCALEOUT_CLIENTS:-64 128 256}"
warmup="${LEPIS_SCALEOUT_WARMUP:-20}"
node_counts="${LEPIS_SCALEOUT_COUNTS:-1 2 4 8}"
workloads="${LEPIS_SCALEOUT_WORKLOADS:-select write tpcb}"
two_at="${LEPIS_SCALEOUT_TWO_AT:-4 8}"
accounts="${LEPIS_SCALEOUT_ACCOUNTS:-10000000}"
branches="${LEPIS_SCALEOUT_BRANCHES:-4000}"
routers_arg="" client_host="" nodes_arg="" label="aws-scaleout"
while [ $# -gt 0 ]; do
	case "$1" in
		--routers) routers_arg="$2"; shift 2 ;;
		--client) client_host="$2"; shift 2 ;;
		--nodes) nodes_arg="$2"; shift 2 ;;
		--*) echo "unknown argument: $1" >&2; exit 2 ;;
		*) label="$1"; shift ;;
	esac
done
[ -n "$routers_arg" ] && [ -n "$client_host" ] && [ -n "$nodes_arg" ] ||
	{ echo "usage: $0 --routers sd-a[,sd-b] --client sd-c --nodes sd-1,...,sd-8 [label]" >&2; exit 2; }
IFS=, read -r -a router_hosts <<<"$routers_arg"
IFS=, read -r -a node_hosts <<<"$nodes_arg"
for n in $node_counts; do
	[ "$n" -le "${#node_hosts[@]}" ] || { echo "a run on $n nodes needs $n --nodes" >&2; exit 2; }
done

say() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
on_host() { bash "$host_exec" --region "$region" --host "$1" --stdin; }

# Each host's private address, and the guard: a host that is not a throwaway is never touched.
# (Plain arrays and a lookup, not an associative array: macOS still ships bash 3.2.)
all_hosts=()
all_ips=()
for h in "${router_hosts[@]}" "$client_host" "${node_hosts[@]}"; do
	read -r addr throwaway <<<"$(aws ec2 describe-instances --region "$region" \
		--filters "Name=tag:Name,Values=$h" Name=instance-state-name,Values=running \
		--query 'Reservations[0].Instances[0].[PrivateIpAddress, Tags[?Key==`Throwaway`]|[0].Value]' --output text)"
	[ "$throwaway" = true ] || { echo "refusing $h: not a running host tagged Throwaway=true" >&2; exit 1; }
	all_hosts+=("$h")
	all_ips+=("$addr")
done
ip_of() { local i; for i in "${!all_hosts[@]}"; do [ "${all_hosts[$i]}" != "$1" ] || echo "${all_ips[$i]}"; done; }
home_ip="$(ip_of "${node_hosts[0]}")"
node_ips="" router_ips="" probes="client=$(ip_of "$client_host")"
for h in "${node_hosts[@]}"; do node_ips="$node_ips $(ip_of "$h")"; done
for i in "${!router_hosts[@]}"; do
	router_ips="$router_ips $(ip_of "${router_hosts[$i]}")"
	probes="$probes router$((i + 1))=$(ip_of "${router_hosts[$i]}")"
done
for i in "${!node_hosts[@]}"; do probes="$probes n$((i + 1))=$(ip_of "${node_hosts[$i]}")"; done
pw="$(openssl rand -hex 16)"       # the nodes' superuser, which is also Lepis's service login
bench_pw="$(openssl rand -hex 16)" # the client role, one verifier on every node

# --- 1. Every host: wait for its first boot, start the CPU probe, fetch its images -------------
probe='import socket
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("0.0.0.0", 15499))
s.listen(64)
while True:
    c, _ = s.accept()
    try:
        c.sendall(open("/proc/stat", "rb").read())
    except OSError:
        pass
    c.close()'
prepare() {
	local h="$1" role="$2"
	{
		cat <<SCRIPT
set -euo pipefail
cloud-init status --wait >/dev/null 2>&1 || true
mkdir -p /var/tmp/lepis-so
systemctl stop lepis-so-cpu >/dev/null 2>&1 || true
systemd-run --quiet --unit=lepis-so-cpu --collect python3 -c '$probe'
echo "\$(hostname) \$(nproc) CPUs, \$(awk '/MemTotal/ {printf "%.1f GB", \$2 / 1048576}' /proc/meminfo), \$(podman --version)"
SCRIPT
		case "$role" in
			node)
				# shared_buffers a quarter of memory; the data on the data volume, fresh each time.
				cat <<SCRIPT
podman pull -q docker.io/library/postgres:18 >/dev/null
podman rm -fv lepis-so-pg >/dev/null 2>&1 || true
data=/srv/snoutpod/lepis-so
mountpoint -q /srv/snoutpod || data=/var/tmp/lepis-so/pg
rm -rf "\$data" && mkdir -p "\$data"
sb="\$(awk '/MemTotal/ {printf "%dMB", \$2 / 4096}' /proc/meminfo)"
podman run -d --name lepis-so-pg --network host --shm-size 1g --ulimit nofile=65536:65536 --pids-limit -1 \
	-v "\$data":/var/lib/postgresql -e POSTGRES_PASSWORD=$pw \
	-e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 -e POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 \
	docker.io/library/postgres:18 -c port=15432 -c max_connections=600 -c shared_buffers="\$sb" \
	-c max_prepared_transactions=64 -c max_wal_size=6GB -c checkpoint_timeout=20min >/dev/null
for _ in 1 2; do
	until podman exec lepis-so-pg pg_isready -q -p 15432 -h 127.0.0.1; do sleep 1; done
	sleep 2
done
echo "postgres up, shared_buffers \$sb, data in \$data"
SCRIPT
				;;
			router)
				cat <<SCRIPT
aws ecr get-login-password --region $region | podman login --username AWS --password-stdin ${image%%/*} >/dev/null
podman pull -q $image >/dev/null
echo "have $image"
SCRIPT
				;;
			client)
				cat <<SCRIPT
podman pull -q docker.io/library/postgres:18 >/dev/null
podman rm -fv lepis-so-client >/dev/null 2>&1 || true
podman run -d --name lepis-so-client --network host --ulimit nofile=65536:65536 --pids-limit -1 \
	-v /var/tmp/lepis-so:/work --entrypoint sleep docker.io/library/postgres:18 infinity >/dev/null
echo "client container up"
SCRIPT
				;;
		esac
	} | on_host "$h" 2>&1 | sed "s/^/  [$h $role] /"
}
say "preparing ${#node_hosts[@]} nodes, ${#router_hosts[@]} router(s) and the client"
pids=()
for h in "${node_hosts[@]}"; do prepare "$h" node & pids+=($!); done
for h in "${router_hosts[@]}"; do prepare "$h" router & pids+=($!); done
prepare "$client_host" client & pids+=($!)
for p in "${pids[@]}"; do wait "$p" || { echo "a host did not prepare" >&2; exit 1; }; done

# --- 2. The driver, on the client: everything from here is psql and pgbench from one place -----
# Shipped once with the catalog's DDL and the run's settings, then called step by step.
# read -d '', not $(cat <<…): bash 3.2 misparses the case patterns inside a command substitution.
read -r -d '' driver <<'DRIVER' || true
#!/usr/bin/env bash
set -euo pipefail
. /work/env
nodes=($NODE_IPS)
routers=($ROUTER_IPS)
export PGCONNECT_TIMEOUT=5
sql() { local h="$1"; shift; PGOPTIONS="-c client_min_messages=warning" PGPASSWORD="$PW" psql -X -q -v ON_ERROR_STOP=1 -h "$h" -p 15432 -U postgres "$@"; }

# One /proc/stat "cpu" line from every probe, at once, into /work/cpu.<tag>.<name>.
sample() {
	# Waits for its own reads only: a bare `wait` here also waited for the pgbench processes, which
	# put the first sample at the end of the run (a first full run's CPU figures were halved by it).
	local tag="$1" p pids=()
	for p in $PROBES; do
		(timeout 3 bash -c "cat </dev/tcp/${p#*=}/15499" 2>/dev/null | head -1 >"/work/cpu.$tag.${p%%=*}") &
		pids+=($!)
	done
	wait "${pids[@]}"
}
# Busy and iowait, percent of the whole host, between two samples of one probe.
busy() {
	paste -d' ' "/work/cpu.a.$1" "/work/cpu.b.$1" 2>/dev/null | awk '{
		t1 = 0; for (i = 2; i <= 9; i++) t1 += $i
		t2 = 0; for (i = 13; i <= 20; i++) t2 += $i
		dt = t2 - t1; idle = ($16 + $17) - ($5 + $6)
		if (dt > 0) printf "%.0f %.0f", 100 * (dt - idle) / dt, 100 * ($17 - $6) / dt; else printf "- -"
	}'
}

case "$1" in
check)
	# Can the client reach every node, router port and probe, and every probe answer?
	for h in "${nodes[@]}"; do
		pg_isready -q -h "$h" -p 15432 && echo "node $h:15432 reachable" || { echo "node $h:15432 UNREACHABLE"; exit 1; }
	done
	sample a
	for p in $PROBES; do
		grep -q '^cpu ' "/work/cpu.a.${p%%=*}" && echo "probe ${p%%=*} ${p#*=}:15499 answers" || { echo "probe $p SILENT"; exit 1; }
	done
	;;
setup)
	# The client role: its password set on node 1, its verifier copied verbatim to the others, so
	# the key a client's SCRAM proof reveals logs it into every node (README, Roles).
	sql "${nodes[0]}" -c "create role bench login password '$BENCH_PW'"
	verifier="$(sql "${nodes[0]}" -Atc "select rolpassword from pg_authid where rolname = 'bench'")"
	for h in "${nodes[@]:1}"; do sql "$h" -c "create role bench login password '$verifier'"; done
	for h in "${nodes[@]}"; do
		sql "$h" -c "create database bench"
		sql "$h" -d bench -c "grant all on schema public to bench" \
			-c "alter default privileges in schema public grant all on tables to bench"
		echo "node $h: role bench, database bench, PG $(sql "$h" -Atc 'show server_version')"
	done
	;;
load)
	n="$2"
	# The catalog goes first (its event trigger would rewrite it as the tables are dropped), then
	# every node's tables, including the nodes beyond n, so an idle node holds nothing in memory.
	sql "${nodes[0]}" -d bench -c "drop event trigger if exists lepis_forget_dropped" -c "drop schema if exists lepis cascade"
	for h in "${nodes[@]}"; do
		sql "$h" -d bench -c "drop table if exists pgbench_accounts, pgbench_history, bench_keys, tpcb_branches, tpcb_tellers, tpcb_accounts, tpcb_history, tpcb_bkeys" &
	done
	wait
	# n equal ranges over the signed 64-bit hash space; node i owns range i in both keyspaces.
	mapfile -t ranges < <(sql "${nodes[0]}" -Atc "select (-9223372036854775808::numeric + w * i)::bigint || ' ' ||
		case when i = $n - 1 then 9223372036854775807 else (-9223372036854775808::numeric + w * i + w - 1)::bigint end
		from (select floor(18446744073709551616::numeric / $n) w) s, generate_series(0, $n - 1) i order by i")
	for i in $(seq 0 $((n - 1))); do
		read -r lo hi <<<"${ranges[$i]}"
		h="${nodes[$i]}"
		# Generated on the node, only the rows its range owns, then fenced with the same expression.
		sql "$h" -d bench <<SQL &
create table pgbench_accounts (aid int not null, bid int, abalance int, filler char(84));
insert into pgbench_accounts select aid, (aid - 1) / 100000 + 1, 0, ''
	from generate_series(1, $ACCOUNTS) aid where hashint4extended(aid, 0) between $lo and $hi;
alter table pgbench_accounts add primary key (aid);
create table pgbench_history (tid int, bid int, aid int, delta int, mtime timestamp, filler char(22));
create table bench_keys as select (row_number() over (order by aid))::int i, aid from pgbench_accounts;
alter table bench_keys add primary key (i);
create table tpcb_branches (bid int primary key, bbalance int, filler char(88));
insert into tpcb_branches select bid, 0, ''
	from generate_series(1, $BRANCHES) bid where hashint4extended(bid, 7) between $lo and $hi;
create table tpcb_tellers (tid int, bid int, tbalance int, filler char(84), primary key (bid, tid));
insert into tpcb_tellers select (b.bid - 1) * 10 + t, b.bid, 0, '' from tpcb_branches b, generate_series(1, 10) t;
create table tpcb_accounts (aid int, bid int, abalance int, filler char(84), primary key (bid, aid));
insert into tpcb_accounts select (b.bid - 1) * 1000 + a, b.bid, 0, '' from tpcb_branches b, generate_series(1, 1000) a;
create table tpcb_history (tid int, bid int, aid int, delta int, mtime timestamp, filler char(22));
create table tpcb_bkeys as select (row_number() over (order by bid))::int j, bid from tpcb_branches;
alter table tpcb_bkeys add primary key (j);
alter table pgbench_accounts add constraint lepis_owns check (hashint4extended(aid, 0) between $lo and $hi) not valid;
alter table pgbench_history add constraint lepis_owns check (hashint4extended(aid, 0) between $lo and $hi) not valid;
alter table tpcb_branches add constraint lepis_owns check (hashint4extended(bid, 7) between $lo and $hi) not valid;
alter table tpcb_tellers add constraint lepis_owns check (hashint4extended(bid, 7) between $lo and $hi) not valid;
alter table tpcb_accounts add constraint lepis_owns check (hashint4extended(bid, 7) between $lo and $hi) not valid;
alter table tpcb_history add constraint lepis_owns check (hashint4extended(bid, 7) between $lo and $hi) not valid;
vacuum analyze;
checkpoint;
-- Everything in memory before the first run, so no run pays for the cold cache (a smoke run's
-- first reads waited on the disk).
create extension if not exists pg_prewarm;
select sum(pg_prewarm(c.oid)) from pg_class c join pg_namespace s on s.oid = c.relnamespace
	where s.nspname = 'public' and c.relkind in ('r', 'i') \g /dev/null
SQL
	done
	wait
	# The catalog on node 1, the home node.
	cat /work/catalog.sql /work/catalog_2pc.sql /work/catalog_jobs.sql | sql "${nodes[0]}" -d bench >/dev/null
	{
		echo "begin;"
		for i in $(seq 0 $((n - 1))); do
			kind=data
			[ "$i" = 0 ] && kind=home
			echo "insert into lepis.node (id, name, host, port, dbname, sslmode, kind, state, server_version_num)
				values ($((i + 1)), 'n$((i + 1))', '${nodes[$i]}', 15432, 'bench', 'disable', '$kind', 'active', $(sql "${nodes[$i]}" -Atc 'show server_version_num'));"
		done
		echo "insert into lepis.keyspace values ('acct', 'hash', 'integer', 0), ('branch', 'hash', 'integer', 7);"
		for i in $(seq 0 $((n - 1))); do
			read -r lo hi <<<"${ranges[$i]}"
			echo "insert into lepis.range values ('acct', $lo, $hi, $((i + 1))), ('branch', $lo, $hi, $((i + 1)));"
		done
		cat <<'SQL'
insert into lepis.relation values
	('public', 'pgbench_accounts', 'sharded', 'acct', 'aid'),
	('public', 'pgbench_history', 'sharded', 'acct', 'aid'),
	('public', 'tpcb_branches', 'sharded', 'branch', 'bid'),
	('public', 'tpcb_tellers', 'sharded', 'branch', 'bid'),
	('public', 'tpcb_accounts', 'sharded', 'branch', 'bid'),
	('public', 'tpcb_history', 'sharded', 'branch', 'bid');
select lepis.bump() \g /dev/null
commit;
SQL
	} | sql "${nodes[0]}" -d bench
	for i in $(seq 0 $((n - 1))); do
		read -r nk nb <<<"$(sql "${nodes[$i]}" -d bench -Atc "select (select count(*) from bench_keys) || ' ' || (select count(*) from tpcb_bkeys)")"
		echo "$nk $nb" >"/work/keys.$((i + 1))"
		echo "n$((i + 1)) ${nodes[$i]}: $nk accounts, $nb branches, $(sql "${nodes[$i]}" -d bench -Atc "select pg_size_pretty(pg_database_size('bench'))")"
	done
	;;
wait-routers)
	for r in "${routers[@]}"; do
		for _ in $(seq 60); do pg_isready -q -h "$r" -p 15433 && break; sleep 1; done
		pg_isready -q -h "$r" -p 15433 || { echo "router $r:15433 not answering"; exit 1; }
		# One routed read, to see the catalog loaded and the client role logging in.
		PGPASSWORD="$BENCH_PW" psql -X -h "$r" -p 15433 -U bench -d bench -Atc 'select count(*) from pgbench_accounts where aid = 1' >/dev/null &&
			echo "router $r:15433 routes" || { echo "router $r:15433 does not route"; exit 1; }
	done
	;;
run)
	n="$2" w="$3" modes="$4" clients="$5"
	cat >/work/select.sql <<SQL
\set aid random(1, $ACCOUNTS)
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
SQL
	cat >/work/select-keyed.sql <<'SQL'
\set i random(1, :nk)
SELECT abalance FROM pgbench_accounts WHERE aid = (SELECT aid FROM bench_keys WHERE i = :i);
SQL
	cat >/work/write.sql <<SQL
\set aid random(1, $ACCOUNTS)
\set bid (:aid - 1) / 100000 + 1
\set delta random(-5000, 5000)
BEGIN;
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (0, :bid, :aid, :delta, CURRENT_TIMESTAMP);
END;
SQL
	cat >/work/write-keyed.sql <<'SQL'
\set i random(1, :nk)
\set delta random(-5000, 5000)
BEGIN;
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = (SELECT aid FROM bench_keys WHERE i = :i);
INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) SELECT 0, (aid - 1) / 100000 + 1, aid, :delta, CURRENT_TIMESTAMP FROM bench_keys WHERE i = :i;
END;
SQL
	tpcb_body='\set tid (:bid - 1) * 10 + random(1, 10)
\set aid (:bid - 1) * 1000 + random(1, 1000)
\set delta random(-5000, 5000)
BEGIN;
UPDATE tpcb_accounts SET abalance = abalance + :delta WHERE bid = :bid AND aid = :aid;
SELECT abalance FROM tpcb_accounts WHERE bid = :bid AND aid = :aid;
UPDATE tpcb_tellers SET tbalance = tbalance + :delta WHERE bid = :bid AND tid = :tid;
UPDATE tpcb_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
INSERT INTO tpcb_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
END;'
	printf '\\set bid random(1, %s)\n%s\n' "$BRANCHES" "$tpcb_body" >/work/tpcb.sql
	printf '\\set j random(1, :nb)\nSELECT bid FROM tpcb_bkeys WHERE j = :j \\gset\n%s\n' "$tpcb_body" >/work/tpcb-keyed.sql
	# Unmeasured first: WARMUP seconds through router 1, so the first measured write run does not
	# carry the full-page images every page writes once after the load's checkpoint.
	PGPASSWORD="$BENCH_PW" pgbench -h "${routers[0]}" -p 15433 -U bench -n -M prepared -T "$WARMUP" -c 64 -j 16 \
		-D nk=0 -D nb=0 -f "/work/$w.sql" bench >/dev/null 2>&1 || echo "warm-up of $w failed"
	for c in $clients; do
		for mode in $modes; do
			# Endpoints for this mode: host, port, script, and the key-table size for keyed.
			eps=()
			case "$mode" in
				direct) eps=("${nodes[0]} 15432 $w.sql 0 0") ;;
				routed1) eps=("${routers[0]} 15433 $w.sql 0 0") ;;
				routed2) for r in "${routers[@]:0:2}"; do eps+=("$r 15433 $w.sql 0 0"); done ;;
				keyed) for i in $(seq 0 $((n - 1))); do eps+=("${nodes[$i]} 15432 $w-keyed.sql $(cat /work/keys.$((i + 1)))"); done ;;
			esac
			e=${#eps[@]}
			per=$((c / e))
			[ "$per" -ge 1 ] || continue
			threads=$((CLIENT_CPUS / e))
			[ "$threads" -ge 1 ] || threads=1
			[ "$threads" -le "$per" ] || threads=$per
			rm -f /work/pgl.* /work/out.* /work/cpu.a.* /work/cpu.b.*
			k=0
			for ep in "${eps[@]}"; do
				read -r host port script nk nb <<<"$ep"
				PGPASSWORD="$BENCH_PW" pgbench -h "$host" -p "$port" -U bench -n -M prepared -T "$SECONDS_" -c "$per" -j "$threads" \
					-D nk="$nk" -D nb="$nb" -f "/work/$script" -l --sampling-rate=0.1 --log-prefix="/work/pgl.$k" bench >"/work/out.$k" 2>&1 &
				k=$((k + 1))
			done
			sleep 3
			sample a
			sleep $((SECONDS_ - 5))
			sample b
			wait || true
			tps="$(cat /work/out.* | grep -oE '^tps = [0-9.]+' | awk '{s += $3} END {printf "%.0f", s}')"
			failed="$(cat /work/out.* | grep -oE 'number of failed transactions: [0-9]+' | awk '{s += $5} END {print s + 0}')"
			aborted="$(cat /work/out.* | grep -c 'aborted\|error' || true)"
			read -r p50 p99 <<<"$(cat /work/pgl.* 2>/dev/null | awk '{print $3}' | sort -n |
				awk '{a[NR] = $1} END {if (NR) printf "%.3f %.3f", a[int(NR * 0.5) + 1] / 1000, a[int(NR * 0.99) + 1] / 1000; else print "- -"}')"
			cpus=""
			for p in $PROBES; do cpus="$cpus ${p%%=*}=$(busy "${p%%=*}" | tr ' ' /)"; done
			line="$n $w $mode $c $tps $p50 $p99 $failed $aborted$cpus"
			echo "$line" | tee -a /work/results.txt
			{
				echo "## N=$n $w $mode, $c clients ($e endpoint(s), $per clients and $threads threads each)"
				echo
				echo '```'
				for f in /work/out.*; do cat "$f"; echo; done
				echo '```'
				echo
			} >>/work/raw.md
		done
	done
	;;
esac
DRIVER
client_cpus="$(echo 'nproc' | on_host "$client_host")"
say "shipping the driver to the client ($client_cpus CPUs)"
{
	echo 'set -euo pipefail'
	echo 'mkdir -p /var/tmp/lepis-so && cd /var/tmp/lepis-so && rm -f results.txt raw.md'
	for f in catalog.sql catalog_2pc.sql catalog_jobs.sql; do
		echo "echo $(base64 <"$here/../src/$f" | tr -d '\n') | base64 -d >$f"
	done
	echo "echo $(printf '%s\n' "$driver" | base64 | tr -d '\n') | base64 -d >driver.sh"
	cat <<ENV
cat >env <<'E'
NODE_IPS="$node_ips"
ROUTER_IPS="$router_ips"
PROBES="$probes"
PW=$pw
BENCH_PW=$bench_pw
ACCOUNTS=$accounts
BRANCHES=$branches
SECONDS_=$seconds
WARMUP=$warmup
CLIENT_CPUS=$client_cpus
E
ENV
} | on_host "$client_host" >/dev/null
drive() { echo "podman exec lepis-so-client bash /work/driver.sh $*" | on_host "$client_host"; }

# --- 3. Reachability first: the client to every node and probe, each router to the home node ---
say "connection test"
drive check
for h in "${router_hosts[@]}"; do
	echo "timeout 3 bash -c '</dev/tcp/$home_ip/15432' && echo '$h reaches the home node on 15432'" | on_host "$h"
done
drive setup

start_routers() {
	local h pids=()
	for h in "${router_hosts[@]}"; do
		cat <<SCRIPT | on_host "$h" & pids+=($!)
podman rm -fv lepis-so-router >/dev/null 2>&1 || true
podman run -d --name lepis-so-router --network host --ulimit nofile=1048576:1048576 --pids-limit -1 \
	-e LEPIS_HOME=$home_ip:15432 -e LEPIS_HOME_SSLMODE=disable -e LEPIS_PORT=15433 \
	-e LEPIS_SERVICE_USER=postgres -e LEPIS_SERVICE_PASSWORD=$pw -e LEPIS_SERVICE_DATABASE=bench \
	-e LEPIS_LOG=warn $image >/dev/null
SCRIPT
	done
	for p in "${pids[@]}"; do wait "$p"; done
	drive wait-routers
}

# --- 4. Each node count: load, start the routers on the new catalog, run the matrix -----------
for n in $node_counts; do
	# LEPIS_SCALEOUT_CLIENTS_<n> overrides the client counts at one node count.
	var="LEPIS_SCALEOUT_CLIENTS_$n"
	clients="${!var:-$client_counts}"
	say "N=$n: loading $accounts accounts and $branches branches over $n node(s)"
	drive load "$n"
	start_routers
	modes="keyed routed1"
	[ "$n" = 1 ] && modes="direct keyed routed1"
	if [ "${#router_hosts[@]}" -ge 2 ] && grep -qw "$n" <<<"$two_at"; then modes="$modes routed2"; fi
	for w in $workloads; do
		say "N=$n $w: $modes at $clients clients, $seconds s each"
		drive run "$n" "$w" "'$modes'" "'$clients'"
	done
done

# --- 5. Fetch the results and the raw pgbench output, in pieces under Session Manager's limit ---
results="$(echo 'cat /var/tmp/lepis-so/results.txt' | on_host "$client_host")"
mkdir -p "$here/results/raw"
day="$(date -u +%Y-%m-%d)"
rawfile="$here/results/raw/$day-$label-pgbench.md"
size="$(echo 'gzip -c /var/tmp/lepis-so/raw.md | base64 -w0 > /var/tmp/lepis-so/raw.b64; wc -c < /var/tmp/lepis-so/raw.b64' | on_host "$client_host")"
b64=""
for ((at = 1; at <= size; at += 20000)); do
	b64="$b64$(echo "cut -c $at-$((at + 19999)) /var/tmp/lepis-so/raw.b64" | on_host "$client_host" | tr -d '\n')"
done
printf '%s' "$b64" | base64 -d | gunzip >"$rawfile"
out="$here/results/$day-$label.md"
{
	echo "# Lepis scale-out: pgbench, $label"
	echo
	echo "\`bash bench/scaleout.sh --routers $routers_arg --client $client_host --nodes $nodes_arg $label\`, finished $(date -u +%FT%TZ)."
	echo "Lepis ${image#*/}, session pool mode. postgres:18 on every node. $accounts accounts (acct), $branches TPC-B branches (branch); $seconds s per run, -M prepared,"
	echo "latency from pgbench -l --sampling-rate=0.1 over every endpoint of a run. CPU: busy percent of the whole host (iowait in the second figure),"
	echo "from /proc/stat sampled 3 s into the run and 2 s before its end."
	echo
	echo "| N | workload | target | clients | tps | p50 (ms) | p99 (ms) | failed | CPU busy/iowait % per host |"
	echo "| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |"
	while read -r n w mode c tps p50 p99 failed aborted rest; do
		[ "$aborted" = 0 ] || failed="$failed ($aborted error lines)"
		echo "| $n | $w | $mode | $c | $tps | $p50 | $p99 | $failed | $rest |"
	done <<<"$results"
	echo
	echo "Raw pgbench output: \`results/raw/$day-$label-pgbench.md\`."
} >"$out"
say "wrote $out and $rawfile"
