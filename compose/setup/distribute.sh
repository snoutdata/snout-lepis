#!/usr/bin/env bash
# Prepares the example cluster: on every run, Lepis's service login on every node and the
# router's certificate; once, the sample schema distributed over the three nodes:
#
#   1. The application role `app`, with ONE SCRAM verifier copied verbatim to every node (L11:
#      Lepis logs into each node with the key a client's proof reveals, so the verifiers must be
#      the same).
#   2. The schema and the same rows on every node.
#   3. The catalog (schema `lepis`) on the home node: the nodes, two hash keyspaces split into
#      equal ranges dealt round-robin over the nodes, and which table is which kind.
#   4. On each node, the rows it does not own are deleted with the same expression its fence
#      then checks: a NOT VALID CHECK constraint `lepis_owns` (L7), so no write can put a row on a
#      node that does not own it, whoever sends it.
#
# Runs on every `docker compose up`; once the cluster is distributed, only the first part runs.
set -euo pipefail

nodes=(home node2 node3)
db=app
tenant_seed=132424935 # 0x07e4a4e7
device_seed=14578126  # 0x00de71ce
ranges_per_node="${RANGES_PER_NODE:-4}"

on() { local host="$1"; shift; psql -X -q -v ON_ERROR_STOP=1 -h "$host" -U postgres -d "$db" "$@"; }
value() { on "$1" -At -c "$2"; }

echo "== Lepis's service login on every node"
# A superuser, with the same password on every node: the operations (moving ranges, two-phase
# commit and its recovery, role sync) need it everywhere, and some of what they do only a
# superuser may (../README.md, "What a node needs").
for n in "${nodes[@]}"; do
	on "$n" -v pw="$LEPIS_SERVICE_PASSWORD" <<'SQL'
select 'create role lepis' where not exists (select from pg_roles where rolname = 'lepis') \gexec
select format('alter role lepis with login superuser password %L', :'pw') \gexec
SQL
done

echo "== the router's certificate"
# Self-signed, for this machine: the router serves Postgres clients and its admin API over TLS
# with it, and `snoutdata shards` trusts it through NODE_EXTRA_CA_CERTS. Made once.
if [ ! -s /certs/lepis.crt ] || [ ! -s /certs/lepis.key ]; then
	openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 825 		-subj /CN=lepis -addext subjectAltName=DNS:localhost,IP:127.0.0.1,DNS:lepis 		-keyout /certs/lepis.key -out /certs/lepis.crt 2>/dev/null
	# The router runs as an unprivileged user and must read its key.
	chmod 644 /certs/lepis.key /certs/lepis.crt
	echo "made certs/lepis.crt"
fi

sharded="'public.tenants'::regclass, 'public.orders'::regclass, 'public.events'::regclass"
fenced() { value "$1" "select count(*) from pg_constraint where conname = 'lepis_owns' and conrelid in (select to_regclass(t) from unnest(array['public.tenants', 'public.orders', 'public.events']) t)"; }
done=1
for n in "${nodes[@]}"; do
	[ "$(fenced "$n")" = 3 ] || done=0
done
if [ "$done" = 1 ]; then
	echo "already distributed (catalog epoch $(value home "select epoch from lepis.cluster")); nothing to do"
	exit 0
fi
# A run that stopped part way: undo its catalog and fences, then start again. The rows come back
# from data.sql, which only inserts what is missing.
for n in "${nodes[@]}"; do
	if [ "$(fenced "$n")" != 0 ]; then
		printf '%s\n' "select format('alter table %s drop constraint lepis_owns', conrelid::regclass)
			from pg_constraint where conname = 'lepis_owns' and conrelid in ($sharded) \gexec" | on "$n"
	fi
done
on home -c "drop schema if exists lepis cascade"

echo "== the application role"
on home -v app_pw="$APP_PASSWORD" <<'SQL'
select format('create role app login password %L', :'app_pw')
	where not exists (select from pg_roles where rolname = 'app') \gexec
select format('alter role app password %L', :'app_pw') \gexec
SQL
verifier="$(value home "select rolpassword from pg_authid where rolname = 'app'")"
case "$verifier" in SCRAM-SHA-256\$*) ;; *) echo "app's password is not a SCRAM verifier" >&2; exit 1 ;; esac
for n in node2 node3; do
	on "$n" -v v="$verifier" <<'SQL'
select 'create role app login' where not exists (select from pg_roles where rolname = 'app') \gexec
select format('alter role app password %L', :'v') \gexec
SQL
done

echo "== schema and rows on every node"
for n in "${nodes[@]}"; do
	on "$n" -f /setup/schema.sql -f /setup/data.sql
done
on home -f /setup/home.sql

echo "== the catalog on home"
for f in catalog.sql catalog_2pc.sql catalog_jobs.sql; do
	if [ -f "/lepis-src/$f" ]; then on home -f "/lepis-src/$f"; fi
done
{
	echo "begin;"
	i=1
	for n in "${nodes[@]}"; do
		kind=data
		[ "$n" = home ] && kind=home
		version="$(value "$n" "select current_setting('server_version_num')")"
		echo "insert into lepis.node (id, name, host, port, dbname, sslmode, kind, state, server_version_num)
			values ($i, '$n', '$n', 5432, '$db', 'disable', '$kind', 'active', $version);"
		i=$((i + 1))
	done
	echo "insert into lepis.keyspace values ('tenant', 'hash', 'bigint', $tenant_seed), ('device', 'hash', 'uuid', $device_seed);"
	# Equal ranges over the signed 64-bit hash space, dealt round-robin to nodes 1..3: the same
	# split Keyspace::even_ranges makes.
	for ks in tenant device; do
		echo "insert into lepis.range (keyspace, lo, hi, node_id)
			select '$ks',
				(-9223372036854775808::numeric + w * i)::bigint,
				case when i = n - 1 then 9223372036854775807
					else (-9223372036854775808::numeric + w * i + w - 1)::bigint end,
				1 + i % ${#nodes[@]}
			from (select ${#nodes[@]} * $ranges_per_node as n) s,
				lateral (select floor(18446744073709551615::numeric / s.n) as w) w,
				generate_series(0, s.n - 1) i;"
	done
	cat <<'SQL'
insert into lepis.relation values
	('public', 'tenants', 'sharded', 'tenant', 'tenant_id'),
	('public', 'orders', 'sharded', 'tenant', 'tenant_id'),
	('public', 'events', 'sharded', 'device', 'device'),
	('public', 'countries', 'reference', null, null),
	('public', 'plans', 'global', null, null);
select lepis.bump() \g /dev/null
commit;
SQL
} | on home

echo "== each node keeps its own rows, and is fenced"
# The fence for one table on one node, from the catalog: the node's ranges over the key's hash,
# as catalog::fence_sql writes it for a Postgres 18 node.
fences() {
	value home "
		select r.table_name || '|' || string_agg(
			format('%s(%I, %s) between %s and %s',
				case k.key_type
					when 'smallint' then 'hashint2extended'
					when 'integer' then 'hashint4extended'
					when 'bigint' then 'hashint8extended'
					when 'text' then 'hashtextextended'
					when 'uuid' then 'uuid_hash_extended'
				end,
				r.key_column, k.seed, g.lo, g.hi),
			' or ' order by g.lo)
		from lepis.relation r
		join lepis.keyspace k on k.name = r.keyspace
		join lepis.range g on g.keyspace = k.name
		join lepis.node n on n.id = g.node_id
		where r.kind = 'sharded' and n.name = '$1'
		group by r.table_name
		-- children first, so a delete never trips a foreign key
		order by case r.table_name when 'orders' then 0 else 1 end"
}
for n in "${nodes[@]}"; do
	while IFS='|' read -r table check; do
		on "$n" -c "delete from public.$table where not ($check)" \
			-c "alter table public.$table drop constraint if exists lepis_owns" \
			-c "alter table public.$table add constraint lepis_owns check ($check) not valid"
	done < <(fences "$n")
done

echo "== where the rows are"
for n in "${nodes[@]}"; do
	echo "$n: $(value "$n" "select format('%s tenants, %s orders, %s events', (select count(*) from tenants), (select count(*) from orders), (select count(*) from events))")"
done
