#!/usr/bin/env bash
# L8's physical move (Phase 6) against real Postgres: a primary, and a streaming
# standby of it made with pg_basebackup, then tests/physical.rs moves half a keyspace onto the
# standby by promoting it, under a write load through Lepis.
#
#   bash scripts/it-physical.sh          postgres:18
#   bash scripts/it-physical.sh 17       postgres:17
#
# In SnoutData Cloud the standby is a pod restored from the source's pgBackRest stanza in S3
# (packages/snoutpod, `restoreStandby`); how it was made is not what this tests. Everything it
# starts is named for this run (lepis-phys-<pid>-*) and removed after, by name.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
stack="$(cd "$here/.." && pwd)"
engine="${STACK_ENGINE:-docker}"
major="${1:-18}"
run="lepis-phys-$$"
net="$run-net"
primary="$run-p"
standby="$run-s"

cleanup() {
	if [ -n "${LEPIS_PHYS_LOGS:-}" ]; then
		"$engine" logs "$standby" 2>&1 | tail -n "$LEPIS_PHYS_LOGS" >&2 || true
	fi
	"$engine" rm -fv "$primary" "$standby" >/dev/null 2>&1 || true
	"$engine" network rm "$net" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# The settings a standby must match or exceed are passed to both.
settings=(-c password_encryption=scram-sha-256 -c wal_level=logical -c max_prepared_transactions=64
	-c max_wal_senders=10 -c max_replication_slots=10 -c hot_standby=on)

"$engine" network create "$net" >/dev/null
"$engine" run -d --name "$primary" --network "$net" -e POSTGRES_PASSWORD=x \
	-e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 -e POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 \
	"docker.io/library/postgres:$major" "${settings[@]}" >/dev/null
for _ in 1 2; do
	until "$engine" exec "$primary" psql -U postgres -Atc 'select 1' >/dev/null 2>&1; do
		if ! "$engine" ps -q -f "name=$primary" | grep -q .; then
			"$engine" logs "$primary" 2>&1 | tail -5 >&2
			echo "the primary did not start" >&2
			exit 1
		fi
		sleep 1
	done
	sleep 2
done
# The image's pg_hba lets every host log in to every DATABASE; replication is not a database,
# so the standby needs its own line (the pod image's snoutpod-hba has the same one for its admin).
"$engine" exec "$primary" bash -c 'echo "host replication postgres all scram-sha-256" >> "$PGDATA/pg_hba.conf"'
"$engine" exec "$primary" psql -U postgres -Atc 'select pg_reload_conf()' >/dev/null

# The standby: a base backup of the primary, streaming from it (-R writes primary_conninfo and
# standby.signal), started with the same settings.
data=/var/lib/postgresql/standby
"$engine" run -d --name "$standby" --network "$net" -e PGPASSWORD=x --entrypoint bash \
	"docker.io/library/postgres:$major" -c "
	set -e
	mkdir -p $data && chown postgres:postgres $data && chmod 700 $data
	gosu postgres pg_basebackup -h $primary -U postgres -D $data -R -X stream -c fast
	exec gosu postgres postgres -D $data ${settings[*]}" >/dev/null
until [ "$("$engine" exec "$standby" psql -U postgres -h 127.0.0.1 -Atc 'select pg_is_in_recovery()' 2>/dev/null || true)" = "t" ]; do
	if ! "$engine" ps -q -f "name=$standby" | grep -q .; then
		"$engine" logs "$standby" >&2 || true
		echo "the standby did not start" >&2
		exit 1
	fi
	sleep 1
done
echo "== $standby is streaming from $primary (postgres:$major)" >&2

STACK_DEV_ARGS="--network $net -e LEPIS_PHYS_PRIMARY=$primary:5432 -e LEPIS_PHYS_STANDBY=$standby:5432" \
	bash "$stack/scripts/dev.sh" cargo test -p snout-lepis --test physical -- --nocapture
