#!/usr/bin/env bash
# tests/cron_sync.rs against three Postgres 18 servers with pg_cron: stock images, the extension
# installed from the image's own PGDG repository at start.
#
#   bash lepis/tests/cron_sync.sh
#
# Everything it starts is named for this run and removed after, by name.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
stack="$(cd "$here/../.." && pwd)"
engine="${STACK_ENGINE:-docker}"
run="lepis-cron-$$"
net="$run-net"
nodes=("$run-1" "$run-2" "$run-3")

cleanup() {
	"$engine" rm -fv "${nodes[@]}" >/dev/null 2>&1 || true
	"$engine" network rm "$net" >/dev/null 2>&1 || true
}
trap cleanup EXIT

"$engine" network create "$net" >/dev/null
for n in "${nodes[@]}"; do
	"$engine" run -d --name "$n" --network "$net" -e POSTGRES_PASSWORD=test -e POSTGRES_DB=app \
		--entrypoint bash docker.io/library/postgres:18 -c \
		'apt-get update -qq >/dev/null && apt-get install -y -qq postgresql-18-cron >/dev/null &&
		 exec docker-entrypoint.sh postgres -c shared_preload_libraries=pg_cron \
		   -c cron.database_name=app -c cron.use_background_workers=on -c max_worker_processes=16' >/dev/null
done
for n in "${nodes[@]}"; do
	for _ in 1 2; do
		until "$engine" exec "$n" psql -U postgres -d app -Atc 'select 1' >/dev/null 2>&1; do
			if ! "$engine" ps -q -f "name=$n" | grep -q .; then
				"$engine" logs "$n" >&2
				exit 1
			fi
			sleep 1
		done
		sleep 2
	done
done

STACK_DEV_ARGS="--network $net -e LEPIS_CRON_NODES=${nodes[0]}:5432,${nodes[1]}:5432,${nodes[2]}:5432" \
	bash "$stack/scripts/dev.sh" cargo test -p snout-lepis --test cron_sync -- --nocapture
