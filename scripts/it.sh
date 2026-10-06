#!/usr/bin/env bash
# The integration tests against a real Postgres, plus the checks only a real libpq can make
# (protocol 3.2, direct TLS), from psql in the Postgres container.
#
#   bash scripts/it.sh          postgres:18
#   bash scripts/it.sh 17       postgres:17
#
# Lepis supports Postgres 17 and 18 as nodes (L1); those are the two this runs.
#
# Everything it starts is named for this run (lepis-it-<pid>-*) and removed after, by name, with
# its anonymous data volumes (`rm -v`): without it every run left five Postgres volumes behind.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
stack="$(cd "$here/.." && pwd)"
engine="${STACK_ENGINE:-docker}"
major="${1:-18}"
if [ "$major" -lt 17 ]; then
	echo "Lepis needs Postgres 17 or later on every node (L1); asked for $major" >&2
	exit 2
fi
run="lepis-it-$$"
net="$run-net"
pg="$run-pg"
router="$run-router"
ref="$run-ref"
# The Phase 1 cluster: its own three nodes, so its catalog never meets the Phase 0 tests.
cluster=("$run-c1" "$run-c2" "$run-c3")

cleanup() {
	"$engine" rm -fv "$pg" "$ref" "$router" "${cluster[@]}" >/dev/null 2>&1 || true
	"$engine" network rm "$net" >/dev/null 2>&1 || true
}
trap cleanup EXIT

"$engine" network create "$net" >/dev/null
# Lepis needs SCRAM on every node (L11), said explicitly rather than left to the image default.
"$engine" run -d --name "$pg" --network "$net" -e POSTGRES_PASSWORD=x \
	-e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 -e POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 \
	"docker.io/library/postgres:$major" -c password_encryption=scram-sha-256 >/dev/null
# The oracle's reference: a separate plain Postgres holding the same data (tests/oracle.rs).
"$engine" run -d --name "$ref" --network "$net" -e POSTGRES_PASSWORD=x \
	-e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 -e POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 \
	"docker.io/library/postgres:$major" -c password_encryption=scram-sha-256 >/dev/null
for c in "${cluster[@]}"; do
	"$engine" run -d --name "$c" --network "$net" -e POSTGRES_PASSWORD=x \
		-e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 -e POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 \
		"docker.io/library/postgres:$major" -c password_encryption=scram-sha-256 \
		-c wal_level=logical -c max_prepared_transactions=64 >/dev/null
done
for c in "$pg" "$ref" "${cluster[@]}"; do
	for _ in 1 2; do
		until "$engine" exec "$c" psql -U postgres -Atc 'select 1' >/dev/null 2>&1; do sleep 1; done
		sleep 2
	done
done

echo "== cargo tests against postgres:$major" >&2
if [ -z "${LEPIS_IT_ONLY:-}" ]; then
STACK_DEV_ARGS="--network $net -e LEPIS_IT_HOME=$pg:5432 -e LEPIS_ORACLE_REFERENCE=$ref:5432" \
	bash "$stack/scripts/dev.sh" cargo test -p snout-lepis --test passthrough --test oracle -- --test-threads 4
fi

# Every tests/cluster*.rs runs here, one binary after another (each redistributes the data it
# needs). LEPIS_IT_ONLY=cluster_scatter runs just that file; LEPIS_IT_SKIP_PSQL=1 stops after.
echo "== the three-node cluster" >&2
cluster_tests=()
for f in "$here"/../tests/cluster*.rs; do
	t="$(basename "$f" .rs)"
	if [ -z "${LEPIS_IT_ONLY:-}" ] || [ "$t" = "$LEPIS_IT_ONLY" ]; then
		cluster_tests+=(--test "$t")
	fi
done
STACK_DEV_ARGS="--network $net -e LEPIS_IT_HOME=${cluster[0]}:5432 -e LEPIS_IT_DATA_NODES=${cluster[1]}:5432,${cluster[2]}:5432 -e LEPIS_ORACLE_REFERENCE=$ref:5432 -e LEPIS_GEN_N -e LEPIS_GEN_SEED -e LEPIS_GEN_ONLY" 	bash "$stack/scripts/dev.sh" cargo test -p snout-lepis "${cluster_tests[@]}" -- --test-threads 1 --nocapture
if [ -n "${LEPIS_IT_SKIP_PSQL:-}" ]; then
	exit 0
fi

# The router as a process, for psql. A self-signed certificate makes TLS testable.
echo "== psql through the router" >&2
STACK_DEV_ARGS="-d --name $router --network $net -e LEPIS_HOME=$pg:5432 -e LEPIS_HOME_SSLMODE=disable \
-e LEPIS_SERVICE_USER=postgres -e LEPIS_SERVICE_PASSWORD=x -e LEPIS_TLS_CERT=/tmp/lepis.crt \
-e LEPIS_TLS_KEY=/tmp/lepis.key" bash "$stack/scripts/dev.sh" bash -c '
	openssl req -x509 -newkey rsa:2048 -nodes -keyout /tmp/lepis.key -out /tmp/lepis.crt \
		-days 1 -subj /CN=lepis >/dev/null 2>&1
	cargo build -q --release -p snout-lepis && exec /cache/target/release/snout-lepis' >/dev/null
until "$engine" logs "$router" 2>&1 | grep -q "snout-lepis listening"; do
	if ! "$engine" ps -q -f "name=$router" | grep -q .; then
		"$engine" logs "$router" >&2 || true
		echo "the router did not start" >&2
		exit 1
	fi
	sleep 1
done

conn="host=$router port=5432 user=lepis_app password=app-pw dbname=postgres"
psql_q() { "$engine" exec "$pg" psql "$1" -XAtc "$2"; }

fail=0
check() {
	local name="$1" want="$2" got="$3"
	if [ "$got" = "$want" ]; then
		echo "ok    $name" >&2
	else
		echo "FAIL  $name: want [$want] got [$got]" >&2
		fail=1
	fi
}

check "plain" "lepis_app" "$(psql_q "$conn sslmode=disable" 'select current_user')"
check "tls via SSLRequest" "t" "$(psql_q "$conn sslmode=require" 'select true')"
check "direct tls" "t" "$(psql_q "$conn sslmode=require sslnegotiation=direct" 'select true')"
# SCRAM-SHA-256-PLUS. The router's RSA/SHA-256 certificate makes the binding a SHA-256 of it;
# over TLS libpq's default (prefer) already takes PLUS, so "require" proves it was offered and
# the server-end-point data matched.
check "channel binding over tls" "t" \
	"$(psql_q "$conn sslmode=require channel_binding=require" 'select true')"
check "plain scram over tls, plus declined" "t" \
	"$(psql_q "$conn sslmode=require channel_binding=disable" 'select true')"
# Without TLS there is no channel to bind to, PLUS is not offered, and libpq refuses.
check "channel binding without tls refused" "refused" \
	"$(psql_q "$conn sslmode=disable channel_binding=require" 'select true' >/dev/null 2>&1 \
		&& echo connected || echo refused)"
check "channel binding over direct tls" "t" \
	"$(psql_q "$conn sslmode=require sslnegotiation=direct channel_binding=require" 'select true')"
if [ "$major" -ge 18 ]; then
	# libpq 18 asks for 3.2 when told to; the client's cancel key is then 32 bytes.
	got="$("$engine" exec "$pg" psql "$conn sslmode=require max_protocol_version=3.2" -X -c '\conninfo' \
		| grep -i 'protocol version' | grep -o '3\.[0-9]' || true)"
	check "protocol 3.2" "3.2" "$got"
fi

exit "$fail"
