# snout-lepis

One Postgres database spread over many plain Postgres servers, behind a router that speaks the
Postgres wire protocol. An application connects to one address with one connection string and
sees one database. Underneath, its large tables are split by a shard key across the servers (the
**nodes**); the router sends each statement to the node that holds its rows, and moves, splits
and merges that data online.

The nodes are stock Postgres 17 or 18, anywhere: your own machines, a managed service, a
laptop. Nothing is installed in them. What Lepis needs inside a node is plain SQL it can write
itself: a schema for its catalog on one node, and a CHECK constraint per sharded table on each.

Lepis is Greek for "scale", the root of *Lepidoptera*: a butterfly's wing is thousands of small
scales that read as one pattern, the way the nodes read as one database.

> **We are building Lepis in the open, and it is not ready for production yet.** Everything
> described here runs and is tested against real Postgres 17 and 18 nodes, but it has not yet
> carried anyone's real workload, and the interfaces may still change. Follow along, try the
> compose example, and open an issue with what you find.

**Status.** A statement that names its shard key goes to the node that owns it. Reads across
nodes are merged (ORDER BY, LIMIT, aggregates, GROUP BY, DISTINCT), and writes across nodes commit
on all of them or none through two-phase commit. Session settings and prepared statements follow
the session from node to node. Ranges split, merge and move between nodes while the application
keeps writing, driven by `snoutdata shards` through the router's admin API. A statement Lepis
cannot yet answer correctly is refused with a sentence saying so, never run wrongly.

- [Quick start](#quick-start): three Postgres nodes and a router, with sample data, in one command
- [How it works](#how-it-works): nodes, keyspaces, ranges, the three kinds of table, fences
- [Running it](#running-it): the binary and every variable
- [What a node needs](#what-a-node-needs)
- [Operations](#operations): distributing tables, roles, catalog changes, logs, upgrades
- [Threat model](#threat-model)
- [Working on it](#working-on-it)

## Quick start

`compose/` runs a router in front of three `postgres:18` containers, with a small multi-tenant
shop already distributed over them. Docker (or Podman with `podman compose`) is all it needs.

```sh
cd compose
for v in NODE_PASSWORD LEPIS_SERVICE_PASSWORD APP_PASSWORD LEPIS_ADMIN_TOKEN; do
  echo "$v=$(openssl rand -hex 24)"; done > .env
docker compose up -d --build --wait
```

The first `up` builds the router's image from this source, which takes a few minutes. Then
`setup` runs once: it creates the roles and a self-signed certificate for the router
(`certs/`), loads the same schema and rows on every node, writes the catalog on `home`, and
leaves each node with only the rows it owns. Its log says where they went:

```sh
docker compose logs setup
```

Connect through the router as the application role (the router is published on
`127.0.0.1:6432`, over TLS; the nodes are not published at all):

```sh
psql "postgresql://app:$(sed -n 's/^APP_PASSWORD=//p' .env)@127.0.0.1:6432/app"
```

The sample tables, all in `public`:

| Table | Kind | Shard key |
| --- | --- | --- |
| `tenants`, `orders` | sharded, colocated (one keyspace, `tenant`) | `tenant_id bigint` |
| `events` | sharded (keyspace `device`) | `device uuid` |
| `countries` | reference: a full copy on every node | |
| `plans` | global: on the home node only | |

Try:

```sql
-- One tenant: one node, whichever owns tenant 42.
select * from tenants where tenant_id = 42;

-- A tenant's orders joined with its row and a reference table: still one node, because tenants
-- and orders share a keyspace and countries is on every node.
select o.order_id, o.total_cents, c.name
from orders o join tenants t using (tenant_id) join countries c on c.code = t.country
where o.tenant_id = 7 order by o.order_id limit 5;

-- A write goes to the owner of its key, and a transaction stays on the node it started on.
begin;
insert into orders (tenant_id, order_id, placed_at, total_cents, status)
	values (7, 1001, now(), 1999, 'placed');
update tenants set name = 'tenant 7, renamed' where tenant_id = 7;
commit;

-- The global table lives on the home node.
select * from plans;

-- Every node at once: asked of each node, and the answers merged.
select count(*) from orders;

-- One statement whose rows belong to several nodes: refused, with SQLSTATE 0A000 and a hint
-- naming the fix, until writes across nodes commit on all of them or none.
insert into orders (tenant_id, order_id, placed_at, total_cents, status)
	values (1, 5000, now(), 1, 'placed'), (2, 5000, now(), 1, 'placed');
```

The data a node holds is guarded by the node itself, not only by the router. Write a row straight
to a node that does not own it and the node refuses it:

```sh
docker compose exec node2 psql -U postgres -d app -c \
  "insert into tenants values (1001, 'x', 'CA', 1, now()), (1002, 'y', 'CA', 1, now()), (1003, 'z', 'CA', 1, now())"
# ERROR:  new row for relation "tenants" violates check constraint "lepis_owns"
```

(Of three new tenants, at least one belongs elsewhere: each node owns a third of the hash space.)

The catalog is ordinary tables on `home`:

```sh
docker compose exec home psql -U postgres -d app -c "select * from lepis.range order by keyspace, lo"
```

### Split while a load runs

The router's admin API is published on `127.0.0.1:7432` (HTTPS, with the same certificate), and
`snoutdata shards` drives it. In a second terminal, in `compose/`:

```sh
export LEPIS_ADMIN_URL=https://localhost:7432
export LEPIS_ADMIN_TOKEN="$(sed -n 's/^LEPIS_ADMIN_TOKEN=//p' .env)"
export NODE_EXTRA_CA_CERTS="$PWD/certs/lepis.crt"     # trust the example's own certificate
npx snoutdata shards status
```

`status` lists every range as `keyspace:lo` with the node that owns it. Start a write load: one
new order per transaction, through the router, for 90 seconds (pgbench, from the Postgres image):

```sh
docker compose run --rm load 90
```

While it runs, from the other terminal, read the plan, then split the first `tenant` range and
move its upper half to `node3`:

```sh
npx snoutdata shards range split tenant:-9223372036854775808 --to node3 --plan
npx snoutdata shards range split tenant:-9223372036854775808 --to node3
```

The plan says what moves (rows, size, estimated copy time) and the write pause to expect. The
second command shows it again, asks, and follows the job: the catalog change, then the transfer
(a publication of the moving slice on `home`, a subscription on `node3`, the copy, the cutover at
a transaction boundary, a check that both sides agree, and the old copy deleted from `home`).
Writes to the tenants that move pause for the cutover; nothing else stops.

When the load ends, every order pgbench committed is on exactly one node, the one that owns it:

```sh
for n in home node2 node3; do
  docker compose exec $n psql -U postgres -d app -Atc "select '$n', count(*) from orders where status = 'load'"
done
npx snoutdata shards verify
npx snoutdata shards jobs
```

The three counts add up to pgbench's "number of transactions actually processed", with none
failed; `verify` checks that every node holds only the rows its ranges say. On a laptop the load's
rate swings with the disk (every commit waits for an fsync on three Postgres at once), which is
the machine, not the move. `snoutdata shards --help` lists the other operations (move, merge,
rebalance, adding and draining nodes, distributing a table), and each takes `--plan`.

### Stopping and starting over

```sh
docker compose down          # stops it; the data stays in the three volumes
docker compose down -v       # removes this example's volumes too: the next `up` starts empty
```

`down -v` removes only the volumes of this compose project (`lepis-example`).

## How it works

- **Nodes.** Each node is a whole Postgres with its own WAL and its own durability. One of them
  is the **home node**: it holds Lepis's catalog (the `lepis` schema), the global tables, and
  its share of the sharded rows. The router is told only where the home node is; the catalog
  says where everything else is.
- **Keyspaces and ranges.** A keyspace is a shard key's domain: a key type and a hash seed. Each
  value hashes to a signed 64-bit number with the same function Postgres uses for that type
  (`hashint8extended`, `uuid_hash_extended`, `hashtextextended`, …), and the 64-bit space is cut
  into **ranges**, each owned by exactly one node. A node may own many ranges. A single key
  value can be **pinned** to a node of its own.
- **Tables** are one of three kinds. A **sharded** table is split by its key column in a
  keyspace; tables in the same keyspace are **colocated**, so a join between them on the key
  stays on one node. A **reference** table is copied in full to every node, so a join with it is
  always local. A **global** table lives on the home node only. A table not in the catalog is
  global.
- **Fences.** Every sharded table on every node carries a CHECK constraint, `lepis_owns`, over
  the same hash: `hashint8extended(tenant_id, <seed>) between <lo> and <hi> or …`. It is added
  `NOT VALID`, so adding it is instant and every new row is checked. A router working from an old
  catalog, a person with psql, or a bug cannot put a row on a node that does not own it.
- **The epoch.** Every catalog change bumps `lepis.cluster.epoch` and sends `NOTIFY
  lepis_epoch`. Routers reload on the notification, and every 30 seconds in case one was missed.
- **Routing.** Statements are parsed with Postgres's own parser (libpg_query). A statement that
  pins its key (`=`, `IN`, `IS NULL`, a parameter bound at execution, `INSERT … VALUES` with
  a column list) goes to the node that owns it; one that touches only global
  tables goes home; anything Lepis cannot place correctly is refused, never guessed at. A query
  Lepis's parser rejects is shown to the home node, whose syntax error is the answer.
- **Authentication** is SCRAM-SHA-256 end to end, and Lepis never sees a password. It checks a
  client's SCRAM proof against the role's verifier (read from the home node), and the proof
  reveals the role's ClientKey, which is what logs the same role into each node. So every node
  must hold the **same** verifier for the role, and row-level security keeps working, because
  each node session runs as the client's own role.
- **Consistency.** A statement and a transaction on one node are full Postgres. A read across
  nodes sees each node at its own moment, not one snapshot of all of them.

## Running it

```sh
LEPIS_HOME=db1.internal:5432 \
LEPIS_SERVICE_USER=lepis LEPIS_SERVICE_PASSWORD=... \
snout-lepis
```

Or the image, built from this repository's `Containerfile`:

```sh
docker build -f Containerfile -t snout-lepis .
docker run -p 6432:5432 -e LEPIS_HOME=db1.internal:5432 -e LEPIS_SERVICE_USER=lepis \
  -e LEPIS_SERVICE_PASSWORD=... snout-lepis
```

All configuration is environment variables. Everything about the cluster's shape (nodes,
keyspaces, ranges, tables) is in the catalog on the home node, never here, so any number of
routers can run with the same five lines.

| Variable | Default | Secret | |
| --- | --- | --- | --- |
| `LEPIS_HOME` | required | | `host:port` of the home node (`[v6addr]:port` for IPv6; port 5432 when left out) |
| `LEPIS_HOME_SSLMODE` | `verify-full` | | `disable`, `require` or `verify-full`, for the connections to the home node |
| `LEPIS_HOME_CA` | the public web roots | | a PEM bundle to trust for `verify-full` |
| `LEPIS_SERVICE_USER` | required | | Lepis's own login: on the home node it reads and writes the catalog and reads role verifiers, and on every node it runs the operations. A superuser with the same password on every node (What a node needs). Never a client's |
| `LEPIS_SERVICE_PASSWORD` | required | yes | its password |
| `LEPIS_SERVICE_DATABASE` | `postgres` | | the database holding the catalog |
| `LEPIS_VERIFIER_QUERY` | reads `pg_authid` | | one row, one column, the role's `rolpassword` for `$1` |
| `LEPIS_HOST`, `LEPIS_PORT` | `0.0.0.0`, `5432` | | where clients connect |
| `LEPIS_TLS_CERT`, `LEPIS_TLS_KEY` | none | the key | PEM files. With them, clients may use TLS, by SSLRequest or directly (`sslnegotiation=direct`), and SCRAM-SHA-256-PLUS with channel binding is offered. Both or neither |
| `LEPIS_ADMIN_ADDR` | none: no admin API | | `host:port` for the admin API (HTTP and JSON, what `snoutdata shards` calls). Off loopback it needs `LEPIS_TLS_CERT`, and is then HTTPS with that certificate; plain HTTP is refused anywhere but loopback |
| `LEPIS_ADMIN_TOKEN` | required with `LEPIS_ADMIN_ADDR` | yes | the bearer token every admin call but `/v1/health` must send; at least 16 characters |
| `LEPIS_MAX_CLIENTS` | `1000` | | connections over this are closed at once, not queued |
| `LEPIS_LOG` | `info` | | a `tracing` filter: `warn`, `debug`, `lepis=debug,info`, … |

A missing required variable stops the router with a sentence naming it; there is no default
password or key anywhere. So does a catalog that does not validate at start (ranges that do not
cover the hash space exactly once, a range on an unknown or removed node): Lepis refuses to
route rather than misroute.

Clients connect with any Postgres driver, as any role that can log in on the nodes. Startup
parameters (`application_name`, `options`, GUCs) reach every node the session uses.
Replication connections are refused: connect to a node directly for those.

## What a node needs

- **Postgres 17 or 18.** A node below 17 is refused wherever it would enter the cluster (`nodes
  add`, `nodes attach`, and a catalog that names one is refused when it loads), with a sentence
  naming the node and its version. Protocol 3.0 is spoken, and 3.2 from Postgres 18 too, on
  either side independently.
- **SCRAM-SHA-256 for every role that logs in through Lepis**: `password_encryption =
  scram-sha-256` when the password was set, and a `scram-sha-256` line in `pg_hba.conf` for
  Lepis's address. A node that asks for md5 or a cleartext password is refused with that sentence.
- **The same verifier for each role on every node** (see Roles, below).
- For the operations that move data and write across nodes: **`wal_level = logical`** (RDS calls
  it `rds.logical_replication = 1`), **`max_prepared_transactions` above 0**, and room for one
  more replication slot and WAL sender per move in flight. `nodes add` checks all of this and
  names what is missing.
- **Lepis's service login (`LEPIS_SERVICE_USER`) as a SUPERUSER on every node, with the same
  password on each**, and a `pg_hba.conf` line that lets it in from Lepis's address and from the
  other nodes (a move's subscription logs into the source node with it). Superuser, because some
  of what the operations do Postgres allows no one else:
  - **finishing another role's prepared transaction**: in-doubt recovery runs `COMMIT PREPARED`
    or `ROLLBACK PREPARED` for transactions the client's role prepared, which only that role or
    a superuser may;
  - **reading `pg_authid`**: role verifiers (the client logins) and role sync, which copies each
    role's verifier verbatim to every node;
  - **creating and altering roles** that may be superusers, replication roles or `BYPASSRLS`
    themselves, which takes a superuser;
  - **moving rows**: a publication of a table needs its owner, and so do the fence (`ALTER TABLE
    … ADD CONSTRAINT`) and the cleanup that deletes what moved away, which must also see rows
    row-level security would hide; a subscription needs a superuser, or
    `pg_create_subscription` plus a subscription owner that can write every table it fills.

  The last two could be pieced together without superuser (membership in each table's owner,
  `BYPASSRLS`, `REPLICATION`, `pg_create_subscription`); the first two cannot, so a
  service login that is not a superuser cannot run two-phase commit recovery or role sync. On a managed service whose admin role is not a true superuser
  (RDS's `rds_superuser`, Cloud SQL's `cloudsqlsuperuser`), those two are the parts to check
  first. The catalog's own writes (jobs, the commit log, router acknowledgements) are in schema
  `lepis` on the home node, which the service login creates and owns.
- Text shard keys need UTF-8 databases and deterministic collations. A `bytea` shard key needs
  Postgres 18 on every node (17 has no SQL-callable bytea hash).

## Operations

### Distributing tables

Every operation on a running cluster is a call to the router's admin API, made with
`snoutdata shards` (`--admin <url>` or `LEPIS_ADMIN_URL`, and `LEPIS_ADMIN_TOKEN`): `keyspace
create`, `table distribute <schema.table> --column C --keyspace K`, `table reference`, `range
split|merge|move`, `tenant pin`, `nodes add|drain|remove`, `rebalance`, `scale`, `verify` and
`cleanup`. Each has a dry run (`--plan`: what moves, how much, the copy time and the write pause
to expect) and runs as a job in the catalog that survives a router restart (`shards jobs`, `jobs
watch`, `jobs resume`). Anything that moves or deletes data asks first, or needs `--yes`.

The example's first catalog is written by hand, which is also a readable account of what the
catalog holds: `compose/setup/distribute.sh` creates the tables on every node, inserts the
nodes, keyspaces, ranges and tables into `lepis.*` on the home node, deletes from each node the
rows it does not own with the same expression its fence then checks, adds the fence, and runs
`select lepis.bump()`. The catalog's DDL is `src/catalog.sql`, with `catalog_2pc.sql` and
`catalog_jobs.sql`; all three are idempotent.

A primary key or unique constraint on a sharded table must include the shard key (each node can
only check its own rows). A foreign key may point at a colocated table (on the key) or a
reference table, never at a table that lives on another node. A sharded table's key needs
`uuidv7()` or a sequence striped by node, never a plain `serial`, since sequences are per node.

### Roles

A role logs in through Lepis only if every node holds the same SCRAM verifier for it. Set the
password once on the home node, then copy the verifier, verbatim, to the others:

```sql
-- on the home node
create role alice login password '...';
select rolpassword from pg_authid where rolname = 'alice';   -- SCRAM-SHA-256$4096:...
-- on every other node, with that exact string
create role alice login password 'SCRAM-SHA-256$4096:...';
```

A node holding a different verifier refuses the login with a sentence saying so. Verifiers are cached
for 30 seconds, so a changed password takes effect within that.

### Catalog changes

Change the catalog in one transaction that ends with `select lepis.bump();`. Every router
reloads within a moment (the notification) or 30 seconds (the poll), and a statement already
running finishes under the catalog it started with. The fence on each node is what keeps rows in
the right place while routers catch up, so change a node's fence in the same step as its ranges.

### Health, logs, metrics

- **Health:** the router accepts connections once it has loaded the catalog; `pg_isready -h
  <router>` answers from then on. With the admin API on, `GET /v1/health` answers without a
  token, and `snoutdata shards status` shows each router's last acknowledged catalog epoch.
- **Logs** go to standard output, filtered by `LEPIS_LOG`. At `info` they hold the start, catalog
  reloads and failed logins (by role), never a statement.
- **Metrics:** none of its own yet. Each node's `pg_stat_activity` shows Lepis's sessions under
  the client's role and `application_name`.

### Stopping and upgrading

SIGTERM or Ctrl-C stops the router at once; clients see their connection close and reconnect to
another router or the restarted one. Routers keep no state but their connections, so an upgrade
is starting the new version and stopping the old one, one router at a time behind whatever
spreads clients across them.

### Cancel

A client's cancel key is minted by Lepis, never a node's, and a cancel is forwarded to the node
running that client's statement. 3.2's longer keys work in front of nodes that only know 3.0.

## Threat model

**What it trusts.** The home node, completely: its catalog decides where every row is read and
written, and its verifiers decide who logs in. The operator who sets the environment. The
network path to each node as far as its `sslmode` protects it. Not the clients: every byte a
client sends before it has logged in is read by code that is fuzzed, and nothing a client sends
after that changes where Lepis connects or as whom.

**What it holds.**

- **Lepis's service password**, in memory, from the environment. The service login is a
  superuser on every node (What a node needs), so this password is every node, whole. It is also
  written, for the length of a move, into the target node's subscription (`pg_subscription`,
  readable by that node's superusers), since that is how the target logs into the source; the
  subscription is dropped when the move ends. Keep it out of anything but the router's
  environment, and change it on every node at once if it may have leaked.
- **The admin token**, in memory, from the environment, when the admin API is on. It runs every
  operation: moving, deleting and verifying data, adding and removing nodes. The API refuses to
  carry it in the clear off loopback, and compares it in constant time.
- **Each connected client's ClientKey**, in memory, for as long as the session lasts: it is
  what logs the client's role into each node the session reaches. A ClientKey logs in as that
  role, on any node with that verifier, until the role's password changes. It is never written
  to disk or to a log, and `Debug` output of any key or verifier prints none of it.
- **The cancel map**: each client's Lepis-minted cancel key and the node session it cancels.
  Keys are random (4 bytes for a 3.0 client, 32 for 3.2), so a client can cancel only its own
  statements.
- No password of any client, ever. Lepis verifies a proof; it never sees a password and has none
  to keep.

**What a compromise gets.** The router process, the service password, or a node's superuser
during a move: the whole cluster, since each of them reaches the service login, a superuser on
every node; and from the router, also the ClientKey of every role connected through it while it
was compromised, so change those roles' passwords too. The admin token: every operation, which
can move or delete data but runs only what the catalog's job engine runs; it reads no rows and
logs into no node by itself. The home node: the whole cluster, because it says where rows go and
who may log in. A data node outside a move: the rows it holds, and nothing on any other node
(routers never send it another node's rows or a client's credentials, and its fence keeps
another node's rows off it).

**What stops what.**

- *A wrong password, or a role that does not exist:* the exchange runs to the end against a mock
  verifier and fails the same way, so the reply does not tell which.
- *A stale or buggy router, or a person with psql on a node:* the fence (`lepis_owns`) refuses a
  row the node does not own, whoever writes it.
- *A man in the middle between client and Lepis:* TLS when `LEPIS_TLS_CERT` is set, and
  SCRAM-SHA-256-PLUS (`channel_binding=require` on the client) binds the proof to Lepis's
  certificate; a client that says it can bind (`y`) after Lepis offered PLUS is refused as a
  downgrade. Between Lepis and a node, `verify-full` (the default).
- *Oversized input before login:* a startup packet over 10,000 bytes, or any setup message over
  64 KiB, ends the connection. After login, a message may be as large as Postgres allows (1 GiB),
  and the router's buffer grows with the bytes actually received, never ahead of them by more
  than 1 MiB.
- *Too many connections:* past `LEPIS_MAX_CLIENTS`, a connection is closed at once.
- *SQL Lepis cannot read:* refused, or shown to the home node, never routed by a guess.

**Known gaps**, each tracked to be closed before the first release:

- **No authentication timeout.** A connection that never finishes logging in holds one of
  `LEPIS_MAX_CLIENTS` until the client goes away; Postgres closes one after
  `authentication_timeout` (60 s).
- **No setting to require TLS.** With a certificate configured, a client may still connect in
  the clear; run the router where only TLS clients can reach it, or put `sslmode=require` in every
  client's connection string, until it can refuse them itself.
- **A node chooses the iteration count of Lepis's own service login** (`i=` in SCRAM), so the
  home node could make that login slow; the home node is trusted, so this is noted, not fixed.

Fuzzing: `scripts/fuzz.sh` runs four targets over the code that reads untrusted bytes:
`lepis_wire` (the startup packet, the setup messages and the framing of every message after),
`lepis_scram` (a stored verifier, both sides of a SCRAM exchange, with the nonce spliced in so
the proof and signature checks are reached), `lepis_hash` (a shard key value in text and binary
form, for every key type) and `lepis_analyze` (any SQL against a fixed three-node catalog, on a
2 MiB stack like the router's workers, analysed and then routed). Report a vulnerability as
`SECURITY.md` says, never as a public issue.

## Working on it

Everything runs in the stack's dev container; Docker is the only requirement.

```sh
bash scripts/dev.sh cargo test -p snout-lepis       # unit tests and the hash fixture
bash scripts/it.sh [18|17]                    # against real Postgres nodes, then psql checks
bash scripts/hash-check.sh [n] [major]        # the shard hash against Postgres, n values per type
bash bench/pgbench.sh [seconds] [clients]     # direct vs Lepis vs a session pooler
bash scripts/fuzz.sh 300 lepis_wire lepis_scram lepis_hash lepis_analyze
```
