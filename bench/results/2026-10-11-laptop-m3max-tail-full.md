# Lepis Phase 0: pgbench, laptop-m3max-tail-full

`bash lepis/bench/pgbench.sh 30 64 laptop-m3max-tail-full`, 2026-10-11T03:17:12Z.
Host: Darwin 27.0.0 arm64, ? CPUs. postgres:18, scale 20, 64 clients, 30 s each.
Lepis: the router (a one-node catalog), pool_mode session and transaction. PgBouncer docker.io/edoburu/pgbouncer:v1.24.1-p1 in session mode.

| workload | target | tps | latency avg (ms) | vs direct | p50 (ms) | p99 (ms) | p50 vs direct |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| select | direct | 215725 | 0.297 | - | 0.170 | 0.486 | - |
| select | lepis-transaction | 158578 | 0.404 | +107 µs | 0.358 | 0.979 | +188 µs |
| select | pgbouncer | 79208 | 0.808 | +511 µs | 0.772 | 1.122 | +602 µs |

## Raw

## select via direct

```
pgbench (18.6 (Debian 18.6-1.pgdg13+2))
transaction type: <builtin: select only>
scaling factor: 20
query mode: prepared
number of clients: 64
number of threads: 4
maximum number of tries: 1
duration: 30 s
number of transactions actually processed: 6450500
number of failed transactions: 0 (0.000%)
latency average = 0.297 ms
initial connection time = 101.035 ms
tps = 215725.295499 (without initial connection time)
```

## select via lepis-transaction

```
pgbench (18.6 (Debian 18.6-1.pgdg13+2))
transaction type: <builtin: select only>
scaling factor: 20
query mode: prepared
number of clients: 64
number of threads: 4
maximum number of tries: 1
duration: 30 s
number of transactions actually processed: 4732360
number of failed transactions: 0 (0.000%)
latency average = 0.404 ms
initial connection time = 158.522 ms
tps = 158578.547309 (without initial connection time)
```

## select via pgbouncer

```
pgbench (18.6 (Debian 18.6-1.pgdg13+2))
transaction type: <builtin: select only>
scaling factor: 20
query mode: prepared
number of clients: 64
number of threads: 4
maximum number of tries: 1
duration: 30 s
number of transactions actually processed: 2362116
number of failed transactions: 0 (0.000%)
latency average = 0.808 ms
initial connection time = 180.778 ms
tps = 79208.992522 (without initial connection time)
```

