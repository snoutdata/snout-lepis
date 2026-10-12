# Lepis Phase 0: pgbench, laptop-m3max-tail-rate60k

`bash lepis/bench/pgbench.sh 30 64 laptop-m3max-tail-rate60k`, 2026-10-11T03:19:17Z.
Host: Darwin 27.0.0 arm64, ? CPUs. postgres:18, scale 20, 64 clients, 30 s each, at a fixed 60000 transactions/s in all (pgbench -R).
Lepis: the router (a one-node catalog), pool_mode session and transaction. PgBouncer docker.io/edoburu/pgbouncer:v1.24.1-p1 in session mode.

| workload | target | tps | latency avg (ms) | vs direct | p50 (ms) | p99 (ms) | p50 vs direct |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| select | direct | 60062 | 0.728 | - | 0.579 | 3.477 | - |
| select | lepis-transaction | 59922 | 0.409 | -319 µs | 0.268 | 1.462 | -311 µs |
| select | pgbouncer | 59999 | 4.806 | +4078 µs | 0.568 | 175.889 | -11 µs |

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
number of transactions actually processed: 1795808
number of failed transactions: 0 (0.000%)
latency average = 0.728 ms
latency stddev = 0.696 ms
rate limit schedule lag: avg 0.540 (max 13.838) ms
initial connection time = 104.420 ms
tps = 60062.318308 (without initial connection time)
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
number of transactions actually processed: 1788252
number of failed transactions: 0 (0.000%)
latency average = 0.409 ms
latency stddev = 0.345 ms
rate limit schedule lag: avg 0.207 (max 7.350) ms
initial connection time = 158.612 ms
tps = 59922.622247 (without initial connection time)
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
number of transactions actually processed: 1789043
number of failed transactions: 0 (0.000%)
latency average = 4.806 ms
latency stddev = 25.629 ms
rate limit schedule lag: avg 4.385 (max 231.202) ms
initial connection time = 184.274 ms
tps = 59999.910120 (without initial connection time)
```

