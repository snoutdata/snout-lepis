# Lepis Phase 0: pgbench, laptop-m3max-tail-rate60k-b

`bash lepis/bench/pgbench.sh 30 64 laptop-m3max-tail-rate60k-b`, 2026-10-11T03:21:20Z.
Host: Darwin 27.0.0 arm64, ? CPUs. postgres:18, scale 20, 64 clients, 30 s each, at a fixed 60000 transactions/s in all (pgbench -R).
Lepis: the router (a one-node catalog), pool_mode session and transaction. PgBouncer docker.io/edoburu/pgbouncer:v1.24.1-p1 in session mode.

| workload | target | tps | latency avg (ms) | vs direct | p50 (ms) | p99 (ms) | p50 vs direct |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| select | direct | 59934 | 0.733 | - | 0.582 | 3.747 | - |
| select | pgbouncer | 59974 | 4.267 | +3534 µs | 0.551 | 155.728 | -31 µs |
| select | lepis-transaction | 60031 | 0.407 | -326 µs | 0.266 | 1.462 | -316 µs |
| select | direct | 60033 | 0.746 | - | 0.592 | 3.779 | - |

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
number of transactions actually processed: 1792747
number of failed transactions: 0 (0.000%)
latency average = 0.733 ms
latency stddev = 0.712 ms
rate limit schedule lag: avg 0.547 (max 12.243) ms
initial connection time = 95.908 ms
tps = 59934.396507 (without initial connection time)
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
number of transactions actually processed: 1788203
number of failed transactions: 0 (0.000%)
latency average = 4.267 ms
latency stddev = 22.985 ms
rate limit schedule lag: avg 3.867 (max 227.865) ms
initial connection time = 185.796 ms
tps = 59974.840258 (without initial connection time)
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
number of transactions actually processed: 1792935
number of failed transactions: 0 (0.000%)
latency average = 0.407 ms
latency stddev = 0.341 ms
rate limit schedule lag: avg 0.209 (max 8.817) ms
initial connection time = 133.797 ms
tps = 60031.415683 (without initial connection time)
```

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
number of transactions actually processed: 1795371
number of failed transactions: 0 (0.000%)
latency average = 0.746 ms
latency stddev = 0.704 ms
rate limit schedule lag: avg 0.552 (max 10.808) ms
initial connection time = 96.176 ms
tps = 60033.167573 (without initial connection time)
```

