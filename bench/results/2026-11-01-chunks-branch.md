# Chunked storage, branch (85b00a0), 2026-11-01

10000000 rows in each of big_a and big_b (Zipf s=1 keys, 2 secondary indexes each), 1M-row dim, 50k-row small; 4 nodes and a plain Postgres, Postgres 18, Docker, one 4-vCPU box.

| measure | value |
| --- | ---: |
| distribute 3 tables (s) | 324 |
| move 25% of node 2: time (s) | 38.9 |
| source WAL during the move (MB) | 107 |
| source disk given back (MB) | 379 |
| cutover write pause (ms) | 144.4 |
| p99 read during the move (ms) | 367.9 |
| p99 write during the move (ms) | 50.7 |
| broadcast join, router vs plain (ms) | 956 vs 528 |
| window over small (gather onto home), router vs plain (ms) | 233 vs 26 |
| advice after the run | none |
| autopilot jobs on skewed reads: first 2 min, next 2 min | 0, 0 |
