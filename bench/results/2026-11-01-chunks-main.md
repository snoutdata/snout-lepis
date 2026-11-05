# Chunked storage, main (866139a), 2026-11-01

10000000 rows in each of big_a and big_b (Zipf s=1 keys, 2 secondary indexes each), 1M-row dim, 50k-row small; 4 nodes and a plain Postgres, Postgres 18, Docker, one 4-vCPU box.

| measure | value |
| --- | ---: |
| distribute 3 tables (s) | 179 |
| move 25% of node 2: time (s) | 130.6 |
| source WAL during the move (MB) | 1584 |
| source disk given back (MB) | -2 |
| cutover write pause (ms) | 48.5 |
| p99 read during the move (ms) | 974.5 |
| p99 write during the move (ms) | 38.1 |
| broadcast join, router vs plain (ms) | 1 vs 632 |
| window over small (gather onto home), router vs plain (ms) | 17 vs 26 |
| advice after the run | range.move (size) |
| autopilot jobs on skewed reads: first 2 min, next 2 min | 0, 0 |
