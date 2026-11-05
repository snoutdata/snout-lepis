# Growing a chunked table, branch (e4ed8a0 + the move-preparation lock fix), 2026-11-01

`ev` grown by 5000000 rows a round over 8 rounds through the router (COPY), 4 nodes, Postgres 18, Docker,
one box. Distributed empty: 4 ranges, 16 chunks. Keys spread over 10M values; key 42 holds 1 row in 33.
Autopilot on: `advice_split_bytes` 512 MB, `advice_min_bytes` an eighth of it, `advice_hold_s` 60, every 10 s. pgbench writes and
point-reads through the router throughout (4 clients).

| round | rows | COPY rows/s | chunks | ranges per node | MB on nodes | MB per node | largest chunk MB | autopilot jobs | settle s | max pause ms | failed jobs | advice left | COPY retries |
| ---: | ---: | ---: | ---: | --- | ---: | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 5000000 | 134535 | 16 | 1 1 1 1 | 852 | 206 207 230 207 | 75 | - | 2 | - | 0 | 0 | 0 |
| 2 | 10000000 | 118449 | 16 | 1 1 1 1 | 1675 | 406 407 452 409 | 145 | - | 2 | - | 0 | 0 | 0 |
| 3 | 15000000 | 109087 | 16 | 2 2 3 1 | 2494 | 756 756 829 151 | 223 | range.move 1, range.split 4 | 85 | 1048.4 | 0 | 0 | 0 |
| 4 | 20000000 | 100595 | 16 | 2 3 3 2 | 3316 | 603 1008 1097 607 | 291 | range.split 2 | 33 | 34.5 | 0 | 0 | 0 |
| 5 | 25000000 | 94189 | 16 | 5 4 4 3 | 4235 | 1286 1029 1144 774 | 367 | range.split 6 | 209 | 35.9 | 0 | 0 | 0 |
| 6 | 30000000 | 81352 | 16 | 5 4 4 3 | 4977 | 1511 1209 1348 907 | 442 | - | 2 | - | 0 | 0 | 0 |
| 7 | 35000000 | 73366 | 17 | 5 4 4 3 | 5704 | 1730 1389 1542 1041 | 349 | chunk.split 1 | 22 | 20.4 | 0 | 0 | 0 |
| 8 | 40000000 | 50244 | 17 | 5 4 4 4 | 6550 | 1986 1594 1570 1398 | 401 | range.split 1 | 12 | 22.3 | 0 | 0 | 0 |

| check | value |
| --- | --- |
| loaded rows, sum(id): expected | 40000000, 800000020000000 |
| loaded rows, sum(id): through the router | 40000000 800000020000000 |
| hot key 42 rows: expected, got | 1212125, 1212125 |
| probe clients | 1711898 tx, mean 2.5 ms, worst 10 s mean 8.5 ms, slowest 1059 ms, 0 idle 10 s intervals; 0 aborted |
| stalled 10 s intervals (epoch s) | none |
| a failed COPY that left rows | none |
