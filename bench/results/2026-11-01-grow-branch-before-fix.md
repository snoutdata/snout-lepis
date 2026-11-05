# Growing a chunked table, branch (e4ed8a0, before the move-preparation lock fix), 2026-11-01

`ev` grown by 5000000 rows a round over 8 rounds through the router (COPY), 4 nodes, Postgres 18, Docker,
one box. Distributed empty: 4 ranges, 16 chunks. Keys spread over 10M values; key 42 holds 1 row in 33.
Autopilot on: `advice_split_bytes` 512 MB, `advice_min_bytes` an eighth of it, `advice_hold_s` 60, every 10 s. pgbench writes and
point-reads through the router throughout (4 clients).

| round | rows | COPY rows/s | chunks | ranges per node | MB on nodes | MB per node | largest chunk MB | autopilot jobs | settle s | max pause ms | failed jobs |
| ---: | ---: | ---: | ---: | --- | ---: | --- | ---: | --- | ---: | ---: | ---: |
| 1 | 5000000 | 124786 | 16 | 1 1 1 1 | 852 | 207 230 207 206 | 75 | - | 2 | - | 0 |
| 2 | 10000000 | 106999 | 16 | 1 1 1 1 | 1670 | 406 449 407 406 | 145 | - | 2 | - | 0 |
| 3 | 15000000 | 112767 | 16 | 3 2 2 2 | 2489 | 604 756 604 523 | 221 | range.move 2, range.split 5 | 127 | 140.6 | 0 |
| 4 | 20000000 | 101488 | 16 | 3 2 2 3 | 3299 | 800 601 803 1094 | 293 | range.split 1 | 12 | 32.1 | 0 |
| 5 | 25000000 | 90858 | 16 | 3 2 2 3 | 4175 | 1018 764 1016 1376 | 359 | - | 2 | - | 0 |
| 6 | 30000000 | 101071 | 16 | 4 4 4 4 | 4944 | 1201 1199 1201 1342 | 438 | range.split 6 | 137 | 46.6 | 0 |
| 7 | 35000000 | 88998 | 17 | 4 4 4 4 | 5681 | 1378 1377 1379 1545 | 348 | chunk.split 1 | 33 | 23.7 | 0 |
| 8 | 40000000 | 101208 | 17 | 5 4 4 4 | 6508 | 1776 1582 1582 1568 | 399 | range.split 1 | 1 | 29.6 | 0 |

| check | value |
| --- | --- |
| loaded rows, sum(id): expected | 40000000, 800000020000000 |
| loaded rows, sum(id): through the router | 40000000 800000020000000 |
| hot key 42 rows: expected, got | 1212121, 1212125 |
| probe clients | 1372184 tx, mean 2.9 ms, worst 10 s mean 759.7 ms, slowest 37321 ms, 8 idle 10 s intervals; 0 aborted |

Notes, added after the run: the probe line read the pgbench script with the latency logs (since fixed),
so 4 of the 8 idle intervals are that script's lines; the 37 s slowest transaction is real. The hot-key
"expected" was wrong (1 in 10M other rows also hash to 42; the script now counts it exactly). A second
run before the fix, with the stall report, saw two ~25 s stalls, each a range split's preparation whose
`ALTER TABLE` on the target's shell waited behind that round's COPY (seen live in `pg_locks`); the
fix's run is `2026-11-01-grow-branch.md`.
