# Repartitioned joins, branch-review (e4ed8a0), 2026-11-01

big_a, big_b: 10000000 rows each; mid_a, mid_b: 1000000 rows each; all distributed by k (chunked), joined on id (not the key). 4 nodes and a plain Postgres, Postgres 18, Docker, one 4-vCPU box. Best of 3 (mid) or 2 (big), ms.

| join | router | plain | same answer |
| --- | ---: | ---: | --- |
| mid ⋈ mid, through the router | 5434 | 232 | yes |
| mid ⋈ mid, node to node | 2402 | 232 | yes |
| big ⋈ big, node to node | 22188 | 2256 | yes |
| big ⋈ big filtered + group, node to node | 22016 | 1867 | yes |
| shuffle tables left on the nodes | 0 | | |
