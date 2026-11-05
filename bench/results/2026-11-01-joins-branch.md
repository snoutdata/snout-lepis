# Repartitioned joins, branch (849b736), 2026-11-01

big_a, big_b: 10000000 rows each; mid_a, mid_b: 1000000 rows each; all distributed by k (chunked), joined on id (not the key). 4 nodes and a plain Postgres, Postgres 18, Docker, one 4-vCPU box. Best of 3 (mid) or 2 (big), ms.

| join | router | plain | same answer |
| --- | ---: | ---: | --- |
| mid ⋈ mid, through the router | 4817 | 231 | yes |
| mid ⋈ mid, node to node | 2821 | 231 | yes |
| big ⋈ big, node to node | 24710 | 2423 | yes |
| big ⋈ big filtered + group, node to node | 22512 | 1856 | yes |
| shuffle tables left on the nodes | 0 | | |
