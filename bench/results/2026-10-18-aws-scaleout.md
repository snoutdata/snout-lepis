# Lepis scale-out: 1, 2, 4 and 8 data nodes, aws

Throughput through the router as one data set is spread over 1, 2, 4 and 8 Postgres nodes, each on
a machine of its own, with each node count also run straight at the nodes (no router) and through
two routers sharing one catalog. Run 2026-10-18 02:43 to 05:12 UTC with `lepis/bench/scaleout.sh`.
All numbers MEASURED.

## Rig

- us-west-2, one zone, default VPC, throwaway hosts (Debian 13, rootful Podman 5.4.2, every container
  on the host's network), torn down after the run:
  - 8 nodes: `c7g.xlarge` (4 vCPU Graviton3, 7.6 GB), 20 GB gp3 data volume (3,000 IOPS, 125 MB/s
    baseline) holding the Postgres data directory.
  - 2 routers: `c7g.2xlarge` (8 vCPU, 15.3 GB).
  - 1 client: `c7g.4xlarge` (16 vCPU, 30.8 GB), pgbench 18.6. It never passed 47% busy.
- Postgres `postgres:18` (18.6, `sha256:a63b2416…`): `max_connections=600`, `shared_buffers=1944MB`
  (a quarter of memory), `max_prepared_transactions=64`, `max_wal_size=6GB`,
  `checkpoint_timeout=20min`, everything else stock (`synchronous_commit=on`, `wal_level=replica`).
- Lepis: the fleet's ECR build `s-fleet/lepis:0.1.11` (`sha256:d553993e…`), `LEPIS_LOG=warn`, pool
  mode session (the default) unless marked `txn-`.
- Data, the same total at every N: 10,000,000 `pgbench_accounts` rows (pgbench scale 100) distributed
  by `aid` (keyspace `acct`, integer, seed 0) with `pgbench_history` colocated; a TPC-B set of 4,000
  branches (10 tellers and 1,000 accounts each, 4,000,000 accounts) distributed by `bid` (keyspace
  `branch`, seed 7). One equal hash range per node in each keyspace. Each node is loaded with only
  the rows its range owns (generated on the node, filtered by the hash its fence then checks), fenced
  with `lepis_owns`, vacuumed, checkpointed and read into memory (`pg_prewarm`). Database size per
  node: 2.7 GB at N=1, 1.36 GB at N=2, about 345 MB at N=8. At N=8 the nodes own 1,248,104 to
  1,251,677 accounts and 462 to 519 branches.
- Each run 30 s, `-M prepared`, after a 20 s unmeasured warm-up of the same workload through router 1
  at the start of each workload's runs. Latency from `pgbench -l --sampling-rate=0.1` over every
  pgbench process of the run. CPU: busy percent of each whole host from `/proc/stat`, sampled 3 s
  into the run and 2 s before its end (a probe on each host answers each connection with
  `/proc/stat`).

## Workloads and targets

- `select`: `SELECT abalance FROM pgbench_accounts WHERE aid = :aid`, aid uniform over 10M.
- `write`: `BEGIN; UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
  INSERT INTO pgbench_history (…) VALUES (…, :aid, …); END;`, one node per transaction.
- `tpcb`: the TPC-B mix (update account, read it, update teller, update branch, insert history) with
  the account, teller and branch of one branch, so the transaction stays on one node. pgbench's own
  mix picks three independent keys and would make nearly every transaction a two-phase one.
- `direct` (N=1 only): pgbench straight at the one node, the same scripts.
- `keyed`: pgbench straight at every node at once, the clients split evenly, each process picking
  keys its node owns from a per-node key table: one extra index probe per statement (a subselect),
  for `tpcb` one extra round trip (a `\gset` before `BEGIN`). The nodes with no router. Its own cost
  shows at N=1: select 54.6k against direct's 73.2k at 32 clients, write and tpcb within 10%.
- `routed1`: every client through router 1. `routed2`: half the clients through each router.
- The client count is the total over all pgbench processes of a run.

## Peak throughput per node count

The best run of each target over its client sweep (tps, at clients, p50 / p99 ms; router CPU is
percent of its 8 vCPUs; nodes is the average busy percent).

### select

| N | direct or keyed | routed1 | router 1 CPU | nodes | routed2 | routers CPU | routed1 vs N=1 |
| ---: | --- | --- | ---: | ---: | --- | ---: | ---: |
| 1 | direct 73,191 @32, .434 / .950 | 68,296 @32, .468 / .622 | 24% | 97% | | | 1.00 |
| 2 | keyed 123,468 @64, .494 / 1.145 | 129,753 @128, .821 / 2.400 | 46% | 98% | | | 1.90 |
| 4 | keyed 249,216 @64, .246 / .516 | 223,356 @128, .535 / 1.316 | 70% | 89% | 237,045 @128, .487 / 1.292 | 42 / 44% | 3.27 |
| 8 | keyed 504,827 @256, .489 / 1.033 | 341,126 @512, 1.408 / 2.497 | **97%** | 87% | 409,449 @256, .537 / 1.771 | 64 / 68% | 4.99 (routed2 6.00) |

### write

| N | direct or keyed | routed1 | router 1 CPU | nodes | routed2 | routers CPU | routed1 vs N=1 |
| ---: | --- | --- | ---: | ---: | --- | ---: | ---: |
| 1 | direct 11,608 @256, 20.0 / 58.4 | 12,169 @64, 4.88 / 23.1 | 19% | 94% | | | 1.00 |
| 2 | keyed 26,028 @128, 4.84 / 7.76 | 25,734 @128, 4.70 / 8.38 | 37% | 93% | | | 2.11 |
| 4 | keyed 51,411 @256, 4.90 / 7.70 | 45,923 @256, 4.87 / 12.6 | 62% | 92% | 47,364 @256, 4.68 / 12.9 | 36 / 35% | 3.77 |
| 8 | keyed 103,722 @512, 4.85 / 7.64 | 76,799 @512\*, 5.83 / 18.1 | **88%** | 86% | 85,626 @512\*, 4.70 / 19.4 | 60 / 60% | 6.31 (routed2 7.04) |

### tpcb

| N | direct or keyed | routed1 | router 1 CPU | nodes | routed2 | routers CPU | routed1 vs N=1 |
| ---: | --- | --- | ---: | ---: | --- | ---: | ---: |
| 1 | direct 7,644 @64, 8.28 / 13.1 | 7,611 @64, 8.25 / 12.1 | 20% | 99% | | | 1.00 |
| 2 | keyed 14,619 @128, 8.71 / 13.3 | 14,124 @128, 8.11 / 19.8 | 37% | 94% | | | 1.86 |
| 4 | keyed 28,483 @256, 8.84 / 14.4 | 25,134 @256, 7.04 / 36.4 | 60% | 91% | 24,759 @128, 4.93 / 8.58 | 32 / 33% | 3.30 |
| 8 | keyed 58,558 @384, 6.46 / 10.4 | 44,238 @512\*, 9.22 / 44.1 | **89%** | 90% | 45,952 @256, 5.17 / 10.4 | 54 / 55% | 5.81 (routed2 6.04) |

\* With the home node at `max_connections=1200` (below). At 600, the best clean N=8 routed runs are
at 256 clients: write 62,551 (routed1) and 71,574 (routed2), tpcb 39,322 and 45,952.

N=1 and N=2 rows are from the rerun (CPU figures below). Against direct at N=1, the router costs
7% of select throughput at 32 clients and nothing at 64 or more; write and tpcb are within run to run
noise of direct.

## What limited it

- **N=1 and N=2: the nodes.** 94-100% busy while the router used 19-46% of its host. Routed is
  within 7% of direct and keyed, either way. A node's select throughput peaks at 32-64 clients and
  falls with more (73k at 32, 48k at 256 direct): 4 vCPUs and hundreds of backends.
- **N=4: mostly the nodes.** Router 1 at 62-70%. routed1 reaches 88-90% of keyed; a second router
  adds 6% (select), 3% (write) and nothing (tpcb).
- **N=8: one router.** Router 1 at 97% (select), 88% (write) and 89% (tpcb) of its 8 vCPUs, the nodes
  at 86-90%: routed1 reaches 68% (select), 74% (write) and 76% (tpcb) of keyed. One `c7g.2xlarge`
  router passes about 340k single-node statements a second. Two routers reach 81%, 83% and 78% of
  keyed with each router at 54-68% and the nodes at 82-91%, so neither the routers nor the nodes are
  saturated there. Two causes measured: (1) a node spends more CPU per transaction behind the router
  than when clients reach it directly: on node 1, select at 256 clients, 75.0 µs of CPU per
  transaction through two routers against 64.0 µs keyed (same packets per transaction, 1.00 in and
  1.00 out in both), though keyed's statement carries an extra index probe; each node then serves
  every client's session (256 backends) instead of its own 32, which is the leading explanation and
  is not shown. Transaction pool mode did not change it (`txn-` rows: select 395k against 409k at
  256 clients, write 71.6k against 71.6k, tpcb 45.5k against 46.0k). (2) The slowest node paces
  routed clients: keyed per-node rates differ by up to 8% (node 4 the slowest, 60.7k against 65.4k
  select at 256), and a routed client waits on whichever node its next key is on.
- **Tails.** Routed runs at high client counts have a lower p50 and a higher p99 than keyed (N=8
  write at 512: p50 4.70 against 4.85 ms, p99 19.4 against 7.6 ms).
- **Not the disks, not the network, not the client.** Node iowait never passed 16%; no ENA allowance
  counter (bandwidth, packets, conntrack) moved on node 1 or router 1; the client stayed under 47%.

## A defect it found: the first prepared transaction of a session commits in two phases

With `-M prepared`, a session's first transaction commits through two-phase commit although every
statement in it is for one node. Measured at N=8: 4 clients doing 1 write transaction each logged 3
two-phase commits on the home node, 4 clients doing 50 each logged 4, and the same with `-M extended`
or `-M simple` logged 0. In the N=4 runs the
home's `lepis.prepared` saw 1,238 inserts while about 14.6M write transactions committed.

What it costs: each such session keeps a connection from the router to the home's two-phase commit
log, so the home holds every client's session plus that many more. Past about 300 clients the home
(600 connections) refused: `ERROR: the transaction was rolled back: the two-phase commit log on the
home node is not usable: the node refused: sorry, too many clients already`, and pgbench dropped each
client that got it (63 to 304 of 384 or 512; marked in the tables, those rows undercount). With the
home at 1200 connections the same runs were clean but one: at N=8 tpcb, 512 clients through two
routers, the burst of first transactions hit the home's `max_prepared_transactions` (64) and 73
clients got `maximum number of prepared transactions reached`. The router's log has `in-doubt
recovery: node 1: the node refused: sorry, too many clients already` five times. Not fixed here.

## Commands

```bash
# Hosts (each, by hand): the fleet's create-host.sh --region us-west-2 --type <type> --throwaway --data-gb 20
# Main run, N = 1, 2, 4, 8:
LEPIS_SCALEOUT_CLIENTS_1="32 64 128 256" LEPIS_SCALEOUT_CLIENTS_2="64 128 256" \
LEPIS_SCALEOUT_CLIENTS_4="64 128 256 512" LEPIS_SCALEOUT_CLIENTS_8="128 256 512" \
  bash lepis/bench/scaleout.sh --routers sd-hxabx4t9,sd-sgage9gd --client sd-25y3668q \
  --nodes sd-sak111he,sd-p3ds0211,sd-08avbsgp,sd-qzjts46e,sd-62sjyqxx,sd-vt9m84bb,sd-154jepby,sd-te74e1zs aws-scaleout
# Then, on the client with N=8 still loaded (the driver's own steps):
driver.sh run 8 write 'keyed routed1 routed2' 384; driver.sh run 8 tpcb 'keyed routed1 routed2' 384
#   the home recreated with max_connections=1200, the routers restarted, then:
driver.sh run 8 write 'routed1 routed2' '384 512'; driver.sh run 8 tpcb 'routed1 routed2' '384 512'
# Rerun of N = 1 and 2 (the main run's first CPU samples were wrong, below):
LEPIS_SCALEOUT_COUNTS="1 2" LEPIS_SCALEOUT_TWO_AT="" LEPIS_SCALEOUT_CLIENTS_1="32 64 128 256" \
LEPIS_SCALEOUT_CLIENTS_2="64 128 256" bash lepis/bench/scaleout.sh … aws-scaleout-rerun
# Then N=8 loaded again (driver.sh load 8, routers restarted), pool_mode transaction set in
# lepis.cluster.settings, and driver.sh run 8 <select|write|tpcb> 'routed1 routed2' '256 512'.
```

**What went wrong in the run, and what was done.** (1) The driver's CPU sample waited for the
pgbench processes as well as for its own reads, so the first sample of each run came at its end and
the window was about half run, half idle: every CPU figure of the main run's N=1 and of its N=2
select was about half the truth. Fixed (`sample()` waits for its own reads only) before N=2's write
runs, and N=1 and N=2 were run again; the tables use the rerun for both. Their throughput is a repeat:
select within -3.4% to +3.0% of the first pass, tpcb -6.6% to +3.4%, N=2 -0.9% to +2.1%, and N=1
write -18% to +7% (one node's write rate moves that much from run to run here). (2) The main run's
last step (fetching its results) failed because the script was edited while bash was still reading
it; the results were fetched by hand. (3) The `txn-` runs' raw pgbench output was not fetched before
the hosts were terminated; their summary lines are below.

Raw pgbench output: `results/raw/2026-10-18-aws-scaleout-pgbench.md` (the main run, its N=8 extras;
its N=1 and N=2 select sections are the first pass) and
`results/raw/2026-10-18-aws-scaleout-rerun-pgbench.md` (the rerun).

## All runs

Notes: "clients aborted" counts pgbench's error lines, one per client dropped (the home's connection
limit, above). CPU figures are percent of the host.

### N=1 and N=2 (rerun)

| N | workload | target | clients | tps | p50 ms | p99 ms | router 1 % | router 2 % | nodes % avg (min-max) | node iowait % max | client % | note |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | --- |
| 1 | select | direct | 32 | 73,191 | 0.434 | 0.950 | - | - | 100 | 0 | 5 |  |
| 1 | select | keyed | 32 | 54,609 | 0.544 | 1.461 | - | - | 100 | 0 | 4 |  |
| 1 | select | routed1 | 32 | 68,296 | 0.468 | 0.622 | 24 | - | 97 | 0 | 5 |  |
| 1 | select | direct | 64 | 68,642 | 0.910 | 2.115 | - | - | 100 | 0 | 6 |  |
| 1 | select | keyed | 64 | 53,596 | 1.157 | 3.025 | - | - | 100 | 0 | 4 |  |
| 1 | select | routed1 | 64 | 68,253 | 0.906 | 1.674 | 25 | - | 100 | 0 | 5 |  |
| 1 | select | direct | 128 | 61,575 | 1.980 | 5.035 | - | - | 100 | 0 | 5 |  |
| 1 | select | keyed | 128 | 45,080 | 2.730 | 7.543 | - | - | 100 | 0 | 4 |  |
| 1 | select | routed1 | 128 | 62,053 | 1.979 | 4.252 | 23 | - | 100 | 0 | 5 |  |
| 1 | select | direct | 256 | 47,896 | 5.037 | 13.596 | - | - | 100 | 0 | 5 |  |
| 1 | select | keyed | 256 | 37,427 | 6.526 | 17.907 | - | - | 100 | 0 | 4 |  |
| 1 | select | routed1 | 256 | 48,554 | 5.035 | 11.924 | 19 | - | 100 | 0 | 5 |  |
| 1 | write | direct | 32 | 10,375 | 3.059 | 4.691 | - | - | 71 | 7 | 3 |  |
| 1 | write | keyed | 32 | 10,354 | 3.146 | 4.601 | - | - | 79 | 6 | 3 |  |
| 1 | write | routed1 | 32 | 7,719 | 3.211 | 22.788 | 10 | - | 53 | 20 | 2 |  |
| 1 | write | direct | 64 | 10,181 | 4.823 | 24.679 | - | - | 69 | 15 | 3 |  |
| 1 | write | keyed | 64 | 9,692 | 5.390 | 24.525 | - | - | 80 | 9 | 3 |  |
| 1 | write | routed1 | 64 | 12,169 | 4.884 | 23.147 | 19 | - | 94 | 2 | 4 |  |
| 1 | write | direct | 128 | 11,133 | 9.980 | 25.750 | - | - | 83 | 8 | 4 |  |
| 1 | write | keyed | 128 | 9,854 | 11.424 | 28.406 | - | - | 85 | 8 | 3 |  |
| 1 | write | routed1 | 128 | 10,986 | 9.936 | 25.189 | 17 | - | 86 | 7 | 4 |  |
| 1 | write | direct | 256 | 11,608 | 20.048 | 58.423 | - | - | 99 | 0 | 4 |  |
| 1 | write | keyed | 256 | 9,984 | 23.224 | 65.006 | - | - | 97 | 1 | 4 |  |
| 1 | write | routed1 | 256 | 10,432 | 23.127 | 52.991 | 16 | - | 90 | 4 | 4 |  |
| 1 | tpcb | direct | 32 | 5,755 | 4.764 | 14.282 | - | - | 73 | 13 | 3 |  |
| 1 | tpcb | keyed | 32 | 6,302 | 5.018 | 7.903 | - | - | 86 | 4 | 4 |  |
| 1 | tpcb | routed1 | 32 | 6,434 | 4.872 | 6.591 | 16 | - | 82 | 6 | 3 |  |
| 1 | tpcb | direct | 64 | 7,644 | 8.280 | 13.078 | - | - | 99 | 0 | 4 |  |
| 1 | tpcb | keyed | 64 | 6,835 | 9.315 | 14.512 | - | - | 99 | 0 | 4 |  |
| 1 | tpcb | routed1 | 64 | 7,611 | 8.254 | 12.147 | 20 | - | 99 | 0 | 4 |  |
| 1 | tpcb | direct | 128 | 6,902 | 17.627 | 41.849 | - | - | 99 | 0 | 4 |  |
| 1 | tpcb | keyed | 128 | 6,275 | 19.533 | 41.420 | - | - | 100 | 0 | 4 |  |
| 1 | tpcb | routed1 | 128 | 6,971 | 17.483 | 38.531 | 19 | - | 100 | 0 | 4 |  |
| 1 | tpcb | direct | 256 | 6,191 | 35.304 | 126.056 | - | - | 100 | 0 | 4 |  |
| 1 | tpcb | keyed | 256 | 5,473 | 40.517 | 131.877 | - | - | 100 | 0 | 4 |  |
| 1 | tpcb | routed1 | 256 | 6,194 | 34.024 | 138.315 | 17 | - | 100 | 0 | 4 |  |
| 2 | select | keyed | 64 | 123,468 | 0.494 | 1.145 | - | - | 100 (100-100) | 0 | 10 |  |
| 2 | select | routed1 | 64 | 126,142 | 0.492 | 0.809 | 44 | - | 92 (90-94) | 0 | 10 |  |
| 2 | select | keyed | 128 | 117,707 | 1.052 | 2.506 | - | - | 100 (100-100) | 0 | 10 |  |
| 2 | select | routed1 | 128 | 129,753 | 0.821 | 2.400 | 46 | - | 98 (96-100) | 0 | 11 |  |
| 2 | select | keyed | 256 | 106,020 | 2.331 | 5.842 | - | - | 100 (100-100) | 0 | 10 |  |
| 2 | select | routed1 | 256 | 102,569 | 1.015 | 8.061 | 39 | - | 92 (85-100) | 0 | 9 |  |
| 2 | write | keyed | 64 | 21,280 | 3.080 | 4.440 | - | - | 76 (76-77) | 6 | 6 |  |
| 2 | write | routed1 | 64 | 19,088 | 3.250 | 4.405 | 28 | - | 69 (67-71) | 9 | 6 |  |
| 2 | write | keyed | 128 | 26,028 | 4.842 | 7.756 | - | - | 96 (96-96) | 1 | 8 |  |
| 2 | write | routed1 | 128 | 25,734 | 4.696 | 8.383 | 37 | - | 93 (89-97) | 3 | 8 |  |
| 2 | write | keyed | 256 | 25,345 | 9.795 | 19.824 | - | - | 99 (99-99) | 0 | 9 |  |
| 2 | write | routed1 | 256 | 23,705 | 8.128 | 32.944 | 36 | - | 94 (87-100) | 3 | 9 |  |
| 2 | tpcb | keyed | 64 | 14,278 | 4.488 | 6.491 | - | - | 92 (92-93) | 3 | 9 |  |
| 2 | tpcb | routed1 | 64 | 12,397 | 5.101 | 7.149 | 31 | - | 80 (77-84) | 6 | 6 |  |
| 2 | tpcb | keyed | 128 | 14,619 | 8.709 | 13.301 | - | - | 99 (99-99) | 0 | 9 |  |
| 2 | tpcb | routed1 | 128 | 14,124 | 8.112 | 19.762 | 37 | - | 94 (89-99) | 3 | 8 |  |
| 2 | tpcb | keyed | 256 | 13,700 | 17.890 | 37.817 | - | - | 100 (100-100) | 0 | 10 |  |
| 2 | tpcb | routed1 | 256 | 12,151 | 15.131 | 88.801 | 33 | - | 91 (82-100) | 5 | 8 |  |

### N=4 and N=8

| N | workload | target | clients | tps | p50 ms | p99 ms | router 1 % | router 2 % | nodes % avg (min-max) | node iowait % max | client % | note |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | --- |
| 4 | select | keyed | 64 | 249,216 | 0.246 | 0.516 | - | - | 97 (92-100) | 0 | 19 |  |
| 4 | select | routed1 | 64 | 158,275 | 0.397 | 0.570 | 54 | - | 64 (59-68) | 0 | 12 |  |
| 4 | select | routed2 | 64 | 178,507 | 0.347 | 0.534 | 31 | 35 | 70 (67-74) | 0 | 14 |  |
| 4 | select | keyed | 128 | 245,835 | 0.494 | 1.082 | - | - | 100 (100-100) | 0 | 20 |  |
| 4 | select | routed1 | 128 | 223,356 | 0.535 | 1.316 | 70 | - | 89 (84-93) | 0 | 18 |  |
| 4 | select | routed2 | 128 | 237,045 | 0.487 | 1.292 | 42 | 44 | 93 (88-97) | 0 | 18 |  |
| 4 | select | keyed | 256 | 236,639 | 1.043 | 2.367 | - | - | 100 (100-100) | 0 | 21 |  |
| 4 | select | routed1 | 256 | 209,002 | 0.685 | 4.664 | 69 | - | 93 (84-99) | 0 | 18 |  |
| 4 | select | routed2 | 256 | 209,237 | 0.505 | 5.252 | 39 | 41 | 92 (84-99) | 0 | 19 |  |
| 4 | select | keyed | 512 | 215,203 | 2.282 | 5.550 | - | - | 100 (100-100) | 0 | 23 |  |
| 4 | select | routed1 | 512 | 187,131 | 0.657 | 12.011 | 67 | - | 92 (85-98) | 0 | 20 |  |
| 4 | select | routed2 | 512 | 183,905 | 0.503 | 12.785 | 37 | 38 | 90 (83-100) | 0 | 20 |  |
| 4 | write | keyed | 64 | 29,145 | 1.990 | 3.138 | - | - | 52 (44-59) | 14 | 8 |  |
| 4 | write | routed1 | 64 | 21,507 | 2.936 | 3.765 | 31 | - | 39 (36-42) | 16 | 6 |  |
| 4 | write | routed2 | 64 | 22,462 | 2.864 | 3.606 | 16 | 17 | 41 (38-43) | 15 | 7 |  |
| 4 | write | keyed | 128 | 42,958 | 3.061 | 4.212 | - | - | 78 (77-78) | 6 | 13 |  |
| 4 | write | routed1 | 128 | 36,833 | 3.389 | 4.742 | 51 | - | 70 (66-74) | 9 | 12 |  |
| 4 | write | routed2 | 128 | 38,593 | 3.228 | 4.628 | 28 | 29 | 73 (69-76) | 8 | 12 |  |
| 4 | write | keyed | 256 | 51,411 | 4.904 | 7.698 | - | - | 96 (95-97) | 1 | 17 |  |
| 4 | write | routed1 | 256 | 45,923 | 4.870 | 12.625 | 62 | - | 92 (87-95) | 3 | 16 |  |
| 4 | write | routed2 | 256 | 47,364 | 4.682 | 12.854 | 36 | 35 | 92 (87-96) | 3 | 16 |  |
| 4 | write | keyed | 512 | 49,596 | 10.002 | 20.379 | - | - | 99 (99-99) | 0 | 20 |  |
| 4 | write | routed1 | 512 | 44,556 | 5.442 | 29.751 | 62 | - | 92 (86-98) | 3 | 16 | 157 clients aborted |
| 4 | write | routed2 | 512 | 44,461 | 4.691 | 31.909 | 32 | 38 | 91 (86-99) | 4 | 17 | 160 clients aborted |
| 4 | tpcb | keyed | 64 | 20,102 | 3.040 | 4.280 | - | - | 68 (66-70) | 9 | 12 |  |
| 4 | tpcb | routed1 | 64 | 15,049 | 4.131 | 5.349 | 38 | - | 50 (47-55) | 13 | 8 |  |
| 4 | tpcb | routed2 | 64 | 16,194 | 3.912 | 5.236 | 20 | 22 | 54 (52-58) | 12 | 9 |  |
| 4 | tpcb | keyed | 128 | 27,924 | 4.544 | 6.534 | - | - | 93 (93-94) | 2 | 17 |  |
| 4 | tpcb | routed1 | 128 | 23,627 | 5.203 | 8.810 | 56 | - | 81 (78-84) | 6 | 13 |  |
| 4 | tpcb | routed2 | 128 | 24,759 | 4.931 | 8.575 | 32 | 33 | 84 (80-88) | 6 | 12 |  |
| 4 | tpcb | keyed | 256 | 28,483 | 8.837 | 14.392 | - | - | 100 (99-100) | 0 | 19 |  |
| 4 | tpcb | routed1 | 256 | 25,134 | 7.035 | 36.396 | 60 | - | 91 (86-98) | 4 | 15 |  |
| 4 | tpcb | routed2 | 256 | 24,742 | 6.148 | 45.641 | 34 | 34 | 90 (84-96) | 4 | 15 |  |
| 4 | tpcb | keyed | 512 | 26,575 | 18.295 | 39.169 | - | - | 100 (100-100) | 0 | 22 |  |
| 4 | tpcb | routed1 | 512 | 23,058 | 6.332 | 90.959 | 58 | - | 88 (81-95) | 5 | 15 | 152 clients aborted |
| 4 | tpcb | routed2 | 512 | 23,190 | 5.603 | 93.922 | 33 | 32 | 87 (81-100) | 5 | 15 | 157 clients aborted |
| 8 | select | keyed | 128 | 497,340 | 0.245 | 0.502 | - | - | 95 (85-100) | 0 | 36 |  |
| 8 | select | routed1 | 128 | 246,694 | 0.497 | 0.991 | 75 | - | 54 (48-60) | 0 | 19 |  |
| 8 | select | routed2 | 128 | 317,696 | 0.393 | 0.605 | 52 | 56 | 67 (61-73) | 0 | 24 |  |
| 8 | select | keyed | 256 | 504,827 | 0.489 | 1.033 | - | - | 100 (100-100) | 0 | 41 |  |
| 8 | select | routed1 | 256 | 314,854 | 0.787 | 1.179 | 87 | - | 74 (64-80) | 0 | 26 |  |
| 8 | select | routed2 | 256 | 409,449 | 0.537 | 1.771 | 64 | 68 | 90 (81-96) | 0 | 33 |  |
| 8 | select | keyed | 512 | 487,765 | 1.024 | 2.303 | - | - | 100 (100-100) | 0 | 47 |  |
| 8 | select | routed1 | 512 | 341,126 | 1.408 | 2.497 | 97 | - | 87 (80-92) | 0 | 33 |  |
| 8 | select | routed2 | 512 | 373,794 | 0.586 | 8.855 | 64 | 65 | 92 (84-100) | 0 | 37 |  |
| 8 | write | keyed | 128 | 59,381 | 1.972 | 3.111 | - | - | 52 (43-59) | 15 | 17 |  |
| 8 | write | routed1 | 128 | 39,976 | 3.116 | 4.264 | 54 | - | 39 (35-43) | 16 | 12 |  |
| 8 | write | routed2 | 128 | 42,900 | 2.938 | 3.830 | 32 | 31 | 42 (37-46) | 16 | 13 |  |
| 8 | write | keyed | 256 | 85,701 | 3.063 | 4.221 | - | - | 78 (76-79) | 6 | 27 |  |
| 8 | write | routed1 | 256 | 62,551 | 3.923 | 6.551 | 75 | - | 65 (61-69) | 10 | 20 |  |
| 8 | write | routed2 | 256 | 71,574 | 3.411 | 5.271 | 49 | 50 | 73 (68-77) | 8 | 23 |  |
| 8 | write | keyed | 512 | 103,722 | 4.852 | 7.639 | - | - | 96 (95-96) | 1 | 38 |  |
| 8 | write | routed1 | 512 | 68,795 | 4.438 | 7.705 | 79 | - | 73 (70-77) | 8 | 23 | 189 clients aborted |
| 8 | write | routed2 | 512 | 80,902 | 3.834 | 7.307 | 50 | 60 | 83 (79-87) | 5 | 28 | 180 clients aborted |
| 8 | tpcb | keyed | 128 | 40,512 | 3.039 | 4.331 | - | - | 68 (65-72) | 9 | 24 |  |
| 8 | tpcb | routed1 | 128 | 26,975 | 4.637 | 7.437 | 61 | - | 48 (42-52) | 14 | 14 |  |
| 8 | tpcb | routed2 | 128 | 30,210 | 4.094 | 5.887 | 37 | 38 | 53 (47-56) | 13 | 16 |  |
| 8 | tpcb | keyed | 256 | 55,673 | 4.529 | 6.943 | - | - | 93 (92-94) | 3 | 35 |  |
| 8 | tpcb | routed1 | 256 | 39,322 | 6.176 | 10.211 | 79 | - | 74 (68-78) | 8 | 23 |  |
| 8 | tpcb | routed2 | 256 | 45,952 | 5.168 | 10.447 | 54 | 55 | 82 (77-88) | 6 | 26 |  |
| 8 | tpcb | keyed | 512 | 56,919 | 8.794 | 14.929 | - | - | 99 (99-100) | 0 | 42 |  |
| 8 | tpcb | routed1 | 512 | 41,791 | 7.214 | 14.848 | 83 | - | 82 (75-90) | 6 | 25 | 182 clients aborted |
| 8 | tpcb | routed2 | 512 | 47,658 | 5.753 | 19.164 | 59 | 57 | 88 (82-94) | 5 | 29 | 189 clients aborted |

### N=8 at 384 clients, home at 600 connections

| N | workload | target | clients | tps | p50 ms | p99 ms | router 1 % | router 2 % | nodes % avg (min-max) | node iowait % max | client % | note |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | --- |
| 8 | write | keyed | 384 | 101,300 | 3.762 | 5.560 | - | - | 92 (91-93) | 2 | 34 |  |
| 8 | write | routed1 | 384 | 69,520 | 4.402 | 7.401 | 79 | - | 73 (69-77) | 8 | 24 | 64 clients aborted |
| 8 | write | routed2 | 384 | 79,852 | 3.767 | 6.626 | 47 | 62 | 81 (78-84) | 6 | 27 | 63 clients aborted |
| 8 | tpcb | keyed | 384 | 58,558 | 6.461 | 10.420 | - | - | 98 (98-99) | 0 | 39 |  |
| 8 | tpcb | routed1 | 384 | 41,909 | 7.048 | 12.596 | 82 | - | 80 (74-85) | 6 | 25 | 68 clients aborted |
| 8 | tpcb | routed2 | 384 | 45,214 | 5.237 | 11.352 | 60 | 52 | 82 (77-87) | 6 | 26 | 125 clients aborted |

### N=8, routed, home at 1200 connections

| N | workload | target | clients | tps | p50 ms | p99 ms | router 1 % | router 2 % | nodes % avg (min-max) | node iowait % max | client % | note |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | --- |
| 8 | write | routed1 | 384 | 72,536 | 4.973 | 8.644 | 84 | - | 79 (75-86) | 6 | 26 |  |
| 8 | write | routed2 | 384 | 82,473 | 4.056 | 12.579 | 57 | 58 | 86 (81-96) | 5 | 29 |  |
| 8 | write | routed1 | 512 | 76,799 | 5.828 | 18.091 | 88 | - | 86 (82-95) | 4 | 30 |  |
| 8 | write | routed2 | 512 | 85,626 | 4.697 | 19.412 | 60 | 60 | 91 (87-96) | 3 | 32 |  |
| 8 | tpcb | routed1 | 384 | 43,794 | 7.966 | 18.066 | 86 | - | 86 (79-91) | 5 | 27 |  |
| 8 | tpcb | routed2 | 384 | 45,303 | 5.656 | 43.005 | 56 | 56 | 86 (79-94) | 6 | 27 |  |
| 8 | tpcb | routed1 | 512 | 44,238 | 9.215 | 44.091 | 89 | - | 90 (82-95) | 4 | 31 |  |
| 8 | tpcb | routed2 | 512 | 43,244 | 5.497 | 65.191 | 57 | 53 | 84 (77-98) | 6 | 27 | 73 clients aborted |

### N=8, routed, pool mode transaction (home at 600)

| N | workload | target | clients | tps | p50 ms | p99 ms | router 1 % | router 2 % | nodes % avg (min-max) | node iowait % max | client % | note |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | --- |
| 8 | select | txn-routed1 | 256 | 294,065 | 0.845 | 1.259 | 86 | - | 70 (59-77) | 0 | 25 |  |
| 8 | select | txn-routed2 | 256 | 395,381 | 0.534 | 2.122 | 66 | 68 | 88 (78-98) | 0 | 32 |  |
| 8 | select | txn-routed1 | 512 | 314,391 | 1.563 | 2.640 | 94 | - | 81 (73-87) | 0 | 31 |  |
| 8 | select | txn-routed2 | 512 | 367,718 | 0.578 | 8.991 | 66 | 67 | 91 (82-100) | 0 | 35 |  |
| 8 | write | txn-routed1 | 256 | 61,398 | 3.994 | 6.385 | 75 | - | 64 (60-70) | 10 | 22 |  |
| 8 | write | txn-routed2 | 256 | 71,585 | 3.429 | 5.255 | 51 | 53 | 73 (68-78) | 8 | 23 |  |
| 8 | write | txn-routed1 | 512 | 60,786 | 4.009 | 6.593 | 75 | - | 65 (61-70) | 10 | 23 | 298 clients aborted |
| 8 | write | txn-routed2 | 512 | 70,051 | 3.424 | 5.281 | 50 | 54 | 73 (68-78) | 8 | 23 | 304 clients aborted |
| 8 | tpcb | txn-routed1 | 256 | 38,212 | 6.332 | 10.644 | 79 | - | 73 (66-80) | 8 | 24 |  |
| 8 | tpcb | txn-routed2 | 256 | 45,508 | 5.195 | 10.680 | 56 | 57 | 82 (76-87) | 6 | 26 |  |
| 8 | tpcb | txn-routed1 | 512 | 38,448 | 6.674 | 11.131 | 81 | - | 75 (68-79) | 8 | 23 | 245 clients aborted |
| 8 | tpcb | txn-routed2 | 512 | 46,544 | 5.482 | 12.402 | 60 | 57 | 85 (79-89) | 5 | 29 | 236 clients aborted |

### N=1 and N=2 throughput, first pass against rerun

| N | workload | target | clients | tps, first pass | tps, rerun | difference |
| ---: | --- | --- | ---: | ---: | ---: | ---: |
| 1 | select | direct | 32 | 72,329 | 73,191 | +1.2% |
| 1 | select | keyed | 32 | 56,544 | 54,609 | -3.4% |
| 1 | select | routed1 | 32 | 67,330 | 68,296 | +1.4% |
| 1 | select | direct | 64 | 68,140 | 68,642 | +0.7% |
| 1 | select | keyed | 64 | 53,934 | 53,596 | -0.6% |
| 1 | select | routed1 | 64 | 67,838 | 68,253 | +0.6% |
| 1 | select | direct | 128 | 61,773 | 61,575 | -0.3% |
| 1 | select | keyed | 128 | 43,894 | 45,080 | +2.7% |
| 1 | select | routed1 | 128 | 60,233 | 62,053 | +3.0% |
| 1 | select | direct | 256 | 47,813 | 47,896 | +0.2% |
| 1 | select | keyed | 256 | 36,473 | 37,427 | +2.6% |
| 1 | select | routed1 | 256 | 47,187 | 48,554 | +2.9% |
| 1 | write | direct | 32 | 10,425 | 10,375 | -0.5% |
| 1 | write | keyed | 32 | 10,287 | 10,354 | +0.7% |
| 1 | write | routed1 | 32 | 9,296 | 7,719 | -17.0% |
| 1 | write | direct | 64 | 12,414 | 10,181 | -18.0% |
| 1 | write | keyed | 64 | 11,867 | 9,692 | -18.3% |
| 1 | write | routed1 | 64 | 11,395 | 12,169 | +6.8% |
| 1 | write | direct | 128 | 12,640 | 11,133 | -11.9% |
| 1 | write | keyed | 128 | 10,548 | 9,854 | -6.6% |
| 1 | write | routed1 | 128 | 11,459 | 10,986 | -4.1% |
| 1 | write | direct | 256 | 11,346 | 11,608 | +2.3% |
| 1 | write | keyed | 256 | 9,921 | 9,984 | +0.6% |
| 1 | write | routed1 | 256 | 11,438 | 10,432 | -8.8% |
| 1 | tpcb | direct | 32 | 5,627 | 5,755 | +2.3% |
| 1 | tpcb | keyed | 32 | 6,750 | 6,302 | -6.6% |
| 1 | tpcb | routed1 | 32 | 6,454 | 6,434 | -0.3% |
| 1 | tpcb | direct | 64 | 7,512 | 7,644 | +1.8% |
| 1 | tpcb | keyed | 64 | 6,781 | 6,835 | +0.8% |
| 1 | tpcb | routed1 | 64 | 7,516 | 7,611 | +1.3% |
| 1 | tpcb | direct | 128 | 6,894 | 6,902 | +0.1% |
| 1 | tpcb | keyed | 128 | 6,203 | 6,275 | +1.2% |
| 1 | tpcb | routed1 | 128 | 6,922 | 6,971 | +0.7% |
| 1 | tpcb | direct | 256 | 5,990 | 6,191 | +3.4% |
| 1 | tpcb | keyed | 256 | 5,553 | 5,473 | -1.4% |
| 1 | tpcb | routed1 | 256 | 6,151 | 6,194 | +0.7% |
| 2 | select | keyed | 64 | 122,251 | 123,468 | +1.0% |
| 2 | select | routed1 | 64 | 125,195 | 126,142 | +0.8% |
| 2 | select | keyed | 128 | 116,514 | 117,707 | +1.0% |
| 2 | select | routed1 | 128 | 127,060 | 129,753 | +2.1% |
| 2 | select | keyed | 256 | 104,201 | 106,020 | +1.7% |
| 2 | select | routed1 | 256 | 100,597 | 102,569 | +2.0% |
| 2 | write | keyed | 64 | 21,371 | 21,280 | -0.4% |
| 2 | write | routed1 | 64 | 18,950 | 19,088 | +0.7% |
| 2 | write | keyed | 128 | 25,951 | 26,028 | +0.3% |
| 2 | write | routed1 | 128 | 25,419 | 25,734 | +1.2% |
| 2 | write | keyed | 256 | 25,266 | 25,345 | +0.3% |
| 2 | write | routed1 | 256 | 23,331 | 23,705 | +1.6% |
| 2 | tpcb | keyed | 64 | 14,023 | 14,278 | +1.8% |
| 2 | tpcb | routed1 | 64 | 12,415 | 12,397 | -0.1% |
| 2 | tpcb | keyed | 128 | 14,399 | 14,619 | +1.5% |
| 2 | tpcb | routed1 | 128 | 13,924 | 14,124 | +1.4% |
| 2 | tpcb | keyed | 256 | 13,413 | 13,700 | +2.1% |
| 2 | tpcb | routed1 | 256 | 12,266 | 12,151 | -0.9% |
