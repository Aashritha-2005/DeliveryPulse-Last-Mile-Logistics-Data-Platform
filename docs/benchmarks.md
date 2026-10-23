# Benchmarks

Measurements on this page come from recorded runs or explicitly identified artifact audits.
Commands, datasets, dates and hardware accompany the results. Derived arithmetic and planning
estimates are labelled; a pending section does not claim a completed measurement.

## Batch evidence and read-only artifact audit

Reference dataset: `seed-42_orders-50000_cfg-449e309b`, generated with 50,000 orders, seed 42,
generator version 2, on the MacBook Pro Apple Silicon reference environment. Existing outputs
from `make profile` and `make quality-report` are in [data_quality_report.md](data_quality_report.md).
On 2026-10-15, a read-only audit of the existing raw CSV files and manifest confirmed:

| Existing artifact | Result |
|---|---|
| Three `shift_events.csv` files, counted with `csv.DictReader` | 16,282 + 16,341 + 16,374 = **48,997 rows** |
| Manifest `config.defect_rates_per_10k` | **22 configured injected types** |
| Manifest `counts` and `cases` | **25 case types**, **4,395 cases**; includes two derived orphan types and one natural update type |
| Manifest SHA-256 | `cc1379190ed1927af2aac600499258b68900365008064581a4b87af59bf8543f` |
| Existing quality-report SHA-256 at audit | `3ecee59b639e0b124e157f490b7f357c589a10a7f36c1e9e3fc600c17a7cb274` |

The manifest was read from `data/ground_truth/seed-42_orders-50000_cfg-449e309b/defect_manifest.json`;
the CSV files were read from the corresponding `data/raw/` batch folders. No generation, load,
profile, report or benchmark command was rerun to obtain these counts. The report's source
totals are 50,484 order rows, 441,222 event rows, 48,997 shift rows and 833 driver rows. Its
reconciliation and quality tables preserve the per-source and per-batch evidence.

Other confirmed historical batch results on that dataset: 49,450 accepted orders,
439,822 delivery events, 1,355 rejected records, replay with identical business counts,
and integration tests for failed-batch recovery and late/orphan handling. The recorded
post-planner-fix load was 121.88 s; its three quality evaluations took 17.81, 21.01 and
27.18 s, with no blocking failure. Their sum, **66.00 s / 121.88 s ≈ 54.15%**, is derived
arithmetic, not a separate gate-disabled measurement. Generation and standalone Parquet
transformation are not included in that load time.

Historical PostgreSQL size was displayed as 686 MB by PostgreSQL's binary-size formatter;
raw output was 86,633,684 bytes. **686 × 1,048,576 / 86,633,684 ≈ 8.30** is a derived ratio
using a rounded historical database size, not the current database size. Later schema-level
measurement reported 686.9 MiB of project tables: raw 218.7, staging 214.9, audit 174.8 and
analytics 78.6 MiB. These are different measurements and are not interchangeable.

Confirmed lake export output is 118 objects, 1,093,033 rows and 57,501,391 bytes;
measured peak RSS changed from
923,090,944 to 653,492,224 bytes after streaming each object. No new export was run.

### Parquet transformation timing (2026-10-11)

User-run on the same dataset and hardware, with the Task 4 transformation code:

| Command | Result |
|---|---|
| `/usr/bin/time -l make transform` (first run) | `transformed 3 batches in 31.68 s`; 32.34 s real; maximum resident set size 1,076,723,712 bytes (about 1.0 GiB) |
| `make transform` (repeat) | `transformed 3 batches in 30.58 s`; per-source dispositions identical to the first run |

The README's "~30 s" is rounded from these two runs. Later changes to the transformation were
not re-timed on this dataset. The time does not include generation or the database load.

### Quality check registry

`src/quality/checks.py` defines **35 checks** in its `CHECKS` registry. Checks that run once per
source produce several results, so a full evaluation stores **50 results**: the "50 checks"
printed by `make quality` and by the demo. The whole-database run had zero blocking failures and
zero warnings. All 25 manifest case types are detected; see
[data_quality_report.md](data_quality_report.md).

## Test coverage

Measured on 2026-10-24 with `make coverage` on the reference MacBook Pro. It ran the unit suite,
then the PostgreSQL integration suite, with coverage combined over `src/`:

| Phase | Exact summary |
|---|---|
| Unit | `639 passed, 181 deselected in 27.02s`; `TOTAL 5918 1375 77%` |
| Unit + integration (combined) | `163 passed, 657 deselected in 174.51s (0:02:54)`; `TOTAL 5918 929 84%`; `Required test coverage of 80% reached. Total coverage: 84.30%` |

The previous recorded figure was 83.64% on 2026-10-15 (5,918 statements, 968 missed). The
optional S3, Spark, Kafka, MongoDB and Kubernetes suites, and `scripts/`, are outside this
figure.

## Isolated core demo

The wrapper uses 5,000 orders, seed 42, a separate database volume and `data/demo/`.

### Current configuration (500 drivers): timing of record

Run by the user on 2026-10-23 at commit `38426c0`, on the MacBook Pro Apple Silicon reference
environment, from a clean demo state. Command: `make demo`. Dataset
`seed-42_orders-5000_cfg-4e94aafb` (5,000 orders, **500 drivers**, seed 42). Evidence
`data/demo/run-20261023T192914223539Z.json` records `image_cached: true`,
`container_running: false`, `raw_files_present: false`, `database_before: "empty"` and
`status: "passed"`. The pull step was skipped because the pinned digest was cached, and
startup used `--pull never`, so the run made no registry call.

| Stage | Seconds |
|---|---:|
| Pull | skipped (digest cached) |
| Healthy startup | 5.841 |
| Generation | 1.190 |
| Transformation | 4.115 |
| Load | 17.312 |
| Reconciliation | 1.011 |
| Whole-database quality | 7.471 |
| Analytics | 0.323 |
| Dashboard query check | 0.340 |
| Total, including wrapper checks | **38.708** |

Stage times and the total are copied from the evidence file and rounded to milliseconds. The
user reported from the console that all 452 manifest cases reconciled and that quality ran
50 checks with zero blocking failures and zero warnings. Those counts are console output and
are not stored in the evidence file. Dependency installation, the image download and the
interactive dashboard are excluded. Evidence records 47,035,543,552 free bytes before and
46,750,527,488 after. That change includes unrelated host activity, so it is not a volume-size
measurement. No peak memory is claimed. The run was not a fresh-copy setup test, so there is
still no general "within 10 minutes" claim.

### Earlier configuration (83 drivers): superseded

The user's first run on 2026-10-15 used the default **83 drivers**, dataset
`seed-42_orders-5000_cfg-99b76f46`, on the MacBook Pro Apple Silicon reference environment.
Command: `make demo`, following `make demo-disk`. Its saved evidence
`data/demo/run-20261015T005538383606Z.json` records `image_cached: true`,
`database_before: "empty"`, no existing demo inputs and `status: "passed"`.

| Stage | Seconds |
|---|---:|
| Cached pull | 4.373 |
| Healthy startup | 6.082 |
| Generation | 0.926 |
| Transformation | 3.783 |
| Load | 15.086 |
| Reconciliation | 0.996 |
| Whole-database quality | 6.901 |
| Analytics | 0.347 |
| Dashboard query check | 0.347 |
| Total, including wrapper checks | **40.106** |

All three batches loaded; all 518 manifest cases reconciled. Each batch and the standalone
quality run had 50 checks, zero blocking failures and zero warnings. Seven dashboard queries
passed. Dependency installation and the interactive dashboard are excluded. Evidence records
21,303,513,088 free bytes before and 21,119,909,888 after; this host-space change includes
unrelated activity and is not an isolated volume-size measurement. No peak-memory measurement
is claimed. This timing measures a **different configuration**, whose simulation had 857
no-driver cancellations out of 5,000 orders (17.14%). It also included a 4.373 s cached pull
that the current wrapper skips. Do not compare it directly with the 38.708 s run.

An offline fleet comparison on 2026-10-15 held orders at 5,000 and seed at 42, using the existing
`GeneratorConfig.create`, `plan_drivers` and `simulate` functions without a database:

| Drivers | No-driver cancellations / all orders | Simulated late / delivered orders |
|---|---|---|
| 150 | 255 / 5,000 = 5.10% | 1,320 / 4,538 = 29.09% |
| 250 | 66 / 5,000 = 1.32% | 841 / 4,740 = 17.74% |
| 500 | 32 / 5,000 = 0.64% | 485 / 4,773 = 10.16% |

The demo now explicitly passes **`--drivers 500`**; defaults and the main 50,000-order
dataset are unchanged. Five cities, the 12 km city radius and 21-day schedule remain fixed,
so fleet size cannot simply shrink in proportion to demand while retaining similar coverage.
This is a chosen synthetic scenario, not fleet optimisation; more drivers lower utilisation.
Changing driver count also changes random-number consumption, so these are different seeded
worlds rather than a controlled comparison using identical orders. The late rates above are
simulation totals before validation, not daily dashboard measurements.

The normal generator entry point was also run with `--orders 5000 --seed 42 --drivers 500`
and raw/ground-truth directories under `data/demo/fleet-check-wug52g7d/`. The resulting dataset
is `seed-42_orders-5000_cfg-4e94aafb`; its manifest SHA-256 is
`9f3aee49cfb253cf7a98acf82e273ec779a23a18fb46d83e8a1da01bee7112b1`.
Read-only verification found zero problems across 452 cases. Generation emitted no capacity
warning and the trial files totalled 10,157,303 bytes. No database load was done for this trial;
the 2026-10-23 run above later loaded this same dataset end to end. Dependency installation
and PostgreSQL image download are excluded from both demo timings (image size about 672 MB,
user-reported; not measured transfer bytes). Download-inclusive timing is skipped, not pending.
No 60–90 s recording was made; it is optional before publishing, with the existing dashboard
screenshot used in the README.

## 1. Query tuning with indexes (metrics and analytics layer)

**Status:** run 1 (section 1.5) and run 2 (section 1.6) measured. Run 2 is the current
index set.

### 1.1 What is measured

`make explain` runs `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` as `deliverypulse_readonly` on:

| Query | Source | Index expected to matter |
|---|---|---|
| `peak_hours` | `sql/analytics.sql` (full period) | `orders_created_ts_idx` |
| `avg_delivery_time_by_city` | `sql/analytics.sql` (last 7 days) | `orders_delivered_ts_idx` |
| `top_bottom_drivers` | `sql/analytics.sql` (last 7 days) | `orders_delivered_ts_idx` |
| `driver_utilisation` | `sql/analytics.sql` (last 7 days) | `driver_performance_date_idx` |
| `order_event_summary` | `sql/analytics.sql` (last day) | `orders_delivered_ts_idx`, `delivery_events_order_id_idx` |
| `refresh_driver_performance` | the `SELECT` that `make load` runs to refresh one reporting day of `driver_performance` | `orders_delivered_ts_idx`, `driver_shift_events_ts_idx` (run 2; run 1 had `driver_shift_events_driver_ts_idx`, which was not used) |

What each index is for: `docs/data_model.md` section 7.

### 1.2 Protocol

1. Load the default dataset (`make load`); the database is otherwise idle.
2. `make indexes-drop`: drop the five indexes, then `ANALYZE` the tables, so the planner has
   fresh statistics for the "before" state too.
3. `make explain LABEL=before`.
4. `make indexes-create`: create the indexes, then `ANALYZE` again.
5. `make explain LABEL=after`.

Per query and label: **one unmeasured warm-up run, then three measured runs**. The reported
figure is the **median execution time** of the three. Planning time, shared-buffer hits/reads
and the scan nodes of the last run are recorded too. The warm-up means both labels are measured
with a warm cache. Neither is a cold-cache measurement.

Parameters are not typed by hand: they are derived from the loaded data (full period, last 7
days, or last day) and stored with each result. Both labels use the same data, so they get the
same parameters.

### 1.3 Outputs

- `docs/benchmarks/explain-before.json`, `explain-after.json`: header (label, dataset,
  PostgreSQL version, data period, runs) plus per-query median, all runs, planning time,
  buffers and scan nodes.
- `docs/benchmarks/explain-before.txt`, `explain-after.txt`: the full text plans.

### 1.4 Honesty rules

- Results are recorded **as measured, even when an index does not help** or makes a query
  slower.
- PostgreSQL may correctly choose a sequential scan, for example when a query reads most of a
  small table (the full-period `peak_hours`). An index is never forced (no `enable_seqscan =
  off`) to manufacture a speedup; the plan is explained instead.
- Timings vary from run to run; differences of a few percent are noise.

### 1.5 Results, run 1 (2026-10-12)

| Item | Value |
|---|---|
| Date | 2026-10-12, measured on the reference run |
| Hardware | MacBook Pro, Apple Silicon (`arm64`), macOS 26, 16 GiB RAM; Docker Desktop VM ~7.7 GiB; PostgreSQL container `mem_limit` 1 GiB, default settings (`shared_buffers` 128 MB) |
| PostgreSQL | 18.6 (`postgres:18.6-trixie`) |
| Dataset | `seed-42_orders-50000_cfg-449e309b`: 49,450 orders, 439,822 delivery events, 48,947 shift events, 828 drivers |
| Data period | 2026-09-10 to 2026-09-30 (reporting dates of `order_created_ts`) |
| Parameters | Full period for `peak_hours`; last 7 days (2026-09-24 to 2026-09-30) for `avg_delivery_time_by_city`, `top_bottom_drivers`, `driver_utilisation`; last day (2026-09-30) for `order_event_summary` and `refresh_driver_performance` |
| Commands | `make indexes-drop`, `make explain LABEL=before`, `make indexes-create`, `make explain LABEL=after` |

Median of 3 measured runs, after 1 warm-up. "Shared hit" counts buffer *accesses*: a page
visited ten times counts ten times.

| Query | Before (ms) | After (ms) | Change | Shared hit before → after | Scan after |
|---|---:|---:|---:|---|---|
| `peak_hours` | 27.061 | 25.617 | −5.3 % | 864 → 143 | Index Only Scan `orders_created_ts_idx` |
| `avg_delivery_time_by_city` | 8.988 | 7.112 | −20.9 % | 864 → 7,052 | Index Scan `orders_delivered_ts_idx` |
| `top_bottom_drivers` | 10.491 | 8.616 | −17.9 % | 864 → 7,052 | Index Scan `orders_delivered_ts_idx` |
| `driver_utilisation` | 2.376 | 1.921 | −19.2 % | 312 → 48 | Index Scan `driver_performance_date_idx` |
| `order_event_summary` | 75.619 | 17.028 | −77.5 % (4.4×) | 3,138 hit + 3,156 read → 10,070 hit | Index Scan `delivery_events_order_id_idx` + `orders_delivered_ts_idx` |
| `refresh_driver_performance` | 125.955 | 112.029 | −11.1 % | 2,149 → 2,529 | still **Seq Scan** `driver_shift_events` |

What the numbers mean:
- **`peak_hours`: little gain, as expected.** Its range is the whole loaded period, so it reads
  every order either way. The index-only scan touches far fewer buffers (143 instead of 864),
  because it reads only the narrow index, not the table. But most of the time goes to
  converting ~49,000 timestamps to Asia/Kolkata and grouping them, not to reading pages. A 5 %
  difference is close to run-to-run noise.
- **`avg_delivery_time_by_city` and `top_bottom_drivers`: faster despite more buffer hits.**
  - The partial index returns only delivered orders of the last 7 days: about a third of the
    delivered orders, without testing the status and date filter on every row.
  - Fetching those rows in index order visits the same table pages many times. Each visit is a
    counted hit, which is why hits rise from 864 (each page read once by the sequential scan)
    to 7,052.
  - All of those pages were already in shared buffers, so each extra hit is cheap. Fewer rows
    processed outweigh the extra page accesses: about 18–21 % faster.
- **`driver_utilisation`: a small absolute gain** (2.4 to 1.9 ms) on a 12,345-row table. The
  index selects 7 of 21 days.
- **`order_event_summary`: the largest gain (4.4×).**
  - Before, it scanned all 439,822 delivery events to find the events of one day's delivered
    orders.
  - After, it looks up only those orders' events by `order_id`.
  - Caveat: in the "before" run, 3,156 blocks were read from outside shared buffers (the OS
    cache or disk), even after the warm-up. Part of that difference is caching, not the plan
    alone.
- **`refresh_driver_performance`: the shift-event index was never used.**
  - The refresh filters shift events on `event_ts` only (the refreshed day ±1 day). An index
    leading with `driver_id` cannot serve that filter, so the plan kept a Seq Scan on
    `driver_shift_events`.
  - The 11 % gain comes from the orders indexes used in the other parts of the query.
  - The index was replaced by `driver_shift_events_ts_idx (event_ts)` and re-measured in run 2
    (section 1.6).

Run 2 overwrote the run-1 files `docs/benchmarks/explain-{before,after}.*` (same labels). The
run-1 numbers above were recorded from that run's console output.

### 1.6 Results, run 2 (2026-10-12): current index set

Same hardware, PostgreSQL version, dataset, data period and parameters as run 1. Differences:
- The shift-event index is `driver_shift_events_ts_idx (event_ts)`.
- `dim_date` had been rebuilt by `make metrics` beforehand (21 rows); no benchmarked query
  reads it.

Files: `docs/benchmarks/explain-before.{json,txt}` and `explain-after.{json,txt}`.

| Query | Before (ms) | After (ms) | Change | Runs after (ms) | Shared hit before → after | Scans after |
|---|---:|---:|---:|---|---|---|
| `peak_hours` | 27.223 | 26.333 | −3.3 % | 26.463, 26.333, 25.856 | 864 → 143 | Index Only Scan `orders_created_ts_idx` |
| `avg_delivery_time_by_city` | 8.068 | 7.225 | −10.4 % | 7.136, 7.225, 7.298 | 864 → 7,052 | Index Scan `orders_delivered_ts_idx` |
| `top_bottom_drivers` | 10.275 | 9.091 | −11.5 % | 9.074, 9.380, 9.091 | 864 → 7,052 | Index Scan `orders_delivered_ts_idx` |
| `driver_utilisation` | 2.412 | 1.995 | −17.3 % | 1.983, 1.995, 2.028 | 312 → 45 | Index Scan `driver_performance_date_idx` |
| `order_event_summary` | 72.487 | 25.323 | −65.1 % (2.9×) | 25.323, 45.787, 21.178 | 1,402 hit + 4,892 read → 10,070 hit | Index Scan `delivery_events_order_id_idx`, Index Scan `orders_delivered_ts_idx` |
| `refresh_driver_performance` | 118.543 | 100.405 | −15.3 % | 100.405, 99.947, 100.745 | 2,149 → 2,155 | **Index Scan `driver_shift_events_ts_idx`**, Index Scan `orders_delivered_ts_idx` |

"Before" scans (all indexes dropped): Seq Scan on `orders` for the first three queries; Seq
Scan on `driver_performance`; Seq Scans on `delivery_events` and `orders` for
`order_event_summary`; Seq Scans on `driver_shift_events` and `orders` for the refresh.

What run 2 shows:
- **The replacement shift-event index is used.** The refresh now reads `driver_shift_events`
  through `driver_shift_events_ts_idx`, for the refreshed day ±1 day, instead of a Seq Scan.
  - The whole refresh SELECT went from 118.5 to 100.4 ms (−15.3 %), against −11.1 % in run 1
    with the unused `(driver_id, event_ts)` index.
  - Shared hits barely changed (2,149 → 2,155). So the gain comes from processing fewer rows,
    not from touching fewer buffers.
  - The index is kept.
- **`order_event_summary` varied a lot in the "after" runs** (21.2 to 45.8 ms). The median of
  25.3 ms is higher than run 1's 17.0 ms. One slow run among three is noise on a laptop. It is
  reported as measured, not re-run until it looks better.
  - The "before" run again read thousands of blocks from outside shared buffers (4,892), so
    part of the 2.9× is caching, as in run 1.
- **The other four queries repeat run 1's pattern** with similar sizes (−3 % to −17 %):
  - `peak_hours` reads the whole period either way.
  - The two 7-day order queries are faster despite more buffer hits.
  - `driver_utilisation` touches 45 instead of 312 buffers.
- **Across runs 1 and 2,** the same query differs by up to about 10 % in the "before" state
  (for example `avg_delivery_time_by_city` 8.988 vs 8.068 ms). Differences smaller than that
  are not meaningful.

### 1.7 Load time with the incremental metric refresh (2026-10-12)

`/usr/bin/time -l make load` on a freshly reset database, same dataset and machine as above:

| Run | Real time | Batch 1 | Batch 2 | Batch 3 | Max resident set size |
|---|---:|---:|---:|---:|---:|
| Loader only (no metric refresh, no analytics indexes) | 53.26 s | 16.06 s | 18.87 s | 16.93 s | 602,423,296 bytes |
| With the metrics layer (refresh in each batch transaction, analytics indexes present) | 55.80 s | 17.12 s | 19.14 s | 17.93 s | 620,953,600 bytes |
| Difference | +2.54 s (+4.8 %) | +1.06 s | +0.27 s | +1.00 s | +18,530,304 bytes (~17.7 MiB) |

- The difference includes both the metric refresh and maintaining the five analytics indexes
  during the load (the loader-only run had neither). This run cannot separate the two.
- A full rebuild (`make metrics`) of all 21 dates gave the same row counts as the incremental
  path: `daily_delivery_metrics` 105, `driver_performance` 12,345. In run 1, `dim_date` had 23
  rows: the incremental path had added 2026-09-09 and 2026-10-01 (shift dates ±1 day), and the
  rebuild did not remove them. Fixed after run 1: both paths now set `dim_date` to the data
  range. Database size was 698 MB.

## 2. Scale benchmark: pandas vs PySpark

### 2.1 What is measured

One defined workload, implemented twice and proved to give identical results:

* **Cleaning.** Orders: required fields, parsable contract timestamps, a known status, and for
  delivered orders a pickup and a delivery time with `delivery_ts >= pickup_ts`; then one row per
  `order_id` (the newest `source_updated_at`). Delivery events: required fields, a parsable
  timestamp, a known event type, and for GPS pings coordinates in range; then one row per
  `event_id`, and GPS pings flagged per driver when the interval is not positive or the Haversine
  speed exceeds 120 km/h.
* **Aggregation.** Orders, delivered, late, cancelled and delivery seconds per reporting date
  (Asia/Kolkata) and city; events, GPS pings and flagged pings per reporting date.
* **Read layouts.** The typed delivery events are written once as a single file and once
  partitioned by date; both engines then count one week's events and GPS pings on each copy.

The exact rules are in `src/bench/spec.py`; the engines are `src/bench/pandas_engine.py`
(vectorised pandas) and `src/bench/spark_engine.py` (PySpark in local mode).

**This workload is not the production pipeline.** It is a vectorised subset of the cleaning
rules, without the stateful disposition engine (pending, stale, cross-batch state), which stays
`src/transformations`. The two engines are proved identical **to each other**
(`make test-spark`, and `results equal` in the table), not to the production engine. The
benchmark answers "where does Spark overtake pandas on this workload", not "is the pipeline
correct".

### 2.2 Protocol

* **Data.** Shards of 50,000 orders, each a generator run with its own seed (shard 1 is the
  default seed 42), IDs prefixed per shard so they stay unique. Converted to Parquet with the
  contract columns as text; the raw files are deleted right after each conversion. A size of
  n x 50,000 orders is shards 1..n, so the smaller sizes are prefixes of the larger ones.
* **Runs.** Each (engine, task, size) runs in its own process: one warm-up, then three measured
  runs; the median is reported. Wall time covers the whole process, including the JVM start,
  which is reported separately for Spark.
* **Memory.** The resident memory of the run's whole process tree (Python plus the Spark JVM) is
  sampled every 0.2 s with psutil. A run above **4 GiB** is killed and recorded as stopped; that
  engine is not attempted at larger sizes, while the other continues.
* **Disk.** Everything the benchmark writes lives under `data/bench/`, including Spark's
  `spark.local.dir` and the JVM's temporary directory. The total must stay within **4 GiB**
  (a higher budget is refused) and at least 10 GiB must remain free on the volume. Before each
  size and each shard the harness projects the size from measured per-shard sizes and stops
  before creating anything that would not fit. Spark's scratch space is deleted after every run.
* **Sizes.** 50k, 250k, 500k, 1M, 2M. **5M was not run**: 100 shards project to about 5.5 GiB,
  above the 4 GiB disk budget. It was excluded by the budget, not attempted and failed.
* **Measured shard sizes** (allocated bytes on disk, one shard): input Parquet 17,301,504; read
  copies 5,054,464; raw files during generation 85,183,532, deleted afterwards. Spark's spill
  measured at 2.04 x the input.
* **Reproducing one number.** The whole run is one command, shown in the generated line below.
  `docs/benchmarks/scale-<date>.json` additionally records, per engine and task, the equivalent
  standalone command (for example
  `uv run python -m src.bench.engine --engine spark --task workload --shards 40 --output data/bench/tmp/result.json`),
  every individual run's wall time next to the median, the peak memory and spill, and the full
  result the engines were compared on. The dataset of a number is its size column: n x 50,000
  orders is shards 1..n, each a generator run with seed 41 + k.

### 2.3 Honesty rules

The numbers below come from one machine and one run of `make benchmark`; the exact command is in
the generated line. Every number is measured, nothing is extrapolated. A stopped run is reported
with its reason, never hidden or replaced by an estimate. pandas and Spark are both run on the
same shards, in the same process-per-run way, with the same warm-up and repeat protocol.

### 2.4 Results

<!-- scale-benchmark:start -->
Generated by `make benchmark BENCH_LARGEST=2m BENCH_RUNS=3` on 2026-10-14. Machine: Mac14,9, 10 cores, 16 GiB RAM, macOS 26.6.2, Python 3.12.10, JDK 21.0.12.1 (Temurin-21.0.12.1+1), numpy 2.5.4, pandas 3.0.6, psutil 7.2.2, py4j 0.10.9.9, pyarrow 25.0.1, pyspark 4.2.0. Limits: memory 4 GiB per run (process tree), disk 4 GiB under `data/bench/`.

| Size | Orders (input rows) | Parquet on disk | Engine | Median wall s | Median engine s | Spark start s | Peak memory | Peak spill/temp | Status |
|---|---|---|---|---|---|---|---|---|---|
| 50k | 50,484 | 16.5 MiB | pandas | 2.502 | 1.922 |  | 530.5 MiB | 0.0 MiB | completed |
| 50k | 50,484 | 16.5 MiB | spark | 16.517 | 11.641 | 3.688 | 1,359.0 MiB | 33.7 MiB | completed |
| 50k | | | both | | | | | | results equal: True |
| 250k | 252,400 | 84.6 MiB | pandas | 11.021 | 10.279 |  | 1,629.1 MiB | 0.0 MiB | completed |
| 250k | 252,400 | 84.6 MiB | spark | 24.973 | 20.121 | 3.622 | 2,122.8 MiB | 166.3 MiB | completed |
| 250k | | | both | | | | | | results equal: True |
| 500k | 504,781 | 170.3 MiB | pandas | 23.757 | 23.072 |  | 1,999.9 MiB | 0.0 MiB | completed |
| 500k | 504,781 | 170.3 MiB | spark | 38.983 | 33.415 | 4.607 | 2,420.8 MiB | 366.3 MiB | completed |
| 500k | | | both | | | | | | results equal: True |
| 1m | 1,009,557 | 338.5 MiB | pandas | 43.374 | 42.611 |  | 4,024.3 MiB | 0.0 MiB | completed |
| 1m | 1,009,557 | 338.5 MiB | spark | 40.919 | 35.71 | 4.014 | 2,649.0 MiB | 865.8 MiB | completed |
| 1m | | | both | | | | | | results equal: True |
| 2m | | 675.9 MiB | pandas | | | | | | stopped: memory (killed at 4.02 GiB (limit 4 GiB)) |
| 2m | 2,019,137 | 675.9 MiB | spark | 63.169 | 57.502 | 3.98 | 2,665.6 MiB | 1,747.1 MiB | completed |

Read benchmark at 2m: one week of the 21 generated days, events and GPS pings per day. Single file 96.6 MiB, partitioned 104.2 MiB.

| Engine | Layout | Median wall s | Median engine s | Peak memory | Status |
|---|---|---|---|---|---|
| pandas | single | 0.851 | 0.323 | 647.1 MiB | completed |
| pandas | partitioned | 1.28 | 0.684 | 880.0 MiB | completed |
| spark | single | 8.473 | 4.097 | 1,137.5 MiB | completed |
| spark | partitioned | 8.442 | 3.818 | 1,050.9 MiB | completed |

Stopping point: every planned size completed.
<!-- scale-benchmark:end -->

### 2.5 Conclusion

**The crossover is at about 1,000,000 orders.** Below it pandas wins by a wide margin: 6.6x at
50,000 orders (2.502 s against 16.517 s) and 1.6x at 500,000 (23.757 s against 38.983 s). At
1,000,000 orders Spark is ahead for the first time, 40.919 s against 43.374 s, a 6 % difference
that is small but consistent across the three measured runs. At 2,000,000 orders only Spark
finishes, in 63.169 s.

**The observed limit is memory.** pandas holds the whole input in memory: 530.5 MiB at
50,000 orders, 1,999.9 MiB at 500,000, 4,024.3 MiB at 1,000,000. At 2,000,000 it was **killed at 4.02 GiB
by the benchmark's own limit**, in the warm-up run. Spark's measured peak ranges from
1,359.0 to 2,665.6 MiB (1.33 to 2.60 GiB) across all five sizes while spilling to disk:
its scratch space grew from 33.7 to 1,747.1 MiB (1.71 GiB), 2.585x the input Parquet at 2M.
That is the engineering trade-off this benchmark shows:
Spark pays a fixed cost of about 4 s of JVM start plus a slower small-data path, and in return
its memory stays below the guard at the measured sizes while the data grows.

**What this means for the pipeline.** The production dataset is 50,000 orders, where pandas is
6.6x faster than Spark and the whole load takes about 2 minutes. Spark would be the wrong choice
today. On this benchmark workload Spark first leads at roughly a million orders, and pandas
exceeds the memory guard at two million. The production engine and machines with less RAM
would need separate measurements before choosing an engine.

**Partitioning did not help here, and that is reported as measured.** Reading one week out of
21 days (5,882,587 rows of 17.6 million): pandas 0.851 s on the single file against 1.28 s on
the partitioned dataset, so the partitioned layout was **50 % slower**; Spark 8.473 s against
8.442 s, no difference beyond noise. The reasons: the single file carries Parquet row-group
statistics on the date column, so the reader already skips most row groups, while the
partitioned layout splits the same data into 840 small files (40 shards x 21 days) whose
per-file open and metadata cost may outweigh the pruning. This is an explanation of the
observed result, not a separately measured cause. Partitioning may pay off with
selective queries over far more days, or with files large enough that per-file overhead
disappears. No layout was tuned to produce a favourable number.

**Sizes not run.** 5,000,000 orders (100 shards) was **not attempted**, because it projects to
about 5.5 GiB under `data/bench/` using the initial 2.04 spill factor, against the 4 GiB disk
budget (about 6.34 GiB using the final measured 2.585 factor). It is excluded by the budget;
it did not fail. The largest size actually measured is **2,000,000 orders**. The JSON records
a projection of 2,764,412,886 bytes for that size and a measured spill factor of 2.585;
the initial 50k spill factor was 2.04. Intermediate sizes above 2M were not tested.

**Both engines agree.** At every size where both finished (50k, 250k, 500k, 1M) the two engines
returned identical counts and identical daily aggregates, row for row; `make test-spark` checks
the same thing on the fixture dataset in CI.

## 3. Kafka streaming (2026-10-15)

Measured on macOS 26.6.2, Apple Silicon arm64, 10 physical/logical CPU cores, 16 GiB host RAM;
Kafka 4.3.1 and PostgreSQL in Docker Desktop, Python 3.12.10, confluent-kafka 2.15.1 on the host.
One session, one initial pass and one separate replay per source slice; no repeated-run confidence
intervals. Exact outputs and resource snapshots: [stream-2026-10-15.json](benchmarks/stream-2026-10-15.json).

Commands, sequentially:

```bash
make kafka-demo COUNT=500 RATE=500 TRIAL=0 MALFORMED=5
make kafka-demo COUNT=100000 RATE=500 TRIAL=1 MALFORMED=0
make kafka-demo COUNT=100000 RATE=2000 TRIAL=2 MALFORMED=0
make kafka-demo COUNT=100000 RATE=0 TRIAL=3 MALFORMED=0
```

Source: `data/raw/seed-42_orders-50000_cfg-449e309b`. Correctness uses 250 order versions and
250 pings plus five malformed messages. Each larger trial uses a distinct 10,000-order-version /
90,000-ping slice. Selection deterministically filters invalid and repeated source identities;
this is a controlled stream workload, not the complete messy batch pipeline. No source files
were rewritten. Consumer starts before the producer. Each pass has its own group and snapshots
current topic offsets. Source selection is excluded from timing; consumer connection setup,
processing and synchronous commits are included. Batches are at most 500 messages or 250 ms.
Latency is host send timestamp to return from committed database work, including queueing;
DLQ messages are excluded from latency. Initial and replay rates must not be conflated.

| Trial | Pass | Records | Inserted | Duplicates | DLQ | Consumer records/s | Producer records/s | Seconds | p50 / p95 / max latency (ms) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| 500 + 5 malformed | initial | 505 | 500 | 0 | 5 | 484.72 | 496.97 | 1.042 | 149.808 / 269.941 / 293.720 |
| 500 + 5 malformed | replay | 500 | 0 | 500 | 0 | 443.76 | 500.16 | 1.127 | 244.110 / 434.888 / 482.595 |
| 100k at 500/s | initial | 100000 | 100000 | 0 | 0 | 499.95 | 499.99 | 200.021 | 164.127 / 287.816 / 755.431 |
| 100k at 500/s | replay | 100000 | 0 | 100000 | 0 | 499.78 | 499.98 | 200.087 | 306.226 / 529.346 / 1081.231 |
| 100k at 2000/s | initial | 100000 | 100000 | 0 | 0 | 1998.84 | 1999.80 | 50.029 | 176.712 / 291.528 / 743.923 |
| 100k at 2000/s | replay | 100000 | 0 | 100000 | 0 | 1863.56 | 1999.96 | 53.661 | 1876.476 / 3696.625 / 3920.232 |
| 100k unthrottled | initial | 100000 | 100000 | 0 | 0 | 11580.93 | 53633.62 | 8.635 | 4186.620 / 6374.134 / 6758.851 |
| 100k unthrottled | replay | 100000 | 0 | 100000 | 0 | 1922.44 | 94705.38 | 52.017 | 25483.584 / 48573.778 / 51031.073 |


Landing row counts progressed **0 → 500 → 100,500 → 200,500 → 300,500**; every replay inserted
zero rows. All valid messages were acknowledged by the producer and reconciled at the consumer.
The correctness pass sent exactly five malformed messages to the DLQ.

**Interpretation.** Insert throughput kept up with both throttled rates. Unthrottled inserts
reached 11,580.93 records/s, but replay reached only 1,922.44 records/s. Replay performs an extra
content-hash lookup for each conflicting primary key; its sequential lookups are a plausible
bottleneck, not a separately isolated causal measurement. Unthrottled replay accumulated a
backlog: p95 latency was 48,573.778 ms. This is not a low-latency guarantee at arbitrary rates.

**Resources.** `make kafka-stats` recorded broker idle memory 362.5 MiB and a largest sampled
value of 507.3 MiB. Sparse snapshots are not a continuously measured peak. PostgreSQL's largest
sample was 185.3 MiB. Kafka data directory usage (`du -sk`, including internal topics) grew
from 184 KiB to 213,464 KiB after the runs; the largest sample was 213,480 KiB. Segment retention
and metadata changes mean disk use need not be monotonic. Configured limits are 1 GiB for the
broker and 256 MiB for the topic initializer; they are not usage measurements.

The combined host demo process (source selection, producer and consumer) had measured peak RSS
121.047 MiB at 2,000/s and 170.531 MiB unthrottled. Peak-RSS instrumentation was added after the
500/s trial; that trial has only instantaneous RSS samples of 16.52 and 91.02 MiB for the
processes with the demo module in their arguments (process roles were not captured).
No consumer-only peak RSS was measured. Do not compare these scopes as equivalent.
A resource probe initially encountered an inaccessible system process; the later probe ignored
inaccessible processes and reported only the running demo processes.

**Limits and estimates.** Single broker, one partition per input, one consumer, local plaintext,
no analytics updates, no S3 writes, stateless validation only, at-least-once DLQ. The source
selection and latency arrays are bounded by the demo count. For planning only, reserve **2 GiB
of additional disk [estimate]** for Kafka/image/database growth and **1–2 GiB combined running
memory [estimate]** for the local core + stream workload. These are not hard caps or production
capacity predictions. Actual image storage was not measured. No Spark or Airflow workload ran
during the stream trials; lightweight editing/lint and resource probes occurred during the
runs. CI configuration is prepared; its new Kafka job has not yet been run on GitHub.

## 4. MongoDB raw event store (2026-10-15)

One session on the same 16 GiB Apple Silicon machine: macOS 26.6.2 arm64, 10 logical CPUs.
MongoDB Community 8.0.32, PyMongo 4.18.2 and dnspython 2.8.0; the image tag and confirmed
multi-platform digest are in `docker-compose.mongo.yml`. Dataset:
`seed-42_orders-50000_cfg-449e309b`, three batches, **441,222 raw delivery-event rows**, one
document per raw row.
Full machine-readable results and plans: [mongo-2026-10-15.json](benchmarks/mongo-2026-10-15.json).

Commands, in order, with the user-started server already healthy:

```bash
make test-mongo
make mongo-stats
make mongo-load
make mongo-load
make mongo-stats
make mongo-query
```

An additional `mongo-stats` sample was taken during the initial load. Initial live tests and
the baseline probe overlapped; only the separate MongoDB test collection received fixture rows.

| Load | Input rows | Inserted | Duplicate | Modified | Elapsed seconds | Host process peak RSS (MiB) |
|---|---:|---:|---:|---:|---:|---:|
| Initial | 441,222 | 441,222 | 0 | 0 | 16.618 | 98.063 |
| Replay | 441,222 | 0 | 441,222 | 0 | 12.296 | 89.891 |

Elapsed load time includes source-manifest verification, parsing, chunk reads/writes and
post-load source checks; it excludes process startup and client construction. Peak RSS is
the operating system's high-water measurement for the host loader process, including imports.
Replay overlapped PostgreSQL integration checks and unit verification. These are one-session
measurements, not isolated repeated throughput benchmarks. Replay never updates ingestion times.

### Query comparison

Question: all raw events for `ORD-0000001`, retaining duplicates, ordered by text timestamp,
batch ID, source filename and source row. Both return **8 equal rows**, with the same projection,
including provenance and extra columns. PostgreSQL uses its ETL role in a read-only transaction.
The read-only dashboard role's grants were not widened. No PostgreSQL index or schema changed.

One warm-up then five measured client queries per store, alternating execution order. Timings
include fetching/materializing results. Equality checks and the separately collected execution
plans are outside the timed sections. These queries ran after the other verification commands
completed and before coverage started.

| Store | Median client query time | Observed plan |
|---|---:|---|
| MongoDB | 1.678 ms | `order_timeline` IXSCAN, FETCH and projection; 8 keys and 8 documents examined |
| PostgreSQL | 26.325 ms | Parallel sequential scan, gather and sort; separate EXPLAIN execution time 24.506 ms |

**Conclusion:** MongoDB's indexed lookup was faster for this query against PostgreSQL's
unchanged raw table, which has no equivalent order lookup index. This compares the implemented
access paths, not the inherent speed of the engines. One order, one session and warm reads do
not establish general performance. A PostgreSQL index comparison would require separate scope.
Mongo's driver and batch indexes were verified to exist but their performance was not benchmarked.
Captured explain JSON canonicalizes object-key order; the artifact separately records the
actual ordered sort keys.

### Memory and disk

| Measurement | Observed value |
|---|---:|
| MongoDB baseline memory sample | 121.3 MiB |
| Memory sample during load | 433.8 MiB |
| Memory sample after replay | 487 MiB |
| Server memory limit / WiredTiger cache | 768 MiB / 0.25 GiB (configured limits) |
| Final collection count | 441,222 |
| Logical BSON size | 271,528,534 bytes |
| Allocated collection storage | 72,101,888 bytes |
| Total allocated index storage | 151,064,576 bytes |
| Data directory, baseline / final | 206,576 / 424,604 KiB |
| Config directory | 4 KiB |

Final index sizes: `_id_` 37,937,152 bytes; `batch_lookup` 2,088,960 bytes;
`driver_timeline` 56,692,736 bytes; `order_timeline` 54,345,728 bytes. The data directory also
includes journal, metadata and small test fixtures, so it is larger than collection plus index
storage. Post-replay container-filesystem free space was 977,297,900 KiB; that virtual filesystem
figure is distinct from physical host free space. The loader separately enforces a 10 GiB host
free-space floor. Image storage and bootstrap memory peak were not measured. Memory samples
are not a continuous peak.

**Limitations and estimates:** standalone local server, no replication, sharding, transactions
across chunks or production durability claim. One loader at a time; concurrent insert races
fail and require an identical replay. No analytics changes, Kafka consumption or lake writes.
Raw storage is duplicated across stores. For future planning, reserve **1–2 GiB additional
disk [estimate, excluding image storage]**; the server's **768 MiB** limit is configuration,
not a promise of workload capacity. Keep heavy profiles separate. CI configuration is locally
validated; the new MongoDB job still requires a real GitHub run.

## Local Kubernetes: measured local run (2026-10-15)

The approved demo uses the same 50,000-order seed-42 dataset and existing Compose PostgreSQL.
Record the host hardware and date with the user-run results. `make k8s-up` records cluster
creation seconds; `make k8s-build` records image bytes and ID; each Job records Kubernetes
start/completion timestamps, observed elapsed seconds and periodic node memory samples.
`make k8s-stats` gathers those records with host free bytes and node filesystem usage. Node
samples include Kubernetes infrastructure; they are not application-only or continuous peaks.
Evidence stays in ignored `data/k8s/` until reviewed and copied here as aggregate results.

Record first/replay load summaries, quality results, and analytics count/content-hash equality.
Successful Job logs are saved before their pods and temporary files are removed. The wrong-
password test uses a separate temporary Secret and must fail without disclosing credentials.
Hardware: MacBook Pro, Apple Silicon arm64, macOS 26, 16 GiB host RAM; Docker Desktop
reported `memory_bytes=8319504384` (stats displays 7.748 GiB). kind v0.33.0, Kubernetes
v1.37.0, Homebrew kubectl v1.37.1. PostgreSQL ran separately in Compose; other heavy profiles
were stopped. The node and Python base use the approved index pins documented in operations.
Commands: `make k8s-build`, `make k8s-up`, `make k8s-load`, `make k8s-secret`, `make k8s-run`,
`make db-counts`, `make k8s-replay`, `make db-counts`, `make test-k8s`, `make k8s-status`,
`make k8s-stats`. All were run with Docker Desktop's binary directory prefixed to PATH.

| Measurement | Result |
|---|---|
| Pipeline image size | 764,439,884 bytes (about 0.71 GiB), tag `deliverypulse-pipeline:3597741be91902099a5e` |
| Cluster creation | 86.07803262514062 s (86.08 s rounded) |
| First pipeline Job | 133 s Kubernetes start-to-completion; 134.1364009170793 s observed |
| Separate replay Job | 126 s Kubernetes start-to-completion; 128.34594708308578 s observed |
| Live-suite replay Job | 119 s Kubernetes start-to-completion; 119.851827459177 s observed |
| Pipeline entry point, excluding generator | 114.60537142699468 / 107.6236397160028 / 102.62140479699883 s, respectively |
| Generator init | 12.49 / 11.41 / 10.50 s, respectively |
| Largest sampled node memory, per pipeline Job | 1.91 / 2.109 / 1.919 GiB, respectively (12 / 11 / 11 samples) |
| Post-test node memory sample | `914MiB / 7.748GiB` |
| Physical host free after measurements | 19,607,977,984 bytes; checkpoint 18.261 GiB |
| Node filesystem after measurements | `overlay 1055761844 19635752 982422668 2% /` (1K-blocks) |
| Live suite | `2 passed, 787 deselected in 158.10s (0:02:38)` |

The image load check compares all 10 ordered rootfs layer digests. Docker's host image ID is
an OCI index digest and differs from the node's CRI config ID; they cannot be compared directly.
Docker inspect exposes no separate config digest here. Both layer arrays matched, with no
rebuild. Tags alone are never accepted as identity.

All three pipeline runs verified source identity before writes, loaded **0 batches** and
skipped **3** already loaded batches. The first run's load took 0.07 s; the two replay loads
took 0.06 s each. Quality results: **50 checks, 0 blocking failures, 0 warnings** in 58.49,
56.95 and 54.99 s, respectively. Both standalone runs and the live-suite replay preserved
whole-row hashes and counts for all eight analytics tables:

| Analytics table | Before and after |
|---|---:|
| orders | 49,450 |
| delivery_events | 439,822 |
| drivers | 828 |
| driver_shift_events | 48,947 |
| daily_delivery_metrics | 105 |
| driver_performance | 12,345 |
| dim_city | 5 |
| dim_date | 21 |

The explicit before/after `make db-counts` checks around the separate replay also preserved
all raw/staging counts and database size (896 MB). Quality audit rows increased as expected:
quality_runs 4 → 5; quality_results 200 → 250. The wrong-credential test failed cleanly with
`ERROR: Kubernetes pipeline failed; check connectivity, credentials, dataset and gate` and
cleaned up its own temporary Secret/Job. Afterwards status returned `secret/deliverypulse-db`;
the real Secret remained, and the CronJob stayed suspended.

Physical host free checkpoints: cluster create **20.170 → 19.361 GiB**; first pipeline run
**18.250 → 18.246 GiB**; replay **18.246 → 18.253 GiB**; measurement stage
**18.261 → 18.261 GiB**. The earlier build-cache cleanup reported 4.348GB internally but
host free space remained 16.121 GiB immediately before/after; it is not counted as host
space regained. Docker VM filesystem availability is distinct from physical host space.

Limitations: one local session, an already populated database (this verifies replay, not a
fresh load), no concurrent writers, suspended rather than scheduled CronJob execution, and
no application-only or continuous memory peak. Node samples include infrastructure/workload
but exclude Compose PostgreSQL. Observed Job durations include polling and log collection;
controller durations come from Job timestamps. Host disk changes include unrelated activity
and delayed Docker reclamation. Approved host image removal and cluster deletion subsequently
exited 0: host-image cleanup checkpoints were 18.265 → 18.320 GiB; cluster deletion checkpoints
were 18.821 → 18.820 GiB. Compose PostgreSQL and its volumes were left running/intact. The
CI pipeline image build/scan passed on GitHub according to the user's 2026-10-15 confirmation.
A second, independent from-scratch README reproduction has not been performed and is deferred
to the pre-publishing check to conserve laptop disk space.
Disk and memory planning estimates are in the [runbook](operations.md#local-kubernetes-staged-runbook).

