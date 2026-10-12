# Benchmarks

Every number on this page comes from a real run on the reference machine, with the command,
dataset, date and hardware recorded next to it. Nothing is estimated. A section without numbers
has not been measured yet.

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
