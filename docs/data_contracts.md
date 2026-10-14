# Data contracts

## Streaming landing contract

Kafka orders and driver pings use a JSON envelope with `kind`, `fields`, `dataset`, `file`
and positive source `row`. `fields` preserves the source values; unknown scalar fields are
retained. Required source columns must be present. Order validation and basic event/GPS
coordinate validation reuse the batch rule functions. The pings topic accepts `gps_ping`
only. Non-JSON messages, invalid envelopes, wrong-topic messages, missing/invalid send-time
headers and failed stateless rules go to the DLQ with a reason and base64 original bytes.

Valid records land only in `raw.stream_events`. Order identity is order ID plus canonical
UTC source update timestamp; ping identity is event ID. Replays with identical canonical
source content are duplicates; conflicting content for an existing identity goes to the DLQ.
Transport metadata and source location do not change identity or content hash. No analytics
tables or S3 objects are updated. Different order versions remain immutable landing records.

The landing path does not apply parent waiting, stale-version selection or historical GPS
speed checks. Those remain batch-pipeline rules; landing acceptance is not analytics acceptance.
Each batch satisfies `consumed = inserted + duplicate + dead_letter`. Database commit comes
before DLQ acknowledgment, which comes before synchronous offset commit. Failure between
these steps can cause redelivery, handled by the landing primary key and content comparison.

The contract between the synthetic source (`src/ingestion/generate_data.py`) and the pipeline
(transform, load and quality checks). The constants live in `src/contract.py` (re-exported by
`src/ingestion/generator_config.py` for the generator). A unit test checks
that the accepted values below equal the CHECK constraints in `sql/schema.sql`.

All data is synthetic. City centres are public reference points; every other coordinate is
random. No real people, addresses, phone numbers or GPS traces are used.

## 1. Delivery layout

```
data/raw/<dataset>/batch-<k>/      the immutable source; the pipeline reads only this tree
    drivers.csv
    orders.csv
    delivery_events.jsonl
    shift_events.csv
    batch.json                     batch identity and file inventory (no defect information)
data/ground_truth/<dataset>/
    defect_manifest.json           ground truth for tests and reports; never read by the pipeline
```

- **Dataset name:** `seed-<seed>_orders-<orders>_cfg-<first 8 hex of sha256(canonical config)>`.
- **Batch order:** batches are processed in ascending `<k>`. Batch *k* covers source changes
  in `[window_start, window_end_exclusive)` (both in `batch.json`).
- **Hidden entries:** the pipeline ignores every dot-prefixed file or folder in `data/raw/` and
  below (for example `.gitkeep`, `.DS_Store` and `.generate-tmp-*` folders left by an interrupted
  run).
- **Immutability:** raw files are never rewritten. They are written read-only (mode 0444).
  Re-running the generator with identical settings is a no-op. If the existing contents differ,
  it fails instead of overwriting.

## 2. File format rules

| Rule | Value |
|---|---|
| Encoding | UTF-8, no BOM |
| Line endings | `\n` |
| CSV | Header row; comma separator; minimal quoting; empty field = missing value |
| JSON Lines | One object per line; fixed key order; `null` = missing value |
| Row order | Sorted by the first column (business key), then by the full row |
| Timestamps | ISO-8601 UTC with seconds and `Z`: `YYYY-MM-DDTHH:MM:SSZ`. Anything else is invalid |
| Dates | `YYYY-MM-DD`; `joined_date` is a calendar date in the reporting timezone |
| Distances | Kilometres, 3 decimals |
| Money | `fare_amount` in INR, 2 decimals |
| Coordinates | WGS84 decimal degrees, 6 decimals |
| Durations in metrics | Minutes |

## 3. `batch_id`

```
batch_id = sha256(
    "deliverypulse-batch-v1\n"
  + canonical_config_json + "\n"     # json.dumps(config, sort_keys=True, separators=(",", ":"))
  + "batch_sequence=<k>\n"
  + for each data file, in sorted file-name order: "<file name> <sha256 of file bytes>\n"
).hexdigest()                         # 64 lowercase hex characters
```

- The inputs are the four data files of the batch and the generator config. The config includes
  `generator_version` (see below).
- `batch.json` and `defect_manifest.json` are **not** inputs.
- The time and randomness are never inputs, so regenerating the same seed and config gives the
  same `batch_id`.
- Implemented by `compute_batch_id` in `src/ingestion/writers.py`.

**`generator_version`** (an integer, `GENERATOR_VERSION` in `generator_config.py`):
- It is part of the canonical config, so it is part of the dataset name's `cfg` hash and of every
  `batch_id`.
- **Bump it whenever a code change alters the generated output for the same parameters** (seed,
  orders, drivers). Then old and new outputs can never share a name or a `batch_id`.
- A bump makes the previous dataset obsolete; remove it with
  `make clean-dataset DATASET=<old name>`.
- History:
  - 1: first version.
  - 2: defect cases drawn uniformly from each type's eligible pool; default drivers
    `max(20, orders // 60)`.

## 4. Reporting timezone

**Asia/Kolkata (UTC+05:30, no daylight saving)** is the reporting timezone. Timestamps are
stored in UTC and converted only for reporting.

| Concept | Rule |
|---|---|
| `metric_date` | The calendar date in Asia/Kolkata of the metric's basis timestamp |
| Day bucketing | Asia/Kolkata midnight to midnight; every reporting day is exactly 24 hours |
| Hour of day | The hour (0–23) in Asia/Kolkata of the basis timestamp |
| Batch windows | Start and end at Asia/Kolkata midnight |

Why Asia/Kolkata rather than UTC:
- The business operates only in Indian cities. A UTC day starts at 05:30 local time, and
  hour-of-day in UTC would label the lunch peak as 06:00–08:00, which misleads analysts.
- There is no daylight saving, so days never have 23 or 25 hours.
- The batch windows already end at local midnight, so a reporting day never spans two batches.

## 5. Metric definitions

Every metric uses the reporting timezone of section 4 for dates and hours.

| Metric | Definition | Basis timestamp |
|---|---|---|
| Order volume | Orders created on the reporting day / in the hour | `order_created_ts` |
| Peak hours | Order volume by hour of day | `order_created_ts` |
| Delivery duration | Minutes from `pickup_ts` to `delivery_ts`, delivered orders only | `delivery_ts` |
| Late delivery | A delivered order with `delivery_ts > promised_ts` | `delivery_ts` |
| Late-delivery rate | Late delivered orders / delivered orders; cancelled orders excluded | `delivery_ts` |
| Cancellation rate | Cancelled orders / all orders created on the reporting day | `order_created_ts` |
| Driver utilisation (%) | Busy minutes / online minutes × 100. Busy = union of pickup-to-delivery intervals (overlaps counted once, cancelled excluded). Online = paired `online`→`offline` sessions minus paired breaks (section 5.1). `NULL` when the driver-day has no paired session | interval overlap with the reporting day |

All rates are computed from counts with **one** division, then rounded half away from zero
(`round()` on `numeric`): delivery minutes to 2 decimals, late and cancellation rates to 4,
utilisation to 2. A rate whose denominator is 0 is `NULL`, never 0. Rates over several days or
cities are recomputed from the stored counts, never averaged.

**Delivery events feed no metric.** Every metric above uses order timestamps and shift events
only. Delivery events are stored for analysis (`order_event_summary` in `sql/analytics.sql`),
always aggregated to order grain before any join with orders.

### 5.1 Shift pairing (utilisation)

Shift events of one driver are ordered by `(event_ts, event_id)`; `event_id` compares by byte
order (`COLLATE "C"`).
- **Session:** an `online` pairs with the next `online`/`offline` event of that driver if that
  event is an `offline`. Two `online`s in a row: the first is unpaired.
- **Break:** a `break_start` pairs with the next `break_start`/`break_end` event if that event is
  a `break_end`.
- **24-hour cap:** a pair longer than 24 hours (`MAX_SESSION_SECONDS` in `src/contract.py`) is
  ignored as implausible, like an unpaired event.
- **Unpaired events are ignored.** They add no online time and no break time.
- **Breaks count only where they overlap a paired session**, clipped to it.
- **Per reporting day:** each session and break is split at Asia/Kolkata midnight. Online
  minutes = session minutes − break minutes on that day.
- **NULL vs 0:** a driver-day with no paired session has `online_minutes` and `utilisation_pct`
  `NULL`. A driver online all day with no deliveries has utilisation `0.00`.
- **Above 100 %:** busy time is *not* clipped to sessions (the definition above is kept as
  written). Utilisation above 100 % therefore means the driver delivered outside paired online
  sessions. It is stored as computed, never hidden or capped, and the quality check
  `utilisation_above_100` (section 11) reports such driver-days. One cause is benign: a session
  that is still open (its `offline` falls after the batch cutoff or the end of the data) has no
  online minutes yet, while its deliveries already count as busy. The check skips those
  driver-days; they are re-evaluated when the `offline` arrives.

### 5.2 Incremental refresh

Each load recomputes only the reporting dates a batch can change, inside the batch's
transaction:
- **Orders:** for every inserted or updated order, both the old and the new version (PostgreSQL
  `RETURNING old.*, new.*`): the dates of `order_created_ts`, `pickup_ts`, `delivery_ts` and
  every day between pickup and delivery. Including the old version means a date an order *leaves*
  is recomputed too.
- **Shift events:** the event's reporting date ±1 day (a session can cross midnight; the 24-hour
  cap keeps the effect within one day either side).
- **Refresh:** delete and reinsert both aggregate tables for those dates, each computed from the
  full analytics tables. So an incremental load gives exactly the same result as a full rebuild
  (`make metrics`), and replaying a refresh changes nothing (both tested).
- **`dim_date`:** reset on every load and rebuild to the continuous range from the first to the
  last reporting date with data. It does not use the refreshed dates, so both paths give the
  same table (tested).

## 6. Sources

### 6.1 `drivers.csv` (key: `driver_id`)

| Column | Type | Required | Rule |
|---|---|---|---|
| `driver_id` | text | yes | Unique; `DRV-NNNNN` |
| `city` | text | yes | Non-empty |
| `vehicle_type` | text | yes | One of `bicycle`, `scooter`, `motorbike`, `car` (else `invalid_vehicle_type`) |
| `joined_date` | date | yes | `YYYY-MM-DD` |
| `rating` | decimal | no | Empty, or 1.00–5.00 (else `out_of_range_value`) |

Driver records are immutable: a driver appears once. An identical re-send is a `duplicate`.

### 6.2 `orders.csv` (key: `order_id`)

Orders arrive like a change-data capture feed. Batch *k* contains each order that was created or
changed during window *k*, in its state at the end of the window. `source_updated_at` is the time
of that state.

| Column | Type | Required | Rule |
|---|---|---|---|
| `order_id` | text | yes | `ORD-NNNNNNN` |
| `customer_id` | text | yes | Non-empty |
| `driver_id` | text | when status is `assigned`, `picked_up` or `delivered` | Must exist in drivers |
| `order_created_ts` | timestamp | yes | |
| `pickup_ts` | timestamp | when status is `picked_up` or `delivered` | `>= order_created_ts` |
| `delivery_ts` | timestamp | only when status is `delivered` | `>= pickup_ts` (else `delivery_before_pickup`) |
| `promised_ts` | timestamp | yes | `> order_created_ts` |
| `distance_km` | decimal | yes | `> 0` (else `out_of_range_value`) |
| `status` | text | yes | After normalisation: `created`, `assigned`, `picked_up`, `delivered`, `cancelled` |
| `city` | text | yes | Non-empty |
| `fare_amount` | decimal | yes | `> 0` (else `out_of_range_value`) |
| `source_updated_at` | timestamp | yes | `>= order_created_ts`; the version of the record |

- **Status normalisation:** trim surrounding whitespace, then lowercase. A value that is still
  not in the list is `invalid_status`.
- **Versions:** an incoming row replaces the loaded order only if its
  `source_updated_at` is **newer**.
  - Older: `stale_ignored`.
  - Same version, identical content: `duplicate`.
- **Same version, different content (tie rule):** two rows with the same `order_id` and the same
  `source_updated_at` but different content are a conflict. There is no safe winner.
  - **Within one batch:** every row of that `(order_id, source_updated_at)` group is
    `rejected` with reason `conflicting_version`. Any version already loaded stays as it is.
  - **Across batches:** an incoming row whose `source_updated_at` equals the loaded version's,
    but whose content differs, is `rejected` with reason `conflicting_version`. The loaded row
    is kept unchanged.
  - **Rationale:** a deterministic, auditable outcome with no silent overwrite. Upstream must send
    a newer `source_updated_at` to correct a record.
  - The generator does not inject this case; the rule is defined for the transform and covered
    by unit tests.
- **Several versions of one order in the same batch, with different `source_updated_at`:** the
  newest valid row is processed and older rows are `stale_ignored`. Not generated.

### 6.3 `delivery_events.jsonl` (key: `event_id`)

| Field | Type | Required | Rule |
|---|---|---|---|
| `event_id` | text | yes | Unique and stable; `EVT-NNNNNNNNN` |
| `order_id` | text | yes | Must exist in orders (else pending, see 7) |
| `driver_id` | text | yes | Must exist in drivers (else pending) |
| `event_type` | text | yes | `assigned`, `picked_up`, `gps_ping`, `delivered`, `cancelled` |
| `event_ts` | timestamp | yes | |
| `lat` | number or null | for `gps_ping` | −90 to 90; set together with `lon` |
| `lon` | number or null | for `gps_ping` | −180 to 180; set together with `lat` |

- **Events are immutable.** Deduplicate by `event_id`: a replay is a `duplicate`, in the same
  batch or a later one.
- **Event identity and event time are independent.** A valid event with a new `event_id` is
  accepted even when its `event_ts` is older than events already processed (a **late event**).
- **File order is not time order.** Sort by `(event_ts, event_id)` before any sequence logic.
- The generator never produces the `cancelled` event type: in this simulation orders are only
  cancelled before a driver is assigned.

**GPS validation** (pings of the same driver, sorted by `(event_ts, event_id)`):
1. Missing coordinates: `rejected`, reason `missing_coordinates`.
2. Out-of-range coordinates: `rejected`, reason `invalid_coordinates`.
3. Compare each ping with its **predecessor**: the driver's latest accepted ping that comes
   before it in `(event_ts, event_id)` order, across all batches processed so far. For in-order
   data this is simply the last accepted ping. For a late ping it is the accepted ping just
   before it in time.
   - Interval of 0 seconds or less: `rejected`, reason `gps_non_positive_interval`.
   - Haversine distance ÷ interval above **120 km/h** (configurable): `rejected`, reason
     `gps_impossible_speed`.
   - Rejected pings never become the comparison point, so only the bad ping is flagged, not the
     ping after it.
   - A ping that passed every check becomes a reference point even if it is pending (its order
     has not arrived). It is not re-checked when it resolves.
4. A long time gap is **not** a jump. A ping 20 minutes or more after the previous one, at a
   plausible speed, is accepted.

**Limitations of the GPS rule:**
- A late ping is checked only against its predecessor, **not its successor**. Accepted pings
  are immutable, so an already-accepted ping that comes after a late ping is never re-checked
  against it.
- A late ping's predecessor is chosen among the pings accepted so far, so the result can depend
  on arrival order when two pings of one driver arrive in different batches.

### 6.4 `shift_events.csv` (key: `event_id`)

| Column | Type | Required | Rule |
|---|---|---|---|
| `event_id` | text | yes | Unique; `SHF-NNNNNNN` |
| `driver_id` | text | yes | Must exist in drivers (else pending) |
| `event_type` | text | yes | `online`, `offline`, `break_start`, `break_end` |
| `event_ts` | timestamp | yes | |

Each shift emits `online`, `break_start`, `break_end` and `offline`, in that order. Unpaired
events are handled by the pairing rule of section 5.1: a driver-day without a paired
online/offline session gets `NULL` utilisation.

## 7. Dispositions and reconciliation

Every input row of each source gets exactly one disposition: `accepted`, `rejected`,
`duplicate`, `stale_ignored` or `pending`. For each source and batch separately:
`rows_input = accepted + rejected + duplicate + stale_ignored + pending`.

- **Pending:** a valid record whose parent has not arrived yet: an event without its order or
  driver, a shift event without its driver, or an order whose `driver_id` is not a known driver
  yet. It is held in `staging.pending_events`, retried in later batches, and accepted once the
  parent exists.
- **Expired orphans:** a record still pending after **2** later batches (configurable,
  `--orphan-max-age-batches`) is rejected with reason `orphan_timeout`. Its original disposition
  in the batch it arrived in stays `pending`; the expiry is a separate resolution record.
- **Retries are separate:** retrying pending records is not part of a later batch's
  `rows_input`. Retries are counted as `pending_resolved`, `pending_remaining` and
  `pending_expired`.

### 7.1 Order of checks (first match wins)

Within a batch the steps run in this order:
1. drivers
2. retry pending orders
3. orders
4. retry pending delivery and shift events
5. delivery events
6. shift events

So a parent that arrives in a batch resolves its pending children in the same batch.

Every source starts with:
- an unparseable record (malformed JSON line, wrong CSV field count): `rejected` /
  `malformed_record`
- an exact copy of an earlier row of the same batch: `duplicate`

| Source | Checks after that, in order |
|---|---|
| drivers | required fields (`missing_required_field`); vehicle type (`invalid_vehicle_type`); joined date (`invalid_timestamp`); rating number (`invalid_number`) and range (`out_of_range_value`); already accepted: identical is `duplicate`, different is `rejected` / `conflicting_version` |
| orders | required fields; status after normalisation (`invalid_status`); any non-empty timestamp unparseable (`invalid_timestamp`); `distance_km` / `fare_amount` not a number (`invalid_number`) or ≤ 0 or too large for the column (`out_of_range_value`); status-dependent fields (`inconsistent_status_fields`); `delivery_ts < pickup_ts` (`delivery_before_pickup`); `pickup_ts < order_created_ts`, `promised_ts ≤ order_created_ts` or `source_updated_at < order_created_ts` (`inconsistent_timestamps`); driver not accepted (`pending` / `missing_driver`); then version logic (section 6.2 and the tie rule) |
| delivery events | required fields; event type (`invalid_event_type`); timestamp (`invalid_timestamp`); `event_id` already seen (accepted or pending): identical is `duplicate`, different is `conflicting_version`; coordinates (`missing_coordinates`, `invalid_coordinates`); GPS predecessor rule (`gps_non_positive_interval`, `gps_impossible_speed`); order not accepted (`pending` / `missing_order`); driver not accepted (`pending` / `missing_driver`) |
| shift events | required fields; event type (`invalid_event_type`); timestamp (`invalid_timestamp`); `event_id` already seen; driver not accepted (`pending` / `missing_driver`) |

Within one batch, for events, the first occurrence of an `event_id` wins; a later row with the
same id is a `duplicate` if identical, otherwise `rejected` / `conflicting_version`.

### 7.2 Reason codes

| Code | Disposition | Meaning |
|---|---|---|
| `missing_required_field` | rejected | A required value is empty |
| `invalid_status` | rejected | Order status not in the list after normalisation |
| `invalid_timestamp` | rejected | A timestamp or date does not match the contract format |
| `invalid_number` | rejected | A numeric field is not a number |
| `out_of_range_value` | rejected | A number outside its allowed range |
| `invalid_vehicle_type` | rejected | Vehicle type not in the list |
| `invalid_event_type` | rejected | Delivery or shift event type not in the list |
| `inconsistent_status_fields` | rejected | Driver, pickup or delivery fields do not fit the order status |
| `delivery_before_pickup` | rejected | `delivery_ts < pickup_ts` |
| `inconsistent_timestamps` | rejected | Other impossible timestamp order (see 7.1) |
| `missing_coordinates` | rejected | A ping without coordinates, or only one of lat/lon |
| `invalid_coordinates` | rejected | Coordinates unparseable or out of range |
| `gps_non_positive_interval` | rejected | Ping not later than its predecessor |
| `gps_impossible_speed` | rejected | Speed from the predecessor above the threshold |
| `conflicting_version` | rejected | Same key and version (or same event id) with different content |
| `malformed_record` | rejected | The row cannot be parsed |
| `orphan_timeout` | rejected (expiry) | Still pending after the maximum age |
| `older_source_updated_at` | stale_ignored | Older than the accepted order version |
| `missing_order` | pending | The event's order is not accepted yet |
| `missing_driver` | pending | The record's driver is not accepted yet |

### 7.3 Derived fields and the cleaned output

The transform writes a cleaned layer under `data/processed/<dataset>/cleaned/`. It is a
**derived, normalised representation of the accepted records, not the source**: statuses are
normalised, timestamps typed in UTC, and only the latest accepted version of each order is kept.

| Folder | Grain | Partition |
|---|---|---|
| `drivers/` | One row per driver | none (small dimension) |
| `orders/` | One row per order (latest accepted version) | `date=` reporting date of `order_created_ts` |
| `delivery_events/` | One row per accepted event | `date=` reporting date of `event_ts` |
| `shift_events/` | One row per accepted shift event | `date=` reporting date of `event_ts` |

Partitions are Hive-style (`date=YYYY-MM-DD`) in the reporting timezone (section 4).

Derived order columns, following section 5:
- `delivery_minutes` = minutes from `pickup_ts` to `delivery_ts`, for delivered orders only;
  `NULL` otherwise.
- `is_late` = `delivery_ts > promised_ts`, for delivered orders only; `NULL` otherwise.

Rejected and pending records are written per batch as `rejected_records.parquet` and
`pending_events.parquet`, shaped like the staging tables. `make load` writes the same records to
PostgreSQL directly.

## 8. Schema changes

- An unknown extra column is preserved in `raw` (`extra_columns`), ignored downstream, and
  logged as a warning.
- A missing required column fails the whole batch with a clear error and a `failed` audit
  record.

## 9. Defect catalogue (what the generator injects)

Counts per type are `max(1, round(rate × orders / 10,000))`; the rates are in
`generator_config.py`. Actual counts per batch are reported by `make profile` in
`docs/data_quality_report.md`. Every scenario uses its own order or driver.

**Selection rule:**
- Each type draws its cases uniformly at random, without replacement, from the orders still
  eligible for that type. Eligible means delivered, with exactly one version, and not used by
  another scenario.
- Types that need a following batch (`unchanged_order_resend`, `stale_order_version`,
  `delayed_order_record`, `replayed_event_next_batch`, `late_event`) can only use batches 1 to
  n−1.
- Every other type is spread over all batches in proportion to each batch's eligible orders.

| Type | Source | How it appears | Expected disposition | Reason |
|---|---|---|---|---|
| `exact_duplicate_row` | orders | Identical row twice in one batch | duplicate (second copy) | |
| `unchanged_order_resend` | orders | Identical row again in the next batch | duplicate | |
| `stale_order_version` | orders | Older version (`assigned`) sent after the final version | stale_ignored | `older_source_updated_at` |
| `missing_required_value` | orders | `customer_id`, `city` or `promised_ts` empty | rejected | `missing_required_field` |
| `invalid_status` | orders | `in_transit`, `lost`, `unknown` | rejected | `invalid_status` |
| `status_needs_normalisation` | orders | ` delivered`, `Delivered`, `DELIVERED`, `delivered ` | accepted | `status_normalised` |
| `delivery_before_pickup` | orders | `delivery_ts < pickup_ts` | rejected | `delivery_before_pickup` |
| `unparseable_timestamp` | orders | `2026-02-30T25:61:00Z`, `yesterday`, `30/09/2026 18:30` | rejected | `invalid_timestamp` |
| `invalid_numeric_range` | orders | `fare_amount` or `distance_km` zero or negative | rejected | `out_of_range_value` |
| `delayed_order_record` | orders | Order row arrives one batch after its events | accepted (next batch) | |
| `orphan_event_missing_order` | delivery_events | Events of a delayed order | pending, resolved next batch | `missing_order` |
| `replayed_event_same_batch` | delivery_events | Identical event twice in one batch | duplicate | |
| `replayed_event_next_batch` | delivery_events | Identical event again in the next batch | duplicate | |
| `missing_coordinates` | delivery_events | `gps_ping` with null `lat` and `lon` | rejected | `missing_coordinates` |
| `out_of_range_coordinates` | delivery_events | `lat` beyond ±90 or `lon` beyond ±180 | rejected | `invalid_coordinates` |
| `impossible_gps_speed` | delivery_events | Ping moved 30–80 km within one ping interval | rejected | `gps_impossible_speed` |
| `duplicate_timestamp` | delivery_events | Extra ping, new `event_id`, same `event_ts` as the previous ping, 200 m away | rejected | `gps_non_positive_interval` |
| `out_of_order_event` | delivery_events | Two consecutive pings whose file order is the reverse of time order | accepted | `reordered_by_event_ts` |
| `gps_long_gap` | delivery_events | Four pings missing (gap of 20 minutes or more) at a plausible speed | accepted | `long_gap_not_a_jump` |
| `late_event` | delivery_events | Event arrives one batch after its time window | accepted | |
| `delayed_driver_record` | drivers | Driver row arrives one batch after the driver's first shift events | accepted (next batch) | |
| `orphan_shift_event_missing_driver` | shift_events | Shift events of a delayed driver | pending, resolved next batch | `missing_driver` |
| `invalid_driver_value` | drivers | `rating` 5.70 or 0.50, or `vehicle_type` `truck` or `drone` (driver has no shifts or orders) | rejected | `out_of_range_value` / `invalid_vehicle_type` |
| `replayed_shift_event` | shift_events | Identical shift event twice in one batch | duplicate | |
| `newer_order_update` (natural) | orders | Later version of an order that was in progress at a cutoff | accepted (update) | |

Orders with a rejected row have no delivery events, so a rejection never creates orphan events
as a side effect.

## 10. Ground truth isolation

`data/ground_truth/` and the `src.groundtruth` package are for tests and reports only. A unit
test parses every module under `src/` and fails if any module references them, except these:
- the generator modules `src/ingestion/{generate_data,simulation,defects,writers,generator_config}.py`
- `src/groundtruth/` itself

Tests may use them freely.

## 11. Data-quality checks

The checks are code in `src/quality/` (registry: `CHECKS` in `src/quality/checks.py`; SQL in
`sql_raw.py` and `sql_integrity.py`). Every check has a name, a layer and table, a severity and
a threshold.

### 11.1 Rules

- **Severity.**
  - `blocking`: a failure means a pipeline bug or corrupt state, so the batch is not marked
    succeeded.
  - `warning`: recorded, never blocks.
- **Threshold kinds.**
  - `count`: failing rows.
  - `share`: failing rows ÷ rows checked.
  - `hours`: a measured lag.
- **Status.**
  - A check passes while its observed value is at most the threshold. A check with no data to
    measure passes.
  - Above the threshold, a blocking check `failed` and a warning `warned`.
- **No check repeats a database constraint.** A check that a constraint already enforces could
  never fail, so it would be vacuous.
- **Raw checks detect defects independently.** They re-derive each defect from the raw text
  (regex, `pg_input_is_valid`, window functions, Haversine) and never read the pipeline's
  dispositions.
  - Every raw check is a `warning`: raw defects are expected, and the pipeline quarantines or
    handles them.
  - Default threshold: **2 %** of the source's rows in the batch.
- **Minimum sample for share thresholds: 50 rows checked** (configurable as
  `--threshold min_share_rows=<whole number>`).
  - Below it, a share check reports `passed`: a share of a tiny sample says nothing. For
    example, batch 2 has 3 driver rows that are all delayed drivers, which is 100 %.
  - Its failing rows, rows checked, observed share and sample keys are still stored, so the
    defects stay visible.
  - Count and hours thresholds are always applied.
  - The coverage proof (`make quality-report`) uses the failing-row counts, which this rule
    does not change.
- **Thresholds are configurable.** Use `--threshold <check>=<value>` on `make load` and
  `make quality`, through `QUALITY_ARGS`. An override names a check (all its sources) or one
  `check.source`. `max_speed_kmh` (120) and `long_gap_seconds` (1,200) are shared with the
  transform.

### 11.2 Catalogue

Raw-layer detection checks: warning, share threshold 2 % unless noted. "Valid timestamp" means
the contract format and `pg_input_is_valid`.

| Check | Sources | Rule | Detects |
|---|---|---|---|
| `raw_duplicate_rows` | all | Same full row more than once in a batch (extra copies) | `exact_duplicate_row`, `replayed_event_same_batch`, `replayed_shift_event` |
| `raw_missing_required` | all | A required field empty | `missing_required_value` |
| `raw_unparseable_timestamps` | all | A non-empty timestamp (drivers: `joined_date`) not valid | `unparseable_timestamp` |
| `raw_invalid_status` | orders | Status not valid even after trim and lowercase | `invalid_status` |
| `raw_status_not_canonical` | orders | Status valid only after trim and lowercase | `status_needs_normalisation` |
| `raw_numeric_out_of_range` | orders | `distance_km` / `fare_amount` not numeric or ≤ 0 | `invalid_numeric_range` |
| `raw_driver_invalid_values` | drivers | Vehicle type not in the list, or rating not 1–5 | `invalid_driver_value` |
| `raw_delivery_before_pickup` | orders | Both valid and `delivery_ts < pickup_ts` | `delivery_before_pickup` |
| `raw_resent_unchanged` | orders | Row identical to an order row of an earlier batch | `unchanged_order_resend` |
| `raw_stale_versions` | orders | `source_updated_at` older than the order's newest version in earlier batches | `stale_order_version` |
| `raw_version_updates` | orders | Newer than the order's newest version in earlier batches (threshold 5 %) | `newer_order_update` |
| `raw_replayed_from_earlier_batch` | delivery and shift events | `event_id` already in an earlier batch | `replayed_event_next_batch` |
| `raw_gps_missing_coordinates` | delivery events | `gps_ping` with lat or lon empty | `missing_coordinates` |
| `raw_gps_invalid_coordinates` | delivery events | lat/lon not numeric or out of range | `out_of_range_coordinates` |
| `raw_gps_impossible_speed` | delivery events | Impossible speed from the driver's previous valid ping (by `event_ts`, `event_id`), unless that ping was itself reached at an impossible speed (the return leg after a jump is not counted) | `impossible_gps_speed` |
| `raw_gps_duplicate_timestamps` | delivery events | Two different valid pings of one driver at the same `event_ts` | `duplicate_timestamp` |
| `raw_events_out_of_file_order` | delivery events | Within one order, a GPS ping's `event_ts` is earlier than the previous ping's in file row order (pings only: a status event between two swapped pings would double-count the swap) | `out_of_order_event` |
| `raw_gps_long_gaps` | delivery events | Consecutive pings of one order ≥ 1,200 s apart at a plausible speed | `gps_long_gap` |
| `raw_late_events` | delivery events | `event_ts` before the batch's `window_start`, at the event's first arrival | `late_event` |
| `raw_orphans` | delivery and shift events | Parent order or driver row not received in this or an earlier batch | `orphan_event_missing_order`, `orphan_shift_event_missing_driver` |
| `raw_parent_after_children` | orders, drivers | First row of an order or driver arrives after its events or shifts | `delayed_order_record`, `delayed_driver_record` |

Integrity checks:

| Check | Layer | Severity | Threshold | Rule |
|---|---|---|---|---|
| `reconciliation_identity` | audit | blocking | 0 | Per source: every raw row of the batch has exactly one disposition, and the counters of succeeded runs equal the dispositions (input = accepted + rejected + duplicate + stale_ignored + pending) |
| `disposition_values` | audit, staging | blocking | 0 | Dispositions and reasons are documented values; every rejection has a reason |
| `rejected_quarantined` | staging | blocking | 0 | Rejected rows per batch and source = rejected dispositions (expired orphans, reason `orphan_timeout`, are counted separately) |
| `accepted_loaded` | analytics | blocking | 0 | Every accepted record of the batch exists in its analytics table |
| `pending_parent_missing` | staging | blocking | 0 | No pending record whose missing parent already exists |
| `orders_required_by_status` | analytics | blocking | 0 | `driver_id` set for assigned / picked_up / delivered; `pickup_ts` set for picked_up / delivered |
| `orders_positive_amounts` | analytics | blocking | 0 | `distance_km > 0` and `fare_amount > 0` (the schema allows 0) |
| `gps_ping_unique` | analytics | blocking | 0 | At most one `gps_ping` per driver and `event_ts` |
| `metrics_match_facts` | analytics | blocking | 0 | Per date and city: total, cancelled, delivered and late counts equal direct counts from `orders` (the refreshed dates in a load; every date in `make quality`) |
| `no_future_timestamps` | analytics | blocking | 0 | No accepted timestamp at or after its batch's `window_end` (columns below) |
| `freshness_lag` | analytics | warning | 6 h | `window_end` minus the newest accepted `event_ts` of the latest batch |
| `known_cities` | analytics | warning | 0 | City not in `CITY_STATES` |
| `utilisation_above_100` | analytics | warning | 0 | Driver-days with `utilisation_pct > 100`. Excluded while the session is still open: the driver's last online/offline event (by `event_ts`, `event_id`) is an `online` that began before the end of that reporting day. Its `offline` has not arrived yet (section 5.1), so the day is re-evaluated when it does |
| `gps_speed_in_analytics` | analytics | warning | 0 | Consecutive accepted pings of a driver above the speed limit (possible through the late-ping limitation in section 6.3) |

### 11.3 `no_future_timestamps`: covered columns

| Table | Columns checked |
|---|---|
| `analytics.orders` | `order_created_ts`, `pickup_ts`, `delivery_ts`, `source_updated_at` |
| `analytics.delivery_events` | `event_ts` |
| `analytics.driver_shift_events` | `event_ts` |

- **`promised_ts` is excluded.** It is a *planned* time: an order created shortly before a
  cutoff is legitimately promised after the batch window ends. Every other column records
  something that has already happened when the batch is cut.
- **Which window applies.** Each order is compared with the window of the batch that accepted
  its *current* version, so a later update is judged against the later batch.
- A test proves that an order with `promised_ts` after `window_end` passes.

### 11.4 The gate in the load

- **Where the checks run.** Inside transaction B, after every write, the pipeline state and
  the metric refresh, and before the audit rows become `succeeded`. So they see this batch's
  raw rows, dispositions, staging rows and analytics effects.
- **Passing.** Results are stored in B and commit atomically with `succeeded`.
- **A blocking failure.**
  - B rolls back: no raw, staging or business rows and no metric refresh.
  - Transaction C marks the batch `failed` with `failure_reason = 'quality gate: <checks>'` and
    stores the quality run (kept in memory) with sample keys.
  - `make load` exits 1.
  - Retry: fix the cause, then run `make load` again (the next attempt).
- **No bypass flag.** Only thresholds are configurable.
- **Coverage proof.**
  - `make quality-report` compares the load-time results with the defect manifest. It writes
    the "Quality checks" section of `docs/data_quality_report.md`.
  - A check may count more than its own defect type only where the difference equals exactly
    another type's manifest count, documented in `EXPLAINED_DIFFERENCES`. Any other difference
    fails.
