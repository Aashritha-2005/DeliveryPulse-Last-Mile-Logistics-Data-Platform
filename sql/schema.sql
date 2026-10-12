-- DeliveryPulse schema (data dictionary: docs/data_model.md).
--
-- Run once, on first initialisation of the data volume, after roles.sql and before
-- grants.sql. Schema responsibilities (never blur these):
--   raw        source records exactly as received (permissive text columns), with batch_id
--              and ingested_at. Append-only.
--   staging    validated per-batch work, rejected_records (with reasons), pending_events.
--   analytics  accepted records only, fully constrained. CHECK constraints live ONLY here.
--   audit      batch and run metadata (pipeline_runs).
-- All timestamps are timestamptz and stored in UTC.

CREATE SCHEMA raw AUTHORIZATION deliverypulse_owner;
CREATE SCHEMA staging AUTHORIZATION deliverypulse_owner;
CREATE SCHEMA analytics AUTHORIZATION deliverypulse_owner;
CREATE SCHEMA audit AUTHORIZATION deliverypulse_owner;

-- Every table below is created by, and therefore owned by, deliverypulse_owner.
SET ROLE deliverypulse_owner;

-- ===================================================================== raw ===
-- Source columns are text so malformed values are preserved, never discarded.
-- extra_columns keeps unknown source columns (docs/data_contracts.md, section 8).
-- (batch_id, source_file, source_row_number) identifies a source row, so replaying a batch
-- into raw can be made idempotent.

CREATE TABLE raw.drivers_raw (
    raw_id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id          text        NOT NULL,
    source_file       text        NOT NULL,
    source_row_number integer     NOT NULL,
    ingested_at       timestamptz NOT NULL DEFAULT now(),
    driver_id         text,
    city              text,
    vehicle_type      text,
    joined_date       text,
    rating            text,
    extra_columns     jsonb,
    UNIQUE (batch_id, source_file, source_row_number)
);

CREATE TABLE raw.orders_raw (
    raw_id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id          text        NOT NULL,
    source_file       text        NOT NULL,
    source_row_number integer     NOT NULL,
    ingested_at       timestamptz NOT NULL DEFAULT now(),
    order_id          text,
    customer_id       text,
    driver_id         text,
    order_created_ts  text,
    pickup_ts         text,
    delivery_ts       text,
    promised_ts       text,
    distance_km       text,
    status            text,
    city              text,
    fare_amount       text,
    source_updated_at text,
    extra_columns     jsonb,
    UNIQUE (batch_id, source_file, source_row_number)
);

CREATE TABLE raw.events_raw (
    raw_id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id          text        NOT NULL,
    source_file       text        NOT NULL,
    source_row_number integer     NOT NULL,
    ingested_at       timestamptz NOT NULL DEFAULT now(),
    event_id          text,
    order_id          text,
    driver_id         text,
    event_type        text,
    event_ts          text,
    lat               text,
    lon               text,
    extra_columns     jsonb,
    UNIQUE (batch_id, source_file, source_row_number)
);

CREATE TABLE raw.shift_events_raw (
    raw_id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id          text        NOT NULL,
    source_file       text        NOT NULL,
    source_row_number integer     NOT NULL,
    ingested_at       timestamptz NOT NULL DEFAULT now(),
    event_id          text,
    driver_id         text,
    event_type        text,
    event_ts          text,
    extra_columns     jsonb,
    UNIQUE (batch_id, source_file, source_row_number)
);

-- ================================================================= staging ===
-- Rejected and pending records, plus the cross-batch pipeline state used by the loader.

CREATE TABLE staging.rejected_records (
    rejected_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id       text        NOT NULL,
    source         text        NOT NULL,
    record_key     text,
    record_payload jsonb       NOT NULL,
    reason         text        NOT NULL,
    rejected_at    timestamptz NOT NULL DEFAULT now()
);

-- Valid records whose parent (order or driver) has not arrived yet (docs/data_contracts.md,
-- section 7).
CREATE TABLE staging.pending_events (
    source            text        NOT NULL,
    record_key        text        NOT NULL,
    batch_id          text        NOT NULL,
    batch_sequence    integer     NOT NULL,
    row_number        integer     NOT NULL,
    missing_reference text        NOT NULL,
    fields            jsonb       NOT NULL,
    record_payload    jsonb       NOT NULL,
    first_seen_at     timestamptz NOT NULL DEFAULT now(),
    last_retry_at     timestamptz,
    retry_count       integer     NOT NULL DEFAULT 0,
    PRIMARY KEY (source, record_key)
);

COMMENT ON COLUMN staging.pending_events.source IS
    'Source of the held record: orders, delivery_events or shift_events.';
COMMENT ON COLUMN staging.pending_events.record_key IS
    'event_id for events; <order_id>@<source_updated_at> for orders (two pending versions never collide).';
COMMENT ON COLUMN staging.pending_events.missing_reference IS
    'Which parent is missing: order or driver.';
COMMENT ON COLUMN staging.pending_events.fields IS
    'The contract fields as received (text), used to rebuild the record when its parent arrives.';

-- Pipeline state: accepted driver and order versions, and every event id seen (accepted or
-- pending), each with a content hash for duplicate and conflict detection.
CREATE TABLE staging.record_state (
    source       text        NOT NULL,
    record_key   text        NOT NULL,
    version_ts   timestamptz,
    content_hash text        NOT NULL,
    batch_id     text        NOT NULL,
    PRIMARY KEY (source, record_key)
);

COMMENT ON COLUMN staging.record_state.version_ts IS
    'Accepted source_updated_at for orders; NULL for drivers and events.';

-- Pipeline state: every GPS ping that passed the GPS checks (accepted or pending). The
-- primary key serves the predecessor lookup by (driver_id, event_ts, event_id).
CREATE TABLE staging.gps_reference (
    driver_id text             NOT NULL,
    event_ts  timestamptz      NOT NULL,
    event_id  text             NOT NULL,
    lat       double precision NOT NULL,
    lon       double precision NOT NULL,
    batch_id  text             NOT NULL,
    PRIMARY KEY (driver_id, event_ts, event_id)
);

-- =============================================================== analytics ===
-- Star schema: orders and delivery_events are facts; drivers, dim_date and dim_city are
-- dimensions. Accepted-value lists are provisional until the Task 3 data contract.

CREATE TABLE analytics.dim_date (
    date_key        date     PRIMARY KEY,
    calendar_year   smallint NOT NULL,
    quarter         smallint NOT NULL,
    month           smallint NOT NULL,
    day_of_month    smallint NOT NULL,
    iso_day_of_week smallint NOT NULL,
    iso_week        smallint NOT NULL,
    is_weekend      boolean  NOT NULL,
    CONSTRAINT dim_date_quarter_range CHECK (quarter BETWEEN 1 AND 4),
    CONSTRAINT dim_date_month_range CHECK (month BETWEEN 1 AND 12),
    CONSTRAINT dim_date_day_range CHECK (day_of_month BETWEEN 1 AND 31),
    CONSTRAINT dim_date_dow_range CHECK (iso_day_of_week BETWEEN 1 AND 7),
    CONSTRAINT dim_date_week_range CHECK (iso_week BETWEEN 1 AND 53)
);

COMMENT ON TABLE analytics.dim_date IS
    'Generated date dimension; one row per reporting date (reporting timezone, default Asia/Kolkata).';

CREATE TABLE analytics.dim_city (
    city  text PRIMARY KEY,
    state text NOT NULL
);

COMMENT ON TABLE analytics.dim_city IS 'Generated city dimension; one row per city.';

CREATE TABLE analytics.drivers (
    driver_id    text         PRIMARY KEY,
    city         text         NOT NULL,
    vehicle_type text         NOT NULL,
    joined_date  date         NOT NULL,
    rating       numeric(3, 2),
    CONSTRAINT drivers_vehicle_type_valid
        CHECK (vehicle_type IN ('bicycle', 'scooter', 'motorbike', 'car')),
    CONSTRAINT drivers_rating_range CHECK (rating BETWEEN 1 AND 5)
);

COMMENT ON COLUMN analytics.drivers.rating IS 'Average customer rating, 1-5; NULL if not yet rated.';

CREATE TABLE analytics.orders (
    order_id          text           PRIMARY KEY,
    customer_id       text           NOT NULL,
    driver_id         text           REFERENCES analytics.drivers (driver_id),
    order_created_ts  timestamptz    NOT NULL,
    pickup_ts         timestamptz,
    delivery_ts       timestamptz,
    promised_ts       timestamptz    NOT NULL,
    distance_km       numeric(7, 3)  NOT NULL,
    status            text           NOT NULL,
    city              text           NOT NULL,
    fare_amount       numeric(10, 2) NOT NULL,
    source_updated_at timestamptz    NOT NULL,
    CONSTRAINT orders_status_valid
        CHECK (status IN ('created', 'assigned', 'picked_up', 'delivered', 'cancelled')),
    CONSTRAINT orders_distance_non_negative CHECK (distance_km >= 0),
    CONSTRAINT orders_fare_non_negative CHECK (fare_amount >= 0),
    CONSTRAINT orders_promised_after_created CHECK (promised_ts > order_created_ts),
    CONSTRAINT orders_pickup_after_created CHECK (pickup_ts >= order_created_ts),
    CONSTRAINT orders_delivery_after_pickup CHECK (delivery_ts >= pickup_ts),
    CONSTRAINT orders_delivered_has_timestamps
        CHECK (status <> 'delivered' OR (pickup_ts IS NOT NULL AND delivery_ts IS NOT NULL)),
    CONSTRAINT orders_delivery_ts_only_when_delivered
        CHECK (delivery_ts IS NULL OR status = 'delivered'),
    CONSTRAINT orders_updated_after_created CHECK (source_updated_at >= order_created_ts)
);

COMMENT ON COLUMN analytics.orders.driver_id IS 'Assigned driver; NULL while unassigned.';
COMMENT ON COLUMN analytics.orders.distance_km IS 'Route distance in kilometres.';
COMMENT ON COLUMN analytics.orders.source_updated_at IS
    'Version of the source record; an update applies only if this is newer.';

CREATE TABLE analytics.delivery_events (
    event_id   text             PRIMARY KEY,
    order_id   text             NOT NULL REFERENCES analytics.orders (order_id),
    driver_id  text             NOT NULL REFERENCES analytics.drivers (driver_id),
    event_type text             NOT NULL,
    event_ts   timestamptz      NOT NULL,
    lat        double precision,
    lon        double precision,
    CONSTRAINT delivery_events_type_valid
        CHECK (event_type IN ('assigned', 'picked_up', 'gps_ping', 'delivered', 'cancelled')),
    CONSTRAINT delivery_events_lat_range CHECK (lat BETWEEN -90 AND 90),
    CONSTRAINT delivery_events_lon_range CHECK (lon BETWEEN -180 AND 180),
    CONSTRAINT delivery_events_coordinates_paired CHECK ((lat IS NULL) = (lon IS NULL)),
    CONSTRAINT delivery_events_ping_has_coordinates
        CHECK (event_type <> 'gps_ping' OR lat IS NOT NULL)
);

CREATE TABLE analytics.driver_shift_events (
    event_id   text        PRIMARY KEY,
    driver_id  text        NOT NULL REFERENCES analytics.drivers (driver_id),
    event_type text        NOT NULL,
    event_ts   timestamptz NOT NULL,
    CONSTRAINT driver_shift_events_type_valid
        CHECK (event_type IN ('online', 'offline', 'break_start', 'break_end'))
);

CREATE TABLE analytics.daily_delivery_metrics (
    metric_date          date          NOT NULL,
    city                 text          NOT NULL,
    total_orders         integer       NOT NULL,
    avg_delivery_minutes numeric(8, 2),
    late_rate            numeric(5, 4),
    cancelled_rate       numeric(5, 4),
    delivered_orders     integer       NOT NULL,
    late_orders          integer       NOT NULL,
    cancelled_orders     integer       NOT NULL,
    PRIMARY KEY (metric_date, city),
    CONSTRAINT daily_metrics_total_orders_non_negative CHECK (total_orders >= 0),
    CONSTRAINT daily_metrics_avg_minutes_non_negative CHECK (avg_delivery_minutes >= 0),
    CONSTRAINT daily_metrics_late_rate_range CHECK (late_rate BETWEEN 0 AND 1),
    CONSTRAINT daily_metrics_cancelled_rate_range CHECK (cancelled_rate BETWEEN 0 AND 1),
    CONSTRAINT daily_metrics_delivered_non_negative CHECK (delivered_orders >= 0),
    CONSTRAINT daily_metrics_late_within_delivered
        CHECK (late_orders >= 0 AND late_orders <= delivered_orders),
    CONSTRAINT daily_metrics_cancelled_within_total
        CHECK (cancelled_orders >= 0 AND cancelled_orders <= total_orders)
);

COMMENT ON TABLE analytics.daily_delivery_metrics IS
    'One row per city per reporting date. Columns use different timestamp bases; see each column.';
COMMENT ON COLUMN analytics.daily_delivery_metrics.metric_date IS
    'Reporting date in the reporting timezone (default Asia/Kolkata).';
COMMENT ON COLUMN analytics.daily_delivery_metrics.total_orders IS
    'Basis order_created_ts: orders created on metric_date.';
COMMENT ON COLUMN analytics.daily_delivery_metrics.avg_delivery_minutes IS
    'Basis delivery_ts: mean pickup-to-delivery minutes of orders delivered on metric_date; NULL if none.';
COMMENT ON COLUMN analytics.daily_delivery_metrics.late_rate IS
    'Basis delivery_ts: late delivered / delivered orders on metric_date (cancelled excluded); NULL if none.';
COMMENT ON COLUMN analytics.daily_delivery_metrics.cancelled_rate IS
    'Basis order_created_ts: cancelled / all orders created on metric_date; NULL if none.';
COMMENT ON COLUMN analytics.daily_delivery_metrics.delivered_orders IS
    'Basis delivery_ts: orders delivered on metric_date.';
COMMENT ON COLUMN analytics.daily_delivery_metrics.late_orders IS
    'Basis delivery_ts: delivered orders with delivery_ts > promised_ts, delivered on metric_date.';
COMMENT ON COLUMN analytics.daily_delivery_metrics.cancelled_orders IS
    'Basis order_created_ts: cancelled orders created on metric_date.';

CREATE TABLE analytics.driver_performance (
    driver_id            text          NOT NULL REFERENCES analytics.drivers (driver_id),
    metric_date          date          NOT NULL,
    deliveries           integer       NOT NULL,
    avg_delivery_minutes numeric(8, 2),
    utilisation_pct      numeric(6, 2),
    busy_minutes         numeric(8, 2) NOT NULL,
    online_minutes       numeric(8, 2),
    PRIMARY KEY (driver_id, metric_date),
    CONSTRAINT driver_perf_deliveries_non_negative CHECK (deliveries >= 0),
    CONSTRAINT driver_perf_avg_minutes_non_negative CHECK (avg_delivery_minutes >= 0),
    CONSTRAINT driver_perf_utilisation_non_negative CHECK (utilisation_pct >= 0),
    CONSTRAINT driver_perf_busy_non_negative CHECK (busy_minutes >= 0),
    CONSTRAINT driver_perf_online_non_negative CHECK (online_minutes >= 0)
);

COMMENT ON TABLE analytics.driver_performance IS
    'One row per driver per reporting date (reporting timezone, default Asia/Kolkata).';
COMMENT ON COLUMN analytics.driver_performance.deliveries IS
    'Basis delivery_ts: orders the driver delivered on metric_date.';
COMMENT ON COLUMN analytics.driver_performance.utilisation_pct IS
    'busy_minutes / online_minutes x 100; NULL without a paired online session. Above 100 means busy time outside paired sessions (inconsistent data).';
COMMENT ON COLUMN analytics.driver_performance.busy_minutes IS
    'Union of pickup-to-delivery intervals of delivered orders, clipped to the reporting day.';
COMMENT ON COLUMN analytics.driver_performance.online_minutes IS
    'Paired online sessions minus paired breaks, clipped to the reporting day; NULL if none.';

-- =================================================================== audit ===
-- status is validated by the pipeline: CHECK constraints are reserved for analytics.
-- One row per batch, source and attempt. An attempt starts as 'started' (committed on its own),
-- then becomes 'succeeded' in the batch transaction or 'failed' in a separate transaction.

CREATE TABLE audit.pipeline_runs (
    run_id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset            text        NOT NULL,
    batch_id           text        NOT NULL,
    batch_sequence     integer     NOT NULL,
    attempt            integer     NOT NULL,
    source             text        NOT NULL,
    status             text        NOT NULL,
    started_at         timestamptz NOT NULL DEFAULT now(),
    finished_at        timestamptz,
    duration_ms        bigint,
    rows_input         bigint      NOT NULL DEFAULT 0,
    rows_accepted      bigint      NOT NULL DEFAULT 0,
    rows_rejected      bigint      NOT NULL DEFAULT 0,
    rows_duplicate     bigint      NOT NULL DEFAULT 0,
    rows_stale_ignored bigint      NOT NULL DEFAULT 0,
    rows_pending       bigint      NOT NULL DEFAULT 0,
    rows_inserted      bigint      NOT NULL DEFAULT 0,
    rows_updated       bigint      NOT NULL DEFAULT 0,
    rows_unchanged     bigint      NOT NULL DEFAULT 0,
    rows_failed        bigint      NOT NULL DEFAULT 0,
    pending_resolved   bigint      NOT NULL DEFAULT 0,
    pending_remaining  bigint      NOT NULL DEFAULT 0,
    pending_expired    bigint      NOT NULL DEFAULT 0,
    failure_reason     text,
    window_start       timestamptz,
    window_end         timestamptz
);

COMMENT ON COLUMN audit.pipeline_runs.window_start IS
    'Start of the batch''s source window (batch.json); used by the late-event and freshness checks.';
COMMENT ON COLUMN audit.pipeline_runs.window_end IS
    'Exclusive end of the batch''s source window (batch.json).';

COMMENT ON COLUMN audit.pipeline_runs.status IS 'One of: started, succeeded, failed.';
COMMENT ON COLUMN audit.pipeline_runs.failure_reason IS
    'Error message of a failed attempt; ''abandoned'' if a run died without finishing.';

-- A batch can succeed only once per source: idempotency is enforced by the database too.
CREATE UNIQUE INDEX pipeline_runs_one_success
    ON audit.pipeline_runs (batch_id, source) WHERE status = 'succeeded';
CREATE INDEX pipeline_runs_dataset_status ON audit.pipeline_runs (dataset, status);

-- The original disposition of every input row (it must stay auditable).
CREATE TABLE audit.record_dispositions (
    batch_id    text    NOT NULL,
    source      text    NOT NULL,
    row_number  integer NOT NULL,
    record_key  text    NOT NULL,
    disposition text    NOT NULL,
    reason      text    NOT NULL,
    PRIMARY KEY (batch_id, source, row_number)
);

-- How each pending record left the queue: resolved (parent arrived) or expired.
CREATE TABLE audit.pending_resolutions (
    source               text NOT NULL,
    record_key           text NOT NULL,
    original_batch_id    text NOT NULL,
    resolved_in_batch_id text NOT NULL,
    outcome              text NOT NULL,
    disposition          text NOT NULL,
    reason               text NOT NULL,
    PRIMARY KEY (source, record_key)
);

-- Data-quality runs (Task 7): one row per evaluation of the check suite. A load evaluates it
-- once per batch attempt (triggered_by 'load'); `make quality` evaluates the whole database
-- (triggered_by 'manual', batch_id NULL). Stored even when the gate blocks the batch.
CREATE TABLE audit.quality_runs (
    quality_run_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset           text        NOT NULL,
    batch_id          text,
    batch_sequence    integer,
    attempt           integer,
    triggered_by      text        NOT NULL,
    status            text        NOT NULL,
    checks_total      integer     NOT NULL,
    blocking_failures integer     NOT NULL,
    warnings          integer     NOT NULL,
    duration_ms       bigint      NOT NULL,
    started_at        timestamptz NOT NULL DEFAULT now()
);

COMMENT ON COLUMN audit.quality_runs.triggered_by IS 'One of: load, manual.';
COMMENT ON COLUMN audit.quality_runs.status IS
    'One of: passed (no failure, no warning), warned (warnings only), failed (a blocking check failed).';

-- One row per check (and source) of a quality run.
CREATE TABLE audit.quality_results (
    quality_run_id bigint  NOT NULL REFERENCES audit.quality_runs (quality_run_id),
    check_name     text    NOT NULL,
    layer          text    NOT NULL,
    table_name     text    NOT NULL,
    severity       text    NOT NULL,
    status         text    NOT NULL,
    observed_value numeric,
    threshold      numeric NOT NULL,
    threshold_kind text    NOT NULL,
    failing_rows   bigint  NOT NULL,
    rows_checked   bigint  NOT NULL,
    sample_keys    text[]  NOT NULL,
    duration_ms    bigint  NOT NULL,
    PRIMARY KEY (quality_run_id, check_name)
);

COMMENT ON COLUMN audit.quality_results.duration_ms IS
    'Run time of the check''s query (shared by the per-source results of one check).';

COMMENT ON COLUMN audit.quality_results.check_name IS
    'Check name, suffixed with the source for per-source checks (e.g. raw_duplicate_rows.orders).';
COMMENT ON COLUMN audit.quality_results.threshold_kind IS
    'count: failing rows; share: failing rows / rows checked; hours: a measured lag.';
COMMENT ON COLUMN audit.quality_results.sample_keys IS
    'Up to 10 business keys of failing rows; never record payloads.';

RESET ROLE;
