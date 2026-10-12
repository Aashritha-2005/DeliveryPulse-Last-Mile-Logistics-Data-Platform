"""SQL of the raw-layer detection checks of the quality framework.

Each query re-derives one kind of defect from the raw text of the batches in
``%(batch_ids)s``, without reading the pipeline's dispositions. Every query returns one row per
instance (source): ``(instance, failing_rows, rows_checked, sample_keys, measure)``; ``measure``
is NULL for count and share checks.

Shared parameters: ``dataset``, ``batch_ids``, ``meta_columns`` (raw bookkeeping columns that
are not part of a record), ``order_statuses``, ``vehicle_types``, ``max_speed_kmh`` and
``long_gap_seconds``. A contract timestamp is valid when it matches
``YYYY-MM-DDTHH:MM:SSZ`` and ``pg_input_is_valid`` accepts it. "Earlier batch" uses the batch
sequence recorded in ``audit.pipeline_runs``.
"""

from typing import LiteralString

DUPLICATE_ROWS: LiteralString = """
WITH sources(source) AS (
    VALUES ('drivers'), ('orders'), ('delivery_events'), ('shift_events')
),
src_rows AS (
    SELECT 'drivers' AS source, r.batch_id, r.driver_id AS record_key,
           to_jsonb(r) - %(meta_columns)s::text[] AS doc
    FROM raw.drivers_raw r WHERE r.batch_id = ANY(%(batch_ids)s)
    UNION ALL
    SELECT 'orders', r.batch_id, r.order_id, to_jsonb(r) - %(meta_columns)s::text[]
    FROM raw.orders_raw r WHERE r.batch_id = ANY(%(batch_ids)s)
    UNION ALL
    SELECT 'delivery_events', r.batch_id, r.event_id, to_jsonb(r) - %(meta_columns)s::text[]
    FROM raw.events_raw r WHERE r.batch_id = ANY(%(batch_ids)s)
    UNION ALL
    SELECT 'shift_events', r.batch_id, r.event_id, to_jsonb(r) - %(meta_columns)s::text[]
    FROM raw.shift_events_raw r WHERE r.batch_id = ANY(%(batch_ids)s)
),
copies AS (
    SELECT source, min(coalesce(record_key, '')) AS record_key, count(*) AS n
    FROM src_rows
    GROUP BY source, batch_id, doc
),
summary AS (
    SELECT source,
           sum(n) AS checked,
           sum(n - 1) FILTER (WHERE n > 1) AS failing,
           (array_agg(record_key ORDER BY record_key) FILTER (WHERE n > 1))[1:10] AS sample
    FROM copies
    GROUP BY source
)
SELECT s.source, coalesce(m.failing, 0), coalesce(m.checked, 0), coalesce(m.sample, '{}'),
       NULL::numeric
FROM sources s LEFT JOIN summary m USING (source)
"""

MISSING_REQUIRED: LiteralString = """
WITH required(source, fields) AS (
    VALUES
        ('drivers', ARRAY['driver_id', 'city', 'vehicle_type', 'joined_date']),
        ('orders', ARRAY['order_id', 'customer_id', 'order_created_ts', 'promised_ts',
                         'distance_km', 'status', 'city', 'fare_amount', 'source_updated_at']),
        ('delivery_events', ARRAY['event_id', 'order_id', 'driver_id', 'event_type', 'event_ts']),
        ('shift_events', ARRAY['event_id', 'driver_id', 'event_type', 'event_ts'])
),
src_rows AS (
    SELECT 'drivers' AS source, r.driver_id AS record_key, to_jsonb(r) AS doc
    FROM raw.drivers_raw r WHERE r.batch_id = ANY(%(batch_ids)s)
    UNION ALL
    SELECT 'orders', r.order_id, to_jsonb(r)
    FROM raw.orders_raw r WHERE r.batch_id = ANY(%(batch_ids)s)
    UNION ALL
    SELECT 'delivery_events', r.event_id, to_jsonb(r)
    FROM raw.events_raw r WHERE r.batch_id = ANY(%(batch_ids)s)
    UNION ALL
    SELECT 'shift_events', r.event_id, to_jsonb(r)
    FROM raw.shift_events_raw r WHERE r.batch_id = ANY(%(batch_ids)s)
),
flagged AS (
    SELECT s.source, coalesce(s.record_key, '') AS record_key,
           EXISTS (
               SELECT 1 FROM unnest(q.fields) AS f(name)
               WHERE coalesce(s.doc ->> f.name, '') = ''
           ) AS bad
    FROM src_rows s JOIN required q USING (source)
),
summary AS (
    SELECT source, count(*) AS checked, count(*) FILTER (WHERE bad) AS failing,
           (array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10] AS sample
    FROM flagged
    GROUP BY source
)
SELECT q.source, coalesce(m.failing, 0), coalesce(m.checked, 0), coalesce(m.sample, '{}'),
       NULL::numeric
FROM required q LEFT JOIN summary m USING (source)
"""

UNPARSEABLE_TIMESTAMPS: LiteralString = """
WITH fields(source, name, pg_type, pattern) AS (
    VALUES
        ('drivers', 'joined_date', 'date', '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'),
        ('orders', 'order_created_ts', 'timestamptz',
         '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'),
        ('orders', 'pickup_ts', 'timestamptz',
         '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'),
        ('orders', 'delivery_ts', 'timestamptz',
         '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'),
        ('orders', 'promised_ts', 'timestamptz',
         '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'),
        ('orders', 'source_updated_at', 'timestamptz',
         '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'),
        ('delivery_events', 'event_ts', 'timestamptz',
         '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'),
        ('shift_events', 'event_ts', 'timestamptz',
         '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$')
),
sources AS (SELECT DISTINCT source FROM fields),
src_rows AS (
    SELECT 'drivers' AS source, r.driver_id AS record_key, to_jsonb(r) AS doc
    FROM raw.drivers_raw r WHERE r.batch_id = ANY(%(batch_ids)s)
    UNION ALL
    SELECT 'orders', r.order_id, to_jsonb(r)
    FROM raw.orders_raw r WHERE r.batch_id = ANY(%(batch_ids)s)
    UNION ALL
    SELECT 'delivery_events', r.event_id, to_jsonb(r)
    FROM raw.events_raw r WHERE r.batch_id = ANY(%(batch_ids)s)
    UNION ALL
    SELECT 'shift_events', r.event_id, to_jsonb(r)
    FROM raw.shift_events_raw r WHERE r.batch_id = ANY(%(batch_ids)s)
),
flagged AS (
    SELECT s.source, coalesce(s.record_key, '') AS record_key,
           EXISTS (
               SELECT 1 FROM fields f
               WHERE f.source = s.source
                 AND coalesce(s.doc ->> f.name, '') <> ''
                 AND NOT ((s.doc ->> f.name) ~ f.pattern
                          AND pg_input_is_valid(s.doc ->> f.name, f.pg_type))
           ) AS bad
    FROM src_rows s
),
summary AS (
    SELECT source, count(*) AS checked, count(*) FILTER (WHERE bad) AS failing,
           (array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10] AS sample
    FROM flagged
    GROUP BY source
)
SELECT s.source, coalesce(m.failing, 0), coalesce(m.checked, 0), coalesce(m.sample, '{}'),
       NULL::numeric
FROM sources s LEFT JOIN summary m USING (source)
"""

INVALID_STATUS: LiteralString = """
WITH flagged AS (
    SELECT coalesce(r.order_id, '') AS record_key,
           coalesce(r.status, '') <> ''
           AND lower(btrim(r.status)) <> ALL(%(order_statuses)s::text[]) AS bad
    FROM raw.orders_raw r
    WHERE r.batch_id = ANY(%(batch_ids)s)
)
SELECT 'orders', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

STATUS_NOT_CANONICAL: LiteralString = """
WITH flagged AS (
    SELECT coalesce(r.order_id, '') AS record_key,
           r.status <> lower(btrim(r.status))
           AND lower(btrim(r.status)) = ANY(%(order_statuses)s::text[]) AS bad
    FROM raw.orders_raw r
    WHERE r.batch_id = ANY(%(batch_ids)s)
)
SELECT 'orders', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

NUMERIC_OUT_OF_RANGE: LiteralString = """
WITH flagged AS (
    SELECT coalesce(r.order_id, '') AS record_key,
           EXISTS (
               SELECT 1 FROM unnest(ARRAY[r.distance_km, r.fare_amount]) AS v(value)
               WHERE coalesce(v.value, '') <> ''
                 AND CASE WHEN NOT pg_input_is_valid(v.value, 'numeric') THEN true
                          ELSE v.value::numeric <= 0 END
           ) AS bad
    FROM raw.orders_raw r
    WHERE r.batch_id = ANY(%(batch_ids)s)
)
SELECT 'orders', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

DRIVER_INVALID_VALUES: LiteralString = """
WITH flagged AS (
    SELECT coalesce(r.driver_id, '') AS record_key,
           (coalesce(r.vehicle_type, '') <> ''
            AND r.vehicle_type <> ALL(%(vehicle_types)s::text[]))
           OR (coalesce(r.rating, '') <> ''
               AND CASE WHEN NOT pg_input_is_valid(r.rating, 'numeric') THEN true
                        ELSE r.rating::numeric NOT BETWEEN 1 AND 5 END) AS bad
    FROM raw.drivers_raw r
    WHERE r.batch_id = ANY(%(batch_ids)s)
)
SELECT 'drivers', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

DELIVERY_BEFORE_PICKUP: LiteralString = """
WITH flagged AS (
    SELECT coalesce(r.order_id, '') AS record_key,
           CASE WHEN r.pickup_ts ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'
                     AND pg_input_is_valid(r.pickup_ts, 'timestamptz')
                     AND r.delivery_ts ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'
                     AND pg_input_is_valid(r.delivery_ts, 'timestamptz')
                THEN r.delivery_ts::timestamptz < r.pickup_ts::timestamptz
                ELSE false END AS bad
    FROM raw.orders_raw r
    WHERE r.batch_id = ANY(%(batch_ids)s)
)
SELECT 'orders', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

RESENT_UNCHANGED: LiteralString = """
WITH batches AS (
    SELECT batch_id, min(batch_sequence) AS batch_sequence FROM audit.pipeline_runs
    WHERE dataset = %(dataset)s GROUP BY batch_id
),
hashed AS (
    SELECT r.batch_id, b.batch_sequence, coalesce(r.order_id, '') AS record_key,
           md5((to_jsonb(r) - %(meta_columns)s::text[])::text) AS h
    FROM raw.orders_raw r JOIN batches b USING (batch_id)
),
first_seen AS (
    SELECT h, min(batch_sequence) AS first_sequence FROM hashed GROUP BY h
),
flagged AS (
    SELECT x.record_key, f.first_sequence < x.batch_sequence AS bad
    FROM hashed x JOIN first_seen f USING (h)
    WHERE x.batch_id = ANY(%(batch_ids)s)
)
SELECT 'orders', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

# Stale versions and version updates share the same comparison: a row's valid
# source_updated_at against the newest valid version of that order in earlier batches.
STALE_VERSIONS: LiteralString = """
WITH batches AS (
    SELECT batch_id, min(batch_sequence) AS batch_sequence FROM audit.pipeline_runs
    WHERE dataset = %(dataset)s GROUP BY batch_id
),
versions AS (
    SELECT r.raw_id, r.batch_id, b.batch_sequence, r.order_id,
           CASE WHEN r.source_updated_at
                     ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'
                     AND pg_input_is_valid(r.source_updated_at, 'timestamptz')
                THEN r.source_updated_at::timestamptz END AS version
    FROM raw.orders_raw r JOIN batches b USING (batch_id)
),
current_rows AS (
    SELECT * FROM versions WHERE batch_id = ANY(%(batch_ids)s)
),
previous AS (
    SELECT c.raw_id, max(p.version) AS previous_version
    FROM current_rows c
    JOIN versions p ON p.order_id = c.order_id AND p.batch_sequence < c.batch_sequence
    GROUP BY c.raw_id
),
flagged AS (
    SELECT coalesce(c.order_id, '') AS record_key,
           coalesce(c.version < p.previous_version, false) AS bad
    FROM current_rows c LEFT JOIN previous p USING (raw_id)
)
SELECT 'orders', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

VERSION_UPDATES: LiteralString = """
WITH batches AS (
    SELECT batch_id, min(batch_sequence) AS batch_sequence FROM audit.pipeline_runs
    WHERE dataset = %(dataset)s GROUP BY batch_id
),
versions AS (
    SELECT r.raw_id, r.batch_id, b.batch_sequence, r.order_id,
           CASE WHEN r.source_updated_at
                     ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'
                     AND pg_input_is_valid(r.source_updated_at, 'timestamptz')
                THEN r.source_updated_at::timestamptz END AS version
    FROM raw.orders_raw r JOIN batches b USING (batch_id)
),
current_rows AS (
    SELECT * FROM versions WHERE batch_id = ANY(%(batch_ids)s)
),
previous AS (
    SELECT c.raw_id, max(p.version) AS previous_version
    FROM current_rows c
    JOIN versions p ON p.order_id = c.order_id AND p.batch_sequence < c.batch_sequence
    GROUP BY c.raw_id
),
flagged AS (
    SELECT coalesce(c.order_id, '') AS record_key,
           coalesce(c.version > p.previous_version, false) AS bad
    FROM current_rows c LEFT JOIN previous p USING (raw_id)
)
SELECT 'orders', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

REPLAYED_FROM_EARLIER_BATCH: LiteralString = """
WITH sources(source) AS (VALUES ('delivery_events'), ('shift_events')),
batches AS (
    SELECT batch_id, min(batch_sequence) AS batch_sequence FROM audit.pipeline_runs
    WHERE dataset = %(dataset)s GROUP BY batch_id
),
events AS (
    SELECT 'delivery_events' AS source, r.batch_id, b.batch_sequence, r.event_id
    FROM raw.events_raw r JOIN batches b USING (batch_id)
    UNION ALL
    SELECT 'shift_events', r.batch_id, b.batch_sequence, r.event_id
    FROM raw.shift_events_raw r JOIN batches b USING (batch_id)
),
first_seen AS (
    SELECT source, event_id, min(batch_sequence) AS first_sequence
    FROM events
    GROUP BY source, event_id
),
flagged AS (
    SELECT e.source, coalesce(e.event_id, '') AS record_key,
           coalesce(f.first_sequence < e.batch_sequence, false) AS bad
    FROM events e LEFT JOIN first_seen f USING (source, event_id)
    WHERE e.batch_id = ANY(%(batch_ids)s)
),
summary AS (
    SELECT source, count(*) AS checked, count(*) FILTER (WHERE bad) AS failing,
           (array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10] AS sample
    FROM flagged
    GROUP BY source
)
SELECT s.source, coalesce(m.failing, 0), coalesce(m.checked, 0), coalesce(m.sample, '{}'),
       NULL::numeric
FROM sources s LEFT JOIN summary m USING (source)
"""

GPS_MISSING_COORDINATES: LiteralString = """
WITH flagged AS (
    SELECT coalesce(r.event_id, '') AS record_key,
           r.event_type = 'gps_ping'
           AND (coalesce(r.lat, '') = '' OR coalesce(r.lon, '') = '') AS bad
    FROM raw.events_raw r
    WHERE r.batch_id = ANY(%(batch_ids)s)
)
SELECT 'delivery_events', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

GPS_INVALID_COORDINATES: LiteralString = """
WITH flagged AS (
    SELECT coalesce(r.event_id, '') AS record_key,
           (coalesce(r.lat, '') <> ''
            AND CASE WHEN NOT pg_input_is_valid(r.lat, 'double precision') THEN true
                     ELSE r.lat::double precision NOT BETWEEN -90 AND 90 END)
           OR (coalesce(r.lon, '') <> ''
               AND CASE WHEN NOT pg_input_is_valid(r.lon, 'double precision') THEN true
                        ELSE r.lon::double precision NOT BETWEEN -180 AND 180 END) AS bad
    FROM raw.events_raw r
    WHERE r.batch_id = ANY(%(batch_ids)s)
)
SELECT 'delivery_events', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

# Valid pings: gps_ping with a valid timestamp and in-range coordinates, one row per event_id
# (replayed copies collapse). Speeds use the same Haversine radius as src/transformations/gps.py.
GPS_IMPOSSIBLE_SPEED: LiteralString = """
WITH candidates AS (
    SELECT DISTINCT ON (r.batch_id, r.event_id)
           r.batch_id, r.event_id, r.driver_id, r.event_ts, r.lat, r.lon
    FROM raw.events_raw r
    WHERE r.batch_id = ANY(%(batch_ids)s)
      AND r.event_type = 'gps_ping'
      AND r.event_ts ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'
      AND pg_input_is_valid(r.event_ts, 'timestamptz')
      AND pg_input_is_valid(r.lat, 'double precision')
      AND pg_input_is_valid(r.lon, 'double precision')
    ORDER BY r.batch_id, r.event_id
),
typed AS (
    SELECT batch_id, event_id, driver_id, event_ts::timestamptz AS ts,
           lat::double precision AS lat, lon::double precision AS lon
    FROM candidates
),
pings AS (
    SELECT * FROM typed WHERE lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180
),
steps AS (
    SELECT batch_id, driver_id, event_id, ts, lat, lon,
           lag(ts) OVER w AS prev_ts, lag(lat) OVER w AS prev_lat, lag(lon) OVER w AS prev_lon
    FROM pings
    WINDOW w AS (PARTITION BY batch_id, driver_id ORDER BY ts, event_id)
),
speeds AS (
    SELECT batch_id, driver_id, event_id, ts,
           CASE WHEN prev_ts IS NOT NULL AND ts > prev_ts THEN
               2 * 6371.0088 * asin(sqrt(
                   power(sin(radians(lat - prev_lat) / 2), 2)
                   + cos(radians(prev_lat)) * cos(radians(lat))
                     * power(sin(radians(lon - prev_lon) / 2), 2)))
               / (extract(epoch FROM ts - prev_ts) / 3600)
           END AS speed_in
    FROM steps
),
-- Impossible speed from the previous valid ping, unless that previous ping was itself reached
-- at an impossible speed: then this ping is the "return leg" after a jump and is not counted.
chained AS (
    SELECT event_id, speed_in,
           lag(speed_in) OVER (PARTITION BY batch_id, driver_id ORDER BY ts, event_id)
               AS prev_speed_in
    FROM speeds
),
flagged AS (
    SELECT event_id,
           coalesce(speed_in > %(max_speed_kmh)s
                    AND coalesce(prev_speed_in <= %(max_speed_kmh)s, true), false) AS bad
    FROM chained
)
SELECT 'delivery_events', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(event_id ORDER BY event_id) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

GPS_DUPLICATE_TIMESTAMPS: LiteralString = """
WITH candidates AS (
    SELECT DISTINCT ON (r.batch_id, r.event_id)
           r.batch_id, r.event_id, r.driver_id, r.event_ts, r.lat, r.lon
    FROM raw.events_raw r
    WHERE r.batch_id = ANY(%(batch_ids)s)
      AND r.event_type = 'gps_ping'
      AND r.event_ts ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'
      AND pg_input_is_valid(r.event_ts, 'timestamptz')
      AND pg_input_is_valid(r.lat, 'double precision')
      AND pg_input_is_valid(r.lon, 'double precision')
    ORDER BY r.batch_id, r.event_id
),
pings AS (
    SELECT batch_id, event_id, driver_id, event_ts::timestamptz AS ts
    FROM candidates
    WHERE lat::double precision BETWEEN -90 AND 90
      AND lon::double precision BETWEEN -180 AND 180
),
groups AS (
    SELECT count(*) AS n, max(event_id) AS record_key
    FROM pings
    GROUP BY batch_id, driver_id, ts
)
SELECT 'delivery_events',
       coalesce(sum(n - 1) FILTER (WHERE n > 1), 0),
       coalesce(sum(n), 0),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE n > 1))[1:10], '{}'),
       NULL::numeric
FROM groups
"""

# GPS pings only (a status event between two swapped pings would turn one swap into two
# backward steps), and only the first ping per order and timestamp in file order (a second
# ping at the same timestamp is reported by raw_gps_duplicate_timestamps instead). So the
# check measures exactly pings written out of time order.
EVENTS_OUT_OF_FILE_ORDER: LiteralString = """
WITH events AS (
    SELECT r.batch_id, r.order_id, r.event_id, r.source_row_number,
           CASE WHEN r.event_ts ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'
                     AND pg_input_is_valid(r.event_ts, 'timestamptz')
                THEN r.event_ts::timestamptz END AS ts
    FROM raw.events_raw r
    WHERE r.batch_id = ANY(%(batch_ids)s) AND r.event_type = 'gps_ping'
),
first_per_timestamp AS (
    SELECT DISTINCT ON (batch_id, order_id, ts) batch_id, order_id, event_id, source_row_number, ts
    FROM events
    WHERE ts IS NOT NULL
    ORDER BY batch_id, order_id, ts, source_row_number
),
steps AS (
    SELECT coalesce(event_id, '') AS record_key, ts,
           lag(ts) OVER (PARTITION BY batch_id, order_id ORDER BY source_row_number) AS prev_ts
    FROM first_per_timestamp
),
flagged AS (
    SELECT record_key, coalesce(ts < prev_ts, false) AS bad FROM steps
)
SELECT 'delivery_events', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

GPS_LONG_GAPS: LiteralString = """
WITH candidates AS (
    SELECT DISTINCT ON (r.batch_id, r.event_id)
           r.batch_id, r.event_id, r.order_id, r.event_ts, r.lat, r.lon
    FROM raw.events_raw r
    WHERE r.batch_id = ANY(%(batch_ids)s)
      AND r.event_type = 'gps_ping'
      AND r.event_ts ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'
      AND pg_input_is_valid(r.event_ts, 'timestamptz')
      AND pg_input_is_valid(r.lat, 'double precision')
      AND pg_input_is_valid(r.lon, 'double precision')
    ORDER BY r.batch_id, r.event_id
),
typed AS (
    SELECT batch_id, event_id, order_id, event_ts::timestamptz AS ts,
           lat::double precision AS lat, lon::double precision AS lon
    FROM candidates
),
pings AS (
    SELECT * FROM typed WHERE lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180
),
steps AS (
    SELECT event_id, ts, lat, lon,
           lag(ts) OVER w AS prev_ts, lag(lat) OVER w AS prev_lat, lag(lon) OVER w AS prev_lon
    FROM pings
    WINDOW w AS (PARTITION BY batch_id, order_id ORDER BY ts, event_id)
),
flagged AS (
    SELECT event_id,
           CASE WHEN prev_ts IS NOT NULL
                     AND extract(epoch FROM ts - prev_ts) >= %(long_gap_seconds)s THEN
               2 * 6371.0088 * asin(sqrt(
                   power(sin(radians(lat - prev_lat) / 2), 2)
                   + cos(radians(prev_lat)) * cos(radians(lat))
                     * power(sin(radians(lon - prev_lon) / 2), 2)))
               / (extract(epoch FROM ts - prev_ts) / 3600) <= %(max_speed_kmh)s
           ELSE false END AS bad
    FROM steps
)
SELECT 'delivery_events', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(event_id ORDER BY event_id) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

LATE_EVENTS: LiteralString = """
WITH batches AS (
    SELECT batch_id, min(batch_sequence) AS batch_sequence, max(window_start) AS window_start
    FROM audit.pipeline_runs
    WHERE dataset = %(dataset)s GROUP BY batch_id
),
events AS (
    SELECT r.batch_id, b.batch_sequence, b.window_start, r.event_id,
           CASE WHEN r.event_ts ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'
                     AND pg_input_is_valid(r.event_ts, 'timestamptz')
                THEN r.event_ts::timestamptz END AS ts
    FROM raw.events_raw r JOIN batches b USING (batch_id)
),
first_seen AS (
    SELECT event_id, min(batch_sequence) AS first_sequence FROM events GROUP BY event_id
),
flagged AS (
    SELECT coalesce(e.event_id, '') AS record_key,
           coalesce(e.ts < e.window_start AND f.first_sequence = e.batch_sequence, false) AS bad
    FROM events e LEFT JOIN first_seen f USING (event_id)
    WHERE e.batch_id = ANY(%(batch_ids)s)
)
SELECT 'delivery_events', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

ORPHANS: LiteralString = """
WITH sources(source) AS (VALUES ('delivery_events'), ('shift_events')),
batches AS (
    SELECT batch_id, min(batch_sequence) AS batch_sequence FROM audit.pipeline_runs
    WHERE dataset = %(dataset)s GROUP BY batch_id
),
orders_seen AS (
    SELECT r.order_id, min(b.batch_sequence) AS first_sequence
    FROM raw.orders_raw r JOIN batches b USING (batch_id)
    GROUP BY r.order_id
),
drivers_seen AS (
    SELECT r.driver_id, min(b.batch_sequence) AS first_sequence
    FROM raw.drivers_raw r JOIN batches b USING (batch_id)
    GROUP BY r.driver_id
),
events AS (
    SELECT 'delivery_events' AS source, r.event_id, r.order_id, r.driver_id, b.batch_sequence
    FROM raw.events_raw r JOIN batches b USING (batch_id)
    WHERE r.batch_id = ANY(%(batch_ids)s)
    UNION ALL
    SELECT 'shift_events', r.event_id, NULL, r.driver_id, b.batch_sequence
    FROM raw.shift_events_raw r JOIN batches b USING (batch_id)
    WHERE r.batch_id = ANY(%(batch_ids)s)
),
flagged AS (
    SELECT e.source, coalesce(e.event_id, '') AS record_key,
           (coalesce(e.order_id, '') <> ''
            AND coalesce(o.first_sequence > e.batch_sequence, true))
           OR (coalesce(e.driver_id, '') <> ''
               AND coalesce(d.first_sequence > e.batch_sequence, true)) AS bad
    FROM events e
    LEFT JOIN orders_seen o ON o.order_id = e.order_id
    LEFT JOIN drivers_seen d ON d.driver_id = e.driver_id
),
summary AS (
    SELECT source, count(*) AS checked, count(*) FILTER (WHERE bad) AS failing,
           (array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10] AS sample
    FROM flagged
    GROUP BY source
)
SELECT s.source, coalesce(m.failing, 0), coalesce(m.checked, 0), coalesce(m.sample, '{}'),
       NULL::numeric
FROM sources s LEFT JOIN summary m USING (source)
"""

PARENT_AFTER_CHILDREN: LiteralString = """
WITH sources(source) AS (VALUES ('drivers'), ('orders')),
batches AS (
    SELECT batch_id, min(batch_sequence) AS batch_sequence FROM audit.pipeline_runs
    WHERE dataset = %(dataset)s GROUP BY batch_id
),
order_rows AS (
    SELECT r.order_id, min(b.batch_sequence) AS first_sequence
    FROM raw.orders_raw r JOIN batches b USING (batch_id)
    GROUP BY r.order_id
),
order_children AS (
    SELECT r.order_id, min(b.batch_sequence) AS first_sequence
    FROM raw.events_raw r JOIN batches b USING (batch_id)
    GROUP BY r.order_id
),
driver_rows AS (
    SELECT r.driver_id, min(b.batch_sequence) AS first_sequence
    FROM raw.drivers_raw r JOIN batches b USING (batch_id)
    GROUP BY r.driver_id
),
driver_children AS (
    SELECT driver_id, min(batch_sequence) AS first_sequence
    FROM (
        SELECT r.driver_id, b.batch_sequence
        FROM raw.shift_events_raw r JOIN batches b USING (batch_id)
        UNION ALL
        SELECT r.driver_id, b.batch_sequence
        FROM raw.events_raw r JOIN batches b USING (batch_id)
    ) AS children
    GROUP BY driver_id
),
candidates AS (
    SELECT 'orders' AS source, coalesce(r.order_id, '') AS record_key,
           b.batch_sequence,
           o.first_sequence AS first_row, c.first_sequence AS first_child
    FROM raw.orders_raw r JOIN batches b USING (batch_id)
    LEFT JOIN order_rows o ON o.order_id = r.order_id
    LEFT JOIN order_children c ON c.order_id = r.order_id
    WHERE r.batch_id = ANY(%(batch_ids)s)
    UNION ALL
    SELECT 'drivers', coalesce(r.driver_id, ''), b.batch_sequence,
           d.first_sequence, c.first_sequence
    FROM raw.drivers_raw r JOIN batches b USING (batch_id)
    LEFT JOIN driver_rows d ON d.driver_id = r.driver_id
    LEFT JOIN driver_children c ON c.driver_id = r.driver_id
    WHERE r.batch_id = ANY(%(batch_ids)s)
),
flagged AS (
    SELECT source, record_key,
           coalesce(first_row = batch_sequence AND first_child < batch_sequence, false) AS bad
    FROM candidates
),
summary AS (
    SELECT source, count(*) AS checked, count(*) FILTER (WHERE bad) AS failing,
           (array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10] AS sample
    FROM flagged
    GROUP BY source
)
SELECT s.source, coalesce(m.failing, 0), coalesce(m.checked, 0), coalesce(m.sample, '{}'),
       NULL::numeric
FROM sources s LEFT JOIN summary m USING (source)
"""
