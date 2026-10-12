"""SQL of the staging, audit and analytics integrity checks of the quality framework.

None of these repeats a database constraint (a check that a constraint already enforces could
never fail). Every query returns ``(instance, failing_rows, rows_checked, sample_keys,
measure)``. Batch-scoped checks use ``%(batch_ids)s``; ``%(dates)s`` are the reporting dates to
compare (the refreshed dates of a load, or every date in `make quality`).
"""

from typing import LiteralString

RECONCILIATION_IDENTITY: LiteralString = """
WITH sources(source) AS (
    VALUES ('drivers'), ('orders'), ('delivery_events'), ('shift_events')
),
raw_rows AS (
    SELECT 'drivers' AS source, batch_id, source_row_number AS row_number
    FROM raw.drivers_raw WHERE batch_id = ANY(%(batch_ids)s)
    UNION ALL
    SELECT 'orders', batch_id, source_row_number
    FROM raw.orders_raw WHERE batch_id = ANY(%(batch_ids)s)
    UNION ALL
    SELECT 'delivery_events', batch_id, source_row_number
    FROM raw.events_raw WHERE batch_id = ANY(%(batch_ids)s)
    UNION ALL
    SELECT 'shift_events', batch_id, source_row_number
    FROM raw.shift_events_raw WHERE batch_id = ANY(%(batch_ids)s)
),
dispositions AS (
    SELECT source, batch_id, row_number, disposition
    FROM audit.record_dispositions WHERE batch_id = ANY(%(batch_ids)s)
),
paired AS (
    SELECT coalesce(r.source, d.source) AS source,
           left(coalesce(r.batch_id, d.batch_id), 12) || ':row-'
               || coalesce(r.row_number, d.row_number) AS record_key,
           r.row_number IS NULL OR d.row_number IS NULL AS bad
    FROM raw_rows r
    FULL JOIN dispositions d
      ON d.source = r.source AND d.batch_id = r.batch_id AND d.row_number = r.row_number
),
per_batch AS (
    SELECT source, batch_id, count(*) AS input,
           count(*) FILTER (WHERE disposition = 'accepted') AS accepted,
           count(*) FILTER (WHERE disposition = 'rejected') AS rejected,
           count(*) FILTER (WHERE disposition = 'duplicate') AS duplicate,
           count(*) FILTER (WHERE disposition = 'stale_ignored') AS stale_ignored,
           count(*) FILTER (WHERE disposition = 'pending') AS pending
    FROM dispositions
    GROUP BY source, batch_id
),
counters AS (
    SELECT p.source, left(p.batch_id, 12) || ':counters' AS record_key,
           p.rows_input <> p.rows_accepted + p.rows_rejected + p.rows_duplicate
                           + p.rows_stale_ignored + p.rows_pending
           OR (p.rows_input, p.rows_accepted, p.rows_rejected, p.rows_duplicate,
               p.rows_stale_ignored, p.rows_pending)
              IS DISTINCT FROM (coalesce(b.input, 0), coalesce(b.accepted, 0),
                                coalesce(b.rejected, 0), coalesce(b.duplicate, 0),
                                coalesce(b.stale_ignored, 0), coalesce(b.pending, 0)) AS bad
    FROM audit.pipeline_runs p
    LEFT JOIN per_batch b ON b.source = p.source AND b.batch_id = p.batch_id
    WHERE p.status = 'succeeded' AND p.batch_id = ANY(%(batch_ids)s)
),
flagged AS (
    SELECT source, record_key, bad FROM paired
    UNION ALL
    SELECT source, record_key, bad FROM counters
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

DISPOSITION_VALUES: LiteralString = """
WITH flagged AS (
    SELECT left(batch_id, 12) || ':' || source || ':row-' || row_number AS record_key,
           disposition <> ALL(%(dispositions)s::text[])
           OR reason <> ALL(%(reason_codes)s::text[])
           OR (disposition = 'rejected' AND reason = '') AS bad
    FROM audit.record_dispositions
    WHERE batch_id = ANY(%(batch_ids)s)
    UNION ALL
    SELECT 'rejected:' || source || ':' || coalesce(record_key, ''),
           reason = '' OR reason <> ALL(%(reason_codes)s::text[])
    FROM staging.rejected_records
    WHERE batch_id = ANY(%(batch_ids)s)
)
SELECT '', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

# Expired pending records are written to rejected_records with their original batch_id and
# reason orphan_timeout; their original disposition stays pending, so they are excluded here.
REJECTED_QUARANTINED: LiteralString = """
WITH rejected AS (
    SELECT batch_id, source, count(*) AS n
    FROM audit.record_dispositions
    WHERE batch_id = ANY(%(batch_ids)s) AND disposition = 'rejected'
    GROUP BY batch_id, source
),
quarantined AS (
    SELECT batch_id, source, count(*) AS n
    FROM staging.rejected_records
    WHERE batch_id = ANY(%(batch_ids)s) AND reason <> 'orphan_timeout'
    GROUP BY batch_id, source
),
paired AS (
    SELECT left(coalesce(r.batch_id, q.batch_id), 12) || ':' || coalesce(r.source, q.source)
               AS record_key,
           coalesce(r.n, 0) AS rejected_rows, coalesce(q.n, 0) AS quarantined_rows
    FROM rejected r FULL JOIN quarantined q USING (batch_id, source)
)
SELECT '', coalesce(sum(abs(rejected_rows - quarantined_rows)), 0),
       coalesce(sum(rejected_rows), 0),
       coalesce((array_agg(record_key ORDER BY record_key)
                 FILTER (WHERE rejected_rows <> quarantined_rows))[1:10], '{}'),
       NULL::numeric
FROM paired
"""

ACCEPTED_LOADED: LiteralString = """
WITH accepted AS (
    SELECT DISTINCT source, record_key
    FROM audit.record_dispositions
    WHERE batch_id = ANY(%(batch_ids)s) AND disposition = 'accepted'
),
flagged AS (
    SELECT a.source || ':' || a.record_key AS record_key,
           CASE a.source
               WHEN 'drivers' THEN NOT EXISTS (
                   SELECT 1 FROM analytics.drivers x WHERE x.driver_id = a.record_key)
               WHEN 'orders' THEN NOT EXISTS (
                   SELECT 1 FROM analytics.orders x WHERE x.order_id = a.record_key)
               WHEN 'delivery_events' THEN NOT EXISTS (
                   SELECT 1 FROM analytics.delivery_events x WHERE x.event_id = a.record_key)
               ELSE NOT EXISTS (
                   SELECT 1 FROM analytics.driver_shift_events x
                   WHERE x.event_id = a.record_key)
           END AS bad
    FROM accepted a
)
SELECT '', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

PENDING_PARENT_MISSING: LiteralString = """
WITH flagged AS (
    SELECT p.source || ':' || p.record_key AS record_key,
           CASE p.missing_reference
               WHEN 'order' THEN EXISTS (
                   SELECT 1 FROM analytics.orders o WHERE o.order_id = p.fields ->> 'order_id')
               ELSE EXISTS (
                   SELECT 1 FROM analytics.drivers d
                   WHERE d.driver_id = p.fields ->> 'driver_id')
           END AS bad
    FROM staging.pending_events p
)
SELECT '', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

ORDERS_REQUIRED_BY_STATUS: LiteralString = """
WITH flagged AS (
    SELECT order_id AS record_key,
           (status IN ('assigned', 'picked_up', 'delivered') AND driver_id IS NULL)
           OR (status IN ('picked_up', 'delivered') AND pickup_ts IS NULL) AS bad
    FROM analytics.orders
)
SELECT '', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

ORDERS_POSITIVE_AMOUNTS: LiteralString = """
WITH flagged AS (
    SELECT order_id AS record_key, distance_km <= 0 OR fare_amount <= 0 AS bad
    FROM analytics.orders
)
SELECT '', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

GPS_PING_UNIQUE: LiteralString = """
WITH groups AS (
    SELECT count(*) AS n, max(event_id) AS record_key
    FROM analytics.delivery_events
    WHERE event_type = 'gps_ping'
    GROUP BY driver_id, event_ts
)
SELECT '', coalesce(sum(n - 1) FILTER (WHERE n > 1), 0), coalesce(sum(n), 0),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE n > 1))[1:10], '{}'),
       NULL::numeric
FROM groups
"""

METRICS_MATCH_FACTS: LiteralString = """
WITH days AS (
    SELECT u.d AS metric_date,
           (u.d::timestamp AT TIME ZONE %(tz)s) AS day_start,
           ((u.d + 1)::timestamp AT TIME ZONE %(tz)s) AS day_end
    FROM unnest(%(dates)s::date[]) AS u(d)
),
created AS (
    SELECT days.metric_date, o.city, count(*) AS total_orders,
           count(*) FILTER (WHERE o.status = 'cancelled') AS cancelled_orders
    FROM days JOIN analytics.orders o
      ON o.order_created_ts >= days.day_start AND o.order_created_ts < days.day_end
    GROUP BY days.metric_date, o.city
),
delivered AS (
    SELECT days.metric_date, o.city, count(*) AS delivered_orders,
           count(*) FILTER (WHERE o.delivery_ts > o.promised_ts) AS late_orders
    FROM days JOIN analytics.orders o
      ON o.status = 'delivered'
     AND o.delivery_ts >= days.day_start AND o.delivery_ts < days.day_end
    GROUP BY days.metric_date, o.city
),
expected AS (
    SELECT coalesce(c.metric_date, v.metric_date) AS metric_date,
           coalesce(c.city, v.city) AS city,
           coalesce(c.total_orders, 0) AS total_orders,
           coalesce(c.cancelled_orders, 0) AS cancelled_orders,
           coalesce(v.delivered_orders, 0) AS delivered_orders,
           coalesce(v.late_orders, 0) AS late_orders
    FROM created c FULL JOIN delivered v USING (metric_date, city)
),
actual AS (
    SELECT metric_date, city, total_orders::bigint AS total_orders,
           cancelled_orders::bigint AS cancelled_orders,
           delivered_orders::bigint AS delivered_orders, late_orders::bigint AS late_orders
    FROM analytics.daily_delivery_metrics
    WHERE metric_date = ANY(%(dates)s::date[])
),
paired AS (
    SELECT coalesce(e.metric_date, a.metric_date)::text || ':' || coalesce(e.city, a.city)
               AS record_key,
           (e.total_orders, e.cancelled_orders, e.delivered_orders, e.late_orders)
               IS DISTINCT FROM
           (a.total_orders, a.cancelled_orders, a.delivered_orders, a.late_orders) AS bad
    FROM expected e FULL JOIN actual a USING (metric_date, city)
)
SELECT '', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM paired
"""

# Covered columns: orders.order_created_ts, pickup_ts, delivery_ts, source_updated_at;
# delivery_events.event_ts; driver_shift_events.event_ts. promised_ts is a planned time and is
# excluded (see docs/data_contracts.md). Each record is compared with the window of the batch
# that accepted its current version.
NO_FUTURE_TIMESTAMPS: LiteralString = """
WITH batches AS (
    SELECT batch_id, min(batch_sequence) AS batch_sequence, max(window_end) AS window_end
    FROM audit.pipeline_runs
    WHERE dataset = %(dataset)s GROUP BY batch_id
),
latest_orders AS (
    SELECT DISTINCT ON (d.record_key) d.record_key, d.batch_id, b.window_end
    FROM audit.record_dispositions d JOIN batches b USING (batch_id)
    WHERE d.source = 'orders' AND d.disposition = 'accepted'
    ORDER BY d.record_key, b.batch_sequence DESC
),
stamps AS (
    SELECT 'orders:' || l.record_key AS record_key, l.window_end,
           greatest(o.order_created_ts, o.pickup_ts, o.delivery_ts, o.source_updated_at)
               AS newest
    FROM latest_orders l JOIN analytics.orders o ON o.order_id = l.record_key
    WHERE l.batch_id = ANY(%(batch_ids)s)
    UNION ALL
    SELECT 'delivery_events:' || d.record_key, b.window_end, e.event_ts
    FROM audit.record_dispositions d JOIN batches b USING (batch_id)
    JOIN analytics.delivery_events e ON e.event_id = d.record_key
    WHERE d.batch_id = ANY(%(batch_ids)s) AND d.source = 'delivery_events'
      AND d.disposition = 'accepted'
    UNION ALL
    SELECT 'shift_events:' || d.record_key, b.window_end, s.event_ts
    FROM audit.record_dispositions d JOIN batches b USING (batch_id)
    JOIN analytics.driver_shift_events s ON s.event_id = d.record_key
    WHERE d.batch_id = ANY(%(batch_ids)s) AND d.source = 'shift_events'
      AND d.disposition = 'accepted'
),
flagged AS (
    SELECT record_key, coalesce(newest >= window_end, false) AS bad FROM stamps
)
SELECT '', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

FRESHNESS_LAG: LiteralString = """
WITH latest AS (
    SELECT batch_id, min(batch_sequence) AS batch_sequence, max(window_end) AS window_end
    FROM audit.pipeline_runs
    WHERE dataset = %(dataset)s AND batch_id = ANY(%(batch_ids)s)
    GROUP BY batch_id
    ORDER BY batch_sequence DESC
    LIMIT 1
),
newest AS (
    SELECT max(e.event_ts) AS event_ts
    FROM latest l
    JOIN audit.record_dispositions d ON d.batch_id = l.batch_id
    JOIN analytics.delivery_events e ON e.event_id = d.record_key
    WHERE d.source = 'delivery_events' AND d.disposition = 'accepted'
)
SELECT '', 0, (SELECT count(*) FROM latest),
       coalesce((SELECT ARRAY[left(batch_id, 12)] FROM latest), '{}'),
       (SELECT extract(epoch FROM l.window_end - n.event_ts) / 3600
        FROM latest l CROSS JOIN newest n)
"""

KNOWN_CITIES: LiteralString = """
WITH flagged AS (
    SELECT 'orders:' || order_id AS record_key, city <> ALL(%(cities)s::text[]) AS bad
    FROM analytics.orders
    UNION ALL
    SELECT 'drivers:' || driver_id, city <> ALL(%(cities)s::text[])
    FROM analytics.drivers
)
SELECT '', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

# A driver-day is excluded while the driver's session is still open: the driver's last
# online/offline event is an `online` that began before the day ended. Its offline has not
# arrived yet (it falls after the batch cutoff, or after the end of the data), so the busy
# minutes are counted but the online minutes are not yet. Those days are incomplete, not
# inconsistent; they are re-evaluated when the offline arrives.
UTILISATION_ABOVE_100: LiteralString = """
WITH on_off AS (
    SELECT driver_id, event_type, event_ts,
           lead(event_type) OVER (
               PARTITION BY driver_id ORDER BY event_ts, event_id COLLATE "C") AS next_type
    FROM analytics.driver_shift_events
    WHERE event_type IN ('online', 'offline')
),
open_sessions AS (
    SELECT driver_id, event_ts AS opened_at
    FROM on_off
    WHERE event_type = 'online' AND next_type IS NULL
),
flagged AS (
    SELECT p.driver_id || '@' || p.metric_date AS record_key,
           coalesce(p.utilisation_pct > 100, false)
           AND NOT EXISTS (
               SELECT 1 FROM open_sessions s
               WHERE s.driver_id = p.driver_id
                 AND s.opened_at < ((p.metric_date + 1)::timestamp AT TIME ZONE %(tz)s)
           ) AS bad
    FROM analytics.driver_performance p
)
SELECT '', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""

GPS_SPEED_IN_ANALYTICS: LiteralString = """
WITH steps AS (
    SELECT event_id, event_ts, lat, lon,
           lag(event_ts) OVER w AS prev_ts, lag(lat) OVER w AS prev_lat,
           lag(lon) OVER w AS prev_lon
    FROM analytics.delivery_events
    WHERE event_type = 'gps_ping'
    WINDOW w AS (PARTITION BY driver_id ORDER BY event_ts, event_id)
),
flagged AS (
    SELECT event_id AS record_key,
           CASE WHEN prev_ts IS NOT NULL AND event_ts > prev_ts THEN
               2 * 6371.0088 * asin(sqrt(
                   power(sin(radians(lat - prev_lat) / 2), 2)
                   + cos(radians(prev_lat)) * cos(radians(lat))
                     * power(sin(radians(lon - prev_lon) / 2), 2)))
               / (extract(epoch FROM event_ts - prev_ts) / 3600) > %(max_speed_kmh)s
           ELSE false END AS bad
    FROM steps
)
SELECT '', count(*) FILTER (WHERE bad), count(*),
       coalesce((array_agg(record_key ORDER BY record_key) FILTER (WHERE bad))[1:10], '{}'),
       NULL::numeric
FROM flagged
"""
