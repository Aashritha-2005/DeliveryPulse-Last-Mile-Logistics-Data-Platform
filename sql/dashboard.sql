-- DeliveryPulse dashboard queries (src/dashboard). Read by the read-only role, `analytics` only.
-- Definitions: docs/data_contracts.md section 5 and docs/data_model.md. Reporting dates and
-- hours are in the reporting timezone (%(tz)s = Asia/Kolkata); stored timestamps are UTC.
--
-- Parameters, always bound by the driver: %(tz)s, %(start_date)s and %(end_date)s (reporting
-- dates, inclusive), %(cities)s (text array), %(min_deliveries)s, %(limit)s.

-- name: data_period
-- First and last reporting date of order creation: the default date range.
SELECT (min(o.order_created_ts) AT TIME ZONE %(tz)s)::date AS first_date,
       (max(o.order_created_ts) AT TIME ZONE %(tz)s)::date AS last_date
FROM analytics.orders o;

-- name: cities
SELECT c.city FROM analytics.dim_city c ORDER BY c.city;

-- name: kpis
-- Counts for the headline figures; the rates are computed from these counts in Python.
-- Orders created and cancelled use order_created_ts; delivered, late and delivery seconds use
-- delivery_ts (delivered orders only).
WITH created AS (
    SELECT count(*) AS orders_created,
           count(*) FILTER (WHERE o.status = 'cancelled') AS cancelled_orders
    FROM analytics.orders o
    WHERE o.order_created_ts >= (%(start_date)s::date::timestamp AT TIME ZONE %(tz)s)
      AND o.order_created_ts < ((%(end_date)s::date + 1)::timestamp AT TIME ZONE %(tz)s)
      AND o.city = ANY(%(cities)s::text[])
),
delivered AS (
    SELECT count(*) AS delivered_orders,
           count(*) FILTER (WHERE o.delivery_ts > o.promised_ts) AS late_orders,
           coalesce(sum(extract(epoch FROM o.delivery_ts - o.pickup_ts)), 0) AS delivery_seconds
    FROM analytics.orders o
    WHERE o.status = 'delivered'
      AND o.delivery_ts >= (%(start_date)s::date::timestamp AT TIME ZONE %(tz)s)
      AND o.delivery_ts < ((%(end_date)s::date + 1)::timestamp AT TIME ZONE %(tz)s)
      AND o.city = ANY(%(cities)s::text[])
)
SELECT c.orders_created, c.cancelled_orders, d.delivered_orders, d.late_orders,
       d.delivery_seconds
FROM created c CROSS JOIN delivered d;

-- name: delivery_time_trend
-- Average delivery minutes per reporting day and city (basis delivery_ts), as stored.
SELECT m.metric_date, m.city, m.avg_delivery_minutes, m.delivered_orders
FROM analytics.daily_delivery_metrics m
WHERE m.metric_date BETWEEN %(start_date)s AND %(end_date)s
  AND m.city = ANY(%(cities)s::text[])
  AND m.delivered_orders > 0
ORDER BY m.metric_date, m.city;

-- name: late_rate_by_city
-- Late-delivery rate per city over the range, recomputed from the stored counts (never an
-- average of daily rates). Basis delivery_ts; cancelled orders excluded.
SELECT m.city,
       sum(m.delivered_orders) AS delivered_orders,
       sum(m.late_orders) AS late_orders
FROM analytics.daily_delivery_metrics m
WHERE m.metric_date BETWEEN %(start_date)s AND %(end_date)s
  AND m.city = ANY(%(cities)s::text[])
GROUP BY m.city
ORDER BY m.city;

-- name: peak_hours
-- Orders created per hour of day in the reporting timezone (basis order_created_ts).
SELECT extract(hour FROM o.order_created_ts AT TIME ZONE %(tz)s)::integer AS hour_of_day,
       count(*) AS orders
FROM analytics.orders o
WHERE o.order_created_ts >= (%(start_date)s::date::timestamp AT TIME ZONE %(tz)s)
  AND o.order_created_ts < ((%(end_date)s::date + 1)::timestamp AT TIME ZONE %(tz)s)
  AND o.city = ANY(%(cities)s::text[])
GROUP BY 1
ORDER BY 1;

-- name: top_drivers
-- Fastest drivers by average delivery minutes (delivered orders, basis delivery_ts, the order's
-- city), among drivers with at least min_deliveries in the range; utilisation is weighted over
-- the range (sum busy / sum online minutes, driver-days with paired sessions only).
WITH per_driver AS (
    SELECT o.driver_id,
           count(*) AS deliveries,
           round(avg(extract(epoch FROM o.delivery_ts - o.pickup_ts)) / 60, 2)
               AS avg_delivery_minutes
    FROM analytics.orders o
    WHERE o.status = 'delivered'
      AND o.delivery_ts >= (%(start_date)s::date::timestamp AT TIME ZONE %(tz)s)
      AND o.delivery_ts < ((%(end_date)s::date + 1)::timestamp AT TIME ZONE %(tz)s)
      AND o.city = ANY(%(cities)s::text[])
    GROUP BY o.driver_id
    HAVING count(*) >= %(min_deliveries)s
),
utilisation AS (
    SELECT p.driver_id,
           round(sum(p.busy_minutes) * 100 / nullif(sum(p.online_minutes), 0), 2)
               AS utilisation_pct
    FROM analytics.driver_performance p
    WHERE p.metric_date BETWEEN %(start_date)s AND %(end_date)s
      AND p.online_minutes IS NOT NULL
    GROUP BY p.driver_id
)
SELECT d.driver_id, r.city, d.deliveries, d.avg_delivery_minutes, u.utilisation_pct
FROM per_driver d
JOIN analytics.drivers r ON r.driver_id = d.driver_id
LEFT JOIN utilisation u ON u.driver_id = d.driver_id
ORDER BY d.avg_delivery_minutes, d.driver_id
LIMIT %(limit)s;
