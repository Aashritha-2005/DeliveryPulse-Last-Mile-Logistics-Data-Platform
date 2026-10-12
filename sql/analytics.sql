-- DeliveryPulse analyst queries. Definitions: docs/data_contracts.md
-- section 5 and docs/data_model.md. Reporting dates and hours are in the reporting timezone
-- (%(tz)s = Asia/Kolkata); stored timestamps are UTC.
--
-- Each query starts with a `-- name: <query>` line. Parameters are bound by the driver, never
-- formatted into the SQL: %(tz)s, %(start_date)s, %(end_date)s (reporting dates, inclusive),
-- %(day)s, %(min_deliveries)s, %(limit)s. `make analytics` runs them all as
-- deliverypulse_readonly with defaults derived from the loaded data; `make explain` measures
-- a subset.

-- name: avg_delivery_time_by_city
-- Average delivery time (pickup -> delivery, minutes) by city. Delivered orders only; basis
-- delivery_ts in [start_date, end_date].
SELECT o.city,
       count(*) AS delivered_orders,
       round(avg(extract(epoch FROM o.delivery_ts - o.pickup_ts)) / 60, 2) AS avg_minutes
FROM analytics.orders o
WHERE o.status = 'delivered'
  AND o.delivery_ts >= (%(start_date)s::date::timestamp AT TIME ZONE %(tz)s)
  AND o.delivery_ts < ((%(end_date)s::date + 1)::timestamp AT TIME ZONE %(tz)s)
GROUP BY o.city
ORDER BY avg_minutes, o.city;

-- name: late_rate_by_day
-- Late-delivery rate per reporting day across cities: late delivered / delivered (cancelled
-- excluded). Basis delivery_ts. Uses the aggregate table's counts (not averages of rates).
SELECT metric_date,
       sum(delivered_orders) AS delivered_orders,
       sum(late_orders) AS late_orders,
       round(sum(late_orders)::numeric / nullif(sum(delivered_orders), 0), 4) AS late_rate
FROM analytics.daily_delivery_metrics
WHERE metric_date BETWEEN %(start_date)s AND %(end_date)s
GROUP BY metric_date
ORDER BY metric_date;

-- name: peak_hours
-- Order volume by hour of day (reporting timezone), basis order_created_ts, with each hour's
-- share of the period and its rank.
WITH hourly AS (
    SELECT extract(hour FROM o.order_created_ts AT TIME ZONE %(tz)s)::integer AS hour_of_day,
           count(*) AS orders
    FROM analytics.orders o
    WHERE o.order_created_ts >= (%(start_date)s::date::timestamp AT TIME ZONE %(tz)s)
      AND o.order_created_ts < ((%(end_date)s::date + 1)::timestamp AT TIME ZONE %(tz)s)
    GROUP BY 1
)
SELECT hour_of_day,
       orders,
       round(orders * 100.0 / sum(orders) OVER (), 2) AS share_pct,
       rank() OVER (ORDER BY orders DESC) AS volume_rank
FROM hourly
ORDER BY hour_of_day;

-- name: top_bottom_drivers
-- Fastest and slowest drivers by average delivery minutes (delivered orders, basis
-- delivery_ts), among drivers with at least min_deliveries deliveries in the period.
WITH per_driver AS (
    SELECT o.driver_id,
           count(*) AS deliveries,
           round(avg(extract(epoch FROM o.delivery_ts - o.pickup_ts)) / 60, 2) AS avg_minutes
    FROM analytics.orders o
    WHERE o.status = 'delivered'
      AND o.delivery_ts >= (%(start_date)s::date::timestamp AT TIME ZONE %(tz)s)
      AND o.delivery_ts < ((%(end_date)s::date + 1)::timestamp AT TIME ZONE %(tz)s)
    GROUP BY o.driver_id
    HAVING count(*) >= %(min_deliveries)s
),
ranked AS (
    SELECT driver_id, deliveries, avg_minutes,
           dense_rank() OVER (ORDER BY avg_minutes ASC) AS fastest_rank,
           dense_rank() OVER (ORDER BY avg_minutes DESC) AS slowest_rank
    FROM per_driver
)
SELECT CASE WHEN fastest_rank <= %(limit)s THEN 'top' ELSE 'bottom' END AS band,
       driver_id, deliveries, avg_minutes, fastest_rank, slowest_rank
FROM ranked
WHERE fastest_rank <= %(limit)s OR slowest_rank <= %(limit)s
ORDER BY fastest_rank, driver_id;

-- name: driver_utilisation
-- Utilisation per driver over the period: sum of busy minutes / sum of online minutes x 100
-- (weighted; never an average of daily percentages). Only driver-days with paired online
-- sessions count. Above 100 means busy time outside paired sessions (inconsistent data).
SELECT p.driver_id,
       sum(p.deliveries) AS deliveries,
       sum(p.busy_minutes) AS busy_minutes,
       sum(p.online_minutes) AS online_minutes,
       round(sum(p.busy_minutes) * 100 / nullif(sum(p.online_minutes), 0), 2) AS utilisation_pct
FROM analytics.driver_performance p
WHERE p.metric_date BETWEEN %(start_date)s AND %(end_date)s
  AND p.online_minutes IS NOT NULL
GROUP BY p.driver_id
ORDER BY utilisation_pct DESC NULLS LAST, p.driver_id
LIMIT %(limit)s;

-- name: rolling_7_day
-- Rolling 7-day figures per city (the day and the 6 before it): orders (basis
-- order_created_ts), late rate (basis delivery_ts) and cancellation rate (basis
-- order_created_ts), computed from counts. The first 6 days of the range have shorter windows.
SELECT city,
       metric_date,
       total_orders,
       sum(total_orders) OVER w AS orders_7d,
       round((sum(late_orders) OVER w)::numeric
             / nullif(sum(delivered_orders) OVER w, 0), 4) AS late_rate_7d,
       round((sum(cancelled_orders) OVER w)::numeric
             / nullif(sum(total_orders) OVER w, 0), 4) AS cancelled_rate_7d
FROM analytics.daily_delivery_metrics
WHERE metric_date BETWEEN %(start_date)s AND %(end_date)s
WINDOW w AS (
    PARTITION BY city ORDER BY metric_date
    RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW
)
ORDER BY city, metric_date;

-- name: order_event_summary
-- Delivered orders of one reporting day with their delivery events. Events are aggregated to
-- order grain FIRST, so the one-to-many join can never multiply order counts (see
-- docs/data_model.md, section 3).
WITH day_orders AS (
    SELECT o.order_id, o.city
    FROM analytics.orders o
    WHERE o.status = 'delivered'
      AND o.delivery_ts >= (%(day)s::date::timestamp AT TIME ZONE %(tz)s)
      AND o.delivery_ts < ((%(day)s::date + 1)::timestamp AT TIME ZONE %(tz)s)
),
events_per_order AS (
    SELECT e.order_id,
           count(*) AS events,
           count(*) FILTER (WHERE e.event_type = 'gps_ping') AS pings
    FROM analytics.delivery_events e
    JOIN day_orders d ON d.order_id = e.order_id
    GROUP BY e.order_id
)
SELECT d.city,
       count(*) AS delivered_orders,
       sum(coalesce(ep.events, 0)) AS events,
       round(avg(coalesce(ep.pings, 0)), 2) AS avg_pings_per_order
FROM day_orders d
LEFT JOIN events_per_order ep ON ep.order_id = d.order_id
GROUP BY d.city
ORDER BY d.city;
