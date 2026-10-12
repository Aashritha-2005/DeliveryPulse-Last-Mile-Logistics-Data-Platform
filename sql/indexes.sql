-- DeliveryPulse analytics indexes. Each index serves a named query in
-- sql/analytics.sql or the metric refresh in src/database/metrics.py; the measured effect of
-- each is recorded in docs/benchmarks.md (before/after, EXPLAIN (ANALYZE, BUFFERS)).
--
-- Applied at first initialisation (init script, both databases) and by `make indexes-create`.
-- Idempotent. Run as the bootstrap superuser; the indexes are created by, and owned by, the
-- table owner.

SET ROLE deliverypulse_owner;

-- Orders created in a reporting-day range: daily totals and cancellations (refresh), peak_hours.
CREATE INDEX IF NOT EXISTS orders_created_ts_idx
    ON analytics.orders (order_created_ts);

-- Delivered orders in a reporting-day range: avg_delivery_time_by_city, top_bottom_drivers,
-- order_event_summary, late counts, driver deliveries and busy intervals (refresh).
CREATE INDEX IF NOT EXISTS orders_delivered_ts_idx
    ON analytics.orders (delivery_ts) WHERE status = 'delivered';

-- Events of a set of orders, aggregated to order grain: order_event_summary.
CREATE INDEX IF NOT EXISTS delivery_events_order_id_idx
    ON analytics.delivery_events (order_id);

-- Shift events in a time window: the refresh's shift_window filters on event_ts only (the
-- refreshed days +-1 day), so the index leads with event_ts. It replaces the earlier
-- (driver_id, event_ts) index, which that filter cannot use (measured: Seq Scan, 2026-10-12).
DROP INDEX IF EXISTS analytics.driver_shift_events_driver_ts_idx;
CREATE INDEX IF NOT EXISTS driver_shift_events_ts_idx
    ON analytics.driver_shift_events (event_ts);

-- Driver-days in a date range: driver_utilisation (the primary key leads with driver_id, so it
-- cannot serve a date range).
CREATE INDEX IF NOT EXISTS driver_performance_date_idx
    ON analytics.driver_performance (metric_date);

ANALYZE analytics.orders;
ANALYZE analytics.delivery_events;
ANALYZE analytics.driver_shift_events;
ANALYZE analytics.driver_performance;
ANALYZE analytics.daily_delivery_metrics;

RESET ROLE;
