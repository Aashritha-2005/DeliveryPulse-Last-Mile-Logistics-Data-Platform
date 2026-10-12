-- Drop the analytics indexes of sql/indexes.sql (for the measured "before" in
-- docs/benchmarks.md only), then refresh planner statistics. Run by `make indexes-drop` as the
-- bootstrap superuser; `make indexes-create` restores them.

SET ROLE deliverypulse_owner;

DROP INDEX IF EXISTS analytics.orders_created_ts_idx;
DROP INDEX IF EXISTS analytics.orders_delivered_ts_idx;
DROP INDEX IF EXISTS analytics.delivery_events_order_id_idx;
DROP INDEX IF EXISTS analytics.driver_shift_events_ts_idx;
DROP INDEX IF EXISTS analytics.driver_performance_date_idx;
-- Replaced on 2026-10-12 by driver_shift_events_ts_idx; dropped here too so a database
-- initialised with the earlier sql/indexes.sql reaches the same "before" state.
DROP INDEX IF EXISTS analytics.driver_shift_events_driver_ts_idx;

ANALYZE analytics.orders;
ANALYZE analytics.delivery_events;
ANALYZE analytics.driver_shift_events;
ANALYZE analytics.driver_performance;
ANALYZE analytics.daily_delivery_metrics;

RESET ROLE;
