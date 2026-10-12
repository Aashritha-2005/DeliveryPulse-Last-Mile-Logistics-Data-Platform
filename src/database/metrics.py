"""Generated dimensions and daily aggregates, recomputed per reporting date.

Definitions: docs/data_contracts.md, section 5, and docs/data_model.md. Every reporting date is
the calendar date in Asia/Kolkata; every bucket is a half-open range on the stored UTC
timestamps, so plain B-tree indexes apply.

* ``affected_dates`` lists the reporting dates a batch can change: dates of every inserted or
  updated order version (old and new: created, pickup..delivery) and of every inserted shift
  event (+-1 day, because a session may cross midnight). Delivery events feed no metric.
* ``refresh_metrics`` deletes the metric rows of those dates and recomputes them from the full
  analytics tables, so an incremental refresh equals a full rebuild and a re-run changes
  nothing. It runs inside the batch transaction of the load.

Usage (full rebuild of every date, one transaction)::

    uv run python -m src.database.metrics
"""

import logging
import sys
from collections.abc import Iterable, Sequence
from datetime import date, timedelta
from typing import Any, LiteralString

import psycopg

from src.config import ConfigError, load_database_settings
from src.contract import (
    CITY_STATES,
    MAX_SESSION_SECONDS,
    REPORTING_TIMEZONE,
    UNKNOWN_STATE,
    reporting_date,
)
from src.database.postgres_state import Connection
from src.utils.logging import configure_logging

logger = logging.getLogger("deliverypulse.metrics")

# Both SELECTs start with the same `days` CTE: the reporting days of %(dates)s as half-open UTC
# ranges [day_start, day_end). Each SELECT is one complete literal (no SQL string building).

DAILY_METRICS_SELECT: LiteralString = """WITH
    days AS (
        SELECT u.d AS metric_date,
               (u.d::timestamp AT TIME ZONE %(tz)s) AS day_start,
               ((u.d + 1)::timestamp AT TIME ZONE %(tz)s) AS day_end
        FROM unnest(%(dates)s::date[]) AS u(d)
    ),
    created AS (
        SELECT days.metric_date, o.city,
               count(*) AS total_orders,
               count(*) FILTER (WHERE o.status = 'cancelled') AS cancelled_orders
        FROM days
        JOIN analytics.orders o
          ON o.order_created_ts >= days.day_start AND o.order_created_ts < days.day_end
        GROUP BY days.metric_date, o.city
    ),
    delivered AS (
        SELECT days.metric_date, o.city,
               count(*) AS delivered_orders,
               count(*) FILTER (WHERE o.delivery_ts > o.promised_ts) AS late_orders,
               sum(extract(epoch FROM o.delivery_ts - o.pickup_ts)) AS delivery_seconds
        FROM days
        JOIN analytics.orders o
          ON o.status = 'delivered'
         AND o.delivery_ts >= days.day_start AND o.delivery_ts < days.day_end
        GROUP BY days.metric_date, o.city
    )
    SELECT coalesce(c.metric_date, d.metric_date) AS metric_date,
           coalesce(c.city, d.city) AS city,
           coalesce(c.total_orders, 0)::integer AS total_orders,
           round(d.delivery_seconds / (60 * d.delivered_orders), 2) AS avg_delivery_minutes,
           round(d.late_orders::numeric / d.delivered_orders, 4) AS late_rate,
           round(c.cancelled_orders::numeric / nullif(c.total_orders, 0), 4) AS cancelled_rate,
           coalesce(d.delivered_orders, 0)::integer AS delivered_orders,
           coalesce(d.late_orders, 0)::integer AS late_orders,
           coalesce(c.cancelled_orders, 0)::integer AS cancelled_orders
    FROM created c
    FULL JOIN delivered d ON c.metric_date = d.metric_date AND c.city = d.city
"""

DRIVER_PERFORMANCE_SELECT: LiteralString = """WITH
    days AS (
        SELECT u.d AS metric_date,
               (u.d::timestamp AT TIME ZONE %(tz)s) AS day_start,
               ((u.d + 1)::timestamp AT TIME ZONE %(tz)s) AS day_end
        FROM unnest(%(dates)s::date[]) AS u(d)
    ),
    bounds AS (
        SELECT min(day_start) - interval '1 day' AS lo, max(day_end) + interval '1 day' AS hi
        FROM days
    ),
    deliveries AS (
        SELECT days.metric_date, o.driver_id,
               count(*) AS deliveries,
               sum(extract(epoch FROM o.delivery_ts - o.pickup_ts)) AS delivery_seconds
        FROM days
        JOIN analytics.orders o
          ON o.status = 'delivered'
         AND o.delivery_ts >= days.day_start AND o.delivery_ts < days.day_end
        GROUP BY days.metric_date, o.driver_id
    ),
    pieces AS (
        SELECT days.metric_date, o.driver_id,
               greatest(o.pickup_ts, days.day_start) AS s,
               least(o.delivery_ts, days.day_end) AS e
        FROM days
        JOIN analytics.orders o
          ON o.status = 'delivered'
         AND o.pickup_ts < days.day_end AND o.delivery_ts > days.day_start
    ),
    flagged AS (
        SELECT metric_date, driver_id, s, e,
               CASE WHEN s <= max(e) OVER (
                        PARTITION BY metric_date, driver_id ORDER BY s, e
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                    THEN 0 ELSE 1 END AS starts_island
        FROM pieces
    ),
    islands AS (
        SELECT metric_date, driver_id, s, e,
               sum(starts_island) OVER (
                   PARTITION BY metric_date, driver_id ORDER BY s, e
                   ROWS UNBOUNDED PRECEDING) AS island
        FROM flagged
    ),
    busy AS (
        SELECT metric_date, driver_id,
               sum(extract(epoch FROM island_end - island_start)) AS busy_seconds
        FROM (
            SELECT metric_date, driver_id, island, min(s) AS island_start, max(e) AS island_end
            FROM islands
            GROUP BY metric_date, driver_id, island
        ) AS merged
        GROUP BY metric_date, driver_id
    ),
    shift_window AS (
        SELECT se.driver_id, se.event_type, se.event_ts, se.event_id
        FROM analytics.driver_shift_events se
        CROSS JOIN bounds
        WHERE se.event_ts >= bounds.lo AND se.event_ts < bounds.hi
    ),
    onoff AS (
        SELECT driver_id, event_type, event_ts,
               lead(event_type) OVER w AS next_type,
               lead(event_ts) OVER w AS next_ts
        FROM shift_window
        WHERE event_type IN ('online', 'offline')
        WINDOW w AS (PARTITION BY driver_id ORDER BY event_ts, event_id COLLATE "C")
    ),
    sessions AS (
        SELECT driver_id, event_ts AS s, next_ts AS e
        FROM onoff
        WHERE event_type = 'online' AND next_type = 'offline'
          AND next_ts - event_ts <= make_interval(secs => %(max_session)s)
    ),
    break_events AS (
        SELECT driver_id, event_type, event_ts,
               lead(event_type) OVER w AS next_type,
               lead(event_ts) OVER w AS next_ts
        FROM shift_window
        WHERE event_type IN ('break_start', 'break_end')
        WINDOW w AS (PARTITION BY driver_id ORDER BY event_ts, event_id COLLATE "C")
    ),
    breaks AS (
        SELECT driver_id, event_ts AS s, next_ts AS e
        FROM break_events
        WHERE event_type = 'break_start' AND next_type = 'break_end'
          AND next_ts - event_ts <= make_interval(secs => %(max_session)s)
    ),
    session_day AS (
        SELECT days.metric_date, ses.driver_id,
               sum(extract(epoch FROM least(ses.e, days.day_end)
                                      - greatest(ses.s, days.day_start))) AS session_seconds
        FROM days
        JOIN sessions ses ON ses.s < days.day_end AND ses.e > days.day_start
        GROUP BY days.metric_date, ses.driver_id
    ),
    break_day AS (
        SELECT days.metric_date, b.driver_id,
               sum(extract(epoch FROM least(b.e, ses.e, days.day_end)
                                      - greatest(b.s, ses.s, days.day_start))) AS break_seconds
        FROM days
        JOIN breaks b ON b.s < days.day_end AND b.e > days.day_start
        JOIN sessions ses ON ses.driver_id = b.driver_id AND ses.s < b.e AND ses.e > b.s
        WHERE least(b.e, ses.e, days.day_end) > greatest(b.s, ses.s, days.day_start)
        GROUP BY days.metric_date, b.driver_id
    ),
    driver_days AS (
        SELECT metric_date, driver_id FROM deliveries
        UNION
        SELECT metric_date, driver_id FROM busy
        UNION
        SELECT metric_date, driver_id FROM session_day
    ),
    combined AS (
        SELECT k.driver_id, k.metric_date,
               coalesce(dl.deliveries, 0) AS deliveries,
               dl.delivery_seconds,
               coalesce(b.busy_seconds, 0) AS busy_seconds,
               sd.session_seconds - coalesce(bd.break_seconds, 0) AS online_seconds
        FROM driver_days k
        LEFT JOIN deliveries dl USING (metric_date, driver_id)
        LEFT JOIN busy b USING (metric_date, driver_id)
        LEFT JOIN session_day sd USING (metric_date, driver_id)
        LEFT JOIN break_day bd USING (metric_date, driver_id)
    )
    SELECT driver_id, metric_date,
           deliveries::integer AS deliveries,
           round(delivery_seconds / (60 * nullif(deliveries, 0)), 2) AS avg_delivery_minutes,
           CASE WHEN online_seconds > 0
                THEN round(busy_seconds * 100 / online_seconds, 2) END AS utilisation_pct,
           round(busy_seconds / 60, 2) AS busy_minutes,
           round(online_seconds / 60, 2) AS online_minutes
    FROM combined
"""

_DAILY_COLUMNS: LiteralString = (
    "metric_date, city, total_orders, avg_delivery_minutes, late_rate, cancelled_rate, "
    "delivered_orders, late_orders, cancelled_orders"
)
_DRIVER_COLUMNS: LiteralString = (
    "driver_id, metric_date, deliveries, avg_delivery_minutes, utilisation_pct, "
    "busy_minutes, online_minutes"
)

_DIM_DATE: LiteralString = """
    INSERT INTO analytics.dim_date
        (date_key, calendar_year, quarter, month, day_of_month, iso_day_of_week, iso_week,
         is_weekend)
    SELECT g.d::date,
           extract(year FROM g.d)::smallint,
           extract(quarter FROM g.d)::smallint,
           extract(month FROM g.d)::smallint,
           extract(day FROM g.d)::smallint,
           extract(isodow FROM g.d)::smallint,
           extract(week FROM g.d)::smallint,
           extract(isodow FROM g.d) IN (6, 7)
    FROM generate_series(%(first)s::timestamp, %(last)s::timestamp, interval '1 day') AS g(d)
    ON CONFLICT (date_key) DO NOTHING
"""
_DIM_DATE_OUTSIDE: LiteralString = (
    "DELETE FROM analytics.dim_date WHERE date_key < %(first)s OR date_key > %(last)s"
)


def metric_parameters(dates: Sequence[date]) -> dict[str, Any]:
    """Bound parameters shared by the metric SELECTs."""
    return {"dates": list(dates), "tz": REPORTING_TIMEZONE, "max_session": MAX_SESSION_SECONDS}


def _days_between(first: int, last: int) -> list[date]:
    start = reporting_date(first)
    end = reporting_date(last)
    return [start + timedelta(days=n) for n in range((end - start).days + 1)]


def affected_dates(
    order_versions: Iterable[tuple[int, int | None, int | None]],
    shift_event_times: Iterable[int],
) -> list[date]:
    """Reporting dates a batch can change, sorted.

    ``order_versions``: (created, pickup, delivery) epochs of every inserted or updated order
    version, old and new. ``shift_event_times``: epochs of every inserted shift event.
    """
    dates: set[date] = set()
    for created, pickup, delivery in order_versions:
        dates.add(reporting_date(created))
        if pickup is not None:
            dates.add(reporting_date(pickup))
        if delivery is not None:
            dates.add(reporting_date(delivery))
        if pickup is not None and delivery is not None and delivery >= pickup:
            dates.update(_days_between(pickup, delivery))
    for ts in shift_event_times:
        day = reporting_date(ts)
        dates.update({day - timedelta(days=1), day, day + timedelta(days=1)})
    return sorted(dates)


def refresh_date_dimension(cur: psycopg.Cursor[Any]) -> None:
    """Make dim_date exactly the continuous range of reporting dates with data.

    The range is ``all_dates``: first to last reporting date of any order timestamp or shift
    event. The incremental load and the full rebuild both apply this one rule, so dim_date
    never depends on the path (the affected dates of a batch, which include shift dates +-1
    day, are not used here).
    """
    dates = all_dates(cur)
    if not dates:
        cur.execute("DELETE FROM analytics.dim_date")
        return
    bounds = {"first": dates[0], "last": dates[-1]}
    cur.execute(_DIM_DATE_OUTSIDE, bounds)
    cur.execute(_DIM_DATE, bounds)


def refresh_dimensions(cur: psycopg.Cursor[Any], cities: Iterable[str]) -> None:
    """Reset dim_date to the data range and add ``cities`` to dim_city."""
    refresh_date_dimension(cur)
    names = sorted(set(cities))
    if names:
        states = [CITY_STATES.get(name, UNKNOWN_STATE) for name in names]
        cur.execute(
            "INSERT INTO analytics.dim_city (city, state) "
            "SELECT * FROM unnest(%s::text[], %s::text[]) ON CONFLICT (city) DO NOTHING",
            (names, states),
        )


def refresh_metrics(cur: psycopg.Cursor[Any], dates: Sequence[date]) -> None:
    """Recompute both aggregate tables for ``dates`` (delete, then insert)."""
    if not dates:
        return
    params = metric_parameters(dates)
    delete_daily: LiteralString = (
        "DELETE FROM analytics.daily_delivery_metrics WHERE metric_date = ANY(%(dates)s)"
    )
    cur.execute(delete_daily, params)
    insert_daily: LiteralString = (
        "INSERT INTO analytics.daily_delivery_metrics (" + _DAILY_COLUMNS + ") "
    )
    cur.execute(insert_daily + DAILY_METRICS_SELECT, params)
    delete_drivers: LiteralString = (
        "DELETE FROM analytics.driver_performance WHERE metric_date = ANY(%(dates)s)"
    )
    cur.execute(delete_drivers, params)
    insert_drivers: LiteralString = (
        "INSERT INTO analytics.driver_performance (" + _DRIVER_COLUMNS + ") "
    )
    cur.execute(insert_drivers + DRIVER_PERFORMANCE_SELECT, params)


def all_dates(cur: psycopg.Cursor[Any]) -> list[date]:
    """Every reporting date from the first to the last order timestamp or shift event."""
    row = cur.execute(
        "SELECT least(min(o.order_created_ts), (SELECT min(event_ts) "
        "FROM analytics.driver_shift_events)), "
        "greatest(max(coalesce(o.delivery_ts, o.order_created_ts)), (SELECT max(event_ts) "
        "FROM analytics.driver_shift_events)) FROM analytics.orders o"
    ).fetchone()
    if row is None or row[0] is None or row[1] is None:
        return []
    return _days_between(int(row[0].timestamp()), int(row[1].timestamp()))


def rebuild_all(conn: Connection) -> list[date]:
    """Full rebuild of dimensions and both aggregate tables, in one transaction."""
    with conn.transaction(), conn.cursor() as cur:
        dates = all_dates(cur)
        cur.execute("DELETE FROM analytics.daily_delivery_metrics")
        cur.execute("DELETE FROM analytics.driver_performance")
        cur.execute("SELECT city FROM analytics.orders UNION SELECT city FROM analytics.drivers")
        cities = [str(row[0]) for row in cur.fetchall()]
        refresh_dimensions(cur, cities)
        refresh_metrics(cur, dates)
    return dates


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point (`make metrics`); returns the process exit code."""
    del argv
    configure_logging()
    try:
        settings = load_database_settings("etl")
        with psycopg.connect(settings.conninfo(), autocommit=True) as conn:
            dates = rebuild_all(conn)
    except (ConfigError, psycopg.Error) as exc:
        logger.error("%s", exc)
        return 1
    if dates:
        logger.info("metrics rebuilt for %d dates (%s to %s)", len(dates), dates[0], dates[-1])
    else:
        logger.info("no data; nothing to rebuild")
    return 0


if __name__ == "__main__":
    sys.exit(main())
