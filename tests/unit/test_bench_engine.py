"""Unit tests of the benchmark's pandas engine: every rule of src/bench/spec.py, on handmade
input. The Spark engine is tested against this one in tests/integration/test_bench_spark.py.
"""

from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.bench import pandas_engine
from src.bench.reads import WEEK, pandas_week_counts, write_read_copies
from src.bench.spec import (
    EVENT_INPUT_COLUMNS,
    EVENTS_FILE,
    ORDER_INPUT_COLUMNS,
    ORDERS_FILE,
    ROW_ID,
    WorkloadResult,
)

pytestmark = pytest.mark.unit

ORDER = {
    "order_id": "ORD-1",
    "customer_id": "CUST-1",
    "driver_id": "DRV-1",
    "order_created_ts": "2026-09-10T02:30:00Z",
    "pickup_ts": "2026-09-10T02:40:00Z",
    "delivery_ts": "2026-09-10T02:50:00Z",
    "promised_ts": "2026-09-10T03:00:00Z",
    "distance_km": "1.5",
    "status": "delivered",
    "city": "Delhi",
    "fare_amount": "50.0",
    "source_updated_at": "2026-09-10T02:50:00Z",
    ROW_ID: 1,
}
EVENT = {
    "event_id": "EVT-1",
    "order_id": "ORD-1",
    "driver_id": "DRV-1",
    "event_type": "gps_ping",
    "event_ts": "2026-09-10T02:35:00Z",
    "lat": "17.3",
    "lon": "78.5",
    ROW_ID: 1,
}


def frame(rows: list[dict[str, Any]], columns: tuple[str, ...]) -> pd.DataFrame:
    """A benchmark input frame: every contract column as text, row_id as int64."""
    data = {
        column: [str(row.get(column, "")) for row in rows] for column in columns if column != ROW_ID
    }
    data[ROW_ID] = [row[ROW_ID] for row in rows]
    return pd.DataFrame(data).astype({ROW_ID: "int64"})


def orders(*overrides: dict[str, Any]) -> pd.DataFrame:
    rows = [{**ORDER, **override, ROW_ID: index} for index, override in enumerate(overrides, 1)]
    return frame(rows, ORDER_INPUT_COLUMNS)


def events(*overrides: dict[str, Any]) -> pd.DataFrame:
    rows = [{**EVENT, **override, ROW_ID: index} for index, override in enumerate(overrides, 1)]
    return frame(rows, EVENT_INPUT_COLUMNS)


# ------------------------------------------------------------------------- orders ---


@pytest.mark.parametrize(
    "override",
    [
        pytest.param({"order_id": ""}, id="missing-order-id"),
        pytest.param({"city": ""}, id="missing-city"),
        pytest.param({"status": ""}, id="missing-status"),
        pytest.param({"order_created_ts": ""}, id="missing-created"),
        pytest.param({"order_created_ts": "2026-13-01T00:00:00Z"}, id="impossible-month"),
        pytest.param({"order_created_ts": "10/09/2026"}, id="wrong-format"),
        pytest.param({"promised_ts": "not-a-time"}, id="unparsable-promised"),
        pytest.param({"source_updated_at": ""}, id="missing-updated"),
        pytest.param({"status": "shipped"}, id="unknown-status"),
        pytest.param({"pickup_ts": "not-a-time"}, id="unparsable-pickup"),
        pytest.param({"delivery_ts": ""}, id="delivered-without-delivery"),
        pytest.param({"pickup_ts": ""}, id="delivered-without-pickup"),
        pytest.param({"delivery_ts": "2026-09-10T02:39:00Z"}, id="delivery-before-pickup"),
    ],
)
def test_invalid_orders_are_rejected(override: dict[str, Any]) -> None:
    clean, rejected, duplicates = pandas_engine.clean_orders(orders(override))

    assert (len(clean), rejected, duplicates) == (0, 1, 0)


@pytest.mark.parametrize(
    "override",
    [
        pytest.param({"status": "cancelled", "pickup_ts": "", "delivery_ts": ""}, id="cancelled"),
        pytest.param({"status": "created", "pickup_ts": "", "delivery_ts": ""}, id="created"),
        pytest.param({"delivery_ts": "2026-09-10T02:40:00Z"}, id="delivery-equals-pickup"),
        pytest.param({"driver_id": ""}, id="no-driver-is-allowed"),
    ],
)
def test_valid_orders_are_kept(override: dict[str, Any]) -> None:
    clean, rejected, duplicates = pandas_engine.clean_orders(orders(override))

    assert (len(clean), rejected, duplicates) == (1, 0, 0)


def test_only_the_newest_version_of_an_order_is_kept() -> None:
    clean, rejected, duplicates = pandas_engine.clean_orders(
        orders(
            {"source_updated_at": "2026-09-10T02:50:00Z", "city": "Delhi"},
            {"source_updated_at": "2026-09-10T03:50:00Z", "city": "Mumbai"},
            {"source_updated_at": "2026-09-10T01:50:00Z", "city": "Chennai"},
        )
    )

    assert (len(clean), rejected, duplicates) == (1, 0, 2)
    assert clean["city"].tolist() == ["Mumbai"]


def test_an_equal_source_updated_at_is_broken_by_the_later_row() -> None:
    clean, _, duplicates = pandas_engine.clean_orders(orders({"city": "Delhi"}, {"city": "Mumbai"}))

    assert duplicates == 1
    assert clean["city"].tolist() == ["Mumbai"]


def test_order_days_use_the_reporting_date_and_count_late_and_cancelled() -> None:
    clean, _, _ = pandas_engine.clean_orders(
        orders(
            {"order_id": "ORD-1"},  # delivered on time, 600 s
            {
                "order_id": "ORD-2",
                "delivery_ts": "2026-09-10T03:10:00Z",  # after promised: late, 1800 s
            },
            {
                "order_id": "ORD-3",
                "status": "cancelled",
                "pickup_ts": "",
                "delivery_ts": "",
            },
            {
                "order_id": "ORD-4",
                "order_created_ts": "2026-09-10T18:31:00Z",  # 2026-09-11 in Asia/Kolkata
                "city": "Mumbai",
                "status": "created",
                "pickup_ts": "",
                "delivery_ts": "",
            },
        )
    )

    assert pandas_engine.order_days(clean) == [
        ("2026-09-10", "Delhi", 3, 2, 1, 1, 2400),
        ("2026-09-11", "Mumbai", 1, 0, 0, 0, 0),
    ]


# ------------------------------------------------------------------------- events ---


@pytest.mark.parametrize(
    "override",
    [
        pytest.param({"event_id": ""}, id="missing-event-id"),
        pytest.param({"order_id": ""}, id="missing-order-id"),
        pytest.param({"driver_id": ""}, id="missing-driver-id"),
        pytest.param({"event_ts": "not-a-time"}, id="unparsable-ts"),
        pytest.param({"event_type": "landed"}, id="unknown-type"),
        pytest.param({"lat": ""}, id="ping-without-lat"),
        pytest.param({"lat": "91.0"}, id="lat-out-of-range"),
        pytest.param({"lon": "-181.0"}, id="lon-out-of-range"),
        pytest.param({"lat": "north"}, id="lat-not-a-number"),
    ],
)
def test_invalid_events_are_rejected(override: dict[str, Any]) -> None:
    clean, rejected, duplicates = pandas_engine.clean_events(events(override))

    assert (len(clean), rejected, duplicates) == (0, 1, 0)


def test_a_non_ping_event_needs_no_coordinates() -> None:
    clean, rejected, _ = pandas_engine.clean_events(
        events({"event_type": "delivered", "lat": "", "lon": ""})
    )

    assert (len(clean), rejected) == (1, 0)


def test_a_replayed_event_id_is_a_duplicate() -> None:
    clean, rejected, duplicates = pandas_engine.clean_events(
        events({"event_id": "EVT-1"}, {"event_id": "EVT-1"}, {"event_id": "EVT-2"})
    )

    assert (len(clean), rejected, duplicates) == (2, 0, 1)


def test_gps_flags_impossible_speed_and_non_positive_intervals_per_driver() -> None:
    clean, _, _ = pandas_engine.clean_events(
        # one driver: a plausible step, then a 1,000 km jump in a minute, then a repeated time
        events(
            {
                "event_id": "EVT-1",
                "event_ts": "2026-09-10T02:30:00Z",
                "lat": "17.30",
                "lon": "78.50",
            },
            {
                "event_id": "EVT-2",
                "event_ts": "2026-09-10T02:31:00Z",
                "lat": "17.31",
                "lon": "78.51",
            },
            {
                "event_id": "EVT-3",
                "event_ts": "2026-09-10T02:32:00Z",
                "lat": "26.00",
                "lon": "78.51",
            },
            {
                "event_id": "EVT-4",
                "event_ts": "2026-09-10T02:32:00Z",
                "lat": "26.00",
                "lon": "78.51",
            },
            # another driver: its first ping is never flagged
            {
                "event_id": "EVT-5",
                "driver_id": "DRV-2",
                "event_ts": "2026-09-10T02:33:00Z",
                "lat": "12.00",
                "lon": "77.00",
            },
        )
    )

    flagged = pandas_engine.flag_gps(clean)

    assert clean.loc[flagged, "event_id"].tolist() == ["EVT-3", "EVT-4"]


def test_event_days_count_events_pings_and_flags() -> None:
    clean, _, _ = pandas_engine.clean_events(
        events(
            {"event_id": "EVT-1"},
            {"event_id": "EVT-2", "event_type": "delivered", "lat": "", "lon": ""},
            {"event_id": "EVT-3", "event_ts": "2026-09-10T18:31:00Z"},  # next reporting day
        )
    )

    days = pandas_engine.event_days(clean, pandas_engine.flag_gps(clean))

    assert days == [("2026-09-10", 2, 1, 0), ("2026-09-11", 1, 1, 0)]


# ------------------------------------------------------- workload and read layouts ---


def write_shard(target: Path, order_rows: pd.DataFrame, event_rows: pd.DataFrame) -> Path:
    target.mkdir(parents=True, exist_ok=True)
    order_rows.to_parquet(target / ORDERS_FILE, index=False)
    event_rows.to_parquet(target / EVENTS_FILE, index=False)
    return target


def test_run_workload_reads_several_shards_and_reports_every_count(tmp_path: Path) -> None:
    first = write_shard(
        tmp_path / "shard-0001", orders({"order_id": "ORD-1"}), events({"event_id": "EVT-1"})
    )
    second = write_shard(
        tmp_path / "shard-0002",
        orders({"order_id": "ORD-2"}, {"order_id": "ORD-3", "status": "shipped"}),
        events({"event_id": "EVT-9", "lat": "91.0"}),
    )

    result = pandas_engine.run_workload([first, second])

    assert result.orders_input == 3
    assert result.orders_rejected == 1
    assert result.orders_clean == 2
    assert result.events_input == 2
    assert result.events_rejected == 1
    assert result.events_clean == 1
    assert sum(row[2] for row in result.order_days) == 2


def test_workload_result_round_trips_through_json() -> None:
    result = WorkloadResult(
        orders_input=1,
        order_days=[("2026-09-10", "Delhi", 1, 1, 0, 0, 600)],
        event_days=[("2026-09-10", 2, 1, 0)],
    )

    assert WorkloadResult.from_json(result.to_json()) == result


def test_read_copies_hold_the_same_week_counts_in_both_layouts(tmp_path: Path) -> None:
    shard = write_shard(
        tmp_path / "shard-0001",
        orders(),
        events(
            {"event_id": "EVT-1", "event_ts": "2026-09-10T02:30:00Z"},
            {"event_id": "EVT-2", "event_ts": "2026-09-10T02:31:00Z", "event_type": "delivered"},
            {"event_id": "EVT-3", "event_ts": "2026-09-30T02:31:00Z"},  # outside the week
            {"event_id": "EVT-4", "event_ts": "broken"},  # dropped: unparsable
        ),
    )

    single, partitioned = write_read_copies([shard], tmp_path / "reads")

    assert single.is_file() and partitioned.is_dir()
    assert {path.parent.name for path in partitioned.rglob("*.parquet")} <= {
        f"date={day}" for day in (*WEEK, "2026-09-30")
    }
    assert pandas_week_counts(single) == pandas_week_counts(partitioned) == [("2026-09-10", 2, 1)]
    assert pq.read_table(single).num_rows == 3  # the unparsable row is not in the copies
    assert pa.types.is_timestamp(pq.read_schema(single).field("event_ts").type)
