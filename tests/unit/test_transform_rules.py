"""One test per contract rule of the transform, on small handmade batches (no services)."""

import logging

import pytest

from src.contract import (
    ACCEPTED,
    CONFLICTING_VERSION,
    DELIVERY_BEFORE_PICKUP,
    DUPLICATE,
    GPS_IMPOSSIBLE_SPEED,
    GPS_NON_POSITIVE_INTERVAL,
    INCONSISTENT_STATUS_FIELDS,
    INCONSISTENT_TIMESTAMPS,
    INVALID_COORDINATES,
    INVALID_NUMBER,
    INVALID_STATUS,
    INVALID_TIMESTAMP,
    INVALID_VEHICLE_TYPE,
    MALFORMED_RECORD,
    MISSING_COORDINATES,
    MISSING_DRIVER,
    MISSING_ORDER,
    MISSING_REQUIRED_FIELD,
    OLDER_SOURCE_UPDATED_AT,
    ORPHAN_TIMEOUT,
    OUT_OF_RANGE_VALUE,
    PENDING,
    REJECTED,
    SOURCE_DELIVERY_EVENTS,
    SOURCE_DRIVERS,
    SOURCE_ORDERS,
    SOURCE_SHIFT_EVENTS,
    STALE_IGNORED,
)
from src.transformations.outputs import batch_summary
from src.transformations.pipeline import EXPIRED, RESOLVED, TransformConfig, process_batch
from src.transformations.records import Batch, BatchSchemaError, read_csv, read_jsonl
from src.transformations.rules import validate_order
from src.transformations.state import BatchSequenceError, LocalState
from tests.unit.transform_helpers import (
    ASSIGNED_ORDER,
    DELIVERED_ORDER,
    DRIVER,
    make_batch,
    order,
    outcome,
    ping,
    run,
    shift,
)

pytestmark = pytest.mark.unit

ORDERS_HEADER = (
    "order_id,customer_id,driver_id,order_created_ts,pickup_ts,delivery_ts,promised_ts,"
    "distance_km,status,city,fare_amount,source_updated_at"
)
T0 = "2026-09-10T10:20:00Z"
T1 = "2026-09-10T10:24:00Z"
T2 = "2026-09-10T10:28:00Z"


# ------------------------------------------------------------------ reading ---


def test_header_only_csv_has_no_rows() -> None:
    assert read_csv(SOURCE_ORDERS, ORDERS_HEADER + "\n") == []


def test_empty_jsonl_has_no_rows() -> None:
    assert read_jsonl(SOURCE_DELIVERY_EVENTS, "") == []


def test_missing_required_column_fails_the_batch() -> None:
    header = ORDERS_HEADER.replace(",city,", ",")
    with pytest.raises(BatchSchemaError, match="city"):
        read_csv(SOURCE_ORDERS, header + "\n")


def test_missing_required_json_field_fails_the_batch() -> None:
    line = '{"event_id":"EVT-1","order_id":"ORD-1","driver_id":"DRV-1","event_ts":"x"}'
    with pytest.raises(BatchSchemaError, match="event_type"):
        read_jsonl(SOURCE_DELIVERY_EVENTS, line + "\n")


def test_extra_column_is_ignored_kept_in_raw_and_logged(caplog: pytest.LogCaptureFixture) -> None:
    content = ORDERS_HEADER + ",note\n" + ",".join(DELIVERED_ORDER.values()) + ",hello\n"
    with caplog.at_level(logging.WARNING):
        records = read_csv(SOURCE_ORDERS, content)
    assert "note" in caplog.text
    assert "note" not in records[0].fields
    assert '"note": "hello"' in records[0].raw


def test_malformed_json_line_is_rejected_not_dropped() -> None:
    good = '{"event_id":"EVT-1","order_id":"ORD-1","driver_id":"DRV-1",'
    good += '"event_type":"assigned","event_ts":"2026-09-10T10:00:00Z","lat":null,"lon":null}'
    records = read_jsonl(SOURCE_DELIVERY_EVENTS, good + "\n{not json\n")
    assert [record.parse_error for record in records] == [False, True]
    batch = make_batch(1)
    batch.records[SOURCE_DELIVERY_EVENTS].extend(records)
    results, _ = run([batch])
    assert outcome(results[0], SOURCE_DELIVERY_EVENTS, 2) == (REJECTED, MALFORMED_RECORD)


# ------------------------------------------------------------------ drivers ---


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"city": ""}, MISSING_REQUIRED_FIELD),
        ({"vehicle_type": "truck"}, INVALID_VEHICLE_TYPE),
        ({"joined_date": "2025-02-30"}, INVALID_TIMESTAMP),
        ({"rating": "5.70"}, OUT_OF_RANGE_VALUE),
        ({"rating": "high"}, INVALID_NUMBER),
    ],
)
def test_invalid_driver_is_rejected(overrides: dict[str, str], reason: str) -> None:
    results, _ = run([make_batch(1, drivers=[{**DRIVER, **overrides}])])
    assert outcome(results[0], SOURCE_DRIVERS, 1) == (REJECTED, reason)


def test_driver_resent_later_is_duplicate_and_changed_is_conflict() -> None:
    changed = {**DRIVER, "city": "Mumbai"}
    results, _ = run([make_batch(1, drivers=[DRIVER]), make_batch(2, drivers=[DRIVER, changed])])
    assert outcome(results[1], SOURCE_DRIVERS, 1) == (DUPLICATE, "")
    assert outcome(results[1], SOURCE_DRIVERS, 2) == (REJECTED, CONFLICTING_VERSION)


# ------------------------------------------------------------------- orders ---


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"customer_id": ""}, (REJECTED, MISSING_REQUIRED_FIELD)),
        ({"promised_ts": ""}, (REJECTED, MISSING_REQUIRED_FIELD)),
        ({"status": "lost"}, (REJECTED, INVALID_STATUS)),
        ({"customer_id": "", "status": "lost"}, (REJECTED, MISSING_REQUIRED_FIELD)),
        ({"status": " Delivered"}, (ACCEPTED, "")),
        ({"pickup_ts": "yesterday"}, (REJECTED, INVALID_TIMESTAMP)),
        ({"promised_ts": "2026-02-30T25:61:00Z"}, (REJECTED, INVALID_TIMESTAMP)),
        ({"fare_amount": "abc"}, (REJECTED, INVALID_NUMBER)),
        ({"fare_amount": "0.00"}, (REJECTED, OUT_OF_RANGE_VALUE)),
        ({"distance_km": "-4.200"}, (REJECTED, OUT_OF_RANGE_VALUE)),
        ({"delivery_ts": ""}, (REJECTED, INCONSISTENT_STATUS_FIELDS)),
        ({"status": "assigned"}, (REJECTED, INCONSISTENT_STATUS_FIELDS)),
        ({"delivery_ts": "2026-09-10T10:05:00Z"}, (REJECTED, DELIVERY_BEFORE_PICKUP)),
        ({"promised_ts": "2026-09-10T10:00:00Z"}, (REJECTED, INCONSISTENT_TIMESTAMPS)),
    ],
)
def test_order_rules_and_their_priority(
    overrides: dict[str, str], expected: tuple[str, str]
) -> None:
    results, _ = run([make_batch(1, drivers=[DRIVER], orders=[order(**overrides)])])
    assert outcome(results[0], SOURCE_ORDERS, 1) == expected


def test_status_is_normalised_in_the_accepted_record() -> None:
    _, state = run([make_batch(1, drivers=[DRIVER], orders=[order(status=" Delivered")])])
    assert state.orders["ORD-0000001"].status == "delivered"


def test_exact_duplicate_row_in_one_batch() -> None:
    results, _ = run([make_batch(1, drivers=[DRIVER], orders=[DELIVERED_ORDER, DELIVERED_ORDER])])
    assert outcome(results[0], SOURCE_ORDERS, 1) == (ACCEPTED, "")
    assert outcome(results[0], SOURCE_ORDERS, 2) == (DUPLICATE, "")


def test_newer_stale_duplicate_and_conflict_across_batches() -> None:
    conflict = order(fare_amount="70.00")
    results, state = run(
        [
            make_batch(1, drivers=[DRIVER], orders=[ASSIGNED_ORDER]),
            make_batch(2, orders=[DELIVERED_ORDER]),
            make_batch(3, orders=[ASSIGNED_ORDER]),
            make_batch(4, orders=[DELIVERED_ORDER]),
            make_batch(5, orders=[conflict]),
        ]
    )
    assert outcome(results[1], SOURCE_ORDERS, 1) == (ACCEPTED, "")
    assert outcome(results[2], SOURCE_ORDERS, 1) == (STALE_IGNORED, OLDER_SOURCE_UPDATED_AT)
    assert outcome(results[3], SOURCE_ORDERS, 1) == (DUPLICATE, "")
    assert outcome(results[4], SOURCE_ORDERS, 1) == (REJECTED, CONFLICTING_VERSION)
    assert str(state.orders["ORD-0000001"].fare_amount) == "67.00"


def test_same_version_conflict_in_one_batch_rejects_all_rows() -> None:
    rows = [DELIVERED_ORDER, order(fare_amount="70.00")]
    results, state = run([make_batch(1, drivers=[DRIVER], orders=rows)])
    assert outcome(results[0], SOURCE_ORDERS, 1) == (REJECTED, CONFLICTING_VERSION)
    assert outcome(results[0], SOURCE_ORDERS, 2) == (REJECTED, CONFLICTING_VERSION)
    assert "ORD-0000001" not in state.orders


def test_older_version_in_the_same_batch_is_stale() -> None:
    rows = [DELIVERED_ORDER, ASSIGNED_ORDER]
    results, state = run([make_batch(1, drivers=[DRIVER], orders=rows)])
    assert outcome(results[0], SOURCE_ORDERS, 1) == (ACCEPTED, "")
    assert outcome(results[0], SOURCE_ORDERS, 2) == (STALE_IGNORED, OLDER_SOURCE_UPDATED_AT)
    assert state.orders["ORD-0000001"].status == "delivered"


def test_order_with_unknown_driver_is_pending_then_resolved() -> None:
    results, state = run([make_batch(1, orders=[DELIVERED_ORDER]), make_batch(2, drivers=[DRIVER])])
    assert outcome(results[0], SOURCE_ORDERS, 1) == (PENDING, MISSING_DRIVER)
    assert [(r.outcome, r.disposition) for r in results[1].resolutions] == [(RESOLVED, ACCEPTED)]
    assert "ORD-0000001" in state.orders


def test_delivery_minutes_and_is_late_follow_the_contract() -> None:
    _, on_time = validate_order(DELIVERED_ORDER)
    _, late = validate_order(order(promised_ts="2026-09-10T10:30:00Z"))
    _, open_order = validate_order(ASSIGNED_ORDER)
    assert on_time is not None and late is not None and open_order is not None
    assert on_time.delivery_minutes() == 25.0  # pickup 10:15 -> delivery 10:40
    assert on_time.is_late() is False
    assert late.is_late() is True
    assert open_order.delivery_minutes() is None
    assert open_order.is_late() is None


# ------------------------------------------------------------------- events ---


def with_parents(sequence: int, events: list[dict[str, str]]) -> Batch:
    """A batch holding the events plus their (valid) driver and order."""
    return make_batch(sequence, drivers=[DRIVER], orders=[DELIVERED_ORDER], events=events)


def test_coordinates_rules() -> None:
    events = [
        ping("EVT-1", T0, "", ""),
        ping("EVT-2", T0, "95.000000", "77.000000"),
        ping("EVT-3", T0, "12.970000", ""),
        ping("EVT-4", T0, "", "", event_type="assigned"),
    ]
    results, _ = run([with_parents(1, events)])
    assert outcome(results[0], SOURCE_DELIVERY_EVENTS, 1) == (REJECTED, MISSING_COORDINATES)
    assert outcome(results[0], SOURCE_DELIVERY_EVENTS, 2) == (REJECTED, INVALID_COORDINATES)
    assert outcome(results[0], SOURCE_DELIVERY_EVENTS, 3) == (REJECTED, MISSING_COORDINATES)
    assert outcome(results[0], SOURCE_DELIVERY_EVENTS, 4) == (ACCEPTED, "")


def test_impossible_jump_is_rejected_and_next_ping_compared_with_last_accepted() -> None:
    events = [
        ping("EVT-1", T0, "12.970000", "77.590000"),
        ping("EVT-2", T1, "13.420000", "77.590000"),  # ~50 km in 4 minutes
        ping("EVT-3", T2, "12.975000", "77.590000"),  # ~0.6 km from EVT-1 in 8 minutes
    ]
    results, _ = run([with_parents(1, events)])
    assert outcome(results[0], SOURCE_DELIVERY_EVENTS, 1) == (ACCEPTED, "")
    assert outcome(results[0], SOURCE_DELIVERY_EVENTS, 2) == (REJECTED, GPS_IMPOSSIBLE_SPEED)
    assert outcome(results[0], SOURCE_DELIVERY_EVENTS, 3) == (ACCEPTED, "")


def test_speed_threshold_is_configurable() -> None:
    events = [
        ping("EVT-1", T0, "12.970000", "77.590000"),
        ping("EVT-2", T1, "12.990000", "77.590000"),  # ~2.2 km in 4 minutes = ~33 km/h
    ]
    config = TransformConfig(max_speed_kmh=20.0)
    results, _ = run([with_parents(1, events)], config)
    assert outcome(results[0], SOURCE_DELIVERY_EVENTS, 2) == (REJECTED, GPS_IMPOSSIBLE_SPEED)


def test_duplicate_timestamp_is_a_non_positive_interval() -> None:
    events = [
        ping("EVT-1", T0, "12.970000", "77.590000"),
        ping("EVT-2", T0, "12.972000", "77.590000"),
    ]
    results, _ = run([with_parents(1, events)])
    assert outcome(results[0], SOURCE_DELIVERY_EVENTS, 2) == (REJECTED, GPS_NON_POSITIVE_INTERVAL)


def test_long_gap_at_plausible_speed_is_accepted() -> None:
    events = [
        ping("EVT-1", T0, "12.970000", "77.590000"),
        ping("EVT-2", "2026-09-10T10:50:00Z", "12.997000", "77.590000"),  # 3 km in 30 min
    ]
    results, _ = run([with_parents(1, events)])
    assert outcome(results[0], SOURCE_DELIVERY_EVENTS, 2) == (ACCEPTED, "")


def test_out_of_order_file_rows_are_sorted_and_accepted() -> None:
    events = [
        ping("EVT-1", T1, "12.972000", "77.590000"),
        ping("EVT-2", T0, "12.970000", "77.590000"),
    ]
    results, _ = run([with_parents(1, events)])
    assert outcome(results[0], SOURCE_DELIVERY_EVENTS, 1) == (ACCEPTED, "")
    assert outcome(results[0], SOURCE_DELIVERY_EVENTS, 2) == (ACCEPTED, "")


def test_late_ping_is_checked_against_its_predecessor_and_accepted() -> None:
    first = [
        ping("EVT-1", T0, "12.970000", "77.590000"),
        ping("EVT-3", T2, "12.974000", "77.590000"),
    ]
    late = [ping("EVT-2", T1, "12.972000", "77.590000")]
    results, state = run([with_parents(1, first), make_batch(2, events=late)])
    assert outcome(results[1], SOURCE_DELIVERY_EVENTS, 1) == (ACCEPTED, "")
    assert "EVT-2" in state.delivery_events


def test_event_replay_is_duplicate_and_changed_content_is_conflict() -> None:
    event = ping("EVT-1", T0, "12.970000", "77.590000")
    changed = ping("EVT-1", T0, "12.971000", "77.590000")
    results, _ = run([with_parents(1, [event]), make_batch(2, events=[event, changed])])
    assert outcome(results[1], SOURCE_DELIVERY_EVENTS, 1) == (DUPLICATE, "")
    assert outcome(results[1], SOURCE_DELIVERY_EVENTS, 2) == (REJECTED, CONFLICTING_VERSION)


def test_orphan_event_is_pending_then_resolved_when_the_order_arrives() -> None:
    event = ping("EVT-1", T0, "12.970000", "77.590000")
    results, state = run(
        [
            make_batch(1, drivers=[DRIVER], events=[event]),
            make_batch(2, orders=[DELIVERED_ORDER]),
        ]
    )
    assert outcome(results[0], SOURCE_DELIVERY_EVENTS, 1) == (PENDING, MISSING_ORDER)
    assert [(r.outcome, r.disposition) for r in results[1].resolutions] == [(RESOLVED, ACCEPTED)]
    assert results[1].rows_input[SOURCE_DELIVERY_EVENTS] == 0
    assert "EVT-1" in state.delivery_events
    assert state.pending() == []


def test_orphan_expires_after_the_configured_age() -> None:
    event = ping("EVT-1", T0, "12.970000", "77.590000")
    config = TransformConfig(orphan_max_age_batches=1)
    results, state = run(
        [make_batch(1, drivers=[DRIVER], events=[event]), make_batch(2), make_batch(3)], config
    )
    assert results[1].resolutions == []
    assert [(r.outcome, r.reason) for r in results[2].resolutions] == [(EXPIRED, ORPHAN_TIMEOUT)]
    assert state.pending() == []


def test_shift_event_waits_for_its_driver() -> None:
    event = shift("SHF-1", T0)
    results, state = run([make_batch(1, shifts=[event]), make_batch(2, drivers=[DRIVER])])
    assert outcome(results[0], SOURCE_SHIFT_EVENTS, 1) == (PENDING, MISSING_DRIVER)
    assert [r.outcome for r in results[1].resolutions] == [RESOLVED]
    assert "SHF-1" in state.shift_events


# ------------------------------------------------------------- reconciliation ---


def test_every_input_row_gets_exactly_one_disposition() -> None:
    events = [ping("EVT-1", T0, "12.970000", "77.590000")] * 2 + [ping("EVT-2", T1, "", "")]
    batch = make_batch(1, drivers=[DRIVER], orders=[DELIVERED_ORDER] * 2, events=events)
    results, _ = run([batch])
    keys = [(d.source, d.row_number) for d in results[0].dispositions]
    assert len(keys) == len(set(keys)) == sum(results[0].rows_input.values())


def test_batches_must_be_processed_in_sequence() -> None:
    with pytest.raises(BatchSequenceError):
        process_batch(make_batch(2), LocalState(), TransformConfig())


def test_pending_counts_and_log_lines_are_per_source(caplog: pytest.LogCaptureFixture) -> None:
    """One pending record per source; only the shift event's driver arrives in batch 2."""
    shift_driver = {**DRIVER, "driver_id": "DRV-00008"}
    pending_order = order(driver_id="DRV-00009")
    event = ping("EVT-1", T0, "", "", driver_id="DRV-00009", event_type="assigned")
    first = make_batch(
        1,
        orders=[pending_order],
        events=[event],
        shifts=[shift("SHF-1", T0, driver_id="DRV-00008")],
    )
    second = make_batch(2, drivers=[shift_driver])
    with caplog.at_level(logging.INFO, logger="deliverypulse.transform"):
        results, _ = run([first, second])

    one, two = (batch_summary(result)["sources"] for result in results)
    assert {s: one[s]["pending_remaining"] for s in one} == {
        SOURCE_DRIVERS: 0,
        SOURCE_ORDERS: 1,
        SOURCE_DELIVERY_EVENTS: 1,
        SOURCE_SHIFT_EVENTS: 1,
    }
    assert {s: (two[s]["pending_resolved"], two[s]["pending_remaining"]) for s in two} == {
        SOURCE_DRIVERS: (0, 0),
        SOURCE_ORDERS: (0, 1),
        SOURCE_DELIVERY_EVENTS: (0, 1),
        SOURCE_SHIFT_EVENTS: (1, 0),
    }
    assert "batch 2 pending orders: resolved=0, expired=0, still pending=1;" in caplog.text
    assert "batch 2 pending events: resolved=1, expired=0, still pending=1;" in caplog.text
    # Dispositions are unchanged by the log fix: every first-batch row is pending.
    assert [d.disposition for d in results[0].dispositions] == [PENDING, PENDING, PENDING]
