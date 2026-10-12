"""Unit tests for the database loader's pure helpers (no database needed)."""

import json

import pytest

from src.contract import SOURCE_DELIVERY_EVENTS, SOURCE_ORDERS, SOURCE_SHIFT_EVENTS
from src.database.load_postgres import payload_json, raw_values
from src.database.postgres_state import needed_keys
from src.transformations.records import RawRecord, read_csv, read_jsonl
from src.transformations.state import PendingRecord
from tests.unit.transform_helpers import DELIVERED_ORDER, make_batch, ping, shift

pytestmark = pytest.mark.unit

ORDERS_HEADER = ",".join(DELIVERED_ORDER)


def test_csv_raw_values_keep_text_and_extra_columns() -> None:
    row = dict(DELIVERED_ORDER, customer_id="")
    content = f"{ORDERS_HEADER},note\n" + ",".join(row.values()) + ",hello\n"
    record = read_csv(SOURCE_ORDERS, content)[0]
    values, extras = raw_values(record)
    assert values[0] == "ORD-0000001"
    assert values[1] == ""  # an empty CSV field stays an empty string
    assert extras is not None and json.loads(extras) == {"note": "hello"}


def test_json_raw_values_keep_number_text_and_nulls() -> None:
    line = (
        '{"event_id":"EVT-1","order_id":"ORD-1","driver_id":"DRV-1","event_type":"gps_ping",'
        '"event_ts":"2026-09-10T10:20:00Z","lat":12.970000,"lon":null}'
    )
    record = read_jsonl(SOURCE_DELIVERY_EVENTS, line + "\n")[0]
    values, extras = raw_values(record)
    assert values[-2:] == ["12.970000", None]
    assert extras is None


def test_malformed_record_is_kept_whole() -> None:
    record = RawRecord(SOURCE_DELIVERY_EVENTS, 1, {}, "{not json", parse_error=True)
    values, extras = raw_values(record)
    assert set(values) == {None}
    assert extras is not None and json.loads(extras) == {"_raw_line": "{not json"}


def test_payload_json_wraps_unparseable_text_only() -> None:
    assert payload_json('{"a": 1}') == '{"a": 1}'
    assert json.loads(payload_json("{oops")) == {"_raw_line": "{oops"}
    assert json.loads(payload_json('{"x": NaN}')) == {"_raw_line": '{"x": NaN}'}


def test_needed_keys_cover_batch_and_pending_records() -> None:
    events = [
        ping("EVT-2", "2026-09-10T10:24:00Z", "12.97", "77.59", driver_id=" DRV-7"),
        ping("EVT-1", "2026-09-10T10:20:00Z", "12.97", "77.59", driver_id=" DRV-7"),
        ping("EVT-3", "not-a-time", "12.97", "77.59", driver_id="DRV-8"),
    ]
    batch = make_batch(1, orders=[DELIVERED_ORDER], events=events, shifts=[shift("SHF-1", "x")])
    pending = PendingRecord(
        source=SOURCE_SHIFT_EVENTS,
        record_key="SHF-0",
        batch_sequence=1,
        batch_id="b",
        row_number=1,
        missing_reference="driver",
        fields_json=json.dumps({"driver_id": "DRV-9", "event_id": "SHF-0"}),
        raw="{}",
        first_seen_at=0,
    )
    keys = needed_keys(batch, [pending])
    assert {" DRV-7", "DRV-7", "DRV-8", "DRV-9", "DRV-00001"} <= keys.drivers
    assert keys.orders == {"ORD-0000001"}
    assert keys.delivery_events == {"EVT-1", "EVT-2", "EVT-3"}
    assert keys.shift_events == {"SHF-1"}
    # Earliest parseable ping per driver (as written in the file); unparseable times ignored.
    assert keys.ping_starts == {" DRV-7": 1789035600}
