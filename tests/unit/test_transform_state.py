"""Unit tests for the local pipeline state: GPS predecessor lookup and Parquet round trip."""

import hashlib
from pathlib import Path

import pytest

from src.transformations.gps import Ping
from src.transformations.state import BatchSequenceError, LocalState
from tests.unit.transform_helpers import DELIVERED_ORDER, DRIVER, make_batch, ping, run, shift

pytestmark = pytest.mark.unit


def _hashes(folder: Path) -> dict[str, str]:
    return {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(folder.iterdir())
    }


def test_previous_ping_is_the_latest_one_before_in_time_then_id_order() -> None:
    state = LocalState()
    for event_ts, event_id in ((100, "E1"), (300, "E3"), (300, "E4")):
        state.add_ping("D1", Ping(event_ts, event_id, 0.0, 0.0))
    assert state.previous_ping("D1", 50, "E0") is None
    previous = state.previous_ping("D1", 200, "E2")
    assert previous is not None and previous.event_id == "E1"
    same_time = state.previous_ping("D1", 300, "E4")
    assert same_time is not None and same_time.event_id == "E3"
    assert state.previous_ping("D2", 500, "E9") is None


def test_batches_are_marked_in_sequence_only() -> None:
    state = LocalState()
    state.mark_processed(1, "a")
    with pytest.raises(BatchSequenceError):
        state.mark_processed(3, "c")


def test_state_survives_a_save_and_load_round_trip(tmp_path: Path) -> None:
    events = [ping("EVT-1", "2026-09-10T10:20:00Z", "12.970000", "77.590000")]
    orphan = [
        ping("EVT-9", "2026-09-10T10:30:00Z", "", "", order_id="ORD-9", event_type="assigned")
    ]
    batch = make_batch(
        1,
        drivers=[DRIVER],
        orders=[DELIVERED_ORDER],
        events=events + orphan,
        shifts=[shift("SHF-1", "2026-09-10T08:00:00Z")],
    )
    _, state = run([batch])
    state.save(tmp_path / "first")
    loaded = LocalState.load(tmp_path / "first")
    loaded.save(tmp_path / "second")
    assert _hashes(tmp_path / "first") == _hashes(tmp_path / "second")
    assert loaded.last_batch() == 1
    assert loaded.orders == state.orders
    assert len(state.pending()) == 1
    assert loaded.pending() == state.pending()
    assert loaded.previous_ping("DRV-00001", 2_000_000_000, "EVT-Z") is not None
