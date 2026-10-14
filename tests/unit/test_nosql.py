"""Raw preservation, immutable replay and bounded loading without MongoDB."""

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from src.nosql.documents import (
    SourceChangedError,
    batch_documents,
    event_document,
    same_content,
    verified_metadata,
)
from src.nosql.load_mongo import check_disk, insert_chunk, load_dataset
from tests.unit.generated import GeneratedDataset

pytestmark = pytest.mark.unit


class MemoryCollection:
    """Insert-only fake: deliberately provides no update or delete operation."""

    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}

    def find(self, filter: dict[str, Any]) -> list[dict[str, Any]]:
        return [copy.deepcopy(self.rows[key]) for key in filter["_id"]["$in"] if key in self.rows]

    def insert_many(self, documents: list[dict[str, Any]], *, ordered: bool) -> None:
        assert ordered
        for document in documents:
            assert document["_id"] not in self.rows
            self.rows[document["_id"]] = copy.deepcopy(document)


def document(row: int = 1, payload: str = '{"event_id":"same","lat":"bad"}') -> dict[str, Any]:
    return event_document(payload, "batch", "delivery_events.jsonl", row)


def test_source_identity_keeps_duplicate_and_missing_event_ids() -> None:
    assert document()["_id"] != document(2)["_id"]
    assert document()["_id"] == document(payload="{}")["_id"]
    assert document()["event_id"] == document(2)["event_id"]


@pytest.mark.parametrize("raw", ["{invalid", "null", "[]", '{"lat":NaN}'])
def test_malformed_content_is_preserved(raw: str) -> None:
    doc = document(payload=raw)
    assert doc["raw_line"] == raw
    assert doc["extra_columns"] == {"_raw_line": raw}
    assert doc["event_id"] is None


def test_types_and_extra_columns_match_raw_contract() -> None:
    doc = document(payload='{"event_id":12,"lat":1.230,"lon":null,"extra":{"x":true}}')
    assert doc["event_id"] == "12"
    assert doc["lat"] == "1.230"
    assert doc["lon"] is None
    assert doc["extra_columns"] == {"extra": {"x": True}}


def test_replay_preserves_full_content_and_ingestion_time() -> None:
    collection = MemoryCollection()
    assert insert_chunk(collection, [document(), document(2)]) == (2, 0)
    before = copy.deepcopy(collection.rows)
    assert insert_chunk(collection, [document(), document(2)]) == (0, 2)
    assert collection.rows == before


def test_changed_content_fails_before_inserting_any_row_in_chunk() -> None:
    collection = MemoryCollection()
    insert_chunk(collection, [document()])
    before = copy.deepcopy(collection.rows)
    with pytest.raises(SourceChangedError, match="Immutable source changed"):
        insert_chunk(collection, [document(2), document(payload="{}")])
    assert collection.rows == before


def test_duplicate_source_identity_in_chunk_is_rejected() -> None:
    with pytest.raises(SourceChangedError, match="Repeated source-row"):
        insert_chunk(MemoryCollection(), [document(), document()])


def test_content_check_detects_extra_or_modified_stored_fields() -> None:
    assert not same_content({**document(), "unexpected": "value"}, document())
    assert not same_content({**document(), "lat": "changed"}, document())


def test_streamed_rows_match_manifest(dataset_a: GeneratedDataset) -> None:
    folder = dataset_a.dataset_dir / "batch-1"
    metadata = verified_metadata(folder)
    iterator = batch_documents(folder, metadata)
    assert iter(iterator) is iterator
    rows = list(iterator)
    assert len(rows) == metadata["files"]["delivery_events.jsonl"]["rows"]
    assert len({row["_id"] for row in rows}) == len(rows)


def test_corrupted_manifest_is_rejected_before_loading(tmp_path: Path) -> None:
    (tmp_path / "delivery_events.jsonl").write_text("{}\n")
    (tmp_path / "batch.json").write_text(
        json.dumps({"files": {"delivery_events.jsonl": {"sha256": "wrong", "rows": 1}}})
    )
    with pytest.raises(SourceChangedError, match="manifest"):
        verified_metadata(tmp_path)


def test_complete_dataset_loader_reconciles_and_replays(
    dataset_a: GeneratedDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.nosql.load_mongo.check_disk", lambda path: None)
    collection = MemoryCollection()
    first = load_dataset(collection, dataset_a.dataset_dir)
    assert first["inserted"] == first["rows"] == len(collection.rows)
    assert first["rows"] > 5000  # Exercise multiple bounded chunks.
    assert first["duplicate"] == first["modified"] == 0
    before = copy.deepcopy(collection.rows)
    replay = load_dataset(collection, dataset_a.dataset_dir)
    assert replay["inserted"] == replay["modified"] == 0
    assert replay["duplicate"] == first["rows"]
    assert before == collection.rows


def test_disk_floor_blocks_load_before_connecting(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.bench.disk import MIN_FREE_BYTES, DiskGuardError

    monkeypatch.setattr("src.nosql.load_mongo.free_bytes", lambda path: MIN_FREE_BYTES - 1)
    with pytest.raises(DiskGuardError, match="10 GiB"):
        check_disk(Path("data/raw"))


def test_no_batches_fails_without_writing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.nosql.load_mongo.check_disk", lambda path: None)
    collection = MemoryCollection()
    with pytest.raises(ValueError, match="No raw batch"):
        load_dataset(collection, tmp_path)
    assert collection.rows == {}
