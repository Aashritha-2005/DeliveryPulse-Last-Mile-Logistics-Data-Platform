"""Build immutable documents using the PostgreSQL raw-value contract."""

import hashlib
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from src.contract import BATCH_METADATA_FILE, SOURCE_COLUMNS, SOURCE_DELIVERY_EVENTS, SOURCE_FILES
from src.database.load_postgres import raw_values
from src.transformations.records import RawRecord

EVENT_FILE = dict(SOURCE_FILES)[SOURCE_DELIVERY_EVENTS]
IDENTITY_FIELDS = ("batch_id", "source_file", "source_row_number")
VALUE_FIELDS = tuple(SOURCE_COLUMNS[SOURCE_DELIVERY_EVENTS]) + ("extra_columns",)


class SourceChangedError(ValueError):
    """A source file or existing immutable document differs from its recorded content."""


def event_document(raw: str, batch_id: str, source_file: str, row_number: int) -> dict[str, Any]:
    """Preserve a raw line, typed-as-text SQL fields and stable source-row identity."""
    identity = [batch_id, source_file, row_number]
    encoded = json.dumps(identity, ensure_ascii=True, separators=(",", ":")).encode()
    values, extras = raw_values(RawRecord(SOURCE_DELIVERY_EVENTS, row_number, {}, raw))
    return {
        "_id": hashlib.sha256(encoded).hexdigest(),
        "batch_id": batch_id,
        "source_file": source_file,
        "source_row_number": row_number,
        "raw_line": raw,
        **dict(zip(SOURCE_COLUMNS[SOURCE_DELIVERY_EVENTS], values, strict=True)),
        "extra_columns": None if extras is None else json.loads(extras),
    }


def verified_metadata(folder: Path) -> dict[str, Any]:
    """Verify the event file against its immutable batch manifest before any inserts."""
    metadata: dict[str, Any] = json.loads((folder / BATCH_METADATA_FILE).read_text())
    with (folder / EVENT_FILE).open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    if digest != metadata["files"][EVENT_FILE]["sha256"]:
        raise SourceChangedError("Event source does not match its batch manifest")
    return metadata


def batch_documents(folder: Path, metadata: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Stream one document per physical JSONL row without buffering the source file."""
    rows = 0
    with (folder / EVENT_FILE).open(encoding="utf-8") as source:
        for rows, line in enumerate(source, 1):
            yield event_document(line.rstrip("\r\n"), str(metadata["batch_id"]), EVENT_FILE, rows)
    if rows != metadata["files"][EVENT_FILE]["rows"]:
        raise SourceChangedError("Event row count does not match its batch manifest")


def same_content(stored: dict[str, Any], incoming: dict[str, Any]) -> bool:
    """Compare all document fields except first-insertion time, which replay preserves."""
    return {k: v for k, v in stored.items() if k != "ingested_at"} == incoming
