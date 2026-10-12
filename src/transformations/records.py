"""Read one raw batch folder into records, enforcing the schema rules of the data contract.

* A missing required column fails the whole batch (:class:`BatchSchemaError`).
* An unknown extra column is logged as a warning; its values stay in the raw payload and are
  ignored downstream.
* A row that cannot be parsed (malformed JSON line, wrong CSV field count) becomes a record with
  ``parse_error`` set, so it still receives exactly one disposition.

Raw files are only read, never modified.
"""

import csv
import io
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.contract import (
    BATCH_FOLDER_PREFIX,
    BATCH_METADATA_FILE,
    SOURCE_COLUMNS,
    SOURCE_FILES,
    SOURCE_KEYS,
    parse_utc,
)

logger = logging.getLogger(__name__)


class BatchSchemaError(ValueError):
    """A batch file violates the schema contract (for example a missing required column)."""


@dataclass(frozen=True)
class RawRecord:
    """One input row exactly as received.

    ``fields`` holds the contract fields as text ("" means missing). ``raw`` is the original
    record: the JSON line for JSON Lines files, or a JSON object of every CSV column (including
    extra columns) for CSV files.
    """

    source: str
    row_number: int
    fields: dict[str, str]
    raw: str
    parse_error: bool = False

    @property
    def key(self) -> str:
        """Business key of the record ("" if missing)."""
        return self.fields.get(SOURCE_KEYS[self.source], "")


@dataclass(frozen=True)
class Batch:
    """All records of one batch, by source, in file order."""

    sequence: int
    batch_id: str
    window_start: int
    window_end: int
    records: dict[str, list[RawRecord]]


def _empty_fields(source: str) -> dict[str, str]:
    return dict.fromkeys(SOURCE_COLUMNS[source], "")


def read_csv(source: str, content: str) -> list[RawRecord]:
    """Parse a CSV source file. The header must contain every contract column."""
    reader = csv.reader(io.StringIO(content, newline=""))
    header = next(reader, None)
    if header is None:
        raise BatchSchemaError(f"{source}: file is empty (no header row)")
    missing = [column for column in SOURCE_COLUMNS[source] if column not in header]
    if missing:
        raise BatchSchemaError(f"{source}: missing required column(s): {', '.join(missing)}")
    extra = [column for column in header if column not in SOURCE_COLUMNS[source]]
    if extra:
        logger.warning("%s: ignoring extra column(s): %s", source, ", ".join(extra))
    records: list[RawRecord] = []
    for row_number, values in enumerate(reader, start=1):
        if len(values) != len(header):
            raw = json.dumps(values, ensure_ascii=True)
            records.append(RawRecord(source, row_number, _empty_fields(source), raw, True))
            continue
        row = dict(zip(header, values, strict=True))
        fields = {column: row[column] for column in SOURCE_COLUMNS[source]}
        raw = json.dumps(row, ensure_ascii=True, sort_keys=True)
        records.append(RawRecord(source, row_number, fields, raw))
    return records


def _json_text(value: Any) -> str | None:
    """Text of a JSON scalar; None for an unsupported type."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return None
    if isinstance(value, str | int):
        return str(value)
    return None


def read_jsonl(source: str, content: str) -> list[RawRecord]:
    """Parse a JSON Lines source file; numbers keep their original text."""
    records: list[RawRecord] = []
    seen_keys: set[str] = set()
    for row_number, line in enumerate(content.splitlines(), start=1):
        try:
            item = json.loads(line, parse_float=str)
        except json.JSONDecodeError:
            item = None
        fields = _empty_fields(source)
        valid = isinstance(item, dict)
        if isinstance(item, dict):
            seen_keys.update(str(key) for key in item)
            for column in SOURCE_COLUMNS[source]:
                text = _json_text(item.get(column))
                if text is None:
                    valid = False
                else:
                    fields[column] = text
        records.append(RawRecord(source, row_number, fields, line, not valid))
    if records and seen_keys:
        missing = [column for column in SOURCE_COLUMNS[source] if column not in seen_keys]
        if missing:
            raise BatchSchemaError(f"{source}: missing required field(s): {', '.join(missing)}")
        extra = sorted(seen_keys - set(SOURCE_COLUMNS[source]))
        if extra:
            logger.warning("%s: ignoring extra field(s): %s", source, ", ".join(extra))
    return records


def read_source(source: str, path: Path) -> list[RawRecord]:
    """Read one source file of a batch."""
    content = path.read_text(encoding="utf-8")
    if path.suffix == ".jsonl":
        return read_jsonl(source, content)
    return read_csv(source, content)


def batch_folders(dataset_dir: Path) -> list[Path]:
    """``batch-<k>`` folders of a raw dataset in sequence order; dot-prefixed entries ignored."""
    folders = [
        path
        for path in dataset_dir.iterdir()
        if path.is_dir()
        and not path.name.startswith(".")
        and path.name.startswith(BATCH_FOLDER_PREFIX)
        and path.name.removeprefix(BATCH_FOLDER_PREFIX).isdigit()
    ]
    return sorted(folders, key=lambda path: int(path.name.removeprefix(BATCH_FOLDER_PREFIX)))


def load_batch(folder: Path) -> Batch:
    """Read the metadata and all four source files of one batch folder."""
    metadata = json.loads((folder / BATCH_METADATA_FILE).read_text(encoding="utf-8"))
    window_start = parse_utc(str(metadata["window_start"]))
    window_end = parse_utc(str(metadata["window_end_exclusive"]))
    if window_start is None or window_end is None:
        raise BatchSchemaError(f"{folder}: invalid window in {BATCH_METADATA_FILE}")
    records = {source: read_source(source, folder / name) for source, name in SOURCE_FILES}
    return Batch(
        sequence=int(metadata["batch_sequence"]),
        batch_id=str(metadata["batch_id"]),
        window_start=window_start,
        window_end=window_end,
        records=records,
    )
