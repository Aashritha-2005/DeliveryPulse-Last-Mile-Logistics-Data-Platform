"""Deterministic serialisation, ``batch_id`` hashing and immutable publishing.

Byte-level rules: UTF-8 without BOM, ``\\n`` line endings, rows sorted by business key and then
by the full row, fixed number formats, no compression and no embedded generation timestamps.
"""

import csv
import hashlib
import io
import json
import logging
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from src.ingestion.generator_config import (
    DELIVERY_EVENT_FIELDS,
    DRIVER_COLUMNS,
    ORDER_COLUMNS,
    SHIFT_EVENT_COLUMNS,
    GeneratorConfig,
    format_utc,
)
from src.ingestion.simulation import Placed, Placements, Row

logger = logging.getLogger(__name__)

BATCH_ID_DOMAIN = "deliverypulse-batch-v1"
TEMP_DIR_PREFIX = ".generate-tmp-"
READ_ONLY_MODE = 0o444
STATUS_CREATED = "created"
STATUS_UNCHANGED = "unchanged"
_COORDINATE_FIELDS = ("lat", "lon")


class ImmutableOutputError(RuntimeError):
    """Existing output differs from what would be written, or the output folder is unsafe."""


@dataclass(frozen=True)
class BatchFiles:
    """Serialised data files of one batch and their row counts."""

    sequence: int
    files: dict[str, bytes]
    rows: dict[str, int]


def _sorted_rows(placed: list[Placed], batch: int) -> list[Row]:
    rows = [item.row for item in placed if item.batch == batch]
    return sorted(rows, key=lambda row: (row[0], row))


def csv_bytes(columns: Sequence[str], rows: Sequence[Row]) -> bytes:
    """CSV with a header row, minimal quoting and ``\\n`` line endings."""
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    writer.writerow(columns)
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


def event_jsonl_bytes(rows: Sequence[Row]) -> bytes:
    """JSON Lines with fixed key order; coordinates are JSON numbers, empty ones are null."""
    lines: list[str] = []
    for row in rows:
        parts: list[str] = []
        for name, value in zip(DELIVERY_EVENT_FIELDS, row, strict=True):
            if name in _COORDINATE_FIELDS:
                rendered = "null" if value == "" else value
            else:
                rendered = json.dumps(value, ensure_ascii=True)
            parts.append(f"{json.dumps(name)}:{rendered}")
        lines.append("{" + ",".join(parts) + "}\n")
    return "".join(lines).encode("utf-8")


def serialize_batches(config: GeneratorConfig, placements: Placements) -> list[BatchFiles]:
    """Serialise every batch's four data files."""
    batches: list[BatchFiles] = []
    for sequence in range(1, config.n_batches + 1):
        drivers = _sorted_rows(placements.drivers, sequence)
        orders = _sorted_rows(placements.orders, sequence)
        events = _sorted_rows(placements.delivery_events, sequence)
        shifts = _sorted_rows(placements.shift_events, sequence)
        files = {
            "drivers.csv": csv_bytes(DRIVER_COLUMNS, drivers),
            "orders.csv": csv_bytes(ORDER_COLUMNS, orders),
            "delivery_events.jsonl": event_jsonl_bytes(events),
            "shift_events.csv": csv_bytes(SHIFT_EVENT_COLUMNS, shifts),
        }
        rows = {
            "drivers.csv": len(drivers),
            "orders.csv": len(orders),
            "delivery_events.jsonl": len(events),
            "shift_events.csv": len(shifts),
        }
        batches.append(BatchFiles(sequence, files, rows))
    return batches


def sha256_hex(content: bytes) -> str:
    """SHA-256 hex digest of ``content``."""
    return hashlib.sha256(content).hexdigest()


def compute_batch_id(
    canonical_config: str, batch_sequence: int, data_files: Mapping[str, bytes]
) -> str:
    """Deterministic batch identifier (see docs/data_contracts.md).

    sha256 of: the domain line, the canonical config JSON, ``batch_sequence=<k>``, then one
    ``<file name> <sha256 of file bytes>`` line per data file in sorted name order.
    """
    lines = [BATCH_ID_DOMAIN, canonical_config, f"batch_sequence={batch_sequence}"]
    lines.extend(f"{name} {sha256_hex(data_files[name])}" for name in sorted(data_files))
    return sha256_hex(("\n".join(lines) + "\n").encode("utf-8"))


def batch_metadata_bytes(config: GeneratorConfig, batch: BatchFiles, batch_id: str) -> bytes:
    """``batch.json``: identity and file inventory of one batch (no defect information)."""
    cutoffs = config.cutoffs()
    starts = (config.window_start_epoch(), *cutoffs[:-1])
    payload = {
        "batch_id": batch_id,
        "batch_sequence": batch.sequence,
        "config_sha256": config.sha256(),
        "dataset": config.dataset_name(),
        "files": {
            name: {
                "bytes": len(batch.files[name]),
                "rows": batch.rows[name],
                "sha256": sha256_hex(batch.files[name]),
            }
            for name in sorted(batch.files)
        },
        "window_end_exclusive": format_utc(cutoffs[batch.sequence - 1]),
        "window_start": format_utc(starts[batch.sequence - 1]),
    }
    text = json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True)
    return (text + "\n").encode("utf-8")


# ------------------------------------------------------------- publishing ---


def remove_own_leftovers(parent: Path) -> None:
    """Remove temp folders left by an interrupted run; refuse any other hidden folder.

    Hidden files (for example ``.gitkeep``) are ignored.
    """
    if not parent.is_dir():
        return
    for entry in sorted(parent.iterdir()):
        if not entry.name.startswith(".") or not entry.is_dir() or entry.is_symlink():
            continue
        if entry.name.startswith(TEMP_DIR_PREFIX):
            logger.warning("Removing leftover temporary folder from an interrupted run: %s", entry)
            shutil.rmtree(entry)
        else:
            raise ImmutableOutputError(
                f"Unexpected hidden folder {entry}. It was not created by the generator; "
                "move or remove it yourself, then retry."
            )


def _visible_files(root: Path) -> set[str]:
    """Relative POSIX paths of files under ``root``, skipping hidden entries at any depth."""
    found: set[str] = set()
    for path in root.rglob("*"):
        relative = path.relative_to(root)
        if path.is_file() and not any(part.startswith(".") for part in relative.parts):
            found.add(relative.as_posix())
    return found


def _differences(target: Path, files: Mapping[str, bytes]) -> list[str]:
    existing = _visible_files(target)
    expected = set(files)
    problems = [f"missing: {name}" for name in sorted(expected - existing)]
    problems += [f"unexpected: {name}" for name in sorted(existing - expected)]
    problems += [
        f"different content: {name}"
        for name in sorted(expected & existing)
        if (target / name).read_bytes() != files[name]
    ]
    return problems


def publish(target: Path, files: Mapping[str, bytes]) -> str:
    """Write ``files`` (relative path -> bytes) as the folder ``target``, never overwriting.

    Returns ``"created"`` for a new folder or ``"unchanged"`` if it already holds identical
    files. Raises :class:`ImmutableOutputError` if the existing folder differs in any way.
    Files are written to a hidden temp folder next to ``target`` and renamed into place, then
    made read-only.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    remove_own_leftovers(target.parent)
    if target.exists():
        problems = _differences(target, files)
        if problems:
            shown = "; ".join(problems[:5])
            raise ImmutableOutputError(
                f"{target} already exists with different contents ({len(problems)} "
                f"differences: {shown}). Raw output is immutable: choose another seed or "
                "size, or delete that folder deliberately."
            )
        return STATUS_UNCHANGED
    staging = Path(tempfile.mkdtemp(prefix=f"{TEMP_DIR_PREFIX}{target.name}-", dir=target.parent))
    try:
        for name in sorted(files):
            path = staging / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(files[name])
            path.chmod(READ_ONLY_MODE)
        staging.rename(target)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return STATUS_CREATED
