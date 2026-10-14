"""Bounded insert-only loading, with changed-content rejection and safe restart."""

import argparse
import json
import resource
import sys
import time
from collections.abc import Iterable
from datetime import UTC, datetime
from itertools import batched
from pathlib import Path
from typing import Any, Protocol

from src.bench.disk import MIN_FREE_BYTES, DiskGuardError, free_bytes
from src.nosql.client import connect
from src.nosql.documents import SourceChangedError, batch_documents, same_content, verified_metadata
from src.transformations.records import batch_folders
from src.transformations.transform import resolve_dataset

CHUNK_SIZE = 5000


class RawCollection(Protocol):
    """Minimal storage interface for live and fault-injection tests."""

    def find(self, filter: dict[str, Any]) -> Iterable[dict[str, Any]]: ...
    def insert_many(self, documents: list[dict[str, Any]], *, ordered: bool) -> Any: ...


def insert_chunk(collection: RawCollection, documents: list[dict[str, Any]]) -> tuple[int, int]:
    """Check existing content before inserting missing rows; never update existing data."""
    identifiers = [doc["_id"] for doc in documents]
    if len(set(identifiers)) != len(identifiers):
        raise SourceChangedError("Repeated source-row identity within a load chunk")
    existing = {doc["_id"]: doc for doc in collection.find({"_id": {"$in": identifiers}})}
    for doc in documents:
        if doc["_id"] in existing and not same_content(existing[doc["_id"]], doc):
            raise SourceChangedError("Immutable source changed at an existing source-row identity")
    now = datetime.now(UTC)
    pending = [{**doc, "ingested_at": now} for doc in documents if doc["_id"] not in existing]
    if pending:
        collection.insert_many(pending, ordered=True)
    return len(pending), len(existing)


def check_disk(path: Path) -> None:
    """Check host free space; Docker virtual-disk capacity needs separate stats inspection."""
    if min(free_bytes(Path("/")), free_bytes(path)) < MIN_FREE_BYTES:
        raise DiskGuardError("MongoDB load requires at least 10 GiB free host disk")


def load_dataset(collection: RawCollection, folder: Path) -> dict[str, Any]:
    """Verify every source first, then stream bounded chunks in batch order."""
    started = time.perf_counter()
    check_disk(folder)
    folders = batch_folders(folder)
    if not folders:
        raise ValueError("No raw batch folders found")
    manifests = [(batch, verified_metadata(batch)) for batch in folders]
    inserted = duplicate = rows = 0
    for batch, metadata in manifests:
        for chunk in batched(batch_documents(batch, metadata), CHUNK_SIZE):
            check_disk(folder)
            added, seen = insert_chunk(collection, list(chunk))
            inserted += added
            duplicate += seen
            rows += len(chunk)
        verified_metadata(batch)
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return {
        "dataset": folder.name,
        "rows": rows,
        "inserted": inserted,
        "duplicate": duplicate,
        "modified": 0,
        "seconds": time.perf_counter() - started,
        "host_peak_rss_mib": peak / (1024**2 if sys.platform == "darwin" else 1024),
    }


def main() -> int:
    """Print only counts and timings; never driver exceptions or credentials."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--dataset")
    args = parser.parse_args()
    try:
        folder = resolve_dataset(args.raw_dir, args.dataset)
        client, database = connect("app")
        with client:
            result = load_dataset(database["raw_events"], folder)
        print(json.dumps(result, sort_keys=True))
    except (SourceChangedError, DiskGuardError) as exc:
        print(f"ERROR: {exc}")
        return 1
    except Exception:
        print("ERROR: MongoDB load failed; check service, credentials and source metadata")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
