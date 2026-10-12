"""Defect manifest: the exact list of injected defects (and natural updates) of a dataset.

The manifest is written next to, never inside, the raw data (``data/ground_truth/<dataset>/``)
and is not part of any ``batch_id``. It is deterministic JSON: sorted keys, two-space indent,
ASCII, ``\\n`` line endings.
"""

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.contract import ACCEPTED as ACCEPTED
from src.contract import DISPOSITIONS as DISPOSITIONS
from src.contract import DUPLICATE as DUPLICATE
from src.contract import PENDING as PENDING
from src.contract import REJECTED as REJECTED
from src.contract import STALE_IGNORED as STALE_IGNORED

MANIFEST_FILE_NAME = "defect_manifest.json"
MANIFEST_VERSION = 1


@dataclass(frozen=True)
class Case:
    """One record with a known expected disposition.

    ``scenario`` groups the cases created by one injection (for example a delayed order and its
    orphan events); different scenarios never share a record, order or driver.
    """

    scenario: str
    defect_type: str
    source: str
    batch: int
    record_key: str
    expected_disposition: str
    reason: str = ""
    related_keys: tuple[str, ...] = ()
    injected: bool = True

    def sort_key(self) -> tuple[int, str, str, str, str]:
        """Deterministic manifest order."""
        return (self.batch, self.source, self.record_key, self.defect_type, self.scenario)


@dataclass(frozen=True)
class Manifest:
    """Ground truth of one generated dataset."""

    dataset: str
    config: dict[str, Any]
    config_sha256: str
    batch_ids: tuple[str, ...]
    simulation_summary: dict[str, int]
    cases: tuple[Case, ...]


def case_counts(cases: tuple[Case, ...] | list[Case]) -> dict[str, dict[str, int]]:
    """Number of cases per defect type per batch (batch keys as strings, sorted)."""
    counter = Counter((case.defect_type, case.batch) for case in cases)
    counts: dict[str, dict[str, int]] = {}
    for (defect_type, batch), count in sorted(counter.items()):
        counts.setdefault(defect_type, {})[str(batch)] = count
    return counts


def manifest_bytes(manifest: Manifest) -> bytes:
    """Serialise ``manifest`` deterministically."""
    payload = {
        "manifest_version": MANIFEST_VERSION,
        "dataset": manifest.dataset,
        "config": manifest.config,
        "config_sha256": manifest.config_sha256,
        "batches": [
            {"sequence": sequence, "batch_id": batch_id}
            for sequence, batch_id in enumerate(manifest.batch_ids, start=1)
        ],
        "simulation_summary": manifest.simulation_summary,
        "counts": case_counts(manifest.cases),
        "cases": [
            {
                "scenario": case.scenario,
                "defect_type": case.defect_type,
                "source": case.source,
                "batch": case.batch,
                "record_key": case.record_key,
                "expected_disposition": case.expected_disposition,
                "reason": case.reason,
                "related_keys": list(case.related_keys),
                "injected": case.injected,
            }
            for case in sorted(manifest.cases, key=Case.sort_key)
        ],
    }
    text = json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True)
    return (text + "\n").encode("utf-8")


def load_manifest(path: Path) -> Manifest:
    """Read a manifest written by :func:`manifest_bytes`."""
    payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("manifest_version") != MANIFEST_VERSION:
        raise ValueError(f"Unsupported manifest version in {path}.")
    cases = tuple(
        Case(
            scenario=str(item["scenario"]),
            defect_type=str(item["defect_type"]),
            source=str(item["source"]),
            batch=int(item["batch"]),
            record_key=str(item["record_key"]),
            expected_disposition=str(item["expected_disposition"]),
            reason=str(item["reason"]),
            related_keys=tuple(str(key) for key in item["related_keys"]),
            injected=bool(item["injected"]),
        )
        for item in payload["cases"]
    )
    batches = sorted(payload["batches"], key=lambda item: int(item["sequence"]))
    summary = {str(key): int(value) for key, value in payload["simulation_summary"].items()}
    config: dict[str, Any] = payload["config"]
    return Manifest(
        dataset=str(payload["dataset"]),
        config=config,
        config_sha256=str(payload["config_sha256"]),
        batch_ids=tuple(str(item["batch_id"]) for item in batches),
        simulation_summary=summary,
        cases=cases,
    )
