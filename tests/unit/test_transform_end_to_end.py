"""End to end on the 1,500-order fixture: transform, then reconcile exactly with the manifest."""

import hashlib
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from src.groundtruth.reconcile import reconcile
from src.transformations.pipeline import TransformConfig
from src.transformations.transform import run_transform
from tests.unit.generated import GeneratedDataset

pytestmark = pytest.mark.unit


def tree_hashes(root: Path) -> dict[str, str]:
    """sha256 of every file under ``root``, keyed by relative path."""
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


@pytest.fixture(scope="module")
def transformed(dataset_a: GeneratedDataset, tmp_path_factory: pytest.TempPathFactory) -> Path:
    output = tmp_path_factory.mktemp("processed") / dataset_a.config.dataset_name()
    run_transform(dataset_a.dataset_dir, output, TransformConfig())
    return output


def test_reconciliation_equals_the_manifest_for_every_source_and_batch(
    dataset_a: GeneratedDataset, transformed: Path
) -> None:
    assert reconcile(dataset_a.dataset_dir, transformed, dataset_a.manifest()) == []


def test_rerun_gives_identical_output_and_leaves_raw_files_untouched(
    dataset_a: GeneratedDataset, transformed: Path
) -> None:
    raw_before = tree_hashes(dataset_a.dataset_dir)
    first = tree_hashes(transformed)
    run_transform(dataset_a.dataset_dir, transformed, TransformConfig())
    assert tree_hashes(transformed) == first
    assert tree_hashes(dataset_a.dataset_dir) == raw_before
    assert not [p for p in transformed.parent.iterdir() if p.name.startswith(".transform-")]


def test_cleaned_layer_is_partitioned_by_reporting_date(transformed: Path) -> None:
    for source in ("orders", "delivery_events", "shift_events"):
        partitions = sorted(p.name for p in (transformed / "cleaned" / source).iterdir())
        assert partitions, source
        assert all(name.startswith("date=2026-09-") for name in partitions), partitions
    assert (transformed / "cleaned" / "drivers" / "part-0.parquet").is_file()


def test_cleaned_orders_carry_delivery_minutes_and_is_late(transformed: Path) -> None:
    rows: list[dict[str, Any]] = []
    for part in sorted((transformed / "cleaned" / "orders").glob("date=*/part-0.parquet")):
        rows += pq.read_table(part).to_pylist()
    delivered = [row for row in rows if row["status"] == "delivered"]
    others = [row for row in rows if row["status"] != "delivered"]
    assert delivered and others
    assert all(row["delivery_minutes"] > 0 and row["is_late"] is not None for row in delivered)
    assert all(row["delivery_minutes"] is None and row["is_late"] is None for row in others)
    assert len({row["order_id"] for row in rows}) == len(rows)
