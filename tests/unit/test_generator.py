"""Unit tests for the synthetic data generator: determinism, batch_id, immutability, format."""

import codecs
import hashlib
import json
import shutil
from collections import Counter
from pathlib import Path

import numpy as np
import pytest

from src.groundtruth.manifest import MANIFEST_FILE_NAME
from src.groundtruth.verify import load_dataset
from src.ingestion.defects import _Candidate, _OrderPicker
from src.ingestion.generate_data import generate
from src.ingestion.generator_config import (
    BATCH_METADATA_FILE,
    DELIVERY_EVENT_FIELDS,
    ORDER_COLUMNS,
    SHIFT_EVENT_COLUMNS,
    SOURCE_FILES,
    GenerationError,
    GeneratorConfig,
    parse_utc,
)
from src.ingestion.writers import (
    STATUS_CREATED,
    STATUS_UNCHANGED,
    TEMP_DIR_PREFIX,
    ImmutableOutputError,
    compute_batch_id,
    publish,
)
from tests.unit.generated import GeneratedDataset

pytestmark = pytest.mark.unit


def tree_hashes(root: Path) -> dict[str, str]:
    """sha256 of every file under ``root``, keyed by relative path."""
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


# ------------------------------------------------------------- determinism ---


def test_same_seed_and_config_give_byte_identical_files(
    dataset_a: GeneratedDataset, dataset_b: GeneratedDataset
) -> None:
    hashes_a = tree_hashes(dataset_a.root)
    hashes_b = tree_hashes(dataset_b.root)
    assert len(hashes_a) == 3 * 5 + 1  # 3 batches x (4 data files + batch.json) + manifest
    assert hashes_a == hashes_b


def test_dataset_name_contains_seed_size_and_config_hash(small_config: GeneratorConfig) -> None:
    expected = f"seed-7_orders-1500_cfg-{small_config.sha256()[:8]}"
    assert small_config.dataset_name() == expected


def test_any_config_change_changes_the_dataset_name(small_config: GeneratorConfig) -> None:
    other = GeneratorConfig.create(orders=1500, seed=7, drivers=small_config.n_drivers + 1)
    assert other.dataset_name() != small_config.dataset_name()


def test_too_small_settings_are_rejected() -> None:
    with pytest.raises(GenerationError, match="--orders"):
        GeneratorConfig.create(orders=10, seed=1)
    with pytest.raises(GenerationError, match="--drivers"):
        GeneratorConfig.create(orders=1500, seed=1, drivers=3)


# ---------------------------------------------------------------- batch_id ---


def test_batch_id_recomputes_from_data_files_and_config(dataset_a: GeneratedDataset) -> None:
    config = dataset_a.config
    for batch_dir in sorted(dataset_a.dataset_dir.glob("batch-*")):
        metadata = json.loads((batch_dir / BATCH_METADATA_FILE).read_text("utf-8"))
        files = {name: (batch_dir / name).read_bytes() for _, name in SOURCE_FILES}
        sequence = int(metadata["batch_sequence"])
        assert compute_batch_id(config.canonical_json(), sequence, files) == metadata["batch_id"]
        assert metadata["config_sha256"] == config.sha256()


def test_batch_ids_are_distinct_and_recorded_in_manifest(dataset_a: GeneratedDataset) -> None:
    recorded = [
        json.loads((batch_dir / BATCH_METADATA_FILE).read_text("utf-8"))["batch_id"]
        for batch_dir in sorted(dataset_a.dataset_dir.glob("batch-*"))
    ]
    assert recorded == list(dataset_a.manifest().batch_ids)
    assert len(set(recorded)) == len(recorded)


def test_batch_id_depends_only_on_data_files_config_and_sequence(
    small_config: GeneratorConfig,
) -> None:
    files = {"a.csv": b"x\n", "b.csv": b"y\n"}
    canonical = small_config.canonical_json()
    base = compute_batch_id(canonical, 1, files)
    assert compute_batch_id(canonical, 1, dict(files)) == base
    assert compute_batch_id(canonical, 1, {"a.csv": b"x\n", "b.csv": b"z\n"}) != base
    assert compute_batch_id(canonical, 2, files) != base
    other = GeneratorConfig.create(orders=1500, seed=8).canonical_json()
    assert compute_batch_id(other, 1, files) != base


def test_manifest_lives_outside_the_raw_tree(dataset_a: GeneratedDataset) -> None:
    assert not dataset_a.result.manifest_path.is_relative_to(dataset_a.raw_dir)
    assert not list(dataset_a.raw_dir.rglob(MANIFEST_FILE_NAME))


# ------------------------------------------------------------- immutability ---


def test_rerun_with_identical_output_is_a_no_op(dataset_a: GeneratedDataset) -> None:
    before = tree_hashes(dataset_a.root)
    result = generate(dataset_a.config, dataset_a.raw_dir, dataset_a.ground_truth_dir)
    assert result.raw_status == STATUS_UNCHANGED
    assert result.manifest_status == STATUS_UNCHANGED
    assert tree_hashes(dataset_a.root) == before


def test_changed_existing_file_is_refused_and_left_alone(
    dataset_a: GeneratedDataset, tmp_path: Path
) -> None:
    copy = tmp_path / "raw"
    shutil.copytree(dataset_a.raw_dir, copy)
    target = copy / dataset_a.config.dataset_name() / "batch-1" / "orders.csv"
    target.chmod(0o644)
    tampered = target.read_bytes() + b"tampered\n"
    target.write_bytes(tampered)
    with pytest.raises(ImmutableOutputError, match="different content"):
        generate(dataset_a.config, copy, tmp_path / "ground_truth")
    assert target.read_bytes() == tampered


def test_raw_files_are_read_only(dataset_a: GeneratedDataset) -> None:
    files = [path for path in dataset_a.root.rglob("*") if path.is_file()]
    assert files
    assert all(path.stat().st_mode & 0o222 == 0 for path in files)


def test_own_leftover_temp_folder_is_removed(tmp_path: Path) -> None:
    leftover = tmp_path / f"{TEMP_DIR_PREFIX}interrupted-run"
    leftover.mkdir()
    (leftover / "partial.csv").write_text("partial\n", encoding="utf-8")
    assert publish(tmp_path / "out", {"a.txt": b"a\n"}) == STATUS_CREATED
    assert not leftover.exists()


def test_foreign_hidden_folder_is_refused(tmp_path: Path) -> None:
    (tmp_path / ".someone-elses-folder").mkdir()
    with pytest.raises(ImmutableOutputError, match="Unexpected hidden folder"):
        publish(tmp_path / "out", {"a.txt": b"a\n"})
    assert not (tmp_path / "out").exists()


def test_hidden_files_are_ignored(tmp_path: Path) -> None:
    (tmp_path / ".gitkeep").write_text("", encoding="utf-8")
    assert publish(tmp_path / "out", {"a.txt": b"a\n"}) == STATUS_CREATED
    assert publish(tmp_path / "out", {"a.txt": b"a\n"}) == STATUS_UNCHANGED


# ------------------------------------------------------------------ format ---


def test_files_are_utf8_lf_without_bom(dataset_a: GeneratedDataset) -> None:
    for path in dataset_a.root.rglob("*"):
        if path.is_file():
            content = path.read_bytes()
            assert b"\r" not in content, path
            assert not content.startswith(codecs.BOM_UTF8), path
            content.decode("utf-8")


def test_rows_are_sorted_by_key_then_full_row(dataset_a: GeneratedDataset) -> None:
    for batch in load_dataset(dataset_a.dataset_dir):
        for source, rows in batch.rows.items():
            assert rows == sorted(rows, key=lambda row: (row[0], row)), (source, batch.sequence)


def test_csv_headers_match_the_contract(dataset_a: GeneratedDataset) -> None:
    batch_dir = dataset_a.dataset_dir / "batch-1"
    orders_header = (batch_dir / "orders.csv").read_text("utf-8").splitlines()[0]
    shifts_header = (batch_dir / "shift_events.csv").read_text("utf-8").splitlines()[0]
    assert orders_header == ",".join(ORDER_COLUMNS)
    assert shifts_header == ",".join(SHIFT_EVENT_COLUMNS)
    first_event = (batch_dir / "delivery_events.jsonl").read_text("utf-8").splitlines()[0]
    assert tuple(json.loads(first_event)) == DELIVERY_EVENT_FIELDS


def test_no_timestamp_is_at_or_after_the_reference(dataset_a: GeneratedDataset) -> None:
    reference = dataset_a.config.reference_epoch()
    stamps: list[int | None] = []
    for batch in load_dataset(dataset_a.dataset_dir):
        stamps += [parse_utc(row[4]) for row in batch.rows["delivery_events"]]
        stamps += [parse_utc(row[3]) for row in batch.rows["shift_events"]]
        stamps += [parse_utc(row[11]) for row in batch.rows["orders"]]
    assert stamps
    assert all(stamp is not None and stamp < reference for stamp in stamps)


def test_simulation_summary_splits_cancellations_by_cause(dataset_a: GeneratedDataset) -> None:
    summary = dataset_a.manifest().simulation_summary
    cancelled = summary["orders_cancelled_customer"] + summary["orders_cancelled_no_driver"]
    assert summary["orders_total"] == dataset_a.config.n_orders
    assert summary["orders_cancelled_customer"] > 0
    assert cancelled + summary["orders_delivered"] <= summary["orders_total"]


# ------------------------------------------------------------ defect sampling ---


def test_order_picker_draws_uniformly_from_the_remaining_eligible_pool() -> None:
    """A type restricted to batches 1-2 must not push later types towards batch 3."""
    candidates = [_Candidate(f"ORD-{i:07d}", i % 3 + 1, i, (), ()) for i in range(9000)]
    picker = _OrderPicker(candidates, np.random.default_rng(0))
    for _ in range(2000):
        picker.take("restricted", lambda c: c.batch < 3)
    later = Counter(picker.take("any", lambda c: True).batch for _ in range(3500))
    # About 2000 / 2000 / 3000 orders remain, so a uniform draw of 3500 gives ~1000 / 1000 / 1500.
    for batch, expected in ((1, 1000), (2, 1000), (3, 1500)):
        assert abs(later[batch] - expected) < 0.15 * expected, later
