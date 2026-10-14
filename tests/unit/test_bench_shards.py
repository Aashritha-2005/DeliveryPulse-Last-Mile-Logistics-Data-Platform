"""Unit tests of the benchmark shard builder and the harness helpers (no generation)."""

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from src.bench.monitor import SIZES, jdk_version, machine_record, median, sizes_up_to
from src.bench.shards import (
    ROW_ID_STRIDE,
    convert_dataset,
    shard_dir,
    shard_prefix,
    shard_seed,
    source_table,
)
from src.bench.spec import EVENTS_FILE, ORDER_INPUT_COLUMNS, ORDERS_FILE, ROW_ID
from src.contract import SOURCE_ORDERS
from src.transformations.records import Batch, RawRecord, batch_folders, load_batch
from tests.unit.generated import GeneratedDataset
from tests.unit.transform_helpers import order

pytestmark = pytest.mark.unit


def test_shard_one_is_the_default_seed_and_ids_are_prefixed_per_shard() -> None:
    assert shard_seed(1) == 42
    assert shard_seed(7) == 48
    assert shard_prefix(1) == "S0001-"
    assert shard_prefix(42) == "S0042-"


def test_source_table_prefixes_ids_keeps_other_text_and_numbers_row_ids() -> None:
    batch = Batch(
        sequence=1,
        batch_id="batch-1",
        window_start=0,
        window_end=1,
        records={
            "drivers": [],
            "orders": [
                RawRecord(SOURCE_ORDERS, 1, order(order_id="ORD-1", driver_id=""), "{}"),
                RawRecord(SOURCE_ORDERS, 2, order(order_id="ORD-2"), "{}"),
            ],
            "delivery_events": [],
            "shift_events": [],
        },
    )

    table = source_table(batch, SOURCE_ORDERS, index=3, first_row=10)

    assert table.column("order_id").to_pylist() == ["S0003-ORD-1", "S0003-ORD-2"]
    assert table.column("driver_id").to_pylist()[0] == ""  # empty stays empty
    assert table.column("city").to_pylist()[0] == order()["city"]  # not an id: unchanged
    assert table.column(ROW_ID).to_pylist() == [3 * ROW_ID_STRIDE + 10, 3 * ROW_ID_STRIDE + 11]
    assert set(table.column_names) == set(ORDER_INPUT_COLUMNS)


def test_row_ids_are_unique_across_shards() -> None:
    first = source_table(_one_order_batch(), SOURCE_ORDERS, index=1, first_row=0)
    second = source_table(_one_order_batch(), SOURCE_ORDERS, index=2, first_row=0)

    assert set(first.column(ROW_ID).to_pylist()).isdisjoint(second.column(ROW_ID).to_pylist())


def _one_order_batch() -> Batch:
    return Batch(
        sequence=1,
        batch_id="batch-1",
        window_start=0,
        window_end=1,
        records={
            "drivers": [],
            "orders": [RawRecord(SOURCE_ORDERS, 1, order(), "{}")],
            "delivery_events": [],
            "shift_events": [],
        },
    )


def test_convert_dataset_writes_both_sources_with_every_input_row(
    dataset_a: GeneratedDataset, tmp_path: Path
) -> None:
    rows = convert_dataset(dataset_a.dataset_dir, tmp_path / "shard-0001", 1)

    expected = {"orders": 0, "delivery_events": 0}
    for folder in batch_folders(dataset_a.dataset_dir):
        batch = load_batch(folder)
        for source in expected:
            expected[source] += len(batch.records[source])
    assert rows == expected
    for source, name in (("orders", ORDERS_FILE), ("delivery_events", EVENTS_FILE)):
        assert pq.read_metadata(tmp_path / "shard-0001" / name).num_rows == expected[source]


def test_shard_builder_discards_the_generators_ground_truth() -> None:
    """The shard builder never reads the manifest: it only names a directory it then deletes.

    Guards the allowlist entry in tests/unit/test_groundtruth.py.
    """
    source = Path("src/bench/shards.py").read_text(encoding="utf-8")

    assert "src.groundtruth" not in source
    assert "defect_manifest" not in source
    assert source.count("ground_truth") == 1  # the destination passed to generate()
    assert 'generate(config, work / "raw", work / "ground_truth")' in source
    assert "remove_tree(work)" in source  # deleted in the finally block


def test_shard_dir_is_numbered_and_sorts(tmp_path: Path) -> None:
    names = [shard_dir(tmp_path, index).name for index in (1, 2, 10)]

    assert names == ["shard-0001", "shard-0002", "shard-0010"]
    assert names == sorted(names)


# ------------------------------------------------------------------ harness helpers ---


def test_sizes_up_to_returns_the_plan_smallest_first() -> None:
    assert sizes_up_to("500k") == [("50k", 1), ("250k", 5), ("500k", 10)]
    assert sizes_up_to("2m")[-1] == ("2m", 40)
    assert list(SIZES) == ["50k", "250k", "500k", "1m", "2m", "5m"]


def test_sizes_up_to_rejects_an_unknown_size() -> None:
    with pytest.raises(ValueError, match="Unknown size"):
        sizes_up_to("3m")


def test_median_of_measured_runs() -> None:
    assert median([3.0, 1.0, 2.0]) == 2.0
    assert median([2.0]) == 2.0


def test_machine_record_keeps_only_the_allowed_fields() -> None:
    record = machine_record(
        {
            "hardware_model": "Mac16,6",
            "cpu_cores": 10,
            "ram_bytes": 17179869184,
            "os": "macOS 26.6.2",
            "python": "3.12.10",
            "jdk": "21.0.12.1",
            "packages": {"pyspark": "4.2.0"},
            "hostname": "a-laptop",
            "user": "someone",
            "cwd": "/Users/someone/DeliveryPulse",
        }
    )

    assert set(record) == {
        "hardware_model",
        "cpu_cores",
        "ram_bytes",
        "os",
        "python",
        "jdk",
        "packages",
    }
    assert "someone" not in str(record)


def test_jdk_version_reads_the_release_file() -> None:
    release = (
        'IMPLEMENTOR="Eclipse Adoptium"\nJAVA_VERSION="21.0.12"\nIMPLEMENTOR_VERSION="21.0.12.1"\n'
    )

    assert jdk_version(release) == "21.0.12 (21.0.12.1)"
    assert jdk_version('JAVA_VERSION="21.0.12"\n') == "21.0.12"
    assert jdk_version("") == ""
