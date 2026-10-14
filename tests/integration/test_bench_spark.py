"""The Spark engine must give exactly the pandas engine's results (marker ``spark``:
`make test-spark`; needs a Java 21 runtime, no database and no object store).

Both engines implement the same workload (src/bench/spec.py). These tests run them over the same
input and compare every count and every aggregate row. Spark's scratch space and temporary files
go into the test's own folder and are removed with it.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from src.bench import pandas_engine
from src.bench.reads import WEEK, pandas_week_counts, write_read_copies
from src.bench.shards import convert_dataset
from src.bench.spec import WorkloadResult

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def shard(fixture_dataset: Path, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The 1,500-order fixture dataset converted to one benchmark shard."""
    target = tmp_path_factory.mktemp("bench_shard") / "shard-0001"
    convert_dataset(fixture_dataset, target, 1)
    return target


@pytest.fixture(scope="module")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    """A local SparkSession whose scratch space is inside the test's temporary folder."""
    from src.bench import spark_engine

    root = tmp_path_factory.mktemp("spark_work")
    session = spark_engine.session(root / "spark-local", root / "tmp", app="deliverypulse-tests")
    yield session
    session.stop()


def test_java_21_is_used() -> None:
    """`make test-spark` resolves JAVA_HOME with java_home -v 21 and fails without it."""
    java_home = os.environ.get("JAVA_HOME", "")
    assert java_home, "JAVA_HOME is not set; run make test-spark"
    release = (Path(java_home) / "release").read_text(encoding="utf-8")
    assert 'JAVA_VERSION="21' in release, release.splitlines()[:3]


def test_both_engines_give_identical_results(spark: SparkSession, shard: Path) -> None:
    from src.bench import spark_engine

    from_pandas = pandas_engine.run_workload([shard])
    from_spark = spark_engine.run_workload(spark, [shard])

    assert from_spark.to_json() == from_pandas.to_json()
    assert from_pandas.orders_input > 0 and from_pandas.events_input > 0
    assert from_pandas.orders_rejected > 0 and from_pandas.orders_duplicate > 0
    assert from_pandas.events_rejected > 0 and from_pandas.events_duplicate > 0
    assert from_pandas.gps_flagged > 0  # the generator injects impossible GPS speeds


def test_both_engines_agree_on_several_shards(
    spark: SparkSession, shard: Path, tmp_path: Path
) -> None:
    """Two copies of the shard: the engines must agree on the doubled input too."""
    from src.bench import spark_engine

    second = tmp_path / "shard-0002"
    second.mkdir()
    for name in ("orders.parquet", "delivery_events.parquet"):
        (second / name).write_bytes((shard / name).read_bytes())
    shards = [shard, second]

    from_pandas = pandas_engine.run_workload(shards)
    from_spark = spark_engine.run_workload(spark, shards)

    assert from_spark.to_json() == from_pandas.to_json()
    # The second copy repeats every id, so every row of it is a duplicate.
    single = pandas_engine.run_workload([shard])
    assert from_pandas.orders_input == single.orders_input * 2
    assert from_pandas.orders_clean == single.orders_clean


def test_both_engines_agree_on_the_read_benchmark_queries(
    spark: SparkSession, shard: Path, tmp_path: Path
) -> None:
    from src.bench import spark_engine

    single, partitioned = write_read_copies([shard], tmp_path / "reads")

    pandas_single = pandas_week_counts(single)
    pandas_partitioned = pandas_week_counts(partitioned)
    spark_single = spark_engine.week_counts(spark, single, WEEK)
    spark_partitioned = spark_engine.week_counts(spark, partitioned, WEEK)

    assert pandas_single == pandas_partitioned
    assert spark_single == spark_partitioned == pandas_single
    assert sum(row[1] for row in pandas_single) > 0


def test_workload_result_json_is_stable_across_engines(spark: SparkSession, shard: Path) -> None:
    """The JSON the harness compares must round-trip for both engines."""
    from src.bench import spark_engine

    for result in (pandas_engine.run_workload([shard]), spark_engine.run_workload(spark, [shard])):
        assert WorkloadResult.from_json(result.to_json()) == result
