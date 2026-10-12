"""The whole chain on the 1,500-order fixture: generate, clean, load, analytics, quality.

Generate is the session fixture; clean is ``run_transform`` (Parquet, offline path); load is
``load_dataset`` (PostgreSQL path, rolled back). Both paths must agree row by row, every
analyst query must return rows, the quality gate must pass, and the raw source files must be
byte-identical afterwards.
"""

import hashlib
from pathlib import Path

import psycopg
import pyarrow.parquet as pq
import pytest
from psycopg.rows import TupleRow

from src.database.analytics_queries import data_period, default_parameters, load_queries
from src.database.load_postgres import LOADED, load_dataset
from src.quality.checks import QualityConfig
from src.quality.run import run_database_quality
from src.transformations.pipeline import TransformConfig
from src.transformations.records import batch_folders
from src.transformations.transform import run_transform

pytestmark = pytest.mark.integration

Connection = psycopg.Connection[TupleRow]
CONFIG = TransformConfig()


def file_hashes(root: Path) -> dict[str, str]:
    """sha256 of every file below ``root``, by relative path."""
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_generate_clean_load_analytics_and_quality(
    test_db: Connection, fixture_dataset: Path, tmp_path: Path
) -> None:
    raw_before = file_hashes(fixture_dataset)

    # clean: the offline Parquet path
    processed = tmp_path / "processed" / fixture_dataset.name
    results = run_transform(fixture_dataset, processed, CONFIG)
    folders = batch_folders(fixture_dataset)
    assert len(results) == len(folders)

    # load: the PostgreSQL path
    loads = load_dataset(test_db, fixture_dataset, CONFIG)
    assert [load.status for load in loads] == [LOADED] * len(folders)
    assert all(load.quality is not None and not load.quality.blocking_failures for load in loads)

    # both paths give the same disposition for every input row
    for folder, load in zip(folders, loads, strict=True):
        parquet = pq.read_table(processed / "batches" / folder.name / "dispositions.parquet")
        from_parquet = sorted(
            (r["source"], r["row_number"], r["record_key"], r["disposition"], r["reason"])
            for r in parquet.to_pylist()
        )
        stored = test_db.execute(
            "SELECT source, row_number, record_key, disposition, reason "
            "FROM audit.record_dispositions WHERE batch_id = %s",
            (load.batch_id,),
        ).fetchall()
        assert sorted(tuple(row) for row in stored) == from_parquet, folder.name
        assert len(from_parquet) > 0

    # analytics: every analyst query returns rows on the loaded data
    first, last = data_period(test_db)
    parameters = default_parameters(first, last)
    for name, text in load_queries().items():
        assert test_db.execute(text, parameters[name]).fetchall(), name

    # quality: the whole-database run has no blocking failure
    manual, _ = run_database_quality(test_db, QualityConfig())
    assert manual.blocking_failures == []

    # the raw source files are untouched by both paths
    assert file_hashes(fixture_dataset) == raw_before
