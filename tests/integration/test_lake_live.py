"""Tests against the live object store (marker ``s3``: `make up`, `make s3-up`, `make test-s3`).

They use the pipeline key from .env, write only under a unique ``test-<hex>/`` prefix of the lake
bucket and delete it afterwards, and load the 1,500-order fixture into the rolled-back test
database. The admin key is used only to prove that the pipeline key is limited to its bucket.
"""

from collections.abc import Iterator
from pathlib import Path
from typing import Any
from uuid import uuid4

import boto3
import psycopg
import pytest
from botocore.exceptions import ClientError
from psycopg.rows import TupleRow

from src.config import S3Settings, load_s3_settings
from src.database.load_postgres import load_dataset
from src.lake.check import check_lake
from src.lake.export import build_lake, export_lake, read_aggregates
from src.lake.s3 import (
    S3Client,
    bucket_exists,
    create_client,
    delete_keys,
    error_code,
    list_objects,
    put_bytes,
)
from src.transformations.pipeline import TransformConfig
from src.transformations.transform import run_transform

pytestmark = pytest.mark.s3

Connection = psycopg.Connection[TupleRow]


@pytest.fixture(scope="module")
def pipeline() -> tuple[S3Client, S3Settings]:
    settings = load_s3_settings("pipeline")
    return create_client(settings), settings


@pytest.fixture
def prefix(pipeline: tuple[S3Client, S3Settings]) -> Iterator[str]:
    """A unique prefix in the lake bucket, removed after the test."""
    client, settings = pipeline
    name = f"test-{uuid4().hex}/"
    yield name
    leftovers = list_objects(client, settings.bucket, name)
    delete_keys(client, settings.bucket, [item.key for item in leftovers])


def admin_client() -> Any:
    settings = load_s3_settings("admin")
    return boto3.client(
        "s3",
        endpoint_url=settings.endpoint_url,
        aws_access_key_id=settings.access_key_id,
        aws_secret_access_key=settings.secret_access_key,
        region_name=settings.region,
    )


def test_export_and_check_round_trip_through_the_live_store(
    test_db: Connection,
    fixture_dataset: Path,
    tmp_path: Path,
    pipeline: tuple[S3Client, S3Settings],
    prefix: str,
) -> None:
    client, settings = pipeline
    config = TransformConfig()
    load_dataset(test_db, fixture_dataset, config)
    aggregates = read_aggregates(test_db)
    processed = tmp_path / "processed" / fixture_dataset.name
    run_transform(fixture_dataset, processed, config)
    objects = list(build_lake(fixture_dataset, processed / "cleaned", aggregates, prefix))

    first = export_lake(client, settings.bucket, objects, prefix)
    checked = check_lake(client, settings.bucket, objects, prefix)
    reports = {report.layer: report for report in checked}

    assert first.stale_deleted == 0
    assert sum(report.objects for report in reports.values()) == len(objects)
    database_rows = sum(len(rows) for rows in aggregates.values())
    assert reports["aggregated"].rows == database_rows > 0
    assert reports["raw"].rows > 0 and reports["cleaned"].rows > 0

    # A replay writes the same keys and bytes; the check still passes.
    second = export_lake(client, settings.bucket, objects, prefix)
    assert second.stale_deleted == 0
    check_lake(client, settings.bucket, objects, prefix)

    # A key the export no longer produces is removed, and only after the upload.
    stale = f"{prefix}raw/orders/date=2000-01-01/batch-09.parquet"
    put_bytes(client, settings.bucket, stale, b"old", "application/octet-stream")
    third = export_lake(client, settings.bucket, objects, prefix)
    assert third.stale_deleted == 1
    check_lake(client, settings.bucket, objects, prefix)


def test_pipeline_key_sees_the_bucket_but_cannot_create_buckets(
    pipeline: tuple[S3Client, S3Settings],
) -> None:
    client, settings = pipeline

    assert bucket_exists(client, settings.bucket)
    with pytest.raises(ClientError) as excinfo:
        client.create_bucket(Bucket=f"dp-forbidden-{uuid4().hex[:12]}")
    assert error_code(excinfo.value) == "AccessDenied"


def test_pipeline_key_cannot_write_to_another_bucket(
    pipeline: tuple[S3Client, S3Settings],
) -> None:
    client, _ = pipeline
    admin = admin_client()
    other = f"dp-other-{uuid4().hex[:12]}"
    admin.create_bucket(Bucket=other)
    try:
        with pytest.raises(ClientError) as excinfo:
            put_bytes(client, other, "probe.txt", b"x", "text/plain")
        assert error_code(excinfo.value) == "AccessDenied"
    finally:
        admin.delete_bucket(Bucket=other)
