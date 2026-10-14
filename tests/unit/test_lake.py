"""Unit tests of the S3 lake (no services): an in-memory S3 fake stands in for the store."""

import io
from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError

from src.contract import SOURCE_FILES, format_reporting_date
from src.lake import bootstrap
from src.lake.check import LakeCheckError, check_lake
from src.lake.export import (
    LakeExportError,
    LakeObject,
    aggregated_objects,
    build_lake,
    cleaned_objects,
    export_lake,
    ingestion_date,
    raw_objects,
    validate_prefix,
)
from src.lake.s3 import (
    LakeS3Error,
    bucket_exists,
    create_bucket,
    delete_keys,
    download_bytes,
    list_objects,
    put_bytes,
)
from src.transformations.pipeline import TransformConfig
from src.transformations.records import batch_folders, load_batch
from src.transformations.transform import run_transform
from tests.unit.generated import GeneratedDataset

pytestmark = pytest.mark.unit

BUCKET = "deliverypulse"


def client_error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


class Body:
    """Minimal stand-in for boto3's StreamingBody."""

    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def read(self) -> bytes:
        return self.payload


class FakeS3:
    """In-memory S3 client with an operation log, paging and injectable failures."""

    def __init__(self, page_size: int = 1000) -> None:
        self.buckets: set[str] = {BUCKET}
        self.objects: dict[tuple[str, str], bytes] = {}
        self.log: list[str] = []
        self.page_size = page_size
        self.create_calls: list[dict[str, Any]] = []
        self.failing_deletes: set[str] = set()
        self.delete_batch_sizes: list[int] = []
        self.head_error: str | None = None

    def create_bucket(self, **kwargs: Any) -> Mapping[str, Any]:
        self.create_calls.append(kwargs)
        self.buckets.add(str(kwargs["Bucket"]))
        return {}

    def head_bucket(self, **kwargs: Any) -> Mapping[str, Any]:
        if self.head_error is not None:
            raise client_error(self.head_error, "HeadBucket")
        if kwargs["Bucket"] not in self.buckets:
            raise client_error("404", "HeadBucket")
        return {}

    def put_object(self, **kwargs: Any) -> Mapping[str, Any]:
        self.log.append(f"put {kwargs['Key']}")
        self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))] = bytes(kwargs["Body"])
        return {}

    def get_object(self, **kwargs: Any) -> Mapping[str, Any]:
        return {"Body": Body(self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))])}

    def delete_objects(self, **kwargs: Any) -> Mapping[str, Any]:
        bucket = str(kwargs["Bucket"])
        errors = []
        self.delete_batch_sizes.append(len(kwargs["Delete"]["Objects"]))
        for item in kwargs["Delete"]["Objects"]:
            key = str(item["Key"])
            self.log.append(f"delete {key}")
            if key in self.failing_deletes:
                errors.append({"Key": key, "Code": "AccessDenied", "Message": "denied"})
            else:
                self.objects.pop((bucket, key), None)
        return {"Errors": errors} if errors else {}

    def list_objects_v2(self, **kwargs: Any) -> Mapping[str, Any]:
        bucket = str(kwargs["Bucket"])
        prefix = str(kwargs.get("Prefix", ""))
        keys = sorted(k for b, k in self.objects if b == bucket and k.startswith(prefix))
        start = int(kwargs.get("ContinuationToken", "0"))
        page = keys[start : start + self.page_size]
        response: dict[str, Any] = {
            "Contents": [{"Key": k, "Size": len(self.objects[(bucket, k)])} for k in page],
            "IsTruncated": start + self.page_size < len(keys),
        }
        if response["IsTruncated"]:
            response["NextContinuationToken"] = str(start + self.page_size)
        return response


def daily_row(day: date, city: str) -> dict[str, Any]:
    return {
        "metric_date": day,
        "city": city,
        "total_orders": 3,
        "avg_delivery_minutes": Decimal("14.25"),
        "late_rate": Decimal("0.5000"),
        "cancelled_rate": Decimal("0.3333"),
        "delivered_orders": 2,
        "late_orders": 1,
        "cancelled_orders": 1,
    }


def driver_row(day: date, driver_id: str) -> dict[str, Any]:
    return {
        "driver_id": driver_id,
        "metric_date": day,
        "deliveries": 2,
        "avg_delivery_minutes": Decimal("12.00"),
        "utilisation_pct": None,
        "busy_minutes": Decimal("24.00"),
        "online_minutes": None,
    }


AGGREGATES = {
    "daily_delivery_metrics": [
        daily_row(date(2026, 9, 10), "Bengaluru"),
        daily_row(date(2026, 9, 10), "Delhi"),
        daily_row(date(2026, 9, 11), "Delhi"),
    ],
    "driver_performance": [driver_row(date(2026, 9, 11), "DRV-00001")],
}


@pytest.fixture(scope="module")
def cleaned_dir(dataset_a: GeneratedDataset, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """`make transform` output of the small generated dataset."""
    processed = tmp_path_factory.mktemp("lake_processed") / dataset_a.dataset_dir.name
    run_transform(dataset_a.dataset_dir, processed, TransformConfig())
    return processed / "cleaned"


@pytest.fixture(scope="module")
def lake(dataset_a: GeneratedDataset, cleaned_dir: Path) -> list[LakeObject]:
    return build_lake(dataset_a.dataset_dir, cleaned_dir, AGGREGATES)


# ------------------------------------------------------------------------- S3 helpers ---


def test_list_objects_follows_continuation_tokens() -> None:
    client = FakeS3(page_size=2)
    for index in range(5):
        put_bytes(client, BUCKET, f"raw/k{index}", b"x", "t")

    assert [item.key for item in list_objects(client, BUCKET, "raw/")] == [
        f"raw/k{index}" for index in range(5)
    ]


def test_delete_keys_raises_on_reported_errors() -> None:
    client = FakeS3()
    put_bytes(client, BUCKET, "raw/a", b"x", "t")
    put_bytes(client, BUCKET, "raw/b", b"x", "t")
    client.failing_deletes = {"raw/b"}

    with pytest.raises(LakeS3Error, match=r"1 of 2 deletes failed; first: raw/b \(AccessDenied"):
        delete_keys(client, BUCKET, ["raw/a", "raw/b"])


def test_delete_keys_sends_at_most_1000_keys_per_request() -> None:
    client = FakeS3()

    assert delete_keys(client, BUCKET, [f"k{index:04d}" for index in range(2500)]) == 2500
    assert client.delete_batch_sizes == [1000, 1000, 500]


def test_create_bucket_sets_location_constraint_outside_us_east_1() -> None:
    client = FakeS3()
    client.buckets.clear()

    assert create_bucket(client, "lake-a", "us-east-1") is True
    assert create_bucket(client, "lake-b", "ap-south-1") is True
    assert create_bucket(client, "lake-b", "ap-south-1") is False  # already exists
    assert client.create_calls == [
        {"Bucket": "lake-a"},
        {"Bucket": "lake-b", "CreateBucketConfiguration": {"LocationConstraint": "ap-south-1"}},
    ]


def test_bucket_exists_reraises_errors_other_than_not_found() -> None:
    client = FakeS3()
    client.head_error = "AccessDenied"

    with pytest.raises(ClientError):
        bucket_exists(client, BUCKET)


@pytest.mark.parametrize("prefix", ["", "test-run/", "a/b/"])
def test_valid_prefixes(prefix: str) -> None:
    assert validate_prefix(prefix) == prefix


@pytest.mark.parametrize("prefix", ["test-run", "/abs/", "a//b/"])
def test_invalid_prefixes(prefix: str) -> None:
    with pytest.raises(LakeExportError, match="Lake prefix"):
        validate_prefix(prefix)


# ----------------------------------------------------------------------------- layers ---


def test_raw_layer_has_one_object_per_batch_and_source_by_ingestion_date(
    dataset_a: GeneratedDataset,
) -> None:
    objects = {item.key: item for item in raw_objects(dataset_a.dataset_dir)}
    folders = batch_folders(dataset_a.dataset_dir)

    assert len(objects) == len(folders) * len(SOURCE_FILES)
    for folder in folders:
        batch = load_batch(folder)
        day = format_reporting_date(batch.window_end)
        assert ingestion_date(batch) == day
        for source, source_file in SOURCE_FILES:
            item = objects[f"raw/{source}/date={day}/batch-{batch.sequence:02d}.parquet"]
            table = pq.read_table(io.BytesIO(item.body))
            assert item.rows == table.num_rows == len(batch.records[source])
            assert set(table.column("source_file").to_pylist()) <= {source_file}
            assert table.column("raw_payload").to_pylist() == [
                record.raw for record in batch.records[source]
            ]


def test_cleaned_layer_mirrors_the_transform_output(cleaned_dir: Path) -> None:
    objects = cleaned_objects(cleaned_dir)
    local = sorted(cleaned_dir.rglob("*.parquet"))

    assert [item.key for item in objects] == [
        f"cleaned/{path.relative_to(cleaned_dir).as_posix()}" for path in local
    ]
    assert [item.body for item in objects] == [path.read_bytes() for path in local]
    keys = {item.key for item in objects}
    assert "cleaned/drivers/part-0.parquet" in keys  # drivers are not partitioned
    for source in ("orders", "delivery_events", "shift_events"):
        assert any(key.startswith(f"cleaned/{source}/date=") for key in keys)


def test_cleaned_layer_requires_the_transform_output(tmp_path: Path) -> None:
    with pytest.raises(LakeExportError, match="run make transform"):
        cleaned_objects(tmp_path / "missing")


def test_aggregated_layer_is_partitioned_by_metric_date() -> None:
    objects = {item.key: item for item in aggregated_objects(AGGREGATES)}

    assert sorted(objects) == [
        "aggregated/daily_delivery_metrics/date=2026-09-10/part-0.parquet",
        "aggregated/daily_delivery_metrics/date=2026-09-11/part-0.parquet",
        "aggregated/driver_performance/date=2026-09-11/part-0.parquet",
    ]
    first = objects["aggregated/daily_delivery_metrics/date=2026-09-10/part-0.parquet"]
    table = pq.read_table(io.BytesIO(first.body))
    assert first.rows == 2
    assert table.column("city").to_pylist() == ["Bengaluru", "Delhi"]
    assert table.column("avg_delivery_minutes").to_pylist() == [Decimal("14.25")] * 2


# ----------------------------------------------------------------------------- export ---


def stored(client: FakeS3) -> dict[str, bytes]:
    return {key: body for (_, key), body in client.objects.items()}


def test_export_writes_every_layer_and_is_idempotent(lake: list[LakeObject]) -> None:
    client = FakeS3()

    first = export_lake(client, BUCKET, lake)
    after_first = stored(client)
    second = export_lake(client, BUCKET, lake)

    assert after_first == {item.key: item.body for item in lake}
    assert stored(client) == after_first  # same keys, same bytes
    assert first.stale_deleted == second.stale_deleted == 0
    assert first.rows == sum(item.rows for item in lake)
    assert {key.split("/", 1)[0] for key in after_first} == {"raw", "cleaned", "aggregated"}


def test_export_uploads_everything_before_deleting_stale_keys(lake: list[LakeObject]) -> None:
    client = FakeS3()
    put_bytes(client, BUCKET, "raw/orders/date=2000-01-01/batch-09.parquet", b"old", "t")
    put_bytes(client, BUCKET, "notes/keep.txt", b"not ours", "t")
    client.log.clear()

    result = export_lake(client, BUCKET, lake)

    assert result.stale_deleted == 1
    assert "raw/orders/date=2000-01-01/batch-09.parquet" not in stored(client)
    assert stored(client)["notes/keep.txt"] == b"not ours"  # outside the layer prefixes
    last_put = max(i for i, entry in enumerate(client.log) if entry.startswith("put "))
    first_delete = min(i for i, entry in enumerate(client.log) if entry.startswith("delete "))
    assert last_put < first_delete


def test_export_with_a_prefix_touches_only_that_prefix(
    dataset_a: GeneratedDataset, cleaned_dir: Path, lake: list[LakeObject]
) -> None:
    client = FakeS3()
    export_lake(client, BUCKET, lake)
    before = stored(client)
    prefixed = build_lake(dataset_a.dataset_dir, cleaned_dir, AGGREGATES, prefix="test-run/")

    export_lake(client, BUCKET, prefixed, prefix="test-run/")

    assert {k: v for k, v in stored(client).items() if not k.startswith("test-run/")} == before
    assert check_lake(client, BUCKET, prefixed, prefix="test-run/")


def test_export_needs_an_existing_bucket(lake: list[LakeObject]) -> None:
    client = FakeS3()
    client.buckets.clear()

    with pytest.raises(LakeExportError, match="run make s3-up"):
        export_lake(client, BUCKET, lake)
    assert client.objects == {}


def test_failed_stale_delete_is_reported_after_the_upload(lake: list[LakeObject]) -> None:
    client = FakeS3()
    stale = "aggregated/driver_performance/date=2000-01-01/part-0.parquet"
    put_bytes(client, BUCKET, stale, b"old", "t")
    client.failing_deletes = {stale}

    with pytest.raises(LakeS3Error, match="deletes failed"):
        export_lake(client, BUCKET, lake)
    assert all(stored(client)[item.key] == item.body for item in lake)  # new data is in place


# ------------------------------------------------------------------------------ check ---


def test_check_reports_rows_per_layer(lake: list[LakeObject]) -> None:
    client = FakeS3()
    export_lake(client, BUCKET, lake)

    reports = {report.layer: report for report in check_lake(client, BUCKET, lake)}

    for layer in ("raw", "cleaned", "aggregated"):
        layer_items = [item for item in lake if item.key.startswith(f"{layer}/")]
        assert reports[layer].objects == len(layer_items)
        assert reports[layer].rows == sum(item.rows for item in layer_items)


def test_check_finds_missing_extra_and_changed_objects(lake: list[LakeObject]) -> None:
    client = FakeS3()
    export_lake(client, BUCKET, lake)
    removed, changed = lake[0].key, lake[-1].key
    client.objects.pop((BUCKET, removed))
    client.objects[(BUCKET, changed)] = aggregated_objects(
        {"driver_performance": [driver_row(date(2026, 9, 12), "DRV-00002")] * 2}
    )[0].body
    put_bytes(client, BUCKET, "cleaned/orders/date=1999-01-01/part-0.parquet", b"x", "t")

    with pytest.raises(LakeCheckError) as excinfo:
        check_lake(client, BUCKET, lake)

    message = str(excinfo.value)
    assert f"missing object: {removed}" in message
    assert "unexpected object: cleaned/orders/date=1999-01-01/part-0.parquet" in message
    assert f"{changed}: 2 rows, source has {lake[-1].rows}" in message
    assert f"{changed}: content differs from the source" in message


def test_download_reads_through_the_client() -> None:
    client = FakeS3()
    put_bytes(client, BUCKET, "raw/a", b"abc", "t")

    assert download_bytes(client, BUCKET, "raw/a") == b"abc"


# -------------------------------------------------------------------------- bootstrap ---


def test_bootstrap_creates_the_bucket_with_the_admin_key(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    client = FakeS3()
    client.buckets.clear()
    roles: list[str] = []

    def settings(role: str) -> Any:
        roles.append(role)
        return type("S", (), {"bucket": BUCKET, "region": "us-east-1"})()

    monkeypatch.setattr(bootstrap, "load_s3_settings", settings)
    monkeypatch.setattr(bootstrap, "create_client", lambda _settings: client)

    assert bootstrap.main([]) == 0
    assert bootstrap.main([]) == 0
    assert roles == ["admin", "admin"]
    output = capsys.readouterr().out
    assert f"bucket {BUCKET}: created" in output
    assert f"bucket {BUCKET}: already exists" in output


def test_bootstrap_reports_s3_errors_on_one_line(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    client = FakeS3()
    client.head_error = "AccessDenied"
    monkeypatch.setattr(
        bootstrap,
        "load_s3_settings",
        lambda role: type("S", (), {"bucket": BUCKET, "region": "us-east-1"})(),
    )
    monkeypatch.setattr(bootstrap, "create_client", lambda _settings: client)

    assert bootstrap.main([]) == 1
    assert capsys.readouterr().err.startswith("ERROR: S3 request failed: An error occurred")
