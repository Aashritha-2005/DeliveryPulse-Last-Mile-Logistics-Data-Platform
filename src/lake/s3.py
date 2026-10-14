"""Small S3 API wrapper used by the lake (works against SeaweedFS locally and AWS S3)."""

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, cast

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from src.config import DEFAULT_S3_REGION, S3Settings

# Errors of any S3 call: a service answer (ClientError) or a client-side failure such as an
# unreachable endpoint (BotoCoreError). Their messages name the operation, never a key.
S3_ERRORS = (BotoCoreError, ClientError)
MISSING_BUCKET_CODES = frozenset({"404", "NoSuchBucket", "NotFound"})
DELETE_BATCH_SIZE = 1000  # S3 DeleteObjects accepts at most 1,000 keys per request


class S3Client(Protocol):
    """Subset of the boto3 S3 client used by DeliveryPulse."""

    def create_bucket(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def delete_objects(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def get_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def head_bucket(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def list_objects_v2(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def put_object(self, **kwargs: Any) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class S3Object:
    """One object in the lake bucket."""

    key: str
    size: int


class LakeS3Error(RuntimeError):
    """An S3 operation of the lake failed in a way the caller should report and stop on."""


def create_client(settings: S3Settings) -> S3Client:
    """A boto3 S3 client for ``settings`` (path-style addressing, SigV4)."""
    return cast(
        S3Client,
        boto3.client(
            "s3",
            endpoint_url=settings.endpoint_url,
            aws_access_key_id=settings.access_key_id,
            aws_secret_access_key=settings.secret_access_key,
            region_name=settings.region,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        ),
    )


def error_code(exc: ClientError) -> str:
    """The S3 error code of ``exc`` (for example ``AccessDenied``)."""
    return str(exc.response.get("Error", {}).get("Code", ""))


def bucket_exists(client: S3Client, bucket: str) -> bool:
    """True if ``bucket`` exists and is reachable with the client's key."""
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        if error_code(exc) in MISSING_BUCKET_CODES:
            return False
        raise
    return True


def create_bucket(client: S3Client, bucket: str, region: str) -> bool:
    """Create ``bucket`` unless it exists; return True if it was created.

    Outside ``us-east-1``, S3 requires the region as ``LocationConstraint``.
    """
    if bucket_exists(client, bucket):
        return False
    if region == DEFAULT_S3_REGION:
        client.create_bucket(Bucket=bucket)
    else:
        client.create_bucket(
            Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region}
        )
    return True


def list_objects(client: S3Client, bucket: str, prefix: str = "") -> list[S3Object]:
    """Every object under ``prefix``, in key order."""
    continuation: str | None = None
    objects: list[S3Object] = []
    while True:
        kwargs: dict[str, str] = {"Bucket": bucket, "Prefix": prefix}
        if continuation is not None:
            kwargs["ContinuationToken"] = continuation
        response = client.list_objects_v2(**kwargs)
        objects.extend(
            S3Object(key=str(item["Key"]), size=int(item["Size"]))
            for item in response.get("Contents", [])
        )
        if not response.get("IsTruncated"):
            return sorted(objects, key=lambda item: item.key)
        continuation = str(response["NextContinuationToken"])


def batched(items: Iterable[str], size: int = DELETE_BATCH_SIZE) -> Iterator[list[str]]:
    """Yield lists of at most ``size`` items."""
    batch: list[str] = []
    for item in items:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def delete_keys(client: S3Client, bucket: str, keys: Iterable[str]) -> int:
    """Delete ``keys``; return how many were deleted.

    ``DeleteObjects`` reports per-key failures in its response instead of raising, so the
    response is checked and any failure raises :class:`LakeS3Error`.
    """
    deleted = 0
    for group in batched(sorted(keys)):
        response = client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in group], "Quiet": True},
        )
        errors = list(response.get("Errors", []))
        if errors:
            first = errors[0]
            raise LakeS3Error(
                f"{len(errors)} of {len(group)} deletes failed; first: "
                f"{first.get('Key')} ({first.get('Code')}: {first.get('Message')})"
            )
        deleted += len(group)
    return deleted


def put_bytes(client: S3Client, bucket: str, key: str, body: bytes, content_type: str) -> S3Object:
    """Upload one object and return its key and size."""
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
    return S3Object(key=key, size=len(body))


def download_bytes(client: S3Client, bucket: str, key: str) -> bytes:
    """Read one object body through the S3 API."""
    body = client.get_object(Bucket=bucket, Key=key)["Body"]
    return bytes(body.read())
