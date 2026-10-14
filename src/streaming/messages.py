"""Wire envelopes and stateless validation shared by the producer and consumer."""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from src.contract import DELIVERY_EVENT_FIELDS, ORDER_COLUMNS
from src.transformations.rules import (
    validate_event_basics,
    validate_event_coordinates,
    validate_order,
)


class InvalidMessageError(ValueError):
    """A malformed envelope or a failed stateless business rule."""


@dataclass(frozen=True)
class Record:
    """Validated identity and source provenance, with the unchanged wire bytes."""

    kind: str
    key: str
    version: str
    digest: str
    payload: bytes
    dataset: str
    file: str
    row: int


def encode(kind: str, fields: dict[str, Any], dataset: str, file: str, row: int) -> bytes:
    """Encode source fields and provenance; transport time is deliberately excluded."""
    return json.dumps(
        {"kind": kind, "fields": fields, "dataset": dataset, "file": file, "row": row},
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()


def decode(payload: bytes) -> Record:
    """Validate envelope shape and reuse the batch path's stateless contract rules."""
    try:
        value = json.loads(payload)
    except (ValueError, UnicodeDecodeError, RecursionError) as exc:
        raise InvalidMessageError("malformed_json") from exc
    if not isinstance(value, dict) or value.get("kind") not in ("order", "driver_ping"):
        raise InvalidMessageError("invalid_envelope")
    if any(not isinstance(value.get(k), str) or not value[k] for k in ("dataset", "file")):
        raise InvalidMessageError("invalid_provenance")
    if type(value.get("row")) is not int or value["row"] < 1:
        raise InvalidMessageError("invalid_provenance")
    raw = value.get("fields")
    if not isinstance(raw, dict) or any(
        not isinstance(k, str) or isinstance(v, (dict, list, bool)) for k, v in raw.items()
    ):
        raise InvalidMessageError("invalid_fields")
    kind = value["kind"]
    columns = ORDER_COLUMNS if kind == "order" else DELIVERY_EVENT_FIELDS
    if not set(columns).issubset(raw):
        raise InvalidMessageError("missing_required_column")
    fields = {k: "" if raw[k] is None else str(raw[k]) for k in columns}
    if kind == "order":
        reason, order = validate_order(fields)
        if reason or order is None:
            raise InvalidMessageError(reason or "invalid_order")
        key = order.order_id
        version = datetime.fromtimestamp(order.source_updated_at, UTC).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    else:
        reason, stamp = validate_event_basics(fields)
        if reason or stamp is None:
            raise InvalidMessageError(reason or "invalid_event")
        if fields["event_type"] != "gps_ping":
            raise InvalidMessageError("not_driver_ping")
        reason, _ = validate_event_coordinates(fields, stamp)
        if reason:
            raise InvalidMessageError(reason)
        key, version = fields["event_id"], ""
    try:
        canonical = json.dumps(raw, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except ValueError as exc:
        raise InvalidMessageError("invalid_number") from exc
    return Record(
        kind,
        key,
        version,
        hashlib.sha256(canonical).hexdigest(),
        payload,
        value["dataset"],
        value["file"],
        value["row"],
    )
