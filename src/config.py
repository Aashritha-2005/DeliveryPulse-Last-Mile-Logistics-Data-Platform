"""Environment-based configuration for DeliveryPulse.

Settings come from the process environment and from the ``.env`` file in the project root
(located relative to this file, not the current working directory). Real environment
variables take precedence over ``.env``. Nothing is read at import time: call
:func:`load_database_settings` or :func:`load_s3_settings` when a connection is actually needed.
"""

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from dotenv import dotenv_values
from psycopg.conninfo import make_conninfo

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ENV_FILE = PROJECT_ROOT / ".env"

PLACEHOLDER = "CHANGE_ME"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5432
CONNECT_TIMEOUT_SECONDS = 5
APPLICATION_NAME = "deliverypulse"
TEST_DATABASE_SUFFIX = "_test"
DEFAULT_S3_REGION = "us-east-1"
AWS_ENDPOINT_MARKER = "amazonaws.com"
# S3 bucket naming rules (lowercase letters, digits and hyphens, 3 to 63 characters).
S3_BUCKET_PATTERN = re.compile(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]")

type DatabaseRole = Literal["etl", "readonly"]
type S3Role = Literal["pipeline", "admin"]

# Login role name and the environment variable holding its password, per application role.
_ROLE_CREDENTIALS: dict[DatabaseRole, tuple[str, str]] = {
    "etl": ("deliverypulse_etl", "DELIVERYPULSE_ETL_PASSWORD"),
    "readonly": ("deliverypulse_readonly", "DELIVERYPULSE_READONLY_PASSWORD"),
}

# Access-key variables per object-store role. The pipeline key may only read, write and list
# the lake bucket; the admin key is used only by `make s3-up` to create that bucket.
_S3_CREDENTIALS: dict[S3Role, tuple[str, str]] = {
    "pipeline": ("S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY"),
    "admin": ("S3_ADMIN_ACCESS_KEY", "S3_ADMIN_SECRET_KEY"),
}


class ConfigError(ValueError):
    """A required setting is missing or invalid. Messages name the setting, never its value."""


@dataclass(frozen=True)
class DatabaseSettings:
    """Connection settings for one PostgreSQL login role. The password is excluded from repr."""

    host: str
    port: int
    dbname: str
    user: str
    password: str = field(repr=False)

    def conninfo(self) -> str:
        """Return a libpq connection string. It contains the password: never log it."""
        return make_conninfo(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            connect_timeout=CONNECT_TIMEOUT_SECONDS,
            application_name=APPLICATION_NAME,
        )


@dataclass(frozen=True)
class S3Settings:
    """S3-compatible object-store settings for one role. The keys are excluded from repr."""

    endpoint_url: str | None
    bucket: str
    region: str
    access_key_id: str = field(repr=False)
    secret_access_key: str = field(repr=False)


def read_environment(env_file: Path = DEFAULT_ENV_FILE) -> dict[str, str]:
    """Merge ``env_file`` (if it exists) with ``os.environ``; real environment values win.

    The process environment is not modified.
    """
    merged: dict[str, str] = {}
    if env_file.is_file():
        merged.update(
            {key: value for key, value in dotenv_values(env_file).items() if value is not None}
        )
    merged.update(os.environ)
    return merged


def _require(values: Mapping[str, str], name: str) -> str:
    """Return the non-empty value of ``name`` or raise :class:`ConfigError`."""
    value = values.get(name, "").strip()
    if not value:
        raise ConfigError(f"Required setting {name} is not set; add it to .env or the environment.")
    return value


def _require_secret(values: Mapping[str, str], name: str) -> str:
    """Like :func:`_require`, but also reject the ``CHANGE_ME`` placeholder."""
    value = _require(values, name)
    if PLACEHOLDER in value:
        raise ConfigError(
            f"Setting {name} still contains the {PLACEHOLDER} placeholder; "
            "set a real local value in .env."
        )
    return value


def _parse_port(raw: str) -> int:
    """Parse a TCP port number, raising :class:`ConfigError` if it is not 1-65535."""
    try:
        port = int(raw)
    except ValueError:
        raise ConfigError(f"POSTGRES_PORT must be an integer, got {raw!r}.") from None
    if not 1 <= port <= 65535:
        raise ConfigError(f"POSTGRES_PORT must be between 1 and 65535, got {port}.")
    return port


def _parse_bool(values: Mapping[str, str], name: str) -> bool:
    """Parse an optional true/false setting (default false)."""
    raw = values.get(name, "").strip().lower()
    if raw in {"", "0", "false", "no", "off"}:
        return False
    if raw in {"1", "true", "yes", "on"}:
        return True
    raise ConfigError(f"{name} must be true or false, got {raw!r}.")


def load_database_settings(
    role: DatabaseRole, *, env_file: Path = DEFAULT_ENV_FILE, test_database: bool = False
) -> DatabaseSettings:
    """Build connection settings for an application role from ``.env`` and the environment.

    Args:
        role: ``"etl"`` for the pipeline writer or ``"readonly"`` for dashboards and tests.
        env_file: The ``.env`` file to read; defaults to the project root ``.env``.
        test_database: Connect to ``<POSTGRES_DB>_test`` (integration tests) instead of the
            main database. The init script creates both with the same schema and grants.

    Raises:
        ConfigError: if a required setting is missing, still a placeholder, or invalid.
    """
    values = read_environment(env_file)
    user, password_variable = _ROLE_CREDENTIALS[role]
    dbname = _require(values, "POSTGRES_DB")
    return DatabaseSettings(
        host=values.get("POSTGRES_HOST", "").strip() or DEFAULT_HOST,
        port=_parse_port(values.get("POSTGRES_PORT", "").strip() or str(DEFAULT_PORT)),
        dbname=f"{dbname}{TEST_DATABASE_SUFFIX}" if test_database else dbname,
        user=user,
        password=_require_secret(values, password_variable),
    )


def load_s3_settings(role: S3Role = "pipeline", *, env_file: Path = DEFAULT_ENV_FILE) -> S3Settings:
    """Build object-store settings for ``role`` from ``.env`` and the environment.

    Local development points ``S3_ENDPOINT_URL`` at the local S3-compatible store. An empty
    endpoint (the AWS default) or an Amazon endpoint is refused unless ``S3_ALLOW_AWS=true``,
    which needs explicit approval. Swapping to AWS S3 is otherwise only a change of these
    settings.

    Raises:
        ConfigError: if a setting is missing, still a placeholder, invalid, or points at AWS
            without approval.
    """
    values = read_environment(env_file)
    endpoint_url = values.get("S3_ENDPOINT_URL", "").strip() or None
    pointing_at_aws = endpoint_url is None or AWS_ENDPOINT_MARKER in endpoint_url
    if pointing_at_aws and not _parse_bool(values, "S3_ALLOW_AWS"):
        raise ConfigError(
            "S3_ENDPOINT_URL is empty or points at AWS; do not run against AWS without "
            "approval. Set S3_ALLOW_AWS=true only after approval."
        )
    bucket = _require(values, "S3_BUCKET")
    if not S3_BUCKET_PATTERN.fullmatch(bucket):
        raise ConfigError("S3_BUCKET must be 3-63 lowercase letters, digits or hyphens.")
    key_variable, secret_variable = _S3_CREDENTIALS[role]
    return S3Settings(
        endpoint_url=endpoint_url,
        bucket=bucket,
        region=values.get("S3_REGION", "").strip() or DEFAULT_S3_REGION,
        access_key_id=_require_secret(values, key_variable),
        secret_access_key=_require_secret(values, secret_variable),
    )
