"""AWS session for the one-off cloud proof: Sydney only, short-lived credentials only.

Credentials come from the standard AWS chain in the shell (an ``aws login`` profile selected
with ``AWS_PROFILE``). They are never read from ``.env`` and never logged. Long-lived access
keys are refused: temporary credentials always carry a session token, long-lived keys never do.
"""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import boto3

from src.config import DEFAULT_ENV_FILE, ConfigError, S3Settings, load_s3_settings, read_environment

PROOF_REGION = "ap-southeast-2"
# Account-identifying text that must never reach the terminal or evidence files.
_ARN = re.compile(r"arn:aws[\w-]*:[^\s\"',]*")
_ACCOUNT_ID = re.compile(r"\b\d{12}\b")
# A Glue/Athena identifier: lowercase letters, digits and underscores (hyphens for workgroups).
_NAME_CHARACTERS = frozenset("abcdefghijklmnopqrstuvwxyz0123456789_-")


class AwsProofError(RuntimeError):
    """The AWS proof cannot run safely (wrong region, missing or long-lived credentials)."""


@dataclass(frozen=True)
class AthenaSettings:
    """Names of the proof's Athena workgroup and Glue database (no credentials)."""

    workgroup: str
    database: str


def redact(text: str) -> str:
    """``text`` with ARNs and 12-digit account IDs replaced, for terminal output and evidence."""
    return _ACCOUNT_ID.sub("<account>", _ARN.sub("<arn>", text))


def contains_account_identifiers(text: str) -> bool:
    """True if ``text`` still contains an ARN or a 12-digit account ID."""
    return bool(_ARN.search(text) or _ACCOUNT_ID.search(text))


def require_aws_settings(settings: S3Settings) -> S3Settings:
    """Return ``settings`` if they target AWS in the proof region; refuse anything else."""
    if not settings.aws_mode:
        raise AwsProofError(
            "S3_ENDPOINT_URL points at a local store; the AWS proof needs it empty."
        )
    if settings.region != PROOF_REGION:
        raise AwsProofError(
            f"S3_REGION must be {PROOF_REGION} for the AWS proof, got {settings.region!r}."
        )
    return settings


def load_aws_settings(*, env_file: Path = DEFAULT_ENV_FILE) -> S3Settings:
    """S3 settings for AWS: the ``S3_ALLOW_AWS`` guard applies, then the proof-region check."""
    return require_aws_settings(load_s3_settings("pipeline", env_file=env_file))


def require_name(values: Mapping[str, str], name: str) -> str:
    """The value of resource-name setting ``name``: set, lowercase and identifier-safe."""
    value = values.get(name, "").strip()
    if not value:
        raise ConfigError(f"Required setting {name} is not set; export it from terraform output.")
    if not set(value) <= _NAME_CHARACTERS or len(value) > 128:
        raise ConfigError(f"{name} must be lowercase letters, digits, '_' or '-'.")
    return value


def load_athena_settings(*, env_file: Path = DEFAULT_ENV_FILE) -> AthenaSettings:
    """The workgroup and database names from ``ATHENA_WORKGROUP`` and ``ATHENA_DATABASE``."""
    values = read_environment(env_file)
    return AthenaSettings(
        workgroup=require_name(values, "ATHENA_WORKGROUP"),
        database=require_name(values, "ATHENA_DATABASE"),
    )


def aws_session(region: str = PROOF_REGION) -> boto3.Session:
    """A boto3 session in ``region`` backed by temporary credentials from the shell.

    Raises:
        AwsProofError: if the region is not the proof region, no credentials are available, or
            the credentials are long-lived (no session token).
    """
    if region != PROOF_REGION:
        raise AwsProofError(f"The AWS proof runs only in {PROOF_REGION}, got {region!r}.")
    session = boto3.Session(region_name=region)
    credentials = session.get_credentials()
    if credentials is None:
        raise AwsProofError("No AWS credentials found; run `aws login` and export AWS_PROFILE.")
    if not credentials.get_frozen_credentials().token:
        raise AwsProofError(
            "Long-lived AWS access keys are refused; use short-lived `aws login` credentials."
        )
    return session
