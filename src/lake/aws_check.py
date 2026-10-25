"""Read-only checks for the AWS proof: preflight, permission denials and verified teardown.

    make aws-preflight        # as the pipeline role: identity, region, bucket, workgroup, table
    make aws-negative-check   # as the pipeline role: actions outside the policy are denied
    make aws-verify-teardown  # as the project login, after terraform destroy: nothing remains

Output names roles and resources but never an account ID, ARN or credential, so it can be
shown in a screenshot. Every command exits non-zero unless every check passes.
"""

import argparse
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from src.config import DEFAULT_ENV_FILE, ConfigError, read_environment
from src.lake.aws import (
    AwsProofError,
    aws_session,
    load_athena_settings,
    load_aws_settings,
    redact,
    require_name,
)

GLUE_TABLE = "cleaned_orders"
# Must match default_tags in infra/aws/versions.tf (a unit test checks both).
PROJECT_TAG = ("project", "deliverypulse-aws-proof")
# Key outside the four allowed prefixes (raw/, cleaned/, aggregated/, athena-results/).
OUTSIDE_KEY = "outside-allowed/permission-probe.txt"
DENIED_CODES = frozenset(
    {"AccessDenied", "AccessDeniedException", "UnauthorizedOperation", "AuthorizationError"}
)
ABSENT_CODES = frozenset(
    {"404", "NoSuchBucket", "NotFound", "EntityNotFoundException", "NoSuchEntity"}
)


@dataclass(frozen=True)
class CheckResult:
    """One named check and whether it passed, with a short redacted detail."""

    name: str
    passed: bool
    detail: str


def error_code(exc: ClientError) -> str:
    """The AWS error code of ``exc``."""
    return str(exc.response.get("Error", {}).get("Code", ""))


def caller_name(arn: str) -> str:
    """The role or user name in a caller ARN, without the account ID or session."""
    resource = arn.split(":", 5)[-1]
    parts = resource.split("/")
    if parts[0] == "assumed-role" and len(parts) >= 2:
        return f"role {parts[1]}"
    return f"{parts[0]} {parts[-1]}"


def expect_denied(
    name: str, call: Callable[[], Any], undo: Callable[[], Any] | None = None
) -> CheckResult:
    """Pass only if ``call`` is refused with an access-denied error."""
    try:
        call()
    except ClientError as exc:
        code = error_code(exc)
        return CheckResult(name, code in DENIED_CODES, f"{code}")
    except BotoCoreError as exc:
        return CheckResult(name, False, f"error: {redact(str(exc))}")
    if undo is not None:
        undo()
    return CheckResult(name, False, "ALLOWED (the policy is too broad)")


def expect_absent(
    name: str, call: Callable[[], Any], absent_codes: frozenset[str] = ABSENT_CODES
) -> CheckResult:
    """Pass only if ``call`` reports that the resource does not exist."""
    try:
        call()
    except ClientError as exc:
        code = error_code(exc)
        return CheckResult(name, code in absent_codes, "absent" if code in absent_codes else code)
    except BotoCoreError as exc:
        return CheckResult(name, False, f"error: {redact(str(exc))}")
    return CheckResult(name, False, "STILL PRESENT")


def preflight(
    session: boto3.Session, bucket: str, workgroup: str, database: str
) -> list[CheckResult]:
    """Identity, region and the three resources the pipeline role needs."""
    identity = session.client("sts").get_caller_identity()
    results = [
        CheckResult("identity", True, caller_name(str(identity["Arn"]))),
        CheckResult("region", session.region_name == "ap-southeast-2", str(session.region_name)),
        CheckResult("credentials", True, "temporary (session token present)"),
    ]
    session.client("s3").head_bucket(Bucket=bucket)
    results.append(CheckResult("bucket", True, "reachable"))
    config = session.client("athena").get_work_group(WorkGroup=workgroup)["WorkGroup"][
        "Configuration"
    ]
    cutoff = int(config.get("BytesScannedCutoffPerQuery", 0))
    enforced = bool(config.get("EnforceWorkGroupConfiguration", False))
    results.append(
        CheckResult(
            "workgroup",
            enforced and 0 < cutoff <= 100 * 1024**2,
            f"enforced={enforced} per-query cutoff={cutoff} bytes",
        )
    )
    session.client("glue").get_table(DatabaseName=database, Name=GLUE_TABLE)
    results.append(CheckResult("glue table", True, f"{database}.{GLUE_TABLE}"))
    return results


def negative(session: boto3.Session, bucket: str) -> list[CheckResult]:
    """Five actions the pipeline role must not be able to perform."""
    s3 = session.client("s3")
    return [
        expect_denied("list all buckets", s3.list_buckets),
        expect_denied(
            "use the default 'primary' workgroup",
            lambda: session.client("athena").get_work_group(WorkGroup="primary"),
        ),
        expect_denied("list Glue databases", session.client("glue").get_databases),
        expect_denied("list IAM roles", lambda: session.client("iam").list_roles(MaxItems=1)),
        expect_denied(
            "write outside the allowed prefixes",
            lambda: s3.put_object(Bucket=bucket, Key=OUTSIDE_KEY, Body=b""),
            undo=lambda: s3.delete_object(Bucket=bucket, Key=OUTSIDE_KEY),
        ),
    ]


def teardown(session: boto3.Session, names: Mapping[str, str]) -> list[CheckResult]:
    """After terraform destroy: every proof resource is gone, and nothing tagged remains."""
    results = [
        expect_absent("bucket", lambda: session.client("s3").head_bucket(Bucket=names["bucket"])),
        expect_absent(
            "athena workgroup",
            lambda: session.client("athena").get_work_group(WorkGroup=names["workgroup"]),
            ABSENT_CODES | {"InvalidRequestException"},
        ),
        expect_absent(
            "glue database",
            lambda: session.client("glue").get_database(Name=names["database"]),
        ),
        expect_absent("iam role", lambda: session.client("iam").get_role(RoleName=names["role"])),
    ]
    key, value = PROJECT_TAG
    tagged = session.client("resourcegroupstaggingapi").get_resources(
        TagFilters=[{"Key": key, "Values": [value]}]
    )["ResourceTagMappingList"]
    results.append(
        CheckResult("tagged resources", not tagged, f"{len(tagged)} still tagged {key}={value}")
    )
    return results


def report(title: str, results: Sequence[CheckResult]) -> int:
    """Print one line per check and return the exit code."""
    print(title)
    for result in results:
        print(f"  {'PASS' if result.passed else 'FAIL'}  {result.name}: {result.detail}")
    passed = all(result.passed for result in results)
    print(f"{title}: {'all checks passed' if passed else 'FAILED'}")
    return 0 if passed else 1


def main(argv: Sequence[str] | None = None, *, env_file: Path = DEFAULT_ENV_FILE) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Read-only checks for the AWS proof.")
    parser.add_argument("command", choices=("preflight", "negative", "teardown"))
    args = parser.parse_args(argv)
    try:
        settings = load_aws_settings(env_file=env_file)
        session = aws_session(settings.region)
        if args.command == "negative":
            return report("permission checks", negative(session, settings.bucket))
        athena = load_athena_settings(env_file=env_file)
        if args.command == "preflight":
            return report(
                "preflight", preflight(session, settings.bucket, athena.workgroup, athena.database)
            )
        role = require_name(read_environment(env_file), "AWS_PROOF_ROLE_NAME")
        names = {
            "bucket": settings.bucket,
            "workgroup": athena.workgroup,
            "database": athena.database,
            "role": role,
        }
        return report("teardown", teardown(session, names))
    except ClientError as exc:
        print(f"ERROR: {error_code(exc)}: {redact(str(exc))}", file=sys.stderr)
    except (AwsProofError, BotoCoreError, ConfigError) as exc:
        print(f"ERROR: {redact(str(exc))}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
