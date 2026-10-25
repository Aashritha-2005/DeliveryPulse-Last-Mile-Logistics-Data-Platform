"""Static checks of the AWS proof's Terraform (infra/aws): no AWS access, no Terraform run.

The pipeline policy must allow exactly one bucket (four prefixes), one Athena workgroup and one
Glue database and table, with no wildcard resource or action; these tests fail if it grows.
"""

import json
import re
from typing import Any

import pyarrow as pa
import pytest

from src.config import PROJECT_ROOT
from src.lake.aws import PROOF_REGION
from src.lake.aws_check import GLUE_TABLE, PROJECT_TAG
from src.transformations.outputs import ORDER_SCHEMA

pytestmark = pytest.mark.unit

INFRA = PROJECT_ROOT / "infra" / "aws"
TEMPLATE = INFRA / "policies" / "pipeline.json.tftpl"
PLACEHOLDERS = {
    "bucket_arn": "arn:aws:s3:::deliverypulse-proof-0a1b2c3d",
    "workgroup_arn": "arn:aws:athena:ap-southeast-2:000000000000:workgroup/deliverypulse-proof",
    "glue_catalog_arn": "arn:aws:glue:ap-southeast-2:000000000000:catalog",
    "glue_database_arn": "arn:aws:glue:ap-southeast-2:000000000000:database/deliverypulse_proof",
    "glue_table_arn": (
        "arn:aws:glue:ap-southeast-2:000000000000:table/deliverypulse_proof/cleaned_orders"
    ),
}
BUCKET = PLACEHOLDERS["bucket_arn"]
ALLOWED_RESOURCES = {
    BUCKET,
    f"{BUCKET}/raw/*",
    f"{BUCKET}/cleaned/*",
    f"{BUCKET}/aggregated/*",
    f"{BUCKET}/athena-results/*",
    PLACEHOLDERS["workgroup_arn"],
    PLACEHOLDERS["glue_catalog_arn"],
    PLACEHOLDERS["glue_database_arn"],
    PLACEHOLDERS["glue_table_arn"],
}
ALLOWED_ACTIONS = {
    "s3:GetBucketLocation",
    "s3:ListBucket",
    "s3:AbortMultipartUpload",
    "s3:DeleteObject",
    "s3:GetObject",
    "s3:ListMultipartUploadParts",
    "s3:PutObject",
    "athena:GetQueryExecution",
    "athena:GetQueryResults",
    "athena:GetWorkGroup",
    "athena:StartQueryExecution",
    "athena:StopQueryExecution",
    "glue:GetDatabase",
    "glue:GetPartition",
    "glue:GetPartitions",
    "glue:GetTable",
    "glue:GetTables",
}


def render_policy(values: dict[str, str] = PLACEHOLDERS) -> dict[str, Any]:
    text = TEMPLATE.read_text(encoding="utf-8")
    for name, value in values.items():
        text = text.replace("${" + name + "}", value)
    policy: dict[str, Any] = json.loads(text)
    return policy


def statements(policy: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    rendered = render_policy() if policy is None else policy
    return list(rendered["Statement"])


def as_list(value: str | list[str]) -> list[str]:
    return [value] if isinstance(value, str) else list(value)


def tf(name: str) -> str:
    return (INFRA / name).read_text(encoding="utf-8")


def test_template_uses_exactly_the_five_expected_placeholders() -> None:
    found = set(re.findall(r"\$\{(\w+)\}", TEMPLATE.read_text(encoding="utf-8")))

    assert found == set(PLACEHOLDERS)


def test_policy_allows_only_the_proof_resources() -> None:
    resources = [r for s in statements() for r in as_list(s["Resource"])]

    assert set(resources) <= ALLOWED_RESOURCES
    assert "*" not in resources
    s3_resources = [r for r in resources if r.startswith("arn:aws:s3:::")]
    assert all(r == BUCKET or r.startswith(f"{BUCKET}/") for r in s3_resources)


def test_policy_allows_only_the_listed_actions_without_wildcards() -> None:
    actions = {a for s in statements() for a in as_list(s["Action"])}

    assert actions == ALLOWED_ACTIONS
    assert not any("*" in action for action in actions)


def test_policy_has_only_allow_statements_without_not_elements() -> None:
    for statement in statements():
        assert statement["Effect"] == "Allow"
        assert not {"NotAction", "NotResource", "NotPrincipal"} & set(statement)


def test_policy_fails_the_check_if_a_second_bucket_is_added() -> None:
    policy = render_policy()
    policy["Statement"][1]["Resource"].append("arn:aws:s3:::someone-elses-bucket/*")

    resources = {r for s in statements(policy) for r in as_list(s["Resource"])}
    assert not resources <= ALLOWED_RESOURCES


def test_main_creates_one_bucket_one_workgroup_and_one_role() -> None:
    main = tf("main.tf")

    assert len(re.findall(r'resource "aws_s3_bucket" "', main)) == 1
    assert len(re.findall(r'resource "aws_athena_workgroup" "', main)) == 1
    assert len(re.findall(r'resource "aws_iam_role" "', main)) == 1
    assert len(re.findall(r'resource "aws_glue_catalog_table" "', main)) == 1
    for forbidden in (
        "aws_iam_access_key",
        "aws_iam_user",
        "aws_kms_key",
        "aws_s3_bucket_versioning",
        "aws_glue_crawler",
        "aws_s3_bucket_acl",
    ):
        assert forbidden not in main


def test_bucket_is_private_encrypted_tls_only_and_destroyable() -> None:
    main = tf("main.tf")

    for setting in (
        "block_public_acls",
        "block_public_policy",
        "ignore_public_acls",
        "restrict_public_buckets",
    ):
        assert re.search(rf"{setting}\s*=\s*true", main)
    assert '"BucketOwnerEnforced"' in main
    assert '"AES256"' in main
    assert '"aws:SecureTransport" = "false"' in main
    assert len(re.findall(r"force_destroy\s*=\s*true", main)) == 2  # bucket and workgroup


def test_lifecycle_and_scan_limit_defaults() -> None:
    variables = tf("variables.tf")

    assert re.search(r'variable "object_retention_days"[^}]*default\s*=\s*7\b', variables)
    assert re.search(r'variable "result_retention_days"[^}]*default\s*=\s*1\b', variables)
    assert re.search(r"default\s*=\s*104857600\b", variables)
    assert "abort_incomplete_multipart_upload" in tf("main.tf")
    assert "enforce_workgroup_configuration    = true" in tf("main.tf")


def test_region_is_locked_to_sydney() -> None:
    assert PROOF_REGION == "ap-southeast-2"
    assert 'condition     = var.region == "ap-southeast-2"' in tf("variables.tf")


def test_role_is_assumable_only_by_the_owner_session_role() -> None:
    main = tf("main.tf")

    assert "Principal = { AWS = data.aws_iam_session_context.current.issuer_arn }" in main
    assert "max_session_duration = 3600" in main


def test_teardown_tag_matches_the_provider_default_tags() -> None:
    key, value = PROJECT_TAG

    assert re.search(rf'{key}\s*=\s*"{value}"', tf("versions.tf"))


def test_versions_are_pinned_and_ci_uses_the_same_terraform() -> None:
    versions = tf("versions.tf")
    workflow = (PROJECT_ROOT / ".github" / "workflows" / "tests.yml").read_text(encoding="utf-8")

    assert 'required_version = "~> 1.16.5"' in versions
    assert re.search(r'version\s*=\s*"= 6\.68\.0"', versions)
    assert re.search(r'version\s*=\s*"= 3\.9\.1"', versions)
    assert 'terraform_version: "1.16.5"' in workflow


ARROW_TO_HIVE = {
    pa.string(): "string",
    pa.float64(): "double",
    pa.bool_(): "boolean",
}


def hive_type(field: pa.Field) -> str:
    if pa.types.is_timestamp(field.type):
        return "timestamp"
    if pa.types.is_decimal(field.type):
        return f"decimal({field.type.precision},{field.type.scale})"
    return ARROW_TO_HIVE[field.type]


def test_glue_columns_match_the_cleaned_orders_parquet_schema() -> None:
    columns = json.loads((INFRA / "tables" / "cleaned_orders.json").read_text(encoding="utf-8"))

    assert columns == [{"name": f.name, "type": hive_type(f)} for f in ORDER_SCHEMA]
    assert f'glue_table            = "{GLUE_TABLE}"' in tf("main.tf")


def test_outputs_expose_names_but_nothing_secret() -> None:
    outputs = tf("outputs.tf")

    assert "sensitive" not in outputs
    assert not re.search(r"\b\d{12}\b", outputs)
    for path in INFRA.rglob("*"):
        if path.is_file():
            assert not re.search(r"\b\d{12}\b", path.read_text(encoding="utf-8")), path


def test_athena_query_file_is_read_only_sql() -> None:
    sql = (PROJECT_ROOT / "sql" / "athena" / "late_rate_by_city.sql").read_text(encoding="utf-8")
    body = "\n".join(line for line in sql.splitlines() if not line.startswith("--"))

    assert body.lstrip().upper().startswith("SELECT")
    assert not re.search(r"\b(INSERT|DELETE|DROP|CREATE|ALTER|UNLOAD)\b", body.upper())
