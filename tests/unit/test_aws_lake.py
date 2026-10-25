"""Unit tests of the AWS proof's Python side. Every AWS client is a fake: no network, no AWS."""

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, cast

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError

from src.config import ConfigError, S3Settings, load_s3_settings
from src.database.postgres_state import Connection
from src.lake import athena, aws, aws_check, bootstrap, s3
from src.lake.athena import AthenaQueryError, CityRate, QueryStats
from src.lake.aws import AthenaSettings, AwsProofError
from src.lake.aws_check import CheckResult
from src.lake.export import LakeExportError, check_dataset_identity, cleaned_order_ids

pytestmark = pytest.mark.unit

AWS_VARIABLES = (
    "S3_ENDPOINT_URL",
    "S3_BUCKET",
    "S3_REGION",
    "S3_ALLOW_AWS",
    "S3_ACCESS_KEY_ID",
    "S3_SECRET_ACCESS_KEY",
    "S3_ADMIN_ACCESS_KEY",
    "S3_ADMIN_SECRET_KEY",
    "ATHENA_WORKGROUP",
    "ATHENA_DATABASE",
    "AWS_PROOF_ROLE_NAME",
)
LOCAL_KEY = "k" * 20
LOCAL_SECRET = "s" * 40
SETTINGS = AthenaSettings(workgroup="deliverypulse-proof", database="deliverypulse_proof")


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in AWS_VARIABLES:
        monkeypatch.delenv(name, raising=False)


def env_file(tmp_path: Path, *extra: str) -> Path:
    path = tmp_path / ".env"
    lines = [
        "S3_BUCKET=deliverypulse-proof-0a1b2c3d",
        f"S3_ACCESS_KEY_ID={LOCAL_KEY}",
        f"S3_SECRET_ACCESS_KEY={LOCAL_SECRET}",
        f"S3_ADMIN_ACCESS_KEY={LOCAL_KEY}x",
        f"S3_ADMIN_SECRET_KEY={LOCAL_SECRET}x",
        *extra,
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def aws_env(tmp_path: Path, *extra: str) -> Path:
    return env_file(tmp_path, "S3_ENDPOINT_URL=", "S3_ALLOW_AWS=true", *extra)


def client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "Operation")


# ----------------------------------------------------------------- settings and session ---


def test_aws_mode_never_carries_the_local_keys(tmp_path: Path) -> None:
    settings = load_s3_settings("pipeline", env_file=aws_env(tmp_path))

    assert settings.aws_mode
    assert settings.access_key_id is None and settings.secret_access_key is None


def test_aws_mode_still_needs_explicit_approval(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="without approval"):
        load_s3_settings(env_file=env_file(tmp_path, "S3_ENDPOINT_URL="))


def test_local_mode_keeps_the_local_keys(tmp_path: Path) -> None:
    settings = load_s3_settings(env_file=env_file(tmp_path, "S3_ENDPOINT_URL=http://127.0.0.1:1"))

    assert not settings.aws_mode
    assert settings.access_key_id == LOCAL_KEY


def test_proof_settings_require_sydney(tmp_path: Path) -> None:
    with pytest.raises(AwsProofError, match="ap-southeast-2"):
        aws.load_aws_settings(env_file=aws_env(tmp_path, "S3_REGION=us-east-1"))

    settings = aws.load_aws_settings(env_file=aws_env(tmp_path, "S3_REGION=ap-southeast-2"))
    assert settings.region == "ap-southeast-2"


def test_proof_settings_refuse_a_local_endpoint() -> None:
    local = S3Settings("http://127.0.0.1:8333", "b-u-c", "ap-southeast-2", "k", "s")

    with pytest.raises(AwsProofError, match="local store"):
        aws.require_aws_settings(local)


@pytest.mark.parametrize(
    ("line", "message"),
    [("", "ATHENA_WORKGROUP is not set"), ("ATHENA_WORKGROUP=Bad Name", "lowercase")],
)
def test_athena_names_are_validated(tmp_path: Path, line: str, message: str) -> None:
    path = aws_env(tmp_path, line, "ATHENA_DATABASE=deliverypulse_proof")

    with pytest.raises(ConfigError, match=message):
        aws.load_athena_settings(env_file=path)


class FrozenCredentials:
    def __init__(self, token: str | None) -> None:
        self.token = token


class Credentials:
    def __init__(self, token: str | None) -> None:
        self.token = token

    def get_frozen_credentials(self) -> FrozenCredentials:
        return FrozenCredentials(self.token)


def fake_boto_session(credentials: Credentials | None) -> Callable[..., Any]:
    class Session:
        def __init__(self, region_name: str) -> None:
            self.region_name = region_name

        def get_credentials(self) -> Credentials | None:
            return credentials

    return Session


def test_session_refuses_long_lived_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(boto3, "Session", fake_boto_session(Credentials(token=None)))

    with pytest.raises(AwsProofError, match="Long-lived"):
        aws.aws_session()


def test_session_refuses_missing_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(boto3, "Session", fake_boto_session(None))

    with pytest.raises(AwsProofError, match="aws login"):
        aws.aws_session()


def test_session_accepts_temporary_credentials_in_sydney_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Any non-empty session token marks temporary credentials.
    monkeypatch.setattr(boto3, "Session", fake_boto_session(Credentials("temporary")))

    assert aws.aws_session().region_name == "ap-southeast-2"
    with pytest.raises(AwsProofError, match="only in"):
        aws.aws_session("us-east-1")


def test_aws_client_uses_the_session_and_virtual_addressing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    sentinel = object()

    class Session:
        def client(self, service: str, **kwargs: Any) -> object:
            calls.append({"service": service, **kwargs})
            return sentinel

    monkeypatch.setattr(s3, "aws_session", lambda region: Session())
    settings = S3Settings(None, "deliverypulse-proof-0a1b2c3d", "ap-southeast-2", None, None)

    created: object = s3.create_client(settings)
    assert created is sentinel
    assert calls[0]["service"] == "s3"
    assert calls[0]["config"].s3 == {"addressing_style": "virtual"}
    assert "aws_access_key_id" not in calls[0]


def test_redact_removes_arns_and_account_ids() -> None:
    message = (
        "User: arn:aws:sts::123456789012:assumed-role/AccountFullAccessRole/abc is not "
        "authorized on account 123456789012"
    )

    redacted = aws.redact(message)

    assert "123456789012" not in redacted and "arn:aws" not in redacted
    assert not aws.contains_account_identifiers(redacted)
    assert aws.contains_account_identifiers(message)


def test_bootstrap_refuses_to_create_buckets_on_aws(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    aws_settings = S3Settings(None, "deliverypulse-proof-0a1b2c3d", "ap-southeast-2", None, None)
    monkeypatch.setattr(bootstrap, "load_s3_settings", lambda role: aws_settings)

    assert bootstrap.main([]) == 1
    assert "Terraform" in capsys.readouterr().err


# ---------------------------------------------------------------- dataset-identity guard ---


class FakeConnection:
    def __init__(self, order_ids: list[str]) -> None:
        self.order_ids = order_ids

    def execute(self, query: str) -> "FakeConnection":
        assert query == "SELECT order_id FROM analytics.orders"
        return self

    def __iter__(self) -> Any:
        return iter([(order_id,) for order_id in self.order_ids])


def connection(order_ids: list[str]) -> Connection:
    return cast(Connection, FakeConnection(order_ids))


def cleaned_dir(tmp_path: Path, partitions: Mapping[str, list[str]]) -> Path:
    root = tmp_path / "seed-42_orders-5000_cfg-test" / "cleaned"
    for day, ids in partitions.items():
        folder = root / "orders" / f"date={day}"
        folder.mkdir(parents=True)
        pq.write_table(pa.table({"order_id": ids}), folder / "part-0.parquet")
    return root


def test_identity_guard_accepts_the_matching_database(tmp_path: Path) -> None:
    root = cleaned_dir(tmp_path, {"2026-09-10": ["o1", "o2"], "2026-09-11": ["o3"]})

    assert cleaned_order_ids(root) == {"o1", "o2", "o3"}
    check_dataset_identity(connection(["o3", "o1", "o2"]), root)


def test_identity_guard_refuses_another_dataset(tmp_path: Path) -> None:
    root = cleaned_dir(tmp_path, {"2026-09-10": ["o1", "o2"]})

    with pytest.raises(LakeExportError, match="different dataset.*2 orders in the database"):
        check_dataset_identity(connection(["o1", "x9"]), root)


def test_identity_guard_needs_cleaned_orders(tmp_path: Path) -> None:
    with pytest.raises(LakeExportError, match="No cleaned orders"):
        cleaned_order_ids(tmp_path)


# ------------------------------------------------------------------------- athena runner ---


@pytest.mark.parametrize(
    ("state", "scanned", "expected"),
    [
        ("FAILED", 5 * 1024**3, 0.0),
        ("SUCCEEDED", 1_000, 10 * 1024**2 / 1024**4 * 5.0),
        ("CANCELLED", 10 * 1024**2, 10 * 1024**2 / 1024**4 * 5.0),
        ("SUCCEEDED", 1024**4, 5.0),
    ],
)
def test_cost_estimate_follows_athena_pricing(state: str, scanned: int, expected: float) -> None:
    assert athena.estimated_cost_usd(state, scanned) == pytest.approx(expected)


class FakeAthena:
    """Athena client fake: a scripted sequence of states and paged results."""

    def __init__(self, states: list[str], pages: list[list[list[str]]]) -> None:
        self.states = states
        self.pages = pages
        self.started: list[dict[str, Any]] = []

    def start_query_execution(self, **kwargs: Any) -> Mapping[str, Any]:
        self.started.append(kwargs)
        return {"QueryExecutionId": "q-1"}

    def get_query_execution(self, **kwargs: Any) -> Mapping[str, Any]:
        state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        stats = {
            "DataScannedInBytes": 2048,
            "EngineExecutionTimeInMillis": 900,
            "QueryQueueTimeInMillis": 50,
            "TotalExecutionTimeInMillis": 1000,
        }
        return {"QueryExecution": {"Status": {"State": state}, "Statistics": stats}}

    def get_query_results(self, **kwargs: Any) -> Mapping[str, Any]:
        index = int(kwargs.get("NextToken", 0))
        rows = [{"Data": [{"VarCharValue": value} for value in row]} for row in self.pages[index]]
        page: dict[str, Any] = {"ResultSet": {"Rows": rows}}
        if index + 1 < len(self.pages):
            page["NextToken"] = str(index + 1)
        return page


HEADER = ["city", "delivered_orders", "late_orders", "late_rate"]


def test_query_runs_in_the_proof_workgroup_and_reads_every_page() -> None:
    client = FakeAthena(
        ["QUEUED", "RUNNING", "SUCCEEDED"],
        [[HEADER, ["bengaluru", "10", "2", "0.2"]], [["chennai", "4", "1", "0.25"]]],
    )

    stats, rates = athena.run_query(client, SETTINGS, "SELECT 1", sleep=lambda s: None)

    assert client.started[0]["WorkGroup"] == "deliverypulse-proof"
    assert client.started[0]["QueryExecutionContext"] == {"Database": "deliverypulse_proof"}
    assert stats == QueryStats("SUCCEEDED", 2048, 900, 50, 1000)
    assert rates == [CityRate("bengaluru", 10, 2), CityRate("chennai", 4, 1)]
    assert rates[1].late_rate == 0.25


def test_failed_query_raises_without_reading_results() -> None:
    client = FakeAthena(["FAILED"], [])

    with pytest.raises(AthenaQueryError, match="ended FAILED"):
        athena.run_query(client, SETTINGS, "SELECT 1", sleep=lambda s: None)


def test_query_wait_times_out() -> None:
    client = FakeAthena(["RUNNING"], [])

    with pytest.raises(AthenaQueryError, match="still RUNNING"):
        athena.wait_for_query(
            client, "q-1", poll_seconds=1, timeout_seconds=2, sleep=lambda s: None
        )


def test_unexpected_result_header_is_refused() -> None:
    client = FakeAthena(["SUCCEEDED"], [[["x", "y", "z"]]])

    with pytest.raises(AthenaQueryError, match="header"):
        athena.read_results(client, "q-1")


def test_evidence_records_the_match_and_never_identifiers() -> None:
    stats = QueryStats("SUCCEEDED", 2048, 900, 50, 1000)
    rates = [CityRate("bengaluru", 10, 2)]

    record = athena.evidence(SETTINGS, stats, rates, rates)

    assert record["matches_postgres"] is True
    assert record["region"] == "ap-southeast-2"
    assert (
        athena.evidence(SETTINGS, stats, rates, [CityRate("bengaluru", 10, 3)])["matches_postgres"]
        is False
    )
    with pytest.raises(AthenaQueryError, match="account ID"):
        athena.evidence(AthenaSettings("123456789012", "db"), stats, rates, rates)


def test_evidence_file_is_written_under_the_given_directory(tmp_path: Path) -> None:
    path = athena.write_evidence({"matches_postgres": True}, tmp_path / "aws")

    assert path.parent == tmp_path / "aws" and path.name.startswith("athena-")


def test_report_shows_rates_cost_and_match(capsys: pytest.CaptureFixture[str]) -> None:
    athena.print_report(
        QueryStats("SUCCEEDED", 2048, 900, 50, 1000), [CityRate("pune", 8, 2)], True
    )

    out = capsys.readouterr().out
    assert "pune" in out and "0.2500" in out
    assert "matches PostgreSQL: yes" in out
    assert not aws.contains_account_identifiers(out)


# ------------------------------------------------------------------------ read-only checks ---


class FakeService:
    """A fake AWS client: each named method returns a value or raises a ClientError code."""

    def __init__(self, **behaviour: Any) -> None:
        self.behaviour = behaviour
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> Callable[..., Any]:
        if name not in self.behaviour:
            raise AttributeError(name)

        def call(**kwargs: Any) -> Any:
            self.calls.append(name)
            outcome = self.behaviour[name]
            if isinstance(outcome, str) and outcome.startswith("error:"):
                raise client_error(outcome.removeprefix("error:"))
            return outcome

        return call


class FakeSession:
    def __init__(self, **services: FakeService) -> None:
        self.services = services
        self.region_name = "ap-southeast-2"

    def client(self, name: str) -> FakeService:
        return self.services[name]


def session(**services: FakeService) -> Any:
    return FakeSession(**services)


def test_caller_name_hides_account_and_session() -> None:
    arn = "arn:aws:sts::123456789012:assumed-role/deliverypulse-proof-pipeline/botocore-session-1"

    assert aws_check.caller_name(arn) == "role deliverypulse-proof-pipeline"


def test_negative_checks_pass_only_when_everything_is_denied() -> None:
    s3_client = FakeService(list_buckets="error:AccessDenied", put_object="error:AccessDenied")
    fake = session(
        s3=s3_client,
        athena=FakeService(get_work_group="error:AccessDeniedException"),
        glue=FakeService(get_databases="error:AccessDeniedException"),
        iam=FakeService(list_roles="error:AccessDenied"),
    )

    results = aws_check.negative(fake, "deliverypulse-proof-0a1b2c3d")

    assert all(result.passed for result in results) and len(results) == 5


def test_an_allowed_write_fails_and_is_undone() -> None:
    s3_client = FakeService(list_buckets="error:AccessDenied", put_object={}, delete_object={})
    fake = session(
        s3=s3_client,
        athena=FakeService(get_work_group="error:AccessDeniedException"),
        glue=FakeService(get_databases="error:AccessDeniedException"),
        iam=FakeService(list_roles="error:AccessDenied"),
    )

    results = aws_check.negative(fake, "deliverypulse-proof-0a1b2c3d")

    write = results[-1]
    assert not write.passed and "ALLOWED" in write.detail
    assert s3_client.calls[-1] == "delete_object"


def test_a_different_error_is_not_counted_as_denied() -> None:
    result = aws_check.expect_denied("x", FakeService(go="error:ThrottlingException").go)

    assert result == CheckResult("x", False, "ThrottlingException")


def test_teardown_passes_only_when_everything_is_absent() -> None:
    names = {"bucket": "b-1", "workgroup": "w", "database": "d", "role": "r"}
    gone = session(
        s3=FakeService(head_bucket="error:404"),
        athena=FakeService(get_work_group="error:InvalidRequestException"),
        glue=FakeService(get_database="error:EntityNotFoundException"),
        iam=FakeService(get_role="error:NoSuchEntity"),
        resourcegroupstaggingapi=FakeService(get_resources={"ResourceTagMappingList": []}),
    )
    left = session(
        s3=FakeService(head_bucket={}),
        athena=FakeService(get_work_group="error:InvalidRequestException"),
        glue=FakeService(get_database="error:EntityNotFoundException"),
        iam=FakeService(get_role="error:NoSuchEntity"),
        resourcegroupstaggingapi=FakeService(get_resources={"ResourceTagMappingList": [{}]}),
    )

    assert all(r.passed for r in aws_check.teardown(gone, names))
    failed = [r.name for r in aws_check.teardown(left, names) if not r.passed]
    assert failed == ["bucket", "tagged resources"]


def test_preflight_requires_an_enforced_small_scan_limit() -> None:
    def preflight_with(config: dict[str, Any]) -> list[CheckResult]:
        fake = session(
            sts=FakeService(get_caller_identity={"Arn": "arn:aws:sts::1:assumed-role/p/s"}),
            s3=FakeService(head_bucket={}),
            athena=FakeService(get_work_group={"WorkGroup": {"Configuration": config}}),
            glue=FakeService(get_table={}),
        )
        return aws_check.preflight(fake, "b-1", "w", "d")

    good = {"EnforceWorkGroupConfiguration": True, "BytesScannedCutoffPerQuery": 104857600}
    assert all(r.passed for r in preflight_with(good))
    loose = {"EnforceWorkGroupConfiguration": False, "BytesScannedCutoffPerQuery": 104857600}
    assert not all(r.passed for r in preflight_with(loose))


def test_report_exit_code(capsys: pytest.CaptureFixture[str]) -> None:
    assert aws_check.report("t", [CheckResult("a", True, "ok")]) == 0
    assert aws_check.report("t", [CheckResult("a", False, "no")]) == 1
    assert "FAIL  a: no" in capsys.readouterr().out


def test_check_command_needs_the_aws_settings(tmp_path: Path) -> None:
    assert aws_check.main(["negative"], env_file=env_file(tmp_path)) == 1
