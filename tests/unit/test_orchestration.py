"""Unit tests (no Airflow, no database): batch windows, task commands and the loader option."""

import argparse
from datetime import UTC, datetime, timedelta, timezone

import pytest

from src.database.load_postgres import positive_int
from src.ingestion.generator_config import (
    DEFAULT_ORDERS,
    DEFAULT_REFERENCE_TS,
    DEFAULT_SEED,
    GeneratorConfig,
)
from src.orchestration import commands
from src.orchestration.batches import BATCH_COUNT, batch_for_interval, batch_windows

pytestmark = pytest.mark.unit

IST = timezone(timedelta(hours=5, minutes=30))


def test_windows_equal_the_generators_batch_windows() -> None:
    config = GeneratorConfig.create(orders=DEFAULT_ORDERS, seed=DEFAULT_SEED)
    ends = [int(end.timestamp()) for _, end in batch_windows()]
    assert ends == list(config.cutoffs())
    assert int(batch_windows()[0][0].timestamp()) == config.window_start_epoch()
    assert BATCH_COUNT == config.n_batches == 3
    assert batch_windows()[-1][1] == datetime.fromisoformat(DEFAULT_REFERENCE_TS)


def test_windows_start_on_thursday_midnight_in_kolkata() -> None:
    # The DAG's cron ("0 0 * * 4", Asia/Kolkata) relies on this.
    for start, end in batch_windows():
        local = start.astimezone(IST)
        assert (local.weekday(), local.hour, local.minute) == (3, 0, 0)
        assert end - start == timedelta(days=7)


@pytest.mark.parametrize("sequence", [1, 2, 3])
def test_each_window_maps_to_its_batch(sequence: int) -> None:
    start, end = batch_windows()[sequence - 1]
    assert batch_for_interval(start, end) == sequence
    # The same instants expressed in IST (as Airflow may pass them) map identically.
    assert batch_for_interval(start.astimezone(IST), end.astimezone(IST)) == sequence


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (datetime(2026, 9, 9, 18, 30, tzinfo=UTC), datetime(2026, 9, 16, 18, 31, tzinfo=UTC)),
        (datetime(2026, 9, 30, 18, 30, tzinfo=UTC), datetime(2026, 10, 7, 18, 30, tzinfo=UTC)),
        (datetime(2026, 9, 2, 18, 30, tzinfo=UTC), datetime(2026, 9, 9, 18, 30, tzinfo=UTC)),
    ],
)
def test_misaligned_or_out_of_range_intervals_are_rejected(start: datetime, end: datetime) -> None:
    with pytest.raises(ValueError, match="not one of the 3 batch windows"):
        batch_for_interval(start, end)


def test_naive_interval_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        batch_for_interval(datetime(2026, 9, 9, 18, 30), datetime(2026, 9, 16, 18, 30))


def test_task_commands_run_the_project_venv_with_exact_arguments() -> None:
    python = "/opt/deliverypulse/.venv/bin/python"
    assert commands.generate() == [
        python,
        "-m",
        "src.ingestion.generate_data",
        "--orders",
        "50000",
        "--seed",
        "42",
        "--raw-dir",
        "data/raw",
    ]
    assert commands.clean() == [
        python,
        "-m",
        "src.transformations.transform",
        "--raw-dir",
        "data/raw",
        "--processed-dir",
        "data/processed",
    ]
    assert commands.load("2")[-2:] == ["--through-batch", "2"]
    assert commands.load("2")[:3] == [python, "-m", "src.database.load_postgres"]
    assert commands.quality_gate() == [
        python,
        "-m",
        "src.quality.run",
        "--output",
        "/opt/airflow/logs/deliverypulse/quality-latest.json",
    ]
    assert commands.refresh_aggregates() == [python, "-m", "src.database.metrics"]


def test_bash_quotes_the_batch_template_as_one_argument() -> None:
    line = commands.bash(commands.load(commands.BATCH_TEMPLATE))
    assert line.endswith(
        "--through-batch '{{ batch_for(data_interval_start, data_interval_end) }}'"
    )


def test_quality_output_is_never_inside_the_project() -> None:
    assert not commands.QUALITY_OUTPUT.startswith(commands.PROJECT_DIR)


@pytest.mark.parametrize(("text", "value"), [("1", 1), ("3", 3)])
def test_through_batch_accepts_positive_whole_numbers(text: str, value: int) -> None:
    assert positive_int(text) == value


@pytest.mark.parametrize("text", ["0", "-1", "two", "1.5"])
def test_through_batch_rejects_anything_else(text: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        positive_int(text)
