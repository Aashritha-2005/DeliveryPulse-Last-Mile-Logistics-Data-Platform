"""The command each DAG task runs, built in one tested place.

Tasks run the project's own command-line entry points with the project's isolated virtual
environment inside the Airflow image (``/opt/deliverypulse/.venv``), never Airflow's Python.
Database settings come from the container environment; nothing secret is part of a command.
"""

import shlex

from src.ingestion.generator_config import DEFAULT_ORDERS, DEFAULT_SEED

PROJECT_DIR = "/opt/deliverypulse"
PYTHON = f"{PROJECT_DIR}/.venv/bin/python"
RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
# The quality JSON is runtime output: written to the Airflow logs volume inside the container,
# never into the project directory.
QUALITY_OUTPUT = "/opt/airflow/logs/deliverypulse/quality-latest.json"
# Rendered by Airflow at run time from the run's data interval (see dags/delivery_pipeline.py).
BATCH_TEMPLATE = "{{ batch_for(data_interval_start, data_interval_end) }}"


def generate() -> list[str]:
    """Generate the default synthetic dataset (a no-op when it already exists unchanged).

    The generator's side outputs go to its own default directory; the pipeline never reads them.
    """
    return [
        PYTHON,
        "-m",
        "src.ingestion.generate_data",
        "--orders",
        str(DEFAULT_ORDERS),
        "--seed",
        str(DEFAULT_SEED),
        "--raw-dir",
        RAW_DIR,
    ]


def clean() -> list[str]:
    """Validate every batch and rebuild the cleaned Parquet layer."""
    return [
        PYTHON,
        "-m",
        "src.transformations.transform",
        "--raw-dir",
        RAW_DIR,
        "--processed-dir",
        PROCESSED_DIR,
    ]


def load(through_batch: str) -> list[str]:
    """Load batches 1..``through_batch`` (already loaded batches are skipped)."""
    return [
        PYTHON,
        "-m",
        "src.database.load_postgres",
        "--raw-dir",
        RAW_DIR,
        "--through-batch",
        through_batch,
    ]


def quality_gate() -> list[str]:
    """Run every quality check on the database; exit 1 on a blocking failure."""
    return [PYTHON, "-m", "src.quality.run", "--output", QUALITY_OUTPUT]


def refresh_aggregates() -> list[str]:
    """Rebuild the dimensions and daily aggregates for every date."""
    return [PYTHON, "-m", "src.database.metrics"]


def bash(argv: list[str]) -> str:
    """A shell command line for ``argv`` (every argument quoted)."""
    return shlex.join(argv)
