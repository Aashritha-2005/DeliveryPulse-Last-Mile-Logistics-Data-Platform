"""Map an Airflow data interval to the generated batch it covers.

The DAG runs weekly on the generator's batch windows (Thursday 00:00 Asia/Kolkata), so each
scheduled run covers exactly one batch window and loads the dataset through that batch. The
windows come from the generator's own configuration, so the DAG and the data cannot drift apart
silently (a unit test compares them).
"""

from datetime import UTC, datetime, timedelta

from src.ingestion.generator_config import (
    DEFAULT_ORDERS,
    DEFAULT_SEED,
    SECONDS_PER_DAY,
    GeneratorConfig,
)

_CONFIG = GeneratorConfig.create(orders=DEFAULT_ORDERS, seed=DEFAULT_SEED)

DAYS_PER_BATCH: int = _CONFIG.days_per_batch
BATCH_COUNT: int = _CONFIG.n_batches
FIRST_WINDOW_START: datetime = datetime.fromtimestamp(_CONFIG.window_start_epoch(), UTC)
WINDOW_LENGTH = timedelta(seconds=DAYS_PER_BATCH * SECONDS_PER_DAY)


def batch_windows() -> list[tuple[datetime, datetime]]:
    """[start, end) of every batch window, oldest first, as UTC datetimes."""
    return [
        (FIRST_WINDOW_START + k * WINDOW_LENGTH, FIRST_WINDOW_START + (k + 1) * WINDOW_LENGTH)
        for k in range(BATCH_COUNT)
    ]


def batch_for_interval(start: datetime, end: datetime) -> int:
    """Batch sequence (1-based) whose window is exactly ``[start, end)``.

    Raises:
        ValueError: if the interval is naive, misaligned, or outside the generated windows.
    """
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("data interval bounds must be timezone-aware")
    for sequence, (window_start, window_end) in enumerate(batch_windows(), start=1):
        if start == window_start and end == window_end:
            return sequence
    raise ValueError(
        f"data interval {start.isoformat()} to {end.isoformat()} is not one of the "
        f"{BATCH_COUNT} batch windows starting {FIRST_WINDOW_START.isoformat()}"
    )
