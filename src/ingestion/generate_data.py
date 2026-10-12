"""Generate a deterministic, multi-batch synthetic delivery dataset with known defects.

Usage (from the project root)::

    uv run python -m src.ingestion.generate_data --orders 50000 --seed 42

Writes ``<raw-dir>/<dataset>/batch-<k>/`` (the immutable source the pipeline reads) and
``<ground-truth-dir>/<dataset>/defect_manifest.json`` (ground truth for tests and reports only).
Re-running with the same arguments is a no-op; differing existing output is an error.
"""

import argparse
import json
import logging
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from src.groundtruth.manifest import MANIFEST_FILE_NAME, Manifest, manifest_bytes
from src.ingestion.defects import inject_defects, plan_drivers
from src.ingestion.generator_config import (
    BATCH_METADATA_FILE,
    GenerationError,
    GeneratorConfig,
    add_config_arguments,
    config_from_args,
)
from src.ingestion.simulation import (
    CANCEL_CAUSE_CUSTOMER,
    CANCEL_CAUSE_NO_DRIVER,
    World,
    order_transitions,
    place_records,
    simulate,
)
from src.ingestion.writers import (
    ImmutableOutputError,
    batch_metadata_bytes,
    compute_batch_id,
    publish,
    serialize_batches,
)
from src.utils.logging import configure_logging

logger = logging.getLogger("deliverypulse.generate")

NO_DRIVER_CANCEL_WARNING_SHARE = 0.05


@dataclass(frozen=True)
class GenerationResult:
    """Outcome of one :func:`generate` call."""

    dataset: str
    raw_dir: Path
    manifest_path: Path
    raw_status: str
    manifest_status: str
    file_sizes: dict[str, int]
    file_rows: dict[str, int]
    simulation_summary: dict[str, int]


def simulation_summary(config: GeneratorConfig, world: World) -> dict[str, int]:
    """Whole-simulation counts recorded in the manifest (ground truth, not in raw files)."""
    final_cutoff = config.cutoffs()[-1]
    causes = [order.cancel_cause for order in world.orders]
    in_progress = sum(
        1 for order in world.orders if order_transitions(order)[-1][0] >= final_cutoff
    )
    return {
        "drivers_total": len(world.drivers),
        "drivers_idle": sum(1 for driver in world.drivers if driver.idle),
        "drivers_delayed": sum(1 for driver in world.drivers if driver.delayed),
        "orders_total": len(world.orders),
        "orders_delivered": sum(1 for order in world.orders if order.delivered is not None),
        "orders_cancelled_customer": causes.count(CANCEL_CAUSE_CUSTOMER),
        "orders_cancelled_no_driver": causes.count(CANCEL_CAUSE_NO_DRIVER),
        "orders_final_state_after_last_cutoff": in_progress,
        "delivery_events_simulated": len(world.delivery_events),
        "shift_events_simulated": len(world.shift_events),
    }


def generate(config: GeneratorConfig, raw_dir: Path, ground_truth_dir: Path) -> GenerationResult:
    """Generate the dataset for ``config`` and publish it immutably."""
    rng = np.random.default_rng(config.seed)
    plan = plan_drivers(config, rng)
    world = simulate(config, rng, plan)
    placements = place_records(config, world)
    cases = inject_defects(config, rng, world, placements, plan)

    canonical = config.canonical_json()
    raw_files: dict[str, bytes] = {}
    file_rows: dict[str, int] = {}
    batch_ids: list[str] = []
    for batch in serialize_batches(config, placements):
        batch_id = compute_batch_id(canonical, batch.sequence, batch.files)
        batch_ids.append(batch_id)
        folder = f"batch-{batch.sequence}"
        for name in sorted(batch.files):
            raw_files[f"{folder}/{name}"] = batch.files[name]
            file_rows[f"{folder}/{name}"] = batch.rows[name]
        raw_files[f"{folder}/{BATCH_METADATA_FILE}"] = batch_metadata_bytes(config, batch, batch_id)

    summary = simulation_summary(config, world)
    manifest = Manifest(
        dataset=config.dataset_name(),
        config=json.loads(canonical),
        config_sha256=config.sha256(),
        batch_ids=tuple(batch_ids),
        simulation_summary=summary,
        cases=tuple(cases),
    )
    dataset_raw = raw_dir / config.dataset_name()
    dataset_truth = ground_truth_dir / config.dataset_name()
    raw_status = publish(dataset_raw, raw_files)
    manifest_status = publish(dataset_truth, {MANIFEST_FILE_NAME: manifest_bytes(manifest)})
    return GenerationResult(
        dataset=config.dataset_name(),
        raw_dir=dataset_raw,
        manifest_path=dataset_truth / MANIFEST_FILE_NAME,
        raw_status=raw_status,
        manifest_status=manifest_status,
        file_sizes={name: len(content) for name, content in sorted(raw_files.items())},
        file_rows=file_rows,
        simulation_summary=summary,
    )


def _log_result(result: GenerationResult, seconds: float) -> None:
    logger.info("dataset: %s", result.dataset)
    logger.info("raw data: %s (%s)", result.raw_dir, result.raw_status)
    logger.info("ground truth: %s (%s)", result.manifest_path, result.manifest_status)
    for name in sorted(result.file_sizes):
        rows = result.file_rows.get(name)
        rows_text = "" if rows is None else f", {rows} rows"
        logger.info("  %s: %d bytes%s", name, result.file_sizes[name], rows_text)
    summary = result.simulation_summary
    total = summary["orders_total"]
    no_driver = summary["orders_cancelled_no_driver"]
    logger.info(
        "orders: %d total, %d cancelled by customer, %d cancelled for lack of a free driver",
        total,
        summary["orders_cancelled_customer"],
        no_driver,
    )
    if no_driver > NO_DRIVER_CANCEL_WARNING_SHARE * total:
        logger.warning(
            "no-free-driver cancellations exceed %.0f%% of orders; consider a larger --drivers",
            NO_DRIVER_CANCEL_WARNING_SHARE * 100,
        )
    logger.info("generation took %.2f s", seconds)


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point; returns the process exit code."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    add_config_arguments(parser)
    args = parser.parse_args(argv)
    configure_logging()
    started = time.perf_counter()
    try:
        config = config_from_args(args)
        raw_dir: Path = args.raw_dir
        ground_truth_dir: Path = args.ground_truth_dir
        logger.info("generating dataset %s", config.dataset_name())
        result = generate(config, raw_dir, ground_truth_dir)
    except (GenerationError, ImmutableOutputError) as exc:
        logger.error("%s", exc)
        return 1
    _log_result(result, time.perf_counter() - started)
    return 0


if __name__ == "__main__":
    sys.exit(main())
