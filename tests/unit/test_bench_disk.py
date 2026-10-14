"""Unit tests of the benchmark's disk budget and guards (no services, no benchmark data)."""

from pathlib import Path

import pytest

from src.bench.disk import (
    GIB,
    MAX_BUDGET_BYTES,
    DiskGuard,
    DiskGuardError,
    ShardSizes,
    allocated_bytes,
    budget_bytes,
    projected_bytes,
    remove_tree,
)

pytestmark = pytest.mark.unit

SIZES = ShardSizes(input_bytes=17_301_504, read_copy_bytes=5_054_464, raw_transient_bytes=10**8)


@pytest.mark.parametrize("gib", [0.5, 1, 4])
def test_budget_accepts_values_up_to_four_gib(gib: float) -> None:
    assert budget_bytes(gib) == int(gib * GIB)


@pytest.mark.parametrize("gib", [4.1, 8, 0, -1])
def test_budget_refuses_more_than_four_gib_or_nothing(gib: float) -> None:
    with pytest.raises(DiskGuardError, match="at most 4 GiB"):
        budget_bytes(gib)


def test_max_budget_is_four_gib() -> None:
    assert MAX_BUDGET_BYTES == 4 * GIB


def test_allocated_bytes_counts_a_tree_and_ignores_missing_paths(tmp_path: Path) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "one.bin").write_bytes(b"x" * 5000)
    (tmp_path / "two.bin").write_bytes(b"y" * 5000)

    assert allocated_bytes(tmp_path) >= 10_000  # allocated blocks, at least the written bytes
    assert allocated_bytes(tmp_path / "missing") == 0


def test_guard_refuses_a_step_that_would_exceed_the_budget(tmp_path: Path) -> None:
    (tmp_path / "data.bin").write_bytes(b"x" * 100_000)
    guard = DiskGuard(tmp_path, budget=150_000, min_free=0)

    guard.check(10_000, "a small step")
    with pytest.raises(DiskGuardError, match="Disk budget: a big step"):
        guard.check(100_000, "a big step")


def test_guard_refuses_when_too_little_free_space_would_remain(tmp_path: Path) -> None:
    guard = DiskGuard(tmp_path, budget=4 * GIB, min_free=10**15)  # more than any disk here

    with pytest.raises(DiskGuardError, match="Free disk"):
        guard.check(0, "any step")


def test_projection_grows_with_shards_and_counts_spill_and_copies() -> None:
    one = projected_bytes(1, SIZES, spill_factor=2.0, reads=True)
    ten = projected_bytes(10, SIZES, spill_factor=2.0, reads=True)
    without_reads = projected_bytes(10, SIZES, spill_factor=2.0, reads=False)

    assert one == SIZES.input_bytes * 3 + SIZES.read_copy_bytes + SIZES.raw_transient_bytes
    assert ten == SIZES.input_bytes * 30 + SIZES.read_copy_bytes * 10 + SIZES.raw_transient_bytes
    assert without_reads == ten - SIZES.read_copy_bytes * 10


def test_projection_matches_the_approved_plan() -> None:
    """2M (40 shards) fits the 4 GiB budget; 5M (100 shards) does not."""
    budget = budget_bytes(4)

    assert projected_bytes(40, SIZES, 2.0, reads=True) < budget
    assert projected_bytes(100, SIZES, 2.0, reads=True) > budget


def test_remove_tree_is_a_no_op_for_a_missing_path(tmp_path: Path) -> None:
    target = tmp_path / "spark-local"
    target.mkdir()
    (target / "spill.bin").write_bytes(b"x")

    remove_tree(target)
    remove_tree(target)  # again: must not raise

    assert not target.exists()
