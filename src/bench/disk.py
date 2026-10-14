"""Disk budget and free-space guards for everything the benchmark writes.

Every benchmark file lives under ``data/bench/``: shards, read-benchmark copies, Spark's
``spark.local.dir`` (``data/bench/spark-local``) and temporary files (``data/bench/tmp``, used as
``TMPDIR`` and ``java.io.tmpdir``). Their total must stay within the budget (default 4 GiB; a
higher value is refused). Free space on ``/`` and on the volume of ``data/`` must stay above
10 GiB. Sizes are allocated bytes (``st_blocks``), what the files really take on disk.
"""

import os
import shutil
from dataclasses import dataclass
from pathlib import Path

GIB = 1024**3
MAX_BUDGET_BYTES = 4 * GIB  # never higher without the user's approval
MIN_FREE_BYTES = 10 * GIB
BENCH_ROOT = Path("data/bench")
SHARDS_DIR = "shards"
READS_DIR = "reads"
SPARK_LOCAL_DIR = "spark-local"
TMP_DIR = "tmp"


class DiskGuardError(RuntimeError):
    """A disk budget or free-space limit would be, or has been, exceeded."""


def budget_bytes(gib: float) -> int:
    """The budget in bytes for ``gib``; refuses anything above 4 GiB or not positive."""
    if not 0 < gib <= MAX_BUDGET_BYTES / GIB:
        raise DiskGuardError(
            f"Disk budget must be above 0 and at most {MAX_BUDGET_BYTES // GIB} GiB, got {gib}."
        )
    return int(gib * GIB)


def allocated_bytes(path: Path) -> int:
    """Bytes allocated on disk for ``path`` (a file or a whole tree); 0 if it does not exist."""
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_blocks * 512
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += (Path(root) / name).stat().st_blocks * 512
            except FileNotFoundError:  # removed while walking (Spark spill files)
                continue
    return total


def free_bytes(path: Path) -> int:
    """Free bytes on the volume holding ``path`` (or its nearest existing parent)."""
    probe = path
    while not probe.exists():
        probe = probe.parent
    return shutil.disk_usage(probe).free


@dataclass(frozen=True)
class DiskGuard:
    """Checks the budget for ``root`` and the free space on ``/`` and on ``root``'s volume."""

    root: Path
    budget: int
    min_free: int = MIN_FREE_BYTES

    def usage(self) -> int:
        """Bytes currently used under the benchmark root."""
        return allocated_bytes(self.root)

    def check(self, projected_extra: int = 0, what: str = "the next step") -> None:
        """Raise :class:`DiskGuardError` if current usage plus ``projected_extra`` would exceed
        the budget, or if free space on ``/`` or on the data volume is below the minimum.
        """
        used = self.usage()
        if used + projected_extra > self.budget:
            raise DiskGuardError(
                f"Disk budget: {what} would need about {_gib(used + projected_extra)} GiB under "
                f"{self.root} (now {_gib(used)} GiB), above the {_gib(self.budget)} GiB budget."
            )
        for volume in (Path("/"), self.root):
            free = free_bytes(volume)
            if free - projected_extra < self.min_free:
                raise DiskGuardError(
                    f"Free disk: {what} would leave {_gib(free - projected_extra)} GiB free on "
                    f"the volume of {'/' if volume == Path('/') else 'data/'}, below "
                    f"{_gib(self.min_free)} GiB."
                )


@dataclass(frozen=True)
class ShardSizes:
    """Measured bytes per 50,000-order shard, used to project a size before creating it."""

    input_bytes: int  # orders + delivery events Parquet
    read_copy_bytes: int  # single-file + partitioned read-benchmark copies
    raw_transient_bytes: int  # raw files of one shard while it is generated


def projected_bytes(shards: int, sizes: ShardSizes, spill_factor: float, reads: bool) -> int:
    """Projected peak bytes under the benchmark root for ``shards`` shards.

    Input of all shards, plus Spark spill (``spill_factor`` x input, one run at a time), plus
    the read-benchmark copies when ``reads``, plus one shard's raw files during generation.
    """
    data = shards * sizes.input_bytes
    spill = int(data * spill_factor)
    copies = shards * sizes.read_copy_bytes if reads else 0
    return data + spill + copies + sizes.raw_transient_bytes


def _gib(value: int) -> str:
    return f"{value / GIB:.2f}"


def remove_tree(path: Path) -> None:
    """Delete a benchmark work directory (spill, temp) if it exists."""
    if path.exists():
        shutil.rmtree(path)
