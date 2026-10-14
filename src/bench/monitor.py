"""Pure helpers of the benchmark harness (scripts/benchmark.py): medians, the size plan and the
privacy-safe machine record.
"""

import statistics
from collections.abc import Mapping, Sequence
from typing import Any

GIB = 1024**3
MEMORY_LIMIT_BYTES = 4 * GIB
# Benchmark sizes: label -> number of 50,000-order shards.
SIZES: dict[str, int] = {"50k": 1, "250k": 5, "500k": 10, "1m": 20, "2m": 40, "5m": 100}
# Only these fields describe the machine; no hostname, user name or path is ever recorded.
MACHINE_FIELDS = (
    "hardware_model",
    "cpu_cores",
    "ram_bytes",
    "os",
    "python",
    "jdk",
    "packages",
)


def median(values: Sequence[float]) -> float:
    """The median of ``values`` (at least one)."""
    return float(statistics.median(values))


def sizes_up_to(largest: str) -> list[tuple[str, int]]:
    """The size plan, smallest first, up to and including ``largest``."""
    if largest not in SIZES:
        raise ValueError(f"Unknown size {largest!r}; choose one of {', '.join(SIZES)}.")
    labels = list(SIZES)
    return [(label, SIZES[label]) for label in labels[: labels.index(largest) + 1]]


def machine_record(values: Mapping[str, Any]) -> dict[str, Any]:
    """Keep only the allowed machine fields (privacy: no hostname, user or path)."""
    return {name: values[name] for name in MACHINE_FIELDS if name in values}


def jdk_version(release_text: str) -> str:
    """The JDK version from the text of ``$JAVA_HOME/release`` (``JAVA_VERSION="21.0.12"``),
    with the vendor build when present (``IMPLEMENTOR_VERSION``)."""
    values: dict[str, str] = {}
    for line in release_text.splitlines():
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"')
    version = values.get("JAVA_VERSION", "")
    vendor = values.get("IMPLEMENTOR_VERSION", "")
    return f"{version} ({vendor})" if version and vendor else version
