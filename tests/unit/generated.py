"""Helpers for unit tests that need a small generated dataset."""

from dataclasses import dataclass
from pathlib import Path

from src.groundtruth.manifest import Manifest, load_manifest
from src.ingestion.generate_data import GenerationResult, generate
from src.ingestion.generator_config import GeneratorConfig

SMALL_ORDERS = 1500
SMALL_SEED = 7


@dataclass(frozen=True)
class GeneratedDataset:
    """A dataset generated into ``root`` (``root/raw`` and ``root/ground_truth``)."""

    config: GeneratorConfig
    root: Path
    result: GenerationResult

    @property
    def raw_dir(self) -> Path:
        return self.root / "raw"

    @property
    def ground_truth_dir(self) -> Path:
        return self.root / "ground_truth"

    @property
    def dataset_dir(self) -> Path:
        return self.raw_dir / self.config.dataset_name()

    def manifest(self) -> Manifest:
        return load_manifest(self.result.manifest_path)


def generate_into(root: Path, config: GeneratorConfig) -> GeneratedDataset:
    """Generate ``config`` into ``root``."""
    result = generate(config, root / "raw", root / "ground_truth")
    return GeneratedDataset(config=config, root=root, result=result)
