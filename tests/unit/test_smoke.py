"""Smoke tests proving the toolchain works: interpreter version and package import."""

import importlib
import sys

import pytest

pytestmark = pytest.mark.unit


def test_python_is_3_12() -> None:
    """The project venv runs the uv-managed Python 3.12 pinned in pyproject.toml."""
    assert sys.version_info[:2] == (3, 12)


def test_src_package_imports() -> None:
    """The ``src`` package is importable from the project root."""
    module = importlib.import_module("src")
    assert module.__name__ == "src"
