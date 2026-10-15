"""User-run live checks; no clients or credentials are created during collection."""

import asyncio

import pytest

from scripts import k8s

pytestmark = pytest.mark.k8s


def test_pipeline_replay_preserves_all_analytics() -> None:
    """Both a successful Job and unchanged full analytics content are required."""
    asyncio.run(k8s.stage("replay"))


def test_wrong_password_fails_cleanly() -> None:
    """A separate wrong credential must fail without printing its value."""
    asyncio.run(k8s.negative_test())
