"""Logging setup shared by DeliveryPulse command-line entry points."""

import logging
import os
import sys

DEFAULT_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def configure_logging(level: str | None = None) -> None:
    """Configure root logging to stderr.

    The level comes from ``level``, else the ``LOG_LEVEL`` environment variable, else INFO.
    Never log secrets, connection strings or full records.
    """
    name = (level or os.environ.get("LOG_LEVEL") or DEFAULT_LEVEL).upper()
    numeric = logging.getLevelNamesMapping().get(name, logging.INFO)
    logging.basicConfig(level=numeric, format=LOG_FORMAT, stream=sys.stderr, force=True)
