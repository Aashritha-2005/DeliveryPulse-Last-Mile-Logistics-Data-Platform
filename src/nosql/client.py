"""Bounded local MongoDB connections; no administrative client in application code."""

from typing import TYPE_CHECKING, Any

from src.config import read_environment
from src.nosql.config import MongoRole, mongo_settings

if TYPE_CHECKING:
    from pymongo import MongoClient
    from pymongo.database import Database


def connect(
    role: MongoRole, *, test_database: bool = False
) -> tuple["MongoClient[dict[str, Any]]", "Database[dict[str, Any]]"]:
    """Authenticate only as the selected non-administrator role, with finite timeouts."""
    from pymongo import MongoClient

    settings = mongo_settings(read_environment(), role)
    name = settings.database + ("_test" if test_database else "")
    client: MongoClient[dict[str, Any]] = MongoClient(
        host="127.0.0.1",
        port=settings.port,
        username=settings.username,
        password=settings.password,
        authSource=name,
        directConnection=True,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        socketTimeoutMS=30000,
        maxPoolSize=2,
        retryWrites=False,
        tz_aware=True,
    )
    return client, client[name]
