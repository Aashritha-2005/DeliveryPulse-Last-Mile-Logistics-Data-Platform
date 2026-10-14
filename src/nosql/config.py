"""Validate optional MongoDB settings without importing the client."""

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

from src.config import ConfigError, read_environment

MongoRole = Literal["app", "readonly"]


@dataclass(frozen=True)
class MongoSettings:
    """Local connection settings; credentials are excluded from representations."""

    port: int
    database: str
    username: str = field(repr=False)
    password: str = field(repr=False)


def validate_environment(values: Mapping[str, str]) -> None:
    """Reject placeholders and invalid names without echoing supplied values."""
    names = ["MONGO_PORT", "MONGO_DB"] + [
        f"MONGO_{role}_{part}"
        for role in ("ROOT", "APP", "READONLY")
        for part in ("USER", "PASSWORD")
    ]
    for name in names:
        if not values.get(name) or "CHANGE_ME" in values[name]:
            raise ConfigError(f"Missing or placeholder setting: {name}")
    port = values["MONGO_PORT"]
    if not port.isascii() or not port.isdecimal() or not 1024 <= int(port) <= 65535:
        raise ConfigError("MONGO_PORT must be an unprivileged TCP port (1024-65535)")
    database = values["MONGO_DB"]
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,39}", database) or database in {
        "admin",
        "local",
        "config",
    }:
        raise ConfigError("MONGO_DB must be a non-system lowercase database name")
    users = []
    passwords = []
    for role in ("ROOT", "APP", "READONLY"):
        user = values[f"MONGO_{role}_USER"]
        password = values[f"MONGO_{role}_PASSWORD"]
        if not re.fullmatch(r"[a-z][a-z0-9_]{2,39}", user):
            raise ConfigError(f"MONGO_{role}_USER must be 3-40 lowercase letters, digits or _")
        if not re.fullmatch(r"[A-Za-z0-9]{20,128}", password):
            raise ConfigError(f"MONGO_{role}_PASSWORD must be 20-128 letters or digits")
        users.append(user)
        passwords.append(password)
    if len(set(users)) != 3 or len(set(passwords)) != 3:
        raise ConfigError("MongoDB users and passwords must be distinct for all three roles")


def mongo_settings(values: Mapping[str, str], role: MongoRole) -> MongoSettings:
    """Select an application identity after validating configuration."""
    validate_environment(values)
    return MongoSettings(
        int(values["MONGO_PORT"]),
        values["MONGO_DB"],
        values[f"MONGO_{role.upper()}_USER"],
        values[f"MONGO_{role.upper()}_PASSWORD"],
    )


def main() -> int:
    """Check configuration without printing any values."""
    try:
        validate_environment(read_environment())
    except ConfigError as exc:
        print(f"ERROR: {exc}")
        return 1
    print("MongoDB configuration: valid local port, database and three distinct credentials")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
