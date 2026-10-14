#!/usr/bin/env bash
# Refuse to build or start Airflow unless its four .env variables are set to real values.
#
# Compose already refuses a missing or empty variable (`${VAR:?}` in docker-compose.airflow.yml);
# this extra check also rejects the CHANGE_ME placeholders copied from .env.example and a
# non-numeric AIRFLOW_UID. Values are never printed. .env is read with grep, never sourced.
#
# Compatible with macOS /bin/bash 3.2.
set -euo pipefail

readonly NAMES="AIRFLOW_UID AIRFLOW_DB_PASSWORD AIRFLOW_FERNET_KEY AIRFLOW_JWT_SECRET"
readonly ENV_FILE=".env"

project_root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$project_root"

if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: ${ENV_FILE} not found. Create it with: cp .env.example .env" >&2
    exit 1
fi

# The value Compose will use: the shell environment wins over .env.
effective_value() {
    local name="$1"
    if [ -n "${!name+set}" ]; then
        printf '%s' "${!name}"
    else
        grep -E "^${name}=" "$ENV_FILE" | tail -n 1 | cut -d= -f2- || true
    fi
}

problems=0
for name in $NAMES; do
    value="$(effective_value "$name")"
    if [ -z "$value" ]; then
        echo "ERROR: ${name} is missing or empty in ${ENV_FILE} (see .env.example)." >&2
        problems=$((problems + 1))
    elif [ "$value" = "CHANGE_ME" ]; then
        echo "ERROR: ${name} is still the CHANGE_ME placeholder (see docs/operations.md, Airflow)." >&2
        problems=$((problems + 1))
    elif [ "$name" = "AIRFLOW_UID" ] && ! printf '%s' "$value" | grep -Eq '^[0-9]+$'; then
        echo "ERROR: AIRFLOW_UID must be a numeric user id (the output of: id -u)." >&2
        problems=$((problems + 1))
    fi
done

if [ "$problems" -gt 0 ]; then
    exit 1
fi
echo "Airflow .env check: the 4 AIRFLOW_ variables are set (values not shown)."
