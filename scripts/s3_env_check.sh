#!/usr/bin/env bash
# Refuse to start or use the object store unless its .env variables are set to safe values.
#
# Compose already refuses missing or empty variables (`${VAR:?}` in docker-compose.s3.yml).
# This extra check also rejects the CHANGE_ME placeholders, requires the four keys to be 16 to
# 128 letters and digits (they are written into a JSON file, so no quoting characters are
# allowed), requires different admin and pipeline keys, and checks the port and bucket name.
# Values are never printed. .env is read with grep, never sourced.
#
# Compatible with macOS /bin/bash 3.2.
set -euo pipefail

readonly KEYS="S3_ADMIN_ACCESS_KEY S3_ADMIN_SECRET_KEY S3_ACCESS_KEY_ID S3_SECRET_ACCESS_KEY"
readonly OTHERS="S3_API_PORT S3_ENDPOINT_URL S3_BUCKET"
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
for name in $OTHERS $KEYS; do
    value="$(effective_value "$name")"
    if [ -z "$value" ]; then
        echo "ERROR: ${name} is missing or empty in ${ENV_FILE} (see .env.example)." >&2
        problems=$((problems + 1))
    elif [ "$value" = "CHANGE_ME" ]; then
        echo "ERROR: ${name} is still the CHANGE_ME placeholder (see docs/operations.md, Object storage lake)." >&2
        problems=$((problems + 1))
    fi
done

for name in $KEYS; do
    value="$(effective_value "$name")"
    if [ -n "$value" ] && [ "$value" != "CHANGE_ME" ] \
        && ! printf '%s' "$value" | grep -Eq '^[A-Za-z0-9]{16,128}$'; then
        echo "ERROR: ${name} must be 16 to 128 letters and digits (for example: openssl rand -hex 20)." >&2
        problems=$((problems + 1))
    fi
done

if [ "$(effective_value S3_ADMIN_ACCESS_KEY)" = "$(effective_value S3_ACCESS_KEY_ID)" ]; then
    echo "ERROR: S3_ADMIN_ACCESS_KEY and S3_ACCESS_KEY_ID must be different keys." >&2
    problems=$((problems + 1))
fi

port="$(effective_value S3_API_PORT)"
if [ -n "$port" ] && ! printf '%s' "$port" | grep -Eq '^[0-9]{1,5}$'; then
    echo "ERROR: S3_API_PORT must be a TCP port number." >&2
    problems=$((problems + 1))
fi

bucket="$(effective_value S3_BUCKET)"
if [ -n "$bucket" ] && ! printf '%s' "$bucket" | grep -Eq '^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$'; then
    echo "ERROR: S3_BUCKET must be 3-63 lowercase letters, digits or hyphens." >&2
    problems=$((problems + 1))
fi

if [ "$problems" -gt 0 ]; then
    exit 1
fi
echo "Object store .env check: the S3_ variables are set (values not shown)."
