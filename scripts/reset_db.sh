#!/usr/bin/env bash
# Delete the DeliveryPulse PostgreSQL data volume, after explicit confirmation.
#
# Stops the core services, then removes the named volume. The next `make up` re-initialises
# the database from sql/ (roles, schema, grants). Development data only.
#
# Compatible with macOS /bin/bash 3.2.
set -euo pipefail

readonly VOLUME_NAME="deliverypulse_pgdata"
readonly CONFIRM_WORD="DELETE"

project_root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$project_root"

if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: docker not found on PATH. Start Docker Desktop and retry." >&2
    exit 1
fi

echo "=================================================================="
echo " WARNING: THIS PERMANENTLY DELETES ALL DELIVERYPULSE DATABASE DATA"
echo "=================================================================="
echo " Docker volume to delete: ${VOLUME_NAME}"
echo " Every schema, table, row and role in it will be lost."
echo " The next 'make up' re-creates an empty database from sql/."
echo " This cannot be undone."
echo "=================================================================="

if [ ! -t 0 ]; then
    echo "Aborted: no interactive terminal to confirm. Nothing was deleted." >&2
    exit 1
fi

printf 'Type %s to confirm (anything else aborts): ' "$CONFIRM_WORD"
answer=""
read -r answer || true
if [ "$answer" != "$CONFIRM_WORD" ]; then
    echo "Aborted. Nothing was deleted." >&2
    exit 1
fi

echo "Stopping core services..."
docker compose --profile core down

if docker volume inspect "$VOLUME_NAME" >/dev/null 2>&1; then
    docker volume rm "$VOLUME_NAME" >/dev/null
    echo "Volume ${VOLUME_NAME} deleted. Run 'make up' to re-initialise the database."
else
    echo "Volume ${VOLUME_NAME} did not exist; nothing to delete."
fi
