#!/usr/bin/env bash
# Delete Airflow's metadata, logs and generated admin password, after explicit confirmation.
#
# Stops core + airflow (docker-compose.yml plus docker-compose.airflow.yml), then removes its three named volumes. The pipeline database
# (deliverypulse_pgdata) and the generated data under data/ are NOT touched. The next
# `make airflow-up` re-creates Airflow's metadata and a new admin password.
#
# Compatible with macOS /bin/bash 3.2.
set -euo pipefail

readonly VOLUMES="deliverypulse_airflow_db deliverypulse_airflow_logs deliverypulse_airflow_auth"
readonly CONFIRM_WORD="DELETE"

project_root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$project_root"

if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: docker not found on PATH. Start Docker Desktop and retry." >&2
    exit 1
fi

echo "=================================================================="
echo " WARNING: THIS PERMANENTLY DELETES AIRFLOW'S METADATA AND LOGS"
echo "=================================================================="
echo " Docker volumes to delete: ${VOLUMES}"
echo " DAG run history, task logs and the generated admin password will be lost."
echo " The pipeline database and data/ are not touched."
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

echo "Stopping core + airflow (volumes are kept by this step)..."
docker compose -f docker-compose.yml -f docker-compose.airflow.yml --profile core --profile airflow down

for volume in $VOLUMES; do
    if docker volume inspect "$volume" >/dev/null 2>&1; then
        docker volume rm "$volume" >/dev/null
        echo "Volume ${volume} deleted."
    else
        echo "Volume ${volume} did not exist; nothing to delete."
    fi
done
