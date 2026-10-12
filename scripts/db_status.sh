#!/usr/bin/env bash
# Print identifying facts about the running DeliveryPulse PostgreSQL container.
#
# system_identifier is assigned when the data directory is initialised and never changes
# afterwards, so an identical value before `make down` and after the next `make up` proves
# the data volume survived. Read-only: nothing is written.
#
# Compatible with macOS /bin/bash 3.2.
set -euo pipefail

project_root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$project_root"

echo "PGDATA=$(docker compose --profile core exec -T postgres printenv PGDATA)"

# Runs as the bootstrap superuser over the container's local socket; no password needed.
docker compose --profile core exec -T postgres \
    sh -c 'psql --no-psqlrc -At -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f -' <<'SQL'
SELECT 'server_version=' || current_setting('server_version');
SELECT 'system_identifier=' || system_identifier FROM pg_control_system();
SELECT 'database_timezone=' || current_setting('TimeZone');
SELECT 'schemas=' || string_agg(nspname, ',' ORDER BY nspname)
FROM pg_namespace
WHERE nspname IN ('raw', 'staging', 'analytics', 'audit');
SQL
