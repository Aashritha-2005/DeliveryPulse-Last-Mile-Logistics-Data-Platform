#!/usr/bin/env bash
# DeliveryPulse database initialisation.
#
# Run by the official postgres image entrypoint ONLY on first initialisation of an empty
# data volume. Later edits to sql/*.sql do not change an existing database; apply them with
# `make reset-db` followed by `make up` (development data only).
#
# Runs roles.sql once (roles are cluster-wide), then schema.sql and grants.sql for the main
# database and for the integration-test database `<POSTGRES_DB>_test`, stopping at the first
# error. The entrypoint sources (rather than executes) non-executable *.sh files, so all work
# happens in a subshell: `set` options and `exit` never leak into the entrypoint, while a
# failure still makes it abort.
(
    set -euo pipefail

    sql_dir="/deliverypulse/sql"
    test_db="${POSTGRES_DB}_test"

    for var in POSTGRES_PASSWORD DELIVERYPULSE_ETL_PASSWORD DELIVERYPULSE_READONLY_PASSWORD; do
        value="${!var:-}"
        case "$value" in
            "" | *CHANGE_ME*)
                echo "deliverypulse-init: ERROR: ${var} is empty or still CHANGE_ME." \
                    "Set it in .env, then run 'make reset-db' and 'make up'." >&2
                exit 1
                ;;
        esac
    done

    run_sql() {
        local database="$1" file="$2"
        echo "deliverypulse-init: running ${file} on ${database}"
        psql --no-psqlrc -v ON_ERROR_STOP=1 -v dbname="$database" \
            --username "$POSTGRES_USER" --dbname "$database" \
            --file "${sql_dir}/${file}"
    }

    run_sql "$POSTGRES_DB" roles.sql

    echo "deliverypulse-init: creating database ${test_db}"
    psql --no-psqlrc -v ON_ERROR_STOP=1 -v test_db="$test_db" \
        --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE DATABASE :"test_db";
SQL

    for database in "$POSTGRES_DB" "$test_db"; do
        run_sql "$database" schema.sql
        run_sql "$database" indexes.sql
        run_sql "$database" grants.sql
    done

    echo "deliverypulse-init: done"
)
