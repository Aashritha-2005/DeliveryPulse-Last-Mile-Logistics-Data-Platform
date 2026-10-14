# Operations

How to set up, run and troubleshoot DeliveryPulse locally: environment, commands per component,
recovery after a failed batch, the database lifecycle and troubleshooting.

## Verified local environment (recorded 2026-10-11)

The machine every measurement in this repository was taken on.

| Item | Value | Consequence for the code |
|---|---|---|
| Machine | MacBook Pro, Apple Silicon (`arm64`), macOS 26 Tahoe, zsh | Every Docker image used must publish a `linux/arm64` variant |
| Homebrew | 6.0.9, prefix `/opt/homebrew` | Tools live in `/opt/homebrew/bin` |
| uv | 0.13.0 via Homebrew | Setup starts at `uv sync --locked` (the lockfile is committed) |
| Python | 3.12, uv-managed only (3.12.10 at `~/.local/share/uv/python/`) | `requires-python = ">=3.12,<3.13"`; `.python-version` containing `3.12`; `[tool.uv] python-preference = "only-managed"` in `pyproject.toml`. The machine also has Homebrew, python.org, pyenv and Apple Pythons; never use them |
| make | GNU Make 3.81 (Apple Command Line Tools) | The Makefile must run on 3.81: no `.ONESHELL`, `.SHELLFLAGS`, `undefine`, `$(file ...)`, `!=`, or `--output-sync`. Each recipe line runs in its own shell; put multi-step logic in `scripts/*.sh` or join lines with `&& \`. The same Makefile must also work on GNU Make 4.x (CI on Linux) |
| Secret scanner | gitleaks 8.30.1 via Homebrew; not a Python dependency | See "Secret scan design" below |
| Docker | Docker Desktop: Engine 29.8.2, Compose v5.5.1; VM architecture `aarch64`, ~7.7 GiB allocated (verified 2026-10-11) | Images must publish `linux/arm64`. Use `docker compose` (v2+ syntax); no top-level `version:` key in compose files. Never run more than one heavy profile at a time |
| Host RAM | 16.0 GiB (Docker VM gets ~7.7 GiB, the default half) | Design for an 8 GB laptop anyway: keep the combined memory of running services within ~4 GB per profile; document approximate RAM per Compose profile |

**Command rule:** every Python command goes through `uv run` (for example `uv run pytest`,
`uv run ruff check .`). Never use bare `python`, `python3`, `pip` or `pytest`: pyenv shims on
this machine intercept bare `python`, and `.python-version` makes them error.

## First-time setup

Run from the project root (`cd ~/DeliveryPulse`):

```bash
uv lock                 # resolve dependencies into uv.lock (generated, never hand-written)
uv sync --locked        # install exactly what uv.lock specifies into .venv/
cp .env.example .env    # local settings; .env is git-ignored
uv run python -c "import sys; print(sys.executable, sys.version)"   # must show .venv/ and 3.12.10
```

Then replace every `CHANGE_ME` in `.env` with real local values. The core profile needs
`POSTGRES_SUPERUSER_PASSWORD`, `DELIVERYPULSE_ETL_PASSWORD` and
`DELIVERYPULSE_READONLY_PASSWORD` **before the first `make up`**. The Airflow profile also needs
`AIRFLOW_DB_PASSWORD`, `AIRFLOW_FERNET_KEY`, `AIRFLOW_JWT_SECRET` and a numeric `AIRFLOW_UID`
before `make airflow-build`, `make airflow-up` or `make airflow-dag-check`. The object store
(`s3` profile) needs the four `S3_` keys before `make s3-up` (see "Object storage lake").

## Everyday commands

| Command | What it does |
|---|---|
| `make lint` | `ruff check`, `ruff format --check`, `mypy src tests` (strict) |
| `make format` | Applies ruff fixes and ruff formatting |
| `make test` | Unit tests only (`pytest -m unit`); needs no services and no `.env` |
| `make test-integration` | Integration tests (`pytest -m integration`); needs `make up` first. Fails (never skips) if PostgreSQL is unreachable |
| `make coverage` | Unit-only coverage of `src/`, then combined unit + integration coverage with missing lines; fails below `COVERAGE_MIN` (default 80 %). Needs `make up` |
| `make scan` | `gitleaks dir .` with `.gitleaks.toml`; fails (non-zero) on any finding |
| `make scan-selftest` | Proves gitleaks detects a synthetic secret in a scratch dir outside the project |
| `make up` | Starts the `core` profile and waits (up to 120 s) until PostgreSQL is healthy |
| `make down` | Stops and removes the containers. **Keeps** the `deliverypulse_pgdata` volume |
| `make db-status` | Prints `PGDATA`, server version, `system_identifier`, timezone and project schemas |
| `make reset-db` | **Deletes all database data** after you type `DELETE` (see below) |

Every test carries exactly one marker, `unit` or `integration` (collection stops otherwise), and
unit tests cannot open a database connection (they fail with "unit tests must not connect to a
database"). So a failure in `make test` is always a unit failure, never a missing service.

## Synthetic data

Needs no Docker and no `.env`. The contract is in `docs/data_contracts.md`.

| Command | What it does |
|---|---|
| `make generate` | Generates `data/raw/<dataset>/batch-{1,2,3}/` and `data/ground_truth/<dataset>/defect_manifest.json` (defaults `ORDERS=50000 SEED=42`) |
| `make generate ORDERS=5000 SEED=1 DRIVERS=80` | Another size or seed; `DRIVERS` defaults to `max(20, ORDERS // 60)` |
| `make generate-check` | Generates twice into a scratch directory outside the project and compares the sha256 of every file |
| `make profile` | Verifies the manifest against the raw files, then writes `docs/data_quality_report.md` |
| `make clean-dataset DATASET=<name>` | **Deletes** one obsolete dataset after you type its name |

- **Immutability:** a dataset folder is never overwritten. Re-running with the same settings
  logs `unchanged` and exits 0. If the folder exists with different contents (for example after
  a generator code change without a `generator_version` bump), the command fails.
- **Removing an obsolete dataset:** `make clean-dataset DATASET=<name>` deletes exactly
  `data/raw/<name>` and `data/ground_truth/<name>`.
  - It refuses empty names, names containing `/` or `..`, symlinks, and paths outside those two
    folders.
  - It prints both paths and requires you to type the dataset name to confirm.
  - It restores write permission, then deletes.
- **Interrupted runs:** a hidden `.generate-tmp-*` folder left by a crash is removed
  automatically on the next run, with a log line. Any other hidden folder in `data/raw/` or
  `data/ground_truth/` makes the generator stop with an error. Hidden files such as `.gitkeep`
  are ignored.
- **Determinism:** byte-identical output is guaranteed for the same seed, settings, `uv.lock` and
  Python version. NumPy does not promise identical random streams across versions.
- **Concurrency:** do not run two generators into the same folders at the same time.

## Transform

Needs no Docker and no `.env`. The rules are in `docs/data_contracts.md`, sections 6 and 7.

| Command | What it does |
|---|---|
| `make transform` | Processes every batch of the dataset in sequence, from fresh local state, into `data/processed/<dataset>/` |
| `make reconcile` | Compares the transform's dispositions with the defect manifest; exits 1 on any mismatch |
| `make transform DATASET=<name>` | Picks the dataset when `data/raw/` holds more than one |

Output layout (`data/processed/<dataset>/`):
- `batches/batch-<k>/`:
  - `dispositions.parquet` (one row per input row)
  - `rejected_records.parquet` and `pending_events.parquet` (shaped like the staging tables;
    `make load` writes the same records to PostgreSQL directly)
  - `summary.json` (counts)
- `state/`: the local pipeline state after the last batch
- `cleaned/`: the cleaned Parquet layer (a derived, normalised representation, not the source)

Behaviour:
- **Raw input is only read.** It is never modified.
- **Re-running replaces the whole output.** The output is built in a hidden
  `.transform-tmp-*` folder and swapped into place, so there are no appended rows or leftover
  files. Leftover `.transform-tmp-*` / `.transform-old-*` folders from an interrupted run are
  removed automatically.
- **A schema error stops everything.** A missing required column fails the batch with a clear
  error, nothing is written, and the command exits 1.
- **Per-batch logs** show, for each source: rows in, every disposition count and the duration.
  They also show pending records resolved, expired and still pending.
- **Thresholds** can be changed with
  `uv run python -m src.transformations.transform --max-speed-kmh 120 --orphan-max-age-batches 2`.

## Database load

Needs `make up`. Connects as `deliverypulse_etl` only, never as the superuser.

| Command | What it does |
|---|---|
| `make load` | Loads every batch of the dataset into PostgreSQL, strictly in sequence. Already-loaded batches are skipped (logged no-op) |
| `make reconcile-db` | Compares the dispositions stored in `audit.record_dispositions` / `audit.pending_resolutions` with the defect manifest; exits 1 on any mismatch |
| `make db-counts` | Row count of every table in `raw`, `staging`, `analytics`, `audit`, plus the database size |

`make transform` is the offline path with local state and Parquet
output. `make load` is the database path. Both run the same `process_batch`, and an integration
test proves they give identical dispositions.

### Transactions per batch
All three run on one connection, as nested `conn.transaction()` blocks:

| Block | Commits | Contents |
|---|---|---|
| A | on its own, before the batch | Leftover `started` audit rows of the batch become `failed` with `failure_reason = 'abandoned'`; four new `started` rows (one per source) with `attempt = previous + 1` |
| B | all or nothing | Raw rows, `audit.record_dispositions`, `staging.rejected_records`, the pending queue (`staging.pending_events` replaced), `audit.pending_resolutions`, analytics upserts, pipeline state (`staging.record_state`, `staging.gps_reference`), the refresh of dimensions and aggregates for the affected reporting dates, the quality checks, and the four audit rows set to `succeeded` |
| C | on its own, only if B failed | The four audit rows set to `failed` with `failure_reason`, `rows_failed = rows_input` and every other counter 0 (nothing of B was committed) |

- **Bulk writes use COPY.** Analytics upserts go through temporary tables:
  - drivers, delivery events and shift events: `ON CONFLICT DO NOTHING` (immutable)
  - orders: `ON CONFLICT DO UPDATE ... WHERE stored.source_updated_at < incoming.source_updated_at`.
    The database itself never lets an older version overwrite a newer one.
- **Idempotency:**
  - A batch whose `batch_id` already has `succeeded` audit rows is skipped before anything is
    written.
  - A unique partial index allows only one `succeeded` row per `(batch_id, source)`.
  - A session advisory lock lets only one load run at a time.
- **Recovery after a failed batch:** just run `make load` again. The failed batch is retried
  as the next attempt; earlier batches are skipped.

### Audit design (`audit.pipeline_runs`)
- One row per batch, source and attempt.
- **Status:** `started` (A), then `succeeded` (B) or `failed` (C). `failed` with
  `failure_reason = 'abandoned'` means the process died between A and the end of B; the next
  run marks it.
- **Counters:** `rows_input = rows_accepted + rows_rejected + rows_duplicate +
  rows_stale_ignored + rows_pending` holds for every `succeeded` row.
- **Database effects:** `rows_inserted`, `rows_updated` and `rows_unchanged` describe what
  happened in the database. They include pending records resolved in that batch.
- **Pending queue:** `pending_resolved`, `pending_remaining` and `pending_expired` track the
  queue.
- **Per row:** `audit.record_dispositions` keeps every input row's disposition, and
  `audit.pending_resolutions` records how each pending record left the queue.

### Switching datasets
The database holds one dataset. `make load` refuses to load a different dataset name into a
database that already has succeeded batches of another one. To switch, run `make reset-db`
alone (it asks you to type `DELETE`; development data only), then `make up` and `make load`.

### Test database and test isolation
- The init script creates a second database, `<POSTGRES_DB>_test`, with the same schema and
  grants. It is created only on first initialisation of the data volume.
- Load integration tests connect to it as `deliverypulse_etl` and run inside an outer
  transaction that is always rolled back. The load's blocks A, B and C become savepoints, so
  nothing is ever committed there.
- A dedicated test loads the full fixture inside such a transaction, rolls back, and asserts
  that every table is empty again.
- The schema and role tests keep using the main database.

## Analytics and query tuning

Needs `make up` and a loaded database. Definitions: `docs/data_model.md`.

| Command | Role | What it does |
|---|---|---|
| `make metrics` | `deliverypulse_etl` | Full rebuild of `dim_date`, `dim_city`, `daily_delivery_metrics` and `driver_performance` for every date, in one transaction. Normally not needed: `make load` refreshes the affected dates of each batch. Use it to verify that the incremental result equals a rebuild |
| `make analytics` | `deliverypulse_readonly` | Runs every query in `sql/analytics.sql`, printing the parameters used and the first rows |
| `make explain LABEL=<label>` | `deliverypulse_readonly` | `EXPLAIN (ANALYZE, BUFFERS)` of the benchmarked queries; writes `docs/benchmarks/explain-<label>.{json,txt}` |
| `make indexes-drop` | bootstrap superuser, `SET ROLE deliverypulse_owner` | Drops the five analytics indexes, then `ANALYZE` (benchmark "before" state) |
| `make indexes-create` | bootstrap superuser, `SET ROLE deliverypulse_owner` | Creates them (`sql/indexes.sql`, idempotent), then `ANALYZE` |

- **No arguments needed.** `make analytics` and `make explain` derive their date ranges from the
  loaded data: the full period (first to last reporting date of `order_created_ts`), the last 7
  days, or the last day, depending on the query. The exact parameters are printed and stored in
  the explain output.
- **Index targets are admin operations.** They run `psql` inside the container as the bootstrap
  superuser over the container's local socket (the user name comes from the container
  environment; no password is passed or printed) and switch to the
  owner role, because no application role may run DDL. The indexes are also created at first
  initialisation, so a normal setup already has them.
- **A database created with an older schema** (without the aggregate count columns) needs the
  reset procedure once: `make reset-db` (type `DELETE`), `make up`, `make load`.
- **Benchmark protocol:** see `docs/benchmarks.md`.

## Data quality

Needs `make up`. Check catalogue and rules: `docs/data_contracts.md` section 11.

| Command | Role | What it does |
|---|---|---|
| `make load` | `deliverypulse_etl` | Runs every check per batch inside transaction B (the quality gate). Logs one line per batch: `batch N quality: <checks> checks, <B> blocking failures, <W> warnings, <s> s`, plus the slowest checks |
| `make quality` | `deliverypulse_etl` | Runs every check over the whole database: all succeeded batches and all dates. Stores a `manual` run, prints a table, writes `docs/quality/quality-latest.json`, exits 1 on a blocking failure |
| `make quality-report` | `deliverypulse_etl` (ground-truth side) | Compares the load-time results with the defect manifest and rewrites the "Quality checks" section of `docs/data_quality_report.md`. Exits 1 if a defect type is undetected or a difference is unexplained |
| `make load QUALITY_ARGS="--threshold raw_duplicate_rows=0.05"` | | Threshold overrides (repeatable; also for `make quality`) |

**Recovery after a quality-gate failure:**
1. Read the failure: `audit.pipeline_runs.failure_reason`
   (`quality gate: <checks>`) and the stored run in `audit.quality_results`, which has sample
   keys. `make load` also logs every failing check with its sample.
2. Nothing of the blocked batch was committed. Fix the cause: the data, or a threshold if the
   tolerance was wrong.
3. Run `make load` again. The batch is retried as the next attempt.

**Why the checks analyze their tables first.** In a load the checks read the batch's uncommitted
rows. Without fresh statistics PostgreSQL estimates them as about one row and picks a nested-loop
plan that becomes quadratic (measured: `raw_parent_after_children` 35 s instead of 0.4 s). The
suite therefore calls `staging.analyze_quality_tables()` before the checks. `ANALYZE` in the same
transaction counts the uncommitted rows, so the planner's estimate is right.

- Only a table's owner (or a role with `MAINTAIN`) may run `ANALYZE`, and the ETL role is
  deliberately neither. The function is therefore owned by `deliverypulse_owner`, declared
  `SECURITY DEFINER` with `SET search_path = pg_catalog`, and analyzes a **fixed list** of the 14
  tables the checks read. `EXECUTE` is revoked from `PUBLIC` and granted to `deliverypulse_etl`
  alone, so the ETL role gains nothing else: `tests/integration/test_roles.py` asserts it holds
  no `MAINTAIN` on any table and may execute no other function in the project schemas.
- It replaced an earlier `SET LOCAL enable_nestloop = off`. **It did not make the load faster:**
  per-batch quality time went from 16.50 / 21.31 / 22.43 s to 17.81 / 21.01 / 27.18 s, and the
  `ANALYZE` itself costs about 1.9 s per batch on the full dataset. The gain is correctness: the
  planner now works from real statistics instead of being forbidden one join type.
- `tests/integration/test_planner_statistics.py` measures the effect: the estimate for 2,000
  uncommitted rows is 1 before the call and 2,000 after.

**A database created before the quality framework** (without the two quality audit tables and
the two `pipeline_runs` window columns) needs the reset procedure once: `make reset-db` (type `DELETE`), `make up`, `make load`.

## Dashboard

Needs `make up` and a loaded database.

| Command | Role | What it does |
|---|---|---|
| `make dashboard` | `deliverypulse_readonly` | Starts the Streamlit dashboard on `http://127.0.0.1:8501` (Ctrl+C stops it). Change the port with `DASHBOARD_PORT=8502` |
| `make dashboard-check` | `deliverypulse_readonly` | Runs every dashboard query once with the default filters, without a browser; prints rows and milliseconds per query; exits 1 on any error |

- **Queries.** `sql/dashboard.sql`, reading the `analytics` schema only, with every value bound
  as a parameter. Code: `src/dashboard/data.py` (data access, rates from counts) and
  `src/dashboard/app.py` (layout).
- **Server flags** (in the Makefile):
  - localhost only (`--server.address 127.0.0.1`);
  - usage statistics off (`--browser.gatherUsageStats false`);
  - no source-file watching (`--server.fileWatcherType none`);
  - minimal toolbar (`--client.toolbarMode minimal`).

  Streamlit's XSRF and CORS protections stay at their defaults.
- **Errors never show connection details:** "Cannot reach the database" means PostgreSQL is not
  running (`make up`); "No data loaded yet" means run `make generate` and `make load`.
- **Caching.** Query results are cached for 60 s per filter combination
  (`st.cache_data`), so a reload within a minute does not query the database again.
- **pandas** is installed only as a Streamlit dependency; the pipeline does not use it.

## Airflow orchestration

Needs the `core` and `airflow` Compose profiles. Airflow is kept in
`docker-compose.airflow.yml`, layered on top of `docker-compose.yml`, so the core pipeline still
runs and tests without any `AIRFLOW_` variables.

**Two-file layout.** This is how the "`airflow` Compose profile" deliverable is implemented:
every Airflow service carries `profiles: ["airflow"]`, but the services live in their own file.
- Compose interpolates every variable of every file it loads, even for services of inactive
  profiles. With Airflow in `docker-compose.yml`, plain `make up` would fail without the
  `AIRFLOW_` variables.
- The `make airflow-*` targets run
  `docker compose -f docker-compose.yml -f docker-compose.airflow.yml --profile core --profile airflow`.
  The core targets load `docker-compose.yml` only.
- `docker-compose.airflow.yml` requires all four variables with `${VAR:?}`, and
  `scripts/airflow_env_check.sh` additionally rejects the `CHANGE_ME` placeholders before
  `make airflow-build` and `make airflow-up`, without printing any value.
- Proven twice: a unit test checks that `docker-compose.yml` contains no `AIRFLOW_` text, and the
  CI integration job runs `make up` with a `.env` from which every `AIRFLOW_` line was removed.

| Container | `mem_limit` | Role |
|---|---|---|
| `airflow-db` | 512 MiB | Airflow's own metadata database (PostgreSQL, no published port) |
| `airflow-init` | 1 GiB | One-shot `airflow db migrate`, then exits |
| `airflow-api-server` | 1 GiB | UI and API on `127.0.0.1:8080` |
| `airflow-scheduler` | 2 GiB | Scheduler; with LocalExecutor it also runs the tasks, so its limit covers the largest task (`clean`, about 1 GiB) |
| `airflow-dag-processor` | 768 MiB | Parses the DAG files |
| `airflow-dag-check` | 1 GiB | One-shot parse check (`make airflow-dag-check`), no network |

**Image size** (`make airflow-image-info`, 2026-10-14): `deliverypulse-airflow:3.3.2` is
3,821,003,600 bytes, of which the official base image is 3,252,573,702 bytes; the project venv
adds 381M. Streamlit and its dependencies (about 130 MB: pandas, streamlit, pydeck, pillow,
altair) are installed too, although no DAG task uses them. They are kept, because moving them to
a separate dependency group would change the dependency layout for a small share of an image that
is about 85 % base image.

**Runtime files.** Task logs (including the quality gate's JSON output,
`/opt/airflow/logs/deliverypulse/quality-latest.json`) live in the named volume
`deliverypulse_airflow_logs`; the generated admin password in `deliverypulse_airflow_auth`;
Airflow's metadata in `deliverypulse_airflow_db`. The only project path the containers write is
`./data` (git-ignored); `dags/`, `src/` and `sql/` are mounted read-only.

| Command | What it does |
|---|---|
| `make airflow-dag-check` | Parses and verifies `delivery_pipeline` inside the pinned Airflow image, without a metadata database or network |
| `make airflow-build` | Builds `deliverypulse-airflow:3.3.2` from the pinned Airflow base image plus the project's locked runtime dependencies |
| `make airflow-up` | Starts PostgreSQL plus Airflow on `http://127.0.0.1:8080` and waits until all services are healthy |
| `make airflow-password` | Prints the generated local admin password from the Airflow auth volume |
| `make airflow-runs` | Lists `delivery_pipeline` runs with state, start and end |
| `make airflow-stats` | Prints one-shot memory and CPU stats for the running DeliveryPulse containers |
| `make airflow-image-info` | Prints the Airflow image size and the largest packages in the project venv inside the image |
| `make airflow-down` | Stops core + Airflow containers and keeps every volume |
| `make airflow-reset` | Deletes only Airflow metadata, logs and generated admin password after you type `DELETE`; it does not delete `deliverypulse_pgdata` or `data/` |

### Startup and UI access

1. Put real local values in `.env` for all `AIRFLOW_` variables. Generate the Fernet key as
   32 URL-safe base64 bytes, and use a long random value for `AIRFLOW_JWT_SECRET`.
2. Run `make airflow-build`.
3. Run `make airflow-up`.
4. Run `make airflow-password` in your terminal. The username is `admin`.
5. Open `http://127.0.0.1:8080` and sign in. The UI is localhost-only.

The DAG is paused at creation (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True`). Example DAGs
are disabled. Runtime task logs and the generated admin password live in named Docker volumes,
not in the project directory.

### Triggering and checking a DAG run

The DAG id is `delivery_pipeline`. It has five tasks:
`generate -> clean -> load -> quality_gate -> refresh_aggregates`.

The schedule is one weekly run per generated batch window. `catchup=True`, `max_active_runs=1`
and `load.depends_on_past=True` make backfills run oldest first. Run 1 loads through batch 1,
run 2 through batch 2 and run 3 through batch 3. Clearing or rerunning a date is expected to be
idempotent: generation is a no-op for an unchanged dataset, cleaning rebuilds derived output,
the loader skips already-succeeded batch ids, the quality gate records a read-only manual check,
and aggregate refresh recomputes deterministically.

To demonstrate the normal path through the UI:
1. In Airflow, unpause `delivery_pipeline`.
2. Let the three scheduled catchup runs complete, or trigger the DAG from the UI.
3. Confirm each task is green for the run you are checking.
4. Run `make airflow-runs` to capture the run table for the project notes.

To demonstrate a quality-gate block:
1. Start from a database that has successfully loaded data.
2. Run `make quality-demo-plant` to insert one synthetic bad order.
3. In the Airflow UI, clear and rerun a date whose `load` task will be skipped as already loaded,
   so `quality_gate` runs against the planted bad row.
4. Confirm `quality_gate` fails and `refresh_aggregates` is not run.
5. Run `make quality-demo-remove` to remove the synthetic bad row, then rerun the date if you
   want the cleared DAG run to return to green.

To demonstrate rerun idempotency:
1. Capture business-table counts or digests before the rerun. `make db-counts` is a quick count
   check; the integration suite has stronger digest tests.
2. In the Airflow UI, clear a completed date and rerun it.
3. Confirm the rerun succeeds and `make db-counts` shows no duplicate business rows.

### Cleanup and reset boundaries

Use `make airflow-down` to stop services while keeping data. Use `make airflow-reset` only when
you want to delete Airflow run history, task logs and the generated admin password. It does not
reset the pipeline database. Database resets are still done only with `make reset-db`, which
prints a data-loss warning and requires typed confirmation.

## Object storage lake

Needs the `s3` Compose profile, plus `make up` and `make transform` for the export. The store
is [SeaweedFS](https://github.com/seaweedfs/seaweedfs) 4.48, an S3-compatible server, pinned by
digest. The lake is written and read only through the S3 API (boto3), so pointing it at AWS S3
is a change of `.env` settings, which must not be done without explicit approval.

**Deviation from the plan's "minio profile":** MinIO's free container images were discontinued
in 2026 (Docker Hub repositories removed, `quay.io/minio` answering 401), so the profile is named
after its role, `s3`, and the store is SeaweedFS.

| Command | What it does |
|---|---|
| `make s3-up` | Checks the `S3_` variables, starts the store (S3 API on `127.0.0.1:${S3_API_PORT}`), waits until healthy, then creates the bucket with the admin key (`bucket deliverypulse: created` or `already exists`) |
| `make lake-export` | Builds the three layers and uploads them with the pipeline key; then deletes stale keys under the layer prefixes. Prints objects, rows, bytes and `stale_deleted` |
| `make lake-check` | Reads every lake object back through S3 and reconciles it with its source; exits 1 on any difference |
| `make test-s3` | Tests against the live store (marker `s3`): export, check and replay round trip; the pipeline key cannot create buckets or write to another bucket |
| `make s3-stats` | Memory and CPU of the store container |
| `make s3-logs` | The last log lines of the store containers |
| `make s3-down` | Stops the store; keeps its volumes (`deliverypulse_s3_data`, `deliverypulse_s3_config`) |

**Two-file layout.** As for Airflow, the store lives in `docker-compose.s3.yml`, layered on
`docker-compose.yml` (`docker compose -f docker-compose.yml -f docker-compose.s3.yml --profile s3`).
`docker-compose.yml` contains no `S3_` variable, so `make up` runs with a `.env` that has none (a
unit test and the CI integration job prove it).

**Keys and least privilege.**
- `.env` holds two keys, each 16 to 128 letters and digits: `S3_ADMIN_ACCESS_KEY` /
  `S3_ADMIN_SECRET_KEY` and the pipeline's `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY`.
  `scripts/s3_env_check.sh` rejects placeholders, other characters and identical keys, without
  printing values.
- The one-shot `s3-config` service writes SeaweedFS's identity file from those variables into the
  named volume `deliverypulse_s3_config` (mode 600, owned by the store's user). The keys never
  enter an image or a project file.
- The admin identity (`Admin`) is used only by `make s3-up` to create the bucket. The pipeline
  identity has `Read`, `List` and `Write` on the lake bucket only. `make test-s3` proves that it
  gets `AccessDenied` for creating a bucket and for writing to another bucket.
- The export and the check read the aggregates as `deliverypulse_readonly`.

**Layout** (Hive-style; one prefix per table, so each table reads as one dataset). This is a
documented deviation from the plan's example `s3://deliverypulse/raw/date=YYYY-MM-DD/...`: the
table comes before the date partition (`raw/<table>/date=...`), so a query engine can read each
table, with its own schema, as one partitioned dataset.

```text
s3://deliverypulse/raw/<source>/date=<ingestion date>/batch-<NN>.parquet
s3://deliverypulse/cleaned/drivers/part-0.parquet
s3://deliverypulse/cleaned/<orders|delivery_events|shift_events>/date=<reporting date>/part-0.parquet
s3://deliverypulse/aggregated/<daily_delivery_metrics|driver_performance>/date=<metric_date>/part-0.parquet
```

- **raw**: one object per batch and source, partitioned by **batch ingestion date**: the
  reporting date (Asia/Kolkata) of the batch cutoff, the window end recorded in `batch.json`
  (2026-09-17, 2026-09-24 and 2026-10-01 for the default dataset). It comes from the data, not
  the clock, so a re-export writes the same keys. Each row holds the batch metadata, the source
  row number, the parse-error flag, the original payload and the contract fields, all as text:
  a normalised representation of the immutable raw files, not a copy of them.
- **cleaned**: the `make transform` output uploaded byte for byte: orders and events by IST
  reporting date, drivers unpartitioned.
- **aggregated**: `analytics.daily_delivery_metrics` and `analytics.driver_performance`, one
  object per `metric_date`.

**Export, idempotency and failure.**
- Every object is uploaded first. Only then are the keys under `raw/`, `cleaned/` and
  `aggregated/` that the new export does not contain deleted, so a failure never leaves the lake
  half empty. Keys outside these prefixes are never touched.
- `DeleteObjects` reports per-key failures in its answer; any failure stops the export with an
  error naming the first key.
- Parquet bytes are deterministic for one `uv.lock`, so a replay writes the same keys and bytes
  (`stale_deleted=0`).
- S3 errors print one line, `ERROR: S3 request failed: <operation and code>`, and exit 1. No key
  is ever printed.
- The bucket is created by `make s3-up`, with `LocationConstraint` set when `S3_REGION` is not
  `us-east-1` (needed on AWS).

**What "the pipeline reads and writes through the S3 API" means here** (approved 2026-10-14):
`make lake-export` writes every layer through S3, and `make lake-check` reads every object back
through S3 and reconciles it with its source. Keys: no missing and no extra object. Rows: the raw
files, the cleaned Parquet and the database aggregates. Bytes: identical. No pipeline stage
consumes the lake yet.

**Run order** (from a loaded database):

```bash
make up
make s3-up
make transform
make lake-export
make lake-check
make s3-down
```

**Measured 2026-10-14** (default dataset, 50,000 orders):

| Step | Result |
|---|---|
| `make s3-up` (first start) | store healthy and bucket created in 7.62 s |
| `make lake-export` | 118 objects, 1,093,033 rows, 57,501,391 bytes in 9.02 s; max RSS 923,090,944 bytes |
| `make lake-check` | raw 12 objects / 541,536 rows, cleaned 64 / 539,047, aggregated 42 / 12,450; all match; 7.99 s |
| Replay (`make lake-export` again) | same 118 objects, `stale_deleted=0`; `make lake-check` passes |
| Store memory (`make s3-stats`) | With the first limit of 512 MiB: 88.14 MiB after start, 403.3 MiB (79 %) after two exports and checks. The limit was then raised to 768 MiB: 151.7 MiB after a restart, 355.9 MiB (46 %) after `make test-s3` and `make lake-check` |

**Known log line.** On start, SeaweedFS logs `Failed to load IAM configuration: no signing key
found for STS service`. It concerns its optional STS (temporary credentials) service. The
identity file is loaded: the admin key creates the bucket, and the live tests prove the pipeline
key's limits.

## Kafka streaming setup

The single-broker overlay is `docker-compose.kafka.yml`; the core profile needs no Kafka
settings. PLAINTEXT is an explicit local-demo choice: the host listener binds only to
`127.0.0.1`. There is no authentication or transport encryption; use synthetic local data.
Internal listeners are available to containers on the Compose network.

Add these non-secret settings to the existing local `.env`; do not replace its database or
other settings. The cluster ID is a public identifier, not a credential. Keep it fixed while
the Kafka data volume exists. If port 9092 is occupied, change both port settings together.

```dotenv
KAFKA_CLUSTER_ID=CHANGE_ME
KAFKA_PORT=9092
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_TOPIC_ORDERS=deliverypulse.orders.v1
KAFKA_TOPIC_PINGS=deliverypulse.driver_pings.v1
KAFKA_TOPIC_DLQ=deliverypulse.events.dlq.v1
```

Generate a new public cluster ID with the pinned image, then replace `CHANGE_ME` in your
local `.env` with the output before starting Kafka:

```bash
docker run --rm apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837 /opt/kafka/bin/kafka-storage.sh random-uuid
```

From the project root, with other heavy profiles stopped:

```bash
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
make kafka-env-check
make kafka-up
```

Startup waits for broker health and then runs the explicit three-topic initializer. Each
topic has one partition, replication factor one, one-hour retention and 256 MiB retention
per partition. Retention deletes closed segments asynchronously, so this is not a hard disk
cap. Automatic topic creation is disabled. Existing topics are retained by the initializer;
it does not alter their configuration. The pinned image is the user-confirmed multi-platform
index for `apache/kafka:4.3.1`.

On failure, run `make kafka-logs` and report the error. Do not remove volumes or change the
cluster ID to work around a startup error. `make kafka-down` stops only the streaming
services and retains volumes and PostgreSQL. Broker memory limit: 1 GiB; initializer limit:
256 MiB. These are configured limits, not measured usage. Measured throughput and latency
are recorded in the Kafka streaming section of `docs/benchmarks.md`.

Install the optional locked client with `uv sync --locked --group streaming`. A default
`uv sync --locked` excludes it. For development lint including Spark, install both groups:
`uv sync --locked --group bench --group streaming`.

The bounded demo starts its consumer before sending messages. Run these sequentially, with
no other producer/consumer using the demo topics and no other heavy profile running:

```bash
make test-kafka
make kafka-demo COUNT=500 RATE=500 TRIAL=0 MALFORMED=5
make kafka-demo COUNT=100000 RATE=500 TRIAL=1 MALFORMED=0
make kafka-demo COUNT=100000 RATE=2000 TRIAL=2 MALFORMED=0
make kafka-demo COUNT=100000 RATE=0 TRIAL=3 MALFORMED=0
make kafka-stats
```

Each command reports its initial pass and a separate replay. `RATE=0` is unthrottled.
For separate processes, `make kafka-produce COUNT=500 RATE=500` publishes a repeatable slice;
running it again republishes the same identities. `make kafka-consume COUNT=500
KAFKA_GROUP=deliverypulse-local-v1` consumes a bounded count and resumes that group's stored
offsets. A new group begins at the earliest retained input, which may include older demo
messages; use `make kafka-demo` for an isolated current-offset measurement window.
The correctness slice is 250 orders and 250 pings. Each larger trial selects a distinct
slice of 10,000 order versions and 90,000 pings. Selection skips invalid and repeated
identities deterministically; raw source files remain unchanged. Repeating a completed trial
will correctly report duplicates on its initial pass too. It does not delete landing data.

The single consumer assigns partition zero of the two input topics directly; it is not a
multi-consumer load-balanced service. Demo groups have unique IDs and start at snapshotted
end offsets. The consumer API also supports restarting a persistent group from committed
offsets. Kafka retention can expire unconsumed records; this demo is not a durable archive.
Timeouts, failed database writes, failed DLQ delivery or failed offset commits fail the run.
After a failure, rerun the same source slice; committed inserts deduplicate. For a persistent
group restart, keep its group ID and omit explicit start offsets.

Latency is host wall-clock send time to return from the committed database batch, including
queueing and batching (500 rows or 250 ms). It covers inserts and duplicate checks, excludes
dead letters, and is not a cross-host clock-synchronization guarantee. Throughput includes
consumer initialization, processing, and offset commits, but excludes source selection.
The JSON's `host_process_peak_rss_mib`, when present, is the operating system's high-water
RSS for the combined producer/consumer demo process, including source selection. It is not
consumer-only memory. Earlier runs without this field have no high-water RSS measurement;
an instantaneous RSS sample must not be described as a peak.
The demo retains its selected payloads and at most 100,005 latency values; it is bounded,
not an indefinitely running service. Kafka queues are limited to 16 MiB per client.
DLQ delivery is at-least-once: a crash before offset acknowledgment can repeat a DLQ entry.
Its key includes cluster/topic/partition/offset for downstream deduplication.

Live tests use the separate test database and leave a few uniquely named immutable landing
rows per run; they do not grant DELETE privileges or clear production data.

### Kafka volumes and deliberate reset

The image declares three writable mount points. Named volumes keep these explicit and
reusable instead of leaving anonymous volumes behind:

| Volume | Mount | Contents and purpose |
|---|---|---|
| `deliverypulse_kafka_data` | `/var/lib/kafka/data` | Partition logs (including DLQ records and consumer offsets), KRaft metadata and the stored cluster identity; preserves broker data across restarts. |
| `deliverypulse_kafka_config` | `/mnt/shared/config` | Shared/generated configuration space provided by the upstream image; keeps this writable configuration mount under an explicit volume name. |
| `deliverypulse_kafka_secrets` | `/etc/kafka/secrets` | Mount reserved by the image for optional TLS/SASL assets. This PLAINTEXT demo does not populate it with credentials; naming it avoids an anonymous volume. |

**Data-loss warning:** the following user-run commands permanently delete all Kafka topics,
messages, dead letters, consumer offsets and cluster metadata, plus any files in the
configuration and secrets volumes. Use them only for an intentional Kafka reset. They leave
PostgreSQL and its landing rows intact; replayed business identities still deduplicate there.

Run these yourself from the project root, after stopping any producer or consumer:

```bash
make kafka-down
docker compose -f docker-compose.yml -f docker-compose.kafka.yml --profile streaming rm -f kafka kafka-topics
docker volume rm deliverypulse_kafka_data deliverypulse_kafka_config deliverypulse_kafka_secrets
```

The cluster ID may change **only after deleting the data volume**. To create a new cluster,
generate an ID with the command above, update `KAFKA_CLUSTER_ID` in your local `.env`, then run
`make kafka-up`. Ordinary stops need only `make kafka-down`; keep the ID and volumes intact.

## MongoDB raw event store setup

MongoDB Community (SSPL) uses the optional `nosql` overlay in `docker-compose.mongo.yml`.
The core profile requires no MongoDB settings. Bootstrap and healthy startup were verified
on 2026-10-15; load/replay and query measurements are recorded in `docs/benchmarks.md`.
The user adds these settings to their existing local `.env`, replacing each placeholder:

```dotenv
MONGO_PORT=27017
MONGO_DB=deliverypulse
MONGO_ROOT_USER=CHANGE_ME
MONGO_ROOT_PASSWORD=CHANGE_ME
MONGO_APP_USER=CHANGE_ME
MONGO_APP_PASSWORD=CHANGE_ME
MONGO_READONLY_USER=CHANGE_ME
MONGO_READONLY_PASSWORD=CHANGE_ME
```

Choose three distinct usernames (3–40 lowercase letters, digits or underscores, beginning
with a letter) and three distinct passwords (20–128 letters or digits). Keep them private.
The application user can read and insert raw documents only. The query user can read and
inspect collection statistics/indexes; neither can administer users or drop data. Bootstrap
creates corresponding isolated users and collections in `deliverypulse_test` for live tests.

With other heavy profiles stopped, the user runs from the project root:

```bash
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
make mongo-env-check
make mongo-up
```

`mongo-up` stops any running MongoDB server before the offline bootstrap opens its volume,
then starts the server and waits for health. Expect a short interruption on repeated startup.
Bootstrap runs with no network access and administrator credentials; the long-running server
receives no credentials. Both run as the pinned image's `mongod` user. Authentication is
enabled and the published port binds only to 127.0.0.1. Transport is unencrypted for this local
synthetic-data demo. The healthcheck proves responsiveness; live role tests must prove access
control. Failed bootstrap exits nonzero without printing credential-bearing server errors.

`deliverypulse_mongo_data` stores the database, indexes, journal and users under `/data/db`.
`deliverypulse_mongo_config` names the image's declared `/data/configdb` mount explicitly to
avoid anonymous volumes; this standalone deployment does not run a sharded config server.
Both persist across `make mongo-down`. Each container has a 768 MiB limit and a 0.25 GiB
WiredTiger cache; bootstrap and the main server must never open the volume simultaneously.
These are configured limits, not measured usage. Bootstrap also runs mongod, so it needs the
same memory limit rather than the roadmap's proposed 256 MiB client-only allowance.

For diagnostics, run `make mongo-logs`; for a normal stop, run `make mongo-down`.
Do not delete volumes to troubleshoot without first reviewing the failure. Any deliberate
volume deletion permanently loses MongoDB documents, indexes and users and requires an
explicit user-run reset procedure. PostgreSQL grants and data do not change in this setup.

Install the optional client with `uv sync --locked --group nosql`. Plain `uv sync --locked`
does not install PyMongo or dnspython. For development lint, include all three optional groups:
`uv sync --locked --group bench --group streaming --group nosql`. With services running:

```bash
make test-mongo
make mongo-load
make mongo-load
make mongo-query
make mongo-stats
```

`mongo-load` reads the only dataset under `data/raw`, or accepts `DATASET=<name>`. Replay must
report zero inserted and zero modified rows. The loader checks the host's 10 GiB free-space
floor before loading and between chunks; Docker virtual-disk capacity is checked separately
through `mongo-stats`. Run one loader at a time. Partial failures retain committed inserts;
rerun the identical immutable source to finish. Existing documents are never replaced.

`mongo-query` selects an existing order and reports its identity, equal-result check, five
query samples per store after a warm-up, medians and execution plans. For a specific order:
`uv run --locked --group nosql python -m src.nosql.compare --order-id <order-id>`.
Both stores project the same fields. PostgreSQL uses the existing ETL role in a read-only
transaction because the dashboard read-only role cannot access raw tables.

`test-mongo` requires PostgreSQL raw events already loaded. It uses MongoDB's separate `_test`
database and leaves a few immutable fixtures per run. Teardown uses no administrator and
drops no databases. CI creates fresh services and loads its own synthetic fixture; local
verification does not establish a green GitHub run.

**Deliberate reset — user-run only:** permanently deletes all MongoDB documents, indexes,
users and journal data. PostgreSQL is unaffected. Stop all loaders and queries first.
Do not reset volumes to work around a network pull error.

```bash
make mongo-down
docker compose -f docker-compose.yml -f docker-compose.mongo.yml --profile nosql rm -f mongo mongo-init
docker volume rm deliverypulse_mongo_data deliverypulse_mongo_config
```

The user can then recreate the empty store with `make mongo-up` and reload with `make mongo-load`.

## Scale benchmark (pandas vs PySpark)

Needs the `bench` dependency group (`uv sync --locked --group bench`) and a **Java 21** runtime
for PySpark. The benchmark is separate from the ETL: it never touches the database, the object
store or `data/raw`. Methodology and results: `docs/benchmarks.md`, section 2.

| Command | What it does |
|---|---|
| `make bench-data SHARDS=n` | Builds n benchmark shards of 50,000 orders under `data/bench/shards/`, one at a time, deleting each shard's raw files after converting it to Parquet |
| `make benchmark` | The whole benchmark up to `BENCH_LARGEST` (default `2m`), `BENCH_RUNS` measured runs each (default 3); writes `docs/benchmarks/scale-<date>.json` and the generated section of `docs/benchmarks.md` |
| `make test-spark` | The pandas/Spark equivalence tests (marker `spark`); no database and no object store |
| `make bench-clean` | **Deletes** `data/bench/` after you type `DELETE` (shards, read copies, Spark scratch space) |

**Java 21.** Every benchmark target resolves `JAVA_HOME` with `/usr/libexec/java_home -v 21` and
stops with an install hint when it is missing. Java 22 or another version is never used silently.
Install it with `brew install --cask temurin@21`.

**Disk budget.** Everything the benchmark writes lives under `data/bench/`: the shards, the read
copies, Spark's `spark.local.dir` (`data/bench/spark-local`) and the temporary files
(`data/bench/tmp`, also used as `TMPDIR` and `java.io.tmpdir`). Their total must stay within
4 GiB; the code refuses a larger budget, so raising it needs a code change. At least 10 GiB must
remain free on `/` and on the volume of `data/`. Before each size and each shard the harness
projects the size from measured per-shard sizes and stops cleanly rather than creating data that
would not fit; during a run the usage is checked every second. Spark's scratch space and the
temporary files are deleted after every run, including after a failure.

**Memory limit.** Each run is one process; the resident memory of its whole process tree (Python
plus the Spark JVM) is sampled every 0.2 s. A run above 4 GiB is killed and recorded as stopped,
and that engine is not attempted at larger sizes; the other engine continues. This keeps the
benchmark within an 8 GB laptop.

**Privacy.** The JSON and the report record the hardware model, core count, RAM, OS version,
Python, JDK and package versions only: no hostname, user name or absolute path. Error messages
have the project and home paths removed.

Typical run (the first call builds the shards, which takes the longest):

```bash
make benchmark                      # up to 2M orders, 3 runs each
make benchmark BENCH_LARGEST=250k   # a quick check
make bench-clean                    # frees data/bench again
```

## Continuous integration and security scanning

Two GitHub Actions workflows run on every push to `main` and every pull request; `security.yml`
also runs weekly. Both have `permissions: contents: read` and use no repository secrets.

| Workflow | Job | What it runs |
|---|---|---|
| `tests.yml` | Lint and type check | `uv sync --locked --group bench --group streaming --group nosql`, `make lint` (ruff, mypy over `src`, `tests` and `scripts/*.py`), `make actionlint` |
| | Unit tests (no services) | `make test` |
| | Integration tests and coverage (PostgreSQL) | `.env` with generated, masked passwords; `make up`; `make test-integration`; `make coverage`; container logs on failure; `make down` |
| | Airflow DAG parse check | `.env` with generated, masked passwords; `make airflow-dag-check` |
| | Object store tests | `.env` with generated, masked passwords and keys; `make up`; `make s3-up`; `make test-s3`; logs on failure; `make s3-down`, `make down` |
| | Benchmark engines | `uv sync --locked --group bench`; the pandas/Spark equivalence tests with the runner's Java 21 |
| | NoSQL tests | Isolated PostgreSQL and MongoDB fixtures; `make test-mongo`, two `make mongo-load` calls and `make mongo-query`; stop services preserving volumes |
| | Determinism check (Linux) | `make generate-check ORDERS=1500 SEED=7` |
| `security.yml` | Secret scan (gitleaks) | `make scan` (working tree), then `gitleaks git` over the full history |
| | Dependency audit | `make audit` |
| | Static analysis | `make bandit` |
| | Trivy | `trivy fs` on the repository (fails on fixable HIGH/CRITICAL); `trivy image` on the pinned PostgreSQL, Airflow and SeaweedFS images (report only, exit code 0) |

Local equivalents (all through `uv run`):

| Command | What it does |
|---|---|
| `make audit` | `scripts/pip_audit.sh`: exports the hash-pinned packages from `uv.lock` and runs `pip-audit --strict` on them (the uv environment has no pip) |
| `make bandit` | `bandit -r src scripts/airflow_dag_check.py scripts/benchmark.py`: the Python files outside `src/` are scanned too, so moving code out of `src/` never leaves it unscanned. No `nosec` marker is used anywhere |
| `make actionlint` | `actionlint` on `.github/workflows/`, plus a YAML parse of `.github/dependabot.yml` |

**Why CI starts PostgreSQL with `make up`, not a `services:` container.**
- GitHub service containers start before checkout, so they cannot mount the init script.
- They also take their passwords from the workflow file, which would put them in the
  repository.
- `make up` gives CI exactly the local database: init script, roles, grants and the `_test`
  database. Passwords are generated in the job with `openssl rand` and masked with
  `::add-mask::`.

### Updating pins

| Pin | Where | How to verify or update |
|---|---|---|
| `actions/checkout` v7.0.1 | both workflows | `git ls-remote https://github.com/actions/checkout refs/tags/v7.0.1` must print the SHA in the `uses:` line |
| `astral-sh/setup-uv` v10.2.0 | both workflows | `git ls-remote https://github.com/astral-sh/setup-uv refs/tags/v10.2.0` |
| `aquasecurity/trivy-action` v0.36.0 (annotated tag) | `security.yml` | `git ls-remote https://github.com/aquasecurity/trivy-action 'refs/tags/v0.36.0^{}'`: the peeled commit is the pinned SHA |
| gitleaks 8.30.1 binary | `security.yml` (`GITLEAKS_SHA256`) | The `gitleaks_8.30.1_linux_x64.tar.gz` line of the release's `gitleaks_8.30.1_checksums.txt` |
| `postgres:18.6-trixie` digest | `tests.yml`, `security.yml` | `docker buildx imagetools inspect postgres:18.6-trixie`: the top-level `Digest:` (the multi-architecture index) |
| `apache/airflow:3.3.2-python3.12` digest `sha256:9df9c8be4096b9cc626bd7cb1f2b8c712eef66c59c615f8c7e6200871ca10bd1` | `docker/airflow/Dockerfile`, `docker-compose.airflow.yml` (`airflow-dag-check`), `Makefile` (`AIRFLOW_BASE_IMAGE`), `security.yml` | `docker buildx imagetools inspect apache/airflow:3.3.2-python3.12`: the top-level `Digest:` |
| `chrislusf/seaweedfs:4.48` digest `sha256:4e61d15fd35994cb1e43e1e553dff106794841fd9a99ade2fc8c8bfce4d7872d` | `docker-compose.s3.yml`, `security.yml` | `docker buildx imagetools inspect chrislusf/seaweedfs:4.48`: the top-level `Digest:`; its platforms include `linux/arm64` and `linux/amd64` |
| `ghcr.io/astral-sh/uv:0.13.0` digest `sha256:cdc6093146eb3ff6a40107b38f008b789e050e77ad87865e381d9917da55a168` | `docker/airflow/Dockerfile` (copies the `uv` binary) | `docker buildx imagetools inspect ghcr.io/astral-sh/uv:0.13.0`: the top-level `Digest:` |

Every pin fails safe: a wrong SHA, checksum or digest stops the job rather than running
something unverified. Dependabot proposes updates for the action pins, `uv.lock`, the Compose
tag and the two `FROM` lines of the Airflow Dockerfile. When a tag changes, update every
listed copy of its digest by hand.

## PostgreSQL (core profile)

| Item | Value |
|---|---|
| Image | `postgres:18.6-trixie` (PostgreSQL 18.6 on Debian trixie; publishes `linux/arm64`) |
| Container | `deliverypulse-postgres-1` (Compose project `deliverypulse`, service `postgres`) |
| Port | `127.0.0.1:${POSTGRES_PORT}` on the host (localhost only) to 5432 in the container |
| Data volume | `deliverypulse_pgdata`, mounted at `/var/lib/postgresql`; `PGDATA=/var/lib/postgresql/18/docker` |
| Init script | `docker/postgres/init-deliverypulse.sh` (read-only mount); runs `sql/roles.sql` once, then `sql/schema.sql`, `sql/indexes.sql`, `sql/grants.sql` for the main and the `_test` database |
| Minimum version | **PostgreSQL 18.** The orders upsert uses `RETURNING old.*, new.*` (new in 18) to recompute the dates an updated order leaves. An older image fails at the first load with a syntax error |
| Memory limit | `mem_limit: 1g`; `shm_size: 256mb` |
| Timezone | Container `TZ=UTC`; database default `timezone = 'UTC'` |

### Roles

| Role | Can log in | Purpose |
|---|---|---|
| `POSTGRES_SUPERUSER` (default `deliverypulse_admin`) | yes | Bootstrap superuser: initialisation and admin only. Never used by the pipeline, dashboards or tests |
| `deliverypulse_owner` | no | Owns all schemas and tables, so no application login can run DDL |
| `deliverypulse_etl` | yes | Pipeline writer: DML only (raw is append-only: SELECT and INSERT) |
| `deliverypulse_readonly` | yes | Dashboards and analysts: SELECT on `analytics` tables only |

Exact privileges are in `sql/grants.sql` and enforced by `tests/integration/test_roles.py`.

### Passwords and the container environment

- All passwords come from `.env`; none is written in any project file.
- `roles.sql` reads the role passwords with psql's `\getenv`, so they never appear on a command
  line.
- **The role passwords are present in the container environment** (they are passed in for the
  init script) and are therefore visible to anyone who can run `docker inspect` on this machine.
  This is acceptable for local development only. A shared or production deployment would use
  Docker secrets or a secret manager instead.

### Init scripts run only once

The postgres image runs `/docker-entrypoint-initdb.d/*` **only when the data directory is empty**,
that is, the first time `deliverypulse_pgdata` is initialised. Consequences:

- Editing `sql/*.sql` or the init script later does **not** change an existing database.
- Changing a password in `.env` later does **not** change the role's password in the database.
- Until a migration tool is added, apply schema or role changes with the reset procedure below
  (development data only).
- If the first initialisation fails part-way (for example a `CHANGE_ME` password), the data
  directory may already exist and the init scripts will not run again. Fix `.env`, then reset.

### Reset procedure (deletes all data)

```bash
make reset-db   # prints a data-loss warning; type DELETE to confirm, anything else aborts
make up         # re-initialises an empty database from sql/
```

`make reset-db` stops the core services, checks whether the volume `deliverypulse_pgdata` exists,
removes it if so, and reports clearly if it did not exist. Aborting (any answer other than
`DELETE`, or no interactive terminal) deletes nothing and exits non-zero.

### Approximate RAM use per profile

| Profile | Services | Approximate RAM | Hard limit |
|---|---|---|---|
| `core` | PostgreSQL | ~50-150 MiB idle; up to a few hundred MiB under analytical queries with default settings (`shared_buffers` 128 MB) | 1 GiB (`mem_limit`) |
| `s3` | SeaweedFS (master, volume, filer and S3 gateway in one process) | Measured 2026-10-14: 88-152 MiB after start; up to 403.3 MiB after exporting and checking the default dataset twice | 768 MiB (`mem_limit`; raised from 512 MiB ahead of larger datasets) |
| `streaming` | Kafka broker | Measured 2026-10-15: 362.5 MiB idle; largest sample 507.3 MiB | 1 GiB (`mem_limit`) |
| `nosql` | MongoDB server | Measured 2026-10-15: 121.3 MiB baseline; 433.8 MiB during load; 487 MiB after replay (sparse samples) | 768 MiB; WiredTiger cache 0.25 GiB. Offline bootstrap has the same limit and runs separately |
| `core` + `airflow` | PostgreSQL, airflow-db, api-server, scheduler, dag-processor | Measured 2026-10-14: about 1.0-1.1 GiB idle; peak 2.12-2.26 GiB during a full DAG run | sum of limits 5.25 GiB (per-container limits in "Airflow orchestration") |

**Airflow, measured 2026-10-14** (`make airflow-stats`, sampled every few seconds while the last
batch window was cleared and rerun from the UI; all five tasks, 2 min 2 s):

| Container | Idle | Peak during the run | Limit |
|---|---|---|---|
| `airflow-scheduler` (runs the tasks) | 531.4 MiB | 1,581.1 MiB | 2 GiB |
| `airflow-api-server` | 231.9 MiB | 294.5 MiB | 1 GiB |
| `airflow-dag-processor` | 173.1 MiB | 253.4 MiB | 768 MiB |
| `airflow-db` | 38.22 MiB | 59.7 MiB | 512 MiB |
| `postgres` | 69.9 MiB (just restarted) | 207.6 MiB | 1 GiB |
| **Total** | **about 1.02 GiB** | **2,172.6 MiB (2.12 GiB)**, highest single sample | |

The per-container peaks happened at different moments, so they add up to more than the highest
total sample. At the highest total sample the scheduler was at 1.544 GiB, 77 % of its 2 GiB
limit. An earlier rerun of the same window the same day peaked at 2,310.0 MiB (2.26 GiB) in
total, scheduler 1,625.1 MiB.

Rows without measured values are estimates. Measure on your machine with
`docker stats --no-stream deliverypulse-postgres-1`. Measured after a full load of the default
dataset (2026-10-11): 178.8 MiB of the 1 GiB limit. Docker Desktop's VM itself uses additional
memory on the host.

### Hardening and its limits

- `cap_drop: [ALL]`, then only `CHOWN`, `DAC_OVERRIDE`, `FOWNER`, `SETGID` and `SETUID` are added
  back: the official entrypoint needs them to fix data-directory ownership and switch to the
  unprivileged `postgres` user. `no-new-privileges` is set.
- `read_only` is **not** set: the entrypoint writes to `/var/run/postgresql` and the data
  directory at start-up. Revisit with tmpfs mounts if needed.
- Image pinned by exact tag locally (`POSTGRES_IMAGE` defaults to `postgres:18.6-trixie`); CI
  sets `POSTGRES_IMAGE` to the same tag pinned by digest (see "Updating pins").

## Secret scanning

- `.gitleaks.toml` extends the gitleaks default rules. Its allowlist covers only generated paths
  (`.venv/`, `.git/`, `data/`, `.ruff_cache/`, `.mypy_cache/`, `.pytest_cache/`) plus the root
  `.env` file.
- **Why `.env` is allowlisted:** it is git-ignored, holds this machine's real local settings, and
  is never published. `.env.example` is published, so it is scanned. Consequence: `make scan`
  will not warn about a secret in `.env`; keep `.env` git-ignored.
- `make scan-selftest` creates a directory with `mktemp -d` under the resolved `$TMPDIR`, writes
  one randomly generated GitHub-token-shaped string there, and requires gitleaks to exit with code
  42 (the "leak found" code the script sets, distinct from errors). A `trap` deletes the scratch
  directory on exit, but only if the path is non-empty, a directory and under the resolved temp
  directory. Run `make scan` afterwards to confirm nothing was left in the project.
- A working-tree scan does not prove that git history is clean. After publishing, run a history
  scan (`gitleaks git .`) yourself.

## Troubleshooting

| Symptom | Check |
|---|---|
| `uv run python` shows a non-`.venv` path or a version other than 3.12 | `uv python list --only-installed`, then `uv sync --locked` again |
| `uv sync --locked` says the lockfile is out of date | `pyproject.toml` changed after `uv lock`; rerun `uv lock` |
| `make scan` reports a finding | Read the redacted report: file and line are shown. Remove the secret; never allowlist a source, config or docs path to silence it |
| `gitleaks not found on PATH` | `which gitleaks`; install with `brew install gitleaks` |
| `make up`: `set POSTGRES_... in .env` | A required variable is missing from `.env`; compare with `.env.example` |
| `make up`: port is already allocated | `lsof -nP -iTCP:5432 -sTCP:LISTEN` shows the other process. Set a free `POSTGRES_PORT` in `.env` (for example 5433) |
| `make up` times out / container unhealthy | `docker compose --profile core logs postgres`. Look for `deliverypulse-init: ERROR` (fix `.env`, then `make reset-db` and `make up`) |
| Integration tests: `password authentication failed` | `.env` passwords changed after first init. Reset (development data only) |
| Integration tests: `Cannot connect to PostgreSQL` | `make up`, then `docker compose --profile core ps` |
| `make up` fails with `init-deliverypulse.sh is not executable` | Guard added after a real failure. Run `chmod 755 docker/postgres/init-deliverypulse.sh`. If an earlier start already ran initdb, the volume is partly initialised (cluster and database exist; roles and schemas do not), so also run `make reset-db` (type `DELETE`) before `make up` |
| Container log: `10-init-deliverypulse.sh: /usr/bin/env: bad interpreter: Permission denied` | Cause (seen 2026-10-11): the init script had host mode `-rw-r--r--` (no execute bit); inside the container it showed as `-rw-r--r-- 0 0`. The entrypoint still logged `running` rather than `sourcing`, so its `[ -x ]` check returned true while the actual exec was refused, most likely because Docker Desktop's file sharing answered the access check. Do not rely on the entrypoint's sourcing fallback; keep the script executable. Same fix as the row above. **Publishing:** git stores the execute bit, so commit the file as executable and check with `git ls-files -s docker/postgres/init-deliverypulse.sh`; the mode must be `100755` |
| `make load`: `column "..." of relation "daily_delivery_metrics" does not exist` or a syntax error near `old` | The database was initialised with an older schema (before the metrics layer), or the image is older than PostgreSQL 18. Run `make db-status` to check the version; reset with `make reset-db` (type `DELETE`), `make up`, `make load` |
| `make explain`: `analytics.orders is empty` | Load first: `make load` |
| `make dashboard`: "port 8501 is already in use" or the page does not load | Another dashboard is running; stop it (Ctrl+C) or use `make dashboard DASHBOARD_PORT=8502` |
| `make load` exits 1 with `quality gate: <check>` | A blocking quality check failed; the batch was rolled back. See "Recovery after a quality-gate failure" above |
| `make load`: `column "window_start" ... does not exist` or `relation "audit.quality_runs" does not exist` | The database predates the quality framework. Reset: `make reset-db` (type `DELETE`), `make up`, `make load` |
| `make quality-report`: `load-time quality results exist for batches ...` | Some batches were loaded before the quality framework existed, or failed; reset and load again |
| Container fails at start with a permission or `chown` error | The capability set is too tight for this Docker version: `docker compose --profile core logs postgres` and report the error |
