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

Then replace every `CHANGE_ME` in `.env` with a real local password
(`POSTGRES_SUPERUSER_PASSWORD`, `DELIVERYPULSE_ETL_PASSWORD`, `DELIVERYPULSE_READONLY_PASSWORD`)
**before the first `make up`**. The init script refuses to initialise with placeholder values.

## Everyday commands

| Command | What it does |
|---|---|
| `make lint` | `ruff check`, `ruff format --check`, `mypy src tests` (strict) |
| `make format` | Applies ruff fixes and ruff formatting |
| `make test` | Unit tests only (`pytest -m unit`); needs no services and no `.env` |
| `make test-integration` | Integration tests (`pytest -m integration`); needs `make up` first. Fails (never skips) if PostgreSQL is unreachable |
| `make coverage` | Unit-only coverage of `src/`, then combined unit + integration coverage with missing lines; fails below `COVERAGE_MIN` (default 80 %). Needs `make up` |

Every test carries exactly one marker, `unit` or `integration` (collection stops otherwise), and
unit tests cannot open a database connection (they fail with "unit tests must not connect to a
database"). So a failure in `make test` is always a unit failure, never a missing service.
| `make scan` | `gitleaks dir .` with `.gitleaks.toml`; fails (non-zero) on any finding |
| `make scan-selftest` | Proves gitleaks detects a synthetic secret in a scratch dir outside the project |
| `make up` | Starts the `core` profile and waits (up to 120 s) until PostgreSQL is healthy |
| `make down` | Stops and removes the containers. **Keeps** the `deliverypulse_pgdata` volume |
| `make db-status` | Prints `PGDATA`, server version, `system_identifier`, timezone and project schemas |
| `make reset-db` | **Deletes all database data** after you type `DELETE` (see below) |

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

**Why nested-loop joins are off during the checks.** In a load the checks read the batch's
uncommitted rows. PostgreSQL has no statistics for them, so it estimates about one row, and a
nested-loop plan becomes quadratic. Measured before the fix: `raw_parent_after_children` took
35 s instead of 0.4 s. The suite therefore runs with `SET LOCAL enable_nestloop = off`, restored
right after. `ANALYZE` inside the transaction would also work, but the ETL role does not own the
tables (by design: see "Roles" below).

**A database created before the quality framework** (without the two quality audit tables and
the two `pipeline_runs` window columns) needs the reset procedure once: `make reset-db` (type `DELETE`), `make up`, `make load`.

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

These are estimates. Measure on your machine with
`docker stats --no-stream deliverypulse-postgres-1`. Measured after a full load of the default
dataset (2026-10-11): 178.8 MiB of the 1 GiB limit. Docker Desktop's VM itself uses additional
memory on the host.

### Hardening and its limits

- `cap_drop: [ALL]`, then only `CHOWN`, `DAC_OVERRIDE`, `FOWNER`, `SETGID` and `SETUID` are added
  back: the official entrypoint needs them to fix data-directory ownership and switch to the
  unprivileged `postgres` user. `no-new-privileges` is set.
- `read_only` is **not** set: the entrypoint writes to `/var/run/postgresql` and the data
  directory at start-up. Revisit with tmpfs mounts if needed.
- Image pinned by exact tag; digest pinning is not used.

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
| `make load` exits 1 with `quality gate: <check>` | A blocking quality check failed; the batch was rolled back. See "Recovery after a quality-gate failure" above |
| `make load`: `column "window_start" ... does not exist` or `relation "audit.quality_runs" does not exist` | The database predates the quality framework. Reset: `make reset-db` (type `DELETE`), `make up`, `make load` |
| `make quality-report`: `load-time quality results exist for batches ...` | Some batches were loaded before the quality framework existed, or failed; reset and load again |
| Container fails at start with a permission or `chown` error | The capability set is too tight for this Docker version: `docker compose --profile core logs postgres` and report the error |
