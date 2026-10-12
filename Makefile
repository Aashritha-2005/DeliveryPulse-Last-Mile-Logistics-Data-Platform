# DeliveryPulse task runner.
#
# Must work on GNU Make 3.81 (macOS) and GNU Make 4.x (Linux CI):
# no .ONESHELL, .SHELLFLAGS, undefine, $(file ...), != or --output-sync.
# Each recipe line runs in its own shell; multi-step logic lives in scripts/*.sh.
# Every Python command goes through `uv run`.

UV_RUN := uv run
GITLEAKS_CONFIG := .gitleaks.toml
COMPOSE := docker compose --profile core
INIT_SCRIPT := docker/postgres/init-deliverypulse.sh

# Synthetic data settings (override on the command line, e.g. `make generate ORDERS=5000`).
ORDERS ?= 50000
SEED ?= 42
DRIVERS ?=
RAW_DIR ?= data/raw
GROUND_TRUTH_DIR ?= data/ground_truth
REPORT ?= docs/data_quality_report.md
GEN_ARGS = --orders $(ORDERS) --seed $(SEED) $(if $(DRIVERS),--drivers $(DRIVERS),)

.DEFAULT_GOAL := help
DATASET ?=
PROCESSED_DIR ?= data/processed
# Transform and reconcile use DATASET=<name>, or the only dataset in RAW_DIR when it is empty.
DATASET_ARG = $(if $(DATASET),--dataset $(DATASET),)
# Label of an `explain` run (written to docs/benchmarks/explain-$(LABEL).{txt,json}).
LABEL ?= current
# Quality-check threshold overrides for `make load` and `make quality`, for example
# QUALITY_ARGS="--threshold raw_duplicate_rows=0.05 --threshold freshness_lag=12".
QUALITY_ARGS ?=
# Local port of `make dashboard` (bound to 127.0.0.1 only).
DASHBOARD_PORT ?= 8501
# Minimum combined (unit + integration) line coverage of src/ for `make coverage`.
COVERAGE_MIN ?= 80
# Admin-only: runs a SQL file inside the container as the bootstrap superuser (index DDL).
ADMIN_PSQL = $(COMPOSE) exec -T postgres sh -c 'psql --no-psqlrc -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -f /deliverypulse/sql/$(1)'

.PHONY: help lint format test test-integration coverage audit bandit actionlint scan scan-selftest up down reset-db db-status generate generate-check profile clean-dataset transform reconcile load reconcile-db db-counts metrics analytics explain indexes-create indexes-drop quality quality-report dashboard dashboard-check

help:
	@echo "DeliveryPulse targets:"
	@echo "  make lint              ruff lint, ruff format check, mypy (strict)"
	@echo "  make format            apply ruff fixes and ruff formatting"
	@echo "  make test              unit tests only (no services needed)"
	@echo "  make test-integration  integration tests (needs 'make up' first)"
	@echo "  make coverage          unit coverage, then combined unit+integration coverage of src/ (needs 'make up'; fails below COVERAGE_MIN=$(COVERAGE_MIN)%)"
	@echo "  make audit             pip-audit: known vulnerabilities in every package locked in uv.lock"
	@echo "  make bandit            bandit: insecure code patterns in src/"
	@echo "  make actionlint        validate the GitHub Actions workflows and the Dependabot file"
	@echo "  make scan              gitleaks secret scan of the project directory"
	@echo "  make scan-selftest     prove gitleaks detects a synthetic secret (scratch dir outside the project)"
	@echo "  make up                start PostgreSQL (core profile) and wait until healthy"
	@echo "  make down              stop services; KEEPS the database volume"
	@echo "  make db-status         print PGDATA, server version, system_identifier, timezone, schemas"
	@echo "  make reset-db          DELETE the database volume (asks for confirmation)"
	@echo "  make generate          generate the synthetic dataset (ORDERS=$(ORDERS) SEED=$(SEED))"
	@echo "  make generate-check    generate twice in temp dirs outside the project and compare sha256"
	@echo "  make profile           verify the defect manifest and write $(REPORT)"
	@echo "  make transform         validate and clean all batches into $(PROCESSED_DIR)/<dataset> (fresh state)"
	@echo "  make reconcile         compare the transform's dispositions with the defect manifest"
	@echo "  make clean-dataset DATASET=<name>  DELETE one obsolete dataset (asks for confirmation)"
	@echo "  make load              load all batches into PostgreSQL (idempotent, in sequence; needs 'make up')"
	@echo "  make reconcile-db      compare the dispositions stored in PostgreSQL with the defect manifest"
	@echo "  make db-counts         row count of every project table, plus the database size"
	@echo "  make metrics           full rebuild of dimensions and daily aggregates (all dates)"
	@echo "  make analytics         run every query in sql/analytics.sql as the read-only role"
	@echo "  make explain LABEL=x   EXPLAIN (ANALYZE, BUFFERS) benchmark into docs/benchmarks/explain-x.*"
	@echo "  make indexes-create    admin: create the analytics indexes (sql/indexes.sql) and ANALYZE"
	@echo "  make indexes-drop      admin: drop the analytics indexes and ANALYZE (benchmark 'before')"
	@echo "  make quality           run every data-quality check on the database (exit 1 on a blocking failure)"
	@echo "  make quality-report    prove every manifest defect type is detected; update $(REPORT)"
	@echo "  make dashboard         start the Streamlit dashboard on http://127.0.0.1:$(DASHBOARD_PORT) (read-only role; needs 'make up')"
	@echo "  make dashboard-check   run every dashboard query as the read-only role; print rows and timings"

lint:
	$(UV_RUN) ruff check .
	$(UV_RUN) ruff format --check .
	$(UV_RUN) mypy src tests

format:
	$(UV_RUN) ruff check --fix .
	$(UV_RUN) ruff format .

test:
	$(UV_RUN) pytest -m unit

test-integration:
	$(UV_RUN) pytest -m integration

coverage:
	$(UV_RUN) pytest -m unit --cov=src --cov-report=term
	$(UV_RUN) pytest -m integration --cov=src --cov-append --cov-report=term-missing --cov-fail-under=$(COVERAGE_MIN)

audit:
	bash scripts/pip_audit.sh

bandit:
	$(UV_RUN) bandit -r src

actionlint:
	$(UV_RUN) actionlint
	$(UV_RUN) python -c "import yaml; yaml.safe_load(open('.github/dependabot.yml', encoding='utf-8')); print('.github/dependabot.yml: valid YAML')"

scan:
	@command -v gitleaks >/dev/null 2>&1 || { echo "ERROR: gitleaks not found on PATH. Install it with: brew install gitleaks" >&2; exit 1; }
	gitleaks dir . --config $(GITLEAKS_CONFIG) --redact --no-banner --verbose

scan-selftest:
	bash scripts/scan_selftest.sh

up:
	@test -x $(INIT_SCRIPT) || { echo "ERROR: $(INIT_SCRIPT) is not executable, so the postgres entrypoint cannot run it." >&2; echo "Fix with: chmod 755 $(INIT_SCRIPT)" >&2; exit 1; }
	$(COMPOSE) up -d --wait --wait-timeout 120

down:
	$(COMPOSE) down

db-status:
	bash scripts/db_status.sh

reset-db:
	bash scripts/reset_db.sh

generate:
	$(UV_RUN) python -m src.ingestion.generate_data $(GEN_ARGS) --raw-dir $(RAW_DIR) --ground-truth-dir $(GROUND_TRUTH_DIR)

generate-check:
	bash scripts/generate_check.sh $(GEN_ARGS)

profile:
	$(UV_RUN) python -m src.groundtruth.profile $(GEN_ARGS) --raw-dir $(RAW_DIR) --ground-truth-dir $(GROUND_TRUTH_DIR) --report $(REPORT)

clean-dataset:
	bash scripts/clean_dataset.sh "$(DATASET)"

transform:
	$(UV_RUN) python -m src.transformations.transform --raw-dir $(RAW_DIR) --processed-dir $(PROCESSED_DIR) $(DATASET_ARG)

reconcile:
	$(UV_RUN) python -m src.groundtruth.reconcile --raw-dir $(RAW_DIR) --processed-dir $(PROCESSED_DIR) --ground-truth-dir $(GROUND_TRUTH_DIR) $(DATASET_ARG)

load:
	$(UV_RUN) python -m src.database.load_postgres --raw-dir $(RAW_DIR) $(DATASET_ARG) $(QUALITY_ARGS)

reconcile-db:
	$(UV_RUN) python -m src.groundtruth.reconcile_db --raw-dir $(RAW_DIR) --ground-truth-dir $(GROUND_TRUTH_DIR) $(DATASET_ARG)

db-counts:
	$(UV_RUN) python -m src.database.db_counts

metrics:
	$(UV_RUN) python -m src.database.metrics

analytics:
	$(UV_RUN) python -m src.database.analytics_queries run --raw-dir $(RAW_DIR)

explain:
	$(UV_RUN) python -m src.database.analytics_queries explain --label $(LABEL) --raw-dir $(RAW_DIR)

indexes-create:
	$(call ADMIN_PSQL,indexes.sql)

indexes-drop:
	$(call ADMIN_PSQL,indexes_drop.sql)

quality:
	$(UV_RUN) python -m src.quality.run $(QUALITY_ARGS)

quality-report:
	$(UV_RUN) python -m src.groundtruth.quality_coverage --raw-dir $(RAW_DIR) --ground-truth-dir $(GROUND_TRUTH_DIR) --report $(REPORT) $(DATASET_ARG)

# Localhost only, Streamlit usage statistics off, no source-file watching, minimal toolbar (no
# "Deploy" button); XSRF and CORS protections stay at their defaults.
# PYTHONPATH makes the project's `src` package importable from the app script.
dashboard:
	PYTHONPATH=$(CURDIR) $(UV_RUN) streamlit run src/dashboard/app.py --server.address 127.0.0.1 --server.port $(DASHBOARD_PORT) --server.headless true --server.fileWatcherType none --client.toolbarMode minimal --browser.gatherUsageStats false

dashboard-check:
	$(UV_RUN) python -m src.dashboard.data
