# DeliveryPulse task runner.
#
# Must work on GNU Make 3.81 (macOS) and GNU Make 4.x (Linux CI):
# no .ONESHELL, .SHELLFLAGS, undefine, $(file ...), != or --output-sync.
# Each recipe line runs in its own shell; multi-step logic lives in scripts/*.sh.
# Every Python command goes through `uv run`.

UV_RUN := uv run
GITLEAKS_CONFIG := .gitleaks.toml
# Python files outside src/ (scripts/*.py); lint, mypy and bandit cover them too, so moving
# code out of src/ never leaves it unscanned. Listed explicitly: GNU Make 3.81 has no $(wildcard
# ...) sorting guarantee issues here, but an explicit list fails loudly when a file is renamed.
PY_SCRIPTS := scripts/airflow_dag_check.py scripts/benchmark.py scripts/k8s.py scripts/k8s_pipeline.py scripts/demo.py
KUBECTL ?= /opt/homebrew/bin/kubectl
KIND ?= kind
K8S_RUN = KUBECTL="$(KUBECTL)" KIND="$(KIND)" $(UV_RUN) --locked python -m scripts.k8s
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
# Airflow: its services live in docker-compose.airflow.yml, layered on the core file so the
# airflow profile always runs together with core (which holds the pipeline data). The core
# targets above never load that file, so they need no AIRFLOW_ variable.
COMPOSE_AIRFLOW_FILES := docker compose -f docker-compose.yml -f docker-compose.airflow.yml
COMPOSE_AIRFLOW := $(COMPOSE_AIRFLOW_FILES) --profile core --profile airflow
AIRFLOW_IMAGE := deliverypulse-airflow:3.3.2
AIRFLOW_BASE_IMAGE := apache/airflow:3.3.2-python3.12@sha256:9df9c8be4096b9cc626bd7cb1f2b8c712eef66c59c615f8c7e6200871ca10bd1
DAG_ID := delivery_pipeline
# Object store: its services live in docker-compose.s3.yml, layered on the core file, so the
# core targets need no S3_ variable.
COMPOSE_S3 := docker compose -f docker-compose.yml -f docker-compose.s3.yml --profile s3
COMPOSE_KAFKA := docker compose -f docker-compose.yml -f docker-compose.kafka.yml --profile streaming
COMPOSE_MONGO := docker compose -f docker-compose.yml -f docker-compose.mongo.yml --profile nosql
# Benchmark (bench dependency group; PySpark needs a Java 21 runtime). Every benchmark command
# resolves JAVA_HOME with java_home -v 21 and fails with a clear message when it is missing.
JAVA_21 = $$(/usr/libexec/java_home -v 21 2>/dev/null || true)
REQUIRE_JAVA_21 = test -n "$(JAVA_21)" || { echo "ERROR: Java 21 not found. Install it (for example: brew install --cask temurin@21) and check with: /usr/libexec/java_home -v 21" >&2; exit 1; }
BENCH_LARGEST ?= 2m
BENCH_RUNS ?= 3
BENCH_SHARDS ?= 1
BENCH_BUDGET_GIB ?= 4
# Minimum combined (unit + integration) line coverage of src/ for `make coverage`.
COVERAGE_MIN ?= 80
# Admin-only: runs a SQL file inside the container as the bootstrap superuser (index DDL).
ADMIN_PSQL = $(COMPOSE) exec -T postgres sh -c 'psql --no-psqlrc -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -f /deliverypulse/sql/$(1)'

.PHONY: help lint format test test-integration coverage audit bandit actionlint scan scan-selftest up down reset-db db-status generate generate-check profile clean-dataset transform reconcile load reconcile-db db-counts metrics analytics explain indexes-create indexes-drop quality quality-report dashboard dashboard-check quality-demo-plant quality-demo-remove airflow-build airflow-up airflow-down airflow-password airflow-runs airflow-stats airflow-dag-check airflow-image-info airflow-reset s3-up s3-down s3-logs s3-stats lake-export lake-check test-s3 test-spark bench-data benchmark bench-clean db-sizes

help:
	@echo "  make demo-preflight    USER: read-only demo isolation, credentials and disk checks"
	@echo "  make demo              USER: isolated 5,000-order PostgreSQL demo (no heavy profiles)"
	@echo "  make demo-down         USER: stop only the demo PostgreSQL; keep all data"
	@echo "  make demo-dashboard    USER: dashboard against the demo database only"
	@echo "  make demo-disk         read-only host free-space checkpoint"
	@echo "  make test-demo         unit tests for demo isolation and failure handling"
	@echo "  make k8s-preflight     user: disk, selected kind/kubectl versions and Docker status"
	@echo "  make test-k8s-manifests static Kubernetes manifest and stage-guard tests"
	@echo "  make test-k8s          USER ONLY: live Kubernetes replay and wrong-password tests"
	@echo "  Kubernetes stages: see docs/operations.md; run ONE stage then review its output"
	@echo "  make mongo-env-check   validate MongoDB settings without printing values"
	@echo "  make mongo-up          stop MongoDB, initialize users/indexes offline, start healthy"
	@echo "  make mongo-down        stop MongoDB; KEEPS both volumes"
	@echo "  make kafka-env-check   validate local streaming configuration (values never printed)"
	@echo "  make kafka-up          start Kafka, wait healthy, create the three explicit topics"
	@echo "  make kafka-down        stop only Kafka services; KEEPS volumes and PostgreSQL"
	@echo "  make kafka-logs        last 100 broker log lines"
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
	@echo "  make quality-demo-plant   demo: insert one bad order so the next quality run fails"
	@echo "  make quality-demo-remove  demo: delete that order again (leaves no trace)"
	@echo "  make airflow-build     build the DeliveryPulse Airflow image"
	@echo "  make airflow-up        start core + airflow (UI on http://127.0.0.1:8080) and wait until healthy"
	@echo "  make airflow-down      stop core + airflow; KEEPS every volume"
	@echo "  make airflow-password  print the generated Airflow admin password"
	@echo "  make airflow-runs      list the $(DAG_ID) runs with state, start and end"
	@echo "  make airflow-stats     memory and CPU of every running DeliveryPulse container"
	@echo "  make airflow-dag-check parse and verify the DAG inside the pinned Airflow image"
	@echo "  make airflow-image-info size of the Airflow image and of the project venv inside it"
	@echo "  make s3-up             start the object store (S3 API on 127.0.0.1), wait until healthy, create the bucket"
	@echo "  make s3-down           stop the object store; KEEPS its volumes"
	@echo "  make s3-logs           last log lines of the object-store containers"
	@echo "  make s3-stats          memory and CPU of the object-store container"
	@echo "  make lake-export       write the raw, cleaned and aggregated Parquet layers to the bucket (needs make up, make transform)"
	@echo "  make lake-check        read every lake object back through S3 and reconcile it with its source"
	@echo "  make test-s3           tests against the live object store (needs make up and make s3-up)"
	@echo "  make bench-data SHARDS=n  build n benchmark shards of 50,000 orders (disk-guarded)"
	@echo "  make benchmark         pandas vs PySpark at every size that fits the disk budget (needs Java 21)"
	@echo "  make test-spark        pandas/Spark equivalence tests (needs Java 21)"
	@echo "  make bench-clean       DELETE data/bench/ (asks for confirmation)"
	@echo "  make db-sizes          on-disk size of every project table, largest first"
	@echo "  make airflow-reset     DELETE Airflow's metadata, logs and password (asks for confirmation)"

lint:
	$(UV_RUN) ruff check .
	$(UV_RUN) ruff format --check .
	$(UV_RUN) --locked --group bench --group streaming --group nosql mypy src tests $(PY_SCRIPTS)

.PHONY: mongo-env-check mongo-up mongo-down mongo-logs
mongo-env-check:
	$(UV_RUN) --locked python -m src.nosql.config

# Only one mongod may open the data volume. The user runs these service actions.
mongo-up: mongo-env-check
	$(COMPOSE_MONGO) stop mongo
	$(COMPOSE_MONGO) run --rm --no-deps mongo-init
	$(COMPOSE_MONGO) up -d --no-deps --wait --wait-timeout 180 mongo

mongo-down:
	$(COMPOSE_MONGO) stop mongo

mongo-logs:
	$(COMPOSE_MONGO) logs --tail 100 mongo

.PHONY: mongo-load mongo-query mongo-stats test-mongo
mongo-load:
	$(UV_RUN) --locked --group nosql python -m src.nosql.load_mongo --raw-dir $(RAW_DIR) $(DATASET_ARG)

mongo-query:
	$(UV_RUN) --locked --group nosql python -m src.nosql.compare

mongo-stats:
	$(UV_RUN) --locked --group nosql python -m src.nosql.stats
	$(COMPOSE_MONGO) exec -T mongo sh -c 'du -sk /data/db /data/configdb; df -k /data/db'
	docker stats --no-stream --format '{{.Name}} {{.MemUsage}} {{.CPUPerc}}' deliverypulse-mongo-1

test-mongo:
	$(UV_RUN) --locked --group nosql pytest -m mongo

.PHONY: kafka-env-check kafka-up kafka-down kafka-logs
kafka-env-check:
	$(UV_RUN) python -m src.streaming.config

kafka-up: kafka-env-check
	$(COMPOSE_KAFKA) up -d --wait --wait-timeout 180 kafka
	$(COMPOSE_KAFKA) run --rm --no-deps kafka-topics

kafka-down:
	$(COMPOSE_KAFKA) stop kafka-topics kafka

kafka-logs:
	$(COMPOSE_KAFKA) logs --tail 100 kafka

COUNT ?= 500
RATE ?= 500
TRIAL ?= 0
MALFORMED ?= 5
KAFKA_GROUP ?= deliverypulse-local-v1
.PHONY: kafka-produce kafka-consume
kafka-produce: kafka-env-check
	$(UV_RUN) --locked --group streaming python -m src.streaming.cli produce --count $(COUNT) --rate $(RATE) --raw-dir $(RAW_DIR)

kafka-consume: kafka-env-check
	$(UV_RUN) --locked --group streaming python -m src.streaming.cli consume --count $(COUNT) --group $(KAFKA_GROUP)

.PHONY: kafka-demo test-kafka kafka-stats
kafka-demo: kafka-env-check
	$(UV_RUN) --locked --group streaming python -m src.streaming.demo --count $(COUNT) --rate $(RATE) --trial $(TRIAL) --malformed $(MALFORMED) --raw-dir $(RAW_DIR)

test-kafka:
	$(UV_RUN) --locked --group streaming pytest -m kafka

kafka-stats:
	$(COMPOSE_KAFKA) exec -T kafka sh -c 'du -sk /var/lib/kafka/data'
	docker stats --no-stream --format '{{.Name}} {{.MemUsage}} {{.CPUPerc}}' deliverypulse-kafka-1 deliverypulse-postgres-1

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
	$(UV_RUN) bandit -r src $(PY_SCRIPTS)

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

db-sizes:
	$(UV_RUN) python -m src.database.db_sizes

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

quality-demo-plant:
	$(UV_RUN) python -m src.quality.demo plant

quality-demo-remove:
	$(UV_RUN) python -m src.quality.demo remove

airflow-build:
	bash scripts/airflow_env_check.sh
	$(COMPOSE_AIRFLOW) build airflow-init

airflow-up:
	@test -x $(INIT_SCRIPT) || { echo "ERROR: $(INIT_SCRIPT) is not executable. Fix with: chmod 755 $(INIT_SCRIPT)" >&2; exit 1; }
	bash scripts/airflow_env_check.sh
	$(COMPOSE_AIRFLOW) up -d --wait --wait-timeout 300

airflow-down:
	$(COMPOSE_AIRFLOW) down

airflow-password:
	@$(COMPOSE_AIRFLOW) exec -T airflow-api-server cat /opt/airflow/auth/passwords.json.generated; echo

airflow-runs:
	$(COMPOSE_AIRFLOW) exec -T airflow-scheduler airflow dags list-runs $(DAG_ID) --output table

airflow-stats:
	docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}" $$($(COMPOSE_AIRFLOW) ps -q)

airflow-dag-check:
	$(COMPOSE_AIRFLOW_FILES) --profile airflow-check run --rm airflow-dag-check

airflow-image-info:
	@echo "Image sizes in bytes (uncompressed):"
	@docker image inspect --format "  $(AIRFLOW_IMAGE)  {{.Size}}" $(AIRFLOW_IMAGE)
	@docker image inspect --format "  base apache/airflow:3.3.2-python3.12  {{.Size}}" $(AIRFLOW_BASE_IMAGE)
	@echo "Project venv inside the image, and its largest packages (KiB):"
	@docker run --rm --network none --entrypoint sh $(AIRFLOW_IMAGE) -c 'du -sh /opt/deliverypulse/.venv; du -sk /opt/deliverypulse/.venv/lib/python3.12/site-packages/* | sort -rn | head -12'

airflow-reset:
	bash scripts/airflow_reset.sh

s3-up:
	bash scripts/s3_env_check.sh
	$(COMPOSE_S3) up -d --wait --wait-timeout 180
	$(UV_RUN) python -m src.lake.bootstrap

s3-down:
	$(COMPOSE_S3) down

s3-logs:
	$(COMPOSE_S3) logs --no-color --tail 60

s3-stats:
	docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}" $$($(COMPOSE_S3) ps -q)

lake-export:
	bash scripts/s3_env_check.sh
	$(UV_RUN) python -m src.lake.export --raw-dir $(RAW_DIR) --processed-dir $(PROCESSED_DIR) $(DATASET_ARG)

lake-check:
	bash scripts/s3_env_check.sh
	$(UV_RUN) python -m src.lake.check --raw-dir $(RAW_DIR) --processed-dir $(PROCESSED_DIR) $(DATASET_ARG)

test-s3:
	bash scripts/s3_env_check.sh
	$(UV_RUN) pytest -m s3

test-spark:
	@$(REQUIRE_JAVA_21)
	JAVA_HOME="$(JAVA_21)" $(UV_RUN) --group bench pytest -m spark

bench-data:
	PYTHONPATH=$(CURDIR) $(UV_RUN) --group bench python -m src.bench.shards --shards $(BENCH_SHARDS) --budget-gib $(BENCH_BUDGET_GIB)

benchmark:
	@$(REQUIRE_JAVA_21)
	JAVA_HOME="$(JAVA_21)" PYTHONPATH=$(CURDIR) $(UV_RUN) --group bench python scripts/benchmark.py --largest $(BENCH_LARGEST) --runs $(BENCH_RUNS) --budget-gib $(BENCH_BUDGET_GIB)

bench-clean:
	bash scripts/bench_clean.sh

# USER ONLY except status/logs and the static manifest tests. No chained stage dependencies.
.PHONY: k8s-preflight k8s-build k8s-cache-clean k8s-up k8s-load k8s-host-image-clean k8s-secret k8s-run k8s-replay k8s-status k8s-logs k8s-stats k8s-down test-k8s test-k8s-manifests
k8s-preflight:
	$(K8S_RUN) preflight
k8s-build:
	$(K8S_RUN) build
k8s-cache-clean:
	$(K8S_RUN) cache-clean
k8s-up:
	$(K8S_RUN) up
k8s-load:
	$(K8S_RUN) load
.PHONY: k8s-image-info
k8s-image-info:
	$(K8S_RUN) image-info
k8s-host-image-clean:
	$(K8S_RUN) host-image-clean
k8s-secret:
	$(K8S_RUN) secret
k8s-run:
	$(K8S_RUN) run
k8s-replay:
	$(K8S_RUN) replay
k8s-status:
	$(K8S_RUN) status
k8s-logs:
	$(K8S_RUN) logs
k8s-stats:
	$(K8S_RUN) stats
k8s-down:
	$(K8S_RUN) down
.PHONY: k8s-abort
k8s-abort:
	$(K8S_RUN) abort
test-k8s-manifests:
	$(UV_RUN) --locked pytest tests/unit/test_k8s.py
test-k8s:
	KUBECTL="$(KUBECTL)" KIND="$(KIND)" $(UV_RUN) --locked pytest -x -m k8s

# User-run lifecycle: fixed standalone project, never the main Compose volume.
.PHONY: demo demo-preflight demo-disk demo-down demo-dashboard demo-reset test-demo
demo:
	$(UV_RUN) --locked python -m scripts.demo run
demo-preflight:
	$(UV_RUN) --locked python -m scripts.demo preflight
demo-disk:
	$(UV_RUN) --locked python -m scripts.demo disk
demo-down:
	$(UV_RUN) --locked python -m scripts.demo down
demo-reset:
	$(UV_RUN) --locked python -m scripts.demo reset
demo-dashboard:
	$(UV_RUN) --locked python -m scripts.demo dashboard
test-demo:
	$(UV_RUN) --locked pytest tests/unit/test_demo.py
