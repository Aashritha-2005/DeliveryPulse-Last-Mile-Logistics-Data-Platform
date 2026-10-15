# Manifest templates

These files use JSON syntax, which is valid YAML. Use the staged make targets in
[operations](../docs/operations.md#local-kubernetes-staged-runbook), rather than applying the
templates directly. `unbuilt` is deliberately unusable: the renderer replaces it only after
the content-tagged image is loaded and its ordered rootfs layer digests verified inside the
node (also config digests when exposed on both sides).

The CronJob is suspended. The namespace enforces restricted Pod Security. No Secret values
are stored here. The user-run `make k8s-secret` selects the ETL credential in memory, applies
it through stdin and checks connectivity before authorizing a pipeline run.

## Run the pipeline on local Kubernetes (kind)

The measured one-node kind Job ran against Compose PostgreSQL **already holding the dataset**.
Every Kubernetes load skipped all three batches: **0 loaded, 3 skipped**. This demonstrates
that the pipeline Job runs on kind and is idempotent, including metric refresh and the quality
gate. It does **not** demonstrate a first-time Kubernetes load into an empty database. The
source-identity guard deliberately requires the three successful database batches first.
See [measurements](../docs/benchmarks.md#local-kubernetes-measured-local-run-2026-10-15) and
[design/security decisions](../docs/decisions/kubernetes.md).

Prerequisites on the tested Mac: Docker Desktop running; kind **v0.33.x** (tested v0.33.0);
Homebrew kubectl **v1.37.x** at `/opt/homebrew/bin/kubectl` (tested v1.37.1); `uv`, Make and
the locked development environment from Quick start. Docker Desktop's bundled kubectl
v1.36.1 is rejected. `KUBECTL` and `KIND` are overridable Make variables for other hosts.
Configure the existing PostgreSQL settings and ETL password locally as described in Quick
start. Never print or commit credentials. Kafka, MongoDB, Airflow and the object store must
be stopped. No concurrent pipeline writer may run during the comparison.

Run these prerequisites from the repository root. The default 50,000-order seed-42 dataset
must already be generated and loaded; these commands establish that prerequisite if needed.
Do not reset an existing database or switch its dataset for this demo.

```bash
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
uv sync --locked
make up
make generate ORDERS=50000 SEED=42
make load
make db-counts
```

Expect PostgreSQL healthy, dataset `seed-42_orders-50000_cfg-449e309b`, three successful
batches (or three skips on replay), 49,450 analytics orders and 439,822 delivery events.
If source identity or counts differ, stop and investigate before Kubernetes setup.

Start with **at least 16 GiB physical host free space recommended**; the build refuses below
13.5 GiB. The hard floor is 10 GiB and cancellation threshold is 11 GiB. Every stage checks
again: cluster creation requires 12.8 GiB; load requires `11 + 2 × image size in GiB + 0.3`;
Jobs require 12.5 GiB. Stop at any failed checkpoint. Docker VM free blocks and cache-prune
totals are not reclaimed host space. Recheck actual host space after cleanup.

Run **one command at a time**, reviewing its output before the next. The PATH export above
applies to every command. Do not paste the whole sequence as an unattended script.

```bash
make k8s-preflight
make k8s-build
make k8s-cache-clean
make k8s-up
make k8s-load
make k8s-image-info
make k8s-secret
make k8s-run
make db-counts
make k8s-replay
make db-counts
make test-k8s
make k8s-status
make k8s-stats
```

| Command | What to check before continuing |
|---|---|
| `k8s-preflight` | Disk checkpoint; selected kind/kubectl versions; Docker architecture/memory; `Kubernetes preflight: passed (no cluster created)` |
| `k8s-build` | JSON with the content-derived tag, image ID, byte size and `loaded: false`; successful final disk checkpoint. Output may be quiet during the build |
| `k8s-cache-clean` | Type `DELETE` to remove unused build cache, including cache from other projects; no volumes or tagged pipeline image removed. Docker reports its reclaimed total; physical host space may not increase immediately |
| `k8s-up` | Actual `create_seconds`, node filesystem usage and final disk checkpoint. Creates one pinned node, localhost API and a private kubeconfig under ignored `data/k8s/` |
| `k8s-load` | Ordered rootfs layers match the saved host build; config digests match when both stores expose them. Different/missing layers fail. Never accept tags alone |
| `k8s-image-info` | Read-only host/node image identifiers and ordered layer arrays; no rebuild or export |
| `k8s-secret` | `Kubernetes PostgreSQL connectivity: passed`; Secret values not shown; CronJob suspended. Selects only the ETL credential, sends it on stdin and never writes a Secret file |
| `k8s-run` | Dataset identity verified before writes; `0 batches loaded, 3 already loaded (skipped)`; 50 quality checks with 0 blocking failures and 0 warnings; analytics counts/content hashes unchanged |
| `k8s-replay` and surrounding `db-counts` | All business tables and aggregates identical, including hashes; audit quality-run counts may increase |
| `test-k8s` | Both live tests pass: replay equality and a clean wrong-credential failure. Only temporary test Secret/Jobs are cleaned up |
| `k8s-status` | CronJob suspended, no active successful Jobs left; `secret/deliverypulse-db` confirms the real Secret by name only |
| `k8s-stats` | Image size, saved Job timings, node memory samples, physical host free bytes and node filesystem usage. Samples are not continuous application peaks |

Successful Jobs save evidence before their built-in cleanup removes temporary pod storage.
The CronJob stays suspended. **Keep the host pipeline image until the final cleanup**, so
recreating a failed cluster does not require rebuilding it. For diagnosis use `make k8s-status`
and `make k8s-logs`; stop on a failure, especially connectivity or identity mismatch. Never
widen PostgreSQL's localhost port binding to resolve a connection failure.

Final cleanup, after reviewing/saving results (one command at a time):

```bash
make k8s-host-image-clean
make k8s-down
```

The first command re-verifies the node copy and removes only the host pipeline image tag.
It must precede cluster deletion. The second asks for `DELETE` and removes the throwaway
cluster, its Kubernetes Secrets and remaining pods; it leaves Compose volumes and PostgreSQL
untouched. Both print fresh disk checkpoints. Removing Docker data may not immediately return
physical host space. Obtain explicit deletion approval when running through an automation
session. See the [operations runbook](../docs/operations.md#local-kubernetes-staged-runbook) for
disk recovery and limitations. The local staged run and CI pipeline image build/scan passed
on 2026-10-15. A second, independent from-scratch reproduction has not been performed. It is an optional,
user-run pre-publishing check with the same disk gates; it is not required for the core demo.


