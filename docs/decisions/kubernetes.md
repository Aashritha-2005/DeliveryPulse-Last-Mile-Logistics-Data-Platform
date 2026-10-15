# Local Kubernetes pipeline

The local kind cluster runs one control-plane node. The application reuses Compose PostgreSQL
through `host.docker.internal`; it does not create another database or change grants. A
connectivity Job must succeed before the pipeline can run. If that fails, stop and investigate
Docker Desktop routing; do not expose PostgreSQL on another address.

One parameterised Dockerfile serves the generator, pipeline Job and suspended CronJob. This is
the agreed interpretation of separate job Dockerfiles: the runtime dependencies and security
settings are identical, so several images would add maintenance and disk usage. No Deployment
is needed for this finite batch workload. The image uses the existing lockfile with runtime
dependencies only, excluding development, Spark, Kafka and MongoDB groups. It still includes
the core dashboard/object-store dependencies; removing those would require a separate packaging
decision. The image contains no data or credentials.

The node image and Python/uv build images are pinned by manifest index digest. Local builds
have a tag derived from their build inputs. Load and host-image removal require identical,
nonempty ordered rootfs layer digests, plus matching config digests when both stores expose
them. The host ID must still match the saved build record. Docker's containerd image store
can expose an index digest as its ID while CRI exposes a config digest, so comparing those
IDs directly is invalid. Tags alone never establish identity. Manifests use `imagePullPolicy: Never`;
the template's `unbuilt` image must be replaced by the renderer. JSON-formatted YAML permits
static validation without another dependency.

The **privileged kind node is an explicitly approved infrastructure exception**. Application
pods run in a namespace enforcing restricted Pod Security v1.37, as UID/GID 10001, without a
service-account token, capabilities or privilege escalation. Their root filesystem is read-only;
bounded `emptyDir` volumes hold generated files and temporary files. There are no host mounts.
Kubernetes Secrets are local-demo storage, not an encrypted external secret manager. The
user-run secret stage selects only the ETL password and sends it through stdin with server-side
apply; it never saves or prints the value or creates a last-applied annotation containing it.

Docker and cluster mutations require explicit authorization through staged make targets;
deletion is separately approved. Static checks may run independently; status, saved logs and
image information are read-only. All cluster commands select the dedicated kubeconfig and context.
`KUBECTL` defaults to `/opt/homebrew/bin/kubectl`, avoiding Docker Desktop's older bundled copy;
`KIND` defaults to `kind`. Both are overridable. Preflight requires kubectl v1.37.x and kind
v0.33.x. No implicit setup stages run behind another target.

The generator uses the existing 50,000-order seed-42 configuration. Before writes, the pipeline
verifies source-file hashes and all three batch identities against successful database loads.
It then transforms, loads, rebuilds metrics and runs the quality gate. Analytics row counts and
ordered whole-row hashes must be identical before and after each run; mutable audit attempts
are deliberately excluded. A second run must also match the first run's saved baseline. Run
no concurrent writers during this demonstration.

Disk pressure requires staged operation: host free space is checked before and after stages,
and polled during commands. The 11 GiB cancellation threshold leaves a buffer above the 10 GiB
hard floor, but cannot guarantee it against unrelated writes or Docker's asynchronous work.
The Job observer requests Job deletion on disk cancellation; a client or deletion failure can
still leave work running. Inspect and stop it before any further stage. The operator must also
inspect Docker's internal filesystem, since host free
space alone does not prove the VM has room. Completed Job evidence is saved before deleting
the Job and its ephemeral storage; failed Jobs remain for diagnosis. The CronJob stays suspended.

The accepted local runs used a database already holding the dataset: every Kubernetes load
skipped all three batches (0 loaded). This proves the pipeline Job runs on kind and is
idempotent, including metric refresh and the quality gate. It does not prove a first-time
load from Kubernetes into an empty database; the current identity guard requires successful
prior database loads. The live replay and wrong-credential tests passed, and the user reported
green GitHub CI including the pipeline image build/scan on 2026-10-15. A second, independent
from-scratch README reproduction has not been performed; it is deferred to the pre-publishing
check to conserve laptop disk space.
