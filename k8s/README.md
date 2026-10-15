# Manifest templates

These files use JSON syntax, which is valid YAML. Use the staged make targets in
[operations](../docs/operations.md#local-kubernetes-staged-runbook), rather than applying the
templates directly. `unbuilt` is deliberately unusable: the renderer replaces it only after
the content-tagged image is loaded and its ordered rootfs layer digests verified inside the
node (also config digests when exposed on both sides).

The CronJob is suspended. The namespace enforces restricted Pod Security. No Secret values
are stored here. The user-run `make k8s-secret` selects the ETL credential in memory, applies
it through stdin and checks connectivity before authorizing a pipeline run.
