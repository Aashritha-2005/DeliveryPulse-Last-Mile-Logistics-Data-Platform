# AWS proof runbook (Step 17)

A one-off, owner-run proof that the existing S3 code path and one Athena query work on real AWS,
followed by a verified teardown. It runs only in **ap-southeast-2 (Sydney)**, on an account on
the AWS **Free plan** (no charges unless the plan is upgraded; usage is estimated at well under
USD 0.01 of credits). Nothing here runs in CI except `terraform fmt -check` and `validate`.

## What gets created

| Resource | Settings |
|---|---|
| S3 bucket `deliverypulse-proof-<8 hex>` | Block Public Access, bucket owner enforced, SSE-S3 (no KMS), TLS-only policy, no versioning, objects expire after 7 days, Athena results after 1 day, incomplete uploads aborted after 1 day, `force_destroy` |
| Glue database `deliverypulse_proof`, table `cleaned_orders` | Parquet, partition projection on `date` (no crawler) |
| Athena workgroup `deliverypulse-proof` | Enforced settings, results in `athena-results/`, SSE-S3, **100 MiB per-query scan limit** (cancels larger queries), `force_destroy` |
| IAM role `deliverypulse-proof-pipeline` | Assumable only by the owner's `aws login` role; one bucket (four prefixes), one workgroup, read-only on one Glue database and table; 1-hour sessions |

## Rules

- Never click **Upgrade plan**; never enable Organizations, Identity Center or Control Tower.
- No access keys anywhere: credentials come from `aws login` (12 hours) and an assumed role.
- `S3_ALLOW_AWS=true` is typed on the command line, never stored in `.env`.
- Terraform state and plan files contain the account ID; they are git-ignored. Delete them
  after the teardown is verified.
- Check **Console home → Cost and usage** before applying and after the teardown.

## 0. Prerequisites

- Budget `deliverypulse-guard` exists (USD 1, monthly).
- AWS CLI 2.32.0 or later; `aws login --profile deliverypulse` works.
- Terraform **1.16.x, at least 1.16.5** (`versions.tf`; newer releases were under 14 days old
  when this was written).
- The isolated demo (`make demo`) has run, so `data/demo/` and the demo database hold the
  5,000-order dataset `seed-42_orders-5000_cfg-4e94aafb`.

## 1. Create the resources (project login)

```bash
cd ~/DeliveryPulse/infra/aws
export AWS_PROFILE=deliverypulse
terraform init
terraform validate
terraform plan -out=proof.tfplan
```

Review the plan: only the resources above, `0 to change, 0 to destroy`. Then:

```bash
terraform apply proof.tfplan
```

## 2. Export the resource names (shell only)

```bash
export S3_BUCKET="$(terraform output -raw bucket)" S3_REGION=ap-southeast-2
export ATHENA_WORKGROUP="$(terraform output -raw athena_workgroup)"
export ATHENA_DATABASE="$(terraform output -raw glue_database)"
export AWS_PROOF_ROLE_NAME="$(terraform output -raw pipeline_role_name)"
```

## 3. Switch to the pipeline role

This writes a profile into your own `~/.aws/config` (outside the repository):

```bash
aws configure set role_arn "$(terraform output -raw pipeline_role_arn)" --profile dp-pipeline
aws configure set source_profile deliverypulse --profile dp-pipeline
aws configure set region ap-southeast-2 --profile dp-pipeline
cd ~/DeliveryPulse
export AWS_PROFILE=dp-pipeline
aws sts get-caller-identity --query Arn --output text | sed -E 's/[0-9]{12}/<account>/'
```

Expected: `arn:aws:sts::<account>:assumed-role/deliverypulse-proof-pipeline/...`. If the CLI
cannot use the `aws login` profile as `source_profile`, stop and report the error.

## 4. Permission checks (pipeline role)

```bash
S3_ALLOW_AWS=true make aws-preflight
S3_ALLOW_AWS=true make aws-negative-check
```

Both must end with `all checks passed`. The second confirms five actions outside the policy
are denied: listing all buckets, the default `primary` workgroup, listing Glue databases,
listing IAM roles and writing outside the four lake prefixes.

## 5. Export the lake and run the query (pipeline role)

The demo database must be running (`make demo` replays without changing business data):

```bash
make demo
S3_ALLOW_AWS=true make aws-export
S3_ALLOW_AWS=true make aws-athena
```

`aws-export` refuses to run unless the demo database holds exactly the orders of the demo's
cleaned files (dataset-identity guard). `aws-athena` prints the late-delivery rate by city,
bytes scanned, timings, the cost estimate and whether the result matches PostgreSQL, and
writes `data/aws/athena-<UTC>.json`. Its output contains no account ID or ARN: take the
terminal screenshot here, and check it yourself before saving it to `docs/images/`.

Then stop the demo database: `make demo-down`.

## 6. Teardown (project login)

```bash
export AWS_PROFILE=deliverypulse
cd ~/DeliveryPulse/infra/aws
terraform plan -destroy -out=destroy.tfplan
terraform apply destroy.tfplan
terraform state list
cd ~/DeliveryPulse
S3_ALLOW_AWS=true make aws-verify-teardown
```

`terraform state list` must print nothing; the teardown check must end with
`all checks passed` (bucket, workgroup, Glue database and role absent; nothing tagged
`project=deliverypulse-aws-proof` left in the region). The budget and the account are kept.

Afterwards, remove the `dp-pipeline` profile from `~/.aws/config` and delete the local state
and plan files in `infra/aws/` (`terraform.tfstate*`, `*.tfplan`, `.terraform/`).

## 7. Cost (24-48 hours later)

Record the **Cost and usage** panel (credits remaining, current month) and the Bills page.

## Measurements to record

Date, Terraform and provider versions, plan counts (add/change/destroy), the five permission
checks, export objects/rows/bytes, Athena state, bytes scanned, engine and total time, result
rows, match with PostgreSQL, estimated cost, destroy counts, the teardown check and the billed
amount.
