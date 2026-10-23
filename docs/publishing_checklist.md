# Publishing checklist

The repository is already pushed. Apply this checklist to the final candidate commit before
presenting it publicly. All version-control, hosting and Docker-changing actions below are
performed by the repository owner, with explicit approval of each destructive operation.

## Evidence and reproduction

- [ ] Preserve the measured scope: 38.708 s is the current 5,000-order, 500-driver, seed-42
  configuration (`seed-42_orders-5000_cfg-4e94aafb`, 2026-10-23), with an empty demo database
  and cached image (pull skipped). The earlier 40.106 s figure was a different, 83-driver
  configuration (`seed-42_orders-5000_cfg-99b76f46`); never present it as the current timing.
- [ ] State that dependency installation and the PostgreSQL image download are excluded.
  Download-inclusive timing was deliberately skipped to conserve disk; there is no general
  "within 10 minutes" setup claim. No image removal or additional timing run is scheduled.
- [ ] If cleaning up, verify only demo resources are removed and the main database remains.
- [ ] Review results and limitations against original benchmark/quality artifacts. Keep old
  production measurements separate from the new demo; label all derived arithmetic and estimates.
- [ ] Review the existing screenshot visually for credentials, personal paths, names, account
  icons, notifications and prohibited attribution; it remains in the README's first screen.
- [ ] Optional before publishing: make a 60–90 s terminal/dashboard recording. **Not done**;
  no nonexistent media is embedded or linked. If made later, perform the same visual review,
  label edits or speed changes, and distinguish recording duration from execution duration.
- [ ] Check Mermaid rendering, links, badges and the screenshot on the hosting site.
- [ ] Optional: independently reproduce the [Kubernetes runbook](../k8s/README.md), one stage
  at a time, with its disk gates and separately approved cleanup. This is user-run, does not
  start with the core demo, and is **not required for core-demo acceptance**. The confirmed
  Kubernetes runs establish replay against a populated database, not first-time loading.

## Working tree and history

Run from the repository root yourself:

```bash
git status --ignored
git ls-files -s docker/postgres/init-deliverypulse.sh scripts/
make scan
gitleaks git . --redact
```

- [ ] Confirm local credentials, private instructions/progress, datasets, kubeconfig, evidence
  and caches are ignored and untracked. Inspect tracked names as well as ignored status: an
  ignore rule does not untrack an already tracked file.
- [ ] Shell scripts and the PostgreSQL init script retain mode `100755`.
- [ ] Both tree and full-history secret scans pass. A clean tree alone proves nothing about
  deleted history. If a real credential appears, stop and rotate it; history changes need a
  separate deliberate review and approval.
- [ ] Supply the prohibited-name/attribution terms **at run time**, as a case-insensitive
  extended regular expression. Do not put the actual terms into committed files. Review every
  matching tracked-file line, commit message and PR body, including co-author attribution.

For tracked files and commit messages, these user-run commands preserve grep's distinction
between matches (0), no matches (1) and errors (>1); do not turn errors into a clean result:

```bash
printf 'Enter the review pattern: '
IFS= read -r REVIEW_PATTERN
if test -n "$REVIEW_PATTERN"; then
  git grep -n -i -E -- "$REVIEW_PATTERN" -- .
  set -o pipefail
  git log --all --format=%B | grep -n -i -E -- "$REVIEW_PATTERN"
else
  printf 'Empty pattern: no search performed.\n'
fi
```

Stop if the pattern is empty. Investigate command errors rather than interpreting them as
zero matches. Review PR descriptions
and comments on the hosting site with the same terms; the tracked-file command does not cover
PR bodies. Binary screenshots and GIFs require the visual review above.

## Verification and dependencies

- [ ] Run format before lint, unit/demo tests, PostgreSQL integration and coverage, secret scan,
  dependency audit, static security checks and workflow validation. Base-only unit tests must
  remain service-free. Full lint requests the locked optional groups without weakening checks.
- [ ] Record current results and confirm both workflows green on the **final pushed SHA**.
  Review skipped jobs explicitly; a local workflow syntax check is not a GitHub run.
- [ ] Re-run affected optional suites only with their prerequisites and resource budget, one
  heavy profile at a time. Never rerun scale measurements just to fill a missing claim.
- [ ] Review image digest pins and action SHA pins; do not silently substitute newer images.
- [ ] Reject the proposed `python:3.14.7-slim-trixie` bump: the project pins Python 3.12.
  Keep any replacement proposal within the approved Python minor version.
- [ ] For uv 0.13.1 and botocore proposals, independently verify upstream release/upload dates
  and apply a 14-day minimum age **on the review date** (2026-10-15 cutoff: 2026-10-01).
  Review every changed lock entry and boto3/botocore compatibility. Cooldown configuration
  alone does not prove eligibility. Leave a too-new or incompatible proposal unmerged.
- [ ] Any dependency edit still needs approval before `uv add` or `uv lock`. Inspect the complete
  change, run the relevant tests/builds/scans, then let the owner decide whether to merge.
  No automatic merge is part of this checklist.
- [ ] Confirm branch protection, dependency alerts and private vulnerability reporting.

## Presentation and resume

- [ ] Lead with PostgreSQL, data quality and idempotency. Every number must map to a confirmed
  run with its dataset, command, date and hardware. The load time is not an end-to-end time.
- [ ] State that Spark is a benchmark subset, Kafka is a local landing demo, MongoDB is
  SSPL-licensed, Kubernetes demonstrates local replay, and AWS was deliberately skipped.
- [ ] Do not imply Java, Hadoop/HDFS, cloud execution or production multi-node deployment.
- [ ] Prefer a few defensible resume bullets over listing unmeasured features. Keep the
  screenshot, architecture, data model, quality report and full query plans easy to find.
- [ ] Review the final changed-file list and commit message yourself, then push. Do not blindly
  rebase or merge open dependency changes into an otherwise verified candidate.
