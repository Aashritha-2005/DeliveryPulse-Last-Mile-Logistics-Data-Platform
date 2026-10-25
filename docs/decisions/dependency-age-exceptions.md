# Dependency-age exceptions

The project normally adopts a release only once it is at least 14 days old. On 2026-10-24 the
Security workflow failed on advisories whose only fixes were newer than that, so the owner
accepted the releases below early. Hiding the findings was rejected: no dependency finding is
on an ignore list, and no scanner was weakened. (The only Trivy exception is a configuration
check, AWS-0132, recorded in [aws.md](aws.md).) Ages are counted to 2026-10-24 from the first
PyPI upload or the image push.

| Package or image | Change | Advisory | Upload / push (UTC) | Age | Would have become eligible |
|---|---|---|---|---|---|
| certifi | 2026.7.22 → 2026.10.19 | GHSA-6q48-6c7w-9gw2 (alias PYSEC-2026-4320), critical: bundled emSign roots whose intermediates had leaked keys; fixed in 2026.10.19 | 2026-10-20 | 4 days | 2026-11-03 |
| pillow (streamlit dependency) | 12.3.0 → 13.0.0 | About 34 advisories reported by pip-audit and Trivy against 12.3.0, fixed in 13.0.0; no 12.x backport exists | 2026-10-16 | 8 days | 2026-10-30 |
| streamlit | 1.64.0 → 1.66.1 | None of its own: 1.66.1 is the first release allowing pillow 13 (`pillow<14`); 1.64.0 to 1.66.0 cap it at `<13` | 2026-10-21 | 3 days | 2026-11-04 |
| `python:3.12.15-slim-trixie` (pipeline image base) | 3.12.14 → 3.12.15, index digest `sha256:a0f57064cbdcd6285fa45f51237ab0b35ea8ebaed06a1f5973e89e19dfb86704` | Debian base-package findings (pcre2, openssl) in the image scan; the 3.12.14 tag has had no rebuild since 2026-09-25 | 2026-10-22 (image created 2026-10-21) | 2 days | 2026-11-05 |

**Reasons.**
- certifi changes only the bundled CA certificates, so a regression is unlikely, and the
  advisory is critical.
- Pillow is not imported by project code; the dashboard uses no image functions. The advisories
  still apply to the locked version, and the only fix needs a newer streamlit.
- The base image stays on Python 3.12, so the version policy holds. The scan after the rebuild
  decides whether its Debian findings are resolved.
- Waiting would have kept the Security workflow red for up to 12 days without reducing risk.

**Effect of the upgrade.** streamlit 1.66.1 no longer requires `toml` or `httptools`, so both
left the lock. No other locked version changed.

**Verification (2026-10-24, local).**
- **uv:** a dry run before writing the lock listed exactly the three updates and two removals.
- **Dependency audit:** 86 locked packages, no known vulnerabilities.
- **Other checks:** bandit, secret scan, lint and strict type checks are clean. Tests: 639 unit
  and 163 PostgreSQL integration, with combined coverage of 84.30%.
- **Dashboard:** its query check passed as the read-only role. A headless server answered its
  health check, and a live render showed no exceptions.
- **Breaking changes:** none in the streamlit 1.66 or pillow 13 release notes affects this
  project. That covers dropped Python 3.10, custom-CSS selectors, `secrets.toml` errors and
  removed Pillow image APIs: the project uses Python 3.12, no custom CSS and no secrets file,
  and never imports Pillow.

**CI verification (user-reported, 2026-10-24).** The Security workflow passed on commit
`9c6ffc2` (run #18) and on `1e4f2ec` (run #21), including the pipeline image build and scan;
the Tests workflow passed on both (runs #17 and #20). The image scan gates on fixable HIGH and
CRITICAL findings, so its pass means none remained in the rebuilt image. That covers the pcre2
and openssl findings that failed on `86ecbf0`. The individual scan rows were not reviewed, so
no Debian CVE IDs are recorded here. Unfixed findings are outside the gate, and this pass
does not show that the image has none.

## filelock 4.2.1 (accepted 2026-10-25)

On commit `02f60d1` the dependency audit failed on six filelock advisories published
2026-10-23, all fixed only in 4.2.1. Age is counted to 2026-10-25.

| Package | Change | Advisory | Upload (UTC) | Age | Would have become eligible |
|---|---|---|---|---|---|
| filelock (dependency of `cachecontrol`, used by `pip-audit`) | 4.1.1 → 4.2.1 | CVE-2026-112906, CVE-2026-112907, CVE-2026-112913, CVE-2026-112914, CVE-2026-112937, CVE-2026-112940 (GHSA-v4x8-3wr5-3wq8, High 7.1: affected 3.20.4 to 4.2.0, fixed in 4.2.1) | 2026-10-17 | 8 days | 2026-10-31 |

**Reason.**
- filelock is reached only through the dev-group security tool `pip-audit`. It is not in the
  runtime, the dashboard or the pipeline image.
- The fix is the only release that clears the advisories.
- Waiting would have kept the Security workflow red for six days.

**Effect.** The dry run and the real `uv lock --upgrade-package filelock==4.2.1` both printed
only `filelock v4.1.1 -> v4.2.1`; no other locked version changed.

## Review

Each of these releases is ordinary from its eligibility date. Newer releases, such as certifi
2026.10.23 and filelock 4.2.4, still follow the 14-day rule.
