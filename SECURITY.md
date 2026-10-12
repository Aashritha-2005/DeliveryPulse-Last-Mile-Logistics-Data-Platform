# Security policy

## Reporting a vulnerability

Please report security issues privately through GitHub's private vulnerability reporting:
open the repository's **Security** tab and choose **Report a vulnerability**. Do not open a
public issue for a security problem.

Include what you found, how to reproduce it, and the commit you tested. You will get a reply
through the same advisory.

## Supported versions

Only the `main` branch is maintained. Fixes are made there.

## Scope and design notes

- All data in this project is synthetic. The repository contains no real personal data and no
  credentials. `.env.example` holds placeholders only, and the real `.env` is git-ignored.
- The PostgreSQL port is bound to `127.0.0.1` only. Application roles follow least privilege
  (see `sql/grants.sql` and `docs/operations.md`).
- Every push and pull request runs secret scanning (working tree and git history), a
  dependency audit, static analysis and a Trivy scan (`.github/workflows/security.yml`).
