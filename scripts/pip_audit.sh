#!/usr/bin/env bash
# Audit every locked Python dependency (runtime and all groups) for known vulnerabilities.
#
# The uv environment has no pip, so pip-audit checks the exact, hash-pinned versions exported
# from uv.lock instead of the installed environment. The export goes to a scratch file under
# the resolved temp directory, removed on exit. Exits non-zero on any known vulnerability.
#
# Compatible with macOS /bin/bash 3.2 and Linux bash.
set -euo pipefail

project_root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$project_root"

tmp_root="$(cd "${TMPDIR:-/tmp}" && pwd -P)"
requirements="$(mktemp "${tmp_root}/deliverypulse-audit.XXXXXX")"
trap 'rm -f -- "$requirements"' EXIT

uv export --frozen --all-groups --no-emit-project --format requirements-txt --quiet --output-file "$requirements"
echo "Auditing $(grep -c '==' "$requirements") locked packages from uv.lock..."
uv run pip-audit --strict --disable-pip --requirement "$requirements"
