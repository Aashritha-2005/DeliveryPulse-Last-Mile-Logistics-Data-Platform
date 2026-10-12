#!/usr/bin/env bash
# Secret-scanner self-test (see "Secret scanning" in docs/operations.md).
#
# Creates a scratch directory OUTSIDE the project (under the resolved temp
# directory), writes one synthetic, randomly generated GitHub-token-shaped
# string into it, and asserts that gitleaks, using the project's
# .gitleaks.toml, reports it. The scratch directory is removed on exit,
# including on failure. Nothing is ever written into the project tree.
#
# The token is built at runtime, so no secret-like literal exists in the repo
# and `make scan` stays clean. The token value is never printed.
#
# Compatible with macOS /bin/bash 3.2.
set -euo pipefail

# Distinct exit code for "leaks found", so a gitleaks error (also non-zero)
# is never mistaken for a successful detection.
readonly LEAK_EXIT_CODE=42
readonly TOKEN_LENGTH=36

project_root="$(cd "$(dirname "$0")/.." && pwd -P)"
config_path="${project_root}/.gitleaks.toml"

if ! command -v gitleaks >/dev/null 2>&1; then
    echo "ERROR: gitleaks not found on PATH. Install it with: brew install gitleaks" >&2
    exit 1
fi

tmp_root="$(cd "${TMPDIR:-/tmp}" && pwd -P)"
scratch=""

# Delete the scratch directory only if it is non-empty, is a directory and
# lives under the resolved temp directory. Otherwise delete nothing.
cleanup() {
    local under_tmp="no"
    case "$scratch" in
        "$tmp_root"/?*) under_tmp="yes" ;;
    esac
    if [ -n "$scratch" ] && [ -d "$scratch" ] && [ "$under_tmp" = "yes" ]; then
        rm -rf -- "$scratch"
        echo "Scratch directory removed: $scratch"
    else
        echo "ERROR: refusing to delete scratch path '${scratch}' (empty, not a directory," \
            "or not under ${tmp_root}). Nothing was deleted." >&2
    fi
}

scratch="$(mktemp -d "${tmp_root}/deliverypulse-scan-selftest.XXXXXX")"
trap cleanup EXIT
echo "Scratch directory created: $scratch"

case "$scratch/" in
    "$project_root"/*)
        echo "ERROR: scratch directory is inside the project; aborting." >&2
        exit 1
        ;;
esac

# pipefail is disabled inside this subshell only: `head` closing the pipe
# makes `tr` exit with SIGPIPE, which is expected here, not an error.
random_chars="$(set +o pipefail; LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c "$TOKEN_LENGTH")"
if [ "${#random_chars}" -ne "$TOKEN_LENGTH" ]; then
    echo "ERROR: failed to generate ${TOKEN_LENGTH} random characters." >&2
    exit 1
fi
printf 'GITHUB_TOKEN = "ghp_%s"\n' "$random_chars" > "${scratch}/selftest_settings.py"

echo "Running gitleaks on the scratch directory (findings are redacted)..."
rc=0
gitleaks dir "$scratch" --config "$config_path" --redact --no-banner \
    --exit-code "$LEAK_EXIT_CODE" || rc=$?

if [ "$rc" -eq "$LEAK_EXIT_CODE" ]; then
    echo "PASS: gitleaks detected the synthetic secret (exit code ${rc})."
else
    echo "FAIL: expected gitleaks exit code ${LEAK_EXIT_CODE} (leak found), got ${rc}." >&2
    exit 1
fi
