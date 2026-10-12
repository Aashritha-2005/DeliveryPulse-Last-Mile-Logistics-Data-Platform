#!/usr/bin/env bash
# Determinism check for the synthetic data generator.
#
# Generates the same dataset twice, into two separate folders inside one scratch directory
# OUTSIDE the project (under the resolved temp directory), then compares the sha256 of every
# file. Fails on any difference. The scratch directory is removed on exit, including on
# failure, but only if it is non-empty, a directory and under the resolved temp directory.
#
# Usage: bash scripts/generate_check.sh --orders 50000 --seed 42 [--drivers N]
# Compatible with macOS /bin/bash 3.2.
set -euo pipefail

project_root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$project_root"

tmp_root="$(cd "${TMPDIR:-/tmp}" && pwd -P)"
scratch=""

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

scratch="$(mktemp -d "${tmp_root}/deliverypulse-generate-check.XXXXXX")"
trap cleanup EXIT
echo "Scratch directory created: $scratch"

case "$scratch/" in
    "$project_root"/*)
        echo "ERROR: scratch directory is inside the project; aborting." >&2
        exit 1
        ;;
esac

for run in run1 run2; do
    echo "Generating ${run}..."
    uv run python -m src.ingestion.generate_data "$@" \
        --raw-dir "${scratch}/${run}/raw" --ground-truth-dir "${scratch}/${run}/ground_truth"
    (cd "${scratch}/${run}" && find . -type f -print | LC_ALL=C sort | xargs shasum -a 256) \
        > "${scratch}/${run}.sha256"
done

file_count="$(wc -l < "${scratch}/run1.sha256" | tr -d ' ')"
if [ "$file_count" -eq 0 ]; then
    echo "FAIL: no files were generated." >&2
    exit 1
fi
if diff -u "${scratch}/run1.sha256" "${scratch}/run2.sha256"; then
    echo "PASS: ${file_count} files are byte-identical across two runs."
else
    echo "FAIL: the two runs differ (see diff above)." >&2
    exit 1
fi
