#!/usr/bin/env bash
# Delete one generated dataset: exactly data/raw/<name> and data/ground_truth/<name>.
#
# Refuses empty names, names containing "/" or "..", names starting with ".", symlinks, and any
# path that does not resolve directly inside data/raw or data/ground_truth of this project.
# Prints both paths and requires typing the dataset name to confirm, then restores write
# permission (raw files are read-only) and deletes.
#
# Usage: bash scripts/clean_dataset.sh <dataset name>
# Compatible with macOS /bin/bash 3.2.
set -euo pipefail

name="${1:-}"

project_root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$project_root"

fail() {
    echo "ERROR: $*" >&2
    exit 1
}

[ -n "$name" ] || fail "no dataset name given. Usage: make clean-dataset DATASET=<name>"
case "$name" in
    */* | *..* | .*) fail "invalid dataset name '${name}' (no '/', no '..', no leading '.')." ;;
esac
case "$name" in
    *[!A-Za-z0-9._-]*) fail "invalid dataset name '${name}' (allowed: letters, digits, . _ -)." ;;
esac

targets=""
for parent in data/raw data/ground_truth; do
    parent_abs="$(cd "$parent" 2>/dev/null && pwd -P)" || fail "folder ${parent} not found."
    path="${parent}/${name}"
    if [ -L "$path" ]; then
        fail "${path} is a symlink; refusing to delete."
    fi
    if [ -e "$path" ]; then
        [ -d "$path" ] || fail "${path} is not a directory; refusing to delete."
        resolved_parent="$(cd "$path/.." && pwd -P)"
        [ "$resolved_parent" = "$parent_abs" ] || fail "${path} is not inside ${parent}."
        targets="${targets} ${path}"
    fi
done

if [ -z "$targets" ]; then
    echo "Dataset '${name}' not found in data/raw or data/ground_truth; nothing to delete."
    exit 0
fi

echo "=================================================================="
echo " WARNING: THIS PERMANENTLY DELETES A GENERATED DATASET"
echo "=================================================================="
for path in $targets; do
    echo " ${project_root}/${path}"
done
echo " Raw files are immutable by design; delete only obsolete datasets."
echo "=================================================================="

if [ ! -t 0 ]; then
    fail "no interactive terminal to confirm. Nothing was deleted."
fi

printf 'Type the dataset name to confirm (anything else aborts): '
answer=""
read -r answer || true
if [ "$answer" != "$name" ]; then
    echo "Aborted. Nothing was deleted." >&2
    exit 1
fi

for path in $targets; do
    # No "--": BSD chmod stops option parsing at the mode and would treat "--" as a file name.
    # Safe because $path is always "data/raw/<name>" or "data/ground_truth/<name>" with a
    # validated name, so it can never start with "-".
    chmod -R u+w "$path"
    rm -rf -- "$path"
    echo "Deleted ${path}"
done
