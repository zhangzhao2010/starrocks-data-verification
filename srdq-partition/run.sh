#!/usr/bin/env bash
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
PYTHON="$ROOT/.venv/bin/python"

if [[ ! -x "$PYTHON" ]]; then
  printf 'ERROR: not installed; run "%s/install.sh" first.\n' "$ROOT" >&2
  exit 1
fi
if [[ ! -f "$ROOT/partition_check.py" ]]; then
  printf 'ERROR: missing application: %s/partition_check.py\n' "$ROOT" >&2
  exit 1
fi

if (( $# == 0 )); then
  set -- -c "$ROOT/config.yaml"
fi
# Keep the caller's cwd so explicitly supplied relative paths stay relative to it.
exec "$PYTHON" "$ROOT/partition_check.py" "$@"
