#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
VENV=${SRDQ_LITE_VENV:-"$ROOT/.venv"}
if [[ ! -x $VENV/bin/python ]]; then
  echo "ERROR: not installed; run $ROOT/install.sh first" >&2
  exit 1
fi
cd "$ROOT"
exec "$VENV/bin/python" -m srdq_lite "$@"
