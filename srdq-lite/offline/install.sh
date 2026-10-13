#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
VENV=${SRDQ_LITE_VENV:-"$ROOT/.venv"}

if [[ $(uname -s) != Linux || $(uname -m) != x86_64 ]]; then
  echo "ERROR: only Linux x86_64 is supported" >&2
  exit 1
fi

if [[ -n ${PYTHON_BIN:-} ]]; then
  PYTHON=$PYTHON_BIN
else
  PYTHON=""
  for candidate in python3.12 python3.11; do
    if command -v "$candidate" >/dev/null 2>&1; then
      PYTHON=$(command -v "$candidate")
      break
    fi
  done
fi

if [[ -z ${PYTHON:-} || ! -x $PYTHON ]]; then
  echo "ERROR: Python 3.11 or 3.12 is required; set PYTHON_BIN if it is not on PATH" >&2
  exit 1
fi

VERSION=$($PYTHON -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
if [[ $VERSION != 3.11 && $VERSION != 3.12 ]]; then
  echo "ERROR: $PYTHON is Python $VERSION; only 3.11 and 3.12 are supported" >&2
  exit 1
fi

PIP_WHEEL=$(find "$ROOT/wheels" -maxdepth 1 -type f -name 'pip-*.whl' -print -quit)
if [[ -z $PIP_WHEEL ]]; then
  echo "ERROR: bundled pip wheel is missing" >&2
  exit 1
fi

rm -rf "$VENV.tmp"
if ! $PYTHON -m venv --without-pip "$VENV.tmp"; then
  echo "ERROR: $PYTHON cannot create a venv; install its standard-library venv component" >&2
  exit 1
fi
PYTHONPATH="$PIP_WHEEL" PIP_DISABLE_PIP_VERSION_CHECK=1 "$VENV.tmp/bin/python" -m pip install \
  --no-index --find-links "$ROOT/wheels" pip -r "$ROOT/requirements.txt"
"$VENV.tmp/bin/python" -c 'import pymysql, yaml; print("dependency check: OK")'
rm -rf "$VENV"
mv "$VENV.tmp" "$VENV"

echo "Installed srdq-lite into $VENV"
echo "Run: $ROOT/run.sh --help"
