#!/usr/bin/env bash
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
VENV="$ROOT/.venv"
PIP_WHEEL="$ROOT/wheels/pip-24.0-py3-none-any.whl"

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

if [[ $(uname -s) != Linux || $(uname -m) != x86_64 ]]; then
  fail 'only Linux x86_64 is supported.'
fi

healthy_venv() {
  [[ -f "$VENV/pyvenv.cfg" && -x "$VENV/bin/python" ]] || return 1
  "$VENV/bin/python" -I - "$VENV" <<'PY'
import importlib.metadata
import os
import platform
import re
import struct
import sys

if (sys.version_info[:2] not in ((3, 11), (3, 12))
        or platform.system() != "Linux" or platform.machine() != "x86_64"
        or struct.calcsize("P") != 8):
    raise SystemExit("venv requires 64-bit Linux x86_64 Python 3.11 or 3.12")
if (sys.prefix == sys.base_prefix
        or os.path.realpath(sys.prefix) != os.path.realpath(sys.argv[1])):
    raise SystemExit("interpreter does not belong to this venv")
try:
    import pymysql
    import yaml
    for package, minimum, maximum in (
        ("PyMySQL", (1, 1), (2, 0)),
        ("PyYAML", (6, 0), (7, 0)),
    ):
        version = importlib.metadata.version(package)
        match = re.match(r"^(\d+)\.(\d+)(?:\.|$)", version)
        if not match or not minimum <= tuple(map(int, match.groups())) < maximum:
            raise RuntimeError(f"unsupported {package} version: {version}")
except Exception as exc:
    raise SystemExit(f"dependency check failed: {exc}")
PY
}

if [[ -e "$VENV" || -L "$VENV" ]]; then
  if healthy_venv; then
    printf 'Reusing healthy venv: %s\n' "$VENV"
    exit 0
  fi
  fail "existing venv is unhealthy or unrecognized: $VENV; inspect it manually. No files were changed."
fi

PYTHON=''
if [[ -n ${PYTHON_BIN:-} ]]; then
  PYTHON=$(command -v -- "$PYTHON_BIN") || fail "Python executable not found: $PYTHON_BIN"
else
  for candidate in python3.12 python3.11 python3; do
    if resolved=$(command -v -- "$candidate") && "$resolved" -I -c \
      'import sys; sys.exit(sys.version_info[:2] not in ((3, 11), (3, 12)))' >/dev/null 2>&1; then
      PYTHON=$resolved
      break
    fi
  done
fi
[[ -n "$PYTHON" && -x "$PYTHON" ]] || fail 'Python 3.11 or 3.12 is required; set PYTHON_BIN to its executable.'
"$PYTHON" -I -c 'import platform, struct, sys, venv; sys.exit(sys.version_info[:2] not in ((3, 11), (3, 12)) or platform.system() != "Linux" or platform.machine() != "x86_64" or struct.calcsize("P") != 8)' \
  || fail 'Python must be 64-bit Linux x86_64 version 3.11 or 3.12 with the stdlib venv module.'
[[ -f "$PIP_WHEEL" ]] || fail "bundled pip 24.0 wheel is missing: $PIP_WHEEL"
[[ -f "$ROOT/requirements.txt" ]] || fail 'requirements.txt is missing.'

# Moving a venv after creation breaks its absolute interpreter paths.
mkdir -- "$VENV" || fail "cannot create venv; no existing directory will be overwritten: $VENV"
installation_failed() {
  printf 'ERROR: installation failed; new directory retained for inspection: %s\n' "$VENV" >&2
}
trap installation_failed ERR
"$PYTHON" -I -m venv --without-pip "$VENV"
# PIP_PYTHON can redirect pip installation to another interpreter.
env -u PIP_PYTHON PIP_CONFIG_FILE=/dev/null "$VENV/bin/python" -I - "$PIP_WHEEL" \
  --isolated --disable-pip-version-check --no-input install \
  --no-index --find-links "$ROOT/wheels" --no-cache-dir \
  pip==24.0 -r "$ROOT/requirements.txt" <<'PY'
import runpy
import sys

sys.path.insert(0, sys.argv.pop(1))
runpy.run_module("pip", run_name="__main__")
PY
if ! healthy_venv; then
  installation_failed
  exit 1
fi
trap - ERR
printf 'Installed srdq-partition into %s\n' "$VENV"
printf 'Before first run, copy "%s/config.example.yaml" to "%s/config.yaml" and edit it.\n' "$ROOT" "$ROOT"
printf 'Run: "%s/run.sh"\n' "$ROOT"
