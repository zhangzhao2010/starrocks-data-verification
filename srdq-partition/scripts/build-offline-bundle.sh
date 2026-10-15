#!/usr/bin/env bash
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
PYTHON=${PIP_PYTHON:-${PYTHON_BIN:-python3}}
NAME='srdq-partition-offline-1.0.2-linux-x86_64'
DIST="$ROOT/dist"

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

command -v -- "$PYTHON" >/dev/null 2>&1 || fail "Python executable not found: $PYTHON"
for file in partition_check.py config.example.yaml requirements.txt install.sh run.sh scripts/build-offline-bundle.sh; do
  [[ -f "$ROOT/$file" && ! -L "$ROOT/$file" ]] || fail "missing or symlinked source file: $ROOT/$file"
done
if [[ -n ${WHEEL_DIR:-} && ! -d "$WHEEL_DIR" ]]; then
  fail "WHEEL_DIR is not a directory: $WHEEL_DIR"
fi

WORK=$(mktemp -d "${TMPDIR:-/tmp}/srdq-partition-build.XXXXXXXX")
cleanup() {
  # WORK is exclusively the directory returned by this invocation of mktemp.
  if [[ -n ${WORK:-} && -d "$WORK" ]]; then
    rm -rf -- "$WORK"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
STAGE="$WORK/$NAME"
mkdir -p -- "$STAGE/wheels" "$STAGE/scripts"

if [[ -n ${WHEEL_DIR:-} ]]; then
  WHEEL_SOURCE=$WHEEL_DIR
  [[ -f "$WHEEL_SOURCE/pip-24.0-py3-none-any.whl" ]] \
    || fail 'WHEEL_DIR must contain pip-24.0-py3-none-any.whl.'
  cp -- "$WHEEL_SOURCE/pip-24.0-py3-none-any.whl" "$STAGE/wheels/"
else
  WHEEL_SOURCE="$STAGE/wheels"
  env -u PIP_PYTHON "$PYTHON" -m pip --version >/dev/null 2>&1 \
    || fail 'online builds need pip; set PIP_PYTHON or use WHEEL_DIR for a fully offline build.'
  for PYVER in 311 312; do
    env -u PIP_PYTHON PIP_CONFIG_FILE=/dev/null "$PYTHON" -m pip --isolated --disable-pip-version-check --no-input download \
      --only-binary=:all: --platform manylinux2014_x86_64 --implementation cp \
      --python-version "$PYVER" --abi "cp$PYVER" --no-deps --no-cache-dir \
      --dest "$STAGE/wheels" -r "$ROOT/requirements.txt"
  done
  env -u PIP_PYTHON PIP_CONFIG_FILE=/dev/null "$PYTHON" -m pip --isolated --disable-pip-version-check --no-input download \
    --only-binary=:all: --python-version 311 --implementation cp --abi cp311 \
    --platform manylinux2014_x86_64 --no-deps --no-cache-dir \
    --dest "$STAGE/wheels" pip==24.0
fi

# Bundled packaging lets offline builds run without system pip.
"$PYTHON" -I - "$WHEEL_SOURCE" "$STAGE/wheels" <<'PY'
import shutil
import sys
import zipfile
from pathlib import Path

source, destination = (Path(arg).resolve() for arg in sys.argv[1:])
pip_wheel = destination / "pip-24.0-py3-none-any.whl"
sys.path.insert(0, str(pip_wheel))
from pip._vendor.packaging.specifiers import SpecifierSet
from pip._vendor.packaging.utils import InvalidWheelFilename, parse_wheel_filename
from pip._vendor.packaging.version import Version

required = {"pip", "pymysql", "pyyaml-cp311", "pyyaml-cp312"}
found = set()
for wheel in sorted(source.glob("*.whl")):
    if not wheel.is_file():
        continue
    try:
        name, version, _, tags = parse_wheel_filename(wheel.name)
    except InvalidWheelFilename:
        continue
    supported = set()
    if name == "pip" and version == Version("24.0"):
        if any(t.interpreter == "py3" and t.abi == "none" and t.platform == "any" for t in tags):
            supported.add("pip")
    elif name == "pymysql" and version in SpecifierSet(">=1.1,<2"):
        if any(t.interpreter == "py3" and t.abi == "none" and t.platform == "any" for t in tags):
            supported.add("pymysql")
    elif name == "pyyaml" and version in SpecifierSet(">=6,<7"):
        for target in ("cp311", "cp312"):
            if any(t.interpreter == target and t.abi == target
                   and t.platform == "manylinux2014_x86_64" for t in tags):
                supported.add(f"pyyaml-{target}")
    if not supported:
        continue
    with zipfile.ZipFile(wheel) as archive:
        bad_member = archive.testzip()
        if bad_member is not None:
            raise SystemExit(f"ERROR: corrupt wheel: {wheel}: {bad_member}")
        if not any(member.endswith(".dist-info/METADATA") for member in archive.namelist()):
            raise SystemExit(f"ERROR: wheel has no package metadata: {wheel}")
    if source != destination:
        shutil.copyfile(wheel, destination / wheel.name)
    found.update(supported)
missing = required - found
if missing:
    raise SystemExit("ERROR: missing compatible wheels: " + ", ".join(sorted(missing)))
print("Wheel validation: Python 3.11 / 3.12 Linux x86_64 OK")
PY

# Explicit allowlist: no real config.yaml, output, credentials, caches or venv.
cp -- "$ROOT/partition_check.py" "$ROOT/config.example.yaml" "$ROOT/requirements.txt" \
  "$ROOT/install.sh" "$ROOT/run.sh" "$STAGE/"
cp -- "$ROOT/scripts/build-offline-bundle.sh" "$STAGE/scripts/"
chmod 0755 "$STAGE/install.sh" "$STAGE/run.sh" "$STAGE/scripts/build-offline-bundle.sh"

tar --sort=name --owner=0 --group=0 --numeric-owner \
  -czf "$WORK/$NAME.tar.gz" -C "$WORK" "$NAME"
"$PYTHON" -I - "$WORK/$NAME.tar.gz" <<'PY'
import hashlib
import sys
from pathlib import Path

archive = Path(sys.argv[1])
digest = hashlib.sha256()
with archive.open("rb") as stream:
    for block in iter(lambda: stream.read(1024 * 1024), b""):
        digest.update(block)
Path(str(archive) + ".sha256").write_text(
    f"{digest.hexdigest()}  {archive.name}\n", encoding="ascii"
)
PY

mkdir -p -- "$DIST"
for suffix in .tar.gz .tar.gz.sha256; do
  target="$DIST/$NAME$suffix"
  [[ ! -L "$target" && ( ! -e "$target" || -f "$target" ) ]] \
    || fail "refusing to replace a symlink or non-file output: $target"
done
# Rename rather than writing through an existing output path.
mv -T -- "$WORK/$NAME.tar.gz" "$DIST/$NAME.tar.gz"
mv -T -- "$WORK/$NAME.tar.gz.sha256" "$DIST/$NAME.tar.gz.sha256"
printf '%s\n' "$DIST/$NAME.tar.gz" "$DIST/$NAME.tar.gz.sha256"
