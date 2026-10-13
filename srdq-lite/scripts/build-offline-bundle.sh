#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
REPO=$(cd "$ROOT/.." && pwd)
PYTHON=${PYTHON_BIN:-"$REPO/.venv/bin/python"}
PIP_PYTHON=${PIP_PYTHON:-python3}
VERSION=$("$PYTHON" -c 'import sys; ns={}; exec(open(sys.argv[1]).read(), ns); print(ns["__version__"])' \
  "$ROOT/srdq_lite/__init__.py")
NAME="srdq-lite-offline-${VERSION}-linux-x86_64"
DIST="$ROOT/dist"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
STAGE="$WORK/$NAME"
mkdir -p "$STAGE/wheels"

for PYVER in 311 312; do
  "$PIP_PYTHON" -m pip download --disable-pip-version-check \
    --only-binary=:all: --platform manylinux2014_x86_64 --implementation cp \
    --python-version "$PYVER" --abi "cp$PYVER" \
    --dest "$STAGE/wheels" -r "$ROOT/requirements.txt"
done
"$PIP_PYTHON" -m pip download --disable-pip-version-check --only-binary=:all: \
  --dest "$STAGE/wheels" pip==24.0

cp -a "$ROOT/srdq_lite" "$STAGE/srdq_lite"
cp -a "$ROOT/tests" "$STAGE/tests"
cp "$ROOT/README.md" "$ROOT/requirements.txt" "$ROOT/config.example.yaml" \
  "$ROOT/tables.example.jsonl" "$ROOT/viewer.html" "$STAGE/"
mkdir -p "$STAGE/table_info/output"
for CLUSTER in 数仓集群 监控集群 账单集群; do
  mkdir -p "$STAGE/table_info/output/$CLUSTER"
  cp "$REPO/table_info/output/$CLUSTER/tables.jsonl" "$STAGE/table_info/output/$CLUSTER/tables.jsonl"
done
cp "$ROOT/offline/install.sh" "$ROOT/offline/run.sh" "$STAGE/"
chmod 0755 "$STAGE/install.sh" "$STAGE/run.sh"
find "$STAGE" -type d -name __pycache__ -prune -exec rm -rf {} +
find "$STAGE" -type d -name .pytest_cache -prune -exec rm -rf {} +
cat > "$STAGE/BUILD-INFO" <<EOF
NAME=$NAME
VERSION=$VERSION
TARGET_OS=linux
TARGET_ARCH=x86_64
PYTHON=3.11,3.12
WHEEL_PLATFORM=manylinux2014_x86_64
MIN_GLIBC=2.17
EOF

mkdir -p "$DIST"
tar --sort=name --owner=0 --group=0 --numeric-owner -czf "$DIST/$NAME.tar.gz" -C "$WORK" "$NAME"
(cd "$DIST" && sha256sum "$NAME.tar.gz" > "$NAME.tar.gz.sha256")
echo "$DIST/$NAME.tar.gz"
echo "$DIST/$NAME.tar.gz.sha256"
