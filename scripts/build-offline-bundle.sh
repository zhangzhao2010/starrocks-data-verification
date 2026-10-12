#!/usr/bin/env bash
set -euo pipefail
umask 0022

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
OUT_DIR="$ROOT/dist"
PROVISION_DIR=
PYTHON_VERSION=3.11
PLATFORM=manylinux2014_x86_64
PIP_VERSION=${PIP_VERSION:-24.0}
UV=${UV:-$(command -v uv || true)}
[[ -n "$UV" || ! -x "$HOME/.local/bin/uv" ]] || UV="$HOME/.local/bin/uv"

usage() {
  cat <<'EOF'
usage: build-offline-bundle.sh --provision-dir DIR [--out-dir DIR]

DIR must contain config.yaml and one secrets/<cluster-id>.json per configured physical cluster.
The config must have run_enabled: true. Worker tokens are generated during installation.
EOF
}

while (($#)); do
  case "$1" in
    --provision-dir) shift; PROVISION_DIR=${1:?--provision-dir needs a directory} ;;
    --out-dir) shift; OUT_DIR=${1:?--out-dir needs a directory} ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
  shift
done

[[ -n "$PROVISION_DIR" ]] || { usage >&2; exit 2; }
PROVISION_DIR=$(cd "$PROVISION_DIR" && pwd)
[[ $(uname -m) == x86_64 ]] || { echo "build host must be x86_64" >&2; exit 1; }
[[ -n "$UV" ]] || { echo "uv is required" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 1; }
command -v unzip >/dev/null || { echo "unzip is required" >&2; exit 1; }
[[ -f "$PROVISION_DIR/config.yaml" && ! -L "$PROVISION_DIR/config.yaml" ]] || {
  echo "missing regular provision config: $PROVISION_DIR/config.yaml" >&2; exit 1;
}
if find "$PROVISION_DIR" -type l -print -quit | grep -q .; then
  echo "provision directory must not contain symbolic links" >&2
  exit 1
fi
if find "$PROVISION_DIR/secrets" -mindepth 1 ! -type f -print -quit | grep -q .; then
  echo "provision secrets directory may contain regular files only" >&2
  exit 1
fi

PYTHONPATH="$ROOT" "$UV" run --frozen --no-dev python - "$PROVISION_DIR" <<'PY'
import json
import sys
from pathlib import Path
from srdq import config

root = Path(sys.argv[1])
cfg = config.load(root / "config.yaml")
if cfg["run_enabled"] is not True:
    raise SystemExit("provision config must set run_enabled: true")
if not cfg.transfer_approved:
    raise SystemExit("provision config must set a non-placeholder transfer.global_approval_ref")
users = {
    "huawei-1": "srdq-hw1", "aliyun-1": "srdq-al1",
    "huawei-2": "srdq-hw2", "aliyun-2": "srdq-al2",
    "huawei-3": "srdq-hw3", "aliyun-3": "srdq-al3",
}
if set(cfg.cluster_ids) != set(users):
    raise SystemExit(f"production config must define exactly these clusters: {sorted(users)}")
for cluster in cfg["physical_clusters"]:
    cluster_id = cluster["id"]
    expected = f"/etc/srdq/secrets/{cluster_id}.json"
    if cluster["credential_file"] != expected or cluster["os_user"] != users[cluster_id]:
        raise SystemExit(f"physical cluster {cluster_id} must use credential_file={expected}, os_user={users[cluster_id]}")
    path = root / "secrets" / f"{cluster_id}.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"invalid {path}: {exc}") from exc
    for key in ("host", "port", "user", "password"):
        value = data.get(key)
        if value in (None, "") or (isinstance(value, str) and value.startswith("<")):
            raise SystemExit(f"{path} needs a production value for {key}")
    if data.get("worker_token"):
        raise SystemExit(f"remove worker_token from {path}; the installer generates it")
    if data.get("ssl_ca"):
        ca = Path(str(data["ssl_ca"]))
        expected_parent = Path("/etc/srdq/secrets")
        if ca.parent != expected_parent or not (root / "secrets" / ca.name).is_file():
            raise SystemExit(f"{path} ssl_ca must name a bundled file under {expected_parent}")
pg = cfg["credentials"]["controller_pg"]
if pg["file"] != "/etc/srdq/secrets/pg.json" or pg["os_user"] != "srdq-ctl" or pg["mode"] != "0600":
    raise SystemExit("credentials.controller_pg must use file=/etc/srdq/secrets/pg.json, os_user=srdq-ctl, mode=0600")
PY

readarray -t META < <(python3 - "$ROOT/pyproject.toml" "$ROOT/uv.lock" <<'PY'
import sys, tomllib
project = tomllib.load(open(sys.argv[1], "rb"))["project"]
lock = tomllib.load(open(sys.argv[2], "rb"))
pg = next(p for p in lock["package"] if p["name"] == "pgserver")
print(project["version"])
print(pg["version"])
PY
)
VERSION=${META[0]}
PGSERVER_VERSION=${META[1]}
[[ $VERSION =~ ^[0-9][0-9A-Za-z._-]*$ ]] || { echo "unsafe project version: $VERSION" >&2; exit 1; }

mkdir -p "$OUT_DIR"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
BUNDLE="$WORK/srdq-offline-$VERSION"
mkdir -p "$BUNDLE/wheels" "$BUNDLE/postgresql" "$BUNDLE/provision/secrets"
install -m 0600 "$PROVISION_DIR/config.yaml" "$BUNDLE/provision/config.yaml"
find "$PROVISION_DIR/secrets" -maxdepth 1 -type f -exec install -m 0600 {} "$BUNDLE/provision/secrets/" \;

"$UV" export --frozen --no-dev --no-emit-project --no-hashes -o "$WORK/requirements.txt"
python3 -m pip download --disable-pip-version-check --dest "$BUNDLE/wheels" \
  --only-binary=:all: --platform "$PLATFORM" --python-version "$PYTHON_VERSION" \
  --implementation cp --abi cp311 -r "$WORK/requirements.txt"
python3 -m pip download --disable-pip-version-check --dest "$BUNDLE/wheels" \
  --only-binary=:all: --platform "$PLATFORM" --python-version "$PYTHON_VERSION" \
  --implementation cp --abi cp311 --no-deps "pgserver==$PGSERVER_VERSION" "pip==$PIP_VERSION"
"$UV" build --wheel --out-dir "$BUNDLE/wheels" "$ROOT"

PG_WHEEL=$(find "$BUNDLE/wheels" -maxdepth 1 -type f -name "pgserver-${PGSERVER_VERSION}-cp311-*x86_64.whl" -print -quit)
[[ -n "$PG_WHEEL" ]] || { echo "pgserver Linux cp311 wheel is missing" >&2; exit 1; }
unzip -q "$PG_WHEEL" 'pgserver/pginstall/*' 'pgserver.libs/*' -d "$BUNDLE/postgresql"
PG_VERSION=$("$BUNDLE/postgresql/pgserver/pginstall/bin/postgres" --version | awk '{print $3}')
PG_MAJOR=${PG_VERSION%%.*}

cp -a "$ROOT/deploy" "$BUNDLE/deploy"
cat > "$BUNDLE/bundle.env" <<EOF
BUNDLE_VERSION=$VERSION
TARGET_ARCH=x86_64
PYTHON_VERSION=$PYTHON_VERSION
GLIBC_MIN=2.17
PG_VERSION=$PG_VERSION
PG_MAJOR=$PG_MAJOR
EOF

(
  cd "$BUNDLE"
  find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS
)
ARCHIVE="$OUT_DIR/srdq-offline-${VERSION}-alinux4-x86_64.tar.gz"
rm -f "$ARCHIVE" "$ARCHIVE.tmp" "$ARCHIVE.sha256"
(
  umask 0077
  tar -C "$WORK" -czf "$ARCHIVE.tmp" "$(basename "$BUNDLE")"
  mv "$ARCHIVE.tmp" "$ARCHIVE"
  cd "$OUT_DIR"
  sha256sum "$(basename "$ARCHIVE")" > "$(basename "$ARCHIVE").sha256"
)
printf 'created %s (contains production credentials; mode 0600)\n' "$ARCHIVE"
