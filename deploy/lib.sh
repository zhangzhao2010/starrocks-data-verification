#!/usr/bin/env bash
set -euo pipefail

SRDQ_ROOT=/opt/srdq
RELEASES_DIR=$SRDQ_ROOT/releases
CURRENT_LINK=$SRDQ_ROOT/current
PG_ROOT=$SRDQ_ROOT/postgresql
PG_CURRENT=$PG_ROOT/current
DATA_ROOT=/var/lib/srdq
PG_DATA=$DATA_ROOT/pg
PG_SOCKET=/run/srdq-pg
BACKUP_DIR=$DATA_ROOT/backups
CONFIG_DIR=/etc/srdq
CONFIG_FILE=$CONFIG_DIR/config.yaml
PG_CREDENTIAL=$CONFIG_DIR/secrets/pg.json
APP_SERVICES=(srdq-controller.service srdq-web.service srdq-executor@huawei.service srdq-executor@aliyun.service)

die() { printf 'srdq deploy: %s\n' "$*" >&2; exit 1; }

require_root() {
  [[ ${EUID:-$(id -u)} -eq 0 ]] || die "run as root"
}

bundle_value() {
  local key=$1 value
  value=$(awk -F= -v key="$key" '$1 == key {sub(/^[^=]*=/, ""); print; exit}' "$BUNDLE_ROOT/bundle.env")
  [[ $value =~ ^[0-9A-Za-z._-]+$ ]] || die "invalid $key in bundle.env"
  printf '%s\n' "$value"
}

load_bundle() {
  BUNDLE_ROOT=$(cd "${1:-$(dirname "${BASH_SOURCE[1]}")/..}" && pwd)
  [[ -f "$BUNDLE_ROOT/bundle.env" && -f "$BUNDLE_ROOT/SHA256SUMS" ]] || die "not an offline bundle: $BUNDLE_ROOT"
  (cd "$BUNDLE_ROOT" && sha256sum -c --quiet SHA256SUMS) || die "bundle checksum verification failed"
  BUNDLE_VERSION=$(bundle_value BUNDLE_VERSION)
  TARGET_ARCH=$(bundle_value TARGET_ARCH)
  PYTHON_VERSION=$(bundle_value PYTHON_VERSION)
  GLIBC_MIN=$(bundle_value GLIBC_MIN)
  PG_VERSION=$(bundle_value PG_VERSION)
  PG_MAJOR=$(bundle_value PG_MAJOR)
  HUAWEI_WORKER_NAME=$(bundle_value HUAWEI_WORKER_NAME)
  ALIYUN_WORKER_NAME=$(bundle_value ALIYUN_WORKER_NAME)
}

find_python() {
  local candidate
  for candidate in "python$PYTHON_VERSION" python3; do
    command -v "$candidate" >/dev/null || continue
    [[ $("$candidate" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")') == "$PYTHON_VERSION" ]] && {
      command -v "$candidate"
      return
    }
  done
  die "Python $PYTHON_VERSION is required"
}

verify_host() {
  [[ $(uname -m) == "$TARGET_ARCH" ]] || die "bundle targets $TARGET_ARCH, host is $(uname -m)"
  local have_glibc ldd_output ldd_first sorted_versions oldest_version
  ldd_output=$(ldd --version 2>&1)
  ldd_first=${ldd_output%%$'\n'*}
  have_glibc=$(grep -oE '[0-9]+\.[0-9]+$' <<< "$ldd_first")
  sorted_versions=$(printf '%s\n%s\n' "$GLIBC_MIN" "$have_glibc" | sort -V)
  oldest_version=${sorted_versions%%$'\n'*}
  [[ "$oldest_version" == "$GLIBC_MIN" ]] || die "glibc $GLIBC_MIN or newer is required (host has $have_glibc)"
  PYTHON_BIN=$(find_python)
  "$PYTHON_BIN" -c 'import venv' >/dev/null 2>&1 || die "Python $PYTHON_VERSION venv module is required"
  command -v systemctl >/dev/null || die "systemd is required"
  command -v runuser >/dev/null || die "runuser is required"
  command -v flock >/dev/null || die "flock is required"
}

ensure_group() {
  getent group "$1" >/dev/null || groupadd --system "$1"
}

ensure_user() {
  local user=$1 group=$2
  if ! id "$user" >/dev/null 2>&1; then
    useradd --system --gid "$group" --shell /sbin/nologin --home-dir /nonexistent "$user"
  fi
}

ensure_accounts_and_dirs() {
  ensure_group srdq
  ensure_group srdq-pg
  ensure_user srdq-pg srdq-pg
  for user in srdq-ctl srdq-hw srdq-al; do ensure_user "$user" srdq; done
  usermod -a -G srdq-pg srdq-ctl

  install -d -o root -g root -m 0755 "$SRDQ_ROOT" "$RELEASES_DIR" "$PG_ROOT" "$SRDQ_ROOT/bin"
  install -d -o root -g srdq -m 0750 "$CONFIG_DIR" "$CONFIG_DIR/secrets"
  install -d -o root -g root -m 0755 "$DATA_ROOT"
  install -d -o srdq-pg -g srdq-pg -m 0700 "$PG_DATA" "$BACKUP_DIR"
  install -d -o srdq-ctl -g srdq -m 0700 "$DATA_ROOT/controller" "$DATA_ROOT/reports"
  install -d -o root -g root -m 0755 "$DATA_ROOT/executor"
  install -d -o srdq-hw -g srdq -m 0700 "$DATA_ROOT/executor/huawei"
  install -d -o srdq-al -g srdq -m 0700 "$DATA_ROOT/executor/aliyun"
}

atomic_symlink() {
  local target=$1 link=$2 tmp
  tmp="${link}.new.$$"
  ln -s "$target" "$tmp"
  mv -Tf "$tmp" "$link"
}

install_release() {
  local destination="$RELEASES_DIR/$BUNDLE_VERSION" digest pip_wheel srdq_wheel
  digest=$(sha256sum "$BUNDLE_ROOT/SHA256SUMS" | awk '{print $1}')
  pip_wheel=$(find "$BUNDLE_ROOT/wheels" -maxdepth 1 -type f -name 'pip-*.whl' -print -quit)
  srdq_wheel=$(find "$BUNDLE_ROOT/wheels" -maxdepth 1 -type f -name "srdq-${BUNDLE_VERSION}-*.whl" -print -quit)
  [[ -n "$pip_wheel" && -n "$srdq_wheel" ]] || die "pip or srdq wheel is missing"
  if [[ -d "$destination" ]]; then
    grep -Fxq "BUNDLE_DIGEST=$digest" "$destination/release.env" 2>/dev/null && return
    die "release $BUNDLE_VERSION already exists from a different bundle; bump project.version"
  fi
  mkdir "$destination"
  trap "rm -rf -- '$destination'" ERR
  "$PYTHON_BIN" -m venv --without-pip "$destination/venv"
  PYTHONPATH="$pip_wheel" "$destination/venv/bin/python" -m pip install --no-index \
    --find-links "$BUNDLE_ROOT/wheels" "$srdq_wheel"
  printf 'BUNDLE_VERSION=%s\nPG_VERSION=%s\nBUNDLE_DIGEST=%s\n' \
    "$BUNDLE_VERSION" "$PG_VERSION" "$digest" > "$destination/release.env"
  chmod -R go-w "$destination"
  trap - ERR
}

install_postgres_binary() {
  local destination="$PG_ROOT/$PG_VERSION" temp="$PG_ROOT/.${PG_VERSION}.$$"
  if [[ ! -d "$destination" ]]; then
    mkdir "$temp"
    cp -a "$BUNDLE_ROOT/postgresql/." "$temp/"
    chown -R root:root "$temp"
    "$temp/pgserver/pginstall/bin/postgres" --version | grep -Fq " $PG_VERSION" || die "bundled PostgreSQL version mismatch"
    mv "$temp" "$destination"
  fi
  chown -R root:root "$destination"
}

set_postgres_current() {
  atomic_symlink "$PG_ROOT/$PG_VERSION" "$PG_CURRENT"
}

install_deploy_files() {
  local source relative
  install -d -o root -g root -m 0755 "$SRDQ_ROOT/deploy"
  install -o root -g root -m 0755 "$BUNDLE_ROOT/deploy/bin/srdq-pg-backup" "$SRDQ_ROOT/bin/srdq-pg-backup"
  install -o root -g root -m 0755 "$BUNDLE_ROOT/deploy/lib.sh" "$BUNDLE_ROOT/deploy/install.sh" \
    "$BUNDLE_ROOT/deploy/upgrade.sh" "$BUNDLE_ROOT/deploy/rollback.sh" "$SRDQ_ROOT/deploy/"
  while IFS= read -r -d '' source; do
    relative=${source#"$BUNDLE_ROOT/deploy/systemd/"}
    install -d -o root -g root -m 0755 "/etc/systemd/system/$relative"
  done < <(find "$BUNDLE_ROOT/deploy/systemd" -mindepth 1 -type d -print0)
  while IFS= read -r -d '' source; do
    relative=${source#"$BUNDLE_ROOT/deploy/systemd/"}
    install -o root -g root -m 0644 "$source" "/etc/systemd/system/$relative"
  done < <(find "$BUNDLE_ROOT/deploy/systemd" -type f -print0)
  systemctl daemon-reload
}

initialize_postgres() {
  local bin="$PG_CURRENT/pgserver/pginstall/bin"
  if [[ ! -f "$PG_DATA/PG_VERSION" ]]; then
    runuser -u srdq-pg -- "$bin/initdb" -D "$PG_DATA" --auth-local=peer --auth-host=reject \
      --encoding=UTF8 --locale=C
    cat >> "$PG_DATA/postgresql.conf" <<EOF
listen_addresses = ''
unix_socket_directories = '$PG_SOCKET'
unix_socket_permissions = 0770
EOF
  fi
  local data_major
  data_major=$(<"$PG_DATA/PG_VERSION")
  [[ "$data_major" == "$PG_MAJOR" ]] || die "database is PostgreSQL $data_major but bundle contains $PG_MAJOR; dump/restore upgrade is required"

  systemctl enable --now srdq-pg.service
  for _ in {1..30}; do
    runuser -u srdq-pg -- "$bin/pg_isready" -h "$PG_SOCKET" -d postgres >/dev/null 2>&1 && break
    sleep 1
  done
  runuser -u srdq-pg -- "$bin/pg_isready" -h "$PG_SOCKET" -d postgres >/dev/null || die "PostgreSQL did not become ready"
  if ! runuser -u srdq-pg -- "$bin/psql" -h "$PG_SOCKET" -d postgres -Atqc \
      "SELECT 1 FROM pg_roles WHERE rolname='srdq-ctl'" | grep -qx 1; then
    runuser -u srdq-pg -- "$bin/createuser" -h "$PG_SOCKET" --no-superuser --no-createdb --no-createrole srdq-ctl
  fi
  if ! runuser -u srdq-pg -- "$bin/psql" -h "$PG_SOCKET" -d postgres -Atqc \
      "SELECT 1 FROM pg_database WHERE datname='srdq'" | grep -qx 1; then
    runuser -u srdq-pg -- "$bin/createdb" -h "$PG_SOCKET" --owner=srdq-ctl srdq
  fi
}

install_provisioned_config() {
  local packaged="$BUNDLE_ROOT/provision" source base side user destination old_token
  [[ -f "$packaged/config.yaml" && -f "$packaged/secrets/huawei.json" && \
     -f "$packaged/secrets/aliyun.json" ]] || die "bundle has no production provision files"
  install -o root -g srdq -m 0640 "$packaged/config.yaml" "$CONFIG_FILE"
  while IFS= read -r -d '' source; do
    base=$(basename "$source")
    [[ "$base" == huawei.json || "$base" == aliyun.json ]] && continue
    install -o root -g srdq -m 0640 "$source" "$CONFIG_DIR/secrets/$base"
  done < <(find "$packaged/secrets" -maxdepth 1 -type f -print0)

  for side in huawei aliyun; do
    [[ "$side" == huawei ]] && user=srdq-hw || user=srdq-al
    destination="$CONFIG_DIR/secrets/$side.json"
    old_token=
    if [[ -f "$destination" ]]; then
      old_token=$("$PYTHON_BIN" - "$destination" <<'PY'
import json, sys
try:
    print(json.load(open(sys.argv[1], encoding="utf-8")).get("worker_token", ""), end="")
except (OSError, ValueError):
    pass
PY
)
    fi
    "$PYTHON_BIN" - "$packaged/secrets/$side.json" "$destination" "$old_token" "$user" <<'PY'
import grp, json, os, pwd, sys, tempfile
source, destination, token, user = sys.argv[1:]
data = json.load(open(source, encoding="utf-8"))
if token:
    data["worker_token"] = token
else:
    data.pop("worker_token", None)
parent = os.path.dirname(destination)
fd, temporary = tempfile.mkstemp(prefix=f".{os.path.basename(destination)}.", dir=parent)
try:
    os.fchmod(fd, 0o600)
    os.fchown(fd, pwd.getpwnam(user).pw_uid, grp.getgrnam("srdq").gr_gid)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        json.dump(data, stream, ensure_ascii=False, separators=(",", ":"))
        stream.write("\n")
    os.replace(temporary, destination)
except BaseException:
    try:
        os.close(fd)
    except OSError:
        pass
    try:
        os.unlink(temporary)
    except OSError:
        pass
    raise
PY
  done

  if [[ ! -f "$PG_CREDENTIAL" ]]; then
    local temp
    temp=$(mktemp "$CONFIG_DIR/secrets/.pg.json.XXXXXX")
    printf '{"dsn":"host=%s dbname=srdq user=srdq-ctl"}\n' "$PG_SOCKET" > "$temp"
    chown srdq-ctl:srdq "$temp"
    chmod 0600 "$temp"
    mv "$temp" "$PG_CREDENTIAL"
  fi
}

snapshot_installed_config() {
  local release=$1 snapshot="$1/config-snapshot" temp="$1/.config-snapshot.$$"
  [[ -d "$snapshot" ]] && return
  install -d -o root -g root -m 0700 "$temp"
  cp -a "$CONFIG_DIR/." "$temp/"
  chown -R root:root "$temp"
  chmod -R go-rwx "$temp"
  mv "$temp" "$snapshot"
}

restore_release_config() {
  local release=$1 snapshot="$1/config-snapshot" source base owner
  [[ -f "$snapshot/config.yaml" && -f "$snapshot/secrets/huawei.json" && \
     -f "$snapshot/secrets/aliyun.json" && -f "$snapshot/secrets/pg.json" ]] || return 1
  install -o root -g srdq -m 0640 "$snapshot/config.yaml" "$CONFIG_FILE"
  while IFS= read -r -d '' source; do
    base=$(basename "$source")
    case "$base" in
      huawei.json) owner=srdq-hw ;;
      aliyun.json) owner=srdq-al ;;
      pg.json) owner=srdq-ctl ;;
      *) owner=root ;;
    esac
    if [[ "$owner" == root ]]; then
      install -o root -g srdq -m 0640 "$source" "$CONFIG_DIR/secrets/$base"
    else
      install -o "$owner" -g srdq -m 0600 "$source" "$CONFIG_DIR/secrets/$base"
    fi
  done < <(find "$snapshot/secrets" -maxdepth 1 -type f -print0)
}

issue_worker_tokens() {
  local release=$1 side name user token_file credential
  for side in huawei aliyun; do
    if [[ "$side" == huawei ]]; then
      name=$HUAWEI_WORKER_NAME
      user=srdq-hw
    else
      name=$ALIYUN_WORKER_NAME
      user=srdq-al
    fi
    token_file="$DATA_ROOT/controller/.worker-token-${side}.$$"
    credential="$CONFIG_DIR/secrets/$side.json"
    if "$PYTHON_BIN" - "$credential" <<'PY'
import json, sys
token = json.load(open(sys.argv[1], encoding="utf-8")).get("worker_token")
raise SystemExit(0 if isinstance(token, str) and token.strip() else 1)
PY
    then
      continue
    fi
    rm -f "$token_file"
    if ! runuser -u srdq-ctl -- env SRDQ_CONFIG="$CONFIG_FILE" "$release/venv/bin/srdq" \
        worker-token create --name "$name" --side "$side" --rotate --out "$token_file" >/dev/null; then
      rm -f "$token_file"
      return 1
    fi
    if ! "$PYTHON_BIN" - "$credential" "$token_file" "$user" <<'PY'
import grp, json, os, pwd, sys, tempfile
credential, token_file, user = sys.argv[1:]
data = json.load(open(credential, encoding="utf-8"))
data["worker_token"] = open(token_file, encoding="utf-8").read().strip()
parent = os.path.dirname(credential)
fd, temporary = tempfile.mkstemp(prefix=f".{os.path.basename(credential)}.", dir=parent)
try:
    os.fchmod(fd, 0o600)
    os.fchown(fd, pwd.getpwnam(user).pw_uid, grp.getgrnam("srdq").gr_gid)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        json.dump(data, stream, ensure_ascii=False, separators=(",", ":"))
        stream.write("\n")
    os.replace(temporary, credential)
except BaseException:
    try:
        os.close(fd)
    except OSError:
        pass
    try:
        os.unlink(temporary)
    except OSError:
        pass
    raise
PY
    then
      rm -f "$token_file"
      return 1
    fi
    rm -f "$token_file"
  done
}

migration_command() {
  local release=$1
  runuser -u srdq-ctl -- env SRDQ_CONFIG="$CONFIG_FILE" "$release/venv/bin/srdq" migrate
}

record_active_services() {
  ACTIVE_SERVICES=()
  local service
  for service in "${APP_SERVICES[@]}"; do
    systemctl is-active --quiet "$service" && ACTIVE_SERVICES+=("$service") || true
  done
}

stop_app_services() {
  systemctl stop "${APP_SERVICES[@]}"
  local service
  for service in "${APP_SERVICES[@]}"; do
    if systemctl is-active --quiet "$service"; then
      die "failed to stop $service"
    fi
  done
  return 0
}

restart_recorded_services() {
  if ((${#ACTIVE_SERVICES[@]})); then
    systemctl start "${ACTIVE_SERVICES[@]}"
  fi
}
