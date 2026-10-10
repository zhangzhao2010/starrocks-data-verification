#!/usr/bin/env bash
set -Eeuo pipefail
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/lib.sh"

require_root
(($# == 2)) || die "usage: rollback.sh <release-version> <pre-upgrade.dump>"
TARGET_VERSION=$1
BACKUP=$2
[[ $TARGET_VERSION =~ ^[0-9][0-9A-Za-z._-]*$ ]] || die "invalid release version"
TARGET_RELEASE="$RELEASES_DIR/$TARGET_VERSION"
[[ -x "$TARGET_RELEASE/venv/bin/srdq" ]] || die "release $TARGET_VERSION is not installed"
[[ -f "$BACKUP" ]] || die "backup file does not exist: $BACKUP"
BACKUP=$(readlink -f "$BACKUP")
[[ "$BACKUP" == "$BACKUP_DIR/"*.dump ]] || die "backup must be a .dump file in $BACKUP_DIR"
[[ -L "$CURRENT_LINK" ]] || die "srdq is not installed"
ORIGINAL_RELEASE=$(readlink -f "$CURRENT_LINK")
ORIGINAL_VERSION=$(basename "$ORIGINAL_RELEASE")
[[ "$ORIGINAL_RELEASE" != "$TARGET_RELEASE" ]] || die "release $TARGET_VERSION is already active"

exec 9>/run/srdq-deploy.lock
flock -n 9 || die "another deployment or backup is in progress"
PG_BIN=$PG_CURRENT/pgserver/pginstall/bin
runuser -u srdq-pg -- "$PG_BIN/pg_restore" --list "$BACKUP" >/dev/null || die "backup is not a valid custom-format dump"
snapshot_installed_config "$ORIGINAL_RELEASE"
[[ -f "$TARGET_RELEASE/config-snapshot/config.yaml" ]] || die "release $TARGET_VERSION has no configuration snapshot"
record_active_services
PHASE=before_replace
SAFETY_BACKUP=

restore_database() {
  local dump=$1
  runuser -u srdq-pg -- "$PG_BIN/dropdb" -h "$PG_SOCKET" --force --if-exists srdq &&
    runuser -u srdq-pg -- "$PG_BIN/createdb" -h "$PG_SOCKET" --owner=srdq-ctl srdq &&
    runuser -u srdq-pg -- "$PG_BIN/pg_restore" -h "$PG_SOCKET" --exit-on-error --no-owner \
      --role=srdq-ctl --dbname=srdq "$dump"
}

on_error() {
  local rc=$?
  trap - ERR
  set +e
  case "$PHASE" in
    before_replace)
      if restart_recorded_services; then
        printf 'rollback stopped before replacing the database; the active release is unchanged\n' >&2
      else
        printf 'rollback stopped before replacing the database, but not all previously active services restarted\n' >&2
      fi
      ;;
    replacing)
      if [[ -n "$SAFETY_BACKUP" ]] && restore_database "$SAFETY_BACKUP" && \
          restore_release_config "$ORIGINAL_RELEASE" && \
          atomic_symlink "$ORIGINAL_RELEASE" "$CURRENT_LINK" && restart_recorded_services; then
        printf 'rollback failed while restoring the target; the original database and release %s were restored\n' \
          "$ORIGINAL_VERSION" >&2
      else
        printf 'rollback and automatic database recovery failed; services remain stopped; safety backup: %s\n' \
          "${SAFETY_BACKUP:-unavailable}" >&2
      fi
      ;;
    target_restored)
      if restore_release_config "$TARGET_RELEASE" && atomic_symlink "$TARGET_RELEASE" "$CURRENT_LINK" && \
          runuser -u srdq-ctl -- env SRDQ_CONFIG="$CONFIG_FILE" "$CURRENT_LINK/venv/bin/srdq" recovery enter \
            --reason "rollback to $TARGET_VERSION from $(basename "$BACKUP")" >/dev/null && \
          restart_recorded_services; then
        printf 'rollback reached the target database and was placed in recovery mode after an intermediate failure\n' >&2
      else
        printf 'target database was restored but recovery mode or service restart could not be confirmed; services remain stopped\n' >&2
      fi
      ;;
    safe_target)
      if restart_recorded_services; then
        printf 'rollback reached release %s in recovery mode, but service validation failed\n' "$TARGET_VERSION" >&2
      else
        printf 'rollback reached release %s in recovery mode, but not all services restarted\n' "$TARGET_VERSION" >&2
      fi
      ;;
  esac
  [[ -z "$SAFETY_BACKUP" ]] || printf 'safety backup: %s\n' "$SAFETY_BACKUP" >&2
  exit "$rc"
}
trap on_error ERR

stop_app_services
SAFETY_BACKUP=$($SRDQ_ROOT/bin/srdq-pg-backup --no-lock --label "pre-rollback-$TARGET_VERSION")
PHASE=replacing
restore_database "$BACKUP"
PHASE=target_restored
restore_release_config "$TARGET_RELEASE"
atomic_symlink "$TARGET_RELEASE" "$CURRENT_LINK"
runuser -u srdq-ctl -- env SRDQ_CONFIG="$CONFIG_FILE" "$CURRENT_LINK/venv/bin/srdq" recovery enter \
  --reason "rollback to $TARGET_VERSION from $(basename "$BACKUP")" >/dev/null
PHASE=safe_target
restart_recorded_services
runuser -u srdq-ctl -- env SRDQ_CONFIG="$CONFIG_FILE" "$CURRENT_LINK/venv/bin/srdq" status >/dev/null
trap - ERR
printf 'rolled back to srdq %s in recovery mode\nsafety backup of replaced database: %s\n' \
  "$TARGET_VERSION" "$SAFETY_BACKUP"
