#!/usr/bin/env bash
set -Eeuo pipefail
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/lib.sh"

require_root
load_bundle "${1:-$SCRIPT_DIR/..}"
verify_host
exec 9>/run/srdq-deploy.lock
flock -n 9 || die "another deployment or backup is in progress"
[[ -L "$CURRENT_LINK" && -L "$PG_CURRENT" ]] || die "srdq is not installed; run install.sh first"

OLD_RELEASE=$(readlink -f "$CURRENT_LINK")
OLD_VERSION=$(basename "$OLD_RELEASE")
OLD_PG=$(readlink -f "$PG_CURRENT")
NEW_RELEASE="$RELEASES_DIR/$BUNDLE_VERSION"
[[ "$OLD_VERSION" != "$BUNDLE_VERSION" ]] || die "version $BUNDLE_VERSION is already active"
[[ -f "$OLD_RELEASE/release.env" ]] || die "current release metadata is missing"
[[ $(<"$PG_DATA/PG_VERSION") == "$PG_MAJOR" ]] || die "PostgreSQL major upgrade requires dump/restore"

ensure_accounts_and_dirs
install_release
install_postgres_binary
record_active_services
PHASE=pre_migration
BACKUP=
on_error() {
  local rc=$? recovery_ok=true
  trap - ERR
  if [[ "$PHASE" == pre_migration ]]; then
    restore_release_config "$OLD_RELEASE" 2>/dev/null || recovery_ok=false
    atomic_symlink "$OLD_RELEASE" "$CURRENT_LINK" 2>/dev/null || recovery_ok=false
    atomic_symlink "$OLD_PG" "$PG_CURRENT" 2>/dev/null || recovery_ok=false
    systemctl start srdq-pg.service 2>/dev/null || recovery_ok=false
    restart_recorded_services 2>/dev/null || recovery_ok=false
    if $recovery_ok; then
      printf 'upgrade failed before migration; the previous release was restored' >&2
    else
      printf 'upgrade failed before migration and automatic recovery was incomplete; inspect services before retrying' >&2
    fi
  else
    atomic_symlink "$NEW_RELEASE" "$CURRENT_LINK" 2>/dev/null || recovery_ok=false
    systemctl start srdq-pg.service 2>/dev/null || recovery_ok=false
    restart_recorded_services 2>/dev/null || recovery_ok=false
    if $recovery_ok; then
      printf 'upgrade failed after migration; the new release remains selected' >&2
    else
      printf 'upgrade failed after migration and automatic recovery was incomplete; keep the new release with the migrated database' >&2
    fi
  fi
  if [[ -n "$BACKUP" ]]; then
    printf '; pre-upgrade backup: %s; rollback: %s %s %s' \
      "$BACKUP" "$SRDQ_ROOT/deploy/rollback.sh" "$OLD_VERSION" "$BACKUP" >&2
  fi
  printf '\n' >&2
  exit "$rc"
}
trap on_error ERR

snapshot_installed_config "$OLD_RELEASE"
stop_app_services
BACKUP=$($SRDQ_ROOT/bin/srdq-pg-backup --no-lock --label "pre-$BUNDLE_VERSION")
install_deploy_files
install_provisioned_config

if [[ $(readlink -f "$PG_CURRENT") != "$PG_ROOT/$PG_VERSION" ]]; then
  systemctl stop srdq-pg.service
  set_postgres_current
  systemctl start srdq-pg.service
  initialize_postgres
fi
migration_command "$NEW_RELEASE"
PHASE=post_migration
issue_worker_tokens "$NEW_RELEASE"
snapshot_installed_config "$NEW_RELEASE"
atomic_symlink "$NEW_RELEASE" "$CURRENT_LINK"

STATE_DIR=$DATA_ROOT/upgrades
install -d -o root -g root -m 0700 "$STATE_DIR"
STATE_FILE="$STATE_DIR/$(date -u +%Y%m%dT%H%M%SZ)-${OLD_VERSION}-to-${BUNDLE_VERSION}.env"
printf 'FROM_VERSION=%s\nTO_VERSION=%s\nBACKUP=%s\n' "$OLD_VERSION" "$BUNDLE_VERSION" "$BACKUP" > "$STATE_FILE"
chmod 0600 "$STATE_FILE"
restart_recorded_services
runuser -u srdq-ctl -- env SRDQ_CONFIG="$CONFIG_FILE" "$CURRENT_LINK/venv/bin/srdq" status >/dev/null
trap - ERR

printf 'upgraded srdq %s -> %s\nbackup: %s\nrollback: %s %s %s\n' \
  "$OLD_VERSION" "$BUNDLE_VERSION" "$BACKUP" "$SRDQ_ROOT/deploy/rollback.sh" "$OLD_VERSION" "$BACKUP"
