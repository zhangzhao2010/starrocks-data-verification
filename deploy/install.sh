#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/lib.sh"

require_root
load_bundle "${1:-$SCRIPT_DIR/..}"
verify_host
exec 9>/run/srdq-deploy.lock
flock -n 9 || die "another deployment or backup is in progress"

ensure_accounts_and_dirs
install_release
install_postgres_binary
set_postgres_current
install_deploy_files
install_provisioned_config
initialize_postgres
migration_command "$RELEASES_DIR/$BUNDLE_VERSION"
issue_worker_tokens "$RELEASES_DIR/$BUNDLE_VERSION"
snapshot_installed_config "$RELEASES_DIR/$BUNDLE_VERSION"
atomic_symlink "$RELEASES_DIR/$BUNDLE_VERSION" "$CURRENT_LINK"
systemctl enable --now "${APP_SERVICES[@]}" srdq-pg-backup.timer
for service in "${APP_SERVICES[@]}"; do systemctl is-active --quiet "$service"; done

cat <<EOF
srdq $BUNDLE_VERSION installed with the packaged production configuration; all services are running.
Configuration: $CONFIG_FILE
Dashboard: http://<ecs-ip>:8470/
EOF
