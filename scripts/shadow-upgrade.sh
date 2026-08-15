#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: shadow-upgrade.sh IMAGE@sha256:DIGEST FULL_RELEASE_SHA" >&2
  exit 2
fi
backup_name="pre-upgrade-$(date -u +%Y%m%dT%H%M%SZ).sqlite3"
if docker compose ps --status running --services | grep -qx app; then
  docker compose exec -T app interexchange-grid backup-state \
    --config /app/config/defaults.yaml --target "/app/state/backups/${backup_name}"
fi
"$(dirname "$0")/shadow-deploy.sh" "$1" "$2"
echo "state_backup=/app/state/backups/${backup_name}"
