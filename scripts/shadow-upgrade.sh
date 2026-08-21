#!/usr/bin/env bash
set -euo pipefail

deployment_state="${IPEG_DEPLOYMENT_STATE_PATH:-.ipeg-deployment-state}"

if [[ $# -ne 2 ]]; then
  echo "usage: shadow-upgrade.sh IMAGE@sha256:DIGEST FULL_RELEASE_SHA" >&2
  exit 2
fi
previous_image=""
previous_sha=""
if [[ -f "$deployment_state" ]]; then
  while IFS='=' read -r key value; do
    case "$key" in
      image_ref) previous_image="$value" ;;
      release_sha) previous_sha="$value" ;;
    esac
  done <"$deployment_state"
  if [[ ! "$previous_image" =~ @sha256:[0-9a-f]{64}$ ]] \
    || [[ ! "$previous_sha" =~ ^[0-9a-f]{40}$ ]]; then
    echo "persisted deployment identity is invalid" >&2
    exit 3
  fi
fi
backup_name="pre-upgrade-$(date -u +%Y%m%dT%H%M%SZ).sqlite3"
backup_created=false
if docker compose ps --status running --services | grep -qx app; then
  docker compose stop app
  if ! docker compose run --rm --no-deps app interexchange-grid backup-state \
    --config /app/config/defaults.yaml --target "/app/state/backups/${backup_name}"; then
    docker compose start app
    echo "upgrade aborted because the quiesced state backup failed" >&2
    exit 4
  fi
  backup_created=true
fi
if bash "$(dirname "$0")/shadow-deploy.sh" "$1" "$2"; then
  :
else
  upgrade_status=$?
  if [[ -z "$previous_image" || -z "$previous_sha" ]]; then
    echo "upgrade failed and no previous deployment identity is available" >&2
    exit "$upgrade_status"
  fi
  echo "upgrade failed; restoring $previous_image" >&2
  export IPEG_IMAGE_REF="$previous_image"
  export IPEG_RELEASE_SHA="$previous_sha"
  export IPEG_CONTAINER_IMAGE_DIGEST="${previous_image##*@}"
  docker compose stop app
  if [[ "$backup_created" == true ]]; then
    docker compose run --rm --no-deps app interexchange-grid restore-state \
      --config /app/config/defaults.yaml \
      --backup "/app/state/backups/${backup_name}"
  fi
  if ! bash "$(dirname "$0")/shadow-deploy.sh" "$previous_image" "$previous_sha"; then
    echo "automatic rollback failed; deployment remains fail-closed" >&2
    exit 5
  fi
  echo "automatic rollback completed" >&2
  exit "$upgrade_status"
fi
if [[ "$backup_created" == true ]]; then
  echo "state_backup=/app/state/backups/${backup_name}"
else
  echo "state_backup=none"
fi
