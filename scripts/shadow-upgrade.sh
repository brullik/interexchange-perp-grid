#!/usr/bin/env bash
set -euo pipefail

deployment_state="${IPEG_DEPLOYMENT_STATE_PATH:-.ipeg-deployment-state}"

if [[ $# -ne 2 ]]; then
  echo "usage: shadow-upgrade.sh IMAGE@sha256:DIGEST FULL_RELEASE_SHA" >&2
  exit 2
fi
target_image="$1"
target_sha="$2"
if [[ ! "$target_image" =~ @sha256:[0-9a-f]{64}$ ]] \
  || [[ ! "$target_sha" =~ ^[0-9a-f]{40}$ ]]; then
  echo "upgrade requires an immutable image digest and full release SHA" >&2
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
upgrade_gate_armed=false
run_upgrade_gate() {
  IPEG_IMAGE_REF="$target_image" \
  IPEG_RELEASE_SHA="$target_sha" \
  IPEG_CONTAINER_IMAGE_DIGEST="${target_image##*@}" \
    docker compose run --rm --no-deps app interexchange-grid deployment-upgrade-gate \
      --config /app/config/defaults.yaml --action "$1"
}
run_previous_compose() {
  IPEG_IMAGE_REF="$previous_image" \
  IPEG_RELEASE_SHA="$previous_sha" \
  IPEG_CONTAINER_IMAGE_DIGEST="${previous_image##*@}" \
    docker compose "$@"
}
release_upgrade_gate() {
  if ! run_upgrade_gate release; then
    echo "deployment is healthy but the upgrade entry freeze could not be released" >&2
    return 1
  fi
  upgrade_gate_armed=false
}
if docker compose ps --status running --services | grep -qx app; then
  if [[ -z "$previous_image" || -z "$previous_sha" ]]; then
    echo "running service has no verified immutable deployment identity" >&2
    exit 3
  fi
  IPEG_IMAGE_REF="$target_image" docker compose pull app
  target_revision="$(IPEG_RELEASE_SHA="$target_sha" docker image inspect "$target_image" \
    --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}')"
  if [[ "$target_revision" != "$target_sha" ]]; then
    echo "upgrade image revision label does not match the requested release SHA" >&2
    exit 3
  fi
  docker compose pause app
  if ! run_previous_compose run --rm --no-deps app interexchange-grid backup-state \
    --config /app/config/defaults.yaml --target "/app/state/backups/${backup_name}"; then
    docker compose unpause app
    echo "upgrade aborted because the paused-state backup failed" >&2
    exit 4
  fi
  backup_created=true
  if ! run_upgrade_gate arm; then
    docker compose unpause app
    echo "upgrade aborted before shutdown; old service resumed for risk reduction" >&2
    exit 6
  fi
  upgrade_gate_armed=true
  docker compose kill app
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
  if ! run_upgrade_gate arm; then
    echo "rollback remains stopped because its legacy safety gate could not be armed" >&2
    exit 6
  fi
  upgrade_gate_armed=true
  if ! bash "$(dirname "$0")/shadow-deploy.sh" "$previous_image" "$previous_sha"; then
    echo "automatic rollback failed; deployment remains fail-closed" >&2
    exit 5
  fi
  if ! release_upgrade_gate; then
    exit 7
  fi
  echo "automatic rollback completed" >&2
  exit "$upgrade_status"
fi
if [[ "$upgrade_gate_armed" == true ]] && ! release_upgrade_gate; then
  exit 7
fi
if [[ "$backup_created" == true ]]; then
  echo "state_backup=/app/state/backups/${backup_name}"
else
  echo "state_backup=none"
fi
