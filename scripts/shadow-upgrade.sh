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
if ! command -v flock >/dev/null 2>&1; then
  echo "flock is required for serialized upgrades" >&2
  exit 8
fi
upgrade_lock="${deployment_state}.upgrade.lock"
exec 9>"$upgrade_lock"
if ! flock -n 9; then
  echo "another deployment upgrade is already in progress" >&2
  exit 8
fi
upgrade_owner="deployment-upgrade-${target_sha}"
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
run_target_compose() {
  IPEG_IMAGE_REF="$target_image" \
  IPEG_RELEASE_SHA="$target_sha" \
  IPEG_CONTAINER_IMAGE_DIGEST="${target_image##*@}" \
    docker compose "$@"
}
run_upgrade_gate() {
  run_target_compose run --rm --no-deps app interexchange-grid deployment-upgrade-gate \
      --config /app/config/defaults.yaml --action "$1" --owner-token "$upgrade_owner"
}
run_previous_compose() {
  IPEG_IMAGE_REF="$previous_image" \
  IPEG_RELEASE_SHA="$previous_sha" \
  IPEG_CONTAINER_IMAGE_DIGEST="${previous_image##*@}" \
    docker compose "$@"
}
release_upgrade_gate() {
  if ! run_upgrade_gate release; then
    echo "target deployment could not release the upgrade entry freeze" >&2
    return 1
  fi
  upgrade_gate_armed=false
}
app_was_running=false
if docker compose ps --status running --services | grep -qx app; then
  app_was_running=true
fi
if [[ "$app_was_running" == true ]] \
  && [[ -z "$previous_image" || -z "$previous_sha" ]]; then
  echo "running service has no verified immutable deployment identity" >&2
  exit 3
fi
if [[ -n "$previous_image" && -n "$previous_sha" ]]; then
  IPEG_IMAGE_REF="$target_image" docker compose pull app
  target_revision="$(IPEG_RELEASE_SHA="$target_sha" docker image inspect "$target_image" \
    --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}')"
  if [[ "$target_revision" != "$target_sha" ]]; then
    echo "upgrade image revision label does not match the requested release SHA" >&2
    exit 3
  fi
  if [[ "$app_was_running" == true ]]; then
    docker compose pause app
  fi
  if ! run_previous_compose run --rm --no-deps app interexchange-grid backup-state \
    --config /app/config/defaults.yaml --target "/app/state/backups/${backup_name}"; then
    if [[ "$app_was_running" == true ]]; then
      docker compose unpause app
    elif ! bash "$(dirname "$0")/shadow-deploy.sh" "$previous_image" "$previous_sha"; then
      echo "backup failed and the previous recovery service could not restart" >&2
      exit 5
    fi
    echo "upgrade aborted because the paused-state backup failed" >&2
    exit 4
  fi
  backup_created=true
  if ! run_upgrade_gate arm; then
    if [[ "$app_was_running" == true ]]; then
      docker compose unpause app
    elif ! bash "$(dirname "$0")/shadow-deploy.sh" "$previous_image" "$previous_sha"; then
      echo "upgrade blocked and the previous recovery service could not restart" >&2
      exit 5
    fi
    echo "upgrade aborted before deployment; old service resumed for risk reduction" >&2
    exit 6
  fi
  upgrade_gate_armed=true
  if [[ "$app_was_running" == true ]]; then
    docker compose kill app
  fi
fi
upgrade_status=0
if bash "$(dirname "$0")/shadow-deploy.sh" "$1" "$2"; then
  if [[ "$upgrade_gate_armed" == true ]] && ! release_upgrade_gate; then
    upgrade_status=7
  fi
else
  upgrade_status=$?
fi
if [[ "$upgrade_status" -ne 0 ]]; then
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
    run_target_compose run --rm --no-deps app interexchange-grid restore-state \
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
if [[ "$backup_created" == true ]]; then
  echo "state_backup=/app/state/backups/${backup_name}"
else
  echo "state_backup=none"
fi
