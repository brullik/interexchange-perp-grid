#!/usr/bin/env bash
set -euo pipefail

deployment_state="${IPEG_DEPLOYMENT_STATE_PATH:-.ipeg-deployment-state}"
secrets_file="${IPEG_ENV_FILE:-.env}"

if [[ $# -ne 2 ]]; then
  echo "usage: shadow-deploy.sh IMAGE@sha256:DIGEST FULL_RELEASE_SHA" >&2
  exit 2
fi
image_ref="$1"
release_sha="$2"
if [[ ! "$image_ref" =~ @sha256:[0-9a-f]{64}$ ]]; then
  echo "deployment requires an immutable registry image digest" >&2
  exit 2
fi
if [[ ! "$release_sha" =~ ^[0-9a-f]{40}$ ]]; then
  echo "deployment requires a full Git commit SHA" >&2
  exit 2
fi
if [[ ! -f "$secrets_file" ]]; then
  echo "deployment requires an external $secrets_file secrets file" >&2
  exit 3
fi
if [[ "$(stat -c '%a' "$secrets_file")" != "600" ]]; then
  echo "$secrets_file must have mode 0600" >&2
  exit 3
fi
if [[ "$(git rev-parse --is-inside-work-tree 2>/dev/null || true)" == "true" ]]; then
  if git ls-files --error-unmatch -- "$secrets_file" >/dev/null 2>&1; then
    echo "$secrets_file must not be tracked by Git" >&2
    exit 3
  fi
elif [[ "$secrets_file" != /* ]]; then
  echo "deployment outside a Git checkout requires an absolute external secrets path" >&2
  exit 3
fi
export IPEG_ENV_FILE="$secrets_file"
export IPEG_IMAGE_REF="$image_ref"
export IPEG_RELEASE_SHA="$release_sha"
export IPEG_CONTAINER_IMAGE_DIGEST="${image_ref##*@}"
docker compose pull app
image_revision="$(docker image inspect "$image_ref" \
  --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}')"
if [[ "$image_revision" != "$release_sha" ]]; then
  echo "container image revision label does not match the requested release SHA" >&2
  exit 4
fi
docker compose up --detach --no-build --wait --wait-timeout 180 app
docker compose exec -T app interexchange-grid health --config /app/config/defaults.yaml
docker compose exec -T app interexchange-grid deployment-identity \
  --config /app/config/defaults.yaml \
  --expected-release-sha "$IPEG_RELEASE_SHA" \
  --expected-image-digest "$IPEG_CONTAINER_IMAGE_DIGEST"

state_directory="$(dirname "$deployment_state")"
mkdir -p "$state_directory"
temporary_state="${deployment_state}.tmp.$$"
umask 077
{
  printf 'image_ref=%s\n' "$image_ref"
  printf 'release_sha=%s\n' "$release_sha"
} >"$temporary_state"
mv -f "$temporary_state" "$deployment_state"
echo "deployment_state=$deployment_state"
