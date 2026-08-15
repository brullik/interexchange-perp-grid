#!/usr/bin/env bash
set -euo pipefail

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
export IPEG_IMAGE_REF="$image_ref"
export IPEG_RELEASE_SHA="$release_sha"
export IPEG_CONTAINER_IMAGE_DIGEST="${image_ref##*@}"
docker compose pull app
docker compose up --detach --no-build --wait --wait-timeout 180 app
docker compose exec -T app interexchange-grid health --config /app/config/defaults.yaml
