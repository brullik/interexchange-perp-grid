#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: shadow-deploy.sh IMAGE@sha256:DIGEST FULL_RELEASE_SHA" >&2
  exit 2
fi
release_sha="$2"
bash "$(dirname "$0")/require-fast-live-acceptance.sh" "$release_sha"
exec bash "$(dirname "$0")/shadow-deploy-mechanics.sh" "$@"
