#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: shadow-upgrade.sh IMAGE@sha256:DIGEST FULL_RELEASE_SHA" >&2
  exit 2
fi
target_sha="$2"
bash "$(dirname "$0")/require-fast-live-acceptance.sh" "$target_sha"
exec bash "$(dirname "$0")/shadow-upgrade-mechanics.sh" "$@"
