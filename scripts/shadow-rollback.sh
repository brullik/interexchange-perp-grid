#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: shadow-rollback.sh PREVIOUS_IMAGE@sha256:DIGEST PREVIOUS_FULL_SHA" >&2
  exit 2
fi
bash "$(dirname "$0")/shadow-upgrade.sh" "$1" "$2"
