#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
release_sha="$(git rev-parse HEAD)"
if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
  echo "tracked worktree must be clean" >&2
  exit 2
fi
image_tag="${1:-interexchange-perp-grid:${release_sha}}"
docker build --label "org.opencontainers.image.revision=${release_sha}" --tag "$image_tag" .
image_digest="$(docker image inspect "$image_tag" --format '{{.Id}}')"
mkdir -p artifacts/runtime
if command -v cyclonedx-py >/dev/null 2>&1; then
  cyclonedx-py requirements requirements.lock --pyproject pyproject.toml \
    --output-reproducible --output-format JSON \
    --output-file artifacts/runtime/release-sbom.json
fi
python -c 'import hashlib,json,pathlib,sys; p=pathlib.Path("artifacts/runtime/release-manifest.json"); p.write_text(json.dumps({"release_sha":sys.argv[1],"image":sys.argv[2],"image_digest":sys.argv[3],"requirements_lock_sha256":hashlib.sha256(pathlib.Path("requirements.lock").read_bytes()).hexdigest()},sort_keys=True)+"\n",encoding="utf-8")' "$release_sha" "$image_tag" "$image_digest"
interexchange-grid release-preflight \
  --repo-root . --config config/defaults.yaml --image-digest "$image_digest"
echo "release_sha=$release_sha image=$image_tag image_digest=$image_digest"
