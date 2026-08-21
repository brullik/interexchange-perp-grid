from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from interexchange_perp_grid.qualification import code_hash, config_hash

_COMMIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_ARTIFACT_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
NATIVE_RUNTIME_KIND = "native-python"


@dataclass(frozen=True, slots=True)
class NativeRuntimeManifest:
    schema_version: int
    generated_at: datetime
    runtime_kind: str
    release_sha: str
    source_sha256: str
    config_sha256: str
    requirements_lock_sha256: str
    interpreter_sha256: str
    installed_distributions_sha256: str
    python_version: str
    platform: str
    artifact_digest: str

    def __post_init__(self) -> None:
        if self.schema_version != 1 or self.runtime_kind != NATIVE_RUNTIME_KIND:
            raise ValueError("native runtime manifest identity is invalid")
        if not _COMMIT_SHA.fullmatch(self.release_sha):
            raise ValueError("native runtime release SHA is invalid")
        digests = (
            self.source_sha256,
            self.config_sha256,
            self.requirements_lock_sha256,
            self.interpreter_sha256,
            self.installed_distributions_sha256,
        )
        if any(re.fullmatch(r"[0-9a-f]{64}", value) is None for value in digests):
            raise ValueError("native runtime component digest is invalid")
        if not _ARTIFACT_DIGEST.fullmatch(self.artifact_digest):
            raise ValueError("native runtime artifact digest is invalid")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _git(repo_root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", *arguments],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return completed.stdout.strip()


def _release_sha(repo_root: Path) -> str:
    observed = _git(repo_root, "rev-parse", "HEAD").lower()
    if not _COMMIT_SHA.fullmatch(observed):
        raise ValueError("native runtime requires one exact Git commit")
    if _git(repo_root, "status", "--porcelain", "--untracked-files=no"):
        raise ValueError("native runtime requires a clean tracked worktree")
    return observed


def _installed_distributions_sha256() -> str:
    entries: list[str] = []
    for distribution in importlib.metadata.distributions():
        name = distribution.metadata.get("Name")
        if name:
            entries.append(f"{name.strip().lower()}=={distribution.version.strip()}")
    payload = "\n".join(sorted(set(entries))).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _identity_payload(manifest: NativeRuntimeManifest) -> dict[str, object]:
    payload = asdict(manifest)
    payload.pop("generated_at")
    payload["artifact_digest"] = ""
    return payload


def _artifact_digest(manifest: NativeRuntimeManifest) -> str:
    encoded = json.dumps(
        _identity_payload(manifest),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def build_native_runtime_manifest(
    repo_root: Path,
    config_path: Path,
    *,
    now: datetime | None = None,
) -> NativeRuntimeManifest:
    root = repo_root.resolve()
    config = config_path.resolve()
    lock_path = root / "requirements.lock"
    interpreter = Path(sys.executable).resolve()
    if sys.version_info[:2] != (3, 12):
        raise ValueError("native runtime requires CPython 3.12")
    unsigned = NativeRuntimeManifest(
        schema_version=1,
        generated_at=now or datetime.now(UTC),
        runtime_kind=NATIVE_RUNTIME_KIND,
        release_sha=_release_sha(root),
        source_sha256=code_hash(root),
        config_sha256=config_hash(config),
        requirements_lock_sha256=_sha256_file(lock_path),
        interpreter_sha256=_sha256_file(interpreter),
        installed_distributions_sha256=_installed_distributions_sha256(),
        python_version=platform.python_version(),
        platform=platform.platform(),
        artifact_digest="sha256:" + "0" * 64,
    )
    return NativeRuntimeManifest(
        **{
            **asdict(unsigned),
            "artifact_digest": _artifact_digest(unsigned),
        }
    )


def write_native_runtime_manifest(path: Path, manifest: NativeRuntimeManifest) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            stream.write(json.dumps(asdict(manifest), default=str, indent=2, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def load_native_runtime_manifest(path: Path) -> NativeRuntimeManifest:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("native runtime manifest must be an object")
    return NativeRuntimeManifest(
        schema_version=int(payload["schema_version"]),
        generated_at=datetime.fromisoformat(str(payload["generated_at"])),
        runtime_kind=str(payload["runtime_kind"]),
        release_sha=str(payload["release_sha"]),
        source_sha256=str(payload["source_sha256"]),
        config_sha256=str(payload["config_sha256"]),
        requirements_lock_sha256=str(payload["requirements_lock_sha256"]),
        interpreter_sha256=str(payload["interpreter_sha256"]),
        installed_distributions_sha256=str(payload["installed_distributions_sha256"]),
        python_version=str(payload["python_version"]),
        platform=str(payload["platform"]),
        artifact_digest=str(payload["artifact_digest"]),
    )


def verify_native_runtime_manifest(
    path: Path,
    repo_root: Path,
    config_path: Path,
) -> NativeRuntimeManifest:
    expected = load_native_runtime_manifest(path)
    observed = build_native_runtime_manifest(repo_root, config_path, now=expected.generated_at)
    if expected != observed:
        raise ValueError("native runtime no longer matches its immutable manifest")
    return expected


def resolve_runtime_artifact_digest(repo_root: Path, config_path: Path) -> str:
    runtime_kind = os.environ.get("IPEG_RUNTIME_KIND", "container").strip().lower()
    configured_digest = os.environ.get("IPEG_CONTAINER_IMAGE_DIGEST", "").strip().lower()
    if runtime_kind == "container":
        if not _ARTIFACT_DIGEST.fullmatch(configured_digest):
            raise ValueError("immutable container image digest is required")
        return configured_digest
    if runtime_kind != NATIVE_RUNTIME_KIND:
        raise ValueError("runtime kind is unsupported")
    manifest_value = os.environ.get("IPEG_NATIVE_RUNTIME_MANIFEST", "").strip()
    if not manifest_value:
        raise ValueError("native runtime manifest path is required")
    manifest = verify_native_runtime_manifest(Path(manifest_value), repo_root, config_path)
    configured_release = os.environ.get("IPEG_RELEASE_SHA", "").strip().lower()
    if configured_release and configured_release != manifest.release_sha:
        raise ValueError("configured release SHA does not match native manifest")
    if configured_digest and configured_digest != manifest.artifact_digest:
        raise ValueError("configured runtime digest does not match native manifest")
    return manifest.artifact_digest
