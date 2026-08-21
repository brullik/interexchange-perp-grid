from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from interexchange_perp_grid.native_runtime import (
    NATIVE_RUNTIME_KIND,
    build_native_runtime_manifest,
    load_native_runtime_manifest,
    resolve_runtime_artifact_digest,
    verify_native_runtime_manifest,
    write_native_runtime_manifest,
)


def _git(repo: Path, *arguments: str) -> None:
    subprocess.run(
        ["git", *arguments],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )


def _repo(tmp_path: Path) -> tuple[Path, Path]:
    repo = tmp_path / "repo"
    source = repo / "src" / "package"
    source.mkdir(parents=True)
    (source / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    config = repo / "config.yaml"
    config.write_text("app: shadow\n", encoding="utf-8")
    (repo / "requirements.lock").write_text("pip==26.0.1\n", encoding="utf-8")
    _git(repo, "init")
    _git(repo, "add", ".")
    _git(
        repo,
        "-c",
        "user.name=Codex Test",
        "-c",
        "user.email=codex@example.invalid",
        "commit",
        "-m",
        "fixture",
    )
    return repo, config


def test_native_manifest_binds_clean_source_config_interpreter_and_dependencies(
    tmp_path: Path,
) -> None:
    repo, config = _repo(tmp_path)
    path = repo / "state" / "native-runtime.json"

    manifest = build_native_runtime_manifest(repo, config)
    write_native_runtime_manifest(path, manifest)

    assert manifest.runtime_kind == NATIVE_RUNTIME_KIND
    assert manifest.artifact_digest.startswith("sha256:")
    assert len(manifest.artifact_digest) == 71
    assert load_native_runtime_manifest(path) == manifest
    assert verify_native_runtime_manifest(path, repo, config) == manifest


def test_native_manifest_rejects_dirty_or_changed_runtime(
    tmp_path: Path,
) -> None:
    repo, config = _repo(tmp_path)
    path = repo / "state" / "native-runtime.json"
    manifest = build_native_runtime_manifest(repo, config)
    write_native_runtime_manifest(path, manifest)

    module = repo / "src" / "package" / "module.py"
    module.write_text("VALUE = 2\n", encoding="utf-8")
    with pytest.raises(ValueError, match="clean tracked worktree"):
        verify_native_runtime_manifest(path, repo, config)

    _git(repo, "add", ".")
    _git(
        repo,
        "-c",
        "user.name=Codex Test",
        "-c",
        "user.email=codex@example.invalid",
        "commit",
        "-m",
        "change",
    )
    with pytest.raises(ValueError, match="no longer matches"):
        verify_native_runtime_manifest(path, repo, config)


def test_runtime_digest_resolver_requires_exact_native_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, config = _repo(tmp_path)
    path = repo / "state" / "native-runtime.json"
    manifest = build_native_runtime_manifest(repo, config)
    write_native_runtime_manifest(path, manifest)
    monkeypatch.setenv("IPEG_RUNTIME_KIND", NATIVE_RUNTIME_KIND)
    monkeypatch.setenv("IPEG_NATIVE_RUNTIME_MANIFEST", str(path))
    monkeypatch.delenv("IPEG_CONTAINER_IMAGE_DIGEST", raising=False)

    assert resolve_runtime_artifact_digest(repo, config) == manifest.artifact_digest

    monkeypatch.setenv("IPEG_RELEASE_SHA", "f" * 40)
    with pytest.raises(ValueError, match="release SHA"):
        resolve_runtime_artifact_digest(repo, config)
    monkeypatch.setenv("IPEG_RELEASE_SHA", manifest.release_sha)
    monkeypatch.setenv("IPEG_CONTAINER_IMAGE_DIGEST", "sha256:" + "f" * 64)
    with pytest.raises(ValueError, match="does not match"):
        resolve_runtime_artifact_digest(repo, config)


def test_windows_onboarding_keeps_live_consent_out_of_encrypted_profile() -> None:
    onboarding = Path("scripts/laptop-onboard.ps1").read_text(encoding="utf-8")
    loader = Path("scripts/laptop-load-env.ps1").read_text(encoding="utf-8")
    qualification = Path("scripts/laptop-qualification.ps1").read_text(encoding="utf-8")
    pilot = Path("scripts/laptop-pilot.ps1").read_text(encoding="utf-8")

    assert "Export-Clixml" in onboarding
    assert "Read-Host" in onboarding and "-AsSecureString" in onboarding
    assert 'QualificationRoute = "BTC:bybit>okx"' in onboarding
    assert "LIVE_CANARY_CONSENT" not in onboarding
    assert "IPEG_LOCAL_UNLOCK_SECRET" not in onboarding
    assert '$env:IPEG_LIVE_ENABLED = "false"' in loader
    assert '$env:IPEG_MODE = "shadow"' in loader
    assert '$env:IPEG_RUNTIME_KIND = "native-python"' in loader
    assert "Docker" not in qualification
    assert "private-probe" in qualification
    assert "SetThreadExecutionState" in qualification
    assert 'if ($consent -cne "I_ACCEPT_LIVE_CANARY_RISK")' in pilot
    assert '$env:IPEG_LIVE_ENABLED = "true"' in pilot
    assert '$env:IPEG_LIVE_ENABLED = "false"' in pilot
    assert '"--duration-seconds", "33000"' in pilot
    assert "laptop-pilot-report" in pilot
    assert "SetThreadExecutionState" in pilot
    assert "LIVE_CANARY_CONSENT" not in loader
    assert "risk-stage-promote" in pilot and "PROMOTE:canary" in pilot
    assert pilot.index("Start-Process") < pilot.index('$env:IPEG_LIVE_ENABLED = "true"')
    assert pilot.index('$env:IPEG_LIVE_ENABLED = "false"') < pilot.index("Start-Process")
