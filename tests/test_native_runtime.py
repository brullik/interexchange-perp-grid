from __future__ import annotations

import subprocess
import sys
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

    assert "Export-Clixml" not in onboarding
    assert "Import-Clixml" not in loader
    assert "[Management.Automation.PSSerializer]::Serialize($payload)" in onboarding
    assert "[Management.Automation.PSSerializer]::Deserialize($serialized)" in loader
    assert "UseSystemPasswordChar = $true" in onboarding
    assert "ShortcutsEnabled = $true" in onboarding
    assert "$input" not in onboarding
    assert "Add_Shown" not in onboarding
    assert "$form.ActiveControl = $secretTextBox" in onboarding
    assert "ConvertTo-SecureString" not in onboarding
    assert "$secure.AppendChar($Value[$index])" in onboarding
    assert "$secure.MakeReadOnly()" in onboarding
    assert "$secretTextBox.Clear()" in onboarding
    assert "Write-Host $plain" not in onboarding
    assert 'QualificationRoute = "BTC:bybit>okx"' in onboarding
    assert "LIVE_CANARY_CONSENT" not in onboarding
    assert "IPEG_LOCAL_UNLOCK_SECRET" not in onboarding
    assert '$env:IPEG_LIVE_ENABLED = "false"' in loader
    assert '$env:IPEG_MODE = "shadow"' in loader
    assert '$env:IPEG_RUNTIME_KIND = "native-python"' in loader
    assert "Docker" not in qualification
    assert "private-probe" in qualification
    assert "--authenticated" in qualification
    assert 'foreach ($venue in @("bybit", "okx"))' in qualification
    assert 'foreach ($venue in @("binanceusdm", "bybit", "okx"))' not in qualification
    assert "$env:TEMP = $laptopTemp" in qualification
    assert "$env:TMP = $laptopTemp" in qualification
    assert "SetThreadExecutionState" in qualification
    assert 'if ($consent -cne "I_ACCEPT_LIVE_CANARY_RISK")' in pilot
    assert '$env:IPEG_LIVE_ENABLED = "true"' in pilot
    assert '$env:IPEG_LIVE_ENABLED = "false"' in pilot
    assert '"--duration-seconds", "33000"' in pilot
    assert '"--receipt", $serviceReceipt' in pilot
    assert "--service-receipt $serviceReceipt" in pilot
    assert "laptop-pilot-report" in pilot
    assert "SetThreadExecutionState" in pilot
    assert "LIVE_CANARY_CONSENT" not in loader
    assert "risk-stage-promote" in pilot and "PROMOTE:canary" in pilot
    assert pilot.index("Start-Process") < pilot.index('$env:IPEG_LIVE_ENABLED = "true"')
    assert pilot.index('$env:IPEG_LIVE_ENABLED = "false"') < pilot.index("Start-Process")


@pytest.mark.skipif(sys.platform != "win32", reason="Windows PowerShell 5.1 only")
def test_windows_onboarding_secure_string_runtime_round_trip() -> None:
    completed = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(Path("scripts/laptop-onboard.ps1").resolve()),
            "-RuntimeSelfTest",
        ],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=20,
    )

    assert "SecureString and DPAPI serializer round-trip PASS" in completed.stdout

    dialog_completed = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(Path("scripts/laptop-onboard.ps1").resolve()),
            "-DialogSelfTest",
        ],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=20,
    )

    assert "Secret dialog runtime round-trip PASS" in dialog_completed.stdout
