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
    s4u_migrator = Path("scripts/laptop-migrate-s4u-profile.ps1").read_text(encoding="utf-8")
    s4u_loader = Path("scripts/laptop-load-s4u-env.ps1").read_text(encoding="utf-8")
    qualification = Path("scripts/laptop-qualification.ps1").read_text(encoding="utf-8")
    pilot = Path("scripts/laptop-pilot.ps1").read_text(encoding="utf-8")
    aggressive = Path("scripts/laptop-aggressive.ps1").read_text(encoding="utf-8")
    aggressive_pilot = Path("scripts/laptop-aggressive-pilot-a.ps1").read_text(encoding="utf-8")

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
    assert "DataProtectionScope]::LocalMachine" in s4u_migrator
    assert "DataProtectionScope]::LocalMachine" in s4u_loader
    assert "Add-Type -AssemblyName System.Security" in s4u_migrator
    assert "Add-Type -AssemblyName System.Security" in s4u_loader
    assert '/inheritance:r /grant:r "${owner}:(F)"' in s4u_migrator
    assert "[IO.File]::GetAccessControl" in s4u_loader
    assert "Get-Acl" not in s4u_loader
    assert "AreAccessRulesProtected" in s4u_loader
    assert "ACL permits another identity" in s4u_loader
    assert '$env:IPEG_LIVE_ENABLED = "false"' in s4u_loader
    assert "Write-Host $plainJson" not in s4u_migrator
    assert "Write-Host $plainJson" not in s4u_loader
    assert "Docker" not in qualification
    assert '[ValidateSet("CurrentUser", "LocalMachine")]' in qualification
    assert "laptop-load-s4u-env.ps1" in qualification
    assert "private-probe" in qualification
    assert "--authenticated" in qualification
    assert 'foreach ($venue in @("bybit", "okx"))' in qualification
    assert 'foreach ($venue in @("binanceusdm", "bybit", "okx"))' not in qualification
    assert "$env:TEMP = $laptopTemp" in qualification
    assert "$env:TMP = $laptopTemp" in qualification
    assert "SetThreadExecutionState" in qualification
    assert "OwnerException12h" in qualification
    assert '"IPEG_LAPTOP_12H_OWNER_EXCEPTION"' in qualification
    assert "I_ACCEPT_LAPTOP_12H_QUALIFICATION_EXCEPTION" in qualification
    assert "--laptop-owner-exception-12h" in qualification
    assert "--maximum-hours 18" in qualification
    assert 'if ($consent -cne "I_ACCEPT_LIVE_CANARY_RISK")' in pilot
    assert "I_ACCEPT_LAPTOP_12H_QUALIFICATION_EXCEPTION" in pilot
    assert "--laptop-owner-exception-12h" in pilot
    assert '$env:IPEG_LIVE_ENABLED = "true"' in pilot
    assert '$env:IPEG_LIVE_ENABLED = "false"' in pilot
    assert '"--duration-seconds", "33000"' in pilot
    assert '"--receipt", $serviceReceipt' in pilot
    assert "--service-receipt $serviceReceipt" in pilot
    assert "laptop-pilot-report" in pilot
    assert "SetThreadExecutionState" in pilot
    assert "aggressive-live-intent-once" in pilot
    assert "--aggressive-intent $aggressiveIntentPath" in pilot
    assert "aggressive-laptop-stage-report" in pilot
    assert '[ValidateSet("CurrentUser", "LocalMachine")]' in pilot
    assert "laptop-load-s4u-env.ps1" in pilot
    assert "LIVE_CANARY_CONSENT" not in loader
    assert "risk-stage-promote" in pilot and "PROMOTE:canary" in pilot
    assert pilot.index("Start-Process") < pilot.index('$env:IPEG_LIVE_ENABLED = "true"')
    assert pilot.index('$env:IPEG_LIVE_ENABLED = "false"') < pilot.index("Start-Process")
    assert (
        'ValidateSet("verify", "shadow", "qualify", "canary", "pilot", "status", "stop")'
        in aggressive
    )
    assert "reference-history-proof" in aggressive
    assert "aggressive-shadow-once" in aggressive
    assert "Start-Sleep -Seconds 60" in aggressive
    assert 'IPEG_LIVE_ENABLED -cne "false"' in aggressive
    assert "separate, explicit live-money authorization" in aggressive
    assert "laptop-pilot.ps1" in aggressive and "-Aggressive" in aggressive
    assert "laptop-aggressive-pilot-a.ps1" in aggressive
    assert "Docker" not in aggressive
    assert "I_ACCEPT_AGGRESSIVE_PILOT_A_RISK" in aggressive_pilot
    assert "minimum_duration_seconds -ne 86400" in aggressive_pilot
    assert "--aggressive-stage pilot_a" in aggressive_pilot
    assert '"--duration-seconds", "28800"' in aggressive_pilot
    assert "--service-receipt $postFlatReceipt" in aggressive_pilot
    assert '$env:IPEG_LIVE_ENABLED = "false"' in aggressive_pilot
    assert "Docker" not in aggressive_pilot


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


@pytest.mark.skipif(sys.platform != "win32", reason="Windows PowerShell 5.1 only")
def test_windows_s4u_local_machine_dpapi_profile_round_trip(tmp_path: Path) -> None:
    fixture = tmp_path / "current-user-profile.clixml"
    envelope = tmp_path / "s4u-profile.json"
    migration = Path("scripts/laptop-migrate-s4u-profile.ps1").resolve()
    loader = Path("scripts/laptop-load-s4u-env.ps1").resolve()
    test_script = tmp_path / "round-trip.ps1"
    test_script.write_text(
        f"""
$ErrorActionPreference = "Stop"
function New-Secret([string]$Value) {{
    $secure = [Security.SecureString]::new()
    foreach ($character in $Value.ToCharArray()) {{ $secure.AppendChar($character) }}
    $secure.MakeReadOnly()
    return $secure
}}
$payload = [pscustomobject]@{{
    SchemaVersion = 1
    QualificationRoute = "BTC:bybit>okx"
    TelegramOwnerChatId = "123456789"
    TelegramBotToken = New-Secret "123456789:AA_ab-CD/+= token"
    BinanceUsdmApiKey = New-Secret "binance-key"
    BinanceUsdmApiSecret = New-Secret "binance-secret"
    BinanceUsdmApiPassword = New-Secret ""
    BybitApiKey = New-Secret "bybit-key"
    BybitApiSecret = New-Secret "bybit-secret"
    BybitApiPassword = New-Secret ""
    OkxApiKey = New-Secret "okx-key"
    OkxApiSecret = New-Secret "okx-secret"
    OkxApiPassword = New-Secret "okx-passphrase"
}}
[IO.File]::WriteAllText(
    '{fixture.as_posix()}',
    [Management.Automation.PSSerializer]::Serialize($payload),
    [Text.UTF8Encoding]::new($false)
)
& '{migration.as_posix()}' `
    -InputPath '{fixture.as_posix()}' `
    -OutputPath '{envelope.as_posix()}'
. '{loader.as_posix()}' `
    -ProfilePath '{envelope.as_posix()}' `
    -StatePath '{(tmp_path / "state.sqlite3").as_posix()}' `
    -MarketPath '{(tmp_path / "market").as_posix()}'
if ($env:IPEG_MODE -cne "shadow" -or $env:IPEG_LIVE_ENABLED -cne "false") {{
    throw "S4U loader widened live authority"
}}
if ($env:IPEG_QUALIFICATION_ROUTE -cne "BTC:bybit>okx") {{
    throw "S4U loader changed the locked route"
}}
if (
    $env:IPEG_TELEGRAM_BOT_TOKEN -cne "123456789:AA_ab-CD/+= token" -or
    $env:IPEG_BYBIT_API_SECRET -cne "bybit-secret" -or
    $env:IPEG_OKX_API_PASSWORD -cne "okx-passphrase"
) {{
    throw "S4U profile round-trip changed a credential"
}}
Write-Host "S4U local-machine DPAPI round-trip PASS"
""",
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(test_script),
        ],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=30,
    )

    assert "S4U local-machine DPAPI round-trip PASS" in completed.stdout
    assert "bybit-secret" not in completed.stdout
    assert "okx-passphrase" not in completed.stdout
