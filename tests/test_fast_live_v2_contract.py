from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

from interexchange_perp_grid.risk_stages import load_locked_risk_stage_table
from interexchange_perp_grid.state import (
    RiskStage,
    initialise_state,
    select_fast_live_risk_stage,
)


def test_fast_live_wrapper_exposes_exact_actions_and_no_qualification_path() -> None:
    text = Path("scripts/laptop-fast-live.ps1").read_text(encoding="utf-8")
    match = re.search(r"\[ValidateSet\(([^)]*)\)\]\s*\[string\]\$Action", text)

    assert match is not None
    actions = tuple(re.findall(r'"([a-z]+)"', match.group(1)))
    assert actions == ("verify", "onboard", "preflight", "canary", "pilot", "status", "stop")
    assert "qualif" not in text.lower()
    assert "scheduledtask" not in text.lower()
    assert "AGGRESSIVE_FAST_LIVE_V2.yaml" in text
    assert "fast-live-preflight" in text
    assert "fast-live-canary" in text
    assert "fast-live-pilot" in text
    assert 'IPEG_MODE = "shadow"' in text
    assert 'IPEG_LIVE_ENABLED = "false"' in text
    assert "IPEG_LOCAL_UNLOCK_SECRET" in text
    assert "laptop-load-env.ps1" in text
    assert "laptop-load-s4u-env.ps1" in text
    assert "laptop-onboard.ps1" in text


@pytest.mark.asyncio
async def test_fast_live_stage_has_no_qualification_lineage_and_pilot_requires_canary_cycle(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state.sqlite3"
    await initialise_state(state)
    policy = load_locked_risk_stage_table(Path("config/RUNTIME_POLICY.yaml"))

    canary = await select_fast_live_risk_stage(
        state,
        RiskStage.CANARY,
        policy.runtime_policy_sha256,
        "test-owner",
    )

    assert canary.stage == RiskStage.CANARY
    assert canary.qualification_hash is None
    with pytest.raises(RuntimeError, match="genuine completed canary"):
        await select_fast_live_risk_stage(
            state,
            RiskStage.PILOT_A,
            policy.runtime_policy_sha256,
            "test-owner",
        )


@pytest.mark.parametrize(
    "path",
    [
        "scripts/laptop-qualification.ps1",
        "scripts/laptop-qualification-scheduled.ps1",
        "scripts/laptop-smoke-detached.ps1",
        "scripts/laptop-pilot.ps1",
        "scripts/laptop-aggressive-pilot-a.ps1",
    ],
)
def test_legacy_laptop_entrypoints_fail_before_operational_work(path: str) -> None:
    text = Path(path).read_text(encoding="utf-8")
    strict = text.index("Set-StrictMode -Version Latest")
    blocked = text.index("throw", strict)

    assert blocked > strict
    assert blocked < text.find("Set-Location") if "Set-Location" in text else True
    assert (
        "disabled" in text[blocked : blocked + 180].lower()
        or "non-authoritative" in text[blocked : blocked + 180].lower()
    )


@pytest.mark.skipif(sys.platform != "win32", reason="Windows PowerShell parser only")
def test_fast_live_wrapper_is_valid_windows_powershell() -> None:
    path = Path("scripts/laptop-fast-live.ps1").resolve()
    command = (
        "$ErrorActionPreference='Stop';"
        f"[void][ScriptBlock]::Create([IO.File]::ReadAllText('{path}'));"
        "Write-Output PASS"
    )
    completed = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            command,
        ],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=20,
    )

    assert completed.stdout.strip() == "PASS"
