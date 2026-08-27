from __future__ import annotations

import re
import sqlite3
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from interexchange_perp_grid.aggressive_activation import (
    AggressiveFastLiveBinding,
    _binding_sha256,
)
from interexchange_perp_grid.aggressive_model import DivergenceDirection
from interexchange_perp_grid.aggressive_runtime import AggressiveTrancheIntent
from interexchange_perp_grid.canary_runtime import _fast_live_data_generation_sha256
from interexchange_perp_grid.domain import (
    BookLevel,
    FundingSnapshot,
    Instrument,
    OrderBookSnapshot,
    Venue,
)
from interexchange_perp_grid.market_data import DataQualityAssessment
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
    assert "runtime_manifest_sha256" in text
    assert "Assert-SafetySupervisorReady" in text
    assert "Test-SupervisorReadinessEvidence" in text
    assert "Restore-PilotEvidenceAndAcceptance" in text
    assert "state/fast-live-independent-review.json" in text
    assert '"--independent-review", $independentReview' in text
    preflight_body = text.index("function Invoke-Preflight")
    assert text.index("Restore-PilotEvidenceAndAcceptance", preflight_body) < text.index(
        "Ensure-HistoryAndModel", preflight_body
    )
    assert "laptop-fast-live-supervisor-runtime.json" in text
    assert "$utcNow.Minute, 0" in text
    assert text.index("Restore-CanaryEvidence", preflight_body) < text.index(
        "Ensure-HistoryAndModel", preflight_body
    )


def test_fast_live_binding_identity_does_not_depend_on_construction_clock() -> None:
    binding = AggressiveFastLiveBinding(
        1,
        datetime(2026, 1, 1, tzinfo=UTC),
        "a" * 40,
        "b" * 64,
        "c" * 64,
        "d" * 64,
        "e" * 64,
        "f" * 64,
        "1" * 64,
        "2" * 64,
        "3" * 64,
        "BTC:okx>bybit",
        DivergenceDirection.POSITIVE,
        "30",
        10,
        "0.7",
        True,
        "0" * 64,
    )
    assert _binding_sha256(binding) == _binding_sha256(
        replace(binding, generated_at=binding.generated_at + timedelta(seconds=1))
    )


def test_fast_live_data_generation_binds_exact_book_and_funding_observation() -> None:
    observed = datetime(2026, 8, 27, tzinfo=UTC)
    book = OrderBookSnapshot(
        Venue.BYBIT,
        "BTC/USDT:USDT",
        (BookLevel(Decimal("100"), Decimal("1")),),
        (BookLevel(Decimal("101"), Decimal("1")),),
        1000,
        observed,
        123,
        10,
        11,
        True,
        True,
        2,
    )
    funding = FundingSnapshot(
        Venue.BYBIT,
        book.symbol,
        Decimal("0.0001"),
        2000,
        "8h",
        Decimal("100.5"),
        Decimal("100.4"),
        1001,
    )
    instrument = SimpleNamespace(
        symbol=book.symbol,
        base="BTC",
        quote="USDT",
        settle="USDT",
        contract_size_base=Decimal("1"),
        amount_step_contracts=Decimal("0.001"),
        price_tick=Decimal("0.1"),
        minimum_notional=Decimal("5"),
    )
    intent = SimpleNamespace(
        reference_interval_start=observed - timedelta(minutes=1),
        reference_spread_bps=Decimal("10"),
        quantity=Decimal("0.01"),
    )
    quality = {Venue.BYBIT: SimpleNamespace(accepted=True, reason=SimpleNamespace(value="PASS"))}

    def digest(candidate_book: OrderBookSnapshot, candidate_funding: FundingSnapshot) -> str:
        return _fast_live_data_generation_sha256(
            cast(dict[Venue, Instrument], {Venue.BYBIT: instrument}),
            "a" * 64,
            "b" * 64,
            cast(AggressiveTrancheIntent, intent),
            {Venue.BYBIT: candidate_book},
            cast(dict[Venue, DataQualityAssessment], quality),
            {Venue.BYBIT: candidate_funding},
        )

    baseline = digest(book, funding)
    assert (
        digest(replace(book, bids=(BookLevel(Decimal("99.9"), Decimal("1")),)), funding) != baseline
    )
    assert (
        digest(replace(book, bids=(BookLevel(Decimal("100"), Decimal("2")),)), funding) != baseline
    )
    assert digest(replace(book, exchange_timestamp_ms=1002), funding) != baseline
    assert digest(book, replace(funding, exchange_timestamp_ms=1002)) != baseline


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


@pytest.mark.asyncio
async def test_fast_live_reselection_clears_legacy_qualification_and_freeze(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state.sqlite3"
    await initialise_state(state)
    policy = load_locked_risk_stage_table(Path("config/RUNTIME_POLICY.yaml"))
    await select_fast_live_risk_stage(
        state, RiskStage.CANARY, policy.runtime_policy_sha256, "first"
    )
    with sqlite3.connect(state) as database:
        database.execute(
            "UPDATE risk_stage_runtime SET qualification_hash = ?, completion_frozen = 1",
            ("a" * 64,),
        )
        database.execute("UPDATE live_entry_controls SET risk_stage_completion_frozen = 1")
    refreshed = await select_fast_live_risk_stage(
        state, RiskStage.CANARY, policy.runtime_policy_sha256, "second"
    )

    assert refreshed.qualification_hash is None
    assert refreshed.completion_frozen is False


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


@pytest.mark.skipif(sys.platform != "win32", reason="Windows PowerShell 5.1 only")
def test_fast_live_supervisor_readiness_evidence_selftest() -> None:
    completed = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(Path("scripts/laptop-fast-live.ps1").resolve()),
            "-Action",
            "verify",
            "-SupervisorReadinessSelfTest",
        ],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=20,
    )

    assert "Supervisor readiness evidence self-test PASS" in completed.stdout
