from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

from interexchange_perp_grid.risk_stages import load_locked_risk_stage_table
from interexchange_perp_grid.state import (
    RiskStage,
    initialise_state,
    promote_risk_stage,
    read_risk_stage,
    record_risk_stage_result,
)

RUNTIME_POLICY = Path(__file__).resolve().parents[1] / "config" / "RUNTIME_POLICY.yaml"


def test_locked_risk_stage_table_has_exact_order_and_bounded_limits() -> None:
    table = load_locked_risk_stage_table(RUNTIME_POLICY)

    assert tuple(stage.stage for stage in table.stages) == (
        RiskStage.CANARY,
        RiskStage.PILOT_A,
        RiskStage.PILOT_B,
        RiskStage.WAVE1_PROD,
        RiskStage.FULL,
    )
    assert len(table.runtime_policy_sha256) == 64
    assert max(stage.leverage for stage in table.stages) <= 3


def test_locked_risk_stage_table_rejects_reordered_or_weakened_policy(tmp_path: Path) -> None:
    payload = yaml.safe_load(RUNTIME_POLICY.read_text(encoding="utf-8"))
    payload["risk_stages"]["canary"]["leverage"] = "4"
    policy = tmp_path / "RUNTIME_POLICY.yaml"
    policy.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(ValueError, match="leverage"):
        load_locked_risk_stage_table(policy)


@pytest.mark.asyncio
async def test_risk_stage_promotion_is_adjacent_confirmed_and_persisted(tmp_path: Path) -> None:
    state_path = tmp_path / "state.sqlite3"
    await initialise_state(state_path)
    initial = await read_risk_stage(state_path)
    assert initial.stage == RiskStage.SHADOW

    promoted = await promote_risk_stage(
        state_path,
        RiskStage.SHADOW,
        RiskStage.CANARY,
        "a" * 64,
        "b" * 64,
        "owner",
        "PROMOTE:canary",
        datetime(2026, 8, 21, tzinfo=UTC),
    )

    assert promoted.stage == RiskStage.CANARY
    assert promoted.qualification_hash == "a" * 64
    assert await read_risk_stage(state_path) == promoted
    with pytest.raises(ValueError, match="exactly one"):
        await promote_risk_stage(
            state_path,
            RiskStage.CANARY,
            RiskStage.WAVE1_PROD,
            "a" * 64,
            "b" * 64,
            "owner",
            "PROMOTE:wave1_prod",
        )
    with pytest.raises(ValueError, match="confirmation"):
        await promote_risk_stage(
            state_path,
            RiskStage.CANARY,
            RiskStage.PILOT_A,
            "a" * 64,
            "b" * 64,
            "owner",
            "PROMOTE:canary",
        )
    with pytest.raises(RuntimeError, match="stable-FLAT"):
        await promote_risk_stage(
            state_path,
            RiskStage.CANARY,
            RiskStage.PILOT_A,
            "a" * 64,
            "b" * 64,
            "owner",
            "PROMOTE:pilot_a",
        )
    completed = await record_risk_stage_result(
        state_path,
        RiskStage.CANARY,
        "a" * 64,
        "b" * 64,
        "c" * 64,
        True,
        "owner",
    )
    assert completed.stable_flat_verified is True
    pilot = await promote_risk_stage(
        state_path,
        RiskStage.CANARY,
        RiskStage.PILOT_A,
        "a" * 64,
        "b" * 64,
        "owner",
        "PROMOTE:pilot_a",
    )
    assert pilot.stage == RiskStage.PILOT_A


@pytest.mark.asyncio
async def test_risk_stage_promotion_rejects_stale_expected_state(tmp_path: Path) -> None:
    state_path = tmp_path / "state.sqlite3"
    await initialise_state(state_path)

    with pytest.raises(RuntimeError, match="changed concurrently"):
        await promote_risk_stage(
            state_path,
            RiskStage.CANARY,
            RiskStage.PILOT_A,
            "a" * 64,
            "b" * 64,
            "owner",
            "PROMOTE:pilot_a",
        )
