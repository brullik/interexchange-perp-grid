from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

import interexchange_perp_grid.aggressive_laptop_acceptance as acceptance_module
from interexchange_perp_grid.aggressive_laptop_acceptance import (
    AggressiveLaptopStageEvidence,
    build_aggressive_laptop_acceptance,
    build_aggressive_laptop_stage_evidence,
    build_aggressive_laptop_stage_evidence_from_journal,
    load_aggressive_laptop_acceptance,
    save_aggressive_laptop_acceptance,
    verify_aggressive_laptop_handoff,
)
from interexchange_perp_grid.aggressive_qualification import (
    AggressiveDirectionBinding,
    AggressiveQualificationBinding,
)
from interexchange_perp_grid.native_runtime import NativeRuntimeManifest

_NOW = datetime(2026, 8, 25, tzinfo=UTC)


def _runtime() -> NativeRuntimeManifest:
    return NativeRuntimeManifest(
        1,
        _NOW,
        "native-python",
        "1" * 40,
        "2" * 64,
        "3" * 64,
        "4" * 64,
        "5" * 64,
        "6" * 64,
        "3.12.13",
        "Windows",
        "sha256:" + "7" * 64,
    )


def _binding() -> AggressiveQualificationBinding:
    direction = AggressiveDirectionBinding(
        "BTC:bybit>okx",
        tuple(Decimal(index) for index in range(1, 6)),
        (Decimal(".10"), Decimal(".15"), Decimal(".20"), Decimal(".25"), Decimal(".30")),
        Decimal(6),
    )
    return AggressiveQualificationBinding(
        1,
        _NOW,
        "8" * 64,
        "9" * 64,
        _runtime().release_sha,
        _runtime().source_sha256,
        _runtime().config_sha256,
        _runtime().artifact_digest,
        "a" * 64,
        "b" * 64,
        "c" * 64,
        "d" * 64,
        "e" * 64,
        "BTC:bybit>okx",
        direction,
        replace(direction, route_identity="BTC:okx>bybit"),
        True,
        "f" * 64,
    )


def _stage(
    stage: str,
    *,
    levels: tuple[int, ...] | None = None,
    maximum_loss: Decimal | None = None,
    post_flat_seconds: int | None = None,
) -> AggressiveLaptopStageEvidence:
    is_canary = stage == "canary"
    return build_aggressive_laptop_stage_evidence(
        stage=stage,
        started_at=_NOW,
        ended_at=_NOW + timedelta(hours=9),
        route_identity="BTC:bybit>okx",
        aggressive_binding_sha256=_binding().binding_sha256,
        completed_level_indices=levels or ((1,) if is_canary else (1, 2, 3, 4, 5)),
        completed_actions_sha256=("1" if is_canary else "2") * 64,
        production_filled_order_count=4 if is_canary else 20,
        active_action_count=0,
        maximum_projected_route_loss_usdt=(
            maximum_loss if maximum_loss is not None else Decimal(1 if is_canary else 5)
        ),
        stable_flat=True,
        post_flat_service_seconds=(
            post_flat_seconds if post_flat_seconds is not None else (28_800 if not is_canary else 0)
        ),
    )


def test_acceptance_requires_exact_canary_five_level_pilot_and_service(tmp_path: Path) -> None:
    acceptance = build_aggressive_laptop_acceptance(
        _binding(),
        _runtime(),
        _stage("canary"),
        _stage("pilot_a"),
        now=_NOW,
    )
    assert acceptance.accepted
    assert acceptance.stable_flat
    assert acceptance.post_flat_service_seconds == 28_800
    assert acceptance.execution_authorized is False
    path = tmp_path / "laptop-aggressive-acceptance.json"
    save_aggressive_laptop_acceptance(path, acceptance)
    loaded = load_aggressive_laptop_acceptance(path)
    assert loaded == acceptance
    verify_aggressive_laptop_handoff(loaded, _runtime())

    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["post_flat_service_seconds"] = 1
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        load_aggressive_laptop_acceptance(path)


@pytest.mark.parametrize(
    ("canary", "pilot", "message"),
    (
        (
            _stage("canary", maximum_loss=Decimal("1.01")),
            _stage("pilot_a"),
            "canary evidence",
        ),
        (
            _stage("canary"),
            _stage("pilot_a", levels=(1, 2, 3, 4)),
            "pilot evidence",
        ),
        (
            _stage("canary"),
            _stage("pilot_a", post_flat_seconds=28_799),
            "pilot evidence",
        ),
    ),
)
def test_acceptance_fails_closed_on_any_missing_live_evidence(
    canary: AggressiveLaptopStageEvidence,
    pilot: AggressiveLaptopStageEvidence,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        build_aggressive_laptop_acceptance(
            _binding(),
            _runtime(),
            canary,
            pilot,
            now=_NOW,
        )


@pytest.mark.asyncio
async def test_pilot_stage_report_reads_exact_five_aggressive_journal_actions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding = _binding()
    actions = tuple(
        SimpleNamespace(
            route=SimpleNamespace(value="BTC:bybit>okx"),
            risk_reservation={
                "strategy": "AGGRESSIVE_SYMBIOSIS_V1",
                "stage": "pilot_a",
                "aggressive_binding_sha256": binding.binding_sha256,
                "level_index": level,
                "projected_stress_usdt": Decimal(level),
            },
            legs=(object(), object(), object(), object()),
        )
        for level in range(1, 6)
    )

    class FakeJournal:
        def __init__(self, path: Path) -> None:
            assert path == tmp_path / "state.sqlite3"

        async def initialise(self) -> None:
            return None

        async def active_actions(self) -> tuple[object, ...]:
            return ()

        async def completed_actions_since(
            self,
            started: datetime,
            qualification: str,
        ) -> tuple[object, ...]:
            assert started == _NOW
            assert qualification == binding.qualification_hash
            return actions

    monkeypatch.setattr(acceptance_module, "LiveOrderJournal", FakeJournal)
    monkeypatch.setattr(acceptance_module, "is_completed_normal_paired_cycle", lambda _: True)
    monkeypatch.setattr(
        acceptance_module,
        "completed_normal_actions_sha256",
        lambda _: "4" * 64,
    )
    evidence = await build_aggressive_laptop_stage_evidence_from_journal(
        tmp_path / "state.sqlite3",
        binding,
        stage="pilot_a",
        started_at=_NOW,
        ended_at=_NOW + timedelta(hours=9),
        post_flat_service_seconds=28_800,
    )
    assert evidence.accepted
    assert evidence.completed_level_indices == (1, 2, 3, 4, 5)
    assert evidence.production_filled_order_count == 20
    assert evidence.stable_flat
