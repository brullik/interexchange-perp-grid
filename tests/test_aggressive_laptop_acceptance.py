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
from interexchange_perp_grid.execution import Side
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
        tuple(Decimal(index) for index in range(5)),
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
        actual_fees_usdt=Decimal("0.04" if is_canary else "0.20"),
        realized_funding_usdt=Decimal(0),
        realized_pnl_usdt=Decimal("0.10" if is_canary else "0.50"),
        active_action_count=0,
        maximum_projected_route_loss_usdt=(
            maximum_loss if maximum_loss is not None else Decimal(1 if is_canary else 5)
        ),
        stable_flat=True,
        final_private_event_watermark=4 if is_canary else 20,
        reconciliation_evidence_sha256=("3" if is_canary else "4") * 64,
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
            pair_action_id=f"action-{level}",
            route=SimpleNamespace(value="BTC:bybit>okx"),
            created_at=_NOW + timedelta(minutes=level),
            updated_at=_NOW + timedelta(minutes=level + 1),
            risk_reservation={
                "strategy": "AGGRESSIVE_SYMBIOSIS_V1",
                "stage": "pilot_a",
                "aggressive_binding_sha256": binding.binding_sha256,
                "level_index": level,
                "projected_stress_usdt": Decimal("0.8"),
                "initial_funding_next_timestamp_ms": {
                    "bybit": int((_NOW + timedelta(hours=8)).timestamp() * 1000),
                    "okx": int((_NOW + timedelta(hours=8)).timestamp() * 1000),
                },
            },
            legs=tuple(
                SimpleNamespace(client_order_id=f"action-{level}-leg-{index}") for index in range(4)
            ),
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

        async def latest_order_events(self, pair_action_id: str) -> tuple[object, ...]:
            return tuple(
                SimpleNamespace(
                    client_order_id=f"{pair_action_id}-leg-{index}",
                    status=SimpleNamespace(value="FILLED"),
                    filled_base_quantity=Decimal("0.01"),
                    average_price=Decimal("100"),
                    fee_usdt=Decimal("0.001"),
                    side=Side.BUY if index % 2 == 0 else Side.SELL,
                )
                for index in range(4)
            )

        async def event_watermark(self) -> int:
            return 20

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
        authoritative_stable_flat=True,
        authoritative_private_event_watermark=20,
        authoritative_reconciliation_sha256="5" * 64,
    )
    assert evidence.accepted
    assert evidence.completed_level_indices == (1, 2, 3, 4, 5)
    assert evidence.production_filled_order_count == 20
    assert evidence.stable_flat


@pytest.mark.asyncio
async def test_stage_report_never_inferrs_flat_from_an_empty_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class EmptyJournal:
        def __init__(self, path: Path) -> None:
            del path

        async def initialise(self) -> None:
            return None

        async def active_actions(self) -> tuple[object, ...]:
            return ()

        async def completed_actions_since(
            self, started: datetime, qualification: str
        ) -> tuple[object, ...]:
            del started, qualification
            return ()

        async def event_watermark(self) -> int:
            return 0

    monkeypatch.setattr(acceptance_module, "LiveOrderJournal", EmptyJournal)
    evidence = await build_aggressive_laptop_stage_evidence_from_journal(
        tmp_path / "state.sqlite3",
        _binding(),
        stage="canary",
        started_at=_NOW,
        ended_at=_NOW + timedelta(minutes=1),
        post_flat_service_seconds=0,
    )

    assert not evidence.accepted
    assert not evidence.stable_flat
    assert "AUTHORITATIVE_PRIVATE_STABLE_FLAT_REQUIRED" in evidence.blockers


@pytest.mark.asyncio
async def test_canary_rebuild_excludes_later_pilot_actions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding = _binding()

    def action(stage: str, level: int, minute: int) -> SimpleNamespace:
        return SimpleNamespace(
            pair_action_id=f"{stage}-{level}",
            route=SimpleNamespace(value="BTC:bybit>okx"),
            created_at=_NOW + timedelta(minutes=minute),
            updated_at=_NOW + timedelta(minutes=minute + 1),
            risk_reservation={
                "strategy": "AGGRESSIVE_SYMBIOSIS_V1",
                "stage": stage,
                "aggressive_binding_sha256": binding.binding_sha256,
                "level_index": level,
                "projected_stress_usdt": Decimal("0.8"),
                "initial_funding_next_timestamp_ms": {
                    "bybit": int((_NOW + timedelta(hours=8)).timestamp() * 1000),
                    "okx": int((_NOW + timedelta(hours=8)).timestamp() * 1000),
                },
            },
            legs=tuple(
                SimpleNamespace(client_order_id=f"{stage}-{level}-leg-{index}")
                for index in range(4)
            ),
        )

    canary_action = action("canary", 1, 1)
    later_pilot = action("pilot_a", 1, 20)

    class FakeJournal:
        def __init__(self, path: Path) -> None:
            del path

        async def initialise(self) -> None:
            return None

        async def active_actions(self) -> tuple[object, ...]:
            return ()

        async def completed_actions_since(
            self, started: datetime, qualification: str
        ) -> tuple[object, ...]:
            del started, qualification
            return (canary_action, later_pilot)

        async def latest_order_events(self, pair_action_id: str) -> tuple[object, ...]:
            return tuple(
                SimpleNamespace(
                    client_order_id=f"{pair_action_id}-leg-{index}",
                    status=SimpleNamespace(value="FILLED"),
                    filled_base_quantity=Decimal("0.01"),
                    average_price=Decimal("100"),
                    fee_usdt=Decimal("0.001"),
                    side=Side.BUY if index % 2 == 0 else Side.SELL,
                )
                for index in range(4)
            )

        async def event_watermark(self) -> int:
            return 8

    monkeypatch.setattr(acceptance_module, "LiveOrderJournal", FakeJournal)
    monkeypatch.setattr(acceptance_module, "is_completed_normal_paired_cycle", lambda _: True)
    monkeypatch.setattr(acceptance_module, "completed_normal_actions_sha256", lambda _: "6" * 64)
    evidence = await build_aggressive_laptop_stage_evidence_from_journal(
        tmp_path / "state.sqlite3",
        binding,
        stage="canary",
        started_at=_NOW,
        ended_at=_NOW + timedelta(minutes=10),
        post_flat_service_seconds=0,
        authoritative_stable_flat=True,
        authoritative_private_event_watermark=8,
        authoritative_reconciliation_sha256="7" * 64,
    )

    assert evidence.accepted
    assert evidence.completed_level_indices == (1,)
