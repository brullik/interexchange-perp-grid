from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

import interexchange_perp_grid.laptop_workflow as workflow_module
from interexchange_perp_grid.config import load_settings
from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.laptop_workflow import (
    LaptopQualificationIdentity,
    build_laptop_pilot_report,
    run_until_qualification_finalized,
)
from interexchange_perp_grid.qualification import QualificationEvidence
from interexchange_perp_grid.service import BoundedServiceReceipt
from interexchange_perp_grid.state import QualificationEpoch, QualificationEpochStatus
from interexchange_perp_grid.strategy import DirectedRouteKey

CONFIG = Path("config/defaults.yaml")
ROUTE = DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX)
RELEASE = "a" * 40
DIGEST = "sha256:" + "b" * 64


@pytest.mark.asyncio
async def test_native_qualification_stops_only_after_exact_epoch_finalizes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(tmp_path / "state.sqlite3")})
    stopped = asyncio.Event()
    observed_at = datetime(2026, 8, 21, tzinfo=UTC)
    epoch = QualificationEpoch(
        "epoch-1",
        ROUTE,
        RELEASE,
        "c" * 64,
        "d" * 64,
        DIGEST,
        observed_at - timedelta(hours=24),
        observed_at,
        QualificationEpochStatus.FINALIZED,
    )

    async def fake_service(self: object, stop_event: asyncio.Event) -> None:
        del self
        await stop_event.wait()
        stopped.set()

    async def finalized(path: object) -> QualificationEpoch:
        del path
        return epoch

    monkeypatch.setattr(
        "interexchange_perp_grid.laptop_workflow.BootstrapService.run",
        fake_service,
    )
    monkeypatch.setattr(workflow_module, "read_qualification_epoch", finalized)

    result = await run_until_qualification_finalized(
        settings,
        LaptopQualificationIdentity(ROUTE, RELEASE, DIGEST),
        maximum_seconds=86_400,
        poll_interval_seconds=0.01,
    )

    assert result == epoch
    assert stopped.is_set()


@pytest.mark.asyncio
async def test_pilot_report_requires_real_paired_cycle_and_eight_hours(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(tmp_path / "state.sqlite3")})
    started_at = datetime(2026, 8, 21, tzinfo=UTC)
    completed_at = started_at + timedelta(minutes=5)
    ended_at = completed_at + timedelta(hours=8)
    action = SimpleNamespace(
        pair_action_id="ipeg-canary-real",
        updated_at=completed_at,
        legs=(object(), object(), object(), object()),
    )

    class FakeJournal:
        def __init__(self, path: Path) -> None:
            del path

        async def initialise(self) -> None:
            return None

        async def active_actions(self) -> tuple[object, ...]:
            return ()

        async def completed_actions_since(
            self, boundary: datetime, qualification_hash: str
        ) -> tuple[object, ...]:
            assert boundary == started_at
            assert qualification_hash == "q" * 64
            return (action,)

    async def stopped_health(*args: object, **kwargs: object) -> SimpleNamespace:
        del args, kwargs
        return SimpleNamespace(status="stopped", starts=2)

    monkeypatch.setattr(workflow_module, "LiveOrderJournal", FakeJournal)
    monkeypatch.setattr(workflow_module, "is_completed_normal_paired_cycle", lambda item: True)
    monkeypatch.setattr(
        workflow_module,
        "completed_normal_actions_sha256",
        lambda actions: "e" * 64,
    )
    monkeypatch.setattr(workflow_module, "read_service_health", stopped_health)
    qualification = cast(
        QualificationEvidence,
        SimpleNamespace(qualification_hash="q" * 64, container_image_digest=DIGEST),
    )
    receipt = BoundedServiceReceipt(
        1,
        "PASS",
        started_at - timedelta(seconds=1),
        ended_at,
        28_800,
        28_800,
        str(Path(settings.storage.sqlite_path).resolve()),
        2,
    )

    report = await build_laptop_pilot_report(
        settings,
        qualification,
        started_at,
        ended_at,
        DIGEST,
        receipt,
    )

    assert report.status == "PASS"
    assert report.post_trade_observation_seconds == 28_800
    assert report.completed_pair_action_ids == ("ipeg-canary-real",)
    assert report.production_filled_order_count == 4
    assert report.live_authorized is False


@pytest.mark.asyncio
async def test_pilot_report_fails_when_post_trade_interval_is_short(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(tmp_path / "state.sqlite3")})
    started_at = datetime(2026, 8, 21, tzinfo=UTC)

    class EmptyJournal:
        def __init__(self, path: Path) -> None:
            del path

        async def initialise(self) -> None:
            return None

        async def active_actions(self) -> tuple[object, ...]:
            return ()

        async def completed_actions_since(
            self, boundary: datetime, qualification_hash: str
        ) -> tuple[object, ...]:
            del boundary, qualification_hash
            return ()

    async def stopped_health(*args: object, **kwargs: object) -> SimpleNamespace:
        del args, kwargs
        return SimpleNamespace(status="stopped", starts=1)

    monkeypatch.setattr(workflow_module, "LiveOrderJournal", EmptyJournal)
    monkeypatch.setattr(workflow_module, "read_service_health", stopped_health)
    qualification = cast(
        QualificationEvidence,
        SimpleNamespace(qualification_hash="q" * 64, container_image_digest=DIGEST),
    )
    ended_at = started_at + timedelta(hours=8)
    receipt = BoundedServiceReceipt(
        1,
        "PASS",
        started_at - timedelta(seconds=1),
        ended_at,
        28_800,
        28_800,
        str(Path(settings.storage.sqlite_path).resolve()),
        1,
    )

    report = await build_laptop_pilot_report(
        settings,
        qualification,
        started_at,
        ended_at,
        DIGEST,
        receipt,
    )

    assert report.status == "FAIL"
    assert "EXACTLY_ONE_COMPLETED_PAIRED_CANARY_REQUIRED" in report.blockers
    assert "EIGHT_HOUR_POST_TRADE_OBSERVATION_REQUIRED" in report.blockers
