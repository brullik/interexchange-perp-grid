from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.domain import FundingSnapshot, Venue
from interexchange_perp_grid.execution import PairActionState, Tranche
from interexchange_perp_grid.qualification import (
    QualificationPolicy,
    build_qualification_progress,
    build_runtime_evidence_from_state,
)
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.state import (
    finalize_qualification_epoch,
    initialise_state,
    record_qualification_exception,
    record_qualification_scan,
    save_tranche,
    start_qualification_epoch,
)
from interexchange_perp_grid.strategy import (
    CostBreakdown,
    DirectedRouteKey,
    SignalDecision,
)


def _decision(route: DirectedRouteKey) -> SignalDecision:
    zero = Decimal(0)
    return SignalDecision(
        accepted=True,
        reason=ReasonCode.ENTRY_ACCEPTED,
        route=route,
        calibration_version=4,
        inputs={
            "size_bucket_base_quantity": Decimal("0.001"),
            "adaptive_entry_threshold_bps": Decimal("12"),
            "target_exit_spread_bps": Decimal("2"),
            "minimum_profit_usdt": Decimal("0.01"),
        },
        cost=CostBreakdown(
            zero,
            zero,
            zero,
            zero,
            zero,
            zero,
            zero,
            zero,
            zero,
            zero,
            zero,
            Decimal("0.10"),
            Decimal("0.10"),
        ),
        risk_breakdown={},
    )


@pytest.mark.asyncio
async def test_runtime_qualification_evidence_comes_from_persisted_route_state(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    await initialise_state(path)
    route = DirectedRouteKey("BTC", Venue.BINANCE_USDM, Venue.OKX)
    start = datetime(2026, 8, 14, 12, tzinfo=UTC)
    epoch = await start_qualification_epoch(
        path,
        route,
        "a" * 40,
        "c" * 64,
        "d" * 64,
        "sha256:" + "b" * 64,
        start,
    )
    for index in range(3):
        observed = start + timedelta(hours=index * 8)
        funding = tuple(
            FundingSnapshot(
                venue,
                "BTC/USDT:USDT",
                Decimal("0.0001"),
                int((observed + timedelta(hours=8)).timestamp() * 1000),
                "8h",
                Decimal("100000"),
                Decimal("100000"),
                int(observed.timestamp() * 1000),
            )
            for venue in (route.long_venue, route.short_venue)
        )
        await record_qualification_scan(
            path,
            epoch.epoch_id,
            "BTC",
            funding,
            (_decision(route),),
            (),
            Decimal(2),
            300,
            3600,
            observed,
        )
    await record_qualification_exception(
        path,
        epoch.epoch_id,
        "InjectedReplayFailure",
        start + timedelta(hours=20),
    )
    await save_tranche(
        path,
        Tranche(
            "unknown-order",
            route,
            Decimal("0.001"),
            Decimal(2),
            Decimal(20),
            Decimal("0.5"),
            state=PairActionState.UNKNOWN_ORDER,
        ),
    )

    await finalize_qualification_epoch(path, epoch.epoch_id, start + timedelta(hours=24))
    evidence = await build_runtime_evidence_from_state(
        path,
        epoch.epoch_id,
        {route.long_venue: Decimal("0.0004"), route.short_venue: Decimal("0.0005")},
        replay_completed=True,
    )

    assert len(evidence.funding_checkpoints) == 6
    assert evidence.replay_shadow.accepted_signals == 3
    assert evidence.replay_shadow.unresolved_order_count == 1
    assert evidence.replay_shadow.unresolved_exposure_count == 1
    assert evidence.replay_shadow.unhandled_exception_count == 1
    assert evidence.strategy.calibration_version == 4
    assert evidence.strategy.size_bucket_base_quantity == Decimal("0.001")


@pytest.mark.asyncio
async def test_running_epoch_progress_reports_exact_remaining_counts_and_blockers(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    data = tmp_path / "market"
    await initialise_state(path)
    route = DirectedRouteKey("BTC", Venue.BINANCE_USDM, Venue.OKX)
    start = datetime(2026, 8, 14, 12, tzinfo=UTC)
    epoch = await start_qualification_epoch(
        path,
        route,
        "a" * 40,
        "c" * 64,
        "d" * 64,
        "sha256:" + "b" * 64,
        start,
    )
    funding = tuple(
        FundingSnapshot(
            venue,
            "BTC/USDT:USDT",
            Decimal("0.0001"),
            int((start + timedelta(hours=8)).timestamp() * 1000),
            "8h",
            Decimal("100000"),
            Decimal("100000"),
            int(start.timestamp() * 1000),
        )
        for venue in (route.long_venue, route.short_venue)
    )
    await record_qualification_scan(
        path,
        epoch.epoch_id,
        "BTC",
        funding,
        (_decision(route),),
        (),
        Decimal(2),
        300,
        3600,
        start,
    )
    policy = QualificationPolicy(
        minimum_duration_seconds=86_400,
        minimum_synchronised_snapshots_per_venue=10_000,
        minimum_funding_checkpoints_per_venue=3,
    )

    progress = await build_qualification_progress(
        path,
        data,
        epoch.epoch_id,
        policy,
        start + timedelta(hours=6),
    )

    assert progress.elapsed_seconds == Decimal(21_600)
    assert progress.remaining_duration_seconds == Decimal(64_800)
    assert progress.completion_ratio == Decimal(0)
    assert progress.accepted_signals == 1
    assert progress.rejected_signals == 0
    assert progress.replay_completed is False
    assert progress.unresolved_order_count == 0
    assert progress.unresolved_exposure_count == 0
    assert progress.ready_to_finalize is False
    assert progress.qualification_ready is False
    assert set(progress.blockers) >= {
        "OBSERVATION_PERIOD_INSUFFICIENT",
        "BINANCEUSDM_SYNCHRONISED_SNAPSHOTS_INSUFFICIENT",
        "OKX_SYNCHRONISED_SNAPSHOTS_INSUFFICIENT",
        "BINANCEUSDM_FUNDING_CHECKPOINTS_INSUFFICIENT",
        "OKX_FUNDING_CHECKPOINTS_INSUFFICIENT",
        "SIMULATED_NET_PNL_NOT_POSITIVE",
        "REPLAY_NOT_COMPLETED",
    }
    assert tuple(item.remaining_synchronised_snapshots for item in progress.venues) == (
        10_000,
        10_000,
    )
    assert tuple(item.remaining_funding_checkpoints for item in progress.venues) == (2, 2)
