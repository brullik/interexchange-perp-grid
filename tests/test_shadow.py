from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.bbo_prefilter import BboPrefilterObservation
from interexchange_perp_grid.candidate_l2 import CandidateL2Result, CandidateL2Stats
from interexchange_perp_grid.config import load_settings
from interexchange_perp_grid.domain import InstrumentKey, Venue
from interexchange_perp_grid.execution import (
    Fill,
    OrderPurpose,
    PairActionState,
    Side,
    Tranche,
)
from interexchange_perp_grid.public_engine import PublicWorkload, ScanResult
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.routes import DirectedRouteQuote
from interexchange_perp_grid.shadow import (
    ContinuousShadowEvaluator,
    OverloadController,
    ShadowRuntime,
    ShadowTrader,
    WorkClass,
)
from interexchange_perp_grid.state import (
    initialise_state,
    read_shadow_snapshot,
    save_tranche,
)
from interexchange_perp_grid.strategy import DirectedRouteKey

CONFIG = Path("config/defaults.yaml")


def hedged_tranche() -> Tranche:
    return Tranche(
        "T1",
        DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX),
        Decimal("0.1"),
        Decimal("1"),
        Decimal("20"),
        Decimal("4"),
        state=PairActionState.HEDGED,
        reason=ReasonCode.ORDERS_HEDGED,
        entry_long_fills=[
            Fill(
                "long",
                Venue.BYBIT,
                Side.BUY,
                OrderPurpose.NORMAL_OPEN,
                Decimal("0.1"),
                Decimal("100"),
                Decimal("0.01"),
            )
        ],
        entry_short_fills=[
            Fill(
                "short",
                Venue.OKX,
                Side.SELL,
                OrderPurpose.NORMAL_OPEN,
                Decimal("0.1"),
                Decimal("110"),
                Decimal("0.01"),
            )
        ],
        processed_order_ids={"long", "short"},
    )


@pytest.mark.asyncio
async def test_restart_restores_activity_and_blocks_until_reconciled(tmp_path: Path) -> None:
    path = tmp_path / "shadow.sqlite3"
    await initialise_state(path)
    await save_tranche(path, hedged_tranche())
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(path)})

    restarted = ShadowRuntime(settings)
    await restarted.start()
    assert tuple(restarted.tranches) == ("T1",)
    assert (await restarted.entry_gate()).reason == ReasonCode.RECONCILIATION_REQUIRED
    assert await restarted.reconcile(set()) == ReasonCode.RECONCILIATION_FAILED
    assert (await restarted.entry_gate()).accepted is False
    assert await restarted.reconcile({"T1"}) == ReasonCode.RECONCILIATION_PASSED
    assert (await restarted.entry_gate()).accepted is True


def test_overload_preserves_risk_reduction_and_disables_new_entries_first() -> None:
    controller = OverloadController(10)
    controller.update_pending(11)
    assert controller.admit(WorkClass.NEW_ENTRY).reason == ReasonCode.OVERLOAD_ENTRY_DISABLED
    assert controller.admit(WorkClass.CANDIDATE_L2).accepted is False
    assert controller.admit(WorkClass.BROAD_BBO).accepted is False
    assert controller.admit(WorkClass.CLOSE).accepted is True
    assert controller.admit(WorkClass.HEDGE).accepted is True
    assert controller.admit(WorkClass.RECONCILE).accepted is True
    assert controller.admit(WorkClass.PRIVATE_STREAM).accepted is True


class OneScanEngine:
    def __init__(
        self,
        stop_event: asyncio.Event,
        workload: PublicWorkload | None = None,
    ) -> None:
        self.stop_event = stop_event
        self.workload = workload or PublicWorkload(0, 0, 0)
        self.closed = False
        self.broad_admissions: list[bool] = []
        self.candidate_admissions: list[bool] = []

    async def scan_once(
        self,
        base: str,
        quantity: Decimal,
        timeout: int,
        *,
        active_route_keys: frozenset[tuple[str, str, str]] = frozenset(),
        entry_work_admitted: bool = True,
    ) -> ScanResult:
        del quantity, timeout, active_route_keys, entry_work_admitted
        self.stop_event.set()
        return ScanResult(base, 1, (), (), (), (), (), ())

    async def scan_candidate_l2(
        self,
        timeout_seconds: int,
        *,
        active_route_keys: frozenset[tuple[str, str, str]] = frozenset(),
        candidates_admitted: bool = True,
        prefilter: tuple[BboPrefilterObservation, ...] | None = None,
        preserve_existing_candidates: bool = False,
    ) -> CandidateL2Result:
        del timeout_seconds, active_route_keys, prefilter, preserve_existing_candidates
        self.candidate_admissions.append(candidates_admitted)
        return CandidateL2Result(
            (),
            CandidateL2Stats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            Decimal(0),
        )

    def public_workload(self) -> PublicWorkload:
        return self.workload

    async def set_broad_bbo_admitted(self, admitted: bool) -> None:
        self.broad_admissions.append(admitted)

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_continuous_evaluator_persists_non_stub_market_snapshot(tmp_path: Path) -> None:
    stop = asyncio.Event()
    state_path = tmp_path / "continuous.sqlite3"
    settings = load_settings(
        CONFIG,
        {
            "IPEG_STATE_PATH": str(state_path),
            "IPEG_PARQUET_DIR": str(tmp_path / "market"),
        },
    )
    engine = OneScanEngine(stop)
    evaluator = ContinuousShadowEvaluator(settings, engine=engine)
    await evaluator.run(stop)
    snapshot = await read_shadow_snapshot(state_path)
    assert snapshot is not None
    assert snapshot["base"] == "BTC"
    assert snapshot["evaluated_at"] <= datetime.now(UTC).isoformat()
    assert snapshot["candidate_l2"]["execution_authorized"] is False
    assert engine.closed is True


@pytest.mark.asyncio
async def test_continuous_evaluator_applies_runtime_overload_before_public_work(
    tmp_path: Path,
) -> None:
    stop = asyncio.Event()
    loaded = load_settings(
        CONFIG,
        {
            "IPEG_STATE_PATH": str(tmp_path / "overload.sqlite3"),
            "IPEG_PARQUET_DIR": str(tmp_path / "market"),
        },
    )
    settings = loaded.model_copy(
        update={
            "shadow": loaded.shadow.model_copy(update={"overload_pending_limit": 2}),
        }
    )
    engine = OneScanEngine(stop, PublicWorkload(2, 2, 2))
    evaluator = ContinuousShadowEvaluator(settings, engine=engine)

    await evaluator.run(stop)

    assert engine.broad_admissions == [False]
    assert engine.candidate_admissions == [False]
    assert evaluator.runtime.overload.admit(WorkClass.NEW_ENTRY).reason == (
        ReasonCode.OVERLOAD_ENTRY_DISABLED
    )


def test_public_workload_does_not_double_count_incoming_entry(tmp_path: Path) -> None:
    loaded = load_settings(
        CONFIG,
        {
            "IPEG_STATE_PATH": str(tmp_path / "entry-capacity.sqlite3"),
            "IPEG_PARQUET_DIR": str(tmp_path / "market"),
        },
    )
    settings = loaded.model_copy(
        update={
            "shadow": loaded.shadow.model_copy(update={"overload_pending_limit": 3}),
        }
    )
    engine = OneScanEngine(asyncio.Event(), PublicWorkload(2, 2, 2))
    evaluator = ContinuousShadowEvaluator(settings, engine=engine)
    active = frozenset({("BTC", Venue.BYBIT.value, Venue.OKX.value)})

    broad_admitted, candidates_admitted = evaluator._apply_public_workload(active)

    assert broad_admitted is False
    assert candidates_admitted is False
    assert evaluator.runtime.overload.admit(WorkClass.NEW_ENTRY).accepted is True


@pytest.mark.asyncio
async def test_shadow_trader_runs_calibration_risk_and_paired_fill_pipeline(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "trader.sqlite3"
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(state_path)})
    runtime = ShadowRuntime(settings)
    await runtime.start()
    await runtime.reconcile(set())
    trader = ShadowTrader(settings, runtime)
    quote = DirectedRouteQuote(
        key=InstrumentKey("BTC", "USDT"),
        long_venue=Venue.BYBIT,
        short_venue=Venue.OKX,
        base_quantity=Decimal("1"),
        eligible=True,
        reason=ReasonCode.QUOTE_READY,
        entry_long_vwap=Decimal("100"),
        entry_short_vwap=Decimal("110"),
        exit_long_vwap=Decimal("104"),
        exit_short_vwap=Decimal("105"),
        entry_spread=Decimal("10"),
        exit_spread=Decimal("1"),
        entry_spread_bps=Decimal("1000"),
        four_leg_fee_estimate=Decimal("0.4"),
        funding_rate_delta=Decimal("0"),
    )
    scan = ScanResult("BTC", 1, (), (), (), (quote,), (), ())
    for _ in range(4):
        assert await trader.process(scan) == ()
    decisions = await trader.process(scan)
    assert len(decisions) == 1
    assert decisions[0].reason == ReasonCode.ENTRY_ACCEPTED
    assert len(runtime.tranches) == 1
    opened = next(iter(runtime.tranches.values()))
    assert opened.state == PairActionState.HEDGED
    assert opened.actual_long_entry_quantity == Decimal("1")
    assert opened.actual_short_entry_quantity == Decimal("1")
    assert opened.projected_stress_usdt <= Decimal("5")
