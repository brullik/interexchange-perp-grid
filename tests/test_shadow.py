from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

import interexchange_perp_grid.shadow as shadow_module
import interexchange_perp_grid.state as state_module
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
from interexchange_perp_grid.route_calibration import (
    RouteCalibrationAssessment,
    RouteCalibrationObservation,
    calibrate_route_size,
)
from interexchange_perp_grid.routes import DirectedRouteQuote
from interexchange_perp_grid.shadow import (
    ContinuousShadowEvaluator,
    OverloadController,
    ShadowRuntime,
    ShadowTrader,
    WorkClass,
)
from interexchange_perp_grid.state import (
    delete_tranche,
    initialise_state,
    load_tranches,
    read_shadow_snapshot,
    save_shadow_snapshot,
    save_tranche,
)
from interexchange_perp_grid.strategy import DirectedRouteKey, SignalDecision

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
        events: list[str] | None = None,
        workload_after_scan: PublicWorkload | None = None,
    ) -> None:
        self.stop_event = stop_event
        self.workload = workload or PublicWorkload(0, 0, 0)
        self.closed = False
        self.broad_admissions: list[bool] = []
        self.candidate_admissions: list[bool] = []
        self.calibration_calls = 0
        self.events = events
        self.workload_after_scan = workload_after_scan

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
        if self.workload_after_scan is not None:
            self.workload = self.workload_after_scan
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

    async def scan_route_calibration_observations(
        self,
        timeout_seconds: int,
        *,
        epoch_id: str | None = None,
    ) -> tuple[RouteCalibrationObservation, ...]:
        del timeout_seconds, epoch_id
        self.calibration_calls += 1
        if self.events is not None:
            self.events.append("calibration")
        return ()

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
    assert engine.calibration_calls == 0
    assert evaluator.runtime.overload.admit(WorkClass.NEW_ENTRY).reason == (
        ReasonCode.OVERLOAD_ENTRY_DISABLED
    )


@pytest.mark.asyncio
async def test_active_close_is_evaluated_before_calibration_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()
    state_path = tmp_path / "active-priority.sqlite3"
    await initialise_state(state_path)
    await save_tranche(state_path, hedged_tranche())
    settings = load_settings(
        CONFIG,
        {
            "IPEG_STATE_PATH": str(state_path),
            "IPEG_PARQUET_DIR": str(tmp_path / "market"),
        },
    )
    events: list[str] = []
    engine = OneScanEngine(stop, events=events)
    evaluator = ContinuousShadowEvaluator(settings, engine=engine)

    async def observe_close(_quotes: tuple[DirectedRouteQuote, ...]) -> None:
        events.append("close")

    monkeypatch.setattr(evaluator._trader, "close_active", observe_close)
    await evaluator.run(stop)

    assert events[:2] == ["close", "calibration"]


@pytest.mark.asyncio
async def test_indeterminate_close_persistence_latches_entry_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "indeterminate-close.sqlite3"
    await initialise_state(state_path)
    await save_tranche(state_path, hedged_tranche())
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(state_path)})
    runtime = ShadowRuntime(settings)
    await runtime.start()
    await runtime.reconcile({"T1"})
    trader = ShadowTrader(settings, runtime)
    quote = DirectedRouteQuote(
        key=InstrumentKey("BTC", "USDT"),
        long_venue=Venue.BYBIT,
        short_venue=Venue.OKX,
        base_quantity=Decimal("0.1"),
        eligible=True,
        reason=ReasonCode.QUOTE_READY,
        entry_long_vwap=Decimal(100),
        entry_short_vwap=Decimal(110),
        exit_long_vwap=Decimal(100),
        exit_short_vwap=Decimal(100),
        entry_spread=Decimal(10),
        exit_spread=Decimal(0),
        entry_spread_bps=Decimal(1000),
        four_leg_fee_estimate=Decimal("0.4"),
        funding_rate_delta=Decimal(0),
    )
    state_any = cast(Any, state_module)
    original_sync_save = state_any._save_tranche_sync
    writer_started = threading.Event()
    writer_release = threading.Event()

    def held_sync_save(*args: Any, **kwargs: Any) -> None:
        writer_started.set()
        writer_release.wait()
        original_sync_save(*args, **kwargs)

    monkeypatch.setattr(state_any, "_save_tranche_sync", held_sync_save)
    closing = asyncio.create_task(trader.close_active((quote,)))
    assert await asyncio.to_thread(writer_started.wait, 1)

    with pytest.raises(RuntimeError, match="close persistence outcome is indeterminate"):
        await asyncio.wait_for(closing, timeout=1.3)

    assert runtime.tranches["T1"].state == PairActionState.CLOSED
    assert (await load_tranches(state_path))[0].state == PairActionState.HEDGED
    gate = await runtime.entry_gate()
    assert gate.accepted is False
    assert gate.reason == ReasonCode.RECONCILIATION_REQUIRED

    writer_release.set()
    for _ in range(100):
        persisted = await load_tranches(state_path)
        if persisted[0].state == PairActionState.CLOSED:
            break
        await asyncio.sleep(0.01)
    assert persisted[0].state == PairActionState.CLOSED


@pytest.mark.asyncio
async def test_midcycle_overload_sheds_candidate_calibration(tmp_path: Path) -> None:
    stop = asyncio.Event()
    loaded = load_settings(
        CONFIG,
        {
            "IPEG_STATE_PATH": str(tmp_path / "midcycle-overload.sqlite3"),
            "IPEG_PARQUET_DIR": str(tmp_path / "market"),
        },
    )
    settings = loaded.model_copy(
        update={"shadow": loaded.shadow.model_copy(update={"overload_pending_limit": 2})}
    )
    engine = OneScanEngine(
        stop,
        PublicWorkload(0, 0, 0),
        workload_after_scan=PublicWorkload(2, 2, 2),
    )
    evaluator = ContinuousShadowEvaluator(settings, engine=engine)

    await evaluator.run(stop)

    assert engine.candidate_admissions == [True, False]
    assert engine.calibration_calls == 0


@pytest.mark.asyncio
async def test_slow_persistent_calibration_cannot_delay_or_false_pass_entry_decision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()
    loaded = load_settings(
        CONFIG,
        {
            "IPEG_STATE_PATH": str(tmp_path / "decision-budget.sqlite3"),
            "IPEG_PARQUET_DIR": str(tmp_path / "market"),
        },
    )
    engine = OneScanEngine(stop)
    observed_at = datetime.now(UTC)
    route = DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX)

    async def observations(
        _timeout_seconds: int,
        *,
        epoch_id: str | None = None,
    ) -> tuple[RouteCalibrationObservation, ...]:
        return (
            RouteCalibrationObservation(
                route,
                Decimal(1),
                Decimal("0.001"),
                epoch_id or "epoch",
                observed_at,
                Decimal(10),
                Decimal(1),
                Decimal(5),
                Decimal(2),
                Decimal("0.5"),
                Decimal(1000),
                Decimal("0.0001"),
                Decimal(4),
                ReasonCode.QUOTE_READY,
            ),
        )

    monkeypatch.setattr(engine, "scan_route_calibration_observations", observations)

    class HeldCalibrator:
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def initialise(self) -> None:
            return None

        async def current_epoch_id(self, _observed_at: datetime) -> str:
            return "epoch"

        async def record_many(
            self,
            _observations: tuple[RouteCalibrationObservation, ...],
            *,
            now: datetime | None = None,
        ) -> tuple[RouteCalibrationAssessment, ...]:
            del now
            self.started.set()
            await self.release.wait()
            return ()

        async def assess_current(
            self,
            _observations: tuple[RouteCalibrationObservation, ...],
        ) -> tuple[RouteCalibrationAssessment, ...]:
            return ()

        async def close(self) -> None:
            self.release.set()

    calibrator = HeldCalibrator()
    evaluator = ContinuousShadowEvaluator(
        loaded,
        engine=engine,
        route_calibrator=cast(Any, calibrator),
    )
    processed = asyncio.Event()
    allow_process_return = asyncio.Event()
    captured: list[ScanResult] = []

    async def observe_process(
        scan: ScanResult,
        *,
        decision_deadline: float | None = None,
    ) -> tuple[SignalDecision, ...]:
        assert decision_deadline is not None
        captured.append(scan)
        processed.set()
        await allow_process_return.wait()
        return ()

    monkeypatch.setattr(evaluator._trader, "process", observe_process)
    running = asyncio.create_task(evaluator.run(stop))
    await asyncio.wait_for(calibrator.started.wait(), timeout=1)
    await asyncio.wait_for(processed.wait(), timeout=0.30)

    assert captured[0].route_calibration == ()
    assert captured[0].candidate_l2 is not None
    assert captured[0].candidate_l2.decision_latency_ms <= Decimal(250)
    assert calibrator.release.is_set() is False

    allow_process_return.set()
    await running


def test_end_to_end_decision_latency_p95_retains_slow_samples(tmp_path: Path) -> None:
    loaded = load_settings(
        CONFIG,
        {
            "IPEG_STATE_PATH": str(tmp_path / "decision-p95.sqlite3"),
            "IPEG_PARQUET_DIR": str(tmp_path / "market"),
        },
    )
    evaluator = ContinuousShadowEvaluator(loaded, engine=OneScanEngine(asyncio.Event()))
    evaluator._decision_latency_samples.extend(Decimal(10) for _ in range(18))
    evaluator._decision_latency_samples.extend((Decimal(240), Decimal(240)))

    first_p95 = evaluator._decision_latency_p95()
    evaluator._decision_latency_samples.append(Decimal(10))

    assert first_p95 is not None
    assert first_p95 > Decimal(200)
    assert evaluator._decision_latency_p95() == Decimal(240)


@pytest.mark.asyncio
async def test_background_calibration_failure_latches_entry_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()
    loaded = load_settings(
        CONFIG,
        {
            "IPEG_STATE_PATH": str(tmp_path / "persistence-failure.sqlite3"),
            "IPEG_PARQUET_DIR": str(tmp_path / "market"),
        },
    )
    settings = loaded.model_copy(
        update={"shadow": loaded.shadow.model_copy(update={"scan_interval_seconds": 0.01})}
    )

    class TwoScanEngine(OneScanEngine):
        def __init__(self) -> None:
            super().__init__(stop)
            self.scans = 0

        async def scan_once(self, *args: Any, **kwargs: Any) -> ScanResult:
            del args, kwargs
            self.scans += 1
            if self.scans == 2:
                stop.set()
            return ScanResult("BTC", 1, (), (), (), (), (), ())

    route = DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX)

    class FailingCalibrator:
        def __init__(self) -> None:
            self.records = 0

        async def initialise(self) -> None:
            return None

        async def current_epoch_id(self, _observed_at: datetime) -> str:
            return "epoch"

        async def record_many(
            self,
            _observations: tuple[RouteCalibrationObservation, ...],
            *,
            now: datetime | None = None,
        ) -> tuple[RouteCalibrationAssessment, ...]:
            del now
            self.records += 1
            if self.records == 2:
                raise OSError("disk full")
            return ()

        async def assess_current(
            self,
            _observations: tuple[RouteCalibrationObservation, ...],
        ) -> tuple[RouteCalibrationAssessment, ...]:
            return (
                RouteCalibrationAssessment(
                    route,
                    Decimal(1),
                    Decimal(1),
                    ReasonCode.QUOTE_READY,
                    5,
                    None,
                ),
            )

        async def close(self) -> None:
            return None

    engine = TwoScanEngine()
    observed = 0

    async def observations(
        _timeout_seconds: int,
        *,
        epoch_id: str | None = None,
    ) -> tuple[RouteCalibrationObservation, ...]:
        nonlocal observed
        observed += 1
        return (
            RouteCalibrationObservation(
                route,
                Decimal(1),
                Decimal(1),
                epoch_id or "epoch",
                datetime.now(UTC) + timedelta(seconds=observed),
                Decimal(10),
                Decimal(1),
                Decimal(5),
                Decimal(2),
                Decimal("0.5"),
                Decimal(1000),
                Decimal("0.0001"),
                Decimal(4),
                ReasonCode.QUOTE_READY,
            ),
        )

    monkeypatch.setattr(engine, "scan_route_calibration_observations", observations)
    calibrator = FailingCalibrator()
    evaluator = ContinuousShadowEvaluator(
        settings,
        engine=engine,
        route_calibrator=cast(Any, calibrator),
    )
    observed_lengths: list[int] = []

    async def observe_process(
        scan: ScanResult,
        *,
        decision_deadline: float | None = None,
    ) -> tuple[SignalDecision, ...]:
        assert decision_deadline is not None
        observed_lengths.append(len(scan.route_calibration))
        return ()

    monkeypatch.setattr(evaluator._trader, "process", observe_process)

    with pytest.raises(RuntimeError, match="route calibration persistence"):
        await evaluator.run(stop)

    assert observed_lengths == [1, 0]
    assert evaluator._route_calibration_persistence_healthy is False


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
    monkeypatch: pytest.MonkeyPatch,
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
    route = DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX)
    now = datetime(2026, 8, 16, tzinfo=UTC)
    observations = tuple(
        RouteCalibrationObservation(
            route=route,
            size_bucket_multiplier=Decimal(1),
            base_quantity=Decimal(1),
            epoch_id="shadow-test",
            observed_at=now - timedelta(hours=20 - index * 5),
            spread_bps=Decimal(100 + index * 10),
            adverse_excursion_after_entry_bps=Decimal(2),
            convergence_seconds=Decimal(30),
            stressed_cost_floor_bps=Decimal(2),
            normalized_tick_bps=Decimal("0.5"),
            notional_usdt=Decimal(105),
            funding_rate_delta=Decimal(0),
            exit_depth_multiple=Decimal(4),
            reason=ReasonCode.QUOTE_READY,
            episode_entry_spread_bps=Decimal(1000),
        )
        for index in range(5)
    )
    calibration = calibrate_route_size(
        observations,
        now=now,
        minimum_samples=5,
        minimum_observation_period=timedelta(hours=20),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        minimum_convergence_samples_per_spread_bucket=3,
    )
    unqualified = ScanResult("BTC", 1, (), (), (), (quote,), (), ())
    assert await trader.process(unqualified) == ()
    bucketless = calibrate_route_size(
        tuple(replace(item, episode_entry_spread_bps=None) for item in observations),
        now=now,
        minimum_samples=5,
        minimum_observation_period=timedelta(hours=20),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        minimum_convergence_samples_per_spread_bucket=3,
    )
    bucketless_scan = replace(unqualified, route_calibration=(bucketless,))
    assert await trader.process(bucketless_scan) == ()
    scan = ScanResult(
        "BTC",
        1,
        (),
        (),
        (),
        (quote,),
        (),
        (),
        route_calibration=(calibration,),
    )
    original_reserve = trader._risk.reserve

    def delayed_reserve(request: Any) -> Any:
        time.sleep(0.06)
        return original_reserve(request)

    monkeypatch.setattr(trader._risk, "reserve", delayed_reserve)
    assert (
        await trader.process(
            scan,
            decision_deadline=asyncio.get_running_loop().time() + 0.05,
        )
        == ()
    )
    assert runtime.tranches == {}
    assert trader._risk.reservations == ()
    monkeypatch.setattr(trader._risk, "reserve", original_reserve)

    original_submit = trader._coordinator.submit_open
    captured_tranches: list[Tranche] = []

    def delayed_submit(
        tranche: Tranche,
        *args: Any,
        **kwargs: Any,
    ) -> bool:
        time.sleep(0.06)
        submitted = original_submit(tranche, *args, **kwargs)
        captured_tranches.append(tranche)
        return submitted

    monkeypatch.setattr(trader._coordinator, "submit_open", delayed_submit)
    assert (
        await trader.process(
            scan,
            decision_deadline=asyncio.get_running_loop().time() + 0.05,
        )
        == ()
    )
    assert len(captured_tranches) == 1
    assert captured_tranches[0].state == PairActionState.RISK_RESERVED
    assert captured_tranches[0].all_fills == ()
    assert runtime.tranches == {}
    assert trader._risk.reservations == ()
    monkeypatch.setattr(trader._coordinator, "submit_open", original_submit)

    captured_after_mutation: list[Tranche] = []

    def delayed_return_after_submit(
        tranche: Tranche,
        *args: Any,
        **kwargs: Any,
    ) -> bool:
        submitted = original_submit(tranche, *args, **kwargs)
        captured_after_mutation.append(tranche)
        time.sleep(0.06)
        return submitted

    monkeypatch.setattr(trader._coordinator, "submit_open", delayed_return_after_submit)
    assert (
        await trader.process(
            scan,
            decision_deadline=asyncio.get_running_loop().time() + 0.05,
        )
        == ()
    )
    assert len(captured_after_mutation) == 1
    assert captured_after_mutation[0].state == PairActionState.RISK_RESERVED
    assert captured_after_mutation[0].all_fills == ()
    assert runtime.tranches == {}
    assert trader._risk.reservations == ()
    monkeypatch.setattr(trader._coordinator, "submit_open", original_submit)

    original_save = save_tranche

    async def save_only_after_deadline(*args: Any, **kwargs: Any) -> None:
        deadline = cast(float, kwargs["deadline_monotonic"])
        await asyncio.sleep(max(0.0, deadline - asyncio.get_running_loop().time()) + 0.01)
        await original_save(*args, **kwargs)

    monkeypatch.setattr(shadow_module, "save_tranche", save_only_after_deadline)
    assert (
        await trader.process(
            scan,
            decision_deadline=asyncio.get_running_loop().time() + 0.02,
        )
        == ()
    )
    assert runtime.tranches == {}
    assert trader._risk.reservations == ()
    assert await load_tranches(state_path) == ()

    async def return_only_after_persisted_deadline(*args: Any, **kwargs: Any) -> None:
        await original_save(*args, **kwargs)
        deadline = cast(float, kwargs["deadline_monotonic"])
        loop = asyncio.get_running_loop()
        while loop.time() < deadline + 0.01:
            await asyncio.sleep(max(0.001, deadline + 0.01 - loop.time()))
        assert loop.time() >= deadline

    monkeypatch.setattr(shadow_module, "save_tranche", return_only_after_persisted_deadline)
    assert (
        await trader.process(
            scan,
            decision_deadline=asyncio.get_running_loop().time() + 0.25,
        )
        == ()
    )
    assert runtime.tranches == {}
    assert trader._risk.reservations == ()
    assert await load_tranches(state_path) == ()
    monkeypatch.setattr(shadow_module, "save_tranche", original_save)

    state_any = cast(Any, state_module)
    original_sync_save = state_any._save_tranche_sync
    writer_started = threading.Event()
    writer_release = threading.Event()

    def held_sync_save(*args: Any, **kwargs: Any) -> None:
        writer_started.set()
        if not writer_release.wait(2):
            raise TimeoutError("test did not release tranche writer")
        original_sync_save(*args, **kwargs)

    monkeypatch.setattr(state_any, "_save_tranche_sync", held_sync_save)
    cancelled_process = asyncio.create_task(
        trader.process(
            scan,
            decision_deadline=asyncio.get_running_loop().time() + 2,
        )
    )
    assert await asyncio.to_thread(writer_started.wait, 1)
    cancelled_process.cancel()
    await asyncio.sleep(0)
    writer_release.set()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_process
    assert len(runtime.tranches) == 1
    assert len(trader._risk.reservations) == 1
    persisted_after_cancellation = await load_tranches(state_path)
    assert len(persisted_after_cancellation) == 1
    cancelled_tranche_id = persisted_after_cancellation[0].tranche_id
    assert cancelled_tranche_id in runtime.tranches

    await delete_tranche(state_path, cancelled_tranche_id)
    runtime.tranches.pop(cancelled_tranche_id)
    trader._managed_ids.discard(cancelled_tranche_id)
    trader._risk.release(cancelled_tranche_id)
    monkeypatch.setattr(state_any, "_save_tranche_sync", original_sync_save)

    failing_writer_started = threading.Event()
    failing_writer_release = threading.Event()

    def held_failing_sync_save(*_args: Any, **_kwargs: Any) -> None:
        failing_writer_started.set()
        if not failing_writer_release.wait(2):
            raise TimeoutError("test did not release failing tranche writer")
        raise OSError("simulated SQLite write failure")

    monkeypatch.setattr(state_any, "_save_tranche_sync", held_failing_sync_save)
    twice_cancelled_process = asyncio.create_task(
        trader.process(
            scan,
            decision_deadline=asyncio.get_running_loop().time() + 2,
        )
    )
    assert await asyncio.to_thread(failing_writer_started.wait, 1)
    twice_cancelled_process.cancel()
    await asyncio.sleep(0)
    twice_cancelled_process.cancel()
    failing_writer_release.set()
    with pytest.raises(OSError, match="simulated SQLite write failure"):
        await twice_cancelled_process
    assert runtime.tranches == {}
    assert trader._risk.reservations == ()
    assert await load_tranches(state_path) == ()
    monkeypatch.setattr(state_any, "_save_tranche_sync", original_sync_save)

    original_sync_delete = state_any._delete_tranche_sync
    delete_started = threading.Event()
    delete_release = threading.Event()

    def held_sync_delete(*args: Any, **kwargs: Any) -> None:
        delete_started.set()
        if not delete_release.wait(2):
            raise TimeoutError("test did not release tranche deletion")
        original_sync_delete(*args, **kwargs)

    monkeypatch.setattr(shadow_module, "save_tranche", return_only_after_persisted_deadline)
    monkeypatch.setattr(state_any, "_delete_tranche_sync", held_sync_delete)
    cancelled_deletion_process = asyncio.create_task(
        trader.process(
            scan,
            decision_deadline=asyncio.get_running_loop().time() + 0.05,
        )
    )
    assert await asyncio.to_thread(delete_started.wait, 1)
    cancelled_deletion_process.cancel()
    await asyncio.sleep(0)
    cancelled_deletion_process.cancel()
    delete_release.set()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_deletion_process
    assert runtime.tranches == {}
    assert trader._risk.reservations == ()
    assert await load_tranches(state_path) == ()
    monkeypatch.setattr(state_any, "_delete_tranche_sync", original_sync_delete)
    monkeypatch.setattr(shadow_module, "save_tranche", original_save)

    decisions = await trader.process(scan)
    assert len(decisions) == 1
    assert decisions[0].reason == ReasonCode.ENTRY_ACCEPTED
    assert len(runtime.tranches) == 1
    opened = next(iter(runtime.tranches.values()))
    assert opened.state == PairActionState.HEDGED
    assert opened.actual_long_entry_quantity == Decimal("1")
    assert opened.actual_short_entry_quantity == Decimal("1")
    assert opened.projected_stress_usdt <= Decimal("5")

    await save_shadow_snapshot(
        state_path,
        {
            "opportunities": [
                {
                    "key": {"base": "BTC"},
                    "long_venue": Venue.BYBIT.value,
                    "short_venue": Venue.OKX.value,
                    "exit_long_vwap": "104",
                    "exit_short_vwap": "105",
                }
            ]
        },
    )
    assert await runtime.close_all_simulated() == (opened.tranche_id,)
    assert trader._risk.reservations == ()
    assert (await load_tranches(state_path))[0].state == PairActionState.FORCED_CLOSED
    await runtime.resume()
    await delete_tranche(state_path, opened.tranche_id)
    runtime.tranches.pop(opened.tranche_id)
    trader._managed_ids.discard(opened.tranche_id)

    hung_writer_started = threading.Event()
    hung_writer_release = threading.Event()

    def hung_sync_save(*args: Any, **kwargs: Any) -> None:
        hung_writer_started.set()
        hung_writer_release.wait()
        original_sync_save(*args, **kwargs)

    monkeypatch.setattr(state_any, "_save_tranche_sync", hung_sync_save)
    hung_process = asyncio.create_task(
        trader.process(
            scan,
            decision_deadline=asyncio.get_running_loop().time() + 2,
        )
    )
    assert await asyncio.to_thread(hung_writer_started.wait, 1)
    hung_process.cancel()
    await asyncio.sleep(0)
    hung_process.cancel()
    bounded_started = asyncio.get_running_loop().time()
    with pytest.raises(RuntimeError, match="persistence outcome is indeterminate"):
        await asyncio.wait_for(hung_process, timeout=1.3)
    assert asyncio.get_running_loop().time() - bounded_started < 1.3
    assert len(runtime.tranches) == 1
    assert len(trader._risk.reservations) == 1
    assert (await runtime.entry_gate()).reason == ReasonCode.RECONCILIATION_REQUIRED

    hung_writer_release.set()
    for _ in range(100):
        persisted_hung = await load_tranches(state_path)
        if persisted_hung:
            break
        await asyncio.sleep(0.01)
    assert len(persisted_hung) == 1
    hung_tranche_id = persisted_hung[0].tranche_id
    assert hung_tranche_id in runtime.tranches
    await delete_tranche(state_path, hung_tranche_id)
    runtime.tranches.pop(hung_tranche_id)
    trader._managed_ids.discard(hung_tranche_id)
    trader._risk.release(hung_tranche_id)
    monkeypatch.setattr(state_any, "_save_tranche_sync", original_sync_save)


@pytest.mark.asyncio
async def test_shadow_trader_deadline_expires_before_any_open_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "trader-deadline.sqlite3"
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(state_path)})
    runtime = ShadowRuntime(settings)
    await runtime.start()
    await runtime.reconcile(set())
    trader = ShadowTrader(settings, runtime)
    original_entry_gate = runtime.entry_gate

    async def delayed_entry_gate() -> Any:
        await asyncio.sleep(0.02)
        return await original_entry_gate()

    monkeypatch.setattr(runtime, "entry_gate", delayed_entry_gate)
    result = ScanResult("BTC", 1, (), (), (), (), (), ())

    decisions = await trader.process(
        result,
        decision_deadline=asyncio.get_running_loop().time() + 0.005,
    )

    assert decisions == ()
    assert runtime.tranches == {}
