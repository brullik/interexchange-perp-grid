from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Protocol

import pytest

from interexchange_perp_grid.client_ids import venue_client_order_id
from interexchange_perp_grid.domain import WAVE1_VENUES, Instrument, Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.live_control import LiveControlService
from interexchange_perp_grid.live_coordinator import (
    CanaryExecutionPlan,
    CloseReason,
    LiveCanaryCoordinator,
)
from interexchange_perp_grid.live_journal import (
    FlatBarrierCommitResult,
    LiveActionState,
    LiveJournalAction,
    LiveOrderJournal,
)
from interexchange_perp_grid.live_reconciliation import (
    FlatBarrierPolicy,
    FlatBarrierResult,
    ReconciliationReport,
    ReconciliationStatus,
    flat_barrier_failure_reason,
    wait_for_stable_flat,
)
from interexchange_perp_grid.live_simulator import (
    DeterministicCanaryMonitor,
    DeterministicPrivateExchange,
    StaticProtectionProvider,
)
from interexchange_perp_grid.private_domain import (
    PrivateOrder,
    PrivateOrderStatus,
    VenueOrderRequest,
)
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.strategy import DirectedRouteKey

_ROUTE = DirectedRouteKey("BTC", Venue.BINANCE_USDM, Venue.OKX)


def _report(
    *,
    status: ReconciliationStatus = ReconciliationStatus.CONSISTENT,
    flat_verified: bool = True,
    snapshots_complete: bool = True,
    open_position_count: int = 0,
) -> ReconciliationReport:
    actual = {Venue.BINANCE_USDM: Decimal("0.001")} if open_position_count else {}
    return ReconciliationReport(
        status=status,
        states={},
        discrepancies=() if flat_verified else ("not-flat",),
        unknown_client_order_ids=(),
        open_bot_order_count=0,
        open_position_count=open_position_count,
        actual_signed_positions=actual,
        expected_signed_positions={},
        residual_delta=sum(actual.values(), Decimal(0)),
        flat_verified=flat_verified,
        raw_open_order_count=0,
        raw_nonzero_position_count=open_position_count,
        unknown_active_record_count=0 if snapshots_complete else 1,
        snapshots_complete=snapshots_complete,
    )


def _instrument(venue: Venue) -> Instrument:
    return Instrument(
        venue,
        "BTC/USDT:USDT",
        "BTCUSDT",
        "BTC",
        "USDT",
        "USDT",
        Decimal("0.001"),
        Decimal(1),
        Decimal("0.1"),
        Decimal(1),
        Decimal("0.01"),
        Decimal("0.0005"),
        "private",
    )


def _adapters() -> dict[Venue, DeterministicPrivateExchange]:
    return {
        venue: DeterministicPrivateExchange(venue, _instrument(venue), ()) for venue in WAVE1_VENUES
    }


class _BarrierPublicResult(Protocol):
    @property
    def success(self) -> bool: ...

    @property
    def flat_barrier_verified(self) -> bool: ...


class _WatermarkedPrivateExchange(DeterministicPrivateExchange):
    private_event_watermark = 0

    def current_private_event_watermark(self) -> int:
        return self.private_event_watermark


class _PrivateWatermarkRaceJournal(LiveOrderJournal):
    def __init__(self, path: Path, adapter: _WatermarkedPrivateExchange) -> None:
        super().__init__(path)
        self._adapter = adapter

    async def commit_flat_barrier(
        self,
        pair_action_id: str | None,
        expected_event_watermark: int,
        details: dict[str, object] | None = None,
        *,
        now: datetime | None = None,
    ) -> FlatBarrierCommitResult:
        result = await super().commit_flat_barrier(
            pair_action_id,
            expected_event_watermark,
            details,
            now=now,
        )
        self._adapter.private_event_watermark += 1
        return result


def _watermarked_adapters(
    watermark: int,
) -> dict[Venue, _WatermarkedPrivateExchange]:
    adapters: dict[Venue, _WatermarkedPrivateExchange] = {}
    for venue in WAVE1_VENUES:
        adapter = _WatermarkedPrivateExchange(venue, _instrument(venue), ())
        adapter.private_event_watermark = watermark
        adapters[venue] = adapter
    return adapters


def _requests(identity: str) -> tuple[VenueOrderRequest, VenueOrderRequest]:
    return (
        VenueOrderRequest(
            Venue.BINANCE_USDM,
            venue_client_order_id(identity, "long"),
            "BTC/USDT:USDT",
            Side.BUY,
            "limit",
            Decimal(1),
            Decimal(100),
            "IOC",
            {},
        ),
        VenueOrderRequest(
            Venue.OKX,
            venue_client_order_id(identity, "short"),
            "BTC/USDT:USDT",
            Side.SELL,
            "limit",
            Decimal(1),
            Decimal(100),
            "IOC",
            {},
        ),
    )


async def _recovering_action(
    journal: LiveOrderJournal,
    identity: str,
) -> tuple[LiveJournalAction, VenueOrderRequest, VenueOrderRequest]:
    await journal.initialise()
    long_request, short_request = _requests(identity)
    action = await journal.prepare(
        identity,
        _ROUTE,
        "tranche-1",
        long_request,
        short_request,
        {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
        {Venue.BINANCE_USDM: Decimal(100), Venue.OKX: Decimal(100)},
        {"projected_stress_usdt": "0.8"},
        "a" * 64,
    )
    await journal.mark_submit_attempted(
        action.pair_action_id,
        (long_request.client_order_id, short_request.client_order_id),
    )
    action = await journal.transition(action.pair_action_id, LiveActionState.RECOVERING)
    return action, long_request, short_request


def _service(journal: LiveOrderJournal) -> LiveControlService:
    return LiveControlService(
        journal,
        _adapters(),
        {venue: _instrument(venue) for venue in WAVE1_VENUES},
        _ROUTE,
        "a" * 64,
    )


def _plan(
    identity: str,
    long_request: VenueOrderRequest,
    short_request: VenueOrderRequest,
) -> CanaryExecutionPlan:
    return CanaryExecutionPlan(
        identity,
        _ROUTE,
        "tranche-1",
        Decimal("0.001"),
        long_request,
        short_request,
        {},
        "a" * 64,
        1,
    )


def _record_negative_public_outcome(
    record_property: Callable[[str, object], None],
    scenario_id: str,
    result: _BarrierPublicResult,
) -> None:
    record_property("c43_scenario_id", scenario_id)
    record_property("c43_public_success", str(result.success).lower())
    record_property("c43_barrier_verified", str(result.flat_barrier_verified).lower())


@pytest.mark.asyncio
async def test_sf_001_one_flat_snapshot_then_timeout_never_succeeds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record_property: Callable[[str, object], None],
) -> None:
    flat = _report()
    calls = 0

    async def report_factory() -> ReconciliationReport:
        nonlocal calls
        calls += 1
        if calls > 1:
            await asyncio.Event().wait()
        return flat

    async def watermark() -> int:
        return 10

    barrier = await wait_for_stable_flat(
        report_factory,
        watermark,
        FlatBarrierPolicy(2, 0, 0.001, 0.05),
    )

    assert barrier.verified is False
    assert barrier.timed_out is True
    assert barrier.consecutive_snapshots == 1
    assert barrier.report.flat_verified is True
    assert flat_barrier_failure_reason(barrier) == ReasonCode.FLAT_BARRIER_TIMEOUT

    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await _recovering_action(journal, "sf-001")
    service = _service(journal)

    async def one_snapshot_timeout(_: object) -> FlatBarrierResult:
        return barrier

    monkeypatch.setattr(service, "_stable_report", one_snapshot_timeout)
    result = await service.emergency_flatten()

    assert result.success is False
    assert result.terminal_state == LiveActionState.QUARANTINED
    assert result.flat_barrier_verified is False
    _record_negative_public_outcome(record_property, "SF-001", result)


@pytest.mark.asyncio
async def test_sf_002_two_flat_snapshots_without_quiet_period_never_succeed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record_property: Callable[[str, object], None],
) -> None:
    flat = _report()

    async def report_factory() -> ReconciliationReport:
        return flat

    async def watermark() -> int:
        return 11

    barrier = await wait_for_stable_flat(
        report_factory,
        watermark,
        FlatBarrierPolicy(2, 0.05, 0.001, 0.05),
    )

    assert barrier.verified is False
    assert barrier.timed_out is True
    assert barrier.consecutive_snapshots >= 2
    assert barrier.report.flat_verified is True

    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await _recovering_action(journal, "sf-002")
    service = _service(journal)

    async def quiet_period_timeout(_: object) -> FlatBarrierResult:
        return barrier

    monkeypatch.setattr(service, "_stable_report", quiet_period_timeout)
    result = await service.emergency_flatten()

    assert result.success is False
    assert result.terminal_state == LiveActionState.QUARANTINED
    assert result.flat_barrier_verified is False
    _record_negative_public_outcome(record_property, "SF-002", result)


@pytest.mark.asyncio
async def test_sf_003_event_watermark_change_resets_stability_counter() -> None:
    flat = _report()
    report_calls = 0
    values = iter((0, 0, 0, 0, 1, 1, 1, 1, 1, 1))

    async def report_factory() -> ReconciliationReport:
        nonlocal report_calls
        report_calls += 1
        return flat

    async def watermark() -> int:
        return next(values)

    barrier = await wait_for_stable_flat(
        report_factory,
        watermark,
        FlatBarrierPolicy(2, 0, 0.001, 0.1),
    )

    assert barrier.verified is True
    assert barrier.event_watermark == 1
    assert report_calls >= 4


@pytest.mark.asyncio
async def test_sf_004_late_position_after_flat_snapshot_never_succeeds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record_property: Callable[[str, object], None],
) -> None:
    flat = _report()
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    action, long_request, _ = await _recovering_action(journal, "sf-004")

    async def report_factory() -> ReconciliationReport:
        return flat

    barrier = await wait_for_stable_flat(
        report_factory,
        journal.event_watermark,
        FlatBarrierPolicy(2, 0, 0.001, 0.05),
    )

    assert barrier.verified is True
    assert barrier.event_watermark == 0
    late_fill = PrivateOrder(
        venue=Venue.BINANCE_USDM,
        order_id="late-fill",
        client_order_id=long_request.client_order_id,
        symbol=long_request.symbol,
        side=Side.BUY,
        status=PrivateOrderStatus.FILLED,
        requested_base_quantity=Decimal("0.001"),
        filled_base_quantity=Decimal("0.001"),
        average_price=Decimal(100),
        fee_usdt=Decimal(0),
        observed_at=datetime.now(UTC),
        limit_price=Decimal(100),
    )
    assert await journal.record_order_event(action.pair_action_id, late_fill, "late-fill")
    assert await journal.event_watermark() == 1

    service = _service(journal)

    async def stale_verified_barrier(_: object) -> FlatBarrierResult:
        return barrier

    monkeypatch.setattr(service, "_stable_report", stale_verified_barrier)
    result = await service.emergency_flatten()

    assert result.success is False
    assert result.terminal_state == LiveActionState.QUARANTINED
    assert result.flat_barrier_verified is False
    assert result.reason == ReasonCode.FLAT_BARRIER_EVENT_RACE
    persisted = await journal.load(action.pair_action_id)
    assert persisted is not None
    assert persisted.state == LiveActionState.QUARANTINED
    assert persisted.legs[0].filled_base_quantity == Decimal("0.001")
    _record_negative_public_outcome(record_property, "SF-004", result)


@pytest.mark.asyncio
async def test_sf_005_identical_snapshots_quiet_period_and_watermark_verify() -> None:
    flat = _report()

    async def report_factory() -> ReconciliationReport:
        return flat

    async def watermark() -> int:
        return 12

    barrier = await wait_for_stable_flat(
        report_factory,
        watermark,
        FlatBarrierPolicy(2, 0.002, 0.001, 0.05),
    )

    assert barrier.verified is True
    assert barrier.timed_out is False
    assert barrier.consecutive_snapshots >= 2
    assert barrier.event_watermark == 12


@pytest.mark.asyncio
async def test_private_watermark_is_preserved_across_atomic_flat_commit(tmp_path: Path) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    adapters = _watermarked_adapters(2)
    service = LiveControlService(
        journal,
        adapters,
        {venue: _instrument(venue) for venue in WAVE1_VENUES},
        _ROUTE,
        "a" * 64,
        flat_barrier_policy=FlatBarrierPolicy(2, 0, 0.001, 0.5),
    )

    result = await service.emergency_flatten()

    assert result.success is True
    assert result.flat_barrier_verified is True
    assert result.flat_barrier_watermark == 6


@pytest.mark.asyncio
async def test_private_event_during_flat_commit_quarantines_action(tmp_path: Path) -> None:
    adapters = _watermarked_adapters(0)
    raced_adapter = adapters[Venue.BINANCE_USDM]
    journal = _PrivateWatermarkRaceJournal(tmp_path / "state.sqlite3", raced_adapter)
    action, _, _ = await _recovering_action(journal, "private-race")
    service = LiveControlService(
        journal,
        adapters,
        {venue: _instrument(venue) for venue in Venue},
        _ROUTE,
        "a" * 64,
        flat_barrier_policy=FlatBarrierPolicy(2, 0, 0.001, 0.05),
    )

    marked_action, barrier = await service._mark_flat_if_needed(
        action,
        "PRIVATE_EVENT_RACE_TEST",
        FlatBarrierResult(True, _report(), 2, 0, False),
    )

    assert marked_action is not None
    assert marked_action.state == LiveActionState.QUARANTINED
    assert barrier.verified is False
    assert barrier.failure_reason == ReasonCode.FLAT_BARRIER_EVENT_RACE
    assert barrier.event_watermark == 1


@pytest.mark.asyncio
async def test_coordinator_private_event_after_flat_commit_quarantines_action(
    tmp_path: Path,
) -> None:
    adapters = _watermarked_adapters(0)
    raced_adapter = adapters[Venue.BINANCE_USDM]
    journal = _PrivateWatermarkRaceJournal(tmp_path / "state.sqlite3", raced_adapter)
    action, _, _ = await _recovering_action(journal, "coordinator-private-race")
    action = await journal.transition(action.pair_action_id, LiveActionState.FLAT)
    coordinator = LiveCanaryCoordinator(
        journal,
        adapters,
        {venue: _instrument(venue) for venue in Venue},
        StaticProtectionProvider({(venue, side): Decimal(100) for venue in Venue for side in Side}),
        DeterministicCanaryMonitor(CloseReason.TARGET_CONVERGENCE),
        Venue.BYBIT,
    )

    marked_action, barrier = await coordinator._to_flat(
        action,
        "COORDINATOR_PRIVATE_EVENT_RACE_TEST",
        FlatBarrierResult(True, _report(), 2, 0, False),
    )

    assert marked_action.state == LiveActionState.QUARANTINED
    assert barrier.verified is False
    assert barrier.failure_reason == ReasonCode.FLAT_BARRIER_EVENT_RACE
    assert barrier.event_watermark == 1


@pytest.mark.asyncio
async def test_sf_006_coordinator_cannot_transition_on_unverified_flat_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record_property: Callable[[str, object], None],
) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    action, long_request, short_request = await _recovering_action(journal, "sf-006")
    action = await journal.transition(action.pair_action_id, LiveActionState.FLAT)
    instruments = {venue: _instrument(venue) for venue in Venue}
    coordinator = LiveCanaryCoordinator(
        journal,
        _adapters(),
        instruments,
        StaticProtectionProvider({(venue, side): Decimal(100) for venue in Venue for side in Side}),
        DeterministicCanaryMonitor(CloseReason.TARGET_CONVERGENCE),
        Venue.BYBIT,
    )
    conflict = FlatBarrierResult(
        False,
        _report(),
        1,
        13,
        True,
        ReasonCode.FLAT_BARRIER_TIMEOUT,
    )

    async def unverified_barrier(_: object) -> FlatBarrierResult:
        return conflict

    monkeypatch.setattr(coordinator, "_verify_stable_flat", unverified_barrier)
    result = await coordinator.run(_plan("sf-006", long_request, short_request))

    assert result.success is False
    assert result.terminal_state == LiveActionState.QUARANTINED
    assert result.flat_barrier_verified is False
    assert result.reason == ReasonCode.FLAT_BARRIER_TIMEOUT
    persisted = await journal.load(action.pair_action_id)
    assert persisted is not None
    assert persisted.state == LiveActionState.QUARANTINED
    _record_negative_public_outcome(record_property, "SF-006", result)


@pytest.mark.asyncio
async def test_sf_007_live_control_returns_failure_for_unverified_flat_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record_property: Callable[[str, object], None],
) -> None:
    instruments = {venue: _instrument(venue) for venue in Venue}
    service = LiveControlService(
        LiveOrderJournal(tmp_path / "state.sqlite3"),
        _adapters(),
        instruments,
        _ROUTE,
        "a" * 64,
    )
    conflict = FlatBarrierResult(
        False,
        _report(),
        0,
        14,
        True,
        ReasonCode.FLAT_BARRIER_EVENT_RACE,
    )

    async def unverified_barrier(_: object) -> FlatBarrierResult:
        return conflict

    monkeypatch.setattr(service, "_stable_report", unverified_barrier)
    result = await service.emergency_flatten()

    assert result.success is False
    assert result.flat_barrier_verified is False
    assert result.flat_barrier_timed_out is True
    assert result.flat_barrier_snapshots == 0
    assert result.flat_barrier_watermark == 14
    assert result.reason == ReasonCode.FLAT_BARRIER_EVENT_RACE
    _record_negative_public_outcome(record_property, "SF-007", result)


@pytest.mark.asyncio
async def test_sf_008_private_unknown_during_barrier_times_out_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record_property: Callable[[str, object], None],
) -> None:
    unknown = _report(
        status=ReconciliationStatus.UNKNOWN,
        flat_verified=False,
        snapshots_complete=False,
    )

    calls = 0

    async def report_factory() -> ReconciliationReport:
        nonlocal calls
        calls += 1
        return unknown if calls == 1 else _report()

    async def watermark() -> int:
        return 15

    barrier = await wait_for_stable_flat(
        report_factory,
        watermark,
        FlatBarrierPolicy(2, 0, 0.001, 0.05),
    )

    assert barrier.verified is False
    assert barrier.timed_out is True
    assert flat_barrier_failure_reason(barrier) == (ReasonCode.FLAT_BARRIER_PRIVATE_STATE_UNKNOWN)
    assert calls > 1

    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await _recovering_action(journal, "sf-008")
    service = _service(journal)

    async def transient_unknown(_: object) -> FlatBarrierResult:
        return barrier

    monkeypatch.setattr(service, "_stable_report", transient_unknown)
    result = await service.emergency_flatten()

    assert result.success is False
    assert result.terminal_state == LiveActionState.QUARANTINED
    assert result.flat_barrier_verified is False
    assert result.reason == ReasonCode.FLAT_BARRIER_PRIVATE_STATE_UNKNOWN
    _record_negative_public_outcome(record_property, "SF-008", result)


@pytest.mark.asyncio
async def test_stable_flat_barrier_hard_times_out_hung_private_snapshot() -> None:
    cancelled = asyncio.Event()

    async def hung_report() -> ReconciliationReport:
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
        raise AssertionError("unreachable")

    async def watermark() -> int:
        return 16

    loop = asyncio.get_running_loop()
    started = loop.time()
    barrier = await wait_for_stable_flat(
        hung_report,
        watermark,
        FlatBarrierPolicy(2, 0, 0.001, 0.01),
    )

    assert loop.time() - started < 0.1
    assert cancelled.is_set()
    assert barrier.verified is False
    assert barrier.timed_out is True
    assert barrier.report.status == ReconciliationStatus.UNKNOWN
    assert flat_barrier_failure_reason(barrier) == ReasonCode.FLAT_BARRIER_PRIVATE_STATE_UNKNOWN
