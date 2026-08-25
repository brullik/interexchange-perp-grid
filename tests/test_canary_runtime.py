from __future__ import annotations

import asyncio
import sqlite3
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from interexchange_perp_grid.canary_runtime import (
    OWNER_CONFIRMATION,
    _coordinate_live_action,
    _rebuild_active_plan,
    _resume_active_canary,
    _shutdown_opening_gate_tasks,
    _stored_opening_request_is_still_protected,
    run_canary_once,
)
from interexchange_perp_grid.config import load_settings
from interexchange_perp_grid.domain import (
    BookLevel,
    CapabilityReport,
    FundingSnapshot,
    Instrument,
    OrderBookSnapshot,
    Venue,
)
from interexchange_perp_grid.execution import ExecutionIntent, OrderPurpose, Side
from interexchange_perp_grid.live_coordinator import (
    CanaryExecutionPlan,
    CloseReason,
    LiveCanaryCoordinator,
)
from interexchange_perp_grid.live_journal import LiveActionState, LiveOrderJournal
from interexchange_perp_grid.live_simulator import (
    DeterministicCanaryMonitor,
    DeterministicPrivateExchange,
    StaticProtectionProvider,
)
from interexchange_perp_grid.market_data import DataQualityAssessment
from interexchange_perp_grid.private_domain import PrivateCapabilityReport, VenueOrderRequest
from interexchange_perp_grid.private_execution import translate_protected_order
from interexchange_perp_grid.qualification import (
    LAPTOP_OWNER_EXCEPTION_CONFIRMATION,
    LAPTOP_OWNER_EXCEPTION_ENV,
    laptop_owner_exception_policy,
)
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.risk_stages import load_locked_risk_stage_table
from interexchange_perp_grid.state import RiskStage, RuntimeControls
from interexchange_perp_grid.strategy import DirectedRouteKey


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
        Decimal("0.001"),
        "private",
    )


@pytest.mark.parametrize(
    ("side", "stored", "current", "expected"),
    (
        (Side.BUY, "100", "99", False),
        (Side.BUY, "100", "100", True),
        (Side.BUY, "100", "101", True),
        (Side.SELL, "100", "101", False),
        (Side.SELL, "100", "100", True),
        (Side.SELL, "100", "99", True),
    ),
)
def test_prepared_opening_gate_never_weakens_stored_price_cap(
    side: Side,
    stored: str,
    current: str,
    expected: bool,
) -> None:
    request = VenueOrderRequest(
        Venue.BYBIT,
        "opening-gate",
        "BTC/USDT:USDT",
        side,
        "limit",
        Decimal("1"),
        Decimal(stored),
        "IOC",
        {},
    )

    assert _stored_opening_request_is_still_protected(request, Decimal(current)) is expected


def test_prepared_opening_gate_requires_stored_ioc_to_remain_marketable() -> None:
    buy = VenueOrderRequest(
        Venue.BYBIT,
        "opening-buy",
        "BTC/USDT:USDT",
        Side.BUY,
        "limit",
        Decimal(1),
        Decimal(100),
        "IOC",
        {},
    )
    sell = VenueOrderRequest(
        Venue.OKX,
        "opening-sell",
        "BTC/USDT:USDT",
        Side.SELL,
        "limit",
        Decimal(1),
        Decimal(100),
        "IOC",
        {},
    )

    assert not _stored_opening_request_is_still_protected(buy, Decimal(101), Decimal("100.5"))
    assert not _stored_opening_request_is_still_protected(sell, Decimal(99), Decimal("99.5"))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        "private",
        "emergency_depth",
        "marketability",
        "transport_timeout",
        "late_kill",
        "controls_timeout",
    ],
)
async def test_production_prepared_gate_denies_current_opening_failure_before_submit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    import interexchange_perp_grid.canary_runtime as runtime_module

    transport_release = asyncio.Event()

    class PublicAdapter:
        def __init__(self, venue: Venue) -> None:
            self.venue = venue

        async def probe_public_capabilities(self) -> CapabilityReport:
            if failure == "transport_timeout" and self.venue == Venue.BYBIT:
                try:
                    await transport_release.wait()
                except asyncio.CancelledError:
                    await transport_release.wait()
            return CapabilityReport(
                self.venue,
                True,
                True,
                True,
                True,
                True,
                0,
                datetime.now(UTC),
                (),
            )

        async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot:
            return FundingSnapshot(
                self.venue,
                instrument.symbol,
                Decimal(0),
                1,
                "8h",
                Decimal(100),
                Decimal(100),
                1,
            )

    class PrivateAdapter(DeterministicPrivateExchange):
        async def probe_private_capabilities(self) -> PrivateCapabilityReport:
            ready = failure != "private" or self.venue != Venue.BYBIT
            return PrivateCapabilityReport(
                self.venue,
                ready,
                ready,
                ready,
                ready,
                ready,
                ready,
                ready,
                ready,
                ready,
                datetime.now(UTC),
                () if ready else ("submit_order",),
                ready,
                ready,
            )

    async def fresh_books(
        settings: object,
        adapters: object,
        instruments: dict[Venue, Instrument],
    ) -> tuple[dict[Venue, OrderBookSnapshot], dict[Venue, DataQualityAssessment]]:
        del settings, adapters
        observed_at = datetime.now(UTC)
        received_ns = time.monotonic_ns()
        books = {
            venue: OrderBookSnapshot(
                venue,
                instrument.symbol,
                (
                    ()
                    if failure == "emergency_depth" and venue == Venue.BYBIT
                    else (BookLevel(Decimal(100), Decimal(1)),)
                ),
                (
                    BookLevel(
                        Decimal("100.5")
                        if failure == "marketability" and venue == Venue.BINANCE_USDM
                        else Decimal(100),
                        Decimal(1),
                    ),
                ),
                1,
                observed_at,
                received_ns,
                1,
                1,
                True,
                True,
                0,
            )
            for venue, instrument in instruments.items()
        }
        return books, {
            venue: DataQualityAssessment(True, ReasonCode.QUOTE_READY, 0) for venue in instruments
        }

    monkeypatch.setattr(runtime_module, "_fresh_books", fresh_books)
    monkeypatch.setattr(
        runtime_module,
        "PublicProtectionProvider",
        lambda *_: StaticProtectionProvider(
            {(venue, side): Decimal(100) for venue in Venue for side in Side}
        ),
    )
    settings = load_settings(
        Path("config/defaults.yaml"),
        {"IPEG_STATE_PATH": str(tmp_path / "state.sqlite3")},
    )
    if failure in {"late_kill", "controls_timeout"}:

        async def controls(_: Path) -> RuntimeControls:
            return RuntimeControls(
                paused=False,
                killed=False,
                reconciliation_state="READY",
                updated_at=datetime.now(UTC),
            )

        async def final_controls(_: Path) -> RuntimeControls:
            if failure == "controls_timeout":
                while not transport_release.is_set():
                    try:
                        await asyncio.shield(transport_release.wait())
                    except asyncio.CancelledError:
                        continue
            return RuntimeControls(
                paused=False,
                killed=failure == "late_kill",
                reconciliation_state="READY",
                updated_at=datetime.now(UTC),
            )

        monkeypatch.setattr(runtime_module, "read_runtime_controls", controls)
        monkeypatch.setattr(runtime_module, "read_runtime_controls_bounded", final_controls)
    instruments = {venue: _instrument(venue) for venue in Venue}
    private = {
        venue: PrivateAdapter(venue, instruments[venue], ())
        for venue in (Venue.BINANCE_USDM, Venue.BYBIT, Venue.OKX)
    }
    public = {venue: PublicAdapter(venue) for venue in private}
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    route = DirectedRouteKey("BTC", Venue.BINANCE_USDM, Venue.OKX)
    plan = CanaryExecutionPlan(
        "prepared-current-gate",
        route,
        "tranche-1",
        Decimal("0.001"),
        VenueOrderRequest(
            Venue.BINANCE_USDM,
            "prepared-current-gate-long",
            instruments[Venue.BINANCE_USDM].symbol,
            Side.BUY,
            "limit",
            Decimal("1"),
            Decimal("100"),
            "IOC",
            {},
        ),
        VenueOrderRequest(
            Venue.OKX,
            "prepared-current-gate-short",
            instruments[Venue.OKX].symbol,
            Side.SELL,
            "limit",
            Decimal("1"),
            Decimal("100"),
            "IOC",
            {},
        ),
        {"projected_stress_usdt": "0.8"},
        "a" * 64,
        30,
    )

    started_at = time.monotonic()
    result = await _coordinate_live_action(
        settings,
        journal,
        private,  # type: ignore[arg-type]
        private,  # type: ignore[arg-type]
        instruments,
        public,  # type: ignore[arg-type]
        plan,
        DeterministicCanaryMonitor(CloseReason.TARGET_CONVERGENCE),
        Venue.BYBIT,
    )
    elapsed = time.monotonic() - started_at
    transport_release.set()
    await _shutdown_opening_gate_tasks()

    assert result.success is False
    assert result.orders_sent == 0
    assert result.terminal_state == LiveActionState.QUARANTINED
    assert sum(adapter.submit_calls for adapter in private.values()) == 0
    if failure in {"transport_timeout", "controls_timeout"}:
        assert elapsed < 1.5


@pytest.mark.asyncio
async def test_prepared_resume_reports_nonterminal_discovery_after_bounded_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import interexchange_perp_grid.canary_runtime as runtime_module

    release = asyncio.Event()

    class PublicAdapter:
        def __init__(self, venue: Venue) -> None:
            self.venue = venue

        async def close(self) -> None:
            while not release.is_set():
                try:
                    await asyncio.shield(release.wait())
                except asyncio.CancelledError:
                    continue

    async def resistant_discovery(
        base: str,
        adapters: object,
    ) -> tuple[dict[Venue, Instrument], dict[Venue, CapabilityReport]]:
        del base, adapters
        while not release.is_set():
            try:
                await asyncio.shield(release.wait())
            except asyncio.CancelledError:
                continue
        return {}, {}

    monkeypatch.setattr(runtime_module, "CcxtProAdapter", PublicAdapter)
    monkeypatch.setattr(runtime_module, "_discover_instruments", resistant_discovery)
    settings = load_settings(
        Path("config/defaults.yaml"),
        {"IPEG_STATE_PATH": str(tmp_path / "state.sqlite3")},
    )
    instruments = {venue: _instrument(venue) for venue in Venue}
    adapters = {
        venue: DeterministicPrivateExchange(venue, instruments[venue], ())
        for venue in (Venue.BINANCE_USDM, Venue.BYBIT, Venue.OKX)
    }
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    coordinator = LiveCanaryCoordinator(
        journal,
        adapters,
        instruments,
        StaticProtectionProvider({}),
        DeterministicCanaryMonitor(CloseReason.TARGET_CONVERGENCE),
        Venue.BYBIT,
    )
    plan = CanaryExecutionPlan(
        "prepared-resistant-discovery",
        DirectedRouteKey("BTC", Venue.BINANCE_USDM, Venue.OKX),
        "tranche-1",
        Decimal("0.001"),
        VenueOrderRequest(
            Venue.BINANCE_USDM,
            "prepared-resistant-long",
            instruments[Venue.BINANCE_USDM].symbol,
            Side.BUY,
            "limit",
            Decimal(1),
            Decimal(100),
            "IOC",
            {},
        ),
        VenueOrderRequest(
            Venue.OKX,
            "prepared-resistant-short",
            instruments[Venue.OKX].symbol,
            Side.SELL,
            "limit",
            Decimal(1),
            Decimal(100),
            "IOC",
            {},
        ),
        {"projected_stress_usdt": "0.8"},
        "a" * 64,
        30,
    )
    active = await coordinator.prepare(plan)

    started_at = time.monotonic()
    with pytest.raises(RuntimeError, match="canary cleanup failed") as cleanup_error:
        await _resume_active_canary(settings, journal, active)
    elapsed = time.monotonic() - started_at

    assert "adapter close: TimeoutError" in str(cleanup_error.value)
    assert "opening capability gate transport did not terminate" in str(cleanup_error.value)
    assert elapsed < 3.5
    quarantined = await journal.load(active.pair_action_id)
    assert quarantined is not None
    assert quarantined.state == LiveActionState.QUARANTINED
    release.set()
    await _shutdown_opening_gate_tasks()


@pytest.mark.asyncio
async def test_owner_gate_denial_performs_no_network_or_adapter_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import interexchange_perp_grid.canary_runtime as runtime_module

    constructed = 0

    class ForbiddenAdapter:
        def __init__(self, *args: object, **kwargs: object) -> None:
            nonlocal constructed
            del args, kwargs
            constructed += 1
            raise AssertionError("network adapter constructed before owner gate")

    monkeypatch.setattr(runtime_module, "CcxtProAdapter", ForbiddenAdapter)
    monkeypatch.setattr(runtime_module, "CcxtPrivateAdapter", ForbiddenAdapter)
    settings = load_settings(
        Path("config/defaults.yaml"),
        {"IPEG_STATE_PATH": str(tmp_path / "state.sqlite3")},
    )
    result = await run_canary_once(
        settings,
        Path("config/defaults.yaml"),
        tmp_path / "missing-qualification.json",
        Path("."),
        "WRONG_OWNER_CONFIRMATION",
    )

    assert result.reason == ReasonCode.OWNER_CONFIRMATION_MISSING
    assert result.orders_sent == 0
    assert constructed == 0


@pytest.mark.asyncio
async def test_aggressive_canary_requires_intent_and_binding_together_before_state(
    tmp_path: Path,
) -> None:
    settings = load_settings(
        Path("config/defaults.yaml"),
        {"IPEG_STATE_PATH": str(tmp_path / "state.sqlite3")},
    )

    result = await run_canary_once(
        settings,
        Path("config/defaults.yaml"),
        tmp_path / "missing-qualification.json",
        Path("."),
        OWNER_CONFIRMATION,
        aggressive_binding=object(),  # type: ignore[arg-type]
    )

    assert result.reason == ReasonCode.CANARY_POLICY_VIOLATION
    assert result.orders_sent == 0
    assert not (tmp_path / "state.sqlite3").exists()


@pytest.mark.asyncio
async def test_shadow_risk_stage_blocks_canary_before_network_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import interexchange_perp_grid.canary_runtime as runtime_module

    constructed = 0

    class ForbiddenAdapter:
        def __init__(self, *args: object, **kwargs: object) -> None:
            nonlocal constructed
            del args, kwargs
            constructed += 1
            raise AssertionError("network adapter constructed before risk stage gate")

    monkeypatch.setattr(runtime_module, "CcxtProAdapter", ForbiddenAdapter)
    monkeypatch.setattr(runtime_module, "CcxtPrivateAdapter", ForbiddenAdapter)
    settings = load_settings(
        Path("config/defaults.yaml"),
        {"IPEG_STATE_PATH": str(tmp_path / "state.sqlite3")},
    )

    result = await run_canary_once(
        settings,
        Path("config/defaults.yaml"),
        tmp_path / "missing-qualification.json",
        Path("."),
        OWNER_CONFIRMATION,
    )

    assert result.reason == ReasonCode.CANARY_POLICY_VIOLATION
    assert result.orders_sent == 0
    assert constructed == 0


@pytest.mark.asyncio
async def test_laptop_exception_allows_only_one_completed_canary_before_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import interexchange_perp_grid.canary_runtime as runtime_module

    constructed = 0

    class ForbiddenAdapter:
        def __init__(self, *args: object, **kwargs: object) -> None:
            nonlocal constructed
            del args, kwargs
            constructed += 1
            raise AssertionError("network adapter constructed after laptop canary was consumed")

    settings = load_settings(
        Path("config/defaults.yaml"),
        {"IPEG_STATE_PATH": str(tmp_path / "state.sqlite3")},
    )
    locked = load_locked_risk_stage_table(Path("config/RUNTIME_POLICY.yaml"))
    route = DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX)
    generated_at = datetime.now(UTC)
    qualification_hash = "a" * 64
    qualification_path = tmp_path / "qualification.json"
    qualification_path.touch()
    evidence = SimpleNamespace(
        route=route,
        strategy=object(),
        replay_shadow=object(),
        policy=laptop_owner_exception_policy(settings),
        generated_at=generated_at,
        qualification_hash=qualification_hash,
    )

    async def risk_stage(_: Path) -> SimpleNamespace:
        return SimpleNamespace(
            stage=RiskStage.CANARY,
            completion_frozen=False,
            qualification_hash=qualification_hash,
            runtime_policy_sha256=locked.runtime_policy_sha256,
        )

    async def completed_actions_since(
        self: LiveOrderJournal,
        started_at: datetime,
        observed_qualification_hash: str,
    ) -> tuple[object, ...]:
        del self
        assert started_at == generated_at
        assert observed_qualification_hash == qualification_hash
        return (object(),)

    monkeypatch.setenv(LAPTOP_OWNER_EXCEPTION_ENV, LAPTOP_OWNER_EXCEPTION_CONFIRMATION)
    monkeypatch.setattr(runtime_module, "read_risk_stage", risk_stage)
    monkeypatch.setattr(runtime_module, "load_qualification", lambda _: evidence)
    monkeypatch.setattr(
        runtime_module,
        "resolve_runtime_artifact_digest",
        lambda *_: "sha256:" + "b" * 64,
    )
    monkeypatch.setattr(
        runtime_module,
        "qualification_is_current",
        lambda *_args, **_kwargs: (True, ReasonCode.QUOTE_READY),
    )
    monkeypatch.setattr(LiveOrderJournal, "completed_actions_since", completed_actions_since)
    monkeypatch.setattr(runtime_module, "CcxtProAdapter", ForbiddenAdapter)
    monkeypatch.setattr(runtime_module, "CcxtPrivateAdapter", ForbiddenAdapter)

    result = await run_canary_once(
        settings,
        Path("config/defaults.yaml"),
        qualification_path,
        Path("."),
        OWNER_CONFIRMATION,
    )

    assert result.reason == ReasonCode.CANARY_POLICY_VIOLATION
    assert result.orders_sent == 0
    assert constructed == 0


@pytest.mark.asyncio
async def test_active_canary_plan_rebuilds_only_from_exact_durable_requests(
    tmp_path: Path,
) -> None:
    route = DirectedRouteKey("BTC", Venue.BINANCE_USDM, Venue.OKX)
    instruments = {venue: _instrument(venue) for venue in Venue}
    pair_id = "ipeg-canary-restart"
    long_request = translate_protected_order(
        ExecutionIntent(
            f"{pair_id}-long",
            route.long_venue,
            Side.BUY,
            OrderPurpose.NORMAL_OPEN,
            Decimal("0.001"),
            Decimal("101"),
        ),
        instruments[route.long_venue],
    )
    short_request = translate_protected_order(
        ExecutionIntent(
            f"{pair_id}-short",
            route.short_venue,
            Side.SELL,
            OrderPurpose.NORMAL_OPEN,
            Decimal("0.001"),
            Decimal("99"),
        ),
        instruments[route.short_venue],
    )
    path = tmp_path / "state.sqlite3"
    journal = LiveOrderJournal(path)
    await journal.initialise()
    action = await journal.prepare(
        pair_id,
        route,
        "tranche-1",
        long_request,
        short_request,
        {route.long_venue: Decimal("0.001"), route.short_venue: Decimal("0.001")},
        {route.long_venue: Decimal("101"), route.short_venue: Decimal("99")},
        {"projected_stress_usdt": "0.8"},
        "a" * 64,
    )

    rebuilt = _rebuild_active_plan(action, instruments, 300)
    assert rebuilt.pair_action_id == pair_id
    assert rebuilt.long_request == long_request
    assert rebuilt.short_request == short_request

    with sqlite3.connect(path) as database:
        database.execute(
            "UPDATE live_order_legs SET request_payload_hash = ? WHERE client_order_id = ?",
            ("0" * 64, long_request.client_order_id),
        )
    tampered = await journal.load(pair_id)
    assert tampered is not None
    with pytest.raises(ValueError, match="durable request"):
        _rebuild_active_plan(tampered, instruments, 300)
