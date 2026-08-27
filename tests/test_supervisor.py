from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from functools import partial
from pathlib import Path
from types import SimpleNamespace

import pytest

import interexchange_perp_grid.canary_runtime as canary_runtime_module
from interexchange_perp_grid.canary_runtime import recover_active_actions, recover_active_canary
from interexchange_perp_grid.client_ids import venue_client_order_id
from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.live_journal import (
    LiveActionState,
    LiveJournalAction,
    LiveOrderJournal,
)
from interexchange_perp_grid.priority_scheduler import (
    PriorityWorkScheduler,
    WorkPriority,
    WorkRejected,
)
from interexchange_perp_grid.private_domain import VenueOrderRequest
from interexchange_perp_grid.strategy import DirectedRouteKey
from interexchange_perp_grid.supervisor import LiveSafetySupervisor, SupervisorMode

_ROUTE = DirectedRouteKey("BTC", Venue.BINANCE_USDM, Venue.OKX)


def _request(venue: Venue, side: Side, role: str) -> VenueOrderRequest:
    return VenueOrderRequest(
        venue=venue,
        client_order_id=venue_client_order_id("supervisor-test", role),
        symbol="BTC/USDT:USDT",
        side=side,
        order_type="limit",
        amount_contracts=Decimal("1"),
        price=Decimal("100"),
        time_in_force="IOC",
        params={"timeInForce": "IOC"},
    )


async def _prepare(journal: LiveOrderJournal) -> LiveJournalAction:
    await journal.initialise()
    long_request = _request(Venue.BINANCE_USDM, Side.BUY, "long")
    short_request = _request(Venue.OKX, Side.SELL, "short")
    return await journal.prepare(
        "supervisor-action",
        _ROUTE,
        "tranche-1",
        long_request,
        short_request,
        {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
        {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
        {
            "supervisor_intent": "LIVE_CANARY",
            "supervisor_queued": True,
            "projected_stress_usdt": "0.8",
        },
        "a" * 64,
    )


async def _prepare_named(
    journal: LiveOrderJournal,
    pair_action_id: str,
    base: str,
) -> LiveJournalAction:
    await journal.initialise()
    route = DirectedRouteKey(base, Venue.BINANCE_USDM, Venue.OKX)
    long_request = replace(
        _request(Venue.BINANCE_USDM, Side.BUY, f"{base.lower()}-long"),
        client_order_id=venue_client_order_id(pair_action_id, "long"),
    )
    short_request = replace(
        _request(Venue.OKX, Side.SELL, f"{base.lower()}-short"),
        client_order_id=venue_client_order_id(pair_action_id, "short"),
    )
    return await journal.prepare(
        pair_action_id,
        route,
        "tranche-1",
        long_request,
        short_request,
        {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
        {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
        {
            "supervisor_intent": "MULTI_ACTION_RECOVERY",
            "projected_stress_usdt": "0.8",
        },
        "b" * 64,
    )


async def _move_to(journal: LiveOrderJournal, target: LiveActionState) -> LiveJournalAction:
    action = await _prepare(journal)
    if target == LiveActionState.PREPARED:
        return action
    await journal.mark_submit_attempted(
        action.pair_action_id,
        tuple(leg.client_order_id for leg in action.legs),
    )
    loaded = await journal.load(action.pair_action_id)
    assert loaded is not None
    action = loaded
    if target == LiveActionState.SUBMITTING:
        return action
    action = await journal.transition(action.pair_action_id, LiveActionState.PARTIAL)
    if target == LiveActionState.PARTIAL:
        return action
    action = await journal.transition(action.pair_action_id, LiveActionState.HEDGED)
    if target == LiveActionState.HEDGED:
        return action
    return await journal.transition(action.pair_action_id, LiveActionState.CLOSING)


async def _force_exchange_verified_flat(
    journal: LiveOrderJournal,
    action: LiveJournalAction,
) -> None:
    if action.state == LiveActionState.PREPARED:
        action = await journal.transition(action.pair_action_id, LiveActionState.QUARANTINED)
    if action.state == LiveActionState.HEDGED:
        action = await journal.transition(action.pair_action_id, LiveActionState.CLOSING)
    if action.state not in {LiveActionState.RECOVERING, LiveActionState.QUARANTINED}:
        action = await journal.transition(action.pair_action_id, LiveActionState.RECOVERING)
    await journal.transition(
        action.pair_action_id,
        LiveActionState.FLAT,
        {"exchange_verified": True},
        residual_delta=Decimal(0),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "restart_state",
    [
        LiveActionState.PREPARED,
        LiveActionState.PARTIAL,
        LiveActionState.HEDGED,
        LiveActionState.CLOSING,
    ],
)
async def test_restart_enters_recovery_only_and_reaches_flat(
    tmp_path: Path,
    restart_state: LiveActionState,
) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    await _move_to(journal, restart_state)
    observed: list[LiveActionState] = []

    async def recover(action: LiveJournalAction) -> object:
        observed.append(action.state)
        await _force_exchange_verified_flat(journal, action)
        return object()

    supervisor = LiveSafetySupervisor(journal, recover, poll_interval_seconds=0.01)
    health = await supervisor.reconcile_once()

    assert observed == [restart_state]
    assert health.mode == SupervisorMode.IDLE
    assert health.action_state == LiveActionState.FLAT
    assert health.active_action_count == 0
    assert health.recovery_required is False
    assert await journal.active() is None
    assert await supervisor.health() == health


@pytest.mark.asyncio
async def test_supervisor_detects_new_durable_intent_without_process_restart(
    tmp_path: Path,
) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    recovered = asyncio.Event()

    async def recover(action: LiveJournalAction) -> object:
        await _force_exchange_verified_flat(journal, action)
        recovered.set()
        return object()

    supervisor = LiveSafetySupervisor(journal, recover, poll_interval_seconds=0.01)
    stop = asyncio.Event()
    task = asyncio.create_task(supervisor.run(stop))
    try:
        for _ in range(100):
            if (await supervisor.health()).mode == SupervisorMode.IDLE:
                break
            await asyncio.sleep(0.01)
        await _prepare(journal)
        await asyncio.wait_for(recovered.wait(), timeout=2)
        assert await journal.active() is None
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=2)


@pytest.mark.asyncio
async def test_supervisor_persists_fail_closed_health_when_recovery_raises(tmp_path: Path) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    action = await _move_to(journal, LiveActionState.PARTIAL)

    async def fail_recovery(active: LiveJournalAction) -> object:
        assert active.pair_action_id == action.pair_action_id
        raise ConnectionError("private transport unavailable")

    supervisor = LiveSafetySupervisor(journal, fail_recovery)
    health = await supervisor.reconcile_once()

    assert health.mode == SupervisorMode.BLOCKED
    assert health.recovery_required is True
    assert health.failure == f"{action.pair_action_id}:ConnectionError"
    assert health.active_pair_action_id == action.pair_action_id
    assert health.active_action_count == 1
    assert await journal.active() is not None


@pytest.mark.asyncio
async def test_one_route_recovery_failure_does_not_block_another_route_flatten(
    tmp_path: Path,
) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    failed = await _prepare_named(journal, "action-btc", "BTC")
    recovered = await _prepare_named(journal, "action-eth", "ETH")
    recovered_flat = asyncio.Event()

    async def recover(action: LiveJournalAction) -> object:
        if action.pair_action_id == failed.pair_action_id:
            await asyncio.wait_for(recovered_flat.wait(), timeout=1)
            raise ConnectionError("BTC private transport unavailable")
        await _force_exchange_verified_flat(journal, action)
        recovered_flat.set()
        return object()

    supervisor = LiveSafetySupervisor(journal, recover)
    health = await supervisor.reconcile_once()

    assert recovered_flat.is_set()
    assert health.mode == SupervisorMode.BLOCKED
    assert health.recovery_required is True
    assert health.active_pair_action_id == failed.pair_action_id
    assert health.active_action_count == 1
    assert health.failure == "action-btc:ConnectionError"
    active = await journal.active_actions()
    assert tuple(action.pair_action_id for action in active) == (failed.pair_action_id,)
    recovered_action = await journal.load(recovered.pair_action_id)
    assert recovered_action is not None and recovered_action.state == LiveActionState.FLAT


@pytest.mark.asyncio
async def test_compatible_aggressive_tranches_use_one_portfolio_recovery_owner(
    tmp_path: Path,
) -> None:
    journal = LiveOrderJournal(tmp_path / "portfolio.sqlite3")
    await journal.initialise()
    for level in (1, 2):
        action_id = f"aggressive-{level}"
        await journal.prepare(
            action_id,
            _ROUTE,
            f"level-{level}",
            replace(
                _request(Venue.BINANCE_USDM, Side.BUY, f"long-{level}"),
                client_order_id=venue_client_order_id(action_id, "long"),
            ),
            replace(
                _request(Venue.OKX, Side.SELL, f"short-{level}"),
                client_order_id=venue_client_order_id(action_id, "short"),
            ),
            {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
            {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
            {
                "strategy": "AGGRESSIVE_SYMBIOSIS_V1",
                "stage": "pilot_a",
                "level_index": level,
                "aggressive_binding_sha256": "c" * 64,
                "strategy_profile_sha256": "d" * 64,
                "projected_stress_usdt": "0.8",
            },
            "b" * 64,
        )
    calls = 0

    async def recover(_: LiveJournalAction) -> object:
        nonlocal calls
        calls += 1
        for action in await journal.active_actions():
            await _force_exchange_verified_flat(journal, action)
        return object()

    health = await LiveSafetySupervisor(journal, recover).reconcile_once()

    assert calls == 1
    assert health.mode == SupervisorMode.IDLE, health


@pytest.mark.asyncio
async def test_v2_portfolio_accepts_distinct_single_use_activations_under_one_owner(
    tmp_path: Path,
) -> None:
    journal = LiveOrderJournal(tmp_path / "fast-portfolio.sqlite3")
    await journal.initialise()
    observed = datetime(2026, 8, 26, 12, tzinfo=UTC)
    for level in (1, 2):
        action_id = f"fast-aggressive-{level}"
        activation = f"{level}" * 64
        expires_at = observed + timedelta(seconds=600)
        await journal.issue_fast_live_preflight(activation, expires_at, now=observed)
        await journal.prepare(
            action_id,
            _ROUTE,
            f"fast-level-{level}",
            replace(
                _request(Venue.BINANCE_USDM, Side.BUY, f"fast-long-{level}"),
                client_order_id=venue_client_order_id(action_id, "long"),
            ),
            replace(
                _request(Venue.OKX, Side.SELL, f"fast-short-{level}"),
                client_order_id=venue_client_order_id(action_id, "short"),
            ),
            {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
            {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
            {
                "strategy": "AGGRESSIVE_FAST_LIVE_V2",
                "stage": "pilot_a",
                "level_index": level,
                "aggressive_binding_sha256": "c" * 64,
                "strategy_profile_sha256": "d" * 64,
                "projected_stress_usdt": "0.8",
                "activation_hash": activation,
                "fast_live_preflight_expires_at": expires_at.isoformat(),
                "consumption_data_generation_sha256": "a" * 64,
            },
            "0" * 64,
            observed,
            activation_hash=activation,
            fast_live_preflight_sha256=activation,
            fast_live_preflight_expires_at=expires_at,
        )
    calls = 0

    async def recover(_: LiveJournalAction) -> object:
        nonlocal calls
        calls += 1
        for action in await journal.active_actions():
            await _force_exchange_verified_flat(journal, action)
        return object()

    health = await LiveSafetySupervisor(journal, recover).reconcile_once()

    assert calls == 1
    assert health.mode == SupervisorMode.IDLE, health


@pytest.mark.asyncio
async def test_v2_recovery_rejects_supervisor_without_exact_runtime_handshake(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = LiveOrderJournal(tmp_path / "runtime-mismatch.sqlite3")
    await journal.initialise()
    activation = "e" * 64
    observed = datetime.now(UTC)
    await journal.issue_fast_live_preflight(
        activation, observed + timedelta(seconds=600), now=observed
    )
    action = await journal.prepare(
        "runtime-mismatch",
        _ROUTE,
        "fast-level-1",
        replace(
            _request(Venue.BINANCE_USDM, Side.BUY, "runtime-long"),
            client_order_id=venue_client_order_id("runtime-mismatch", "long"),
        ),
        replace(
            _request(Venue.OKX, Side.SELL, "runtime-short"),
            client_order_id=venue_client_order_id("runtime-mismatch", "short"),
        ),
        {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
        {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
        {
            "strategy": "AGGRESSIVE_FAST_LIVE_V2",
            "stage": "canary",
            "level_index": 1,
            "aggressive_binding_sha256": "c" * 64,
            "strategy_profile_sha256": "d" * 64,
            "projected_stress_usdt": "0.8",
            "activation_hash": activation,
            "fast_live_preflight_expires_at": (observed + timedelta(seconds=600)).isoformat(),
            "consumption_data_generation_sha256": "f" * 64,
            "supervisor_intent": "LIVE_CANARY",
            "supervisor_queued": True,
            "opening_client_order_ids": {
                "long": venue_client_order_id("runtime-mismatch", "long"),
                "short": venue_client_order_id("runtime-mismatch", "short"),
            },
            "fast_live_identity": {
                "release_sha": "a" * 40,
                "source_sha256": "b" * 64,
                "config_sha256": "c" * 64,
                "native_runtime_sha256": "d" * 64,
            },
        },
        activation,
        observed,
        activation_hash=activation,
        fast_live_preflight_sha256=activation,
        fast_live_preflight_expires_at=observed + timedelta(seconds=600),
    )
    for name in (
        "IPEG_FAST_LIVE_SUPERVISOR_RELEASE_SHA",
        "IPEG_FAST_LIVE_SUPERVISOR_SOURCE_SHA256",
        "IPEG_FAST_LIVE_SUPERVISOR_CONFIG_SHA256",
        "IPEG_FAST_LIVE_SUPERVISOR_NATIVE_RUNTIME_SHA256",
    ):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(RuntimeError, match="supervisor runtime identity"):
        await recover_active_canary(object(), journal, action)  # type: ignore[arg-type]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exposed_state",
    [LiveActionState.PARTIAL, LiveActionState.HEDGED, LiveActionState.RECOVERING],
)
async def test_v2_exposed_recovery_ignores_runtime_drift_and_reaches_flat(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    exposed_state: LiveActionState,
) -> None:
    journal = LiveOrderJournal(tmp_path / f"runtime-drift-{exposed_state.value}.sqlite3")
    await journal.initialise()
    activation = "9" * 64
    observed = datetime.now(UTC)
    expires_at = observed + timedelta(seconds=600)
    await journal.issue_fast_live_preflight(activation, expires_at, now=observed)
    action = await journal.prepare(
        f"runtime-drift-{exposed_state.value.lower()}",
        _ROUTE,
        "fast-level-1",
        replace(
            _request(Venue.BINANCE_USDM, Side.BUY, "drift-long"),
            client_order_id=venue_client_order_id("runtime-drift", "long"),
        ),
        replace(
            _request(Venue.OKX, Side.SELL, "drift-short"),
            client_order_id=venue_client_order_id("runtime-drift", "short"),
        ),
        {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
        {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
        {
            "strategy": "AGGRESSIVE_FAST_LIVE_V2",
            "stage": "canary",
            "level_index": 1,
            "aggressive_binding_sha256": "c" * 64,
            "strategy_profile_sha256": "d" * 64,
            "projected_stress_usdt": "0.8",
            "activation_hash": activation,
            "fast_live_preflight_expires_at": expires_at.isoformat(),
            "consumption_data_generation_sha256": "8" * 64,
            "fast_live_identity": {
                "release_sha": "a" * 40,
                "source_sha256": "b" * 64,
                "config_sha256": "c" * 64,
                "native_runtime_sha256": "d" * 64,
            },
        },
        activation,
        observed,
        activation_hash=activation,
        fast_live_preflight_sha256=activation,
        fast_live_preflight_expires_at=expires_at,
    )
    await journal.mark_submit_attempted(
        action.pair_action_id,
        tuple(leg.client_order_id for leg in action.legs),
    )
    action = await journal.transition(action.pair_action_id, LiveActionState.PARTIAL)
    if exposed_state == LiveActionState.HEDGED:
        action = await journal.transition(action.pair_action_id, LiveActionState.HEDGED)
    elif exposed_state == LiveActionState.RECOVERING:
        action = await journal.transition(action.pair_action_id, LiveActionState.RECOVERING)
    for name in (
        "IPEG_FAST_LIVE_SUPERVISOR_RELEASE_SHA",
        "IPEG_FAST_LIVE_SUPERVISOR_SOURCE_SHA256",
        "IPEG_FAST_LIVE_SUPERVISOR_CONFIG_SHA256",
        "IPEG_FAST_LIVE_SUPERVISOR_NATIVE_RUNTIME_SHA256",
    ):
        monkeypatch.setenv(name, "0" * 64)

    async def recover_to_flat(
        _settings: object,
        current_journal: LiveOrderJournal,
        active: LiveJournalAction,
        **_kwargs: object,
    ) -> object:
        await _force_exchange_verified_flat(current_journal, active)
        return object()

    monkeypatch.setattr(canary_runtime_module, "_resume_active_canary", recover_to_flat)
    await recover_active_canary(object(), journal, action)  # type: ignore[arg-type]

    recovered = await journal.load(action.pair_action_id)
    assert recovered is not None and recovered.state == LiveActionState.FLAT


@pytest.mark.asyncio
async def test_v2_portfolio_runtime_drift_recovers_exposure_but_never_opens_prepared(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal = LiveOrderJournal(tmp_path / "portfolio-runtime-drift.sqlite3")
    await journal.initialise()
    observed = datetime.now(UTC)
    prepared: list[LiveJournalAction] = []
    for level in (1, 2):
        action_id = f"portfolio-runtime-drift-{level}"
        activation = f"{level}" * 64
        expires_at = observed + timedelta(seconds=600)
        await journal.issue_fast_live_preflight(activation, expires_at, now=observed)
        long_id = venue_client_order_id(action_id, "long")
        short_id = venue_client_order_id(action_id, "short")
        prepared.append(
            await journal.prepare(
                action_id,
                _ROUTE,
                f"fast-level-{level}",
                replace(
                    _request(Venue.BINANCE_USDM, Side.BUY, f"portfolio-long-{level}"),
                    client_order_id=long_id,
                ),
                replace(
                    _request(Venue.OKX, Side.SELL, f"portfolio-short-{level}"),
                    client_order_id=short_id,
                ),
                {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
                {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
                {
                    "strategy": "AGGRESSIVE_FAST_LIVE_V2",
                    "stage": "pilot_a",
                    "level_index": level,
                    "aggressive_binding_sha256": "c" * 64,
                    "strategy_profile_sha256": "d" * 64,
                    "projected_stress_usdt": "0.8",
                    "activation_hash": activation,
                    "fast_live_preflight_expires_at": expires_at.isoformat(),
                    "consumption_data_generation_sha256": "8" * 64,
                    "supervisor_intent": "LIVE_CANARY",
                    "supervisor_queued": True,
                    "opening_client_order_ids": {"long": long_id, "short": short_id},
                    "fast_live_identity": {
                        "release_sha": "a" * 40,
                        "source_sha256": "b" * 64,
                        "config_sha256": "c" * 64,
                        "native_runtime_sha256": "d" * 64,
                    },
                },
                activation,
                observed,
                activation_hash=activation,
                fast_live_preflight_sha256=activation,
                fast_live_preflight_expires_at=expires_at,
            )
        )
    await journal.mark_submit_attempted(
        prepared[0].pair_action_id,
        tuple(leg.client_order_id for leg in prepared[0].legs),
    )
    first = await journal.transition(prepared[0].pair_action_id, LiveActionState.PARTIAL)
    first = await journal.transition(first.pair_action_id, LiveActionState.HEDGED)
    second = prepared[1]
    for name in (
        "IPEG_FAST_LIVE_SUPERVISOR_RELEASE_SHA",
        "IPEG_FAST_LIVE_SUPERVISOR_SOURCE_SHA256",
        "IPEG_FAST_LIVE_SUPERVISOR_CONFIG_SHA256",
        "IPEG_FAST_LIVE_SUPERVISOR_NATIVE_RUNTIME_SHA256",
    ):
        monkeypatch.delenv(name, raising=False)
    resumed: list[str] = []

    async def resume_exposed(
        _settings: object,
        current_journal: LiveOrderJournal,
        active: LiveJournalAction,
        **_kwargs: object,
    ) -> object:
        resumed.append(active.pair_action_id)
        await _force_exchange_verified_flat(current_journal, active)
        return SimpleNamespace(success=True, terminal_state=LiveActionState.FLAT)

    emergency_called = False

    class FakeControlPlane:
        def __init__(self, _settings: object) -> None:
            pass

        async def emergency_flatten(self) -> object:
            nonlocal emergency_called
            emergency_called = True
            for active in await journal.active_actions():
                await _force_exchange_verified_flat(journal, active)
            return SimpleNamespace(success=True, instruction=None)

    monkeypatch.setattr(canary_runtime_module, "_resume_active_canary", resume_exposed)
    monkeypatch.setattr(canary_runtime_module, "OnDemandLiveControlPlane", FakeControlPlane)

    await recover_active_actions(object(), journal, (first, second))  # type: ignore[arg-type]

    assert resumed == [first.pair_action_id]
    assert emergency_called is True
    assert await journal.active_actions() == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("use_scheduler", [False, True])
async def test_new_aggressive_tranche_replaces_single_tranche_recovery_owner(
    tmp_path: Path,
    use_scheduler: bool,
) -> None:
    journal = LiveOrderJournal(tmp_path / "portfolio-growth.sqlite3")
    await journal.initialise()
    first_id = "supervisor-action-1"
    first = await journal.prepare(
        first_id,
        _ROUTE,
        "tranche-1",
        replace(
            _request(Venue.BINANCE_USDM, Side.BUY, "long-1"),
            client_order_id=venue_client_order_id(first_id, "long"),
        ),
        replace(
            _request(Venue.OKX, Side.SELL, "short-1"),
            client_order_id=venue_client_order_id(first_id, "short"),
        ),
        {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
        {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
        {
            "strategy": "AGGRESSIVE_SYMBIOSIS_V1",
            "stage": "pilot_a",
            "level_index": 1,
            "aggressive_binding_sha256": "c" * 64,
            "strategy_profile_sha256": "d" * 64,
            "projected_stress_usdt": "0.8",
        },
        "b" * 64,
    )
    first_started = asyncio.Event()
    first_cancelled = asyncio.Event()
    first_exited = asyncio.Event()
    calls: list[str] = []

    async def recover(action: LiveJournalAction) -> object:
        calls.append(action.pair_action_id)
        if len(calls) == 1:
            first_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                first_cancelled.set()
                raise
            finally:
                first_exited.set()
        assert first_exited.is_set()
        for current in await journal.active_actions():
            await _force_exchange_verified_flat(journal, current)
        return object()

    scheduler = PriorityWorkScheduler(pending_limit=16, worker_count=6) if use_scheduler else None
    supervisor = LiveSafetySupervisor(
        journal,
        recover,
        recovery_timeout_seconds=0.02,
        priority_scheduler=scheduler,
    )
    first_health = await supervisor.reconcile_once()
    await asyncio.wait_for(first_started.wait(), timeout=1)
    assert first_health.failure == f"{first.pair_action_id}:TimeoutError"
    supervisor._recovery_timeout_seconds = 1

    second_id = "supervisor-action-2"
    await journal.prepare(
        second_id,
        _ROUTE,
        "tranche-2",
        replace(
            _request(Venue.BINANCE_USDM, Side.BUY, "long-2"),
            client_order_id=venue_client_order_id(second_id, "long"),
        ),
        replace(
            _request(Venue.OKX, Side.SELL, "short-2"),
            client_order_id=venue_client_order_id(second_id, "short"),
        ),
        {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
        {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
        {
            "strategy": "AGGRESSIVE_SYMBIOSIS_V1",
            "stage": "pilot_a",
            "level_index": 2,
            "aggressive_binding_sha256": "c" * 64,
            "strategy_profile_sha256": "d" * 64,
            "projected_stress_usdt": "0.8",
        },
        "b" * 64,
    )
    health = await supervisor.reconcile_once()

    assert first_cancelled.is_set()
    assert calls == [first.pair_action_id, first.pair_action_id]
    assert health.mode == SupervisorMode.IDLE
    assert await journal.active_actions() == ()
    if scheduler is not None:
        await scheduler.close()


@pytest.mark.asyncio
async def test_supervisor_stop_health_is_fail_closed_with_multiple_actions(tmp_path: Path) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await _prepare_named(journal, "action-btc", "BTC")
    await _prepare_named(journal, "action-eth", "ETH")

    async def should_not_run(_action: LiveJournalAction) -> object:
        raise AssertionError("pre-stopped supervisor must not begin recovery")

    supervisor = LiveSafetySupervisor(journal, should_not_run)
    stop = asyncio.Event()
    stop.set()
    await supervisor.run(stop)
    health = await supervisor.health()

    assert health.mode == SupervisorMode.STOPPED
    assert health.recovery_required is True
    assert health.active_pair_action_id == "action-btc"
    assert health.action_state is None
    assert health.active_action_count == 2


@pytest.mark.asyncio
async def test_hung_route_recovery_is_single_flight_while_other_route_reaches_flat(
    tmp_path: Path,
) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    hung = await _prepare_named(journal, "action-btc", "BTC")
    await _prepare_named(journal, "action-eth", "ETH")
    hung_started = asyncio.Event()
    hung_release = asyncio.Event()
    hung_calls = 0

    async def recover(action: LiveJournalAction) -> object:
        nonlocal hung_calls
        if action.pair_action_id == hung.pair_action_id:
            hung_calls += 1
            hung_started.set()
            await hung_release.wait()
            return object()
        await _force_exchange_verified_flat(journal, action)
        return object()

    supervisor = LiveSafetySupervisor(journal, recover, recovery_timeout_seconds=0.02)
    first = await supervisor.reconcile_once()
    await asyncio.wait_for(hung_started.wait(), timeout=1)
    second = await supervisor.reconcile_once()

    assert first.mode == second.mode == SupervisorMode.BLOCKED
    assert first.failure is not None and "action-btc:TimeoutError" in first.failure
    assert second.failure == "action-btc:TimeoutError"
    assert hung_calls == 1
    assert tuple(action.pair_action_id for action in await journal.active_actions()) == (
        hung.pair_action_id,
    )

    hung_release.set()
    await asyncio.sleep(0)
    third = await supervisor.reconcile_once()
    assert third.mode == SupervisorMode.BLOCKED
    assert third.failure is None
    assert hung_calls == 1


@pytest.mark.asyncio
async def test_supervisor_shutdown_reports_cancellation_resistant_recovery(
    tmp_path: Path,
) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await _prepare_named(journal, "action-btc", "BTC")
    started = asyncio.Event()
    release = asyncio.Event()

    async def resistant(_action: LiveJournalAction) -> object:
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            await release.wait()
        return object()

    supervisor = LiveSafetySupervisor(journal, resistant, recovery_timeout_seconds=0.02)
    health = await supervisor.reconcile_once()
    assert health.failure == "action-btc:TimeoutError"
    await asyncio.wait_for(started.wait(), timeout=1)
    stop = asyncio.Event()
    stop.set()

    with pytest.raises(RuntimeError, match="recovery tasks remain active"):
        await supervisor.run(stop)
    stopped = await supervisor.health()
    assert stopped.mode == SupervisorMode.STOPPED
    assert stopped.failure == "RecoveryShutdownTimeout"

    release.set()
    await asyncio.wait_for(
        asyncio.gather(*supervisor._recovery_tasks.values()),
        timeout=1,
    )


@pytest.mark.asyncio
async def test_priority_scheduler_recovers_ten_actions_while_p4_p6_are_shed(
    tmp_path: Path,
) -> None:
    journal = LiveOrderJournal(tmp_path / "priority-overload.sqlite3")
    actions = tuple(
        [
            await _prepare_named(journal, f"action-{index:02d}", f"B{index:02d}")
            for index in range(10)
        ]
    )
    scheduler = PriorityWorkScheduler(pending_limit=16, worker_count=6)
    low_release = asyncio.Event()
    low_started = [asyncio.Event() for _ in range(2)]

    async def held_low(index: int) -> None:
        low_started[index].set()
        await low_release.wait()

    low_tasks = tuple(
        asyncio.create_task(
            scheduler.run(
                WorkPriority.BROAD_BBO_HISTORY,
                f"broad-{index}",
                partial(held_low, index),
            )
        )
        for index in range(2)
    )
    await asyncio.gather(*(asyncio.wait_for(started.wait(), timeout=1) for started in low_started))

    first_started = asyncio.Event()
    release_first = asyncio.Event()

    async def recover(action: LiveJournalAction) -> object:
        assert action.pair_action_id in {item.pair_action_id for item in actions}
        first_started.set()
        await release_first.wait()
        for current in await journal.active_actions():
            await _force_exchange_verified_flat(journal, current)
        return object()

    supervisor = LiveSafetySupervisor(
        journal,
        recover,
        recovery_timeout_seconds=2,
        priority_scheduler=scheduler,
    )
    reconciliation = asyncio.create_task(supervisor.reconcile_once())
    await asyncio.wait_for(first_started.wait(), timeout=1)
    assert scheduler.snapshot().critical_work_count == 1
    with pytest.raises(WorkRejected, match="critical work"):
        await scheduler.run(
            WorkPriority.NEW_ENTRY,
            "new-entry",
            lambda: asyncio.sleep(0),
        )

    release_first.set()
    health = await asyncio.wait_for(reconciliation, timeout=2)
    assert health.mode == SupervisorMode.IDLE
    assert health.active_action_count == 0
    assert await journal.active_actions() == ()
    assert all(not task.done() for task in low_tasks)

    low_release.set()
    await asyncio.gather(*low_tasks)
    await scheduler.close()


@pytest.mark.asyncio
async def test_prepared_live_canary_is_scheduled_as_p4_new_entry(tmp_path: Path) -> None:
    journal = LiveOrderJournal(tmp_path / "prepared-entry.sqlite3")
    action = await _prepare(journal)
    scheduler = PriorityWorkScheduler(pending_limit=16, worker_count=6)
    started = asyncio.Event()
    release = asyncio.Event()

    async def recover(current: LiveJournalAction) -> object:
        assert current.pair_action_id == action.pair_action_id
        started.set()
        await release.wait()
        await _force_exchange_verified_flat(journal, current)
        return object()

    supervisor = LiveSafetySupervisor(
        journal,
        recover,
        recovery_timeout_seconds=2,
        priority_scheduler=scheduler,
    )
    reconciliation = asyncio.create_task(supervisor.reconcile_once())
    await asyncio.wait_for(started.wait(), timeout=1)
    snapshot = scheduler.snapshot()
    assert snapshot.running_by_priority[int(WorkPriority.NEW_ENTRY)] == 1
    assert snapshot.critical_work_count == 0

    release.set()
    assert (await reconciliation).mode == SupervisorMode.IDLE
    await scheduler.close()
