from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, replace
from datetime import UTC, datetime
from decimal import Decimal
from functools import partial
from pathlib import Path
from typing import TypedDict, cast

from interexchange_perp_grid.client_ids import venue_client_order_id
from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.live_control import LiveControlResult, LiveControlService
from interexchange_perp_grid.live_journal import (
    LiveActionState,
    LiveJournalAction,
    LiveOrderJournal,
)
from interexchange_perp_grid.live_reconciliation import shutdown_private_requests
from interexchange_perp_grid.priority_scheduler import (
    PriorityWorkScheduler,
    WorkPriority,
    WorkRejected,
)
from interexchange_perp_grid.private_domain import (
    AccountSnapshot,
    PositionSnapshot,
    PrivateActiveSnapshot,
    PrivateOrder,
    PrivateOrderStatus,
    SnapshotCompleteness,
    VenueOrderRequest,
)
from interexchange_perp_grid.strategy import DirectedRouteKey
from interexchange_perp_grid.supervisor import LiveSafetySupervisor, SupervisorMode
from interexchange_perp_grid.supervisor_smoke import RecoverySmokeTransition


class _PrivateStatePayload(TypedDict):
    positions: dict[str, str]
    orders: dict[str, dict[str, object]]
    submit_calls: int


class PersistentPrivateSmokeExchange:
    """Account-wide file-backed private transport used only by process-kill proof."""

    def __init__(
        self,
        venue: Venue,
        instruments: tuple[Instrument, ...],
        state_path: Path,
    ) -> None:
        self.venue = venue
        self._instruments = {instrument.symbol: instrument for instrument in instruments}
        self._state_path = state_path
        self._write_lock = asyncio.Lock()

    def initialise(self, *, reset: bool) -> None:
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        if reset or not self._state_path.exists():
            self._write({"positions": {}, "orders": {}, "submit_calls": 0})

    def seed_position(self, symbol: str, signed_quantity: Decimal) -> None:
        payload = self._read()
        positions = dict(payload["positions"])
        positions[symbol] = str(signed_quantity)
        payload["positions"] = positions
        self._write(payload)

    def seed_order(self, order: PrivateOrder) -> None:
        payload = self._read()
        orders = dict(payload["orders"])
        orders[order.client_order_id] = cast(dict[str, object], asdict(order))
        payload["orders"] = orders
        self._write(payload)

    @property
    def submit_calls(self) -> int:
        return self._read()["submit_calls"]

    async def submit_order(
        self,
        request: VenueOrderRequest,
        instrument: Instrument,
    ) -> PrivateOrder:
        async with self._write_lock:
            if request.venue != self.venue or self._instruments.get(request.symbol) != instrument:
                raise ValueError("private smoke request does not match venue instrument")
            payload = self._read()
            positions = dict(payload["positions"])
            signed = Decimal(str(positions.get(request.symbol, "0")))
            quantity = request.amount_contracts * instrument.contract_size_base
            signed_fill = quantity if request.side == Side.BUY else -quantity
            if signed == 0 or abs(signed_fill) > abs(signed) or signed * signed_fill >= 0:
                raise RuntimeError("private smoke refuses a non-reducing emergency order")
            remaining = signed + signed_fill
            if remaining == 0:
                positions.pop(request.symbol, None)
            else:
                positions[request.symbol] = str(remaining)
            submit_calls = payload["submit_calls"] + 1
            order = PrivateOrder(
                self.venue,
                f"private-smoke-{submit_calls}",
                request.client_order_id,
                request.symbol,
                request.side,
                PrivateOrderStatus.FILLED,
                quantity,
                quantity,
                Decimal("100"),
                Decimal("0.01"),
                datetime.now(UTC),
                request.price,
            )
            orders = dict(payload["orders"])
            orders[order.client_order_id] = cast(dict[str, object], asdict(order))
            self._write(
                {
                    "positions": positions,
                    "orders": orders,
                    "submit_calls": submit_calls,
                }
            )
            return order

    async def fetch_account(self, instrument: Instrument) -> AccountSnapshot:
        self._require_instrument(instrument)
        return AccountSnapshot(
            self.venue,
            Decimal("500"),
            Decimal("400"),
            "cross",
            "oneway",
            True,
            ("read", "trade"),
            datetime.now(UTC),
            False,
            False,
        )

    async def fetch_active_snapshot(self) -> PrivateActiveSnapshot:
        payload = self._read()
        positions = tuple(
            PositionSnapshot(
                self.venue,
                symbol,
                Side.BUY if Decimal(str(signed)) > 0 else Side.SELL,
                abs(Decimal(str(signed))),
                Decimal("100"),
                Decimal("100"),
                datetime.now(UTC),
            )
            for symbol, signed in sorted(dict(payload["positions"]).items())
        )
        return PrivateActiveSnapshot(
            self.venue,
            0,
            len(positions),
            (),
            positions,
            (),
            SnapshotCompleteness.COMPLETE,
            datetime.now(UTC),
            account_wide=True,
        )

    async def fetch_all_open_orders(self) -> tuple[PrivateOrder, ...]:
        return ()

    async def fetch_all_positions(self) -> tuple[PositionSnapshot, ...]:
        return (await self.fetch_active_snapshot()).positions

    async def fetch_closed_orders(self, instrument: Instrument) -> tuple[PrivateOrder, ...]:
        self._require_instrument(instrument)
        return tuple(order for order in self._orders() if order.symbol == instrument.symbol)

    async def fetch_trading_fee(self, instrument: Instrument) -> Decimal:
        self._require_instrument(instrument)
        return Decimal("0.0005")

    async def watch_orders(self, instrument: Instrument) -> tuple[PrivateOrder, ...]:
        return await self.fetch_closed_orders(instrument)

    async def find_order_by_client_id(
        self,
        client_order_id: str,
        instrument: Instrument,
    ) -> PrivateOrder | None:
        self._require_instrument(instrument)
        return next(
            (order for order in self._orders() if order.client_order_id == client_order_id),
            None,
        )

    async def cancel_order(self, order_id: str, instrument: Instrument) -> PrivateOrder:
        del order_id, instrument
        raise KeyError("private smoke has no open orders")

    async def resolve_instrument(self, symbol: str) -> Instrument | None:
        return self._instruments.get(symbol)

    async def list_instruments(self) -> tuple[Instrument, ...]:
        return tuple(self._instruments.values())

    def _orders(self) -> tuple[PrivateOrder, ...]:
        return tuple(
            _order_from_payload(payload) for payload in dict(self._read()["orders"]).values()
        )

    def _require_instrument(self, instrument: Instrument) -> None:
        if self._instruments.get(instrument.symbol) != instrument:
            raise ValueError("private smoke instrument is unknown")

    def _read(self) -> _PrivateStatePayload:
        return cast(
            _PrivateStatePayload,
            json.loads(self._state_path.read_text(encoding="utf-8")),
        )

    def _write(self, payload: _PrivateStatePayload) -> None:
        temporary = self._state_path.with_suffix(self._state_path.suffix + ".pending")
        temporary.write_text(
            json.dumps(payload, default=str, sort_keys=True),
            encoding="utf-8",
        )
        temporary.replace(self._state_path)


async def run_private_transition_recovery_smoke(
    state_path: Path,
    private_state_dir: Path,
    *,
    hold_after_active: bool,
    transition_state: RecoverySmokeTransition,
    ready_path: Path | None = None,
    action_count: int = 10,
) -> dict[str, object]:
    if not 1 <= action_count <= 10:
        raise ValueError("private transition smoke action count must be between 1 and 10")
    state = LiveActionState(transition_state.value)
    journal = LiveOrderJournal(state_path)
    await journal.initialise()
    expected_ids = tuple(f"private-transition-{index:02d}" for index in range(action_count))
    existing = tuple(
        action
        for action in await asyncio.gather(*(journal.load(action_id) for action_id in expected_ids))
        if action is not None
    )
    instruments = {
        (venue, instrument.symbol): instrument
        for venue in Venue
        for instrument in (_instrument(venue, index) for index in range(action_count))
    }
    adapters = {
        venue: PersistentPrivateSmokeExchange(
            venue,
            tuple(instruments[(venue, f"A{index:03d}/USDT:USDT")] for index in range(action_count)),
            private_state_dir / f"{venue.value}.json",
        )
        for venue in Venue
    }
    if hold_after_active and not existing:
        for adapter in adapters.values():
            adapter.initialise(reset=True)
        existing = tuple(
            await asyncio.gather(
                *(_seed_action(journal, adapters, index, state) for index in range(action_count))
            )
        )
    else:
        for adapter in adapters.values():
            adapter.initialise(reset=False)
    _validate_transition_set(existing, expected_ids, state)
    if hold_after_active:
        if ready_path is not None:
            ready_path.write_text(
                "".join(f"{action.pair_action_id}:{action.state.value}\n" for action in existing),
                encoding="utf-8",
            )
        await asyncio.Event().wait()
        raise RuntimeError("unreachable")

    service = LiveControlService(journal, adapters, instruments)
    scheduler = PriorityWorkScheduler(pending_limit=16, worker_count=6)
    low_release = asyncio.Event()
    low_started = (asyncio.Event(), asyncio.Event())

    async def held_lower_work(index: int) -> None:
        low_started[index].set()
        await low_release.wait()

    lower_tasks = tuple(
        asyncio.create_task(
            scheduler.run(
                WorkPriority.BROAD_BBO_HISTORY,
                f"restart-broad-{index}",
                partial(held_lower_work, index),
            )
        )
        for index in range(2)
    )
    await asyncio.gather(*(asyncio.wait_for(event.wait(), timeout=1) for event in low_started))
    recovery_started = asyncio.Event()
    recovery_release = asyncio.Event()
    control_result: LiveControlResult | None = None
    dispatch_lock = asyncio.Lock()

    async def priority_recovery(_action: LiveJournalAction) -> object:
        nonlocal control_result
        async with dispatch_lock:
            current = await journal.active_actions()
            if not current:
                return object()
            recovery_started.set()
            await recovery_release.wait()
            control_result = await service.emergency_flatten()
            return control_result

    supervisor = LiveSafetySupervisor(
        journal,
        priority_recovery,
        recovery_timeout_seconds=30,
        priority_scheduler=scheduler,
    )
    recovery_task = asyncio.create_task(supervisor.reconcile_once())
    await asyncio.wait_for(recovery_started.wait(), timeout=1)
    entry_shed = False
    try:
        try:
            await scheduler.run(
                WorkPriority.NEW_ENTRY,
                "restart-new-entry",
                _unexpected_entry,
            )
        except WorkRejected:
            entry_shed = True
        recovery_release.set()
        health = await recovery_task
        if health.mode != SupervisorMode.IDLE or control_result is None:
            raise RuntimeError("priority supervisor did not complete account recovery")
        result = control_result
    finally:
        recovery_release.set()
        low_release.set()
        await asyncio.gather(*lower_tasks, return_exceptions=True)
        await scheduler.close()
        await shutdown_private_requests(adapters)
    snapshots = await asyncio.gather(
        *(adapter.fetch_active_snapshot() for adapter in adapters.values())
    )
    if not result.success or not result.flat_barrier_verified:
        raise RuntimeError(f"private transition recovery failed: {state.value}:{result.reason}")
    if await journal.active_actions() or any(snapshot.positions for snapshot in snapshots):
        raise RuntimeError("private transition recovery did not reach exchange-verified FLAT")
    return {
        "status": "PASS",
        "process_restart_recovery": True,
        "restarted_transition_state": state.value,
        "expected_action_count": len(existing),
        "recovered_action_count": len(existing),
        "simulated_private_submit_calls": sum(
            adapter.submit_calls for adapter in adapters.values()
        ),
        "production_exchange_transports_opened": 0,
        "flat_barrier_verified": result.flat_barrier_verified,
        "priority_scheduler_restart_proof": True,
        "new_entry_shed_while_p0_active": entry_shed,
    }


async def _unexpected_entry() -> None:
    raise RuntimeError("new entry ran while emergency recovery was active")


async def _seed_action(
    journal: LiveOrderJournal,
    adapters: dict[Venue, PersistentPrivateSmokeExchange],
    index: int,
    state: LiveActionState,
) -> LiveJournalAction:
    base = f"A{index:03d}"
    symbol = f"{base}/USDT:USDT"
    action_id = f"private-transition-{index:02d}"
    long_request = _request(Venue.BINANCE_USDM, Side.BUY, action_id, "long", symbol)
    short_request = _request(Venue.OKX, Side.SELL, action_id, "short", symbol)
    action = await journal.prepare(
        action_id,
        DirectedRouteKey(base, Venue.BINANCE_USDM, Venue.OKX),
        f"private-transition-{index:02d}",
        long_request,
        short_request,
        {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
        {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
        {"projected_stress_usdt": "5", "production_submit_calls": 0},
        "0" * 64,
    )
    if state == LiveActionState.PREPARED:
        return action
    await journal.mark_submit_attempted(
        action_id,
        (long_request.client_order_id, short_request.client_order_id),
    )
    if state == LiveActionState.SUBMITTING:
        for request in (long_request, short_request):
            adapters[request.venue].seed_order(_rejected_order(request))
        loaded = await journal.load(action_id)
        if loaded is None:
            raise RuntimeError("private transition action disappeared")
        return loaded

    if state == LiveActionState.PARTIAL:
        await _seed_filled(journal, adapters, action_id, long_request)
        rejected = _rejected_order(short_request)
        adapters[short_request.venue].seed_order(rejected)
        await journal.record_order_event(action_id, rejected, f"seed:{rejected.client_order_id}")
    elif state in {
        LiveActionState.FILLED,
        LiveActionState.RECOVERING,
        LiveActionState.HEDGED,
        LiveActionState.CLOSING,
        LiveActionState.QUARANTINED,
    }:
        await _seed_filled(journal, adapters, action_id, long_request)
        await _seed_filled(journal, adapters, action_id, short_request)
    elif state in {
        LiveActionState.ACKNOWLEDGED,
        LiveActionState.REJECTED,
        LiveActionState.UNKNOWN,
    }:
        for request in (long_request, short_request):
            rejected = _rejected_order(request)
            adapters[request.venue].seed_order(rejected)
            if state in {LiveActionState.REJECTED, LiveActionState.UNKNOWN}:
                observed = (
                    rejected
                    if state == LiveActionState.REJECTED
                    else replace(rejected, status=PrivateOrderStatus.UNKNOWN)
                )
                await journal.record_order_event(
                    action_id,
                    observed,
                    f"seed:{observed.client_order_id}",
                )

    if state == LiveActionState.HEDGED:
        action = await journal.transition(action_id, LiveActionState.FILLED)
        return await journal.transition(action.pair_action_id, LiveActionState.HEDGED)
    if state == LiveActionState.CLOSING:
        action = await journal.transition(action_id, LiveActionState.FILLED)
        action = await journal.transition(action.pair_action_id, LiveActionState.HEDGED)
        return await journal.transition(action.pair_action_id, LiveActionState.CLOSING)
    return await journal.transition(action_id, state)


async def _seed_filled(
    journal: LiveOrderJournal,
    adapters: dict[Venue, PersistentPrivateSmokeExchange],
    action_id: str,
    request: VenueOrderRequest,
) -> None:
    order = PrivateOrder(
        request.venue,
        f"seed-{request.client_order_id}",
        request.client_order_id,
        request.symbol,
        request.side,
        PrivateOrderStatus.FILLED,
        Decimal("0.001"),
        Decimal("0.001"),
        Decimal("100"),
        Decimal("0.00005"),
        datetime.now(UTC),
        request.price,
    )
    adapters[request.venue].seed_order(order)
    adapters[request.venue].seed_position(
        request.symbol,
        Decimal("0.001") if request.side == Side.BUY else Decimal("-0.001"),
    )
    await journal.record_order_event(action_id, order, f"seed:{order.client_order_id}")


def _validate_transition_set(
    actions: tuple[LiveJournalAction, ...],
    expected_ids: tuple[str, ...],
    expected_state: LiveActionState,
) -> None:
    if tuple(action.pair_action_id for action in actions) != expected_ids:
        raise RuntimeError("private transition durable action set is incomplete")
    if any(action.state != expected_state for action in actions):
        raise RuntimeError(
            "private transition durable state does not match requested restart state"
        )


def _instrument(venue: Venue, index: int) -> Instrument:
    base = f"A{index:03d}"
    return Instrument(
        venue,
        f"{base}/USDT:USDT",
        f"{base}USDT",
        base,
        "USDT",
        "USDT",
        Decimal("0.001"),
        Decimal("1"),
        Decimal("0.1"),
        Decimal("1"),
        Decimal("0.01"),
        Decimal("0.0005"),
        "private-transition-smoke",
    )


def _request(
    venue: Venue,
    side: Side,
    action_id: str,
    role: str,
    symbol: str,
) -> VenueOrderRequest:
    return VenueOrderRequest(
        venue,
        venue_client_order_id(action_id, role),
        symbol,
        side,
        "limit",
        Decimal("1"),
        Decimal("100"),
        "IOC",
        {"timeInForce": "IOC"},
    )


def _rejected_order(request: VenueOrderRequest) -> PrivateOrder:
    return PrivateOrder(
        request.venue,
        f"resolved-{request.client_order_id}",
        request.client_order_id,
        request.symbol,
        request.side,
        PrivateOrderStatus.REJECTED,
        Decimal("0.001"),
        Decimal(0),
        None,
        Decimal(0),
        datetime.now(UTC),
        request.price,
    )


def _order_from_payload(payload: object) -> PrivateOrder:
    if not isinstance(payload, dict):
        raise ValueError("private smoke order payload is malformed")
    return PrivateOrder(
        Venue(str(payload["venue"])),
        str(payload["order_id"]) if payload["order_id"] is not None else None,
        str(payload["client_order_id"]),
        str(payload["symbol"]),
        Side(str(payload["side"])),
        PrivateOrderStatus(str(payload["status"])),
        Decimal(str(payload["requested_base_quantity"])),
        Decimal(str(payload["filled_base_quantity"])),
        Decimal(str(payload["average_price"])) if payload["average_price"] is not None else None,
        Decimal(str(payload["fee_usdt"])),
        datetime.fromisoformat(str(payload["observed_at"])),
        Decimal(str(payload["limit_price"])) if payload["limit_price"] is not None else None,
    )
