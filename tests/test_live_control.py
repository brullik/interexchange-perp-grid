from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.client_ids import venue_client_order_id
from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.execution import ExecutionIntent, OrderPurpose, Side
from interexchange_perp_grid.live_control import (
    EMERGENCY_CONFIRMATION,
    LiveControlService,
    emergency_unlock_valid,
)
from interexchange_perp_grid.live_journal import LiveActionState, LiveOrderJournal
from interexchange_perp_grid.live_reconciliation import shutdown_private_requests
from interexchange_perp_grid.live_simulator import (
    DeterministicPrivateExchange,
    ScriptedOrderOutcome,
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
from interexchange_perp_grid.private_execution import translate_protected_order
from interexchange_perp_grid.reason_codes import ReasonCode
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
        Decimal("1"),
        Decimal("0.1"),
        Decimal("1"),
        Decimal("0.01"),
        Decimal("0.0005"),
        "private",
    )


def _eth_instrument(venue: Venue) -> Instrument:
    return Instrument(
        venue,
        "ETH/USDT:USDT",
        "ETHUSDT",
        "ETH",
        "USDT",
        "USDT",
        Decimal("0.001"),
        Decimal("1"),
        Decimal("0.01"),
        Decimal("1"),
        Decimal("1"),
        Decimal("0.0005"),
        "private",
    )


def _portfolio_instrument(venue: Venue, index: int) -> Instrument:
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
        Decimal("0.01"),
        Decimal("1"),
        Decimal("1"),
        Decimal("0.0005"),
        "private",
    )


class MultiSymbolEmergencyExchange(DeterministicPrivateExchange):
    def __init__(self, venue: Venue) -> None:
        super().__init__(venue, _instrument(venue), ())
        self.eth = _eth_instrument(venue)
        self._signed_position = Decimal("0.01")
        self.submitted_symbols: list[str] = []

    async def fetch_all_positions(self) -> tuple[PositionSnapshot, ...]:
        if self._signed_position == 0:
            return ()
        return (
            PositionSnapshot(
                self.venue,
                self.eth.symbol,
                Side.BUY,
                self._signed_position,
                Decimal("2000"),
                Decimal("2000"),
                datetime.now(UTC),
            ),
        )

    async def list_instruments(self) -> tuple[Instrument, ...]:
        return (self.instrument, self.eth)

    async def fetch_closed_orders(self, instrument: Instrument) -> tuple[PrivateOrder, ...]:
        if instrument not in {self.instrument, self.eth}:
            raise ValueError("simulator instrument mismatch")
        return tuple(order for order in self._recent.values() if order.symbol == instrument.symbol)

    async def resolve_instrument(self, symbol: str) -> Instrument | None:
        return next(
            (
                instrument
                for instrument in await self.list_instruments()
                if instrument.symbol == symbol
            ),
            None,
        )

    async def submit_order(
        self,
        request: VenueOrderRequest,
        instrument: Instrument,
    ) -> PrivateOrder:
        assert instrument == self.eth
        self.submitted_symbols.append(request.symbol)
        quantity = request.amount_contracts * instrument.contract_size_base
        self._signed_position = Decimal(0)
        self.submit_calls += 1
        order = PrivateOrder(
            venue=self.venue,
            order_id=f"emergency-{self.submit_calls}",
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            side=request.side,
            status=PrivateOrderStatus.FILLED,
            requested_base_quantity=quantity,
            filled_base_quantity=quantity,
            average_price=Decimal("2000"),
            fee_usdt=Decimal("0.01"),
            observed_at=datetime.now(UTC),
        )
        self._recent[request.client_order_id] = order
        return order


class PortfolioEmergencyExchange:
    """Account-wide deterministic adapter for multi-symbol production-control tests."""

    def __init__(self, venue: Venue, instruments: tuple[Instrument, ...]) -> None:
        self.venue = venue
        self._instruments = {instrument.symbol: instrument for instrument in instruments}
        self._positions: dict[str, Decimal] = {}
        self._recent: dict[str, PrivateOrder] = {}
        self.submit_calls = 0
        self.submitted_symbols: list[str] = []
        self.closed_order_symbols: list[str] = []
        self.rejected_symbols: set[str] = set()

    def seed_position(self, symbol: str, signed_quantity: Decimal) -> None:
        if symbol not in self._instruments or signed_quantity == 0:
            raise ValueError("portfolio simulator position must reference a known non-zero symbol")
        self._positions[symbol] = signed_quantity

    def seed_order(self, order: PrivateOrder) -> None:
        if order.venue != self.venue or order.symbol not in self._instruments:
            raise ValueError("portfolio simulator order must reference a known venue symbol")
        self._recent[order.client_order_id] = order

    async def submit_order(
        self,
        request: VenueOrderRequest,
        instrument: Instrument,
    ) -> PrivateOrder:
        if request.venue != self.venue or self._instruments.get(request.symbol) != instrument:
            raise ValueError("portfolio simulator request does not match venue instrument")
        if request.symbol in self.rejected_symbols:
            self.submit_calls += 1
            self.submitted_symbols.append(request.symbol)
            order = PrivateOrder(
                self.venue,
                f"portfolio-emergency-{self.submit_calls}",
                request.client_order_id,
                request.symbol,
                request.side,
                PrivateOrderStatus.REJECTED,
                request.amount_contracts * instrument.contract_size_base,
                Decimal(0),
                None,
                Decimal(0),
                datetime.now(UTC),
            )
            self._recent[request.client_order_id] = order
            return order
        signed = self._positions.get(request.symbol, Decimal(0))
        quantity = request.amount_contracts * instrument.contract_size_base
        signed_fill = quantity if request.side == Side.BUY else -quantity
        if signed == 0 or abs(signed_fill) > abs(signed) or signed * signed_fill >= 0:
            raise RuntimeError("portfolio simulator refuses a non-reducing emergency order")
        remaining = signed + signed_fill
        if remaining == 0:
            self._positions.pop(request.symbol)
        else:
            self._positions[request.symbol] = remaining
        self.submit_calls += 1
        self.submitted_symbols.append(request.symbol)
        order = PrivateOrder(
            venue=self.venue,
            order_id=f"portfolio-emergency-{self.submit_calls}",
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            side=request.side,
            status=PrivateOrderStatus.FILLED,
            requested_base_quantity=quantity,
            filled_base_quantity=quantity,
            average_price=Decimal("100"),
            fee_usdt=Decimal("0.01"),
            observed_at=datetime.now(UTC),
        )
        self._recent[request.client_order_id] = order
        return order

    async def fetch_account(self, instrument: Instrument) -> AccountSnapshot:
        if instrument.symbol not in self._instruments:
            raise ValueError("portfolio simulator instrument is unknown")
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
        positions = tuple(
            PositionSnapshot(
                self.venue,
                symbol,
                Side.BUY if signed > 0 else Side.SELL,
                abs(signed),
                Decimal("100"),
                Decimal("100"),
                datetime.now(UTC),
            )
            for symbol, signed in sorted(self._positions.items())
        )
        return PrivateActiveSnapshot(
            venue=self.venue,
            raw_open_order_count=0,
            raw_nonzero_position_count=len(positions),
            open_orders=(),
            positions=positions,
            unknown_active_records=(),
            completeness=SnapshotCompleteness.COMPLETE,
            observed_at=datetime.now(UTC),
            account_wide=True,
        )

    async def fetch_all_open_orders(self) -> tuple[PrivateOrder, ...]:
        return ()

    async def fetch_all_positions(self) -> tuple[PositionSnapshot, ...]:
        return (await self.fetch_active_snapshot()).positions

    async def fetch_closed_orders(self, instrument: Instrument) -> tuple[PrivateOrder, ...]:
        if instrument.symbol not in self._instruments:
            raise ValueError("portfolio simulator instrument is unknown")
        self.closed_order_symbols.append(instrument.symbol)
        return tuple(order for order in self._recent.values() if order.symbol == instrument.symbol)

    async def fetch_trading_fee(self, instrument: Instrument) -> Decimal:
        if instrument.symbol not in self._instruments:
            raise ValueError("portfolio simulator instrument is unknown")
        return Decimal("0.0005")

    async def watch_orders(self, instrument: Instrument) -> tuple[PrivateOrder, ...]:
        return await self.fetch_closed_orders(instrument)

    async def find_order_by_client_id(
        self,
        client_order_id: str,
        instrument: Instrument,
    ) -> PrivateOrder | None:
        if instrument.symbol not in self._instruments:
            raise ValueError("portfolio simulator instrument is unknown")
        return self._recent.get(client_order_id)

    async def cancel_order(self, order_id: str, instrument: Instrument) -> PrivateOrder:
        del order_id, instrument
        raise KeyError("portfolio simulator has no open orders")

    async def resolve_instrument(self, symbol: str) -> Instrument | None:
        return self._instruments.get(symbol)

    async def list_instruments(self) -> tuple[Instrument, ...]:
        return tuple(self._instruments.values())


async def _seed_portfolio_recovery(
    path: Path,
    action_count: int,
    *,
    action_state: LiveActionState = LiveActionState.HEDGED,
) -> tuple[
    LiveOrderJournal,
    dict[Venue, PortfolioEmergencyExchange],
    dict[tuple[Venue, str], Instrument],
]:
    instruments = {
        (venue, instrument.symbol): instrument
        for venue in Venue
        for instrument in tuple(
            _portfolio_instrument(venue, index) for index in range(action_count)
        )
    }
    adapters = {
        venue: PortfolioEmergencyExchange(
            venue,
            tuple(instruments[(venue, f"A{index:03d}/USDT:USDT")] for index in range(action_count)),
        )
        for venue in Venue
    }
    journal = LiveOrderJournal(path)
    await journal.initialise()
    observed = datetime.now(UTC)
    for index in range(action_count):
        base = f"A{index:03d}"
        symbol = f"{base}/USDT:USDT"
        action_id = f"portfolio-recovery-{index:02d}"
        long_request = VenueOrderRequest(
            Venue.BINANCE_USDM,
            venue_client_order_id(action_id, "long"),
            symbol,
            Side.BUY,
            "limit",
            Decimal("1"),
            Decimal("100"),
            "IOC",
            {"timeInForce": "IOC"},
        )
        short_request = replace(
            long_request,
            venue=Venue.OKX,
            client_order_id=venue_client_order_id(action_id, "short"),
            side=Side.SELL,
        )
        await journal.prepare(
            action_id,
            DirectedRouteKey(base, Venue.BINANCE_USDM, Venue.OKX),
            f"tranche-{index:02d}",
            long_request,
            short_request,
            {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
            {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
            {"projected_stress_usdt": "5"},
            "a" * 64,
        )
        if action_state == LiveActionState.PREPARED:
            continue
        await journal.mark_submit_attempted(
            action_id,
            (long_request.client_order_id, short_request.client_order_id),
        )
        filled_requests = (
            ((long_request, Decimal("0.001"), PrivateOrderStatus.FILLED),)
            if action_state == LiveActionState.PARTIAL
            else (
                (
                    request,
                    Decimal("0.001"),
                    PrivateOrderStatus.FILLED,
                )
                for request in (long_request, short_request)
            )
            if action_state
            in {
                LiveActionState.FILLED,
                LiveActionState.RECOVERING,
                LiveActionState.HEDGED,
                LiveActionState.CLOSING,
                LiveActionState.QUARANTINED,
            }
            else ()
        )
        for request, filled_quantity, order_status in filled_requests:
            seed_order = PrivateOrder(
                request.venue,
                f"seed-{request.client_order_id}",
                request.client_order_id,
                request.symbol,
                request.side,
                order_status,
                Decimal("0.001"),
                filled_quantity,
                Decimal("100"),
                filled_quantity * Decimal("0.05"),
                observed,
                request.price,
            )
            await journal.record_order_event(
                action_id,
                seed_order,
                f"seed-{request.client_order_id}",
            )
            adapters[request.venue].seed_order(seed_order)
            adapters[request.venue].seed_position(
                symbol,
                filled_quantity if request.side == Side.BUY else -filled_quantity,
            )
        if action_state in {
            LiveActionState.SUBMITTING,
            LiveActionState.ACKNOWLEDGED,
            LiveActionState.REJECTED,
            LiveActionState.UNKNOWN,
            LiveActionState.PARTIAL,
        }:
            resolved_requests = (
                (short_request,)
                if action_state == LiveActionState.PARTIAL
                else (long_request, short_request)
            )
            for request in resolved_requests:
                resolved_order = PrivateOrder(
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
                    observed,
                    request.price,
                )
                adapters[request.venue].seed_order(resolved_order)
                if action_state in {
                    LiveActionState.REJECTED,
                    LiveActionState.UNKNOWN,
                    LiveActionState.PARTIAL,
                }:
                    journal_order = (
                        resolved_order
                        if action_state in {LiveActionState.REJECTED, LiveActionState.PARTIAL}
                        else replace(resolved_order, status=PrivateOrderStatus.UNKNOWN)
                    )
                    await journal.record_order_event(
                        action_id,
                        journal_order,
                        f"seed-{request.client_order_id}",
                    )
        if action_state == LiveActionState.SUBMITTING:
            continue
        if action_state == LiveActionState.HEDGED:
            await journal.transition(action_id, LiveActionState.FILLED)
            await journal.transition(
                action_id,
                LiveActionState.HEDGED,
                residual_delta=Decimal(0),
            )
        elif action_state == LiveActionState.CLOSING:
            await journal.transition(action_id, LiveActionState.FILLED)
            await journal.transition(
                action_id,
                LiveActionState.HEDGED,
                residual_delta=Decimal(0),
            )
            await journal.transition(action_id, LiveActionState.CLOSING)
        else:
            await journal.transition(action_id, action_state)
    return journal, adapters, instruments


def _seed_request(venue: Venue, client_id: str, side: Side) -> VenueOrderRequest:
    return VenueOrderRequest(
        venue,
        client_id,
        "BTC/USDT:USDT",
        side,
        "limit",
        Decimal("1"),
        Decimal("100"),
        "IOC",
        {},
    )


def _service(
    tmp_path: Path,
    *,
    long_outcomes: tuple[ScriptedOrderOutcome, ...],
    short_outcomes: tuple[ScriptedOrderOutcome, ...] = (),
    emergency_outcomes: tuple[ScriptedOrderOutcome, ...] = (),
) -> tuple[LiveControlService, dict[Venue, DeterministicPrivateExchange]]:
    instruments = {venue: _instrument(venue) for venue in Venue}
    adapters = {
        Venue.BINANCE_USDM: DeterministicPrivateExchange(
            Venue.BINANCE_USDM,
            instruments[Venue.BINANCE_USDM],
            long_outcomes,
        ),
        Venue.OKX: DeterministicPrivateExchange(
            Venue.OKX,
            instruments[Venue.OKX],
            short_outcomes,
        ),
        Venue.BYBIT: DeterministicPrivateExchange(
            Venue.BYBIT,
            instruments[Venue.BYBIT],
            emergency_outcomes,
        ),
    }
    service = LiveControlService(
        LiveOrderJournal(tmp_path / "state.sqlite3"),
        adapters,
        instruments,
        DirectedRouteKey("BTC", Venue.BINANCE_USDM, Venue.OKX),
        "a" * 64,
    )
    return service, adapters


@pytest.mark.asyncio
async def test_live_reads_are_actual_private_exchange_state(tmp_path: Path) -> None:
    service, adapters = _service(
        tmp_path,
        long_outcomes=(
            ScriptedOrderOutcome(PrivateOrderStatus.FILLED, Decimal("1")),
            ScriptedOrderOutcome(PrivateOrderStatus.FILLED, Decimal("1")),
        ),
    )
    await adapters[Venue.BINANCE_USDM].submit_order(
        _seed_request(Venue.BINANCE_USDM, "external-seed", Side.BUY),
        _instrument(Venue.BINANCE_USDM),
    )
    snapshot = await service.snapshot()
    assert snapshot["source"] == "PRIVATE_EXCHANGE"
    positions = snapshot["positions"]
    assert isinstance(positions, list)
    assert positions[0]["venue"] == Venue.BINANCE_USDM.value
    assert positions[0]["base_quantity"] == "0.001"
    balances = snapshot["balances"]
    assert isinstance(balances, list)
    assert len(balances) == 3
    orders = snapshot["orders"]
    assert isinstance(orders, list)
    assert any(
        order["record_type"] == "PRIVATE_ORDER"
        and order["client_order_id"] == "external-seed"
        and order["status"] == PrivateOrderStatus.FILLED.value
        for order in orders
    )
    assert snapshot["risk"] == {
        "status": "INVALID_RISK_DATA",
        "reason": "UNJOURNALED_PRIVATE_EXPOSURE",
    }


@pytest.mark.asyncio
async def test_live_reads_expose_flat_zero_risk_from_actual_journal_state(tmp_path: Path) -> None:
    service, _ = _service(tmp_path, long_outcomes=())
    snapshot = await service.snapshot()
    assert snapshot["risk"] == {
        "status": "OK",
        "scope": "JOURNAL_RESERVATION",
        "reservation_count": 0,
        "per_route_stress_usdt": {},
        "portfolio_stress_usdt": "0",
    }


@pytest.mark.asyncio
async def test_live_reads_expose_exact_active_journal_risk(tmp_path: Path) -> None:
    service, adapters = _service(
        tmp_path,
        long_outcomes=(ScriptedOrderOutcome(PrivateOrderStatus.FILLED, Decimal("1")),),
    )
    route = DirectedRouteKey("BTC", Venue.BINANCE_USDM, Venue.OKX)
    await service._journal.initialise()
    await service._journal.prepare(
        "active-risk",
        route,
        "tranche-1",
        _seed_request(Venue.BINANCE_USDM, "risk-long", Side.BUY),
        _seed_request(Venue.OKX, "risk-short", Side.SELL),
        {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
        {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
        {"projected_stress_usdt": "1.25"},
        "a" * 64,
    )
    snapshot = await service.snapshot()
    orders = snapshot["orders"]
    assert isinstance(orders, list)
    assert {order["client_order_id"] for order in orders} == {"risk-long", "risk-short"}
    assert all(order["record_type"] == "JOURNAL_LEG" for order in orders)
    assert all(order["status"] == "PREPARED" for order in orders)
    assert snapshot["risk"] == {
        "status": "OK",
        "scope": "JOURNAL_RESERVATION",
        "reservation_count": 1,
        "per_route_stress_usdt": {route.value: "1.25"},
        "portfolio_stress_usdt": "1.25",
    }
    await adapters[Venue.BINANCE_USDM].submit_order(
        _seed_request(Venue.BINANCE_USDM, "external-risk", Side.BUY),
        _instrument(Venue.BINANCE_USDM),
    )
    mismatched = await service.snapshot()
    assert mismatched["risk"] == {
        "status": "INVALID_RISK_DATA",
        "reason": "PRIVATE_POSITION_JOURNAL_MISMATCH",
    }


@pytest.mark.asyncio
async def test_live_reads_aggregate_multiple_durable_route_reservations(tmp_path: Path) -> None:
    service, _ = _service(tmp_path, long_outcomes=())
    await service._journal.initialise()
    await service._journal.prepare(
        "active-btc",
        DirectedRouteKey("BTC", Venue.BINANCE_USDM, Venue.OKX),
        "tranche-btc",
        _seed_request(Venue.BINANCE_USDM, "risk-btc-long", Side.BUY),
        _seed_request(Venue.OKX, "risk-btc-short", Side.SELL),
        {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
        {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
        {"projected_stress_usdt": "1.25"},
        "a" * 64,
    )
    await service._journal.prepare(
        "active-eth",
        DirectedRouteKey("ETH", Venue.BINANCE_USDM, Venue.OKX),
        "tranche-eth",
        replace(
            _seed_request(Venue.BINANCE_USDM, "risk-eth-long", Side.BUY),
            symbol="ETH/USDT:USDT",
        ),
        replace(
            _seed_request(Venue.OKX, "risk-eth-short", Side.SELL),
            symbol="ETH/USDT:USDT",
        ),
        {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
        {Venue.BINANCE_USDM: Decimal("2000"), Venue.OKX: Decimal("2000")},
        {"projected_stress_usdt": "2.50"},
        "b" * 64,
    )

    snapshot = await service.snapshot()

    status = snapshot["status"]
    assert isinstance(status, dict)
    assert status["journal_state"] == LiveActionState.PREPARED.value
    assert status["pair_action_ids"] == ("active-btc", "active-eth")
    assert snapshot["risk"] == {
        "status": "OK",
        "scope": "JOURNAL_RESERVATION",
        "reservation_count": 2,
        "per_route_stress_usdt": {
            "BTC:binanceusdm>okx": "1.25",
            "ETH:binanceusdm>okx": "2.50",
        },
        "portfolio_stress_usdt": "3.75",
    }


@pytest.mark.asyncio
async def test_live_reads_reject_extreme_finite_decimal_risk_without_overflow(
    tmp_path: Path,
) -> None:
    service, _ = _service(tmp_path, long_outcomes=())
    await service._journal.initialise()
    await service._journal.prepare(
        "extreme-risk",
        DirectedRouteKey("BTC", Venue.BINANCE_USDM, Venue.OKX),
        "tranche-extreme",
        _seed_request(Venue.BINANCE_USDM, "extreme-long", Side.BUY),
        _seed_request(Venue.OKX, "extreme-short", Side.SELL),
        {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
        {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
        {"projected_stress_usdt": "1e999999999"},
        "d" * 64,
    )

    snapshot = await service.snapshot()

    assert snapshot["risk"] == {
        "status": "INVALID_RISK_DATA",
        "reason": "RISK_RESERVATION_UNKNOWN",
    }


@pytest.mark.asyncio
async def test_close_all_live_commits_multiple_flat_actions_atomically(tmp_path: Path) -> None:
    service, _ = _service(tmp_path, long_outcomes=())
    await service._journal.initialise()
    for pair_id, base in (("active-btc", "BTC"), ("active-eth", "ETH")):
        symbol = f"{base}/USDT:USDT"
        await service._journal.prepare(
            pair_id,
            DirectedRouteKey(base, Venue.BINANCE_USDM, Venue.OKX),
            f"tranche-{base.lower()}",
            replace(
                _seed_request(Venue.BINANCE_USDM, f"{pair_id}-long", Side.BUY),
                symbol=symbol,
            ),
            replace(
                _seed_request(Venue.OKX, f"{pair_id}-short", Side.SELL),
                symbol=symbol,
            ),
            {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
            {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
            {"projected_stress_usdt": "1"},
            "c" * 64,
        )

    result = await service.close_all_live()

    assert result.success is True
    assert result.flat_barrier_verified is True
    assert await service._journal.active_actions() == ()
    loaded = await asyncio.gather(
        *(service._journal.load(pair_id) for pair_id in ("active-btc", "active-eth"))
    )
    assert all(action is not None for action in loaded)
    assert tuple(action.state for action in loaded if action is not None) == (
        LiveActionState.FLAT,
        LiveActionState.FLAT,
    )


@pytest.mark.asyncio
async def test_live_reads_never_report_zero_risk_when_private_state_is_unknown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, adapters = _service(tmp_path, long_outcomes=())

    async def unavailable_account(instrument: Instrument) -> object:
        del instrument
        raise TimeoutError("held private state")

    monkeypatch.setattr(adapters[Venue.OKX], "fetch_account", unavailable_account)
    snapshot = await service.snapshot()
    assert snapshot["risk"] == {
        "status": "INVALID_RISK_DATA",
        "reason": "PRIVATE_STATE_UNAVAILABLE",
    }


@pytest.mark.asyncio
async def test_live_reads_reject_symbol_scoped_private_snapshot_for_risk(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, adapters = _service(tmp_path, long_outcomes=())
    adapter = adapters[Venue.OKX]
    original = adapter.fetch_active_snapshot

    async def symbol_scoped_snapshot() -> object:
        return replace(await original(), account_wide=False)

    monkeypatch.setattr(adapter, "fetch_active_snapshot", symbol_scoped_snapshot)
    snapshot = await service.snapshot()
    assert snapshot["risk"] == {
        "status": "INVALID_RISK_DATA",
        "reason": "PRIVATE_STATE_UNAVAILABLE",
    }


@pytest.mark.asyncio
async def test_emergency_flatten_journals_then_exchange_verifies_flat(tmp_path: Path) -> None:
    service, adapters = _service(
        tmp_path,
        long_outcomes=(
            ScriptedOrderOutcome(PrivateOrderStatus.FILLED, Decimal("1")),
            ScriptedOrderOutcome(PrivateOrderStatus.FILLED, Decimal("1")),
        ),
    )
    await adapters[Venue.BINANCE_USDM].submit_order(
        _seed_request(Venue.BINANCE_USDM, "external-seed", Side.BUY),
        _instrument(Venue.BINANCE_USDM),
    )
    result = await service.emergency_flatten()
    assert result.reconciliation is not None
    assert result.reconciliation.flat_verified, (
        result.reconciliation.discrepancies,
        result.reconciliation.unknown_client_order_ids,
        result.reconciliation.expected_signed_positions,
        result.reconciliation.actual_signed_positions,
    )
    assert result.success is True, (
        result.reconciliation.discrepancies if result.reconciliation else None,
        result.reconciliation.unknown_client_order_ids if result.reconciliation else None,
        result.reconciliation.actual_signed_positions if result.reconciliation else None,
        result.reconciliation.expected_signed_positions if result.reconciliation else None,
    )
    assert result.orders_sent == 1
    assert result.terminal_state == LiveActionState.FLAT
    assert result.reconciliation.flat_verified is True
    assert await adapters[Venue.BINANCE_USDM].fetch_all_positions() == ()


@pytest.mark.asyncio
async def test_indeterminate_emergency_submit_retains_exclusive_flatten_lease(
    tmp_path: Path,
) -> None:
    service, adapters = _service(
        tmp_path,
        long_outcomes=(
            ScriptedOrderOutcome(PrivateOrderStatus.FILLED, Decimal("1")),
            ScriptedOrderOutcome(
                PrivateOrderStatus.UNKNOWN,
                submit_fault="TIMEOUT",
            ),
        ),
    )
    await adapters[Venue.BINANCE_USDM].submit_order(
        _seed_request(Venue.BINANCE_USDM, "indeterminate-seed", Side.BUY),
        _instrument(Venue.BINANCE_USDM),
    )

    with pytest.raises(RuntimeError, match="indeterminate"):
        await service.emergency_flatten()

    retained = await LiveOrderJournal(tmp_path / "state.sqlite3").acquire_account_flatten_lease()
    assert retained is not None
    active = await service._journal.active_actions()
    assert len(active) == 1
    assert any(leg.submit_attempted and leg.status is None for leg in active[0].legs)

    restarted = LiveControlService(
        LiveOrderJournal(tmp_path / "state.sqlite3"),
        adapters,
        {venue: _instrument(venue) for venue in Venue},
    )
    with pytest.raises(RuntimeError, match="remains unknown"):
        await restarted.emergency_flatten()


@pytest.mark.asyncio
async def test_ten_route_private_reconciliation_closes_twenty_positions_atomically(
    tmp_path: Path,
) -> None:
    journal, adapters, instruments = await _seed_portfolio_recovery(
        tmp_path / "portfolio-state.sqlite3",
        10,
    )
    service = LiveControlService(journal, adapters, instruments)
    result = await service.emergency_flatten()

    assert result.success is True
    assert result.orders_sent == 20
    assert result.terminal_state == LiveActionState.FLAT
    assert result.flat_barrier_verified is True
    assert result.reconciliation is not None and result.reconciliation.flat_verified is True
    assert await journal.active_actions() == ()
    assert adapters[Venue.BINANCE_USDM].submit_calls == 10
    assert adapters[Venue.OKX].submit_calls == 10
    assert adapters[Venue.BYBIT].submit_calls == 0
    expected_symbols = {f"A{index:03d}/USDT:USDT" for index in range(10)}
    assert set(adapters[Venue.BINANCE_USDM].closed_order_symbols) == expected_symbols
    assert set(adapters[Venue.OKX].closed_order_symbols) == expected_symbols
    snapshots = await asyncio.gather(
        *(adapter.fetch_active_snapshot() for adapter in adapters.values())
    )
    assert all(not snapshot.positions for snapshot in snapshots)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "restart_state",
    [
        LiveActionState.PREPARED,
        LiveActionState.SUBMITTING,
        LiveActionState.ACKNOWLEDGED,
        LiveActionState.PARTIAL,
        LiveActionState.FILLED,
        LiveActionState.REJECTED,
        LiveActionState.UNKNOWN,
        LiveActionState.RECOVERING,
        LiveActionState.HEDGED,
        LiveActionState.CLOSING,
        LiveActionState.QUARANTINED,
    ],
)
async def test_ten_route_private_recovery_is_transition_complete(
    tmp_path: Path,
    restart_state: LiveActionState,
) -> None:
    journal, adapters, instruments = await _seed_portfolio_recovery(
        tmp_path / f"transition-{restart_state.value}.sqlite3",
        10,
        action_state=restart_state,
    )

    result = await LiveControlService(journal, adapters, instruments).emergency_flatten()

    assert result.success is True
    assert result.terminal_state == LiveActionState.FLAT
    assert result.flat_barrier_verified is True
    assert await journal.active_actions() == ()
    snapshots = await asyncio.gather(
        *(adapter.fetch_active_snapshot() for adapter in adapters.values())
    )
    assert all(not snapshot.positions for snapshot in snapshots)
    expected_submits = (
        10
        if restart_state == LiveActionState.PARTIAL
        else 20
        if restart_state
        in {
            LiveActionState.FILLED,
            LiveActionState.RECOVERING,
            LiveActionState.HEDGED,
            LiveActionState.CLOSING,
            LiveActionState.QUARANTINED,
        }
        else 0
    )
    assert sum(adapter.submit_calls for adapter in adapters.values()) == expected_submits


@pytest.mark.asyncio
async def test_restart_never_infers_missing_submit_outcomes_from_flat_positions(
    tmp_path: Path,
) -> None:
    journal, adapters, instruments = await _seed_portfolio_recovery(
        tmp_path / "missing-submit-outcome.sqlite3",
        10,
        action_state=LiveActionState.SUBMITTING,
    )
    for adapter in adapters.values():
        adapter._recent.clear()

    result = await LiveControlService(journal, adapters, instruments).emergency_flatten()

    assert result.success is False
    assert result.reason == ReasonCode.FLAT_BARRIER_PRIVATE_STATE_UNKNOWN
    assert all(adapter.submit_calls == 0 for adapter in adapters.values())
    active = await journal.active_actions()
    assert len(active) == 10
    assert all(action.state == LiveActionState.QUARANTINED for action in active)


@pytest.mark.asyncio
async def test_hung_restart_lookup_is_bounded_and_other_routes_still_flatten(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal, adapters, instruments = await _seed_portfolio_recovery(
        tmp_path / "hung-restart-lookup.sqlite3",
        10,
    )
    action = (await journal.active_actions())[0]
    symbol = "A000/USDT:USDT"
    unresolved = VenueOrderRequest(
        Venue.BYBIT,
        venue_client_order_id(action.pair_action_id, "unresolved"),
        symbol,
        Side.BUY,
        "limit",
        Decimal("1"),
        Decimal("100"),
        "IOC",
        {"timeInForce": "IOC"},
    )
    await journal.append_order_leg(
        action.pair_action_id,
        unresolved,
        Decimal("0.001"),
        Decimal("100"),
    )
    await journal.mark_leg_submit_attempted(
        action.pair_action_id,
        unresolved.client_order_id,
    )
    release_lookup = asyncio.Event()

    async def hung_lookup(client_order_id: str, instrument: Instrument) -> PrivateOrder | None:
        del client_order_id, instrument
        await release_lookup.wait()
        return None

    monkeypatch.setattr(adapters[Venue.BYBIT], "find_order_by_client_id", hung_lookup)
    result = await asyncio.wait_for(
        LiveControlService(journal, adapters, instruments).emergency_flatten(),
        timeout=6.0,
    )
    assert result.success is False
    assert result.orders_sent == 20
    snapshots = await asyncio.gather(
        *(adapter.fetch_active_snapshot() for adapter in adapters.values())
    )
    assert all(not snapshot.positions for snapshot in snapshots)
    release_lookup.set()
    await asyncio.sleep(0)
    await shutdown_private_requests(adapters)


@pytest.mark.asyncio
async def test_one_route_close_failure_does_not_block_other_portfolio_reductions(
    tmp_path: Path,
) -> None:
    journal, adapters, instruments = await _seed_portfolio_recovery(
        tmp_path / "isolated-route-failure.sqlite3",
        10,
    )
    failed_symbol = "A005/USDT:USDT"
    adapters[Venue.OKX].rejected_symbols.add(failed_symbol)

    result = await LiveControlService(journal, adapters, instruments).emergency_flatten()

    assert result.success is False
    assert result.orders_sent == 20
    assert result.reason == ReasonCode.FLAT_BARRIER_TIMEOUT
    active = await journal.active_actions()
    assert len(active) == 10
    assert all(action.state == LiveActionState.QUARANTINED for action in active)
    binance = await adapters[Venue.BINANCE_USDM].fetch_active_snapshot()
    okx = await adapters[Venue.OKX].fetch_active_snapshot()
    assert binance.positions == ()
    assert tuple(position.symbol for position in okx.positions) == (failed_symbol,)
    assert adapters[Venue.BINANCE_USDM].submit_calls == 10
    assert adapters[Venue.OKX].submit_calls == 10


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted_before_restart", [False, True])
async def test_restart_adopts_dead_flatten_lease_and_reuses_journaled_client_ids(
    tmp_path: Path,
    accepted_before_restart: bool,
) -> None:
    path = tmp_path / "adopted-flatten.sqlite3"
    journal, adapters, instruments = await _seed_portfolio_recovery(path, 1)
    action_id = "portfolio-recovery-00"
    symbol = "A000/USDT:USDT"
    pending_ids: dict[Venue, str] = {}
    for venue, side in (
        (Venue.BINANCE_USDM, Side.SELL),
        (Venue.OKX, Side.BUY),
    ):
        client_id = venue_client_order_id(action_id, f"restart-{venue.value}")
        pending_ids[venue] = client_id
        request = translate_protected_order(
            ExecutionIntent(
                client_id,
                venue,
                side,
                OrderPurpose.EMERGENCY_CLOSE,
                Decimal("0.001"),
                None,
                True,
            ),
            instruments[(venue, symbol)],
        )
        await journal.append_order_leg(action_id, request, Decimal("0.001"), None)
        await journal.mark_leg_submit_attempted(action_id, client_id)
        if accepted_before_restart:
            await adapters[venue].submit_order(request, instruments[(venue, symbol)])
    lease = await journal.acquire_account_flatten_lease()
    assert lease is not None
    with sqlite3.connect(path) as database:
        database.execute(
            "UPDATE live_control_leases SET owner_pid = ? WHERE lease_key = 'ACCOUNT_WIDE_FLATTEN'",
            (2_147_483_647,),
        )

    service = LiveControlService(LiveOrderJournal(path), adapters, instruments)
    if not accepted_before_restart:
        with pytest.raises(RuntimeError, match="remains unknown"):
            await service.emergency_flatten()
        assert len(await journal.active_actions()) == 1
        assert adapters[Venue.BINANCE_USDM].submit_calls == 0
        assert adapters[Venue.OKX].submit_calls == 0
        assert await LiveOrderJournal(path).acquire_account_flatten_lease() is not None
        return

    result = await service.emergency_flatten()

    assert result.success is True
    assert result.orders_sent == 0
    assert await journal.active_actions() == ()
    assert adapters[Venue.BINANCE_USDM].submitted_symbols == [symbol]
    assert adapters[Venue.OKX].submitted_symbols == [symbol]
    assert adapters[Venue.BINANCE_USDM].submit_calls == 1
    assert adapters[Venue.OKX].submit_calls == 1
    for venue, client_id in pending_ids.items():
        assert (
            await adapters[venue].find_order_by_client_id(
                client_id,
                instruments[(venue, symbol)],
            )
            is not None
        )


@pytest.mark.asyncio
async def test_concurrent_account_flatten_is_durable_single_flight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instruments = {(venue, _instrument(venue).symbol): _instrument(venue) for venue in Venue}
    instruments[(Venue.BINANCE_USDM, "ETH/USDT:USDT")] = _eth_instrument(Venue.BINANCE_USDM)
    exposed = MultiSymbolEmergencyExchange(Venue.BINANCE_USDM)
    adapters = {
        Venue.BINANCE_USDM: exposed,
        Venue.OKX: DeterministicPrivateExchange(Venue.OKX, _instrument(Venue.OKX), ()),
        Venue.BYBIT: DeterministicPrivateExchange(Venue.BYBIT, _instrument(Venue.BYBIT), ()),
    }
    state_path = tmp_path / "state.sqlite3"
    journal = LiveOrderJournal(state_path)
    await journal.initialise()
    first_service = LiveControlService(journal, adapters, instruments)
    second_service = LiveControlService(LiveOrderJournal(state_path), adapters, instruments)
    submit_started = asyncio.Event()
    release_submit = asyncio.Event()
    original_submit = exposed.submit_order

    async def held_submit(
        request: VenueOrderRequest,
        instrument: Instrument,
    ) -> PrivateOrder:
        submit_started.set()
        await release_submit.wait()
        return await original_submit(request, instrument)

    monkeypatch.setattr(exposed, "submit_order", held_submit)
    first_task = asyncio.create_task(first_service.emergency_flatten())
    await asyncio.wait_for(submit_started.wait(), timeout=1)

    second_task = asyncio.create_task(second_service.emergency_flatten())
    await asyncio.sleep(0)
    assert not second_task.done()
    release_submit.set()
    first = await asyncio.wait_for(first_task, timeout=2)
    second = await asyncio.wait_for(second_task, timeout=2)

    assert first.success is True
    assert second.success is True
    assert second.orders_sent == 0
    assert exposed.submit_calls == 1
    assert exposed.submitted_symbols == ["ETH/USDT:USDT"]


@pytest.mark.asyncio
async def test_cancel_all_live_verifies_exchange_and_reports_failure(tmp_path: Path) -> None:
    service, adapters = _service(
        tmp_path,
        long_outcomes=(ScriptedOrderOutcome(PrivateOrderStatus.OPEN),),
    )
    await adapters[Venue.BINANCE_USDM].submit_order(
        _seed_request(
            Venue.BINANCE_USDM,
            venue_client_order_id("cancel-integration", "open"),
            Side.BUY,
        ),
        _instrument(Venue.BINANCE_USDM),
    )
    result = await service.cancel_all_live()
    assert result.success is True
    assert result.cancelled_orders == 1

    failed_service, failed_adapters = _service(
        tmp_path / "failed",
        long_outcomes=(ScriptedOrderOutcome(PrivateOrderStatus.OPEN, cancel_failure=True),),
    )
    await failed_adapters[Venue.BINANCE_USDM].submit_order(
        _seed_request(
            Venue.BINANCE_USDM,
            venue_client_order_id("cancel-integration-failed", "open"),
            Side.BUY,
        ),
        _instrument(Venue.BINANCE_USDM),
    )
    failed = await failed_service.cancel_all_live()
    assert failed.success is False
    assert failed.instruction is not None


@pytest.mark.asyncio
async def test_emergency_flatten_cancels_external_orders_on_dedicated_accounts(
    tmp_path: Path,
) -> None:
    service, adapters = _service(
        tmp_path,
        long_outcomes=(ScriptedOrderOutcome(PrivateOrderStatus.OPEN),),
    )
    await adapters[Venue.BINANCE_USDM].submit_order(
        _seed_request(Venue.BINANCE_USDM, "external-active-order", Side.BUY),
        _instrument(Venue.BINANCE_USDM),
    )
    result = await service.emergency_flatten()
    assert result.success is True
    assert result.cancelled_orders == 1
    assert await adapters[Venue.BINANCE_USDM].fetch_all_open_orders() == ()


@pytest.mark.asyncio
async def test_emergency_flatten_without_qualification_closes_actual_position_symbol(
    tmp_path: Path,
) -> None:
    instruments = {(venue, _instrument(venue).symbol): _instrument(venue) for venue in Venue}
    instruments[(Venue.BINANCE_USDM, "ETH/USDT:USDT")] = _eth_instrument(Venue.BINANCE_USDM)
    exposed = MultiSymbolEmergencyExchange(Venue.BINANCE_USDM)
    adapters = {
        Venue.BINANCE_USDM: exposed,
        Venue.OKX: DeterministicPrivateExchange(Venue.OKX, _instrument(Venue.OKX), ()),
        Venue.BYBIT: DeterministicPrivateExchange(Venue.BYBIT, _instrument(Venue.BYBIT), ()),
    }
    state_path = tmp_path / "state.sqlite3"
    service = LiveControlService(
        LiveOrderJournal(state_path),
        adapters,
        instruments,
        qualified_route=None,
        qualification_hash=None,
    )
    result = await service.emergency_flatten()
    assert result.success is True, (
        result.reconciliation.discrepancies if result.reconciliation else None,
        result.reconciliation.unknown_client_order_ids if result.reconciliation else None,
        result.reconciliation.actual_signed_positions if result.reconciliation else None,
        result.reconciliation.expected_signed_positions if result.reconciliation else None,
    )
    assert exposed.submitted_symbols == ["ETH/USDT:USDT"]
    with sqlite3.connect(state_path) as database:
        symbols = database.execute("SELECT symbol FROM live_order_legs").fetchall()
        reservation = database.execute(
            "SELECT risk_reservation_json FROM live_pair_actions"
        ).fetchone()
    assert symbols == [("ETH/USDT:USDT",)]
    assert reservation is not None
    assert '"qualification_bypassed_for_risk_reduction": true' in str(reservation[0])


def test_cli_emergency_flatten_requires_separate_secret_and_exact_phrase() -> None:
    assert emergency_unlock_valid(
        EMERGENCY_CONFIRMATION,
        {
            "IPEG_EMERGENCY_UNLOCK_SECRET": "separate-secret",
            "IPEG_EMERGENCY_UNLOCK": "separate-secret",
        },
    )
    assert not emergency_unlock_valid(
        "WRONG",
        {
            "IPEG_EMERGENCY_UNLOCK_SECRET": "separate-secret",
            "IPEG_EMERGENCY_UNLOCK": "separate-secret",
        },
    )
    assert not emergency_unlock_valid(
        EMERGENCY_CONFIRMATION,
        {
            "IPEG_EMERGENCY_UNLOCK_SECRET": "one",
            "IPEG_EMERGENCY_UNLOCK": "two",
        },
    )
