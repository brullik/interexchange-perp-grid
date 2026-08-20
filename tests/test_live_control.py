from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.client_ids import venue_client_order_id
from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.live_control import (
    EMERGENCY_CONFIRMATION,
    LiveControlService,
    emergency_unlock_valid,
)
from interexchange_perp_grid.live_journal import LiveActionState, LiveOrderJournal
from interexchange_perp_grid.live_simulator import (
    DeterministicPrivateExchange,
    ScriptedOrderOutcome,
)
from interexchange_perp_grid.private_domain import (
    PositionSnapshot,
    PrivateOrder,
    PrivateOrderStatus,
    VenueOrderRequest,
)
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
