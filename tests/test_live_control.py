from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

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
from interexchange_perp_grid.private_domain import PrivateOrderStatus, VenueOrderRequest
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
    assert result.success is True
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
        _seed_request(Venue.BINANCE_USDM, "ipeg-open-order", Side.BUY),
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
        _seed_request(Venue.BINANCE_USDM, "ipeg-open-failed", Side.BUY),
        _instrument(Venue.BINANCE_USDM),
    )
    failed = await failed_service.cancel_all_live()
    assert failed.success is False
    assert failed.instruction is not None


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
