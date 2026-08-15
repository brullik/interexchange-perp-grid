from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.canary_runtime import _rebuild_active_plan
from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.execution import ExecutionIntent, OrderPurpose, Side
from interexchange_perp_grid.live_journal import LiveOrderJournal
from interexchange_perp_grid.private_execution import translate_protected_order
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
