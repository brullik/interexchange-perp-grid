from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.live_journal import LiveOrderJournal
from interexchange_perp_grid.live_reconciliation import (
    ReconciliationStatus,
    VenuePrivateState,
    evaluate_canary_risk_from_private_state,
    reconcile_private_states,
)
from interexchange_perp_grid.private_domain import (
    AccountSnapshot,
    PositionSnapshot,
    PrivateOrder,
    PrivateOrderStatus,
    VenueOrderRequest,
)
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.strategy import DirectedRouteKey

_ROUTE = DirectedRouteKey("BTC", Venue.BINANCE_USDM, Venue.OKX)
_REQUIRED = {Venue.BINANCE_USDM, Venue.OKX, Venue.BYBIT}


def _account(venue: Venue) -> AccountSnapshot:
    return AccountSnapshot(
        venue,
        Decimal("100"),
        Decimal("100"),
        "cross",
        "oneway",
        True,
        ("read", "trade"),
        datetime.now(UTC),
        False,
        False,
    )


def _state(
    venue: Venue,
    *,
    orders: tuple[PrivateOrder, ...] = (),
    recent: tuple[PrivateOrder, ...] = (),
    positions: tuple[PositionSnapshot, ...] = (),
    error: str | None = None,
) -> VenuePrivateState:
    return VenuePrivateState(
        venue,
        None if error else _account(venue),
        orders,
        recent,
        positions,
        None if error else Decimal("0.0005"),
        error,
    )


def _order(
    venue: Venue,
    client_id: str,
    side: Side,
    quantity: str,
    status: PrivateOrderStatus = PrivateOrderStatus.FILLED,
) -> PrivateOrder:
    return PrivateOrder(
        venue,
        f"exchange-{client_id}",
        client_id,
        "BTC/USDT:USDT",
        side,
        status,
        Decimal("0.001"),
        Decimal(quantity),
        Decimal("100") if Decimal(quantity) else None,
        Decimal("0.001") if Decimal(quantity) else None,
        datetime.now(UTC),
        Decimal("100"),
    )


def _position(venue: Venue, side: Side, quantity: str) -> PositionSnapshot:
    return PositionSnapshot(
        venue,
        "BTC/USDT:USDT",
        side,
        Decimal(quantity),
        Decimal("100"),
        Decimal("100"),
        datetime.now(UTC),
    )


def _empty_states() -> dict[Venue, VenuePrivateState]:
    return {venue: _state(venue) for venue in _REQUIRED}


def test_preentry_reconciliation_requires_all_three_venues_empty_and_known() -> None:
    report = reconcile_private_states(None, _empty_states(), set(), _REQUIRED)
    assert report.status == ReconciliationStatus.CONSISTENT
    assert report.flat_verified is True

    exposed = _empty_states()
    exposed[Venue.BINANCE_USDM] = _state(
        Venue.BINANCE_USDM,
        positions=(_position(Venue.BINANCE_USDM, Side.BUY, "0.001"),),
    )
    report = reconcile_private_states(None, exposed, set(), _REQUIRED)
    assert report.status == ReconciliationStatus.INCONSISTENT
    assert "binanceusdm:POSITION_MISMATCH" in report.discrepancies
    assert report.flat_verified is False

    unknown = _empty_states()
    unknown[Venue.BYBIT] = _state(Venue.BYBIT, error="timeout")
    report = reconcile_private_states(None, unknown, set(), _REQUIRED)
    assert report.status == ReconciliationStatus.UNKNOWN


def test_offsetting_nonzero_positions_can_never_be_reported_as_flat() -> None:
    states = _empty_states()
    states[Venue.BINANCE_USDM] = _state(
        Venue.BINANCE_USDM,
        positions=(
            _position(Venue.BINANCE_USDM, Side.BUY, "0.001"),
            _position(Venue.BINANCE_USDM, Side.SELL, "0.001"),
        ),
    )
    report = reconcile_private_states(None, states, set(), _REQUIRED)
    assert report.actual_signed_positions[Venue.BINANCE_USDM] == 0
    assert report.open_position_count == 2
    assert report.status == ReconciliationStatus.INCONSISTENT
    assert report.flat_verified is False


@pytest.mark.asyncio
async def test_active_journal_is_matched_to_orders_and_actual_positions(tmp_path: Path) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    long_request = VenueOrderRequest(
        Venue.BINANCE_USDM,
        "ipeg-pair-long",
        "BTC/USDT:USDT",
        Side.BUY,
        "limit",
        Decimal("1"),
        Decimal("100"),
        "IOC",
        {},
    )
    short_request = VenueOrderRequest(
        Venue.OKX,
        "ipeg-pair-short",
        "BTC/USDT:USDT",
        Side.SELL,
        "limit",
        Decimal("1"),
        Decimal("100"),
        "IOC",
        {},
    )
    await journal.prepare(
        "pair-1",
        _ROUTE,
        "tranche-1",
        long_request,
        short_request,
        {Venue.BINANCE_USDM: Decimal("0.001"), Venue.OKX: Decimal("0.001")},
        {Venue.BINANCE_USDM: Decimal("100"), Venue.OKX: Decimal("100")},
        {"stress": "0.8"},
        "a" * 64,
    )
    await journal.mark_submit_attempted("pair-1", ("ipeg-pair-long", "ipeg-pair-short"))
    long_order = _order(Venue.BINANCE_USDM, "ipeg-pair-long", Side.BUY, "0.001")
    short_order = _order(Venue.OKX, "ipeg-pair-short", Side.SELL, "0.001")
    await journal.record_order_event("pair-1", long_order, "long-filled")
    await journal.record_order_event("pair-1", short_order, "short-filled")
    action = await journal.load("pair-1")
    assert action is not None

    states = _empty_states()
    states[Venue.BINANCE_USDM] = _state(
        Venue.BINANCE_USDM,
        recent=(long_order,),
        positions=(_position(Venue.BINANCE_USDM, Side.BUY, "0.001"),),
    )
    states[Venue.OKX] = _state(
        Venue.OKX,
        recent=(short_order,),
        positions=(_position(Venue.OKX, Side.SELL, "0.001"),),
    )
    report = reconcile_private_states(
        action,
        states,
        await journal.known_client_order_ids(),
        _REQUIRED,
    )
    assert report.status == ReconciliationStatus.CONSISTENT
    assert report.residual_delta == 0
    assert report.flat_verified is False

    states[Venue.OKX] = _state(
        Venue.OKX,
        positions=(_position(Venue.OKX, Side.SELL, "0.001"),),
    )
    missing_order = reconcile_private_states(
        action,
        states,
        await journal.known_client_order_ids(),
        _REQUIRED,
    )
    assert missing_order.status == ReconciliationStatus.UNKNOWN
    assert "ipeg-pair-short" in missing_order.unknown_client_order_ids


def test_canary_risk_bootstraps_from_exchange_positions_and_one_dollar_limit() -> None:
    accepted = evaluate_canary_risk_from_private_state(
        _ROUTE,
        _empty_states(),
        Decimal("5"),
        Decimal("0.8"),
        pair_stress_limit_usdt=Decimal("1"),
        portfolio_stress_limit_usdt=Decimal("50"),
        free_margin_floor_ratio=Decimal("0.20"),
        effective_leverage_cap=Decimal("3"),
        exit_depth_sufficient=True,
    )
    assert accepted.accepted is True

    exposed = _empty_states()
    exposed[Venue.BINANCE_USDM] = _state(
        Venue.BINANCE_USDM,
        positions=(_position(Venue.BINANCE_USDM, Side.BUY, "0.001"),),
    )
    rejected = evaluate_canary_risk_from_private_state(
        _ROUTE,
        exposed,
        Decimal("5"),
        Decimal("0.8"),
        pair_stress_limit_usdt=Decimal("1"),
        portfolio_stress_limit_usdt=Decimal("50"),
        free_margin_floor_ratio=Decimal("0.20"),
        effective_leverage_cap=Decimal("3"),
        exit_depth_sufficient=True,
    )
    assert rejected.accepted is False
    assert rejected.reason == ReasonCode.UNRESOLVED_EXECUTION_STATE

    over_limit = evaluate_canary_risk_from_private_state(
        _ROUTE,
        _empty_states(),
        Decimal("5"),
        Decimal("1.01"),
        pair_stress_limit_usdt=Decimal("1"),
        portfolio_stress_limit_usdt=Decimal("50"),
        free_margin_floor_ratio=Decimal("0.20"),
        effective_leverage_cap=Decimal("3"),
        exit_depth_sufficient=True,
    )
    assert over_limit.reason == ReasonCode.PAIR_STRESS_LIMIT
