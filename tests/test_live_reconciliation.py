from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.client_ids import venue_client_order_id
from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.live_journal import LiveOrderJournal
from interexchange_perp_grid.live_reconciliation import (
    FlatBarrierPolicy,
    ReconciliationReport,
    ReconciliationStatus,
    VenuePrivateState,
    evaluate_canary_risk_from_private_state,
    reconcile_private_states,
    wait_for_stable_flat,
)
from interexchange_perp_grid.private_domain import (
    AccountSnapshot,
    PositionSnapshot,
    PrivateOrder,
    PrivateOrderStatus,
    SnapshotCompleteness,
    UnknownActiveRecord,
    VenueOrderRequest,
)
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.strategy import DirectedRouteKey

_ROUTE = DirectedRouteKey("BTC", Venue.BINANCE_USDM, Venue.OKX)
_REQUIRED = {Venue.BINANCE_USDM, Venue.OKX, Venue.BYBIT}
_LONG_ID = venue_client_order_id("pair-1", "long")
_SHORT_ID = venue_client_order_id("pair-1", "short")


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
        len(orders),
        len(positions),
        (),
        SnapshotCompleteness.UNKNOWN if error else SnapshotCompleteness.COMPLETE,
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


def test_unknown_raw_active_record_and_count_mismatch_deny_entry_and_flat() -> None:
    states = _empty_states()
    states[Venue.OKX] = VenuePrivateState(
        Venue.OKX,
        _account(Venue.OKX),
        (),
        (),
        (),
        Decimal("0.0005"),
        None,
        1,
        0,
        (
            UnknownActiveRecord(
                Venue.OKX,
                "OPEN_ORDER",
                "UNKNOWN_SYMBOL",
                {"symbol": "UNKNOWN", "id": "active-1"},
            ),
        ),
        SnapshotCompleteness.UNKNOWN,
    )
    report = reconcile_private_states(None, states, set(), _REQUIRED)
    assert report.status == ReconciliationStatus.UNKNOWN
    assert report.raw_open_order_count == 1
    assert report.unknown_active_record_count == 1
    assert report.snapshots_complete is False
    assert report.flat_verified is False


@pytest.mark.asyncio
async def test_late_fill_after_first_empty_snapshot_resets_flat_barrier() -> None:
    flat = reconcile_private_states(None, _empty_states(), set(), _REQUIRED)
    exposed_states = _empty_states()
    exposed_states[Venue.OKX] = _state(
        Venue.OKX,
        positions=(_position(Venue.OKX, Side.BUY, "0.001"),),
    )
    late_fill = reconcile_private_states(None, exposed_states, set(), _REQUIRED)
    reports = [flat, late_fill, flat, flat]
    calls = 0

    async def report_factory() -> ReconciliationReport:
        nonlocal calls
        selected = reports[min(calls, len(reports) - 1)]
        calls += 1
        return selected

    async def watermark() -> int:
        return 1 if calls >= 2 else 0

    result = await wait_for_stable_flat(
        report_factory,
        watermark,
        FlatBarrierPolicy(2, 0, 0.001, 0.1),
    )
    assert result.verified is True
    assert calls >= 4
    assert result.consecutive_snapshots >= 2


@pytest.mark.asyncio
async def test_incomplete_private_state_times_out_without_flat_success() -> None:
    states = _empty_states()
    states[Venue.BYBIT] = _state(Venue.BYBIT, error="timeout")
    unknown = reconcile_private_states(None, states, set(), _REQUIRED)

    async def report_factory() -> ReconciliationReport:
        return unknown

    async def watermark() -> int:
        return 0

    result = await wait_for_stable_flat(
        report_factory,
        watermark,
        FlatBarrierPolicy(2, 0, 0.001, 0.01),
    )
    assert result.verified is False
    assert result.timed_out is True
    assert result.report.flat_verified is False


@pytest.mark.asyncio
async def test_active_journal_is_matched_to_orders_and_actual_positions(tmp_path: Path) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    long_request = VenueOrderRequest(
        Venue.BINANCE_USDM,
        _LONG_ID,
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
        _SHORT_ID,
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
    await journal.mark_submit_attempted("pair-1", (_LONG_ID, _SHORT_ID))
    long_order = _order(Venue.BINANCE_USDM, _LONG_ID, Side.BUY, "0.001")
    short_order = _order(Venue.OKX, _SHORT_ID, Side.SELL, "0.001")
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
    assert _SHORT_ID in missing_order.unknown_client_order_ids


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
