from __future__ import annotations

from decimal import Decimal

import pytest

from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.execution import (
    ExecutionIntent,
    OrderPurpose,
    PairActionState,
    PairExecutionCoordinator,
    Side,
    SimulatedOrderResult,
    SimulatedOrderStatus,
    Tranche,
)
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.risk import RiskDecision
from interexchange_perp_grid.strategy import DirectedRouteKey

ROUTE = DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX)
ACCEPTED_RISK = RiskDecision(
    True,
    ReasonCode.RISK_RESERVED,
    {"projected_route_stress_usdt": Decimal("4")},
)


def tranche(identifier: str = "T1", quantity: str = "1") -> Tranche:
    return Tranche(
        tranche_id=identifier,
        route=ROUTE,
        requested_quantity=Decimal(quantity),
        target_close_spread=Decimal("1"),
        stop_spread=Decimal("20"),
        projected_stress_usdt=Decimal("4"),
    )


def assert_state(item: Tranche, expected: PairActionState) -> None:
    assert item.state == expected


def assert_reason(item: Tranche, expected: ReasonCode) -> None:
    assert item.reason == expected


def result(
    order_id: str,
    venue: Venue,
    side: Side,
    purpose: OrderPurpose,
    requested: str,
    filled: str,
    price: str | None,
    fee: str,
    status: SimulatedOrderStatus,
    *,
    unbounded: bool = False,
) -> SimulatedOrderResult:
    intent = ExecutionIntent(
        client_order_id=order_id,
        venue=venue,
        side=side,
        purpose=purpose,
        quantity=Decimal(requested),
        worst_acceptable_price=(
            None if unbounded else Decimal("1000") if side == Side.BUY else Decimal("1")
        ),
        unbounded_market=unbounded,
    )
    return SimulatedOrderResult(
        intent=intent,
        status=status,
        actual_fill_quantity=Decimal(filled),
        fill_price=None if price is None else Decimal(price),
        fee_usdt=Decimal(fee),
    )


def open_full(
    coordinator: PairExecutionCoordinator,
    item: Tranche,
    *,
    long_price: str = "100",
    short_price: str = "110",
) -> None:
    coordinator.precheck_and_reserve(item, ACCEPTED_RISK)
    coordinator.submit_open(
        item,
        result(
            f"{item.tranche_id}-open-long",
            Venue.BYBIT,
            Side.BUY,
            OrderPurpose.NORMAL_OPEN,
            str(item.requested_quantity),
            str(item.requested_quantity),
            long_price,
            "0.1",
            SimulatedOrderStatus.FILLED,
        ),
        result(
            f"{item.tranche_id}-open-short",
            Venue.OKX,
            Side.SELL,
            OrderPurpose.NORMAL_OPEN,
            str(item.requested_quantity),
            str(item.requested_quantity),
            short_price,
            "0.1",
            SimulatedOrderStatus.FILLED,
        ),
    )


def close_result(
    order_id: str,
    venue: Venue,
    side: Side,
    quantity: str,
    price: str,
    fee: str,
    purpose: OrderPurpose = OrderPurpose.NORMAL_CLOSE,
    *,
    unbounded: bool = False,
) -> SimulatedOrderResult:
    return result(
        order_id,
        venue,
        side,
        purpose,
        quantity,
        quantity,
        price,
        fee,
        SimulatedOrderStatus.FILLED,
        unbounded=unbounded,
    )


def test_normal_execution_requires_a_protected_price_and_market_is_emergency_only() -> None:
    with pytest.raises(ValueError, match=ReasonCode.PROTECTED_PRICE_MISSING.value):
        ExecutionIntent(
            "normal",
            Venue.BYBIT,
            Side.BUY,
            OrderPurpose.NORMAL_OPEN,
            Decimal("1"),
            None,
        )
    with pytest.raises(ValueError, match="emergency-only"):
        ExecutionIntent(
            "normal-market",
            Venue.BYBIT,
            Side.BUY,
            OrderPurpose.NORMAL_OPEN,
            Decimal("1"),
            Decimal("101"),
            True,
        )
    emergency = ExecutionIntent(
        "emergency",
        Venue.BINANCE_USDM,
        Side.SELL,
        OrderPurpose.EMERGENCY_HEDGE,
        Decimal("1"),
        None,
        True,
    )
    assert emergency.unbounded_market is True
    with pytest.raises(ValueError, match="protected price cap"):
        SimulatedOrderResult(
            ExecutionIntent(
                "capped-buy",
                Venue.BYBIT,
                Side.BUY,
                OrderPurpose.NORMAL_OPEN,
                Decimal("1"),
                Decimal("100"),
            ),
            SimulatedOrderStatus.FILLED,
            Decimal("1"),
            Decimal("101"),
            Decimal("0.1"),
        )


def test_open_add_partial_close_and_full_close_produces_four_leg_pnl() -> None:
    coordinator = PairExecutionCoordinator()
    first = tranche("T1")
    second = tranche("T2", "0.5")
    open_full(coordinator, first)
    open_full(coordinator, second, long_price="101", short_price="111")
    assert_state(first, PairActionState.HEDGED)
    assert_state(second, PairActionState.HEDGED)
    assert first.paired_quantity == Decimal("1")
    assert first.target_close_spread == Decimal("1")
    assert first.stop_spread == Decimal("20")
    assert first.projected_stress_usdt == Decimal("4")

    coordinator.close(
        first,
        close_result("T1-close-long-1", Venue.BYBIT, Side.SELL, "0.4", "104", "0.04"),
        close_result("T1-close-short-1", Venue.OKX, Side.BUY, "0.4", "106", "0.04"),
    )
    assert_state(first, PairActionState.CLOSING)
    assert first.closed_quantity == Decimal("0.4")
    coordinator.close(
        first,
        close_result("T1-close-long-2", Venue.BYBIT, Side.SELL, "0.6", "105", "0.06"),
        close_result("T1-close-short-2", Venue.OKX, Side.BUY, "0.6", "105", "0.06"),
    )
    first.add_funding(Decimal("0.2"))
    pnl = first.pnl()
    assert_state(first, PairActionState.CLOSED)
    assert pnl.gross_price_pnl_usdt == Decimal("9.20")
    assert pnl.fees_usdt == Decimal("0.40")
    assert pnl.funding_usdt == Decimal("0.2")
    assert pnl.net_pnl_usdt == Decimal("9.00")

    coordinator.close(
        second,
        close_result("T2-close-long", Venue.BYBIT, Side.SELL, "0.5", "90", "0.05"),
        close_result("T2-close-short", Venue.OKX, Side.BUY, "0.5", "121", "0.05"),
    )
    second.add_funding(Decimal("-0.1"))
    losing_pnl = second.pnl()
    assert losing_pnl.gross_price_pnl_usdt == Decimal("-10.5")
    assert losing_pnl.net_pnl_usdt == Decimal("-10.90")


def test_partial_fill_and_rejected_second_leg_hedge_only_actual_residual() -> None:
    coordinator = PairExecutionCoordinator()
    item = tranche()
    coordinator.precheck_and_reserve(item, ACCEPTED_RISK)
    coordinator.submit_open(
        item,
        result(
            "open-long",
            Venue.BYBIT,
            Side.BUY,
            OrderPurpose.NORMAL_OPEN,
            "1",
            "0.4",
            "100",
            "0.04",
            SimulatedOrderStatus.PARTIAL,
        ),
        result(
            "open-short",
            Venue.OKX,
            Side.SELL,
            OrderPurpose.NORMAL_OPEN,
            "1",
            "0",
            None,
            "0",
            SimulatedOrderStatus.REJECTED,
        ),
    )
    assert_state(item, PairActionState.PARTIALLY_HEDGED)
    assert_reason(item, ReasonCode.SECOND_LEG_REJECTED)
    assert item.residual_quantity == Decimal("0.4")

    coordinator.emergency_hedge(
        item,
        result(
            "third-venue-hedge",
            Venue.BINANCE_USDM,
            Side.SELL,
            OrderPurpose.EMERGENCY_HEDGE,
            "0.4",
            "0.4",
            "99",
            "0.05",
            SimulatedOrderStatus.FILLED,
            unbounded=True,
        ),
    )
    assert_state(item, PairActionState.EMERGENCY_HEDGED)
    assert item.emergency_fills[0].quantity == Decimal("0.4")
    assert item.residual_quantity == 0


def test_unknown_stale_outage_and_forced_close_have_deterministic_recovery() -> None:
    coordinator = PairExecutionCoordinator()
    unknown = tranche("unknown")
    coordinator.precheck_and_reserve(unknown, ACCEPTED_RISK)
    coordinator.submit_open(
        unknown,
        result(
            "unknown-long",
            Venue.BYBIT,
            Side.BUY,
            OrderPurpose.NORMAL_OPEN,
            "1",
            "0",
            None,
            "0",
            SimulatedOrderStatus.UNKNOWN,
        ),
        result(
            "unknown-short",
            Venue.OKX,
            Side.SELL,
            OrderPurpose.NORMAL_OPEN,
            "1",
            "0",
            None,
            "0",
            SimulatedOrderStatus.UNKNOWN,
        ),
    )
    assert_state(unknown, PairActionState.UNKNOWN_ORDER)
    coordinator.resolve_unknown(
        unknown,
        result(
            "unknown-long",
            Venue.BYBIT,
            Side.BUY,
            OrderPurpose.NORMAL_OPEN,
            "1",
            "1",
            "100",
            "0.1",
            SimulatedOrderStatus.FILLED,
        ),
        result(
            "unknown-short",
            Venue.OKX,
            Side.SELL,
            OrderPurpose.NORMAL_OPEN,
            "1",
            "1",
            "110",
            "0.1",
            SimulatedOrderStatus.FILLED,
        ),
    )
    assert_state(unknown, PairActionState.HEDGED)
    assert len(unknown.entry_long_fills) == 1

    coordinator.mark_private_stream_stale(unknown)
    assert_reason(unknown, ReasonCode.PRIVATE_STREAM_STALE)
    coordinator.mark_venue_outage(unknown)
    assert_reason(unknown, ReasonCode.VENUE_OUTAGE)
    coordinator.force_close(
        unknown,
        close_result(
            "force-long",
            Venue.BYBIT,
            Side.SELL,
            "1",
            "95",
            "0.2",
            OrderPurpose.EMERGENCY_CLOSE,
            unbounded=True,
        ),
        close_result(
            "force-short",
            Venue.OKX,
            Side.BUY,
            "1",
            "115",
            "0.2",
            OrderPurpose.EMERGENCY_CLOSE,
            unbounded=True,
        ),
    )
    assert_state(unknown, PairActionState.FORCED_CLOSED)
    assert_reason(unknown, ReasonCode.FORCED_CLOSED)
    assert unknown.pnl().net_pnl_usdt == Decimal("-10.60")


def test_first_venue_failure_hedges_short_residual_on_third_venue() -> None:
    coordinator = PairExecutionCoordinator()
    item = tranche("first-venue-failure")
    coordinator.precheck_and_reserve(item, ACCEPTED_RISK)
    coordinator.submit_open(
        item,
        result(
            "failed-long",
            Venue.BYBIT,
            Side.BUY,
            OrderPurpose.NORMAL_OPEN,
            "1",
            "0",
            None,
            "0",
            SimulatedOrderStatus.REJECTED,
        ),
        result(
            "filled-short",
            Venue.OKX,
            Side.SELL,
            OrderPurpose.NORMAL_OPEN,
            "1",
            "0.3",
            "110",
            "0.03",
            SimulatedOrderStatus.PARTIAL,
        ),
    )
    assert item.signed_residual_quantity == Decimal("-0.3")
    coordinator.emergency_hedge(
        item,
        result(
            "third-buy",
            Venue.BINANCE_USDM,
            Side.BUY,
            OrderPurpose.EMERGENCY_HEDGE,
            "0.3",
            "0.3",
            "111",
            "0.03",
            SimulatedOrderStatus.FILLED,
            unbounded=True,
        ),
    )
    assert_state(item, PairActionState.EMERGENCY_HEDGED)
    assert item.residual_quantity == 0
