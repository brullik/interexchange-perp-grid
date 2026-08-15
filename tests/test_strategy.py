from __future__ import annotations

from decimal import Decimal

import pytest

from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.strategy import (
    AdaptiveGridCalibrator,
    CalibrationObservation,
    CostInputs,
    DirectedRouteKey,
    calculate_stressed_cost,
    evaluate_entry_signal,
)


def observations(*spreads: str) -> tuple[CalibrationObservation, ...]:
    return tuple(
        CalibrationObservation(Decimal(spread), Decimal(index + 1) * 10)
        for index, spread in enumerate(spreads)
    )


def cost_inputs(entry_short: str = "110") -> CostInputs:
    return CostInputs(
        quantity=Decimal("1"),
        entry_long_price=Decimal("100"),
        entry_short_price=Decimal(entry_short),
        target_exit_long_price=Decimal("104"),
        target_exit_short_price=Decimal("105"),
        long_fee_rate=Decimal("0.001"),
        short_fee_rate=Decimal("0.001"),
        entry_impact_usdt=Decimal("0.1"),
        exit_impact_usdt=Decimal("0.1"),
        expected_funding_cost_usdt=Decimal("0.1"),
        funding_stress_usdt=Decimal("0.1"),
        latency_reserve_usdt=Decimal("0.1"),
        unmatched_hedge_reserve_usdt=Decimal("0.1"),
        reconciliation_forced_exit_reserve_usdt=Decimal("0.1"),
        liquidation_distance_reserve_usdt=Decimal("0.1"),
    )


def test_calibration_is_robust_versioned_bounded_and_isolated() -> None:
    calibrator = AdaptiveGridCalibrator(5, Decimal("0.20"))
    forward = DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX)
    reverse = DirectedRouteKey("BTC", Venue.OKX, Venue.BYBIT)
    first = calibrator.calibrate(
        forward,
        Decimal("0.01"),
        observations("10", "11", "12", "13", "10000"),
        Decimal("2"),
        Decimal("3"),
        Decimal("0.5"),
    )
    assert first.version == 1
    assert first.median_spread_bps == Decimal("12")
    assert first.mad_spread_bps == Decimal("1")
    assert first.grid_step_bps == Decimal("3")
    assert first.convergence_p90_seconds == Decimal("46.0")

    changed = calibrator.calibrate(
        forward,
        Decimal("0.01"),
        observations("100", "110", "120", "130", "140"),
        Decimal("20"),
        Decimal("20"),
        Decimal("0.5"),
    )
    assert changed.version == 2
    assert changed.grid_step_bps == Decimal("3.60")

    other_size = calibrator.calibrate(
        forward,
        Decimal("0.10"),
        observations("4", "5", "6", "7", "8"),
        Decimal("1"),
        Decimal("1"),
        Decimal("0.5"),
    )
    reverse_parameters = calibrator.calibrate(
        reverse,
        Decimal("0.01"),
        observations("20", "21", "22", "23", "24"),
        Decimal("1"),
        Decimal("1"),
        Decimal("0.5"),
    )
    assert other_size.version == 1
    assert reverse_parameters.version == 1
    assert calibrator.get(forward, Decimal("0.01")) == changed


def test_calibration_rejects_an_unqualified_window() -> None:
    calibrator = AdaptiveGridCalibrator(5, Decimal("0.20"))
    route = DirectedRouteKey("ETH", Venue.BYBIT, Venue.OKX)
    with pytest.raises(ValueError, match=ReasonCode.CALIBRATION_INSUFFICIENT.value):
        calibrator.calibrate(
            route,
            Decimal("0.1"),
            observations("1", "2"),
            Decimal("1"),
            Decimal("1"),
            Decimal("0.1"),
        )


def test_cost_model_and_signal_expose_complete_numeric_breakdown() -> None:
    calibrator = AdaptiveGridCalibrator(5, Decimal("0.20"))
    parameters = calibrator.calibrate(
        DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX),
        Decimal("1"),
        observations("10", "11", "12", "13", "14"),
        Decimal("1"),
        Decimal("1"),
        Decimal("1"),
    )
    inputs = cost_inputs()
    cost = calculate_stressed_cost(inputs)
    assert cost.four_leg_fees_usdt == Decimal("0.419")
    assert cost.stressed_total_cost_usdt == Decimal("1.219")
    assert cost.expected_gross_pnl_usdt == Decimal("9")
    assert cost.expected_net_pnl_usdt == Decimal("7.781")

    decision = evaluate_entry_signal(
        inputs,
        parameters,
        Decimal("2"),
        True,
        ReasonCode.RISK_RESERVED,
        {"projected_route_stress_usdt": Decimal("4")},
    )
    assert decision.accepted is True
    assert decision.reason == ReasonCode.ENTRY_ACCEPTED
    assert decision.inputs["required_gross_pnl_usdt"] == Decimal("2.438")
    assert decision.risk_breakdown["projected_route_stress_usdt"] == 4


def test_signal_fails_closed_on_cost_profit_and_risk() -> None:
    calibrator = AdaptiveGridCalibrator(3, Decimal("0.20"))
    parameters = calibrator.calibrate(
        DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX),
        Decimal("1"),
        observations("1", "2", "3"),
        Decimal("1"),
        Decimal("1"),
        Decimal("8"),
    )
    below_cost = evaluate_entry_signal(
        cost_inputs("102"),
        parameters,
        Decimal("2"),
        True,
        ReasonCode.RISK_RESERVED,
        {},
    )
    assert below_cost.reason == ReasonCode.GROSS_BELOW_COST_FLOOR

    below_profit = evaluate_entry_signal(
        cost_inputs(),
        parameters,
        Decimal("1"),
        True,
        ReasonCode.RISK_RESERVED,
        {},
    )
    assert below_profit.reason == ReasonCode.NET_BELOW_MINIMUM

    parameters_low_profit = calibrator.calibrate(
        DirectedRouteKey("ETH", Venue.BYBIT, Venue.OKX),
        Decimal("1"),
        observations("1", "2", "3"),
        Decimal("1"),
        Decimal("1"),
        Decimal("1"),
    )
    risk_rejected = evaluate_entry_signal(
        cost_inputs(),
        parameters_low_profit,
        Decimal("1"),
        False,
        ReasonCode.PAIR_STRESS_LIMIT,
        {"projected_route_stress_usdt": Decimal("6")},
    )
    assert risk_rejected.reason == ReasonCode.PAIR_STRESS_LIMIT
    assert risk_rejected.accepted is False
