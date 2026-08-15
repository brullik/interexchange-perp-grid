from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from math import gcd

from interexchange_perp_grid.domain import (
    FundingSnapshot,
    Instrument,
    OrderBookSnapshot,
    Venue,
)
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.private_execution import protected_ioc_price
from interexchange_perp_grid.qualification import QualifiedStrategyParameters
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.risk import RiskDecision
from interexchange_perp_grid.routes import DirectedRouteQuote, executable_vwap
from interexchange_perp_grid.strategy import (
    CostInputs,
    DirectedRouteKey,
    GridParameters,
    SignalDecision,
    evaluate_entry_signal,
)

_INTERVAL = re.compile(r"^(?P<count>[1-9][0-9]*)(?P<unit>[mhd])$")


@dataclass(frozen=True, slots=True)
class LiveEconomicPolicy:
    entry_slippage_cap_bps: Decimal
    latency_reserve_bps: Decimal
    partial_fill_reserve_bps: Decimal
    emergency_hedge_reserve_bps: Decimal
    reconciliation_forced_exit_reserve_bps: Decimal
    funding_stress_multiplier: Decimal
    minimum_profit_usdt: Decimal

    def __post_init__(self) -> None:
        reserves = (
            self.entry_slippage_cap_bps,
            self.latency_reserve_bps,
            self.partial_fill_reserve_bps,
            self.emergency_hedge_reserve_bps,
            self.reconciliation_forced_exit_reserve_bps,
        )
        if any(not value.is_finite() or value < 0 for value in reserves):
            raise ValueError("live economic bps values must be non-negative and finite")
        if self.funding_stress_multiplier < 1:
            raise ValueError("funding stress multiplier must be at least one")
        if self.minimum_profit_usdt <= 0:
            raise ValueError("live minimum profit must be positive")


@dataclass(frozen=True, slots=True)
class LiveEconomicDecision:
    accepted: bool
    reason: ReasonCode
    route: DirectedRouteKey
    signal: SignalDecision | None
    long_protected_price: Decimal | None
    short_protected_price: Decimal | None
    long_marginal_price: Decimal | None
    short_marginal_price: Decimal | None
    emergency_venue: EmergencyVenueAssessment | None = None


@dataclass(frozen=True, slots=True)
class EmergencyVenueAssessment:
    passed: bool
    reason: ReasonCode
    venue: Venue
    fee_rate: Decimal | None
    residual_quantum: Decimal
    residual_quantities: tuple[Decimal, ...]
    maximum_residual_quantity: Decimal
    buy_marginal_price: Decimal | None
    sell_marginal_price: Decimal | None
    buy_protected_price: Decimal | None
    sell_protected_price: Decimal | None
    worst_hedge_and_flatten_cost_usdt: Decimal
    rounding_loss_base_quantity: Decimal
    checks: dict[str, bool]


def evaluate_emergency_venue(
    long_instrument: Instrument,
    short_instrument: Instrument,
    emergency_instrument: Instrument,
    emergency_book: OrderBookSnapshot,
    private_taker_fee_rate: Decimal | None,
    quantity: Decimal,
    *,
    capability_ready: bool,
    account_ready: bool,
    data_quality_ready: bool,
    slippage_cap_bps: Decimal,
) -> EmergencyVenueAssessment:
    """Prove every possible main-leg residual is executable on the third venue."""
    main_quantum = _decimal_gcd(
        long_instrument.base_amount_step,
        short_instrument.base_amount_step,
    )
    residual_count = int(quantity / main_quantum)
    residuals = tuple(main_quantum * index for index in range(1, residual_count + 1))
    metadata_ready = (
        quantity > 0
        and residual_count > 0
        and residual_count <= 10_000
        and long_instrument.base == short_instrument.base == emergency_instrument.base
        and long_instrument.settle
        == short_instrument.settle
        == emergency_instrument.settle
        == "USDT"
        and emergency_book.venue == emergency_instrument.venue
        and emergency_book.symbol == emergency_instrument.symbol
    )
    fee_ready = (
        private_taker_fee_rate is not None
        and private_taker_fee_rate.is_finite()
        and private_taker_fee_rate >= 0
    )
    step_ready = bool(residuals) and all(
        _quantity_is_executable(residual, emergency_instrument, emergency_book)
        for residual in residuals
    )
    depth_ready = metadata_ready and bool(emergency_book.bids) and bool(emergency_book.asks)
    costs: list[Decimal] = []
    buy_marginal: Decimal | None = None
    sell_marginal: Decimal | None = None
    if depth_ready and step_ready and fee_ready:
        assert private_taker_fee_rate is not None
        for residual in residuals:
            buy = executable_vwap(emergency_book.asks, residual)
            sell = executable_vwap(emergency_book.bids, residual)
            if buy is None or sell is None:
                depth_ready = False
                break
            buy_cap = protected_ioc_price(
                Side.BUY,
                buy.marginal_price,
                emergency_instrument.price_tick,
                slippage_cap_bps,
            )
            sell_cap = protected_ioc_price(
                Side.SELL,
                sell.marginal_price,
                emergency_instrument.price_tick,
                slippage_cap_bps,
            )
            round_trip_cost = residual * (
                max(Decimal(0), buy_cap - sell_cap) + private_taker_fee_rate * (buy_cap + sell_cap)
            )
            costs.append(round_trip_cost)
            buy_marginal = buy.marginal_price
            sell_marginal = sell.marginal_price
    checks = {
        "capability": capability_ready,
        "account": account_ready,
        "data_quality": data_quality_ready,
        "instrument_metadata": metadata_ready,
        "private_fee": fee_ready,
        "all_residual_steps": step_ready,
        "both_side_depth": depth_ready and len(costs) == len(residuals),
    }
    passed = all(checks.values())
    reason = (
        ReasonCode.ENTRY_ACCEPTED
        if passed
        else (
            ReasonCode.RESIDUAL_NOT_EXECUTABLE
            if not step_ready
            else ReasonCode.EMERGENCY_VENUE_PREFLIGHT_FAILED
        )
    )
    buy_protected = (
        protected_ioc_price(
            Side.BUY,
            buy_marginal,
            emergency_instrument.price_tick,
            slippage_cap_bps,
        )
        if passed and buy_marginal is not None
        else None
    )
    sell_protected = (
        protected_ioc_price(
            Side.SELL,
            sell_marginal,
            emergency_instrument.price_tick,
            slippage_cap_bps,
        )
        if passed and sell_marginal is not None
        else None
    )
    return EmergencyVenueAssessment(
        passed=passed,
        reason=reason,
        venue=emergency_instrument.venue,
        fee_rate=private_taker_fee_rate,
        residual_quantum=main_quantum,
        residual_quantities=residuals,
        maximum_residual_quantity=max(residuals, default=Decimal(0)),
        buy_marginal_price=buy_marginal,
        sell_marginal_price=sell_marginal,
        buy_protected_price=buy_protected,
        sell_protected_price=sell_protected,
        worst_hedge_and_flatten_cost_usdt=max(costs, default=Decimal(0)),
        rounding_loss_base_quantity=Decimal(0) if step_ready else main_quantum,
        checks=checks,
    )


def evaluate_live_entry(
    quote: DirectedRouteQuote,
    long_instrument: Instrument,
    short_instrument: Instrument,
    long_book: OrderBookSnapshot,
    short_book: OrderBookSnapshot,
    long_funding: FundingSnapshot,
    short_funding: FundingSnapshot,
    private_taker_fee_rates: dict[Venue, Decimal],
    qualified: QualifiedStrategyParameters,
    policy: LiveEconomicPolicy,
    risk: RiskDecision,
    *,
    emergency_assessment: EmergencyVenueAssessment | None = None,
) -> LiveEconomicDecision:
    route = DirectedRouteKey(quote.key.base, quote.long_venue, quote.short_venue)
    required_values = (
        quote.entry_long_vwap,
        quote.entry_short_vwap,
        quote.exit_long_vwap,
        quote.exit_short_vwap,
        quote.entry_long_marginal_price,
        quote.entry_short_marginal_price,
    )
    fees = (
        private_taker_fee_rates.get(route.long_venue),
        private_taker_fee_rates.get(route.short_venue),
    )
    funding_values = (
        long_funding.rate,
        short_funding.rate,
        long_funding.interval,
        short_funding.interval,
        long_funding.next_funding_timestamp_ms,
        short_funding.next_funding_timestamp_ms,
        long_funding.exchange_timestamp_ms,
        short_funding.exchange_timestamp_ms,
    )
    if (
        not quote.eligible
        or any(value is None for value in required_values)
        or any(value is None for value in fees)
        or any(value is None for value in funding_values)
        or quote.base_quantity != qualified.size_bucket_base_quantity
        or (emergency_assessment is not None and not emergency_assessment.passed)
    ):
        return LiveEconomicDecision(
            False,
            ReasonCode.ECONOMIC_PREFLIGHT_FAILED,
            route,
            None,
            None,
            None,
            quote.entry_long_marginal_price,
            quote.entry_short_marginal_price,
            emergency_assessment,
        )
    assert quote.entry_long_vwap is not None
    assert quote.entry_short_vwap is not None
    assert quote.exit_long_vwap is not None
    assert quote.exit_short_vwap is not None
    assert quote.entry_long_marginal_price is not None
    assert quote.entry_short_marginal_price is not None
    assert long_funding.rate is not None
    assert short_funding.rate is not None
    assert long_funding.interval is not None
    assert short_funding.interval is not None
    long_fee, short_fee = fees
    assert long_fee is not None
    assert short_fee is not None

    quantity = quote.base_quantity
    notional = quantity * max(quote.entry_long_vwap, quote.entry_short_vwap)
    target_spread = quote.entry_long_vwap * qualified.target_exit_spread_bps / Decimal(10_000)
    target_exit_long = quote.entry_long_vwap
    target_exit_short = target_exit_long + target_spread
    entry_impact = quantity * (
        max(Decimal(0), quote.entry_long_vwap - long_book.asks[0].price)
        + max(Decimal(0), short_book.bids[0].price - quote.entry_short_vwap)
    )
    exit_impact = quantity * (
        max(Decimal(0), long_book.bids[0].price - quote.exit_long_vwap)
        + max(Decimal(0), quote.exit_short_vwap - short_book.asks[0].price)
    )
    expected_funding = _funding_cost(
        notional,
        long_funding,
        short_funding,
        qualified.expected_holding_seconds,
        stress=False,
        multiplier=Decimal(1),
    )
    funding_stress = _funding_cost(
        notional,
        long_funding,
        short_funding,
        qualified.maximum_holding_seconds,
        stress=True,
        multiplier=policy.funding_stress_multiplier,
    )
    bps_unit = notional / Decimal(10_000)
    inputs = CostInputs(
        quantity=quantity,
        entry_long_price=quote.entry_long_vwap,
        entry_short_price=quote.entry_short_vwap,
        target_exit_long_price=target_exit_long,
        target_exit_short_price=target_exit_short,
        long_fee_rate=long_fee,
        short_fee_rate=short_fee,
        entry_impact_usdt=entry_impact,
        exit_impact_usdt=exit_impact,
        expected_funding_cost_usdt=expected_funding,
        funding_stress_usdt=funding_stress,
        latency_reserve_usdt=bps_unit * policy.latency_reserve_bps,
        unmatched_hedge_reserve_usdt=Decimal(0),
        reconciliation_forced_exit_reserve_usdt=(
            bps_unit * policy.reconciliation_forced_exit_reserve_bps
        ),
        liquidation_distance_reserve_usdt=Decimal(0),
        partial_fill_reserve_usdt=bps_unit * policy.partial_fill_reserve_bps,
        emergency_hedge_reserve_usdt=(
            bps_unit * policy.emergency_hedge_reserve_bps
            + (
                emergency_assessment.worst_hedge_and_flatten_cost_usdt
                if emergency_assessment is not None
                else Decimal(0)
            )
        ),
    )
    parameters = GridParameters(
        route=route,
        size_bucket=qualified.size_bucket_base_quantity,
        version=qualified.calibration_version,
        sample_count=0,
        median_spread_bps=qualified.adaptive_entry_threshold_bps,
        mad_spread_bps=Decimal(0),
        entry_quantile_bps=qualified.adaptive_entry_threshold_bps,
        exit_quantile_bps=qualified.target_exit_spread_bps,
        convergence_p90_seconds=Decimal(qualified.expected_holding_seconds),
        grid_step_bps=qualified.adaptive_entry_threshold_bps,
        minimum_profit_usdt=max(qualified.minimum_profit_usdt, policy.minimum_profit_usdt),
    )
    signal = evaluate_entry_signal(
        inputs,
        parameters,
        qualified.stressed_cost_multiplier,
        risk.accepted,
        risk.reason,
        risk.breakdown,
    )
    long_protected = protected_ioc_price(
        Side.BUY,
        quote.entry_long_marginal_price,
        long_instrument.price_tick,
        policy.entry_slippage_cap_bps,
    )
    short_protected = protected_ioc_price(
        Side.SELL,
        quote.entry_short_marginal_price,
        short_instrument.price_tick,
        policy.entry_slippage_cap_bps,
    )
    return LiveEconomicDecision(
        accepted=signal.accepted and signal.cost.expected_net_pnl_usdt > 0,
        reason=signal.reason,
        route=route,
        signal=signal,
        long_protected_price=long_protected,
        short_protected_price=short_protected,
        long_marginal_price=quote.entry_long_marginal_price,
        short_marginal_price=quote.entry_short_marginal_price,
        emergency_venue=emergency_assessment,
    )


def _decimal_gcd(first: Decimal, second: Decimal) -> Decimal:
    if not first.is_finite() or not second.is_finite() or first <= 0 or second <= 0:
        raise ValueError("residual amount steps must be positive and finite")
    first_exponent = first.normalize().as_tuple().exponent
    second_exponent = second.normalize().as_tuple().exponent
    if not isinstance(first_exponent, int) or not isinstance(second_exponent, int):
        raise ValueError("residual amount steps must be finite")
    places = max(0, -first_exponent, -second_exponent)
    scale = 10**places
    return Decimal(gcd(int(first * scale), int(second * scale))) / Decimal(scale)


def _quantity_is_executable(
    quantity: Decimal,
    instrument: Instrument,
    book: OrderBookSnapshot,
) -> bool:
    step = instrument.base_amount_step
    if step <= 0 or quantity < instrument.minimum_base_amount:
        return False
    units = quantity / step
    if units != units.to_integral_value():
        return False
    if instrument.minimum_notional is None:
        return instrument.no_fixed_minimum_notional
    if not book.bids or not book.asks:
        return False
    return quantity * min(book.bids[0].price, book.asks[0].price) >= instrument.minimum_notional


def _funding_cost(
    notional: Decimal,
    long_funding: FundingSnapshot,
    short_funding: FundingSnapshot,
    holding_seconds: int,
    *,
    stress: bool,
    multiplier: Decimal,
) -> Decimal:
    assert long_funding.rate is not None
    assert short_funding.rate is not None
    long_periods = _funding_payments(long_funding, holding_seconds)
    short_periods = _funding_payments(short_funding, holding_seconds)
    if stress:
        base = abs(long_funding.rate) * long_periods + abs(short_funding.rate) * short_periods
    else:
        base = max(
            Decimal(0),
            long_funding.rate * long_periods - short_funding.rate * short_periods,
        )
    return notional * base * multiplier


def _funding_payments(snapshot: FundingSnapshot, holding_seconds: int) -> Decimal:
    if (
        snapshot.interval is None
        or snapshot.next_funding_timestamp_ms is None
        or snapshot.exchange_timestamp_ms is None
    ):
        raise ValueError("funding schedule is incomplete")
    seconds_until_next = max(
        0,
        (snapshot.next_funding_timestamp_ms - snapshot.exchange_timestamp_ms) // 1_000,
    )
    if holding_seconds < seconds_until_next:
        return Decimal(0)
    interval_seconds = _interval_seconds(snapshot.interval)
    return Decimal(1 + (holding_seconds - seconds_until_next) // interval_seconds)


def _interval_seconds(value: str) -> int:
    matched = _INTERVAL.fullmatch(value.strip().lower())
    if matched is None:
        raise ValueError(f"unsupported funding interval: {value!r}")
    count = int(matched.group("count"))
    unit = matched.group("unit")
    multiplier = {"m": 60, "h": 3_600, "d": 86_400}[unit]
    return count * multiplier
