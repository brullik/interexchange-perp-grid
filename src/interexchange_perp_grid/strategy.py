from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.reason_codes import ReasonCode


@dataclass(frozen=True, slots=True, order=True)
class DirectedRouteKey:
    base: str
    long_venue: Venue
    short_venue: Venue

    def __post_init__(self) -> None:
        if not self.base.strip():
            raise ValueError("route base must be non-empty")
        if self.long_venue == self.short_venue:
            raise ValueError("directed route venues must differ")

    @property
    def value(self) -> str:
        return f"{self.base}:{self.long_venue.value}>{self.short_venue.value}"


@dataclass(frozen=True, slots=True)
class CalibrationObservation:
    spread_bps: Decimal
    convergence_seconds: Decimal | None

    def __post_init__(self) -> None:
        if not self.spread_bps.is_finite():
            raise ValueError("spread must be finite")
        if self.convergence_seconds is not None and (
            not self.convergence_seconds.is_finite() or self.convergence_seconds < 0
        ):
            raise ValueError("convergence time must be finite and non-negative")


@dataclass(frozen=True, slots=True)
class GridParameters:
    route: DirectedRouteKey
    size_bucket: Decimal
    version: int
    sample_count: int
    median_spread_bps: Decimal
    mad_spread_bps: Decimal
    entry_quantile_bps: Decimal
    exit_quantile_bps: Decimal
    convergence_p90_seconds: Decimal | None
    grid_step_bps: Decimal
    minimum_profit_usdt: Decimal


def _quantile(values: tuple[Decimal, ...], probability: Decimal) -> Decimal:
    if not values:
        raise ValueError("quantile requires observations")
    if probability < 0 or probability > 1:
        raise ValueError("quantile probability must be between zero and one")
    ordered = tuple(sorted(values))
    if len(ordered) == 1:
        return ordered[0]
    position = probability * Decimal(len(ordered) - 1)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - Decimal(lower)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


class AdaptiveGridCalibrator:
    """Robust, versioned calibration isolated by directed route and size bucket."""

    def __init__(self, minimum_samples: int, parameter_change_limit_ratio: Decimal) -> None:
        if minimum_samples < 3:
            raise ValueError("calibration requires at least three samples")
        if parameter_change_limit_ratio <= 0 or parameter_change_limit_ratio > Decimal("0.50"):
            raise ValueError("invalid parameter change limit")
        self._minimum_samples = minimum_samples
        self._change_limit = parameter_change_limit_ratio
        self._parameters: dict[tuple[DirectedRouteKey, Decimal], GridParameters] = {}

    def get(self, route: DirectedRouteKey, size_bucket: Decimal) -> GridParameters | None:
        return self._parameters.get((route, size_bucket))

    def calibrate(
        self,
        route: DirectedRouteKey,
        size_bucket: Decimal,
        observations: tuple[CalibrationObservation, ...],
        cost_floor_bps: Decimal,
        liquidity_floor_bps: Decimal,
        minimum_profit_usdt: Decimal,
    ) -> GridParameters:
        if size_bucket <= 0:
            raise ValueError("size bucket must be positive")
        if len(observations) < self._minimum_samples:
            raise ValueError(ReasonCode.CALIBRATION_INSUFFICIENT.value)
        if min(cost_floor_bps, liquidity_floor_bps, minimum_profit_usdt) < 0:
            raise ValueError("calibration floors must be non-negative")

        spreads = tuple(observation.spread_bps for observation in observations)
        median = _quantile(spreads, Decimal("0.50"))
        deviations = tuple(abs(value - median) for value in spreads)
        mad = _quantile(deviations, Decimal("0.50"))
        robust_volatility = mad * Decimal("1.4826") * Decimal(2)
        raw_step = max(cost_floor_bps, liquidity_floor_bps, robust_volatility)

        previous = self.get(route, size_bucket)
        if previous is None:
            version = 1
            bounded_step = raw_step
        else:
            version = previous.version + 1
            lower = previous.grid_step_bps * (Decimal(1) - self._change_limit)
            upper = previous.grid_step_bps * (Decimal(1) + self._change_limit)
            bounded_step = min(max(raw_step, lower), upper)

        convergence = tuple(
            observation.convergence_seconds
            for observation in observations
            if observation.convergence_seconds is not None
        )
        parameters = GridParameters(
            route=route,
            size_bucket=size_bucket,
            version=version,
            sample_count=len(observations),
            median_spread_bps=median,
            mad_spread_bps=mad,
            entry_quantile_bps=_quantile(spreads, Decimal("0.75")),
            exit_quantile_bps=_quantile(spreads, Decimal("0.25")),
            convergence_p90_seconds=(
                _quantile(convergence, Decimal("0.90")) if convergence else None
            ),
            grid_step_bps=bounded_step,
            minimum_profit_usdt=minimum_profit_usdt,
        )
        self._parameters[(route, size_bucket)] = parameters
        return parameters


@dataclass(frozen=True, slots=True)
class CostInputs:
    quantity: Decimal
    entry_long_price: Decimal
    entry_short_price: Decimal
    target_exit_long_price: Decimal
    target_exit_short_price: Decimal
    long_fee_rate: Decimal
    short_fee_rate: Decimal
    entry_impact_usdt: Decimal
    exit_impact_usdt: Decimal
    expected_funding_cost_usdt: Decimal
    funding_stress_usdt: Decimal
    latency_reserve_usdt: Decimal
    unmatched_hedge_reserve_usdt: Decimal
    reconciliation_forced_exit_reserve_usdt: Decimal
    liquidation_distance_reserve_usdt: Decimal
    precomputed_four_leg_fee_usdt: Decimal | None = None

    def __post_init__(self) -> None:
        positive = (
            self.quantity,
            self.entry_long_price,
            self.entry_short_price,
            self.target_exit_long_price,
            self.target_exit_short_price,
        )
        non_negative = (
            self.long_fee_rate,
            self.short_fee_rate,
            self.entry_impact_usdt,
            self.exit_impact_usdt,
            self.expected_funding_cost_usdt,
            self.funding_stress_usdt,
            self.latency_reserve_usdt,
            self.unmatched_hedge_reserve_usdt,
            self.reconciliation_forced_exit_reserve_usdt,
            self.liquidation_distance_reserve_usdt,
        )
        if any(not value.is_finite() or value <= 0 for value in positive):
            raise ValueError("quantity and prices must be positive finite decimals")
        if any(not value.is_finite() or value < 0 for value in non_negative):
            raise ValueError("cost inputs must be non-negative finite decimals")
        if self.precomputed_four_leg_fee_usdt is not None and (
            not self.precomputed_four_leg_fee_usdt.is_finite()
            or self.precomputed_four_leg_fee_usdt < 0
        ):
            raise ValueError("precomputed fees must be non-negative and finite")


@dataclass(frozen=True, slots=True)
class CostBreakdown:
    four_leg_fees_usdt: Decimal
    entry_exit_impact_usdt: Decimal
    expected_funding_cost_usdt: Decimal
    funding_stress_usdt: Decimal
    latency_reserve_usdt: Decimal
    unmatched_hedge_reserve_usdt: Decimal
    reconciliation_forced_exit_reserve_usdt: Decimal
    liquidation_distance_reserve_usdt: Decimal
    stressed_total_cost_usdt: Decimal
    expected_gross_pnl_usdt: Decimal
    expected_net_pnl_usdt: Decimal


def calculate_stressed_cost(inputs: CostInputs) -> CostBreakdown:
    fees = (
        inputs.precomputed_four_leg_fee_usdt
        if inputs.precomputed_four_leg_fee_usdt is not None
        else inputs.quantity
        * (
            (inputs.entry_long_price + inputs.target_exit_long_price) * inputs.long_fee_rate
            + (inputs.entry_short_price + inputs.target_exit_short_price) * inputs.short_fee_rate
        )
    )
    impact = inputs.entry_impact_usdt + inputs.exit_impact_usdt
    total = (
        fees
        + impact
        + inputs.expected_funding_cost_usdt
        + inputs.funding_stress_usdt
        + inputs.latency_reserve_usdt
        + inputs.unmatched_hedge_reserve_usdt
        + inputs.reconciliation_forced_exit_reserve_usdt
        + inputs.liquidation_distance_reserve_usdt
    )
    gross = inputs.quantity * (
        (inputs.entry_short_price - inputs.entry_long_price)
        - (inputs.target_exit_short_price - inputs.target_exit_long_price)
    )
    return CostBreakdown(
        four_leg_fees_usdt=fees,
        entry_exit_impact_usdt=impact,
        expected_funding_cost_usdt=inputs.expected_funding_cost_usdt,
        funding_stress_usdt=inputs.funding_stress_usdt,
        latency_reserve_usdt=inputs.latency_reserve_usdt,
        unmatched_hedge_reserve_usdt=inputs.unmatched_hedge_reserve_usdt,
        reconciliation_forced_exit_reserve_usdt=(inputs.reconciliation_forced_exit_reserve_usdt),
        liquidation_distance_reserve_usdt=inputs.liquidation_distance_reserve_usdt,
        stressed_total_cost_usdt=total,
        expected_gross_pnl_usdt=gross,
        expected_net_pnl_usdt=gross - total,
    )


@dataclass(frozen=True, slots=True)
class SignalDecision:
    accepted: bool
    reason: ReasonCode
    calibration_version: int
    inputs: dict[str, Decimal]
    cost: CostBreakdown
    risk_breakdown: dict[str, Decimal]


def evaluate_entry_signal(
    inputs: CostInputs,
    parameters: GridParameters,
    cost_multiplier: Decimal,
    risk_accepted: bool,
    risk_reason: ReasonCode,
    risk_breakdown: dict[str, Decimal],
) -> SignalDecision:
    if cost_multiplier < 1:
        raise ValueError("cost multiplier must be at least one")
    cost = calculate_stressed_cost(inputs)
    required_gross = cost_multiplier * cost.stressed_total_cost_usdt
    if cost.expected_gross_pnl_usdt < required_gross:
        reason = ReasonCode.GROSS_BELOW_COST_FLOOR
    elif cost.expected_net_pnl_usdt < parameters.minimum_profit_usdt:
        reason = ReasonCode.NET_BELOW_MINIMUM
    elif not risk_accepted:
        reason = risk_reason
    else:
        reason = ReasonCode.ENTRY_ACCEPTED
    return SignalDecision(
        accepted=reason == ReasonCode.ENTRY_ACCEPTED,
        reason=reason,
        calibration_version=parameters.version,
        inputs={
            "quantity": inputs.quantity,
            "entry_long_price": inputs.entry_long_price,
            "entry_short_price": inputs.entry_short_price,
            "target_exit_long_price": inputs.target_exit_long_price,
            "target_exit_short_price": inputs.target_exit_short_price,
            "cost_multiplier": cost_multiplier,
            "required_gross_pnl_usdt": required_gross,
        },
        cost=cost,
        risk_breakdown=dict(risk_breakdown),
    )
