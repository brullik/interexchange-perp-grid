from __future__ import annotations

import hashlib
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from decimal import ROUND_FLOOR, Decimal, localcontext
from enum import StrEnum
from pathlib import Path

import yaml

from interexchange_perp_grid.aggressive_model import DivergenceDirection
from interexchange_perp_grid.domain import OrderBookSnapshot, Venue
from interexchange_perp_grid.routes import executable_vwap

_BPS = Decimal("10000")


class AggressiveEntryReason(StrEnum):
    ACCEPTED = "ACCEPTED"
    REFERENCE_NOT_CROSSED = "REFERENCE_NOT_CROSSED"
    CONFIRMATION_INSUFFICIENT = "CONFIRMATION_INSUFFICIENT"
    BOOK_UNAVAILABLE = "BOOK_UNAVAILABLE"
    BOOK_STALE = "BOOK_STALE"
    BOOK_UNSYNCHRONISED = "BOOK_UNSYNCHRONISED"
    DEPTH_INSUFFICIENT = "DEPTH_INSUFFICIENT"
    PRIVATE_FEE_UNKNOWN = "PRIVATE_FEE_UNKNOWN"
    FUNDING_UNKNOWN = "FUNDING_UNKNOWN"
    CONVERGENCE_NON_POSITIVE = "CONVERGENCE_NON_POSITIVE"
    ECONOMICS_INSUFFICIENT = "ECONOMICS_INSUFFICIENT"
    RISK_REJECTED = "RISK_REJECTED"
    ROUTE_IDENTITY_MISMATCH = "ROUTE_IDENTITY_MISMATCH"
    STATE_UNHEALTHY = "STATE_UNHEALTHY"
    HISTORICAL_MODEL_INELIGIBLE = "HISTORICAL_MODEL_INELIGIBLE"
    REGIME_BLOCKED = "REGIME_BLOCKED"


class AggressiveEntryStage(StrEnum):
    NORMAL = "NORMAL"
    LOCKED_CANARY = "LOCKED_CANARY"


class AggressiveExitReason(StrEnum):
    NONE = "NONE"
    EMERGENCY_OR_UNKNOWN = "EMERGENCY_OR_UNKNOWN"
    HARD_PROJECTED_LOSS_OR_REFERENCE_STOP = "HARD_PROJECTED_LOSS_OR_REFERENCE_STOP"
    HARD_HOLDING_TIME = "HARD_HOLDING_TIME"
    ADVERSE_FUNDING = "ADVERSE_FUNDING"
    REVERSE_GRID_TARGET = "REVERSE_GRID_TARGET"


@dataclass(frozen=True, slots=True)
class AggressiveDecisionPolicy:
    confirmation_snapshots: int
    confirmation_minimum_elapsed_ms: int
    stressed_cost_multiplier: Decimal
    normal_minimum_expected_net_profit_usdt: Decimal
    canary_minimum_expected_net_profit_usdt: Decimal
    positive_funding_credit_ratio: Decimal
    adverse_funding_charge_ratio: Decimal
    funding_stress_multiplier: Decimal
    route_modelled_loss_limit_usdt: Decimal
    route_hard_projected_loss_limit_usdt: Decimal
    portfolio_modelled_loss_limit_usdt: Decimal
    portfolio_hard_projected_loss_limit_usdt: Decimal
    local_free_margin_floor_ratio: Decimal
    initial_effective_leverage_cap: Decimal
    hard_max_hold_seconds: int

    def __post_init__(self) -> None:
        if self.confirmation_snapshots != 3 or self.confirmation_minimum_elapsed_ms != 500:
            raise ValueError("aggressive confirmation policy must remain 3 snapshots / 500 ms")
        if self.stressed_cost_multiplier != Decimal("1.35"):
            raise ValueError("aggressive cost multiplier must remain 1.35")
        if self.normal_minimum_expected_net_profit_usdt != Decimal("0.15"):
            raise ValueError("normal minimum expected profit must remain 0.15 USDT")
        if self.canary_minimum_expected_net_profit_usdt != Decimal("0.01"):
            raise ValueError("canary minimum expected profit must remain 0.01 USDT")
        if (
            self.positive_funding_credit_ratio != Decimal("0.50")
            or self.adverse_funding_charge_ratio != Decimal("1.00")
            or self.funding_stress_multiplier != Decimal("2.00")
        ):
            raise ValueError("aggressive funding treatment does not match locked policy")
        if (
            self.route_modelled_loss_limit_usdt != Decimal("4.50")
            or self.route_hard_projected_loss_limit_usdt != Decimal("5.00")
            or self.portfolio_modelled_loss_limit_usdt != Decimal("45.00")
            or self.portfolio_hard_projected_loss_limit_usdt != Decimal("50.00")
        ):
            raise ValueError("aggressive risk limits do not match locked policy")
        if not 0 < self.local_free_margin_floor_ratio < 1:
            raise ValueError("local free-margin floor must be within (0, 1)")
        if self.initial_effective_leverage_cap <= 0 or self.hard_max_hold_seconds <= 0:
            raise ValueError("leverage cap and holding limit must be positive")


@dataclass(frozen=True, slots=True)
class LoadedAggressiveDecisionPolicy:
    policy: AggressiveDecisionPolicy
    profile_sha256: str


@dataclass(frozen=True, slots=True)
class CostReserves:
    entry_impact_usdt: Decimal
    exit_impact_usdt: Decimal
    entry_slippage_usdt: Decimal
    exit_slippage_usdt: Decimal
    latency_usdt: Decimal
    partial_fill_unmatched_usdt: Decimal
    emergency_hedge_usdt: Decimal
    reconciliation_forced_exit_usdt: Decimal
    liquidation_distance_usdt: Decimal

    def total(self) -> Decimal:
        values = (
            self.entry_impact_usdt,
            self.exit_impact_usdt,
            self.entry_slippage_usdt,
            self.exit_slippage_usdt,
            self.latency_usdt,
            self.partial_fill_unmatched_usdt,
            self.emergency_hedge_usdt,
            self.reconciliation_forced_exit_usdt,
            self.liquidation_distance_usdt,
        )
        if any(not value.is_finite() or value < 0 for value in values):
            raise ValueError("economic reserves must be non-negative and finite")
        return sum(values, Decimal(0))


@dataclass(frozen=True, slots=True)
class VenueFundingProjection:
    venue: Venue
    rate: Decimal
    mark_price: Decimal
    event_count: int
    next_funding_timestamp_ms: int
    interval_seconds: int

    def __post_init__(self) -> None:
        if (
            not self.rate.is_finite()
            or not self.mark_price.is_finite()
            or self.mark_price <= 0
            or self.event_count < 0
            or self.next_funding_timestamp_ms <= 0
            or self.interval_seconds <= 0
        ):
            raise ValueError("funding projection is incomplete or invalid")


@dataclass(frozen=True, slots=True)
class HybridEntryInput:
    route_identity: str
    direction: DivergenceDirection
    level_index: int
    reference_spread_bps: Decimal
    reference_trigger_bps: Decimal
    reverse_target_bps: Decimal
    quantity: Decimal
    long_venue: Venue
    short_venue: Venue
    long_book: OrderBookSnapshot
    short_book: OrderBookSnapshot
    long_private_taker_fee_rate: Decimal | None
    short_private_taker_fee_rate: Decimal | None
    long_funding: VenueFundingProjection | None
    short_funding: VenueFundingProjection | None
    reserves: CostReserves
    observed_monotonic_ns: int
    maximum_book_age_ms: int
    now: datetime
    stage: AggressiveEntryStage = AggressiveEntryStage.NORMAL
    state_reconciled: bool = True
    historical_model_eligible: bool = True
    regime_ready: bool = True


@dataclass(frozen=True, slots=True)
class AggressiveEconomicDecision:
    accepted: bool
    reason: AggressiveEntryReason
    route_identity: str
    level_index: int
    executable_entry_spread_bps: Decimal | None
    long_entry_vwap: Decimal | None
    short_entry_vwap: Decimal | None
    four_leg_fees_usdt: Decimal
    stressed_total_cost_usdt: Decimal
    favorable_funding_credit_usdt: Decimal
    expected_gross_convergence_pnl_usdt: Decimal
    expected_net_pnl_usdt: Decimal
    execution_authorized: bool = field(default=False, init=False)


@dataclass(frozen=True, slots=True)
class GridSizingResult:
    accepted: bool
    reason: AggressiveEntryReason
    full_route_base_quantity: Decimal
    tranche_base_quantities: tuple[Decimal, ...]
    projected_route_loss_usdt: Decimal
    projected_portfolio_loss_usdt: Decimal
    projected_margin_usdt: Decimal
    execution_authorized: bool = field(default=False, init=False)


@dataclass(frozen=True, slots=True)
class AggressiveExitInput:
    direction: DivergenceDirection
    executable_spread_bps: Decimal
    effective_stop_bps: Decimal
    reverse_target_bps: Decimal
    projected_route_loss_usdt: Decimal
    projected_portfolio_loss_usdt: Decimal
    holding_deadline: datetime
    now: datetime
    emergency_or_unknown: bool
    adverse_funding_destroys_profit: bool


@dataclass(frozen=True, slots=True)
class RouteScoreCandidate:
    route_identity: str
    score: Decimal
    executable_depth: Decimal
    total_slippage: Decimal
    data_latency_ms: Decimal
    total_fee: Decimal
    adverse_funding: Decimal


class CrossingConfirmationTracker:
    def __init__(self, snapshots: int, minimum_elapsed_ms: int) -> None:
        if snapshots <= 0 or minimum_elapsed_ms < 0:
            raise ValueError("confirmation tracker policy is invalid")
        self._snapshots = snapshots
        self._minimum_elapsed_ns = minimum_elapsed_ms * 1_000_000
        self._observations: dict[tuple[str, int], deque[int]] = {}

    def observe(
        self,
        route_identity: str,
        level_index: int,
        *,
        crossed: bool,
        synchronized: bool,
        observed_monotonic_ns: int,
    ) -> bool:
        key = (route_identity, level_index)
        if not crossed or not synchronized:
            self._observations.pop(key, None)
            return False
        observations = self._observations.setdefault(key, deque(maxlen=self._snapshots))
        if observations and observed_monotonic_ns <= observations[-1]:
            self._observations.pop(key, None)
            return False
        observations.append(observed_monotonic_ns)
        return (
            len(observations) == self._snapshots
            and observations[-1] - observations[0] >= self._minimum_elapsed_ns
        )

    def clear(self, route_identity: str, level_index: int) -> None:
        self._observations.pop((route_identity, level_index), None)


def evaluate_hybrid_entry(
    proposal: HybridEntryInput,
    *,
    policy: AggressiveDecisionPolicy,
    confirmations: CrossingConfirmationTracker,
) -> AggressiveEconomicDecision:
    empty = _rejected(proposal, AggressiveEntryReason.REFERENCE_NOT_CROSSED)
    if not _route_matches_books(proposal):
        return _rejected(proposal, AggressiveEntryReason.ROUTE_IDENTITY_MISMATCH)
    if not proposal.state_reconciled:
        return _rejected(proposal, AggressiveEntryReason.STATE_UNHEALTHY)
    if not proposal.historical_model_eligible:
        return _rejected(proposal, AggressiveEntryReason.HISTORICAL_MODEL_INELIGIBLE)
    if not proposal.regime_ready:
        return _rejected(proposal, AggressiveEntryReason.REGIME_BLOCKED)
    crossed = (
        proposal.reference_spread_bps >= proposal.reference_trigger_bps
        if proposal.direction == DivergenceDirection.POSITIVE
        else proposal.reference_spread_bps <= proposal.reference_trigger_bps
    )
    if not crossed:
        confirmations.observe(
            proposal.route_identity,
            proposal.level_index,
            crossed=False,
            synchronized=False,
            observed_monotonic_ns=proposal.observed_monotonic_ns,
        )
        return empty
    quality_reason = _book_quality_reason(proposal)
    if quality_reason is not None:
        confirmations.observe(
            proposal.route_identity,
            proposal.level_index,
            crossed=True,
            synchronized=False,
            observed_monotonic_ns=proposal.observed_monotonic_ns,
        )
        return _rejected(proposal, quality_reason)
    confirmed = confirmations.observe(
        proposal.route_identity,
        proposal.level_index,
        crossed=True,
        synchronized=True,
        observed_monotonic_ns=proposal.observed_monotonic_ns,
    )
    if not confirmed:
        return _rejected(proposal, AggressiveEntryReason.CONFIRMATION_INSUFFICIENT)
    if (
        proposal.long_private_taker_fee_rate is None
        or proposal.short_private_taker_fee_rate is None
    ):
        return _rejected(proposal, AggressiveEntryReason.PRIVATE_FEE_UNKNOWN)
    fees = (proposal.long_private_taker_fee_rate, proposal.short_private_taker_fee_rate)
    if any(not fee.is_finite() or fee < 0 for fee in fees):
        return _rejected(proposal, AggressiveEntryReason.PRIVATE_FEE_UNKNOWN)
    if proposal.long_funding is None or proposal.short_funding is None:
        return _rejected(proposal, AggressiveEntryReason.FUNDING_UNKNOWN)
    if proposal.quantity <= 0 or not proposal.quantity.is_finite():
        return _rejected(proposal, AggressiveEntryReason.DEPTH_INSUFFICIENT)
    long_fill = executable_vwap(proposal.long_book.asks, proposal.quantity)
    short_fill = executable_vwap(proposal.short_book.bids, proposal.quantity)
    if long_fill is None or short_fill is None:
        return _rejected(proposal, AggressiveEntryReason.DEPTH_INSUFFICIENT)
    with localcontext() as context:
        context.prec = 50
        executable_spread = (short_fill.price / long_fill.price).ln() * _BPS
    average_price = (long_fill.price + short_fill.price) / Decimal(2)
    gross = (
        proposal.quantity
        * average_price
        * abs(executable_spread - proposal.reverse_target_bps)
        / _BPS
    )
    if gross <= 0:
        return _rejected(proposal, AggressiveEntryReason.CONVERGENCE_NON_POSITIVE)
    four_leg_fees = (
        Decimal(2) * proposal.quantity * (long_fill.price * fees[0] + short_fill.price * fees[1])
    )
    if (
        proposal.long_funding.venue != proposal.long_venue
        or proposal.short_funding.venue != proposal.short_venue
    ):
        return _rejected(proposal, AggressiveEntryReason.FUNDING_UNKNOWN)
    funding = projected_net_funding_usdt(
        proposal.long_funding,
        proposal.short_funding,
        proposal.quantity,
    )
    favorable_credit = max(Decimal(0), funding) * policy.positive_funding_credit_ratio
    adverse_charge = (
        max(Decimal(0), -funding)
        * policy.adverse_funding_charge_ratio
        * policy.funding_stress_multiplier
    )
    stressed_cost = four_leg_fees + proposal.reserves.total() + adverse_charge
    net = gross - stressed_cost + favorable_credit
    minimum_profit = (
        policy.canary_minimum_expected_net_profit_usdt
        if proposal.stage == AggressiveEntryStage.LOCKED_CANARY
        else policy.normal_minimum_expected_net_profit_usdt
    )
    accepted = gross >= policy.stressed_cost_multiplier * stressed_cost and net >= minimum_profit
    return AggressiveEconomicDecision(
        accepted=accepted,
        reason=(
            AggressiveEntryReason.ACCEPTED
            if accepted
            else AggressiveEntryReason.ECONOMICS_INSUFFICIENT
        ),
        route_identity=proposal.route_identity,
        level_index=proposal.level_index,
        executable_entry_spread_bps=executable_spread,
        long_entry_vwap=long_fill.price,
        short_entry_vwap=short_fill.price,
        four_leg_fees_usdt=four_leg_fees,
        stressed_total_cost_usdt=stressed_cost,
        favorable_funding_credit_usdt=favorable_credit,
        expected_gross_convergence_pnl_usdt=gross,
        expected_net_pnl_usdt=net,
    )


def size_aggressive_grid(
    *,
    direction_levels_bps: tuple[Decimal, ...],
    tranche_weights: tuple[Decimal, ...],
    effective_stop_bps: Decimal,
    reference_price: Decimal,
    quantity_step: Decimal,
    minimum_base_quantity: Decimal,
    minimum_notional_usdt: Decimal,
    per_full_base_reserve_usdt: Decimal,
    existing_route_loss_usdt: Decimal,
    existing_portfolio_loss_usdt: Decimal,
    free_margin_usdt: Decimal,
    policy: AggressiveDecisionPolicy,
) -> GridSizingResult:
    values = (
        effective_stop_bps,
        reference_price,
        quantity_step,
        minimum_base_quantity,
        minimum_notional_usdt,
        per_full_base_reserve_usdt,
        existing_route_loss_usdt,
        existing_portfolio_loss_usdt,
        free_margin_usdt,
    )
    if any(not value.is_finite() for value in values) or reference_price <= 0 or quantity_step <= 0:
        raise ValueError("grid sizing inputs are invalid")
    if len(direction_levels_bps) != 5 or len(tranche_weights) != 5 or sum(tranche_weights) != 1:
        raise ValueError("grid sizing requires exact five-level weights")
    weighted_stop_distance = sum(
        weight * abs(effective_stop_bps - level)
        for level, weight in zip(direction_levels_bps, tranche_weights, strict=True)
    )
    loss_per_full_base = (
        reference_price * weighted_stop_distance / _BPS + per_full_base_reserve_usdt
    )
    route_remaining = policy.route_modelled_loss_limit_usdt - existing_route_loss_usdt
    portfolio_remaining = policy.portfolio_modelled_loss_limit_usdt - existing_portfolio_loss_usdt
    margin_capacity = (
        free_margin_usdt
        * (Decimal(1) - policy.local_free_margin_floor_ratio)
        * policy.initial_effective_leverage_cap
        / reference_price
    )
    if loss_per_full_base <= 0 or route_remaining <= 0 or portfolio_remaining <= 0:
        return _risk_rejected(existing_route_loss_usdt, existing_portfolio_loss_usdt)
    raw_quantity = min(
        route_remaining / loss_per_full_base,
        portfolio_remaining / loss_per_full_base,
        margin_capacity,
    )
    full_quantity = _round_down(raw_quantity, quantity_step)
    tranche_quantities = tuple(
        _round_down(full_quantity * weight, quantity_step) for weight in tranche_weights
    )
    minimum = max(minimum_base_quantity, minimum_notional_usdt / reference_price)
    tranche_quantities = tuple(
        quantity if quantity >= minimum else Decimal(0) for quantity in tranche_quantities
    )
    effective_full_quantity = sum(tranche_quantities, Decimal(0))
    projected_route = existing_route_loss_usdt + effective_full_quantity * loss_per_full_base
    projected_portfolio = (
        existing_portfolio_loss_usdt + effective_full_quantity * loss_per_full_base
    )
    projected_margin = (
        effective_full_quantity * reference_price / policy.initial_effective_leverage_cap
    )
    accepted = (
        effective_full_quantity > 0
        and projected_route <= policy.route_modelled_loss_limit_usdt
        and projected_route < policy.route_hard_projected_loss_limit_usdt
        and projected_portfolio <= policy.portfolio_modelled_loss_limit_usdt
        and projected_portfolio < policy.portfolio_hard_projected_loss_limit_usdt
        and projected_margin <= free_margin_usdt * (1 - policy.local_free_margin_floor_ratio)
    )
    return GridSizingResult(
        accepted=accepted,
        reason=AggressiveEntryReason.ACCEPTED if accepted else AggressiveEntryReason.RISK_REJECTED,
        full_route_base_quantity=effective_full_quantity,
        tranche_base_quantities=tranche_quantities,
        projected_route_loss_usdt=projected_route,
        projected_portfolio_loss_usdt=projected_portfolio,
        projected_margin_usdt=projected_margin,
    )


def projected_net_funding_usdt(
    long_funding: VenueFundingProjection,
    short_funding: VenueFundingProjection,
    base_quantity: Decimal,
) -> Decimal:
    if not base_quantity.is_finite() or base_quantity <= 0:
        raise ValueError("funding base quantity must be positive and finite")
    long_payment = -(
        base_quantity * long_funding.mark_price * long_funding.rate * long_funding.event_count
    )
    short_payment = (
        base_quantity * short_funding.mark_price * short_funding.rate * short_funding.event_count
    )
    return long_payment + short_payment


def route_score(
    *,
    convergence_probability: Decimal,
    expected_net_profit_usdt: Decimal,
    projected_stress_usdt: Decimal,
    expected_holding_hours: Decimal,
) -> Decimal:
    if projected_stress_usdt <= 0 or any(
        not value.is_finite()
        for value in (
            convergence_probability,
            expected_net_profit_usdt,
            projected_stress_usdt,
            expected_holding_hours,
        )
    ):
        raise ValueError("route score inputs are invalid")
    return (
        convergence_probability
        * expected_net_profit_usdt
        / (projected_stress_usdt * max(expected_holding_hours, Decimal("0.25")))
    )


def select_route_candidate(
    candidates: tuple[RouteScoreCandidate, ...],
) -> RouteScoreCandidate | None:
    if not candidates:
        return None
    for candidate in candidates:
        values = (
            candidate.score,
            candidate.executable_depth,
            candidate.total_slippage,
            candidate.data_latency_ms,
            candidate.total_fee,
            candidate.adverse_funding,
        )
        if not candidate.route_identity or any(not value.is_finite() for value in values):
            raise ValueError("route candidate score is incomplete or non-finite")
    return min(
        candidates,
        key=lambda candidate: (
            -candidate.score,
            -candidate.executable_depth,
            candidate.total_slippage,
            candidate.data_latency_ms,
            candidate.total_fee,
            candidate.adverse_funding,
            candidate.route_identity,
        ),
    )


def select_aggressive_exit_reason(
    state: AggressiveExitInput,
    policy: AggressiveDecisionPolicy,
) -> AggressiveExitReason:
    if state.emergency_or_unknown:
        return AggressiveExitReason.EMERGENCY_OR_UNKNOWN
    stop_crossed = (
        state.executable_spread_bps >= state.effective_stop_bps
        if state.direction == DivergenceDirection.POSITIVE
        else state.executable_spread_bps <= state.effective_stop_bps
    )
    if (
        stop_crossed
        or state.projected_route_loss_usdt >= policy.route_hard_projected_loss_limit_usdt
        or state.projected_portfolio_loss_usdt >= policy.portfolio_hard_projected_loss_limit_usdt
    ):
        return AggressiveExitReason.HARD_PROJECTED_LOSS_OR_REFERENCE_STOP
    if state.now >= state.holding_deadline:
        return AggressiveExitReason.HARD_HOLDING_TIME
    if state.adverse_funding_destroys_profit:
        return AggressiveExitReason.ADVERSE_FUNDING
    reverse_crossed = (
        state.executable_spread_bps <= state.reverse_target_bps
        if state.direction == DivergenceDirection.POSITIVE
        else state.executable_spread_bps >= state.reverse_target_bps
    )
    return (
        AggressiveExitReason.REVERSE_GRID_TARGET if reverse_crossed else AggressiveExitReason.NONE
    )


def load_aggressive_decision_policy(path: Path) -> LoadedAggressiveDecisionPolicy:
    raw = path.read_bytes()
    loaded = yaml.safe_load(raw)
    if not isinstance(loaded, dict):
        raise ValueError("aggressive profile must be a mapping")
    grid = _mapping(loaded, "aggressive_grid")
    economics = _mapping(loaded, "entry_economics")
    risk = _mapping(loaded, "risk")
    _exact(grid, "one_tranche_per_decision_cycle", True)
    _exact(grid, "sequential_catch_up_after_gap", True)
    _exact(grid, "require_fresh_books_between_catch_up_tranches", True)
    _exact(grid, "reverse_grid_exit", True)
    _exact(economics, "convergence_pnl_must_be_positive_without_positive_funding", True)
    _exact(economics, "use_actual_private_taker_fees_when_available", True)
    _exact(economics, "unknown_fee_or_funding_blocks_entry", True)
    _exact(risk, "size_from_exchange_max_leverage", False)
    policy = AggressiveDecisionPolicy(
        confirmation_snapshots=_integer(grid, "confirmation_snapshots"),
        confirmation_minimum_elapsed_ms=_integer(grid, "confirmation_minimum_elapsed_ms"),
        stressed_cost_multiplier=_decimal(economics, "stressed_cost_multiplier"),
        normal_minimum_expected_net_profit_usdt=_decimal(
            economics, "normal_minimum_expected_net_profit_usdt"
        ),
        canary_minimum_expected_net_profit_usdt=_decimal(
            economics, "canary_minimum_expected_net_profit_usdt"
        ),
        positive_funding_credit_ratio=_decimal(economics, "positive_funding_credit_ratio"),
        adverse_funding_charge_ratio=_decimal(economics, "adverse_funding_charge_ratio"),
        funding_stress_multiplier=_decimal(economics, "funding_stress_multiplier"),
        route_modelled_loss_limit_usdt=_decimal(risk, "route_modelled_loss_limit_usdt"),
        route_hard_projected_loss_limit_usdt=_decimal(risk, "route_hard_projected_loss_limit_usdt"),
        portfolio_modelled_loss_limit_usdt=_decimal(risk, "portfolio_modelled_loss_limit_usdt"),
        portfolio_hard_projected_loss_limit_usdt=_decimal(
            risk, "portfolio_hard_projected_loss_limit_usdt"
        ),
        local_free_margin_floor_ratio=_decimal(risk, "local_free_margin_floor_ratio"),
        initial_effective_leverage_cap=_decimal(risk, "initial_effective_leverage_cap"),
        hard_max_hold_seconds=_integer(risk, "hard_max_hold_seconds"),
    )
    return LoadedAggressiveDecisionPolicy(policy, hashlib.sha256(raw).hexdigest())


def _book_quality_reason(proposal: HybridEntryInput) -> AggressiveEntryReason | None:
    books = (proposal.long_book, proposal.short_book)
    if (
        proposal.long_book.venue != proposal.long_venue
        or proposal.short_book.venue != proposal.short_venue
        or not all(book.bids and book.asks for book in books)
    ):
        return AggressiveEntryReason.BOOK_UNAVAILABLE
    if not all(book.synchronised and book.sequence_contiguous for book in books):
        return AggressiveEntryReason.BOOK_UNSYNCHRONISED
    if proposal.now.tzinfo is None or proposal.now.utcoffset() is None:
        return AggressiveEntryReason.BOOK_STALE
    if any(
        proposal.observed_monotonic_ns < book.received_monotonic_ns
        or proposal.observed_monotonic_ns - book.received_monotonic_ns
        > proposal.maximum_book_age_ms * 1_000_000
        for book in books
    ):
        return AggressiveEntryReason.BOOK_STALE
    return None


def _route_matches_books(proposal: HybridEntryInput) -> bool:
    try:
        _, venues = proposal.route_identity.split(":", maxsplit=1)
        long_value, short_value = venues.split(">", maxsplit=1)
    except ValueError:
        return False
    return (long_value, short_value) == (
        proposal.long_venue.value,
        proposal.short_venue.value,
    )


def _rejected(
    proposal: HybridEntryInput,
    reason: AggressiveEntryReason,
) -> AggressiveEconomicDecision:
    return AggressiveEconomicDecision(
        accepted=False,
        reason=reason,
        route_identity=proposal.route_identity,
        level_index=proposal.level_index,
        executable_entry_spread_bps=None,
        long_entry_vwap=None,
        short_entry_vwap=None,
        four_leg_fees_usdt=Decimal(0),
        stressed_total_cost_usdt=Decimal(0),
        favorable_funding_credit_usdt=Decimal(0),
        expected_gross_convergence_pnl_usdt=Decimal(0),
        expected_net_pnl_usdt=Decimal(0),
    )


def _risk_rejected(route_loss: Decimal, portfolio_loss: Decimal) -> GridSizingResult:
    return GridSizingResult(
        accepted=False,
        reason=AggressiveEntryReason.RISK_REJECTED,
        full_route_base_quantity=Decimal(0),
        tranche_base_quantities=(Decimal(0),) * 5,
        projected_route_loss_usdt=route_loss,
        projected_portfolio_loss_usdt=portfolio_loss,
        projected_margin_usdt=Decimal(0),
    )


def _round_down(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def _mapping(parent: dict[object, object], key: str) -> dict[object, object]:
    value = parent.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"aggressive profile requires mapping {key}")
    return value


def _exact(parent: dict[object, object], key: str, expected: object) -> None:
    if parent.get(key) != expected:
        raise ValueError(f"aggressive profile value {key} does not match locked policy")


def _integer(parent: dict[object, object], key: str) -> int:
    value = parent.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"aggressive profile requires integer {key}")
    return value


def _decimal(parent: dict[object, object], key: str) -> Decimal:
    if key not in parent or isinstance(parent[key], bool):
        raise ValueError(f"aggressive profile requires decimal {key}")
    try:
        value = Decimal(str(parent[key]))
    except Exception as error:
        raise ValueError(f"aggressive profile decimal {key} is invalid") from error
    if not value.is_finite():
        raise ValueError(f"aggressive profile decimal {key} must be finite")
    return value
