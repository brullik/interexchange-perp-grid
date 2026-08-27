from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from enum import StrEnum

from interexchange_perp_grid.aggressive_evaluator import (
    AggressiveDecisionPolicy,
    AggressiveEconomicDecision,
    AggressiveEntryReason,
    AggressiveEntryStage,
    CostReserves,
    CrossingConfirmationTracker,
    GridSizingResult,
    HybridEntryInput,
    canonical_executable_spread_bps,
    evaluate_hybrid_entry,
    revalidate_hybrid_entry_once,
    size_aggressive_grid,
)
from interexchange_perp_grid.aggressive_grid import (
    AggressiveGridStore,
    FrozenGridSizingPlan,
    GridLevelState,
)
from interexchange_perp_grid.aggressive_model import (
    DirectionHistoricalModel,
    DivergenceDirection,
    HistoricalReferenceModel,
    ModelEligibility,
    historical_model_sha256,
)

_BPS = Decimal("10000")


class AggressiveRuntimeMode(StrEnum):
    REPLAY = "REPLAY"
    SHADOW = "SHADOW"
    LIVE = "LIVE"


def aggressive_runtime_manifest_sha256(
    model: HistoricalReferenceModel,
    config_sha256: str,
) -> str:
    """Bind the shared decision runtime to exact code/model/profile/config identity."""
    if len(config_sha256) != 64:
        raise ValueError("aggressive runtime config digest must be SHA-256")
    material = "|".join(
        (
            model.code_sha,
            historical_model_sha256(model),
            model.strategy_profile_sha256,
            config_sha256,
        )
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def projected_tranche_loss_at_current_economics(
    intent: AggressiveTrancheIntent,
    economics: AggressiveEconomicDecision,
) -> Decimal:
    """Reprice one immutable intent with the latest executable cost evidence."""
    if (
        economics.executable_entry_spread_bps is None
        or economics.long_entry_vwap is None
        or economics.short_entry_vwap is None
        or any(
            not value.is_finite() or value <= 0
            for value in (
                intent.quantity,
                economics.long_entry_vwap,
                economics.short_entry_vwap,
            )
        )
        or not economics.stressed_total_cost_usdt.is_finite()
        or economics.stressed_total_cost_usdt < 0
        or not economics.executable_entry_spread_bps.is_finite()
        or not intent.effective_stop_bps.is_finite()
        or not intent.reference_trigger_bps.is_finite()
    ):
        raise ValueError("current aggressive tranche risk evidence is invalid")
    average_price = (economics.long_entry_vwap + economics.short_entry_vwap) / Decimal(2)
    stop_loss = (
        intent.quantity
        * average_price
        * abs(intent.effective_stop_bps - economics.executable_entry_spread_bps)
        / _BPS
    )
    return stop_loss + economics.stressed_total_cost_usdt


class ReplayMinuteOutcome(StrEnum):
    NONE = "NONE"
    TARGET = "TARGET"
    STOP = "STOP"


@dataclass(frozen=True, slots=True)
class AggressiveSizingInput:
    quantity_step: Decimal
    minimum_base_quantity: Decimal
    minimum_notional_usdt: Decimal
    per_full_base_reserve_usdt: Decimal
    existing_route_loss_usdt: Decimal
    existing_portfolio_loss_usdt: Decimal
    free_margin_usdt: Decimal
    frozen_route_sizing: FrozenGridSizingPlan | None = None


@dataclass(frozen=True, slots=True)
class AggressiveStrategyRequest:
    mode: AggressiveRuntimeMode
    model: HistoricalReferenceModel
    proposal: HybridEntryInput
    sizing: AggressiveSizingInput
    reserves_per_base: CostReserves
    effective_stop_bps: Decimal
    decision_cycle: int
    runtime_manifest_sha256: str
    state_reconciled: bool = True
    reference_identity_valid: bool = True

    def __post_init__(self) -> None:
        if self.decision_cycle < 0:
            raise ValueError("aggressive decision cycle must be non-negative")
        if not self.runtime_manifest_sha256:
            raise ValueError("aggressive runtime manifest identity is required")


@dataclass(frozen=True, slots=True)
class AggressiveTrancheIntent:
    base: str
    route_identity: str
    direction: DivergenceDirection
    level_index: int
    decision_cycle: int
    quantity: Decimal
    long_venue: str
    short_venue: str
    long_symbol: str
    short_symbol: str
    reference_interval_start: datetime
    reference_trigger_bps: Decimal
    reference_spread_bps: Decimal
    grid_step_bps: Decimal
    stressed_cost_move_bps: Decimal
    minimum_profit_move_bps: Decimal
    normal_low_bps: Decimal
    normal_high_bps: Decimal
    reserves: CostReserves
    entry_stage: AggressiveEntryStage
    adverse_funding_reserve_usdt: Decimal
    remaining_close_fees_usdt: Decimal
    executable_entry_spread_bps: Decimal
    reverse_target_bps: Decimal
    effective_stop_bps: Decimal
    long_entry_vwap: Decimal
    short_entry_vwap: Decimal
    projected_route_loss_usdt: Decimal
    projected_portfolio_loss_usdt: Decimal
    incremental_tranche_loss_usdt: Decimal
    expected_net_pnl_usdt: Decimal
    model_sha256: str
    strategy_profile_sha256: str
    source_manifest_sha256: str
    reference_manifest_sha256: str
    runtime_manifest_sha256: str
    contract_metadata_version_a: str
    contract_metadata_version_b: str
    decided_at: datetime
    execution_authorized: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        if (
            self.decided_at.tzinfo is None
            or self.decided_at.utcoffset() is None
            or self.reference_interval_start.tzinfo is None
            or self.reference_interval_start.utcoffset() is None
            or self.reference_interval_start.second != 0
            or self.reference_interval_start.microsecond != 0
        ):
            raise ValueError("aggressive intent timestamp must be timezone-aware")
        if not 1 <= self.level_index <= 5 or self.quantity <= 0:
            raise ValueError("aggressive intent level or quantity is invalid")
        identities = (
            self.base,
            self.route_identity,
            self.long_venue,
            self.short_venue,
            self.long_symbol,
            self.short_symbol,
            self.model_sha256,
            self.strategy_profile_sha256,
            self.source_manifest_sha256,
            self.reference_manifest_sha256,
            self.runtime_manifest_sha256,
            self.contract_metadata_version_a,
            self.contract_metadata_version_b,
        )
        if any(not value for value in identities):
            raise ValueError("aggressive intent identity is incomplete")
        numbers = (
            self.quantity,
            self.reference_trigger_bps,
            self.reference_spread_bps,
            self.grid_step_bps,
            self.stressed_cost_move_bps,
            self.minimum_profit_move_bps,
            self.normal_low_bps,
            self.normal_high_bps,
            self.adverse_funding_reserve_usdt,
            self.remaining_close_fees_usdt,
            self.executable_entry_spread_bps,
            self.reverse_target_bps,
            self.effective_stop_bps,
            self.long_entry_vwap,
            self.short_entry_vwap,
            self.projected_route_loss_usdt,
            self.projected_portfolio_loss_usdt,
            self.incremental_tranche_loss_usdt,
            self.expected_net_pnl_usdt,
        )
        if any(not value.is_finite() for value in numbers):
            raise ValueError("aggressive intent contains a non-finite value")
        self.reserves.total()
        if self.execution_authorized:
            raise ValueError("strategy intent never authorizes exchange execution")

    @property
    def intent_id(self) -> str:
        material = "|".join(
            (
                self.route_identity,
                str(self.level_index),
                str(self.decision_cycle),
                str(self.quantity),
                self.model_sha256,
                self.runtime_manifest_sha256,
            )
        )
        return f"aggressive-{hashlib.sha256(material.encode('utf-8')).hexdigest()[:32]}"


@dataclass(frozen=True, slots=True)
class AggressiveStrategyDecision:
    accepted: bool
    reason: AggressiveEntryReason
    economics: AggressiveEconomicDecision
    sizing: GridSizingResult
    intent: AggressiveTrancheIntent | None
    execution_authorized: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        if self.accepted != (self.intent is not None):
            raise ValueError("accepted aggressive decision must own exactly one intent")
        if self.execution_authorized:
            raise ValueError("strategy decision never authorizes exchange execution")


@dataclass(frozen=True, slots=True)
class ActualFillRiskInput:
    direction: DivergenceDirection
    base_quantity: Decimal
    long_fill_price: Decimal
    short_fill_price: Decimal
    actual_fees_usdt: Decimal
    adverse_funding_usdt: Decimal
    other_reserves_usdt: Decimal
    effective_stop_bps: Decimal
    existing_route_loss_usdt: Decimal
    existing_portfolio_loss_usdt: Decimal


@dataclass(frozen=True, slots=True)
class ActualFillRiskResult:
    accepted: bool
    projected_route_loss_usdt: Decimal
    projected_portfolio_loss_usdt: Decimal
    actual_entry_spread_bps: Decimal


class AggressiveDecisionCore:
    """One deterministic decision core shared by replay, shadow and live intent creation."""

    def __init__(self, policy: AggressiveDecisionPolicy) -> None:
        self.policy = policy
        self._confirmations = CrossingConfirmationTracker(
            policy.confirmation_snapshots,
            policy.confirmation_minimum_elapsed_ms,
        )

    def evaluate(self, request: AggressiveStrategyRequest) -> AggressiveStrategyDecision:
        direction_model = _direction_model(request)
        expected_route = (
            request.model.positive_route
            if request.proposal.direction == DivergenceDirection.POSITIVE
            else request.model.negative_route
        )
        level_index = request.proposal.level_index
        reference_price = Decimal(1)
        if request.proposal.long_book.asks and request.proposal.short_book.bids:
            reference_price = (
                request.proposal.long_book.asks[0].price + request.proposal.short_book.bids[0].price
            ) / Decimal(2)
        frozen = request.sizing.frozen_route_sizing
        if frozen is None:
            sizing = size_aggressive_grid(
                direction_levels_bps=direction_model.levels_bps,
                tranche_weights=direction_model.tranche_weights,
                effective_stop_bps=request.effective_stop_bps,
                reference_price=reference_price,
                quantity_step=request.sizing.quantity_step,
                minimum_base_quantity=request.sizing.minimum_base_quantity,
                minimum_notional_usdt=request.sizing.minimum_notional_usdt,
                per_full_base_reserve_usdt=request.sizing.per_full_base_reserve_usdt,
                existing_route_loss_usdt=request.sizing.existing_route_loss_usdt,
                existing_portfolio_loss_usdt=request.sizing.existing_portfolio_loss_usdt,
                free_margin_usdt=request.sizing.free_margin_usdt,
                policy=self.policy,
            )
        else:
            if (
                frozen.route_identity != expected_route
                or frozen.model_sha256 != historical_model_sha256(request.model)
            ):
                raise ValueError("frozen route sizing identity changed")
            remaining_loss = sum(
                frozen.tranche_projected_losses_usdt[level_index - 1 :],
                Decimal(0),
            )
            projected_route = request.sizing.existing_route_loss_usdt + remaining_loss
            projected_portfolio = request.sizing.existing_portfolio_loss_usdt + remaining_loss
            next_quantity = frozen.tranche_base_quantities[level_index - 1]
            next_margin = (
                next_quantity * reference_price / self.policy.initial_effective_leverage_cap
            )
            frozen_accepted = (
                next_quantity > 0
                and projected_route <= self.policy.route_modelled_loss_limit_usdt
                and projected_route < self.policy.route_hard_projected_loss_limit_usdt
                and projected_portfolio <= self.policy.portfolio_modelled_loss_limit_usdt
                and projected_portfolio < self.policy.portfolio_hard_projected_loss_limit_usdt
                and next_margin
                <= request.sizing.free_margin_usdt
                * (Decimal(1) - self.policy.local_free_margin_floor_ratio)
            )
            sizing = GridSizingResult(
                accepted=frozen_accepted,
                reason=(
                    AggressiveEntryReason.ACCEPTED
                    if frozen_accepted
                    else AggressiveEntryReason.RISK_REJECTED
                ),
                full_route_base_quantity=frozen.full_route_base_quantity,
                tranche_base_quantities=frozen.tranche_base_quantities,
                tranche_projected_losses_usdt=frozen.tranche_projected_losses_usdt,
                projected_route_loss_usdt=projected_route,
                projected_portfolio_loss_usdt=projected_portfolio,
                projected_margin_usdt=frozen.projected_margin_usdt,
            )
        canary_minimum = max(
            request.sizing.minimum_base_quantity,
            request.sizing.minimum_notional_usdt / reference_price,
        )
        quantity = (
            (canary_minimum / request.sizing.quantity_step).to_integral_value(
                rounding=ROUND_CEILING
            )
            * request.sizing.quantity_step
            if request.proposal.stage == AggressiveEntryStage.LOCKED_CANARY
            else sizing.tranche_base_quantities[level_index - 1]
            if sizing.accepted and 1 <= level_index <= len(sizing.tranche_base_quantities)
            else request.proposal.quantity
        )
        grid_step = direction_model.range_bps / Decimal(5)
        geometry_matches = (
            1 <= level_index <= 5
            and request.proposal.route_identity == expected_route
            and request.proposal.reference_trigger_bps
            == direction_model.levels_bps[level_index - 1]
            and request.proposal.grid_step_bps == grid_step
            and request.proposal.normal_low_bps == request.model.normal_low_bps
            and request.proposal.normal_high_bps == request.model.normal_high_bps
        )
        live_eligible = direction_model.eligibility == ModelEligibility.LIVE_ELIGIBLE
        model_eligible = (
            direction_model.eligibility != ModelEligibility.DISABLED
            and (request.mode != AggressiveRuntimeMode.LIVE or live_eligible)
            and geometry_matches
            and request.reference_identity_valid
        )
        proposal = replace(
            request.proposal,
            quantity=quantity,
            reserves=_scale_reserves(request.reserves_per_base, quantity),
            state_reconciled=request.state_reconciled,
            historical_model_eligible=model_eligible,
            regime_ready=not direction_model.regime_drift_blocked,
        )
        economics = evaluate_hybrid_entry(
            proposal,
            policy=self.policy,
            confirmations=self._confirmations,
        )
        if (
            economics.accepted
            and sizing.accepted
            and frozen is None
            and request.proposal.stage == AggressiveEntryStage.NORMAL
        ):
            for _iteration in range(5):
                resized = _resize_for_complete_executable_risk(
                    request,
                    sizing,
                    direction_model,
                    reference_price,
                    self.policy,
                )
                resized_quantity = (
                    resized.tranche_base_quantities[level_index - 1]
                    if resized.accepted
                    else Decimal(0)
                )
                sizing = resized
                if resized_quantity <= 0:
                    sizing = _runtime_risk_rejected(request)
                    break
                if resized_quantity == quantity:
                    break
                if resized_quantity > quantity:
                    raise RuntimeError("executable-risk sizing attempted to increase exposure")
                quantity = resized_quantity
                proposal = replace(
                    proposal,
                    quantity=quantity,
                    reserves=_scale_reserves(request.reserves_per_base, quantity),
                )
                economics = revalidate_hybrid_entry_once(proposal, policy=self.policy)
                if not economics.accepted:
                    break
            else:
                sizing = _runtime_risk_rejected(request)
        elif (
            economics.accepted
            and sizing.accepted
            and frozen is None
            and request.proposal.stage == AggressiveEntryStage.LOCKED_CANARY
        ):
            canary_quantities = tuple(
                quantity if index == level_index else Decimal(0) for index in range(1, 6)
            )
            sizing = _evaluate_complete_executable_grid_risk(
                request,
                replace(
                    sizing,
                    full_route_base_quantity=quantity,
                    tranche_base_quantities=canary_quantities,
                ),
                direction_model,
                reference_price,
                self.policy,
            )
        elif economics.accepted and sizing.accepted and frozen is not None:
            sizing = _evaluate_complete_executable_grid_risk(
                request,
                sizing,
                direction_model,
                reference_price,
                self.policy,
            )
        if not economics.accepted or not sizing.accepted:
            reason = (
                AggressiveEntryReason.RISK_REJECTED if not sizing.accepted else economics.reason
            )
            return AggressiveStrategyDecision(False, reason, economics, sizing, None)
        if (
            economics.executable_entry_spread_bps is None
            or economics.long_entry_vwap is None
            or economics.short_entry_vwap is None
        ):
            raise RuntimeError("accepted aggressive economics is incomplete")
        complete_tranche_losses = sizing.tranche_projected_losses_usdt
        incremental_tranche_loss = complete_tranche_losses[level_index - 1]
        projected_route_loss = (
            request.sizing.existing_route_loss_usdt + incremental_tranche_loss
            if request.proposal.stage == AggressiveEntryStage.LOCKED_CANARY
            else request.sizing.existing_route_loss_usdt
            + sum(complete_tranche_losses[level_index - 1 :], Decimal(0))
        )
        projected_portfolio_loss = (
            request.sizing.existing_portfolio_loss_usdt + incremental_tranche_loss
            if request.proposal.stage == AggressiveEntryStage.LOCKED_CANARY
            else request.sizing.existing_portfolio_loss_usdt
            + sum(complete_tranche_losses[level_index - 1 :], Decimal(0))
        )
        sizing = replace(
            sizing,
            tranche_projected_losses_usdt=complete_tranche_losses,
            projected_route_loss_usdt=projected_route_loss,
            projected_portfolio_loss_usdt=projected_portfolio_loss,
        )
        if request.proposal.stage == AggressiveEntryStage.LOCKED_CANARY and (
            incremental_tranche_loss > Decimal(1)
            or projected_route_loss > Decimal(1)
            or projected_portfolio_loss > Decimal(1)
        ):
            return AggressiveStrategyDecision(
                False,
                AggressiveEntryReason.RISK_REJECTED,
                economics,
                sizing,
                None,
            )
        if request.proposal.stage == AggressiveEntryStage.NORMAL and (
            projected_route_loss > self.policy.route_modelled_loss_limit_usdt
            or projected_route_loss >= self.policy.route_hard_projected_loss_limit_usdt
            or projected_portfolio_loss > self.policy.portfolio_modelled_loss_limit_usdt
            or projected_portfolio_loss >= self.policy.portfolio_hard_projected_loss_limit_usdt
        ):
            return AggressiveStrategyDecision(
                False,
                AggressiveEntryReason.RISK_REJECTED,
                economics,
                sizing,
                None,
            )
        if (
            economics.reverse_target_bps is None
            or economics.long_entry_vwap is None
            or economics.short_entry_vwap is None
        ):
            raise RuntimeError("accepted aggressive target evidence is incomplete")
        intent = AggressiveTrancheIntent(
            base=request.model.base,
            route_identity=expected_route,
            direction=proposal.direction,
            level_index=level_index,
            decision_cycle=request.decision_cycle,
            quantity=quantity,
            long_venue=proposal.long_venue.value,
            short_venue=proposal.short_venue.value,
            long_symbol=proposal.long_book.symbol,
            short_symbol=proposal.short_book.symbol,
            reference_interval_start=(
                proposal.reference_interval_start
                if proposal.reference_interval_start is not None
                else proposal.now.replace(second=0, microsecond=0) - timedelta(minutes=1)
            ),
            reference_trigger_bps=proposal.reference_trigger_bps,
            reference_spread_bps=proposal.reference_spread_bps,
            grid_step_bps=proposal.grid_step_bps,
            stressed_cost_move_bps=proposal.stressed_cost_move_bps,
            minimum_profit_move_bps=proposal.minimum_profit_move_bps,
            normal_low_bps=proposal.normal_low_bps,
            normal_high_bps=proposal.normal_high_bps,
            reserves=proposal.reserves,
            entry_stage=proposal.stage,
            adverse_funding_reserve_usdt=max(
                Decimal(0),
                economics.stressed_total_cost_usdt
                - economics.four_leg_fees_usdt
                - economics.measured_book_impact_usdt
                - proposal.reserves.total(),
            ),
            remaining_close_fees_usdt=economics.remaining_close_fees_usdt,
            executable_entry_spread_bps=economics.executable_entry_spread_bps,
            reverse_target_bps=economics.reverse_target_bps,
            effective_stop_bps=request.effective_stop_bps,
            long_entry_vwap=economics.long_entry_vwap,
            short_entry_vwap=economics.short_entry_vwap,
            projected_route_loss_usdt=projected_route_loss,
            projected_portfolio_loss_usdt=projected_portfolio_loss,
            incremental_tranche_loss_usdt=incremental_tranche_loss,
            expected_net_pnl_usdt=economics.expected_net_pnl_usdt,
            model_sha256=historical_model_sha256(request.model),
            strategy_profile_sha256=request.model.strategy_profile_sha256,
            source_manifest_sha256=request.model.source_manifest_sha256,
            reference_manifest_sha256=request.model.reference_manifest_sha256,
            runtime_manifest_sha256=request.runtime_manifest_sha256,
            contract_metadata_version_a=request.model.contract_metadata_version_a,
            contract_metadata_version_b=request.model.contract_metadata_version_b,
            decided_at=proposal.now,
        )
        return AggressiveStrategyDecision(
            True, AggressiveEntryReason.ACCEPTED, economics, sizing, intent
        )

    @staticmethod
    def reserve(store: AggressiveGridStore, decision: AggressiveStrategyDecision) -> None:
        if not decision.accepted or decision.intent is None:
            raise RuntimeError("only an accepted aggressive intent may reserve grid ownership")
        intent = decision.intent
        frozen = store.frozen_sizing_plan(intent.route_identity)
        if frozen is None:
            if intent.level_index != 1:
                raise RuntimeError("first route sizing reservation must be level one")
            store.freeze_sizing_plan(
                FrozenGridSizingPlan(
                    route_identity=intent.route_identity,
                    model_sha256=intent.model_sha256,
                    full_route_base_quantity=decision.sizing.full_route_base_quantity,
                    tranche_base_quantities=decision.sizing.tranche_base_quantities,
                    tranche_projected_losses_usdt=decision.sizing.tranche_projected_losses_usdt,
                    projected_margin_usdt=decision.sizing.projected_margin_usdt,
                    created_at=intent.decided_at,
                )
            )
        selected = store.first_unfilled_crossed_level(
            intent.route_identity,
            intent.reference_spread_bps,
        )
        if selected is None or selected.level_index != intent.level_index:
            raise RuntimeError("aggressive grid changed after decision")
        pending = store.reserve_entry(
            intent.route_identity,
            reference_spread_bps=intent.reference_spread_bps,
            decision_cycle=intent.decision_cycle,
            reserved_stress_usdt=intent.incremental_tranche_loss_usdt,
            now=intent.decided_at,
        )
        if pending.state != GridLevelState.ENTRY_PENDING:
            raise RuntimeError("aggressive grid reservation did not become entry-pending")


def recompute_actual_fill_risk(
    fill: ActualFillRiskInput,
    policy: AggressiveDecisionPolicy,
) -> ActualFillRiskResult:
    values = (
        fill.base_quantity,
        fill.long_fill_price,
        fill.short_fill_price,
        fill.actual_fees_usdt,
        fill.adverse_funding_usdt,
        fill.other_reserves_usdt,
        fill.effective_stop_bps,
        fill.existing_route_loss_usdt,
        fill.existing_portfolio_loss_usdt,
    )
    if any(not value.is_finite() for value in values):
        raise ValueError("actual-fill risk contains a non-finite value")
    if (
        fill.base_quantity <= 0
        or fill.long_fill_price <= 0
        or fill.short_fill_price <= 0
        or min(fill.actual_fees_usdt, fill.adverse_funding_usdt, fill.other_reserves_usdt) < 0
    ):
        raise ValueError("actual-fill risk contains an invalid amount")
    spread = canonical_executable_spread_bps(
        fill.direction,
        fill.long_fill_price,
        fill.short_fill_price,
    )
    average_price = (fill.long_fill_price + fill.short_fill_price) / Decimal(2)
    market_loss = fill.base_quantity * average_price * abs(fill.effective_stop_bps - spread) / _BPS
    incremental = (
        market_loss + fill.actual_fees_usdt + fill.adverse_funding_usdt + fill.other_reserves_usdt
    )
    route = fill.existing_route_loss_usdt + incremental
    portfolio = fill.existing_portfolio_loss_usdt + incremental
    return ActualFillRiskResult(
        accepted=(
            route <= policy.route_modelled_loss_limit_usdt
            and route < policy.route_hard_projected_loss_limit_usdt
            and portfolio <= policy.portfolio_modelled_loss_limit_usdt
            and portfolio < policy.portfolio_hard_projected_loss_limit_usdt
        ),
        projected_route_loss_usdt=route,
        projected_portfolio_loss_usdt=portfolio,
        actual_entry_spread_bps=spread,
    )


def worst_case_replay_minute(
    direction: DivergenceDirection,
    *,
    minute_high_bps: Decimal,
    minute_low_bps: Decimal,
    reverse_target_bps: Decimal,
    effective_stop_bps: Decimal,
) -> ReplayMinuteOutcome:
    values = (minute_high_bps, minute_low_bps, reverse_target_bps, effective_stop_bps)
    if any(not value.is_finite() for value in values) or minute_low_bps > minute_high_bps:
        raise ValueError("replay minute envelope is invalid")
    if direction == DivergenceDirection.POSITIVE:
        stop_hit = minute_high_bps >= effective_stop_bps
        target_hit = minute_low_bps <= reverse_target_bps
    else:
        stop_hit = minute_low_bps <= effective_stop_bps
        target_hit = minute_high_bps >= reverse_target_bps
    if stop_hit:
        return ReplayMinuteOutcome.STOP
    if target_hit:
        return ReplayMinuteOutcome.TARGET
    return ReplayMinuteOutcome.NONE


def validate_live_intent(
    intent: AggressiveTrancheIntent,
    *,
    expected_model_sha256: str,
    expected_profile_sha256: str,
    expected_runtime_manifest_sha256: str,
) -> AggressiveTrancheIntent:
    if (
        intent.model_sha256 != expected_model_sha256
        or intent.strategy_profile_sha256 != expected_profile_sha256
        or intent.runtime_manifest_sha256 != expected_runtime_manifest_sha256
    ):
        raise RuntimeError("aggressive live intent identity mismatch")
    if intent.execution_authorized:
        raise RuntimeError("aggressive strategy intent cannot authorize live execution")
    return intent


def _direction_model(request: AggressiveStrategyRequest) -> DirectionHistoricalModel:
    return (
        request.model.positive
        if request.proposal.direction == DivergenceDirection.POSITIVE
        else request.model.negative
    )


def _scale_reserves(reserves: CostReserves, quantity: Decimal) -> CostReserves:
    return replace(
        reserves,
        entry_impact_usdt=reserves.entry_impact_usdt * quantity,
        exit_impact_usdt=reserves.exit_impact_usdt * quantity,
        entry_slippage_usdt=reserves.entry_slippage_usdt * quantity,
        exit_slippage_usdt=reserves.exit_slippage_usdt * quantity,
        latency_usdt=reserves.latency_usdt * quantity,
        partial_fill_unmatched_usdt=reserves.partial_fill_unmatched_usdt * quantity,
        emergency_hedge_usdt=reserves.emergency_hedge_usdt * quantity,
        reconciliation_forced_exit_usdt=(reserves.reconciliation_forced_exit_usdt * quantity),
        liquidation_distance_usdt=reserves.liquidation_distance_usdt * quantity,
    )


def _resize_for_complete_executable_risk(
    request: AggressiveStrategyRequest,
    sizing: GridSizingResult,
    direction_model: DirectionHistoricalModel,
    reference_price: Decimal,
    policy: AggressiveDecisionPolicy,
) -> GridSizingResult:
    """Round down the initial route size using the complete executable risk formula.

    The first L2 evaluation is performed at the largest preliminary size. Each smaller candidate
    is then re-evaluated through the same pure confirmed-entry economics before another possible
    round-down. Exposure never increases inside one decision, and the result must converge while
    satisfying the locked route, portfolio and local-margin limits.
    """
    assessed = _evaluate_complete_executable_grid_risk(
        request,
        sizing,
        direction_model,
        reference_price,
        policy,
    )
    route_remaining = policy.route_modelled_loss_limit_usdt - (
        request.sizing.existing_route_loss_usdt
    )
    portfolio_remaining = policy.portfolio_modelled_loss_limit_usdt - (
        request.sizing.existing_portfolio_loss_usdt
    )
    total_loss = sum(assessed.tranche_projected_losses_usdt, Decimal(0))
    if min(total_loss, route_remaining, portfolio_remaining) <= 0:
        return _runtime_risk_rejected(request)
    preliminary_full_cap = max(
        (
            quantity / weight
            for quantity, weight in zip(
                sizing.tranche_base_quantities,
                direction_model.tranche_weights,
                strict=True,
            )
            if quantity > 0
        ),
        default=Decimal(0),
    )
    loss_scale = min(route_remaining / total_loss, portfolio_remaining / total_loss, Decimal(1))
    margin_limit = request.sizing.free_margin_usdt * (
        Decimal(1) - policy.local_free_margin_floor_ratio
    )
    margin_scale = (
        min(margin_limit / assessed.projected_margin_usdt, Decimal(1))
        if assessed.projected_margin_usdt > 0
        else Decimal(0)
    )
    complete_scale = min(loss_scale, margin_scale)
    if complete_scale == 1:
        return assessed
    raw_full_quantity = preliminary_full_cap * complete_scale
    full_quantity = (raw_full_quantity / request.sizing.quantity_step).to_integral_value(
        rounding=ROUND_FLOOR
    ) * request.sizing.quantity_step
    minimum = max(
        request.sizing.minimum_base_quantity,
        request.sizing.minimum_notional_usdt / reference_price,
    )
    tranche_quantities = tuple(
        quantity if quantity >= minimum else Decimal(0)
        for quantity in (
            (full_quantity * weight / request.sizing.quantity_step).to_integral_value(
                rounding=ROUND_FLOOR
            )
            * request.sizing.quantity_step
            for weight in direction_model.tranche_weights
        )
    )
    effective_full_quantity = sum(tranche_quantities, Decimal(0))
    projected_margin = (
        effective_full_quantity * reference_price / policy.initial_effective_leverage_cap
    )
    return GridSizingResult(
        accepted=effective_full_quantity > 0,
        reason=(
            AggressiveEntryReason.ACCEPTED
            if effective_full_quantity > 0
            else AggressiveEntryReason.RISK_REJECTED
        ),
        full_route_base_quantity=effective_full_quantity,
        tranche_base_quantities=tranche_quantities,
        # The next loop iteration re-evaluates every non-linear tranche exactly at these sizes.
        tranche_projected_losses_usdt=(Decimal(0),) * 5,
        projected_route_loss_usdt=request.sizing.existing_route_loss_usdt,
        projected_portfolio_loss_usdt=request.sizing.existing_portfolio_loss_usdt,
        projected_margin_usdt=projected_margin,
    )


def _evaluate_complete_executable_grid_risk(
    request: AggressiveStrategyRequest,
    sizing: GridSizingResult,
    direction_model: DirectionHistoricalModel,
    reference_price: Decimal,
    policy: AggressiveDecisionPolicy,
) -> GridSizingResult:
    """Price every planned tranche against the current non-linear L2 book."""
    losses: list[Decimal] = []
    margin_reference_price = reference_price
    start_index = request.proposal.level_index - 1 if request.sizing.frozen_route_sizing else 0
    for tranche_index, (quantity, level_bps) in enumerate(
        zip(
            sizing.tranche_base_quantities,
            direction_model.levels_bps,
            strict=True,
        )
    ):
        if tranche_index < start_index:
            losses.append(Decimal(0))
            continue
        if quantity <= 0:
            losses.append(Decimal(0))
            continue
        proposal = replace(
            request.proposal,
            quantity=quantity,
            reserves=_scale_reserves(request.reserves_per_base, quantity),
            state_reconciled=request.state_reconciled,
            historical_model_eligible=True,
            regime_ready=not direction_model.regime_drift_blocked,
        )
        economics = revalidate_hybrid_entry_once(proposal, policy=policy)
        if (
            not economics.accepted
            or economics.executable_entry_spread_bps is None
            or economics.long_entry_vwap is None
            or economics.short_entry_vwap is None
        ):
            return _runtime_risk_rejected(request)
        executable_reference_price = (
            economics.long_entry_vwap + economics.short_entry_vwap
        ) / Decimal(2)
        margin_reference_price = max(margin_reference_price, executable_reference_price)
        current_stop_distance = abs(
            request.effective_stop_bps - economics.executable_entry_spread_bps
        )
        stop_distance = (
            current_stop_distance
            if tranche_index == request.proposal.level_index - 1
            else max(abs(request.effective_stop_bps - level_bps), current_stop_distance)
        )
        losses.append(
            quantity * executable_reference_price * stop_distance / _BPS
            + economics.stressed_total_cost_usdt
        )
    total_loss = sum(losses, Decimal(0))
    projected_route = request.sizing.existing_route_loss_usdt + total_loss
    projected_portfolio = request.sizing.existing_portfolio_loss_usdt + total_loss
    projected_margin = (
        sum(sizing.tranche_base_quantities[start_index:], Decimal(0))
        * margin_reference_price
        / policy.initial_effective_leverage_cap
    )
    accepted = (
        total_loss > 0
        and projected_route <= policy.route_modelled_loss_limit_usdt
        and projected_route < policy.route_hard_projected_loss_limit_usdt
        and projected_portfolio <= policy.portfolio_modelled_loss_limit_usdt
        and projected_portfolio < policy.portfolio_hard_projected_loss_limit_usdt
        and projected_margin
        <= request.sizing.free_margin_usdt * (Decimal(1) - policy.local_free_margin_floor_ratio)
    )
    return replace(
        sizing,
        accepted=accepted,
        reason=(
            AggressiveEntryReason.ACCEPTED if accepted else AggressiveEntryReason.RISK_REJECTED
        ),
        tranche_projected_losses_usdt=tuple(losses),
        projected_route_loss_usdt=projected_route,
        projected_portfolio_loss_usdt=projected_portfolio,
        projected_margin_usdt=projected_margin,
    )


def _runtime_risk_rejected(request: AggressiveStrategyRequest) -> GridSizingResult:
    return GridSizingResult(
        accepted=False,
        reason=AggressiveEntryReason.RISK_REJECTED,
        full_route_base_quantity=Decimal(0),
        tranche_base_quantities=(Decimal(0),) * 5,
        tranche_projected_losses_usdt=(Decimal(0),) * 5,
        projected_route_loss_usdt=request.sizing.existing_route_loss_usdt,
        projected_portfolio_loss_usdt=request.sizing.existing_portfolio_loss_usdt,
        projected_margin_usdt=Decimal(0),
    )
