from __future__ import annotations

import math
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from decimal import Decimal, localcontext

from interexchange_perp_grid.aggressive_evaluator import (
    AggressiveDecisionPolicy,
    AggressiveEntryStage,
    AggressiveExitInput,
    AggressiveExitReason,
    CostReserves,
    HybridEntryInput,
    VenueFundingProjection,
    select_aggressive_exit_reason,
)
from interexchange_perp_grid.aggressive_grid import (
    AggressiveGridStore,
    GridLegFill,
    GridLevelRecord,
    GridLevelState,
    GridTrancheOwnership,
)
from interexchange_perp_grid.aggressive_model import (
    DivergenceDirection,
    HistoricalReferenceModel,
)
from interexchange_perp_grid.aggressive_runtime import (
    AggressiveDecisionCore,
    AggressiveRuntimeMode,
    AggressiveSizingInput,
    AggressiveStrategyDecision,
    AggressiveStrategyRequest,
)
from interexchange_perp_grid.domain import (
    FundingSnapshot,
    Instrument,
    OrderBookSnapshot,
    Venue,
)
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.public_engine import AggressiveRouteMarketSnapshot
from interexchange_perp_grid.reference_history import ReferenceSpreadBar
from interexchange_perp_grid.routes import executable_vwap

_BPS = Decimal("10000")


@dataclass(frozen=True, slots=True)
class AggressiveShadowDecisionInput:
    model: HistoricalReferenceModel
    reference_bar: ReferenceSpreadBar
    market: AggressiveRouteMarketSnapshot
    effective_stop_bps: Decimal
    # Dollar reserves for one base unit; the shared core scales them after lot rounding.
    reserves: CostReserves
    existing_route_loss_usdt: Decimal
    existing_portfolio_loss_usdt: Decimal
    free_margin_usdt: Decimal
    decision_cycle: int
    runtime_manifest_sha256: str
    maximum_book_age_ms: int
    now: datetime
    stage: AggressiveEntryStage = AggressiveEntryStage.NORMAL
    private_long_taker_fee_rate: Decimal | None = None
    private_short_taker_fee_rate: Decimal | None = None
    state_reconciled: bool = True

    def __post_init__(self) -> None:
        if self.now.tzinfo is None or self.now.utcoffset() is None:
            raise ValueError("aggressive shadow clock must be timezone-aware")
        values = (
            self.effective_stop_bps,
            self.reserves.total(),
            self.existing_route_loss_usdt,
            self.existing_portfolio_loss_usdt,
            self.free_margin_usdt,
        )
        if any(not value.is_finite() for value in values):
            raise ValueError("aggressive shadow risk input is non-finite")
        if min(values[1:]) < 0 or self.maximum_book_age_ms <= 0:
            raise ValueError("aggressive shadow risk input is invalid")


class AggressiveShadowDecisionBridge:
    """Maps the existing public engine's raw data into the one shared decision core."""

    def __init__(
        self,
        core: AggressiveDecisionCore,
        grid: AggressiveGridStore,
    ) -> None:
        self._core = core
        self._grid = grid

    def evaluate(
        self,
        inputs: AggressiveShadowDecisionInput,
    ) -> AggressiveStrategyDecision:
        direction = _direction_for_route(inputs.model, inputs.market.route.value)
        direction_model = (
            inputs.model.positive
            if direction == DivergenceDirection.POSITIVE
            else inputs.model.negative
        )
        route_identity = inputs.market.route.value
        level = self._grid.first_unfilled_crossed_level(
            route_identity,
            _reference_envelope(inputs.reference_bar, direction),
        )
        if level is None:
            level_index = 1
            reference_trigger = direction_model.levels_bps[0]
        else:
            level_index = level.level_index
            reference_trigger = level.trigger_bps
        long_fee = (
            inputs.private_long_taker_fee_rate
            if inputs.private_long_taker_fee_rate is not None
            else inputs.market.long_instrument.taker_fee_rate
        )
        short_fee = (
            inputs.private_short_taker_fee_rate
            if inputs.private_short_taker_fee_rate is not None
            else inputs.market.short_instrument.taker_fee_rate
        )
        funding_long = _funding_projection(
            inputs.market.long_funding,
            inputs.market.long_instrument.venue,
            inputs.now,
            self._core.policy.hard_max_hold_seconds,
        )
        funding_short = _funding_projection(
            inputs.market.short_funding,
            inputs.market.short_instrument.venue,
            inputs.now,
            self._core.policy.hard_max_hold_seconds,
        )
        notional = _indicative_notional(inputs.market)
        minimum_profit = (
            self._core.policy.canary_minimum_expected_net_profit_usdt
            if inputs.stage == AggressiveEntryStage.LOCKED_CANARY
            else self._core.policy.normal_minimum_expected_net_profit_usdt
        )
        proposal = HybridEntryInput(
            route_identity=route_identity,
            direction=direction,
            level_index=level_index,
            reference_spread_bps=_reference_envelope(inputs.reference_bar, direction),
            reference_trigger_bps=reference_trigger,
            grid_step_bps=direction_model.range_bps / Decimal(5),
            stressed_cost_move_bps=(
                inputs.reserves.total()
                * self._core.policy.stressed_cost_multiplier
                / notional
                * _BPS
                if notional > 0
                else Decimal(0)
            ),
            minimum_profit_move_bps=(
                minimum_profit / notional * _BPS if notional > 0 else Decimal(0)
            ),
            normal_low_bps=inputs.model.normal_low_bps,
            normal_high_bps=inputs.model.normal_high_bps,
            quantity=Decimal(1),
            long_venue=inputs.market.long_instrument.venue,
            short_venue=inputs.market.short_instrument.venue,
            long_book=inputs.market.long_book
            or _empty_book(inputs.market.long_instrument, inputs.now),
            short_book=inputs.market.short_book
            or _empty_book(inputs.market.short_instrument, inputs.now),
            long_private_taker_fee_rate=long_fee,
            short_private_taker_fee_rate=short_fee,
            long_funding=funding_long,
            short_funding=funding_short,
            reserves=CostReserves(*(Decimal(0) for _ in range(9))),
            observed_monotonic_ns=inputs.market.observed_monotonic_ns,
            maximum_book_age_ms=inputs.maximum_book_age_ms,
            now=inputs.now,
            stage=inputs.stage,
            state_reconciled=(
                inputs.state_reconciled
                and not (
                    inputs.market.unavailable_venues
                    & {
                        inputs.market.long_instrument.venue,
                        inputs.market.short_instrument.venue,
                    }
                )
                and inputs.market.long_quality.accepted
                and inputs.market.short_quality.accepted
            ),
        )
        minimum_notional = _minimum_notional(
            inputs.market.long_instrument,
            inputs.market.short_instrument,
        )
        request = AggressiveStrategyRequest(
            mode=AggressiveRuntimeMode.SHADOW,
            model=inputs.model,
            proposal=proposal,
            sizing=AggressiveSizingInput(
                quantity_step=_common_base_step(
                    inputs.market.long_instrument,
                    inputs.market.short_instrument,
                ),
                minimum_base_quantity=max(
                    inputs.market.long_instrument.minimum_base_amount,
                    inputs.market.short_instrument.minimum_base_amount,
                ),
                minimum_notional_usdt=minimum_notional,
                per_full_base_reserve_usdt=inputs.reserves.total(),
                existing_route_loss_usdt=inputs.existing_route_loss_usdt,
                existing_portfolio_loss_usdt=inputs.existing_portfolio_loss_usdt,
                free_margin_usdt=inputs.free_margin_usdt,
            ),
            reserves_per_base=inputs.reserves,
            effective_stop_bps=inputs.effective_stop_bps,
            decision_cycle=inputs.decision_cycle,
            runtime_manifest_sha256=inputs.runtime_manifest_sha256,
            state_reconciled=proposal.state_reconciled,
            reference_identity_valid=_reference_matches_model(
                inputs.reference_bar,
                inputs.model,
            ),
        )
        return self._core.evaluate(request)

    def reserve(self, decision: AggressiveStrategyDecision) -> None:
        self._core.reserve(self._grid, decision)


class AggressiveShadowPortfolio:
    """Persistent simulated fill/exit lifecycle driven by the shared immutable intent."""

    def __init__(
        self,
        grid: AggressiveGridStore,
        policy: AggressiveDecisionPolicy,
    ) -> None:
        self._grid = grid
        self._policy = policy

    def open(self, decision: AggressiveStrategyDecision) -> GridLevelRecord:
        if not decision.accepted or decision.intent is None:
            raise RuntimeError("shadow may open only an accepted aggressive intent")
        AggressiveDecisionCore.reserve(self._grid, decision)
        intent = decision.intent
        entry_fee_per_leg = decision.economics.four_leg_fees_usdt / Decimal(4)
        ownership = GridTrancheOwnership(
            tranche_id=intent.intent_id,
            normalized_base_quantity=intent.quantity,
            legs=(
                GridLegFill(
                    venue=Venue(intent.long_venue),
                    symbol=intent.long_symbol,
                    side=Side.BUY,
                    base_quantity=intent.quantity,
                    average_price=intent.long_entry_vwap,
                    fee_usdt=entry_fee_per_leg,
                    funding_usdt=Decimal(0),
                ),
                GridLegFill(
                    venue=Venue(intent.short_venue),
                    symbol=intent.short_symbol,
                    side=Side.SELL,
                    base_quantity=intent.quantity,
                    average_price=intent.short_entry_vwap,
                    fee_usdt=entry_fee_per_leg,
                    funding_usdt=Decimal(0),
                ),
            ),
            executable_entry_spread_bps=intent.executable_entry_spread_bps,
            reverse_target_bps=intent.reverse_target_bps,
            effective_stop_bps=intent.effective_stop_bps,
            maximum_holding_deadline=intent.decided_at
            + timedelta(seconds=self._policy.hard_max_hold_seconds),
            reserved_stress_usdt=intent.projected_route_loss_usdt,
            entry_slippage_usdt=Decimal(0),
            realised_pnl_usdt=Decimal(0),
            unrealised_pnl_usdt=Decimal(0),
            opened_at=intent.decided_at,
        )
        return self._grid.mark_open(
            intent.route_identity,
            intent.level_index,
            ownership,
            decision_cycle=intent.decision_cycle,
            now=intent.decided_at,
        )

    def close_due(
        self,
        *,
        model: HistoricalReferenceModel,
        reference_bar: ReferenceSpreadBar,
        market: AggressiveRouteMarketSnapshot,
        now: datetime,
        projected_portfolio_loss_usdt: Decimal,
        adverse_funding_destroys_profit: bool = False,
        emergency_or_unknown: bool = False,
    ) -> tuple[tuple[int, AggressiveExitReason], ...]:
        if market.route.value not in {model.positive_route, model.negative_route}:
            return ()
        closed: list[tuple[int, AggressiveExitReason]] = []
        for level in self._grid.levels(market.route.value):
            if level.state != GridLevelState.OPEN or level.ownership is None:
                continue
            ownership = level.ownership
            if market.long_book is None or market.short_book is None:
                continue
            long_exit = executable_vwap(
                market.long_book.bids,
                ownership.normalized_base_quantity,
            )
            short_exit = executable_vwap(
                market.short_book.asks,
                ownership.normalized_base_quantity,
            )
            if long_exit is None or short_exit is None:
                continue
            with localcontext() as context:
                context.prec = 50
                executable_spread = (short_exit.price / long_exit.price).ln() * _BPS
            reference_stop_crossed = (
                reference_bar.high_bps >= ownership.effective_stop_bps
                if level.direction == DivergenceDirection.POSITIVE
                else reference_bar.low_bps <= ownership.effective_stop_bps
            )
            reason = select_aggressive_exit_reason(
                AggressiveExitInput(
                    direction=level.direction,
                    executable_spread_bps=executable_spread,
                    effective_stop_bps=ownership.effective_stop_bps,
                    reverse_target_bps=ownership.reverse_target_bps,
                    projected_route_loss_usdt=(
                        self._policy.route_hard_projected_loss_limit_usdt
                        if reference_stop_crossed
                        else ownership.reserved_stress_usdt
                    ),
                    projected_portfolio_loss_usdt=projected_portfolio_loss_usdt,
                    holding_deadline=ownership.maximum_holding_deadline,
                    now=now,
                    emergency_or_unknown=emergency_or_unknown,
                    adverse_funding_destroys_profit=adverse_funding_destroys_profit,
                ),
                self._policy,
            )
            if reason == AggressiveExitReason.NONE:
                continue
            self._grid.reserve_exit(
                level.route_identity,
                level.level_index,
                tranche_id=ownership.tranche_id,
                now=now,
            )
            quantity = ownership.normalized_base_quantity
            entry_long, entry_short = ownership.legs
            gross = quantity * (
                (long_exit.price - entry_long.average_price)
                + (entry_short.average_price - short_exit.price)
            )
            exit_fees = _exit_fees(
                market,
                quantity,
                long_exit.price,
                short_exit.price,
            )
            realised = (
                gross
                - entry_long.fee_usdt
                - entry_short.fee_usdt
                - exit_fees
                + entry_long.funding_usdt
                + entry_short.funding_usdt
            )
            final = replace(
                ownership,
                realised_pnl_usdt=realised,
                unrealised_pnl_usdt=Decimal(0),
            )
            self._grid.mark_closed(
                level.route_identity,
                level.level_index,
                final,
                now=now,
            )
            closed.append((level.level_index, reason))
        return tuple(closed)

    def rearm_stable_flat(
        self,
        route_identity: str,
        *,
        reference_spread_bps: Decimal,
        now: datetime,
    ) -> tuple[int, ...]:
        levels = self._grid.levels(route_identity)
        active = {GridLevelState.ENTRY_PENDING, GridLevelState.OPEN, GridLevelState.EXIT_PENDING}
        if any(level.state in active for level in levels):
            return ()
        rearmed: list[int] = []
        for level in levels:
            if level.state != GridLevelState.CLOSED_WAIT_REARM or level.ownership is None:
                continue
            try:
                self._grid.rearm(
                    route_identity,
                    level.level_index,
                    reference_spread_bps=reference_spread_bps,
                    stable_flat=True,
                    tranche_id=level.ownership.tranche_id,
                    now=now,
                )
            except RuntimeError as error:
                if "retreat" not in str(error):
                    raise
            else:
                rearmed.append(level.level_index)
        return tuple(rearmed)


def _direction_for_route(
    model: HistoricalReferenceModel,
    route_identity: str,
) -> DivergenceDirection:
    if route_identity == model.positive_route:
        return DivergenceDirection.POSITIVE
    if route_identity == model.negative_route:
        return DivergenceDirection.NEGATIVE
    raise ValueError("public route is not linked to the historical reference model")


def _reference_envelope(
    bar: ReferenceSpreadBar,
    direction: DivergenceDirection,
) -> Decimal:
    return bar.high_bps if direction == DivergenceDirection.POSITIVE else bar.low_bps


def _reference_matches_model(
    bar: ReferenceSpreadBar,
    model: HistoricalReferenceModel,
) -> bool:
    return (
        bar.instrument.base == model.base
        and bar.venue_a.value == model.venue_a
        and bar.venue_b.value == model.venue_b
        and bar.contract_metadata_version_a == model.contract_metadata_version_a
        and bar.contract_metadata_version_b == model.contract_metadata_version_b
    )


def _indicative_notional(market: AggressiveRouteMarketSnapshot) -> Decimal:
    if market.long_book is None or market.short_book is None:
        return Decimal(0)
    if not market.long_book.asks or not market.short_book.bids:
        return Decimal(0)
    return (market.long_book.asks[0].price + market.short_book.bids[0].price) / Decimal(2)


def _funding_projection(
    snapshot: FundingSnapshot | None,
    venue: Venue,
    now: datetime,
    maximum_hold_seconds: int,
) -> VenueFundingProjection | None:
    if (
        snapshot is None
        or snapshot.venue != venue
        or snapshot.rate is None
        or snapshot.mark_price is None
        or snapshot.next_funding_timestamp_ms is None
        or snapshot.interval is None
    ):
        return None
    interval_seconds = _funding_interval_seconds(snapshot.interval)
    now_ms = int(now.timestamp() * 1000)
    remaining_ms = snapshot.next_funding_timestamp_ms - now_ms
    if remaining_ms < 0:
        return None
    horizon_ms = maximum_hold_seconds * 1000
    event_count = (
        0
        if remaining_ms > horizon_ms
        else 1 + (horizon_ms - remaining_ms) // (interval_seconds * 1000)
    )
    return VenueFundingProjection(
        venue=venue,
        rate=snapshot.rate,
        mark_price=snapshot.mark_price,
        event_count=event_count,
        next_funding_timestamp_ms=snapshot.next_funding_timestamp_ms,
        interval_seconds=interval_seconds,
    )


def _funding_interval_seconds(value: str) -> int:
    raw = value.strip().lower()
    if len(raw) < 2 or raw[-1] not in {"h", "m"} or not raw[:-1].isdigit():
        raise ValueError("funding interval is not a supported exact duration")
    multiplier = 3600 if raw[-1] == "h" else 60
    seconds = int(raw[:-1]) * multiplier
    if seconds <= 0:
        raise ValueError("funding interval must be positive")
    return seconds


def _minimum_notional(long: Instrument, short: Instrument) -> Decimal:
    values: list[Decimal] = []
    for instrument in (long, short):
        if instrument.minimum_notional is not None:
            values.append(instrument.minimum_notional)
        elif not instrument.no_fixed_minimum_notional:
            raise ValueError("minimum notional is unknown")
    return max(values, default=Decimal(0))


def _common_base_step(long: Instrument, short: Instrument) -> Decimal:
    first = long.base_amount_step
    second = short.base_amount_step
    first_exponent = first.normalize().as_tuple().exponent
    second_exponent = second.normalize().as_tuple().exponent
    if not isinstance(first_exponent, int) or not isinstance(second_exponent, int):
        raise ValueError("base amount step must be finite")
    places = max(0, -first_exponent, -second_exponent)
    scale = 10**places
    first_integer = int(first * scale)
    second_integer = int(second * scale)
    common_integer = abs(first_integer * second_integer) // math.gcd(
        first_integer,
        second_integer,
    )
    return Decimal(common_integer) / Decimal(scale)


def _exit_fees(
    market: AggressiveRouteMarketSnapshot,
    quantity: Decimal,
    long_exit_price: Decimal,
    short_exit_price: Decimal,
) -> Decimal:
    long_fee = market.long_instrument.taker_fee_rate
    short_fee = market.short_instrument.taker_fee_rate
    if long_fee is None or short_fee is None:
        raise RuntimeError("shadow exit requires known venue taker fees")
    if min(long_fee, short_fee) < 0:
        raise RuntimeError("shadow exit taker fee is invalid")
    return quantity * (long_exit_price * long_fee + short_exit_price * short_fee)


def _empty_book(instrument: Instrument, now: datetime) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        venue=instrument.venue,
        symbol=instrument.symbol,
        bids=(),
        asks=(),
        exchange_timestamp_ms=None,
        received_at=now,
        received_monotonic_ns=0,
        sequence_start=None,
        sequence_end=None,
        is_snapshot=False,
        synchronised=False,
        clock_skew_ms=None,
    )
