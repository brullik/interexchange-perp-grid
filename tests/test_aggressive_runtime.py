from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.aggressive_evaluator import (
    AggressiveEntryReason,
    CostReserves,
    HybridEntryInput,
    VenueFundingProjection,
    load_aggressive_decision_policy,
)
from interexchange_perp_grid.aggressive_grid import AggressiveGridStore, GridLevelState
from interexchange_perp_grid.aggressive_model import (
    DivergenceDirection,
    HistoricalModelPolicy,
    HistoricalReferenceModel,
    ModelEligibility,
    build_historical_reference_model,
    historical_model_sha256,
)
from interexchange_perp_grid.aggressive_runtime import (
    ActualFillRiskInput,
    AggressiveDecisionCore,
    AggressiveRuntimeMode,
    AggressiveSizingInput,
    AggressiveStrategyDecision,
    AggressiveStrategyRequest,
    ReplayMinuteOutcome,
    recompute_actual_fill_risk,
    validate_live_intent,
    worst_case_replay_minute,
)
from interexchange_perp_grid.domain import (
    BookLevel,
    InstrumentKey,
    OrderBookSnapshot,
    ProductType,
    Venue,
)
from interexchange_perp_grid.execution import PairActionState, PairExecutionCoordinator
from interexchange_perp_grid.reference_history import ReferenceSpreadBar

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_KEY = InstrumentKey("BTC", "USDT", "USDT", ProductType.LINEAR_USDT_PERPETUAL)


def _bar(minute: int) -> ReferenceSpreadBar:
    return ReferenceSpreadBar(
        venue_a=Venue.BYBIT,
        venue_b=Venue.OKX,
        instrument=_KEY,
        interval_start=_NOW + timedelta(minutes=minute),
        open_bps=Decimal(0),
        high_bps=Decimal(1000),
        low_bps=Decimal(-1000),
        close_bps=Decimal(0),
        contract_metadata_version_a="bybit-v1",
        contract_metadata_version_b="okx-v1",
    )


def _model() -> HistoricalReferenceModel:
    model = build_historical_reference_model(
        tuple(_bar(minute) for minute in range(20)),
        policy=HistoricalModelPolicy(
            history_target_days=Decimal("0.02"),
            history_minimum_live_days=Decimal("0.015"),
            history_minimum_shadow_days=Decimal("0.01"),
        ),
        source_manifest_sha256="source",
        strategy_profile_sha256="profile",
        code_sha="a" * 40,
    )
    return replace(
        model,
        positive=replace(
            model.positive,
            eligibility=ModelEligibility.LIVE_ELIGIBLE,
            regime_drift_blocked=False,
        ),
    )


def _book(venue: Venue, bid: str, ask: str) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        venue=venue,
        symbol="BTC/USDT:USDT",
        bids=(BookLevel(Decimal(bid), Decimal(100)),),
        asks=(BookLevel(Decimal(ask), Decimal(100)),),
        exchange_timestamp_ms=1,
        received_at=_NOW,
        received_monotonic_ns=1_000_000_000,
        sequence_start=1,
        sequence_end=1,
        is_snapshot=True,
        synchronised=True,
        clock_skew_ms=0,
    )


def _funding(venue: Venue) -> VenueFundingProjection:
    return VenueFundingProjection(venue, Decimal(0), Decimal(100), 1, 1, 28_800)


def _request(mode: AggressiveRuntimeMode, offset_ns: int) -> AggressiveStrategyRequest:
    model = _model()
    proposal = HybridEntryInput(
        route_identity=model.positive_route,
        direction=DivergenceDirection.POSITIVE,
        level_index=1,
        reference_spread_bps=Decimal(200),
        reference_trigger_bps=model.positive.levels_bps[0],
        grid_step_bps=model.positive.range_bps / Decimal(5),
        stressed_cost_move_bps=Decimal("0.5"),
        minimum_profit_move_bps=Decimal("0.5"),
        normal_low_bps=model.normal_low_bps,
        normal_high_bps=model.normal_high_bps,
        quantity=Decimal(1),
        long_venue=Venue.OKX,
        short_venue=Venue.BYBIT,
        long_book=_book(Venue.OKX, "99.9", "100"),
        short_book=_book(Venue.BYBIT, "103.1", "103.2"),
        long_private_taker_fee_rate=Decimal(0),
        short_private_taker_fee_rate=Decimal(0),
        long_funding=_funding(Venue.OKX),
        short_funding=_funding(Venue.BYBIT),
        reserves=CostReserves(*(Decimal(0) for _ in range(9))),
        observed_monotonic_ns=1_000_000_000 + offset_ns,
        maximum_book_age_ms=1000,
        now=_NOW,
    )
    return AggressiveStrategyRequest(
        mode=mode,
        model=model,
        proposal=proposal,
        sizing=AggressiveSizingInput(
            quantity_step=Decimal("0.01"),
            minimum_base_quantity=Decimal("0.01"),
            minimum_notional_usdt=Decimal("0.01"),
            per_full_base_reserve_usdt=Decimal("0.01"),
            existing_route_loss_usdt=Decimal(0),
            existing_portfolio_loss_usdt=Decimal(0),
            free_margin_usdt=Decimal(100),
        ),
        effective_stop_bps=model.positive.reference_stop_bps,
        decision_cycle=1,
        runtime_manifest_sha256="runtime",
    )


def _accepted(mode: AggressiveRuntimeMode) -> AggressiveStrategyDecision:
    core = AggressiveDecisionCore(
        load_aggressive_decision_policy(Path("config/AGGRESSIVE_SYMBIOSIS_V1.yaml")).policy
    )
    decision = None
    for offset in (0, 250_000_000, 500_000_000):
        decision = core.evaluate(_request(mode, offset))
    assert decision is not None
    return decision


def test_replay_shadow_and_live_create_the_same_immutable_intent() -> None:
    replay = _accepted(AggressiveRuntimeMode.REPLAY)
    shadow = _accepted(AggressiveRuntimeMode.SHADOW)
    live = _accepted(AggressiveRuntimeMode.LIVE)
    assert replay.accepted and shadow.accepted and live.accepted
    assert replay.intent == shadow.intent == live.intent
    assert replay.intent is not None
    assert live.intent is not None
    assert replay.intent.quantity == replay.sizing.tranche_base_quantities[0]
    assert not replay.intent.execution_authorized
    assert (
        validate_live_intent(
            live.intent,
            expected_model_sha256=historical_model_sha256(_model()),
            expected_profile_sha256="profile",
            expected_runtime_manifest_sha256="runtime",
        )
        == live.intent
    )
    tranche = PairExecutionCoordinator.prepare_aggressive_tranche(live.intent)
    assert tranche.tranche_id == live.intent.intent_id
    assert tranche.state == PairActionState.CREATED
    assert tranche.route.value == live.intent.route_identity
    assert tranche.entry_long_fills == []
    assert tranche.entry_short_fills == []


def test_shared_core_reserves_exactly_one_persisted_level(tmp_path: Path) -> None:
    decision = _accepted(AggressiveRuntimeMode.SHADOW)
    assert decision.intent is not None
    store = AggressiveGridStore(tmp_path / "grid.sqlite3")
    store.initialise()
    store.initialise_route(
        _model(),
        DivergenceDirection.POSITIVE,
        now=_NOW,
        rearm_retreat_step_fraction=Decimal("0.25"),
    )
    AggressiveDecisionCore.reserve(store, decision)
    levels = store.levels(decision.intent.route_identity)
    assert levels[0].state == GridLevelState.ENTRY_PENDING
    assert sum(level.state == GridLevelState.ENTRY_PENDING for level in levels) == 1
    with pytest.raises(RuntimeError, match="changed after decision"):
        AggressiveDecisionCore.reserve(store, decision)


def test_model_or_runtime_identity_mismatch_fails_closed() -> None:
    decision = _accepted(AggressiveRuntimeMode.LIVE)
    assert decision.intent is not None
    with pytest.raises(RuntimeError, match="identity mismatch"):
        validate_live_intent(
            decision.intent,
            expected_model_sha256="wrong",
            expected_profile_sha256="profile",
            expected_runtime_manifest_sha256="runtime",
        )
    core = AggressiveDecisionCore(
        load_aggressive_decision_policy(Path("config/AGGRESSIVE_SYMBIOSIS_V1.yaml")).policy
    )
    request = _request(AggressiveRuntimeMode.SHADOW, 0)
    result = core.evaluate(
        replace(request, proposal=replace(request.proposal, grid_step_bps=Decimal(999)))
    )
    assert not result.accepted
    assert result.reason == AggressiveEntryReason.HISTORICAL_MODEL_INELIGIBLE


def test_actual_fill_recomputation_rejects_hard_loss_after_slippage() -> None:
    policy = load_aggressive_decision_policy(Path("config/AGGRESSIVE_SYMBIOSIS_V1.yaml")).policy
    result = recompute_actual_fill_risk(
        ActualFillRiskInput(
            direction=DivergenceDirection.POSITIVE,
            base_quantity=Decimal(10),
            long_fill_price=Decimal(100),
            short_fill_price=Decimal(101),
            actual_fees_usdt=Decimal(1),
            adverse_funding_usdt=Decimal(1),
            other_reserves_usdt=Decimal(3),
            effective_stop_bps=Decimal(120),
            existing_route_loss_usdt=Decimal(0),
            existing_portfolio_loss_usdt=Decimal(0),
        ),
        policy,
    )
    assert not result.accepted
    assert result.projected_route_loss_usdt >= Decimal(5)


@pytest.mark.parametrize("direction", tuple(DivergenceDirection))
def test_replay_uses_stop_when_target_and_stop_order_is_unknown(
    direction: DivergenceDirection,
) -> None:
    outcome = worst_case_replay_minute(
        direction,
        minute_high_bps=Decimal(20),
        minute_low_bps=Decimal(-20),
        reverse_target_bps=Decimal(2),
        effective_stop_bps=Decimal(10 if direction == DivergenceDirection.POSITIVE else -10),
    )
    assert outcome == ReplayMinuteOutcome.STOP
