from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.aggressive_evaluator import (
    AggressiveEntryReason,
    AggressiveExitReason,
    CostReserves,
    HybridEntryInput,
    VenueFundingProjection,
    load_aggressive_decision_policy,
)
from interexchange_perp_grid.aggressive_grid import (
    AggressiveGridStore,
    FrozenGridSizingPlan,
    GridLevelState,
)
from interexchange_perp_grid.aggressive_live import (
    AggressiveLaptopLiveStage,
    AggressiveLiveIntentEnvelope,
    aggressive_intent_sha256,
    load_aggressive_live_intent,
    prepare_aggressive_live_plan,
    save_aggressive_live_intent,
)
from interexchange_perp_grid.aggressive_model import (
    DivergenceDirection,
    HistoricalModelPolicy,
    HistoricalReferenceModel,
    ModelEligibility,
    build_historical_reference_model,
    historical_model_sha256,
)
from interexchange_perp_grid.aggressive_qualification import (
    AggressiveDirectionBinding,
    AggressiveQualificationBinding,
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
from interexchange_perp_grid.aggressive_shadow import (
    AggressiveShadowDecisionBridge,
    AggressiveShadowDecisionInput,
    AggressiveShadowPortfolio,
)
from interexchange_perp_grid.domain import (
    BookLevel,
    FundingSnapshot,
    Instrument,
    InstrumentKey,
    OrderBookSnapshot,
    ProductType,
    Venue,
)
from interexchange_perp_grid.execution import PairActionState, PairExecutionCoordinator
from interexchange_perp_grid.market_data import DataQualityAssessment
from interexchange_perp_grid.public_engine import AggressiveRouteMarketSnapshot
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.reference_history import ReferenceSpreadBar
from interexchange_perp_grid.strategy import DirectedRouteKey

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


def _instrument(venue: Venue) -> Instrument:
    return Instrument(
        venue=venue,
        symbol="BTC/USDT:USDT",
        exchange_symbol="BTCUSDT",
        base="BTC",
        quote="USDT",
        settle="USDT",
        contract_size_base=Decimal(1),
        amount_step_contracts=Decimal("0.01"),
        price_tick=Decimal("0.1"),
        minimum_amount_contracts=Decimal("0.01"),
        minimum_notional=Decimal("0.01"),
        taker_fee_rate=Decimal(0),
        fee_source="fixture",
    )


def _market(offset_ns: int = 0) -> AggressiveRouteMarketSnapshot:
    next_funding = int((_NOW + timedelta(hours=8)).timestamp() * 1000)
    long = _instrument(Venue.OKX)
    short = _instrument(Venue.BYBIT)
    return AggressiveRouteMarketSnapshot(
        route=DirectedRouteKey("BTC", Venue.OKX, Venue.BYBIT),
        long_instrument=long,
        short_instrument=short,
        long_book=_book(Venue.OKX, "99.9", "100"),
        short_book=_book(Venue.BYBIT, "103.1", "103.2"),
        long_quality=DataQualityAssessment(True, ReasonCode.QUOTE_READY, 0),
        short_quality=DataQualityAssessment(True, ReasonCode.QUOTE_READY, 0),
        long_funding=FundingSnapshot(
            Venue.OKX,
            long.symbol,
            Decimal(0),
            next_funding,
            "8h",
            Decimal(100),
            Decimal(100),
            int(_NOW.timestamp() * 1000),
        ),
        short_funding=FundingSnapshot(
            Venue.BYBIT,
            short.symbol,
            Decimal(0),
            next_funding,
            "8h",
            Decimal(100),
            Decimal(100),
            int(_NOW.timestamp() * 1000),
        ),
        observed_monotonic_ns=1_000_000_000 + offset_ns,
        unavailable_venues=frozenset(),
    )


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
        reserves_per_base=CostReserves(*(Decimal(0) for _ in range(9))),
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


def _live_binding(intent_model_sha256: str) -> AggressiveQualificationBinding:
    geometry = AggressiveDirectionBinding(
        "BTC:okx>bybit",
        tuple(Decimal(index) for index in range(1, 6)),
        tuple(Decimal(index) for index in range(5)),
        (Decimal("0.10"), Decimal("0.15"), Decimal("0.20"), Decimal("0.25"), Decimal("0.30")),
        Decimal(6),
    )
    return AggressiveQualificationBinding(
        schema_version=1,
        generated_at=_NOW,
        qualification_hash="1" * 64,
        qualification_data_sha256="2" * 64,
        release_sha="3" * 40,
        source_sha256="4" * 64,
        config_sha256="5" * 64,
        runtime_artifact_digest="sha256:" + "6" * 64,
        decision_runtime_sha256="7" * 64,
        model_sha256=intent_model_sha256,
        source_manifest_sha256="8" * 64,
        reference_manifest_sha256="9" * 64,
        profile_sha256="a" * 64,
        qualification_route="BTC:bybit>okx",
        positive=geometry,
        negative=replace(geometry, route_identity="BTC:bybit>okx"),
        accepted=True,
        binding_sha256="b" * 64,
    )


def test_live_canary_and_pilot_consume_the_same_immutable_intent_without_submit() -> None:
    decision = _accepted(AggressiveRuntimeMode.LIVE)
    assert decision.intent is not None
    intent = replace(
        decision.intent,
        strategy_profile_sha256="a" * 64,
        source_manifest_sha256="8" * 64,
        reference_manifest_sha256="9" * 64,
        runtime_manifest_sha256="7" * 64,
        projected_route_loss_usdt=Decimal("0.5"),
        projected_portfolio_loss_usdt=Decimal("0.5"),
    )
    binding = _live_binding(intent.model_sha256)
    canary = prepare_aggressive_live_plan(
        intent,
        binding,
        _instrument(Venue.OKX),
        _instrument(Venue.BYBIT),
        long_protected_price=Decimal("100.1"),
        short_protected_price=Decimal("102.9"),
        stage=AggressiveLaptopLiveStage.CANARY,
        timeout_seconds=30,
    )
    assert canary.route.value == intent.route_identity
    assert canary.quantity == intent.quantity
    assert canary.risk_reservation["aggressive_intent_sha256"] == aggressive_intent_sha256(intent)
    assert canary.risk_reservation["execution_authorized"] is False
    assert canary.long_request.time_in_force == "IOC"
    assert canary.short_request.time_in_force == "IOC"

    deeper = replace(
        intent,
        level_index=5,
        projected_route_loss_usdt=Decimal("4.5"),
        projected_portfolio_loss_usdt=Decimal("4.5"),
    )
    with pytest.raises(ValueError, match="level exceeds"):
        prepare_aggressive_live_plan(
            deeper,
            binding,
            _instrument(Venue.OKX),
            _instrument(Venue.BYBIT),
            long_protected_price=Decimal("100.1"),
            short_protected_price=Decimal("102.9"),
            stage=AggressiveLaptopLiveStage.CANARY,
            timeout_seconds=30,
        )
    pilot = prepare_aggressive_live_plan(
        deeper,
        binding,
        _instrument(Venue.OKX),
        _instrument(Venue.BYBIT),
        long_protected_price=Decimal("100.1"),
        short_protected_price=Decimal("102.9"),
        stage=AggressiveLaptopLiveStage.PILOT_A,
        timeout_seconds=30,
    )
    assert pilot.risk_reservation["stage"] == "pilot_a"


def test_live_intent_envelope_is_hash_protected_and_non_authorizing(tmp_path: Path) -> None:
    decision = _accepted(AggressiveRuntimeMode.LIVE)
    assert decision.intent is not None
    envelope = AggressiveLiveIntentEnvelope(
        1,
        _NOW,
        "a" * 64,
        "b" * 64,
        decision.intent,
        aggressive_intent_sha256(decision.intent),
    )
    path = tmp_path / "intent.json"
    save_aggressive_live_intent(path, envelope)
    assert load_aggressive_live_intent(path) == envelope
    assert not envelope.intent.execution_authorized
    payload = path.read_text(encoding="utf-8").replace('"level_index": 1', '"level_index": 2')
    path.write_text(payload, encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        load_aggressive_live_intent(path)


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


def test_later_level_uses_first_level_frozen_route_allocation() -> None:
    first = _accepted(AggressiveRuntimeMode.LIVE)
    assert first.intent is not None
    frozen = FrozenGridSizingPlan(
        first.intent.route_identity,
        first.intent.model_sha256,
        first.sizing.full_route_base_quantity,
        first.sizing.tranche_base_quantities,
        first.sizing.tranche_projected_losses_usdt,
        first.sizing.projected_margin_usdt,
        first.intent.decided_at,
    )
    core = AggressiveDecisionCore(
        load_aggressive_decision_policy(Path("config/AGGRESSIVE_SYMBIOSIS_V1.yaml")).policy
    )
    second = None
    model = _model()
    for offset in (0, 250_000_000, 500_000_000):
        request = _request(AggressiveRuntimeMode.LIVE, offset)
        request = replace(
            request,
            proposal=replace(
                request.proposal,
                level_index=2,
                reference_trigger_bps=model.positive.levels_bps[1],
                reference_spread_bps=model.positive.levels_bps[-1] + Decimal(1),
            ),
            sizing=replace(
                request.sizing,
                existing_route_loss_usdt=first.sizing.tranche_projected_losses_usdt[0],
                existing_portfolio_loss_usdt=first.sizing.tranche_projected_losses_usdt[0],
                frozen_route_sizing=frozen,
            ),
            decision_cycle=2,
        )
        second = core.evaluate(request)

    assert second is not None and second.accepted
    assert second.intent is not None
    assert second.intent.quantity == first.sizing.tranche_base_quantities[1]
    assert second.sizing.tranche_base_quantities == first.sizing.tranche_base_quantities


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


def test_public_engine_raw_market_bridge_drives_the_same_core(tmp_path: Path) -> None:
    model = _model()
    grid = AggressiveGridStore(tmp_path / "grid.sqlite3")
    grid.initialise()
    grid.initialise_route(
        model,
        DivergenceDirection.POSITIVE,
        now=_NOW,
        rearm_retreat_step_fraction=Decimal("0.25"),
    )
    policy = load_aggressive_decision_policy(Path("config/AGGRESSIVE_SYMBIOSIS_V1.yaml")).policy
    bridge = AggressiveShadowDecisionBridge(AggressiveDecisionCore(policy), grid)
    decision = None
    for offset in (0, 250_000_000, 500_000_000):
        decision = bridge.evaluate(
            AggressiveShadowDecisionInput(
                model=model,
                reference_bar=_bar(19),
                market=_market(offset),
                effective_stop_bps=model.positive.reference_stop_bps,
                reserves=CostReserves(*(Decimal(0) for _ in range(9))),
                existing_route_loss_usdt=Decimal(0),
                existing_portfolio_loss_usdt=Decimal(0),
                free_margin_usdt=Decimal(100),
                decision_cycle=7,
                runtime_manifest_sha256="runtime",
                maximum_book_age_ms=1000,
                now=_NOW,
            )
        )
    assert decision is not None and decision.accepted
    assert decision.intent is not None
    assert decision.intent.route_identity == model.positive_route
    portfolio = AggressiveShadowPortfolio(grid, policy)
    portfolio.open(decision)
    assert grid.levels(model.positive_route)[0].state == GridLevelState.OPEN

    closing_market = replace(
        _market(600_000_000),
        long_book=_book(Venue.OKX, "100", "100.1"),
        short_book=_book(Venue.BYBIT, "100.4", "100.5"),
    )
    stop_bar = replace(_bar(20), high_bps=Decimal(1200))
    closed = portfolio.close_due(
        model=model,
        reference_bar=stop_bar,
        market=closing_market,
        now=_NOW + timedelta(minutes=1),
        projected_portfolio_loss_usdt=Decimal(0),
    )
    assert closed == ((1, AggressiveExitReason.HARD_PROJECTED_LOSS_OR_REFERENCE_STOP),)
    assert grid.levels(model.positive_route)[0].state == GridLevelState.CLOSED_WAIT_REARM
    assert portfolio.rearm_stable_flat(
        model.positive_route,
        reference_spread_bps=Decimal(100),
        now=_NOW + timedelta(minutes=2),
    ) == (1,)
    assert grid.levels(model.positive_route)[0].state == GridLevelState.ARMED


def test_public_market_bridge_rejects_wrong_reference_contract_identity(
    tmp_path: Path,
) -> None:
    model = _model()
    grid = AggressiveGridStore(tmp_path / "grid.sqlite3")
    grid.initialise()
    grid.initialise_route(
        model,
        DivergenceDirection.POSITIVE,
        now=_NOW,
        rearm_retreat_step_fraction=Decimal("0.25"),
    )
    bridge = AggressiveShadowDecisionBridge(
        AggressiveDecisionCore(
            load_aggressive_decision_policy(Path("config/AGGRESSIVE_SYMBIOSIS_V1.yaml")).policy
        ),
        grid,
    )
    wrong = replace(_bar(19), contract_metadata_version_b="wrong")
    result = bridge.evaluate(
        AggressiveShadowDecisionInput(
            model=model,
            reference_bar=wrong,
            market=_market(),
            effective_stop_bps=model.positive.reference_stop_bps,
            reserves=CostReserves(*(Decimal(0) for _ in range(9))),
            existing_route_loss_usdt=Decimal(0),
            existing_portfolio_loss_usdt=Decimal(0),
            free_margin_usdt=Decimal(100),
            decision_cycle=8,
            runtime_manifest_sha256="runtime",
            maximum_book_age_ms=1000,
            now=_NOW,
        )
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
