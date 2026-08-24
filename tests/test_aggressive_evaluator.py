from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from hypothesis import given
from hypothesis import strategies as st

from interexchange_perp_grid.aggressive_evaluator import (
    AggressiveDecisionPolicy,
    AggressiveEconomicDecision,
    AggressiveEntryReason,
    AggressiveExitInput,
    AggressiveExitReason,
    CostReserves,
    CrossingConfirmationTracker,
    HybridEntryInput,
    RouteScoreCandidate,
    VenueFundingProjection,
    evaluate_hybrid_entry,
    load_aggressive_decision_policy,
    route_score,
    select_aggressive_exit_reason,
    select_route_candidate,
    size_aggressive_grid,
)
from interexchange_perp_grid.aggressive_model import DivergenceDirection
from interexchange_perp_grid.domain import BookLevel, OrderBookSnapshot, Venue

_NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _book(venue: Venue, bid: str, ask: str, *, age_ms: int = 0) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        venue=venue,
        symbol="BTC/USDT:USDT",
        bids=(BookLevel(Decimal(bid), Decimal("2")),),
        asks=(BookLevel(Decimal(ask), Decimal("2")),),
        exchange_timestamp_ms=1,
        received_at=_NOW - timedelta(milliseconds=age_ms),
        received_monotonic_ns=1_000_000_000 - age_ms * 1_000_000,
        sequence_start=1,
        sequence_end=1,
        is_snapshot=True,
        synchronised=True,
        clock_skew_ms=0,
    )


def _reserves(value: str = "0.01") -> CostReserves:
    amount = Decimal(value)
    return CostReserves(amount, amount, amount, amount, amount, amount, amount, amount, amount)


def _funding(venue: Venue, rate: str = "0") -> VenueFundingProjection:
    return VenueFundingProjection(
        venue=venue,
        rate=Decimal(rate),
        mark_price=Decimal("100"),
        event_count=1,
        next_funding_timestamp_ms=1,
        interval_seconds=28_800,
    )


def _proposal(**changes: object) -> HybridEntryInput:
    values: dict[str, object] = {
        "route_identity": "BTC:okx>bybit",
        "direction": DivergenceDirection.POSITIVE,
        "level_index": 1,
        "reference_spread_bps": Decimal("2.1"),
        "reference_trigger_bps": Decimal("2"),
        "reverse_target_bps": Decimal("2"),
        "quantity": Decimal("1"),
        "long_venue": Venue.OKX,
        "short_venue": Venue.BYBIT,
        "long_book": _book(Venue.OKX, "99.9", "100"),
        "short_book": _book(Venue.BYBIT, "101", "101.1"),
        "long_private_taker_fee_rate": Decimal("0.0004"),
        "short_private_taker_fee_rate": Decimal("0.0004"),
        "long_funding": _funding(Venue.OKX),
        "short_funding": _funding(Venue.BYBIT),
        "reserves": _reserves(),
        "observed_monotonic_ns": 1_000_000_000,
        "maximum_book_age_ms": 1000,
        "now": _NOW,
    }
    values.update(changes)
    return HybridEntryInput(**values)  # type: ignore[arg-type]


def _policy() -> AggressiveDecisionPolicy:
    return load_aggressive_decision_policy(Path("config/AGGRESSIVE_SYMBIOSIS_V1.yaml")).policy


def test_locked_policy_loads_all_economic_and_risk_constants() -> None:
    loaded = load_aggressive_decision_policy(Path("config/AGGRESSIVE_SYMBIOSIS_V1.yaml"))
    assert loaded.policy.confirmation_snapshots == 3
    assert loaded.policy.confirmation_minimum_elapsed_ms == 500
    assert loaded.policy.stressed_cost_multiplier == Decimal("1.35")
    assert loaded.policy.route_modelled_loss_limit_usdt == Decimal("4.50")
    assert loaded.policy.portfolio_hard_projected_loss_limit_usdt == Decimal("50.00")
    assert len(loaded.profile_sha256) == 64


def test_hybrid_entry_requires_three_fresh_l2_decisions_spanning_500ms() -> None:
    policy = _policy()
    tracker = CrossingConfirmationTracker(
        policy.confirmation_snapshots, policy.confirmation_minimum_elapsed_ms
    )
    decisions = [
        evaluate_hybrid_entry(
            _proposal(observed_monotonic_ns=1_000_000_000 + offset),
            policy=policy,
            confirmations=tracker,
        )
        for offset in (0, 250_000_000, 500_000_000)
    ]
    assert [decision.reason for decision in decisions[:2]] == [
        AggressiveEntryReason.CONFIRMATION_INSUFFICIENT,
        AggressiveEntryReason.CONFIRMATION_INSUFFICIENT,
    ]
    accepted = decisions[2]
    assert accepted.accepted
    assert accepted.reason == AggressiveEntryReason.ACCEPTED
    assert accepted.long_entry_vwap == Decimal("100")
    assert accepted.short_entry_vwap == Decimal("101")
    assert accepted.expected_gross_convergence_pnl_usdt > 0
    assert accepted.expected_net_pnl_usdt >= Decimal("0.15")
    assert not accepted.execution_authorized


def test_reference_high_alone_never_opens_without_executable_books() -> None:
    policy = _policy()
    tracker = CrossingConfirmationTracker(3, 500)
    stale = _proposal(long_book=_book(Venue.OKX, "99.9", "100", age_ms=1001))
    for offset in (0, 250_000_000, 500_000_000):
        result = evaluate_hybrid_entry(
            replace(stale, observed_monotonic_ns=1_000_000_000 + offset),
            policy=policy,
            confirmations=tracker,
        )
        assert not result.accepted
        assert result.reason == AggressiveEntryReason.BOOK_STALE


def test_unknown_fee_funding_or_depth_fail_closed() -> None:
    policy = _policy()
    cases = (
        (_proposal(long_private_taker_fee_rate=None), AggressiveEntryReason.PRIVATE_FEE_UNKNOWN),
        (_proposal(long_funding=None), AggressiveEntryReason.FUNDING_UNKNOWN),
        (_proposal(quantity=Decimal("3")), AggressiveEntryReason.DEPTH_INSUFFICIENT),
    )
    for proposal, expected in cases:
        tracker = CrossingConfirmationTracker(3, 500)
        for offset in (0, 250_000_000):
            evaluate_hybrid_entry(
                replace(proposal, observed_monotonic_ns=1_000_000_000 + offset),
                policy=policy,
                confirmations=tracker,
            )
        result = evaluate_hybrid_entry(
            replace(proposal, observed_monotonic_ns=1_500_000_000),
            policy=policy,
            confirmations=tracker,
        )
        assert result.reason == expected
        assert not result.accepted


def test_state_history_regime_and_route_identity_fail_before_confirmation() -> None:
    policy = _policy()
    cases = (
        (_proposal(state_reconciled=False), AggressiveEntryReason.STATE_UNHEALTHY),
        (
            _proposal(historical_model_eligible=False),
            AggressiveEntryReason.HISTORICAL_MODEL_INELIGIBLE,
        ),
        (_proposal(regime_ready=False), AggressiveEntryReason.REGIME_BLOCKED),
        (
            _proposal(route_identity="BTC:bybit>okx"),
            AggressiveEntryReason.ROUTE_IDENTITY_MISMATCH,
        ),
    )
    for proposal, reason in cases:
        result = evaluate_hybrid_entry(
            proposal,
            policy=policy,
            confirmations=CrossingConfirmationTracker(3, 500),
        )
        assert not result.accepted
        assert result.reason == reason


def test_funding_is_asymmetric_and_positive_credit_cannot_rescue_gross_gate() -> None:
    policy = _policy()

    def final(proposal: HybridEntryInput) -> AggressiveEconomicDecision:
        tracker = CrossingConfirmationTracker(3, 500)
        result = None
        for offset in (0, 250_000_000, 500_000_000):
            result = evaluate_hybrid_entry(
                replace(proposal, observed_monotonic_ns=1_000_000_000 + offset),
                policy=policy,
                confirmations=tracker,
            )
        assert result is not None
        return result

    favorable = final(_proposal(short_funding=_funding(Venue.BYBIT, "0.002")))
    adverse = final(_proposal(short_funding=_funding(Venue.BYBIT, "-0.002")))
    assert favorable.favorable_funding_credit_usdt == Decimal("0.1000")
    assert adverse.favorable_funding_credit_usdt == 0
    assert adverse.stressed_total_cost_usdt - favorable.stressed_total_cost_usdt == Decimal(
        "0.4000"
    )
    nonconvergent = final(
        _proposal(
            reverse_target_bps=Decimal("99.5033085316808409"),
            short_funding=_funding(Venue.BYBIT, "1"),
        )
    )
    assert not nonconvergent.accepted


def test_grid_sizing_respects_modelled_hard_portfolio_and_margin_limits() -> None:
    policy = _policy()
    result = size_aggressive_grid(
        direction_levels_bps=tuple(Decimal(index * 2) for index in range(1, 6)),
        tranche_weights=(
            Decimal("0.10"),
            Decimal("0.15"),
            Decimal("0.20"),
            Decimal("0.25"),
            Decimal("0.30"),
        ),
        effective_stop_bps=Decimal("11.5"),
        reference_price=Decimal("100"),
        quantity_step=Decimal("0.001"),
        minimum_base_quantity=Decimal("0.001"),
        minimum_notional_usdt=Decimal("0.01"),
        per_full_base_reserve_usdt=Decimal("1"),
        existing_route_loss_usdt=Decimal("0"),
        existing_portfolio_loss_usdt=Decimal("40"),
        free_margin_usdt=Decimal("100"),
        policy=policy,
    )
    assert result.accepted
    assert result.projected_route_loss_usdt <= Decimal("4.50")
    assert result.projected_route_loss_usdt < Decimal("5.00")
    assert result.projected_portfolio_loss_usdt <= Decimal("45.00")
    assert result.projected_portfolio_loss_usdt < Decimal("50.00")
    assert result.projected_margin_usdt <= Decimal("80")
    assert len(result.tranche_base_quantities) == 5
    assert not result.execution_authorized


def test_grid_sizing_fails_when_residual_budget_cannot_fund_any_level() -> None:
    policy = _policy()
    result = size_aggressive_grid(
        direction_levels_bps=tuple(Decimal(index * 2) for index in range(1, 6)),
        tranche_weights=(
            Decimal("0.10"),
            Decimal("0.15"),
            Decimal("0.20"),
            Decimal("0.25"),
            Decimal("0.30"),
        ),
        effective_stop_bps=Decimal("11.5"),
        reference_price=Decimal("100"),
        quantity_step=Decimal("1"),
        minimum_base_quantity=Decimal("1"),
        minimum_notional_usdt=Decimal("100"),
        per_full_base_reserve_usdt=Decimal("10"),
        existing_route_loss_usdt=Decimal("4.49"),
        existing_portfolio_loss_usdt=Decimal("44.99"),
        free_margin_usdt=Decimal("1"),
        policy=policy,
    )
    assert not result.accepted
    assert result.full_route_base_quantity == 0


@given(
    route_loss=st.decimals(min_value="0", max_value="4.49", places=2),
    portfolio_loss=st.decimals(min_value="0", max_value="44.99", places=2),
    free_margin=st.decimals(min_value="1", max_value="1000", places=2),
)
def test_accepted_sizing_never_exceeds_any_locked_boundary(
    route_loss: Decimal,
    portfolio_loss: Decimal,
    free_margin: Decimal,
) -> None:
    policy = _policy()
    result = size_aggressive_grid(
        direction_levels_bps=tuple(Decimal(index * 2) for index in range(1, 6)),
        tranche_weights=(
            Decimal("0.10"),
            Decimal("0.15"),
            Decimal("0.20"),
            Decimal("0.25"),
            Decimal("0.30"),
        ),
        effective_stop_bps=Decimal("11.5"),
        reference_price=Decimal("100"),
        quantity_step=Decimal("0.001"),
        minimum_base_quantity=Decimal("0.001"),
        minimum_notional_usdt=Decimal("0.01"),
        per_full_base_reserve_usdt=Decimal("1"),
        existing_route_loss_usdt=route_loss,
        existing_portfolio_loss_usdt=portfolio_loss,
        free_margin_usdt=free_margin,
        policy=policy,
    )
    if result.accepted:
        assert result.projected_route_loss_usdt <= Decimal("4.50")
        assert result.projected_route_loss_usdt < Decimal("5.00")
        assert result.projected_portfolio_loss_usdt <= Decimal("45.00")
        assert result.projected_portfolio_loss_usdt < Decimal("50.00")
        assert result.projected_margin_usdt <= free_margin * Decimal("0.80")


def test_route_score_uses_locked_formula() -> None:
    assert route_score(
        convergence_probability=Decimal("0.8"),
        expected_net_profit_usdt=Decimal("2"),
        projected_stress_usdt=Decimal("4"),
        expected_holding_hours=Decimal("0.1"),
    ) == Decimal("1.6")


def test_route_selection_uses_every_locked_tiebreaker_in_order() -> None:
    baseline = RouteScoreCandidate(
        route_identity="BTC:okx>bybit",
        score=Decimal("1"),
        executable_depth=Decimal("10"),
        total_slippage=Decimal("1"),
        data_latency_ms=Decimal("10"),
        total_fee=Decimal("0.1"),
        adverse_funding=Decimal("0.01"),
    )
    deeper = replace(baseline, route_identity="BTC:bybit>okx", executable_depth=Decimal("11"))
    assert select_route_candidate((baseline, deeper)) == deeper
    lexical = replace(baseline, route_identity="AAA:bybit>okx")
    assert select_route_candidate((baseline, lexical)) == lexical


def test_exit_priority_is_shared_and_hard_limits_precede_targets() -> None:
    policy = _policy()
    base = AggressiveExitInput(
        direction=DivergenceDirection.POSITIVE,
        executable_spread_bps=Decimal("1"),
        effective_stop_bps=Decimal("11.5"),
        reverse_target_bps=Decimal("2"),
        projected_route_loss_usdt=Decimal("0"),
        projected_portfolio_loss_usdt=Decimal("0"),
        holding_deadline=_NOW + timedelta(hours=24),
        now=_NOW,
        emergency_or_unknown=False,
        adverse_funding_destroys_profit=False,
    )
    assert select_aggressive_exit_reason(base, policy) == AggressiveExitReason.REVERSE_GRID_TARGET
    assert (
        select_aggressive_exit_reason(replace(base, projected_route_loss_usdt=Decimal("5")), policy)
        == AggressiveExitReason.HARD_PROJECTED_LOSS_OR_REFERENCE_STOP
    )
    assert (
        select_aggressive_exit_reason(
            replace(
                base,
                projected_route_loss_usdt=Decimal("5"),
                emergency_or_unknown=True,
            ),
            policy,
        )
        == AggressiveExitReason.EMERGENCY_OR_UNKNOWN
    )
    assert (
        select_aggressive_exit_reason(replace(base, now=_NOW + timedelta(hours=25)), policy)
        == AggressiveExitReason.HARD_HOLDING_TIME
    )
