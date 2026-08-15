from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

from interexchange_perp_grid.domain import (
    BookLevel,
    FundingSnapshot,
    Instrument,
    OrderBookSnapshot,
    Venue,
)
from interexchange_perp_grid.live_economics import (
    EmergencyVenueAssessment,
    LiveEconomicDecision,
    LiveEconomicPolicy,
    _funding_cost,
    evaluate_emergency_venue,
    evaluate_live_entry,
)
from interexchange_perp_grid.market_data import DataQualityAssessment
from interexchange_perp_grid.qualification import QualifiedStrategyParameters
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.risk import RiskDecision
from interexchange_perp_grid.routes import evaluate_directed_route


def _instrument(venue: Venue) -> Instrument:
    return Instrument(
        venue=venue,
        symbol="BTC/USDT:USDT",
        exchange_symbol="BTCUSDT",
        base="BTC",
        quote="USDT",
        settle="USDT",
        contract_size_base=Decimal("0.001"),
        amount_step_contracts=Decimal("1"),
        price_tick=Decimal("0.1"),
        minimum_amount_contracts=Decimal("1"),
        minimum_notional=Decimal("0.01"),
        taker_fee_rate=Decimal("0.0005"),
        fee_source="public",
    )


def _book(venue: Venue, bids: tuple[str, ...], asks: tuple[str, ...]) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        venue=venue,
        symbol="BTC/USDT:USDT",
        bids=tuple(BookLevel(Decimal(price), Decimal("0.001")) for price in bids),
        asks=tuple(BookLevel(Decimal(price), Decimal("0.001")) for price in asks),
        exchange_timestamp_ms=1_700_000_000_000,
        received_at=datetime.now(UTC),
        received_monotonic_ns=1,
        sequence_start=1,
        sequence_end=1,
        is_snapshot=True,
        synchronised=True,
        clock_skew_ms=0,
    )


def _funding(venue: Venue, rate: str) -> FundingSnapshot:
    return FundingSnapshot(
        venue,
        "BTC/USDT:USDT",
        Decimal(rate),
        1_700_028_800_000,
        "8h",
        Decimal("101"),
        Decimal("101"),
        1_700_000_000_000,
    )


def _strategy(threshold: str = "5") -> QualifiedStrategyParameters:
    return QualifiedStrategyParameters(
        calibration_version=9,
        size_bucket_base_quantity=Decimal("0.002"),
        adaptive_entry_threshold_bps=Decimal(threshold),
        target_exit_spread_bps=Decimal("2"),
        minimum_profit_usdt=Decimal("0.001"),
        stressed_cost_multiplier=Decimal("2"),
        expected_holding_seconds=300,
        maximum_holding_seconds=3600,
    )


def _policy() -> LiveEconomicPolicy:
    return LiveEconomicPolicy(
        entry_slippage_cap_bps=Decimal("5"),
        latency_reserve_bps=Decimal("2"),
        partial_fill_reserve_bps=Decimal("3"),
        emergency_hedge_reserve_bps=Decimal("10"),
        reconciliation_forced_exit_reserve_bps=Decimal("10"),
        funding_stress_multiplier=Decimal("2"),
        minimum_profit_usdt=Decimal("0.001"),
    )


def _decision(
    *,
    strategy: QualifiedStrategyParameters | None = None,
    fees: dict[Venue, Decimal] | None = None,
    long_book: OrderBookSnapshot | None = None,
    emergency: EmergencyVenueAssessment | None = None,
) -> LiveEconomicDecision:
    long_instrument = _instrument(Venue.BINANCE_USDM)
    short_instrument = _instrument(Venue.OKX)
    selected_long_book = long_book or _book(Venue.BINANCE_USDM, ("99.9", "99.8"), ("100", "100.2"))
    short_book = _book(Venue.OKX, ("103", "102.8"), ("103.1", "103.2"))
    long_funding = _funding(Venue.BINANCE_USDM, "0.0001")
    short_funding = _funding(Venue.OKX, "0.0001")
    quality = DataQualityAssessment(True, ReasonCode.QUOTE_READY, 0)
    quote = evaluate_directed_route(
        long_instrument,
        short_instrument,
        selected_long_book,
        short_book,
        long_funding,
        short_funding,
        quality,
        quality,
        Decimal("0.002"),
    )
    return evaluate_live_entry(
        quote,
        long_instrument,
        short_instrument,
        selected_long_book,
        short_book,
        long_funding,
        short_funding,
        fees
        or {
            Venue.BINANCE_USDM: Decimal("0.0004"),
            Venue.OKX: Decimal("0.0005"),
        },
        strategy or _strategy(),
        _policy(),
        RiskDecision(True, ReasonCode.RISK_RESERVED, {"stress": Decimal("0.5")}),
        emergency_assessment=emergency,
    )


def test_live_uses_full_private_fee_economics_and_marginal_protected_prices() -> None:
    decision = _decision()
    assert decision.accepted is True
    assert decision.signal is not None
    assert decision.signal.cost.expected_net_pnl_usdt > 0
    assert decision.signal.cost.partial_fill_reserve_usdt > 0
    assert decision.signal.cost.emergency_hedge_reserve_usdt > 0
    assert decision.long_marginal_price == Decimal("100.2")
    assert decision.long_protected_price == Decimal("100.3")
    assert decision.short_marginal_price == Decimal("102.8")
    assert decision.short_protected_price == Decimal("102.7")


def test_live_rejects_below_adaptive_threshold_and_private_fee_cost() -> None:
    threshold = _decision(strategy=_strategy("1000"))
    assert threshold.accepted is False
    assert threshold.reason == ReasonCode.ADAPTIVE_THRESHOLD_NOT_MET

    expensive = _decision(
        fees={
            Venue.BINANCE_USDM: Decimal("0.10"),
            Venue.OKX: Decimal("0.10"),
        }
    )
    assert expensive.accepted is False
    assert expensive.reason == ReasonCode.GROSS_BELOW_COST_FLOOR


def test_price_movement_recomputes_cap_from_new_marginal_level() -> None:
    moved_book = _book(
        Venue.BINANCE_USDM,
        ("99.9", "99.8"),
        ("100.4", "100.6"),
    )
    moved = _decision(long_book=moved_book)
    assert moved.long_marginal_price == Decimal("100.6")
    assert moved.long_protected_price == Decimal("100.7")
    assert moved.long_protected_price != _decision().long_protected_price


def test_funding_cost_counts_the_actual_next_checkpoint_not_a_fractional_interval() -> None:
    now_ms = 1_700_000_000_000
    long_funding = FundingSnapshot(
        Venue.BINANCE_USDM,
        "BTC/USDT:USDT",
        Decimal("0.0001"),
        now_ms + 60_000,
        "8h",
        Decimal("100"),
        Decimal("100"),
        now_ms,
    )
    short_funding = FundingSnapshot(
        Venue.OKX,
        "BTC/USDT:USDT",
        Decimal("0.00005"),
        now_ms + 60_000,
        "8h",
        Decimal("100"),
        Decimal("100"),
        now_ms,
    )
    assert _funding_cost(
        Decimal("1000"),
        long_funding,
        short_funding,
        300,
        stress=False,
        multiplier=Decimal(1),
    ) == Decimal("0.05000")
    assert _funding_cost(
        Decimal("1000"),
        long_funding,
        short_funding,
        300,
        stress=True,
        multiplier=Decimal(2),
    ) == Decimal("0.30000")


def _emergency_assessment(
    *,
    instrument: Instrument | None = None,
    book: OrderBookSnapshot | None = None,
    fee: Decimal | None = Decimal("0.0006"),
) -> EmergencyVenueAssessment:
    return evaluate_emergency_venue(
        _instrument(Venue.BINANCE_USDM),
        _instrument(Venue.OKX),
        instrument or _instrument(Venue.BYBIT),
        book
        or _book(
            Venue.BYBIT,
            ("99.9", "99.8"),
            ("100", "100.2"),
        ),
        fee,
        Decimal("0.002"),
        capability_ready=True,
        account_ready=True,
        data_quality_ready=True,
        slippage_cap_bps=Decimal("8"),
    )


def test_third_venue_exact_fee_depth_and_round_trip_cost_are_in_stress() -> None:
    assessment = _emergency_assessment()
    assert assessment.passed is True
    assert assessment.residual_quantities == (Decimal("0.001"), Decimal("0.002"))
    assert assessment.fee_rate == Decimal("0.0006")
    assert assessment.worst_hedge_and_flatten_cost_usdt > 0
    assert assessment.buy_protected_price is not None
    assert assessment.sell_protected_price is not None

    decision = _decision(emergency=assessment)
    baseline = _decision()
    assert decision.signal is not None
    assert baseline.signal is not None
    assert (
        decision.signal.cost.emergency_hedge_reserve_usdt
        == baseline.signal.cost.emergency_hedge_reserve_usdt
        + assessment.worst_hedge_and_flatten_cost_usdt
    )


def test_third_venue_blocks_when_any_quantized_residual_is_not_executable() -> None:
    third = replace(
        _instrument(Venue.BYBIT),
        contract_size_base=Decimal("0.002"),
    )
    assessment = _emergency_assessment(instrument=third)
    assert assessment.passed is False
    assert assessment.reason == ReasonCode.RESIDUAL_NOT_EXECUTABLE
    assert assessment.checks["all_residual_steps"] is False
    assert _decision(emergency=assessment).accepted is False


def test_third_venue_blocks_when_either_side_depth_cannot_cover_maximum_residual() -> None:
    shallow = OrderBookSnapshot(
        venue=Venue.BYBIT,
        symbol="BTC/USDT:USDT",
        bids=_book(Venue.BYBIT, ("99.9",), ("100",)).bids,
        asks=_book(Venue.BYBIT, ("99.9",), ("100",)).asks,
        exchange_timestamp_ms=1_700_000_000_000,
        received_at=datetime.now(UTC),
        received_monotonic_ns=1,
        sequence_start=1,
        sequence_end=1,
        is_snapshot=True,
        synchronised=True,
        clock_skew_ms=0,
    )
    assessment = _emergency_assessment(book=shallow)
    assert assessment.passed is False
    assert assessment.reason == ReasonCode.EMERGENCY_VENUE_PREFLIGHT_FAILED
    assert assessment.checks["both_side_depth"] is False
