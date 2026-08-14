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
from interexchange_perp_grid.market_data import DataQualityAssessment
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.routes import evaluate_directed_route, match_common_instruments


def instrument(venue: Venue, contract_size: str, amount_step: str, fee: str) -> Instrument:
    return Instrument(
        venue=venue,
        symbol="BTC/USDT:USDT",
        exchange_symbol=f"{venue.value}-BTC",
        base="BTC",
        quote="USDT",
        settle="USDT",
        contract_size_base=Decimal(contract_size),
        amount_step_contracts=Decimal(amount_step),
        price_tick=Decimal("0.1"),
        minimum_amount_contracts=Decimal(amount_step),
        minimum_notional=None,
        taker_fee_rate=Decimal(fee),
        fee_source="fixture",
    )


def book(venue: Venue, bid: str, ask: str) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        venue=venue,
        symbol="BTC/USDT:USDT",
        bids=(
            BookLevel(Decimal(bid), Decimal("0.001")),
            BookLevel(Decimal(bid) - 1, Decimal("1")),
        ),
        asks=(
            BookLevel(Decimal(ask), Decimal("0.001")),
            BookLevel(Decimal(ask) + 1, Decimal("1")),
        ),
        exchange_timestamp_ms=1_700_000_000_000,
        received_at=datetime.now(UTC),
        received_monotonic_ns=1,
        sequence_start=1,
        sequence_end=1,
        is_snapshot=True,
        synchronised=True,
        clock_skew_ms=0,
    )


def funding(venue: Venue, rate: str) -> FundingSnapshot:
    return FundingSnapshot(
        venue=venue,
        symbol="BTC/USDT:USDT",
        rate=Decimal(rate),
        next_funding_timestamp_ms=1_700_000_100_000,
        interval="8h",
        mark_price=Decimal("101"),
        index_price=Decimal("100.9"),
        exchange_timestamp_ms=1_700_000_000_000,
    )


def test_directed_vwap_honours_contract_steps_depth_fees_and_funding() -> None:
    bybit = instrument(Venue.BYBIT, "1", "0.001", "0.0006")
    okx = instrument(Venue.OKX, "0.01", "0.01", "0.0005")
    accepted = DataQualityAssessment(True, ReasonCode.QUOTE_READY, 1)
    quote = evaluate_directed_route(
        bybit,
        okx,
        book(Venue.BYBIT, "100", "101"),
        book(Venue.OKX, "103", "104"),
        funding(Venue.BYBIT, "0.0001"),
        funding(Venue.OKX, "0.0002"),
        accepted,
        accepted,
        Decimal("0.0027"),
    )
    assert quote.eligible is True
    assert quote.base_quantity == Decimal("0.002")
    assert quote.entry_long_vwap == Decimal("101.5")
    assert quote.entry_short_vwap == Decimal("102.5")
    assert quote.entry_spread == Decimal("1.0")
    assert quote.funding_rate_delta == Decimal("0.0001")
    assert quote.four_leg_fee_estimate is not None


def test_directed_routes_are_not_symmetric() -> None:
    bybit = instrument(Venue.BYBIT, "1", "0.001", "0.0006")
    okx = instrument(Venue.OKX, "0.01", "0.01", "0.0005")
    accepted = DataQualityAssessment(True, ReasonCode.QUOTE_READY, 1)
    forward = evaluate_directed_route(
        bybit,
        okx,
        book(Venue.BYBIT, "100", "101"),
        book(Venue.OKX, "103", "104"),
        funding(Venue.BYBIT, "0.0001"),
        funding(Venue.OKX, "0.0002"),
        accepted,
        accepted,
        Decimal("0.001"),
    )
    reverse = evaluate_directed_route(
        okx,
        bybit,
        book(Venue.OKX, "103", "104"),
        book(Venue.BYBIT, "100", "101"),
        funding(Venue.OKX, "0.0002"),
        funding(Venue.BYBIT, "0.0001"),
        accepted,
        accepted,
        Decimal("0.001"),
    )
    assert forward.entry_spread == Decimal("2")
    assert reverse.entry_spread == Decimal("-4")


def test_unknown_mark_or_index_blocks_route() -> None:
    bybit = instrument(Venue.BYBIT, "1", "0.001", "0.0006")
    okx = instrument(Venue.OKX, "0.01", "0.01", "0.0005")
    accepted = DataQualityAssessment(True, ReasonCode.QUOTE_READY, 1)
    unknown_mark = replace(funding(Venue.BYBIT, "0.0001"), mark_price=None)
    quote = evaluate_directed_route(
        bybit,
        okx,
        book(Venue.BYBIT, "100", "101"),
        book(Venue.OKX, "103", "104"),
        unknown_mark,
        funding(Venue.OKX, "0.0002"),
        accepted,
        accepted,
        Decimal("0.001"),
    )
    assert quote.eligible is False
    assert quote.reason == ReasonCode.MARK_INDEX_UNKNOWN


def test_ambiguous_contract_mapping_is_rejected() -> None:
    bybit = instrument(Venue.BYBIT, "1", "0.001", "0.0006")
    duplicate = replace(bybit, exchange_symbol="duplicate")
    okx = instrument(Venue.OKX, "0.01", "0.01", "0.0005")
    assert match_common_instruments({Venue.BYBIT: (bybit, duplicate), Venue.OKX: (okx,)}) == ()
