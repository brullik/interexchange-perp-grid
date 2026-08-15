from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from itertools import permutations

from interexchange_perp_grid.domain import (
    BookLevel,
    CommonInstrument,
    FundingSnapshot,
    Instrument,
    InstrumentKey,
    OrderBookSnapshot,
    Venue,
)
from interexchange_perp_grid.market_data import DataQualityAssessment
from interexchange_perp_grid.reason_codes import ReasonCode


@dataclass(frozen=True, slots=True)
class VwapResult:
    price: Decimal
    quantity: Decimal
    marginal_price: Decimal


@dataclass(frozen=True, slots=True)
class DirectedRouteQuote:
    key: InstrumentKey
    long_venue: Venue
    short_venue: Venue
    base_quantity: Decimal
    eligible: bool
    reason: ReasonCode
    entry_long_vwap: Decimal | None = None
    entry_long_marginal_price: Decimal | None = None
    entry_short_vwap: Decimal | None = None
    entry_short_marginal_price: Decimal | None = None
    exit_long_vwap: Decimal | None = None
    exit_long_marginal_price: Decimal | None = None
    exit_short_vwap: Decimal | None = None
    exit_short_marginal_price: Decimal | None = None
    entry_spread: Decimal | None = None
    exit_spread: Decimal | None = None
    entry_spread_bps: Decimal | None = None
    four_leg_fee_estimate: Decimal | None = None
    funding_rate_delta: Decimal | None = None


def match_common_instruments(
    instruments_by_venue: dict[Venue, tuple[Instrument, ...]],
    minimum_venues: int = 2,
) -> tuple[CommonInstrument, ...]:
    candidates: dict[InstrumentKey, dict[Venue, list[Instrument]]] = {}
    for venue, instruments in instruments_by_venue.items():
        for instrument in instruments:
            candidates.setdefault(instrument.key, {}).setdefault(venue, []).append(instrument)
    common: list[CommonInstrument] = []
    for key, by_venue in candidates.items():
        unambiguous = {
            venue: venue_instruments[0]
            for venue, venue_instruments in by_venue.items()
            if len(venue_instruments) == 1
        }
        if len(unambiguous) < minimum_venues:
            continue
        common.append(
            CommonInstrument(
                key,
                tuple(sorted(unambiguous.values(), key=lambda item: item.venue.value)),
            )
        )
    return tuple(sorted(common, key=lambda item: (item.key.base, item.key.settle)))


def directed_pairs(common: CommonInstrument) -> tuple[tuple[Instrument, Instrument], ...]:
    return tuple(permutations(common.instruments, 2))


def executable_vwap(levels: tuple[BookLevel, ...], quantity: Decimal) -> VwapResult | None:
    if quantity <= 0:
        raise ValueError("VWAP quantity must be positive")
    remaining = quantity
    notional = Decimal(0)
    for level in levels:
        consumed = min(remaining, level.base_quantity)
        notional += consumed * level.price
        remaining -= consumed
        if remaining == 0:
            return VwapResult(notional / quantity, quantity, level.price)
    return None


def _common_decimal_step(first: Decimal, second: Decimal) -> Decimal:
    if first <= 0 or second <= 0:
        raise ValueError("amount steps must be positive")
    first_exponent = first.normalize().as_tuple().exponent
    second_exponent = second.normalize().as_tuple().exponent
    if not isinstance(first_exponent, int) or not isinstance(second_exponent, int):
        raise ValueError("amount steps must be finite")
    places = max(0, -first_exponent, -second_exponent)
    scale = 10**places
    first_integer = int(first * scale)
    second_integer = int(second * scale)
    common_integer = abs(first_integer * second_integer) // math.gcd(first_integer, second_integer)
    return Decimal(common_integer) / Decimal(scale)


def common_base_quantity(
    requested: Decimal,
    long_instrument: Instrument,
    short_instrument: Instrument,
) -> Decimal:
    step = _common_decimal_step(
        long_instrument.base_amount_step,
        short_instrument.base_amount_step,
    )
    return (requested / step).to_integral_value(rounding=ROUND_FLOOR) * step


def minimum_common_base_quantity(
    long_instrument: Instrument,
    short_instrument: Instrument,
    long_price: Decimal,
    short_price: Decimal,
) -> Decimal:
    minimum = max(
        long_instrument.minimum_base_amount,
        short_instrument.minimum_base_amount,
        (
            long_instrument.minimum_notional / long_price
            if long_instrument.minimum_notional is not None
            else Decimal(0)
        ),
        (
            short_instrument.minimum_notional / short_price
            if short_instrument.minimum_notional is not None
            else Decimal(0)
        ),
    )
    step = _common_decimal_step(
        long_instrument.base_amount_step,
        short_instrument.base_amount_step,
    )
    return (minimum / step).to_integral_value(rounding=ROUND_CEILING) * step


def evaluate_directed_route(
    long_instrument: Instrument,
    short_instrument: Instrument,
    long_book: OrderBookSnapshot,
    short_book: OrderBookSnapshot,
    long_funding: FundingSnapshot,
    short_funding: FundingSnapshot,
    long_quality: DataQualityAssessment,
    short_quality: DataQualityAssessment,
    requested_base_quantity: Decimal,
) -> DirectedRouteQuote:
    if not long_quality.accepted:
        return _rejected(
            long_instrument, short_instrument, requested_base_quantity, long_quality.reason
        )
    if not short_quality.accepted:
        return _rejected(
            long_instrument, short_instrument, requested_base_quantity, short_quality.reason
        )
    quantity = common_base_quantity(requested_base_quantity, long_instrument, short_instrument)
    if quantity <= 0 or quantity < max(
        long_instrument.minimum_base_amount,
        short_instrument.minimum_base_amount,
    ):
        return _rejected(
            long_instrument,
            short_instrument,
            quantity,
            ReasonCode.CONTRACT_METADATA_UNKNOWN,
        )
    if long_instrument.taker_fee_rate is None or short_instrument.taker_fee_rate is None:
        return _rejected(long_instrument, short_instrument, quantity, ReasonCode.FEE_UNKNOWN)
    if any(
        value is None
        for value in (
            long_funding.rate,
            long_funding.next_funding_timestamp_ms,
            short_funding.rate,
            short_funding.next_funding_timestamp_ms,
        )
    ):
        return _rejected(long_instrument, short_instrument, quantity, ReasonCode.FUNDING_UNKNOWN)
    if any(
        value is None
        for value in (
            long_funding.mark_price,
            long_funding.index_price,
            short_funding.mark_price,
            short_funding.index_price,
        )
    ):
        return _rejected(
            long_instrument,
            short_instrument,
            quantity,
            ReasonCode.MARK_INDEX_UNKNOWN,
        )
    entry_long = executable_vwap(long_book.asks, quantity)
    entry_short = executable_vwap(short_book.bids, quantity)
    exit_long = executable_vwap(long_book.bids, quantity)
    exit_short = executable_vwap(short_book.asks, quantity)
    if any(result is None for result in (entry_long, entry_short, exit_long, exit_short)):
        return _rejected(long_instrument, short_instrument, quantity, ReasonCode.DEPTH_INSUFFICIENT)
    assert entry_long is not None
    assert entry_short is not None
    assert exit_long is not None
    assert exit_short is not None
    if not _meets_notional(long_instrument, entry_long.price, quantity) or not _meets_notional(
        short_instrument, entry_short.price, quantity
    ):
        return _rejected(
            long_instrument,
            short_instrument,
            quantity,
            ReasonCode.CONTRACT_METADATA_UNKNOWN,
        )
    entry_spread = entry_short.price - entry_long.price
    exit_spread = exit_short.price - exit_long.price
    four_leg_fee = quantity * (
        (entry_long.price + exit_long.price) * long_instrument.taker_fee_rate
        + (entry_short.price + exit_short.price) * short_instrument.taker_fee_rate
    )
    funding_delta = short_funding.rate - long_funding.rate  # type: ignore[operator]
    return DirectedRouteQuote(
        key=long_instrument.key,
        long_venue=long_instrument.venue,
        short_venue=short_instrument.venue,
        base_quantity=quantity,
        eligible=True,
        reason=ReasonCode.QUOTE_READY,
        entry_long_vwap=entry_long.price,
        entry_long_marginal_price=entry_long.marginal_price,
        entry_short_vwap=entry_short.price,
        entry_short_marginal_price=entry_short.marginal_price,
        exit_long_vwap=exit_long.price,
        exit_long_marginal_price=exit_long.marginal_price,
        exit_short_vwap=exit_short.price,
        exit_short_marginal_price=exit_short.marginal_price,
        entry_spread=entry_spread,
        exit_spread=exit_spread,
        entry_spread_bps=(entry_spread / entry_long.price) * Decimal(10_000),
        four_leg_fee_estimate=four_leg_fee,
        funding_rate_delta=funding_delta,
    )


def _meets_notional(instrument: Instrument, price: Decimal, quantity: Decimal) -> bool:
    if not isinstance(instrument.no_fixed_minimum_notional, bool):
        return False
    if instrument.minimum_notional is None:
        return instrument.no_fixed_minimum_notional and instrument.venue == Venue.OKX
    return (
        isinstance(instrument.minimum_notional, Decimal)
        and not instrument.no_fixed_minimum_notional
        and instrument.minimum_notional.is_finite()
        and instrument.minimum_notional > 0
        and price * quantity >= instrument.minimum_notional
    )


def _rejected(
    long_instrument: Instrument,
    short_instrument: Instrument,
    quantity: Decimal,
    reason: ReasonCode,
) -> DirectedRouteQuote:
    return DirectedRouteQuote(
        key=long_instrument.key,
        long_venue=long_instrument.venue,
        short_venue=short_instrument.venue,
        base_quantity=quantity,
        eligible=False,
        reason=reason,
    )
