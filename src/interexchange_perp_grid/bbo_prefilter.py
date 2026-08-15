from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from decimal import Decimal

from interexchange_perp_grid.domain import BboQuote, Venue
from interexchange_perp_grid.market_universe import UniverseRoute
from interexchange_perp_grid.reason_codes import ReasonCode


@dataclass(frozen=True, slots=True)
class BboCacheStats:
    known_keys: int
    entries: int
    peak_entries: int
    accepted_updates: int
    coalesced_updates: int
    rejected_updates: int


@dataclass(frozen=True, slots=True)
class BboPrefilterObservation:
    base: str
    long_venue: Venue
    short_venue: Venue
    long_symbol: str
    short_symbol: str
    gross_entry_spread_bps: Decimal | None
    estimated_four_leg_fee_bps: Decimal | None
    estimated_edge_bps: Decimal | None
    observed_monotonic_ns: int
    reason: ReasonCode
    execution_authorized: bool = field(default=False, init=False)

    @property
    def stable_key(self) -> tuple[str, str, str]:
        return self.base, self.long_venue.value, self.short_venue.value


class LatestBboCache:
    """Bounded latest-value BBO cache; unknown keys can never allocate storage."""

    def __init__(self, *, maximum_age_ms: int, maximum_clock_skew_ms: int) -> None:
        if maximum_age_ms <= 0 or maximum_clock_skew_ms <= 0:
            raise ValueError("BBO cache quality limits must be positive")
        self._maximum_age_ns = maximum_age_ms * 1_000_000
        self._maximum_clock_skew_ms = maximum_clock_skew_ms
        self._known_keys: frozenset[tuple[Venue, str]] = frozenset()
        self._quotes: dict[tuple[Venue, str], BboQuote] = {}
        self._peak_entries = 0
        self._accepted_updates = 0
        self._coalesced_updates = 0
        self._rejected_updates = 0

    def set_known_keys(self, keys: frozenset[tuple[Venue, str]]) -> None:
        self._known_keys = keys
        self._quotes = {key: quote for key, quote in self._quotes.items() if key in keys}

    def ingest(self, quotes: Iterable[BboQuote], *, now_monotonic_ns: int) -> None:
        for quote in quotes:
            key = (quote.venue, quote.symbol)
            if key not in self._known_keys or not self._valid(quote, now_monotonic_ns):
                self._rejected_updates += 1
                continue
            previous = self._quotes.get(key)
            if previous is not None:
                if quote.received_monotonic_ns < previous.received_monotonic_ns:
                    self._rejected_updates += 1
                    continue
                self._coalesced_updates += 1
            self._quotes[key] = quote
            self._accepted_updates += 1
            self._peak_entries = max(self._peak_entries, len(self._quotes))

    def fresh(self, *, now_monotonic_ns: int) -> tuple[BboQuote, ...]:
        return tuple(
            quote
            for _, quote in sorted(
                self._quotes.items(),
                key=lambda item: (item[0][0].value, item[0][1]),
            )
            if self._valid(quote, now_monotonic_ns)
        )

    @property
    def stats(self) -> BboCacheStats:
        return BboCacheStats(
            len(self._known_keys),
            len(self._quotes),
            self._peak_entries,
            self._accepted_updates,
            self._coalesced_updates,
            self._rejected_updates,
        )

    def _valid(self, quote: BboQuote, now_monotonic_ns: int) -> bool:
        age_ns = now_monotonic_ns - quote.received_monotonic_ns
        return (
            0 <= age_ns <= self._maximum_age_ns
            and quote.bid_price > 0
            and quote.ask_price > 0
            and quote.bid_price < quote.ask_price
            and (quote.bid_base_quantity is None or quote.bid_base_quantity > 0)
            and (quote.ask_base_quantity is None or quote.ask_base_quantity > 0)
            and quote.clock_skew_ms is not None
            and abs(quote.clock_skew_ms) <= self._maximum_clock_skew_ms
        )


def rank_bbo_prefilter(
    routes: tuple[UniverseRoute, ...],
    quotes: tuple[BboQuote, ...],
) -> tuple[BboPrefilterObservation, ...]:
    by_key = {(quote.venue, quote.symbol): quote for quote in quotes}
    observations: list[BboPrefilterObservation] = []
    for route in routes:
        long = route.long_instrument
        short = route.short_instrument
        long_quote = by_key.get((long.venue, long.symbol))
        short_quote = by_key.get((short.venue, short.symbol))
        observed_ns = max(
            long_quote.received_monotonic_ns if long_quote is not None else 0,
            short_quote.received_monotonic_ns if short_quote is not None else 0,
        )
        if long_quote is None or short_quote is None:
            observations.append(
                BboPrefilterObservation(
                    long.base,
                    long.venue,
                    short.venue,
                    long.symbol,
                    short.symbol,
                    None,
                    None,
                    None,
                    observed_ns,
                    ReasonCode.BOOK_EMPTY,
                )
            )
            continue
        if long.taker_fee_rate is None or short.taker_fee_rate is None:
            observations.append(
                BboPrefilterObservation(
                    long.base,
                    long.venue,
                    short.venue,
                    long.symbol,
                    short.symbol,
                    None,
                    None,
                    None,
                    observed_ns,
                    ReasonCode.FEE_UNKNOWN,
                )
            )
            continue
        reference = (long_quote.ask_price + short_quote.bid_price) / Decimal(2)
        if reference <= 0:
            continue
        spread_bps = (short_quote.bid_price - long_quote.ask_price) / reference * Decimal(10_000)
        fee_bps = Decimal(2) * (long.taker_fee_rate + short.taker_fee_rate) * Decimal(10_000)
        observations.append(
            BboPrefilterObservation(
                long.base,
                long.venue,
                short.venue,
                long.symbol,
                short.symbol,
                spread_bps,
                fee_bps,
                spread_bps - fee_bps,
                observed_ns,
                ReasonCode.QUOTE_READY,
            )
        )
    return tuple(
        sorted(
            observations,
            key=lambda item: (
                item.estimated_edge_bps is None,
                -(item.estimated_edge_bps or Decimal(0)),
                *item.stable_key,
            ),
        )
    )
