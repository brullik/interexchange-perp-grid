from __future__ import annotations

import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast

import pytest

from interexchange_perp_grid.bbo_prefilter import (
    BboPrefilterObservation,
    LatestBboCache,
    rank_bbo_prefilter,
)
from interexchange_perp_grid.domain import WAVE1_VENUES, BboQuote, Instrument, ProductType, Venue
from interexchange_perp_grid.market_universe import (
    InstrumentRegistry,
    UniverseRoute,
    UniverseService,
)
from interexchange_perp_grid.reason_codes import ReasonCode

NOW = datetime(2026, 8, 15, tzinfo=UTC)


def _instrument(
    venue: Venue,
    base: str,
    *,
    listed_days_ago: int | None = 30,
    active: bool = True,
) -> Instrument:
    return Instrument(
        venue,
        f"{base}/USDT:USDT",
        f"{venue.value}-{base}",
        base,
        "USDT",
        "USDT",
        Decimal("1"),
        Decimal("0.001"),
        Decimal("0.1"),
        Decimal("0.001"),
        Decimal("5"),
        Decimal("0.0005"),
        "fixture",
        active,
        NOW - timedelta(days=listed_days_ago) if listed_days_ago is not None else None,
    )


@pytest.fixture
def large_universe() -> dict[Venue, tuple[Instrument, ...]]:
    by_venue: dict[Venue, list[Instrument]] = {venue: [] for venue in WAVE1_VENUES}
    for index in range(100):
        base = f"A{index:03d}"
        for venue in WAVE1_VENUES:
            by_venue[venue].append(_instrument(venue, base))
    for venue in WAVE1_VENUES:
        by_venue[venue].extend(
            (
                _instrument(venue, "EXACT14", listed_days_ago=14),
                _instrument(venue, "YOUNG13", listed_days_ago=13),
                _instrument(venue, "UNKNOWN", listed_days_ago=None),
                _instrument(venue, "INACTIVE", active=False),
                _instrument(venue, "AMBIG"),
            )
        )
    by_venue[Venue.BYBIT].append(
        replace(_instrument(Venue.BYBIT, "AMBIG"), exchange_symbol="duplicate")
    )
    return {venue: tuple(instruments) for venue, instruments in by_venue.items()}


def _quote(instrument: Instrument, received_ns: int, *, offset: int = 0) -> BboQuote:
    mid = Decimal(100 + offset)
    return BboQuote(
        instrument.venue,
        instrument.symbol,
        mid - Decimal("0.5"),
        Decimal(1),
        mid + Decimal("0.5"),
        Decimal(1),
        1_700_000_000_000,
        NOW,
        received_ns,
        0,
    )


def test_large_live_universe_filters_age_activity_and_ambiguity(
    large_universe: dict[Venue, tuple[Instrument, ...]],
) -> None:
    registry = InstrumentRegistry(minimum_listing_age_days=14, enforce_listing_age=True)

    snapshot = registry.build(
        large_universe,
        now=NOW,
        monotonic_ns=1,
        generation=1,
    )

    assert len(snapshot.common) == 102
    assert len(snapshot.routes) == 608
    assert len(snapshot.known_bbo_keys) == 305
    assert {item.key.base for item in snapshot.common} == {
        *(f"A{index:03d}" for index in range(100)),
        "AMBIG",
        "EXACT14",
    }
    assert all(
        route.long_instrument.venue != route.short_instrument.venue for route in snapshot.routes
    )
    ambiguous = next(item for item in snapshot.common if item.key.base == "AMBIG")
    assert {instrument.venue for instrument in ambiguous.instruments} == {
        Venue.BINANCE_USDM,
        Venue.OKX,
    }


def test_registry_rejects_cross_quote_and_incomplete_contract_metadata() -> None:
    registry = InstrumentRegistry(minimum_listing_age_days=14, enforce_listing_age=True)
    valid = _instrument(Venue.BYBIT, "BTC")
    malformed = (
        replace(_instrument(Venue.OKX, "BTC"), quote="USDC"),
        replace(_instrument(Venue.OKX, "ETH"), minimum_notional=None),
        replace(_instrument(Venue.OKX, "SOL"), price_tick=Decimal("-0.1")),
        replace(_instrument(Venue.OKX, "XRP"), amount_step_contracts=Decimal(0)),
        replace(
            _instrument(Venue.OKX, "DOT"),
            minimum_notional=None,
            no_fixed_minimum_notional=cast(bool, "yes"),
        ),
        replace(
            _instrument(Venue.OKX, "DOGE"),
            product_type=cast(ProductType, "inverse_perpetual"),
        ),
    )

    snapshot = registry.build(
        {
            Venue.BYBIT: (
                valid,
                _instrument(Venue.BYBIT, "ETH"),
                _instrument(Venue.BYBIT, "SOL"),
                _instrument(Venue.BYBIT, "XRP"),
                _instrument(Venue.BYBIT, "DOGE"),
                replace(
                    _instrument(Venue.BYBIT, "BNB"),
                    minimum_notional=None,
                    no_fixed_minimum_notional=True,
                ),
                replace(
                    _instrument(Venue.BYBIT, "ADA"),
                    minimum_notional=cast(Decimal, 5.0),
                ),
                _instrument(Venue.BYBIT, "DOT"),
            ),
            Venue.OKX: (
                *malformed,
                _instrument(Venue.OKX, "BNB"),
                _instrument(Venue.OKX, "ADA"),
            ),
        },
        now=NOW,
        monotonic_ns=1,
        generation=1,
    )

    assert snapshot.common == ()
    assert valid.key.quote == valid.quote == "USDT"


def test_universe_refreshes_only_on_six_hour_expiry_or_reconnect(
    large_universe: dict[Venue, tuple[Instrument, ...]],
) -> None:
    service = UniverseService(
        InstrumentRegistry(minimum_listing_age_days=14, enforce_listing_age=True),
        refresh_seconds=21600,
    )
    first = service.refresh(large_universe, now=NOW, monotonic_ns=0)
    changed = {
        **large_universe,
        Venue.BYBIT: (*large_universe[Venue.BYBIT], _instrument(Venue.BYBIT, "NEW")),
    }

    before_expiry = service.refresh(
        changed,
        now=NOW + timedelta(hours=5),
        monotonic_ns=21_599_999_999_999,
    )
    expired = service.refresh(
        changed,
        now=NOW + timedelta(hours=6),
        monotonic_ns=21_600_000_000_000,
    )
    reconnect = service.refresh(
        large_universe,
        now=NOW + timedelta(hours=6, seconds=1),
        monotonic_ns=21_601_000_000_000,
        force=True,
    )

    assert before_expiry is first
    assert expired.generation == 2
    assert reconnect.generation == 3


def test_100k_bbo_burst_is_coalesced_with_fixed_memory(
    large_universe: dict[Venue, tuple[Instrument, ...]],
) -> None:
    snapshot = InstrumentRegistry(
        minimum_listing_age_days=14,
        enforce_listing_age=True,
    ).build(large_universe, now=NOW, monotonic_ns=1_000_000, generation=1)
    instruments = tuple(
        instrument for common in snapshot.common for instrument in common.instruments
    )
    cache = LatestBboCache(maximum_age_ms=1500, maximum_clock_skew_ms=1000)
    cache.set_known_keys(snapshot.known_bbo_keys)
    now_ns = 1_000_000

    cache.ingest(
        (
            _quote(instruments[index % len(instruments)], now_ns, offset=index % 3)
            for index in range(100_000)
        ),
        now_monotonic_ns=now_ns,
    )
    cache.ingest(
        (_quote(_instrument(Venue.BYBIT, "NOT_IN_UNIVERSE"), now_ns),),
        now_monotonic_ns=now_ns,
    )

    stats = cache.stats
    assert stats.known_keys == len(instruments)
    assert stats.entries == len(instruments)
    assert stats.peak_entries == len(instruments)
    assert stats.accepted_updates == 100_000
    assert stats.coalesced_updates == 100_000 - len(instruments)
    assert stats.rejected_updates == 1


def test_bbo_cache_rejects_stale_future_crossed_and_unknown_clock_quotes() -> None:
    instrument = _instrument(Venue.BYBIT, "BTC")
    cache = LatestBboCache(maximum_age_ms=1500, maximum_clock_skew_ms=1000)
    cache.set_known_keys(frozenset({(instrument.venue, instrument.symbol)}))
    now_ns = 2_000_000_000
    valid = _quote(instrument, now_ns)

    cache.ingest(
        (
            replace(valid, received_monotonic_ns=now_ns - 1_500_000_001),
            replace(valid, received_monotonic_ns=now_ns + 1),
            replace(valid, bid_price=Decimal(102), ask_price=Decimal(101)),
            replace(valid, bid_price=Decimal(101), ask_price=Decimal(101)),
            replace(valid, clock_skew_ms=None),
        ),
        now_monotonic_ns=now_ns,
    )

    assert cache.stats.entries == 0
    assert cache.stats.rejected_updates == 5
    cache.ingest((valid,), now_monotonic_ns=now_ns)
    assert cache.fresh(now_monotonic_ns=now_ns) == (valid,)
    assert cache.fresh(now_monotonic_ns=now_ns + 1_500_000_001) == ()


def test_bbo_prefilter_is_stable_non_executable_and_p95_under_100ms(
    large_universe: dict[Venue, tuple[Instrument, ...]],
) -> None:
    snapshot = InstrumentRegistry(
        minimum_listing_age_days=14,
        enforce_listing_age=True,
    ).build(large_universe, now=NOW, monotonic_ns=1_000_000, generation=1)
    quotes = tuple(
        _quote(instrument, 1_000_000, offset=list(Venue).index(instrument.venue))
        for common in snapshot.common
        for instrument in common.instruments
    )
    samples_ms: list[float] = []
    ranked: tuple[BboPrefilterObservation, ...] = ()
    for _ in range(40):
        started = time.perf_counter_ns()
        ranked = rank_bbo_prefilter(snapshot.routes, quotes)
        samples_ms.append((time.perf_counter_ns() - started) / 1_000_000)
    p95 = sorted(samples_ms)[37]

    assert len(ranked) == len(snapshot.routes)
    assert all(observation.execution_authorized is False for observation in ranked)
    assert tuple(item.stable_key for item in ranked) == tuple(
        item.stable_key
        for item in sorted(
            ranked,
            key=lambda item: (
                item.estimated_edge_bps is None,
                -(item.estimated_edge_bps or Decimal(0)),
                *item.stable_key,
            ),
        )
    )
    assert p95 <= 100


def test_prefilter_reports_missing_fee_instead_of_omitting_route() -> None:
    long = replace(_instrument(Venue.BYBIT, "BTC"), taker_fee_rate=None)
    short = _instrument(Venue.OKX, "BTC")
    route = UniverseRoute(long, short)

    observations = rank_bbo_prefilter(
        (route,),
        (_quote(long, 1), _quote(short, 1, offset=1)),
    )

    assert len(observations) == 1
    assert observations[0].reason == ReasonCode.FEE_UNKNOWN
    assert observations[0].estimated_edge_bps is None
    assert observations[0].execution_authorized is False


def test_prefilter_distinguishes_stale_from_never_observed_bbo() -> None:
    long = _instrument(Venue.BYBIT, "BTC")
    short = _instrument(Venue.OKX, "BTC")
    cache = LatestBboCache(maximum_age_ms=1500, maximum_clock_skew_ms=1000)
    cache.set_known_keys(frozenset({(long.venue, long.symbol), (short.venue, short.symbol)}))
    cache.ingest((_quote(long, 1),), now_monotonic_ns=1)
    now_ns = 1_500_000_002

    observations = rank_bbo_prefilter(
        (UniverseRoute(long, short),),
        cache.fresh(now_monotonic_ns=now_ns),
        stale_keys=cache.stale_keys(now_monotonic_ns=now_ns),
    )
    stats = cache.stats_at(now_monotonic_ns=now_ns)

    assert observations[0].reason == ReasonCode.BOOK_STALE
    assert stats.entries == stats.stale_entries == 1
    assert stats.fresh_entries == 0
