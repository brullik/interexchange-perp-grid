from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from interexchange_perp_grid.domain import (
    NO_FIXED_MINIMUM_NOTIONAL_VENUES,
    CommonInstrument,
    Instrument,
    ProductType,
    Venue,
)
from interexchange_perp_grid.routes import directed_pairs, match_common_instruments


@dataclass(frozen=True, slots=True)
class UniverseRoute:
    long_instrument: Instrument
    short_instrument: Instrument

    @property
    def stable_key(self) -> tuple[str, str, str]:
        return (
            self.long_instrument.base,
            self.long_instrument.venue.value,
            self.short_instrument.venue.value,
        )


@dataclass(frozen=True, slots=True)
class UniverseSnapshot:
    generation: int
    refreshed_at: datetime
    refreshed_monotonic_ns: int
    common: tuple[CommonInstrument, ...]
    routes: tuple[UniverseRoute, ...]

    @property
    def known_bbo_keys(self) -> frozenset[tuple[Venue, str]]:
        return frozenset(
            (instrument.venue, instrument.symbol)
            for common in self.common
            for instrument in common.instruments
        )


class InstrumentRegistry:
    """Build one immutable, ambiguity-free latest universe snapshot."""

    def __init__(
        self,
        *,
        minimum_listing_age_days: int,
        enforce_listing_age: bool,
    ) -> None:
        if minimum_listing_age_days < 14:
            raise ValueError("live listing age must be at least 14 days")
        self._minimum_listing_age_seconds = Decimal(minimum_listing_age_days * 86400)
        self._enforce_listing_age = enforce_listing_age

    def build(
        self,
        instruments_by_venue: dict[Venue, tuple[Instrument, ...]],
        *,
        now: datetime,
        monotonic_ns: int,
        generation: int,
    ) -> UniverseSnapshot:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("universe clock must be timezone-aware")
        eligible: dict[Venue, tuple[Instrument, ...]] = {}
        for venue, instruments in instruments_by_venue.items():
            filtered = tuple(
                instrument for instrument in instruments if self._eligible(instrument, now)
            )
            if filtered:
                eligible[venue] = filtered
        common = match_common_instruments(eligible)
        routes = tuple(
            UniverseRoute(long_instrument, short_instrument)
            for item in common
            for long_instrument, short_instrument in directed_pairs(item)
        )
        return UniverseSnapshot(
            generation,
            now.astimezone(UTC),
            monotonic_ns,
            common,
            tuple(sorted(routes, key=lambda route: route.stable_key)),
        )

    def _eligible(self, instrument: Instrument, now: datetime) -> bool:
        required_positive = (
            instrument.contract_size_base,
            instrument.amount_step_contracts,
            instrument.price_tick,
            instrument.minimum_amount_contracts,
        )
        if (
            not instrument.active
            or instrument.product_type != ProductType.LINEAR_USDT_PERPETUAL
            or instrument.quote != "USDT"
            or instrument.settle != "USDT"
            or not instrument.base
            or instrument.base != instrument.base.upper()
            or not instrument.symbol
            or not instrument.exchange_symbol
            or not isinstance(instrument.no_fixed_minimum_notional, bool)
            or (instrument.minimum_notional is None and not instrument.no_fixed_minimum_notional)
            or (instrument.minimum_notional is not None and instrument.no_fixed_minimum_notional)
            or (
                instrument.no_fixed_minimum_notional
                and instrument.venue not in NO_FIXED_MINIMUM_NOTIONAL_VENUES
            )
            or (
                instrument.minimum_notional is not None
                and (
                    not isinstance(instrument.minimum_notional, Decimal)
                    or not instrument.minimum_notional.is_finite()
                    or instrument.minimum_notional <= 0
                )
            )
            or any(
                not isinstance(value, Decimal) or not value.is_finite() or value <= 0
                for value in required_positive
            )
        ):
            return False
        age = instrument.listing_age_seconds(now)
        if age is not None and age < 0:
            return False
        if not self._enforce_listing_age:
            return True
        return age is not None and age >= self._minimum_listing_age_seconds


class UniverseService:
    """Refresh a single latest universe on startup, six-hour expiry, or reconnect."""

    def __init__(
        self,
        registry: InstrumentRegistry,
        *,
        refresh_seconds: int,
    ) -> None:
        if refresh_seconds <= 0:
            raise ValueError("universe refresh interval must be positive")
        self._registry = registry
        self._refresh_ns = refresh_seconds * 1_000_000_000
        self._snapshot: UniverseSnapshot | None = None

    def refresh(
        self,
        instruments_by_venue: dict[Venue, tuple[Instrument, ...]],
        *,
        now: datetime,
        monotonic_ns: int,
        force: bool = False,
    ) -> UniverseSnapshot:
        current = self._snapshot
        if (
            current is not None
            and not force
            and monotonic_ns - current.refreshed_monotonic_ns < self._refresh_ns
        ):
            return current
        if current is not None and monotonic_ns < current.refreshed_monotonic_ns:
            raise ValueError("universe monotonic clock regressed")
        snapshot = self._registry.build(
            instruments_by_venue,
            now=now,
            monotonic_ns=monotonic_ns,
            generation=1 if current is None else current.generation + 1,
        )
        self._snapshot = snapshot
        return snapshot

    def refresh_due(self, monotonic_ns: int) -> bool:
        current = self._snapshot
        if current is None:
            return True
        if monotonic_ns < current.refreshed_monotonic_ns:
            raise ValueError("universe monotonic clock regressed")
        return monotonic_ns - current.refreshed_monotonic_ns >= self._refresh_ns

    @property
    def snapshot(self) -> UniverseSnapshot | None:
        return self._snapshot
