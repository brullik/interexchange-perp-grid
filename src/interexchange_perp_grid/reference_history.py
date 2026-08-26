from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from interexchange_perp_grid.domain import Instrument, InstrumentKey, Venue
from interexchange_perp_grid.spread_math import log_ratio_bps
from interexchange_perp_grid.strategy import DirectedRouteKey


class SourceBarQuality(StrEnum):
    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"
    DISCONTINUITY = "DISCONTINUITY"
    AMBIGUOUS_DUPLICATE = "AMBIGUOUS_DUPLICATE"


class ReferenceBarQuality(StrEnum):
    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"


class ReferenceRejectionReason(StrEnum):
    MISSING_SOURCE = "MISSING_SOURCE"
    SOURCE_INCOMPLETE = "SOURCE_INCOMPLETE"
    SOURCE_DISCONTINUITY = "SOURCE_DISCONTINUITY"
    SOURCE_NON_POSITIVE = "SOURCE_NON_POSITIVE"
    SOURCE_NON_FINITE = "SOURCE_NON_FINITE"
    SOURCE_OHLC_INVALID = "SOURCE_OHLC_INVALID"
    DUPLICATE_CONFLICT = "DUPLICATE_CONFLICT"
    UNSYNCHRONISED_MINUTE = "UNSYNCHRONISED_MINUTE"
    CONTRACT_MISMATCH = "CONTRACT_MISMATCH"
    VENUE_PAIR_INVALID = "VENUE_PAIR_INVALID"


@dataclass(frozen=True, slots=True)
class SourceMinuteBar:
    venue: Venue
    instrument: InstrumentKey
    symbol: str
    interval_start: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    contract_metadata_version: str
    quality: SourceBarQuality = SourceBarQuality.COMPLETE

    def __post_init__(self) -> None:
        _require_utc_minute(self.interval_start)
        if not self.symbol:
            raise ValueError("source bar symbol must not be empty")
        if not self.contract_metadata_version:
            raise ValueError("source bar contract metadata version must not be empty")


@dataclass(frozen=True, slots=True)
class ReferenceSpreadBar:
    venue_a: Venue
    venue_b: Venue
    instrument: InstrumentKey
    interval_start: datetime
    open_bps: Decimal
    high_bps: Decimal
    low_bps: Decimal
    close_bps: Decimal
    contract_metadata_version_a: str
    contract_metadata_version_b: str
    quality: ReferenceBarQuality = ReferenceBarQuality.COMPLETE
    synthetic_high_low_envelope: bool = True
    executable: bool = False

    def __post_init__(self) -> None:
        _require_utc_minute(self.interval_start)
        if (self.venue_a.value, self.venue_b.value) != tuple(
            sorted((self.venue_a.value, self.venue_b.value))
        ):
            raise ValueError("reference venue pair must use canonical lexical order")
        if self.venue_a == self.venue_b:
            raise ValueError("reference venue pair must contain two venues")
        if not self.synthetic_high_low_envelope or self.executable:
            raise ValueError("reference OHLC envelope is never executable")


@dataclass(frozen=True, slots=True)
class ReferenceMinuteRejection:
    interval_start: datetime
    reason: ReferenceRejectionReason


@dataclass(frozen=True, slots=True)
class ReferenceMinuteResult:
    bar: ReferenceSpreadBar | None
    rejection: ReferenceMinuteRejection | None

    def __post_init__(self) -> None:
        if (self.bar is None) == (self.rejection is None):
            raise ValueError("reference minute result must contain exactly one outcome")


@dataclass(frozen=True, slots=True)
class ReferenceSeriesResult:
    bars: tuple[ReferenceSpreadBar, ...]
    rejections: tuple[ReferenceMinuteRejection, ...]
    window_start: datetime | None = None
    window_end: datetime | None = None
    venue_a: Venue | None = None
    venue_b: Venue | None = None
    instrument: InstrumentKey | None = None
    symbol_a: str = ""
    symbol_b: str = ""
    source_a_sha256: str = ""
    source_b_sha256: str = ""
    dataset_sha256: str = ""

    def __post_init__(self) -> None:
        _validate_requested_window(self.window_start, self.window_end)
        if (self.window_start is None) != (self.window_end is None):
            raise ValueError("reference series window must include both bounds")
        if (self.venue_a is None) != (self.venue_b is None):
            raise ValueError("reference series identity must include both venues")
        if (
            self.venue_a is not None
            and self.venue_b is not None
            and canonical_venue_pair(self.venue_a, self.venue_b) != (self.venue_a, self.venue_b)
        ):
            raise ValueError("reference series identity is not canonical")
        expected = reference_series_sha256(self)
        if self.dataset_sha256 and self.dataset_sha256 != expected:
            raise ValueError("reference series dataset hash mismatch")
        object.__setattr__(self, "dataset_sha256", expected)


@dataclass(frozen=True, slots=True)
class DirectedReferenceRoutes:
    venue_a: Venue
    venue_b: Venue
    positive: DirectedRouteKey
    negative: DirectedRouteKey


@dataclass(frozen=True, slots=True)
class AggregatedReferenceBar:
    venue_a: Venue
    venue_b: Venue
    instrument: InstrumentKey
    interval_start: datetime
    timeframe_minutes: int
    quality: ReferenceBarQuality
    observed_minutes: int
    expected_minutes: int
    open_bps: Decimal | None
    high_bps: Decimal | None
    low_bps: Decimal | None
    close_bps: Decimal | None
    synthetic_high_low_envelope: bool = True
    executable: bool = False

    def __post_init__(self) -> None:
        _require_utc_minute(self.interval_start)
        if self.timeframe_minutes not in (5, 15, 60, 240, 1440):
            raise ValueError("unsupported reference aggregation timeframe")
        values = (self.open_bps, self.high_bps, self.low_bps, self.close_bps)
        if self.quality == ReferenceBarQuality.COMPLETE:
            if self.observed_minutes != self.expected_minutes or any(
                value is None for value in values
            ):
                raise ValueError("complete aggregate requires every constituent minute")
        elif any(value is not None for value in values):
            raise ValueError("incomplete aggregate must not expose OHLC values")
        if not self.synthetic_high_low_envelope or self.executable:
            raise ValueError("aggregated reference envelope is never executable")


def canonical_venue_pair(first: Venue, second: Venue) -> tuple[Venue, Venue]:
    if first == second:
        raise ValueError("canonical venue pair requires distinct venues")
    return tuple(sorted((first, second), key=lambda venue: venue.value))  # type: ignore[return-value]


def directed_routes_for_reference_pair(
    base: str,
    first: Venue,
    second: Venue,
) -> DirectedReferenceRoutes:
    venue_a, venue_b = canonical_venue_pair(first, second)
    normalized_base = base.strip().upper()
    if not normalized_base:
        raise ValueError("directed reference route base must not be empty")
    return DirectedReferenceRoutes(
        venue_a=venue_a,
        venue_b=venue_b,
        positive=DirectedRouteKey(
            base=normalized_base,
            long_venue=venue_b,
            short_venue=venue_a,
        ),
        negative=DirectedRouteKey(
            base=normalized_base,
            long_venue=venue_a,
            short_venue=venue_b,
        ),
    )


def build_reference_minute(
    first: SourceMinuteBar, second: SourceMinuteBar
) -> ReferenceMinuteResult:
    interval_start = min(first.interval_start, second.interval_start)
    try:
        venue_a, venue_b = canonical_venue_pair(first.venue, second.venue)
    except ValueError:
        return _rejected(interval_start, ReferenceRejectionReason.VENUE_PAIR_INVALID)
    source_a, source_b = (first, second) if first.venue == venue_a else (second, first)
    if source_a.interval_start != source_b.interval_start:
        return _rejected(interval_start, ReferenceRejectionReason.UNSYNCHRONISED_MINUTE)
    if source_a.instrument != source_b.instrument:
        return _rejected(interval_start, ReferenceRejectionReason.CONTRACT_MISMATCH)
    for source in (source_a, source_b):
        reason = _source_rejection_reason(source)
        if reason is not None:
            return _rejected(interval_start, reason)
    return ReferenceMinuteResult(
        bar=ReferenceSpreadBar(
            venue_a=venue_a,
            venue_b=venue_b,
            instrument=source_a.instrument,
            interval_start=source_a.interval_start,
            open_bps=_spread_bps(source_a.open, source_b.open),
            high_bps=_spread_bps(source_a.high, source_b.low),
            low_bps=_spread_bps(source_a.low, source_b.high),
            close_bps=_spread_bps(source_a.close, source_b.close),
            contract_metadata_version_a=source_a.contract_metadata_version,
            contract_metadata_version_b=source_b.contract_metadata_version,
        ),
        rejection=None,
    )


def build_reference_series(
    first: tuple[SourceMinuteBar, ...],
    second: tuple[SourceMinuteBar, ...],
    *,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
) -> ReferenceSeriesResult:
    _validate_requested_window(window_start, window_end)
    identity_sources = (*first, *second)
    if window_start is not None and window_end is not None:
        first = tuple(bar for bar in first if window_start <= bar.interval_start < window_end)
        second = tuple(bar for bar in second if window_start <= bar.interval_start < window_end)
    first_by_minute, first_conflicts = _deduplicate(first)
    second_by_minute, second_conflicts = _deduplicate(second)
    conflict_minutes = first_conflicts | second_conflicts
    if window_start is not None and window_end is not None:
        all_minutes = []
        minute = window_start
        while minute < window_end:
            all_minutes.append(minute)
            minute += timedelta(minutes=1)
    else:
        all_minutes = sorted(set(first_by_minute) | set(second_by_minute) | conflict_minutes)
    first_version = _first_version(first_by_minute)
    second_version = _first_version(second_by_minute)
    bars: list[ReferenceSpreadBar] = []
    rejections: list[ReferenceMinuteRejection] = []
    for minute in all_minutes:
        if minute in conflict_minutes:
            rejections.append(
                ReferenceMinuteRejection(minute, ReferenceRejectionReason.DUPLICATE_CONFLICT)
            )
            continue
        left = first_by_minute.get(minute)
        right = second_by_minute.get(minute)
        if left is None or right is None:
            rejections.append(
                ReferenceMinuteRejection(minute, ReferenceRejectionReason.MISSING_SOURCE)
            )
            continue
        if (
            left.contract_metadata_version != first_version
            or right.contract_metadata_version != second_version
        ):
            rejections.append(
                ReferenceMinuteRejection(minute, ReferenceRejectionReason.CONTRACT_MISMATCH)
            )
            continue
        result = build_reference_minute(left, right)
        if result.bar is not None:
            bars.append(result.bar)
        else:
            assert result.rejection is not None
            rejections.append(result.rejection)
    identity_venues = tuple(
        sorted({bar.venue for bar in identity_sources}, key=lambda item: item.value)
    )
    identity_instruments = {bar.instrument for bar in identity_sources}
    venue_a = identity_venues[0] if len(identity_venues) == 2 else None
    venue_b = identity_venues[1] if len(identity_venues) == 2 else None
    instrument = next(iter(identity_instruments)) if len(identity_instruments) == 1 else None
    canonical_a = (
        tuple(bar for bar in (*first, *second) if bar.venue == venue_a)
        if venue_a is not None
        else ()
    )
    canonical_b = (
        tuple(bar for bar in (*first, *second) if bar.venue == venue_b)
        if venue_b is not None
        else ()
    )
    symbols_a = {bar.symbol for bar in canonical_a}
    symbols_b = {bar.symbol for bar in canonical_b}
    return ReferenceSeriesResult(
        tuple(bars),
        tuple(rejections),
        window_start,
        window_end,
        venue_a,
        venue_b,
        instrument,
        next(iter(symbols_a)) if len(symbols_a) == 1 else "",
        next(iter(symbols_b)) if len(symbols_b) == 1 else "",
        source_bars_sha256(canonical_a),
        source_bars_sha256(canonical_b),
    )


def aggregate_reference_bars(
    bars: tuple[ReferenceSpreadBar, ...],
    timeframe_minutes: int,
    *,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
    venue_a: Venue | None = None,
    venue_b: Venue | None = None,
    instrument: InstrumentKey | None = None,
) -> tuple[AggregatedReferenceBar, ...]:
    if timeframe_minutes not in (5, 15, 60, 240, 1440):
        raise ValueError("unsupported reference aggregation timeframe")
    if any(not isinstance(bar, ReferenceSpreadBar) for bar in bars):
        raise TypeError("reference aggregates accept only canonical one-minute spread bars")
    _validate_requested_window(window_start, window_end)
    if not bars and (
        window_start is None
        or window_end is None
        or venue_a is None
        or venue_b is None
        or instrument is None
    ):
        return ()
    ordered = tuple(sorted(bars, key=lambda bar: bar.interval_start))
    groups: dict[datetime, list[ReferenceSpreadBar]] = {}
    for bar in ordered:
        if (
            window_start is not None
            and window_end is not None
            and not (window_start <= bar.interval_start < window_end)
        ):
            continue
        groups.setdefault(_interval_floor(bar.interval_start, timeframe_minutes), []).append(bar)
    if window_start is not None and window_end is not None:
        interval_start = _interval_floor(window_start, timeframe_minutes)
        while interval_start < window_end:
            groups.setdefault(interval_start, [])
            interval_start += timedelta(minutes=timeframe_minutes)
    aggregates: list[AggregatedReferenceBar] = []
    for interval_start, group in sorted(groups.items()):
        first = group[0] if group else ordered[0] if ordered else None
        identity_a = first.venue_a if first is not None else venue_a
        identity_b = first.venue_b if first is not None else venue_b
        identity_instrument = first.instrument if first is not None else instrument
        assert identity_a is not None and identity_b is not None and identity_instrument is not None
        identity_matches = all(
            (bar.venue_a, bar.venue_b, bar.instrument)
            == (identity_a, identity_b, identity_instrument)
            for bar in group
        )
        expected_starts = {
            interval_start + timedelta(minutes=offset) for offset in range(timeframe_minutes)
        }
        observed_starts = {bar.interval_start for bar in group}
        complete = (
            identity_matches
            and len(group) == timeframe_minutes
            and observed_starts == expected_starts
            and all(bar.quality == ReferenceBarQuality.COMPLETE for bar in group)
        )
        if complete:
            aggregates.append(
                AggregatedReferenceBar(
                    venue_a=identity_a,
                    venue_b=identity_b,
                    instrument=identity_instrument,
                    interval_start=interval_start,
                    timeframe_minutes=timeframe_minutes,
                    quality=ReferenceBarQuality.COMPLETE,
                    observed_minutes=len(group),
                    expected_minutes=timeframe_minutes,
                    open_bps=group[0].open_bps,
                    high_bps=max(bar.high_bps for bar in group),
                    low_bps=min(bar.low_bps for bar in group),
                    close_bps=group[-1].close_bps,
                )
            )
        else:
            aggregates.append(
                AggregatedReferenceBar(
                    venue_a=identity_a,
                    venue_b=identity_b,
                    instrument=identity_instrument,
                    interval_start=interval_start,
                    timeframe_minutes=timeframe_minutes,
                    quality=ReferenceBarQuality.INCOMPLETE,
                    observed_minutes=len(observed_starts),
                    expected_minutes=timeframe_minutes,
                    open_bps=None,
                    high_bps=None,
                    low_bps=None,
                    close_bps=None,
                )
            )
    return tuple(aggregates)


def _validate_requested_window(
    window_start: datetime | None,
    window_end: datetime | None,
) -> None:
    if (window_start is None) != (window_end is None):
        raise ValueError("reference history window requires both start and end")
    if window_start is None or window_end is None:
        return
    _require_utc_minute(window_start)
    _require_utc_minute(window_end)
    if window_end <= window_start:
        raise ValueError("reference history window end must follow start")


def reference_bars_sha256(bars: tuple[ReferenceSpreadBar, ...]) -> str:
    rows = [
        {
            "venue_a": bar.venue_a.value,
            "venue_b": bar.venue_b.value,
            "instrument": {
                "base": bar.instrument.base,
                "quote": bar.instrument.quote,
                "settle": bar.instrument.settle,
                "product_type": bar.instrument.product_type.value,
            },
            "interval_start": bar.interval_start.isoformat(),
            "open_bps": str(bar.open_bps),
            "high_bps": str(bar.high_bps),
            "low_bps": str(bar.low_bps),
            "close_bps": str(bar.close_bps),
            "contract_metadata_version_a": bar.contract_metadata_version_a,
            "contract_metadata_version_b": bar.contract_metadata_version_b,
            "quality": bar.quality.value,
            "synthetic_high_low_envelope": bar.synthetic_high_low_envelope,
            "executable": bar.executable,
        }
        for bar in sorted(bars, key=lambda item: item.interval_start)
    ]
    payload = json.dumps(rows, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def reference_series_sha256(series: ReferenceSeriesResult) -> str:
    instrument = series.instrument
    outcomes: list[dict[str, object]] = [
        {
            "interval_start": bar.interval_start.isoformat(),
            "kind": "BAR",
            "bar_sha256": reference_bars_sha256((bar,)),
        }
        for bar in series.bars
    ]
    outcomes.extend(
        {
            "interval_start": rejection.interval_start.isoformat(),
            "kind": "REJECTION",
            "reason": rejection.reason.value,
        }
        for rejection in series.rejections
    )
    outcomes.sort(key=lambda item: str(item["interval_start"]))
    payload = {
        "window_start": series.window_start.isoformat() if series.window_start else None,
        "window_end": series.window_end.isoformat() if series.window_end else None,
        "venue_a": series.venue_a.value if series.venue_a else None,
        "venue_b": series.venue_b.value if series.venue_b else None,
        "instrument": (
            {
                "base": instrument.base,
                "quote": instrument.quote,
                "settle": instrument.settle,
                "product_type": instrument.product_type.value,
            }
            if instrument is not None
            else None
        ),
        "symbol_a": series.symbol_a,
        "symbol_b": series.symbol_b,
        "source_a_sha256": series.source_a_sha256,
        "source_b_sha256": series.source_b_sha256,
        "outcomes": outcomes,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def source_bars_sha256(bars: tuple[SourceMinuteBar, ...]) -> str:
    rows = [
        {
            "venue": bar.venue.value,
            "symbol": bar.symbol,
            "instrument": {
                "base": bar.instrument.base,
                "quote": bar.instrument.quote,
                "settle": bar.instrument.settle,
                "product_type": bar.instrument.product_type.value,
            },
            "interval_start": bar.interval_start.isoformat(),
            "open": str(bar.open),
            "high": str(bar.high),
            "low": str(bar.low),
            "close": str(bar.close),
            "contract_metadata_version": bar.contract_metadata_version,
            "quality": bar.quality.value,
        }
        for bar in sorted(bars, key=lambda item: (item.venue.value, item.interval_start))
    ]
    payload = json.dumps(rows, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def contract_metadata_version(instrument: Instrument) -> str:
    payload = {
        "venue": instrument.venue.value,
        "symbol": instrument.symbol,
        "exchange_symbol": instrument.exchange_symbol,
        "base": instrument.base,
        "quote": instrument.quote,
        "settle": instrument.settle,
        "product_type": instrument.product_type.value,
        "contract_size_base": str(instrument.contract_size_base),
        "amount_step_contracts": str(instrument.amount_step_contracts),
        "price_tick": str(instrument.price_tick),
        "minimum_amount_contracts": str(instrument.minimum_amount_contracts),
        "active": instrument.active,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _source_rejection_reason(source: SourceMinuteBar) -> ReferenceRejectionReason | None:
    if source.quality == SourceBarQuality.INCOMPLETE:
        return ReferenceRejectionReason.SOURCE_INCOMPLETE
    if source.quality == SourceBarQuality.DISCONTINUITY:
        return ReferenceRejectionReason.SOURCE_DISCONTINUITY
    if source.quality == SourceBarQuality.AMBIGUOUS_DUPLICATE:
        return ReferenceRejectionReason.DUPLICATE_CONFLICT
    values = (source.open, source.high, source.low, source.close)
    if any(not value.is_finite() for value in values):
        return ReferenceRejectionReason.SOURCE_NON_FINITE
    if any(value <= 0 for value in values):
        return ReferenceRejectionReason.SOURCE_NON_POSITIVE
    if source.high < max(source.open, source.close) or source.low > min(source.open, source.close):
        return ReferenceRejectionReason.SOURCE_OHLC_INVALID
    if source.high < source.low:
        return ReferenceRejectionReason.SOURCE_OHLC_INVALID
    return None


def _spread_bps(numerator: Decimal, denominator: Decimal) -> Decimal:
    return log_ratio_bps(numerator, denominator)


def _require_utc_minute(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("minute timestamp must be timezone-aware")
    utc_value = value.astimezone(UTC)
    if value != utc_value or utc_value.second != 0 or utc_value.microsecond != 0:
        raise ValueError("minute timestamp must be an exact UTC minute")


def _rejected(interval_start: datetime, reason: ReferenceRejectionReason) -> ReferenceMinuteResult:
    return ReferenceMinuteResult(
        bar=None,
        rejection=ReferenceMinuteRejection(interval_start=interval_start, reason=reason),
    )


def _deduplicate(
    bars: tuple[SourceMinuteBar, ...],
) -> tuple[dict[datetime, SourceMinuteBar], set[datetime]]:
    result: dict[datetime, SourceMinuteBar] = {}
    conflicts: set[datetime] = set()
    for bar in bars:
        existing = result.get(bar.interval_start)
        if existing is None:
            result[bar.interval_start] = bar
        elif existing != bar:
            conflicts.add(bar.interval_start)
            result.pop(bar.interval_start, None)
    return result, conflicts


def _interval_floor(value: datetime, timeframe_minutes: int) -> datetime:
    utc_value = value.astimezone(UTC)
    if timeframe_minutes == 1440:
        return utc_value.replace(hour=0, minute=0)
    minute_of_day = utc_value.hour * 60 + utc_value.minute
    floored = minute_of_day - (minute_of_day % timeframe_minutes)
    return utc_value.replace(hour=floored // 60, minute=floored % 60)


def _first_version(bars: dict[datetime, SourceMinuteBar]) -> str | None:
    return bars[min(bars)].contract_metadata_version if bars else None
