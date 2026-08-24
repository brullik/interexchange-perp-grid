from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from interexchange_perp_grid.domain import InstrumentKey, Venue
from interexchange_perp_grid.reference_history import (
    ReferenceBarQuality,
    ReferenceRejectionReason,
    SourceBarQuality,
    SourceMinuteBar,
    aggregate_reference_bars,
    build_reference_minute,
    build_reference_series,
    canonical_venue_pair,
    reference_bars_sha256,
)

START = datetime(2026, 1, 1, tzinfo=UTC)
KEY = InstrumentKey(base="BTC", settle="USDT")


def _source(
    venue: Venue,
    *,
    minute: int = 0,
    open_: str = "100",
    high: str = "120",
    low: str = "80",
    close: str = "110",
    quality: SourceBarQuality = SourceBarQuality.COMPLETE,
) -> SourceMinuteBar:
    return SourceMinuteBar(
        venue=venue,
        instrument=KEY,
        symbol="BTC/USDT:USDT",
        interval_start=START + timedelta(minutes=minute),
        open=Decimal(open_),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        contract_metadata_version=f"{venue.value}-v1",
        quality=quality,
    )


def test_reference_minute_uses_canonical_pair_and_exact_synthetic_formulas() -> None:
    bybit = _source(Venue.BYBIT)
    okx = _source(Venue.OKX, open_="100", high="100", low="100", close="100")

    forward = build_reference_minute(bybit, okx)
    reverse = build_reference_minute(okx, bybit)

    assert forward == reverse
    assert forward.rejection is None
    assert forward.bar is not None
    assert (forward.bar.venue_a, forward.bar.venue_b) == (Venue.BYBIT, Venue.OKX)
    assert forward.bar.open_bps == Decimal("0E-8")
    assert forward.bar.high_bps == Decimal("1823.21556794")
    assert forward.bar.low_bps == Decimal("-2231.43551314")
    assert forward.bar.close_bps == Decimal("953.10179804")
    assert forward.bar.synthetic_high_low_envelope is True
    assert forward.bar.executable is False


def test_reference_series_rejects_missing_conflicting_and_incomplete_minutes() -> None:
    bybit_zero = _source(Venue.BYBIT)
    bybit_conflict = replace(bybit_zero, close=Decimal("109"))
    bybit_incomplete = _source(Venue.BYBIT, minute=1, quality=SourceBarQuality.INCOMPLETE)
    okx_zero = _source(Venue.OKX)
    okx_one = _source(Venue.OKX, minute=1)
    okx_two = _source(Venue.OKX, minute=2)

    result = build_reference_series(
        (bybit_zero, bybit_conflict, bybit_incomplete),
        (okx_zero, okx_one, okx_two),
    )

    assert result.bars == ()
    assert tuple(item.reason for item in result.rejections) == (
        ReferenceRejectionReason.DUPLICATE_CONFLICT,
        ReferenceRejectionReason.SOURCE_INCOMPLETE,
        ReferenceRejectionReason.MISSING_SOURCE,
    )


def test_identical_duplicate_is_idempotent_and_hash_is_order_independent() -> None:
    bybit = tuple(_source(Venue.BYBIT, minute=minute) for minute in range(5))
    okx = tuple(_source(Venue.OKX, minute=minute) for minute in range(5))

    first = build_reference_series((bybit[0], *bybit), okx)
    second = build_reference_series(tuple(reversed(bybit)), tuple(reversed(okx)))

    assert first.rejections == ()
    assert second.rejections == ()
    assert first.bars == second.bars
    assert reference_bars_sha256(first.bars) == reference_bars_sha256(second.bars)


def test_aggregation_uses_only_complete_consecutive_one_minute_reference_bars() -> None:
    source = build_reference_series(
        tuple(_source(Venue.BYBIT, minute=minute) for minute in range(5)),
        tuple(_source(Venue.OKX, minute=minute) for minute in range(5)),
    )

    complete = aggregate_reference_bars(source.bars, 5)
    incomplete = aggregate_reference_bars(source.bars[:-1], 5)

    assert len(complete) == 1
    assert complete[0].quality == ReferenceBarQuality.COMPLETE
    assert complete[0].open_bps == source.bars[0].open_bps
    assert complete[0].high_bps == max(bar.high_bps for bar in source.bars)
    assert complete[0].low_bps == min(bar.low_bps for bar in source.bars)
    assert complete[0].close_bps == source.bars[-1].close_bps
    assert complete[0].executable is False
    assert incomplete[0].quality == ReferenceBarQuality.INCOMPLETE
    assert incomplete[0].observed_minutes == 4
    assert incomplete[0].open_bps is None
    assert incomplete[0].close_bps is None


def test_reference_minute_rejects_contract_sync_price_and_quality_faults() -> None:
    valid = _source(Venue.BYBIT)
    other = _source(Venue.OKX)
    cases = (
        (
            replace(other, interval_start=START + timedelta(minutes=1)),
            ReferenceRejectionReason.UNSYNCHRONISED_MINUTE,
        ),
        (
            replace(other, instrument=InstrumentKey(base="ETH", settle="USDT")),
            ReferenceRejectionReason.CONTRACT_MISMATCH,
        ),
        (replace(other, close=Decimal("NaN")), ReferenceRejectionReason.SOURCE_NON_FINITE),
        (replace(other, close=Decimal("0")), ReferenceRejectionReason.SOURCE_NON_POSITIVE),
        (replace(other, high=Decimal("90")), ReferenceRejectionReason.SOURCE_OHLC_INVALID),
        (
            replace(other, quality=SourceBarQuality.DISCONTINUITY),
            ReferenceRejectionReason.SOURCE_DISCONTINUITY,
        ),
    )
    for broken, expected in cases:
        result = build_reference_minute(valid, broken)
        assert result.bar is None
        assert result.rejection is not None
        assert result.rejection.reason == expected


def test_canonical_pair_and_source_timestamp_fail_closed() -> None:
    with pytest.raises(ValueError, match="distinct venues"):
        canonical_venue_pair(Venue.OKX, Venue.OKX)
    with pytest.raises(ValueError, match="exact UTC minute"):
        replace(_source(Venue.OKX), interval_start=START + timedelta(seconds=1))
