from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.adapters.base import ExchangeAdapter
from interexchange_perp_grid.config import load_settings
from interexchange_perp_grid.domain import (
    BboQuote,
    BookLevel,
    CapabilityReport,
    FundingSnapshot,
    Instrument,
    OrderBookSnapshot,
    Venue,
)
from interexchange_perp_grid.history import ParquetMarketRecorder, query_recorded_level_count
from interexchange_perp_grid.public_engine import PublicMarketEngine

CONFIG = Path("config/defaults.yaml")


def make_instrument(venue: Venue) -> Instrument:
    return Instrument(
        venue=venue,
        symbol="BTC/USDT:USDT",
        exchange_symbol=f"{venue.value}-BTC",
        base="BTC",
        quote="USDT",
        settle="USDT",
        contract_size_base=Decimal("1"),
        amount_step_contracts=Decimal("0.001"),
        price_tick=Decimal("0.1"),
        minimum_amount_contracts=Decimal("0.001"),
        minimum_notional=None,
        taker_fee_rate=Decimal("0.0005"),
        fee_source="fixture",
    )


class FakeAdapter(ExchangeAdapter):
    def __init__(self, venue: Venue, *, fail_probe: bool = False) -> None:
        self.venue = venue
        self.fail_probe = fail_probe
        self.instrument = make_instrument(venue)
        self.closed = False
        self.book_calls = 0

    async def probe_public_capabilities(self) -> CapabilityReport:
        if self.fail_probe:
            raise ConnectionError("fixture outage")
        return CapabilityReport(
            venue=self.venue,
            bbo_stream=True,
            l2_stream=True,
            funding=True,
            mark_index=True,
            server_time=True,
            clock_skew_ms=0,
            checked_at=datetime.now(UTC),
            missing=(),
        )

    async def discover_instruments(self) -> tuple[Instrument, ...]:
        return (self.instrument,)

    async def watch_bbo(self, symbols: tuple[str, ...]) -> tuple[BboQuote, ...]:
        assert symbols == (self.instrument.symbol,)
        offset = Decimal("1") if self.venue == Venue.BYBIT else Decimal("0")
        return (
            BboQuote(
                venue=self.venue,
                symbol=self.instrument.symbol,
                bid_price=Decimal("100") + offset,
                bid_base_quantity=Decimal("1"),
                ask_price=Decimal("101") + offset,
                ask_base_quantity=Decimal("1"),
                exchange_timestamp_ms=1_700_000_000_000,
                received_at=datetime.now(UTC),
                received_monotonic_ns=time.monotonic_ns(),
                clock_skew_ms=0,
            ),
        )

    async def watch_order_book(self, instrument: Instrument, limit: int = 50) -> OrderBookSnapshot:
        del limit
        self.book_calls += 1
        offset = Decimal("1") if self.venue == Venue.BYBIT else Decimal("0")
        return OrderBookSnapshot(
            venue=self.venue,
            symbol=instrument.symbol,
            bids=(BookLevel(Decimal("100") + offset, Decimal("1")),),
            asks=(BookLevel(Decimal("101") + offset, Decimal("1")),),
            exchange_timestamp_ms=1_700_000_000_000,
            received_at=datetime.now(UTC),
            received_monotonic_ns=time.monotonic_ns(),
            sequence_start=1,
            sequence_end=1,
            is_snapshot=True,
            synchronised=True,
            clock_skew_ms=0,
        )

    async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot:
        return FundingSnapshot(
            venue=self.venue,
            symbol=instrument.symbol,
            rate=Decimal("0.0001"),
            next_funding_timestamp_ms=1_700_000_100_000,
            interval="8h",
            mark_price=Decimal("101"),
            index_price=Decimal("100.9"),
            exchange_timestamp_ms=1_700_000_000_000,
        )

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_engine_quarantines_failed_venue_and_continues(tmp_path: Path) -> None:
    adapters = {
        Venue.BINANCE_USDM: FakeAdapter(Venue.BINANCE_USDM),
        Venue.BYBIT: FakeAdapter(Venue.BYBIT),
        Venue.OKX: FakeAdapter(Venue.OKX, fail_probe=True),
    }
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
    )
    result = await engine.scan_once("BTC", Decimal("0.001"), timeout_seconds=1)
    await engine.close()

    assert result.common_instrument_count == 1
    assert len(result.bbo) == 2
    assert len(result.quotes) == 2
    assert all(quote.eligible for quote in result.quotes)
    assert {record.venue for record in result.quarantined} == {Venue.OKX}
    assert query_recorded_level_count(tmp_path) == 4
    assert adapters[Venue.BINANCE_USDM].book_calls == 2
    assert adapters[Venue.BYBIT].book_calls == 2
    assert all(adapter.closed for adapter in adapters.values())
