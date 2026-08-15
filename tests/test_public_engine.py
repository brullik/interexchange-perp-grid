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


def make_many_instruments(venue: Venue, count: int = 100) -> tuple[Instrument, ...]:
    return tuple(
        Instrument(
            venue=venue,
            symbol=f"A{index:03d}/USDT:USDT",
            exchange_symbol=f"{venue.value}-A{index:03d}",
            base=f"A{index:03d}",
            quote="USDT",
            settle="USDT",
            contract_size_base=Decimal("1"),
            amount_step_contracts=Decimal("0.001"),
            price_tick=Decimal("0.1"),
            minimum_amount_contracts=Decimal("0.001"),
            minimum_notional=Decimal("5"),
            taker_fee_rate=Decimal("0.0005"),
            fee_source="fixture",
            active=True,
            listed_at=datetime(2025, 1, 1, tzinfo=UTC),
        )
        for index in range(count)
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


class BroadFakeAdapter(ExchangeAdapter):
    def __init__(
        self,
        venue: Venue,
        received_ns: int,
        *,
        fail_probe: bool = False,
        fail_bbo: bool = False,
    ) -> None:
        self.venue = venue
        self.received_ns = received_ns
        self.fail_probe = fail_probe
        self.fail_bbo = fail_bbo
        self.instruments = make_many_instruments(venue)
        self.discover_calls = 0
        self.bbo_calls = 0

    async def probe_public_capabilities(self) -> CapabilityReport:
        if self.fail_probe:
            raise ConnectionError("fixture probe outage")
        return CapabilityReport(
            self.venue,
            True,
            True,
            True,
            True,
            True,
            0,
            datetime.now(UTC),
            (),
        )

    async def discover_instruments(self) -> tuple[Instrument, ...]:
        self.discover_calls += 1
        return self.instruments

    async def watch_bbo(self, symbols: tuple[str, ...]) -> tuple[BboQuote, ...]:
        self.bbo_calls += 1
        assert symbols == tuple(instrument.symbol for instrument in self.instruments)
        if self.fail_bbo:
            raise ConnectionError("fixture BBO outage")
        offset = Decimal(list(Venue).index(self.venue))
        return tuple(
            BboQuote(
                self.venue,
                instrument.symbol,
                Decimal(100) + offset,
                Decimal(1),
                Decimal("100.5") + offset,
                Decimal(1),
                1_700_000_000_000,
                datetime.now(UTC),
                self.received_ns,
                0,
            )
            for instrument in self.instruments
        )

    async def watch_order_book(
        self,
        instrument: Instrument,
        limit: int = 50,
    ) -> OrderBookSnapshot:
        del instrument, limit
        raise AssertionError("broad BBO prefilter must not subscribe to L2")

    async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot:
        del instrument
        raise AssertionError("broad BBO prefilter must not fetch funding")

    async def close(self) -> None:
        return None


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


@pytest.mark.asyncio
async def test_wave1_public_scan_does_not_require_private_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for venue in Venue:
        prefix = venue.value.upper()
        monkeypatch.delenv(f"IPEG_{prefix}_API_KEY", raising=False)
        monkeypatch.delenv(f"IPEG_{prefix}_API_SECRET", raising=False)
        monkeypatch.delenv(f"IPEG_{prefix}_API_PASSWORD", raising=False)
    adapters = {venue: FakeAdapter(venue) for venue in Venue}
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
    )

    result = await engine.scan_once("BTC", Decimal("0.001"), timeout_seconds=1)
    await engine.close()

    assert len(result.bbo) == 3
    assert len(result.quotes) == 6
    assert result.quarantined == ()


@pytest.mark.asyncio
async def test_broad_bbo_scans_100_common_instruments_and_isolates_one_venue(
    tmp_path: Path,
) -> None:
    clock = 1_000_000_000
    adapters = {
        venue: BroadFakeAdapter(
            venue,
            clock,
            fail_bbo=venue == Venue.OKX,
        )
        for venue in Venue
    }
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock,
    )

    result = await engine.scan_broad_bbo(timeout_seconds=1)
    await engine.close()

    assert result.common_instrument_count == 100
    assert result.directed_route_count == 600
    assert len(result.bbo) == 200
    assert len(result.prefilter) == 200
    assert all(observation.execution_authorized is False for observation in result.prefilter)
    assert result.cache.known_keys == 200
    assert result.cache.entries == result.cache.peak_entries == 200
    assert result.prefilter_latency_ms <= Decimal(100)
    assert {record.venue for record in result.quarantined} == {Venue.OKX}
    assert all(adapter.bbo_calls == 1 for adapter in adapters.values())


@pytest.mark.asyncio
async def test_reconnect_forces_universe_refresh_and_restores_venue(tmp_path: Path) -> None:
    clock = 1_000_000_000
    adapters = {venue: BroadFakeAdapter(venue, clock) for venue in Venue}
    adapters[Venue.OKX].fail_probe = True
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock,
    )
    await engine.initialise(timeout_seconds=1)
    initial = await engine.refresh_universe(timeout_seconds=1)
    adapters[Venue.OKX].fail_probe = False

    refreshed = await engine.refresh_universe(
        timeout_seconds=1,
        reconnected=(Venue.OKX,),
    )
    await engine.close()

    assert len(initial.routes) == 200
    assert len(refreshed.routes) == 600
    assert refreshed.generation == initial.generation + 1
    assert adapters[Venue.OKX].discover_calls == 1
