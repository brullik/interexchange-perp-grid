from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
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
from interexchange_perp_grid.public_engine import (
    PublicMarketEngine,
    reconnect_backoff_seconds,
    reconnect_delay_seconds,
)

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
        minimum_notional=Decimal("0.01"),
        taker_fee_rate=Decimal("0.0005"),
        fee_source="fixture",
        listed_at=datetime(2025, 1, 1, tzinfo=UTC),
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

    async def unwatch_bbo(self, symbols: tuple[str, ...]) -> None:
        del symbols

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


class CoordinatedFundingFakeAdapter(FakeAdapter):
    def __init__(self, venue: Venue) -> None:
        super().__init__(venue)
        self.funding_started = asyncio.Event()
        self.allow_funding = asyncio.Event()

    async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot:
        self.funding_started.set()
        await self.allow_funding.wait()
        return await super().fetch_funding(instrument)


class CoordinatedRecorder(ParquetMarketRecorder):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.append_started = asyncio.Event()
        self.allow_append = asyncio.Event()
        self.appended: tuple[OrderBookSnapshot, ...] = ()

    async def append_books(self, books: tuple[OrderBookSnapshot, ...]) -> tuple[Path, ...]:
        self.append_started.set()
        await self.allow_append.wait()
        self.appended = books
        return ()


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
        self.probe_calls = 0
        self.bbo_calls = 0
        self.bbo_unwatch_calls: list[tuple[str, ...]] = []
        self.last_bbo_symbols: tuple[str, ...] = ()
        self.bbo_subscription_changes = 0
        self.closed = False

    async def probe_public_capabilities(self) -> CapabilityReport:
        self.probe_calls += 1
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
        if symbols != self.last_bbo_symbols:
            self.last_bbo_symbols = symbols
            self.bbo_subscription_changes += 1
        assert set(symbols).issubset({instrument.symbol for instrument in self.instruments})
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
            if instrument.symbol in symbols
        )

    async def unwatch_bbo(self, symbols: tuple[str, ...]) -> None:
        self.bbo_unwatch_calls.append(symbols)

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
        self.closed = True


class IncrementalBroadFakeAdapter(BroadFakeAdapter):
    def __init__(self, venue: Venue, received_ns: int) -> None:
        super().__init__(venue, received_ns)
        self._next_index = 0
        self.active_calls = 0
        self.peak_concurrent_calls = 0

    async def watch_bbo(self, symbols: tuple[str, ...]) -> tuple[BboQuote, ...]:
        self.bbo_calls += 1
        self.active_calls += 1
        self.peak_concurrent_calls = max(self.peak_concurrent_calls, self.active_calls)
        try:
            await asyncio.sleep(0)
            instrument = self.instruments[self._next_index % len(self.instruments)]
            self._next_index += 1
            assert instrument.symbol in symbols
            offset = Decimal(list(Venue).index(self.venue))
            return (
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
                ),
            )
        finally:
            self.active_calls -= 1


class CancellationResistantBroadFakeAdapter(BroadFakeAdapter):
    def __init__(self, venue: Venue, received_ns: int) -> None:
        super().__init__(venue, received_ns)
        self.release = asyncio.Event()
        self.active_calls = 0
        self.peak_concurrent_calls = 0

    async def watch_bbo(self, symbols: tuple[str, ...]) -> tuple[BboQuote, ...]:
        self.bbo_calls += 1
        self.last_bbo_symbols = symbols
        self.active_calls += 1
        self.peak_concurrent_calls = max(self.peak_concurrent_calls, self.active_calls)
        try:
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                await self.release.wait()
            return ()
        finally:
            self.active_calls -= 1

    async def close(self) -> None:
        self.release.set()
        await super().close()


class CoordinatedRecycleBroadFakeAdapter(CancellationResistantBroadFakeAdapter):
    def __init__(self, venue: Venue, received_ns: int) -> None:
        super().__init__(venue, received_ns)
        self.close_started = asyncio.Event()
        self.allow_close = asyncio.Event()
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1
        self.close_started.set()
        await self.allow_close.wait()
        await super().close()


class CoordinatedProbeBroadFakeAdapter(BroadFakeAdapter):
    def __init__(self, venue: Venue, received_ns: int) -> None:
        super().__init__(venue, received_ns)
        self.probe_started = asyncio.Event()
        self.allow_probe = asyncio.Event()

    async def probe_public_capabilities(self) -> CapabilityReport:
        self.probe_started.set()
        await self.allow_probe.wait()
        return await super().probe_public_capabilities()


class CoordinatedDiscoveryBroadFakeAdapter(BroadFakeAdapter):
    def __init__(self, venue: Venue, received_ns: int) -> None:
        super().__init__(venue, received_ns)
        self.discovery_started = asyncio.Event()
        self.allow_discovery = asyncio.Event()

    async def discover_instruments(self) -> tuple[Instrument, ...]:
        self.discovery_started.set()
        await self.allow_discovery.wait()
        return await super().discover_instruments()


class PermanentlyCancellationResistantBroadFakeAdapter(BroadFakeAdapter):
    def __init__(self, venue: Venue, received_ns: int) -> None:
        super().__init__(venue, received_ns)
        self.release = asyncio.Event()
        self.stopped = asyncio.Event()
        self.active_calls = 0
        self.cancellation_count = 0

    async def watch_bbo(self, symbols: tuple[str, ...]) -> tuple[BboQuote, ...]:
        self.bbo_calls += 1
        self.last_bbo_symbols = symbols
        self.active_calls += 1
        try:
            while not self.release.is_set():
                try:
                    await self.release.wait()
                except asyncio.CancelledError:
                    self.cancellation_count += 1
            return ()
        finally:
            self.active_calls -= 1
            self.stopped.set()

    async def close(self) -> None:
        return None


class FailingCloseBroadFakeAdapter(BroadFakeAdapter):
    async def close(self) -> None:
        self.closed = True
        raise RuntimeError("fixture close failure")


class SilentFailingCloseBroadFakeAdapter(BroadFakeAdapter):
    def __init__(self, venue: Venue, received_ns: int) -> None:
        super().__init__(venue, received_ns)
        self.never_returns = asyncio.Event()
        self.close_calls = 0

    async def watch_bbo(self, symbols: tuple[str, ...]) -> tuple[BboQuote, ...]:
        self.bbo_calls += 1
        self.last_bbo_symbols = symbols
        await self.never_returns.wait()
        return ()

    async def close(self) -> None:
        self.close_calls += 1
        self.closed = True
        raise RuntimeError("fixture close failure")


class DelayedBroadFakeAdapter(BroadFakeAdapter):
    async def watch_bbo(self, symbols: tuple[str, ...]) -> tuple[BboQuote, ...]:
        await asyncio.sleep(0.02)
        return await super().watch_bbo(symbols)


class OneLateBatchBroadFakeAdapter(BroadFakeAdapter):
    def __init__(self, venue: Venue, received_ns: int) -> None:
        super().__init__(venue, received_ns)
        self.transport_calls = 0
        self.late_batch_started = asyncio.Event()
        self.release_late_batch = asyncio.Event()
        self.recovered = asyncio.Event()

    async def watch_bbo(self, symbols: tuple[str, ...]) -> tuple[BboQuote, ...]:
        self.transport_calls += 1
        if self.transport_calls == 2:
            captured_ns = self.received_ns
            self.late_batch_started.set()
            await self.release_late_batch.wait()
            current_ns = self.received_ns
            self.received_ns = captured_ns
            try:
                return await super().watch_bbo(symbols)
            finally:
                self.received_ns = current_ns
        quotes = await super().watch_bbo(symbols)
        if self.transport_calls >= 3:
            self.recovered.set()
        return quotes


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
async def test_shutdown_never_allows_route_scan_to_resume_on_closed_adapters(
    tmp_path: Path,
) -> None:
    adapters: dict[Venue, FakeAdapter] = {venue: FakeAdapter(venue) for venue in Venue}
    funding = CoordinatedFundingFakeAdapter(Venue.OKX)
    adapters[Venue.OKX] = funding
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
    )

    scan = asyncio.create_task(engine.scan_once("BTC", Decimal("0.001"), timeout_seconds=5))
    await asyncio.wait_for(funding.funding_started.wait(), timeout=1)
    with pytest.raises(RuntimeError, match=r"shutdown deadline exceeded.*public scans"):
        await engine.close()
    assert all(adapter.book_calls == 0 for adapter in adapters.values())

    funding.allow_funding.set()
    with pytest.raises(asyncio.CancelledError):
        await scan

    assert all(adapter.book_calls == 0 for adapter in adapters.values())
    await engine.close()


@pytest.mark.asyncio
async def test_shutdown_cancels_blocked_recorder_before_return(tmp_path: Path) -> None:
    adapters = {venue: FakeAdapter(venue) for venue in Venue}
    recorder = CoordinatedRecorder(tmp_path)
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=recorder,
    )

    scan = asyncio.create_task(engine.scan_once("BTC", Decimal("0.001"), timeout_seconds=5))
    await asyncio.wait_for(recorder.append_started.wait(), timeout=1)
    with pytest.raises(RuntimeError, match=r"shutdown deadline exceeded.*public scans"):
        await engine.close()

    assert recorder.appended == ()
    recorder.allow_append.set()
    with pytest.raises(asyncio.CancelledError):
        await scan
    assert recorder.appended == ()
    await engine.close()


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
    assert result.discovered_route_count == 600
    assert result.directed_route_count == 200
    assert len(result.bbo) == 200
    assert len(result.prefilter) == 200
    assert all(observation.execution_authorized is False for observation in result.prefilter)
    assert result.cache.known_keys == 200
    assert result.cache.entries == result.cache.peak_entries == 200
    assert result.prefilter_latency_ms <= Decimal(100)
    assert {record.venue for record in result.quarantined} == {Venue.OKX}
    assert adapters[Venue.OKX].bbo_calls == 1
    assert all(adapter.bbo_calls >= 1 for adapter in adapters.values())
    assert all(adapter.bbo_subscription_changes == 1 for adapter in adapters.values())


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


@pytest.mark.asyncio
async def test_incremental_batch_updates_fill_bounded_cache_with_one_watcher_per_venue(
    tmp_path: Path,
) -> None:
    clock = 1_000_000_000
    adapters = {venue: IncrementalBroadFakeAdapter(venue, clock) for venue in Venue}
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock,
    )

    result = await engine.scan_broad_bbo(timeout_seconds=2)
    await engine.close()

    assert result.cache.known_keys == result.cache.entries == 300
    assert len(result.bbo) == 300
    assert len(result.prefilter) == 600
    assert all(adapter.bbo_calls >= 100 for adapter in adapters.values())
    assert all(adapter.peak_concurrent_calls == 1 for adapter in adapters.values())


@pytest.mark.asyncio
async def test_failed_bbo_venue_is_retried_automatically_after_backoff(tmp_path: Path) -> None:
    clock = [1_000_000_000]
    adapters = {venue: BroadFakeAdapter(venue, clock[0]) for venue in Venue}
    adapters[Venue.OKX].fail_probe = True
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
        reconnect_jitter=lambda venue, attempt: Decimal(1),
    )

    initial = await engine.scan_broad_bbo(timeout_seconds=1)
    adapters[Venue.OKX].fail_probe = False
    before_retry = await engine.scan_broad_bbo(timeout_seconds=1)
    clock[0] += 1_000_000_000
    for adapter in adapters.values():
        adapter.received_ns = clock[0]
    recovered = await engine.scan_broad_bbo(timeout_seconds=1)
    await engine.close()

    assert initial.directed_route_count == before_retry.directed_route_count == 200
    assert recovered.directed_route_count == recovered.discovered_route_count == 600
    assert recovered.quarantined == ()
    assert adapters[Venue.OKX].probe_calls == 2


def test_reconnect_backoff_is_exponential_and_capped_at_30_seconds() -> None:
    assert tuple(reconnect_backoff_seconds(attempt) for attempt in range(1, 9)) == (
        1,
        2,
        4,
        8,
        16,
        30,
        30,
        30,
    )


def test_reconnect_delay_applies_bounded_jitter() -> None:
    assert reconnect_delay_seconds(
        Venue.OKX,
        3,
        lambda venue, attempt: Decimal("0.9"),
    ) == Decimal("3.6")
    assert reconnect_delay_seconds(
        Venue.OKX,
        6,
        lambda venue, attempt: Decimal("1.1"),
    ) == Decimal("30")
    with pytest.raises(ValueError, match="jitter"):
        reconnect_delay_seconds(
            Venue.OKX,
            1,
            lambda venue, attempt: Decimal("1.3"),
        )


@pytest.mark.asyncio
async def test_bbo_failure_streak_survives_probe_until_stream_recovers(tmp_path: Path) -> None:
    clock = [1_000_000_000]
    adapters = {venue: BroadFakeAdapter(venue, clock[0]) for venue in Venue}
    failing = adapters[Venue.OKX]
    failing.fail_bbo = True
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    settings = settings.model_copy(
        update={"market_data": settings.market_data.model_copy(update={"max_bbo_age_ms": 10_000})}
    )
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
        reconnect_jitter=lambda venue, attempt: Decimal(1),
    )

    await engine.scan_broad_bbo(timeout_seconds=1)
    assert failing.probe_calls == 1
    assert failing.bbo_calls == 1
    assert engine._reconnect_attempts[Venue.OKX] == 1

    clock[0] += 1_000_000_000
    for adapter in adapters.values():
        adapter.received_ns = clock[0]
    await engine.scan_broad_bbo(timeout_seconds=1)
    assert failing.probe_calls == 2
    assert failing.bbo_calls == 2
    assert engine._reconnect_attempts[Venue.OKX] == 2

    clock[0] += 1_000_000_000
    for adapter in adapters.values():
        adapter.received_ns = clock[0]
    await engine.scan_broad_bbo(timeout_seconds=1)
    assert failing.probe_calls == 2
    assert failing.bbo_calls == 2

    clock[0] += 1_000_000_000
    failing.fail_bbo = False
    for adapter in adapters.values():
        adapter.received_ns = clock[0]
    recovered = await engine.scan_broad_bbo(timeout_seconds=1)
    await engine.close()

    assert failing.probe_calls == 3
    assert recovered.directed_route_count == 600
    assert Venue.OKX not in engine._reconnect_attempts


@pytest.mark.asyncio
async def test_one_late_batch_does_not_quarantine_an_otherwise_healthy_stream(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters: dict[Venue, BroadFakeAdapter] = {
        venue: BroadFakeAdapter(venue, clock[0]) for venue in Venue
    }
    delayed = OneLateBatchBroadFakeAdapter(Venue.BYBIT, clock[0])
    adapters[Venue.BYBIT] = delayed
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    settings = settings.model_copy(
        update={"market_data": settings.market_data.model_copy(update={"max_bbo_age_ms": 500})}
    )
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )

    initial = await engine.scan_broad_bbo(timeout_seconds=1)
    await asyncio.wait_for(delayed.late_batch_started.wait(), timeout=1)
    clock[0] += 600_000_000
    for adapter in adapters.values():
        adapter.received_ns = clock[0]
    delayed.release_late_batch.set()
    await asyncio.wait_for(delayed.recovered.wait(), timeout=1)
    recovered = await engine.scan_broad_bbo(timeout_seconds=1)
    await engine.close()

    assert initial.directed_route_count == recovered.directed_route_count == 600
    assert recovered.quarantined == ()
    assert recovered.cache.rejected_updates >= 100


@pytest.mark.asyncio
async def test_silent_bbo_transport_is_recycled_without_duplicate_watcher(tmp_path: Path) -> None:
    clock = [1_000_000_000]
    adapters: dict[Venue, BroadFakeAdapter] = {
        venue: BroadFakeAdapter(venue, clock[0]) for venue in Venue
    }
    hanging = CancellationResistantBroadFakeAdapter(Venue.OKX, clock[0])
    replacement = BroadFakeAdapter(Venue.OKX, clock[0])
    okx_factory_calls = 0

    def adapter_factory(venue: Venue) -> BroadFakeAdapter:
        nonlocal okx_factory_calls
        if venue != Venue.OKX:
            return adapters[venue]
        okx_factory_calls += 1
        return hanging if okx_factory_calls == 1 else replacement

    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    settings = settings.model_copy(
        update={"market_data": settings.market_data.model_copy(update={"max_bbo_age_ms": 20})}
    )
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapter_factory,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
        reconnect_jitter=lambda venue, attempt: Decimal(1),
    )

    failed = await engine.scan_broad_bbo(timeout_seconds=1)
    clock[0] += 1_000_000_000
    for adapter in (*adapters.values(), replacement):
        adapter.received_ns = clock[0]
    recovered = await engine.scan_broad_bbo(timeout_seconds=1)

    assert {record.venue for record in failed.quarantined} == {Venue.OKX}
    assert "TimeoutError" in failed.quarantined[0].reason
    assert hanging.bbo_calls == 1
    assert hanging.active_calls == 0
    assert hanging.peak_concurrent_calls == 1
    assert replacement.bbo_calls >= 1
    assert okx_factory_calls == 2
    assert recovered.directed_route_count == 600
    assert recovered.quarantined == ()

    await engine.close()
    await asyncio.sleep(0)

    assert hanging.active_calls == 0
    assert not tuple(
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
        and task.get_name().startswith("broad-bbo-")
        and not task.done()
    )


@pytest.mark.asyncio
async def test_concurrent_reconnects_recycle_one_adapter_once(tmp_path: Path) -> None:
    clock = [1_000_000_000]
    adapters: dict[Venue, BroadFakeAdapter] = {
        venue: BroadFakeAdapter(venue, clock[0]) for venue in Venue
    }
    hanging = CoordinatedRecycleBroadFakeAdapter(Venue.OKX, clock[0])
    created_okx_adapters: list[BroadFakeAdapter] = []

    def adapter_factory(venue: Venue) -> BroadFakeAdapter:
        if venue != Venue.OKX:
            return adapters[venue]
        adapter: BroadFakeAdapter = (
            hanging if not created_okx_adapters else BroadFakeAdapter(Venue.OKX, clock[0])
        )
        created_okx_adapters.append(adapter)
        return adapter

    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    settings = settings.model_copy(
        update={"market_data": settings.market_data.model_copy(update={"max_bbo_age_ms": 20})}
    )
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapter_factory,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
        reconnect_jitter=lambda venue, attempt: Decimal(1),
    )

    failed = await engine.scan_broad_bbo(timeout_seconds=1)
    assert {record.venue for record in failed.quarantined} == {Venue.OKX}
    clock[0] += 1_000_000_000
    for adapter in adapters.values():
        adapter.received_ns = clock[0]

    scans = tuple(asyncio.create_task(engine.scan_broad_bbo(timeout_seconds=1)) for _ in range(2))
    try:
        await asyncio.wait_for(hanging.close_started.wait(), timeout=1)
        await asyncio.sleep(0)
        hanging.allow_close.set()
        recovered = await asyncio.gather(*scans)
    finally:
        hanging.allow_close.set()
        for scan in scans:
            if not scan.done():
                scan.cancel()
        await asyncio.gather(*scans, return_exceptions=True)

    replacement = created_okx_adapters[-1]
    assert hanging.close_calls == 1
    assert len(created_okx_adapters) == 2
    assert replacement is not hanging
    assert replacement.probe_calls == 1
    assert replacement.discover_calls == 1
    assert replacement.bbo_calls >= 1
    assert all(result.directed_route_count == 600 for result in recovered)
    assert all(result.quarantined == () for result in recovered)

    await engine.close()
    await asyncio.sleep(0)

    assert all(adapter.closed for adapter in created_okx_adapters)
    assert not tuple(
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
        and task.get_name().startswith("broad-bbo-")
        and not task.done()
    )


@pytest.mark.asyncio
async def test_shutdown_waits_for_recycle_and_prevents_escaped_replacement(tmp_path: Path) -> None:
    clock = [1_000_000_000]
    adapters: dict[Venue, BroadFakeAdapter] = {
        venue: BroadFakeAdapter(venue, clock[0]) for venue in Venue
    }
    hanging = CoordinatedRecycleBroadFakeAdapter(Venue.OKX, clock[0])
    created_okx_adapters: list[BroadFakeAdapter] = []

    def adapter_factory(venue: Venue) -> BroadFakeAdapter:
        if venue != Venue.OKX:
            return adapters[venue]
        adapter: BroadFakeAdapter = (
            hanging if not created_okx_adapters else BroadFakeAdapter(Venue.OKX, clock[0])
        )
        created_okx_adapters.append(adapter)
        return adapter

    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    settings = settings.model_copy(
        update={"market_data": settings.market_data.model_copy(update={"max_bbo_age_ms": 20})}
    )
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapter_factory,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
        reconnect_jitter=lambda venue, attempt: Decimal(1),
    )

    failed = await engine.scan_broad_bbo(timeout_seconds=1)
    assert {record.venue for record in failed.quarantined} == {Venue.OKX}
    clock[0] += 1_000_000_000
    for adapter in adapters.values():
        adapter.received_ns = clock[0]

    scan = asyncio.create_task(engine.scan_broad_bbo(timeout_seconds=1))
    await asyncio.wait_for(hanging.close_started.wait(), timeout=1)
    shutdown = asyncio.create_task(engine.close())
    await asyncio.sleep(0)
    hanging.allow_close.set()
    recovered, shutdown_result = await asyncio.gather(scan, shutdown, return_exceptions=True)

    assert shutdown_result is None
    assert isinstance(recovered, RuntimeError)
    assert str(recovered) == "public market engine is closed"
    assert len(created_okx_adapters) == 1
    assert hanging.close_calls == 2
    assert hanging.closed is True
    assert all(adapters[venue].closed for venue in Venue if venue != Venue.OKX)
    assert not tuple(
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
        and task.get_name().startswith("broad-bbo-")
        and not task.done()
    )


@pytest.mark.asyncio
async def test_shutdown_during_replacement_probe_cannot_report_recovery(tmp_path: Path) -> None:
    clock = [1_000_000_000]
    adapters: dict[Venue, BroadFakeAdapter] = {
        venue: BroadFakeAdapter(venue, clock[0]) for venue in Venue
    }
    hanging = CancellationResistantBroadFakeAdapter(Venue.OKX, clock[0])
    replacement = CoordinatedProbeBroadFakeAdapter(Venue.OKX, clock[0])
    created_okx_adapters: list[BroadFakeAdapter] = []

    def adapter_factory(venue: Venue) -> BroadFakeAdapter:
        if venue != Venue.OKX:
            return adapters[venue]
        adapter = hanging if not created_okx_adapters else replacement
        created_okx_adapters.append(adapter)
        return adapter

    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    settings = settings.model_copy(
        update={"market_data": settings.market_data.model_copy(update={"max_bbo_age_ms": 20})}
    )
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapter_factory,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
        reconnect_jitter=lambda venue, attempt: Decimal(1),
    )

    failed = await engine.scan_broad_bbo(timeout_seconds=1)
    assert {record.venue for record in failed.quarantined} == {Venue.OKX}
    clock[0] += 1_000_000_000
    for adapter in (*adapters.values(), replacement):
        adapter.received_ns = clock[0]

    scan = asyncio.create_task(engine.scan_broad_bbo(timeout_seconds=1))
    await asyncio.wait_for(replacement.probe_started.wait(), timeout=1)
    shutdown = asyncio.create_task(engine.close())
    await asyncio.sleep(0)
    replacement.allow_probe.set()
    recovered, shutdown_result = await asyncio.gather(scan, shutdown, return_exceptions=True)

    assert shutdown_result is None
    assert isinstance(recovered, RuntimeError)
    assert str(recovered) == "public market engine is closed"
    assert len(created_okx_adapters) == 2
    assert replacement.probe_calls == 1
    assert replacement.discover_calls == 0
    assert replacement.closed is True
    assert Venue.OKX in engine._quarantined
    assert Venue.OKX not in engine._instruments
    assert not tuple(
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
        and task.get_name().startswith("broad-bbo-")
        and not task.done()
    )


@pytest.mark.asyncio
async def test_closed_uninitialised_engine_cannot_create_adapters(tmp_path: Path) -> None:
    created: list[BroadFakeAdapter] = []

    def adapter_factory(venue: Venue) -> BroadFakeAdapter:
        adapter = BroadFakeAdapter(venue, 1_000_000_000)
        created.append(adapter)
        return adapter

    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapter_factory,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: 1_000_000_000,
    )

    await engine.close()

    with pytest.raises(RuntimeError, match="public market engine is closed"):
        await engine.initialise(timeout_seconds=1)
    with pytest.raises(RuntimeError, match="public market engine is closed"):
        await engine.scan_once("BTC", Decimal("0.001"), timeout_seconds=1)
    assert created == []


@pytest.mark.asyncio
async def test_concurrent_initialise_calls_share_one_adapter_generation(tmp_path: Path) -> None:
    created: list[BroadFakeAdapter] = []

    def adapter_factory(venue: Venue) -> BroadFakeAdapter:
        adapter = BroadFakeAdapter(venue, 1_000_000_000)
        created.append(adapter)
        return adapter

    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapter_factory,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: 1_000_000_000,
    )

    await asyncio.gather(
        engine.initialise(timeout_seconds=1),
        engine.initialise(timeout_seconds=1),
    )
    await engine.close()

    assert len(created) == 3
    assert all(adapter.probe_calls == 1 for adapter in created)
    assert all(adapter.discover_calls == 1 for adapter in created)
    assert all(adapter.closed for adapter in created)


@pytest.mark.asyncio
async def test_partial_adapter_factory_failure_rolls_back_created_adapters(tmp_path: Path) -> None:
    created: list[BroadFakeAdapter] = []

    def adapter_factory(venue: Venue) -> BroadFakeAdapter:
        if venue == Venue.OKX:
            raise RuntimeError("fixture factory failure")
        adapter = BroadFakeAdapter(venue, 1_000_000_000)
        created.append(adapter)
        return adapter

    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapter_factory,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: 1_000_000_000,
    )

    with pytest.raises(RuntimeError, match="fixture factory failure"):
        await engine.initialise(timeout_seconds=1)

    assert len(created) == 2
    assert all(adapter.closed for adapter in created)
    assert engine._adapters == {}
    await engine.close()


@pytest.mark.asyncio
async def test_cancelled_partial_factory_rollback_retains_adapter_ownership(
    tmp_path: Path,
) -> None:
    created: list[CoordinatedRecycleBroadFakeAdapter] = []
    factory_failed = asyncio.Event()

    def adapter_factory(venue: Venue) -> BroadFakeAdapter:
        if venue == Venue.OKX:
            factory_failed.set()
            raise RuntimeError("fixture factory failure")
        adapter = CoordinatedRecycleBroadFakeAdapter(venue, 1_000_000_000)
        created.append(adapter)
        return adapter

    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapter_factory,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: 1_000_000_000,
    )

    initialise = asyncio.create_task(engine.initialise(timeout_seconds=1))
    await asyncio.wait_for(factory_failed.wait(), timeout=1)
    await asyncio.gather(*(adapter.close_started.wait() for adapter in created))
    initialise.cancel()
    with pytest.raises(asyncio.CancelledError):
        await initialise

    assert engine._closed is True
    assert set(engine._adapters) == {Venue.BINANCE_USDM, Venue.BYBIT}
    assert set(engine._retiring_adapter_closers) == {Venue.BINANCE_USDM, Venue.BYBIT}

    for adapter in created:
        adapter.allow_close.set()
    await engine.close()
    await asyncio.sleep(0)

    assert all(adapter.closed for adapter in created)
    assert engine._retiring_adapter_closers == {}
    assert not tuple(
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
        and task.get_name().startswith("rollback-close-")
        and not task.done()
    )


@pytest.mark.asyncio
async def test_concurrent_cold_scans_wait_for_one_initialisation(tmp_path: Path) -> None:
    created: list[BroadFakeAdapter] = []

    def adapter_factory(venue: Venue) -> BroadFakeAdapter:
        adapter = BroadFakeAdapter(venue, 1_000_000_000)
        created.append(adapter)
        return adapter

    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapter_factory,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: 1_000_000_000,
    )

    scans = await asyncio.gather(
        engine.scan_broad_bbo(timeout_seconds=1),
        engine.scan_broad_bbo(timeout_seconds=1),
    )
    await engine.close()

    assert len(created) == 3
    assert all(adapter.probe_calls == 1 for adapter in created)
    assert all(adapter.discover_calls == 1 for adapter in created)
    assert {result.universe_generation for result in scans} == {1}
    assert all(result.directed_route_count == 600 for result in scans)


@pytest.mark.asyncio
async def test_shutdown_during_initial_probe_closes_all_created_adapters(tmp_path: Path) -> None:
    clock = 1_000_000_000
    adapters: dict[Venue, BroadFakeAdapter] = {
        venue: BroadFakeAdapter(venue, clock) for venue in Venue
    }
    probing = CoordinatedProbeBroadFakeAdapter(Venue.OKX, clock)
    adapters[Venue.OKX] = probing
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock,
    )

    initialise = asyncio.create_task(engine.initialise(timeout_seconds=1))
    await asyncio.wait_for(probing.probe_started.wait(), timeout=1)
    shutdown = asyncio.create_task(engine.close())
    await asyncio.sleep(0)
    probing.allow_probe.set()
    initialise_result, shutdown_result = await asyncio.gather(
        initialise,
        shutdown,
        return_exceptions=True,
    )

    assert isinstance(initialise_result, RuntimeError)
    assert str(initialise_result) == "public market engine is closed"
    assert shutdown_result is None
    assert all(adapter.closed for adapter in adapters.values())


@pytest.mark.asyncio
async def test_late_initial_probe_cannot_mutate_state_after_shutdown_timeout(
    tmp_path: Path,
) -> None:
    clock = 1_000_000_000
    adapters: dict[Venue, BroadFakeAdapter] = {
        venue: BroadFakeAdapter(venue, clock) for venue in Venue
    }
    probing = CoordinatedProbeBroadFakeAdapter(Venue.OKX, clock)
    adapters[Venue.OKX] = probing
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock,
    )

    initialise = asyncio.create_task(engine.initialise(timeout_seconds=5))
    await asyncio.wait_for(probing.probe_started.wait(), timeout=1)
    with pytest.raises(RuntimeError, match=r"shutdown deadline exceeded.*public lifecycle"):
        await engine.close()
    capabilities_after_close = dict(engine._capabilities)
    instruments_after_close = dict(engine._instruments)

    probing.allow_probe.set()
    with pytest.raises(RuntimeError, match="public market engine is closed"):
        await initialise

    assert engine._capabilities == capabilities_after_close
    assert engine._instruments == instruments_after_close
    assert Venue.OKX not in engine._capabilities
    assert Venue.OKX not in engine._instruments


@pytest.mark.asyncio
async def test_late_initial_discovery_cannot_mutate_state_after_shutdown_timeout(
    tmp_path: Path,
) -> None:
    clock = 1_000_000_000
    adapters: dict[Venue, BroadFakeAdapter] = {
        venue: BroadFakeAdapter(venue, clock) for venue in Venue
    }
    discovering = CoordinatedDiscoveryBroadFakeAdapter(Venue.OKX, clock)
    adapters[Venue.OKX] = discovering
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock,
    )

    initialise = asyncio.create_task(engine.initialise(timeout_seconds=5))
    await asyncio.wait_for(discovering.discovery_started.wait(), timeout=1)
    with pytest.raises(RuntimeError, match=r"shutdown deadline exceeded.*public lifecycle"):
        await engine.close()
    capabilities_after_close = dict(engine._capabilities)
    instruments_after_close = dict(engine._instruments)

    discovering.allow_discovery.set()
    with pytest.raises(RuntimeError, match="public market engine is closed"):
        await initialise

    assert engine._capabilities == capabilities_after_close
    assert engine._instruments == instruments_after_close
    assert Venue.OKX not in engine._capabilities
    assert Venue.OKX not in engine._instruments


@pytest.mark.asyncio
async def test_shutdown_waits_for_forced_refresh_and_blocks_late_state_mutation(
    tmp_path: Path,
) -> None:
    clock = 1_000_000_000
    adapters: dict[Venue, BroadFakeAdapter] = {
        venue: BroadFakeAdapter(venue, clock) for venue in Venue
    }
    probing = CoordinatedProbeBroadFakeAdapter(Venue.OKX, clock)
    probing.allow_probe.set()
    adapters[Venue.OKX] = probing
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock,
    )
    await engine.initialise(timeout_seconds=1)
    probing.probe_started.clear()
    probing.allow_probe.clear()

    refresh = asyncio.create_task(engine.refresh_universe(timeout_seconds=5, force=True))
    await asyncio.wait_for(probing.probe_started.wait(), timeout=1)
    with pytest.raises(RuntimeError, match=r"shutdown deadline exceeded.*public lifecycle"):
        await engine.close()
    capabilities_after_close = dict(engine._capabilities)
    instruments_after_close = dict(engine._instruments)

    probing.allow_probe.set()
    with pytest.raises(RuntimeError, match="public market engine is closed"):
        await refresh

    assert engine._capabilities == capabilities_after_close
    assert engine._instruments == instruments_after_close


@pytest.mark.asyncio
async def test_concurrent_failed_recycle_observes_new_backoff(tmp_path: Path) -> None:
    clock = [1_000_000_000]
    adapters: dict[Venue, BroadFakeAdapter] = {
        venue: BroadFakeAdapter(venue, clock[0]) for venue in Venue
    }
    failing = SilentFailingCloseBroadFakeAdapter(Venue.OKX, clock[0])
    adapters[Venue.OKX] = failing
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    settings = settings.model_copy(
        update={"market_data": settings.market_data.model_copy(update={"max_bbo_age_ms": 20})}
    )
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
        reconnect_jitter=lambda venue, attempt: Decimal(1),
    )

    failed = await engine.scan_broad_bbo(timeout_seconds=1)
    assert {record.venue for record in failed.quarantined} == {Venue.OKX}
    clock[0] += 1_000_000_000
    for adapter in adapters.values():
        adapter.received_ns = clock[0]

    recoveries = await asyncio.gather(
        engine.scan_broad_bbo(timeout_seconds=1),
        engine.scan_broad_bbo(timeout_seconds=1),
    )

    assert failing.close_calls == 1
    assert all(result.directed_route_count == 200 for result in recoveries)
    assert all(
        {record.venue for record in result.quarantined} == {Venue.OKX} for result in recoveries
    )

    with pytest.raises(RuntimeError, match=r"adapter shutdown failed.*okx"):
        await engine.close()
    assert failing.close_calls == 2


@pytest.mark.asyncio
async def test_concurrent_explicit_reconnects_coalesce_new_recycle_failure(
    tmp_path: Path,
) -> None:
    clock = [1_000_000_000]
    adapters: dict[Venue, BroadFakeAdapter] = {
        venue: BroadFakeAdapter(venue, clock[0]) for venue in Venue
    }
    failing = SilentFailingCloseBroadFakeAdapter(Venue.OKX, clock[0])
    adapters[Venue.OKX] = failing
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    settings = settings.model_copy(
        update={"market_data": settings.market_data.model_copy(update={"max_bbo_age_ms": 20})}
    )
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
        reconnect_jitter=lambda venue, attempt: Decimal(1),
    )

    failed = await engine.scan_broad_bbo(timeout_seconds=1)
    assert {record.venue for record in failed.quarantined} == {Venue.OKX}

    refreshed = await asyncio.gather(
        engine.refresh_universe(timeout_seconds=1, reconnected=(Venue.OKX,)),
        engine.refresh_universe(timeout_seconds=1, reconnected=(Venue.OKX,)),
    )

    assert failing.close_calls == 1
    assert all(snapshot.generation == failed.universe_generation for snapshot in refreshed)
    assert Venue.OKX in engine._quarantined

    with pytest.raises(RuntimeError, match=r"adapter shutdown failed.*okx"):
        await engine.close()
    assert failing.close_calls == 2


@pytest.mark.asyncio
async def test_close_reports_cancellation_resistant_transport_instead_of_false_success(
    tmp_path: Path,
) -> None:
    clock = 1_000_000_000
    adapters: dict[Venue, BroadFakeAdapter] = {
        venue: BroadFakeAdapter(venue, clock) for venue in Venue
    }
    hanging = PermanentlyCancellationResistantBroadFakeAdapter(Venue.OKX, clock)
    adapters[Venue.OKX] = hanging
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    settings = settings.model_copy(
        update={"market_data": settings.market_data.model_copy(update={"max_bbo_age_ms": 20})}
    )
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock,
    )

    await engine.scan_broad_bbo(timeout_seconds=1)
    started = time.perf_counter()
    with pytest.raises(RuntimeError, match=r"shutdown deadline exceeded.*okx"):
        await engine.close()
    elapsed = time.perf_counter() - started

    assert elapsed < 1.5
    assert hanging.active_calls == 1
    assert hanging.cancellation_count >= 2
    assert Venue.OKX in engine._retiring_bbo_transports

    hanging.release.set()
    await asyncio.wait_for(hanging.stopped.wait(), timeout=1)
    await engine.close()


@pytest.mark.asyncio
async def test_close_reports_adapter_teardown_failure(tmp_path: Path) -> None:
    clock = 1_000_000_000
    adapters: dict[Venue, BroadFakeAdapter] = {
        venue: BroadFakeAdapter(venue, clock) for venue in Venue
    }
    failing = FailingCloseBroadFakeAdapter(Venue.OKX, clock)
    adapters[Venue.OKX] = failing
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock,
    )

    result = await engine.scan_broad_bbo(timeout_seconds=1)
    assert result.directed_route_count == 600

    with pytest.raises(
        RuntimeError,
        match=r"adapter shutdown failed.*okx: RuntimeError: fixture close failure",
    ):
        await engine.close()

    assert all(adapter.closed for adapter in adapters.values())


@pytest.mark.asyncio
async def test_six_hour_refresh_resubscribes_watchers_to_new_symbols(tmp_path: Path) -> None:
    clock = [1_000_000_000]
    adapters = {venue: BroadFakeAdapter(venue, clock[0]) for venue in Venue}
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )

    initial = await engine.scan_broad_bbo(timeout_seconds=1)
    for adapter in adapters.values():
        template = adapter.instruments[0]
        adapter.instruments = (
            *adapter.instruments,
            replace(
                template,
                symbol="NEW/USDT:USDT",
                exchange_symbol=f"{adapter.venue.value}-NEW",
                base="NEW",
            ),
        )
    clock[0] += settings.universe.instrument_refresh_seconds * 1_000_000_000
    for adapter in adapters.values():
        adapter.received_ns = clock[0]

    refreshed = await engine.scan_broad_bbo(timeout_seconds=1)
    await engine.close()

    assert initial.common_instrument_count == 100
    assert refreshed.common_instrument_count == 101
    assert refreshed.discovered_route_count == refreshed.directed_route_count == 606
    assert refreshed.cache.known_keys == refreshed.cache.entries == 303
    assert all("NEW/USDT:USDT" in adapter.last_bbo_symbols for adapter in adapters.values())
    assert all(adapter.bbo_subscription_changes == 2 for adapter in adapters.values())


@pytest.mark.asyncio
async def test_restart_recreates_identical_bounded_universe_without_watcher_leaks(
    tmp_path: Path,
) -> None:
    clock = 1_000_000_000
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})

    async def run_once() -> tuple[int, int, int, tuple[tuple[str, str, str], ...]]:
        adapters = {venue: BroadFakeAdapter(venue, clock) for venue in Venue}
        engine = PublicMarketEngine(
            settings,
            adapter_factory=adapters.__getitem__,
            recorder=ParquetMarketRecorder(tmp_path),
            monotonic_ns=lambda: clock,
        )
        result = await engine.scan_broad_bbo(timeout_seconds=1)
        stable_keys = tuple(observation.stable_key for observation in result.prefilter)
        await engine.close()
        await asyncio.sleep(0)
        assert not tuple(
            task
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task()
            and task.get_name().startswith("broad-bbo-")
            and not task.done()
        )
        return (
            result.common_instrument_count,
            result.discovered_route_count,
            result.cache.peak_entries,
            stable_keys,
        )

    assert await run_once() == await run_once()


@pytest.mark.asyncio
async def test_prefilter_latency_measures_bbo_arrival_to_ranking(tmp_path: Path) -> None:
    clock = 1_000_000_000
    adapters = {venue: DelayedBroadFakeAdapter(venue, clock) for venue in Venue}
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock,
    )

    result = await engine.scan_broad_bbo(timeout_seconds=1)
    await engine.close()

    assert Decimal(15) <= result.prefilter_latency_ms <= Decimal(100)


@pytest.mark.asyncio
async def test_prefilter_latency_includes_age_of_quotes_used_for_ranking(tmp_path: Path) -> None:
    clock = [1_000_000_000]
    adapters = {venue: BroadFakeAdapter(venue, clock[0]) for venue in Venue}
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock[0],
    )

    await engine.scan_broad_bbo(timeout_seconds=1)
    clock[0] += 250_000_000
    aged = await engine.scan_broad_bbo(timeout_seconds=1)
    await engine.close()

    assert len(aged.prefilter) == 600
    assert aged.prefilter_latency_ms >= Decimal(250)


@pytest.mark.asyncio
async def test_shadow_universe_excludes_young_and_unknown_age_from_candidate_routes(
    tmp_path: Path,
) -> None:
    clock = 1_000_000_000
    now = datetime(2026, 8, 15, tzinfo=UTC)
    adapters = {venue: BroadFakeAdapter(venue, clock) for venue in Venue}
    for adapter in adapters.values():
        template = adapter.instruments[0]
        adapter.instruments = (
            *adapter.instruments,
            replace(
                template,
                symbol="YOUNG/USDT:USDT",
                exchange_symbol=f"{adapter.venue.value}-YOUNG",
                base="YOUNG",
                listed_at=now - timedelta(days=13),
            ),
            replace(
                template,
                symbol="UNKNOWN/USDT:USDT",
                exchange_symbol=f"{adapter.venue.value}-UNKNOWN",
                base="UNKNOWN",
                listed_at=None,
            ),
        )
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})
    engine = PublicMarketEngine(
        settings,
        adapter_factory=adapters.__getitem__,
        recorder=ParquetMarketRecorder(tmp_path),
        monotonic_ns=lambda: clock,
        now_factory=lambda: now,
    )

    result = await engine.scan_broad_bbo(timeout_seconds=1)
    await engine.close()

    assert result.common_instrument_count == 100
    assert all(observation.base not in {"YOUNG", "UNKNOWN"} for observation in result.prefilter)
