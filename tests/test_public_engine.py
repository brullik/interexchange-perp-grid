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
        self.probe_calls = 0
        self.bbo_calls = 0
        self.last_bbo_symbols: tuple[str, ...] = ()
        self.bbo_subscription_changes = 0

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


class DelayedBroadFakeAdapter(BroadFakeAdapter):
    async def watch_bbo(self, symbols: tuple[str, ...]) -> tuple[BboQuote, ...]:
        await asyncio.sleep(0.02)
        return await super().watch_bbo(symbols)


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
async def test_silent_bbo_transport_times_out_without_duplicate_watcher(tmp_path: Path) -> None:
    clock = [1_000_000_000]
    adapters: dict[Venue, BroadFakeAdapter] = {
        venue: BroadFakeAdapter(venue, clock[0]) for venue in Venue
    }
    hanging = CancellationResistantBroadFakeAdapter(Venue.OKX, clock[0])
    adapters[Venue.OKX] = hanging
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
    clock[0] += 1_000_000_000
    await engine.scan_broad_bbo(timeout_seconds=1)

    assert {record.venue for record in failed.quarantined} == {Venue.OKX}
    assert "TimeoutError" in failed.quarantined[0].reason
    assert hanging.bbo_calls == 1
    assert hanging.active_calls == hanging.peak_concurrent_calls == 1

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
