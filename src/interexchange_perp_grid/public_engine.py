from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from interexchange_perp_grid.adapters import CcxtProAdapter, ExchangeAdapter
from interexchange_perp_grid.config import Settings
from interexchange_perp_grid.domain import (
    BboQuote,
    CapabilityReport,
    FundingSnapshot,
    Instrument,
    OrderBookSnapshot,
    QuarantineRecord,
    Venue,
)
from interexchange_perp_grid.history import ParquetMarketRecorder
from interexchange_perp_grid.market_data import BookRegistry, DataQualityAssessment
from interexchange_perp_grid.routes import (
    DirectedRouteQuote,
    directed_pairs,
    evaluate_directed_route,
    match_common_instruments,
)

AdapterFactory = Callable[[Venue], ExchangeAdapter]


@dataclass(frozen=True, slots=True)
class ScanResult:
    base: str
    common_instrument_count: int
    bbo: tuple[BboQuote, ...]
    funding: tuple[FundingSnapshot, ...]
    data_quality: tuple[VenueDataQuality, ...]
    quotes: tuple[DirectedRouteQuote, ...]
    capabilities: tuple[CapabilityReport, ...]
    quarantined: tuple[QuarantineRecord, ...]


@dataclass(frozen=True, slots=True)
class VenueDataQuality:
    venue: Venue
    symbol: str
    assessment: DataQualityAssessment


class PublicMarketEngine:
    def __init__(
        self,
        settings: Settings,
        adapter_factory: AdapterFactory | None = None,
        recorder: ParquetMarketRecorder | None = None,
    ) -> None:
        self.settings = settings
        self._adapter_factory = adapter_factory or CcxtProAdapter
        self._recorder = recorder or ParquetMarketRecorder(Path(settings.storage.parquet_dir))
        self._adapters: dict[Venue, ExchangeAdapter] = {}
        self._capabilities: dict[Venue, CapabilityReport] = {}
        self._instruments: dict[Venue, tuple[Instrument, ...]] = {}
        self._quarantined: dict[Venue, QuarantineRecord] = {}
        self._books = BookRegistry()

    async def initialise(self, timeout_seconds: int = 30) -> None:
        configured = tuple(Venue(value) for value in self.settings.venues.wave1_public)
        self._adapters = {venue: self._adapter_factory(venue) for venue in configured}
        await asyncio.gather(
            *(self._initialise_venue_with_timeout(venue, timeout_seconds) for venue in configured)
        )

    async def _initialise_venue_with_timeout(self, venue: Venue, timeout_seconds: int) -> None:
        try:
            await asyncio.wait_for(self._initialise_venue(venue), timeout=timeout_seconds)
        except TimeoutError:
            self._quarantine(venue, f"capability probe timed out after {timeout_seconds}s")

    async def _initialise_venue(self, venue: Venue) -> None:
        adapter = self._adapters[venue]
        try:
            report = await adapter.probe_public_capabilities()
            self._capabilities[venue] = report
            if not report.public_ready:
                self._quarantine(venue, f"missing capabilities: {', '.join(report.missing)}")
                return
            if report.clock_skew_ms is None:
                self._quarantine(venue, "clock skew is unknown")
                return
            if abs(report.clock_skew_ms) > self.settings.market_data.max_clock_skew_ms:
                self._quarantine(
                    venue,
                    f"clock skew {report.clock_skew_ms}ms exceeds policy",
                )
                return
            instruments = await adapter.discover_instruments()
            if not instruments:
                self._quarantine(venue, "no qualified linear USDT perpetual instruments")
                return
            self._instruments[venue] = instruments
        except Exception as error:
            self._quarantine(venue, f"capability probe failed: {type(error).__name__}: {error}")

    def _quarantine(self, venue: Venue, reason: str) -> None:
        self._quarantined[venue] = QuarantineRecord(venue, reason, datetime.now(UTC))
        self._instruments.pop(venue, None)

    async def scan_once(
        self,
        base: str,
        requested_base_quantity: Decimal,
        timeout_seconds: int,
    ) -> ScanResult:
        if not self._adapters:
            await self.initialise(timeout_seconds)
        common = match_common_instruments(self._instruments)
        selected = next(
            (
                item
                for item in common
                if item.key.base == base.upper() and item.key.settle == "USDT"
            ),
            None,
        )
        if selected is None:
            return self._result(base, len(common), (), (), (), ())
        bbo_results = await asyncio.gather(
            *(
                self._sample_bbo(instrument, timeout_seconds)
                for instrument in selected.instruments
                if instrument.venue not in self._quarantined
            )
        )
        bbo = tuple(quote for quote in bbo_results if quote is not None)
        funding_samples = await asyncio.gather(
            *(
                self._sample_funding(instrument, timeout_seconds)
                for instrument in selected.instruments
                if instrument.venue not in self._quarantined
            )
        )
        funding_by_venue = {
            instrument.venue: (instrument, funding)
            for instrument, funding in funding_samples
            if funding is not None and instrument.venue not in self._quarantined
        }
        warmup_samples = await asyncio.gather(
            *(
                self._sample_book(instrument, timeout_seconds)
                for instrument, _ in funding_by_venue.values()
            )
        )
        warmed_instruments = tuple(
            instrument
            for instrument, book in warmup_samples
            if book is not None and instrument.venue not in self._quarantined
        )
        book_samples = await asyncio.gather(
            *(self._sample_book(instrument, timeout_seconds) for instrument in warmed_instruments)
        )
        complete = {
            instrument.venue: (instrument, book, funding_by_venue[instrument.venue][1])
            for instrument, book in book_samples
            if book is not None and instrument.venue in funding_by_venue
        }
        books = tuple(sample[1] for sample in complete.values())
        quality: dict[Venue, DataQualityAssessment] = {}
        for venue, (_, book, _) in complete.items():
            quality[venue] = self._books.accept(
                book,
                max_age_ms=self.settings.market_data.max_l2_age_ms,
                max_clock_skew_ms=self.settings.market_data.max_clock_skew_ms,
            )
            if not quality[venue].accepted:
                self._quarantine(venue, f"L2 quality failed: {quality[venue].reason.value}")
        data_quality = tuple(
            VenueDataQuality(venue, complete[venue][0].symbol, quality[venue])
            for venue in sorted(quality, key=str)
        )
        quotes = tuple(
            evaluate_directed_route(
                long_instrument,
                short_instrument,
                complete[long_instrument.venue][1],
                complete[short_instrument.venue][1],
                complete[long_instrument.venue][2],
                complete[short_instrument.venue][2],
                quality[long_instrument.venue],
                quality[short_instrument.venue],
                requested_base_quantity,
            )
            for long_instrument, short_instrument in directed_pairs(selected)
            if long_instrument.venue in complete and short_instrument.venue in complete
        )
        if books:
            await self._recorder.append_books(books)
        return self._result(
            base,
            len(common),
            bbo,
            tuple(item[2] for item in complete.values()),
            data_quality,
            quotes,
        )

    async def _sample_bbo(self, instrument: Instrument, timeout_seconds: int) -> BboQuote | None:
        adapter = self._adapters[instrument.venue]
        try:
            quotes = await asyncio.wait_for(
                adapter.watch_bbo((instrument.symbol,)),
                timeout=timeout_seconds,
            )
            selected = next((quote for quote in quotes if quote.symbol == instrument.symbol), None)
            if selected is None:
                raise RuntimeError("BBO stream returned no requested symbol")
            return selected
        except Exception as error:
            self._quarantine(
                instrument.venue,
                f"BBO stream failed: {type(error).__name__}: {error}",
            )
            return None

    async def _sample_funding(
        self,
        instrument: Instrument,
        timeout_seconds: int,
    ) -> tuple[Instrument, FundingSnapshot | None]:
        adapter = self._adapters[instrument.venue]
        try:
            funding = await asyncio.wait_for(
                adapter.fetch_funding(instrument),
                timeout=timeout_seconds,
            )
            return instrument, funding
        except Exception as error:
            self._quarantine(
                instrument.venue,
                f"funding data failed: {type(error).__name__}: {error}",
            )
            return instrument, None

    async def _sample_book(
        self,
        instrument: Instrument,
        timeout_seconds: int,
    ) -> tuple[Instrument, OrderBookSnapshot | None]:
        adapter = self._adapters[instrument.venue]
        try:
            book = await asyncio.wait_for(
                adapter.watch_order_book(instrument),
                timeout=timeout_seconds,
            )
            return instrument, book
        except Exception as error:
            self._quarantine(
                instrument.venue,
                f"L2 stream failed: {type(error).__name__}: {error}",
            )
            return instrument, None

    def _result(
        self,
        base: str,
        common_count: int,
        bbo: tuple[BboQuote, ...],
        funding: tuple[FundingSnapshot, ...],
        data_quality: tuple[VenueDataQuality, ...],
        quotes: tuple[DirectedRouteQuote, ...],
    ) -> ScanResult:
        return ScanResult(
            base=base.upper(),
            common_instrument_count=common_count,
            bbo=bbo,
            funding=funding,
            data_quality=data_quality,
            quotes=quotes,
            capabilities=tuple(
                self._capabilities[venue] for venue in sorted(self._capabilities, key=str)
            ),
            quarantined=tuple(
                self._quarantined[venue] for venue in sorted(self._quarantined, key=str)
            ),
        )

    async def close(self) -> None:
        await asyncio.gather(
            *(adapter.close() for adapter in self._adapters.values()),
            return_exceptions=True,
        )
