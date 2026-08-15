from __future__ import annotations

from abc import ABC, abstractmethod

from interexchange_perp_grid.domain import (
    BboQuote,
    CapabilityReport,
    FundingSnapshot,
    Instrument,
    OrderBookSnapshot,
    Venue,
)


class ExchangeAdapter(ABC):
    """Venue boundary; no exchange response object crosses this interface."""

    venue: Venue

    @abstractmethod
    async def probe_public_capabilities(self) -> CapabilityReport:
        raise NotImplementedError

    @abstractmethod
    async def discover_instruments(self) -> tuple[Instrument, ...]:
        raise NotImplementedError

    @abstractmethod
    async def watch_bbo(self, symbols: tuple[str, ...]) -> tuple[BboQuote, ...]:
        raise NotImplementedError

    @abstractmethod
    async def watch_order_book(self, instrument: Instrument, limit: int = 50) -> OrderBookSnapshot:
        raise NotImplementedError

    @abstractmethod
    async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    async def __aenter__(self) -> ExchangeAdapter:
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        await self.close()
