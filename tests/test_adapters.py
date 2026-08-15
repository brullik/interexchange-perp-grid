from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import ClassVar

import pytest

from interexchange_perp_grid.adapters.ccxt_pro import CcxtProAdapter, normalize_market
from interexchange_perp_grid.domain import Instrument, Venue

FIXTURE = Path("tests/fixtures/wave1_markets.json")


def test_wave1_fixtures_accept_only_exact_linear_usdt_perpetuals() -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    qualified = {
        venue: tuple(
            instrument
            for market in payload[venue.value]
            if (instrument := normalize_market(venue, market)) is not None
        )
        for venue in Venue
    }
    assert {venue: len(instruments) for venue, instruments in qualified.items()} == {
        Venue.BINANCE_USDM: 1,
        Venue.BYBIT: 1,
        Venue.OKX: 1,
    }
    assert {instruments[0].key for instruments in qualified.values()} == {
        qualified[Venue.BINANCE_USDM][0].key
    }
    assert (
        qualified[Venue.OKX][0].contract_size_base * qualified[Venue.OKX][0].amount_step_contracts
        == qualified[Venue.OKX][0].base_amount_step
    )


class FundingExchange:
    has: ClassVar[dict[str, object]] = {
        "fetchMarkPrice": True,
        "fetchIndexOHLCV": True,
    }

    async def fetch_funding_rate(self, symbol: str) -> dict[str, object]:
        assert symbol == "BTC/USDT:USDT"
        return {
            "fundingRate": "0.0001",
            "nextFundingTimestamp": None,
            "fundingTimestamp": 1_700_000_100_000,
            "interval": "8h",
            "markPrice": None,
            "indexPrice": None,
            "timestamp": 1_700_000_000_000,
        }

    async def fetch_mark_price(self, symbol: str) -> dict[str, object]:
        assert symbol == "BTC/USDT:USDT"
        return {"markPrice": "100.1", "indexPrice": None}

    async def fetch_index_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: None,
        limit: int,
    ) -> list[list[object]]:
        assert (symbol, timeframe, since, limit) == ("BTC/USDT:USDT", "1m", None, 1)
        return [[1_700_000_000_000, "99", "101", "98", "100.0", "10"]]

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_funding_schedule_falls_back_to_unified_funding_timestamp() -> None:
    adapter = CcxtProAdapter(Venue.BYBIT, exchange=FundingExchange())
    instrument = Instrument(
        venue=Venue.BYBIT,
        symbol="BTC/USDT:USDT",
        exchange_symbol="BTCUSDT",
        base="BTC",
        quote="USDT",
        settle="USDT",
        contract_size_base=Decimal("1"),
        amount_step_contracts=Decimal("0.001"),
        price_tick=Decimal("0.1"),
        minimum_amount_contracts=Decimal("0.001"),
        minimum_notional=None,
        taker_fee_rate=Decimal("0.0006"),
        fee_source="fixture",
    )
    funding = await adapter.fetch_funding(instrument)
    assert funding.next_funding_timestamp_ms == 1_700_000_100_000
    assert funding.mark_price == Decimal("100.1")
    assert funding.index_price == Decimal("100.0")


class SequenceBookExchange:
    async def watch_order_book(self, symbol: str, limit: int) -> dict[str, object]:
        assert (symbol, limit) == ("BTC/USDT:USDT", 50)
        return {
            "bids": [["100", "2"]],
            "asks": [["101", "3"]],
            "nonce": 105,
            "timestamp": 1_700_000_000_000,
            "ipegSequenceReset": False,
            "ipegSequenceContiguous": False,
        }


@pytest.mark.asyncio
async def test_ccxt_book_carries_native_non_contiguous_sequence_evidence() -> None:
    adapter = CcxtProAdapter(Venue.BYBIT, exchange=SequenceBookExchange())
    selected = Instrument(
        Venue.BYBIT,
        "BTC/USDT:USDT",
        "BTCUSDT",
        "BTC",
        "USDT",
        "USDT",
        Decimal("0.001"),
        Decimal("1"),
        Decimal("0.1"),
        Decimal("1"),
        Decimal("5"),
        Decimal("0.0005"),
        "fixture",
    )

    book = await adapter.watch_order_book(selected)

    assert book.sequence_start == book.sequence_end == 105
    assert book.sequence_contiguous is False
    assert book.sequence_reset is False
