from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import ClassVar

import pytest

from interexchange_perp_grid.adapters.bitget_classic import ClassicBitgetExchange
from interexchange_perp_grid.adapters.ccxt_pro import CcxtProAdapter, normalize_market
from interexchange_perp_grid.domain import WAVE1_VENUES, Instrument, Venue
from interexchange_perp_grid.market_data import BookRegistry
from interexchange_perp_grid.market_universe import InstrumentRegistry
from interexchange_perp_grid.reason_codes import ReasonCode

FIXTURE = Path("tests/fixtures/wave1_markets.json")


def test_wave1_fixtures_accept_only_exact_linear_usdt_perpetuals() -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    qualified = {
        venue: tuple(
            instrument
            for market in payload[venue.value]
            if (instrument := normalize_market(venue, market)) is not None
        )
        for venue in WAVE1_VENUES
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
    assert all(
        instrument.active and instrument.listed_at is not None
        for instruments in qualified.values()
        for instrument in instruments
    )
    snapshot = InstrumentRegistry(
        minimum_listing_age_days=14,
        enforce_listing_age=True,
    ).build(
        qualified,
        now=datetime(2026, 8, 15, tzinfo=UTC),
        monotonic_ns=1,
        generation=1,
    )
    assert len(snapshot.common) == 1
    assert len(snapshot.routes) == 6
    assert qualified[Venue.BYBIT][0].minimum_notional == Decimal(5)
    assert qualified[Venue.OKX][0].minimum_notional is None
    assert qualified[Venue.OKX][0].no_fixed_minimum_notional is True


def test_inactive_market_is_rejected_and_listing_time_is_normalised() -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    market = payload[Venue.BYBIT.value][0]
    assert normalize_market(Venue.BYBIT, market) is not None
    market["active"] = False
    assert normalize_market(Venue.BYBIT, market) is None


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


class UnwatchExchange:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def un_watch_order_book(
        self,
        symbol: str,
        params: dict[str, object],
    ) -> None:
        self.calls.append((symbol, params))


class TickerOnlyExchange:
    has: ClassVar[dict[str, object]] = {"watchTicker": True}

    def __init__(self) -> None:
        self.calls = 0

    async def watch_ticker(self, symbol: str) -> dict[str, object]:
        del symbol
        self.calls += 1
        return {}


class PairedBatchTickerExchange:
    has: ClassVar[dict[str, object]] = {
        "watchBidsAsks": True,
        "unWatchBidsAsks": None,
        "watchTickers": True,
        "unWatchTickers": True,
    }

    def __init__(self) -> None:
        self.watch_calls: list[tuple[list[str], dict[str, str]]] = []
        self.unwatch_calls: list[tuple[list[str], dict[str, str]]] = []

    async def watch_tickers(
        self,
        symbols: list[str],
        params: dict[str, str],
    ) -> dict[str, object]:
        self.watch_calls.append((symbols, params))
        return {}

    async def un_watch_tickers(
        self,
        symbols: list[str],
        params: dict[str, str],
    ) -> None:
        self.unwatch_calls.append((symbols, params))


class CapturingClassicBitgetExchange(ClassicBitgetExchange):
    def __init__(self) -> None:
        self.urls = {"api": {"ws": {"public": "wss://classic.example.invalid"}}}
        self.options: dict[str, object] = {}
        self.newUpdates = True
        self.captured: list[tuple[str, list[str], dict[str, object], list[str]]] = []

    async def load_markets(self) -> dict[str, object]:
        return {}

    def market_symbols(
        self,
        symbols: list[str] | None,
        type: str | None = None,
        allow_empty: bool = False,
        same_type_only: bool = False,
    ) -> list[str]:
        del type, allow_empty, same_type_only
        return symbols or []

    def market(self, symbol: str) -> dict[str, str]:
        return {"symbol": symbol, "id": symbol.split("/", 1)[0] + "USDT"}

    def handle_option_and_params(
        self,
        params: dict[str, object],
        method_name: str,
        option_name: str,
        default_value: object = None,
    ) -> tuple[bool, dict[str, object]]:
        del method_name, option_name, default_value
        return False, params

    def get_inst_type(
        self,
        method_name: str,
        market: dict[str, str],
        uta: bool,
        params: dict[str, object],
    ) -> tuple[str, dict[str, object]]:
        del method_name, market, uta
        return "USDT-FUTURES", params

    async def watch_multiple(
        self,
        url: str,
        message_hashes: list[str],
        message: dict[str, object],
        subscription_hashes: list[str],
    ) -> object:
        self.captured.append((url, message_hashes, message, subscription_hashes))
        if message.get("op") == "subscribe":
            symbol = message_hashes[0].removeprefix("ticker:")
            return {"symbol": symbol}
        return True


@pytest.mark.asyncio
async def test_bitget_classic_batch_ticker_unsubscribe_matches_official_frame() -> None:
    exchange = CapturingClassicBitgetExchange()
    symbols = ["BTC/USDT:USDT", "ETH/USDT:USDT"]

    assert await exchange.un_watch_tickers(symbols) == [True]

    hashes = [f"unsubscribe:ticker:{symbol}" for symbol in symbols]
    assert exchange.captured == [
        (
            "wss://classic.example.invalid",
            hashes,
            {
                "op": "unsubscribe",
                "args": [
                    {
                        "instType": "USDT-FUTURES",
                        "channel": "ticker",
                        "instId": "BTCUSDT",
                    },
                    {
                        "instType": "USDT-FUTURES",
                        "channel": "ticker",
                        "instId": "ETHUSDT",
                    },
                ],
            },
            hashes,
        )
    ]


@pytest.mark.asyncio
async def test_bitget_classic_ticker_frames_are_bounded_and_ack_dispatches() -> None:
    exchange = CapturingClassicBitgetExchange()
    symbols = [f"SYMBOL{index:03d}/USDT:USDT" for index in range(100)]

    updates = await exchange.watch_tickers(symbols)

    assert len(exchange.captured) >= 2
    assert len(updates) == len(exchange.captured)
    assert sum(len(item[1]) for item in exchange.captured) == 100
    assert all(
        item[2]["op"] == "subscribe"
        and len(json.dumps(item[2], separators=(",", ":")).encode()) <= 4096
        for item in exchange.captured
    )

    exchange.captured.clear()
    await exchange.un_watch_tickers(symbols)

    assert len(exchange.captured) >= 2
    assert sum(len(item[1]) for item in exchange.captured) == 100
    assert all(
        len(json.dumps(item[2], separators=(",", ":")).encode()) <= 4096
        for item in exchange.captured
    )

    class DispatchExchange(CapturingClassicBitgetExchange):
        def __init__(self) -> None:
            super().__init__()
            self.acks = 0

        def handle_un_subscription_status(self, client: object, message: object) -> object:
            del client
            self.acks += 1
            return message

    dispatch = DispatchExchange()
    dispatch.handle_message(object(), {"op": "unsubscribe", "args": []})
    assert dispatch.acks == 1


@pytest.mark.asyncio
async def test_broad_bbo_rejects_unbounded_per_symbol_ticker_fallback() -> None:
    exchange = TickerOnlyExchange()
    adapter = CcxtProAdapter(Venue.BYBIT, exchange=exchange)

    with pytest.raises(RuntimeError, match="batch BBO stream capability"):
        await adapter.watch_bbo(tuple(f"A{index}/USDT:USDT" for index in range(101)))

    assert exchange.calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("venue", "expected_params"),
    (
        (Venue.BINANCE_USDM, {"name": "ticker"}),
        (Venue.BYBIT, {}),
        (Venue.OKX, {}),
        (Venue.BITGET, {}),
    ),
)
async def test_broad_bbo_uses_only_a_batch_stream_with_matching_unsubscribe(
    venue: Venue,
    expected_params: dict[str, str],
) -> None:
    exchange = PairedBatchTickerExchange()
    adapter = CcxtProAdapter(venue, exchange=exchange)
    symbols = ("BTC/USDT:USDT", "ETH/USDT:USDT")

    assert await adapter.watch_bbo(symbols) == ()
    await adapter.unwatch_bbo(symbols)

    expected_call = (list(symbols), expected_params)
    assert exchange.watch_calls == [expected_call]
    assert exchange.unwatch_calls == [expected_call]


@pytest.mark.parametrize("venue", (*WAVE1_VENUES, Venue.BITGET))
def test_pinned_wave1_broad_bbo_transport_has_matching_unsubscribe(venue: Venue) -> None:
    adapter = CcxtProAdapter(venue)

    assert adapter._bbo_stream_kind() == "tickers"


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("venue", "expected_params"),
    (
        (Venue.BINANCE_USDM, {}),
        (Venue.BYBIT, {"limit": 50}),
        (Venue.OKX, {"depth": "books"}),
        (Venue.BITGET, {"limit": 15}),
    ),
)
async def test_wave1_candidate_l2_unsubscribe_matches_subscription_contract(
    venue: Venue,
    expected_params: dict[str, object],
) -> None:
    exchange = UnwatchExchange()
    adapter = CcxtProAdapter(venue, exchange=exchange)
    instrument = Instrument(
        venue,
        "BTC/USDT:USDT",
        "BTCUSDT",
        "BTC",
        "USDT",
        "USDT",
        Decimal("0.001"),
        Decimal(1),
        Decimal("0.1"),
        Decimal(1),
        Decimal(5),
        Decimal("0.0005"),
        "fixture",
    )

    await adapter.unwatch_order_book(instrument)

    assert exchange.calls == [(instrument.symbol, expected_params)]


class BitgetBookExchange:
    def __init__(self) -> None:
        self.limit: int | None = None

    async def watch_order_book(self, symbol: str, limit: int) -> dict[str, object]:
        assert symbol == "BTC/USDT:USDT"
        self.limit = limit
        return {
            "bids": [["100", "2"]],
            "asks": [["101", "3"]],
            "nonce": 7,
            "timestamp": 1_700_000_000_000,
        }


@pytest.mark.asyncio
async def test_bitget_classic_l2_uses_documented_books15_contract() -> None:
    exchange = BitgetBookExchange()
    adapter = CcxtProAdapter(Venue.BITGET, exchange=exchange)
    selected = Instrument(
        Venue.BITGET,
        "BTC/USDT:USDT",
        "BTCUSDT",
        "BTC",
        "USDT",
        "USDT",
        Decimal("0.001"),
        Decimal(1),
        Decimal("0.1"),
        Decimal(1),
        Decimal(5),
        Decimal("0.0005"),
        "fixture",
    )

    book = await adapter.watch_order_book(selected)

    assert exchange.limit == 15
    assert book.sequence_start == book.sequence_end == 7


def _pinned_bitget_market() -> dict[str, object]:
    return {
        "id": "BTCUSDT",
        "symbol": "BTC/USDT:USDT",
        "base": "BTC",
        "quote": "USDT",
        "settle": "USDT",
        "settleId": "USDT",
        "type": "swap",
        "spot": False,
        "margin": False,
        "swap": True,
        "future": False,
        "option": False,
        "contract": True,
        "linear": True,
        "inverse": False,
        "active": True,
        "precision": {"amount": 1, "price": 0.1},
        "limits": {
            "amount": {"min": 1, "max": None},
            "price": {"min": None, "max": None},
            "cost": {"min": None, "max": None},
            "leverage": {"min": None, "max": None},
        },
        "contractSize": 0.001,
        "info": {},
    }


class ResolveClient:
    def __init__(self) -> None:
        self.value: object = None

    def resolve(self, value: object, message_hash: str) -> None:
        assert message_hash == "orderbook:BTC/USDT:USDT"
        self.value = value


def test_pinned_bitget_handler_carries_raw_books15_sequence() -> None:
    exchange = ClassicBitgetExchange({"newUpdates": True})
    exchange.set_markets([_pinned_bitget_market()])
    client = ResolveClient()

    exchange.handle_order_book(
        client,
        {
            "action": "snapshot",
            "arg": {
                "instType": "USDT-FUTURES",
                "channel": "books15",
                "instId": "BTCUSDT",
            },
            "data": [
                {
                    "asks": [["101", "3"]],
                    "bids": [["100", "2"]],
                    "ts": "1700000000000",
                    "seq": "7",
                    "pseq": "0",
                }
            ],
        },
    )

    assert isinstance(client.value, dict)
    assert client.value["nonce"] == 7
    assert client.value["ipegSequenceReset"] is False


class RegressingBitgetBookExchange:
    def __init__(self) -> None:
        self.sequences = iter((7, 6))

    async def watch_order_book(self, symbol: str, limit: int) -> dict[str, object]:
        assert (symbol, limit) == ("BTC/USDT:USDT", 15)
        return {
            "bids": [["100", "2"]],
            "asks": [["101", "3"]],
            "nonce": next(self.sequences),
            "timestamp": 1_700_000_000_000,
            "ipegSequenceReset": False,
            "ipegSequenceContiguous": True,
        }


@pytest.mark.asyncio
async def test_bitget_books15_sequence_regression_fails_closed() -> None:
    adapter = CcxtProAdapter(Venue.BITGET, exchange=RegressingBitgetBookExchange())
    adapter._clock_skew_ms = 0
    selected = Instrument(
        Venue.BITGET,
        "BTC/USDT:USDT",
        "BTCUSDT",
        "BTC",
        "USDT",
        "USDT",
        Decimal("0.001"),
        Decimal(1),
        Decimal("0.1"),
        Decimal(1),
        Decimal(5),
        Decimal("0.0005"),
        "fixture",
    )
    registry = BookRegistry()

    first = await adapter.watch_order_book(selected)
    second = await adapter.watch_order_book(selected)

    assert registry.accept(first, max_age_ms=1000, max_clock_skew_ms=1000).accepted is True
    rejected = registry.accept(second, max_age_ms=1000, max_clock_skew_ms=1000)
    assert rejected.accepted is False
    assert rejected.reason == ReasonCode.BOOK_SEQUENCE_GAP
