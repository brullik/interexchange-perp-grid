from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import ccxt.pro as ccxtpro  # type: ignore[import-untyped]

from interexchange_perp_grid.adapters.base import ExchangeAdapter
from interexchange_perp_grid.adapters.bingx_swap import SequenceQualifiedBingxExchange
from interexchange_perp_grid.adapters.bitget_classic import ClassicBitgetExchange
from interexchange_perp_grid.adapters.bybit_v5 import SequenceQualifiedBybitExchange
from interexchange_perp_grid.adapters.kucoin_classic import ClassicKucoinFuturesExchange
from interexchange_perp_grid.adapters.mexc_swap import SequenceQualifiedMexcExchange
from interexchange_perp_grid.domain import (
    BboQuote,
    BookLevel,
    CapabilityReport,
    FundingSnapshot,
    Instrument,
    OrderBookSnapshot,
    Venue,
)


def _decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return result if result.is_finite() else None


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _supported(value: object) -> bool:
    return value is True or value == "emulated"


def _listing_datetime(market: Mapping[str, Any]) -> datetime | None:
    info = _mapping(market.get("info"))
    candidates = (
        market.get("created"),
        market.get("listingTimestamp"),
        info.get("onboardDate"),
        info.get("launchTime"),
        info.get("listTime"),
    )
    for candidate in candidates:
        timestamp = _decimal(candidate)
        if timestamp is None or timestamp <= 0:
            continue
        milliseconds = timestamp * 1000 if timestamp < Decimal("100000000000") else timestamp
        try:
            return datetime.fromtimestamp(float(milliseconds / 1000), tz=UTC)
        except (OSError, OverflowError, ValueError):
            continue
    return None


def normalize_market(venue: Venue, market: Mapping[str, Any]) -> Instrument | None:
    if not (
        market.get("active") is True
        and market.get("contract") is True
        and market.get("swap") is True
        and market.get("linear") is True
        and market.get("inverse") is False
        and market.get("expiry") is None
        and market.get("settle") == "USDT"
        and market.get("quote") == "USDT"
    ):
        return None
    contract_size = _decimal(market.get("contractSize"))
    precision = _mapping(market.get("precision"))
    limits = _mapping(market.get("limits"))
    info = _mapping(market.get("info"))
    amount_limits = _mapping(limits.get("amount"))
    cost_limits = _mapping(limits.get("cost"))
    amount_step = _decimal(precision.get("amount"))
    price_tick = _decimal(precision.get("price"))
    minimum_amount = _decimal(amount_limits.get("min"))
    if venue == Venue.MEXC and info.get("apiAllowed") is not True:
        return None
    if contract_size is None or contract_size <= 0:
        return None
    if amount_step is None or amount_step <= 0:
        return None
    if price_tick is None or price_tick <= 0:
        return None
    if minimum_amount is None or minimum_amount <= 0:
        return None
    base = market.get("base")
    symbol = market.get("symbol")
    exchange_symbol = market.get("id")
    if not isinstance(base, str) or not base:
        return None
    if not isinstance(symbol, str) or not symbol:
        return None
    if not isinstance(exchange_symbol, str) or not exchange_symbol:
        return None
    if venue == Venue.MEXC and not (
        info.get("apiAllowed") is True
        and str(info.get("state")) == "0"
        and info.get("symbol") == exchange_symbol
        and info.get("baseCoin") == base
        and info.get("quoteCoin") == "USDT"
        and info.get("settleCoin") == "USDT"
        and _decimal(info.get("contractSize")) == contract_size
        and _decimal(info.get("priceUnit")) == price_tick
        and _decimal(info.get("volUnit")) == amount_step
        and _decimal(info.get("minVol")) == minimum_amount
    ):
        return None
    taker_fee = _decimal(market.get("taker"))
    minimum_notional = _decimal(cost_limits.get("min"))
    if minimum_notional is None and venue == Venue.BYBIT:
        minimum_notional = _decimal(_mapping(info.get("lotSizeFilter")).get("minNotionalValue"))
    no_fixed_minimum_notional = minimum_notional is None and (
        (
            venue == Venue.OKX
            and info.get("instType") == "SWAP"
            and info.get("ctType") == "linear"
            and _decimal(info.get("minSz")) == minimum_amount
        )
        or (
            venue == Venue.KUCOIN_FUTURES
            and info.get("status") == "Open"
            and info.get("settleCurrency") == "USDT"
            and _decimal(info.get("lotSize")) == minimum_amount
            and _decimal(info.get("multiplier")) == contract_size
        )
        or (
            venue == Venue.MEXC
            and info.get("apiAllowed") is True
            and str(info.get("state")) == "0"
            and _decimal(info.get("contractSize")) == contract_size
            and _decimal(info.get("volUnit")) == amount_step
            and _decimal(info.get("minVol")) == minimum_amount
        )
    )
    return Instrument(
        venue=venue,
        symbol=symbol,
        exchange_symbol=exchange_symbol,
        base=base,
        quote="USDT",
        settle="USDT",
        contract_size_base=contract_size,
        amount_step_contracts=amount_step,
        price_tick=price_tick,
        minimum_amount_contracts=minimum_amount,
        minimum_notional=minimum_notional,
        taker_fee_rate=taker_fee,
        fee_source="ccxt_market_metadata" if taker_fee is not None else None,
        active=True,
        listed_at=_listing_datetime(market),
        no_fixed_minimum_notional=no_fixed_minimum_notional,
    )


class CcxtProAdapter(ExchangeAdapter):
    def __init__(self, venue: Venue, exchange: Any | None = None) -> None:
        self.venue = venue
        self._exchange: Any = exchange if exchange is not None else self._build_exchange(venue)
        self._clock_skew_ms: int | None = None
        self._instruments: dict[str, Instrument] = {}
        self._bbo_subscription_kind: str | None = None

    @staticmethod
    def _build_exchange(venue: Venue) -> Any:
        configuration: dict[str, object] = {
            "enableRateLimit": True,
            "newUpdates": True,
            "options": {"defaultType": "swap"},
        }
        if venue == Venue.BINANCE_USDM:
            configuration["options"] = {
                "defaultType": "future",
                "defaultSubType": "linear",
            }
            return ccxtpro.binance(configuration)
        if venue == Venue.BYBIT:
            return SequenceQualifiedBybitExchange(configuration)
        if venue == Venue.BITGET:
            return ClassicBitgetExchange(configuration)
        if venue == Venue.KUCOIN_FUTURES:
            return ClassicKucoinFuturesExchange(configuration)
        if venue == Venue.BINGX:
            return SequenceQualifiedBingxExchange(configuration)
        if venue == Venue.MEXC:
            return SequenceQualifiedMexcExchange(configuration)
        exchange_class = getattr(ccxtpro, venue.value)
        return exchange_class(configuration)

    def _has(self, capability: str) -> bool:
        capabilities = _mapping(self._exchange.has)
        return _supported(capabilities.get(capability))

    def _has_concrete_method(self, method_name: str) -> bool:
        method = getattr(self._exchange, method_name, None)
        if not callable(method):
            return False
        for owner in type(self._exchange).__mro__:
            if method_name not in owner.__dict__:
                continue
            return owner.__module__ != "ccxt.async_support.base.exchange"
        return False

    def _bbo_stream_kind(self) -> str | None:
        capabilities = _mapping(self._exchange.has)
        pairs: tuple[tuple[str, str, str, str], ...] = (
            ("bids_asks", "watchBidsAsks", "unWatchBidsAsks", "un_watch_bids_asks"),
            ("tickers", "watchTickers", "unWatchTickers", "un_watch_tickers"),
        )
        if self.venue == Venue.MEXC:
            return None
        if self.venue == Venue.BITGET:
            pairs = (pairs[1],)
        for kind, watch_capability, unwatch_capability, unwatch_method in pairs:
            declared_unwatch = capabilities.get(unwatch_capability)
            if (
                self._has(watch_capability)
                and declared_unwatch is not False
                and self._has_concrete_method(unwatch_method)
            ):
                return kind
        return None

    def _bbo_ticker_params(self) -> dict[str, str]:
        return {"name": "ticker"} if self.venue == Venue.BINANCE_USDM else {}

    async def probe_public_capabilities(self) -> CapabilityReport:
        await self._exchange.load_markets()
        bbo_stream = self._bbo_stream_kind() is not None
        l2_stream = self._has("watchOrderBook")
        funding = self._has("fetchFundingRate") or self._has("fetchFundingRates")
        mark_index = self._has("fetchMarkPrice") or self._has("fetchTicker")
        server_time = self._has("fetchTime")
        missing = tuple(
            name
            for name, available in (
                ("bbo_stream", bbo_stream),
                ("l2_stream", l2_stream),
                ("funding", funding),
                ("mark_index", mark_index),
                ("server_time", server_time),
            )
            if not available
        )
        if server_time:
            started_ms = time.time_ns() // 1_000_000
            exchange_ms = int(await self._exchange.fetch_time())
            finished_ms = time.time_ns() // 1_000_000
            self._clock_skew_ms = exchange_ms - ((started_ms + finished_ms) // 2)
        return CapabilityReport(
            venue=self.venue,
            bbo_stream=bbo_stream,
            l2_stream=l2_stream,
            funding=funding,
            mark_index=mark_index,
            server_time=server_time,
            clock_skew_ms=self._clock_skew_ms,
            checked_at=datetime.now(UTC),
            missing=missing,
        )

    async def discover_instruments(self) -> tuple[Instrument, ...]:
        raw_markets = await self._exchange.load_markets()
        if not isinstance(raw_markets, Mapping):
            raise TypeError("CCXT load_markets must return a mapping")
        instruments = tuple(
            instrument
            for raw_market in raw_markets.values()
            if isinstance(raw_market, Mapping)
            and (instrument := normalize_market(self.venue, raw_market)) is not None
        )
        self._instruments = {instrument.symbol: instrument for instrument in instruments}
        return tuple(sorted(instruments, key=lambda item: item.symbol))

    def _normalise_bbo(self, raw: Mapping[str, Any]) -> BboQuote:
        symbol = raw.get("symbol")
        if not isinstance(symbol, str) or symbol not in self._instruments:
            raise ValueError("BBO symbol was not discovered")
        instrument = self._instruments[symbol]
        bid = _decimal(raw.get("bid"))
        ask = _decimal(raw.get("ask"))
        if bid is None or ask is None or bid <= 0 or ask <= 0:
            raise ValueError(f"invalid BBO for {self.venue}:{symbol}")
        bid_contracts = _decimal(raw.get("bidVolume"))
        ask_contracts = _decimal(raw.get("askVolume"))
        return BboQuote(
            venue=self.venue,
            symbol=symbol,
            bid_price=bid,
            bid_base_quantity=(
                bid_contracts * instrument.contract_size_base if bid_contracts is not None else None
            ),
            ask_price=ask,
            ask_base_quantity=(
                ask_contracts * instrument.contract_size_base if ask_contracts is not None else None
            ),
            exchange_timestamp_ms=int(raw["timestamp"])
            if raw.get("timestamp") is not None
            else None,
            received_at=datetime.now(UTC),
            received_monotonic_ns=time.monotonic_ns(),
            clock_skew_ms=self._clock_skew_ms,
        )

    async def watch_bbo(self, symbols: tuple[str, ...]) -> tuple[BboQuote, ...]:
        if not symbols:
            return ()
        stream_kind = self._bbo_stream_kind()
        if stream_kind == "bids_asks":
            self._bbo_subscription_kind = stream_kind
            raw_result = await self._exchange.watch_bids_asks(list(symbols))
            if not isinstance(raw_result, Mapping):
                raise TypeError("CCXT watch_bids_asks must return a mapping")
            return tuple(
                self._normalise_bbo(raw)
                for raw in raw_result.values()
                if isinstance(raw, Mapping) and raw.get("symbol") in symbols
            )
        if stream_kind == "tickers":
            self._bbo_subscription_kind = stream_kind
            raw_result = await self._exchange.watch_tickers(
                list(symbols),
                self._bbo_ticker_params(),
            )
            if not isinstance(raw_result, Mapping):
                raise TypeError("CCXT watch_tickers must return a mapping")
            return tuple(
                self._normalise_bbo(raw)
                for raw in raw_result.values()
                if isinstance(raw, Mapping) and raw.get("symbol") in symbols
            )
        raise RuntimeError("batch BBO stream capability is required")

    async def unwatch_bbo(self, symbols: tuple[str, ...]) -> None:
        if self._bbo_subscription_kind == "bids_asks":
            await self._exchange.un_watch_bids_asks(list(symbols))
        elif self._bbo_subscription_kind == "tickers":
            await self._exchange.un_watch_tickers(
                list(symbols),
                self._bbo_ticker_params(),
            )
        else:
            raise RuntimeError("broad BBO subscription kind is unknown")
        self._bbo_subscription_kind = None

    async def watch_order_book(self, instrument: Instrument, limit: int = 50) -> OrderBookSnapshot:
        if self.venue == Venue.OKX:
            raw = await self._exchange.watch_order_book(
                instrument.symbol,
                None,
                {"depth": "books"},
            )
        elif self.venue == Venue.BITGET:
            raw = await self._exchange.watch_order_book(instrument.symbol, 15)
        elif self.venue == Venue.KUCOIN_FUTURES:
            raw = await self._exchange.watch_order_book(instrument.symbol, 50)
        else:
            raw = await self._exchange.watch_order_book(instrument.symbol, limit)
        if not isinstance(raw, Mapping):
            raise TypeError("CCXT watch_order_book must return a mapping")
        bids = self._normalise_levels(raw.get("bids"), instrument)[:limit]
        asks = self._normalise_levels(raw.get("asks"), instrument)[:limit]
        nonce = raw.get("nonce")
        sequence = int(nonce) if isinstance(nonce, int) else None
        return OrderBookSnapshot(
            venue=self.venue,
            symbol=instrument.symbol,
            bids=bids,
            asks=asks,
            exchange_timestamp_ms=int(raw["timestamp"])
            if raw.get("timestamp") is not None
            else None,
            received_at=datetime.now(UTC),
            received_monotonic_ns=time.monotonic_ns(),
            sequence_start=sequence,
            sequence_end=sequence,
            is_snapshot=True,
            synchronised=bool(bids and asks),
            clock_skew_ms=self._clock_skew_ms,
            sequence_reset=raw.get("ipegSequenceReset") is True,
            sequence_contiguous=raw.get("ipegSequenceContiguous") is not False,
        )

    async def unwatch_order_book(self, instrument: Instrument, limit: int = 50) -> None:
        if self.venue == Venue.OKX:
            await self._exchange.un_watch_order_book(
                instrument.symbol,
                {"depth": "books"},
            )
        elif self.venue == Venue.BYBIT:
            await self._exchange.un_watch_order_book(
                instrument.symbol,
                {"limit": limit},
            )
        elif self.venue == Venue.BITGET:
            await self._exchange.un_watch_order_book(
                instrument.symbol,
                {"limit": 15},
            )
        elif self.venue == Venue.KUCOIN_FUTURES:
            await self._exchange.un_watch_order_book(
                instrument.symbol,
                {"limit": 50},
            )
        else:
            await self._exchange.un_watch_order_book(instrument.symbol, {})

    @staticmethod
    def _normalise_levels(raw_levels: object, instrument: Instrument) -> tuple[BookLevel, ...]:
        if not isinstance(raw_levels, Sequence):
            return ()
        levels: list[BookLevel] = []
        for raw_level in raw_levels:
            if not isinstance(raw_level, Sequence) or len(raw_level) < 2:
                continue
            price = _decimal(raw_level[0])
            contracts = _decimal(raw_level[1])
            if price is None or contracts is None or price <= 0 or contracts <= 0:
                continue
            levels.append(BookLevel(price, contracts * instrument.contract_size_base))
        return tuple(levels)

    async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot:
        raw = await self._exchange.fetch_funding_rate(instrument.symbol)
        if not isinstance(raw, Mapping):
            raise TypeError("CCXT fetch_funding_rate must return a mapping")
        next_timestamp = raw.get("nextFundingTimestamp") or raw.get("fundingTimestamp")
        mark_price = _decimal(raw.get("markPrice"))
        index_price = _decimal(raw.get("indexPrice"))
        if mark_price is None or index_price is None:
            fallback: Mapping[str, Any] = {}
            if self._has("fetchMarkPrice"):
                raw_mark = await self._exchange.fetch_mark_price(instrument.symbol)
                fallback = raw_mark if isinstance(raw_mark, Mapping) else {}
            elif self._has("fetchTicker"):
                raw_ticker = await self._exchange.fetch_ticker(instrument.symbol)
                fallback = raw_ticker if isinstance(raw_ticker, Mapping) else {}
            if mark_price is None:
                mark_price = _decimal(fallback.get("markPrice"))
            if index_price is None:
                index_price = _decimal(fallback.get("indexPrice"))
        if index_price is None and self._has("fetchIndexOHLCV"):
            raw_index = await self._exchange.fetch_index_ohlcv(
                instrument.symbol,
                "1m",
                None,
                1,
            )
            if isinstance(raw_index, Sequence) and raw_index:
                latest = raw_index[-1]
                if isinstance(latest, Sequence) and len(latest) >= 5:
                    index_price = _decimal(latest[4])
        return FundingSnapshot(
            venue=self.venue,
            symbol=instrument.symbol,
            rate=_decimal(raw.get("fundingRate")),
            next_funding_timestamp_ms=(int(next_timestamp) if next_timestamp is not None else None),
            interval=str(raw["interval"]) if raw.get("interval") is not None else None,
            mark_price=mark_price,
            index_price=index_price,
            exchange_timestamp_ms=int(raw["timestamp"])
            if raw.get("timestamp") is not None
            else None,
        )

    async def close(self) -> None:
        await self._exchange.close()
