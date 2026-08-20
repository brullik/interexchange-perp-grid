from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum


class Venue(StrEnum):
    BINANCE_USDM = "binanceusdm"
    BYBIT = "bybit"
    OKX = "okx"
    BITGET = "bitget"
    KUCOIN_FUTURES = "kucoinfutures"
    BINGX = "bingx"


WAVE1_VENUES = (Venue.BINANCE_USDM, Venue.BYBIT, Venue.OKX)
NO_FIXED_MINIMUM_NOTIONAL_VENUES = frozenset({Venue.OKX, Venue.KUCOIN_FUTURES})


class BookSide(StrEnum):
    BID = "bid"
    ASK = "ask"


class ProductType(StrEnum):
    LINEAR_USDT_PERPETUAL = "linear_usdt_perpetual"


@dataclass(frozen=True, slots=True, order=True)
class InstrumentKey:
    base: str
    settle: str
    quote: str = "USDT"
    product_type: ProductType = ProductType.LINEAR_USDT_PERPETUAL


@dataclass(frozen=True, slots=True)
class Instrument:
    venue: Venue
    symbol: str
    exchange_symbol: str
    base: str
    quote: str
    settle: str
    contract_size_base: Decimal
    amount_step_contracts: Decimal
    price_tick: Decimal
    minimum_amount_contracts: Decimal
    minimum_notional: Decimal | None
    taker_fee_rate: Decimal | None
    fee_source: str | None
    active: bool = True
    listed_at: datetime | None = None
    product_type: ProductType = ProductType.LINEAR_USDT_PERPETUAL
    no_fixed_minimum_notional: bool = False

    def __post_init__(self) -> None:
        if self.listed_at is not None and self.listed_at.tzinfo is None:
            raise ValueError("instrument listing timestamp must be timezone-aware")
        if self.listed_at is not None and self.listed_at.utcoffset() is None:
            raise ValueError("instrument listing timestamp must have a UTC offset")

    @property
    def key(self) -> InstrumentKey:
        return InstrumentKey(
            base=self.base,
            settle=self.settle,
            quote=self.quote,
            product_type=self.product_type,
        )

    @property
    def base_amount_step(self) -> Decimal:
        return self.amount_step_contracts * self.contract_size_base

    @property
    def minimum_base_amount(self) -> Decimal:
        return self.minimum_amount_contracts * self.contract_size_base

    def listing_age_seconds(self, now: datetime) -> Decimal | None:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("listing-age clock must be timezone-aware")
        if self.listed_at is None:
            return None
        return Decimal(str((now.astimezone(UTC) - self.listed_at.astimezone(UTC)).total_seconds()))


@dataclass(frozen=True, slots=True)
class CapabilityReport:
    venue: Venue
    bbo_stream: bool
    l2_stream: bool
    funding: bool
    mark_index: bool
    server_time: bool
    clock_skew_ms: int | None
    checked_at: datetime
    missing: tuple[str, ...]

    @property
    def public_ready(self) -> bool:
        return (
            self.bbo_stream
            and self.l2_stream
            and self.funding
            and self.mark_index
            and self.server_time
        )


@dataclass(frozen=True, slots=True)
class BboQuote:
    venue: Venue
    symbol: str
    bid_price: Decimal
    bid_base_quantity: Decimal | None
    ask_price: Decimal
    ask_base_quantity: Decimal | None
    exchange_timestamp_ms: int | None
    received_at: datetime
    received_monotonic_ns: int
    clock_skew_ms: int | None


@dataclass(frozen=True, slots=True)
class BookLevel:
    price: Decimal
    base_quantity: Decimal

    def __post_init__(self) -> None:
        if self.price <= 0 or self.base_quantity <= 0:
            raise ValueError("book price and quantity must be positive")


@dataclass(frozen=True, slots=True)
class OrderBookSnapshot:
    venue: Venue
    symbol: str
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    exchange_timestamp_ms: int | None
    received_at: datetime
    received_monotonic_ns: int
    sequence_start: int | None
    sequence_end: int | None
    is_snapshot: bool
    synchronised: bool
    clock_skew_ms: int | None
    sequence_reset: bool = False
    sequence_contiguous: bool = True


@dataclass(frozen=True, slots=True)
class FundingSnapshot:
    venue: Venue
    symbol: str
    rate: Decimal | None
    next_funding_timestamp_ms: int | None
    interval: str | None
    mark_price: Decimal | None
    index_price: Decimal | None
    exchange_timestamp_ms: int | None


@dataclass(frozen=True, slots=True)
class CommonInstrument:
    key: InstrumentKey
    instruments: tuple[Instrument, ...]

    def for_venue(self, venue: Venue) -> Instrument:
        for instrument in self.instruments:
            if instrument.venue == venue:
                return instrument
        raise KeyError(venue)


@dataclass(frozen=True, slots=True)
class QuarantineRecord:
    venue: Venue
    reason: str
    observed_at: datetime
