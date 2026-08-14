from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.execution import Side


@dataclass(frozen=True, slots=True)
class PrivateCapabilityReport:
    venue: Venue
    order_stream: bool
    position_stream: bool
    balance_stream: bool
    fetch_balance: bool
    fetch_positions: bool
    submit_order: bool
    cancel_order: bool
    fetch_order: bool
    fetch_fee: bool
    checked_at: datetime
    missing: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return not self.missing


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    venue: Venue
    equity_usdt: Decimal
    free_margin_usdt: Decimal
    margin_mode: str | None
    position_mode: str | None
    trading_enabled: bool | None
    permissions: tuple[str, ...]
    observed_at: datetime

    def __post_init__(self) -> None:
        if self.equity_usdt < 0 or self.free_margin_usdt < 0:
            raise ValueError("account balances must be non-negative")
        if self.free_margin_usdt > self.equity_usdt:
            raise ValueError("free margin cannot exceed equity")


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    venue: Venue
    symbol: str
    side: Side
    base_quantity: Decimal
    entry_price: Decimal | None
    mark_price: Decimal | None
    observed_at: datetime


class PrivateOrderStatus(StrEnum):
    OPEN = "OPEN"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class PrivateOrder:
    venue: Venue
    order_id: str | None
    client_order_id: str
    symbol: str
    side: Side
    status: PrivateOrderStatus
    requested_base_quantity: Decimal
    filled_base_quantity: Decimal
    average_price: Decimal | None
    fee_usdt: Decimal | None
    observed_at: datetime

    def __post_init__(self) -> None:
        if not self.client_order_id.strip() or not self.symbol.strip():
            raise ValueError("order identifiers must be non-empty")
        if self.requested_base_quantity <= 0:
            raise ValueError("requested order quantity must be positive")
        if not 0 <= self.filled_base_quantity <= self.requested_base_quantity:
            raise ValueError("filled quantity is outside requested quantity")
        if self.filled_base_quantity > 0 and (
            self.average_price is None or self.average_price <= 0
        ):
            raise ValueError("filled order requires an average price")
        if (
            self.status == PrivateOrderStatus.FILLED
            and self.filled_base_quantity != self.requested_base_quantity
        ):
            raise ValueError("filled status requires the full requested quantity")
        if self.fee_usdt is not None and self.fee_usdt < 0:
            raise ValueError("order fee must be non-negative")


@dataclass(frozen=True, slots=True)
class VenueOrderRequest:
    venue: Venue
    client_order_id: str
    symbol: str
    side: Side
    order_type: str
    amount_contracts: Decimal
    price: Decimal | None
    time_in_force: str | None
    params: dict[str, object]

    def __post_init__(self) -> None:
        if self.amount_contracts <= 0:
            raise ValueError("venue order amount must be positive")
        if self.order_type == "limit" and (self.price is None or self.price <= 0):
            raise ValueError("limit order requires a positive price")
