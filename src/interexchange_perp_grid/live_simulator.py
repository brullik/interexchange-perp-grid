from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.execution import OrderPurpose, Side
from interexchange_perp_grid.live_coordinator import CloseReason
from interexchange_perp_grid.private_domain import (
    AccountSnapshot,
    PositionSnapshot,
    PrivateActiveSnapshot,
    PrivateOrder,
    PrivateOrderStatus,
    SnapshotCompleteness,
    VenueOrderRequest,
)


@dataclass(frozen=True, slots=True)
class ScriptedOrderOutcome:
    status: PrivateOrderStatus
    fill_ratio: Decimal = Decimal(0)
    submit_fault: str | None = None
    persist_before_fault: bool = False
    cancel_failure: bool = False
    late_fill_ratio_on_cancel: Decimal = Decimal(0)
    duplicate_private_events: bool = False

    def __post_init__(self) -> None:
        if not Decimal(0) <= self.fill_ratio <= 1:
            raise ValueError("fill ratio must be between zero and one")
        if not Decimal(0) <= self.late_fill_ratio_on_cancel <= 1:
            raise ValueError("late fill ratio must be between zero and one")
        if self.status == PrivateOrderStatus.FILLED and self.fill_ratio != 1:
            raise ValueError("FILLED scripted outcome requires fill_ratio=1")
        if self.status == PrivateOrderStatus.PARTIAL and not 0 < self.fill_ratio < 1:
            raise ValueError("PARTIAL scripted outcome requires a partial fill ratio")
        if self.submit_fault not in {None, "TIMEOUT", "DISCONNECT"}:
            raise ValueError("unsupported submit fault")


class DeterministicPrivateExchange:
    """No-network exchange used for fault injection against the production coordinator."""

    def __init__(
        self,
        venue: Venue,
        instrument: Instrument,
        outcomes: tuple[ScriptedOrderOutcome, ...],
        *,
        equity_usdt: Decimal = Decimal("100"),
        taker_fee_rate: Decimal = Decimal("0.0005"),
    ) -> None:
        self.venue = venue
        self.instrument = instrument
        self._outcomes = deque(outcomes)
        self._equity = equity_usdt
        self._fee = taker_fee_rate
        self._open: dict[str, PrivateOrder] = {}
        self._recent: dict[str, PrivateOrder] = {}
        self._outcome_by_client: dict[str, ScriptedOrderOutcome] = {}
        self._signed_position = Decimal(0)
        self.submit_calls = 0
        self.cancel_calls = 0

    async def submit_order(
        self,
        request: VenueOrderRequest,
        instrument: Instrument,
    ) -> PrivateOrder:
        if instrument != self.instrument or request.venue != self.venue:
            raise ValueError("simulator request does not match venue instrument")
        if not self._outcomes:
            raise RuntimeError(
                f"no scripted outcome for {self.venue.value}:{request.client_order_id}"
            )
        outcome = self._outcomes.popleft()
        self.submit_calls += 1
        order = self._make_order(request, outcome.status, outcome.fill_ratio)
        if outcome.persist_before_fault or outcome.submit_fault is None:
            self._persist(order)
            self._outcome_by_client[request.client_order_id] = outcome
        if outcome.submit_fault == "TIMEOUT":
            raise TimeoutError("scripted timeout before acknowledgement")
        if outcome.submit_fault == "DISCONNECT":
            raise ConnectionError("scripted disconnect after submit")
        return order

    async def watch_orders(self, instrument: Instrument) -> tuple[PrivateOrder, ...]:
        self._require_instrument(instrument)
        orders = tuple((*self._open.values(), *self._recent.values()))
        duplicated: list[PrivateOrder] = []
        for order in orders:
            duplicated.append(order)
            outcome = self._outcome_by_client.get(order.client_order_id)
            if outcome is not None and outcome.duplicate_private_events:
                duplicated.append(order)
        return tuple(duplicated)

    async def find_order_by_client_id(
        self,
        client_order_id: str,
        instrument: Instrument,
    ) -> PrivateOrder | None:
        self._require_instrument(instrument)
        return self._open.get(client_order_id) or self._recent.get(client_order_id)

    async def cancel_order(
        self,
        order_id: str,
        instrument: Instrument,
    ) -> PrivateOrder:
        self._require_instrument(instrument)
        current = next(
            (order for order in self._open.values() if order.order_id == order_id),
            None,
        )
        if current is None:
            existing = next(
                (order for order in self._recent.values() if order.order_id == order_id),
                None,
            )
            if existing is None:
                raise KeyError(order_id)
            return existing
        outcome = self._outcome_by_client[current.client_order_id]
        self.cancel_calls += 1
        if outcome.cancel_failure:
            raise ConnectionError("scripted cancel failure")
        late_total = current.requested_base_quantity * outcome.late_fill_ratio_on_cancel
        late_total = max(late_total, current.filled_base_quantity)
        if late_total > current.filled_base_quantity:
            self._apply_fill(current.side, late_total - current.filled_base_quantity)
        status = PrivateOrderStatus.PARTIAL if late_total > 0 else PrivateOrderStatus.CANCELLED
        cancelled = PrivateOrder(
            venue=current.venue,
            order_id=current.order_id,
            client_order_id=current.client_order_id,
            symbol=current.symbol,
            side=current.side,
            status=status,
            requested_base_quantity=current.requested_base_quantity,
            filled_base_quantity=late_total,
            average_price=(current.average_price or current.limit_price or Decimal("100"))
            if late_total
            else None,
            fee_usdt=(
                late_total
                * (current.average_price or current.limit_price or Decimal("100"))
                * self._fee
            ),
            observed_at=datetime.now(UTC),
            limit_price=current.limit_price,
        )
        self._open.pop(current.client_order_id)
        self._recent[current.client_order_id] = cancelled
        return cancelled

    async def fetch_account(self, instrument: Instrument) -> AccountSnapshot:
        self._require_instrument(instrument)
        margin_used = abs(self._signed_position) * Decimal("100") / Decimal("3")
        return AccountSnapshot(
            venue=self.venue,
            equity_usdt=self._equity,
            free_margin_usdt=max(Decimal(0), self._equity - margin_used),
            margin_mode="cross",
            position_mode="oneway",
            trading_enabled=True,
            permissions=("read", "trade"),
            observed_at=datetime.now(UTC),
            withdrawal_enabled=False,
            transfer_enabled=False,
        )

    async def fetch_all_open_orders(self) -> tuple[PrivateOrder, ...]:
        return tuple(self._open.values())

    async def fetch_closed_orders(self, instrument: Instrument) -> tuple[PrivateOrder, ...]:
        self._require_instrument(instrument)
        return tuple(self._recent.values())

    async def fetch_all_positions(self) -> tuple[PositionSnapshot, ...]:
        if self._signed_position == 0:
            return ()
        return (
            PositionSnapshot(
                venue=self.venue,
                symbol=self.instrument.symbol,
                side=Side.BUY if self._signed_position > 0 else Side.SELL,
                base_quantity=abs(self._signed_position),
                entry_price=Decimal("100"),
                mark_price=Decimal("100"),
                observed_at=datetime.now(UTC),
            ),
        )

    async def fetch_active_snapshot(self) -> PrivateActiveSnapshot:
        open_orders = await self.fetch_all_open_orders()
        positions = await self.fetch_all_positions()
        return PrivateActiveSnapshot(
            venue=self.venue,
            raw_open_order_count=len(open_orders),
            raw_nonzero_position_count=len(positions),
            open_orders=open_orders,
            positions=positions,
            unknown_active_records=(),
            completeness=SnapshotCompleteness.COMPLETE,
            observed_at=datetime.now(UTC),
            request_count=0,
            account_wide=True,
        )

    async def resolve_instrument(self, symbol: str) -> Instrument | None:
        return self.instrument if symbol == self.instrument.symbol else None

    async def list_instruments(self) -> tuple[Instrument, ...]:
        return (self.instrument,)

    async def fetch_positions(
        self,
        instrument: Instrument,
    ) -> tuple[PositionSnapshot, ...]:
        self._require_instrument(instrument)
        return await self.fetch_all_positions()

    async def fetch_trading_fee(self, instrument: Instrument) -> Decimal:
        self._require_instrument(instrument)
        return self._fee

    def _make_order(
        self,
        request: VenueOrderRequest,
        status: PrivateOrderStatus,
        fill_ratio: Decimal,
    ) -> PrivateOrder:
        quantity = request.amount_contracts * self.instrument.contract_size_base
        filled = quantity * fill_ratio
        average = request.price or Decimal("100")
        return PrivateOrder(
            venue=self.venue,
            order_id=f"{self.venue.value}-{self.submit_calls}",
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            side=request.side,
            status=status,
            requested_base_quantity=quantity,
            filled_base_quantity=filled,
            average_price=average if filled else None,
            fee_usdt=filled * average * self._fee if filled else Decimal(0),
            observed_at=datetime.now(UTC),
            limit_price=request.price,
        )

    def _persist(self, order: PrivateOrder) -> None:
        if order.filled_base_quantity:
            self._apply_fill(order.side, order.filled_base_quantity)
        if order.status in {PrivateOrderStatus.OPEN, PrivateOrderStatus.UNKNOWN}:
            self._open[order.client_order_id] = order
        else:
            self._recent[order.client_order_id] = order

    def _apply_fill(self, side: Side, quantity: Decimal) -> None:
        self._signed_position += quantity if side == Side.BUY else -quantity

    def _require_instrument(self, instrument: Instrument) -> None:
        if instrument != self.instrument:
            raise ValueError("simulator instrument mismatch")


class DeterministicCanaryMonitor:
    def __init__(self, reason: CloseReason) -> None:
        self.reason = reason
        self.calls = 0

    async def wait_for_close(self, timeout_seconds: int) -> CloseReason:
        if timeout_seconds <= 0:
            raise ValueError("canary timeout must be positive")
        self.calls += 1
        return self.reason


class StaticProtectionProvider:
    def __init__(self, prices: dict[tuple[Venue, Side], Decimal]) -> None:
        self.prices = prices

    async def price(
        self,
        venue: Venue,
        side: Side,
        quantity: Decimal,
        purpose: OrderPurpose,
    ) -> Decimal:
        del purpose
        if quantity <= 0:
            raise ValueError("protected quantity must be positive")
        return self.prices[(venue, side)]
