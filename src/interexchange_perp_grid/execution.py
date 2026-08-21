from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum

from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.risk import RiskDecision
from interexchange_perp_grid.strategy import DirectedRouteKey


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderPurpose(StrEnum):
    NORMAL_OPEN = "NORMAL_OPEN"
    NORMAL_CLOSE = "NORMAL_CLOSE"
    EMERGENCY_HEDGE = "EMERGENCY_HEDGE"
    EMERGENCY_CLOSE = "EMERGENCY_CLOSE"
    LIQUIDATION_PREVENTION = "LIQUIDATION_PREVENTION"


class SimulatedOrderStatus(StrEnum):
    FILLED = "FILLED"
    PARTIAL = "PARTIAL"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"


class PairActionState(StrEnum):
    CREATED = "CREATED"
    PRECHECKED = "PRECHECKED"
    RISK_RESERVED = "RISK_RESERVED"
    ORDERS_SENT = "ORDERS_SENT"
    PARTIALLY_HEDGED = "PARTIALLY_HEDGED"
    HEDGED = "HEDGED"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    UNKNOWN_ORDER = "UNKNOWN_ORDER"
    RECOVERING = "RECOVERING"
    EMERGENCY_HEDGED = "EMERGENCY_HEDGED"
    FORCED_CLOSED = "FORCED_CLOSED"


EMERGENCY_PURPOSES = {
    OrderPurpose.EMERGENCY_HEDGE,
    OrderPurpose.EMERGENCY_CLOSE,
    OrderPurpose.LIQUIDATION_PREVENTION,
}


@dataclass(frozen=True, slots=True)
class ExecutionIntent:
    client_order_id: str
    venue: Venue
    side: Side
    purpose: OrderPurpose
    quantity: Decimal
    worst_acceptable_price: Decimal | None
    unbounded_market: bool = False

    def __post_init__(self) -> None:
        if not self.client_order_id.strip():
            raise ValueError("client order ID must be non-empty")
        if not self.quantity.is_finite() or self.quantity <= 0:
            raise ValueError("execution quantity must be a positive finite decimal")
        if self.worst_acceptable_price is not None and (
            not self.worst_acceptable_price.is_finite() or self.worst_acceptable_price <= 0
        ):
            raise ValueError("worst acceptable price must be positive and finite")
        if self.purpose not in EMERGENCY_PURPOSES and self.worst_acceptable_price is None:
            raise ValueError(ReasonCode.PROTECTED_PRICE_MISSING.value)
        if self.unbounded_market and self.purpose not in EMERGENCY_PURPOSES:
            raise ValueError("unbounded market execution is emergency-only")
        if not self.unbounded_market and self.worst_acceptable_price is None:
            raise ValueError(ReasonCode.PROTECTED_PRICE_MISSING.value)


@dataclass(frozen=True, slots=True)
class SimulatedOrderResult:
    intent: ExecutionIntent
    status: SimulatedOrderStatus
    actual_fill_quantity: Decimal
    fill_price: Decimal | None
    fee_usdt: Decimal

    def __post_init__(self) -> None:
        if (
            not self.actual_fill_quantity.is_finite()
            or self.actual_fill_quantity < 0
            or self.actual_fill_quantity > self.intent.quantity
        ):
            raise ValueError("actual fill quantity is outside the requested quantity")
        if not self.fee_usdt.is_finite() or self.fee_usdt < 0:
            raise ValueError("fee must be a non-negative finite decimal")
        if self.actual_fill_quantity > 0 and (
            self.fill_price is None or not self.fill_price.is_finite() or self.fill_price <= 0
        ):
            raise ValueError("a positive fill requires a positive finite price")
        if self.status in {SimulatedOrderStatus.REJECTED, SimulatedOrderStatus.UNKNOWN} and (
            self.actual_fill_quantity != 0 or self.fill_price is not None
        ):
            raise ValueError("rejected or unknown results cannot claim a fill")
        if self.status == SimulatedOrderStatus.FILLED and (
            self.actual_fill_quantity != self.intent.quantity
        ):
            raise ValueError("filled status requires the full requested quantity")
        if self.actual_fill_quantity > 0 and self.intent.worst_acceptable_price is not None:
            assert self.fill_price is not None
            if self.intent.side == Side.BUY and (
                self.fill_price > self.intent.worst_acceptable_price
            ):
                raise ValueError("buy fill exceeded the protected price cap")
            if self.intent.side == Side.SELL and (
                self.fill_price < self.intent.worst_acceptable_price
            ):
                raise ValueError("sell fill exceeded the protected price floor")


@dataclass(frozen=True, slots=True)
class Fill:
    client_order_id: str
    venue: Venue
    side: Side
    purpose: OrderPurpose
    quantity: Decimal
    price: Decimal
    fee_usdt: Decimal


@dataclass(frozen=True, slots=True)
class PnlBreakdown:
    closed_quantity: Decimal
    long_price_pnl_usdt: Decimal
    short_price_pnl_usdt: Decimal
    gross_price_pnl_usdt: Decimal
    fees_usdt: Decimal
    funding_usdt: Decimal
    net_pnl_usdt: Decimal


@dataclass(slots=True)
class Tranche:
    tranche_id: str
    route: DirectedRouteKey
    requested_quantity: Decimal
    target_close_spread: Decimal
    stop_spread: Decimal
    projected_stress_usdt: Decimal
    state: PairActionState = PairActionState.CREATED
    reason: ReasonCode | None = None
    entry_long_fills: list[Fill] = field(default_factory=list)
    entry_short_fills: list[Fill] = field(default_factory=list)
    close_long_fills: list[Fill] = field(default_factory=list)
    close_short_fills: list[Fill] = field(default_factory=list)
    emergency_fills: list[Fill] = field(default_factory=list)
    funding_usdt: Decimal = Decimal(0)
    processed_order_ids: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        if not self.tranche_id.strip():
            raise ValueError("tranche ID must be non-empty")
        if self.requested_quantity <= 0:
            raise ValueError("tranche quantity must be positive")
        if self.projected_stress_usdt <= 0:
            raise ValueError("tranche stress must be positive")

    @property
    def actual_long_entry_quantity(self) -> Decimal:
        return sum((fill.quantity for fill in self.entry_long_fills), Decimal(0))

    @property
    def actual_short_entry_quantity(self) -> Decimal:
        return sum((fill.quantity for fill in self.entry_short_fills), Decimal(0))

    @property
    def paired_quantity(self) -> Decimal:
        return min(self.actual_long_entry_quantity, self.actual_short_entry_quantity)

    @property
    def signed_residual_quantity(self) -> Decimal:
        emergency_delta = sum(
            (
                fill.quantity if fill.side == Side.BUY else -fill.quantity
                for fill in self.emergency_fills
            ),
            Decimal(0),
        )
        return self.actual_long_entry_quantity - self.actual_short_entry_quantity + emergency_delta

    @property
    def residual_quantity(self) -> Decimal:
        return abs(self.signed_residual_quantity)

    @property
    def closed_quantity(self) -> Decimal:
        long_closed = sum((fill.quantity for fill in self.close_long_fills), Decimal(0))
        short_closed = sum((fill.quantity for fill in self.close_short_fills), Decimal(0))
        return min(long_closed, short_closed, self.paired_quantity)

    @property
    def all_fills(self) -> tuple[Fill, ...]:
        return tuple(
            self.entry_long_fills
            + self.entry_short_fills
            + self.close_long_fills
            + self.close_short_fills
            + self.emergency_fills
        )

    def add_funding(self, signed_amount_usdt: Decimal) -> None:
        if not signed_amount_usdt.is_finite():
            raise ValueError("funding must be finite")
        self.funding_usdt += signed_amount_usdt

    def pnl(self) -> PnlBreakdown:
        quantity = self.closed_quantity
        if quantity <= 0:
            return PnlBreakdown(
                Decimal(0),
                Decimal(0),
                Decimal(0),
                Decimal(0),
                sum((fill.fee_usdt for fill in self.all_fills), Decimal(0)),
                self.funding_usdt,
                self.funding_usdt - sum((fill.fee_usdt for fill in self.all_fills), Decimal(0)),
            )
        entry_long = _weighted_price(self.entry_long_fills, quantity)
        entry_short = _weighted_price(self.entry_short_fills, quantity)
        exit_long = _weighted_price(self.close_long_fills, quantity)
        exit_short = _weighted_price(self.close_short_fills, quantity)
        long_pnl = (exit_long - entry_long) * quantity
        short_pnl = (entry_short - exit_short) * quantity
        gross = long_pnl + short_pnl
        fees = sum((fill.fee_usdt for fill in self.all_fills), Decimal(0))
        return PnlBreakdown(
            closed_quantity=quantity,
            long_price_pnl_usdt=long_pnl,
            short_price_pnl_usdt=short_pnl,
            gross_price_pnl_usdt=gross,
            fees_usdt=fees,
            funding_usdt=self.funding_usdt,
            net_pnl_usdt=gross - fees + self.funding_usdt,
        )


def _weighted_price(fills: list[Fill], quantity: Decimal) -> Decimal:
    remaining = quantity
    notional = Decimal(0)
    for fill in fills:
        consumed = min(remaining, fill.quantity)
        notional += consumed * fill.price
        remaining -= consumed
        if remaining == 0:
            return notional / quantity
    raise ValueError("fills do not cover the requested PnL quantity")


class PairExecutionCoordinator:
    def precheck_and_reserve(self, tranche: Tranche, risk: RiskDecision) -> None:
        self._require_state(tranche, {PairActionState.CREATED})
        tranche.state = PairActionState.PRECHECKED
        tranche.reason = risk.reason
        if risk.accepted:
            tranche.state = PairActionState.RISK_RESERVED

    def submit_open(
        self,
        tranche: Tranche,
        long_result: SimulatedOrderResult,
        short_result: SimulatedOrderResult,
        *,
        mutation_guard: Callable[[], bool] | None = None,
    ) -> bool:
        self._require_state(tranche, {PairActionState.RISK_RESERVED})
        self._validate_open_legs(tranche, long_result, short_result)
        # The shadow decision deadline must be checked at the mutation
        # boundary, not merely by its async caller.  This guard is evaluated
        # after validation and immediately before any state/fill mutation, so
        # a synchronous delay around this method cannot leave an unowned
        # HEDGED tranche after the caller has failed the decision closed.
        if mutation_guard is not None and not mutation_guard():
            return False
        tranche.state = PairActionState.ORDERS_SENT
        self._apply_open_results(tranche, long_result, short_result)
        return True

    def resolve_unknown(
        self,
        tranche: Tranche,
        long_result: SimulatedOrderResult,
        short_result: SimulatedOrderResult,
    ) -> None:
        self._require_state(tranche, {PairActionState.UNKNOWN_ORDER})
        self._validate_open_legs(tranche, long_result, short_result)
        self._apply_open_results(tranche, long_result, short_result)

    def rollback_unpublished_open(self, tranche: Tranche) -> None:
        """Undo a purely simulated open that missed its publication deadline.

        This coordinator has no network side effects; its fills exist only on
        the supplied in-memory tranche.  The shadow caller may therefore
        restore the pre-submit state until it publishes the tranche into the
        durable runtime.  Live coordinators intentionally have no equivalent
        rollback and must reconcile real exchange truth instead.
        """

        self._require_state(
            tranche,
            {
                PairActionState.ORDERS_SENT,
                PairActionState.PARTIALLY_HEDGED,
                PairActionState.HEDGED,
                PairActionState.UNKNOWN_ORDER,
            },
        )
        tranche.entry_long_fills.clear()
        tranche.entry_short_fills.clear()
        tranche.state = PairActionState.RISK_RESERVED
        tranche.reason = ReasonCode.RISK_RESERVED

    def _apply_open_results(
        self,
        tranche: Tranche,
        long_result: SimulatedOrderResult,
        short_result: SimulatedOrderResult,
    ) -> None:
        if SimulatedOrderStatus.UNKNOWN in {long_result.status, short_result.status}:
            tranche.state = PairActionState.UNKNOWN_ORDER
            tranche.reason = ReasonCode.UNKNOWN_ORDER_STATE
            return
        self._record_result(tranche, long_result, tranche.entry_long_fills)
        self._record_result(tranche, short_result, tranche.entry_short_fills)
        if tranche.residual_quantity == 0 and tranche.paired_quantity > 0:
            tranche.state = PairActionState.HEDGED
            tranche.reason = ReasonCode.ORDERS_HEDGED
            return
        tranche.state = PairActionState.PARTIALLY_HEDGED
        tranche.reason = (
            ReasonCode.SECOND_LEG_REJECTED
            if SimulatedOrderStatus.REJECTED in {long_result.status, short_result.status}
            else ReasonCode.PARTIAL_FILL_RECOVERY
        )

    def close(
        self,
        tranche: Tranche,
        long_result: SimulatedOrderResult,
        short_result: SimulatedOrderResult,
    ) -> None:
        self._require_state(tranche, {PairActionState.HEDGED, PairActionState.CLOSING})
        self._validate_close_legs(tranche, long_result, short_result, emergency=False)
        tranche.state = PairActionState.CLOSING
        self._validate_close_quantities(tranche, long_result, short_result)
        self._record_result(tranche, long_result, tranche.close_long_fills)
        self._record_result(tranche, short_result, tranche.close_short_fills)
        if tranche.closed_quantity == tranche.paired_quantity:
            tranche.state = PairActionState.CLOSED
            tranche.reason = ReasonCode.TRANCHE_CLOSED

    def force_close(
        self,
        tranche: Tranche,
        long_result: SimulatedOrderResult,
        short_result: SimulatedOrderResult,
    ) -> None:
        self._require_state(
            tranche,
            {
                PairActionState.HEDGED,
                PairActionState.CLOSING,
                PairActionState.RECOVERING,
                PairActionState.EMERGENCY_HEDGED,
            },
        )
        self._validate_close_legs(tranche, long_result, short_result, emergency=True)
        self._validate_close_quantities(tranche, long_result, short_result)
        self._record_result(tranche, long_result, tranche.close_long_fills)
        self._record_result(tranche, short_result, tranche.close_short_fills)
        if tranche.closed_quantity == tranche.paired_quantity:
            tranche.state = PairActionState.FORCED_CLOSED
            tranche.reason = ReasonCode.FORCED_CLOSED
        else:
            tranche.state = PairActionState.RECOVERING
            tranche.reason = ReasonCode.PARTIAL_FILL_RECOVERY

    def emergency_hedge(
        self,
        tranche: Tranche,
        result: SimulatedOrderResult,
    ) -> None:
        self._require_state(
            tranche,
            {
                PairActionState.PARTIALLY_HEDGED,
                PairActionState.UNKNOWN_ORDER,
                PairActionState.RECOVERING,
            },
        )
        if result.intent.purpose != OrderPurpose.EMERGENCY_HEDGE:
            raise ValueError("recovery requires an emergency hedge intent")
        if result.intent.venue in {tranche.route.long_venue, tranche.route.short_venue}:
            raise ValueError("emergency hedge must use a third venue")
        residual = tranche.signed_residual_quantity
        required_side = Side.SELL if residual > 0 else Side.BUY
        if residual == 0 or result.intent.side != required_side:
            raise ValueError("emergency hedge side does not neutralise actual residual")
        self._record_result(tranche, result, tranche.emergency_fills)
        if result.actual_fill_quantity != abs(residual):
            tranche.state = PairActionState.RECOVERING
            tranche.reason = ReasonCode.PARTIAL_FILL_RECOVERY
            return
        tranche.state = PairActionState.EMERGENCY_HEDGED
        tranche.reason = ReasonCode.EMERGENCY_HEDGED

    def mark_private_stream_stale(self, tranche: Tranche) -> None:
        self._mark_recovering(tranche, ReasonCode.PRIVATE_STREAM_STALE)

    def mark_venue_outage(self, tranche: Tranche) -> None:
        self._mark_recovering(tranche, ReasonCode.VENUE_OUTAGE)

    @staticmethod
    def _mark_recovering(tranche: Tranche, reason: ReasonCode) -> None:
        if tranche.state in {PairActionState.CLOSED, PairActionState.FORCED_CLOSED}:
            raise ValueError("closed tranche cannot enter recovery")
        tranche.state = PairActionState.RECOVERING
        tranche.reason = reason

    @staticmethod
    def _validate_open_legs(
        tranche: Tranche,
        long_result: SimulatedOrderResult,
        short_result: SimulatedOrderResult,
    ) -> None:
        if (
            long_result.intent.venue != tranche.route.long_venue
            or long_result.intent.side != Side.BUY
            or long_result.intent.purpose != OrderPurpose.NORMAL_OPEN
            or short_result.intent.venue != tranche.route.short_venue
            or short_result.intent.side != Side.SELL
            or short_result.intent.purpose != OrderPurpose.NORMAL_OPEN
        ):
            raise ValueError("open results do not match the directed route")

    @staticmethod
    def _validate_close_legs(
        tranche: Tranche,
        long_result: SimulatedOrderResult,
        short_result: SimulatedOrderResult,
        *,
        emergency: bool,
    ) -> None:
        allowed = (
            {OrderPurpose.EMERGENCY_CLOSE, OrderPurpose.LIQUIDATION_PREVENTION}
            if emergency
            else {OrderPurpose.NORMAL_CLOSE}
        )
        if (
            long_result.intent.venue != tranche.route.long_venue
            or long_result.intent.side != Side.SELL
            or long_result.intent.purpose not in allowed
            or short_result.intent.venue != tranche.route.short_venue
            or short_result.intent.side != Side.BUY
            or short_result.intent.purpose not in allowed
        ):
            raise ValueError("close results do not match the directed route")

    @staticmethod
    def _validate_close_quantities(
        tranche: Tranche,
        long_result: SimulatedOrderResult,
        short_result: SimulatedOrderResult,
    ) -> None:
        long_closed = sum((fill.quantity for fill in tranche.close_long_fills), Decimal(0))
        short_closed = sum((fill.quantity for fill in tranche.close_short_fills), Decimal(0))
        if (
            long_closed + long_result.actual_fill_quantity > tranche.paired_quantity
            or short_closed + short_result.actual_fill_quantity > tranche.paired_quantity
        ):
            raise ValueError("close fills cannot exceed the actual paired quantity")

    @staticmethod
    def _record_result(
        tranche: Tranche,
        result: SimulatedOrderResult,
        destination: list[Fill],
    ) -> None:
        order_id = result.intent.client_order_id
        if order_id in tranche.processed_order_ids:
            return
        tranche.processed_order_ids.add(order_id)
        if result.actual_fill_quantity == 0:
            return
        assert result.fill_price is not None
        destination.append(
            Fill(
                client_order_id=order_id,
                venue=result.intent.venue,
                side=result.intent.side,
                purpose=result.intent.purpose,
                quantity=result.actual_fill_quantity,
                price=result.fill_price,
                fee_usdt=result.fee_usdt,
            )
        )

    @staticmethod
    def _require_state(tranche: Tranche, allowed: set[PairActionState]) -> None:
        if tranche.state not in allowed:
            allowed_values = ", ".join(sorted(state.value for state in allowed))
            raise ValueError(f"state {tranche.state.value} is not one of {allowed_values}")
