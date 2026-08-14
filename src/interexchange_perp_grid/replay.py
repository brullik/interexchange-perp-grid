from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.execution import (
    ExecutionIntent,
    SimulatedOrderResult,
    SimulatedOrderStatus,
)
from interexchange_perp_grid.reason_codes import ReasonCode


class ReplayEventKind(StrEnum):
    MARKET_DATA = "MARKET_DATA"
    ORDER_RESULT = "ORDER_RESULT"
    DISCONNECT = "DISCONNECT"
    RECONNECT = "RECONNECT"
    PRIVATE_STREAM_STALE = "PRIVATE_STREAM_STALE"


@dataclass(frozen=True, slots=True)
class ReplayEvent:
    at_ms: int
    sequence: int
    kind: ReplayEventKind
    venue: Venue | None
    label: str

    def __post_init__(self) -> None:
        if self.at_ms < 0 or self.sequence < 0:
            raise ValueError("replay time and sequence must be non-negative")
        if not self.label.strip():
            raise ValueError("replay event label must be non-empty")
        if self.kind != ReplayEventKind.MARKET_DATA and self.venue is None:
            raise ValueError("non-market replay events require a venue")


@dataclass(frozen=True, slots=True)
class ReplayObservation:
    delivered_at_ms: int
    event: ReplayEvent
    accepted: bool
    reason: ReasonCode


class DeterministicReplay:
    """Orders same-time events by sequence and injects deterministic transport latency."""

    def __init__(self, latency_ms: int = 0) -> None:
        if latency_ms < 0:
            raise ValueError("latency cannot be negative")
        self._latency_ms = latency_ms

    def run(self, events: tuple[ReplayEvent, ...]) -> tuple[ReplayObservation, ...]:
        disconnected: set[Venue] = set()
        observations: list[ReplayObservation] = []
        for event in sorted(events, key=lambda item: (item.at_ms, item.sequence)):
            if event.kind == ReplayEventKind.DISCONNECT:
                assert event.venue is not None
                disconnected.add(event.venue)
                accepted = False
                reason = ReasonCode.VENUE_OUTAGE
            elif event.kind == ReplayEventKind.RECONNECT:
                assert event.venue is not None
                disconnected.discard(event.venue)
                accepted = True
                reason = ReasonCode.REPLAY_EVENT_APPLIED
            elif event.kind == ReplayEventKind.PRIVATE_STREAM_STALE:
                accepted = False
                reason = ReasonCode.PRIVATE_STREAM_STALE
            elif event.venue is not None and event.venue in disconnected:
                accepted = False
                reason = ReasonCode.VENUE_OUTAGE
            else:
                accepted = True
                reason = ReasonCode.REPLAY_EVENT_APPLIED
            observations.append(
                ReplayObservation(
                    delivered_at_ms=event.at_ms + self._latency_ms,
                    event=event,
                    accepted=accepted,
                    reason=reason,
                )
            )
        return tuple(observations)


@dataclass(frozen=True, slots=True)
class SimulatedFillPlan:
    status: SimulatedOrderStatus
    actual_fill_quantity: Decimal
    fill_price: Decimal | None
    fee_rate: Decimal

    def __post_init__(self) -> None:
        if not self.actual_fill_quantity.is_finite() or self.actual_fill_quantity < 0:
            raise ValueError("planned fill quantity must be non-negative and finite")
        if self.fill_price is not None and (
            not self.fill_price.is_finite() or self.fill_price <= 0
        ):
            raise ValueError("planned fill price must be positive and finite")
        if not self.fee_rate.is_finite() or self.fee_rate < 0:
            raise ValueError("planned fee rate must be non-negative and finite")


class DeterministicOrderSimulator:
    """Scripted venue outcomes with fail-closed disconnects and idempotent submissions."""

    def __init__(self, plans: dict[str, SimulatedFillPlan]) -> None:
        self._plans = dict(plans)
        self._results: dict[str, SimulatedOrderResult] = {}
        self._disconnected: set[Venue] = set()
        self._stale_private: set[Venue] = set()

    def set_disconnected(self, venue: Venue, disconnected: bool) -> None:
        if disconnected:
            self._disconnected.add(venue)
        else:
            self._disconnected.discard(venue)

    def set_private_stale(self, venue: Venue, stale: bool) -> None:
        if stale:
            self._stale_private.add(venue)
        else:
            self._stale_private.discard(venue)

    def execute(self, intent: ExecutionIntent) -> SimulatedOrderResult:
        existing = self._results.get(intent.client_order_id)
        if existing is not None:
            if existing.intent != intent:
                raise ValueError("client order ID was reused with different intent")
            return existing
        if intent.venue in self._disconnected or intent.venue in self._stale_private:
            result = SimulatedOrderResult(
                intent=intent,
                status=SimulatedOrderStatus.UNKNOWN,
                actual_fill_quantity=Decimal(0),
                fill_price=None,
                fee_usdt=Decimal(0),
            )
        else:
            plan = self._plans[intent.client_order_id]
            fee = (
                plan.actual_fill_quantity * plan.fill_price * plan.fee_rate
                if plan.fill_price is not None
                else Decimal(0)
            )
            result = SimulatedOrderResult(
                intent=intent,
                status=plan.status,
                actual_fill_quantity=plan.actual_fill_quantity,
                fill_price=plan.fill_price,
                fee_usdt=fee,
            )
        self._results[intent.client_order_id] = result
        return result

    def reconcile(self, result: SimulatedOrderResult) -> SimulatedOrderResult:
        order_id = result.intent.client_order_id
        existing = self._results.get(order_id)
        if existing is not None and existing.intent != result.intent:
            raise ValueError("reconciliation intent does not match original submission")
        if existing is not None and existing.status != SimulatedOrderStatus.UNKNOWN:
            if existing != result:
                raise ValueError("known order result cannot change during reconciliation")
            return existing
        self._results[order_id] = result
        return result
