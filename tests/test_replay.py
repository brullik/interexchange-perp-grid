from __future__ import annotations

from decimal import Decimal

from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.execution import (
    ExecutionIntent,
    OrderPurpose,
    Side,
    SimulatedOrderResult,
    SimulatedOrderStatus,
)
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.replay import (
    DeterministicOrderSimulator,
    DeterministicReplay,
    ReplayEvent,
    ReplayEventKind,
    SimulatedFillPlan,
)


def test_replay_is_deterministic_with_controllable_latency_and_disconnects() -> None:
    events = (
        ReplayEvent(20, 4, ReplayEventKind.RECONNECT, Venue.BYBIT, "bybit-up"),
        ReplayEvent(10, 2, ReplayEventKind.ORDER_RESULT, Venue.BYBIT, "blocked-fill"),
        ReplayEvent(10, 1, ReplayEventKind.DISCONNECT, Venue.BYBIT, "bybit-down"),
        ReplayEvent(21, 5, ReplayEventKind.ORDER_RESULT, Venue.BYBIT, "accepted-fill"),
        ReplayEvent(15, 3, ReplayEventKind.PRIVATE_STREAM_STALE, Venue.OKX, "okx-stale"),
    )
    replay = DeterministicReplay(latency_ms=7)
    first = replay.run(events)
    second = replay.run(tuple(reversed(events)))
    assert first == second
    assert [observation.delivered_at_ms for observation in first] == [17, 17, 22, 27, 28]
    assert first[0].reason == ReasonCode.VENUE_OUTAGE
    assert first[1].reason == ReasonCode.VENUE_OUTAGE
    assert first[2].reason == ReasonCode.PRIVATE_STREAM_STALE
    assert first[3].accepted is True
    assert first[4].reason == ReasonCode.REPLAY_EVENT_APPLIED


def test_order_simulator_scripts_partial_fill_and_reconciles_unknown_without_duplicate() -> None:
    intent = ExecutionIntent(
        "bybit-open",
        Venue.BYBIT,
        Side.BUY,
        OrderPurpose.NORMAL_OPEN,
        Decimal("1"),
        Decimal("101"),
    )
    simulator = DeterministicOrderSimulator(
        {
            "bybit-open": SimulatedFillPlan(
                SimulatedOrderStatus.PARTIAL,
                Decimal("0.4"),
                Decimal("100"),
                Decimal("0.001"),
            )
        }
    )
    first = simulator.execute(intent)
    retry = simulator.execute(intent)
    assert first is retry
    assert first.actual_fill_quantity == Decimal("0.4")
    assert first.fee_usdt == Decimal("0.0400")

    outage_intent = ExecutionIntent(
        "okx-open",
        Venue.OKX,
        Side.SELL,
        OrderPurpose.NORMAL_OPEN,
        Decimal("1"),
        Decimal("99"),
    )
    simulator.set_disconnected(Venue.OKX, True)
    unknown = simulator.execute(outage_intent)
    assert unknown.status == SimulatedOrderStatus.UNKNOWN
    reconciled = simulator.reconcile(
        SimulatedOrderResult(
            outage_intent,
            SimulatedOrderStatus.FILLED,
            Decimal("1"),
            Decimal("100"),
            Decimal("0.1"),
        )
    )
    assert reconciled.actual_fill_quantity == Decimal("1")
    assert simulator.execute(outage_intent) is reconciled
