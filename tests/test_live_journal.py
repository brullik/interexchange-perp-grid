from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.client_ids import venue_client_order_id
from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.live_journal import (
    JournalEventQuarantinedError,
    LiveActionState,
    LiveJournalAction,
    LiveOrderJournal,
    request_payload_hash,
)
from interexchange_perp_grid.private_domain import (
    PrivateOrder,
    PrivateOrderStatus,
    VenueOrderRequest,
)
from interexchange_perp_grid.strategy import DirectedRouteKey

_ROUTE = DirectedRouteKey("BTC", Venue.BINANCE_USDM, Venue.OKX)
_QUALIFICATION = "a" * 64


def test_client_order_ids_fit_every_wave1_venue_contract() -> None:
    first = venue_client_order_id("an-internal-action-id-with-no-length-limit", "close", 1)
    second = venue_client_order_id("an-internal-action-id-with-no-length-limit", "close", 2)
    assert first.isalnum()
    assert len(first) <= 32
    assert first.startswith("ipeg")
    assert first != second


def _request(venue: Venue, client_id: str, side: Side) -> VenueOrderRequest:
    return VenueOrderRequest(
        venue=venue,
        client_order_id=client_id,
        symbol="BTC/USDT:USDT",
        side=side,
        order_type="limit",
        amount_contracts=Decimal("1"),
        price=Decimal("100"),
        time_in_force="IOC",
        params={"client": client_id},
    )


async def _prepare(
    journal: LiveOrderJournal,
    pair_id: str = "pair-1",
    long_client_id: str = "pair-1-long",
    short_client_id: str = "pair-1-short",
) -> LiveJournalAction:
    long_request = _request(Venue.BINANCE_USDM, long_client_id, Side.BUY)
    short_request = _request(Venue.OKX, short_client_id, Side.SELL)
    return await journal.prepare(
        pair_id,
        _ROUTE,
        "tranche-1",
        long_request,
        short_request,
        {
            Venue.BINANCE_USDM: Decimal("0.001"),
            Venue.OKX: Decimal("0.001"),
        },
        {
            Venue.BINANCE_USDM: Decimal("100.1"),
            Venue.OKX: Decimal("99.9"),
        },
        {"reservation_id": "risk-1", "projected_stress_usdt": "0.8"},
        _QUALIFICATION,
    )


@pytest.mark.asyncio
async def test_prepare_is_atomic_durable_and_blocks_restart_entry(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    journal = LiveOrderJournal(path)
    await journal.initialise()
    prepared = await _prepare(journal)
    assert prepared.state == LiveActionState.PREPARED
    assert len(prepared.legs) == 2
    assert all(len(leg.request_payload_hash) == 64 for leg in prepared.legs)
    assert all(not leg.submit_attempted for leg in prepared.legs)
    assert prepared.risk_reservation["projected_stress_usdt"] == "0.8"

    restarted = LiveOrderJournal(path)
    await restarted.initialise()
    active = await restarted.active()
    assert active is not None
    assert active.state == LiveActionState.PREPARED
    with pytest.raises(RuntimeError, match="unreconciled live action"):
        await _prepare(
            restarted,
            "pair-2",
            "pair-2-long",
            "pair-2-short",
        )


@pytest.mark.asyncio
async def test_submit_attempt_is_durable_and_client_id_is_never_retried(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    journal = LiveOrderJournal(path)
    await journal.initialise()
    await _prepare(journal)
    await journal.mark_submit_attempted("pair-1", ("pair-1-long", "pair-1-short"))

    restarted = LiveOrderJournal(path)
    action = await restarted.load("pair-1")
    assert action is not None
    assert action.state == LiveActionState.SUBMITTING
    assert all(leg.submit_attempted for leg in action.legs)
    with pytest.raises(RuntimeError, match="only a PREPARED"):
        await restarted.mark_submit_attempted("pair-1", ("pair-1-long", "pair-1-short"))


@pytest.mark.asyncio
async def test_every_transition_survives_restart_and_duplicate_events_are_idempotent(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    journal = LiveOrderJournal(path)
    await journal.initialise()
    await _prepare(journal)
    await journal.mark_submit_attempted("pair-1", ("pair-1-long", "pair-1-short"))
    order = PrivateOrder(
        venue=Venue.BINANCE_USDM,
        order_id="exchange-1",
        client_order_id="pair-1-long",
        symbol="BTC/USDT:USDT",
        side=Side.BUY,
        status=PrivateOrderStatus.PARTIAL,
        requested_base_quantity=Decimal("0.001"),
        filled_base_quantity=Decimal("0.0005"),
        average_price=Decimal("100"),
        fee_usdt=Decimal("0.001"),
        observed_at=datetime.now(UTC),
    )
    assert await journal.record_order_event("pair-1", order, "event-1") is True
    assert await journal.record_order_event("pair-1", order, "event-1") is False
    for state in (
        LiveActionState.ACKNOWLEDGED,
        LiveActionState.PARTIAL,
        LiveActionState.RECOVERING,
        LiveActionState.HEDGED,
        LiveActionState.CLOSING,
        LiveActionState.FLAT,
    ):
        journal = LiveOrderJournal(path)
        loaded = await journal.load("pair-1")
        assert loaded is not None
        await journal.transition(
            "pair-1",
            state,
            {"restart_state": loaded.state.value},
            residual_delta=Decimal("0"),
        )
        restarted = LiveOrderJournal(path)
        observed = await restarted.load("pair-1")
        assert observed is not None
        assert observed.state == state

    loaded = await journal.load("pair-1")
    assert loaded is not None
    long_leg = next(leg for leg in loaded.legs if leg.client_order_id == "pair-1-long")
    assert long_leg.filled_base_quantity == Decimal("0.0005")


@pytest.mark.asyncio
async def test_reusing_a_client_id_after_flat_is_rejected(tmp_path: Path) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    await _prepare(journal)
    await journal.mark_submit_attempted("pair-1", ("pair-1-long", "pair-1-short"))
    await journal.transition("pair-1", LiveActionState.REJECTED)
    await journal.transition("pair-1", LiveActionState.FLAT)
    with pytest.raises(sqlite3.IntegrityError):
        await _prepare(
            journal,
            "pair-2",
            "pair-1-long",
            "pair-2-short",
        )


def test_request_hash_changes_with_exact_payload() -> None:
    first = _request(Venue.BINANCE_USDM, "client-1", Side.BUY)
    second = replace(first, price=Decimal("100.1"))
    assert request_payload_hash(first) != request_payload_hash(second)


def _event(
    *,
    venue: Venue = Venue.BINANCE_USDM,
    symbol: str = "BTC/USDT:USDT",
    side: Side = Side.BUY,
    order_id: str = "exchange-1",
    status: PrivateOrderStatus = PrivateOrderStatus.PARTIAL,
    filled: str = "0.0005",
    observed_at: datetime | None = None,
) -> PrivateOrder:
    quantity = Decimal(filled)
    return PrivateOrder(
        venue=venue,
        order_id=order_id,
        client_order_id="pair-1-long",
        symbol=symbol,
        side=side,
        status=status,
        requested_base_quantity=Decimal("0.001"),
        filled_base_quantity=quantity,
        average_price=Decimal("100") if quantity else None,
        fee_usdt=Decimal("0.001") if quantity else None,
        observed_at=observed_at or datetime.now(UTC),
    )


@pytest.mark.parametrize(
    "order",
    [
        _event(venue=Venue.OKX),
        _event(symbol="ETH/USDT:USDT"),
        _event(side=Side.SELL),
    ],
    ids=["wrong-venue", "wrong-symbol", "wrong-side"],
)
@pytest.mark.asyncio
async def test_journal_identity_mismatch_quarantines_without_mutating_leg(
    tmp_path: Path,
    order: PrivateOrder,
) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    await _prepare(journal)
    await journal.mark_submit_attempted("pair-1", ("pair-1-long", "pair-1-short"))
    with pytest.raises(JournalEventQuarantinedError):
        await journal.record_order_event("pair-1", order, "bad-identity")
    action = await journal.load("pair-1")
    assert action is not None and action.state == LiveActionState.QUARANTINED
    leg = next(item for item in action.legs if item.client_order_id == "pair-1-long")
    assert leg.status is None
    assert leg.filled_base_quantity == 0
    assert leg.order_id is None
    assert await journal.event_watermark() == 1


@pytest.mark.asyncio
async def test_fill_status_and_exchange_order_identity_cannot_regress(tmp_path: Path) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    await _prepare(journal)
    await journal.mark_submit_attempted("pair-1", ("pair-1-long", "pair-1-short"))
    first = _event()
    await journal.record_order_event("pair-1", first, "first")
    conflict = _event(order_id="exchange-2", filled="0.0006")
    with pytest.raises(JournalEventQuarantinedError, match="EXCHANGE_ORDER_ID_CONFLICT"):
        await journal.record_order_event("pair-1", conflict, "conflict")
    action = await journal.load("pair-1")
    assert action is not None and action.state == LiveActionState.QUARANTINED
    leg = next(item for item in action.legs if item.client_order_id == "pair-1-long")
    assert leg.order_id == "exchange-1"
    assert leg.filled_base_quantity == Decimal("0.0005")


@pytest.mark.asyncio
async def test_terminal_status_and_fill_regression_quarantine_action(tmp_path: Path) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    await _prepare(journal)
    await journal.mark_submit_attempted("pair-1", ("pair-1-long", "pair-1-short"))
    filled = _event(status=PrivateOrderStatus.FILLED, filled="0.001")
    await journal.record_order_event("pair-1", filled, "filled")
    regressed = _event(status=PrivateOrderStatus.PARTIAL, filled="0.0005")
    with pytest.raises(JournalEventQuarantinedError) as raised:
        await journal.record_order_event("pair-1", regressed, "regressed")
    assert "FILL_REGRESSION" in str(raised.value)
    assert "STATUS_REGRESSION" in str(raised.value)
