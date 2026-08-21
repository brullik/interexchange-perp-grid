from __future__ import annotations

import asyncio
import os
import sqlite3
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.client_ids import venue_client_order_id
from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.live_journal import (
    JournalEventQuarantinedError,
    JournalLeg,
    LiveActionState,
    LiveJournalAction,
    LiveOrderJournal,
    is_completed_normal_paired_cycle,
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


def test_stage_completion_accepts_only_canonical_normal_open_and_close_cycle() -> None:
    observed = datetime(2026, 8, 21, tzinfo=UTC)

    def leg(role: str, sequence: int, side: Side) -> JournalLeg:
        return JournalLeg(
            venue_client_order_id("cycle-1", role, sequence),
            Venue.BINANCE_USDM if side == Side.BUY else Venue.OKX,
            "BTC/USDT:USDT",
            side,
            "a" * 64,
            Decimal("0.001"),
            Decimal("100"),
            True,
            f"order-{sequence}",
            PrivateOrderStatus.FILLED,
            Decimal("0.001"),
        )

    normal = LiveJournalAction(
        "cycle-1",
        _ROUTE,
        "tranche-1",
        LiveActionState.FLAT,
        {},
        _QUALIFICATION,
        Decimal(0),
        None,
        observed,
        observed,
        (
            leg("long", 0, Side.BUY),
            leg("short", 0, Side.SELL),
            leg("close", 1, Side.SELL),
            leg("close", 2, Side.BUY),
        ),
    )
    assert is_completed_normal_paired_cycle(normal)
    assert not is_completed_normal_paired_cycle(
        replace(normal, recovery_action="EMERGENCY_FLATTEN")
    )
    assert not is_completed_normal_paired_cycle(replace(normal, legs=normal.legs[:2]))


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
    route: DirectedRouteKey = _ROUTE,
) -> LiveJournalAction:
    long_request = _request(route.long_venue, long_client_id, Side.BUY)
    short_request = _request(route.short_venue, short_client_id, Side.SELL)
    return await journal.prepare(
        pair_id,
        route,
        "tranche-1",
        long_request,
        short_request,
        {
            route.long_venue: Decimal("0.001"),
            route.short_venue: Decimal("0.001"),
        },
        {
            route.long_venue: Decimal("100.1"),
            route.short_venue: Decimal("99.9"),
        },
        {"reservation_id": "risk-1", "projected_stress_usdt": "0.8"},
        _QUALIFICATION,
    )


@pytest.mark.asyncio
async def test_prepare_is_atomic_durable_and_blocks_same_base_restart_entry(tmp_path: Path) -> None:
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
    with pytest.raises(RuntimeError, match="lease is already held"):
        await _prepare(
            restarted,
            "pair-2",
            "pair-2-long",
            "pair-2-short",
        )


@pytest.mark.asyncio
async def test_journal_allows_ten_distinct_bases_and_releases_durable_leases(
    tmp_path: Path,
) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    for index in range(10):
        await _prepare(
            journal,
            f"pair-{index}",
            f"pair-{index}-long",
            f"pair-{index}-short",
            DirectedRouteKey(f"A{index:03d}", Venue.BINANCE_USDM, Venue.OKX),
        )
    active = await journal.active_actions()
    assert tuple(action.pair_action_id for action in active) == tuple(
        f"pair-{index}" for index in range(10)
    )
    with pytest.raises(RuntimeError, match="multiple live actions"):
        await journal.active()

    restarted = LiveOrderJournal(journal.path)
    await restarted.initialise()
    assert len(await restarted.active_actions()) == 10
    with pytest.raises(RuntimeError, match="lease is already held"):
        await _prepare(
            restarted,
            "same-base-after-restart",
            "same-base-after-restart-long",
            "same-base-after-restart-short",
            DirectedRouteKey("A000", Venue.BYBIT, Venue.OKX),
        )
    journal = restarted
    with pytest.raises(RuntimeError, match="maximum active live action"):
        await _prepare(
            journal,
            "pair-10",
            "pair-10-long",
            "pair-10-short",
            DirectedRouteKey("A010", Venue.BINANCE_USDM, Venue.OKX),
        )

    await journal.transition("pair-0", LiveActionState.QUARANTINED)
    await journal.transition("pair-0", LiveActionState.FLAT)
    replacement = await _prepare(
        journal,
        "pair-replacement",
        "pair-replacement-long",
        "pair-replacement-short",
        DirectedRouteKey("A000", Venue.BYBIT, Venue.OKX),
    )
    assert replacement.route.base == "A000"


@pytest.mark.asyncio
async def test_initialise_rebuilds_missing_leases_for_legacy_active_action(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    journal = LiveOrderJournal(path)
    await journal.initialise()
    await _prepare(journal)
    with sqlite3.connect(path) as database:
        database.execute("DELETE FROM live_action_leases")

    restarted = LiveOrderJournal(path)
    await restarted.initialise()
    with pytest.raises(RuntimeError, match="lease is already held"):
        await _prepare(
            restarted,
            "pair-conflict",
            "pair-conflict-long",
            "pair-conflict-short",
        )


@pytest.mark.asyncio
async def test_initialise_rejects_more_than_ten_legacy_active_actions(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    journal = LiveOrderJournal(path)
    await journal.initialise()
    for index in range(10):
        await _prepare(
            journal,
            f"pair-{index}",
            f"pair-{index}-long",
            f"pair-{index}-short",
            DirectedRouteKey(f"A{index:03d}", Venue.BINANCE_USDM, Venue.OKX),
        )
    with sqlite3.connect(path) as database:
        database.execute(
            """
            INSERT INTO live_pair_actions (
                pair_action_id, route_base, long_venue, short_venue, tranche_id,
                state, risk_reservation_json, qualification_hash, residual_delta,
                recovery_action, created_at, updated_at
            ) SELECT ?, ?, long_venue, short_venue, tranche_id, state,
                     risk_reservation_json, qualification_hash, residual_delta,
                     recovery_action, created_at, updated_at
              FROM live_pair_actions WHERE pair_action_id = ?
            """,
            ("legacy-over-limit", "A010", "pair-0"),
        )

    restarted = LiveOrderJournal(path)
    with pytest.raises(RuntimeError, match="legacy active live actions exceed"):
        await restarted.initialise()


@pytest.mark.asyncio
async def test_initialise_rejects_legacy_emergency_beside_normal_action(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    journal = LiveOrderJournal(path)
    await journal.initialise()
    await _prepare(
        journal,
        "normal-eth",
        "normal-eth-long",
        "normal-eth-short",
        DirectedRouteKey("ETH", Venue.BINANCE_USDM, Venue.OKX),
    )
    with sqlite3.connect(path) as database:
        database.execute(
            """
            INSERT INTO live_pair_actions (
                pair_action_id, route_base, long_venue, short_venue, tranche_id,
                state, risk_reservation_json, qualification_hash, residual_delta,
                recovery_action, created_at, updated_at
            ) SELECT ?, ?, long_venue, short_venue, tranche_id, state,
                     risk_reservation_json, qualification_hash, residual_delta,
                     'EMERGENCY_FLATTEN', created_at, updated_at
              FROM live_pair_actions WHERE pair_action_id = ?
            """,
            ("legacy-emergency", "BTC", "normal-eth"),
        )
        database.execute("DELETE FROM live_action_leases")

    restarted = LiveOrderJournal(path)
    with pytest.raises(RuntimeError, match="requires exclusive active ownership"):
        await restarted.initialise()


@pytest.mark.asyncio
async def test_concurrent_same_base_prepare_has_exactly_one_durable_winner(tmp_path: Path) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    results = await asyncio.gather(
        _prepare(
            journal,
            "pair-left",
            "pair-left-long",
            "pair-left-short",
            DirectedRouteKey("BTC", Venue.BINANCE_USDM, Venue.OKX),
        ),
        _prepare(
            journal,
            "pair-right",
            "pair-right-long",
            "pair-right-short",
            DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX),
        ),
        return_exceptions=True,
    )
    assert sum(isinstance(result, LiveJournalAction) for result in results) == 1
    assert sum(isinstance(result, RuntimeError) for result in results) == 1
    assert len(await journal.active_actions()) == 1


@pytest.mark.asyncio
async def test_emergency_action_holds_same_durable_route_leases(tmp_path: Path) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    requests = tuple(
        replace(
            _request(venue, f"emergency-{venue.value}", side),
            order_type="market",
            price=None,
            time_in_force=None,
        )
        for venue, side in (
            (Venue.BINANCE_USDM, Side.SELL),
            (Venue.OKX, Side.BUY),
        )
    )
    await journal.prepare_emergency(
        "emergency-pair",
        _ROUTE,
        "emergency-tranche",
        requests,
        {request.client_order_id: Decimal("0.001") for request in requests},
        {"action": "EMERGENCY_FLATTEN"},
        _QUALIFICATION,
    )
    with pytest.raises(RuntimeError, match="lease is already held"):
        await _prepare(
            journal,
            "normal-pair",
            "normal-pair-long",
            "normal-pair-short",
        )
    with pytest.raises(RuntimeError, match="global emergency"):
        await _prepare(
            journal,
            "different-base",
            "different-base-long",
            "different-base-short",
            DirectedRouteKey("ETH", Venue.BINANCE_USDM, Venue.OKX),
        )


@pytest.mark.asyncio
async def test_risk_stage_completion_freeze_blocks_entry_but_not_emergency_flatten(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    journal = LiveOrderJournal(path)
    await journal.initialise()
    with sqlite3.connect(path) as database:
        database.execute(
            "UPDATE live_entry_controls SET risk_stage_completion_frozen = 1 WHERE singleton = 1"
        )
    with pytest.raises(RuntimeError, match="completion freeze"):
        await _prepare(journal)

    requests = tuple(
        replace(
            _request(venue, f"emergency-{venue.value}", side),
            order_type="market",
            price=None,
            time_in_force=None,
        )
        for venue, side in (
            (Venue.BINANCE_USDM, Side.SELL),
            (Venue.OKX, Side.BUY),
        )
    )
    emergency = await journal.prepare_emergency(
        "emergency-pair",
        _ROUTE,
        "emergency-tranche",
        requests,
        {request.client_order_id: Decimal("0.001") for request in requests},
        {"action": "EMERGENCY_FLATTEN"},
        _QUALIFICATION,
    )
    assert emergency.recovery_action == "EMERGENCY_FLATTEN"


@pytest.mark.asyncio
async def test_flat_action_cannot_reactivate_over_replacement_base_lease(tmp_path: Path) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    await _prepare(journal, "old", "old-long", "old-short")
    await journal.transition("old", LiveActionState.QUARANTINED)
    await journal.transition("old", LiveActionState.FLAT)
    await _prepare(journal, "replacement", "replacement-long", "replacement-short")

    with pytest.raises(RuntimeError, match="lease conflict"):
        await journal.transition("old", LiveActionState.QUARANTINED)
    old = await journal.load("old")
    assert old is not None and old.state == LiveActionState.FLAT
    assert tuple(action.pair_action_id for action in await journal.active_actions()) == (
        "replacement",
    )


@pytest.mark.asyncio
async def test_flat_emergency_cannot_reactivate_beside_different_base(tmp_path: Path) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    requests = tuple(
        replace(
            _request(venue, f"emergency-{venue.value}", side),
            order_type="market",
            price=None,
            time_in_force=None,
        )
        for venue, side in (
            (Venue.BINANCE_USDM, Side.SELL),
            (Venue.OKX, Side.BUY),
        )
    )
    await journal.prepare_emergency(
        "emergency-pair",
        _ROUTE,
        "emergency-tranche",
        requests,
        {request.client_order_id: Decimal("0.001") for request in requests},
        {"action": "EMERGENCY_FLATTEN"},
        _QUALIFICATION,
    )
    await journal.transition("emergency-pair", LiveActionState.QUARANTINED)
    await journal.transition("emergency-pair", LiveActionState.FLAT)
    await _prepare(
        journal,
        "normal-eth",
        "normal-eth-long",
        "normal-eth-short",
        DirectedRouteKey("ETH", Venue.BINANCE_USDM, Venue.OKX),
    )

    with pytest.raises(RuntimeError, match="requires exclusive active ownership"):
        await journal.transition("emergency-pair", LiveActionState.QUARANTINED)
    emergency = await journal.load("emergency-pair")
    assert emergency is not None and emergency.state == LiveActionState.FLAT
    assert tuple(action.pair_action_id for action in await journal.active_actions()) == (
        "normal-eth",
    )


@pytest.mark.asyncio
async def test_noncanonical_route_base_is_rejected_before_lease_creation(tmp_path: Path) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    with pytest.raises(ValueError, match="canonical uppercase"):
        await _prepare(
            journal,
            "lowercase",
            "lowercase-long",
            "lowercase-short",
            DirectedRouteKey("btc", Venue.BINANCE_USDM, Venue.OKX),
        )
    assert await journal.active_actions() == ()


@pytest.mark.asyncio
async def test_active_actions_returns_one_consistent_read_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    await _prepare(journal)
    loaded = threading.Event()
    release = threading.Event()
    original = LiveOrderJournal._load_in_transaction

    def pause_after_snapshot_load(
        self: LiveOrderJournal,
        database: sqlite3.Connection,
        pair_action_id: str,
    ) -> LiveJournalAction | None:
        action = original(self, database, pair_action_id)
        loaded.set()
        release.wait(timeout=2)
        return action

    monkeypatch.setattr(LiveOrderJournal, "_load_in_transaction", pause_after_snapshot_load)
    reader = asyncio.create_task(journal.active_actions())
    assert await asyncio.to_thread(loaded.wait, 2)
    await journal.transition("pair-1", LiveActionState.QUARANTINED)
    await journal.transition("pair-1", LiveActionState.FLAT)
    release.set()
    snapshot = await reader

    assert len(snapshot) == 1 and snapshot[0].state == LiveActionState.PREPARED
    assert await journal.active_actions() == ()


@pytest.mark.asyncio
async def test_flat_barrier_commits_multiple_actions_and_releases_leases_atomically(
    tmp_path: Path,
) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    first = await _prepare(journal, "first", "first-long", "first-short")
    second = await _prepare(
        journal,
        "second",
        "second-long",
        "second-short",
        DirectedRouteKey("ETH", Venue.BINANCE_USDM, Venue.OKX),
    )
    await journal.transition(first.pair_action_id, LiveActionState.QUARANTINED)
    await journal.transition(second.pair_action_id, LiveActionState.QUARANTINED)
    watermark = await journal.event_watermark()

    commit = await journal.commit_flat_barrier_many(
        (first.pair_action_id, second.pair_action_id),
        watermark,
        {"exchange_verified": True},
    )

    assert commit.committed is True
    assert tuple(action.state for action in commit.actions) == (
        LiveActionState.FLAT,
        LiveActionState.FLAT,
    )
    assert await journal.active_actions() == ()
    replacements = await asyncio.gather(
        _prepare(journal, "replacement-btc", "replacement-btc-long", "replacement-btc-short"),
        _prepare(
            journal,
            "replacement-eth",
            "replacement-eth-long",
            "replacement-eth-short",
            DirectedRouteKey("ETH", Venue.BINANCE_USDM, Venue.OKX),
        ),
    )
    assert len(replacements) == 2


@pytest.mark.asyncio
async def test_multi_action_flat_barrier_event_race_keeps_every_lease_active(
    tmp_path: Path,
) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    first = await _prepare(journal, "first", "first-long", "first-short")
    second = await _prepare(
        journal,
        "second",
        "second-long",
        "second-short",
        DirectedRouteKey("ETH", Venue.BINANCE_USDM, Venue.OKX),
    )
    await journal.transition(first.pair_action_id, LiveActionState.QUARANTINED)
    await journal.transition(second.pair_action_id, LiveActionState.QUARANTINED)
    expected_watermark = await journal.event_watermark()
    with pytest.raises(JournalEventQuarantinedError):
        await journal.record_order_event(
            first.pair_action_id,
            replace(_event(venue=Venue.OKX), client_order_id="first-long"),
            "late-race",
        )

    commit = await journal.commit_flat_barrier_many(
        (first.pair_action_id, second.pair_action_id),
        expected_watermark,
    )

    assert commit.committed is False
    assert tuple(action.state for action in commit.actions) == (
        LiveActionState.QUARANTINED,
        LiveActionState.QUARANTINED,
    )
    assert len(await journal.active_actions()) == 2


@pytest.mark.asyncio
async def test_multi_action_flat_barrier_rejects_a_new_unobserved_active_action(
    tmp_path: Path,
) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    first = await _prepare(journal, "first", "first-long", "first-short")
    expected_watermark = await journal.event_watermark()
    await _prepare(
        journal,
        "late",
        "late-long",
        "late-short",
        DirectedRouteKey("ETH", Venue.BINANCE_USDM, Venue.OKX),
    )

    commit = await journal.commit_flat_barrier_many(
        (first.pair_action_id,),
        expected_watermark,
    )

    assert commit.committed is False
    assert tuple(action.pair_action_id for action in commit.actions) == ("first", "late")
    assert all(action.state == LiveActionState.QUARANTINED for action in commit.actions)
    assert len(await journal.active_actions()) == 2


@pytest.mark.asyncio
async def test_account_flatten_lease_blocks_a_new_normal_live_action(tmp_path: Path) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    token = await journal.acquire_account_flatten_lease()
    assert token is not None

    with pytest.raises(RuntimeError, match="account-wide flatten is in progress"):
        await _prepare(journal)

    assert await journal.active_actions() == ()
    await journal.release_account_flatten_lease(token)
    created = await _prepare(journal)
    assert created.state == LiveActionState.PREPARED


@pytest.mark.asyncio
async def test_dead_process_account_flatten_lease_can_be_adopted(tmp_path: Path) -> None:
    path = tmp_path / "stale-flatten.sqlite3"
    journal = LiveOrderJournal(path)
    await journal.initialise()
    original = await journal.acquire_account_flatten_lease()
    assert original is not None
    assert await LiveOrderJournal(path).acquire_account_flatten_lease() == original
    with sqlite3.connect(path) as database:
        database.execute(
            "UPDATE live_control_leases SET owner_pid = ? WHERE lease_key = 'ACCOUNT_WIDE_FLATTEN'",
            (2_147_483_647,),
        )

    adopted = await LiveOrderJournal(path).acquire_account_flatten_lease()

    assert adopted is not None and adopted != original
    await journal.release_account_flatten_lease(adopted)


@pytest.mark.asyncio
async def test_reused_pid_with_new_process_identity_can_adopt_lease(tmp_path: Path) -> None:
    path = tmp_path / "pid-reuse.sqlite3"
    journal = LiveOrderJournal(path)
    await journal.initialise()
    original = await journal.acquire_account_flatten_lease()
    assert original is not None
    with sqlite3.connect(path) as database:
        database.execute(
            "UPDATE live_control_leases SET owner_pid = ?, owner_incarnation = ?, "
            "owner_process_identity = ? "
            "WHERE lease_key = 'ACCOUNT_WIDE_FLATTEN'",
            (os.getpid(), "prior-process-with-reused-pid", "prior-process-identity"),
        )

    adopted = await LiveOrderJournal(path).acquire_account_flatten_lease()

    assert adopted is not None and adopted != original
    await journal.release_account_flatten_lease(adopted)


@pytest.mark.asyncio
async def test_legacy_flatten_lease_requires_external_flat_verification(
    tmp_path: Path,
) -> None:
    path = tmp_path / "legacy-owner.sqlite3"
    journal = LiveOrderJournal(path)
    await journal.initialise()
    original = await journal.acquire_account_flatten_lease()
    assert original is not None
    with sqlite3.connect(path) as database:
        database.execute(
            "UPDATE live_control_leases SET owner_pid = 0, owner_incarnation = '', "
            "owner_process_identity = '' "
            "WHERE lease_key = 'ACCOUNT_WIDE_FLATTEN'"
        )

    assert await LiveOrderJournal(path).acquire_account_flatten_lease() is None


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
async def test_completed_actions_since_binds_flat_filled_cycle_to_qualification(
    tmp_path: Path,
) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    started_at = datetime.now(UTC) - timedelta(seconds=1)
    await _prepare(journal)
    await journal.mark_submit_attempted("pair-1", ("pair-1-long", "pair-1-short"))
    for event_id, venue, client_id, side in (
        ("event-long", Venue.BINANCE_USDM, "pair-1-long", Side.BUY),
        ("event-short", Venue.OKX, "pair-1-short", Side.SELL),
    ):
        await journal.record_order_event(
            "pair-1",
            PrivateOrder(
                venue=venue,
                order_id=event_id,
                client_order_id=client_id,
                symbol="BTC/USDT:USDT",
                side=side,
                status=PrivateOrderStatus.FILLED,
                requested_base_quantity=Decimal("0.001"),
                filled_base_quantity=Decimal("0.001"),
                average_price=Decimal("100"),
                fee_usdt=Decimal("0.001"),
                observed_at=datetime.now(UTC),
            ),
            event_id,
        )
    for state in (
        LiveActionState.FILLED,
        LiveActionState.HEDGED,
        LiveActionState.CLOSING,
        LiveActionState.FLAT,
    ):
        await journal.transition("pair-1", state, residual_delta=Decimal("0"))

    completed = await journal.completed_actions_since(started_at, _QUALIFICATION)
    assert tuple(action.pair_action_id for action in completed) == ("pair-1",)
    assert all(leg.status == PrivateOrderStatus.FILLED for leg in completed[0].legs)
    assert await journal.completed_actions_since(started_at, "b" * 64) == ()


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


@pytest.mark.asyncio
async def test_late_event_after_flat_is_audited_without_reactivating_action(tmp_path: Path) -> None:
    journal = LiveOrderJournal(tmp_path / "state.sqlite3")
    await journal.initialise()
    await _prepare(journal)
    await journal.mark_submit_attempted("pair-1", ("pair-1-long", "pair-1-short"))
    await journal.transition("pair-1", LiveActionState.REJECTED)
    await journal.transition("pair-1", LiveActionState.FLAT)

    with pytest.raises(JournalEventQuarantinedError, match="EVENT_AFTER_FLAT"):
        await journal.record_order_event("pair-1", _event(), "late-after-flat")

    action = await journal.load("pair-1")
    assert action is not None and action.state == LiveActionState.FLAT
    assert await journal.active_actions() == ()
    assert await journal.event_watermark() == 1


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
