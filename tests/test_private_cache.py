from __future__ import annotations

import asyncio
import time
from collections.abc import Iterable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.private_cache import (
    CachedPrivateStateAdapter,
    PrivateCachePolicy,
    PrivateCacheStatus,
    PrivateStateCache,
    Wave1PrivateStateSupervisor,
)
from interexchange_perp_grid.private_domain import (
    AccountSnapshot,
    PositionSnapshot,
    PrivateActiveSnapshot,
    PrivateOrder,
    PrivateOrderStatus,
    PrivateStreamEvent,
    PrivateStreamKind,
    SnapshotCompleteness,
    UnknownActiveRecord,
)
from interexchange_perp_grid.state import initialise_state, read_private_event_watermark


def _snapshot(
    watermark: int,
    *,
    complete: bool = True,
    latency_ms: str = "10",
    request_count: int = 4,
    observed_at: datetime | None = None,
    venue: Venue = Venue.BYBIT,
    source_monotonic_ns: int | None = None,
) -> PrivateActiveSnapshot:
    unknown = () if complete else (UnknownActiveRecord(venue, "POSITION", "MALFORMED", {}),)
    return PrivateActiveSnapshot(
        venue=venue,
        raw_open_order_count=0,
        raw_nonzero_position_count=0,
        open_orders=(),
        positions=(),
        unknown_active_records=unknown,
        completeness=(SnapshotCompleteness.COMPLETE if complete else SnapshotCompleteness.UNKNOWN),
        observed_at=observed_at or datetime.now(UTC),
        event_watermark=watermark,
        request_count=request_count,
        latency_ms=Decimal(latency_ms),
        account_wide=True,
        source_monotonic_ns=source_monotonic_ns,
    )


def _instrument(venue: Venue = Venue.BYBIT) -> Instrument:
    return Instrument(
        venue,
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


class ScriptedSnapshotAdapter:
    def __init__(self, snapshots: Iterable[PrivateActiveSnapshot]) -> None:
        self._snapshots = iter(snapshots)
        self.calls = 0
        self.seeded_watermark: int | None = None

    def seed_private_event_watermark(self, watermark: int) -> None:
        self.seeded_watermark = watermark

    def acknowledge_private_event(self, watermark: int) -> None:
        del watermark

    async def fetch_active_snapshot(self) -> PrivateActiveSnapshot:
        self.calls += 1
        return next(self._snapshots)

    async def watch_account_wide_orders(self) -> PrivateStreamEvent:
        raise AssertionError("stream watcher is not configured for this test")

    async def watch_account_wide_positions(self) -> PrivateStreamEvent:
        raise AssertionError("stream watcher is not configured for this test")

    async def watch_account_wide_balance(self) -> PrivateStreamEvent:
        raise AssertionError("stream watcher is not configured for this test")


class CancellationResistantStreamAdapter(ScriptedSnapshotAdapter):
    def __init__(self, snapshots: Iterable[PrivateActiveSnapshot]) -> None:
        super().__init__(snapshots)
        self.started = asyncio.Event()

    async def watch_account_wide_orders(self) -> PrivateStreamEvent:
        self.started.set()
        try:
            await asyncio.Future[None]()
            raise AssertionError("unreachable stream watcher completion")
        except asyncio.CancelledError:
            await asyncio.sleep(0.2)
            raise


class CachedDelegate(ScriptedSnapshotAdapter):
    def __init__(self, snapshots: Iterable[PrivateActiveSnapshot]) -> None:
        super().__init__(snapshots)
        self.account_calls = 0

    async def fetch_account(self, instrument: Instrument) -> AccountSnapshot:
        self.account_calls += 1
        return AccountSnapshot(
            instrument.venue,
            Decimal("200"),
            Decimal("150"),
            "cross",
            "oneway",
            True,
            ("trade",),
            datetime.now(UTC),
        )

    async def fetch_closed_orders(self, instrument: Instrument) -> tuple[PrivateOrder, ...]:
        del instrument
        return ()

    async def fetch_trading_fee(self, instrument: Instrument) -> Decimal:
        del instrument
        return Decimal("0.0005")


@pytest.mark.asyncio
async def test_startup_requires_complete_account_wide_snapshot_within_request_budget() -> None:
    adapter = ScriptedSnapshotAdapter((_snapshot(0),))
    cache = PrivateStateCache(adapter)

    before = await cache.view()
    after = await cache.startup()

    assert before.status == PrivateCacheStatus.UNKNOWN
    assert before.reason == "PRIVATE_CACHE_NOT_INITIALISED"
    assert after.ready is True
    assert after.snapshot is not None
    assert after.snapshot.request_count == 4
    assert after.source == "REST:STARTUP"
    assert adapter.calls == 1


@pytest.mark.asyncio
async def test_incomplete_raw_record_and_request_budget_fail_closed() -> None:
    incomplete = PrivateStateCache(ScriptedSnapshotAdapter((_snapshot(0, complete=False),)))
    over_budget = PrivateStateCache(ScriptedSnapshotAdapter((_snapshot(0, request_count=5),)))

    incomplete_view = await incomplete.startup()
    budget_view = await over_budget.startup()

    assert incomplete_view.status == PrivateCacheStatus.UNKNOWN
    assert incomplete_view.reason == "PRIVATE_SNAPSHOT_INCOMPLETE"
    assert budget_view.status == PrivateCacheStatus.UNKNOWN
    assert budget_view.reason == "PRIVATE_REQUEST_BUDGET_EXCEEDED"


@pytest.mark.asyncio
async def test_cache_staleness_blocks_entry_after_two_seconds() -> None:
    clock = [1_000_000_000]
    cache = PrivateStateCache(
        ScriptedSnapshotAdapter((_snapshot(0),)),
        monotonic_ns=lambda: clock[0],
    )
    assert (await cache.startup()).ready is True

    clock[0] += 2_000_000_001
    stale = await cache.view()

    assert stale.status == PrivateCacheStatus.UNKNOWN
    assert stale.reason == "PRIVATE_CACHE_STALE"


@pytest.mark.asyncio
async def test_stream_watermark_and_timestamp_regressions_poison_cache() -> None:
    observed = datetime.now(UTC)
    cache = PrivateStateCache(
        ScriptedSnapshotAdapter(
            (
                _snapshot(5, observed_at=observed),
                _snapshot(6, observed_at=observed + timedelta(milliseconds=1)),
            )
        )
    )
    assert (await cache.startup()).ready is True

    repeated = await cache.ingest_stream_snapshot(_snapshot(5, observed_at=observed))
    assert repeated.reason == "PRIVATE_EVENT_WATERMARK_NOT_INCREASING"

    recovered = await cache.reconcile("AFTER_SEQUENCE_GAP")
    assert recovered.ready is True
    assert recovered.snapshot is not None
    assert recovered.snapshot.event_watermark == 6


@pytest.mark.asyncio
async def test_delayed_rest_reconciliation_cannot_regress_stream_watermark() -> None:
    observed = datetime.now(UTC)
    cache = PrivateStateCache(
        ScriptedSnapshotAdapter(
            (
                _snapshot(5, observed_at=observed),
                _snapshot(4, observed_at=observed + timedelta(milliseconds=2)),
            )
        )
    )
    assert (await cache.startup()).ready is True
    assert (
        await cache.ingest_stream_snapshot(
            _snapshot(
                6,
                request_count=0,
                observed_at=observed + timedelta(milliseconds=1),
                source_monotonic_ns=time.monotonic_ns(),
            )
        )
    ).ready is True

    delayed = await cache.reconcile("DELAYED_RESPONSE")

    assert delayed.reason == "PRIVATE_EVENT_WATERMARK_REGRESSION"
    assert delayed.snapshot is not None
    assert delayed.snapshot.event_watermark == 6


@pytest.mark.asyncio
async def test_equal_watermark_rest_conflict_cannot_overwrite_stream_state() -> None:
    observed = datetime.now(UTC)
    delayed_position = PositionSnapshot(
        Venue.BYBIT,
        "BTC/USDT:USDT",
        Side.BUY,
        Decimal("0.001"),
        Decimal("100"),
        Decimal("101"),
        observed + timedelta(milliseconds=2),
    )
    delayed_rest = replace(
        _snapshot(6, observed_at=observed + timedelta(milliseconds=2)),
        raw_nonzero_position_count=1,
        positions=(delayed_position,),
    )
    cache = PrivateStateCache(
        ScriptedSnapshotAdapter(
            (
                _snapshot(5, observed_at=observed),
                delayed_rest,
            )
        )
    )
    assert (await cache.startup()).ready is True
    streamed = await cache.ingest_stream_snapshot(
        _snapshot(
            6,
            request_count=0,
            observed_at=observed + timedelta(milliseconds=1),
            source_monotonic_ns=time.monotonic_ns(),
        )
    )
    assert streamed.ready is True

    conflicted = await cache.reconcile("DELAYED_EQUAL_WATERMARK")

    assert conflicted.reason == "PRIVATE_REST_CONFLICT_AT_STREAM_WATERMARK"
    assert conflicted.snapshot is not None
    assert conflicted.snapshot.positions == ()


@pytest.mark.asyncio
async def test_stream_snapshot_with_new_watermark_updates_latest_value() -> None:
    observed = datetime.now(UTC)
    cache = PrivateStateCache(ScriptedSnapshotAdapter((_snapshot(1, observed_at=observed),)))
    assert (await cache.startup()).ready is True

    updated = await cache.ingest_stream_snapshot(
        _snapshot(
            2,
            request_count=0,
            observed_at=observed + timedelta(milliseconds=1),
            source_monotonic_ns=time.monotonic_ns(),
        )
    )

    assert updated.ready is True
    assert updated.cache_watermark == 2
    assert updated.snapshot is not None
    assert updated.snapshot.event_watermark == 2
    assert updated.source == "PRIVATE_STREAM"


@pytest.mark.asyncio
async def test_account_wide_stream_events_merge_and_remove_latest_values() -> None:
    observed = datetime.now(UTC)
    cache = PrivateStateCache(ScriptedSnapshotAdapter((_snapshot(0, observed_at=observed),)))
    assert (await cache.startup()).ready is True
    open_order = PrivateOrder(
        Venue.BYBIT,
        "order-stream-1",
        "client-stream-1",
        "BTC/USDT:USDT",
        Side.BUY,
        PrivateOrderStatus.OPEN,
        Decimal("0.001"),
        Decimal(0),
        None,
        None,
        observed + timedelta(microseconds=1),
    )
    position = PositionSnapshot(
        Venue.BYBIT,
        "BTC/USDT:USDT",
        Side.BUY,
        Decimal("0.001"),
        Decimal("100"),
        Decimal("101"),
        observed + timedelta(microseconds=2),
    )
    account = AccountSnapshot(
        Venue.BYBIT,
        Decimal("100"),
        Decimal("80"),
        "cross",
        "oneway",
        True,
        ("trade",),
        observed + timedelta(microseconds=3),
    )

    orders_view = await cache.ingest_stream_event(
        PrivateStreamEvent(
            Venue.BYBIT,
            PrivateStreamKind.ORDERS,
            1,
            observed + timedelta(microseconds=1),
            time.monotonic_ns(),
            orders=(open_order,),
        )
    )
    positions_view = await cache.ingest_stream_event(
        PrivateStreamEvent(
            Venue.BYBIT,
            PrivateStreamKind.POSITIONS,
            2,
            observed + timedelta(microseconds=2),
            time.monotonic_ns(),
            positions=(position,),
        )
    )
    account_view = await cache.ingest_stream_event(
        PrivateStreamEvent(
            Venue.BYBIT,
            PrivateStreamKind.ACCOUNT,
            3,
            observed + timedelta(microseconds=3),
            time.monotonic_ns(),
            account=account,
        )
    )

    assert orders_view.ready is True
    assert positions_view.ready is True
    assert account_view.ready is True
    assert account_view.snapshot is not None
    assert account_view.snapshot.open_orders == (open_order,)
    assert account_view.snapshot.positions == (position,)
    assert await cache.account_snapshot() == account

    cancelled = replace(
        open_order,
        status=PrivateOrderStatus.CANCELLED,
        observed_at=observed + timedelta(microseconds=4),
    )
    flat_position = replace(
        position,
        base_quantity=Decimal(0),
        observed_at=observed + timedelta(microseconds=5),
    )
    await cache.ingest_stream_event(
        PrivateStreamEvent(
            Venue.BYBIT,
            PrivateStreamKind.ORDERS,
            4,
            observed + timedelta(microseconds=4),
            time.monotonic_ns(),
            orders=(cancelled,),
        )
    )
    flat = await cache.ingest_stream_event(
        PrivateStreamEvent(
            Venue.BYBIT,
            PrivateStreamKind.POSITIONS,
            5,
            observed + timedelta(microseconds=5),
            time.monotonic_ns(),
            positions=(flat_position,),
        )
    )

    assert flat.ready is True
    assert flat.snapshot is not None
    assert flat.snapshot.open_orders == ()
    assert flat.snapshot.positions == ()
    assert flat.event_p95_latency_ms is not None


@pytest.mark.asyncio
async def test_unknown_account_wide_stream_event_poison_cache_without_dropping_state() -> None:
    observed = datetime.now(UTC)
    cache = PrivateStateCache(ScriptedSnapshotAdapter((_snapshot(0, observed_at=observed),)))
    assert (await cache.startup()).ready is True

    unknown = await cache.ingest_stream_event(
        PrivateStreamEvent(
            Venue.BYBIT,
            PrivateStreamKind.POSITIONS,
            1,
            observed + timedelta(microseconds=1),
            time.monotonic_ns(),
            unknown_active_records=(
                UnknownActiveRecord(Venue.BYBIT, "POSITION", "UNKNOWN_SYMBOL", {}),
            ),
        )
    )

    assert unknown.status == PrivateCacheStatus.UNKNOWN
    assert unknown.reason == "PRIVATE_STREAM_EVENT_INCOMPLETE"
    assert unknown.snapshot is not None
    assert unknown.snapshot.event_watermark == 0

    unrelated = await cache.ingest_stream_event(
        PrivateStreamEvent(
            Venue.BYBIT,
            PrivateStreamKind.ORDERS,
            2,
            observed + timedelta(microseconds=2),
            time.monotonic_ns(),
        )
    )

    assert unrelated.status == PrivateCacheStatus.UNKNOWN
    assert unrelated.reason == "PRIVATE_STREAM_EVENT_INCOMPLETE"


@pytest.mark.asyncio
async def test_delivered_event_completes_same_watermark_pending_rest_snapshot() -> None:
    observed = datetime.now(UTC)
    pending_snapshot = replace(
        _snapshot(1, observed_at=observed + timedelta(microseconds=2)),
        completeness=SnapshotCompleteness.UNKNOWN,
        unknown_active_records=(
            UnknownActiveRecord(
                Venue.BYBIT,
                "SNAPSHOT",
                "PRIVATE_EVENT_DELIVERY_PENDING",
                {},
            ),
        ),
    )
    cache = PrivateStateCache(
        ScriptedSnapshotAdapter(
            (
                _snapshot(0, observed_at=observed),
                pending_snapshot,
            )
        )
    )
    assert (await cache.startup()).ready is True
    pending = await cache.reconcile("PRE_SUBMIT")
    assert pending.status == PrivateCacheStatus.UNKNOWN
    assert pending.reason == "PRIVATE_EVENT_DELIVERY_PENDING"
    assert pending.snapshot is not None
    assert pending.snapshot.event_watermark == 0
    order = PrivateOrder(
        Venue.BYBIT,
        "order-pending-1",
        "client-pending-1",
        "BTC/USDT:USDT",
        Side.BUY,
        PrivateOrderStatus.OPEN,
        Decimal("0.001"),
        Decimal(0),
        None,
        None,
        observed + timedelta(microseconds=1),
    )

    delivered = await cache.ingest_stream_event(
        PrivateStreamEvent(
            Venue.BYBIT,
            PrivateStreamKind.ORDERS,
            1,
            observed + timedelta(microseconds=1),
            time.monotonic_ns(),
            orders=(order,),
        )
    )

    assert delivered.ready is True
    assert delivered.snapshot is not None
    assert delivered.snapshot.open_orders == (order,)


@pytest.mark.asyncio
async def test_out_of_order_stream_events_are_buffered_until_gap_closes() -> None:
    observed = datetime.now(UTC)
    source_ns = 1_000_000
    cache = PrivateStateCache(
        ScriptedSnapshotAdapter((_snapshot(0, observed_at=observed),)),
        monotonic_ns=lambda: source_ns + 100,
    )
    assert (await cache.startup()).ready is True
    first_order = PrivateOrder(
        Venue.BYBIT,
        "order-1",
        "client-1",
        "BTC/USDT:USDT",
        Side.BUY,
        PrivateOrderStatus.OPEN,
        Decimal("0.001"),
        Decimal(0),
        None,
        None,
        observed + timedelta(microseconds=1),
    )
    second_order = replace(
        first_order,
        order_id="order-2",
        client_order_id="client-2",
        observed_at=observed + timedelta(microseconds=2),
    )

    gap = await cache.ingest_stream_event(
        PrivateStreamEvent(
            Venue.BYBIT,
            PrivateStreamKind.ORDERS,
            2,
            observed + timedelta(microseconds=2),
            source_ns + 2,
            orders=(second_order,),
        )
    )
    assert gap.status == PrivateCacheStatus.UNKNOWN
    assert gap.reason == "PRIVATE_EVENT_DELIVERY_OUT_OF_ORDER"
    assert gap.snapshot is not None
    assert gap.snapshot.event_watermark == 0

    drained = await cache.ingest_stream_event(
        PrivateStreamEvent(
            Venue.BYBIT,
            PrivateStreamKind.ORDERS,
            1,
            observed + timedelta(microseconds=1),
            source_ns + 1,
            orders=(first_order,),
        )
    )

    assert drained.ready is True
    assert drained.snapshot is not None
    assert drained.snapshot.event_watermark == 2
    assert drained.snapshot.open_orders == (first_order, second_order)


@pytest.mark.asyncio
async def test_rest_recovery_prunes_buffered_events_at_authoritative_watermark() -> None:
    observed = datetime.now(UTC)
    cache = PrivateStateCache(
        ScriptedSnapshotAdapter(
            (
                _snapshot(0, observed_at=observed),
                _snapshot(3, observed_at=observed + timedelta(microseconds=3)),
            )
        ),
        monotonic_ns=lambda: 1_000_000,
    )
    assert (await cache.startup()).ready is True
    for watermark in (2, 3):
        await cache.ingest_stream_event(
            PrivateStreamEvent(
                Venue.BYBIT,
                PrivateStreamKind.ORDERS,
                watermark,
                observed + timedelta(microseconds=watermark),
                watermark,
            )
        )

    recovered = await cache.reconcile("AFTER_STREAM_GAP")

    assert recovered.status == PrivateCacheStatus.UNKNOWN
    assert recovered.reason == "PRIVATE_EVENT_DELIVERY_OUT_OF_ORDER"
    assert cache._pending_stream_events == {}
    confirmed = await cache.ingest_stream_event(
        PrivateStreamEvent(
            Venue.BYBIT,
            PrivateStreamKind.ORDERS,
            4,
            observed + timedelta(microseconds=4),
            4,
        )
    )
    assert confirmed.ready is True


@pytest.mark.asyncio
async def test_stream_reorder_buffer_is_bounded_and_requires_rest_recovery_after_overflow() -> None:
    observed = datetime.now(UTC)
    cache = PrivateStateCache(
        ScriptedSnapshotAdapter(
            (
                _snapshot(0, observed_at=observed),
                _snapshot(4, observed_at=observed + timedelta(microseconds=4)),
            )
        ),
        PrivateCachePolicy(maximum_pending_stream_events=2),
        monotonic_ns=lambda: 1_000_000,
    )
    assert (await cache.startup()).ready is True

    for watermark in (2, 3):
        await cache.ingest_stream_event(
            PrivateStreamEvent(
                Venue.BYBIT,
                PrivateStreamKind.ORDERS,
                watermark,
                observed + timedelta(microseconds=watermark),
                watermark,
            )
        )
    overflow = await cache.ingest_stream_event(
        PrivateStreamEvent(
            Venue.BYBIT,
            PrivateStreamKind.ORDERS,
            4,
            observed + timedelta(microseconds=4),
            4,
        )
    )
    assert overflow.reason == "PRIVATE_EVENT_BUFFER_LIMIT_EXCEEDED"
    assert len(cache._pending_stream_events) == 2

    drained = await cache.ingest_stream_event(
        PrivateStreamEvent(
            Venue.BYBIT,
            PrivateStreamKind.ORDERS,
            1,
            observed + timedelta(microseconds=1),
            1,
        )
    )
    assert drained.reason == "PRIVATE_EVENT_BUFFER_LIMIT_EXCEEDED"
    assert cache._pending_stream_events == {}

    recovered = await cache.reconcile("AFTER_BUFFER_OVERFLOW")
    assert recovered.ready is True


@pytest.mark.asyncio
async def test_account_events_do_not_mask_stale_order_and_position_channels() -> None:
    observed = datetime.now(UTC)
    clock = [1_000_000_000]
    cache = PrivateStateCache(
        ScriptedSnapshotAdapter((_snapshot(0, observed_at=observed),)),
        monotonic_ns=lambda: clock[0],
    )
    assert (await cache.startup()).ready is True
    clock[0] += 1_500_000_000
    account = AccountSnapshot(
        Venue.BYBIT,
        Decimal("100"),
        Decimal("80"),
        "cross",
        "oneway",
        True,
        ("trade",),
        observed + timedelta(seconds=1),
    )

    refreshed_account = await cache.ingest_stream_event(
        PrivateStreamEvent(
            Venue.BYBIT,
            PrivateStreamKind.ACCOUNT,
            1,
            observed + timedelta(seconds=1),
            clock[0],
            account=account,
        )
    )
    assert refreshed_account.ready is True
    clock[0] += 500_000_001

    stale = await cache.view()

    assert stale.status == PrivateCacheStatus.UNKNOWN
    assert stale.reason == "PRIVATE_CACHE_STALE"


@pytest.mark.asyncio
async def test_cached_adapter_consumes_only_fresh_account_stream_state() -> None:
    observed = datetime.now(UTC)
    clock = [1_000_000_000]
    delegate = CachedDelegate((_snapshot(0, observed_at=observed),))
    cache = PrivateStateCache(delegate, monotonic_ns=lambda: clock[0])
    cached = CachedPrivateStateAdapter(delegate, cache)
    assert (await cache.startup()).ready is True
    streamed_account = AccountSnapshot(
        Venue.BYBIT,
        Decimal("100"),
        Decimal("80"),
        "cross",
        "oneway",
        True,
        ("trade",),
        observed + timedelta(microseconds=1),
    )
    await cache.ingest_stream_event(
        PrivateStreamEvent(
            Venue.BYBIT,
            PrivateStreamKind.ACCOUNT,
            1,
            observed + timedelta(microseconds=1),
            clock[0],
            account=streamed_account,
        )
    )

    assert await cached.fetch_account(_instrument()) == streamed_account
    assert delegate.account_calls == 0
    clock[0] += 2_000_000_001

    fallback = await cached.fetch_account(_instrument())

    assert fallback.equity_usdt == Decimal("200")
    assert delegate.account_calls == 1


@pytest.mark.asyncio
async def test_consumer_reconciliations_coalesce_and_obey_per_minute_budget() -> None:
    observed = datetime.now(UTC)
    clock = [1_000_000_000]
    adapter = ScriptedSnapshotAdapter(
        (
            _snapshot(0, observed_at=observed),
            _snapshot(0, observed_at=observed + timedelta(seconds=3)),
            _snapshot(0, observed_at=observed + timedelta(seconds=4)),
            _snapshot(0, observed_at=observed + timedelta(seconds=70)),
        )
    )
    cache = PrivateStateCache(
        adapter,
        PrivateCachePolicy(maximum_rest_requests_per_minute=8),
        monotonic_ns=lambda: clock[0],
    )
    assert (await cache.startup()).ready is True
    clock[0] += 2_000_000_001

    first, coalesced = await asyncio.gather(
        cache.reconcile("CONSUMER_FAIL_CLOSED_REFRESH:first"),
        cache.reconcile("CONSUMER_FAIL_CLOSED_REFRESH:second"),
    )

    assert first.ready is True
    assert coalesced.ready is True
    assert adapter.calls == 2
    clock[0] += 2_000_000_001
    limited = await cache.reconcile("CONSUMER_FAIL_CLOSED_REFRESH:third")
    assert limited.status == PrivateCacheStatus.UNKNOWN
    assert limited.reason == "PRIVATE_REST_RATE_BUDGET_EXCEEDED"
    assert adapter.calls == 2

    recovery = await cache.reconcile("PRE_CLOSE:bybit")
    assert recovery.ready is True
    assert adapter.calls == 3

    clock[0] += 60_000_000_001
    recovered = await cache.reconcile("CONSUMER_FAIL_CLOSED_REFRESH:after_window")

    assert recovered.ready is True
    assert adapter.calls == 4


@pytest.mark.asyncio
async def test_reconciliation_p95_budget_and_periodic_refresh_are_bounded() -> None:
    snapshots = tuple(_snapshot(index, latency_ms="4") for index in range(10))
    adapter = ScriptedSnapshotAdapter(snapshots)
    cache = PrivateStateCache(
        adapter,
        PrivateCachePolicy(
            maximum_age_seconds=Decimal(2),
            reconciliation_interval_seconds=Decimal("0.001"),
            maximum_rest_requests=4,
            maximum_p95_latency_ms=Decimal(5),
        ),
    )
    assert (await cache.startup()).ready is True
    stop_event = asyncio.Event()
    task = asyncio.create_task(cache.run_periodic(stop_event))
    while adapter.calls < 3:
        await asyncio.sleep(0.001)
    stop_event.set()
    await task

    view = await cache.view()
    assert view.ready is True
    assert view.p95_latency_ms == Decimal(4)
    assert 3 <= adapter.calls <= 4


@pytest.mark.asyncio
async def test_stream_shutdown_does_not_wait_for_cancellation_resistant_watcher() -> None:
    adapter = CancellationResistantStreamAdapter((_snapshot(0),))
    cache = PrivateStateCache(
        adapter,
        PrivateCachePolicy(stream_shutdown_timeout_seconds=Decimal("0.01")),
    )
    assert (await cache.startup()).ready is True
    stop_event = asyncio.Event()
    task = asyncio.create_task(cache.run_stream(PrivateStreamKind.ORDERS, stop_event))
    await adapter.started.wait()

    started = time.perf_counter()
    stop_event.set()
    await asyncio.wait_for(task, timeout=0.15)
    elapsed = time.perf_counter() - started

    assert elapsed < 0.1
    await asyncio.sleep(0.21)


@pytest.mark.asyncio
async def test_new_process_has_no_qualified_cache_until_startup_reconciliation() -> None:
    first = PrivateStateCache(ScriptedSnapshotAdapter((_snapshot(3),)))
    assert (await first.startup()).ready is True

    restarted = PrivateStateCache(ScriptedSnapshotAdapter((_snapshot(4),)))

    assert (await restarted.view()).reason == "PRIVATE_CACHE_NOT_INITIALISED"
    assert (await restarted.startup()).ready is True


@pytest.mark.asyncio
async def test_synthetic_private_event_load_stays_within_p95_and_memory_window() -> None:
    observed = datetime.now(UTC)
    cache = PrivateStateCache(ScriptedSnapshotAdapter((_snapshot(0, observed_at=observed),)))
    assert (await cache.startup()).ready is True

    for watermark in range(1, 1001):
        source_ns = time.monotonic_ns()
        await asyncio.sleep(0)
        view = await cache.ingest_stream_snapshot(
            _snapshot(
                watermark,
                request_count=0,
                observed_at=observed + timedelta(microseconds=watermark),
                source_monotonic_ns=source_ns,
            )
        )

    assert view.ready is True
    assert view.cache_watermark == 1001
    assert view.event_p95_latency_ms is not None
    assert view.event_p95_latency_ms <= Decimal(250)


@pytest.mark.asyncio
async def test_private_event_p95_violation_fails_closed() -> None:
    observed = datetime.now(UTC)
    clock = [1_000_000_000]
    cache = PrivateStateCache(
        ScriptedSnapshotAdapter((_snapshot(0, observed_at=observed),)),
        monotonic_ns=lambda: clock[0],
    )
    assert (await cache.startup()).ready is True
    source_ns = clock[0]
    clock[0] += 251_000_000

    overloaded = await cache.ingest_stream_snapshot(
        _snapshot(
            1,
            request_count=0,
            observed_at=observed + timedelta(microseconds=1),
            source_monotonic_ns=source_ns,
        )
    )

    assert overloaded.status == PrivateCacheStatus.UNKNOWN
    assert overloaded.reason == "PRIVATE_EVENT_P95_EXCEEDED"


@pytest.mark.asyncio
async def test_private_stream_without_measurable_source_time_fails_closed() -> None:
    observed = datetime.now(UTC)
    cache = PrivateStateCache(ScriptedSnapshotAdapter((_snapshot(0, observed_at=observed),)))
    assert (await cache.startup()).ready is True

    unknown = await cache.ingest_stream_snapshot(
        _snapshot(1, request_count=0, observed_at=observed + timedelta(microseconds=1))
    )

    assert unknown.status == PrivateCacheStatus.UNKNOWN
    assert unknown.reason == "PRIVATE_EVENT_LATENCY_UNKNOWN"


class FailingSnapshotAdapter(ScriptedSnapshotAdapter):
    def __init__(self) -> None:
        super().__init__(())

    async def fetch_active_snapshot(self) -> PrivateActiveSnapshot:
        raise TimeoutError("synthetic private timeout")


class HangingSnapshotAdapter(ScriptedSnapshotAdapter):
    def __init__(self) -> None:
        super().__init__(())

    async def fetch_active_snapshot(self) -> PrivateActiveSnapshot:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


@pytest.mark.asyncio
async def test_hung_reconciliation_has_hard_deadline_and_fails_closed() -> None:
    cache = PrivateStateCache(
        HangingSnapshotAdapter(),
        PrivateCachePolicy(reconciliation_timeout_seconds=Decimal("0.001")),
    )

    view = await asyncio.wait_for(cache.startup(), timeout=1)

    assert view.status == PrivateCacheStatus.UNKNOWN
    assert view.reason == "REST_RECONCILIATION_FAILED:TimeoutError"


@pytest.mark.asyncio
async def test_wave1_supervisor_isolates_venue_failure() -> None:
    binance = ScriptedSnapshotAdapter((_snapshot(0, venue=Venue.BINANCE_USDM),))
    okx = ScriptedSnapshotAdapter((_snapshot(0, venue=Venue.OKX),))
    supervisor = Wave1PrivateStateSupervisor(
        {
            Venue.BINANCE_USDM: binance,
            Venue.BYBIT: FailingSnapshotAdapter(),
            Venue.OKX: okx,
        }
    )

    views = await supervisor.startup()

    assert views[Venue.BINANCE_USDM].ready is True
    assert views[Venue.OKX].ready is True
    assert views[Venue.BYBIT].reason == "REST_RECONCILIATION_FAILED:TimeoutError"
    assert binance.calls == 1
    assert okx.calls == 1


@pytest.mark.asyncio
async def test_wave1_supervisor_rejects_cross_venue_snapshot() -> None:
    supervisor = Wave1PrivateStateSupervisor(
        {
            Venue.BINANCE_USDM: ScriptedSnapshotAdapter((_snapshot(0, venue=Venue.BYBIT),)),
            Venue.BYBIT: ScriptedSnapshotAdapter((_snapshot(0, venue=Venue.BYBIT),)),
            Venue.OKX: ScriptedSnapshotAdapter((_snapshot(0, venue=Venue.OKX),)),
        }
    )

    views = await supervisor.startup()

    assert views[Venue.BINANCE_USDM].reason == "PRIVATE_SNAPSHOT_VENUE_MISMATCH"
    assert views[Venue.BYBIT].ready is True
    assert views[Venue.OKX].ready is True


@pytest.mark.asyncio
async def test_wave1_cache_restart_and_reconciliation_chaos() -> None:
    observed = datetime.now(UTC)
    initial = Wave1PrivateStateSupervisor(
        {
            venue: ScriptedSnapshotAdapter(
                (
                    _snapshot(0, observed_at=observed, venue=venue),
                    _snapshot(
                        1,
                        observed_at=observed + timedelta(milliseconds=2),
                        venue=venue,
                    ),
                )
            )
            for venue in Venue
        }
    )
    assert all(view.ready for view in (await initial.startup()).values())
    streamed = await initial.ingest_stream_snapshot(
        Venue.BYBIT,
        _snapshot(
            1,
            request_count=0,
            observed_at=observed + timedelta(milliseconds=1),
            venue=Venue.BYBIT,
            source_monotonic_ns=time.monotonic_ns(),
        ),
    )
    assert streamed.ready is True
    assert (await initial.reconcile(Venue.BYBIT, "AFTER_RESTART")).ready is True

    restarted = Wave1PrivateStateSupervisor(
        {
            venue: ScriptedSnapshotAdapter(
                (
                    _snapshot(
                        2,
                        observed_at=observed + timedelta(milliseconds=3),
                        venue=venue,
                    ),
                )
            )
            for venue in Venue
        }
    )

    assert all(not view.ready for view in (await restarted.views()).values())
    assert all(view.ready for view in (await restarted.startup()).values())


@pytest.mark.asyncio
async def test_wave1_supervisor_restores_persistent_event_watermarks(tmp_path: Path) -> None:
    state_path = tmp_path / "state.sqlite3"
    await initialise_state(state_path)
    observed = datetime.now(UTC)
    initial_adapters = {
        venue: ScriptedSnapshotAdapter((_snapshot(0, observed_at=observed, venue=venue),))
        for venue in Venue
    }
    initial = Wave1PrivateStateSupervisor(initial_adapters, state_path=state_path)
    assert all(view.ready for view in (await initial.startup()).values())

    streamed = await initial.ingest_stream_snapshot(
        Venue.BYBIT,
        _snapshot(
            1,
            request_count=0,
            observed_at=observed + timedelta(microseconds=1),
            venue=Venue.BYBIT,
            source_monotonic_ns=time.monotonic_ns(),
        ),
    )

    assert streamed.ready is True
    assert await read_private_event_watermark(state_path, Venue.BYBIT) == 1
    restarted_adapters = {
        venue: ScriptedSnapshotAdapter(
            (
                _snapshot(
                    1 if venue == Venue.BYBIT else 0,
                    observed_at=observed + timedelta(microseconds=2),
                    venue=venue,
                ),
            )
        )
        for venue in Venue
    }
    restarted = Wave1PrivateStateSupervisor(restarted_adapters, state_path=state_path)

    assert all(view.ready for view in (await restarted.startup()).values())
    assert restarted_adapters[Venue.BYBIT].seeded_watermark == 1
    assert restarted_adapters[Venue.BINANCE_USDM].seeded_watermark == 0
    assert restarted_adapters[Venue.OKX].seeded_watermark == 0


@pytest.mark.asyncio
async def test_cache_is_unknown_until_watermark_persistence_commits() -> None:
    observed = datetime.now(UTC)
    persistence_started = asyncio.Event()
    allow_persistence = asyncio.Event()

    async def persist(watermark: int) -> None:
        assert watermark == 1
        persistence_started.set()
        await allow_persistence.wait()

    cache = PrivateStateCache(
        ScriptedSnapshotAdapter(()),
        persist_watermark=persist,
    )
    update_task = asyncio.create_task(
        cache.ingest_stream_snapshot(
            _snapshot(
                1,
                request_count=0,
                observed_at=observed,
                source_monotonic_ns=time.monotonic_ns(),
            )
        )
    )
    await persistence_started.wait()

    pending = await cache.view()

    assert pending.status == PrivateCacheStatus.UNKNOWN
    assert pending.reason == "PRIVATE_WATERMARK_PERSIST_PENDING"
    allow_persistence.set()
    assert (await update_task).ready is True


@pytest.mark.asyncio
async def test_failed_watermark_persistence_never_exposes_ready_state() -> None:
    observed = datetime.now(UTC)
    persistence_started = asyncio.Event()
    allow_failure = asyncio.Event()

    async def fail_persistence(watermark: int) -> None:
        assert watermark == 1
        persistence_started.set()
        await allow_failure.wait()
        raise OSError("synthetic durable write failure")

    cache = PrivateStateCache(
        ScriptedSnapshotAdapter(()),
        persist_watermark=fail_persistence,
    )
    update_task = asyncio.create_task(
        cache.ingest_stream_snapshot(
            _snapshot(
                1,
                request_count=0,
                observed_at=observed,
                source_monotonic_ns=time.monotonic_ns(),
            )
        )
    )
    await persistence_started.wait()
    assert (await cache.view()).reason == "PRIVATE_WATERMARK_PERSIST_PENDING"
    allow_failure.set()

    failed = await update_task

    assert failed.status == PrivateCacheStatus.UNKNOWN
    assert failed.reason == "PRIVATE_WATERMARK_PERSIST_FAILED:OSError"


@pytest.mark.asyncio
async def test_rest_reconciliation_does_not_hide_failed_stream_channel() -> None:
    observed = datetime.now(UTC)
    cache = PrivateStateCache(
        ScriptedSnapshotAdapter(
            (
                _snapshot(0, observed_at=observed),
                _snapshot(0, observed_at=observed + timedelta(microseconds=1)),
            )
        )
    )
    assert (await cache.startup()).ready is True
    await cache.invalidate_stream(
        PrivateStreamKind.ORDERS,
        "PRIVATE_STREAM_FAILED:ORDERS:ConnectionError",
    )

    reconciled = await cache.reconcile("CONSUMER_FAIL_CLOSED_REFRESH")

    assert reconciled.status == PrivateCacheStatus.UNKNOWN
    assert reconciled.reason == "PRIVATE_STREAM_FAILED:ORDERS:ConnectionError"
