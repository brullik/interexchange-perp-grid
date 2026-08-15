from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from functools import partial
from pathlib import Path
from typing import Any, Protocol, cast

from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.private_domain import (
    AccountSnapshot,
    PositionSnapshot,
    PrivateActiveSnapshot,
    PrivateOrder,
    PrivateOrderStatus,
    PrivateStreamEvent,
    PrivateStreamKind,
    SnapshotCompleteness,
)
from interexchange_perp_grid.state import (
    read_private_event_watermark,
    save_private_event_watermark,
)

WAVE1_PRIVATE_VENUES = (Venue.BINANCE_USDM, Venue.BYBIT, Venue.OKX)


class PrivateCacheStatus(StrEnum):
    READY = "READY"
    UNKNOWN = "UNKNOWN"


class AccountWidePrivateAdapter(Protocol):
    def seed_private_event_watermark(self, watermark: int) -> None: ...

    def acknowledge_private_event(self, watermark: int) -> None: ...

    async def fetch_active_snapshot(self) -> PrivateActiveSnapshot: ...

    async def watch_account_wide_orders(self) -> PrivateStreamEvent: ...

    async def watch_account_wide_positions(self) -> PrivateStreamEvent: ...

    async def watch_account_wide_balance(self) -> PrivateStreamEvent: ...


class CachedPrivateDelegate(AccountWidePrivateAdapter, Protocol):
    async def fetch_account(self, instrument: Instrument) -> AccountSnapshot: ...

    async def fetch_closed_orders(self, instrument: Instrument) -> tuple[PrivateOrder, ...]: ...

    async def fetch_trading_fee(self, instrument: Instrument) -> Decimal | None: ...


@dataclass(frozen=True, slots=True)
class PrivateCachePolicy:
    maximum_age_seconds: Decimal = Decimal(2)
    reconciliation_interval_seconds: Decimal = Decimal(30)
    reconciliation_timeout_seconds: Decimal = Decimal(2)
    stream_shutdown_timeout_seconds: Decimal = Decimal("0.1")
    maximum_rest_requests: int = 4
    maximum_rest_requests_per_minute: int = 120
    maximum_pending_stream_events: int = 1024
    maximum_p95_latency_ms: Decimal = Decimal(250)

    def __post_init__(self) -> None:
        if (
            self.maximum_age_seconds <= 0
            or self.reconciliation_interval_seconds <= 0
            or self.reconciliation_timeout_seconds <= 0
            or self.stream_shutdown_timeout_seconds <= 0
        ):
            raise ValueError("private cache timing must be positive")
        if (
            self.maximum_rest_requests <= 0
            or self.maximum_rest_requests_per_minute < self.maximum_rest_requests
            or self.maximum_pending_stream_events <= 0
            or self.maximum_p95_latency_ms <= 0
        ):
            raise ValueError("private cache budgets must be positive")


@dataclass(frozen=True, slots=True)
class PrivateCacheView:
    status: PrivateCacheStatus
    snapshot: PrivateActiveSnapshot | None
    cache_watermark: int
    source: str | None
    reason: str | None
    age_seconds: Decimal | None
    p95_latency_ms: Decimal | None
    event_p95_latency_ms: Decimal | None

    @property
    def ready(self) -> bool:
        return self.status == PrivateCacheStatus.READY


class PrivateStateCache:
    """Fail-closed latest-value cache backed by bounded account-wide reconciliation."""

    def __init__(
        self,
        adapter: AccountWidePrivateAdapter,
        policy: PrivateCachePolicy | None = None,
        *,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        expected_venue: Venue | None = None,
        persist_watermark: Callable[[int], Awaitable[None]] | None = None,
    ) -> None:
        self._adapter = adapter
        self._policy = policy or PrivateCachePolicy()
        self._monotonic_ns = monotonic_ns
        self._expected_venue = expected_venue
        self._persist_watermark = persist_watermark
        self._lock = asyncio.Lock()
        self._reconcile_lock = asyncio.Lock()
        self._persist_lock = asyncio.Lock()
        self._snapshot: PrivateActiveSnapshot | None = None
        self._updated_monotonic_ns: int | None = None
        self._orders_updated_monotonic_ns: int | None = None
        self._positions_updated_monotonic_ns: int | None = None
        self._cache_watermark = 0
        self._source: str | None = None
        self._stream_watermark: int | None = None
        self._stream_signature: tuple[object, ...] | None = None
        self._account_snapshot: AccountSnapshot | None = None
        self._account_updated_monotonic_ns: int | None = None
        self._invalid_reason: str | None = "PRIVATE_CACHE_NOT_INITIALISED"
        self._pending_persistence_watermark: int | None = None
        self._delivery_error: str | None = None
        self._stream_errors: dict[PrivateStreamKind, str] = {}
        self._pending_stream_events: dict[int, PrivateStreamEvent] = {}
        self._rest_latencies_ms: deque[Decimal] = deque(maxlen=256)
        self._rest_request_times_ns: deque[int] = deque()
        self._event_latencies_ms: deque[Decimal] = deque(maxlen=256)

    async def startup(self) -> PrivateCacheView:
        return await self.reconcile("STARTUP")

    async def reconcile(self, trigger: str) -> PrivateCacheView:
        if not trigger.strip():
            raise ValueError("private cache reconciliation trigger is required")
        async with self._reconcile_lock:
            if trigger.startswith("CONSUMER_FAIL_CLOSED_REFRESH"):
                async with self._lock:
                    current = self._view_locked()
                    if current.ready:
                        return current
            if _uses_consumer_rest_budget(trigger):
                now_ns = self._monotonic_ns()
                minute_ago_ns = now_ns - 60_000_000_000
                while (
                    self._rest_request_times_ns and self._rest_request_times_ns[0] <= minute_ago_ns
                ):
                    self._rest_request_times_ns.popleft()
                if (
                    len(self._rest_request_times_ns) + self._policy.maximum_rest_requests
                    > self._policy.maximum_rest_requests_per_minute
                ):
                    async with self._lock:
                        self._invalid_reason = "PRIVATE_REST_RATE_BUDGET_EXCEEDED"
                        return self._view_locked()
                self._rest_request_times_ns.extend(
                    now_ns for _ in range(self._policy.maximum_rest_requests)
                )
            try:
                snapshot = await asyncio.wait_for(
                    self._adapter.fetch_active_snapshot(),
                    timeout=float(self._policy.reconciliation_timeout_seconds),
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                async with self._lock:
                    self._invalid_reason = f"REST_RECONCILIATION_FAILED:{type(error).__name__}"
                    return self._view_locked()
            async with self._lock:
                accepted = self._accept_snapshot_locked(
                    snapshot,
                    f"REST:{trigger}",
                    require_newer=False,
                )
                if accepted and snapshot.account_wide:
                    self._rest_latencies_ms.append(snapshot.latency_ms)
                    self._orders_updated_monotonic_ns = self._updated_monotonic_ns
                    self._positions_updated_monotonic_ns = self._updated_monotonic_ns
                self._mark_persistence_pending_locked(snapshot.event_watermark, accepted)
                view = self._view_locked()
            return await self._persist_accepted_watermark(snapshot.event_watermark, view, accepted)

    async def ingest_stream_snapshot(self, snapshot: PrivateActiveSnapshot) -> PrivateCacheView:
        async with self._lock:
            accepted = self._accept_snapshot_locked(
                snapshot,
                "PRIVATE_STREAM",
                require_newer=True,
            )
            if accepted and snapshot.account_wide:
                self._stream_errors.clear()
                self._orders_updated_monotonic_ns = self._updated_monotonic_ns
                self._positions_updated_monotonic_ns = self._updated_monotonic_ns
                source_ns = snapshot.source_monotonic_ns
                accepted_ns = self._updated_monotonic_ns
                if source_ns is None or accepted_ns is None:
                    self._invalid_reason = "PRIVATE_EVENT_LATENCY_UNKNOWN"
                elif source_ns > accepted_ns:
                    self._invalid_reason = "PRIVATE_EVENT_MONOTONIC_TIME_INVALID"
                else:
                    self._event_latencies_ms.append(
                        Decimal(accepted_ns - source_ns) / Decimal(1_000_000)
                    )
            self._mark_persistence_pending_locked(snapshot.event_watermark, accepted)
            view = self._view_locked()
        return await self._persist_accepted_watermark(snapshot.event_watermark, view, accepted)

    async def ingest_stream_event(self, event: PrivateStreamEvent) -> PrivateCacheView:
        accepted_watermark: int | None = None
        async with self._lock:
            if self._expected_venue is not None and event.venue != self._expected_venue:
                self._invalid_reason = "PRIVATE_STREAM_EVENT_VENUE_MISMATCH"
                return self._view_locked()
            previous = self._snapshot
            if previous is None:
                self._invalid_reason = "PRIVATE_STREAM_EVENT_BEFORE_STARTUP"
                return self._view_locked()
            if (
                event.event_watermark <= previous.event_watermark
                or event.event_watermark in self._pending_stream_events
            ):
                self._stream_errors[event.kind] = "PRIVATE_EVENT_WATERMARK_NOT_INCREASING"
                return self._view_locked()
            if event.unknown_active_records:
                self._stream_errors[event.kind] = "PRIVATE_STREAM_EVENT_INCOMPLETE"
                return self._view_locked()
            if event.kind == PrivateStreamKind.ACCOUNT and event.account is None:
                self._stream_errors[event.kind] = "PRIVATE_ACCOUNT_STREAM_EMPTY"
                return self._view_locked()

            expected_watermark = previous.event_watermark + 1
            if (
                len(self._pending_stream_events) >= self._policy.maximum_pending_stream_events
                and event.event_watermark != expected_watermark
            ):
                self._delivery_error = "PRIVATE_EVENT_BUFFER_LIMIT_EXCEEDED"
                return self._view_locked()
            self._pending_stream_events[event.event_watermark] = event
            if event.event_watermark != expected_watermark and not self._stream_errors:
                self._stream_errors[event.kind] = "PRIVATE_EVENT_DELIVERY_OUT_OF_ORDER"
            while (
                next_event := self._pending_stream_events.pop(expected_watermark, None)
            ) is not None:
                if not self._apply_stream_event_locked(next_event):
                    break
                accepted_watermark = next_event.event_watermark
                expected_watermark += 1
            self._mark_persistence_pending_locked(
                accepted_watermark or event.event_watermark,
                accepted_watermark is not None,
            )
            view = self._view_locked()
        return await self._persist_accepted_watermark(
            accepted_watermark or event.event_watermark,
            view,
            accepted_watermark is not None,
        )

    def _apply_stream_event_locked(self, event: PrivateStreamEvent) -> bool:
        previous = self._snapshot
        if previous is None or event.event_watermark != previous.event_watermark + 1:
            self._delivery_error = "PRIVATE_EVENT_DELIVERY_OUT_OF_ORDER"
            return False
        if (
            previous.source_monotonic_ns is not None
            and event.source_monotonic_ns < previous.source_monotonic_ns
        ):
            self._delivery_error = "PRIVATE_EVENT_MONOTONIC_TIME_REGRESSION"
            return False
        orders = {_order_key(order): order for order in previous.open_orders}
        positions = {
            (position.symbol, position.side.value): position for position in previous.positions
        }
        if event.kind == PrivateStreamKind.ORDERS:
            for order in event.orders:
                matching = tuple(
                    key
                    for key, existing in orders.items()
                    if (order.order_id is not None and existing.order_id == order.order_id)
                    or existing.client_order_id == order.client_order_id
                )
                for key in matching:
                    orders.pop(key, None)
                if order.status in {PrivateOrderStatus.OPEN, PrivateOrderStatus.PARTIAL}:
                    orders[_order_key(order)] = order
        elif event.kind == PrivateStreamKind.POSITIONS:
            for position in event.positions:
                key = (position.symbol, position.side.value)
                if position.base_quantity == 0:
                    positions.pop(key, None)
                else:
                    positions[key] = position

        stream_snapshot = PrivateActiveSnapshot(
            event.venue,
            len(orders),
            len(positions),
            tuple(sorted(orders.values(), key=_order_key)),
            tuple(
                sorted(
                    positions.values(),
                    key=lambda position: (position.symbol, position.side.value),
                )
            ),
            (),
            SnapshotCompleteness.COMPLETE,
            max(previous.observed_at, event.observed_at),
            event.event_watermark,
            0,
            Decimal(0),
            True,
            event.source_monotonic_ns,
        )
        accepted = self._accept_snapshot_locked(
            stream_snapshot,
            "PRIVATE_STREAM",
            require_newer=True,
        )
        if not accepted:
            self._delivery_error = self._invalid_reason or "PRIVATE_STREAM_REJECTED"
            return False
        self._stream_errors.pop(event.kind, None)
        accepted_ns = self._updated_monotonic_ns
        if event.kind == PrivateStreamKind.ORDERS:
            self._orders_updated_monotonic_ns = accepted_ns
        elif event.kind == PrivateStreamKind.POSITIONS:
            self._positions_updated_monotonic_ns = accepted_ns
        elif event.kind == PrivateStreamKind.ACCOUNT:
            self._account_snapshot = event.account
            self._account_updated_monotonic_ns = accepted_ns
        if accepted_ns is None or event.source_monotonic_ns > accepted_ns:
            self._stream_errors[event.kind] = "PRIVATE_EVENT_MONOTONIC_TIME_INVALID"
        else:
            self._event_latencies_ms.append(
                Decimal(accepted_ns - event.source_monotonic_ns) / Decimal(1_000_000)
            )
        return True

    async def invalidate(self, reason: str) -> PrivateCacheView:
        if not reason.strip():
            raise ValueError("private cache invalidation reason is required")
        async with self._lock:
            self._invalid_reason = reason
            return self._view_locked()

    async def invalidate_stream(self, kind: PrivateStreamKind, reason: str) -> PrivateCacheView:
        if not reason.strip():
            raise ValueError("private stream invalidation reason is required")
        async with self._lock:
            self._stream_errors[kind] = reason
            return self._view_locked()

    async def view(self) -> PrivateCacheView:
        async with self._lock:
            return self._view_locked()

    async def account_snapshot(self) -> AccountSnapshot | None:
        async with self._lock:
            if (
                self._account_snapshot is None
                or self._account_updated_monotonic_ns is None
                or PrivateStreamKind.ACCOUNT in self._stream_errors
            ):
                return None
            age = Decimal(self._monotonic_ns() - self._account_updated_monotonic_ns) / Decimal(
                1_000_000_000
            )
            if age > self._policy.maximum_age_seconds:
                return None
            return self._account_snapshot

    async def _persist_accepted_watermark(
        self,
        watermark: int,
        view: PrivateCacheView,
        accepted: bool,
    ) -> PrivateCacheView:
        if not accepted or self._persist_watermark is None:
            return view
        try:
            async with self._persist_lock:
                await self._persist_watermark(watermark)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            async with self._lock:
                if self._pending_persistence_watermark == watermark:
                    self._pending_persistence_watermark = None
                self._invalid_reason = f"PRIVATE_WATERMARK_PERSIST_FAILED:{type(error).__name__}"
                return self._view_locked()
        async with self._lock:
            if self._pending_persistence_watermark == watermark:
                self._pending_persistence_watermark = None
            if self._invalid_reason is not None and self._invalid_reason.startswith(
                "PRIVATE_WATERMARK_PERSIST_FAILED:"
            ):
                self._invalid_reason = None
            return self._view_locked()

    def _mark_persistence_pending_locked(self, watermark: int, accepted: bool) -> None:
        if accepted and self._persist_watermark is not None:
            self._pending_persistence_watermark = watermark

    async def run_periodic(self, stop_event: asyncio.Event) -> None:
        interval = float(self._policy.reconciliation_interval_seconds)
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except TimeoutError:
                await self.reconcile("PERIODIC_30S")

    async def run_stream(self, kind: PrivateStreamKind, stop_event: asyncio.Event) -> None:
        watchers = {
            PrivateStreamKind.ORDERS: self._adapter.watch_account_wide_orders,
            PrivateStreamKind.POSITIONS: self._adapter.watch_account_wide_positions,
            PrivateStreamKind.ACCOUNT: self._adapter.watch_account_wide_balance,
        }
        watcher = watchers[kind]
        while not stop_event.is_set():
            watch_task = asyncio.create_task(watcher())
            stop_task = asyncio.create_task(stop_event.wait())
            done, pending = await asyncio.wait(
                {watch_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            stop_requested = stop_task in done and stop_task.result()
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.wait(
                    pending,
                    timeout=float(self._policy.stream_shutdown_timeout_seconds),
                )
                for task in pending:
                    if task.done():
                        _consume_task_result(task)
                    else:
                        task.add_done_callback(_consume_task_result)
            if stop_requested:
                return
            try:
                event = watch_task.result()
                try:
                    if event.kind != kind:
                        await self.invalidate_stream(kind, "PRIVATE_STREAM_KIND_MISMATCH")
                    else:
                        await self.ingest_stream_event(event)
                finally:
                    self._adapter.acknowledge_private_event(event.event_watermark)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await self.invalidate_stream(
                    kind,
                    f"PRIVATE_STREAM_FAILED:{kind.value}:{type(error).__name__}",
                )
                with suppress(TimeoutError):
                    await asyncio.wait_for(stop_event.wait(), timeout=1)

    def _accept_snapshot_locked(
        self,
        snapshot: PrivateActiveSnapshot,
        source: str,
        *,
        require_newer: bool,
    ) -> bool:
        if self._expected_venue is not None and snapshot.venue != self._expected_venue:
            self._invalid_reason = "PRIVATE_SNAPSHOT_VENUE_MISMATCH"
            return False
        if source != "PRIVATE_STREAM" and _snapshot_has_unknown_reason(
            snapshot, "PRIVATE_EVENT_DELIVERY_PENDING"
        ):
            self._invalid_reason = "PRIVATE_EVENT_DELIVERY_PENDING"
            return False
        signature = _private_snapshot_signature(snapshot)
        if (
            source != "PRIVATE_STREAM"
            and self._stream_watermark is not None
            and snapshot.event_watermark == self._stream_watermark
            and signature != self._stream_signature
        ):
            self._invalid_reason = "PRIVATE_REST_CONFLICT_AT_STREAM_WATERMARK"
            return False
        previous = self._snapshot
        if previous is not None:
            if snapshot.event_watermark < previous.event_watermark:
                self._invalid_reason = "PRIVATE_EVENT_WATERMARK_REGRESSION"
                return False
            if require_newer and snapshot.event_watermark == previous.event_watermark:
                self._invalid_reason = "PRIVATE_EVENT_WATERMARK_NOT_INCREASING"
                return False
            if snapshot.observed_at < previous.observed_at:
                self._invalid_reason = "PRIVATE_SNAPSHOT_TIME_REGRESSION"
                return False
        self._snapshot = snapshot
        if source != "PRIVATE_STREAM":
            stale_watermarks = tuple(
                watermark
                for watermark in self._pending_stream_events
                if watermark <= snapshot.event_watermark
            )
            for watermark in stale_watermarks:
                self._pending_stream_events.pop(watermark, None)
            self._delivery_error = None
        self._updated_monotonic_ns = self._monotonic_ns()
        self._cache_watermark += 1
        self._source = source
        if source == "PRIVATE_STREAM":
            self._stream_watermark = snapshot.event_watermark
            self._stream_signature = signature
        self._invalid_reason = None
        return True

    def _view_locked(self) -> PrivateCacheView:
        snapshot = self._snapshot
        now_ns = self._monotonic_ns()
        active_updated_ns = (
            min(self._orders_updated_monotonic_ns, self._positions_updated_monotonic_ns)
            if self._orders_updated_monotonic_ns is not None
            and self._positions_updated_monotonic_ns is not None
            else None
        )
        age = (
            Decimal(now_ns - active_updated_ns) / Decimal(1_000_000_000)
            if active_updated_ns is not None
            else None
        )
        p95 = self._p95_latency_ms()
        event_p95 = self._p95(self._event_latencies_ms)
        reason = self._invalid_reason
        if reason is None and self._pending_persistence_watermark is not None:
            reason = "PRIVATE_WATERMARK_PERSIST_PENDING"
        if reason is None and self._delivery_error is not None:
            reason = self._delivery_error
        if reason is None and self._stream_errors:
            reason = sorted(self._stream_errors.items(), key=lambda item: item[0].value)[0][1]
        if snapshot is None:
            if reason is None:
                reason = "PRIVATE_CACHE_NOT_INITIALISED"
        else:
            if reason is None and not snapshot.account_wide:
                reason = "PRIVATE_SNAPSHOT_NOT_ACCOUNT_WIDE"
            if reason is None and snapshot.request_count > self._policy.maximum_rest_requests:
                reason = "PRIVATE_REQUEST_BUDGET_EXCEEDED"
            if reason is None and snapshot.completeness != SnapshotCompleteness.COMPLETE:
                reason = "PRIVATE_SNAPSHOT_INCOMPLETE"
        if reason is None and age is not None and age > self._policy.maximum_age_seconds:
            reason = "PRIVATE_CACHE_STALE"
        if reason is None and p95 is not None and p95 > self._policy.maximum_p95_latency_ms:
            reason = "PRIVATE_RECONCILIATION_P95_EXCEEDED"
        if (
            reason is None
            and event_p95 is not None
            and event_p95 > self._policy.maximum_p95_latency_ms
        ):
            reason = "PRIVATE_EVENT_P95_EXCEEDED"
        return PrivateCacheView(
            PrivateCacheStatus.READY if reason is None else PrivateCacheStatus.UNKNOWN,
            snapshot,
            self._cache_watermark,
            self._source,
            reason,
            age,
            p95,
            event_p95,
        )

    def _p95_latency_ms(self) -> Decimal | None:
        return self._p95(self._rest_latencies_ms)

    @staticmethod
    def _p95(values: deque[Decimal]) -> Decimal | None:
        if not values:
            return None
        ordered = sorted(values)
        index = max(0, (len(ordered) * 95 + 99) // 100 - 1)
        return ordered[index]


def _private_snapshot_signature(snapshot: PrivateActiveSnapshot) -> tuple[object, ...]:
    return (
        snapshot.raw_open_order_count,
        snapshot.raw_nonzero_position_count,
        snapshot.completeness.value,
        tuple(
            sorted(
                (
                    order.order_id or "",
                    order.client_order_id,
                    order.symbol,
                    order.side.value,
                    order.status.value,
                    str(order.requested_base_quantity),
                    str(order.filled_base_quantity),
                )
                for order in snapshot.open_orders
            )
        ),
        tuple(
            sorted(
                (position.symbol, position.side.value, str(position.base_quantity))
                for position in snapshot.positions
            )
        ),
        tuple(sorted((record.kind, record.reason) for record in snapshot.unknown_active_records)),
    )


def _snapshot_has_unknown_reason(snapshot: PrivateActiveSnapshot, reason: str) -> bool:
    return any(record.reason == reason for record in snapshot.unknown_active_records)


def _uses_consumer_rest_budget(trigger: str) -> bool:
    return trigger.startswith(
        (
            "STARTUP",
            "PERIODIC_30S",
            "CONSUMER_FAIL_CLOSED_REFRESH",
            "PRE_SUBMIT",
        )
    )


def _consume_task_result(task: asyncio.Task[Any]) -> None:
    with suppress(asyncio.CancelledError, Exception):
        task.result()


def _order_key(order: object) -> tuple[str, str]:
    order_id = getattr(order, "order_id", None)
    client_order_id = getattr(order, "client_order_id", None)
    return str(order_id or ""), str(client_order_id or "")


class CachedPrivateStateAdapter:
    """Use the qualified latest-value cache for active state and delegate other operations."""

    def __init__(self, delegate: CachedPrivateDelegate, cache: PrivateStateCache) -> None:
        self._delegate = delegate
        self._cache = cache

    async def fetch_active_snapshot(self) -> PrivateActiveSnapshot:
        view = await self._cache.view()
        if not view.ready:
            view = await self._cache.reconcile("CONSUMER_FAIL_CLOSED_REFRESH")
        if not view.ready or view.snapshot is None:
            raise RuntimeError(view.reason or "PRIVATE_CACHE_UNKNOWN")
        return view.snapshot

    async def reconcile_active_snapshot(self, trigger: str) -> PrivateActiveSnapshot:
        view = await self._cache.reconcile(trigger)
        if not view.ready or view.snapshot is None:
            raise RuntimeError(view.reason or "PRIVATE_RECONCILIATION_UNKNOWN")
        return view.snapshot

    async def fetch_account(self, instrument: Instrument) -> AccountSnapshot:
        cached = await self._cache.account_snapshot()
        if cached is not None:
            return cached
        return await self._delegate.fetch_account(instrument)

    async def fetch_all_open_orders(self) -> tuple[PrivateOrder, ...]:
        snapshot = await self.fetch_active_snapshot()
        return snapshot.open_orders

    async def fetch_closed_orders(self, instrument: Instrument) -> tuple[PrivateOrder, ...]:
        return await self._delegate.fetch_closed_orders(instrument)

    async def fetch_all_positions(self) -> tuple[PositionSnapshot, ...]:
        snapshot = await self.fetch_active_snapshot()
        return snapshot.positions

    async def fetch_trading_fee(self, instrument: Instrument) -> Decimal | None:
        return await self._delegate.fetch_trading_fee(instrument)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


class Wave1PrivateStateSupervisor:
    """Own exactly one bounded latest-value private cache for each Wave 1 venue."""

    def __init__(
        self,
        adapters: Mapping[Venue, AccountWidePrivateAdapter],
        policy: PrivateCachePolicy | None = None,
        *,
        state_path: Path | None = None,
    ) -> None:
        if set(adapters) != set(WAVE1_PRIVATE_VENUES):
            raise ValueError("private cache supervisor requires exactly the three Wave 1 venues")
        self._adapters = dict(adapters)
        self._state_path = state_path
        self._caches = {
            venue: PrivateStateCache(
                adapters[venue],
                policy,
                expected_venue=venue,
                persist_watermark=(
                    partial(save_private_event_watermark, state_path, venue)
                    if state_path is not None
                    else None
                ),
            )
            for venue in WAVE1_PRIVATE_VENUES
        }

    async def startup(self) -> dict[Venue, PrivateCacheView]:
        async def start(venue: Venue) -> PrivateCacheView:
            if self._state_path is not None:
                try:
                    watermark = await read_private_event_watermark(self._state_path, venue)
                    self._adapters[venue].seed_private_event_watermark(watermark)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    return await self._caches[venue].invalidate(
                        f"PRIVATE_WATERMARK_LOAD_FAILED:{type(error).__name__}"
                    )
            return await self._caches[venue].startup()

        views = await asyncio.gather(*(start(venue) for venue in WAVE1_PRIVATE_VENUES))
        return dict(zip(WAVE1_PRIVATE_VENUES, views, strict=True))

    async def views(self) -> dict[Venue, PrivateCacheView]:
        views = await asyncio.gather(
            *(self._caches[venue].view() for venue in WAVE1_PRIVATE_VENUES)
        )
        return dict(zip(WAVE1_PRIVATE_VENUES, views, strict=True))

    async def reconcile(self, venue: Venue, trigger: str) -> PrivateCacheView:
        return await self._caches[venue].reconcile(trigger)

    async def ingest_stream_snapshot(
        self,
        venue: Venue,
        snapshot: PrivateActiveSnapshot,
    ) -> PrivateCacheView:
        if snapshot.venue != venue:
            raise ValueError("private stream snapshot venue does not match its cache")
        return await self._caches[venue].ingest_stream_snapshot(snapshot)

    async def account_snapshot(self, venue: Venue) -> AccountSnapshot | None:
        return await self._caches[venue].account_snapshot()

    def cached_adapter(self, venue: Venue) -> CachedPrivateStateAdapter:
        return CachedPrivateStateAdapter(
            cast(CachedPrivateDelegate, self._adapters[venue]),
            self._caches[venue],
        )

    def cached_adapters(self) -> dict[Venue, CachedPrivateStateAdapter]:
        return {venue: self.cached_adapter(venue) for venue in WAVE1_PRIVATE_VENUES}

    async def run_periodic(self, stop_event: asyncio.Event) -> None:
        await asyncio.gather(
            *(self._caches[venue].run_periodic(stop_event) for venue in WAVE1_PRIVATE_VENUES)
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        await asyncio.gather(
            self.run_periodic(stop_event),
            *(
                self._caches[venue].run_stream(kind, stop_event)
                for venue in WAVE1_PRIVATE_VENUES
                for kind in PrivateStreamKind
            ),
        )
