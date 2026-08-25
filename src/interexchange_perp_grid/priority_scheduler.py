from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import IntEnum
from typing import TypeVar, cast


class WorkPriority(IntEnum):
    EMERGENCY_FLATTEN = 0
    UNMATCHED_HEDGE = 1
    NORMAL_CLOSE = 2
    PRIVATE_RECONCILE = 3
    NEW_ENTRY = 4
    CANDIDATE_L2 = 5
    BROAD_BBO_HISTORY = 6

    @property
    def critical(self) -> bool:
        return self <= WorkPriority.PRIVATE_RECONCILE


class WorkRejected(RuntimeError):
    """Lower-priority work was shed before it could consume execution capacity."""


@dataclass(frozen=True, slots=True)
class SchedulerSnapshot:
    queued_by_priority: tuple[int, ...]
    running_by_priority: tuple[int, ...]
    waiting_critical_count: int
    pending_limit: int
    active_key_limit: int
    closed: bool

    @property
    def critical_work_count(self) -> int:
        return (
            sum(self.queued_by_priority[:4])
            + sum(self.running_by_priority[:4])
            + self.waiting_critical_count
        )


@dataclass(slots=True)
class _QueuedWork:
    priority: WorkPriority
    sequence: int
    key: str
    operation: Callable[[], Awaitable[object]]
    future: asyncio.Future[object]


T = TypeVar("T")


class PriorityWorkScheduler:
    """Bounded in-process P0-P6 scheduler with one reserved critical worker.

    P0-P3 callers apply backpressure and can displace queued P4-P6 work. P4-P6
    callers never wait for capacity: they are rejected fail-closed when critical
    work exists or when no lower-priority queued item can be shed.
    """

    def __init__(
        self,
        *,
        pending_limit: int,
        worker_count: int = 6,
        shutdown_timeout_seconds: float = 5.0,
    ) -> None:
        if pending_limit <= 0:
            raise ValueError("scheduler pending limit must be positive")
        if worker_count < 6:
            raise ValueError("scheduler requires four critical lanes and two general workers")
        if shutdown_timeout_seconds <= 0:
            raise ValueError("scheduler shutdown timeout must be positive")
        self._pending_limit = pending_limit
        self._general_active_key_limit = pending_limit + worker_count
        self._active_key_limit = self._general_active_key_limit + 4
        self._worker_count = worker_count
        self._shutdown_timeout_seconds = shutdown_timeout_seconds
        self._condition = asyncio.Condition()
        self._queue: list[_QueuedWork] = []
        self._by_key: dict[str, _QueuedWork] = {}
        self._admissions: dict[str, tuple[WorkPriority, asyncio.Future[object]]] = {}
        self._reserved_active_keys: dict[WorkPriority, str] = {}
        self._running: dict[str, WorkPriority] = {}
        self._operations: dict[str, asyncio.Future[object]] = {}
        self._workers: set[asyncio.Task[None]] = set()
        self._waiting_critical = 0
        self._sequence = 0
        self._started = False
        self._closed = False

    async def start(self) -> None:
        async with self._condition:
            if self._closed:
                raise RuntimeError("priority scheduler is closed")
            if self._started:
                return
            self._started = True
            for index in range(self._worker_count):
                reserved_priority = (
                    WorkPriority(index) if index <= int(WorkPriority.PRIVATE_RECONCILE) else None
                )
                worker = asyncio.create_task(
                    self._worker(reserved_priority=reserved_priority),
                    name=f"priority-work-{index}",
                )
                self._workers.add(worker)

    def snapshot(self) -> SchedulerSnapshot:
        queued = [0] * len(WorkPriority)
        running = [0] * len(WorkPriority)
        for item in self._queue:
            queued[int(item.priority)] += 1
        for priority in self._running.values():
            running[int(priority)] += 1
        return SchedulerSnapshot(
            tuple(queued),
            tuple(running),
            self._waiting_critical,
            self._pending_limit,
            self._active_key_limit,
            self._closed,
        )

    def critical_work_count(self) -> int:
        return self.snapshot().critical_work_count

    async def cancel_and_wait(self, key: str, *, timeout_seconds: float) -> bool:
        """Cancel one owned operation and prove it stopped before returning true."""
        if not key:
            raise ValueError("scheduler work key cannot be empty")
        if timeout_seconds <= 0:
            raise ValueError("scheduler cancellation timeout must be positive")
        async with self._condition:
            item = self._by_key.get(key)
            if item is None:
                return True
            operation = self._operations.get(key)
            if operation is None:
                if item in self._queue:
                    self._queue.remove(item)
                    self._by_key.pop(key, None)
                    if not item.future.done():
                        item.future.cancel()
                    self._condition.notify_all()
                    return True
                return False
            operation.cancel()
        _done, pending = await asyncio.wait({operation}, timeout=timeout_seconds)
        return not pending

    async def run(
        self,
        priority: WorkPriority,
        key: str,
        operation: Callable[[], Awaitable[T]],
    ) -> T:
        if not key:
            raise ValueError("scheduler work key cannot be empty")
        await self.start()
        shared: asyncio.Future[object]
        admission_interrupted = False
        async with self._condition:
            leader = False
            while True:
                if self._closed:
                    raise RuntimeError("priority scheduler is closed")
                existing = self._by_key.get(key)
                if existing is not None:
                    if existing.priority != priority:
                        raise RuntimeError("scheduler key cannot change priority while active")
                    shared = existing.future
                    break
                admission = self._admissions.get(key)
                if admission is not None:
                    admitted_priority, shared = admission
                    if admitted_priority != priority:
                        raise RuntimeError("scheduler key cannot change priority while active")
                    break
                active_key_count = len(self._by_key) + len(self._admissions)
                reserve_for_priority = (
                    priority.critical
                    and priority not in self._reserved_active_keys
                    and active_key_count < self._active_key_limit
                )
                if active_key_count < self._general_active_key_limit or reserve_for_priority:
                    loop = asyncio.get_running_loop()
                    shared = loop.create_future()
                    shared.add_done_callback(_consume_future_exception)
                    self._admissions[key] = (priority, shared)
                    if active_key_count >= self._general_active_key_limit:
                        self._reserved_active_keys[priority] = key
                    leader = True
                    break
                if not priority.critical:
                    raise WorkRejected(f"{priority.name} rejected by active-key bound")
                self._waiting_critical += 1
                try:
                    await self._condition.wait()
                finally:
                    self._waiting_critical -= 1
            if leader:
                try:
                    if priority.critical:
                        self._waiting_critical += 1
                        self._shed_queued_lower_work(priority)
                    try:
                        while True:
                            try:
                                await self._reserve_queue_slot(priority)
                                break
                            except asyncio.CancelledError:
                                # Once the admission placeholder is visible, the
                                # scheduler owns completing this single-flight.
                                # Preserve followers even if the leader leaves.
                                admission_interrupted = True
                    finally:
                        if priority.critical:
                            self._waiting_critical -= 1
                    item = _QueuedWork(
                        priority,
                        self._sequence,
                        key,
                        cast(Callable[[], Awaitable[object]], operation),
                        shared,
                    )
                    self._sequence += 1
                    self._queue.append(item)
                    self._by_key[key] = item
                    self._admissions.pop(key, None)
                    self._condition.notify_all()
                except BaseException as error:
                    self._admissions.pop(key, None)
                    if self._reserved_active_keys.get(priority) == key:
                        self._reserved_active_keys.pop(priority, None)
                    if not shared.done():
                        if isinstance(error, asyncio.CancelledError):
                            shared.cancel()
                        else:
                            shared.set_exception(error)
                    self._condition.notify_all()
                    raise
        if admission_interrupted:
            raise asyncio.CancelledError
        return cast(T, await asyncio.shield(shared))

    async def _reserve_queue_slot(self, priority: WorkPriority) -> None:
        while len(self._queue) >= self._pending_limit:
            victim = self._lowest_priority_queued_below(priority)
            if victim is not None:
                self._queue.remove(victim)
                self._by_key.pop(victim.key, None)
                if not victim.future.done():
                    victim.future.set_exception(
                        WorkRejected(f"{victim.priority.name} shed for {priority.name}")
                    )
                continue
            if not priority.critical:
                raise WorkRejected(f"{priority.name} rejected by bounded scheduler")
            if not any(item.priority == priority for item in self._queue):
                return
            await self._condition.wait()
            if self._closed:
                raise RuntimeError("priority scheduler is closed")
        if not priority.critical and self.critical_work_count() > 0:
            raise WorkRejected(f"{priority.name} rejected while critical work is active")

    def _lowest_priority_queued_below(self, incoming: WorkPriority) -> _QueuedWork | None:
        candidates = [
            item for item in self._queue if not item.priority.critical and item.priority > incoming
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda item: (item.priority, item.sequence))

    def _shed_queued_lower_work(self, incoming: WorkPriority) -> None:
        victims = tuple(item for item in self._queue if not item.priority.critical)
        for victim in victims:
            self._queue.remove(victim)
            self._by_key.pop(victim.key, None)
            if not victim.future.done():
                victim.future.set_exception(
                    WorkRejected(f"{victim.priority.name} shed for {incoming.name}")
                )

    async def _worker(self, *, reserved_priority: WorkPriority | None) -> None:
        while True:
            async with self._condition:
                item = self._next_item(reserved_priority)
                while item is None:
                    if self._closed:
                        return
                    await self._condition.wait()
                    item = self._next_item(reserved_priority)
                self._queue.remove(item)
                self._running[item.key] = item.priority
                self._condition.notify_all()
            try:
                operation: asyncio.Future[object] = asyncio.ensure_future(item.operation())
                async with self._condition:
                    self._operations[item.key] = operation
                result = await operation
            except asyncio.CancelledError:
                if not item.future.done():
                    item.future.cancel()
                current = asyncio.current_task()
                if self._closed or (current is not None and current.cancelling()):
                    raise
            except Exception as error:
                if not item.future.done():
                    item.future.set_exception(error)
            else:
                if not item.future.done():
                    item.future.set_result(result)
            finally:
                async with self._condition:
                    self._operations.pop(item.key, None)
                    self._running.pop(item.key, None)
                    self._by_key.pop(item.key, None)
                    if self._reserved_active_keys.get(item.priority) == item.key:
                        self._reserved_active_keys.pop(item.priority, None)
                    self._condition.notify_all()

    def _next_item(self, reserved_priority: WorkPriority | None) -> _QueuedWork | None:
        candidates = (
            self._queue
            if reserved_priority is None
            else [item for item in self._queue if item.priority == reserved_priority]
        )
        if not candidates:
            return None
        return min(candidates, key=lambda item: (item.priority, item.sequence))

    async def close(self) -> None:
        async with self._condition:
            self._closed = True
            for item in self._queue:
                if not item.future.done():
                    item.future.set_exception(RuntimeError("priority scheduler closed before run"))
            self._queue.clear()
            for _priority, future in self._admissions.values():
                if not future.done():
                    future.set_exception(RuntimeError("priority scheduler closed during admission"))
            self._admissions.clear()
            self._reserved_active_keys.clear()
            self._by_key = {key: item for key, item in self._by_key.items() if key in self._running}
            workers = tuple(worker for worker in self._workers if not worker.done())
            for worker in workers:
                worker.cancel()
            self._condition.notify_all()
        if workers:
            done, pending = await asyncio.wait(
                workers,
                timeout=self._shutdown_timeout_seconds,
            )
            for worker in done:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    worker.result()
        else:
            pending = set()
        self._workers = {worker for worker in self._workers if not worker.done()}
        if pending:
            raise RuntimeError("priority scheduler shutdown failed: work remains active")


def _consume_future_exception(future: asyncio.Future[object]) -> None:
    if future.cancelled():
        return
    with contextlib.suppress(Exception):
        future.exception()
