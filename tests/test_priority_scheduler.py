from __future__ import annotations

import asyncio
from functools import partial

import pytest

from interexchange_perp_grid.priority_scheduler import (
    PriorityWorkScheduler,
    WorkPriority,
    WorkRejected,
)


@pytest.mark.asyncio
async def test_reserved_worker_runs_critical_work_while_broad_work_is_blocked() -> None:
    scheduler = PriorityWorkScheduler(pending_limit=4, worker_count=6)
    broad_started = asyncio.Event()
    release_broad = asyncio.Event()
    critical_ran = asyncio.Event()
    release_critical = asyncio.Event()

    async def broad() -> str:
        broad_started.set()
        await release_broad.wait()
        return "broad"

    async def critical() -> str:
        critical_ran.set()
        await release_critical.wait()
        return "flat"

    broad_task = asyncio.create_task(scheduler.run(WorkPriority.BROAD_BBO_HISTORY, "broad", broad))
    await asyncio.wait_for(broad_started.wait(), timeout=1)
    critical_task = asyncio.create_task(
        scheduler.run(WorkPriority.EMERGENCY_FLATTEN, "flat", critical)
    )
    await asyncio.wait_for(critical_ran.wait(), timeout=1)
    with pytest.raises(WorkRejected, match="critical work"):
        await scheduler.run(WorkPriority.NEW_ENTRY, "entry", broad)

    release_critical.set()
    assert await critical_task == "flat"
    release_broad.set()
    assert await broad_task == "broad"
    await scheduler.close()


@pytest.mark.asyncio
async def test_all_critical_lanes_are_reserved_and_queue_is_bounded() -> None:
    scheduler = PriorityWorkScheduler(pending_limit=3, worker_count=6)
    release = asyncio.Event()
    started = [asyncio.Event() for _ in range(6)]
    order: list[str] = []

    async def held(index: int) -> None:
        started[index].set()
        await release.wait()

    priorities = (
        WorkPriority.BROAD_BBO_HISTORY,
        WorkPriority.BROAD_BBO_HISTORY,
        WorkPriority.PRIVATE_RECONCILE,
        WorkPriority.NORMAL_CLOSE,
        WorkPriority.UNMATCHED_HEDGE,
        WorkPriority.EMERGENCY_FLATTEN,
    )
    blocker_list: list[asyncio.Task[None]] = []
    for index, priority in enumerate(priorities):
        blocker_list.append(
            asyncio.create_task(scheduler.run(priority, f"held-{index}", partial(held, index)))
        )
        await asyncio.wait_for(started[index].wait(), timeout=1)
    blockers = tuple(blocker_list)

    async def record(label: str) -> str:
        order.append(label)
        return label

    p2 = asyncio.create_task(scheduler.run(WorkPriority.NORMAL_CLOSE, "p2", lambda: record("p2")))
    p1 = asyncio.create_task(
        scheduler.run(WorkPriority.UNMATCHED_HEDGE, "p1", lambda: record("p1"))
    )
    p0 = asyncio.create_task(
        scheduler.run(WorkPriority.EMERGENCY_FLATTEN, "p0", lambda: record("p0"))
    )
    await asyncio.sleep(0)
    assert sum(scheduler.snapshot().queued_by_priority) == 3

    waiter = asyncio.create_task(
        scheduler.run(WorkPriority.NORMAL_CLOSE, "bounded-waiter", lambda: record("waiter"))
    )
    await asyncio.sleep(0)
    assert not waiter.done()
    assert sum(scheduler.snapshot().queued_by_priority) == 3

    release.set()
    assert set(await asyncio.gather(p0, p1, p2, waiter)) == {"p0", "p1", "p2", "waiter"}
    assert set(order) == {"p0", "p1", "p2", "waiter"}
    await asyncio.gather(*blockers)
    await scheduler.close()


@pytest.mark.asyncio
async def test_higher_priority_work_sheds_queued_broad_before_entry() -> None:
    scheduler = PriorityWorkScheduler(pending_limit=2, worker_count=6)
    release = asyncio.Event()
    started = [asyncio.Event(), asyncio.Event()]

    async def held(index: int = 0) -> str:
        started[index].set()
        await release.wait()
        return "done"

    running_one = asyncio.create_task(
        scheduler.run(WorkPriority.BROAD_BBO_HISTORY, "running-1", lambda: held(0))
    )
    running_two = asyncio.create_task(
        scheduler.run(WorkPriority.BROAD_BBO_HISTORY, "running-2", lambda: held(1))
    )
    await asyncio.gather(*(asyncio.wait_for(event.wait(), timeout=1) for event in started))
    queued_one = asyncio.create_task(
        scheduler.run(WorkPriority.BROAD_BBO_HISTORY, "queued-1", held)
    )
    queued_two = asyncio.create_task(
        scheduler.run(WorkPriority.BROAD_BBO_HISTORY, "queued-2", held)
    )
    await asyncio.sleep(0)

    entry = asyncio.create_task(
        scheduler.run(WorkPriority.NEW_ENTRY, "entry", lambda: _return("entry"))
    )
    for _ in range(10):
        await asyncio.sleep(0)
        if any(task.done() for task in (queued_one, queued_two)):
            break
    shed = [task for task in (queued_one, queued_two) if task.done()]
    assert len(shed) == 1
    with pytest.raises(WorkRejected, match="shed for NEW_ENTRY"):
        await shed[0]

    release.set()
    assert await entry == "entry"
    remaining = queued_two if shed[0] is queued_one else queued_one
    assert await remaining == "done"
    assert await running_one == "done"
    assert await running_two == "done"
    await scheduler.close()


@pytest.mark.asyncio
async def test_critical_arrival_sheds_already_queued_new_entry() -> None:
    scheduler = PriorityWorkScheduler(pending_limit=8, worker_count=6)
    release_low = asyncio.Event()
    low_started = (asyncio.Event(), asyncio.Event())

    async def held_low(index: int) -> None:
        low_started[index].set()
        await release_low.wait()

    low = tuple(
        asyncio.create_task(
            scheduler.run(
                WorkPriority.BROAD_BBO_HISTORY,
                f"active-broad-{index}",
                partial(held_low, index),
            )
        )
        for index in range(2)
    )
    await asyncio.gather(*(asyncio.wait_for(event.wait(), timeout=1) for event in low_started))
    queued_entry = asyncio.create_task(
        scheduler.run(WorkPriority.NEW_ENTRY, "queued-entry", lambda: _return("entry"))
    )
    await asyncio.sleep(0)
    assert scheduler.snapshot().queued_by_priority[int(WorkPriority.NEW_ENTRY)] == 1

    assert (
        await scheduler.run(
            WorkPriority.PRIVATE_RECONCILE,
            "critical-reconcile",
            lambda: _return("reconciled"),
        )
        == "reconciled"
    )
    with pytest.raises(WorkRejected, match="shed for PRIVATE_RECONCILE"):
        await queued_entry

    release_low.set()
    await asyncio.gather(*low)
    await scheduler.close()


@pytest.mark.asyncio
async def test_same_key_is_single_flight_and_close_reports_resistant_work() -> None:
    scheduler = PriorityWorkScheduler(
        pending_limit=4,
        worker_count=6,
        shutdown_timeout_seconds=0.02,
    )
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def resistant() -> str:
        nonlocal calls
        calls += 1
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            await release.wait()
        return "recovered"

    first = asyncio.create_task(
        scheduler.run(WorkPriority.UNMATCHED_HEDGE, "same-action", resistant)
    )
    second = asyncio.create_task(
        scheduler.run(WorkPriority.UNMATCHED_HEDGE, "same-action", resistant)
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    with pytest.raises(RuntimeError, match="work remains active"):
        await scheduler.close()
    assert calls == 1
    assert scheduler.snapshot().critical_work_count == 1

    release.set()
    assert await first == "recovered"
    assert await second == "recovered"
    await scheduler.close()


@pytest.mark.asyncio
async def test_cancelled_admission_leader_does_not_cancel_single_flight_follower() -> None:
    scheduler = PriorityWorkScheduler(pending_limit=1, worker_count=6)
    release = asyncio.Event()
    started = tuple(asyncio.Event() for _ in range(3))

    async def hold(index: int) -> None:
        started[index].set()
        await release.wait()

    running = tuple(
        asyncio.create_task(
            scheduler.run(
                WorkPriority.PRIVATE_RECONCILE,
                f"occupied-p3-{index}",
                partial(hold, index),
            )
        )
        for index in range(3)
    )
    await asyncio.gather(*(asyncio.wait_for(event.wait(), timeout=1) for event in started))
    queued = asyncio.create_task(
        scheduler.run(
            WorkPriority.PRIVATE_RECONCILE,
            "queued-p3",
            lambda: _return("queued"),
        )
    )
    for _ in range(100):
        if scheduler.snapshot().queued_by_priority[int(WorkPriority.PRIVATE_RECONCILE)]:
            break
        await asyncio.sleep(0)

    calls = 0

    async def operation() -> str:
        nonlocal calls
        calls += 1
        return "shared"

    leader = asyncio.create_task(
        scheduler.run(WorkPriority.PRIVATE_RECONCILE, "shared-p3", operation)
    )
    for _ in range(100):
        if "shared-p3" in scheduler._admissions:
            break
        await asyncio.sleep(0)
    assert "shared-p3" in scheduler._admissions
    follower = asyncio.create_task(
        scheduler.run(WorkPriority.PRIVATE_RECONCILE, "shared-p3", operation)
    )
    await asyncio.sleep(0)
    leader.cancel()
    await asyncio.sleep(0)
    assert not follower.done()

    release.set()
    await asyncio.gather(*running)
    assert await queued == "queued"
    with pytest.raises(asyncio.CancelledError):
        await leader
    assert await follower == "shared"
    assert calls == 1
    await scheduler.close()


@pytest.mark.asyncio
async def test_external_worker_cancel_terminates_worker() -> None:
    scheduler = PriorityWorkScheduler(pending_limit=2, worker_count=6)
    await scheduler.start()
    worker = next(iter(scheduler._workers))
    worker.cancel()
    with pytest.raises(asyncio.CancelledError):
        await worker
    assert worker.done()
    await scheduler.close()


@pytest.mark.asyncio
async def test_p0_has_own_active_key_reserve_when_p3_waiters_fill_general_bound() -> None:
    scheduler = PriorityWorkScheduler(pending_limit=1, worker_count=6)
    release = asyncio.Event()
    started = tuple(asyncio.Event() for _ in range(3))

    async def hold(index: int) -> None:
        started[index].set()
        await release.wait()

    running = tuple(
        asyncio.create_task(
            scheduler.run(
                WorkPriority.PRIVATE_RECONCILE,
                f"reserved-bound-running-{index}",
                partial(hold, index),
            )
        )
        for index in range(3)
    )
    await asyncio.gather(*(asyncio.wait_for(event.wait(), timeout=1) for event in started))
    waiters = tuple(
        asyncio.create_task(
            scheduler.run(
                WorkPriority.PRIVATE_RECONCILE,
                f"reserved-bound-waiter-{index}",
                lambda: _return("p3"),
            )
        )
        for index in range(8)
    )
    for _ in range(100):
        if scheduler.snapshot().waiting_critical_count >= 7:
            break
        await asyncio.sleep(0)

    assert (
        await asyncio.wait_for(
            scheduler.run(
                WorkPriority.EMERGENCY_FLATTEN,
                "reserved-bound-p0",
                lambda: _return("flat"),
            ),
            timeout=1,
        )
        == "flat"
    )

    release.set()
    await asyncio.gather(*running, *waiters)
    await scheduler.close()


@pytest.mark.asyncio
async def test_thousand_item_overload_stays_bounded_and_p0_still_runs() -> None:
    scheduler = PriorityWorkScheduler(pending_limit=32, worker_count=6)
    release = asyncio.Event()
    started = (asyncio.Event(), asyncio.Event())

    async def held(index: int) -> None:
        started[index].set()
        await release.wait()

    running = tuple(
        asyncio.create_task(
            scheduler.run(
                WorkPriority.BROAD_BBO_HISTORY,
                f"running-{index}",
                partial(held, index),
            )
        )
        for index in range(2)
    )
    await asyncio.gather(*(asyncio.wait_for(event.wait(), timeout=1) for event in started))
    flooded = tuple(
        asyncio.create_task(
            scheduler.run(
                WorkPriority.BROAD_BBO_HISTORY,
                f"flood-{index:04d}",
                lambda: _return("broad"),
            )
        )
        for index in range(1000)
    )
    for _ in range(20):
        await asyncio.sleep(0)
    assert sum(scheduler.snapshot().queued_by_priority) <= 32
    assert (
        await scheduler.run(
            WorkPriority.EMERGENCY_FLATTEN,
            "overload-flat",
            lambda: _return("flat"),
        )
        == "flat"
    )

    release.set()
    outcomes = await asyncio.gather(*flooded, return_exceptions=True)
    await asyncio.gather(*running)
    assert sum(isinstance(outcome, WorkRejected) for outcome in outcomes) >= 968
    assert scheduler.snapshot().critical_work_count == 0
    await scheduler.close()


@pytest.mark.asyncio
async def test_p0_bypasses_full_lower_critical_queue_into_reserved_lane() -> None:
    scheduler = PriorityWorkScheduler(pending_limit=2, worker_count=6)
    release = asyncio.Event()
    started = [asyncio.Event() for _ in range(3)]

    async def held_p3(index: int) -> None:
        started[index].set()
        await release.wait()

    running = tuple(
        asyncio.create_task(
            scheduler.run(
                WorkPriority.PRIVATE_RECONCILE,
                f"p3-running-{index}",
                partial(held_p3, index),
            )
        )
        for index in range(3)
    )
    await asyncio.gather(*(asyncio.wait_for(event.wait(), timeout=1) for event in started))
    queued = tuple(
        asyncio.create_task(
            scheduler.run(
                WorkPriority.PRIVATE_RECONCILE,
                f"p3-queued-{index}",
                lambda: _return("p3"),
            )
        )
        for index in range(2)
    )
    await asyncio.sleep(0)
    assert sum(scheduler.snapshot().queued_by_priority) == 2
    assert (
        await asyncio.wait_for(
            scheduler.run(
                WorkPriority.EMERGENCY_FLATTEN,
                "urgent-p0",
                lambda: _return("flat"),
            ),
            timeout=1,
        )
        == "flat"
    )

    release.set()
    await asyncio.gather(*running, *queued)
    await scheduler.close()


@pytest.mark.asyncio
async def test_same_key_waiters_recheck_single_flight_after_capacity_wait() -> None:
    scheduler = PriorityWorkScheduler(pending_limit=2, worker_count=6)
    release_general = asyncio.Event()
    general_started = (asyncio.Event(), asyncio.Event())

    async def held_general(index: int) -> None:
        general_started[index].set()
        await release_general.wait()

    general = tuple(
        asyncio.create_task(
            scheduler.run(
                WorkPriority.BROAD_BBO_HISTORY,
                f"held-general-{index}",
                partial(held_general, index),
            )
        )
        for index in range(2)
    )
    await asyncio.gather(*(asyncio.wait_for(event.wait(), timeout=1) for event in general_started))
    release_running = asyncio.Event()
    running_started = asyncio.Event()

    async def held_p0() -> None:
        running_started.set()
        await release_running.wait()

    running = asyncio.create_task(
        scheduler.run(WorkPriority.EMERGENCY_FLATTEN, "occupied-p0", held_p0)
    )
    await asyncio.wait_for(running_started.wait(), timeout=1)
    queued_release = asyncio.Event()
    queued_started = (asyncio.Event(), asyncio.Event())

    async def queued_p0(index: int) -> None:
        queued_started[index].set()
        await queued_release.wait()

    occupied_queue = tuple(
        asyncio.create_task(
            scheduler.run(
                WorkPriority.EMERGENCY_FLATTEN,
                f"queued-p0-{index}",
                partial(queued_p0, index),
            )
        )
        for index in range(2)
    )
    for _ in range(10):
        await asyncio.sleep(0)
        if scheduler.snapshot().queued_by_priority[int(WorkPriority.EMERGENCY_FLATTEN)] == 2:
            break
    assert scheduler.snapshot().queued_by_priority[int(WorkPriority.EMERGENCY_FLATTEN)] == 2
    calls = 0

    async def same_operation() -> str:
        nonlocal calls
        calls += 1
        return "same"

    first = asyncio.create_task(
        scheduler.run(WorkPriority.EMERGENCY_FLATTEN, "same-p0", same_operation)
    )
    second = asyncio.create_task(
        scheduler.run(WorkPriority.EMERGENCY_FLATTEN, "same-p0", same_operation)
    )
    for _ in range(10):
        await asyncio.sleep(0)
        if scheduler.snapshot().waiting_critical_count == 1:
            break
    assert scheduler.snapshot().waiting_critical_count == 1

    release_running.set()
    await asyncio.wait_for(queued_started[0].wait(), timeout=1)
    queued_release.set()
    assert tuple(await asyncio.gather(first, second)) == ("same", "same")
    assert calls == 1
    await asyncio.gather(running, *occupied_queue)
    release_general.set()
    await asyncio.gather(*general)
    await scheduler.close()


@pytest.mark.asyncio
async def test_operation_cancelled_error_does_not_destroy_reserved_lane() -> None:
    scheduler = PriorityWorkScheduler(pending_limit=2, worker_count=6)

    async def self_cancel() -> None:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await scheduler.run(WorkPriority.EMERGENCY_FLATTEN, "cancelled-p0", self_cancel)
    assert sum(worker.done() for worker in scheduler._workers) == 0
    assert (
        await scheduler.run(
            WorkPriority.EMERGENCY_FLATTEN,
            "next-p0",
            lambda: _return("flat"),
        )
        == "flat"
    )
    await scheduler.close()


async def _return(value: str) -> str:
    return value
