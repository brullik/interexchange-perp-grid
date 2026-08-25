from __future__ import annotations

import asyncio
import contextlib
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

from interexchange_perp_grid.config import Settings
from interexchange_perp_grid.live_journal import (
    LiveOrderJournal,
    completed_normal_actions_sha256,
    is_completed_normal_paired_cycle,
)
from interexchange_perp_grid.qualification import (
    LAPTOP_OWNER_EXCEPTION_SCAN_INTERVAL_SECONDS,
    LAPTOP_SMOKE_SCAN_INTERVAL_SECONDS,
    QualificationEvidence,
    QualificationPolicy,
    laptop_owner_exception_policy,
    laptop_smoke_policy,
    qualification_policy_from_settings,
)
from interexchange_perp_grid.service import BootstrapService, BoundedServiceReceipt
from interexchange_perp_grid.state import (
    QualificationEpoch,
    QualificationEpochStatus,
    initialise_state,
    read_qualification_epoch,
    read_service_health,
)
from interexchange_perp_grid.strategy import DirectedRouteKey


@dataclass(frozen=True, slots=True)
class LaptopQualificationIdentity:
    route: DirectedRouteKey
    release_sha: str
    runtime_artifact_digest: str


@dataclass(frozen=True, slots=True)
class LaptopPilotReport:
    schema_version: int
    status: str
    started_at: datetime
    ended_at: datetime
    post_trade_observation_seconds: int
    qualification_hash: str
    runtime_artifact_digest: str
    completed_pair_action_ids: tuple[str, ...]
    completed_pair_actions_sha256: str
    production_filled_order_count: int
    active_pair_action_ids: tuple[str, ...]
    service_starts: int
    blockers: tuple[str, ...]
    live_authorized: bool = False


async def _stop_service(
    stop_event: asyncio.Event,
    service: asyncio.Task[None],
) -> None:
    stop_event.set()
    try:
        await asyncio.wait_for(asyncio.shield(service), timeout=30)
    except TimeoutError as error:
        service.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await service
        raise RuntimeError("native qualification service did not stop within 30 seconds") from error


async def run_until_qualification_finalized(
    settings: Settings,
    identity: LaptopQualificationIdentity,
    *,
    maximum_seconds: float = 108_000,
    poll_interval_seconds: float = 5,
    qualification_policy: QualificationPolicy | None = None,
) -> QualificationEpoch:
    selected_policy = qualification_policy or qualification_policy_from_settings(settings)
    if not selected_policy.minimum_duration_seconds <= maximum_seconds <= 108_000:
        raise ValueError(
            "native qualification deadline must cover its policy and stay within 30 hours"
        )
    if not 0 < poll_interval_seconds <= 60:
        raise ValueError("native qualification polling interval is invalid")
    service_settings = settings
    if selected_policy == laptop_owner_exception_policy(settings):
        service_settings = settings.model_copy(
            update={
                "shadow": settings.shadow.model_copy(
                    update={
                        "scan_interval_seconds": LAPTOP_OWNER_EXCEPTION_SCAN_INTERVAL_SECONDS,
                        "qualification_min_duration_seconds": (
                            selected_policy.minimum_duration_seconds
                        ),
                        "qualification_min_synchronised_snapshots_per_venue": (
                            selected_policy.minimum_synchronised_snapshots_per_venue
                        ),
                        "qualification_min_funding_checkpoints_per_venue": (
                            selected_policy.minimum_funding_checkpoints_per_venue
                        ),
                    }
                )
            }
        )
    elif selected_policy in (
        laptop_smoke_policy(settings, 5),
        laptop_smoke_policy(settings, 30),
    ):
        service_settings = settings.model_copy(
            update={
                "shadow": settings.shadow.model_copy(
                    update={
                        "scan_interval_seconds": LAPTOP_SMOKE_SCAN_INTERVAL_SECONDS,
                        "qualification_min_duration_seconds": (
                            selected_policy.minimum_duration_seconds
                        ),
                        "qualification_min_synchronised_snapshots_per_venue": (
                            selected_policy.minimum_synchronised_snapshots_per_venue
                        ),
                        "qualification_min_funding_checkpoints_per_venue": (
                            selected_policy.minimum_funding_checkpoints_per_venue
                        ),
                    }
                )
            }
        )
    state_path = Path(settings.storage.sqlite_path)
    # A smoke run always has a fresh per-run database. Initialise it before
    # scheduling the service task so the first epoch read cannot win the event-loop race.
    await initialise_state(state_path)
    stop_event = asyncio.Event()
    service = asyncio.create_task(
        BootstrapService(service_settings, qualification_policy=selected_policy).run(stop_event),
        name="native-laptop-qualification-service",
    )
    deadline = asyncio.get_running_loop().time() + maximum_seconds
    try:
        while True:
            if service.done():
                await service
                raise RuntimeError("native qualification service stopped before finalization")
            epoch = await read_qualification_epoch(state_path)
            if (
                epoch is not None
                and epoch.status == QualificationEpochStatus.FINALIZED
                and epoch.route == identity.route
                and epoch.release_sha == identity.release_sha
                and epoch.container_image_digest == identity.runtime_artifact_digest
            ):
                await _stop_service(stop_event, service)
                return epoch
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise TimeoutError("native qualification did not finalize within 30 hours")
            await asyncio.sleep(min(poll_interval_seconds, remaining))
    finally:
        if not service.done():
            await _stop_service(stop_event, service)


async def build_laptop_pilot_report(
    settings: Settings,
    qualification: QualificationEvidence,
    started_at: datetime,
    ended_at: datetime,
    runtime_artifact_digest: str,
    bounded_service: BoundedServiceReceipt,
) -> LaptopPilotReport:
    if (
        started_at.tzinfo is None
        or started_at.utcoffset() is None
        or ended_at.tzinfo is None
        or ended_at.utcoffset() is None
        or ended_at < started_at
    ):
        raise ValueError("laptop pilot timestamps must be ordered and timezone-aware")
    journal = LiveOrderJournal(Path(settings.storage.sqlite_path))
    await journal.initialise()
    active = await journal.active_actions()
    completed = await journal.completed_actions_since(started_at, qualification.qualification_hash)
    normal = tuple(action for action in completed if is_completed_normal_paired_cycle(action))
    blockers: list[str] = []
    if active:
        blockers.append("ACTIVE_LIVE_ACTION_REMAINS")
    if len(completed) != 1 or len(normal) != 1:
        blockers.append("EXACTLY_ONE_COMPLETED_PAIRED_CANARY_REQUIRED")
    post_trade_seconds = 0
    if normal:
        post_trade_seconds = max(
            0,
            int((bounded_service.ended_at - normal[0].updated_at).total_seconds()),
        )
    if post_trade_seconds < 28_800:
        blockers.append("EIGHT_HOUR_POST_TRADE_OBSERVATION_REQUIRED")
    if qualification.container_image_digest != runtime_artifact_digest:
        blockers.append("RUNTIME_ARTIFACT_IDENTITY_MISMATCH")
    if (
        Path(bounded_service.state_path).resolve() != Path(settings.storage.sqlite_path).resolve()
        or bounded_service.started_at > started_at
        or bounded_service.ended_at > ended_at
        or (ended_at - bounded_service.ended_at).total_seconds() > 300
        or bounded_service.requested_seconds < 28_800
        or bounded_service.observed_monotonic_seconds < bounded_service.requested_seconds
    ):
        blockers.append("BOUNDED_SERVICE_RECEIPT_MISMATCH")
    health = await read_service_health(
        Path(settings.storage.sqlite_path),
        settings.app.health_max_age_seconds,
        ended_at,
    )
    if health.status != "stopped" or health.starts < 1:
        blockers.append("BOUNDED_SERVICE_STOP_EVIDENCE_MISSING")
    return LaptopPilotReport(
        schema_version=1,
        status="PASS" if not blockers else "FAIL",
        started_at=started_at,
        ended_at=ended_at,
        post_trade_observation_seconds=post_trade_seconds,
        qualification_hash=qualification.qualification_hash,
        runtime_artifact_digest=runtime_artifact_digest,
        completed_pair_action_ids=tuple(action.pair_action_id for action in normal),
        completed_pair_actions_sha256=completed_normal_actions_sha256(normal),
        production_filled_order_count=sum(len(action.legs) for action in normal),
        active_pair_action_ids=tuple(action.pair_action_id for action in active),
        service_starts=health.starts,
        blockers=tuple(blockers),
    )


def write_laptop_pilot_report(path: Path, report: LaptopPilotReport) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        temporary.write_text(
            json.dumps(asdict(report), default=str, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
