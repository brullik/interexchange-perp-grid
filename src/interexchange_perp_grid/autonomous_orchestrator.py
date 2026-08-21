from __future__ import annotations

import asyncio
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from interexchange_perp_grid.config import Settings
from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.qualification import (
    QualificationPolicy,
    QualificationProgress,
    build_qualification_progress,
    code_hash,
    config_hash,
)
from interexchange_perp_grid.state import (
    QualificationEpochStatus,
    finalize_qualification_epoch,
    initialise_state,
    read_active_qualification_epoch,
    read_qualification_epoch,
    start_qualification_epoch,
)
from interexchange_perp_grid.strategy import DirectedRouteKey

_RELEASE_SHA = re.compile(r"^[0-9a-f]{40}$")
_IMAGE_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_LOCKED_QUALIFICATION_ROUTE = "BTC:bybit>okx"


@dataclass(frozen=True, slots=True)
class AutonomousRuntimeStatus:
    schema_version: int
    updated_at: datetime
    state: str
    release_sha: str | None
    image_digest: str | None
    route: str | None
    epoch_id: str | None
    epoch_status: str | None
    completion_ratio: Decimal
    blockers: tuple[str, ...]
    next_action: str
    live_authorized: bool = False


@dataclass(frozen=True, slots=True)
class _ProgressSnapshot:
    ready_to_finalize: bool
    completion_ratio: Decimal
    blockers: tuple[str, ...]


def _parse_route(raw: str) -> DirectedRouteKey:
    base, separator, venues = raw.strip().partition(":")
    long_venue, direction, short_venue = venues.partition(">")
    if not separator or not direction:
        raise ValueError("qualification route must use BASE:long_venue>short_venue")
    return DirectedRouteKey(
        base=base.upper(),
        long_venue=Venue(long_venue.lower()),
        short_venue=Venue(short_venue.lower()),
    )


def _policy(settings: Settings) -> QualificationPolicy:
    shadow = settings.shadow
    return QualificationPolicy(
        minimum_duration_seconds=shadow.qualification_min_duration_seconds,
        minimum_synchronised_snapshots_per_venue=(
            shadow.qualification_min_synchronised_snapshots_per_venue
        ),
        minimum_funding_checkpoints_per_venue=(
            shadow.qualification_min_funding_checkpoints_per_venue
        ),
        maximum_inter_snapshot_gap_seconds=(shadow.qualification_max_inter_snapshot_gap_seconds),
        maximum_sequence_gaps=shadow.qualification_max_sequence_gaps,
        maximum_stale_snapshots=shadow.qualification_max_stale_snapshots,
        maximum_sequence_unknown_snapshots=(shadow.qualification_max_sequence_unknown_snapshots),
        maximum_clock_skew_snapshots=shadow.qualification_max_clock_skew_snapshots,
        maximum_clock_skew_ms=settings.market_data.max_clock_skew_ms,
        maximum_snapshot_age_ms=settings.market_data.max_l2_age_ms,
    )


def _write_status_sync(path: Path, status: AutonomousRuntimeStatus) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    payload = json.dumps(asdict(status), default=str, sort_keys=True) + "\n"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def load_autonomous_runtime_status(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise ValueError("autonomous runtime status is invalid")
    return payload


def _build_progress_sync(
    state_path: Path,
    data_root: Path,
    epoch_id: str,
    policy: QualificationPolicy,
) -> QualificationProgress:
    # Parquet inspection is intentionally isolated from the latency-sensitive service loop.
    return asyncio.run(build_qualification_progress(state_path, data_root, epoch_id, policy))


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except TimeoutError:
        process.kill()
        await process.wait()


async def _progress_from_subprocess(
    repo_root: Path,
    config_path: Path,
    epoch_id: str,
) -> _ProgressSnapshot:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "interexchange_perp_grid.cli",
        "qualification-epoch-status",
        "--epoch-id",
        epoch_id,
        "--config",
        str(config_path.resolve()),
        cwd=repo_root.resolve(),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=120)
    except (TimeoutError, asyncio.CancelledError):
        await _terminate_process(process)
        raise
    if process.returncode != 0:
        message = stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"qualification progress worker failed: {message}")
    payload = json.loads(stdout.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("qualification progress worker returned invalid JSON")
    blockers = payload.get("blockers")
    if not isinstance(blockers, list) or not all(isinstance(item, str) for item in blockers):
        raise ValueError("qualification progress blockers are invalid")
    return _ProgressSnapshot(
        ready_to_finalize=payload.get("ready_to_finalize") is True,
        completion_ratio=Decimal(str(payload.get("completion_ratio", "0"))),
        blockers=tuple(blockers),
    )


@dataclass(slots=True)
class AutonomousOrchestrator:
    settings: Settings
    repo_root: Path = Path(".")
    config_path: Path = Path("config/defaults.yaml")
    poll_interval_seconds: float = 5.0
    status_path: Path | None = None
    use_progress_subprocess: bool = True

    @property
    def state_path(self) -> Path:
        return Path(self.settings.storage.sqlite_path)

    @property
    def runtime_status_path(self) -> Path:
        return self.status_path or self.state_path.parent / "autonomous-orchestrator.json"

    async def _publish(self, status: AutonomousRuntimeStatus) -> AutonomousRuntimeStatus:
        await asyncio.to_thread(_write_status_sync, self.runtime_status_path, status)
        return status

    async def reconcile_once(self) -> AutonomousRuntimeStatus:
        now = datetime.now(UTC)
        release_sha = os.environ.get("IPEG_RELEASE_SHA", "").strip().lower()
        image_digest = os.environ.get("IPEG_CONTAINER_IMAGE_DIGEST", "").strip().lower()
        raw_route = os.environ.get("IPEG_QUALIFICATION_ROUTE", "").strip()
        if not _RELEASE_SHA.fullmatch(release_sha) or not _IMAGE_DIGEST.fullmatch(image_digest):
            return await self._publish(
                AutonomousRuntimeStatus(
                    1,
                    now,
                    "WAITING_DEPLOYMENT_IDENTITY",
                    release_sha or None,
                    image_digest or None,
                    raw_route or None,
                    None,
                    None,
                    Decimal(0),
                    ("EXACT_DEPLOYMENT_IDENTITY_REQUIRED",),
                    "Deploy one exact immutable image digest before qualification.",
                )
            )
        if os.environ.get("IPEG_OWNER_ONBOARDING_CONFIRMED", "").strip().lower() != "true":
            return await self._publish(
                AutonomousRuntimeStatus(
                    1,
                    now,
                    "WAITING_OWNER_ONBOARDING",
                    release_sha,
                    image_digest,
                    raw_route or None,
                    None,
                    None,
                    Decimal(0),
                    ("OWNER_ONBOARDING_CONFIRMATION_REQUIRED",),
                    "Complete local owner onboarding; live remains disabled.",
                )
            )
        if not raw_route:
            return await self._publish(
                AutonomousRuntimeStatus(
                    1,
                    now,
                    "WAITING_OWNER_ONBOARDING",
                    release_sha,
                    image_digest,
                    None,
                    None,
                    None,
                    Decimal(0),
                    ("QUALIFICATION_ROUTE_REQUIRED",),
                    "Complete local owner onboarding; live remains disabled.",
                )
            )
        if raw_route != _LOCKED_QUALIFICATION_ROUTE:
            return await self._publish(
                AutonomousRuntimeStatus(
                    1,
                    now,
                    "WAITING_OWNER_ONBOARDING",
                    release_sha,
                    image_digest,
                    raw_route,
                    None,
                    None,
                    Decimal(0),
                    ("QUALIFICATION_ROUTE_LOCK_MISMATCH",),
                    "Restore the locked onboarding route; live remains disabled.",
                )
            )
        route = _parse_route(raw_route)
        await initialise_state(self.state_path)
        source_sha256 = code_hash(self.repo_root.resolve())
        config_sha256 = config_hash(self.config_path.resolve())
        active = await read_active_qualification_epoch(self.state_path)
        identity = (
            route,
            release_sha,
            source_sha256,
            config_sha256,
            image_digest,
        )
        if (
            active is None
            or (
                active.route,
                active.release_sha,
                active.source_sha256,
                active.config_sha256,
                active.container_image_digest,
            )
            != identity
        ):
            latest = await read_qualification_epoch(self.state_path)
            if latest is not None and (
                latest.status == QualificationEpochStatus.FINALIZED
                and (
                    latest.route,
                    latest.release_sha,
                    latest.source_sha256,
                    latest.config_sha256,
                    latest.container_image_digest,
                )
                == identity
            ):
                return await self._publish(
                    AutonomousRuntimeStatus(
                        1,
                        now,
                        "QUALIFICATION_FINALIZED",
                        release_sha,
                        image_digest,
                        route.value,
                        latest.epoch_id,
                        latest.status.value,
                        Decimal(1),
                        ("REPLAY_AND_RUNTIME_EVIDENCE_REQUIRED",),
                        "Build exact replay/runtime evidence; do not arm canary without consent.",
                    )
                )
            active = await start_qualification_epoch(self.state_path, *identity)
        if self.use_progress_subprocess:
            progress = await _progress_from_subprocess(
                self.repo_root,
                self.config_path,
                active.epoch_id,
            )
        else:
            observed = await asyncio.to_thread(
                _build_progress_sync,
                self.state_path,
                Path(self.settings.storage.parquet_dir),
                active.epoch_id,
                _policy(self.settings),
            )
            progress = _ProgressSnapshot(
                observed.ready_to_finalize,
                observed.completion_ratio,
                observed.blockers,
            )
        if progress.ready_to_finalize:
            finalized = await finalize_qualification_epoch(self.state_path, active.epoch_id)
            return await self._publish(
                AutonomousRuntimeStatus(
                    1,
                    now,
                    "QUALIFICATION_FINALIZED",
                    release_sha,
                    image_digest,
                    route.value,
                    finalized.epoch_id,
                    finalized.status.value,
                    progress.completion_ratio,
                    tuple(progress.blockers),
                    "Build exact replay/runtime evidence; do not arm canary without consent.",
                )
            )
        return await self._publish(
            AutonomousRuntimeStatus(
                1,
                now,
                "QUALIFICATION_EPOCH",
                release_sha,
                image_digest,
                route.value,
                active.epoch_id,
                active.status.value,
                progress.completion_ratio,
                tuple(progress.blockers),
                "Continue immutable shadow qualification and recovery monitoring.",
            )
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await self.reconcile_once()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.poll_interval_seconds)
            except TimeoutError:
                continue
