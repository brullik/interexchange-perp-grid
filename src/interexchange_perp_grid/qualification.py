from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from itertools import pairwise
from pathlib import Path
from typing import Any

import duckdb

from interexchange_perp_grid.config import Settings
from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.execution import PairActionState
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.state import QualificationEpochStatus
from interexchange_perp_grid.strategy import DirectedRouteKey

_IMAGE_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_COMMIT_SHA = re.compile(r"^[0-9a-f]{40}$")
LAPTOP_OWNER_EXCEPTION_MINIMUM_DURATION_SECONDS = 43_200
LAPTOP_OWNER_EXCEPTION_SCAN_INTERVAL_SECONDS = 3
LAPTOP_OWNER_EXCEPTION_CONFIRMATION = "I_ACCEPT_LAPTOP_12H_QUALIFICATION_EXCEPTION"
LAPTOP_OWNER_EXCEPTION_ENV = "IPEG_LAPTOP_12H_OWNER_EXCEPTION"


@dataclass(frozen=True, slots=True)
class FundingCheckpoint:
    venue: Venue
    observed_at: datetime
    rate: Decimal
    next_funding_timestamp_ms: int
    interval: str


@dataclass(frozen=True, slots=True)
class ReplayShadowStatistics:
    replay_completed: bool
    accepted_signals: int
    rejected_signals: int
    simulated_net_pnl_usdt: Decimal
    maximum_adverse_excursion_usdt: Decimal
    unresolved_order_count: int
    unresolved_exposure_count: int
    unhandled_exception_count: int

    def __post_init__(self) -> None:
        counts = (
            self.accepted_signals,
            self.rejected_signals,
            self.unresolved_order_count,
            self.unresolved_exposure_count,
            self.unhandled_exception_count,
        )
        if any(value < 0 for value in counts):
            raise ValueError("qualification counters must be non-negative")
        if not self.simulated_net_pnl_usdt.is_finite():
            raise ValueError("simulated net PnL must be finite")
        if (
            not self.maximum_adverse_excursion_usdt.is_finite()
            or self.maximum_adverse_excursion_usdt < 0
        ):
            raise ValueError("maximum adverse excursion must be non-negative and finite")


@dataclass(frozen=True, slots=True)
class QualifiedStrategyParameters:
    calibration_version: int
    size_bucket_base_quantity: Decimal
    adaptive_entry_threshold_bps: Decimal
    target_exit_spread_bps: Decimal
    minimum_profit_usdt: Decimal
    stressed_cost_multiplier: Decimal
    expected_holding_seconds: int
    maximum_holding_seconds: int

    def __post_init__(self) -> None:
        if self.calibration_version < 1:
            raise ValueError("calibration version must be positive")
        decimals = (
            self.size_bucket_base_quantity,
            self.adaptive_entry_threshold_bps,
            self.minimum_profit_usdt,
        )
        if any(not value.is_finite() or value <= 0 for value in decimals):
            raise ValueError("qualified strategy values must be positive finite decimals")
        if not self.target_exit_spread_bps.is_finite():
            raise ValueError("target exit spread must be finite")
        if self.stressed_cost_multiplier < 1 or not self.stressed_cost_multiplier.is_finite():
            raise ValueError("stressed cost multiplier must be at least one")
        if not 0 < self.expected_holding_seconds <= self.maximum_holding_seconds <= 86_400:
            raise ValueError("invalid qualified holding horizon")


@dataclass(frozen=True, slots=True)
class QualificationRuntimeEvidence:
    epoch_id: str
    epoch_started_at: datetime
    epoch_ended_at: datetime
    epoch_status: QualificationEpochStatus
    route: DirectedRouteKey
    release_code_sha: str
    source_sha256: str
    config_sha256: str
    container_image_digest: str
    private_taker_fee_rates: dict[Venue, Decimal]
    funding_checkpoints: tuple[FundingCheckpoint, ...]
    replay_shadow: ReplayShadowStatistics
    strategy: QualifiedStrategyParameters

    def __post_init__(self) -> None:
        if not self.epoch_id.strip():
            raise ValueError("qualification epoch ID must be non-empty")
        if self.epoch_status != QualificationEpochStatus.FINALIZED:
            raise ValueError("qualification runtime evidence requires a finalized epoch")
        if self.epoch_ended_at < self.epoch_started_at:
            raise ValueError("qualification epoch end precedes start")
        if not _COMMIT_SHA.fullmatch(self.release_code_sha):
            raise ValueError("release code SHA must be an exact 40-character commit SHA")
        if not re.fullmatch(r"[0-9a-f]{64}", self.source_sha256):
            raise ValueError("qualification source hash must be SHA-256")
        if not re.fullmatch(r"[0-9a-f]{64}", self.config_sha256):
            raise ValueError("qualification config hash must be SHA-256")
        if not _IMAGE_DIGEST.fullmatch(self.container_image_digest):
            raise ValueError("container image digest must be sha256:<64 lowercase hex>")
        route_venues = {self.route.long_venue, self.route.short_venue}
        if set(self.private_taker_fee_rates) != route_venues:
            raise ValueError("private fee rates must cover the exact directed route")
        if any(
            not value.is_finite() or value < 0 for value in self.private_taker_fee_rates.values()
        ):
            raise ValueError("private taker fees must be non-negative finite decimals")


@dataclass(frozen=True, slots=True)
class QualificationPolicy:
    minimum_duration_seconds: int = 86_400
    minimum_synchronised_snapshots_per_venue: int = 10_000
    minimum_funding_checkpoints_per_venue: int = 3
    maximum_inter_snapshot_gap_seconds: int = 60
    maximum_sequence_gaps: int = 0
    maximum_stale_snapshots: int = 0
    maximum_sequence_unknown_snapshots: int = 0
    maximum_clock_skew_snapshots: int = 0
    maximum_clock_skew_ms: int = 1_000
    maximum_snapshot_age_ms: int = 1_000

    def __post_init__(self) -> None:
        positive = (
            self.minimum_duration_seconds,
            self.minimum_synchronised_snapshots_per_venue,
            self.minimum_funding_checkpoints_per_venue,
            self.maximum_inter_snapshot_gap_seconds,
            self.maximum_clock_skew_ms,
            self.maximum_snapshot_age_ms,
        )
        non_negative = (
            self.maximum_sequence_gaps,
            self.maximum_stale_snapshots,
            self.maximum_sequence_unknown_snapshots,
            self.maximum_clock_skew_snapshots,
        )
        if any(value <= 0 for value in positive) or any(value < 0 for value in non_negative):
            raise ValueError("invalid qualification policy")


LAPTOP_SMOKE_MINIMUM_DURATION_SECONDS = 1_800
LAPTOP_SMOKE_MINIMUM_SYNCHRONISED_SNAPSHOTS_PER_VENUE = 500
LAPTOP_SMOKE_SCAN_INTERVAL_SECONDS = 2


def qualification_policy_from_settings(settings: Settings) -> QualificationPolicy:
    """Build the standard, repository-locked qualification policy."""
    return QualificationPolicy(
        minimum_duration_seconds=settings.shadow.qualification_min_duration_seconds,
        minimum_synchronised_snapshots_per_venue=(
            settings.shadow.qualification_min_synchronised_snapshots_per_venue
        ),
        minimum_funding_checkpoints_per_venue=(
            settings.shadow.qualification_min_funding_checkpoints_per_venue
        ),
        maximum_inter_snapshot_gap_seconds=(
            settings.shadow.qualification_max_inter_snapshot_gap_seconds
        ),
        maximum_sequence_gaps=settings.shadow.qualification_max_sequence_gaps,
        maximum_stale_snapshots=settings.shadow.qualification_max_stale_snapshots,
        maximum_sequence_unknown_snapshots=(
            settings.shadow.qualification_max_sequence_unknown_snapshots
        ),
        maximum_clock_skew_snapshots=settings.shadow.qualification_max_clock_skew_snapshots,
        maximum_clock_skew_ms=settings.market_data.max_clock_skew_ms,
        maximum_snapshot_age_ms=settings.market_data.max_l2_age_ms,
    )


def laptop_owner_exception_policy(settings: Settings) -> QualificationPolicy:
    """Return the one allowed laptop-only policy delta; VPS/default stays at 24 hours."""
    standard = qualification_policy_from_settings(settings)
    if standard.minimum_duration_seconds != 86_400:
        raise ValueError("laptop exception requires the exact standard 24-hour policy")
    return replace(
        standard,
        minimum_duration_seconds=LAPTOP_OWNER_EXCEPTION_MINIMUM_DURATION_SECONDS,
    )


def laptop_smoke_policy(settings: Settings) -> QualificationPolicy:
    """Return a non-accepting 30-minute operational rehearsal policy."""
    standard = qualification_policy_from_settings(settings)
    if standard.minimum_duration_seconds != 86_400:
        raise ValueError("laptop smoke requires the exact standard 24-hour policy")
    return replace(
        standard,
        minimum_duration_seconds=LAPTOP_SMOKE_MINIMUM_DURATION_SECONDS,
        minimum_synchronised_snapshots_per_venue=(
            LAPTOP_SMOKE_MINIMUM_SYNCHRONISED_SNAPSHOTS_PER_VENUE
        ),
    )


def laptop_owner_exception_authorized(
    environ: Mapping[str, str] | None = None,
    *,
    platform: str | None = None,
) -> bool:
    """Require an explicit local Windows receipt; it is not a live-order consent."""
    observed = environ if environ is not None else os.environ
    return (platform or sys.platform) == "win32" and observed.get(
        LAPTOP_OWNER_EXCEPTION_ENV, ""
    ) == LAPTOP_OWNER_EXCEPTION_CONFIRMATION


@dataclass(frozen=True, slots=True)
class VenueBookStatistics:
    venue: Venue
    unique_order_book_events: int
    synchronised_snapshots: int
    first_observed_at: datetime | None
    last_observed_at: datetime | None
    observation_period_seconds: Decimal
    sequence_gap_count: int
    maximum_sequence_gap: int
    maximum_inter_snapshot_gap_seconds: Decimal
    stale_snapshot_count: int
    sequence_unknown_snapshot_count: int
    unsynchronised_snapshot_count: int
    clock_skew_snapshot_count: int
    maximum_absolute_clock_skew_ms: int


@dataclass(frozen=True, slots=True)
class VenueQualificationProgress:
    venue: Venue
    synchronised_snapshots: int
    required_synchronised_snapshots: int
    remaining_synchronised_snapshots: int
    funding_checkpoints: int
    required_funding_checkpoints: int
    remaining_funding_checkpoints: int
    sequence_gap_count: int
    stale_snapshot_count: int
    sequence_unknown_snapshot_count: int
    unsynchronised_snapshot_count: int
    clock_skew_snapshot_count: int
    maximum_inter_snapshot_gap_seconds: Decimal


@dataclass(frozen=True, slots=True)
class QualificationProgress:
    schema_version: int
    generated_at: datetime
    epoch_id: str
    epoch_status: QualificationEpochStatus
    route: DirectedRouteKey
    release_sha: str
    source_sha256: str
    config_sha256: str
    container_image_digest: str
    elapsed_seconds: Decimal
    required_duration_seconds: int
    remaining_duration_seconds: Decimal
    completion_ratio: Decimal
    accepted_signals: int
    rejected_signals: int
    unhandled_exception_count: int
    replay_completed: bool
    unresolved_order_count: int
    unresolved_exposure_count: int
    simulated_net_pnl_usdt: Decimal
    venues: tuple[VenueQualificationProgress, ...]
    blockers: tuple[str, ...]
    ready_to_finalize: bool
    qualification_ready: bool


@dataclass(frozen=True, slots=True)
class QualificationEvidence:
    schema_version: int
    generated_at: datetime
    epoch_id: str
    epoch_started_at: datetime
    epoch_ended_at: datetime
    epoch_status: QualificationEpochStatus
    route: DirectedRouteKey | None
    code_commit_sha: str
    code_sha256: str
    config_sha256: str
    data_sha256: str
    data_manifest: dict[str, str]
    container_image_digest: str
    venue_statistics: tuple[VenueBookStatistics, ...]
    route_observation_period_seconds: Decimal
    private_taker_fee_rates: dict[Venue, Decimal]
    funding_checkpoint_counts: dict[Venue, int]
    funding_intervals: dict[Venue, tuple[str, ...]]
    replay_shadow: ReplayShadowStatistics | None
    strategy: QualifiedStrategyParameters | None
    policy: QualificationPolicy
    accepted: bool
    blockers: tuple[str, ...]
    reason: ReasonCode
    qualification_hash: str


@dataclass(frozen=True, slots=True)
class _BookEvent:
    event_id: str
    received_at: datetime
    exchange_timestamp_ms: int | None
    sequence_start: int | None
    sequence_end: int | None
    is_snapshot: bool
    synchronised: bool
    clock_skew_ms: int | None


def _hash_files(files: tuple[Path, ...], relative_to: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(files, key=lambda item: item.as_posix()):
        digest.update(path.relative_to(relative_to).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def code_hash(repo_root: Path) -> str:
    source_root = repo_root / "src"
    return _hash_files(tuple(source_root.rglob("*.py")), repo_root)


def config_hash(config_path: Path) -> str:
    return hashlib.sha256(config_path.read_bytes()).hexdigest()


def data_hash(data_root: Path) -> str:
    return _manifest_hash(_data_manifest(data_root))


def _data_manifest(data_root: Path) -> dict[str, str]:
    files = tuple(data_root.rglob("*.parquet")) if data_root.is_dir() else ()
    return {
        path.relative_to(data_root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(files, key=lambda item: item.as_posix())
    }


def _manifest_hash(manifest: dict[str, str]) -> str:
    encoded = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _manifest_is_current(data_root: Path, manifest: dict[str, str]) -> bool:
    root = data_root.resolve()
    for relative, expected_hash in manifest.items():
        relative_path = Path(relative)
        if relative_path.is_absolute() or ".." in relative_path.parts:
            return False
        path = (root / relative_path).resolve()
        if root not in path.parents or not path.is_file():
            return False
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected_hash:
            return False
    return bool(manifest)


def current_code_commit_sha(repo_root: Path) -> str | None:
    configured = os.environ.get("IPEG_RELEASE_SHA", "").strip().lower()
    if _COMMIT_SHA.fullmatch(configured):
        return configured
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    observed = completed.stdout.strip().lower()
    return observed if _COMMIT_SHA.fullmatch(observed) else None


def load_runtime_evidence(path: Path) -> QualificationRuntimeEvidence:
    payload = json.loads(path.read_text(encoding="utf-8"))
    route_payload = _require_mapping(payload, "route")
    route = DirectedRouteKey(
        str(route_payload["base"]).upper(),
        Venue(str(route_payload["long_venue"])),
        Venue(str(route_payload["short_venue"])),
    )
    fees_payload = _require_mapping(payload, "private_taker_fee_rates")
    checkpoints_payload = payload.get("funding_checkpoints")
    if not isinstance(checkpoints_payload, list):
        raise ValueError("funding_checkpoints must be a list")
    replay_payload = _require_mapping(payload, "replay_shadow")
    return QualificationRuntimeEvidence(
        epoch_id=str(payload["epoch_id"]),
        epoch_started_at=datetime.fromisoformat(str(payload["epoch_started_at"])),
        epoch_ended_at=datetime.fromisoformat(str(payload["epoch_ended_at"])),
        epoch_status=QualificationEpochStatus(str(payload["epoch_status"])),
        route=route,
        release_code_sha=str(payload["release_code_sha"]).lower(),
        source_sha256=str(payload["source_sha256"]).lower(),
        config_sha256=str(payload["config_sha256"]).lower(),
        container_image_digest=str(payload["container_image_digest"]).lower(),
        private_taker_fee_rates={
            Venue(str(venue)): Decimal(str(rate)) for venue, rate in fees_payload.items()
        },
        funding_checkpoints=tuple(
            FundingCheckpoint(
                Venue(str(item["venue"])),
                datetime.fromisoformat(str(item["observed_at"])),
                Decimal(str(item["rate"])),
                int(item["next_funding_timestamp_ms"]),
                str(item["interval"]),
            )
            for item in checkpoints_payload
            if isinstance(item, dict)
        ),
        replay_shadow=ReplayShadowStatistics(
            replay_completed=bool(replay_payload["replay_completed"]),
            accepted_signals=int(replay_payload["accepted_signals"]),
            rejected_signals=int(replay_payload["rejected_signals"]),
            simulated_net_pnl_usdt=Decimal(str(replay_payload["simulated_net_pnl_usdt"])),
            maximum_adverse_excursion_usdt=Decimal(
                str(replay_payload["maximum_adverse_excursion_usdt"])
            ),
            unresolved_order_count=int(replay_payload["unresolved_order_count"]),
            unresolved_exposure_count=int(replay_payload["unresolved_exposure_count"]),
            unhandled_exception_count=int(replay_payload["unhandled_exception_count"]),
        ),
        strategy=QualifiedStrategyParameters(
            calibration_version=int(_require_mapping(payload, "strategy")["calibration_version"]),
            size_bucket_base_quantity=Decimal(
                str(_require_mapping(payload, "strategy")["size_bucket_base_quantity"])
            ),
            adaptive_entry_threshold_bps=Decimal(
                str(_require_mapping(payload, "strategy")["adaptive_entry_threshold_bps"])
            ),
            target_exit_spread_bps=Decimal(
                str(_require_mapping(payload, "strategy")["target_exit_spread_bps"])
            ),
            minimum_profit_usdt=Decimal(
                str(_require_mapping(payload, "strategy")["minimum_profit_usdt"])
            ),
            stressed_cost_multiplier=Decimal(
                str(_require_mapping(payload, "strategy")["stressed_cost_multiplier"])
            ),
            expected_holding_seconds=int(
                _require_mapping(payload, "strategy")["expected_holding_seconds"]
            ),
            maximum_holding_seconds=int(
                _require_mapping(payload, "strategy")["maximum_holding_seconds"]
            ),
        ),
    )


def write_runtime_evidence(
    evidence: QualificationRuntimeEvidence,
    path: Path,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(
        json.dumps(asdict(evidence), default=str, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _require_mapping(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{key} must be a mapping")
    return value


def run_qualification(
    repo_root: Path,
    config_path: Path,
    data_root: Path,
    evidence_path: Path,
    minimum_samples: int | None = None,
    now: datetime | None = None,
    *,
    runtime_evidence: QualificationRuntimeEvidence | None = None,
    policy: QualificationPolicy | None = None,
) -> QualificationEvidence:
    """Create exact-route evidence; a Parquet row count can never qualify a release."""
    selected_policy = policy or QualificationPolicy()
    if minimum_samples is not None:
        if minimum_samples <= 0:
            raise ValueError("qualification minimum event count must be positive")
        selected_policy = replace(
            selected_policy,
            minimum_synchronised_snapshots_per_venue=minimum_samples,
        )
    observed_at = now or datetime.now(UTC)
    route = runtime_evidence.route if runtime_evidence is not None else None
    statistics = (
        tuple(
            _venue_book_statistics(
                data_root,
                venue,
                route.base,
                selected_policy,
                runtime_evidence.epoch_started_at,
                runtime_evidence.epoch_ended_at,
            )
            for venue in (route.long_venue, route.short_venue)
        )
        if runtime_evidence is not None and route is not None
        else ()
    )
    route_period = _route_observation_period(statistics)
    checkpoint_counts: dict[Venue, int] = {}
    funding_intervals: dict[Venue, tuple[str, ...]] = {}
    if runtime_evidence is not None:
        for venue in (runtime_evidence.route.long_venue, runtime_evidence.route.short_venue):
            venue_checkpoints = tuple(
                checkpoint
                for checkpoint in runtime_evidence.funding_checkpoints
                if checkpoint.venue == venue
            )
            unique_checkpoints = {
                (checkpoint.next_funding_timestamp_ms, checkpoint.interval)
                for checkpoint in venue_checkpoints
            }
            checkpoint_counts[venue] = len(unique_checkpoints)
            funding_intervals[venue] = tuple(
                sorted({checkpoint.interval for checkpoint in venue_checkpoints})
            )

    blockers = _qualification_blockers(
        runtime_evidence,
        statistics,
        route_period,
        checkpoint_counts,
        selected_policy,
        current_code_commit_sha(repo_root),
        code_hash(repo_root),
        config_hash(config_path),
    )
    accepted = not blockers
    manifest = _data_manifest(data_root)
    unsigned = QualificationEvidence(
        schema_version=4,
        generated_at=observed_at,
        epoch_id=runtime_evidence.epoch_id if runtime_evidence else "UNAVAILABLE",
        epoch_started_at=(runtime_evidence.epoch_started_at if runtime_evidence else observed_at),
        epoch_ended_at=(runtime_evidence.epoch_ended_at if runtime_evidence else observed_at),
        epoch_status=(
            runtime_evidence.epoch_status if runtime_evidence else QualificationEpochStatus.CLOSED
        ),
        route=route,
        code_commit_sha=(runtime_evidence.release_code_sha if runtime_evidence else "UNAVAILABLE"),
        code_sha256=code_hash(repo_root),
        config_sha256=config_hash(config_path),
        data_sha256=_manifest_hash(manifest),
        data_manifest=manifest,
        container_image_digest=(
            runtime_evidence.container_image_digest if runtime_evidence else "UNAVAILABLE"
        ),
        venue_statistics=statistics,
        route_observation_period_seconds=route_period,
        private_taker_fee_rates=(
            dict(runtime_evidence.private_taker_fee_rates) if runtime_evidence else {}
        ),
        funding_checkpoint_counts=checkpoint_counts,
        funding_intervals=funding_intervals,
        replay_shadow=runtime_evidence.replay_shadow if runtime_evidence else None,
        strategy=runtime_evidence.strategy if runtime_evidence else None,
        policy=selected_policy,
        accepted=accepted,
        blockers=blockers,
        reason=(
            ReasonCode.QUALIFICATION_PASSED if accepted else ReasonCode.QUALIFICATION_INSUFFICIENT
        ),
        qualification_hash="",
    )
    evidence = replace(unsigned, qualification_hash=_qualification_hash(unsigned))
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = evidence_path.with_suffix(f"{evidence_path.suffix}.tmp")
    temporary.write_text(
        json.dumps(asdict(evidence), default=str, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(evidence_path)
    return evidence


def _read_book_events(
    data_root: Path,
    venue: Venue,
    base: str,
    started_at: datetime,
    ended_at: datetime,
) -> tuple[_BookEvent, ...]:
    files = tuple(data_root.rglob("*.parquet")) if data_root.is_dir() else ()
    if not files:
        return ()
    parquet_glob = str(data_root / "**" / "*.parquet").replace("\\", "/")
    with duckdb.connect(":memory:") as database:
        rows = database.execute(
            """
            SELECT event_id,
                   min(received_at) AS received_at,
                   max(exchange_timestamp_ms) AS exchange_timestamp_ms,
                   max(sequence_start) AS sequence_start,
                   max(sequence_end) AS sequence_end,
                   bool_and(is_snapshot) AS is_snapshot,
                   bool_and(synchronised) AS synchronised,
                   max(clock_skew_ms) AS clock_skew_ms
            FROM read_parquet(?, hive_partitioning = true, union_by_name = true)
            WHERE venue = ? AND upper(split_part(symbol, '/', 1)) = ?
              AND received_at >= ? AND received_at <= ?
            GROUP BY event_id
            ORDER BY received_at, event_id
            """,
            [parquet_glob, venue.value, base.upper(), started_at.isoformat(), ended_at.isoformat()],
        ).fetchall()
    return tuple(
        _BookEvent(
            event_id=str(row[0]),
            received_at=datetime.fromisoformat(str(row[1])),
            exchange_timestamp_ms=int(row[2]) if row[2] is not None else None,
            sequence_start=int(row[3]) if row[3] is not None else None,
            sequence_end=int(row[4]) if row[4] is not None else None,
            is_snapshot=bool(row[5]),
            synchronised=bool(row[6]),
            clock_skew_ms=int(row[7]) if row[7] is not None else None,
        )
        for row in rows
    )


def _venue_book_statistics(
    data_root: Path,
    venue: Venue,
    base: str,
    policy: QualificationPolicy,
    started_at: datetime,
    ended_at: datetime,
) -> VenueBookStatistics:
    events = _read_book_events(data_root, venue, base, started_at, ended_at)
    if not events:
        return VenueBookStatistics(
            venue, 0, 0, None, None, Decimal(0), 0, 0, Decimal(0), 0, 0, 0, 0, 0
        )
    snapshot_events = tuple(event for event in events if event.is_snapshot)
    synchronised = tuple(event for event in snapshot_events if event.synchronised)
    inter_event_gaps = tuple(
        Decimal(str((current.received_at - previous.received_at).total_seconds()))
        for previous, current in pairwise(synchronised)
    )
    sequence_gaps: list[int] = []
    for previous, current in pairwise(synchronised):
        if previous.sequence_end is None or current.sequence_start is None:
            continue
        gap = current.sequence_start - previous.sequence_end - 1
        if gap != 0:
            sequence_gaps.append(abs(gap))
    stale = sum(
        1
        for event in snapshot_events
        if event.exchange_timestamp_ms is None
        or Decimal(str(event.received_at.timestamp() * 1_000 - event.exchange_timestamp_ms))
        > policy.maximum_snapshot_age_ms
    )
    unknown_sequence = sum(
        1 for event in snapshot_events if event.sequence_start is None or event.sequence_end is None
    )
    skewed = sum(
        1
        for event in snapshot_events
        if event.clock_skew_ms is None or abs(event.clock_skew_ms) > policy.maximum_clock_skew_ms
    )
    duration = Decimal(str((events[-1].received_at - events[0].received_at).total_seconds()))
    return VenueBookStatistics(
        venue=venue,
        unique_order_book_events=len(events),
        synchronised_snapshots=len(synchronised),
        first_observed_at=events[0].received_at,
        last_observed_at=events[-1].received_at,
        observation_period_seconds=max(Decimal(0), duration),
        sequence_gap_count=len(sequence_gaps),
        maximum_sequence_gap=max(sequence_gaps, default=0),
        maximum_inter_snapshot_gap_seconds=max(inter_event_gaps, default=Decimal(0)),
        stale_snapshot_count=stale,
        sequence_unknown_snapshot_count=unknown_sequence,
        unsynchronised_snapshot_count=len(snapshot_events) - len(synchronised),
        clock_skew_snapshot_count=skewed,
        maximum_absolute_clock_skew_ms=max(
            (
                abs(event.clock_skew_ms)
                for event in snapshot_events
                if event.clock_skew_ms is not None
            ),
            default=0,
        ),
    )


def _route_observation_period(statistics: tuple[VenueBookStatistics, ...]) -> Decimal:
    if len(statistics) != 2 or any(
        item.first_observed_at is None or item.last_observed_at is None for item in statistics
    ):
        return Decimal(0)
    first = max(item.first_observed_at for item in statistics if item.first_observed_at)
    last = min(item.last_observed_at for item in statistics if item.last_observed_at)
    return max(Decimal(0), Decimal(str((last - first).total_seconds())))


async def build_qualification_progress(
    state_path: Path,
    data_root: Path,
    epoch_id: str | None,
    policy: QualificationPolicy,
    now: datetime | None = None,
) -> QualificationProgress:
    """Report exact persisted policy progress without treating elapsed time as qualification."""
    from interexchange_perp_grid.state import (
        load_tranches,
        read_qualification_epoch,
        read_qualification_statistics,
    )

    epoch = await read_qualification_epoch(state_path, epoch_id)
    if epoch is None:
        raise KeyError(epoch_id or "current")
    observed_at = now or datetime.now(UTC)
    effective_end = min(observed_at, epoch.ended_at or observed_at)
    elapsed = max(Decimal(0), Decimal(str((effective_end - epoch.started_at).total_seconds())))
    statistics = tuple(
        _venue_book_statistics(
            data_root,
            venue,
            epoch.route.base,
            policy,
            epoch.started_at,
            effective_end,
        )
        for venue in (epoch.route.long_venue, epoch.route.short_venue)
    )
    stored = await read_qualification_statistics(state_path, epoch.epoch_id)
    tranches = await load_tranches(state_path)
    route_tranches = tuple(tranche for tranche in tranches if tranche.route == epoch.route)
    unresolved_orders = sum(
        tranche.state == PairActionState.UNKNOWN_ORDER for tranche in route_tranches
    )
    unresolved_exposure = sum(
        tranche.state not in {PairActionState.CLOSED, PairActionState.CREATED}
        or tranche.residual_quantity != 0
        for tranche in route_tranches
    )
    checkpoint_counts = {
        venue: len(
            {
                (next_funding_timestamp_ms, interval)
                for (
                    observed_venue,
                    _,
                    _,
                    next_funding_timestamp_ms,
                    interval,
                ) in stored.funding_rows
                if observed_venue == venue
            }
        )
        for venue in (epoch.route.long_venue, epoch.route.short_venue)
    }
    venue_progress = tuple(
        VenueQualificationProgress(
            venue=item.venue,
            synchronised_snapshots=item.synchronised_snapshots,
            required_synchronised_snapshots=(policy.minimum_synchronised_snapshots_per_venue),
            remaining_synchronised_snapshots=max(
                0,
                policy.minimum_synchronised_snapshots_per_venue - item.synchronised_snapshots,
            ),
            funding_checkpoints=checkpoint_counts[item.venue],
            required_funding_checkpoints=policy.minimum_funding_checkpoints_per_venue,
            remaining_funding_checkpoints=max(
                0,
                policy.minimum_funding_checkpoints_per_venue - checkpoint_counts[item.venue],
            ),
            sequence_gap_count=item.sequence_gap_count,
            stale_snapshot_count=item.stale_snapshot_count,
            sequence_unknown_snapshot_count=item.sequence_unknown_snapshot_count,
            unsynchronised_snapshot_count=item.unsynchronised_snapshot_count,
            clock_skew_snapshot_count=item.clock_skew_snapshot_count,
            maximum_inter_snapshot_gap_seconds=item.maximum_inter_snapshot_gap_seconds,
        )
        for item in statistics
    )
    blockers: list[str] = []
    if epoch.status != QualificationEpochStatus.RUNNING:
        blockers.append(f"EPOCH_{epoch.status.value}")
    if elapsed < policy.minimum_duration_seconds:
        blockers.append("OBSERVATION_PERIOD_INSUFFICIENT")
    for item in venue_progress:
        prefix = item.venue.value.upper()
        if item.remaining_synchronised_snapshots:
            blockers.append(f"{prefix}_SYNCHRONISED_SNAPSHOTS_INSUFFICIENT")
        if item.remaining_funding_checkpoints:
            blockers.append(f"{prefix}_FUNDING_CHECKPOINTS_INSUFFICIENT")
        if item.maximum_inter_snapshot_gap_seconds > policy.maximum_inter_snapshot_gap_seconds:
            blockers.append(f"{prefix}_CONTINUITY_GAP_EXCEEDED")
        if item.sequence_gap_count > policy.maximum_sequence_gaps:
            blockers.append(f"{prefix}_SEQUENCE_GAPS_EXCEEDED")
        if item.stale_snapshot_count > policy.maximum_stale_snapshots:
            blockers.append(f"{prefix}_STALE_SNAPSHOTS_EXCEEDED")
        if item.sequence_unknown_snapshot_count > policy.maximum_sequence_unknown_snapshots:
            blockers.append(f"{prefix}_BOOK_SEQUENCE_UNKNOWN")
        if item.unsynchronised_snapshot_count:
            blockers.append(f"{prefix}_UNSYNCHRONISED_SNAPSHOTS")
        if item.clock_skew_snapshot_count > policy.maximum_clock_skew_snapshots:
            blockers.append(f"{prefix}_CLOCK_SKEW_EXCEEDED")
    if stored.accepted_signals + stored.rejected_signals == 0:
        blockers.append("SIGNAL_STATISTICS_MISSING")
    simulated_pnl = Decimal(stored.latest_simulated_net_pnl_usdt)
    if simulated_pnl <= 0:
        blockers.append("SIMULATED_NET_PNL_NOT_POSITIVE")
    if stored.strategy is None:
        blockers.append("STRATEGY_PARAMETERS_MISSING")
    if stored.unhandled_exception_count:
        blockers.append("UNHANDLED_EXCEPTIONS")
    collection_blockers = tuple(blockers)
    # Replay evidence is bound only by the immutable final qualification artifact.  A
    # running epoch must never imply that replay has already passed.
    blockers.append("REPLAY_NOT_COMPLETED")
    if unresolved_orders:
        blockers.append("UNRESOLVED_ORDER_STATE")
    if unresolved_exposure:
        blockers.append("UNRESOLVED_EXPOSURE")
    remaining_duration = max(Decimal(0), Decimal(policy.minimum_duration_seconds) - elapsed)
    duration_ratio = min(Decimal(1), elapsed / Decimal(policy.minimum_duration_seconds))
    venue_ratios = tuple(
        min(
            Decimal(1),
            Decimal(item.synchronised_snapshots) / Decimal(item.required_synchronised_snapshots),
            Decimal(item.funding_checkpoints) / Decimal(item.required_funding_checkpoints),
        )
        for item in venue_progress
    )
    completion_ratio = min(duration_ratio, *venue_ratios)
    return QualificationProgress(
        schema_version=1,
        generated_at=observed_at,
        epoch_id=epoch.epoch_id,
        epoch_status=epoch.status,
        route=epoch.route,
        release_sha=epoch.release_sha,
        source_sha256=epoch.source_sha256,
        config_sha256=epoch.config_sha256,
        container_image_digest=epoch.container_image_digest,
        elapsed_seconds=elapsed,
        required_duration_seconds=policy.minimum_duration_seconds,
        remaining_duration_seconds=remaining_duration,
        completion_ratio=completion_ratio,
        accepted_signals=stored.accepted_signals,
        rejected_signals=stored.rejected_signals,
        unhandled_exception_count=stored.unhandled_exception_count,
        replay_completed=False,
        unresolved_order_count=unresolved_orders,
        unresolved_exposure_count=unresolved_exposure,
        simulated_net_pnl_usdt=simulated_pnl,
        venues=venue_progress,
        blockers=tuple(blockers),
        ready_to_finalize=not collection_blockers,
        qualification_ready=not blockers,
    )


def _qualification_blockers(
    runtime: QualificationRuntimeEvidence | None,
    statistics: tuple[VenueBookStatistics, ...],
    route_period: Decimal,
    checkpoint_counts: dict[Venue, int],
    policy: QualificationPolicy,
    observed_code_sha: str | None,
    observed_source_sha256: str,
    observed_config_sha256: str,
) -> tuple[str, ...]:
    blockers: list[str] = []
    if runtime is None:
        return ("RUNTIME_EVIDENCE_MISSING",)
    if observed_code_sha is None or observed_code_sha != runtime.release_code_sha:
        blockers.append("RELEASE_CODE_SHA_MISMATCH")
    if runtime.source_sha256 != observed_source_sha256:
        blockers.append("SOURCE_SHA256_MISMATCH")
    if runtime.config_sha256 != observed_config_sha256:
        blockers.append("CONFIG_SHA256_MISMATCH")
    if runtime.epoch_status != QualificationEpochStatus.FINALIZED:
        blockers.append("QUALIFICATION_EPOCH_NOT_FINALIZED")
    if runtime.epoch_ended_at < runtime.epoch_started_at:
        blockers.append("QUALIFICATION_EPOCH_INVALID")
    if route_period < policy.minimum_duration_seconds:
        blockers.append("OBSERVATION_PERIOD_INSUFFICIENT")
    for item in statistics:
        prefix = item.venue.value.upper()
        if item.synchronised_snapshots < policy.minimum_synchronised_snapshots_per_venue:
            blockers.append(f"{prefix}_SYNCHRONISED_SNAPSHOTS_INSUFFICIENT")
        if item.maximum_inter_snapshot_gap_seconds > policy.maximum_inter_snapshot_gap_seconds:
            blockers.append(f"{prefix}_CONTINUITY_GAP_EXCEEDED")
        if item.sequence_gap_count > policy.maximum_sequence_gaps:
            blockers.append(f"{prefix}_SEQUENCE_GAPS_EXCEEDED")
        if item.stale_snapshot_count > policy.maximum_stale_snapshots:
            blockers.append(f"{prefix}_STALE_SNAPSHOTS_EXCEEDED")
        if item.sequence_unknown_snapshot_count > policy.maximum_sequence_unknown_snapshots:
            blockers.append(f"{prefix}_BOOK_SEQUENCE_UNKNOWN")
        if item.unsynchronised_snapshot_count:
            blockers.append(f"{prefix}_UNSYNCHRONISED_SNAPSHOTS")
        if item.clock_skew_snapshot_count > policy.maximum_clock_skew_snapshots:
            blockers.append(f"{prefix}_CLOCK_SKEW_EXCEEDED")
        if checkpoint_counts.get(item.venue, 0) < policy.minimum_funding_checkpoints_per_venue:
            blockers.append(f"{prefix}_FUNDING_CHECKPOINTS_INSUFFICIENT")
    shadow = runtime.replay_shadow
    if not shadow.replay_completed:
        blockers.append("REPLAY_NOT_COMPLETED")
    if shadow.accepted_signals + shadow.rejected_signals == 0:
        blockers.append("SIGNAL_STATISTICS_MISSING")
    if shadow.simulated_net_pnl_usdt <= 0:
        blockers.append("SIMULATED_NET_PNL_NOT_POSITIVE")
    if shadow.unresolved_order_count:
        blockers.append("UNRESOLVED_ORDERS")
    if shadow.unresolved_exposure_count:
        blockers.append("UNRESOLVED_EXPOSURE")
    if shadow.unhandled_exception_count:
        blockers.append("UNHANDLED_EXCEPTIONS")
    return tuple(blockers)


def _qualification_hash(evidence: QualificationEvidence) -> str:
    payload = asdict(evidence)
    payload["qualification_hash"] = ""
    encoded = json.dumps(payload, default=str, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def load_qualification(path: Path) -> QualificationEvidence:
    payload = json.loads(path.read_text(encoding="utf-8"))
    route_payload = payload.get("route")
    route = (
        DirectedRouteKey(
            str(route_payload["base"]),
            Venue(str(route_payload["long_venue"])),
            Venue(str(route_payload["short_venue"])),
        )
        if isinstance(route_payload, dict)
        else None
    )
    policy_payload = _require_mapping(payload, "policy")
    replay_payload = payload.get("replay_shadow")
    strategy_payload = payload.get("strategy")
    evidence = QualificationEvidence(
        schema_version=int(payload["schema_version"]),
        generated_at=datetime.fromisoformat(str(payload["generated_at"])),
        epoch_id=str(payload["epoch_id"]),
        epoch_started_at=datetime.fromisoformat(str(payload["epoch_started_at"])),
        epoch_ended_at=datetime.fromisoformat(str(payload["epoch_ended_at"])),
        epoch_status=QualificationEpochStatus(str(payload["epoch_status"])),
        route=route,
        code_commit_sha=str(payload["code_commit_sha"]),
        code_sha256=str(payload["code_sha256"]),
        config_sha256=str(payload["config_sha256"]),
        data_sha256=str(payload["data_sha256"]),
        data_manifest={
            str(path): str(digest)
            for path, digest in _require_mapping(payload, "data_manifest").items()
        },
        container_image_digest=str(payload["container_image_digest"]),
        venue_statistics=tuple(
            VenueBookStatistics(
                venue=Venue(str(item["venue"])),
                unique_order_book_events=int(item["unique_order_book_events"]),
                synchronised_snapshots=int(item["synchronised_snapshots"]),
                first_observed_at=(
                    datetime.fromisoformat(str(item["first_observed_at"]))
                    if item.get("first_observed_at") is not None
                    else None
                ),
                last_observed_at=(
                    datetime.fromisoformat(str(item["last_observed_at"]))
                    if item.get("last_observed_at") is not None
                    else None
                ),
                observation_period_seconds=Decimal(str(item["observation_period_seconds"])),
                sequence_gap_count=int(item["sequence_gap_count"]),
                maximum_sequence_gap=int(item["maximum_sequence_gap"]),
                maximum_inter_snapshot_gap_seconds=Decimal(
                    str(item["maximum_inter_snapshot_gap_seconds"])
                ),
                stale_snapshot_count=int(item["stale_snapshot_count"]),
                sequence_unknown_snapshot_count=int(item["sequence_unknown_snapshot_count"]),
                unsynchronised_snapshot_count=int(item["unsynchronised_snapshot_count"]),
                clock_skew_snapshot_count=int(item["clock_skew_snapshot_count"]),
                maximum_absolute_clock_skew_ms=int(item["maximum_absolute_clock_skew_ms"]),
            )
            for item in payload["venue_statistics"]
        ),
        route_observation_period_seconds=Decimal(str(payload["route_observation_period_seconds"])),
        private_taker_fee_rates={
            Venue(str(venue)): Decimal(str(value))
            for venue, value in _require_mapping(payload, "private_taker_fee_rates").items()
        },
        funding_checkpoint_counts={
            Venue(str(venue)): int(value)
            for venue, value in _require_mapping(payload, "funding_checkpoint_counts").items()
        },
        funding_intervals={
            Venue(str(venue)): tuple(str(interval) for interval in value)
            for venue, value in _require_mapping(payload, "funding_intervals").items()
            if isinstance(value, list)
        },
        replay_shadow=(
            ReplayShadowStatistics(
                replay_completed=bool(replay_payload["replay_completed"]),
                accepted_signals=int(replay_payload["accepted_signals"]),
                rejected_signals=int(replay_payload["rejected_signals"]),
                simulated_net_pnl_usdt=Decimal(str(replay_payload["simulated_net_pnl_usdt"])),
                maximum_adverse_excursion_usdt=Decimal(
                    str(replay_payload["maximum_adverse_excursion_usdt"])
                ),
                unresolved_order_count=int(replay_payload["unresolved_order_count"]),
                unresolved_exposure_count=int(replay_payload["unresolved_exposure_count"]),
                unhandled_exception_count=int(replay_payload["unhandled_exception_count"]),
            )
            if isinstance(replay_payload, dict)
            else None
        ),
        strategy=(
            QualifiedStrategyParameters(
                calibration_version=int(strategy_payload["calibration_version"]),
                size_bucket_base_quantity=Decimal(
                    str(strategy_payload["size_bucket_base_quantity"])
                ),
                adaptive_entry_threshold_bps=Decimal(
                    str(strategy_payload["adaptive_entry_threshold_bps"])
                ),
                target_exit_spread_bps=Decimal(str(strategy_payload["target_exit_spread_bps"])),
                minimum_profit_usdt=Decimal(str(strategy_payload["minimum_profit_usdt"])),
                stressed_cost_multiplier=Decimal(str(strategy_payload["stressed_cost_multiplier"])),
                expected_holding_seconds=int(strategy_payload["expected_holding_seconds"]),
                maximum_holding_seconds=int(strategy_payload["maximum_holding_seconds"]),
            )
            if isinstance(strategy_payload, dict)
            else None
        ),
        policy=QualificationPolicy(**{key: int(value) for key, value in policy_payload.items()}),
        accepted=bool(payload["accepted"]),
        blockers=tuple(str(item) for item in payload["blockers"]),
        reason=ReasonCode(str(payload["reason"])),
        qualification_hash=str(payload["qualification_hash"]),
    )
    if evidence.qualification_hash != _qualification_hash(evidence):
        raise ValueError("qualification evidence hash is invalid")
    return evidence


def qualification_is_current(
    evidence: QualificationEvidence,
    repo_root: Path,
    config_path: Path,
    data_root: Path,
    max_age_seconds: int,
    now: datetime | None = None,
    *,
    expected_route: DirectedRouteKey | None = None,
    current_container_image_digest: str | None = None,
    current_release_code_sha: str | None = None,
    accepted_policies: tuple[QualificationPolicy, ...] | None = None,
    enforce_age: bool = True,
) -> tuple[bool, ReasonCode]:
    observed_at = now or datetime.now(UTC)
    release_sha = current_release_code_sha or current_code_commit_sha(repo_root)
    hashes_match = (
        evidence.schema_version == 4
        and evidence.epoch_status == QualificationEpochStatus.FINALIZED
        and evidence.epoch_ended_at >= evidence.epoch_started_at
        and evidence.code_commit_sha == release_sha
        and evidence.code_sha256 == code_hash(repo_root)
        and evidence.config_sha256 == config_hash(config_path)
        and evidence.data_sha256 == _manifest_hash(evidence.data_manifest)
        and _manifest_is_current(data_root, evidence.data_manifest)
        and evidence.container_image_digest == current_container_image_digest
        and evidence.qualification_hash == _qualification_hash(evidence)
    )
    route_matches = expected_route is None or evidence.route == expected_route
    policy_matches = accepted_policies is None or evidence.policy in accepted_policies
    age_seconds = (observed_at - evidence.generated_at).total_seconds()
    fresh = not enforce_age or 0 <= age_seconds <= max_age_seconds
    if (
        not evidence.accepted
        or not hashes_match
        or not route_matches
        or not policy_matches
        or not fresh
    ):
        return False, ReasonCode.QUALIFICATION_HASH_MISMATCH
    return True, ReasonCode.QUALIFICATION_PASSED


async def build_runtime_evidence_from_state(
    state_path: Path,
    epoch_id: str,
    private_taker_fee_rates: dict[Venue, Decimal],
    *,
    replay_completed: bool,
) -> QualificationRuntimeEvidence:
    from interexchange_perp_grid.state import (  # local import avoids state startup coupling
        load_tranches,
        read_qualification_epoch,
        read_qualification_statistics,
    )

    epoch = await read_qualification_epoch(state_path, epoch_id)
    if (
        epoch is None
        or epoch.status != QualificationEpochStatus.FINALIZED
        or epoch.ended_at is None
    ):
        raise ValueError("runtime evidence requires one finalized exact qualification epoch")
    route = epoch.route
    stored = await read_qualification_statistics(state_path, epoch_id)
    if stored.strategy is None:
        raise ValueError("route-specific shadow strategy evidence is missing")
    strategy = stored.strategy
    tranches = await load_tranches(state_path)
    route_tranches = tuple(tranche for tranche in tranches if tranche.route == route)
    unresolved_orders = sum(
        tranche.state == PairActionState.UNKNOWN_ORDER for tranche in route_tranches
    )
    unresolved_exposure = sum(
        tranche.state
        not in {
            PairActionState.CLOSED,
            PairActionState.CREATED,
        }
        or tranche.residual_quantity != 0
        for tranche in route_tranches
    )
    return QualificationRuntimeEvidence(
        epoch_id=epoch.epoch_id,
        epoch_started_at=epoch.started_at,
        epoch_ended_at=epoch.ended_at,
        epoch_status=epoch.status,
        route=route,
        release_code_sha=epoch.release_sha,
        source_sha256=epoch.source_sha256,
        config_sha256=epoch.config_sha256,
        container_image_digest=epoch.container_image_digest,
        private_taker_fee_rates=private_taker_fee_rates,
        funding_checkpoints=tuple(
            FundingCheckpoint(
                venue=venue,
                observed_at=observed_at,
                rate=Decimal(rate),
                next_funding_timestamp_ms=next_timestamp,
                interval=interval,
            )
            for venue, observed_at, rate, next_timestamp, interval in stored.funding_rows
        ),
        replay_shadow=ReplayShadowStatistics(
            replay_completed=replay_completed,
            accepted_signals=stored.accepted_signals,
            rejected_signals=stored.rejected_signals,
            simulated_net_pnl_usdt=Decimal(stored.latest_simulated_net_pnl_usdt),
            maximum_adverse_excursion_usdt=Decimal(stored.maximum_adverse_excursion_usdt),
            unresolved_order_count=unresolved_orders,
            unresolved_exposure_count=unresolved_exposure,
            unhandled_exception_count=stored.unhandled_exception_count,
        ),
        strategy=QualifiedStrategyParameters(
            calibration_version=int(strategy["calibration_version"]),
            size_bucket_base_quantity=Decimal(str(strategy["size_bucket_base_quantity"])),
            adaptive_entry_threshold_bps=Decimal(str(strategy["adaptive_entry_threshold_bps"])),
            target_exit_spread_bps=Decimal(str(strategy["target_exit_spread_bps"])),
            minimum_profit_usdt=Decimal(str(strategy["minimum_profit_usdt"])),
            stressed_cost_multiplier=Decimal(str(strategy["stressed_cost_multiplier"])),
            expected_holding_seconds=int(strategy["expected_holding_seconds"]),
            maximum_holding_seconds=int(strategy["maximum_holding_seconds"]),
        ),
    )
