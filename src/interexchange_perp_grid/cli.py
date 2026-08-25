from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import re
import subprocess
import sys
from collections import Counter
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Annotated

import typer

from interexchange_perp_grid.adapters.ccxt_pro import CcxtProAdapter
from interexchange_perp_grid.adapters.private import CcxtPrivateAdapter, PrivateCredentials
from interexchange_perp_grid.aggressive_evaluator import (
    AggressiveEntryStage,
    AggressiveExitReason,
    CostReserves,
    load_aggressive_decision_policy,
)
from interexchange_perp_grid.aggressive_grid import (
    AggressiveGridStore,
    ExternalGridLevelProjection,
    GridLegFill,
    GridLevelState,
    GridTrancheOwnership,
)
from interexchange_perp_grid.aggressive_laptop_acceptance import (
    build_aggressive_laptop_acceptance,
    build_aggressive_laptop_stage_evidence_from_journal,
    load_aggressive_laptop_acceptance,
    load_aggressive_laptop_stage_evidence,
    save_aggressive_laptop_acceptance,
    save_aggressive_laptop_stage_evidence,
    verify_aggressive_laptop_handoff,
)
from interexchange_perp_grid.aggressive_live import (
    AggressiveLaptopLiveStage,
    AggressiveLiveIntentEnvelope,
    aggressive_intent_sha256,
    load_aggressive_live_intent,
    save_aggressive_live_intent,
)
from interexchange_perp_grid.aggressive_model import (
    DivergenceDirection,
    HistoricalReferenceModel,
    build_historical_reference_model,
    historical_model_payload,
    historical_model_sha256,
    load_historical_model,
    load_historical_model_policy,
    save_historical_model,
)
from interexchange_perp_grid.aggressive_qualification import (
    AggressiveQualificationBinding,
    build_aggressive_qualification_binding,
    load_aggressive_qualification_binding,
    save_aggressive_qualification_binding,
    verify_aggressive_qualification_binding,
)
from interexchange_perp_grid.aggressive_runtime import (
    AggressiveDecisionCore,
    AggressiveRuntimeMode,
    AggressiveStrategyDecision,
    AggressiveTrancheIntent,
    aggressive_runtime_manifest_sha256,
)
from interexchange_perp_grid.aggressive_shadow import (
    AggressiveShadowDecisionBridge,
    AggressiveShadowDecisionInput,
    AggressiveShadowPortfolio,
)
from interexchange_perp_grid.autonomous_orchestrator import load_autonomous_runtime_status
from interexchange_perp_grid.c4_3_proof import run_c4_3_proof
from interexchange_perp_grid.c4_proof import run_c4_proof
from interexchange_perp_grid.canary_runtime import (
    PILOT_A_OWNER_CONFIRMATION,
    collect_authoritative_live_flat_evidence,
    run_canary_once,
    run_emergency_flatten,
)
from interexchange_perp_grid.config import Settings, load_settings
from interexchange_perp_grid.domain import Instrument, InstrumentKey, ProductType, Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.laptop_workflow import (
    LaptopQualificationIdentity,
    build_laptop_pilot_report,
    run_until_qualification_finalized,
    write_laptop_pilot_report,
)
from interexchange_perp_grid.live_journal import (
    DeploymentUpgradeGate,
    LiveActionState,
    LiveOrderJournal,
    completed_normal_actions_sha256,
    is_completed_normal_paired_cycle,
)
from interexchange_perp_grid.maintenance import (
    backup_sqlite,
    prune_market_history,
    restore_sqlite,
)
from interexchange_perp_grid.native_runtime import (
    build_native_runtime_manifest,
    load_native_runtime_manifest,
    resolve_runtime_artifact_digest,
    verify_native_runtime_manifest,
    write_native_runtime_manifest,
)
from interexchange_perp_grid.observability import configure_logging, render_metrics
from interexchange_perp_grid.ops_evidence import build_operations_proof
from interexchange_perp_grid.private_domain import PrivateCapabilityReport
from interexchange_perp_grid.private_transition_smoke import (
    run_private_transition_recovery_smoke,
)
from interexchange_perp_grid.public_engine import PublicMarketEngine, ScanResult
from interexchange_perp_grid.qualification import (
    LAPTOP_OWNER_EXCEPTION_CONFIRMATION,
    LAPTOP_OWNER_EXCEPTION_ENV,
    QualificationPolicy,
    QualificationProgress,
    QualificationRuntimeEvidence,
    build_qualification_progress,
    build_runtime_evidence_from_state,
    code_hash,
    config_hash,
    current_code_commit_sha,
    laptop_owner_exception_authorized,
    laptop_owner_exception_policy,
    load_qualification,
    load_runtime_evidence,
    qualification_is_current,
    qualification_policy_from_settings,
    run_qualification,
    write_runtime_evidence,
)
from interexchange_perp_grid.reference_history import (
    SourceBarQuality,
    SourceMinuteBar,
    aggregate_reference_bars,
    build_reference_series,
    directed_routes_for_reference_pair,
    reference_bars_sha256,
    source_bars_sha256,
)
from interexchange_perp_grid.reference_store import ParquetReferenceHistoryStore
from interexchange_perp_grid.region_latency import (
    MAXIMUM_PROBE_DURATION_SECONDS,
    WAVE1_VENUES,
    LatencySample,
    RegionLatencyPolicy,
    attestation_sha256,
    bounded_operation,
    build_region_latency_report,
    collect_region_latency_samples,
    load_latency_samples,
    load_region_attestation,
    load_region_latency_policy,
    load_region_latency_report,
    local_host_fingerprint,
    select_deployment_region,
    validate_region_probe_request,
    verify_provider_evidence,
    write_latency_samples,
    write_region_latency_report,
)
from interexchange_perp_grid.release_evidence import REPLAY_TEST_FILES, run_replay_proof
from interexchange_perp_grid.release_preflight import evaluate_release_preflight
from interexchange_perp_grid.risk_stages import (
    load_locked_risk_stage_table,
    verify_risk_stage_completion_evidence,
)
from interexchange_perp_grid.safety import LiveContext, evaluate_live_order
from interexchange_perp_grid.service import (
    load_bounded_service_receipt,
    run_for_duration,
    run_until_signal,
    write_bounded_service_receipt,
)
from interexchange_perp_grid.shadow import ShadowRuntime
from interexchange_perp_grid.state import (
    QualificationEpoch,
    RiskStage,
    RiskStageResult,
    RiskStageState,
    ServiceHealth,
    finalize_qualification_epoch,
    initialise_state,
    promote_risk_stage,
    read_qualification_epoch,
    read_risk_stage,
    read_service_health,
    record_risk_stage_result,
    start_qualification_epoch,
)
from interexchange_perp_grid.strategy import DirectedRouteKey
from interexchange_perp_grid.supervisor import SupervisorHealth, read_supervisor_health
from interexchange_perp_grid.supervisor_smoke import (
    RecoverySmokeTransition,
    run_supervisor_recovery_smoke,
)

app = typer.Typer(no_args_is_help=True, add_completion=False)
ConfigPath = Annotated[
    Path,
    typer.Option(
        "--config",
        envvar="IPEG_CONFIG_PATH",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
]


@app.callback()
def main() -> None:
    """Interexchange perpetual grid control CLI."""


def _load(config: Path) -> Settings:
    settings = load_settings(config)
    configure_logging(settings.app.log_level)
    return settings


def _qualification_policy(settings: Settings) -> QualificationPolicy:
    return qualification_policy_from_settings(settings)


def _selected_qualification_policy(
    settings: Settings,
    laptop_owner_exception_12h: bool,
) -> QualificationPolicy:
    standard = _qualification_policy(settings)
    if not laptop_owner_exception_12h:
        return standard
    if not laptop_owner_exception_authorized():
        raise typer.BadParameter(
            f"Windows laptop exception requires {LAPTOP_OWNER_EXCEPTION_ENV}="
            f"{LAPTOP_OWNER_EXCEPTION_CONFIRMATION}"
        )
    return laptop_owner_exception_policy(settings)


def _current_region_evidence_identity(repo_root: Path, config: Path) -> tuple[str, str]:
    root = repo_root.resolve()
    package_root = (root / "src/interexchange_perp_grid").resolve()
    if (
        not (root / ".git").exists()
        or not package_root.is_dir()
        or Path(__file__).resolve().parent != package_root
    ):
        raise ValueError("region evidence requires the current repository checkout")
    source_sha256 = code_hash(root)
    if source_sha256 == hashlib.sha256(b"").hexdigest():
        raise ValueError("region evidence source hash cannot be empty")
    return source_sha256, config_hash(config.resolve())


def _load_locked_region_policy(repo_root: Path, runtime_policy: Path) -> RegionLatencyPolicy:
    expected = (repo_root.resolve() / "config/RUNTIME_POLICY.yaml").resolve()
    if runtime_policy.resolve() != expected:
        raise ValueError("region evidence requires the current checkout locked runtime policy")
    return load_region_latency_policy(expected)


def _parse_route(value: str) -> DirectedRouteKey:
    base, separator, venues = value.strip().partition(":")
    long_venue, direction, short_venue = venues.partition(">")
    if not separator or not direction:
        raise typer.BadParameter("route must use BASE:long_venue>short_venue")
    try:
        return DirectedRouteKey(
            base=base.upper(),
            long_venue=Venue(long_venue.lower()),
            short_venue=Venue(short_venue.lower()),
        )
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error


@app.command()
def doctor(config: ConfigPath = Path("config/defaults.yaml")) -> None:
    """Validate configuration, state storage, and default live denial."""
    settings = _load(config)
    state_path = Path(settings.storage.sqlite_path)
    asyncio.run(initialise_state(state_path))
    live_decision = evaluate_live_order(settings, LiveContext())
    if live_decision.allowed:
        raise typer.Exit(code=2)
    typer.echo(
        json.dumps(
            {
                "status": "PASS",
                "mode": settings.app.mode,
                "live_orders_allowed": live_decision.allowed,
                "live_deny_reason": live_decision.reason,
                "wave1": settings.venues.wave1_public,
                "state_path": str(state_path),
            },
            default=str,
            sort_keys=True,
        )
    )


@app.command("aggressive-laptop-acceptance")
def aggressive_laptop_acceptance(
    binding: Annotated[Path, typer.Option("--binding")],
    runtime_manifest: Annotated[Path, typer.Option("--runtime-manifest")],
    canary_evidence: Annotated[Path, typer.Option("--canary-evidence")],
    pilot_evidence: Annotated[Path, typer.Option("--pilot-evidence")],
    qualification: Annotated[Path, typer.Option("--qualification")] = Path(
        "state/qualification.json"
    ),
    model: Annotated[Path, typer.Option("--model")] = Path(
        "state/aggressive-historical-model.json"
    ),
    grid: Annotated[Path, typer.Option("--grid")] = Path("state/aggressive-grid.sqlite3"),
    live_grid: Annotated[Path, typer.Option("--live-grid")] = Path(
        "state/aggressive-live-grid.sqlite3"
    ),
    profile: Annotated[Path, typer.Option("--profile")] = Path(
        "config/AGGRESSIVE_SYMBIOSIS_V1.yaml"
    ),
    history_root: Annotated[Path, typer.Option("--history-root")] = Path("data/reference-history"),
    output: Annotated[Path, typer.Option("--output")] = Path(
        "state/laptop-aggressive-acceptance.json"
    ),
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Create accepted laptop evidence only after exact canary, pilot and stable FLAT."""
    for required in (
        binding,
        runtime_manifest,
        canary_evidence,
        pilot_evidence,
        qualification,
        model,
        grid,
        live_grid,
        profile,
    ):
        if not required.is_file():
            raise typer.BadParameter(f"required laptop acceptance input is missing: {required}")
    settings = _load(config)
    runtime = verify_native_runtime_manifest(
        runtime_manifest.resolve(),
        repo_root.resolve(),
        config.resolve(),
    )
    loaded_binding = load_aggressive_qualification_binding(binding.resolve())
    loaded_qualification = load_qualification(qualification.resolve())
    qualification_current, _ = qualification_is_current(
        loaded_qualification,
        repo_root.resolve(),
        config.resolve(),
        Path(settings.storage.parquet_dir),
        settings.live.qualification_max_age_seconds,
        expected_route=_parse_route(loaded_binding.qualification_route),
        current_container_image_digest=runtime.artifact_digest,
        accepted_policies=(
            qualification_policy_from_settings(settings),
            laptop_owner_exception_policy(settings),
        ),
        enforce_age=False,
    )
    if not qualification_current:
        raise typer.BadParameter("aggressive laptop qualification is no longer current")
    loaded_model = load_historical_model(model.resolve())
    _verify_aggressive_model_window(history_root.resolve(), loaded_model)
    grid_store = AggressiveGridStore(grid.resolve())
    grid_store.initialise()
    verify_aggressive_qualification_binding(
        loaded_binding,
        loaded_qualification,
        loaded_model,
        runtime,
        grid_store,
        profile_sha256=hashlib.sha256(profile.read_bytes()).hexdigest(),
    )
    live_grid_store = AggressiveGridStore(live_grid.resolve())
    live_grid_store.initialise()
    _require_aggressive_live_grid_flat(live_grid_store, loaded_binding)
    canary = load_aggressive_laptop_stage_evidence(canary_evidence.resolve())
    pilot = load_aggressive_laptop_stage_evidence(pilot_evidence.resolve())
    journal = LiveOrderJournal(Path(settings.storage.sqlite_path))
    post_pilot_tail = asyncio.run(
        journal.actions_updated_after(
            pilot.ended_at,
            loaded_binding.qualification_hash,
        )
    )
    if post_pilot_tail:
        raise typer.BadParameter("JOURNAL_CHANGED_AFTER_ACCEPTED_PILOT")
    fresh_flat = asyncio.run(
        collect_authoritative_live_flat_evidence(
            settings,
            loaded_binding.qualification_route.partition(":")[0],
        )
    )
    live_grid_store.initialise()
    _require_aggressive_live_grid_flat(live_grid_store, loaded_binding)
    post_private_tail = asyncio.run(
        journal.actions_updated_after(
            pilot.ended_at,
            loaded_binding.qualification_hash,
        )
    )
    if post_private_tail:
        raise typer.BadParameter("JOURNAL_CHANGED_AFTER_ACCEPTED_PILOT")
    if not fresh_flat.stable_flat or any(
        evidence.reconciliation_evidence_sha256 != fresh_flat.reconciliation_sha256
        for evidence in (canary, pilot)
    ):
        raise typer.BadParameter("fresh private stable-FLAT evidence is required")
    rebuilt_canary = asyncio.run(
        build_aggressive_laptop_stage_evidence_from_journal(
            Path(settings.storage.sqlite_path),
            loaded_binding,
            stage="canary",
            started_at=canary.started_at,
            ended_at=canary.ended_at,
            post_flat_service_seconds=canary.post_flat_service_seconds,
            authoritative_stable_flat=True,
            authoritative_private_event_watermark=canary.final_private_event_watermark,
            authoritative_reconciliation_sha256=canary.reconciliation_evidence_sha256,
        )
    )
    rebuilt_pilot = asyncio.run(
        build_aggressive_laptop_stage_evidence_from_journal(
            Path(settings.storage.sqlite_path),
            loaded_binding,
            stage="pilot_a",
            started_at=pilot.started_at,
            ended_at=pilot.ended_at,
            post_flat_service_seconds=pilot.post_flat_service_seconds,
            authoritative_stable_flat=True,
            authoritative_private_event_watermark=pilot.final_private_event_watermark,
            authoritative_reconciliation_sha256=pilot.reconciliation_evidence_sha256,
        )
    )
    if rebuilt_canary != canary or rebuilt_pilot != pilot:
        raise typer.BadParameter("laptop stage evidence is no longer current")
    acceptance = build_aggressive_laptop_acceptance(
        loaded_binding,
        runtime,
        canary,
        pilot,
    )
    save_aggressive_laptop_acceptance(output.resolve(), acceptance)
    typer.echo(json.dumps(asdict(acceptance), default=str, sort_keys=True))


@app.command("aggressive-laptop-stage-report")
def aggressive_laptop_stage_report(
    stage: Annotated[str, typer.Option("--stage")],
    started_at: Annotated[str, typer.Option("--started-at")],
    ended_at: Annotated[str, typer.Option("--ended-at")],
    post_flat_service_seconds: Annotated[
        int,
        typer.Option("--post-flat-service-seconds", min=0),
    ],
    binding: Annotated[Path, typer.Option("--binding")],
    output: Annotated[Path, typer.Option("--output")],
    service_receipt: Annotated[Path | None, typer.Option("--service-receipt")] = None,
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Build exact stage evidence from the durable journal without submitting an order."""
    if stage not in {"canary", "pilot_a"}:
        raise typer.BadParameter("stage must be canary or pilot_a")
    try:
        started = datetime.fromisoformat(started_at)
        ended = datetime.fromisoformat(ended_at)
    except ValueError as error:
        raise typer.BadParameter("stage timestamps must be ISO-8601 with UTC offsets") from error
    if any(value.tzinfo is None or value.utcoffset() is None for value in (started, ended)):
        raise typer.BadParameter("stage timestamps must be ISO-8601 with UTC offsets")
    started = started.astimezone(UTC)
    ended = ended.astimezone(UTC)
    settings = _load(config)
    if stage == "pilot_a":
        if service_receipt is None or not service_receipt.is_file():
            raise typer.BadParameter("pilot_a requires an exact post-FLAT service receipt")
        bounded_service = load_bounded_service_receipt(service_receipt.resolve())
        if (
            Path(bounded_service.state_path).resolve()
            != Path(settings.storage.sqlite_path).resolve()
            or bounded_service.started_at < ended
            or bounded_service.requested_seconds < 28_800
            or bounded_service.observed_monotonic_seconds < 28_800
        ):
            raise typer.BadParameter("pilot_a post-FLAT service receipt is invalid")
        post_flat_service_seconds = min(
            int(bounded_service.requested_seconds),
            int(bounded_service.observed_monotonic_seconds),
        )
    loaded_binding = load_aggressive_qualification_binding(binding.resolve())
    authoritative_flat = asyncio.run(
        collect_authoritative_live_flat_evidence(
            settings,
            loaded_binding.qualification_route.partition(":")[0],
        )
    )
    evidence = asyncio.run(
        build_aggressive_laptop_stage_evidence_from_journal(
            Path(settings.storage.sqlite_path),
            loaded_binding,
            stage=stage,
            started_at=started,
            ended_at=ended,
            post_flat_service_seconds=post_flat_service_seconds,
            authoritative_stable_flat=authoritative_flat.stable_flat,
            authoritative_private_event_watermark=(authoritative_flat.private_event_watermark),
            authoritative_reconciliation_sha256=(authoritative_flat.reconciliation_sha256),
        )
    )
    save_aggressive_laptop_stage_evidence(output.resolve(), evidence)
    typer.echo(json.dumps(asdict(evidence), default=str, sort_keys=True))
    if not evidence.accepted:
        raise typer.Exit(code=3)


@app.command("aggressive-laptop-stage-progress")
def aggressive_laptop_stage_progress(
    stage: Annotated[str, typer.Option("--stage")],
    started_at: Annotated[str, typer.Option("--started-at")],
    binding: Annotated[Path, typer.Option("--binding")],
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Read non-authorizing durable stage progress; never fabricate acceptance."""
    if stage not in {"canary", "pilot_a"}:
        raise typer.BadParameter("stage must be canary or pilot_a")
    try:
        started = datetime.fromisoformat(started_at)
    except ValueError as error:
        raise typer.BadParameter("stage start must be an aware ISO-8601 timestamp") from error
    if started.tzinfo is None or started.utcoffset() is None:
        raise typer.BadParameter("stage start must be an aware ISO-8601 timestamp")
    started = started.astimezone(UTC)
    settings = _load(config)
    evidence = asyncio.run(
        build_aggressive_laptop_stage_evidence_from_journal(
            Path(settings.storage.sqlite_path),
            load_aggressive_qualification_binding(binding.resolve()),
            stage=stage,
            started_at=started,
            ended_at=datetime.now(UTC),
            post_flat_service_seconds=0,
        )
    )
    payload = asdict(evidence)
    payload["stable_flat"] = evidence.active_action_count == 0
    typer.echo(json.dumps(payload, default=str, sort_keys=True))


@app.command("aggressive-laptop-promote-pilot-a")
def aggressive_laptop_promote_pilot_a(
    canary_evidence: Annotated[Path, typer.Option("--canary-evidence")],
    binding: Annotated[Path, typer.Option("--binding")],
    confirmation: Annotated[str, typer.Option("--confirmation")],
    actor: Annotated[str, typer.Option("--actor")] = "laptop-owner",
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Promote the local laptop from canary to pilot_a using exact durable evidence."""
    if confirmation != PILOT_A_OWNER_CONFIRMATION:
        raise typer.BadParameter("aggressive pilot_a owner confirmation is invalid")
    settings = _load(config)
    loaded_binding = load_aggressive_qualification_binding(binding.resolve())
    evidence = load_aggressive_laptop_stage_evidence(canary_evidence.resolve())
    if (
        evidence.stage != "canary"
        or not evidence.accepted
        or evidence.aggressive_binding_sha256 != loaded_binding.binding_sha256
    ):
        raise typer.BadParameter("accepted exact aggressive canary evidence is required")
    table = load_locked_risk_stage_table(config.resolve().parent / "RUNTIME_POLICY.yaml")
    fresh_flat = asyncio.run(
        collect_authoritative_live_flat_evidence(
            settings,
            loaded_binding.qualification_route.partition(":")[0],
        )
    )
    if (
        not fresh_flat.stable_flat
        or fresh_flat.reconciliation_sha256 != evidence.reconciliation_evidence_sha256
    ):
        raise typer.BadParameter("fresh private stable-FLAT evidence is required")

    async def promote() -> RiskStageState:
        state_path = Path(settings.storage.sqlite_path)
        await initialise_state(state_path)
        current = await read_risk_stage(state_path)
        if (
            current.stage != RiskStage.CANARY
            or current.promoted_at is None
            or current.qualification_hash != loaded_binding.qualification_hash
            or current.runtime_policy_sha256 != table.runtime_policy_sha256
        ):
            raise RuntimeError("current canary stage identity is unavailable")
        rebuilt = await build_aggressive_laptop_stage_evidence_from_journal(
            state_path,
            loaded_binding,
            stage="canary",
            started_at=evidence.started_at,
            ended_at=evidence.ended_at,
            post_flat_service_seconds=evidence.post_flat_service_seconds,
            authoritative_stable_flat=True,
            authoritative_private_event_watermark=evidence.final_private_event_watermark,
            authoritative_reconciliation_sha256=evidence.reconciliation_evidence_sha256,
        )
        if rebuilt != evidence:
            raise RuntimeError("canary evidence does not match the current durable journal")
        journal = LiveOrderJournal(state_path)
        await journal.initialise()
        if await journal.active_actions():
            raise RuntimeError("pilot promotion requires stable FLAT and zero active actions")
        completed = tuple(
            sorted(
                await journal.completed_actions_since(
                    current.promoted_at,
                    loaded_binding.qualification_hash,
                ),
                key=lambda action: action.pair_action_id,
            )
        )
        completed_ids = tuple(action.pair_action_id for action in completed)
        completed_sha256 = completed_normal_actions_sha256(completed)
        if completed_sha256 != evidence.completed_actions_sha256:
            raise RuntimeError("canary completion set changed before pilot promotion")
        journal_actions_sha256 = hashlib.sha256(
            json.dumps(completed_ids, separators=(",", ":")).encode()
        ).hexdigest()
        await record_risk_stage_result(
            state_path,
            RiskStage.CANARY,
            loaded_binding.qualification_hash,
            table.runtime_policy_sha256,
            evidence.evidence_sha256,
            True,
            actor,
            await journal.event_watermark(),
            completed_sha256,
            journal_actions_sha256,
            completed_ids,
        )
        return await promote_risk_stage(
            state_path,
            RiskStage.CANARY,
            RiskStage.PILOT_A,
            loaded_binding.qualification_hash,
            table.runtime_policy_sha256,
            actor,
            "PROMOTE:pilot_a",
        )

    result = asyncio.run(promote())
    typer.echo(json.dumps(asdict(result), default=str, sort_keys=True))


@app.command("aggressive-vps-handoff-check")
def aggressive_vps_handoff_check(
    acceptance: Annotated[Path, typer.Option("--acceptance")],
    runtime_manifest: Annotated[Path, typer.Option("--runtime-manifest")],
    binding: Annotated[Path, typer.Option("--binding")],
    qualification: Annotated[Path, typer.Option("--qualification")],
    model: Annotated[Path, typer.Option("--model")],
    grid: Annotated[Path, typer.Option("--grid")],
    live_grid: Annotated[Path, typer.Option("--live-grid")],
    canary_evidence: Annotated[Path, typer.Option("--canary-evidence")],
    pilot_evidence: Annotated[Path, typer.Option("--pilot-evidence")],
    profile: Annotated[Path, typer.Option("--profile")] = Path(
        "config/AGGRESSIVE_SYMBIOSIS_V1.yaml"
    ),
    history_root: Annotated[Path, typer.Option("--history-root")] = Path("data/reference-history"),
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Verify a future VPS handoff without connecting to or modifying any VPS."""
    required = (
        acceptance,
        runtime_manifest,
        binding,
        qualification,
        model,
        grid,
        live_grid,
        canary_evidence,
        pilot_evidence,
        profile,
    )
    if any(not path.is_file() for path in required):
        raise typer.BadParameter("the complete exact laptop artifact set is required")
    settings = _load(config)
    loaded = load_aggressive_laptop_acceptance(acceptance.resolve())
    runtime = verify_native_runtime_manifest(
        runtime_manifest.resolve(),
        repo_root.resolve(),
        config.resolve(),
    )
    loaded_binding = load_aggressive_qualification_binding(binding.resolve())
    loaded_qualification = load_qualification(qualification.resolve())
    qualification_current, _ = qualification_is_current(
        loaded_qualification,
        repo_root.resolve(),
        config.resolve(),
        Path(settings.storage.parquet_dir),
        settings.live.qualification_max_age_seconds,
        expected_route=_parse_route(loaded_binding.qualification_route),
        current_container_image_digest=runtime.artifact_digest,
        accepted_policies=(
            qualification_policy_from_settings(settings),
            laptop_owner_exception_policy(settings),
        ),
        enforce_age=False,
    )
    if not qualification_current:
        raise typer.BadParameter("aggressive laptop qualification is no longer current")
    loaded_model = load_historical_model(model.resolve())
    _verify_aggressive_model_window(history_root.resolve(), loaded_model)
    grid_store = AggressiveGridStore(grid.resolve())
    grid_store.initialise()
    verify_aggressive_qualification_binding(
        loaded_binding,
        loaded_qualification,
        loaded_model,
        runtime,
        grid_store,
        profile_sha256=hashlib.sha256(profile.read_bytes()).hexdigest(),
    )
    live_grid_store = AggressiveGridStore(live_grid.resolve())
    live_grid_store.initialise()
    _require_aggressive_live_grid_flat(live_grid_store, loaded_binding)
    canary = load_aggressive_laptop_stage_evidence(canary_evidence.resolve())
    pilot = load_aggressive_laptop_stage_evidence(pilot_evidence.resolve())
    journal = LiveOrderJournal(Path(settings.storage.sqlite_path))
    post_pilot_tail = asyncio.run(
        journal.actions_updated_after(
            pilot.ended_at,
            loaded_binding.qualification_hash,
        )
    )
    if post_pilot_tail:
        raise typer.BadParameter("JOURNAL_CHANGED_AFTER_ACCEPTED_PILOT")
    fresh_flat = asyncio.run(
        collect_authoritative_live_flat_evidence(
            settings,
            loaded_binding.qualification_route.partition(":")[0],
        )
    )
    live_grid_store.initialise()
    _require_aggressive_live_grid_flat(live_grid_store, loaded_binding)
    post_private_tail = asyncio.run(
        journal.actions_updated_after(
            pilot.ended_at,
            loaded_binding.qualification_hash,
        )
    )
    if post_private_tail:
        raise typer.BadParameter("JOURNAL_CHANGED_AFTER_ACCEPTED_PILOT")
    if not fresh_flat.stable_flat or any(
        evidence.reconciliation_evidence_sha256 != fresh_flat.reconciliation_sha256
        for evidence in (canary, pilot)
    ):
        raise typer.BadParameter("fresh private stable-FLAT evidence is required for handoff")
    rebuilt_canary = asyncio.run(
        build_aggressive_laptop_stage_evidence_from_journal(
            Path(settings.storage.sqlite_path),
            loaded_binding,
            stage="canary",
            started_at=canary.started_at,
            ended_at=canary.ended_at,
            post_flat_service_seconds=canary.post_flat_service_seconds,
            authoritative_stable_flat=True,
            authoritative_private_event_watermark=canary.final_private_event_watermark,
            authoritative_reconciliation_sha256=canary.reconciliation_evidence_sha256,
        )
    )
    rebuilt_pilot = asyncio.run(
        build_aggressive_laptop_stage_evidence_from_journal(
            Path(settings.storage.sqlite_path),
            loaded_binding,
            stage="pilot_a",
            started_at=pilot.started_at,
            ended_at=pilot.ended_at,
            post_flat_service_seconds=pilot.post_flat_service_seconds,
            authoritative_stable_flat=True,
            authoritative_private_event_watermark=pilot.final_private_event_watermark,
            authoritative_reconciliation_sha256=pilot.reconciliation_evidence_sha256,
        )
    )
    if rebuilt_canary != canary or rebuilt_pilot != pilot:
        raise typer.BadParameter("laptop stage evidence is no longer current for handoff")
    rebuilt = build_aggressive_laptop_acceptance(
        loaded_binding,
        runtime,
        canary,
        pilot,
        now=loaded.accepted_at,
    )
    if rebuilt != loaded:
        raise typer.BadParameter("aggressive laptop acceptance artifact set changed")
    verify_aggressive_laptop_handoff(loaded, runtime)
    typer.echo(
        json.dumps(
            {
                "status": "PASS",
                "accepted": True,
                "release_sha": loaded.release_sha,
                "acceptance_sha256": loaded.acceptance_sha256,
                "vps_modified": False,
                "execution_authorized": False,
            },
            sort_keys=True,
        )
    )


def _require_aggressive_live_grid_flat(
    grid_store: AggressiveGridStore,
    binding: AggressiveQualificationBinding,
) -> None:
    active = {
        GridLevelState.ENTRY_PENDING,
        GridLevelState.OPEN,
        GridLevelState.EXIT_PENDING,
    }
    for direction in (binding.positive, binding.negative):
        levels = grid_store.levels(str(direction.route_identity))
        if len(levels) != 5 or any(level.state in active for level in levels):
            raise typer.BadParameter("aggressive live grid is not durably flat")


def _journal_action_effective_stress(risk_reservation: object) -> Decimal:
    if not isinstance(risk_reservation, dict):
        raise RuntimeError("live journal stress reservation is invalid")
    try:
        planned = Decimal(str(risk_reservation["projected_stress_usdt"]))
        actual = risk_reservation.get("actual_fill_risk")
        repriced = (
            Decimal(str(actual["incremental_stress_usdt"])) if isinstance(actual, dict) else planned
        )
    except (KeyError, ValueError, ArithmeticError) as error:
        raise RuntimeError("live journal stress reservation is incomplete") from error
    if not planned.is_finite() or not repriced.is_finite() or min(planned, repriced) <= 0:
        raise RuntimeError("live journal stress reservation is invalid")
    return max(planned, repriced)


def _required_order_decimal(value: Decimal | None) -> Decimal:
    if value is None or not value.is_finite():
        raise ValueError("live journal fill value is missing")
    return value


@app.command()
def health(config: ConfigPath = Path("config/defaults.yaml")) -> None:
    """Fail unless the persisted service heartbeat is current."""
    settings = _load(config)
    state_path = Path(settings.storage.sqlite_path)

    async def read_health() -> tuple[ServiceHealth, SupervisorHealth | None]:
        return (
            await read_service_health(state_path, settings.app.health_max_age_seconds),
            await read_supervisor_health(state_path),
        )

    result, supervisor = asyncio.run(read_health())
    typer.echo(
        json.dumps(
            {
                "status": "PASS" if result.healthy else "FAIL",
                "reason": result.reason,
                "service_status": result.status,
                "heartbeat_at": result.heartbeat_at,
                "starts": result.starts,
                "supervisor_mode": supervisor.mode if supervisor is not None else None,
                "active_pair_action_id": (
                    supervisor.active_pair_action_id if supervisor is not None else None
                ),
                "terminal_or_action_state": (
                    supervisor.action_state if supervisor is not None else None
                ),
                "recovery_required": (
                    supervisor.recovery_required if supervisor is not None else None
                ),
                "recovery_outcome": supervisor.outcome if supervisor is not None else None,
                "supervisor_failure": supervisor.failure if supervisor is not None else None,
            },
            default=str,
            sort_keys=True,
        )
    )
    if not result.healthy:
        raise typer.Exit(code=1)


@app.command("deployment-identity")
def deployment_identity(
    expected_release_sha: Annotated[str, typer.Option("--expected-release-sha")],
    expected_image_digest: Annotated[str, typer.Option("--expected-image-digest")],
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Fail unless the running shadow container has the exact deployed identity."""
    settings = _load(config)
    release_sha = os.environ.get("IPEG_RELEASE_SHA", "").strip().lower()
    image_digest = os.environ.get("IPEG_CONTAINER_IMAGE_DIGEST", "").strip().lower()
    expected_release = expected_release_sha.strip().lower()
    expected_image = expected_image_digest.strip().lower()
    passed = (
        re.fullmatch(r"[0-9a-f]{40}", expected_release) is not None
        and re.fullmatch(r"sha256:[0-9a-f]{64}", expected_image) is not None
        and release_sha == expected_release
        and image_digest == expected_image
        and settings.app.mode == "shadow"
        and settings.live.enabled is False
        and settings.execution.normal_unbounded_market_allowed is False
    )
    typer.echo(
        json.dumps(
            {
                "status": "PASS" if passed else "FAIL",
                "release_sha": release_sha,
                "image_digest": image_digest,
                "mode": settings.app.mode,
                "live_enabled": settings.live.enabled,
            },
            sort_keys=True,
        )
    )
    if not passed:
        raise typer.Exit(code=8)


@app.command()
def metrics() -> None:
    """Print the current Prometheus metric exposition."""
    typer.echo(render_metrics(), nl=False)


@app.command("autonomous-status")
def autonomous_status(config: ConfigPath = Path("config/defaults.yaml")) -> None:
    """Print the installed orchestrator's fail-closed runtime state."""
    settings = _load(config)
    path = Path(settings.storage.sqlite_path).parent / "autonomous-orchestrator.json"
    try:
        payload = load_autonomous_runtime_status(path)
    except (FileNotFoundError, ValueError) as error:
        typer.echo(json.dumps({"status": "FAIL", "reason": str(error)}, sort_keys=True))
        raise typer.Exit(code=1) from error
    typer.echo(json.dumps(payload, default=str, sort_keys=True))


def _scan_payload(result: ScanResult) -> dict[str, object]:
    return {
        "base": result.base,
        "common_instrument_count": result.common_instrument_count,
        "bbo": [asdict(quote) for quote in result.bbo],
        "funding": [asdict(snapshot) for snapshot in result.funding],
        "data_quality": [asdict(assessment) for assessment in result.data_quality],
        "routes": [asdict(quote) for quote in result.quotes],
        "capabilities": [asdict(report) for report in result.capabilities],
        "quarantined": [asdict(record) for record in result.quarantined],
        "venue_capability_matrix": (
            asdict(result.venue_capability_matrix)
            if result.venue_capability_matrix is not None
            else None
        ),
    }


async def _run_public_scan(
    settings: Settings,
    base: str,
    quantity: Decimal,
    timeout_seconds: int,
) -> ScanResult:
    engine = PublicMarketEngine(
        settings,
        public_venues=tuple(Venue(value) for value in settings.venues.public_runtime),
    )
    try:
        return await engine.scan_once(base, quantity, timeout_seconds)
    finally:
        await engine.close()


@app.command("public-scan")
def public_scan(
    config: ConfigPath = Path("config/defaults.yaml"),
    base: Annotated[str, typer.Option("--base")] = "BTC",
    quantity: Annotated[str, typer.Option("--quantity")] = "0.01",
    timeout_seconds: Annotated[int, typer.Option("--timeout", min=1, max=120)] = 30,
) -> None:
    """Print one capability-gated public-data route snapshot across configured waves."""
    settings = _load(config)
    try:
        parsed_quantity = Decimal(quantity)
    except InvalidOperation as error:
        raise typer.BadParameter("quantity must be a decimal number") from error
    if not parsed_quantity.is_finite() or parsed_quantity <= 0:
        raise typer.BadParameter("quantity must be a positive finite decimal")
    result = asyncio.run(_run_public_scan(settings, base, parsed_quantity, timeout_seconds))
    typer.echo(json.dumps(_scan_payload(result), default=str, sort_keys=True))
    if not any(quote.eligible for quote in result.quotes):
        raise typer.Exit(code=3)


def _one_base_instrument(instruments: tuple[Instrument, ...], base: str) -> Instrument:
    matches = tuple(instrument for instrument in instruments if instrument.base == base.upper())
    if len(matches) != 1:
        raise ValueError(f"expected exactly one active {base.upper()} linear-USDT perpetual")
    return matches[0]


async def _fetch_reference_source_pair(
    venue_a: Venue,
    venue_b: Venue,
    base: str,
    since: datetime,
    limit: int,
) -> tuple[tuple[SourceMinuteBar, ...], tuple[SourceMinuteBar, ...], Instrument, Instrument]:
    adapter_a = CcxtProAdapter(venue_a)
    adapter_b = CcxtProAdapter(venue_b)
    try:
        instruments_a, instruments_b = await asyncio.gather(
            adapter_a.discover_instruments(),
            adapter_b.discover_instruments(),
        )
        instrument_a = _one_base_instrument(instruments_a, base)
        instrument_b = _one_base_instrument(instruments_b, base)
        bars_a, bars_b = await asyncio.gather(
            adapter_a.fetch_closed_minute_bars(instrument_a, since, limit),
            adapter_b.fetch_closed_minute_bars(instrument_b, since, limit),
        )
        return bars_a, bars_b, instrument_a, instrument_b
    finally:
        await asyncio.gather(adapter_a.close(), adapter_b.close(), return_exceptions=True)


async def _fetch_reference_source_pair_window(
    venue_a: Venue,
    venue_b: Venue,
    base: str,
    start: datetime,
    end: datetime,
    batch_limit: int,
    store: ParquetReferenceHistoryStore,
) -> tuple[tuple[SourceMinuteBar, ...], tuple[SourceMinuteBar, ...], Instrument, Instrument]:
    """Fetch an exact closed-minute window in bounded, resumable API pages."""
    adapter_a = CcxtProAdapter(venue_a)
    adapter_b = CcxtProAdapter(venue_b)
    try:
        instruments_a, instruments_b = await asyncio.gather(
            adapter_a.discover_instruments(),
            adapter_b.discover_instruments(),
        )
        instrument_a = _one_base_instrument(instruments_a, base)
        instrument_b = _one_base_instrument(instruments_b, base)
        rows_a: list[SourceMinuteBar] = []
        rows_b: list[SourceMinuteBar] = []
        cursor = start
        while cursor < end:
            remaining = int((end - cursor).total_seconds() // 60)
            page_size = min(batch_limit, remaining)
            page_end = cursor + timedelta(minutes=page_size)
            cached_a = store.query_source_bars(
                venue=venue_a,
                symbol=instrument_a.symbol,
                start=cursor,
                end=page_end,
            )
            cached_b = store.query_source_bars(
                venue=venue_b,
                symbol=instrument_b.symbol,
                start=cursor,
                end=page_end,
            )
            expected_minutes = {cursor + timedelta(minutes=index) for index in range(page_size)}
            cached_complete = {bar.interval_start for bar in cached_a} == expected_minutes and {
                bar.interval_start for bar in cached_b
            } == expected_minutes
            if cached_complete:
                page_a, page_b = cached_a, cached_b
            else:
                fetched_a, fetched_b = await asyncio.gather(
                    adapter_a.fetch_closed_minute_bars(instrument_a, cursor, page_size),
                    adapter_b.fetch_closed_minute_bars(instrument_b, cursor, page_size),
                )
                page_a = _normalize_source_page_duplicates(
                    tuple(bar for bar in fetched_a if cursor <= bar.interval_start < page_end)
                )
                page_b = _normalize_source_page_duplicates(
                    tuple(bar for bar in fetched_b if cursor <= bar.interval_start < page_end)
                )
                store.append_source_bars(page_a)
                store.append_source_bars(page_b)
            rows_a.extend(page_a)
            rows_b.extend(page_b)
            # Exchanges may silently cap OHLC responses below the requested
            # limit. Advance only through the prefix both venues had a chance
            # to return; the longer page is idempotently re-read on the next
            # iteration instead of creating a permanent hole.
            last_a = max(
                (bar.interval_start for bar in page_a), default=page_end - timedelta(minutes=1)
            )
            last_b = max(
                (bar.interval_start for bar in page_b), default=page_end - timedelta(minutes=1)
            )
            cursor = min(last_a, last_b) + timedelta(minutes=1)
        return (
            tuple(sorted(rows_a, key=lambda bar: bar.interval_start)),
            tuple(sorted(rows_b, key=lambda bar: bar.interval_start)),
            instrument_a,
            instrument_b,
        )
    finally:
        await asyncio.gather(adapter_a.close(), adapter_b.close(), return_exceptions=True)


def _normalize_source_page_duplicates(
    bars: tuple[SourceMinuteBar, ...],
) -> tuple[SourceMinuteBar, ...]:
    """Persist one explicit ambiguity marker instead of last-write-wins OHLC."""
    grouped: dict[datetime, list[SourceMinuteBar]] = {}
    for bar in bars:
        grouped.setdefault(bar.interval_start, []).append(bar)
    normalized: list[SourceMinuteBar] = []
    for interval_start in sorted(grouped):
        candidates = grouped[interval_start]
        first = candidates[0]
        normalized.append(
            first
            if all(candidate == first for candidate in candidates[1:])
            else replace(first, quality=SourceBarQuality.AMBIGUOUS_DUPLICATE)
        )
    return tuple(normalized)


@app.command("reference-history-proof")
def reference_history_proof(
    venue_a: Annotated[Venue, typer.Option("--venue-a")],
    venue_b: Annotated[Venue, typer.Option("--venue-b")],
    since: Annotated[str, typer.Option("--since")],
    end: Annotated[str | None, typer.Option("--end")] = None,
    base: Annotated[str, typer.Option("--base")] = "BTC",
    limit: Annotated[int, typer.Option("--limit", min=1, max=1000)] = 1000,
    output_root: Annotated[Path, typer.Option("--output-root")] = Path("data/reference-history"),
    profile: Annotated[Path, typer.Option("--profile")] = Path(
        "config/AGGRESSIVE_SYMBIOSIS_V1.yaml"
    ),
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Build one bounded public 1m reference-history proof; never submit an order."""
    _load(config)
    if venue_a == venue_b:
        raise typer.BadParameter("reference history requires two distinct venues")
    try:
        raw_since = datetime.fromisoformat(since)
    except ValueError as error:
        raise typer.BadParameter("since must be an ISO-8601 timestamp with UTC offset") from error
    if raw_since.tzinfo is None or raw_since.utcoffset() is None:
        raise typer.BadParameter("since must include a UTC offset")
    parsed_since = raw_since.astimezone(UTC)
    if parsed_since.second != 0 or parsed_since.microsecond != 0:
        raise typer.BadParameter("since must be aligned to an exact UTC minute")
    if not profile.is_file():
        raise typer.BadParameter("aggressive strategy profile is missing")
    latest_closed = datetime.now(UTC).replace(second=0, microsecond=0)
    if end is None:
        requested_end = parsed_since + timedelta(minutes=limit)
    else:
        try:
            raw_end = datetime.fromisoformat(end)
        except ValueError as error:
            raise typer.BadParameter("end must be an ISO-8601 timestamp with UTC offset") from error
        if raw_end.tzinfo is None or raw_end.utcoffset() is None:
            raise typer.BadParameter("end must include a UTC offset")
        requested_end = raw_end.astimezone(UTC)
        if requested_end.second != 0 or requested_end.microsecond != 0:
            raise typer.BadParameter("end must be aligned to an exact UTC minute")
    bounded_end = min(latest_closed, requested_end)
    if bounded_end <= parsed_since:
        raise typer.BadParameter("since must precede the latest closed UTC minute")
    store = ParquetReferenceHistoryStore(output_root.resolve())
    raw_a, raw_b, instrument_a, instrument_b = asyncio.run(
        _fetch_reference_source_pair_window(
            venue_a,
            venue_b,
            base,
            parsed_since,
            bounded_end,
            limit,
            store,
        )
    )
    bars_a = tuple(raw_a)
    bars_b = tuple(raw_b)
    if instrument_a.key != instrument_b.key:
        raise typer.BadParameter("venue instruments do not share one canonical contract identity")
    store.append_source_bars(bars_a)
    store.append_source_bars(bars_b)
    window_end = bounded_end
    cached_a = store.query_source_bars(
        venue=venue_a,
        symbol=instrument_a.symbol,
        start=parsed_since,
        end=window_end,
    )
    cached_b = store.query_source_bars(
        venue=venue_b,
        symbol=instrument_b.symbol,
        start=parsed_since,
        end=window_end,
    )
    result = build_reference_series(
        cached_a,
        cached_b,
        window_start=parsed_since,
        window_end=window_end,
    )
    directed_routes = directed_routes_for_reference_pair(base, venue_a, venue_b)
    store.append_reference_bars(result.bars)
    window_manifest = store.write_window_manifest(result, cached_a, cached_b)
    store.verify_window_manifest(window_manifest)
    aggregates = {
        str(minutes): aggregate_reference_bars(
            result.bars,
            minutes,
            window_start=parsed_since,
            window_end=window_end,
            venue_a=result.venue_a,
            venue_b=result.venue_b,
            instrument=result.instrument,
        )
        for minutes in (5, 15, 60, 240, 1440)
    }
    rejection_counts = Counter(rejection.reason.value for rejection in result.rejections)
    payload = {
        "status": "PASS" if result.bars else "FAIL",
        "base": base.upper(),
        "venue_a": min(venue_a.value, venue_b.value),
        "venue_b": max(venue_a.value, venue_b.value),
        "positive_directed_route": directed_routes.positive.value,
        "negative_directed_route": directed_routes.negative.value,
        "source_rows": len(cached_a) + len(cached_b),
        "reference_rows": len(result.bars),
        "rejected_minutes": len(result.rejections),
        "rejection_reasons": dict(sorted(rejection_counts.items())),
        "coverage_start": result.bars[0].interval_start if result.bars else None,
        "coverage_end": result.bars[-1].interval_start if result.bars else None,
        "source_sha256": source_bars_sha256((*cached_a, *cached_b)),
        "reference_sha256": reference_bars_sha256(result.bars),
        "reference_dataset_sha256": result.dataset_sha256,
        "reference_window_manifest_sha256": window_manifest.manifest_sha256,
        "reference_window_manifest": str(
            output_root.resolve() / "windows" / f"{result.dataset_sha256}.json"
        ),
        "store_manifest_sha256": store.manifest_sha256(),
        "profile_sha256": hashlib.sha256(profile.read_bytes()).hexdigest(),
        "intervals": {
            name: {
                "complete": sum(bar.quality.value == "COMPLETE" for bar in bars),
                "incomplete": sum(bar.quality.value == "INCOMPLETE" for bar in bars),
            }
            for name, bars in aggregates.items()
        },
        "synthetic_high_low_envelope": True,
        "executable": False,
        "production_submit_calls": 0,
    }
    typer.echo(json.dumps(payload, default=str, sort_keys=True))
    if not result.bars:
        raise typer.Exit(code=3)


@app.command("aggressive-live-intent-once")
def aggressive_live_intent_once(
    binding: Annotated[Path, typer.Option("--binding")],
    qualification: Annotated[Path, typer.Option("--qualification")],
    runtime_manifest: Annotated[Path, typer.Option("--runtime-manifest")],
    output: Annotated[Path, typer.Option("--output")],
    model: Annotated[Path, typer.Option("--model")] = Path(
        "state/aggressive-historical-model.json"
    ),
    history_root: Annotated[Path, typer.Option("--history-root")] = Path("data/reference-history"),
    grid: Annotated[Path, typer.Option("--grid")] = Path("state/aggressive-live-grid.sqlite3"),
    qualification_grid: Annotated[Path, typer.Option("--qualification-grid")] = Path(
        "state/aggressive-grid.sqlite3"
    ),
    profile: Annotated[Path, typer.Option("--profile")] = Path(
        "config/AGGRESSIVE_SYMBIOSIS_V1.yaml"
    ),
    timeout_seconds: Annotated[int, typer.Option("--timeout", min=1, max=120)] = 30,
    stage: Annotated[AggressiveLaptopLiveStage, typer.Option("--stage")] = (
        AggressiveLaptopLiveStage.CANARY
    ),
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Create one fresh shared LIVE-mode intent; never authorize or submit it."""
    settings = _load(config)
    if settings.app.mode != "shadow" or settings.live.enabled:
        raise typer.BadParameter("live intent preparation requires shadow mode and live=false")
    loaded_binding = load_aggressive_qualification_binding(binding.resolve())
    loaded_qualification = load_qualification(qualification.resolve())
    loaded_model = load_historical_model(model.resolve())
    loaded_runtime = load_native_runtime_manifest(runtime_manifest.resolve())
    qualification_grid_store = AggressiveGridStore(qualification_grid.resolve())
    qualification_grid_store.initialise()
    verify_aggressive_qualification_binding(
        loaded_binding,
        loaded_qualification,
        loaded_model,
        loaded_runtime,
        qualification_grid_store,
        profile_sha256=hashlib.sha256(profile.read_bytes()).hexdigest(),
    )
    grid_store = AggressiveGridStore(grid.resolve())
    grid_store.initialise()
    for direction in (loaded_model.positive.direction, loaded_model.negative.direction):
        grid_store.initialise_route(
            loaded_model,
            direction,
            now=datetime.now(UTC),
            rearm_retreat_step_fraction=Decimal("0.25"),
        )

    async def synchronize_live_levels() -> bool:
        state_path = Path(settings.storage.sqlite_path)
        await initialise_state(state_path)
        current = await read_risk_stage(state_path)
        expected = (
            RiskStage.CANARY if stage == AggressiveLaptopLiveStage.CANARY else RiskStage.PILOT_A
        )
        if (
            current.stage != expected
            or current.promoted_at is None
            or current.qualification_hash != loaded_binding.qualification_hash
        ):
            raise RuntimeError("aggressive laptop live stage is not current")
        journal = LiveOrderJournal(state_path)
        await journal.initialise()
        completed = await journal.completed_actions_since(
            current.promoted_at,
            loaded_binding.qualification_hash,
        )
        active = await journal.active_actions()
        actions = (*completed, *active)
        stage_actions = tuple(
            action
            for action in actions
            if action.risk_reservation.get("strategy") == "AGGRESSIVE_SYMBIOSIS_V1"
            and action.risk_reservation.get("stage") == stage.value
            and action.risk_reservation.get("aggressive_binding_sha256")
            == loaded_binding.binding_sha256
        )
        if len(stage_actions) != len(actions):
            raise RuntimeError("live stage journal contains an incompatible action")
        levels: set[int] = set()
        projections: list[ExternalGridLevelProjection] = []
        transitional = False
        for action in stage_actions:
            level = int(str(action.risk_reservation.get("level_index", 0)))
            if not 1 <= level <= (1 if stage == AggressiveLaptopLiveStage.CANARY else 5):
                raise RuntimeError("live stage journal level is invalid")
            if level in levels:
                raise RuntimeError("live stage journal contains a duplicate level")
            levels.add(level)
            stress = _journal_action_effective_stress(action.risk_reservation)
            if action in completed:
                projections.append(
                    ExternalGridLevelProjection(
                        level,
                        GridLevelState.CLOSED_WAIT_REARM,
                        Decimal(0),
                    )
                )
            elif action.state == LiveActionState.HEDGED:
                orders = await journal.latest_order_events(action.pair_action_id)
                opening_ids = action.risk_reservation.get("opening_client_order_ids")
                if not isinstance(opening_ids, dict):
                    raise RuntimeError("live journal opening identity is incomplete")
                by_id = {order.client_order_id: order for order in orders}
                try:
                    long_order = by_id[str(opening_ids["long"])]
                    short_order = by_id[str(opening_ids["short"])]
                    actual = action.risk_reservation["actual_fill_risk"]
                    if not isinstance(actual, dict):
                        raise ValueError("actual fill risk is absent")
                    ownership = GridTrancheOwnership(
                        tranche_id=action.tranche_id,
                        normalized_base_quantity=long_order.filled_base_quantity,
                        legs=(
                            GridLegFill(
                                action.route.long_venue,
                                long_order.symbol,
                                Side.BUY,
                                long_order.filled_base_quantity,
                                _required_order_decimal(long_order.average_price),
                                _required_order_decimal(long_order.fee_usdt),
                                Decimal(0),
                            ),
                            GridLegFill(
                                action.route.short_venue,
                                short_order.symbol,
                                Side.SELL,
                                short_order.filled_base_quantity,
                                _required_order_decimal(short_order.average_price),
                                _required_order_decimal(short_order.fee_usdt),
                                Decimal(0),
                            ),
                        ),
                        executable_entry_spread_bps=Decimal(str(actual["actual_entry_spread_bps"])),
                        reverse_target_bps=Decimal(
                            str(action.risk_reservation["target_exit_spread_bps"])
                        ),
                        effective_stop_bps=Decimal(
                            str(action.risk_reservation["effective_stop_bps"])
                        ),
                        maximum_holding_deadline=datetime.fromisoformat(
                            str(action.risk_reservation["hard_holding_deadline"])
                        ),
                        reserved_stress_usdt=stress,
                        entry_slippage_usdt=Decimal(0),
                        realised_pnl_usdt=Decimal(0),
                        unrealised_pnl_usdt=Decimal(0),
                        opened_at=datetime.fromisoformat(
                            str(action.risk_reservation["route_opened_at"])
                        ),
                    )
                except (KeyError, ValueError, ArithmeticError) as error:
                    raise RuntimeError("live journal hedge ownership is incomplete") from error
                projections.append(
                    ExternalGridLevelProjection(
                        level,
                        GridLevelState.OPEN,
                        stress,
                        ownership=ownership,
                    )
                )
            elif action.state in {
                LiveActionState.PREPARED,
                LiveActionState.PARTIAL,
                LiveActionState.FILLED,
                LiveActionState.RECOVERING,
            }:
                transitional = True
                projections.append(
                    ExternalGridLevelProjection(
                        level,
                        GridLevelState.ENTRY_PENDING,
                        stress,
                        decision_cycle=int(str(action.risk_reservation["decision_cycle"])),
                    )
                )
            else:
                transitional = True
        grid_store.synchronize_journal_levels(
            loaded_binding.qualification_route,
            tuple(projections),
            now=datetime.now(UTC),
        )
        return transitional

    if asyncio.run(synchronize_live_levels()):
        typer.echo(json.dumps({"status": "WAITING_FOR_DURABLE_LIVE_TRANSITION"}, sort_keys=True))
        raise typer.Exit(code=3)
    intents: list[AggressiveTrancheIntent] = []
    result = asyncio.run(
        _run_aggressive_shadow_once(
            settings,
            model.resolve(),
            history_root.resolve(),
            grid.resolve(),
            profile.resolve(),
            config.resolve(),
            timeout_seconds,
            runtime_mode=AggressiveRuntimeMode.LIVE,
            simulate_fills=False,
            private_fee_rates=loaded_qualification.private_taker_fee_rates,
            entry_stage=(
                AggressiveEntryStage.LOCKED_CANARY
                if stage == AggressiveLaptopLiveStage.CANARY
                else AggressiveEntryStage.NORMAL
            ),
            intent_sink=intents,
        )
    )
    selected = tuple(
        intent for intent in intents if intent.route_identity == loaded_binding.qualification_route
    )
    if len(selected) != 1 or (
        stage == AggressiveLaptopLiveStage.CANARY and selected[0].level_index != 1
    ):
        typer.echo(json.dumps(result, default=str, sort_keys=True))
        raise typer.Exit(code=3)
    envelope = AggressiveLiveIntentEnvelope(
        schema_version=1,
        generated_at=datetime.now(UTC),
        aggressive_binding_sha256=loaded_binding.binding_sha256,
        qualification_hash=loaded_binding.qualification_hash,
        intent=selected[0],
        intent_sha256=aggressive_intent_sha256(selected[0]),
    )
    save_aggressive_live_intent(output.resolve(), envelope)
    typer.echo(
        json.dumps(
            {
                "status": "PASS",
                "intent": asdict(envelope),
                "execution_authorized": False,
                "production_submit_calls": 0,
            },
            default=str,
            sort_keys=True,
        )
    )


@app.command("aggressive-model-proof")
def aggressive_model_proof(
    venue_a: Annotated[Venue, typer.Option("--venue-a")],
    venue_b: Annotated[Venue, typer.Option("--venue-b")],
    start: Annotated[str, typer.Option("--start")],
    end: Annotated[str, typer.Option("--end")],
    base: Annotated[str, typer.Option("--base")] = "BTC",
    history_root: Annotated[Path, typer.Option("--history-root")] = Path("data/reference-history"),
    artifact: Annotated[Path, typer.Option("--artifact")] = Path(
        "state/aggressive-historical-model.json"
    ),
    profile: Annotated[Path, typer.Option("--profile")] = Path(
        "config/AGGRESSIVE_SYMBIOSIS_V1.yaml"
    ),
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Build a deterministic non-executable historical model from local reference Parquet."""
    _load(config)
    if venue_a == venue_b:
        raise typer.BadParameter("historical model requires two distinct venues")
    try:
        parsed_start = datetime.fromisoformat(start)
        parsed_end = datetime.fromisoformat(end)
    except ValueError as error:
        raise typer.BadParameter("start and end must be ISO-8601 timestamps") from error
    if any(
        value.tzinfo is None or value.utcoffset() is None for value in (parsed_start, parsed_end)
    ):
        raise typer.BadParameter("start and end must include UTC offsets")
    parsed_start = parsed_start.astimezone(UTC)
    parsed_end = parsed_end.astimezone(UTC)
    if parsed_end <= parsed_start:
        raise typer.BadParameter("end must be later than start")
    if not profile.is_file():
        raise typer.BadParameter("aggressive strategy profile is missing")
    code_sha = current_code_commit_sha(Path(".").resolve())
    if code_sha is None:
        raise typer.BadParameter("exact code commit SHA is unavailable")
    canonical_a, canonical_b = sorted((venue_a, venue_b), key=lambda venue: venue.value)
    instrument = InstrumentKey(
        base=base.strip().upper(),
        quote="USDT",
        settle="USDT",
        product_type=ProductType.LINEAR_USDT_PERPETUAL,
    )
    store = ParquetReferenceHistoryStore(history_root.resolve())
    if not any((history_root.resolve() / "source").rglob("*.parquet")):
        raise typer.BadParameter("source history manifest is missing")
    try:
        window_manifest = store.find_window_manifest(
            venue_a=canonical_a,
            venue_b=canonical_b,
            instrument=instrument,
            start=parsed_start,
            end=parsed_end,
        )
        series = store.verify_window_manifest(window_manifest)
    except (OSError, ValueError) as error:
        raise typer.BadParameter(
            "an exact verified reference window manifest is required"
        ) from error
    bars = series.bars
    if not bars:
        raise typer.BadParameter("no reference bars exist for the requested identity/window")
    loaded_policy = load_historical_model_policy(profile)
    model = build_historical_reference_model(
        bars,
        rejections=series.rejections,
        policy=loaded_policy.policy,
        source_manifest_sha256=window_manifest.manifest_sha256,
        strategy_profile_sha256=loaded_policy.profile_sha256,
        code_sha=code_sha,
        window_start=window_manifest.window_start,
        window_end=window_manifest.window_end,
        reference_dataset_sha256=window_manifest.dataset_sha256,
    )
    model_hash = save_historical_model(artifact.resolve(), model)
    payload = historical_model_payload(model)
    typer.echo(
        json.dumps(
            {
                "status": "PASS",
                "model_sha256": model_hash,
                "model": payload,
                "artifact": str(artifact.resolve()),
                "reference_rows": len(bars),
                "reference_window_manifest_sha256": window_manifest.manifest_sha256,
                "reference_dataset_sha256": window_manifest.dataset_sha256,
                "positive_eligibility": model.positive.eligibility.value,
                "negative_eligibility": model.negative.eligibility.value,
                "executable": False,
                "production_submit_calls": 0,
            },
            sort_keys=True,
        )
    )
    if historical_model_sha256(model) != model_hash:
        raise typer.Exit(code=3)


@app.command("aggressive-qualification-bind")
def aggressive_qualification_bind(
    qualification: Annotated[Path, typer.Option("--qualification")],
    runtime_manifest: Annotated[Path, typer.Option("--runtime-manifest")],
    model: Annotated[Path, typer.Option("--model")] = Path(
        "state/aggressive-historical-model.json"
    ),
    grid: Annotated[Path, typer.Option("--grid")] = Path("state/aggressive-grid.sqlite3"),
    profile: Annotated[Path, typer.Option("--profile")] = Path(
        "config/AGGRESSIVE_SYMBIOSIS_V1.yaml"
    ),
    history_root: Annotated[Path, typer.Option("--history-root")] = Path("data/reference-history"),
    output: Annotated[Path, typer.Option("--output")] = Path("state/aggressive-qualification.json"),
) -> None:
    """Bind accepted qualification to exact aggressive geometry; never authorize execution."""
    for required in (qualification, runtime_manifest, model, grid, profile):
        if not required.is_file():
            raise typer.BadParameter(f"required aggressive binding input is missing: {required}")
    loaded_model = load_historical_model(model.resolve())
    _verify_aggressive_model_window(history_root.resolve(), loaded_model)
    grid_store = AggressiveGridStore(grid.resolve())
    grid_store.initialise()
    binding = build_aggressive_qualification_binding(
        load_qualification(qualification.resolve()),
        loaded_model,
        load_native_runtime_manifest(runtime_manifest.resolve()),
        grid_store,
        profile_sha256=hashlib.sha256(profile.read_bytes()).hexdigest(),
    )
    save_aggressive_qualification_binding(output.resolve(), binding)
    typer.echo(json.dumps(asdict(binding), default=str, sort_keys=True))


@app.command("aggressive-qualification-check")
def aggressive_qualification_check(
    binding: Annotated[Path, typer.Option("--binding")],
    qualification: Annotated[Path, typer.Option("--qualification")],
    runtime_manifest: Annotated[Path, typer.Option("--runtime-manifest")],
    model: Annotated[Path, typer.Option("--model")] = Path(
        "state/aggressive-historical-model.json"
    ),
    grid: Annotated[Path, typer.Option("--grid")] = Path("state/aggressive-grid.sqlite3"),
    profile: Annotated[Path, typer.Option("--profile")] = Path(
        "config/AGGRESSIVE_SYMBIOSIS_V1.yaml"
    ),
    history_root: Annotated[Path, typer.Option("--history-root")] = Path("data/reference-history"),
) -> None:
    """Fail closed unless every accepted aggressive qualification identity still matches."""
    for required in (binding, qualification, runtime_manifest, model, grid, profile):
        if not required.is_file():
            raise typer.BadParameter(f"required aggressive binding input is missing: {required}")
    loaded = load_aggressive_qualification_binding(binding.resolve())
    loaded_model = load_historical_model(model.resolve())
    _verify_aggressive_model_window(history_root.resolve(), loaded_model)
    grid_store = AggressiveGridStore(grid.resolve())
    grid_store.initialise()
    verify_aggressive_qualification_binding(
        loaded,
        load_qualification(qualification.resolve()),
        loaded_model,
        load_native_runtime_manifest(runtime_manifest.resolve()),
        grid_store,
        profile_sha256=hashlib.sha256(profile.read_bytes()).hexdigest(),
    )
    typer.echo(
        json.dumps(
            {
                "status": "PASS",
                "accepted": loaded.accepted,
                "binding_sha256": loaded.binding_sha256,
                "execution_authorized": False,
            },
            sort_keys=True,
        )
    )


def _aggressive_reserves_per_base(settings: Settings, price: Decimal) -> CostReserves:
    unit = price / Decimal(10_000)
    return CostReserves(
        entry_impact_usdt=Decimal(0),
        exit_impact_usdt=Decimal(0),
        entry_slippage_usdt=unit * settings.live.canary_entry_slippage_cap_bps,
        exit_slippage_usdt=unit * settings.live.canary_close_slippage_cap_bps,
        latency_usdt=unit * settings.execution.latency_reserve_bps,
        partial_fill_unmatched_usdt=unit * settings.execution.partial_fill_reserve_bps,
        emergency_hedge_usdt=unit * settings.execution.emergency_hedge_reserve_bps,
        reconciliation_forced_exit_usdt=(
            unit * settings.execution.reconciliation_forced_exit_reserve_bps
        ),
        liquidation_distance_usdt=Decimal(0),
    )


def _verify_aggressive_model_window(
    history_root: Path,
    model: HistoricalReferenceModel,
) -> None:
    store = ParquetReferenceHistoryStore(history_root)
    try:
        manifest = store.load_window_manifest(model.reference_manifest_sha256)
        series = store.verify_window_manifest(manifest)
    except (OSError, ValueError) as error:
        raise ValueError("aggressive model reference window is unavailable or changed") from error
    if (
        manifest.manifest_sha256 != model.source_manifest_sha256
        or series.dataset_sha256 != model.reference_manifest_sha256
        or manifest.window_start != model.window_start
        or manifest.window_end != model.window_end
    ):
        raise ValueError("aggressive model reference window identity changed")


def _aggressive_effective_stop(
    model: HistoricalReferenceModel,
    direction: DivergenceDirection,
) -> Decimal:
    direction_model = (
        model.positive if direction == DivergenceDirection.POSITIVE else model.negative
    )
    tail = model.window_30d.q999_abs_bps
    if tail is None:
        return direction_model.reference_stop_bps
    adaptive = (
        model.s0_bps + tail if direction == DivergenceDirection.POSITIVE else model.s0_bps - tail
    )
    return (
        max(direction_model.reference_stop_bps, adaptive)
        if direction == DivergenceDirection.POSITIVE
        else min(direction_model.reference_stop_bps, adaptive)
    )


async def _run_aggressive_shadow_once(
    settings: Settings,
    model_path: Path,
    history_root: Path,
    grid_path: Path,
    profile_path: Path,
    config_path: Path,
    timeout_seconds: int,
    *,
    runtime_mode: AggressiveRuntimeMode = AggressiveRuntimeMode.SHADOW,
    simulate_fills: bool = True,
    private_fee_rates: dict[Venue, Decimal] | None = None,
    entry_stage: AggressiveEntryStage = AggressiveEntryStage.NORMAL,
    intent_sink: list[AggressiveTrancheIntent] | None = None,
) -> dict[str, object]:
    model = load_historical_model(model_path)
    profile = load_aggressive_decision_policy(profile_path)
    if profile.profile_sha256 != model.strategy_profile_sha256:
        raise RuntimeError("aggressive model/profile identity mismatch")
    now = datetime.now(UTC)
    store = ParquetReferenceHistoryStore(history_root)
    _verify_aggressive_model_window(history_root, model)
    bars = store.query_reference_bars(
        venue_a=Venue(model.venue_a),
        venue_b=Venue(model.venue_b),
        instrument=InstrumentKey(
            model.base,
            "USDT",
            "USDT",
            ProductType.LINEAR_USDT_PERPETUAL,
        ),
        start=now - timedelta(days=2),
        end=now + timedelta(minutes=1),
    )
    if not bars:
        raise RuntimeError("aggressive shadow requires current reference history")
    reference_bar = max(bars, key=lambda item: item.interval_start)
    if reference_bar.interval_start + timedelta(minutes=2) < now:
        raise RuntimeError("aggressive reference minute is stale")
    grid = AggressiveGridStore(grid_path)
    grid.initialise()
    for direction in (model.positive.direction, model.negative.direction):
        grid.initialise_route(
            model,
            direction,
            now=now,
            rearm_retreat_step_fraction=Decimal("0.25"),
        )
    core = AggressiveDecisionCore(profile.policy)
    bridge = AggressiveShadowDecisionBridge(core, grid)
    portfolio = AggressiveShadowPortfolio(grid, profile.policy)
    runtime_identity = aggressive_runtime_manifest_sha256(model, config_hash(config_path))
    engine = PublicMarketEngine(
        settings,
        public_venues=tuple(Venue(value) for value in settings.venues.public_runtime),
    )
    decisions: list[AggressiveStrategyDecision] = []
    exits: list[tuple[int, AggressiveExitReason]] = []
    rearmed: dict[str, tuple[int, ...]] = {}
    try:
        broad = await engine.scan_once(
            model.base,
            settings.shadow.quantity,
            timeout_seconds,
        )
        active = frozenset(
            {
                (
                    model.base,
                    model.positive_route.split(":", 1)[1].split(">", 1)[0],
                    model.positive_route.rsplit(">", 1)[1],
                ),
                (
                    model.base,
                    model.negative_route.split(":", 1)[1].split(">", 1)[0],
                    model.negative_route.rsplit(">", 1)[1],
                ),
            }
        )
        await engine.scan_candidate_l2(
            timeout_seconds,
            active_route_keys=active,
            candidates_admitted=False,
            prefilter=broad.prefilter,
        )
        for confirmation in range(profile.policy.confirmation_snapshots):
            markets = await engine.aggressive_route_market_snapshots(timeout_seconds)
            for market in markets:
                if market.route.value not in {model.positive_route, model.negative_route}:
                    continue
                direction = (
                    model.positive.direction
                    if market.route.value == model.positive_route
                    else model.negative.direction
                )
                if simulate_fills:
                    exits.extend(
                        portfolio.close_due(
                            model=model,
                            reference_bar=reference_bar,
                            market=market,
                            now=datetime.now(UTC),
                            projected_portfolio_loss_usdt=sum(
                                (
                                    level.reserved_stress_usdt
                                    for route in (model.positive_route, model.negative_route)
                                    for level in grid.levels(route)
                                    if level.state == GridLevelState.OPEN
                                ),
                                Decimal(0),
                            ),
                        )
                    )
                levels = grid.levels(market.route.value)
                other_route_active = any(
                    level.state
                    in {
                        GridLevelState.ENTRY_PENDING,
                        GridLevelState.OPEN,
                        GridLevelState.EXIT_PENDING,
                    }
                    for route in (model.positive_route, model.negative_route)
                    if route != market.route.value
                    for level in grid.levels(route)
                )
                if other_route_active:
                    continue
                existing_route = sum(
                    (level.reserved_stress_usdt for level in levels),
                    Decimal(0),
                )
                existing_portfolio = sum(
                    (
                        level.reserved_stress_usdt
                        for route in (model.positive_route, model.negative_route)
                        for level in grid.levels(route)
                    ),
                    Decimal(0),
                )
                if market.long_book is None or market.short_book is None:
                    continue
                price = (
                    market.long_book.asks[0].price + market.short_book.bids[0].price
                ) / Decimal(2)
                decision = bridge.evaluate(
                    AggressiveShadowDecisionInput(
                        model=model,
                        reference_bar=reference_bar,
                        market=market,
                        effective_stop_bps=_aggressive_effective_stop(model, direction),
                        reserves=_aggressive_reserves_per_base(settings, price),
                        existing_route_loss_usdt=existing_route,
                        existing_portfolio_loss_usdt=existing_portfolio,
                        free_margin_usdt=settings.risk.reference_capital_usdt / Decimal(2),
                        decision_cycle=grid.next_decision_cycle(market.route.value),
                        runtime_manifest_sha256=runtime_identity,
                        maximum_book_age_ms=settings.market_data.max_l2_age_ms,
                        now=datetime.now(UTC),
                        runtime_mode=runtime_mode,
                        stage=entry_stage,
                        private_long_taker_fee_rate=(
                            private_fee_rates.get(market.long_instrument.venue)
                            if private_fee_rates is not None
                            else None
                        ),
                        private_short_taker_fee_rate=(
                            private_fee_rates.get(market.short_instrument.venue)
                            if private_fee_rates is not None
                            else None
                        ),
                    )
                )
                decisions.append(decision)
                if decision.accepted and decision.intent is not None and intent_sink is not None:
                    intent_sink.append(decision.intent)
                if decision.accepted and simulate_fills:
                    portfolio.open(decision)
            if confirmation + 1 < profile.policy.confirmation_snapshots:
                await asyncio.sleep(
                    profile.policy.confirmation_minimum_elapsed_ms
                    / 1000
                    / (profile.policy.confirmation_snapshots - 1)
                )
        if simulate_fills:
            for route, direction in (
                (model.positive_route, model.positive.direction),
                (model.negative_route, model.negative.direction),
            ):
                rearmed[route] = portfolio.rearm_stable_flat(
                    route,
                    reference_spread_bps=(
                        reference_bar.high_bps
                        if direction == DivergenceDirection.POSITIVE
                        else reference_bar.low_bps
                    ),
                    now=datetime.now(UTC),
                )
    finally:
        await engine.close()
    return {
        "status": "PASS",
        "model_sha256": historical_model_sha256(model),
        "profile_sha256": profile.profile_sha256,
        "runtime_manifest_sha256": runtime_identity,
        "reference_manifest_sha256": model.reference_manifest_sha256,
        "reference_minute": reference_bar.interval_start,
        "decisions": [asdict(item) for item in decisions],
        "accepted_decisions": sum(item.accepted for item in decisions),
        "exits": exits,
        "rearmed": rearmed,
        "grid": {
            route: [asdict(level) for level in grid.levels(route)]
            for route in (model.positive_route, model.negative_route)
        },
        "live_enabled": False,
        "production_submit_calls": 0,
    }


@app.command("aggressive-shadow-once")
def aggressive_shadow_once(
    model: Annotated[Path, typer.Option("--model")] = Path(
        "state/aggressive-historical-model.json"
    ),
    history_root: Annotated[Path, typer.Option("--history-root")] = Path("data/reference-history"),
    grid: Annotated[Path, typer.Option("--grid")] = Path("state/aggressive-grid.sqlite3"),
    profile: Annotated[Path, typer.Option("--profile")] = Path(
        "config/AGGRESSIVE_SYMBIOSIS_V1.yaml"
    ),
    timeout_seconds: Annotated[int, typer.Option("--timeout", min=1, max=120)] = 30,
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Run one native public aggressive cycle; never authorize or submit an order."""
    settings = _load(config)
    if settings.app.mode != "shadow" or settings.live.enabled:
        raise typer.BadParameter("aggressive public shadow requires mode=shadow and live=false")
    for required in (model, profile):
        if not required.is_file():
            raise typer.BadParameter(f"required aggressive artifact is missing: {required}")
    result = asyncio.run(
        _run_aggressive_shadow_once(
            settings,
            model.resolve(),
            history_root.resolve(),
            grid.resolve(),
            profile.resolve(),
            config.resolve(),
            timeout_seconds,
        )
    )
    typer.echo(json.dumps(result, default=str, sort_keys=True))


@app.command("native-runtime-manifest")
def native_runtime_manifest(
    output: Annotated[Path, typer.Option("--output")] = Path("state/native-runtime-manifest.json"),
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Bind native CPython, dependencies, source and config without Docker."""
    manifest = build_native_runtime_manifest(repo_root.resolve(), config.resolve())
    write_native_runtime_manifest(output.resolve(), manifest)
    typer.echo(json.dumps(asdict(manifest), default=str, sort_keys=True))


@app.command("run")
def run_service(config: ConfigPath = Path("config/defaults.yaml")) -> None:
    """Run the safe asynchronous bootstrap service."""
    settings = _load(config)
    decision = evaluate_live_order(settings, LiveContext())
    if settings.app.mode == "live" or decision.allowed:
        typer.echo("service refuses live mode without runtime gates", err=True)
        raise typer.Exit(code=2)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run_until_signal(settings))


@app.command("run-for")
def run_service_for(
    duration_seconds: Annotated[
        int,
        typer.Option("--duration-seconds", min=1, max=86_400),
    ],
    receipt: Annotated[Path | None, typer.Option("--receipt")] = None,
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Run shadow/recovery service for one exact bounded observation interval."""
    settings = _load(config)
    decision = evaluate_live_order(settings, LiveContext())
    if settings.app.mode == "live" or decision.allowed:
        typer.echo("bounded service refuses live mode without runtime gates", err=True)
        raise typer.Exit(code=2)
    result = asyncio.run(run_for_duration(settings, duration_seconds))
    if receipt is not None:
        write_bounded_service_receipt(receipt.resolve(), result)
    typer.echo(json.dumps(asdict(result), default=str, sort_keys=True))


@app.command("laptop-qualification-run")
def laptop_qualification_run(
    maximum_hours: Annotated[
        float,
        typer.Option("--maximum-hours", min=12, max=30),
    ] = 30,
    laptop_owner_exception_12h: Annotated[
        bool,
        typer.Option("--laptop-owner-exception-12h"),
    ] = False,
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Run native shadow until the selected exact laptop qualification finalizes."""
    settings = _load(config)
    if settings.app.mode != "shadow" or settings.live.enabled:
        raise typer.BadParameter("laptop qualification requires shadow mode and live disabled")
    release_sha = current_code_commit_sha(repo_root.resolve())
    if release_sha is None:
        raise typer.BadParameter("exact release commit SHA is unavailable")
    runtime_digest = resolve_runtime_artifact_digest(repo_root.resolve(), config.resolve())
    route = _parse_route(os.environ.get("IPEG_QUALIFICATION_ROUTE", ""))
    if route.value != "BTC:bybit>okx":
        raise typer.BadParameter("laptop qualification route must be BTC:bybit>okx")
    policy = _selected_qualification_policy(settings, laptop_owner_exception_12h)
    if not laptop_owner_exception_12h and maximum_hours < 24:
        raise typer.BadParameter("standard laptop qualification requires at least 24 hours")
    epoch = asyncio.run(
        run_until_qualification_finalized(
            settings,
            LaptopQualificationIdentity(route, release_sha, runtime_digest),
            maximum_seconds=maximum_hours * 3600,
            qualification_policy=policy,
        )
    )
    typer.echo(json.dumps(asdict(epoch), default=str, sort_keys=True))


@app.command("laptop-pilot-report")
def laptop_pilot_report(
    started_at: Annotated[datetime, typer.Option("--started-at")],
    ended_at: Annotated[datetime, typer.Option("--ended-at")],
    qualification: Annotated[
        Path,
        typer.Option("--qualification", exists=True, dir_okay=False, readable=True),
    ] = Path("state/qualification.json"),
    service_receipt: Annotated[
        Path,
        typer.Option("--service-receipt", exists=True, dir_okay=False, readable=True),
    ] = Path("state/laptop-service-receipt.json"),
    output: Annotated[Path, typer.Option("--output")] = Path("state/laptop-pilot-report.json"),
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Prove one real paired canary followed by eight native service hours."""
    settings = _load(config)
    evidence = load_qualification(qualification.resolve())
    bounded_service = load_bounded_service_receipt(service_receipt.resolve())
    runtime_digest = resolve_runtime_artifact_digest(repo_root.resolve(), config.resolve())
    report = asyncio.run(
        build_laptop_pilot_report(
            settings,
            evidence,
            started_at,
            ended_at,
            runtime_digest,
            bounded_service,
        )
    )
    write_laptop_pilot_report(output.resolve(), report)
    typer.echo(json.dumps(asdict(report), default=str, sort_keys=True))
    if report.status != "PASS":
        raise typer.Exit(code=7)


@app.command("shadow-status")
def shadow_status(config: ConfigPath = Path("config/defaults.yaml")) -> None:
    """Print persisted shadow controls, positions, opportunities, and data health."""
    runtime = ShadowRuntime(_load(config))

    async def read() -> dict[str, object]:
        await runtime.start()
        return await runtime.snapshot()

    typer.echo(json.dumps(asyncio.run(read()), default=str, sort_keys=True))


@app.command("qualify")
def qualify(
    config: ConfigPath = Path("config/defaults.yaml"),
    evidence: Annotated[Path, typer.Option("--evidence")] = Path("state/qualification.json"),
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    runtime_evidence: Annotated[
        Path,
        typer.Option(
            "--runtime-evidence",
            exists=True,
            dir_okay=False,
            readable=True,
        ),
    ] = Path("state/qualification-runtime.json"),
    laptop_owner_exception_12h: Annotated[
        bool,
        typer.Option("--laptop-owner-exception-12h"),
    ] = False,
) -> None:
    """Write exact-route, release/image/data-bound qualification evidence."""
    settings = _load(config)
    policy = _selected_qualification_policy(settings, laptop_owner_exception_12h)
    result = run_qualification(
        repo_root.resolve(),
        config.resolve(),
        Path(settings.storage.parquet_dir).resolve(),
        evidence.resolve(),
        runtime_evidence=load_runtime_evidence(runtime_evidence.resolve()),
        policy=policy,
    )
    typer.echo(json.dumps(asdict(result), default=str, sort_keys=True))
    if not result.accepted:
        raise typer.Exit(code=4)


@app.command("qualification-epoch-start")
def qualification_epoch_start(
    route: Annotated[str, typer.Option("--route")],
    container_image_digest: Annotated[
        str,
        typer.Option("--container-image-digest", envvar="IPEG_CONTAINER_IMAGE_DIGEST"),
    ],
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Idempotently start one exact release/config/image/route observation epoch."""
    settings = _load(config)
    selected_route = _parse_route(route)
    release_sha = current_code_commit_sha(repo_root.resolve())
    if release_sha is None:
        raise typer.BadParameter("exact release commit SHA is unavailable")

    async def start() -> QualificationEpoch:
        state_path = Path(settings.storage.sqlite_path)
        await initialise_state(state_path)
        return await start_qualification_epoch(
            state_path,
            selected_route,
            release_sha,
            code_hash(repo_root.resolve()),
            config_hash(config.resolve()),
            container_image_digest.lower(),
        )

    typer.echo(json.dumps(asdict(asyncio.run(start())), default=str, sort_keys=True))


@app.command("qualification-epoch-status")
def qualification_epoch_status(
    epoch_id: Annotated[str | None, typer.Option("--epoch-id")] = None,
    laptop_owner_exception_12h: Annotated[
        bool,
        typer.Option("--laptop-owner-exception-12h"),
    ] = False,
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Print exact selected-policy progress and fail-closed blockers as JSON."""
    settings = _load(config)
    policy = _selected_qualification_policy(settings, laptop_owner_exception_12h)

    async def status() -> QualificationProgress:
        state_path = Path(settings.storage.sqlite_path)
        await initialise_state(state_path)
        return await build_qualification_progress(
            state_path,
            Path(settings.storage.parquet_dir),
            epoch_id,
            policy,
        )

    try:
        progress = asyncio.run(status())
    except KeyError:
        raise typer.Exit(code=4) from None
    typer.echo(json.dumps(asdict(progress), default=str, sort_keys=True))


@app.command("qualification-epoch-finalize")
def qualification_epoch_finalize(
    epoch_id: Annotated[str, typer.Option("--epoch-id")],
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Idempotently close an exact epoch to further observations."""
    settings = _load(config)

    async def finalize() -> QualificationEpoch:
        state_path = Path(settings.storage.sqlite_path)
        await initialise_state(state_path)
        return await finalize_qualification_epoch(state_path, epoch_id)

    typer.echo(json.dumps(asdict(asyncio.run(finalize())), default=str, sort_keys=True))


@app.command("deployment-upgrade-gate")
def deployment_upgrade_gate(
    action: Annotated[str, typer.Option("--action")],
    owner_token: Annotated[str, typer.Option("--owner-token")],
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Atomically freeze new live entry before deployment or release it after recovery."""
    if action not in {"arm", "release"}:
        raise typer.BadParameter("action must be arm or release")
    settings = _load(config)

    async def update_gate() -> tuple[DeploymentUpgradeGate, bool]:
        state_path = Path(settings.storage.sqlite_path)
        journal = LiveOrderJournal(state_path)
        if action == "arm" and state_path.is_file():
            legacy_result = await journal.arm_legacy_deployment_upgrade(owner_token)
            if legacy_result.active_action_count > 0:
                return legacy_result, True
        await initialise_state(state_path)
        await journal.initialise()
        result = (
            await journal.arm_deployment_upgrade(owner_token)
            if action == "arm"
            else await journal.release_deployment_upgrade(owner_token)
        )
        return result, action == "arm" and result.active_action_count > 0

    result, blocked = asyncio.run(update_gate())
    typer.echo(json.dumps(asdict(result), default=str, sort_keys=True))
    if blocked:
        typer.echo("deployment upgrade requires zero active live actions", err=True)
        raise typer.Exit(6)


@app.command("risk-stage-status")
def risk_stage_status(config: ConfigPath = Path("config/defaults.yaml")) -> None:
    """Print the persisted stage and exact locked limits without authorizing promotion."""
    settings = _load(config)
    table = load_locked_risk_stage_table(config.resolve().parent / "RUNTIME_POLICY.yaml")

    async def status() -> RiskStageState:
        state_path = Path(settings.storage.sqlite_path)
        await initialise_state(state_path)
        return await read_risk_stage(state_path)

    typer.echo(
        json.dumps(
            {"state": asdict(asyncio.run(status())), "locked_table": asdict(table)},
            default=str,
            sort_keys=True,
        )
    )


@app.command("risk-stage-promote")
def risk_stage_promote(
    expected_current: Annotated[RiskStage, typer.Option("--expected-current")],
    target: Annotated[RiskStage, typer.Option("--target")],
    actor: Annotated[str, typer.Option("--actor")],
    confirmation: Annotated[str, typer.Option("--confirmation")],
    qualification: Annotated[
        Path,
        typer.Option("--qualification", exists=True, dir_okay=False, readable=True),
    ],
    container_image_digest: Annotated[
        str,
        typer.Option("--container-image-digest", envvar="IPEG_CONTAINER_IMAGE_DIGEST"),
    ],
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    laptop_owner_exception_12h: Annotated[
        bool,
        typer.Option("--laptop-owner-exception-12h"),
    ] = False,
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Persist one adjacent owner-confirmed promotion after exact qualification validation."""
    settings = _load(config)
    evidence = load_qualification(qualification.resolve())
    standard_policy = _qualification_policy(settings)
    selected_policy = _selected_qualification_policy(settings, laptop_owner_exception_12h)
    current, reason = qualification_is_current(
        evidence,
        repo_root.resolve(),
        config.resolve(),
        Path(settings.storage.parquet_dir).resolve(),
        settings.live.qualification_max_age_seconds,
        current_container_image_digest=container_image_digest.lower(),
        accepted_policies=(selected_policy,)
        if selected_policy != standard_policy
        else (standard_policy,),
    )
    if not current:
        raise typer.BadParameter(f"qualification is not current: {reason.value}")
    table = load_locked_risk_stage_table(config.resolve().parent / "RUNTIME_POLICY.yaml")

    async def promote() -> RiskStageState:
        state_path = Path(settings.storage.sqlite_path)
        await initialise_state(state_path)
        journal = LiveOrderJournal(state_path)
        await journal.initialise()
        if await journal.active_actions():
            raise RuntimeError("risk stage promotion requires zero active live actions")
        return await promote_risk_stage(
            state_path,
            expected_current,
            target,
            evidence.qualification_hash,
            table.runtime_policy_sha256,
            actor,
            confirmation,
        )

    result = asyncio.run(promote())
    typer.echo(json.dumps(asdict(result), default=str, sort_keys=True))


@app.command("risk-stage-complete")
def risk_stage_complete(
    stage: Annotated[RiskStage, typer.Option("--stage")],
    actor: Annotated[str, typer.Option("--actor")],
    evidence: Annotated[
        Path,
        typer.Option("--evidence", exists=True, dir_okay=False, readable=True),
    ],
    qualification: Annotated[
        Path,
        typer.Option("--qualification", exists=True, dir_okay=False, readable=True),
    ],
    attestation_public_key: Annotated[
        Path,
        typer.Option("--attestation-public-key", exists=True, dir_okay=False, readable=True),
    ],
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Bind one signed, account-wide completed stage before any next promotion."""
    settings = _load(config)
    policy_path = config.resolve().parent / "RUNTIME_POLICY.yaml"
    table = load_locked_risk_stage_table(policy_path)
    limits = next((item for item in table.stages if item.stage == stage), None)
    if limits is None:
        raise typer.BadParameter("shadow cannot produce a live risk-stage result")
    attestation_policy = load_region_latency_policy(policy_path)
    try:
        attested = verify_risk_stage_completion_evidence(
            evidence.resolve(),
            attestation_public_key.resolve(),
            attestation_policy.attestation_public_key_sha256,
            limits,
            required_consecutive_snapshots=table.flat_barrier_snapshots,
            required_quiet_period_seconds=table.flat_barrier_quiet_seconds,
            hard_maximum_holding_seconds=table.hard_maximum_holding_seconds,
        )
    except (ValueError, json.JSONDecodeError) as error:
        raise typer.BadParameter(str(error)) from error
    qualification_evidence = load_qualification(qualification.resolve())

    async def complete() -> RiskStageResult:
        state_path = Path(settings.storage.sqlite_path)
        await initialise_state(state_path)
        current = await read_risk_stage(state_path)
        if current.stage != stage or current.qualification_hash is None:
            raise RuntimeError("stage evidence does not match the current risk stage")
        if (
            attested.qualification_hash != current.qualification_hash
            or attested.qualification_hash != qualification_evidence.qualification_hash
            or attested.runtime_policy_sha256 != table.runtime_policy_sha256
            or attested.release_sha != qualification_evidence.code_commit_sha
            or attested.source_sha256 != qualification_evidence.code_sha256
            or attested.config_sha256 != qualification_evidence.config_sha256
            or attested.container_image_digest != qualification_evidence.container_image_digest
            or current.promoted_at is None
            or attested.stage_started_at != current.promoted_at
        ):
            raise RuntimeError("stage evidence identity does not match current runtime state")
        journal = LiveOrderJournal(state_path)
        await journal.initialise()
        if await journal.active_actions():
            raise RuntimeError("risk stage completion requires zero active live actions")
        all_completed = tuple(
            sorted(
                await journal.completed_actions_since(
                    current.promoted_at,
                    current.qualification_hash,
                ),
                key=lambda action: action.pair_action_id,
            )
        )
        completed = tuple(
            action for action in all_completed if is_completed_normal_paired_cycle(action)
        )
        completed_ids = tuple(action.pair_action_id for action in completed)
        completed_sha256 = completed_normal_actions_sha256(all_completed)
        all_completed_ids = tuple(action.pair_action_id for action in all_completed)
        journal_pair_actions_sha256 = hashlib.sha256(
            json.dumps(all_completed_ids, separators=(",", ":")).encode()
        ).hexdigest()
        if (
            completed_ids != attested.completed_pair_action_ids
            or completed_sha256 != attested.completed_pair_actions_sha256
        ):
            raise RuntimeError("stage evidence does not match durable completed paired cycles")
        return await record_risk_stage_result(
            state_path,
            stage,
            current.qualification_hash,
            table.runtime_policy_sha256,
            attested.evidence_sha256,
            True,
            actor,
            await journal.event_watermark(),
            completed_sha256,
            journal_pair_actions_sha256,
            all_completed_ids,
        )

    result = asyncio.run(complete())
    typer.echo(json.dumps(asdict(result), default=str, sort_keys=True))


@app.command("ops-proof")
def ops_proof(
    junit: Annotated[
        Path,
        typer.Option("--junit", exists=True, dir_okay=False, readable=True),
    ],
    output: Annotated[Path, typer.Option("--output")] = Path("state/ops-proof.json"),
    evidence_dir: Annotated[Path, typer.Option("--evidence-dir")] = Path("state/ops-evidence"),
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Validate raw Docker evidence and bind all final criteria to one exact SHA."""
    result = build_operations_proof(
        repo_root.resolve(),
        config.resolve(),
        config.resolve().parent / "RUNTIME_POLICY.yaml",
        config.resolve().parent / "FINAL_ACCEPTANCE_MANIFEST.json",
        config.resolve().parent / "ops-scenario-nodeids.json",
        junit.resolve(),
        evidence_dir.resolve(),
        output.resolve(),
    )
    typer.echo(json.dumps(asdict(result), default=str, sort_keys=True))


@app.command("qualification-runtime")
def qualification_runtime(
    epoch_id: Annotated[str, typer.Option("--epoch-id")],
    route: Annotated[str, typer.Option("--route")],
    container_image_digest: Annotated[
        str,
        typer.Option("--container-image-digest", envvar="IPEG_CONTAINER_IMAGE_DIGEST"),
    ],
    replay_proof: Annotated[
        Path,
        typer.Option("--replay-proof", exists=True, dir_okay=False, readable=True),
    ],
    output: Annotated[Path, typer.Option("--output")] = Path("state/qualification-runtime.json"),
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Collect private fees and persisted route shadow/replay evidence without trading."""
    settings = _load(config)
    selected_route = _parse_route(route)
    release_sha = current_code_commit_sha(repo_root.resolve())
    if release_sha is None:
        raise typer.BadParameter("exact release commit SHA is unavailable")
    proof = json.loads(replay_proof.read_text(encoding="utf-8"))
    junit_path = replay_proof.with_suffix(".junit.xml")
    replay_passed = (
        isinstance(proof, dict)
        and proof.get("passed") is True
        and proof.get("code_commit_sha") == release_sha
        and proof.get("source_sha256") == code_hash(repo_root.resolve())
        and proof.get("config_sha256") == config_hash(config.resolve())
        and int(proof.get("scenario_count", 0)) >= 11
        and int(proof.get("failure_count", -1)) == 0
        and int(proof.get("error_count", -1)) == 0
        and int(proof.get("skipped_count", -1)) == 0
        and tuple(proof.get("test_files", ())) == REPLAY_TEST_FILES
        and junit_path.is_file()
        and proof.get("junit_sha256") == hashlib.sha256(junit_path.read_bytes()).hexdigest()
    )
    if not replay_passed:
        raise typer.BadParameter("replay proof does not match the exact release/config")

    async def collect() -> dict[Venue, Decimal]:
        fees: dict[Venue, Decimal] = {}
        for venue in (selected_route.long_venue, selected_route.short_venue):
            public = CcxtProAdapter(venue)
            private = CcxtPrivateAdapter(
                venue,
                PrivateCredentials.from_environment(venue),
            )
            try:
                instruments = await public.discover_instruments()
                instrument = next(
                    item
                    for item in instruments
                    if item.base == selected_route.base and item.settle == "USDT"
                )
                fee = await private.fetch_trading_fee(instrument)
                if fee is None:
                    raise RuntimeError(f"{venue.value}: private taker fee is unavailable")
                fees[venue] = fee
            finally:
                await asyncio.gather(public.close(), private.close(), return_exceptions=True)
        return fees

    async def build() -> QualificationRuntimeEvidence:
        epoch = await read_qualification_epoch(Path(settings.storage.sqlite_path), epoch_id)
        if (
            epoch is None
            or epoch.route != selected_route
            or epoch.release_sha != release_sha
            or epoch.source_sha256 != code_hash(repo_root.resolve())
            or epoch.config_sha256 != config_hash(config.resolve())
            or epoch.container_image_digest != container_image_digest.lower()
        ):
            raise ValueError("qualification epoch identity does not match the exact release")
        fees = await collect()
        return await build_runtime_evidence_from_state(
            Path(settings.storage.sqlite_path),
            epoch_id,
            fees,
            replay_completed=True,
        )

    runtime = asyncio.run(build())
    write_runtime_evidence(runtime, output.resolve())
    typer.echo(json.dumps(asdict(runtime), default=str, sort_keys=True))


@app.command("replay-proof")
def replay_proof(
    output: Annotated[Path, typer.Option("--output")] = Path("state/replay-proof.json"),
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Run the exact release replay/fault/restart suite and write hashed evidence."""
    proof = run_replay_proof(repo_root.resolve(), config.resolve(), output.resolve())
    typer.echo(json.dumps(asdict(proof), default=str, sort_keys=True))
    if not proof.passed:
        raise typer.Exit(code=5)


@app.command("c4-proof")
def c4_proof(
    image_digest: Annotated[
        str,
        typer.Option("--image-digest", envvar="IPEG_CONTAINER_IMAGE_DIGEST"),
    ],
    output_root: Annotated[Path, typer.Option("--output-root")] = Path("artifacts"),
    baseline: Annotated[Path, typer.Option("--baseline")] = Path(
        "config/c4-critical-test-manifest.json"
    ),
    nodeids: Annotated[Path, typer.Option("--nodeids")] = Path("config/c4-scenario-nodeids.json"),
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Create exact-head C4 critical proof with zero production submit calls."""
    result = run_c4_proof(
        repo_root.resolve(),
        config.resolve(),
        baseline.resolve(),
        nodeids.resolve(),
        output_root.resolve(),
        image_digest,
    )
    typer.echo(json.dumps(asdict(result), default=str, sort_keys=True))


@app.command("c4-3-proof")
def c4_3_proof_command(
    image_digest: Annotated[
        str,
        typer.Option("--image-digest", envvar="IPEG_CONTAINER_IMAGE_DIGEST"),
    ],
    output_root: Annotated[Path, typer.Option("--output-root")] = Path("artifacts"),
    scenarios: Annotated[Path, typer.Option("--scenarios")] = Path(
        "config/c4-3-required-scenarios.json"
    ),
    runtime_policy: Annotated[Path, typer.Option("--runtime-policy")] = Path(
        "config/RUNTIME_POLICY.yaml"
    ),
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Create the exact-head C4.3 stable-FLAT proof artifact."""
    result = run_c4_3_proof(
        repo_root.resolve(),
        config.resolve(),
        runtime_policy.resolve(),
        scenarios.resolve(),
        output_root.resolve(),
        image_digest,
    )
    typer.echo(json.dumps(asdict(result), default=str, sort_keys=True))


@app.command("supervisor-recovery-smoke")
def supervisor_recovery_smoke(
    state: Annotated[Path, typer.Option("--state")],
    hold_after_active: Annotated[bool, typer.Option("--hold-after-active")] = False,
    ready: Annotated[Path | None, typer.Option("--ready")] = None,
    action_count: Annotated[int, typer.Option("--action-count", min=1, max=10)] = 10,
    transition_state: Annotated[
        RecoverySmokeTransition,
        typer.Option("--transition-state", case_sensitive=False),
    ] = RecoverySmokeTransition.PARTIAL,
) -> None:
    """Run deterministic Docker process-kill/restart recovery proof without exchange I/O."""
    result = asyncio.run(
        run_supervisor_recovery_smoke(
            state.resolve(),
            hold_after_active=hold_after_active,
            ready_path=ready.resolve() if ready is not None else None,
            action_count=action_count,
            transition_state=transition_state,
        )
    )
    typer.echo(json.dumps(result, default=str, sort_keys=True))


@app.command("private-transition-recovery-smoke")
def private_transition_recovery_smoke(
    state: Annotated[Path, typer.Option("--state")],
    private_state_dir: Annotated[Path, typer.Option("--private-state-dir")],
    transition_state: Annotated[
        RecoverySmokeTransition,
        typer.Option("--transition-state", case_sensitive=False),
    ],
    hold_after_active: Annotated[bool, typer.Option("--hold-after-active")] = False,
    ready: Annotated[Path | None, typer.Option("--ready")] = None,
    action_count: Annotated[int, typer.Option("--action-count", min=1, max=50)] = 10,
    tranches_per_route: Annotated[
        int,
        typer.Option("--tranches-per-route", min=1, max=5),
    ] = 1,
) -> None:
    """Prove killed-process recovery through production private reconciliation."""
    result = asyncio.run(
        run_private_transition_recovery_smoke(
            state.resolve(),
            private_state_dir.resolve(),
            hold_after_active=hold_after_active,
            transition_state=transition_state,
            ready_path=ready.resolve() if ready is not None else None,
            action_count=action_count,
            tranches_per_route=tranches_per_route,
        )
    )
    typer.echo(json.dumps(result, default=str, sort_keys=True))


@app.command("release-preflight")
def release_preflight(
    image_digest: Annotated[
        str,
        typer.Option("--image-digest", envvar="IPEG_CONTAINER_IMAGE_DIGEST"),
    ],
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Print machine-readable exact-release shadow deployment admission."""
    result = evaluate_release_preflight(
        _load(config),
        repo_root.resolve(),
        config.resolve(),
        image_digest,
    )
    typer.echo(json.dumps(asdict(result), default=str, sort_keys=True))
    if not result.passed:
        raise typer.Exit(code=7)


@app.command("backup-state")
def backup_state(
    target: Annotated[Path, typer.Option("--target")],
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Create an online SQLite backup and verify its integrity."""
    settings = _load(config)
    written = backup_sqlite(Path(settings.storage.sqlite_path), target)
    typer.echo(json.dumps({"status": "PASS", "backup": str(written)}, sort_keys=True))


@app.command("restore-state")
def restore_state(
    backup: Annotated[Path, typer.Option("--backup", exists=True, dir_okay=False)],
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Restore the configured SQLite state from an integrity-checked backup."""
    settings = _load(config)
    restored = restore_sqlite(backup, Path(settings.storage.sqlite_path))
    typer.echo(json.dumps({"status": "PASS", "restored": str(restored)}, sort_keys=True))


@app.command("prune-history")
def prune_history(config: ConfigPath = Path("config/defaults.yaml")) -> None:
    """Apply configured retention to dated Parquet partitions."""
    settings = _load(config)
    removed = prune_market_history(
        Path(settings.storage.parquet_dir),
        settings.shadow.history_retention_days,
    )
    typer.echo(json.dumps({"status": "PASS", "removed": removed}, sort_keys=True))


@app.command("region-latency-report")
def region_latency_report(
    samples: Annotated[Path, typer.Option("--samples", exists=True, dir_okay=False)],
    output: Annotated[Path, typer.Option("--output")],
    minimum_samples: Annotated[int, typer.Option("--minimum-samples")] = 30,
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Build one hash-bound p50/p95/p99 Wave 1 region report from NDJSON measurements."""
    settings = _load(config)
    source_sha256, config_sha256 = _current_region_evidence_identity(repo_root, config)
    report = build_region_latency_report(
        load_latency_samples(samples),
        expected_source_sha256=source_sha256,
        expected_config_sha256=config_sha256,
        maximum_clock_skew_ms=settings.market_data.max_clock_skew_ms,
        minimum_samples_per_cell=minimum_samples,
    )
    write_region_latency_report(report, output)
    typer.echo(json.dumps(asdict(report), default=str, sort_keys=True))


@app.command("region-latency-probe-worker", hidden=True)
def region_latency_probe_worker(
    region: Annotated[str, typer.Option("--region")],
    output: Annotated[Path, typer.Option("--output")],
    attestation: Annotated[
        Path,
        typer.Option("--attestation", exists=True, dir_okay=False),
    ],
    provider_evidence: Annotated[
        Path,
        typer.Option("--provider-evidence", exists=True, dir_okay=False),
    ],
    attestation_public_key: Annotated[
        Path,
        typer.Option("--attestation-public-key", exists=True, dir_okay=False),
    ],
    base: Annotated[str, typer.Option("--base")] = "BTC",
    samples_per_cell: Annotated[int, typer.Option("--samples-per-cell")] = 30,
    timeout_seconds: Annotated[float, typer.Option("--timeout-seconds")] = 5,
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    config: ConfigPath = Path("config/defaults.yaml"),
    runtime_policy: Annotated[
        Path,
        typer.Option("--runtime-policy", exists=True, dir_okay=False),
    ] = Path("config/RUNTIME_POLICY.yaml"),
) -> None:
    """Measure Wave 1 public feed/API and account-wide private-event latency on this VPS."""
    settings = _load(config)
    policy = _load_locked_region_policy(repo_root, runtime_policy)
    source_sha256, config_sha256 = _current_region_evidence_identity(repo_root, config)
    try:
        validate_region_probe_request(region, base, samples_per_cell, timeout_seconds)
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error
    normalized_base = base.strip().upper()
    if not normalized_base:
        raise typer.BadParameter("base must not be empty")
    attested = load_region_attestation(attestation, expected_region=region)
    verify_provider_evidence(
        attested,
        provider_evidence,
        attestation_public_key,
        policy.attestation_public_key_sha256,
    )
    credentials = {venue: PrivateCredentials.from_environment(venue) for venue in WAVE1_VENUES}

    async def probe() -> tuple[LatencySample, ...]:
        public: dict[Venue, CcxtProAdapter] = {}
        private: dict[Venue, CcxtPrivateAdapter] = {}
        try:
            for venue in WAVE1_VENUES:
                public[venue] = CcxtProAdapter(venue)
                private[venue] = CcxtPrivateAdapter(venue, credentials[venue])
            capabilities = await asyncio.gather(
                *(
                    bounded_operation(adapter.probe_public_capabilities, timeout_seconds)
                    for adapter in public.values()
                )
            )
            if any(
                report.clock_skew_ms is None
                or abs(report.clock_skew_ms) > settings.market_data.max_clock_skew_ms
                for report in capabilities
            ):
                raise RuntimeError(
                    "every Wave 1 venue requires a policy-qualified server clock skew"
                )
            discovered = await asyncio.gather(
                *(
                    bounded_operation(adapter.discover_instruments, timeout_seconds)
                    for adapter in public.values()
                )
            )
            instruments = {}
            for venue, venue_instruments in zip(WAVE1_VENUES, discovered, strict=True):
                selected = next(
                    (
                        instrument
                        for instrument in venue_instruments
                        if instrument.base == normalized_base
                    ),
                    None,
                )
                if selected is None:
                    raise RuntimeError(
                        f"{venue.value} has no qualified {normalized_base} instrument"
                    )
                instruments[venue] = selected
            return await collect_region_latency_samples(
                region=region,
                host_fingerprint=local_host_fingerprint(),
                attestation_sha256=attestation_sha256(attested),
                source_sha256=source_sha256,
                config_sha256=config_sha256,
                base=normalized_base,
                public_adapters=public,
                private_adapters=private,
                instruments=instruments,
                samples_per_cell=samples_per_cell,
                timeout_seconds=timeout_seconds,
            )
        finally:
            results = await asyncio.gather(
                *(bounded_operation(adapter.close, timeout_seconds) for adapter in public.values()),
                *(
                    bounded_operation(adapter.close, timeout_seconds)
                    for adapter in private.values()
                ),
                return_exceptions=True,
            )
            failures = tuple(result for result in results if isinstance(result, BaseException))
            if failures:
                raise RuntimeError("region probe adapter shutdown failed")

    loop = asyncio.new_event_loop()
    shutdown_survivors: tuple[asyncio.Task[object], ...] = ()
    measured: tuple[LatencySample, ...] | None = None
    probe_failure: BaseException | None = None
    try:
        measured = loop.run_until_complete(probe())
    except BaseException as error:
        probe_failure = error
    finally:
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.sleep(0))
        shutdown_survivors = tuple(task for task in asyncio.all_tasks(loop) if not task.done())
        loop.close()
    if shutdown_survivors:
        raise RuntimeError(
            f"region probe shutdown retained {len(shutdown_survivors)} nonterminal task(s)"
        ) from probe_failure
    if probe_failure is not None:
        raise probe_failure
    if measured is None:
        raise RuntimeError("region probe returned no measurement set")
    write_latency_samples(measured, output)
    typer.echo(json.dumps({"status": "PASS", "samples": len(measured), "output": str(output)}))


@app.command("region-latency-probe")
def region_latency_probe(
    region: Annotated[str, typer.Option("--region")],
    output: Annotated[Path, typer.Option("--output")],
    attestation: Annotated[
        Path,
        typer.Option("--attestation", exists=True, dir_okay=False),
    ],
    provider_evidence: Annotated[
        Path,
        typer.Option("--provider-evidence", exists=True, dir_okay=False),
    ],
    attestation_public_key: Annotated[
        Path,
        typer.Option("--attestation-public-key", exists=True, dir_okay=False),
    ],
    base: Annotated[str, typer.Option("--base")] = "BTC",
    samples_per_cell: Annotated[int, typer.Option("--samples-per-cell")] = 30,
    timeout_seconds: Annotated[float, typer.Option("--timeout-seconds")] = 5,
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    config: ConfigPath = Path("config/defaults.yaml"),
    runtime_policy: Annotated[
        Path,
        typer.Option("--runtime-policy", exists=True, dir_okay=False),
    ] = Path("config/RUNTIME_POLICY.yaml"),
) -> None:
    """Run the real latency probe in a hard-deadline child process."""
    _load(config)
    policy = _load_locked_region_policy(repo_root, runtime_policy)
    _current_region_evidence_identity(repo_root, config)
    try:
        validate_region_probe_request(region, base, samples_per_cell, timeout_seconds)
        attested = load_region_attestation(attestation, expected_region=region)
        verify_provider_evidence(
            attested,
            provider_evidence,
            attestation_public_key,
            policy.attestation_public_key_sha256,
        )
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error
    command = (
        sys.executable,
        "-m",
        "interexchange_perp_grid.cli",
        "region-latency-probe-worker",
        "--region",
        region,
        "--output",
        str(output.resolve()),
        "--attestation",
        str(attestation.resolve()),
        "--provider-evidence",
        str(provider_evidence.resolve()),
        "--attestation-public-key",
        str(attestation_public_key.resolve()),
        "--base",
        base,
        "--samples-per-cell",
        str(samples_per_cell),
        "--timeout-seconds",
        str(timeout_seconds),
        "--repo-root",
        str(repo_root.resolve()),
        "--config",
        str(config.resolve()),
        "--runtime-policy",
        str(runtime_policy.resolve()),
    )
    try:
        completed = subprocess.run(
            command,
            cwd=repo_root.resolve(),
            check=False,
            timeout=MAXIMUM_PROBE_DURATION_SECONDS,
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError("region latency probe exceeded its one-hour process deadline") from error
    if completed.returncode != 0:
        raise RuntimeError(
            f"region latency probe worker failed with exit code {completed.returncode}"
        )


@app.command("region-latency-select")
def region_latency_select(
    germany: Annotated[Path, typer.Option("--germany", exists=True, dir_okay=False)],
    japan: Annotated[Path, typer.Option("--japan", exists=True, dir_okay=False)],
    germany_samples: Annotated[
        Path,
        typer.Option("--germany-samples", exists=True, dir_okay=False),
    ],
    japan_samples: Annotated[
        Path,
        typer.Option("--japan-samples", exists=True, dir_okay=False),
    ],
    germany_attestation: Annotated[
        Path,
        typer.Option("--germany-attestation", exists=True, dir_okay=False),
    ],
    japan_attestation: Annotated[
        Path,
        typer.Option("--japan-attestation", exists=True, dir_okay=False),
    ],
    germany_provider_evidence: Annotated[
        Path,
        typer.Option("--germany-provider-evidence", exists=True, dir_okay=False),
    ],
    japan_provider_evidence: Annotated[
        Path,
        typer.Option("--japan-provider-evidence", exists=True, dir_okay=False),
    ],
    attestation_public_key: Annotated[
        Path,
        typer.Option("--attestation-public-key", exists=True, dir_okay=False),
    ],
    runtime_policy: Annotated[
        Path,
        typer.Option("--runtime-policy", exists=True, dir_okay=False),
    ] = Path("config/RUNTIME_POLICY.yaml"),
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Select Germany/Japan only from comparable reports and the locked latency policy."""
    settings = _load(config)
    source_sha256, config_sha256 = _current_region_evidence_identity(repo_root, config)
    germany_attested = load_region_attestation(
        germany_attestation,
        expected_region="Germany",
        bind_local_host=False,
    )
    japan_attested = load_region_attestation(
        japan_attestation,
        expected_region="Japan",
        bind_local_host=False,
    )
    policy = _load_locked_region_policy(repo_root, runtime_policy)
    verify_provider_evidence(
        germany_attested,
        germany_provider_evidence,
        attestation_public_key,
        policy.attestation_public_key_sha256,
    )
    verify_provider_evidence(
        japan_attested,
        japan_provider_evidence,
        attestation_public_key,
        policy.attestation_public_key_sha256,
    )
    selection = select_deployment_region(
        load_region_latency_report(germany),
        load_region_latency_report(japan),
        policy,
        germany_samples=load_latency_samples(germany_samples),
        japan_samples=load_latency_samples(japan_samples),
        germany_attestation=germany_attested,
        japan_attestation=japan_attested,
        expected_source_sha256=source_sha256,
        expected_config_sha256=config_sha256,
        maximum_clock_skew_ms=settings.market_data.max_clock_skew_ms,
    )
    typer.echo(json.dumps(asdict(selection), default=str, sort_keys=True))


@app.command("private-probe")
def private_probe(
    venue: Annotated[str, typer.Option("--venue")],
    authenticated: Annotated[bool, typer.Option("--authenticated")] = False,
) -> None:
    """Read-only probe of declared capabilities and optional account authentication."""
    selected = Venue(venue)

    async def probe() -> PrivateCapabilityReport:
        credentials = PrivateCredentials.from_environment(selected) if authenticated else None
        adapter = CcxtPrivateAdapter(selected, credentials=credentials)
        try:
            report = await adapter.probe_private_capabilities()
            if authenticated:
                instruments = await adapter.list_instruments()
                instrument = next(
                    (candidate for candidate in instruments if candidate.base == "BTC"),
                    None,
                )
                if instrument is None:
                    raise RuntimeError("BTC linear instrument is unavailable")
                await adapter.fetch_account(instrument)
            return report
        finally:
            await adapter.close()

    try:
        report = asyncio.run(probe())
    except Exception as error:
        typer.echo(
            json.dumps(
                {
                    "venue": selected.value,
                    "qualified": False,
                    "error_type": type(error).__name__,
                },
                sort_keys=True,
            ),
            err=True,
        )
        raise typer.Exit(code=4) from None
    typer.echo(json.dumps(asdict(report), default=str, sort_keys=True))


@app.command("canary-run")
def canary_run(
    confirmation: Annotated[str, typer.Option("--confirmation")],
    qualification: Annotated[Path, typer.Option("--qualification")] = Path(
        "state/qualification.json"
    ),
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    aggressive_intent: Annotated[Path | None, typer.Option("--aggressive-intent")] = None,
    aggressive_binding: Annotated[Path | None, typer.Option("--aggressive-binding")] = None,
    aggressive_stage: Annotated[AggressiveLaptopLiveStage, typer.Option("--aggressive-stage")] = (
        AggressiveLaptopLiveStage.CANARY
    ),
    runtime_manifest: Annotated[Path, typer.Option("--runtime-manifest")] = Path(
        "state/laptop/native-runtime-manifest.json"
    ),
    aggressive_model: Annotated[Path, typer.Option("--aggressive-model")] = Path(
        "state/aggressive-historical-model.json"
    ),
    aggressive_grid: Annotated[Path, typer.Option("--aggressive-grid")] = Path(
        "state/aggressive-grid.sqlite3"
    ),
    aggressive_profile: Annotated[Path, typer.Option("--aggressive-profile")] = Path(
        "config/AGGRESSIVE_SYMBIOSIS_V1.yaml"
    ),
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Run at most one minimum-notional canary pair after every independent gate."""
    settings = _load(config)
    if (aggressive_intent is None) != (aggressive_binding is None):
        raise typer.BadParameter("aggressive intent and binding must be supplied together")
    loaded_intent = None
    loaded_binding = None
    if aggressive_intent is not None and aggressive_binding is not None:
        loaded_intent = load_aggressive_live_intent(aggressive_intent.resolve())
        loaded_binding = load_aggressive_qualification_binding(aggressive_binding.resolve())
        grid_store = AggressiveGridStore(aggressive_grid.resolve())
        grid_store.initialise()
        verify_aggressive_qualification_binding(
            loaded_binding,
            load_qualification(qualification.resolve()),
            load_historical_model(aggressive_model.resolve()),
            verify_native_runtime_manifest(
                runtime_manifest.resolve(),
                repo_root.resolve(),
                config.resolve(),
            ),
            grid_store,
            profile_sha256=hashlib.sha256(aggressive_profile.read_bytes()).hexdigest(),
        )
    result = asyncio.run(
        run_canary_once(
            settings,
            config.resolve(),
            qualification.resolve(),
            repo_root.resolve(),
            confirmation,
            aggressive_intent=loaded_intent,
            aggressive_binding=loaded_binding,
            aggressive_stage=aggressive_stage,
        )
    )
    typer.echo(json.dumps(asdict(result), default=str, sort_keys=True))
    if not result.success:
        raise typer.Exit(code=5)


@app.command("emergency-flatten")
def emergency_flatten(
    confirmation: Annotated[str, typer.Option("--confirmation")],
    qualification: Annotated[Path, typer.Option("--qualification")] = Path(
        "state/qualification.json"
    ),
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Cancel and flatten live exposure with a separate unlock and exact phrase."""
    settings = _load(config)
    result = asyncio.run(
        run_emergency_flatten(
            settings,
            config.resolve(),
            qualification.resolve(),
            repo_root.resolve(),
            confirmation,
        )
    )
    if result is None:
        typer.echo("EMERGENCY_UNLOCK_OR_QUALIFICATION_INVALID", err=True)
        raise typer.Exit(code=6)
    typer.echo(json.dumps(asdict(result), default=str, sort_keys=True))
    if not result.success:
        raise typer.Exit(code=6)


if __name__ == "__main__":
    app()
