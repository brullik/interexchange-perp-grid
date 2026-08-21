from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import re
import subprocess
import sys
from dataclasses import asdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Annotated

import typer

from interexchange_perp_grid.adapters.ccxt_pro import CcxtProAdapter
from interexchange_perp_grid.adapters.private import CcxtPrivateAdapter, PrivateCredentials
from interexchange_perp_grid.autonomous_orchestrator import load_autonomous_runtime_status
from interexchange_perp_grid.c4_3_proof import run_c4_3_proof
from interexchange_perp_grid.c4_proof import run_c4_proof
from interexchange_perp_grid.canary_runtime import run_canary_once, run_emergency_flatten
from interexchange_perp_grid.config import Settings, load_settings
from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.live_journal import (
    LiveOrderJournal,
    completed_normal_actions_sha256,
    is_completed_normal_paired_cycle,
)
from interexchange_perp_grid.maintenance import (
    backup_sqlite,
    prune_market_history,
    restore_sqlite,
)
from interexchange_perp_grid.observability import configure_logging, render_metrics
from interexchange_perp_grid.ops_evidence import build_operations_proof
from interexchange_perp_grid.private_domain import PrivateCapabilityReport
from interexchange_perp_grid.private_transition_smoke import (
    run_private_transition_recovery_smoke,
)
from interexchange_perp_grid.public_engine import PublicMarketEngine, ScanResult
from interexchange_perp_grid.qualification import (
    QualificationPolicy,
    QualificationProgress,
    QualificationRuntimeEvidence,
    build_qualification_progress,
    build_runtime_evidence_from_state,
    code_hash,
    config_hash,
    current_code_commit_sha,
    load_qualification,
    load_runtime_evidence,
    qualification_is_current,
    run_qualification,
    write_runtime_evidence,
)
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
from interexchange_perp_grid.service import run_until_signal
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
    engine = PublicMarketEngine(settings)
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
    """Print one live Wave 1 public-data route snapshot."""
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
) -> None:
    """Write exact-route, release/image/data-bound qualification evidence."""
    settings = _load(config)
    policy = _qualification_policy(settings)
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
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Print exact 24h progress, remaining work, and fail-closed blockers as JSON."""
    settings = _load(config)

    async def status() -> QualificationProgress:
        state_path = Path(settings.storage.sqlite_path)
        await initialise_state(state_path)
        return await build_qualification_progress(
            state_path,
            Path(settings.storage.parquet_dir),
            epoch_id,
            _qualification_policy(settings),
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
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Persist one adjacent owner-confirmed promotion after exact qualification validation."""
    settings = _load(config)
    evidence = load_qualification(qualification.resolve())
    current, reason = qualification_is_current(
        evidence,
        repo_root.resolve(),
        config.resolve(),
        Path(settings.storage.parquet_dir).resolve(),
        settings.live.qualification_max_age_seconds,
        current_container_image_digest=container_image_digest.lower(),
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
) -> None:
    """Read-only probe of one venue transport's declared private capabilities."""
    selected = Venue(venue)

    async def probe() -> PrivateCapabilityReport:
        adapter = CcxtPrivateAdapter(selected)
        try:
            return await adapter.probe_private_capabilities()
        finally:
            await adapter.close()

    report = asyncio.run(probe())
    typer.echo(json.dumps(asdict(report), default=str, sort_keys=True))


@app.command("canary-run")
def canary_run(
    confirmation: Annotated[str, typer.Option("--confirmation")],
    qualification: Annotated[Path, typer.Option("--qualification")] = Path(
        "state/qualification.json"
    ),
    repo_root: Annotated[Path, typer.Option("--repo-root")] = Path("."),
    config: ConfigPath = Path("config/defaults.yaml"),
) -> None:
    """Run at most one minimum-notional canary pair after every independent gate."""
    settings = _load(config)
    result = asyncio.run(
        run_canary_once(
            settings,
            config.resolve(),
            qualification.resolve(),
            repo_root.resolve(),
            confirmation,
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
