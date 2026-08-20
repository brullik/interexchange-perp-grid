from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
from dataclasses import asdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Annotated

import typer

from interexchange_perp_grid.adapters.ccxt_pro import CcxtProAdapter
from interexchange_perp_grid.adapters.private import CcxtPrivateAdapter, PrivateCredentials
from interexchange_perp_grid.c4_3_proof import run_c4_3_proof
from interexchange_perp_grid.c4_proof import run_c4_proof
from interexchange_perp_grid.canary_runtime import run_canary_once, run_emergency_flatten
from interexchange_perp_grid.config import Settings, load_settings
from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.maintenance import (
    backup_sqlite,
    prune_market_history,
    restore_sqlite,
)
from interexchange_perp_grid.observability import configure_logging, render_metrics
from interexchange_perp_grid.private_domain import PrivateCapabilityReport
from interexchange_perp_grid.public_engine import PublicMarketEngine, ScanResult
from interexchange_perp_grid.qualification import (
    QualificationPolicy,
    QualificationRuntimeEvidence,
    build_runtime_evidence_from_state,
    code_hash,
    config_hash,
    current_code_commit_sha,
    load_runtime_evidence,
    run_qualification,
    write_runtime_evidence,
)
from interexchange_perp_grid.release_evidence import REPLAY_TEST_FILES, run_replay_proof
from interexchange_perp_grid.release_preflight import evaluate_release_preflight
from interexchange_perp_grid.safety import LiveContext, evaluate_live_order
from interexchange_perp_grid.service import run_until_signal
from interexchange_perp_grid.shadow import ShadowRuntime
from interexchange_perp_grid.state import (
    QualificationEpoch,
    ServiceHealth,
    finalize_qualification_epoch,
    initialise_state,
    read_qualification_epoch,
    read_service_health,
    start_qualification_epoch,
)
from interexchange_perp_grid.strategy import DirectedRouteKey
from interexchange_perp_grid.supervisor import SupervisorHealth, read_supervisor_health
from interexchange_perp_grid.supervisor_smoke import run_supervisor_recovery_smoke

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


@app.command()
def metrics() -> None:
    """Print the current Prometheus metric exposition."""
    typer.echo(render_metrics(), nl=False)


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
    policy = QualificationPolicy(
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
        maximum_clock_skew_snapshots=(settings.shadow.qualification_max_clock_skew_snapshots),
        maximum_clock_skew_ms=settings.market_data.max_clock_skew_ms,
        maximum_snapshot_age_ms=settings.market_data.max_l2_age_ms,
    )
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
    """Print the current or selected immutable qualification epoch as JSON."""
    settings = _load(config)

    async def status() -> QualificationEpoch | None:
        state_path = Path(settings.storage.sqlite_path)
        await initialise_state(state_path)
        return await read_qualification_epoch(state_path, epoch_id)

    epoch = asyncio.run(status())
    if epoch is None:
        raise typer.Exit(code=4)
    typer.echo(json.dumps(asdict(epoch), default=str, sort_keys=True))


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
) -> None:
    """Run deterministic Docker process-kill/restart recovery proof without exchange I/O."""
    result = asyncio.run(
        run_supervisor_recovery_smoke(
            state.resolve(),
            hold_after_active=hold_after_active,
            ready_path=ready.resolve() if ready is not None else None,
            action_count=action_count,
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


@app.command("private-probe")
def private_probe(
    venue: Annotated[str, typer.Option("--venue")],
) -> None:
    """Read-only probe of one Wave 1 venue's CCXT Pro private capabilities."""
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
