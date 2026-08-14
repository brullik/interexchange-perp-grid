from __future__ import annotations

import asyncio
import contextlib
import json
from dataclasses import asdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Annotated

import typer

from interexchange_perp_grid.config import Settings, load_settings
from interexchange_perp_grid.maintenance import (
    backup_sqlite,
    prune_market_history,
    restore_sqlite,
)
from interexchange_perp_grid.observability import configure_logging, render_metrics
from interexchange_perp_grid.public_engine import PublicMarketEngine, ScanResult
from interexchange_perp_grid.qualification import run_qualification
from interexchange_perp_grid.safety import LiveContext, evaluate_live_order
from interexchange_perp_grid.service import run_until_signal
from interexchange_perp_grid.shadow import ShadowRuntime
from interexchange_perp_grid.state import initialise_state, read_service_health

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
    result = asyncio.run(
        read_service_health(
            Path(settings.storage.sqlite_path),
            settings.app.health_max_age_seconds,
        )
    )
    typer.echo(
        json.dumps(
            {
                "status": "PASS" if result.healthy else "FAIL",
                "reason": result.reason,
                "service_status": result.status,
                "heartbeat_at": result.heartbeat_at,
                "starts": result.starts,
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
    minimum_samples: Annotated[int, typer.Option("--minimum-samples", min=0)] = 0,
) -> None:
    """Write code/config/data-hash-bound shadow qualification evidence."""
    settings = _load(config)
    required = minimum_samples or settings.shadow.qualification_min_samples
    result = run_qualification(
        repo_root.resolve(),
        config.resolve(),
        Path(settings.storage.parquet_dir).resolve(),
        evidence.resolve(),
        required,
    )
    typer.echo(json.dumps(asdict(result), default=str, sort_keys=True))
    if not result.accepted:
        raise typer.Exit(code=4)


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


if __name__ == "__main__":
    app()
