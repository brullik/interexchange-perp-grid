from __future__ import annotations

import asyncio
import contextlib
import json
from pathlib import Path
from typing import Annotated

import typer

from interexchange_perp_grid.config import Settings, load_settings
from interexchange_perp_grid.observability import configure_logging, render_metrics
from interexchange_perp_grid.safety import LiveContext, evaluate_live_order
from interexchange_perp_grid.service import run_until_signal
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


if __name__ == "__main__":
    app()
