from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Annotated

import typer

from interexchange_perp_grid.config import load_settings
from interexchange_perp_grid.safety import LiveContext, evaluate_live_order
from interexchange_perp_grid.state import initialise_state

app = typer.Typer(no_args_is_help=True, add_completion=False)

@app.callback()
def main() -> None:
    """Interexchange perpetual grid control CLI."""


@app.command()
def doctor(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, file_okay=True, dir_okay=False, readable=True),
    ] = Path("config/defaults.yaml"),
) -> None:
    """Validate configuration, state storage, and default live denial."""
    settings = load_settings(config)
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


@app.command("run")
def run_service(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, file_okay=True, dir_okay=False, readable=True),
    ] = Path("config/defaults.yaml"),
) -> None:
    """Run the safe bootstrap service until the Wave 1 shadow loop replaces it."""
    settings = load_settings(config)
    decision = evaluate_live_order(settings, LiveContext())
    if settings.app.mode == "live" or decision.allowed:
        typer.echo("bootstrap service refuses live mode", err=True)
        raise typer.Exit(code=2)
    asyncio.run(initialise_state(Path(settings.storage.sqlite_path)))
    typer.echo(
        json.dumps(
            {
                "event": "BOOTSTRAP_SERVICE_STARTED",
                "mode": settings.app.mode,
                "live_orders_allowed": False,
            },
            sort_keys=True,
        )
    )
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        typer.echo("bootstrap service stopped")


if __name__ == "__main__":
    app()
