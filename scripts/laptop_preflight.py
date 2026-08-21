from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from interexchange_perp_grid.config import Settings, load_settings
from interexchange_perp_grid.domain import WAVE1_VENUES, Venue
from interexchange_perp_grid.history import query_recorded_level_count
from interexchange_perp_grid.public_engine import PublicMarketEngine, ScanResult
from interexchange_perp_grid.service import BootstrapService
from interexchange_perp_grid.state import read_service_health

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config" / "defaults.yaml"


def _command(command: list[str]) -> None:
    print(f"[laptop-preflight] {' '.join(command)}", flush=True)
    subprocess.run(command, cwd=ROOT, check=True)


def _run_static_gate() -> None:
    python = sys.executable
    _command([python, "-m", "pip", "check"])
    _command(
        [
            python,
            "scripts/check_lock.py",
            "--lock",
            "requirements.lock",
            "--pyproject",
            "pyproject.toml",
        ]
    )
    _command([python, "-m", "ruff", "format", "--check", "src", "tests", "scripts"])
    _command([python, "-m", "ruff", "check", "src", "tests", "scripts"])
    _command([python, "-m", "mypy"])
    _command([python, "-m", "pytest"])
    _command(
        [
            python,
            "-m",
            "interexchange_perp_grid.cli",
            "doctor",
            "--config",
            "config/defaults.yaml",
        ]
    )
    _command(["git", "diff", "--check"])


def _git_head() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _scan_summary(result: ScanResult) -> dict[str, object]:
    eligible = tuple(quote for quote in result.quotes if quote.eligible)
    eligible_wave1 = tuple(
        quote
        for quote in eligible
        if quote.long_venue in WAVE1_VENUES and quote.short_venue in WAVE1_VENUES
    )
    qualified_wave1 = set[Venue]()
    matrix = result.venue_capability_matrix
    if matrix is not None:
        for venue in WAVE1_VENUES:
            if matrix.for_venue(venue).public_runtime.value == "QUALIFIED":
                qualified_wave1.add(venue)
    return {
        "common_instruments": result.common_instrument_count,
        "bbo_quotes": len(result.bbo),
        "routes": len(result.quotes),
        "eligible_routes": len(eligible),
        "eligible_wave1_routes": len(eligible_wave1),
        "qualified_wave1_venues": sorted(venue.value for venue in qualified_wave1),
        "quarantined": [asdict(record) for record in result.quarantined],
    }


async def _service_run(settings: Settings, seconds: float) -> dict[str, object]:
    stop = asyncio.Event()
    task = asyncio.create_task(BootstrapService(settings).run(stop))
    try:
        await asyncio.sleep(seconds)
        if task.done():
            await task
        health = await read_service_health(
            Path(settings.storage.sqlite_path),
            settings.app.health_max_age_seconds,
        )
        return {
            "healthy": health.healthy,
            "reason": health.reason.value,
            "status": health.status,
            "starts": health.starts,
        }
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=10)


async def _run_runtime_gate(
    config: Path,
    output_dir: Path,
    *,
    scan_timeout_seconds: int,
) -> dict[str, object]:
    state_path = output_dir / "state.sqlite3"
    market_path = output_dir / "market"
    os.environ["IPEG_STATE_PATH"] = str(state_path)
    os.environ["IPEG_PARQUET_DIR"] = str(market_path)
    settings = load_settings(config)
    if settings.app.mode != "shadow" or settings.live.enabled:
        raise RuntimeError("laptop preflight requires shadow mode with live disabled")

    engine = PublicMarketEngine(
        settings,
        public_venues=tuple(Venue(value) for value in settings.venues.public_runtime),
    )
    try:
        result = await engine.scan_once(
            settings.shadow.base,
            settings.shadow.quantity,
            scan_timeout_seconds,
        )
    finally:
        await engine.close()
    scan = _scan_summary(result)
    qualified_wave1 = scan["qualified_wave1_venues"]
    eligible_wave1 = scan["eligible_wave1_routes"]
    if not isinstance(qualified_wave1, list) or len(qualified_wave1) < 2:
        raise RuntimeError(f"fewer than two Wave 1 venues qualified: {scan['quarantined']}")
    if not isinstance(eligible_wave1, int) or eligible_wave1 < 2:
        raise RuntimeError(f"no paired Wave 1 route is ready: {scan['quarantined']}")

    parquet_levels = await asyncio.to_thread(query_recorded_level_count, market_path)
    if parquet_levels <= 0:
        raise RuntimeError("public scan produced no replayable Parquet levels")
    if tuple(market_path.rglob("*.pending")):
        raise RuntimeError("public scan left unpublished Parquet staging files")

    first = await _service_run(settings, 15)
    second = await _service_run(settings, 6)
    if not first["healthy"] or not second["healthy"]:
        raise RuntimeError(f"native service heartbeat failed: first={first}, second={second}")
    first_starts = first["starts"]
    second_starts = second["starts"]
    if (
        not isinstance(first_starts, int)
        or not isinstance(second_starts, int)
        or second_starts != first_starts + 1
    ):
        raise RuntimeError(
            f"native restart counter did not advance: first={first}, second={second}"
        )

    return {
        "scan": scan,
        "parquet_levels": parquet_levels,
        "sqlite_exists": state_path.is_file(),
        "sqlite_wal_exists": state_path.with_name(f"{state_path.name}-wal").is_file(),
        "pending_parquet_files": 0,
        "first_service_run": first,
        "second_service_run": second,
        "mode": settings.app.mode,
        "live_enabled": settings.live.enabled,
        "docker_required": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the complete native laptop gate without Docker or private credentials."
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--skip-static", action="store_true")
    parser.add_argument("--scan-timeout", type=int, default=30)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if not 1 <= args.scan_timeout <= 120:
        raise SystemExit("--scan-timeout must be in [1, 120]")
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir = ROOT / "artifacts" / "runtime" / "laptop-preflight" / run_id
    output_dir.mkdir(parents=True, exist_ok=False)
    report_path = output_dir / "report.json"
    report: dict[str, Any] = {
        "status": "FAIL",
        "head_sha": _git_head(),
        "python": sys.version,
        "platform": sys.platform,
        "docker_required": False,
        "started_at": datetime.now(UTC).isoformat(),
        "output_dir": str(output_dir),
    }
    try:
        if not args.skip_static:
            _run_static_gate()
        report["runtime"] = asyncio.run(
            _run_runtime_gate(
                args.config.resolve(),
                output_dir,
                scan_timeout_seconds=args.scan_timeout,
            )
        )
        report["status"] = "PASS"
        return_code = 0
    except Exception as error:
        report["failure"] = f"{type(error).__name__}: {error}"
        return_code = 1
    finally:
        report["finished_at"] = datetime.now(UTC).isoformat()
        report_path.write_text(
            json.dumps(report, default=str, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(report, default=str, sort_keys=True))
        print(f"[laptop-preflight] report={report_path}")
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
