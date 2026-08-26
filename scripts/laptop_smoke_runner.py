from __future__ import annotations

import argparse
import asyncio
import ctypes
import os
import re
import shutil
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote

from interexchange_perp_grid.aggressive_grid import AggressiveGridStore
from interexchange_perp_grid.aggressive_model import load_historical_model
from interexchange_perp_grid.cli import (
    aggressive_model_proof,
    aggressive_qualification_bind,
    laptop_qualification_run,
    laptop_qualification_smoke_run,
    qualification_runtime,
    qualify,
    reference_history_proof,
    replay_proof,
)
from interexchange_perp_grid.config import load_settings
from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.qualification import (
    LAPTOP_OWNER_EXCEPTION_CONFIRMATION,
    LAPTOP_OWNER_EXCEPTION_ENV,
    code_hash,
    config_hash,
)
from interexchange_perp_grid.state import (
    QualificationEpochStatus,
    initialise_state,
    read_qualification_epoch,
    start_qualification_epoch,
)
from interexchange_perp_grid.strategy import DirectedRouteKey

ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001
ES_AWAYMODE_REQUIRED = 0x00000040


class _RedactingWriter:
    def __init__(self, target: Any, secrets: tuple[str, ...]) -> None:
        self._target = target
        encoded = tuple(quote(value, safe="") for value in secrets)
        self._secrets = tuple(sorted({*secrets, *encoded}, key=len, reverse=True))

    def write(self, value: str) -> int:
        redacted = value
        for secret in self._secrets:
            redacted = redacted.replace(secret, "<REDACTED_SECRET>")
        self._target.write(redacted)
        return len(value)

    def flush(self) -> None:
        self._target.flush()


def _loaded_secrets() -> tuple[str, ...]:
    secret_name = re.compile(r"(?:TOKEN|SECRET|PASSWORD|PASSPHRASE|API_KEY)")
    return tuple(
        value
        for name, value in os.environ.items()
        if secret_name.search(name.upper()) and len(value) >= 4
    )


def _git_directory(root: Path) -> Path:
    marker = root / ".git"
    if marker.is_dir():
        return marker
    if marker.is_file():
        value = marker.read_text(encoding="utf-8").strip()
        if value.startswith("gitdir: "):
            return (root / value.removeprefix("gitdir: ")).resolve()
    raise RuntimeError("exact Git checkout metadata is unavailable")


def _read_git_ref(git_dir: Path, ref: str) -> str:
    loose = git_dir / Path(ref)
    if loose.is_file():
        return loose.read_text(encoding="ascii").strip().lower()
    packed = git_dir / "packed-refs"
    if packed.is_file():
        suffix = f" {ref}"
        for line in packed.read_text(encoding="ascii").splitlines():
            if line.endswith(suffix):
                return line.split(" ", 1)[0].lower()
    raise RuntimeError(f"required Git ref is unavailable: {ref}")


def _require_exact_local_main(root: Path) -> str:
    git_dir = _git_directory(root)
    if (git_dir / "HEAD").read_text(encoding="ascii").strip() != "ref: refs/heads/main":
        raise RuntimeError("12-hour qualification requires the local main branch")
    local = _read_git_ref(git_dir, "refs/heads/main")
    remote = _read_git_ref(git_dir, "refs/remotes/origin/main")
    if not re.fullmatch(r"[0-9a-f]{40}", local) or local != remote:
        raise RuntimeError("local main must exactly match the fetched origin/main")
    if os.environ.get("IPEG_RELEASE_SHA", "").lower() != local:
        raise RuntimeError("native runtime release does not match exact main")
    return local


def _quarantine_previous_evidence(root: Path, run_state: Path) -> None:
    for path in (
        root / "state" / "qualification.json",
        root / "state" / "aggressive-qualification.json",
        root / "state" / "laptop" / "qualification-runtime.json",
    ):
        if path.is_file():
            target = run_state / f"superseded-{path.name}"
            target.unlink(missing_ok=True)
            os.replace(path, target)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--minutes", type=int, choices=(5, 30))
    mode.add_argument("--qualification-12h", action="store_true")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    root = args.repo_root.resolve()
    run_id = args.run_id
    state = root / "state" / "laptop"
    qualification_12h = bool(args.qualification_12h)
    run_kind = "qualification" if qualification_12h else "smoke"
    run_state = state / run_kind / run_id
    log_path = run_state / "runner.log"
    exit_path = run_state / "runner.exit"
    pid_path = run_state / "runner.pid"
    lock_path = state / ("qualification.lock" if qualification_12h else "qualification-smoke.lock")
    result = 1
    armed = 0
    kernel32 = cast(Any, ctypes).windll.kernel32
    try:
        if (
            re.fullmatch(r"[0-9a-f]{32}", run_id) is None
            or os.environ.get("IPEG_LAPTOP_RUN_ID", "") != run_id
        ):
            raise RuntimeError("detached laptop runner lacks a valid run id")
        if os.environ.get("IPEG_LIVE_ENABLED", "").lower() != "false":
            raise RuntimeError("detached laptop runner requires live=false")
        if os.environ.get("IPEG_TELEGRAM_ENABLED", "").lower() != "false":
            raise RuntimeError("detached laptop runner requires Telegram disabled")
        if qualification_12h and os.environ.get(LAPTOP_OWNER_EXCEPTION_ENV) != (
            LAPTOP_OWNER_EXCEPTION_CONFIRMATION
        ):
            raise RuntimeError("detached 12-hour qualification lacks owner authorization")
        if qualification_12h:
            _require_exact_local_main(root)
        run_state.mkdir(parents=True, exist_ok=True)
        runtime_temp = run_state / "tmp"
        runtime_temp.mkdir(parents=True, exist_ok=True)
        os.environ["TEMP"] = str(runtime_temp)
        os.environ["TMP"] = str(runtime_temp)
        tempfile.tempdir = str(runtime_temp)
        exit_path.unlink(missing_ok=True)
        pid_path.write_text(str(os.getpid()), encoding="ascii")
        armed = kernel32.SetThreadExecutionState(
            ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_AWAYMODE_REQUIRED
        )
        if armed == 0:
            raise RuntimeError("Windows sleep prevention could not be armed")
        with (
            log_path.open("w", encoding="utf-8", buffering=1) as log,
            redirect_stdout(_RedactingWriter(log, _loaded_secrets())),
            redirect_stderr(_RedactingWriter(log, _loaded_secrets())),
        ):
            history_end = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
            history_start = history_end - timedelta(days=181)
            config = root / "config" / "defaults.yaml"
            profile = root / "config" / "AGGRESSIVE_SYMBIOSIS_V1.yaml"
            history = root / "data" / "reference-history"
            reference_history_proof(
                venue_a=Venue.BYBIT,
                venue_b=Venue.OKX,
                since=history_start.isoformat(),
                end=history_end.isoformat(),
                base="BTC",
                limit=1000,
                output_root=history,
                profile=profile,
                config=config,
            )
            aggressive_model_proof(
                venue_a=Venue.BYBIT,
                venue_b=Venue.OKX,
                start=history_start.isoformat(),
                end=history_end.isoformat(),
                base="BTC",
                history_root=history,
                artifact=root / "state" / "aggressive-historical-model.json",
                profile=profile,
                config=config,
            )
            if qualification_12h:
                model_path = root / "state" / "aggressive-historical-model.json"
                grid_path = root / "state" / "aggressive-grid.sqlite3"
                loaded_model = load_historical_model(model_path)
                fresh_grid_path = run_state / "aggressive-grid.sqlite3"
                grid = AggressiveGridStore(fresh_grid_path)
                grid.initialise()
                for direction in (
                    loaded_model.positive.direction,
                    loaded_model.negative.direction,
                ):
                    grid.initialise_route(
                        loaded_model,
                        direction,
                        now=datetime.now(UTC),
                        rearm_retreat_step_fraction=Decimal("0.25"),
                    )
                replay_path = state / "replay-proof.json"
                runtime_path = state / "qualification-runtime.json"
                qualification_path = root / "state" / "qualification.json"
                manifest_path = state / "native-runtime-manifest.json"
                replay_proof(output=replay_path, repo_root=root, config=config)
                _quarantine_previous_evidence(root, run_state)
                settings = load_settings(config)
                qualification_state = Path(settings.storage.sqlite_path)
                asyncio.run(initialise_state(qualification_state))
                epoch = asyncio.run(
                    start_qualification_epoch(
                        qualification_state,
                        DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX),
                        os.environ["IPEG_RELEASE_SHA"],
                        code_hash(root),
                        config_hash(config),
                        os.environ["IPEG_CONTAINER_IMAGE_DIGEST"],
                        force_new=True,
                    )
                )
                laptop_qualification_run(
                    maximum_hours=18,
                    laptop_owner_exception_12h=True,
                    repo_root=root,
                    config=config,
                )
                finalized = asyncio.run(
                    read_qualification_epoch(qualification_state, epoch.epoch_id)
                )
                if finalized is None or finalized.status != QualificationEpochStatus.FINALIZED:
                    raise RuntimeError("qualification epoch did not finalize")
                qualification_runtime(
                    epoch_id=finalized.epoch_id,
                    route="BTC:bybit>okx",
                    container_image_digest=os.environ["IPEG_CONTAINER_IMAGE_DIGEST"],
                    replay_proof=replay_path,
                    output=runtime_path,
                    repo_root=root,
                    config=config,
                )
                qualify(
                    config=config,
                    evidence=qualification_path,
                    repo_root=root,
                    runtime_evidence=runtime_path,
                    laptop_owner_exception_12h=True,
                )
                grid_stage = grid_path.with_name(f".{grid_path.name}.{run_id}.tmp")
                shutil.copy2(fresh_grid_path, grid_stage)
                os.replace(grid_stage, grid_path)
                aggressive_qualification_bind(
                    qualification=qualification_path,
                    runtime_manifest=manifest_path,
                    model=model_path,
                    grid=grid_path,
                    profile=profile,
                    history_root=history,
                    output=root / "state" / "aggressive-qualification.json",
                )
            else:
                laptop_qualification_smoke_run(
                    minutes=args.minutes,
                    repo_root=root,
                    config=config,
                )
            result = 0
    except BaseException as exc:
        run_state.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as log:
            log.write(f"RUNNER_ERROR_TYPE={type(exc).__name__}\n")
            log.write("RUNNER_ERROR=FAIL_CLOSED_SEE_PRECEDING_STAGE\n")
    finally:
        run_state.mkdir(parents=True, exist_ok=True)
        exit_path.write_text(str(result), encoding="ascii")
        pid_path.unlink(missing_ok=True)
        if re.fullmatch(r"[0-9a-f]{32}", run_id) is not None:
            try:
                if lock_path.read_text(encoding="ascii").strip().startswith(f"{run_id}|"):
                    lock_path.unlink()
            except FileNotFoundError:
                pass
        if armed != 0:
            kernel32.SetThreadExecutionState(ES_CONTINUOUS)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
