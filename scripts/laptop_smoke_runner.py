from __future__ import annotations

import argparse
import ctypes
import os
import re
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from interexchange_perp_grid.cli import laptop_qualification_smoke_run

ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001
ES_AWAYMODE_REQUIRED = 0x00000040


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--minutes", type=int, choices=(5, 30), required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    root = args.repo_root.resolve()
    run_id = args.run_id
    state = root / "state" / "laptop"
    run_state = state / "smoke" / run_id
    log_path = run_state / "runner.log"
    exit_path = run_state / "runner.exit"
    pid_path = run_state / "runner.pid"
    lock_path = state / "qualification-smoke.lock"
    result = 1
    armed = 0
    try:
        if (
            re.fullmatch(r"[0-9a-f]{32}", run_id) is None
            or os.environ.get("IPEG_LAPTOP_SMOKE_RUN_ID", "") != run_id
        ):
            raise RuntimeError("detached smoke runner lacks a valid run id")
        run_state.mkdir(parents=True, exist_ok=True)
        exit_path.unlink(missing_ok=True)
        pid_path.write_text(str(os.getpid()), encoding="ascii")
        armed = ctypes.windll.kernel32.SetThreadExecutionState(
            ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_AWAYMODE_REQUIRED
        )
        if armed == 0:
            raise RuntimeError("Windows sleep prevention could not be armed")
        with (
            log_path.open("w", encoding="utf-8", buffering=1) as log,
            redirect_stdout(log),
            redirect_stderr(log),
        ):
            laptop_qualification_smoke_run(
                minutes=args.minutes,
                repo_root=root,
                config=root / "config" / "defaults.yaml",
            )
            result = 0
    except BaseException as exc:
        run_state.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as log:
            log.write(f"RUNNER_ERROR_TYPE={type(exc).__name__}\n")
            log.write(f"RUNNER_ERROR_MESSAGE={exc}\n")
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
            ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
