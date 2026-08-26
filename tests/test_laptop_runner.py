from __future__ import annotations

import importlib.util
import shutil
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from interexchange_perp_grid.qualification import (
    LAPTOP_OWNER_EXCEPTION_CONFIRMATION,
    LAPTOP_OWNER_EXCEPTION_ENV,
)
from interexchange_perp_grid.state import QualificationEpochStatus


def _runner_module() -> ModuleType:
    path = Path("scripts/laptop_smoke_runner.py").resolve()
    spec = importlib.util.spec_from_file_location("tested_laptop_runner", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _git_main(root: Path, sha: str) -> None:
    git = root / ".git"
    (git / "refs/heads").mkdir(parents=True)
    (git / "refs/remotes/origin").mkdir(parents=True)
    (git / "HEAD").write_text("ref: refs/heads/main\n", encoding="ascii")
    (git / "refs/heads/main").write_text(f"{sha}\n", encoding="ascii")
    (git / "refs/remotes/origin/main").write_text(f"{sha}\n", encoding="ascii")


def test_exact_local_main_requires_matching_branch_release_and_origin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _runner_module()
    sha = "a" * 40
    _git_main(tmp_path, sha)
    monkeypatch.setenv("IPEG_RELEASE_SHA", sha)
    assert runner._require_exact_local_main(tmp_path) == sha
    (tmp_path / ".git/HEAD").write_text(
        "ref: refs/heads/codex/aggressive-symbiosis-v1\n", encoding="ascii"
    )
    with pytest.raises(RuntimeError, match="main branch"):
        runner._require_exact_local_main(tmp_path)


@pytest.mark.parametrize(
    "failure_stage",
    [None, "qualification:run", "runtime", "qualify", "grid:publish", "bind"],
    ids=["pass", "qualification", "runtime", "qualify", "grid-publish", "bind"],
)
def test_detached_qualification_orchestration_and_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_stage: str | None,
) -> None:
    runner = _runner_module()
    sha = "a" * 40
    digest = "sha256:" + "b" * 64
    run_id = "c" * 32
    _git_main(tmp_path, sha)
    (tmp_path / "config").mkdir()
    (tmp_path / "state/laptop").mkdir(parents=True)
    for evidence in (
        tmp_path / "state/qualification.json",
        tmp_path / "state/aggressive-qualification.json",
        tmp_path / "state/laptop/qualification-runtime.json",
    ):
        evidence.write_text("stale", encoding="ascii")
    lock = tmp_path / "state/laptop/qualification.lock"
    lock.write_text(f"{run_id}|0", encoding="ascii")
    state_path = tmp_path / "state/ipeg.sqlite3"
    calls: list[str] = []

    class Kernel:
        def SetThreadExecutionState(self, flags: int) -> int:
            calls.append(f"sleep:{flags}")
            return 1

    class Grid:
        def __init__(self, path: Path) -> None:
            self.path = path

        def initialise(self) -> None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_bytes(b"fresh-grid")
            calls.append("grid:init")

        def initialise_route(self, *_args: Any, **_kwargs: Any) -> None:
            calls.append("grid:route")

    async def initialise_state(_path: Path) -> None:
        calls.append("state:init")

    async def start_epoch(*_args: Any, **kwargs: Any) -> SimpleNamespace:
        assert kwargs["force_new"] is True
        calls.append("epoch:new")
        return SimpleNamespace(epoch_id="epoch-new")

    async def read_epoch(_path: Path, epoch_id: str) -> SimpleNamespace:
        assert epoch_id == "epoch-new"
        calls.append("epoch:read")
        return SimpleNamespace(
            epoch_id=epoch_id,
            status=QualificationEpochStatus.FINALIZED,
        )

    def record(name: str) -> Any:
        def invoke(*_args: Any, **_kwargs: Any) -> None:
            calls.append(name)
            if failure_stage == name:
                raise RuntimeError("PRIVATE_VALUE")

        return invoke

    model = SimpleNamespace(
        positive=SimpleNamespace(direction="positive"),
        negative=SimpleNamespace(direction="negative"),
    )
    monkeypatch.setattr(runner.ctypes, "windll", SimpleNamespace(kernel32=Kernel()), raising=False)
    monkeypatch.setattr(runner, "AggressiveGridStore", Grid)
    monkeypatch.setattr(runner, "load_historical_model", lambda _path: model)
    monkeypatch.setattr(
        runner,
        "load_settings",
        lambda _path: SimpleNamespace(storage=SimpleNamespace(sqlite_path=state_path)),
    )
    monkeypatch.setattr(runner, "initialise_state", initialise_state)
    monkeypatch.setattr(runner, "start_qualification_epoch", start_epoch)
    monkeypatch.setattr(runner, "read_qualification_epoch", read_epoch)
    for name, target in (
        ("history", "reference_history_proof"),
        ("model", "aggressive_model_proof"),
        ("replay", "replay_proof"),
        ("qualification:run", "laptop_qualification_run"),
        ("runtime", "qualification_runtime"),
        ("qualify", "qualify"),
        ("bind", "aggressive_qualification_bind"),
    ):
        monkeypatch.setattr(runner, target, record(name))
    monkeypatch.setattr(runner, "code_hash", lambda _path: "d" * 64)
    monkeypatch.setattr(runner, "config_hash", lambda _path: "e" * 64)
    original_copy = shutil.copy2

    def copy_grid(source: Path, target: Path) -> Path:
        if failure_stage == "grid:publish":
            raise RuntimeError("PRIVATE_VALUE")
        return Path(original_copy(source, target))

    monkeypatch.setattr(runner.shutil, "copy2", copy_grid)
    for name, value in {
        "IPEG_LAPTOP_RUN_ID": run_id,
        "IPEG_LIVE_ENABLED": "false",
        "IPEG_TELEGRAM_ENABLED": "false",
        LAPTOP_OWNER_EXCEPTION_ENV: LAPTOP_OWNER_EXCEPTION_CONFIRMATION,
        "IPEG_RELEASE_SHA": sha,
        "IPEG_CONTAINER_IMAGE_DIGEST": digest,
        "BYBIT_API_SECRET": "PRIVATE_VALUE",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(
        sys,
        "argv",
        ["runner", "--repo-root", str(tmp_path), "--qualification-12h", "--run-id", run_id],
    )

    result = runner.main()
    run_state = tmp_path / "state/laptop/qualification" / run_id
    assert result == (1 if failure_stage is not None else 0)
    assert (run_state / "runner.exit").read_text(encoding="ascii") == str(result)
    assert not (run_state / "runner.pid").exists()
    assert not lock.exists()
    log = (run_state / "runner.log").read_text(encoding="utf-8")
    assert "PRIVATE_VALUE" not in log
    assert len(tuple(run_state.glob("superseded-*.json"))) == 3
    assert calls[-1] == "sleep:2147483648"
    if failure_stage is not None:
        if failure_stage == "qualification:run":
            assert "runtime" not in calls and "qualify" not in calls and "bind" not in calls
        if failure_stage == "runtime":
            assert "qualify" not in calls and "bind" not in calls
        if failure_stage in {"qualify", "grid:publish"}:
            assert "bind" not in calls
    else:
        assert calls.index("epoch:new") < calls.index("qualification:run")
        assert calls.index("qualification:run") < calls.index("epoch:read")
        assert calls.index("qualify") < calls.index("bind")
        assert (tmp_path / "state/aggressive-grid.sqlite3").read_bytes() == b"fresh-grid"
