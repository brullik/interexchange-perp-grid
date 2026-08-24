from __future__ import annotations

import asyncio
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

import interexchange_perp_grid.autonomous_orchestrator as orchestrator_module
from interexchange_perp_grid.autonomous_orchestrator import (
    AutonomousOrchestrator,
    load_autonomous_runtime_status,
)
from interexchange_perp_grid.config import load_settings
from interexchange_perp_grid.qualification import (
    LAPTOP_OWNER_EXCEPTION_CONFIRMATION,
    LAPTOP_OWNER_EXCEPTION_ENV,
    laptop_owner_exception_policy,
)
from interexchange_perp_grid.state import (
    QualificationEpochStatus,
    read_active_qualification_epoch,
    read_qualification_epoch,
)

CONFIG = Path("config/defaults.yaml")
RELEASE_SHA = "a" * 40
IMAGE_DIGEST = f"sha256:{'b' * 64}"


def _settings(tmp_path: Path):  # type: ignore[no-untyped-def]
    return load_settings(
        CONFIG,
        {
            "IPEG_STATE_PATH": str(tmp_path / "state" / "ipeg.sqlite3"),
            "IPEG_PARQUET_DIR": str(tmp_path / "data"),
        },
    )


def _confirm_onboarding(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IPEG_OWNER_ONBOARDING_CONFIRMED", "true")


def test_orchestrator_requires_local_receipt_for_laptop_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    policy = laptop_owner_exception_policy(settings)
    monkeypatch.delenv(LAPTOP_OWNER_EXCEPTION_ENV, raising=False)

    with pytest.raises(ValueError, match="lacks the local Windows receipt"):
        AutonomousOrchestrator(settings, qualification_policy=policy)

    monkeypatch.setenv(LAPTOP_OWNER_EXCEPTION_ENV, LAPTOP_OWNER_EXCEPTION_CONFIRMATION)
    monkeypatch.setattr(
        orchestrator_module,
        "laptop_owner_exception_authorized",
        lambda: True,
    )
    orchestrator = AutonomousOrchestrator(settings, qualification_policy=policy)
    assert orchestrator.qualification_policy == policy


@pytest.mark.asyncio
async def test_orchestrator_waits_fail_closed_without_owner_confirmation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setenv("IPEG_RELEASE_SHA", RELEASE_SHA)
    monkeypatch.setenv("IPEG_CONTAINER_IMAGE_DIGEST", IMAGE_DIGEST)
    monkeypatch.setenv("IPEG_QUALIFICATION_ROUTE", "BTC:bybit>okx")
    monkeypatch.delenv("IPEG_OWNER_ONBOARDING_CONFIRMED", raising=False)
    orchestrator = AutonomousOrchestrator(settings, use_progress_subprocess=False)

    status = await orchestrator.reconcile_once()

    assert status.state == "WAITING_OWNER_ONBOARDING"
    assert status.live_authorized is False
    assert status.blockers == ("OWNER_ONBOARDING_CONFIRMATION_REQUIRED",)


@pytest.mark.asyncio
async def test_orchestrator_rejects_unverified_native_runtime_before_state_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setenv("IPEG_RELEASE_SHA", RELEASE_SHA)
    monkeypatch.setenv("IPEG_RUNTIME_KIND", "native-python")
    monkeypatch.setenv("IPEG_QUALIFICATION_ROUTE", "BTC:bybit>okx")
    _confirm_onboarding(monkeypatch)

    def invalid(*args: object, **kwargs: object) -> str:
        del args, kwargs
        raise ValueError("native runtime mismatch")

    monkeypatch.setattr(orchestrator_module, "resolve_runtime_artifact_digest", invalid)
    orchestrator = AutonomousOrchestrator(settings, use_progress_subprocess=False)

    status = await orchestrator.reconcile_once()

    assert status.state == "WAITING_DEPLOYMENT_IDENTITY"
    assert status.blockers == ("EXACT_DEPLOYMENT_IDENTITY_REQUIRED",)
    assert not orchestrator.state_path.exists()


@pytest.mark.asyncio
async def test_orchestrator_waits_fail_closed_without_owner_route(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setenv("IPEG_RELEASE_SHA", RELEASE_SHA)
    monkeypatch.setenv("IPEG_CONTAINER_IMAGE_DIGEST", IMAGE_DIGEST)
    monkeypatch.delenv("IPEG_QUALIFICATION_ROUTE", raising=False)
    _confirm_onboarding(monkeypatch)
    orchestrator = AutonomousOrchestrator(settings, use_progress_subprocess=False)

    status = await orchestrator.reconcile_once()

    assert status.state == "WAITING_OWNER_ONBOARDING"
    assert status.live_authorized is False
    assert status.blockers == ("QUALIFICATION_ROUTE_REQUIRED",)
    assert load_autonomous_runtime_status(orchestrator.runtime_status_path)["state"] == (
        "WAITING_OWNER_ONBOARDING"
    )


@pytest.mark.asyncio
async def test_orchestrator_rejects_non_locked_qualification_route(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setenv("IPEG_RELEASE_SHA", RELEASE_SHA)
    monkeypatch.setenv("IPEG_CONTAINER_IMAGE_DIGEST", IMAGE_DIGEST)
    monkeypatch.setenv("IPEG_QUALIFICATION_ROUTE", "ETH:binanceusdm>bybit")
    _confirm_onboarding(monkeypatch)
    orchestrator = AutonomousOrchestrator(settings, use_progress_subprocess=False)

    status = await orchestrator.reconcile_once()

    assert status.state == "WAITING_OWNER_ONBOARDING"
    assert status.blockers == ("QUALIFICATION_ROUTE_LOCK_MISMATCH",)
    assert not orchestrator.state_path.exists()


@pytest.mark.asyncio
async def test_orchestrator_idempotently_starts_exact_immutable_epoch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setenv("IPEG_RELEASE_SHA", RELEASE_SHA)
    monkeypatch.setenv("IPEG_CONTAINER_IMAGE_DIGEST", IMAGE_DIGEST)
    monkeypatch.setenv("IPEG_QUALIFICATION_ROUTE", "BTC:bybit>okx")
    _confirm_onboarding(monkeypatch)
    orchestrator = AutonomousOrchestrator(settings, use_progress_subprocess=False)

    first = await orchestrator.reconcile_once()
    second = await orchestrator.reconcile_once()

    assert first.state == second.state == "QUALIFICATION_EPOCH"
    assert first.epoch_id == second.epoch_id
    active = await read_active_qualification_epoch(orchestrator.state_path)
    assert active is not None
    assert active.release_sha == RELEASE_SHA
    assert active.container_image_digest == IMAGE_DIGEST
    assert active.route.value == "BTC:bybit>okx"


@pytest.mark.asyncio
async def test_production_progress_worker_reports_epoch_without_blocking_service_loop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "state" / "ipeg.sqlite3"
    data_path = tmp_path / "data"
    settings = _settings(tmp_path)
    monkeypatch.setenv("IPEG_STATE_PATH", str(state_path))
    monkeypatch.setenv("IPEG_PARQUET_DIR", str(data_path))
    monkeypatch.setenv("IPEG_RELEASE_SHA", RELEASE_SHA)
    monkeypatch.setenv("IPEG_CONTAINER_IMAGE_DIGEST", IMAGE_DIGEST)
    monkeypatch.setenv("IPEG_QUALIFICATION_ROUTE", "BTC:bybit>okx")
    _confirm_onboarding(monkeypatch)
    orchestrator = AutonomousOrchestrator(settings)

    status = await asyncio.wait_for(orchestrator.reconcile_once(), timeout=10)

    assert status.state == "QUALIFICATION_EPOCH"
    assert status.live_authorized is False
    assert "OBSERVATION_PERIOD_INSUFFICIENT" in status.blockers


@pytest.mark.asyncio
async def test_orchestrator_finalizes_collection_but_never_authorizes_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setenv("IPEG_RELEASE_SHA", RELEASE_SHA)
    monkeypatch.setenv("IPEG_CONTAINER_IMAGE_DIGEST", IMAGE_DIGEST)
    monkeypatch.setenv("IPEG_QUALIFICATION_ROUTE", "BTC:bybit>okx")
    _confirm_onboarding(monkeypatch)

    async def ready(*args: object, **kwargs: object) -> SimpleNamespace:
        del args, kwargs
        return SimpleNamespace(
            ready_to_finalize=True,
            completion_ratio=Decimal(1),
            blockers=("REPLAY_AND_RUNTIME_EVIDENCE_REQUIRED",),
        )

    monkeypatch.setattr(orchestrator_module, "build_qualification_progress", ready)
    orchestrator = AutonomousOrchestrator(settings, use_progress_subprocess=False)

    status = await orchestrator.reconcile_once()

    assert status.state == "QUALIFICATION_FINALIZED"
    assert status.live_authorized is False
    assert "do not arm canary" in status.next_action
    latest = await read_qualification_epoch(orchestrator.state_path)
    assert latest is not None
    assert latest.status == QualificationEpochStatus.FINALIZED


@pytest.mark.asyncio
async def test_cancelled_progress_worker_is_terminated_without_orphan_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = asyncio.create_subprocess_exec
    started = asyncio.Event()
    process: asyncio.subprocess.Process | None = None

    async def slow_worker(*args: object, **kwargs: object) -> asyncio.subprocess.Process:
        nonlocal process
        del args, kwargs
        process = await original(
            sys.executable,
            "-c",
            "import time; time.sleep(60)",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        started.set()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", slow_worker)
    task = asyncio.create_task(
        orchestrator_module._progress_from_subprocess(tmp_path, CONFIG, "epoch")
    )
    await asyncio.wait_for(started.wait(), timeout=2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert process is not None
    assert process.returncode is not None
