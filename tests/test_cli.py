from __future__ import annotations

import asyncio
import re
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from typer.testing import CliRunner

import interexchange_perp_grid.cli as cli_module
from interexchange_perp_grid.adapters.private import PrivateCredentials
from interexchange_perp_grid.cli import _run_public_scan, app
from interexchange_perp_grid.config import load_settings
from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.public_engine import ScanResult

runner = CliRunner()
ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
CONFIG = Path("config/defaults.yaml")
UPGRADE_OWNER = f"deployment-upgrade-{'b' * 40}"


def test_cli_and_public_scan_help_render() -> None:
    assert runner.invoke(app, ["--help"]).exit_code == 0
    public_help = runner.invoke(app, ["public-scan", "--help"])
    assert public_help.exit_code == 0
    assert "--quantity" in ANSI_ESCAPE.sub("", public_help.output)


def test_deployment_upgrade_gate_cli_persists_and_releases_freeze(tmp_path: Path) -> None:
    environment = {"IPEG_STATE_PATH": str(tmp_path / "state.sqlite3")}

    armed = runner.invoke(
        app,
        [
            "deployment-upgrade-gate",
            "--action",
            "arm",
            "--owner-token",
            UPGRADE_OWNER,
        ],
        env=environment,
    )
    released = runner.invoke(
        app,
        [
            "deployment-upgrade-gate",
            "--action",
            "release",
            "--owner-token",
            UPGRADE_OWNER,
        ],
        env=environment,
    )

    assert armed.exit_code == released.exit_code == 0
    assert '"entry_frozen": true' in armed.output
    assert '"entry_frozen": false' in released.output


def test_public_scan_rejects_non_decimal_quantity_before_network() -> None:
    result = runner.invoke(app, ["public-scan", "--quantity", "not-a-number"])
    assert result.exit_code == 2
    assert "quantity must be a decimal number" in result.output


def test_private_probe_reports_only_sanitized_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingPrivateAdapter:
        def __init__(self, venue: Venue, credentials: object | None = None) -> None:
            del venue, credentials

        async def probe_private_capabilities(self) -> object:
            raise RuntimeError("signed request and credential-like material")

        async def close(self) -> None:
            return None

    monkeypatch.setattr(cli_module, "CcxtPrivateAdapter", FailingPrivateAdapter)

    result = runner.invoke(app, ["private-probe", "--venue", "bybit"])

    assert result.exit_code == 4
    assert '"error_type": "RuntimeError"' in result.output
    assert '"qualified": false' in result.output
    assert "credential-like" not in result.output
    assert "Traceback" not in result.output


def test_private_probe_authenticated_reads_account_without_exposing_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FailingAccountAdapter:
        def __init__(self, venue: Venue, credentials: object | None = None) -> None:
            captured["venue"] = venue
            captured["credentials"] = credentials

        async def probe_private_capabilities(self) -> object:
            return object()

        async def list_instruments(self) -> tuple[SimpleNamespace, ...]:
            return (SimpleNamespace(base="BTC"),)

        async def fetch_account(self, instrument: object) -> None:
            captured["account_instrument"] = instrument
            raise RuntimeError("account response must remain private")

        async def close(self) -> None:
            captured["closed"] = True

    monkeypatch.setattr(cli_module, "CcxtPrivateAdapter", FailingAccountAdapter)
    environment = {
        "IPEG_BYBIT_API_KEY": "fixture-key",
        "IPEG_BYBIT_API_SECRET": "fixture-secret",
    }

    result = runner.invoke(
        app,
        ["private-probe", "--venue", "bybit", "--authenticated"],
        env=environment,
    )

    assert result.exit_code == 4
    assert '"error_type": "RuntimeError"' in result.output
    assert "account response" not in result.output
    assert captured["venue"] == Venue.BYBIT
    assert isinstance(captured["credentials"], PrivateCredentials)
    assert "account_instrument" in captured
    assert captured["closed"] is True


def test_public_scan_wires_every_configured_public_venue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    sentinel = cast(ScanResult, object())

    class CapturingPublicEngine:
        def __init__(self, settings: object, **kwargs: object) -> None:
            del settings
            captured.update(kwargs)

        async def scan_once(
            self,
            base: str,
            quantity: Decimal,
            timeout_seconds: int,
        ) -> ScanResult:
            del base, quantity, timeout_seconds
            return sentinel

        async def close(self) -> None:
            captured["closed"] = True

    monkeypatch.setattr(cli_module, "PublicMarketEngine", CapturingPublicEngine)
    settings = load_settings(CONFIG, {"IPEG_PARQUET_DIR": str(tmp_path)})

    result = asyncio.run(_run_public_scan(settings, "BTC", Decimal("0.01"), 1))

    assert result is sentinel
    assert captured["public_venues"] == tuple(
        Venue(value) for value in settings.venues.public_runtime
    )
    assert captured["closed"] is True


def test_canary_run_requires_explicit_live_money_phrase_before_network() -> None:
    result = runner.invoke(
        app,
        ["canary-run", "--confirmation", "NO"],
    )
    assert result.exit_code == 5
    assert "OWNER_CONFIRMATION_MISSING" in result.output


def test_emergency_flatten_requires_separate_unlock_before_network() -> None:
    result = runner.invoke(
        app,
        ["emergency-flatten", "--confirmation", "WRONG"],
    )
    assert result.exit_code == 6
    assert "EMERGENCY_UNLOCK_OR_QUALIFICATION_INVALID" in result.output


def test_deployment_identity_requires_exact_container_environment() -> None:
    release_sha = "a" * 40
    image_digest = "sha256:" + "b" * 64
    accepted = runner.invoke(
        app,
        [
            "deployment-identity",
            "--expected-release-sha",
            release_sha,
            "--expected-image-digest",
            image_digest,
        ],
        env={
            "IPEG_RELEASE_SHA": release_sha,
            "IPEG_CONTAINER_IMAGE_DIGEST": image_digest,
        },
    )
    rejected = runner.invoke(
        app,
        [
            "deployment-identity",
            "--expected-release-sha",
            release_sha,
            "--expected-image-digest",
            image_digest,
        ],
        env={
            "IPEG_RELEASE_SHA": "c" * 40,
            "IPEG_CONTAINER_IMAGE_DIGEST": image_digest,
        },
    )

    assert accepted.exit_code == 0
    assert '"status": "PASS"' in accepted.output
    assert rejected.exit_code == 8
    assert '"status": "FAIL"' in rejected.output
