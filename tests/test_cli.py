from __future__ import annotations

import asyncio
import json
import re
from datetime import UTC, datetime, timedelta
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
from interexchange_perp_grid.domain import Instrument, InstrumentKey, ProductType, Venue
from interexchange_perp_grid.public_engine import ScanResult
from interexchange_perp_grid.reference_history import ReferenceSpreadBar, SourceMinuteBar
from interexchange_perp_grid.reference_store import ParquetReferenceHistoryStore

runner = CliRunner()
ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
CONFIG = Path("config/defaults.yaml")
UPGRADE_OWNER = f"deployment-upgrade-{'b' * 40}"


def test_cli_and_public_scan_help_render() -> None:
    assert runner.invoke(app, ["--help"]).exit_code == 0
    public_help = runner.invoke(app, ["public-scan", "--help"])
    assert public_help.exit_code == 0
    assert "--quantity" in ANSI_ESCAPE.sub("", public_help.output)


def test_laptop_twelve_hour_profile_requires_explicit_local_receipt(
    tmp_path: Path,
) -> None:
    result = runner.invoke(
        app,
        ["qualification-epoch-status", "--laptop-owner-exception-12h"],
        env={"IPEG_STATE_PATH": str(tmp_path / "state.sqlite3")},
    )

    assert result.exit_code == 2
    assert "IPEG_LAPTOP_12H_OWNER_EXCEPTION" in result.output


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


def test_reference_history_proof_is_public_deterministic_and_non_executable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    instances: list[object] = []

    class PublicHistoryAdapter:
        def __init__(self, venue: Venue) -> None:
            self.venue = venue
            instances.append(self)

        async def discover_instruments(self) -> tuple[Instrument, ...]:
            return (
                Instrument(
                    venue=self.venue,
                    symbol="BTC/USDT:USDT",
                    exchange_symbol="BTCUSDT",
                    base="BTC",
                    quote="USDT",
                    settle="USDT",
                    contract_size_base=Decimal("1"),
                    amount_step_contracts=Decimal("0.001"),
                    price_tick=Decimal("0.1"),
                    minimum_amount_contracts=Decimal("0.001"),
                    minimum_notional=Decimal("5"),
                    taker_fee_rate=None,
                    fee_source=None,
                ),
            )

        async def fetch_closed_minute_bars(
            self,
            instrument: Instrument,
            since: datetime,
            limit: int,
        ) -> tuple[SourceMinuteBar, ...]:
            assert instrument.base == "BTC"
            assert since == start
            assert limit == 5
            return tuple(
                SourceMinuteBar(
                    venue=self.venue,
                    instrument=instrument.key,
                    symbol=instrument.symbol,
                    interval_start=start.replace(minute=minute),
                    open=Decimal("100"),
                    high=Decimal("101"),
                    low=Decimal("99"),
                    close=Decimal("100"),
                    contract_metadata_version=f"{self.venue.value}-v1",
                )
                for minute in range(5)
            )

        async def close(self) -> None:
            return None

    monkeypatch.setattr(cli_module, "CcxtProAdapter", PublicHistoryAdapter)

    result = runner.invoke(
        app,
        [
            "reference-history-proof",
            "--venue-a",
            "bybit",
            "--venue-b",
            "okx",
            "--since",
            start.isoformat(),
            "--limit",
            "5",
            "--output-root",
            str(tmp_path / "history"),
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "PASS"
    assert payload["source_rows"] == 10
    assert payload["reference_rows"] == 5
    assert payload["positive_directed_route"] == "BTC:okx>bybit"
    assert payload["negative_directed_route"] == "BTC:bybit>okx"
    assert payload["intervals"]["5"] == {"complete": 1, "incomplete": 0}
    assert payload["synthetic_high_low_envelope"] is True
    assert payload["executable"] is False
    assert payload["production_submit_calls"] == 0
    assert len(payload["source_sha256"]) == 64
    assert len(payload["reference_sha256"]) == 64
    assert len(instances) == 2


def test_reference_history_proof_rejects_naive_since_before_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cli_module,
        "CcxtProAdapter",
        lambda venue: pytest.fail(f"unexpected network adapter for {venue}"),
    )
    result = runner.invoke(
        app,
        [
            "reference-history-proof",
            "--venue-a",
            "bybit",
            "--venue-b",
            "okx",
            "--since",
            "2026-01-01T00:00:00",
        ],
    )
    assert result.exit_code == 2
    assert "since must include a UTC offset" in result.output


def test_aggressive_model_proof_replays_local_reference_history_without_submit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    history_root = tmp_path / "history"
    artifact = tmp_path / "model.json"
    key = InstrumentKey("BTC", "USDT", "USDT", ProductType.LINEAR_USDT_PERPETUAL)
    bars = tuple(
        ReferenceSpreadBar(
            venue_a=Venue.BYBIT,
            venue_b=Venue.OKX,
            instrument=key,
            interval_start=start + timedelta(minutes=minute),
            open_bps=Decimal("0"),
            high_bps=Decimal("10"),
            low_bps=Decimal("-10"),
            close_bps=Decimal("0"),
            contract_metadata_version_a="bybit-v1",
            contract_metadata_version_b="okx-v1",
        )
        for minute in range(5)
    )
    store = ParquetReferenceHistoryStore(history_root)
    for venue in (Venue.BYBIT, Venue.OKX):
        store.append_source_bars(
            tuple(
                SourceMinuteBar(
                    venue=venue,
                    instrument=key,
                    symbol="BTC/USDT:USDT",
                    interval_start=start + timedelta(minutes=minute),
                    open=Decimal("100"),
                    high=Decimal("101"),
                    low=Decimal("99"),
                    close=Decimal("100"),
                    contract_metadata_version=f"{venue.value}-v1",
                )
                for minute in range(5)
            )
        )
    store.append_reference_bars(bars)
    monkeypatch.setattr(cli_module, "current_code_commit_sha", lambda root: "a" * 40)

    result = runner.invoke(
        app,
        [
            "aggressive-model-proof",
            "--venue-a",
            "okx",
            "--venue-b",
            "bybit",
            "--start",
            start.isoformat(),
            "--end",
            (start + timedelta(minutes=5)).isoformat(),
            "--history-root",
            str(history_root),
            "--artifact",
            str(artifact),
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "PASS"
    assert payload["reference_rows"] == 5
    assert payload["model"]["identity"]["positive_route"] == "BTC:okx>bybit"
    assert payload["model"]["identity"]["negative_route"] == "BTC:bybit>okx"
    assert payload["positive_eligibility"] == "DISABLED"
    assert payload["negative_eligibility"] == "DISABLED"
    assert payload["executable"] is False
    assert payload["production_submit_calls"] == 0
    assert artifact.is_file()


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
