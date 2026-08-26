from __future__ import annotations

import asyncio
import json
import re
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
import typer
from typer.testing import CliRunner

import interexchange_perp_grid.cli as cli_module
from interexchange_perp_grid.adapters.private import PrivateCredentials
from interexchange_perp_grid.aggressive_model import (
    DivergenceDirection,
    HistoricalReferenceModel,
)
from interexchange_perp_grid.cli import (
    _aggressive_effective_stop,
    _aggressive_reserves_per_base,
    _fast_live_cost_evidence_complete,
    _run_public_scan,
    app,
)
from interexchange_perp_grid.config import load_settings
from interexchange_perp_grid.domain import Instrument, InstrumentKey, ProductType, Venue
from interexchange_perp_grid.live_journal import (
    LiveActionState,
    LiveJournalAction,
    LiveOrderJournal,
)
from interexchange_perp_grid.public_engine import ScanResult
from interexchange_perp_grid.reference_history import (
    SourceBarQuality,
    SourceMinuteBar,
    build_reference_series,
)
from interexchange_perp_grid.reference_store import ParquetReferenceHistoryStore
from interexchange_perp_grid.strategy import DirectedRouteKey

runner = CliRunner()
ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
CONFIG = Path("config/defaults.yaml")
UPGRADE_OWNER = f"deployment-upgrade-{'b' * 40}"


@pytest.mark.parametrize("tail_during_private", [False, True])
@pytest.mark.parametrize("command", ["acceptance", "handoff"])
def test_acceptance_commands_reject_post_pilot_journal_tail_around_private_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tail_during_private: bool,
    command: str,
) -> None:
    names = (
        "acceptance",
        "runtime",
        "binding",
        "qualification",
        "model",
        "grid",
        "live-grid",
        "canary",
        "pilot",
        "profile",
    )
    paths = {name: tmp_path / f"{name}.json" for name in names}
    for path in paths.values():
        path.write_text("{}", encoding="utf-8")
    pilot = SimpleNamespace(ended_at=datetime.now(UTC))
    binding = SimpleNamespace(
        qualification_route="BTC:binanceusdm>okx",
        qualification_hash="a" * 64,
    )
    settings = SimpleNamespace(
        storage=SimpleNamespace(
            sqlite_path=str(tmp_path / "state.sqlite3"),
            parquet_dir=str(tmp_path / "parquet"),
        ),
        live=SimpleNamespace(qualification_max_age_seconds=1),
    )

    monkeypatch.setattr(cli_module, "_load", lambda _path: settings)
    monkeypatch.setattr(cli_module, "load_aggressive_laptop_acceptance", lambda _path: object())
    monkeypatch.setattr(
        cli_module,
        "verify_native_runtime_manifest",
        lambda *_args: SimpleNamespace(artifact_digest="sha256:" + "b" * 64),
    )
    monkeypatch.setattr(cli_module, "load_aggressive_qualification_binding", lambda _path: binding)
    monkeypatch.setattr(cli_module, "load_qualification", lambda _path: object())
    monkeypatch.setattr(
        cli_module, "qualification_is_current", lambda *_args, **_kwargs: (True, ())
    )
    monkeypatch.setattr(
        cli_module, "qualification_policy_from_settings", lambda _settings: object()
    )
    monkeypatch.setattr(cli_module, "laptop_owner_exception_policy", lambda _settings: object())
    monkeypatch.setattr(cli_module, "load_historical_model", lambda _path: object())
    monkeypatch.setattr(cli_module, "_verify_aggressive_model_window", lambda *_args: None)
    monkeypatch.setattr(
        cli_module,
        "AggressiveGridStore",
        lambda _path: SimpleNamespace(initialise=lambda: None),
    )
    monkeypatch.setattr(
        cli_module, "verify_aggressive_qualification_binding", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(cli_module, "_require_aggressive_live_grid_flat", lambda *_args: None)
    monkeypatch.setattr(
        cli_module,
        "load_aggressive_laptop_stage_evidence",
        lambda path: pilot if path == paths["pilot"].resolve() else SimpleNamespace(),
    )

    private_called = False

    async def changed_tail(
        self: object, boundary: datetime, qualification_hash: str
    ) -> tuple[object, ...]:
        assert boundary == pilot.ended_at
        assert qualification_hash == binding.qualification_hash
        return (object(),) if not tail_during_private or private_called else ()

    async def private_access(*_args: object) -> object:
        nonlocal private_called
        private_called = True
        return object()

    monkeypatch.setattr(LiveOrderJournal, "actions_updated_after", changed_tail)
    monkeypatch.setattr(cli_module, "collect_authoritative_live_flat_evidence", private_access)

    with pytest.raises(typer.BadParameter, match="JOURNAL_CHANGED_AFTER_ACCEPTED_PILOT"):
        if command == "handoff":
            cli_module.aggressive_vps_handoff_check(
                paths["acceptance"],
                paths["runtime"],
                paths["binding"],
                paths["qualification"],
                paths["model"],
                paths["grid"],
                paths["live-grid"],
                paths["canary"],
                paths["pilot"],
                paths["profile"],
                tmp_path / "history",
                tmp_path,
                tmp_path / "config.yaml",
            )
        else:
            cli_module.aggressive_laptop_acceptance(
                paths["binding"],
                paths["runtime"],
                paths["canary"],
                paths["pilot"],
                paths["qualification"],
                paths["model"],
                paths["grid"],
                paths["live-grid"],
                paths["profile"],
                tmp_path / "history",
                tmp_path / "output.json",
                tmp_path,
                tmp_path / "config.yaml",
            )
    assert private_called is tail_during_private


def test_cli_and_public_scan_help_render() -> None:
    assert runner.invoke(app, ["--help"]).exit_code == 0
    public_help = runner.invoke(app, ["public-scan", "--help"])
    assert public_help.exit_code == 0
    assert "--quantity" in ANSI_ESCAPE.sub("", public_help.output)


@pytest.mark.parametrize(
    "command",
    [
        "fast-live-preflight",
        "fast-live-canary",
        "fast-live-pilot",
        "fast-live-stage-select",
    ],
)
def test_current_fast_live_contract_has_no_qualification_dependency(command: str) -> None:
    rendered = runner.invoke(app, [command, "--help"])
    output = ANSI_ESCAPE.sub("", rendered.output)

    assert rendered.exit_code == 0, output
    assert "--qualification" not in output
    if command != "fast-live-stage-select":
        assert "--preflight" in output


def test_fast_live_acceptance_requires_complete_private_fee_and_funding_evidence() -> None:
    observed = datetime(2026, 8, 26, tzinfo=UTC)
    reservation: dict[str, object] = {
        "initial_private_taker_fee_rates": {
            Venue.BYBIT.value: "0.00055",
            Venue.OKX.value: "0.00050",
        },
        "initial_funding_rates": {
            Venue.BYBIT.value: "-0.0001",
            Venue.OKX.value: "0.0002",
        },
        "initial_funding_next_timestamp_ms": {
            Venue.BYBIT.value: "1787702400000",
            Venue.OKX.value: "1787702400000",
        },
        "actual_fill_risk": {
            "incremental_stress_usdt": "0.4",
            "route_total_usdt": "0.4",
            "portfolio_total_usdt": "0.4",
            "actual_entry_spread_bps": "8.1",
            "actual_open_fees_usdt": "0.05",
            "remaining_close_fees_usdt": "0.05",
            "initial_measured_book_impact_usdt": "0.02",
            "adverse_funding_usdt": "0.01",
            "other_reserves_usdt": "0.2",
            "realized_funding_usdt": "0",
            "fill_event_watermark": 4,
        },
    }
    action = LiveJournalAction(
        pair_action_id="fast-live-canary",
        route=DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX),
        tranche_id="level-1",
        state=LiveActionState.FLAT,
        risk_reservation=reservation,
        qualification_hash="0" * 64,
        residual_delta=Decimal(0),
        recovery_action=None,
        created_at=observed,
        updated_at=observed,
        legs=(),
        activation_hash="a" * 64,
    )

    assert _fast_live_cost_evidence_complete(action)
    assert not _fast_live_cost_evidence_complete(
        replace(
            action,
            risk_reservation={
                **reservation,
                "initial_funding_rates": {Venue.BYBIT.value: "NaN"},
            },
        )
    )
    actual_risk = reservation["actual_fill_risk"]
    assert isinstance(actual_risk, dict)
    assert not _fast_live_cost_evidence_complete(
        replace(
            action,
            risk_reservation={
                **reservation,
                "actual_fill_risk": {
                    key: value
                    for key, value in actual_risk.items()
                    if key != "realized_funding_usdt"
                },
            },
        )
    )
    assert not _fast_live_cost_evidence_complete(
        replace(
            action,
            risk_reservation={
                **reservation,
                "initial_funding_next_timestamp_ms": {
                    Venue.BYBIT.value: "0",
                    Venue.OKX.value: "1787702400000",
                },
            },
        )
    )


def test_effective_stop_uses_directional_tail_distance_from_nonzero_mode() -> None:
    model = SimpleNamespace(
        s0_bps=Decimal("50"),
        positive=SimpleNamespace(
            directional_q999_bps=Decimal("7"),
            reference_stop_bps=Decimal("55"),
        ),
        negative=SimpleNamespace(
            directional_q999_bps=Decimal("11"),
            reference_stop_bps=Decimal("42"),
        ),
    )

    typed_model = cast(HistoricalReferenceModel, model)
    assert _aggressive_effective_stop(typed_model, DivergenceDirection.POSITIVE) == Decimal("57")
    assert _aggressive_effective_stop(typed_model, DivergenceDirection.NEGATIVE) == Decimal("39")


def test_aggressive_reserves_charge_a_distinct_liquidation_distance_component() -> None:
    reserves = _aggressive_reserves_per_base(load_settings(CONFIG), Decimal("100"))

    assert reserves.liquidation_distance_usdt > 0
    assert reserves.total() > reserves.liquidation_distance_usdt


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
    monkeypatch.setattr(cli_module, "current_code_commit_sha", lambda root: "a" * 40)

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

    model_artifact = tmp_path / "model.json"
    replay = runner.invoke(
        app,
        [
            "aggressive-model-proof",
            "--venue-a",
            "bybit",
            "--venue-b",
            "okx",
            "--start",
            start.isoformat(),
            "--end",
            (start + timedelta(minutes=5)).isoformat(),
            "--history-root",
            str(tmp_path / "history"),
            "--artifact",
            str(model_artifact),
            "--profile",
            "config/AGGRESSIVE_FAST_LIVE_V2.yaml",
        ],
    )
    assert replay.exit_code == 0, replay.output
    replay_payload = json.loads(replay.output)
    assert replay_payload["status"] == "PASS"
    assert replay_payload["reference_rows"] == 5
    assert replay_payload["production_submit_calls"] == 0
    assert model_artifact.is_file()


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


def test_reference_history_proof_paginates_an_exact_requested_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    calls: list[tuple[Venue, datetime, int]] = []

    class PagedHistoryAdapter:
        def __init__(self, venue: Venue) -> None:
            self.venue = venue

        async def discover_instruments(self) -> tuple[Instrument, ...]:
            return (
                Instrument(
                    self.venue,
                    "BTC/USDT:USDT",
                    "BTCUSDT",
                    "BTC",
                    "USDT",
                    "USDT",
                    Decimal(1),
                    Decimal("0.001"),
                    Decimal("0.1"),
                    Decimal("0.001"),
                    Decimal(5),
                    None,
                    None,
                ),
            )

        async def fetch_closed_minute_bars(
            self, instrument: Instrument, since: datetime, limit: int
        ) -> tuple[SourceMinuteBar, ...]:
            calls.append((self.venue, since, limit))
            return tuple(
                SourceMinuteBar(
                    venue=self.venue,
                    instrument=instrument.key,
                    symbol=instrument.symbol,
                    interval_start=since + timedelta(minutes=index),
                    open=Decimal(100),
                    high=Decimal(101),
                    low=Decimal(99),
                    close=Decimal(100),
                    contract_metadata_version="v1",
                )
                for index in range(limit)
            )

        async def close(self) -> None:
            return None

    monkeypatch.setattr(cli_module, "CcxtProAdapter", PagedHistoryAdapter)
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
            "--end",
            (start + timedelta(minutes=12)).isoformat(),
            "--limit",
            "5",
            "--output-root",
            str(tmp_path / "history"),
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["reference_rows"] == 12
    assert sorted((since - start).seconds // 60 for _, since, _ in calls) == [0, 0, 5, 5, 10, 10]
    assert sorted(limit for _, _, limit in calls) == [2, 2, 5, 5, 5, 5]


def test_reference_history_paginator_does_not_skip_short_exchange_pages(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    calls: list[tuple[Venue, datetime, int]] = []

    class ShortPageAdapter:
        def __init__(self, venue: Venue) -> None:
            self.venue = venue

        async def discover_instruments(self) -> tuple[Instrument, ...]:
            return (
                Instrument(
                    self.venue,
                    "BTC/USDT:USDT",
                    "BTCUSDT",
                    "BTC",
                    "USDT",
                    "USDT",
                    Decimal(1),
                    Decimal("0.001"),
                    Decimal("0.1"),
                    Decimal("0.001"),
                    Decimal(5),
                    None,
                    None,
                ),
            )

        async def fetch_closed_minute_bars(
            self, instrument: Instrument, since: datetime, limit: int
        ) -> tuple[SourceMinuteBar, ...]:
            calls.append((self.venue, since, limit))
            returned = min(limit, 5)
            return tuple(
                SourceMinuteBar(
                    self.venue,
                    instrument.key,
                    instrument.symbol,
                    since + timedelta(minutes=index),
                    Decimal(100),
                    Decimal(101),
                    Decimal(99),
                    Decimal(100),
                    "v1",
                )
                for index in range(returned)
            )

        async def close(self) -> None:
            return None

    monkeypatch.setattr(cli_module, "CcxtProAdapter", ShortPageAdapter)
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
            "--end",
            (start + timedelta(minutes=12)).isoformat(),
            "--limit",
            "1000",
            "--output-root",
            str(tmp_path / "history"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["reference_rows"] == 12
    assert sorted({(since - start).seconds // 60 for _, since, _ in calls}) == [0, 5, 10]


def test_conflicting_source_duplicates_become_explicit_ambiguity() -> None:
    interval = datetime(2026, 1, 1, tzinfo=UTC)
    key = InstrumentKey("BTC", "USDT", "USDT", ProductType.LINEAR_USDT_PERPETUAL)
    first = SourceMinuteBar(
        Venue.BYBIT,
        key,
        "BTC/USDT:USDT",
        interval,
        Decimal(100),
        Decimal(101),
        Decimal(99),
        Decimal(100),
        "v1",
    )
    conflict = replace(first, close=Decimal(102))

    normalized = cli_module._normalize_source_page_duplicates((first, conflict))

    assert len(normalized) == 1
    assert normalized[0].quality == SourceBarQuality.AMBIGUOUS_DUPLICATE


def test_aggressive_model_proof_replays_local_reference_history_without_submit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    history_root = tmp_path / "history"
    artifact = tmp_path / "model.json"
    key = InstrumentKey("BTC", "USDT", "USDT", ProductType.LINEAR_USDT_PERPETUAL)
    store = ParquetReferenceHistoryStore(history_root)
    source: dict[Venue, tuple[SourceMinuteBar, ...]] = {}
    for venue in (Venue.BYBIT, Venue.OKX):
        source[venue] = tuple(
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
        store.append_source_bars(source[venue])
    series = build_reference_series(
        source[Venue.BYBIT],
        source[Venue.OKX],
        window_start=start,
        window_end=start + timedelta(minutes=5),
    )
    store.append_reference_bars(series.bars)
    store.write_window_manifest(
        series,
        source[Venue.BYBIT],
        source[Venue.OKX],
    )
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


def test_aggressive_shadow_cli_uses_public_non_submit_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = tmp_path / "model.json"
    model.write_text("{}", encoding="utf-8")

    async def run_once(*args: object, **kwargs: object) -> dict[str, object]:
        assert args
        assert kwargs == {}
        return {
            "status": "PASS",
            "live_enabled": False,
            "production_submit_calls": 0,
        }

    monkeypatch.setattr(cli_module, "_run_aggressive_shadow_once", run_once)
    result = runner.invoke(
        app,
        [
            "aggressive-shadow-once",
            "--model",
            str(model),
            "--history-root",
            str(tmp_path / "history"),
            "--grid",
            str(tmp_path / "grid.sqlite3"),
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload == {
        "live_enabled": False,
        "production_submit_calls": 0,
        "status": "PASS",
    }


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
    assert result.exit_code == 2
    assert "legacy canary-run is disabled" in result.output


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
