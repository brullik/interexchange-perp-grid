from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.aggressive_grid import AggressiveGridStore
from interexchange_perp_grid.aggressive_model import (
    HistoricalModelPolicy,
    HistoricalReferenceModel,
    build_historical_reference_model,
)
from interexchange_perp_grid.aggressive_qualification import (
    build_aggressive_qualification_binding,
    load_aggressive_qualification_binding,
    save_aggressive_qualification_binding,
    verify_aggressive_qualification_binding,
)
from interexchange_perp_grid.domain import InstrumentKey, ProductType, Venue
from interexchange_perp_grid.native_runtime import NativeRuntimeManifest
from interexchange_perp_grid.qualification import QualificationEvidence, QualificationPolicy
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.reference_history import ReferenceSpreadBar
from interexchange_perp_grid.state import QualificationEpochStatus
from interexchange_perp_grid.strategy import DirectedRouteKey

_NOW = datetime(2026, 8, 25, tzinfo=UTC)
_SOURCE = "b" * 64
_PROFILE = "c" * 64
_CODE = "d" * 40
_CODE_HASH = "e" * 64
_CONFIG = "f" * 64
_KEY = InstrumentKey("BTC", "USDT", "USDT", ProductType.LINEAR_USDT_PERPETUAL)


def _model() -> HistoricalReferenceModel:
    bars = tuple(
        ReferenceSpreadBar(
            venue_a=Venue.BYBIT,
            venue_b=Venue.OKX,
            instrument=_KEY,
            interval_start=_NOW + timedelta(minutes=index),
            open_bps=Decimal(0),
            high_bps=Decimal(10),
            low_bps=Decimal(-10),
            close_bps=Decimal(index % 3 - 1),
            contract_metadata_version_a="bybit-v1",
            contract_metadata_version_b="okx-v1",
        )
        for index in range(50)
    )
    return build_historical_reference_model(
        bars,
        policy=HistoricalModelPolicy(
            history_target_days=Decimal("0.03"),
            history_minimum_live_days=Decimal("0.02"),
            history_minimum_shadow_days=Decimal("0.01"),
        ),
        source_manifest_sha256=_SOURCE,
        strategy_profile_sha256=_PROFILE,
        code_sha=_CODE,
    )


def _qualification(route: str) -> QualificationEvidence:
    base, venues = route.split(":", 1)
    long_venue, short_venue = venues.split(">", 1)
    return QualificationEvidence(
        schema_version=3,
        generated_at=_NOW,
        epoch_id="epoch-aggressive",
        epoch_started_at=_NOW - timedelta(hours=24),
        epoch_ended_at=_NOW,
        epoch_status=QualificationEpochStatus.FINALIZED,
        route=DirectedRouteKey(base, Venue(long_venue), Venue(short_venue)),
        code_commit_sha=_CODE,
        code_sha256=_CODE_HASH,
        config_sha256=_CONFIG,
        data_sha256="1" * 64,
        data_manifest={},
        container_image_digest="sha256:" + "2" * 64,
        venue_statistics=(),
        route_observation_period_seconds=Decimal(86_400),
        private_taker_fee_rates={},
        funding_checkpoint_counts={},
        funding_intervals={},
        replay_shadow=None,
        strategy=None,
        policy=QualificationPolicy(),
        accepted=True,
        blockers=(),
        reason=ReasonCode.QUALIFICATION_PASSED,
        qualification_hash="3" * 64,
    )


def _runtime() -> NativeRuntimeManifest:
    return NativeRuntimeManifest(
        schema_version=1,
        generated_at=_NOW,
        runtime_kind="native-python",
        release_sha=_CODE,
        source_sha256=_CODE_HASH,
        config_sha256=_CONFIG,
        requirements_lock_sha256="4" * 64,
        interpreter_sha256="5" * 64,
        installed_distributions_sha256="6" * 64,
        python_version="3.12.13",
        platform="Windows",
        artifact_digest="sha256:" + "7" * 64,
    )


def test_aggressive_qualification_binds_exact_geometry_and_detects_tampering(
    tmp_path: Path,
) -> None:
    model = _model()
    grid = AggressiveGridStore(tmp_path / "grid.sqlite3")
    grid.initialise()
    for direction in (model.positive.direction, model.negative.direction):
        grid.initialise_route(
            model,
            direction,
            now=_NOW,
            rearm_retreat_step_fraction=Decimal("0.25"),
        )
    qualification = _qualification(model.positive_route)
    binding = build_aggressive_qualification_binding(
        qualification,
        model,
        _runtime(),
        grid,
        profile_sha256=_PROFILE,
        now=_NOW,
    )
    path = tmp_path / "binding.json"
    save_aggressive_qualification_binding(path, binding)
    loaded = load_aggressive_qualification_binding(path)
    assert loaded == binding
    assert loaded.positive.levels_bps == model.positive.levels_bps
    assert loaded.negative.tranche_weights == model.negative.tranche_weights
    assert loaded.execution_authorized is False
    verify_aggressive_qualification_binding(
        loaded,
        qualification,
        model,
        _runtime(),
        grid,
        profile_sha256=_PROFILE,
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["positive"]["levels_bps"][0] = "999"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        load_aggressive_qualification_binding(path)


def test_aggressive_qualification_rejects_any_identity_drift(tmp_path: Path) -> None:
    model = _model()
    grid = AggressiveGridStore(tmp_path / "grid.sqlite3")
    grid.initialise()
    for direction in (model.positive.direction, model.negative.direction):
        grid.initialise_route(
            model,
            direction,
            now=_NOW,
            rearm_retreat_step_fraction=Decimal("0.25"),
        )
    with pytest.raises(ValueError, match="identity mismatch"):
        build_aggressive_qualification_binding(
            _qualification(model.positive_route),
            model,
            replace(_runtime(), config_sha256="8" * 64),
            grid,
            profile_sha256=_PROFILE,
            now=_NOW,
        )
    with pytest.raises(ValueError, match="not part"):
        build_aggressive_qualification_binding(
            _qualification("ETH:bybit>okx"),
            model,
            _runtime(),
            grid,
            profile_sha256=_PROFILE,
            now=_NOW,
        )
