from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.domain import BookLevel, OrderBookSnapshot, Venue
from interexchange_perp_grid.history import ParquetMarketRecorder
from interexchange_perp_grid.qualification import (
    FundingCheckpoint,
    QualificationPolicy,
    QualificationRuntimeEvidence,
    QualifiedStrategyParameters,
    ReplayShadowStatistics,
    code_hash,
    config_hash,
    qualification_is_current,
    run_qualification,
)
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.state import QualificationEpochStatus
from interexchange_perp_grid.strategy import DirectedRouteKey

_SHA = "a" * 40
_IMAGE = "sha256:" + "b" * 64
_ROUTE = DirectedRouteKey("BTC", Venue.BINANCE_USDM, Venue.OKX)


def _runtime(source_sha256: str, config_sha256: str) -> QualificationRuntimeEvidence:
    start = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)
    return QualificationRuntimeEvidence(
        epoch_id="epoch-fixture",
        epoch_started_at=start,
        epoch_ended_at=start + timedelta(hours=24),
        epoch_status=QualificationEpochStatus.FINALIZED,
        route=_ROUTE,
        release_code_sha=_SHA,
        source_sha256=source_sha256,
        config_sha256=config_sha256,
        container_image_digest=_IMAGE,
        private_taker_fee_rates={
            Venue.BINANCE_USDM: Decimal("0.0004"),
            Venue.OKX: Decimal("0.0005"),
        },
        funding_checkpoints=tuple(
            FundingCheckpoint(
                venue,
                start + timedelta(hours=index * 8),
                Decimal("0.0001"),
                int((start + timedelta(hours=(index + 1) * 8)).timestamp() * 1000),
                "8h",
            )
            for venue in (Venue.BINANCE_USDM, Venue.OKX)
            for index in range(3)
        ),
        replay_shadow=ReplayShadowStatistics(
            replay_completed=True,
            accepted_signals=2,
            rejected_signals=7,
            simulated_net_pnl_usdt=Decimal("1.25"),
            maximum_adverse_excursion_usdt=Decimal("0.40"),
            unresolved_order_count=0,
            unresolved_exposure_count=0,
            unhandled_exception_count=0,
        ),
        strategy=QualifiedStrategyParameters(
            calibration_version=7,
            size_bucket_base_quantity=Decimal("0.001"),
            adaptive_entry_threshold_bps=Decimal("12"),
            target_exit_spread_bps=Decimal("2"),
            minimum_profit_usdt=Decimal("0.01"),
            stressed_cost_multiplier=Decimal("2"),
            expected_holding_seconds=300,
            maximum_holding_seconds=3600,
        ),
    )


def _policy() -> QualificationPolicy:
    return QualificationPolicy(
        minimum_duration_seconds=2,
        minimum_synchronised_snapshots_per_venue=3,
        minimum_funding_checkpoints_per_venue=3,
        maximum_inter_snapshot_gap_seconds=1,
        maximum_sequence_gaps=0,
        maximum_stale_snapshots=0,
        maximum_sequence_unknown_snapshots=0,
        maximum_clock_skew_snapshots=0,
        maximum_clock_skew_ms=1000,
        maximum_snapshot_age_ms=1000,
    )


async def _record_route(data: Path, start: datetime, levels: int = 1) -> None:
    recorder = ParquetMarketRecorder(data)
    for index in range(3):
        observed = start + timedelta(seconds=index)
        timestamp = int(observed.timestamp() * 1000)
        await recorder.append_books(
            tuple(
                OrderBookSnapshot(
                    venue,
                    "BTC/USDT:USDT",
                    tuple(
                        BookLevel(Decimal("100") - level, Decimal("1")) for level in range(levels)
                    ),
                    tuple(
                        BookLevel(Decimal("101") + level, Decimal("1")) for level in range(levels)
                    ),
                    timestamp,
                    observed,
                    index + 1,
                    index + 1,
                    index + 1,
                    True,
                    True,
                    0,
                )
                for venue in (Venue.BINANCE_USDM, Venue.OKX)
            )
        )


@pytest.mark.asyncio
async def test_qualification_counts_unique_events_not_book_levels(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "src" / "product.py").write_text("SAFE = True\n", encoding="utf-8")
    config = repo / "config.yaml"
    config.write_text("mode: shadow\n", encoding="utf-8")
    data = repo / "data"
    observed = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)
    recorder = ParquetMarketRecorder(data)
    await recorder.append_books(
        (
            OrderBookSnapshot(
                Venue.BINANCE_USDM,
                "BTC/USDT:USDT",
                tuple(BookLevel(Decimal("100") - i, Decimal("1")) for i in range(5)),
                tuple(BookLevel(Decimal("101") + i, Decimal("1")) for i in range(5)),
                int(observed.timestamp() * 1000),
                observed,
                1,
                1,
                1,
                True,
                True,
                0,
            ),
        )
    )
    monkeypatch.setenv("IPEG_RELEASE_SHA", _SHA)
    evidence = run_qualification(
        repo,
        config,
        data,
        repo / "qualification.json",
        runtime_evidence=_runtime(code_hash(repo), config_hash(config)),
        policy=_policy(),
        now=observed,
    )
    stats = {item.venue: item for item in evidence.venue_statistics}
    assert stats[Venue.BINANCE_USDM].unique_order_book_events == 1
    assert evidence.accepted is False
    assert "BINANCEUSDM_SYNCHRONISED_SNAPSHOTS_INSUFFICIENT" in evidence.blockers


@pytest.mark.asyncio
async def test_route_qualification_binds_complete_release_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    code_file = repo / "src" / "product.py"
    code_file.write_text("SAFE = True\n", encoding="utf-8")
    config = repo / "config.yaml"
    config.write_text("mode: shadow\n", encoding="utf-8")
    data = repo / "data"
    observed = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)
    await _record_route(data, observed, levels=4)
    monkeypatch.setenv("IPEG_RELEASE_SHA", _SHA)

    evidence = run_qualification(
        repo,
        config,
        data,
        repo / "qualification.json",
        runtime_evidence=_runtime(code_hash(repo), config_hash(config)),
        policy=_policy(),
        now=observed + timedelta(seconds=2),
    )
    assert evidence.accepted is True
    assert evidence.route == _ROUTE
    assert evidence.route_observation_period_seconds == 2
    assert {item.unique_order_book_events for item in evidence.venue_statistics} == {3}
    assert evidence.funding_checkpoint_counts == {
        Venue.BINANCE_USDM: 3,
        Venue.OKX: 3,
    }
    assert qualification_is_current(
        evidence,
        repo,
        config,
        data,
        3600,
        observed + timedelta(minutes=1),
        expected_route=_ROUTE,
        current_container_image_digest=_IMAGE,
        current_release_code_sha=_SHA,
    ) == (True, ReasonCode.QUALIFICATION_PASSED)
    mismatched_policy = replace(evidence.policy, minimum_duration_seconds=43_200)
    assert qualification_is_current(
        evidence,
        repo,
        config,
        data,
        3600,
        now=observed + timedelta(minutes=1),
        expected_route=_ROUTE,
        current_container_image_digest=_IMAGE,
        current_release_code_sha=_SHA,
        accepted_policies=(mismatched_policy,),
    ) == (False, ReasonCode.QUALIFICATION_HASH_MISMATCH)
    assert qualification_is_current(
        evidence,
        repo,
        config,
        data,
        3600,
        now=observed + timedelta(minutes=1),
        expected_route=_ROUTE,
        current_container_image_digest=_IMAGE,
        current_release_code_sha=_SHA,
        accepted_policies=(evidence.policy,),
    ) == (True, ReasonCode.QUALIFICATION_PASSED)

    await _record_route(data, observed + timedelta(seconds=10))
    assert qualification_is_current(
        evidence,
        repo,
        config,
        data,
        3600,
        observed + timedelta(minutes=1),
        expected_route=_ROUTE,
        current_container_image_digest=_IMAGE,
        current_release_code_sha=_SHA,
    ) == (True, ReasonCode.QUALIFICATION_PASSED)
    manifest_path = data / next(iter(evidence.data_manifest))
    original_bytes = manifest_path.read_bytes()
    manifest_path.write_bytes(original_bytes + b"tampered")
    assert qualification_is_current(
        evidence,
        repo,
        config,
        data,
        3600,
        observed + timedelta(minutes=1),
        expected_route=_ROUTE,
        current_container_image_digest=_IMAGE,
        current_release_code_sha=_SHA,
    ) == (False, ReasonCode.QUALIFICATION_HASH_MISMATCH)
    manifest_path.write_bytes(original_bytes)

    code_file.write_text("SAFE = False\n", encoding="utf-8")
    assert qualification_is_current(
        evidence,
        repo,
        config,
        data,
        3600,
        observed + timedelta(minutes=1),
        expected_route=_ROUTE,
        current_container_image_digest=_IMAGE,
        current_release_code_sha=_SHA,
    ) == (False, ReasonCode.QUALIFICATION_HASH_MISMATCH)
    code_file.write_text("SAFE = True\n", encoding="utf-8")
    assert qualification_is_current(
        evidence,
        repo,
        config,
        data,
        3600,
        observed + timedelta(minutes=1),
        expected_route=_ROUTE,
        current_container_image_digest="sha256:" + "c" * 64,
        current_release_code_sha=_SHA,
    ) == (False, ReasonCode.QUALIFICATION_HASH_MISMATCH)


def test_missing_runtime_evidence_is_an_honest_failure(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "src" / "product.py").write_text("SAFE = True\n", encoding="utf-8")
    config = repo / "config.yaml"
    config.write_text("mode: shadow\n", encoding="utf-8")
    evidence = run_qualification(
        repo,
        config,
        repo / "missing-data",
        repo / "qualification.json",
    )
    assert evidence.accepted is False
    assert evidence.blockers == ("RUNTIME_EVIDENCE_MISSING",)
    assert evidence.reason == ReasonCode.QUALIFICATION_INSUFFICIENT
