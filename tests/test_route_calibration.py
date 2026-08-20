from __future__ import annotations

import asyncio
import sqlite3
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

import interexchange_perp_grid.route_calibration as route_calibration_module
from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.route_calibration import (
    PersistentRouteCalibrator,
    RouteCalibrationAssessment,
    RouteCalibrationObservation,
    RouteCalibrationSamplingPolicy,
    calibrate_route_size,
)
from interexchange_perp_grid.strategy import DirectedRouteKey


def observation(
    route: DirectedRouteKey,
    size: str,
    observed_at: datetime,
    spread: str,
    *,
    epoch: str = "epoch-1",
    adverse: str | None = "2",
    convergence: str | None = "30",
    funding: str | None = "0.0001",
    depth: str | None = "4",
    cost: str | None = "2",
    reason: ReasonCode = ReasonCode.QUOTE_READY,
    base_quantity: str | None = None,
    episode_entry: str | None = None,
) -> RouteCalibrationObservation:
    return RouteCalibrationObservation(
        route=route,
        size_bucket_multiplier=Decimal(size),
        base_quantity=(Decimal(base_quantity) if base_quantity is not None else Decimal(size)),
        epoch_id=epoch,
        observed_at=observed_at,
        spread_bps=Decimal(spread) if reason == ReasonCode.QUOTE_READY else None,
        adverse_excursion_after_entry_bps=(Decimal(adverse) if adverse is not None else None),
        convergence_seconds=Decimal(convergence) if convergence is not None else None,
        stressed_cost_floor_bps=(
            Decimal(cost) if reason == ReasonCode.QUOTE_READY and cost is not None else None
        ),
        normalized_tick_bps=Decimal("0.5") if reason == ReasonCode.QUOTE_READY else None,
        notional_usdt=Decimal("1000") if reason == ReasonCode.QUOTE_READY else None,
        funding_rate_delta=Decimal(funding) if funding is not None else None,
        exit_depth_multiple=Decimal(depth) if depth is not None else None,
        reason=reason,
        episode_entry_spread_bps=(Decimal(episode_entry) if episode_entry is not None else None),
    )


def qualified_window(
    route: DirectedRouteKey,
    size: str,
    now: datetime,
    *,
    shift: Decimal = Decimal(0),
    epoch: str = "epoch-1",
) -> tuple[RouteCalibrationObservation, ...]:
    return tuple(
        observation(
            route,
            size,
            now - timedelta(hours=20 - index * 5),
            str(Decimal(10 + index) + shift),
            epoch=epoch,
            adverse=str(index + 1),
            convergence=str((index + 1) * 10),
        )
        for index in range(5)
    )


def test_locked_formula_is_route_size_specific_and_change_bounded() -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    route = DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX)
    first = calibrate_route_size(
        qualified_window(route, "0.01", now),
        now=now,
        minimum_samples=5,
        minimum_observation_period=timedelta(hours=20),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
    )

    assert first.ready is True
    assert first.parameters is not None
    assert first.parameters.robust_sigma_bps == Decimal("1.4826")
    assert first.parameters.entry_levels_bps == (
        Decimal("15.70650"),
        Decimal("19.70650"),
        Decimal("23.70650"),
        Decimal("27.70650"),
        Decimal("31.70650"),
    )
    assert first.parameters.grid_step_bps == Decimal(4)
    assert first.parameters.route_stop_bps == Decimal("37.70650")
    assert first.parameters.target_close_reference_bps == Decimal("11.70650")
    assert first.parameters.execution_authorized is False

    changed_window = tuple(
        observation(
            route,
            "0.01",
            now + timedelta(hours=1) - timedelta(hours=20 - index * 5),
            str(10 + index),
            adverse="100",
            convergence=str((index + 1) * 10),
            cost="100",
        )
        for index in range(5)
    )
    changed = calibrate_route_size(
        changed_window,
        now=now + timedelta(hours=1),
        minimum_samples=5,
        minimum_observation_period=timedelta(hours=20),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        previous=first.parameters,
    )
    assert changed.parameters is None
    assert changed.reason == ReasonCode.CALIBRATION_REGIME_SHIFT
    assert changed.staged_parameters is not None
    assert changed.staged_parameters.entry_levels_bps[0] == Decimal("18.847800")


def test_time_to_convergence_is_calibrated_by_grid_spread_bucket() -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    route = DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX)
    expected_entries = ("16", "20", "24", "28", "32")
    expected_convergence = ("10", "20", "30", "40", "50")
    observations = tuple(
        observation(
            route,
            "0.01",
            now - timedelta(minutes=1200 - sample_index * 1200 // 14),
            str(10 + sample_index % 5),
            adverse=str(sample_index % 5 + 1),
            episode_entry=expected_entries[sample_index // 3],
            convergence=expected_convergence[sample_index // 3],
        )
        for sample_index in range(15)
    )

    assessment = calibrate_route_size(
        observations,
        now=now,
        minimum_samples=5,
        minimum_observation_period=timedelta(hours=20),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        minimum_convergence_samples_per_spread_bucket=3,
    )

    assert assessment.parameters is not None
    buckets = assessment.parameters.convergence_by_spread_bucket
    assert tuple(item.lower_bound_bps for item in buckets) == assessment.parameters.entry_levels_bps
    assert tuple(item.sample_count for item in buckets) == (3, 3, 3, 3, 3)
    assert all(item.ready for item in buckets)
    assert tuple(item.convergence_p90_seconds for item in buckets) == tuple(
        Decimal(value) for value in expected_convergence
    )
    assert assessment.parameters.convergence_p90_for_spread(Decimal("25")) == Decimal(30)
    assert (
        assessment.parameters.convergence_p90_for_spread(
            assessment.parameters.entry_levels_bps[0] - Decimal("0.00001")
        )
        is None
    )
    malformed_buckets = (
        replace(buckets[0], upper_bound_bps=Decimal(999)),
        *buckets[1:],
    )
    with pytest.raises(ValueError, match="contiguous"):
        replace(assessment.parameters, convergence_by_spread_bucket=malformed_buckets)


def test_spread_bucket_rejects_bootstrap_episodes_below_final_entry_floor() -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    route = DirectedRouteKey("HIGHCOST", Venue.BYBIT, Venue.OKX)
    bootstrap = tuple(
        replace(
            item,
            stressed_cost_floor_bps=Decimal(100),
            episode_entry_spread_bps=Decimal(10),
            convergence_seconds=Decimal(1),
        )
        for item in qualified_window(route, "1", now)
    )
    assessment = calibrate_route_size(
        bootstrap,
        now=now,
        minimum_samples=5,
        minimum_observation_period=timedelta(hours=20),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
    )

    assert assessment.parameters is not None
    levels = assessment.parameters.entry_levels_bps
    assert tuple(
        bucket.sample_count for bucket in assessment.parameters.convergence_by_spread_bucket
    ) == (0, 0, 0, 0, 0)
    assert assessment.parameters.convergence_p90_for_spread(levels[0]) is None

    boundary_observations = tuple(
        replace(
            item,
            episode_entry_spread_bps=levels[index],
            convergence_seconds=Decimal(index + 1),
        )
        for index, item in enumerate(bootstrap)
    )
    boundary_assessment = calibrate_route_size(
        boundary_observations,
        now=now,
        minimum_samples=5,
        minimum_observation_period=timedelta(hours=20),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
    )
    assert boundary_assessment.parameters is not None
    assert tuple(
        bucket.sample_count
        for bucket in boundary_assessment.parameters.convergence_by_spread_bucket
    ) == (1, 1, 1, 1, 1)
    assert all(
        bucket.minimum_sample_count == 30
        for bucket in boundary_assessment.parameters.convergence_by_spread_bucket
    )
    assert boundary_assessment.parameters.convergence_p90_for_spread(levels[0]) is None
    assert boundary_assessment.parameters.convergence_p90_for_spread(levels[1]) is None


def test_inactive_change_cap_candidate_advances_safely_each_day() -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    route = DirectedRouteKey("CAP", Venue.BYBIT, Venue.OKX)

    def cost_window(at: datetime, cost: str) -> tuple[RouteCalibrationObservation, ...]:
        return tuple(
            observation(
                route,
                "1",
                at - timedelta(hours=20 - index * 5),
                "0",
                cost=cost,
            )
            for index in range(5)
        )

    baseline = calibrate_route_size(
        cost_window(now, "2"),
        now=now,
        minimum_samples=5,
        minimum_observation_period=timedelta(hours=20),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
    )
    assert baseline.parameters is not None
    assert baseline.parameters.entry_levels_bps[0] == Decimal("4.10000")

    first_day = calibrate_route_size(
        cost_window(now + timedelta(days=1), "2.5"),
        now=now + timedelta(days=1),
        minimum_samples=5,
        minimum_observation_period=timedelta(hours=20),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        previous=baseline.parameters,
    )
    assert first_day.parameters is None
    assert first_day.staged_parameters is not None
    assert first_day.staged_parameters.entry_levels_bps[0] == Decimal("4.920000")

    second_day = calibrate_route_size(
        cost_window(now + timedelta(days=2), "2.5"),
        now=now + timedelta(days=2),
        minimum_samples=5,
        minimum_observation_period=timedelta(hours=20),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        previous=first_day.staged_parameters,
    )
    assert second_day.parameters is not None
    assert second_day.parameters.entry_levels_bps[0] == Decimal("5.10000")


def test_regime_funding_depth_and_incomplete_episode_gates_fail_closed() -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    route = DirectedRouteKey("ETH", Venue.BYBIT, Venue.OKX)
    old = tuple(
        observation(
            route,
            "0.1",
            now - timedelta(hours=48 - index * 6),
            "10",
        )
        for index in range(5)
    )
    current = qualified_window(route, "0.1", now, shift=Decimal(90))
    shifted = calibrate_route_size(
        (*old, *current),
        now=now,
        minimum_samples=5,
        minimum_observation_period=timedelta(hours=20),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
    )
    assert shifted.reason == ReasonCode.CALIBRATION_REGIME_SHIFT

    no_funding = (
        *qualified_window(route, "0.1", now)[:-1],
        observation(route, "0.1", now, "14", funding=None),
    )
    assert (
        calibrate_route_size(
            no_funding,
            now=now,
            minimum_samples=5,
            minimum_observation_period=timedelta(hours=20),
            minimum_profit_usdt=Decimal("0.01"),
            parameter_change_limit_ratio_per_day=Decimal("0.20"),
        ).reason
        == ReasonCode.FUNDING_UNKNOWN
    )

    shallow = (
        *qualified_window(route, "0.1", now)[:-1],
        observation(route, "0.1", now, "14", depth="2.99"),
    )
    assert (
        calibrate_route_size(
            shallow,
            now=now,
            minimum_samples=5,
            minimum_observation_period=timedelta(hours=20),
            minimum_profit_usdt=Decimal("0.01"),
            parameter_change_limit_ratio_per_day=Decimal("0.20"),
        ).reason
        == ReasonCode.DEPTH_INSUFFICIENT
    )

    incomplete = tuple(
        observation(route, "0.1", item.observed_at, str(item.spread_bps), adverse=None)
        for item in qualified_window(route, "0.1", now)
    )
    assert (
        calibrate_route_size(
            incomplete,
            now=now,
            minimum_samples=5,
            minimum_observation_period=timedelta(hours=20),
            minimum_profit_usdt=Decimal("0.01"),
            parameter_change_limit_ratio_per_day=Decimal("0.20"),
        ).reason
        == ReasonCode.CALIBRATION_INSUFFICIENT
    )


def test_incomplete_ready_history_cannot_satisfy_sample_gate() -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    route = DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX)
    incomplete = tuple(
        observation(
            route,
            "1",
            now - timedelta(hours=20 - index * 5),
            str(10 + index),
            funding=None,
        )
        for index in range(4)
    )
    latest = observation(route, "1", now, "14")

    assessment = calibrate_route_size(
        (*incomplete, latest),
        now=now,
        minimum_samples=3,
        minimum_observation_period=timedelta(0),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
    )

    assert assessment.ready is False
    assert assessment.reason == ReasonCode.CALIBRATION_INSUFFICIENT
    assert assessment.sample_count == 1


@pytest.mark.asyncio
async def test_persistent_calibrator_restores_identical_parameters_and_bounds_rows(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    route = DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX)
    path = tmp_path / "state.sqlite3"
    first = PersistentRouteCalibrator(
        path,
        minimum_samples=5,
        minimum_observation_period=timedelta(hours=20),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        maximum_inter_observation_gap=timedelta(hours=6),
        maximum_observations_per_key=5,
    )
    await first.initialise()
    calibration_window = tuple(
        replace(item, episode_entry_spread_bps=Decimal(entry))
        for item, entry in zip(
            qualified_window(route, "0.01", now),
            ("16", "20", "24", "28", "32"),
            strict=True,
        )
    )
    assessments = await first.record_many(calibration_window, now=now)
    assert assessments[0].ready is True
    stored = await first.latest(route, Decimal("0.01"))
    assert stored == assessments[0].parameters
    assert stored is not None
    assert tuple(item.sample_count for item in stored.convergence_by_spread_bucket) == (
        1,
        1,
        1,
        1,
        1,
    )

    restarted = PersistentRouteCalibrator(
        path,
        minimum_samples=5,
        minimum_observation_period=timedelta(hours=20),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        maximum_inter_observation_gap=timedelta(hours=6),
        maximum_observations_per_key=5,
    )
    await restarted.initialise()
    assert await restarted.latest(route, Decimal("0.01")) == stored

    extra = tuple(
        observation(route, "0.01", now + timedelta(minutes=index + 1), str(15 + index))
        for index in range(3)
    )
    await restarted.record_many(extra, now=now + timedelta(minutes=3))
    with sqlite3.connect(path) as database:
        assert database.execute(
            "SELECT count(*) FROM route_calibration_observations WHERE route = ?",
            (route.value,),
        ).fetchone() == (5,)


@pytest.mark.asyncio
async def test_episode_tracking_survives_restart_and_quality_gap_resets_current_epoch(
    tmp_path: Path,
) -> None:
    started = datetime(2026, 8, 16, tzinfo=UTC)
    route = DirectedRouteKey("SOL", Venue.BYBIT, Venue.OKX)
    path = tmp_path / "episodes.sqlite3"

    def raw(index: int, spread: str) -> RouteCalibrationObservation:
        return observation(
            route,
            "1",
            started + timedelta(seconds=index),
            spread,
            adverse=None,
            convergence=None,
        )

    calibrator = PersistentRouteCalibrator(
        path,
        minimum_samples=5,
        minimum_observation_period=timedelta(0),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
    )
    await calibrator.initialise()
    before_restart = tuple(
        raw(index, spread)
        for index, spread in enumerate(
            ("10", "11", "12", "11", "10", "14", "16", "10", "15", "17", "10", "16", "18")
        )
    )
    assert (await calibrator.record_many(before_restart, now=started + timedelta(seconds=12)))[
        0
    ].reason == ReasonCode.CALIBRATION_INSUFFICIENT

    restarted = PersistentRouteCalibrator(
        path,
        minimum_samples=5,
        minimum_observation_period=timedelta(0),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
    )
    await restarted.initialise()
    completed = await restarted.record_many(
        (raw(13, "10"),),
        now=started + timedelta(seconds=13),
    )
    assert completed[0].ready is True
    assert completed[0].parameters is not None
    assert completed[0].parameters.q75_adverse_excursion_bps == Decimal(2)
    assert completed[0].parameters.convergence_p90_seconds == Decimal(2)
    with sqlite3.connect(path) as database:
        episode_observations = tuple(
            route_calibration_module._observation_from_payload(str(row[0]))
            for row in database.execute(
                "SELECT payload_json FROM route_calibration_observations WHERE route = ?",
                (route.value,),
            )
        )
    assert sum(item.episode_entry_spread_bps is not None for item in episode_observations) == 3

    gap = observation(
        route,
        "1",
        started + timedelta(seconds=14),
        "0",
        reason=ReasonCode.BOOK_SEQUENCE_GAP,
    )
    assert (await restarted.record_many((gap,), now=gap.observed_at))[0].reason == (
        ReasonCode.BOOK_SEQUENCE_GAP
    )
    after_gap = tuple(raw(15 + index, str(10 + index)) for index in range(3))
    reset = await restarted.record_many(after_gap, now=started + timedelta(seconds=17))
    assert reset[0].reason == ReasonCode.CALIBRATION_INSUFFICIENT
    assert reset[0].sample_count == 3

    regressed = observation(
        route,
        "1",
        started + timedelta(seconds=14, milliseconds=500),
        "12",
        adverse=None,
        convergence=None,
    )
    with pytest.raises(ValueError, match="cannot regress"):
        await restarted.record_many((regressed,), now=started + timedelta(seconds=18))


@pytest.mark.asyncio
async def test_non_converging_episode_times_out_and_invalidates_current_parameters(
    tmp_path: Path,
) -> None:
    started = datetime(2026, 8, 16, tzinfo=UTC)
    route = DirectedRouteKey("TIMEOUT", Venue.BYBIT, Venue.OKX)
    path = tmp_path / "episode-timeout.sqlite3"
    calibrator = PersistentRouteCalibrator(
        path,
        minimum_samples=3,
        minimum_observation_period=timedelta(0),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        maximum_inter_observation_gap=timedelta(seconds=20),
        sampling_policy=RouteCalibrationSamplingPolicy(
            (Decimal(1),),
            10,
            60,
            Decimal(2),
            Decimal(0),
            Decimal(0),
            Decimal(0),
            Decimal(0),
        ),
    )
    await calibrator.initialise()
    baseline = tuple(
        observation(
            route,
            "1",
            started + timedelta(seconds=index),
            str(10 + index),
            adverse="1",
            convergence="5",
        )
        for index in range(3)
    )
    ready = (await calibrator.record_many(baseline, now=baseline[-1].observed_at))[0]
    assert ready.ready is True
    opened = observation(
        route,
        "1",
        started + timedelta(seconds=3),
        "20",
        adverse=None,
        convergence=None,
    )
    await calibrator.record_many((opened,), now=opened.observed_at)
    peak = observation(
        route,
        "1",
        started + timedelta(seconds=4),
        "24",
        adverse=None,
        convergence=None,
    )
    await calibrator.record_many((peak,), now=peak.observed_at)
    timed_out = observation(
        route,
        "1",
        started + timedelta(seconds=14),
        "18",
        adverse=None,
        convergence=None,
    )

    current_gate = (await calibrator.assess_current((timed_out,)))[0]

    assessment = (await calibrator.record_many((timed_out,), now=timed_out.observed_at))[0]

    assert current_gate.reason == ReasonCode.CALIBRATION_REGIME_SHIFT
    assert current_gate.ready is False
    assert assessment.reason == ReasonCode.CALIBRATION_REGIME_SHIFT
    assert assessment.ready is False
    assert await calibrator.latest(route, Decimal(1)) is None
    with sqlite3.connect(path) as database:
        assert database.execute(
            "SELECT count(*) FROM route_calibration_episodes WHERE route = ?",
            (route.value,),
        ).fetchone() == (0,)
        payload = database.execute(
            """
            SELECT payload_json FROM route_calibration_observations
            WHERE route = ? ORDER BY observed_at DESC LIMIT 1
            """,
            (route.value,),
        ).fetchone()
    assert payload is not None
    recorded = route_calibration_module._observation_from_payload(str(payload[0]))
    assert recorded.convergence_seconds == Decimal(11)
    assert recorded.episode_peak_spread_bps == Decimal(24)

    recovered_observations = tuple(
        observation(
            route,
            "1",
            started + timedelta(seconds=15 + index),
            str(10 + index),
            adverse="1",
            convergence="5",
        )
        for index in range(3)
    )
    recovered = (
        await calibrator.record_many(
            recovered_observations,
            now=recovered_observations[-1].observed_at,
        )
    )[0]
    assert recovered.parameters is not None
    assert recovered.parameters.long_tail_q999_spread_bps >= Decimal(24)
    assert recovered.parameters.convergence_p90_seconds > Decimal(5)


@pytest.mark.asyncio
async def test_retention_is_bounded_across_restart_epochs(tmp_path: Path) -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    route = DirectedRouteKey("XRP", Venue.BYBIT, Venue.OKX)
    path = tmp_path / "epochs.sqlite3"
    calibrator = PersistentRouteCalibrator(
        path,
        minimum_samples=3,
        minimum_observation_period=timedelta(0),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        maximum_inter_observation_gap=timedelta(seconds=1),
        maximum_observations_per_key=5,
    )
    await calibrator.initialise()
    for epoch_index in range(4):
        epoch_time = now + timedelta(seconds=epoch_index * 10)
        epoch = await calibrator.current_epoch_id(epoch_time)
        values = tuple(
            observation(
                route,
                "1",
                now + timedelta(seconds=epoch_index * 10 + sample_index),
                str(10 + sample_index),
                epoch=epoch,
                adverse=str(sample_index + 1),
                convergence=str(sample_index + 1),
            )
            for sample_index in range(3)
        )
        await calibrator.record_many(values, now=values[-1].observed_at)

    with sqlite3.connect(path) as database:
        assert database.execute(
            "SELECT count(*) FROM route_calibration_observations WHERE route = ?",
            (route.value,),
        ).fetchone() == (5,)
        assert database.execute(
            "SELECT count(*) FROM route_calibration_episodes WHERE route = ?",
            (route.value,),
        ).fetchone() == (0,)


@pytest.mark.asyncio
async def test_large_universe_calibrates_all_directions_and_three_size_buckets(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    calibrator = PersistentRouteCalibrator(
        tmp_path / "large.sqlite3",
        minimum_samples=3,
        minimum_observation_period=timedelta(0),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        maximum_observations_per_key=3,
    )
    await calibrator.initialise()
    observations: list[RouteCalibrationObservation] = []
    for base_index in range(100):
        for route in (
            DirectedRouteKey(f"A{base_index:03d}", Venue.BYBIT, Venue.OKX),
            DirectedRouteKey(f"A{base_index:03d}", Venue.OKX, Venue.BYBIT),
        ):
            for multiplier in ("1", "2", "5"):
                observations.extend(
                    observation(
                        route,
                        multiplier,
                        now - timedelta(seconds=2 - sample_index),
                        str(10 + sample_index),
                        adverse=str(sample_index + 1),
                        convergence=str(10 + sample_index),
                        base_quantity=str(Decimal("0.01") * Decimal(multiplier)),
                    )
                    for sample_index in range(3)
                )

    assessments = await calibrator.record_many(tuple(observations), now=now)

    assert len(assessments) == 600
    assert all(assessment.ready for assessment in assessments)
    assert len({assessment.route.base for assessment in assessments}) == 100
    assert all(assessment.execution_authorized is False for assessment in assessments)


@pytest.mark.asyncio
async def test_incremental_reuse_is_bounded_and_keeps_locked_safety_envelopes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    path = tmp_path / "bounded-incremental.sqlite3"
    calibrator = PersistentRouteCalibrator(
        path,
        minimum_samples=3,
        minimum_observation_period=timedelta(0),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        maximum_observations_per_key=20_000,
    )
    await calibrator.initialise()
    routes = tuple(DirectedRouteKey(f"P{index:02d}", Venue.BYBIT, Venue.OKX) for index in range(10))
    initial = tuple(
        observation(
            route,
            "1",
            now - timedelta(seconds=2 - sample),
            str(10 + sample),
        )
        for route in routes
        for sample in range(3)
    )
    first = await calibrator.record_many(initial, now=now)
    assert all(item.ready for item in first)

    with sqlite3.connect(path) as database:
        rows: list[tuple[str, str, str, str, str, str]] = []
        for route in routes:
            for sample in range(500):
                item = observation(
                    route,
                    "1",
                    now + timedelta(microseconds=sample + 1),
                    "11",
                )
                rows.append(
                    (
                        route.value,
                        "1",
                        "epoch-1",
                        item.observed_at.isoformat(),
                        item.reason.value,
                        route_calibration_module._observation_payload(item),
                    )
                )
        database.executemany(
            """
            INSERT INTO route_calibration_observations(
                route, size_bucket_multiplier, epoch_id, observed_at, reason, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        database.commit()

    decoded = 0
    original_decode = route_calibration_module._observation_from_payload

    def counted_decode(payload: str) -> RouteCalibrationObservation:
        nonlocal decoded
        decoded += 1
        return original_decode(payload)

    monkeypatch.setattr(route_calibration_module, "_observation_from_payload", counted_decode)
    next_observations = tuple(
        observation(route, "1", now + timedelta(seconds=1), "11") for route in routes
    )
    second = await calibrator.record_many(next_observations, now=now + timedelta(seconds=1))

    assert all(item.ready for item in second)
    assert decoded <= len(routes) * 16
    deepest = first[0].parameters
    assert deepest is not None
    normal_grid_level = observation(
        routes[0],
        "1",
        now + timedelta(seconds=2),
        str(deepest.entry_levels_bps[4]),
    )
    assert (await calibrator.record_many((normal_grid_level,), now=normal_grid_level.observed_at))[
        0
    ].ready
    unsafe_cost = observation(
        routes[0],
        "1",
        now + timedelta(seconds=3),
        "11",
        cost="20",
    )
    rejected = (await calibrator.record_many((unsafe_cost,), now=unsafe_cost.observed_at))[0]
    assert rejected.reason == ReasonCode.CALIBRATION_REGIME_SHIFT
    assert rejected.ready is False
    assert await calibrator.latest(routes[0], Decimal(1)) is None
    await calibrator.close()
    calibrator = PersistentRouteCalibrator(
        path,
        minimum_samples=3,
        minimum_observation_period=timedelta(0),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        maximum_observations_per_key=20_000,
    )
    await calibrator.initialise()
    assert await calibrator.latest(routes[0], Decimal(1)) is None
    recovered_at = now + timedelta(seconds=4)
    recovered = (
        await calibrator.record_many(
            (observation(routes[0], "1", recovered_at, "11"),),
            now=recovered_at,
        )
    )[0]
    assert recovered.ready is True
    assert await calibrator.latest(routes[0], Decimal(1)) is not None


@pytest.mark.asyncio
async def test_prolonged_transient_cost_gate_requires_due_full_rebuild(tmp_path: Path) -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    route = DirectedRouteKey("TRANSIENT", Venue.BYBIT, Venue.OKX)
    calibrator = PersistentRouteCalibrator(
        tmp_path / "transient-due.sqlite3",
        minimum_samples=3,
        minimum_observation_period=timedelta(0),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
    )
    await calibrator.initialise()
    baseline = tuple(
        observation(route, "1", now - timedelta(seconds=2 - index), "0", cost="2")
        for index in range(3)
    )
    initial = (await calibrator.record_many(baseline, now=now))[0]
    assert initial.ready is True

    for seconds in (1, 30):
        high = observation(route, "1", now + timedelta(seconds=seconds), "0", cost="2.5")
        rejected = (await calibrator.record_many((high,), now=high.observed_at))[0]
        assert rejected.reason == ReasonCode.CALIBRATION_REGIME_SHIFT
        assert rejected.staged_parameters is None
    due = observation(route, "1", now + timedelta(seconds=60), "0", cost="2.5")
    rebuilt = (await calibrator.record_many((due,), now=due.observed_at))[0]
    assert rebuilt.reason == ReasonCode.CALIBRATION_REGIME_SHIFT
    assert rebuilt.staged_parameters is not None
    assert await calibrator.latest(route, Decimal(1)) is None

    safe = observation(route, "1", now + timedelta(seconds=61), "0", cost="2")
    still_blocked = (await calibrator.record_many((safe,), now=safe.observed_at))[0]
    assert still_blocked.ready is False
    assert await calibrator.latest(route, Decimal(1)) is None


@pytest.mark.asyncio
async def test_due_rebuild_fairness_is_independent_of_batch_order(tmp_path: Path) -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    calibrator = PersistentRouteCalibrator(
        tmp_path / "fair-rebuild.sqlite3",
        minimum_samples=3,
        minimum_observation_period=timedelta(0),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        maximum_inter_observation_gap=timedelta(minutes=2),
    )
    await calibrator.initialise()
    routes = tuple(DirectedRouteKey(f"F{index}", Venue.BYBIT, Venue.OKX) for index in range(6))
    initial = tuple(
        observation(route, "1", now - timedelta(seconds=2 - sample), str(10 + sample))
        for route in routes
        for sample in range(3)
    )
    assert all(item.ready for item in await calibrator.record_many(initial, now=now))

    first_due_at = now + timedelta(seconds=61)
    first_due = tuple(observation(route, "1", first_due_at, "11") for route in routes)
    assert all(item.ready for item in await calibrator.record_many(first_due, now=first_due_at))
    second_due_at = first_due_at + timedelta(seconds=1)
    reordered = tuple(observation(route, "1", second_due_at, "11") for route in reversed(routes))
    assert all(item.ready for item in await calibrator.record_many(reordered, now=second_due_at))

    latest = [await calibrator.latest(route, Decimal(1)) for route in routes]
    assert all(item is not None and item.calibrated_at >= first_due_at for item in latest)


@pytest.mark.asyncio
async def test_fresh_candidate_churn_cannot_starve_due_persisted_keys(tmp_path: Path) -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    calibrator = PersistentRouteCalibrator(
        tmp_path / "churn-fairness.sqlite3",
        minimum_samples=3,
        minimum_observation_period=timedelta(0),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        maximum_inter_observation_gap=timedelta(minutes=2),
    )
    await calibrator.initialise()
    old_routes = tuple(DirectedRouteKey(f"Z{index}", Venue.BYBIT, Venue.OKX) for index in range(6))
    initial = tuple(
        observation(route, "1", now - timedelta(seconds=2 - sample), str(10 + sample))
        for route in old_routes
        for sample in range(3)
    )
    assert all(item.ready for item in await calibrator.record_many(initial, now=now))

    for cycle in range(2):
        observed_at = now + timedelta(seconds=61 + cycle)
        fresh_routes = tuple(
            DirectedRouteKey(f"A{cycle}{index}", Venue.BYBIT, Venue.OKX) for index in range(3)
        )
        old = tuple(observation(route, "1", observed_at, "11") for route in old_routes)
        fresh = tuple(
            observation(
                route,
                "1",
                observed_at - timedelta(microseconds=2 - sample),
                str(10 + sample),
            )
            for route in fresh_routes
            for sample in range(3)
        )
        batch = old + fresh if cycle == 0 else fresh + old
        assert all(item.ready for item in await calibrator.record_many(batch, now=observed_at))

    latest = [await calibrator.latest(route, Decimal(1)) for route in old_routes]
    assert all(
        item is not None and item.calibrated_at >= now + timedelta(seconds=61) for item in latest
    )


def test_window_coverage_is_truthful_at_24h_7d_and_30d_boundaries() -> None:
    route = DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX)
    started = datetime(2026, 7, 17, tzinfo=UTC)
    all_values = tuple(
        observation(
            route,
            "1",
            started + timedelta(hours=index),
            "10",
            adverse="2",
            convergence="30",
            base_quantity="0.01",
        )
        for index in range(30 * 24 + 1)
    )

    def assess(hours: int) -> RouteCalibrationAssessment:
        values = all_values[: hours + 1]
        return calibrate_route_size(
            values,
            now=values[-1].observed_at,
            minimum_samples=3,
            minimum_observation_period=timedelta(hours=24),
            minimum_profit_usdt=Decimal("0.01"),
            parameter_change_limit_ratio_per_day=Decimal("0.20"),
        )

    at_24h = assess(24)
    at_7d = assess(7 * 24)
    at_30d = assess(30 * 24)
    assert at_24h.parameters is not None
    assert at_24h.parameters.window_24h.complete is True
    assert at_24h.parameters.window_7d.complete is False
    assert at_24h.parameters.window_30d.complete is False
    assert at_7d.parameters is not None
    assert at_7d.parameters.window_7d.complete is True
    assert at_7d.parameters.window_30d.complete is False
    assert at_30d.parameters is not None
    assert at_30d.parameters.window_30d.complete is True


@pytest.mark.asyncio
async def test_stable_multiplier_key_survives_executable_quantity_boundary(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    route = DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX)
    path = tmp_path / "stable-size.sqlite3"
    calibrator = PersistentRouteCalibrator(
        path,
        minimum_samples=3,
        minimum_observation_period=timedelta(0),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        maximum_inter_observation_gap=timedelta(minutes=5),
    )
    await calibrator.initialise()
    initial = tuple(
        observation(
            route,
            "1",
            now + timedelta(seconds=index),
            str(10 + index),
            base_quantity="0.05",
        )
        for index in range(3)
    )
    assert (await calibrator.record_many(initial, now=initial[-1].observed_at))[0].ready
    crossed = observation(
        route,
        "1",
        now + timedelta(seconds=3),
        "13",
        base_quantity="0.06",
    )
    assessment = (await calibrator.record_many((crossed,), now=crossed.observed_at))[0]
    assert assessment.ready is True
    assert assessment.latest_base_quantity == Decimal("0.06")
    assert assessment.size_bucket_multiplier == Decimal(1)
    with sqlite3.connect(path) as database:
        assert database.execute(
            """
            SELECT count(DISTINCT size_bucket_multiplier)
            FROM route_calibration_observations WHERE route = ?
            """,
            (route.value,),
        ).fetchone() == (1,)


@pytest.mark.asyncio
async def test_persisted_epoch_resumes_only_within_policy_gap(tmp_path: Path) -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    path = tmp_path / "epoch.sqlite3"

    def service() -> PersistentRouteCalibrator:
        return PersistentRouteCalibrator(
            path,
            minimum_samples=3,
            minimum_observation_period=timedelta(0),
            minimum_profit_usdt=Decimal("0.01"),
            parameter_change_limit_ratio_per_day=Decimal("0.20"),
            maximum_inter_observation_gap=timedelta(seconds=10),
        )

    first = service()
    await first.initialise()
    epoch = await first.current_epoch_id(now)
    values = tuple(
        observation(
            DirectedRouteKey("ETH", Venue.BYBIT, Venue.OKX),
            "1",
            now + timedelta(seconds=index),
            str(10 + index),
            epoch=epoch,
        )
        for index in range(3)
    )
    await first.record_many(values, now=values[-1].observed_at)

    restarted = service()
    await restarted.initialise()
    assert await restarted.current_epoch_id(now + timedelta(seconds=5)) == epoch
    replacement = await restarted.current_epoch_id(now + timedelta(seconds=20))
    assert replacement != epoch
    assert await restarted.latest(values[0].route, Decimal(1)) is None
    with pytest.raises(ValueError, match="epoch is not current"):
        await restarted.record_many(
            (observation(values[0].route, "1", now + timedelta(seconds=21), "12", epoch=epoch),),
            now=now + timedelta(seconds=21),
        )


@pytest.mark.asyncio
async def test_sampling_cost_policy_change_rotates_persisted_epoch(tmp_path: Path) -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    path = tmp_path / "policy-epoch.sqlite3"

    def service(latency_reserve: str) -> PersistentRouteCalibrator:
        return PersistentRouteCalibrator(
            path,
            minimum_samples=3,
            minimum_observation_period=timedelta(0),
            minimum_profit_usdt=Decimal("0.01"),
            parameter_change_limit_ratio_per_day=Decimal("0.20"),
            sampling_policy=RouteCalibrationSamplingPolicy(
                (Decimal(1), Decimal(2), Decimal(5)),
                3600,
                60,
                Decimal(2),
                Decimal(latency_reserve),
                Decimal(3),
                Decimal(10),
                Decimal(10),
            ),
            maximum_l2_age_ms=1000,
        )

    first = service("2")
    await first.initialise()
    epoch = await first.current_epoch_id(now)
    changed = service("3")
    await changed.initialise()

    assert await changed.current_epoch_id(now + timedelta(seconds=1)) != epoch


@pytest.mark.asyncio
async def test_same_timestamp_invalid_marker_revokes_ready_parameters(tmp_path: Path) -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    route = DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX)
    path = tmp_path / "same-timestamp-invalid.sqlite3"
    calibrator = PersistentRouteCalibrator(
        path,
        minimum_samples=3,
        minimum_observation_period=timedelta(0),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
    )
    await calibrator.initialise()
    values = tuple(
        observation(route, "1", now + timedelta(seconds=index), str(10 + index))
        for index in range(3)
    )
    assert (await calibrator.record_many(values, now=values[-1].observed_at))[0].ready
    assert await calibrator.latest(route, Decimal(1)) is not None

    invalid = replace(
        values[-1],
        base_quantity=None,
        spread_bps=None,
        adverse_excursion_after_entry_bps=None,
        convergence_seconds=None,
        stressed_cost_floor_bps=None,
        normalized_tick_bps=None,
        notional_usdt=None,
        funding_rate_delta=None,
        exit_depth_multiple=None,
        reason=ReasonCode.BOOK_SEQUENCE_GAP,
    )
    assessment = (await calibrator.record_many((invalid,), now=invalid.observed_at))[0]
    assert assessment.reason == ReasonCode.BOOK_SEQUENCE_GAP
    assert assessment.ready is False
    assert await calibrator.latest(route, Decimal(1)) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("rotate_epoch", [False, True])
async def test_gap_keeps_safety_anchor_but_never_exposes_stale_parameters(
    tmp_path: Path,
    rotate_epoch: bool,
) -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    route = DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX)
    path = tmp_path / f"anchor-{rotate_epoch}.sqlite3"
    calibrator = PersistentRouteCalibrator(
        path,
        minimum_samples=3,
        minimum_observation_period=timedelta(0),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        maximum_inter_observation_gap=timedelta(seconds=10),
    )
    await calibrator.initialise()
    epoch = await calibrator.current_epoch_id(now)
    high_spreads = ("800", "1000", "1200")
    high = tuple(
        observation(
            route,
            "1",
            now + timedelta(seconds=index),
            high_spreads[index],
            epoch=epoch,
        )
        for index in range(3)
    )
    initial = (await calibrator.record_many(high, now=high[-1].observed_at))[0]
    assert initial.parameters is not None
    assert await calibrator.latest(route, Decimal(1)) is not None

    if rotate_epoch:
        low_started = now + timedelta(seconds=20)
        next_epoch = await calibrator.current_epoch_id(low_started)
        assert next_epoch != epoch
    else:
        invalid = replace(
            high[-1],
            observed_at=now + timedelta(seconds=3),
            base_quantity=None,
            spread_bps=None,
            adverse_excursion_after_entry_bps=None,
            convergence_seconds=None,
            stressed_cost_floor_bps=None,
            normalized_tick_bps=None,
            notional_usdt=None,
            funding_rate_delta=None,
            exit_depth_multiple=None,
            reason=ReasonCode.BOOK_EMPTY,
        )
        invalid_assessment = (await calibrator.record_many((invalid,), now=invalid.observed_at))[0]
        assert invalid_assessment.ready is False
        low_started = now + timedelta(seconds=4)
        next_epoch = epoch
    assert await calibrator.latest(route, Decimal(1)) is None

    low_spreads = ("600", "750", "900")
    low = tuple(
        observation(
            route,
            "1",
            low_started + timedelta(seconds=index),
            low_spreads[index],
            epoch=next_epoch,
        )
        for index in range(3)
    )
    recovered = (await calibrator.record_many(low, now=low[-1].observed_at))[0]
    assert recovered.parameters is not None
    assert recovered.parameters.entry_levels_bps[0] >= (
        initial.parameters.entry_levels_bps[0] * Decimal("0.80")
    )
    assert (
        recovered.parameters.long_tail_q999_spread_bps
        >= initial.parameters.long_tail_q999_spread_bps
    )
    assert await calibrator.latest(route, Decimal(1)) == recovered.parameters


@pytest.mark.asyncio
async def test_rate_limit_never_bypasses_regime_shift_gate(tmp_path: Path) -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    route = DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX)
    path = tmp_path / "regime-rate-limit.sqlite3"
    calibrator = PersistentRouteCalibrator(
        path,
        minimum_samples=5,
        minimum_observation_period=timedelta(hours=20),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        maximum_inter_observation_gap=timedelta(hours=12),
    )
    await calibrator.initialise()
    epoch = await calibrator.current_epoch_id(now)
    stable = tuple(
        observation(
            route,
            "1",
            now - timedelta(hours=48) + timedelta(hours=6 * index),
            "10",
            epoch=epoch,
        )
        for index in range(9)
    )
    initial = (await calibrator.record_many(stable, now=now))[0]
    assert initial.ready is True

    shock = observation(
        route,
        "1",
        now + timedelta(seconds=5),
        "1000",
        epoch=epoch,
    )
    shifted = (await calibrator.record_many((shock,), now=shock.observed_at))[0]

    assert shifted.reason == ReasonCode.CALIBRATION_REGIME_SHIFT
    assert shifted.ready is False
    assert await calibrator.latest(route, Decimal(1)) is None


@pytest.mark.asyncio
async def test_cancelled_sqlite_worker_cannot_commit_after_caller_shutdown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    path = tmp_path / "cancel.sqlite3"
    calibrator = PersistentRouteCalibrator(
        path,
        minimum_samples=3,
        minimum_observation_period=timedelta(0),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
    )
    await calibrator.initialise()
    route = DirectedRouteKey("SOL", Venue.BYBIT, Venue.OKX)
    values = tuple(
        observation(route, "1", now + timedelta(seconds=index), str(10 + index))
        for index in range(3)
    )
    started = threading.Event()
    release = threading.Event()
    original = calibrator._record_many_sync

    def delayed(
        incoming: tuple[RouteCalibrationObservation, ...],
        observed_at: datetime,
        abort: threading.Event,
    ) -> tuple[RouteCalibrationAssessment, ...]:
        started.set()
        release.wait(timeout=2)
        return original(incoming, observed_at, abort)

    monkeypatch.setattr(calibrator, "_record_many_sync", delayed)
    caller = asyncio.create_task(calibrator.record_many(values, now=values[-1].observed_at))
    assert await asyncio.to_thread(started.wait, 1)
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    release.set()
    await calibrator.close()
    with sqlite3.connect(path) as database:
        assert database.execute(
            "SELECT count(*) FROM route_calibration_observations"
        ).fetchone() == (0,)


@pytest.mark.asyncio
async def test_close_aborts_inflight_write_and_rejects_all_future_calls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    path = tmp_path / "closed.sqlite3"
    calibrator = PersistentRouteCalibrator(
        path,
        minimum_samples=3,
        minimum_observation_period=timedelta(0),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
    )
    await calibrator.initialise()
    epoch = await calibrator.current_epoch_id(now)
    route = DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX)
    item = observation(route, "1", now, "10", epoch=epoch)
    started = threading.Event()
    original = calibrator._record_many_sync

    def wait_for_abort(
        incoming: tuple[RouteCalibrationObservation, ...],
        observed_at: datetime,
        abort: threading.Event,
    ) -> tuple[RouteCalibrationAssessment, ...]:
        started.set()
        assert abort.wait(timeout=2)
        return original(incoming, observed_at, abort)

    monkeypatch.setattr(calibrator, "_record_many_sync", wait_for_abort)
    writer = asyncio.create_task(calibrator.record_many((item,), now=now))
    assert await asyncio.to_thread(started.wait, 1)
    await calibrator.close()
    with pytest.raises(RuntimeError, match="write aborted"):
        await writer

    with pytest.raises(RuntimeError, match="closed"):
        await calibrator.initialise()
    with pytest.raises(RuntimeError, match="closed"):
        await calibrator.current_epoch_id(now)
    with pytest.raises(RuntimeError, match="closed"):
        await calibrator.record_many((item,), now=now)
    with pytest.raises(RuntimeError, match="closed"):
        await calibrator.latest(route, Decimal(1))
    with sqlite3.connect(path) as database:
        assert database.execute(
            "SELECT count(*) FROM route_calibration_observations"
        ).fetchone() == (0,)


@pytest.mark.asyncio
async def test_epoch_is_single_flight_and_global_watermark_cannot_regress(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 16, tzinfo=UTC)
    path = tmp_path / "epoch-order.sqlite3"
    calibrator = PersistentRouteCalibrator(
        path,
        minimum_samples=3,
        minimum_observation_period=timedelta(0),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        maximum_inter_observation_gap=timedelta(seconds=30),
    )
    await calibrator.initialise()
    concurrent = await asyncio.gather(*(calibrator.current_epoch_id(now) for _ in range(20)))
    assert len(set(concurrent)) == 1
    epoch = concurrent[0]
    first = observation(
        DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX),
        "1",
        now + timedelta(seconds=100),
        "10",
        epoch=epoch,
    )
    older_other_route = observation(
        DirectedRouteKey("ETH", Venue.BYBIT, Venue.OKX),
        "1",
        now + timedelta(seconds=50),
        "10",
        epoch=epoch,
    )
    await calibrator.record_many((first,), now=first.observed_at)
    await calibrator.record_many((older_other_route,), now=older_other_route.observed_at)

    assert await calibrator.current_epoch_id(now + timedelta(seconds=120)) == epoch


@pytest.mark.asyncio
async def test_aware_offsets_are_normalized_before_global_watermark_comparison(
    tmp_path: Path,
) -> None:
    utc_time = datetime(2026, 8, 16, 10, tzinfo=UTC)
    plus_three = timezone(timedelta(hours=3))
    path = tmp_path / "offset-watermark.sqlite3"
    calibrator = PersistentRouteCalibrator(
        path,
        minimum_samples=3,
        minimum_observation_period=timedelta(0),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        maximum_inter_observation_gap=timedelta(hours=1),
    )
    await calibrator.initialise()
    epoch = await calibrator.current_epoch_id(utc_time)
    first = observation(
        DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX),
        "1",
        datetime(2026, 8, 16, 13, tzinfo=plus_three),
        "10",
        epoch=epoch,
    )
    second = observation(
        DirectedRouteKey("ETH", Venue.BYBIT, Venue.OKX),
        "1",
        utc_time + timedelta(minutes=30),
        "10",
        epoch=epoch,
    )
    await calibrator.record_many((first,), now=first.observed_at)
    await calibrator.record_many((second,), now=second.observed_at)

    assert await calibrator.current_epoch_id(utc_time + timedelta(minutes=70)) == epoch


@pytest.mark.asyncio
async def test_global_retention_bounds_retired_route_size_keys_across_restart(
    tmp_path: Path,
) -> None:
    started = datetime(2026, 7, 1, tzinfo=UTC)
    path = tmp_path / "global-retention.sqlite3"

    def service() -> PersistentRouteCalibrator:
        return PersistentRouteCalibrator(
            path,
            minimum_samples=3,
            minimum_observation_period=timedelta(0),
            minimum_profit_usdt=Decimal("0.01"),
            parameter_change_limit_ratio_per_day=Decimal("0.20"),
            maximum_inter_observation_gap=timedelta(minutes=5),
            maximum_route_size_keys=3,
        )

    calibrator = service()
    await calibrator.initialise()
    epoch = await calibrator.current_epoch_id(started)
    for index in range(5):
        route = DirectedRouteKey(f"R{index}", Venue.BYBIT, Venue.OKX)
        base_time = started + timedelta(seconds=index * 10)
        ready = tuple(
            observation(
                route,
                "1",
                base_time + timedelta(seconds=sample),
                str(10 + sample),
                epoch=epoch,
            )
            for sample in range(3)
        )
        assert (await calibrator.record_many(ready, now=ready[-1].observed_at))[0].ready
        invalid = replace(
            ready[-1],
            observed_at=base_time + timedelta(seconds=3),
            base_quantity=None,
            spread_bps=None,
            adverse_excursion_after_entry_bps=None,
            convergence_seconds=None,
            stressed_cost_floor_bps=None,
            normalized_tick_bps=None,
            notional_usdt=None,
            funding_rate_delta=None,
            exit_depth_multiple=None,
            reason=ReasonCode.BOOK_EMPTY,
        )
        await calibrator.record_many((invalid,), now=invalid.observed_at)

    restarted = service()
    await restarted.initialise()
    epoch = await restarted.current_epoch_id(started + timedelta(seconds=55))
    newest = observation(
        DirectedRouteKey("NEW", Venue.BYBIT, Venue.OKX),
        "1",
        started + timedelta(seconds=56),
        "10",
        epoch=epoch,
    )
    await restarted.record_many((newest,), now=newest.observed_at)
    with sqlite3.connect(path) as database:
        for table in (
            "route_calibration_observations",
            "route_calibration_parameters",
            "route_calibration_segments",
            "route_calibration_episodes",
        ):
            assert (
                database.execute(
                    f"SELECT count(DISTINCT route || ':' || size_bucket_multiplier) FROM {table}"
                ).fetchone()[0]
                <= 3
            )


@pytest.mark.asyncio
async def test_global_retention_prunes_unrelated_rows_older_than_thirty_days(
    tmp_path: Path,
) -> None:
    started = datetime(2026, 7, 1, tzinfo=UTC)
    path = tmp_path / "age-retention.sqlite3"
    calibrator = PersistentRouteCalibrator(
        path,
        minimum_samples=3,
        minimum_observation_period=timedelta(0),
        minimum_profit_usdt=Decimal("0.01"),
        parameter_change_limit_ratio_per_day=Decimal("0.20"),
        maximum_inter_observation_gap=timedelta(minutes=5),
    )
    await calibrator.initialise()
    old_epoch = await calibrator.current_epoch_id(started)
    old = observation(
        DirectedRouteKey("OLD", Venue.BYBIT, Venue.OKX),
        "1",
        started,
        "10",
        epoch=old_epoch,
    )
    await calibrator.record_many((old,), now=started)
    later = started + timedelta(days=31)
    new_epoch = await calibrator.current_epoch_id(later)
    new = observation(
        DirectedRouteKey("NEW", Venue.BYBIT, Venue.OKX),
        "1",
        later,
        "10",
        epoch=new_epoch,
    )
    await calibrator.record_many((new,), now=later)

    with sqlite3.connect(path) as database:
        assert database.execute(
            "SELECT DISTINCT route FROM route_calibration_observations"
        ).fetchall() == [(new.route.value,)]
