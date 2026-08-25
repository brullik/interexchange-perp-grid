from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import ROUND_FLOOR, ROUND_HALF_EVEN, Decimal
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from typing import cast
from uuid import uuid4

import yaml

from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.reference_history import (
    ReferenceMinuteRejection,
    ReferenceSpreadBar,
    directed_routes_for_reference_pair,
    reference_bars_sha256,
)
from interexchange_perp_grid.strategy import DirectedRouteKey

_ONE = Decimal("1")
_ROBUST_SIGMA_FACTOR = Decimal("1.4826")
_LOCKED_LEVEL_FRACTIONS = (
    Decimal("0.20"),
    Decimal("0.40"),
    Decimal("0.60"),
    Decimal("0.80"),
    Decimal("1.00"),
)
_LOCKED_TRANCHE_WEIGHTS = (
    Decimal("0.10"),
    Decimal("0.15"),
    Decimal("0.20"),
    Decimal("0.25"),
    Decimal("0.30"),
)


class DivergenceDirection(StrEnum):
    POSITIVE = "POSITIVE"
    NEGATIVE = "NEGATIVE"


class ModelEligibility(StrEnum):
    DISABLED = "DISABLED"
    SHADOW_ONLY = "SHADOW_ONLY"
    LIVE_ELIGIBLE = "LIVE_ELIGIBLE"


class EpisodeCloseReason(StrEnum):
    NORMAL_RETURN = "NORMAL_RETURN"
    HORIZON = "HORIZON"
    DATA_UNAVAILABLE = "DATA_UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class HistoricalModelPolicy:
    history_target_days: Decimal = Decimal("180")
    history_minimum_live_days: Decimal = Decimal("90")
    history_minimum_shadow_days: Decimal = Decimal("30")
    mode_bucket_bps: Decimal = Decimal("1")
    normal_zone_minimum_half_width_bps: Decimal = Decimal("2")
    minimum_completed_episodes: int = 10
    minimum_convergence_rate: Decimal = Decimal("0.70")
    convergence_horizon_seconds: int = 86_400
    regime_drift_range_fraction: Decimal = Decimal("0.25")
    regime_drift_robust_sigma_multiple: Decimal = Decimal("3")
    parameter_change_limit_ratio_per_day: Decimal = Decimal("0.20")
    level_fractions: tuple[Decimal, ...] = _LOCKED_LEVEL_FRACTIONS
    tranche_weights: tuple[Decimal, ...] = _LOCKED_TRANCHE_WEIGHTS
    stop_buffer_ratio: Decimal = Decimal("0.15")
    rearm_retreat_step_fraction: Decimal = Decimal("0.25")

    def __post_init__(self) -> None:
        decimal_values = (
            self.history_target_days,
            self.history_minimum_live_days,
            self.history_minimum_shadow_days,
            self.mode_bucket_bps,
            self.normal_zone_minimum_half_width_bps,
            self.minimum_convergence_rate,
            self.regime_drift_range_fraction,
            self.regime_drift_robust_sigma_multiple,
            self.parameter_change_limit_ratio_per_day,
            *self.level_fractions,
            *self.tranche_weights,
            self.stop_buffer_ratio,
            self.rearm_retreat_step_fraction,
        )
        if any(not value.is_finite() for value in decimal_values):
            raise ValueError("historical model policy values must be finite")
        if not (
            self.history_target_days
            >= self.history_minimum_live_days
            > self.history_minimum_shadow_days
            > 0
        ):
            raise ValueError("historical coverage thresholds are invalid")
        if self.mode_bucket_bps <= 0 or self.normal_zone_minimum_half_width_bps <= 0:
            raise ValueError("mode bucket and normal-zone width must be positive")
        if self.minimum_completed_episodes <= 0:
            raise ValueError("minimum completed episodes must be positive")
        if not 0 <= self.minimum_convergence_rate <= 1:
            raise ValueError("minimum convergence rate must be within [0, 1]")
        if self.convergence_horizon_seconds <= 0:
            raise ValueError("convergence horizon must be positive")
        if self.level_fractions != _LOCKED_LEVEL_FRACTIONS:
            raise ValueError("level fractions must remain locked at 20/40/60/80/100 percent")
        if self.tranche_weights != _LOCKED_TRANCHE_WEIGHTS:
            raise ValueError("tranche weights must remain locked at 10/15/20/25/30 percent")
        if self.stop_buffer_ratio != Decimal("0.15"):
            raise ValueError("stop buffer must remain locked at 0.15")
        if self.rearm_retreat_step_fraction != Decimal("0.25"):
            raise ValueError("rearm retreat fraction must remain locked at 0.25")


@dataclass(frozen=True, slots=True)
class LoadedHistoricalModelPolicy:
    policy: HistoricalModelPolicy
    profile_sha256: str


@dataclass(frozen=True, slots=True)
class RobustWindow:
    minutes: int
    sample_count: int
    complete: bool
    median_bps: Decimal | None
    mad_bps: Decimal | None
    q10_bps: Decimal | None
    q90_bps: Decimal | None
    q999_abs_bps: Decimal | None


@dataclass(frozen=True, slots=True)
class LevelEpisodeSample:
    level_index: int
    level_bps: Decimal
    reached_at: datetime
    ended_at: datetime
    convergence_seconds: int
    adverse_excursion_bps: Decimal
    censored: bool
    close_reason: EpisodeCloseReason


@dataclass(frozen=True, slots=True)
class HistoricalEpisode:
    direction: DivergenceDirection
    started_at: datetime
    ended_at: datetime
    converged: bool
    close_reason: EpisodeCloseReason
    level_samples: tuple[LevelEpisodeSample, ...]


@dataclass(frozen=True, slots=True)
class DirectionHistoricalModel:
    direction: DivergenceDirection
    extreme_bps: Decimal
    range_bps: Decimal
    levels_bps: tuple[Decimal, ...]
    tranche_weights: tuple[Decimal, ...]
    reference_stop_bps: Decimal
    episodes: tuple[HistoricalEpisode, ...]
    per_level_samples: tuple[tuple[LevelEpisodeSample, ...], ...]
    convergence_rate: Decimal
    regime_drift_blocked: bool
    eligibility: ModelEligibility


@dataclass(frozen=True, slots=True)
class HistoricalReferenceModel:
    schema_version: int
    algorithm_version: str
    window_start: datetime
    window_end: datetime
    coverage_days: Decimal
    target_coverage_met: bool
    s0_bps: Decimal
    normal_half_width_bps: Decimal
    normal_low_bps: Decimal
    normal_high_bps: Decimal
    positive: DirectionHistoricalModel
    negative: DirectionHistoricalModel
    window_24h: RobustWindow
    window_7d: RobustWindow
    window_30d: RobustWindow
    source_manifest_sha256: str
    reference_manifest_sha256: str
    strategy_profile_sha256: str
    code_sha: str
    venue_a: str
    venue_b: str
    base: str
    positive_route: str
    negative_route: str
    contract_metadata_version_a: str
    contract_metadata_version_b: str
    execution_authorized: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ValueError("unsupported historical model schema version")
        if not self.algorithm_version or not self.code_sha:
            raise ValueError("historical model identity is incomplete")
        if self.execution_authorized:
            raise ValueError("historical reference model is never executable")
        validate_historical_model(self)


def build_historical_reference_model(
    bars: tuple[ReferenceSpreadBar, ...],
    *,
    rejections: tuple[ReferenceMinuteRejection, ...] = (),
    policy: HistoricalModelPolicy | None = None,
    source_manifest_sha256: str,
    strategy_profile_sha256: str,
    code_sha: str,
    algorithm_version: str = "aggressive-symbiosis-historical-v1",
    window_start: datetime | None = None,
    window_end: datetime | None = None,
    reference_dataset_sha256: str | None = None,
) -> HistoricalReferenceModel:
    policy = policy or HistoricalModelPolicy()
    ordered = tuple(sorted(bars, key=lambda bar: bar.interval_start))
    if not ordered:
        raise ValueError("historical model requires reference bars")
    _require_single_identity(ordered)
    effective_start = window_start or ordered[0].interval_start
    effective_end = window_end or ordered[-1].interval_start + timedelta(minutes=1)
    if effective_end <= effective_start or any(
        not effective_start <= bar.interval_start < effective_end for bar in ordered
    ):
        raise ValueError("historical model exact window is invalid")
    if window_start is not None or window_end is not None:
        if window_start is None or window_end is None:
            raise ValueError("historical model exact window requires both bounds")
        outcomes = {bar.interval_start for bar in ordered} | {
            rejection.interval_start for rejection in rejections
        }
        if len(outcomes) != int((effective_end - effective_start).total_seconds() // 60):
            raise ValueError("historical model ledger does not cover the exact window")
    closes = tuple(bar.close_bps for bar in ordered)
    s0 = modal_bucket(closes, policy.mode_bucket_bps)
    distances = tuple(abs(close - s0) for close in closes)
    half_width = max(
        policy.normal_zone_minimum_half_width_bps,
        decimal_quantile(distances, Decimal("0.10")),
    )
    normal_low = s0 - half_width
    normal_high = s0 + half_width
    coverage_days = Decimal(_complete_utc_day_count(ordered))
    windows = (
        robust_window(ordered, 1440, window_end=effective_end),
        robust_window(ordered, 7 * 1440, window_end=effective_end),
        robust_window(ordered, 30 * 1440, window_end=effective_end),
    )
    positive = _build_direction(
        DivergenceDirection.POSITIVE,
        ordered,
        rejections,
        s0,
        normal_low,
        normal_high,
        coverage_days,
        windows[1],
        policy,
    )
    negative = _build_direction(
        DivergenceDirection.NEGATIVE,
        ordered,
        rejections,
        s0,
        normal_low,
        normal_high,
        coverage_days,
        windows[1],
        policy,
    )
    first = ordered[0]
    routes = directed_routes_for_reference_pair(
        first.instrument.base,
        first.venue_a,
        first.venue_b,
    )
    return HistoricalReferenceModel(
        schema_version=1,
        algorithm_version=algorithm_version,
        window_start=effective_start,
        window_end=effective_end,
        coverage_days=coverage_days,
        target_coverage_met=coverage_days >= policy.history_target_days,
        s0_bps=s0,
        normal_half_width_bps=half_width,
        normal_low_bps=normal_low,
        normal_high_bps=normal_high,
        positive=positive,
        negative=negative,
        window_24h=windows[0],
        window_7d=windows[1],
        window_30d=windows[2],
        source_manifest_sha256=source_manifest_sha256,
        reference_manifest_sha256=reference_dataset_sha256 or reference_bars_sha256(ordered),
        strategy_profile_sha256=strategy_profile_sha256,
        code_sha=code_sha,
        venue_a=first.venue_a.value,
        venue_b=first.venue_b.value,
        base=first.instrument.base,
        positive_route=_route_identity(routes.positive),
        negative_route=_route_identity(routes.negative),
        contract_metadata_version_a=first.contract_metadata_version_a,
        contract_metadata_version_b=first.contract_metadata_version_b,
    )


def load_historical_model_policy(path: Path) -> LoadedHistoricalModelPolicy:
    raw = path.read_bytes()
    loaded = yaml.safe_load(raw)
    if not isinstance(loaded, dict):
        raise ValueError("aggressive strategy profile must be a mapping")
    reference = _required_mapping(loaded, "reference_spread")
    historical = _required_mapping(loaded, "historical_model")
    grid = _required_mapping(loaded, "aggressive_grid")
    _require_profile_value(reference, "source_timeframe_seconds", 60)
    _require_profile_value(reference, "day_boundary_utc", "00:00")
    _require_profile_value(historical, "positive_and_negative_directions_separate", True)
    _require_profile_value(
        historical,
        "extreme_source",
        "maximum_valid_reference_bar_high_or_low",
    )
    _require_profile_value(historical, "current_regime_windows_hours", [24, 168, 720])
    _require_profile_value(historical, "freeze_model_after_first_tranche", True)
    _require_profile_value(grid, "effective_stop_uses_farther_of_reference_and_adaptive_tail", True)
    policy = HistoricalModelPolicy(
        history_target_days=_required_decimal(reference, "history_target_days"),
        history_minimum_live_days=_required_decimal(reference, "history_minimum_live_days"),
        history_minimum_shadow_days=_required_decimal(reference, "history_minimum_shadow_days"),
        mode_bucket_bps=_required_decimal(reference, "mode_bucket_bps"),
        normal_zone_minimum_half_width_bps=_required_decimal(
            reference, "normal_zone_minimum_half_width_bps"
        ),
        minimum_completed_episodes=_required_int(historical, "minimum_completed_episodes"),
        minimum_convergence_rate=_required_decimal(historical, "minimum_convergence_rate"),
        convergence_horizon_seconds=_required_int(historical, "convergence_horizon_seconds"),
        regime_drift_range_fraction=_required_decimal(historical, "regime_drift_range_fraction"),
        regime_drift_robust_sigma_multiple=_required_decimal(
            historical, "regime_drift_robust_sigma_multiple"
        ),
        parameter_change_limit_ratio_per_day=_required_decimal(
            historical, "parameter_change_limit_ratio_per_day"
        ),
        level_fractions=_required_decimal_tuple(grid, "level_fractions"),
        tranche_weights=_required_decimal_tuple(grid, "tranche_weights"),
        stop_buffer_ratio=_required_decimal(grid, "stop_buffer_ratio"),
        rearm_retreat_step_fraction=_required_decimal(grid, "rearm_retreat_step_fraction"),
    )
    return LoadedHistoricalModelPolicy(
        policy=policy,
        profile_sha256=hashlib.sha256(raw).hexdigest(),
    )


def effective_stop_bps(
    direction: DivergenceDirection,
    reference_stop_bps: Decimal,
    adaptive_tail_stop_bps: Decimal | None,
) -> Decimal:
    if adaptive_tail_stop_bps is None:
        return reference_stop_bps
    if not reference_stop_bps.is_finite() or not adaptive_tail_stop_bps.is_finite():
        raise ValueError("effective stop inputs must be finite")
    if direction == DivergenceDirection.POSITIVE:
        return max(reference_stop_bps, adaptive_tail_stop_bps)
    return min(reference_stop_bps, adaptive_tail_stop_bps)


def modal_bucket(values: tuple[Decimal, ...], bucket_bps: Decimal) -> Decimal:
    if not values or bucket_bps <= 0:
        raise ValueError("modal bucket requires values and a positive bucket")
    exact_median = decimal_quantile(values, Decimal("0.5"))
    counts: dict[Decimal, int] = {}
    for value in values:
        bucket = (value / bucket_bps).quantize(_ONE, rounding=ROUND_HALF_EVEN) * bucket_bps
        counts[bucket] = counts.get(bucket, 0) + 1
    maximum = max(counts.values())
    candidates = (bucket for bucket, count in counts.items() if count == maximum)
    return min(candidates, key=lambda bucket: (abs(bucket - exact_median), abs(bucket), bucket))


def decimal_quantile(values: tuple[Decimal, ...], quantile: Decimal) -> Decimal:
    if not values or not 0 <= quantile <= 1:
        raise ValueError("quantile requires values and q within [0, 1]")
    ordered = tuple(sorted(values))
    if len(ordered) == 1:
        return ordered[0]
    position = Decimal(len(ordered) - 1) * quantile
    lower_index = int(position.to_integral_value(rounding=ROUND_FLOOR))
    upper_index = min(lower_index + 1, len(ordered) - 1)
    fraction = position - Decimal(lower_index)
    return ordered[lower_index] + (ordered[upper_index] - ordered[lower_index]) * fraction


def robust_window(
    bars: tuple[ReferenceSpreadBar, ...],
    minutes: int,
    *,
    window_end: datetime | None = None,
) -> RobustWindow:
    if minutes <= 0:
        raise ValueError("robust window minutes must be positive")
    ordered = tuple(sorted(bars, key=lambda bar: bar.interval_start))
    effective_end = window_end or (
        ordered[-1].interval_start + timedelta(minutes=1) if ordered else None
    )
    selected = (
        tuple(
            bar
            for bar in ordered
            if effective_end - timedelta(minutes=minutes) <= bar.interval_start < effective_end
        )
        if effective_end is not None
        else ()
    )
    values = tuple(bar.close_bps for bar in selected)
    if not values:
        return RobustWindow(minutes, 0, False, None, None, None, None, None)
    median = decimal_quantile(values, Decimal("0.5"))
    absolute_deviations = tuple(abs(value - median) for value in values)
    return RobustWindow(
        minutes=minutes,
        sample_count=len(values),
        complete=len(values) == minutes and _consecutive(selected),
        median_bps=median,
        mad_bps=decimal_quantile(absolute_deviations, Decimal("0.5")),
        q10_bps=decimal_quantile(values, Decimal("0.10")),
        q90_bps=decimal_quantile(values, Decimal("0.90")),
        q999_abs_bps=decimal_quantile(tuple(abs(value) for value in values), Decimal("0.999")),
    )


def validate_historical_model(model: HistoricalReferenceModel) -> None:
    """Recompute every locked internal geometry invariant from persisted primitives."""
    decimals = (
        model.coverage_days,
        model.s0_bps,
        model.normal_half_width_bps,
        model.normal_low_bps,
        model.normal_high_bps,
        model.positive.extreme_bps,
        model.positive.range_bps,
        model.positive.reference_stop_bps,
        model.negative.extreme_bps,
        model.negative.range_bps,
        model.negative.reference_stop_bps,
        *model.positive.levels_bps,
        *model.negative.levels_bps,
        *model.positive.tranche_weights,
        *model.negative.tranche_weights,
    )
    if any(not value.is_finite() for value in decimals):
        raise ValueError("historical model geometry contains a non-finite value")
    if (
        model.window_end <= model.window_start
        or model.coverage_days < 0
        or model.normal_half_width_bps < Decimal(2)
        or model.normal_low_bps != model.s0_bps - model.normal_half_width_bps
        or model.normal_high_bps != model.s0_bps + model.normal_half_width_bps
    ):
        raise ValueError("historical model normal geometry is inconsistent")
    if (
        model.positive.direction != DivergenceDirection.POSITIVE
        or model.negative.direction != DivergenceDirection.NEGATIVE
        or min(model.positive.range_bps, model.negative.range_bps) < 0
        or (
            model.positive.range_bps == 0
            and model.positive.eligibility != ModelEligibility.DISABLED
        )
        or (
            model.negative.range_bps == 0
            and model.negative.eligibility != ModelEligibility.DISABLED
        )
    ):
        raise ValueError("historical model directional identity or range is invalid")
    routes = directed_routes_for_reference_pair(
        model.base,
        Venue(model.venue_a),
        Venue(model.venue_b),
    )
    if (
        model.venue_a >= model.venue_b
        or model.positive_route != _route_identity(routes.positive)
        or model.negative_route != _route_identity(routes.negative)
    ):
        raise ValueError("historical model canonical routes are inconsistent")
    expected = (
        (
            model.positive,
            model.positive.extreme_bps - model.s0_bps,
            tuple(
                model.s0_bps + (model.positive.extreme_bps - model.s0_bps) * fraction
                for fraction in _LOCKED_LEVEL_FRACTIONS
            ),
            model.s0_bps + (model.positive.extreme_bps - model.s0_bps) * Decimal("1.15"),
        ),
        (
            model.negative,
            model.s0_bps - model.negative.extreme_bps,
            tuple(
                model.s0_bps - (model.s0_bps - model.negative.extreme_bps) * fraction
                for fraction in _LOCKED_LEVEL_FRACTIONS
            ),
            model.s0_bps - (model.s0_bps - model.negative.extreme_bps) * Decimal("1.15"),
        ),
    )
    for direction, expected_range, levels, stop in expected:
        if (
            direction.range_bps != expected_range
            or direction.levels_bps != levels
            or direction.tranche_weights != _LOCKED_TRANCHE_WEIGHTS
            or direction.reference_stop_bps != stop
            or len(direction.per_level_samples) != 5
        ):
            raise ValueError("historical model locked directional geometry is inconsistent")


def historical_model_sha256(model: HistoricalReferenceModel) -> str:
    payload = historical_model_payload(model)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def historical_model_payload(model: HistoricalReferenceModel) -> dict[str, object]:
    return {
        "schema_version": model.schema_version,
        "algorithm_version": model.algorithm_version,
        "window_start": model.window_start.isoformat(),
        "window_end": model.window_end.isoformat(),
        "coverage_days": str(model.coverage_days),
        "target_coverage_met": model.target_coverage_met,
        "s0_bps": str(model.s0_bps),
        "normal_half_width_bps": str(model.normal_half_width_bps),
        "normal_low_bps": str(model.normal_low_bps),
        "normal_high_bps": str(model.normal_high_bps),
        "positive": _direction_payload(model.positive),
        "negative": _direction_payload(model.negative),
        "windows": {
            "24h": _window_payload(model.window_24h),
            "7d": _window_payload(model.window_7d),
            "30d": _window_payload(model.window_30d),
        },
        "identity": {
            "source_manifest_sha256": model.source_manifest_sha256,
            "reference_manifest_sha256": model.reference_manifest_sha256,
            "strategy_profile_sha256": model.strategy_profile_sha256,
            "code_sha": model.code_sha,
            "venue_a": model.venue_a,
            "venue_b": model.venue_b,
            "base": model.base,
            "positive_route": model.positive_route,
            "negative_route": model.negative_route,
            "contract_metadata_version_a": model.contract_metadata_version_a,
            "contract_metadata_version_b": model.contract_metadata_version_b,
        },
        "execution_authorized": False,
    }


def save_historical_model(path: Path, model: HistoricalReferenceModel) -> str:
    model_hash = historical_model_sha256(model)
    envelope = {
        "model": historical_model_payload(model),
        "model_sha256": model_hash,
    }
    encoded = json.dumps(
        envelope,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = path.with_name(f"{path.name}.{uuid4().hex}.pending")
    try:
        pending.write_bytes(encoded)
        pending.replace(path)
    finally:
        pending.unlink(missing_ok=True)
    return model_hash


def load_historical_model(path: Path) -> HistoricalReferenceModel:
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ValueError("persisted historical model is unreadable") from error
    envelope = _string_object_mapping(decoded, "historical model envelope")
    _require_exact_keys(envelope, {"model", "model_sha256"}, "historical model envelope")
    payload = _string_object_mapping(envelope["model"], "historical model payload")
    persisted_hash = _string_value(envelope, "model_sha256")
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    if hashlib.sha256(encoded.encode("utf-8")).hexdigest() != persisted_hash:
        raise ValueError("persisted historical model hash mismatch")
    model = _historical_model_from_payload(payload)
    if historical_model_sha256(model) != persisted_hash:
        raise ValueError("persisted historical model is not canonical")
    return model


def route_model_update_allowed(
    previous: HistoricalReferenceModel,
    candidate: HistoricalReferenceModel,
    *,
    elapsed_seconds: int,
    policy: HistoricalModelPolicy,
) -> bool:
    if elapsed_seconds < 0:
        raise ValueError("model update elapsed time must not be negative")
    if (previous.venue_a, previous.venue_b, previous.base) != (
        candidate.venue_a,
        candidate.venue_b,
        candidate.base,
    ):
        return False
    maximum_ratio = (
        policy.parameter_change_limit_ratio_per_day * Decimal(elapsed_seconds) / Decimal(86_400)
    )
    previous_values = _bounded_model_values(previous)
    candidate_values = _bounded_model_values(candidate)
    return all(
        abs(new - old) / max(abs(old), _ONE) <= maximum_ratio
        for old, new in zip(previous_values, candidate_values, strict=True)
    )


def select_route_model(
    current: HistoricalReferenceModel,
    candidate: HistoricalReferenceModel,
    *,
    route_active: bool,
    elapsed_seconds: int,
    policy: HistoricalModelPolicy,
) -> HistoricalReferenceModel:
    if route_active:
        return current
    if not route_model_update_allowed(
        current,
        candidate,
        elapsed_seconds=elapsed_seconds,
        policy=policy,
    ):
        raise ValueError("historical model update exceeds bounded-change policy")
    return candidate


def _build_direction(
    direction: DivergenceDirection,
    bars: tuple[ReferenceSpreadBar, ...],
    rejections: tuple[ReferenceMinuteRejection, ...],
    s0: Decimal,
    normal_low: Decimal,
    normal_high: Decimal,
    coverage_days: Decimal,
    window_7d: RobustWindow,
    policy: HistoricalModelPolicy,
) -> DirectionHistoricalModel:
    extreme = (
        max(bar.high_bps for bar in bars)
        if direction == DivergenceDirection.POSITIVE
        else min(bar.low_bps for bar in bars)
    )
    spread_range = extreme - s0 if direction == DivergenceDirection.POSITIVE else s0 - extreme
    levels = tuple(
        s0 + fraction * spread_range
        if direction == DivergenceDirection.POSITIVE
        else s0 - fraction * spread_range
        for fraction in policy.level_fractions
    )
    stop = (
        s0 + (1 + policy.stop_buffer_ratio) * spread_range
        if direction == DivergenceDirection.POSITIVE
        else s0 - (1 + policy.stop_buffer_ratio) * spread_range
    )
    episodes = (
        _historical_episodes(
            direction,
            bars,
            rejections,
            levels,
            normal_low,
            normal_high,
            policy.convergence_horizon_seconds,
        )
        if spread_range > 0
        else ()
    )
    per_level = tuple(
        tuple(
            sample
            for episode in episodes
            for sample in episode.level_samples
            if sample.level_index == i
        )
        for i in range(1, 6)
    )
    converged = sum(episode.converged for episode in episodes)
    convergence_rate = Decimal(converged) / Decimal(len(episodes)) if episodes else Decimal(0)
    regime_blocked = _regime_blocked(
        direction,
        window_7d,
        s0,
        spread_range,
        policy,
    )
    if spread_range <= 0 or coverage_days < policy.history_minimum_shadow_days:
        eligibility = ModelEligibility.DISABLED
    elif (
        coverage_days >= policy.history_minimum_live_days
        and len(episodes) >= policy.minimum_completed_episodes
        and convergence_rate >= policy.minimum_convergence_rate
        and not regime_blocked
    ):
        eligibility = ModelEligibility.LIVE_ELIGIBLE
    else:
        eligibility = ModelEligibility.SHADOW_ONLY
    return DirectionHistoricalModel(
        direction=direction,
        extreme_bps=extreme,
        range_bps=spread_range,
        levels_bps=levels,
        tranche_weights=policy.tranche_weights,
        reference_stop_bps=stop,
        episodes=episodes,
        per_level_samples=per_level,
        convergence_rate=convergence_rate,
        regime_drift_blocked=regime_blocked,
        eligibility=eligibility,
    )


def _historical_episodes(
    direction: DivergenceDirection,
    bars: tuple[ReferenceSpreadBar, ...],
    rejections: tuple[ReferenceMinuteRejection, ...],
    levels: tuple[Decimal, ...],
    normal_low: Decimal,
    normal_high: Decimal,
    horizon_seconds: int,
) -> tuple[HistoricalEpisode, ...]:
    events: dict[datetime, ReferenceSpreadBar | None] = {bar.interval_start: bar for bar in bars}
    for rejection in rejections:
        events.setdefault(rejection.interval_start, None)
    result: list[HistoricalEpisode] = []
    armed = False
    active: _ActiveEpisode | None = None
    previous_minute: datetime | None = None
    for minute, bar in sorted(events.items()):
        if previous_minute is not None and minute != previous_minute + timedelta(minutes=1):
            if active is not None:
                result.append(
                    active.close(
                        previous_minute + timedelta(minutes=1), EpisodeCloseReason.DATA_UNAVAILABLE
                    )
                )
            active = None
            armed = False
        previous_minute = minute
        if bar is None:
            if active is not None:
                result.append(active.close(minute, EpisodeCloseReason.DATA_UNAVAILABLE))
            active = None
            armed = False
            continue
        in_normal = normal_low <= bar.close_bps <= normal_high
        if active is not None:
            active.observe(bar, levels)
            elapsed = int((minute + timedelta(minutes=1) - active.started_at).total_seconds())
            if in_normal:
                result.append(
                    active.close(minute + timedelta(minutes=1), EpisodeCloseReason.NORMAL_RETURN)
                )
                active = None
                armed = True
            elif elapsed >= horizon_seconds:
                result.append(
                    active.close(
                        active.started_at + timedelta(seconds=horizon_seconds),
                        EpisodeCloseReason.HORIZON,
                    )
                )
                active = None
                armed = False
            continue
        if in_normal:
            armed = True
        crossed = (
            bar.high_bps >= levels[0]
            if direction == DivergenceDirection.POSITIVE
            else bar.low_bps <= levels[0]
        )
        if armed and crossed:
            active = _ActiveEpisode.start(direction, minute, bar, levels)
            if in_normal:
                result.append(
                    active.close(minute + timedelta(minutes=1), EpisodeCloseReason.NORMAL_RETURN)
                )
                active = None
                armed = True
    if active is not None:
        result.append(
            active.close(
                active.last_observed_at + timedelta(minutes=1), EpisodeCloseReason.DATA_UNAVAILABLE
            )
        )
    return tuple(result)


@dataclass(slots=True)
class _ActiveEpisode:
    direction: DivergenceDirection
    started_at: datetime
    last_observed_at: datetime
    reached: dict[int, tuple[datetime, Decimal, Decimal]]

    @classmethod
    def start(
        cls,
        direction: DivergenceDirection,
        minute: datetime,
        bar: ReferenceSpreadBar,
        levels: tuple[Decimal, ...],
    ) -> _ActiveEpisode:
        episode = cls(direction, minute, minute, {})
        episode.observe(bar, levels)
        return episode

    def observe(self, bar: ReferenceSpreadBar, levels: tuple[Decimal, ...]) -> None:
        self.last_observed_at = bar.interval_start
        for index, level in enumerate(levels, start=1):
            crossed = (
                bar.high_bps >= level
                if self.direction == DivergenceDirection.POSITIVE
                else bar.low_bps <= level
            )
            if crossed and index not in self.reached:
                self.reached[index] = (bar.interval_start, Decimal(0), level)
            existing = self.reached.get(index)
            if existing is not None:
                adverse = (
                    max(Decimal(0), bar.high_bps - level)
                    if self.direction == DivergenceDirection.POSITIVE
                    else max(Decimal(0), level - bar.low_bps)
                )
                self.reached[index] = (existing[0], max(existing[1], adverse), level)

    def close(self, ended_at: datetime, reason: EpisodeCloseReason) -> HistoricalEpisode:
        converged = reason == EpisodeCloseReason.NORMAL_RETURN
        samples = tuple(
            LevelEpisodeSample(
                level_index=index,
                level_bps=level,
                reached_at=reached_at,
                ended_at=ended_at,
                convergence_seconds=max(0, int((ended_at - reached_at).total_seconds())),
                adverse_excursion_bps=adverse,
                censored=not converged,
                close_reason=reason,
            )
            for index, (reached_at, adverse, level) in sorted(self.reached.items())
        )
        return HistoricalEpisode(
            direction=self.direction,
            started_at=self.started_at,
            ended_at=ended_at,
            converged=converged,
            close_reason=reason,
            level_samples=samples,
        )


def _regime_blocked(
    direction: DivergenceDirection,
    window: RobustWindow,
    s0: Decimal,
    spread_range: Decimal,
    policy: HistoricalModelPolicy,
) -> bool:
    if not window.complete or window.median_bps is None or window.mad_bps is None:
        return True
    drift = window.median_bps - s0
    directional_drift = drift if direction == DivergenceDirection.POSITIVE else -drift
    robust_sigma = _ROBUST_SIGMA_FACTOR * window.mad_bps
    return (
        directional_drift > policy.regime_drift_range_fraction * spread_range
        and directional_drift > policy.regime_drift_robust_sigma_multiple * robust_sigma
    )


def _require_single_identity(bars: tuple[ReferenceSpreadBar, ...]) -> None:
    first = bars[0]
    identity = (
        first.venue_a,
        first.venue_b,
        first.instrument,
        first.contract_metadata_version_a,
        first.contract_metadata_version_b,
    )
    if any(
        (
            bar.venue_a,
            bar.venue_b,
            bar.instrument,
            bar.contract_metadata_version_a,
            bar.contract_metadata_version_b,
        )
        != identity
        for bar in bars
    ):
        raise ValueError("historical model requires one stable reference identity")
    if len({bar.interval_start for bar in bars}) != len(bars):
        raise ValueError("historical model rejects duplicate reference minutes")


def _consecutive(bars: tuple[ReferenceSpreadBar, ...]) -> bool:
    return all(
        right.interval_start == left.interval_start + timedelta(minutes=1)
        for left, right in pairwise(bars)
    )


def _complete_utc_day_count(bars: tuple[ReferenceSpreadBar, ...]) -> int:
    """Count only UTC calendar days containing every exact minute."""
    by_day: dict[datetime, set[datetime]] = {}
    for bar in bars:
        start = bar.interval_start.replace(hour=0, minute=0)
        by_day.setdefault(start, set()).add(bar.interval_start)
    return sum(
        starts == {day_start + timedelta(minutes=offset) for offset in range(1440)}
        for day_start, starts in by_day.items()
    )


def _window_payload(window: RobustWindow) -> dict[str, object]:
    return {
        "minutes": window.minutes,
        "sample_count": window.sample_count,
        "complete": window.complete,
        "median_bps": None if window.median_bps is None else str(window.median_bps),
        "mad_bps": None if window.mad_bps is None else str(window.mad_bps),
        "q10_bps": None if window.q10_bps is None else str(window.q10_bps),
        "q90_bps": None if window.q90_bps is None else str(window.q90_bps),
        "q999_abs_bps": None if window.q999_abs_bps is None else str(window.q999_abs_bps),
    }


def _direction_payload(direction: DirectionHistoricalModel) -> dict[str, object]:
    return {
        "direction": direction.direction.value,
        "extreme_bps": str(direction.extreme_bps),
        "range_bps": str(direction.range_bps),
        "levels_bps": [str(value) for value in direction.levels_bps],
        "tranche_weights": [str(value) for value in direction.tranche_weights],
        "reference_stop_bps": str(direction.reference_stop_bps),
        "episode_count": len(direction.episodes),
        "convergence_rate": str(direction.convergence_rate),
        "regime_drift_blocked": direction.regime_drift_blocked,
        "eligibility": direction.eligibility.value,
        "episodes": [
            {
                "started_at": episode.started_at.isoformat(),
                "ended_at": episode.ended_at.isoformat(),
                "converged": episode.converged,
                "close_reason": episode.close_reason.value,
                "level_samples": [
                    {
                        "level_index": sample.level_index,
                        "level_bps": str(sample.level_bps),
                        "reached_at": sample.reached_at.isoformat(),
                        "ended_at": sample.ended_at.isoformat(),
                        "convergence_seconds": sample.convergence_seconds,
                        "adverse_excursion_bps": str(sample.adverse_excursion_bps),
                        "censored": sample.censored,
                        "close_reason": sample.close_reason.value,
                    }
                    for sample in episode.level_samples
                ],
            }
            for episode in direction.episodes
        ],
    }


def _required_mapping(parent: dict[object, object], key: str) -> dict[object, object]:
    value = parent.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"strategy profile requires mapping {key}")
    return value


def _required_decimal(parent: dict[object, object], key: str) -> Decimal:
    if key not in parent or isinstance(parent[key], bool):
        raise ValueError(f"strategy profile requires decimal {key}")
    try:
        value = Decimal(str(parent[key]))
    except Exception as error:
        raise ValueError(f"strategy profile decimal {key} is invalid") from error
    if not value.is_finite():
        raise ValueError(f"strategy profile decimal {key} must be finite")
    return value


def _required_int(parent: dict[object, object], key: str) -> int:
    value = parent.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"strategy profile requires integer {key}")
    return value


def _required_decimal_tuple(parent: dict[object, object], key: str) -> tuple[Decimal, ...]:
    value = parent.get(key)
    if not isinstance(value, list):
        raise ValueError(f"strategy profile requires list {key}")
    parsed: list[Decimal] = []
    for item in value:
        if isinstance(item, bool):
            raise ValueError(f"strategy profile list {key} contains invalid decimal")
        try:
            decimal = Decimal(str(item))
        except Exception as error:
            raise ValueError(f"strategy profile list {key} contains invalid decimal") from error
        if not decimal.is_finite():
            raise ValueError(f"strategy profile list {key} must contain finite decimals")
        parsed.append(decimal)
    return tuple(parsed)


def _require_profile_value(parent: dict[object, object], key: str, expected: object) -> None:
    if parent.get(key) != expected:
        raise ValueError(f"strategy profile value {key} does not match the model contract")


def _historical_model_from_payload(payload: dict[str, object]) -> HistoricalReferenceModel:
    _require_exact_keys(
        payload,
        {
            "schema_version",
            "algorithm_version",
            "window_start",
            "window_end",
            "coverage_days",
            "target_coverage_met",
            "s0_bps",
            "normal_half_width_bps",
            "normal_low_bps",
            "normal_high_bps",
            "positive",
            "negative",
            "windows",
            "identity",
            "execution_authorized",
        },
        "historical model payload",
    )
    if payload["execution_authorized"] is not False:
        raise ValueError("persisted historical model cannot authorize execution")
    identity = _string_object_mapping(payload["identity"], "historical model identity")
    _require_exact_keys(
        identity,
        {
            "source_manifest_sha256",
            "reference_manifest_sha256",
            "strategy_profile_sha256",
            "code_sha",
            "venue_a",
            "venue_b",
            "base",
            "positive_route",
            "negative_route",
            "contract_metadata_version_a",
            "contract_metadata_version_b",
        },
        "historical model identity",
    )
    windows = _string_object_mapping(payload["windows"], "historical model windows")
    _require_exact_keys(windows, {"24h", "7d", "30d"}, "historical model windows")
    schema_version = payload["schema_version"]
    if isinstance(schema_version, bool) or not isinstance(schema_version, int):
        raise ValueError("historical model schema version must be an integer")
    target_met = payload["target_coverage_met"]
    if not isinstance(target_met, bool):
        raise ValueError("historical model target coverage flag must be boolean")
    return HistoricalReferenceModel(
        schema_version=schema_version,
        algorithm_version=_string_value(payload, "algorithm_version"),
        window_start=_datetime_value(payload, "window_start"),
        window_end=_datetime_value(payload, "window_end"),
        coverage_days=_decimal_value(payload, "coverage_days"),
        target_coverage_met=target_met,
        s0_bps=_decimal_value(payload, "s0_bps"),
        normal_half_width_bps=_decimal_value(payload, "normal_half_width_bps"),
        normal_low_bps=_decimal_value(payload, "normal_low_bps"),
        normal_high_bps=_decimal_value(payload, "normal_high_bps"),
        positive=_direction_from_payload(
            _string_object_mapping(payload["positive"], "positive model")
        ),
        negative=_direction_from_payload(
            _string_object_mapping(payload["negative"], "negative model")
        ),
        window_24h=_window_from_payload(_string_object_mapping(windows["24h"], "24h window")),
        window_7d=_window_from_payload(_string_object_mapping(windows["7d"], "7d window")),
        window_30d=_window_from_payload(_string_object_mapping(windows["30d"], "30d window")),
        source_manifest_sha256=_string_value(identity, "source_manifest_sha256"),
        reference_manifest_sha256=_string_value(identity, "reference_manifest_sha256"),
        strategy_profile_sha256=_string_value(identity, "strategy_profile_sha256"),
        code_sha=_string_value(identity, "code_sha"),
        venue_a=_string_value(identity, "venue_a"),
        venue_b=_string_value(identity, "venue_b"),
        base=_string_value(identity, "base"),
        positive_route=_string_value(identity, "positive_route"),
        negative_route=_string_value(identity, "negative_route"),
        contract_metadata_version_a=_string_value(identity, "contract_metadata_version_a"),
        contract_metadata_version_b=_string_value(identity, "contract_metadata_version_b"),
    )


def _direction_from_payload(payload: dict[str, object]) -> DirectionHistoricalModel:
    _require_exact_keys(
        payload,
        {
            "direction",
            "extreme_bps",
            "range_bps",
            "levels_bps",
            "tranche_weights",
            "reference_stop_bps",
            "episode_count",
            "convergence_rate",
            "regime_drift_blocked",
            "eligibility",
            "episodes",
        },
        "direction model",
    )
    episodes_value = payload["episodes"]
    if not isinstance(episodes_value, list):
        raise ValueError("direction model episodes must be a list")
    direction = DivergenceDirection(_string_value(payload, "direction"))
    episodes = tuple(
        _episode_from_payload(
            direction,
            _string_object_mapping(value, "historical episode"),
        )
        for value in episodes_value
    )
    episode_count = payload["episode_count"]
    if (
        isinstance(episode_count, bool)
        or not isinstance(episode_count, int)
        or episode_count != len(episodes)
    ):
        raise ValueError("direction model episode count mismatch")
    regime_blocked = payload["regime_drift_blocked"]
    if not isinstance(regime_blocked, bool):
        raise ValueError("direction regime flag must be boolean")
    levels = _decimal_list(payload, "levels_bps")
    weights = _decimal_list(payload, "tranche_weights")
    if len(levels) != 5 or len(weights) != 5 or sum(weights) != 1:
        raise ValueError("persisted direction geometry is invalid")
    per_level = tuple(
        tuple(
            sample
            for episode in episodes
            for sample in episode.level_samples
            if sample.level_index == index
        )
        for index in range(1, 6)
    )
    return DirectionHistoricalModel(
        direction=direction,
        extreme_bps=_decimal_value(payload, "extreme_bps"),
        range_bps=_decimal_value(payload, "range_bps"),
        levels_bps=levels,
        tranche_weights=weights,
        reference_stop_bps=_decimal_value(payload, "reference_stop_bps"),
        episodes=episodes,
        per_level_samples=per_level,
        convergence_rate=_decimal_value(payload, "convergence_rate"),
        regime_drift_blocked=regime_blocked,
        eligibility=ModelEligibility(_string_value(payload, "eligibility")),
    )


def _episode_from_payload(
    direction: DivergenceDirection,
    payload: dict[str, object],
) -> HistoricalEpisode:
    _require_exact_keys(
        payload,
        {"started_at", "ended_at", "converged", "close_reason", "level_samples"},
        "historical episode",
    )
    converged = payload["converged"]
    if not isinstance(converged, bool):
        raise ValueError("episode converged flag must be boolean")
    samples_value = payload["level_samples"]
    if not isinstance(samples_value, list):
        raise ValueError("episode level samples must be a list")
    close_reason = EpisodeCloseReason(_string_value(payload, "close_reason"))
    if converged != (close_reason == EpisodeCloseReason.NORMAL_RETURN):
        raise ValueError("episode convergence and close reason conflict")
    return HistoricalEpisode(
        direction=direction,
        started_at=_datetime_value(payload, "started_at"),
        ended_at=_datetime_value(payload, "ended_at"),
        converged=converged,
        close_reason=close_reason,
        level_samples=tuple(
            _level_sample_from_payload(_string_object_mapping(value, "episode level sample"))
            for value in samples_value
        ),
    )


def _level_sample_from_payload(payload: dict[str, object]) -> LevelEpisodeSample:
    _require_exact_keys(
        payload,
        {
            "level_index",
            "level_bps",
            "reached_at",
            "ended_at",
            "convergence_seconds",
            "adverse_excursion_bps",
            "censored",
            "close_reason",
        },
        "episode level sample",
    )
    level_index = payload["level_index"]
    convergence_seconds = payload["convergence_seconds"]
    censored = payload["censored"]
    if (
        isinstance(level_index, bool)
        or not isinstance(level_index, int)
        or not 1 <= level_index <= 5
    ):
        raise ValueError("episode level index is invalid")
    if (
        isinstance(convergence_seconds, bool)
        or not isinstance(convergence_seconds, int)
        or convergence_seconds < 0
    ):
        raise ValueError("episode convergence seconds are invalid")
    if not isinstance(censored, bool):
        raise ValueError("episode censoring flag must be boolean")
    close_reason = EpisodeCloseReason(_string_value(payload, "close_reason"))
    if censored == (close_reason == EpisodeCloseReason.NORMAL_RETURN):
        raise ValueError("episode sample censoring and close reason conflict")
    return LevelEpisodeSample(
        level_index=level_index,
        level_bps=_decimal_value(payload, "level_bps"),
        reached_at=_datetime_value(payload, "reached_at"),
        ended_at=_datetime_value(payload, "ended_at"),
        convergence_seconds=convergence_seconds,
        adverse_excursion_bps=_decimal_value(payload, "adverse_excursion_bps"),
        censored=censored,
        close_reason=close_reason,
    )


def _window_from_payload(payload: dict[str, object]) -> RobustWindow:
    _require_exact_keys(
        payload,
        {
            "minutes",
            "sample_count",
            "complete",
            "median_bps",
            "mad_bps",
            "q10_bps",
            "q90_bps",
            "q999_abs_bps",
        },
        "robust window",
    )
    minutes = payload["minutes"]
    sample_count = payload["sample_count"]
    complete = payload["complete"]
    if isinstance(minutes, bool) or not isinstance(minutes, int) or minutes <= 0:
        raise ValueError("robust window minutes are invalid")
    if isinstance(sample_count, bool) or not isinstance(sample_count, int) or sample_count < 0:
        raise ValueError("robust window sample count is invalid")
    if not isinstance(complete, bool):
        raise ValueError("robust window completeness must be boolean")
    return RobustWindow(
        minutes=minutes,
        sample_count=sample_count,
        complete=complete,
        median_bps=_optional_decimal_value(payload, "median_bps"),
        mad_bps=_optional_decimal_value(payload, "mad_bps"),
        q10_bps=_optional_decimal_value(payload, "q10_bps"),
        q90_bps=_optional_decimal_value(payload, "q90_bps"),
        q999_abs_bps=_optional_decimal_value(payload, "q999_abs_bps"),
    )


def _bounded_model_values(model: HistoricalReferenceModel) -> tuple[Decimal, ...]:
    return (
        model.s0_bps,
        model.normal_half_width_bps,
        model.positive.extreme_bps,
        model.positive.range_bps,
        *model.positive.levels_bps,
        model.positive.reference_stop_bps,
        model.negative.extreme_bps,
        model.negative.range_bps,
        *model.negative.levels_bps,
        model.negative.reference_stop_bps,
    )


def _string_object_mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"{label} must be a string-keyed mapping")
    return cast(dict[str, object], value)


def _require_exact_keys(value: dict[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise ValueError(f"{label} has missing or unknown fields")


def _string_value(parent: dict[str, object], key: str) -> str:
    value = parent.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"persisted historical model requires string {key}")
    return value


def _decimal_value(parent: dict[str, object], key: str) -> Decimal:
    value = _string_value(parent, key)
    try:
        parsed = Decimal(value)
    except Exception as error:
        raise ValueError(f"persisted historical model decimal {key} is invalid") from error
    if not parsed.is_finite():
        raise ValueError(f"persisted historical model decimal {key} must be finite")
    return parsed


def _optional_decimal_value(parent: dict[str, object], key: str) -> Decimal | None:
    if parent.get(key) is None:
        return None
    return _decimal_value(parent, key)


def _datetime_value(parent: dict[str, object], key: str) -> datetime:
    value = _string_value(parent, key)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"persisted historical model datetime {key} is invalid") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"persisted historical model datetime {key} must be aware")
    return parsed


def _decimal_list(parent: dict[str, object], key: str) -> tuple[Decimal, ...]:
    value = parent.get(key)
    if not isinstance(value, list):
        raise ValueError(f"persisted historical model requires list {key}")
    parsed: list[Decimal] = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError(f"persisted historical model list {key} must contain strings")
        try:
            decimal = Decimal(item)
        except Exception as error:
            raise ValueError(
                f"persisted historical model list {key} contains invalid decimal"
            ) from error
        if not decimal.is_finite():
            raise ValueError(f"persisted historical model list {key} must contain finite decimals")
        parsed.append(decimal)
    return tuple(parsed)


def _route_identity(route: DirectedRouteKey) -> str:
    return f"{route.base}:{route.long_venue.value}>{route.short_venue.value}"
