from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import threading
from contextlib import suppress
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

from interexchange_perp_grid.candidate_l2 import BookKey, CandidateL2BookState
from interexchange_perp_grid.domain import FundingSnapshot, Venue
from interexchange_perp_grid.market_universe import UniverseRoute
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.routes import (
    common_base_quantity,
    evaluate_directed_route,
    minimum_common_base_quantity,
)
from interexchange_perp_grid.state import initialise_state
from interexchange_perp_grid.strategy import DirectedRouteKey

MINIMUM_SPREAD_BUCKET_CONVERGENCE_SAMPLES = 30


@dataclass(frozen=True, slots=True)
class RouteCalibrationEpisodeSample:
    entry_spread_bps: Decimal
    peak_spread_bps: Decimal
    convergence_seconds: Decimal
    spread_bucket_index: int | None = None
    censored: bool = False

    def __post_init__(self) -> None:
        values = (
            self.entry_spread_bps,
            self.peak_spread_bps,
            self.convergence_seconds,
        )
        if any(not value.is_finite() or value < 0 for value in values):
            raise ValueError("calibration episode samples must be finite and non-negative")
        if self.peak_spread_bps < self.entry_spread_bps:
            raise ValueError("calibration episode peak cannot precede its entry spread")
        if self.spread_bucket_index is not None and not 0 <= self.spread_bucket_index < 5:
            raise ValueError("calibration episode spread bucket must be within [0, 4]")
        if not isinstance(self.censored, bool):
            raise ValueError("calibration episode censored flag must be boolean")


@dataclass(frozen=True, slots=True)
class RouteCalibrationObservation:
    route: DirectedRouteKey
    size_bucket_multiplier: Decimal
    base_quantity: Decimal | None
    epoch_id: str
    observed_at: datetime
    spread_bps: Decimal | None
    adverse_excursion_after_entry_bps: Decimal | None
    convergence_seconds: Decimal | None
    stressed_cost_floor_bps: Decimal | None
    normalized_tick_bps: Decimal | None
    notional_usdt: Decimal | None
    funding_rate_delta: Decimal | None
    exit_depth_multiple: Decimal | None
    reason: ReasonCode
    episode_peak_spread_bps: Decimal | None = None
    episode_entry_spread_bps: Decimal | None = None
    episode_samples: tuple[RouteCalibrationEpisodeSample, ...] = ()

    def __post_init__(self) -> None:
        if self.size_bucket_multiplier <= 0 or not self.size_bucket_multiplier.is_finite():
            raise ValueError("calibration size multiplier must be finite and positive")
        if self.base_quantity is not None and (
            self.base_quantity <= 0 or not self.base_quantity.is_finite()
        ):
            raise ValueError("calibration base quantity must be finite and positive")
        if not self.epoch_id.strip():
            raise ValueError("calibration epoch must be non-empty")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("calibration observation time must be timezone-aware")
        object.__setattr__(self, "observed_at", self.observed_at.astimezone(UTC))
        non_negative = (
            self.adverse_excursion_after_entry_bps,
            self.convergence_seconds,
            self.stressed_cost_floor_bps,
            self.normalized_tick_bps,
            self.notional_usdt,
            self.exit_depth_multiple,
            self.episode_peak_spread_bps,
            self.episode_entry_spread_bps,
        )
        if any(
            value is not None and (not value.is_finite() or value < 0) for value in non_negative
        ):
            raise ValueError("calibration measurements must be finite and non-negative")
        if self.spread_bps is not None and not self.spread_bps.is_finite():
            raise ValueError("calibration spread must be finite")
        if self.funding_rate_delta is not None and not self.funding_rate_delta.is_finite():
            raise ValueError("calibration funding must be finite")

    @property
    def key(self) -> tuple[DirectedRouteKey, Decimal]:
        return self.route, self.size_bucket_multiplier


def _effective_observation_reason(observation: RouteCalibrationObservation) -> ReasonCode:
    if observation.reason != ReasonCode.QUOTE_READY:
        return observation.reason
    if observation.funding_rate_delta is None:
        return ReasonCode.FUNDING_UNKNOWN
    required = (
        observation.base_quantity,
        observation.spread_bps,
        observation.stressed_cost_floor_bps,
        observation.normalized_tick_bps,
        observation.notional_usdt,
        observation.exit_depth_multiple,
    )
    if any(value is None for value in required):
        return ReasonCode.CALIBRATION_INSUFFICIENT
    if observation.exit_depth_multiple is not None and observation.exit_depth_multiple < Decimal(3):
        return ReasonCode.DEPTH_INSUFFICIENT
    return ReasonCode.QUOTE_READY


def _episode_samples_for_observation(
    observation: RouteCalibrationObservation,
) -> tuple[RouteCalibrationEpisodeSample, ...]:
    if observation.episode_samples:
        return observation.episode_samples
    if observation.episode_entry_spread_bps is None or observation.convergence_seconds is None:
        return ()
    peak = observation.episode_peak_spread_bps
    if peak is None:
        peak = observation.episode_entry_spread_bps + (
            observation.adverse_excursion_after_entry_bps or Decimal(0)
        )
    return (
        RouteCalibrationEpisodeSample(
            observation.episode_entry_spread_bps,
            peak,
            observation.convergence_seconds,
        ),
    )


def _episode_adverse_values(observation: RouteCalibrationObservation) -> tuple[Decimal, ...]:
    samples = _episode_samples_for_observation(observation)
    if samples:
        return tuple(
            max(Decimal(0), sample.peak_spread_bps - sample.entry_spread_bps) for sample in samples
        )
    return (
        (observation.adverse_excursion_after_entry_bps,)
        if observation.adverse_excursion_after_entry_bps is not None
        else ()
    )


def _episode_convergence_values(
    observation: RouteCalibrationObservation,
) -> tuple[Decimal, ...]:
    samples = _episode_samples_for_observation(observation)
    if samples:
        return tuple(sample.convergence_seconds for sample in samples if not sample.censored)
    return (observation.convergence_seconds,) if observation.convergence_seconds is not None else ()


@dataclass(frozen=True, slots=True)
class CalibrationWindow:
    hours: int
    sample_count: int
    observation_period_seconds: int
    complete: bool
    median_spread_bps: Decimal
    mad_spread_bps: Decimal
    q99_spread_bps: Decimal
    q90_stressed_cost_floor_bps: Decimal
    q10_exit_depth_multiple: Decimal
    q90_absolute_funding_rate_delta: Decimal


@dataclass(frozen=True, slots=True)
class SpreadBucketConvergence:
    bucket_index: int
    lower_bound_bps: Decimal
    upper_bound_bps: Decimal | None
    sample_count: int
    minimum_sample_count: int
    convergence_p90_seconds: Decimal | None

    def __post_init__(self) -> None:
        if not 0 <= self.bucket_index < 5:
            raise ValueError("spread bucket index must be within [0, 4]")
        if not self.lower_bound_bps.is_finite():
            raise ValueError("spread bucket lower bound must be finite")
        if self.upper_bound_bps is not None and (
            not self.upper_bound_bps.is_finite() or self.upper_bound_bps <= self.lower_bound_bps
        ):
            raise ValueError("spread bucket upper bound must be finite and increasing")
        if self.sample_count < 0:
            raise ValueError("spread bucket sample count cannot be negative")
        if self.minimum_sample_count < 3:
            raise ValueError("spread bucket minimum sample count must be at least three")
        if (self.convergence_p90_seconds is None) != (self.sample_count == 0):
            raise ValueError("spread bucket convergence requires supporting samples")
        if self.convergence_p90_seconds is not None and (
            not self.convergence_p90_seconds.is_finite() or self.convergence_p90_seconds < 0
        ):
            raise ValueError("spread bucket convergence must be finite and non-negative")

    @property
    def ready(self) -> bool:
        return (
            self.sample_count >= self.minimum_sample_count
            and self.convergence_p90_seconds is not None
        )


@dataclass(frozen=True, slots=True)
class RouteCalibrationParameters:
    route: DirectedRouteKey
    size_bucket_multiplier: Decimal
    latest_base_quantity: Decimal
    epoch_id: str
    version: int
    calibrated_at: datetime
    sample_count: int
    observation_period_seconds: int
    window_24h: CalibrationWindow
    window_7d: CalibrationWindow
    window_30d: CalibrationWindow
    robust_sigma_bps: Decimal
    q90_spread_bps: Decimal
    q999_spread_bps: Decimal
    q75_adverse_excursion_bps: Decimal
    convergence_p90_seconds: Decimal
    convergence_by_spread_bucket: tuple[
        SpreadBucketConvergence,
        SpreadBucketConvergence,
        SpreadBucketConvergence,
        SpreadBucketConvergence,
        SpreadBucketConvergence,
    ]
    stressed_cost_floor_bps: Decimal
    minimum_profit_usdt: Decimal
    minimum_profit_bps: Decimal
    normalized_tick_bps: Decimal
    raw_entry_level_1_bps: Decimal
    raw_grid_step_bps: Decimal
    raw_route_stop_bps: Decimal
    entry_levels_bps: tuple[Decimal, Decimal, Decimal, Decimal, Decimal]
    grid_step_bps: Decimal
    route_stop_bps: Decimal
    target_close_reference_bps: Decimal
    long_tail_q999_spread_bps: Decimal
    change_anchor_at: datetime
    change_anchor_entry_level_1_bps: Decimal
    change_anchor_grid_step_bps: Decimal
    change_anchor_route_stop_bps: Decimal
    cost_model_scope: str = "PUBLIC_SHADOW_ESTIMATE"
    execution_authorized: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        if len(self.convergence_by_spread_bucket) != 5:
            raise ValueError("route calibration requires five convergence spread buckets")
        if tuple(item.bucket_index for item in self.convergence_by_spread_bucket) != tuple(
            range(5)
        ):
            raise ValueError("convergence spread buckets must be ordered and complete")
        if tuple(item.lower_bound_bps for item in self.convergence_by_spread_bucket) != (
            self.entry_levels_bps
        ):
            raise ValueError("convergence spread buckets must align with grid entry levels")
        if tuple(item.upper_bound_bps for item in self.convergence_by_spread_bucket) != (
            *self.entry_levels_bps[1:],
            None,
        ):
            raise ValueError("convergence spread buckets must be contiguous and final-open")

    def convergence_p90_for_spread(self, spread_bps: Decimal) -> Decimal | None:
        if not spread_bps.is_finite():
            raise ValueError("entry spread must be finite")
        if spread_bps < self.convergence_by_spread_bucket[0].lower_bound_bps:
            return None
        selected = self.convergence_by_spread_bucket[0]
        for bucket in self.convergence_by_spread_bucket:
            if spread_bps >= bucket.lower_bound_bps:
                selected = bucket
            else:
                break
        return selected.convergence_p90_seconds if selected.ready else None

    def target_close_bps(self, tranche_entry_bps: Decimal) -> Decimal:
        if not tranche_entry_bps.is_finite():
            raise ValueError("tranche entry spread must be finite")
        return min(
            self.window_24h.median_spread_bps + Decimal("0.5") * self.robust_sigma_bps,
            tranche_entry_bps
            - max(
                self.grid_step_bps,
                self.stressed_cost_floor_bps + self.minimum_profit_bps,
            ),
        )


@dataclass(frozen=True, slots=True)
class RouteCalibrationAssessment:
    route: DirectedRouteKey
    size_bucket_multiplier: Decimal
    latest_base_quantity: Decimal | None
    reason: ReasonCode
    sample_count: int
    parameters: RouteCalibrationParameters | None
    staged_parameters: RouteCalibrationParameters | None = field(default=None, repr=False)
    execution_authorized: bool = field(default=False, init=False)

    @property
    def ready(self) -> bool:
        return self.reason == ReasonCode.QUOTE_READY and self.parameters is not None


@dataclass(frozen=True, slots=True)
class RouteCalibrationSamplingPolicy:
    size_multipliers: tuple[Decimal, ...]
    maximum_holding_seconds: int
    funding_maximum_age_seconds: int
    funding_stress_multiplier: Decimal
    latency_reserve_bps: Decimal
    partial_fill_reserve_bps: Decimal
    emergency_hedge_reserve_bps: Decimal
    reconciliation_forced_exit_reserve_bps: Decimal

    def __post_init__(self) -> None:
        if not self.size_multipliers or len(self.size_multipliers) > 5:
            raise ValueError("calibration requires between one and five size multipliers")
        if tuple(sorted(set(self.size_multipliers))) != self.size_multipliers:
            raise ValueError("calibration size multipliers must be unique and increasing")
        if any(not value.is_finite() or value <= 0 for value in self.size_multipliers):
            raise ValueError("calibration size multipliers must be finite and positive")
        if self.maximum_holding_seconds <= 0:
            raise ValueError("calibration maximum holding time must be positive")
        if self.funding_maximum_age_seconds <= 0:
            raise ValueError("calibration funding maximum age must be positive")
        non_negative = (
            self.latency_reserve_bps,
            self.partial_fill_reserve_bps,
            self.emergency_hedge_reserve_bps,
            self.reconciliation_forced_exit_reserve_bps,
        )
        if any(not value.is_finite() or value < 0 for value in non_negative):
            raise ValueError("calibration reserves must be finite and non-negative")
        if not self.funding_stress_multiplier.is_finite() or self.funding_stress_multiplier < 1:
            raise ValueError("calibration funding stress multiplier must be at least one")


def _quantile(values: tuple[Decimal, ...], probability: Decimal) -> Decimal:
    if not values:
        raise ValueError("quantile requires observations")
    ordered = tuple(sorted(values))
    position = probability * Decimal(len(ordered) - 1)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - Decimal(lower)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _convergence_by_spread_bucket(
    observations: tuple[RouteCalibrationObservation, ...],
    entry_levels_bps: tuple[Decimal, Decimal, Decimal, Decimal, Decimal],
    minimum_sample_count: int,
) -> tuple[
    SpreadBucketConvergence,
    SpreadBucketConvergence,
    SpreadBucketConvergence,
    SpreadBucketConvergence,
    SpreadBucketConvergence,
]:
    samples: list[list[Decimal]] = [[], [], [], [], []]
    for observation in observations:
        for episode in _episode_samples_for_observation(observation):
            if episode.censored:
                continue
            entry_spread = episode.entry_spread_bps
            bucket_index = episode.spread_bucket_index
            if bucket_index is None:
                if entry_spread < entry_levels_bps[0]:
                    continue
                bucket_index = 0
                for index, lower_bound in enumerate(entry_levels_bps):
                    if entry_spread >= lower_bound:
                        bucket_index = index
                    else:
                        break
            samples[bucket_index].append(episode.convergence_seconds)
    buckets = tuple(
        SpreadBucketConvergence(
            bucket_index=index,
            lower_bound_bps=lower_bound,
            upper_bound_bps=(entry_levels_bps[index + 1] if index < 4 else None),
            sample_count=len(samples[index]),
            minimum_sample_count=minimum_sample_count,
            convergence_p90_seconds=(
                _quantile(tuple(samples[index]), Decimal("0.90")) if samples[index] else None
            ),
        )
        for index, lower_bound in enumerate(entry_levels_bps)
    )
    return buckets  # type: ignore[return-value]


def _funding_interval_seconds(value: str | None) -> int | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if len(normalized) < 2 or normalized[-1] not in {"h", "m"}:
        return None
    try:
        amount = int(normalized[:-1])
    except ValueError:
        return None
    if amount <= 0:
        return None
    return amount * (3600 if normalized[-1] == "h" else 60)


def _funding_stress_bps(
    long_funding: FundingSnapshot,
    short_funding: FundingSnapshot,
    policy: RouteCalibrationSamplingPolicy,
) -> Decimal | None:
    if long_funding.rate is None or short_funding.rate is None:
        return None
    long_interval = _funding_interval_seconds(long_funding.interval)
    short_interval = _funding_interval_seconds(short_funding.interval)
    if long_interval is None or short_interval is None:
        return None
    long_payments = max(1, (policy.maximum_holding_seconds + long_interval - 1) // long_interval)
    short_payments = max(1, (policy.maximum_holding_seconds + short_interval - 1) // short_interval)
    expected = (
        abs(long_funding.rate) * Decimal(long_payments)
        + abs(short_funding.rate) * Decimal(short_payments)
    ) * Decimal(10_000)
    return expected * (Decimal(1) + policy.funding_stress_multiplier)


def _qualified_funding(
    snapshot: FundingSnapshot | None,
    *,
    venue: Venue,
    symbol: str,
    observed_at: datetime,
    maximum_age_seconds: int,
) -> FundingSnapshot | None:
    if (
        snapshot is None
        or snapshot.venue != venue
        or snapshot.symbol != symbol
        or snapshot.rate is None
        or snapshot.interval is None
        or snapshot.mark_price is None
        or snapshot.index_price is None
        or snapshot.next_funding_timestamp_ms is None
        or snapshot.exchange_timestamp_ms is None
    ):
        return None
    if (
        not isinstance(snapshot.rate, Decimal)
        or not snapshot.rate.is_finite()
        or not isinstance(snapshot.mark_price, Decimal)
        or not snapshot.mark_price.is_finite()
        or snapshot.mark_price <= 0
        or not isinstance(snapshot.index_price, Decimal)
        or not snapshot.index_price.is_finite()
        or snapshot.index_price <= 0
    ):
        return None
    observed_ms = int(observed_at.timestamp() * 1000)
    age_ms = observed_ms - snapshot.exchange_timestamp_ms
    if age_ms < 0 or age_ms > maximum_age_seconds * 1000:
        return None
    if snapshot.next_funding_timestamp_ms < observed_ms:
        return None
    return snapshot


def build_route_calibration_observations(
    routes: tuple[UniverseRoute, ...],
    states: dict[BookKey, CandidateL2BookState],
    funding: dict[BookKey, FundingSnapshot],
    *,
    policy: RouteCalibrationSamplingPolicy,
    epoch_id: str,
    observed_at: datetime,
    unavailable_venues: frozenset[Venue] = frozenset(),
    missing_active_routes: tuple[tuple[str, str, str], ...] = (),
    removed_routes: tuple[DirectedRouteKey, ...] = (),
) -> tuple[RouteCalibrationObservation, ...]:
    """Build immutable, non-executable route/size observations from one L2 generation."""
    observations: list[RouteCalibrationObservation] = []

    def invalid(
        key: DirectedRouteKey,
        reason: ReasonCode,
    ) -> None:
        observations.extend(
            RouteCalibrationObservation(
                key,
                multiplier,
                None,
                epoch_id,
                observed_at,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                reason,
            )
            for multiplier in policy.size_multipliers
        )

    for base, long_venue, short_venue in missing_active_routes:
        key = DirectedRouteKey(base, Venue(long_venue), Venue(short_venue))
        reason = (
            ReasonCode.VENUE_OUTAGE
            if key.long_venue in unavailable_venues or key.short_venue in unavailable_venues
            else ReasonCode.CONTRACT_METADATA_UNKNOWN
        )
        invalid(key, reason)
    for key in removed_routes:
        invalid(key, ReasonCode.BOOK_EMPTY)
    for route in routes:
        long = route.long_instrument
        short = route.short_instrument
        key = DirectedRouteKey(long.base, long.venue, short.venue)
        if long.venue in unavailable_venues or short.venue in unavailable_venues:
            invalid(key, ReasonCode.VENUE_OUTAGE)
            continue
        long_state = states.get((long.venue, long.symbol))
        short_state = states.get((short.venue, short.symbol))
        if (
            long_state is None
            or short_state is None
            or long_state.book is None
            or short_state.book is None
            or not long_state.book.asks
            or not short_state.book.bids
        ):
            invalid(key, ReasonCode.BOOK_EMPTY)
            continue
        if not long_state.quality.accepted:
            invalid(key, long_state.quality.reason)
            continue
        if not short_state.quality.accepted:
            invalid(key, short_state.quality.reason)
            continue
        long_book = long_state.book
        short_book = short_state.book
        try:
            minimum = minimum_common_base_quantity(
                long,
                short,
                long_book.asks[0].price,
                short_book.bids[0].price,
            )
        except ValueError:
            invalid(key, ReasonCode.CONTRACT_METADATA_UNKNOWN)
            continue
        long_funding = _qualified_funding(
            funding.get((long.venue, long.symbol)),
            venue=long.venue,
            symbol=long.symbol,
            observed_at=observed_at,
            maximum_age_seconds=policy.funding_maximum_age_seconds,
        )
        long_funding = long_funding or FundingSnapshot(
            long.venue,
            long.symbol,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        short_funding = _qualified_funding(
            funding.get((short.venue, short.symbol)),
            venue=short.venue,
            symbol=short.symbol,
            observed_at=observed_at,
            maximum_age_seconds=policy.funding_maximum_age_seconds,
        )
        short_funding = short_funding or FundingSnapshot(
            short.venue,
            short.symbol,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        for multiplier in policy.size_multipliers:
            quantity = common_base_quantity(minimum * multiplier, long, short)
            quote = evaluate_directed_route(
                long,
                short,
                long_book,
                short_book,
                long_funding,
                short_funding,
                long_state.quality,
                short_state.quality,
                quantity,
            )
            if not quote.eligible:
                observations.append(
                    RouteCalibrationObservation(
                        DirectedRouteKey(long.base, long.venue, short.venue),
                        multiplier,
                        quantity,
                        epoch_id,
                        observed_at,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        quote.reason,
                    )
                )
                continue
            assert quote.entry_long_vwap is not None
            assert quote.entry_short_vwap is not None
            assert quote.exit_long_vwap is not None
            assert quote.exit_short_vwap is not None
            assert quote.entry_spread_bps is not None
            assert quote.four_leg_fee_estimate is not None
            assert quote.funding_rate_delta is not None
            midpoint_notional = (
                quantity * (quote.entry_long_vwap + quote.entry_short_vwap) / Decimal(2)
            )
            if midpoint_notional <= 0:
                observations.append(
                    RouteCalibrationObservation(
                        key,
                        multiplier,
                        quantity,
                        epoch_id,
                        observed_at,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        ReasonCode.CONTRACT_METADATA_UNKNOWN,
                    )
                )
                continue
            impact_usdt = quantity * (
                max(Decimal(0), quote.entry_long_vwap - long_book.asks[0].price)
                + max(Decimal(0), short_book.bids[0].price - quote.entry_short_vwap)
                + max(Decimal(0), long_book.bids[0].price - quote.exit_long_vwap)
                + max(Decimal(0), quote.exit_short_vwap - short_book.asks[0].price)
            )
            funding_bps = _funding_stress_bps(long_funding, short_funding, policy)
            if funding_bps is None:
                reason = ReasonCode.FUNDING_UNKNOWN
                cost_floor = None
            else:
                reason = ReasonCode.QUOTE_READY
                fee_bps = quote.four_leg_fee_estimate / midpoint_notional * Decimal(10_000)
                impact_bps = impact_usdt / midpoint_notional * Decimal(10_000)
                cost_floor = (
                    fee_bps
                    + impact_bps
                    + funding_bps
                    + policy.latency_reserve_bps
                    + policy.partial_fill_reserve_bps
                    + policy.emergency_hedge_reserve_bps
                    + policy.reconciliation_forced_exit_reserve_bps
                )
            normalized_tick = max(
                long.price_tick / quote.entry_long_vwap,
                short.price_tick / quote.entry_short_vwap,
            ) * Decimal(10_000)
            exit_depth = (
                min(
                    sum((level.base_quantity for level in long_book.bids), Decimal(0)),
                    sum((level.base_quantity for level in short_book.asks), Decimal(0)),
                )
                / quantity
            )
            observations.append(
                RouteCalibrationObservation(
                    DirectedRouteKey(long.base, long.venue, short.venue),
                    multiplier,
                    quantity,
                    epoch_id,
                    observed_at,
                    quote.entry_spread_bps,
                    None,
                    None,
                    cost_floor,
                    normalized_tick,
                    midpoint_notional,
                    quote.funding_rate_delta,
                    exit_depth,
                    reason,
                )
            )
    return tuple(observations)


def _window(observations: tuple[RouteCalibrationObservation, ...], hours: int) -> CalibrationWindow:
    spreads = tuple(observation.spread_bps for observation in observations)
    if not spreads or any(value is None for value in spreads):
        raise ValueError("calibration window requires complete spreads")
    complete = tuple(value for value in spreads if value is not None)
    median = _quantile(complete, Decimal("0.50"))
    mad = _quantile(tuple(abs(value - median) for value in complete), Decimal("0.50"))
    costs = tuple(
        item.stressed_cost_floor_bps
        for item in observations
        if item.stressed_cost_floor_bps is not None
    )
    depths = tuple(
        item.exit_depth_multiple for item in observations if item.exit_depth_multiple is not None
    )
    funding = tuple(
        abs(item.funding_rate_delta) for item in observations if item.funding_rate_delta is not None
    )
    if not costs or not depths or not funding:
        raise ValueError("calibration window requires cost, depth, and funding measurements")
    period_seconds = max(
        0,
        int((observations[-1].observed_at - observations[0].observed_at).total_seconds()),
    )
    return CalibrationWindow(
        hours,
        len(complete),
        period_seconds,
        period_seconds >= hours * 3600,
        median,
        mad,
        _quantile(complete, Decimal("0.99")),
        _quantile(costs, Decimal("0.90")),
        _quantile(depths, Decimal("0.10")),
        _quantile(funding, Decimal("0.90")),
    )


def _bound_from_anchor(raw: Decimal, anchor: Decimal, limit: Decimal) -> Decimal:
    if anchor <= 0:
        return raw
    return min(max(raw, anchor * (Decimal(1) - limit)), anchor * (Decimal(1) + limit))


def _incremental_safety_reason(
    observation: RouteCalibrationObservation,
    previous: RouteCalibrationParameters,
    minimum_profit_usdt: Decimal,
) -> ReasonCode:
    """Fail closed between bounded full-window recalibrations."""

    reason = _effective_observation_reason(observation)
    if reason != ReasonCode.QUOTE_READY:
        return reason
    if observation.spread_bps is None:
        return ReasonCode.CALIBRATION_INSUFFICIENT
    # A single observation is not a regime window: normal deeper grid levels
    # intentionally exceed the old median and q99.  The exact median/q99
    # regime comparison runs on the bounded round-robin rebuild.  Between
    # rebuilds, only an observation beyond the already locked route stop is a
    # safe point-wise reason to invalidate the old parameter version.
    if observation.spread_bps > previous.route_stop_bps:
        return ReasonCode.CALIBRATION_REGIME_SHIFT
    assert observation.stressed_cost_floor_bps is not None
    assert observation.normalized_tick_bps is not None
    assert observation.notional_usdt is not None
    if observation.notional_usdt <= 0:
        return ReasonCode.CALIBRATION_INSUFFICIENT
    current_minimum_profit_bps = minimum_profit_usdt / observation.notional_usdt * Decimal(10_000)
    current_required_entry_bps = (
        Decimal(2) * observation.stressed_cost_floor_bps + current_minimum_profit_bps
    )
    if (
        current_required_entry_bps > previous.entry_levels_bps[0]
        or observation.normalized_tick_bps > previous.grid_step_bps
        or (
            observation.adverse_excursion_after_entry_bps is not None
            and observation.adverse_excursion_after_entry_bps > previous.grid_step_bps
        )
    ):
        return ReasonCode.CALIBRATION_REGIME_SHIFT
    return ReasonCode.QUOTE_READY


def calibrate_route_size(
    observations: tuple[RouteCalibrationObservation, ...],
    *,
    now: datetime,
    minimum_samples: int,
    minimum_observation_period: timedelta,
    minimum_profit_usdt: Decimal,
    parameter_change_limit_ratio_per_day: Decimal,
    previous: RouteCalibrationParameters | None = None,
    previous_window_coverage_tolerance: timedelta = timedelta(minutes=1),
    minimum_convergence_samples_per_spread_bucket: int = (
        MINIMUM_SPREAD_BUCKET_CONVERGENCE_SAMPLES
    ),
) -> RouteCalibrationAssessment:
    if not observations:
        raise ValueError("route calibration requires observations")
    if minimum_samples < 3:
        raise ValueError("route calibration requires at least three samples")
    if minimum_observation_period.total_seconds() < 0:
        raise ValueError("minimum observation period cannot be negative")
    if minimum_profit_usdt < 0 or not minimum_profit_usdt.is_finite():
        raise ValueError("minimum profit must be finite and non-negative")
    if not Decimal(0) < parameter_change_limit_ratio_per_day <= Decimal("0.50"):
        raise ValueError("parameter change limit must be within (0, 0.50]")
    if not timedelta(0) <= previous_window_coverage_tolerance <= timedelta(minutes=5):
        raise ValueError("previous-window coverage tolerance must be within five minutes")
    if minimum_convergence_samples_per_spread_bucket < 3:
        raise ValueError("spread bucket convergence requires at least three episode samples")
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("calibration time must be aware and not precede observations")
    now = now.astimezone(UTC)
    ordered = tuple(sorted(observations, key=lambda item: item.observed_at))
    latest = ordered[-1]
    if now < latest.observed_at:
        raise ValueError("calibration time must be aware and not precede observations")
    if previous is not None and now < previous.calibrated_at:
        raise ValueError("calibration parameter time cannot regress")
    if any(
        observation.key != latest.key or observation.epoch_id != latest.epoch_id
        for observation in ordered
    ):
        raise ValueError("calibration observations must share route, size, and epoch")
    latest_reason = _effective_observation_reason(latest)
    if latest_reason != ReasonCode.QUOTE_READY:
        return RouteCalibrationAssessment(
            latest.route,
            latest.size_bucket_multiplier,
            latest.base_quantity,
            latest_reason,
            sum(
                _effective_observation_reason(observation) == ReasonCode.QUOTE_READY
                for observation in ordered
            ),
            None,
        )
    last_invalid = max(
        (
            index
            for index, observation in enumerate(ordered)
            if _effective_observation_reason(observation) != ReasonCode.QUOTE_READY
        ),
        default=-1,
    )
    ready = tuple(
        observation
        for observation in ordered[last_invalid + 1 :]
        if _effective_observation_reason(observation) == ReasonCode.QUOTE_READY
    )
    if len(ready) < minimum_samples:
        return RouteCalibrationAssessment(
            latest.route,
            latest.size_bucket_multiplier,
            latest.base_quantity,
            ReasonCode.CALIBRATION_INSUFFICIENT,
            len(ready),
            None,
        )
    period = ready[-1].observed_at - ready[0].observed_at
    if period < minimum_observation_period:
        return RouteCalibrationAssessment(
            latest.route,
            latest.size_bucket_multiplier,
            latest.base_quantity,
            ReasonCode.CALIBRATION_INSUFFICIENT,
            len(ready),
            None,
        )
    current_24h = tuple(item for item in ready if item.observed_at >= now - timedelta(hours=24))
    current_7d = tuple(item for item in ready if item.observed_at >= now - timedelta(days=7))
    current_30d = tuple(item for item in ready if item.observed_at >= now - timedelta(days=30))
    if len(current_24h) < minimum_samples or not current_7d or not current_30d:
        return RouteCalibrationAssessment(
            latest.route,
            latest.size_bucket_multiplier,
            latest.base_quantity,
            ReasonCode.CALIBRATION_INSUFFICIENT,
            len(ready),
            None,
        )
    historical_30d = tuple(item for item in ordered if item.observed_at >= now - timedelta(days=30))
    adverse = tuple(
        adverse_value for item in historical_30d for adverse_value in _episode_adverse_values(item)
    )
    convergence = tuple(
        convergence_value
        for item in historical_30d
        for convergence_value in _episode_convergence_values(item)
    )
    if len(adverse) < 3 or len(convergence) < 3:
        return RouteCalibrationAssessment(
            latest.route,
            latest.size_bucket_multiplier,
            latest.base_quantity,
            ReasonCode.CALIBRATION_INSUFFICIENT,
            len(ready),
            None,
        )
    window_24h = _window(current_24h, 24)
    window_7d = _window(current_7d, 24 * 7)
    window_30d = _window(current_30d, 24 * 30)
    robust_sigma = Decimal("1.4826") * window_24h.mad_spread_bps
    previous_24h = tuple(
        item
        for item in ready
        if now - timedelta(hours=48) <= item.observed_at <= now - timedelta(hours=24)
    )
    empirical_prior = _window(previous_24h, 24) if len(previous_24h) >= 3 else None
    prior_coverage_seconds = 24 * 3600 - int(previous_window_coverage_tolerance.total_seconds())
    prior_window = (
        empirical_prior
        if empirical_prior is not None
        and empirical_prior.observation_period_seconds >= prior_coverage_seconds
        else previous.window_24h
        if previous is not None
        else None
    )
    if prior_window is not None:
        prior = prior_window
        median_shift = abs(window_24h.median_spread_bps - prior.median_spread_bps)
        q99_denominator = max(abs(prior.q99_spread_bps), Decimal("0.00000001"))
        q99_change = abs(window_24h.q99_spread_bps - prior.q99_spread_bps) / q99_denominator
        if median_shift > Decimal(3) * max(
            Decimal("1.4826") * prior.mad_spread_bps,
            Decimal("0.00000001"),
        ) or q99_change > Decimal("0.30"):
            return RouteCalibrationAssessment(
                latest.route,
                latest.size_bucket_multiplier,
                latest.base_quantity,
                ReasonCode.CALIBRATION_REGIME_SHIFT,
                len(ready),
                None,
            )
    spreads_24h = tuple(item.spread_bps for item in current_24h if item.spread_bps is not None)
    costs_24h = tuple(
        item.stressed_cost_floor_bps
        for item in current_24h
        if item.stressed_cost_floor_bps is not None
    )
    ticks_24h = tuple(
        item.normalized_tick_bps for item in current_24h if item.normalized_tick_bps is not None
    )
    notionals_24h = tuple(
        item.notional_usdt for item in current_24h if item.notional_usdt is not None
    )
    if not costs_24h or not ticks_24h or not notionals_24h:
        return RouteCalibrationAssessment(
            latest.route,
            latest.size_bucket_multiplier,
            latest.base_quantity,
            ReasonCode.CALIBRATION_INSUFFICIENT,
            len(ready),
            None,
        )
    cost_floor = _quantile(costs_24h, Decimal("0.90"))
    normalized_tick = max(ticks_24h)
    notional = _quantile(notionals_24h, Decimal("0.50"))
    if notional <= 0:
        raise ValueError("calibration notional must be positive")
    minimum_profit_bps = minimum_profit_usdt / notional * Decimal(10_000)
    q90 = _quantile(spreads_24h, Decimal("0.90"))
    episode_peaks = tuple(
        episode.peak_spread_bps
        for item in historical_30d
        for episode in _episode_samples_for_observation(item)
    )
    long_tail_spreads = (
        tuple(item.spread_bps for item in current_30d if item.spread_bps is not None)
        + episode_peaks
    )
    q999 = _quantile(long_tail_spreads, Decimal("0.999"))
    q75_adverse = _quantile(adverse, Decimal("0.75"))
    convergence_p90 = _quantile(convergence, Decimal("0.90"))
    raw_entry = max(
        q90,
        window_24h.median_spread_bps + Decimal("2.5") * robust_sigma,
        Decimal(2) * cost_floor + minimum_profit_bps,
    )
    raw_step = max(
        Decimal("1.25") * robust_sigma,
        q75_adverse,
        cost_floor,
        normalized_tick,
    )
    prior_long_tail = previous.long_tail_q999_spread_bps if previous is not None else q999
    episode_peak = max(episode_peaks, default=q999)
    long_tail = max(q999, episode_peak, prior_long_tail)
    raw_stop = max(long_tail, raw_entry + Decimal(5) * raw_step) + Decimal("0.5") * raw_step
    if previous is None:
        version = 1
        anchor_at = now
        anchor_entry = raw_entry
        anchor_step = raw_step
        anchor_stop = raw_stop
        entry = raw_entry
        step = raw_step
        stop = raw_stop
    else:
        version = previous.version + 1
        if now - previous.change_anchor_at >= timedelta(hours=24):
            anchor_at = now
            anchor_entry = previous.entry_levels_bps[0]
            anchor_step = previous.grid_step_bps
            anchor_stop = previous.route_stop_bps
        else:
            anchor_at = previous.change_anchor_at
            anchor_entry = previous.change_anchor_entry_level_1_bps
            anchor_step = previous.change_anchor_grid_step_bps
            anchor_stop = previous.change_anchor_route_stop_bps
        entry = _bound_from_anchor(raw_entry, anchor_entry, parameter_change_limit_ratio_per_day)
        step = _bound_from_anchor(raw_step, anchor_step, parameter_change_limit_ratio_per_day)
        stop = _bound_from_anchor(raw_stop, anchor_stop, parameter_change_limit_ratio_per_day)
    levels = tuple(entry + Decimal(index) * step for index in range(5))
    convergence_by_spread_bucket = _convergence_by_spread_bucket(
        historical_30d,
        levels,  # type: ignore[arg-type]
        minimum_convergence_samples_per_spread_bucket,
    )
    parameters = RouteCalibrationParameters(
        latest.route,
        latest.size_bucket_multiplier,
        latest.base_quantity,  # type: ignore[arg-type]
        latest.epoch_id,
        version,
        now,
        len(ready),
        int(period.total_seconds()),
        window_24h,
        window_7d,
        window_30d,
        robust_sigma,
        q90,
        q999,
        q75_adverse,
        convergence_p90,
        convergence_by_spread_bucket,
        cost_floor,
        minimum_profit_usdt,
        minimum_profit_bps,
        normalized_tick,
        raw_entry,
        raw_step,
        raw_stop,
        levels,  # type: ignore[arg-type]
        step,
        stop,
        min(
            window_24h.median_spread_bps + Decimal("0.5") * robust_sigma,
            entry - max(step, cost_floor + minimum_profit_bps),
        ),
        long_tail,
        anchor_at,
        anchor_entry,
        anchor_step,
        anchor_stop,
    )
    if entry < raw_entry or step < raw_step or stop < raw_stop:
        return RouteCalibrationAssessment(
            latest.route,
            latest.size_bucket_multiplier,
            latest.base_quantity,
            ReasonCode.CALIBRATION_REGIME_SHIFT,
            len(ready),
            None,
            parameters,
        )
    return RouteCalibrationAssessment(
        latest.route,
        latest.size_bucket_multiplier,
        latest.base_quantity,
        ReasonCode.QUOTE_READY,
        len(ready),
        parameters,
    )


def _observation_payload(observation: RouteCalibrationObservation) -> str:
    return json.dumps(asdict(observation), default=str, sort_keys=True, separators=(",", ":"))


def _route_from_value(value: str) -> DirectedRouteKey:
    base, venues = value.split(":", 1)
    long_venue, short_venue = venues.split(">", 1)
    return DirectedRouteKey(base, Venue(long_venue), Venue(short_venue))


def _observation_from_payload(payload: str) -> RouteCalibrationObservation:
    value = json.loads(payload)
    parsed_episode_samples: list[RouteCalibrationEpisodeSample] = []
    for item in value.get("episode_samples", ()):
        censored = item.get("censored", False)
        if not isinstance(censored, bool):
            raise ValueError("calibration episode censored flag must be boolean")
        parsed_episode_samples.append(
            RouteCalibrationEpisodeSample(
                Decimal(str(item["entry_spread_bps"])),
                Decimal(str(item["peak_spread_bps"])),
                Decimal(str(item["convergence_seconds"])),
                int(item["spread_bucket_index"])
                if item.get("spread_bucket_index") is not None
                else None,
                censored,
            )
        )
    episode_samples = tuple(parsed_episode_samples)
    return RouteCalibrationObservation(
        _route_from_value(
            str(value["route"]["base"])
            + ":"
            + str(value["route"]["long_venue"])
            + ">"
            + str(value["route"]["short_venue"])
        ),
        Decimal(str(value["size_bucket_multiplier"])),
        Decimal(str(value["base_quantity"])) if value["base_quantity"] is not None else None,
        str(value["epoch_id"]),
        datetime.fromisoformat(str(value["observed_at"])),
        Decimal(str(value["spread_bps"])) if value["spread_bps"] is not None else None,
        Decimal(str(value["adverse_excursion_after_entry_bps"]))
        if value["adverse_excursion_after_entry_bps"] is not None
        else None,
        Decimal(str(value["convergence_seconds"]))
        if value["convergence_seconds"] is not None
        else None,
        Decimal(str(value["stressed_cost_floor_bps"]))
        if value["stressed_cost_floor_bps"] is not None
        else None,
        Decimal(str(value["normalized_tick_bps"]))
        if value["normalized_tick_bps"] is not None
        else None,
        Decimal(str(value["notional_usdt"])) if value["notional_usdt"] is not None else None,
        Decimal(str(value["funding_rate_delta"]))
        if value["funding_rate_delta"] is not None
        else None,
        Decimal(str(value["exit_depth_multiple"]))
        if value["exit_depth_multiple"] is not None
        else None,
        ReasonCode(str(value["reason"])),
        Decimal(str(value["episode_peak_spread_bps"]))
        if value.get("episode_peak_spread_bps") is not None
        else None,
        Decimal(str(value["episode_entry_spread_bps"]))
        if value.get("episode_entry_spread_bps") is not None
        else None,
        episode_samples,
    )


def _is_idempotent_observation_retry(
    stored: RouteCalibrationObservation,
    incoming: RouteCalibrationObservation,
) -> bool:
    if stored == incoming:
        return True
    return (
        stored.route == incoming.route
        and stored.size_bucket_multiplier == incoming.size_bucket_multiplier
        and stored.base_quantity == incoming.base_quantity
        and stored.epoch_id == incoming.epoch_id
        and stored.observed_at == incoming.observed_at
        and stored.spread_bps == incoming.spread_bps
        and stored.stressed_cost_floor_bps == incoming.stressed_cost_floor_bps
        and stored.normalized_tick_bps == incoming.normalized_tick_bps
        and stored.notional_usdt == incoming.notional_usdt
        and stored.funding_rate_delta == incoming.funding_rate_delta
        and stored.exit_depth_multiple == incoming.exit_depth_multiple
        and stored.reason == incoming.reason
        and (
            incoming.episode_peak_spread_bps is None
            or stored.episode_peak_spread_bps == incoming.episode_peak_spread_bps
        )
        and (
            incoming.episode_entry_spread_bps is None
            or stored.episode_entry_spread_bps == incoming.episode_entry_spread_bps
        )
        and (
            incoming.adverse_excursion_after_entry_bps is None
            or stored.adverse_excursion_after_entry_bps
            == incoming.adverse_excursion_after_entry_bps
        )
        and (
            incoming.convergence_seconds is None
            or stored.convergence_seconds == incoming.convergence_seconds
        )
        and (not incoming.episode_samples or stored.episode_samples == incoming.episode_samples)
    )


def _parameter_payload(parameters: RouteCalibrationParameters) -> str:
    return json.dumps(asdict(parameters), default=str, sort_keys=True, separators=(",", ":"))


def _parameter_from_payload(payload: str) -> RouteCalibrationParameters:
    value: dict[str, Any] = json.loads(payload)

    def window(name: str) -> CalibrationWindow:
        item = value[name]
        return CalibrationWindow(
            int(item["hours"]),
            int(item["sample_count"]),
            int(item["observation_period_seconds"]),
            bool(item["complete"]),
            Decimal(str(item["median_spread_bps"])),
            Decimal(str(item["mad_spread_bps"])),
            Decimal(str(item["q99_spread_bps"])),
            Decimal(str(item["q90_stressed_cost_floor_bps"])),
            Decimal(str(item["q10_exit_depth_multiple"])),
            Decimal(str(item["q90_absolute_funding_rate_delta"])),
        )

    def convergence_buckets() -> tuple[
        SpreadBucketConvergence,
        SpreadBucketConvergence,
        SpreadBucketConvergence,
        SpreadBucketConvergence,
        SpreadBucketConvergence,
    ]:
        stored = value.get("convergence_by_spread_bucket")
        entry_levels = tuple(Decimal(str(item)) for item in value["entry_levels_bps"])
        if stored is None:
            items = tuple(
                SpreadBucketConvergence(
                    bucket_index=index,
                    lower_bound_bps=lower_bound,
                    upper_bound_bps=(entry_levels[index + 1] if index < 4 else None),
                    sample_count=0,
                    minimum_sample_count=MINIMUM_SPREAD_BUCKET_CONVERGENCE_SAMPLES,
                    convergence_p90_seconds=None,
                )
                for index, lower_bound in enumerate(entry_levels)
            )
        else:
            items = tuple(
                SpreadBucketConvergence(
                    bucket_index=int(item["bucket_index"]),
                    lower_bound_bps=Decimal(str(item["lower_bound_bps"])),
                    upper_bound_bps=(
                        Decimal(str(item["upper_bound_bps"]))
                        if item["upper_bound_bps"] is not None
                        else None
                    ),
                    sample_count=int(item["sample_count"]),
                    minimum_sample_count=int(
                        item.get(
                            "minimum_sample_count",
                            MINIMUM_SPREAD_BUCKET_CONVERGENCE_SAMPLES,
                        )
                    ),
                    convergence_p90_seconds=(
                        Decimal(str(item["convergence_p90_seconds"]))
                        if item["convergence_p90_seconds"] is not None
                        else None
                    ),
                )
                for item in stored
            )
        return items  # type: ignore[return-value]

    return RouteCalibrationParameters(
        route=_route_from_value(
            str(value["route"]["base"])
            + ":"
            + str(value["route"]["long_venue"])
            + ">"
            + str(value["route"]["short_venue"])
        ),
        size_bucket_multiplier=Decimal(str(value["size_bucket_multiplier"])),
        latest_base_quantity=Decimal(str(value["latest_base_quantity"])),
        epoch_id=str(value["epoch_id"]),
        version=int(value["version"]),
        calibrated_at=datetime.fromisoformat(str(value["calibrated_at"])).astimezone(UTC),
        sample_count=int(value["sample_count"]),
        observation_period_seconds=int(value["observation_period_seconds"]),
        window_24h=window("window_24h"),
        window_7d=window("window_7d"),
        window_30d=window("window_30d"),
        robust_sigma_bps=Decimal(str(value["robust_sigma_bps"])),
        q90_spread_bps=Decimal(str(value["q90_spread_bps"])),
        q999_spread_bps=Decimal(str(value["q999_spread_bps"])),
        q75_adverse_excursion_bps=Decimal(str(value["q75_adverse_excursion_bps"])),
        convergence_p90_seconds=Decimal(str(value["convergence_p90_seconds"])),
        convergence_by_spread_bucket=convergence_buckets(),
        stressed_cost_floor_bps=Decimal(str(value["stressed_cost_floor_bps"])),
        minimum_profit_usdt=Decimal(str(value["minimum_profit_usdt"])),
        minimum_profit_bps=Decimal(str(value["minimum_profit_bps"])),
        normalized_tick_bps=Decimal(str(value["normalized_tick_bps"])),
        raw_entry_level_1_bps=Decimal(str(value["raw_entry_level_1_bps"])),
        raw_grid_step_bps=Decimal(str(value["raw_grid_step_bps"])),
        raw_route_stop_bps=Decimal(str(value["raw_route_stop_bps"])),
        entry_levels_bps=tuple(Decimal(str(item)) for item in value["entry_levels_bps"]),  # type: ignore[arg-type]
        grid_step_bps=Decimal(str(value["grid_step_bps"])),
        route_stop_bps=Decimal(str(value["route_stop_bps"])),
        target_close_reference_bps=Decimal(str(value["target_close_reference_bps"])),
        long_tail_q999_spread_bps=Decimal(str(value["long_tail_q999_spread_bps"])),
        change_anchor_at=datetime.fromisoformat(str(value["change_anchor_at"])).astimezone(UTC),
        change_anchor_entry_level_1_bps=Decimal(str(value["change_anchor_entry_level_1_bps"])),
        change_anchor_grid_step_bps=Decimal(str(value["change_anchor_grid_step_bps"])),
        change_anchor_route_stop_bps=Decimal(str(value["change_anchor_route_stop_bps"])),
        cost_model_scope=str(value.get("cost_model_scope", "PUBLIC_SHADOW_ESTIMATE")),
    )


class PersistentRouteCalibrator:
    def __init__(
        self,
        path: Path,
        *,
        minimum_samples: int,
        minimum_observation_period: timedelta,
        minimum_profit_usdt: Decimal,
        parameter_change_limit_ratio_per_day: Decimal,
        maximum_inter_observation_gap: timedelta = timedelta(seconds=60),
        maximum_observations_per_key: int = 70_000,
        maximum_route_size_keys: int = 10_000,
        sampling_policy: RouteCalibrationSamplingPolicy | None = None,
        maximum_l2_age_ms: int | None = None,
        minimum_convergence_samples_per_spread_bucket: int = (
            MINIMUM_SPREAD_BUCKET_CONVERGENCE_SAMPLES
        ),
    ) -> None:
        if maximum_observations_per_key < minimum_samples:
            raise ValueError("calibration retention must cover the minimum sample count")
        if maximum_route_size_keys < 1:
            raise ValueError("calibration route-size key limit must be positive")
        if maximum_l2_age_ms is not None and maximum_l2_age_ms <= 0:
            raise ValueError("calibration L2 maximum age must be positive")
        if minimum_convergence_samples_per_spread_bucket < 3:
            raise ValueError("spread bucket convergence requires at least three episode samples")
        self._path = path
        self._minimum_samples = minimum_samples
        self._minimum_period = minimum_observation_period
        self._minimum_profit = minimum_profit_usdt
        self._change_limit = parameter_change_limit_ratio_per_day
        if maximum_inter_observation_gap.total_seconds() <= 0:
            raise ValueError("calibration maximum observation gap must be positive")
        self._maximum_gap = maximum_inter_observation_gap
        self._maximum_observations = maximum_observations_per_key
        self._maximum_keys = maximum_route_size_keys
        self._minimum_convergence_samples_per_spread_bucket = (
            minimum_convergence_samples_per_spread_bucket
        )
        self._maximum_holding_seconds = (
            sampling_policy.maximum_holding_seconds if sampling_policy is not None else None
        )
        policy = {
            "minimum_samples": minimum_samples,
            "minimum_period_seconds": int(minimum_observation_period.total_seconds()),
            "minimum_profit_usdt": str(minimum_profit_usdt),
            "change_limit": str(parameter_change_limit_ratio_per_day),
            "maximum_gap_seconds": int(maximum_inter_observation_gap.total_seconds()),
            "algorithm_version": 2,
            "maximum_l2_age_ms": maximum_l2_age_ms,
            "minimum_convergence_samples_per_spread_bucket": (
                minimum_convergence_samples_per_spread_bucket
            ),
            "sampling_policy": (
                {
                    "size_multipliers": tuple(
                        str(value) for value in sampling_policy.size_multipliers
                    ),
                    "maximum_holding_seconds": sampling_policy.maximum_holding_seconds,
                    "funding_maximum_age_seconds": (sampling_policy.funding_maximum_age_seconds),
                    "funding_stress_multiplier": str(sampling_policy.funding_stress_multiplier),
                    "latency_reserve_bps": str(sampling_policy.latency_reserve_bps),
                    "partial_fill_reserve_bps": str(sampling_policy.partial_fill_reserve_bps),
                    "emergency_hedge_reserve_bps": str(sampling_policy.emergency_hedge_reserve_bps),
                    "reconciliation_forced_exit_reserve_bps": str(
                        sampling_policy.reconciliation_forced_exit_reserve_bps
                    ),
                }
                if sampling_policy is not None
                else None
            ),
        }
        self._policy_fingerprint = hashlib.sha256(
            json.dumps(policy, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        self._lifecycle_lock = asyncio.Lock()
        self._record_lock = asyncio.Lock()
        self._closed = False
        self._workers: dict[asyncio.Task[Any], threading.Event | None] = {}
        self._rebuild_attempted_at: dict[tuple[str, str], datetime] = {}

    async def initialise(self) -> None:
        async with self._lifecycle_lock:
            self._require_open()
            worker = asyncio.create_task(
                initialise_state(self._path),
                name="route-calibration-initialise",
            )
            self._workers[worker] = None
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError:
            worker.add_done_callback(self._consume_worker)
            raise
        finally:
            if worker.done():
                self._workers.pop(worker, None)
        async with self._lifecycle_lock:
            self._require_open()

    async def current_epoch_id(self, observed_at: datetime) -> str:
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("calibration epoch time must be timezone-aware")
        observed_at = observed_at.astimezone(UTC)
        abort = threading.Event()
        async with self._lifecycle_lock:
            self._require_open()
            worker = asyncio.create_task(
                asyncio.to_thread(self._current_epoch_id_sync, observed_at, abort),
                name="route-calibration-epoch",
            )
            self._workers[worker] = abort
        try:
            epoch_id = await asyncio.shield(worker)
        except asyncio.CancelledError:
            abort.set()
            worker.add_done_callback(self._consume_worker)
            raise
        finally:
            if worker.done():
                self._workers.pop(worker, None)
        async with self._lifecycle_lock:
            self._require_open()
        return epoch_id

    async def record_many(
        self,
        observations: tuple[RouteCalibrationObservation, ...],
        *,
        now: datetime | None = None,
    ) -> tuple[RouteCalibrationAssessment, ...]:
        async with self._record_lock:
            return await self._record_many_serialized(observations, now=now)

    async def assess_current(
        self,
        observations: tuple[RouteCalibrationObservation, ...],
    ) -> tuple[RouteCalibrationAssessment, ...]:
        """Apply a read-only current-point gate without rebuilding rolling windows."""
        if not observations:
            return ()
        async with self._lifecycle_lock:
            self._require_open()
            worker = asyncio.create_task(
                asyncio.to_thread(self._assess_current_sync, observations),
                name="route-calibration-current-gate",
            )
            self._workers[worker] = None
        try:
            assessments = await asyncio.shield(worker)
        except asyncio.CancelledError:
            worker.add_done_callback(self._consume_worker)
            raise
        finally:
            if worker.done():
                self._workers.pop(worker, None)
        async with self._lifecycle_lock:
            self._require_open()
        return assessments

    async def _record_many_serialized(
        self,
        observations: tuple[RouteCalibrationObservation, ...],
        *,
        now: datetime | None = None,
    ) -> tuple[RouteCalibrationAssessment, ...]:
        effective_now = now or datetime.now(UTC)
        if effective_now.tzinfo is None or effective_now.utcoffset() is None:
            raise ValueError("calibration write time must be timezone-aware")
        effective_now = effective_now.astimezone(UTC)
        abort = threading.Event()
        async with self._lifecycle_lock:
            self._require_open()
            worker = asyncio.create_task(
                asyncio.to_thread(
                    self._record_many_sync,
                    observations,
                    effective_now,
                    abort,
                ),
                name="route-calibration-sqlite",
            )
            self._workers[worker] = abort
        try:
            assessments = await asyncio.shield(worker)
        except asyncio.CancelledError:
            abort.set()
            worker.add_done_callback(self._consume_worker)
            raise
        finally:
            if worker.done():
                self._workers.pop(worker, None)
        async with self._lifecycle_lock:
            self._require_open()
        return assessments

    async def close(self, timeout_seconds: float = 1.0) -> None:
        if timeout_seconds <= 0:
            raise ValueError("route calibration shutdown timeout must be positive")
        async with self._lifecycle_lock:
            self._closed = True
            workers = tuple(self._workers)
            for abort in self._workers.values():
                if abort is not None:
                    abort.set()
        if not workers:
            return
        _done, pending = await asyncio.wait(
            workers,
            timeout=timeout_seconds,
        )
        for task in workers:
            if task.done():
                self._workers.pop(task, None)
                self._consume_worker(task)
        if pending:
            raise RuntimeError("route calibration shutdown deadline exceeded")

    def _consume_worker(
        self,
        task: asyncio.Task[Any],
    ) -> None:
        self._workers.pop(task, None)
        with suppress(asyncio.CancelledError, Exception):
            task.result()

    async def latest(
        self,
        route: DirectedRouteKey,
        size_bucket_multiplier: Decimal,
    ) -> RouteCalibrationParameters | None:
        async with self._lifecycle_lock:
            self._require_open()
            worker = asyncio.create_task(
                asyncio.to_thread(self._latest_sync, route, size_bucket_multiplier),
                name="route-calibration-latest",
            )
            self._workers[worker] = None
        try:
            parameters = await asyncio.shield(worker)
        except asyncio.CancelledError:
            worker.add_done_callback(self._consume_worker)
            raise
        finally:
            if worker.done():
                self._workers.pop(worker, None)
        async with self._lifecycle_lock:
            self._require_open()
        return parameters

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("route calibrator is closed")

    def _connect(self) -> sqlite3.Connection:
        database = sqlite3.connect(self._path, timeout=1)
        database.execute("PRAGMA busy_timeout=1000")
        database.execute("PRAGMA foreign_keys=ON")
        return database

    def _latest_in_database(
        self,
        database: sqlite3.Connection,
        route: DirectedRouteKey,
        size: Decimal,
    ) -> RouteCalibrationParameters | None:
        row = database.execute(
            """
            SELECT payload_json FROM route_calibration_parameters
            WHERE route = ? AND size_bucket_multiplier = ?
            """,
            (route.value, str(size)),
        ).fetchone()
        return _parameter_from_payload(str(row[0])) if row is not None else None

    @staticmethod
    def _parameter_status_in_database(
        database: sqlite3.Connection,
        route: DirectedRouteKey,
        size: Decimal,
    ) -> tuple[bool, bool]:
        row = database.execute(
            """
            SELECT active, transient_blocked FROM route_calibration_parameters
            WHERE route = ? AND size_bucket_multiplier = ?
            """,
            (route.value, str(size)),
        ).fetchone()
        if row is None:
            return False, False
        return int(row[0]) == 1, int(row[1]) == 1

    def _latest_sync(
        self,
        route: DirectedRouteKey,
        size: Decimal,
    ) -> RouteCalibrationParameters | None:
        with self._connect() as database:
            row = database.execute(
                """
                SELECT parameters.payload_json, runtime.epoch_id, parameters.updated_at,
                       segment.ready_sample_count, segment.segment_started_at,
                       segment.last_observed_at
                FROM route_calibration_parameters AS parameters
                JOIN route_calibration_runtime AS runtime ON runtime.singleton = 1
                JOIN route_calibration_segments AS segment
                  ON segment.route = parameters.route
                 AND segment.size_bucket_multiplier = parameters.size_bucket_multiplier
                 AND segment.epoch_id = runtime.epoch_id
                WHERE parameters.route = ?
                  AND parameters.size_bucket_multiplier = ?
                  AND runtime.policy_fingerprint = ?
                  AND parameters.active = 1
                  AND segment.last_reason = ?
                """,
                (
                    route.value,
                    str(size),
                    self._policy_fingerprint,
                    ReasonCode.QUOTE_READY.value,
                ),
            ).fetchone()
            if row is None:
                return None
            parameters = _parameter_from_payload(str(row[0]))
            segment_started = (
                datetime.fromisoformat(str(row[4])).astimezone(UTC) if row[4] is not None else None
            )
            segment_last = datetime.fromisoformat(str(row[5])).astimezone(UTC)
            parameters_updated = datetime.fromisoformat(str(row[2])).astimezone(UTC)
            ready = (
                parameters.epoch_id == str(row[1])
                and int(row[3]) >= self._minimum_samples
                and segment_started is not None
                and segment_last - segment_started >= self._minimum_period
                and parameters_updated >= segment_started
            )
            return parameters if ready else None

    def _assess_current_sync(
        self,
        observations: tuple[RouteCalibrationObservation, ...],
    ) -> tuple[RouteCalibrationAssessment, ...]:
        keyed: dict[tuple[DirectedRouteKey, Decimal], RouteCalibrationObservation] = {}
        for observation in observations:
            key = observation.route, observation.size_bucket_multiplier
            existing = keyed.get(key)
            if existing is not None and existing != observation:
                raise ValueError("current calibration gate requires one observation per key")
            keyed[key] = observation
        assessments: list[RouteCalibrationAssessment] = []
        with self._connect() as database:
            runtime = database.execute(
                """
                SELECT epoch_id, policy_fingerprint
                FROM route_calibration_runtime WHERE singleton = 1
                """
            ).fetchone()
            current_epoch = str(runtime[0]) if runtime is not None else None
            policy_matches = runtime is not None and str(runtime[1]) == self._policy_fingerprint
            for (route, size), observation in sorted(
                keyed.items(),
                key=lambda item: (item[0][0].value, item[0][1]),
            ):
                reason = _effective_observation_reason(observation)
                row = database.execute(
                    """
                    SELECT parameters.payload_json, parameters.active,
                           parameters.transient_blocked, segment.ready_sample_count,
                           segment.last_observed_at
                    FROM route_calibration_parameters AS parameters
                    LEFT JOIN route_calibration_segments AS segment
                      ON segment.route = parameters.route
                     AND segment.size_bucket_multiplier = parameters.size_bucket_multiplier
                     AND segment.epoch_id = ?
                    WHERE parameters.route = ? AND parameters.size_bucket_multiplier = ?
                    """,
                    (observation.epoch_id, route.value, str(size)),
                ).fetchone()
                sample_count = int(row[3]) if row is not None and row[3] is not None else 0
                parameters: RouteCalibrationParameters | None = None
                if (
                    reason == ReasonCode.QUOTE_READY
                    and policy_matches
                    and current_epoch == observation.epoch_id
                    and row is not None
                    and int(row[1]) == 1
                    and int(row[2]) == 0
                ):
                    previous = _parameter_from_payload(str(row[0]))
                    last_observed_at = (
                        datetime.fromisoformat(str(row[4])).astimezone(UTC)
                        if row[4] is not None
                        else None
                    )
                    if previous.epoch_id != observation.epoch_id:
                        reason = ReasonCode.CALIBRATION_INSUFFICIENT
                    elif last_observed_at is not None and (
                        observation.observed_at < last_observed_at
                        or observation.observed_at - last_observed_at > self._maximum_gap
                    ):
                        reason = ReasonCode.BOOK_STALE
                    elif (
                        last_observed_at is not None and observation.observed_at == last_observed_at
                    ):
                        stored_row = database.execute(
                            """
                            SELECT payload_json FROM route_calibration_observations
                            WHERE route = ? AND size_bucket_multiplier = ?
                              AND epoch_id = ? AND observed_at = ?
                            """,
                            (
                                route.value,
                                str(size),
                                observation.epoch_id,
                                observation.observed_at.isoformat(),
                            ),
                        ).fetchone()
                        if stored_row is None or not _is_idempotent_observation_retry(
                            _observation_from_payload(str(stored_row[0])),
                            observation,
                        ):
                            reason = ReasonCode.CALIBRATION_REGIME_SHIFT
                        else:
                            reason = _incremental_safety_reason(
                                observation,
                                previous,
                                self._minimum_profit,
                            )
                    else:
                        episodes = database.execute(
                            """
                            SELECT convergence_target_bps, started_at
                            FROM route_calibration_episodes
                            WHERE route = ? AND size_bucket_multiplier = ? AND epoch_id = ?
                            """,
                            (route.value, str(size), observation.epoch_id),
                        ).fetchall()
                        if (
                            episodes
                            and self._maximum_holding_seconds is not None
                            and observation.spread_bps is not None
                            and any(
                                observation.spread_bps > Decimal(str(episode[0]))
                                and observation.observed_at
                                - datetime.fromisoformat(str(episode[1])).astimezone(UTC)
                                >= timedelta(seconds=self._maximum_holding_seconds)
                                for episode in episodes
                            )
                        ):
                            reason = ReasonCode.CALIBRATION_REGIME_SHIFT
                        else:
                            reason = _incremental_safety_reason(
                                observation,
                                previous,
                                self._minimum_profit,
                            )
                    if reason == ReasonCode.QUOTE_READY:
                        assert observation.base_quantity is not None
                        parameters = replace(
                            previous,
                            latest_base_quantity=observation.base_quantity,
                            sample_count=max(previous.sample_count, sample_count),
                        )
                elif reason == ReasonCode.QUOTE_READY:
                    reason = ReasonCode.CALIBRATION_INSUFFICIENT
                assessments.append(
                    RouteCalibrationAssessment(
                        route,
                        size,
                        observation.base_quantity,
                        reason,
                        sample_count,
                        parameters,
                    )
                )
        return tuple(assessments)

    def _current_epoch_id_sync(
        self,
        observed_at: datetime,
        abort: threading.Event,
    ) -> str:
        if abort.is_set():
            raise RuntimeError("route calibration epoch update aborted")
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            row = database.execute(
                """
                SELECT epoch_id, policy_fingerprint, last_observed_at
                FROM route_calibration_runtime WHERE singleton = 1
                """
            ).fetchone()
            last = (
                datetime.fromisoformat(str(row[2])).astimezone(UTC)
                if row is not None and row[2] is not None
                else None
            )
            reusable = (
                row is not None
                and str(row[1]) == self._policy_fingerprint
                and (
                    last is None
                    or (observed_at >= last and observed_at - last <= self._maximum_gap)
                )
            )
            epoch_id = str(row[0]) if reusable else uuid4().hex
            if not reusable:
                database.execute(
                    """
                    UPDATE route_calibration_parameters
                    SET active = 0, transient_blocked = 0
                    """
                )
            database.execute(
                """
                INSERT INTO route_calibration_runtime(
                    singleton, epoch_id, policy_fingerprint, last_observed_at
                ) VALUES (1, ?, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    epoch_id = excluded.epoch_id,
                    policy_fingerprint = excluded.policy_fingerprint,
                    last_observed_at = excluded.last_observed_at
                """,
                (
                    epoch_id,
                    self._policy_fingerprint,
                    last.isoformat() if reusable and last is not None else None,
                ),
            )
            if abort.is_set():
                database.rollback()
                raise RuntimeError("route calibration epoch update aborted")
            database.commit()
            return epoch_id

    def _record_many_sync(
        self,
        observations: tuple[RouteCalibrationObservation, ...],
        now: datetime,
        abort: threading.Event,
    ) -> tuple[RouteCalibrationAssessment, ...]:
        if not observations:
            return ()
        if abort.is_set():
            raise RuntimeError("route calibration write aborted")
        grouped: dict[tuple[DirectedRouteKey, Decimal, str], list[RouteCalibrationObservation]] = {}
        for observation in observations:
            grouped.setdefault(
                (observation.route, observation.size_bucket_multiplier, observation.epoch_id),
                [],
            ).append(observation)
        epochs = {item.epoch_id for item in observations}
        if len(epochs) != 1:
            raise ValueError("one calibration batch must use one current epoch")
        incoming_epoch = next(iter(epochs))
        current_keys = {(route.value, str(size)) for route, size, _epoch in grouped}
        if len(current_keys) > self._maximum_keys:
            raise ValueError("calibration batch exceeds route-size key limit")
        assessments: list[RouteCalibrationAssessment] = []
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            runtime = database.execute(
                """
                SELECT epoch_id, policy_fingerprint, last_observed_at
                FROM route_calibration_runtime WHERE singleton = 1
                """
            ).fetchone()
            if runtime is None:
                database.execute(
                    """
                    INSERT INTO route_calibration_runtime(
                        singleton, epoch_id, policy_fingerprint, last_observed_at
                    ) VALUES (1, ?, ?, NULL)
                    """,
                    (incoming_epoch, self._policy_fingerprint),
                )
            elif str(runtime[0]) != incoming_epoch:
                raise ValueError("calibration observation epoch is not current")
            elif str(runtime[1]) != self._policy_fingerprint:
                raise ValueError("calibration policy fingerprint changed")
            ordered_keys = tuple(grouped)
            self._rebuild_attempted_at = {
                key: attempted
                for key, attempted in self._rebuild_attempted_at.items()
                if key in current_keys
            }
            previous_by_key = {
                key: self._latest_in_database(database, key[0], key[1]) for key in ordered_keys
            }
            previous_status_by_key = {
                key: self._parameter_status_in_database(database, key[0], key[1])
                for key in ordered_keys
            }
            persisted_history_by_key = {
                key: database.execute(
                    """
                    SELECT 1 FROM route_calibration_observations
                    WHERE route = ? AND size_bucket_multiplier = ? AND epoch_id = ?
                    LIMIT 1
                    """,
                    (key[0].value, str(key[1]), key[2]),
                ).fetchone()
                is not None
                for key in ordered_keys
            }
            due_candidates: list[tuple[DirectedRouteKey, Decimal, str]] = []
            for key in ordered_keys:
                prior = previous_by_key[key]
                if persisted_history_by_key[key] and (
                    prior is None
                    or not previous_status_by_key[key][0]
                    or prior.epoch_id != key[2]
                    or now < prior.calibrated_at
                    or now - prior.calibrated_at >= timedelta(seconds=60)
                ):
                    due_candidates.append(key)

            def rebuild_priority(
                key: tuple[DirectedRouteKey, Decimal, str],
            ) -> tuple[datetime, str, Decimal]:
                prior = previous_by_key[key]
                last_success = (
                    prior.calibrated_at if prior is not None else datetime.min.replace(tzinfo=UTC)
                )
                last_attempt = self._rebuild_attempted_at.get(
                    (key[0].value, str(key[1])),
                    datetime.min.replace(tzinfo=UTC),
                )
                return max(last_attempt, last_success), key[0].value, key[1]

            due_keys = tuple(
                sorted(
                    due_candidates,
                    key=rebuild_priority,
                )
            )
            rebuild_keys = set(due_keys[:3])
            for route, size, _epoch in rebuild_keys:
                self._rebuild_attempted_at[(route.value, str(size))] = now
            for (route, size, epoch_id), incoming in grouped.items():
                if abort.is_set():
                    database.rollback()
                    raise RuntimeError("route calibration write aborted")
                database.execute(
                    """
                    DELETE FROM route_calibration_episodes
                    WHERE route = ? AND size_bucket_multiplier = ? AND epoch_id <> ?
                    """,
                    (route.value, str(size), epoch_id),
                )
                database.execute(
                    """
                    DELETE FROM route_calibration_segments
                    WHERE route = ? AND size_bucket_multiplier = ? AND epoch_id <> ?
                    """,
                    (route.value, str(size), epoch_id),
                )
                history_rows = database.execute(
                    """
                    SELECT payload_json FROM route_calibration_observations
                    WHERE route = ? AND size_bucket_multiplier = ? AND epoch_id = ?
                    ORDER BY observed_at DESC, observation_id DESC LIMIT 16
                    """,
                    (route.value, str(size), epoch_id),
                ).fetchall()
                had_persisted_history = persisted_history_by_key[(route, size, epoch_id)]
                history = [_observation_from_payload(str(row[0])) for row in reversed(history_rows)]
                segment_row = database.execute(
                    """
                    SELECT ready_sample_count, segment_started_at, last_observed_at, last_reason
                    FROM route_calibration_segments
                    WHERE route = ? AND size_bucket_multiplier = ? AND epoch_id = ?
                    """,
                    (route.value, str(size), epoch_id),
                ).fetchone()
                ready_count = int(segment_row[0]) if segment_row is not None else 0
                segment_started_at = (
                    datetime.fromisoformat(str(segment_row[1])).astimezone(UTC)
                    if segment_row is not None and segment_row[1] is not None
                    else None
                )
                segment_last_at = (
                    datetime.fromisoformat(str(segment_row[2])).astimezone(UTC)
                    if segment_row is not None
                    else None
                )
                segment_last_reason = (
                    ReasonCode(str(segment_row[3]))
                    if segment_row is not None
                    else ReasonCode.CALIBRATION_INSUFFICIENT
                )
                for observation in sorted(incoming, key=lambda item: item.observed_at):
                    duplicate = database.execute(
                        """
                        SELECT observation_id, payload_json
                        FROM route_calibration_observations
                        WHERE route = ? AND size_bucket_multiplier = ?
                          AND epoch_id = ? AND observed_at = ?
                        """,
                        (
                            route.value,
                            str(size),
                            epoch_id,
                            observation.observed_at.isoformat(),
                        ),
                    ).fetchone()
                    if duplicate is not None:
                        stored = _observation_from_payload(str(duplicate[1]))
                        if _is_idempotent_observation_retry(stored, observation):
                            continue
                        if not history or history[-1].observed_at != observation.observed_at:
                            raise ValueError("conflicting historical calibration observation")
                        if (
                            stored.reason == ReasonCode.QUOTE_READY
                            and observation.reason == ReasonCode.QUOTE_READY
                        ):
                            raise ValueError("conflicting calibration observation")
                        invalid = (
                            observation if observation.reason != ReasonCode.QUOTE_READY else stored
                        )
                        database.execute(
                            """
                            UPDATE route_calibration_observations
                            SET reason = ?, payload_json = ?
                            WHERE observation_id = ?
                            """,
                            (
                                invalid.reason.value,
                                _observation_payload(invalid),
                                int(duplicate[0]),
                            ),
                        )
                        database.execute(
                            """
                            DELETE FROM route_calibration_episodes
                            WHERE route = ? AND size_bucket_multiplier = ? AND epoch_id = ?
                            """,
                            (route.value, str(size), epoch_id),
                        )
                        history[-1] = invalid
                        ready_count = 0
                        segment_started_at = None
                        segment_last_at = invalid.observed_at
                        segment_last_reason = invalid.reason
                        continue
                    if history and observation.observed_at <= history[-1].observed_at:
                        raise ValueError("calibration observation time cannot regress")
                    if (
                        history
                        and observation.observed_at - history[-1].observed_at > self._maximum_gap
                        and observation.reason == ReasonCode.QUOTE_READY
                    ):
                        observation = replace(
                            observation,
                            base_quantity=None,
                            spread_bps=None,
                            adverse_excursion_after_entry_bps=None,
                            convergence_seconds=None,
                            stressed_cost_floor_bps=None,
                            normalized_tick_bps=None,
                            notional_usdt=None,
                            funding_rate_delta=None,
                            exit_depth_multiple=None,
                            reason=ReasonCode.BOOK_STALE,
                        )
                    enriched = self._enrich_episode(database, observation, tuple(history))
                    effective_reason = _effective_observation_reason(enriched)
                    if effective_reason != enriched.reason:
                        enriched = replace(enriched, reason=effective_reason)
                    database.execute(
                        """
                        INSERT INTO route_calibration_observations(
                            route, size_bucket_multiplier, epoch_id, observed_at,
                            reason, payload_json
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            route.value,
                            str(size),
                            epoch_id,
                            enriched.observed_at.isoformat(),
                            enriched.reason.value,
                            _observation_payload(enriched),
                        ),
                    )
                    history.append(enriched)
                    continues_segment = (
                        segment_last_at is not None
                        and enriched.observed_at - segment_last_at <= self._maximum_gap
                        and segment_last_reason == ReasonCode.QUOTE_READY
                    )
                    if enriched.reason == ReasonCode.QUOTE_READY:
                        ready_count = ready_count + 1 if continues_segment else 1
                        if not continues_segment:
                            segment_started_at = enriched.observed_at
                    else:
                        ready_count = 0
                        segment_started_at = None
                    segment_last_at = enriched.observed_at
                    segment_last_reason = enriched.reason
                if segment_last_at is not None:
                    database.execute(
                        """
                        INSERT INTO route_calibration_segments(
                            route, size_bucket_multiplier, epoch_id, ready_sample_count,
                            segment_started_at, last_observed_at, last_reason
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(route, size_bucket_multiplier, epoch_id) DO UPDATE SET
                            ready_sample_count = excluded.ready_sample_count,
                            segment_started_at = excluded.segment_started_at,
                            last_observed_at = excluded.last_observed_at,
                            last_reason = excluded.last_reason
                        """,
                        (
                            route.value,
                            str(size),
                            epoch_id,
                            ready_count,
                            segment_started_at.isoformat()
                            if segment_started_at is not None
                            else None,
                            segment_last_at.isoformat(),
                            segment_last_reason.value,
                        ),
                    )
                cutoff_30d = (now - timedelta(days=30)).isoformat()
                cutoff_24h = (now - timedelta(hours=24)).isoformat()
                database.execute(
                    """
                    DELETE FROM route_calibration_observations
                    WHERE route = ? AND size_bucket_multiplier = ? AND observed_at < ?
                    """,
                    (route.value, str(size), cutoff_30d),
                )
                database.execute(
                    """
                    DELETE FROM route_calibration_observations
                    WHERE observation_id IN (
                        SELECT observation_id FROM (
                            SELECT observation_id,
                                   ROW_NUMBER() OVER (
                                       PARTITION BY substr(observed_at, 1, 16)
                                       ORDER BY observed_at DESC, observation_id DESC
                                   ) AS minute_rank
                            FROM route_calibration_observations
                            WHERE route = ? AND size_bucket_multiplier = ?
                              AND observed_at < ?
                        ) WHERE minute_rank > 1
                    )
                    """,
                    (route.value, str(size), cutoff_24h),
                )
                self._thin_route_observations_to_bound(
                    database,
                    route.value,
                    str(size),
                )
                previous = previous_by_key[(route, size, epoch_id)]
                previous_active, previous_transient = previous_status_by_key[
                    (route, size, epoch_id)
                ]
                latest = history[-1]
                transient_rejection = False
                segment_period = (
                    segment_last_at - segment_started_at
                    if segment_last_at is not None and segment_started_at is not None
                    else timedelta(0)
                )
                if latest.reason != ReasonCode.QUOTE_READY:
                    assessment = RouteCalibrationAssessment(
                        route,
                        size,
                        latest.base_quantity,
                        latest.reason,
                        ready_count,
                        None,
                    )
                elif ready_count < self._minimum_samples or segment_period < self._minimum_period:
                    assessment = RouteCalibrationAssessment(
                        route,
                        size,
                        latest.base_quantity,
                        ReasonCode.CALIBRATION_INSUFFICIENT,
                        ready_count,
                        None,
                    )
                elif (
                    latest.base_quantity is None
                    or latest.funding_rate_delta is None
                    or latest.stressed_cost_floor_bps is None
                    or latest.normalized_tick_bps is None
                    or latest.notional_usdt is None
                    or latest.exit_depth_multiple is None
                ):
                    reason = (
                        ReasonCode.FUNDING_UNKNOWN
                        if latest.funding_rate_delta is None
                        else ReasonCode.CALIBRATION_INSUFFICIENT
                    )
                    assessment = RouteCalibrationAssessment(
                        route,
                        size,
                        latest.base_quantity,
                        reason,
                        ready_count,
                        None,
                    )
                elif latest.exit_depth_multiple < Decimal(3):
                    assessment = RouteCalibrationAssessment(
                        route,
                        size,
                        latest.base_quantity,
                        ReasonCode.DEPTH_INSUFFICIENT,
                        ready_count,
                        None,
                    )
                else:
                    can_reuse = (
                        previous is not None
                        and previous.epoch_id == epoch_id
                        and now >= previous.calibrated_at
                        and (
                            (
                                previous_transient
                                and now - previous.calibrated_at < timedelta(seconds=60)
                            )
                            or (
                                previous_active
                                and (
                                    now - previous.calibrated_at < timedelta(seconds=60)
                                    or (route, size, epoch_id) not in rebuild_keys
                                )
                            )
                        )
                    )
                    if can_reuse and previous is not None:
                        incremental_reason = _incremental_safety_reason(
                            latest,
                            previous,
                            self._minimum_profit,
                        )
                        if incremental_reason == ReasonCode.QUOTE_READY:
                            parameters = replace(
                                previous,
                                latest_base_quantity=latest.base_quantity,
                                sample_count=ready_count,
                                observation_period_seconds=int(segment_period.total_seconds()),
                            )
                            assessment = RouteCalibrationAssessment(
                                route,
                                size,
                                latest.base_quantity,
                                ReasonCode.QUOTE_READY,
                                ready_count,
                                parameters,
                            )
                        else:
                            transient_rejection = (
                                latest.spread_bps is not None
                                and latest.spread_bps <= previous.route_stop_bps
                            )
                            assessment = RouteCalibrationAssessment(
                                route,
                                size,
                                latest.base_quantity,
                                incremental_reason,
                                ready_count,
                                None,
                            )
                    elif (route, size, epoch_id) in rebuild_keys or not had_persisted_history:
                        rows = database.execute(
                            """
                            SELECT payload_json FROM route_calibration_observations
                            WHERE route = ? AND size_bucket_multiplier = ? AND epoch_id = ?
                            ORDER BY observed_at, observation_id
                            """,
                            (route.value, str(size), epoch_id),
                        ).fetchall()
                        window = tuple(_observation_from_payload(str(row[0])) for row in rows)
                        assessment = calibrate_route_size(
                            window,
                            now=now,
                            minimum_samples=self._minimum_samples,
                            minimum_observation_period=self._minimum_period,
                            minimum_profit_usdt=self._minimum_profit,
                            parameter_change_limit_ratio_per_day=self._change_limit,
                            previous=previous,
                            previous_window_coverage_tolerance=min(
                                self._maximum_gap,
                                timedelta(minutes=5),
                            ),
                            minimum_convergence_samples_per_spread_bucket=(
                                self._minimum_convergence_samples_per_spread_bucket
                            ),
                        )
                    else:
                        assessment = RouteCalibrationAssessment(
                            route,
                            size,
                            latest.base_quantity,
                            ReasonCode.CALIBRATION_INSUFFICIENT,
                            ready_count,
                            None,
                        )
                if assessment.parameters is not None:
                    database.execute(
                        """
                        INSERT INTO route_calibration_parameters(
                            route, size_bucket_multiplier, payload_json, updated_at,
                            active, transient_blocked
                        ) VALUES (?, ?, ?, ?, 1, 0)
                        ON CONFLICT(route, size_bucket_multiplier) DO UPDATE SET
                            payload_json = excluded.payload_json,
                            updated_at = excluded.updated_at,
                            active = 1,
                            transient_blocked = 0
                        """,
                        (
                            route.value,
                            str(size),
                            _parameter_payload(assessment.parameters),
                            now.isoformat(),
                        ),
                    )
                elif assessment.staged_parameters is not None:
                    database.execute(
                        """
                        INSERT INTO route_calibration_parameters(
                            route, size_bucket_multiplier, payload_json, updated_at,
                            active, transient_blocked
                        ) VALUES (?, ?, ?, ?, 0, 0)
                        ON CONFLICT(route, size_bucket_multiplier) DO UPDATE SET
                            payload_json = excluded.payload_json,
                            updated_at = excluded.updated_at,
                            active = 0,
                            transient_blocked = 0
                        """,
                        (
                            route.value,
                            str(size),
                            _parameter_payload(assessment.staged_parameters),
                            now.isoformat(),
                        ),
                    )
                elif transient_rejection:
                    database.execute(
                        """
                        UPDATE route_calibration_parameters
                        SET active = 0, transient_blocked = 1
                        WHERE route = ? AND size_bucket_multiplier = ?
                        """,
                        (route.value, str(size)),
                    )
                else:
                    database.execute(
                        """
                        UPDATE route_calibration_parameters
                        SET active = 0, transient_blocked = 0
                        WHERE route = ? AND size_bucket_multiplier = ?
                        """,
                        (route.value, str(size)),
                    )
                assessments.append(assessment)
            self._prune_global_history(
                database,
                current_keys=current_keys,
                current_epoch=incoming_epoch,
                now=now,
            )
            if abort.is_set():
                database.rollback()
                raise RuntimeError("route calibration write aborted")
            latest_observed_at = max(item.observed_at for item in observations)
            database.execute(
                """
                UPDATE route_calibration_runtime
                SET last_observed_at = CASE
                    WHEN last_observed_at IS NULL OR last_observed_at < ? THEN ?
                    ELSE last_observed_at
                END
                WHERE singleton = 1 AND epoch_id = ?
                """,
                (
                    latest_observed_at.isoformat(),
                    latest_observed_at.isoformat(),
                    incoming_epoch,
                ),
            )
            database.commit()
        return tuple(
            sorted(assessments, key=lambda item: (item.route.value, item.size_bucket_multiplier))
        )

    def _prune_global_history(
        self,
        database: sqlite3.Connection,
        *,
        current_keys: set[tuple[str, str]],
        current_epoch: str,
        now: datetime,
    ) -> None:
        cutoff = (now - timedelta(days=30)).isoformat()
        database.execute(
            "DELETE FROM route_calibration_observations WHERE observed_at < ?",
            (cutoff,),
        )
        database.execute(
            """
            DELETE FROM route_calibration_episodes
            WHERE epoch_id <> ? OR started_at < ?
            """,
            (current_epoch, cutoff),
        )
        database.execute(
            """
            DELETE FROM route_calibration_segments
            WHERE epoch_id <> ? OR last_observed_at < ?
            """,
            (current_epoch, cutoff),
        )
        stale_rows = database.execute(
            """
            SELECT route, size_bucket_multiplier FROM route_calibration_parameters
            WHERE NOT EXISTS (
                SELECT 1 FROM route_calibration_segments AS segments
                WHERE segments.route = route_calibration_parameters.route
                  AND segments.size_bucket_multiplier =
                      route_calibration_parameters.size_bucket_multiplier
                  AND segments.last_observed_at >= ?
            )
            """,
            (cutoff,),
        ).fetchall()
        for row in stale_rows:
            key = (str(row[0]), str(row[1]))
            if key not in current_keys:
                self._delete_route_size_key(database, key)
        rows = database.execute(
            """
            SELECT route, size_bucket_multiplier, last_observed_at AS latest
            FROM route_calibration_segments
            ORDER BY last_observed_at DESC, route, size_bucket_multiplier
            """
        ).fetchall()
        excess = max(0, len(rows) - self._maximum_keys)
        if excess:
            removable = [
                (str(row[0]), str(row[1]))
                for row in reversed(rows)
                if (str(row[0]), str(row[1])) not in current_keys
            ]
            if len(removable) < excess:
                raise RuntimeError("calibration key bound cannot preserve current batch")
            for key in removable[:excess]:
                self._delete_route_size_key(database, key)

    @staticmethod
    def _delete_route_size_key(
        database: sqlite3.Connection,
        key: tuple[str, str],
    ) -> None:
        database.execute(
            """
            DELETE FROM route_calibration_observations
            WHERE route = ? AND size_bucket_multiplier = ?
            """,
            key,
        )
        database.execute(
            """
            DELETE FROM route_calibration_parameters
            WHERE route = ? AND size_bucket_multiplier = ?
            """,
            key,
        )
        database.execute(
            """
            DELETE FROM route_calibration_episodes
            WHERE route = ? AND size_bucket_multiplier = ?
            """,
            key,
        )
        database.execute(
            """
            DELETE FROM route_calibration_segments
            WHERE route = ? AND size_bucket_multiplier = ?
            """,
            key,
        )

    def _thin_route_observations_to_bound(
        self,
        database: sqlite3.Connection,
        route_value: str,
        size_value: str,
    ) -> None:
        count_row = database.execute(
            """
            SELECT count(*) FROM route_calibration_observations
            WHERE route = ? AND size_bucket_multiplier = ?
            """,
            (route_value, size_value),
        ).fetchone()
        if count_row is None or int(count_row[0]) <= self._maximum_observations:
            return
        rows = database.execute(
            """
            SELECT observation_id, payload_json
            FROM route_calibration_observations
            WHERE route = ? AND size_bucket_multiplier = ?
            ORDER BY observed_at, observation_id
            """,
            (route_value, size_value),
        ).fetchall()

        # Keeping only the newest N rows shortens a 24-hour window whenever
        # the observation cadence is faster than N/24h.  Instead retain a
        # deterministic, approximately uniform time sample, while preserving
        # the endpoints, explicit quality boundaries, and completed episode
        # evidence.  A 10% hysteresis avoids an O(N) rebuild on every steady
        # one-row append at production cardinality.
        target = max(
            self._minimum_samples,
            3,
            (
                self._maximum_observations
                if self._maximum_observations < 100
                else int(self._maximum_observations * 0.9)
            ),
        )
        target = min(target, self._maximum_observations)

        def uniformly_select(indices: tuple[int, ...], count: int) -> set[int]:
            if count <= 0 or not indices:
                return set()
            if count >= len(indices):
                return set(indices)
            if count == 1:
                return {indices[len(indices) // 2]}
            return {
                indices[round(position * (len(indices) - 1) / (count - 1))]
                for position in range(count)
            }

        parsed = tuple(_observation_from_payload(str(row[1])) for row in rows)
        recent_cutoff = parsed[-1].observed_at - timedelta(hours=24)
        recent_indices = tuple(
            index
            for index, observation in enumerate(parsed)
            if observation.observed_at >= recent_cutoff
        )
        recent_budget = min(
            len(recent_indices),
            max(self._minimum_samples, target // 3),
        )
        recent_ready_indices = tuple(
            index for index in recent_indices if parsed[index].reason == ReasonCode.QUOTE_READY
        )
        recent_keep = uniformly_select(
            recent_ready_indices,
            min(len(recent_ready_indices), self._minimum_samples, target),
        )
        remaining_recent = tuple(index for index in recent_indices if index not in recent_keep)
        recent_keep.update(
            uniformly_select(
                remaining_recent,
                min(len(remaining_recent), recent_budget - len(recent_keep)),
            )
        )
        must_keep = {len(rows) - 1, *recent_keep}
        if len(must_keep) < target:
            must_keep.add(0)
        required = set(must_keep)
        for index, observation in enumerate(parsed):
            if observation.reason != ReasonCode.QUOTE_READY or _episode_samples_for_observation(
                observation
            ):
                required.add(index)

        if len(required) > target:
            keep = set(must_keep)
            optional_required = tuple(sorted(required - must_keep))
            keep.update(
                uniformly_select(
                    optional_required,
                    target - len(keep),
                )
            )
        else:
            keep = set(required)
            candidates = tuple(index for index in range(len(rows)) if index not in keep)
            keep.update(uniformly_select(candidates, target - len(keep)))
        delete_ids = tuple((int(row[0]),) for index, row in enumerate(rows) if index not in keep)
        database.executemany(
            "DELETE FROM route_calibration_observations WHERE observation_id = ?",
            delete_ids,
        )

    def _enrich_episode(
        self,
        database: sqlite3.Connection,
        observation: RouteCalibrationObservation,
        history: tuple[RouteCalibrationObservation, ...],
    ) -> RouteCalibrationObservation:
        key = (
            observation.route.value,
            str(observation.size_bucket_multiplier),
            observation.epoch_id,
        )
        if observation.reason != ReasonCode.QUOTE_READY or observation.spread_bps is None:
            database.execute(
                """
                DELETE FROM route_calibration_episodes
                WHERE route = ? AND size_bucket_multiplier = ? AND epoch_id = ?
                """,
                key,
            )
            return observation
        if (
            observation.adverse_excursion_after_entry_bps is not None
            and observation.convergence_seconds is not None
        ):
            return observation
        episodes = database.execute(
            """
            SELECT spread_bucket_index, entry_spread_bps, convergence_target_bps,
                   peak_spread_bps, started_at
            FROM route_calibration_episodes
            WHERE route = ? AND size_bucket_multiplier = ? AND epoch_id = ?
            ORDER BY spread_bucket_index
            """,
            key,
        ).fetchall()
        completed_samples: list[RouteCalibrationEpisodeSample] = []
        timed_out = self._maximum_holding_seconds is not None and any(
            observation.spread_bps > Decimal(str(episode[2]))
            and observation.observed_at - datetime.fromisoformat(str(episode[4])).astimezone(UTC)
            >= timedelta(seconds=self._maximum_holding_seconds)
            for episode in episodes
        )
        open_buckets: set[int] = set()
        for episode in episodes:
            bucket_index = int(episode[0])
            open_buckets.add(bucket_index)
            entry = Decimal(str(episode[1]))
            target = Decimal(str(episode[2]))
            peak = max(Decimal(str(episode[3])), observation.spread_bps)
            started = datetime.fromisoformat(str(episode[4])).astimezone(UTC)
            elapsed_seconds = max(0, int((observation.observed_at - started).total_seconds()))
            converged = observation.spread_bps <= target
            episode_timed_out = timed_out and not converged
            if converged or episode_timed_out:
                database.execute(
                    """
                    DELETE FROM route_calibration_episodes
                    WHERE route = ? AND size_bucket_multiplier = ? AND epoch_id = ?
                      AND spread_bucket_index = ?
                    """,
                    (*key, bucket_index),
                )
                open_buckets.discard(bucket_index)
                completed_samples.append(
                    RouteCalibrationEpisodeSample(
                        entry,
                        peak,
                        Decimal(elapsed_seconds),
                        bucket_index,
                        episode_timed_out,
                    )
                )
            else:
                database.execute(
                    """
                    UPDATE route_calibration_episodes SET peak_spread_bps = ?
                    WHERE route = ? AND size_bucket_multiplier = ? AND epoch_id = ?
                      AND spread_bucket_index = ?
                    """,
                    (str(peak), *key, bucket_index),
                )
        last_invalid = max(
            (index for index, item in enumerate(history) if item.reason != ReasonCode.QUOTE_READY),
            default=-1,
        )
        previous = self._latest_in_database(
            database,
            observation.route,
            observation.size_bucket_multiplier,
        )
        segment = tuple(
            item.spread_bps
            for item in history[last_invalid + 1 :]
            if item.reason == ReasonCode.QUOTE_READY and item.spread_bps is not None
        )
        entry_thresholds: tuple[Decimal, ...]
        convergence_targets: tuple[Decimal, ...]
        if previous is not None and previous.epoch_id == observation.epoch_id:
            grid_aligned = True
            entry_thresholds = previous.entry_levels_bps
            convergence_targets = tuple(
                previous.target_close_bps(threshold) for threshold in entry_thresholds
            )
        elif len(segment) >= 5:
            grid_aligned = False
            entry_thresholds = (_quantile(segment, Decimal("0.90")),)
            convergence_targets = (_quantile(segment, Decimal("0.50")),)
        else:
            grid_aligned = False
            entry_thresholds = ()
            convergence_targets = ()
        if timed_out:
            database.execute(
                """
                DELETE FROM route_calibration_episodes
                WHERE route = ? AND size_bucket_multiplier = ? AND epoch_id = ?
                """,
                key,
            )
        else:
            for bucket_index, (entry_threshold, convergence_target) in enumerate(
                zip(entry_thresholds, convergence_targets, strict=True)
            ):
                if bucket_index in open_buckets or observation.spread_bps < entry_threshold:
                    continue
                database.execute(
                    """
                    INSERT INTO route_calibration_episodes(
                        route, size_bucket_multiplier, epoch_id, spread_bucket_index,
                        entry_spread_bps, convergence_target_bps, peak_spread_bps, started_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(
                        route, size_bucket_multiplier, epoch_id, spread_bucket_index
                    ) DO NOTHING
                    """,
                    (
                        *key,
                        bucket_index,
                        str(entry_threshold if grid_aligned else observation.spread_bps),
                        str(convergence_target),
                        str(observation.spread_bps),
                        observation.observed_at.isoformat(),
                    ),
                )
        if not completed_samples:
            return observation
        representative = completed_samples[-1]
        return replace(
            observation,
            adverse_excursion_after_entry_bps=max(
                max(Decimal(0), sample.peak_spread_bps - sample.entry_spread_bps)
                for sample in completed_samples
            ),
            convergence_seconds=max(sample.convergence_seconds for sample in completed_samples),
            reason=(ReasonCode.CALIBRATION_REGIME_SHIFT if timed_out else observation.reason),
            episode_peak_spread_bps=max(sample.peak_spread_bps for sample in completed_samples),
            episode_entry_spread_bps=representative.entry_spread_bps,
            episode_samples=tuple(completed_samples),
        )
