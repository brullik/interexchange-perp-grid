from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.aggressive_model import (
    DivergenceDirection,
    EpisodeCloseReason,
    HistoricalModelPolicy,
    HistoricalReferenceModel,
    ModelEligibility,
    build_historical_reference_model,
    decimal_quantile,
    effective_stop_bps,
    historical_model_payload,
    historical_model_sha256,
    load_historical_model,
    load_historical_model_policy,
    modal_bucket,
    route_model_update_allowed,
    save_historical_model,
    select_route_model,
)
from interexchange_perp_grid.domain import InstrumentKey, ProductType, Venue
from interexchange_perp_grid.reference_history import (
    ReferenceMinuteRejection,
    ReferenceRejectionReason,
    ReferenceSpreadBar,
)

_START = datetime(2026, 1, 1, tzinfo=UTC)
_KEY = InstrumentKey("BTC", "USDT", "USDT", ProductType.LINEAR_USDT_PERPETUAL)


def _bar(
    minute: int,
    close: str = "0",
    high: str | None = None,
    low: str | None = None,
) -> ReferenceSpreadBar:
    close_value = Decimal(close)
    return ReferenceSpreadBar(
        venue_a=Venue.BYBIT,
        venue_b=Venue.OKX,
        instrument=_KEY,
        interval_start=_START + timedelta(minutes=minute),
        open_bps=close_value,
        high_bps=Decimal(high) if high is not None else close_value,
        low_bps=Decimal(low) if low is not None else close_value,
        close_bps=close_value,
        contract_metadata_version_a="bybit-v1",
        contract_metadata_version_b="okx-v1",
    )


def _policy(**changes: object) -> HistoricalModelPolicy:
    values: dict[str, object] = {
        "history_target_days": Decimal("7"),
        "history_minimum_live_days": Decimal("6"),
        "history_minimum_shadow_days": Decimal("1"),
        "minimum_completed_episodes": 10,
        "minimum_convergence_rate": Decimal("0.70"),
    }
    values.update(changes)
    return HistoricalModelPolicy(**values)  # type: ignore[arg-type]


def _seven_day_history() -> tuple[ReferenceSpreadBar, ...]:
    overrides: dict[int, tuple[str, str, str]] = {}
    for episode in range(10):
        positive_minute = 10 + episode * 4
        negative_minute = positive_minute + 2
        overrides[positive_minute] = ("10", "10", "0")
        overrides[negative_minute] = ("-10", "0", "-10")
    return tuple(
        _bar(minute, *overrides.get(minute, ("0", "0", "0"))) for minute in range(7 * 1440)
    )


def test_modal_bucket_uses_all_locked_tie_breakers() -> None:
    assert modal_bucket(
        (Decimal("-2.2"), Decimal("-1.8"), Decimal("1.8"), Decimal("2.2")),
        Decimal("1"),
    ) == Decimal("-2")
    assert modal_bucket(
        (Decimal("-8.1"), Decimal("-7.9"), Decimal("1.1"), Decimal("1.2"), Decimal("4")),
        Decimal("1"),
    ) == Decimal("1")


def test_decimal_quantile_is_deterministic_linear_interpolation() -> None:
    values = (Decimal("0"), Decimal("10"), Decimal("20"), Decimal("30"))
    assert decimal_quantile(values, Decimal("0.10")) == Decimal("3.0")
    assert decimal_quantile(values, Decimal("0.50")) == Decimal("15.0")


def test_locked_profile_is_the_only_policy_source(tmp_path: Path) -> None:
    profile_path = Path("config/AGGRESSIVE_SYMBIOSIS_V1.yaml")
    loaded = load_historical_model_policy(profile_path)
    assert loaded.policy.history_target_days == 180
    assert loaded.policy.history_minimum_live_days == 90
    assert loaded.policy.history_minimum_shadow_days == 30
    assert loaded.policy.minimum_completed_episodes == 10
    assert loaded.policy.minimum_convergence_rate == Decimal("0.70")
    assert loaded.policy.level_fractions == (
        Decimal("0.20"),
        Decimal("0.40"),
        Decimal("0.60"),
        Decimal("0.80"),
        Decimal("1.00"),
    )
    assert loaded.profile_sha256 == hashlib.sha256(profile_path.read_bytes()).hexdigest()

    broken = tmp_path / "broken.yaml"
    broken.write_text("reference_spread: {}\nhistorical_model: {}\naggressive_grid: {}\n")
    with pytest.raises(ValueError, match="strategy profile"):
        load_historical_model_policy(broken)

    drifted = tmp_path / "drifted.yaml"
    drifted.write_text(
        profile_path.read_text(encoding="utf-8").replace(
            "positive_and_negative_directions_separate: true",
            "positive_and_negative_directions_separate: false",
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="does not match the model contract"):
        load_historical_model_policy(drifted)


@pytest.mark.parametrize(
    ("original", "replacement", "message"),
    (
        (
            "['0.20', '0.40', '0.60', '0.80', '1.00']",
            "['0.10', '0.40', '0.60', '0.80', '1.00']",
            "level fractions",
        ),
        (
            "['0.10', '0.15', '0.20', '0.25', '0.30']",
            "['0.05', '0.15', '0.20', '0.25', '0.35']",
            "tranche weights",
        ),
        ("stop_buffer_ratio: '0.15'", "stop_buffer_ratio: '0.16'", "stop buffer"),
        (
            "rearm_retreat_step_fraction: '0.25'",
            "rearm_retreat_step_fraction: '0.20'",
            "rearm retreat fraction",
        ),
    ),
)
def test_locked_profile_rejects_fixed_geometry_mutation(
    tmp_path: Path,
    original: str,
    replacement: str,
    message: str,
) -> None:
    profile = Path("config/AGGRESSIVE_SYMBIOSIS_V1.yaml").read_text(encoding="utf-8")
    assert original in profile
    mutated = tmp_path / "mutated.yaml"
    mutated.write_text(profile.replace(original, replacement), encoding="utf-8")
    with pytest.raises(ValueError, match=message):
        load_historical_model_policy(mutated)


def test_historical_policy_rejects_nonfinite_values() -> None:
    with pytest.raises(ValueError, match="finite"):
        HistoricalModelPolicy(history_target_days=Decimal("NaN"))


def test_effective_stop_uses_farther_outward_boundary() -> None:
    assert effective_stop_bps(DivergenceDirection.POSITIVE, Decimal("11.5"), None) == Decimal(
        "11.5"
    )
    assert effective_stop_bps(
        DivergenceDirection.POSITIVE, Decimal("11.5"), Decimal("13")
    ) == Decimal("13")
    assert effective_stop_bps(
        DivergenceDirection.NEGATIVE, Decimal("-11.5"), Decimal("-13")
    ) == Decimal("-13")


def test_model_builds_separate_geometry_episodes_and_live_gate() -> None:
    bars = _seven_day_history()
    model = build_historical_reference_model(
        bars,
        policy=_policy(),
        source_manifest_sha256="source-hash",
        strategy_profile_sha256="profile-hash",
        code_sha="code-sha",
    )

    assert model.s0_bps == 0
    assert model.normal_half_width_bps == 2
    assert model.positive.extreme_bps == 10
    assert model.negative.extreme_bps == -10
    assert model.positive.levels_bps == (
        Decimal("2.00"),
        Decimal("4.00"),
        Decimal("6.00"),
        Decimal("8.00"),
        Decimal("10.00"),
    )
    assert model.negative.levels_bps == tuple(-level for level in model.positive.levels_bps)
    assert model.positive.tranche_weights == (
        Decimal("0.10"),
        Decimal("0.15"),
        Decimal("0.20"),
        Decimal("0.25"),
        Decimal("0.30"),
    )
    assert model.positive.reference_stop_bps == Decimal("11.50")
    assert model.negative.reference_stop_bps == Decimal("-11.50")
    for direction in (model.positive, model.negative):
        assert direction.eligibility == ModelEligibility.LIVE_ELIGIBLE
        assert len(direction.episodes) == 10
        assert direction.convergence_rate == 1
        assert tuple(len(samples) for samples in direction.per_level_samples) == (10,) * 5
        assert all(
            not sample.censored for samples in direction.per_level_samples for sample in samples
        )
    assert model.window_7d.complete
    assert not model.execution_authorized


def test_live_coverage_uses_longest_uninterrupted_minute_run() -> None:
    first_run = tuple(_bar(minute) for minute in range(7 * 1440))
    scattered = tuple(
        _bar((100 + day * 2) * 1440 + minute) for day in range(14) for minute in range(720)
    )
    model = build_historical_reference_model(
        (*first_run, *scattered),
        policy=_policy(
            history_target_days=Decimal("14"),
            history_minimum_live_days=Decimal("12"),
            history_minimum_shadow_days=Decimal("6"),
        ),
        source_manifest_sha256="source-hash",
        strategy_profile_sha256="profile-hash",
        code_sha="code-sha",
    )

    assert model.coverage_days == Decimal(7)
    assert not model.target_coverage_met
    assert model.positive.eligibility != ModelEligibility.LIVE_ELIGIBLE


def test_noon_to_noon_minutes_do_not_count_as_a_complete_utc_day() -> None:
    bars = tuple(
        replace(_bar(minute), interval_start=_START + timedelta(hours=12, minutes=minute))
        for minute in range(1440)
    )
    model = build_historical_reference_model(
        bars,
        source_manifest_sha256="source-hash",
        strategy_profile_sha256="profile-hash",
        code_sha="code-sha",
    )

    assert model.coverage_days == 0


def test_episode_requires_normal_reset_and_gap_censors_every_reached_level() -> None:
    bars = (_bar(0), _bar(1, "10", high="10", low="0"), _bar(3))
    rejection = ReferenceMinuteRejection(
        interval_start=_START + timedelta(minutes=2),
        reason=ReferenceRejectionReason.MISSING_SOURCE,
    )
    model = build_historical_reference_model(
        bars,
        rejections=(rejection,),
        policy=_policy(
            history_target_days=Decimal("0.003"),
            history_minimum_live_days=Decimal("0.002"),
            history_minimum_shadow_days=Decimal("0.001"),
            minimum_completed_episodes=1,
        ),
        source_manifest_sha256="source-hash",
        strategy_profile_sha256="profile-hash",
        code_sha="code-sha",
    )

    assert len(model.positive.episodes) == 1
    episode = model.positive.episodes[0]
    assert not episode.converged
    assert episode.close_reason == EpisodeCloseReason.DATA_UNAVAILABLE
    assert tuple(sample.level_index for sample in episode.level_samples) == (1, 2, 3, 4, 5)
    assert all(sample.censored for sample in episode.level_samples)
    assert model.positive.eligibility == ModelEligibility.DISABLED


def test_horizon_censors_and_records_adverse_excursion() -> None:
    bars = (
        *(_bar(minute) for minute in range(10)),
        _bar(10, "10", high="10", low="0"),
        _bar(11, "12", high="12", low="10"),
        _bar(12, "12", high="12", low="10"),
    )
    model = build_historical_reference_model(
        bars,
        policy=_policy(
            history_target_days=Decimal("0.003"),
            history_minimum_live_days=Decimal("0.002"),
            history_minimum_shadow_days=Decimal("0.001"),
            minimum_completed_episodes=1,
            convergence_horizon_seconds=120,
        ),
        source_manifest_sha256="source-hash",
        strategy_profile_sha256="profile-hash",
        code_sha="code-sha",
    )
    episode = model.positive.episodes[0]
    assert episode.close_reason == EpisodeCloseReason.HORIZON
    assert episode.ended_at == _START + timedelta(minutes=12)
    assert episode.level_samples[0].adverse_excursion_bps == Decimal("9.6")


def test_direction_can_be_independently_disabled() -> None:
    bars = tuple(_bar(minute, "0", high="10", low="0") for minute in range(1440))
    model = build_historical_reference_model(
        bars,
        policy=_policy(
            history_target_days=Decimal("2"),
            history_minimum_live_days=Decimal("1.5"),
            history_minimum_shadow_days=Decimal("1"),
            minimum_completed_episodes=1,
        ),
        source_manifest_sha256="source-hash",
        strategy_profile_sha256="profile-hash",
        code_sha="code-sha",
    )
    assert model.positive.eligibility == ModelEligibility.SHADOW_ONLY
    assert model.negative.range_bps == 0
    assert model.negative.eligibility == ModelEligibility.DISABLED


def test_seven_day_regime_drift_blocks_only_affected_direction() -> None:
    first_week = tuple(_bar(minute, "0", high="12", low="-12") for minute in range(7 * 1440))
    second_week = tuple(
        _bar(minute, "10", high="12", low="10") for minute in range(7 * 1440, 14 * 1440)
    )
    model = build_historical_reference_model(
        (*first_week, *second_week),
        policy=_policy(
            history_target_days=Decimal("14"),
            history_minimum_live_days=Decimal("12"),
            history_minimum_shadow_days=Decimal("6"),
        ),
        source_manifest_sha256="source-hash",
        strategy_profile_sha256="profile-hash",
        code_sha="code-sha",
    )
    assert model.s0_bps == 0
    assert model.window_7d.complete
    assert model.window_7d.median_bps == 10
    assert model.positive.regime_drift_blocked
    assert not model.negative.regime_drift_blocked
    assert model.positive.eligibility != ModelEligibility.LIVE_ELIGIBLE


def test_model_identity_is_restart_deterministic_and_binds_inputs() -> None:
    kwargs = {
        "policy": _policy(),
        "source_manifest_sha256": "source-hash",
        "strategy_profile_sha256": "profile-hash",
        "code_sha": "code-sha",
    }
    first = build_historical_reference_model(_seven_day_history(), **kwargs)  # type: ignore[arg-type]
    second = build_historical_reference_model(_seven_day_history(), **kwargs)  # type: ignore[arg-type]
    assert historical_model_payload(first) == historical_model_payload(second)
    assert historical_model_sha256(first) == historical_model_sha256(second)
    changed = build_historical_reference_model(
        _seven_day_history(),
        policy=_policy(),
        source_manifest_sha256="other-source",
        strategy_profile_sha256="profile-hash",
        code_sha="code-sha",
    )
    assert historical_model_sha256(changed) != historical_model_sha256(first)


def test_model_persistence_is_atomic_strict_and_restart_identical(tmp_path: Path) -> None:
    model = build_historical_reference_model(
        _seven_day_history(),
        policy=_policy(),
        source_manifest_sha256="source-hash",
        strategy_profile_sha256="profile-hash",
        code_sha="code-sha",
    )
    path = tmp_path / "historical-model.json"
    saved_hash = save_historical_model(path, model)
    restored = load_historical_model(path)
    assert restored == model
    assert historical_model_sha256(restored) == saved_hash
    assert not tuple(tmp_path.glob("*.pending"))

    envelope = json.loads(path.read_text(encoding="utf-8"))
    del envelope["model"]["normal_low_bps"]
    payload = json.dumps(envelope["model"], sort_keys=True, separators=(",", ":"))
    envelope["model_sha256"] = hashlib.sha256(payload.encode()).hexdigest()
    path.write_text(json.dumps(envelope), encoding="utf-8")
    with pytest.raises(ValueError, match="missing or unknown fields"):
        load_historical_model(path)

    save_historical_model(path, model)
    envelope = json.loads(path.read_text(encoding="utf-8"))
    envelope["model"]["positive"]["levels_bps"][0] = "123.456"
    payload = json.dumps(envelope["model"], sort_keys=True, separators=(",", ":"))
    envelope["model_sha256"] = hashlib.sha256(payload.encode()).hexdigest()
    path.write_text(json.dumps(envelope), encoding="utf-8")
    with pytest.raises(ValueError, match="locked directional geometry"):
        load_historical_model(path)


def test_active_model_is_frozen_and_flat_updates_are_bounded() -> None:
    current = build_historical_reference_model(
        _seven_day_history(),
        policy=_policy(),
        source_manifest_sha256="source-hash",
        strategy_profile_sha256="profile-hash",
        code_sha="code-sha",
    )

    def scaled_positive(factor: Decimal) -> HistoricalReferenceModel:
        spread_range = current.positive.range_bps * factor
        return replace(
            current,
            positive=replace(
                current.positive,
                extreme_bps=current.s0_bps + spread_range,
                range_bps=spread_range,
                levels_bps=tuple(
                    current.s0_bps + spread_range * fraction
                    for fraction in _policy().level_fractions
                ),
                reference_stop_bps=current.s0_bps + spread_range * Decimal("1.15"),
            ),
        )

    ten_percent = scaled_positive(Decimal("1.10"))
    thirty_percent = scaled_positive(Decimal("1.30"))
    policy = _policy()
    assert route_model_update_allowed(current, ten_percent, elapsed_seconds=86_400, policy=policy)
    assert not route_model_update_allowed(
        current, thirty_percent, elapsed_seconds=86_400, policy=policy
    )
    assert (
        select_route_model(
            current,
            thirty_percent,
            route_active=True,
            elapsed_seconds=86_400,
            policy=policy,
        )
        is current
    )
    with pytest.raises(ValueError, match="bounded-change"):
        select_route_model(
            current,
            thirty_percent,
            route_active=False,
            elapsed_seconds=86_400,
            policy=policy,
        )


def test_model_rejects_mixed_identity_and_duplicates() -> None:
    first = _bar(0)
    other_version = ReferenceSpreadBar(
        venue_a=first.venue_a,
        venue_b=first.venue_b,
        instrument=first.instrument,
        interval_start=_START + timedelta(minutes=1),
        open_bps=first.open_bps,
        high_bps=first.high_bps,
        low_bps=first.low_bps,
        close_bps=first.close_bps,
        contract_metadata_version_a="changed",
        contract_metadata_version_b=first.contract_metadata_version_b,
    )
    kwargs = {
        "source_manifest_sha256": "source",
        "strategy_profile_sha256": "profile",
        "code_sha": "code",
    }
    with pytest.raises(ValueError, match="stable reference identity"):
        build_historical_reference_model((first, other_version), **kwargs)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="duplicate reference minutes"):
        build_historical_reference_model((first, first), **kwargs)  # type: ignore[arg-type]


def test_direction_enum_is_explicit_in_persisted_evidence() -> None:
    model = build_historical_reference_model(
        _seven_day_history(),
        policy=_policy(),
        source_manifest_sha256="source-hash",
        strategy_profile_sha256="profile-hash",
        code_sha="code-sha",
    )
    payload = historical_model_payload(model)
    assert payload["positive"]["direction"] == DivergenceDirection.POSITIVE.value  # type: ignore[index]
    assert payload["negative"]["direction"] == DivergenceDirection.NEGATIVE.value  # type: ignore[index]
    assert payload["execution_authorized"] is False

    with pytest.raises(ValueError, match="directional identity"):
        replace(
            model,
            positive=replace(model.positive, direction=DivergenceDirection.NEGATIVE),
        )
    with pytest.raises(ValueError, match="directional identity"):
        replace(
            model,
            positive=replace(
                model.positive,
                range_bps=Decimal("-1"),
                extreme_bps=model.s0_bps - Decimal(1),
                levels_bps=tuple(
                    model.s0_bps - fraction
                    for fraction in (
                        Decimal("0.2"),
                        Decimal("0.4"),
                        Decimal("0.6"),
                        Decimal("0.8"),
                        Decimal(1),
                    )
                ),
                reference_stop_bps=model.s0_bps - Decimal("1.15"),
            ),
        )
