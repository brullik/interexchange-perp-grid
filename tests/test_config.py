from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from interexchange_perp_grid.config import Settings, load_settings

CONFIG = Path("config/defaults.yaml")


def test_defaults_are_safe_and_match_owner_limits() -> None:
    settings = load_settings(CONFIG)
    assert settings.app.mode == "shadow"
    assert settings.live.enabled is False
    assert settings.risk.reference_capital_usdt == 500
    assert settings.risk.pair_stressed_loss_limit_usdt == 5
    assert settings.risk.portfolio_stressed_loss_limit_usdt == 50
    assert settings.risk.max_active_routes == 10
    assert settings.risk.max_tranches_per_route == 5
    assert settings.risk.local_free_margin_floor_ratio >= Decimal("0.20")
    assert settings.risk.initial_effective_leverage_cap <= Decimal("3")
    assert settings.risk.max_hold_seconds <= 86400


def test_runtime_universe_policy_is_typed_and_locked() -> None:
    settings = load_settings(CONFIG)
    policy = yaml.safe_load(Path("config/RUNTIME_POLICY.yaml").read_text(encoding="utf-8"))

    assert (
        settings.universe.live_min_listing_age_days
        == policy["universe"]["live_min_listing_age_days"]
    )
    assert (
        settings.universe.instrument_refresh_seconds
        == policy["universe"]["instrument_refresh_seconds"]
    )
    assert (
        settings.universe.max_dynamic_l2_candidates
        == policy["universe"]["max_dynamic_l2_candidates"]
    )
    assert settings.universe.decision_debounce_ms == policy["universe"]["decision_debounce_ms"]
    assert settings.market_data.max_bbo_age_ms == policy["data"]["max_bbo_age_ms"]
    assert list(settings.strategy.calibration_size_multipliers) == [
        Decimal(value) for value in policy["strategy"]["calibration_size_multipliers"]
    ]
    assert (
        settings.strategy.calibration_funding_refresh_seconds
        == policy["strategy"]["calibration_funding_refresh_seconds"]
    )


@pytest.mark.parametrize(
    ("key", "value"),
    (
        ("calibration_size_multipliers", ["1", "3", "5"]),
        ("calibration_funding_refresh_seconds", 61),
    ),
)
def test_calibration_policy_drift_fails_startup(
    tmp_path: Path,
    key: str,
    value: object,
) -> None:
    raw = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
    raw["strategy"][key] = value
    config = tmp_path / "defaults.yaml"
    config.write_text(yaml.safe_dump(raw), encoding="utf-8")
    (tmp_path / "RUNTIME_POLICY.yaml").write_text(
        Path("config/RUNTIME_POLICY.yaml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=f"configuration {key} differs"):
        load_settings(config)


@pytest.mark.parametrize(
    ("section", "key", "value", "message"),
    (
        (
            "strategy",
            "grid_parameter_change_limit_ratio",
            "0.50",
            "change limit exceeds",
        ),
        (
            "strategy",
            "stressed_cost_multiplier",
            "1.0",
            "stressed cost multiplier is below",
        ),
        ("market_data", "max_l2_age_ms", 1001, "max_l2_age_ms differs"),
    ),
)
def test_locked_calibration_safety_cannot_be_weakened(
    tmp_path: Path,
    section: str,
    key: str,
    value: object,
    message: str,
) -> None:
    raw = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
    raw[section][key] = value
    config = tmp_path / "defaults.yaml"
    config.write_text(yaml.safe_dump(raw), encoding="utf-8")
    (tmp_path / "RUNTIME_POLICY.yaml").write_text(
        Path("config/RUNTIME_POLICY.yaml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=message):
        load_settings(config)


@pytest.mark.parametrize(
    ("key", "value"),
    (
        ("canary_max_routes", 2),
        ("canary_max_tranches", 2),
        ("canary_pair_stressed_loss_limit_usdt", "2"),
        ("canary_effective_leverage_cap", "2"),
    ),
)
def test_canary_runtime_limits_cannot_drift_from_locked_stage(
    tmp_path: Path,
    key: str,
    value: object,
) -> None:
    raw = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
    raw["live"][key] = value
    config = tmp_path / "defaults.yaml"
    config.write_text(yaml.safe_dump(raw), encoding="utf-8")
    (tmp_path / "RUNTIME_POLICY.yaml").write_text(
        Path("config/RUNTIME_POLICY.yaml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=f"configuration {key} differs"):
        load_settings(config)


def test_risk_budget_relationship_is_enforced() -> None:
    settings = load_settings(CONFIG)
    raw = settings.model_dump(mode="json")
    raw["risk"]["portfolio_stressed_loss_limit_usdt"] = "20"
    with pytest.raises(ValidationError):
        Settings.model_validate(raw)


def test_scan_cadence_must_leave_funding_refresh_safety_margin() -> None:
    settings = load_settings(CONFIG)
    raw = settings.model_dump(mode="json")
    raw["shadow"]["scan_interval_seconds"] = 31

    with pytest.raises(ValidationError, match="funding refresh safety margin"):
        Settings.model_validate(raw)


def test_unsupported_product_cannot_be_configured() -> None:
    settings = load_settings(CONFIG)
    raw = settings.model_dump(mode="json")
    raw["products"]["settlement"] = "USDC"
    with pytest.raises(ValidationError):
        Settings.model_validate(raw)


def test_venue_profiles_are_known_unique_and_canary_is_wave1_only() -> None:
    settings = load_settings(CONFIG)
    raw = settings.model_dump(mode="json")
    raw["venues"]["wave2"] = ["bitget", "unknown"]
    with pytest.raises(ValidationError, match="unknown venue profiles"):
        Settings.model_validate(raw)

    raw = settings.model_dump(mode="json")
    raw["venues"]["wave2"] = ["bitget", "bitget"]
    with pytest.raises(ValidationError, match="duplicate venue profile"):
        Settings.model_validate(raw)

    raw = settings.model_dump(mode="json")
    raw["venues"]["canary_primary"] = ["bitget"]
    with pytest.raises(ValidationError, match="subset of wave1_public"):
        Settings.model_validate(raw)

    raw = settings.model_dump(mode="json")
    raw["venues"]["wave1_public"] = ["mexc", "bybit", "okx"]
    raw["venues"]["wave3"] = ["binanceusdm", "bingx"]
    raw["venues"]["canary_primary"] = ["mexc", "bybit"]
    with pytest.raises(ValidationError, match="wave1_public must remain exactly"):
        Settings.model_validate(raw)


def test_typed_environment_overrides_are_applied() -> None:
    settings = load_settings(
        CONFIG,
        {
            "IPEG_MODE": "replay",
            "IPEG_LOG_LEVEL": "WARNING",
            "IPEG_STATE_PATH": "state/from-env.sqlite3",
            "IPEG_PARQUET_DIR": "data/from-env",
            "IPEG_MAX_CLOCK_SKEW_MS": "2000",
            "IPEG_LIVE_ENABLED": "false",
            "IPEG_TELEGRAM_ENABLED": "true",
            "IPEG_TELEGRAM_OWNER_CHAT_ID": "42",
        },
    )
    assert settings.app.mode == "replay"
    assert settings.app.log_level == "WARNING"
    assert settings.storage.sqlite_path == "state/from-env.sqlite3"
    assert settings.storage.parquet_dir == "data/from-env"
    assert settings.market_data.max_clock_skew_ms == 2000
    assert settings.live.enabled is False
    assert settings.telegram.enabled is True
    assert settings.telegram.owner_chat_id == 42


def test_invalid_environment_boolean_fails_startup() -> None:
    with pytest.raises(ValueError, match="invalid boolean"):
        load_settings(CONFIG, {"IPEG_LIVE_ENABLED": "sometimes"})


def test_missing_safety_field_fails_startup() -> None:
    settings = load_settings(CONFIG, {})
    raw = settings.model_dump(mode="json")
    del raw["execution"]["normal_intent"]
    with pytest.raises(ValidationError):
        Settings.model_validate(raw)


def test_shadow_allows_telegram_fallback_without_owner_credentials() -> None:
    settings = load_settings(CONFIG, {"IPEG_TELEGRAM_ENABLED": "true"})

    assert settings.app.mode == "shadow"
    assert settings.telegram.enabled is True
    assert settings.telegram.owner_chat_id is None


def test_live_telegram_requires_owner_chat_id() -> None:
    settings = load_settings(CONFIG)
    raw = settings.model_dump(mode="json")
    raw["app"]["mode"] = "live"
    raw["live"]["enabled"] = True
    raw["telegram"]["enabled"] = True
    raw["telegram"]["owner_chat_id"] = None

    with pytest.raises(ValidationError, match="live Telegram control requires"):
        Settings.model_validate(raw)
