from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
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


def test_risk_budget_relationship_is_enforced() -> None:
    settings = load_settings(CONFIG)
    raw = settings.model_dump(mode="json")
    raw["risk"]["portfolio_stressed_loss_limit_usdt"] = "20"
    with pytest.raises(ValidationError):
        Settings.model_validate(raw)


def test_unsupported_product_cannot_be_configured() -> None:
    settings = load_settings(CONFIG)
    raw = settings.model_dump(mode="json")
    raw["products"]["settlement"] = "USDC"
    with pytest.raises(ValidationError):
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
