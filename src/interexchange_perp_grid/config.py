from __future__ import annotations

import copy
import os
from collections.abc import Callable, Mapping
from decimal import Decimal
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class AppConfig(StrictModel):
    name: str
    mode: Literal["replay", "shadow", "live"]
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    heartbeat_interval_seconds: int = Field(gt=0, le=60)
    health_max_age_seconds: int = Field(gt=0, le=300)

    @model_validator(mode="after")
    def health_window_exceeds_heartbeat(self) -> AppConfig:
        if self.health_max_age_seconds <= self.heartbeat_interval_seconds:
            raise ValueError("health max age must exceed heartbeat interval")
        if not self.name.strip():
            raise ValueError("application name must be non-empty")
        return self


class VenuesConfig(StrictModel):
    wave1_public: tuple[str, ...]
    canary_primary: tuple[str, ...]
    canary_alternate: tuple[str, ...]
    wave2: tuple[str, ...]
    wave3: tuple[str, ...]
    quarantine_on_unknown_capability: bool = True

    @model_validator(mode="after")
    def unique_venues(self) -> VenuesConfig:
        all_venues = (
            self.wave1_public
            + self.canary_primary
            + self.canary_alternate
            + self.wave2
            + self.wave3
        )
        if any(not venue.strip() for venue in all_venues):
            raise ValueError("venue identifiers must be non-empty")
        return self


class ProductsConfig(StrictModel):
    settlement: Literal["USDT"]
    linear_only: Literal[True]
    perpetual_only: Literal[True]


class RiskConfig(StrictModel):
    reference_capital_usdt: Decimal = Field(gt=0)
    pair_stressed_loss_limit_usdt: Decimal = Field(gt=0)
    portfolio_stressed_loss_limit_usdt: Decimal = Field(gt=0)
    max_active_routes: int = Field(ge=1, le=10)
    max_routes_per_base: Literal[1]
    max_tranches_per_route: int = Field(ge=1, le=5)
    local_free_margin_floor_ratio: Decimal = Field(ge=Decimal("0.20"), lt=1)
    initial_effective_leverage_cap: Decimal = Field(gt=0, le=Decimal("3"))
    max_hold_seconds: int = Field(gt=0, le=86400)

    @model_validator(mode="after")
    def validate_risk_relationships(self) -> RiskConfig:
        if self.pair_stressed_loss_limit_usdt > self.portfolio_stressed_loss_limit_usdt:
            raise ValueError("pair risk cannot exceed portfolio risk")
        if (
            self.pair_stressed_loss_limit_usdt * self.max_active_routes
            > self.portfolio_stressed_loss_limit_usdt
        ):
            raise ValueError("configured route count can exceed portfolio stress budget")
        if self.portfolio_stressed_loss_limit_usdt > self.reference_capital_usdt:
            raise ValueError("portfolio stress budget cannot exceed reference capital")
        return self


class StrategyConfig(StrictModel):
    adaptive_grid: Literal[True]
    stressed_cost_multiplier: Decimal = Field(ge=Decimal("1"))
    minimum_profit_usdt: Decimal | None = Field(default=None, ge=0)
    grid_parameter_change_limit_ratio: Decimal = Field(gt=0, le=Decimal("0.50"))


class ExecutionConfig(StrictModel):
    normal_intent: Literal["PROTECTED_AGGRESSIVE_TAKER"]
    normal_unbounded_market_allowed: Literal[False]
    emergency_unbounded_market_allowed: bool
    idempotent_client_order_ids: Literal[True]
    third_venue_emergency_hedge: bool


class MarketDataConfig(StrictModel):
    broad_feed: Literal["bbo"]
    candidate_feed: Literal["l2"]
    max_bbo_age_ms: int = Field(gt=0)
    max_l2_age_ms: int = Field(gt=0)
    max_clock_skew_ms: int = Field(gt=0)


class StorageConfig(StrictModel):
    sqlite_path: str
    parquet_dir: str
    sqlite_wal: Literal[True]

    @model_validator(mode="after")
    def paths_are_non_empty(self) -> StorageConfig:
        if not self.sqlite_path.strip() or not self.parquet_dir.strip():
            raise ValueError("storage paths must be non-empty")
        return self


class LiveConfig(StrictModel):
    enabled: bool
    require_local_unlock_secret: Literal[True]
    require_telegram_challenge: Literal[True]
    require_current_hash_qualification: Literal[True]
    canary_max_routes: Literal[1]
    canary_max_tranches: Literal[1]


class TelegramConfig(StrictModel):
    enabled: bool


class Settings(StrictModel):
    app: AppConfig
    venues: VenuesConfig
    products: ProductsConfig
    risk: RiskConfig
    strategy: StrategyConfig
    execution: ExecutionConfig
    market_data: MarketDataConfig
    storage: StorageConfig
    live: LiveConfig
    telegram: TelegramConfig

    @model_validator(mode="after")
    def live_mode_requires_flag(self) -> Settings:
        if self.app.mode == "live" and not self.live.enabled:
            raise ValueError("live mode requires live.enabled=true")
        return self


def _parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"invalid boolean environment value: {value!r}")


EnvironmentParser = Callable[[str], object]
ENVIRONMENT_BINDINGS: dict[str, tuple[str, str, EnvironmentParser]] = {
    "IPEG_MODE": ("app", "mode", str),
    "IPEG_LOG_LEVEL": ("app", "log_level", str),
    "IPEG_STATE_PATH": ("storage", "sqlite_path", str),
    "IPEG_PARQUET_DIR": ("storage", "parquet_dir", str),
    "IPEG_MAX_CLOCK_SKEW_MS": ("market_data", "max_clock_skew_ms", int),
    "IPEG_LIVE_ENABLED": ("live", "enabled", _parse_bool),
    "IPEG_TELEGRAM_ENABLED": ("telegram", "enabled", _parse_bool),
}


def _apply_environment(raw: dict[str, object], environ: Mapping[str, str]) -> None:
    for variable, (section, key, parser) in ENVIRONMENT_BINDINGS.items():
        if variable not in environ:
            continue
        section_value = raw.get(section)
        if not isinstance(section_value, dict):
            raise ValueError(f"configuration section {section!r} must be a mapping")
        section_value[key] = parser(environ[variable])


def load_settings(path: Path, environ: Mapping[str, str] | None = None) -> Settings:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("configuration root must be a mapping")
    merged = copy.deepcopy(raw)
    _apply_environment(merged, os.environ if environ is None else environ)
    return Settings.model_validate(merged)
