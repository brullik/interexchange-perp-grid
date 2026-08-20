from __future__ import annotations

import copy
import os
from collections.abc import Callable, Mapping
from decimal import Decimal
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from interexchange_perp_grid.domain import Venue

KNOWN_VENUE_PROFILES = frozenset({venue.value for venue in Venue} | {"bingx", "mexc"})


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
        unknown = sorted(set(all_venues) - KNOWN_VENUE_PROFILES)
        if unknown:
            raise ValueError(f"unknown venue profiles: {', '.join(unknown)}")
        for name, values in (
            ("wave1_public", self.wave1_public),
            ("canary_primary", self.canary_primary),
            ("canary_alternate", self.canary_alternate),
            ("wave2", self.wave2),
            ("wave3", self.wave3),
        ):
            if len(values) != len(set(values)):
                raise ValueError(f"duplicate venue profile in {name}")
        if not set(self.canary_primary + self.canary_alternate) <= set(self.wave1_public):
            raise ValueError("canary venues must be a subset of wave1_public")
        if set(self.wave1_public) & set(self.wave2 + self.wave3):
            raise ValueError("wave1, wave2, and wave3 venue profiles must not overlap")
        if set(self.wave2) & set(self.wave3):
            raise ValueError("wave2 and wave3 venue profiles must not overlap")
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
    calibration_size_multipliers: tuple[Decimal, ...]
    calibration_funding_refresh_seconds: int = Field(ge=30, le=3600)

    @model_validator(mode="after")
    def calibration_buckets_are_bounded(self) -> StrategyConfig:
        if not self.calibration_size_multipliers or len(self.calibration_size_multipliers) > 5:
            raise ValueError("strategy requires between one and five calibration size buckets")
        if tuple(
            sorted(set(self.calibration_size_multipliers))
        ) != self.calibration_size_multipliers or any(
            value <= 0 for value in self.calibration_size_multipliers
        ):
            raise ValueError("calibration size multipliers must be unique, positive, and sorted")
        return self


class ExecutionConfig(StrictModel):
    normal_intent: Literal["PROTECTED_AGGRESSIVE_TAKER"]
    normal_unbounded_market_allowed: Literal[False]
    emergency_unbounded_market_allowed: bool
    idempotent_client_order_ids: Literal[True]
    third_venue_emergency_hedge: bool
    latency_reserve_bps: Decimal = Field(ge=0, le=Decimal("100"))
    partial_fill_reserve_bps: Decimal = Field(ge=0, le=Decimal("100"))
    emergency_hedge_reserve_bps: Decimal = Field(ge=0, le=Decimal("500"))
    reconciliation_forced_exit_reserve_bps: Decimal = Field(ge=0, le=Decimal("500"))
    funding_stress_multiplier: Decimal = Field(ge=1, le=Decimal("10"))


class MarketDataConfig(StrictModel):
    broad_feed: Literal["bbo"]
    candidate_feed: Literal["l2"]
    max_bbo_age_ms: int = Field(gt=0)
    max_l2_age_ms: int = Field(gt=0)
    max_clock_skew_ms: int = Field(gt=0)


class UniverseConfig(StrictModel):
    live_min_listing_age_days: int = Field(ge=14, le=3650)
    instrument_refresh_seconds: int = Field(ge=60, le=86400)
    max_dynamic_l2_candidates: int = Field(ge=1, le=30)
    decision_debounce_ms: int = Field(ge=1, le=1000)


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
    canary_pair_stressed_loss_limit_usdt: Decimal = Field(gt=0, le=Decimal("1"))
    canary_effective_leverage_cap: Decimal = Field(gt=0, le=Decimal("3"))
    canary_free_margin_floor_ratio: Decimal = Field(ge=Decimal("0.20"), lt=1)
    canary_entry_slippage_cap_bps: Decimal = Field(gt=0, le=Decimal("100"))
    canary_close_slippage_cap_bps: Decimal = Field(gt=0, le=Decimal("200"))
    canary_timeout_seconds: int = Field(gt=0, le=3600)
    canary_minimum_profit_usdt: Decimal = Field(gt=0)
    qualification_max_age_seconds: int = Field(gt=0, le=604800)
    flat_barrier_consecutive_snapshots: int = Field(ge=2, le=10)
    flat_barrier_quiet_period_seconds: Decimal = Field(gt=0, le=Decimal("30"))
    flat_barrier_poll_interval_seconds: Decimal = Field(gt=0, le=Decimal("5"))
    flat_barrier_timeout_seconds: Decimal = Field(gt=0, le=Decimal("120"))


class TelegramConfig(StrictModel):
    enabled: bool
    owner_chat_id: int | None = None
    challenge_ttl_seconds: int = Field(gt=0, le=600)


class ShadowConfig(StrictModel):
    base: str
    quantity: Decimal = Field(gt=0)
    scan_interval_seconds: int = Field(gt=0, le=300)
    scan_timeout_seconds: int = Field(gt=0, le=120)
    overload_pending_limit: int = Field(gt=0, le=10000)
    history_retention_days: int = Field(gt=0, le=3650)
    qualification_min_duration_seconds: int = Field(ge=86400, le=604800)
    qualification_min_synchronised_snapshots_per_venue: int = Field(ge=10000, le=10000000)
    qualification_min_funding_checkpoints_per_venue: int = Field(ge=3, le=1000)
    qualification_max_inter_snapshot_gap_seconds: int = Field(gt=0, le=3600)
    qualification_max_sequence_gaps: int = Field(ge=0, le=10000)
    qualification_max_stale_snapshots: int = Field(ge=0, le=10000)
    qualification_max_sequence_unknown_snapshots: int = Field(ge=0, le=10000)
    qualification_max_clock_skew_snapshots: int = Field(ge=0, le=10000)

    @model_validator(mode="after")
    def base_is_valid(self) -> ShadowConfig:
        if not self.base.strip() or not self.base.isascii() or not self.base.isalnum():
            raise ValueError("shadow base must be a non-empty ASCII asset code")
        return self


class Settings(StrictModel):
    app: AppConfig
    venues: VenuesConfig
    products: ProductsConfig
    risk: RiskConfig
    strategy: StrategyConfig
    execution: ExecutionConfig
    market_data: MarketDataConfig
    universe: UniverseConfig
    storage: StorageConfig
    live: LiveConfig
    telegram: TelegramConfig
    shadow: ShadowConfig

    @model_validator(mode="after")
    def live_mode_requires_flag(self) -> Settings:
        if self.app.mode == "live" and not self.live.enabled:
            raise ValueError("live mode requires live.enabled=true")
        if (
            self.app.mode == "live"
            and self.telegram.enabled
            and self.telegram.owner_chat_id is None
        ):
            raise ValueError("live Telegram control requires an owner chat ID")
        if (
            self.shadow.scan_interval_seconds * 2
            > self.strategy.calibration_funding_refresh_seconds
        ):
            raise ValueError("shadow scan interval must leave a 50% funding refresh safety margin")
        return self


def _parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"invalid boolean environment value: {value!r}")


def _parse_optional_int(value: str) -> int | None:
    return int(value) if value.strip() else None


EnvironmentParser = Callable[[str], object]
ENVIRONMENT_BINDINGS: dict[str, tuple[str, str, EnvironmentParser]] = {
    "IPEG_MODE": ("app", "mode", str),
    "IPEG_LOG_LEVEL": ("app", "log_level", str),
    "IPEG_STATE_PATH": ("storage", "sqlite_path", str),
    "IPEG_PARQUET_DIR": ("storage", "parquet_dir", str),
    "IPEG_MAX_CLOCK_SKEW_MS": ("market_data", "max_clock_skew_ms", int),
    "IPEG_LIVE_ENABLED": ("live", "enabled", _parse_bool),
    "IPEG_TELEGRAM_ENABLED": ("telegram", "enabled", _parse_bool),
    "IPEG_TELEGRAM_OWNER_CHAT_ID": ("telegram", "owner_chat_id", _parse_optional_int),
}


def _apply_environment(raw: dict[str, object], environ: Mapping[str, str]) -> None:
    for variable, (section, key, parser) in ENVIRONMENT_BINDINGS.items():
        if variable not in environ:
            continue
        section_value = raw.get(section)
        if not isinstance(section_value, dict):
            raise ValueError(f"configuration section {section!r} must be a mapping")
        section_value[key] = parser(environ[variable])


def _require_mapping(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"configuration section {name!r} must be a mapping")
    return value


def _require_sequence(value: object, name: str) -> tuple[object, ...]:
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"configuration {name!r} must be a sequence")
    return tuple(value)


def _validate_locked_runtime_policy(raw: dict[str, object], path: Path) -> None:
    policy_path = path.parent / "RUNTIME_POLICY.yaml"
    policy = yaml.safe_load(policy_path.read_text(encoding="utf-8"))
    if not isinstance(policy, dict):
        raise ValueError("locked runtime policy root must be a mapping")
    configured_universe = _require_mapping(raw.get("universe"), "universe")
    locked_universe = _require_mapping(policy.get("universe"), "runtime_policy.universe")
    configured_data = _require_mapping(raw.get("market_data"), "market_data")
    locked_data = _require_mapping(policy.get("data"), "runtime_policy.data")
    configured_strategy = _require_mapping(raw.get("strategy"), "strategy")
    locked_strategy = _require_mapping(policy.get("strategy"), "runtime_policy.strategy")
    configured_size_multipliers = _require_sequence(
        configured_strategy.get("calibration_size_multipliers"),
        "strategy.calibration_size_multipliers",
    )
    locked_size_multipliers = _require_sequence(
        locked_strategy.get("calibration_size_multipliers"),
        "runtime_policy.strategy.calibration_size_multipliers",
    )
    comparisons = {
        "live_min_listing_age_days": (
            configured_universe.get("live_min_listing_age_days"),
            locked_universe.get("live_min_listing_age_days"),
        ),
        "instrument_refresh_seconds": (
            configured_universe.get("instrument_refresh_seconds"),
            locked_universe.get("instrument_refresh_seconds"),
        ),
        "max_dynamic_l2_candidates": (
            configured_universe.get("max_dynamic_l2_candidates"),
            locked_universe.get("max_dynamic_l2_candidates"),
        ),
        "decision_debounce_ms": (
            configured_universe.get("decision_debounce_ms"),
            locked_universe.get("decision_debounce_ms"),
        ),
        "max_bbo_age_ms": (
            configured_data.get("max_bbo_age_ms"),
            locked_data.get("max_bbo_age_ms"),
        ),
        "max_l2_age_ms": (
            configured_data.get("max_l2_age_ms"),
            locked_data.get("max_l2_age_ms"),
        ),
        "calibration_size_multipliers": (
            tuple(str(value) for value in configured_size_multipliers),
            tuple(str(value) for value in locked_size_multipliers),
        ),
        "calibration_funding_refresh_seconds": (
            configured_strategy.get("calibration_funding_refresh_seconds"),
            locked_strategy.get("calibration_funding_refresh_seconds"),
        ),
    }
    for name, (configured, locked) in comparisons.items():
        if configured != locked:
            raise ValueError(f"configuration {name} differs from locked runtime policy")
    configured_change_limit = Decimal(
        str(configured_strategy.get("grid_parameter_change_limit_ratio"))
    )
    locked_change_limit = Decimal(str(locked_strategy.get("max_parameter_change_ratio_per_day")))
    if configured_change_limit > locked_change_limit:
        raise ValueError("configuration grid parameter change limit exceeds locked policy")
    configured_stress_multiplier = Decimal(str(configured_strategy.get("stressed_cost_multiplier")))
    locked_stress_multiplier = Decimal(str(locked_strategy.get("stressed_cost_multiplier")))
    if configured_stress_multiplier < locked_stress_multiplier:
        raise ValueError("configuration stressed cost multiplier is below locked policy")
    if (
        str(configured_data.get("broad_feed", "")).upper()
        != str(locked_universe.get("broad_feed", "")).upper()
    ):
        raise ValueError("configuration broad_feed differs from locked runtime policy")
    if (
        str(configured_data.get("candidate_feed", "")).upper()
        != str(locked_universe.get("candidate_feed", "")).upper()
    ):
        raise ValueError("configuration candidate_feed differs from locked runtime policy")


def load_settings(path: Path, environ: Mapping[str, str] | None = None) -> Settings:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("configuration root must be a mapping")
    merged = copy.deepcopy(raw)
    _apply_environment(merged, os.environ if environ is None else environ)
    _validate_locked_runtime_policy(merged, path)
    return Settings.model_validate(merged)
