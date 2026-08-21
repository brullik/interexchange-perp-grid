from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal
from itertools import pairwise
from pathlib import Path

import yaml

from interexchange_perp_grid.state import RISK_STAGE_ORDER, RiskStage


@dataclass(frozen=True, slots=True)
class RiskStageLimits:
    stage: RiskStage
    routes: int
    tranches: int
    pair_usdt: Decimal
    portfolio_usdt: Decimal
    leverage: Decimal

    def __post_init__(self) -> None:
        if self.stage == RiskStage.SHADOW:
            raise ValueError("shadow has no live risk allocation")
        if not 1 <= self.routes <= 10 or not 1 <= self.tranches <= 5:
            raise ValueError("risk stage route/tranche limits are outside locked bounds")
        values = (self.pair_usdt, self.portfolio_usdt, self.leverage)
        if any(not value.is_finite() or value <= 0 for value in values):
            raise ValueError("risk stage monetary/leverage limits must be positive and finite")
        if self.leverage > 3 or self.portfolio_usdt < self.pair_usdt:
            raise ValueError("risk stage leverage or portfolio bound is invalid")


@dataclass(frozen=True, slots=True)
class LockedRiskStageTable:
    runtime_policy_sha256: str
    stages: tuple[RiskStageLimits, ...]


def load_locked_risk_stage_table(runtime_policy_path: Path) -> LockedRiskStageTable:
    raw_bytes = runtime_policy_path.read_bytes()
    payload = yaml.safe_load(raw_bytes)
    if not isinstance(payload, dict) or not isinstance(payload.get("risk_stages"), dict):
        raise ValueError("locked runtime policy risk_stages must be a mapping")
    raw_stages = payload["risk_stages"]
    expected = RISK_STAGE_ORDER[1:]
    if tuple(raw_stages) != tuple(stage.value for stage in expected):
        raise ValueError("locked risk stages must use the exact promotion order")
    stages: list[RiskStageLimits] = []
    for stage in expected:
        raw = raw_stages.get(stage.value)
        if not isinstance(raw, dict) or set(raw) != {
            "routes",
            "tranches",
            "pair_usdt",
            "portfolio_usdt",
            "leverage",
        }:
            raise ValueError(f"locked risk stage {stage.value} has an invalid schema")
        stages.append(
            RiskStageLimits(
                stage=stage,
                routes=int(raw["routes"]),
                tranches=int(raw["tranches"]),
                pair_usdt=Decimal(str(raw["pair_usdt"])),
                portfolio_usdt=Decimal(str(raw["portfolio_usdt"])),
                leverage=Decimal(str(raw["leverage"])),
            )
        )
    for previous, current in pairwise(stages):
        if (
            current.routes < previous.routes
            or current.tranches < previous.tranches
            or current.pair_usdt < previous.pair_usdt
            or current.portfolio_usdt < previous.portfolio_usdt
        ):
            raise ValueError("locked risk allocation cannot regress during promotion")
    return LockedRiskStageTable(
        runtime_policy_sha256=hashlib.sha256(raw_bytes).hexdigest(),
        stages=tuple(stages),
    )
