from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime

from interexchange_perp_grid.aggressive_grid import AggressiveGridStore
from interexchange_perp_grid.aggressive_model import (
    DirectionHistoricalModel,
    DivergenceDirection,
    HistoricalReferenceModel,
    historical_model_sha256,
)
from interexchange_perp_grid.aggressive_runtime import aggressive_runtime_manifest_sha256
from interexchange_perp_grid.native_runtime import NativeRuntimeManifest

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")


@dataclass(frozen=True, slots=True)
class AggressiveFastLiveBinding:
    schema_version: int
    generated_at: datetime
    release_sha: str
    source_sha256: str
    config_sha256: str
    native_runtime_sha256: str
    decision_runtime_sha256: str
    model_sha256: str
    source_manifest_sha256: str
    reference_manifest_sha256: str
    profile_sha256: str
    route: str
    direction: DivergenceDirection
    history_days: str
    completed_episodes: int
    convergence_rate: str
    regime_clear: bool
    binding_sha256: str
    execution_authorized: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        if self.schema_version != 1 or self.execution_authorized:
            raise ValueError("aggressive fast-live binding schema is invalid")
        if self.generated_at.tzinfo is None or self.generated_at.utcoffset() is None:
            raise ValueError("aggressive fast-live binding time must be aware")
        if _COMMIT.fullmatch(self.release_sha) is None:
            raise ValueError("aggressive fast-live binding release is invalid")
        hashes = (
            self.source_sha256,
            self.config_sha256,
            self.native_runtime_sha256,
            self.decision_runtime_sha256,
            self.model_sha256,
            self.source_manifest_sha256,
            self.reference_manifest_sha256,
            self.profile_sha256,
            self.binding_sha256,
        )
        if any(_SHA256.fullmatch(value) is None for value in hashes):
            raise ValueError("aggressive fast-live binding digest is invalid")
        if not self.route or self.completed_episodes < 0:
            raise ValueError("aggressive fast-live route/history identity is invalid")
        if self.binding_sha256 != "0" * 64 and _binding_sha256(self) != self.binding_sha256:
            raise ValueError("aggressive fast-live binding hash mismatch")


def build_aggressive_fast_live_binding(
    model: HistoricalReferenceModel,
    runtime: NativeRuntimeManifest,
    grid: AggressiveGridStore,
    *,
    route: str,
    profile_sha256: str,
    now: datetime | None = None,
) -> AggressiveFastLiveBinding:
    if route == model.positive_route:
        direction = model.positive
    elif route == model.negative_route:
        direction = model.negative
    else:
        raise ValueError("fast-live route is not part of the historical model")
    if not (
        model.code_sha == runtime.release_sha and profile_sha256 == model.strategy_profile_sha256
    ):
        raise ValueError("fast-live model, runtime, or profile identity mismatch")
    model_sha = historical_model_sha256(model)
    levels = grid.levels(route)
    if (
        len(levels) != 5
        or any(level.model_sha256 != model_sha for level in levels)
        or tuple(level.trigger_bps for level in levels) != direction.levels_bps
        or tuple(level.allocated_weight for level in levels) != direction.tranche_weights
    ):
        raise ValueError("fast-live grid is not exact-bound to the model")
    unsigned = AggressiveFastLiveBinding(
        schema_version=1,
        generated_at=now or datetime.now(UTC),
        release_sha=runtime.release_sha,
        source_sha256=runtime.source_sha256,
        config_sha256=runtime.config_sha256,
        native_runtime_sha256=_native_runtime_sha256(runtime),
        decision_runtime_sha256=aggressive_runtime_manifest_sha256(
            model,
            runtime.config_sha256,
        ),
        model_sha256=model_sha,
        source_manifest_sha256=model.source_manifest_sha256,
        reference_manifest_sha256=model.reference_manifest_sha256,
        profile_sha256=profile_sha256,
        route=route,
        direction=direction.direction,
        history_days=str(model.coverage_days),
        completed_episodes=_completed_episode_count(direction),
        convergence_rate=str(direction.convergence_rate),
        regime_clear=not direction.regime_drift_blocked,
        binding_sha256="0" * 64,
    )
    return replace(unsigned, binding_sha256=_binding_sha256(unsigned))


def verify_aggressive_fast_live_binding(
    binding: AggressiveFastLiveBinding,
    model: HistoricalReferenceModel,
    runtime: NativeRuntimeManifest,
    grid: AggressiveGridStore,
    *,
    profile_sha256: str,
) -> None:
    observed = build_aggressive_fast_live_binding(
        model,
        runtime,
        grid,
        route=binding.route,
        profile_sha256=profile_sha256,
        now=binding.generated_at,
    )
    if observed != binding:
        raise ValueError("aggressive fast-live binding is no longer current")


def _completed_episode_count(direction: DirectionHistoricalModel) -> int:
    return direction.completed_episode_count


def _native_runtime_sha256(runtime: NativeRuntimeManifest) -> str:
    return hashlib.sha256(
        json.dumps(asdict(runtime), default=str, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _binding_sha256(binding: AggressiveFastLiveBinding) -> str:
    payload = asdict(binding)
    payload["binding_sha256"] = ""
    return hashlib.sha256(
        json.dumps(payload, default=str, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
