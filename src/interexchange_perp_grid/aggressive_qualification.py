from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from interexchange_perp_grid.aggressive_grid import AggressiveGridStore
from interexchange_perp_grid.aggressive_model import (
    DirectionHistoricalModel,
    HistoricalReferenceModel,
    historical_model_sha256,
)
from interexchange_perp_grid.aggressive_runtime import aggressive_runtime_manifest_sha256
from interexchange_perp_grid.native_runtime import NativeRuntimeManifest
from interexchange_perp_grid.qualification import QualificationEvidence

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_ARTIFACT = re.compile(r"^sha256:[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class AggressiveDirectionBinding:
    route_identity: str
    levels_bps: tuple[Decimal, ...]
    tranche_weights: tuple[Decimal, ...]
    reference_stop_bps: Decimal

    def __post_init__(self) -> None:
        if not self.route_identity or len(self.levels_bps) != 5 or len(self.tranche_weights) != 5:
            raise ValueError("aggressive direction binding must contain exact five-level geometry")
        if any(not value.is_finite() for value in (*self.levels_bps, *self.tranche_weights)):
            raise ValueError("aggressive direction geometry must be finite")
        if sum(self.tranche_weights) != 1 or not self.reference_stop_bps.is_finite():
            raise ValueError("aggressive direction weights or stop are invalid")


@dataclass(frozen=True, slots=True)
class AggressiveQualificationBinding:
    schema_version: int
    generated_at: datetime
    qualification_hash: str
    qualification_data_sha256: str
    release_sha: str
    source_sha256: str
    config_sha256: str
    runtime_artifact_digest: str
    decision_runtime_sha256: str
    model_sha256: str
    source_manifest_sha256: str
    reference_manifest_sha256: str
    profile_sha256: str
    qualification_route: str
    positive: AggressiveDirectionBinding
    negative: AggressiveDirectionBinding
    accepted: bool
    binding_sha256: str
    execution_authorized: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        if self.schema_version != 1 or not self.accepted or self.execution_authorized:
            raise ValueError("aggressive qualification binding is not accepted")
        if self.generated_at.tzinfo is None or self.generated_at.utcoffset() is None:
            raise ValueError("aggressive qualification time must be timezone-aware")
        digests = (
            self.qualification_hash,
            self.qualification_data_sha256,
            self.source_sha256,
            self.config_sha256,
            self.model_sha256,
            self.decision_runtime_sha256,
            self.source_manifest_sha256,
            self.reference_manifest_sha256,
            self.profile_sha256,
            self.binding_sha256,
        )
        if any(_SHA256.fullmatch(value) is None for value in digests):
            raise ValueError("aggressive qualification digest is invalid")
        if _COMMIT.fullmatch(self.release_sha) is None:
            raise ValueError("aggressive qualification release is invalid")
        if _ARTIFACT.fullmatch(self.runtime_artifact_digest) is None:
            raise ValueError("aggressive qualification runtime digest is invalid")


def build_aggressive_qualification_binding(
    qualification: QualificationEvidence,
    model: HistoricalReferenceModel,
    runtime: NativeRuntimeManifest,
    grid: AggressiveGridStore,
    *,
    profile_sha256: str,
    now: datetime | None = None,
) -> AggressiveQualificationBinding:
    if not qualification.accepted or qualification.route is None:
        raise ValueError("accepted route-specific qualification is required")
    route = qualification.route.value
    if route not in {model.positive_route, model.negative_route}:
        raise ValueError("qualification route is not part of the aggressive model")
    if not (
        qualification.code_commit_sha == runtime.release_sha == model.code_sha
        and qualification.code_sha256 == runtime.source_sha256
        and qualification.config_sha256 == runtime.config_sha256
    ):
        raise ValueError("qualification, runtime, model, or config identity mismatch")
    if profile_sha256 != model.strategy_profile_sha256:
        raise ValueError("aggressive strategy profile identity mismatch")
    model_sha = historical_model_sha256(model)
    for route_identity, direction in (
        (model.positive_route, model.positive),
        (model.negative_route, model.negative),
    ):
        levels = grid.levels(route_identity)
        if any(level.model_sha256 != model_sha for level in levels):
            raise ValueError("aggressive grid model identity mismatch")
        if tuple(level.trigger_bps for level in levels) != direction.levels_bps:
            raise ValueError("aggressive grid geometry mismatch")
        if tuple(level.allocated_weight for level in levels) != direction.tranche_weights:
            raise ValueError("aggressive grid weights mismatch")
    unsigned = AggressiveQualificationBinding(
        schema_version=1,
        generated_at=now or datetime.now(UTC),
        qualification_hash=qualification.qualification_hash,
        qualification_data_sha256=qualification.data_sha256,
        release_sha=runtime.release_sha,
        source_sha256=runtime.source_sha256,
        config_sha256=runtime.config_sha256,
        runtime_artifact_digest=runtime.artifact_digest,
        decision_runtime_sha256=aggressive_runtime_manifest_sha256(
            model,
            runtime.config_sha256,
        ),
        model_sha256=model_sha,
        source_manifest_sha256=model.source_manifest_sha256,
        reference_manifest_sha256=model.reference_manifest_sha256,
        profile_sha256=profile_sha256,
        qualification_route=route,
        positive=_direction_binding(model.positive_route, model.positive),
        negative=_direction_binding(model.negative_route, model.negative),
        accepted=True,
        binding_sha256="0" * 64,
    )
    return replace(unsigned, binding_sha256=_binding_sha256(unsigned))


def save_aggressive_qualification_binding(
    path: Path,
    binding: AggressiveQualificationBinding,
) -> None:
    if _binding_sha256(binding) != binding.binding_sha256:
        raise ValueError("aggressive qualification binding hash mismatch")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            stream.write(json.dumps(asdict(binding), default=str, indent=2, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def load_aggressive_qualification_binding(path: Path) -> AggressiveQualificationBinding:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("aggressive qualification binding must be an object")
    binding = AggressiveQualificationBinding(
        schema_version=int(payload["schema_version"]),
        generated_at=datetime.fromisoformat(str(payload["generated_at"])),
        qualification_hash=str(payload["qualification_hash"]),
        qualification_data_sha256=str(payload["qualification_data_sha256"]),
        release_sha=str(payload["release_sha"]),
        source_sha256=str(payload["source_sha256"]),
        config_sha256=str(payload["config_sha256"]),
        runtime_artifact_digest=str(payload["runtime_artifact_digest"]),
        decision_runtime_sha256=str(payload["decision_runtime_sha256"]),
        model_sha256=str(payload["model_sha256"]),
        source_manifest_sha256=str(payload["source_manifest_sha256"]),
        reference_manifest_sha256=str(payload["reference_manifest_sha256"]),
        profile_sha256=str(payload["profile_sha256"]),
        qualification_route=str(payload["qualification_route"]),
        positive=_load_direction(payload["positive"]),
        negative=_load_direction(payload["negative"]),
        accepted=bool(payload["accepted"]),
        binding_sha256=str(payload["binding_sha256"]),
    )
    if _binding_sha256(binding) != binding.binding_sha256:
        raise ValueError("aggressive qualification binding hash mismatch")
    return binding


def verify_aggressive_qualification_binding(
    binding: AggressiveQualificationBinding,
    qualification: QualificationEvidence,
    model: HistoricalReferenceModel,
    runtime: NativeRuntimeManifest,
    grid: AggressiveGridStore,
    *,
    profile_sha256: str,
) -> None:
    observed = build_aggressive_qualification_binding(
        qualification,
        model,
        runtime,
        grid,
        profile_sha256=profile_sha256,
        now=binding.generated_at,
    )
    if observed != binding:
        raise ValueError("aggressive qualification binding is no longer current")


def _direction_binding(
    route_identity: str,
    direction: DirectionHistoricalModel,
) -> AggressiveDirectionBinding:
    return AggressiveDirectionBinding(
        route_identity,
        direction.levels_bps,
        direction.tranche_weights,
        direction.reference_stop_bps,
    )


def _load_direction(value: object) -> AggressiveDirectionBinding:
    if not isinstance(value, dict):
        raise ValueError("aggressive direction binding must be an object")
    levels = value.get("levels_bps")
    weights = value.get("tranche_weights")
    if not isinstance(levels, list) or not isinstance(weights, list):
        raise ValueError("aggressive direction arrays are invalid")
    return AggressiveDirectionBinding(
        route_identity=str(value["route_identity"]),
        levels_bps=tuple(Decimal(str(item)) for item in levels),
        tranche_weights=tuple(Decimal(str(item)) for item in weights),
        reference_stop_bps=Decimal(str(value["reference_stop_bps"])),
    )


def _binding_sha256(binding: AggressiveQualificationBinding) -> str:
    payload = asdict(binding)
    payload["binding_sha256"] = ""
    encoded = json.dumps(payload, default=str, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()
