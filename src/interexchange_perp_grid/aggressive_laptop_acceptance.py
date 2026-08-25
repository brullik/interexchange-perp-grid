from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from interexchange_perp_grid.aggressive_qualification import AggressiveQualificationBinding
from interexchange_perp_grid.native_runtime import NativeRuntimeManifest

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")


@dataclass(frozen=True, slots=True)
class AggressiveLaptopStageEvidence:
    schema_version: int
    stage: str
    started_at: datetime
    ended_at: datetime
    route_identity: str
    aggressive_binding_sha256: str
    completed_level_indices: tuple[int, ...]
    completed_actions_sha256: str
    production_filled_order_count: int
    active_action_count: int
    maximum_projected_route_loss_usdt: Decimal
    stable_flat: bool
    post_flat_service_seconds: int
    accepted: bool
    blockers: tuple[str, ...]
    evidence_sha256: str
    execution_authorized: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        if self.schema_version != 1 or self.execution_authorized:
            raise ValueError("aggressive laptop stage evidence identity is invalid")
        if self.stage not in {"canary", "pilot_a"}:
            raise ValueError("aggressive laptop stage is invalid")
        if any(
            value.tzinfo is None or value.utcoffset() is None
            for value in (self.started_at, self.ended_at)
        ):
            raise ValueError("aggressive laptop stage timestamps must be aware")
        if self.ended_at < self.started_at:
            raise ValueError("aggressive laptop stage timestamps are out of order")
        if any(
            _SHA256.fullmatch(value) is None
            for value in (
                self.aggressive_binding_sha256,
                self.completed_actions_sha256,
                self.evidence_sha256,
            )
        ):
            raise ValueError("aggressive laptop stage digest is invalid")
        if (
            self.production_filled_order_count < 0
            or self.active_action_count < 0
            or self.post_flat_service_seconds < 0
            or not self.maximum_projected_route_loss_usdt.is_finite()
            or self.maximum_projected_route_loss_usdt < 0
        ):
            raise ValueError("aggressive laptop stage counters are invalid")


@dataclass(frozen=True, slots=True)
class AggressiveLaptopAcceptance:
    schema_version: int
    accepted_at: datetime
    release_sha: str
    source_sha256: str
    config_sha256: str
    profile_sha256: str
    model_sha256: str
    source_manifest_sha256: str
    reference_manifest_sha256: str
    runtime_artifact_digest: str
    qualification_hash: str
    aggressive_binding_sha256: str
    canary_evidence_sha256: str
    pilot_evidence_sha256: str
    route_identity: str
    stable_flat: bool
    post_flat_service_seconds: int
    accepted: bool
    acceptance_sha256: str
    execution_authorized: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        if self.schema_version != 1 or self.execution_authorized:
            raise ValueError("aggressive laptop acceptance identity is invalid")
        if self.accepted_at.tzinfo is None or self.accepted_at.utcoffset() is None:
            raise ValueError("aggressive laptop acceptance time must be aware")
        if _COMMIT.fullmatch(self.release_sha) is None:
            raise ValueError("aggressive laptop acceptance release is invalid")
        digests = (
            self.source_sha256,
            self.config_sha256,
            self.profile_sha256,
            self.model_sha256,
            self.source_manifest_sha256,
            self.reference_manifest_sha256,
            self.qualification_hash,
            self.aggressive_binding_sha256,
            self.canary_evidence_sha256,
            self.pilot_evidence_sha256,
            self.acceptance_sha256,
        )
        if any(_SHA256.fullmatch(value) is None for value in digests):
            raise ValueError("aggressive laptop acceptance digest is invalid")
        if not self.runtime_artifact_digest.startswith("sha256:"):
            raise ValueError("aggressive laptop runtime digest is invalid")


def build_aggressive_laptop_acceptance(
    binding: AggressiveQualificationBinding,
    runtime: NativeRuntimeManifest,
    canary: AggressiveLaptopStageEvidence,
    pilot: AggressiveLaptopStageEvidence,
    *,
    now: datetime | None = None,
) -> AggressiveLaptopAcceptance:
    _verify_stage_evidence(canary)
    _verify_stage_evidence(pilot)
    if not (
        binding.accepted
        and canary.accepted
        and pilot.accepted
        and binding.release_sha == runtime.release_sha
        and binding.source_sha256 == runtime.source_sha256
        and binding.config_sha256 == runtime.config_sha256
        and binding.runtime_artifact_digest == runtime.artifact_digest
    ):
        raise ValueError("aggressive laptop identity or stage evidence is not accepted")
    if canary.aggressive_binding_sha256 != binding.binding_sha256 or (
        pilot.aggressive_binding_sha256 != binding.binding_sha256
    ):
        raise ValueError("aggressive laptop stage uses a different qualification binding")
    if canary.route_identity != pilot.route_identity:
        raise ValueError("aggressive laptop stages use different routes")
    if (
        canary.stage != "canary"
        or canary.completed_level_indices != (1,)
        or canary.maximum_projected_route_loss_usdt > Decimal(1)
        or canary.production_filled_order_count < 4
    ):
        raise ValueError("aggressive canary evidence does not prove the locked minimum stage")
    if (
        pilot.stage != "pilot_a"
        or pilot.completed_level_indices != (1, 2, 3, 4, 5)
        or pilot.maximum_projected_route_loss_usdt > Decimal(5)
        or pilot.production_filled_order_count < 20
        or pilot.post_flat_service_seconds < 28_800
    ):
        raise ValueError("aggressive pilot evidence does not prove five levels and service time")
    if (
        not canary.stable_flat
        or not pilot.stable_flat
        or any(evidence.active_action_count for evidence in (canary, pilot))
    ):
        raise ValueError("aggressive laptop stages did not finish stable FLAT")
    unsigned = AggressiveLaptopAcceptance(
        schema_version=1,
        accepted_at=now or datetime.now(UTC),
        release_sha=runtime.release_sha,
        source_sha256=runtime.source_sha256,
        config_sha256=runtime.config_sha256,
        profile_sha256=binding.profile_sha256,
        model_sha256=binding.model_sha256,
        source_manifest_sha256=binding.source_manifest_sha256,
        reference_manifest_sha256=binding.reference_manifest_sha256,
        runtime_artifact_digest=runtime.artifact_digest,
        qualification_hash=binding.qualification_hash,
        aggressive_binding_sha256=binding.binding_sha256,
        canary_evidence_sha256=canary.evidence_sha256,
        pilot_evidence_sha256=pilot.evidence_sha256,
        route_identity=pilot.route_identity,
        stable_flat=True,
        post_flat_service_seconds=pilot.post_flat_service_seconds,
        accepted=True,
        acceptance_sha256="0" * 64,
    )
    return replace(unsigned, acceptance_sha256=_artifact_sha256(unsigned, "acceptance_sha256"))


def build_aggressive_laptop_stage_evidence(
    *,
    stage: str,
    started_at: datetime,
    ended_at: datetime,
    route_identity: str,
    aggressive_binding_sha256: str,
    completed_level_indices: tuple[int, ...],
    completed_actions_sha256: str,
    production_filled_order_count: int,
    active_action_count: int,
    maximum_projected_route_loss_usdt: Decimal,
    stable_flat: bool,
    post_flat_service_seconds: int,
    blockers: tuple[str, ...] = (),
) -> AggressiveLaptopStageEvidence:
    unsigned = AggressiveLaptopStageEvidence(
        schema_version=1,
        stage=stage,
        started_at=started_at,
        ended_at=ended_at,
        route_identity=route_identity,
        aggressive_binding_sha256=aggressive_binding_sha256,
        completed_level_indices=completed_level_indices,
        completed_actions_sha256=completed_actions_sha256,
        production_filled_order_count=production_filled_order_count,
        active_action_count=active_action_count,
        maximum_projected_route_loss_usdt=maximum_projected_route_loss_usdt,
        stable_flat=stable_flat,
        post_flat_service_seconds=post_flat_service_seconds,
        accepted=not blockers,
        blockers=blockers,
        evidence_sha256="0" * 64,
    )
    return replace(unsigned, evidence_sha256=_artifact_sha256(unsigned, "evidence_sha256"))


def save_aggressive_laptop_stage_evidence(
    path: Path,
    evidence: AggressiveLaptopStageEvidence,
) -> None:
    _verify_stage_evidence(evidence)
    _atomic_write(path, evidence)


def load_aggressive_laptop_stage_evidence(path: Path) -> AggressiveLaptopStageEvidence:
    payload = _load_object(path)
    evidence = AggressiveLaptopStageEvidence(
        schema_version=int(str(payload["schema_version"])),
        stage=str(payload["stage"]),
        started_at=datetime.fromisoformat(str(payload["started_at"])),
        ended_at=datetime.fromisoformat(str(payload["ended_at"])),
        route_identity=str(payload["route_identity"]),
        aggressive_binding_sha256=str(payload["aggressive_binding_sha256"]),
        completed_level_indices=tuple(
            int(str(item)) for item in _list_value(payload, "completed_level_indices")
        ),
        completed_actions_sha256=str(payload["completed_actions_sha256"]),
        production_filled_order_count=int(str(payload["production_filled_order_count"])),
        active_action_count=int(str(payload["active_action_count"])),
        maximum_projected_route_loss_usdt=Decimal(
            str(payload["maximum_projected_route_loss_usdt"])
        ),
        stable_flat=_bool_value(payload, "stable_flat"),
        post_flat_service_seconds=int(str(payload["post_flat_service_seconds"])),
        accepted=_bool_value(payload, "accepted"),
        blockers=tuple(str(item) for item in _list_value(payload, "blockers")),
        evidence_sha256=str(payload["evidence_sha256"]),
    )
    _verify_stage_evidence(evidence)
    return evidence


def save_aggressive_laptop_acceptance(path: Path, acceptance: AggressiveLaptopAcceptance) -> None:
    if _artifact_sha256(acceptance, "acceptance_sha256") != acceptance.acceptance_sha256:
        raise ValueError("aggressive laptop acceptance hash mismatch")
    _atomic_write(path, acceptance)


def load_aggressive_laptop_acceptance(path: Path) -> AggressiveLaptopAcceptance:
    payload = _load_object(path)
    acceptance = AggressiveLaptopAcceptance(
        schema_version=int(str(payload["schema_version"])),
        accepted_at=datetime.fromisoformat(str(payload["accepted_at"])),
        release_sha=str(payload["release_sha"]),
        source_sha256=str(payload["source_sha256"]),
        config_sha256=str(payload["config_sha256"]),
        profile_sha256=str(payload["profile_sha256"]),
        model_sha256=str(payload["model_sha256"]),
        source_manifest_sha256=str(payload["source_manifest_sha256"]),
        reference_manifest_sha256=str(payload["reference_manifest_sha256"]),
        runtime_artifact_digest=str(payload["runtime_artifact_digest"]),
        qualification_hash=str(payload["qualification_hash"]),
        aggressive_binding_sha256=str(payload["aggressive_binding_sha256"]),
        canary_evidence_sha256=str(payload["canary_evidence_sha256"]),
        pilot_evidence_sha256=str(payload["pilot_evidence_sha256"]),
        route_identity=str(payload["route_identity"]),
        stable_flat=_bool_value(payload, "stable_flat"),
        post_flat_service_seconds=int(str(payload["post_flat_service_seconds"])),
        accepted=_bool_value(payload, "accepted"),
        acceptance_sha256=str(payload["acceptance_sha256"]),
    )
    if _artifact_sha256(acceptance, "acceptance_sha256") != acceptance.acceptance_sha256:
        raise ValueError("aggressive laptop acceptance hash mismatch")
    return acceptance


def verify_aggressive_laptop_handoff(
    acceptance: AggressiveLaptopAcceptance,
    runtime: NativeRuntimeManifest,
) -> None:
    if not (
        acceptance.accepted
        and acceptance.stable_flat
        and acceptance.post_flat_service_seconds >= 28_800
        and acceptance.release_sha == runtime.release_sha
        and acceptance.source_sha256 == runtime.source_sha256
        and acceptance.config_sha256 == runtime.config_sha256
        and acceptance.runtime_artifact_digest == runtime.artifact_digest
    ):
        raise ValueError("aggressive VPS handoff is blocked without exact laptop acceptance")


def _verify_stage_evidence(evidence: AggressiveLaptopStageEvidence) -> None:
    if _artifact_sha256(evidence, "evidence_sha256") != evidence.evidence_sha256:
        raise ValueError("aggressive laptop stage evidence hash mismatch")
    if evidence.accepted != (not evidence.blockers):
        raise ValueError("aggressive laptop stage acceptance contradicts its blockers")


def _artifact_sha256(
    value: AggressiveLaptopStageEvidence | AggressiveLaptopAcceptance,
    hash_field: str,
) -> str:
    payload = asdict(value)
    payload[hash_field] = ""
    encoded = json.dumps(payload, default=str, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _load_object(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("aggressive laptop artifact must be an object")
    return payload


def _list_value(payload: dict[str, object], key: str) -> list[object]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise ValueError(f"aggressive laptop field {key} must be an array")
    return value


def _bool_value(payload: dict[str, object], key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise ValueError(f"aggressive laptop field {key} must be boolean")
    return value


def _atomic_write(
    path: Path,
    value: AggressiveLaptopStageEvidence | AggressiveLaptopAcceptance,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            stream.write(json.dumps(asdict(value), default=str, indent=2, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
