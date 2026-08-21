from __future__ import annotations

import base64
import binascii
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from itertools import pairwise
from pathlib import Path
from typing import Any

import yaml
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

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
        if any(
            isinstance(value, bool) or not isinstance(value, int)
            for value in (self.routes, self.tranches)
        ):
            raise ValueError("risk stage route/tranche limits must be exact integers")
        if not 1 <= self.routes <= 10 or not 1 <= self.tranches <= 5:
            raise ValueError("risk stage route/tranche limits are outside locked bounds")
        if any(
            isinstance(value, bool) or not isinstance(value, Decimal)
            for value in (self.pair_usdt, self.portfolio_usdt, self.leverage)
        ):
            raise ValueError("risk stage monetary/leverage limits must be Decimal values")
        values = (self.pair_usdt, self.portfolio_usdt, self.leverage)
        if any(not value.is_finite() or value <= 0 for value in values):
            raise ValueError("risk stage monetary/leverage limits must be positive and finite")
        if (
            self.pair_usdt > Decimal("5")
            or self.portfolio_usdt > Decimal("50")
            or self.leverage > 3
            or self.portfolio_usdt < self.pair_usdt
        ):
            raise ValueError("risk stage leverage or portfolio bound is invalid")


@dataclass(frozen=True, slots=True)
class LockedRiskStageTable:
    runtime_policy_sha256: str
    flat_barrier_snapshots: int
    flat_barrier_quiet_seconds: Decimal
    hard_maximum_holding_seconds: int
    stages: tuple[RiskStageLimits, ...]


@dataclass(frozen=True, slots=True)
class RiskStageCompletionEvidence:
    stage: RiskStage
    qualification_hash: str
    runtime_policy_sha256: str
    release_sha: str
    source_sha256: str
    config_sha256: str
    container_image_digest: str
    stage_started_at: datetime
    stage_ended_at: datetime
    completed_pair_action_ids: tuple[str, ...]
    completed_pair_actions_sha256: str
    evidence_sha256: str


_STAGE_MINIMUM_DURATION_SECONDS = {
    RiskStage.CANARY: 0,
    RiskStage.PILOT_A: 86_400,
    RiskStage.PILOT_B: 259_200,
    RiskStage.WAVE1_PROD: 604_800,
    RiskStage.FULL: 0,
}

_COMPLETION_KEYS = {
    "schema_version",
    "producer",
    "stage",
    "qualification_hash",
    "runtime_policy_sha256",
    "release_sha",
    "source_sha256",
    "config_sha256",
    "container_image_digest",
    "stage_started_at",
    "stage_ended_at",
    "stable_flat_verified",
    "stable_flat_consecutive_snapshots",
    "quiet_period_seconds",
    "private_snapshots_complete",
    "first_snapshot_sha256",
    "second_snapshot_sha256",
    "event_watermark_before",
    "event_watermark_after",
    "active_action_count",
    "raw_open_order_count",
    "raw_nonzero_position_count",
    "unknown_active_record_count",
    "unresolved_order_count",
    "unresolved_exposure_count",
    "liquidation_count",
    "adl_count",
    "manual_emergency_intervention_count",
    "risk_invariant_violation_count",
    "availability_ratio",
    "private_completeness_ratio",
    "realized_net_pnl_usdt",
    "maximum_realized_loss_usdt",
    "maximum_holding_seconds_observed",
    "completed_pair_cycle_count",
    "completed_pair_action_ids",
    "completed_pair_actions_sha256",
    "operator_signature_ed25519",
}


def _exact_int(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"risk stage evidence {key} must be an exact integer")
    return value


def _decimal_string(payload: dict[str, Any], key: str) -> Decimal:
    value = payload.get(key)
    if not isinstance(value, str):
        raise ValueError(f"risk stage evidence {key} must be a decimal string")
    try:
        parsed = Decimal(value)
    except Exception as error:
        raise ValueError(f"risk stage evidence {key} is invalid") from error
    if not parsed.is_finite():
        raise ValueError(f"risk stage evidence {key} must be finite")
    return parsed


def _aware_datetime(payload: dict[str, Any], key: str) -> datetime:
    value = payload.get(key)
    if not isinstance(value, str):
        raise ValueError(f"risk stage evidence {key} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"risk stage evidence {key} is invalid") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"risk stage evidence {key} must be timezone-aware")
    return parsed


def verify_risk_stage_completion_evidence(
    evidence_path: Path,
    public_key_path: Path,
    expected_public_key_sha256: str,
    limits: RiskStageLimits,
    *,
    required_consecutive_snapshots: int,
    required_quiet_period_seconds: Decimal,
    hard_maximum_holding_seconds: int,
    observed_at: datetime | None = None,
    maximum_clock_skew_seconds: Decimal = Decimal("1"),
) -> RiskStageCompletionEvidence:
    """Verify one signed, exact-schema, fail-closed stage completion artifact."""
    raw = evidence_path.read_bytes()
    payload = json.loads(raw)
    if not isinstance(payload, dict) or set(payload) != _COMPLETION_KEYS:
        raise ValueError("risk stage evidence has an invalid exact schema")
    if (
        _exact_int(payload, "schema_version") != 1
        or payload.get("producer") != "ipegctl-stage-result-v1"
    ):
        raise ValueError("risk stage evidence producer/schema is invalid")
    if payload.get("stage") != limits.stage.value:
        raise ValueError("risk stage evidence stage does not match the locked stage")
    hashes = (
        "qualification_hash",
        "runtime_policy_sha256",
        "source_sha256",
        "config_sha256",
        "first_snapshot_sha256",
        "second_snapshot_sha256",
        "completed_pair_actions_sha256",
    )
    if any(
        not isinstance(payload.get(key), str)
        or len(str(payload[key])) != 64
        or any(character not in "0123456789abcdef" for character in str(payload[key]))
        for key in hashes
    ):
        raise ValueError("risk stage evidence contains an invalid SHA-256 identity")
    release_sha = payload.get("release_sha")
    image_digest = payload.get("container_image_digest")
    if (
        not isinstance(release_sha, str)
        or len(release_sha) != 40
        or any(character not in "0123456789abcdef" for character in release_sha)
        or not isinstance(image_digest, str)
        or not image_digest.startswith("sha256:")
        or len(image_digest) != 71
        or any(character not in "0123456789abcdef" for character in image_digest[7:])
    ):
        raise ValueError("risk stage evidence release/image identity is invalid")
    started_at = _aware_datetime(payload, "stage_started_at")
    ended_at = _aware_datetime(payload, "stage_ended_at")
    checked_at = observed_at or datetime.now(UTC)
    if (
        ended_at < started_at
        or (ended_at - started_at).total_seconds() < _STAGE_MINIMUM_DURATION_SECONDS[limits.stage]
        or Decimal(str((ended_at - checked_at).total_seconds())) > maximum_clock_skew_seconds
    ):
        raise ValueError("risk stage evidence does not cover the locked stage duration")
    exact_zero_counts = (
        "active_action_count",
        "raw_open_order_count",
        "raw_nonzero_position_count",
        "unknown_active_record_count",
        "unresolved_order_count",
        "unresolved_exposure_count",
        "liquidation_count",
        "adl_count",
        "manual_emergency_intervention_count",
        "risk_invariant_violation_count",
    )
    if any(_exact_int(payload, key) != 0 for key in exact_zero_counts):
        raise ValueError("risk stage evidence contains unresolved risk or account state")
    completed_count = _exact_int(payload, "completed_pair_cycle_count")
    completed_ids = payload.get("completed_pair_action_ids")
    if (
        completed_count < 1
        or not isinstance(completed_ids, list)
        or any(not isinstance(value, str) or not value.strip() for value in completed_ids)
        or completed_ids != sorted(set(completed_ids))
        or len(completed_ids) != completed_count
    ):
        raise ValueError("risk stage evidence requires durable completed paired cycles")
    if (
        payload.get("stable_flat_verified") is not True
        or payload.get("private_snapshots_complete") is not True
        or _exact_int(payload, "stable_flat_consecutive_snapshots") < required_consecutive_snapshots
        or _decimal_string(payload, "quiet_period_seconds") < required_quiet_period_seconds
        or payload["first_snapshot_sha256"] != payload["second_snapshot_sha256"]
        or _exact_int(payload, "event_watermark_before") < 0
        or _exact_int(payload, "event_watermark_before")
        != _exact_int(payload, "event_watermark_after")
    ):
        raise ValueError("risk stage evidence does not prove stable account-wide FLAT")
    availability = _decimal_string(payload, "availability_ratio")
    completeness = _decimal_string(payload, "private_completeness_ratio")
    pnl = _decimal_string(payload, "realized_net_pnl_usdt")
    maximum_loss = _decimal_string(payload, "maximum_realized_loss_usdt")
    maximum_holding = _exact_int(payload, "maximum_holding_seconds_observed")
    if (
        not Decimal("0.99") <= availability <= 1
        or not Decimal("0.999") <= completeness <= 1
        or pnl < 0
        or maximum_loss < 0
        or maximum_loss > limits.portfolio_usdt
        or maximum_holding < 0
        or maximum_holding > hard_maximum_holding_seconds
    ):
        raise ValueError("risk stage evidence violates locked promotion metrics")
    key_payload = json.loads(public_key_path.read_text(encoding="utf-8"))
    if (
        not isinstance(key_payload, dict)
        or set(key_payload) != {"schema_version", "algorithm", "public_key_base64"}
        or _exact_int(key_payload, "schema_version") != 1
        or key_payload.get("algorithm") != "Ed25519"
        or not isinstance(key_payload.get("public_key_base64"), str)
    ):
        raise ValueError("invalid stage-evidence public key")
    try:
        public_key_raw = base64.b64decode(key_payload["public_key_base64"], validate=True)
        public_key = Ed25519PublicKey.from_public_bytes(public_key_raw)
        signature = base64.b64decode(payload["operator_signature_ed25519"], validate=True)
    except (ValueError, TypeError, binascii.Error) as error:
        raise ValueError("invalid stage-evidence key or signature") from error
    if hashlib.sha256(public_key_raw).hexdigest() != expected_public_key_sha256:
        raise ValueError("stage-evidence public key is not pinned by locked policy")
    signed_payload = {
        key: payload[key] for key in _COMPLETION_KEYS if key != "operator_signature_ed25519"
    }
    try:
        public_key.verify(
            signature,
            json.dumps(signed_payload, sort_keys=True, separators=(",", ":")).encode(),
        )
    except (InvalidSignature, ValueError, TypeError) as error:
        raise ValueError("risk stage evidence signature is invalid") from error
    return RiskStageCompletionEvidence(
        stage=limits.stage,
        qualification_hash=str(payload["qualification_hash"]),
        runtime_policy_sha256=str(payload["runtime_policy_sha256"]),
        release_sha=release_sha,
        source_sha256=str(payload["source_sha256"]),
        config_sha256=str(payload["config_sha256"]),
        container_image_digest=image_digest,
        stage_started_at=started_at,
        stage_ended_at=ended_at,
        completed_pair_action_ids=tuple(completed_ids),
        completed_pair_actions_sha256=str(payload["completed_pair_actions_sha256"]),
        evidence_sha256=hashlib.sha256(raw).hexdigest(),
    )


def load_locked_risk_stage_table(runtime_policy_path: Path) -> LockedRiskStageTable:
    raw_bytes = runtime_policy_path.read_bytes()
    payload = yaml.safe_load(raw_bytes)
    if not isinstance(payload, dict) or not isinstance(payload.get("risk_stages"), dict):
        raise ValueError("locked runtime policy risk_stages must be a mapping")
    raw_stages = payload["risk_stages"]
    raw_data = payload.get("data")
    raw_strategy = payload.get("strategy")
    if not isinstance(raw_data, dict) or not isinstance(raw_strategy, dict):
        raise ValueError("locked runtime policy data/strategy sections are required")
    flat_snapshots = raw_data.get("flat_barrier_snapshots")
    hard_maximum_holding = raw_strategy.get("hard_max_hold_seconds")
    if (
        isinstance(flat_snapshots, bool)
        or not isinstance(flat_snapshots, int)
        or flat_snapshots < 2
        or isinstance(hard_maximum_holding, bool)
        or not isinstance(hard_maximum_holding, int)
        or hard_maximum_holding <= 0
    ):
        raise ValueError("locked barrier/holding limits require exact positive integers")
    try:
        flat_quiet_seconds = Decimal(str(raw_data["flat_barrier_quiet_seconds"]))
    except Exception as error:
        raise ValueError("locked flat barrier quiet period is invalid") from error
    if not flat_quiet_seconds.is_finite() or flat_quiet_seconds <= 0:
        raise ValueError("locked flat barrier quiet period must be positive and finite")
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
        routes = raw["routes"]
        tranches = raw["tranches"]
        if any(
            isinstance(value, bool) or not isinstance(value, int) for value in (routes, tranches)
        ):
            raise ValueError(f"locked risk stage {stage.value} requires exact integer limits")
        try:
            pair_usdt = Decimal(str(raw["pair_usdt"]))
            portfolio_usdt = Decimal(str(raw["portfolio_usdt"]))
            leverage = Decimal(str(raw["leverage"]))
        except Exception as error:
            raise ValueError(
                f"locked risk stage {stage.value} has invalid decimal limits"
            ) from error
        stages.append(
            RiskStageLimits(
                stage=stage,
                routes=routes,
                tranches=tranches,
                pair_usdt=pair_usdt,
                portfolio_usdt=portfolio_usdt,
                leverage=leverage,
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
        flat_barrier_snapshots=flat_snapshots,
        flat_barrier_quiet_seconds=flat_quiet_seconds,
        hard_maximum_holding_seconds=hard_maximum_holding,
        stages=tuple(stages),
    )
