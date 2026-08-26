from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from interexchange_perp_grid.reason_codes import ReasonCode

FAST_LIVE_PREFLIGHT_TTL_SECONDS = 600
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")


@dataclass(frozen=True, slots=True)
class FastLiveIdentity:
    release_sha: str
    source_sha256: str
    config_sha256: str
    profile_sha256: str
    native_runtime_sha256: str
    history_sha256: str
    model_sha256: str
    route: str
    direction: str
    account_generation_sha256: str
    data_generation_sha256: str
    risk_stage: str
    intent_sha256: str

    def __post_init__(self) -> None:
        if _COMMIT.fullmatch(self.release_sha) is None:
            raise ValueError("fast-live release must be an exact commit SHA")
        digests = (
            self.source_sha256,
            self.config_sha256,
            self.profile_sha256,
            self.native_runtime_sha256,
            self.history_sha256,
            self.model_sha256,
            self.account_generation_sha256,
            self.data_generation_sha256,
            self.intent_sha256,
        )
        if any(_SHA256.fullmatch(value) is None for value in digests):
            raise ValueError("fast-live identity contains an invalid SHA-256")
        if not self.route.strip() or not self.direction.strip() or not self.risk_stage.strip():
            raise ValueError("fast-live route, direction, and risk stage are required")


@dataclass(frozen=True, slots=True)
class FastLivePreflightInput:
    identity: FastLiveIdentity
    exact_merged_clean_source: bool
    money_movement_capability_absent: bool
    private_capabilities_ready: bool
    emergency_capability_ready: bool
    account_modes_permissions_ready: bool
    fees_funding_metadata_ready: bool
    stable_flat: bool
    zero_open_orders: bool
    journal_known_and_reconciled: bool
    clocks_and_market_data_ready: bool
    executable_depth_ready: bool
    history_model_ready: bool
    regime_clear: bool
    economics_positive: bool
    risk_margin_leverage_ready: bool
    owner_unlock_absent: bool
    telegram_challenge_absent: bool
    numerical_breakdown: dict[str, str]


@dataclass(frozen=True, slots=True)
class FastLivePreflightCheck:
    name: str
    passed: bool
    failure_reason: ReasonCode


@dataclass(frozen=True, slots=True)
class FastLivePreflightReport:
    schema_version: int
    status: str
    reason: ReasonCode
    created_at: datetime
    expires_at: datetime
    identity: FastLiveIdentity
    checks: tuple[FastLivePreflightCheck, ...]
    numerical_breakdown: dict[str, str]
    preflight_sha256: str
    consumed_at: datetime | None = None
    consumed_intent_sha256: str | None = None
    execution_authorized: bool = False

    def __post_init__(self) -> None:
        if self.schema_version != 1 or self.status not in {"PASS", "FAIL"}:
            raise ValueError("fast-live preflight schema or status is invalid")
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("fast-live preflight creation time must be aware")
        if self.expires_at != self.created_at + timedelta(seconds=FAST_LIVE_PREFLIGHT_TTL_SECONDS):
            raise ValueError("fast-live preflight TTL must be exactly 600 seconds")
        if self.execution_authorized:
            raise ValueError("fast-live preflight cannot authorize execution")
        if _SHA256.fullmatch(self.preflight_sha256) is None:
            raise ValueError("fast-live preflight hash is invalid")
        if self.consumed_at is not None:
            if self.consumed_at.tzinfo is None or self.consumed_at.utcoffset() is None:
                raise ValueError("fast-live preflight consumption time must be aware")
            if self.consumed_intent_sha256 is None:
                raise ValueError("consumed fast-live preflight requires an intent hash")
        if (
            self.consumed_intent_sha256 is not None
            and _SHA256.fullmatch(self.consumed_intent_sha256) is None
        ):
            raise ValueError("fast-live intent hash is invalid")
        if self.preflight_sha256 != "0" * 64 and _report_sha256(self) != self.preflight_sha256:
            raise ValueError("fast-live preflight hash mismatch")


_CHECKS: tuple[tuple[str, str, ReasonCode], ...] = (
    (
        "exact_merged_clean_source",
        "exact_merged_clean_source",
        ReasonCode.FAST_LIVE_SOURCE_IDENTITY_INVALID,
    ),
    (
        "money_movement_capability_absent",
        "money_movement_capability_absent",
        ReasonCode.CAPABILITY_UNKNOWN,
    ),
    (
        "private_capabilities_ready",
        "private_capabilities_ready",
        ReasonCode.PRIVATE_CAPABILITY_MISSING,
    ),
    (
        "emergency_capability_ready",
        "emergency_capability_ready",
        ReasonCode.EMERGENCY_VENUE_PREFLIGHT_FAILED,
    ),
    (
        "account_modes_permissions_ready",
        "account_modes_permissions_ready",
        ReasonCode.PREFLIGHT_FAILED,
    ),
    (
        "fees_funding_metadata_ready",
        "fees_funding_metadata_ready",
        ReasonCode.FEE_UNKNOWN,
    ),
    ("stable_flat", "stable_flat", ReasonCode.FAST_LIVE_ACCOUNT_NOT_FLAT),
    ("zero_open_orders", "zero_open_orders", ReasonCode.UNKNOWN_ORDER_BLOCK),
    (
        "journal_known_and_reconciled",
        "journal_known_and_reconciled",
        ReasonCode.RECONCILIATION_INCOMPLETE,
    ),
    (
        "clocks_and_market_data_ready",
        "clocks_and_market_data_ready",
        ReasonCode.MARKET_DATA_PREFLIGHT_FAILED,
    ),
    ("executable_depth_ready", "executable_depth_ready", ReasonCode.DEPTH_INSUFFICIENT),
    (
        "history_model_ready",
        "history_model_ready",
        ReasonCode.FAST_LIVE_HISTORY_MODEL_INVALID,
    ),
    ("regime_clear", "regime_clear", ReasonCode.CALIBRATION_REGIME_SHIFT),
    ("economics_positive", "economics_positive", ReasonCode.ECONOMIC_PREFLIGHT_FAILED),
    (
        "risk_margin_leverage_ready",
        "risk_margin_leverage_ready",
        ReasonCode.RISK_PREFLIGHT_FAILED,
    ),
    (
        "owner_unlock_absent",
        "owner_unlock_absent",
        ReasonCode.FAST_LIVE_OWNER_CONTROL_ACTIVE,
    ),
    (
        "telegram_challenge_absent",
        "telegram_challenge_absent",
        ReasonCode.FAST_LIVE_OWNER_CONTROL_ACTIVE,
    ),
)


def evaluate_fast_live_preflight(
    inputs: FastLivePreflightInput,
    *,
    now: datetime | None = None,
) -> FastLivePreflightReport:
    created_at = now or datetime.now(UTC)
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise ValueError("fast-live preflight time must be aware")
    checks = tuple(
        FastLivePreflightCheck(name, bool(getattr(inputs, attribute)), reason)
        for name, attribute, reason in _CHECKS
    )
    failure = next((check.failure_reason for check in checks if not check.passed), None)
    unsigned = FastLivePreflightReport(
        schema_version=1,
        status="FAIL" if failure is not None else "PASS",
        reason=failure or ReasonCode.FAST_LIVE_PREFLIGHT_PASSED,
        created_at=created_at,
        expires_at=created_at + timedelta(seconds=FAST_LIVE_PREFLIGHT_TTL_SECONDS),
        identity=inputs.identity,
        checks=checks,
        numerical_breakdown=dict(sorted(inputs.numerical_breakdown.items())),
        preflight_sha256="0" * 64,
    )
    return replace(unsigned, preflight_sha256=_report_sha256(unsigned))


def validate_fast_live_preflight(
    report: FastLivePreflightReport,
    current_identity: FastLiveIdentity,
    *,
    now: datetime | None = None,
) -> ReasonCode | None:
    report.__post_init__()
    checked_at = now or datetime.now(UTC)
    if report.status != "PASS":
        return ReasonCode.FAST_LIVE_PREFLIGHT_FAILED
    if report.consumed_at is not None:
        return ReasonCode.FAST_LIVE_PREFLIGHT_ALREADY_USED
    if checked_at < report.created_at or checked_at > report.expires_at:
        return ReasonCode.FAST_LIVE_PREFLIGHT_EXPIRED
    if report.identity != current_identity:
        return ReasonCode.FAST_LIVE_PREFLIGHT_IDENTITY_CHANGED
    return None


def save_fast_live_preflight(path: Path, report: FastLivePreflightReport) -> None:
    report.__post_init__()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            stream.write(json.dumps(asdict(report), default=str, indent=2, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def load_fast_live_preflight(path: Path) -> FastLivePreflightReport:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("fast-live preflight must be an object")
    raw_identity = payload.get("identity")
    raw_checks = payload.get("checks")
    raw_breakdown = payload.get("numerical_breakdown")
    if (
        not isinstance(raw_identity, dict)
        or not isinstance(raw_checks, list)
        or not isinstance(raw_breakdown, dict)
    ):
        raise ValueError("fast-live preflight payload is invalid")
    report = FastLivePreflightReport(
        schema_version=int(payload["schema_version"]),
        status=str(payload["status"]),
        reason=ReasonCode(str(payload["reason"])),
        created_at=datetime.fromisoformat(str(payload["created_at"])),
        expires_at=datetime.fromisoformat(str(payload["expires_at"])),
        identity=FastLiveIdentity(**{key: str(value) for key, value in raw_identity.items()}),
        checks=tuple(
            FastLivePreflightCheck(
                name=str(item["name"]),
                passed=bool(item["passed"]),
                failure_reason=ReasonCode(str(item["failure_reason"])),
            )
            for item in raw_checks
            if isinstance(item, dict)
        ),
        numerical_breakdown={str(key): str(value) for key, value in raw_breakdown.items()},
        preflight_sha256=str(payload["preflight_sha256"]),
        consumed_at=(
            datetime.fromisoformat(str(payload["consumed_at"]))
            if payload.get("consumed_at") is not None
            else None
        ),
        consumed_intent_sha256=(
            str(payload["consumed_intent_sha256"])
            if payload.get("consumed_intent_sha256") is not None
            else None
        ),
        execution_authorized=bool(payload.get("execution_authorized", False)),
    )
    if report.preflight_sha256 == "0" * 64:
        raise ValueError("fast-live preflight hash is not finalized")
    report.__post_init__()
    return report


def consume_fast_live_preflight(
    path: Path,
    current_identity: FastLiveIdentity,
    intent_sha256: str,
    *,
    now: datetime | None = None,
) -> FastLivePreflightReport:
    if _SHA256.fullmatch(intent_sha256) is None:
        raise ValueError("fast-live intent hash is invalid")
    lock_path = path.with_name(f".{path.name}.consume.lock")
    descriptor: int | None = None
    try:
        descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        report = load_fast_live_preflight(path)
        consumed_at = now or datetime.now(UTC)
        failure = validate_fast_live_preflight(report, current_identity, now=consumed_at)
        if failure is not None:
            raise ValueError(failure.value)
        consumed = replace(
            report,
            consumed_at=consumed_at,
            consumed_intent_sha256=intent_sha256,
        )
        save_fast_live_preflight(path, consumed)
        return consumed
    except FileExistsError as error:
        raise ValueError(ReasonCode.FAST_LIVE_PREFLIGHT_ALREADY_USED.value) from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
            lock_path.unlink(missing_ok=True)


def _report_sha256(report: FastLivePreflightReport) -> str:
    payload = asdict(report)
    payload["preflight_sha256"] = ""
    payload["consumed_at"] = None
    payload["consumed_intent_sha256"] = None
    return hashlib.sha256(
        json.dumps(payload, default=str, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
