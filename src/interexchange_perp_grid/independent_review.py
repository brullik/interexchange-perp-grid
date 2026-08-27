from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_REVIEWER = re.compile(r"^[a-z0-9][a-z0-9._:/-]{2,127}$")
_REVIEW_KIND = "independent-read-only-adversarial-v1"


@dataclass(frozen=True, slots=True)
class IndependentReviewerVerdict:
    reviewer_id: str
    verdict_sha256: str
    p0_open: int
    p1_open: int
    p2_open: int
    material_findings: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            _REVIEWER.fullmatch(self.reviewer_id) is None
            or _SHA256.fullmatch(self.verdict_sha256) is None
            or self.p0_open != 0
            or self.p1_open != 0
            or self.p2_open != 0
            or self.material_findings
        ):
            raise ValueError("independent reviewer verdict is not accepting")


@dataclass(frozen=True, slots=True)
class IndependentReviewReceipt:
    schema_version: int
    review_kind: str
    release_sha: str
    source_sha256: str
    config_sha256: str
    required_checks_sha256: str
    reviewed_at: datetime
    reviewer_verdicts: tuple[IndependentReviewerVerdict, ...]
    receipt_sha256: str

    def __post_init__(self) -> None:
        if (
            self.schema_version != 1
            or self.review_kind != _REVIEW_KIND
            or _COMMIT.fullmatch(self.release_sha) is None
            or any(
                _SHA256.fullmatch(value) is None
                for value in (
                    self.source_sha256,
                    self.config_sha256,
                    self.required_checks_sha256,
                    self.receipt_sha256,
                )
            )
            or self.reviewed_at.tzinfo is None
            or self.reviewed_at > datetime.now(UTC) + timedelta(minutes=5)
            or len(self.reviewer_verdicts) < 2
            or len({verdict.reviewer_id for verdict in self.reviewer_verdicts})
            != len(self.reviewer_verdicts)
        ):
            raise ValueError("independent review receipt is not accepting")
        if self.receipt_sha256 != _receipt_sha256(self):
            raise ValueError("independent review receipt hash mismatch")

    @property
    def reviewers(self) -> tuple[str, ...]:
        return tuple(verdict.reviewer_id for verdict in self.reviewer_verdicts)

    @property
    def p0_open(self) -> int:
        return sum(verdict.p0_open for verdict in self.reviewer_verdicts)

    @property
    def p1_open(self) -> int:
        return sum(verdict.p1_open for verdict in self.reviewer_verdicts)

    @property
    def p2_open(self) -> int:
        return sum(verdict.p2_open for verdict in self.reviewer_verdicts)


def load_independent_review_receipt(path: Path) -> IndependentReviewReceipt:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("independent review receipt must be an object")
    expected_keys = {
        "schema_version",
        "review_kind",
        "release_sha",
        "source_sha256",
        "config_sha256",
        "required_checks_sha256",
        "reviewed_at",
        "reviewer_verdicts",
        "receipt_sha256",
    }
    if set(raw) != expected_keys:
        raise ValueError("independent review receipt schema is invalid")
    raw_verdicts = raw["reviewer_verdicts"]
    if not isinstance(raw_verdicts, list):
        raise ValueError("independent reviewer verdicts must be a list")
    verdicts: list[IndependentReviewerVerdict] = []
    verdict_keys = {
        "reviewer_id",
        "verdict_sha256",
        "p0_open",
        "p1_open",
        "p2_open",
        "material_findings",
    }
    for item in raw_verdicts:
        if not isinstance(item, dict) or set(item) != verdict_keys:
            raise ValueError("independent reviewer verdict schema is invalid")
        findings = item["material_findings"]
        if not isinstance(findings, list) or not all(
            isinstance(finding, str) for finding in findings
        ):
            raise ValueError("independent reviewer findings are invalid")
        verdicts.append(
            IndependentReviewerVerdict(
                reviewer_id=_strict_str(item, "reviewer_id"),
                verdict_sha256=_strict_str(item, "verdict_sha256"),
                p0_open=_strict_int(item, "p0_open"),
                p1_open=_strict_int(item, "p1_open"),
                p2_open=_strict_int(item, "p2_open"),
                material_findings=tuple(findings),
            )
        )
    return IndependentReviewReceipt(
        schema_version=_strict_int(raw, "schema_version"),
        review_kind=_strict_str(raw, "review_kind"),
        release_sha=_strict_str(raw, "release_sha"),
        source_sha256=_strict_str(raw, "source_sha256"),
        config_sha256=_strict_str(raw, "config_sha256"),
        required_checks_sha256=_strict_str(raw, "required_checks_sha256"),
        reviewed_at=datetime.fromisoformat(_strict_str(raw, "reviewed_at")),
        reviewer_verdicts=tuple(verdicts),
        receipt_sha256=_strict_str(raw, "receipt_sha256"),
    )


def verify_independent_review_receipt(
    path: Path,
    *,
    release_sha: str,
    source_sha256: str,
    config_sha256: str,
    required_checks_sha256: str,
) -> IndependentReviewReceipt:
    receipt = load_independent_review_receipt(path)
    if (
        receipt.release_sha != release_sha
        or receipt.source_sha256 != source_sha256
        or receipt.config_sha256 != config_sha256
        or receipt.required_checks_sha256 != required_checks_sha256
    ):
        raise ValueError("independent review receipt identity is stale")
    return receipt


def _strict_int(payload: dict[str, Any], key: str) -> int:
    value = payload[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"independent review {key} must be an integer")
    return int(value)


def _strict_str(payload: dict[str, Any], key: str) -> str:
    value = payload[key]
    if not isinstance(value, str):
        raise ValueError(f"independent review {key} must be a string")
    return value


def _receipt_sha256(receipt: IndependentReviewReceipt) -> str:
    payload = asdict(receipt)
    payload.pop("receipt_sha256", None)
    payload["reviewed_at"] = receipt.reviewed_at.isoformat()
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
