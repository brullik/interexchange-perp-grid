from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from interexchange_perp_grid.independent_review import (
    load_independent_review_receipt,
    verify_independent_review_receipt,
)


def _write_receipt(path: Path, **changes: object) -> None:
    payload: dict[str, object] = {
        "schema_version": 1,
        "review_kind": "independent-read-only-adversarial-v1",
        "release_sha": "a" * 40,
        "source_sha256": "b" * 64,
        "config_sha256": "c" * 64,
        "required_checks_sha256": "d" * 64,
        "reviewed_at": datetime.now(UTC).isoformat(),
        "reviewer_verdicts": [
            {
                "reviewer_id": "codex-subagent:laptop-gap",
                "verdict_sha256": "1" * 64,
                "p0_open": 0,
                "p1_open": 0,
                "p2_open": 0,
                "material_findings": [],
            },
            {
                "reviewer_id": "codex-subagent:strategy-gap",
                "verdict_sha256": "2" * 64,
                "p0_open": 0,
                "p1_open": 0,
                "p2_open": 0,
                "material_findings": [],
            },
        ],
    }
    payload.update(changes)
    payload["receipt_sha256"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_load_independent_review_receipt_requires_two_exact_zero_reviewers(
    tmp_path: Path,
) -> None:
    path = tmp_path / "review.json"
    _write_receipt(path)

    receipt = load_independent_review_receipt(path)

    assert receipt.p0_open == receipt.p1_open == receipt.p2_open == 0
    assert receipt.reviewers == (
        "codex-subagent:laptop-gap",
        "codex-subagent:strategy-gap",
    )


@pytest.mark.parametrize(
    "change",
    [
        {
            "reviewer_verdicts": [
                {
                    "reviewer_id": "codex-subagent:one",
                    "verdict_sha256": "1" * 64,
                    "p0_open": 0,
                    "p1_open": 0,
                    "p2_open": 0,
                    "material_findings": [],
                }
            ]
        },
        {
            "reviewer_verdicts": [
                {
                    "reviewer_id": "codex-subagent:one",
                    "verdict_sha256": "1" * 64,
                    "p0_open": 1,
                    "p1_open": 0,
                    "p2_open": 0,
                    "material_findings": [],
                },
                {
                    "reviewer_id": "codex-subagent:two",
                    "verdict_sha256": "2" * 64,
                    "p0_open": 0,
                    "p1_open": 0,
                    "p2_open": 0,
                    "material_findings": [],
                },
            ]
        },
        {
            "reviewer_verdicts": [
                {
                    "reviewer_id": "codex-subagent:one",
                    "verdict_sha256": "1" * 64,
                    "p0_open": 0,
                    "p1_open": 0,
                    "p2_open": 0,
                    "material_findings": ["unresolved finding"],
                },
                {
                    "reviewer_id": "codex-subagent:two",
                    "verdict_sha256": "2" * 64,
                    "p0_open": 0,
                    "p1_open": 0,
                    "p2_open": 0,
                    "material_findings": [],
                },
            ]
        },
    ],
)
def test_independent_review_receipt_rejects_nonaccepting_content(
    tmp_path: Path, change: dict[str, object]
) -> None:
    path = tmp_path / "review.json"
    _write_receipt(path, **change)

    with pytest.raises(ValueError, match="not accepting"):
        load_independent_review_receipt(path)


def test_independent_review_receipt_rejects_tampering(tmp_path: Path) -> None:
    path = tmp_path / "review.json"
    _write_receipt(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["source_sha256"] = "e" * 64
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="hash mismatch"):
        load_independent_review_receipt(path)


@pytest.mark.parametrize(
    ("field", "wrong"),
    [
        ("release_sha", "f" * 40),
        ("source_sha256", "f" * 64),
        ("config_sha256", "f" * 64),
        ("required_checks_sha256", "f" * 64),
    ],
)
def test_acceptance_review_boundary_rejects_every_stale_identity(
    tmp_path: Path, field: str, wrong: str
) -> None:
    path = tmp_path / "review.json"
    _write_receipt(path)
    expected = {
        "release_sha": "a" * 40,
        "source_sha256": "b" * 64,
        "config_sha256": "c" * 64,
        "required_checks_sha256": "d" * 64,
    }
    expected[field] = wrong

    with pytest.raises(ValueError, match="identity is stale"):
        verify_independent_review_receipt(path, **expected)


def test_acceptance_review_boundary_rejects_missing_receipt(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        verify_independent_review_receipt(
            tmp_path / "missing.json",
            release_sha="a" * 40,
            source_sha256="b" * 64,
            config_sha256="c" * 64,
            required_checks_sha256="d" * 64,
        )
