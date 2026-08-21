from __future__ import annotations

import json
from pathlib import Path

import pytest

from interexchange_perp_grid.ops_evidence import (
    EXPECTED_PRIVATE_STATES,
    OperationsProof,
    build_operations_proof,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _junit(path: Path, *, complete: bool = True, failures: int = 0) -> None:
    mapping = json.loads(
        (REPO_ROOT / "config/ops-scenario-nodeids.json").read_text(encoding="utf-8")
    )
    names = sorted(
        {
            value.partition("::")[2].split("[", 1)[0]
            for values in mapping.values()
            for value in values
            if value.startswith("tests/")
        }
    )
    if not complete:
        names = names[:3]
    cases = "".join(f'<testcase classname="tests" name="{name}" />' for name in names)
    path.write_text(
        f'<testsuite tests="{len(names)}" failures="{failures}" errors="0" '
        f'skipped="0">{cases}</testsuite>',
        encoding="utf-8",
    )


def _runtime_evidence(root: Path) -> None:
    image = "localhost:5000/interexchange-perp-grid@sha256:" + "b" * 64
    _write_json(root / "platform.json", {"runner_image": "ubuntu-24.04"})
    _write_json(root / "clean-health.json", {"status": "PASS", "starts": 1})
    _write_json(root / "restart-health.json", {"status": "PASS", "starts": 3})
    _write_json(
        root / "deployment-identity.json",
        {"status": "PASS", "release_sha": "a" * 40, "image_digest": "sha256:" + "b" * 64},
    )
    _write_json(
        root / "rollback.json",
        {
            "status": "PASS",
            "failed_upgrade_exit_nonzero": True,
            "state_restored": True,
            "release_sha": "a" * 40,
            "image_ref": image,
        },
    )
    for state in EXPECTED_PRIVATE_STATES:
        _write_json(
            root / f"private-recovery-{state.lower()}.json",
            {
                "status": "PASS",
                "restarted_transition_state": state,
                "flat_barrier_verified": True,
                "production_exchange_transports_opened": 0,
            },
        )


def _build(tmp_path: Path) -> OperationsProof:
    return build_operations_proof(
        REPO_ROOT,
        REPO_ROOT / "config/defaults.yaml",
        REPO_ROOT / "config/RUNTIME_POLICY.yaml",
        REPO_ROOT / "config/FINAL_ACCEPTANCE_MANIFEST.json",
        REPO_ROOT / "config/ops-scenario-nodeids.json",
        tmp_path / "ops.junit.xml",
        tmp_path / "evidence",
        tmp_path / "ops.json",
    )


def test_operations_proof_binds_full_manifest_and_raw_runtime_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _junit(tmp_path / "ops.junit.xml")
    _runtime_evidence(tmp_path / "evidence")
    monkeypatch.setenv("IPEG_RELEASE_SHA", "a" * 40)

    proof = _build(tmp_path)

    assert proof.passed is True
    assert len(proof.criteria) == 42
    assert proof.production_exchange_transports_opened == 0


def test_operations_proof_rejects_incomplete_junit_even_when_summary_is_green(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _junit(tmp_path / "ops.junit.xml", complete=False)
    _runtime_evidence(tmp_path / "evidence")
    monkeypatch.setenv("IPEG_RELEASE_SHA", "a" * 40)

    with pytest.raises(ValueError, match="unproved test nodeid"):
        _build(tmp_path)


def test_operations_proof_fails_closed_on_raw_rollback_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _junit(tmp_path / "ops.junit.xml")
    _runtime_evidence(tmp_path / "evidence")
    rollback = tmp_path / "evidence/rollback.json"
    payload = json.loads(rollback.read_text(encoding="utf-8"))
    payload["state_restored"] = False
    _write_json(rollback, payload)
    monkeypatch.setenv("IPEG_RELEASE_SHA", "a" * 40)

    with pytest.raises(RuntimeError, match="did not pass"):
        _build(tmp_path)
