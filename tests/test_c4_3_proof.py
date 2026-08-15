from __future__ import annotations

import json
from pathlib import Path

import pytest

from interexchange_perp_grid.c4_3_proof import (
    NEGATIVE_PUBLIC_SCENARIO_IDS,
    REQUIRED_SCENARIO_IDS,
    _junit_public_outcomes,
    _validate_image_digest,
    load_stable_flat_scenarios,
)

_MANIFEST = Path("config/c4-3-required-scenarios.json")


def test_c4_3_manifest_is_exact_ordered_and_has_unique_nodeids() -> None:
    scenarios = load_stable_flat_scenarios(_MANIFEST)

    assert tuple(item.scenario_id for item in scenarios) == REQUIRED_SCENARIO_IDS
    assert len({item.pytest_nodeid for item in scenarios}) == 8


def test_c4_3_manifest_rejects_missing_or_reordered_scenario(tmp_path: Path) -> None:
    payload = json.loads(_MANIFEST.read_text(encoding="utf-8"))
    payload["required_scenarios"] = payload["required_scenarios"][1:]
    changed = tmp_path / "changed.json"
    changed.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="exact ordered"):
        load_stable_flat_scenarios(changed)


def test_c4_3_proof_requires_immutable_sha256_image_digest() -> None:
    valid = f"sha256:{'a' * 64}"
    assert _validate_image_digest(valid.upper()) == valid

    with pytest.raises(ValueError, match="immutable sha256"):
        _validate_image_digest("latest")


def test_c4_3_proof_counts_public_negative_outcomes_from_junit(tmp_path: Path) -> None:
    cases = "".join(
        (
            f'<testcase name="{scenario_id}"><properties>'
            f'<property name="c43_scenario_id" value="{scenario_id}" />'
            '<property name="c43_public_success" value="false" />'
            '<property name="c43_barrier_verified" value="false" />'
            "</properties></testcase>"
        )
        for scenario_id in sorted(NEGATIVE_PUBLIC_SCENARIO_IDS)
    )
    junit = tmp_path / "junit.xml"
    junit.write_text(f'<testsuite tests="6">{cases}</testsuite>', encoding="utf-8")

    outcomes = _junit_public_outcomes(junit)

    assert outcomes == {scenario_id: False for scenario_id in NEGATIVE_PUBLIC_SCENARIO_IDS}


def test_c4_3_proof_rejects_missing_public_outcome_evidence(tmp_path: Path) -> None:
    junit = tmp_path / "junit.xml"
    junit.write_text('<testsuite tests="0" />', encoding="utf-8")

    with pytest.raises(RuntimeError, match=r"missing C4\.3 public outcome evidence"):
        _junit_public_outcomes(junit)
