from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from interexchange_perp_grid.adapters.private import (
    CcxtPrivateAdapter,
    production_submit_guard_active,
)
from interexchange_perp_grid.c4_proof import (
    _junit_counts,
    load_scenario_bindings,
    validate_exact_identity,
    validate_observed_scenarios,
)

_BASELINE = Path("config/c4-critical-test-manifest.json")
_NODEIDS = Path("config/c4-scenario-nodeids.json")


def test_required_scenario_manifest_rejects_missing_or_reordered_scenario(
    tmp_path: Path,
) -> None:
    bindings = load_scenario_bindings(_BASELINE, _NODEIDS)
    assert len(bindings) == 30
    with pytest.raises(ValueError, match="missing, extra, or reordered"):
        validate_observed_scenarios(bindings, tuple(item.scenario_id for item in bindings[:-1]))

    payload = json.loads(_NODEIDS.read_text(encoding="utf-8"))
    payload["scenario_nodeids"] = payload["scenario_nodeids"][1:]
    missing = tmp_path / "missing.json"
    missing.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="exactly match"):
        load_scenario_bindings(_BASELINE, missing)


def test_exact_head_source_config_manifest_and_image_identity_mismatch_fails() -> None:
    expected = {
        "head_sha": "a" * 40,
        "source_sha256": "b" * 64,
        "config_sha256": "c" * 64,
        "test_manifest_sha256": "d" * 64,
        "image_digest": f"sha256:{'e' * 64}",
    }
    validate_exact_identity(expected, dict(expected))
    for field in expected:
        changed = dict(expected)
        changed[field] = "mismatch"
        with pytest.raises(ValueError, match=field):
            validate_exact_identity(expected, changed)


def test_ci_guard_makes_production_submit_transport_unreachable_without_invocation() -> None:
    assert production_submit_guard_active({"IPEG_CI_PRODUCTION_SUBMIT_GUARD": "1"})
    source = inspect.getsource(CcxtPrivateAdapter.submit_order)
    assert source.index("_enforce_production_submit_guard") < source.index("create_order")


def test_junit_counts_supports_pytest_testsuites_root(tmp_path: Path) -> None:
    junit = tmp_path / "junit.xml"
    junit.write_text(
        '<testsuites><testsuite tests="33" failures="0" errors="0" skipped="0" /></testsuites>',
        encoding="utf-8",
    )

    assert _junit_counts(junit) == (33, 0, 0, 0)
