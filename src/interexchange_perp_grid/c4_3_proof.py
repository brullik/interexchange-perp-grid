from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from interexchange_perp_grid.c4_proof import (
    _dependency_manifest,
    _junit_counts,
    _require_clean_head,
    _sha256_file,
    _source_manifest,
    _write_json,
)
from interexchange_perp_grid.qualification import (
    code_hash,
    config_hash,
    current_code_commit_sha,
)

REQUIRED_SCENARIO_IDS = tuple(f"SF-{index:03d}" for index in range(1, 9))
NEGATIVE_PUBLIC_SCENARIO_IDS = frozenset(
    {"SF-001", "SF-002", "SF-004", "SF-006", "SF-007", "SF-008"}
)


@dataclass(frozen=True, slots=True)
class StableFlatScenario:
    scenario_id: str
    pytest_nodeid: str
    expected: str


@dataclass(frozen=True, slots=True)
class C43ProofResult:
    artifact_dir: Path
    proof_path: Path
    passed: bool
    required_scenarios: int
    observed_scenarios: int


def load_stable_flat_scenarios(path: Path) -> tuple[StableFlatScenario, ...]:
    payload = _mapping(json.loads(path.read_text(encoding="utf-8")))
    raw = payload.get("required_scenarios")
    if not isinstance(raw, list):
        raise ValueError("C4.3 manifest must contain required_scenarios")
    scenarios = tuple(
        StableFlatScenario(
            scenario_id=str(item["id"]),
            pytest_nodeid=str(item["pytest_nodeid"]),
            expected=str(item["expected"]),
        )
        for value in raw
        if isinstance(value, dict)
        for item in (_mapping(value),)
    )
    if tuple(item.scenario_id for item in scenarios) != REQUIRED_SCENARIO_IDS:
        raise ValueError("C4.3 manifest must contain exact ordered SF-001 through SF-008")
    if len({item.pytest_nodeid for item in scenarios}) != len(scenarios):
        raise ValueError("C4.3 scenarios require unique exact pytest node IDs")
    if any("::test_sf_" not in item.pytest_nodeid for item in scenarios):
        raise ValueError("C4.3 scenario node ID is not exact")
    return scenarios


def run_c4_3_proof(
    repo_root: Path,
    config_path: Path,
    runtime_policy_path: Path,
    scenario_manifest_path: Path,
    output_root: Path,
    image_digest: str,
) -> C43ProofResult:
    root = repo_root.resolve()
    config = config_path.resolve()
    runtime_policy = runtime_policy_path.resolve()
    manifest = scenario_manifest_path.resolve()
    _require_clean_head(root)
    sha = current_code_commit_sha(root)
    if sha is None:
        raise RuntimeError("exact Git HEAD is unavailable")
    normalized_image = _validate_image_digest(image_digest)
    scenarios = load_stable_flat_scenarios(manifest)
    identity_before = _identity(
        root,
        config,
        runtime_policy,
        manifest,
        sha,
        normalized_image,
    )
    artifact = output_root.resolve() / f"c4-3-proof-{sha}"
    artifact.mkdir(parents=True, exist_ok=True)
    junit = artifact / "junit.xml"
    submit_counter = artifact / ".production-submit-attempts"
    if submit_counter.exists():
        submit_counter.unlink()

    nodeids = tuple(item.pytest_nodeid for item in scenarios)
    environment = os.environ.copy()
    environment["IPEG_CI_PRODUCTION_SUBMIT_GUARD"] = "1"
    environment["IPEG_PRODUCTION_SUBMIT_COUNTER_FILE"] = str(submit_counter)
    environment["IPEG_RELEASE_SHA"] = sha
    environment["IPEG_CONTAINER_IMAGE_DIGEST"] = normalized_image
    collected = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", *nodeids],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    if collected.returncode != 0:
        raise RuntimeError(
            f"C4.3 node-ID collection failed:\n{collected.stdout}\n{collected.stderr}"
        )
    executed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-o",
            "junit_family=xunit1",
            f"--junitxml={junit}",
            *nodeids,
        ],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    tests, failures, errors, skipped = _junit_counts(junit)
    production_submit_calls = _counter_value(submit_counter)
    observed_ids = tuple(item.scenario_id for item in scenarios)
    public_outcomes = _junit_public_outcomes(junit)
    false_successes = sum(
        1 for scenario_id in NEGATIVE_PUBLIC_SCENARIO_IDS if public_outcomes[scenario_id]
    )
    identity_after = _identity(
        root,
        config,
        runtime_policy,
        manifest,
        sha,
        normalized_image,
    )
    if identity_before != identity_after:
        raise RuntimeError("C4.3 exact head/source/config/image identity changed during proof")
    passed = (
        observed_ids == REQUIRED_SCENARIO_IDS
        and executed.returncode == 0
        and tests == len(scenarios)
        and failures == errors == skipped == 0
        and false_successes == 0
        and production_submit_calls == 0
    )

    (artifact / "observed-nodeids.txt").write_text(
        "".join(f"{item.scenario_id}\t{item.pytest_nodeid}\n" for item in scenarios),
        encoding="utf-8",
    )
    _write_json(
        artifact / "required-scenarios.json",
        {
            "schema_version": 1,
            "manifest_sha256": _sha256_file(manifest),
            "required_scenarios": [asdict(item) for item in scenarios],
        },
    )
    _write_json(
        artifact / "source-manifest.json",
        _source_manifest(root, identity_before["source_sha256"]),
    )
    _write_json(artifact / "dependency-manifest.json", _dependency_manifest(root))
    proof = {
        "schema_version": 1,
        "passed": passed,
        "code_commit_sha": sha,
        **identity_before,
        "scenario_required_count": len(scenarios),
        "scenario_observed_count": len(observed_ids),
        "test_count": tests,
        "failure_count": failures,
        "error_count": errors,
        "skipped_count": skipped,
        "assertions": {
            "false_success_when_barrier_unverified": false_successes,
            "production_submit_calls": production_submit_calls,
        },
        "public_success_outcomes": {
            scenario_id: public_outcomes[scenario_id]
            for scenario_id in sorted(NEGATIVE_PUBLIC_SCENARIO_IDS)
        },
        "false_success_assertion_nodeids": [
            item.pytest_nodeid
            for item in scenarios
            if item.scenario_id in {"SF-001", "SF-002", "SF-004", "SF-006", "SF-007", "SF-008"}
        ],
        "junit_sha256": _sha256_file(junit),
        "stdout_sha256": hashlib.sha256(executed.stdout.encode()).hexdigest(),
        "stderr_sha256": hashlib.sha256(executed.stderr.encode()).hexdigest(),
    }
    proof_path = artifact / "proof.json"
    _write_json(proof_path, proof)
    if submit_counter.exists():
        submit_counter.unlink()
    if not passed:
        raise RuntimeError(
            "C4.3 proof failed "
            f"(tests={tests}, failures={failures}, errors={errors}, skipped={skipped}, "
            f"false_successes={false_successes}, production_submit_calls={production_submit_calls})"
        )
    return C43ProofResult(artifact, proof_path, True, len(scenarios), len(observed_ids))


def _junit_public_outcomes(path: Path) -> dict[str, bool]:
    outcomes: dict[str, bool] = {}
    # This path is the local JUnit file emitted by the exact pytest subprocess above.
    root = ET.parse(path).getroot()  # nosec B314
    for case in root.iter("testcase"):
        properties = {
            str(item.attrib.get("name")): str(item.attrib.get("value"))
            for item in case.findall("./properties/property")
        }
        scenario_id = properties.get("c43_scenario_id")
        if scenario_id is None:
            continue
        if scenario_id not in NEGATIVE_PUBLIC_SCENARIO_IDS:
            raise RuntimeError(f"unexpected C4.3 public outcome scenario {scenario_id}")
        if scenario_id in outcomes:
            raise RuntimeError(f"duplicate C4.3 public outcome scenario {scenario_id}")
        raw_success = properties.get("c43_public_success")
        if raw_success not in {"true", "false"}:
            raise RuntimeError(f"invalid C4.3 public success outcome for {scenario_id}")
        if properties.get("c43_barrier_verified") != "false":
            raise RuntimeError(f"C4.3 negative outcome lacks an unverified barrier: {scenario_id}")
        outcomes[scenario_id] = raw_success == "true"
    missing = NEGATIVE_PUBLIC_SCENARIO_IDS - outcomes.keys()
    if missing:
        raise RuntimeError(f"missing C4.3 public outcome evidence: {sorted(missing)}")
    return outcomes


def _identity(
    root: Path,
    config: Path,
    runtime_policy: Path,
    manifest: Path,
    sha: str,
    image_digest: str,
) -> dict[str, str]:
    return {
        "head_sha": sha,
        "source_sha256": code_hash(root),
        "config_sha256": config_hash(config),
        "runtime_policy_sha256": _sha256_file(runtime_policy),
        "scenario_manifest_sha256": _sha256_file(manifest),
        "image_digest": image_digest,
    }


def _validate_image_digest(value: str) -> str:
    normalized = value.strip().lower()
    if (
        not normalized.startswith("sha256:")
        or len(normalized) != 71
        or any(character not in "0123456789abcdef" for character in normalized[7:])
    ):
        raise ValueError("C4.3 proof requires an immutable sha256 image digest")
    return normalized


def _counter_value(path: Path) -> int:
    if not path.is_file():
        return 0
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def _mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("expected a JSON object")
    return value
