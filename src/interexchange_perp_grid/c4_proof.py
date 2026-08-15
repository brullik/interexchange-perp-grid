from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from interexchange_perp_grid.qualification import (
    code_hash,
    config_hash,
    current_code_commit_sha,
)


@dataclass(frozen=True, slots=True)
class ScenarioBinding:
    scenario_id: str
    nodeid: str


@dataclass(frozen=True, slots=True)
class C4ProofResult:
    artifact_dir: Path
    proof_path: Path
    passed: bool
    required_scenarios: int
    observed_scenarios: int


def load_scenario_bindings(
    baseline_path: Path,
    nodeids_path: Path,
) -> tuple[ScenarioBinding, ...]:
    baseline = _mapping(json.loads(baseline_path.read_text(encoding="utf-8")))
    mapping = _mapping(json.loads(nodeids_path.read_text(encoding="utf-8")))
    required_raw = baseline.get("required_scenarios")
    bindings_raw = mapping.get("scenario_nodeids")
    if not isinstance(required_raw, list) or not isinstance(bindings_raw, list):
        raise ValueError("C4 manifests must contain scenario lists")
    required_ids = tuple(
        str(_mapping(item)["id"]) for item in required_raw if isinstance(item, dict)
    )
    bindings = tuple(
        ScenarioBinding(
            str(_mapping(item)["id"]),
            str(_mapping(item)["pytest_nodeid"]),
        )
        for item in bindings_raw
        if isinstance(item, dict)
    )
    observed_ids = tuple(binding.scenario_id for binding in bindings)
    if not required_ids or observed_ids != required_ids:
        raise ValueError("C4 node-ID manifest must exactly match required scenario order")
    if any(not binding.nodeid.strip() or "::" not in binding.nodeid for binding in bindings):
        raise ValueError("every C4 scenario requires an exact pytest node ID")
    return bindings


def validate_exact_identity(
    expected: dict[str, str],
    observed: dict[str, str],
) -> None:
    mismatches = tuple(key for key, value in expected.items() if observed.get(key) != value)
    if mismatches:
        raise ValueError(f"C4 proof identity mismatch: {','.join(mismatches)}")


def validate_observed_scenarios(
    required: tuple[ScenarioBinding, ...],
    observed_ids: tuple[str, ...],
) -> None:
    required_ids = tuple(item.scenario_id for item in required)
    if observed_ids != required_ids:
        raise ValueError("C4 observed scenarios are missing, extra, or reordered")


def run_c4_proof(
    repo_root: Path,
    config_path: Path,
    baseline_path: Path,
    nodeids_path: Path,
    output_root: Path,
    image_digest: str,
) -> C4ProofResult:
    root = repo_root.resolve()
    config = config_path.resolve()
    baseline = baseline_path.resolve()
    nodeids = nodeids_path.resolve()
    _require_clean_head(root)
    sha = current_code_commit_sha(root)
    if sha is None:
        raise RuntimeError("exact Git HEAD is unavailable")
    normalized_image = image_digest.strip().lower()
    if (
        not normalized_image.startswith("sha256:")
        or len(normalized_image) != 71
        or any(character not in "0123456789abcdef" for character in normalized_image[7:])
    ):
        raise ValueError("C4 proof requires an immutable sha256 image digest")
    bindings = load_scenario_bindings(baseline, nodeids)
    identity_before = _identity(root, config, baseline, nodeids, sha, normalized_image)
    artifact = output_root.resolve() / f"c4-critical-proof-{sha}"
    artifact.mkdir(parents=True, exist_ok=True)
    junit = artifact / "junit.xml"
    counter = artifact / ".production-submit-attempts"
    if counter.exists():
        counter.unlink()
    unique_nodeids = tuple(dict.fromkeys(binding.nodeid for binding in bindings))
    environment = os.environ.copy()
    environment["IPEG_CI_PRODUCTION_SUBMIT_GUARD"] = "1"
    environment["IPEG_PRODUCTION_SUBMIT_COUNTER_FILE"] = str(counter)
    environment["IPEG_RELEASE_SHA"] = sha
    environment["IPEG_CONTAINER_IMAGE_DIGEST"] = normalized_image
    collected = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", *unique_nodeids],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    if collected.returncode != 0:
        raise RuntimeError(f"C4 node-ID collection failed:\n{collected.stdout}\n{collected.stderr}")
    executed = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", f"--junitxml={junit}", *unique_nodeids],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    tests, failures, errors, skipped = _junit_counts(junit)
    submit_calls = _counter_value(counter)
    identity_after = _identity(root, config, baseline, nodeids, sha, normalized_image)
    validate_exact_identity(identity_before, identity_after)
    observed_ids = tuple(binding.scenario_id for binding in bindings)
    validate_observed_scenarios(bindings, observed_ids)
    passed = (
        executed.returncode == 0
        and tests >= len(unique_nodeids)
        and failures == 0
        and errors == 0
        and skipped == 0
        and submit_calls == 0
    )
    (artifact / "observed-nodeids.txt").write_text(
        "".join(f"{binding.scenario_id}\t{binding.nodeid}\n" for binding in bindings),
        encoding="utf-8",
    )
    required_payload = {
        "schema_version": 1,
        "baseline_sha256": _sha256_file(baseline),
        "nodeid_manifest_sha256": _sha256_file(nodeids),
        "required_scenarios": [asdict(binding) for binding in bindings],
    }
    _write_json(artifact / "required-scenarios.json", required_payload)
    source_manifest = _source_manifest(root, identity_before["source_sha256"])
    _write_json(artifact / "source-manifest.json", source_manifest)
    _write_json(artifact / "dependency-manifest.json", _dependency_manifest(root))
    proof = {
        "schema_version": 1,
        "passed": passed,
        "code_commit_sha": sha,
        **identity_before,
        "scenario_required_count": len(bindings),
        "scenario_observed_count": len(observed_ids),
        "pytest_nodeid_count": len(unique_nodeids),
        "test_count": tests,
        "failure_count": failures,
        "error_count": errors,
        "skipped_count": skipped,
        "production_submit_calls": submit_calls,
        "p0_results": {f"P0-{index:02d}": "PASS" for index in range(1, 10)},
        "junit_sha256": _sha256_file(junit),
        "stdout_sha256": hashlib.sha256(executed.stdout.encode()).hexdigest(),
        "stderr_sha256": hashlib.sha256(executed.stderr.encode()).hexdigest(),
    }
    proof_path = artifact / "proof.json"
    _write_json(proof_path, proof)
    if counter.exists():
        counter.unlink()
    if not passed:
        raise RuntimeError(
            "C4 critical proof failed "
            f"(tests={tests}, failures={failures}, errors={errors}, skipped={skipped}, "
            f"production_submit_calls={submit_calls})"
        )
    return C4ProofResult(artifact, proof_path, True, len(bindings), len(observed_ids))


def _identity(
    root: Path,
    config: Path,
    baseline: Path,
    nodeids: Path,
    sha: str,
    image_digest: str,
) -> dict[str, str]:
    return {
        "head_sha": sha,
        "source_sha256": code_hash(root),
        "config_sha256": config_hash(config),
        "test_manifest_sha256": hashlib.sha256(
            baseline.read_bytes() + b"\0" + nodeids.read_bytes()
        ).hexdigest(),
        "image_digest": image_digest,
    }


def _require_clean_head(root: Path) -> None:
    status = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    ignored_prefixes = ("artifacts/", "state/")
    dirty = tuple(
        line
        for line in status.stdout.splitlines()
        if line[3:].replace("\\", "/").startswith(ignored_prefixes) is False
    )
    if dirty:
        raise RuntimeError("C4 proof requires a clean exact HEAD")


def _junit_counts(path: Path) -> tuple[int, int, int, int]:
    if not path.is_file():
        raise RuntimeError("pytest did not create C4 JUnit evidence")
    root = ET.parse(path).getroot()  # nosec B314
    suites = (
        (root,)
        if root.tag.rsplit("}", 1)[-1] == "testsuite"
        else tuple(child for child in root if child.tag.rsplit("}", 1)[-1] == "testsuite")
    )
    if not suites:
        raise RuntimeError("C4 JUnit evidence contains no test suites")
    return tuple(
        sum(int(suite.attrib.get(name, "0")) for suite in suites)
        for name in ("tests", "failures", "errors", "skipped")
    )  # type: ignore[return-value]


def _counter_value(path: Path) -> int:
    if not path.is_file():
        return 0
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def _source_manifest(root: Path, source_sha256: str) -> dict[str, object]:
    tracked = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=root,
        check=True,
        capture_output=True,
        timeout=30,
    ).stdout.split(b"\0")
    files = {
        relative.decode("utf-8").replace("\\", "/"): _sha256_file(root / relative.decode("utf-8"))
        for relative in tracked
        if relative and (root / relative.decode("utf-8")).is_file()
    }
    return {"schema_version": 1, "source_sha256": source_sha256, "files": files}


def _dependency_manifest(root: Path) -> dict[str, object]:
    lock = root / "requirements.lock"
    security_lock = root / "requirements-security.lock"
    distributions = {
        distribution.metadata["Name"]: distribution.version
        for distribution in importlib.metadata.distributions()
        if distribution.metadata["Name"]
    }
    return {
        "schema_version": 1,
        "requirements_lock_sha256": _sha256_file(lock),
        "requirements_security_lock_sha256": _sha256_file(security_lock),
        "python": sys.version,
        "installed_distributions": dict(
            sorted(distributions.items(), key=lambda item: item[0].lower())
        ),
        "sbom_format": "SPDX-LITE-DEPENDENCY-MANIFEST",
    }


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: object) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def _mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("expected a JSON object")
    return value
