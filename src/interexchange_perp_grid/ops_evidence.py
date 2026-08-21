from __future__ import annotations

import hashlib
import json
import re
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from interexchange_perp_grid.qualification import code_hash, config_hash, current_code_commit_sha

EXPECTED_PRIVATE_STATES = {
    "PREPARED",
    "SUBMITTING",
    "ACKNOWLEDGED",
    "PARTIAL",
    "FILLED",
    "REJECTED",
    "UNKNOWN",
    "RECOVERING",
    "HEDGED",
    "CLOSING",
    "QUARANTINED",
}


@dataclass(frozen=True, slots=True)
class OperationsProof:
    schema_version: int
    generated_at: datetime
    code_commit_sha: str
    source_sha256: str
    config_sha256: str
    runtime_policy_sha256: str
    acceptance_manifest_sha256: str
    junit_sha256: str
    junit_tests: int
    runner_image: str
    deployed_image_ref: str
    clean_start_count: int
    host_restart_count: int
    private_transition_states: tuple[str, ...]
    production_exchange_transports_opened: int
    criteria: dict[str, tuple[str, ...]]
    passed: bool


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path.name} must contain one JSON object")
    return payload


def _junit(path: Path) -> tuple[int, int, int, int, set[str]]:
    root = ET.parse(path).getroot()  # nosec B314
    if root.tag not in {"testsuite", "testsuites"}:
        raise ValueError("operations proof requires a JUnit testsuite")
    suites = (root,) if root.tag == "testsuite" else tuple(root.findall("testsuite"))
    if not suites:
        raise ValueError("operations proof requires a JUnit testsuite")
    counts = tuple(
        sum(int(suite.attrib.get(field, "0")) for suite in suites)
        for field in ("tests", "failures", "errors", "skipped")
    )
    names = {
        str(case.attrib.get("name", "")).split("[", 1)[0]
        for suite in suites
        for case in suite.iter("testcase")
    }
    return counts[0], counts[1], counts[2], counts[3], names


def _load_criteria(
    repo_root: Path,
    mapping_path: Path,
    manifest_path: Path,
    junit_names: set[str],
) -> dict[str, tuple[str, ...]]:
    mapping = _read_object(mapping_path)
    manifest = _read_object(manifest_path)
    required = {
        str(item["id"])
        for item in manifest.get("criteria", ())
        if isinstance(item, dict) and item.get("id")
    }
    if len(required) != int(manifest.get("criteria_count", -1)) or set(mapping) != required:
        raise ValueError("operations evidence must cover the exact final acceptance manifest")
    criteria: dict[str, tuple[str, ...]] = {}
    for criterion in sorted(required):
        values = mapping[criterion]
        if (
            not isinstance(values, list)
            or not values
            or any(not isinstance(value, str) or not value.strip() for value in values)
        ):
            raise ValueError(f"{criterion} evidence mapping is invalid")
        for value in values:
            if value.startswith("tests/"):
                relative, separator, node_name = value.partition("::")
                function_name = node_name.split("[", 1)[0]
                test_file = repo_root / relative
                if (
                    not separator
                    or not test_file.is_file()
                    or f"def {function_name}(" not in test_file.read_text(encoding="utf-8")
                    or function_name not in junit_names
                ):
                    raise ValueError(f"{criterion} references an unproved test nodeid")
            elif not value.startswith("ci/"):
                raise ValueError(f"{criterion} evidence must reference a test or CI scenario")
        criteria[criterion] = tuple(values)
    return criteria


def build_operations_proof(
    repo_root: Path,
    config_path: Path,
    runtime_policy_path: Path,
    manifest_path: Path,
    mapping_path: Path,
    junit_path: Path,
    evidence_dir: Path,
    output_path: Path,
) -> OperationsProof:
    release_sha = current_code_commit_sha(repo_root)
    if release_sha is None:
        raise ValueError("operations proof requires an exact release commit SHA")
    tests, failures, errors, skipped, junit_names = _junit(junit_path)
    criteria = _load_criteria(repo_root, mapping_path, manifest_path, junit_names)
    platform = _read_object(evidence_dir / "platform.json")
    clean = _read_object(evidence_dir / "clean-health.json")
    restart = _read_object(evidence_dir / "restart-health.json")
    identity = _read_object(evidence_dir / "deployment-identity.json")
    rollback = _read_object(evidence_dir / "rollback.json")
    private_payloads = tuple(
        _read_object(path) for path in sorted(evidence_dir.glob("private-recovery-*.json"))
    )
    states = tuple(sorted(str(item.get("restarted_transition_state")) for item in private_payloads))
    image_ref = str(rollback.get("image_ref", ""))
    image_digest = str(identity.get("image_digest", ""))
    production_transports = sum(
        int(item.get("production_exchange_transports_opened", -1)) for item in private_payloads
    )
    runtime_passed = (
        platform.get("runner_image") == "ubuntu-24.04"
        and clean.get("status") == "PASS"
        and int(clean.get("starts", 0)) >= 1
        and restart.get("status") == "PASS"
        and int(restart.get("starts", 0)) >= 3
        and identity.get("status") == "PASS"
        and identity.get("release_sha") == release_sha
        and re.fullmatch(r"sha256:[0-9a-f]{64}", image_digest) is not None
        and re.fullmatch(r".+@sha256:[0-9a-f]{64}", image_ref) is not None
        and image_ref.endswith("@" + image_digest)
        and rollback.get("status") == "PASS"
        and rollback.get("failed_upgrade_exit_nonzero") is True
        and rollback.get("state_restored") is True
        and rollback.get("release_sha") == release_sha
        and rollback.get("image_ref") == image_ref
        and len(private_payloads) == len(EXPECTED_PRIVATE_STATES)
        and set(states) == EXPECTED_PRIVATE_STATES
        and all(item.get("status") == "PASS" for item in private_payloads)
        and all(item.get("flat_barrier_verified") is True for item in private_payloads)
        and production_transports == 0
    )
    passed = tests > 0 and failures == errors == skipped == 0 and runtime_passed
    proof = OperationsProof(
        schema_version=2,
        generated_at=datetime.now(UTC),
        code_commit_sha=release_sha,
        source_sha256=code_hash(repo_root),
        config_sha256=config_hash(config_path),
        runtime_policy_sha256=_sha256(runtime_policy_path),
        acceptance_manifest_sha256=_sha256(manifest_path),
        junit_sha256=_sha256(junit_path),
        junit_tests=tests,
        runner_image=str(platform.get("runner_image", "")),
        deployed_image_ref=image_ref,
        clean_start_count=int(clean.get("starts", 0)),
        host_restart_count=int(restart.get("starts", 0)),
        private_transition_states=states,
        production_exchange_transports_opened=production_transports,
        criteria=criteria,
        passed=passed,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(f"{output_path.suffix}.tmp")
    temporary.write_text(
        json.dumps(asdict(proof), default=str, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output_path)
    if not passed:
        raise RuntimeError("operations proof did not pass")
    return proof
