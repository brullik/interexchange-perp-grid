from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from interexchange_perp_grid.qualification import (
    code_hash,
    config_hash,
    current_code_commit_sha,
)

REPLAY_TEST_FILES = (
    "tests/test_live_coordinator.py",
    "tests/test_live_economics.py",
    "tests/test_live_journal.py",
    "tests/test_live_reconciliation.py",
)


@dataclass(frozen=True, slots=True)
class ReplayProof:
    generated_at: datetime
    passed: bool
    code_commit_sha: str
    source_sha256: str
    config_sha256: str
    scenario_count: int
    failure_count: int
    error_count: int
    skipped_count: int
    test_files: tuple[str, ...]
    junit_sha256: str


def _junit_counts(path: Path) -> tuple[int, int, int, int]:
    root = ET.parse(path).getroot()
    if root.tag not in {"testsuite", "testsuites"}:
        raise ValueError("unexpected JUnit root element")
    suites = tuple(root.findall("testsuite")) if root.tag == "testsuites" else ()
    elements = suites or (root,)
    return (
        sum(int(element.attrib.get("tests", "0")) for element in elements),
        sum(int(element.attrib.get("failures", "0")) for element in elements),
        sum(int(element.attrib.get("errors", "0")) for element in elements),
        sum(int(element.attrib.get("skipped", "0")) for element in elements),
    )


def run_replay_proof(
    repo_root: Path,
    config_path: Path,
    output_path: Path,
) -> ReplayProof:
    status = subprocess.run(
        [
            "git",
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--",
            "src",
            "tests",
            "config",
            "pyproject.toml",
            "Makefile",
        ],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if status.stdout.strip():
        raise RuntimeError("replay proof requires a clean exact source/test/config tree")
    release_sha = current_code_commit_sha(repo_root)
    if release_sha is None:
        raise RuntimeError("exact release commit SHA is unavailable")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    junit_path = output_path.with_suffix(".junit.xml")
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            *REPLAY_TEST_FILES,
            f"--junitxml={junit_path}",
        ],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
        timeout=300,
    )
    if not junit_path.is_file():
        raise RuntimeError("replay/fault test run did not produce JUnit evidence")
    scenarios, failures, errors, skipped = _junit_counts(junit_path)
    proof = ReplayProof(
        generated_at=datetime.now(UTC),
        passed=(
            completed.returncode == 0
            and scenarios >= 11
            and failures == 0
            and errors == 0
            and skipped == 0
        ),
        code_commit_sha=release_sha,
        source_sha256=code_hash(repo_root),
        config_sha256=config_hash(config_path),
        scenario_count=scenarios,
        failure_count=failures,
        error_count=errors,
        skipped_count=skipped,
        test_files=REPLAY_TEST_FILES,
        junit_sha256=hashlib.sha256(junit_path.read_bytes()).hexdigest(),
    )
    temporary = output_path.with_suffix(f"{output_path.suffix}.tmp")
    temporary.write_text(
        json.dumps(asdict(proof), default=str, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output_path)
    return proof
