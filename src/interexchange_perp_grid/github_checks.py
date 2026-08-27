from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import Any, cast
from urllib.request import Request, urlopen

REQUIRED_FAST_LIVE_CHECKS = frozenset(
    {
        "verify",
        "security",
        "c4-critical-proof",
        "c4-3-proof",
        "docker-smoke",
        "fast-live-adversarial",
    }
)
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_WORKFLOW_PATH = ".github/workflows/ci.yml"


@dataclass(frozen=True, slots=True)
class RequiredChecksEvidence:
    repository: str
    release_sha: str
    remote_main_sha: str
    workflow_id: int
    workflow_path: str
    workflow_run_id: int
    workflow_run_attempt: int
    check_suite_id: int
    event: str
    head_branch: str
    jobs: tuple[tuple[str, int, str, str], ...]
    evidence_sha256: str

    def __post_init__(self) -> None:
        if (
            self.repository != "brullik/interexchange-perp-grid"
            or _COMMIT.fullmatch(self.release_sha) is None
            or self.remote_main_sha != self.release_sha
            or self.workflow_id <= 0
            or self.workflow_path != _WORKFLOW_PATH
            or self.workflow_run_id <= 0
            or self.workflow_run_attempt <= 0
            or self.check_suite_id <= 0
            or self.event != "push"
            or self.head_branch != "main"
            or {name for name, _, _, _ in self.jobs} != REQUIRED_FAST_LIVE_CHECKS
            or any(
                job_id <= 0 or status != "completed" or conclusion != "success"
                for _, job_id, status, conclusion in self.jobs
            )
            or not re.fullmatch(r"[0-9a-f]{64}", self.evidence_sha256)
        ):
            raise ValueError("required GitHub checks evidence is invalid")
        if self.evidence_sha256 != _checks_sha256(self):
            raise ValueError("required GitHub checks evidence hash mismatch")


def fetch_required_checks_evidence(
    release_sha: str,
    *,
    repository: str = "brullik/interexchange-perp-grid",
    timeout_seconds: float = 10.0,
    fetch_json: Callable[[str, float], object] | None = None,
) -> RequiredChecksEvidence:
    """Bind the release to one official ci.yml main-push workflow run and exact jobs."""
    if repository != "brullik/interexchange-perp-grid" or _COMMIT.fullmatch(release_sha) is None:
        raise ValueError("required GitHub checks request identity is invalid")
    fetch = fetch_json or _fetch_public_github_json
    base = f"https://api.github.com/repos/{repository}"
    workflow_selector = "ci.yml"
    branch = fetch(f"{base}/branches/main", timeout_seconds)
    workflow = fetch(f"{base}/actions/workflows/{workflow_selector}", timeout_seconds)
    runs_payload = fetch(
        f"{base}/actions/workflows/{workflow_selector}/runs?branch=main&event=push&per_page=100",
        timeout_seconds,
    )
    if not all(isinstance(item, dict) for item in (branch, workflow, runs_payload)):
        raise ValueError("required GitHub workflow response is malformed")
    branch = cast(dict[str, Any], branch)
    workflow = cast(dict[str, Any], workflow)
    runs_payload = cast(dict[str, Any], runs_payload)
    commit = branch.get("commit")
    remote_main_sha = commit.get("sha") if isinstance(commit, dict) else None
    if remote_main_sha != release_sha:
        raise ValueError("local release is not the current remote main")
    workflow_id = workflow.get("id")
    if (
        not isinstance(workflow_id, int)
        or workflow_id <= 0
        or workflow.get("path") != _WORKFLOW_PATH
        or workflow.get("state") != "active"
    ):
        raise ValueError("required GitHub workflow identity is invalid")
    raw_runs = runs_payload.get("workflow_runs")
    if not isinstance(raw_runs, list):
        raise ValueError("GitHub workflow-runs response is malformed")
    candidates = tuple(
        run
        for run in raw_runs
        if isinstance(run, dict)
        and run.get("head_sha") == release_sha
        and run.get("workflow_id") == workflow_id
        and run.get("event") == "push"
        and run.get("head_branch") == "main"
        and isinstance(run.get("id"), int)
        and isinstance(run.get("run_attempt"), int)
        and isinstance(run.get("check_suite_id"), int)
    )
    if not candidates:
        raise ValueError("exact main-push workflow run is unavailable")
    # A newer push run for the same commit always supersedes every attempt of an
    # older run.  ``run_attempt`` is only meaningful within one workflow-run ID.
    newest_run_id = max(int(run["id"]) for run in candidates)
    selected = max(
        (run for run in candidates if int(run["id"]) == newest_run_id),
        key=lambda run: int(run["run_attempt"]),
    )
    if selected.get("status") != "completed" or selected.get("conclusion") != "success":
        raise ValueError("latest exact main-push workflow did not succeed")
    run_id = int(selected["id"])
    jobs_payload = fetch(
        f"{base}/actions/runs/{run_id}/jobs?filter=latest&per_page=100",
        timeout_seconds,
    )
    if not isinstance(jobs_payload, dict) or not isinstance(jobs_payload.get("jobs"), list):
        raise ValueError("GitHub workflow-jobs response is malformed")
    selected_jobs: dict[str, tuple[int, str, str]] = {}
    for raw in jobs_payload["jobs"]:
        if not isinstance(raw, dict) or raw.get("name") not in REQUIRED_FAST_LIVE_CHECKS:
            continue
        name = str(raw["name"])
        job_id = raw.get("id")
        if (
            not isinstance(job_id, int)
            or raw.get("run_id") != run_id
            or raw.get("head_sha") != release_sha
            or not isinstance(raw.get("status"), str)
            or not isinstance(raw.get("conclusion"), str)
        ):
            continue
        if name in selected_jobs:
            raise ValueError("required GitHub workflow job is ambiguous")
        selected_jobs[name] = (job_id, str(raw["status"]), str(raw["conclusion"]))
    jobs = tuple(
        sorted(
            (name, job_id, status, conclusion)
            for name, (job_id, status, conclusion) in selected_jobs.items()
        )
    )
    evidence_payload: dict[str, Any] = {
        "repository": repository,
        "release_sha": release_sha,
        "remote_main_sha": release_sha,
        "workflow_id": workflow_id,
        "workflow_path": _WORKFLOW_PATH,
        "workflow_run_id": run_id,
        "workflow_run_attempt": int(selected["run_attempt"]),
        "check_suite_id": int(selected["check_suite_id"]),
        "event": "push",
        "head_branch": "main",
        "jobs": jobs,
    }
    digest = hashlib.sha256(
        json.dumps(evidence_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return RequiredChecksEvidence(**evidence_payload, evidence_sha256=digest)


def _fetch_public_github_json(url: str, timeout_seconds: float) -> object:
    request = Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "interexchange-perp-grid-fast-live-preflight",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urlopen(request, timeout=timeout_seconds) as response:  # nosec B310
        return json.loads(response.read().decode("utf-8"))


def _checks_sha256(evidence: RequiredChecksEvidence) -> str:
    payload: dict[str, Any] = asdict(evidence)
    payload.pop("evidence_sha256", None)
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
