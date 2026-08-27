from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from interexchange_perp_grid.github_checks import (
    REQUIRED_FAST_LIVE_CHECKS,
    fetch_required_checks_evidence,
)


def _responses(
    release: str,
    *,
    conclusion: str = "success",
    missing: str | None = None,
    wrong_job_run: bool = False,
) -> Callable[[str, float], object]:
    workflow_id = 271
    run_id = 901
    jobs = [
        {
            "id": index,
            "run_id": run_id + int(wrong_job_run and name == "verify"),
            "head_sha": release,
            "name": name,
            "status": "completed",
            "conclusion": "success",
        }
        for index, name in enumerate(sorted(REQUIRED_FAST_LIVE_CHECKS), start=1)
        if name != missing
    ]

    def fetch(url: str, timeout: float) -> object:
        assert timeout > 0
        if url.endswith("/branches/main"):
            return {"commit": {"sha": release}}
        if "/actions/workflows/" in url and url.endswith("ci.yml"):
            assert "/actions/workflows/ci.yml" in url
            return {"id": workflow_id, "path": ".github/workflows/ci.yml", "state": "active"}
        if "/actions/workflows/" in url and "/runs?" in url:
            return {
                "workflow_runs": [
                    {
                        "id": run_id,
                        "run_attempt": 1,
                        "check_suite_id": 501,
                        "workflow_id": workflow_id,
                        "head_sha": release,
                        "head_branch": "main",
                        "event": "push",
                        "status": "completed",
                        "conclusion": conclusion,
                    }
                ]
            }
        if f"/actions/runs/{run_id}/jobs" in url:
            return {"jobs": jobs}
        raise AssertionError(url)

    return fetch


def test_required_checks_accept_one_official_exact_main_workflow_run() -> None:
    release = "a" * 40
    evidence = fetch_required_checks_evidence(release, fetch_json=_responses(release))

    assert evidence.remote_main_sha == release
    assert evidence.workflow_path == ".github/workflows/ci.yml"
    assert evidence.workflow_run_id == 901
    assert {name for name, _, _, _ in evidence.jobs} == REQUIRED_FAST_LIVE_CHECKS


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"missing": "security"}, "evidence is invalid"),
        ({"conclusion": "failure"}, "did not succeed"),
        ({"wrong_job_run": True}, "evidence is invalid"),
    ],
)
def test_required_checks_reject_missing_failing_or_cross_run_jobs(
    kwargs: dict[str, Any], message: str
) -> None:
    release = "b" * 40
    with pytest.raises(ValueError, match=message):
        fetch_required_checks_evidence(release, fetch_json=_responses(release, **kwargs))


def test_required_checks_reject_non_main_release() -> None:
    release = "c" * 40
    fetch = _responses("d" * 40)
    with pytest.raises(ValueError, match="not the current remote main"):
        fetch_required_checks_evidence(release, fetch_json=fetch)


def test_required_checks_latest_attempt_failure_cannot_fall_back_to_old_success() -> None:
    release = "e" * 40
    base_fetch = _responses(release)

    def fetch(url: str, timeout: float) -> object:
        if "/runs?" not in url:
            return base_fetch(url, timeout)
        return {
            "workflow_runs": [
                {
                    "id": 900,
                    "run_attempt": 1,
                    "check_suite_id": 500,
                    "workflow_id": 271,
                    "head_sha": release,
                    "head_branch": "main",
                    "event": "push",
                    "status": "completed",
                    "conclusion": "success",
                },
                {
                    "id": 901,
                    "run_attempt": 2,
                    "check_suite_id": 501,
                    "workflow_id": 271,
                    "head_sha": release,
                    "head_branch": "main",
                    "event": "push",
                    "status": "completed",
                    "conclusion": "failure",
                },
            ]
        }

    with pytest.raises(ValueError, match="did not succeed"):
        fetch_required_checks_evidence(release, fetch_json=fetch)


def test_required_checks_newer_failed_run_beats_older_successful_rerun() -> None:
    release = "f" * 40
    base_fetch = _responses(release)

    def fetch(url: str, timeout: float) -> object:
        if "/runs?" not in url:
            return base_fetch(url, timeout)
        return {
            "workflow_runs": [
                {
                    "id": 900,
                    "run_attempt": 2,
                    "check_suite_id": 500,
                    "workflow_id": 271,
                    "head_sha": release,
                    "head_branch": "main",
                    "event": "push",
                    "status": "completed",
                    "conclusion": "success",
                },
                {
                    "id": 901,
                    "run_attempt": 1,
                    "check_suite_id": 501,
                    "workflow_id": 271,
                    "head_sha": release,
                    "head_branch": "main",
                    "event": "push",
                    "status": "completed",
                    "conclusion": "failure",
                },
            ]
        }

    with pytest.raises(ValueError, match="did not succeed"):
        fetch_required_checks_evidence(release, fetch_json=fetch)


def test_required_checks_newer_in_progress_run_blocks_older_success() -> None:
    release = "1" * 40
    base_fetch = _responses(release)

    def fetch(url: str, timeout: float) -> object:
        if "/runs?" not in url:
            return base_fetch(url, timeout)
        return {
            "workflow_runs": [
                {
                    "id": 900,
                    "run_attempt": 1,
                    "check_suite_id": 500,
                    "workflow_id": 271,
                    "head_sha": release,
                    "head_branch": "main",
                    "event": "push",
                    "status": "completed",
                    "conclusion": "success",
                },
                {
                    "id": 901,
                    "run_attempt": 1,
                    "check_suite_id": 501,
                    "workflow_id": 271,
                    "head_sha": release,
                    "head_branch": "main",
                    "event": "push",
                    "status": "in_progress",
                    "conclusion": None,
                },
            ]
        }

    with pytest.raises(ValueError, match="did not succeed"):
        fetch_required_checks_evidence(release, fetch_json=fetch)
