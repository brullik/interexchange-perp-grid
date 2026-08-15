from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

from interexchange_perp_grid.config import Settings
from interexchange_perp_grid.qualification import (
    code_hash,
    config_hash,
    current_code_commit_sha,
)

_PIN = re.compile(r"^[A-Za-z0-9_.-]+==[^\s;]+$")


@dataclass(frozen=True, slots=True)
class ReleasePreflight:
    passed: bool
    release_sha: str | None
    source_sha256: str
    config_sha256: str
    image_digest: str
    checks: dict[str, bool]


def evaluate_release_preflight(
    settings: Settings,
    repo_root: Path,
    config_path: Path,
    image_digest: str,
) -> ReleasePreflight:
    root = repo_root.resolve()
    release_sha = current_code_commit_sha(root)
    normalized_image = image_digest.strip().lower()
    status = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=no"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    lock_lines = tuple(
        line.strip()
        for line in (root / "requirements.lock").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )
    checks = {
        "exact_git_head": release_sha is not None,
        "tracked_worktree_clean": status.returncode == 0 and not status.stdout.strip(),
        "immutable_image_digest": (
            normalized_image.startswith("sha256:")
            and len(normalized_image) == 71
            and all(value in "0123456789abcdef" for value in normalized_image[7:])
        ),
        "deterministic_lock": bool(lock_lines) and all(_PIN.fullmatch(line) for line in lock_lines),
        "live_default_disabled": settings.live.enabled is False and settings.app.mode == "shadow",
        "normal_market_forbidden": settings.execution.normal_unbounded_market_allowed is False,
        "wave1_only": set(settings.venues.wave1_public) == {"binanceusdm", "bybit", "okx"},
    }
    return ReleasePreflight(
        passed=all(checks.values()),
        release_sha=release_sha,
        source_sha256=code_hash(root),
        config_sha256=config_hash(config_path.resolve()),
        image_digest=normalized_image,
        checks=checks,
    )
