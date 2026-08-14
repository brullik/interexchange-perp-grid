from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from interexchange_perp_grid.history import query_recorded_level_count
from interexchange_perp_grid.reason_codes import ReasonCode


@dataclass(frozen=True, slots=True)
class QualificationEvidence:
    generated_at: datetime
    code_sha256: str
    config_sha256: str
    data_sha256: str
    sample_count: int
    minimum_samples: int
    accepted: bool
    reason: ReasonCode


def _hash_files(files: tuple[Path, ...], relative_to: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(files, key=lambda item: item.as_posix()):
        digest.update(path.relative_to(relative_to).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def code_hash(repo_root: Path) -> str:
    source_root = repo_root / "src"
    return _hash_files(tuple(source_root.rglob("*.py")), repo_root)


def config_hash(config_path: Path) -> str:
    return hashlib.sha256(config_path.read_bytes()).hexdigest()


def data_hash(data_root: Path) -> str:
    files = tuple(data_root.rglob("*.parquet")) if data_root.is_dir() else ()
    return _hash_files(files, data_root) if files else hashlib.sha256(b"").hexdigest()


def run_qualification(
    repo_root: Path,
    config_path: Path,
    data_root: Path,
    evidence_path: Path,
    minimum_samples: int,
    now: datetime | None = None,
) -> QualificationEvidence:
    if minimum_samples <= 0:
        raise ValueError("qualification minimum sample count must be positive")
    samples = query_recorded_level_count(data_root)
    accepted = samples >= minimum_samples
    evidence = QualificationEvidence(
        generated_at=now or datetime.now(UTC),
        code_sha256=code_hash(repo_root),
        config_sha256=config_hash(config_path),
        data_sha256=data_hash(data_root),
        sample_count=samples,
        minimum_samples=minimum_samples,
        accepted=accepted,
        reason=(
            ReasonCode.QUALIFICATION_PASSED if accepted else ReasonCode.QUALIFICATION_INSUFFICIENT
        ),
    )
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = evidence_path.with_suffix(f"{evidence_path.suffix}.tmp")
    temporary.write_text(
        json.dumps(asdict(evidence), default=str, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(evidence_path)
    return evidence


def load_qualification(path: Path) -> QualificationEvidence:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return QualificationEvidence(
        generated_at=datetime.fromisoformat(str(payload["generated_at"])),
        code_sha256=str(payload["code_sha256"]),
        config_sha256=str(payload["config_sha256"]),
        data_sha256=str(payload["data_sha256"]),
        sample_count=int(payload["sample_count"]),
        minimum_samples=int(payload["minimum_samples"]),
        accepted=bool(payload["accepted"]),
        reason=ReasonCode(str(payload["reason"])),
    )


def qualification_is_current(
    evidence: QualificationEvidence,
    repo_root: Path,
    config_path: Path,
    data_root: Path,
    max_age_seconds: int,
    now: datetime | None = None,
) -> tuple[bool, ReasonCode]:
    observed_at = now or datetime.now(UTC)
    hashes_match = (
        evidence.code_sha256 == code_hash(repo_root)
        and evidence.config_sha256 == config_hash(config_path)
        and evidence.data_sha256 == data_hash(data_root)
    )
    fresh = (observed_at - evidence.generated_at).total_seconds() <= max_age_seconds
    if not evidence.accepted or not hashes_match or not fresh:
        return False, ReasonCode.QUALIFICATION_HASH_MISMATCH
    return True, ReasonCode.QUALIFICATION_PASSED
