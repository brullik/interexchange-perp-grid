from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path


def _resolved_distinct(source: Path, target: Path) -> tuple[Path, Path]:
    resolved_source = source.resolve()
    resolved_target = target.resolve()
    if resolved_source == resolved_target:
        raise ValueError("source and target paths must differ")
    return resolved_source, resolved_target


def backup_sqlite(source: Path, target: Path) -> Path:
    resolved_source, resolved_target = _resolved_distinct(source, target)
    if not resolved_source.is_file():
        raise FileNotFoundError(resolved_source)
    resolved_target.parent.mkdir(parents=True, exist_ok=True)
    with (
        sqlite3.connect(resolved_source) as source_database,
        sqlite3.connect(resolved_target) as target_database,
    ):
        source_database.backup(target_database)
        integrity = target_database.execute("PRAGMA integrity_check").fetchone()
        if integrity != ("ok",):
            raise RuntimeError("backup integrity check failed")
    return resolved_target


def restore_sqlite(backup: Path, target: Path) -> Path:
    resolved_backup, resolved_target = _resolved_distinct(backup, target)
    if not resolved_backup.is_file():
        raise FileNotFoundError(resolved_backup)
    with sqlite3.connect(f"file:{resolved_backup.as_posix()}?mode=ro", uri=True) as backup_database:
        integrity = backup_database.execute("PRAGMA integrity_check").fetchone()
        if integrity != ("ok",):
            raise RuntimeError("restore source integrity check failed")
        resolved_target.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(resolved_target) as target_database:
            backup_database.backup(target_database)
    return resolved_target


def prune_market_history(root: Path, retention_days: int, today: date | None = None) -> int:
    if retention_days <= 0:
        raise ValueError("retention days must be positive")
    resolved_root = root.resolve()
    if not resolved_root.is_dir():
        return 0
    cutoff = (today or date.today()) - timedelta(days=retention_days)
    removed = 0
    for path in resolved_root.rglob("*.parquet"):
        resolved_path = path.resolve()
        if resolved_root not in resolved_path.parents:
            raise RuntimeError("history path escaped the configured root")
        partition_date = next(
            (
                part.removeprefix("date=")
                for part in path.relative_to(resolved_root).parts
                if part.startswith("date=")
            ),
            None,
        )
        if partition_date is None:
            continue
        try:
            observed_date = date.fromisoformat(partition_date)
        except ValueError:
            continue
        if observed_date < cutoff:
            path.unlink()
            removed += 1
    return removed
