from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from interexchange_perp_grid.maintenance import (
    backup_sqlite,
    prune_market_history,
    restore_sqlite,
)
from interexchange_perp_grid.state import (
    initialise_state,
    read_runtime_controls,
    update_runtime_controls,
)


@pytest.mark.asyncio
async def test_sqlite_backup_and_restore_are_integrity_checked(tmp_path: Path) -> None:
    state = tmp_path / "state.sqlite3"
    backup = tmp_path / "backup.sqlite3"
    await initialise_state(state)
    await update_runtime_controls(state, paused=True)
    assert backup_sqlite(state, backup) == backup.resolve()
    await update_runtime_controls(state, paused=False)
    assert (await read_runtime_controls(state)).paused is False
    assert restore_sqlite(backup, state) == state.resolve()
    assert (await read_runtime_controls(state)).paused is True


def test_partition_retention_removes_only_expired_parquet(tmp_path: Path) -> None:
    old = tmp_path / "date=2026-01-01" / "venue=okx" / "old.parquet"
    current = tmp_path / "date=2026-08-14" / "venue=okx" / "current.parquet"
    old.parent.mkdir(parents=True)
    current.parent.mkdir(parents=True)
    old.write_bytes(b"old")
    current.write_bytes(b"current")
    removed = prune_market_history(tmp_path, 30, today=date(2026, 8, 14))
    assert removed == 1
    assert old.exists() is False
    assert current.read_bytes() == b"current"
