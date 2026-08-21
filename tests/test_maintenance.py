from __future__ import annotations

import gc
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
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
    gc.collect()
    assert restore_sqlite(backup, state) == state.resolve()
    assert not state.with_name(f"{state.name}-wal").exists()
    assert not state.with_name(f"{state.name}-shm").exists()
    assert (await read_runtime_controls(state)).paused is True


def test_restore_replaces_a_real_abrupt_wal_without_reapplying_it(tmp_path: Path) -> None:
    state = tmp_path / "state.sqlite3"
    backup = tmp_path / "backup.sqlite3"
    with closing(sqlite3.connect(state)) as database:
        database.execute("CREATE TABLE marker(value TEXT NOT NULL)")
        database.execute("INSERT INTO marker VALUES ('before')")
        database.commit()
    backup_sqlite(state, backup)
    subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import os,sqlite3,sys; "
                "db=sqlite3.connect(sys.argv[1]); "
                "db.execute('PRAGMA journal_mode=WAL'); "
                "db.execute(\"UPDATE marker SET value='after'\"); "
                "db.commit(); os._exit(0)"
            ),
            str(state),
        ],
        check=True,
    )
    assert state.with_name(f"{state.name}-wal").is_file()

    restore_sqlite(backup, state)

    with closing(sqlite3.connect(state)) as database:
        assert database.execute("SELECT value FROM marker").fetchone() == ("before",)


def test_restore_failure_puts_the_original_database_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = tmp_path / "state.sqlite3"
    backup = tmp_path / "backup.sqlite3"
    with closing(sqlite3.connect(state)) as database:
        database.execute("CREATE TABLE marker(value TEXT NOT NULL)")
        database.execute("INSERT INTO marker VALUES ('before')")
        database.commit()
    backup_sqlite(state, backup)
    with closing(sqlite3.connect(state)) as database:
        database.execute("UPDATE marker SET value = 'after'")
        database.commit()
    real_replace = os.replace

    def reject_staging_install(
        source: str | os.PathLike[str], target: str | os.PathLike[str]
    ) -> None:
        if ".restore-" in Path(source).name:
            raise OSError("injected install failure")
        real_replace(source, target)

    monkeypatch.setattr(os, "replace", reject_staging_install)
    with pytest.raises(OSError, match="injected install failure"):
        restore_sqlite(backup, state)

    with closing(sqlite3.connect(state)) as database:
        assert database.execute("SELECT value FROM marker").fetchone() == ("after",)


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
