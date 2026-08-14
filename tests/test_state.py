from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from interexchange_perp_grid.state import initialise_state


@pytest.mark.asyncio
async def test_state_store_uses_wal_and_is_restart_safe(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    await initialise_state(path)
    await initialise_state(path)

    with sqlite3.connect(path) as database:
        journal_mode = database.execute("PRAGMA journal_mode").fetchone()
        assert journal_mode is not None
        assert journal_mode[0].lower() == "wal"
        version = database.execute(
            "SELECT value FROM metadata WHERE key = ?", ("schema_version",)
        ).fetchone()
        assert version == ("1",)
