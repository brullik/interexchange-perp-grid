from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path


SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def _initialise_state_sync(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as database:
        database.execute("PRAGMA journal_mode=WAL")
        database.execute("PRAGMA synchronous=FULL")
        database.executescript(SCHEMA)
        database.execute(
            "INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)",
            ("schema_version", "1"),
        )
        database.commit()


async def initialise_state(path: Path) -> None:
    await asyncio.to_thread(_initialise_state_sync, path)
