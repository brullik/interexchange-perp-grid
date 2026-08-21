from __future__ import annotations

import sqlite3
import time
from pathlib import Path

path = Path("/app/state/ipeg.sqlite3")
deadline = time.monotonic() + 30
while not path.is_file() and time.monotonic() < deadline:
    time.sleep(0.1)
with sqlite3.connect(path) as database:
    database.execute(
        "INSERT INTO metadata(key, value) VALUES ('ops_rollback_sentinel', 'bad') "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
    )
    database.execute("UPDATE metadata SET value = 'invalid' WHERE key = 'schema_version'")
    database.commit()
while True:
    time.sleep(60)
