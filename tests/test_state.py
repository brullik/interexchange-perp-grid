from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.execution import (
    Fill,
    OrderPurpose,
    PairActionState,
    Side,
    Tranche,
)
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.state import (
    SCHEMA_VERSION,
    initialise_state,
    load_tranches,
    save_tranche,
)
from interexchange_perp_grid.strategy import DirectedRouteKey


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
        assert version == (SCHEMA_VERSION,)


@pytest.mark.asyncio
async def test_version_one_state_migrates_without_losing_metadata(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(path) as database:
        database.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        database.execute("INSERT INTO metadata VALUES ('schema_version', '1')")
        database.execute("INSERT INTO metadata VALUES ('owner_value', 'preserved')")
    await initialise_state(path)
    with sqlite3.connect(path) as database:
        assert database.execute(
            "SELECT value FROM metadata WHERE key = 'schema_version'"
        ).fetchone() == (SCHEMA_VERSION,)
        assert database.execute(
            "SELECT value FROM metadata WHERE key = 'owner_value'"
        ).fetchone() == ("preserved",)


@pytest.mark.asyncio
async def test_full_simulated_tranche_ledger_round_trips(tmp_path: Path) -> None:
    path = tmp_path / "ledger.sqlite3"
    await initialise_state(path)
    item = Tranche(
        "T1",
        DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX),
        Decimal("0.1"),
        Decimal("1"),
        Decimal("20"),
        Decimal("4"),
        state=PairActionState.HEDGED,
        reason=ReasonCode.ORDERS_HEDGED,
        entry_long_fills=[
            Fill(
                "long",
                Venue.BYBIT,
                Side.BUY,
                OrderPurpose.NORMAL_OPEN,
                Decimal("0.1"),
                Decimal("100"),
                Decimal("0.01"),
            )
        ],
        entry_short_fills=[
            Fill(
                "short",
                Venue.OKX,
                Side.SELL,
                OrderPurpose.NORMAL_OPEN,
                Decimal("0.1"),
                Decimal("110"),
                Decimal("0.01"),
            )
        ],
        funding_usdt=Decimal("0.02"),
        processed_order_ids={"long", "short"},
    )
    await save_tranche(path, item)
    restored = await load_tranches(path)
    assert restored == (item,)
