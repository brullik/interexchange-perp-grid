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
    read_private_event_watermark,
    save_private_event_watermark,
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
async def test_version_seven_route_calibration_schema_migrates_before_indexes(
    tmp_path: Path,
) -> None:
    path = tmp_path / "legacy-v7.sqlite3"
    with sqlite3.connect(path) as database:
        database.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        database.execute("INSERT INTO metadata VALUES ('schema_version', '7')")
        database.execute(
            """
            CREATE TABLE route_calibration_observations (
                observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                route TEXT NOT NULL,
                size_bucket_base_quantity TEXT NOT NULL,
                epoch_id TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )
        database.execute(
            """
            CREATE TABLE route_calibration_parameters (
                route TEXT NOT NULL,
                size_bucket_base_quantity TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(route, size_bucket_base_quantity)
            )
            """
        )
        database.execute(
            """
            CREATE TABLE route_calibration_episodes (
                route TEXT NOT NULL,
                size_bucket_base_quantity TEXT NOT NULL,
                epoch_id TEXT NOT NULL,
                entry_spread_bps TEXT NOT NULL,
                convergence_target_bps TEXT NOT NULL,
                peak_spread_bps TEXT NOT NULL,
                started_at TEXT NOT NULL,
                PRIMARY KEY(route, size_bucket_base_quantity, epoch_id)
            )
            """
        )
        database.execute(
            """
            INSERT INTO route_calibration_observations(
                route, size_bucket_base_quantity, epoch_id, observed_at, payload_json
            ) VALUES ('BTC:BYBIT->OKX', '0.001', 'legacy', '2026-01-01T00:00:00Z', '{}')
            """
        )

    await initialise_state(path)

    with sqlite3.connect(path) as database:
        columns = {
            str(row[1])
            for row in database.execute("PRAGMA table_info(route_calibration_observations)")
        }
        assert "size_bucket_multiplier" in columns
        assert "size_bucket_base_quantity" not in columns
        assert "reason" in columns
        parameter_columns = {
            str(row[1])
            for row in database.execute("PRAGMA table_info(route_calibration_parameters)")
        }
        assert "active" in parameter_columns
        assert "transient_blocked" in parameter_columns
        assert database.execute(
            "SELECT count(*) FROM route_calibration_observations_legacy_v7"
        ).fetchone() == (1,)
        assert database.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'index' AND name = "
            "'route_calibration_observations_key_time_v8'"
        ).fetchone() == ("route_calibration_observations_key_time_v8",)


@pytest.mark.asyncio
async def test_version_nine_adds_transient_calibration_gate(tmp_path: Path) -> None:
    path = tmp_path / "legacy-v9.sqlite3"
    with sqlite3.connect(path) as database:
        database.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        database.execute("INSERT INTO metadata VALUES ('schema_version', '9')")
        database.execute(
            """
            CREATE TABLE route_calibration_parameters (
                route TEXT NOT NULL,
                size_bucket_multiplier TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
                PRIMARY KEY(route, size_bucket_multiplier)
            )
            """
        )

    await initialise_state(path)

    with sqlite3.connect(path) as database:
        columns = {
            str(row[1])
            for row in database.execute("PRAGMA table_info(route_calibration_parameters)")
        }
        assert "transient_blocked" in columns


@pytest.mark.asyncio
async def test_private_event_watermark_persists_and_cannot_regress(tmp_path: Path) -> None:
    path = tmp_path / "private-watermark.sqlite3"
    await initialise_state(path)

    assert await read_private_event_watermark(path, Venue.BYBIT) == 0
    await save_private_event_watermark(path, Venue.BYBIT, 7)
    await save_private_event_watermark(path, Venue.BYBIT, 7)

    assert await read_private_event_watermark(path, Venue.BYBIT) == 7
    with pytest.raises(ValueError, match="cannot regress"):
        await save_private_event_watermark(path, Venue.BYBIT, 6)


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
