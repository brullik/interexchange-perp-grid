from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.state import (
    QualificationEpochStatus,
    finalize_qualification_epoch,
    initialise_state,
    read_qualification_epoch,
    record_qualification_exception,
    start_qualification_epoch,
)
from interexchange_perp_grid.strategy import DirectedRouteKey

_ROUTE = DirectedRouteKey("BTC", Venue.BINANCE_USDM, Venue.OKX)
_RELEASE = "a" * 40
_SOURCE = "b" * 64
_CONFIG = "c" * 64
_IMAGE = "sha256:" + "d" * 64


@pytest.mark.asyncio
async def test_exact_epoch_commands_are_idempotent_and_finalize_is_immutable(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    await initialise_state(path)
    started_at = datetime(2026, 8, 15, tzinfo=UTC)
    first = await start_qualification_epoch(
        path, _ROUTE, _RELEASE, _SOURCE, _CONFIG, _IMAGE, started_at
    )
    repeated = await start_qualification_epoch(
        path, _ROUTE, _RELEASE, _SOURCE, _CONFIG, _IMAGE, started_at + timedelta(seconds=1)
    )
    assert repeated == first
    await record_qualification_exception(path, first.epoch_id, "Injected", started_at)
    finalized = await finalize_qualification_epoch(
        path, first.epoch_id, started_at + timedelta(hours=24)
    )
    assert finalized.status == QualificationEpochStatus.FINALIZED
    assert await finalize_qualification_epoch(path, first.epoch_id) == finalized
    assert (
        await start_qualification_epoch(path, _ROUTE, _RELEASE, _SOURCE, _CONFIG, _IMAGE)
        == finalized
    )
    with pytest.raises(RuntimeError, match="running exact epoch"):
        await record_qualification_exception(path, first.epoch_id, "LateObservation")


@pytest.mark.parametrize(
    ("route", "release", "source", "config", "image"),
    [
        (_ROUTE, "e" * 40, _SOURCE, _CONFIG, _IMAGE),
        (_ROUTE, _RELEASE, "e" * 64, _CONFIG, _IMAGE),
        (_ROUTE, _RELEASE, _SOURCE, "e" * 64, _IMAGE),
        (_ROUTE, _RELEASE, _SOURCE, _CONFIG, "sha256:" + "e" * 64),
        (
            DirectedRouteKey("ETH", Venue.BINANCE_USDM, Venue.OKX),
            _RELEASE,
            _SOURCE,
            _CONFIG,
            _IMAGE,
        ),
    ],
    ids=["release", "source", "config", "image", "route"],
)
@pytest.mark.asyncio
async def test_any_identity_change_closes_old_epoch_and_resets_duration(
    tmp_path: Path,
    route: DirectedRouteKey,
    release: str,
    source: str,
    config: str,
    image: str,
) -> None:
    path = tmp_path / "state.sqlite3"
    await initialise_state(path)
    start = datetime(2026, 8, 15, tzinfo=UTC)
    old = await start_qualification_epoch(path, _ROUTE, _RELEASE, _SOURCE, _CONFIG, _IMAGE, start)
    changed_at = start + timedelta(hours=23)
    new = await start_qualification_epoch(path, route, release, source, config, image, changed_at)
    persisted_old = await read_qualification_epoch(path, old.epoch_id)
    assert persisted_old is not None
    assert persisted_old.status == QualificationEpochStatus.CLOSED
    assert persisted_old.ended_at == changed_at
    assert new.epoch_id != old.epoch_id
    assert new.started_at == changed_at
    assert new.status == QualificationEpochStatus.RUNNING
    with pytest.raises(RuntimeError, match="running exact epoch"):
        await record_qualification_exception(path, old.epoch_id, "MixedEpoch")


@pytest.mark.asyncio
async def test_orphan_observation_is_rejected_and_every_row_has_epoch_fk(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    await initialise_state(path)
    with pytest.raises(RuntimeError, match="running exact epoch"):
        await record_qualification_exception(path, "missing", "Orphan")
    epoch = await start_qualification_epoch(path, _ROUTE, _RELEASE, _SOURCE, _CONFIG, _IMAGE)
    await record_qualification_exception(path, epoch.epoch_id, "Bound")
    with sqlite3.connect(path) as database:
        rows = database.execute("SELECT epoch_id FROM qualification_runtime_errors").fetchall()
    assert rows == [(epoch.epoch_id,)]
