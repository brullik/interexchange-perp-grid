from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.domain import InstrumentKey, Venue
from interexchange_perp_grid.reference_history import SourceMinuteBar, build_reference_series
from interexchange_perp_grid.reference_store import ParquetReferenceHistoryStore

START = datetime(2026, 1, 1, tzinfo=UTC)
KEY = InstrumentKey(base="BTC", settle="USDT")


def _bars(venue: Venue) -> tuple[SourceMinuteBar, ...]:
    return tuple(
        SourceMinuteBar(
            venue=venue,
            instrument=KEY,
            symbol="BTC/USDT:USDT",
            interval_start=START + timedelta(minutes=minute),
            open=Decimal("100") + minute,
            high=Decimal("102") + minute,
            low=Decimal("99") + minute,
            close=Decimal("101") + minute,
            contract_metadata_version=f"{venue.value}-v1",
        )
        for minute in range(5)
    )


def _file_bytes(root: Path) -> tuple[tuple[str, bytes], ...]:
    return tuple(
        (path.relative_to(root).as_posix(), path.read_bytes())
        for path in sorted(root.rglob("*.parquet"))
    )


def test_store_is_idempotent_restart_queryable_and_byte_deterministic(tmp_path: Path) -> None:
    bybit = _bars(Venue.BYBIT)
    okx = _bars(Venue.OKX)
    reference = build_reference_series(bybit, okx).bars
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"

    first = ParquetReferenceHistoryStore(first_root)
    first.append_source_bars(bybit)
    first.append_source_bars(okx)
    first.append_reference_bars(reference)
    original = _file_bytes(first_root)
    first.append_source_bars(tuple(reversed(bybit)))
    first.append_reference_bars(tuple(reversed(reference)))

    second = ParquetReferenceHistoryStore(second_root)
    second.append_source_bars(tuple(reversed(bybit)))
    second.append_source_bars(tuple(reversed(okx)))
    second.append_reference_bars(tuple(reversed(reference)))

    assert _file_bytes(first_root) == original
    assert _file_bytes(first_root) == _file_bytes(second_root)
    assert first.manifest_sha256() == second.manifest_sha256()
    restarted = ParquetReferenceHistoryStore(first_root)
    assert (
        restarted.query_source_bars(
            venue=Venue.BYBIT,
            symbol="BTC/USDT:USDT",
            start=START,
            end=START + timedelta(minutes=5),
        )
        == bybit
    )
    assert (
        restarted.query_reference_bars(
            venue_a=Venue.BYBIT,
            venue_b=Venue.OKX,
            instrument=KEY,
            start=START,
            end=START + timedelta(minutes=5),
        )
        == reference
    )


def test_store_rejects_conflicting_resume_without_mutating_partition(tmp_path: Path) -> None:
    store = ParquetReferenceHistoryStore(tmp_path)
    bars = _bars(Venue.BYBIT)
    paths = store.append_source_bars(bars)
    before = paths[0].read_bytes()

    with pytest.raises(ValueError, match="conflicting history row"):
        store.append_source_bars((replace(bars[0], close=Decimal("999")),))

    assert paths[0].read_bytes() == before
    assert tuple(tmp_path.rglob("*.pending")) == ()
