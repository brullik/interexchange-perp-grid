from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.domain import BookLevel, OrderBookSnapshot, Venue
from interexchange_perp_grid.history import ParquetMarketRecorder
from interexchange_perp_grid.qualification import (
    qualification_is_current,
    run_qualification,
)
from interexchange_perp_grid.reason_codes import ReasonCode


@pytest.mark.asyncio
async def test_qualification_is_bound_to_code_config_data_and_freshness(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    source = repo / "src"
    source.mkdir(parents=True)
    code_file = source / "product.py"
    code_file.write_text("SAFE = True\n", encoding="utf-8")
    config = repo / "config.yaml"
    config.write_text("mode: shadow\n", encoding="utf-8")
    data = repo / "data"
    recorder = ParquetMarketRecorder(data)
    observed_at = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)
    await recorder.append_books(
        (
            OrderBookSnapshot(
                Venue.BYBIT,
                "BTC/USDT:USDT",
                (BookLevel(Decimal("100"), Decimal("1")),),
                (BookLevel(Decimal("101"), Decimal("1")),),
                1,
                observed_at,
                1,
                1,
                1,
                True,
                True,
                0,
            ),
        )
    )
    evidence_path = repo / "state" / "qualification.json"
    evidence = run_qualification(repo, config, data, evidence_path, 2, observed_at)
    assert evidence.accepted is True
    assert evidence.reason == ReasonCode.QUALIFICATION_PASSED
    assert qualification_is_current(
        evidence,
        repo,
        config,
        data,
        3600,
        observed_at + timedelta(minutes=1),
    ) == (True, ReasonCode.QUALIFICATION_PASSED)

    code_file.write_text("SAFE = False\n", encoding="utf-8")
    assert qualification_is_current(
        evidence,
        repo,
        config,
        data,
        3600,
        observed_at + timedelta(minutes=1),
    ) == (False, ReasonCode.QUALIFICATION_HASH_MISMATCH)
    code_file.write_text("SAFE = True\n", encoding="utf-8")
    assert qualification_is_current(
        evidence,
        repo,
        config,
        data,
        60,
        observed_at + timedelta(minutes=2),
    ) == (False, ReasonCode.QUALIFICATION_HASH_MISMATCH)


def test_insufficient_qualification_writes_honest_failure(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "src" / "product.py").write_text("SAFE = True\n", encoding="utf-8")
    config = repo / "config.yaml"
    config.write_text("mode: shadow\n", encoding="utf-8")
    evidence = run_qualification(
        repo,
        config,
        repo / "missing-data",
        repo / "qualification.json",
        1,
    )
    assert evidence.accepted is False
    assert evidence.reason == ReasonCode.QUALIFICATION_INSUFFICIENT
