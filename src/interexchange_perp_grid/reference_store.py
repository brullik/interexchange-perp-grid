from __future__ import annotations

import hashlib
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import cast
from uuid import uuid4

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from interexchange_perp_grid.domain import InstrumentKey, ProductType, Venue
from interexchange_perp_grid.reference_history import (
    ReferenceBarQuality,
    ReferenceSpreadBar,
    SourceBarQuality,
    SourceMinuteBar,
)

_SOURCE_QUERY = """
SELECT * FROM read_parquet(?)
WHERE venue = ? AND symbol = ? AND interval_start >= ? AND interval_start < ?
ORDER BY interval_start
"""
_REFERENCE_QUERY = """
SELECT * FROM read_parquet(?)
WHERE venue_a = ? AND venue_b = ? AND base = ? AND quote = ? AND settle = ?
  AND product_type = ? AND interval_start >= ? AND interval_start < ?
ORDER BY interval_start
"""


class ParquetReferenceHistoryStore:
    """Idempotent source/reference history with atomic deterministic partitions."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self._lock = Lock()

    def append_source_bars(self, bars: tuple[SourceMinuteBar, ...]) -> tuple[Path, ...]:
        groups: dict[Path, list[dict[str, object]]] = {}
        for bar in bars:
            path = self._source_path(bar)
            groups.setdefault(path, []).append(_source_row(bar))
        return self._append_groups(groups, ("venue", "symbol", "interval_start"))

    def append_reference_bars(self, bars: tuple[ReferenceSpreadBar, ...]) -> tuple[Path, ...]:
        groups: dict[Path, list[dict[str, object]]] = {}
        for bar in bars:
            path = self._reference_path(bar)
            groups.setdefault(path, []).append(_reference_row(bar))
        return self._append_groups(
            groups,
            ("venue_a", "venue_b", "base", "quote", "settle", "interval_start"),
        )

    def query_source_bars(
        self,
        *,
        venue: Venue,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> tuple[SourceMinuteBar, ...]:
        files = tuple((self.root / "source").rglob("*.parquet"))
        if not files:
            return ()
        rows = _query(
            files,
            _SOURCE_QUERY,
            (venue.value, symbol, start.isoformat(), end.isoformat()),
        )
        return tuple(_source_from_row(row) for row in rows)

    def query_reference_bars(
        self,
        *,
        venue_a: Venue,
        venue_b: Venue,
        instrument: InstrumentKey,
        start: datetime,
        end: datetime,
    ) -> tuple[ReferenceSpreadBar, ...]:
        files = tuple((self.root / "reference").rglob("*.parquet"))
        if not files:
            return ()
        rows = _query(
            files,
            _REFERENCE_QUERY,
            (
                venue_a.value,
                venue_b.value,
                instrument.base,
                instrument.quote,
                instrument.settle,
                instrument.product_type.value,
                start.isoformat(),
                end.isoformat(),
            ),
        )
        return tuple(_reference_from_row(row) for row in rows)

    def manifest_sha256(self) -> str:
        digest = hashlib.sha256()
        for path in sorted(self.root.rglob("*.parquet")):
            digest.update(path.relative_to(self.root).as_posix().encode("utf-8"))
            digest.update(hashlib.sha256(path.read_bytes()).digest())
        return digest.hexdigest()

    def _append_groups(
        self,
        groups: dict[Path, list[dict[str, object]]],
        key_fields: tuple[str, ...],
    ) -> tuple[Path, ...]:
        written: list[Path] = []
        with self._lock:
            for path, rows in sorted(groups.items(), key=lambda item: str(item[0])):
                path.parent.mkdir(parents=True, exist_ok=True)
                existing = pq.read_table(path).to_pylist() if path.exists() else []
                merged = _merge_rows((*existing, *rows), key_fields)
                table = pa.Table.from_pylist(merged)
                pending = path.with_name(f"{path.name}.{uuid4().hex}.pending")
                try:
                    pq.write_table(
                        table,
                        pending,
                        compression="zstd",
                        version="2.6",
                        write_statistics=True,
                    )
                    pending.replace(path)
                finally:
                    pending.unlink(missing_ok=True)
                written.append(path)
        return tuple(written)

    def _source_path(self, bar: SourceMinuteBar) -> Path:
        symbol_key = hashlib.sha256(bar.symbol.encode("utf-8")).hexdigest()[:16]
        return (
            self.root
            / "source"
            / bar.venue.value
            / symbol_key
            / bar.contract_metadata_version
            / f"{bar.interval_start.date().isoformat()}.parquet"
        )

    def _reference_path(self, bar: ReferenceSpreadBar) -> Path:
        identity = f"{bar.instrument.base}-{bar.instrument.quote}-{bar.instrument.settle}"
        return (
            self.root
            / "reference"
            / f"{bar.venue_a.value}-{bar.venue_b.value}"
            / identity
            / f"{bar.interval_start.date().isoformat()}.parquet"
        )


def _merge_rows(
    rows: Iterable[dict[str, object]],
    key_fields: tuple[str, ...],
) -> list[dict[str, object]]:
    by_key: dict[tuple[object, ...], dict[str, object]] = {}
    for row in rows:
        normalized = {str(key): value for key, value in row.items()}
        key = tuple(normalized[field] for field in key_fields)
        existing = by_key.get(key)
        if existing is not None and existing != normalized:
            raise ValueError(f"conflicting history row for key {key!r}")
        by_key[key] = normalized
    return [
        by_key[key] for key in sorted(by_key, key=lambda item: tuple(str(part) for part in item))
    ]


def _query(
    files: tuple[Path, ...],
    query: str,
    parameters: tuple[object, ...],
) -> list[dict[str, object]]:
    parquet_files = [str(path).replace("\\", "/") for path in files]
    with duckdb.connect(":memory:") as database:
        table = database.execute(
            query,
            (parquet_files, *parameters),
        ).to_arrow_table()
    return cast(list[dict[str, object]], table.to_pylist())


def _instrument_row(key: InstrumentKey) -> dict[str, object]:
    return {
        "base": key.base,
        "quote": key.quote,
        "settle": key.settle,
        "product_type": key.product_type.value,
    }


def _source_row(bar: SourceMinuteBar) -> dict[str, object]:
    return {
        "venue": bar.venue.value,
        "symbol": bar.symbol,
        **_instrument_row(bar.instrument),
        "interval_start": bar.interval_start.isoformat(),
        "open": str(bar.open),
        "high": str(bar.high),
        "low": str(bar.low),
        "close": str(bar.close),
        "contract_metadata_version": bar.contract_metadata_version,
        "quality": bar.quality.value,
    }


def _reference_row(bar: ReferenceSpreadBar) -> dict[str, object]:
    return {
        "venue_a": bar.venue_a.value,
        "venue_b": bar.venue_b.value,
        **_instrument_row(bar.instrument),
        "interval_start": bar.interval_start.isoformat(),
        "open_bps": str(bar.open_bps),
        "high_bps": str(bar.high_bps),
        "low_bps": str(bar.low_bps),
        "close_bps": str(bar.close_bps),
        "contract_metadata_version_a": bar.contract_metadata_version_a,
        "contract_metadata_version_b": bar.contract_metadata_version_b,
        "quality": bar.quality.value,
        "synthetic_high_low_envelope": bar.synthetic_high_low_envelope,
        "executable": bar.executable,
    }


def _key_from_row(row: dict[str, object]) -> InstrumentKey:
    return InstrumentKey(
        base=str(row["base"]),
        quote=str(row["quote"]),
        settle=str(row["settle"]),
        product_type=ProductType(str(row["product_type"])),
    )


def _source_from_row(row: dict[str, object]) -> SourceMinuteBar:
    from decimal import Decimal

    return SourceMinuteBar(
        venue=Venue(str(row["venue"])),
        instrument=_key_from_row(row),
        symbol=str(row["symbol"]),
        interval_start=datetime.fromisoformat(str(row["interval_start"])),
        open=Decimal(str(row["open"])),
        high=Decimal(str(row["high"])),
        low=Decimal(str(row["low"])),
        close=Decimal(str(row["close"])),
        contract_metadata_version=str(row["contract_metadata_version"]),
        quality=SourceBarQuality(str(row["quality"])),
    )


def _reference_from_row(row: dict[str, object]) -> ReferenceSpreadBar:
    from decimal import Decimal

    return ReferenceSpreadBar(
        venue_a=Venue(str(row["venue_a"])),
        venue_b=Venue(str(row["venue_b"])),
        instrument=_key_from_row(row),
        interval_start=datetime.fromisoformat(str(row["interval_start"])),
        open_bps=Decimal(str(row["open_bps"])),
        high_bps=Decimal(str(row["high_bps"])),
        low_bps=Decimal(str(row["low_bps"])),
        close_bps=Decimal(str(row["close_bps"])),
        contract_metadata_version_a=str(row["contract_metadata_version_a"]),
        contract_metadata_version_b=str(row["contract_metadata_version_b"]),
        quality=ReferenceBarQuality(str(row["quality"])),
        synthetic_high_low_envelope=bool(row["synthetic_high_low_envelope"]),
        executable=bool(row["executable"]),
    )
