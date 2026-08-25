from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass, replace
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
    ReferenceSeriesResult,
    ReferenceSpreadBar,
    SourceBarQuality,
    SourceMinuteBar,
    build_reference_series,
    reference_bars_sha256,
    source_bars_sha256,
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


@dataclass(frozen=True, slots=True)
class ReferenceWindowManifest:
    schema_version: int
    window_start: datetime
    window_end: datetime
    venue_a: Venue
    venue_b: Venue
    instrument: InstrumentKey
    symbol_a: str
    symbol_b: str
    source_a_sha256: str
    source_b_sha256: str
    reference_bars_sha256: str
    dataset_sha256: str
    accepted_minutes: int
    rejected_minutes: int
    rejection_ledger: tuple[tuple[str, str], ...]
    manifest_sha256: str

    def __post_init__(self) -> None:
        if self.schema_version != 1 or self.window_end <= self.window_start:
            raise ValueError("reference window manifest is invalid")
        if self.venue_a.value >= self.venue_b.value:
            raise ValueError("reference window manifest venue order is not canonical")
        if self.accepted_minutes < 0 or self.rejected_minutes < 0:
            raise ValueError("reference window manifest counts are invalid")
        if self.accepted_minutes + self.rejected_minutes != int(
            (self.window_end - self.window_start).total_seconds() // 60
        ):
            raise ValueError("reference window manifest does not cover every requested minute")
        if self.manifest_sha256 and _window_manifest_sha256(self) != self.manifest_sha256:
            raise ValueError("reference window manifest hash mismatch")


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
        return _tree_manifest_sha256(self.root)

    def source_manifest_sha256(self) -> str:
        return _tree_manifest_sha256(self.root / "source")

    def reference_manifest_sha256(self) -> str:
        return _tree_manifest_sha256(self.root / "reference")

    def write_window_manifest(
        self,
        series: ReferenceSeriesResult,
        source_a: tuple[SourceMinuteBar, ...],
        source_b: tuple[SourceMinuteBar, ...],
    ) -> ReferenceWindowManifest:
        if (
            series.window_start is None
            or series.window_end is None
            or series.venue_a is None
            or series.venue_b is None
            or series.instrument is None
        ):
            raise ValueError("exact reference window identity is required")
        by_venue = {source_a[0].venue: source_a, source_b[0].venue: source_b}
        try:
            canonical_a = by_venue[series.venue_a]
            canonical_b = by_venue[series.venue_b]
        except (IndexError, KeyError) as error:
            raise ValueError("reference window source identity is incomplete") from error
        if (
            series.symbol_a != canonical_a[0].symbol
            or series.symbol_b != canonical_b[0].symbol
            or series.source_a_sha256 != source_bars_sha256(canonical_a)
            or series.source_b_sha256 != source_bars_sha256(canonical_b)
        ):
            raise ValueError("reference window source provenance changed")
        manifest = ReferenceWindowManifest(
            schema_version=1,
            window_start=series.window_start,
            window_end=series.window_end,
            venue_a=series.venue_a,
            venue_b=series.venue_b,
            instrument=series.instrument,
            symbol_a=series.symbol_a,
            symbol_b=series.symbol_b,
            source_a_sha256=series.source_a_sha256,
            source_b_sha256=series.source_b_sha256,
            reference_bars_sha256=reference_bars_sha256(series.bars),
            dataset_sha256=series.dataset_sha256,
            accepted_minutes=len(series.bars),
            rejected_minutes=len(series.rejections),
            rejection_ledger=tuple(
                (item.interval_start.isoformat(), item.reason.value) for item in series.rejections
            ),
            manifest_sha256="",
        )
        manifest = replace(manifest, manifest_sha256=_window_manifest_sha256(manifest))
        path = self._window_manifest_path(manifest.dataset_sha256)
        path.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(asdict(manifest), default=str, indent=2, sort_keys=True) + "\n"
        if path.exists():
            if path.read_text(encoding="utf-8") != encoded:
                raise RuntimeError("immutable reference window manifest already exists")
            return manifest
        pending = path.with_name(f".{path.name}.{uuid4().hex}.pending")
        try:
            pending.write_text(encoded, encoding="utf-8")
            pending.replace(path)
        finally:
            pending.unlink(missing_ok=True)
        return manifest

    def load_window_manifest(self, dataset_sha256: str) -> ReferenceWindowManifest:
        path = self._window_manifest_path(dataset_sha256)
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("reference window manifest payload is invalid")
        instrument = raw.get("instrument")
        if not isinstance(instrument, dict):
            raise ValueError("reference window manifest instrument is invalid")
        manifest = ReferenceWindowManifest(
            schema_version=int(str(raw["schema_version"])),
            window_start=datetime.fromisoformat(str(raw["window_start"])),
            window_end=datetime.fromisoformat(str(raw["window_end"])),
            venue_a=Venue(str(raw["venue_a"])),
            venue_b=Venue(str(raw["venue_b"])),
            instrument=InstrumentKey(
                base=str(instrument["base"]),
                quote=str(instrument["quote"]),
                settle=str(instrument["settle"]),
                product_type=ProductType(str(instrument["product_type"])),
            ),
            symbol_a=str(raw["symbol_a"]),
            symbol_b=str(raw["symbol_b"]),
            source_a_sha256=str(raw["source_a_sha256"]),
            source_b_sha256=str(raw["source_b_sha256"]),
            reference_bars_sha256=str(raw["reference_bars_sha256"]),
            dataset_sha256=str(raw["dataset_sha256"]),
            accepted_minutes=int(str(raw["accepted_minutes"])),
            rejected_minutes=int(str(raw["rejected_minutes"])),
            rejection_ledger=tuple(
                (str(item[0]), str(item[1])) for item in raw["rejection_ledger"]
            ),
            manifest_sha256=str(raw["manifest_sha256"]),
        )
        if not manifest.manifest_sha256:
            raise ValueError("reference window manifest hash is missing")
        if manifest.dataset_sha256 != dataset_sha256:
            raise ValueError("reference window manifest filename identity mismatch")
        return manifest

    def verify_window_manifest(self, manifest: ReferenceWindowManifest) -> ReferenceSeriesResult:
        source_a = self.query_source_bars(
            venue=manifest.venue_a,
            symbol=manifest.symbol_a,
            start=manifest.window_start,
            end=manifest.window_end,
        )
        source_b = self.query_source_bars(
            venue=manifest.venue_b,
            symbol=manifest.symbol_b,
            start=manifest.window_start,
            end=manifest.window_end,
        )
        if (
            source_bars_sha256(source_a) != manifest.source_a_sha256
            or source_bars_sha256(source_b) != manifest.source_b_sha256
        ):
            raise ValueError("reference window source data changed")
        series = build_reference_series(
            source_a,
            source_b,
            window_start=manifest.window_start,
            window_end=manifest.window_end,
        )
        if (
            series.dataset_sha256 != manifest.dataset_sha256
            or reference_bars_sha256(series.bars) != manifest.reference_bars_sha256
            or tuple(
                (item.interval_start.isoformat(), item.reason.value) for item in series.rejections
            )
            != manifest.rejection_ledger
        ):
            raise ValueError("reference window reconstruction changed")
        return series

    def find_window_manifest(
        self,
        *,
        venue_a: Venue,
        venue_b: Venue,
        instrument: InstrumentKey,
        start: datetime,
        end: datetime,
    ) -> ReferenceWindowManifest:
        matches: list[ReferenceWindowManifest] = []
        for path in sorted((self.root / "windows").glob("*.json")):
            manifest = self.load_window_manifest(path.stem)
            if (
                manifest.venue_a == venue_a
                and manifest.venue_b == venue_b
                and manifest.instrument == instrument
                and manifest.window_start == start
                and manifest.window_end == end
            ):
                try:
                    self.verify_window_manifest(manifest)
                except ValueError:
                    # A resumed backfill changes the exact source dataset. Keep
                    # the old immutable receipt for audit, but it is no longer
                    # a candidate for the current complete window.
                    continue
                matches.append(manifest)
        if len(matches) != 1:
            raise ValueError("exactly one reference window manifest is required")
        return matches[0]

    def _window_manifest_path(self, dataset_sha256: str) -> Path:
        if len(dataset_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in dataset_sha256
        ):
            raise ValueError("reference dataset identity must be SHA-256")
        return self.root / "windows" / f"{dataset_sha256}.json"

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


def _tree_manifest_sha256(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*.parquet")):
        digest.update(path.relative_to(root).as_posix().encode("utf-8"))
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def _window_manifest_sha256(manifest: ReferenceWindowManifest) -> str:
    payload = asdict(manifest)
    payload.pop("manifest_sha256", None)
    encoded = json.dumps(
        payload,
        default=str,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


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
