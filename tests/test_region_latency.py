from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import time
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from typer.testing import CliRunner

from interexchange_perp_grid.cli import app
from interexchange_perp_grid.domain import (
    BookLevel,
    FundingSnapshot,
    Instrument,
    OrderBookSnapshot,
    Venue,
)
from interexchange_perp_grid.private_domain import (
    AccountSnapshot,
    PrivateStreamEvent,
    PrivateStreamKind,
)
from interexchange_perp_grid.qualification import code_hash, config_hash
from interexchange_perp_grid.region_latency import (
    MAXIMUM_INTER_SAMPLE_GAP_SECONDS,
    WAVE1_VENUES,
    LatencyChannel,
    LatencySample,
    RegionAttestation,
    RegionLatencyPolicy,
    attestation_sha256,
    bounded_operation,
    build_region_latency_report,
    collect_region_latency_samples,
    load_latency_samples,
    load_region_attestation,
    load_region_latency_report,
    local_host_fingerprint,
    select_deployment_region,
    verify_provider_evidence,
    write_latency_samples,
    write_region_latency_report,
)

SHA_A = "a" * 64
SHA_B = "b" * 64
GERMANY_HOST = "c" * 64
JAPAN_HOST = "d" * 64
ATTESTED_AT = datetime.now(UTC)
_TEST_ATTESTATION_KEY = Ed25519PrivateKey.generate()


def _provider_evidence(region: str) -> dict[str, str]:
    unsigned = {
        "provider": "aws",
        "provider_region": "eu-central-1" if region == "Germany" else "ap-northeast-1",
        "instance_id_sha256": ("1" if region == "Germany" else "4") * 64,
        "public_ip_sha256": ("2" if region == "Germany" else "5") * 64,
        "metadata_document": f"fixture-{region}",
    }
    signature = _TEST_ATTESTATION_KEY.sign(
        json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode()
    )
    return {
        **unsigned,
        "operator_signature_ed25519": base64.b64encode(signature).decode(),
    }


def _provider_evidence_bytes(region: str) -> bytes:
    return json.dumps(_provider_evidence(region), sort_keys=True).encode()


def _write_attestation_public_key(path: Path) -> None:
    public_raw = _TEST_ATTESTATION_KEY.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "algorithm": "Ed25519",
                "public_key_base64": base64.b64encode(public_raw).decode(),
            }
        ),
        encoding="utf-8",
    )


def _attestation_public_key_sha256() -> str:
    public_raw = _TEST_ATTESTATION_KEY.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    return hashlib.sha256(public_raw).hexdigest()


def _attestation(region: str, host: str) -> RegionAttestation:
    return RegionAttestation(
        1,
        region,
        host,
        "aws",
        "eu-central-1" if region == "Germany" else "ap-northeast-1",
        ("1" if region == "Germany" else "4") * 64,
        ("2" if region == "Germany" else "5") * 64,
        hashlib.sha256(_provider_evidence_bytes(region)).hexdigest(),
        ATTESTED_AT,
    )


GERMANY_ATTESTATION = _attestation("Germany", GERMANY_HOST)
JAPAN_ATTESTATION = _attestation("Japan", JAPAN_HOST)
GERMANY_ATTESTATION_SHA = attestation_sha256(GERMANY_ATTESTATION)
JAPAN_ATTESTATION_SHA = attestation_sha256(JAPAN_ATTESTATION)


def _samples(
    region: str,
    value: Decimal,
    *,
    counts: tuple[int, ...] | None = None,
    per_cell_values: tuple[Decimal, ...] | None = None,
    source_sha256: str = SHA_A,
    config_sha256: str = SHA_B,
    base: str = "BTC",
) -> tuple[LatencySample, ...]:
    started = (datetime.now(UTC) - timedelta(minutes=10)).replace(microsecond=0)
    host = GERMANY_HOST if region == "Germany" else JAPAN_HOST
    attestation = GERMANY_ATTESTATION_SHA if region == "Germany" else JAPAN_ATTESTATION_SHA
    cells = tuple((venue, channel) for venue in WAVE1_VENUES for channel in LatencyChannel)
    cell_counts = counts or (30,) * len(cells)
    values = per_cell_values or (value,) * len(cells)
    return tuple(
        _sample(
            region=region,
            host=host,
            attestation=attestation,
            source_sha256=source_sha256,
            config_sha256=config_sha256,
            base=base,
            venue=venue,
            channel=channel,
            sequence=sequence,
            latency_ms=values[cell_index],
            observed_at=started + timedelta(seconds=sequence),
        )
        for cell_index, (venue, channel) in enumerate(cells)
        for sequence in range(cell_counts[cell_index])
    )


def _sample(
    *,
    region: str,
    host: str,
    attestation: str,
    source_sha256: str,
    config_sha256: str,
    base: str,
    venue: Venue,
    channel: LatencyChannel,
    sequence: int,
    latency_ms: Decimal,
    observed_at: datetime,
) -> LatencySample:
    is_api = channel == LatencyChannel.PUBLIC_API
    started_ns = sequence * 1_000_000_000 + 1
    return LatencySample(
        region,
        host,
        attestation,
        source_sha256,
        config_sha256,
        base,
        "ACCOUNT_WIDE" if channel == LatencyChannel.PRIVATE_EVENT else "BTC/USDT:USDT",
        venue,
        channel,
        sequence,
        latency_ms,
        observed_at,
        None if is_api else int(Decimal(str(observed_at.timestamp() * 1000)) - latency_ms),
        None if is_api else 0,
        observed_at - timedelta(milliseconds=float(latency_ms)) if is_api else None,
        started_ns if is_api else None,
        started_ns + int(latency_ms * Decimal(1_000_000)) if is_api else None,
    )


def _shift_sample(
    sample: LatencySample, delta: timedelta, *, keep_source: bool = False
) -> LatencySample:
    return replace(
        sample,
        latency_ms=sample.latency_ms
        + (Decimal(str(delta.total_seconds() * 1000)) if keep_source else Decimal(0)),
        observed_at=sample.observed_at + delta,
        exchange_timestamp_ms=(
            sample.exchange_timestamp_ms
            if keep_source or sample.exchange_timestamp_ms is None
            else sample.exchange_timestamp_ms + int(delta.total_seconds() * 1000)
        ),
        request_started_at=(
            sample.request_started_at + delta
            if sample.request_started_at is not None and not keep_source
            else sample.request_started_at
        ),
        request_completed_monotonic_ns=(
            sample.request_completed_monotonic_ns + int(delta.total_seconds() * 1_000_000_000)
            if keep_source and sample.request_completed_monotonic_ns is not None
            else sample.request_completed_monotonic_ns
        ),
    )


def _policy() -> RegionLatencyPolicy:
    return RegionLatencyPolicy(
        "Germany", Decimal("0.20"), Decimal("0.50"), _attestation_public_key_sha256()
    )


def _instrument(venue: Venue) -> Instrument:
    return Instrument(
        venue,
        "BTC/USDT:USDT",
        "BTCUSDT",
        "BTC",
        "USDT",
        "USDT",
        Decimal("0.001"),
        Decimal(1),
        Decimal("0.1"),
        Decimal(1),
        Decimal(5),
        Decimal("0.0005"),
        "fixture",
        True,
        datetime(2020, 1, 1, tzinfo=UTC),
    )


def test_report_requires_complete_bounded_wave1_matrix_and_instrument_identity() -> None:
    samples = _samples("Germany", Decimal(100))
    report = build_region_latency_report(
        samples,
        expected_source_sha256=SHA_A,
        expected_config_sha256=SHA_B,
        maximum_clock_skew_ms=1_000,
    )

    assert report.schema_version == 2
    assert len(report.distributions) == 9
    assert all(item.sample_count == 30 for item in report.distributions)
    assert report.execution_authorized is False
    with pytest.raises(ValueError, match="insufficient samples"):
        build_region_latency_report(
            samples[:-1],
            expected_source_sha256=SHA_A,
            expected_config_sha256=SHA_B,
            maximum_clock_skew_ms=1_000,
        )
    mixed = list(samples)
    mixed[0] = replace(mixed[0], base="ETH", symbol="ETH/USDT:USDT")
    with pytest.raises(ValueError, match="mix instruments"):
        build_region_latency_report(
            tuple(mixed), expected_source_sha256=SHA_A, expected_config_sha256=SHA_B
        )
    gapped = tuple(
        _shift_sample(sample, timedelta(minutes=2)) if sample.sequence == 29 else sample
        for sample in samples
    )
    with pytest.raises(ValueError, match="sample gap exceeds"):
        build_region_latency_report(
            gapped, expected_source_sha256=SHA_A, expected_config_sha256=SHA_B
        )


def test_selection_rebuilds_raw_evidence_and_applies_weighted_p95_and_p99_guard() -> None:
    germany_samples = _samples("Germany", Decimal(100))
    japan_samples = _samples("Japan", Decimal(70))
    germany_report = build_region_latency_report(
        germany_samples, expected_source_sha256=SHA_A, expected_config_sha256=SHA_B
    )
    japan_report = build_region_latency_report(
        japan_samples, expected_source_sha256=SHA_A, expected_config_sha256=SHA_B
    )
    selected = select_deployment_region(
        germany_report,
        japan_report,
        _policy(),
        germany_samples=germany_samples,
        japan_samples=japan_samples,
        germany_attestation=GERMANY_ATTESTATION,
        japan_attestation=JAPAN_ATTESTATION,
        expected_source_sha256=SHA_A,
        expected_config_sha256=SHA_B,
        maximum_clock_skew_ms=1_000,
    )
    assert selected.selected_region == "Japan"
    assert selected.execution_authorized is False

    tampered = list(japan_samples)
    tampered[0] = _shift_sample(tampered[0], timedelta(milliseconds=1), keep_source=True)
    with pytest.raises(ValueError, match="raw samples"):
        select_deployment_region(
            germany_report,
            japan_report,
            _policy(),
            germany_samples=germany_samples,
            japan_samples=tuple(tampered),
            germany_attestation=GERMANY_ATTESTATION,
            japan_attestation=JAPAN_ATTESTATION,
            expected_source_sha256=SHA_A,
            expected_config_sha256=SHA_B,
            maximum_clock_skew_ms=1_000,
        )

    fast = list(japan_samples)
    spike_index = next(
        index
        for index, sample in enumerate(fast)
        if sample.venue == Venue.OKX
        and sample.channel == LatencyChannel.PRIVATE_EVENT
        and sample.sequence == 29
    )
    fast[spike_index] = _shift_sample(
        fast[spike_index], timedelta(milliseconds=90), keep_source=True
    )
    guarded = build_region_latency_report(
        tuple(fast), expected_source_sha256=SHA_A, expected_config_sha256=SHA_B
    )
    decision = select_deployment_region(
        germany_report,
        guarded,
        _policy(),
        germany_samples=germany_samples,
        japan_samples=tuple(fast),
        germany_attestation=GERMANY_ATTESTATION,
        japan_attestation=JAPAN_ATTESTATION,
        expected_source_sha256=SHA_A,
        expected_config_sha256=SHA_B,
        maximum_clock_skew_ms=1_000,
    )
    assert decision.selected_region == "Germany"
    assert decision.maximum_p99_worsening_ratio == Decimal("0.6")

    forged_policy_report = build_region_latency_report(
        germany_samples,
        expected_source_sha256=SHA_A,
        expected_config_sha256=SHA_B,
        maximum_clock_skew_ms=300_000,
    )
    with pytest.raises(ValueError, match="identity-equivalent"):
        select_deployment_region(
            forged_policy_report,
            build_region_latency_report(
                japan_samples,
                expected_source_sha256=SHA_A,
                expected_config_sha256=SHA_B,
                maximum_clock_skew_ms=300_000,
            ),
            _policy(),
            germany_samples=germany_samples,
            japan_samples=japan_samples,
            germany_attestation=GERMANY_ATTESTATION,
            japan_attestation=JAPAN_ATTESTATION,
            expected_source_sha256=SHA_A,
            expected_config_sha256=SHA_B,
            maximum_clock_skew_ms=1_000,
        )


@pytest.mark.asyncio
async def test_collector_uses_event_propagation_and_api_round_trip_not_market_quietness() -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    exchange_ms = int(now.timestamp() * 1000)

    class PublicProbe:
        async def watch_order_book(
            self, instrument: Instrument, limit: int = 50
        ) -> OrderBookSnapshot:
            del limit
            return OrderBookSnapshot(
                instrument.venue,
                instrument.symbol,
                (BookLevel(Decimal(99), Decimal(1)),),
                (BookLevel(Decimal(101), Decimal(1)),),
                exchange_ms,
                now + timedelta(milliseconds=7),
                1,
                1,
                1,
                True,
                True,
                2,
            )

        async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot:
            return FundingSnapshot(
                instrument.venue,
                instrument.symbol,
                Decimal(0),
                None,
                None,
                Decimal(1),
                Decimal(1),
                exchange_ms,
            )

    class PrivateProbe:
        def __init__(self, venue: Venue) -> None:
            self.venue = venue

        async def watch_account_wide_balance(self) -> PrivateStreamEvent:
            return PrivateStreamEvent(
                self.venue,
                PrivateStreamKind.ACCOUNT,
                1,
                now + timedelta(milliseconds=11),
                1,
                account=AccountSnapshot(
                    self.venue,
                    Decimal(100),
                    Decimal(100),
                    "cross",
                    "hedged",
                    True,
                    ("trade",),
                    now,
                ),
                exchange_timestamp_ms=exchange_ms,
            )

    samples = await collect_region_latency_samples(
        region="Germany",
        host_fingerprint=GERMANY_HOST,
        attestation_sha256=GERMANY_ATTESTATION_SHA,
        source_sha256=SHA_A,
        config_sha256=SHA_B,
        base="BTC",
        public_adapters={venue: PublicProbe() for venue in WAVE1_VENUES},
        private_adapters={venue: PrivateProbe(venue) for venue in WAVE1_VENUES},
        instruments={venue: _instrument(venue) for venue in WAVE1_VENUES},
    )

    assert len(samples) == 270
    assert {s.latency_ms for s in samples if s.channel == LatencyChannel.PUBLIC_FEED} == {
        Decimal(9)
    }
    assert {s.latency_ms for s in samples if s.channel == LatencyChannel.PRIVATE_EVENT} == {
        Decimal(13)
    }
    assert all(
        s.symbol == "BTC/USDT:USDT" for s in samples if s.channel != LatencyChannel.PRIVATE_EVENT
    )


@pytest.mark.asyncio
async def test_collector_rejects_empty_l2_and_malformed_private_event() -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    exchange_ms = int(now.timestamp() * 1000)

    class PublicProbe:
        def __init__(self, *, empty: bool) -> None:
            self.empty = empty

        async def watch_order_book(
            self, instrument: Instrument, limit: int = 50
        ) -> OrderBookSnapshot:
            del limit
            return OrderBookSnapshot(
                instrument.venue,
                instrument.symbol,
                () if self.empty else (BookLevel(Decimal(99), Decimal(1)),),
                () if self.empty else (BookLevel(Decimal(101), Decimal(1)),),
                exchange_ms,
                now + timedelta(milliseconds=1),
                1,
                1,
                1,
                True,
                not self.empty,
                0,
            )

        async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot:
            return FundingSnapshot(
                instrument.venue,
                instrument.symbol,
                Decimal(0),
                None,
                None,
                Decimal(1),
                Decimal(1),
                exchange_ms,
            )

    class PrivateProbe:
        def __init__(self, venue: Venue) -> None:
            self.venue = venue

        async def watch_account_wide_balance(self) -> PrivateStreamEvent:
            return PrivateStreamEvent(
                self.venue,
                PrivateStreamKind.ACCOUNT,
                1,
                now + timedelta(milliseconds=1),
                1,
                exchange_timestamp_ms=exchange_ms,
            )

    private_adapters = {venue: PrivateProbe(venue) for venue in WAVE1_VENUES}
    instruments = {venue: _instrument(venue) for venue in WAVE1_VENUES}
    with pytest.raises(ValueError, match="unqualified L2"):
        await collect_region_latency_samples(
            region="Germany",
            host_fingerprint=GERMANY_HOST,
            attestation_sha256=GERMANY_ATTESTATION_SHA,
            source_sha256=SHA_A,
            config_sha256=SHA_B,
            base="BTC",
            private_adapters=private_adapters,
            instruments=instruments,
            public_adapters={venue: PublicProbe(empty=True) for venue in WAVE1_VENUES},
        )
    with pytest.raises(ValueError, match="unqualified account"):
        await collect_region_latency_samples(
            region="Germany",
            host_fingerprint=GERMANY_HOST,
            attestation_sha256=GERMANY_ATTESTATION_SHA,
            source_sha256=SHA_A,
            config_sha256=SHA_B,
            base="BTC",
            private_adapters=private_adapters,
            instruments=instruments,
            public_adapters={venue: PublicProbe(empty=False) for venue in WAVE1_VENUES},
        )


@pytest.mark.asyncio
async def test_hard_operation_deadline_does_not_wait_for_cancellation_resistant_child() -> None:
    release = asyncio.Event()

    async def resistant() -> object:
        try:
            await asyncio.Future[None]()
        except asyncio.CancelledError:
            await release.wait()
            return object()
        raise AssertionError("unreachable")

    started = time.monotonic()
    with pytest.raises(TimeoutError, match="hard deadline"):
        await bounded_operation(resistant, 0.01)
    assert time.monotonic() - started < 0.1
    release.set()
    await asyncio.sleep(0)


def test_attestation_is_strict_current_and_host_bound(tmp_path: Path) -> None:
    evidence_path = tmp_path / "provider.json"
    public_key_path = tmp_path / "attestation-public-key.json"
    evidence_path.write_bytes(_provider_evidence_bytes("Germany"))
    _write_attestation_public_key(public_key_path)
    attestation = RegionAttestation(
        1,
        "Germany",
        local_host_fingerprint(),
        "aws",
        "eu-central-1",
        "1" * 64,
        "2" * 64,
        hashlib.sha256(evidence_path.read_bytes()).hexdigest(),
        datetime.now(UTC),
    )
    path = tmp_path / "attestation.json"
    path.write_text(
        json.dumps(
            attestation.__dict__
            if hasattr(attestation, "__dict__")
            else {
                "schema_version": 1,
                "region": "Germany",
                "host_fingerprint": attestation.host_fingerprint,
                "provider": attestation.provider,
                "provider_region": attestation.provider_region,
                "instance_id_sha256": attestation.instance_id_sha256,
                "public_ip_sha256": attestation.public_ip_sha256,
                "provider_evidence_sha256": attestation.provider_evidence_sha256,
                "observed_at": attestation.observed_at.isoformat(),
            }
        ),
        encoding="utf-8",
    )
    loaded = load_region_attestation(path, expected_region="Germany")
    verify_provider_evidence(
        loaded, evidence_path, public_key_path, _attestation_public_key_sha256()
    )
    assert loaded == attestation
    assert len(attestation_sha256(loaded)) == 64
    with pytest.raises(ValueError, match="provider region"):
        load_region_attestation(path, expected_region="Japan")
    forged_path = tmp_path / "forged.json"
    forged_path.write_text(
        json.dumps(
            asdict(
                replace(
                    attestation,
                    provider="evil",
                    provider_region="infrastructure-east",
                )
            ),
            default=str,
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="provider region"):
        load_region_attestation(forged_path, expected_region="Germany")


def test_sample_and_report_roundtrip_and_cli_requires_raw_selection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import interexchange_perp_grid.cli as cli_module

    monkeypatch.setattr(cli_module, "load_region_latency_policy", lambda _path: _policy())
    runner = CliRunner()
    current_source = code_hash(Path(".").resolve())
    current_config = config_hash(Path("config/defaults.yaml").resolve())
    sample_paths = (tmp_path / "germany.ndjson", tmp_path / "japan.ndjson")
    for region, value, path in zip(
        ("Germany", "Japan"), (Decimal(100), Decimal(70)), sample_paths, strict=True
    ):
        write_latency_samples(
            _samples(
                region,
                value,
                source_sha256=current_source,
                config_sha256=current_config,
            ),
            path,
        )
    reports = (tmp_path / "germany.json", tmp_path / "japan.json")
    attestation_paths = (tmp_path / "germany-attestation.json", tmp_path / "japan-attestation.json")
    provider_paths = (tmp_path / "germany-provider.json", tmp_path / "japan-provider.json")
    public_key_path = tmp_path / "attestation-public-key.json"
    _write_attestation_public_key(public_key_path)
    for attested, path in zip(
        (GERMANY_ATTESTATION, JAPAN_ATTESTATION), attestation_paths, strict=True
    ):
        path.write_text(json.dumps(asdict(attested), default=str), encoding="utf-8")
    for region, path in zip(("Germany", "Japan"), provider_paths, strict=True):
        path.write_bytes(_provider_evidence_bytes(region))
    for samples, output in zip(sample_paths, reports, strict=True):
        result = runner.invoke(
            app,
            [
                "region-latency-report",
                "--samples",
                str(samples),
                "--output",
                str(output),
                "--repo-root",
                ".",
                "--config",
                "config/defaults.yaml",
            ],
        )
        assert result.exit_code == 0, result.output
        assert load_region_latency_report(output).samples_sha256
    selected = runner.invoke(
        app,
        [
            "region-latency-select",
            "--germany",
            str(reports[0]),
            "--japan",
            str(reports[1]),
            "--germany-samples",
            str(sample_paths[0]),
            "--japan-samples",
            str(sample_paths[1]),
            "--germany-attestation",
            str(attestation_paths[0]),
            "--japan-attestation",
            str(attestation_paths[1]),
            "--germany-provider-evidence",
            str(provider_paths[0]),
            "--japan-provider-evidence",
            str(provider_paths[1]),
            "--attestation-public-key",
            str(public_key_path),
            "--repo-root",
            ".",
            "--config",
            "config/defaults.yaml",
        ],
    )
    assert selected.exit_code == 0, selected.output
    assert json.loads(selected.output)["selected_region"] == "Japan"

    report = load_region_latency_report(reports[0])
    roundtrip = tmp_path / "roundtrip.json"
    write_region_latency_report(report, roundtrip)
    assert load_region_latency_report(roundtrip) == report
    assert len(load_latency_samples(sample_paths[0])) == 270
    raw_payloads = sample_paths[0].read_text(encoding="utf-8").splitlines()
    forged_sample = json.loads(raw_payloads[0])
    forged_sample["latency_ms"] = "0"
    forged_samples_path = tmp_path / "forged.ndjson"
    forged_samples_path.write_text(
        json.dumps(forged_sample) + "\n" + "\n".join(raw_payloads[1:]) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="derived latency"):
        load_latency_samples(forged_samples_path)


def test_probe_validates_credentials_before_transport_and_closes_partial_construction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import interexchange_perp_grid.cli as cli_module

    monkeypatch.setattr(cli_module, "load_region_latency_policy", lambda _path: _policy())

    local_attestation = RegionAttestation(
        1,
        "Germany",
        local_host_fingerprint(),
        "aws",
        "eu-central-1",
        "1" * 64,
        "2" * 64,
        hashlib.sha256(_provider_evidence_bytes("Germany")).hexdigest(),
        datetime.now(UTC),
    )
    attestation_path = tmp_path / "attestation.json"
    attestation_path.write_text(
        json.dumps(asdict(local_attestation), default=str), encoding="utf-8"
    )
    provider_path = tmp_path / "provider.json"
    public_key_path = tmp_path / "attestation-public-key.json"
    provider_path.write_bytes(_provider_evidence_bytes("Germany"))
    _write_attestation_public_key(public_key_path)
    public_created: list[object] = []

    class Credentials:
        @classmethod
        def from_environment(cls, venue: Venue) -> object:
            if venue == Venue.BYBIT:
                raise RuntimeError("missing fixture credential")
            return object()

    class NeverPublic:
        def __init__(self, venue: Venue) -> None:
            public_created.append(venue)

    monkeypatch.setattr(cli_module, "PrivateCredentials", Credentials)
    monkeypatch.setattr(cli_module, "CcxtProAdapter", NeverPublic)
    result = CliRunner().invoke(
        app,
        [
            "region-latency-probe-worker",
            "--region",
            "Germany",
            "--attestation",
            str(attestation_path),
            "--output",
            str(tmp_path / "unused.ndjson"),
            "--provider-evidence",
            str(provider_path),
            "--attestation-public-key",
            str(public_key_path),
        ],
    )
    assert result.exit_code != 0
    assert public_created == []

    class GoodCredentials:
        @classmethod
        def from_environment(cls, venue: Venue) -> object:
            del venue
            return object()

    class PartialPublic:
        def __init__(self, venue: Venue) -> None:
            if venue == Venue.BYBIT:
                raise RuntimeError("partial construction")
            self.closed = False
            partial_public.append(self)

        async def close(self) -> None:
            self.closed = True

    class PartialPrivate:
        def __init__(self, venue: Venue, credentials: object) -> None:
            del venue, credentials
            self.closed = False
            partial_private.append(self)

        async def close(self) -> None:
            self.closed = True

    partial_public: list[PartialPublic] = []
    partial_private: list[PartialPrivate] = []
    monkeypatch.setattr(cli_module, "PrivateCredentials", GoodCredentials)
    monkeypatch.setattr(cli_module, "CcxtProAdapter", PartialPublic)
    monkeypatch.setattr(cli_module, "CcxtPrivateAdapter", PartialPrivate)
    result = CliRunner().invoke(
        app,
        [
            "region-latency-probe-worker",
            "--region",
            "Germany",
            "--attestation",
            str(attestation_path),
            "--output",
            str(tmp_path / "unused.ndjson"),
            "--provider-evidence",
            str(provider_path),
            "--attestation-public-key",
            str(public_key_path),
        ],
    )
    assert result.exit_code != 0
    assert all(adapter.closed for adapter in partial_public)
    assert all(adapter.closed for adapter in partial_private)


def test_probe_process_deadline_is_external_and_request_bounds_are_finite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import subprocess

    import interexchange_perp_grid.cli as cli_module

    monkeypatch.setattr(cli_module, "load_region_latency_policy", lambda _path: _policy())

    local_attestation = replace(
        GERMANY_ATTESTATION,
        host_fingerprint=local_host_fingerprint(),
    )
    attestation_path = tmp_path / "attestation.json"
    provider_path = tmp_path / "provider.json"
    public_key_path = tmp_path / "attestation-public-key.json"
    provider_path.write_bytes(_provider_evidence_bytes("Germany"))
    _write_attestation_public_key(public_key_path)
    attestation_path.write_text(
        json.dumps(asdict(local_attestation), default=str), encoding="utf-8"
    )

    def expire(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise subprocess.TimeoutExpired("fixture", 3600)

    monkeypatch.setattr(subprocess, "run", expire)
    result = CliRunner().invoke(
        app,
        [
            "region-latency-probe",
            "--region",
            "Germany",
            "--attestation",
            str(attestation_path),
            "--provider-evidence",
            str(provider_path),
            "--attestation-public-key",
            str(public_key_path),
            "--output",
            str(tmp_path / "unused.ndjson"),
        ],
    )
    assert result.exit_code != 0
    assert isinstance(result.exception, RuntimeError)
    for invalid in ("inf", "1000"):
        bounded = CliRunner().invoke(
            app,
            [
                "region-latency-probe",
                "--region",
                "Germany",
                "--attestation",
                str(attestation_path),
                "--provider-evidence",
                str(provider_path),
                "--attestation-public-key",
                str(public_key_path),
                "--output",
                str(tmp_path / "unused.ndjson"),
                "--timeout-seconds",
                invalid,
            ],
        )
        assert bounded.exit_code != 0


def test_report_rejects_inter_sample_gap_policy_boundary() -> None:
    samples = list(_samples("Germany", Decimal(10)))
    index = next(
        i
        for i, sample in enumerate(samples)
        if sample.venue == Venue.BINANCE_USDM
        and sample.channel == LatencyChannel.PUBLIC_FEED
        and sample.sequence == 1
    )
    samples[index] = _shift_sample(
        samples[index],
        timedelta(seconds=MAXIMUM_INTER_SAMPLE_GAP_SECONDS + 1),
    )
    with pytest.raises(ValueError):
        build_region_latency_report(
            tuple(samples), expected_source_sha256=SHA_A, expected_config_sha256=SHA_B
        )


def test_report_rejects_coordinated_exchange_timestamp_and_clock_skew_forgery() -> None:
    samples = list(_samples("Germany", Decimal(10)))
    index = next(
        i for i, sample in enumerate(samples) if sample.channel == LatencyChannel.PUBLIC_FEED
    )
    sample = samples[index]
    samples[index] = replace(
        sample,
        exchange_timestamp_ms=sample.exchange_timestamp_ms - 216_000
        if sample.exchange_timestamp_ms is not None
        else None,
        clock_skew_ms=-216_000,
    )
    with pytest.raises(ValueError, match="clock-skew"):
        build_region_latency_report(
            tuple(samples),
            expected_source_sha256=SHA_A,
            expected_config_sha256=SHA_B,
            maximum_clock_skew_ms=1_000,
        )


def test_api_latency_uses_monotonic_evidence_and_rejects_backward_wall_clock() -> None:
    sample = next(
        item
        for item in _samples("Germany", Decimal(10))
        if item.channel == LatencyChannel.PUBLIC_API
    )
    assert sample.latency_ms == Decimal(10)
    assert sample.request_started_at is not None
    request_started_at = sample.request_started_at
    assert replace(sample, observed_at=request_started_at).latency_ms == Decimal(10)
    with pytest.raises(ValueError, match="wall clock evidence moved backwards"):
        replace(sample, observed_at=request_started_at - timedelta(milliseconds=1))


def test_provider_evidence_requires_valid_operator_signature(tmp_path: Path) -> None:
    evidence = _provider_evidence("Germany")
    evidence["operator_signature_ed25519"] = base64.b64encode(b"x" * 64).decode()
    raw = json.dumps(evidence, sort_keys=True).encode()
    evidence_path = tmp_path / "forged-provider.json"
    evidence_path.write_bytes(raw)
    public_key_path = tmp_path / "attestation-public-key.json"
    _write_attestation_public_key(public_key_path)
    attestation = replace(
        GERMANY_ATTESTATION,
        provider_evidence_sha256=hashlib.sha256(raw).hexdigest(),
    )
    with pytest.raises(ValueError, match="signature is invalid"):
        verify_provider_evidence(
            attestation,
            evidence_path,
            public_key_path,
            _attestation_public_key_sha256(),
        )


def test_provider_evidence_rejects_unpinned_key_and_boolean_schema(tmp_path: Path) -> None:
    evidence_path = tmp_path / "provider.json"
    evidence_path.write_bytes(_provider_evidence_bytes("Germany"))
    public_key_path = tmp_path / "attestation-public-key.json"
    _write_attestation_public_key(public_key_path)
    with pytest.raises(ValueError, match="not pinned"):
        verify_provider_evidence(
            GERMANY_ATTESTATION,
            evidence_path,
            public_key_path,
            "f" * 64,
        )
    key_payload = json.loads(public_key_path.read_text(encoding="utf-8"))
    key_payload["schema_version"] = True
    public_key_path.write_text(json.dumps(key_payload), encoding="utf-8")
    with pytest.raises(ValueError, match="invalid region-attestation public key"):
        verify_provider_evidence(
            GERMANY_ATTESTATION,
            evidence_path,
            public_key_path,
            _attestation_public_key_sha256(),
        )


def test_report_cli_rejects_hashing_a_nonexecuting_checkout(tmp_path: Path) -> None:
    fake_root = tmp_path / "fake-repository"
    (fake_root / ".git").mkdir(parents=True)
    (fake_root / "src" / "interexchange_perp_grid").mkdir(parents=True)
    config = Path("config/defaults.yaml").resolve()
    samples = tmp_path / "samples.ndjson"
    write_latency_samples(_samples("Germany", Decimal(10)), samples)
    result = CliRunner().invoke(
        app,
        [
            "region-latency-report",
            "--samples",
            str(samples),
            "--output",
            str(tmp_path / "report.json"),
            "--repo-root",
            str(fake_root),
            "--config",
            str(config),
        ],
    )
    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)
    assert "current repository checkout" in str(result.exception)


def test_region_evidence_rejects_an_external_runtime_policy(tmp_path: Path) -> None:
    import interexchange_perp_grid.cli as cli_module

    external = tmp_path / "RUNTIME_POLICY.yaml"
    external.write_text(
        Path("config/RUNTIME_POLICY.yaml")
        .read_text(encoding="utf-8")
        .replace("0" * 64, _attestation_public_key_sha256()),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="current checkout locked runtime policy"):
        cli_module._load_locked_region_policy(Path("."), external)


@pytest.mark.parametrize("latency", [Decimal("NaN"), Decimal("Infinity"), Decimal(-1)])
def test_latency_samples_reject_nonfinite_or_negative_values(latency: Decimal) -> None:
    with pytest.raises(ValueError, match="finite and non-negative"):
        replace(_samples("Germany", Decimal(1))[0], latency_ms=latency)
