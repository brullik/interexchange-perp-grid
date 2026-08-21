from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import math
import platform
import re
import time
from collections.abc import Callable, Coroutine, Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from functools import partial
from itertools import pairwise
from pathlib import Path
from typing import Any, Protocol

import yaml
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from interexchange_perp_grid.domain import FundingSnapshot, Instrument, OrderBookSnapshot, Venue
from interexchange_perp_grid.private_domain import PrivateStreamEvent

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
SUPPORTED_REGIONS = ("Germany", "Japan")
WAVE1_VENUES = (Venue.BINANCE_USDM, Venue.BYBIT, Venue.OKX)
MINIMUM_SAMPLES_PER_CELL = 30
MAXIMUM_PROBE_DURATION_SECONDS = 3600
MAXIMUM_OPERATION_BUDGET_SECONDS = 3300
MAXIMUM_INTER_SAMPLE_GAP_SECONDS = 60
_RETIRED_REGION_LATENCY_TASKS: set[asyncio.Task[Any]] = set()
_QUALIFIED_PROVIDER_REGIONS = {
    "aws": {
        "Germany": {"eu-central-1"},
        "Japan": {"ap-northeast-1", "ap-northeast-3"},
    },
    "gcp": {
        "Germany": {"europe-west3"},
        "Japan": {"asia-northeast1", "asia-northeast2"},
    },
    "azure": {
        "Germany": {"germanywestcentral", "germanynorth"},
        "Japan": {"japaneast", "japanwest"},
    },
    "hetzner": {"Germany": {"fsn1", "nbg1"}, "Japan": set()},
}


class LatencyChannel(StrEnum):
    PUBLIC_FEED = "public_feed"
    PUBLIC_API = "public_api"
    PRIVATE_EVENT = "private_event"


class PublicLatencyProbe(Protocol):
    async def watch_order_book(
        self, instrument: Instrument, limit: int = 50
    ) -> OrderBookSnapshot: ...

    async def fetch_funding(self, instrument: Instrument) -> FundingSnapshot: ...


class PrivateLatencyProbe(Protocol):
    async def watch_account_wide_balance(self) -> PrivateStreamEvent: ...


@dataclass(frozen=True, slots=True)
class RegionLatencyPolicy:
    default_region: str
    japan_migration_p95_improvement_ratio: Decimal
    max_single_venue_p99_worsening_ratio: Decimal
    attestation_public_key_sha256: str

    def __post_init__(self) -> None:
        if self.default_region != "Germany":
            raise ValueError("locked default region must remain Germany")
        if self.japan_migration_p95_improvement_ratio != Decimal("0.20"):
            raise ValueError("locked Japan p95 improvement ratio must remain 0.20")
        if self.max_single_venue_p99_worsening_ratio != Decimal("0.50"):
            raise ValueError("locked single-venue p99 worsening ratio must remain 0.50")
        if not _SHA256.fullmatch(self.attestation_public_key_sha256):
            raise ValueError("locked attestation public-key fingerprint must be SHA-256")


@dataclass(frozen=True, slots=True)
class RegionAttestation:
    schema_version: int
    region: str
    host_fingerprint: str
    provider: str
    provider_region: str
    instance_id_sha256: str
    public_ip_sha256: str
    provider_evidence_sha256: str
    observed_at: datetime

    def __post_init__(self) -> None:
        if self.schema_version != 1 or self.region not in SUPPORTED_REGIONS:
            raise ValueError("unsupported region attestation")
        if not self.provider.strip() or not self.provider_region.strip():
            raise ValueError("cloud provider and provider region are required")
        if not all(
            _SHA256.fullmatch(value)
            for value in (
                self.host_fingerprint,
                self.instance_id_sha256,
                self.public_ip_sha256,
                self.provider_evidence_sha256,
            )
        ):
            raise ValueError("attestation identity fields must be lowercase SHA-256")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("attestation timestamp must be timezone-aware")


@dataclass(frozen=True, slots=True)
class LatencySample:
    region: str
    host_fingerprint: str
    attestation_sha256: str
    source_sha256: str
    config_sha256: str
    base: str
    symbol: str
    venue: Venue
    channel: LatencyChannel
    sequence: int
    latency_ms: Decimal
    observed_at: datetime
    exchange_timestamp_ms: int | None
    clock_skew_ms: int | None
    request_started_at: datetime | None
    request_started_monotonic_ns: int | None
    request_completed_monotonic_ns: int | None

    def __post_init__(self) -> None:
        if not isinstance(self.latency_ms, Decimal):
            raise ValueError("latency sample must use Decimal")
        if self.region not in SUPPORTED_REGIONS:
            raise ValueError("latency sample region must be Germany or Japan")
        if not all(
            _SHA256.fullmatch(value)
            for value in (
                self.host_fingerprint,
                self.attestation_sha256,
                self.source_sha256,
                self.config_sha256,
            )
        ):
            raise ValueError("sample identity fields must be lowercase SHA-256")
        if not self.base.strip() or not self.symbol.strip():
            raise ValueError("sample instrument identity is required")
        if self.venue not in WAVE1_VENUES:
            raise ValueError("region selection accepts Wave 1 venues only")
        if self.sequence < 0:
            raise ValueError("latency sample sequence cannot be negative")
        if not self.latency_ms.is_finite() or self.latency_ms < 0:
            raise ValueError("latency sample must be finite and non-negative")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("latency sample timestamp must be timezone-aware")
        event_channel = self.channel in {LatencyChannel.PUBLIC_FEED, LatencyChannel.PRIVATE_EVENT}
        if event_channel != (self.exchange_timestamp_ms is not None):
            raise ValueError("event channels require an exchange timestamp; API RTT must not")
        if self.exchange_timestamp_ms is not None and (
            isinstance(self.exchange_timestamp_ms, bool)
            or not isinstance(self.exchange_timestamp_ms, int)
            or self.exchange_timestamp_ms <= 0
        ):
            raise ValueError("exchange timestamp must be positive")
        if self.clock_skew_ms is not None and (
            isinstance(self.clock_skew_ms, bool) or not isinstance(self.clock_skew_ms, int)
        ):
            raise ValueError("clock skew must be an integer")
        if event_channel != (self.clock_skew_ms is not None):
            raise ValueError("event channels require clock skew; API RTT must not")
        if (self.channel == LatencyChannel.PUBLIC_API) != (self.request_started_at is not None):
            raise ValueError("API RTT requires a request start; event delivery must not")
        if self.request_started_at is not None:
            if (
                self.request_started_at.tzinfo is None
                or self.request_started_at.utcoffset() is None
            ):
                raise ValueError("API request start must be timezone-aware")
            if self.observed_at < self.request_started_at:
                raise ValueError("API wall clock evidence moved backwards")
        has_monotonic_rtt = (
            self.request_started_monotonic_ns is not None
            and self.request_completed_monotonic_ns is not None
        )
        if (self.channel == LatencyChannel.PUBLIC_API) != has_monotonic_rtt:
            raise ValueError("API RTT requires monotonic start/completion evidence")
        if has_monotonic_rtt and (
            isinstance(self.request_started_monotonic_ns, bool)
            or isinstance(self.request_completed_monotonic_ns, bool)
            or not isinstance(self.request_started_monotonic_ns, int)
            or not isinstance(self.request_completed_monotonic_ns, int)
            or self.request_started_monotonic_ns < 0
            or self.request_completed_monotonic_ns < self.request_started_monotonic_ns
        ):
            raise ValueError("API monotonic timestamps must be non-negative and ordered")
        if self.request_started_at is not None and (
            self.request_started_at.tzinfo is None
            or self.request_started_at.utcoffset() is None
            or self.request_started_at > self.observed_at
        ):
            raise ValueError("API request timestamps must be aware and increasing")
        if self.latency_ms != _recalculate_latency_ms(self):
            raise ValueError("derived latency does not match its primary timing evidence")


@dataclass(frozen=True, slots=True)
class LatencyDistribution:
    venue: Venue
    channel: LatencyChannel
    base: str
    symbol: str
    sample_count: int
    started_at: datetime
    completed_at: datetime
    p50_ms: Decimal
    p95_ms: Decimal
    p99_ms: Decimal

    def __post_init__(self) -> None:
        if not self.base.strip() or not self.symbol.strip():
            raise ValueError("distribution instrument identity is required")
        if self.sample_count < MINIMUM_SAMPLES_PER_CELL:
            raise ValueError("latency distribution is under-sampled")
        values = (self.p50_ms, self.p95_ms, self.p99_ms)
        if any(not value.is_finite() or value < 0 for value in values):
            raise ValueError("latency percentiles must be finite and non-negative")
        if not self.p50_ms <= self.p95_ms <= self.p99_ms:
            raise ValueError("latency percentiles must be monotonic")
        if (
            self.started_at.tzinfo is None
            or self.completed_at.tzinfo is None
            or self.completed_at <= self.started_at
        ):
            raise ValueError("distribution timestamps must be aware and increasing")


@dataclass(frozen=True, slots=True)
class RegionLatencyReport:
    schema_version: int
    region: str
    host_fingerprint: str
    attestation_sha256: str
    source_sha256: str
    config_sha256: str
    samples_sha256: str
    started_at: datetime
    completed_at: datetime
    minimum_samples_per_cell: int
    maximum_probe_duration_seconds: int
    maximum_inter_sample_gap_seconds: int
    maximum_clock_skew_ms: int
    distributions: tuple[LatencyDistribution, ...]
    execution_authorized: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        if self.schema_version != 2 or self.region not in SUPPORTED_REGIONS:
            raise ValueError("unsupported region latency report")
        if not all(
            _SHA256.fullmatch(value)
            for value in (
                self.host_fingerprint,
                self.attestation_sha256,
                self.source_sha256,
                self.config_sha256,
                self.samples_sha256,
            )
        ):
            raise ValueError("report identity fields must be lowercase SHA-256")
        if self.minimum_samples_per_cell < MINIMUM_SAMPLES_PER_CELL:
            raise ValueError("region report requires at least 30 samples per cell")
        if self.maximum_probe_duration_seconds != MAXIMUM_PROBE_DURATION_SECONDS:
            raise ValueError("report probe duration policy is not locked")
        if self.maximum_inter_sample_gap_seconds != MAXIMUM_INTER_SAMPLE_GAP_SECONDS:
            raise ValueError("report sample gap policy is not locked")
        if self.maximum_clock_skew_ms <= 0:
            raise ValueError("report maximum clock skew must be positive")
        if (
            self.started_at.tzinfo is None
            or self.completed_at.tzinfo is None
            or self.completed_at <= self.started_at
            or self.completed_at - self.started_at
            > timedelta(seconds=self.maximum_probe_duration_seconds)
        ):
            raise ValueError("report timestamps exceed the locked probe window")
        keys = tuple((item.venue, item.channel) for item in self.distributions)
        expected = tuple((venue, channel) for venue in WAVE1_VENUES for channel in LatencyChannel)
        if keys != expected:
            raise ValueError("report must contain the exact ordered Wave 1 channel matrix")
        if any(item.sample_count < self.minimum_samples_per_cell for item in self.distributions):
            raise ValueError("report contains an under-sampled venue/channel cell")


@dataclass(frozen=True, slots=True)
class RegionSelection:
    selected_region: str
    reason: str
    germany_weighted_p95_ms: Decimal
    japan_weighted_p95_ms: Decimal
    japan_p95_improvement_ratio: Decimal
    maximum_p99_worsening_ratio: Decimal
    source_sha256: str
    config_sha256: str
    attestation_public_key_sha256: str
    germany_report_sha256: str
    japan_report_sha256: str
    execution_authorized: bool = field(default=False, init=False)


def local_host_fingerprint() -> str:
    machine_id_path = Path("/etc/machine-id")
    machine_id = (
        machine_id_path.read_text(encoding="utf-8").strip()
        if machine_id_path.is_file()
        else platform.node()
    )
    return hashlib.sha256(
        f"{platform.system()}:{platform.machine()}:{machine_id}".encode()
    ).hexdigest()


def load_region_attestation(
    path: Path, *, expected_region: str, bind_local_host: bool = True
) -> RegionAttestation:
    payload = json.loads(path.read_text(encoding="utf-8"))
    keys = {
        "schema_version",
        "region",
        "host_fingerprint",
        "provider",
        "provider_region",
        "instance_id_sha256",
        "public_ip_sha256",
        "provider_evidence_sha256",
        "observed_at",
    }
    if not isinstance(payload, dict) or set(payload) != keys:
        raise ValueError("invalid cloud region attestation schema")
    if isinstance(payload["schema_version"], bool) or not isinstance(
        payload["schema_version"], int
    ):
        raise ValueError("attestation schema version must be an integer")
    if not all(isinstance(payload[key], str) for key in keys - {"schema_version"}):
        raise ValueError("attestation fields must not be coerced")
    result = RegionAttestation(
        payload["schema_version"],
        payload["region"],
        payload["host_fingerprint"],
        payload["provider"],
        payload["provider_region"],
        payload["instance_id_sha256"],
        payload["public_ip_sha256"],
        payload["provider_evidence_sha256"],
        datetime.fromisoformat(payload["observed_at"]),
    )
    provider = result.provider.strip().lower()
    provider_region = result.provider_region.strip().lower()
    if (
        result.region != expected_region
        or provider not in _QUALIFIED_PROVIDER_REGIONS
        or provider_region not in _QUALIFIED_PROVIDER_REGIONS[provider][expected_region]
    ):
        raise ValueError("attestation provider region does not match the requested region")
    if bind_local_host and result.host_fingerprint != local_host_fingerprint():
        raise ValueError("attestation does not bind this host")
    current = datetime.now(UTC)
    if result.observed_at.astimezone(UTC) > current + timedelta(minutes=1):
        raise ValueError("cloud region attestation is from the future")
    if current - result.observed_at.astimezone(UTC) > timedelta(hours=24):
        raise ValueError("cloud region attestation is stale")
    return result


def attestation_sha256(attestation: RegionAttestation) -> str:
    return hashlib.sha256(
        json.dumps(asdict(attestation), default=str, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def verify_provider_evidence(
    attestation: RegionAttestation,
    path: Path,
    public_key_path: Path,
    expected_public_key_sha256: str,
) -> None:
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != attestation.provider_evidence_sha256:
        raise ValueError("cloud provider evidence hash does not match the attestation")
    payload = json.loads(raw)
    keys = {
        "provider",
        "provider_region",
        "instance_id_sha256",
        "public_ip_sha256",
        "metadata_document",
        "operator_signature_ed25519",
    }
    if (
        not isinstance(payload, dict)
        or set(payload) != keys
        or not all(isinstance(payload[key], str) for key in keys)
        or not payload["metadata_document"].strip()
        or (
            payload["provider"].lower(),
            payload["provider_region"].lower(),
            payload["instance_id_sha256"],
            payload["public_ip_sha256"],
        )
        != (
            attestation.provider.lower(),
            attestation.provider_region.lower(),
            attestation.instance_id_sha256,
            attestation.public_ip_sha256,
        )
    ):
        raise ValueError("cloud provider evidence does not match the attested deployment")
    key_payload = json.loads(public_key_path.read_text(encoding="utf-8"))
    if (
        not isinstance(key_payload, dict)
        or set(key_payload) != {"schema_version", "algorithm", "public_key_base64"}
        or isinstance(key_payload.get("schema_version"), bool)
        or not isinstance(key_payload.get("schema_version"), int)
        or key_payload.get("schema_version") != 1
        or key_payload.get("algorithm") != "Ed25519"
        or not isinstance(key_payload.get("public_key_base64"), str)
    ):
        raise ValueError("invalid region-attestation public key")
    try:
        public_key_raw = base64.b64decode(key_payload["public_key_base64"], validate=True)
        public_key = Ed25519PublicKey.from_public_bytes(public_key_raw)
    except (ValueError, TypeError, binascii.Error) as error:
        raise ValueError("invalid region-attestation public key") from error
    if hashlib.sha256(public_key_raw).hexdigest() != expected_public_key_sha256:
        raise ValueError("region-attestation public key is not pinned by locked policy")
    try:
        signature = base64.b64decode(payload["operator_signature_ed25519"], validate=True)
        signed_payload = {key: payload[key] for key in keys if key != "operator_signature_ed25519"}
        public_key.verify(
            signature,
            json.dumps(signed_payload, sort_keys=True, separators=(",", ":")).encode(),
        )
    except (ValueError, TypeError, binascii.Error, InvalidSignature) as error:
        raise ValueError("cloud provider evidence signature is invalid") from error


def load_region_latency_policy(path: Path) -> RegionLatencyPolicy:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or not isinstance(raw.get("deployment"), dict):
        raise ValueError("runtime policy deployment section is required")
    deployment = raw["deployment"]
    return RegionLatencyPolicy(
        str(deployment.get("default_region")),
        Decimal(str(deployment.get("japan_migration_p95_improvement_ratio"))),
        Decimal(str(deployment.get("max_single_venue_p99_worsening_ratio"))),
        str(deployment.get("attestation_public_key_sha256")),
    )


async def bounded_operation[T](
    operation: Callable[[], Coroutine[Any, Any, T]], timeout_seconds: float
) -> T:
    task: asyncio.Task[T] = asyncio.create_task(operation(), name="region-latency-operation")
    try:
        done, _ = await asyncio.wait((task,), timeout=timeout_seconds)
    except asyncio.CancelledError:
        task.cancel()
        _retire_region_latency_task(task)
        raise
    if task not in done:
        task.cancel()
        _retire_region_latency_task(task)
        raise TimeoutError("region latency operation exceeded its hard deadline")
    return task.result()


def _retire_region_latency_task(task: asyncio.Task[Any]) -> None:
    _RETIRED_REGION_LATENCY_TASKS.add(task)
    task.add_done_callback(_consume_task)


def _consume_task(task: asyncio.Task[Any]) -> None:
    _RETIRED_REGION_LATENCY_TASKS.discard(task)
    if task.done() and not task.cancelled():
        task.exception()


def _event_delivery_latency_ms(
    *, received_at: datetime, exchange_timestamp_ms: int | None, clock_skew_ms: int | None
) -> Decimal:
    if exchange_timestamp_ms is None or exchange_timestamp_ms <= 0 or clock_skew_ms is None:
        raise ValueError("event propagation latency requires exchange timestamp and clock skew")
    received_ms = Decimal(str(received_at.astimezone(UTC).timestamp() * 1000))
    latency = received_ms - Decimal(exchange_timestamp_ms) + Decimal(clock_skew_ms)
    if not latency.is_finite() or latency < 0:
        raise ValueError("event propagation latency is negative or non-finite")
    return latency


def _api_round_trip_latency_ms(started_ns: int, completed_ns: int) -> Decimal:
    return Decimal(completed_ns - started_ns) / Decimal(1_000_000)


def _recalculate_latency_ms(sample: LatencySample) -> Decimal:
    if sample.channel == LatencyChannel.PUBLIC_API:
        if (
            sample.request_started_monotonic_ns is None
            or sample.request_completed_monotonic_ns is None
        ):
            raise ValueError("API RTT is missing monotonic evidence")
        return _api_round_trip_latency_ms(
            sample.request_started_monotonic_ns,
            sample.request_completed_monotonic_ns,
        )
    return _event_delivery_latency_ms(
        received_at=sample.observed_at,
        exchange_timestamp_ms=sample.exchange_timestamp_ms,
        clock_skew_ms=sample.clock_skew_ms,
    )


async def collect_region_latency_samples(
    *,
    region: str,
    host_fingerprint: str,
    attestation_sha256: str,
    source_sha256: str,
    config_sha256: str,
    base: str,
    public_adapters: Mapping[Venue, PublicLatencyProbe],
    private_adapters: Mapping[Venue, PrivateLatencyProbe],
    instruments: Mapping[Venue, Instrument],
    samples_per_cell: int = MINIMUM_SAMPLES_PER_CELL,
    timeout_seconds: float = 5,
) -> tuple[LatencySample, ...]:
    validate_region_probe_request(region, base, samples_per_cell, timeout_seconds)
    required = set(WAVE1_VENUES)
    if (
        set(public_adapters) != required
        or set(private_adapters) != required
        or set(instruments) != required
    ):
        raise ValueError("region probe requires the exact Wave 1 adapter/instrument set")
    samples: list[LatencySample] = []
    for sequence in range(samples_per_cell):
        for venue in WAVE1_VENUES:
            public, private, instrument = (
                public_adapters[venue],
                private_adapters[venue],
                instruments[venue],
            )
            book = await bounded_operation(
                partial(public.watch_order_book, instrument),
                timeout_seconds,
            )
            if (
                book.venue != venue
                or book.symbol != instrument.symbol
                or not book.synchronised
                or not book.bids
                or not book.asks
                or book.sequence_start is None
                or book.sequence_end is None
                or book.sequence_end < book.sequence_start
                or not book.sequence_contiguous
            ):
                raise ValueError("public feed returned unqualified L2 evidence")
            samples.append(
                LatencySample(
                    region,
                    host_fingerprint,
                    attestation_sha256,
                    source_sha256,
                    config_sha256,
                    base.upper(),
                    instrument.symbol,
                    venue,
                    LatencyChannel.PUBLIC_FEED,
                    sequence,
                    _event_delivery_latency_ms(
                        received_at=book.received_at,
                        exchange_timestamp_ms=book.exchange_timestamp_ms,
                        clock_skew_ms=book.clock_skew_ms,
                    ),
                    book.received_at,
                    book.exchange_timestamp_ms,
                    book.clock_skew_ms,
                    None,
                    None,
                    None,
                )
            )
            request_started_at = datetime.now(UTC)
            request_started_ns = time.monotonic_ns()
            funding = await bounded_operation(
                partial(public.fetch_funding, instrument),
                timeout_seconds,
            )
            if funding.venue != venue or funding.symbol != instrument.symbol:
                raise ValueError("public API returned the wrong instrument identity")
            request_completed_at = datetime.now(UTC)
            if request_completed_at < request_started_at:
                raise ValueError("wall clock moved backwards during API latency measurement")
            request_completed_ns = time.monotonic_ns()
            samples.append(
                LatencySample(
                    region,
                    host_fingerprint,
                    attestation_sha256,
                    source_sha256,
                    config_sha256,
                    base.upper(),
                    instrument.symbol,
                    venue,
                    LatencyChannel.PUBLIC_API,
                    sequence,
                    _api_round_trip_latency_ms(request_started_ns, request_completed_ns),
                    request_completed_at,
                    None,
                    None,
                    request_started_at,
                    request_started_ns,
                    request_completed_ns,
                )
            )
            event = await bounded_operation(private.watch_account_wide_balance, timeout_seconds)
            if (
                event.venue != venue
                or not event.account_wide
                or event.kind.value != "ACCOUNT"
                or event.account is None
                or event.unknown_active_records
            ):
                raise ValueError("private stream returned unqualified account evidence")
            samples.append(
                LatencySample(
                    region,
                    host_fingerprint,
                    attestation_sha256,
                    source_sha256,
                    config_sha256,
                    base.upper(),
                    "ACCOUNT_WIDE",
                    venue,
                    LatencyChannel.PRIVATE_EVENT,
                    sequence,
                    _event_delivery_latency_ms(
                        received_at=event.observed_at,
                        exchange_timestamp_ms=event.exchange_timestamp_ms,
                        clock_skew_ms=book.clock_skew_ms,
                    ),
                    event.observed_at,
                    event.exchange_timestamp_ms,
                    book.clock_skew_ms,
                    None,
                    None,
                    None,
                )
            )
    return tuple(samples)


def validate_region_probe_request(
    region: str, base: str, samples_per_cell: int, timeout_seconds: float
) -> None:
    if region not in SUPPORTED_REGIONS or not base.strip():
        raise ValueError("region probe identity is invalid")
    if (
        isinstance(samples_per_cell, bool)
        or samples_per_cell < MINIMUM_SAMPLES_PER_CELL
        or not math.isfinite(timeout_seconds)
        or timeout_seconds <= 0
        or samples_per_cell * len(WAVE1_VENUES) * len(LatencyChannel) * timeout_seconds
        > MAXIMUM_OPERATION_BUDGET_SECONDS
    ):
        raise ValueError("region probe sample count/timeout is invalid")


def write_latency_samples(samples: tuple[LatencySample, ...], path: Path) -> None:
    if not samples:
        raise ValueError("cannot write an empty latency sample set")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        "".join(
            json.dumps(asdict(sample), default=str, sort_keys=True) + "\n" for sample in samples
        ),
        encoding="utf-8",
    )
    temporary.replace(path)


def load_latency_samples(path: Path) -> tuple[LatencySample, ...]:
    samples: list[LatencySample] = []
    keys = {
        "region",
        "host_fingerprint",
        "attestation_sha256",
        "source_sha256",
        "config_sha256",
        "base",
        "symbol",
        "venue",
        "channel",
        "sequence",
        "latency_ms",
        "observed_at",
        "exchange_timestamp_ms",
        "clock_skew_ms",
        "request_started_at",
        "request_started_monotonic_ns",
        "request_completed_monotonic_ns",
    }
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw_line.strip():
            continue
        try:
            payload = json.loads(raw_line)
            if not isinstance(payload, dict) or set(payload) != keys:
                raise ValueError("sample must contain the exact latency schema")
            sequence, exchange_timestamp_ms = payload["sequence"], payload["exchange_timestamp_ms"]
            clock_skew_ms = payload["clock_skew_ms"]
            request_started_ns = payload["request_started_monotonic_ns"]
            request_completed_ns = payload["request_completed_monotonic_ns"]
            if isinstance(sequence, bool) or not isinstance(sequence, int):
                raise ValueError("sample sequence must be an integer")
            if exchange_timestamp_ms is not None and (
                isinstance(exchange_timestamp_ms, bool)
                or not isinstance(exchange_timestamp_ms, int)
            ):
                raise ValueError("exchange timestamp must be an integer or null")
            if clock_skew_ms is not None and (
                isinstance(clock_skew_ms, bool) or not isinstance(clock_skew_ms, int)
            ):
                raise ValueError("clock skew must be an integer or null")
            for name, value in (
                ("request_started_monotonic_ns", request_started_ns),
                ("request_completed_monotonic_ns", request_completed_ns),
            ):
                if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
                    raise ValueError(f"{name} must be an integer or null")
            if not all(
                isinstance(payload[key], str)
                for key in keys
                - {
                    "sequence",
                    "exchange_timestamp_ms",
                    "clock_skew_ms",
                    "request_started_at",
                    "request_started_monotonic_ns",
                    "request_completed_monotonic_ns",
                }
            ):
                raise ValueError("sample string fields must not be coerced")
            if payload["request_started_at"] is not None and not isinstance(
                payload["request_started_at"], str
            ):
                raise ValueError("request start must be an ISO string or null")
            samples.append(
                LatencySample(
                    payload["region"],
                    payload["host_fingerprint"],
                    payload["attestation_sha256"],
                    payload["source_sha256"],
                    payload["config_sha256"],
                    payload["base"],
                    payload["symbol"],
                    Venue(payload["venue"]),
                    LatencyChannel(payload["channel"]),
                    sequence,
                    Decimal(payload["latency_ms"]),
                    datetime.fromisoformat(payload["observed_at"]),
                    exchange_timestamp_ms,
                    clock_skew_ms,
                    datetime.fromisoformat(payload["request_started_at"])
                    if payload["request_started_at"] is not None
                    else None,
                    request_started_ns,
                    request_completed_ns,
                )
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ValueError(f"invalid latency sample on line {line_number}: {error}") from error
    if not samples:
        raise ValueError("latency sample file is empty")
    return tuple(samples)


def _nearest_rank(values: tuple[Decimal, ...], percentile: Decimal) -> Decimal:
    return values[max(0, math.ceil(len(values) * float(percentile)) - 1)]


def _samples_sha256(samples: tuple[LatencySample, ...]) -> str:
    canonical = tuple(
        sorted(json.dumps(asdict(sample), default=str, sort_keys=True) for sample in samples)
    )
    return hashlib.sha256(json.dumps(canonical, separators=(",", ":")).encode()).hexdigest()


def build_region_latency_report(
    samples: tuple[LatencySample, ...],
    *,
    expected_source_sha256: str,
    expected_config_sha256: str,
    maximum_clock_skew_ms: int = 1_000,
    minimum_samples_per_cell: int = MINIMUM_SAMPLES_PER_CELL,
) -> RegionLatencyReport:
    if minimum_samples_per_cell < MINIMUM_SAMPLES_PER_CELL or not samples:
        raise ValueError("region report requires at least 30 samples per cell")
    identities = {
        (s.region, s.host_fingerprint, s.attestation_sha256, s.source_sha256, s.config_sha256)
        for s in samples
    }
    if len(identities) != 1:
        raise ValueError("one report cannot mix region/host/attestation/code/config identities")
    region, host, attestation, source, config = next(iter(identities))
    if source != expected_source_sha256 or config != expected_config_sha256:
        raise ValueError("sample identity does not match the current code/config")
    if maximum_clock_skew_ms <= 0 or any(
        sample.clock_skew_ms is not None and abs(sample.clock_skew_ms) > maximum_clock_skew_ms
        for sample in samples
    ):
        raise ValueError("raw samples exceed the current clock-skew policy")
    if any(sample.latency_ms != _recalculate_latency_ms(sample) for sample in samples):
        raise ValueError("raw samples contain inconsistent derived latency")
    distributions: list[LatencyDistribution] = []
    for venue in WAVE1_VENUES:
        for channel in LatencyChannel:
            cell = tuple(
                sorted(
                    (s for s in samples if s.venue == venue and s.channel == channel),
                    key=lambda s: s.sequence,
                )
            )
            if len(cell) < minimum_samples_per_cell:
                raise ValueError(f"insufficient samples for {venue.value}/{channel.value}")
            if tuple(s.sequence for s in cell) != tuple(range(len(cell))):
                raise ValueError(f"non-contiguous sequence for {venue.value}/{channel.value}")
            cell_identity = {(s.base, s.symbol) for s in cell}
            if len(cell_identity) != 1:
                raise ValueError("one latency cell cannot mix instruments")
            observed = tuple(s.observed_at.astimezone(UTC) for s in cell)
            if any(later <= earlier for earlier, later in pairwise(observed)):
                raise ValueError(f"non-increasing timestamp for {venue.value}/{channel.value}")
            if any(
                later - earlier > timedelta(seconds=MAXIMUM_INTER_SAMPLE_GAP_SECONDS)
                for earlier, later in pairwise(observed)
            ):
                raise ValueError(f"sample gap exceeds policy for {venue.value}/{channel.value}")
            exchange_timestamps = tuple(
                s.exchange_timestamp_ms for s in cell if s.exchange_timestamp_ms is not None
            )
            if channel != LatencyChannel.PUBLIC_API and (
                len(exchange_timestamps) != len(cell)
                or any(later <= earlier for earlier, later in pairwise(exchange_timestamps))
            ):
                raise ValueError(
                    f"event timestamps are not fresh for {venue.value}/{channel.value}"
                )
            values = tuple(sorted(s.latency_ms for s in cell))
            base, symbol = next(iter(cell_identity))
            distributions.append(
                LatencyDistribution(
                    venue,
                    channel,
                    base,
                    symbol,
                    len(values),
                    observed[0],
                    observed[-1],
                    _nearest_rank(values, Decimal("0.50")),
                    _nearest_rank(values, Decimal("0.95")),
                    _nearest_rank(values, Decimal("0.99")),
                )
            )
    return RegionLatencyReport(
        2,
        region,
        host,
        attestation,
        source,
        config,
        _samples_sha256(samples),
        min(d.started_at for d in distributions),
        max(d.completed_at for d in distributions),
        minimum_samples_per_cell,
        MAXIMUM_PROBE_DURATION_SECONDS,
        MAXIMUM_INTER_SAMPLE_GAP_SECONDS,
        maximum_clock_skew_ms,
        tuple(distributions),
    )


def _report_sha256(report: RegionLatencyReport) -> str:
    return hashlib.sha256(
        json.dumps(asdict(report), default=str, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def verify_report_samples(
    report: RegionLatencyReport,
    samples: tuple[LatencySample, ...],
    *,
    expected_source_sha256: str,
    expected_config_sha256: str,
) -> None:
    rebuilt = build_region_latency_report(
        samples,
        expected_source_sha256=expected_source_sha256,
        expected_config_sha256=expected_config_sha256,
        minimum_samples_per_cell=report.minimum_samples_per_cell,
        maximum_clock_skew_ms=report.maximum_clock_skew_ms,
    )
    if rebuilt != report:
        raise ValueError("region report does not exactly match its raw samples")


def select_deployment_region(
    germany: RegionLatencyReport,
    japan: RegionLatencyReport,
    policy: RegionLatencyPolicy,
    *,
    germany_samples: tuple[LatencySample, ...],
    japan_samples: tuple[LatencySample, ...],
    germany_attestation: RegionAttestation,
    japan_attestation: RegionAttestation,
    expected_source_sha256: str,
    expected_config_sha256: str,
    maximum_clock_skew_ms: int,
    now: datetime | None = None,
) -> RegionSelection:
    verify_report_samples(
        germany,
        germany_samples,
        expected_source_sha256=expected_source_sha256,
        expected_config_sha256=expected_config_sha256,
    )
    verify_report_samples(
        japan,
        japan_samples,
        expected_source_sha256=expected_source_sha256,
        expected_config_sha256=expected_config_sha256,
    )
    if (
        germany_attestation.region != "Germany"
        or japan_attestation.region != "Japan"
        or germany.attestation_sha256 != attestation_sha256(germany_attestation)
        or japan.attestation_sha256 != attestation_sha256(japan_attestation)
        or germany.host_fingerprint != germany_attestation.host_fingerprint
        or japan.host_fingerprint != japan_attestation.host_fingerprint
    ):
        raise ValueError("reports do not match their cloud region attestations")
    current = (now or datetime.now(UTC)).astimezone(UTC)
    for expected_region, attestation in (
        ("Germany", germany_attestation),
        ("Japan", japan_attestation),
    ):
        provider = attestation.provider.strip().lower()
        if (
            provider not in _QUALIFIED_PROVIDER_REGIONS
            or attestation.provider_region.strip().lower()
            not in _QUALIFIED_PROVIDER_REGIONS[provider][expected_region]
            or attestation.observed_at.astimezone(UTC) > current + timedelta(minutes=1)
            or current - attestation.observed_at.astimezone(UTC) > timedelta(hours=24)
        ):
            raise ValueError("region selection received an invalid cloud attestation")
    if germany.region != "Germany" or japan.region != "Japan":
        raise ValueError("region comparison requires Germany and Japan reports")
    if (
        germany.host_fingerprint == japan.host_fingerprint
        or germany.attestation_sha256 == japan.attestation_sha256
    ):
        raise ValueError("Germany and Japan measurements require distinct attested hosts")
    for report in (germany, japan):
        if report.completed_at.astimezone(UTC) > current + timedelta(
            minutes=1
        ) or current - report.completed_at.astimezone(UTC) > timedelta(hours=24):
            raise ValueError("region reports must be current and not from the future")
    if (
        germany.source_sha256 != japan.source_sha256
        or germany.config_sha256 != japan.config_sha256
        or germany.source_sha256 != expected_source_sha256
        or germany.config_sha256 != expected_config_sha256
        or germany.minimum_samples_per_cell != japan.minimum_samples_per_cell
        or germany.maximum_clock_skew_ms != japan.maximum_clock_skew_ms
        or germany.maximum_clock_skew_ms != maximum_clock_skew_ms
        or tuple(d.sample_count for d in germany.distributions)
        != tuple(d.sample_count for d in japan.distributions)
    ):
        raise ValueError("region reports are not current identity-equivalent evidence")
    for german, japanese in zip(germany.distributions, japan.distributions, strict=True):
        if (german.venue, german.channel, german.base, german.symbol) != (
            japanese.venue,
            japanese.channel,
            japanese.base,
            japanese.symbol,
        ):
            raise ValueError("region reports measured different instruments or channels")
        if max(german.started_at, japanese.started_at) > min(
            german.completed_at, japanese.completed_at
        ):
            raise ValueError("corresponding region measurement windows must overlap")
        ratio = Decimal(
            str((japanese.completed_at - japanese.started_at).total_seconds())
        ) / Decimal(str((german.completed_at - german.started_at).total_seconds()))
        if not Decimal("0.5") <= ratio <= Decimal(2):
            raise ValueError("corresponding measurement durations are not comparable")
    germany_weight = sum(d.sample_count for d in germany.distributions)
    japan_weight = sum(d.sample_count for d in japan.distributions)
    germany_p95 = sum(
        (d.p95_ms * d.sample_count for d in germany.distributions), Decimal(0)
    ) / Decimal(germany_weight)
    japan_p95 = sum((d.p95_ms * d.sample_count for d in japan.distributions), Decimal(0)) / Decimal(
        japan_weight
    )
    improvement = Decimal(0) if germany_p95 == 0 else (germany_p95 - japan_p95) / germany_p95
    worsening = max(
        Decimal(0)
        if g.p99_ms == 0 and j.p99_ms == 0
        else (Decimal("Infinity") if g.p99_ms == 0 else (j.p99_ms - g.p99_ms) / g.p99_ms)
        for g, j in zip(germany.distributions, japan.distributions, strict=True)
    )
    japan_allowed = (
        improvement >= policy.japan_migration_p95_improvement_ratio
        and worsening <= policy.max_single_venue_p99_worsening_ratio
    )
    return RegionSelection(
        "Japan" if japan_allowed else policy.default_region,
        "JAPAN_MEETS_LOCKED_LATENCY_POLICY" if japan_allowed else "GERMANY_DEFAULT_POLICY",
        germany_p95,
        japan_p95,
        improvement,
        worsening,
        germany.source_sha256,
        germany.config_sha256,
        policy.attestation_public_key_sha256,
        _report_sha256(germany),
        _report_sha256(japan),
    )


def write_region_latency_report(report: RegionLatencyReport, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(asdict(report), default=str, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def load_region_latency_report(path: Path) -> RegionLatencyReport:
    payload = json.loads(path.read_text(encoding="utf-8"))
    report_keys = {
        "schema_version",
        "region",
        "host_fingerprint",
        "attestation_sha256",
        "source_sha256",
        "config_sha256",
        "samples_sha256",
        "started_at",
        "completed_at",
        "minimum_samples_per_cell",
        "maximum_probe_duration_seconds",
        "maximum_inter_sample_gap_seconds",
        "maximum_clock_skew_ms",
        "distributions",
        "execution_authorized",
    }
    if (
        not isinstance(payload, dict)
        or set(payload) != report_keys
        or not isinstance(payload.get("distributions"), list)
    ):
        raise ValueError("invalid region latency report")
    if payload["execution_authorized"] is not False:
        raise ValueError("region latency report cannot authorize execution")
    int_keys = {
        "schema_version",
        "minimum_samples_per_cell",
        "maximum_probe_duration_seconds",
        "maximum_inter_sample_gap_seconds",
        "maximum_clock_skew_ms",
    }
    if any(isinstance(payload[k], bool) or not isinstance(payload[k], int) for k in int_keys):
        raise ValueError("region report integer fields must not be coerced")
    distribution_keys = {
        "venue",
        "channel",
        "base",
        "symbol",
        "sample_count",
        "started_at",
        "completed_at",
        "p50_ms",
        "p95_ms",
        "p99_ms",
    }
    distributions: list[LatencyDistribution] = []
    for item in payload["distributions"]:
        if (
            not isinstance(item, dict)
            or set(item) != distribution_keys
            or isinstance(item["sample_count"], bool)
            or not isinstance(item["sample_count"], int)
        ):
            raise ValueError("invalid latency distribution payload")
        distributions.append(
            LatencyDistribution(
                Venue(item["venue"]),
                LatencyChannel(item["channel"]),
                item["base"],
                item["symbol"],
                item["sample_count"],
                datetime.fromisoformat(item["started_at"]),
                datetime.fromisoformat(item["completed_at"]),
                Decimal(item["p50_ms"]),
                Decimal(item["p95_ms"]),
                Decimal(item["p99_ms"]),
            )
        )
    return RegionLatencyReport(
        payload["schema_version"],
        payload["region"],
        payload["host_fingerprint"],
        payload["attestation_sha256"],
        payload["source_sha256"],
        payload["config_sha256"],
        payload["samples_sha256"],
        datetime.fromisoformat(payload["started_at"]),
        datetime.fromisoformat(payload["completed_at"]),
        payload["minimum_samples_per_cell"],
        payload["maximum_probe_duration_seconds"],
        payload["maximum_inter_sample_gap_seconds"],
        payload["maximum_clock_skew_ms"],
        tuple(distributions),
    )
