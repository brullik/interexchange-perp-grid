from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from interexchange_perp_grid.config import Settings
from interexchange_perp_grid.domain import WAVE1_VENUES, CapabilityReport, Venue
from interexchange_perp_grid.private_domain import PrivateCapabilityReport


class CapabilityState(StrEnum):
    QUALIFIED = "QUALIFIED"
    QUARANTINED = "QUARANTINED"
    DISABLED = "DISABLED"


class CapabilityReason(StrEnum):
    PUBLIC_CAPABILITY_UNKNOWN = "PUBLIC_CAPABILITY_UNKNOWN"
    PUBLIC_CAPABILITY_MISSING = "PUBLIC_CAPABILITY_MISSING"
    PUBLIC_CAPABILITY_STALE = "PUBLIC_CAPABILITY_STALE"
    PRIVATE_CAPABILITY_UNKNOWN = "PRIVATE_CAPABILITY_UNKNOWN"
    PRIVATE_CAPABILITY_MISSING = "PRIVATE_CAPABILITY_MISSING"
    PRIVATE_CAPABILITY_STALE = "PRIVATE_CAPABILITY_STALE"
    PUBLIC_RUNTIME_DISABLED = "PUBLIC_RUNTIME_DISABLED"
    LIVE_ALLOWLIST_DISABLED = "LIVE_ALLOWLIST_DISABLED"
    ACCOUNT_PREFLIGHT_MISSING = "ACCOUNT_PREFLIGHT_MISSING"
    VENUE_QUARANTINED = "VENUE_QUARANTINED"


@dataclass(frozen=True, slots=True)
class VenueCapabilityRow:
    venue: Venue
    wave: int
    public_contract: CapabilityState
    private_contract: CapabilityState
    public_runtime: CapabilityState
    live_capability: CapabilityState
    public_missing: tuple[str, ...]
    private_missing: tuple[str, ...]
    reasons: tuple[CapabilityReason, ...]
    execution_authorized: bool = field(default=False, init=False)


@dataclass(frozen=True, slots=True)
class VenueCapabilityMatrix:
    rows: tuple[VenueCapabilityRow, ...]
    execution_authorized: bool = field(default=False, init=False)

    def for_venue(self, venue: Venue) -> VenueCapabilityRow:
        return next(row for row in self.rows if row.venue == venue)


def build_venue_capability_matrix(
    settings: Settings,
    *,
    public_reports: Mapping[Venue, CapabilityReport],
    private_reports: Mapping[Venue, PrivateCapabilityReport],
    account_preflight_passed: frozenset[Venue] = frozenset(),
    quarantined_venues: frozenset[Venue] = frozenset(),
    now: datetime | None = None,
    maximum_report_age_seconds: int = 300,
    require_all_profiles: bool = True,
    public_runtime_enabled: frozenset[Venue] | None = None,
) -> VenueCapabilityMatrix:
    observed_now = now or datetime.now(UTC)
    if observed_now.tzinfo is None or observed_now.utcoffset() is None:
        raise ValueError("capability matrix clock must be timezone-aware")
    if maximum_report_age_seconds <= 0:
        raise ValueError("capability report maximum age must be positive")
    waves = _configured_waves(settings, require_all_profiles=require_all_profiles)
    public_enabled = (
        frozenset(Venue(value) for value in settings.venues.public_runtime)
        if public_runtime_enabled is None
        else public_runtime_enabled
    )
    if not public_enabled <= frozenset(waves):
        raise ValueError("public runtime contains a venue outside the configured waves")
    live_allowlist = frozenset(
        Venue(value) for value in settings.venues.canary_primary + settings.venues.canary_alternate
    ) & frozenset(WAVE1_VENUES)
    _validate_report_identity(public_reports, private_reports)

    rows = tuple(
        _build_row(
            venue,
            waves[venue],
            public_reports.get(venue),
            private_reports.get(venue),
            public_enabled=public_enabled,
            live_allowlist=live_allowlist,
            account_preflight_passed=account_preflight_passed,
            quarantined_venues=quarantined_venues,
            now=observed_now,
            maximum_report_age_seconds=maximum_report_age_seconds,
            maximum_clock_skew_ms=settings.market_data.max_clock_skew_ms,
        )
        for venue in Venue
    )
    return VenueCapabilityMatrix(rows=rows)


def _configured_waves(settings: Settings, *, require_all_profiles: bool) -> dict[Venue, int]:
    waves: dict[Venue, int] = {}
    for wave, values in (
        (1, settings.venues.wave1_public),
        (2, settings.venues.wave2),
        (3, settings.venues.wave3),
    ):
        for value in values:
            venue = Venue(value)
            if venue in waves:
                raise ValueError(f"venue appears in more than one wave: {venue.value}")
            waves[venue] = wave
    missing = set(Venue) - set(waves)
    if missing:
        if not require_all_profiles:
            waves.update({venue: 0 for venue in missing})
            return waves
        names = ", ".join(sorted(venue.value for venue in missing))
        raise ValueError(f"full-target capability matrix is missing venues: {names}")
    return waves


def _validate_report_identity(
    public_reports: Mapping[Venue, CapabilityReport],
    private_reports: Mapping[Venue, PrivateCapabilityReport],
) -> None:
    for venue, public_report in public_reports.items():
        if public_report.venue != venue:
            raise ValueError(f"public capability report identity mismatch for {venue.value}")
    for venue, private_report in private_reports.items():
        if private_report.venue != venue:
            raise ValueError(f"private capability report identity mismatch for {venue.value}")


def _build_row(
    venue: Venue,
    wave: int,
    public_report: CapabilityReport | None,
    private_report: PrivateCapabilityReport | None,
    *,
    public_enabled: frozenset[Venue],
    live_allowlist: frozenset[Venue],
    account_preflight_passed: frozenset[Venue],
    quarantined_venues: frozenset[Venue],
    now: datetime,
    maximum_report_age_seconds: int,
    maximum_clock_skew_ms: int,
) -> VenueCapabilityRow:
    public_current = public_report is not None and _report_is_current(
        public_report.checked_at,
        now,
        maximum_report_age_seconds,
    )
    private_current = private_report is not None and _report_is_current(
        private_report.checked_at,
        now,
        maximum_report_age_seconds,
    )
    public_contract = (
        CapabilityState.QUALIFIED
        if public_report is not None
        and public_current
        and _public_contract_ready(public_report, maximum_clock_skew_ms)
        else CapabilityState.QUARANTINED
    )
    private_contract = (
        CapabilityState.QUALIFIED
        if private_report is not None
        and private_current
        and _private_contract_ready(private_report)
        else CapabilityState.QUARANTINED
    )
    public_missing = (
        public_report.missing
        if public_report is not None and public_current
        else ("capability_report",)
        if public_report is None
        else ("capability_report_stale",)
    )
    private_missing = (
        private_report.missing
        if private_report is not None and private_current
        else ("capability_report",)
        if private_report is None
        else ("capability_report_stale",)
    )
    reasons: list[CapabilityReason] = []
    if public_report is None:
        reasons.append(CapabilityReason.PUBLIC_CAPABILITY_UNKNOWN)
    elif not public_current:
        reasons.append(CapabilityReason.PUBLIC_CAPABILITY_STALE)
    elif public_contract != CapabilityState.QUALIFIED:
        reasons.append(CapabilityReason.PUBLIC_CAPABILITY_MISSING)
    if private_report is None:
        reasons.append(CapabilityReason.PRIVATE_CAPABILITY_UNKNOWN)
    elif not private_current:
        reasons.append(CapabilityReason.PRIVATE_CAPABILITY_STALE)
    elif private_contract != CapabilityState.QUALIFIED:
        reasons.append(CapabilityReason.PRIVATE_CAPABILITY_MISSING)

    quarantined = venue in quarantined_venues
    if quarantined:
        reasons.append(CapabilityReason.VENUE_QUARANTINED)
    if venue not in public_enabled:
        public_runtime = CapabilityState.DISABLED
        reasons.append(CapabilityReason.PUBLIC_RUNTIME_DISABLED)
    elif quarantined:
        public_runtime = CapabilityState.QUARANTINED
    else:
        public_runtime = public_contract

    if venue not in live_allowlist:
        live_capability = CapabilityState.DISABLED
        reasons.append(CapabilityReason.LIVE_ALLOWLIST_DISABLED)
    elif quarantined:
        live_capability = CapabilityState.QUARANTINED
    elif (
        public_contract == CapabilityState.QUALIFIED
        and private_contract == CapabilityState.QUALIFIED
        and venue in account_preflight_passed
    ):
        live_capability = CapabilityState.QUALIFIED
    else:
        live_capability = CapabilityState.QUARANTINED
        if venue not in account_preflight_passed:
            reasons.append(CapabilityReason.ACCOUNT_PREFLIGHT_MISSING)

    return VenueCapabilityRow(
        venue=venue,
        wave=wave,
        public_contract=public_contract,
        private_contract=private_contract,
        public_runtime=public_runtime,
        live_capability=live_capability,
        public_missing=public_missing,
        private_missing=private_missing,
        reasons=tuple(reasons),
    )


def _report_is_current(
    checked_at: datetime,
    now: datetime,
    maximum_age_seconds: int,
) -> bool:
    if not isinstance(checked_at, datetime):
        return False
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        return False
    age_seconds = (now.astimezone(UTC) - checked_at.astimezone(UTC)).total_seconds()
    return 0 <= age_seconds <= maximum_age_seconds


def _private_contract_ready(report: PrivateCapabilityReport) -> bool:
    return (
        isinstance(report.missing, tuple)
        and not report.missing
        and all(isinstance(value, str) and value for value in report.missing)
        and all(
            value is True
            for value in (
                report.order_stream,
                report.position_stream,
                report.balance_stream,
                report.fetch_balance,
                report.fetch_positions,
                report.submit_order,
                report.cancel_order,
                report.fetch_order,
                report.fetch_fee,
                report.fetch_open_orders,
                report.fetch_closed_orders,
            )
        )
    )


def _public_contract_ready(report: CapabilityReport, maximum_clock_skew_ms: int) -> bool:
    return (
        isinstance(report.missing, tuple)
        and not report.missing
        and all(isinstance(value, str) and value for value in report.missing)
        and isinstance(report.clock_skew_ms, int)
        and not isinstance(report.clock_skew_ms, bool)
        and abs(report.clock_skew_ms) <= maximum_clock_skew_ms
        and all(
            value is True
            for value in (
                report.bbo_stream,
                report.l2_stream,
                report.funding,
                report.mark_index,
                report.server_time,
            )
        )
    )
