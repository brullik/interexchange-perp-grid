from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from interexchange_perp_grid.config import load_settings
from interexchange_perp_grid.domain import CapabilityReport, Venue
from interexchange_perp_grid.private_domain import PrivateCapabilityReport
from interexchange_perp_grid.venue_capabilities import (
    CapabilityReason,
    CapabilityState,
    build_venue_capability_matrix,
)


def _public(venue: Venue, *, ready: bool = True) -> CapabilityReport:
    return CapabilityReport(
        venue=venue,
        bbo_stream=ready,
        l2_stream=True,
        funding=True,
        mark_index=True,
        server_time=True,
        clock_skew_ms=0,
        checked_at=datetime.now(UTC),
        missing=() if ready else ("bbo_stream",),
    )


def _private(venue: Venue, *, ready: bool = True) -> PrivateCapabilityReport:
    return PrivateCapabilityReport(
        venue=venue,
        order_stream=True,
        position_stream=True,
        balance_stream=True,
        fetch_balance=True,
        fetch_positions=True,
        submit_order=ready,
        cancel_order=ready,
        fetch_order=True,
        fetch_fee=True,
        checked_at=datetime.now(UTC),
        missing=() if ready else ("submit_order", "cancel_order"),
        fetch_open_orders=True,
        fetch_closed_orders=True,
    )


def _reports() -> tuple[dict[Venue, CapabilityReport], dict[Venue, PrivateCapabilityReport]]:
    public = {venue: _public(venue) for venue in Venue}
    private = {venue: _private(venue) for venue in Venue}
    public[Venue.BINGX] = _public(Venue.BINGX, ready=False)
    public[Venue.MEXC] = _public(Venue.MEXC, ready=False)
    private[Venue.MEXC] = _private(Venue.MEXC, ready=False)
    return public, private


def test_default_matrix_covers_all_seven_without_enabling_expansion() -> None:
    settings = load_settings(Path("config/defaults.yaml"))
    public, private = _reports()

    matrix = build_venue_capability_matrix(
        settings,
        public_reports=public,
        private_reports=private,
        account_preflight_passed=frozenset({Venue.BINANCE_USDM, Venue.BYBIT, Venue.OKX}),
    )

    assert tuple(row.venue for row in matrix.rows) == tuple(Venue)
    assert {row.wave for row in matrix.rows} == {1, 2, 3}
    assert matrix.execution_authorized is False
    for venue in (Venue.BINANCE_USDM, Venue.BYBIT, Venue.OKX):
        row = matrix.for_venue(venue)
        assert row.public_runtime == CapabilityState.QUALIFIED
        assert row.live_capability == CapabilityState.QUALIFIED
        assert row.execution_authorized is False
    for venue in (Venue.BITGET, Venue.KUCOIN_FUTURES, Venue.BINGX, Venue.MEXC):
        row = matrix.for_venue(venue)
        assert row.public_runtime == CapabilityState.DISABLED
        assert row.live_capability == CapabilityState.DISABLED
        assert CapabilityReason.PUBLIC_RUNTIME_DISABLED in row.reasons
        assert CapabilityReason.LIVE_ALLOWLIST_DISABLED in row.reasons
    assert matrix.for_venue(Venue.BINGX).public_contract == CapabilityState.QUARANTINED
    mexc = matrix.for_venue(Venue.MEXC)
    assert mexc.public_contract == CapabilityState.QUARANTINED
    assert mexc.private_contract == CapabilityState.QUARANTINED
    assert mexc.private_missing == ("submit_order", "cancel_order")


def test_transient_quarantine_is_not_reported_as_permanent_removal() -> None:
    settings = load_settings(Path("config/defaults.yaml"))
    public, private = _reports()

    matrix = build_venue_capability_matrix(
        settings,
        public_reports=public,
        private_reports=private,
        account_preflight_passed=frozenset({Venue.BINANCE_USDM, Venue.BYBIT, Venue.OKX}),
        quarantined_venues=frozenset({Venue.OKX}),
    )

    okx = matrix.for_venue(Venue.OKX)
    assert okx.public_runtime == CapabilityState.QUARANTINED
    assert okx.live_capability == CapabilityState.QUARANTINED
    assert CapabilityReason.VENUE_QUARANTINED in okx.reasons


def test_runtime_report_ttl_matches_six_hour_capability_refresh() -> None:
    settings = load_settings(Path("config/defaults.yaml"))
    public, private = _reports()
    now = datetime.now(UTC)
    public[Venue.OKX] = replace(
        public[Venue.OKX],
        checked_at=now - timedelta(seconds=301),
    )

    matrix = build_venue_capability_matrix(
        settings,
        public_reports=public,
        private_reports=private,
        now=now,
        maximum_report_age_seconds=settings.universe.instrument_refresh_seconds,
    )

    assert matrix.for_venue(Venue.OKX).public_contract == CapabilityState.QUALIFIED


def test_unknown_capability_quarantines_only_the_affected_enabled_venue() -> None:
    settings = load_settings(Path("config/defaults.yaml"))
    public, private = _reports()
    del public[Venue.OKX]
    del private[Venue.OKX]

    matrix = build_venue_capability_matrix(
        settings,
        public_reports=public,
        private_reports=private,
        account_preflight_passed=frozenset({Venue.BINANCE_USDM, Venue.BYBIT, Venue.OKX}),
    )

    okx = matrix.for_venue(Venue.OKX)
    assert okx.public_runtime == CapabilityState.QUARANTINED
    assert okx.live_capability == CapabilityState.QUARANTINED
    assert CapabilityReason.PUBLIC_CAPABILITY_UNKNOWN in okx.reasons
    assert CapabilityReason.PRIVATE_CAPABILITY_UNKNOWN in okx.reasons
    assert matrix.for_venue(Venue.BINANCE_USDM).public_runtime == CapabilityState.QUALIFIED
    assert matrix.for_venue(Venue.BYBIT).public_runtime == CapabilityState.QUALIFIED


def test_matrix_requires_all_full_target_profiles_and_matching_report_identity() -> None:
    settings = load_settings(Path("config/defaults.yaml"))
    public, private = _reports()
    incomplete = settings.model_copy(
        update={"venues": settings.venues.model_copy(update={"wave3": (Venue.BINGX.value,)})}
    )
    with pytest.raises(ValueError, match="missing venues: mexc"):
        build_venue_capability_matrix(
            incomplete,
            public_reports=public,
            private_reports=private,
        )
    canary_matrix = build_venue_capability_matrix(
        incomplete,
        public_reports=public,
        private_reports=private,
        require_all_profiles=False,
    )
    assert canary_matrix.for_venue(Venue.MEXC).wave == 0
    assert canary_matrix.for_venue(Venue.MEXC).public_runtime == CapabilityState.DISABLED

    public[Venue.OKX] = _public(Venue.BYBIT)
    with pytest.raises(ValueError, match="public capability report identity mismatch"):
        build_venue_capability_matrix(
            settings,
            public_reports=public,
            private_reports=private,
        )


def test_stale_or_internally_inconsistent_reports_fail_closed() -> None:
    settings = load_settings(Path("config/defaults.yaml"))
    public, private = _reports()
    now = datetime.now(UTC)
    public[Venue.OKX] = replace(
        public[Venue.OKX],
        checked_at=now - timedelta(seconds=301),
    )
    private[Venue.BYBIT] = replace(
        private[Venue.BYBIT],
        submit_order=False,
        missing=(),
    )
    public[Venue.BINANCE_USDM] = replace(
        public[Venue.BINANCE_USDM],
        missing=("bbo_stream",),
    )

    matrix = build_venue_capability_matrix(
        settings,
        public_reports=public,
        private_reports=private,
        account_preflight_passed=frozenset({Venue.BINANCE_USDM, Venue.BYBIT, Venue.OKX}),
        now=now,
    )

    okx = matrix.for_venue(Venue.OKX)
    assert okx.public_runtime == CapabilityState.QUARANTINED
    assert okx.public_missing == ("capability_report_stale",)
    assert CapabilityReason.PUBLIC_CAPABILITY_STALE in okx.reasons
    bybit = matrix.for_venue(Venue.BYBIT)
    assert bybit.private_contract == CapabilityState.QUARANTINED
    assert bybit.live_capability == CapabilityState.QUARANTINED
    assert CapabilityReason.PRIVATE_CAPABILITY_MISSING in bybit.reasons
    binance = matrix.for_venue(Venue.BINANCE_USDM)
    assert binance.public_contract == CapabilityState.QUARANTINED
    assert binance.live_capability == CapabilityState.QUARANTINED
    assert CapabilityReason.PUBLIC_CAPABILITY_MISSING in binance.reasons


def test_non_boolean_capability_metadata_fails_closed() -> None:
    settings = load_settings(Path("config/defaults.yaml"))
    public, private = _reports()
    public[Venue.OKX] = replace(public[Venue.OKX], bbo_stream=1)  # type: ignore[arg-type]
    private[Venue.BYBIT] = replace(private[Venue.BYBIT], submit_order="yes")  # type: ignore[arg-type]

    matrix = build_venue_capability_matrix(
        settings,
        public_reports=public,
        private_reports=private,
        account_preflight_passed=frozenset({Venue.BINANCE_USDM, Venue.BYBIT, Venue.OKX}),
    )

    assert matrix.for_venue(Venue.OKX).public_runtime == CapabilityState.QUARANTINED
    assert matrix.for_venue(Venue.BYBIT).live_capability == CapabilityState.QUARANTINED


def test_unknown_clock_and_malformed_missing_metadata_fail_closed() -> None:
    settings = load_settings(Path("config/defaults.yaml"))
    public, private = _reports()
    public[Venue.OKX] = replace(public[Venue.OKX], clock_skew_ms=None)
    public[Venue.BINANCE_USDM] = replace(
        public[Venue.BINANCE_USDM],
        missing=[],  # type: ignore[arg-type]
    )
    private[Venue.BYBIT] = replace(
        private[Venue.BYBIT],
        missing=[],  # type: ignore[arg-type]
    )

    matrix = build_venue_capability_matrix(
        settings,
        public_reports=public,
        private_reports=private,
        account_preflight_passed=frozenset({Venue.BINANCE_USDM, Venue.BYBIT, Venue.OKX}),
    )

    assert matrix.for_venue(Venue.OKX).public_runtime == CapabilityState.QUARANTINED
    assert matrix.for_venue(Venue.BINANCE_USDM).public_runtime == CapabilityState.QUARANTINED
    assert matrix.for_venue(Venue.BYBIT).live_capability == CapabilityState.QUARANTINED


def test_yaml_reclassification_cannot_enable_expansion_live() -> None:
    settings = load_settings(Path("config/defaults.yaml"))
    public, private = _reports()
    public[Venue.MEXC] = _public(Venue.MEXC)
    private[Venue.MEXC] = _private(Venue.MEXC)
    reclassified = settings.model_copy(
        update={
            "venues": settings.venues.model_copy(
                update={
                    "wave1_public": (
                        Venue.MEXC.value,
                        Venue.BYBIT.value,
                        Venue.OKX.value,
                    ),
                    "canary_primary": (Venue.MEXC.value, Venue.BYBIT.value),
                    "canary_alternate": (Venue.OKX.value,),
                    "wave3": (Venue.BINANCE_USDM.value, Venue.BINGX.value),
                }
            )
        }
    )

    matrix = build_venue_capability_matrix(
        reclassified,
        public_reports=public,
        private_reports=private,
        account_preflight_passed=frozenset(Venue),
    )

    mexc = matrix.for_venue(Venue.MEXC)
    assert mexc.public_contract == CapabilityState.QUALIFIED
    assert mexc.private_contract == CapabilityState.QUALIFIED
    assert mexc.public_runtime == CapabilityState.DISABLED
    assert mexc.live_capability == CapabilityState.DISABLED
    assert mexc.execution_authorized is False
