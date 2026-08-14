from __future__ import annotations

import asyncio
import os
import time
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from interexchange_perp_grid.adapters.ccxt_pro import CcxtProAdapter
from interexchange_perp_grid.adapters.private import (
    CcxtPrivateAdapter,
    PrivateCredentials,
)
from interexchange_perp_grid.config import Settings
from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.execution import ExecutionIntent, OrderPurpose, Side
from interexchange_perp_grid.market_data import BookRegistry
from interexchange_perp_grid.private_execution import (
    CanaryAction,
    CanaryPolicy,
    IdempotentOrderExecutor,
    LiveCanaryExecutor,
    LivePairResult,
    PrivatePreflightInput,
    PrivatePreflightReport,
    run_private_preflight,
)
from interexchange_perp_grid.qualification import (
    load_qualification,
    qualification_is_current,
)
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.risk import (
    RiskBook,
    RiskLimits,
    RiskRequest,
    VenueProjection,
)
from interexchange_perp_grid.routes import (
    DirectedRouteQuote,
    evaluate_directed_route,
    minimum_common_base_quantity,
)
from interexchange_perp_grid.safety import LiveContext
from interexchange_perp_grid.state import (
    initialise_state,
    live_confirmation_valid,
    load_tranches,
    read_runtime_controls,
)
from interexchange_perp_grid.strategy import DirectedRouteKey

OWNER_CONFIRMATION = "I_ACCEPT_LIVE_CANARY_RISK"


@dataclass(frozen=True, slots=True)
class CanaryRunEvidence:
    submitted: bool
    reason: ReasonCode | None
    route: str
    quantity: Decimal | None
    public_quote: dict[str, object] | None
    preflights: tuple[PrivatePreflightReport, ...]
    pair_result: LivePairResult | None


async def run_canary_once(
    settings: Settings,
    config_path: Path,
    qualification_path: Path,
    repo_root: Path,
    owner_confirmation: str,
) -> CanaryRunEvidence:
    route = DirectedRouteKey(
        settings.live.canary_base.upper(),
        Venue(settings.live.canary_long_venue),
        Venue(settings.live.canary_short_venue),
    )
    if owner_confirmation != OWNER_CONFIRMATION:
        return CanaryRunEvidence(
            False,
            ReasonCode.OWNER_CONFIRMATION_MISSING,
            route.value,
            None,
            None,
            (),
            None,
        )
    state_path = Path(settings.storage.sqlite_path)
    await initialise_state(state_path)
    public_adapters = {
        venue: CcxtProAdapter(venue) for venue in (route.long_venue, route.short_venue)
    }
    private_adapters: dict[Venue, CcxtPrivateAdapter] = {}
    try:
        instruments: dict[Venue, Instrument] = {}
        public_reports = {}
        for venue, adapter in public_adapters.items():
            public_reports[venue] = await adapter.probe_public_capabilities()
            discovered = await adapter.discover_instruments()
            selected = next(
                (
                    instrument
                    for instrument in discovered
                    if instrument.base == route.base and instrument.settle == "USDT"
                ),
                None,
            )
            if selected is None:
                return _denied(route, ReasonCode.SYMBOL_UNAVAILABLE)
            instruments[venue] = selected

        long_instrument = instruments[route.long_venue]
        short_instrument = instruments[route.short_venue]
        registry = BookRegistry()
        for venue, instrument in instruments.items():
            await public_adapters[venue].watch_order_book(instrument)
        books = {
            venue: await public_adapters[venue].watch_order_book(instrument)
            for venue, instrument in instruments.items()
        }
        quality = {
            venue: registry.accept(
                book,
                max_age_ms=settings.market_data.max_l2_age_ms,
                max_clock_skew_ms=settings.market_data.max_clock_skew_ms,
            )
            for venue, book in books.items()
        }
        funding = {
            venue: await public_adapters[venue].fetch_funding(instrument)
            for venue, instrument in instruments.items()
        }
        preliminary_quantity = minimum_common_base_quantity(
            long_instrument,
            short_instrument,
            books[route.long_venue].asks[0].price,
            books[route.short_venue].bids[0].price,
        )
        quote = evaluate_directed_route(
            long_instrument,
            short_instrument,
            books[route.long_venue],
            books[route.short_venue],
            funding[route.long_venue],
            funding[route.short_venue],
            quality[route.long_venue],
            quality[route.short_venue],
            preliminary_quantity,
        )
        if not quote.eligible:
            return _denied(route, quote.reason, quote)

        preflights: list[PrivatePreflightReport] = []
        accounts = {}
        fees = {}
        for venue, instrument in instruments.items():
            credentials = PrivateCredentials.from_environment(venue)
            private_adapter = CcxtPrivateAdapter(venue, credentials)
            private_adapters[venue] = private_adapter
            capability = await private_adapter.probe_private_capabilities()
            account = await private_adapter.fetch_account(instrument)
            fee = await private_adapter.fetch_trading_fee(instrument)
            accounts[venue] = account
            fees[venue] = fee
            report = run_private_preflight(
                PrivatePreflightInput(
                    capability=capability,
                    account=account,
                    instrument=instrument,
                    fee_rate=fee,
                    funding_known=funding[venue].rate is not None
                    and funding[venue].next_funding_timestamp_ms is not None,
                    clock_skew_ms=public_reports[venue].clock_skew_ms,
                    maximum_clock_skew_ms=settings.market_data.max_clock_skew_ms,
                    symbol_available=True,
                    data_quality_passed=quality[venue].accepted,
                    reconciliation_passed=True,
                    risk_passed=True,
                    free_margin_floor_ratio=settings.risk.local_free_margin_floor_ratio,
                )
            )
            preflights.append(report)

        notional = _quote_notional(quote)
        projected_stress = (quote.four_leg_fee_estimate or Decimal(0)) + notional * Decimal("0.01")
        risk = RiskBook(
            RiskLimits(
                settings.risk.pair_stressed_loss_limit_usdt,
                settings.risk.portfolio_stressed_loss_limit_usdt,
                settings.risk.max_active_routes,
                settings.risk.max_routes_per_base,
                settings.risk.max_tranches_per_route,
                settings.risk.local_free_margin_floor_ratio,
                settings.risk.initial_effective_leverage_cap,
            )
        ).reserve(
            RiskRequest(
                "live-canary-reservation",
                route.value,
                route.base,
                "live-canary-tranche",
                projected_stress,
                tuple(
                    VenueProjection(
                        venue,
                        accounts[venue].equity_usdt,
                        notional,
                        accounts[venue].equity_usdt
                        - accounts[venue].free_margin_usdt
                        + notional / settings.risk.initial_effective_leverage_cap,
                        projected_stress / 2,
                    )
                    for venue in (route.long_venue, route.short_venue)
                ),
                True,
            )
        )
        controls = await read_runtime_controls(state_path)
        tranches = await load_tranches(state_path)
        unknown_order = any(tranche.state.value == "UNKNOWN_ORDER" for tranche in tranches)
        qualification_valid = False
        if qualification_path.is_file():
            evidence = load_qualification(qualification_path)
            qualification_valid, _ = qualification_is_current(
                evidence,
                repo_root,
                config_path,
                Path(settings.storage.parquet_dir),
                settings.live.qualification_max_age_seconds,
            )
        policy = CanaryPolicy(route.base, route)
        policy_passed, _ = policy.evaluate(CanaryAction(route, 1, notional, notional))
        live_context = LiveContext(
            ci_or_test=_ci_or_test_environment(),
            simulation_or_replay=settings.app.mode != "live",
            local_unlock_present=bool(os.environ.get("IPEG_LOCAL_UNLOCK_SECRET")),
            telegram_challenge_valid=await live_confirmation_valid(state_path),
            current_qualification_valid=qualification_valid,
            route_allowlisted=True,
            canary_policy_passed=policy_passed,
            capability_preflight_passed=all(report.passed for report in preflights),
            account_preflight_passed=all(report.passed for report in preflights),
            market_data_preflight_passed=all(item.accepted for item in quality.values()),
            reconciliation_passed=controls.reconciliation_state == "CONSISTENT",
            risk_preflight_passed=risk.accepted,
            pause_or_kill_active=controls.paused or controls.killed,
            unknown_order_exists=unknown_order,
        )
        prefix = f"canary-{time.time_ns()}-{uuid4().hex[:8]}"
        long_intent = ExecutionIntent(
            f"{prefix}-long",
            route.long_venue,
            Side.BUY,
            OrderPurpose.NORMAL_OPEN,
            quote.base_quantity,
            quote.entry_long_vwap,
        )
        short_intent = ExecutionIntent(
            f"{prefix}-short",
            route.short_venue,
            Side.SELL,
            OrderPurpose.NORMAL_OPEN,
            quote.base_quantity,
            quote.entry_short_vwap,
        )
        pair_result = await LiveCanaryExecutor(
            settings,
            policy,
            IdempotentOrderExecutor(private_adapters[route.long_venue]),
            IdempotentOrderExecutor(private_adapters[route.short_venue]),
        ).submit_pair(
            CanaryAction(route, 1, notional, notional),
            live_context,
            long_intent,
            short_intent,
            long_instrument,
            short_instrument,
        )
        return CanaryRunEvidence(
            pair_result.submitted,
            pair_result.reason,
            route.value,
            quote.base_quantity,
            asdict(quote),
            tuple(preflights),
            pair_result,
        )
    finally:
        await asyncio.gather(
            *(adapter.close() for adapter in public_adapters.values()),
            *(adapter.close() for adapter in private_adapters.values()),
            return_exceptions=True,
        )


def _quote_notional(quote: DirectedRouteQuote) -> Decimal:
    assert quote.entry_long_vwap is not None
    assert quote.entry_short_vwap is not None
    return quote.base_quantity * max(quote.entry_long_vwap, quote.entry_short_vwap)


def _denied(
    route: DirectedRouteKey,
    reason: ReasonCode,
    quote: DirectedRouteQuote | None = None,
) -> CanaryRunEvidence:
    return CanaryRunEvidence(
        False,
        reason,
        route.value,
        quote.base_quantity if quote is not None else None,
        asdict(quote) if quote is not None else None,
        (),
        None,
    )


def _ci_or_test_environment() -> bool:
    return os.environ.get("CI", "").lower() == "true" or "PYTEST_CURRENT_TEST" in os.environ
