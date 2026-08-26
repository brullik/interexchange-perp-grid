from __future__ import annotations

from dataclasses import dataclass

from interexchange_perp_grid.config import Settings
from interexchange_perp_grid.observability import LIVE_GUARD_DENIALS
from interexchange_perp_grid.reason_codes import ReasonCode

LiveDenyReason = ReasonCode


@dataclass(frozen=True, slots=True)
class LiveContext:
    ci_or_test: bool = True
    simulation_or_replay: bool = True
    local_unlock_present: bool = False
    telegram_challenge_valid: bool = False
    fast_live_preflight_valid: bool = False
    route_allowlisted: bool = False
    canary_policy_passed: bool = False
    capability_preflight_passed: bool = False
    account_preflight_passed: bool = False
    market_data_preflight_passed: bool = False
    reconciliation_passed: bool = False
    risk_preflight_passed: bool = False
    pause_or_kill_active: bool = True
    unknown_order_exists: bool = True


@dataclass(frozen=True, slots=True)
class LiveDecision:
    allowed: bool
    reason: ReasonCode | None


def evaluate_live_order(settings: Settings, context: LiveContext) -> LiveDecision:
    checks: tuple[tuple[bool, ReasonCode], ...] = (
        (settings.app.mode == "live", LiveDenyReason.NON_LIVE_MODE),
        (settings.live.enabled, LiveDenyReason.LIVE_FLAG_DISABLED),
        (not context.ci_or_test, LiveDenyReason.CI_OR_TEST_ENVIRONMENT),
        (not context.simulation_or_replay, LiveDenyReason.CI_OR_TEST_ENVIRONMENT),
        (context.local_unlock_present, LiveDenyReason.LOCAL_UNLOCK_MISSING),
        (context.telegram_challenge_valid, LiveDenyReason.TELEGRAM_CHALLENGE_MISSING),
        (
            context.fast_live_preflight_valid,
            LiveDenyReason.FAST_LIVE_PREFLIGHT_MISSING,
        ),
        (context.route_allowlisted, LiveDenyReason.ROUTE_NOT_ALLOWLISTED),
        (context.canary_policy_passed, LiveDenyReason.CANARY_POLICY_VIOLATION),
        (
            context.capability_preflight_passed,
            LiveDenyReason.PRIVATE_CAPABILITY_MISSING,
        ),
        (context.account_preflight_passed, LiveDenyReason.PREFLIGHT_FAILED),
        (
            context.market_data_preflight_passed,
            LiveDenyReason.MARKET_DATA_PREFLIGHT_FAILED,
        ),
        (context.reconciliation_passed, LiveDenyReason.RECONCILIATION_INCOMPLETE),
        (context.risk_preflight_passed, LiveDenyReason.RISK_PREFLIGHT_FAILED),
        (not context.pause_or_kill_active, LiveDenyReason.PAUSE_OR_KILL_ACTIVE),
        (not context.unknown_order_exists, LiveDenyReason.UNKNOWN_ORDER_BLOCK),
    )
    for passed, reason in checks:
        if not passed:
            LIVE_GUARD_DENIALS.labels(reason=reason.value).inc()
            return LiveDecision(allowed=False, reason=reason)
    return LiveDecision(allowed=True, reason=None)
