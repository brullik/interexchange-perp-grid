from __future__ import annotations

from dataclasses import dataclass

from interexchange_perp_grid.config import Settings
from interexchange_perp_grid.observability import LIVE_GUARD_DENIALS
from interexchange_perp_grid.reason_codes import ReasonCode

LiveDenyReason = ReasonCode


@dataclass(frozen=True, slots=True)
class LiveContext:
    ci_or_test: bool = True
    local_unlock_present: bool = False
    telegram_challenge_valid: bool = False
    current_qualification_valid: bool = False
    route_allowlisted: bool = False
    all_preflights_pass: bool = False


@dataclass(frozen=True, slots=True)
class LiveDecision:
    allowed: bool
    reason: ReasonCode | None


def evaluate_live_order(settings: Settings, context: LiveContext) -> LiveDecision:
    checks: tuple[tuple[bool, ReasonCode], ...] = (
        (settings.app.mode == "live", LiveDenyReason.NON_LIVE_MODE),
        (settings.live.enabled, LiveDenyReason.LIVE_FLAG_DISABLED),
        (not context.ci_or_test, LiveDenyReason.CI_OR_TEST_ENVIRONMENT),
        (context.local_unlock_present, LiveDenyReason.LOCAL_UNLOCK_MISSING),
        (context.telegram_challenge_valid, LiveDenyReason.TELEGRAM_CHALLENGE_MISSING),
        (
            context.current_qualification_valid,
            LiveDenyReason.CURRENT_QUALIFICATION_MISSING,
        ),
        (context.route_allowlisted, LiveDenyReason.ROUTE_NOT_ALLOWLISTED),
        (context.all_preflights_pass, LiveDenyReason.PREFLIGHT_FAILED),
    )
    for passed, reason in checks:
        if not passed:
            LIVE_GUARD_DENIALS.labels(reason=reason.value).inc()
            return LiveDecision(allowed=False, reason=reason)
    return LiveDecision(allowed=True, reason=None)
