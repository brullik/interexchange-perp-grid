from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from interexchange_perp_grid.config import Settings


class LiveDenyReason(StrEnum):
    NON_LIVE_MODE = "NON_LIVE_MODE"
    LIVE_FLAG_DISABLED = "LIVE_FLAG_DISABLED"
    CI_OR_TEST_ENVIRONMENT = "CI_OR_TEST_ENVIRONMENT"
    LOCAL_UNLOCK_MISSING = "LOCAL_UNLOCK_MISSING"
    TELEGRAM_CHALLENGE_MISSING = "TELEGRAM_CHALLENGE_MISSING"
    CURRENT_QUALIFICATION_MISSING = "CURRENT_QUALIFICATION_MISSING"
    ROUTE_NOT_ALLOWLISTED = "ROUTE_NOT_ALLOWLISTED"
    PREFLIGHT_FAILED = "PREFLIGHT_FAILED"


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
    reason: LiveDenyReason | None


def evaluate_live_order(settings: Settings, context: LiveContext) -> LiveDecision:
    checks: tuple[tuple[bool, LiveDenyReason], ...] = (
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
            return LiveDecision(allowed=False, reason=reason)
    return LiveDecision(allowed=True, reason=None)
