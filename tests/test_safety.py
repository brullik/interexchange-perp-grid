from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from interexchange_perp_grid.config import Settings, load_settings
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.safety import LiveContext, LiveDenyReason, evaluate_live_order

CONFIG = Path("config/defaults.yaml")


def test_defaults_deny_live_orders() -> None:
    decision = evaluate_live_order(load_settings(CONFIG), LiveContext())
    assert decision.allowed is False
    assert decision.reason == LiveDenyReason.NON_LIVE_MODE
    assert decision.reason == ReasonCode.NON_LIVE_MODE


def test_live_flag_alone_is_insufficient() -> None:
    settings = load_settings(CONFIG)
    raw = settings.model_dump(mode="json")
    raw["app"]["mode"] = "live"
    raw["live"]["enabled"] = True
    live_settings = Settings.model_validate(raw)

    decision = evaluate_live_order(live_settings, LiveContext(ci_or_test=True))
    assert decision.allowed is False
    assert decision.reason == LiveDenyReason.CI_OR_TEST_ENVIRONMENT


def test_every_independent_gate_is_required() -> None:
    settings = load_settings(CONFIG)
    raw = settings.model_dump(mode="json")
    raw["app"]["mode"] = "live"
    raw["live"]["enabled"] = True
    live_settings = Settings.model_validate(raw)

    context = LiveContext(
        ci_or_test=False,
        simulation_or_replay=False,
        local_unlock_present=True,
        telegram_challenge_valid=True,
        fast_live_preflight_valid=True,
        route_allowlisted=True,
        canary_policy_passed=True,
        capability_preflight_passed=True,
        account_preflight_passed=True,
        market_data_preflight_passed=True,
        reconciliation_passed=True,
        risk_preflight_passed=True,
        pause_or_kill_active=False,
        unknown_order_exists=False,
    )
    assert evaluate_live_order(live_settings, context).allowed is True

    expected_failures = (
        ("ci_or_test", True, ReasonCode.CI_OR_TEST_ENVIRONMENT),
        ("simulation_or_replay", True, ReasonCode.CI_OR_TEST_ENVIRONMENT),
        ("local_unlock_present", False, ReasonCode.LOCAL_UNLOCK_MISSING),
        ("telegram_challenge_valid", False, ReasonCode.TELEGRAM_CHALLENGE_MISSING),
        (
            "fast_live_preflight_valid",
            False,
            ReasonCode.FAST_LIVE_PREFLIGHT_MISSING,
        ),
        ("route_allowlisted", False, ReasonCode.ROUTE_NOT_ALLOWLISTED),
        ("canary_policy_passed", False, ReasonCode.CANARY_POLICY_VIOLATION),
        (
            "capability_preflight_passed",
            False,
            ReasonCode.PRIVATE_CAPABILITY_MISSING,
        ),
        ("account_preflight_passed", False, ReasonCode.PREFLIGHT_FAILED),
        (
            "market_data_preflight_passed",
            False,
            ReasonCode.MARKET_DATA_PREFLIGHT_FAILED,
        ),
        (
            "reconciliation_passed",
            False,
            ReasonCode.RECONCILIATION_INCOMPLETE,
        ),
        ("risk_preflight_passed", False, ReasonCode.RISK_PREFLIGHT_FAILED),
        ("pause_or_kill_active", True, ReasonCode.PAUSE_OR_KILL_ACTIVE),
        ("unknown_order_exists", True, ReasonCode.UNKNOWN_ORDER_BLOCK),
    )
    for field, value, expected_reason in expected_failures:
        rejected = evaluate_live_order(live_settings, replace(context, **{field: value}))
        assert rejected.allowed is False
        assert rejected.reason == expected_reason


def test_shadow_replay_and_configuration_only_never_activate_live() -> None:
    settings = load_settings(CONFIG)
    complete_context = LiveContext(
        ci_or_test=False,
        simulation_or_replay=False,
        local_unlock_present=True,
        telegram_challenge_valid=True,
        fast_live_preflight_valid=True,
        route_allowlisted=True,
        canary_policy_passed=True,
        capability_preflight_passed=True,
        account_preflight_passed=True,
        market_data_preflight_passed=True,
        reconciliation_passed=True,
        risk_preflight_passed=True,
        pause_or_kill_active=False,
        unknown_order_exists=False,
    )
    assert evaluate_live_order(settings, complete_context).reason == ReasonCode.NON_LIVE_MODE

    raw = settings.model_dump(mode="json")
    raw["app"]["mode"] = "live"
    raw["live"]["enabled"] = True
    live_settings = Settings.model_validate(raw)
    assert evaluate_live_order(live_settings, LiveContext()).allowed is False
