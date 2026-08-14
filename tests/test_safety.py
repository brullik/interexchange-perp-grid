from __future__ import annotations

from pathlib import Path

from interexchange_perp_grid.config import Settings, load_settings
from interexchange_perp_grid.safety import LiveContext, LiveDenyReason, evaluate_live_order


CONFIG = Path("config/defaults.yaml")


def test_defaults_deny_live_orders() -> None:
    decision = evaluate_live_order(load_settings(CONFIG), LiveContext())
    assert decision.allowed is False
    assert decision.reason == LiveDenyReason.NON_LIVE_MODE


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
        local_unlock_present=True,
        telegram_challenge_valid=True,
        current_qualification_valid=True,
        route_allowlisted=True,
        all_preflights_pass=True,
    )
    assert evaluate_live_order(live_settings, context).allowed is True
