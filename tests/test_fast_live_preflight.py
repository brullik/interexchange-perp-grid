from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from interexchange_perp_grid.fast_live_preflight import (
    FAST_LIVE_PREFLIGHT_TTL_SECONDS,
    FastLiveIdentity,
    FastLivePreflightInput,
    consume_fast_live_preflight,
    evaluate_fast_live_preflight,
    load_fast_live_preflight,
    save_fast_live_preflight,
    validate_fast_live_preflight,
)
from interexchange_perp_grid.reason_codes import ReasonCode


def _identity(**updates: str) -> FastLiveIdentity:
    values = {
        "release_sha": "a" * 40,
        "source_sha256": "b" * 64,
        "config_sha256": "c" * 64,
        "profile_sha256": "d" * 64,
        "native_runtime_sha256": "e" * 64,
        "history_sha256": "f" * 64,
        "model_sha256": "1" * 64,
        "route": "BTC:bybit>okx",
        "direction": "positive",
        "account_generation_sha256": "2" * 64,
        "data_generation_sha256": "3" * 64,
        "risk_stage": "canary",
        "intent_sha256": "4" * 64,
    }
    values.update(updates)
    return FastLiveIdentity(**values)


def _inputs(**updates: object) -> FastLivePreflightInput:
    values: dict[str, object] = {
        "identity": _identity(),
        "exact_merged_clean_source": True,
        "money_movement_capability_absent": True,
        "private_capabilities_ready": True,
        "emergency_capability_ready": True,
        "account_modes_permissions_ready": True,
        "fees_funding_metadata_ready": True,
        "stable_flat": True,
        "zero_open_orders": True,
        "journal_known_and_reconciled": True,
        "clocks_and_market_data_ready": True,
        "executable_depth_ready": True,
        "history_model_ready": True,
        "regime_clear": True,
        "economics_positive": True,
        "risk_margin_leverage_ready": True,
        "owner_unlock_absent": True,
        "telegram_challenge_absent": True,
        "numerical_breakdown": {
            "history_days": "30",
            "projected_route_loss_usdt": "0.91",
        },
    }
    values.update(updates)
    return FastLivePreflightInput(**values)  # type: ignore[arg-type]


def test_fast_live_preflight_pass_is_exact_bound_ttl_and_non_authorizing() -> None:
    now = datetime(2026, 8, 26, tzinfo=UTC)
    report = evaluate_fast_live_preflight(_inputs(), now=now)

    assert report.status == "PASS"
    assert report.reason == ReasonCode.FAST_LIVE_PREFLIGHT_PASSED
    assert (report.expires_at - report.created_at).total_seconds() == (
        FAST_LIVE_PREFLIGHT_TTL_SECONDS
    )
    assert not report.execution_authorized
    assert validate_fast_live_preflight(report, _identity(), now=now) is None


@pytest.mark.parametrize(
    ("field", "reason"),
    [
        ("exact_merged_clean_source", ReasonCode.FAST_LIVE_SOURCE_IDENTITY_INVALID),
        ("money_movement_capability_absent", ReasonCode.CAPABILITY_UNKNOWN),
        ("private_capabilities_ready", ReasonCode.PRIVATE_CAPABILITY_MISSING),
        ("emergency_capability_ready", ReasonCode.EMERGENCY_VENUE_PREFLIGHT_FAILED),
        ("account_modes_permissions_ready", ReasonCode.PREFLIGHT_FAILED),
        ("fees_funding_metadata_ready", ReasonCode.FEE_UNKNOWN),
        ("stable_flat", ReasonCode.FAST_LIVE_ACCOUNT_NOT_FLAT),
        ("zero_open_orders", ReasonCode.UNKNOWN_ORDER_BLOCK),
        ("journal_known_and_reconciled", ReasonCode.RECONCILIATION_INCOMPLETE),
        ("clocks_and_market_data_ready", ReasonCode.MARKET_DATA_PREFLIGHT_FAILED),
        ("executable_depth_ready", ReasonCode.DEPTH_INSUFFICIENT),
        ("history_model_ready", ReasonCode.FAST_LIVE_HISTORY_MODEL_INVALID),
        ("regime_clear", ReasonCode.CALIBRATION_REGIME_SHIFT),
        ("economics_positive", ReasonCode.ECONOMIC_PREFLIGHT_FAILED),
        ("risk_margin_leverage_ready", ReasonCode.RISK_PREFLIGHT_FAILED),
        ("owner_unlock_absent", ReasonCode.FAST_LIVE_OWNER_CONTROL_ACTIVE),
        ("telegram_challenge_absent", ReasonCode.FAST_LIVE_OWNER_CONTROL_ACTIVE),
    ],
)
def test_fast_live_preflight_fails_closed_for_each_blocker(
    field: str,
    reason: ReasonCode,
) -> None:
    report = evaluate_fast_live_preflight(_inputs(**{field: False}))

    assert report.status == "FAIL"
    assert report.reason == reason
    assert (
        validate_fast_live_preflight(report, report.identity)
        == ReasonCode.FAST_LIVE_PREFLIGHT_FAILED
    )


def test_fast_live_preflight_expires_and_identity_change_invalidates() -> None:
    now = datetime(2026, 8, 26, tzinfo=UTC)
    report = evaluate_fast_live_preflight(_inputs(), now=now)

    assert (
        validate_fast_live_preflight(
            report,
            report.identity,
            now=now + timedelta(seconds=601),
        )
        == ReasonCode.FAST_LIVE_PREFLIGHT_EXPIRED
    )
    assert (
        validate_fast_live_preflight(
            report,
            _identity(data_generation_sha256="4" * 64),
            now=now,
        )
        == ReasonCode.FAST_LIVE_PREFLIGHT_IDENTITY_CHANGED
    )


def test_fast_live_preflight_is_persisted_and_consumed_once(tmp_path: Path) -> None:
    now = datetime(2026, 8, 26, tzinfo=UTC)
    path = tmp_path / "fast-live-preflight.json"
    report = evaluate_fast_live_preflight(_inputs(), now=now)
    save_fast_live_preflight(path, report)

    consumed = consume_fast_live_preflight(path, report.identity, "9" * 64, now=now)
    assert consumed.consumed_intent_sha256 == "9" * 64
    assert load_fast_live_preflight(path) == consumed
    assert (
        validate_fast_live_preflight(consumed, report.identity, now=now)
        == ReasonCode.FAST_LIVE_PREFLIGHT_ALREADY_USED
    )
    with pytest.raises(ValueError, match=ReasonCode.FAST_LIVE_PREFLIGHT_ALREADY_USED.value):
        consume_fast_live_preflight(path, report.identity, "8" * 64, now=now)


def test_fast_live_preflight_rejects_tampering(tmp_path: Path) -> None:
    path = tmp_path / "fast-live-preflight.json"
    report = evaluate_fast_live_preflight(_inputs())
    save_fast_live_preflight(path, report)
    payload = path.read_text(encoding="utf-8").replace(
        '"history_days": "30"', '"history_days": "29"'
    )
    path.write_text(payload, encoding="utf-8")

    with pytest.raises(ValueError, match="hash mismatch"):
        load_fast_live_preflight(path)


def test_fast_live_preflight_cannot_be_mutated_to_authorize_execution() -> None:
    report = evaluate_fast_live_preflight(_inputs())
    with pytest.raises(ValueError, match="cannot authorize"):
        replace(report, execution_authorized=True)
