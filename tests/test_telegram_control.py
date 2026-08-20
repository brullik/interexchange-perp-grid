from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.config import load_settings
from interexchange_perp_grid.live_control import LiveControlResult
from interexchange_perp_grid.live_journal import LiveActionState
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.shadow import ShadowRuntime
from interexchange_perp_grid.state import live_confirmation_valid, read_command_audit
from interexchange_perp_grid.telegram_control import TelegramCommandRouter, run_telegram_bot

CONFIG = Path("config/defaults.yaml")


class FakeLiveControl:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def snapshot(self) -> dict[str, object]:
        return {
            "source": "PRIVATE_EXCHANGE",
            "status": {"journal_state": "HEDGED"},
            "positions": [
                {
                    "venue": "okx",
                    "symbol": "BTC/USDT:USDT",
                    "side": "BUY",
                    "base_quantity": "0.001",
                }
            ],
            "balances": [{"venue": "okx", "equity_usdt": "100"}],
            "pnl": {"unrealized_pnl_usdt": "0.01"},
            "risk": {"portfolio_stress_usdt": "1.25", "reservation_count": 2},
        }

    async def close_all_live(self) -> LiveControlResult:
        return self._result("CLOSE_ALL_LIVE")

    async def cancel_all_live(self) -> LiveControlResult:
        return self._result("CANCEL_ALL_LIVE")

    async def emergency_flatten(self) -> LiveControlResult:
        return self._result("EMERGENCY_FLATTEN")

    async def kill(self) -> LiveControlResult:
        return self._result("KILL_CANCEL_FLATTEN")

    def _result(self, action: str) -> LiveControlResult:
        self.calls.append(action)
        return LiveControlResult(True, action, 1, 1, LiveActionState.FLAT, None, None)


class MissingPrivateCredentialsControl(FakeLiveControl):
    async def snapshot(self) -> dict[str, object]:
        raise ValueError("private API key and secret are required")

    async def close_all_live(self) -> LiveControlResult:
        raise ValueError("private API key and secret are required")

    async def cancel_all_live(self) -> LiveControlResult:
        raise ValueError("private API key and secret are required")

    async def emergency_flatten(self) -> LiveControlResult:
        raise ValueError("private API key and secret are required")

    async def kill(self) -> LiveControlResult:
        raise ValueError("private API key and secret are required")


@pytest.mark.asyncio
async def test_owner_allowlist_challenge_controls_and_audit(tmp_path: Path) -> None:
    state_path = tmp_path / "telegram.sqlite3"
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(state_path)})
    runtime = ShadowRuntime(settings)
    await runtime.start()
    router = TelegramCommandRouter(runtime, 42, 60, token_factory=lambda: "ABC123")
    now = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)

    assert await router.handle(7, "/status", now) == ReasonCode.TELEGRAM_UNAUTHORIZED
    assert "reconciliation_state" in await router.handle(42, "/status", now)
    assert await router.handle(42, "/pause", now) == "paused=true"
    assert (await runtime.entry_gate()).reason == ReasonCode.ENTRY_PAUSED
    assert await router.handle(42, "/resume", now) == "paused=false"

    assert await router.handle(42, "/kill", now) == ReasonCode.TELEGRAM_CHALLENGE_REQUIRED
    assert "ABC123" in await router.handle(42, "/challenge", now)
    assert await router.handle(42, "/kill WRONG", now) == ReasonCode.TELEGRAM_CHALLENGE_INVALID
    assert "ABC123" in await router.handle(42, "/challenge", now + timedelta(seconds=1))
    assert await router.handle(42, "/kill ABC123", now + timedelta(seconds=2)) == (
        "killed=true paused=true"
    )
    assert (await runtime.entry_gate()).reason == ReasonCode.KILL_SWITCH_ACTIVE

    assert "ABC123" in await router.handle(42, "/challenge", now + timedelta(seconds=3))
    confirmation = await router.handle(
        42,
        "/confirm_live ABC123",
        now + timedelta(seconds=4),
    )
    assert "live_confirmed_until" in confirmation
    assert await live_confirmation_valid(
        state_path,
        now + timedelta(seconds=30),
    )
    assert not await live_confirmation_valid(
        state_path,
        now + timedelta(seconds=120),
    )

    audits = await read_command_audit(state_path)
    assert len(audits) == 11
    assert audits[0].reason == ReasonCode.TELEGRAM_UNAUTHORIZED
    assert audits[-1].reason == ReasonCode.TELEGRAM_COMMAND_ACCEPTED


@pytest.mark.asyncio
async def test_live_commands_use_private_state_and_challenge_protected_workflows(
    tmp_path: Path,
) -> None:
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(tmp_path / "telegram.sqlite3")})
    runtime = ShadowRuntime(settings)
    await runtime.start()
    live = FakeLiveControl()
    router = TelegramCommandRouter(
        runtime,
        42,
        60,
        token_factory=lambda: "LIVE42",
        live_control=live,
    )
    now = datetime(2026, 8, 15, 12, 0, tzinfo=UTC)

    assert "PRIVATE_EXCHANGE" in await router.handle(42, "/status", now)
    assert "base_quantity" in await router.handle(42, "/positions", now)
    assert "equity_usdt" in await router.handle(42, "/balances", now)
    assert "unrealized_pnl_usdt" in await router.handle(42, "/pnl", now)
    live_risk = await router.handle(42, "/risk", now)
    assert "PRIVATE_EXCHANGE" in live_risk
    assert "1.25" in live_risk

    for command, expected in (
        ("/cancel_all_live", "CANCEL_ALL_LIVE"),
        ("/close_all_live", "CLOSE_ALL_LIVE"),
        ("/emergency_flatten", "EMERGENCY_FLATTEN"),
        ("/kill", "KILL_CANCEL_FLATTEN"),
    ):
        assert await router.handle(42, command, now) == (ReasonCode.TELEGRAM_CHALLENGE_REQUIRED)
        await router.handle(42, "/challenge", now)
        response = await router.handle(42, f"{command} LIVE42", now)
        assert expected in response
    assert live.calls == [
        "CANCEL_ALL_LIVE",
        "CLOSE_ALL_LIVE",
        "EMERGENCY_FLATTEN",
        "KILL_CANCEL_FLATTEN",
    ]


@pytest.mark.asyncio
async def test_shadow_telegram_without_token_stays_fail_closed_and_nonfatal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("IPEG_TELEGRAM_BOT_TOKEN", raising=False)
    settings = load_settings(
        CONFIG,
        {
            "IPEG_STATE_PATH": str(tmp_path / "telegram.sqlite3"),
            "IPEG_TELEGRAM_ENABLED": "true",
        },
    )
    runtime = ShadowRuntime(settings)
    await runtime.start()
    stop_event = asyncio.Event()

    task = asyncio.create_task(run_telegram_bot(settings, runtime, stop_event))
    await asyncio.sleep(0)

    assert task.done() is False
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)


@pytest.mark.asyncio
async def test_missing_private_exchange_credentials_fall_back_to_shadow_and_never_crash(
    tmp_path: Path,
) -> None:
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(tmp_path / "telegram.sqlite3")})
    runtime = ShadowRuntime(settings)
    await runtime.start()
    router = TelegramCommandRouter(
        runtime,
        42,
        60,
        token_factory=lambda: "NO_KEYS",
        live_control=MissingPrivateCredentialsControl(),
    )
    now = datetime(2026, 8, 15, 12, 0, tzinfo=UTC)

    for command in ("/status", "/positions", "/pnl", "/risk", "/balances"):
        payload = await router.handle(42, command, now)
        assert "PRIVATE_STATE_UNAVAILABLE" in payload
        assert "SHADOW_FALLBACK" in payload

    for command in ("/cancel_all_live", "/close_all_live", "/emergency_flatten"):
        await router.handle(42, "/challenge", now)
        payload = await router.handle(42, f"{command} NO_KEYS", now)
        assert '"success": false' in payload
        assert "PRIVATE_STATE_UNAVAILABLE" in payload

    await router.handle(42, "/challenge", now)
    killed = await router.handle(42, "/kill NO_KEYS", now)
    assert '"killed": true' in killed
    assert '"success": false' in killed
    assert "PRIVATE_STATE_UNAVAILABLE" in killed


@pytest.mark.asyncio
async def test_shadow_commands_render_bounded_ten_route_portfolio_risk_and_pnl(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(tmp_path / "telegram.sqlite3")})
    runtime = ShadowRuntime(settings)
    await runtime.start()
    positions = [
        {
            "tranche_id": f"t-{route_index}-{tranche_index}",
            "route": f"A{route_index:03d}:bybit>okx",
            "state": "HEDGED",
            "paired_quantity": "0.001",
            "residual_quantity": "0",
            "net_pnl_usdt": "0.10",
        }
        for route_index in range(10)
        for tranche_index in range(5)
    ]

    async def snapshot() -> dict[str, object]:
        return {
            "mode": "shadow",
            "paused": False,
            "killed": False,
            "reconciliation_state": "CONSISTENT",
            "overloaded": False,
            "persistence_indeterminate": False,
            "risk": {
                "reservation_count": 50,
                "per_route_stress_usdt": {f"A{index:03d}:bybit>okx": "5" for index in range(10)},
                "portfolio_stress_usdt": "50",
            },
            "positions": positions,
            "market": {"data_health": [], "quarantined": []},
        }

    monkeypatch.setattr(runtime, "snapshot", snapshot)
    router = TelegramCommandRouter(runtime, 42, 60)
    now = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)

    status = await router.handle(42, "/status", now)
    positions_payload = await router.handle(42, "/positions", now)
    pnl = await router.handle(42, "/pnl", now)
    risk = await router.handle(42, "/risk", now)

    assert len(status) < 4096
    assert len(positions_payload) < 4096
    assert '"route_count": 10' in positions_payload
    assert '"tranche_count": 50' in positions_payload
    assert '"portfolio_stress_usdt": "50"' in risk
    assert '"reservation_count": 50' in risk
    assert f'"total_net_pnl_usdt": "{Decimal("0.10") * 50}"' in pnl


@pytest.mark.asyncio
async def test_shadow_visibility_bounds_long_routes_and_rejects_malformed_numbers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(tmp_path / "telegram.sqlite3")})
    runtime = ShadowRuntime(settings)
    await runtime.start()
    long_routes = [f"BASE{index}-{'X' * 500}:bybit>okx" for index in range(10)]
    positions = [
        {
            "tranche_id": f"t-{route_index}-{tranche_index}",
            "route": route,
            "state": "HEDGED" if tranche_index % 2 == 0 else "PARTIAL",
            "paired_quantity": "0.001",
            "residual_quantity": "0.0001",
            "net_pnl_usdt": "-0.10" if tranche_index == 0 else "0.20",
        }
        for route_index, route in enumerate(long_routes)
        for tranche_index in range(5)
    ]
    risk = {
        "reservation_count": 50,
        "per_route_stress_usdt": {route: "5" for route in long_routes},
        "portfolio_stress_usdt": "50",
    }

    async def snapshot() -> dict[str, object]:
        return {
            "mode": "shadow",
            "paused": False,
            "killed": False,
            "reconciliation_state": "CONSISTENT",
            "overloaded": False,
            "persistence_indeterminate": False,
            "risk": risk,
            "positions": positions,
            "market": {"data_health": [], "quarantined": []},
        }

    monkeypatch.setattr(runtime, "snapshot", snapshot)
    router = TelegramCommandRouter(runtime, 42, 60)
    now = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
    for command in ("/status", "/positions", "/pnl", "/risk"):
        response = await router.handle(42, command, now)
        assert len(response) <= 4096
        assert "#" in response

    positions[0]["net_pnl_usdt"] = "NaN"
    malformed = await router.handle(42, "/positions", now)
    assert "INVALID_PORTFOLIO_DATA" in malformed
    assert '"invalid_position_count": 1' in malformed
    risk["portfolio_stress_usdt"] = "NaN"
    malformed_risk = await router.handle(42, "/risk", now)
    assert "INVALID_RISK_DATA" in malformed_risk
    audits = await read_command_audit(runtime.state_path)
    assert len(audits) == 6

    risk.clear()
    risk.update(
        {
            "status": "INVALID_RISK_DATA",
            "reason": "PRIVATE_POSITION_JOURNAL_MISMATCH",
        }
    )
    reasoned_risk = await router.handle(42, "/risk", now)
    assert "PRIVATE_POSITION_JOURNAL_MISMATCH" in reasoned_risk


@pytest.mark.asyncio
async def test_live_visibility_oversize_is_bounded_and_explicitly_omits_detail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(tmp_path / "telegram.sqlite3")})
    runtime = ShadowRuntime(settings)
    await runtime.start()
    live = FakeLiveControl()

    async def oversized_snapshot() -> dict[str, object]:
        return {
            "source": "PRIVATE_EXCHANGE",
            "positions": [
                {
                    "venue": "okx",
                    "symbol": "BTC/USDT:USDT",
                    "side": "BUY",
                    "base_quantity": "0.001",
                    "route": "X" * 1000,
                }
                for _ in range(50)
            ],
            "pnl": {"unrealized_pnl_usdt": "1"},
            "risk": {
                "reservation_count": 50,
                "portfolio_stress_usdt": "50",
                "per_route_stress_usdt": {f"route-{index}": "5" for index in range(10)},
            },
        }

    monkeypatch.setattr(live, "snapshot", oversized_snapshot)
    router = TelegramCommandRouter(runtime, 42, 60, live_control=live)
    now = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)

    status = await router.handle(42, "/status", now)
    positions = await router.handle(42, "/positions", now)
    assert len(status) <= 4096
    assert len(positions) <= 4096
    assert "DETAIL_OMITTED" in status
    assert "DETAIL_OMITTED" in positions
    assert '"position_count": 50' in status


@pytest.mark.asyncio
async def test_all_live_outputs_are_bounded_and_malformed_private_state_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(tmp_path / "telegram.sqlite3")})
    runtime = ShadowRuntime(settings)
    await runtime.start()
    live = FakeLiveControl()

    async def malformed_snapshot() -> dict[str, object]:
        return {
            "source": "PRIVATE_EXCHANGE",
            "status": {"journal_state": "HEDGED"},
            "positions": [
                {
                    "venue": "okx",
                    "symbol": "BTC/USDT:USDT",
                    "side": "BUY",
                    "base_quantity": "NaN",
                    "residual_quantity": "-1",
                }
            ],
            "pnl": {"unrealized_pnl_usdt": "Infinity"},
            "risk": {},
            "balances": [],
        }

    monkeypatch.setattr(live, "snapshot", malformed_snapshot)
    router = TelegramCommandRouter(
        runtime,
        42,
        60,
        token_factory=lambda: "BOUND",
        live_control=live,
    )
    now = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
    assert "INVALID_PRIVATE_POSITION_DATA" in await router.handle(42, "/positions", now)
    assert "INVALID_PRIVATE_PNL_DATA" in await router.handle(42, "/pnl", now)
    status = await router.handle(42, "/status", now)
    assert "INVALID_PRIVATE_POSITION_DATA" in status
    assert "INVALID_PRIVATE_PNL_DATA" in status
    assert "INVALID_RISK_DATA" in status

    malformed = await malformed_snapshot()
    malformed["positions"] = [
        {
            "venue": "okx",
            "symbol": "BTC/USDT:USDT",
            "side": "BUY",
            "base_quantity": "-1",
        }
    ]

    async def negative_quantity_snapshot() -> dict[str, object]:
        return malformed

    monkeypatch.setattr(live, "snapshot", negative_quantity_snapshot)
    assert "INVALID_PRIVATE_POSITION_DATA" in await router.handle(42, "/positions", now)

    malformed["positions"] = [
        {
            "venue": "evil",
            "symbol": "BTC/USDT:USDT",
            "side": "BUY",
            "base_quantity": "1",
        }
    ]
    assert "INVALID_PRIVATE_POSITION_DATA" in await router.handle(42, "/positions", now)

    def oversized_result(action: str) -> LiveControlResult:
        return LiveControlResult(
            True,
            action,
            1,
            1,
            LiveActionState.FLAT,
            None,
            "X" * 5000,
        )

    monkeypatch.setattr(live, "_result", oversized_result)
    for command in ("/cancel_all_live", "/close_all_live", "/emergency_flatten", "/kill"):
        await router.handle(42, "/challenge", now)
        response = await router.handle(42, f"{command} BOUND", now)
        assert len(response) <= 4096
        assert "DETAIL_OMITTED" in response


@pytest.mark.asyncio
async def test_extreme_shadow_decimals_and_empty_records_fail_closed_and_are_audited(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(tmp_path / "telegram.sqlite3")})
    runtime = ShadowRuntime(settings)
    await runtime.start()

    async def snapshot() -> dict[str, object]:
        return {
            "mode": "shadow",
            "risk": {},
            "positions": [
                {
                    "tranche_id": "extreme",
                    "route": "BTC:bybit>okx",
                    "state": "HEDGED",
                    "paired_quantity": "1e999999999",
                    "residual_quantity": "0",
                    "net_pnl_usdt": "0",
                },
                {},
            ],
            "market": {},
        }

    monkeypatch.setattr(runtime, "snapshot", snapshot)
    router = TelegramCommandRouter(runtime, 42, 60)
    now = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
    positions = await router.handle(42, "/positions", now)
    risk = await router.handle(42, "/risk", now)
    assert "INVALID_PORTFOLIO_DATA" in positions
    assert "INVALID_RISK_DATA" in risk
    audits = await read_command_audit(runtime.state_path)
    assert len(audits) == 2

    async def invalid_container_snapshot() -> dict[str, object]:
        return {"mode": "shadow", "risk": {}, "positions": "corrupt", "market": {}}

    monkeypatch.setattr(runtime, "snapshot", invalid_container_snapshot)
    assert "INVALID_PORTFOLIO_DATA" in await router.handle(42, "/positions", now)
