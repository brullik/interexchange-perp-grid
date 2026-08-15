from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
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
            "positions": [{"venue": "okx", "base_quantity": "0.001"}],
            "balances": [{"venue": "okx", "equity_usdt": "100"}],
            "pnl": {"unrealized_pnl_usdt": "0.01"},
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

    for command in ("/status", "/positions", "/pnl", "/balances"):
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
