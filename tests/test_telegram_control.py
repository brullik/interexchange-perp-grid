from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from interexchange_perp_grid.config import load_settings
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.shadow import ShadowRuntime
from interexchange_perp_grid.state import live_confirmation_valid, read_command_audit
from interexchange_perp_grid.telegram_control import TelegramCommandRouter

CONFIG = Path("config/defaults.yaml")


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
