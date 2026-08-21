from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.config import load_settings
from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.live_control import LiveControlResult
from interexchange_perp_grid.live_journal import LiveActionState
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.shadow import ShadowRuntime
from interexchange_perp_grid.state import (
    finalize_qualification_epoch,
    live_confirmation_valid,
    read_command_audit,
    save_shadow_snapshot,
    start_qualification_epoch,
)
from interexchange_perp_grid.strategy import DirectedRouteKey
from interexchange_perp_grid.telegram_control import (
    READ_COMMANDS,
    TelegramCommandRouter,
    run_telegram_bot,
)

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
            "orders": [
                {
                    "record_type": "PRIVATE_ORDER",
                    "source": "PRIVATE_EXCHANGE",
                    "venue": "okx",
                    "client_order_id": "IPEG-ORDER-1",
                    "symbol": "BTC/USDT:USDT",
                    "side": "BUY",
                    "status": "OPEN",
                    "requested_base_quantity": "0.001",
                    "filled_base_quantity": "0",
                    "observed_at": "2026-08-15T12:00:00+00:00",
                    "is_open": True,
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
    assert "IPEG-ORDER-1" in await router.handle(42, "/orders", now)
    assert "PRIVATE_EXCHANGE" in await router.handle(42, "/orders", now)
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
async def test_all_locked_read_commands_are_available_audited_and_fail_closed(
    tmp_path: Path,
) -> None:
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(tmp_path / "telegram.sqlite3")})
    runtime = ShadowRuntime(settings)
    await runtime.start()
    router = TelegramCommandRouter(runtime, 42, 60)
    now = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
    locked_commands = {
        "/status",
        "/health",
        "/opportunities",
        "/routes",
        "/positions",
        "/orders",
        "/pnl",
        "/risk",
        "/data_health",
        "/exchanges",
        "/qualification",
    }

    assert locked_commands <= READ_COMMANDS
    for command in sorted(locked_commands):
        payload = json.loads(await router.handle(42, command, now))
        assert payload["source"] == "SHADOW"
        assert payload["private_state"] == ReasonCode.PRIVATE_STATE_UNAVAILABLE.value

    health = json.loads(await router.handle(42, "/health", now))
    assert health["service"]["healthy"] is False
    assert health["service"]["reason"] == ReasonCode.SERVICE_STATE_MISSING.value
    qualification = json.loads(await router.handle(42, "/qualification", now))
    assert qualification["status"] == "NOT_RUNNING"
    assert qualification["reason"] == "QUALIFICATION_EPOCH_UNAVAILABLE"
    exchanges = json.loads(await router.handle(42, "/exchanges", now))
    assert exchanges["capability_matrix"] is None
    orders = json.loads(await router.handle(42, "/orders", now))
    assert orders["status"] == "SHADOW_ONLY"
    assert orders["orders"] == []

    audits = await read_command_audit(runtime.state_path)
    assert len(audits) == len(locked_commands) + 4
    assert all(audit.outcome == "ACCEPTED" for audit in audits)
    assert all(audit.reason == ReasonCode.TELEGRAM_COMMAND_ACCEPTED for audit in audits)


@pytest.mark.asyncio
async def test_qualification_and_exchange_visibility_is_durable_and_freshness_gated(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "telegram.sqlite3"
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(state_path)})
    runtime = ShadowRuntime(settings)
    await runtime.start()
    started_at = datetime(2026, 8, 19, 12, 0, tzinfo=UTC)
    epoch = await start_qualification_epoch(
        state_path,
        DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX),
        "a" * 40,
        "b" * 64,
        "c" * 64,
        "sha256:" + "d" * 64,
        started_at,
    )
    await finalize_qualification_epoch(state_path, epoch.epoch_id, started_at + timedelta(days=1))
    await save_shadow_snapshot(
        state_path,
        {
            "evaluated_at": datetime(2020, 1, 1, tzinfo=UTC).isoformat(),
            "venue_capability_matrix": {"okx": {"status": "QUALIFIED"}},
            "quarantined": [],
        },
        started_at,
    )
    router = TelegramCommandRouter(runtime, 42, 60)
    now = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)

    qualification = json.loads(await router.handle(42, "/qualification", now))
    assert qualification["status"] == "FINALIZED"
    assert qualification["epoch_id"] == epoch.epoch_id
    exchanges = json.loads(await router.handle(42, "/exchanges", now))
    assert exchanges["status"] == "STALE"
    assert exchanges["reason"] == "CAPABILITY_MATRIX_STALE"
    assert exchanges["capability_matrix"] is None


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
async def test_read_command_has_one_bounded_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(tmp_path / "telegram.sqlite3")})
    runtime = ShadowRuntime(settings)
    await runtime.start()

    release = asyncio.Event()

    async def blocked_snapshot() -> dict[str, object]:
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                continue
        return {"mode": "shadow", "risk": {}, "positions": [], "market": {}}

    monkeypatch.setattr(runtime, "snapshot", blocked_snapshot)
    monkeypatch.setattr(
        "interexchange_perp_grid.telegram_control.TELEGRAM_READ_DEADLINE_SECONDS", 0.01
    )
    router = TelegramCommandRouter(runtime, 42, 60)
    response = json.loads(await asyncio.wait_for(router.handle(42, "/status"), timeout=0.25))
    assert response["status"] == "UNAVAILABLE"
    assert response["reason"] == "TELEGRAM_READ_DEADLINE"
    assert len(router._retiring_reads) == 1
    with pytest.raises(RuntimeError, match=r"read task.*nonterminal"):
        await router.close(0.01)
    release.set()
    await asyncio.sleep(0.01)
    assert not router._retiring_reads
    await router.close(0.01)


@pytest.mark.asyncio
async def test_read_deadline_includes_mandatory_audit_persistence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "telegram.sqlite3"
    settings = load_settings(CONFIG, {"IPEG_STATE_PATH": str(state_path)})
    runtime = ShadowRuntime(settings)
    await runtime.start()
    router = TelegramCommandRouter(runtime, 42, 60)
    monkeypatch.setattr(
        "interexchange_perp_grid.telegram_control.TELEGRAM_READ_DEADLINE_SECONDS", 0.05
    )
    locker = sqlite3.connect(state_path, isolation_level=None)
    locker.execute("BEGIN IMMEDIATE")
    started = asyncio.get_running_loop().time()
    try:
        response = await asyncio.wait_for(router.handle(42, "/status"), timeout=0.20)
    finally:
        locker.rollback()
        locker.close()
    assert response == "AUDIT_PERSISTENCE_UNAVAILABLE"
    assert asyncio.get_running_loop().time() - started < 0.15


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
            "market": {
                "data_health": [],
                "quarantined": [],
                "opportunities": [{"blob": "Y" * 10000}],
            },
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
            "market": {
                "data_health": [],
                "quarantined": [],
                "opportunities": [{"blob": "Y" * 10000}],
            },
        }

    monkeypatch.setattr(runtime, "snapshot", snapshot)
    router = TelegramCommandRouter(runtime, 42, 60)
    now = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
    for command in ("/status", "/positions", "/pnl", "/risk"):
        response = await router.handle(42, command, now)
        assert len(response) <= 4096
        assert "#" in response
    routes = await router.handle(42, "/routes", now)
    assert len(routes) <= 4096
    for route in long_routes:
        assert TelegramCommandRouter._bounded_label(route) in routes

    positions[0]["net_pnl_usdt"] = "NaN"
    malformed = await router.handle(42, "/positions", now)
    assert "INVALID_PORTFOLIO_DATA" in malformed
    assert '"invalid_position_count": 1' in malformed
    risk["portfolio_stress_usdt"] = "NaN"
    malformed_risk = await router.handle(42, "/risk", now)
    assert "INVALID_RISK_DATA" in malformed_risk
    audits = await read_command_audit(runtime.state_path)
    assert len(audits) == 7

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
            "orders": [
                {
                    "record_type": "PRIVATE_ORDER",
                    "source": "PRIVATE_EXCHANGE",
                    "venue": "okx",
                    "client_order_id": f"IPEG-VERY-LONG-ORDER-{index}-{'Z' * 100}",
                    "symbol": "BTC/USDT:USDT",
                    "side": "BUY",
                    "status": "OPEN",
                    "requested_base_quantity": "0.001",
                    "filled_base_quantity": "0",
                    "observed_at": "2026-08-20T12:00:00+00:00",
                    "is_open": True,
                }
                for index in range(101)
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
    orders = await router.handle(42, "/orders", now)
    assert len(status) <= 4096
    assert len(positions) <= 4096
    assert len(orders) <= 4096
    assert "DETAIL_OMITTED" in status
    assert "DETAIL_OMITTED" in positions
    assert "DETAIL_OMITTED" in orders
    assert '"position_count": 50' in status
    assert '"shown_order_ref_count": 101' in orders
    assert '"omitted_order_ref_count": 0' in orders
    for index in range(101):
        identifier = f"IPEG-VERY-LONG-ORDER-{index}-{'Z' * 100}"
        assert (
            TelegramCommandRouter._compact_reference(
                TelegramCommandRouter._bounded_label(identifier)
            )
            in orders
        )
    all_refs: list[str] = []
    for index in range(101):
        identifier = f"IPEG-VERY-LONG-ORDER-{index}-{'Z' * 100}"
        compact = TelegramCommandRouter._compact_reference(
            TelegramCommandRouter._bounded_label(identifier)
        )
        all_refs.append(f"{compact}:OPEN")
    expected_digest = hashlib.sha256(
        json.dumps(all_refs, separators=(",", ":")).encode()
    ).hexdigest()
    assert expected_digest in orders


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


def test_live_order_visibility_rejects_incomplete_and_cross_wired_records() -> None:
    malformed = (
        {"status": "OPEN"},
        {
            "record_type": "PRIVATE_ORDER",
            "source": "LIVE_JOURNAL",
            "status": "OPEN",
        },
        {
            "record_type": "JOURNAL_LEG",
            "source": "PRIVATE_EXCHANGE",
            "status": "SUBMITTING",
        },
    )
    for record in malformed:
        assert TelegramCommandRouter._live_orders_summary([record]) == {
            "source": "PRIVATE_EXCHANGE",
            "status": "INVALID_PRIVATE_ORDER_DATA",
        }


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
