from __future__ import annotations

import asyncio
import json
import os
import secrets
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Protocol

from telegram import Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters

from interexchange_perp_grid.config import Settings
from interexchange_perp_grid.live_control import LiveControlResult, render_control_result
from interexchange_perp_grid.observability import get_logger
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.shadow import ShadowRuntime
from interexchange_perp_grid.state import record_command_audit, record_live_confirmation

DANGEROUS_COMMANDS = {
    "/close_all_simulated",
    "/close_all_live",
    "/cancel_all_live",
    "/emergency_flatten",
    "/kill",
    "/confirm_live",
}
READ_COMMANDS = {
    "/status",
    "/opportunities",
    "/positions",
    "/pnl",
    "/data_health",
    "/balances",
}


class LiveControlPlane(Protocol):
    async def snapshot(self) -> dict[str, object]: ...

    async def close_all_live(self) -> LiveControlResult: ...

    async def cancel_all_live(self) -> LiveControlResult: ...

    async def emergency_flatten(self) -> LiveControlResult: ...

    async def kill(self) -> LiveControlResult: ...


class TelegramCommandRouter:
    def __init__(
        self,
        runtime: ShadowRuntime,
        owner_chat_id: int,
        challenge_ttl_seconds: int,
        token_factory: Callable[[], str] | None = None,
        live_control: LiveControlPlane | None = None,
    ) -> None:
        self._runtime = runtime
        self._owner_chat_id = owner_chat_id
        self._ttl = challenge_ttl_seconds
        self._token_factory = token_factory or (lambda: secrets.token_hex(3).upper())
        self._challenge: tuple[str, datetime] | None = None
        self._live_control = live_control

    async def handle(
        self,
        chat_id: int,
        text: str,
        now: datetime | None = None,
    ) -> str:
        observed_at = now or datetime.now(UTC)
        command, *arguments = text.strip().split()
        command = command.lower().split("@", maxsplit=1)[0]
        actor = str(chat_id)
        if chat_id != self._owner_chat_id:
            await self._audit(
                actor,
                command or "<empty>",
                "DENIED",
                ReasonCode.TELEGRAM_UNAUTHORIZED,
                observed_at,
            )
            return ReasonCode.TELEGRAM_UNAUTHORIZED.value

        if command == "/challenge":
            token = self._token_factory()
            self._challenge = (token, observed_at + timedelta(seconds=self._ttl))
            await self._audit(
                actor,
                command,
                "ACCEPTED",
                ReasonCode.TELEGRAM_COMMAND_ACCEPTED,
                observed_at,
            )
            return f"challenge={token} expires_in={self._ttl}s"

        if command in DANGEROUS_COMMANDS and not self._consume_challenge(arguments, observed_at):
            reason = (
                ReasonCode.TELEGRAM_CHALLENGE_REQUIRED
                if not arguments
                else ReasonCode.TELEGRAM_CHALLENGE_INVALID
            )
            await self._audit(actor, command, "DENIED", reason, observed_at)
            return reason.value

        if command in READ_COMMANDS:
            response = await self._read_response(command)
        elif command == "/pause":
            await self._runtime.pause()
            response = "paused=true"
        elif command == "/resume":
            await self._runtime.resume()
            response = "paused=false"
        elif command == "/close_all_simulated":
            closed = await self._runtime.close_all_simulated()
            response = f"closed_simulated={len(closed)} paused=true"
        elif command == "/close_all_live":
            response = await self._live_response("close_all_live")
        elif command == "/cancel_all_live":
            response = await self._live_response("cancel_all_live")
        elif command == "/emergency_flatten":
            response = await self._live_response("emergency_flatten")
        elif command == "/kill":
            await self._runtime.kill()
            if self._live_control is None:
                response = "killed=true paused=true"
            else:
                try:
                    result = await self._live_control.kill()
                    live_workflow = json.loads(render_control_result(result))
                except Exception as error:
                    get_logger().warning(
                        "telegram_private_state_unavailable",
                        operation="kill",
                        error_type=type(error).__name__,
                    )
                    live_workflow = self._private_unavailable("kill")
                response = json.dumps(
                    {"killed": True, "paused": True, "live_workflow": live_workflow},
                    sort_keys=True,
                )
        elif command == "/confirm_live":
            confirmed_until = observed_at + timedelta(seconds=self._ttl)
            await record_live_confirmation(
                self._runtime.state_path,
                actor,
                confirmed_until,
                observed_at,
            )
            response = f"live_confirmed_until={confirmed_until.isoformat()}"
        else:
            await self._audit(
                actor,
                command or "<empty>",
                "DENIED",
                ReasonCode.PREFLIGHT_FAILED,
                observed_at,
            )
            return "UNKNOWN_COMMAND"

        await self._audit(
            actor,
            command,
            "ACCEPTED",
            ReasonCode.TELEGRAM_COMMAND_ACCEPTED,
            observed_at,
        )
        return response

    def _consume_challenge(self, arguments: list[str], now: datetime) -> bool:
        if len(arguments) != 1 or self._challenge is None:
            return False
        expected, expires_at = self._challenge
        self._challenge = None
        return now <= expires_at and secrets.compare_digest(arguments[0], expected)

    async def _read_response(self, command: str) -> str:
        if self._live_control is not None and command in {
            "/status",
            "/positions",
            "/pnl",
            "/balances",
        }:
            try:
                live = await self._live_control.snapshot()
            except Exception as error:
                get_logger().warning(
                    "telegram_private_state_unavailable",
                    operation=command.removeprefix("/"),
                    error_type=type(error).__name__,
                )
                shadow_payload = await self._shadow_read_payload(command)
                return json.dumps(
                    {
                        "source": "SHADOW_FALLBACK",
                        "private_state": ReasonCode.PRIVATE_STATE_UNAVAILABLE.value,
                        "shadow": shadow_payload,
                    },
                    sort_keys=True,
                    default=str,
                )
            key = command.removeprefix("/")
            live_payload = (
                live if command == "/status" else live.get(key, {"status": "UNAVAILABLE"})
            )
            return json.dumps(live_payload, sort_keys=True, default=str)
        return json.dumps(
            await self._shadow_read_payload(command),
            sort_keys=True,
            default=str,
        )

    async def _shadow_read_payload(self, command: str) -> object:
        snapshot = await self._runtime.snapshot()
        market = snapshot.get("market")
        market_payload = market if isinstance(market, dict) else {}
        positions_value = snapshot.get("positions")
        positions = positions_value if isinstance(positions_value, list) else []
        if command == "/status":
            payload: object = snapshot
        elif command == "/positions":
            payload = positions
        elif command == "/pnl":
            payload = {
                "positions": [
                    {
                        "tranche_id": item["tranche_id"],
                        "net_pnl_usdt": item["net_pnl_usdt"],
                    }
                    for item in positions
                    if isinstance(item, dict)
                ]
            }
        elif command == "/opportunities":
            payload = market_payload.get("opportunities", [])
        elif command == "/data_health":
            payload = {
                "data_health": market_payload.get("data_health", []),
                "quarantined": market_payload.get("quarantined", []),
            }
        else:
            payload = market_payload.get("balances", {"status": "UNAVAILABLE"})
        return payload

    async def _live_response(self, operation: str) -> str:
        if self._live_control is None:
            return json.dumps(
                {"success": False, "operation": operation, "reason": "LIVE_CONTROL_UNAVAILABLE"},
                sort_keys=True,
            )
        try:
            if operation == "close_all_live":
                result = await self._live_control.close_all_live()
            elif operation == "cancel_all_live":
                result = await self._live_control.cancel_all_live()
            elif operation == "emergency_flatten":
                result = await self._live_control.emergency_flatten()
            else:
                raise ValueError(f"unsupported live control operation: {operation}")
        except Exception as error:
            get_logger().warning(
                "telegram_private_state_unavailable",
                operation=operation,
                error_type=type(error).__name__,
            )
            return json.dumps(self._private_unavailable(operation), sort_keys=True)
        return render_control_result(result)

    @staticmethod
    def _private_unavailable(operation: str) -> dict[str, object]:
        return {
            "success": False,
            "operation": operation,
            "reason": ReasonCode.PRIVATE_STATE_UNAVAILABLE.value,
        }

    async def _audit(
        self,
        actor: str,
        command: str,
        outcome: str,
        reason: ReasonCode,
        now: datetime,
    ) -> None:
        await record_command_audit(
            self._runtime.state_path,
            actor,
            command,
            outcome,
            reason,
            now,
        )


async def run_telegram_bot(
    settings: Settings,
    runtime: ShadowRuntime,
    stop_event: asyncio.Event,
    live_control: LiveControlPlane | None = None,
) -> None:
    if not settings.telegram.enabled:
        return
    token = os.environ.get("IPEG_TELEGRAM_BOT_TOKEN", "")
    owner_chat_id = settings.telegram.owner_chat_id
    if not token or owner_chat_id is None:
        if settings.app.mode == "shadow":
            get_logger().warning(
                "telegram_shadow_fallback",
                reason="runtime_credentials_missing",
            )
            await stop_event.wait()
            return
        raise RuntimeError("Telegram is enabled but runtime credentials are missing")
    router = TelegramCommandRouter(
        runtime,
        owner_chat_id,
        settings.telegram.challenge_ttl_seconds,
        live_control=live_control,
    )

    async def on_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        if update.effective_chat is None or update.effective_message is None:
            return
        text = update.effective_message.text or ""
        response = await router.handle(update.effective_chat.id, text)
        await update.effective_message.reply_text(response)

    application = Application.builder().token(token).build()
    application.add_handler(MessageHandler(filters.COMMAND, on_command))
    if application.updater is None:
        raise RuntimeError("Telegram polling updater is unavailable")
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    try:
        await stop_event.wait()
    finally:
        await application.updater.stop()
        await application.stop()
        await application.shutdown()
