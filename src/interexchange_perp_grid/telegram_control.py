from __future__ import annotations

import asyncio
import json
import os
import secrets
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from telegram import Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters

from interexchange_perp_grid.config import Settings
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.shadow import ShadowRuntime
from interexchange_perp_grid.state import record_command_audit

DANGEROUS_COMMANDS = {"/close_all_simulated", "/kill"}
READ_COMMANDS = {
    "/status",
    "/opportunities",
    "/positions",
    "/pnl",
    "/data_health",
    "/balances",
}


class TelegramCommandRouter:
    def __init__(
        self,
        runtime: ShadowRuntime,
        owner_chat_id: int,
        challenge_ttl_seconds: int,
        token_factory: Callable[[], str] | None = None,
    ) -> None:
        self._runtime = runtime
        self._owner_chat_id = owner_chat_id
        self._ttl = challenge_ttl_seconds
        self._token_factory = token_factory or (lambda: secrets.token_hex(3).upper())
        self._challenge: tuple[str, datetime] | None = None

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
        elif command == "/kill":
            await self._runtime.kill()
            response = "killed=true paused=true"
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
        return json.dumps(payload, sort_keys=True, default=str)

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
) -> None:
    if not settings.telegram.enabled:
        return
    token = os.environ.get("IPEG_TELEGRAM_BOT_TOKEN", "")
    owner_chat_id = settings.telegram.owner_chat_id
    if not token or owner_chat_id is None:
        raise RuntimeError("Telegram is enabled but runtime credentials are missing")
    router = TelegramCommandRouter(
        runtime,
        owner_chat_id,
        settings.telegram.challenge_ttl_seconds,
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
