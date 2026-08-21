from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal, DecimalException
from typing import Protocol, TypedDict

from telegram import Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters

from interexchange_perp_grid.config import Settings
from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.execution import Side
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
    "/risk",
    "/data_health",
    "/balances",
}
TELEGRAM_MESSAGE_LIMIT = 4096
ROUTE_LABEL_LIMIT = 96
MAX_VISIBILITY_DECIMAL = Decimal("1e18")


class LiveControlPlane(Protocol):
    async def snapshot(self) -> dict[str, object]: ...

    async def close_all_live(self) -> LiveControlResult: ...

    async def cancel_all_live(self) -> LiveControlResult: ...

    async def emergency_flatten(self) -> LiveControlResult: ...

    async def kill(self) -> LiveControlResult: ...


class _MutableRouteSummary(TypedDict):
    route: str
    tranche_count: int
    states: dict[str, int]
    paired_quantity: Decimal
    residual_quantity: Decimal
    net_pnl_usdt: Decimal


class _RouteSummary(TypedDict):
    route: str
    tranche_count: int
    states: dict[str, int]
    paired_quantity: str
    residual_quantity: str
    net_pnl_usdt: str


class _PortfolioSummary(TypedDict):
    status: str
    route_count: int
    tranche_count: int
    invalid_position_count: int
    total_net_pnl_usdt: str
    routes: list[_RouteSummary]


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

        response = self._bound_text_response(command, response)
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
            "/risk",
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
                return self._render_response(
                    command,
                    {
                        "source": "SHADOW_FALLBACK",
                        "private_state": ReasonCode.PRIVATE_STATE_UNAVAILABLE.value,
                        "shadow": shadow_payload,
                    },
                )
            key = command.removeprefix("/")
            if command == "/status":
                live_payload: object = self._live_status_summary(live)
            elif command == "/risk":
                live_payload = {
                    "source": live.get("source", "PRIVATE_EXCHANGE"),
                    "risk": self._risk_summary(live.get(key)),
                }
            elif command == "/positions":
                live_payload = self._live_positions_summary(live.get(key))
            elif command == "/pnl":
                live_payload = self._live_pnl_summary(live.get(key))
            else:
                live_payload = live.get(key, {"status": "UNAVAILABLE"})
            return self._render_response(command, live_payload)
        return self._render_response(command, await self._shadow_read_payload(command))

    async def _shadow_read_payload(self, command: str) -> object:
        snapshot = await self._runtime.snapshot()
        market = snapshot.get("market")
        market_payload = market if isinstance(market, dict) else {}
        positions_value = snapshot.get("positions")
        portfolio = self._portfolio_summary(positions_value)
        if command == "/status":
            payload: object = {
                "mode": snapshot.get("mode"),
                "paused": snapshot.get("paused"),
                "killed": snapshot.get("killed"),
                "reconciliation_state": snapshot.get("reconciliation_state"),
                "overloaded": snapshot.get("overloaded"),
                "persistence_indeterminate": snapshot.get("persistence_indeterminate"),
                "risk": self._risk_summary(snapshot.get("risk")),
                "portfolio": portfolio,
                "data_health": market_payload.get("data_health", []),
                "quarantined": market_payload.get("quarantined", []),
            }
        elif command == "/positions":
            payload = portfolio
        elif command == "/pnl":
            payload = {
                "route_pnl": [
                    {
                        "route": item["route"],
                        "net_pnl_usdt": item["net_pnl_usdt"],
                    }
                    for item in portfolio["routes"]
                ],
                "total_net_pnl_usdt": portfolio["total_net_pnl_usdt"],
            }
        elif command == "/risk":
            payload = {"source": "SHADOW", "risk": self._risk_summary(snapshot.get("risk"))}
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

    @staticmethod
    def _portfolio_summary(value: object) -> _PortfolioSummary:
        if not isinstance(value, list):
            return {
                "status": "INVALID_PORTFOLIO_DATA",
                "route_count": 0,
                "tranche_count": 0,
                "invalid_position_count": 1,
                "total_net_pnl_usdt": "UNKNOWN",
                "routes": [],
            }
        positions: list[object] = value
        by_route: dict[str, _MutableRouteSummary] = {}
        total_pnl = Decimal("0")
        invalid = 0
        for value in positions:
            if not isinstance(value, dict):
                invalid += 1
                continue
            if (
                not {
                    "tranche_id",
                    "route",
                    "state",
                    "paired_quantity",
                    "residual_quantity",
                    "net_pnl_usdt",
                }
                <= value.keys()
            ):
                invalid += 1
                continue
            route = TelegramCommandRouter._bounded_label(str(value.get("route", "UNKNOWN")))
            state = str(value.get("state", "UNKNOWN"))
            try:
                paired = Decimal(str(value.get("paired_quantity", "0")))
                residual = Decimal(str(value.get("residual_quantity", "0")))
                pnl = Decimal(str(value.get("net_pnl_usdt", "0")))
            except (DecimalException, ValueError):
                invalid += 1
                continue
            if (
                not paired.is_finite()
                or not residual.is_finite()
                or not pnl.is_finite()
                or paired < 0
                or residual < 0
                or paired.copy_abs() > MAX_VISIBILITY_DECIMAL
                or residual.copy_abs() > MAX_VISIBILITY_DECIMAL
                or pnl.copy_abs() > MAX_VISIBILITY_DECIMAL
            ):
                invalid += 1
                continue
            summary = by_route.setdefault(
                route,
                {
                    "route": route,
                    "tranche_count": 0,
                    "states": {},
                    "paired_quantity": Decimal("0"),
                    "residual_quantity": Decimal("0"),
                    "net_pnl_usdt": Decimal("0"),
                },
            )
            summary["tranche_count"] += 1
            summary["states"][state] = summary["states"].get(state, 0) + 1
            try:
                summary["paired_quantity"] += paired
                summary["residual_quantity"] += residual
                summary["net_pnl_usdt"] += pnl
                total_pnl += pnl
            except DecimalException:
                invalid += 1

        if invalid:
            return {
                "status": "INVALID_PORTFOLIO_DATA",
                "route_count": 0,
                "tranche_count": 0,
                "invalid_position_count": invalid,
                "total_net_pnl_usdt": "UNKNOWN",
                "routes": [],
            }

        routes: list[_RouteSummary] = []
        for route in sorted(by_route):
            summary = by_route[route]
            routes.append(
                {
                    "route": summary["route"],
                    "tranche_count": summary["tranche_count"],
                    "states": summary["states"],
                    "paired_quantity": str(summary["paired_quantity"]),
                    "residual_quantity": str(summary["residual_quantity"]),
                    "net_pnl_usdt": str(summary["net_pnl_usdt"]),
                }
            )
        return {
            "status": "OK",
            "route_count": len(routes),
            "tranche_count": sum(item["tranche_count"] for item in routes),
            "invalid_position_count": 0,
            "total_net_pnl_usdt": str(total_pnl),
            "routes": routes,
        }

    @staticmethod
    def _risk_summary(value: object) -> dict[str, object]:
        if not isinstance(value, dict):
            return {"status": "INVALID_RISK_DATA"}
        invalid_reason = value.get("reason")
        if value.get("status") == "INVALID_RISK_DATA":
            return {
                "status": "INVALID_RISK_DATA",
                "reason": TelegramCommandRouter._bounded_label(
                    invalid_reason
                    if isinstance(invalid_reason, str) and invalid_reason
                    else "RISK_DATA_INVALID"
                ),
            }
        try:
            if not {"reservation_count", "portfolio_stress_usdt"} <= value.keys():
                raise ValueError("risk totals are required")
            reservation_value = value.get("reservation_count", 0)
            if not isinstance(reservation_value, int) or isinstance(reservation_value, bool):
                raise ValueError("reservation count must be an integer")
            reservation_count = reservation_value
            portfolio_stress = Decimal(str(value.get("portfolio_stress_usdt", "0")))
            per_route_value = value.get("per_route_stress_usdt", {})
            if not isinstance(per_route_value, dict):
                raise ValueError("per-route risk must be an object")
            per_route: dict[str, str] = {}
            for route, stress_value in sorted(
                per_route_value.items(), key=lambda item: str(item[0])
            ):
                stress = Decimal(str(stress_value))
                if not stress.is_finite() or stress < 0 or stress > MAX_VISIBILITY_DECIMAL:
                    raise ValueError("route stress must be finite and non-negative")
                per_route[TelegramCommandRouter._bounded_label(str(route))] = str(stress)
            if (
                reservation_count < 0
                or not portfolio_stress.is_finite()
                or portfolio_stress < 0
                or portfolio_stress > MAX_VISIBILITY_DECIMAL
            ):
                raise ValueError("portfolio risk must be finite and non-negative")
        except (DecimalException, TypeError, ValueError):
            return {"status": "INVALID_RISK_DATA"}
        return {
            "status": "OK",
            "scope": TelegramCommandRouter._bounded_label(str(value.get("scope", "RISK_BOOK"))),
            "reservation_count": reservation_count,
            "per_route_stress_usdt": per_route,
            "portfolio_stress_usdt": str(portfolio_stress),
        }

    @staticmethod
    def _bounded_label(value: str) -> str:
        if len(value) <= ROUTE_LABEL_LIMIT:
            return value
        digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
        return f"{value[: ROUTE_LABEL_LIMIT - 15]}...#{digest}"

    @staticmethod
    def _live_positions_summary(value: object) -> dict[str, object]:
        if not isinstance(value, list):
            return {"status": "INVALID_PRIVATE_POSITION_DATA"}
        positions: list[dict[str, str]] = []
        for item in value:
            if (
                not isinstance(item, dict)
                or not {
                    "venue",
                    "symbol",
                    "side",
                    "base_quantity",
                }
                <= item.keys()
            ):
                return {"status": "INVALID_PRIVATE_POSITION_DATA"}
            try:
                quantity = Decimal(str(item["base_quantity"]))
                residual = Decimal(str(item.get("residual_quantity", "0")))
                side = Side(str(item["side"]))
                venue = Venue(str(item["venue"]))
            except (DecimalException, ValueError):
                return {"status": "INVALID_PRIVATE_POSITION_DATA"}
            symbol = str(item["symbol"])
            if (
                not quantity.is_finite()
                or not residual.is_finite()
                or quantity.copy_abs() > MAX_VISIBILITY_DECIMAL
                or quantity <= 0
                or residual < 0
                or residual > MAX_VISIBILITY_DECIMAL
                or not symbol
            ):
                return {"status": "INVALID_PRIVATE_POSITION_DATA"}
            rendered = {
                "venue": venue.value,
                "symbol": TelegramCommandRouter._bounded_label(symbol),
                "side": side.value,
                "base_quantity": str(quantity),
            }
            for key in ("route", "state"):
                if key in item:
                    rendered[key] = TelegramCommandRouter._bounded_label(str(item[key]))
            if "residual_quantity" in item:
                rendered["residual_quantity"] = str(residual)
            positions.append(rendered)
        return {"status": "OK", "position_count": len(positions), "positions": positions}

    @staticmethod
    def _live_pnl_summary(value: object) -> dict[str, object]:
        if not isinstance(value, dict):
            return {"status": "INVALID_PRIVATE_PNL_DATA"}
        rendered: dict[str, object] = {"status": "OK"}
        for key, raw in value.items():
            if "pnl" not in str(key).lower():
                continue
            try:
                amount = Decimal(str(raw))
            except (DecimalException, ValueError):
                return {"status": "INVALID_PRIVATE_PNL_DATA"}
            if not amount.is_finite() or amount.copy_abs() > MAX_VISIBILITY_DECIMAL:
                return {"status": "INVALID_PRIVATE_PNL_DATA"}
            rendered[str(key)] = str(amount)
        if len(rendered) == 1:
            return {"status": "INVALID_PRIVATE_PNL_DATA"}
        return rendered

    @staticmethod
    def _live_status_summary(live: dict[str, object]) -> dict[str, object]:
        balances = live.get("balances")
        return {
            "source": TelegramCommandRouter._bounded_label(
                str(live.get("source", "PRIVATE_EXCHANGE"))
            ),
            "status": live.get("status", {"status": "UNAVAILABLE"}),
            "positions": TelegramCommandRouter._live_positions_summary(live.get("positions")),
            "risk": TelegramCommandRouter._risk_summary(live.get("risk")),
            "pnl": TelegramCommandRouter._live_pnl_summary(live.get("pnl")),
            "balance_count": len(balances) if isinstance(balances, list) else 0,
        }

    @staticmethod
    def _bound_text_response(command: str, response: str) -> str:
        if len(response) <= TELEGRAM_MESSAGE_LIMIT:
            return response
        try:
            payload: object = json.loads(response)
        except json.JSONDecodeError:
            payload = {
                "status": "DETAIL_OMITTED",
                "reason": "TELEGRAM_RESPONSE_TOO_LARGE",
                "text_sha256": hashlib.sha256(response.encode("utf-8")).hexdigest(),
                "original_characters": len(response),
            }
        return TelegramCommandRouter._render_response(command, payload)

    @staticmethod
    def _render_response(command: str, payload: object) -> str:
        encoded = json.dumps(payload, sort_keys=True, default=str)
        if len(encoded) <= TELEGRAM_MESSAGE_LIMIT:
            return encoded
        digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        summary: dict[str, object] = {
            "status": "DETAIL_OMITTED",
            "reason": "TELEGRAM_RESPONSE_TOO_LARGE",
            "command": command,
            "original_characters": len(encoded),
            "payload_sha256": digest,
        }
        if isinstance(payload, dict):
            for key in (
                "source",
                "mode",
                "paused",
                "killed",
                "reconciliation_state",
                "overloaded",
                "persistence_indeterminate",
                "route_count",
                "tranche_count",
                "total_net_pnl_usdt",
                "portfolio_stress_usdt",
                "reservation_count",
            ):
                value = payload.get(key)
                if isinstance(value, (bool, int, float)) or value is None:
                    summary[key] = value
                elif isinstance(value, str):
                    summary[key] = TelegramCommandRouter._bounded_label(value)
            summary["omitted_collections"] = {
                str(key): len(value)
                for key, value in payload.items()
                if isinstance(value, (dict, list, tuple))
            }
            for nested_key in ("risk", "portfolio", "positions", "pnl"):
                nested = payload.get(nested_key)
                if not isinstance(nested, dict):
                    continue
                nested_summary: dict[str, object] = {}
                for key in (
                    "status",
                    "reservation_count",
                    "portfolio_stress_usdt",
                    "position_count",
                    "route_count",
                    "tranche_count",
                    "total_net_pnl_usdt",
                ):
                    value = nested.get(key)
                    if isinstance(value, (bool, int, float)) or value is None:
                        nested_summary[key] = value
                    elif isinstance(value, str):
                        nested_summary[key] = TelegramCommandRouter._bounded_label(value)
                for key, value in nested.items():
                    if isinstance(value, (dict, list, tuple)):
                        nested_summary[f"{key}_count"] = len(value)
                summary[nested_key] = nested_summary
        rendered = json.dumps(summary, sort_keys=True, default=str)
        if len(rendered) > TELEGRAM_MESSAGE_LIMIT:
            raise RuntimeError("bounded Telegram diagnostic exceeds platform limit")
        return rendered

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
