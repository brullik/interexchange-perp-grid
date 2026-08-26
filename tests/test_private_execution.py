from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.adapters.bingx_swap import SequenceQualifiedBingxExchange
from interexchange_perp_grid.adapters.bitget_classic import ClassicBitgetExchange
from interexchange_perp_grid.adapters.kucoin_classic import ClassicKucoinFuturesExchange
from interexchange_perp_grid.config import Settings, load_settings
from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.execution import (
    ExecutionIntent,
    OrderPurpose,
    Side,
)
from interexchange_perp_grid.private_domain import (
    AccountSnapshot,
    PositionSnapshot,
    PrivateCapabilityReport,
    PrivateOrder,
    PrivateOrderStatus,
    VenueOrderRequest,
)
from interexchange_perp_grid.private_execution import (
    CanaryAction,
    CanaryPolicy,
    IdempotentOrderExecutor,
    LiveCanaryExecutor,
    PrivatePreflightInput,
    protected_ioc_price,
    run_private_preflight,
    translate_protected_order,
)
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.safety import LiveContext
from interexchange_perp_grid.strategy import DirectedRouteKey


def instrument(venue: Venue) -> Instrument:
    return Instrument(
        venue,
        "BTC/USDT:USDT",
        "BTCUSDT",
        "BTC",
        "USDT",
        "USDT",
        Decimal("0.01"),
        Decimal("1"),
        Decimal("0.1"),
        Decimal("1"),
        Decimal("5"),
        Decimal("0.0005"),
        "fixture",
    )


@pytest.mark.parametrize(
    ("venue", "client_key"),
    [
        (Venue.BYBIT, "orderLinkId"),
        (Venue.OKX, "clOrdId"),
        (Venue.BINANCE_USDM, "newClientOrderId"),
        (Venue.BITGET, "clientOid"),
        (Venue.KUCOIN_FUTURES, "clientOid"),
        (Venue.BINGX, "clientOrderId"),
    ],
)
def test_protected_ioc_translation_is_contract_tested_per_venue(
    venue: Venue,
    client_key: str,
) -> None:
    intent = ExecutionIntent(
        "client-1",
        venue,
        Side.BUY,
        OrderPurpose.NORMAL_OPEN,
        Decimal("0.10"),
        Decimal("101"),
    )
    request = translate_protected_order(intent, instrument(venue))
    assert request.order_type == "limit"
    assert request.time_in_force == "IOC"
    assert request.price == Decimal("101")
    assert request.amount_contracts == Decimal("10")
    assert request.params[client_key] == "client-1"
    if venue != Venue.BITGET:
        assert request.params["timeInForce"] == "IOC"
    if venue == Venue.OKX:
        assert request.params["tdMode"] == "cross"
    if venue == Venue.BITGET:
        assert request.params["productType"] == "USDT-FUTURES"
        assert request.params["marginMode"] == "cross"
        assert request.params["marginCoin"] == "USDT"
        assert request.params["force"] == "IOC"
        assert "timeInForce" not in request.params
    if venue == Venue.KUCOIN_FUTURES:
        assert request.params["marginMode"] == "cross"


def test_pinned_kucoin_futures_rest_request_keeps_cross_ioc_client_id() -> None:
    exchange = ClassicKucoinFuturesExchange({})
    exchange.set_markets(
        [
            {
                "id": "XBTUSDTM",
                "symbol": "BTC/USDT:USDT",
                "base": "BTC",
                "quote": "USDT",
                "settle": "USDT",
                "settleId": "USDT",
                "type": "swap",
                "spot": False,
                "margin": False,
                "swap": True,
                "future": False,
                "option": False,
                "contract": True,
                "linear": True,
                "inverse": False,
                "active": True,
                "precision": {"amount": 1, "price": 0.1},
                "limits": {"amount": {"min": 1}, "cost": {"min": 1}},
                "contractSize": 0.01,
                "info": {},
            }
        ]
    )
    intent = ExecutionIntent(
        "kucoin-client",
        Venue.KUCOIN_FUTURES,
        Side.BUY,
        OrderPurpose.NORMAL_OPEN,
        Decimal("0.10"),
        Decimal("101"),
    )
    translated = translate_protected_order(intent, instrument(Venue.KUCOIN_FUTURES))

    request = exchange.create_contract_order_request(
        translated.symbol,
        translated.order_type,
        translated.side.value.lower(),
        float(translated.amount_contracts),
        float(translated.price) if translated.price is not None else None,
        translated.params,
    )

    assert request["clientOid"] == "kucoin-client"
    assert request["marginMode"] == "CROSS"
    assert request["timeInForce"] == "IOC"


def test_pinned_bitget_rest_request_maps_unified_cross_to_classic_crossed() -> None:
    exchange = ClassicBitgetExchange({})
    exchange.set_markets(
        [
            {
                "id": "BTCUSDT",
                "symbol": "BTC/USDT:USDT",
                "base": "BTC",
                "quote": "USDT",
                "settle": "USDT",
                "settleId": "USDT",
                "type": "swap",
                "spot": False,
                "margin": False,
                "swap": True,
                "future": False,
                "option": False,
                "contract": True,
                "linear": True,
                "inverse": False,
                "active": True,
                "precision": {"amount": 1, "price": 0.1},
                "limits": {
                    "amount": {"min": 1, "max": None},
                    "price": {"min": None, "max": None},
                    "cost": {"min": None, "max": None},
                    "leverage": {"min": None, "max": None},
                },
                "contractSize": 0.01,
                "info": {},
            }
        ]
    )
    intent = ExecutionIntent(
        "bitget-client",
        Venue.BITGET,
        Side.BUY,
        OrderPurpose.NORMAL_OPEN,
        Decimal("0.10"),
        Decimal("101"),
    )
    translated = translate_protected_order(intent, instrument(Venue.BITGET))

    request = exchange.create_order_request(
        translated.symbol,
        translated.order_type,
        translated.side.value.lower(),
        float(translated.amount_contracts),
        float(translated.price) if translated.price is not None else None,
        translated.params,
    )

    assert request["clientOid"] == "bitget-client"
    assert request["marginMode"] == "crossed"
    assert request["force"] == "IOC"
    assert request["productType"] == "USDT-FUTURES"
    assert request["marginCoin"] == "USDT"
    assert "timeInForce" not in request


def test_pinned_bingx_rest_request_keeps_protected_ioc_client_id() -> None:
    exchange = SequenceQualifiedBingxExchange({})
    exchange.set_markets(
        [
            {
                "id": "BTC-USDT",
                "symbol": "BTC/USDT:USDT",
                "base": "BTC",
                "quote": "USDT",
                "settle": "USDT",
                "settleId": "USDT",
                "type": "swap",
                "spot": False,
                "margin": False,
                "swap": True,
                "future": False,
                "option": False,
                "contract": True,
                "linear": True,
                "inverse": False,
                "active": True,
                "precision": {"amount": 0.0001, "price": 0.1},
                "limits": {
                    "amount": {"min": 0.0001, "max": None},
                    "price": {"min": None, "max": None},
                    "cost": {"min": 2, "max": None},
                    "leverage": {"min": None, "max": None},
                },
                "contractSize": 1,
                "info": {},
            }
        ]
    )
    intent = ExecutionIntent(
        "bingx-client",
        Venue.BINGX,
        Side.BUY,
        OrderPurpose.NORMAL_OPEN,
        Decimal("0.10"),
        Decimal("101"),
    )
    translated = translate_protected_order(intent, instrument(Venue.BINGX))

    request = exchange.create_order_request(
        translated.symbol,
        translated.order_type,
        translated.side.value.lower(),
        float(translated.amount_contracts),
        float(translated.price) if translated.price is not None else None,
        translated.params,
    )

    assert request["clientOrderID"] == "bingx-client"
    assert request["timeInForce"] == "IOC"
    assert request["positionSide"] == "BOTH"
    assert request["price"] == 101


def test_protected_price_uses_marginal_level_side_cap_and_tick_rounding() -> None:
    assert protected_ioc_price(
        Side.BUY,
        Decimal("101.03"),
        Decimal("0.1"),
        Decimal("5"),
    ) == Decimal("101.1")
    assert protected_ioc_price(
        Side.SELL,
        Decimal("99.97"),
        Decimal("0.1"),
        Decimal("5"),
    ) == Decimal("99.9")


def test_emergency_market_translation_is_explicit_and_close_is_reduce_only() -> None:
    intent = ExecutionIntent(
        "emergency-close",
        Venue.OKX,
        Side.SELL,
        OrderPurpose.EMERGENCY_CLOSE,
        Decimal("0.10"),
        None,
        True,
    )
    request = translate_protected_order(intent, instrument(Venue.OKX))
    assert request.order_type == "market"
    assert request.price is None
    assert request.params["reduceOnly"] is True


class UnknownThenReconciledAdapter:
    def __init__(self) -> None:
        self.submit_calls = 0
        self.find_calls = 0
        self.position_calls = 0
        self.reconciled: PrivateOrder | None = None

    async def submit_order(
        self,
        request: VenueOrderRequest,
        selected: Instrument,
    ) -> PrivateOrder:
        del request, selected
        self.submit_calls += 1
        raise TimeoutError("result unknown")

    async def find_order_by_client_id(
        self,
        client_order_id: str,
        selected: Instrument,
    ) -> PrivateOrder | None:
        del client_order_id, selected
        self.find_calls += 1
        return self.reconciled

    async def fetch_positions(self, selected: Instrument) -> tuple[PositionSnapshot, ...]:
        del selected
        self.position_calls += 1
        return ()


@pytest.mark.asyncio
async def test_unknown_result_reconciles_without_duplicate_submission() -> None:
    adapter = UnknownThenReconciledAdapter()
    executor = IdempotentOrderExecutor(adapter)
    intent = ExecutionIntent(
        "same-client-id",
        Venue.BYBIT,
        Side.BUY,
        OrderPurpose.NORMAL_OPEN,
        Decimal("0.10"),
        Decimal("101"),
    )
    selected = instrument(Venue.BYBIT)
    unknown = await executor.execute(intent, selected)
    assert unknown.status == PrivateOrderStatus.UNKNOWN
    assert adapter.submit_calls == 1

    adapter.reconciled = PrivateOrder(
        Venue.BYBIT,
        "order-1",
        intent.client_order_id,
        selected.symbol,
        Side.BUY,
        PrivateOrderStatus.PARTIAL,
        intent.quantity,
        Decimal("0.04"),
        Decimal("100"),
        Decimal("0.01"),
        datetime.now(UTC),
    )
    reconciled = await executor.execute(intent, selected)
    retry = await executor.execute(intent, selected)
    assert reconciled is retry
    assert reconciled.filled_base_quantity == Decimal("0.04")
    assert adapter.submit_calls == 1
    assert adapter.position_calls >= 2


def ready_capability() -> PrivateCapabilityReport:
    return PrivateCapabilityReport(
        Venue.BYBIT,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        datetime.now(UTC),
        (),
    )


def test_private_preflight_checks_every_account_and_runtime_gate() -> None:
    account = AccountSnapshot(
        Venue.BYBIT,
        Decimal("100"),
        Decimal("80"),
        "cross",
        "oneway",
        True,
        ("trade",),
        datetime.now(UTC),
        False,
        False,
    )
    inputs = PrivatePreflightInput(
        ready_capability(),
        account,
        instrument(Venue.BYBIT),
        Decimal("0.0005"),
        True,
        10,
        1000,
        True,
        True,
        True,
        True,
        Decimal("0.20"),
    )
    report = run_private_preflight(inputs)
    assert report.passed is True
    assert all(report.checks.values())

    unknown_restrictions = run_private_preflight(
        replace(inputs, account=replace(account, transfer_enabled=None))
    )
    assert unknown_restrictions.passed is False
    assert unknown_restrictions.checks["credential_restrictions"] is False
    assert unknown_restrictions.reason == ReasonCode.TRADING_PERMISSION_MISSING

    unsafe_account = AccountSnapshot(
        Venue.BYBIT,
        Decimal("100"),
        Decimal("80"),
        "isolated",
        "hedge",
        True,
        ("trade", "withdraw", "transfer"),
        datetime.now(UTC),
        True,
        True,
    )
    rejected = run_private_preflight(
        PrivatePreflightInput(
            ready_capability(),
            unsafe_account,
            instrument(Venue.BYBIT),
            Decimal("0.0005"),
            True,
            10,
            1000,
            True,
            True,
            True,
            True,
            Decimal("0.20"),
        )
    )
    assert rejected.reason == ReasonCode.ACCOUNT_MODE_INVALID
    assert rejected.checks["trading_permission"] is False


def test_canary_policy_allows_only_one_minimum_tranche_on_exact_route() -> None:
    route = DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX)
    policy = CanaryPolicy("BTC", route)
    safe = CanaryAction(
        route,
        1,
        Decimal("5"),
        Decimal("5"),
        Decimal("0.8"),
        Decimal("2"),
        Decimal("0.50"),
        0,
        0,
    )
    assert policy.evaluate(safe) == (
        True,
        None,
    )
    reverse = DirectedRouteKey("BTC", Venue.OKX, Venue.BYBIT)
    assert policy.evaluate(replace(safe, route=reverse)) == (
        False,
        ReasonCode.CANARY_POLICY_VIOLATION,
    )
    assert policy.evaluate(replace(safe, tranche_count=2))[0] is False
    assert policy.evaluate(replace(safe, notional_usdt=Decimal("10")))[0] is False
    assert policy.evaluate(replace(safe, projected_stressed_loss_usdt=Decimal("1.01")))[0] is False
    assert policy.evaluate(replace(safe, existing_position_count=1))[0] is False


class FilledAdapter:
    def __init__(self) -> None:
        self.submit_calls = 0

    async def find_order_by_client_id(
        self,
        client_order_id: str,
        selected: Instrument,
    ) -> PrivateOrder | None:
        del client_order_id, selected
        return None

    async def fetch_positions(self, selected: Instrument) -> tuple[PositionSnapshot, ...]:
        del selected
        return ()

    async def submit_order(
        self,
        request: VenueOrderRequest,
        selected: Instrument,
    ) -> PrivateOrder:
        self.submit_calls += 1
        return PrivateOrder(
            request.venue,
            f"order-{request.client_order_id}",
            request.client_order_id,
            request.symbol,
            request.side,
            PrivateOrderStatus.FILLED,
            request.amount_contracts * selected.contract_size_base,
            request.amount_contracts * selected.contract_size_base,
            request.price,
            Decimal("0.01"),
            datetime.now(UTC),
        )


@pytest.mark.asyncio
async def test_live_canary_submission_is_physically_behind_every_guard() -> None:
    settings = load_settings(Path("config/defaults.yaml"))
    raw = settings.model_dump(mode="json")
    raw["app"]["mode"] = "live"
    raw["live"]["enabled"] = True
    live_settings = Settings.model_validate(raw)
    route = DirectedRouteKey("BTC", Venue.BYBIT, Venue.OKX)
    long_adapter = FilledAdapter()
    short_adapter = FilledAdapter()
    executor = LiveCanaryExecutor(
        live_settings,
        CanaryPolicy("BTC", route),
        IdempotentOrderExecutor(long_adapter),
        IdempotentOrderExecutor(short_adapter),
    )
    action = CanaryAction(
        route,
        1,
        Decimal("10"),
        Decimal("10"),
        Decimal("0.8"),
        Decimal("2"),
        Decimal("0.50"),
        0,
        0,
    )
    long_intent = ExecutionIntent(
        "live-long",
        Venue.BYBIT,
        Side.BUY,
        OrderPurpose.NORMAL_OPEN,
        Decimal("0.10"),
        Decimal("101"),
    )
    short_intent = ExecutionIntent(
        "live-short",
        Venue.OKX,
        Side.SELL,
        OrderPurpose.NORMAL_OPEN,
        Decimal("0.10"),
        Decimal("99"),
    )
    denied = await executor.submit_pair(
        action,
        LiveContext(),
        long_intent,
        short_intent,
        instrument(Venue.BYBIT),
        instrument(Venue.OKX),
    )
    assert denied.submitted is False
    assert long_adapter.submit_calls == 0
    assert short_adapter.submit_calls == 0

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
    submitted = await executor.submit_pair(
        action,
        complete_context,
        long_intent,
        short_intent,
        instrument(Venue.BYBIT),
        instrument(Venue.OKX),
    )
    assert submitted.submitted is True
    assert submitted.long_order is not None
    assert submitted.short_order is not None
    assert long_adapter.submit_calls == 1
    assert short_adapter.submit_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "candidate_venue",
    [Venue.BITGET, Venue.KUCOIN_FUTURES, Venue.BINGX, Venue.MEXC],
)
async def test_expansion_code_candidate_cannot_expand_live_canary_allowlist(
    candidate_venue: Venue,
) -> None:
    settings = load_settings(Path("config/defaults.yaml"))
    raw = settings.model_dump(mode="json")
    raw["app"]["mode"] = "live"
    raw["live"]["enabled"] = True
    live_settings = Settings.model_validate(raw)
    route = DirectedRouteKey("BTC", candidate_venue, Venue.OKX)
    long_adapter = FilledAdapter()
    short_adapter = FilledAdapter()
    executor = LiveCanaryExecutor(
        live_settings,
        CanaryPolicy("BTC", route),
        IdempotentOrderExecutor(long_adapter),
        IdempotentOrderExecutor(short_adapter),
    )
    action = CanaryAction(
        route,
        1,
        Decimal("5"),
        Decimal("5"),
        Decimal("0.8"),
        Decimal("2"),
        Decimal("0.50"),
        0,
        0,
    )
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

    denied = await executor.submit_pair(
        action,
        context,
        ExecutionIntent(
            "candidate-long",
            candidate_venue,
            Side.BUY,
            OrderPurpose.NORMAL_OPEN,
            Decimal("0.10"),
            Decimal("101"),
        ),
        ExecutionIntent(
            "okx-short",
            Venue.OKX,
            Side.SELL,
            OrderPurpose.NORMAL_OPEN,
            Decimal("0.10"),
            Decimal("99"),
        ),
        instrument(candidate_venue),
        instrument(Venue.OKX),
    )

    assert denied.submitted is False
    assert denied.reason == ReasonCode.CANARY_POLICY_VIOLATION
    assert long_adapter.submit_calls == short_adapter.submit_calls == 0
