from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Protocol

from interexchange_perp_grid.config import Settings
from interexchange_perp_grid.domain import Instrument, Venue
from interexchange_perp_grid.execution import (
    EMERGENCY_PURPOSES,
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
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.safety import LiveContext, evaluate_live_order
from interexchange_perp_grid.strategy import DirectedRouteKey


def protected_ioc_price(
    side: Side,
    marginal_worst_price: Decimal,
    tick_size: Decimal,
    slippage_cap_bps: Decimal,
) -> Decimal:
    """Return the side-aware IOC cap from the marginal consumed book level, not VWAP."""
    if marginal_worst_price <= 0 or tick_size <= 0:
        raise ValueError("marginal price and tick size must be positive")
    if slippage_cap_bps < 0 or slippage_cap_bps > Decimal(10_000):
        raise ValueError("slippage cap must be between zero and 10000 bps")
    ratio = slippage_cap_bps / Decimal(10_000)
    if side == Side.BUY:
        raw = marginal_worst_price * (Decimal(1) + ratio)
        ticks = (raw / tick_size).to_integral_value(rounding=ROUND_CEILING)
    else:
        raw = marginal_worst_price * (Decimal(1) - ratio)
        ticks = (raw / tick_size).to_integral_value(rounding=ROUND_FLOOR)
    protected = ticks * tick_size
    if protected <= 0:
        raise ValueError("protected price must remain positive")
    return protected


def translate_protected_order(
    intent: ExecutionIntent,
    instrument: Instrument,
) -> VenueOrderRequest:
    if intent.venue != instrument.venue:
        raise ValueError("execution intent venue does not match instrument")
    contracts = intent.quantity / instrument.contract_size_base
    if contracts % instrument.amount_step_contracts != 0:
        raise ValueError("execution quantity does not align to the venue amount step")
    if contracts < instrument.minimum_amount_contracts:
        raise ValueError("execution quantity is below the venue minimum")
    is_emergency_market = intent.unbounded_market and intent.purpose in EMERGENCY_PURPOSES
    order_type = "market" if is_emergency_market else "limit"
    price = None if is_emergency_market else intent.worst_acceptable_price
    time_in_force = None if is_emergency_market else "IOC"
    params: dict[str, object] = {}
    if intent.venue == Venue.BYBIT:
        params.update({"orderLinkId": intent.client_order_id, "positionIdx": 0})
    elif intent.venue == Venue.OKX:
        params.update({"clOrdId": intent.client_order_id, "tdMode": "cross"})
    elif intent.venue == Venue.BITGET:
        params.update(
            {
                "clientOid": intent.client_order_id,
                "productType": "USDT-FUTURES",
                "marginMode": "cross",
                "marginCoin": "USDT",
            }
        )
    else:
        params["newClientOrderId"] = intent.client_order_id
    if time_in_force is not None:
        if intent.venue == Venue.BITGET:
            params["force"] = time_in_force
        else:
            params["timeInForce"] = time_in_force
    if intent.purpose in {
        OrderPurpose.NORMAL_CLOSE,
        OrderPurpose.EMERGENCY_CLOSE,
        OrderPurpose.LIQUIDATION_PREVENTION,
    }:
        params["reduceOnly"] = True
    return VenueOrderRequest(
        venue=intent.venue,
        client_order_id=intent.client_order_id,
        symbol=instrument.symbol,
        side=intent.side,
        order_type=order_type,
        amount_contracts=contracts,
        price=price,
        time_in_force=time_in_force,
        params=params,
    )


class OrderAdapter(Protocol):
    async def submit_order(
        self,
        request: VenueOrderRequest,
        instrument: Instrument,
    ) -> PrivateOrder: ...

    async def find_order_by_client_id(
        self,
        client_order_id: str,
        instrument: Instrument,
    ) -> PrivateOrder | None: ...

    async def fetch_positions(
        self,
        instrument: Instrument,
    ) -> tuple[PositionSnapshot, ...]: ...


class IdempotentOrderExecutor:
    """Never resubmits an unknown client ID; reconciliation is the only next action."""

    def __init__(self, adapter: OrderAdapter) -> None:
        self._adapter = adapter
        self._attempted: dict[str, VenueOrderRequest] = {}
        self._known: dict[str, PrivateOrder] = {}

    async def execute(
        self,
        intent: ExecutionIntent,
        instrument: Instrument,
    ) -> PrivateOrder:
        request = translate_protected_order(intent, instrument)
        known = self._known.get(intent.client_order_id)
        if known is not None:
            return known
        attempted = self._attempted.get(intent.client_order_id)
        if attempted is not None:
            if attempted != request:
                raise ValueError("client order ID cannot be reused with a different request")
            return await self.reconcile(intent, instrument)

        existing = await self._adapter.find_order_by_client_id(
            intent.client_order_id,
            instrument,
        )
        if existing is not None:
            self._known[intent.client_order_id] = existing
            return existing
        self._attempted[intent.client_order_id] = request
        try:
            submitted = await self._adapter.submit_order(request, instrument)
        except (TimeoutError, ConnectionError):
            return await self.reconcile(intent, instrument)
        if submitted.status != PrivateOrderStatus.UNKNOWN:
            self._known[intent.client_order_id] = submitted
        return submitted

    async def reconcile(
        self,
        intent: ExecutionIntent,
        instrument: Instrument,
    ) -> PrivateOrder:
        await self._adapter.fetch_positions(instrument)
        found = await self._adapter.find_order_by_client_id(intent.client_order_id, instrument)
        if found is not None:
            self._known[intent.client_order_id] = found
            return found
        return PrivateOrder(
            venue=intent.venue,
            order_id=None,
            client_order_id=intent.client_order_id,
            symbol=instrument.symbol,
            side=intent.side,
            status=PrivateOrderStatus.UNKNOWN,
            requested_base_quantity=intent.quantity,
            filled_base_quantity=Decimal(0),
            average_price=None,
            fee_usdt=None,
            observed_at=_utc_now(),
        )


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class PrivatePreflightInput:
    capability: PrivateCapabilityReport
    account: AccountSnapshot
    instrument: Instrument
    fee_rate: Decimal | None
    funding_known: bool
    clock_skew_ms: int | None
    maximum_clock_skew_ms: int
    symbol_available: bool
    data_quality_passed: bool
    reconciliation_passed: bool
    risk_passed: bool
    free_margin_floor_ratio: Decimal


@dataclass(frozen=True, slots=True)
class PrivatePreflightReport:
    passed: bool
    reason: ReasonCode | None
    checks: dict[str, bool]


def run_private_preflight(inputs: PrivatePreflightInput) -> PrivatePreflightReport:
    account = inputs.account
    permissions = {permission.lower() for permission in account.permissions}
    free_ratio = (
        account.free_margin_usdt / account.equity_usdt if account.equity_usdt > 0 else Decimal(0)
    )
    checks = {
        "capability": inputs.capability.ready,
        "account_mode": account.margin_mode == "cross",
        "position_mode": account.position_mode == "oneway",
        "trading_permission": (
            "trade" in permissions
            and "withdraw" not in permissions
            and "transfer" not in permissions
            and "wallet" not in permissions
        ),
        "credential_restrictions": (
            account.withdrawal_enabled is False and account.transfer_enabled is False
        ),
        "api_trading": account.trading_enabled is True,
        "symbol": inputs.symbol_available,
        "fee": inputs.fee_rate is not None and inputs.fee_rate >= 0,
        "funding": inputs.funding_known,
        "clock": inputs.clock_skew_ms is not None
        and abs(inputs.clock_skew_ms) <= inputs.maximum_clock_skew_ms,
        "data_quality": inputs.data_quality_passed,
        "reconciliation": inputs.reconciliation_passed,
        "risk": inputs.risk_passed,
        "local_margin": free_ratio >= inputs.free_margin_floor_ratio,
    }
    reasons = (
        ("capability", ReasonCode.PRIVATE_CAPABILITY_MISSING),
        ("account_mode", ReasonCode.ACCOUNT_MODE_INVALID),
        ("position_mode", ReasonCode.POSITION_MODE_INVALID),
        ("trading_permission", ReasonCode.TRADING_PERMISSION_MISSING),
        ("credential_restrictions", ReasonCode.TRADING_PERMISSION_MISSING),
        ("api_trading", ReasonCode.API_TRADING_UNAVAILABLE),
        ("symbol", ReasonCode.SYMBOL_UNAVAILABLE),
        ("fee", ReasonCode.FEE_UNKNOWN),
        ("funding", ReasonCode.FUNDING_UNKNOWN),
        ("clock", ReasonCode.CLOCK_SKEW_EXCEEDED),
        ("data_quality", ReasonCode.MARKET_DATA_PREFLIGHT_FAILED),
        ("reconciliation", ReasonCode.RECONCILIATION_INCOMPLETE),
        ("risk", ReasonCode.RISK_PREFLIGHT_FAILED),
        ("local_margin", ReasonCode.MARGIN_INSUFFICIENT),
    )
    reason = next((reason for name, reason in reasons if not checks[name]), None)
    return PrivatePreflightReport(reason is None, reason, checks)


@dataclass(frozen=True, slots=True)
class CanaryAction:
    route: DirectedRouteKey
    tranche_count: int
    notional_usdt: Decimal
    minimum_valid_notional_usdt: Decimal
    projected_stressed_loss_usdt: Decimal
    maximum_effective_leverage: Decimal
    minimum_stressed_free_margin_ratio: Decimal
    existing_position_count: int
    existing_open_order_count: int


@dataclass(frozen=True, slots=True)
class CanaryPolicy:
    base: str
    route: DirectedRouteKey
    pair_stressed_loss_limit_usdt: Decimal = Decimal("1")
    effective_leverage_cap: Decimal = Decimal("3")
    free_margin_floor_ratio: Decimal = Decimal("0.20")

    def evaluate(self, action: CanaryAction) -> tuple[bool, ReasonCode | None]:
        passed = (
            action.route == self.route
            and action.route.base == self.base
            and action.tranche_count == 1
            and action.notional_usdt == action.minimum_valid_notional_usdt
            and action.minimum_valid_notional_usdt > 0
            and 0 < action.projected_stressed_loss_usdt <= self.pair_stressed_loss_limit_usdt
            and action.maximum_effective_leverage <= self.effective_leverage_cap
            and action.minimum_stressed_free_margin_ratio >= self.free_margin_floor_ratio
            and action.existing_position_count == 0
            and action.existing_open_order_count == 0
        )
        return (True, None) if passed else (False, ReasonCode.CANARY_POLICY_VIOLATION)


@dataclass(frozen=True, slots=True)
class LivePairResult:
    submitted: bool
    reason: ReasonCode | None
    long_order: PrivateOrder | None
    short_order: PrivateOrder | None


class LiveCanaryExecutor:
    """The sole guarded boundary that may call private order submission."""

    def __init__(
        self,
        settings: Settings,
        policy: CanaryPolicy,
        long_executor: IdempotentOrderExecutor,
        short_executor: IdempotentOrderExecutor,
    ) -> None:
        self._settings = settings
        self._policy = policy
        self._long_executor = long_executor
        self._short_executor = short_executor

    async def submit_pair(
        self,
        action: CanaryAction,
        live_context: LiveContext,
        long_intent: ExecutionIntent,
        short_intent: ExecutionIntent,
        long_instrument: Instrument,
        short_instrument: Instrument,
    ) -> LivePairResult:
        policy_passed, policy_reason = self._policy.evaluate(action)
        if not policy_passed:
            return LivePairResult(False, policy_reason, None, None)
        canary_venues = {
            Venue(value)
            for value in (
                self._settings.venues.canary_primary + self._settings.venues.canary_alternate
            )
        }
        if {
            action.route.long_venue,
            action.route.short_venue,
            long_intent.venue,
            short_intent.venue,
        } - canary_venues:
            return LivePairResult(False, ReasonCode.CANARY_POLICY_VIOLATION, None, None)
        decision = evaluate_live_order(self._settings, live_context)
        if not decision.allowed:
            return LivePairResult(False, decision.reason, None, None)
        long_order, short_order = await asyncio.gather(
            self._long_executor.execute(long_intent, long_instrument),
            self._short_executor.execute(short_intent, short_instrument),
        )
        return LivePairResult(True, None, long_order, short_order)
