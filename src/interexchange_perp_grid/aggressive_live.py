from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from decimal import Decimal
from enum import StrEnum

from interexchange_perp_grid.aggressive_qualification import AggressiveQualificationBinding
from interexchange_perp_grid.aggressive_runtime import AggressiveTrancheIntent
from interexchange_perp_grid.client_ids import venue_client_order_id
from interexchange_perp_grid.domain import Instrument, ProductType, Venue
from interexchange_perp_grid.execution import ExecutionIntent, OrderPurpose, Side
from interexchange_perp_grid.live_coordinator import CanaryExecutionPlan
from interexchange_perp_grid.private_execution import translate_protected_order
from interexchange_perp_grid.strategy import DirectedRouteKey


class AggressiveLaptopLiveStage(StrEnum):
    CANARY = "canary"
    PILOT_A = "pilot_a"


@dataclass(frozen=True, slots=True)
class AggressiveLiveStageLimits:
    maximum_level: int
    route_hard_loss_usdt: Decimal
    portfolio_hard_loss_usdt: Decimal


_LIMITS = {
    AggressiveLaptopLiveStage.CANARY: AggressiveLiveStageLimits(1, Decimal(1), Decimal(1)),
    AggressiveLaptopLiveStage.PILOT_A: AggressiveLiveStageLimits(5, Decimal(5), Decimal(5)),
}


def aggressive_intent_sha256(intent: AggressiveTrancheIntent) -> str:
    encoded = json.dumps(
        asdict(intent),
        default=str,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def prepare_aggressive_live_plan(
    intent: AggressiveTrancheIntent,
    binding: AggressiveQualificationBinding,
    long_instrument: Instrument,
    short_instrument: Instrument,
    *,
    long_protected_price: Decimal,
    short_protected_price: Decimal,
    stage: AggressiveLaptopLiveStage,
    timeout_seconds: int,
) -> CanaryExecutionPlan:
    """Translate one shared strategy intent into the existing journal-first live plan.

    This function cannot submit and cannot authorize execution. Owner, Telegram, live guard,
    private preflight, journal, reconciliation and supervisor gates remain downstream.
    """
    limits = _LIMITS[stage]
    if intent.execution_authorized or not binding.accepted or binding.execution_authorized:
        raise ValueError("aggressive strategy artifacts cannot authorize execution")
    if timeout_seconds <= 0:
        raise ValueError("aggressive live timeout must be positive")
    if intent.level_index > limits.maximum_level:
        raise ValueError("aggressive level exceeds the selected laptop stage")
    if not (
        Decimal(0) < intent.projected_route_loss_usdt <= limits.route_hard_loss_usdt
        and Decimal(0) < intent.projected_portfolio_loss_usdt <= limits.portfolio_hard_loss_usdt
    ):
        raise ValueError("aggressive intent exceeds the selected laptop risk stage")
    identities = (
        (intent.model_sha256, binding.model_sha256),
        (intent.strategy_profile_sha256, binding.profile_sha256),
        (intent.source_manifest_sha256, binding.source_manifest_sha256),
        (intent.reference_manifest_sha256, binding.reference_manifest_sha256),
        (intent.runtime_manifest_sha256, binding.decision_runtime_sha256),
    )
    if any(actual != expected for actual, expected in identities):
        raise ValueError("aggressive live intent identity does not match qualification")
    route = DirectedRouteKey(intent.base, Venue(intent.long_venue), Venue(intent.short_venue))
    if route.value != intent.route_identity:
        raise ValueError("aggressive live route identity is inconsistent")
    qualified = _parse_route(binding.qualification_route)
    if route.base != qualified.base or {route.long_venue, route.short_venue} != {
        qualified.long_venue,
        qualified.short_venue,
    }:
        raise ValueError("aggressive live route is outside the qualified venue pair")
    _validate_instrument(intent, long_instrument, route.long_venue, intent.long_symbol)
    _validate_instrument(intent, short_instrument, route.short_venue, intent.short_symbol)
    _validate_notional(intent.quantity, long_protected_price, long_instrument)
    _validate_notional(intent.quantity, short_protected_price, short_instrument)

    pair_action_id = intent.intent_id
    long_client_id = venue_client_order_id(pair_action_id, "long")
    short_client_id = venue_client_order_id(pair_action_id, "short")
    long_request = translate_protected_order(
        ExecutionIntent(
            long_client_id,
            route.long_venue,
            Side.BUY,
            OrderPurpose.NORMAL_OPEN,
            intent.quantity,
            long_protected_price,
        ),
        long_instrument,
    )
    short_request = translate_protected_order(
        ExecutionIntent(
            short_client_id,
            route.short_venue,
            Side.SELL,
            OrderPurpose.NORMAL_OPEN,
            intent.quantity,
            short_protected_price,
        ),
        short_instrument,
    )
    return CanaryExecutionPlan(
        pair_action_id=pair_action_id,
        route=route,
        tranche_id=f"{pair_action_id}-level-{intent.level_index}",
        quantity=intent.quantity,
        long_request=long_request,
        short_request=short_request,
        risk_reservation={
            "strategy": "AGGRESSIVE_SYMBIOSIS_V1",
            "stage": stage.value,
            "level_index": intent.level_index,
            "decision_cycle": intent.decision_cycle,
            "projected_stress_usdt": intent.projected_route_loss_usdt,
            "projected_portfolio_loss_usdt": intent.projected_portfolio_loss_usdt,
            "target_exit_spread_bps": intent.reverse_target_bps,
            "effective_stop_bps": intent.effective_stop_bps,
            "aggressive_intent_sha256": aggressive_intent_sha256(intent),
            "aggressive_binding_sha256": binding.binding_sha256,
            "opening_client_order_ids": {
                "long": long_client_id,
                "short": short_client_id,
            },
            "execution_authorized": False,
        },
        qualification_hash=binding.qualification_hash,
        timeout_seconds=timeout_seconds,
    )


def _validate_instrument(
    intent: AggressiveTrancheIntent,
    instrument: Instrument,
    venue: Venue,
    symbol: str,
) -> None:
    if (
        instrument.venue != venue
        or instrument.symbol != symbol
        or instrument.base != intent.base
        or instrument.quote != "USDT"
        or instrument.settle != "USDT"
        or instrument.product_type != ProductType.LINEAR_USDT_PERPETUAL
        or not instrument.active
    ):
        raise ValueError("aggressive live instrument identity is invalid")


def _parse_route(value: str) -> DirectedRouteKey:
    base, separator, venues = value.partition(":")
    long_venue, direction, short_venue = venues.partition(">")
    if not separator or not direction:
        raise ValueError("aggressive qualification route identity is invalid")
    return DirectedRouteKey(base, Venue(long_venue), Venue(short_venue))


def _validate_notional(quantity: Decimal, price: Decimal, instrument: Instrument) -> None:
    if not price.is_finite() or price <= 0:
        raise ValueError("aggressive protected price is invalid")
    minimum = instrument.minimum_notional
    if minimum is not None and quantity * price < minimum:
        raise ValueError("aggressive live notional is below the venue minimum")
