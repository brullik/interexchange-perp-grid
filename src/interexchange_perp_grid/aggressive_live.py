from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path

from interexchange_perp_grid.aggressive_activation import AggressiveFastLiveBinding
from interexchange_perp_grid.aggressive_evaluator import AggressiveEntryStage, CostReserves
from interexchange_perp_grid.aggressive_model import DivergenceDirection
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


@dataclass(frozen=True, slots=True)
class AggressiveLiveIntentEnvelope:
    schema_version: int
    generated_at: datetime
    aggressive_binding_sha256: str
    qualification_hash: str
    intent: AggressiveTrancheIntent
    intent_sha256: str

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ValueError("aggressive live intent envelope version is invalid")
        if self.generated_at.tzinfo is None or self.generated_at.utcoffset() is None:
            raise ValueError("aggressive live intent envelope time must be aware")
        if self.intent.execution_authorized:
            raise ValueError("aggressive live intent envelope cannot authorize execution")
        for value in (
            self.aggressive_binding_sha256,
            self.qualification_hash,
            self.intent_sha256,
        ):
            if re.fullmatch(r"[0-9a-f]{64}", value) is None:
                raise ValueError("aggressive live intent envelope identity is invalid")
        if aggressive_intent_sha256(self.intent) != self.intent_sha256:
            raise ValueError("aggressive live intent envelope hash mismatch")


@dataclass(frozen=True, slots=True)
class AggressiveFastLiveIntentEnvelope:
    schema_version: int
    generated_at: datetime
    activation_binding_sha256: str
    intent: AggressiveTrancheIntent
    intent_sha256: str
    execution_authorized: bool = False

    def __post_init__(self) -> None:
        if self.schema_version != 2 or self.execution_authorized:
            raise ValueError("aggressive fast-live intent envelope is invalid")
        if self.generated_at.tzinfo is None or self.generated_at.utcoffset() is None:
            raise ValueError("aggressive fast-live intent time must be aware")
        if self.intent.execution_authorized:
            raise ValueError("aggressive fast-live intent cannot authorize execution")
        if any(
            re.fullmatch(r"[0-9a-f]{64}", value) is None
            for value in (self.activation_binding_sha256, self.intent_sha256)
        ):
            raise ValueError("aggressive fast-live intent identity is invalid")
        if aggressive_intent_sha256(self.intent) != self.intent_sha256:
            raise ValueError("aggressive fast-live intent hash mismatch")


def save_aggressive_fast_live_intent(
    path: Path,
    envelope: AggressiveFastLiveIntentEnvelope,
) -> None:
    envelope.__post_init__()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            stream.write(json.dumps(asdict(envelope), default=str, indent=2, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def load_aggressive_fast_live_intent(path: Path) -> AggressiveFastLiveIntentEnvelope:
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw = payload.get("intent") if isinstance(payload, dict) else None
    if not isinstance(payload, dict) or not isinstance(raw, dict):
        raise ValueError("aggressive fast-live intent envelope is invalid")
    return AggressiveFastLiveIntentEnvelope(
        schema_version=int(payload["schema_version"]),
        generated_at=datetime.fromisoformat(str(payload["generated_at"])),
        activation_binding_sha256=str(payload["activation_binding_sha256"]),
        intent=aggressive_intent_from_mapping(raw),
        intent_sha256=str(payload["intent_sha256"]),
        execution_authorized=bool(payload.get("execution_authorized", False)),
    )


def save_aggressive_live_intent(
    path: Path,
    envelope: AggressiveLiveIntentEnvelope,
) -> None:
    envelope.__post_init__()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            stream.write(json.dumps(asdict(envelope), default=str, indent=2, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def load_aggressive_live_intent(path: Path) -> AggressiveLiveIntentEnvelope:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("intent"), dict):
        raise ValueError("aggressive live intent envelope is invalid")
    raw = payload["intent"]
    intent = aggressive_intent_from_mapping(raw)
    return AggressiveLiveIntentEnvelope(
        schema_version=int(str(payload["schema_version"])),
        generated_at=datetime.fromisoformat(str(payload["generated_at"])),
        aggressive_binding_sha256=str(payload["aggressive_binding_sha256"]),
        qualification_hash=str(payload["qualification_hash"]),
        intent=intent,
        intent_sha256=str(payload["intent_sha256"]),
    )


def aggressive_intent_from_mapping(raw: object) -> AggressiveTrancheIntent:
    if not isinstance(raw, dict):
        raise ValueError("aggressive live intent payload is invalid")
    reserves = raw.get("reserves")
    if not isinstance(reserves, dict):
        raise ValueError("aggressive live intent reserves are invalid")
    intent = AggressiveTrancheIntent(
        base=str(raw["base"]),
        route_identity=str(raw["route_identity"]),
        direction=DivergenceDirection(str(raw["direction"])),
        level_index=int(str(raw["level_index"])),
        decision_cycle=int(str(raw["decision_cycle"])),
        quantity=Decimal(str(raw["quantity"])),
        long_venue=str(raw["long_venue"]),
        short_venue=str(raw["short_venue"]),
        long_symbol=str(raw["long_symbol"]),
        short_symbol=str(raw["short_symbol"]),
        reference_interval_start=datetime.fromisoformat(str(raw["reference_interval_start"])),
        reference_trigger_bps=Decimal(str(raw["reference_trigger_bps"])),
        reference_spread_bps=Decimal(str(raw["reference_spread_bps"])),
        grid_step_bps=Decimal(str(raw["grid_step_bps"])),
        stressed_cost_move_bps=Decimal(str(raw["stressed_cost_move_bps"])),
        minimum_profit_move_bps=Decimal(str(raw["minimum_profit_move_bps"])),
        normal_low_bps=Decimal(str(raw["normal_low_bps"])),
        normal_high_bps=Decimal(str(raw["normal_high_bps"])),
        reserves=CostReserves(**{key: Decimal(str(value)) for key, value in reserves.items()}),
        entry_stage=AggressiveEntryStage(str(raw["entry_stage"])),
        adverse_funding_reserve_usdt=Decimal(str(raw["adverse_funding_reserve_usdt"])),
        remaining_close_fees_usdt=Decimal(str(raw["remaining_close_fees_usdt"])),
        executable_entry_spread_bps=Decimal(str(raw["executable_entry_spread_bps"])),
        reverse_target_bps=Decimal(str(raw["reverse_target_bps"])),
        effective_stop_bps=Decimal(str(raw["effective_stop_bps"])),
        long_entry_vwap=Decimal(str(raw["long_entry_vwap"])),
        short_entry_vwap=Decimal(str(raw["short_entry_vwap"])),
        projected_route_loss_usdt=Decimal(str(raw["projected_route_loss_usdt"])),
        projected_portfolio_loss_usdt=Decimal(str(raw["projected_portfolio_loss_usdt"])),
        incremental_tranche_loss_usdt=Decimal(str(raw["incremental_tranche_loss_usdt"])),
        expected_net_pnl_usdt=Decimal(str(raw["expected_net_pnl_usdt"])),
        model_sha256=str(raw["model_sha256"]),
        strategy_profile_sha256=str(raw["strategy_profile_sha256"]),
        source_manifest_sha256=str(raw["source_manifest_sha256"]),
        reference_manifest_sha256=str(raw["reference_manifest_sha256"]),
        runtime_manifest_sha256=str(raw["runtime_manifest_sha256"]),
        contract_metadata_version_a=str(raw["contract_metadata_version_a"]),
        contract_metadata_version_b=str(raw["contract_metadata_version_b"]),
        decided_at=datetime.fromisoformat(str(raw["decided_at"])),
    )
    return intent


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
    if intent.execution_authorized or not binding.accepted or binding.execution_authorized:
        raise ValueError("aggressive strategy artifacts cannot authorize execution")
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
    return _build_aggressive_live_plan(
        intent,
        long_instrument,
        short_instrument,
        route,
        long_protected_price=long_protected_price,
        short_protected_price=short_protected_price,
        stage=stage,
        timeout_seconds=timeout_seconds,
        binding_sha256=binding.binding_sha256,
        strategy_name="AGGRESSIVE_SYMBIOSIS_V1",
        compatibility_qualification_hash=binding.qualification_hash,
    )


def prepare_aggressive_fast_live_plan(
    intent: AggressiveTrancheIntent,
    binding: AggressiveFastLiveBinding,
    long_instrument: Instrument,
    short_instrument: Instrument,
    *,
    preflight_sha256: str,
    preflight_expires_at: datetime,
    long_protected_price: Decimal,
    short_protected_price: Decimal,
    stage: AggressiveLaptopLiveStage,
    timeout_seconds: int,
) -> CanaryExecutionPlan:
    if intent.execution_authorized or binding.execution_authorized:
        raise ValueError("fast-live strategy artifacts cannot authorize execution")
    identities = (
        (intent.model_sha256, binding.model_sha256),
        (intent.strategy_profile_sha256, binding.profile_sha256),
        (intent.source_manifest_sha256, binding.source_manifest_sha256),
        (intent.reference_manifest_sha256, binding.reference_manifest_sha256),
        (intent.runtime_manifest_sha256, binding.decision_runtime_sha256),
    )
    if any(actual != expected for actual, expected in identities):
        raise ValueError("aggressive live intent identity does not match fast-live binding")
    route = DirectedRouteKey(intent.base, Venue(intent.long_venue), Venue(intent.short_venue))
    if route.value != intent.route_identity or route.value != binding.route:
        raise ValueError("aggressive fast-live route identity is inconsistent")
    return _build_aggressive_live_plan(
        intent,
        long_instrument,
        short_instrument,
        route,
        long_protected_price=long_protected_price,
        short_protected_price=short_protected_price,
        stage=stage,
        timeout_seconds=timeout_seconds,
        binding_sha256=binding.binding_sha256,
        strategy_name="AGGRESSIVE_FAST_LIVE_V2",
        compatibility_qualification_hash="0" * 64,
        activation_hash=preflight_sha256,
        fast_live_preflight_expires_at=preflight_expires_at,
    )


def _build_aggressive_live_plan(
    intent: AggressiveTrancheIntent,
    long_instrument: Instrument,
    short_instrument: Instrument,
    route: DirectedRouteKey,
    *,
    long_protected_price: Decimal,
    short_protected_price: Decimal,
    stage: AggressiveLaptopLiveStage,
    timeout_seconds: int,
    binding_sha256: str,
    strategy_name: str,
    compatibility_qualification_hash: str,
    activation_hash: str | None = None,
    fast_live_preflight_expires_at: datetime | None = None,
) -> CanaryExecutionPlan:
    limits = _LIMITS[stage]
    if timeout_seconds <= 0:
        raise ValueError("aggressive live timeout must be positive")
    if intent.level_index > limits.maximum_level:
        raise ValueError("aggressive level exceeds the selected laptop stage")
    if (
        stage == AggressiveLaptopLiveStage.PILOT_A
        and intent.entry_stage != AggressiveEntryStage.NORMAL
    ):
        raise ValueError("aggressive intent economics do not match the selected laptop stage")
    if not Decimal(0) < intent.incremental_tranche_loss_usdt <= limits.route_hard_loss_usdt:
        raise ValueError("aggressive intent exceeds the selected laptop risk stage")
    if stage == AggressiveLaptopLiveStage.PILOT_A and not (
        intent.projected_route_loss_usdt <= limits.route_hard_loss_usdt
        and intent.projected_portfolio_loss_usdt <= limits.portfolio_hard_loss_usdt
    ):
        raise ValueError("aggressive intent exceeds the selected laptop risk stage")
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
            "strategy": strategy_name,
            "stage": stage.value,
            "level_index": intent.level_index,
            "decision_cycle": intent.decision_cycle,
            "projected_stress_usdt": intent.incremental_tranche_loss_usdt,
            "projected_route_total_usdt": intent.projected_route_loss_usdt,
            "projected_portfolio_total_usdt": intent.projected_portfolio_loss_usdt,
            "projected_portfolio_loss_usdt": intent.projected_portfolio_loss_usdt,
            "target_exit_spread_bps": intent.reverse_target_bps,
            "effective_stop_bps": intent.effective_stop_bps,
            "direction": intent.direction.value,
            "reference_interval_start": intent.reference_interval_start.isoformat(),
            "reference_trigger_bps": intent.reference_trigger_bps,
            "reference_spread_bps": intent.reference_spread_bps,
            "grid_step_bps": intent.grid_step_bps,
            "stressed_cost_move_bps": intent.stressed_cost_move_bps,
            "minimum_profit_move_bps": intent.minimum_profit_move_bps,
            "normal_low_bps": intent.normal_low_bps,
            "normal_high_bps": intent.normal_high_bps,
            "reserves": asdict(intent.reserves),
            "entry_stage": intent.entry_stage.value,
            "adverse_funding_reserve_usdt": intent.adverse_funding_reserve_usdt,
            "remaining_close_fees_usdt": intent.remaining_close_fees_usdt,
            "executable_entry_spread_bps": intent.executable_entry_spread_bps,
            "planned_long_entry_vwap": intent.long_entry_vwap,
            "planned_short_entry_vwap": intent.short_entry_vwap,
            "expected_net_pnl_usdt": intent.expected_net_pnl_usdt,
            "decided_at": intent.decided_at.isoformat(),
            "route_opened_at": intent.decided_at.isoformat(),
            "hard_holding_deadline": (intent.decided_at + timedelta(hours=24)).isoformat(),
            "route_hard_loss_usdt": limits.route_hard_loss_usdt,
            "portfolio_hard_loss_usdt": limits.portfolio_hard_loss_usdt,
            "aggressive_intent_sha256": aggressive_intent_sha256(intent),
            "aggressive_intent": asdict(intent),
            "aggressive_binding_sha256": binding_sha256,
            "strategy_profile_sha256": intent.strategy_profile_sha256,
            "opening_client_order_ids": {
                "long": long_client_id,
                "short": short_client_id,
            },
            "execution_authorized": False,
        },
        qualification_hash=compatibility_qualification_hash,
        timeout_seconds=(
            24 * 60 * 60 if stage == AggressiveLaptopLiveStage.PILOT_A else timeout_seconds
        ),
        activation_hash=activation_hash,
        fast_live_preflight_expires_at=fast_live_preflight_expires_at,
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
