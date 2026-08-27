from __future__ import annotations

import multiprocessing
import os
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from interexchange_perp_grid.aggressive_grid import (
    AggressiveGridStore,
    ExternalGridLevelProjection,
    FrozenGridSizingPlan,
    GridLegFill,
    GridLevelState,
    GridTrancheOwnership,
    reverse_grid_target_bps,
)
from interexchange_perp_grid.aggressive_model import (
    DivergenceDirection,
    HistoricalModelPolicy,
    HistoricalReferenceModel,
    build_historical_reference_model,
    historical_model_sha256,
)
from interexchange_perp_grid.domain import InstrumentKey, ProductType, Venue
from interexchange_perp_grid.execution import Side
from interexchange_perp_grid.reference_history import ReferenceSpreadBar

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_KEY = InstrumentKey("BTC", "USDT", "USDT", ProductType.LINEAR_USDT_PERPETUAL)


def _bar(minute: int, close: Decimal, high: Decimal, low: Decimal) -> ReferenceSpreadBar:
    return ReferenceSpreadBar(
        venue_a=Venue.BYBIT,
        venue_b=Venue.OKX,
        instrument=_KEY,
        interval_start=_NOW + timedelta(minutes=minute),
        open_bps=close,
        high_bps=high,
        low_bps=low,
        close_bps=close,
        contract_metadata_version_a="bybit-v1",
        contract_metadata_version_b="okx-v1",
    )


def _model() -> HistoricalReferenceModel:
    bars = tuple(_bar(minute, Decimal(0), Decimal(10), Decimal(-10)) for minute in range(1440))
    return build_historical_reference_model(
        bars,
        policy=HistoricalModelPolicy(
            history_target_days=Decimal("2"),
            history_minimum_live_days=Decimal("1.5"),
            history_minimum_shadow_days=Decimal("1"),
        ),
        source_manifest_sha256="source",
        strategy_profile_sha256="profile",
        code_sha="a" * 40,
    )


def _ownership(index: int) -> GridTrancheOwnership:
    quantity = Decimal("0.001")
    opened = _NOW + timedelta(minutes=index)
    return GridTrancheOwnership(
        tranche_id=f"tranche-{index}",
        normalized_base_quantity=quantity,
        legs=(
            GridLegFill(
                venue=Venue.OKX,
                symbol="BTC/USDT:USDT",
                side=Side.BUY,
                base_quantity=quantity,
                average_price=Decimal("100000"),
                fee_usdt=Decimal("0.05"),
                funding_usdt=Decimal("0"),
            ),
            GridLegFill(
                venue=Venue.BYBIT,
                symbol="BTC/USDT:USDT",
                side=Side.SELL,
                base_quantity=quantity,
                average_price=Decimal("100100"),
                fee_usdt=Decimal("0.05"),
                funding_usdt=Decimal("0"),
            ),
        ),
        executable_entry_spread_bps=Decimal(index * 2),
        reverse_target_bps=Decimal(max(2, index * 2 - 2)),
        effective_stop_bps=Decimal("11.5"),
        maximum_holding_deadline=opened + timedelta(hours=24),
        reserved_stress_usdt=Decimal("0.5"),
        entry_slippage_usdt=Decimal("0.01"),
        realised_pnl_usdt=Decimal("0"),
        unrealised_pnl_usdt=Decimal("0"),
        opened_at=opened,
    )


def _commit_grid_stage_and_exit(
    path: str,
    route: str,
    stage: str,
    ownership: GridTrancheOwnership,
) -> None:
    store = AggressiveGridStore(Path(path))
    if stage == "ENTRY_PENDING":
        store.reserve_entry(
            route,
            reference_spread_bps=Decimal("10"),
            decision_cycle=0,
            reserved_stress_usdt=ownership.reserved_stress_usdt,
            now=_NOW + timedelta(seconds=1),
        )
    elif stage == "OPEN":
        store.mark_open(route, 1, ownership, decision_cycle=0, now=_NOW + timedelta(seconds=2))
    elif stage == "EXIT_PENDING":
        store.reserve_exit(
            route,
            1,
            tranche_id=ownership.tranche_id,
            now=_NOW + timedelta(seconds=3),
        )
    elif stage == "CLOSED_WAIT_REARM":
        store.mark_closed(route, 1, ownership, now=_NOW + timedelta(seconds=4))
    else:
        raise ValueError("unknown grid process-kill stage")
    os._exit(0)


def test_decision_cycle_continues_durably_after_restart(tmp_path: Path) -> None:
    path = tmp_path / "grid.sqlite3"
    model = _model()
    route = model.positive_route
    store = AggressiveGridStore(path)
    store.initialise()
    store.initialise_route(
        model,
        DivergenceDirection.POSITIVE,
        now=_NOW,
        rearm_retreat_step_fraction=Decimal("0.25"),
    )
    assert store.next_decision_cycle(route) == 0

    store.reserve_entry(
        route,
        reference_spread_bps=model.positive.levels_bps[0],
        decision_cycle=0,
        reserved_stress_usdt=Decimal("1"),
        now=_NOW + timedelta(seconds=1),
    )

    restarted = AggressiveGridStore(path)
    restarted.initialise()
    assert restarted.next_decision_cycle(route) == 1


def _store(tmp_path: Path) -> tuple[AggressiveGridStore, str]:
    store = AggressiveGridStore(tmp_path / "grid.sqlite3")
    store.initialise()
    model = _model()
    records = store.initialise_route(
        model,
        DivergenceDirection.POSITIVE,
        now=_NOW,
        rearm_retreat_step_fraction=Decimal("0.25"),
    )
    return store, records[0].route_identity


def test_initialise_persists_exact_five_level_geometry_across_restart(tmp_path: Path) -> None:
    store, route = _store(tmp_path)
    levels = store.levels(route)
    assert len(levels) == 5
    assert tuple(level.level_index for level in levels) == (1, 2, 3, 4, 5)
    assert tuple(level.trigger_bps for level in levels) == (
        Decimal("2.00"),
        Decimal("4.00"),
        Decimal("6.00"),
        Decimal("8.00"),
        Decimal("10.00"),
    )
    assert tuple(level.allocated_weight for level in levels) == (
        Decimal("0.10"),
        Decimal("0.15"),
        Decimal("0.20"),
        Decimal("0.25"),
        Decimal("0.30"),
    )
    assert all(level.state == GridLevelState.ARMED for level in levels)
    assert all(not level.execution_authorized for level in levels)
    assert AggressiveGridStore(store.path).levels(route) == levels


def test_model_refresh_is_allowed_only_while_route_has_no_owned_tranche(tmp_path: Path) -> None:
    store, route = _store(tmp_path)
    refreshed_model = replace(_model(), code_sha="b" * 40)

    refreshed = store.initialise_route(
        refreshed_model,
        DivergenceDirection.POSITIVE,
        now=_NOW + timedelta(minutes=1),
        rearm_retreat_step_fraction=Decimal("0.25"),
    )
    assert all(
        level.model_sha256 == historical_model_sha256(refreshed_model) for level in refreshed
    )
    assert all(level.state == GridLevelState.ARMED for level in refreshed)
    assert store.next_decision_cycle(route) == 0

    store.reserve_entry(
        route,
        reference_spread_bps=Decimal("10"),
        decision_cycle=0,
        reserved_stress_usdt=Decimal("0.5"),
        now=_NOW + timedelta(minutes=2),
    )
    with pytest.raises(RuntimeError, match="active grid route model identity mismatch"):
        store.initialise_route(
            replace(refreshed_model, code_sha="c" * 40),
            DivergenceDirection.POSITIVE,
            now=_NOW + timedelta(minutes=3),
            rearm_retreat_step_fraction=Decimal("0.25"),
        )

    store.mark_entry_failed(
        route,
        1,
        decision_cycle=0,
        now=_NOW + timedelta(minutes=4),
    )
    newest_model = replace(refreshed_model, code_sha="d" * 40)
    store.initialise_route(
        newest_model,
        DivergenceDirection.POSITIVE,
        now=_NOW + timedelta(minutes=5),
        rearm_retreat_step_fraction=Decimal("0.25"),
    )
    assert store.next_decision_cycle(route) == 1
    store.reserve_entry(
        route,
        reference_spread_bps=Decimal("10"),
        decision_cycle=1,
        reserved_stress_usdt=Decimal("0.5"),
        now=_NOW + timedelta(minutes=6),
    )
    with pytest.raises(RuntimeError, match="decision cycle is stale"):
        store.mark_open(
            route,
            1,
            _ownership(1),
            decision_cycle=0,
            now=_NOW + timedelta(minutes=7),
        )


def test_negative_route_uses_its_own_levels_and_crossing_direction(tmp_path: Path) -> None:
    store = AggressiveGridStore(tmp_path / "grid.sqlite3")
    store.initialise()
    records = store.initialise_route(
        _model(),
        DivergenceDirection.NEGATIVE,
        now=_NOW,
        rearm_retreat_step_fraction=Decimal("0.25"),
    )
    route = records[0].route_identity
    assert route == "BTC:bybit>okx"
    assert tuple(level.trigger_bps for level in records) == (
        Decimal("-2.00"),
        Decimal("-4.00"),
        Decimal("-6.00"),
        Decimal("-8.00"),
        Decimal("-10.00"),
    )
    assert store.first_unfilled_crossed_level(route, Decimal("-1.9")) is None
    assert store.first_unfilled_crossed_level(route, Decimal("-8")).level_index == 1  # type: ignore[union-attr]


def test_gap_opens_one_first_unfilled_level_per_fresh_decision_cycle(tmp_path: Path) -> None:
    store, route = _store(tmp_path)
    for index in range(1, 6):
        selected = store.first_unfilled_crossed_level(route, Decimal("12"))
        assert selected is not None
        assert selected.level_index == index
        pending = store.reserve_entry(
            route,
            reference_spread_bps=Decimal("12"),
            decision_cycle=index,
            reserved_stress_usdt=Decimal("0.5"),
            now=_NOW + timedelta(seconds=index),
        )
        assert pending.level_index == index
        with pytest.raises(RuntimeError, match="decision cycle"):
            store.reserve_entry(
                route,
                reference_spread_bps=Decimal("12"),
                decision_cycle=index,
                reserved_stress_usdt=Decimal("0.5"),
                now=_NOW + timedelta(seconds=index),
            )
        store.mark_open(
            route,
            index,
            _ownership(index),
            decision_cycle=index,
            now=_NOW + timedelta(minutes=index),
        )
    assert store.first_unfilled_crossed_level(route, Decimal("12")) is None
    with pytest.raises(RuntimeError, match="no armed grid level"):
        store.reserve_entry(
            route,
            reference_spread_bps=Decimal("12"),
            decision_cycle=6,
            reserved_stress_usdt=Decimal("0.5"),
            now=_NOW + timedelta(minutes=6),
        )
    assert len(store.levels(route)) == 5


def test_reverse_close_rearm_requires_flat_retreat_and_new_cross(tmp_path: Path) -> None:
    store, route = _store(tmp_path)
    pending = store.reserve_entry(
        route,
        reference_spread_bps=Decimal("2.1"),
        decision_cycle=1,
        reserved_stress_usdt=Decimal("0.5"),
        now=_NOW + timedelta(seconds=1),
    )
    assert pending.level_index == 1
    ownership = _ownership(1)
    store.mark_open(route, 1, ownership, decision_cycle=1, now=_NOW + timedelta(minutes=1))
    store.reserve_exit(route, 1, tranche_id=ownership.tranche_id, now=_NOW + timedelta(minutes=2))
    closed = store.mark_closed(
        route,
        1,
        replace(ownership, realised_pnl_usdt=Decimal("0.12")),
        now=_NOW + timedelta(minutes=3),
    )
    assert closed.state == GridLevelState.CLOSED_WAIT_REARM
    with pytest.raises(RuntimeError, match="stable-FLAT retreat"):
        store.rearm(
            route,
            1,
            reference_spread_bps=Decimal("2"),
            stable_flat=True,
            tranche_id=ownership.tranche_id,
            now=_NOW + timedelta(minutes=4),
        )
    rearmed = store.rearm(
        route,
        1,
        reference_spread_bps=Decimal("1.5"),
        stable_flat=True,
        tranche_id=ownership.tranche_id,
        now=_NOW + timedelta(minutes=5),
    )
    assert rearmed.state == GridLevelState.ARMED
    assert rearmed.ownership is None
    assert store.first_unfilled_crossed_level(route, Decimal("1.5")) is None
    assert store.first_unfilled_crossed_level(route, Decimal("2.1")).level_index == 1  # type: ignore[union-attr]


def test_model_refresh_cannot_erase_closed_level_rearm_fence(tmp_path: Path) -> None:
    store, route = _store(tmp_path)
    pending = store.reserve_entry(
        route,
        reference_spread_bps=Decimal("2.1"),
        decision_cycle=1,
        reserved_stress_usdt=Decimal("0.5"),
        now=_NOW + timedelta(seconds=1),
    )
    ownership = _ownership(pending.level_index)
    store.mark_open(route, 1, ownership, decision_cycle=1, now=_NOW + timedelta(minutes=1))
    store.reserve_exit(route, 1, tranche_id=ownership.tranche_id, now=_NOW + timedelta(minutes=2))
    store.mark_closed(route, 1, ownership, now=_NOW + timedelta(minutes=3))

    changed = replace(_model(), source_manifest_sha256="e" * 64)
    with pytest.raises(RuntimeError, match="active grid route model identity mismatch"):
        store.initialise_route(
            changed,
            DivergenceDirection.POSITIVE,
            now=_NOW + timedelta(minutes=4),
            rearm_retreat_step_fraction=Decimal("0.25"),
        )

    assert store.levels(route)[0].state == GridLevelState.CLOSED_WAIT_REARM
    assert store.first_unfilled_crossed_level(route, Decimal("100")) is not None
    assert store.first_unfilled_crossed_level(route, Decimal("100")).level_index == 2  # type: ignore[union-attr]


def test_restart_preserves_pending_open_exit_and_closed_states_without_duplicates(
    tmp_path: Path,
) -> None:
    store, route = _store(tmp_path)
    store.reserve_entry(
        route,
        reference_spread_bps=Decimal("10"),
        decision_cycle=1,
        reserved_stress_usdt=Decimal("0.5"),
        now=_NOW + timedelta(seconds=1),
    )
    restarted = AggressiveGridStore(store.path)
    assert restarted.levels(route)[0].state == GridLevelState.ENTRY_PENDING
    ownership = _ownership(1)
    restarted.mark_open(route, 1, ownership, decision_cycle=1, now=_NOW + timedelta(minutes=1))
    assert AggressiveGridStore(store.path).levels(route)[0].ownership == ownership
    with pytest.raises(RuntimeError, match="requires ENTRY_PENDING"):
        restarted.mark_open(route, 1, ownership, decision_cycle=1, now=_NOW + timedelta(minutes=2))
    restarted.reserve_exit(
        route, 1, tranche_id=ownership.tranche_id, now=_NOW + timedelta(minutes=3)
    )
    assert AggressiveGridStore(store.path).levels(route)[0].state == GridLevelState.EXIT_PENDING
    restarted.mark_closed(route, 1, ownership, now=_NOW + timedelta(minutes=4))
    with pytest.raises(RuntimeError, match="requires EXIT_PENDING"):
        restarted.mark_closed(route, 1, ownership, now=_NOW + timedelta(minutes=5))


def test_process_exit_preserves_every_active_grid_transition(tmp_path: Path) -> None:
    store, route = _store(tmp_path)
    ownership = _ownership(1)
    context = multiprocessing.get_context("spawn")
    for stage, expected in (
        ("ENTRY_PENDING", GridLevelState.ENTRY_PENDING),
        ("OPEN", GridLevelState.OPEN),
        ("EXIT_PENDING", GridLevelState.EXIT_PENDING),
        ("CLOSED_WAIT_REARM", GridLevelState.CLOSED_WAIT_REARM),
    ):
        process = context.Process(
            target=_commit_grid_stage_and_exit,
            args=(str(store.path), route, stage, ownership),
        )
        process.start()
        process.join(timeout=10)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
            pytest.fail(f"grid {stage} process did not terminate")
        assert process.exitcode == 0
        assert AggressiveGridStore(store.path).levels(route)[0].state == expected


def test_failed_entry_rearms_without_enlarging_earlier_level(tmp_path: Path) -> None:
    store, route = _store(tmp_path)
    store.reserve_entry(
        route,
        reference_spread_bps=Decimal("10"),
        decision_cycle=1,
        reserved_stress_usdt=Decimal("0.5"),
        now=_NOW + timedelta(seconds=1),
    )
    failed = store.mark_entry_failed(route, 1, decision_cycle=1, now=_NOW + timedelta(seconds=2))
    assert failed.state == GridLevelState.ARMED
    assert failed.reserved_stress_usdt == 0
    assert failed.allocated_weight == Decimal("0.10")
    assert store.first_unfilled_crossed_level(route, Decimal("10")).level_index == 1  # type: ignore[union-attr]


def test_stale_entry_callback_cannot_mutate_a_new_level_generation(tmp_path: Path) -> None:
    store, route = _store(tmp_path)
    store.reserve_entry(
        route,
        reference_spread_bps=Decimal("10"),
        decision_cycle=1,
        reserved_stress_usdt=Decimal("0.5"),
        now=_NOW + timedelta(seconds=1),
    )
    with pytest.raises(RuntimeError, match="decision cycle is stale"):
        store.mark_entry_failed(route, 1, decision_cycle=0, now=_NOW + timedelta(seconds=2))
    assert store.levels(route)[0].state == GridLevelState.ENTRY_PENDING
    store.mark_entry_failed(route, 1, decision_cycle=1, now=_NOW + timedelta(seconds=3))
    store.reserve_entry(
        route,
        reference_spread_bps=Decimal("10"),
        decision_cycle=2,
        reserved_stress_usdt=Decimal("0.5"),
        now=_NOW + timedelta(seconds=4),
    )
    with pytest.raises(RuntimeError, match="decision cycle is stale"):
        store.mark_open(
            route,
            1,
            _ownership(1),
            decision_cycle=1,
            now=_NOW + timedelta(seconds=5),
        )
    assert store.levels(route)[0].pending_decision_cycle == 2


def test_closed_ownership_cannot_be_replaced_by_another_tranche(tmp_path: Path) -> None:
    store, route = _store(tmp_path)
    store.reserve_entry(
        route,
        reference_spread_bps=Decimal("2"),
        decision_cycle=1,
        reserved_stress_usdt=Decimal("0.5"),
        now=_NOW + timedelta(seconds=1),
    )
    ownership = _ownership(1)
    store.mark_open(route, 1, ownership, decision_cycle=1, now=_NOW + timedelta(minutes=1))
    store.reserve_exit(route, 1, tranche_id=ownership.tranche_id, now=_NOW + timedelta(minutes=2))
    with pytest.raises(RuntimeError, match="does not match"):
        store.mark_closed(
            route,
            1,
            replace(ownership, tranche_id="other"),
            now=_NOW + timedelta(minutes=3),
        )


def test_open_rejects_legs_that_do_not_match_directed_route(tmp_path: Path) -> None:
    store, route = _store(tmp_path)
    store.reserve_entry(
        route,
        reference_spread_bps=Decimal("2"),
        decision_cycle=1,
        reserved_stress_usdt=Decimal("0.5"),
        now=_NOW + timedelta(seconds=1),
    )
    ownership = _ownership(1)
    wrong = replace(
        ownership,
        legs=(
            replace(ownership.legs[0], venue=Venue.BYBIT),
            replace(ownership.legs[1], venue=Venue.OKX),
        ),
    )
    with pytest.raises(RuntimeError, match="directed route"):
        store.mark_open(route, 1, wrong, decision_cycle=1, now=_NOW + timedelta(minutes=1))


def test_deterministic_five_level_replay_reverse_exits_rearm_and_second_oscillation(
    tmp_path: Path,
) -> None:
    store, route = _store(tmp_path)
    for index in range(1, 6):
        store.reserve_entry(
            route,
            reference_spread_bps=Decimal("12"),
            decision_cycle=index,
            reserved_stress_usdt=Decimal("0.5"),
            now=_NOW + timedelta(seconds=index),
        )
        store.mark_open(
            route,
            index,
            _ownership(index),
            decision_cycle=index,
            now=_NOW + timedelta(minutes=index),
        )
    for index in range(5, 0, -1):
        ownership = store.levels(route)[index - 1].ownership
        assert ownership is not None
        store.reserve_exit(
            route,
            index,
            tranche_id=ownership.tranche_id,
            now=_NOW + timedelta(minutes=10 + index),
        )
        store.mark_closed(
            route,
            index,
            replace(ownership, realised_pnl_usdt=Decimal(index) / Decimal(10)),
            now=_NOW + timedelta(minutes=20 + index),
        )
    assert all(level.state == GridLevelState.CLOSED_WAIT_REARM for level in store.levels(route))
    for index in range(1, 6):
        store.rearm(
            route,
            index,
            reference_spread_bps=Decimal("0"),
            stable_flat=True,
            tranche_id=f"tranche-{index}",
            now=_NOW + timedelta(minutes=30 + index),
        )
    store.reserve_entry(
        route,
        reference_spread_bps=Decimal("2.1"),
        decision_cycle=6,
        reserved_stress_usdt=Decimal("0.5"),
        now=_NOW + timedelta(minutes=40),
    )
    second = replace(_ownership(1), tranche_id="tranche-1-second")
    store.mark_open(route, 1, second, decision_cycle=6, now=_NOW + timedelta(minutes=41))
    store.reserve_exit(route, 1, tranche_id=second.tranche_id, now=_NOW + timedelta(minutes=42))
    store.mark_closed(route, 1, second, now=_NOW + timedelta(minutes=43))
    final = store.levels(route)
    assert final[0].state == GridLevelState.CLOSED_WAIT_REARM
    assert all(level.state == GridLevelState.ARMED for level in final[1:])
    assert not any(
        level.state
        in (GridLevelState.ENTRY_PENDING, GridLevelState.OPEN, GridLevelState.EXIT_PENDING)
        for level in final
    )


def test_reverse_grid_target_matches_both_direction_formulas() -> None:
    assert reverse_grid_target_bps(
        DivergenceDirection.POSITIVE,
        actual_entry_spread_bps=Decimal("10"),
        grid_step_bps=Decimal("2"),
        stressed_cost_move_bps=Decimal("1"),
        minimum_profit_move_bps=Decimal("0.5"),
        normal_low_bps=Decimal("-2"),
        normal_high_bps=Decimal("2"),
    ) == Decimal("8")
    assert reverse_grid_target_bps(
        DivergenceDirection.NEGATIVE,
        actual_entry_spread_bps=Decimal("-10"),
        grid_step_bps=Decimal("2"),
        stressed_cost_move_bps=Decimal("2"),
        minimum_profit_move_bps=Decimal("1"),
        normal_low_bps=Decimal("-2"),
        normal_high_bps=Decimal("2"),
    ) == Decimal("-7")


def test_incompatible_schema_and_partial_route_fail_closed(tmp_path: Path) -> None:
    store, route = _store(tmp_path)
    with sqlite3.connect(store.path) as database:
        database.execute("UPDATE aggressive_grid_schema SET schema_version = 99")
    with pytest.raises(RuntimeError, match="unsupported aggressive grid schema"):
        AggressiveGridStore(store.path).initialise()
    with sqlite3.connect(store.path) as database:
        database.execute("UPDATE aggressive_grid_schema SET schema_version = 1")
        database.execute(
            "DELETE FROM aggressive_grid_levels WHERE route_identity = ? AND level_index = 5",
            (route,),
        )
    with pytest.raises(RuntimeError, match="exactly five"):
        AggressiveGridStore(store.path).levels(route)


def test_live_journal_projection_fences_consumed_levels_across_restart(tmp_path: Path) -> None:
    store, route = _store(tmp_path)

    projected = store.synchronize_externally_owned_levels(
        route,
        frozenset({1, 3}),
        now=_NOW,
    )

    assert [item.state for item in projected] == [
        GridLevelState.CLOSED_WAIT_REARM,
        GridLevelState.ARMED,
        GridLevelState.CLOSED_WAIT_REARM,
        GridLevelState.ARMED,
        GridLevelState.ARMED,
    ]
    restarted = AggressiveGridStore(store.path)
    restarted.initialise()
    selected = restarted.first_unfilled_crossed_level(route, Decimal("1000"))
    assert selected is not None and selected.level_index == 2
    assert restarted.synchronize_externally_owned_levels(
        route,
        frozenset({1, 3}),
        now=_NOW + timedelta(seconds=1),
    ) == restarted.levels(route)


def test_journal_projection_preserves_active_risk_and_closes_idempotently(tmp_path: Path) -> None:
    model = _model()
    route = model.positive_route
    store = AggressiveGridStore(tmp_path / "grid.sqlite3")
    store.initialise()
    store.initialise_route(
        model,
        DivergenceDirection.POSITIVE,
        now=_NOW,
        rearm_retreat_step_fraction=Decimal("0.25"),
    )
    store.synchronize_journal_levels(
        route,
        (
            ExternalGridLevelProjection(
                1,
                GridLevelState.ENTRY_PENDING,
                Decimal("0.5"),
                decision_cycle=0,
            ),
        ),
        now=_NOW + timedelta(seconds=1),
    )
    opened = store.synchronize_journal_levels(
        route,
        (
            ExternalGridLevelProjection(
                1,
                GridLevelState.OPEN,
                Decimal("0.8"),
                ownership=replace(_ownership(1), reserved_stress_usdt=Decimal("0.8")),
            ),
        ),
        now=_NOW + timedelta(seconds=2),
    )[0]
    assert opened.state == GridLevelState.OPEN
    assert opened.reserved_stress_usdt == Decimal("0.8")

    closed = store.synchronize_journal_levels(
        route,
        (ExternalGridLevelProjection(1, GridLevelState.CLOSED_WAIT_REARM, Decimal(0)),),
        now=_NOW + timedelta(seconds=3),
    )[0]
    assert closed.state == GridLevelState.CLOSED_WAIT_REARM
    assert closed.reserved_stress_usdt == 0


def test_journal_completed_level_survives_restart_until_retreat_and_new_cross(
    tmp_path: Path,
) -> None:
    store, route = _store(tmp_path)
    ownership = _ownership(1)
    store.synchronize_journal_levels(
        route,
        (
            ExternalGridLevelProjection(
                1,
                GridLevelState.ENTRY_PENDING,
                Decimal("0.5"),
                decision_cycle=7,
            ),
        ),
        now=_NOW + timedelta(seconds=1),
    )
    store.synchronize_journal_levels(
        route,
        (
            ExternalGridLevelProjection(
                1,
                GridLevelState.CLOSED_WAIT_REARM,
                Decimal(0),
                decision_cycle=7,
                ownership=ownership,
            ),
        ),
        now=_NOW + timedelta(seconds=2),
    )

    restarted = AggressiveGridStore(store.path)
    assert restarted.next_decision_cycle(route) == 8
    assert restarted.first_unfilled_crossed_level(route, Decimal("100")).level_index == 2  # type: ignore[union-attr]
    restarted.rearm(
        route,
        1,
        reference_spread_bps=Decimal("1.5"),
        stable_flat=True,
        tranche_id=ownership.tranche_id,
        now=_NOW + timedelta(seconds=3),
    )
    assert restarted.first_unfilled_crossed_level(route, Decimal("1.5")) is None
    assert restarted.first_unfilled_crossed_level(route, Decimal("2.1")).level_index == 1  # type: ignore[union-attr]


def test_route_sizing_plan_is_frozen_and_restart_durable(tmp_path: Path) -> None:
    model = _model()
    route = model.positive_route
    store = AggressiveGridStore(tmp_path / "grid.sqlite3")
    store.initialise()
    store.initialise_route(
        model,
        DivergenceDirection.POSITIVE,
        now=_NOW,
        rearm_retreat_step_fraction=Decimal("0.25"),
    )
    plan = FrozenGridSizingPlan(
        route,
        store.levels(route)[0].model_sha256,
        Decimal("1"),
        (Decimal(".1"), Decimal(".15"), Decimal(".2"), Decimal(".25"), Decimal(".3")),
        tuple(Decimal(".5") for _ in range(5)),
        Decimal("10"),
        _NOW,
    )
    store.freeze_sizing_plan(plan)

    assert AggressiveGridStore(store.path).frozen_sizing_plan(route) == plan
    with pytest.raises(RuntimeError, match="cannot change"):
        store.freeze_sizing_plan(replace(plan, projected_margin_usdt=Decimal("11")))


def test_full_stable_flat_rearm_releases_frozen_route_sizing(tmp_path: Path) -> None:
    store, route = _store(tmp_path)
    plan = FrozenGridSizingPlan(
        route,
        store.levels(route)[0].model_sha256,
        Decimal("1"),
        (Decimal(".1"), Decimal(".15"), Decimal(".2"), Decimal(".25"), Decimal(".3")),
        tuple(Decimal(".5") for _ in range(5)),
        Decimal("10"),
        _NOW,
    )
    store.freeze_sizing_plan(plan)
    store.reserve_entry(
        route,
        reference_spread_bps=Decimal("2.1"),
        decision_cycle=1,
        reserved_stress_usdt=Decimal("0.5"),
        now=_NOW + timedelta(seconds=1),
    )
    ownership = _ownership(1)
    store.mark_open(route, 1, ownership, decision_cycle=1, now=_NOW + timedelta(minutes=1))
    store.reserve_exit(route, 1, tranche_id=ownership.tranche_id, now=_NOW + timedelta(minutes=2))
    store.mark_closed(route, 1, ownership, now=_NOW + timedelta(minutes=3))
    store.rearm(
        route,
        1,
        reference_spread_bps=Decimal("1.5"),
        stable_flat=True,
        tranche_id=ownership.tranche_id,
        now=_NOW + timedelta(minutes=4),
    )

    assert store.frozen_sizing_plan(route) is None
