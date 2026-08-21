from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
import yaml
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from interexchange_perp_grid.client_ids import venue_client_order_id
from interexchange_perp_grid.live_journal import LiveOrderJournal, completed_normal_actions_sha256
from interexchange_perp_grid.risk_stages import (
    RiskStageLimits,
    load_locked_risk_stage_table,
    verify_risk_stage_completion_evidence,
)
from interexchange_perp_grid.state import (
    RiskStage,
    initialise_state,
    promote_risk_stage,
    read_risk_stage,
    record_risk_stage_result,
)

RUNTIME_POLICY = Path(__file__).resolve().parents[1] / "config" / "RUNTIME_POLICY.yaml"


def test_locked_risk_stage_table_has_exact_order_and_bounded_limits() -> None:
    table = load_locked_risk_stage_table(RUNTIME_POLICY)

    assert tuple(stage.stage for stage in table.stages) == (
        RiskStage.CANARY,
        RiskStage.PILOT_A,
        RiskStage.PILOT_B,
        RiskStage.WAVE1_PROD,
        RiskStage.FULL,
    )
    assert len(table.runtime_policy_sha256) == 64
    assert max(stage.leverage for stage in table.stages) <= 3


def test_locked_risk_stage_table_rejects_reordered_or_weakened_policy(tmp_path: Path) -> None:
    payload = yaml.safe_load(RUNTIME_POLICY.read_text(encoding="utf-8"))
    payload["risk_stages"]["canary"]["leverage"] = "4"
    policy = tmp_path / "RUNTIME_POLICY.yaml"
    policy.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(ValueError, match="leverage"):
        load_locked_risk_stage_table(policy)

    payload = yaml.safe_load(RUNTIME_POLICY.read_text(encoding="utf-8"))
    payload["risk_stages"]["full"]["routes"] = True
    policy.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(ValueError, match="exact integer"):
        load_locked_risk_stage_table(policy)


def test_risk_stage_limits_reject_bool_and_global_cap_bypass() -> None:
    with pytest.raises(ValueError, match="exact integers"):
        RiskStageLimits(
            RiskStage.FULL,
            True,
            True,
            Decimal("5"),
            Decimal("50"),
            Decimal("3"),
        )
    with pytest.raises(ValueError, match="portfolio bound"):
        RiskStageLimits(
            RiskStage.FULL,
            10,
            5,
            Decimal("100"),
            Decimal("100"),
            Decimal("3"),
        )


def _signed_stage_evidence(
    tmp_path: Path,
) -> tuple[Path, Path, str, dict[str, object], Ed25519PrivateKey]:
    private_key = Ed25519PrivateKey.generate()
    public_key_raw = private_key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    public_key_path = tmp_path / "operator-public-key.json"
    public_key_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "algorithm": "Ed25519",
                "public_key_base64": base64.b64encode(public_key_raw).decode(),
            }
        ),
        encoding="utf-8",
    )
    started_at = datetime(2026, 8, 1, tzinfo=UTC)
    payload: dict[str, object] = {
        "schema_version": 1,
        "producer": "ipegctl-stage-result-v1",
        "stage": "pilot_a",
        "qualification_hash": "a" * 64,
        "runtime_policy_sha256": "b" * 64,
        "release_sha": "c" * 40,
        "source_sha256": "d" * 64,
        "config_sha256": "e" * 64,
        "container_image_digest": "sha256:" + "f" * 64,
        "stage_started_at": started_at.isoformat(),
        "stage_ended_at": (started_at + timedelta(days=1)).isoformat(),
        "stable_flat_verified": True,
        "stable_flat_consecutive_snapshots": 2,
        "quiet_period_seconds": "1",
        "private_snapshots_complete": True,
        "first_snapshot_sha256": "1" * 64,
        "second_snapshot_sha256": "1" * 64,
        "event_watermark_before": 20,
        "event_watermark_after": 20,
        "active_action_count": 0,
        "raw_open_order_count": 0,
        "raw_nonzero_position_count": 0,
        "unknown_active_record_count": 0,
        "unresolved_order_count": 0,
        "unresolved_exposure_count": 0,
        "liquidation_count": 0,
        "adl_count": 0,
        "manual_emergency_intervention_count": 0,
        "risk_invariant_violation_count": 0,
        "availability_ratio": "0.999",
        "private_completeness_ratio": "1",
        "realized_net_pnl_usdt": "0",
        "maximum_realized_loss_usdt": "1",
        "maximum_holding_seconds_observed": 60,
        "completed_pair_cycle_count": 1,
        "completed_pair_action_ids": ["A1"],
        "completed_pair_actions_sha256": "2" * 64,
    }
    signature = private_key.sign(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    )
    payload["operator_signature_ed25519"] = base64.b64encode(signature).decode()
    evidence_path = tmp_path / "stage-result.json"
    evidence_path.write_text(json.dumps(payload), encoding="utf-8")
    return (
        public_key_path,
        evidence_path,
        hashlib.sha256(public_key_raw).hexdigest(),
        payload,
        private_key,
    )


def test_stage_completion_requires_signed_exact_account_wide_evidence(tmp_path: Path) -> None:
    public_key, evidence_path, key_hash, payload, _ = _signed_stage_evidence(tmp_path)
    limits = RiskStageLimits(
        RiskStage.PILOT_A,
        1,
        2,
        Decimal("2"),
        Decimal("2"),
        Decimal("2"),
    )
    evidence = verify_risk_stage_completion_evidence(
        evidence_path,
        public_key,
        key_hash,
        limits,
        required_consecutive_snapshots=2,
        required_quiet_period_seconds=Decimal("1"),
        hard_maximum_holding_seconds=86_400,
    )
    assert evidence.qualification_hash == "a" * 64

    payload["active_action_count"] = 1
    evidence_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="unresolved risk"):
        verify_risk_stage_completion_evidence(
            evidence_path,
            public_key,
            key_hash,
            limits,
            required_consecutive_snapshots=2,
            required_quiet_period_seconds=Decimal("1"),
            hard_maximum_holding_seconds=86_400,
        )


def test_stage_completion_rejects_unsigned_minimal_self_declaration(tmp_path: Path) -> None:
    public_key, evidence_path, key_hash, _, _ = _signed_stage_evidence(tmp_path)
    evidence_path.write_text(
        json.dumps({"stage": "pilot_a", "stable_flat_verified": True, "active_action_count": 0}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="exact schema"):
        verify_risk_stage_completion_evidence(
            evidence_path,
            public_key,
            key_hash,
            RiskStageLimits(
                RiskStage.PILOT_A,
                1,
                2,
                Decimal("2"),
                Decimal("2"),
                Decimal("2"),
            ),
            required_consecutive_snapshots=2,
            required_quiet_period_seconds=Decimal("1"),
            hard_maximum_holding_seconds=86_400,
        )


def test_stage_completion_rejects_future_time_and_negative_watermark(tmp_path: Path) -> None:
    public_key, evidence_path, key_hash, payload, private_key = _signed_stage_evidence(tmp_path)
    limits = RiskStageLimits(
        RiskStage.PILOT_A,
        1,
        2,
        Decimal("2"),
        Decimal("2"),
        Decimal("2"),
    )

    def write_signed() -> None:
        unsigned = {
            key: value for key, value in payload.items() if key != "operator_signature_ed25519"
        }
        payload["operator_signature_ed25519"] = base64.b64encode(
            private_key.sign(json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode())
        ).decode()
        evidence_path.write_text(json.dumps(payload), encoding="utf-8")

    payload["stage_ended_at"] = datetime(2026, 8, 30, tzinfo=UTC).isoformat()
    write_signed()
    with pytest.raises(ValueError, match="duration"):
        verify_risk_stage_completion_evidence(
            evidence_path,
            public_key,
            key_hash,
            limits,
            required_consecutive_snapshots=2,
            required_quiet_period_seconds=Decimal("2"),
            hard_maximum_holding_seconds=86_400,
            observed_at=datetime(2026, 8, 2, tzinfo=UTC),
        )

    payload["stage_ended_at"] = datetime(2026, 8, 2, tzinfo=UTC).isoformat()
    payload["quiet_period_seconds"] = "2"
    payload["event_watermark_before"] = -1
    payload["event_watermark_after"] = -1
    write_signed()
    with pytest.raises(ValueError, match="stable account-wide FLAT"):
        verify_risk_stage_completion_evidence(
            evidence_path,
            public_key,
            key_hash,
            limits,
            required_consecutive_snapshots=2,
            required_quiet_period_seconds=Decimal("2"),
            hard_maximum_holding_seconds=86_400,
            observed_at=datetime(2026, 8, 2, tzinfo=UTC),
        )


@pytest.mark.asyncio
async def test_risk_stage_promotion_is_adjacent_confirmed_and_persisted(tmp_path: Path) -> None:
    state_path = tmp_path / "state.sqlite3"
    await initialise_state(state_path)
    journal = LiveOrderJournal(state_path)
    await journal.initialise()
    initial = await read_risk_stage(state_path)
    assert initial.stage == RiskStage.SHADOW

    promoted = await promote_risk_stage(
        state_path,
        RiskStage.SHADOW,
        RiskStage.CANARY,
        "a" * 64,
        "b" * 64,
        "owner",
        "PROMOTE:canary",
        datetime(2026, 8, 21, tzinfo=UTC),
    )

    assert promoted.stage == RiskStage.CANARY
    assert promoted.qualification_hash == "a" * 64
    assert await read_risk_stage(state_path) == promoted
    with pytest.raises(ValueError, match="exactly one"):
        await promote_risk_stage(
            state_path,
            RiskStage.CANARY,
            RiskStage.WAVE1_PROD,
            "a" * 64,
            "b" * 64,
            "owner",
            "PROMOTE:wave1_prod",
        )
    with pytest.raises(ValueError, match="confirmation"):
        await promote_risk_stage(
            state_path,
            RiskStage.CANARY,
            RiskStage.PILOT_A,
            "a" * 64,
            "b" * 64,
            "owner",
            "PROMOTE:canary",
        )
    with pytest.raises(RuntimeError, match="stable-FLAT"):
        await promote_risk_stage(
            state_path,
            RiskStage.CANARY,
            RiskStage.PILOT_A,
            "a" * 64,
            "b" * 64,
            "owner",
            "PROMOTE:pilot_a",
        )
    completed = await record_risk_stage_result(
        state_path,
        RiskStage.CANARY,
        "a" * 64,
        "b" * 64,
        "c" * 64,
        True,
        "owner",
        0,
        hashlib.sha256(b"[]").hexdigest(),
        hashlib.sha256(b"[]").hexdigest(),
        (),
    )
    assert completed.stable_flat_verified is True
    assert completed.journal_event_watermark == 0
    assert completed.completed_actions_sha256 == hashlib.sha256(b"[]").hexdigest()
    assert (await read_risk_stage(state_path)).completion_frozen is True
    pilot = await promote_risk_stage(
        state_path,
        RiskStage.CANARY,
        RiskStage.PILOT_A,
        "d" * 64,
        "b" * 64,
        "owner",
        "PROMOTE:pilot_a",
    )
    assert pilot.stage == RiskStage.PILOT_A
    assert pilot.qualification_hash == "d" * 64
    assert pilot.completion_frozen is False


@pytest.mark.asyncio
async def test_risk_stage_promotion_rejects_stale_expected_state(tmp_path: Path) -> None:
    state_path = tmp_path / "state.sqlite3"
    await initialise_state(state_path)

    with pytest.raises(RuntimeError, match="changed concurrently"):
        await promote_risk_stage(
            state_path,
            RiskStage.CANARY,
            RiskStage.PILOT_A,
            "a" * 64,
            "b" * 64,
            "owner",
            "PROMOTE:pilot_a",
        )


@pytest.mark.asyncio
async def test_stage_result_rechecks_completed_cycle_content_inside_freeze_transaction(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.sqlite3"
    await initialise_state(state_path)
    journal = LiveOrderJournal(state_path)
    await journal.initialise()
    promoted_at = datetime(2026, 8, 21, tzinfo=UTC)
    await promote_risk_stage(
        state_path,
        RiskStage.SHADOW,
        RiskStage.CANARY,
        "a" * 64,
        "b" * 64,
        "owner",
        "PROMOTE:canary",
        promoted_at,
    )
    with sqlite3.connect(state_path) as database:
        database.execute(
            "INSERT INTO live_pair_actions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "cycle-1",
                "BTC",
                "binanceusdm",
                "okx",
                "tranche-1",
                "FLAT",
                "{}",
                "a" * 64,
                "0",
                None,
                promoted_at.isoformat(),
                promoted_at.isoformat(),
            ),
        )
        for client_id, venue, side in (
            (venue_client_order_id("cycle-1", "long"), "binanceusdm", "BUY"),
            (venue_client_order_id("cycle-1", "short"), "okx", "SELL"),
            (venue_client_order_id("cycle-1", "close", 1), "binanceusdm", "SELL"),
            (venue_client_order_id("cycle-1", "close", 2), "okx", "BUY"),
        ):
            database.execute(
                "INSERT INTO live_order_legs VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)",
                (
                    client_id,
                    "cycle-1",
                    venue,
                    "BTC/USDT:USDT",
                    side,
                    "c" * 64,
                    "0.001",
                    "100",
                    f"order-{venue}-{side}",
                    "FILLED",
                    "0.001",
                    promoted_at.isoformat(),
                ),
            )
    actions = await journal.completed_actions_since(promoted_at, "a" * 64)
    completed_hash = completed_normal_actions_sha256(actions)
    with sqlite3.connect(state_path) as database:
        database.execute(
            "UPDATE live_pair_actions SET recovery_action = 'EMERGENCY_FLATTEN' "
            "WHERE pair_action_id = 'cycle-1'"
        )
    with pytest.raises(RuntimeError, match="paired-cycle content changed"):
        await record_risk_stage_result(
            state_path,
            RiskStage.CANARY,
            "a" * 64,
            "b" * 64,
            "d" * 64,
            True,
            "owner",
            0,
            completed_hash,
            hashlib.sha256(b'["cycle-1"]').hexdigest(),
            ("cycle-1",),
        )


@pytest.mark.asyncio
async def test_v13_result_is_archived_and_can_be_re_attested(tmp_path: Path) -> None:
    state_path = tmp_path / "state.sqlite3"
    await initialise_state(state_path)
    journal = LiveOrderJournal(state_path)
    await journal.initialise()
    await promote_risk_stage(
        state_path,
        RiskStage.SHADOW,
        RiskStage.CANARY,
        "a" * 64,
        "b" * 64,
        "owner",
        "PROMOTE:canary",
    )
    with sqlite3.connect(state_path) as database:
        database.execute("UPDATE risk_stage_runtime SET stage = 'pilot_b' WHERE singleton = 1")
        database.execute("ALTER TABLE risk_stage_results RENAME TO risk_stage_results_v14")
        database.execute(
            "CREATE TABLE risk_stage_results(stage TEXT PRIMARY KEY, qualification_hash TEXT "
            "NOT NULL, runtime_policy_sha256 TEXT NOT NULL, evidence_sha256 TEXT NOT NULL, "
            "stable_flat_verified INTEGER NOT NULL, completed_by TEXT NOT NULL, "
            "completed_at TEXT NOT NULL)"
        )
        database.execute(
            "INSERT INTO risk_stage_results VALUES (?, ?, ?, ?, 1, ?, ?)",
            ("canary", "a" * 64, "b" * 64, "c" * 64, "legacy", datetime.now(UTC).isoformat()),
        )
        database.execute("DROP TABLE risk_stage_results_v14")
        database.execute("UPDATE metadata SET value = '13' WHERE key = 'schema_version'")
    await initialise_state(state_path)
    reset = await read_risk_stage(state_path)
    assert reset.stage == RiskStage.SHADOW
    assert reset.qualification_hash is None
    await promote_risk_stage(
        state_path,
        RiskStage.SHADOW,
        RiskStage.CANARY,
        "a" * 64,
        "b" * 64,
        "owner",
        "PROMOTE:canary",
    )
    completed = await record_risk_stage_result(
        state_path,
        RiskStage.CANARY,
        "a" * 64,
        "b" * 64,
        "d" * 64,
        True,
        "owner",
        0,
        hashlib.sha256(b"[]").hexdigest(),
        hashlib.sha256(b"[]").hexdigest(),
        (),
    )
    assert completed.evidence_sha256 == "d" * 64
    with sqlite3.connect(state_path) as database:
        assert database.execute(
            "SELECT evidence_sha256 FROM risk_stage_results_legacy_v13_archive"
        ).fetchone() == ("c" * 64,)
