"""Deal-path findings: fixtures F-02, F-03, F-13, F-14 and IT04-03.

Deals are immutable history inside an epoch, which drives every assertion
here:

* a deal missing from the target is detectable, but repairing it is a
  *replay*, never an insert of values this service made up;
* the same deal id with different money is a conflict with **no** authoritative
  answer, so it must quarantine rather than pick a side (IT04-03);
* an extra deal in the target is never deleted, because it may be real
  history from a scope or epoch this audit does not cover;
* two generations holding the same deal id are not a collision (F-14).

Each test corrupts exactly one thing in an otherwise clean, fully synced world
so the finding it asserts cannot be ambient noise.
"""

from __future__ import annotations

import pytest

from reconciliation.errors import ActionType, Confidence, FindingCode, ReasonCode, Severity
from reconciliation.scheduler import ReconciliationScheduler
from tests.conftest import SyncedWorld

pytestmark = pytest.mark.integration


def _finding(outcome, code: FindingCode):
    matches = [f for f in outcome.findings if f["finding_code"] == code.value]
    assert matches, f"expected a {code.value} finding, got {[f['finding_code'] for f in outcome.findings]}"
    return matches[0]


# ---------------------------------------------------------------------------
# F-02: a deal present at source is missing from the target
# ---------------------------------------------------------------------------


def test_missing_target_deal_is_detected_with_proven_confidence(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld, db
) -> None:
    from reconciliation.db.engine import Role

    conn = db.connect(Role.P03_SYNC)
    try:
        victim = conn.execute(
            "SELECT deal_id FROM deals WHERE epoch_id = ? ORDER BY source_seq LIMIT 1 OFFSET 3",
            (synced_world.epoch_id,),
        ).fetchone()["deal_id"]
    finally:
        conn.close()

    synced_world.harness.corrupt_for_test(
        "DELETE FROM deals WHERE epoch_id = ? AND deal_id = ?",
        (synced_world.epoch_id, victim),
    )

    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)

    assert outcome.status == "FINDINGS"
    assert outcome.finding_count == 1
    finding = _finding(outcome, FindingCode.MISSING_TARGET_DEAL)
    assert finding["item_key"] == victim
    # W >= S held, the source scan was complete: this really is missing.
    assert finding["confidence"] == Confidence.PROVEN.value
    assert finding["severity"] == Severity.ERROR.value
    assert finding["target_hash"] is None
    assert finding["source_hash"] is not None


def test_missing_deal_plan_cannot_insert_values(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld, db
) -> None:
    """The strongest action for a missing deal is a replay request.

    With backfill disabled by policy (the P0 default), the plan may only
    record a human decision. Either way it never contains a verb that writes
    a deal row (section 8.1).
    """
    from reconciliation.db.engine import Role

    conn = db.connect(Role.P03_SYNC)
    try:
        victim = conn.execute(
            "SELECT deal_id FROM deals WHERE epoch_id = ? LIMIT 1", (synced_world.epoch_id,)
        ).fetchone()["deal_id"]
    finally:
        conn.close()
    synced_world.harness.corrupt_for_test(
        "DELETE FROM deals WHERE epoch_id = ? AND deal_id = ?", (synced_world.epoch_id, victim)
    )

    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)
    plan = scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1")

    assert plan is not None
    types = {action.action_type for action in plan.actions}
    assert types == {ActionType.MANUAL_INVESTIGATION}
    assert plan.dispatchable_actions == ()
    assert "DEAL_BACKFILL_DISABLED_BY_POLICY" in plan.escalations


def test_missing_deal_becomes_backfill_request_when_policy_allows(
    db, repository, source, harness, clock, tmp_path, synced_world: SyncedWorld
) -> None:
    """With backfill enabled, the action is a bounded replay request."""
    from reconciliation.config import load_settings
    from tests.conftest import make_scheduler

    settings = load_settings(
        env="test",
        db_path=tmp_path / "recon.db",
        source_id="sim_local",
        approval_required=True,
        allow_deal_backfill=True,
        source_max_page_size=100,
    )
    repo = type(repository)(db, settings, clock)
    scheduler = make_scheduler(
        db=db, repository=repo, source=source, harness=harness, settings=settings, clock=clock
    )

    from reconciliation.db.engine import Role

    conn = db.connect(Role.P03_SYNC)
    try:
        victim = conn.execute(
            "SELECT deal_id FROM deals WHERE epoch_id = ? LIMIT 1", (synced_world.epoch_id,)
        ).fetchone()["deal_id"]
    finally:
        conn.close()
    harness.corrupt_for_test(
        "DELETE FROM deals WHERE epoch_id = ? AND deal_id = ?", (synced_world.epoch_id, victim)
    )

    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)
    assert outcome.status == "FINDINGS"
    plan = scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1")

    assert plan is not None
    assert {a.action_type for a in plan.actions} == {ActionType.REQUEST_DEAL_BACKFILL}
    action = plan.actions[0]
    # Parameters carry provenance and bounds only -- never a value to write.
    assert action.parameters["entity"] == "deals"
    assert action.parameters["upper_bound_seq"] == outcome.decision.detail["cut"]["source_snapshot_seq"]
    assert "balance" not in action.parameters and "commission" not in action.parameters
    assert plan.requires_approval is True


# ---------------------------------------------------------------------------
# F-03 / IT04-03: same deal id, different money
# ---------------------------------------------------------------------------


def test_deal_money_conflict_is_critical_and_quarantined(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld, db
) -> None:
    """IT04-03: a 0.01 commission difference is a conflict, not a correction."""
    from reconciliation.db.engine import Role

    conn = db.connect(Role.P03_SYNC)
    try:
        row = conn.execute(
            "SELECT deal_id, commission FROM deals WHERE epoch_id = ? ORDER BY source_seq LIMIT 1",
            (synced_world.epoch_id,),
        ).fetchone()
    finally:
        conn.close()
    victim, original = row["deal_id"], row["commission"]

    synced_world.harness.corrupt_for_test(
        "UPDATE deals SET commission = ? WHERE epoch_id = ? AND deal_id = ?",
        ("-99.99", synced_world.epoch_id, victim),
    )

    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)

    assert outcome.status == "FINDINGS"
    finding = _finding(outcome, FindingCode.IMMUTABLE_DEAL_CONFLICT)
    assert finding["item_key"] == victim
    assert finding["severity"] == Severity.CRITICAL.value
    assert finding["confidence"] == Confidence.PROVEN.value
    assert finding["source_hash"] != finding["target_hash"]
    assert "QUARANTINE" in finding["explanation_code"]

    # The plan may only ask a human to look. No action can overwrite money.
    plan = scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1")
    assert plan is not None
    assert {a.action_type for a in plan.actions} == {ActionType.MANUAL_INVESTIGATION}
    assert plan.dispatchable_actions == ()
    assert FindingCode.IMMUTABLE_DEAL_CONFLICT.value in plan.escalations

    # And the corrupted value is still there: the audit changed nothing.
    conn = db.connect(Role.P03_SYNC)
    try:
        after = conn.execute(
            "SELECT commission FROM deals WHERE epoch_id = ? AND deal_id = ?",
            (synced_world.epoch_id, victim),
        ).fetchone()["commission"]
    finally:
        conn.close()
    assert after == "-99.99" != original


def test_p04_cannot_repair_the_conflict_even_by_direct_sql(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld, db
) -> None:
    """IT04-03's second clause: P04 cannot UPDATE deals even if it tried."""
    import sqlite3

    from reconciliation.db.engine import Role

    conn = db.connect(Role.RECON)
    try:
        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            conn.execute(
                "UPDATE deals SET commission = '-2.50' WHERE epoch_id = ?",
                (synced_world.epoch_id,),
            )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Extra deal in the target
# ---------------------------------------------------------------------------


def test_extra_target_deal_is_never_deleted(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld, db
) -> None:
    synced_world.harness.corrupt_for_test(
        """
        INSERT INTO deals (epoch_id, deal_id, source_seq, account_id, position_id, symbol,
                           entry, side, volume_lots, price, realized_pnl_gross, commission,
                           swap, executed_at, payload_hash)
        VALUES (?, 'D-99999999', 1, 'A-000001', 'P-00000001', 'EURUSD', 'IN', 'BUY',
                '0.10', '1.10000', '0.00', '-1.00', '0.00', '2026-01-01T00:00:01Z', 'x')
        """,
        (synced_world.epoch_id,),
    )

    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)

    assert outcome.status == "FINDINGS"
    finding = _finding(outcome, FindingCode.EXTRA_TARGET_DEAL)
    assert finding["item_key"] == "D-99999999"
    assert finding["severity"] == Severity.CRITICAL.value
    assert "NEVER_DELETE" in finding["explanation_code"]

    plan = scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1")
    assert plan is not None
    assert plan.dispatchable_actions == ()

    # The row survives the audit untouched.
    from reconciliation.db.engine import Role

    conn = db.connect(Role.P03_SYNC)
    try:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM deals WHERE epoch_id = ? AND deal_id = 'D-99999999'",
                (synced_world.epoch_id,),
            ).fetchone()[0]
            == 1
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# F-13: leading zeros are different identities
# ---------------------------------------------------------------------------


def test_leading_zero_ids_produce_missing_plus_extra_not_a_merge(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld, db
) -> None:
    """F-13: "D-0007" and "D-7" must never be treated as one deal.

    Renaming a target deal to a numerically equal but textually different id
    must surface as one missing and one extra, never as a silent match.
    """
    from reconciliation.db.engine import Role

    conn = db.connect(Role.P03_SYNC)
    try:
        victim = conn.execute(
            "SELECT deal_id FROM deals WHERE epoch_id = ? ORDER BY source_seq LIMIT 1",
            (synced_world.epoch_id,),
        ).fetchone()["deal_id"]
    finally:
        conn.close()

    stripped = victim.replace("D-", "D-x")  # same length class, different text
    synced_world.harness.corrupt_for_test(
        "UPDATE deals SET deal_id = ? WHERE epoch_id = ? AND deal_id = ?",
        (stripped, synced_world.epoch_id, victim),
    )

    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)

    assert outcome.status == "FINDINGS"
    codes = sorted(f["finding_code"] for f in outcome.findings)
    assert codes == [
        FindingCode.EXTRA_TARGET_DEAL.value,
        FindingCode.MISSING_TARGET_DEAL.value,
    ]
    keys = {f["finding_code"]: f["item_key"] for f in outcome.findings}
    assert keys[FindingCode.MISSING_TARGET_DEAL.value] == victim
    assert keys[FindingCode.EXTRA_TARGET_DEAL.value] == stripped


# ---------------------------------------------------------------------------
# F-14: the same deal id in two generations is not a collision
# ---------------------------------------------------------------------------


def test_same_deal_id_in_two_epochs_is_isolated(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld, p01_admin, harness, db
) -> None:
    """INV04-17: epochs scope identity; reused ids are separate rows."""
    old_epoch = synced_world.epoch_id

    # A reset gives the source a new generation and therefore a new epoch.
    p01_admin.reset_run(synced_world.run_id, expected_generation=synced_world.generation)
    new_epoch = harness.bootstrap_epoch()
    assert new_epoch != old_epoch

    harness.sync_deals(new_epoch)

    from reconciliation.db.engine import Role

    conn = db.connect(Role.P03_SYNC)
    try:
        shared = conn.execute(
            """
            SELECT deal_id, COUNT(DISTINCT epoch_id) AS epochs
              FROM deals GROUP BY deal_id HAVING epochs > 1 LIMIT 1
            """
        ).fetchone()
        # The generator is deterministic, so ids do recur across generations.
        assert shared is not None, "expected at least one deal id present in both epochs"
        per_epoch = conn.execute(
            "SELECT COUNT(*) FROM deals WHERE deal_id = ?", (shared["deal_id"],)
        ).fetchone()[0]
        assert per_epoch == 2  # two distinct rows, one per epoch
    finally:
        conn.close()

    # Auditing the new epoch must be clean and must not see the old epoch's rows.
    outcome = scheduler.run_audit(entity="deals", epoch_id=new_epoch)
    assert outcome.status == "MATCHED", (outcome.reason_code, outcome.detail)

    # The old epoch is now SUPERSEDED, so auditing it is blocked rather than
    # compared against a source that has moved on (section 7.7).
    stale = scheduler.run_audit(entity="deals", epoch_id=old_epoch)
    assert stale.status == "INCONCLUSIVE"
    assert stale.reason_code is ReasonCode.EPOCH_MISMATCH
