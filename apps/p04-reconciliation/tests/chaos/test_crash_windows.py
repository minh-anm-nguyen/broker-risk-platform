"""The 14 crash windows of section 7.6, one test each.

Section 7.6 tabulates every point where this service can die, what is durable
at that moment, and the rule for recovering. These tests assert the recovery
rule, because the failure mode they guard against is subtle: after a crash it
is tempting to redo work, and redoing work is how a lost ACK becomes a
duplicate repair or a second audit of the same scope.

Most windows are exercised by *abandoning* a worker mid-protocol and starting a
fresh one against the same durable state, which is precisely what a restart
is. One window additionally kills a real OS process with no chance to clean
up, so the durability claim is not resting entirely on simulated restarts.

Every test states which row of the table it covers.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from reconciliation.canonical import page_hash, payload_hash
from reconciliation.db.engine import Role
from reconciliation.dto import CapturedItem, CapturedPage
from reconciliation.errors import DataIntegrityError, ReasonCode, SourceError
from reconciliation.ports.source import FakeSource
from reconciliation.source_capture import SourceCaptureWorker
from tests.conftest import SyncedWorld, make_scheduler

pytestmark = [pytest.mark.chaos, pytest.mark.slow]


def _page(world: SyncedWorld, index: int, keys: list[str], terminal: bool) -> CapturedPage:
    items = [
        CapturedItem(item_key=key, payload_hash=f"hash-{key}", source_seq=i + 1)
        for i, key in enumerate(keys)
    ]
    return CapturedPage(
        page_index=index,
        run_id=world.run_id,
        generation=world.generation,
        snapshot_seq=world.snapshot_seq,
        items=items,
        page_hash=page_hash("deals", [(i.item_key, i.payload_hash) for i in items]),
        is_terminal=terminal,
    )


# ---------------------------------------------------------------------------
# Window 1: before the run insert commits -> no run exists
# ---------------------------------------------------------------------------


def test_w01_crash_before_run_commit_leaves_no_partial_run(
    repository, synced_world: SyncedWorld
) -> None:
    """Rule: an idempotent trigger creates a fresh run.

    Nothing was committed, so there is nothing to resume, and the retried
    trigger must not find a half-created run.
    """
    with repository.session() as conn:
        before = conn.execute("SELECT COUNT(*) FROM recon_runs").fetchone()[0]

    # A crash here means the transaction never committed. Simulated by simply
    # never calling create_run, then having the retry call it.
    run = repository.create_run(
        request_id="w01", source_id="sim_local", epoch_id=synced_world.epoch_id, entity="deals"
    )

    with repository.session() as conn:
        after = conn.execute("SELECT COUNT(*) FROM recon_runs").fetchone()[0]
    assert after == before + 1
    assert run.status == "CREATED"


# ---------------------------------------------------------------------------
# Window 2: after the run commits, before the first page
# ---------------------------------------------------------------------------


def test_w02_crash_after_run_commit_resumes_from_created(
    repository, synced_world: SyncedWorld
) -> None:
    """Rule: resume or restart within the deadline."""
    run = repository.create_run(
        request_id="w02", source_id="sim_local", epoch_id=synced_world.epoch_id, entity="deals"
    )

    # New worker, same durable state.
    recovered = repository.get_run(run.recon_run_id)
    assert recovered is not None
    assert recovered.status == "CREATED"
    assert recovered.source_complete is False
    assert repository.page_receipt_count(run.recon_run_id) == 0
    # The trigger is still idempotent after the restart.
    again = repository.create_run(
        request_id="w02", source_id="sim_local", epoch_id=synced_world.epoch_id, entity="deals"
    )
    assert again.recon_run_id == run.recon_run_id


# ---------------------------------------------------------------------------
# Window 3: page fetched, receipt not yet committed
# ---------------------------------------------------------------------------


def test_w03_crash_before_receipt_commit_leaves_no_items(
    repository, synced_world: SyncedWorld, db
) -> None:
    """Rule: re-fetch the same cursor; nothing of that page is durable.

    The receipt and its items share one transaction, so a crash before the
    commit must leave neither.
    """
    run = repository.create_run(
        request_id="w03", source_id="sim_local", epoch_id=synced_world.epoch_id, entity="deals"
    )
    repository.pin_source_identity(
        run.recon_run_id,
        run_id_source=synced_world.run_id,
        generation=synced_world.generation,
        snapshot_seq=synced_world.snapshot_seq,
    )

    # Force the page transaction to fail midway: a duplicate key inside the
    # same page violates the manifest primary key.
    bad = _page(synced_world, 0, ["D-1", "D-1"], False)
    with pytest.raises(SourceError) as exc:
        repository.commit_page(run.recon_run_id, bad)
    assert exc.value.reason is ReasonCode.SOURCE_DUPLICATE_KEY

    assert repository.page_receipt_count(run.recon_run_id) == 0
    assert repository.source_item_count(run.recon_run_id) == 0, (
        "items must not survive a rolled-back page transaction"
    )


# ---------------------------------------------------------------------------
# Window 4: receipt committed, ACK lost
# ---------------------------------------------------------------------------


def test_w04_lost_receipt_ack_resumes_without_duplicating(
    repository, settings, clock, synced_world: SyncedWorld, harness
) -> None:
    """IT04-01. Rule: query the receipt and validate page_hash; do not re-insert.

    The worker re-walks the scan from page 0. Pages already durable are
    recognised by their hash and skipped, so the manifest is unchanged and no
    second semantic page appears.
    """
    rows = list(harness.iter_target_rows(synced_world.epoch_id, "deals"))
    fake = FakeSource(
        source_id="sim_local",
        run_id=synced_world.run_id,
        generation=synced_world.generation,
        snapshot_seq=synced_world.snapshot_seq,
        records={"deals": rows},
        page_size=5,
    )
    worker = SourceCaptureWorker(
        repository=repository, source=fake, settings=settings, clock=clock
    )
    run = repository.create_run(
        request_id="w04", source_id="sim_local", epoch_id=synced_world.epoch_id, entity="deals"
    )
    identity = fake.read_identity()

    # First attempt: dies after page 2 commits (the ACK for it is "lost").
    fake.fail_at_page[3] = SourceError("connection reset after page 2 committed")
    first = worker.capture(run.recon_run_id, entity="deals", expected_identity=identity)
    assert first.complete is False
    receipts_after_crash = repository.page_receipt_count(run.recon_run_id)
    items_after_crash = repository.source_item_count(run.recon_run_id)
    assert receipts_after_crash == 3

    # Restart: same run, same source, no fault this time.
    fake.fail_at_page.clear()
    second = worker.capture(run.recon_run_id, entity="deals", expected_identity=identity)

    assert second.complete is True
    assert second.resumed_pages == 3, "the already-durable pages must be recognised, not rewritten"
    # Exactly one receipt per page index: no duplicates were appended.
    with repository.session() as conn:
        indices = [
            row["page_index"]
            for row in conn.execute(
                "SELECT page_index FROM recon_source_page_receipts WHERE recon_run_id = ? ORDER BY page_index",
                (run.recon_run_id,),
            )
        ]
    assert indices == sorted(set(indices))
    assert repository.source_item_count(run.recon_run_id) == len(rows)
    assert repository.source_item_count(run.recon_run_id) >= items_after_crash


def test_w04b_source_serving_different_bytes_for_a_committed_page_is_refused(
    repository, settings, clock, synced_world: SyncedWorld, harness
) -> None:
    """The dangerous variant: the source changed its mind about a stored page.

    Overwriting the stored evidence would destroy the audit trail, so the run
    ends instead.
    """
    rows = list(harness.iter_target_rows(synced_world.epoch_id, "deals"))
    fake = FakeSource(
        source_id="sim_local",
        run_id=synced_world.run_id,
        generation=synced_world.generation,
        snapshot_seq=synced_world.snapshot_seq,
        records={"deals": rows},
        page_size=5,
    )
    worker = SourceCaptureWorker(repository=repository, source=fake, settings=settings, clock=clock)
    run = repository.create_run(
        request_id="w04b", source_id="sim_local", epoch_id=synced_world.epoch_id, entity="deals"
    )
    identity = fake.read_identity()

    fake.fail_at_page[2] = SourceError("dropped after page 1")
    assert worker.capture(run.recon_run_id, entity="deals", expected_identity=identity).complete is False

    # On restart the source serves different *content* at the same position.
    # One field is altered and the row order is left intact, so this exercises
    # the page-hash comparison alone. Reordering the rows would also trip the
    # sequence-ordering guard, and the test would then pass on the wrong
    # violation.
    fake.fail_at_page.clear()
    altered = [dict(row) for row in rows]
    altered[0]["commission"] = "-77.77"
    fake.records["deals"] = altered

    result = worker.capture(run.recon_run_id, entity="deals", expected_identity=identity)

    assert result.complete is False
    assert result.reason_code is ReasonCode.SOURCE_CUT_DRIFTED


# ---------------------------------------------------------------------------
# Window 5: terminal page stored, completeness marker not set
# ---------------------------------------------------------------------------


def test_w05_terminal_page_without_marker_requires_validated_cas(
    repository, synced_world: SyncedWorld
) -> None:
    """Rule: validate all receipts, then compare-and-set ``source_complete``."""
    run = repository.create_run(
        request_id="w05", source_id="sim_local", epoch_id=synced_world.epoch_id, entity="deals"
    )
    repository.pin_source_identity(
        run.recon_run_id,
        run_id_source=synced_world.run_id,
        generation=synced_world.generation,
        snapshot_seq=synced_world.snapshot_seq,
    )
    repository.commit_page(run.recon_run_id, _page(synced_world, 0, ["D-1", "D-2"], False))
    repository.commit_page(run.recon_run_id, _page(synced_world, 1, ["D-3"], True))

    # The crash happened here: receipts are terminal, the flag is not set.
    mid = repository.get_run(run.recon_run_id)
    assert mid is not None and mid.source_complete is False

    ok, reason, stats = repository.validate_receipt_chain(run.recon_run_id)
    assert ok is True and reason is None
    repository.mark_source_complete(run.recon_run_id)

    after = repository.get_run(run.recon_run_id)
    assert after is not None
    assert after.source_complete is True
    assert after.source_count == 3
    assert after.source_root_hash is not None


def test_w05b_marking_complete_twice_is_safe(repository, synced_world: SyncedWorld) -> None:
    """A retried completion after a lost ACK must not corrupt the totals."""
    run = repository.create_run(
        request_id="w05b", source_id="sim_local", epoch_id=synced_world.epoch_id, entity="deals"
    )
    repository.pin_source_identity(
        run.recon_run_id,
        run_id_source=synced_world.run_id,
        generation=synced_world.generation,
        snapshot_seq=synced_world.snapshot_seq,
    )
    repository.commit_page(run.recon_run_id, _page(synced_world, 0, ["D-1"], True))

    first = repository.mark_source_complete(run.recon_run_id)
    second = repository.mark_source_complete(run.recon_run_id)

    assert first.stored_items == second.stored_items == 1
    run_after = repository.get_run(run.recon_run_id)
    assert run_after is not None and run_after.source_count == 1


# ---------------------------------------------------------------------------
# Window 6: source complete, target capture not started
# ---------------------------------------------------------------------------


def test_w06_source_complete_without_target_cannot_diff(
    repository, synced_world: SyncedWorld
) -> None:
    """Rule: capture the target anew; never diff a half-captured run."""
    run = repository.create_run(
        request_id="w06", source_id="sim_local", epoch_id=synced_world.epoch_id, entity="deals"
    )
    repository.pin_source_identity(
        run.recon_run_id,
        run_id_source=synced_world.run_id,
        generation=synced_world.generation,
        snapshot_seq=synced_world.snapshot_seq,
    )
    repository.commit_page(run.recon_run_id, _page(synced_world, 0, ["D-1"], True))
    repository.mark_source_complete(run.recon_run_id)

    with pytest.raises(SourceError) as exc:
        repository.compute_diff(run.recon_run_id)
    assert exc.value.reason is ReasonCode.TARGET_INCOMPLETE


# ---------------------------------------------------------------------------
# Window 7: crash during target capture
# ---------------------------------------------------------------------------


def test_w07_target_capture_rolls_back_entirely(
    db, repository, settings, clock, synced_world: SyncedWorld
) -> None:
    """Rule: the transaction rolls back, or a committed manifest is readable.

    There is no third state in which half a manifest is durable.
    """
    from reconciliation.db.engine import transaction as tx

    run = repository.create_run(
        request_id="w07", source_id="sim_local", epoch_id=synced_world.epoch_id, entity="accounts"
    )

    conn = db.connect(Role.RECON)
    try:
        with pytest.raises(RuntimeError, match="crash during capture"):
            with tx(conn):
                conn.execute(
                    """
                    INSERT INTO recon_target_items (recon_run_id, item_key, source_seq, payload_hash)
                    VALUES (?, 'A-000001', NULL, 'abc')
                    """,
                    (run.recon_run_id,),
                )
                raise RuntimeError("crash during capture")
    finally:
        conn.close()

    with repository.session() as conn:
        stored = conn.execute(
            "SELECT COUNT(*) FROM recon_target_items WHERE recon_run_id = ?", (run.recon_run_id,)
        ).fetchone()[0]
    assert stored == 0
    after = repository.get_run(run.recon_run_id)
    assert after is not None and after.target_complete is False


# ---------------------------------------------------------------------------
# Window 8: diff committed, response lost
# ---------------------------------------------------------------------------


def test_w08_findings_are_durable_and_replay_does_not_duplicate(
    scheduler, synced_world: SyncedWorld, repository, db
) -> None:
    """Rule: query the run and findings by id; no duplicate findings."""
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

    outcome = scheduler.run_audit(
        entity="deals", epoch_id=synced_world.epoch_id, request_id="w08"
    )
    assert outcome.status == "FINDINGS"
    first = len(repository.findings_for(outcome.recon_run_id))

    # The response was lost; the client retries with the same request id.
    replay = scheduler.run_audit(
        entity="deals", epoch_id=synced_world.epoch_id, request_id="w08"
    )

    assert replay.recon_run_id == outcome.recon_run_id
    assert replay.status == "FINDINGS"
    assert len(repository.findings_for(outcome.recon_run_id)) == first

    # Re-recording the identical findings is also idempotent at the store.
    reinserted = repository.record_findings(
        outcome.recon_run_id,
        [
            {
                "item_key": row["item_key"],
                "finding_code": row["finding_code"],
                "severity": row["severity"],
                "confidence": row["confidence"],
                "evidence_hash": row["evidence_hash"],
                "explanation_code": row["explanation_code"],
            }
            for row in repository.findings_for(outcome.recon_run_id)
        ],
    )
    assert reinserted == 0
    assert len(repository.findings_for(outcome.recon_run_id)) == first


# ---------------------------------------------------------------------------
# Window 9: plan drafted, approval not yet given
# ---------------------------------------------------------------------------


def test_w09_drafted_plan_is_immutable_across_a_restart(
    scheduler, synced_world: SyncedWorld, repository, db
) -> None:
    """Rule: re-open the plan; no mutation."""
    conn = db.connect(Role.P03_SYNC)
    try:
        victim = conn.execute(
            "SELECT account_id FROM accounts_current WHERE epoch_id = ? LIMIT 1",
            (synced_world.epoch_id,),
        ).fetchone()["account_id"]
    finally:
        conn.close()
    synced_world.harness.corrupt_for_test(
        "UPDATE accounts_current SET balance = '7.77' WHERE epoch_id = ? AND account_id = ?",
        (synced_world.epoch_id, victim),
    )

    outcome = scheduler.run_audit(entity="accounts", epoch_id=synced_world.epoch_id)
    plan = scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1")
    assert plan is not None

    stored = repository.get_plan(plan.plan_id)
    assert stored is not None
    assert stored["plan_hash"] == plan.plan_hash
    assert stored["status"] == "PENDING_APPROVAL"

    # A second planning attempt on the same run does not rewrite the plan.
    again = scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1")
    reread = repository.get_plan(plan.plan_id)
    assert reread["plan_hash"] == plan.plan_hash
    if again is not None:
        # A distinct plan id may be minted, but the original is untouched.
        assert repository.get_plan(plan.plan_id)["status"] == "PENDING_APPROVAL"


# ---------------------------------------------------------------------------
# Window 10: approved, not yet dispatched
# ---------------------------------------------------------------------------


def test_w10_approved_plan_revalidates_preconditions_after_restart(
    scheduler, synced_world: SyncedWorld, db, harness
) -> None:
    """Rule: validate the preconditions again before dispatching."""
    from reconciliation.approvals import Role as ApprovalRole

    conn = db.connect(Role.P03_SYNC)
    try:
        victim = conn.execute(
            "SELECT account_id FROM accounts_current WHERE epoch_id = ? LIMIT 1",
            (synced_world.epoch_id,),
        ).fetchone()["account_id"]
    finally:
        conn.close()
    harness.corrupt_for_test(
        "UPDATE accounts_current SET balance = '8.88' WHERE epoch_id = ? AND account_id = ?",
        (synced_world.epoch_id, victim),
    )
    outcome = scheduler.run_audit(entity="accounts", epoch_id=synced_world.epoch_id)
    plan = scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1")
    assert plan is not None
    scheduler.approvals.decide(
        plan_id=plan.plan_id,
        actor_id="approver-2",
        role=ApprovalRole.APPROVER,
        decision="APPROVE",
        plan_hash=plan.plan_hash,
    )

    # While the worker was down, P03 published a newer scan.
    harness.publish_snapshot(synced_world.epoch_id, "accounts")

    result = scheduler.dispatch(plan)

    assert result.plan_status == "STALE"
    assert result.reason_code is ReasonCode.PLAN_STALE


# ---------------------------------------------------------------------------
# Windows 11 and 12: P03 accepted / committed, P04 never heard
# ---------------------------------------------------------------------------


def test_w11_w12_lost_ack_is_resolved_by_receipt_not_retry(
    scheduler, synced_world: SyncedWorld, db, harness, repository
) -> None:
    """Rule: query the P03 intent receipt; never mint a new intent id."""
    from reconciliation.approvals import Role as ApprovalRole
    from reconciliation.executor import RepairExecutor

    conn = db.connect(Role.P03_SYNC)
    try:
        victim = conn.execute(
            "SELECT account_id FROM accounts_current WHERE epoch_id = ? LIMIT 1",
            (synced_world.epoch_id,),
        ).fetchone()["account_id"]
    finally:
        conn.close()
    harness.corrupt_for_test(
        "UPDATE accounts_current SET balance = '9.99' WHERE epoch_id = ? AND account_id = ?",
        (synced_world.epoch_id, victim),
    )
    outcome = scheduler.run_audit(entity="accounts", epoch_id=synced_world.epoch_id)
    plan = scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1")
    assert plan is not None
    scheduler.approvals.decide(
        plan_id=plan.plan_id,
        actor_id="approver-2",
        role=ApprovalRole.APPROVER,
        decision="APPROVE",
        plan_hash=plan.plan_hash,
    )

    harness.faults.lose_repair_ack.arm(1)
    result = scheduler.dispatch(plan)
    assert harness.faults.lose_repair_ack.fired
    assert result.plan_status == "UNKNOWN"

    intent_id = plan.actions[0].intent_id
    # The durable attempt row records that an intent may be in flight.
    attempts = repository.attempts_for(plan.actions[0].action_id)
    assert any(row["status"] == "UNKNOWN" for row in attempts)

    executor = RepairExecutor(
        repository=repository,
        approvals=scheduler.approvals,
        leases=scheduler.leases,
        read_port=harness,
        repair_port=harness,
        settings=scheduler._settings,
        clock=scheduler._clock,
    )
    resolved = executor.resolve_unknown(
        action_id=plan.actions[0].action_id, intent_id=intent_id
    )

    assert resolved.status == "COMMITTED"
    assert harness.repair_intent_status(intent_id).execution_count == 1


# ---------------------------------------------------------------------------
# Windows 13 and 14: verification interrupted / verified then ACK lost
# ---------------------------------------------------------------------------


def test_w13_w14_verification_restarts_and_verified_is_durable(
    scheduler, synced_world: SyncedWorld, db, harness, repository
) -> None:
    """Rules: restart verification without undoing anything; a durable
    ``VERIFIED`` is returned again rather than re-earned."""
    from reconciliation.approvals import Role as ApprovalRole

    conn = db.connect(Role.P03_SYNC)
    try:
        victim = conn.execute(
            "SELECT account_id FROM accounts_current WHERE epoch_id = ? LIMIT 1",
            (synced_world.epoch_id,),
        ).fetchone()["account_id"]
    finally:
        conn.close()
    harness.corrupt_for_test(
        "UPDATE accounts_current SET balance = '3.33' WHERE epoch_id = ? AND account_id = ?",
        (synced_world.epoch_id, victim),
    )
    outcome = scheduler.run_audit(entity="accounts", epoch_id=synced_world.epoch_id)
    plan = scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1")
    assert plan is not None
    scheduler.approvals.decide(
        plan_id=plan.plan_id,
        actor_id="approver-2",
        role=ApprovalRole.APPROVER,
        decision="APPROVE",
        plan_hash=plan.plan_hash,
    )
    dispatched = scheduler.dispatch(plan)
    assert dispatched.all_committed

    # Window 13: verification is interrupted, then restarted from VERIFYING.
    assert repository.get_plan(plan.plan_id)["status"] == "VERIFYING"
    first = scheduler.verify(plan, entity="accounts")
    assert first.verified is True

    # Window 14: VERIFIED is durable; a replayed verification does not
    # re-open the plan or undo the repair.
    second = scheduler.verify(plan, entity="accounts")
    assert second.verified is False  # already terminal, so not re-verified
    assert repository.get_plan(plan.plan_id)["status"] == "VERIFIED"


# ---------------------------------------------------------------------------
# A genuine process kill, so durability is not only simulated
# ---------------------------------------------------------------------------


def test_real_process_kill_after_page_commit_leaves_valid_evidence(
    tmp_path: Path, settings
) -> None:
    """SIGKILL-equivalent mid-scan: committed pages survive, the flag does not.

    The child process commits two page receipts and then dies without any
    cleanup. A fresh process must find exactly those two receipts, and the
    chain must still refuse to call itself complete.
    """
    db_path = tmp_path / "kill.db"
    script = textwrap.dedent(
        f"""
        import os, sys, uuid
        sys.path.insert(0, {str(Path(__file__).resolve().parents[2] / "src")!r})
        from reconciliation.canonical import page_hash
        from reconciliation.clock import ManualClock
        from reconciliation.config import load_settings
        from reconciliation.db.engine import Database, Role, transaction
        from reconciliation.db.repository import ReconRepository
        from reconciliation.dto import CapturedItem, CapturedPage

        settings = load_settings(env="test", db_path={str(db_path)!r}, source_id="sim_local")
        db = Database(settings.db_path, synchronous="FULL")
        db.create_schema()
        clock = ManualClock()
        repo = ReconRepository(db, settings, clock)

        epoch = str(uuid.uuid4())
        conn = db.connect(Role.P03_SYNC)
        with transaction(conn):
            conn.execute("INSERT INTO sync_sources VALUES('sim_local','local')")
            conn.execute(
                "INSERT INTO source_epochs VALUES(?,?,?,?,?,?)",
                (epoch, 'sim_local', 'run_0001', 1, 'ACTIVE', '2026-01-01T00:00:00.000000Z'),
            )
        conn.close()

        run = repo.create_run(request_id="kill-1", source_id="sim_local",
                              epoch_id=epoch, entity="deals")
        repo.pin_source_identity(run.recon_run_id, run_id_source="run_0001",
                                 generation=1, snapshot_seq=100)

        def page(index, keys, terminal):
            items = [CapturedItem(item_key=k, payload_hash="h-"+k, source_seq=i+1)
                     for i, k in enumerate(keys)]
            return CapturedPage(page_index=index, run_id="run_0001", generation=1,
                                snapshot_seq=100, items=items,
                                page_hash=page_hash("deals", [(i.item_key, i.payload_hash) for i in items]),
                                is_terminal=terminal)

        repo.commit_page(run.recon_run_id, page(0, ["D-1", "D-2"], False))
        repo.commit_page(run.recon_run_id, page(1, ["D-3"], False))
        print(run.recon_run_id, flush=True)
        # Die with no unwinding, no atexit, no flush of anything pending.
        os._exit(137)
        """
    )
    script_path = tmp_path / "killer.py"
    script_path.write_text(script, encoding="utf-8")

    completed = subprocess.run(
        [sys.executable, str(script_path)], capture_output=True, text=True, timeout=120
    )
    assert completed.returncode == 137, completed.stderr
    run_id = completed.stdout.strip().splitlines()[-1]

    # A brand-new process inspects what survived.
    from reconciliation.clock import ManualClock
    from reconciliation.config import load_settings
    from reconciliation.db.engine import Database
    from reconciliation.db.repository import ReconRepository

    fresh_settings = load_settings(env="test", db_path=db_path, source_id="sim_local")
    fresh_db = Database(db_path, synchronous="FULL")
    repo = ReconRepository(fresh_db, fresh_settings, ManualClock())

    assert repo.page_receipt_count(run_id) == 2
    assert repo.source_item_count(run_id) == 3
    run = repo.get_run(run_id)
    assert run is not None
    assert run.source_complete is False

    ok, reason, stats = repo.validate_receipt_chain(run_id)
    assert ok is False
    assert reason is ReasonCode.SOURCE_INCOMPLETE  # no terminal receipt
    assert stats.declared_items == stats.stored_items == 3

    with pytest.raises(SourceError):
        repo.mark_source_complete(run_id)
