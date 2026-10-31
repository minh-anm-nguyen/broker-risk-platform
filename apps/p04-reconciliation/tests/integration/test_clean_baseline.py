"""Fixture F-01-CLEAN: a correctly synced world must audit as MATCHED.

This is the baseline every other integration test depends on. If a clean world
does not come back ``MATCHED`` with zero findings, every "we detected the
corruption" test below it is worthless, because the detection might be noise.

It also pins the two directions of the false-verdict problem:

* a clean world must not produce findings (no false positives), and
* the independent P01 oracle must agree with the manifest the service built,
  so a ``MATCHED`` is not just the service agreeing with itself (section 10.1).
"""

from __future__ import annotations

import pytest

from reconciliation.canonical import payload_hash, root_hash
from reconciliation.db.engine import Role
from reconciliation.errors import ReasonCode
from reconciliation.scheduler import ReconciliationScheduler
from tests.conftest import P01Admin, SyncedWorld, deal_rows, entity_rows

pytestmark = pytest.mark.integration


def test_deals_audit_matches_on_clean_world(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld
) -> None:
    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)

    assert outcome.status == "MATCHED", (outcome.status, outcome.reason_code, outcome.detail)
    assert outcome.reason_code is None
    assert outcome.finding_count == 0
    assert outcome.source_count == outcome.target_count
    assert outcome.source_count and outcome.source_count > 0
    assert outcome.decision is not None and outcome.decision.strict_compare_allowed


@pytest.mark.parametrize("entity", ["accounts", "positions"])
def test_mutable_projections_match_on_clean_world(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld, entity: str
) -> None:
    outcome = scheduler.run_audit(entity=entity, epoch_id=synced_world.epoch_id)

    assert outcome.status == "MATCHED", (entity, outcome.reason_code, outcome.detail)
    assert outcome.finding_count == 0
    # The mutable rule actually applied: equal cuts, not the deal watermark rule.
    assert outcome.decision is not None
    assert outcome.decision.detail["cut"]["rule"] == "mutable_projection_requires_equal_S"


def test_matched_run_records_complete_provable_evidence(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld, repository
) -> None:
    """A MATCHED verdict must be backed by a provable receipt chain."""
    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)
    assert outcome.status == "MATCHED"

    run = repository.get_run(outcome.recon_run_id)
    assert run is not None
    assert run.source_complete is True
    assert run.target_complete is True
    assert run.source_root_hash == run.target_root_hash
    assert run.source_snapshot_seq is not None
    assert run.target_deal_watermark is not None
    assert run.target_deal_watermark >= run.source_snapshot_seq

    ok, reason, stats = repository.validate_receipt_chain(outcome.recon_run_id)
    assert ok is True and reason is None
    assert stats.terminal_count == 1
    assert stats.terminal_index == stats.max_page_index
    assert stats.declared_items == stats.stored_items
    assert stats.distinct_cuts == 1


def test_independent_p01_oracle_agrees_with_the_captured_manifest(
    scheduler: ReconciliationScheduler,
    synced_world: SyncedWorld,
    p01_admin: P01Admin,
    p01_client,
    db,
) -> None:
    """The service's manifest must equal an oracle built outside the service.

    The oracle re-reads P01 directly and recomputes the root hash from the
    returned rows. It shares the canonical encoder (that is the contract being
    asserted), but it does not share the capture path, the pagination loop or
    the database, so a bug in any of those is caught here rather than agreeing
    with itself (section 10.1).
    """
    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)
    assert outcome.status == "MATCHED"

    oracle_rows = deal_rows(p01_admin, p01_client)
    expected = root_hash(
        "deals",
        [(row["deal_id"], payload_hash("deals", row)) for row in oracle_rows],
    )

    conn = db.connect(Role.RECON)
    try:
        stored = conn.execute(
            "SELECT source_root_hash, target_root_hash, source_count FROM recon_runs WHERE recon_run_id = ?",
            (outcome.recon_run_id,),
        ).fetchone()
    finally:
        conn.close()

    assert stored["source_count"] == len(oracle_rows)
    assert stored["source_root_hash"] == expected
    assert stored["target_root_hash"] == expected


def test_account_manifest_agrees_with_independent_oracle(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld, p01_client, db
) -> None:
    outcome = scheduler.run_audit(entity="accounts", epoch_id=synced_world.epoch_id)
    assert outcome.status == "MATCHED"

    rows = entity_rows(p01_client, "accounts")
    expected = root_hash(
        "accounts",
        [(row["account_id"], payload_hash("accounts", row)) for row in rows],
    )
    conn = db.connect(Role.RECON)
    try:
        stored = conn.execute(
            "SELECT source_root_hash, target_root_hash FROM recon_runs WHERE recon_run_id = ?",
            (outcome.recon_run_id,),
        ).fetchone()
    finally:
        conn.close()
    assert stored["source_root_hash"] == expected
    assert stored["target_root_hash"] == expected


def test_clean_world_yields_no_plan(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld
) -> None:
    """Nothing to fix means no plan exists to be approved or dispatched."""
    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)
    assert outcome.status == "MATCHED"
    assert scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1") is None


def test_repeated_trigger_is_idempotent(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld
) -> None:
    """A retried trigger returns the original run, not a second audit.

    Section 7.6: after a lost response the client retries. Creating a second
    audit of the same scope would double the load and produce two verdicts
    for one question.
    """
    request_id = "trigger-abc-123"
    first = scheduler.run_audit(
        entity="deals", epoch_id=synced_world.epoch_id, request_id=request_id
    )
    second = scheduler.run_audit(
        entity="deals", epoch_id=synced_world.epoch_id, request_id=request_id
    )

    assert first.recon_run_id == second.recon_run_id
    assert second.status == first.status == "MATCHED"
    assert second.detail.get("recovered") is True


def test_p04_cannot_write_p03_tables(db) -> None:
    """INV04-07 and INV04-18, enforced by the engine rather than by review."""
    from tests.conftest import assert_no_p03_mutation_by_p04

    assert_no_p03_mutation_by_p04(db)
