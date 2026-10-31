"""Source-boundary faults: F-07, F-08, F-16, IT04-05, IT04-10.

The source is untrusted (section 3.2). These tests make it misbehave in the
specific ways the design anticipates and assert that the audit fails *closed*
every time:

* **410 mid-scan** (F-08): a genuine ``SNAPSHOT_EXPIRED`` from P01 after its
  cursor TTL elapses. Never resumed, never partially promoted.
* **Incomplete pagination** (IT04-05): a scan that dies before the terminal
  page must emit **zero** proven absence findings, no matter how many rows the
  target appears to have in excess.
* **Source reset mid-scan** (F-07): a new generation ends the run with
  ``EPOCH_MISMATCH``; no plan is produced.
* **Silent truncation** (IT04-10): a source that omits a row while claiming a
  terminal page is protocol-indistinguishable from a correct empty-ish
  listing. P04 must not delete the target row, and the **independent P01
  oracle** must catch the source lying -- which is the whole point of the
  exercise.
* **Mass count drop** (F-16): a sharp drop in source rows is escalated, not
  actioned.

Every fault asserts its own activation where the simulator exposes a counter,
because a test whose fault never fired is not evidence (section 10.6).
"""

from __future__ import annotations

import time

import pytest

from reconciliation.db.engine import Role
from reconciliation.dto import SourceCapabilities
from reconciliation.errors import (
    DataIntegrityError,
    FindingCode,
    ReasonCode,
    SnapshotExpiredError,
    SourceError,
)
from reconciliation.ports.source import FakeSource
from reconciliation.scheduler import ReconciliationScheduler
from tests.conftest import SyncedWorld, deal_rows, make_scheduler

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# F-08: genuine 410 SNAPSHOT_EXPIRED from the real simulator
# ---------------------------------------------------------------------------


class TestGenuineSnapshotExpiry:
    """Uses a 1-second cursor TTL so the 410 is real, not simulated."""

    @pytest.fixture
    def p01_ttl_seconds(self) -> int:
        return 1

    def test_expired_cursor_raises_and_is_never_resumed(
        self, p01_admin, source, p01_client
    ) -> None:
        p01_admin.create_run(accounts=3, deals=30, max_open_positions=3)

        pages = source.iter_pages("deals", page_size=5)
        first = next(pages)
        assert first.next_cursor is not None, "fixture needs more than one page"

        # Outlive the cursor TTL. This is the one place a real sleep is
        # correct: the expiry being tested is a wall-clock property of P01.
        time.sleep(1.3)

        with pytest.raises(SnapshotExpiredError) as exc:
            next(pages)
        assert exc.value.reason is ReasonCode.SNAPSHOT_EXPIRED

    def test_capture_interrupted_by_expiry_is_inconclusive_with_no_findings(
        self, db, repository, source, harness, settings, clock, p01_admin
    ) -> None:
        """IT04-05's core claim, driven by a genuine P01 ``410``.

        The delay is applied on the *client* side rather than with P01's
        ``delay_before_response`` fault, because that fault lands before the
        cursor is minted: the lease would start after the delay and never
        expire, and the case would quietly stop testing expiry at all.
        Sleeping between pages puts the elapsed time exactly where it has to
        be — after page 1's cursor was issued, before page 2 is requested.

        The ``410`` itself still comes from P01, so the oracle is the
        simulator's own response, and ``SNAPSHOT_EXPIRED`` in the outcome is
        the proof it occurred.
        """
        from reconciliation.config import load_settings

        p01_admin.create_run(accounts=3, deals=30, max_open_positions=3)
        epoch_id = harness.bootstrap_epoch()
        harness.sync_deals(epoch_id)

        class SleepsBetweenPages:
            """Delegates to the real source, pausing past the cursor TTL."""

            def __init__(self, inner) -> None:
                self._inner = inner
                self.pages_yielded = 0

            def read_identity(self):
                return self._inner.read_identity()

            def read_capabilities(self):
                return self._inner.read_capabilities()

            def iter_pages(self, entity, *, scope="UNFILTERED", page_size=None):
                for page in self._inner.iter_pages(
                    entity, scope=scope, page_size=page_size
                ):
                    self.pages_yielded += 1
                    yield page
                    if self.pages_yielded == 1:
                        # Outlive the 1s TTL before page 2 is fetched.
                        time.sleep(1.4)

        # A small page size is essential, not cosmetic: all 30 deals fit in
        # one 100-row page, and a single-request scan has no later request for
        # an expired cursor to fail.
        tight = load_settings(
            env="test",
            db_path=settings.db_path,
            artifacts_dir=settings.artifacts_dir,
            source_id="sim_local",
            source_max_page_size=5,
        )
        slow_source = SleepsBetweenPages(source)
        repo = type(repository)(db, tight, clock)
        scheduler = make_scheduler(
            db=db,
            repository=repo,
            source=slow_source,
            harness=harness,
            settings=tight,
            clock=clock,
        )

        outcome = scheduler.run_audit(entity="deals", epoch_id=epoch_id)

        assert slow_source.pages_yielded >= 1, "the scan must have started"
        assert outcome.status == "INCONCLUSIVE"
        # This reason code can only come from a real 410 served by P01.
        assert outcome.reason_code is ReasonCode.SNAPSHOT_EXPIRED

        run = repo.get_run(outcome.recon_run_id)
        assert run is not None and run.source_complete is False
        # Not one proven finding came out of a partial manifest.
        assert all(f["confidence"] != "PROVEN" for f in outcome.findings)
        assert scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1") is None


# ---------------------------------------------------------------------------
# IT04-05: incomplete pagination must not cause a false deletion
# ---------------------------------------------------------------------------


def test_partial_scan_emits_no_absence_findings_even_with_many_extra_target_rows(
    db, repository, harness, settings, clock, synced_world: SyncedWorld
) -> None:
    """IT04-05 verbatim: the last page fails, target holds rows the partial
    manifest never saw, and **zero** proven absence findings are emitted.

    A deterministic ``FakeSource`` is used because the test needs the failure
    at a precise page boundary. The data it serves is the real deal set, so
    the target genuinely contains rows the truncated scan did not reach.
    """
    rows = list(harness.iter_target_rows(synced_world.epoch_id, "deals"))
    assert len(rows) > 6

    fake = FakeSource(
        source_id="sim_local",
        run_id=synced_world.run_id,
        generation=synced_world.generation,
        snapshot_seq=synced_world.snapshot_seq,
        records={"deals": rows},
        page_size=3,
    )
    # Page 2 dies: pages 0 and 1 are durable, the rest were never read.
    fake.fail_at_page[2] = SnapshotExpiredError("snapshot expired on the final page")

    scheduler = make_scheduler(
        db=db, repository=repository, source=fake, harness=harness, settings=settings, clock=clock
    )
    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)

    assert outcome.status == "INCONCLUSIVE"
    assert outcome.reason_code is ReasonCode.SNAPSHOT_EXPIRED

    run = repository.get_run(outcome.recon_run_id)
    assert run is not None
    assert run.source_complete is False
    assert run.target_complete is False, "no target work may follow an incomplete source scan"

    # The decisive assertion: no EXTRA/absence finding was produced, although
    # the target holds many rows the partial manifest lacks.
    forbidden = {
        FindingCode.EXTRA_TARGET_DEAL.value,
        FindingCode.ABSENT_AT_SOURCE_ACCOUNT.value,
        FindingCode.ABSENT_AT_SOURCE_POSITION.value,
    }
    assert not [f for f in outcome.findings if f["finding_code"] in forbidden]
    assert scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1") is None

    # Durable receipts exist for the pages that did land, and the chain
    # correctly refuses to call itself complete.
    assert repository.page_receipt_count(outcome.recon_run_id) == 2
    ok, reason, _stats = repository.validate_receipt_chain(outcome.recon_run_id)
    assert ok is False
    assert reason is ReasonCode.SOURCE_INCOMPLETE


def test_missing_middle_page_cannot_complete(
    db, repository, harness, settings, clock, synced_world: SyncedWorld
) -> None:
    """A gap in the receipt chain is detected even when a terminal page exists.

    This is the subtle version of IT04-05: the scan *did* reach a terminal
    page, but page 1 is absent, so ``MAX(page_index)+1 != COUNT(receipts)``.
    """
    from reconciliation.canonical import page_hash, payload_hash
    from reconciliation.dto import CapturedItem, CapturedPage

    run = repository.create_run(
        request_id="gap-1",
        source_id="sim_local",
        epoch_id=synced_world.epoch_id,
        entity="deals",
    )
    repository.pin_source_identity(
        run.recon_run_id,
        run_id_source=synced_world.run_id,
        generation=synced_world.generation,
        snapshot_seq=synced_world.snapshot_seq,
    )

    def page(index: int, keys: list[str], terminal: bool) -> CapturedPage:
        items = [CapturedItem(item_key=k, payload_hash=f"h-{k}", source_seq=i + 1) for i, k in enumerate(keys)]
        return CapturedPage(
            page_index=index,
            run_id=synced_world.run_id,
            generation=synced_world.generation,
            snapshot_seq=synced_world.snapshot_seq,
            items=items,
            page_hash=page_hash("deals", [(i.item_key, i.payload_hash) for i in items]),
            is_terminal=terminal,
        )

    repository.commit_page(run.recon_run_id, page(0, ["D-1"], False))
    repository.commit_page(run.recon_run_id, page(2, ["D-3"], True))  # page 1 missing

    ok, reason, stats = repository.validate_receipt_chain(run.recon_run_id)
    assert ok is False
    assert reason is ReasonCode.SOURCE_PAGE_MISSING
    assert stats.receipt_count == 2 and stats.max_page_index == 2

    with pytest.raises(SourceError) as exc:
        repository.mark_source_complete(run.recon_run_id)
    assert exc.value.reason is ReasonCode.SOURCE_PAGE_MISSING


# ---------------------------------------------------------------------------
# F-07: source reset mid-scan
# ---------------------------------------------------------------------------


def test_generation_change_between_pages_is_epoch_mismatch(
    db, repository, harness, settings, clock, synced_world: SyncedWorld
) -> None:
    """F-07: pages 2..N must match page 1's (run_id, generation)."""
    rows = list(harness.iter_target_rows(synced_world.epoch_id, "deals"))
    fake = FakeSource(
        source_id="sim_local",
        run_id=synced_world.run_id,
        generation=synced_world.generation,
        snapshot_seq=synced_world.snapshot_seq,
        records={"deals": rows},
        page_size=4,
    )

    def bump_generation(index, page):
        if index < 2:
            return page
        from dataclasses import replace

        return replace(page, generation=page.generation + 1)

    fake.mutate_page = bump_generation

    scheduler = make_scheduler(
        db=db, repository=repository, source=fake, harness=harness, settings=settings, clock=clock
    )
    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)

    assert outcome.status == "INCONCLUSIVE"
    assert outcome.reason_code is ReasonCode.EPOCH_MISMATCH
    assert scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1") is None


def test_snapshot_drift_between_pages_is_detected(
    db, repository, harness, settings, clock, synced_world: SyncedWorld
) -> None:
    """Two pages from two snapshots must never be stitched into one manifest."""
    rows = list(harness.iter_target_rows(synced_world.epoch_id, "deals"))
    fake = FakeSource(
        source_id="sim_local",
        run_id=synced_world.run_id,
        generation=synced_world.generation,
        snapshot_seq=synced_world.snapshot_seq,
        records={"deals": rows},
        page_size=4,
    )

    def drift_snapshot(index, page):
        if index < 2:
            return page
        from dataclasses import replace

        return replace(page, snapshot_seq=page.snapshot_seq + 5)

    fake.mutate_page = drift_snapshot

    scheduler = make_scheduler(
        db=db, repository=repository, source=fake, harness=harness, settings=settings, clock=clock
    )
    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)

    assert outcome.status == "INCONCLUSIVE"
    assert outcome.reason_code is ReasonCode.SOURCE_CUT_DRIFTED


def test_source_reset_before_audit_blocks_the_old_epoch(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld, p01_admin
) -> None:
    """INV04-02: no strict diff when the source and target epochs disagree.

    After a reset the old epoch cannot be compared with what the source now
    serves, so the run is blocked rather than diffed.
    """
    p01_admin.reset_run(synced_world.run_id, expected_generation=synced_world.generation)

    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)

    assert outcome.status == "INCONCLUSIVE"
    assert outcome.reason_code is ReasonCode.EPOCH_MISMATCH
    assert scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1") is None


# ---------------------------------------------------------------------------
# IT04-10: silent truncation and the independent oracle
# ---------------------------------------------------------------------------


def test_silently_truncated_source_does_not_cause_a_delete_and_is_caught_by_the_oracle(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld, p01_admin, p01_client, db
) -> None:
    """IT04-10: the source omits a row while claiming a terminal page.

    Protocol-wise the response is valid, so reconciliation alone cannot know
    the source lied. Two things must hold:

    1. P04 must **not** delete the target row it now appears to have in
       excess -- it records ``EXTRA_TARGET_DEAL`` and proposes nothing.
    2. The independent P01 oracle (read with faults cleared) must detect the
       source-contract violation, so the test fails even though every
       individual response was syntactically fine.
    """
    truth = {row["deal_id"] for row in deal_rows(p01_admin, p01_client)}
    assert truth

    rule = p01_admin.add_fault(
        mode="drop_one_item",
        parameters={"selection": "first"},
        path="/v1/deals",
        trigger="always",
        max_activations=1,
    )

    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)

    # 1. The fault really fired, so this case tested its own premise.
    assert p01_admin.fault_activations(rule) == 1

    # 2. P04 recorded an extra-target finding and proposed no destructive act.
    assert outcome.status == "FINDINGS"
    extras = [
        f for f in outcome.findings if f["finding_code"] == FindingCode.EXTRA_TARGET_DEAL.value
    ]
    assert len(extras) == 1
    plan = scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1")
    assert plan is not None
    assert plan.dispatchable_actions == (), "no action may follow from a possibly-lying source"

    victim = extras[0]["item_key"]
    conn = db.connect(Role.P03_SYNC)
    try:
        still_there = conn.execute(
            "SELECT COUNT(*) FROM deals WHERE epoch_id = ? AND deal_id = ?",
            (synced_world.epoch_id, victim),
        ).fetchone()[0]
    finally:
        conn.close()
    assert still_there == 1, "the target row must survive a source that omitted it"

    # 3. The independent oracle proves the source violated its own contract.
    p01_admin.clear_faults()
    after = {row["deal_id"] for row in deal_rows(p01_admin, p01_client)}
    assert victim in after, (
        "ORACLE_VIOLATION: the deal the source omitted does exist, so the "
        "source lied about a complete listing"
    )
    assert after == truth


# ---------------------------------------------------------------------------
# F-16: large count drop and the empty listing
# ---------------------------------------------------------------------------


def test_sharp_source_count_drop_is_escalated_not_actioned(
    db, repository, harness, settings, clock, synced_world: SyncedWorld
) -> None:
    """F-16 / FMEA "zero record count from source": escalate, never delete."""
    fake = FakeSource(
        source_id="sim_local",
        run_id=synced_world.run_id,
        generation=synced_world.generation,
        snapshot_seq=synced_world.snapshot_seq,
        records={"deals": []},  # a complete, terminal, *empty* listing
        page_size=100,
    )
    scheduler = make_scheduler(
        db=db, repository=repository, source=fake, harness=harness, settings=settings, clock=clock
    )

    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)

    assert outcome.status == "FINDINGS"
    assert outcome.source_count == 0
    assert outcome.target_count and outcome.target_count > 0
    assert any(
        f["finding_code"] == FindingCode.LARGE_SOURCE_COUNT_DROP.value for f in outcome.findings
    )

    plan = scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1")
    assert plan is not None
    assert plan.dispatchable_actions == ()
    assert FindingCode.LARGE_SOURCE_COUNT_DROP.value in plan.escalations


def test_truly_empty_world_matches_without_a_drop_alert(
    db, repository, harness, settings, clock, p01_admin
) -> None:
    """F-16's other half: empty on both sides is MATCHED, not a crash."""
    p01_admin.create_run(accounts=0, deals=0, max_open_positions=0)
    epoch_id = harness.bootstrap_epoch()
    harness.sync_deals(epoch_id)

    from reconciliation.scheduler import ReconciliationScheduler as S

    scheduler = make_scheduler(
        db=db,
        repository=repository,
        source=harness._source,  # the real P01 source behind the harness
        harness=harness,
        settings=settings,
        clock=clock,
    )
    outcome = scheduler.run_audit(entity="deals", epoch_id=epoch_id)

    assert outcome.status == "MATCHED"
    assert outcome.source_count == 0 and outcome.target_count == 0
    assert outcome.finding_count == 0


# ---------------------------------------------------------------------------
# Transport faults and payload validation
# ---------------------------------------------------------------------------


def test_repeated_503_exhausts_bounded_retries_and_stays_inconclusive(
    scheduler: ReconciliationScheduler, synced_world: SyncedWorld, p01_admin, repository
) -> None:
    rule = p01_admin.add_fault(
        mode="http_error",
        parameters={"status": 503},
        path="/v1/deals",
        trigger="always",
        max_activations=50,
    )

    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)

    assert p01_admin.fault_activations(rule) >= 1
    assert outcome.status == "INCONCLUSIVE"
    assert outcome.reason_code is ReasonCode.SOURCE_TRANSPORT_FAILED
    run = repository.get_run(outcome.recon_run_id)
    assert run is not None and run.source_complete is False


@pytest.mark.parametrize(
    ("mode", "parameters"),
    [
        ("malformed_json", {}),
        ("wrong_content_type", {}),
        ("truncate_response", {"keep_bytes": 10}),
    ],
)
def test_malformed_source_responses_are_refused_at_the_boundary(
    scheduler: ReconciliationScheduler,
    synced_world: SyncedWorld,
    p01_admin,
    mode: str,
    parameters: dict,
) -> None:
    """A broken payload is a source fault, never a data difference."""
    rule = p01_admin.add_fault(
        mode=mode, parameters=parameters, path="/v1/deals", trigger="always", max_activations=50
    )

    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)

    assert p01_admin.fault_activations(rule) >= 1
    assert outcome.status == "INCONCLUSIVE"
    assert outcome.reason_code in {
        ReasonCode.SOURCE_PAYLOAD_INVALID,
        ReasonCode.SOURCE_TRANSPORT_FAILED,
    }
    assert outcome.finding_count == 0 or all(
        f["confidence"] != "PROVEN" for f in outcome.findings
    )


def test_duplicate_item_across_pages_is_a_source_contract_failure(
    db, repository, harness, settings, clock, synced_world: SyncedWorld
) -> None:
    """A repeated key cannot be silently deduplicated into a valid manifest."""
    rows = list(harness.iter_target_rows(synced_world.epoch_id, "deals"))
    duplicated = rows + [rows[0]]
    fake = FakeSource(
        source_id="sim_local",
        run_id=synced_world.run_id,
        generation=synced_world.generation,
        snapshot_seq=synced_world.snapshot_seq,
        records={"deals": duplicated},
        page_size=4,
    )
    scheduler = make_scheduler(
        db=db, repository=repository, source=fake, harness=harness, settings=settings, clock=clock
    )

    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)

    assert outcome.status == "INCONCLUSIVE"
    assert outcome.reason_code is ReasonCode.SOURCE_DUPLICATE_KEY


def test_source_without_stable_snapshot_capability_cannot_be_strictly_compared(
    db, repository, harness, settings, clock, synced_world: SyncedWorld
) -> None:
    """Section 2.3: no stable snapshot means diagnostics only, no verdict."""
    rows = list(harness.iter_target_rows(synced_world.epoch_id, "deals"))
    fake = FakeSource(
        source_id="sim_local",
        run_id=synced_world.run_id,
        generation=synced_world.generation,
        snapshot_seq=synced_world.snapshot_seq,
        records={"deals": rows},
        capabilities=SourceCapabilities(stable_snapshot_pagination=False),
        page_size=100,
    )
    scheduler = make_scheduler(
        db=db, repository=repository, source=fake, harness=harness, settings=settings, clock=clock
    )

    outcome = scheduler.run_audit(entity="deals", epoch_id=synced_world.epoch_id)

    assert outcome.status == "INCONCLUSIVE"
    assert outcome.reason_code is ReasonCode.UNSUPPORTED_CAPABILITY
    assert scheduler.plan_repair(outcome.recon_run_id, created_by="auditor-1") is None
