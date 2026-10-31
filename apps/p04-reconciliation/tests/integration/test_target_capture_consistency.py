"""Target capture must be one consistent cut: IT04-07, INV04-14.

Section 7.3 names the failure precisely: paging a mutable projection across
several statements while the publisher promotes a new scan, then believing all
the pages belonged to one publication. The result is a manifest that never
existed in the database -- a *hybrid* -- and every verdict derived from it is
meaningless.

The tests below attack that property with real concurrency rather than
asserting it by inspection:

* ``test_capture_never_mixes_two_atomic_publications`` runs captures while a
  background thread flips the whole projection between two versions inside
  single transactions. Every successful capture must be entirely one version.
* The ``DEFERRED_SNAPSHOT`` strategy is shown to **fail closed**: when the
  publisher commits under a pinned read snapshot, the capture's own write is
  refused and ``target_complete`` stays false.
* The ``IMMEDIATE`` strategy is shown to exclude the publisher for the
  duration of the capture.

The hybrid detector is exact: each version gives every account a known
balance, so each account has two possible canonical digests and a mixed
capture is directly observable.
"""

from __future__ import annotations

import sqlite3
import threading
import time

import pytest

from reconciliation.canonical import payload_hash
from reconciliation.db.engine import CaptureStrategy, Role, capture_transaction, transaction
from reconciliation.db.repository import ReconRepository
from reconciliation.errors import ReasonCode, TargetConflictError
from reconciliation.target_capture import TargetCaptureWorker
from tests.conftest import SyncedWorld

pytestmark = pytest.mark.integration

VERSION_A = "1111.11"
VERSION_B = "2222.22"


def _account_ids(db, epoch_id: str) -> list[str]:
    conn = db.connect(Role.P03_SYNC)
    try:
        return [
            row["account_id"]
            for row in conn.execute(
                "SELECT account_id FROM accounts_current WHERE epoch_id = ? ORDER BY account_id",
                (epoch_id,),
            )
        ]
    finally:
        conn.close()


def _expected_hashes(account_ids: list[str], balance: str) -> set[str]:
    """Canonical digests for "every account holds this balance"."""
    return {
        payload_hash(
            "accounts",
            {"account_id": aid, "currency": "USD", "balance": balance, "credit": "0.00"},
        )
        for aid in account_ids
    }


def _publish_version(db, epoch_id: str, balance: str) -> None:
    """One atomic publication of a whole projection version.

    A single transaction replacing every row is exactly what P03's atomic
    promotion does; writing it directly here lets the test control the
    interleaving precisely.
    """
    conn = db.connect(Role.P03_SYNC)
    try:
        with transaction(conn):
            conn.execute(
                "UPDATE accounts_current SET balance = ?, credit = '0.00' WHERE epoch_id = ?",
                (balance, epoch_id),
            )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# The hybrid test
# ---------------------------------------------------------------------------


def test_capture_never_mixes_two_atomic_publications(
    db, repository: ReconRepository, settings, clock, synced_world: SyncedWorld
) -> None:
    """IT04-07: captured items correspond to exactly one publication."""
    epoch_id = synced_world.epoch_id
    account_ids = _account_ids(db, epoch_id)
    assert len(account_ids) >= 4, "need several accounts for a meaningful interleave"

    hashes_a = _expected_hashes(account_ids, VERSION_A)
    hashes_b = _expected_hashes(account_ids, VERSION_B)
    _publish_version(db, epoch_id, VERSION_A)

    stop = threading.Event()
    publish_count = 0

    def publisher() -> None:
        nonlocal publish_count
        toggle = True
        while not stop.is_set():
            try:
                _publish_version(db, epoch_id, VERSION_B if toggle else VERSION_A)
                publish_count += 1
                toggle = not toggle
            except sqlite3.OperationalError:
                # Locked out by an in-flight IMMEDIATE capture. Correct: the
                # publisher waits rather than tearing a capture in half.
                pass
            # Yield between publications. Without this the publisher holds the
            # write lock almost continuously and the captures spend the test
            # waiting on it, which measures lock contention rather than the
            # hybrid-read property under test.
            time.sleep(0.01)

    worker = TargetCaptureWorker(
        db=db,
        repository=repository,
        settings=settings,
        clock=clock,
        strategy=CaptureStrategy.IMMEDIATE,
    )

    thread = threading.Thread(target=publisher, daemon=True)
    thread.start()
    try:
        completed = 0
        conflicts = 0
        for attempt in range(10):
            run = repository.create_run(
                request_id=f"hybrid-{attempt}",
                source_id="sim_local",
                epoch_id=epoch_id,
                entity="accounts",
            )
            result = worker.capture(
                run.recon_run_id,
                entity="accounts",
                epoch_id=epoch_id,
                source_snapshot_seq=synced_world.snapshot_seq,
            )
            if not result.complete:
                # The only acceptable failure is an unprovable cut.
                assert result.reason_code in {
                    ReasonCode.TARGET_CAPTURE_CONFLICT,
                    ReasonCode.TARGET_DB_ERROR,
                }
                stored = repository.get_run(run.recon_run_id)
                assert stored is not None and stored.target_complete is False
                conflicts += 1
                continue

            completed += 1
            with repository.session() as conn:
                captured = {
                    row["payload_hash"]
                    for row in conn.execute(
                        "SELECT payload_hash FROM recon_target_items WHERE recon_run_id = ?",
                        (run.recon_run_id,),
                    )
                }
            assert captured, "a complete capture cannot be empty here"
            # The decisive assertion: wholly version A or wholly version B.
            assert captured <= hashes_a or captured <= hashes_b, (
                "captured manifest mixes two publications (hybrid read)"
            )
            assert len(captured) == len(account_ids)
    finally:
        stop.set()
        thread.join(timeout=5)

    assert publish_count > 0, "the publisher must actually have published"
    assert completed > 0, "at least one capture must have succeeded"


# ---------------------------------------------------------------------------
# Strategy behaviour, at the transaction boundary
# ---------------------------------------------------------------------------


def test_deferred_snapshot_capture_fails_closed_on_concurrent_publish(
    db, synced_world: SyncedWorld
) -> None:
    """A stale-but-consistent read is refused, not promoted.

    The capture pins a read snapshot and reads the P03 metadata. The publisher
    then commits. The capture's write is refused with ``SQLITE_BUSY_SNAPSHOT``,
    which this service reports as ``TARGET_CAPTURE_CONFLICT``.
    """
    epoch_id = synced_world.epoch_id
    _publish_version(db, epoch_id, VERSION_A)

    conn = db.connect(Role.RECON)
    try:
        with pytest.raises(TargetConflictError) as exc:
            with capture_transaction(conn, strategy=CaptureStrategy.DEFERRED_SNAPSHOT):
                # Pin the snapshot by reading the metadata the capture relies on.
                pinned = conn.execute(
                    "SELECT published_scan_id FROM sync_checkpoints WHERE epoch_id = ? AND entity = 'accounts'",
                    (epoch_id,),
                ).fetchone()
                assert pinned is not None

                # Another writer publishes a new version underneath us.
                _publish_version(db, epoch_id, VERSION_B)

                # Reads still show the pinned snapshot: internally consistent,
                # but no longer the current state of the database.
                balances = {
                    row["balance"]
                    for row in conn.execute(
                        "SELECT balance FROM accounts_current WHERE epoch_id = ?", (epoch_id,)
                    )
                }
                assert balances == {VERSION_A}

                conn.execute(
                    """
                    INSERT INTO recon_target_items (recon_run_id, item_key, source_seq, payload_hash)
                    SELECT 'stale-run', account_id, NULL, payload_hash
                      FROM accounts_current WHERE epoch_id = ?
                    """,
                    (epoch_id,),
                )
        assert exc.value.reason is ReasonCode.TARGET_CAPTURE_CONFLICT
    finally:
        conn.close()

    # Nothing from the refused capture survived.
    conn = db.connect(Role.RECON)
    try:
        leftover = conn.execute(
            "SELECT COUNT(*) FROM recon_target_items WHERE recon_run_id = 'stale-run'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert leftover == 0


def test_immediate_capture_excludes_the_publisher(db, synced_world: SyncedWorld) -> None:
    """The IMMEDIATE strategy holds the write lock for the whole capture."""
    epoch_id = synced_world.epoch_id
    conn = db.connect(Role.RECON)
    blocked: list[str] = []

    def try_publish() -> None:
        other = sqlite3.connect(db.path, isolation_level=None, timeout=0.2)
        try:
            other.execute("BEGIN IMMEDIATE")
            other.execute(
                "UPDATE accounts_current SET balance = ? WHERE epoch_id = ?", (VERSION_B, epoch_id)
            )
            other.execute("COMMIT")
        except sqlite3.OperationalError as exc:
            blocked.append(str(exc))
        finally:
            other.close()

    try:
        with capture_transaction(conn, strategy=CaptureStrategy.IMMEDIATE):
            conn.execute("SELECT COUNT(*) FROM accounts_current WHERE epoch_id = ?", (epoch_id,))
            thread = threading.Thread(target=try_publish)
            thread.start()
            thread.join(timeout=5)
    finally:
        conn.close()

    assert blocked, "the publisher should have been locked out during the capture"
    assert "locked" in blocked[0].lower()


def test_capture_refuses_when_no_scan_is_published(
    db, repository: ReconRepository, settings, clock, p01_admin, harness
) -> None:
    """No published scan means no target evidence, so no comparison."""
    p01_admin.create_run(accounts=3, deals=10, max_open_positions=3)
    epoch_id = harness.bootstrap_epoch()  # no publish at all

    run = repository.create_run(
        request_id="unpublished-1",
        source_id="sim_local",
        epoch_id=epoch_id,
        entity="accounts",
    )
    worker = TargetCaptureWorker(
        db=db, repository=repository, settings=settings, clock=clock
    )

    result = worker.capture(
        run.recon_run_id, entity="accounts", epoch_id=epoch_id, source_snapshot_seq=10
    )

    assert result.complete is False
    assert result.reason_code is ReasonCode.TARGET_NO_PUBLISHED_SCAN
    stored = repository.get_run(run.recon_run_id)
    assert stored is not None and stored.target_complete is False


def test_capture_recomputes_hashes_and_ignores_p03_stored_hash(
    db, repository: ReconRepository, settings, clock, synced_world: SyncedWorld
) -> None:
    """Section 6.4: P03's own payload_hash is never trusted for comparison.

    The harness stores a deliberately different digest. If the capture copied
    it instead of recomputing from business fields, every row would mismatch.
    Corrupting the stored hash alone must therefore change nothing.
    """
    epoch_id = synced_world.epoch_id
    synced_world.harness.corrupt_for_test(
        "UPDATE accounts_current SET payload_hash = 'deadbeef' WHERE epoch_id = ?", (epoch_id,)
    )

    run = repository.create_run(
        request_id="rehash-1", source_id="sim_local", epoch_id=epoch_id, entity="accounts"
    )
    worker = TargetCaptureWorker(db=db, repository=repository, settings=settings, clock=clock)
    result = worker.capture(
        run.recon_run_id,
        entity="accounts",
        epoch_id=epoch_id,
        source_snapshot_seq=synced_world.snapshot_seq,
    )

    assert result.complete is True
    with repository.session() as conn:
        stored = [
            row["payload_hash"]
            for row in conn.execute(
                "SELECT payload_hash FROM recon_target_items WHERE recon_run_id = ?",
                (run.recon_run_id,),
            )
        ]
    assert stored and "deadbeef" not in stored
    assert all(len(digest) == 64 for digest in stored)
