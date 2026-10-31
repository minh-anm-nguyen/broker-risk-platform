"""Project 03 **contract harness** -- not Project 03 itself.

Project 03 does not exist yet in this repository. Project 04 is defined
entirely in terms of two narrow ports (``reconciliation.ports.p03``), so this
module implements those ports with the semantics Project 03's design
specifies, and owns the P03 tables while doing it. It exists to give the
reconciliation service a counterparty that behaves the way the contract says,
including the ways it is allowed to fail.

What it deliberately gets *right*, because P04's correctness depends on it:

* **The Deal watermark advances only after a terminal page.** A partial scan
  leaves ``W`` where it was, which is what makes ``TARGET_NOT_CAUGHT_UP`` a
  real state rather than a hypothetical one (section 2.1, INV04-04).
* **Account and Position snapshots publish atomically** from staging, so a
  reader sees the whole old scan or the whole new one (section 2.1, IT04-07).
* **Repair intents dedup on ``intent_id``** and re-verify epoch, lease version
  and expected revision *at the commit boundary*, not when the request
  arrives. A caller-side precondition is not a guard (INV04-19).
* **It computes its own ``payload_hash`` with its own encoder.** This is
  intentional: section 6.4 warns that P03's stored hash must not be assumed
  comparable with P04's. Using a deliberately different scheme here means any
  code that shortcuts to comparing stored hashes fails the tests instead of
  passing by luck.

What it is **not**: a performance-representative sync service, a replacement
for Project 03's own test suite, or evidence that Project 03 works. Results
obtained against this harness are evidence about Project 04 only.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from reconciliation.canonical import canonical_json_bytes
from reconciliation.clock import Clock, to_iso
from reconciliation.db.engine import Database, Role, transaction
from reconciliation.dto import RepairReceipt, SyncStatus
from reconciliation.errors import ReasonCode, ReconError, SourceError
from reconciliation.ports.source import SourcePort

#: Business columns per entity, in the order the harness stores them.
_COLUMNS: Mapping[str, tuple[str, ...]] = {
    "accounts": ("account_id", "currency", "balance", "credit"),
    "positions": (
        "position_id",
        "account_id",
        "symbol",
        "side",
        "volume_lots",
        "open_price",
        "status",
    ),
    "deals": (
        "deal_id",
        "source_seq",
        "account_id",
        "position_id",
        "symbol",
        "entry",
        "side",
        "volume_lots",
        "price",
        "realized_pnl_gross",
        "commission",
        "swap",
        "executed_at",
    ),
}


def p03_payload_hash(entity: str, record: Mapping[str, Any]) -> str:
    """P03's own content hash, deliberately *not* P04's.

    Different domain string and different field ordering, so the two hashes
    never coincide. Any attempt to compare a stored P03 hash against a P04
    hash therefore fails loudly rather than silently reporting every row as a
    mismatch in production (section 6.4).
    """
    columns = _COLUMNS[entity]
    body = {"v": "p03-storage-v1", "e": entity, "f": [str(record.get(c, "")) for c in columns]}
    return hashlib.sha256(canonical_json_bytes(body)).hexdigest()


class Fault:
    """One injectable fault with a confirmable activation count.

    Section 10.6 requires a test to prove its fault actually fired. A test
    that asserts an outcome without checking ``activations`` may be passing
    for the wrong reason, so every fault counts itself.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.armed_times = 0
        self.activations = 0

    def arm(self, times: int = 1) -> Fault:
        self.armed_times = times
        return self

    def disarm(self) -> None:
        self.armed_times = 0

    def should_fire(self) -> bool:
        if self.armed_times <= 0:
            return False
        self.armed_times -= 1
        self.activations += 1
        return True

    @property
    def fired(self) -> bool:
        return self.activations > 0


@dataclass
class HarnessFaults:
    """The fault surface this harness exposes to tests."""

    #: Commit the repair work, then fail the response. The classic
    #: "COMMIT succeeded, ACK lost" window (section 7.6, F-09).
    lose_repair_ack: Fault = field(default_factory=lambda: Fault("lose_repair_ack"))
    #: Accept the intent, then fail during source capture (IT04-09).
    accept_then_fail: Fault = field(default_factory=lambda: Fault("accept_then_fail"))
    #: Drop the deal checkpoint commit after pages were stored, leaving W
    #: behind the data (section 7.6 "after fetch page before receipt COMMIT").
    skip_checkpoint_commit: Fault = field(default_factory=lambda: Fault("skip_checkpoint_commit"))
    #: Refuse the intent outright with a stated reason.
    reject_intent: Fault = field(default_factory=lambda: Fault("reject_intent"))

    def all(self) -> tuple[Fault, ...]:
        return (
            self.lose_repair_ack,
            self.accept_then_fail,
            self.skip_checkpoint_commit,
            self.reject_intent,
        )

    def activation_report(self) -> dict[str, int]:
        return {fault.name: fault.activations for fault in self.all()}


class P03SyncHarness:
    """Implements ``P03ReadPort`` and ``P03RepairPort`` over the P03 tables."""

    def __init__(
        self,
        *,
        db: Database,
        source: SourcePort,
        clock: Clock,
        source_id: str = "sim_local",
        page_size: int = 500,
    ) -> None:
        self._db = db
        self._source = source
        self._clock = clock
        self._source_id = source_id
        self._page_size = page_size
        self.faults = HarnessFaults()

    # ---- connection ------------------------------------------------------

    def _conn(self):
        return self._db.connect(Role.P03_SYNC)

    # ---- bootstrap -------------------------------------------------------

    def bootstrap_epoch(self) -> str:
        """Register the source and its current ``(run_id, generation)`` epoch.

        A changed generation creates a *new* epoch and supersedes the old one;
        it never mutates the existing row. That is what keeps the same deal id
        in two generations from being a collision (section 7.7, INV04-17).
        """
        identity = self._source.read_identity()
        conn = self._conn()
        try:
            with transaction(conn):
                conn.execute(
                    "INSERT OR IGNORE INTO sync_sources (source_id, display_name) VALUES (?,?)",
                    (self._source_id, f"source {self._source_id}"),
                )
                row = conn.execute(
                    """
                    SELECT epoch_id FROM source_epochs
                     WHERE source_id = ? AND source_run_id = ? AND source_generation = ?
                    """,
                    (self._source_id, identity.run_id, identity.generation),
                ).fetchone()
                if row is not None:
                    return row["epoch_id"]

                conn.execute(
                    """
                    UPDATE source_epochs SET status = 'SUPERSEDED'
                     WHERE source_id = ? AND status = 'ACTIVE'
                    """,
                    (self._source_id,),
                )
                epoch_id = str(uuid.uuid4())
                conn.execute(
                    """
                    INSERT INTO source_epochs
                      (epoch_id, source_id, source_run_id, source_generation, status, created_at)
                    VALUES (?,?,?,?, 'ACTIVE', ?)
                    """,
                    (
                        epoch_id,
                        self._source_id,
                        identity.run_id,
                        identity.generation,
                        to_iso(self._clock.now()),
                    ),
                )
                for entity in ("accounts", "positions", "deals"):
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO sync_checkpoints
                          (epoch_id, entity, watermark_seq, filter_hash, published, lease_version)
                        VALUES (?,?,0,'UNFILTERED',0,0)
                        """,
                        (epoch_id, entity),
                    )
                return epoch_id
        finally:
            conn.close()

    def quarantine_epoch(self, epoch_id: str) -> None:
        conn = self._conn()
        try:
            with transaction(conn):
                conn.execute(
                    "UPDATE source_epochs SET status = 'QUARANTINED' WHERE epoch_id = ?", (epoch_id,)
                )
        finally:
            conn.close()

    # ---- lease -----------------------------------------------------------

    def acquire_lease(self, epoch_id: str, entity: str) -> tuple[str, int]:
        """Take the sync lease, bumping ``lease_version``.

        A second worker calling this fences the first: the old version can no
        longer commit, which is how the two-worker takeover test (IT04-04)
        gets a real stale worker rather than a simulated one.
        """
        conn = self._conn()
        try:
            with transaction(conn):
                row = conn.execute(
                    "SELECT lease_version FROM sync_checkpoints WHERE epoch_id = ? AND entity = ?",
                    (epoch_id, entity),
                ).fetchone()
                if row is None:
                    raise ReconError("no checkpoint for epoch/entity", epoch_id=epoch_id, entity=entity)
                token = str(uuid.uuid4())
                version = int(row["lease_version"]) + 1
                conn.execute(
                    """
                    UPDATE sync_checkpoints
                       SET lease_token = ?, lease_version = ?, lease_until = ?
                     WHERE epoch_id = ? AND entity = ?
                    """,
                    (token, version, to_iso(self._clock.now()), epoch_id, entity),
                )
                return token, version
        finally:
            conn.close()

    def lease_version(self, epoch_id: str, entity: str) -> int:
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT lease_version FROM sync_checkpoints WHERE epoch_id = ? AND entity = ?",
                (epoch_id, entity),
            ).fetchone()
            return 0 if row is None else int(row["lease_version"])
        finally:
            conn.close()

    # ---- deal sync -------------------------------------------------------

    def sync_deals(
        self,
        epoch_id: str,
        *,
        max_pages: int | None = None,
        advance_watermark: bool = True,
    ) -> int:
        """Replay the Deal log into ``deals`` and maybe advance ``W``.

        ``max_pages`` stops the scan early, which is how a test produces a
        target that holds data without a watermark to vouch for it. In that
        case the watermark is left untouched: section 2.1 allows ``W`` to move
        only after a terminal page, and section 5.2 step 4 requires ``W >= S``
        before any missing-deal claim.
        """
        conn = self._conn()
        try:
            scan_id = str(uuid.uuid4())
            with transaction(conn):
                conn.execute(
                    """
                    INSERT INTO sync_runs (scan_id, epoch_id, entity, filter_hash, status, started_at)
                    VALUES (?,?, 'deals', 'UNFILTERED', 'RUNNING', ?)
                    """,
                    (scan_id, epoch_id, to_iso(self._clock.now())),
                )

            pages = 0
            terminal = False
            snapshot_seq = 0
            highest_seq = 0
            stored = 0

            for page in self._source.iter_pages("deals", page_size=self._page_size):
                snapshot_seq = page.snapshot_seq
                with transaction(conn):
                    for record in page.items:
                        # ON CONFLICT DO NOTHING, never DO UPDATE: a deal that
                        # already exists is immutable history and replay must
                        # not rewrite its money (section 8.1, INV04-05).
                        conn.execute(
                            """
                            INSERT INTO deals (epoch_id, deal_id, source_seq, account_id, position_id,
                                               symbol, entry, side, volume_lots, price,
                                               realized_pnl_gross, commission, swap, executed_at, payload_hash)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            ON CONFLICT (epoch_id, deal_id) DO NOTHING
                            """,
                            (
                                epoch_id,
                                record["deal_id"],
                                int(record["source_seq"]),
                                record["account_id"],
                                record["position_id"],
                                record["symbol"],
                                record["entry"],
                                record["side"],
                                record["volume_lots"],
                                record["price"],
                                record["realized_pnl_gross"],
                                record["commission"],
                                record["swap"],
                                record["executed_at"],
                                p03_payload_hash("deals", record),
                            ),
                        )
                        highest_seq = max(highest_seq, int(record["source_seq"]))
                        stored += 1
                pages += 1
                terminal = page.is_terminal
                if max_pages is not None and pages >= max_pages:
                    break

            watermark = self.deal_watermark(epoch_id)
            if terminal and advance_watermark and not self.faults.skip_checkpoint_commit.should_fire():
                watermark = snapshot_seq
                with transaction(conn):
                    conn.execute(
                        """
                        UPDATE sync_checkpoints
                           SET watermark_seq = ?, published = 1, published_scan_id = ?
                         WHERE epoch_id = ? AND entity = 'deals'
                        """,
                        (watermark, scan_id, epoch_id),
                    )
                    conn.execute(
                        """
                        UPDATE sync_runs
                           SET status = 'PUBLISHED', terminal_receipt = 1, snapshot_seq = ?,
                               record_count = ?, published_at = ?
                         WHERE scan_id = ?
                        """,
                        (snapshot_seq, stored, to_iso(self._clock.now()), scan_id),
                    )
            else:
                with transaction(conn):
                    conn.execute(
                        "UPDATE sync_runs SET status = 'ABANDONED', snapshot_seq = ? WHERE scan_id = ?",
                        (snapshot_seq, scan_id),
                    )
            return watermark
        finally:
            conn.close()

    def deal_watermark(self, epoch_id: str) -> int:
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT watermark_seq FROM sync_checkpoints WHERE epoch_id = ? AND entity = 'deals'",
                (epoch_id,),
            ).fetchone()
            return 0 if row is None else int(row["watermark_seq"])
        finally:
            conn.close()

    # ---- mutable projections --------------------------------------------

    def publish_snapshot(self, epoch_id: str, entity: str) -> str:
        """Full scan to staging, then one atomic promotion.

        The promotion is a single transaction that replaces the whole
        projection and moves ``published_scan_id`` with it. A reader therefore
        cannot observe rows from two scans at once, which is the property
        IT04-07 checks (section 2.1).
        """
        if entity not in ("accounts", "positions"):
            raise ReconError("snapshot publication is only for mutable projections", entity=entity)

        rows: list[Mapping[str, Any]] = []
        snapshot_seq = 0
        for page in self._source.iter_pages(entity, page_size=self._page_size):
            snapshot_seq = page.snapshot_seq
            rows.extend(page.items)

        conn = self._conn()
        try:
            scan_id = str(uuid.uuid4())
            staging = "account_staging" if entity == "accounts" else "position_staging"
            current = "accounts_current" if entity == "accounts" else "positions_current"
            key = "account_id" if entity == "accounts" else "position_id"
            columns = _COLUMNS[entity]

            with transaction(conn):
                conn.execute(
                    """
                    INSERT INTO sync_runs (scan_id, epoch_id, entity, snapshot_seq, filter_hash,
                                           status, record_count, started_at)
                    VALUES (?,?,?,?, 'UNFILTERED', 'RUNNING', ?, ?)
                    """,
                    (scan_id, epoch_id, entity, snapshot_seq, len(rows), to_iso(self._clock.now())),
                )
                placeholders = ",".join("?" * (len(columns) + 2))
                for record in rows:
                    conn.execute(
                        f"INSERT INTO {staging} (scan_id, {','.join(columns)}, payload_hash) VALUES ({placeholders})",
                        (
                            scan_id,
                            *[record[c] for c in columns],
                            p03_payload_hash(entity, record),
                        ),
                    )

            # The atomic publication.
            with transaction(conn):
                conn.execute(f"DELETE FROM {current} WHERE epoch_id = ?", (epoch_id,))
                conn.execute(
                    f"""
                    INSERT INTO {current} (epoch_id, {",".join(columns)}, published_scan_id, payload_hash)
                    SELECT ?, {",".join(columns)}, ?, payload_hash FROM {staging} WHERE scan_id = ?
                    """,
                    (epoch_id, scan_id, scan_id),
                )
                conn.execute(
                    """
                    UPDATE sync_runs SET status = 'PUBLISHED', terminal_receipt = 1, published_at = ?
                     WHERE scan_id = ?
                    """,
                    (to_iso(self._clock.now()), scan_id),
                )
                conn.execute(
                    """
                    UPDATE sync_checkpoints
                       SET published = 1, published_scan_id = ?, watermark_seq = ?
                     WHERE epoch_id = ? AND entity = ?
                    """,
                    (scan_id, snapshot_seq, epoch_id, entity),
                )
            return scan_id
        finally:
            conn.close()

    # ---- direct mutations, for fixture construction only -----------------

    def corrupt_for_test(self, sql: str, params: Sequence[Any] = ()) -> None:
        """Apply an arbitrary write to P03's tables to build a fixture.

        This is how a test creates a target that is *wrong* -- a deleted deal,
        an altered commission -- so the audit has something real to find. It
        runs under the P03 role, so it is not a back door around the
        privilege separation that denies P04 these writes.
        """
        conn = self._conn()
        try:
            with transaction(conn):
                conn.execute(sql, tuple(params))
        finally:
            conn.close()

    # ---- P03ReadPort -----------------------------------------------------

    def sync_status(self, source_id: str, epoch_id: str, entity: str) -> SyncStatus:
        conn = self._conn()
        try:
            epoch = conn.execute(
                "SELECT status FROM source_epochs WHERE epoch_id = ?", (epoch_id,)
            ).fetchone()
            checkpoint = conn.execute(
                "SELECT * FROM sync_checkpoints WHERE epoch_id = ? AND entity = ?",
                (epoch_id, entity),
            ).fetchone()
            if epoch is None or checkpoint is None:
                raise ReconError(
                    "no P03 state for this epoch/entity",
                    reason=ReasonCode.TARGET_NO_PUBLISHED_SCAN,
                    epoch_id=epoch_id,
                    entity=entity,
                )
            scan = None
            if checkpoint["published_scan_id"]:
                scan = conn.execute(
                    "SELECT * FROM sync_runs WHERE scan_id = ?", (checkpoint["published_scan_id"],)
                ).fetchone()
            return SyncStatus(
                source_id=source_id,
                epoch_id=epoch_id,
                entity=entity,
                epoch_status=epoch["status"],
                published_scan_id=checkpoint["published_scan_id"],
                snapshot_seq=None if scan is None else scan["snapshot_seq"],
                watermark_seq=int(checkpoint["watermark_seq"]),
                filter_hash=checkpoint["filter_hash"],
                published=bool(checkpoint["published"]),
                lease_version=int(checkpoint["lease_version"]),
                terminal_receipt=bool(scan["terminal_receipt"]) if scan is not None else False,
                record_count=None if scan is None else scan["record_count"],
            )
        finally:
            conn.close()

    def source_capability_profile(self, source_id: str) -> Mapping[str, Any]:
        caps = self._source.read_capabilities()
        return {
            "source_id": source_id,
            "driver": "p01_simulator",
            "stable_snapshot_pagination": caps.stable_snapshot_pagination,
            "durable_epoch_identity": caps.durable_epoch_identity,
            "complete_deal_history": caps.complete_deal_history,
            "full_unfiltered_listing": caps.full_unfiltered_listing,
            "global_cut_across_entities": caps.global_cut_across_entities,
            "identical_financial_semantics": caps.identical_financial_semantics,
            "max_page_size": caps.max_page_size,
            "snapshot_ttl_seconds": caps.snapshot_ttl_seconds,
        }

    # ---- P03RepairPort ---------------------------------------------------

    def _existing_receipt(self, conn, intent_id: str) -> RepairReceipt | None:
        row = conn.execute(
            "SELECT * FROM repair_intents WHERE intent_id = ?", (intent_id,)
        ).fetchone()
        if row is None:
            return None
        return RepairReceipt(
            intent_id=row["intent_id"],
            status=row["status"],
            reason_code=row["reason_code"],
            receipt_hash=row["receipt_hash"],
            result_scan_id=row["result_scan_id"],
            result_watermark=row["result_watermark"],
            execution_count=int(row["execution_count"]),
        )

    def repair_intent_status(self, intent_id: str) -> RepairReceipt | None:
        conn = self._conn()
        try:
            return self._existing_receipt(conn, intent_id)
        finally:
            conn.close()

    def _receipt_hash(self, intent_id: str, status: str, detail: Mapping[str, Any]) -> str:
        return hashlib.sha256(
            canonical_json_bytes({"intent_id": intent_id, "status": status, "detail": detail})
        ).hexdigest()

    def _dispatch(
        self,
        *,
        intent_id: str,
        action_type: str,
        entity: str | None,
        expected_epoch_id: str,
        expected_revision: str | None,
        expected_lease_version: int | None,
        expected_watermark: int | None,
        approved_plan_hash: str,
        parameters: Mapping[str, Any],
    ) -> RepairReceipt:
        """Shared intent handling: dedup, fence, execute, receipt."""
        conn = self._conn()
        try:
            existing = self._existing_receipt(conn, intent_id)
            if existing is not None:
                # Idempotent replay: the work does not run again and
                # execution_count does not move (INV04-08).
                return existing

            parameters_hash = hashlib.sha256(canonical_json_bytes(parameters)).hexdigest()
            now = to_iso(self._clock.now())

            # Preconditions are evaluated here, inside P03, against P03's own
            # authoritative rows -- not against whatever the caller believed.
            epoch = conn.execute(
                "SELECT status FROM source_epochs WHERE epoch_id = ?", (expected_epoch_id,)
            ).fetchone()
            checkpoint = conn.execute(
                "SELECT * FROM sync_checkpoints WHERE epoch_id = ? AND entity = ?",
                (expected_epoch_id, entity or "deals"),
            ).fetchone()

            rejection: str | None = None
            if self.faults.reject_intent.should_fire():
                rejection = "INJECTED_REJECTION"
            elif epoch is None or epoch["status"] != "ACTIVE":
                rejection = "EPOCH_NOT_ACTIVE"
            elif checkpoint is None:
                rejection = "NO_CHECKPOINT"
            elif (
                expected_lease_version is not None
                and int(checkpoint["lease_version"]) != expected_lease_version
            ):
                rejection = "LEASE_VERSION_STALE"
            elif (
                expected_revision is not None
                and checkpoint["published_scan_id"] is not None
                and checkpoint["published_scan_id"] != expected_revision
            ):
                rejection = "REVISION_STALE"
            elif (
                expected_watermark is not None
                and int(checkpoint["watermark_seq"]) != expected_watermark
            ):
                rejection = "WATERMARK_STALE"

            if rejection is not None:
                with transaction(conn):
                    conn.execute(
                        """
                        INSERT INTO repair_intents (intent_id, source_id, epoch_id, action_type,
                          parameters_hash, status, reason_code, receipt_hash, execution_count,
                          accepted_at, completed_at)
                        VALUES (?,?,?,?,?, 'REJECTED', ?, ?, 0, ?, ?)
                        """,
                        (
                            intent_id,
                            self._source_id,
                            expected_epoch_id,
                            action_type,
                            parameters_hash,
                            rejection,
                            self._receipt_hash(intent_id, "REJECTED", {"reason": rejection}),
                            now,
                            now,
                        ),
                    )
                return RepairReceipt(
                    intent_id=intent_id,
                    status="REJECTED",
                    reason_code=rejection,
                    receipt_hash=self._receipt_hash(intent_id, "REJECTED", {"reason": rejection}),
                )

            with transaction(conn):
                conn.execute(
                    """
                    INSERT INTO repair_intents (intent_id, source_id, epoch_id, action_type,
                      parameters_hash, status, execution_count, accepted_at)
                    VALUES (?,?,?,?,?, 'ACCEPTED', 0, ?)
                    """,
                    (intent_id, self._source_id, expected_epoch_id, action_type, parameters_hash, now),
                )
        finally:
            conn.close()

        # ACCEPTED is durable before the work starts, so a crash mid-work
        # leaves a receipt the caller can find (section 7.6).
        if self.faults.accept_then_fail.should_fire():
            self._complete_intent(
                intent_id, status="FAILED", reason_code="INJECTED_SOURCE_CAPTURE_FAILURE"
            )
            return RepairReceipt(
                intent_id=intent_id, status="FAILED", reason_code="INJECTED_SOURCE_CAPTURE_FAILURE"
            )

        result_scan_id: str | None = None
        result_watermark: int | None = None
        try:
            if action_type in ("REQUEST_FULL_ACCOUNT_RESYNC", "REQUEST_FULL_POSITION_RESYNC"):
                assert entity is not None
                result_scan_id = self.publish_snapshot(expected_epoch_id, entity)
            elif action_type == "REQUEST_DEAL_BACKFILL":
                result_watermark = self.sync_deals(expected_epoch_id)
            else:
                raise ReconError("unsupported action type for dispatch", action_type=action_type)
        except (SourceError, ReconError) as exc:
            self._complete_intent(intent_id, status="FAILED", reason_code=str(getattr(exc, "reason", "FAILED")))
            return RepairReceipt(
                intent_id=intent_id, status="FAILED", reason_code=str(getattr(exc, "reason", "FAILED"))
            )

        receipt = self._complete_intent(
            intent_id,
            status="COMMITTED",
            result_scan_id=result_scan_id,
            result_watermark=result_watermark,
        )

        if self.faults.lose_repair_ack.should_fire():
            # The work is committed and the receipt is durable; only the
            # response is lost. The caller must resolve this by querying the
            # intent, not by retrying it (section 7.6, INV04-09).
            raise SourceError(
                "connection dropped after P03 committed the repair",
                reason=ReasonCode.REPAIR_UNKNOWN,
                intent_id=intent_id,
            )
        return receipt

    def _complete_intent(
        self,
        intent_id: str,
        *,
        status: str,
        reason_code: str | None = None,
        result_scan_id: str | None = None,
        result_watermark: int | None = None,
    ) -> RepairReceipt:
        detail = {
            "result_scan_id": result_scan_id,
            "result_watermark": result_watermark,
            "reason_code": reason_code,
        }
        receipt_hash = self._receipt_hash(intent_id, status, detail)
        conn = self._conn()
        try:
            with transaction(conn):
                conn.execute(
                    """
                    UPDATE repair_intents
                       SET status = ?, reason_code = ?, receipt_hash = ?, result_scan_id = ?,
                           result_watermark = ?, execution_count = execution_count + 1,
                           completed_at = ?
                     WHERE intent_id = ?
                    """,
                    (
                        status,
                        reason_code,
                        receipt_hash,
                        result_scan_id,
                        result_watermark,
                        to_iso(self._clock.now()),
                        intent_id,
                    ),
                )
                row = conn.execute(
                    "SELECT execution_count FROM repair_intents WHERE intent_id = ?", (intent_id,)
                ).fetchone()
            return RepairReceipt(
                intent_id=intent_id,
                status=status,
                reason_code=reason_code,
                receipt_hash=receipt_hash,
                result_scan_id=result_scan_id,
                result_watermark=result_watermark,
                execution_count=int(row["execution_count"]) if row else 1,
            )
        finally:
            conn.close()

    def request_full_resync(
        self,
        *,
        intent_id: str,
        source_id: str,
        entity: str,
        expected_epoch_id: str,
        expected_revision: str | None,
        expected_lease_version: int | None,
        approved_plan_hash: str,
        parameters: Mapping[str, Any],
    ) -> RepairReceipt:
        action = (
            "REQUEST_FULL_ACCOUNT_RESYNC" if entity == "accounts" else "REQUEST_FULL_POSITION_RESYNC"
        )
        return self._dispatch(
            intent_id=intent_id,
            action_type=action,
            entity=entity,
            expected_epoch_id=expected_epoch_id,
            expected_revision=expected_revision,
            expected_lease_version=expected_lease_version,
            expected_watermark=None,
            approved_plan_hash=approved_plan_hash,
            parameters=parameters,
        )

    def request_deal_backfill(
        self,
        *,
        intent_id: str,
        source_id: str,
        expected_epoch_id: str,
        expected_watermark: int | None,
        approved_plan_hash: str,
        parameters: Mapping[str, Any],
    ) -> RepairReceipt:
        return self._dispatch(
            intent_id=intent_id,
            action_type="REQUEST_DEAL_BACKFILL",
            entity="deals",
            expected_epoch_id=expected_epoch_id,
            expected_revision=None,
            expected_lease_version=None,
            expected_watermark=expected_watermark,
            approved_plan_hash=approved_plan_hash,
            parameters=parameters,
        )

    # ---- oracle support --------------------------------------------------

    def iter_target_rows(self, epoch_id: str, entity: str, *, max_seq: int | None = None) -> Iterator[dict[str, Any]]:
        """Read published rows, for independent oracle comparisons in tests."""
        conn = self._conn()
        try:
            columns = _COLUMNS[entity]
            if entity == "deals":
                sql = f"SELECT {','.join(columns)} FROM deals WHERE epoch_id = ?"
                params: list[Any] = [epoch_id]
                if max_seq is not None:
                    sql += " AND source_seq <= ?"
                    params.append(max_seq)
                sql += " ORDER BY source_seq"
                rows = conn.execute(sql, tuple(params)).fetchall()
            else:
                table = "accounts_current" if entity == "accounts" else "positions_current"
                rows = conn.execute(
                    f"SELECT {','.join(columns)} FROM {table} WHERE epoch_id = ? ORDER BY 1", (epoch_id,)
                ).fetchall()
            for row in rows:
                yield {column: row[column] for column in columns}
        finally:
            conn.close()
