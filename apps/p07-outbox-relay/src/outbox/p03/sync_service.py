"""A stand-in for Project 03's sync worker -- the call site, not the service.

Project 03 is a separate mini-project and is not implemented in this
repository. But the outbox writer's central guarantee is about *its caller's*
transaction, so there has to be a caller: something that inserts Deals and
publishes scans in real transactions, with the fault hooks from design section
15 in the right places, or the atomicity invariants cannot be executed at all.

This is that caller, and the boundary is deliberate. It owns the P03 tables and
the transaction; it calls :class:`~outbox.writer.OutboxWriter` from inside that
transaction and never otherwise. What it is *not* is a model of P03's real
behaviour -- no paging, no connector, no checkpoint protocol, no lease renewal.
Design section 6.3 carries the matching warning in the other direction: if the
real P03 commits the Deal and writes the outbox row in a *second* transaction,
the invariant fails and integration must be blocked until P03 changes. Nothing
here can detect that on P03's behalf.

The duplicate-Deal path is worth reading. ``INSERT ... ON CONFLICT DO NOTHING``
means a re-fetched page inserts nothing, and the hash of the existing row is
then compared against the hash of the fact we were handed. Equal means a
genuine duplicate fetch and *no event is emitted* (ADR07-02). Unequal means the
source has two different stories about one deal id, which is quarantined rather
than resolved.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ..clock import Clock, RealClock, to_wire
from ..errors import IntegrityIncident, ValidationError
from ..faults.hooks import HOOKS, HookRegistry, KillPoint
from ..writer.outbox_writer import DealFact, OutboxWriter, WriteResult


@dataclass(frozen=True, slots=True)
class DealInsertOutcome:
    deal_id: str
    inserted: bool
    duplicate_of_identical: bool
    write: WriteResult | None


class SyncService:
    """Inserts Deals and publishes scans, with the outbox in the same commit."""

    def __init__(
        self,
        store: Any,
        writer: OutboxWriter,
        *,
        source_id: str,
        clock: Clock | None = None,
        hooks: HookRegistry | None = None,
    ) -> None:
        self._store = store
        self._writer = writer
        self._source_id = source_id
        self._clock = clock or RealClock()
        self._hooks = hooks or HOOKS

    @property
    def source_id(self) -> str:
        return self._source_id

    # --- epoch lifecycle ----------------------------------------------------
    def open_epoch(self, epoch_id: str, *, status: str = "ACTIVE") -> None:
        with self._store.transaction() as tx:
            tx.execute(
                "INSERT INTO source_epochs (epoch_id, source_id, status, opened_at) VALUES (?, ?, ?, ?)",
                (epoch_id, self._source_id, status, to_wire(self._clock.now())),
            )

    def set_epoch_status(self, epoch_id: str, status: str) -> None:
        if status not in ("ACTIVE", "BLOCKED", "RETIRED"):
            raise ValidationError(f"unknown epoch status {status!r}")
        with self._store.transaction() as tx:
            tx.execute(
                "UPDATE source_epochs SET status = ? WHERE epoch_id = ? AND source_id = ?",
                (status, epoch_id, self._source_id),
            )

    def reset_epoch(self, *, old_epoch_id: str, new_epoch_id: str, reason: str = "RESET") -> WriteResult:
        """Retire one epoch and adopt another, announcing it once.

        Both halves and the announcement are one transaction. A retired old
        epoch with no new epoch, or a new epoch nobody was told about, would
        each be a state the rest of the system has no way to interpret.
        """
        with self._store.transaction() as tx:
            tx.execute(
                "UPDATE source_epochs SET status = 'RETIRED' WHERE epoch_id = ? AND source_id = ?",
                (old_epoch_id, self._source_id),
            )
            tx.execute(
                "INSERT INTO source_epochs (epoch_id, source_id, status, opened_at) VALUES (?, ?, ?, ?)",
                (new_epoch_id, self._source_id, "ACTIVE", to_wire(self._clock.now())),
            )
            self._hooks.fire(
                KillPoint.EPOCH_TRANSITION_MID_FLIGHT, old=old_epoch_id, new=new_epoch_id
            )
            return self._writer.record_epoch_changed(
                tx,
                source_id=self._source_id,
                epoch_id=new_epoch_id,
                previous_epoch_id=old_epoch_id,
                reason=reason,
            )

    # --- deals --------------------------------------------------------------
    def insert_deals(self, epoch_id: str, deals: Sequence[DealFact]) -> list[DealInsertOutcome]:
        """Insert a page of Deals and their events in one transaction.

        Deals are applied in ``(source_seq, deal_id)`` order so that two runs
        over the same page produce the same aggregate versions. Without a
        deterministic order the versions would depend on dict iteration order,
        and a replay would produce a different -- equally valid, but different
        -- version assignment, which makes any comparison between runs useless.
        """
        ordered = sorted(deals, key=lambda deal: (deal.source_seq, deal.deal_id))
        self._hooks.fire(KillPoint.P03_BEFORE_BEGIN, epoch_id=epoch_id, count=len(ordered))
        outcomes: list[DealInsertOutcome] = []
        with self._store.transaction() as tx:
            self._hooks.fire(KillPoint.P03_AFTER_BEGIN, epoch_id=epoch_id)
            for deal in ordered:
                outcomes.append(self._insert_one_deal(tx, epoch_id, deal))
            self._hooks.fire(KillPoint.P03_BEFORE_COMMIT, epoch_id=epoch_id, count=len(ordered))
        # The transaction is durable from here. A crash at this point is the
        # ambiguous-ACK case: the client never learns the commit succeeded.
        self._hooks.fire(KillPoint.P03_AFTER_COMMIT_BEFORE_ACK, epoch_id=epoch_id)
        return outcomes

    def _insert_one_deal(self, tx: Any, epoch_id: str, deal: DealFact) -> DealInsertOutcome:
        business_hash = deal.business_hash
        cursor = tx.execute(
            """
            INSERT INTO deals (
                epoch_id, deal_id, account_id, position_id, symbol, side, entry_kind,
                volume_lots, price, realized_pnl_gross, commission, swap, money_currency,
                source_seq, executed_at, payload_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (epoch_id, deal_id) DO NOTHING
            """,
            (
                epoch_id,
                deal.deal_id,
                deal.account_id,
                deal.position_id,
                deal.symbol,
                deal.side,
                deal.entry_kind,
                deal.volume_lots,
                deal.price,
                deal.realized_pnl_gross,
                deal.commission,
                deal.swap,
                deal.currency.code,
                deal.source_seq,
                deal.executed_at,
                business_hash,
            ),
        )
        inserted = cursor.rowcount == 1
        self._hooks.fire(KillPoint.P03_AFTER_DEAL_INSERT, deal_id=deal.deal_id, inserted=inserted)

        if not inserted:
            row = tx.execute(
                "SELECT payload_hash FROM deals WHERE epoch_id = ? AND deal_id = ?",
                (epoch_id, deal.deal_id),
            ).fetchone()
            stored_hash = row["payload_hash"] if row is not None else None
            if stored_hash != business_hash:
                raise IntegrityIncident(
                    f"deal {deal.deal_id!r} in epoch {epoch_id} is already stored with hash "
                    f"{stored_hash} but the source now reports {business_hash}; a Deal is "
                    "immutable, so this is a source conflict for P03 to quarantine, not a "
                    "correction to publish"
                )
            # Identical re-fetch: no new event (ADR07-02).
            return DealInsertOutcome(
                deal_id=deal.deal_id, inserted=False, duplicate_of_identical=True, write=None
            )

        write = self._writer.record_deal(tx, source_id=self._source_id, epoch_id=epoch_id, deal=deal)
        return DealInsertOutcome(
            deal_id=deal.deal_id, inserted=True, duplicate_of_identical=False, write=write
        )

    # --- scans --------------------------------------------------------------
    def stage_scan(
        self,
        *,
        epoch_id: str,
        entity: str,
        scan_id: str,
        snapshot_seq: int,
        record_ids: Sequence[str],
        lease_token: str,
        complete: bool = True,
        manifest_sha256: str | None = None,
    ) -> None:
        """Stage a scan's rows.

        ``complete=False`` stages the rows but records a ``record_count`` that
        the staged set does not satisfy, which is how a "first page only" scan is
        represented: :meth:`publish_scan` then refuses it.
        """
        if entity not in ("accounts", "positions"):
            raise ValidationError(f"entity {entity!r} must be 'accounts' or 'positions'")
        table = "accounts_staging" if entity == "accounts" else "positions_staging"
        column = "account_id" if entity == "accounts" else "position_id"
        declared_count = len(record_ids) if complete else len(record_ids) + 1
        manifest = manifest_sha256 or _manifest_hash(scan_id, record_ids)
        with self._store.transaction() as tx:
            tx.execute(
                "INSERT INTO sync_runs (scan_id, epoch_id, entity, status, snapshot_seq, "
                "record_count, manifest_sha256, lease_token, published_at) "
                "VALUES (?, ?, ?, 'STAGED', ?, ?, ?, ?, NULL)",
                (scan_id, epoch_id, entity, snapshot_seq, declared_count, manifest, lease_token),
            )
            for record_id in record_ids:
                tx.execute(
                    f"INSERT INTO {table} (scan_id, epoch_id, {column}) VALUES (?, ?, ?)",
                    (scan_id, epoch_id, record_id),
                )
            self._hooks.fire(KillPoint.P03_SNAPSHOT_AFTER_STAGING, scan_id=scan_id, entity=entity)

    def publish_scan(self, *, epoch_id: str, entity: str, scan_id: str, lease_token: str) -> WriteResult:
        """Atomically make a complete staged scan current, and emit its marker.

        The completeness check is inside the transaction and compares the staged
        row count against the count the scan declared. A scan that lost its last
        page fails here, leaving the previous ``*_current`` contents
        authoritative and producing no marker.
        """
        table = "accounts_staging" if entity == "accounts" else "positions_staging"
        current = "accounts_current" if entity == "accounts" else "positions_current"
        column = "account_id" if entity == "accounts" else "position_id"
        now = to_wire(self._clock.now())

        with self._store.transaction() as tx:
            row = tx.execute(
                "SELECT status, record_count, lease_token FROM sync_runs "
                "WHERE scan_id = ? AND epoch_id = ? AND entity = ?",
                (scan_id, epoch_id, entity),
            ).fetchone()
            if row is None:
                raise ValidationError(f"scan {scan_id!r} is not staged for {entity} in {epoch_id}")
            if row["status"] != "STAGED":
                raise IntegrityIncident(f"scan {scan_id!r} is {row['status']!r}, not STAGED")
            if row["lease_token"] != lease_token:
                raise IntegrityIncident(
                    f"scan {scan_id!r} is held under a different lease; this worker has been fenced"
                )
            staged = tx.execute(
                f"SELECT COUNT(*) AS n FROM {table} WHERE scan_id = ?", (scan_id,)
            ).fetchone()["n"]
            if int(staged) != int(row["record_count"]):
                raise IntegrityIncident(
                    f"scan {scan_id!r} staged {staged} of {row['record_count']} declared rows; "
                    "an incomplete scan is never published and never produces a marker"
                )

            tx.execute(
                "UPDATE sync_runs SET status = 'PUBLISHED', published_at = ? "
                "WHERE scan_id = ? AND status = 'STAGED' AND lease_token = ?",
                (now, scan_id, lease_token),
            )
            tx.execute(f"DELETE FROM {current} WHERE epoch_id = ?", (epoch_id,))
            tx.execute(
                f"INSERT INTO {current} (epoch_id, {column}, scan_id) "
                f"SELECT epoch_id, {column}, scan_id FROM {table} WHERE scan_id = ?",
                (scan_id,),
            )
            self._hooks.fire(KillPoint.P03_SNAPSHOT_AFTER_PUBLISH_UPDATE, scan_id=scan_id)

            write = self._writer.record_snapshot_published(
                tx,
                source_id=self._source_id,
                epoch_id=epoch_id,
                entity=entity,
                scan_id=scan_id,
                lease_token=lease_token,
            )
            self._hooks.fire(KillPoint.P03_SNAPSHOT_AFTER_MARKER_INSERT, scan_id=scan_id)
            return write

    def abort_scan(self, *, epoch_id: str, scan_id: str) -> None:
        with self._store.transaction() as tx:
            tx.execute(
                "UPDATE sync_runs SET status = 'ABORTED' WHERE scan_id = ? AND epoch_id = ?",
                (scan_id, epoch_id),
            )

    # --- P04 hook -----------------------------------------------------------
    def record_verified_reconciliation(
        self,
        *,
        epoch_id: str,
        revision: int,
        scope: str,
        evidence_id: str,
        status: str = "VERIFIED",
        verified_at: datetime | None = None,
    ) -> WriteResult:
        with self._store.transaction() as tx:
            return self._writer.record_reconciliation_verified(
                tx,
                source_id=self._source_id,
                epoch_id=epoch_id,
                reconciliation_revision=revision,
                scope=scope,
                evidence_id=evidence_id,
                status=status,
                verified_at=verified_at,
            )


def _manifest_hash(scan_id: str, record_ids: Sequence[str]) -> str:
    """A scan manifest digest over the sorted record ids.

    Sorted so that the same set of rows arriving in a different page order
    yields the same manifest; the manifest attests to content, not to delivery
    order.
    """
    from ..canonical import payload_hash

    return payload_hash({"scan_id": scan_id, "record_ids": sorted(record_ids)})


def new_epoch_id() -> str:
    return str(uuid.uuid4())
