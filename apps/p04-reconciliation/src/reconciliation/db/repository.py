"""Durable state for reconciliation runs, findings, plans and attempts.

Design rules enforced here (SYSTEM_DESIGN sections 6.2, 6.3, 7.4, 7.6):

* **A page receipt and its items commit together.** ``commit_page`` is one
  transaction holding both, so a crash can never leave manifest rows that no
  receipt vouches for, nor a receipt claiming items that were not stored
  (section 7.4 step 4).

* **``source_complete`` is a validated transition, not a flag.** Section 6.2
  caveat 1 is explicit that no single-table CHECK can prove the receipt chain
  is contiguous, terminal and internally consistent, so
  ``validate_receipt_chain`` checks all of it inside the transaction that
  would set the flag, and ``mark_source_complete`` refuses otherwise. This is
  what makes INV04-01 ("no MATCHED if the source scan is incomplete") true by
  construction rather than by convention.

* **A repeated trigger reuses its run.** ``create_run`` writes
  ``recon_triggers`` in the same transaction as the run, keyed by the caller's
  ``request_id``, so a client that retries after a lost response gets the
  original run back instead of starting a second audit of the same scope
  (section 7.6 "before run insert COMMIT").

* **Writes to Project 03's tables are impossible here.** Every method uses a
  ``Role.RECON`` connection; the authoriser in ``engine.py`` denies such a
  write at the engine level (INV04-07, INV04-18).
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Final

from ..audit import AuditEvent, AuditLog
from ..canonical import HASH_VERSION, SCHEMA_VERSION, content_hash, root_hash
from ..clock import Clock, from_iso, to_iso
from ..config import Settings
from ..dto import CapturedPage, DiffRow
from ..errors import (
    ReasonCode,
    ReconError,
    SourceError,
)
from .engine import Database, Role, transaction

#: Run states that accept no further transitions (section 4.1).
#:
#: ``FINDINGS`` is deliberately **not** here: the state diagram in section 4.1
#: has ``FINDINGS --> PLANNED``, so a run that found differences is resting,
#: not finished, and policy may still build a plan from it.
TERMINAL_RUN_STATES: Final[frozenset[str]] = frozenset(
    {"MATCHED", "PLANNED", "INCONCLUSIVE", "FAILED", "CANCELLED"}
)

#: States where capture or diff work is still in progress. Only these count
#: against the concurrency limit: a run awaiting a planning decision is not
#: consuming source or database capacity (FR04-12).
IN_FLIGHT_RUN_STATES: Final[frozenset[str]] = frozenset(
    {"CREATED", "CAPTURING_SOURCE", "CAPTURING_TARGET", "DIFFING"}
)


@dataclass(frozen=True, slots=True)
class RunRecord:
    """Typed view of one ``recon_runs`` row."""

    recon_run_id: str
    source_id: str
    source_epoch_id: str
    entity: str
    scope: str
    scope_hash: str
    policy_hash: str
    schema_version: str
    hash_version: str
    status: str
    worker_id: str
    source_run_id: str | None
    source_generation: int | None
    source_snapshot_seq: int | None
    target_snapshot_seq: int | None
    target_published_scan_id: str | None
    target_deal_watermark: int | None
    source_complete: bool
    target_complete: bool
    source_count: int | None
    target_count: int | None
    source_root_hash: str | None
    target_root_hash: str | None
    reason_code: str | None
    verifies_plan_id: str | None
    started_at: str
    deadline_at: str
    ended_at: str | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> RunRecord:
        return cls(
            recon_run_id=row["recon_run_id"],
            source_id=row["source_id"],
            source_epoch_id=row["source_epoch_id"],
            entity=row["entity"],
            scope=row["scope"],
            scope_hash=row["scope_hash"],
            policy_hash=row["policy_hash"],
            schema_version=row["schema_version"],
            hash_version=row["hash_version"],
            status=row["status"],
            worker_id=row["worker_id"],
            source_run_id=row["source_run_id"],
            source_generation=row["source_generation"],
            source_snapshot_seq=row["source_snapshot_seq"],
            target_snapshot_seq=row["target_snapshot_seq"],
            target_published_scan_id=row["target_published_scan_id"],
            target_deal_watermark=row["target_deal_watermark"],
            source_complete=bool(row["source_complete"]),
            target_complete=bool(row["target_complete"]),
            source_count=row["source_count"],
            target_count=row["target_count"],
            source_root_hash=row["source_root_hash"],
            target_root_hash=row["target_root_hash"],
            reason_code=row["reason_code"],
            verifies_plan_id=row["verifies_plan_id"],
            started_at=row["started_at"],
            deadline_at=row["deadline_at"],
            ended_at=row["ended_at"],
        )


@dataclass(frozen=True, slots=True)
class ChainStats:
    """What the receipt-chain validator actually observed."""

    receipt_count: int
    max_page_index: int | None
    terminal_count: int
    terminal_index: int | None
    declared_items: int
    stored_items: int
    distinct_cuts: int
    snapshot_seq: int | None
    run_id: str | None
    generation: int | None


class ReconRepository:
    """All ``recon_*`` persistence. One instance per worker."""

    def __init__(self, db: Database, settings: Settings, clock: Clock) -> None:
        self._db = db
        self._settings = settings
        self._clock = clock

    # ---- connection helpers ---------------------------------------------

    def connect(self) -> sqlite3.Connection:
        """Open a RECON-role connection. The caller must close it.

        Prefer ``session()``: a ``with`` block on a raw ``sqlite3.Connection``
        ends its *transaction*, not the connection, so using it directly leaks
        the handle and -- in WAL mode -- keeps a read snapshot pinned.
        """
        return self._db.connect(Role.RECON)

    @contextmanager
    def session(self) -> Iterator[sqlite3.Connection]:
        """A RECON connection that is always closed on exit."""
        conn = self._db.connect(Role.RECON)
        try:
            yield conn
        finally:
            conn.close()

    def _audit(self, conn: sqlite3.Connection) -> AuditLog:
        return AuditLog(conn, self._clock)

    # ---- runs ------------------------------------------------------------

    def create_run(
        self,
        *,
        request_id: str,
        source_id: str,
        epoch_id: str,
        entity: str,
        scope: str = "UNFILTERED",
        verifies_plan_id: str | None = None,
    ) -> RunRecord:
        """Create (or recover) one audit run.

        Idempotent on ``request_id``. The fingerprint binds the key to the
        scope it was issued for, so reusing a key for a *different* audit is
        an error rather than a silent alias (section 6.2 ``recon_triggers``).
        """
        scope_hash = self._settings.scope_hash(entity, scope)
        fingerprint = content_hash(
            {
                "source_id": source_id,
                "epoch_id": epoch_id,
                "entity": entity,
                "scope": scope,
                "policy_hash": self._settings.policy_hash,
            }
        )
        now = self._clock.now()
        with self.session() as conn:
            existing = conn.execute(
                "SELECT request_fingerprint, recon_run_id FROM recon_triggers WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if existing is not None:
                if existing["request_fingerprint"] != fingerprint:
                    raise ReconError(
                        "idempotency key was already used for a different audit scope",
                        reason=ReasonCode.ADMISSION_REJECTED,
                        request_id=request_id,
                    )
                run = self.get_run(existing["recon_run_id"])
                assert run is not None  # FK guarantees the row exists
                return run

            run_id = str(uuid.uuid4())
            with transaction(conn):
                conn.execute(
                    """
                    INSERT INTO recon_runs (
                      recon_run_id, source_id, source_epoch_id, entity, scope, scope_hash,
                      policy_hash, schema_version, hash_version, status, worker_id,
                      verifies_plan_id, started_at, deadline_at)
                    VALUES (?,?,?,?,?,?,?,?,?,'CREATED',?,?,?,?)
                    """,
                    (
                        run_id,
                        source_id,
                        epoch_id,
                        entity,
                        scope,
                        scope_hash,
                        self._settings.policy_hash,
                        SCHEMA_VERSION,
                        HASH_VERSION,
                        self._settings.worker_id,
                        verifies_plan_id,
                        to_iso(now),
                        to_iso(now + timedelta(seconds=self._settings.run_deadline_s)),
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO recon_triggers (request_id, request_fingerprint, recon_run_id, created_at)
                    VALUES (?,?,?,?)
                    """,
                    (request_id, fingerprint, run_id, to_iso(now)),
                )
                self._audit(conn).append(
                    AuditEvent.RUN_CREATED,
                    recon_run_id=run_id,
                    actor_id=self._settings.worker_id,
                    entity=entity,
                    scope=scope,
                    epoch_id=epoch_id,
                    policy_hash=self._settings.policy_hash,
                )
            run = self.get_run(run_id)
            assert run is not None
            return run

    def get_run(self, run_id: str) -> RunRecord | None:
        with self.session() as conn:
            row = conn.execute(
                "SELECT * FROM recon_runs WHERE recon_run_id = ?", (run_id,)
            ).fetchone()
            return None if row is None else RunRecord.from_row(row)

    def set_status(self, run_id: str, status: str, *, reason_code: ReasonCode | None = None) -> None:
        """Move a run to ``status``, refusing to leave a terminal state.

        Section 4.1 makes ``INCONCLUSIVE`` terminal. Allowing a later write to
        turn it into ``MATCHED`` would defeat the whole fail-closed design, so
        the guard lives in the write path.
        """
        now = self._clock.now()
        # The audit itself is over once capture and diff are done, so FINDINGS
        # stamps ``ended_at`` too even though the run may still be planned.
        ended = to_iso(now) if status not in IN_FLIGHT_RUN_STATES else None
        with self.session() as conn, transaction(conn):
            current = conn.execute(
                "SELECT status FROM recon_runs WHERE recon_run_id = ?", (run_id,)
            ).fetchone()
            if current is None:
                raise ReconError("unknown run", recon_run_id=run_id)
            if current["status"] in TERMINAL_RUN_STATES and current["status"] != status:
                raise ReconError(
                    "refusing to move a run out of a terminal state",
                    reason=ReasonCode.POLICY_BLOCK,
                    recon_run_id=run_id,
                    current=current["status"],
                    requested=status,
                )
            conn.execute(
                """
                UPDATE recon_runs
                   SET status = ?,
                       reason_code = COALESCE(?, reason_code),
                       ended_at = COALESCE(?, ended_at)
                 WHERE recon_run_id = ?
                """,
                (status, reason_code.value if reason_code else None, ended, run_id),
            )
            self._audit(conn).append(
                AuditEvent.RUN_STATUS_CHANGED,
                recon_run_id=run_id,
                actor_id=self._settings.worker_id,
                previous=current["status"],
                status=status,
                reason_code=reason_code.value if reason_code else None,
            )

    def finish_inconclusive(self, run_id: str, reason_code: ReasonCode) -> RunRecord:
        """The honest terminal state when evidence is insufficient."""
        self.set_status(run_id, "INCONCLUSIVE", reason_code=reason_code)
        run = self.get_run(run_id)
        assert run is not None
        return run

    def check_deadline(self, run_id: str) -> None:
        """Raise if the run has outlived its budget (FR04-12)."""
        run = self.get_run(run_id)
        if run is None:
            raise ReconError("unknown run", recon_run_id=run_id)
        if self._clock.now() > from_iso(run.deadline_at):
            raise ReconError(
                "run exceeded its deadline",
                reason=ReasonCode.RUN_DEADLINE_EXCEEDED,
                recon_run_id=run_id,
                deadline_at=run.deadline_at,
            )

    def active_run_count(self) -> int:
        """Runs still doing capture or diff work (admission control)."""
        with self.session() as conn:
            return int(
                conn.execute(
                    f"""
                    SELECT COUNT(*) FROM recon_runs
                     WHERE status IN ({",".join("?" * len(IN_FLIGHT_RUN_STATES))})
                    """,
                    tuple(sorted(IN_FLIGHT_RUN_STATES)),
                ).fetchone()[0]
            )

    # ---- source capture --------------------------------------------------

    def pin_source_identity(
        self, run_id: str, *, run_id_source: str, generation: int, snapshot_seq: int
    ) -> None:
        """Record the cut this scan is bound to (section 7.4 step 2).

        Refuses to re-pin a different cut: once page 1 has established S, a
        later page claiming a different S is a contract violation, not a new
        truth to adopt.
        """
        with self.session() as conn, transaction(conn):
            row = conn.execute(
                "SELECT source_run_id, source_generation, source_snapshot_seq FROM recon_runs WHERE recon_run_id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                raise ReconError("unknown run", recon_run_id=run_id)
            if row["source_snapshot_seq"] is not None:
                if (
                    row["source_run_id"] != run_id_source
                    or row["source_generation"] != generation
                    or row["source_snapshot_seq"] != snapshot_seq
                ):
                    raise SourceError(
                        "source identity changed after the cut was pinned",
                        reason=ReasonCode.SOURCE_CUT_DRIFTED,
                        recon_run_id=run_id,
                        pinned=(row["source_run_id"], row["source_generation"], row["source_snapshot_seq"]),
                        offered=(run_id_source, generation, snapshot_seq),
                    )
                return
            conn.execute(
                """
                UPDATE recon_runs
                   SET source_run_id = ?, source_generation = ?, source_snapshot_seq = ?,
                       status = CASE WHEN status = 'CREATED' THEN 'CAPTURING_SOURCE' ELSE status END
                 WHERE recon_run_id = ?
                """,
                (run_id_source, generation, snapshot_seq, run_id),
            )
            self._audit(conn).append(
                AuditEvent.SOURCE_IDENTITY_PINNED,
                recon_run_id=run_id,
                actor_id=self._settings.worker_id,
                source_run_id=run_id_source,
                generation=generation,
                snapshot_seq=snapshot_seq,
            )

    def commit_page(self, run_id: str, page: CapturedPage) -> None:
        """Persist one page's receipt and items atomically (section 7.4).

        A duplicate ``item_key`` violates the manifest primary key. That is
        translated into ``SOURCE_DUPLICATE_KEY`` rather than surfacing as a
        raw storage error, because a source repeating a key across pages is a
        source-contract failure the operator needs named (section 7.4 step 3).
        """
        now = to_iso(self._clock.now())
        with self.session() as conn:
            try:
                with transaction(conn):
                    conn.execute(
                        """
                        INSERT INTO recon_source_page_receipts (
                          recon_run_id, page_index, snapshot_seq, run_id, generation,
                          item_count, page_hash, cursor_digest, is_terminal, committed_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            run_id,
                            page.page_index,
                            page.snapshot_seq,
                            page.run_id,
                            page.generation,
                            len(page.items),
                            page.page_hash,
                            page.cursor_digest,
                            1 if page.is_terminal else 0,
                            now,
                        ),
                    )
                    conn.executemany(
                        """
                        INSERT INTO recon_source_items (
                          recon_run_id, page_index, item_key, source_seq, payload_hash)
                        VALUES (?,?,?,?,?)
                        """,
                        [
                            (run_id, page.page_index, item.item_key, item.source_seq, item.payload_hash)
                            for item in page.items
                        ],
                    )
                    self._audit(conn).append(
                        AuditEvent.SOURCE_PAGE_COMMITTED,
                        recon_run_id=run_id,
                        actor_id=self._settings.worker_id,
                        page_index=page.page_index,
                        item_count=len(page.items),
                        page_hash=page.page_hash,
                        is_terminal=page.is_terminal,
                    )
            except sqlite3.IntegrityError as exc:
                text = str(exc)
                if "recon_source_items" in text:
                    raise SourceError(
                        "source returned the same item key on more than one page",
                        reason=ReasonCode.SOURCE_DUPLICATE_KEY,
                        recon_run_id=run_id,
                        page_index=page.page_index,
                        detail=text,
                    ) from exc
                if "ux_recon_one_terminal_page" in text:
                    raise SourceError(
                        "a terminal page was already recorded for this scan",
                        reason=ReasonCode.SOURCE_PAGE_MISSING,
                        recon_run_id=run_id,
                        page_index=page.page_index,
                        detail=text,
                    ) from exc
                raise

    def get_page_receipt(self, run_id: str, page_index: int) -> sqlite3.Row | None:
        """Read back a receipt, which is how a resumed scan avoids a re-insert.

        After a lost commit ACK the worker does not know whether page N
        landed. It asks here and compares ``page_hash`` before deciding
        (section 7.6, IT04-01).
        """
        with self.session() as conn:
            return conn.execute(
                "SELECT * FROM recon_source_page_receipts WHERE recon_run_id = ? AND page_index = ?",
                (run_id, page_index),
            ).fetchone()

    def page_receipt_count(self, run_id: str) -> int:
        with self.session() as conn:
            return int(
                conn.execute(
                    "SELECT COUNT(*) FROM recon_source_page_receipts WHERE recon_run_id = ?",
                    (run_id,),
                ).fetchone()[0]
            )

    def source_item_count(self, run_id: str) -> int:
        with self.session() as conn:
            return int(
                conn.execute(
                    "SELECT COUNT(*) FROM recon_source_items WHERE recon_run_id = ?", (run_id,)
                ).fetchone()[0]
            )

    def _chain_stats(self, conn: sqlite3.Connection, run_id: str) -> ChainStats:
        row = conn.execute(
            """
            SELECT COUNT(*) AS receipt_count,
                   MAX(page_index) AS max_page_index,
                   SUM(is_terminal) AS terminal_count,
                   SUM(item_count) AS declared_items,
                   COUNT(DISTINCT snapshot_seq || '|' || run_id || '|' || generation) AS distinct_cuts,
                   MIN(snapshot_seq) AS snapshot_seq,
                   MIN(run_id) AS run_id,
                   MIN(generation) AS generation
              FROM recon_source_page_receipts
             WHERE recon_run_id = ?
            """,
            (run_id,),
        ).fetchone()
        terminal_index = conn.execute(
            """
            SELECT page_index FROM recon_source_page_receipts
             WHERE recon_run_id = ? AND is_terminal = 1
            """,
            (run_id,),
        ).fetchone()
        stored = conn.execute(
            "SELECT COUNT(*) FROM recon_source_items WHERE recon_run_id = ?", (run_id,)
        ).fetchone()[0]
        return ChainStats(
            receipt_count=int(row["receipt_count"] or 0),
            max_page_index=row["max_page_index"],
            terminal_count=int(row["terminal_count"] or 0),
            terminal_index=None if terminal_index is None else int(terminal_index["page_index"]),
            declared_items=int(row["declared_items"] or 0),
            stored_items=int(stored or 0),
            distinct_cuts=int(row["distinct_cuts"] or 0),
            snapshot_seq=row["snapshot_seq"],
            run_id=row["run_id"],
            generation=row["generation"],
        )

    def validate_receipt_chain(self, run_id: str) -> tuple[bool, ReasonCode | None, ChainStats]:
        """Prove (or refuse) that the captured source scan is complete.

        Every condition section 6.2 caveat 1 lists is checked here:

        1. at least one receipt exists;
        2. indices are contiguous from 0 (``MAX+1 == COUNT``);
        3. exactly one terminal receipt;
        4. the terminal receipt is the *last* page;
        5. all receipts share one ``(run_id, generation, snapshot_seq)``;
        6. declared item counts equal stored manifest rows.

        Returning the stats alongside the verdict means a refusal can say
        which condition failed, which is what the runbooks need.
        """
        with self.session() as conn:
            stats = self._chain_stats(conn, run_id)
            if stats.receipt_count == 0:
                return False, ReasonCode.SOURCE_INCOMPLETE, stats
            if stats.max_page_index is None or stats.max_page_index + 1 != stats.receipt_count:
                return False, ReasonCode.SOURCE_PAGE_MISSING, stats
            if stats.terminal_count != 1:
                return False, ReasonCode.SOURCE_INCOMPLETE, stats
            if stats.terminal_index != stats.max_page_index:
                return False, ReasonCode.SOURCE_PAGE_MISSING, stats
            if stats.distinct_cuts != 1:
                return False, ReasonCode.SOURCE_CUT_DRIFTED, stats
            if stats.declared_items != stats.stored_items:
                return False, ReasonCode.SOURCE_INCOMPLETE, stats
            return True, None, stats

    def mark_source_complete(self, run_id: str) -> ChainStats:
        """Set ``source_complete`` only when the chain proves completeness.

        The validation and the flag are set in one transaction so a concurrent
        writer cannot add a page between the check and the update.
        """
        with self.session() as conn, transaction(conn):
            stats = self._chain_stats(conn, run_id)
            failure: ReasonCode | None = None
            if stats.receipt_count == 0:
                failure = ReasonCode.SOURCE_INCOMPLETE
            elif stats.max_page_index is None or stats.max_page_index + 1 != stats.receipt_count:
                failure = ReasonCode.SOURCE_PAGE_MISSING
            elif stats.terminal_count != 1 or stats.terminal_index != stats.max_page_index:
                failure = ReasonCode.SOURCE_INCOMPLETE
            elif stats.distinct_cuts != 1:
                failure = ReasonCode.SOURCE_CUT_DRIFTED
            elif stats.declared_items != stats.stored_items:
                failure = ReasonCode.SOURCE_INCOMPLETE

            if failure is not None:
                self._audit(conn).append(
                    AuditEvent.SOURCE_COMPLETE_REFUSED,
                    recon_run_id=run_id,
                    actor_id=self._settings.worker_id,
                    reason_code=failure.value,
                    receipt_count=stats.receipt_count,
                    terminal_count=stats.terminal_count,
                    declared_items=stats.declared_items,
                    stored_items=stats.stored_items,
                )
                raise SourceError(
                    "source manifest is not provably complete",
                    reason=failure,
                    recon_run_id=run_id,
                    receipt_count=stats.receipt_count,
                    max_page_index=stats.max_page_index,
                    terminal_index=stats.terminal_index,
                    declared_items=stats.declared_items,
                    stored_items=stats.stored_items,
                )

            items = conn.execute(
                "SELECT item_key, payload_hash FROM recon_source_items WHERE recon_run_id = ?",
                (run_id,),
            ).fetchall()
            entity = conn.execute(
                "SELECT entity FROM recon_runs WHERE recon_run_id = ?", (run_id,)
            ).fetchone()["entity"]
            digest = root_hash(entity, ((r["item_key"], r["payload_hash"]) for r in items))
            conn.execute(
                """
                UPDATE recon_runs
                   SET source_complete = 1, source_count = ?, source_root_hash = ?
                 WHERE recon_run_id = ?
                """,
                (len(items), digest, run_id),
            )
            self._audit(conn).append(
                AuditEvent.SOURCE_COMPLETE,
                recon_run_id=run_id,
                actor_id=self._settings.worker_id,
                source_count=len(items),
                source_root_hash=digest,
                snapshot_seq=stats.snapshot_seq,
            )
            return stats

    # ---- target capture --------------------------------------------------

    def record_target_manifest(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        rows: Iterable[tuple[str, str, int | None]],
    ) -> int:
        """Insert target manifest rows inside a caller-owned transaction.

        The connection is a parameter on purpose: section 7.3 requires the
        manifest rows and the P03 metadata beside them to come from one
        consistent cut, so the caller owns the transaction boundary and this
        method must not open its own.
        """
        count = 0
        batch: list[tuple[str, str, int | None, str]] = []
        for item_key, payload_hash, source_seq in rows:
            batch.append((run_id, item_key, source_seq, payload_hash))
            count += 1
            if len(batch) >= 1_000:
                conn.executemany(
                    "INSERT INTO recon_target_items (recon_run_id, item_key, source_seq, payload_hash) VALUES (?,?,?,?)",
                    batch,
                )
                batch.clear()
        if batch:
            conn.executemany(
                "INSERT INTO recon_target_items (recon_run_id, item_key, source_seq, payload_hash) VALUES (?,?,?,?)",
                batch,
            )
        return count

    def finalise_target_capture(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        *,
        entity: str,
        target_count: int,
        published_scan_id: str | None,
        target_snapshot_seq: int | None,
        target_deal_watermark: int | None,
    ) -> str:
        """Mark the target capture complete within the capture transaction."""
        items = conn.execute(
            "SELECT item_key, payload_hash FROM recon_target_items WHERE recon_run_id = ?",
            (run_id,),
        ).fetchall()
        digest = root_hash(entity, ((r["item_key"], r["payload_hash"]) for r in items))
        conn.execute(
            """
            UPDATE recon_runs
               SET target_complete = 1, target_count = ?, target_root_hash = ?,
                   target_published_scan_id = ?, target_snapshot_seq = ?,
                   target_deal_watermark = ?, status = 'CAPTURING_TARGET'
             WHERE recon_run_id = ?
            """,
            (
                target_count,
                digest,
                published_scan_id,
                target_snapshot_seq,
                target_deal_watermark,
                run_id,
            ),
        )
        AuditLog(conn, self._clock).append(
            AuditEvent.TARGET_CAPTURED,
            recon_run_id=run_id,
            actor_id=self._settings.worker_id,
            target_count=target_count,
            target_root_hash=digest,
            published_scan_id=published_scan_id,
            target_snapshot_seq=target_snapshot_seq,
            target_deal_watermark=target_deal_watermark,
        )
        return digest

    # ---- diff ------------------------------------------------------------

    def compute_diff(self, run_id: str) -> list[DiffRow]:
        """Set-diff the two manifests (section 6.3).

        Refuses to run unless both manifests are marked complete. The SQL
        alone proves nothing about alignment -- section 6.3 says so
        explicitly -- so the guard is here and the cut checks live in
        ``alignment.py``.
        """
        run = self.get_run(run_id)
        if run is None:
            raise ReconError("unknown run", recon_run_id=run_id)
        if not run.source_complete or not run.target_complete:
            raise SourceError(
                "refusing to diff manifests that are not both complete",
                reason=ReasonCode.SOURCE_INCOMPLETE if not run.source_complete else ReasonCode.TARGET_INCOMPLETE,
                recon_run_id=run_id,
                source_complete=run.source_complete,
                target_complete=run.target_complete,
            )
        with self.session() as conn:
            rows = conn.execute(
                """
                SELECT COALESCE(s.item_key, t.item_key) AS item_key,
                       CASE
                         WHEN s.item_key IS NULL THEN 'EXTRA_TARGET'
                         WHEN t.item_key IS NULL THEN 'MISSING_TARGET'
                         WHEN s.payload_hash <> t.payload_hash THEN 'FIELD_MISMATCH'
                         ELSE 'MATCHED'
                       END AS diff_type,
                       s.payload_hash AS source_hash,
                       t.payload_hash AS target_hash,
                       COALESCE(s.source_seq, t.source_seq) AS source_seq
                  FROM recon_source_items AS s
                  FULL OUTER JOIN recon_target_items AS t
                    ON s.recon_run_id = t.recon_run_id AND s.item_key = t.item_key
                 WHERE COALESCE(s.recon_run_id, t.recon_run_id) = ?
                   AND (s.item_key IS NULL OR t.item_key IS NULL
                        OR s.payload_hash <> t.payload_hash)
                 ORDER BY item_key
                """,
                (run_id,),
            ).fetchall()
        return [
            DiffRow(
                item_key=row["item_key"],
                diff_type=row["diff_type"],
                source_hash=row["source_hash"],
                target_hash=row["target_hash"],
                source_seq=row["source_seq"],
            )
            for row in rows
        ]

    # ---- findings --------------------------------------------------------

    def record_findings(self, run_id: str, findings: Sequence[Mapping[str, Any]]) -> int:
        """Persist classified findings with their evidence digests.

        ``INSERT OR IGNORE`` against ``UNIQUE (run, code, item_key)`` makes a
        replayed diff after a crash idempotent rather than duplicating rows
        (section 7.6 "after diff commit before response").
        """
        now = to_iso(self._clock.now())
        with self.session() as conn, transaction(conn):
            inserted = 0
            for finding in findings:
                cursor = conn.execute(
                    """
                    INSERT OR IGNORE INTO recon_findings (
                      finding_id, recon_run_id, item_key, finding_code, severity, confidence,
                      source_hash, target_hash, source_seq, evidence_hash, explanation_code, created_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        str(uuid.uuid4()),
                        run_id,
                        finding.get("item_key"),
                        str(finding["finding_code"]),
                        str(finding["severity"]),
                        str(finding["confidence"]),
                        finding.get("source_hash"),
                        finding.get("target_hash"),
                        finding.get("source_seq"),
                        str(finding["evidence_hash"]),
                        str(finding["explanation_code"]),
                        now,
                    ),
                )
                inserted += cursor.rowcount or 0
            self._audit(conn).append(
                AuditEvent.FINDINGS_RECORDED,
                recon_run_id=run_id,
                actor_id=self._settings.worker_id,
                offered=len(findings),
                inserted=inserted,
            )
            return inserted

    def findings_for(self, run_id: str) -> list[sqlite3.Row]:
        with self.session() as conn:
            return conn.execute(
                "SELECT * FROM recon_findings WHERE recon_run_id = ? ORDER BY finding_code, item_key",
                (run_id,),
            ).fetchall()

    def finding_counts(self, run_id: str) -> dict[str, int]:
        with self.session() as conn:
            return {
                row["finding_code"]: int(row["n"])
                for row in conn.execute(
                    "SELECT finding_code, COUNT(*) AS n FROM recon_findings WHERE recon_run_id = ? GROUP BY finding_code",
                    (run_id,),
                )
            }

    # ---- plans, actions, approvals, attempts -----------------------------

    def insert_plan(
        self,
        *,
        plan_id: str,
        run_id: str,
        plan_hash: str,
        expected_epoch_id: str,
        expected_target_scan_id: str | None,
        expected_deal_w: int | None,
        expected_lease_version: int | None,
        evidence_captured_at: str,
        status: str,
        created_by: str,
        actions: Sequence[Mapping[str, Any]],
    ) -> None:
        now = self._clock.now()
        with self.session() as conn, transaction(conn):
            conn.execute(
                """
                INSERT INTO recon_plans (
                  plan_id, recon_run_id, plan_hash, expected_epoch_id, expected_target_scan_id,
                  expected_deal_w, expected_lease_version, policy_hash, evidence_captured_at,
                  status, expires_at, created_by, created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    plan_id,
                    run_id,
                    plan_hash,
                    expected_epoch_id,
                    expected_target_scan_id,
                    expected_deal_w,
                    expected_lease_version,
                    self._settings.policy_hash,
                    evidence_captured_at,
                    status,
                    to_iso(now + timedelta(seconds=self._settings.plan_ttl_s)),
                    created_by,
                    to_iso(now),
                ),
            )
            for action in actions:
                conn.execute(
                    """
                    INSERT INTO recon_actions (
                      action_id, plan_id, action_order, action_type, intent_id,
                      status, parameters_json, parameters_hash)
                    VALUES (?,?,?,?,?,?,?,?)
                    """,
                    (
                        action["action_id"],
                        plan_id,
                        action["action_order"],
                        str(action["action_type"]),
                        action["intent_id"],
                        action.get("status", "PLANNED"),
                        json.dumps(action["parameters"], sort_keys=True, separators=(",", ":")),
                        action["parameters_hash"],
                    ),
                )
            self._audit(conn).append(
                AuditEvent.PLAN_DRAFTED,
                recon_run_id=run_id,
                plan_id=plan_id,
                actor_id=created_by,
                plan_hash=plan_hash,
                status=status,
                action_count=len(actions),
                action_types=sorted({str(a["action_type"]) for a in actions}),
            )

    def get_plan(self, plan_id: str) -> sqlite3.Row | None:
        with self.session() as conn:
            return conn.execute("SELECT * FROM recon_plans WHERE plan_id = ?", (plan_id,)).fetchone()

    def plan_actions(self, plan_id: str) -> list[sqlite3.Row]:
        with self.session() as conn:
            return conn.execute(
                "SELECT * FROM recon_actions WHERE plan_id = ? ORDER BY action_order", (plan_id,)
            ).fetchall()

    def set_plan_status(
        self, plan_id: str, status: str, *, reason_code: ReasonCode | None = None, actor_id: str | None = None
    ) -> None:
        with self.session() as conn, transaction(conn):
            row = conn.execute("SELECT status, recon_run_id FROM recon_plans WHERE plan_id = ?", (plan_id,)).fetchone()
            if row is None:
                raise ReconError("unknown plan", plan_id=plan_id)
            conn.execute(
                "UPDATE recon_plans SET status = ?, reason_code = COALESCE(?, reason_code) WHERE plan_id = ?",
                (status, reason_code.value if reason_code else None, plan_id),
            )
            self._audit(conn).append(
                AuditEvent.PLAN_STALE if status == "STALE" else AuditEvent.DISPATCH_RESOLVED,
                recon_run_id=row["recon_run_id"],
                plan_id=plan_id,
                actor_id=actor_id or self._settings.worker_id,
                previous=row["status"],
                status=status,
                reason_code=reason_code.value if reason_code else None,
            )

    def set_action_status(
        self, action_id: str, status: str, *, reason_code: ReasonCode | None = None
    ) -> None:
        with self.session() as conn, transaction(conn):
            conn.execute(
                "UPDATE recon_actions SET status = ?, reason_code = COALESCE(?, reason_code) WHERE action_id = ?",
                (status, reason_code.value if reason_code else None, action_id),
            )

    def record_approval(
        self, *, approval_id: str, plan_id: str, actor_id: str, role: str, decision: str, plan_hash: str
    ) -> None:
        with self.session() as conn, transaction(conn):
            row = conn.execute("SELECT recon_run_id FROM recon_plans WHERE plan_id = ?", (plan_id,)).fetchone()
            conn.execute(
                """
                INSERT INTO recon_approvals (approval_id, plan_id, actor_id, role, decision, plan_hash, decided_at)
                VALUES (?,?,?,?,?,?,?)
                """,
                (approval_id, plan_id, actor_id, role, decision, plan_hash, to_iso(self._clock.now())),
            )
            self._audit(conn).append(
                AuditEvent.APPROVAL_RECORDED,
                recon_run_id=row["recon_run_id"] if row else None,
                plan_id=plan_id,
                actor_id=actor_id,
                role=role,
                decision=decision,
                plan_hash=plan_hash,
            )

    def approvals_for(self, plan_id: str) -> list[sqlite3.Row]:
        with self.session() as conn:
            return conn.execute(
                "SELECT * FROM recon_approvals WHERE plan_id = ? ORDER BY decided_at", (plan_id,)
            ).fetchall()

    def create_attempt(self, *, attempt_id: str, action_id: str, request_id: str, status: str) -> None:
        with self.session() as conn, transaction(conn):
            conn.execute(
                """
                INSERT INTO recon_attempts (attempt_id, action_id, request_id, status, created_at)
                VALUES (?,?,?,?,?)
                """,
                (attempt_id, action_id, request_id, status, to_iso(self._clock.now())),
            )

    def resolve_attempt(
        self, attempt_id: str, *, status: str, receipt_hash: str | None = None, error_code: str | None = None
    ) -> None:
        with self.session() as conn, transaction(conn):
            conn.execute(
                """
                UPDATE recon_attempts
                   SET status = ?, receipt_hash = ?, error_code = ?, resolved_at = ?
                 WHERE attempt_id = ?
                """,
                (status, receipt_hash, error_code, to_iso(self._clock.now()), attempt_id),
            )

    def attempts_for(self, action_id: str) -> list[sqlite3.Row]:
        with self.session() as conn:
            return conn.execute(
                "SELECT * FROM recon_attempts WHERE action_id = ? ORDER BY created_at", (action_id,)
            ).fetchall()

    # ---- retention (INV04-15) -------------------------------------------

    def release_evidence(self, *, now_iso: str | None = None) -> int:
        """Drop manifests for runs whose evidence is past retention.

        Refuses to touch a run that any non-terminal plan still references:
        deleting the evidence under a repair that is still in flight would
        leave an unexplainable change (section 3.3 ``recon.cleanup`` "delete
        evidence of active repair").
        """
        cutoff = now_iso or to_iso(
            self._clock.now() - timedelta(seconds=self._settings.evidence_retention_s)
        )
        with self.session() as conn, transaction(conn):
            candidates = [
                row["recon_run_id"]
                for row in conn.execute(
                    f"""
                    SELECT r.recon_run_id
                      FROM recon_runs r
                     WHERE r.status NOT IN ({",".join("?" * len(IN_FLIGHT_RUN_STATES))})
                       AND r.ended_at IS NOT NULL
                       AND r.ended_at < ?
                       AND NOT EXISTS (
                             SELECT 1 FROM recon_plans p
                              WHERE p.recon_run_id = r.recon_run_id
                                AND p.status NOT IN ('VERIFIED','FAILED','REJECTED','STALE'))
                    """,
                    (*sorted(IN_FLIGHT_RUN_STATES), cutoff),
                )
            ]
            released = 0
            for run_id in candidates:
                conn.execute("DELETE FROM recon_target_items WHERE recon_run_id = ?", (run_id,))
                conn.execute("DELETE FROM recon_source_items WHERE recon_run_id = ?", (run_id,))
                released += 1
            if released:
                self._audit(conn).append(
                    AuditEvent.EVIDENCE_GC,
                    actor_id=self._settings.worker_id,
                    runs_released=released,
                    cutoff=cutoff,
                )
            return released
