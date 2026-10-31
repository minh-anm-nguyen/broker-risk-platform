"""Capture a consistent target manifest from PostgreSQL (section 7.3).

The hard requirement is that the captured rows and the P03 metadata describing
them come from **one** database cut. Section 7.3 is explicit about the failure
mode it is guarding against: paging ``accounts_current`` across several
``READ COMMITTED`` statements while P03 promotes a new scan, then believing
every page belonged to the same publication.

So this module does all of it inside one transaction opened by
``capture_transaction``:

1. read the P03 checkpoint and published scan metadata,
2. re-verify the guards against *those* values -- not against a status fetched
   over a service call before the transaction began,
3. stream the rows and recompute each canonical digest,
4. write the manifest and mark the capture complete.

If the cut cannot be proven -- a concurrent publish, a lock timeout, a stale
snapshot -- the whole transaction rolls back and ``target_complete`` stays
false. A stale-but-internally-consistent capture is still refused, because the
metadata it would be compared against has moved (IT04-07, INV04-14).

Hashes are always recomputed from business fields with P04's own canonical
mapper. P03's stored ``payload_hash`` is never compared directly: section 6.4
only permits that once both encoders are confirmed identical, and they are
not.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from .canonical import item_key as canonical_item_key
from .canonical import payload_hash
from .clock import Clock
from .config import Settings
from .db.engine import CaptureStrategy, Database, Role, capture_transaction, classify_sqlite_error
from .db.repository import ReconRepository
from .errors import (
    CanonicalizationError,
    ReasonCode,
    TargetCaptureError,
    TargetConflictError,
)

#: Business columns read per entity. ``source_seq`` travels beside the hash as
#: provenance for deals; it is never part of the digest (section 5.2 rule 7).
_SELECT: Mapping[str, tuple[str, str]] = {
    "accounts": (
        "SELECT account_id, currency, balance, credit FROM accounts_current WHERE epoch_id = ?",
        "accounts_current",
    ),
    "positions": (
        "SELECT position_id, account_id, symbol, side, volume_lots, open_price, status"
        " FROM positions_current WHERE epoch_id = ?",
        "positions_current",
    ),
}


@dataclass(frozen=True, slots=True)
class TargetCaptureResult:
    complete: bool
    count: int
    reason_code: ReasonCode | None
    published_scan_id: str | None = None
    target_snapshot_seq: int | None = None
    target_deal_watermark: int | None = None
    root_hash: str | None = None


class TargetCaptureWorker:
    """Reads P03's published state as evidence, never as something to edit."""

    def __init__(
        self,
        *,
        db: Database,
        repository: ReconRepository,
        settings: Settings,
        clock: Clock,
        strategy: CaptureStrategy = CaptureStrategy.IMMEDIATE,
    ) -> None:
        self._db = db
        self._repo = repository
        self._settings = settings
        self._clock = clock
        self._strategy = strategy

    def capture(
        self,
        run_id: str,
        *,
        entity: str,
        epoch_id: str,
        source_snapshot_seq: int,
    ) -> TargetCaptureResult:
        """Capture the target manifest, or refuse with a reason code."""
        conn = self._db.connect(Role.RECON)
        try:
            with capture_transaction(conn, strategy=self._strategy):
                metadata = self._read_metadata(conn, epoch_id=epoch_id, entity=entity)

                guard = self._check_guards(
                    metadata, entity=entity, source_snapshot_seq=source_snapshot_seq
                )
                if guard is not None:
                    raise TargetCaptureError(
                        "target state does not satisfy the comparison preconditions",
                        reason=guard,
                        epoch_id=epoch_id,
                        entity=entity,
                    )

                rows = self._iter_manifest_rows(
                    conn, entity=entity, epoch_id=epoch_id, max_seq=source_snapshot_seq
                )
                count = self._repo.record_target_manifest(conn, run_id, rows)

                digest = self._repo.finalise_target_capture(
                    conn,
                    run_id,
                    entity=entity,
                    target_count=count,
                    published_scan_id=metadata["published_scan_id"],
                    target_snapshot_seq=metadata["snapshot_seq"],
                    target_deal_watermark=metadata["watermark_seq"],
                )

            return TargetCaptureResult(
                complete=True,
                count=count,
                reason_code=None,
                published_scan_id=metadata["published_scan_id"],
                target_snapshot_seq=metadata["snapshot_seq"],
                target_deal_watermark=metadata["watermark_seq"],
                root_hash=digest,
            )

        except TargetConflictError:
            # The publisher moved under us. Reported as inconclusive, which is
            # the honest answer: we do not know whether the target is correct.
            return TargetCaptureResult(
                complete=False, count=0, reason_code=ReasonCode.TARGET_CAPTURE_CONFLICT
            )
        except TargetCaptureError as exc:
            return TargetCaptureResult(complete=False, count=0, reason_code=exc.reason)
        except CanonicalizationError as exc:
            return TargetCaptureResult(complete=False, count=0, reason_code=exc.reason)
        except sqlite3.Error as exc:
            return TargetCaptureResult(
                complete=False, count=0, reason_code=classify_sqlite_error(exc).reason
            )
        finally:
            conn.close()

    # ---- internals -------------------------------------------------------

    def _read_metadata(
        self, conn: sqlite3.Connection, *, epoch_id: str, entity: str
    ) -> dict[str, Any]:
        """Read epoch, checkpoint and published scan in the capture snapshot."""
        epoch = conn.execute(
            "SELECT status FROM source_epochs WHERE epoch_id = ?", (epoch_id,)
        ).fetchone()
        checkpoint = conn.execute(
            "SELECT * FROM sync_checkpoints WHERE epoch_id = ? AND entity = ?",
            (epoch_id, entity),
        ).fetchone()
        scan = None
        if checkpoint is not None and checkpoint["published_scan_id"]:
            scan = conn.execute(
                "SELECT * FROM sync_runs WHERE scan_id = ?", (checkpoint["published_scan_id"],)
            ).fetchone()
        return {
            "epoch_status": None if epoch is None else epoch["status"],
            "published": bool(checkpoint["published"]) if checkpoint is not None else False,
            "published_scan_id": checkpoint["published_scan_id"] if checkpoint is not None else None,
            "watermark_seq": int(checkpoint["watermark_seq"]) if checkpoint is not None else None,
            "filter_hash": checkpoint["filter_hash"] if checkpoint is not None else None,
            "snapshot_seq": scan["snapshot_seq"] if scan is not None else None,
            "terminal_receipt": bool(scan["terminal_receipt"]) if scan is not None else False,
            "scan_status": scan["status"] if scan is not None else None,
        }

    def _check_guards(
        self, metadata: Mapping[str, Any], *, entity: str, source_snapshot_seq: int
    ) -> ReasonCode | None:
        """Re-verify every target-side precondition inside the capture.

        Returns the reason code of the first failure, or None to proceed.
        """
        if metadata["epoch_status"] is None:
            return ReasonCode.EPOCH_MISMATCH
        if metadata["epoch_status"] != "ACTIVE":
            return ReasonCode.EPOCH_MISMATCH
        if metadata["filter_hash"] not in (None, "UNFILTERED"):
            # A watermark derived from a filtered scan cannot bound a
            # full-set comparison (section 5.2 step 4).
            return ReasonCode.SOURCE_SCAN_FILTERED

        if entity == "deals":
            watermark = metadata["watermark_seq"]
            if watermark is None:
                return ReasonCode.TARGET_NO_PUBLISHED_SCAN
            if watermark < source_snapshot_seq:
                # Not a missing-data finding: P03 simply has not reached this
                # snapshot yet (INV04-04). Checked before ``published`` so the
                # reason code matches section 5.6 and the runbook (RB04-05).
                return ReasonCode.TARGET_NOT_CAUGHT_UP
            if not metadata["published"]:
                return ReasonCode.TARGET_NO_PUBLISHED_SCAN
            return None

        if not metadata["published"] or metadata["published_scan_id"] is None:
            return ReasonCode.TARGET_NO_PUBLISHED_SCAN
        if metadata["scan_status"] != "PUBLISHED" or not metadata["terminal_receipt"]:
            # A projection whose scan never reached a terminal receipt is a
            # P03 invariant violation, not a data difference (section 5.6).
            return ReasonCode.P03_INVARIANT_BROKEN
        return None

    def _iter_manifest_rows(
        self,
        conn: sqlite3.Connection,
        *,
        entity: str,
        epoch_id: str,
        max_seq: int,
    ) -> Iterator[tuple[str, str, int | None]]:
        """Stream ``(item_key, payload_hash, source_seq)`` for the target.

        A generator, not a list: section 5.2 rule 6 forbids loading a million
        rows into memory, and ``record_target_manifest`` batches the inserts
        as they arrive.
        """
        if entity == "deals":
            cursor = conn.execute(
                """
                SELECT deal_id, source_seq, account_id, position_id, symbol, entry, side,
                       volume_lots, price, realized_pnl_gross, commission, swap, executed_at
                  FROM deals
                 WHERE epoch_id = ? AND source_seq <= ?
                 ORDER BY source_seq
                """,
                (epoch_id, max_seq),
            )
        else:
            sql, _table = _SELECT[entity]
            cursor = conn.execute(sql, (epoch_id,))

        for row in cursor:
            record = {key: row[key] for key in row.keys()}
            source_seq = record.pop("source_seq", None)
            yield (
                canonical_item_key(entity, record),
                payload_hash(entity, record),
                int(source_seq) if source_seq is not None else None,
            )
