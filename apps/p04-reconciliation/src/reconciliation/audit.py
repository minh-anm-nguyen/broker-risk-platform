"""Tamper-evident audit log (SYSTEM_DESIGN sections 6.2, 12.2).

Each event stores the digest of the previous event, so editing or deleting a
row in the middle breaks every link after it and ``verify_chain`` reports the
first broken sequence number.

What this does **not** give you, stated plainly because the design is explicit
about it (section 6.2 caveat 3): a privileged operator who rewrites the whole
table and recomputes the chain produces a valid chain. Detecting that requires
an append-only store or an external anchor outside this database. The chain
catches accidental and single-row tampering, which is what it is for.

Events carry ``safe_details`` only: reason codes, hashes, counts and
identifiers. Account balances and deal amounts are deliberately absent, so an
audit export cannot become an unintended copy of financial data (section
12.2, FR04-13 "safe redacted evidence").
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Mapping
from typing import Any, Final

from .canonical import chain_hash
from .clock import Clock, to_iso

#: Keys that may never appear in an audit detail payload. Money belongs in the
#: manifests, which are access-controlled; it does not belong in a log line.
_REDACTED_KEYS: Final[frozenset[str]] = frozenset(
    {
        "balance",
        "credit",
        "commission",
        "swap",
        "price",
        "open_price",
        "realized_pnl_gross",
        "volume_lots",
        "token",
        "lease_token",
        "authorization",
        "source_data_token",
    }
)


class AuditEvent:
    """Well-known event types. Strings are a contract (section 12.2)."""

    RUN_CREATED = "RUN_CREATED"
    RUN_STATUS_CHANGED = "RUN_STATUS_CHANGED"
    SOURCE_IDENTITY_PINNED = "SOURCE_IDENTITY_PINNED"
    SOURCE_PAGE_COMMITTED = "SOURCE_PAGE_COMMITTED"
    SOURCE_COMPLETE = "SOURCE_COMPLETE"
    SOURCE_COMPLETE_REFUSED = "SOURCE_COMPLETE_REFUSED"
    TARGET_CAPTURED = "TARGET_CAPTURED"
    TARGET_CAPTURE_REFUSED = "TARGET_CAPTURE_REFUSED"
    ALIGNMENT_DECIDED = "ALIGNMENT_DECIDED"
    DIFF_COMPLETED = "DIFF_COMPLETED"
    FINDINGS_RECORDED = "FINDINGS_RECORDED"
    PLAN_DRAFTED = "PLAN_DRAFTED"
    PLAN_REJECTED = "PLAN_REJECTED"
    APPROVAL_RECORDED = "APPROVAL_RECORDED"
    APPROVAL_REFUSED = "APPROVAL_REFUSED"
    PLAN_APPROVED = "PLAN_APPROVED"
    DISPATCH_STARTED = "DISPATCH_STARTED"
    DISPATCH_UNKNOWN = "DISPATCH_UNKNOWN"
    DISPATCH_RESOLVED = "DISPATCH_RESOLVED"
    PLAN_STALE = "PLAN_STALE"
    VERIFICATION_STARTED = "VERIFICATION_STARTED"
    VERIFICATION_RESULT = "VERIFICATION_RESULT"
    LEASE_ACQUIRED = "LEASE_ACQUIRED"
    LEASE_LOST = "LEASE_LOST"
    EVIDENCE_GC = "EVIDENCE_GC"
    PRIVILEGE_DENIED = "PRIVILEGE_DENIED"


def redact(details: Mapping[str, Any]) -> dict[str, Any]:
    """Drop value-bearing financial fields, keeping their presence visible."""
    safe: dict[str, Any] = {}
    for key, value in details.items():
        if key.lower() in _REDACTED_KEYS:
            safe[key] = "[redacted]"
        elif isinstance(value, Mapping):
            safe[key] = redact(value)
        else:
            safe[key] = value
    return safe


class AuditLog:
    """Append-only writer over an existing connection.

    Takes a connection rather than opening one so an audit event can be
    committed inside the same transaction as the state change it describes.
    A decision that is durable while its audit record is not would leave the
    system unable to explain itself after a crash (section 7.6).
    """

    def __init__(self, conn: sqlite3.Connection, clock: Clock) -> None:
        self._conn = conn
        self._clock = clock

    def append(
        self,
        event_type: str,
        *,
        recon_run_id: str | None = None,
        plan_id: str | None = None,
        actor_id: str | None = None,
        **details: Any,
    ) -> str:
        row = self._conn.execute(
            "SELECT sequence_no, event_hash FROM recon_audit ORDER BY sequence_no DESC LIMIT 1"
        ).fetchone()
        previous_hash = row["event_hash"] if row is not None else None
        sequence_no = (row["sequence_no"] + 1) if row is not None else 1

        safe_details = redact(details)
        created_at = to_iso(self._clock.now())
        event_body = {
            "sequence_no": sequence_no,
            "event_type": event_type,
            "recon_run_id": recon_run_id,
            "plan_id": plan_id,
            "actor_id": actor_id,
            "safe_details": safe_details,
            "created_at": created_at,
        }
        event_hash = chain_hash(previous_hash, event_body)
        audit_id = str(uuid.uuid4())
        self._conn.execute(
            """
            INSERT INTO recon_audit (audit_id, sequence_no, recon_run_id, plan_id, event_type,
                                     actor_id, safe_details, prev_event_hash, event_hash, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                audit_id,
                sequence_no,
                recon_run_id,
                plan_id,
                event_type,
                actor_id,
                json.dumps(safe_details, sort_keys=True, separators=(",", ":")),
                previous_hash,
                event_hash,
                created_at,
            ),
        )
        return event_hash


def verify_chain(conn: sqlite3.Connection) -> tuple[bool, int | None, str | None]:
    """Recompute the whole chain.

    Returns ``(ok, first_bad_sequence_no, detail)``. Used by the evidence
    integrity test and by ``reconctl audit verify``.
    """
    previous_hash: str | None = None
    expected_sequence = 1
    for row in conn.execute(
        """
        SELECT sequence_no, recon_run_id, plan_id, event_type, actor_id,
               safe_details, prev_event_hash, event_hash, created_at
        FROM recon_audit ORDER BY sequence_no
        """
    ):
        if row["sequence_no"] != expected_sequence:
            return False, row["sequence_no"], "sequence numbers are not contiguous"
        if row["prev_event_hash"] != previous_hash:
            return False, row["sequence_no"], "prev_event_hash does not match the preceding event"
        body = {
            "sequence_no": row["sequence_no"],
            "event_type": row["event_type"],
            "recon_run_id": row["recon_run_id"],
            "plan_id": row["plan_id"],
            "actor_id": row["actor_id"],
            "safe_details": json.loads(row["safe_details"]),
            "created_at": row["created_at"],
        }
        recomputed = chain_hash(previous_hash, body)
        if recomputed != row["event_hash"]:
            return False, row["sequence_no"], "event_hash does not match the recorded body"
        previous_hash = row["event_hash"]
        expected_sequence += 1
    return True, None, None
