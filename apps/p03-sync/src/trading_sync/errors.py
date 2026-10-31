"""Typed error taxonomy.

Every failure the sync engine can hit is one of these, and each one carries the
three facts the design's action table (section 9.1) keys off:

``reason_code``
    A stable string recorded in ``sync_audit`` / ``quarantine_items`` and used
    as a low-cardinality metric label. Never a free-form message.
``retryable``
    Whether a *bounded* retry may re-attempt the same work. A money or identity
    conflict is never retryable: replaying it would only rediscover the
    conflict while burning the retry budget (section 9.2).
``checkpoint_safe``
    Whether the watermark may still advance if the rest of the scan succeeded.
    Only true for faults that cannot have cost us data. Anything that might
    mean "we did not see every record" is false, which is what stops a
    99-of-100-page scan from publishing (INV-02).

``safe_details`` is the only payload that reaches a log line, an audit row or a
quarantine row, so it must never carry raw source records, credentials or
account identifiers -- see :func:`trading_sync.infrastructure.logging.redact`.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "SyncError",
    "SourceContractError",
    "SourceCapabilityError",
    "SchemaError",
    "InvalidIdentifierError",
    "InvalidMoneyError",
    "InvalidTimestampError",
    "ProvenanceError",
    "MixedSnapshotError",
    "SourceEpochChangedError",
    "EpochBlockedError",
    "SequenceError",
    "SnapshotBoundError",
    "CursorError",
    "PageContractError",
    "FilterScopeError",
    "IncompleteScanError",
    "SnapshotExpiredError",
    "DataIntegrityError",
    "ImmutableDealConflict",
    "DuplicateKeyConflict",
    "ReceiptConflict",
    "TransientSourceError",
    "UpstreamTimeoutError",
    "OverloadedError",
    "UpstreamUnavailableError",
    "AuthenticationError",
    "AuthorizationError",
    "ResponseTooLargeError",
    "LeaseLostError",
    "StaleScanError",
    "TransientDatabaseError",
    "DatabaseUnavailableError",
    "StorageFullError",
    "AdmissionRejectedError",
    "ScanBudgetExceededError",
    "ConfigurationError",
]


class SyncError(Exception):
    """Base class; subclasses set the three policy attributes as defaults."""

    reason_code: str = "UNCLASSIFIED"
    retryable: bool = False
    #: May the watermark advance if everything else about the scan is sound?
    checkpoint_safe: bool = False

    def __init__(self, message: str = "", /, **safe_details: Any) -> None:
        super().__init__(message or self.reason_code)
        self.message = message or self.reason_code
        self.safe_details: dict[str, Any] = dict(safe_details)

    def __str__(self) -> str:
        if not self.safe_details:
            return f"{self.reason_code}: {self.message}"
        rendered = " ".join(f"{k}={v!r}" for k, v in sorted(self.safe_details.items()))
        return f"{self.reason_code}: {self.message} ({rendered})"


# ---------------------------------------------------------------------------
# Source contract and capability negotiation (matrix group A)
# ---------------------------------------------------------------------------


class SourceContractError(SyncError):
    """The source answered, but not in a shape the pinned contract allows.

    Raised instead of filling in what is missing. A meta response without a
    ``run_id`` cannot be repaired by generating one: a synthetic identity would
    let two different simulator runs share a watermark (WF3-001, WF3-002).
    """

    reason_code = "SOURCE_CONTRACT_ERROR"


class SourceCapabilityError(SyncError):
    """The source cannot support the sync mode being asked of it.

    A ``manager_sdk`` source with no stable event sequence cannot drive
    watermark-based incremental deal sync at all. Refusing is the whole point;
    the alternative is a watermark over a sequence nobody promised to keep
    monotonic (WF3-008, WF3-180).
    """

    reason_code = "CAPABILITY_UNSUPPORTED"


class SchemaError(SyncError):
    """A field is missing, has the wrong type, or the schema version is unknown.

    Note the asymmetry with a terminal page: an *absent* ``next_cursor`` field
    is a schema error, not a terminal page. Reading it as terminal would turn a
    truncated response into a completed scan (WF3-044).
    """

    reason_code = "SCHEMA_ERROR"


class InvalidIdentifierError(SchemaError):
    reason_code = "INVALID_IDENTIFIER"


class InvalidMoneyError(SchemaError):
    """A monetary value is not exactly representable under declared policy.

    Covers JSON floats, non-finite values and anything outside
    ``NUMERIC(38,12)``. Rejecting beats letting PostgreSQL round, because a
    silent round changes a number somebody is accountable for (WF3-086..089).
    """

    reason_code = "INVALID_MONEY"


class InvalidTimestampError(SchemaError):
    reason_code = "INVALID_TIMESTAMP"


# ---------------------------------------------------------------------------
# Provenance and source identity (matrix group B)
# ---------------------------------------------------------------------------


class ProvenanceError(SyncError):
    reason_code = "PROVENANCE_MISMATCH"


class MixedSnapshotError(ProvenanceError):
    """Pages in one scan disagree about ``(run_id, generation, snapshot_seq)``.

    The scan is abandoned rather than stitched together, because pages from two
    snapshots are not a snapshot of anything (INV-03, WF3-038).
    """

    reason_code = "MIXED_SNAPSHOT"


class SourceEpochChangedError(ProvenanceError):
    """The source restarted or reset underneath an in-flight scan.

    Deal ids are unique only within an epoch, so continuing would merge two
    namespaces and could overwrite one run's money with another's
    (WF3-013..WF3-015).
    """

    reason_code = "SOURCE_EPOCH_CHANGED"


class EpochBlockedError(ProvenanceError):
    """The epoch was fenced while this scan was still running.

    Raised at the commit guard, deliberately later than the fetch: pages
    already in hand for a fenced epoch must not reach the sink (WF3-018,
    WF3-019).
    """

    reason_code = "SOURCE_EPOCH_BLOCKED"


# ---------------------------------------------------------------------------
# Sequence, cursor and completeness (matrix groups C and D)
# ---------------------------------------------------------------------------


class SequenceError(SyncError):
    """``source_seq`` violates the ordering the source contract promises.

    Gaps are legal and are *not* this error -- non-deal events consume sequence
    numbers too (INV-11, WF3-025). Going backwards, or repeating a sequence
    under a different deal id, is this error (WF3-026, WF3-029).
    """

    reason_code = "SEQUENCE_VIOLATION"


class SnapshotBoundError(SequenceError):
    """A record sits outside ``(W, S]``, the window the snapshot covers.

    A ``source_seq`` above ``S`` means the page is not from the snapshot it
    claims to be from, so that snapshot cannot justify a watermark (WF3-032).
    """

    reason_code = "SNAPSHOT_BOUND_VIOLATION"


class CursorError(SyncError):
    """Cursor misuse: a repeat, a cycle, or a cursor from another entity."""

    reason_code = "CURSOR_ERROR"
    retryable = True


class PageContractError(SyncError):
    """Page-shaped contract breach: oversized page, duplicate ids, bad count."""

    reason_code = "PAGE_CONTRACT_ERROR"


class FilterScopeError(SyncError):
    """A filtered read is being used to justify an unfiltered conclusion.

    Two hazards share this code and both fail closed: advancing the global deal
    watermark from a filtered scan (INV-18, WF3-058), and inferring that a
    position is absent everywhere because it is absent from a ``status=OPEN``
    listing (WF3-079, WF3-224).
    """

    reason_code = "FILTER_SCOPE_VIOLATION"


class IncompleteScanError(SyncError):
    """The scan stopped before proving it had read the whole snapshot.

    The watermark holds. This is the error that makes "99 of 100 pages
    committed" a failure rather than a near-success (INV-02, WF3-061).
    """

    reason_code = "INCOMPLETE_SCAN"
    retryable = True


class SnapshotExpiredError(SyncError):
    """The source's snapshot or cursor aged out mid-scan (HTTP 410).

    Retryable, but only as a *fresh scan from the durable watermark*. Resuming
    the dead cursor is exactly what would skip records (WF3-037, WF3-135).
    """

    reason_code = "SNAPSHOT_EXPIRED"
    retryable = True


# ---------------------------------------------------------------------------
# Data integrity -- never retried hot (matrix groups H and I)
# ---------------------------------------------------------------------------


class DataIntegrityError(SyncError):
    reason_code = "DATA_INTEGRITY_CONFLICT"


class ImmutableDealConflict(DataIntegrityError):
    """A stored deal came back from the source with different content.

    The stored row wins and the scan stops. ``ON CONFLICT DO UPDATE`` on a
    money column would make this class of incident invisible, which is why the
    sink never uses one for deals (section 6.3, WF3-028, FMEA row 2).
    """

    reason_code = "IMMUTABLE_DEAL_CONFLICT"


class DuplicateKeyConflict(DataIntegrityError):
    """One page carries the same key twice with different content.

    Caught before the SQL batch so the outcome is a classified conflict rather
    than a cardinality-violation surprise out of ``ON CONFLICT`` (WF3-099).
    """

    reason_code = "DUPLICATE_KEY_CONFLICT"


class ReceiptConflict(DataIntegrityError):
    """A page index was already committed under a different content hash.

    An honest replay of page 7 produces identical content; a different hash
    means the source changed its answer, so the original receipt stands
    (WF3-059).
    """

    reason_code = "RECEIPT_CONFLICT"


# ---------------------------------------------------------------------------
# Transport (matrix group L)
# ---------------------------------------------------------------------------


class TransientSourceError(SyncError):
    reason_code = "UPSTREAM_TRANSIENT"
    retryable = True


class UpstreamTimeoutError(TransientSourceError):
    reason_code = "UPSTREAM_TIMEOUT"


class OverloadedError(TransientSourceError):
    """429. ``retry_after_seconds`` is clamped by policy, not trusted as sent."""

    reason_code = "UPSTREAM_OVERLOADED"

    def __init__(
        self,
        message: str = "",
        /,
        retry_after_seconds: float | None = None,
        **safe_details: Any,
    ) -> None:
        super().__init__(message, **safe_details)
        self.retry_after_seconds = retry_after_seconds


class UpstreamUnavailableError(TransientSourceError):
    reason_code = "UPSTREAM_UNAVAILABLE"


class AuthenticationError(SyncError):
    """401. Deliberately not retryable: retrying wrong credentials cannot fix
    them, and a retry loop against an auth endpoint looks like an attack
    (WF3-138)."""

    reason_code = "UPSTREAM_UNAUTHENTICATED"


class AuthorizationError(SyncError):
    """403. Not retryable, for the same reason as 401 (WF3-139)."""

    reason_code = "UPSTREAM_FORBIDDEN"


class ResponseTooLargeError(SyncError):
    """The response passed the byte cap and was refused before a full decode.

    The cap is enforced while reading rather than after, so an unexpected 50 MB
    body never becomes resident memory (WF3-147, INV-15).
    """

    reason_code = "RESPONSE_TOO_LARGE"


# ---------------------------------------------------------------------------
# Ownership and durability (matrix groups J and K)
# ---------------------------------------------------------------------------


class LeaseLostError(SyncError):
    """This worker no longer owns the scan scope.

    Pages it already committed are harmless -- they are idempotent and the new
    owner re-reads them. What it must not do is publish, because its view of
    the snapshot is no longer authoritative (INV-09, WF3-122).
    """

    reason_code = "LEASE_LOST"


class StaleScanError(SyncError):
    """A scan finished after a newer one had already published.

    Letting it promote would roll the current projection back to an older
    snapshot (WF3-068).
    """

    reason_code = "STALE_SCAN"


class OutboxEmissionError(SyncError):
    """An outbox row could not be produced for a Deal that was about to commit.

    Not retryable by replaying the page: the same input would fail the same
    way. The transaction rolls back, so the Deal is not committed either -
    which is the point. A Deal in the database with no event in the outbox
    would be invisible to every downstream component and would look like
    success (contract gap CG-08).
    """

    reason_code = "OUTBOX_EMISSION_FAILED"


class TransientDatabaseError(SyncError):
    """Serialization failure (40001) or deadlock (40P01).

    Retryable because the *whole* transaction is replayed; a partial retry
    would not be idempotent (WF3-114, WF3-115).
    """

    reason_code = "DB_TRANSIENT"
    retryable = True


class DatabaseUnavailableError(SyncError):
    """The database is unreachable, or a connection dropped at an unknown point.

    A connection lost during COMMIT proves nothing either way: recovery re-reads
    durable state rather than guessing (section 6.5, WF3-111).
    """

    reason_code = "DB_UNAVAILABLE"
    retryable = True


class StorageFullError(SyncError):
    """Out of disk. Fetching stops and the watermark holds (WF3-112, WF3-149)."""

    reason_code = "STORAGE_FULL"


# ---------------------------------------------------------------------------
# Local admission and configuration
# ---------------------------------------------------------------------------


class AdmissionRejectedError(SyncError):
    """Refused at the gate because the queue is at capacity.

    Rejecting *is* the containment: a thousand triggers must not become a
    thousand scans (WF3-148).
    """

    reason_code = "ADMISSION_REJECTED"
    retryable = True


class ScanBudgetExceededError(SyncError):
    """The scan hit its wall-time or page-count cap.

    Recoverable and watermark-safe: the next scan restarts from the durable
    watermark (WF3-047, WF3-150).
    """

    reason_code = "SCAN_BUDGET_EXCEEDED"
    retryable = True


class ConfigurationError(SyncError):
    reason_code = "CONFIGURATION_ERROR"
