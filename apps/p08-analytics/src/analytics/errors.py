"""Status taxonomy and structured failures.

The design (sections 8.1, 8.4) insists that a caller can always tell *why* a
number is missing, because the alternative - returning ``READY`` with some
rows in it - is the failure mode this whole project exists to prevent. So the
status is a closed enum, every refusal carries a machine-readable reason, and
there is deliberately no "unknown/other" status that code could default to.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any


class ReportStatus(StrEnum):
    """Status of an analytics answer. Section 8.1.

    The distinction between these is not cosmetic: ``STALE`` means *verified
    but old*, ``INCOMPLETE`` means *we know data is missing*, and ``BLOCKED``
    means *we found a contradiction*. Collapsing them would let an operator
    treat an integrity incident as a freshness blip.
    """

    READY = "READY"
    STALE = "STALE"
    INCOMPLETE = "INCOMPLETE"
    BLOCKED = "BLOCKED"
    DEGRADED = "DEGRADED"
    UNSUPPORTED = "UNSUPPORTED"


class BuildState(StrEnum):
    """Lifecycle of one immutable build attempt. Section 3.4."""

    REQUESTED = "REQUESTED"
    BUILDING = "BUILDING"
    VALIDATING = "VALIDATING"
    VERIFIED = "VERIFIED"
    PUBLISHED = "PUBLISHED"
    SUPERSEDED = "SUPERSEDED"
    FAILED = "FAILED"
    QUARANTINED = "QUARANTINED"


#: Transitions the control plane will accept. Anything absent here is a caller
#: bug, not a state the registry should reach by accident (INV08-09).
ALLOWED_BUILD_TRANSITIONS: dict[BuildState, frozenset[BuildState]] = {
    BuildState.REQUESTED: frozenset({BuildState.BUILDING, BuildState.FAILED}),
    BuildState.BUILDING: frozenset(
        {BuildState.VALIDATING, BuildState.FAILED, BuildState.QUARANTINED}
    ),
    BuildState.VALIDATING: frozenset(
        {BuildState.VERIFIED, BuildState.FAILED, BuildState.QUARANTINED}
    ),
    BuildState.VERIFIED: frozenset(
        {BuildState.PUBLISHED, BuildState.FAILED, BuildState.QUARANTINED}
    ),
    BuildState.PUBLISHED: frozenset({BuildState.SUPERSEDED}),
    BuildState.SUPERSEDED: frozenset(),
    BuildState.FAILED: frozenset(),
    BuildState.QUARANTINED: frozenset(),
}

#: States from which a build may never again be written to or published.
TERMINAL_BUILD_STATES: frozenset[BuildState] = frozenset(
    {BuildState.SUPERSEDED, BuildState.FAILED, BuildState.QUARANTINED}
)


class Reason(StrEnum):
    """Machine-readable refusal reasons.

    These strings land in evidence bundles and API responses, so they are part
    of the contract and may not be reworded casually.
    """

    # Contract / envelope
    SCHEMA_UNKNOWN = "SCHEMA_UNKNOWN"
    SCHEMA_VERSION_UNSUPPORTED = "SCHEMA_VERSION_UNSUPPORTED"
    ENVELOPE_MALFORMED = "ENVELOPE_MALFORMED"
    ENVELOPE_TOO_LARGE = "ENVELOPE_TOO_LARGE"
    FIELD_MISSING = "FIELD_MISSING"
    FIELD_INVALID = "FIELD_INVALID"
    HASH_MISMATCH = "HASH_MISMATCH"
    HASH_MALFORMED = "HASH_MALFORMED"
    TENANT_UNMAPPED = "TENANT_UNMAPPED"
    TOPIC_NOT_ALLOWED = "TOPIC_NOT_ALLOWED"
    EVENT_TYPE_NOT_ALLOWED = "EVENT_TYPE_NOT_ALLOWED"

    # Money / policy
    MONEY_MALFORMED = "MONEY_MALFORMED"
    MONEY_SCALE_EXCEEDED = "MONEY_SCALE_EXCEEDED"
    MONEY_OUT_OF_BOUNDS = "MONEY_OUT_OF_BOUNDS"
    MONEY_NOT_A_STRING = "MONEY_NOT_A_STRING"
    CURRENCY_MISSING = "CURRENCY_MISSING"
    CURRENCY_UNSUPPORTED = "CURRENCY_UNSUPPORTED"
    CURRENCY_MIXED = "CURRENCY_MIXED"
    FEE_COVERAGE_UNKNOWN = "FEE_COVERAGE_UNKNOWN"
    POLICY_VERSION_UNKNOWN = "POLICY_VERSION_UNKNOWN"
    SYMBOL_UNKNOWN = "SYMBOL_UNKNOWN"
    ENTRY_KIND_UNKNOWN = "ENTRY_KIND_UNKNOWN"
    SIDE_UNKNOWN = "SIDE_UNKNOWN"
    FX_CONVERSION_UNSUPPORTED = "FX_CONVERSION_UNSUPPORTED"

    # Identity / conflict
    EVENT_ID_HASH_CONFLICT = "EVENT_ID_HASH_CONFLICT"
    DEAL_SEMANTIC_CONFLICT = "DEAL_SEMANTIC_CONFLICT"
    EVENT_ID_SCOPE_COLLISION = "EVENT_ID_SCOPE_COLLISION"
    CORRECTION_UNSUPPORTED = "CORRECTION_UNSUPPORTED"

    # Completeness / source trust
    SOURCE_MANIFEST_MISSING = "SOURCE_MANIFEST_MISSING"
    SOURCE_DEALS_MISSING = "SOURCE_DEALS_MISSING"
    SOURCE_DEALS_UNEXPECTED = "SOURCE_DEALS_UNEXPECTED"
    SOURCE_HASH_MISMATCH = "SOURCE_HASH_MISMATCH"
    RECONCILIATION_NOT_VERIFIED = "RECONCILIATION_NOT_VERIFIED"
    RECONCILIATION_REVOKED = "RECONCILIATION_REVOKED"
    QUARANTINE_PENDING_IN_CUT = "QUARANTINE_PENDING_IN_CUT"
    RETENTION_GAP = "RETENTION_GAP"
    SNAPSHOT_CONTRACT_MISSING = "SNAPSHOT_CONTRACT_MISSING"

    # Build / publication
    LEASE_LOST = "LEASE_LOST"
    LEASE_HELD_BY_OTHER = "LEASE_HELD_BY_OTHER"
    BUILD_STATE_INVALID = "BUILD_STATE_INVALID"
    BUILD_NOT_VERIFIED = "BUILD_NOT_VERIFIED"
    BUILD_ID_REUSED = "BUILD_ID_REUSED"
    BUILD_NOT_FOUND = "BUILD_NOT_FOUND"
    EVIDENCE_MISSING = "EVIDENCE_MISSING"
    PUBLICATION_REVISION_STALE = "PUBLICATION_REVISION_STALE"
    READ_VISIBILITY_UNPROVEN = "READ_VISIBILITY_UNPROVEN"
    EPOCH_CHANGED = "EPOCH_CHANGED"
    CUT_CHANGED = "CUT_CHANGED"
    GC_PROTECTED = "GC_PROTECTED"
    REPORT_FAMILY_UNKNOWN = "REPORT_FAMILY_UNKNOWN"

    # Oracle
    ORACLE_COUNT_MISMATCH = "ORACLE_COUNT_MISMATCH"
    ORACLE_IDENTITY_MISMATCH = "ORACLE_IDENTITY_MISMATCH"
    ORACLE_MONEY_MISMATCH = "ORACLE_MONEY_MISMATCH"
    ORACLE_NOT_INDEPENDENT = "ORACLE_NOT_INDEPENDENT"
    ROLLUP_SET_MISMATCH = "ROLLUP_SET_MISMATCH"
    CANONICAL_DIGEST_MISMATCH = "CANONICAL_DIGEST_MISMATCH"

    # Infrastructure / resources
    INSERT_AMBIGUOUS = "INSERT_AMBIGUOUS"
    INSERT_REJECTED = "INSERT_REJECTED"
    STORAGE_FULL = "STORAGE_FULL"
    PARTITION_REVOKED = "PARTITION_REVOKED"
    PARTITION_PAUSED = "PARTITION_PAUSED"
    OFFSET_OUT_OF_RANGE = "OFFSET_OUT_OF_RANGE"
    OFFSET_INVALID = "OFFSET_INVALID"
    BUDGET_EXCEEDED = "BUDGET_EXCEEDED"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    OVERLOADED = "OVERLOADED"
    BACKEND_UNAVAILABLE = "BACKEND_UNAVAILABLE"

    # Access
    UNAUTHENTICATED = "UNAUTHENTICATED"
    FORBIDDEN_SCOPE = "FORBIDDEN_SCOPE"
    AUDIT_UNAVAILABLE = "AUDIT_UNAVAILABLE"

    # Query
    CURSOR_INVALID = "CURSOR_INVALID"
    CURSOR_BUILD_MISMATCH = "CURSOR_BUILD_MISMATCH"
    RANGE_TOO_LARGE = "RANGE_TOO_LARGE"
    RANGE_INVALID = "RANGE_INVALID"
    SORT_KEY_NOT_ALLOWED = "SORT_KEY_NOT_ALLOWED"
    METRIC_UNSUPPORTED = "METRIC_UNSUPPORTED"
    NO_PUBLICATION = "NO_PUBLICATION"


class AnalyticsError(Exception):
    """Base error carrying a :class:`Reason` and redaction-safe details."""

    status_code = 500

    def __init__(self, reason: Reason, message: str = "", **details: Any) -> None:
        self.reason = reason
        self.details = details
        super().__init__(message or str(reason))

    def to_dict(self) -> dict[str, Any]:
        return {"reason": str(self.reason), "message": str(self), "details": self.details}


class ConfigurationError(AnalyticsError):
    """Unsafe or internally inconsistent configuration; always fatal at boot."""

    def __init__(self, message: str, **details: Any) -> None:
        super().__init__(Reason.FIELD_INVALID, message, **details)


class ContractError(AnalyticsError):
    """The event or manifest does not satisfy the agreed cross-project contract."""

    status_code = 422


class PoisonError(ContractError):
    """A record can never become valid by retrying; it must be quarantined.

    Separated from :class:`RetriableError` because the two demand opposite
    handling: poison must be durably recorded before the stream advances,
    while a retriable fault must not advance the stream at all (section 6.1).
    """


class RetriableError(AnalyticsError):
    """A transient fault. The caller must not treat the record as consumed."""

    status_code = 503


class AmbiguousWriteError(RetriableError):
    """A write may or may not have been persisted. Section 3.2.

    This is the most dangerous state in the pipeline, so it gets its own type:
    the only safe response is to replay and let semantic dedup absorb the
    duplicate, never to commit an offset (ADR08-09).
    """

    def __init__(self, message: str = "insert acknowledgement is unproven", **details: Any) -> None:
        super().__init__(Reason.INSERT_AMBIGUOUS, message, **details)


class FencedError(AnalyticsError):
    """The caller lost its lease or assignment and may not write. INV08-11."""

    status_code = 409


class IntegrityError(AnalyticsError):
    """Two observations of one identity disagree. Publication must stop. INV08-02."""

    status_code = 409


class CompletenessError(AnalyticsError):
    """Required evidence for a cut is missing; ``READY`` is not available. INV08-05."""

    status_code = 409


class AuthorizationError(AnalyticsError):
    status_code = 403


class AuthenticationError(AnalyticsError):
    status_code = 401


class QueryError(AnalyticsError):
    status_code = 400


class OverloadError(RetriableError):
    status_code = 429

    def __init__(self, message: str = "bounded capacity exhausted", **details: Any) -> None:
        super().__init__(Reason.OVERLOADED, message, **details)
