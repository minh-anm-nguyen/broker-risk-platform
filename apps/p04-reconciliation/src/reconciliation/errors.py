"""Reason codes, finding codes and the exception hierarchy.

Design rules enforced here (SYSTEM_DESIGN sections 4.3, 5.5, 5.6, 8.1):

* Every non-``MATCHED`` outcome carries a machine-readable ``reason_code``.
  "It failed" is not an outcome a runbook can act on (FR04-13).
* ``INCONCLUSIVE`` is a first-class result, never an alias for ``MATCHED``.
  The single most dangerous bug this service can have is reporting agreement
  it did not prove (INV04-01).
* The set of repair actions is a closed allowlist. Anything that overwrites
  money or deletes history is absent from the enum entirely, so it cannot be
  constructed even by a caller that wants to (section 8.1, INV04-13).
"""

from __future__ import annotations

from enum import StrEnum


class ReasonCode(StrEnum):
    """Why a run or plan reached a terminal state.

    Grouped by the failure taxonomy in section 5.6. The string values are a
    stable contract: they appear in persisted rows, logs, metric labels and
    runbooks, so renaming one is a breaking change.
    """

    # ---- Source-side evidence problems (INCONCLUSIVE_SOURCE) -------------
    SNAPSHOT_EXPIRED = "SNAPSHOT_EXPIRED"
    SOURCE_PAGE_MISSING = "SOURCE_PAGE_MISSING"
    SOURCE_CURSOR_CYCLE = "SOURCE_CURSOR_CYCLE"
    SOURCE_CUT_DRIFTED = "SOURCE_CUT_DRIFTED"
    SOURCE_TRANSPORT_FAILED = "SOURCE_TRANSPORT_FAILED"
    SOURCE_PAYLOAD_INVALID = "SOURCE_PAYLOAD_INVALID"
    SOURCE_DUPLICATE_KEY = "SOURCE_DUPLICATE_KEY"
    SOURCE_ORDER_VIOLATED = "SOURCE_ORDER_VIOLATED"
    SOURCE_SEQ_AFTER_SNAPSHOT = "SOURCE_SEQ_AFTER_SNAPSHOT"
    SOURCE_PAGE_OVERSIZED = "SOURCE_PAGE_OVERSIZED"
    SOURCE_SCAN_FILTERED = "SOURCE_SCAN_FILTERED"
    SOURCE_INCOMPLETE = "SOURCE_INCOMPLETE"
    SOURCE_BUDGET_EXHAUSTED = "SOURCE_BUDGET_EXHAUSTED"

    # ---- Target-side evidence problems (INCONCLUSIVE_TARGET) -------------
    TARGET_CAPTURE_CONFLICT = "TARGET_CAPTURE_CONFLICT"
    TARGET_DB_ERROR = "TARGET_DB_ERROR"
    TARGET_INCOMPLETE = "TARGET_INCOMPLETE"
    TARGET_NO_PUBLISHED_SCAN = "TARGET_NO_PUBLISHED_SCAN"

    # ---- Alignment / capability ------------------------------------------
    DIFFERENT_CUT = "DIFFERENT_CUT"
    TARGET_NOT_CAUGHT_UP = "TARGET_NOT_CAUGHT_UP"
    EPOCH_MISMATCH = "EPOCH_MISMATCH"
    SOURCE_IDENTITY_CHANGED = "SOURCE_IDENTITY_CHANGED"
    INCOMPARABLE_SCHEMA = "INCOMPARABLE_SCHEMA"
    UNSUPPORTED_CAPABILITY = "UNSUPPORTED_CAPABILITY"
    P03_INVARIANT_BROKEN = "P03_INVARIANT_BROKEN"

    # ---- Policy / authorization ------------------------------------------
    POLICY_BLOCK = "POLICY_BLOCK"
    MASS_DRIFT_ESCALATION = "MASS_DRIFT_ESCALATION"
    EVIDENCE_STALE = "EVIDENCE_STALE"
    APPROVAL_MISSING = "APPROVAL_MISSING"
    APPROVAL_EXPIRED = "APPROVAL_EXPIRED"
    APPROVAL_SELF = "APPROVAL_SELF"
    APPROVAL_ROLE_INVALID = "APPROVAL_ROLE_INVALID"
    PLAN_DIGEST_CHANGED = "PLAN_DIGEST_CHANGED"
    FORBIDDEN_ACTION = "FORBIDDEN_ACTION"

    # ---- Execution / fencing ---------------------------------------------
    LEASE_LOST = "LEASE_LOST"
    PLAN_STALE = "PLAN_STALE"
    REPAIR_UNKNOWN = "REPAIR_UNKNOWN"
    REPAIR_REJECTED = "REPAIR_REJECTED"
    REPAIR_FAILED = "REPAIR_FAILED"
    VERIFICATION_FAILED = "VERIFICATION_FAILED"

    # ---- Admission / resource bounds (section 8.1, FR04-12) --------------
    ADMISSION_REJECTED = "ADMISSION_REJECTED"
    RUN_DEADLINE_EXCEEDED = "RUN_DEADLINE_EXCEEDED"
    SCAN_QUOTA_EXCEEDED = "SCAN_QUOTA_EXCEEDED"

    # ---- Lifecycle --------------------------------------------------------
    CANCELLED = "CANCELLED"
    INTERNAL_ERROR = "INTERNAL_ERROR"


#: Reason codes that mean "we could not gather enough evidence". A run holding
#: one of these must land in ``INCONCLUSIVE`` and must never produce a diff
#: finding with ``PROVEN`` confidence (INV04-01, INV04-04, INV04-14).
INCONCLUSIVE_REASONS: frozenset[ReasonCode] = frozenset(
    {
        ReasonCode.SNAPSHOT_EXPIRED,
        ReasonCode.SOURCE_PAGE_MISSING,
        ReasonCode.SOURCE_CURSOR_CYCLE,
        ReasonCode.SOURCE_CUT_DRIFTED,
        ReasonCode.SOURCE_TRANSPORT_FAILED,
        ReasonCode.SOURCE_PAYLOAD_INVALID,
        ReasonCode.SOURCE_DUPLICATE_KEY,
        ReasonCode.SOURCE_ORDER_VIOLATED,
        ReasonCode.SOURCE_SEQ_AFTER_SNAPSHOT,
        ReasonCode.SOURCE_PAGE_OVERSIZED,
        ReasonCode.SOURCE_SCAN_FILTERED,
        ReasonCode.SOURCE_INCOMPLETE,
        ReasonCode.SOURCE_BUDGET_EXHAUSTED,
        ReasonCode.TARGET_CAPTURE_CONFLICT,
        ReasonCode.TARGET_DB_ERROR,
        ReasonCode.TARGET_INCOMPLETE,
        ReasonCode.TARGET_NO_PUBLISHED_SCAN,
        ReasonCode.DIFFERENT_CUT,
        ReasonCode.TARGET_NOT_CAUGHT_UP,
        ReasonCode.EPOCH_MISMATCH,
        ReasonCode.SOURCE_IDENTITY_CHANGED,
        ReasonCode.INCOMPARABLE_SCHEMA,
        ReasonCode.UNSUPPORTED_CAPABILITY,
        ReasonCode.RUN_DEADLINE_EXCEEDED,
        ReasonCode.SCAN_QUOTA_EXCEEDED,
    }
)


class FindingCode(StrEnum):
    """Classification of one observed difference (section 4.3).

    ``ABSENT_*`` is deliberately distinct from any "deleted" or "closed"
    notion: absence from a listing is an *observation*, and only a source that
    states ``status=CLOSED`` closes a position (section 5.3, F-05/F-06).
    """

    MISSING_TARGET_DEAL = "MISSING_TARGET_DEAL"
    EXTRA_TARGET_DEAL = "EXTRA_TARGET_DEAL"
    IMMUTABLE_DEAL_CONFLICT = "IMMUTABLE_DEAL_CONFLICT"
    ACCOUNT_FIELD_MISMATCH = "ACCOUNT_FIELD_MISMATCH"
    POSITION_FIELD_MISMATCH = "POSITION_FIELD_MISMATCH"
    MISSING_TARGET_ACCOUNT = "MISSING_TARGET_ACCOUNT"
    MISSING_TARGET_POSITION = "MISSING_TARGET_POSITION"
    ABSENT_AT_SOURCE_ACCOUNT = "ABSENT_AT_SOURCE_ACCOUNT"
    ABSENT_AT_SOURCE_POSITION = "ABSENT_AT_SOURCE_POSITION"
    DIFFERENT_CUT = "DIFFERENT_CUT"
    PROVISIONAL_CROSS_ENTITY = "PROVISIONAL_CROSS_ENTITY"
    EPOCH_MISMATCH = "EPOCH_MISMATCH"
    INCONCLUSIVE_SOURCE = "INCONCLUSIVE_SOURCE"
    LARGE_SOURCE_COUNT_DROP = "LARGE_SOURCE_COUNT_DROP"


#: Findings whose subject is an immutable financial record. Policy must never
#: build an automatic corrective action for these (section 8.1, INV04-13).
MONEY_CONFLICT_FINDINGS: frozenset[FindingCode] = frozenset(
    {FindingCode.IMMUTABLE_DEAL_CONFLICT, FindingCode.EXTRA_TARGET_DEAL}
)


class Severity(StrEnum):
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class Confidence(StrEnum):
    """How much the evidence actually supports the finding (section 6.2).

    ``PROVEN`` requires every comparison precondition in section 5.1 to hold.
    ``PROVISIONAL`` means the observation is real but its interpretation
    depends on a capability we do not have (for example cross-entity checks
    without a global cut, section 5.4). ``INCONCLUSIVE`` records that we
    looked and could not tell.
    """

    PROVEN = "PROVEN"
    PROVISIONAL = "PROVISIONAL"
    INCONCLUSIVE = "INCONCLUSIVE"


class ActionType(StrEnum):
    """The closed allowlist of repair actions (sections 6.2, 8.1).

    There is intentionally no ``UPDATE_DEAL``, ``DELETE_DEAL`` or
    ``SET_ACCOUNT_BALANCE`` member. P04 orchestrates P03; it is not a second
    writer for synced tables (section 0.2 decision 3, INV04-07).
    """

    REQUEST_FULL_ACCOUNT_RESYNC = "REQUEST_FULL_ACCOUNT_RESYNC"
    REQUEST_FULL_POSITION_RESYNC = "REQUEST_FULL_POSITION_RESYNC"
    REQUEST_DEAL_BACKFILL = "REQUEST_DEAL_BACKFILL"
    MANUAL_INVESTIGATION = "MANUAL_INVESTIGATION"


#: Actions that change nothing on their own and so need no fencing guard, but
#: still require a persisted plan so the operator decision is auditable.
NON_DISPATCHING_ACTIONS: frozenset[ActionType] = frozenset({ActionType.MANUAL_INVESTIGATION})

#: Action types that require manual approval even when policy would otherwise
#: auto-draft them. Deal backfill touches immutable history (section 8.1, P1).
HIGH_RISK_ACTIONS: frozenset[ActionType] = frozenset({ActionType.REQUEST_DEAL_BACKFILL})


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class ReconError(RuntimeError):
    """Base class. Always carries a reason code so callers never guess."""

    reason: ReasonCode = ReasonCode.INTERNAL_ERROR

    def __init__(self, message: str, *, reason: ReasonCode | None = None, **details: object) -> None:
        super().__init__(message)
        if reason is not None:
            self.reason = reason
        self.details = details

    def __str__(self) -> str:  # pragma: no cover - diagnostic sugar
        base = RuntimeError.__str__(self)
        if not self.details:
            return f"[{self.reason}] {base}"
        rendered = ", ".join(f"{key}={value!r}" for key, value in sorted(self.details.items()))
        return f"[{self.reason}] {base} ({rendered})"


class ConfigurationError(ReconError):
    """Unsafe or internally inconsistent configuration. Raised at startup."""


class SourceError(ReconError):
    """Any failure attributable to the source boundary (section 3.2)."""

    reason = ReasonCode.SOURCE_TRANSPORT_FAILED


class SnapshotExpiredError(SourceError):
    reason = ReasonCode.SNAPSHOT_EXPIRED


class SourceEpochChangedError(SourceError):
    reason = ReasonCode.EPOCH_MISMATCH


class SourceIdentityError(SourceError):
    reason = ReasonCode.SOURCE_IDENTITY_CHANGED


class DataIntegrityError(SourceError):
    """A page was syntactically fine but violated the source contract."""

    reason = ReasonCode.SOURCE_PAYLOAD_INVALID


class CanonicalizationError(ReconError):
    """A value cannot be represented under the declared canonical policy."""

    reason = ReasonCode.INCOMPARABLE_SCHEMA


class TargetCaptureError(ReconError):
    reason = ReasonCode.TARGET_DB_ERROR


class TargetConflictError(TargetCaptureError):
    """The target changed under us; the capture would have been a hybrid."""

    reason = ReasonCode.TARGET_CAPTURE_CONFLICT


class AlignmentError(ReconError):
    """Comparison preconditions (section 5.1) are not satisfied."""

    reason = ReasonCode.DIFFERENT_CUT


class PolicyViolation(ReconError):
    reason = ReasonCode.POLICY_BLOCK


class ForbiddenActionError(PolicyViolation):
    reason = ReasonCode.FORBIDDEN_ACTION


class ApprovalError(PolicyViolation):
    reason = ReasonCode.APPROVAL_MISSING


class LeaseLostError(ReconError):
    reason = ReasonCode.LEASE_LOST


class PlanStaleError(ReconError):
    reason = ReasonCode.PLAN_STALE


class RepairUnknownError(ReconError):
    """Dispatch outcome is genuinely unknown; never retry blindly."""

    reason = ReasonCode.REPAIR_UNKNOWN


class AdmissionRejected(ReconError):
    reason = ReasonCode.ADMISSION_REJECTED


class PrivilegeError(ReconError):
    """P04 attempted a write it must never be able to perform (INV04-07)."""

    reason = ReasonCode.POLICY_BLOCK
