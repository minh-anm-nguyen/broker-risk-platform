"""Reason-code taxonomy and the exception hierarchy of the risk engine.

Section 7.3 of the design document fixes the externally visible error codes. They
are reproduced here verbatim so that the service, the gates and the P06 contract
all speak one vocabulary; a code that is not in :class:`ReasonCode` can never
reach a caller.

Two rules shape this module:

* Every non-``READY`` outcome carries a reason code (INV: WF5-283). A bare
  exception without a code is a bug, so :class:`RiskError` requires one.
* A reason code is data, never a free-form message. Messages are for humans and
  may be redacted; codes are what tests and dashboards assert on.
"""

from __future__ import annotations

from enum import StrEnum, unique
from typing import Any

__all__ = [
    "ReasonCode",
    "RETRYABLE_CODES",
    "RiskError",
    "ContractError",
    "AuthorizationError",
    "QualityGateError",
    "UnsupportedError",
    "ResourceError",
    "AuditError",
]


@unique
class ReasonCode(StrEnum):
    """Closed set of machine-readable outcomes."""

    # --- authorization / scope -------------------------------------------------
    UNAUTHORIZED_SCOPE = "UNAUTHORIZED_SCOPE"

    # --- source identity / epoch ----------------------------------------------
    EPOCH_CHANGED = "EPOCH_CHANGED"
    SOURCE_NOT_ACTIVE = "SOURCE_NOT_ACTIVE"
    EPOCH_IDENTITY_CONFLICT = "EPOCH_IDENTITY_CONFLICT"

    # --- upstream publication / completeness ----------------------------------
    P03_UNPUBLISHED = "P03_UNPUBLISHED"
    DEAL_WATERMARK_UNPROVEN = "DEAL_WATERMARK_UNPROVEN"
    SCAN_PROVENANCE_VIOLATION = "SCAN_PROVENANCE_VIOLATION"
    CONTRACT_MISSING_FIELD = "CONTRACT_MISSING_FIELD"
    UNSUPPORTED_SOURCE_COMPLETENESS = "UNSUPPORTED_SOURCE_COMPLETENESS"
    ENUM_VERSION_UNMAPPED = "ENUM_VERSION_UNMAPPED"

    # --- reconciliation evidence ----------------------------------------------
    RECON_BLOCKING = "RECON_BLOCKING"
    RECON_INCONCLUSIVE = "RECON_INCONCLUSIVE"
    RECON_EVIDENCE_SCOPE_UNKNOWN = "RECON_EVIDENCE_SCOPE_UNKNOWN"
    RECON_EVIDENCE_STALE = "RECON_EVIDENCE_STALE"

    # --- cut / lineage --------------------------------------------------------
    INCOMPATIBLE_CUT = "INCOMPATIBLE_CUT"
    QUOTE_CUT_UNPROVEN = "QUOTE_CUT_UNPROVEN"
    HISTORICAL_CUT_UNPROVEN = "HISTORICAL_CUT_UNPROVEN"
    SNAPSHOT_EXPIRED = "SNAPSHOT_EXPIRED"

    # --- market data ----------------------------------------------------------
    STALE_MARK = "STALE_MARK"
    INVALID_QUOTE = "INVALID_QUOTE"
    QUOTE_MISSING = "QUOTE_MISSING"
    QUOTE_IMMUTABILITY_CONFLICT = "QUOTE_IMMUTABILITY_CONFLICT"
    QUOTE_SYMBOL_MISMATCH = "QUOTE_SYMBOL_MISMATCH"
    QUOTE_LOOKAHEAD = "QUOTE_LOOKAHEAD"
    QUOTE_SOURCE_RESTRICTED = "QUOTE_SOURCE_RESTRICTED"

    # --- FX -------------------------------------------------------------------
    FX_RATE_MISSING = "FX_RATE_MISSING"
    FX_SPREAD_INVALID = "FX_SPREAD_INVALID"
    FX_RATE_INVALID = "FX_RATE_INVALID"
    FX_PATH_CYCLE = "FX_PATH_CYCLE"
    FX_PATH_TOO_LONG = "FX_PATH_TOO_LONG"
    FX_PROVIDER_CONFLICT = "FX_PROVIDER_CONFLICT"
    FX_RATE_SET_INCOHERENT = "FX_RATE_SET_INCOHERENT"

    # --- money / numeric ------------------------------------------------------
    MONEY_FIELD_MISSING = "MONEY_FIELD_MISSING"
    INVALID_DECIMAL = "INVALID_DECIMAL"
    DECIMAL_STRING_REQUIRED = "DECIMAL_STRING_REQUIRED"
    NONFINITE_MONEY = "NONFINITE_MONEY"
    DECIMAL_SCALE_EXCEEDED = "DECIMAL_SCALE_EXCEEDED"
    DECIMAL_MAGNITUDE_EXCEEDED = "DECIMAL_MAGNITUDE_EXCEEDED"
    CURRENCY_MISMATCH = "CURRENCY_MISMATCH"
    UNIT_MISMATCH = "UNIT_MISMATCH"
    INVALID_CURRENCY = "INVALID_CURRENCY"
    INVALID_ACCOUNT_CURRENCY = "INVALID_ACCOUNT_CURRENCY"

    # --- instrument / calc mode ----------------------------------------------
    UNSUPPORTED_CALC_MODE = "UNSUPPORTED_CALC_MODE"
    UNSUPPORTED_SYMBOL = "UNSUPPORTED_SYMBOL"
    INVALID_INSTRUMENT_SPEC = "INVALID_INSTRUMENT_SPEC"
    SPEC_VERSION_MISMATCH = "SPEC_VERSION_MISMATCH"
    VOLUME_STEP_VIOLATION = "VOLUME_STEP_VIOLATION"
    PRICE_OFF_TICK = "PRICE_OFF_TICK"

    # --- position / deal facts ------------------------------------------------
    INVALID_POSITION_VOLUME = "INVALID_POSITION_VOLUME"
    INVALID_POSITION_PRICE = "INVALID_POSITION_PRICE"
    UNSUPPORTED_POSITION_SIDE = "UNSUPPORTED_POSITION_SIDE"
    UNSUPPORTED_POSITION_STATUS = "UNSUPPORTED_POSITION_STATUS"
    VOLUME_LIMIT_EXCEEDED = "VOLUME_LIMIT_EXCEEDED"
    DEAL_TYPE_UNCLASSIFIED = "DEAL_TYPE_UNCLASSIFIED"
    DEAL_CORRECTION_UNSUPPORTED = "DEAL_CORRECTION_UNSUPPORTED"
    DATA_INTEGRITY_CONFLICT = "DATA_INTEGRITY_CONFLICT"

    # --- time -----------------------------------------------------------------
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    NAIVE_TIMESTAMP_REJECTED = "NAIVE_TIMESTAMP_REJECTED"
    INVALID_TIME_WINDOW = "INVALID_TIME_WINDOW"

    # --- coverage disclosure --------------------------------------------------
    FEE_COVERAGE_UNAVAILABLE = "FEE_COVERAGE_UNAVAILABLE"

    # --- runtime / resources --------------------------------------------------
    OVERLOADED = "OVERLOADED"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    CANCELLED = "CANCELLED"
    INPUT_TOO_LARGE = "INPUT_TOO_LARGE"
    LIMIT_EXCEEDED = "LIMIT_EXCEEDED"

    # --- infrastructure -------------------------------------------------------
    DB_UNAVAILABLE = "DB_UNAVAILABLE"
    AUDIT_PERSISTENCE_FAILED = "AUDIT_PERSISTENCE_FAILED"
    POLICY_VERSION_CHANGED = "POLICY_VERSION_CHANGED"
    POLICY_UNAVAILABLE = "POLICY_UNAVAILABLE"


#: Codes for which a bounded retry by the caller may succeed without operator
#: action. ``UNAUTHORIZED_SCOPE`` is deliberately absent: blind retry of a
#: rejected authorization is an attack pattern, not a recovery strategy.
RETRYABLE_CODES: frozenset[ReasonCode] = frozenset(
    {
        ReasonCode.EPOCH_CHANGED,
        ReasonCode.OVERLOADED,
        ReasonCode.DEADLINE_EXCEEDED,
        ReasonCode.DB_UNAVAILABLE,
        ReasonCode.AUDIT_PERSISTENCE_FAILED,
        ReasonCode.POLICY_VERSION_CHANGED,
    }
)


class RiskError(Exception):
    """Base class for every controlled failure inside the engine.

    ``detail`` holds low-cardinality structured context (field names, symbols,
    counts). It must never carry money amounts, balances or raw account
    identifiers, because error paths are the ones most likely to be logged
    without redaction (WF5-281).
    """

    reason: ReasonCode

    def __init__(self, reason: ReasonCode, message: str = "", **detail: Any) -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(message or str(reason))

    @property
    def retryable(self) -> bool:
        return self.reason in RETRYABLE_CODES

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"{type(self).__name__}({self.reason!s}, detail={self.detail!r})"


class ContractError(RiskError):
    """Upstream data or schema violates the agreed contract."""


class AuthorizationError(RiskError):
    """Caller is not entitled to the requested scope."""


class QualityGateError(RiskError):
    """Inputs exist but their quality/lineage does not permit a result."""


class UnsupportedError(RiskError):
    """Input is well-formed but outside the declared supported profile."""


class ResourceError(RiskError):
    """Workload exceeds an admission, memory or deadline budget."""


class AuditError(RiskError):
    """The mandatory audit record could not be made durable."""
