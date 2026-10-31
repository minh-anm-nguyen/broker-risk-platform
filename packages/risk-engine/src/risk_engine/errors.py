"""Project 05 error taxonomy (design doc section 7.3).

Each error carries a stable ``code`` that Project 06 maps to HTTP without
inventing new semantics. Nothing here knows about HTTP.
"""

from __future__ import annotations

__all__ = [
    "RiskEngineError",
    "UnauthorizedScopeError",
    "EpochChangedError",
    "SourceNotActiveError",
    "UnpublishedScanError",
    "DealWatermarkUnprovenError",
    "ReconBlockingError",
    "IncompatibleCutError",
    "QuoteCutUnprovenError",
    "StaleMarkError",
    "FxRateMissingError",
    "MoneyFieldMissingError",
    "UnsupportedCalcModeError",
    "DealTypeUnclassifiedError",
    "InvalidDecimalError",
    "DataIntegrityConflictError",
    "EngineOverloadedError",
    "EngineDeadlineExceededError",
    "PolicyVersionChangedError",
    "ERROR_CODES",
]


class RiskEngineError(Exception):
    """Base class for every deterministic engine failure.

    ``retryable`` is advice for the caller, not a promise that a retry succeeds.
    """

    code = "ENGINE_ERROR"
    retryable = False

    def __init__(self, detail: str = "", *, reason_codes: tuple[str, ...] = ()) -> None:
        super().__init__(detail or self.code)
        self.detail = detail
        self.reason_codes = reason_codes or (self.code,)


class UnauthorizedScopeError(RiskEngineError):
    code = "UNAUTHORIZED_SCOPE"


class EpochChangedError(RiskEngineError):
    code = "EPOCH_CHANGED"
    retryable = True


class SourceNotActiveError(RiskEngineError):
    code = "SOURCE_NOT_ACTIVE"


class UnpublishedScanError(RiskEngineError):
    code = "P03_UNPUBLISHED"
    retryable = True


class DealWatermarkUnprovenError(RiskEngineError):
    code = "DEAL_WATERMARK_UNPROVEN"
    retryable = True


class ReconBlockingError(RiskEngineError):
    code = "RECON_BLOCKING"


class IncompatibleCutError(RiskEngineError):
    code = "INCOMPATIBLE_CUT"


class QuoteCutUnprovenError(RiskEngineError):
    code = "QUOTE_CUT_UNPROVEN"
    retryable = True


class StaleMarkError(RiskEngineError):
    code = "STALE_MARK"
    retryable = True


class FxRateMissingError(RiskEngineError):
    code = "FX_RATE_MISSING"


class MoneyFieldMissingError(RiskEngineError):
    code = "MONEY_FIELD_MISSING"


class UnsupportedCalcModeError(RiskEngineError):
    code = "UNSUPPORTED_CALC_MODE"


class DealTypeUnclassifiedError(RiskEngineError):
    code = "DEAL_TYPE_UNCLASSIFIED"


class InvalidDecimalError(RiskEngineError):
    code = "INVALID_DECIMAL"


class DataIntegrityConflictError(RiskEngineError):
    code = "DATA_INTEGRITY_CONFLICT"


class EngineOverloadedError(RiskEngineError):
    code = "OVERLOADED"
    retryable = True


class EngineDeadlineExceededError(RiskEngineError):
    code = "DEADLINE_EXCEEDED"
    retryable = True


class PolicyVersionChangedError(RiskEngineError):
    code = "POLICY_VERSION_CHANGED"
    retryable = True


ERROR_CODES: tuple[str, ...] = tuple(
    cls.code
    for cls in (
        UnauthorizedScopeError,
        EpochChangedError,
        SourceNotActiveError,
        UnpublishedScanError,
        DealWatermarkUnprovenError,
        ReconBlockingError,
        IncompatibleCutError,
        QuoteCutUnprovenError,
        StaleMarkError,
        FxRateMissingError,
        MoneyFieldMissingError,
        UnsupportedCalcModeError,
        DealTypeUnclassifiedError,
        InvalidDecimalError,
        DataIntegrityConflictError,
        EngineOverloadedError,
        EngineDeadlineExceededError,
        PolicyVersionChangedError,
    )
)
