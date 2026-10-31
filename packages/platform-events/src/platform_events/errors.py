"""Error taxonomy (design section 7.4).

Every failure the relay or the consumer can see must map onto exactly one
disposition, because the disposition decides whether a committed financial
event is retried, parked for a human, or allowed to stop the pipeline. An
unclassified exception is itself a defect: ``classify`` fails closed by
returning ``QUARANTINE`` rather than guessing that a retry is safe.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass


class Disposition(enum.StrEnum):
    """What the relay does with a claimed row after an attempt."""

    RETRY = "RETRY"
    """Transient: keep the durable row, back off, try the same event_id again."""

    QUARANTINE = "QUARANTINE"
    """Permanent for this payload/config: park it, alert, never mutate it."""

    CIRCUIT_OPEN = "CIRCUIT_OPEN"
    """Systemic: stop claiming at all until a cooldown elapses."""

    BACKPRESSURE = "BACKPRESSURE"
    """Local resource exhaustion: stop claiming, drain callbacks, no data loss."""


class ErrorCode(enum.StrEnum):
    """Low-cardinality error classes. Safe as a metric label."""

    BROKER_UNREACHABLE = "BROKER_UNREACHABLE"
    BROKER_LEADER_UNAVAILABLE = "BROKER_LEADER_UNAVAILABLE"
    BROKER_TIMEOUT = "BROKER_TIMEOUT"
    BROKER_ACK_AMBIGUOUS = "BROKER_ACK_AMBIGUOUS"
    BROKER_NOT_ENOUGH_REPLICAS = "BROKER_NOT_ENOUGH_REPLICAS"
    AUTH_DENIED = "AUTH_DENIED"
    MESSAGE_TOO_LARGE = "MESSAGE_TOO_LARGE"
    SERIALIZATION_UNSUPPORTED = "SERIALIZATION_UNSUPPORTED"
    SCHEMA_UNSUPPORTED = "SCHEMA_UNSUPPORTED"
    PRODUCER_QUEUE_FULL = "PRODUCER_QUEUE_FULL"
    DB_UNAVAILABLE = "DB_UNAVAILABLE"
    EPOCH_BLOCKED = "EPOCH_BLOCKED"
    TENANT_FORBIDDEN = "TENANT_FORBIDDEN"
    INTEGRITY_CONFLICT = "INTEGRITY_CONFLICT"
    UNKNOWN = "UNKNOWN"


#: The classification table. Kept as data so tests can assert the whole map at
#: once, and so a new error code cannot be added without choosing a disposition.
DISPOSITIONS: dict[ErrorCode, Disposition] = {
    ErrorCode.BROKER_UNREACHABLE: Disposition.CIRCUIT_OPEN,
    ErrorCode.BROKER_LEADER_UNAVAILABLE: Disposition.RETRY,
    ErrorCode.BROKER_TIMEOUT: Disposition.RETRY,
    ErrorCode.BROKER_ACK_AMBIGUOUS: Disposition.RETRY,
    ErrorCode.BROKER_NOT_ENOUGH_REPLICAS: Disposition.RETRY,
    # Looping on a rejected ACL a thousand times a second is a denial of
    # service against our own broker, so auth failures stop the claim loop.
    ErrorCode.AUTH_DENIED: Disposition.CIRCUIT_OPEN,
    # Never auto-truncate a financial field to make it fit.
    ErrorCode.MESSAGE_TOO_LARGE: Disposition.QUARANTINE,
    ErrorCode.SERIALIZATION_UNSUPPORTED: Disposition.QUARANTINE,
    ErrorCode.SCHEMA_UNSUPPORTED: Disposition.QUARANTINE,
    ErrorCode.PRODUCER_QUEUE_FULL: Disposition.BACKPRESSURE,
    ErrorCode.DB_UNAVAILABLE: Disposition.RETRY,
    ErrorCode.EPOCH_BLOCKED: Disposition.QUARANTINE,
    ErrorCode.TENANT_FORBIDDEN: Disposition.QUARANTINE,
    ErrorCode.INTEGRITY_CONFLICT: Disposition.QUARANTINE,
    ErrorCode.UNKNOWN: Disposition.QUARANTINE,
}


class OutboxError(Exception):
    """Base class carrying a classified error code and optional identity context.

    ``context`` exists for one specific job: a record can fail validation
    *after* enough of it has been read to know which ordering stream it belongs
    to. A hash mismatch is the clearest case -- the source, epoch and aggregate
    are all legible, only the payload is untrustworthy. Carrying them out with
    the exception lets the consumer block that one aggregate instead of halting
    the whole partition, which is the difference between one account stopping
    and every account that happens to hash alongside it stopping too.

    It holds identifiers only. Never a payload, never a credential.
    """

    code: ErrorCode = ErrorCode.UNKNOWN

    def __init__(
        self,
        message: str,
        *,
        code: ErrorCode | None = None,
        context: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code
        self.context: dict[str, object] = dict(context or {})


class ConfigurationError(OutboxError):
    """Startup configuration is unusable. Fail loudly, do not run degraded."""


class ValidationError(OutboxError):
    """A payload or envelope violated the pinned schema contract."""

    code = ErrorCode.SCHEMA_UNSUPPORTED


class MoneyPolicyError(ValidationError):
    """A monetary value was a float, lost precision, or lacked a currency."""

    code = ErrorCode.SERIALIZATION_UNSUPPORTED


class IntegrityIncident(OutboxError):
    """Same semantic identity, different content. Never silently reconciled."""

    code = ErrorCode.INTEGRITY_CONFLICT


class EpochBlocked(OutboxError):
    """The source epoch is not accepting new event creation."""

    code = ErrorCode.EPOCH_BLOCKED


class TenantForbidden(OutboxError):
    """source_id is outside the allowlist, or the epoch/source mapping is wrong."""

    code = ErrorCode.TENANT_FORBIDDEN


class BrokerError(OutboxError):
    """A produce attempt failed. ``code`` decides retry versus quarantine."""


class ProducerQueueFull(BrokerError):
    code = ErrorCode.PRODUCER_QUEUE_FULL


class MessageTooLarge(BrokerError):
    code = ErrorCode.MESSAGE_TOO_LARGE


class StoreUnavailable(OutboxError):
    code = ErrorCode.DB_UNAVAILABLE


@dataclass(frozen=True, slots=True)
class Classification:
    code: ErrorCode
    disposition: Disposition
    retryable: bool


def classify(exc: BaseException) -> Classification:
    """Map an exception onto a code and a disposition, failing closed."""
    code = getattr(exc, "code", ErrorCode.UNKNOWN)
    if not isinstance(code, ErrorCode):
        code = ErrorCode.UNKNOWN
    disposition = DISPOSITIONS[code]
    return Classification(
        code=code,
        disposition=disposition,
        retryable=disposition in (Disposition.RETRY, Disposition.CIRCUIT_OPEN, Disposition.BACKPRESSURE),
    )
