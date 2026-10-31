"""The typed error taxonomy (design section 4.4).

``code`` is the stable part of this contract: a consumer branches on it to
decide whether to retry, re-sync or stop, so renaming one is a breaking
change. Every error answers four questions without the caller having to parse
a message:

* ``retryable`` - may the *same* operation be attempted again?
* ``resync_required`` - must the consumer abandon its scan and start over?
* ``safe_action`` - what the consumer should do, in words meant for a runbook.
* ``category`` - which family this belongs to, for metrics with bounded
  cardinality.

Two distinctions carry most of the weight:

* **Transport failure vs data failure.** A 503 is worth retrying; a page whose
  commission is NaN is not, and retrying it would turn a source bug into a
  retry storm while still never producing a usable page
  (:class:`SourceContractError`, WF2-H-05).
* **Timeout vs cancellation.** :class:`DeadlineExceededError` means the caller
  gave up; it does *not* mean the native call stopped. ``CancelledError`` is
  never translated into a connector error - it is the caller's lifecycle
  signal and is re-raised (design section 4.4).
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "ConnectorError",
    "AuthenticationError",
    "PermissionDeniedError",
    "ConfigurationError",
    "IncompatibleSdkError",
    "CapabilityUnsupportedError",
    "TransportError",
    "UpstreamUnavailableError",
    "RateLimitedError",
    "UpstreamTimeoutError",
    "DeadlineExceededError",
    "OverloadedError",
    "CircuitOpenError",
    "ConnectorClosedError",
    "SourceContractError",
    "DataIntegrityError",
    "SnapshotExpiredError",
    "SourceEpochChangedError",
    "ResyncRequiredError",
    "ERROR_CODES",
    "RESYNC_CODES",
]


class ConnectorError(Exception):
    """Base class for every failure this connector reports.

    Subclasses set the class-level policy flags. Instances carry only context,
    never a payload: an error object must be safe to log, so nothing that
    reaches it may contain a token, a URL with a query string, or a raw
    financial record (INV-15).
    """

    code: str = "CONNECTOR_ERROR"
    category: str = "unknown"
    retryable: bool = False
    resync_required: bool = False
    safe_action: str = "inspect connector logs"

    def __init__(
        self,
        message: str,
        *,
        source_id: str | None = None,
        operation: str | None = None,
        request_id: str | None = None,
        attempt: int | None = None,
        worker_epoch: int | None = None,
        source_generation: int | None = None,
        upstream_status: int | None = None,
        retry_after_s: float | None = None,
        latency_ms: float | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.source_id = source_id
        self.operation = operation
        self.request_id = request_id
        self.attempt = attempt
        self.worker_epoch = worker_epoch
        self.source_generation = source_generation
        self.upstream_status = upstream_status
        self.retry_after_s = retry_after_s
        self.latency_ms = latency_ms
        #: Non-sensitive metadata only. Callers add field names and counts
        #: here, never field *values* from a financial payload.
        self.details: dict[str, Any] = dict(details or {})

    def with_context(self, **context: Any) -> "ConnectorError":
        """Attach request context to an error raised deeper in the stack.

        Returns ``self`` so it can be used inline in a ``raise``. Only unset
        attributes are filled, so context added closer to the failure wins
        over context added by an outer frame.
        """
        for key, value in context.items():
            if value is None:
                continue
            if key == "details":
                self.details.update(value)
            elif getattr(self, key, None) is None:
                setattr(self, key, value)
        return self

    def to_envelope(self) -> dict[str, Any]:
        """The wire form handed to a consumer (design section 16.2)."""
        return {
            "source_id": self.source_id,
            "operation": self.operation,
            "error": {
                "code": self.code,
                "category": self.category,
                "retryable": self.retryable,
                "resync_required": self.resync_required,
                "safe_action": self.safe_action,
                "message": self.message,
            },
            "request_id": self.request_id,
            "attempt": self.attempt,
            "worker_epoch": self.worker_epoch,
            "source_generation": self.source_generation,
            "upstream_status": self.upstream_status,
            "retry_after_s": self.retry_after_s,
            "latency_ms": self.latency_ms,
            "details": self.details,
        }

    def __repr__(self) -> str:  # pragma: no cover - diagnostic only
        return f"{type(self).__name__}(code={self.code!r}, message={self.message!r})"


# ---------------------------------------------------------------------------
# Permanent failures - retrying cannot help
# ---------------------------------------------------------------------------


class AuthenticationError(ConnectorError):
    code = "AUTH_FAILED"
    category = "identity"
    safe_action = (
        "fix or rotate the source credential; the source stays DISABLED until configuration changes"
    )


class PermissionDeniedError(ConnectorError):
    code = "PERMISSION_DENIED"
    category = "identity"
    safe_action = "grant the source role read access to the requested entity; do not retry"


class ConfigurationError(ConnectorError):
    code = "CONFIGURATION_ERROR"
    category = "configuration"
    safe_action = "correct the connector configuration and restart; nothing was sent upstream"


class IncompatibleSdkError(ConnectorError):
    code = "INCOMPATIBLE_SDK"
    category = "configuration"
    safe_action = "disable this source; its driver or contract version is not supported by this build"


class CapabilityUnsupportedError(ConnectorError):
    code = "CAPABILITY_UNSUPPORTED"
    category = "configuration"
    safe_action = (
        "the source does not declare the capability this read plan needs; "
        "choose a strategy the source actually supports rather than assuming one"
    )


# ---------------------------------------------------------------------------
# Transient failures - retry inside the remaining budget
# ---------------------------------------------------------------------------


class TransportError(ConnectorError):
    code = "TRANSPORT_ERROR"
    category = "transport"
    retryable = True
    safe_action = "retry inside the remaining deadline with full-jitter backoff"


class UpstreamUnavailableError(ConnectorError):
    code = "UPSTREAM_UNAVAILABLE"
    category = "transport"
    retryable = True
    safe_action = "retry inside the remaining deadline; the breaker opens if this persists"


class RateLimitedError(ConnectorError):
    code = "RATE_LIMITED"
    category = "transport"
    retryable = True
    safe_action = "wait the capped Retry-After, then retry if budget remains"


class UpstreamTimeoutError(ConnectorError):
    code = "UPSTREAM_TIMEOUT"
    category = "transport"
    retryable = True
    safe_action = (
        "ambiguous for a read: the call may still be running upstream. "
        "Retrying is safe only because every data-plane operation is a GET"
    )


# ---------------------------------------------------------------------------
# Local refusals - the connector decided, not the source
# ---------------------------------------------------------------------------


class DeadlineExceededError(ConnectorError):
    code = "DEADLINE_EXCEEDED"
    category = "deadline"
    safe_action = (
        "no attempts remain inside the caller's deadline. "
        "A native call already dispatched keeps its session slot until it actually returns"
    )


class OverloadedError(ConnectorError):
    code = "OVERLOADED"
    category = "capacity"
    retryable = True
    safe_action = "the admission queue is at capacity; reduce caller concurrency or page size"


class CircuitOpenError(ConnectorError):
    code = "CIRCUIT_OPEN"
    category = "capacity"
    retryable = True
    safe_action = (
        "the connector is refusing work for this source locally - an open breaker or "
        "a quarantined session. Wait rather than retrying immediately; ``details.reason`` "
        "says which"
    )


class ConnectorClosedError(ConnectorError):
    code = "CONNECTOR_CLOSED"
    category = "lifecycle"
    safe_action = "this instance is draining; resubmit to a live instance"


# ---------------------------------------------------------------------------
# Data and consistency failures - quarantine, never normalise
# ---------------------------------------------------------------------------


class SourceContractError(ConnectorError):
    code = "SOURCE_CONTRACT_ERROR"
    category = "contract"
    safe_action = (
        "quarantine the page and compare against the source contract. "
        "Do not retry and do not repair the payload"
    )


class DataIntegrityError(ConnectorError):
    code = "DATA_INTEGRITY_ERROR"
    category = "data"
    resync_required = True
    safe_action = "quarantine the page and hand the anomaly to reconciliation (Project 04)"


class SnapshotExpiredError(ConnectorError):
    code = "SNAPSHOT_EXPIRED"
    category = "consistency"
    resync_required = True
    safe_action = (
        "the cursor's snapshot is gone. Restart the scan from the durable checkpoint; "
        "never reuse the expired cursor"
    )


class SourceEpochChangedError(ConnectorError):
    code = "SOURCE_EPOCH_CHANGED"
    category = "consistency"
    resync_required = True
    safe_action = "restart the scan from the durable checkpoint and reconcile source identity"


class ResyncRequiredError(ConnectorError):
    code = "RESYNC_REQUIRED"
    category = "consistency"
    resync_required = True
    safe_action = "stop advancing the checkpoint and run a fresh scan plus reconciliation"


#: Every code this build can emit, published so a consumer can enumerate what
#: it must handle instead of discovering codes in production.
_ERROR_CLASSES: tuple[type[ConnectorError], ...] = (
    AuthenticationError,
    PermissionDeniedError,
    ConfigurationError,
    IncompatibleSdkError,
    CapabilityUnsupportedError,
    TransportError,
    UpstreamUnavailableError,
    RateLimitedError,
    UpstreamTimeoutError,
    DeadlineExceededError,
    OverloadedError,
    CircuitOpenError,
    ConnectorClosedError,
    SourceContractError,
    DataIntegrityError,
    SnapshotExpiredError,
    SourceEpochChangedError,
    ResyncRequiredError,
)

ERROR_CODES: tuple[str, ...] = tuple(sorted(cls.code for cls in _ERROR_CLASSES))

#: Codes that oblige the consumer to abandon its scan. Derived from the class
#: flags rather than written out twice, so the two can never disagree.
RESYNC_CODES: frozenset[str] = frozenset(
    cls.code for cls in _ERROR_CLASSES if cls.resync_required
)
