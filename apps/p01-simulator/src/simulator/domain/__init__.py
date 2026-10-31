"""Domain layer: entities, Decimal accounting, the event state machine and the
runnable invariant suite.

Nothing in this package imports FastAPI, SQLite or the HTTP layer. That is
deliberate: the same code must be callable from the writer transaction, from a
pure reference model in property tests, and from the offline invariant script.
"""

from .models import (
    Account,
    ConflictError,
    Deal,
    DealEntry,
    DomainError,
    EventType,
    IdempotencyConflictError,
    InvalidCursorError,
    JobStatus,
    Mark,
    NotFoundError,
    Position,
    PositionStatus,
    RateLimitedError,
    RunReplacedError,
    RunStatus,
    ServiceNotReadyError,
    Side,
    SimulatorError,
    SnapshotExpiredError,
    StorageError,
    SymbolSpec,
    UnsupportedOperationError,
    ValidationError,
    WriterOverloadedError,
)

__all__ = [
    "Account",
    "ConflictError",
    "Deal",
    "DealEntry",
    "DomainError",
    "EventType",
    "IdempotencyConflictError",
    "InvalidCursorError",
    "JobStatus",
    "Mark",
    "NotFoundError",
    "Position",
    "PositionStatus",
    "RateLimitedError",
    "RunReplacedError",
    "RunStatus",
    "ServiceNotReadyError",
    "Side",
    "SimulatorError",
    "SnapshotExpiredError",
    "StorageError",
    "SymbolSpec",
    "UnsupportedOperationError",
    "ValidationError",
    "WriterOverloadedError",
]
