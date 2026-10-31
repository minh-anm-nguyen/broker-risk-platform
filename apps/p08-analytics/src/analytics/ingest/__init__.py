"""Kafka to ClickHouse ingestion.

The ordering rule of section 6.1 is the whole contract:

    validate -> durably quarantine any poison -> insert raw -> *then* commit
    the highest contiguous eligible offset

Reversing the last two steps produces silent data loss (P08-E-04), and
committing past an unresolved record produces the same loss one offset at a
time (P08-E-05). Everything in this package exists to make that order
structural rather than a comment.
"""

from .consumer import BatchOutcome, IngestWorker, PartitionProgress
from .quarantine import QuarantineLedger
from .validator import ValidatedBatch, ValidationFailure, validate_records

__all__ = [
    "BatchOutcome",
    "IngestWorker",
    "PartitionProgress",
    "QuarantineLedger",
    "ValidatedBatch",
    "ValidationFailure",
    "validate_records",
]
