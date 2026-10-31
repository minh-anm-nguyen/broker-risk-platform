"""The transactional outbox writer, used from inside P03/P04 transactions."""

from .outbox_writer import DealFact, OutboxWriter, WriteResult

__all__ = ["DealFact", "OutboxWriter", "WriteResult"]
