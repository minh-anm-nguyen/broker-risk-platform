"""Adapters implementing the ports.

Each port has a deterministic in-process implementation used by the whole test
suite and a thin production implementation that speaks to the real system.
Both satisfy the same protocol, so the logic under test is the logic that
ships; what changes is whether a fault is scheduled or waited for.

The production adapters import their drivers lazily, which keeps
``pip install -e .`` free of a Kafka or PostgreSQL dependency for anyone who
only wants to run the suite.
"""

from .clickhouse_memory import InMemoryClickHouse
from .control_sqlite import SqliteControlPlane
from .kafka_memory import InMemoryBroker, InMemoryConsumer

__all__ = [
    "InMemoryBroker",
    "InMemoryClickHouse",
    "InMemoryConsumer",
    "SqliteControlPlane",
]
