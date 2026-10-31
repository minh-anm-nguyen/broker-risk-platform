"""Shared event contract for the broker risk platform.

The envelope, identity, schema, routing, money and canonical-serialisation
rules that Projects 03, 07 and 08 must agree on, in one place with no database
and no broker dependency. A consumer that has never heard of the outbox table
can import this and still validate what it received.

This was extracted from ``outbox.events``, whose own docstring already
described it as "the ``broker-events`` package ... shared by the writer (which
runs inside P03's transaction), the relay and every consumer". The extraction
makes that real: Project 03 can now build an event without importing Project
07, which is the wrong direction of dependency for a producer.

Nothing here performs I/O. That is the property that lets Project 03 construct
an event inside its own SQLAlchemy transaction and persist it with its own
session, which is the only way the outbox row and the business row can share a
transaction (P07 design decision 1).
"""

from . import canonical, clock, errors, money
from .events import (
    AGGREGATE_ACCOUNTS,
    AGGREGATE_POSITIONS,
    AGGREGATE_RECONCILIATION,
    AGGREGATE_SOURCE_LIFECYCLE,
    ALL_TOPICS,
    CONSUMABLE_TOPICS,
    REGISTRY,
    DedupeKey,
    Envelope,
    EventSpec,
    EventType,
    Route,
    TopicSpec,
    build_dedupe_key,
    derive_event_id,
    get_spec,
    parse_dedupe_key,
    partition_key,
    route,
    topic_for,
    validate_payload,
)

__version__ = "1.0.0"

#: Schema version this package implements. A producer and a consumer that
#: disagree here are not compatible, and neither should guess.
EVENT_SCHEMA_VERSION = 1

__all__ = [
    "AGGREGATE_ACCOUNTS",
    "AGGREGATE_POSITIONS",
    "AGGREGATE_RECONCILIATION",
    "AGGREGATE_SOURCE_LIFECYCLE",
    "ALL_TOPICS",
    "CONSUMABLE_TOPICS",
    "EVENT_SCHEMA_VERSION",
    "REGISTRY",
    "DedupeKey",
    "Envelope",
    "EventSpec",
    "EventType",
    "Route",
    "TopicSpec",
    "build_dedupe_key",
    "canonical",
    "clock",
    "derive_event_id",
    "errors",
    "get_spec",
    "money",
    "parse_dedupe_key",
    "partition_key",
    "route",
    "topic_for",
    "validate_payload",
]
