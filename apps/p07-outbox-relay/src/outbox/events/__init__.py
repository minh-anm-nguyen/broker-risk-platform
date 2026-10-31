"""Event identity, schema, envelope and routing -- the ``broker-events`` package.

Shared by the writer (which runs inside P03's transaction), the relay and every
consumer. It deliberately has no database or broker dependency: the contract
must be importable by a consumer that has never heard of our outbox table.
"""

from .envelope import Envelope
from .keys import (
    AGGREGATE_ACCOUNTS,
    AGGREGATE_POSITIONS,
    AGGREGATE_RECONCILIATION,
    AGGREGATE_SOURCE_LIFECYCLE,
    DedupeKey,
    EventType,
    build_dedupe_key,
    derive_event_id,
    parse_dedupe_key,
    partition_key,
)
from .routing import ALL_TOPICS, CONSUMABLE_TOPICS, Route, TopicSpec, route, topic_for
from .schema import REGISTRY, EventSpec, get_spec, validate_payload

__all__ = [
    "AGGREGATE_ACCOUNTS",
    "AGGREGATE_POSITIONS",
    "AGGREGATE_RECONCILIATION",
    "AGGREGATE_SOURCE_LIFECYCLE",
    "ALL_TOPICS",
    "CONSUMABLE_TOPICS",
    "REGISTRY",
    "DedupeKey",
    "Envelope",
    "EventSpec",
    "EventType",
    "Route",
    "TopicSpec",
    "build_dedupe_key",
    "derive_event_id",
    "get_spec",
    "parse_dedupe_key",
    "partition_key",
    "route",
    "topic_for",
    "validate_payload",
]
