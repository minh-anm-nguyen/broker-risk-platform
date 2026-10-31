"""Topic routing, partition keys and the tenant allowlist.

Routing is decided at *write* time and stored on the outbox row, not recomputed
at publish time. That is deliberate: if the topic map changes while rows are
pending, the pending rows keep the destination they were written for, and the
change applies to new events. Recomputing on publish would silently move
in-flight events to a different topic -- and therefore a different partition
count, a different ordering stream and a different retention policy -- in the
middle of a backlog drain.

``authorize_source`` exists because design section 10.3 is specific: the
``source_id`` comes from the P03 epoch, server-side, and is never taken from
client-supplied metadata. A broker id must not become a topic suffix without
validation, so the topic name is looked up from a fixed table and the source is
checked against an allowlist; neither is built by string concatenation from
input.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..errors import TenantForbidden, ValidationError
from .keys import EventType, partition_key


@dataclass(frozen=True, slots=True)
class TopicSpec:
    """A topic's pinned configuration.

    Partition count and retention are part of the contract, not operational
    trivia: section 5.2 warns that growing an existing topic's partition count
    remaps keys and breaks per-aggregate order, so both values live in code
    where a change shows up in review.
    """

    name: str
    partitions: int
    retention_seconds: int


#: Example values from design section 5.2. Production requires ACLs,
#: replication factor and min.insync.replicas set against the real broker count.
DEALS_TOPIC = TopicSpec("broker.deals.v1", partitions=6, retention_seconds=7 * 86_400)
SNAPSHOTS_TOPIC = TopicSpec("broker.snapshots.v1", partitions=3, retention_seconds=14 * 86_400)
LIFECYCLE_TOPIC = TopicSpec("broker.lifecycle.v1", partitions=3, retention_seconds=14 * 86_400)
#: Sanitised poison metadata only -- never the raw private payload.
POISON_TOPIC = TopicSpec("broker.poison.v1", partitions=3, retention_seconds=30 * 86_400)

TOPIC_MAP: dict[EventType, TopicSpec] = {
    EventType.DEAL_RECORDED: DEALS_TOPIC,
    EventType.ACCOUNT_SNAPSHOT_PUBLISHED: SNAPSHOTS_TOPIC,
    EventType.POSITION_SNAPSHOT_PUBLISHED: SNAPSHOTS_TOPIC,
    EventType.SOURCE_EPOCH_CHANGED: LIFECYCLE_TOPIC,
    EventType.RECONCILIATION_VERIFIED: LIFECYCLE_TOPIC,
}

ALL_TOPICS: tuple[TopicSpec, ...] = (DEALS_TOPIC, SNAPSHOTS_TOPIC, LIFECYCLE_TOPIC, POISON_TOPIC)

#: Topics the reference consumer is allowed to subscribe to. The poison topic is
#: excluded: a sink that consumed its own quarantine stream would apply the
#: thing it quarantined.
CONSUMABLE_TOPICS: tuple[str, ...] = (DEALS_TOPIC.name, SNAPSHOTS_TOPIC.name, LIFECYCLE_TOPIC.name)


@dataclass(frozen=True, slots=True)
class Route:
    topic: str
    partition_key: str


def topic_for(event_type: EventType) -> TopicSpec:
    try:
        return TOPIC_MAP[EventType(event_type)]
    except (KeyError, ValueError) as exc:
        raise ValidationError(f"no topic is mapped for event type {event_type!r}") from exc


def authorize_source(source_id: str, allowlist: tuple[str, ...]) -> str:
    """Confirm the source is one this deployment is configured to publish."""
    if source_id not in allowlist:
        raise TenantForbidden(f"source_id {source_id!r} is not in the configured allowlist")
    return source_id


def route(
    *,
    event_type: EventType,
    source_id: str,
    epoch_id: str,
    aggregate_key: str,
    allowlist: tuple[str, ...],
) -> Route:
    """Resolve the destination topic and record key for one event."""
    authorize_source(source_id, allowlist)
    spec = topic_for(event_type)
    return Route(topic=spec.name, partition_key=partition_key(source_id, epoch_id, aggregate_key))


def partition_for_key(key: str, partitions: int) -> int:
    """Stable key-to-partition mapping.

    A plain hash of the key bytes. Not Kafka's murmur2, and it does not need to
    be: what the design requires is that one key always lands on one partition
    for a given partition count, and that the mapping is reproducible in a test.
    The real adapter delegates to the broker's own partitioner.
    """
    if partitions < 1:
        raise ValidationError("partitions must be >= 1")
    from hashlib import blake2b

    digest = blake2b(key.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % partitions
