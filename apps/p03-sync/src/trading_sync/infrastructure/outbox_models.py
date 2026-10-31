"""Project 07's outbox tables, declared so Project 03 can write into them.

Why these live in their own ``MetaData``
---------------------------------------
``trading_sync.infrastructure.db_models.metadata`` is what alembic revision
0001 passes to ``create_all``. Adding ``event_outbox`` there would make P03
create a table Project 07 owns, which is the mistake contract gap CG-07
records in the other direction. A separate ``MetaData`` gives the reader and
the writer a typed table object without ever putting it in P03's creation set.

So this module declares; ``outbox migrate`` creates. If the two drift, inserts
fail loudly against a real database, which is the right failure: it is visible,
immediate and attributable.

Why P03 writes this table at all
--------------------------------
Project 07's first design decision is that the business row and its outbox row
commit in one PostgreSQL transaction. P07's own ``OutboxWriter`` is synchronous
psycopg; P03 is async SQLAlchemy over asyncpg. Calling P07's writer from P03
would use a second connection, which is a second transaction - precisely the
arrangement the invariant forbids. The event *construction* is pure and now
lives in ``platform-events``, so P03 builds the row and persists it with its
own session, inside its own unit of work (contract gap CG-08).

Only the columns a producer supplies are declared NOT NULL here. The relay's
own lifecycle columns (``state``, ``attempt_count``, ``claim_*``,
``lease_until``, ``published_at``, ``kafka_*``) carry database defaults and
belong to P07; P03 never sets them, and declaring them without defaults would
invite it to.
"""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

#: Deliberately NOT ``db_models.metadata``. Nothing in P03's migrations may
#: create these tables.
outbox_metadata = MetaData()

event_outbox = Table(
    "event_outbox",
    outbox_metadata,
    # Identity column: assigned by the database, never supplied by a producer.
    Column("outbox_id", BigInteger, primary_key=True, autoincrement=True),
    Column("event_id", UUID(as_uuid=False), nullable=False, unique=True),
    Column("source_id", Text, nullable=False),
    Column("epoch_id", UUID(as_uuid=False), nullable=False),
    Column("dedupe_key", Text, nullable=False),
    Column("aggregate_type", Text, nullable=False),
    Column("aggregate_key", Text, nullable=False),
    Column("aggregate_version", BigInteger, nullable=False),
    Column("event_type", Text, nullable=False),
    Column("schema_version", Integer, nullable=False),
    Column("topic", Text, nullable=False),
    Column("partition_key", Text, nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("payload_sha256", String(64), nullable=False),
    Column("wire_bytes", LargeBinary, nullable=False),
    # Relay-owned from here down. Present so the reader can see them; P03
    # supplies only created_at, and only so the row and the envelope agree on
    # one timestamp instead of two clocks disagreeing by a few microseconds.
    Column("state", Text, nullable=False, server_default="PENDING"),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("next_attempt_at", DateTime(timezone=True), nullable=False),
    Column("attempt_count", Integer, nullable=False, server_default="0"),
    UniqueConstraint("source_id", "epoch_id", "dedupe_key", name="uq_outbox_dedupe"),
    UniqueConstraint(
        "source_id",
        "epoch_id",
        "aggregate_key",
        "aggregate_version",
        name="uq_outbox_aggregate_version",
    ),
    CheckConstraint("aggregate_version > 0", name="ck_outbox_aggregate_version_positive"),
)

outbox_aggregate_heads = Table(
    "outbox_aggregate_heads",
    outbox_metadata,
    Column("source_id", Text, primary_key=True),
    Column("epoch_id", UUID(as_uuid=False), primary_key=True),
    Column("aggregate_key", Text, primary_key=True),
    Column("last_version", BigInteger, nullable=False, server_default="0"),
)

__all__ = ["event_outbox", "outbox_aggregate_heads", "outbox_metadata"]
