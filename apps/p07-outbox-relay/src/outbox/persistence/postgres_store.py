"""PostgreSQL-backed outbox store.

Import-gated: ``psycopg`` is an optional dependency, so this module raises a
clear :class:`~outbox.errors.ConfigurationError` at construction rather than at
import if the driver is missing. Tests that target it are marked ``postgres``
and skip when ``OUTBOX_PG_DSN`` is unset, which keeps the default suite
runnable on a laptop with no server.

The differences from the SQLite backend are exactly the two the design cares
about, and both are visible in the SQL rather than hidden behind a translation
layer:

* the claim is the single ``WITH ... FOR UPDATE SKIP LOCKED`` statement from
  design section 7.2, so concurrent owners take disjoint rows instead of
  queueing behind one writer lock, and
* lease and due-time comparisons use the server's ``clock_timestamp()``, so
  expiry is judged by the database rather than by a relay whose clock may have
  drifted.

Everything else -- the CAS predicate, the immutability trigger, the unique
semantic key -- is the same contract, which is the point: a test that passes
here and on SQLite is testing the protocol, not one driver.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ..clock import Clock, RealClock
from ..errors import ConfigurationError, IntegrityIncident, StoreUnavailable
from ..events.envelope import Envelope
from .ports import AttemptOutcome, Claim, EventState, OutboxRecord, OutboxStats
from .schema_sql import (
    CLAIM_UPDATE_POSTGRES,
    OUTBOX_POSTGRES_DDL,
    P03_POSTGRES_DDL,
)

_RECORD_COLUMNS = """
    outbox_id, event_id, source_id, epoch_id, dedupe_key, aggregate_type,
    aggregate_key, aggregate_version, event_type, schema_version, topic,
    partition_key, payload, payload_sha256, wire_bytes, state, created_at,
    next_attempt_at, attempt_count, claim_owner, claim_token, claim_version,
    lease_until, published_at, kafka_partition, kafka_offset, last_error_code,
    quarantine_reason
"""


def _require_psycopg() -> Any:
    try:
        import psycopg  # noqa: PLC0415 - optional dependency, probed on demand
        from psycopg.rows import dict_row  # noqa: PLC0415
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised by env
        raise ConfigurationError(
            "store_backend=postgres requires the 'postgres' extra: pip install "
            "'postgres-outbox-kafka[postgres]'"
        ) from exc
    return psycopg, dict_row


def _as_record(row: dict[str, Any]) -> OutboxRecord:
    payload = row["payload"]
    return OutboxRecord(
        outbox_id=int(row["outbox_id"]),
        event_id=row["event_id"] if isinstance(row["event_id"], uuid.UUID) else uuid.UUID(row["event_id"]),
        source_id=row["source_id"],
        epoch_id=str(row["epoch_id"]),
        dedupe_key=row["dedupe_key"],
        aggregate_type=row["aggregate_type"],
        aggregate_key=row["aggregate_key"],
        aggregate_version=int(row["aggregate_version"]),
        event_type=row["event_type"],
        schema_version=int(row["schema_version"]),
        topic=row["topic"],
        partition_key=row["partition_key"],
        payload=payload if isinstance(payload, dict) else json.loads(payload),
        payload_sha256=row["payload_sha256"],
        wire_bytes=bytes(row["wire_bytes"]),
        state=EventState(row["state"]),
        created_at=row["created_at"],
        next_attempt_at=row["next_attempt_at"],
        attempt_count=int(row["attempt_count"]),
        claim_owner=row["claim_owner"],
        claim_token=row["claim_token"],
        claim_version=int(row["claim_version"]),
        lease_until=row["lease_until"],
        published_at=row["published_at"],
        kafka_partition=row["kafka_partition"],
        kafka_offset=row["kafka_offset"],
        last_error_code=row["last_error_code"],
        quarantine_reason=row["quarantine_reason"],
    )


@dataclass(slots=True)
class PostgresTransaction:
    """A live PostgreSQL transaction the outbox writer joins."""

    conn: Any
    _heads_locked: set[tuple[str, str, str]]

    def execute(self, sql: str, params: Sequence[Any] = ()) -> Any:
        cursor = self.conn.cursor()
        cursor.execute(sql, tuple(params))
        return cursor

    def reserve_aggregate_head(self, source_id: str, epoch_id: str, aggregate_key: str) -> int:
        """Take a row lock on the aggregate head, creating it if absent.

        ``ON CONFLICT DO UPDATE`` with a no-op assignment is how you get a row
        lock on a row that may not exist yet: a plain ``DO NOTHING`` would not
        lock the conflicting row, leaving two writers free to both read
        ``last_version`` and both allocate the same next version.
        """
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO outbox_aggregate_heads (source_id, epoch_id, aggregate_key, last_version)
                VALUES (%s, %s, %s, 0)
                ON CONFLICT (source_id, epoch_id, aggregate_key)
                DO UPDATE SET last_version = outbox_aggregate_heads.last_version
                RETURNING last_version
                """,
                (source_id, epoch_id, aggregate_key),
            )
            row = cursor.fetchone()
        self._heads_locked.add((source_id, epoch_id, aggregate_key))
        return int(row["last_version"] if isinstance(row, dict) else row[0])

    def bump_aggregate_head(self, source_id: str, epoch_id: str, aggregate_key: str) -> int:
        key = (source_id, epoch_id, aggregate_key)
        if key not in self._heads_locked:
            raise IntegrityIncident(
                f"aggregate head {key} was not reserved in this transaction; "
                "reserve it before allocating a version"
            )
        with self.conn.cursor() as cursor:
            cursor.execute(
                "UPDATE outbox_aggregate_heads SET last_version = last_version + 1 "
                "WHERE source_id = %s AND epoch_id = %s AND aggregate_key = %s "
                "RETURNING last_version",
                key,
            )
            row = cursor.fetchone()
        return int(row["last_version"] if isinstance(row, dict) else row[0])

    def find_event_by_dedupe(self, source_id: str, epoch_id: str, dedupe_key: str) -> OutboxRecord | None:
        with self.conn.cursor() as cursor:
            cursor.execute(
                f"SELECT {_RECORD_COLUMNS} FROM event_outbox "
                "WHERE source_id = %s AND epoch_id = %s AND dedupe_key = %s",
                (source_id, epoch_id, dedupe_key),
            )
            row = cursor.fetchone()
        return _as_record(row) if row else None

    def epoch_status(self, source_id: str, epoch_id: str) -> str:
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT status FROM source_epochs WHERE source_id = %s AND epoch_id = %s",
                (source_id, epoch_id),
            )
            row = cursor.fetchone()
        if row is None:
            return "UNKNOWN"
        return str(row["status"] if isinstance(row, dict) else row[0])

    def insert_event(self, envelope: Envelope, *, topic: str, partition_key: str, now: datetime) -> int:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO event_outbox (
                    event_id, source_id, epoch_id, dedupe_key, aggregate_type, aggregate_key,
                    aggregate_version, event_type, schema_version, topic, partition_key,
                    payload, payload_sha256, wire_bytes, state, created_at, next_attempt_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s,
                          'PENDING', %s, %s)
                RETURNING outbox_id
                """,
                (
                    str(envelope.event_id),
                    envelope.source_id,
                    envelope.epoch_id,
                    envelope.dedupe_key,
                    envelope.aggregate_type,
                    envelope.aggregate_key,
                    envelope.aggregate_version,
                    str(envelope.event_type),
                    envelope.schema_version,
                    topic,
                    partition_key,
                    json.dumps(envelope.payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
                    envelope.payload_sha256,
                    envelope.to_wire_bytes(),
                    now,
                    now,
                ),
            )
            row = cursor.fetchone()
        return int(row["outbox_id"] if isinstance(row, dict) else row[0])


class PostgresOutboxStore:
    """Publisher-side store over a PostgreSQL database."""

    def __init__(self, dsn: str, *, clock: Clock | None = None, pool_max: int = 8):
        self._psycopg, self._dict_row = _require_psycopg()
        self._dsn = dsn
        self._clock = clock or RealClock()
        self._pool_max = pool_max
        self._conns: list[Any] = []

    def _connect(self) -> Any:
        try:
            conn = self._psycopg.connect(self._dsn, autocommit=True, row_factory=self._dict_row)
        except self._psycopg.Error as exc:
            raise StoreUnavailable(f"cannot connect to PostgreSQL: {exc}") from exc
        self._conns.append(conn)
        return conn

    @property
    def conn(self) -> Any:
        if not self._conns:
            return self._connect()
        return self._conns[0]

    def close(self) -> None:
        for conn in self._conns:
            try:
                conn.close()
            except Exception:  # pragma: no cover - best effort on teardown
                pass
        self._conns = []

    def migrate(self) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
            cursor.execute(P03_POSTGRES_DDL)
            cursor.execute(OUTBOX_POSTGRES_DDL)

    @contextmanager
    def transaction(self) -> Iterator[PostgresTransaction]:
        conn = self.conn
        with conn.transaction():
            yield PostgresTransaction(conn=conn, _heads_locked=set())

    def claim_due(self, *, owner: str, batch_size: int, lease_seconds: float, now: datetime) -> list[Claim]:
        """One statement: select eligible, lock, fence and return.

        ``now`` is accepted for interface parity with the SQLite backend and for
        deterministic tests, but note that the lease *deadline* is computed by
        the server from this value, so a relay cannot grant itself a longer
        lease than the interval it asked for.
        """
        conn = self.conn
        with conn.transaction(), conn.cursor() as cursor:
            cursor.execute(
                CLAIM_UPDATE_POSTGRES,
                {"now": now, "batch_size": batch_size, "owner": owner, "lease_seconds": lease_seconds},
            )
            rows = cursor.fetchall()
        return [
            Claim(
                outbox_id=int(row["outbox_id"]),
                event_id=row["event_id"],
                source_id=row["source_id"],
                epoch_id=str(row["epoch_id"]),
                dedupe_key=row["dedupe_key"],
                aggregate_key=row["aggregate_key"],
                aggregate_version=int(row["aggregate_version"]),
                event_type=row["event_type"],
                schema_version=int(row["schema_version"]),
                topic=row["topic"],
                partition_key=row["partition_key"],
                wire_bytes=bytes(row["wire_bytes"]),
                payload_sha256=row["payload_sha256"],
                attempt_count=int(row["attempt_count"]),
                claim_owner=row["claim_owner"],
                claim_token=row["claim_token"],
                claim_version=int(row["claim_version"]),
                lease_until=row["lease_until"],
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def _cas(self, claim: Claim, sql: str, params: tuple[Any, ...]) -> bool:
        conn = self.conn
        with conn.transaction(), conn.cursor() as cursor:
            cursor.execute(sql, (*params, claim.outbox_id, claim.claim_token, claim.claim_version))
            return cursor.rowcount == 1

    def mark_published_if_owner(
        self, *, claim: Claim, kafka_partition: int, kafka_offset: int, now: datetime
    ) -> bool:
        return self._cas(
            claim,
            """
            UPDATE event_outbox
               SET state = 'PUBLISHED', published_at = %s, kafka_partition = %s,
                   kafka_offset = %s, claim_owner = NULL, claim_token = NULL,
                   lease_until = NULL, last_error_code = NULL
             WHERE outbox_id = %s AND state = 'CLAIMED'
               AND claim_token = %s AND claim_version = %s
            """,
            (now, kafka_partition, kafka_offset),
        )

    def reschedule_if_owner(
        self, *, claim: Claim, delay_seconds: float, error_code: str, now: datetime
    ) -> bool:
        return self._cas(
            claim,
            """
            UPDATE event_outbox
               SET state = 'PENDING',
                   next_attempt_at = %s + (%s * interval '1 second'),
                   last_error_code = %s, claim_owner = NULL, claim_token = NULL,
                   lease_until = NULL
             WHERE outbox_id = %s AND state = 'CLAIMED'
               AND claim_token = %s AND claim_version = %s
            """,
            (now, delay_seconds, error_code),
        )

    def quarantine_if_owner(self, *, claim: Claim, reason: str, error_code: str, now: datetime) -> bool:
        return self._cas(
            claim,
            """
            UPDATE event_outbox
               SET state = 'QUARANTINED', quarantine_reason = %s, last_error_code = %s,
                   next_attempt_at = %s, claim_owner = NULL, claim_token = NULL,
                   lease_until = NULL
             WHERE outbox_id = %s AND state = 'CLAIMED'
               AND claim_token = %s AND claim_version = %s
            """,
            (reason, error_code, now),
        )

    def release_if_owner(self, *, claim: Claim, now: datetime) -> bool:
        return self._cas(
            claim,
            """
            UPDATE event_outbox
               SET state = 'PENDING', next_attempt_at = %s, claim_owner = NULL,
                   claim_token = NULL, lease_until = NULL
             WHERE outbox_id = %s AND state = 'CLAIMED'
               AND claim_token = %s AND claim_version = %s
            """,
            (now,),
        )

    def record_attempt(
        self, *, claim: Claim, outcome: AttemptOutcome, error_code: str | None, now: datetime
    ) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO outbox_attempts "
                "(attempt_id, event_id, claim_version, outcome, error_code, attempted_at) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (str(uuid.uuid4()), claim.event_id, claim.claim_version, str(outcome), error_code, now),
            )

    def get(self, event_id: uuid.UUID) -> OutboxRecord | None:
        with self.conn.cursor() as cursor:
            cursor.execute(f"SELECT {_RECORD_COLUMNS} FROM event_outbox WHERE event_id = %s", (event_id,))
            row = cursor.fetchone()
        return _as_record(row) if row else None

    def get_by_dedupe(self, source_id: str, epoch_id: str, dedupe_key: str) -> OutboxRecord | None:
        with self.conn.cursor() as cursor:
            cursor.execute(
                f"SELECT {_RECORD_COLUMNS} FROM event_outbox "
                "WHERE source_id = %s AND epoch_id = %s AND dedupe_key = %s",
                (source_id, epoch_id, dedupe_key),
            )
            row = cursor.fetchone()
        return _as_record(row) if row else None

    def iter_records(self, *, state: EventState | None = None) -> Iterator[OutboxRecord]:
        sql = f"SELECT {_RECORD_COLUMNS} FROM event_outbox"
        params: tuple[Any, ...] = ()
        if state is not None:
            sql += " WHERE state = %s"
            params = (str(state),)
        sql += " ORDER BY outbox_id"
        with self.conn.cursor() as cursor:
            cursor.execute(sql, params)
            for row in cursor.fetchall():
                yield _as_record(row)

    def aggregate_head(self, source_id: str, epoch_id: str, aggregate_key: str) -> int:
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT last_version FROM outbox_aggregate_heads "
                "WHERE source_id = %s AND epoch_id = %s AND aggregate_key = %s",
                (source_id, epoch_id, aggregate_key),
            )
            row = cursor.fetchone()
        return int(row["last_version"]) if row else 0

    def stats(self, *, now: datetime, include_storage: bool = True) -> OutboxStats:
        """See the SQLite store for why ``include_storage`` exists."""
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT state, COUNT(*) AS n FROM event_outbox GROUP BY state")
            counts = {str(row["state"]): int(row["n"]) for row in cursor.fetchall()}
            cursor.execute(
                "SELECT MIN(created_at) AS oldest FROM event_outbox WHERE state IN ('PENDING','CLAIMED')"
            )
            oldest = cursor.fetchone()["oldest"]
        storage = self.storage_bytes() if include_storage else 0
        age = (now - oldest).total_seconds() if oldest else 0.0
        return OutboxStats(
            pending_total=counts.get("PENDING", 0),
            claimed_total=counts.get("CLAIMED", 0),
            published_total=counts.get("PUBLISHED", 0),
            quarantined_total=counts.get("QUARANTINED", 0),
            oldest_pending_age_seconds=max(0.0, age),
            storage_bytes=storage,
        )

    def storage_bytes(self) -> int:
        """Heap plus indexes plus TOAST, which is what actually fills the disk."""
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT COALESCE(pg_total_relation_size('event_outbox'), 0) "
                "+ COALESCE(pg_total_relation_size('outbox_attempts'), 0) AS bytes"
            )
            return int(cursor.fetchone()["bytes"])

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        with self.conn.cursor() as cursor:
            cursor.execute(sql, tuple(params))
            return list(cursor.fetchall())
