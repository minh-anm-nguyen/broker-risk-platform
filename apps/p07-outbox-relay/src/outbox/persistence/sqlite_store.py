"""SQLite-backed outbox store: real transactions, real fencing.

Why SQLite is a legitimate backend for this project and not a shortcut: every
invariant in design section 4 that concerns the *database* -- atomic business +
outbox commit, the unique semantic key, the aggregate version certificate, the
claim CAS -- is a property of ACID transactions and SQL constraints, and SQLite
provides both. Running the claim loop here exercises the same control flow and
the same constraints as :mod:`outbox.persistence.postgres_store`.

What SQLite does *not* give us is equally important to state: no ``SKIP
LOCKED``, so concurrent claims serialise on the single writer lock instead of
taking disjoint rows, and no replication, so nothing here says anything about
surviving a primary loss. The MVP discipline in design section 7.1 is a single
active relay owner, which is exactly what one writer lock enforces -- but
multi-shard claim throughput must be measured on PostgreSQL, not inferred from
these tests.

Transactions are opened with ``BEGIN IMMEDIATE``. The default deferred mode
would acquire the write lock lazily, on the first write, which means two writers
can both read, both decide to allocate version ``n+1``, and one then fails at
commit time. Taking the lock up front turns that race into a short wait.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ..clock import Clock, RealClock, from_wire, to_wire
from ..errors import IntegrityIncident, StoreUnavailable
from ..events.envelope import Envelope
from .ports import AttemptOutcome, Claim, EventState, OutboxRecord, OutboxStats
from .schema_sql import CLAIM_SELECT_SQLITE, OUTBOX_SQLITE_DDL, P03_SQLITE_DDL

_RECORD_COLUMNS = """
    outbox_id, event_id, source_id, epoch_id, dedupe_key, aggregate_type,
    aggregate_key, aggregate_version, event_type, schema_version, topic,
    partition_key, payload, payload_sha256, wire_bytes, state, created_at,
    next_attempt_at, attempt_count, claim_owner, claim_token, claim_version,
    lease_until, published_at, kafka_partition, kafka_offset, last_error_code,
    quarantine_reason
"""


def _as_record(row: sqlite3.Row) -> OutboxRecord:
    return OutboxRecord(
        outbox_id=row["outbox_id"],
        event_id=uuid.UUID(row["event_id"]),
        source_id=row["source_id"],
        epoch_id=row["epoch_id"],
        dedupe_key=row["dedupe_key"],
        aggregate_type=row["aggregate_type"],
        aggregate_key=row["aggregate_key"],
        aggregate_version=row["aggregate_version"],
        event_type=row["event_type"],
        schema_version=row["schema_version"],
        topic=row["topic"],
        partition_key=row["partition_key"],
        payload=json.loads(row["payload"]),
        payload_sha256=row["payload_sha256"],
        wire_bytes=bytes(row["wire_bytes"]),
        state=EventState(row["state"]),
        created_at=from_wire(row["created_at"]),
        next_attempt_at=from_wire(row["next_attempt_at"]),
        attempt_count=row["attempt_count"],
        claim_owner=row["claim_owner"],
        claim_token=uuid.UUID(row["claim_token"]) if row["claim_token"] else None,
        claim_version=row["claim_version"],
        lease_until=from_wire(row["lease_until"]) if row["lease_until"] else None,
        published_at=from_wire(row["published_at"]) if row["published_at"] else None,
        kafka_partition=row["kafka_partition"],
        kafka_offset=row["kafka_offset"],
        last_error_code=row["last_error_code"],
        quarantine_reason=row["quarantine_reason"],
    )


@dataclass(slots=True)
class SqliteTransaction:
    """A live transaction. Handed to the outbox writer, never opened by it."""

    conn: sqlite3.Connection
    _heads_locked: set[tuple[str, str, str]]

    def execute(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, tuple(params))

    def reserve_aggregate_head(self, source_id: str, epoch_id: str, aggregate_key: str) -> int:
        """Create the head row if absent and return its current last_version.

        Under ``BEGIN IMMEDIATE`` the write lock is already held, so creating
        and reading the head is atomic with respect to other writers. The
        ``_heads_locked`` set is not a lock -- it records that this transaction
        has been through the head for this aggregate, so that
        :meth:`bump_aggregate_head` can refuse to allocate a version for an
        aggregate whose head was never reserved. That ordering (reserve, check
        the semantic key, only then allocate) is design section 6.3 step 3, and
        skipping it is how duplicate versions get minted.
        """
        self.conn.execute(
            "INSERT INTO outbox_aggregate_heads (source_id, epoch_id, aggregate_key, last_version) "
            "VALUES (?, ?, ?, 0) ON CONFLICT DO NOTHING",
            (source_id, epoch_id, aggregate_key),
        )
        row = self.conn.execute(
            "SELECT last_version FROM outbox_aggregate_heads "
            "WHERE source_id = ? AND epoch_id = ? AND aggregate_key = ?",
            (source_id, epoch_id, aggregate_key),
        ).fetchone()
        self._heads_locked.add((source_id, epoch_id, aggregate_key))
        return int(row["last_version"])

    def bump_aggregate_head(self, source_id: str, epoch_id: str, aggregate_key: str) -> int:
        key = (source_id, epoch_id, aggregate_key)
        if key not in self._heads_locked:
            raise IntegrityIncident(
                f"aggregate head {key} was not reserved in this transaction; "
                "reserve it before allocating a version"
            )
        self.conn.execute(
            "UPDATE outbox_aggregate_heads SET last_version = last_version + 1 "
            "WHERE source_id = ? AND epoch_id = ? AND aggregate_key = ?",
            key,
        )
        row = self.conn.execute(
            "SELECT last_version FROM outbox_aggregate_heads "
            "WHERE source_id = ? AND epoch_id = ? AND aggregate_key = ?",
            key,
        ).fetchone()
        return int(row["last_version"])

    def find_event_by_dedupe(self, source_id: str, epoch_id: str, dedupe_key: str) -> OutboxRecord | None:
        row = self.conn.execute(
            f"SELECT {_RECORD_COLUMNS} FROM event_outbox "
            "WHERE source_id = ? AND epoch_id = ? AND dedupe_key = ?",
            (source_id, epoch_id, dedupe_key),
        ).fetchone()
        return _as_record(row) if row else None

    def epoch_status(self, source_id: str, epoch_id: str) -> str:
        row = self.conn.execute(
            "SELECT status FROM source_epochs WHERE source_id = ? AND epoch_id = ?",
            (source_id, epoch_id),
        ).fetchone()
        if row is None:
            # An unknown epoch is not an ACTIVE epoch. Returning a status that
            # reads as "fine" for a row that does not exist is how an event for
            # the wrong source gets published.
            return "UNKNOWN"
        return str(row["status"])

    def insert_event(self, envelope: Envelope, *, topic: str, partition_key: str, now: datetime) -> int:
        cursor = self.conn.execute(
            """
            INSERT INTO event_outbox (
                event_id, source_id, epoch_id, dedupe_key, aggregate_type, aggregate_key,
                aggregate_version, event_type, schema_version, topic, partition_key,
                payload, payload_sha256, wire_bytes, state, created_at, next_attempt_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)
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
                to_wire(now),
                to_wire(now),
            ),
        )
        return int(cursor.lastrowid or 0)


class SqliteOutboxStore:
    """Publisher-side store over one SQLite database file."""

    def __init__(
        self,
        path: str | Path,
        *,
        clock: Clock | None = None,
        busy_timeout_ms: int = 5_000,
        synchronous: str = "FULL",
    ):
        """``synchronous`` defaults to ``FULL``, which is the right production value.

        A test suite may lower it to ``NORMAL``. That is not a weakening of what
        the suite proves: ``synchronous`` governs whether a commit survives an
        *operating system or power* failure, and the fault harness simulates a
        process crash in-process, where the database file is never left
        mid-write. Transaction atomicity -- the property every kill-point test
        asserts -- holds identically at either setting, so ``FULL`` in tests
        buys fsync latency and no extra coverage.
        """
        self._path = str(path)
        self._clock = clock or RealClock()
        self._busy_timeout_ms = busy_timeout_ms
        self._synchronous = synchronous
        self._local = threading.local()
        self._all_conns: list[sqlite3.Connection] = []
        self._conns_lock = threading.Lock()

    # --- connection handling ------------------------------------------------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(f"PRAGMA synchronous={self._synchronous}")
        conn.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
        # Declared foreign keys are documentation until this is on, and the
        # epoch reference is one of the constraints the design relies on.
        conn.execute("PRAGMA foreign_keys=ON")
        with self._conns_lock:
            self._all_conns.append(conn)
        return conn

    @property
    def conn(self) -> sqlite3.Connection:
        """One connection per thread, so two threads can be two relay owners."""
        existing = getattr(self._local, "conn", None)
        if existing is None:
            existing = self._connect()
            self._local.conn = existing
        return existing

    def close(self) -> None:
        with self._conns_lock:
            conns, self._all_conns = self._all_conns, []
        for conn in conns:
            try:
                conn.close()
            except sqlite3.Error:  # pragma: no cover - best effort on teardown
                pass
        self._local = threading.local()

    def migrate(self) -> None:
        conn = self.conn
        conn.executescript(P03_SQLITE_DDL)
        conn.executescript(OUTBOX_SQLITE_DDL)

    @contextmanager
    def transaction(self) -> Iterator[SqliteTransaction]:
        """Open ``BEGIN IMMEDIATE``; commit on success, roll back on any error.

        Rolling back on *any* exception, including a crash signal injected by
        the fault harness, is what makes the kill-point tests meaningful: the
        harness raises inside the block and the assertion is that neither the
        Deal nor the outbox row survived.
        """
        conn = self.conn
        try:
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            raise StoreUnavailable(f"could not begin transaction: {exc}") from exc
        tx = SqliteTransaction(conn=conn, _heads_locked=set())
        try:
            yield tx
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")

    # --- relay operations ---------------------------------------------------
    def claim_due(self, *, owner: str, batch_size: int, lease_seconds: float, now: datetime) -> list[Claim]:
        """Claim up to ``batch_size`` eligible rows in one short transaction.

        Short on purpose: design section 7.2 point 4 forbids holding row locks
        across a broker call, so this method returns and the produce happens
        outside any transaction.
        """
        lease_until = now + timedelta(seconds=lease_seconds)
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            ids = [
                int(row["outbox_id"])
                for row in conn.execute(
                    CLAIM_SELECT_SQLITE, {"now": to_wire(now), "batch_size": batch_size}
                ).fetchall()
            ]
            claims: list[Claim] = []
            for outbox_id in ids:
                token = uuid.uuid4()
                cursor = conn.execute(
                    """
                    UPDATE event_outbox
                       SET state = 'CLAIMED',
                           claim_owner = ?,
                           claim_token = ?,
                           claim_version = claim_version + 1,
                           lease_until = ?,
                           attempt_count = attempt_count + 1
                     WHERE outbox_id = ?
                       AND (
                             (state = 'PENDING' AND next_attempt_at <= ?)
                          OR (state = 'CLAIMED' AND lease_until < ?)
                           )
                    """,
                    (owner, str(token), to_wire(lease_until), outbox_id, to_wire(now), to_wire(now)),
                )
                if cursor.rowcount != 1:
                    # Someone else took it between the SELECT and the UPDATE.
                    # Skipping is correct: the row is owned and will be retried
                    # by its owner or reclaimed when that lease expires.
                    continue
                row = conn.execute(
                    f"SELECT {_RECORD_COLUMNS} FROM event_outbox WHERE outbox_id = ?", (outbox_id,)
                ).fetchone()
                record = _as_record(row)
                assert record.claim_token is not None and record.lease_until is not None
                claims.append(
                    Claim(
                        outbox_id=record.outbox_id,
                        event_id=record.event_id,
                        source_id=record.source_id,
                        epoch_id=record.epoch_id,
                        dedupe_key=record.dedupe_key,
                        aggregate_key=record.aggregate_key,
                        aggregate_version=record.aggregate_version,
                        event_type=record.event_type,
                        schema_version=record.schema_version,
                        topic=record.topic,
                        partition_key=record.partition_key,
                        wire_bytes=record.wire_bytes,
                        payload_sha256=record.payload_sha256,
                        attempt_count=record.attempt_count,
                        claim_owner=owner,
                        claim_token=record.claim_token,
                        claim_version=record.claim_version,
                        lease_until=record.lease_until,
                        created_at=record.created_at,
                    )
                )
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")
        return claims

    def _cas(self, claim: Claim, sql: str, params: tuple[Any, ...]) -> bool:
        """Run a lifecycle update conditional on still owning the claim.

        The predicate is ``state='CLAIMED' AND claim_token=? AND
        claim_version=?``. Checking the token alone would be enough today, but
        the version makes the condition monotonic: a token could in principle
        be reissued, a version cannot go backwards.

        Note what is deliberately *not* in the predicate: ``lease_until > now``.
        Fencing exists to stop a stale worker overwriting a **new owner's**
        state, and a reclaim is what bumps ``claim_version``, so the token pair
        already catches every case where someone else holds the row. If nobody
        has taken it over, an expired lease means only that this worker is late
        -- and rejecting its write would send a row that was already delivered
        back for another publish, manufacturing a duplicate to protect against
        a conflict that does not exist.
        """
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            cursor = conn.execute(
                sql, (*params, claim.outbox_id, str(claim.claim_token), claim.claim_version)
            )
            changed = cursor.rowcount == 1
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")
        return changed

    def mark_published_if_owner(
        self, *, claim: Claim, kafka_partition: int, kafka_offset: int, now: datetime
    ) -> bool:
        return self._cas(
            claim,
            """
            UPDATE event_outbox
               SET state = 'PUBLISHED',
                   published_at = ?,
                   kafka_partition = ?,
                   kafka_offset = ?,
                   claim_owner = NULL,
                   claim_token = NULL,
                   lease_until = NULL,
                   last_error_code = NULL
             WHERE outbox_id = ? AND state = 'CLAIMED'
               AND claim_token = ? AND claim_version = ?
            """,
            (to_wire(now), kafka_partition, kafka_offset),
        )

    def reschedule_if_owner(
        self, *, claim: Claim, delay_seconds: float, error_code: str, now: datetime
    ) -> bool:
        next_attempt = now + timedelta(seconds=delay_seconds)
        return self._cas(
            claim,
            """
            UPDATE event_outbox
               SET state = 'PENDING',
                   next_attempt_at = ?,
                   last_error_code = ?,
                   claim_owner = NULL,
                   claim_token = NULL,
                   lease_until = NULL
             WHERE outbox_id = ? AND state = 'CLAIMED'
               AND claim_token = ? AND claim_version = ?
            """,
            (to_wire(next_attempt), error_code),
        )

    def quarantine_if_owner(self, *, claim: Claim, reason: str, error_code: str, now: datetime) -> bool:
        return self._cas(
            claim,
            """
            UPDATE event_outbox
               SET state = 'QUARANTINED',
                   quarantine_reason = ?,
                   last_error_code = ?,
                   claim_owner = NULL,
                   claim_token = NULL,
                   lease_until = NULL,
                   next_attempt_at = ?
             WHERE outbox_id = ? AND state = 'CLAIMED'
               AND claim_token = ? AND claim_version = ?
            """,
            (reason, error_code, to_wire(now)),
        )

    def release_if_owner(self, *, claim: Claim, now: datetime) -> bool:
        """Give a claim back immediately, used by graceful drain (KP07-17)."""
        return self._cas(
            claim,
            """
            UPDATE event_outbox
               SET state = 'PENDING',
                   next_attempt_at = ?,
                   claim_owner = NULL,
                   claim_token = NULL,
                   lease_until = NULL
             WHERE outbox_id = ? AND state = 'CLAIMED'
               AND claim_token = ? AND claim_version = ?
            """,
            (to_wire(now),),
        )

    def record_attempt(
        self, *, claim: Claim, outcome: AttemptOutcome, error_code: str | None, now: datetime
    ) -> None:
        self.conn.execute(
            "INSERT INTO outbox_attempts "
            "(attempt_id, event_id, claim_version, outcome, error_code, attempted_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                str(uuid.uuid4()),
                str(claim.event_id),
                claim.claim_version,
                str(outcome),
                error_code,
                to_wire(now),
            ),
        )

    def requeue_quarantined(self, *, event_id: uuid.UUID, now: datetime, approved_by: str) -> bool:
        """Operator-approved return of a quarantined row to PENDING.

        The ``event_id`` is preserved, which is the whole contract: a repaired
        event re-enters the pipeline as the *same* logical event, so downstream
        dedup still recognises it (design section 10.6).
        """
        if not approved_by:
            raise IntegrityIncident("requeue_quarantined requires an approver for the audit trail")
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            cursor = conn.execute(
                "UPDATE event_outbox SET state = 'PENDING', next_attempt_at = ?, attempt_count = 0 "
                "WHERE event_id = ? AND state = 'QUARANTINED'",
                (to_wire(now), str(event_id)),
            )
            changed = cursor.rowcount == 1
            if changed:
                conn.execute(
                    "INSERT INTO outbox_attempts "
                    "(attempt_id, event_id, claim_version, outcome, error_code, attempted_at) "
                    "VALUES (?, ?, 0, 'REQUEUED', ?, ?)",
                    (str(uuid.uuid4()), str(event_id), f"approved_by={approved_by}", to_wire(now)),
                )
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")
        return changed

    # --- reads --------------------------------------------------------------
    def get(self, event_id: uuid.UUID) -> OutboxRecord | None:
        row = self.conn.execute(
            f"SELECT {_RECORD_COLUMNS} FROM event_outbox WHERE event_id = ?", (str(event_id),)
        ).fetchone()
        return _as_record(row) if row else None

    def get_by_dedupe(self, source_id: str, epoch_id: str, dedupe_key: str) -> OutboxRecord | None:
        row = self.conn.execute(
            f"SELECT {_RECORD_COLUMNS} FROM event_outbox "
            "WHERE source_id = ? AND epoch_id = ? AND dedupe_key = ?",
            (source_id, epoch_id, dedupe_key),
        ).fetchone()
        return _as_record(row) if row else None

    def iter_records(self, *, state: EventState | None = None) -> Iterator[OutboxRecord]:
        sql = f"SELECT {_RECORD_COLUMNS} FROM event_outbox"
        params: tuple[Any, ...] = ()
        if state is not None:
            sql += " WHERE state = ?"
            params = (str(state),)
        sql += " ORDER BY outbox_id"
        for row in self.conn.execute(sql, params):
            yield _as_record(row)

    def attempts(self, event_id: uuid.UUID) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT outcome, error_code, claim_version, attempted_at FROM outbox_attempts "
                "WHERE event_id = ? ORDER BY attempted_at, rowid",
                (str(event_id),),
            )
        )

    def aggregate_head(self, source_id: str, epoch_id: str, aggregate_key: str) -> int:
        row = self.conn.execute(
            "SELECT last_version FROM outbox_aggregate_heads "
            "WHERE source_id = ? AND epoch_id = ? AND aggregate_key = ?",
            (source_id, epoch_id, aggregate_key),
        ).fetchone()
        return int(row["last_version"]) if row else 0

    def stats(self, *, now: datetime, include_storage: bool = True) -> OutboxStats:
        """Backlog counts and age, and optionally the storage figure.

        ``include_storage=False`` skips the ``PRAGMA page_count`` pair. The
        relay reads these gauges every poll cycle and the storage number is the
        expensive part, so it is refreshed on a slower cadence; the counts come
        from an index scan on ``state``.
        """
        counts = {
            str(row["state"]): int(row["n"])
            for row in self.conn.execute("SELECT state, COUNT(*) AS n FROM event_outbox GROUP BY state")
        }
        oldest = self.conn.execute(
            "SELECT MIN(created_at) AS oldest FROM event_outbox WHERE state IN ('PENDING','CLAIMED')"
        ).fetchone()["oldest"]
        age = (now - from_wire(oldest)).total_seconds() if oldest else 0.0
        storage_bytes = self.storage_bytes() if include_storage else 0
        return OutboxStats(
            pending_total=counts.get("PENDING", 0),
            claimed_total=counts.get("CLAIMED", 0),
            published_total=counts.get("PUBLISHED", 0),
            quarantined_total=counts.get("QUARANTINED", 0),
            # Clamp: a clock that moved backwards must not report a negative
            # backlog age, because an alert threshold would then never fire.
            oldest_pending_age_seconds=max(0.0, age),
            storage_bytes=storage_bytes,
        )

    def storage_bytes(self) -> int:
        """Bytes the database file occupies, including indexes and free pages."""
        page_count = self.conn.execute("PRAGMA page_count").fetchone()[0]
        page_size = self.conn.execute("PRAGMA page_size").fetchone()[0]
        return int(page_count) * int(page_size)

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        """Read-only SQL for oracles and operational inspection."""
        return list(self.conn.execute(sql, tuple(params)))
