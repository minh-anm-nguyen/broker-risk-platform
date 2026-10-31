"""The consumer's own database: inbox, version ledger, projection, quarantine.

A separate store class over a separate database file, which is the design's
instruction taken literally (section 6.1): the consumer's tables are "logically
separate from publisher DB ownership" and must not be joined to the outbox by a
foreign key. Making them a different file means such a key cannot be written
even by accident, and it keeps the honest property that the reference sink is a
*downstream system*, not a second schema in ours.

The inbox is what turns at-least-once delivery into an effectively-once business
effect. Three cases, and the middle one is the one people get wrong:

* **New ``event_id``** -- apply, in the same transaction as the inbox insert.
* **Duplicate with the same payload hash** -- a redelivery. Record nothing, apply
  nothing, let the offset advance. This is the expected outcome after a relay
  crash between ACK and CAS.
* **Duplicate with a different payload hash** -- two records claim to be the same
  event and disagree about its content. That is not a duplicate, it is an
  integrity incident; it is quarantined and the aggregate stops.

Money sums are stored as decimal text and accumulated with ``Decimal``, so the
projection stays byte-comparable with the source ``NUMERIC`` values for oracle
O6. A float accumulator would make that oracle fail for reasons unrelated to the
pipeline, and would make it pass for the wrong reasons once the error cancelled.
"""

from __future__ import annotations

import enum
import json
import sqlite3
import threading
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from ..clock import Clock, RealClock, to_wire
from ..errors import IntegrityIncident
from ..money import CurrencyStatus, MoneyCurrency, add_money
from ..persistence.schema_sql import CONSUMER_SQLITE_DDL


class ProcessedOutcome(enum.StrEnum):
    NEW = "NEW"
    DUPLICATE_SAME_HASH = "DUPLICATE_SAME_HASH"
    DUPLICATE_DIFFERENT_HASH = "DUPLICATE_DIFFERENT_HASH"


@dataclass(frozen=True, slots=True)
class VersionState:
    last_applied_version: int
    blocked_reason: str | None

    @property
    def blocked(self) -> bool:
        return self.blocked_reason is not None


@dataclass(frozen=True, slots=True)
class ProjectionRow:
    account_id: str
    money_currency: str | None
    deal_count: int
    realized_pnl_gross: str
    commission: str
    swap: str
    unproven_currency_deal_count: int


class ConsumerStore:
    """The reference sink's persistence."""

    def __init__(
        self,
        path: str | Path,
        *,
        clock: Clock | None = None,
        busy_timeout_ms: int = 5_000,
        synchronous: str = "FULL",
    ):
        """See :class:`~outbox.persistence.sqlite_store.SqliteOutboxStore` for
        why a test suite may lower ``synchronous`` without losing coverage."""
        self._path = str(path)
        self._clock = clock or RealClock()
        self._busy_timeout_ms = busy_timeout_ms
        self._synchronous = synchronous
        self._local = threading.local()
        self._all_conns: list[sqlite3.Connection] = []
        self._conns_lock = threading.Lock()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(f"PRAGMA synchronous={self._synchronous}")
        conn.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
        conn.execute("PRAGMA foreign_keys=ON")
        with self._conns_lock:
            self._all_conns.append(conn)
        return conn

    @property
    def conn(self) -> sqlite3.Connection:
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
            except sqlite3.Error:  # pragma: no cover
                pass
        self._local = threading.local()

    def migrate(self) -> None:
        self.conn.executescript(CONSUMER_SQLITE_DDL)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")

    # --- inbox --------------------------------------------------------------
    def insert_processed(
        self,
        conn: sqlite3.Connection,
        *,
        consumer_name: str,
        event_id: uuid.UUID,
        payload_sha256: str,
        source_id: str,
        epoch_id: str,
        aggregate_key: str,
        aggregate_version: int,
        topic: str,
        partition: int,
        offset: int,
        now: datetime,
    ) -> ProcessedOutcome:
        """Claim this ``event_id`` for this consumer, or report why we cannot."""
        cursor = conn.execute(
            "INSERT INTO consumer_processed_events "
            "(consumer_name, event_id, payload_sha256, source_id, epoch_id, aggregate_key, "
            " aggregate_version, topic, kafka_partition, kafka_offset, processed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (consumer_name, event_id) DO NOTHING",
            (
                consumer_name,
                str(event_id),
                payload_sha256,
                source_id,
                epoch_id,
                aggregate_key,
                aggregate_version,
                topic,
                partition,
                offset,
                to_wire(now),
            ),
        )
        if cursor.rowcount == 1:
            return ProcessedOutcome.NEW
        row = conn.execute(
            "SELECT payload_sha256 FROM consumer_processed_events "
            "WHERE consumer_name = ? AND event_id = ?",
            (consumer_name, str(event_id)),
        ).fetchone()
        stored = row["payload_sha256"] if row else None
        if stored == payload_sha256:
            return ProcessedOutcome.DUPLICATE_SAME_HASH
        return ProcessedOutcome.DUPLICATE_DIFFERENT_HASH

    def processed_count(self, consumer_name: str) -> int:
        return int(
            self.conn.execute(
                "SELECT COUNT(*) AS n FROM consumer_processed_events WHERE consumer_name = ?",
                (consumer_name,),
            ).fetchone()["n"]
        )

    def processed_event_ids(self, consumer_name: str) -> set[str]:
        return {
            row["event_id"]
            for row in self.conn.execute(
                "SELECT event_id FROM consumer_processed_events WHERE consumer_name = ?",
                (consumer_name,),
            )
        }

    # --- version ledger -----------------------------------------------------
    def version_state(
        self,
        conn: sqlite3.Connection,
        *,
        consumer_name: str,
        source_id: str,
        epoch_id: str,
        aggregate_key: str,
    ) -> VersionState:
        """Take the aggregate's version row, creating it at version 0.

        Created rather than defaulted so the row exists to be locked: a new
        aggregate's first event must serialise against a concurrent copy of
        itself, which a missing row cannot do.
        """
        conn.execute(
            "INSERT INTO consumer_aggregate_versions "
            "(consumer_name, source_id, epoch_id, aggregate_key, last_applied_version) "
            "VALUES (?, ?, ?, ?, 0) ON CONFLICT DO NOTHING",
            (consumer_name, source_id, epoch_id, aggregate_key),
        )
        row = conn.execute(
            "SELECT last_applied_version, blocked_reason FROM consumer_aggregate_versions "
            "WHERE consumer_name = ? AND source_id = ? AND epoch_id = ? AND aggregate_key = ?",
            (consumer_name, source_id, epoch_id, aggregate_key),
        ).fetchone()
        return VersionState(
            last_applied_version=int(row["last_applied_version"]),
            blocked_reason=row["blocked_reason"],
        )

    def advance_version(
        self,
        conn: sqlite3.Connection,
        *,
        consumer_name: str,
        source_id: str,
        epoch_id: str,
        aggregate_key: str,
        version: int,
    ) -> None:
        cursor = conn.execute(
            "UPDATE consumer_aggregate_versions SET last_applied_version = ? "
            "WHERE consumer_name = ? AND source_id = ? AND epoch_id = ? AND aggregate_key = ? "
            "AND last_applied_version = ?",
            (version, consumer_name, source_id, epoch_id, aggregate_key, version - 1),
        )
        if cursor.rowcount != 1:
            # The guard is the whole point: this update may only move the
            # version by exactly one, from the value we read under the lock.
            raise IntegrityIncident(
                f"refusing to advance {aggregate_key!r} to version {version}: "
                "the ledger no longer holds the version this transaction read"
            )

    def block_aggregate(
        self,
        conn: sqlite3.Connection,
        *,
        consumer_name: str,
        source_id: str,
        epoch_id: str,
        aggregate_key: str,
        reason: str,
    ) -> None:
        conn.execute(
            "INSERT INTO consumer_aggregate_versions "
            "(consumer_name, source_id, epoch_id, aggregate_key, last_applied_version, blocked_reason) "
            "VALUES (?, ?, ?, ?, 0, ?) "
            "ON CONFLICT (consumer_name, source_id, epoch_id, aggregate_key) "
            "DO UPDATE SET blocked_reason = excluded.blocked_reason",
            (consumer_name, source_id, epoch_id, aggregate_key, reason),
        )

    def unblock_aggregate(
        self, *, consumer_name: str, source_id: str, epoch_id: str, aggregate_key: str, approved_by: str
    ) -> None:
        """Operator-approved resumption after a repair."""
        if not approved_by:
            raise IntegrityIncident("unblocking an aggregate requires an approver")
        with self.transaction() as conn:
            conn.execute(
                "UPDATE consumer_aggregate_versions SET blocked_reason = NULL "
                "WHERE consumer_name = ? AND source_id = ? AND epoch_id = ? AND aggregate_key = ?",
                (consumer_name, source_id, epoch_id, aggregate_key),
            )

    def blocked_aggregates(self, consumer_name: str) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT source_id, epoch_id, aggregate_key, last_applied_version, blocked_reason "
                "FROM consumer_aggregate_versions "
                "WHERE consumer_name = ? AND blocked_reason IS NOT NULL",
                (consumer_name,),
            )
        )

    def version_ledger(self, consumer_name: str) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT source_id, epoch_id, aggregate_key, last_applied_version, blocked_reason "
                "FROM consumer_aggregate_versions WHERE consumer_name = ? "
                "ORDER BY source_id, epoch_id, aggregate_key",
                (consumer_name,),
            )
        )

    # --- projection ---------------------------------------------------------
    def apply_deal(
        self,
        conn: sqlite3.Connection,
        *,
        consumer_name: str,
        source_id: str,
        epoch_id: str,
        event_id: uuid.UUID,
        payload: dict[str, Any],
        now: datetime,
    ) -> None:
        """Fold one Deal into the account's running totals.

        An ``UNPROVEN`` currency is counted and not summed. A *change* of proven
        currency for one account raises: adding USD to a EUR total would produce
        a number that is not money in any currency, and there is no safe way to
        pick one.
        """
        account_id = payload["account_id"]
        currency = MoneyCurrency.from_payload(payload)
        conn.execute(
            "INSERT INTO consumer_projection "
            "(consumer_name, source_id, epoch_id, account_id, money_currency, updated_at) "
            "VALUES (?, ?, ?, ?, NULL, ?) ON CONFLICT DO NOTHING",
            (consumer_name, source_id, epoch_id, account_id, to_wire(now)),
        )
        row = conn.execute(
            "SELECT money_currency, deal_count, realized_pnl_gross, commission, swap, "
            "unproven_currency_deal_count FROM consumer_projection "
            "WHERE consumer_name = ? AND source_id = ? AND epoch_id = ? AND account_id = ?",
            (consumer_name, source_id, epoch_id, account_id),
        ).fetchone()

        if currency.status is CurrencyStatus.UNPROVEN:
            conn.execute(
                "UPDATE consumer_projection SET deal_count = deal_count + 1, "
                "unproven_currency_deal_count = unproven_currency_deal_count + 1, "
                "last_event_id = ?, updated_at = ? "
                "WHERE consumer_name = ? AND source_id = ? AND epoch_id = ? AND account_id = ?",
                (str(event_id), to_wire(now), consumer_name, source_id, epoch_id, account_id),
            )
            return

        stored_currency = row["money_currency"]
        if stored_currency is not None and stored_currency != currency.code:
            raise IntegrityIncident(
                f"account {account_id!r} already has totals in {stored_currency}; this Deal "
                f"reports {currency.code}. Summing two currencies would produce a figure that "
                "is not money, so the aggregate stops instead."
            )
        conn.execute(
            "UPDATE consumer_projection SET money_currency = ?, deal_count = deal_count + 1, "
            "realized_pnl_gross = ?, commission = ?, swap = ?, last_event_id = ?, updated_at = ? "
            "WHERE consumer_name = ? AND source_id = ? AND epoch_id = ? AND account_id = ?",
            (
                currency.code,
                add_money(row["realized_pnl_gross"], payload["realized_pnl_gross"], field="realized_pnl_gross"),
                add_money(row["commission"], payload["commission"], field="commission"),
                add_money(row["swap"], payload["swap"], field="swap"),
                str(event_id),
                to_wire(now),
                consumer_name,
                source_id,
                epoch_id,
                account_id,
            ),
        )

    def record_snapshot_notice(
        self,
        conn: sqlite3.Connection,
        *,
        consumer_name: str,
        source_id: str,
        epoch_id: str,
        payload: dict[str, Any],
        now: datetime,
    ) -> None:
        conn.execute(
            "INSERT INTO consumer_snapshot_notices "
            "(consumer_name, source_id, epoch_id, entity, scan_id, snapshot_seq, record_count, noticed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
            (
                consumer_name,
                source_id,
                epoch_id,
                payload["entity"],
                payload["scan_id"],
                payload["snapshot_seq"],
                payload["record_count"],
                to_wire(now),
            ),
        )

    def projection(self, consumer_name: str, source_id: str, epoch_id: str) -> list[ProjectionRow]:
        return [
            ProjectionRow(
                account_id=row["account_id"],
                money_currency=row["money_currency"],
                deal_count=int(row["deal_count"]),
                realized_pnl_gross=row["realized_pnl_gross"],
                commission=row["commission"],
                swap=row["swap"],
                unproven_currency_deal_count=int(row["unproven_currency_deal_count"]),
            )
            for row in self.conn.execute(
                "SELECT account_id, money_currency, deal_count, realized_pnl_gross, commission, "
                "swap, unproven_currency_deal_count FROM consumer_projection "
                "WHERE consumer_name = ? AND source_id = ? AND epoch_id = ? ORDER BY account_id",
                (consumer_name, source_id, epoch_id),
            )
        ]

    def snapshot_notices(self, consumer_name: str) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT source_id, epoch_id, entity, scan_id, snapshot_seq, record_count "
                "FROM consumer_snapshot_notices WHERE consumer_name = ? "
                "ORDER BY epoch_id, entity, scan_id",
                (consumer_name,),
            )
        )

    # --- quarantine ---------------------------------------------------------
    def quarantine(
        self,
        *,
        consumer_name: str,
        topic: str,
        partition: int,
        offset: int,
        error_code: str,
        reason: str,
        now: datetime,
        event_id: str | None = None,
        source_id: str | None = None,
        epoch_id: str | None = None,
        aggregate_key: str | None = None,
        aggregate_version: int | None = None,
        payload_sha256: str | None = None,
    ) -> str:
        """Park one record, keyed by its position so re-reads are idempotent.

        ``reason`` is a sanitised message, never a payload: design section 10.4
        keeps raw account detail out of DLQ and quarantine records.
        """
        quarantine_id = f"{topic}:{partition}:{offset}"
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO consumer_quarantine "
                "(consumer_name, quarantine_id, event_id, topic, kafka_partition, kafka_offset, "
                " source_id, epoch_id, aggregate_key, aggregate_version, error_code, reason, "
                " payload_sha256, quarantined_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (consumer_name, quarantine_id) DO NOTHING",
                (
                    consumer_name,
                    quarantine_id,
                    event_id,
                    topic,
                    partition,
                    offset,
                    source_id,
                    epoch_id,
                    aggregate_key,
                    aggregate_version,
                    error_code,
                    reason[:500],
                    payload_sha256,
                    to_wire(now),
                ),
            )
        return quarantine_id

    def quarantined(self, consumer_name: str) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT quarantine_id, event_id, topic, kafka_partition, kafka_offset, error_code, "
                "reason, aggregate_key FROM consumer_quarantine WHERE consumer_name = ? "
                "ORDER BY topic, kafka_partition, kafka_offset",
                (consumer_name,),
            )
        )

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        return list(self.conn.execute(sql, tuple(params)))

    def dump_projection_json(self, consumer_name: str) -> str:
        rows = self.conn.execute(
            "SELECT source_id, epoch_id, account_id, money_currency, deal_count, "
            "realized_pnl_gross, commission, swap, unproven_currency_deal_count "
            "FROM consumer_projection WHERE consumer_name = ? ORDER BY epoch_id, account_id",
            (consumer_name,),
        )
        return json.dumps([dict(row) for row in rows], indent=2, sort_keys=True)
