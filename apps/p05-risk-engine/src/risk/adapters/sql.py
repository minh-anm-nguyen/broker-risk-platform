"""SQL execution port with a SQLite and a PostgreSQL adapter.

The reader is written once, against a port, for a practical reason: the
correctness properties that matter here -- row-set identity, watermark
scoping, bounded streaming, parameterised queries, read-only transactions --
are all testable on a relational engine that needs no server. SQLite provides
that, so the integration suite exercises real SQL rather than a hand-rolled
fake. PostgreSQL is the production target and gets the stronger isolation
guarantee it is the only one of the two able to make.

Honest limits of the substitution:

* SQLite's ``BEGIN DEFERRED`` gives a stable read view of its own, but it is
  not PostgreSQL MVCC. Tests that depend on ``REPEATABLE READ`` semantics
  specifically are marked ``needs_postgres`` and skip without a DSN.
* SQLite has no roles, so the "cannot UPDATE deals" guarantee is enforced two
  ways: structurally here (the read-only executor refuses non-SELECT text) and
  by a GRANT-based test against PostgreSQL when one is available.

Every query is parameterised. Not one SQL string in this package is built by
concatenating a caller-supplied value (WF5-198).
"""

from __future__ import annotations

import re
import sqlite3
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Mapping, Protocol, runtime_checkable

from risk.errors import ReasonCode, RiskError

__all__ = [
    "SqlExecutor",
    "SqliteExecutor",
    "PostgresExecutor",
    "ReadOnlyViolation",
    "to_psycopg_params",
    "assert_read_only",
]

_PARAM_RE = re.compile(r":([a-zA-Z_][a-zA-Z0-9_]*)")

#: Statement prefixes a read-only executor will run. Anything else is refused
#: before it reaches the driver, so a mistake in this package cannot mutate
#: upstream facts even when the database role would allow it (INV05-13).
_READ_ONLY_PREFIXES = ("select", "with", "explain", "pragma")


class ReadOnlyViolation(RiskError):
    """A non-SELECT statement was submitted through a read-only executor."""

    def __init__(self, statement_head: str) -> None:
        super().__init__(
            ReasonCode.UNAUTHORIZED_SCOPE,
            "read-only executor refused a mutating statement",
            statement_head=statement_head,
        )


def assert_read_only(sql: str) -> None:
    """Refuse anything that is not a read."""
    stripped = sql.lstrip()
    while stripped.startswith("--"):
        _, _, stripped = stripped.partition("\n")
        stripped = stripped.lstrip()
    head = stripped[:16].lower()
    if not head.startswith(_READ_ONLY_PREFIXES):
        raise ReadOnlyViolation(stripped[:24])
    # A trailing statement after a semicolon would slip past a prefix check.
    body = stripped.rstrip().rstrip(";")
    if ";" in body:
        raise ReadOnlyViolation("multi-statement")


def to_psycopg_params(sql: str) -> str:
    """Translate ``:name`` placeholders into psycopg's ``%(name)s`` form."""
    return _PARAM_RE.sub(lambda m: f"%({m.group(1)})s", sql)


@runtime_checkable
class SqlExecutor(Protocol):
    """Minimal read-only SQL surface the readers depend on."""

    @property
    def dialect(self) -> str: ...

    async def fetch_all(
        self, sql: str, params: Mapping[str, Any] | None = None
    ) -> list[dict[str, Any]]: ...

    async def fetch_one(
        self, sql: str, params: Mapping[str, Any] | None = None
    ) -> dict[str, Any] | None: ...

    def stream(
        self, sql: str, params: Mapping[str, Any] | None = None, *, batch_size: int = 1000
    ) -> AsyncIterator[tuple[dict[str, Any], ...]]: ...

    def read_snapshot(self) -> Any: ...


@dataclass
class _Stats:
    """Observability counters, also used by tests as a query oracle."""

    queries: int = 0
    rows: int = 0
    batches: int = 0
    transactions: int = 0


class SqliteExecutor:
    """Read-only executor over a SQLite connection.

    The connection is configured so that numbers come back as text where the
    schema stores them as text, which keeps money exact end to end. SQLite's
    type affinity would happily hand back a float for a column declared
    ``NUMERIC``; storing money as ``TEXT`` in the harness schema avoids that
    entirely.
    """

    dialect = "sqlite"

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._conn = connection
        self._conn.row_factory = sqlite3.Row
        self.stats = _Stats()
        self._in_snapshot = False

    @classmethod
    def in_memory(cls) -> SqliteExecutor:
        conn = sqlite3.connect(":memory:")
        conn.execute("PRAGMA foreign_keys = ON")
        return cls(conn)

    def _execute(self, sql: str, params: Mapping[str, Any] | None) -> sqlite3.Cursor:
        assert_read_only(sql)
        self.stats.queries += 1
        return self._conn.execute(sql, dict(params or {}))

    async def fetch_all(self, sql: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        cursor = self._execute(sql, params)
        rows = [dict(r) for r in cursor.fetchall()]
        self.stats.rows += len(rows)
        return rows

    async def fetch_one(
        self, sql: str, params: Mapping[str, Any] | None = None
    ) -> dict[str, Any] | None:
        cursor = self._execute(sql, params)
        row = cursor.fetchone()
        if row is None:
            return None
        self.stats.rows += 1
        return dict(row)

    async def stream(
        self, sql: str, params: Mapping[str, Any] | None = None, *, batch_size: int = 1000
    ) -> AsyncIterator[tuple[dict[str, Any], ...]]:
        """Yield fixed-size batches without materialising the whole result."""
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        cursor = self._execute(sql, params)
        while True:
            chunk = cursor.fetchmany(batch_size)
            if not chunk:
                break
            self.stats.rows += len(chunk)
            self.stats.batches += 1
            yield tuple(dict(r) for r in chunk)

    @asynccontextmanager
    async def read_snapshot(self) -> AsyncIterator[SqliteExecutor]:
        """Open a deferred read transaction.

        SQLite's snapshot isolation is weaker than PostgreSQL's REPEATABLE
        READ, and this method does not pretend otherwise; it exists so the
        reader has one code path and so connection cleanup is tested.
        """
        self.stats.transactions += 1
        self._conn.execute("BEGIN DEFERRED")
        self._in_snapshot = True
        try:
            yield self
        finally:
            self._in_snapshot = False
            # Always rollback: a read transaction has nothing to commit, and
            # rolling back proves nothing was written (WF5-325).
            self._conn.rollback()

    def close(self) -> None:
        self._conn.close()


class PostgresExecutor:
    """Read-only executor over psycopg 3, using REPEATABLE READ snapshots.

    ``REPEATABLE READ`` is what lets a single capture read accounts, positions
    and deals at one database revision, so a concurrent Project 03 publish
    cannot be observed half-applied (WF5-217). It does *not* make the quote
    provider or the Project 04 database agree with that instant -- that is what
    the cut vector is for.
    """

    dialect = "postgresql"

    def __init__(self, connection: Any) -> None:
        self._conn = connection
        self.stats = _Stats()

    @classmethod
    async def connect(cls, dsn: str) -> PostgresExecutor:
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RiskError(
                ReasonCode.DB_UNAVAILABLE, "psycopg is not installed; install the postgres extra"
            ) from exc
        try:
            conn = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
        except Exception as exc:  # pragma: no cover - needs a live server
            raise RiskError(ReasonCode.DB_UNAVAILABLE, "could not connect to PostgreSQL") from exc
        return cls(conn)

    async def _cursor(self, sql: str, params: Mapping[str, Any] | None) -> Any:
        assert_read_only(sql)
        self.stats.queries += 1
        from psycopg.rows import dict_row

        cursor = self._conn.cursor(row_factory=dict_row)
        await cursor.execute(to_psycopg_params(sql), dict(params or {}))
        return cursor

    async def fetch_all(self, sql: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        cursor = await self._cursor(sql, params)
        rows = await cursor.fetchall()
        self.stats.rows += len(rows)
        return list(rows)

    async def fetch_one(
        self, sql: str, params: Mapping[str, Any] | None = None
    ) -> dict[str, Any] | None:
        cursor = await self._cursor(sql, params)
        row = await cursor.fetchone()
        if row is not None:
            self.stats.rows += 1
        return row

    async def stream(
        self, sql: str, params: Mapping[str, Any] | None = None, *, batch_size: int = 1000
    ) -> AsyncIterator[tuple[dict[str, Any], ...]]:
        cursor = await self._cursor(sql, params)
        while True:
            chunk = await cursor.fetchmany(batch_size)
            if not chunk:
                break
            self.stats.rows += len(chunk)
            self.stats.batches += 1
            yield tuple(chunk)

    @asynccontextmanager
    async def read_snapshot(self) -> AsyncIterator[PostgresExecutor]:
        self.stats.transactions += 1
        await self._conn.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        try:
            yield self
        finally:
            # Rolling back a read-only transaction releases the snapshot
            # promptly instead of holding it for the pool's idle timeout.
            await self._conn.rollback()

    async def close(self) -> None:
        await self._conn.close()


def apply_script(connection: sqlite3.Connection, script: str) -> None:
    """Run a DDL/seed script against a SQLite harness database.

    Separate from the read-only executor on purpose: schema setup is a
    privileged operation and must not share a code path with query execution.
    """
    connection.executescript(script)
    connection.commit()

