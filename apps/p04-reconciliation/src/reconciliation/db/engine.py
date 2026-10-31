"""Connection management, role separation and transaction boundaries.

Design rules enforced here (SYSTEM_DESIGN sections 3.1, 6.2, 7.3):

* **The reconciliation role cannot write Project 03's tables.** This is not a
  code-review convention: a SQLite authoriser denies INSERT/UPDATE/DELETE on
  every table in ``P03_OWNED_TABLES`` at the engine level, so an attempt
  raises ``sqlite3.DatabaseError: not authorized`` even from hand-written SQL.
  INV04-07 and INV04-18 are therefore testable facts, not promises.

  The reference PostgreSQL deployment expresses the same thing with
  ``REVOKE``/``GRANT`` on a dedicated role (see ``database/migrations``). The
  authoriser is this build's equivalent of that grant, not a substitute for
  it in production.

* **Target capture holds exactly one transaction.** Section 7.3 requires a
  consistent database cut, and warns specifically against reading a mutable
  projection across several ``READ COMMITTED`` statements while the publisher
  promotes a new scan. Two strategies are offered here and both fail closed:

  - ``IMMEDIATE`` takes the write lock before reading, so no publish can
    interleave. A publisher that tries gets ``SQLITE_BUSY`` and retries.
  - ``DEFERRED_SNAPSHOT`` pins a read snapshot and lets the publisher commit;
    the capture's own write then fails with ``SQLITE_BUSY_SNAPSHOT``, which is
    reported as ``TARGET_CAPTURE_CONFLICT``. A stale-but-consistent capture is
    never promoted to ``target_complete``.

  Either way the captured rows and the P03 metadata read beside them belong to
  exactly one published scan, which is what IT04-07 asserts.

* **Foreign keys are on.** SQLite disables them per connection by default, and
  the receipt/item relationship in section 7.4 is one of the few structural
  guarantees the storage layer can give us for free.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from enum import StrEnum
from pathlib import Path
from typing import Any, Final

from ..errors import PrivilegeError, ReasonCode, TargetCaptureError, TargetConflictError
from .schema import ALL_DDL, P03_OWNED_TABLES, P04_OWNED_TABLES


class Role(StrEnum):
    """Who is holding the connection.

    ``RECON`` is the service's own role and the only one used in production
    code paths. ``P03_SYNC`` belongs to the sync-service harness that owns the
    synced tables. ``MIGRATOR`` exists solely to create the schema.
    """

    RECON = "RECON"
    P03_SYNC = "P03_SYNC"
    MIGRATOR = "MIGRATOR"


#: Writes are denied to these tables for the given role. Reads stay allowed:
#: the whole point of the audit is to read P03's state (section 3.2).
_DENIED_WRITES: Final[dict[Role, frozenset[str]]] = {
    Role.RECON: P03_OWNED_TABLES,
    Role.P03_SYNC: P04_OWNED_TABLES,
    Role.MIGRATOR: frozenset(),
}

_WRITE_ACTIONS: Final[frozenset[int]] = frozenset(
    {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE}
)

#: Schema-level operations. Only the migrator may perform them; a service that
#: can DROP the table it is denied writes on has no privilege separation at
#: all.
_SCHEMA_ACTIONS: Final[frozenset[int]] = frozenset(
    {
        sqlite3.SQLITE_CREATE_TABLE,
        sqlite3.SQLITE_CREATE_INDEX,
        sqlite3.SQLITE_DROP_TABLE,
        sqlite3.SQLITE_DROP_INDEX,
        sqlite3.SQLITE_ALTER_TABLE,
    }
)


class CaptureStrategy(StrEnum):
    IMMEDIATE = "IMMEDIATE"
    DEFERRED_SNAPSHOT = "DEFERRED_SNAPSHOT"


def _authorizer_for(role: Role):
    denied = _DENIED_WRITES[role]

    def authorize(action: int, arg1: str | None, arg2: str | None, dbname: str | None, source: str | None) -> int:
        if action in _WRITE_ACTIONS and arg1 in denied:
            return sqlite3.SQLITE_DENY
        if action in _SCHEMA_ACTIONS and role is not Role.MIGRATOR:
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    return authorize


class Database:
    """Factory for role-scoped connections against one database file."""

    def __init__(
        self,
        path: Path | str,
        *,
        busy_timeout_ms: int = 5_000,
        synchronous: str = "FULL",
    ) -> None:
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms
        self.synchronous = synchronous

    # ---- lifecycle -------------------------------------------------------

    def create_schema(self) -> None:
        """Create every table. Idempotent, and the only MIGRATOR use."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect(Role.MIGRATOR) as conn:
            for statement in ALL_DDL:
                conn.execute(statement)
            conn.commit()

    def connect(self, role: Role) -> sqlite3.Connection:
        """Open a connection whose privileges match ``role``.

        ``isolation_level=None`` turns off the driver's implicit transaction
        handling so this module controls every ``BEGIN`` explicitly. Section
        7.3 depends on knowing exactly when a transaction starts.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            self.path,
            isolation_level=None,
            timeout=self.busy_timeout_ms / 1000.0,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(f"PRAGMA synchronous={self.synchronous}")
        conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        conn.execute("PRAGMA foreign_keys=ON")
        # Installed last so the PRAGMAs above are not themselves authorised.
        conn.set_authorizer(_authorizer_for(role))
        return conn


# ---------------------------------------------------------------------------
# Transaction helpers
# ---------------------------------------------------------------------------


@contextmanager
def transaction(conn: sqlite3.Connection, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
    """Run a block in one transaction, rolling back on any exception.

    Page receipts use this with ``immediate=True``: section 7.4 step 4 puts
    the receipt and its items in the same transaction and commits before the
    next page is fetched, so a crash can never leave items without the receipt
    that vouches for them.
    """
    conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN DEFERRED")
    try:
        yield conn
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:  # pragma: no cover - rollback of a dead tx
            pass
        raise
    else:
        conn.execute("COMMIT")


@contextmanager
def capture_transaction(
    conn: sqlite3.Connection,
    *,
    strategy: CaptureStrategy = CaptureStrategy.IMMEDIATE,
) -> Iterator[sqlite3.Connection]:
    """One consistent database cut for a target capture (section 7.3).

    Raises ``TargetConflictError`` if the cut could not be proven, which the
    caller must turn into ``INCONCLUSIVE``/``TARGET_CAPTURE_CONFLICT`` rather
    than a partial manifest (IT04-07, INV04-14).
    """
    if strategy is CaptureStrategy.IMMEDIATE:
        try:
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            raise TargetConflictError(
                "could not acquire an exclusive cut for target capture",
                detail=str(exc),
            ) from exc
    else:
        conn.execute("BEGIN DEFERRED")
        # Force the read snapshot to be pinned now rather than at the first
        # business read, so the window the test exercises is well defined.
        conn.execute("SELECT 1 FROM sqlite_schema LIMIT 1").fetchone()

    try:
        yield conn
    except sqlite3.OperationalError as exc:
        # Under DEFERRED_SNAPSHOT the refusal arrives on the first *write*
        # inside the block, not at COMMIT: SQLite will not let a transaction
        # holding a stale read snapshot upgrade to a writer. That is the
        # fail-closed behaviour this strategy exists for, so it is classified
        # here rather than escaping as a bare driver error (IT04-07).
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:  # pragma: no cover
            pass
        classified = classify_sqlite_error(exc)
        if isinstance(classified, TargetConflictError):
            raise classified from exc
        raise
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:  # pragma: no cover
            pass
        raise

    try:
        conn.execute("COMMIT")
    except sqlite3.OperationalError as exc:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:  # pragma: no cover
            pass
        # SQLITE_BUSY_SNAPSHOT: the target moved while we held a read
        # snapshot. Committing anything derived from it would be a capture of
        # a cut that no longer exists.
        raise TargetConflictError(
            "target changed during capture; the cut could not be proven",
            detail=str(exc),
            sqlite_error=getattr(exc, "sqlite_errorname", None),
        ) from exc


def classify_sqlite_error(exc: sqlite3.Error) -> TargetCaptureError:
    """Map a driver error onto the taxonomy in section 5.6."""
    name = getattr(exc, "sqlite_errorname", "") or ""
    text = str(exc)
    if "not authorized" in text:
        return PrivilegeError(
            "the reconciliation role is not permitted to write this table",
            detail=text,
        )
    if "BUSY_SNAPSHOT" in name or "SNAPSHOT" in name:
        return TargetConflictError("target snapshot became stale", detail=text)
    if "BUSY" in name or "locked" in text:
        return TargetConflictError("target was locked by a concurrent writer", detail=text)
    return TargetCaptureError("database error during capture", reason=ReasonCode.TARGET_DB_ERROR, detail=text)


def fetch_one(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
    return conn.execute(sql, params).fetchone()


def fetch_scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> Any:
    row = conn.execute(sql, params).fetchone()
    return None if row is None else row[0]
