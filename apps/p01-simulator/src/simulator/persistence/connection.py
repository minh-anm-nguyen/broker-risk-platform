"""SQLite connection management: PRAGMAs, the read pool, and the process lock.

Two rules shape this module:

* **One writer process.** ``seq`` allocation and the single-writer queue are
  only correct with one process attached to a database. An advisory lock file
  makes a second process fail loudly at startup instead of corrupting the
  sequence (WF-120).
* **Short-lived reads.** A long-running read transaction prevents WAL
  checkpointing and grows the WAL without bound (WF-084). Snapshot consistency
  here comes from the ``seq`` predicate in the query, not from holding a
  transaction open across pages, so reads never need one.
"""

from __future__ import annotations

import queue
import sqlite3
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from ..settings import ConfigurationError, Settings
from .migrations import migrate


def _apply_pragmas(conn: sqlite3.Connection, settings: Settings, *, writable: bool) -> None:
    # WAL lets readers run concurrently with the single writer, which is the
    # entire reason a one-writer design can still serve 100 req/s of reads.
    if writable:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(f"PRAGMA synchronous={settings.sqlite_synchronous}")
    conn.execute(f"PRAGMA busy_timeout={settings.sqlite_busy_timeout_ms}")
    # Enforced per connection, not per database: without this the declared
    # foreign keys are documentation rather than constraints.
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-20000")  # ~20 MiB page cache


def open_connection(
    settings: Settings,
    *,
    writable: bool = True,
    check_same_thread: bool = True,
) -> sqlite3.Connection:
    """Open a connection with manual transaction control.

    ``isolation_level=None`` disables the driver's implicit transaction
    handling so the writer can issue ``BEGIN IMMEDIATE`` itself and know
    exactly where its transaction starts.
    """
    path = settings.db_path
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(path),
        timeout=settings.sqlite_busy_timeout_ms / 1000.0,
        isolation_level=None,
        check_same_thread=check_same_thread,
    )
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn, settings, writable=writable)
    return conn


def initialize_database(settings: Settings) -> int:
    """Migrate the database and return the schema version in force.

    A damaged file fails startup closed, with a message naming the file. The
    raw ``sqlite3.DatabaseError`` would be accurate but leaves an operator
    guessing which path to look at during the one incident where that matters
    most (WF-082).
    """
    try:
        conn = open_connection(settings)
    except sqlite3.DatabaseError as exc:
        raise RuntimeError(
            f"SQLite refused to open {settings.db_path}: {exc}. "
            "Preserve the file and its WAL for forensics before attempting any repair."
        ) from exc
    try:
        version = migrate(conn)
        if settings.integrity_check_on_start:
            verify_integrity(conn)
        return version
    except sqlite3.DatabaseError as exc:
        raise RuntimeError(
            f"SQLite integrity_check failed for {settings.db_path}: {exc}. "
            "Preserve the file and its WAL for forensics before attempting any repair."
        ) from exc
    finally:
        conn.close()


def verify_integrity(conn: sqlite3.Connection) -> None:
    """Run ``PRAGMA integrity_check`` and raise on anything but ``ok``.

    SQLite reports damage in two different ways: it returns rows describing
    the problems, or - when the file is damaged badly enough that it cannot
    even walk the pages - it raises ``DatabaseError``. Both mean the same
    thing to a caller, so both become one error here.

    Not run on every start by default: it reads the whole database and would
    blow the 30 s restart-readiness target on a large dataset (section 4.3).
    """
    try:
        rows = conn.execute("PRAGMA integrity_check").fetchall()
    except sqlite3.DatabaseError as exc:
        raise RuntimeError(f"SQLite integrity_check failed: {exc}") from exc
    problems = [row[0] for row in rows if row[0] != "ok"]
    if problems:
        raise RuntimeError("SQLite integrity_check failed: " + "; ".join(problems[:10]))


# ---------------------------------------------------------------------------
# Single-process advisory lock
# ---------------------------------------------------------------------------


class ProcessLock:
    """Exclusive advisory lock on a file next to the database.

    SQLite would let a second process attach and interleave writes. It would
    not corrupt the file, but it would break ``seq`` contiguity and the
    single-active-run model, which is worse: the data would look fine.
    """

    __slots__ = ("_path", "_handle")

    def __init__(self, path: Path) -> None:
        self._path = path
        self._handle: Any = None

    def acquire(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        handle = open(self._path, "a+b")
        try:
            if sys.platform == "win32":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise ConfigurationError(
                f"Another simulator process already owns {self._path}. "
                "Project 01 supports a single writer process per database."
            ) from exc
        self._handle = handle

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        self._handle = None
        try:
            if sys.platform == "win32":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except OSError:  # pragma: no cover - releasing a dead lock is harmless
            pass
        finally:
            handle.close()


# ---------------------------------------------------------------------------
# Bounded read pool
# ---------------------------------------------------------------------------


class ReadConnectionPool:
    """Fixed-size pool of read connections handed to worker threads.

    Bounded on purpose: this is the knob that stops 500 concurrent readers from
    opening 500 file handles and 500 page caches (WF-092). A caller that cannot
    get a connection within its budget is rejected, not queued forever.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._size = settings.read_pool_size
        self._pool: queue.LifoQueue[sqlite3.Connection] = queue.LifoQueue(maxsize=self._size)
        self._created = 0
        self._lock = threading.Lock()
        self._closed = False

    @property
    def size(self) -> int:
        return self._size

    def _new_connection(self) -> sqlite3.Connection:
        # Handed across threads by design, so the driver's same-thread check
        # must be off; the pool itself guarantees one thread at a time.
        return open_connection(self._settings, writable=False, check_same_thread=False)

    @contextmanager
    def acquire(self, timeout: float | None = None) -> Iterator[sqlite3.Connection]:
        if self._closed:
            raise RuntimeError("read pool is closed")
        conn: sqlite3.Connection | None = None
        try:
            conn = self._pool.get_nowait()
        except queue.Empty:
            with self._lock:
                if self._created < self._size:
                    self._created += 1
                    conn = self._new_connection()
        if conn is None:
            try:
                conn = self._pool.get(timeout=timeout)
            except queue.Empty as exc:
                raise TimeoutError("no read connection available within budget") from exc
        try:
            yield conn
        finally:
            if self._closed:
                conn.close()
            else:
                # A connection left mid-transaction would hold a read snapshot
                # and block WAL checkpointing for every other caller.
                if conn.in_transaction:
                    conn.rollback()
                self._pool.put(conn)

    def close(self) -> None:
        self._closed = True
        while True:
            try:
                self._pool.get_nowait().close()
            except queue.Empty:
                return
