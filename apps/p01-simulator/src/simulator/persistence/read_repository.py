"""Snapshot reads with keyset pagination.

Two decisions carry most of the weight here.

**Keyset, not OFFSET.** ``LIMIT 100 OFFSET 900000`` makes SQLite walk 900k rows
to discard them. Paging a million deals that way is quadratic, and the
symptoms arrive late - page 1 is instant and page 9000 times out (WF-045).
Every listing therefore orders by an indexed key and continues from the last
key it returned.

**Snapshot consistency comes from the ``seq`` predicate, not from a
transaction.** A reader filters ``source_seq <= S`` on immutable deals, and
``valid_from_seq <= S < valid_to_seq`` on versioned projections. That is why a
writer can keep committing while a 100-page scan runs and the scan still sees
exactly the state at ``S`` (WF-032, WF-033) - and why no read ever needs to
hold a transaction open and stall WAL checkpointing (WF-084).

Filtering a *current-state* table by ``updated_seq <= S`` would not be enough,
and this is the trap the versioned design exists to avoid: once a row is
updated after ``S``, its pre-``S`` value is gone and the scan silently loses
it.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from ..domain.models import Account, Deal, Mark, Position, ServiceNotReadyError
from ..settings import Settings
from . import event_store as es
from .connection import ReadConnectionPool

T = TypeVar("T")

_ASOF = "valid_from_seq <= ? AND (valid_to_seq IS NULL OR valid_to_seq > ?)"

_DEAL_COLUMNS = (
    "deal_id, source_seq, position_id, account_id, symbol, entry, side, volume_dec, "
    "price_dec, gross_pnl_dec, commission_dec, swap_dec, executed_at"
)
_ACCOUNT_COLUMNS = "account_id, currency, balance_dec, credit_dec"
_POSITION_COLUMNS = (
    "position_id, account_id, symbol, side, volume_dec, open_price_dec, status"
)
_MARK_COLUMNS = "symbol, bid_dec, ask_dec, market_time"


@dataclass(frozen=True, slots=True)
class Page(Generic[T]):
    """One page plus the key to continue from.

    ``last_key is None`` means this snapshot has no further rows. It does *not*
    mean no more data will ever exist - a later snapshot may well have more,
    which is why the API documents it that way (section 5.1).
    """

    items: list[T]
    last_key: list[Any] | None


class ReadRepository:
    def __init__(self, pool: ReadConnectionPool, settings: Settings) -> None:
        self._pool = pool
        self._settings = settings
        self._pool_timeout = max(0.5, settings.sqlite_busy_timeout_ms / 1000.0)

    def _acquire(self):
        return self._pool.acquire(timeout=self._pool_timeout)

    # -- run metadata ------------------------------------------------------

    def active_run(self) -> es.RunSummary | None:
        with self._guard():
            with self._acquire() as conn:
                run_id = es.load_active_run_id(conn)
                return es.load_run_summary(conn, run_id) if run_id else None

    def run(self, run_id: str) -> es.RunSummary | None:
        with self._guard():
            with self._acquire() as conn:
                return es.load_run_summary(conn, run_id)

    def _guard(self):
        return _PoolGuard()

    # -- accounts ----------------------------------------------------------

    def get_account(self, *, run_id: str, account_id: str, as_of_seq: int) -> Account | None:
        with self._guard(), self._acquire() as conn:
            row = conn.execute(
                f"SELECT {_ACCOUNT_COLUMNS} FROM account_versions "
                f"WHERE run_id = ? AND account_id = ? AND {_ASOF}",
                (run_id, account_id, as_of_seq, as_of_seq),
            ).fetchone()
            return es.account_from_row(row) if row is not None else None

    def list_accounts(
        self,
        *,
        run_id: str,
        snapshot_seq: int,
        last_key: list[Any] | None = None,
        limit: int = 100,
    ) -> Page[Account]:
        clauses = ["run_id = ?", _ASOF]
        params: list[Any] = [run_id, snapshot_seq, snapshot_seq]
        if last_key:
            clauses.append("account_id > ?")
            params.append(str(last_key[0]))
        sql = (
            f"SELECT {_ACCOUNT_COLUMNS} FROM account_versions WHERE "
            + " AND ".join(clauses)
            + " ORDER BY account_id LIMIT ?"
        )
        params.append(limit + 1)
        with self._guard(), self._acquire() as conn:
            rows = conn.execute(sql, params).fetchall()
        return self._paginate(rows, limit, es.account_from_row, lambda row: [row["account_id"]])

    # -- positions ---------------------------------------------------------

    def list_positions(
        self,
        *,
        run_id: str,
        snapshot_seq: int,
        account_id: str | None = None,
        symbol: str | None = None,
        status: str | None = None,
        last_key: list[Any] | None = None,
        limit: int = 100,
    ) -> Page[Position]:
        clauses = ["run_id = ?", _ASOF]
        params: list[Any] = [run_id, snapshot_seq, snapshot_seq]
        if account_id:
            clauses.append("account_id = ?")
            params.append(account_id)
        if symbol:
            clauses.append("symbol = ?")
            params.append(symbol)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if last_key:
            clauses.append("position_id > ?")
            params.append(str(last_key[0]))
        sql = (
            f"SELECT {_POSITION_COLUMNS} FROM position_versions WHERE "
            + " AND ".join(clauses)
            + " ORDER BY position_id LIMIT ?"
        )
        params.append(limit + 1)
        with self._guard(), self._acquire() as conn:
            rows = conn.execute(sql, params).fetchall()
        return self._paginate(rows, limit, es.position_from_row, lambda row: [row["position_id"]])

    def get_position(
        self, *, run_id: str, position_id: str, as_of_seq: int
    ) -> Position | None:
        with self._guard(), self._acquire() as conn:
            row = conn.execute(
                f"SELECT {_POSITION_COLUMNS} FROM position_versions "
                f"WHERE run_id = ? AND position_id = ? AND {_ASOF}",
                (run_id, position_id, as_of_seq, as_of_seq),
            ).fetchone()
            return es.position_from_row(row) if row is not None else None

    # -- deals -------------------------------------------------------------

    def list_deals(
        self,
        *,
        run_id: str,
        snapshot_seq: int,
        account_id: str | None = None,
        symbol: str | None = None,
        from_time_ms: int | None = None,
        to_time_ms: int | None = None,
        after_seq: int | None = None,
        last_key: list[Any] | None = None,
        limit: int = 100,
    ) -> Page[Deal]:
        clauses, params = self._deal_filter(
            run_id=run_id,
            snapshot_seq=snapshot_seq,
            account_id=account_id,
            symbol=symbol,
            from_time_ms=from_time_ms,
            to_time_ms=to_time_ms,
            after_seq=after_seq,
        )
        if last_key:
            # Composite keyset: (source_seq, deal_id) is unique and indexed, so
            # this resumes in one seek rather than a scan.
            clauses.append("(source_seq > ? OR (source_seq = ? AND deal_id > ?))")
            params.extend([int(last_key[0]), int(last_key[0]), str(last_key[1])])
        sql = (
            f"SELECT {_DEAL_COLUMNS} FROM deals WHERE "
            + " AND ".join(clauses)
            + " ORDER BY source_seq, deal_id LIMIT ?"
        )
        params.append(limit + 1)
        with self._guard(), self._acquire() as conn:
            rows = conn.execute(sql, params).fetchall()
        return self._paginate(
            rows, limit, es.deal_from_row, lambda row: [int(row["source_seq"]), row["deal_id"]]
        )

    def deal_identity_set(
        self,
        *,
        run_id: str,
        snapshot_seq: int,
        account_id: str | None = None,
        symbol: str | None = None,
        from_time_ms: int | None = None,
        to_time_ms: int | None = None,
        after_seq: int | None = None,
    ) -> set[str]:
        """Full deal-id set for a snapshot, used as the pagination oracle.

        INV-11 asks whether a paged scan returned exactly this set: no missing
        row and no duplicate. Answering that needs a reference computed without
        pagination, which is what this is for.
        """
        clauses, params = self._deal_filter(
            run_id=run_id,
            snapshot_seq=snapshot_seq,
            account_id=account_id,
            symbol=symbol,
            from_time_ms=from_time_ms,
            to_time_ms=to_time_ms,
            after_seq=after_seq,
        )
        sql = "SELECT deal_id FROM deals WHERE " + " AND ".join(clauses)
        with self._guard(), self._acquire() as conn:
            return {row["deal_id"] for row in conn.execute(sql, params)}

    @staticmethod
    def _deal_filter(
        *,
        run_id: str,
        snapshot_seq: int,
        account_id: str | None,
        symbol: str | None,
        from_time_ms: int | None,
        to_time_ms: int | None,
        after_seq: int | None,
    ) -> tuple[list[str], list[Any]]:
        clauses = ["run_id = ?", "source_seq <= ?"]
        params: list[Any] = [run_id, snapshot_seq]
        if after_seq is not None:
            # Exclusive, per section 5.1: a client that just processed seq N
            # must not be handed seq N again.
            clauses.append("source_seq > ?")
            params.append(after_seq)
        if account_id:
            clauses.append("account_id = ?")
            params.append(account_id)
        if symbol:
            clauses.append("symbol = ?")
            params.append(symbol)
        # Time range is [from, to): inclusive start, exclusive end, so adjacent
        # windows tile without double counting a boundary deal (WF-041).
        if from_time_ms is not None:
            clauses.append("executed_at_ms >= ?")
            params.append(from_time_ms)
        if to_time_ms is not None:
            clauses.append("executed_at_ms < ?")
            params.append(to_time_ms)
        return clauses, params

    # -- marks -------------------------------------------------------------

    def list_marks(
        self,
        *,
        run_id: str,
        snapshot_seq: int,
        symbols: tuple[str, ...] | None = None,
        last_key: list[Any] | None = None,
        limit: int = 100,
    ) -> Page[Mark]:
        clauses = ["run_id = ?", _ASOF]
        params: list[Any] = [run_id, snapshot_seq, snapshot_seq]
        if symbols:
            placeholders = ", ".join("?" for _ in symbols)
            clauses.append(f"symbol IN ({placeholders})")
            params.extend(symbols)
        if last_key:
            clauses.append("symbol > ?")
            params.append(str(last_key[0]))
        sql = (
            f"SELECT {_MARK_COLUMNS} FROM mark_versions WHERE "
            + " AND ".join(clauses)
            + " ORDER BY symbol LIMIT ?"
        )
        params.append(limit + 1)
        with self._guard(), self._acquire() as conn:
            rows = conn.execute(sql, params).fetchall()
        return self._paginate(rows, limit, es.mark_from_row, lambda row: [row["symbol"]])

    def get_mark(self, *, run_id: str, symbol: str, as_of_seq: int) -> Mark | None:
        with self._guard(), self._acquire() as conn:
            row = conn.execute(
                f"SELECT {_MARK_COLUMNS} FROM mark_versions "
                f"WHERE run_id = ? AND symbol = ? AND {_ASOF}",
                (run_id, symbol, as_of_seq, as_of_seq),
            ).fetchone()
            return es.mark_from_row(row) if row is not None else None

    # -- shared ------------------------------------------------------------

    @staticmethod
    def _paginate(
        rows: list[sqlite3.Row],
        limit: int,
        mapper: Any,
        key_of: Any,
    ) -> Page[Any]:
        """Turn ``limit + 1`` rows into a page plus a continuation key.

        Over-fetching by one is how the page knows whether more rows exist
        without a second COUNT query - and without claiming a next page that
        turns out to be empty.
        """
        has_more = len(rows) > limit
        visible = rows[:limit]
        items = [mapper(row) for row in visible]
        last_key = key_of(visible[-1]) if (has_more and visible) else None
        return Page(items=items, last_key=last_key)


class _PoolGuard:
    """Translates read-pool starvation into the public error contract."""

    def __enter__(self) -> "_PoolGuard":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        if exc_type is TimeoutError:
            raise ServiceNotReadyError(
                "no read capacity available; retry after backoff",
                details={"cause": "read_pool_exhausted"},
            ) from exc
        return False
