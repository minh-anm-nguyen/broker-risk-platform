"""Row mapping and versioned projection writes.

This module is the only place that knows how a domain object becomes SQLite
rows and back. Keeping the mapping in one place is what lets the writer, the
read repository and the invariant checker agree on what a version interval
means.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from ..canonical import canonical_json, format_timestamp, parse_timestamp, to_epoch_ms
from ..domain.accounting import format_decimal
from ..domain.models import (
    Account,
    Deal,
    DealEntry,
    EventType,
    Mark,
    Position,
    PositionStatus,
    RunStatus,
    Side,
)

#: A version is visible at sequence ``S`` when ``valid_from_seq <= S`` and the
#: interval has not yet closed. Half-open by construction: a version closing at
#: ``S`` is already invisible at ``S``, so exactly one version is visible at the
#: boundary (INV-04).
AS_OF_PREDICATE = "valid_from_seq <= ? AND (valid_to_seq IS NULL OR valid_to_seq > ?)"

CURRENT_PREDICATE = "valid_to_seq IS NULL"


# ---------------------------------------------------------------------------
# Row -> domain
# ---------------------------------------------------------------------------


def account_from_row(row: sqlite3.Row) -> Account:
    return Account(
        account_id=row["account_id"],
        currency=row["currency"],
        balance=Decimal(row["balance_dec"]),
        credit=Decimal(row["credit_dec"]),
    )


def position_from_row(row: sqlite3.Row) -> Position:
    return Position(
        position_id=row["position_id"],
        account_id=row["account_id"],
        symbol=row["symbol"],
        side=Side(row["side"]),
        volume_lots=Decimal(row["volume_dec"]),
        open_price=Decimal(row["open_price_dec"]),
        status=PositionStatus(row["status"]),
    )


def deal_from_row(row: sqlite3.Row) -> Deal:
    return Deal(
        deal_id=row["deal_id"],
        source_seq=int(row["source_seq"]),
        position_id=row["position_id"],
        account_id=row["account_id"],
        symbol=row["symbol"],
        entry=DealEntry(row["entry"]),
        side=Side(row["side"]),
        volume_lots=Decimal(row["volume_dec"]),
        price=Decimal(row["price_dec"]),
        realized_pnl_gross=Decimal(row["gross_pnl_dec"]),
        commission=Decimal(row["commission_dec"]),
        swap=Decimal(row["swap_dec"]),
        executed_at=parse_timestamp(row["executed_at"]),
    )


def mark_from_row(row: sqlite3.Row) -> Mark:
    return Mark(
        symbol=row["symbol"],
        bid=Decimal(row["bid_dec"]),
        ask=Decimal(row["ask_dec"]),
        market_time=parse_timestamp(row["market_time"]),
    )


# ---------------------------------------------------------------------------
# Current-version point lookups (writer path)
# ---------------------------------------------------------------------------


def load_current_account(conn: sqlite3.Connection, run_id: str, account_id: str) -> Account | None:
    row = conn.execute(
        "SELECT account_id, currency, balance_dec, credit_dec FROM account_versions "
        f"WHERE run_id = ? AND account_id = ? AND {CURRENT_PREDICATE}",
        (run_id, account_id),
    ).fetchone()
    return account_from_row(row) if row is not None else None


def load_current_open_position(
    conn: sqlite3.Connection, run_id: str, account_id: str, symbol: str
) -> Position | None:
    row = conn.execute(
        "SELECT position_id, account_id, symbol, side, volume_dec, open_price_dec, status "
        f"FROM position_versions WHERE run_id = ? AND account_id = ? AND symbol = ? "
        f"AND {CURRENT_PREDICATE} AND status = 'OPEN'",
        (run_id, account_id, symbol),
    ).fetchone()
    return position_from_row(row) if row is not None else None


def load_current_mark(conn: sqlite3.Connection, run_id: str, symbol: str) -> Mark | None:
    row = conn.execute(
        "SELECT symbol, bid_dec, ask_dec, market_time FROM mark_versions "
        f"WHERE run_id = ? AND symbol = ? AND {CURRENT_PREDICATE}",
        (run_id, symbol),
    ).fetchone()
    return mark_from_row(row) if row is not None else None


def current_position_status(
    conn: sqlite3.Connection, run_id: str, position_id: str
) -> PositionStatus | None:
    row = conn.execute(
        f"SELECT status FROM position_versions WHERE run_id = ? AND position_id = ? AND {CURRENT_PREDICATE}",
        (run_id, position_id),
    ).fetchone()
    return PositionStatus(row["status"]) if row is not None else None


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------


def insert_event(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    seq: int,
    event_id: str,
    event_type: EventType,
    payload: dict[str, Any],
    virtual_time: datetime,
    created_at: datetime,
) -> None:
    conn.execute(
        "INSERT INTO event_log (run_id, seq, event_id, event_type, payload_json, virtual_time, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            run_id,
            seq,
            event_id,
            event_type.value,
            canonical_json(payload),
            format_timestamp(virtual_time),
            format_timestamp(created_at),
        ),
    )


def _close_current_version(
    conn: sqlite3.Connection,
    table: str,
    key_column: str,
    key_value: Any,
    run_id: str,
    seq: int,
) -> None:
    """Close the open-ended version at ``seq``.

    There is deliberately no DELETE guarding the degenerate "same key written
    twice at the same seq" case. One event produces at most one mutation per
    key, so it cannot happen; if a future change made it possible, the primary
    key raises immediately. A loud failure beats a silent zero-width interval,
    which would quietly break "exactly one version visible at S".
    """
    conn.execute(
        f"UPDATE {table} SET valid_to_seq = ? WHERE run_id = ? AND {key_column} = ? "
        "AND valid_to_seq IS NULL",
        (seq, run_id, key_value),
    )


def write_account_version(
    conn: sqlite3.Connection, run_id: str, seq: int, account: Account, *, is_new: bool
) -> None:
    """Persist a new account version.

    ``is_new`` comes from the engine, which already knows whether this event
    creates the account. A brand-new account has no prior version to close, and
    an existing one needs no identity row, so each path issues exactly two
    statements.
    """
    if is_new:
        conn.execute(
            "INSERT INTO account_identity (run_id, account_id, created_seq) VALUES (?, ?, ?)",
            (run_id, account.account_id, seq),
        )
    else:
        _close_current_version(
            conn, "account_versions", "account_id", account.account_id, run_id, seq
        )
    conn.execute(
        "INSERT INTO account_versions "
        "(run_id, account_id, valid_from_seq, valid_to_seq, currency, balance_dec, credit_dec) "
        "VALUES (?, ?, ?, NULL, ?, ?, ?)",
        (
            run_id,
            account.account_id,
            seq,
            account.currency,
            format_decimal(account.balance),
            format_decimal(account.credit),
        ),
    )


def write_position_version(
    conn: sqlite3.Connection, run_id: str, seq: int, position: Position, *, is_new: bool
) -> None:
    """Persist a new position version."""
    if is_new:
        conn.execute(
            "INSERT INTO position_identity "
            "(run_id, position_id, account_id, symbol, created_seq) VALUES (?, ?, ?, ?, ?)",
            (run_id, position.position_id, position.account_id, position.symbol, seq),
        )
    else:
        _close_current_version(
            conn, "position_versions", "position_id", position.position_id, run_id, seq
        )
    conn.execute(
        "INSERT INTO position_versions "
        "(run_id, position_id, valid_from_seq, valid_to_seq, account_id, symbol, side, "
        " volume_dec, open_price_dec, status) "
        "VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?)",
        (
            run_id,
            position.position_id,
            seq,
            position.account_id,
            position.symbol,
            position.side.value,
            format_decimal(position.volume_lots),
            format_decimal(position.open_price),
            position.status.value,
        ),
    )


def write_mark_version(conn: sqlite3.Connection, run_id: str, seq: int, mark: Mark) -> None:
    # Marks have no identity table: the first write simply closes nothing.
    _close_current_version(conn, "mark_versions", "symbol", mark.symbol, run_id, seq)
    conn.execute(
        "INSERT INTO mark_versions "
        "(run_id, symbol, valid_from_seq, valid_to_seq, bid_dec, ask_dec, market_time) "
        "VALUES (?, ?, ?, NULL, ?, ?, ?)",
        (
            run_id,
            mark.symbol,
            seq,
            format_decimal(mark.bid),
            format_decimal(mark.ask),
            format_timestamp(mark.market_time),
        ),
    )


# ---------------------------------------------------------------------------
# Run metadata
# ---------------------------------------------------------------------------

_RUN_SUMMARY_COLUMNS = (
    "run_id, generation, seed, scenario_name, spec_hash, spec_json, generator_version, "
    "accounting_policy_version, symbol_catalog_version, status, committed_seq, virtual_time, "
    "start_virtual_time, account_count, deal_count, open_position_count, max_open_positions, "
    "max_deals, created_at, replaced_at"
)


@dataclass(frozen=True, slots=True)
class RunSummary:
    """Everything a caller needs to know about a run without a second query."""

    run_id: str
    generation: int
    seed: int
    scenario_name: str
    spec_hash: str
    spec_payload: dict[str, Any]
    generator_version: str
    accounting_policy_version: str
    symbol_catalog_version: str
    status: RunStatus
    committed_seq: int
    virtual_time: datetime
    start_virtual_time: datetime
    account_count: int
    deal_count: int
    open_position_count: int
    max_open_positions: int
    max_deals: int
    created_at: str
    replaced_at: str | None

    @property
    def spec(self) -> Any:
        """The scenario spec as a typed object.

        Imported inside the property on purpose: the generation package depends
        on persistence, so a module-level import here would close that cycle.
        """
        from ..generation.generator import spec_from_mapping

        return spec_from_mapping(self.spec_payload)

    def to_payload(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "generation": self.generation,
            "seed": self.seed,
            "scenario_name": self.scenario_name,
            "spec_hash": self.spec_hash,
            "generator_version": self.generator_version,
            "accounting_policy_version": self.accounting_policy_version,
            "symbol_catalog_version": self.symbol_catalog_version,
            "status": self.status.value,
            "committed_seq": self.committed_seq,
            "virtual_time": format_timestamp(self.virtual_time),
            "start_virtual_time": format_timestamp(self.start_virtual_time),
            "account_count": self.account_count,
            "deal_count": self.deal_count,
            "open_position_count": self.open_position_count,
            "max_open_positions": self.max_open_positions,
            "max_deals": self.max_deals,
            "created_at": self.created_at,
            "replaced_at": self.replaced_at,
        }


def load_active_run_id(conn: sqlite3.Connection) -> str | None:
    row = conn.execute("SELECT run_id FROM active_run WHERE id = 1").fetchone()
    return row["run_id"] if row is not None else None


def load_run_summary(conn: sqlite3.Connection, run_id: str) -> RunSummary | None:
    row = conn.execute(
        f"SELECT {_RUN_SUMMARY_COLUMNS} FROM runs WHERE run_id = ?", (run_id,)
    ).fetchone()
    if row is None:
        return None
    return RunSummary(
        run_id=row["run_id"],
        generation=int(row["generation"]),
        seed=int(row["seed"]),
        scenario_name=row["scenario_name"],
        spec_hash=row["spec_hash"],
        spec_payload=json.loads(row["spec_json"]),
        generator_version=row["generator_version"],
        accounting_policy_version=row["accounting_policy_version"],
        symbol_catalog_version=row["symbol_catalog_version"],
        status=RunStatus(row["status"]),
        committed_seq=int(row["committed_seq"]),
        virtual_time=parse_timestamp(row["virtual_time"]),
        start_virtual_time=parse_timestamp(row["start_virtual_time"]),
        account_count=int(row["account_count"]),
        deal_count=int(row["deal_count"]),
        open_position_count=int(row["open_position_count"]),
        max_open_positions=int(row["max_open_positions"]),
        max_deals=int(row["max_deals"]),
        created_at=row["created_at"],
        replaced_at=row["replaced_at"],
    )


def check_event_log_consistency(conn: sqlite3.Connection, run_id: str) -> str | None:
    """Cheap startup check for projection/event-log disagreement (section 4.3).

    Returns a human-readable reason, or ``None`` when consistent. Deliberately
    cheap: it runs on every start, so it uses aggregates and an existence test
    rather than a full recompute. A full sweep is what
    ``/admin/v1/diagnostics/invariants`` is for.
    """
    run = conn.execute("SELECT committed_seq FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    if run is None:
        return None
    committed = int(run["committed_seq"])
    aggregate = conn.execute(
        "SELECT COUNT(*) AS total, COALESCE(MAX(seq), 0) AS highest FROM event_log WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    total = int(aggregate["total"])
    highest = int(aggregate["highest"])
    if total != committed or highest != committed:
        return (
            f"committed_seq={committed} but event_log has count={total} and max(seq)={highest}"
        )
    orphan_deals = conn.execute(
        "SELECT COUNT(*) FROM deals d WHERE d.run_id = ? AND NOT EXISTS "
        "(SELECT 1 FROM event_log e WHERE e.run_id = d.run_id AND e.seq = d.source_seq)",
        (run_id,),
    ).fetchone()[0]
    if orphan_deals:
        return f"{orphan_deals} deal(s) reference a source_seq with no matching event"
    return None


def insert_deal(conn: sqlite3.Connection, run_id: str, deal: Deal) -> None:
    """Insert an immutable deal row.

    A plain INSERT, never an upsert: a duplicate ``deal_id`` must raise so the
    caller finds out, rather than overwriting a money figure (INV-02, WF-014).
    """
    conn.execute(
        "INSERT INTO deals (run_id, deal_id, source_seq, position_id, account_id, symbol, entry, "
        " side, volume_dec, price_dec, gross_pnl_dec, commission_dec, swap_dec, executed_at, "
        " executed_at_ms) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            run_id,
            deal.deal_id,
            deal.source_seq,
            deal.position_id,
            deal.account_id,
            deal.symbol,
            deal.entry.value,
            deal.side.value,
            format_decimal(deal.volume_lots),
            format_decimal(deal.price),
            format_decimal(deal.realized_pnl_gross),
            format_decimal(deal.commission),
            format_decimal(deal.swap),
            format_timestamp(deal.executed_at),
            to_epoch_ms(deal.executed_at),
        ),
    )
