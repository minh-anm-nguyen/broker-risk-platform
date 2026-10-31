"""Runnable invariant suite (INV-01..INV-16) and the canonical state digest.

Two design rules govern this module.

**An invariant that cannot be checked here reports its status honestly.**
``NOT_APPLICABLE`` means "not provable from storage alone" and names the test
that does prove it; ``SKIPPED`` means "we did not look this time". Neither is
ever folded into ``passed``. A suite that quietly counts unchecked invariants
as passing produces a green scorecard nobody can act on, which is worse than
having no suite.

**The sweep is snapshot-consistent.** Every check is bounded by one
``committed_seq`` read once at the start. Without that bound a writer
committing between two of the sweep's queries makes ``COUNT(*)`` disagree with
``committed_seq``, and INV-01 reports a failure that never existed. A checker
that cries wolf while the system is live is a checker nobody runs during a
chaos test - exactly when it is needed most. Bounding by ``seq <= S`` is the
same technique the read API uses, and unlike holding a long read transaction
it does not stall WAL checkpointing (WF-084).
"""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from ..canonical import StreamingDigest
from . import accounting as acc
from .models import PositionStatus, RunStatus, Side, UnsupportedOperationError, symbol_spec

PASS = "PASS"
FAIL = "FAIL"
NOT_APPLICABLE = "NOT_APPLICABLE"
#: Checkable from storage, but not run in this invocation. Distinct from
#: NOT_APPLICABLE on purpose: "we did not look" and "this cannot be answered
#: here" are different facts, and neither is a pass.
SKIPPED = "SKIPPED"

STORAGE_SCOPE = "storage"
HARNESS_SCOPE = "harness"

#: Aggregate-only checks: constant work regardless of dataset size, so they
#: are safe to run inside an HTTP request.
FAST_SCOPE = "fast"
#: Adds the checks that iterate every deal and position, plus the canonical
#: digest. Measured in tens of seconds on a 100k-deal dataset, which is why it
#: is opt-in rather than the endpoint default (section 5.2).
FULL_SCOPE = "full"


@dataclass(frozen=True, slots=True)
class InvariantSpec:
    invariant_id: str
    title: str
    scope: str
    #: For harness-scope invariants: the test that actually proves it.
    covered_by: str = ""


@dataclass(frozen=True, slots=True)
class InvariantResult:
    invariant_id: str
    title: str
    scope: str
    status: str
    detail: str = ""
    covered_by: str = ""
    observed: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return {
            "invariant_id": self.invariant_id,
            "title": self.title,
            "scope": self.scope,
            "status": self.status,
            "detail": self.detail,
            "covered_by": self.covered_by,
            "observed": self.observed,
        }


INVARIANTS: tuple[InvariantSpec, ...] = (
    InvariantSpec("INV-01", "event_log.seq is contiguous 1..committed_seq", STORAGE_SCOPE),
    InvariantSpec("INV-02", "deal_id is unique within a run", STORAGE_SCOPE),
    InvariantSpec("INV-03", "every deal references an existing account and position", STORAGE_SCOPE),
    InvariantSpec("INV-04", "no two versions of one key are valid at the same seq", STORAGE_SCOPE),
    InvariantSpec("INV-05", "OPEN positions have volume > 0, CLOSED have volume = 0", STORAGE_SCOPE),
    InvariantSpec("INV-06", "closed volume never exceeds opened volume", STORAGE_SCOPE),
    InvariantSpec(
        "INV-07",
        "balance recomputed from deals and adjustments matches the projection",
        STORAGE_SCOPE,
    ),
    InvariantSpec("INV-08", "unrealized P&L uses the mark version valid at the same seq", STORAGE_SCOPE),
    InvariantSpec(
        "INV-09",
        "same seed, spec and generator version produce the same canonical digest",
        HARNESS_SCOPE,
        "tests/integration/test_determinism.py::test_same_seed_same_digest",
    ),
    InvariantSpec(
        "INV-10",
        "every returned page contains only records within the snapshot seq",
        HARNESS_SCOPE,
        "tests/integration/test_pagination_snapshot.py::test_pages_stay_within_the_snapshot",
    ),
    InvariantSpec(
        "INV-11",
        "a full paged scan returns every record exactly once",
        HARNESS_SCOPE,
        "tests/integration/test_pagination_snapshot.py::test_a_full_scan_returns_every_record_exactly_once",
    ),
    InvariantSpec(
        "INV-12",
        "response-level faults never change the canonical store",
        HARNESS_SCOPE,
        "tests/integration/test_fault_injection.py::test_faults_do_not_change_truth",
    ),
    InvariantSpec("INV-13", "an idempotency key maps to exactly one committed effect", STORAGE_SCOPE),
    InvariantSpec("INV-14", "no event exists beyond the committed watermark", STORAGE_SCOPE),
    InvariantSpec("INV-15", "exactly one active run, and replaced runs are marked", STORAGE_SCOPE),
    InvariantSpec(
        "INV-16",
        "queue, memory and concurrency stay within configured bounds",
        HARNESS_SCOPE,
        "tests/performance/test_baseline_benchmark.py::test_backpressure_is_bounded",
    ),
)

_BY_ID = {spec.invariant_id: spec for spec in INVARIANTS}

#: Checks whose cost is independent of dataset size.
_FAST_CHECKS = ("INV-01", "INV-02", "INV-03", "INV-04", "INV-13", "INV-14", "INV-15")


def _result(invariant_id: str, status: str, detail: str = "", **observed: Any) -> InvariantResult:
    spec = _BY_ID[invariant_id]
    return InvariantResult(
        invariant_id=spec.invariant_id,
        title=spec.title,
        scope=spec.scope,
        status=status,
        detail=detail,
        covered_by=spec.covered_by,
        observed=observed,
    )


def committed_seq_of(conn: sqlite3.Connection, run_id: str) -> int:
    row = conn.execute("SELECT committed_seq FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    if row is None:
        raise LookupError(f"run {run_id!r} does not exist")
    return int(row["committed_seq"])


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run_all(
    conn: sqlite3.Connection, run_id: str, *, scope: str = FULL_SCOPE
) -> list[InvariantResult]:
    """Run the storage-scope invariants against ``run_id``.

    ``scope=fast`` runs only the aggregate checks and reports the rest as
    ``SKIPPED``. Release gates must use ``scope=full``: a scorecard built from
    a fast sweep has not checked the money.
    """
    checks: dict[str, Any] = {
        "INV-01": _inv01_seq_contiguity,
        "INV-02": _inv02_deal_uniqueness,
        "INV-03": _inv03_referential_integrity,
        "INV-04": _inv04_version_intervals,
        "INV-05": _inv05_position_volume_status,
        "INV-06": _inv06_closed_not_exceeding_opened,
        "INV-07": _inv07_balance_recompute,
        "INV-08": _inv08_mark_provenance,
        "INV-13": _inv13_idempotency_effects,
        "INV-14": _inv14_committed_watermark,
        "INV-15": _inv15_run_isolation,
    }
    # Read the watermark once; every check below is bounded by it.
    snapshot = committed_seq_of(conn, run_id)

    results: list[InvariantResult] = []
    for invariant_id, check in checks.items():
        if scope == FAST_SCOPE and invariant_id not in _FAST_CHECKS:
            results.append(
                _result(
                    invariant_id,
                    SKIPPED,
                    "iterates the full dataset; re-run with scope=full",
                )
            )
            continue
        try:
            results.append(check(conn, run_id, snapshot))
        except Exception as exc:  # noqa: BLE001 - a failing check is a FAIL, not a crash
            results.append(
                _result(
                    invariant_id,
                    FAIL,
                    f"invariant check raised {type(exc).__name__}: {exc}",
                )
            )
    for spec in INVARIANTS:
        if spec.scope != HARNESS_SCOPE:
            continue
        results.append(
            _result(
                spec.invariant_id,
                NOT_APPLICABLE,
                "not provable from storage alone; proven by the named test",
            )
        )
    results.sort(key=lambda item: item.invariant_id)
    return results


def summarize(results: list[InvariantResult]) -> dict[str, Any]:
    """Counts for a scorecard.

    ``NOT_APPLICABLE`` and ``SKIPPED`` are reported separately and never folded
    into ``passed``. ``storage_scope_ok`` is true only when every storage-scope
    invariant actually ran *and* passed, so a fast sweep cannot produce a green
    gate.
    """
    counts = {PASS: 0, FAIL: 0, NOT_APPLICABLE: 0, SKIPPED: 0}
    for item in results:
        counts[item.status] = counts.get(item.status, 0) + 1
    storage = [item for item in results if item.scope == STORAGE_SCOPE]
    return {
        "total": len(results),
        "passed": counts[PASS],
        "failed": counts[FAIL],
        "not_applicable": counts[NOT_APPLICABLE],
        "skipped": counts[SKIPPED],
        "failed_ids": [item.invariant_id for item in results if item.status == FAIL],
        "skipped_ids": [item.invariant_id for item in results if item.status == SKIPPED],
        "storage_scope_ok": bool(storage) and all(item.status == PASS for item in storage),
    }


# ---------------------------------------------------------------------------
# Storage-scope checks. Each is bounded by `snapshot`.
# ---------------------------------------------------------------------------


def _inv01_seq_contiguity(
    conn: sqlite3.Connection, run_id: str, snapshot: int
) -> InvariantResult:
    row = conn.execute(
        "SELECT COUNT(*) AS total, COUNT(DISTINCT seq) AS distinct_seq, "
        "COALESCE(MIN(seq), 0) AS lowest, COALESCE(MAX(seq), 0) AS highest "
        "FROM event_log WHERE run_id = ? AND seq <= ?",
        (run_id, snapshot),
    ).fetchone()
    total, distinct_seq = int(row["total"]), int(row["distinct_seq"])
    lowest, highest = int(row["lowest"]), int(row["highest"])
    observed = {
        "committed_seq": snapshot,
        "event_count": total,
        "distinct_seq": distinct_seq,
        "min_seq": lowest,
        "max_seq": highest,
    }
    if total == 0:
        if snapshot == 0:
            return _result("INV-01", PASS, "empty run", **observed)
        return _result("INV-01", FAIL, "committed_seq advanced with no events", **observed)
    problems = []
    if total != distinct_seq:
        problems.append("duplicate seq values")
    if lowest != 1:
        problems.append(f"sequence starts at {lowest}, expected 1")
    if highest != snapshot:
        problems.append(f"max(seq)={highest} but committed_seq={snapshot}")
    if total != snapshot:
        # A gap shows up here even when min and max look right.
        problems.append(f"event count {total} does not equal committed_seq {snapshot}")
    if problems:
        return _result("INV-01", FAIL, "; ".join(problems), **observed)
    return _result("INV-01", PASS, "contiguous", **observed)


def _inv02_deal_uniqueness(
    conn: sqlite3.Connection, run_id: str, snapshot: int
) -> InvariantResult:
    row = conn.execute(
        "SELECT COUNT(*) AS total, COUNT(DISTINCT deal_id) AS unique_ids, "
        "COUNT(DISTINCT source_seq) AS unique_seqs FROM deals "
        "WHERE run_id = ? AND source_seq <= ?",
        (run_id, snapshot),
    ).fetchone()
    total, unique_ids = int(row["total"]), int(row["unique_ids"])
    unique_seqs = int(row["unique_seqs"])
    observed = {
        "deal_count": total,
        "unique_deal_ids": unique_ids,
        "unique_source_seqs": unique_seqs,
    }
    if total != unique_ids:
        return _result("INV-02", FAIL, "duplicate deal_id present", **observed)
    if total != unique_seqs:
        # The MVP emits at most one deal per event, so a shared source_seq
        # means two deals were attributed to one event.
        return _result("INV-02", FAIL, "two deals share a source_seq", **observed)
    return _result("INV-02", PASS, "unique", **observed)


def _inv03_referential_integrity(
    conn: sqlite3.Connection, run_id: str, snapshot: int
) -> InvariantResult:
    orphan_accounts = conn.execute(
        "SELECT COUNT(*) FROM deals d WHERE d.run_id = ? AND d.source_seq <= ? AND NOT EXISTS "
        "(SELECT 1 FROM account_identity a WHERE a.run_id = d.run_id AND a.account_id = d.account_id)",
        (run_id, snapshot),
    ).fetchone()[0]
    orphan_positions = conn.execute(
        "SELECT COUNT(*) FROM deals d WHERE d.run_id = ? AND d.source_seq <= ? AND NOT EXISTS "
        "(SELECT 1 FROM position_identity p WHERE p.run_id = d.run_id AND p.position_id = d.position_id)",
        (run_id, snapshot),
    ).fetchone()[0]
    mismatched = conn.execute(
        "SELECT COUNT(*) FROM deals d JOIN position_identity p "
        "ON p.run_id = d.run_id AND p.position_id = d.position_id "
        "WHERE d.run_id = ? AND d.source_seq <= ? "
        "AND (p.account_id <> d.account_id OR p.symbol <> d.symbol)",
        (run_id, snapshot),
    ).fetchone()[0]
    observed = {
        "orphan_account_refs": int(orphan_accounts),
        "orphan_position_refs": int(orphan_positions),
        "account_symbol_mismatches": int(mismatched),
    }
    if any(observed.values()):
        return _result("INV-03", FAIL, "deals reference missing or mismatched parents", **observed)
    return _result("INV-03", PASS, "all references resolve", **observed)


_VERSION_TABLES = (
    ("account_versions", "account_id"),
    ("position_versions", "position_id"),
    ("mark_versions", "symbol"),
)


def _inv04_version_intervals(
    conn: sqlite3.Connection, run_id: str, snapshot: int
) -> InvariantResult:
    observed: dict[str, Any] = {}
    problems: list[str] = []
    for table, key in _VERSION_TABLES:
        degenerate = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE run_id = ? AND valid_from_seq <= ? "
            "AND valid_to_seq IS NOT NULL AND valid_to_seq <= valid_from_seq",
            (run_id, snapshot),
        ).fetchone()[0]
        # Adjacent-pair comparison via LEAD, not a self-join.
        #
        # The self-join form reads naturally but is O(versions^2) per key, and
        # a price tick creates a new mark version: a 100k-deal run holds ~40k
        # mark versions across 3 symbols, which is ~270M pairs and turns this
        # check into a hang rather than a check.
        #
        # Adjacent comparison is sufficient and O(n log n). Intervals are
        # half-open and ordered by start, so if every interval ends at or
        # before the next one's start, no pair can overlap: interval i ends at
        # or before i+1 starts, which is at or before i+2 starts, and so on. A
        # non-final version with valid_to_seq IS NULL is caught by the same
        # condition.
        overlapping = conn.execute(
            "SELECT COUNT(*) FROM ("
            f"  SELECT valid_to_seq, LEAD(valid_from_seq) OVER ("
            f"    PARTITION BY {key} ORDER BY valid_from_seq"
            f"  ) AS next_from FROM {table} WHERE run_id = ? AND valid_from_seq <= ?"
            ") WHERE next_from IS NOT NULL "
            "AND (valid_to_seq IS NULL OR valid_to_seq > next_from)",
            (run_id, snapshot),
        ).fetchone()[0]
        multiple_current = conn.execute(
            f"SELECT COUNT(*) FROM (SELECT {key} FROM {table} WHERE run_id = ? "
            "AND valid_from_seq <= ? AND (valid_to_seq IS NULL OR valid_to_seq > ?) "
            f"GROUP BY {key} HAVING COUNT(*) > 1)",
            (run_id, snapshot, snapshot),
        ).fetchone()[0]
        observed[f"{table}_degenerate"] = int(degenerate)
        observed[f"{table}_overlapping"] = int(overlapping)
        observed[f"{table}_multiple_current"] = int(multiple_current)
        if degenerate:
            problems.append(f"{table}: {degenerate} zero or negative width interval(s)")
        if overlapping:
            problems.append(f"{table}: {overlapping} overlapping interval pair(s)")
        if multiple_current:
            problems.append(
                f"{table}: {multiple_current} key(s) with more than one version at the snapshot"
            )
    if problems:
        return _result("INV-04", FAIL, "; ".join(problems), **observed)
    return _result("INV-04", PASS, "intervals are disjoint and half-open", **observed)


def _inv05_position_volume_status(
    conn: sqlite3.Connection, run_id: str, snapshot: int
) -> InvariantResult:
    bad_open: list[str] = []
    bad_closed: list[str] = []
    negative: list[str] = []
    for row in conn.execute(
        "SELECT position_id, status, volume_dec FROM position_versions "
        "WHERE run_id = ? AND valid_from_seq <= ?",
        (run_id, snapshot),
    ):
        volume = Decimal(row["volume_dec"])
        status = PositionStatus(row["status"])
        if volume < 0:
            negative.append(row["position_id"])
        elif status is PositionStatus.OPEN and volume <= 0:
            bad_open.append(row["position_id"])
        elif status is PositionStatus.CLOSED and volume != 0:
            bad_closed.append(row["position_id"])
    observed = {
        "negative_volume": negative[:10],
        "open_with_zero_volume": bad_open[:10],
        "closed_with_volume": bad_closed[:10],
    }
    if negative or bad_open or bad_closed:
        return _result("INV-05", FAIL, "position volume contradicts its status", **observed)
    return _result("INV-05", PASS, "volume and status agree", **observed)


def _inv06_closed_not_exceeding_opened(
    conn: sqlite3.Connection, run_id: str, snapshot: int
) -> InvariantResult:
    opened: dict[str, Decimal] = defaultdict(lambda: Decimal(0))
    closed: dict[str, Decimal] = defaultdict(lambda: Decimal(0))
    for row in conn.execute(
        "SELECT position_id, entry, volume_dec FROM deals WHERE run_id = ? AND source_seq <= ?",
        (run_id, snapshot),
    ):
        volume = Decimal(row["volume_dec"])
        if row["entry"] == "IN":
            opened[row["position_id"]] += volume
        else:
            closed[row["position_id"]] += volume

    over_closed: list[str] = []
    mismatched: list[dict[str, str]] = []
    current = {
        row["position_id"]: Decimal(row["volume_dec"])
        for row in conn.execute(
            "SELECT position_id, volume_dec FROM position_versions "
            "WHERE run_id = ? AND valid_from_seq <= ? "
            "AND (valid_to_seq IS NULL OR valid_to_seq > ?)",
            (run_id, snapshot, snapshot),
        )
    }
    for position_id, opened_volume in opened.items():
        closed_volume = closed.get(position_id, Decimal(0))
        if closed_volume > opened_volume:
            over_closed.append(position_id)
            continue
        expected = opened_volume - closed_volume
        actual = current.get(position_id)
        if actual is not None and actual != expected:
            mismatched.append(
                {
                    "position_id": position_id,
                    "expected": acc.format_decimal(expected),
                    "actual": acc.format_decimal(actual),
                }
            )
    observed = {
        "positions": len(opened),
        "over_closed": over_closed[:10],
        "volume_mismatches": mismatched[:10],
    }
    if over_closed or mismatched:
        return _result(
            "INV-06",
            FAIL,
            "closed volume exceeds opened, or remaining volume disagrees",
            **observed,
        )
    return _result("INV-06", PASS, "volumes reconcile per position", **observed)


def _inv07_balance_recompute(
    conn: sqlite3.Connection, run_id: str, snapshot: int
) -> InvariantResult:
    """Recompute every balance from the event log and the deal ledger.

    The initial balance comes from the ``ACCOUNT_CREATED`` event rather than
    the scenario spec, so this stays a genuinely independent recomputation even
    for accounts an operator injected by hand.
    """
    initial: dict[str, Decimal] = {}
    adjustments: dict[str, Decimal] = defaultdict(lambda: Decimal(0))
    for row in conn.execute(
        "SELECT event_type, payload_json FROM event_log WHERE run_id = ? AND seq <= ? "
        "AND event_type IN ('ACCOUNT_CREATED', 'ACCOUNT_ADJUSTED') ORDER BY seq",
        (run_id, snapshot),
    ):
        payload = json.loads(row["payload_json"])
        account_id = payload["account_id"]
        if row["event_type"] == "ACCOUNT_CREATED":
            initial[account_id] = Decimal(payload["balance"])
        else:
            adjustments[account_id] += Decimal(payload["amount"])

    deal_effect: dict[str, Decimal] = defaultdict(lambda: Decimal(0))
    for row in conn.execute(
        "SELECT account_id, gross_pnl_dec, commission_dec, swap_dec FROM deals "
        "WHERE run_id = ? AND source_seq <= ?",
        (run_id, snapshot),
    ):
        deal_effect[row["account_id"]] += acc.balance_delta(
            Decimal(row["gross_pnl_dec"]),
            Decimal(row["commission_dec"]),
            Decimal(row["swap_dec"]),
        )

    mismatches: list[dict[str, str]] = []
    checked = 0
    for row in conn.execute(
        "SELECT account_id, balance_dec FROM account_versions "
        "WHERE run_id = ? AND valid_from_seq <= ? "
        "AND (valid_to_seq IS NULL OR valid_to_seq > ?)",
        (run_id, snapshot, snapshot),
    ):
        account_id = row["account_id"]
        projected = Decimal(row["balance_dec"])
        expected = acc.quantize_money(
            initial.get(account_id, Decimal(0))
            + deal_effect.get(account_id, Decimal(0))
            + adjustments.get(account_id, Decimal(0)),
            field="balance",
        )
        checked += 1
        if expected != projected:
            mismatches.append(
                {
                    "account_id": account_id,
                    "expected": acc.format_decimal(expected),
                    "projected": acc.format_decimal(projected),
                }
            )
    observed = {"accounts_checked": checked, "mismatches": mismatches[:10]}
    if mismatches:
        return _result("INV-07", FAIL, f"{len(mismatches)} balance mismatch(es)", **observed)
    return _result("INV-07", PASS, "every balance reconciles to the cent", **observed)


def _inv08_mark_provenance(
    conn: sqlite3.Connection, run_id: str, snapshot: int
) -> InvariantResult:
    """Mark-to-market every open position using the mark valid at the same seq.

    A missing quote is *not* a failure: the contract is that it surfaces as
    ``null`` rather than zero. What would be a failure is a mark drawn from
    outside the snapshot, or a computation producing a non-finite number.
    """
    marks: dict[str, tuple[Decimal, Decimal, int, int | None]] = {}
    for row in conn.execute(
        "SELECT symbol, bid_dec, ask_dec, valid_from_seq, valid_to_seq FROM mark_versions "
        "WHERE run_id = ? AND valid_from_seq <= ? AND (valid_to_seq IS NULL OR valid_to_seq > ?)",
        (run_id, snapshot, snapshot),
    ):
        marks[row["symbol"]] = (
            Decimal(row["bid_dec"]),
            Decimal(row["ask_dec"]),
            int(row["valid_from_seq"]),
            None if row["valid_to_seq"] is None else int(row["valid_to_seq"]),
        )

    priced = 0
    without_mark: list[str] = []
    bad_provenance: list[str] = []
    unsupported: list[str] = []
    for row in conn.execute(
        "SELECT position_id, symbol, side, volume_dec, open_price_dec FROM position_versions "
        "WHERE run_id = ? AND valid_from_seq <= ? "
        "AND (valid_to_seq IS NULL OR valid_to_seq > ?) AND status = 'OPEN'",
        (run_id, snapshot, snapshot),
    ):
        quote = marks.get(row["symbol"])
        if quote is None:
            without_mark.append(row["position_id"])
            continue
        bid, ask, valid_from, valid_to = quote
        if valid_from > snapshot or (valid_to is not None and valid_to <= snapshot):
            bad_provenance.append(row["position_id"])
            continue
        try:
            spec = symbol_spec(row["symbol"])
        except UnsupportedOperationError:
            unsupported.append(row["symbol"])
            continue
        side = Side(row["side"])
        close_price = bid if side is Side.BUY else ask
        value = acc.gross_pnl(
            side,
            Decimal(row["open_price_dec"]),
            close_price,
            Decimal(row["volume_dec"]),
            spec,
        )
        acc.ensure_finite(value, field="unrealized_pnl")
        priced += 1
    observed = {
        "snapshot_seq": snapshot,
        "positions_priced": priced,
        "positions_without_mark": len(without_mark),
        "bad_provenance": bad_provenance[:10],
        "unsupported_symbols": sorted(set(unsupported)),
    }
    if bad_provenance or unsupported:
        return _result(
            "INV-08", FAIL, "mark used outside the snapshot, or unsupported symbol", **observed
        )
    return _result("INV-08", PASS, "every priced position used an in-snapshot mark", **observed)


def _inv13_idempotency_effects(
    conn: sqlite3.Connection, run_id: str, snapshot: int
) -> InvariantResult:
    dangling = conn.execute(
        "SELECT COUNT(*) FROM idempotency_records r WHERE r.scope = ? "
        "AND r.result_seq IS NOT NULL AND r.result_seq <= ? AND NOT EXISTS "
        "(SELECT 1 FROM event_log e WHERE e.run_id = r.scope AND e.seq = r.result_seq)",
        (run_id, snapshot),
    ).fetchone()[0]
    # Two keys pointing at one event would mean one effect was claimed twice.
    shared = conn.execute(
        "SELECT COUNT(*) FROM (SELECT result_seq FROM idempotency_records "
        "WHERE scope = ? AND result_seq IS NOT NULL AND result_seq <= ? "
        "GROUP BY result_seq HAVING COUNT(*) > 1)",
        (run_id, snapshot),
    ).fetchone()[0]
    observed = {"dangling_result_seq": int(dangling), "shared_result_seq": int(shared)}
    if dangling or shared:
        return _result("INV-13", FAIL, "idempotency records do not map 1:1 to events", **observed)
    return _result("INV-13", PASS, "one key, one effect", **observed)


def _inv14_committed_watermark(
    conn: sqlite3.Connection, run_id: str, snapshot: int
) -> InvariantResult:
    """No row may exist beyond the run's committed watermark.

    This is the one check that deliberately ignores the shared snapshot, and
    the read order is the whole point. ``MAX(seq)`` is read *first* and the
    watermark *second*, so a writer committing in between can only raise the
    watermark - which makes the comparison more likely to pass, never to fail
    falsely.

    Using the shared snapshot here would be wrong in the opposite direction:
    while generation is running, rows beyond an older snapshot are simply
    newer commits, and reporting them as "beyond the watermark" was a false
    alarm this check used to raise on every live sweep.
    """
    highest_event = conn.execute(
        "SELECT COALESCE(MAX(seq), 0) FROM event_log WHERE run_id = ?", (run_id,)
    ).fetchone()[0]
    highest_deal = conn.execute(
        "SELECT COALESCE(MAX(source_seq), 0) FROM deals WHERE run_id = ?", (run_id,)
    ).fetchone()[0]
    committed = committed_seq_of(conn, run_id)

    events_beyond = max(0, int(highest_event) - committed)
    deals_beyond = max(0, int(highest_deal) - committed)
    observed = {
        "committed_seq": committed,
        "sweep_snapshot_seq": snapshot,
        "max_event_seq": int(highest_event),
        "max_deal_source_seq": int(highest_deal),
        "events_beyond": events_beyond,
        "deals_beyond": deals_beyond,
    }
    if events_beyond or deals_beyond:
        return _result("INV-14", FAIL, "rows exist beyond the committed watermark", **observed)
    return _result("INV-14", PASS, "nothing beyond the watermark", **observed)


def _inv15_run_isolation(
    conn: sqlite3.Connection, run_id: str, snapshot: int
) -> InvariantResult:
    active_rows = conn.execute("SELECT COUNT(*) FROM active_run").fetchone()[0]
    active = conn.execute("SELECT run_id FROM active_run WHERE id = 1").fetchone()
    active_run_id = active["run_id"] if active is not None else None
    active_status = None
    if active_run_id:
        row = conn.execute("SELECT status FROM runs WHERE run_id = ?", (active_run_id,)).fetchone()
        active_status = row["status"] if row is not None else None
    unmarked_replaced = conn.execute(
        "SELECT COUNT(*) FROM runs WHERE status = ? AND replaced_at IS NULL",
        (RunStatus.REPLACED.value,),
    ).fetchone()[0]
    duplicate_generations = conn.execute(
        "SELECT COUNT(*) FROM (SELECT generation FROM runs GROUP BY generation HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    observed = {
        "active_run_rows": int(active_rows),
        "active_run_id": active_run_id,
        "active_run_status": active_status,
        "replaced_without_timestamp": int(unmarked_replaced),
        "duplicate_generations": int(duplicate_generations),
    }
    problems = []
    if active_rows > 1:
        problems.append("more than one active_run row")
    if active_status == RunStatus.REPLACED.value:
        problems.append("the active run is marked REPLACED")
    if unmarked_replaced:
        problems.append("a replaced run has no replaced_at timestamp")
    if duplicate_generations:
        problems.append("two runs share a generation number")
    if problems:
        return _result("INV-15", FAIL, "; ".join(problems), **observed)
    return _result("INV-15", PASS, "run identity is isolated", **observed)


# ---------------------------------------------------------------------------
# Canonical state digest
# ---------------------------------------------------------------------------


def canonical_state_digest(
    conn: sqlite3.Connection, run_id: str, *, snapshot_seq: int | None = None
) -> tuple[str, dict[str, int]]:
    """Content digest of a run's data, plus per-section record counts.

    Included: event order, type, payload and virtual time; every deal; the
    account, position and mark versions valid at the snapshot.

    Excluded, deliberately: ``run_id``, ``generation``, wall-clock
    ``created_at`` and job identifiers. Those differ between two legitimate
    replays of the same seed, so including any of them would make the digest
    never match and therefore prove nothing (INV-09).

    Bounded by ``snapshot_seq`` for the same reason the checks above are: a
    digest taken while a writer is active would otherwise depend on when each
    query happened to run.

    Every value folded in is already a canonical string on disk - decimals are
    stored as TEXT and payloads were written by ``canonical_json`` - so they go
    in verbatim rather than being parsed and re-serialized.
    """
    snapshot = committed_seq_of(conn, run_id) if snapshot_seq is None else snapshot_seq
    digest = StreamingDigest()
    counts: dict[str, int] = {}

    events = 0
    for row in conn.execute(
        "SELECT seq, event_type, payload_json, virtual_time FROM event_log "
        "WHERE run_id = ? AND seq <= ? ORDER BY seq",
        (run_id, snapshot),
    ):
        digest.update_text(
            "event",
            str(row["seq"]),
            row["event_type"],
            row["virtual_time"],
            row["payload_json"],
        )
        events += 1
    counts["events"] = events

    deals = 0
    for row in conn.execute(
        "SELECT deal_id, source_seq, position_id, account_id, symbol, entry, side, volume_dec, "
        "price_dec, gross_pnl_dec, commission_dec, swap_dec, executed_at FROM deals "
        "WHERE run_id = ? AND source_seq <= ? ORDER BY deal_id",
        (run_id, snapshot),
    ):
        digest.update_text(
            "deal",
            row["deal_id"],
            str(row["source_seq"]),
            row["position_id"],
            row["account_id"],
            row["symbol"],
            row["entry"],
            row["side"],
            row["volume_dec"],
            row["price_dec"],
            row["gross_pnl_dec"],
            row["commission_dec"],
            row["swap_dec"],
            row["executed_at"],
        )
        deals += 1
    counts["deals"] = deals

    accounts = 0
    for row in conn.execute(
        "SELECT account_id, currency, balance_dec, credit_dec FROM account_versions "
        "WHERE run_id = ? AND valid_from_seq <= ? "
        "AND (valid_to_seq IS NULL OR valid_to_seq > ?) ORDER BY account_id",
        (run_id, snapshot, snapshot),
    ):
        digest.update_text(
            "account",
            row["account_id"],
            row["currency"],
            row["balance_dec"],
            row["credit_dec"],
        )
        accounts += 1
    counts["accounts"] = accounts

    positions = 0
    for row in conn.execute(
        "SELECT position_id, account_id, symbol, side, volume_dec, open_price_dec, status "
        "FROM position_versions WHERE run_id = ? AND valid_from_seq <= ? "
        "AND (valid_to_seq IS NULL OR valid_to_seq > ?) ORDER BY position_id",
        (run_id, snapshot, snapshot),
    ):
        digest.update_text(
            "position",
            row["position_id"],
            row["account_id"],
            row["symbol"],
            row["side"],
            row["volume_dec"],
            row["open_price_dec"],
            row["status"],
        )
        positions += 1
    counts["positions"] = positions

    marks = 0
    for row in conn.execute(
        "SELECT symbol, bid_dec, ask_dec, market_time FROM mark_versions "
        "WHERE run_id = ? AND valid_from_seq <= ? "
        "AND (valid_to_seq IS NULL OR valid_to_seq > ?) ORDER BY symbol",
        (run_id, snapshot, snapshot),
    ):
        digest.update_text(
            "mark", row["symbol"], row["bid_dec"], row["ask_dec"], row["market_time"]
        )
        marks += 1
    counts["marks"] = marks

    return digest.hexdigest(), counts
