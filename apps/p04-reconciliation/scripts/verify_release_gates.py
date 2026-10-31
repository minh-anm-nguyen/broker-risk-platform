#!/usr/bin/env python
"""Release-gate verification (SYSTEM_DESIGN section 14).

Section 14 defines gates G0–G4 and a scorecard template. This script turns the
*data correctness* and *controlled recovery* gates into executable checks
against a reconciliation database, so "G0 passed" is a query result rather
than a claim in a document.

Each check is a safety invariant that must hold over **every** row the
database has ever held, not just the happy path:

* no run may claim completeness without a provable receipt chain;
* no ``MATCHED`` run may hold a finding or an incomplete manifest;
* no ``PROVEN`` finding may belong to an inconclusive run;
* no plan may be ``VERIFIED`` without an independent verification run that
  itself reached ``MATCHED``;
* no action may have executed more than once;
* the audit hash chain must recompute.

A violation is a correctness defect, not a test failure: it means the service
recorded a verdict its own evidence does not support.

Usage:
    python scripts/verify_release_gates.py [--db PATH] [--json]

Exit codes: 0 all checks passed, 1 at least one violation, 2 usage error.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from reconciliation.audit import verify_chain  # noqa: E402
from reconciliation.db.engine import Database, Role  # noqa: E402
from reconciliation.db.schema import P03_OWNED_TABLES  # noqa: E402


@dataclass
class Check:
    gate: str
    check_id: str
    description: str
    sql: str | None = None
    violations: list[dict] = field(default_factory=list)
    passed: bool | None = None
    detail: str = ""

    @property
    def status(self) -> str:
        if self.passed is None:
            return "NOT_RUN"
        return "PASS" if self.passed else "FAIL"


# ---------------------------------------------------------------------------
# G0 — Data correctness
# ---------------------------------------------------------------------------

G0_CHECKS: list[Check] = [
    Check(
        gate="G0",
        check_id="G0-01",
        description="no run claims source completeness without a provable receipt chain",
        sql="""
        SELECT r.recon_run_id,
               (SELECT COUNT(*) FROM recon_source_page_receipts p
                 WHERE p.recon_run_id = r.recon_run_id) AS receipts,
               (SELECT COALESCE(SUM(p.is_terminal),0) FROM recon_source_page_receipts p
                 WHERE p.recon_run_id = r.recon_run_id) AS terminals
          FROM recon_runs r
         WHERE r.source_complete = 1
           AND (
             -- no terminal receipt
             NOT EXISTS (SELECT 1 FROM recon_source_page_receipts p
                          WHERE p.recon_run_id = r.recon_run_id AND p.is_terminal = 1)
             -- indices not contiguous from zero
             OR (SELECT MAX(p.page_index) + 1 FROM recon_source_page_receipts p
                  WHERE p.recon_run_id = r.recon_run_id)
                <> (SELECT COUNT(*) FROM recon_source_page_receipts p
                     WHERE p.recon_run_id = r.recon_run_id)
             -- more than one terminal receipt
             OR (SELECT SUM(p.is_terminal) FROM recon_source_page_receipts p
                  WHERE p.recon_run_id = r.recon_run_id) <> 1
             -- more than one distinct cut among the receipts
             OR (SELECT COUNT(DISTINCT p.snapshot_seq || '|' || p.run_id || '|' || p.generation)
                   FROM recon_source_page_receipts p
                  WHERE p.recon_run_id = r.recon_run_id) <> 1
           )
        """,
    ),
    Check(
        gate="G0",
        check_id="G0-02",
        description="declared page item counts equal stored manifest rows on complete runs",
        sql="""
        SELECT r.recon_run_id,
               (SELECT COALESCE(SUM(p.item_count),0) FROM recon_source_page_receipts p
                 WHERE p.recon_run_id = r.recon_run_id) AS declared,
               (SELECT COUNT(*) FROM recon_source_items i
                 WHERE i.recon_run_id = r.recon_run_id) AS stored
          FROM recon_runs r
         WHERE r.source_complete = 1
           AND (SELECT COALESCE(SUM(p.item_count),0) FROM recon_source_page_receipts p
                 WHERE p.recon_run_id = r.recon_run_id)
            <> (SELECT COUNT(*) FROM recon_source_items i
                 WHERE i.recon_run_id = r.recon_run_id)
        """,
    ),
    Check(
        gate="G0",
        check_id="G0-03",
        description="no MATCHED run is incomplete or holds a finding",
        sql="""
        SELECT r.recon_run_id, r.source_complete, r.target_complete,
               (SELECT COUNT(*) FROM recon_findings f
                 WHERE f.recon_run_id = r.recon_run_id) AS findings
          FROM recon_runs r
         WHERE r.status = 'MATCHED'
           AND (r.source_complete = 0 OR r.target_complete = 0
                OR EXISTS (SELECT 1 FROM recon_findings f
                            WHERE f.recon_run_id = r.recon_run_id))
        """,
    ),
    Check(
        gate="G0",
        check_id="G0-04",
        description="no MATCHED run has disagreeing source and target root hashes",
        sql="""
        SELECT recon_run_id, source_root_hash, target_root_hash
          FROM recon_runs
         WHERE status = 'MATCHED'
           AND (source_root_hash IS NULL OR target_root_hash IS NULL
                OR source_root_hash <> target_root_hash)
        """,
    ),
    Check(
        gate="G0",
        check_id="G0-05",
        description="no PROVEN finding belongs to an inconclusive run",
        sql="""
        SELECT f.finding_id, f.finding_code, r.status, r.reason_code
          FROM recon_findings f
          JOIN recon_runs r ON r.recon_run_id = f.recon_run_id
         WHERE f.confidence = 'PROVEN' AND r.status = 'INCONCLUSIVE'
        """,
    ),
    Check(
        gate="G0",
        check_id="G0-06",
        description="no finding exists on a run with an incomplete manifest at PROVEN confidence",
        sql="""
        SELECT f.finding_id, f.finding_code, r.source_complete, r.target_complete
          FROM recon_findings f
          JOIN recon_runs r ON r.recon_run_id = f.recon_run_id
         WHERE f.confidence = 'PROVEN'
           AND (r.source_complete = 0 OR r.target_complete = 0)
        """,
    ),
    Check(
        gate="G0",
        check_id="G0-07",
        description="every finding has an evidence hash and an explanation code",
        sql="""
        SELECT finding_id FROM recon_findings
         WHERE evidence_hash IS NULL OR LENGTH(evidence_hash) <> 64
            OR explanation_code IS NULL OR explanation_code = ''
        """,
    ),
    Check(
        gate="G0",
        check_id="G0-08",
        description="every terminated run carries a reason code unless it MATCHED",
        sql="""
        SELECT recon_run_id, status FROM recon_runs
         WHERE status IN ('INCONCLUSIVE','FAILED')
           AND (reason_code IS NULL OR reason_code = '')
        """,
    ),
    Check(
        gate="G0",
        check_id="G0-09",
        description="no deal manifest row carries a sequence beyond its run's snapshot",
        sql="""
        SELECT i.recon_run_id, i.item_key, i.source_seq, r.source_snapshot_seq
          FROM recon_source_items i
          JOIN recon_runs r ON r.recon_run_id = i.recon_run_id
         WHERE r.entity = 'deals' AND i.source_seq IS NOT NULL
           AND r.source_snapshot_seq IS NOT NULL
           AND i.source_seq > r.source_snapshot_seq
        """,
    ),
]

# ---------------------------------------------------------------------------
# G1 — Controlled recovery
# ---------------------------------------------------------------------------

G1_CHECKS: list[Check] = [
    Check(
        gate="G1",
        check_id="G1-01",
        description="no plan is VERIFIED without an independent MATCHED verification run",
        sql="""
        SELECT p.plan_id, p.status
          FROM recon_plans p
         WHERE p.status = 'VERIFIED'
           AND EXISTS (SELECT 1 FROM recon_actions a
                        WHERE a.plan_id = p.plan_id
                          AND a.action_type <> 'MANUAL_INVESTIGATION')
           AND NOT EXISTS (SELECT 1 FROM recon_runs v
                            WHERE v.verifies_plan_id = p.plan_id AND v.status = 'MATCHED')
        """,
    ),
    Check(
        gate="G1",
        check_id="G1-02",
        description="no dispatchable plan reached a terminal state without an approval on its hash",
        sql="""
        SELECT p.plan_id, p.status, p.plan_hash
          FROM recon_plans p
         WHERE p.status IN ('DISPATCHING','RUNNING','VERIFYING','VERIFIED')
           AND EXISTS (SELECT 1 FROM recon_actions a
                        WHERE a.plan_id = p.plan_id
                          AND a.action_type <> 'MANUAL_INVESTIGATION')
           AND NOT EXISTS (SELECT 1 FROM recon_approvals ap
                            WHERE ap.plan_id = p.plan_id
                              AND ap.decision = 'APPROVE'
                              AND ap.plan_hash = p.plan_hash)
        """,
    ),
    Check(
        gate="G1",
        check_id="G1-03",
        description="no plan was approved by its own author",
        sql="""
        SELECT p.plan_id, p.created_by
          FROM recon_plans p
          JOIN recon_approvals a ON a.plan_id = p.plan_id
         WHERE a.decision = 'APPROVE' AND a.actor_id = p.created_by
        """,
    ),
    Check(
        gate="G1",
        check_id="G1-04",
        description="no action records more than one committed attempt (idempotency)",
        sql="""
        SELECT a.action_id, a.intent_id, COUNT(*) AS committed_attempts
          FROM recon_actions a
          JOIN recon_attempts t ON t.action_id = a.action_id
         WHERE t.status = 'COMMITTED'
         GROUP BY a.action_id, a.intent_id
        HAVING COUNT(*) > 1
        """,
    ),
    Check(
        gate="G1",
        check_id="G1-05",
        description="no two actions share an intent id",
        sql="""
        SELECT intent_id, COUNT(*) AS actions
          FROM recon_actions GROUP BY intent_id HAVING COUNT(*) > 1
        """,
    ),
    Check(
        gate="G1",
        check_id="G1-06",
        description="no action type outside the permitted allowlist was ever persisted",
        sql="""
        SELECT action_id, action_type FROM recon_actions
         WHERE action_type NOT IN (
           'REQUEST_FULL_ACCOUNT_RESYNC','REQUEST_FULL_POSITION_RESYNC',
           'REQUEST_DEAL_BACKFILL','MANUAL_INVESTIGATION')
        """,
    ),
    Check(
        gate="G1",
        check_id="G1-07",
        description="no plan built from a money conflict carries a dispatchable action",
        sql="""
        SELECT DISTINCT p.plan_id, a.action_type
          FROM recon_plans p
          JOIN recon_actions a ON a.plan_id = p.plan_id
          JOIN recon_findings f ON f.recon_run_id = p.recon_run_id
         WHERE f.finding_code IN ('IMMUTABLE_DEAL_CONFLICT','EXTRA_TARGET_DEAL')
           AND a.action_type <> 'MANUAL_INVESTIGATION'
        """,
    ),
    Check(
        gate="G1",
        check_id="G1-08",
        description="no plan exists for a run that never produced a finding",
        sql="""
        SELECT p.plan_id, p.recon_run_id
          FROM recon_plans p
         WHERE NOT EXISTS (SELECT 1 FROM recon_findings f
                            WHERE f.recon_run_id = p.recon_run_id)
        """,
    ),
    Check(
        gate="G1",
        check_id="G1-09",
        description="no plan built from an INCONCLUSIVE-confidence finding is dispatchable",
        sql="""
        SELECT DISTINCT p.plan_id, a.action_type
          FROM recon_plans p
          JOIN recon_actions a ON a.plan_id = p.plan_id
         WHERE a.action_type <> 'MANUAL_INVESTIGATION'
           AND EXISTS (SELECT 1 FROM recon_findings f
                        WHERE f.recon_run_id = p.recon_run_id
                          AND f.confidence = 'INCONCLUSIVE')
        """,
    ),
]


def run_sql_checks(conn: sqlite3.Connection, checks: list[Check]) -> None:
    for check in checks:
        assert check.sql is not None
        try:
            rows = conn.execute(check.sql).fetchall()
        except sqlite3.Error as exc:
            check.passed = None
            check.detail = f"query failed: {exc}"
            continue
        check.violations = [dict(row) for row in rows]
        check.passed = not check.violations
        check.detail = (
            "no violations" if check.passed else f"{len(check.violations)} violation(s)"
        )


def run_structural_checks(conn: sqlite3.Connection) -> list[Check]:
    """Checks that are not expressible as a single violation query."""
    checks: list[Check] = []

    # G0-10: the audit chain must recompute end to end.
    chain = Check(
        gate="G0",
        check_id="G0-10",
        description="the tamper-evident audit chain recomputes",
    )
    ok, bad_sequence, detail = verify_chain(conn)
    chain.passed = ok
    chain.detail = "chain intact" if ok else f"broken at sequence {bad_sequence}: {detail}"
    if not ok:
        chain.violations = [{"sequence_no": bad_sequence, "detail": detail}]
    checks.append(chain)

    # G1-10: the role holding this connection must be denied every P03 write.
    privilege = Check(
        gate="G1",
        check_id="G1-10",
        description="the reconciliation role cannot write any Project 03 table",
    )
    allowed: list[str] = []
    for table in sorted(P03_OWNED_TABLES):
        try:
            conn.execute(f"DELETE FROM {table} WHERE 1=0")
            allowed.append(table)
        except sqlite3.DatabaseError:
            continue
    privilege.passed = not allowed
    privilege.violations = [{"writable_table": name} for name in allowed]
    privilege.detail = (
        f"all {len(P03_OWNED_TABLES)} tables denied"
        if privilege.passed
        else f"writable: {allowed}"
    )
    checks.append(privilege)

    return checks


def summarise(conn: sqlite3.Connection) -> dict:
    """Context for the scorecard: what the database actually contains."""
    def scalar(sql: str) -> int:
        row = conn.execute(sql).fetchone()
        return 0 if row is None or row[0] is None else int(row[0])

    return {
        "runs": scalar("SELECT COUNT(*) FROM recon_runs"),
        "runs_matched": scalar("SELECT COUNT(*) FROM recon_runs WHERE status='MATCHED'"),
        "runs_findings": scalar("SELECT COUNT(*) FROM recon_runs WHERE status IN ('FINDINGS','PLANNED')"),
        "runs_inconclusive": scalar("SELECT COUNT(*) FROM recon_runs WHERE status='INCONCLUSIVE'"),
        "findings": scalar("SELECT COUNT(*) FROM recon_findings"),
        "findings_proven": scalar("SELECT COUNT(*) FROM recon_findings WHERE confidence='PROVEN'"),
        "plans": scalar("SELECT COUNT(*) FROM recon_plans"),
        "plans_verified": scalar("SELECT COUNT(*) FROM recon_plans WHERE status='VERIFIED'"),
        "plans_stale_or_failed": scalar(
            "SELECT COUNT(*) FROM recon_plans WHERE status IN ('STALE','FAILED','REJECTED')"
        ),
        "attempts": scalar("SELECT COUNT(*) FROM recon_attempts"),
        "audit_events": scalar("SELECT COUNT(*) FROM recon_audit"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify Project 04 release gates (section 14)")
    parser.add_argument("--db", type=Path, default=Path("./data/recon.db"))
    parser.add_argument("--json", action="store_true", help="emit machine-readable output")
    args = parser.parse_args(argv)

    if not args.db.exists():
        print(f"database not found: {args.db}", file=sys.stderr)
        return 2

    database = Database(args.db)
    conn = database.connect(Role.RECON)
    try:
        checks = [*G0_CHECKS, *G1_CHECKS]
        run_sql_checks(conn, checks)
        checks.extend(run_structural_checks(conn))
        context = summarise(conn)
    finally:
        conn.close()

    failed = [c for c in checks if c.passed is False]
    not_run = [c for c in checks if c.passed is None]

    if args.json:
        print(
            json.dumps(
                {
                    "database": str(args.db),
                    "context": context,
                    "checks": [
                        {
                            "gate": c.gate,
                            "check_id": c.check_id,
                            "description": c.description,
                            "status": c.status,
                            "detail": c.detail,
                            "violations": c.violations[:20],
                        }
                        for c in checks
                    ],
                    "gates": {
                        "G0": all(c.passed for c in checks if c.gate == "G0"),
                        "G1": all(c.passed for c in checks if c.gate == "G1"),
                    },
                    "passed": not failed and not not_run,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0 if not failed and not not_run else 1

    print(f"Release gate verification - {args.db}")
    print()
    print("Database contents (context, not a gate):")
    for key, value in context.items():
        print(f"  {key:<24} {value}")
    print()
    for gate in ("G0", "G1"):
        title = "Data correctness" if gate == "G0" else "Controlled recovery"
        print(f"{gate} - {title} (mandatory)")
        for check in [c for c in checks if c.gate == gate]:
            mark = {"PASS": "[x]", "FAIL": "[!]", "NOT_RUN": "[ ]"}[check.status]
            print(f"  {mark} {check.check_id}  {check.description}")
            if check.status != "PASS":
                print(f"        -> {check.detail}")
                for violation in check.violations[:5]:
                    print(f"           {violation}")
        print()

    if failed:
        print(f"RESULT: FAIL - {len(failed)} check(s) violated")
        print()
        print(
            "A violation here is a correctness defect, not a flaky test: the service\n"
            "recorded a verdict its own evidence does not support. Do not release."
        )
        return 1
    if not_run:
        print(f"RESULT: INCOMPLETE - {len(not_run)} check(s) could not run")
        return 1

    print("RESULT: PASS - every G0 and G1 invariant holds over this database")
    print()
    print(
        "Note: G2 (what-if campaigns) is reported by `reconctl campaign`;\n"
        "G3 (performance and operations) requires the measurement rig in\n"
        "section 11.5 and is NOT verified by this script."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
