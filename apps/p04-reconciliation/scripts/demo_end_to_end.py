#!/usr/bin/env python
"""The suggested first end-to-end demo (SYSTEM_DESIGN section 18).

Walks the whole lifecycle against a live Project 01 simulator and leaves a
populated database behind, so `scripts/verify_release_gates.py` has something
real to check.

The demo deliberately exercises the *refusals* as well as the happy path,
because the refusals are the point of the service:

1. a clean world audits as `MATCHED`;
2. a corrupted account is detected, planned, approved by a second actor,
   dispatched, and independently verified;
3. a deal money conflict is detected and produces **no** dispatchable action;
4. a moved source cut is reported as `DIFFERENT_CUT`, not corruption;
5. a lost ACK becomes `UNKNOWN` and resolves by receipt query, once;
6. a stale plan is refused at dispatch.

Usage:
    python scripts/demo_end_to_end.py [--db PATH] [--keep]

It boots the simulator in-process, so no separate server is needed.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from reconciliation.approvals import ApprovalService  # noqa: E402
from reconciliation.approvals import Role as ApprovalRole  # noqa: E402
from reconciliation.audit import verify_chain  # noqa: E402
from reconciliation.campaign import CampaignWorld  # noqa: E402
from reconciliation.db.engine import Role  # noqa: E402
from reconciliation.errors import ReasonCode  # noqa: E402
from reconciliation.executor import RepairExecutor  # noqa: E402

AUTHOR = "demo-auditor"
APPROVER = "demo-approver"


def _step(number: int, title: str) -> None:
    print()
    print(f"--- {number}. {title} " + "-" * max(0, 60 - len(title)))


def _show(label: str, value: object) -> None:
    print(f"    {label:<34} {value}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Project 04 end-to-end demo")
    parser.add_argument("--db", type=Path, default=Path("./data/recon.db"))
    parser.add_argument(
        "--keep", action="store_true", help="keep the simulator database too"
    )
    args = parser.parse_args(argv)

    world = CampaignWorld(seed=42)
    failures: list[str] = []

    def expect(condition: bool, message: str) -> None:
        if not condition:
            failures.append(message)
            print(f"    !! UNEXPECTED: {message}")

    try:
        _step(1, "build a synced world and audit it clean")
        state = world.synced(accounts=6, deals=45, positions=6)
        _show("source epoch", state["epoch_id"])
        _show("source snapshot_seq (S)", state["snapshot_seq"])
        _show("P03 deal watermark (W)", state["watermark"])

        for entity in ("deals", "accounts", "positions"):
            outcome = world.scheduler.run_audit(entity=entity, epoch_id=state["epoch_id"])
            _show(f"audit {entity}", f"{outcome.status} findings={outcome.finding_count}")
            expect(outcome.status == "MATCHED", f"{entity} should audit MATCHED when clean")

        _step(2, "corrupt one account, then repair it under approval")
        victim = world.first_value(
            "SELECT account_id FROM accounts_current WHERE epoch_id = ? ORDER BY account_id LIMIT 1",
            (state["epoch_id"],),
        )
        world.harness.corrupt_for_test(
            "UPDATE accounts_current SET balance = '0.01' WHERE epoch_id = ? AND account_id = ?",
            (state["epoch_id"], victim),
        )
        _show("corrupted account", victim)

        outcome = world.scheduler.run_audit(entity="accounts", epoch_id=state["epoch_id"])
        _show("audit result", f"{outcome.status} findings={outcome.finding_count}")
        expect(outcome.status == "FINDINGS", "a same-cut account mismatch must be detected")

        plan = world.scheduler.plan_repair(outcome.recon_run_id, created_by=AUTHOR)
        expect(plan is not None, "policy should propose a re-sync for an account mismatch")
        assert plan is not None
        _show("plan status", plan.status)
        _show("plan actions", [a.action_type.value for a in plan.actions])

        approvals = ApprovalService(
            repository=world.repository, settings=world.settings, clock=world.clock
        )

        # The author cannot approve their own plan.
        self_approval = approvals.decide(
            plan_id=plan.plan_id,
            actor_id=AUTHOR,
            role=ApprovalRole.APPROVER,
            decision="APPROVE",
            plan_hash=plan.plan_hash,
        )
        _show("self-approval refused", self_approval.reason_code)
        expect(
            not self_approval.allowed
            and self_approval.reason_code is ReasonCode.APPROVAL_SELF,
            "self-approval must be refused",
        )

        # Dispatch before approval is refused.
        premature = world.scheduler.dispatch(plan)
        _show("dispatch before approval", premature.reason_code)
        expect(
            premature.reason_code is ReasonCode.APPROVAL_MISSING,
            "dispatch without approval must be refused",
        )

        granted = approvals.decide(
            plan_id=plan.plan_id,
            actor_id=APPROVER,
            role=ApprovalRole.APPROVER,
            decision="APPROVE",
            plan_hash=plan.plan_hash,
        )
        _show("approval by second actor", granted.allowed)
        expect(granted.allowed, "a distinct approver should be accepted")

        dispatched = world.scheduler.dispatch(plan)
        _show("dispatch result", f"{dispatched.plan_status} (not VERIFIED yet)")
        expect(
            dispatched.plan_status == "VERIFYING",
            "a committed repair must await independent verification",
        )

        repaired = world.first_value(
            "SELECT balance FROM accounts_current WHERE epoch_id = ? AND account_id = ?",
            (state["epoch_id"], victim),
        )
        _show("balance after re-sync", repaired)
        expect(repaired != "0.01", "P03 should have re-derived the account from the source")

        verified = world.scheduler.verify(plan, entity="accounts")
        _show("independent verification", f"{verified.plan_status}")
        _show("verification run", verified.verification_run_id)
        expect(verified.verified, "the independent re-audit should match")

        _step(3, "a deal money conflict is quarantined, never corrected")
        deal = world.first_value(
            "SELECT deal_id FROM deals WHERE epoch_id = ? ORDER BY source_seq LIMIT 1",
            (state["epoch_id"],),
        )
        world.harness.corrupt_for_test(
            "UPDATE deals SET commission = '-999.99' WHERE epoch_id = ? AND deal_id = ?",
            (state["epoch_id"], deal),
        )
        conflict = world.scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
        codes = sorted({f["finding_code"] for f in conflict.findings})
        _show("conflicted deal", deal)
        _show("findings", codes)
        conflict_plan = world.scheduler.plan_repair(conflict.recon_run_id, created_by=AUTHOR)
        dispatchable = conflict_plan.dispatchable_actions if conflict_plan else ()
        _show("dispatchable actions", len(dispatchable))
        expect("IMMUTABLE_DEAL_CONFLICT" in codes, "a money conflict must be reported")
        expect(not dispatchable, "a money conflict must never be auto-correctable")

        still_wrong = world.first_value(
            "SELECT commission FROM deals WHERE epoch_id = ? AND deal_id = ?",
            (state["epoch_id"], deal),
        )
        _show("commission after audit", f"{still_wrong} (audit changed nothing)")
        expect(still_wrong == "-999.99", "the audit must not have modified the deal")

        _step(4, "a moved source cut is DIFFERENT_CUT, not corruption")
        world.advance(state["run_id"], events=14)
        moved = world.scheduler.run_audit(entity="accounts", epoch_id=state["epoch_id"])
        _show("audit result", f"{moved.status}/{moved.reason_code}")
        _show("plan proposed", world.scheduler.plan_repair(moved.recon_run_id, created_by=AUTHOR))
        expect(
            moved.reason_code is ReasonCode.DIFFERENT_CUT,
            "a source read past the published scan must be DIFFERENT_CUT",
        )

        _step(5, "a lost ACK becomes UNKNOWN and resolves exactly once")
        world.harness.publish_snapshot(state["epoch_id"], "accounts")  # realign
        victim2 = world.first_value(
            "SELECT account_id FROM accounts_current WHERE epoch_id = ? ORDER BY account_id LIMIT 1",
            (state["epoch_id"],),
        )
        world.harness.corrupt_for_test(
            "UPDATE accounts_current SET balance = '0.02' WHERE epoch_id = ? AND account_id = ?",
            (state["epoch_id"], victim2),
        )
        second = world.scheduler.run_audit(entity="accounts", epoch_id=state["epoch_id"])
        plan2 = world.scheduler.plan_repair(second.recon_run_id, created_by=AUTHOR)
        if plan2 is None:
            expect(False, "expected a plan for the second account mismatch")
        else:
            approvals.decide(
                plan_id=plan2.plan_id,
                actor_id=APPROVER,
                role=ApprovalRole.APPROVER,
                decision="APPROVE",
                plan_hash=plan2.plan_hash,
            )
            world.harness.faults.lose_repair_ack.arm(1)
            lost = world.scheduler.dispatch(plan2)
            _show("dispatch outcome", f"{lost.plan_status}/{lost.reason_code}")
            _show("fault actually fired", world.harness.faults.lose_repair_ack.fired)
            expect(lost.plan_status == "UNKNOWN", "a lost ACK must yield UNKNOWN")

            intent_id = plan2.actions[0].intent_id
            executor = RepairExecutor(
                repository=world.repository,
                approvals=approvals,
                leases=world.scheduler.leases,
                read_port=world.harness,
                repair_port=world.harness,
                settings=world.settings,
                clock=world.clock,
            )
            resolved = executor.resolve_unknown(
                action_id=plan2.actions[0].action_id, intent_id=intent_id
            )
            receipt = world.harness.repair_intent_status(intent_id)
            _show("resolved by receipt query", resolved.status)
            _show("P03 execution_count", receipt.execution_count if receipt else None)
            expect(resolved.status == "COMMITTED", "the receipt should report a commit")
            expect(
                receipt is not None and receipt.execution_count == 1,
                "the repair must have executed exactly once",
            )
            world.scheduler.verify(plan2, entity="accounts")

        _step(6, "a stale plan is refused at dispatch")
        victim3 = world.first_value(
            "SELECT account_id FROM accounts_current WHERE epoch_id = ? ORDER BY account_id DESC LIMIT 1",
            (state["epoch_id"],),
        )
        world.harness.corrupt_for_test(
            "UPDATE accounts_current SET balance = '0.03' WHERE epoch_id = ? AND account_id = ?",
            (state["epoch_id"], victim3),
        )
        third = world.scheduler.run_audit(entity="accounts", epoch_id=state["epoch_id"])
        plan3 = world.scheduler.plan_repair(third.recon_run_id, created_by=AUTHOR)
        if plan3 is not None:
            approvals.decide(
                plan_id=plan3.plan_id,
                actor_id=APPROVER,
                role=ApprovalRole.APPROVER,
                decision="APPROVE",
                plan_hash=plan3.plan_hash,
            )
            # Project 03 publishes a newer scan: the plan's evidence is now
            # about a projection that no longer exists.
            world.harness.publish_snapshot(state["epoch_id"], "accounts")
            stale = world.scheduler.dispatch(plan3)
            _show("dispatch outcome", f"{stale.plan_status}/{stale.reason_code}")
            expect(stale.plan_status == "STALE", "a moved revision must make the plan STALE")

        _step(7, "evidence integrity")
        conn = world.db.connect(Role.RECON)
        try:
            ok, bad, detail = verify_chain(conn)
            events = conn.execute("SELECT COUNT(*) FROM recon_audit").fetchone()[0]
            runs = conn.execute("SELECT COUNT(*) FROM recon_runs").fetchone()[0]
            findings = conn.execute("SELECT COUNT(*) FROM recon_findings").fetchone()[0]
            plans = conn.execute("SELECT COUNT(*) FROM recon_plans").fetchone()[0]
        finally:
            conn.close()
        _show("audit events", events)
        _show("audit chain valid", ok if ok else f"BROKEN at {bad}: {detail}")
        _show("runs / findings / plans", f"{runs} / {findings} / {plans}")
        expect(ok, "the audit hash chain must recompute")

        # Preserve the database so the release-gate script can check it.
        args.db.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(world.settings.db_path, args.db)
        for suffix in ("-wal", "-shm"):
            side = Path(str(world.settings.db_path) + suffix)
            if side.exists():
                shutil.copy2(side, Path(str(args.db) + suffix))
        print()
        _show("database written to", args.db)
        print()
        print("Next: python scripts/verify_release_gates.py --db " + str(args.db))

    finally:
        if not args.keep:
            world.close()

    print()
    if failures:
        print(f"DEMO FAILED - {len(failures)} unexpected outcome(s):")
        for message in failures:
            print(f"  - {message}")
        return 1
    print("DEMO OK - every expected outcome, including every refusal, occurred.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
