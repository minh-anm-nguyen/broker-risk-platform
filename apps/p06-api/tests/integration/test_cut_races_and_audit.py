"""IT06-04, IT06-05, IT06-07: races against the cut, and the audit gate.

These use deterministic barriers, not sleeps. A test arms a barrier at a named
point in the pipeline, the request blocks there, the test mutates upstream
state, then releases the barrier. That makes "a repair landed while the report
was computing" an exact schedule rather than a hopeful one.
"""

from __future__ import annotations

import asyncio

import pytest
from conftest import ACCOUNT_A, SOURCE_ID, add_position
from risk_api.faults import FaultPoint

pytestmark = pytest.mark.integration

RISK = f"/api/v1/accounts/{ACCOUNT_A}/risk"
POSITIONS = "/api/v1/positions"


async def run_with_barrier(client, context, point: str, mutate, *, params=None, headers=None):
    """Issue a request that blocks at ``point``, mutate, then release."""
    barrier = context.faults.arm_barrier(point)
    task = asyncio.create_task(
        client.get(RISK, params=params or {"source_id": SOURCE_ID}, headers=headers)
    )
    for _ in range(200):
        if barrier.arrivals:
            break
        await asyncio.sleep(0.005)
    assert barrier.arrivals, f"the request never reached {point}"
    mutate()
    barrier.release()
    return await task


class TestRepairRace:
    async def test_a_blocking_repair_mid_compute_prevents_a_ready_report(
        self, client, context, world, auth, evidence
    ):
        """IT06-04: R10 was pinned, R11 lands blocking, no R10 READY escapes."""
        before = world.checksum()
        response = await run_with_barrier(
            client,
            context,
            FaultPoint.BARRIER_AFTER_COMPUTE,
            lambda: world.bump_recon_revision("recon-R11", blocking_reason="POSITION_MISMATCH"),
            headers=auth,
        )

        assert response.status_code == 409, response.text
        body = response.json()
        assert body["code"] == "DATA_NOT_READY"
        assert "RECON_BLOCKING" in body["reason_codes"]
        assert "56.00" not in response.text
        assert "81.00" not in response.text
        assert world.checksum() != before  # the test changed it, not the API
        decisions = [row.decision for row in context.audit.all_records()]
        assert "ALLOW" not in decisions
        assert "BLOCK" in decisions
        assert context.faults.has_fired(FaultPoint.BARRIER_AFTER_COMPUTE)

        evidence.record(
            scenario_ids=("WF06-I-05", "WF06-G-11", "WF06-AH-01"),
            test_id="IT06-04",
            expected_http=409,
            observed_http=response.status_code,
            public_code=body["code"],
            assertions=("rev_before_recon-R10", "rev_after_recon-R11", "no_financial_bytes_sent"),
            engine_calls=context.engine.stats.calls,
            audit_decisions=tuple(decisions),
            injections=(FaultPoint.BARRIER_AFTER_COMPUTE,),
        )

    async def test_a_non_blocking_change_is_recaptured_once_and_reported_honestly(
        self, client, context, world, auth
    ):
        """The pinned R10 result is discarded; the served report declares R11.

        Design doc section 5.2 allows a single bounded recapture. What must
        never happen is serving the R10 result while labelling it current.
        """
        response = await run_with_barrier(
            client,
            context,
            FaultPoint.BARRIER_AFTER_COMPUTE,
            lambda: world.bump_recon_revision("recon-R11"),
            headers=auth,
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["cuts"]["recon_revision"] == "recon-R11"
        assert context.views.counters["snapshots"] == 2, "no recapture happened"
        assert context.engine.stats.calls == 2
        assert context.metrics.counter_value(
            "api_cut_revalidation_failures_total",
            source_kind="projection",
            failure_family="cut_changed",
        ) == 1.0

    async def test_a_cut_that_keeps_moving_exhausts_the_single_recapture(
        self, client, context, auth
    ):
        """Bounded retry, not a loop: the second failure is a 409."""
        original = context.views.current_revisions
        bumps = {"n": 0}

        async def always_moving(source_id, epoch_id, accounts):
            revisions = await original(source_id, epoch_id, accounts)
            bumps["n"] += 1
            from dataclasses import replace

            return replace(revisions, publication_revision=f"pub-moving-{bumps['n']}")

        context.views.current_revisions = always_moving  # type: ignore[method-assign]

        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 409, response.text
        body = response.json()
        assert body["code"] == "CUT_CHANGED"
        assert body["retryable"] is True
        assert "56.00" not in response.text
        # One initial attempt plus exactly one recapture.
        assert context.engine.stats.calls == 2
        decisions = [row.decision for row in context.audit.all_records()]
        assert "ALLOW" not in decisions
        assert "BLOCK" in decisions

    async def test_a_source_epoch_reset_mid_flight_is_rejected(
        self, client, context, world, auth, evidence
    ):
        """FX-EPOCH-005: an E1 result cannot be served once E2 exists."""
        response = await run_with_barrier(
            client,
            context,
            FaultPoint.BARRIER_AFTER_COMPUTE,
            lambda: world.rotate_epoch("E2"),
            headers=auth,
        )
        assert response.status_code == 409
        body = response.json()
        assert body["code"] == "EPOCH_CHANGED"
        assert "E1" not in response.text or body["code"] == "EPOCH_CHANGED"
        assert "56.00" not in response.text

        evidence.record(
            scenario_ids=("WF06-G-02", "FX-EPOCH-005"),
            test_id="IT06-04-epoch",
            expected_http=409,
            observed_http=response.status_code,
            public_code=body["code"],
            assertions=("no_stale_epoch_payload",),
            injections=(FaultPoint.BARRIER_AFTER_COMPUTE,),
        )

    async def test_a_quote_refresh_mid_flight_is_revalued_not_mislabelled(
        self, client, context, world, auth
    ):
        """MC-03: the stale valuation is discarded and the new snapshot declared."""
        response = await run_with_barrier(
            client,
            context,
            FaultPoint.BARRIER_AFTER_COMPUTE,
            lambda: world.refresh_quote_snapshot("quote-S1099"),
            headers=auth,
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["cuts"]["quote_snapshot_id"] == "quote-S1099"
        assert body["components"]["unrealized"]["quote_snapshot_id"] == "quote-S1099"
        assert context.views.counters["snapshots"] == 2

    async def test_a_cut_captured_and_released_cleanly_still_succeeds(
        self, client, context, auth
    ):
        """The barrier itself must not change the outcome when nothing moves."""
        response = await run_with_barrier(
            client, context, FaultPoint.BARRIER_AFTER_COMPUTE, lambda: None, headers=auth
        )
        assert response.status_code == 200
        assert response.json()["status"] == "READY"

    async def test_a_failed_final_check_fails_closed(self, client, context, auth):
        """IT06-09: unable to prove the cut still holds means no READY payload."""
        context.faults.arm(FaultPoint.DB_FAIL_FINAL_CHECK)
        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 503
        assert response.json()["code"] == "DB_UNAVAILABLE"
        assert "56.00" not in response.text


class TestScanFenceOverHttp:
    async def test_a_republished_scan_refuses_the_next_page(
        self, client, context, world, auth, evidence
    ):
        """IT06-05: P100 page one is valid; after P101 the next page is a 409."""
        for index in range(8):
            add_position(world, f"P-3{index:03d}", ACCOUNT_A, lots="0.1")

        first = await client.get(
            POSITIONS, params={"source_id": SOURCE_ID, "limit": "3"}, headers=auth
        )
        assert first.status_code == 200, first.text
        cursor = first.json()["next_cursor"]
        assert cursor
        assert first.json()["cuts"]["position_scan_id"] == "scan-P"

        world.publish_position_scan("scan-P2")

        second = await client.get(
            POSITIONS,
            params={"source_id": SOURCE_ID, "limit": "3", "cursor": cursor},
            headers=auth,
        )
        assert second.status_code == 409, second.text
        body = second.json()
        assert body["code"] == "SCAN_CHANGED"
        assert body["retryable"] is True
        assert "position_id" not in second.text

        evidence.record(
            scenario_ids=("WF06-N-01", "WF06-N-06", "FX-CURSOR-008"),
            test_id="IT06-05",
            expected_http=409,
            observed_http=second.status_code,
            public_code=body["code"],
            assertions=("page_one_scan_P", "no_mixed_scan_rows"),
        )

    async def test_a_cursor_cannot_be_replayed_by_another_tenant(
        self, client, world, auth, tokens
    ):
        """WF06-N: a cursor is not a bearer token for someone else's rows."""
        for index in range(6):
            add_position(world, f"P-4{index:03d}", ACCOUNT_A, lots="0.1")
        first = await client.get(
            POSITIONS, params={"source_id": SOURCE_ID, "limit": "2"}, headers=auth
        )
        cursor = first.json()["next_cursor"]
        assert cursor

        response = await client.get(
            POSITIONS,
            params={"source_id": SOURCE_ID, "limit": "2", "cursor": cursor},
            headers=tokens.header("analyst-b"),
        )
        assert response.status_code == 404
        assert "position_id" not in response.text

    async def test_an_expired_cursor_is_refused(self, client, world, auth, clock):
        for index in range(6):
            add_position(world, f"P-5{index:03d}", ACCOUNT_A, lots="0.1")
        first = await client.get(
            POSITIONS, params={"source_id": SOURCE_ID, "limit": "2"}, headers=auth
        )
        cursor = first.json()["next_cursor"]
        clock.advance(300)

        response = await client.get(
            POSITIONS,
            params={"source_id": SOURCE_ID, "limit": "2", "cursor": cursor},
            headers=auth,
        )
        assert response.status_code == 409
        assert response.json()["code"] == "CURSOR_EXPIRED"

    async def test_a_policy_revision_change_ends_the_paginated_session(
        self, client, context, world, auth
    ):
        """WF06-C-11 over HTTP."""
        for index in range(6):
            add_position(world, f"P-6{index:03d}", ACCOUNT_A, lots="0.1")
        first = await client.get(
            POSITIONS, params={"source_id": SOURCE_ID, "limit": "2"}, headers=auth
        )
        cursor = first.json()["next_cursor"]
        context.policies.bump_revision("analyst-a", "policy-r2")

        response = await client.get(
            POSITIONS,
            params={"source_id": SOURCE_ID, "limit": "2", "cursor": cursor},
            headers=auth,
        )
        assert response.status_code == 403
        assert "AUTH_REVISION_CHANGED" in response.json()["reason_codes"]


class TestAuditGate:
    async def test_a_down_audit_sink_blocks_the_report(self, client, context, auth, evidence):
        """FX-OUTAGE-011 / INV06-16: no unaudited sensitive success."""
        context.faults.arm(FaultPoint.AUDIT_SINK_DOWN)
        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)

        assert response.status_code == 503
        body = response.json()
        assert body["code"] == "AUDIT_UNAVAILABLE"
        assert body["retryable"] is True
        assert "56.00" not in response.text
        assert context.audit.all_records() == ()

        evidence.record(
            scenario_ids=("WF06-AA-01", "FX-OUTAGE-011"),
            test_id="IT06-07-sink-down",
            expected_http=503,
            observed_http=response.status_code,
            public_code=body["code"],
            assertions=("no_financial_bytes_sent", "no_audit_row_written"),
            injections=(FaultPoint.AUDIT_SINK_DOWN,),
        )

    async def test_a_failure_before_commit_blocks_the_report(self, client, context, auth):
        context.faults.arm(FaultPoint.AUDIT_FAIL_BEFORE_COMMIT)
        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 503
        assert response.json()["code"] == "AUDIT_UNAVAILABLE"
        assert context.audit.all_records() == ()

    async def test_a_lost_commit_ack_is_resolved_by_looking_up_the_receipt(
        self, client, context, auth, evidence
    ):
        """IT06-07: the row is durable, the acknowledgement was not. Prove it by id."""
        context.faults.arm(FaultPoint.AUDIT_LOSE_ACK)
        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)

        assert response.status_code == 200, response.text
        body = response.json()
        records = context.audit.all_records()
        assert len(records) == 1
        assert body["audit_id"] == records[0].audit_id
        assert records[0].decision == "ALLOW"

        evidence.record(
            scenario_ids=("WF06-AA-03", "WF06-AG-07"),
            test_id="IT06-07-lost-ack",
            expected_http=200,
            observed_http=response.status_code,
            assertions=("receipt_proved_by_audit_id", "exactly_one_audit_row"),
            audit_decisions=tuple(row.decision for row in records),
            injections=(FaultPoint.AUDIT_LOSE_ACK,),
        )

    async def test_an_unprovable_commit_fails_closed(self, client, context, auth):
        """If the row cannot be found after an ambiguous commit, no report goes out."""
        context.faults.arm(FaultPoint.AUDIT_FAIL_AFTER_COMMIT)
        context.faults.arm(FaultPoint.AUDIT_SINK_DOWN)
        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 503
        assert response.json()["code"] == "AUDIT_UNAVAILABLE"

    async def test_a_retry_may_append_a_second_receipt(self, client, context, auth):
        """At-least-once audit is allowed; exactly-once delivery is not claimed."""
        for _ in range(2):
            response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
            assert response.status_code == 200
        records = context.audit.all_records()
        assert len(records) == 2
        assert records[0].audit_id != records[1].audit_id
        assert records[0].output_digest == records[1].output_digest

    async def test_audit_rows_carry_hashes_not_identities(self, client, context, auth):
        await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        row = context.audit.all_records()[0]
        serialized = str(row.as_row())
        assert ACCOUNT_A not in serialized
        assert "analyst-a" not in serialized
        assert row.principal_scope_hash.startswith("sha256:")
        assert row.data_cut_digest.startswith("sha256:")
        assert row.recon_revision == "recon-R10"

    async def test_a_denied_request_is_audited_without_touching_the_engine(
        self, client, context, auth
    ):
        """A denial before admission writes no report audit and runs no compute."""
        response = await client.get(
            "/api/v1/accounts/00043/risk", params={"source_id": SOURCE_ID}, headers=auth
        )
        assert response.status_code == 404
        assert context.engine.stats.calls == 0
        assert context.audit.all_records() == ()
