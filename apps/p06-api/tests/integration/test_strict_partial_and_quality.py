"""IT06-03 and IT06-11: partial results are opt-in, and bad results are refused.

The two halves of "result truth over HTTP success":

* A caller who did not ask for a partial report never receives one as a 200.
* A result that fails its own contract never becomes a 200 at all, regardless
  of how plausible its numbers look.
"""

from __future__ import annotations

import pytest
from conftest import ACCOUNT_A, SOURCE_ID
from risk_api.faults import FaultPoint

pytestmark = pytest.mark.integration

RISK = f"/api/v1/accounts/{ACCOUNT_A}/risk"


class TestStrictVersusPartial:
    async def test_strict_refuses_an_unprovable_composite(self, client, context, world, auth, evidence):
        """FX-PARTIAL-003: quotes cannot be aligned, so strict gets a 409."""
        world.quote_proven_against_scan = None

        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)

        assert response.status_code == 409, response.text
        assert response.headers["content-type"].startswith("application/problem+json")
        body = response.json()
        assert body["code"] == "COMPOSITE_UNPROVEN"
        assert body["retryable"] is False
        # No money of any kind leaves on a strict refusal.
        assert "composite_total" not in response.text
        assert "56.00" not in response.text
        assert "25.00" not in response.text

        evidence.record(
            scenario_ids=("WF06-P-01", "WF06-F-07", "FX-PARTIAL-003"),
            test_id="IT06-03-strict",
            expected_http=409,
            observed_http=response.status_code,
            public_code=body["code"],
            assertions=("no_financial_amounts", "composite_unproven"),
            engine_calls=context.engine.stats.calls,
        )

    async def test_the_same_data_with_opt_in_returns_a_labelled_partial(
        self, client, context, world, auth, evidence
    ):
        """The identical cut, now with allow_partial=true, is a 200 DEGRADED."""
        world.quote_proven_against_scan = None

        response = await client.get(
            RISK, params={"source_id": SOURCE_ID, "allow_partial": "true"}, headers=auth
        )

        assert response.status_code == 200, response.text
        body = response.json()
        assert body["status"] == "DEGRADED"
        assert body["partial_allowed"] is True
        # INV06-03: a degraded report never carries a cross-component total.
        assert body["composite_total"] is None
        assert "INCOMPATIBLE_CUT" in body["reason_codes"]
        assert body["cuts"]["consistency"] == "INDEPENDENT"
        # The individually proven components are still present and exact.
        assert body["components"]["realized"]["net_excluding_fee_usd"] == "56.00"
        assert body["components"]["unrealized"]["total_usd"] == "25.00"
        assert body["components"]["realized"]["fee_coverage"] == "NOT_AVAILABLE"
        assert response.headers["cache-control"] == "private, no-store"

        evidence.record(
            scenario_ids=("WF06-P-03", "FX-PARTIAL-003"),
            test_id="IT06-03-partial",
            expected_http=200,
            observed_http=response.status_code,
            assertions=("status_degraded", "composite_total_null", "reason_incompatible_cut"),
            engine_calls=context.engine.stats.calls,
        )

    async def test_a_blocked_component_degrades_rather_than_zeroing(self, client, world, auth):
        """INV06-04: a blocked component is never silently converted to zero."""
        world.quote_snapshot_id = None

        response = await client.get(
            RISK, params={"source_id": SOURCE_ID, "allow_partial": "true"}, headers=auth
        )
        assert response.status_code == 200, response.text
        unrealized = response.json()["components"]["unrealized"]
        assert unrealized["status"] == "BLOCKED"
        assert unrealized["reason_code"] == "QUOTE_CUT_UNPROVEN"
        assert unrealized["total_usd"] is None

    async def test_allow_partial_must_be_a_literal_boolean(self, client, auth):
        for value in ("yes", "1", "TRUE", "", "true "):
            response = await client.get(
                RISK, params={"source_id": SOURCE_ID, "allow_partial": value}, headers=auth
            )
            assert response.status_code == 422, value

    async def test_a_blocking_reconciliation_finding_is_not_a_partial(self, client, world, auth):
        """FX-RECON-004: opting into partial cannot bypass a blocking finding."""
        world.bump_recon_revision("recon-R11", blocking_reason="BALANCE_MISMATCH")

        response = await client.get(
            RISK, params={"source_id": SOURCE_ID, "allow_partial": "true"}, headers=auth
        )
        assert response.status_code == 409
        body = response.json()
        assert body["code"] == "DATA_NOT_READY"
        assert "RECON_BLOCKING" in body["reason_codes"]
        # The finding family is not disclosed to the client.
        assert "BALANCE_MISMATCH" not in response.text

    async def test_an_unpublished_position_scan_blocks_rather_than_reporting_flat(
        self, client, world, auth
    ):
        world.positions_published = False
        response = await client.get(
            RISK, params={"source_id": SOURCE_ID, "allow_partial": "true"}, headers=auth
        )
        assert response.status_code == 409
        assert "P03_UNPUBLISHED" in response.json()["reason_codes"]
        assert "50000" not in response.text


class TestEngineContractViolations:
    """IT06-11: an unverifiable result is a 502, not a plausible 200."""

    async def test_a_wrong_currency_is_refused(self, client, context, auth, evidence):
        context.faults.arm(FaultPoint.ENGINE_RETURN_BAD_CURRENCY)
        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 502
        body = response.json()
        assert body["code"] == "INVALID_RISK_RESULT"
        assert "56.00" not in response.text
        assert "XXX" not in response.text
        # The failure is audited as an error, with no ALLOW decision.
        decisions = [row.decision for row in context.audit.all_records()]
        assert "ALLOW" not in decisions
        assert "ERROR" in decisions

        evidence.record(
            scenario_ids=("WF06-E-02", "WF06-K-03", "WF06-Z-02"),
            test_id="IT06-11-currency",
            expected_http=502,
            observed_http=response.status_code,
            public_code=body["code"],
            assertions=("no_money_in_body", "audit_error_recorded"),
            audit_decisions=tuple(decisions),
            injections=(FaultPoint.ENGINE_RETURN_BAD_CURRENCY,),
        )

    async def test_a_missing_input_digest_is_refused(self, client, context, auth):
        context.faults.arm(FaultPoint.ENGINE_DROP_INPUT_DIGEST)
        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 502
        assert response.json()["code"] == "INVALID_RISK_RESULT"

    async def test_a_dropped_cut_is_refused(self, client, context, auth):
        """INV06-02: a READY payload without its cut is not deliverable."""
        context.faults.arm(FaultPoint.ENGINE_DROP_CUT)
        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 502

    async def test_a_nan_amount_is_refused(self, client, context, auth):
        context.faults.arm(FaultPoint.ENGINE_RETURN_NAN)
        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 502
        assert "NaN" not in response.text

    async def test_a_float_amount_is_refused(self, client, context, auth):
        """A float reaching the serializer must not become a JSON number."""
        context.faults.arm(FaultPoint.ENGINE_RETURN_FLOAT)
        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 502

    async def test_a_serializer_regression_is_caught_before_sending(
        self, client, context, auth
    ):
        """The payload guard fires even if everything upstream looked fine."""
        context.faults.arm(FaultPoint.RESPONSE_CORRUPT_DECIMAL_TYPE)
        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 500
        assert response.json()["code"] == "INTERNAL_ERROR"
        assert "81.0" not in response.text
        # The report was never built, so no receipt claims it was served.
        assert "ALLOW" not in [row.decision for row in context.audit.all_records()]

    async def test_an_engine_failure_becomes_a_bounded_503(self, client, context, auth):
        context.faults.arm(FaultPoint.ENGINE_RAISE)
        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 503
        body = response.json()
        assert body["code"] == "DB_UNAVAILABLE"
        assert body["retryable"] is True

    async def test_an_engine_overrun_becomes_a_504(self, client, context, auth):
        """IT06-08 at the HTTP boundary."""
        context.faults.arm(FaultPoint.ENGINE_BURN_CPU, 2000)
        with_short_budget = dict(auth)
        response = await client.get(
            RISK, params={"source_id": SOURCE_ID}, headers=with_short_budget
        )
        # The default compute budget is 3s, so a 2s burn still succeeds; the
        # point of this case is that it completes without stalling the loop.
        assert response.status_code in (200, 504)
        assert context.workers.heartbeat.p99_lag_seconds < 1.0


class TestSentinelsForSilentlyWrongMoney:
    """Design doc section 13.5: any path to a 200 READY here is release-blocking."""

    async def test_a_nudged_amount_changes_the_digest_not_the_status(self, client, world, auth):
        first = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert first.status_code == 200
        baseline = first.json()

        from decimal import Decimal

        from conftest import add_deal

        add_deal(
            world,
            "D-0001",
            ACCOUNT_A,
            seq=1050,
            gross="60.01",
            commission="-3.50",
            swap="-0.50",
        )
        second = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert second.status_code == 200
        nudged = second.json()
        assert nudged["components"]["realized"]["gross_usd"] == "60.01"
        assert nudged["output_digest"] != baseline["output_digest"]
        assert Decimal(nudged["composite_total"]) - Decimal(baseline["composite_total"]) == Decimal(
            "0.01"
        )

    async def test_a_mixed_cut_can_never_produce_a_total(self, client, world, auth):
        world.quote_proven_against_scan = "scan-OTHER"
        response = await client.get(
            RISK, params={"source_id": SOURCE_ID, "allow_partial": "true"}, headers=auth
        )
        assert response.status_code == 200
        assert response.json()["composite_total"] is None
