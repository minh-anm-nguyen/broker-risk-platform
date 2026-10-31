"""IT06-02 and IT06-06: the authorization matrix, and session/write isolation.

The matrix is exhaustive across principals, endpoints and target accounts, so
a new endpoint that forgets its permission check fails here rather than in
production. The isolation tests prove Project 06 neither writes upstream nor
shares a read session between concurrent requests.
"""

from __future__ import annotations

import asyncio

import pytest
from conftest import ACCOUNT_A, ACCOUNT_B, SOURCE_ID, add_position
from risk_api.repositories.read_views import world_table_checksums

pytestmark = pytest.mark.integration

WINDOW = {"period_start": "2026-10-01T00:00:00Z", "period_end": "2026-10-30T00:00:00Z"}


def endpoints(account: str) -> dict[str, tuple[str, dict[str, str]]]:
    return {
        "risk": (f"/api/v1/accounts/{account}/risk", {"source_id": SOURCE_ID}),
        "pnl": (f"/api/v1/accounts/{account}/pnl", {"source_id": SOURCE_ID, **WINDOW}),
        "positions": ("/api/v1/positions", {"source_id": SOURCE_ID, "account_id": account}),
        "exposure": ("/api/v1/exposure", {"source_id": SOURCE_ID, "account_ids": account}),
        "daily": (
            "/api/v1/reports/daily",
            {"source_id": SOURCE_ID, "date": "2026-10-15", "account_id": account},
        ),
    }


class TestObjectLevelAuthorization:
    @pytest.mark.parametrize("name", sorted(endpoints(ACCOUNT_A)))
    async def test_the_owner_can_read_their_own_account(self, client, auth, name):
        path, params = endpoints(ACCOUNT_A)[name]
        response = await client.get(path, params=params, headers=auth)
        assert response.status_code == 200, f"{name}: {response.text}"

    @pytest.mark.parametrize("name", sorted(endpoints(ACCOUNT_B)))
    async def test_every_endpoint_masks_another_tenants_account(
        self, client, context, auth, name
    ):
        """IT06-02: no endpoint is a side door into account 00043."""
        path, params = endpoints(ACCOUNT_B)[name]
        response = await client.get(path, params=params, headers=auth)
        assert response.status_code in (403, 404), f"{name}: {response.status_code}"
        assert ACCOUNT_B not in response.text, name
        assert context.engine.stats.calls == 0, name

    @pytest.mark.parametrize("name", sorted(endpoints(ACCOUNT_B)))
    async def test_the_other_tenant_can_read_their_own(self, client, tokens, name):
        path, params = endpoints(ACCOUNT_B)[name]
        response = await client.get(path, params=params, headers=tokens.header("analyst-b"))
        assert response.status_code == 200, f"{name}: {response.text}"
        assert ACCOUNT_A not in response.text

    async def test_a_nonexistent_account_is_indistinguishable_from_a_forbidden_one(
        self, client, auth
    ):
        """WF06-C-01: the two refusals must look the same to a prober."""
        forbidden = await client.get(
            f"/api/v1/accounts/{ACCOUNT_B}/risk", params={"source_id": SOURCE_ID}, headers=auth
        )
        absent = await client.get(
            "/api/v1/accounts/99999/risk", params={"source_id": SOURCE_ID}, headers=auth
        )
        assert forbidden.status_code == absent.status_code == 404
        a, b = forbidden.json(), absent.json()
        for body in (a, b):
            body.pop("request_id")
            body.pop("instance")
        assert a == b

    async def test_an_unknown_source_is_masked(self, client, auth):
        """WF06-C-04: source scope is bound, so another source is simply not found."""
        response = await client.get(
            f"/api/v1/accounts/{ACCOUNT_A}/risk",
            params={"source_id": "other-broker"},
            headers=auth,
        )
        assert response.status_code == 404
        assert "other-broker" not in response.text

    async def test_a_revoked_account_stops_being_readable(self, client, context, auth):
        first = await client.get(
            f"/api/v1/accounts/{ACCOUNT_A}/risk", params={"source_id": SOURCE_ID}, headers=auth
        )
        assert first.status_code == 200

        context.policies.revoke_account("analyst-a", SOURCE_ID, ACCOUNT_A, revision="policy-r2")
        second = await client.get(
            f"/api/v1/accounts/{ACCOUNT_A}/risk", params={"source_id": SOURCE_ID}, headers=auth
        )
        assert second.status_code == 404


class TestFunctionLevelAuthorization:
    async def test_a_viewer_cannot_submit_a_calculation(self, client, context, tokens):
        """WF06-C-07 / BFLA: the role, not the token, decides."""
        response = await client.post(
            "/api/v1/risk-calculations",
            json={"source_id": SOURCE_ID, "account_ids": [ACCOUNT_A], "components": ["EXPOSURE"]},
            headers=tokens.header("viewer-a", roles=("risk_viewer",)),
        )
        assert response.status_code == 403
        assert context.engine.stats.calls == 0

    async def test_a_viewer_cannot_read_the_daily_report(self, client, tokens):
        response = await client.get(
            "/api/v1/reports/daily",
            params={"source_id": SOURCE_ID, "date": "2026-10-15", "account_id": ACCOUNT_A},
            headers=tokens.header("viewer-a", roles=("risk_viewer",)),
        )
        assert response.status_code == 403

    async def test_the_admin_role_gets_no_data_reads(self, client, tokens):
        """Policy administration happens outside this API."""
        response = await client.get(
            f"/api/v1/accounts/{ACCOUNT_A}/risk",
            params={"source_id": SOURCE_ID},
            headers=tokens.header("analyst-a", roles=("risk_admin",)),
        )
        assert response.status_code == 403

    async def test_a_token_scope_cannot_add_a_permission_the_role_lacks(self, client, tokens):
        """A token asking for aggregate on a viewer role still gets nothing."""
        response = await client.get(
            "/api/v1/exposure",
            params={"source_id": SOURCE_ID, "account_ids": f"{ACCOUNT_A},{ACCOUNT_B}"},
            headers=tokens.header(
                "viewer-a", roles=("risk_viewer",), scopes=("risk:read_aggregate",)
            ),
        )
        assert response.status_code == 403

    async def test_a_token_scope_can_narrow_a_role(self, client, tokens):
        """Least privilege works in the safe direction."""
        allowed = await client.get(
            f"/api/v1/accounts/{ACCOUNT_A}/risk",
            params={"source_id": SOURCE_ID},
            headers=tokens.header("analyst-a", scopes=("risk:read_account",)),
        )
        assert allowed.status_code == 200

        narrowed = await client.get(
            f"/api/v1/accounts/{ACCOUNT_A}/pnl",
            params={"source_id": SOURCE_ID, **WINDOW},
            headers=tokens.header("analyst-a", scopes=("risk:read_account",)),
        )
        assert narrowed.status_code == 403


class TestUpstreamIsNeverWritten:
    @pytest.mark.parametrize(
        ("method", "path", "params", "body"),
        [
            ("GET", f"/api/v1/accounts/{ACCOUNT_A}/risk", {"source_id": SOURCE_ID}, None),
            ("GET", f"/api/v1/accounts/{ACCOUNT_A}/pnl", {"source_id": SOURCE_ID, **WINDOW}, None),
            ("GET", "/api/v1/positions", {"source_id": SOURCE_ID}, None),
            ("GET", "/api/v1/exposure", {"source_id": SOURCE_ID}, None),
            (
                "GET",
                "/api/v1/reports/daily",
                {"source_id": SOURCE_ID, "date": "2026-10-15", "account_id": ACCOUNT_A},
                None,
            ),
            (
                "POST",
                "/api/v1/risk-calculations",
                None,
                {"source_id": SOURCE_ID, "account_ids": [ACCOUNT_A], "components": ["EXPOSURE"]},
            ),
        ],
    )
    async def test_no_endpoint_changes_upstream_state(
        self, client, world, auth, method, path, params, body
    ):
        """INV06-20, asserted per table rather than for the world as a whole."""
        before = world_table_checksums(world)
        response = await client.request(method, path, params=params, json=body, headers=auth)
        assert response.status_code == 200, response.text
        assert world_table_checksums(world) == before


class TestSessionAndPoolIsolation:
    async def test_concurrent_reads_never_share_a_session(self, client, context, auth):
        """IT06-06: sessions are per task, and all of them are returned."""
        responses = await asyncio.gather(
            *(
                client.get(
                    f"/api/v1/accounts/{ACCOUNT_A}/risk",
                    params={"source_id": SOURCE_ID},
                    headers=auth,
                )
                for _ in range(8)
            )
        )
        # Eight heavy reads at weight 4 fit inside the 40-token burst, so this
        # exercises session isolation rather than the rate limiter.
        assert all(response.status_code == 200 for response in responses), [
            response.status_code for response in responses
        ]
        digests = {response.json()["output_digest"] for response in responses}
        assert len(digests) == 1, "concurrent reads disagreed on the same cut"
        assert context.views.open_sessions == 0, "a read session leaked"
        assert context.views.peak_open_sessions <= context.views.pool_size

    async def test_a_cancelled_request_returns_its_session(self, client, context, world, auth):
        """A client that disconnects must not leave a session checked out."""
        for index in range(30):
            add_position(world, f"P-7{index:03d}", ACCOUNT_A, lots="0.1")

        task = asyncio.create_task(
            client.get("/api/v1/positions", params={"source_id": SOURCE_ID}, headers=auth)
        )
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises((asyncio.CancelledError, Exception)):
            await task
        await asyncio.sleep(0.05)
        assert context.views.open_sessions == 0

    async def test_pool_pressure_surfaces_as_a_typed_503(
        self, client, context, auth
    ):
        """WF06-U-02: pool exhaustion is DB_POOL_BUSY, not an unbounded wait."""
        from risk_api.faults import FaultPoint

        context.faults.arm(FaultPoint.DB_POOL_EXHAUSTED)
        response = await client.get(
            f"/api/v1/accounts/{ACCOUNT_A}/risk", params={"source_id": SOURCE_ID}, headers=auth
        )
        assert response.status_code == 503
        body = response.json()
        assert body["code"] == "DB_POOL_BUSY"
        assert body["retryable"] is True

    async def test_a_statement_timeout_surfaces_as_a_typed_503(self, client, context, auth):
        from risk_api.faults import FaultPoint

        context.faults.arm(FaultPoint.DB_STATEMENT_TIMEOUT)
        response = await client.get(
            f"/api/v1/accounts/{ACCOUNT_A}/risk", params={"source_id": SOURCE_ID}, headers=auth
        )
        assert response.status_code == 503
        assert response.json()["code"] == "DB_UNAVAILABLE"
        assert context.views.open_sessions == 0

    async def test_a_projection_outage_before_the_snapshot_is_a_503(self, client, context, auth):
        from risk_api.faults import FaultPoint

        context.faults.arm(FaultPoint.DB_FAIL_BEFORE_SNAPSHOT)
        response = await client.get(
            f"/api/v1/accounts/{ACCOUNT_A}/risk", params={"source_id": SOURCE_ID}, headers=auth
        )
        assert response.status_code == 503
        assert context.views.open_sessions == 0
        assert context.engine.stats.calls == 0

    async def test_one_report_uses_one_snapshot(self, client, context, auth):
        """INV06-07: a single cut manifest per response, not per query."""
        await client.get(
            f"/api/v1/accounts/{ACCOUNT_A}/risk", params={"source_id": SOURCE_ID}, headers=auth
        )
        assert context.views.counters["snapshots"] == 1
        assert context.views.counters["final_checks"] == 1
