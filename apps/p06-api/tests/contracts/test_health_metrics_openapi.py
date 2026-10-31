"""Health semantics, internal metrics access and the published schema.

WF06-AC and WF06-Z. Liveness must not depend on the database, readiness must
report coarse reasons only, metrics must not be reachable by a report-reading
role, and the OpenAPI document must not describe money as a number.
"""

from __future__ import annotations

import pytest
from conftest import ACCOUNT_A, SOURCE_ID
from risk_api.faults import FaultPoint

pytestmark = pytest.mark.contract


class TestLiveness:
    async def test_liveness_answers_from_the_event_loop_alone(self, client, context):
        """A projection outage must not restart every replica."""
        context.faults.arm(FaultPoint.DB_FAIL_BEFORE_SNAPSHOT)
        response = await client.get("/health/live")
        assert response.status_code == 200
        assert response.json()["status"] == "live"
        assert context.views.counters.get("snapshots", 0) == 0

    async def test_liveness_needs_no_credential(self, client):
        assert (await client.get("/health/live")).status_code == 200


class TestReadiness:
    async def test_a_healthy_instance_is_ready(self, client):
        response = await client.get("/health/ready")
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "ready"
        assert set(body["checks"].values()) == {"ok"}

    async def test_a_projection_outage_makes_the_instance_not_ready(self, client, context):
        context.faults.arm(FaultPoint.DB_FAIL_BEFORE_SNAPSHOT)
        response = await client.get("/health/ready")
        assert response.status_code == 503
        assert response.json()["checks"]["projection_store"] == "failed"

    async def test_an_audit_outage_makes_the_instance_not_ready(self, client, context):
        context.faults.arm(FaultPoint.AUDIT_SINK_DOWN)
        response = await client.get("/health/ready")
        assert response.status_code == 503
        assert response.json()["checks"]["audit_sink"] == "failed"

    async def test_readiness_reveals_no_internal_detail(self, client, context):
        """Coarse names only: no hostnames, connection strings or secrets."""
        context.faults.arm(FaultPoint.DB_FAIL_BEFORE_SNAPSHOT)
        text = (await client.get("/health/ready")).text
        for leak in ("postgres", "password", "dev-only", "localhost", "127.0.0.1", "Traceback"):
            assert leak not in text


class TestInternalMetrics:
    async def test_a_report_reading_role_cannot_scrape_metrics(self, client, auth):
        """WF06-C-06: least privilege does not include the metrics surface."""
        response = await client.get("/internal/metrics", headers=auth)
        assert response.status_code == 403
        assert response.json()["code"] == "ACCESS_DENIED"

    async def test_an_unauthenticated_scrape_is_refused(self, client):
        assert (await client.get("/internal/metrics")).status_code == 401

    async def test_the_ops_role_can_scrape(self, client, tokens):
        header = tokens.header("ops-a", roles=("ops_reader",))
        response = await client.get("/internal/metrics", headers=header)
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/plain")
        assert "api_http_requests_total" in response.text

    async def test_metrics_never_contain_an_account_number(self, client, auth, tokens):
        """INV06-01: the scrape is not a side channel for account existence."""
        await client.get(
            f"/api/v1/accounts/{ACCOUNT_A}/risk", params={"source_id": SOURCE_ID}, headers=auth
        )
        body = (
            await client.get("/internal/metrics", headers=tokens.header("ops-a", roles=("ops_reader",)))
        ).text
        assert ACCOUNT_A not in body
        assert "analyst-a" not in body
        assert "sha256:" not in body

    async def test_the_unauthorized_access_counter_is_zero(self, client, auth, tokens):
        """The critical signal from design doc section 10.1."""
        await client.get(
            f"/api/v1/accounts/{ACCOUNT_A}/risk", params={"source_id": SOURCE_ID}, headers=auth
        )
        await client.get("/api/v1/accounts/00043/risk", params={"source_id": SOURCE_ID}, headers=auth)
        body = (
            await client.get("/internal/metrics", headers=tokens.header("ops-a", roles=("ops_reader",)))
        ).text
        for line in body.splitlines():
            if line.startswith("api_unauthorized_data_access_total{"):
                assert line.rsplit(" ", 1)[1] in ("0.0", "0")

    async def test_route_labels_are_templates(self, client, auth, tokens):
        await client.get(
            f"/api/v1/accounts/{ACCOUNT_A}/risk", params={"source_id": SOURCE_ID}, headers=auth
        )
        body = (
            await client.get("/internal/metrics", headers=tokens.header("ops-a", roles=("ops_reader",)))
        ).text
        assert "{account_id}" in body

    async def test_a_denial_is_counted_by_family(self, client, auth, tokens):
        await client.get("/api/v1/accounts/00043/risk", params={"source_id": SOURCE_ID}, headers=auth)
        body = (
            await client.get("/internal/metrics", headers=tokens.header("ops-a", roles=("ops_reader",)))
        ).text
        assert "api_scope_denials_total" in body
        assert "account_out_of_scope" in body

    async def test_metrics_can_be_disabled_entirely(self, settings_kwargs, world, policies, clock):
        """A disabled internal route hides its existence rather than advertising it."""
        from httpx import ASGITransport, AsyncClient
        from risk_api.faults import FaultRegistry
        from risk_api.lifespan import build_context
        from risk_api.main import create_app
        from risk_api.settings import Settings

        settings = Settings(**{**settings_kwargs, "expose_internal_metrics": False})
        context = build_context(
            settings=settings,
            world=world,
            policies=policies,
            faults=FaultRegistry(enabled=True),
            clock=clock.monotonic,
            wall_clock=clock.wall,
        )
        app = create_app(context=context, settings=settings)
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as http:
            response = await http.get("/internal/metrics")
        assert response.status_code == 404


class TestOpenApi:
    async def test_the_document_is_served(self, client):
        response = await client.get("/openapi.json")
        assert response.status_code == 200
        assert response.json()["openapi"].startswith("3.")

    async def test_every_v1_route_is_described(self, client):
        paths = (await client.get("/openapi.json")).json()["paths"]
        expected = {
            "/api/v1/accounts/{account_id}/risk",
            "/api/v1/accounts/{account_id}/pnl",
            "/api/v1/positions",
            "/api/v1/exposure",
            "/api/v1/reports/daily",
            "/api/v1/risk-calculations",
            "/health/live",
            "/health/ready",
            "/internal/metrics",
        }
        assert expected <= set(paths)

    async def test_the_response_schemas_are_published(self, client):
        """FR06-14: the contract has to be pinned to be consumer-testable."""
        schemas = (await client.get("/openapi.json")).json()["components"]["schemas"]
        expected = {
            "RiskResponse",
            "PnlResponse",
            "PositionsPage",
            "ExposureResponse",
            "DailyReport",
            "ProblemResponse",
            "RealizedComponentOut",
            "ExposureComponentOut",
            "CutsBlock",
            "ScopeBlock",
            "CoverageBlock",
        }
        assert expected <= set(schemas), sorted(expected - set(schemas))

    async def test_money_is_never_declared_as_a_number(self, client):
        """WF06-Z: a schema that types money as a number invites float clients."""
        schemas = (await client.get("/openapi.json")).json()["components"]["schemas"]
        offenders = []
        for name, schema in schemas.items():
            for field, spec in (schema.get("properties") or {}).items():
                if not any(
                    token in field for token in ("usd", "units", "total", "price", "lots")
                ):
                    continue
                variants = [spec, *spec.get("anyOf", [])]
                if any(variant.get("type") == "number" for variant in variants):
                    offenders.append(f"{name}.{field}")
        assert offenders == [], offenders

    async def test_the_realized_block_never_offers_an_unqualified_net(self, client):
        """INV06-06, at the schema level as well as in the payload."""
        schemas = (await client.get("/openapi.json")).json()["components"]["schemas"]
        realized = schemas["RealizedComponentOut"]["properties"]
        assert "net_excluding_fee_usd" in realized
        assert "net_usd" not in realized

    async def test_the_interactive_docs_are_not_exposed(self, client):
        assert (await client.get("/docs")).status_code == 404
        assert (await client.get("/redoc")).status_code == 404
