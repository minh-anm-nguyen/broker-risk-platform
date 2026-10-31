"""Routing, negotiation and authentication over real HTTP.

WF06-A and WF06-B at the transport level. The recurring assertion is that a
refusal costs nothing: no engine call, no projection read, no account
identifier in the body.
"""

from __future__ import annotations

import pytest
from conftest import ACCOUNT_A, ACCOUNT_B, SOURCE_ID

pytestmark = pytest.mark.contract

RISK = f"/api/v1/accounts/{ACCOUNT_A}/risk"


class TestRouting:
    async def test_an_unknown_query_key_is_refused(self, client, context, auth):
        """WF06-A-02: a silently ignored parameter is a security problem."""
        response = await client.get(
            RISK, params={"source_id": SOURCE_ID, "bypass_cut": "true"}, headers=auth
        )
        assert response.status_code == 422
        body = response.json()
        assert body["code"] == "INVALID_REQUEST"
        assert "bypass_cut" in body["detail"]
        assert context.engine.stats.calls == 0

    async def test_an_unknown_component_is_refused(self, client, context, auth):
        response = await client.get(
            RISK, params={"source_id": SOURCE_ID, "components": "EXPOSURE,BOGUS"}, headers=auth
        )
        assert response.status_code == 422
        assert context.engine.stats.calls == 0

    async def test_a_missing_source_id_is_refused(self, client, auth):
        response = await client.get(RISK, headers=auth)
        assert response.status_code == 422
        assert response.json()["code"] == "INVALID_REQUEST"

    async def test_an_unsupported_method_returns_405_with_allow(self, client, auth):
        """WF06-A-07: and nothing is mutated."""
        response = await client.put(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 405
        assert "Allow" in response.headers
        assert response.json()["code"] == "METHOD_NOT_ALLOWED"

    async def test_delete_is_also_refused(self, client, auth):
        response = await client.delete(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 405

    async def test_an_unknown_path_is_a_problem_document(self, client, auth):
        response = await client.get("/api/v1/nope", headers=auth)
        assert response.status_code == 404
        assert response.headers["content-type"].startswith("application/problem+json")
        assert response.json()["code"] == "RESOURCE_NOT_FOUND"

    async def test_an_unversioned_path_has_no_implicit_fallback(self, client, auth):
        """WF06-A-12: there is no unversioned alias to drift onto."""
        response = await client.get(
            f"/accounts/{ACCOUNT_A}/risk", params={"source_id": SOURCE_ID}, headers=auth
        )
        assert response.status_code == 404

    async def test_a_body_on_a_get_cannot_widen_scope(self, client, context, auth):
        """WF06-A-06: the declared contract ignores it; scope stays the path account."""
        response = await client.request(
            "GET",
            RISK,
            params={"source_id": SOURCE_ID},
            headers={**auth, "content-type": "application/json"},
            content=b'{"account_ids":["00043"]}',
        )
        assert response.status_code == 200
        assert response.json()["scope"]["account_ids"] == [ACCOUNT_A]


class TestNegotiation:
    async def test_an_xml_only_client_gets_406(self, client, auth):
        """WF06-A-09: never serve financial data as some other type."""
        response = await client.get(
            RISK, params={"source_id": SOURCE_ID}, headers={**auth, "accept": "application/xml"}
        )
        assert response.status_code == 406
        assert response.json()["code"] == "NOT_ACCEPTABLE"

    @pytest.mark.parametrize("accept", ["*/*", "application/json", "application/*", None])
    async def test_json_capable_clients_are_served(self, client, auth, accept):
        headers = dict(auth)
        if accept is not None:
            headers["accept"] = accept
        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=headers)
        assert response.status_code == 200

    async def test_a_post_with_a_text_content_type_is_415(self, client, context, auth):
        """WF06-A-04: no deserialization and no engine work."""
        response = await client.post(
            "/api/v1/risk-calculations",
            headers={**auth, "content-type": "text/plain"},
            content=b'{"source_id":"sim-broker-a"}',
        )
        assert response.status_code == 415
        assert response.json()["code"] == "UNSUPPORTED_MEDIA_TYPE"
        assert context.engine.stats.calls == 0

    async def test_a_compressed_post_body_is_refused_in_this_version(self, client, auth):
        response = await client.post(
            "/api/v1/risk-calculations",
            headers={**auth, "content-type": "application/json", "content-encoding": "gzip"},
            content=b'{"source_id":"sim-broker-a"}',
        )
        assert response.status_code == 415

    async def test_an_overlong_query_string_is_refused(self, client, auth):
        response = await client.get(
            RISK, params={"source_id": SOURCE_ID, "components": "E" * 2000}, headers=auth
        )
        assert response.status_code in (414, 422)
        assert response.json()["code"] in ("URI_TOO_LONG", "INVALID_REQUEST")


class TestAuthentication:
    async def test_no_credential_is_401_with_a_challenge(self, client, context):
        """WF06-B-01: zero projection reads and zero engine calls."""
        response = await client.get(RISK, params={"source_id": SOURCE_ID})
        assert response.status_code == 401
        assert response.headers["www-authenticate"].startswith("Bearer")
        assert response.json()["code"] == "UNAUTHENTICATED"
        assert context.engine.stats.calls == 0
        assert context.views.counters.get("snapshots", 0) == 0

    @pytest.mark.parametrize(
        "header",
        [
            "",
            "Bearer",
            "Bearer ",
            "Basic dXNlcjpwYXNz",
            "token abc",
            "Bearer not.a.jwt",
            "Bearer a.b",
        ],
    )
    async def test_malformed_credentials_are_401(self, client, context, header: str):
        response = await client.get(
            RISK, params={"source_id": SOURCE_ID}, headers={"Authorization": header}
        )
        assert response.status_code == 401
        assert context.engine.stats.calls == 0

    async def test_every_authentication_failure_looks_identical(self, client, tokens, issuer):
        """A prober must not learn which check failed."""
        bad_signature = issuer.mint_with_tampered_payload(
            tokens.spec("analyst-a"), now=1_767_225_600, overrides={"sub": "analyst-b"}
        )
        wrong_audience = issuer.mint(
            tokens.spec("analyst-a", audience="p04-admin"), now=1_767_225_600
        )
        expired = issuer.mint(tokens.spec("analyst-a", expires_in=1), now=1_767_225_000)

        bodies = []
        for token in (bad_signature, wrong_audience, expired):
            response = await client.get(
                RISK, params={"source_id": SOURCE_ID}, headers={"Authorization": f"Bearer {token}"}
            )
            assert response.status_code == 401
            body = response.json()
            body.pop("request_id")
            body.pop("instance")
            bodies.append(body)
        assert bodies[0] == bodies[1] == bodies[2]

    async def test_an_unsigned_token_is_refused(self, client, tokens, issuer):
        token = issuer.mint_unsigned(tokens.spec("analyst-a"), now=1_767_225_600)
        response = await client.get(
            RISK, params={"source_id": SOURCE_ID}, headers={"Authorization": f"Bearer {token}"}
        )
        assert response.status_code == 401

    async def test_a_token_without_a_tenant_claim_is_denied(self, client, issuer, clock):
        """A credential with no tenant binding cannot be scoped, so it is denied."""
        from risk_api.authn.test_issuer import TokenSpec

        token = issuer.mint(
            TokenSpec(subject="analyst-a", extra_claims={"roles": ["risk_analyst"]}),
            now=clock.wall(),
        )
        header = {"Authorization": f"Bearer {token}"}
        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=header)
        assert response.status_code == 403
        assert response.json()["code"] == "ACCESS_DENIED"

    async def test_a_principal_without_a_policy_record_is_denied(self, client, tokens):
        header = tokens.header("stranger", tenant_id="tenant-a")
        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=header)
        assert response.status_code == 403
        assert response.json()["code"] == "ACCESS_DENIED"


class TestTenantHeaderIsNotTrusted:
    async def test_a_tenant_header_cannot_widen_access(self, client, auth):
        """WF06-C-03: the claim decides, the header is noise."""
        response = await client.get(
            RISK,
            params={"source_id": SOURCE_ID},
            headers={**auth, "X-Tenant-ID": "tenant-b"},
        )
        assert response.status_code == 200
        assert response.json()["scope"]["account_ids"] == [ACCOUNT_A]

    async def test_a_tenant_header_cannot_reach_another_tenants_account(self, client, auth):
        response = await client.get(
            f"/api/v1/accounts/{ACCOUNT_B}/risk",
            params={"source_id": SOURCE_ID},
            headers={**auth, "X-Tenant-ID": "tenant-b"},
        )
        assert response.status_code == 404


class TestRequestIdIsServerGenerated:
    async def test_a_client_supplied_request_id_is_ignored(self, client, auth):
        """WF06-AC: forged correlation would poison the audit trail."""
        response = await client.get(
            RISK,
            params={"source_id": SOURCE_ID},
            headers={**auth, "X-Request-ID": "forged-by-client"},
        )
        assert response.status_code == 200
        assert response.headers["x-request-id"] != "forged-by-client"
        assert response.json()["request_id"] == response.headers["x-request-id"]

    async def test_each_request_gets_a_distinct_id(self, client, auth):
        ids = set()
        for _ in range(4):
            response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
            ids.add(response.headers["x-request-id"])
        assert len(ids) == 4

    async def test_error_responses_also_carry_the_id(self, client):
        response = await client.get(RISK, params={"source_id": SOURCE_ID})
        assert response.headers["x-request-id"].startswith("req-")
        assert response.json()["request_id"] == response.headers["x-request-id"]


class TestHeaderPolicyEverywhere:
    @pytest.mark.parametrize(
        ("path", "params"),
        [
            (RISK, {"source_id": SOURCE_ID}),
            ("/api/v1/positions", {"source_id": SOURCE_ID}),
            ("/api/v1/exposure", {"source_id": SOURCE_ID}),
            ("/api/v1/reports/daily", {"source_id": SOURCE_ID, "date": "2026-10-15", "account_id": ACCOUNT_A}),
        ],
    )
    async def test_successes_are_never_cacheable(self, client, auth, path, params):
        """INV06-17: a proxy caching a financial report is a cross-principal leak."""
        response = await client.get(path, params=params, headers=auth)
        assert response.status_code == 200, response.text
        assert response.headers["cache-control"] == "private, no-store"
        assert response.headers["vary"] == "Authorization"

    async def test_errors_are_never_cacheable(self, client):
        response = await client.get(RISK, params={"source_id": SOURCE_ID})
        assert response.headers["cache-control"] == "private, no-store"

    async def test_health_responses_also_carry_the_policy(self, client):
        response = await client.get("/health/live")
        assert response.status_code == 200
        assert response.headers["cache-control"] == "private, no-store"
