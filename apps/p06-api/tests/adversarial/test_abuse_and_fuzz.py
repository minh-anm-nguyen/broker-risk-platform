"""Adversarial input, abuse and leak probing.

WF06-Y, WF06-D, WF06-R. The pass condition throughout: a documented refusal,
no stack trace, no internal identifier, no unbounded work, and never a 500 for
something the service should have classified.
"""

from __future__ import annotations

import asyncio

import pytest
from conftest import ACCOUNT_A, ACCOUNT_B, SOURCE_ID, add_position

pytestmark = pytest.mark.adversarial

RISK = f"/api/v1/accounts/{ACCOUNT_A}/risk"
CALC = "/api/v1/risk-calculations"
POSITIONS = "/api/v1/positions"

LEAK_MARKERS = (
    "Traceback",
    "File \"",
    "SELECT ",
    "psycopg",
    "asyncpg",
    "sqlalchemy",
    "dev-only",
    "/usr/",
    "C:\\\\",
    "site-packages",
    "risk_api.app",
    "localhost",
)


def assert_no_leak(response) -> None:
    text = response.text
    for marker in LEAK_MARKERS:
        assert marker not in text, f"leaked {marker!r}"


class TestAdversarialPathAndQuery:
    @pytest.mark.parametrize(
        "account",
        [
            "../../etc/passwd",
            "%2e%2e%2f",
            "00042%00",
            "0" * 200,
            "00042 OR 1=1",
            "00042'; DROP TABLE positions;--",
            "\u202eUser",
            "ＡＣＣ",
            "00042%0A",
            "00042%20",
            "{{7*7}}",
            "<script>alert(1)</script>",
        ],
    )
    async def test_hostile_account_identifiers_are_refused_cleanly(self, client, auth, account):
        response = await client.get(
            f"/api/v1/accounts/{account}/risk", params={"source_id": SOURCE_ID}, headers=auth
        )
        assert response.status_code in (404, 422), response.status_code
        assert_no_leak(response)

    @pytest.mark.parametrize(
        "source",
        ["../other", "SIM-BROKER-A", "sim broker", "sim;broker", "s" * 200, "", "%00"],
    )
    async def test_hostile_source_identifiers_are_refused(self, client, auth, source):
        response = await client.get(RISK, params={"source_id": source}, headers=auth)
        assert response.status_code in (404, 422)
        assert_no_leak(response)

    @pytest.mark.parametrize(
        "symbol", ["EURUSD; DROP", "../EURUSD", "eurusd", "E" * 100, "EUR USD", "*"]
    )
    async def test_hostile_symbol_filters_are_refused(self, client, auth, symbol):
        response = await client.get(
            POSITIONS, params={"source_id": SOURCE_ID, "symbol": symbol}, headers=auth
        )
        assert response.status_code == 422
        assert_no_leak(response)

    async def test_repeated_query_keys_do_not_smuggle_a_second_value(self, client, auth):
        """Two source_id values must not let one of them win silently."""
        response = await client.get(
            f"{RISK}?source_id={SOURCE_ID}&source_id=other-broker", headers=auth
        )
        # Either refused outright or resolved to one authorized source, never
        # a report attributed to the wrong source.
        assert response.status_code in (200, 404, 422)
        if response.status_code == 200:
            assert response.json()["scope"]["source_id"] == SOURCE_ID

    async def test_an_encoded_slash_cannot_widen_the_authorized_scope(self, client, auth):
        """However the client or server normalises %2F, the scope is the path account."""
        response = await client.get(
            "/api/v1/accounts/00042%2Frisk", params={"source_id": SOURCE_ID}, headers=auth
        )
        assert response.status_code in (200, 404, 422)
        assert_no_leak(response)
        if response.status_code == 200:
            assert response.json()["scope"]["account_ids"] == [ACCOUNT_A]

    @pytest.mark.parametrize(
        "components",
        ["EXPOSURE" * 200, "EXPOSURE,EXPOSURE,EXPOSURE,BOGUS", "\u0000", "EXPOSURE\n,REALIZED"],
    )
    async def test_hostile_component_lists_are_refused(self, client, auth, components):
        response = await client.get(
            RISK, params={"source_id": SOURCE_ID, "components": components}, headers=auth
        )
        assert response.status_code == 422
        assert_no_leak(response)


class TestAdversarialBodies:
    @pytest.mark.parametrize(
        "raw",
        [
            b"",
            b"   ",
            b"null",
            b"[]",
            b"42",
            b'"string"',
            b"{",
            b'{"source_id":}',
            b'{"source_id":"sim-broker-a",}',
            b"\xff\xfe\x00",
            b'{"a":' + b"[" * 200 + b"]" * 200 + b"}",
            b'{"__proto__":{"admin":true}}',
            b'{"source_id":"sim-broker-a","account_ids":"00042","components":["EXPOSURE"]}',
        ],
    )
    async def test_hostile_bodies_are_refused_cleanly(self, client, context, auth, raw):
        response = await client.post(
            CALC, content=raw, headers={**auth, "content-type": "application/json"}
        )
        assert response.status_code in (413, 422), response.status_code
        assert response.headers["content-type"].startswith("application/problem+json")
        assert_no_leak(response)
        assert context.engine.stats.calls == 0

    async def test_a_unicode_normalisation_trick_does_not_match_an_account(self, client, auth):
        """A visually similar identifier is not the same identifier."""
        response = await client.post(
            CALC,
            json={
                "source_id": SOURCE_ID,
                "account_ids": ["\uff10\uff10\uff10\uff14\uff12"],
                "components": ["EXPOSURE"],
            },
            headers=auth,
        )
        assert response.status_code in (404, 422)

    async def test_a_very_wide_account_list_is_refused_before_compute(self, client, context, auth):
        response = await client.post(
            CALC,
            json={
                "source_id": SOURCE_ID,
                "account_ids": [f"{index:05d}" for index in range(500)],
                "components": ["EXPOSURE"],
            },
            headers=auth,
        )
        assert response.status_code == 422
        assert response.json()["code"] == "ACCOUNT_SCOPE_TOO_LARGE"
        assert context.engine.stats.calls == 0

    async def test_a_hostile_version_string_cannot_select_code(self, client, context, auth):
        for version in ("../../evil", "risk-policy-v1; import os", "x" * 500):
            response = await client.post(
                CALC,
                json={
                    "source_id": SOURCE_ID,
                    "account_ids": [ACCOUNT_A],
                    "components": ["EXPOSURE"],
                    "risk_policy_version": version,
                },
                headers=auth,
            )
            assert response.status_code == 422
            assert context.engine.stats.calls == 0


class TestHeaderAbuse:
    @pytest.mark.parametrize(
        ("name", "value"),
        [
            ("X-Forwarded-For", "127.0.0.1, 10.0.0.1"),
            ("X-Forwarded-Host", "internal.admin.test"),
            ("X-Original-URL", "/internal/metrics"),
            ("X-Rewrite-URL", "/internal/metrics"),
            ("X-Debug", "true"),
            ("X-Allow-Partial", "true"),
            ("X-Bypass-Cut", "true"),
            ("X-Tenant-ID", "tenant-b"),
            ("X-Account-ID", ACCOUNT_B),
        ],
    )
    async def test_speculative_headers_change_nothing(self, client, auth, name, value):
        response = await client.get(
            RISK, params={"source_id": SOURCE_ID}, headers={**auth, name: value}
        )
        assert response.status_code == 200
        body = response.json()
        assert body["scope"]["account_ids"] == [ACCOUNT_A]
        assert body["partial_allowed"] is False
        assert ACCOUNT_B not in response.text

    async def test_a_second_authorization_header_cannot_elevate(self, client, tokens):
        """Only one credential is considered, and it is not the attacker's choice."""
        viewer = tokens.mint("viewer-a", roles=("risk_viewer",))
        analyst = tokens.mint("analyst-b")
        response = await client.get(
            f"/api/v1/accounts/{ACCOUNT_B}/risk",
            params={"source_id": SOURCE_ID},
            headers=[("Authorization", f"Bearer {viewer}"), ("Authorization", f"Bearer {analyst}")],
        )
        assert response.status_code in (401, 403, 404)
        assert ACCOUNT_B not in response.text or response.status_code != 200


class TestCursorAbuseOverHttp:
    @pytest.mark.parametrize(
        "cursor",
        [
            "",
            "x",
            "YWJj.ZGVm",
            "a.b.c",
            "../../etc/passwd",
            "A" * 2000,
            "eyJ2IjoxfQ.c2ln",
            "%00",
        ],
    )
    async def test_a_forged_cursor_is_refused_before_any_query(
        self, client, context, auth, cursor
    ):
        response = await client.get(
            POSITIONS, params={"source_id": SOURCE_ID, "cursor": cursor}, headers=auth
        )
        assert response.status_code in (409, 422), response.status_code
        # An oversized cursor may be refused by the query budget first; both
        # refusals happen before any projection read.
        assert response.json()["code"] in (
            "INVALID_CURSOR",
            "CURSOR_EXPIRED",
            "INVALID_REQUEST",
        )
        assert_no_leak(response)
        assert context.views.counters.get("positions", 0) == 0

    async def test_a_valid_cursor_cannot_be_widened_by_changing_filters(
        self, client, world, auth
    ):
        for index in range(6):
            add_position(world, f"P-8{index:03d}", ACCOUNT_A, lots="0.1")
        first = await client.get(
            POSITIONS, params={"source_id": SOURCE_ID, "limit": "2"}, headers=auth
        )
        cursor = first.json()["next_cursor"]
        assert cursor

        response = await client.get(
            POSITIONS,
            params={"source_id": SOURCE_ID, "limit": "50", "cursor": cursor},
            headers=auth,
        )
        assert response.status_code == 422
        assert response.json()["code"] == "INVALID_CURSOR"


class TestAbusePressure:
    async def test_a_malformed_flood_does_not_starve_legitimate_traffic(
        self, client, context, auth
    ):
        """MC-06: parsing isolation and fairness under a bad-payload flood."""
        flood = [
            client.post(CALC, content=b"{", headers={**auth, "content-type": "application/json"})
            for _ in range(40)
        ]
        legitimate = [
            client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth) for _ in range(4)
        ]
        results = await asyncio.gather(*flood, *legitimate, return_exceptions=True)

        bad = results[:40]
        good = results[40:]
        assert all(getattr(r, "status_code", None) == 422 for r in bad)
        # Legitimate traffic is either served or refused by a documented
        # capacity code; it is never a 500.
        for response in good:
            assert getattr(response, "status_code", None) in (200, 429, 503)
        assert context.admission.stats.peak_queued_heavy <= context.admission.max_heavy_queued

    async def test_probing_does_not_raise_the_unauthorized_access_signal(
        self, client, context, auth, tokens
    ):
        for account in (ACCOUNT_B, "99999", "00000", "abcdef"):
            await client.get(
                f"/api/v1/accounts/{account}/risk", params={"source_id": SOURCE_ID}, headers=auth
            )
        assert (
            context.metrics.counter_value("api_unauthorized_data_access_total", surface="response")
            == 0.0
        )

    async def test_quota_exhaustion_is_a_429_with_retry_after(self, client, context, auth):
        """WF06-R-01: the principal's own quota, not service capacity."""
        statuses = []
        for _ in range(20):
            response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
            statuses.append(response.status_code)
            if response.status_code == 429:
                assert response.headers["retry-after"].isdigit()
                assert response.json()["code"] == "RATE_LIMITED"
                break
        assert 429 in statuses, "the quota never engaged"
        assert context.admission.stats.rejected_quota >= 1

    async def test_a_refused_request_leaves_no_capacity_held(self, client, context, auth):
        for _ in range(20):
            await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert context.admission.stats.executing_heavy == 0
        assert context.admission.stats.queued_heavy == 0
        assert context.views.open_sessions == 0
