"""The read-only HTTP contract. Section 8.1, what-if domains Z, AI, AK.

Two conventions are asserted repeatedly because downstream consumers will
depend on them:

* **Business statuses come back as HTTP 200 with a ``status`` field.**
  ``INCOMPLETE`` and ``BLOCKED`` are answers *about data*, and a dashboard
  that only checks the HTTP code must not read them as a success with no
  rows. Authentication and scope failures do use HTTP codes, because those
  are not answers about data at all.
* **Money is a JSON string.** A JSON number would be a float on the far side,
  which is the defect section 7.2 exists to prevent.
"""

from __future__ import annotations

from dataclasses import replace

import pytest
from fastapi.testclient import TestClient

from analytics.errors import Reason
from analytics.fixtures.golden import generate_deals, specs_for
from analytics.query.auth import Scope

from helpers import OPERATOR_TOKEN, READER_TOKEN, expected_daily, run_pipeline

pytestmark = pytest.mark.contract

BASE = "/internal/analytics/v1"


@pytest.fixture
def client(stack):
    return TestClient(stack.asgi_app())


@pytest.fixture
def published(stack, scope):
    specs = specs_for("G08-01")
    outcome, _ = run_pipeline(stack, specs, scope)
    return outcome, specs


def auth_headers(token: str = READER_TOKEN) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def daily_params(scope, **overrides) -> dict[str, str]:
    params = {
        "source_id": scope.source_id,
        "epoch_id": scope.epoch_id,
        "from": "2026-10-01",
        "to": "2026-10-31",
    }
    params.update(overrides)
    return params


class TestAuthentication:
    def test_missing_token_is_401(self, client, scope):
        response = client.get(f"{BASE}/daily", params=daily_params(scope))
        assert response.status_code == 401
        assert response.json()["detail"]["reason"] == str(Reason.UNAUTHENTICATED)

    def test_unknown_token_is_401(self, client, scope):
        response = client.get(
            f"{BASE}/daily", params=daily_params(scope), headers=auth_headers("nope-not-a-token")
        )
        assert response.status_code == 401

    def test_bare_token_without_bearer_prefix_is_accepted(self, client, scope, published):
        response = client.get(
            f"{BASE}/daily", params=daily_params(scope), headers={"Authorization": READER_TOKEN}
        )
        assert response.status_code == 200

    def test_reader_token_has_no_operator_scope(self, stack):
        reader = stack.authenticator.authenticate(READER_TOKEN)
        operator = stack.authenticator.authenticate(OPERATOR_TOKEN)
        assert reader.scope.operator is False
        assert operator.scope.operator is True
        with pytest.raises(Exception):
            reader.scope.require_operator()


class TestDailyRoute:
    def test_returns_rows_with_the_build_id(self, client, scope, published):
        outcome, specs = published
        response = client.get(f"{BASE}/daily", params=daily_params(scope), headers=auth_headers())
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "READY"
        assert body["build_id"] == outcome.build_id
        assert body["policy_version"] == scope.policy_version
        assert body["reporting_cut"] == scope.cut_id
        assert body["fee_coverage"] == "COMMISSION_AND_SWAP_ONLY"
        assert len(body["items"]) == len(expected_daily(specs))

    def test_money_is_serialised_as_strings(self, client, scope, published):
        """Section 7.2: a JSON number here becomes a float downstream."""
        response = client.get(f"{BASE}/daily", params=daily_params(scope), headers=auth_headers())
        for item in response.json()["items"]:
            for field in ("realized_pnl_gross", "commission", "swap", "volume_lots"):
                assert isinstance(item[field], str), f"{field} must be a string"
        # And the wire bytes quote the value rather than emitting a number.
        assert '"realized_pnl_gross":"' in response.text.replace(": ", ":")

    def test_net_carries_its_coverage_label(self, client, scope, published):
        body = client.get(
            f"{BASE}/daily", params=daily_params(scope), headers=auth_headers()
        ).json()
        for item in body["items"]:
            assert item["net_label"] == "NET_COMPONENTS_COVERED"
            assert item["net_label"] != "NET_FULL"
            assert isinstance(item["net_realized"], str)

    def test_coverage_names_the_requested_and_present_days(self, client, scope, published):
        """P08-T-11: a range is not 'full' just because some rows came back."""
        body = client.get(
            f"{BASE}/daily", params=daily_params(scope), headers=auth_headers()
        ).json()
        assert body["coverage"]["requested_from"] == "2026-10-01"
        assert body["coverage"]["requested_to"] == "2026-10-31"
        assert body["coverage"]["days_requested"] == 31
        assert body["coverage"]["days_present"] == 1

    def test_no_publication_is_a_200_with_incomplete(self, client, scope):
        """A business status, not a transport error."""
        response = client.get(f"{BASE}/daily", params=daily_params(scope), headers=auth_headers())
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "INCOMPLETE"
        assert body["reason"] == str(Reason.NO_PUBLICATION)
        assert body["items"] == []

    def test_inverted_range_is_refused(self, client, scope, published):
        response = client.get(
            f"{BASE}/daily",
            params=daily_params(scope, **{"from": "2026-10-31", "to": "2026-10-01"}),
            headers=auth_headers(),
        )
        assert response.json()["reason"] == str(Reason.RANGE_INVALID)

    def test_oversized_range_is_refused(self, client, scope, published):
        response = client.get(
            f"{BASE}/daily",
            params=daily_params(scope, **{"from": "2000-01-01", "to": "2030-01-01"}),
            headers=auth_headers(),
        )
        assert response.json()["reason"] == str(Reason.RANGE_TOO_LARGE)

    def test_unsupported_currency_filter_is_refused(self, client, scope, published):
        response = client.get(
            f"{BASE}/daily", params=daily_params(scope, currency="JPY"), headers=auth_headers()
        )
        assert response.json()["reason"] == str(Reason.CURRENCY_UNSUPPORTED)

    def test_malformed_date_is_a_422(self, client, scope, published):
        response = client.get(
            f"{BASE}/daily",
            params=daily_params(scope, **{"from": "not-a-date"}),
            headers=auth_headers(),
        )
        assert response.status_code == 422

    def test_account_filter_narrows_the_result(self, client, scope, published):
        response = client.get(
            f"{BASE}/daily", params=daily_params(scope, account_id="A-1"), headers=auth_headers()
        )
        items = response.json()["items"]
        assert {item["account_id"] for item in items} == {"A-1"}
        assert response.json()["coverage"]["accounts_filtered"] is True


class TestDealsRoute:
    @pytest.fixture
    def many(self, stack, scope):
        specs = [
            replace(spec, account_id="A-1")
            for spec in generate_deals(25, seed=77, accounts=1, days=1)
        ]
        outcome, _ = run_pipeline(stack, specs, scope)
        return outcome, specs

    def test_paginates_within_one_build(self, client, scope, many):
        outcome, specs = many
        seen: list[str] = []
        cursor = None
        for _ in range(10):
            params = {
                "source_id": scope.source_id,
                "epoch_id": scope.epoch_id,
                "limit": "10",
            }
            if cursor:
                params["cursor"] = cursor
            body = client.get(
                f"{BASE}/accounts/A-1/deals", params=params, headers=auth_headers()
            ).json()
            assert body["build_id"] == outcome.build_id
            seen.extend(item["deal_id"] for item in body["items"])
            cursor = body.get("next_cursor")
            if not cursor:
                break
        assert len(seen) == len(specs)
        assert len(set(seen)) == len(seen), "pagination must not repeat a Deal"

    def test_non_allowlisted_sort_is_refused(self, client, scope, many):
        """Section 8.2: an arbitrary sort expression is an injection surface."""
        response = client.get(
            f"{BASE}/accounts/A-1/deals",
            params={
                "source_id": scope.source_id,
                "epoch_id": scope.epoch_id,
                "sort": "executed_at; DROP TABLE x",
            },
            headers=auth_headers(),
        )
        assert response.json()["reason"] == str(Reason.SORT_KEY_NOT_ALLOWED)

    @pytest.mark.parametrize("sort", ["executed_at", "deal_id", "account_id"])
    def test_allowlisted_sorts_work(self, client, scope, many, sort):
        response = client.get(
            f"{BASE}/accounts/A-1/deals",
            params={"source_id": scope.source_id, "epoch_id": scope.epoch_id, "sort": sort},
            headers=auth_headers(),
        )
        assert response.status_code == 200
        assert response.json()["status"] == "READY"

    def test_oversized_limit_is_refused(self, client, scope, many):
        response = client.get(
            f"{BASE}/accounts/A-1/deals",
            params={
                "source_id": scope.source_id,
                "epoch_id": scope.epoch_id,
                "limit": "999999",
            },
            headers=auth_headers(),
        )
        assert response.json()["reason"] == str(Reason.RANGE_TOO_LARGE)

    def test_a_forged_cursor_is_refused(self, client, scope, many):
        response = client.get(
            f"{BASE}/accounts/A-1/deals",
            params={
                "source_id": scope.source_id,
                "epoch_id": scope.epoch_id,
                "cursor": "k1.Zm9yZ2Vk.YmFkc2ln",
            },
            headers=auth_headers(),
        )
        assert response.json()["reason"] == str(Reason.CURSOR_INVALID)

    def test_deal_rows_expose_the_content_hash(self, client, scope, many):
        """An auditor needs to tie a row back to its source fingerprint."""
        body = client.get(
            f"{BASE}/accounts/A-1/deals",
            params={"source_id": scope.source_id, "epoch_id": scope.epoch_id, "limit": "1"},
            headers=auth_headers(),
        ).json()
        item = body["items"][0]
        assert len(item["deal_payload_hash"]) == 64
        assert item["fee_coverage"] == "COMMISSION_AND_SWAP_ONLY"


class TestPublicationRoutes:
    def test_current_publication_reports_the_cut_and_evidence(self, client, scope, published):
        outcome, _ = published
        body = client.get(
            f"{BASE}/publications/current",
            params={"source_id": scope.source_id, "epoch_id": scope.epoch_id},
            headers=auth_headers(),
        ).json()
        assert body["build_id"] == outcome.build_id
        assert body["publication_revision"] == 1
        assert body["reporting_cut"] == scope.cut_id
        assert body["evidence_sha256"] == outcome.evidence_digest
        assert body["reconciliation_revision"]

    def test_evidence_route_returns_the_bundle_parts(self, client, scope, published):
        outcome, _ = published
        body = client.get(
            f"{BASE}/publications/{outcome.build_id}/evidence", headers=auth_headers()
        ).json()
        kinds = {record["evidence_kind"] for record in body["evidence"]}
        assert {"decimal_oracle", "source_manifest", "build_summary", "verified_cut"} <= kinds

    def test_another_tenants_build_is_not_readable(self, client, stack, published):
        """A ``build_id`` is not a capability token."""
        outcome, _ = published
        stack.authenticator.register(
            "rival-token-value", principal="r@rival", scope=Scope(tenant_id="rival-tenant")
        )
        response = client.get(
            f"{BASE}/publications/{outcome.build_id}/evidence",
            headers=auth_headers("rival-token-value"),
        )
        assert response.status_code == 409
        assert response.json()["detail"]["reason"] == str(Reason.BUILD_NOT_FOUND)

    def test_unknown_publication_is_reported_not_crashed(self, client, scope):
        body = client.get(
            f"{BASE}/publications/current",
            params={"source_id": scope.source_id, "epoch_id": scope.epoch_id},
            headers=auth_headers(),
        ).json()
        assert body["status"] == "INCOMPLETE"
        assert body["reason"] == str(Reason.NO_PUBLICATION)


class TestUnsupportedMetrics:
    def test_exposure_is_declared_unsupported(self, client, scope, published):
        """INV08-16 / ADR08-07: no holdings exist to compute from."""
        body = client.get(
            f"{BASE}/accounts/A-1/exposure",
            params={"source_id": scope.source_id, "epoch_id": scope.epoch_id},
            headers=auth_headers(),
        ).json()
        assert body["status"] == "UNSUPPORTED"
        assert body["reason"] == str(Reason.METRIC_UNSUPPORTED)
        assert body["items"] == []
        assert "P05/P06" in body["message"]


class TestHealth:
    def test_live_is_unconditional(self, client):
        assert client.get("/health/live").json() == {"status": "live"}

    def test_ready_checks_storage(self, client):
        """A process that answers ready while storage is down just attracts
        traffic it cannot serve."""
        response = client.get("/health/ready")
        assert response.status_code == 200
        body = response.json()
        assert body["ready"] is True
        assert "tables" in body


class TestScopeIsolation:
    def test_another_tenant_sees_no_rows(self, client, stack, scope, published):
        stack.authenticator.register(
            "rival-token-value", principal="r@rival", scope=Scope(tenant_id="rival-tenant")
        )
        body = client.get(
            f"{BASE}/daily", params=daily_params(scope), headers=auth_headers("rival-token-value")
        ).json()
        assert body["items"] == []
        assert body["status"] != "READY"

    def test_a_source_outside_scope_is_forbidden(self, client, stack, scope, published):
        stack.authenticator.register(
            "narrow-token-value",
            principal="narrow@lab",
            scope=Scope(tenant_id=scope.tenant_id, source_ids=frozenset({"sim-broker-99"})),
        )
        body = client.get(
            f"{BASE}/daily", params=daily_params(scope), headers=auth_headers("narrow-token-value")
        ).json()
        assert body["reason"] == str(Reason.FORBIDDEN_SCOPE)
        assert body["items"] == []

    def test_an_account_outside_scope_is_forbidden(self, client, stack, scope, published):
        """Refused, not silently filtered: an empty result would read as
        'this account had no trades'."""
        stack.authenticator.register(
            "acct-token-value",
            principal="acct@lab",
            scope=Scope(tenant_id=scope.tenant_id, account_ids=frozenset({"A-2"})),
        )
        body = client.get(
            f"{BASE}/daily",
            params=daily_params(scope, account_id="A-1"),
            headers=auth_headers("acct-token-value"),
        ).json()
        assert body["reason"] == str(Reason.FORBIDDEN_SCOPE)

    def test_a_report_family_outside_scope_is_forbidden(self, client, stack, scope, published):
        stack.authenticator.register(
            "family-token-value",
            principal="family@lab",
            scope=Scope(
                tenant_id=scope.tenant_id, report_families=frozenset({"other_family_v1"})
            ),
        )
        body = client.get(
            f"{BASE}/daily", params=daily_params(scope), headers=auth_headers("family-token-value")
        ).json()
        assert body["reason"] == str(Reason.FORBIDDEN_SCOPE)
