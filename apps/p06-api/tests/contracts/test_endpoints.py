"""The remaining v1 endpoints: P&L, positions, exposure, daily, calculations.

WF06-L, M, N, O and the POST body limits. Each endpoint is checked for its own
semantics and for the limits at ``L``, ``L+1`` and nonsense.
"""

from __future__ import annotations

import json

import pytest
from conftest import ACCOUNT_A, ACCOUNT_B, SOURCE_ID, add_instrument, add_position

pytestmark = pytest.mark.contract

PNL = f"/api/v1/accounts/{ACCOUNT_A}/pnl"
POSITIONS = "/api/v1/positions"
EXPOSURE = "/api/v1/exposure"
DAILY = "/api/v1/reports/daily"
CALC = "/api/v1/risk-calculations"

WINDOW = {"period_start": "2026-10-01T00:00:00Z", "period_end": "2026-10-30T00:00:00Z"}


class TestPnl:
    async def test_components_are_explicitly_labelled(self, client, auth, no_float_money):
        """FR06-02: gross, commission, swap and net *excluding* unavailable fees."""
        response = await client.get(PNL, params={"source_id": SOURCE_ID, **WINDOW}, headers=auth)
        assert response.status_code == 200, response.text
        no_float_money(response.text)
        body = response.json()
        realized = body["realized"]
        assert realized["gross_usd"] == "60.00"
        assert realized["commission_usd"] == "-3.50"
        assert realized["swap_usd"] == "-0.50"
        assert realized["net_excluding_fee_usd"] == "56.00"
        assert realized["fee_coverage"] == "NOT_AVAILABLE"
        assert "net_usd" not in realized
        assert body["period"] == {
            "start_utc": "2026-10-01T00:00:00Z",
            "end_utc": "2026-10-30T00:00:00Z",
        }
        assert "components" not in body

    async def test_the_window_is_required(self, client, auth):
        response = await client.get(PNL, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 422
        assert response.json()["code"] == "INVALID_REQUEST"

    async def test_an_inverted_window_is_refused(self, client, auth):
        response = await client.get(
            PNL,
            params={
                "source_id": SOURCE_ID,
                "period_start": "2026-10-30T00:00:00Z",
                "period_end": "2026-10-01T00:00:00Z",
            },
            headers=auth,
        )
        assert response.status_code == 422
        assert response.json()["code"] == "INVALID_TIME_WINDOW"

    async def test_an_empty_window_is_refused(self, client, auth):
        response = await client.get(
            PNL,
            params={
                "source_id": SOURCE_ID,
                "period_start": "2026-10-01T00:00:00Z",
                "period_end": "2026-10-01T00:00:00Z",
            },
            headers=auth,
        )
        assert response.status_code == 422

    async def test_a_window_at_the_limit_is_accepted(self, client, auth):
        response = await client.get(
            PNL,
            params={
                "source_id": SOURCE_ID,
                "period_start": "2026-10-01T00:00:00Z",
                "period_end": "2026-11-01T00:00:00Z",
            },
            headers=auth,
        )
        assert response.status_code == 200

    async def test_a_window_beyond_the_limit_is_refused(self, client, context, auth):
        """L+1: 32 days exceeds the interactive historical budget."""
        response = await client.get(
            PNL,
            params={
                "source_id": SOURCE_ID,
                "period_start": "2026-10-01T00:00:00Z",
                "period_end": "2026-11-02T00:00:00Z",
            },
            headers=auth,
        )
        assert response.status_code == 422
        assert response.json()["code"] == "HISTORICAL_WINDOW_TOO_LARGE"
        assert context.engine.stats.calls == 0

    async def test_another_tenants_account_is_masked(self, client, auth):
        response = await client.get(
            f"/api/v1/accounts/{ACCOUNT_B}/pnl",
            params={"source_id": SOURCE_ID, **WINDOW},
            headers=auth,
        )
        assert response.status_code == 404


class TestPositions:
    async def test_a_page_carries_its_scan_and_decimal_strings(
        self, client, auth, no_float_money
    ):
        response = await client.get(POSITIONS, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 200, response.text
        no_float_money(response.text)
        body = response.json()
        assert body["cuts"]["position_scan_id"] == "scan-P"
        assert body["items"][0]["account_id"] == ACCOUNT_A
        assert body["items"][0]["volume_lots"] == "0.5"
        assert body["items"][0]["open_price"] == "1.1"
        assert body["has_more"] is False
        assert body["next_cursor"] is None

    async def test_only_authorized_accounts_appear(self, client, auth):
        response = await client.get(POSITIONS, params={"source_id": SOURCE_ID}, headers=auth)
        accounts = {item["account_id"] for item in response.json()["items"]}
        assert accounts == {ACCOUNT_A}
        assert ACCOUNT_B not in response.text

    async def test_an_explicit_out_of_scope_account_is_masked(self, client, auth):
        response = await client.get(
            POSITIONS, params={"source_id": SOURCE_ID, "account_id": ACCOUNT_B}, headers=auth
        )
        assert response.status_code == 404

    async def test_a_symbol_filter_narrows_the_page(self, client, auth):
        response = await client.get(
            POSITIONS, params={"source_id": SOURCE_ID, "symbol": "EURUSD"}, headers=auth
        )
        assert response.status_code == 200
        assert all(item["symbol"] == "EURUSD" for item in response.json()["items"])

    async def test_a_symbol_with_no_positions_returns_an_empty_page(self, client, auth):
        response = await client.get(
            POSITIONS, params={"source_id": SOURCE_ID, "symbol": "GBPUSD"}, headers=auth
        )
        assert response.status_code == 200
        assert response.json()["items"] == []

    @pytest.mark.parametrize("limit", ["0", "-1", "abc", "1.5", "", "1e2"])
    async def test_a_nonsense_limit_is_refused(self, client, auth, limit: str):
        response = await client.get(
            POSITIONS, params={"source_id": SOURCE_ID, "limit": limit}, headers=auth
        )
        assert response.status_code == 422
        assert response.json()["code"] in ("INVALID_REQUEST", "LIMIT_EXCEEDED")

    async def test_the_limit_boundary(self, client, auth):
        """L and L+1 around the documented page ceiling of 100."""
        at_limit = await client.get(
            POSITIONS, params={"source_id": SOURCE_ID, "limit": "100"}, headers=auth
        )
        assert at_limit.status_code == 200
        assert at_limit.json()["page_size"] == 100

        beyond = await client.get(
            POSITIONS, params={"source_id": SOURCE_ID, "limit": "101"}, headers=auth
        )
        assert beyond.status_code == 422
        assert beyond.json()["code"] == "LIMIT_EXCEEDED"

    async def test_pages_partition_the_scan_exactly_once(self, client, context, world, auth):
        """Property: the union of pages equals the snapshot, with no duplicates."""
        for index in range(12):
            add_position(world, f"P-1{index:03d}", ACCOUNT_A, lots="0.1")

        seen: list[str] = []
        cursor = None
        pages = 0
        while True:
            params = {"source_id": SOURCE_ID, "limit": "5"}
            if cursor:
                params["cursor"] = cursor
            response = await client.get(POSITIONS, params=params, headers=auth)
            assert response.status_code == 200, response.text
            body = response.json()
            seen.extend(item["position_id"] for item in body["items"])
            cursor = body["next_cursor"]
            pages += 1
            if not cursor:
                break
            assert pages < 20, "pagination did not terminate"

        assert len(seen) == len(set(seen)), "a position appeared on two pages"
        expected = {
            position_id
            for position_id, row in world.positions.items()
            if row.account_id == ACCOUNT_A
        }
        assert set(seen) == expected

    async def test_page_size_does_not_change_the_union(self, client, world, auth):
        for index in range(9):
            add_position(world, f"P-2{index:03d}", ACCOUNT_A, lots="0.1")

        async def collect(limit: str) -> list[str]:
            found: list[str] = []
            cursor = None
            while True:
                params = {"source_id": SOURCE_ID, "limit": limit}
                if cursor:
                    params["cursor"] = cursor
                body = (await client.get(POSITIONS, params=params, headers=auth)).json()
                found.extend(item["position_id"] for item in body["items"])
                cursor = body["next_cursor"]
                if not cursor:
                    return found

        assert sorted(await collect("3")) == sorted(await collect("7"))


class TestExposure:
    async def test_units_are_grouped_by_symbol_with_their_currency(self, client, auth):
        """Exposure is per-symbol units, not one universal notional number."""
        response = await client.get(EXPOSURE, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["group_count"] == 1
        bucket = body["exposure"]["by_symbol"]["EURUSD"]
        assert bucket["unit_currency"] == "EUR"
        assert bucket["net_units"] == "50000"
        assert "components" not in body

    async def test_an_explicit_account_selection_is_honoured(self, client, auth):
        response = await client.get(
            EXPOSURE, params={"source_id": SOURCE_ID, "account_ids": ACCOUNT_A}, headers=auth
        )
        assert response.status_code == 200
        assert response.json()["scope"]["account_ids"] == [ACCOUNT_A]

    async def test_a_mixed_selection_denies_the_whole_request(self, client, context, auth):
        """WF06-C-02: never a partial aggregate that hints at the other account."""
        response = await client.get(
            EXPOSURE,
            params={"source_id": SOURCE_ID, "account_ids": f"{ACCOUNT_A},{ACCOUNT_B}"},
            headers=auth,
        )
        assert response.status_code == 403
        assert response.json()["code"] == "ACCESS_DENIED"
        assert ACCOUNT_B not in response.text
        assert context.engine.stats.calls == 0

    async def test_an_empty_account_list_is_not_a_wildcard(self, client, auth):
        """WF06-C-08."""
        response = await client.get(
            EXPOSURE, params={"source_id": SOURCE_ID, "account_ids": ""}, headers=auth
        )
        assert response.status_code == 422

    async def test_a_viewer_cannot_request_a_broker_wide_aggregate(self, client, context, tokens):
        """WF06-C-07: refused before any grouping work."""
        header = tokens.header("viewer-a", roles=("risk_viewer",))
        response = await client.get(
            EXPOSURE,
            params={"source_id": SOURCE_ID, "account_ids": f"{ACCOUNT_A},{ACCOUNT_B}"},
            headers=header,
        )
        assert response.status_code == 403
        assert context.engine.stats.calls == 0

    async def test_too_many_accounts_is_refused_before_compute(self, client, context, auth):
        """FX-TOO-LARGE-010: 101 accounts never reaches the engine."""
        many = ",".join(f"{index:05d}" for index in range(101))
        response = await client.get(
            EXPOSURE, params={"source_id": SOURCE_ID, "account_ids": many}, headers=auth
        )
        assert response.status_code == 422
        assert response.json()["code"] == "ACCOUNT_SCOPE_TOO_LARGE"
        assert context.engine.stats.calls == 0

    async def test_multiple_symbols_each_keep_their_own_units(self, client, world, auth):
        add_instrument(world, "GBPUSD", base="GBP")
        add_position(world, "P-GBP", ACCOUNT_A, symbol="GBPUSD", lots="0.3")
        response = await client.get(EXPOSURE, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 200, response.text
        buckets = response.json()["exposure"]["by_symbol"]
        assert buckets["EURUSD"]["unit_currency"] == "EUR"
        assert buckets["GBPUSD"]["unit_currency"] == "GBP"
        assert buckets["GBPUSD"]["net_units"] == "30000"


class TestDailyReport:
    async def test_the_day_is_a_half_open_utc_interval(self, client, auth):
        response = await client.get(
            DAILY,
            params={"source_id": SOURCE_ID, "date": "2026-10-15", "account_id": ACCOUNT_A},
            headers=auth,
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["report_date"] == "2026-10-15"
        assert body["period"] == {
            "start_utc": "2026-10-15T00:00:00Z",
            "end_utc": "2026-10-16T00:00:00Z",
        }
        assert body["realized"]["deals_counted"] == 1
        assert body["realized"]["net_excluding_fee_usd"] == "56.00"

    async def test_a_proven_empty_day_is_an_explicit_zero(self, client, auth):
        """IT06-10 / FX-EMPTY-007: zero is not the same as unknown."""
        response = await client.get(
            DAILY,
            params={"source_id": SOURCE_ID, "date": "2026-10-14", "account_id": ACCOUNT_A},
            headers=auth,
        )
        assert response.status_code == 200
        body = response.json()
        assert body["realized"]["status"] == "READY"
        assert body["realized"]["net_excluding_fee_usd"] == "0.00"
        assert body["realized"]["deals_counted"] == 0
        assert body["coverage"]["deals_complete"] is True

    async def test_an_incomplete_day_blocks_rather_than_reporting_zero(
        self, client, context, world, auth
    ):
        """IT06-10: a missing deal interval is a blocker, never a zero."""
        world.deals_complete = False
        response = await client.get(
            DAILY,
            params={"source_id": SOURCE_ID, "date": "2026-10-15", "account_id": ACCOUNT_A},
            headers=auth,
        )
        assert response.status_code == 409
        body = response.json()
        assert body["code"] == "DATA_NOT_READY"
        assert "DEAL_WATERMARK_UNPROVEN" in body["reason_codes"]
        assert "0.00" not in response.text

    @pytest.mark.parametrize(
        "date", ["2026-10-15T00:00:00Z", "15-10-2026", "2026-02-30", "2026-10", ""]
    )
    async def test_a_malformed_date_is_refused(self, client, auth, date: str):
        response = await client.get(
            DAILY,
            params={"source_id": SOURCE_ID, "date": date, "account_id": ACCOUNT_A},
            headers=auth,
        )
        assert response.status_code == 422

    async def test_a_future_day_is_refused(self, client, auth):
        response = await client.get(
            DAILY,
            params={"source_id": SOURCE_ID, "date": "2099-01-01", "account_id": ACCOUNT_A},
            headers=auth,
        )
        assert response.status_code == 422
        assert response.json()["code"] == "INVALID_TIME_WINDOW"

    async def test_a_broker_wide_daily_report_is_not_offered(self, client, auth):
        response = await client.get(
            DAILY, params={"source_id": SOURCE_ID, "date": "2026-10-15"}, headers=auth
        )
        assert response.status_code == 422
        assert "account_id" in response.json()["detail"]


class TestCalculations:
    def body(self, **overrides) -> dict:
        base = {
            "source_id": SOURCE_ID,
            "account_ids": [ACCOUNT_A],
            "components": ["EXPOSURE"],
        }
        base.update(overrides)
        return base

    async def test_a_valid_request_computes(self, client, auth, no_float_money):
        response = await client.post(CALC, json=self.body(), headers=auth)
        assert response.status_code == 200, response.text
        no_float_money(response.text)
        assert response.json()["scope"]["account_ids"] == [ACCOUNT_A]

    async def test_strict_composite_is_the_default(self, client, auth):
        response = await client.post(CALC, json=self.body(), headers=auth)
        assert response.json()["partial_allowed"] is False

    async def test_an_unknown_member_is_refused(self, client, context, auth):
        response = await client.post(CALC, json=self.body(bypass_cut=True), headers=auth)
        assert response.status_code == 422
        assert context.engine.stats.calls == 0

    async def test_duplicate_members_are_refused(self, client, context, auth):
        """WF06-A-10 over the wire."""
        raw = b'{"source_id":"sim-broker-a","account_ids":["00042"],"components":["EXPOSURE"],"allow_partial":true,"allow_partial":false}'
        response = await client.post(
            CALC, content=raw, headers={**auth, "content-type": "application/json"}
        )
        assert response.status_code == 422
        assert response.json()["code"] == "INVALID_PAYLOAD"
        assert context.engine.stats.calls == 0

    async def test_truncated_json_is_refused(self, client, auth):
        response = await client.post(
            CALC, content=b'{"source_id":"sim', headers={**auth, "content-type": "application/json"}
        )
        assert response.status_code == 422
        assert response.json()["code"] == "INVALID_PAYLOAD"

    async def test_an_oversized_body_is_refused(self, client, context, auth):
        payload = self.body(account_ids=[ACCOUNT_A])
        payload["padding"] = "x" * 70_000
        response = await client.post(CALC, json=payload, headers=auth)
        assert response.status_code == 413
        assert response.json()["code"] == "REQUEST_TOO_LARGE"
        assert context.engine.stats.calls == 0

    async def test_deeply_nested_json_is_refused(self, client, auth):
        nested: object = 1
        for _ in range(20):
            nested = {"a": nested}
        response = await client.post(
            CALC,
            content=json.dumps({"source_id": SOURCE_ID, "nest": nested}).encode("utf-8"),
            headers={**auth, "content-type": "application/json"},
        )
        assert response.status_code == 422

    async def test_a_nan_literal_is_refused(self, client, auth):
        response = await client.post(
            CALC,
            content=b'{"source_id":"sim-broker-a","account_ids":["00042"],"components":["EXPOSURE"],"extra":NaN}',
            headers={**auth, "content-type": "application/json"},
        )
        assert response.status_code == 422

    async def test_an_out_of_scope_account_in_the_body_is_denied(self, client, context, auth):
        response = await client.post(CALC, json=self.body(account_ids=[ACCOUNT_B]), headers=auth)
        assert response.status_code == 404
        assert context.engine.stats.calls == 0

    async def test_a_query_parameter_on_the_post_is_refused(self, client, auth):
        response = await client.post(
            CALC, json=self.body(), params={"source_id": SOURCE_ID}, headers=auth
        )
        assert response.status_code == 422

    async def test_an_uninstalled_policy_version_is_refused(self, client, context, auth):
        response = await client.post(
            CALC, json=self.body(risk_policy_version="risk-policy-v99"), headers=auth
        )
        assert response.status_code == 422
        assert response.json()["code"] == "UNSUPPORTED_CALC_MODE"
        assert context.engine.stats.calls == 0

    async def test_a_pinned_epoch_is_refused_while_the_feature_is_gated(
        self, client, context, auth
    ):
        """WF06-C-05: a client cannot select an epoch to dodge a finding."""
        response = await client.post(CALC, json=self.body(epoch_id="E0"), headers=auth)
        assert response.status_code == 403
        assert "EPOCH_PINNING_NOT_PERMITTED" in response.json()["reason_codes"]
        assert context.engine.stats.calls == 0
