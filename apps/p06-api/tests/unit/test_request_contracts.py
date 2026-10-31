"""Request parsing bounds.

WF06-A and WF06-Y. An input the service does not understand is an error. The
nastier cases live here: duplicate JSON members, non-finite constants, deep
nesting, oversized bodies and unknown query keys.
"""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError
from risk_api.contracts.problem_details import ApiProblem
from risk_api.contracts.requests import (
    RiskCalculationRequest,
    decode_json_body,
    enforce_query_allowlist,
    json_depth,
    parse_components,
    parse_rfc3339_utc,
    parse_utc_date,
)

pytestmark = pytest.mark.unit


class TestComponents:
    def test_omitted_components_default_to_all(self):
        assert parse_components(None) == ("EXPOSURE", "REALIZED", "UNREALIZED")

    def test_an_explicit_subset_is_honoured(self):
        assert parse_components("REALIZED") == ("REALIZED",)

    def test_order_is_canonicalised(self):
        assert parse_components("UNREALIZED,EXPOSURE") == ("EXPOSURE", "UNREALIZED")

    def test_duplicates_collapse(self):
        assert parse_components("REALIZED,REALIZED") == ("REALIZED",)

    def test_an_unknown_member_fails_the_whole_request(self):
        """WF06-A-03: no partial compute from a partially understood enum."""
        with pytest.raises(ApiProblem) as caught:
            parse_components("EXPOSURE,BOGUS")
        assert caught.value.code == "INVALID_REQUEST"
        assert "BOGUS" in caught.value.detail

    @pytest.mark.parametrize("raw", [",", "EXPOSURE,", ",EXPOSURE", "EXPOSURE,,REALIZED"])
    def test_empty_members_are_refused(self, raw: str):
        with pytest.raises(ApiProblem):
            parse_components(raw)

    def test_case_is_significant(self):
        with pytest.raises(ApiProblem):
            parse_components("exposure")


class TestInstants:
    def test_a_utc_instant_parses(self):
        parsed = parse_rfc3339_utc("2026-10-01T00:00:00Z", field="period_start")
        assert parsed.isoformat() == "2026-10-01T00:00:00+00:00"

    @pytest.mark.parametrize(
        "raw",
        [
            "2026-10-01T00:00:00",
            "2026-10-01T00:00:00+02:00",
            "2026-10-01 00:00:00Z",
            "2026-13-01T00:00:00Z",
            "not-a-date",
            "",
            "2026-10-01T00:00:00.000000000000000000000Z",
        ],
    )
    def test_anything_but_a_z_suffixed_utc_instant_is_refused(self, raw: str):
        with pytest.raises(ApiProblem) as caught:
            parse_rfc3339_utc(raw, field="period_start")
        assert caught.value.status == 422

    def test_a_calendar_date_parses(self):
        assert parse_utc_date("2026-10-30", field="date").isoformat() == "2026-10-30"

    @pytest.mark.parametrize(
        "raw", ["2026-10-30T00:00:00Z", "2026/10/30", "26-10-30", "2026-02-30", "", "2026-1-1"]
    )
    def test_a_malformed_date_is_refused(self, raw: str):
        with pytest.raises(ApiProblem):
            parse_utc_date(raw, field="date")


class TestQueryAllowlist:
    def test_declared_keys_pass(self):
        enforce_query_allowlist(["source_id", "components"], frozenset({"source_id", "components"}))

    def test_an_undeclared_key_is_refused(self):
        """WF06-A-02: a silently ignored bypass_cut=true is indistinguishable
        from an honoured one, so it is an error."""
        with pytest.raises(ApiProblem) as caught:
            enforce_query_allowlist(["source_id", "bypass_cut"], frozenset({"source_id"}))
        assert caught.value.code == "INVALID_REQUEST"
        assert "bypass_cut" in caught.value.detail

    def test_several_undeclared_keys_are_all_named(self):
        with pytest.raises(ApiProblem) as caught:
            enforce_query_allowlist(["a", "b"], frozenset())
        assert "a" in caught.value.detail and "b" in caught.value.detail


class TestJsonDecoding:
    def test_a_well_formed_object_decodes(self):
        assert decode_json_body(b'{"a":1}', max_bytes=1000, max_depth=5) == {"a": 1}

    def test_a_duplicate_member_is_refused(self):
        """WF06-A-10: opposing values for one key cannot be silently resolved."""
        raw = b'{"allow_partial":true,"allow_partial":false}'
        with pytest.raises(ApiProblem) as caught:
            decode_json_body(raw, max_bytes=1000, max_depth=5)
        assert caught.value.code == "INVALID_PAYLOAD"
        assert "duplicate" in caught.value.detail

    def test_a_nested_duplicate_member_is_refused(self):
        raw = b'{"outer":{"x":1,"x":2}}'
        with pytest.raises(ApiProblem):
            decode_json_body(raw, max_bytes=1000, max_depth=5)

    @pytest.mark.parametrize("constant", [b"NaN", b"Infinity", b"-Infinity"])
    def test_non_finite_json_constants_are_refused(self, constant: bytes):
        raw = b'{"amount":' + constant + b"}"
        with pytest.raises(ApiProblem) as caught:
            decode_json_body(raw, max_bytes=1000, max_depth=5)
        assert caught.value.code == "INVALID_PAYLOAD"

    def test_an_oversized_body_is_refused_before_parsing(self):
        with pytest.raises(ApiProblem) as caught:
            decode_json_body(b'{"a":"' + b"x" * 100 + b'"}', max_bytes=10, max_depth=5)
        assert caught.value.code == "REQUEST_TOO_LARGE"
        assert caught.value.status == 413

    def test_an_empty_body_is_refused(self):
        with pytest.raises(ApiProblem):
            decode_json_body(b"   ", max_bytes=100, max_depth=5)

    def test_truncated_json_is_refused(self):
        """WF06-A-05: a missing bracket costs bounded CPU and returns 422."""
        with pytest.raises(ApiProblem) as caught:
            decode_json_body(b'{"a":[1,2', max_bytes=1000, max_depth=5)
        assert caught.value.code == "INVALID_PAYLOAD"

    def test_invalid_utf8_is_refused(self):
        with pytest.raises(ApiProblem):
            decode_json_body(b'{"a":"\xff\xfe"}', max_bytes=1000, max_depth=5)

    def test_a_json_array_body_is_refused(self):
        with pytest.raises(ApiProblem):
            decode_json_body(b"[1,2,3]", max_bytes=1000, max_depth=5)

    def test_excess_nesting_is_refused(self):
        payload = json.dumps({"a": {"b": {"c": {"d": {"e": 1}}}}}).encode("utf-8")
        with pytest.raises(ApiProblem) as caught:
            decode_json_body(payload, max_bytes=1000, max_depth=3)
        assert "nesting" in caught.value.detail

    def test_nesting_at_the_exact_limit_is_accepted(self):
        payload = json.dumps({"a": {"b": 1}}).encode("utf-8")
        assert decode_json_body(payload, max_bytes=1000, max_depth=3) == {"a": {"b": 1}}

    @pytest.mark.parametrize(
        ("value", "depth"),
        [(1, 1), ({}, 1), ([], 1), ({"a": 1}, 2), ({"a": {"b": 1}}, 3), ([[1]], 3)],
    )
    def test_depth_measurement(self, value: object, depth: int):
        assert json_depth(value) == depth


class TestCalculationRequestSchema:
    def body(self, **overrides) -> dict:
        base = {
            "source_id": "sim-broker-a",
            "account_ids": ["00042"],
            "components": ["EXPOSURE"],
        }
        base.update(overrides)
        return base

    def test_a_minimal_body_validates(self):
        model = RiskCalculationRequest.model_validate(self.body())
        assert model.allow_partial is False, "strict composite is the default"
        assert model.canonical_accounts() == ("00042",)

    def test_an_unknown_member_is_refused(self):
        with pytest.raises(ValidationError):
            RiskCalculationRequest.model_validate(self.body(bypass_cut=True))

    def test_an_empty_account_list_is_refused(self):
        """WF06-C-08: an empty list is not a wildcard over the broker."""
        with pytest.raises(ValidationError):
            RiskCalculationRequest.model_validate(self.body(account_ids=[]))

    def test_repeated_accounts_are_refused(self):
        with pytest.raises(ValidationError):
            RiskCalculationRequest.model_validate(self.body(account_ids=["00042", "00042"]))

    def test_an_empty_component_list_is_refused(self):
        with pytest.raises(ValidationError):
            RiskCalculationRequest.model_validate(self.body(components=[]))

    def test_an_unknown_component_is_refused(self):
        with pytest.raises(ValidationError):
            RiskCalculationRequest.model_validate(self.body(components=["BOGUS"]))

    def test_realized_without_a_period_is_refused(self):
        with pytest.raises(ValidationError):
            RiskCalculationRequest.model_validate(self.body(components=["REALIZED"]))

    def test_realized_with_a_period_validates(self):
        model = RiskCalculationRequest.model_validate(
            self.body(
                components=["REALIZED"],
                period_start_utc="2026-10-01T00:00:00Z",
                period_end_utc="2026-10-30T00:00:00Z",
            )
        )
        assert model.period_start_utc == "2026-10-01T00:00:00Z"

    def test_a_malformed_account_id_is_refused(self):
        with pytest.raises(ValidationError):
            RiskCalculationRequest.model_validate(self.body(account_ids=["../../etc/passwd"]))

    def test_an_overlong_account_id_is_refused(self):
        with pytest.raises(ValidationError):
            RiskCalculationRequest.model_validate(self.body(account_ids=["0" * 64]))

    def test_a_malformed_source_id_is_refused(self):
        with pytest.raises(ValidationError):
            RiskCalculationRequest.model_validate(self.body(source_id="SIM-Broker-A"))

    def test_allow_partial_must_be_a_boolean_not_a_string(self):
        """Strict mode means "true" is not the same as true."""
        with pytest.raises(ValidationError):
            RiskCalculationRequest.model_validate(self.body(allow_partial="true"))

    def test_the_model_is_frozen(self):
        model = RiskCalculationRequest.model_validate(self.body())
        with pytest.raises(ValidationError):
            model.allow_partial = True  # type: ignore[misc]

    def test_components_are_canonicalised(self):
        model = RiskCalculationRequest.model_validate(
            self.body(
                components=["UNREALIZED", "EXPOSURE", "REALIZED"],
                period_start_utc="2026-10-01T00:00:00Z",
                period_end_utc="2026-10-30T00:00:00Z",
            )
        )
        assert model.canonical_components() == ("EXPOSURE", "REALIZED", "UNREALIZED")

    def test_accounts_are_canonically_sorted(self):
        model = RiskCalculationRequest.model_validate(
            self.body(account_ids=["00043", "00042"])
        )
        assert model.canonical_accounts() == ("00042", "00043")
