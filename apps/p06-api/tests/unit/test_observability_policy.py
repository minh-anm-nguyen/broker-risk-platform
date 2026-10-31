"""Metrics cardinality and log redaction.

WF06-AC and WF06-D. A metrics endpoint that accepts an account id as a label
is a data leak with a monitoring label on it, and a log line holding a bearer
token is a credential leak. Both are prevented in code, so both are tested.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from io import StringIO

import pytest
from risk_api.observability.logging import (
    REDACTED,
    configure_logging,
    log_event,
    safe_event,
)
from risk_api.observability.metrics import LabelPolicyError, Metrics

pytestmark = pytest.mark.unit


@pytest.fixture
def metrics() -> Metrics:
    return Metrics()


class TestMetricCardinality:
    def test_a_route_template_is_accepted(self, metrics):
        metrics.observe_request(
            route="/api/v1/accounts/{account_id}/risk",
            method="GET",
            status=200,
            duration_seconds=0.01,
        )
        assert (
            metrics.counter_value(
                "api_http_requests_total",
                route="/api/v1/accounts/{account_id}/risk",
                method="GET",
                status_class="2xx",
            )
            == 1.0
        )

    def test_a_concrete_path_with_an_account_id_is_refused(self, metrics):
        """INV06-01: an account number must never become a label value."""
        with pytest.raises(LabelPolicyError):
            metrics.observe_request(
                route="/api/v1/accounts/00042/risk", method="GET", status=200, duration_seconds=0.01
            )

    def test_a_numeric_label_value_is_refused(self, metrics):
        with pytest.raises(LabelPolicyError):
            metrics.observe_denial(denial_family="00042", route="/api/v1/exposure")

    def test_a_digest_label_value_is_refused(self, metrics):
        with pytest.raises(LabelPolicyError):
            metrics.observe_financial_status(
                status="READY", reason_family="sha256:" + "a" * 64
            )

    def test_an_overlong_label_value_is_refused(self, metrics):
        with pytest.raises(LabelPolicyError):
            metrics.observe_engine(outcome="x" * 200)

    def test_an_empty_label_value_is_refused(self, metrics):
        with pytest.raises(LabelPolicyError):
            metrics.observe_engine(outcome="")

    def test_status_is_bucketed_into_a_class(self, metrics):
        for status in (200, 201, 204):
            metrics.observe_request(
                route="/health/live", method="GET", status=status, duration_seconds=0.001
            )
        assert (
            metrics.counter_value(
                "api_http_requests_total", route="/health/live", method="GET", status_class="2xx"
            )
            == 3.0
        )


class TestCriticalSignal:
    def test_the_unauthorized_access_counter_starts_at_zero(self, metrics):
        """This counter must remain zero in every passing run."""
        assert metrics.counter_value("api_unauthorized_data_access_total", surface="response") == 0.0

    def test_it_can_be_raised_only_deliberately(self, metrics):
        metrics.flag_unauthorized_data_access(surface="response")
        assert metrics.counter_value("api_unauthorized_data_access_total", surface="response") == 1.0


class TestRenderingAndGauges:
    def test_render_emits_prometheus_text(self, metrics):
        metrics.observe_audit(result="allow")
        body = metrics.render().decode("utf-8")
        assert "api_audit_receipts_total" in body
        assert "api_unauthorized_data_access_total" in body

    def test_gauges_reflect_live_state(self, metrics):
        metrics.set_gauges(inflight=3, heavy_queue_depth=5, heartbeat_p99=0.004)
        assert metrics.counter_value("api_inflight_requests") == 3.0
        assert metrics.counter_value("api_heavy_queue_depth") == 5.0
        assert metrics.counter_value("api_worker_heartbeat_lag", worker_kind="engine") == 0.004

    def test_each_instance_has_its_own_registry(self):
        """Two app instances in one test session must not share counters."""
        first, second = Metrics(), Metrics()
        first.observe_engine(outcome="completed")
        assert first.counter_value("api_engine_invocations_total", outcome="completed") == 1.0
        assert second.counter_value("api_engine_invocations_total", outcome="completed") == 0.0


class TestLogRedaction:
    def test_a_forbidden_field_name_is_replaced(self):
        event = safe_event("report_served", account_id="00042", amount="56.00")
        assert event["account_id"] == REDACTED
        assert event["amount"] == REDACTED

    def test_a_bearer_token_in_any_field_is_replaced(self):
        event = safe_event("authentication_failed", note="Authorization: Bearer abcdef1234567890")
        assert event["note"] == REDACTED

    def test_a_bare_jwt_is_replaced(self):
        jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbmFseXN0LWEifQ.c2lnbmF0dXJlLWJ5dGVz"
        assert safe_event("probe", token_like=jwt)["token_like"] == REDACTED

    def test_a_money_looking_string_is_replaced(self):
        assert safe_event("probe", note="56.00")["note"] == REDACTED

    def test_a_float_is_replaced(self):
        assert safe_event("probe", value=56.0)["value"] == REDACTED

    def test_nested_forbidden_keys_are_replaced(self):
        event = safe_event("probe", payload={"components": {"realized": {"net_usd": "56.00"}}})
        assert event["payload"]["components"]["realized"]["net_usd"] == REDACTED

    def test_safe_fields_survive(self):
        event = safe_event(
            "report_served", request_id="req-1", status="READY", endpoint_id="accounts.risk"
        )
        assert event["request_id"] == "req-1"
        assert event["status"] == "READY"
        assert event["endpoint_id"] == "accounts.risk"

    def test_long_strings_are_truncated(self):
        assert len(safe_event("probe", note="x" * 5000)["note"]) <= 520

    def test_deeply_nested_values_are_collapsed(self):
        nested: object = "leaf"
        for _ in range(12):
            nested = {"inner": nested}
        assert safe_event("probe", payload=nested)["payload"] is not None

    def test_the_formatter_emits_one_json_object_per_record(self):
        stream = StringIO()
        logger = configure_logging(level="INFO", stream=stream)
        log_event(logger, logging.INFO, "report_served", request_id="req-9", account_id="00042")
        line = stream.getvalue().strip()
        assert line.startswith("{") and line.endswith("}")
        assert '"request_id":"req-9"' in line
        assert "00042" not in line

    def test_an_exception_is_logged_by_type_without_a_stack_trace(self):
        stream = StringIO()
        logger = configure_logging(level="INFO", stream=stream)
        try:
            raise ValueError("secret detail about /var/lib/postgresql")
        except ValueError:
            logger.exception("boom", extra={"event_payload": safe_event("boom")})
        line = stream.getvalue()
        assert '"exception_type":"ValueError"' in line
        assert "postgresql" not in line
        assert "Traceback" not in line

    def test_decimal_values_are_stringified_not_dropped(self):
        event = safe_event("probe", count=3, label=str(Decimal("1")))
        assert event["count"] == 3
        assert event["label"] == "1"
