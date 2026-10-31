"""Redaction, log framing, metric cardinality, and the read-plan contract.

Covers WF2-B-06, WF2-N-05, WF2-N-06, WF2-N-07, WF2-P-05, WF2-P-09, WF2-I-05,
WF2-A-08, INV-15 and the type-level half of ADR-207.
"""

from __future__ import annotations

import json
import logging
from decimal import Decimal

import pytest

from mt5_connector.errors import ConfigurationError, TransportError
from mt5_connector.ports import (
    CheckpointCandidate,
    Entity,
    Page,
    Provenance,
    ReadPlan,
    ScanOutcome,
    ScanResult,
    SourceIdentity,
)
from mt5_connector.telemetry import (
    REDACTED,
    EventLoopMonitor,
    JsonFormatter,
    Metrics,
    RedactingFilter,
    SecretRegistry,
    sanitize_token_text,
)

pytestmark = pytest.mark.unit

CANARY = "SECRET_CANARY_123_do_not_log_me"


class TestRedaction:
    def test_a_registered_secret_is_scrubbed(self) -> None:
        """WF2-N-07, INV-15."""
        secrets = SecretRegistry()
        secrets.add(CANARY)
        assert CANARY not in secrets.scrub(f"connecting with {CANARY} to source")
        assert REDACTED in secrets.scrub(f"token={CANARY}")

    def test_a_too_short_value_is_not_registered(self) -> None:
        # A two-character "secret" would match everywhere and redact the logs
        # into uselessness.
        secrets = SecretRegistry()
        secrets.add("ab")
        assert secrets.count == 0
        assert secrets.scrub("ab cd") == "ab cd"

    @pytest.mark.parametrize(
        "text",
        [
            "Authorization: Bearer abcdef1234567890",
            "authorization=abcdef1234567890",
            "token: abcdef1234567890",
            "api_key=abcdef1234567890",
            "password: hunter2hunter2",
            "https://host/v1/deals?token=abcdef1234567890",
        ],
    )
    def test_credential_shaped_text_is_scrubbed_even_when_unregistered(self, text: str) -> None:
        """A rotated token still sitting in a buffer must not survive a log."""
        secrets = SecretRegistry()
        scrubbed = secrets.scrub(text)
        assert REDACTED in scrubbed
        assert "abcdef1234567890" not in scrubbed
        assert "hunter2hunter2" not in scrubbed

    def test_the_filter_scrubs_message_args_and_extras(self) -> None:
        secrets = SecretRegistry()
        secrets.add(CANARY)
        log_filter = RedactingFilter(secrets)
        record = logging.LogRecord(
            name="t", level=logging.WARNING, pathname=__file__, lineno=1,
            msg="failed with %s", args=(CANARY,), exc_info=None,
        )
        record.source_id = f"broker-{CANARY}"
        assert log_filter.filter(record) is True
        assert CANARY not in record.getMessage()
        assert CANARY not in record.source_id

    def test_the_filter_scrubs_an_exception_argument(self) -> None:
        """WF2-B-06: httpx puts the full URL in some of its exceptions."""
        secrets = SecretRegistry()
        secrets.add(CANARY)
        log_filter = RedactingFilter(secrets)
        error = TransportError(f"GET https://host/v1/deals?token={CANARY} failed")
        record = logging.LogRecord(
            name="t", level=logging.ERROR, pathname=__file__, lineno=1,
            msg="boom", args=(), exc_info=(type(error), error, None),
        )
        log_filter.filter(record)
        assert CANARY not in "".join(str(arg) for arg in error.args)


class TestLogFraming:
    def test_each_record_is_one_valid_json_object(self) -> None:
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="mt5_connector.client", level=logging.WARNING, pathname=__file__,
            lineno=1, msg="page rejected", args=(), exc_info=None,
        )
        record.request_id = "req-007"
        record.worker_epoch = 13
        payload = json.loads(formatter.format(record))
        assert payload["level"] == "WARN" or payload["level"] == "WARNING"
        assert payload["service"] == "mt5-connector"
        assert payload["request_id"] == "req-007"
        assert payload["worker_epoch"] == 13
        assert payload["message"] == "page rejected"

    def test_a_control_character_cannot_break_the_log_stream(self) -> None:
        """WF2-N-06: an injected newline would forge a second log line."""
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="t", level=logging.INFO, pathname=__file__, lineno=1,
            msg="ok", args=(), exc_info=None,
        )
        record.request_id = sanitize_token_text("req-1\n{\"level\":\"ERROR\"}")
        line = formatter.format(record)
        assert line.count("\n") == 0
        assert json.loads(line)["request_id"] == 'req-1{"level":"ERROR"}'

    def test_a_non_scalar_extra_is_collapsed_not_dumped(self) -> None:
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="t", level=logging.INFO, pathname=__file__, lineno=1,
            msg="ok", args=(), exc_info=None,
        )
        record.page = {"items": [1, 2, 3]}
        assert json.loads(formatter.format(record))["page"] == "<dict>"

    def test_html_in_data_is_escaped_as_data_not_rendered(self) -> None:
        """WF2-N-05: a payload is data; it never becomes markup or a log line."""
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="t", level=logging.INFO, pathname=__file__, lineno=1,
            msg="ok", args=(), exc_info=None,
        )
        record.symbol = sanitize_token_text("<script>alert(1)</script>")
        decoded = json.loads(formatter.format(record))
        assert decoded["symbol"] == "<script>alert(1)</script>"

    def test_sanitize_bounds_the_length(self) -> None:
        assert len(sanitize_token_text("x" * 5_000)) == 256


class TestMetricCardinality:
    def test_no_collector_takes_a_business_identifier_as_a_label(self) -> None:
        """WF2-P-09: an unbounded label takes the metrics backend down."""
        metrics = Metrics(source_id="broker-sim-01")
        forbidden = {"account_id", "deal_id", "position_id", "request_id", "cursor", "token"}
        for name in dir(metrics):
            collector = getattr(metrics, name, None)
            labels = getattr(collector, "_labelnames", None)
            if labels is None:
                continue
            assert not forbidden & set(labels), f"{name} exposes {set(labels) & forbidden}"

    def test_rendering_produces_prometheus_text(self) -> None:
        metrics = Metrics(source_id="broker-sim-01")
        metrics.observe_request(
            operation="fetch_deals", outcome="success", error_class="none", duration_s=0.01
        )
        rendered = metrics.render().decode()
        assert "connector_requests_total" in rendered
        assert 'operation="fetch_deals"' in rendered

    def test_retry_attempts_are_bucketed(self) -> None:
        metrics = Metrics(source_id="s")
        for attempt in (1, 2, 3, 4, 9):
            metrics.observe_retry(error_class="UPSTREAM_UNAVAILABLE", attempt=attempt)
        rendered = metrics.render().decode()
        assert 'attempt="3+"' in rendered
        assert 'attempt="9"' not in rendered

    def test_a_label_value_is_sanitized(self) -> None:
        metrics = Metrics(source_id="s")
        metrics.observe_validation_error(entity="deals", field_name="price\nfake")
        assert "pricefake" in metrics.render().decode()


class TestEventLoopMonitor:
    def test_summary_is_empty_without_samples(self) -> None:
        summary = EventLoopMonitor().summary()
        assert summary.samples == 0 and summary.p99_ms == 0.0

    def test_quantiles_are_computed_over_recorded_lag(self) -> None:
        monitor = EventLoopMonitor()
        for value in [0.001] * 99 + [0.5]:
            monitor.record(value)
        summary = monitor.summary()
        assert summary.samples == 100
        assert summary.p50_ms == pytest.approx(1.0, abs=0.1)
        assert summary.max_ms == pytest.approx(500.0, abs=0.1)

    def test_samples_are_bounded_for_a_soak_run(self) -> None:
        monitor = EventLoopMonitor(capacity=100)
        for _ in range(10_000):
            monitor.record(0.001)
        assert len(monitor.samples) == 100


class TestReadPlan:
    def test_a_cursor_must_travel_alone(self) -> None:
        """WF2-I-05: the connector cannot build the illegal mix."""
        for extra in ({"after_seq": 5}, {"account_id": "A"}, {"symbol": "EURUSD"}):
            with pytest.raises(ConfigurationError) as excinfo:
                ReadPlan(entity=Entity.DEALS, limit=10, cursor="abc", **extra)
            assert excinfo.value.details["field"] == "cursor"

    def test_advance_drops_every_filter(self) -> None:
        plan = ReadPlan(
            entity=Entity.DEALS, limit=10, after_seq=130, account_id="A", symbol="EURUSD"
        )
        nxt = plan.advance("cursor-1")
        assert nxt.cursor == "cursor-1"
        assert nxt.after_seq is None and nxt.account_id is None and nxt.symbol is None
        assert nxt.limit == 10 and nxt.entity is Entity.DEALS

    def test_after_seq_is_rejected_for_non_deal_entities(self) -> None:
        with pytest.raises(ConfigurationError) as excinfo:
            ReadPlan(entity=Entity.ACCOUNTS, limit=10, after_seq=1)
        assert excinfo.value.details["entity"] == "accounts"

    def test_invalid_limits_and_cursors_are_rejected(self) -> None:
        with pytest.raises(ConfigurationError):
            ReadPlan(entity=Entity.DEALS, limit=0)
        with pytest.raises(ConfigurationError):
            ReadPlan(entity=Entity.DEALS, limit=10, cursor="")
        with pytest.raises(ConfigurationError):
            ReadPlan(entity=Entity.DEALS, limit=10, cursor="c" * 5_000)
        with pytest.raises(ConfigurationError):
            ReadPlan(entity=Entity.DEALS, limit=10, after_seq=-1)

    def test_a_plan_is_immutable(self) -> None:
        plan = ReadPlan(entity=Entity.DEALS, limit=10)
        with pytest.raises(Exception):
            plan.limit = 500  # type: ignore[misc]

    def test_filter_fields_identify_the_scan(self) -> None:
        """WF2-A-08: the filter set is snapshotted at scan start."""
        fields = ReadPlan(
            entity=Entity.DEALS, limit=10, after_seq=130, account_id="A"
        ).filter_fields()
        assert fields == {
            "entity": "deals",
            "limit": 10,
            "after_seq": 130,
            "account_id": "A",
            "symbol": None,
        }


class TestHandoffTypes:
    def test_a_checkpoint_candidate_says_what_it_is(self) -> None:
        """ADR-207: the type itself carries the caveat."""
        candidate = CheckpointCandidate(
            source_id="s", entity=Entity.DEALS, run_id="r", generation=1,
            snapshot_seq=130, filter_digest="d", pages_committed_by_source=2, items_seen=20,
        )
        assert "candidate only" in candidate.note
        assert not hasattr(candidate, "commit")

    def test_a_non_complete_scan_has_no_candidate(self) -> None:
        """WF2-I-06, WF2-K-03."""
        identity = SourceIdentity(source_kind="fake", source_id="s", worker_epoch=1)
        result = ScanResult(
            outcome=ScanOutcome.RESYNC_REQUIRED, entity=Entity.DEALS, identity=identity,
            pages_seen=17, items_seen=170, snapshot_seq=130, error_code="SOURCE_EPOCH_CHANGED",
        )
        assert result.checkpoint_candidate is None
        assert not result.complete

    def test_page_items_are_a_tuple_so_evidence_cannot_be_mutated(self) -> None:
        identity = SourceIdentity(source_kind="fake", source_id="s", worker_epoch=1)
        provenance = Provenance(
            source_id="s", entity=Entity.DEALS, run_id="r", generation=1, snapshot_seq=130,
            worker_epoch=1, schema_version="v1", filter_digest="d",
        )
        page: Page = Page(
            identity=identity, provenance=provenance, items=(Decimal("1"),), next_cursor=None,
            snapshot_seq=130, requested_after_seq=None, observed_upper_bound=None,
            page_index=0, response_bytes=10,
        )
        assert isinstance(page.items, tuple)
        assert page.is_terminal

    def test_terminal_means_no_more_pages_in_this_snapshot_only(self) -> None:
        """WF2-Q-04: a consumer reading it as "no more data ever" stops polling."""
        assert "Page.is_terminal" or True
        assert "snapshot" in Page.is_terminal.__doc__
