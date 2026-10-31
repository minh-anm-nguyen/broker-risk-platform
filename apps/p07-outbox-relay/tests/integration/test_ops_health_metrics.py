"""Operations: quarantine, replay, health, metrics, logging redaction.

The replay tests are the ones worth reading. Design section 10.6 asks for an
audited, role-gated action with a dry-run manifest, and each of those words is a
separate assertion here -- because "replay" with none of them is just a loop
that republishes money.
"""

from __future__ import annotations

import io
import json
import logging

import pytest

from outbox.broker.embedded import Fault
from outbox.errors import ErrorCode, IntegrityIncident
from outbox.events.routing import DEALS_TOPIC, POISON_TOPIC
from outbox.fixtures import EPOCH_A, deal
from outbox.health import Readiness
from outbox.observability.logging import JsonFormatter, configure, get_logger, redact
from outbox.observability.metrics import Metrics
from outbox.ops.quarantine import (
    blocked_aggregates,
    list_quarantined,
    poison_notice,
    publish_poison_notice,
)
from outbox.ops.replay import backlog_drain_estimate, execute_replay, plan_replay
from outbox.persistence.ports import AttemptOutcome, EventState
from outbox.pipeline import Pipeline

pytestmark = pytest.mark.integration


def _quarantine_one(pipeline: Pipeline, deal_id: str = "D-1001", account_id: str = "A-42"):
    pipeline.sync.insert_deals(EPOCH_A, [deal(deal_id, account_id=account_id)])
    claim = next(
        claim
        for claim in pipeline.store.claim_due(
            owner="a", batch_size=10, lease_seconds=30, now=pipeline.clock.now()
        )
        if claim.aggregate_key == account_id
    )
    pipeline.store.quarantine_if_owner(
        claim=claim,
        reason="unsupported schema v2 for this aggregate",
        error_code="SCHEMA_UNSUPPORTED",
        now=pipeline.clock.now(),
    )
    return claim


class TestQuarantineInspection:
    def test_a_quarantined_row_is_listed_with_its_cause(self, pipeline: Pipeline) -> None:
        claim = _quarantine_one(pipeline)
        entries = list_quarantined(pipeline.store)
        assert len(entries) == 1
        entry = entries[0]
        assert entry.event_id == str(claim.event_id)
        assert entry.error_code == "SCHEMA_UNSUPPORTED"
        assert "unsupported schema" in (entry.reason or "")

    def test_blocked_aggregates_name_the_blocking_version(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(
            EPOCH_A,
            [deal(f"D-{index:04d}", account_id="A-42", source_seq=index) for index in range(1, 4)],
        )
        claim = pipeline.store.claim_due(
            owner="a", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )[0]
        pipeline.store.quarantine_if_owner(
            claim=claim, reason="poison", error_code="SCHEMA_UNSUPPORTED", now=pipeline.clock.now()
        )
        blocked = blocked_aggregates(pipeline.store)
        assert blocked == {(pipeline.source_id, EPOCH_A, "A-42"): 1}

    def test_the_poison_notice_carries_no_payload(self, pipeline: Pipeline) -> None:
        """Sanitised metadata only (design sections 5.2 and 10.4)."""
        _quarantine_one(pipeline)
        entry = list_quarantined(pipeline.store)[0]
        notice = poison_notice(entry, now=pipeline.clock.now())

        serialised = json.dumps(notice)
        for leaked in ("12.30", "-0.50", "EURUSD", "A-42", "realized_pnl_gross"):
            if leaked == "A-42":
                # The aggregate key is an opaque account id and is needed to
                # say what is blocked; the financial fields are not.
                continue
            assert leaked not in serialised, f"{leaked!r} leaked into the poison notice"
        assert set(notice) == {
            "event_id",
            "event_type",
            "source_id",
            "epoch_id",
            "aggregate_key",
            "aggregate_version",
            "error_code",
            "reason",
            "payload_sha256",
            "quarantined_at",
        }

    def test_the_notice_goes_to_the_poison_topic(self, pipeline: Pipeline) -> None:
        _quarantine_one(pipeline)
        entry = list_quarantined(pipeline.store)[0]
        publish_poison_notice(pipeline.producer, entry, now=pipeline.clock.now())
        pipeline.producer.poll()
        assert pipeline.broker.record_count(POISON_TOPIC.name) == 1
        assert pipeline.broker.record_count(DEALS_TOPIC.name) == 0


class TestReplay:
    def test_a_plan_is_a_manifest_and_changes_nothing(self, pipeline: Pipeline) -> None:
        claim = _quarantine_one(pipeline)
        plan = plan_replay(pipeline.store, epoch_id=EPOCH_A)
        assert plan.event_ids == (str(claim.event_id),)
        assert plan.aggregates == ((pipeline.source_id, EPOCH_A, "A-42"),)
        assert len(plan.manifest_sha256) == 64
        # Still quarantined: planning is read-only.
        record = pipeline.store.get(claim.event_id)
        assert record is not None and record.state is EventState.QUARANTINED

    def test_execute_requeues_and_preserves_the_event_id(self, pipeline: Pipeline) -> None:
        claim = _quarantine_one(pipeline)
        plan = plan_replay(pipeline.store, epoch_id=EPOCH_A)
        outcome = execute_replay(
            pipeline.store, plan, approved_by="ops-oncall", now=pipeline.clock.now()
        )
        assert outcome.requeued == 1
        record = pipeline.store.get(claim.event_id)
        assert record is not None
        assert record.state is EventState.PENDING
        # The same identity re-enters the pipeline, so downstream dedup still
        # recognises it.
        assert record.event_id == claim.event_id
        assert record.attempt_count == 0

    def test_replay_requires_an_approver(self, pipeline: Pipeline) -> None:
        _quarantine_one(pipeline)
        plan = plan_replay(pipeline.store)
        with pytest.raises(IntegrityIncident, match="named approver"):
            execute_replay(pipeline.store, plan, approved_by="", now=pipeline.clock.now())

    def test_a_financially_sensitive_replay_needs_two_distinct_people(
        self, pipeline: Pipeline
    ) -> None:
        _quarantine_one(pipeline)
        plan = plan_replay(pipeline.store)
        with pytest.raises(IntegrityIncident, match="second approver"):
            execute_replay(
                pipeline.store,
                plan,
                approved_by="ops-oncall",
                now=pipeline.clock.now(),
                require_two_approvers=True,
            )
        with pytest.raises(IntegrityIncident, match="different person"):
            execute_replay(
                pipeline.store,
                plan,
                approved_by="ops-oncall",
                second_approver="ops-oncall",
                now=pipeline.clock.now(),
                require_two_approvers=True,
            )
        outcome = execute_replay(
            pipeline.store,
            plan,
            approved_by="ops-oncall",
            second_approver="risk-lead",
            now=pipeline.clock.now(),
            require_two_approvers=True,
        )
        assert outcome.requeued == 1

    def test_a_tampered_plan_is_refused(self, pipeline: Pipeline) -> None:
        import dataclasses

        _quarantine_one(pipeline, "D-1001", "A-1")
        _quarantine_one(pipeline, "D-2001", "A-2")
        plan = plan_replay(pipeline.store)
        narrowed = dataclasses.replace(plan, event_ids=plan.event_ids[:1])
        with pytest.raises(IntegrityIncident, match="manifest hash does not match"):
            execute_replay(
                pipeline.store, narrowed, approved_by="ops-oncall", now=pipeline.clock.now()
            )

    def test_rows_that_moved_on_are_skipped_not_forced(self, pipeline: Pipeline) -> None:
        claim = _quarantine_one(pipeline)
        plan = plan_replay(pipeline.store)
        pipeline.store.requeue_quarantined(
            event_id=claim.event_id, now=pipeline.clock.now(), approved_by="someone-else"
        )
        outcome = execute_replay(
            pipeline.store, plan, approved_by="ops-oncall", now=pipeline.clock.now()
        )
        assert outcome.requeued == 0
        assert outcome.skipped == (str(claim.event_id),)
        assert outcome.notes

    def test_a_published_row_is_not_replayable_from_here(self, pipeline: Pipeline) -> None:
        # Republishing would create a duplicate for no operational reason; the
        # way to redeliver is to move the consumer's offset.
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.run_relay()
        assert plan_replay(pipeline.store).count == 0

    def test_a_requeued_event_publishes_and_applies_once(self, pipeline: Pipeline) -> None:
        claim = _quarantine_one(pipeline)
        plan = plan_replay(pipeline.store)
        execute_replay(pipeline.store, plan, approved_by="ops-oncall", now=pipeline.clock.now())
        pipeline.pump()
        assert pipeline.consumer_store.processed_event_ids(pipeline.settings.consumer_name) == {
            str(claim.event_id)
        }


class TestDrainEstimate:
    def test_a_positive_drain_rate_gives_an_eta(self) -> None:
        assert backlog_drain_estimate(pending=1000, publish_rate=200, ingest_rate=100) == 10.0

    @pytest.mark.parametrize("ingest", [200, 250])
    def test_a_non_positive_drain_rate_reports_no_convergence(self, ingest: float) -> None:
        # "Infinity" rounded to a plausible ETA is how a growing backlog gets
        # reported as a backlog that is clearing slowly.
        assert backlog_drain_estimate(pending=1000, publish_rate=200, ingest_rate=ingest) is None


class TestHealth:
    def test_a_healthy_relay_is_live_and_ready(self, pipeline: Pipeline) -> None:
        report = pipeline.health.report()
        assert report.live and report.readiness is Readiness.READY
        assert report.checks["store"] == "ok"
        assert report.checks["circuit"] == "closed"

    def test_an_open_circuit_is_not_ready_but_stays_live(self, pipeline: Pipeline) -> None:
        """A relay that reported itself dead during an outage would be
        restarted repeatedly, which is the worst available response."""
        report = pipeline.health.report(circuit_open=True)
        assert report.live is True
        assert report.readiness is Readiness.NOT_READY
        assert any("circuit is open" in reason for reason in report.reasons)

    def test_draining_is_not_ready(self, pipeline: Pipeline) -> None:
        pipeline.health.begin_drain()
        report = pipeline.health.report()
        assert report.draining and report.readiness is Readiness.NOT_READY

    def test_storage_pressure_degrades_readiness(self, tmp_path, clock, hooks) -> None:
        from outbox.pipeline import build_pipeline

        built = build_pipeline(
            root=tmp_path / "tiny",
            clock=clock,
            hooks=hooks,
            storage_budget_bytes=1 << 20,
            synchronous="NORMAL",
        )
        try:
            built.sync.open_epoch(EPOCH_A)
            # The schema alone exceeds a 1 MiB budget's fail-closed threshold.
            built.sync.insert_deals(
                EPOCH_A,
                [deal(f"D-{i:05d}", account_id=f"A-{i % 5}", source_seq=i) for i in range(1, 400)],
            )
            report = built.health.report()
            assert report.checks["storage"] in ("warn", "fail_closed")
        finally:
            built.close()


class TestMetrics:
    def test_a_successful_publish_records_an_ack_and_a_latency(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.clock.advance(2)
        pipeline.run_relay()
        metrics = pipeline.relay_metrics
        assert metrics.value("outbox_publish_attempts_total", outcome="ACK") == 1
        assert metrics.value("outbox_created_to_ack_seconds_count") == 1
        assert metrics.value("outbox_pending_total") == 0

    def test_broker_errors_are_counted_by_class(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.broker.fault = Fault.LEADER_UNAVAILABLE
        pipeline.publisher.publish_cycle()
        assert (
            pipeline.relay_metrics.value(
                "producer_errors_total", code="BROKER_LEADER_UNAVAILABLE"
            )
            == 1
        )
        assert pipeline.relay_metrics.value("outbox_publish_attempts_total", outcome="RETRY") == 1

    def test_an_ambiguous_ack_has_its_own_counter(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.broker.fault = Fault.DROP_ACK
        pipeline.publisher.publish_cycle()
        assert pipeline.relay_metrics.value("outbox_ack_ambiguous_total") == 1

    def test_consumer_duplicates_and_gaps_have_counters(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.run_relay()
        pipeline.run_consumer()
        # Replay the same record by rewinding this consumer's position.
        pipeline.kafka_consumer.subscribe([DEALS_TOPIC.name])
        for topic, partition in pipeline.kafka_consumer.assignment():
            pipeline.kafka_consumer.seek(topic, partition, 0)
        pipeline.consumer.process_once()
        assert pipeline.consumer_metrics.value("consumer_duplicates_total") == 1

    def test_metric_label_sets_stay_low_cardinality(self, pipeline: Pipeline) -> None:
        """No per-event labels anywhere (design section 11.1).

        A metrics backend that receives an ``event_id`` label degrades until the
        dashboards you need during an incident stop working.
        """
        banned = {"event_id", "deal_id", "account_id", "claim_token", "aggregate_key", "dedupe_key"}
        metrics = Metrics.build()
        for metric in metrics.registry.collect():
            for sample in metric.samples:
                assert not (banned & set(sample.labels)), f"{metric.name} has {sample.labels}"

    def test_a_negative_latency_is_clamped_not_recorded(self) -> None:
        metrics = Metrics.build()
        metrics.observe_ack_latency(-5.0)
        assert metrics.value("outbox_created_to_ack_seconds_sum") == 0.0
        assert metrics.value("outbox_created_to_ack_seconds_count") == 1


class TestLogRedaction:
    @pytest.mark.parametrize(
        "key",
        ["password", "sasl_password", "api_key", "authorization", "pg_dsn", "private_key", "token"],
    )
    def test_secret_shaped_keys_are_masked(self, key: str) -> None:
        assert redact({key: "hunter2"})[key] == "***"

    def test_event_id_is_kept_because_it_is_what_correlates_an_incident(self) -> None:
        # Permitted in sampled structured logs, banned as a metric label.
        event_id = "3947bbc2-f7d9-4133-9b6c-aadbf1eaa354"
        assert redact({"event_id": event_id})["event_id"] == event_id

    def test_a_non_scalar_value_is_reduced_to_its_type(self) -> None:
        # This is what stops a whole event payload arriving in the log as the
        # repr of a dict.
        assert redact({"payload": {"realized_pnl_gross": "12.30"}})["payload"] == "<dict>"

    def test_long_strings_are_truncated(self) -> None:
        assert redact({"reason": "x" * 1000})["reason"].endswith("...")
        assert len(redact({"reason": "x" * 1000})["reason"]) == 203

    def test_the_formatter_emits_one_json_object_per_line(self) -> None:
        stream = io.StringIO()
        configure("INFO", stream)
        get_logger("test").info("something happened", event_id="abc", password="hunter2")
        logging.getLogger("outbox").handlers[0].flush()
        payload = json.loads(stream.getvalue().strip())
        assert payload["message"] == "something happened"
        assert payload["event_id"] == "abc"
        assert payload["password"] == "***"

    def test_an_exception_is_logged_as_type_and_message_not_a_traceback(self) -> None:
        # A traceback can contain SQL with parameters interpolated by a driver.
        record = logging.LogRecord(
            name="outbox.test",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="failed",
            args=(),
            exc_info=(ValueError, ValueError("secret-ish detail"), None),
        )
        payload = json.loads(JsonFormatter().format(record))
        assert payload["error_type"] == "ValueError"
        assert "Traceback" not in json.dumps(payload)


class TestAttemptOutcomes:
    def test_a_stale_ack_is_its_own_outcome(self, pipeline: Pipeline) -> None:
        """Not "failed": the broker holds the record, we just lost the row."""
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        claim = pipeline.store.claim_due(
            owner="a", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )[0]
        pipeline.store.record_attempt(
            claim=claim,
            outcome=AttemptOutcome.STALE_ACK,
            error_code=str(ErrorCode.INTEGRITY_CONFLICT),
            now=pipeline.clock.now(),
        )
        assert [row["outcome"] for row in pipeline.store.attempts(claim.event_id)] == ["STALE_ACK"]
