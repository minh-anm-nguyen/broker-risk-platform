"""Metrics cardinality and log redaction -- asserted, not merely intended."""

from __future__ import annotations

import io
import json
import logging

import pytest

from trading_sync.infrastructure.logging import (
    REDACTED,
    configure_logging,
    hash_source_key,
    redact,
    sanitize,
)
from trading_sync.infrastructure.metrics import FORBIDDEN_LABELS, SyncMetrics

pytestmark = pytest.mark.unit


@pytest.mark.whatif("WF3-198", "WF3-204")
def test_no_metric_carries_a_high_cardinality_label() -> None:
    """One time series per deal would break the metrics store, and account ids
    as labels would publish customer identifiers into a system with none of
    the database's access controls (design 12.1).
    """
    metrics = SyncMetrics()
    offenders: list[tuple[str, str]] = []
    for collector in list(metrics.registry._collector_to_names):  # noqa: SLF001
        for label in getattr(collector, "_labelnames", ()):
            if label in FORBIDDEN_LABELS:
                offenders.append((getattr(collector, "_name", "?"), label))
    assert offenders == []


@pytest.mark.whatif("WF3-195")
def test_watermark_metric_is_documented_as_an_event_sequence() -> None:
    """The gauge counts source events, not pending deals.

    A distance of 1000 with one deal outstanding is normal, because non-deal
    events consume sequence numbers. Presenting it as "1000 deals missing"
    would turn a healthy system into a false alarm.
    """
    metrics = SyncMetrics()
    doc = metrics.checkpoint_source_seq._documentation  # noqa: SLF001
    assert "not a deal count" in doc


@pytest.mark.whatif("WF3-185")
@pytest.mark.parametrize(
    "line",
    [
        "connect failed: postgresql+asyncpg://postgres:hunter2@db:5432/sync",
        "Authorization: Bearer eyJhbGciOiJIUzI1",
        'config {"api_key": "sk-live-1234567"}',
        "password=p@ssw0rd!",
    ],
)
def test_credentials_are_redacted_from_arbitrary_strings(line: str) -> None:
    """Redaction sits in the formatter because the line that leaks a secret is
    usually an exception string nobody wrote by hand.
    """
    cleaned = redact(line)
    assert REDACTED in cleaned
    for secret in ("hunter2", "eyJhbGciOiJIUzI1", "sk-live-1234567", "p@ssw0rd!"):
        assert secret not in cleaned


def test_ordinary_text_is_left_alone() -> None:
    assert redact("scan published, watermark 1200") == "scan published, watermark 1200"


@pytest.mark.whatif("WF3-185")
def test_formatter_redacts_a_dsn_inside_an_exception() -> None:
    stream = io.StringIO()
    configure_logging(service="trading-data-sync", stream=stream)
    logger = logging.getLogger("trading_sync.test")
    try:
        raise RuntimeError("could not connect to postgresql://u:topsecret@h/db")
    except RuntimeError:
        logger.exception("database unreachable")
    payload = json.loads(stream.getvalue().strip().splitlines()[-1])
    assert "topsecret" not in json.dumps(payload)
    assert payload["service"] == "trading-data-sync"


@pytest.mark.whatif("WF3-190")
def test_sanitize_drops_sensitive_keys_entirely() -> None:
    """Quarantine and audit rows carry only what is safe to keep forever."""
    cleaned = sanitize(
        {"reason_code": "IMMUTABLE_DEAL_CONFLICT", "payload": {"commission": "-2.50"}, "token": "abc"}
    )
    assert cleaned["payload"] == REDACTED
    assert cleaned["token"] == REDACTED
    assert cleaned["reason_code"] == "IMMUTABLE_DEAL_CONFLICT"


@pytest.mark.whatif("WF3-198")
def test_source_key_hash_is_stable_and_not_reversible_by_inspection() -> None:
    """Lets two reports of one conflicting deal be correlated without storing
    the id (design 11.6)."""
    assert hash_source_key("D1") == hash_source_key("D1")
    assert hash_source_key("D1") != hash_source_key("D2")
    assert "D1" not in hash_source_key("D1")
