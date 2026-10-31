"""Structured logging with redaction on by default.

Section 8.3 puts the high-cardinality context - Deal ids, account ids,
payloads - in logs rather than metrics, which only helps if those logs are not
themselves a privacy incident (section 9, P08-AJ). So account and Deal
identifiers are hashed to short stable tokens by default: an operator can
still correlate two lines, without the log holding the identifier.

``redact`` is applied at the logging boundary rather than trusted to each call
site, because the one call site that forgets is the one that leaks.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any, Mapping

#: Fields hashed rather than printed when redaction is on.
SENSITIVE_FIELDS: frozenset[str] = frozenset(
    {"account_id", "deal_id", "payload", "payload_json", "token", "key", "cursor"}
)

#: Fields dropped entirely; a token is never useful in a log, even hashed.
DROPPED_FIELDS: frozenset[str] = frozenset({"token", "reader_token", "operator_token", "password"})


def _short_hash(value: object) -> str:
    raw = str(value).encode("utf-8")
    return "h:" + hashlib.sha256(raw).hexdigest()[:12]


def redact(payload: Mapping[str, Any], *, enabled: bool = True) -> dict[str, Any]:
    """Return a copy safe to write to a log sink."""
    if not enabled:
        return dict(payload)
    out: dict[str, Any] = {}
    for key, value in payload.items():
        if key in DROPPED_FIELDS:
            continue
        if key in SENSITIVE_FIELDS:
            out[key] = _short_hash(value)
        elif isinstance(value, Mapping):
            out[key] = redact(value, enabled=enabled)
        else:
            out[key] = value
    return out


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        extra = getattr(record, "context", None)
        if isinstance(extra, Mapping):
            payload["context"] = extra
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, sort_keys=True, default=str)


def configure_logging(level: str = "INFO", *, stream=None) -> None:
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(_JsonFormatter())
    root = logging.getLogger("analytics")
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    root.propagate = False


class StructuredLogger:
    """Thin wrapper that redacts context before it reaches a handler."""

    __slots__ = ("_logger", "_redact")

    def __init__(self, name: str, *, redact_pii: bool = True) -> None:
        self._logger = logging.getLogger(f"analytics.{name}")
        self._redact = redact_pii

    def _emit(self, level: int, message: str, context: Mapping[str, Any]) -> None:
        self._logger.log(level, message, extra={"context": redact(context, enabled=self._redact)})

    def debug(self, message: str, **context: Any) -> None:
        self._emit(logging.DEBUG, message, context)

    def info(self, message: str, **context: Any) -> None:
        self._emit(logging.INFO, message, context)

    def warning(self, message: str, **context: Any) -> None:
        self._emit(logging.WARNING, message, context)

    def error(self, message: str, **context: Any) -> None:
        self._emit(logging.ERROR, message, context)


def structured_logger(name: str, *, redact_pii: bool = True) -> StructuredLogger:
    return StructuredLogger(name, redact_pii=redact_pii)
