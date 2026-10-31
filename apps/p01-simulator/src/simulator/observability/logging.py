"""Structured logging with mandatory secret redaction.

Two requirements drive this module:

* Every line carries the correlation fields that make an incident traceable:
  ``request_id``, ``run_id``, ``generation``, ``seq`` where known (section 11.2).
* No line ever contains a bearer token, an ``Authorization`` header or a
  signing key, including when a token was mistakenly passed in a query string
  (WF-107). Redaction is applied by the formatter, not by call sites, because a
  call site that forgets is exactly the failure being guarded against.
"""

from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

REDACTED = "[redacted]"

#: Field names whose values are replaced wholesale. Matched case-insensitively
#: against both log-record attributes and nested mapping keys.
_SENSITIVE_KEYS = frozenset(
    {
        "authorization",
        "token",
        "data_token",
        "admin_token",
        "bearer",
        "secret",
        "password",
        "signing_key",
        "cursor_signing_key",
        "api_key",
        "apikey",
        "set-cookie",
        "cookie",
    }
)

#: Reserved ``LogRecord`` attributes, excluded when harvesting structured extras.
_RESERVED = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "taskName",
        "thread",
        "threadName",
    }
)

_request_id: ContextVar[str | None] = ContextVar("request_id", default=None)
_run_id: ContextVar[str | None] = ContextVar("run_id", default=None)


def bind_request_id(value: str | None) -> None:
    _request_id.set(value)


def bind_run_id(value: str | None) -> None:
    _run_id.set(value)


def current_request_id() -> str | None:
    return _request_id.get()


def redact(value: Any, *, depth: int = 0) -> Any:
    """Recursively replace sensitive values, bounded in depth.

    The depth bound matters: a hostile or merely cyclic payload must not turn a
    log call into unbounded recursion (WF-110).
    """
    if depth > 6:
        return "[truncated]"
    if isinstance(value, dict):
        return {
            key: (REDACTED if str(key).lower() in _SENSITIVE_KEYS else redact(item, depth=depth + 1))
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact(item, depth=depth + 1) for item in value[:50]]
    if isinstance(value, str) and value.lower().startswith("bearer "):
        return REDACTED
    return value


class JsonFormatter(logging.Formatter):
    """Renders one JSON object per line, UTC timestamps, secrets removed."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC)
            .isoformat()
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "component": record.name,
            "message": record.getMessage(),
        }
        request_id = _request_id.get()
        if request_id:
            payload["request_id"] = request_id
        run_id = _run_id.get()
        if run_id:
            payload["run_id"] = run_id
        for key, value in record.__dict__.items():
            if key in _RESERVED or key.startswith("_"):
                continue
            payload[key] = (
                REDACTED if key.lower() in _SENSITIVE_KEYS else redact(value)
            )
        if record.exc_info:
            # The message and type, not the full traceback: a traceback in a
            # structured log line is unreadable and can echo request data.
            exc_type, exc_value, _ = record.exc_info
            payload["error_type"] = getattr(exc_type, "__name__", str(exc_type))
            payload["error_message"] = str(exc_value)
        try:
            return json.dumps(payload, ensure_ascii=False, default=str)
        except (TypeError, ValueError):  # pragma: no cover - default=str covers most
            return json.dumps(
                {"level": record.levelname, "component": record.name, "message": record.getMessage()}
            )


class TextFormatter(logging.Formatter):
    """Human-readable fallback for local debugging."""

    def __init__(self) -> None:
        super().__init__("%(asctime)s %(levelname)-7s %(name)s | %(message)s")

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        request_id = _request_id.get()
        return f"{base} [req={request_id}]" if request_id else base


def configure_logging(level: str = "INFO", fmt: str = "json") -> None:
    """Install a single stderr handler. Idempotent across app factory calls."""
    root = logging.getLogger("simulator")
    root.setLevel(level)
    root.propagate = False
    for handler in list(root.handlers):
        root.removeHandler(handler)
    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(JsonFormatter() if fmt == "json" else TextFormatter())
    root.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"simulator.{name}")
