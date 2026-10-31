"""Structured logs that cannot carry a payload or a secret out with them.

Design section 10 bans credentials, raw account details and unsanitised
exceptions from logs and DLQ records. A redaction pass that each call site has
to remember is a redaction pass that eventually gets forgotten, so this module
filters at the only place that formats a log line:

* keys that look like a secret are replaced with ``***`` by name, and
* any value that is not a small scalar is dropped rather than stringified,
  which is what stops a whole event payload from arriving in the log as the
  repr of a dict.

``event_id`` is permitted. It is an opaque derived identifier, it is what
correlates a log line with an outbox row during an incident, and design section
11.1 explicitly allows it in sampled structured logs while banning it as a
metric label.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

#: Substring match, lowercase. Deliberately broad: a false redaction costs a
#: debugging session, a missed one costs a credential.
_SENSITIVE_HINTS = (
    "password",
    "passwd",
    "secret",
    "token",
    "credential",
    "authorization",
    "api_key",
    "apikey",
    "sasl",
    "dsn",
    "private",
)

#: Allowed through even though they contain a banned substring: these are
#: fencing identifiers, not credentials, and they are what makes a takeover
#: incident readable.
_SENSITIVE_EXEMPT = ("claim_token_present", "lease_token_present")

#: Longest free-text value kept in a log line.
_MAX_VALUE_LENGTH = 200


def redact(fields: dict[str, Any]) -> dict[str, Any]:
    """Drop or mask anything that should not be in a log line."""
    clean: dict[str, Any] = {}
    for key, value in fields.items():
        lowered = key.lower()
        if lowered not in _SENSITIVE_EXEMPT and any(hint in lowered for hint in _SENSITIVE_HINTS):
            clean[key] = "***"
            continue
        if value is None or isinstance(value, (bool, int, float)):
            clean[key] = value
            continue
        if isinstance(value, str):
            clean[key] = value if len(value) <= _MAX_VALUE_LENGTH else value[:_MAX_VALUE_LENGTH] + "..."
            continue
        # Not a scalar: name the type, never the contents. This is the rule that
        # keeps a payload dict or an exception's args out of the log.
        clean[key] = f"<{type(value).__name__}>"
    return clean


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        extra = getattr(record, "fields", None)
        if isinstance(extra, dict):
            payload.update(redact(extra))
        if record.exc_info:
            # Type and message only. A traceback can contain SQL with
            # parameters interpolated by a driver's error formatting.
            exc_type, exc_value, _ = record.exc_info
            payload["error_type"] = getattr(exc_type, "__name__", str(exc_type))
            payload["error"] = str(exc_value)[:_MAX_VALUE_LENGTH]
        return json.dumps(payload, sort_keys=True, ensure_ascii=False)


def configure(level: str = "INFO", stream: Any = None) -> None:
    """Install the JSON formatter on the package logger."""
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("outbox")
    logger.handlers = [handler]
    logger.setLevel(level.upper())
    logger.propagate = False


def get_logger(name: str) -> StructuredLogger:
    return StructuredLogger(logging.getLogger(f"outbox.{name}"))


class StructuredLogger:
    """Thin wrapper so call sites pass fields as keywords, not f-strings."""

    __slots__ = ("_logger",)

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def _log(self, level: int, message: str, exc_info: bool = False, **fields: Any) -> None:
        self._logger.log(level, message, extra={"fields": fields}, exc_info=exc_info)

    def debug(self, message: str, **fields: Any) -> None:
        self._log(logging.DEBUG, message, **fields)

    def info(self, message: str, **fields: Any) -> None:
        self._log(logging.INFO, message, **fields)

    def warning(self, message: str, **fields: Any) -> None:
        self._log(logging.WARNING, message, **fields)

    def error(self, message: str, exc_info: bool = False, **fields: Any) -> None:
        self._log(logging.ERROR, message, exc_info=exc_info, **fields)
