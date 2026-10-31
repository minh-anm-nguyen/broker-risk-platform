"""Structured logging with redaction built in.

The redactor is not advice, it is the only way to build a log record here.
``safe_event`` drops any field whose name looks sensitive and refuses raw
values that look like a bearer token or a money amount, so a careless
``logger.info(..., extra={"token": raw})`` cannot leak.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from typing import Any, Final, Mapping

__all__ = ["configure_logging", "safe_event", "RedactingJsonFormatter", "REDACTED"]

REDACTED: Final = "[redacted]"

# Field names that must never carry a value into a log line.
_FORBIDDEN_KEYS: Final = frozenset(
    {
        "authorization",
        "token",
        "access_token",
        "bearer",
        "jwt",
        "secret",
        "password",
        "signing_key",
        "cursor_signing_key",
        "account_id",
        "account_ids",
        "accounts",
        "amount",
        "gross",
        "gross_usd",
        "commission",
        "commission_usd",
        "swap",
        "swap_usd",
        "net",
        "net_usd",
        "net_excluding_fee_usd",
        "composite_total",
        "balance",
        "equity",
        "by_symbol",
        "items",
    }
)

_BEARER_RE: Final = re.compile(r"(?i)bearer\s+[A-Za-z0-9._-]{8,}")
_JWT_RE: Final = re.compile(r"[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}")
_MONEY_RE: Final = re.compile(r"^-?\d+\.\d{2,}$")


def _scrub(value: Any, *, depth: int = 0) -> Any:
    if depth > 6:
        return REDACTED
    if isinstance(value, str):
        if _BEARER_RE.search(value) or _JWT_RE.search(value):
            return REDACTED
        if _MONEY_RE.fullmatch(value):
            return REDACTED
        return value if len(value) <= 512 else value[:512] + "..."
    if isinstance(value, bool) or value is None or isinstance(value, int):
        return value
    if isinstance(value, float):
        return REDACTED
    if isinstance(value, Mapping):
        return {
            key: (REDACTED if str(key).lower() in _FORBIDDEN_KEYS else _scrub(item, depth=depth + 1))
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [_scrub(item, depth=depth + 1) for item in list(value)[:32]]
    return REDACTED


def safe_event(event: str, **fields: Any) -> dict[str, Any]:
    """Build a log payload with every sensitive field removed."""
    payload: dict[str, Any] = {"event": event}
    for key, value in fields.items():
        if key.lower() in _FORBIDDEN_KEYS:
            payload[key] = REDACTED
            continue
        payload[key] = _scrub(value)
    return payload


class RedactingJsonFormatter(logging.Formatter):
    """Emit one JSON object per record, scrubbed."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        extra = getattr(record, "event_payload", None)
        if isinstance(extra, Mapping):
            payload.update(_scrub(dict(extra)))
        if record.exc_info:
            # Exception type only. A stack trace is internal detail.
            payload["exception_type"] = record.exc_info[0].__name__ if record.exc_info[0] else None
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def configure_logging(*, level: str = "INFO", stream: Any | None = None) -> logging.Logger:
    """Install the redacting handler on this service's logger."""
    logger = logging.getLogger("risk_api")
    logger.setLevel(level.upper())
    logger.handlers.clear()
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(RedactingJsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def log_event(logger: logging.Logger, level: int, event: str, **fields: Any) -> None:
    """Log a structured event through the redactor."""
    logger.log(level, event, extra={"event_payload": safe_event(event, **fields)})
