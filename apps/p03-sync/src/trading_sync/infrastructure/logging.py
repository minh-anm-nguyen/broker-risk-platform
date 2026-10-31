"""Structured JSON logging with redaction at the boundary.

Redaction happens in the formatter, not at each call site. Call-site redaction
only protects the lines somebody remembered to protect, and the line that leaks
a credential is usually an exception string nobody wrote by hand -- a
connection error carrying the full DSN, for instance (WF3-185).

Raw source payloads are never logged. ``telemetry.payload_logging`` exists so
the intent is explicit and auditable, and it defaults to off in every
environment; investigating a payload is a secure-evidence-store question, not
a logging one (design 11.6, 12.1).
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import sys
from typing import Any, Iterable, Mapping

__all__ = [
    "REDACTED",
    "JsonFormatter",
    "configure_logging",
    "get_logger",
    "hash_source_key",
    "redact",
]

REDACTED = "***REDACTED***"

#: Credentials embedded in a URL, e.g. postgresql+asyncpg://user:secret@host/db
_DSN_CREDENTIALS = re.compile(r"(?P<scheme>[a-zA-Z0-9+.-]+://)(?P<user>[^:/@\s]+):(?P<secret>[^@\s]+)@")

#: ``Bearer <token>`` puts the secret after a word rather than after a
#: separator, so the generic key=value pattern below would mask only the word
#: "Bearer" and leave the token itself in the log line.
_BEARER_TOKEN = re.compile(r"(?i)\bbearer\s+(?P<value>[A-Za-z0-9._~+/=-]{4,})")

#: ``key=value`` and ``"key": "value"`` forms for obviously sensitive keys.
#: The optional quote after the key name matters: in JSON the closing quote
#: sits between the key and the colon, so a pattern without it misses every
#: secret that arrives as serialized config.
_SENSITIVE_KEY = re.compile(
    r"(?i)\b(password|passwd|secret|token|api[_-]?key|authorization)\b[\"']?"
    r"\s*[:=]\s*(?P<quote>[\"']?)(?P<value>[^\s,;\"'}]+)(?P=quote)"
)

_SENSITIVE_FIELD_NAMES = frozenset(
    {
        "password",
        "passwd",
        "secret",
        "token",
        "api_key",
        "apikey",
        "authorization",
        "dsn",
        "database_url",
        "admin_token",
        "payload",
        "raw_payload",
    }
)

_RESERVED_RECORD_KEYS = frozenset(
    {
        "args", "asctime", "created", "exc_info", "exc_text", "filename", "funcName",
        "levelname", "levelno", "lineno", "module", "msecs", "message", "msg", "name",
        "pathname", "process", "processName", "relativeCreated", "stack_info",
        "thread", "threadName", "taskName",
    }
)


def redact(value: str) -> str:
    """Mask credentials inside an arbitrary string."""
    masked = _DSN_CREDENTIALS.sub(
        lambda m: f"{m.group('scheme')}{m.group('user')}:{REDACTED}@", value
    )
    masked = _BEARER_TOKEN.sub(
        lambda m: m.group(0).replace(m.group("value"), REDACTED), masked
    )
    return _SENSITIVE_KEY.sub(
        lambda m: m.group(0).replace(m.group("value"), REDACTED), masked
    )


def hash_source_key(key: str, *, length: int = 16) -> str:
    """Return a short stable digest, for referring to a record without storing it.

    Lets two reports of the same conflicting deal be correlated while keeping
    the id itself out of the quarantine table and the logs (design 11.6).
    """
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:length]


def sanitize(details: Mapping[str, Any]) -> dict[str, Any]:
    """Drop sensitive keys and redact string values in a details mapping."""
    clean: dict[str, Any] = {}
    for key, value in details.items():
        if key.lower() in _SENSITIVE_FIELD_NAMES:
            clean[key] = REDACTED
        elif isinstance(value, str):
            clean[key] = redact(value)
        elif isinstance(value, Mapping):
            clean[key] = sanitize(value)
        elif isinstance(value, (list, tuple)):
            clean[key] = [redact(v) if isinstance(v, str) else v for v in value]
        else:
            clean[key] = value
    return clean


class JsonFormatter(logging.Formatter):
    """One JSON object per line, with structured extras preserved."""

    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "level": record.levelname,
            "service": self.service,
            "logger": record.name,
            "message": redact(record.getMessage()),
        }
        for key, value in record.__dict__.items():
            if key in _RESERVED_RECORD_KEYS or key.startswith("_"):
                continue
            payload[key] = value
        payload = sanitize(payload)
        if record.exc_info:
            payload["exception"] = redact(self.formatException(record.exc_info))
        return json.dumps(payload, default=str, ensure_ascii=False)


def configure_logging(*, service: str, level: str = "INFO", stream: Any = None) -> None:
    handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
    handler.setFormatter(JsonFormatter(service))
    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level.upper())


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
