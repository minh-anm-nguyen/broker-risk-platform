"""Signed pagination cursors bound to one build and one filter.

A cursor carries the tenant, the build, a digest of the filter and the
position. All four are inside the signed payload, so:

* a cursor from tenant A cannot be replayed against tenant B;
* a cursor cannot survive a pointer flip into a different build, which is what
  would otherwise let one paginated response mix two builds (P08-T-02);
* changing the date range mid-pagination invalidates the cursor instead of
  silently re-slicing a different result set.

The signing key must outlive the process. Regenerating it at startup would
invalidate every outstanding cursor on deploy, so :class:`CursorCodec`
refuses to invent one.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any, Mapping

from ..errors import ConfigurationError, QueryError, Reason


@dataclass(frozen=True, slots=True)
class CursorPayload:
    """Opaque-to-clients pagination state."""

    tenant_id: str
    build_id: str
    filter_digest: str
    #: Last key returned, as a list so it survives a JSON round trip.
    last_key: tuple[str, ...]
    page: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "t": self.tenant_id,
            "b": self.build_id,
            "f": self.filter_digest,
            "k": list(self.last_key),
            "p": self.page,
        }


def filter_digest(parts: Mapping[str, Any]) -> str:
    """Digest of the query filter, with ``None`` values dropped.

    Dropping them means an omitted parameter and an explicitly null one
    produce the same digest, which is what a client sending either form
    reasonably expects.
    """
    cleaned = {
        key: (sorted(value) if isinstance(value, (set, frozenset)) else value)
        for key, value in sorted(parts.items())
        if value is not None
    }
    raw = json.dumps(cleaned, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


class CursorCodec:
    """Encodes and verifies cursors with HMAC-SHA256."""

    def __init__(self, *, signing_key: str, key_id: str = "k1", previous_key: str = "") -> None:
        if not signing_key:
            raise ConfigurationError(
                "cursor_signing_key must be supplied; a per-process key would "
                "invalidate every outstanding cursor on restart"
            )
        self._key = signing_key.encode("utf-8")
        self._key_id = key_id
        # A previous key stays valid for verification during rotation, so an
        # in-flight pagination is not broken by a key change.
        self._previous = previous_key.encode("utf-8") if previous_key else None

    def encode(self, payload: CursorPayload) -> str:
        body = json.dumps(payload.as_dict(), sort_keys=True, separators=(",", ":")).encode("utf-8")
        signature = hmac.new(self._key, body, hashlib.sha256).digest()[:16]
        return "{}.{}.{}".format(
            self._key_id, _b64(body), _b64(signature)
        )

    def decode(
        self,
        cursor: str,
        *,
        expected_tenant: str,
        expected_build: str,
        expected_filter: str,
    ) -> CursorPayload:
        """Verify and bind a cursor, or refuse with a specific reason."""
        try:
            key_id, body_b64, signature_b64 = cursor.split(".")
            body = _unb64(body_b64)
            signature = _unb64(signature_b64)
        except Exception as exc:
            raise QueryError(Reason.CURSOR_INVALID, "cursor is malformed") from exc

        if not self._verify(body, signature):
            raise QueryError(Reason.CURSOR_INVALID, "cursor signature does not verify")

        try:
            data = json.loads(body.decode("utf-8"))
            payload = CursorPayload(
                tenant_id=str(data["t"]),
                build_id=str(data["b"]),
                filter_digest=str(data["f"]),
                last_key=tuple(str(part) for part in data["k"]),
                page=int(data["p"]),
            )
        except Exception as exc:
            raise QueryError(Reason.CURSOR_INVALID, "cursor payload is unreadable") from exc

        if payload.tenant_id != expected_tenant:
            # A valid signature on another tenant's cursor is still a scope
            # violation, so this is reported as such rather than as a bad
            # cursor.
            raise QueryError(
                Reason.FORBIDDEN_SCOPE, "cursor belongs to a different tenant"
            )
        if payload.build_id != expected_build:
            raise QueryError(
                Reason.CURSOR_BUILD_MISMATCH,
                "cursor was issued for a different build; restart pagination",
                cursor_build=payload.build_id,
                current_build=expected_build,
            )
        if payload.filter_digest != expected_filter:
            raise QueryError(
                Reason.CURSOR_INVALID, "cursor was issued for a different filter"
            )
        del key_id
        return payload

    def _verify(self, body: bytes, signature: bytes) -> bool:
        for key in filter(None, (self._key, self._previous)):
            expected = hmac.new(key, body, hashlib.sha256).digest()[:16]
            if hmac.compare_digest(expected, signature):
                return True
        return False


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _unb64(text: str) -> bytes:
    padding = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + padding)
