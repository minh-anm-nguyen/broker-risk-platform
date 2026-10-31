"""Signed keyset cursors for position listing.

Design doc section 5.3. The properties that matter:

* **Keyset, not OFFSET.** Ordering is on ``position_id``, so a changing table
  cannot shift rows under a page.
* **Scan fenced.** The cursor carries the published scan it belongs to. If a
  newer scan is published, the next page is refused with ``409 SCAN_CHANGED``
  rather than stitching rows from two different scans.
* **Signed, and not a secret store.** HMAC proves integrity; it does *not*
  encrypt. So the payload holds a scope *hash*, never raw account ids.
* **Scope bound.** A cursor minted for one principal, tenant, source, epoch,
  filter set or authorization revision is rejected for any other.

Full enumeration across arbitrary upstream change would need immutable
retained snapshots in Project 03. P0 makes no such promise and fails loudly
instead.
"""

from __future__ import annotations

import base64
import binascii
import hmac
import json
from dataclasses import dataclass
from hashlib import sha256
from typing import Final

from ..authz.principal_context import AuthorizedScope, scope_hash
from ..contracts.problem_details import ApiProblem

__all__ = ["CursorPayload", "CursorCodec", "filter_hash"]

CURSOR_VERSION: Final = 1
_MAX_CURSOR_CHARS: Final = 1024


def filter_hash(*, symbol: str | None, accounts: tuple[str, ...], page_size: int) -> str:
    """Hash of everything that must not change across a paginated session."""
    return scope_hash("filters", symbol or "", page_size, *accounts)


@dataclass(frozen=True, slots=True)
class CursorPayload:
    """What a cursor pins. Every field is checked on the next page."""

    version: int
    tenant_scope_hash: str
    auth_scope_revision: str
    source_id: str
    epoch_id: str
    published_scan_id: str
    last_position_id: str
    filter_hash: str
    expires_at: int
    key_id: str

    def to_json(self) -> dict[str, object]:
        return {
            "v": self.version,
            "tsh": self.tenant_scope_hash,
            "asr": self.auth_scope_revision,
            "src": self.source_id,
            "epo": self.epoch_id,
            "scan": self.published_scan_id,
            "last": self.last_position_id,
            "fh": self.filter_hash,
            "exp": self.expires_at,
            "kid": self.key_id,
        }

    @classmethod
    def from_json(cls, raw: dict[str, object]) -> "CursorPayload":
        try:
            return cls(
                version=int(raw["v"]),  # type: ignore[arg-type]
                tenant_scope_hash=str(raw["tsh"]),
                auth_scope_revision=str(raw["asr"]),
                source_id=str(raw["src"]),
                epoch_id=str(raw["epo"]),
                published_scan_id=str(raw["scan"]),
                last_position_id=str(raw["last"]),
                filter_hash=str(raw["fh"]),
                expires_at=int(raw["exp"]),  # type: ignore[arg-type]
                key_id=str(raw["kid"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ApiProblem("INVALID_CURSOR", "the pagination cursor is malformed") from exc


class CursorCodec:
    """Encode, decode and authorize cursors."""

    def __init__(self, *, signing_key: str, key_id: str, ttl_seconds: int) -> None:
        if not signing_key:
            raise ValueError("a cursor signing key is required")
        self._keys = {key_id: signing_key}
        self._key_id = key_id
        self._ttl = ttl_seconds

    def add_key(self, key_id: str, signing_key: str) -> None:
        """Register an additional verification key, for rotation windows."""
        self._keys[key_id] = signing_key

    def retire_key(self, key_id: str) -> None:
        self._keys.pop(key_id, None)

    # ---------------------------------------------------------------- encode

    def encode(
        self,
        *,
        scope: AuthorizedScope,
        published_scan_id: str,
        last_position_id: str,
        filters_hash: str,
        now: int,
    ) -> str:
        payload = CursorPayload(
            version=CURSOR_VERSION,
            tenant_scope_hash=scope.tenant_scope_hash,
            auth_scope_revision=scope.auth_scope_revision,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            published_scan_id=published_scan_id,
            last_position_id=last_position_id,
            filter_hash=filters_hash,
            expires_at=now + self._ttl,
            key_id=self._key_id,
        )
        body = json.dumps(payload.to_json(), sort_keys=True, separators=(",", ":")).encode("utf-8")
        signature = self._sign(body, self._keys[self._key_id])
        return f"{_b64(body)}.{_b64(signature)}"

    # ---------------------------------------------------------------- decode

    def decode(self, cursor: str) -> CursorPayload:
        """Verify integrity and shape. No scope decisions are made here."""
        if not isinstance(cursor, str) or not cursor:
            raise ApiProblem("INVALID_CURSOR", "the pagination cursor is empty")
        if len(cursor) > _MAX_CURSOR_CHARS:
            raise ApiProblem("INVALID_CURSOR", "the pagination cursor is too long")
        parts = cursor.split(".")
        if len(parts) != 2:
            raise ApiProblem("INVALID_CURSOR", "the pagination cursor is malformed")
        try:
            body = _unb64(parts[0])
            signature = _unb64(parts[1])
        except (binascii.Error, ValueError) as exc:
            raise ApiProblem("INVALID_CURSOR", "the pagination cursor is not decodable") from exc

        try:
            raw = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ApiProblem("INVALID_CURSOR", "the pagination cursor is not valid JSON") from exc
        if not isinstance(raw, dict):
            raise ApiProblem("INVALID_CURSOR", "the pagination cursor is not an object")

        key_id = raw.get("kid")
        if not isinstance(key_id, str):
            raise ApiProblem("INVALID_CURSOR", "the pagination cursor names no signing key")
        secret = self._keys.get(key_id)
        if secret is None:
            raise ApiProblem("INVALID_CURSOR", "the pagination cursor was signed by an unknown key")
        if not hmac.compare_digest(self._sign(body, secret), signature):
            # Tampering is rejected before any query is built.
            raise ApiProblem("INVALID_CURSOR", "the pagination cursor signature is invalid")

        payload = CursorPayload.from_json(raw)
        if payload.version != CURSOR_VERSION:
            raise ApiProblem("INVALID_CURSOR", "the pagination cursor version is not supported")
        return payload

    # ------------------------------------------------------------- authorize

    def authorize(
        self,
        payload: CursorPayload,
        *,
        scope: AuthorizedScope,
        filters_hash: str,
        now: int,
    ) -> None:
        """Bind a decoded cursor to the caller presenting it."""
        if payload.expires_at <= now:
            raise ApiProblem(
                "CURSOR_EXPIRED",
                "the pagination cursor expired; start a new scan",
                reason_codes=("CURSOR_EXPIRED",),
            )
        if not hmac.compare_digest(payload.tenant_scope_hash, scope.tenant_scope_hash):
            # A cursor from another principal or tenant is simply not found.
            raise ApiProblem(
                "RESOURCE_NOT_FOUND",
                "the requested resource does not exist or is not visible to this principal",
                reason_codes=("CURSOR_OUT_OF_SCOPE",),
            )
        if payload.source_id != scope.source_id or payload.epoch_id != scope.epoch_id:
            raise ApiProblem(
                "EPOCH_CHANGED",
                "the pagination cursor belongs to a different source epoch",
                reason_codes=("EPOCH_CHANGED",),
            )
        if payload.auth_scope_revision != scope.auth_scope_revision:
            # Authorization changed mid-session: the next page is denied.
            raise ApiProblem(
                "ACCESS_DENIED",
                "the authorization scope changed; restart the paginated session",
                reason_codes=("AUTH_REVISION_CHANGED",),
            )
        if not hmac.compare_digest(payload.filter_hash, filters_hash):
            raise ApiProblem(
                "INVALID_CURSOR",
                "the pagination cursor does not match the supplied filters",
            )

    @staticmethod
    def assert_same_scan(payload: CursorPayload, published_scan_id: str) -> None:
        """Refuse to continue a page set across a republished scan."""
        if payload.published_scan_id != published_scan_id:
            raise ApiProblem(
                "SCAN_CHANGED",
                "the published position scan changed; restart pagination",
                reason_codes=("SCAN_CHANGED",),
            )

    # -------------------------------------------------------------- internals

    @staticmethod
    def _sign(body: bytes, secret: str) -> bytes:
        return hmac.new(secret.encode("utf-8"), body, sha256).digest()


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _unb64(segment: str) -> bytes:
    if "=" in segment or "+" in segment or "/" in segment:
        raise ValueError("not base64url without padding")
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))
