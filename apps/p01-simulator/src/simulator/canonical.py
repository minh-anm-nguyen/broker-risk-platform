"""Canonical serialization, timestamps and content digests.

The canonical digest answers exactly one question: *did two runs with the same
seed, scenario spec and generator version produce the same data?* (WF-001,
INV-09.)

To answer that honestly the digest must exclude everything that legitimately
differs between two runs - ``run_id``, ``generation``, wall-clock
``created_at``, job ids - and include everything that must not: entity ids,
amounts, event types, event order and virtual time. A digest that accidentally
includes a wall-clock field never matches and is therefore useless; one that
accidentally excludes an amount always matches and is worse than useless.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

#: Bumped when the canonical encoding itself changes. A digest is only
#: comparable against another digest with the same canonical version.
#:
#: v2: run digests fold in stored canonical strings verbatim instead of
#: re-serializing parsed values. See :meth:`StreamingDigest.update_text`.
CANONICAL_VERSION = "2"

#: Record separators. Using explicit control bytes rather than, say, a comma
#: means a value containing the separator cannot shift the framing.
_FIELD_SEP = b"\x1f"
_RECORD_SEP = b"\x1e"


def format_timestamp(value: datetime) -> str:
    """RFC 3339 in UTC with a trailing ``Z``.

    A naive datetime is treated as UTC rather than rejected, because virtual
    clock arithmetic inside the generator is UTC by construction; mixing in a
    local offset here would be the bug worth catching, and it cannot happen
    without a tzinfo.
    """
    if value.tzinfo is None:
        aware = value.replace(tzinfo=UTC)
    else:
        aware = value.astimezone(UTC)
    return aware.isoformat().replace("+00:00", "Z")


_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def to_epoch_ms(value: datetime) -> int:
    """Milliseconds since the Unix epoch, computed in integer arithmetic.

    Range filters must never compare RFC 3339 *strings*: ``...:01.500000Z``
    sorts before ``...:01Z`` lexicographically because ``.`` precedes ``Z``, so
    a sub-second timestamp would land on the wrong side of a boundary. Storing
    an integer alongside the display string keeps the documented wire format
    and still gives exact ordering.
    """
    if value.tzinfo is None:
        aware = value.replace(tzinfo=UTC)
    else:
        aware = value.astimezone(UTC)
    delta = aware - _EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000


def parse_timestamp(text: str) -> datetime:
    """Parse an RFC 3339 timestamp, normalizing to aware UTC."""
    candidate = text.strip()
    if candidate.endswith(("Z", "z")):
        candidate = candidate[:-1] + "+00:00"
    parsed = datetime.fromisoformat(candidate)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        if obj.is_nan() or obj.is_infinite():
            raise ValueError("refusing to serialize a non-finite Decimal")
        return format(obj + Decimal(0) if obj == 0 else obj, "f")
    if isinstance(obj, datetime):
        return format_timestamp(obj)
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, (set, frozenset)):
        return sorted(obj)
    raise TypeError(f"{type(obj).__name__} is not canonically serializable")


def canonical_json(value: Any) -> str:
    """Deterministic JSON: sorted keys, no whitespace, no NaN, no exponents."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
        default=_json_default,
    )


def digest_of(value: Any) -> str:
    """SHA-256 of the canonical JSON of ``value``, prefixed with the algorithm."""
    payload = canonical_json(value).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


class StreamingDigest:
    """Incremental canonical digest for datasets too large to hold in memory.

    A 100k-deal run must be hashable without first materializing a list of
    100k dictionaries (section 9.3), so records are fed in and folded into the
    hash one at a time. Callers are responsible for feeding records in a
    deterministic order; the digest cannot detect an unstable sort for them.
    """

    __slots__ = ("_hash", "_count")

    def __init__(self) -> None:
        self._hash = hashlib.sha256()
        self._count = 0
        self._hash.update(f"canonical/v{CANONICAL_VERSION}".encode())
        self._hash.update(_RECORD_SEP)

    def update(self, kind: str, payload: Any) -> None:
        self._hash.update(kind.encode("utf-8"))
        self._hash.update(_FIELD_SEP)
        self._hash.update(canonical_json(payload).encode("utf-8"))
        self._hash.update(_RECORD_SEP)
        self._count += 1

    def update_text(self, kind: str, *fields: str) -> None:
        """Fold in already-canonical strings, in the caller's field order.

        Preferred over :meth:`update` for values that are *already* canonical
        on disk - a stored ``payload_json`` or a decimal held as TEXT. Parsing
        such a value only to re-serialize it was measured at 23x the cost on a
        40k-deal run, and it introduces a way for the digest to disagree with
        the bytes that were actually committed.

        Field order is explicit here rather than sorted, which also makes the
        included field set visible at the call site instead of implied by a
        dict's keys.
        """
        self._hash.update(kind.encode("utf-8"))
        for field in fields:
            self._hash.update(_FIELD_SEP)
            self._hash.update(field.encode("utf-8"))
        self._hash.update(_RECORD_SEP)
        self._count += 1

    @property
    def record_count(self) -> int:
        return self._count

    def hexdigest(self) -> str:
        # The record count is folded in last so that a truncated stream cannot
        # collide with a complete one that happens to share a prefix.
        final = self._hash.copy()
        final.update(f"count={self._count}".encode())
        return "sha256:" + final.hexdigest()


def filter_digest(parts: dict[str, Any]) -> str:
    """Digest of a query filter, used to bind a cursor to its filter (WF-036).

    ``None`` values are dropped so that an omitted parameter and an explicitly
    null one produce the same digest, which is what a client sending either
    form reasonably expects.
    """
    cleaned = {key: value for key, value in sorted(parts.items()) if value is not None}
    return digest_of(cleaned)
