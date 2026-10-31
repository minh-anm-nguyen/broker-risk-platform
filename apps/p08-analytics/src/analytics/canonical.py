"""Canonical encoding, length-prefixed set digests and UTC time handling.

Three jobs, all of them load-bearing for the release gates:

1. **Canonical bytes.** Two independently produced encodings of the same Deal
   must be byte-identical, or ``deal_payload_hash`` comparisons against P07
   become meaningless. ``hash(str(dict))`` is explicitly forbidden by section
   6.1, so field order, Decimal formatting and separators are all pinned.

2. **Set digests that cannot be gamed by concatenation.** Section 11.3 asks
   for length-prefixed canonical serialization. Without it the two identity
   sets ``{("ab", "c")}`` and ``{("a", "bc")}`` hash alike, and a "set
   equality" proof that cannot distinguish those is not a proof.

3. **UTC discipline.** ``trade_day`` is the UTC day of ``executed_at``
   (INV08-14). Parsing that silently accepts a naive timestamp would let an
   ingest-local timezone shift a Deal into the wrong reporting day, so naive
   input is rejected rather than assumed.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Iterable

#: Bumped when the canonical encoding itself changes. A digest is comparable
#: only against another digest produced with the same version, so the version
#: is folded into every digest rather than kept as a side note.
CANONICAL_VERSION = "p08/1"

_HEX64 = re.compile(r"\A[0-9a-f]{64}\Z")


class EncodingError(ValueError):
    """The value cannot be canonically encoded, so it must not be hashed."""


def is_sha256_hex(value: object) -> bool:
    """True for exactly 64 lowercase hex characters.

    ``FixedString(64)`` in ClickHouse accepts any 64 bytes, so the DDL alone
    proves nothing about hashing (section 5.2). This is the real gate, applied
    before insert.
    """
    return isinstance(value, str) and bool(_HEX64.match(value))


def require_sha256_hex(value: object, field: str) -> str:
    if not is_sha256_hex(value):
        raise EncodingError(f"{field} must be 64 lowercase hex characters")
    return str(value)


# --------------------------------------------------------------------------
# Time
# --------------------------------------------------------------------------


def ensure_utc(value: datetime, field: str = "timestamp") -> datetime:
    """Return ``value`` as aware UTC, rejecting naive input.

    P01 can safely treat naive datetimes as UTC because its virtual clock is
    UTC by construction. P08 cannot: its inputs cross a process and a wire
    format, so a missing offset is a real ambiguity and the only safe answer
    is to refuse (INV08-14).
    """
    if value.tzinfo is None:
        raise EncodingError(f"{field} must be timezone-aware; naive input is ambiguous")
    return value.astimezone(UTC)


def format_timestamp(value: datetime) -> str:
    """RFC 3339 in UTC with a trailing ``Z`` and microsecond precision.

    Precision is fixed rather than minimal so that ``...:01.000000Z`` and
    ``...:01Z`` cannot produce two different digests for one instant.
    """
    aware = ensure_utc(value)
    return aware.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def parse_timestamp(text: str, field: str = "timestamp") -> datetime:
    """Parse an RFC 3339 timestamp; an absent offset is an error, not UTC."""
    if not isinstance(text, str) or not text.strip():
        raise EncodingError(f"{field} must be a non-empty RFC 3339 string")
    candidate = text.strip()
    if candidate.endswith(("Z", "z")):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise EncodingError(f"{field} is not a valid RFC 3339 timestamp") from exc
    if parsed.tzinfo is None:
        raise EncodingError(f"{field} must carry a UTC offset")
    return parsed.astimezone(UTC)


def trade_day_of(executed_at: datetime) -> date:
    """UTC calendar day of a Deal execution. INV08-14.

    Deliberately not configurable at the call site. A reporting timezone other
    than UTC is a versioned policy decision (section 7.1) that must change the
    ``policy_version`` and therefore the ``build_id``; it is not something an
    individual query may pass in.
    """
    return ensure_utc(executed_at, "executed_at").date()


# --------------------------------------------------------------------------
# Canonical JSON
# --------------------------------------------------------------------------


def canonical_decimal_text(value: Decimal) -> str:
    """One text form per numeric value, for hashing.

    Three different spellings of one amount reach this function in practice -
    ``12.30``, ``12.300`` and ``1.230E+1`` - and ``format(x, "f")`` alone
    preserves the first two as distinct strings. That would make a producer
    trimming a trailing zero look like two conflicting versions of the same
    Deal, and P08 blocks a build on exactly that signal (INV08-03). A false
    integrity incident is nearly as damaging as a missed one, so trailing
    zeros are stripped and negative zero collapses to ``0``.

    Display formatting is deliberately *not* this: ``format_money`` keeps the
    scale a reader expects to see. Only the digest normalises.
    """
    if not value.is_finite():
        raise EncodingError("refusing to encode a non-finite Decimal")
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return canonical_decimal_text(obj)
    if isinstance(obj, datetime):
        return format_timestamp(obj)
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, (set, frozenset)):
        return sorted(obj)
    raise EncodingError(f"{type(obj).__name__} is not canonically serializable")


def canonical_json(value: Any) -> str:
    """Deterministic JSON: sorted keys, no whitespace, no NaN, no floats.

    Floats are rejected outright rather than formatted. A float that reached
    this function is a money bug somewhere upstream, and silently writing
    ``0.1`` for a value that is really ``0.1000000000000000055`` would hide
    exactly the defect the project most needs to catch (section 7.2).
    """
    _reject_floats(value)
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
        default=_json_default,
    )


def _reject_floats(value: Any, path: str = "$") -> None:
    if isinstance(value, float):
        raise EncodingError(f"float at {path}: money and identity fields must not use binary float")
    if isinstance(value, dict):
        for key, item in value.items():
            _reject_floats(item, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _reject_floats(item, f"{path}[{index}]")


def digest_of(value: Any) -> str:
    """Bare SHA-256 hex of the canonical JSON of ``value``.

    Returned without an ``sha256:`` prefix because these digests are compared
    against P07 ``payload_sha256`` values and stored in ClickHouse
    ``FixedString(64)`` columns.
    """
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------
# Length-prefixed set and sequence digests
# --------------------------------------------------------------------------


def _encode_part(part: object) -> bytes:
    """Length-prefixed encoding of one tuple element.

    The prefix is what makes concatenation unambiguous: ``2:ab1:c`` and
    ``1:a2:bc`` differ, where ``abc`` and ``abc`` would not (section 11.3).
    """
    if isinstance(part, bytes):
        raw = part
    elif isinstance(part, str):
        raw = part.encode("utf-8")
    elif isinstance(part, bool):
        # Checked before int, since bool is a subclass of int.
        raw = (b"true" if part else b"false")
    elif isinstance(part, int):
        raw = str(part).encode("ascii")
    elif isinstance(part, Decimal):
        raw = canonical_decimal_text(part).encode("ascii")
    elif isinstance(part, datetime):
        raw = format_timestamp(part).encode("ascii")
    elif isinstance(part, date):
        raw = part.isoformat().encode("ascii")
    elif isinstance(part, Enum):
        return _encode_part(part.value)
    elif part is None:
        # A distinct tag, so None and the literal string "None" differ.
        return b"\x00:"
    else:
        raise EncodingError(f"{type(part).__name__} cannot be length-prefix encoded")
    return str(len(raw)).encode("ascii") + b":" + raw


def encode_tuple(parts: Iterable[object]) -> bytes:
    """Length-prefixed encoding of a tuple, terminated by its arity."""
    chunks = [_encode_part(part) for part in parts]
    return b"".join(chunks) + b"#" + str(len(chunks)).encode("ascii")


def sequence_digest(rows: Iterable[Iterable[object]], *, kind: str) -> str:
    """Digest of an *ordered* sequence of tuples.

    ``kind`` is folded in so that, say, a raw-identity digest can never
    accidentally compare equal to a rollup digest of the same tuples.
    """
    hasher = hashlib.sha256()
    hasher.update(CANONICAL_VERSION.encode("ascii"))
    hasher.update(b"\x1e")
    hasher.update(kind.encode("utf-8"))
    hasher.update(b"\x1e")
    count = 0
    for row in rows:
        hasher.update(encode_tuple(row))
        hasher.update(b"\x1e")
        count += 1
    # Count last, so a truncated stream cannot collide with a complete one
    # that shares its prefix.
    hasher.update(f"count={count}".encode("ascii"))
    return hasher.hexdigest()


def set_digest(rows: Iterable[Iterable[object]], *, kind: str) -> str:
    """Digest of an *unordered* set of tuples, by sorting their encodings.

    Sorting the encoded bytes rather than the Python tuples avoids comparing
    heterogeneous types (``int`` vs ``str`` raises) and makes the digest
    independent of the physical row order the storage engine happens to
    return (INV08-22, P08-R-11).
    """
    encoded = sorted(encode_tuple(row) for row in rows)
    hasher = hashlib.sha256()
    hasher.update(CANONICAL_VERSION.encode("ascii"))
    hasher.update(b"\x1e")
    hasher.update(f"set/{kind}".encode("utf-8"))
    hasher.update(b"\x1e")
    for item in encoded:
        hasher.update(item)
        hasher.update(b"\x1e")
    hasher.update(f"count={len(encoded)}".encode("ascii"))
    return hasher.hexdigest()


class ChunkedSetDigest:
    """Set digest for row counts too large to sort in memory at once.

    Section 11.3 asks for chunk manifests on million-row fixtures. Chunks are
    hashed independently, then the *sorted* chunk digests are folded together,
    so the result stays independent of the order chunks were fed in while each
    chunk's own contents remain order-independent too.
    """

    __slots__ = ("_kind", "_chunk", "_chunk_size", "_digests", "_count")

    def __init__(self, kind: str, chunk_size: int = 100_000) -> None:
        if chunk_size < 1:
            raise ValueError("chunk_size must be positive")
        self._kind = kind
        self._chunk: list[bytes] = []
        self._chunk_size = chunk_size
        self._digests: list[str] = []
        self._count = 0

    def add(self, row: Iterable[object]) -> None:
        self._chunk.append(encode_tuple(row))
        self._count += 1
        if len(self._chunk) >= self._chunk_size:
            self._flush()

    def _flush(self) -> None:
        if not self._chunk:
            return
        hasher = hashlib.sha256()
        for item in sorted(self._chunk):
            hasher.update(item)
            hasher.update(b"\x1e")
        self._digests.append(hasher.hexdigest())
        self._chunk = []

    @property
    def count(self) -> int:
        return self._count

    def chunk_digests(self) -> list[str]:
        self._flush()
        return list(self._digests)

    def hexdigest(self) -> str:
        self._flush()
        hasher = hashlib.sha256()
        hasher.update(CANONICAL_VERSION.encode("ascii"))
        hasher.update(b"\x1e")
        hasher.update(f"chunked-set/{self._kind}".encode("utf-8"))
        hasher.update(b"\x1e")
        for item in sorted(self._digests):
            hasher.update(item.encode("ascii"))
            hasher.update(b"\x1e")
        hasher.update(f"count={self._count}".encode("ascii"))
        return hasher.hexdigest()
