"""One documented canonical encoding, and the SHA-256 taken over it.

``payload_sha256`` is only meaningful if every producer and every consumer
agrees byte-for-byte on how a payload is encoded. Section 5.4 of the design is
explicit that the hash must not depend on incidental Python formatting, so this
module owns the single encoding and nothing else is allowed to serialise an
event for hashing or for the wire.

The rules, all of them load-bearing:

* **Keys sorted by Unicode code point, no whitespace.** Two structurally equal
  payloads must produce one byte string.
* **No floats, ever.** ``json`` would happily write ``0.1`` as
  ``0.1000000000000000055511151231257827``-adjacent repr or accept ``NaN``.
  Money arrives as a decimal string (see :mod:`outbox.money`); a ``float``
  anywhere in the tree is a defect, not something to coerce.
* **Reject, do not repair.** Unknown types, non-string keys, oversized or
  too-deeply-nested structures raise instead of being normalised, because a
  silent repair changes a financial fact.
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any

from .errors import ErrorCode, ValidationError

#: Hard ceiling on nesting. Protects the recursive validator and any consumer
#: that walks the tree; an event envelope needs nothing close to this.
MAX_DEPTH = 12

#: Default ceiling on one canonical payload/envelope encoding, in bytes.
#: Overridable per deployment via settings; the design proposes 128 KiB.
DEFAULT_MAX_WIRE_BYTES = 131_072

JsonScalar = str | int | bool | None
JsonValue = JsonScalar | dict[str, Any] | list[Any]


def _reject(reason: str) -> ValidationError:
    return ValidationError(reason, code=ErrorCode.SERIALIZATION_UNSUPPORTED)


def validate_canonical_tree(value: Any, *, _depth: int = 0, _path: str = "$") -> None:
    """Raise unless ``value`` is encodable under the canonical rules.

    Separated from encoding so callers can validate a structure they built
    before they commit to serialising it, and so the error message can name the
    offending JSON path rather than just the type.
    """
    if _depth > MAX_DEPTH:
        raise _reject(f"canonical payload nested deeper than {MAX_DEPTH} at {_path}")

    if isinstance(value, bool) or value is None or isinstance(value, str):
        return
    if isinstance(value, int):
        # Python ints are unbounded; a consumer in another language is not.
        if not (-(2**63) <= value < 2**63):
            raise _reject(f"integer at {_path} does not fit in signed 64 bits")
        return
    if isinstance(value, float):
        raise _reject(f"float at {_path}: monetary and numeric fields must be decimal strings")
    if isinstance(value, Decimal):
        # A Decimal is right in the domain layer and wrong on the wire: it has
        # no JSON representation, so the caller must choose a string form via
        # outbox.money and keep the scale explicit.
        raise _reject(f"Decimal at {_path}: serialise through outbox.money first")
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise _reject(f"non-string object key at {_path}: {type(key).__name__}")
            if "\x00" in key:
                raise _reject(f"NUL in object key at {_path}")
            validate_canonical_tree(item, _depth=_depth + 1, _path=f"{_path}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            validate_canonical_tree(item, _depth=_depth + 1, _path=f"{_path}[{index}]")
        return
    raise _reject(f"unsupported type at {_path}: {type(value).__name__}")


def canonical_json_bytes(value: JsonValue, *, max_bytes: int | None = DEFAULT_MAX_WIRE_BYTES) -> bytes:
    """Encode ``value`` under the single canonical encoding.

    ``max_bytes=None`` skips the size check; callers that are hashing a payload
    they will embed in a larger envelope use it so the limit is applied once,
    against the thing that actually goes on the wire.
    """
    validate_canonical_tree(value)
    # allow_nan=False is belt-and-braces: validate_canonical_tree already
    # refuses float, and NaN/Infinity are the only way json emits non-JSON.
    text = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    encoded = text.encode("utf-8")
    if max_bytes is not None and len(encoded) > max_bytes:
        raise ValidationError(
            f"canonical encoding is {len(encoded)} bytes, limit is {max_bytes}",
            code=ErrorCode.MESSAGE_TOO_LARGE,
        )
    return encoded


def sha256_hex(data: bytes) -> str:
    """Lowercase hex SHA-256. The one digest format stored and compared."""
    return hashlib.sha256(data).hexdigest()


def payload_hash(payload: JsonValue) -> str:
    """SHA-256 over the canonical encoding of a payload.

    Size is deliberately unbounded here: the payload is hashed before it is
    wrapped, and the envelope encoding enforces the wire limit for both.
    """
    return sha256_hex(canonical_json_bytes(payload, max_bytes=None))


def decode_wire_bytes(data: bytes) -> dict[str, Any]:
    """Parse a wire record back to a dict under the same strictness.

    Consumers must not accept what producers may not emit, otherwise a record
    that no honest producer could have written would still be applied.
    """
    if not isinstance(data, (bytes, bytearray)):
        raise _reject(f"wire record is {type(data).__name__}, expected bytes")
    try:
        text = bytes(data).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _reject(f"wire record is not valid UTF-8: {exc.reason}") from exc
    try:
        value = json.loads(text, parse_float=_refuse_float, parse_constant=_refuse_constant)
    except json.JSONDecodeError as exc:
        raise _reject(f"wire record is not valid JSON: {exc.msg}") from exc
    if not isinstance(value, dict):
        raise _reject(f"wire record decoded to {type(value).__name__}, expected object")
    validate_canonical_tree(value)
    return value


def _refuse_float(raw: str) -> Any:
    raise _reject(f"JSON number {raw!r} would decode to float; decimal strings only")


def _refuse_constant(raw: str) -> Any:
    raise _reject(f"JSON constant {raw!r} is not permitted")
