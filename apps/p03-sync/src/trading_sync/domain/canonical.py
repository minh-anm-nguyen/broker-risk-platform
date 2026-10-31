"""Canonical serialization and content hashing.

The hash produced here is the service's definition of "same record". It decides
whether a replayed deal is a harmless duplicate or a money-integrity incident
(INV-04 versus INV-05), so its rules have to be explicit and versioned.

**Numeric equivalence, not textual.** ``'1.0'`` and ``'1.00'`` are the same
quantity and hash identically. The alternative -- hashing the source's
digit string -- would raise a conflict incident every time the source changed
its trailing-zero formatting, training operators to ignore the one alert that
must never be ignored (WF3-095).

**Versioned.** ``CANONICAL_HASH_VERSION`` is mixed into every digest. Changing
the rules therefore changes every hash, which is why a change requires a
deliberate migration and reconciliation rather than a silent redeploy: without
the version tag, the same stored row would start reading as a conflict
(WF3-173).
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping, Sequence

__all__ = [
    "CANONICAL_HASH_VERSION",
    "canonical_decimal_str",
    "canonical_payload",
    "page_hash",
    "payload_hash",
]

#: Bump only together with a migration plan for every stored ``payload_hash``.
CANONICAL_HASH_VERSION = "v1"


def canonical_decimal_str(value: Decimal) -> str:
    """Render a Decimal in one plain, trailing-zero-free form.

    ``normalize()`` alone is not enough: it turns ``Decimal('10')`` into
    ``Decimal('1E+1')``, so the exponent is folded back into plain notation
    before formatting.
    """
    if not value.is_finite():  # pragma: no cover - parse_money rejects these first
        raise ValueError("cannot canonicalize a non-finite Decimal")
    normalized = value.normalize()
    exponent = normalized.as_tuple().exponent
    assert isinstance(exponent, int)
    if exponent > 0:
        normalized = normalized.quantize(Decimal(1))
    rendered = format(normalized, "f")
    # Decimal keeps a sign on negative zero; one zero should hash as one value.
    return "0" if rendered in ("-0", "0") else rendered


def canonical_payload(entity: str, fields: Mapping[str, Any]) -> str:
    """Serialize ``fields`` deterministically for hashing.

    Keys are sorted and values are reduced to a small set of exactly
    representable types, so the digest depends on content alone and not on dict
    ordering or on how the source happened to format a number.
    """
    encodable = {key: _encode(value) for key, value in fields.items()}
    body = json.dumps(
        encodable,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )
    return f"{CANONICAL_HASH_VERSION}|{entity}|{body}"


def payload_hash(entity: str, fields: Mapping[str, Any]) -> str:
    """Return the 64-character hex digest of a record's canonical payload."""
    return hashlib.sha256(canonical_payload(entity, fields).encode("utf-8")).hexdigest()


def page_hash(
    *,
    entity: str,
    run_id: str,
    generation: int,
    snapshot_seq: int,
    page_index: int,
    item_hashes: Sequence[str],
) -> str:
    """Return the content hash recorded on a page receipt.

    Provenance is part of the digest, not just the items: an identical set of
    rows arriving under a different ``snapshot_seq`` is a different page, and a
    receipt replay must notice that (WF3-059).

    Item order is preserved rather than sorted, because deal page ordering is
    itself part of the contract being verified.
    """
    fields = {
        "entity": entity,
        "run_id": run_id,
        "generation": generation,
        "snapshot_seq": snapshot_seq,
        "page_index": page_index,
        "items": list(item_hashes),
    }
    return payload_hash("page", fields)


def _encode(value: Any) -> Any:
    if isinstance(value, Decimal):
        return canonical_decimal_str(value)
    if isinstance(value, datetime):
        if value.utcoffset() is None:
            raise ValueError("refusing to canonicalize a naive datetime")
        # Normalize to UTC first: the instant is the content, the offset is
        # only how the source happened to write it. Hashing the local
        # representation would make one moment in two zones look like two
        # different records (INV-13).
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, bool) or value is None or isinstance(value, (int, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [_encode(item) for item in value]
    if isinstance(value, Mapping):
        return {str(k): _encode(v) for k, v in value.items()}
    if isinstance(value, float):
        # A float in a canonical payload means a money field escaped validation.
        raise ValueError("refusing to canonicalize a binary float")
    raise ValueError(f"cannot canonicalize {type(value).__name__}")
