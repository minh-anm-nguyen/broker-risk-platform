"""Canonical manifest serialization and digests.

A digest is only meaningful if two independent processes can rebuild the exact
same bytes. This module is the single place that decides those bytes.
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any

__all__ = ["canonical_json", "digest", "canonicalize"]


def canonicalize(value: Any) -> Any:
    """Recursively convert a value tree into digest-stable primitives."""
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, bool) or value is None or isinstance(value, (int, str)):
        return value
    if isinstance(value, float):  # pragma: no cover - guarded by callers
        raise TypeError("float is not allowed in a canonical manifest")
    if isinstance(value, dict):
        return {str(k): canonicalize(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, (list, tuple)):
        return [canonicalize(v) for v in value]
    if hasattr(value, "to_manifest"):
        return canonicalize(value.to_manifest())
    raise TypeError(f"not canonicalizable: {type(value)!r}")


def canonical_json(value: Any) -> bytes:
    """Serialize to deterministic UTF-8 JSON bytes."""
    return json.dumps(
        canonicalize(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def digest(value: Any) -> str:
    """SHA-256 of the canonical JSON, prefixed with its algorithm."""
    return "sha256:" + hashlib.sha256(canonical_json(value)).hexdigest()
