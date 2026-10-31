"""Canonical serialization, input/output digests and the lineage manifest.

A digest is only an oracle if two hosts agree on it. That means canonical
serialization has to remove every source of host-dependent variation:

* dictionary order (sorted keys),
* Decimal formatting (no exponent notation, no negative zero),
* floats (rejected outright -- a float in a digest input is a precision bug
  that would otherwise be hashed into permanence),
* timezone rendering (always UTC, always the same shape),
* set ordering (sorted),
* text encoding (UTF-8, no ASCII escaping surprises).

The result is that an input digest identifies *exactly* which facts, versions
and policies produced a number, and an output digest lets a replay prove the
number has not changed (INV05-10, WF5-282, WF5-330).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Mapping, Sequence

from risk.domain.facts import DealFact, PositionFact, QuoteSnapshot, RiskCut, RiskInputBundle
from risk.domain.money import Money, Quantity, canonical_decimal_str
from risk.errors import ContractError, ReasonCode
from risk.status import Consistency, ExactnessClass

__all__ = [
    "canonicalize",
    "canonical_json",
    "digest",
    "scope_hash",
    "LineageManifest",
    "build_input_manifest",
    "classify_exactness",
]


def canonicalize(value: Any) -> Any:
    """Convert a value into a JSON-safe, deterministic shape."""
    if value is None or isinstance(value, (str, bool, int)):
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            # int is exact; large ints are fine in JSON as digits.
            return value
        return value
    if isinstance(value, float):
        raise ContractError(
            ReasonCode.DECIMAL_STRING_REQUIRED, "a float may never enter a canonical digest"
        )
    if isinstance(value, Decimal):
        return canonical_decimal_str(value)
    if isinstance(value, (Money, Quantity)):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ContractError(ReasonCode.NAIVE_TIMESTAMP_REJECTED, "naive datetime in digest input")
        return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
    if isinstance(value, Mapping):
        return {str(k): canonicalize(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, (set, frozenset)):
        return sorted(canonicalize(v) for v in value)
    if isinstance(value, Sequence):
        return [canonicalize(v) for v in value]
    # Enums (StrEnum) and anything else explicitly stringified rather than
    # reflected over, so a new attribute cannot change a digest by accident.
    return str(value)


def canonical_json(value: Any) -> str:
    """Serialize to the one byte-sequence this project hashes."""
    return json.dumps(
        canonicalize(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def digest(value: Any) -> str:
    """SHA-256 of the canonical JSON form."""
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def scope_hash(tenant_id: str, source_id: str, account_ids: frozenset[str]) -> str:
    """Stable hash standing in for the account list.

    Account identifiers are not safe to put in metric labels or audit rows at
    rest, but a scope still has to be comparable across requests. Hashing gives
    comparability without disclosure (section 6.5, WF5-200).
    """
    return digest({"tenant": tenant_id, "source": source_id, "accounts": sorted(account_ids)})


@dataclass(frozen=True)
class LineageManifest:
    """Everything needed to re-derive a result, and nothing else.

    "Nothing else" is a requirement, not minimalism: the manifest is persisted
    and may be read by operators, so it holds row counts and digests rather
    than amounts and account numbers (WF5-281).
    """

    cut: dict[str, object]
    scope_hash: str
    risk_policy_version: str
    symbol_catalog_version: str
    rounding_mode: str
    source_capability: str
    requested_components: tuple[str, ...]
    period_start: str | None
    period_end: str | None
    valuation_as_of: str | None
    row_counts: dict[str, int]
    fact_digests: dict[str, str]
    coverage: dict[str, object]
    strict_composite: bool

    def as_payload(self) -> dict[str, object]:
        return {
            "cut": self.cut,
            "scope_hash": self.scope_hash,
            "risk_policy_version": self.risk_policy_version,
            "symbol_catalog_version": self.symbol_catalog_version,
            "rounding_mode": self.rounding_mode,
            "source_capability": self.source_capability,
            "requested_components": list(self.requested_components),
            "period_start": self.period_start,
            "period_end": self.period_end,
            "valuation_as_of": self.valuation_as_of,
            "row_counts": self.row_counts,
            "fact_digests": self.fact_digests,
            "coverage": self.coverage,
            "strict_composite": self.strict_composite,
        }

    def input_digest(self) -> str:
        return digest(self.as_payload())


def _position_fingerprint(positions: Sequence[PositionFact]) -> str:
    """Digest the position set, order-independently.

    Sorting before hashing is what makes the digest a *set* fingerprint: a
    reader that returns the same rows in a different order must not produce a
    different lineage (WF5-303).
    """
    rows = sorted(
        (
            p.position_id,
            p.account_id,
            p.symbol,
            str(p.side),
            canonical_decimal_str(p.lots),
            canonical_decimal_str(p.open_price),
            p.source_status,
            str(p.presence),
            p.published_scan_id,
        )
        for p in positions
    )
    return digest(rows)


def _deal_fingerprint(deals: Sequence[DealFact]) -> str:
    rows = sorted(
        (
            d.deal_id,
            d.account_id,
            d.source_seq,
            d.entry_kind,
            d.deal_type or "",
            d.gross_profit.canonical(),
            d.commission.canonical(),
            d.swap.canonical(),
            d.executed_at.astimezone(UTC).isoformat(),
            d.currency,
        )
        for d in deals
    )
    return digest(rows)


def _quote_fingerprint(snapshot: QuoteSnapshot | None) -> str:
    if snapshot is None:
        return ""
    rows = sorted(
        (
            q.symbol,
            canonical_decimal_str(q.bid),
            canonical_decimal_str(q.ask),
            q.market_time.astimezone(UTC).isoformat(),
            q.quote_id,
            q.feed_status,
        )
        for q in snapshot.quotes.values()
    )
    return digest({"snapshot_id": snapshot.snapshot_id, "quotes": rows})


def build_input_manifest(
    bundle: RiskInputBundle,
    *,
    risk_policy_version: str,
    rounding_mode: str,
    source_capability: str,
    requested_components: Sequence[str],
    strict_composite: bool,
) -> LineageManifest:
    """Fingerprint a captured bundle into a replayable manifest."""
    accounts = sorted(
        (a.account_id, a.currency, a.published_scan_id, str(a.presence)) for a in bundle.accounts
    )
    return LineageManifest(
        cut=bundle.cut.as_manifest(),
        scope_hash=scope_hash(bundle.scope.tenant_id, bundle.scope.source_id, bundle.scope.account_ids),
        risk_policy_version=risk_policy_version,
        symbol_catalog_version=bundle.catalog.catalog_version,
        rounding_mode=rounding_mode,
        source_capability=source_capability,
        requested_components=tuple(sorted(requested_components)),
        period_start=None if bundle.period_start is None else bundle.period_start.isoformat(),
        period_end=None if bundle.period_end is None else bundle.period_end.isoformat(),
        valuation_as_of=None if bundle.valuation_as_of is None else bundle.valuation_as_of.isoformat(),
        row_counts={
            "accounts": len(bundle.accounts),
            "positions": len(bundle.positions),
            "deals": len(bundle.deals),
            "quotes": 0 if bundle.quotes is None else len(bundle.quotes.quotes),
            "catalog_symbols": len(bundle.catalog.specs),
        },
        fact_digests={
            "accounts": digest(accounts),
            "positions": _position_fingerprint(bundle.positions),
            "deals": _deal_fingerprint(bundle.deals),
            "quotes": _quote_fingerprint(bundle.quotes),
        },
        coverage={
            "account_scan_published": bundle.coverage.account_scan_published,
            "position_scan_published": bundle.coverage.position_scan_published,
            "position_scan_terminal": bundle.coverage.position_scan_terminal,
            "deal_watermark_terminal": bundle.coverage.deal_watermark_terminal,
            "deal_watermark_unfiltered": bundle.coverage.deal_watermark_unfiltered,
            "epoch_active": bundle.coverage.epoch_active,
            "quote_snapshot_pinned": bundle.coverage.quote_snapshot_pinned,
            "fee_fields_available": bundle.coverage.fee_fields_available,
            "notes": list(bundle.coverage.notes),
        },
        strict_composite=strict_composite,
    )


def classify_exactness(cut: RiskCut, *, composite_requested: bool) -> ExactnessClass:
    """Which of the three result classes of section 4.1 applies."""
    if cut.consistency is Consistency.UNPROVEN:
        return ExactnessClass.PROVISIONAL
    if composite_requested and cut.consistency is Consistency.COMPATIBLE:
        return ExactnessClass.COMPOSITE_ALIGNED
    if composite_requested:
        return ExactnessClass.PROVISIONAL
    return ExactnessClass.COMPONENT_EXACT
