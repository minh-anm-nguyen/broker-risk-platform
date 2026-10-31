"""Typed records that cross the source trust boundary (sections 3.2, 2.1).

Everything here is validated *before* it can influence a verdict. The source
is explicitly untrusted in this architecture: a malformed page, a page from a
different snapshot, or a page carrying a float where money belongs must be
rejected at the boundary rather than classified as a data difference later
(section 3.2, FMEA "external source lies").

These are plain frozen dataclasses rather than pydantic models because the
connector layer already validates wire types; what this module adds is the
*structural* contract P04 relies on -- page provenance travels with every
page, so no code downstream can forget to check which snapshot it came from.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from .errors import DataIntegrityError, ReasonCode


@dataclass(frozen=True, slots=True)
class SourceIdentity:
    """``/v1/meta`` provenance: which source, run and generation (section 2.1).

    ``(run_id, generation)`` is the source's own identity for its data set. A
    change in either means the data was regenerated, so previously captured
    manifests describe a world that no longer exists (section 7.7).
    """

    source_id: str
    run_id: str
    generation: int
    snapshot_seq: int
    account_count: int = 0
    deal_count: int = 0
    open_position_count: int = 0

    def matches(self, other: SourceIdentity) -> bool:
        return (
            self.source_id == other.source_id
            and self.run_id == other.run_id
            and self.generation == other.generation
        )


@dataclass(frozen=True, slots=True)
class SourceCapabilities:
    """What the source actually promises (sections 2.3, 11.2).

    Defaults describe the P01 simulator as designed. A real Manager API
    adapter must set these from verified behaviour, not copy them: section
    0.4 forbids assuming a real SDK provides ``source_seq`` or stable
    snapshots.
    """

    api_version: str = "v1"
    max_page_size: int = 500
    snapshot_ttl_seconds: int = 900
    after_seq_is_exclusive: bool = True
    stable_snapshot_pagination: bool = True
    durable_epoch_identity: bool = True
    complete_deal_history: bool = True
    full_unfiltered_listing: bool = True
    global_cut_across_entities: bool = False
    identical_financial_semantics: bool = True
    generator_version: str = ""
    accounting_policy_version: str = ""

    @classmethod
    def from_meta(cls, payload: Mapping[str, Any]) -> SourceCapabilities:
        caps = payload.get("capabilities") or {}
        return cls(
            api_version=str(caps.get("api_version", "v1")),
            max_page_size=int(caps.get("max_page_size", 500)),
            snapshot_ttl_seconds=int(caps.get("snapshot_ttl_seconds", 900)),
            after_seq_is_exclusive=bool(caps.get("after_seq_is_exclusive", True)),
            generator_version=str(caps.get("generator_version", "")),
            accounting_policy_version=str(caps.get("accounting_policy_version", "")),
            # P01 publishes run_id/generation/snapshot_seq/source_seq, which is
            # what the stronger capabilities below depend on. A source that
            # does not list them cannot support a full-set compare at S.
            stable_snapshot_pagination="snapshot_seq" in (caps.get("simulator_specific_fields") or []),
            durable_epoch_identity="run_id" in (caps.get("simulator_specific_fields") or []),
            complete_deal_history="source_seq" in (caps.get("simulator_specific_fields") or []),
            full_unfiltered_listing=True,
            # Never inferred. Section 0.4: no global cut is claimed unless a
            # tested contract provides one.
            global_cut_across_entities=bool(caps.get("global_cut_across_entities", False)),
        )


@dataclass(frozen=True, slots=True)
class SourcePage:
    """One validated page of a source listing.

    The envelope fields are mandatory, not optional: a page that cannot say
    which snapshot it belongs to is unusable as evidence (section 2.1
    invariant on ``run_id``/``generation``/``snapshot_seq``).
    """

    page_index: int
    run_id: str
    generation: int
    snapshot_seq: int
    items: Sequence[Mapping[str, Any]]
    next_cursor: str | None
    filtered: bool = False

    @property
    def is_terminal(self) -> bool:
        """``next_cursor=null`` ends *this snapshot*, not all future events.

        Section 2.1 calls this out explicitly; the name avoids the reading
        that the data set is now closed forever.
        """
        return self.next_cursor is None

    def validate_envelope(self, expected: SourcePage | None, *, max_page_size: int) -> None:
        """Check this page against the snapshot the scan is pinned to.

        Page 1 establishes ``(run_id, generation, snapshot_seq)``; pages 2..N
        must match exactly (section 7.4 step 2). A drift here is the
        difference between comparing one snapshot and silently stitching two
        together.
        """
        if len(self.items) > max_page_size:
            raise DataIntegrityError(
                "page exceeds the declared maximum page size",
                reason=ReasonCode.SOURCE_PAGE_OVERSIZED,
                page_index=self.page_index,
                item_count=len(self.items),
                max_page_size=max_page_size,
            )
        if self.filtered:
            raise DataIntegrityError(
                "a filtered listing cannot support a full-set comparison",
                reason=ReasonCode.SOURCE_SCAN_FILTERED,
                page_index=self.page_index,
            )
        if expected is None:
            return
        if self.run_id != expected.run_id or self.generation != expected.generation:
            raise DataIntegrityError(
                "page belongs to a different source run or generation",
                reason=ReasonCode.EPOCH_MISMATCH,
                page_index=self.page_index,
                expected_run_id=expected.run_id,
                expected_generation=expected.generation,
                actual_run_id=self.run_id,
                actual_generation=self.generation,
            )
        if self.snapshot_seq != expected.snapshot_seq:
            raise DataIntegrityError(
                "page belongs to a different snapshot than the pinned cut",
                reason=ReasonCode.SOURCE_CUT_DRIFTED,
                page_index=self.page_index,
                expected_snapshot_seq=expected.snapshot_seq,
                actual_snapshot_seq=self.snapshot_seq,
            )


@dataclass(frozen=True, slots=True)
class SyncStatus:
    """P03's answer to ``sync_status`` (section 2.2).

    ``watermark_seq`` is meaningful only together with ``published`` and
    ``filter_hash``: an unpublished watermark, or one derived from a filtered
    scan, cannot bound a full-set Deal comparison (section 5.2 step 4).
    """

    source_id: str
    epoch_id: str
    entity: str
    epoch_status: str
    published_scan_id: str | None
    snapshot_seq: int | None
    watermark_seq: int
    filter_hash: str
    published: bool
    lease_version: int
    terminal_receipt: bool = False
    record_count: int | None = None


@dataclass(frozen=True, slots=True)
class RepairReceipt:
    """P03's authoritative outcome for one intent (sections 2.2, 7.5).

    ``ACCEPTED`` is not ``COMMITTED`` and ``COMMITTED`` is not ``VERIFIED``
    (section 4.2 invariant). ``execution_count`` lets the idempotency test
    assert that a replayed intent did not run the work twice (INV04-08).
    """

    intent_id: str
    status: str
    reason_code: str | None = None
    receipt_hash: str | None = None
    result_scan_id: str | None = None
    result_watermark: int | None = None
    execution_count: int = 0

    @property
    def is_terminal(self) -> bool:
        return self.status in {"COMMITTED", "FAILED", "REJECTED"}

    @property
    def is_committed(self) -> bool:
        return self.status == "COMMITTED"


@dataclass(frozen=True, slots=True)
class CapturedItem:
    """One manifest row: identity, provenance and content digest.

    The raw record is intentionally absent. Manifests are evidence about
    shape, not a second copy of the data (section 6.2 caveat 2).
    """

    item_key: str
    payload_hash: str
    source_seq: int | None = None


@dataclass(frozen=True, slots=True)
class CapturedPage:
    """A page reduced to the rows that will be committed with its receipt."""

    page_index: int
    run_id: str
    generation: int
    snapshot_seq: int
    items: Sequence[CapturedItem]
    page_hash: str
    is_terminal: bool
    cursor_digest: str | None = None


@dataclass(frozen=True, slots=True)
class DiffRow:
    """One row of the set-diff result (section 6.3)."""

    item_key: str
    diff_type: str
    source_hash: str | None
    target_hash: str | None
    source_seq: int | None = None


@dataclass(frozen=True, slots=True)
class AlignmentDecision:
    """The outcome of the comparison-precondition gate (section 5.1).

    ``strict_compare_allowed`` false means no ``PROVEN`` finding may be
    emitted from this run; the reason code says which precondition failed.
    """

    strict_compare_allowed: bool
    reason_code: ReasonCode | None
    confidence: str
    checks: Mapping[str, bool] = field(default_factory=dict)
    detail: Mapping[str, Any] = field(default_factory=dict)
