"""Cut scopes, source manifests, P04 trust and build manifests. Sections 4, 5.4.

The load-bearing idea in this module is that **completeness is a set
property, not a count**. Section 6.3 spells out why: a cut that lost one Deal
and duplicated another has the right ``count()`` and the right
``uniqExact()``, and is still wrong. So :class:`SourceCutManifest` carries the
full ``(deal_id, deal_payload_hash)`` set, and the comparison against a build
is a set difference in both directions.

The second idea is that a manifest can admit its own ignorance.
``complete=False`` means P03 could not enumerate the cut, and a build on such
a manifest reports ``INCOMPLETE`` instead of publishing what it happened to
find (FR08-06, INV08-13).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, Mapping

from ..canonical import ensure_utc, require_sha256_hex, set_digest
from ..errors import CompletenessError, Reason


@dataclass(frozen=True, slots=True)
class CutScope:
    """Everything that must be pinned for a build to mean anything.

    Section 4 requires every P0 proof to be scoped by exactly these six
    values, so they travel as one object. A function taking a ``CutScope``
    cannot forget the epoch the way a function taking six strings can.
    """

    tenant_id: str
    source_id: str
    epoch_id: str
    report_family: str
    cut_id: str
    policy_version: str

    @property
    def identity(self) -> tuple[str, str, str]:
        return (self.tenant_id, self.source_id, self.epoch_id)

    @property
    def claim_key(self) -> tuple[str, str, str, str, str, str]:
        return (
            self.tenant_id,
            self.source_id,
            self.epoch_id,
            self.report_family,
            self.cut_id,
            self.policy_version,
        )

    @property
    def publication_key(self) -> tuple[str, str, str, str]:
        """Pointer identity: one published build per family per source epoch."""
        return (self.tenant_id, self.source_id, self.epoch_id, self.report_family)

    def as_wire(self) -> dict[str, str]:
        return {
            "tenant_id": self.tenant_id,
            "source_id": self.source_id,
            "epoch_id": self.epoch_id,
            "report_family": self.report_family,
            "cut_id": self.cut_id,
            "policy_version": self.policy_version,
        }


@dataclass(frozen=True, slots=True)
class SourceDealIdentity:
    """One Deal as P03 claims it exists at the cut."""

    deal_id: str
    deal_payload_hash: str
    source_seq: int

    def __post_init__(self) -> None:
        require_sha256_hex(self.deal_payload_hash, "deal_payload_hash")


@dataclass(frozen=True, slots=True)
class SourceCutManifest:
    """The authoritative expected Deal set for one cut. Sections 5.4, 6.2.

    Produced by P03 under a consistent read, never by P08 from its own sink.
    An oracle that derived this from ClickHouse would agree with ClickHouse by
    construction and prove nothing (section 9.1, P08-R-09).
    """

    scope: CutScope
    deal_watermark: int
    captured_at: datetime
    deals: tuple[SourceDealIdentity, ...]
    kafka_cut: Mapping[str, int] = field(default_factory=dict)
    #: ``False`` when P03 could not enumerate the complete set at this cut.
    complete: bool = True
    incomplete_reason: str | None = None
    #: Oldest event time still retained upstream. Used to detect the case
    #: where recovery is impossible because history has already expired.
    retention_horizon: datetime | None = None

    def __post_init__(self) -> None:
        ensure_utc(self.captured_at, "captured_at")
        seen: set[str] = set()
        for deal in self.deals:
            if deal.deal_id in seen:
                # A source manifest with a duplicate Deal id is itself
                # broken; accepting it would make set equality meaningless.
                raise CompletenessError(
                    Reason.SOURCE_MANIFEST_MISSING,
                    f"source manifest repeats deal {deal.deal_id!r}",
                    deal_id=deal.deal_id,
                )
            seen.add(deal.deal_id)

    @property
    def expected_unique_deals(self) -> int:
        return len(self.deals)

    def identity_pairs(self) -> list[tuple[str, str]]:
        return [(deal.deal_id, deal.deal_payload_hash) for deal in self.deals]

    def identity_digest(self) -> str:
        """Order-independent digest of the expected ``(deal, hash)`` set."""
        return set_digest(
            ((self.scope.tenant_id, self.scope.source_id, self.scope.epoch_id, d, h)
             for d, h in self.identity_pairs()),
            kind="source-deal-identity",
        )

    def hash_by_deal(self) -> dict[str, str]:
        return {deal.deal_id: deal.deal_payload_hash for deal in self.deals}

    def require_complete(self) -> None:
        if not self.complete:
            raise CompletenessError(
                Reason.SOURCE_MANIFEST_MISSING,
                self.incomplete_reason or "source could not enumerate the cut",
                cut_id=self.scope.cut_id,
            )

    def manifest_sha256(self) -> str:
        """Digest binding scope, watermark and the expected set together.

        Stored on the build row so a later audit can prove which manifest the
        build was verified against, and detect a manifest swapped underneath
        a running build (P08-Q).
        """
        return set_digest(
            [
                ("scope", *self.scope.claim_key),
                ("watermark", str(self.deal_watermark)),
                ("complete", "1" if self.complete else "0"),
                ("identities", self.identity_digest()),
            ],
            kind="source-cut-manifest",
        )


@dataclass(frozen=True, slots=True)
class ReconciliationTrust:
    """P04's verdict on a cut. Section 6.7.

    ``revoked`` exists because P08 cannot hold a lock in P04's transaction
    domain. The pre-publish gate re-reads this object, and a revocation
    arriving between build and publish must block the promotion rather than be
    missed (P08-S-12, P08-X).
    """

    revision: str
    scope: CutScope
    verified: bool
    blocking_findings: tuple[str, ...] = ()
    revoked: bool = False
    evaluated_at: datetime | None = None

    def require_publishable(self) -> None:
        if self.revoked:
            raise CompletenessError(
                Reason.RECONCILIATION_REVOKED,
                f"reconciliation revision {self.revision} was revoked",
                revision=self.revision,
            )
        if not self.verified or self.blocking_findings:
            raise CompletenessError(
                Reason.RECONCILIATION_NOT_VERIFIED,
                f"reconciliation revision {self.revision} is not verified",
                revision=self.revision,
                findings=list(self.blocking_findings),
            )


@dataclass(frozen=True, slots=True)
class BuildManifest:
    """Immutable description of what one build attempt promised to produce.

    Written before the build starts and compared afterwards. ``measured``
    stays ``None`` until the oracle fills it in, which keeps "what we expected"
    and "what we got" in separate fields instead of one mutable number.
    """

    build_id: str
    scope: CutScope
    attempt_no: int
    source_manifest_sha256: str
    reconciliation_revision: str
    fee_coverage: str
    currency_policy: str
    expected_unique_deals: int
    expected_identity_digest: str
    deal_watermark: int
    kafka_cut: Mapping[str, int] = field(default_factory=dict)
    created_at: datetime | None = None
    measured: Mapping[str, object] | None = None

    def as_wire(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "build_id": self.build_id,
            **self.scope.as_wire(),
            "attempt_no": self.attempt_no,
            "source_manifest_sha256": self.source_manifest_sha256,
            "reconciliation_revision": self.reconciliation_revision,
            "fee_coverage": self.fee_coverage,
            "currency_policy": self.currency_policy,
            "expected_unique_deals": self.expected_unique_deals,
            "expected_identity_digest_sha256": self.expected_identity_digest,
            "deal_watermark": self.deal_watermark,
            "kafka_cut": dict(sorted(self.kafka_cut.items())),
            "measured": dict(self.measured) if self.measured is not None else None,
        }
        return payload


def build_source_manifest(
    scope: CutScope,
    deals: Iterable[tuple[str, str, int]],
    *,
    captured_at: datetime,
    deal_watermark: int | None = None,
    kafka_cut: Mapping[str, int] | None = None,
    complete: bool = True,
    incomplete_reason: str | None = None,
    retention_horizon: datetime | None = None,
) -> SourceCutManifest:
    """Helper for building a manifest from ``(deal_id, hash, source_seq)`` rows."""
    identities = tuple(
        SourceDealIdentity(deal_id=deal_id, deal_payload_hash=deal_hash, source_seq=seq)
        for deal_id, deal_hash, seq in deals
    )
    watermark = deal_watermark
    if watermark is None:
        watermark = max((d.source_seq for d in identities), default=0)
    return SourceCutManifest(
        scope=scope,
        deal_watermark=watermark,
        captured_at=captured_at,
        deals=identities,
        kafka_cut=dict(kafka_cut or {}),
        complete=complete,
        incomplete_reason=incomplete_reason,
        retention_horizon=retention_horizon,
    )
