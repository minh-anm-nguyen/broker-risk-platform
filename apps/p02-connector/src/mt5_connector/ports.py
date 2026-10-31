"""The async read port and the value types that cross it (design section 4.1).

This is the contract Project 03 compiles against. Nothing in it mentions HTTP,
threads or a driver, which is the point: the consumer must not change when the
source behind it does.

Two choices in here are load-bearing:

* **A** :class:`ReadPlan` **instead of loose keyword arguments.** A plan is
  immutable and carries its own capability requirements, so an illegal
  combination - a cursor together with a changed filter, say - is rejected
  when the plan is built rather than assembled into a malformed upstream
  request (WF2-I-05).
* **A** :class:`CheckpointCandidate` **, not a checkpoint.** The connector
  reports the sequence a consumer *could* commit once it has persisted every
  page. Naming it a candidate is the type-level version of ADR-207: there is
  no method here that advances a watermark, so "the connector advanced the
  checkpoint" is not a mistake this interface permits.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any, Generic, Literal, Protocol, TypeVar

from .errors import ConfigurationError

T = TypeVar("T")

__all__ = [
    "Entity",
    "Capabilities",
    "SourceIdentity",
    "Provenance",
    "ReadPlan",
    "Page",
    "CheckpointCandidate",
    "ScanOutcome",
    "ScanResult",
    "MT5Client",
]


class Entity(StrEnum):
    ACCOUNTS = "accounts"
    POSITIONS = "positions"
    DEALS = "deals"


@dataclass(frozen=True, slots=True)
class Capabilities:
    """What the source declared it can do (design section 4.2).

    Every flag defaults to the *conservative* value. An absent field means
    ``unknown``, and unknown is treated as ``False``: assuming a capability
    the source never claimed is how a connector ends up asserting a gap-free
    sync it cannot support (WF2-J-10, WF2-R-07).
    """

    contract_version: str
    source_kind: Literal["simulator", "manager_sdk", "fake"]
    supported_entities: frozenset[Entity] = frozenset()
    snapshot_cursor: bool = False
    incremental_deals_after_seq: bool = False
    #: Whether one snapshot spans every entity atomically. The simulator
    #: declares this ``False`` and so does every real source known to this
    #: build; a consumer wanting a broker-wide cut must get it from Project 04
    #: (WF2-Q-05).
    stable_snapshot_across_entities: bool = False
    after_seq_is_exclusive: bool = False
    cursor_is_opaque: bool = True
    max_page_size: int = 100
    #: Verbatim from the source, for evidence files. Never branched on: a
    #: decision must go through a named flag above so that it is reviewable.
    raw: dict[str, Any] = field(default_factory=dict)

    def supports(self, entity: Entity) -> bool:
        return entity in self.supported_entities


@dataclass(frozen=True, slots=True)
class SourceIdentity:
    """Who the connector is talking to, and which incarnation of it.

    ``source_id`` is stable configuration. ``run_id`` and ``generation`` are
    the *source's* own identity for its dataset and are simulator semantics -
    both are ``None`` for a source that does not publish them.

    ``worker_epoch`` is the connector's own incarnation counter and has
    nothing to do with the source: it increments on every session spawn or
    reconnect so a completion from a replaced session can be recognised and
    dropped (ADR-206). Conflating it with ``generation`` would make a stale
    result from the *same* source generation look fresh.
    """

    source_kind: Literal["simulator", "manager_sdk", "fake"]
    source_id: str
    worker_epoch: int
    run_id: str | None = None
    generation: int | None = None
    contract_version: str | None = None

    @property
    def source_fingerprint(self) -> tuple[str, str | None, int | None]:
        """The triple that must not change inside one scan."""
        return (self.source_id, self.run_id, self.generation)


@dataclass(frozen=True, slots=True)
class Provenance:
    """Where one page came from. Attached to every emitted page.

    Carried per page rather than per scan so that a consumer merging two
    entities cannot lose the fact that they were read at different sequences
    (WF2-J-06).
    """

    source_id: str
    entity: Entity
    run_id: str | None
    generation: int | None
    snapshot_seq: int | None
    worker_epoch: int
    schema_version: str
    #: Digest of the normalised filter set. Two scans with different filters
    #: produce different digests, which is what keeps a checkpoint scoped to
    #: the filter that produced it (WF2-K-06).
    filter_digest: str


@dataclass(frozen=True, slots=True)
class ReadPlan:
    """An immutable description of one read.

    ``cursor`` and the filter fields are mutually exclusive by construction:
    once a cursor exists it is the whole request, because the cursor already
    encodes the filters the source pinned. Sending both is what WF2-I-05
    forbids, and :meth:`advance` is the only way to build the next page's
    plan, so the connector cannot assemble that illegal mix even by mistake.
    """

    entity: Entity
    limit: int
    cursor: str | None = None
    after_seq: int | None = None
    account_id: str | None = None
    symbol: str | None = None

    def __post_init__(self) -> None:
        if self.limit < 1:
            raise ConfigurationError(
                "read plan limit must be at least 1", details={"field": "limit"}
            )
        if self.cursor is not None:
            if self.cursor == "" or len(self.cursor) > 4096:
                raise ConfigurationError(
                    "cursor is empty or exceeds the permitted length",
                    details={"field": "cursor", "maximum": 4096},
                )
            if any(
                value is not None for value in (self.after_seq, self.account_id, self.symbol)
            ):
                raise ConfigurationError(
                    "a cursor must be sent alone: it already encodes the filters the "
                    "source pinned, and sending both would change the scan mid-flight",
                    details={"field": "cursor"},
                )
        if self.after_seq is not None and self.after_seq < 0:
            raise ConfigurationError(
                "after_seq must not be negative", details={"field": "after_seq"}
            )
        if self.entity is not Entity.DEALS and self.after_seq is not None:
            raise ConfigurationError(
                f"after_seq is only meaningful for deals, not {self.entity.value}",
                details={"field": "after_seq", "entity": self.entity.value},
            )

    @property
    def is_first_page(self) -> bool:
        return self.cursor is None

    def advance(self, cursor: str) -> "ReadPlan":
        """The plan for the next page of this same scan.

        Every filter is dropped, because the cursor carries them. This is the
        only supported way to paginate.
        """
        return replace(
            self,
            cursor=cursor,
            after_seq=None,
            account_id=None,
            symbol=None,
        )

    def filter_fields(self) -> dict[str, Any]:
        """The filters that identify this scan, for the provenance digest."""
        return {
            "entity": self.entity.value,
            "limit": self.limit,
            "after_seq": self.after_seq,
            "account_id": self.account_id,
            "symbol": self.symbol,
        }


@dataclass(frozen=True, slots=True)
class Page(Generic[T]):
    """One validated page.

    ``items`` is a tuple: a page is evidence of what the source said at a
    sequence, and a consumer must not be able to mutate it after the fact.
    """

    identity: SourceIdentity
    provenance: Provenance
    items: tuple[T, ...]
    next_cursor: str | None
    snapshot_seq: int | None
    requested_after_seq: int | None
    #: The highest ``source_seq`` seen in this page, when the source publishes
    #: one. Not a checkpoint: a page is not persisted data.
    observed_upper_bound: int | None
    page_index: int
    response_bytes: int

    @property
    def is_terminal(self) -> bool:
        """Whether this snapshot has no further pages.

        Terminal means "no more pages *in this snapshot*". It does not mean no
        more data will ever exist, and a consumer that reads it that way stops
        polling (WF2-Q-04).
        """
        return self.next_cursor is None


@dataclass(frozen=True, slots=True)
class CheckpointCandidate:
    """A sequence the consumer *may* commit once it has persisted everything.

    Produced only by a scan that reached its terminal page with every page
    validated. There is deliberately no method here that commits it: the
    connector holds no durable state and a fetched page is not a persisted
    page (ADR-207, WF2-Q-01).
    """

    source_id: str
    entity: Entity
    run_id: str | None
    generation: int | None
    snapshot_seq: int
    filter_digest: str
    pages_committed_by_source: int
    items_seen: int
    #: Spelled out in the value itself so it survives being logged, copied
    #: into a ticket, or read by someone who has not read ADR-207.
    note: str = (
        "candidate only: commit after every page of this scan is durably persisted"
    )


class ScanOutcome(StrEnum):
    COMPLETE = "complete"
    INCOMPLETE = "incomplete"
    RESYNC_REQUIRED = "resync_required"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class ScanResult:
    """How a scan ended.

    ``checkpoint_candidate`` is populated only for :attr:`ScanOutcome.COMPLETE`.
    Any other outcome leaves it ``None``, so a consumer cannot reach for a
    watermark that a partial scan produced (WF2-I-06, WF2-K-03).
    """

    outcome: ScanOutcome
    entity: Entity
    identity: SourceIdentity
    pages_seen: int
    items_seen: int
    snapshot_seq: int | None
    checkpoint_candidate: CheckpointCandidate | None = None
    error_code: str | None = None
    reason: str | None = None

    @property
    def complete(self) -> bool:
        return self.outcome is ScanOutcome.COMPLETE


class MT5Client(Protocol):
    """The async port. Implemented by :class:`mt5_connector.client.Connector`.

    Every method may raise any :class:`~mt5_connector.errors.ConnectorError`.
    ``CancelledError`` propagates untouched: it is the caller's lifecycle
    signal, not a source failure.
    """

    async def connect(self) -> SourceIdentity: ...

    async def capabilities(self) -> Capabilities: ...

    async def read_page(self, plan: ReadPlan, *, deadline_ms: int | None = None) -> Page[Any]: ...

    async def close(self) -> None: ...
