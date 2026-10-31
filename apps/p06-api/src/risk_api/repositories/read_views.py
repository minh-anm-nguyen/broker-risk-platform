"""Read-only access to the Project 03 / Project 04 projections.

Two things are modelled here deliberately:

1. **A snapshot boundary.** Multiple reads that belong to one report must come
   from one consistent target read, the way ``REPEATABLE READ READ ONLY``
   behaves. ``InMemoryReadViews.snapshot()`` freezes the world once and serves
   every subsequent read from that frozen copy.
2. **A bounded pool.** Checkout is a semaphore with a timeout, so pool
   exhaustion surfaces as ``DB_POOL_BUSY`` instead of an unbounded wait.

The read interface has no mutators. Test and harness mutations go through
``SourceWorld``, which the read views only ever read. That separation is what
lets a test prove Project 06 never wrote to a Project 03/04 table.
"""

from __future__ import annotations

import asyncio
import hashlib
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import AsyncIterator, Iterable, Mapping, Protocol, Sequence

from risk_engine import (
    AccountFact,
    CoverageDeclaration,
    DealFact,
    InstrumentSpec,
    PositionFact,
    QuoteFact,
    canonical_json,
    parse_rfc3339,
)

from ..contracts.problem_details import ApiProblem
from ..contracts.versioning import CapabilityHandshake
from ..faults import FaultPoint, FaultRegistry

__all__ = [
    "EpochStatusRow",
    "PublishedCutRow",
    "ReconciliationRow",
    "QuoteSnapshotRow",
    "CutRevisions",
    "ReadSnapshot",
    "ReadViews",
    "SourceWorld",
    "InMemoryReadViews",
    "DatabaseUnavailable",
]


class DatabaseUnavailable(RuntimeError):
    """The projection store could not serve a read."""


@dataclass(frozen=True, slots=True)
class EpochStatusRow:
    """One row of ``v_risk_epoch_status_v1``."""

    source_id: str
    epoch_id: str
    status: str
    revision: str


@dataclass(frozen=True, slots=True)
class PublishedCutRow:
    """One row of ``v_risk_published_cut_v1``."""

    epoch_id: str
    account_scan_id: str | None
    position_scan_id: str | None
    deal_watermark_seq: int | None
    publication_revision: str
    accounts_published: bool
    positions_published: bool
    deals_complete: bool
    published_at_utc: str


@dataclass(frozen=True, slots=True)
class ReconciliationRow:
    """One row of ``v_risk_reconciliation_v1``."""

    epoch_id: str
    scope_hash: str
    recon_revision: str
    blocking_reason_family: str | None


@dataclass(frozen=True, slots=True)
class QuoteSnapshotRow:
    snapshot_id: str
    captured_at_utc: str
    age_seconds: int
    max_age_seconds: int
    proven_against_position_scan: str | None


@dataclass(frozen=True, slots=True)
class CutRevisions:
    """The small revision vector the final validity check compares."""

    epoch_id: str
    epoch_status: str
    publication_revision: str
    recon_revision: str
    recon_blocking: bool
    quote_snapshot_id: str | None

    def differs_from(self, other: "CutRevisions") -> tuple[str, ...]:
        """Names of the fields that moved, for a typed conflict response.

        ``self`` is the pinned vector and ``other`` the freshly read one. A
        dimension the report never pinned is not compared: a report that did
        not value positions has no stake in a quote refresh, and failing it for
        one would be a false conflict.
        """
        changed: list[str] = []
        if self.epoch_id != other.epoch_id or self.epoch_status != other.epoch_status:
            changed.append("epoch")
        if self.publication_revision != other.publication_revision:
            changed.append("publication")
        if self.recon_revision != other.recon_revision or self.recon_blocking != other.recon_blocking:
            changed.append("reconciliation")
        if self.quote_snapshot_id is not None and self.quote_snapshot_id != other.quote_snapshot_id:
            changed.append("quote_snapshot")
        return tuple(changed)


class ReadSnapshot(Protocol):
    """One consistent target read. All reads for a report come from one of these."""

    async def epoch_status(self, source_id: str) -> EpochStatusRow | None: ...

    async def published_cut(self, source_id: str, epoch_id: str) -> PublishedCutRow | None: ...

    async def reconciliation(
        self, source_id: str, epoch_id: str, accounts: Sequence[str]
    ) -> ReconciliationRow | None: ...

    async def quote_snapshot(self, source_id: str, epoch_id: str) -> QuoteSnapshotRow | None: ...

    async def accounts(
        self, source_id: str, epoch_id: str, accounts: Sequence[str]
    ) -> tuple[AccountFact, ...]: ...

    async def positions(
        self,
        source_id: str,
        epoch_id: str,
        accounts: Sequence[str],
        *,
        scan_id: str,
        symbol: str | None = None,
        after_position_id: str | None = None,
        limit: int | None = None,
    ) -> tuple[PositionFact, ...]: ...

    async def deals(
        self,
        source_id: str,
        epoch_id: str,
        accounts: Sequence[str],
        *,
        start_utc: str,
        end_utc: str,
    ) -> tuple[DealFact, ...]: ...

    async def quotes(
        self, source_id: str, epoch_id: str, symbols: Iterable[str], *, snapshot_id: str
    ) -> tuple[QuoteFact, ...]: ...

    async def instruments(
        self, catalog_version: str, symbols: Iterable[str]
    ) -> tuple[InstrumentSpec, ...]: ...


class ReadViews(Protocol):
    """Port the application layer depends on."""

    def snapshot(self): ...

    async def current_revisions(
        self, source_id: str, epoch_id: str, accounts: Sequence[str]
    ) -> CutRevisions: ...

    async def health(self) -> bool: ...

    def capability(self) -> CapabilityHandshake: ...


# --------------------------------------------------------------------- world


@dataclass(slots=True)
class SourceWorld:
    """Mutable upstream state, owned by Project 01-04 in a real deployment.

    Only tests and the fault harness mutate this. Every mutator bumps the
    revision a report pinned, which is exactly what the final validity check
    is there to notice.
    """

    source_id: str
    epoch_id: str = "E1"
    epoch_status: str = "ACTIVE"
    epoch_revision: str = "epoch-r1"
    account_scan_id: str | None = "scan-A"
    position_scan_id: str | None = "scan-P"
    deal_watermark_seq: int | None = 1050
    publication_revision: str = "pub-R1"
    published_at_utc: str = "2026-10-30T00:00:00Z"
    accounts_published: bool = True
    positions_published: bool = True
    deals_complete: bool = True
    recon_revision: str = "recon-R10"
    recon_blocking_reason: str | None = None
    quote_snapshot_id: str | None = "quote-S1030"
    quote_captured_at_utc: str = "2026-10-30T00:00:00Z"
    quote_age_seconds: int = 5
    quote_max_age_seconds: int = 60
    quote_proven_against_scan: str | None = "scan-P"
    # Whether the upstream will vouch for the quote snapshot at all. Distinct
    # from whether that snapshot lines up with the pinned position scan.
    quote_cut_proven: bool = True
    fee_coverage: str = "NOT_AVAILABLE"
    symbol_catalog_version: str = "sim-usd-fx-v1"

    accounts: dict[str, AccountFact] = field(default_factory=dict)
    positions: dict[str, PositionFact] = field(default_factory=dict)
    deals: dict[str, DealFact] = field(default_factory=dict)
    quotes: dict[str, QuoteFact] = field(default_factory=dict)
    instruments: dict[str, InstrumentSpec] = field(default_factory=dict)

    # --------------------------------------------------------- test mutators

    def publish_position_scan(self, scan_id: str, *, revision: str | None = None) -> None:
        self.position_scan_id = scan_id
        self.publication_revision = revision or f"pub-{scan_id}"

    def advance_deal_watermark(self, seq: int, *, revision: str | None = None) -> None:
        self.deal_watermark_seq = seq
        self.publication_revision = revision or f"pub-W{seq}"

    def bump_recon_revision(self, revision: str, *, blocking_reason: str | None = None) -> None:
        self.recon_revision = revision
        self.recon_blocking_reason = blocking_reason

    def rotate_epoch(self, epoch_id: str, *, status: str = "ACTIVE") -> None:
        """Model a source generation reset: a brand new epoch replaces the old one."""
        self.epoch_id = epoch_id
        self.epoch_status = status
        self.epoch_revision = f"epoch-{epoch_id}"

    def refresh_quote_snapshot(self, snapshot_id: str, *, age_seconds: int = 1) -> None:
        self.quote_snapshot_id = snapshot_id
        self.quote_age_seconds = age_seconds
        for symbol, quote in list(self.quotes.items()):
            self.quotes[symbol] = replace(quote, snapshot_id=snapshot_id)

    def checksum(self) -> str:
        """Digest of the whole upstream state.

        A report must never change this; the integration tests compare it
        before and after every request.
        """
        manifest = {
            "source_id": self.source_id,
            "epoch": [self.epoch_id, self.epoch_status, self.epoch_revision],
            "publication": [
                self.published_at_utc,
                self.account_scan_id,
                self.position_scan_id,
                self.deal_watermark_seq,
                self.publication_revision,
                self.accounts_published,
                self.positions_published,
                self.deals_complete,
            ],
            "recon": [self.recon_revision, self.recon_blocking_reason],
            "quotes_meta": [self.quote_snapshot_id, self.quote_age_seconds],
            "accounts": sorted(
                [a.account_id, a.currency, a.presence, a.published_scan_id] for a in self.accounts.values()
            ),
            "positions": sorted(
                [
                    p.position_id,
                    p.account_id,
                    p.symbol,
                    p.side,
                    format(p.lots, "f"),
                    format(p.open_price, "f"),
                    p.source_status,
                    p.presence,
                ]
                for p in self.positions.values()
            ),
            "deals": sorted(
                [
                    d.deal_id,
                    d.account_id,
                    str(d.source_seq),
                    format(d.gross_profit, "f"),
                    format(d.commission, "f"),
                    format(d.swap, "f"),
                    d.executed_at_utc,
                ]
                for d in self.deals.values()
            ),
            "quotes": sorted(
                [q.symbol, format(q.bid, "f"), format(q.ask, "f"), q.snapshot_id]
                for q in self.quotes.values()
            ),
        }
        return "sha256:" + hashlib.sha256(canonical_json(manifest)).hexdigest()


@dataclass(frozen=True, slots=True)
class _FrozenWorld:
    """An immutable copy of the world, taken once per snapshot."""

    source_id: str
    epoch: EpochStatusRow
    cut: PublishedCutRow
    recon: ReconciliationRow
    quote: QuoteSnapshotRow | None
    accounts: tuple[AccountFact, ...]
    positions: tuple[PositionFact, ...]
    deals: tuple[DealFact, ...]
    quotes: tuple[QuoteFact, ...]
    instruments: tuple[InstrumentSpec, ...]
    symbol_catalog_version: str
    fee_coverage: str


def _freeze(world: SourceWorld) -> _FrozenWorld:
    return _FrozenWorld(
        source_id=world.source_id,
        epoch=EpochStatusRow(
            source_id=world.source_id,
            epoch_id=world.epoch_id,
            status=world.epoch_status,
            revision=world.epoch_revision,
        ),
        cut=PublishedCutRow(
            epoch_id=world.epoch_id,
            account_scan_id=world.account_scan_id,
            position_scan_id=world.position_scan_id,
            deal_watermark_seq=world.deal_watermark_seq,
            publication_revision=world.publication_revision,
            accounts_published=world.accounts_published,
            positions_published=world.positions_published,
            deals_complete=world.deals_complete,
            published_at_utc=world.published_at_utc,
        ),
        recon=ReconciliationRow(
            epoch_id=world.epoch_id,
            scope_hash="*",
            recon_revision=world.recon_revision,
            blocking_reason_family=world.recon_blocking_reason,
        ),
        quote=(
            None
            if world.quote_snapshot_id is None
            else QuoteSnapshotRow(
                snapshot_id=world.quote_snapshot_id,
                captured_at_utc=world.quote_captured_at_utc,
                age_seconds=world.quote_age_seconds,
                max_age_seconds=world.quote_max_age_seconds,
                proven_against_position_scan=world.quote_proven_against_scan,
            )
        ),
        accounts=tuple(world.accounts.values()),
        positions=tuple(world.positions.values()),
        deals=tuple(world.deals.values()),
        quotes=tuple(world.quotes.values()),
        instruments=tuple(world.instruments.values()),
        symbol_catalog_version=world.symbol_catalog_version,
        fee_coverage=world.fee_coverage,
    )


class _InMemorySnapshot:
    """Serves every read from one frozen copy, under a statement budget."""

    def __init__(
        self,
        frozen: _FrozenWorld,
        *,
        statement_timeout_ms: int,
        faults: FaultRegistry,
        counters: dict[str, int],
    ) -> None:
        self._frozen = frozen
        self._statement_timeout_ms = statement_timeout_ms
        self._faults = faults
        self._counters = counters

    async def _statement(self, name: str) -> None:
        """Account for one statement and apply any armed statement fault."""
        self._counters[name] = self._counters.get(name, 0) + 1
        self._counters["statements"] = self._counters.get("statements", 0) + 1
        if self._faults.active(FaultPoint.DB_STATEMENT_TIMEOUT) is not None:
            raise DatabaseUnavailable(f"statement {name} exceeded {self._statement_timeout_ms} ms")
        if self._faults.active(FaultPoint.DB_FAIL_AFTER_SNAPSHOT) is not None:
            raise DatabaseUnavailable(f"projection read {name} failed")
        await asyncio.sleep(0)

    async def epoch_status(self, source_id: str) -> EpochStatusRow | None:
        await self._statement("epoch_status")
        if source_id != self._frozen.source_id:
            return None
        return self._frozen.epoch

    async def published_cut(self, source_id: str, epoch_id: str) -> PublishedCutRow | None:
        await self._statement("published_cut")
        if source_id != self._frozen.source_id or epoch_id != self._frozen.epoch.epoch_id:
            return None
        return self._frozen.cut

    async def reconciliation(
        self, source_id: str, epoch_id: str, accounts: Sequence[str]
    ) -> ReconciliationRow | None:
        await self._statement("reconciliation")
        if source_id != self._frozen.source_id or epoch_id != self._frozen.epoch.epoch_id:
            return None
        return self._frozen.recon

    async def quote_snapshot(self, source_id: str, epoch_id: str) -> QuoteSnapshotRow | None:
        await self._statement("quote_snapshot")
        if source_id != self._frozen.source_id or epoch_id != self._frozen.epoch.epoch_id:
            return None
        return self._frozen.quote

    async def accounts(
        self, source_id: str, epoch_id: str, accounts: Sequence[str]
    ) -> tuple[AccountFact, ...]:
        await self._statement("accounts")
        wanted = frozenset(accounts)
        return tuple(
            sorted(
                (row for row in self._frozen.accounts if row.account_id in wanted),
                key=lambda row: row.account_id,
            )
        )

    async def positions(
        self,
        source_id: str,
        epoch_id: str,
        accounts: Sequence[str],
        *,
        scan_id: str,
        symbol: str | None = None,
        after_position_id: str | None = None,
        limit: int | None = None,
    ) -> tuple[PositionFact, ...]:
        await self._statement("positions")
        wanted = frozenset(accounts)
        rows = [
            row
            for row in self._frozen.positions
            if row.account_id in wanted
            and (row.published_scan_id is None or row.published_scan_id == scan_id)
            and (symbol is None or row.symbol == symbol)
        ]
        # Keyset ordering on position_id; never OFFSET.
        rows.sort(key=lambda row: row.position_id)
        if after_position_id is not None:
            rows = [row for row in rows if row.position_id > after_position_id]
        if limit is not None:
            rows = rows[:limit]
        return tuple(rows)

    async def deals(
        self,
        source_id: str,
        epoch_id: str,
        accounts: Sequence[str],
        *,
        start_utc: str,
        end_utc: str,
    ) -> tuple[DealFact, ...]:
        await self._statement("deals")
        wanted = frozenset(accounts)
        start = parse_rfc3339(start_utc, field="start_utc")
        end = parse_rfc3339(end_utc, field="end_utc")
        rows = [
            row
            for row in self._frozen.deals
            if row.account_id in wanted
            and start <= parse_rfc3339(row.executed_at_utc, field="executed_at_utc") < end
        ]
        rows.sort(key=lambda row: (row.source_seq, row.deal_id))
        return tuple(rows)

    async def quotes(
        self, source_id: str, epoch_id: str, symbols: Iterable[str], *, snapshot_id: str
    ) -> tuple[QuoteFact, ...]:
        await self._statement("quotes")
        wanted = frozenset(symbols)
        return tuple(
            sorted(
                (
                    row
                    for row in self._frozen.quotes
                    if row.symbol in wanted and row.snapshot_id == snapshot_id
                ),
                key=lambda row: row.symbol,
            )
        )

    async def instruments(
        self, catalog_version: str, symbols: Iterable[str]
    ) -> tuple[InstrumentSpec, ...]:
        await self._statement("instruments")
        if catalog_version != self._frozen.symbol_catalog_version:
            return ()
        wanted = frozenset(symbols)
        return tuple(
            sorted(
                (row for row in self._frozen.instruments if row.symbol in wanted),
                key=lambda row: row.symbol,
            )
        )

    @property
    def fee_coverage(self) -> str:
        return self._frozen.fee_coverage

    @property
    def symbol_catalog_version(self) -> str:
        return self._frozen.symbol_catalog_version


class InMemoryReadViews:
    """Reference implementation of the read port.

    Deterministic, so a race test can schedule it exactly; and bounded, so a
    pool-pressure test sees the same refusal a real pool would produce.
    """

    def __init__(
        self,
        world: SourceWorld,
        *,
        pool_size: int = 5,
        pool_timeout_seconds: float = 0.3,
        statement_timeout_ms: int = 1000,
        faults: FaultRegistry | None = None,
    ) -> None:
        self._world = world
        self._pool = asyncio.Semaphore(pool_size)
        self._pool_size = pool_size
        self._pool_timeout = pool_timeout_seconds
        self._statement_timeout_ms = statement_timeout_ms
        self._faults = faults or FaultRegistry()
        self.counters: dict[str, int] = {}
        self.open_sessions = 0
        self.peak_open_sessions = 0
        self.leaked_sessions = 0

    # ------------------------------------------------------------- snapshot

    @asynccontextmanager
    async def snapshot(self) -> AsyncIterator[_InMemorySnapshot]:
        """Open one read-only session and freeze the world for its lifetime."""
        if self._faults.active(FaultPoint.DB_POOL_EXHAUSTED) is not None:
            raise ApiProblem("DB_POOL_BUSY", "database pool is saturated")
        try:
            await asyncio.wait_for(self._pool.acquire(), timeout=self._pool_timeout)
        except TimeoutError as exc:
            self.counters["pool_timeouts"] = self.counters.get("pool_timeouts", 0) + 1
            raise ApiProblem("DB_POOL_BUSY", "database pool checkout timed out") from exc

        if self._faults.active(FaultPoint.DB_FAIL_BEFORE_SNAPSHOT) is not None:
            self._pool.release()
            raise DatabaseUnavailable("could not open a read snapshot")

        self.open_sessions += 1
        self.peak_open_sessions = max(self.peak_open_sessions, self.open_sessions)
        self.counters["snapshots"] = self.counters.get("snapshots", 0) + 1
        frozen = _freeze(self._world)
        snapshot = _InMemorySnapshot(
            frozen,
            statement_timeout_ms=self._statement_timeout_ms,
            faults=self._faults,
            counters=self.counters,
        )
        try:
            yield snapshot
        finally:
            # Always returned, including on cancellation: no idle-in-transaction leak.
            self.open_sessions -= 1
            self._pool.release()

    # ------------------------------------------------------------ revisions

    async def current_revisions(
        self, source_id: str, epoch_id: str, accounts: Sequence[str]
    ) -> CutRevisions:
        """Re-read the revision vector outside the pinned snapshot."""
        if self._faults.active(FaultPoint.DB_FAIL_FINAL_CHECK) is not None:
            raise DatabaseUnavailable("final validity check could not reach the projection store")
        self.counters["final_checks"] = self.counters.get("final_checks", 0) + 1
        world = self._world
        return CutRevisions(
            epoch_id=world.epoch_id,
            epoch_status=world.epoch_status,
            publication_revision=world.publication_revision,
            recon_revision=world.recon_revision,
            recon_blocking=world.recon_blocking_reason is not None,
            quote_snapshot_id=world.quote_snapshot_id,
        )

    # --------------------------------------------------------------- health

    async def health(self) -> bool:
        if self._faults.active(FaultPoint.DB_FAIL_BEFORE_SNAPSHOT) is not None:
            return False
        return True

    def capability(self) -> CapabilityHandshake:
        return CapabilityHandshake(
            read_views={
                "v_risk_epoch_status": "v1",
                "v_risk_published_cut": "v1",
                "v_risk_reconciliation": "v1",
            },
            provider_kind="in-memory-reference",
        )

    # -------------------------------------------------------------- helpers

    @property
    def world(self) -> SourceWorld:
        """The mutable world, for tests and the fault harness only."""
        return self._world

    @property
    def pool_size(self) -> int:
        return self._pool_size

    def coverage_for(
        self, cut: PublishedCutRow, quote: QuoteSnapshotRow | None
    ) -> CoverageDeclaration:
        """Translate published-cut rows into what the engine may rely on.

        ``quote_cut_proven`` answers only "may this snapshot be used to value a
        position". Whether it sits on the same logical cut as the deals is a
        separate decision, made by the capturer as the cut consistency.
        """
        quote_proven = quote is not None and self._world.quote_cut_proven
        return CoverageDeclaration(
            deals_complete=cut.deals_complete,
            fee_coverage="COVERED" if self._world.fee_coverage == "COVERED" else "NOT_AVAILABLE",
            quote_cut_proven=quote_proven,
            accounts_published=cut.accounts_published,
            positions_published=cut.positions_published,
            quote_max_age_seconds=None if quote is None else quote.max_age_seconds,
            quote_age_seconds=None if quote is None else quote.age_seconds,
        )


def build_demo_world(
    *,
    source_id: str = "sim-broker-a",
    now_utc: str | None = None,
) -> SourceWorld:
    """A small, fully proven world matching golden fixture ``FX-READY-001``."""
    captured = now_utc or datetime(2026, 10, 30, tzinfo=timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )
    world = SourceWorld(source_id=source_id, quote_captured_at_utc=captured)
    world.instruments["EURUSD"] = InstrumentSpec(
        symbol="EURUSD",
        base_currency="EUR",
        quote_currency="USD",
        profit_currency="USD",
        calc_mode="FOREX_LINEAR",
        contract_size=Decimal("100000"),
        tick_size=Decimal("0.00001"),
        volume_step=Decimal("0.01"),
        spec_version="sim-usd-fx-v1",
    )
    world.accounts["00042"] = AccountFact("00042", "USD", "PRESENT", "scan-A")
    world.positions["P-0001"] = PositionFact(
        position_id="P-0001",
        account_id="00042",
        symbol="EURUSD",
        side="BUY",
        lots=Decimal("0.5"),
        open_price=Decimal("1.1000"),
        source_status="OPEN",
        presence="PRESENT",
        published_scan_id="scan-P",
    )
    world.deals["D-0001"] = DealFact(
        deal_id="D-0001",
        account_id="00042",
        source_seq=1050,
        entry_kind="OUT",
        gross_profit=Decimal("60.00"),
        commission=Decimal("-3.50"),
        swap=Decimal("-0.50"),
        executed_at_utc="2026-10-15T10:00:00Z",
    )
    world.quotes["EURUSD"] = QuoteFact(
        symbol="EURUSD",
        bid=Decimal("1.1005"),
        ask=Decimal("1.1007"),
        market_time_utc=captured,
        quote_id="q-0001",
        snapshot_id="quote-S1030",
    )
    return world


def world_table_checksums(world: SourceWorld) -> Mapping[str, str]:
    """Per-table checksums, used by the no-writes oracle."""
    def _digest(rows: Iterable[object]) -> str:
        return "sha256:" + hashlib.sha256(
            canonical_json([repr(row) for row in sorted(rows, key=repr)])
        ).hexdigest()

    return {
        "accounts": _digest(world.accounts.values()),
        "positions": _digest(world.positions.values()),
        "deals": _digest(world.deals.values()),
        "quotes": _digest(world.quotes.values()),
    }
