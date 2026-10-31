"""A fake MT5 source with simulator semantics, fault injection and an oracle.

Stands in for Project 01 behind Project 02 and implements
:class:`~trading_sync.application.ports.SyncSource`, so the orchestration under
test is the real one.

Two properties make it usable as a test oracle, and both matter.

**The oracle does not read through the faulted path.** ``oracle_deals`` reads
the internal event log directly, while ``iter_deals`` goes through paging,
cursors and the fault layer. If the oracle used the same endpoint, a fault that
drops records would corrupt the expected answer as well and the test would pass
while the system lost data (design 15.2, WF3-141).

**Injected faults are counted.** ``fault_firings`` records what actually fired,
so a test can assert its injector ran. A crash-window test whose injector never
triggered proves nothing and must not report PASS (WF3-241, design 15.6).

Simulator semantics reproduced here: a monotonic event sequence shared by deal
and non-deal events, so deal sequences legitimately have gaps (INV-11); stable
snapshots with opaque cursors and a TTL; ``after_seq`` exclusive; and
``(run_id, generation)`` identity that changes on reset.
"""

from __future__ import annotations

import itertools
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, AsyncIterator, Iterable

from trading_sync.domain.entities import AccountRecord, DealRecord, PositionRecord
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity, RawPage, SourceIdentity, SourceKind
from trading_sync.errors import (
    AuthenticationError,
    AuthorizationError,
    OverloadedError,
    SchemaError,
    SnapshotExpiredError,
    SourceEpochChangedError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)

__all__ = ["Fault", "FakeSimulatorSource"]


@dataclass
class Fault:
    """One injected fault.

    ``mode`` names the behaviour; ``at_page`` is the zero-based page index it
    applies to, and ``entity`` narrows it to one entity. ``times`` bounds how
    often it fires so a test can model a fault that heals.
    """

    mode: str
    at_page: int = 0
    entity: Entity | None = None
    times: int = 1
    #: Free-form knob, used by a few modes (e.g. the replacement value).
    payload: Any = None

    def matches(self, entity: Entity, page_index: int) -> bool:
        if self.times <= 0:
            return False
        if self.entity is not None and self.entity is not entity:
            return False
        return self.at_page == page_index


@dataclass
class _DealEvent:
    source_seq: int
    payload: dict[str, Any]


class FakeSimulatorSource:
    """Mutable in-memory source with simulator-compatible read semantics."""

    def __init__(
        self,
        *,
        source_id: str = "sim_local",
        run_id: str = "run_0001",
        generation: int = 0,
        page_size: int = 500,
        source_kind: SourceKind = SourceKind.SIMULATOR,
        supports_incremental_deals: bool = True,
    ) -> None:
        self.source_id = source_id
        self.run_id = run_id
        self.generation = generation
        self.page_size = page_size
        self.source_kind = source_kind
        self.supports_incremental_deals = supports_incremental_deals

        self._seq = itertools.count(1)
        self._committed_seq = 0
        self._deals: list[_DealEvent] = []
        self._accounts: dict[str, dict[str, Any]] = {}
        self._positions: dict[str, dict[str, Any]] = {}

        self.faults: list[Fault] = []
        self.fault_firings: Counter[str] = Counter()
        #: Pages actually served, for backpressure assertions.
        self.pages_served: Counter[str] = Counter()
        self.meta_reads = 0

    # -- source identity ------------------------------------------------------

    @property
    def committed_seq(self) -> int:
        return self._committed_seq

    @property
    def epoch_label(self) -> str:
        return f"{self.run_id}:G{self.generation}"

    async def read_meta(self, source_id: str) -> SourceIdentity:
        self.meta_reads += 1
        self._maybe_fire("meta", Entity.DEALS, 0)
        return SourceIdentity(
            source_id=self.source_id,
            source_kind=self.source_kind,
            run_id=self.run_id,
            generation=self.generation,
            committed_seq=self._committed_seq,
            supports_incremental_deals=self.supports_incremental_deals,
        )

    def reset(self, *, run_id: str | None = None, generation: int | None = None) -> None:
        """Simulate a simulator restart or reset.

        The event log is cleared and identity changes, which is what makes the
        old watermark meaningless for the new epoch (design 8.3).
        """
        if run_id is not None:
            self.run_id = run_id
        if generation is not None:
            self.generation = generation
        else:
            self.generation += 1
        self._seq = itertools.count(1)
        self._committed_seq = 0
        self._deals.clear()
        self._positions.clear()
        self._accounts.clear()

    # -- content mutation -----------------------------------------------------

    def advance_events(self, count: int = 1) -> int:
        """Consume sequence numbers without creating deals.

        This is what produces legitimate gaps in the deal sequence; a sync that
        reports those as missing data is raising a false alarm (INV-11,
        WF3-025, fixture G03).
        """
        for _ in range(count):
            self._committed_seq = next(self._seq)
        return self._committed_seq

    def add_deal(
        self,
        deal_id: str,
        *,
        account_id: str = "A1",
        position_id: str | None = "P1",
        symbol: str = "EURUSD",
        entry_kind: str = "IN",
        side: str = "BUY",
        volume_lots: str = "0.10",
        price: str = "1.10000",
        realized_pnl_gross: str = "0.00",
        commission: str = "-2.50",
        swap: str = "0.00",
        executed_at: str = "2026-01-01T00:00:00+00:00",
    ) -> int:
        """Append a deal, consuming one sequence number. Returns its seq."""
        seq = next(self._seq)
        self._committed_seq = seq
        self._deals.append(
            _DealEvent(
                source_seq=seq,
                payload={
                    "deal_id": deal_id,
                    "source_seq": seq,
                    "account_id": account_id,
                    "position_id": position_id,
                    "symbol": symbol,
                    "entry_kind": entry_kind,
                    "side": side,
                    "volume_lots": volume_lots,
                    "price": price,
                    "realized_pnl_gross": realized_pnl_gross,
                    "commission": commission,
                    "swap": swap,
                    "executed_at": executed_at,
                },
            )
        )
        return seq

    def amend_deal(self, deal_id: str, **changes: Any) -> None:
        """Rewrite a stored deal in place, keeping its id and sequence.

        A source doing this is a contract violation, and reproducing it is the
        only way to test that the sink refuses it instead of overwriting money
        (WF3-028, fixture G10).
        """
        for event in self._deals:
            if event.payload["deal_id"] == deal_id:
                event.payload.update(changes)
                return
        raise KeyError(deal_id)

    def set_account(
        self,
        account_id: str,
        *,
        currency: str = "USD",
        balance: str = "1000.00",
        credit: str = "0.00",
    ) -> int:
        self._accounts[account_id] = {
            "account_id": account_id,
            "currency": currency,
            "balance": balance,
            "credit": credit,
        }
        return self.advance_events()

    def set_position(
        self,
        position_id: str,
        *,
        account_id: str = "A1",
        symbol: str = "EURUSD",
        side: str = "BUY",
        volume_lots: str = "0.10",
        open_price: str = "1.10000",
        source_status: str = "OPEN",
    ) -> int:
        self._positions[position_id] = {
            "position_id": position_id,
            "account_id": account_id,
            "symbol": symbol,
            "side": side,
            "volume_lots": volume_lots,
            "open_price": open_price,
            "source_status": source_status,
        }
        return self.advance_events()

    def remove_position(self, position_id: str) -> int:
        """Drop a position from the current listing without any tombstone.

        The source says nothing about why it went. Inferring CLOSED from this
        is exactly what INV-08 forbids (WF3-073).
        """
        self._positions.pop(position_id, None)
        return self.advance_events()

    def remove_account(self, account_id: str) -> int:
        self._accounts.pop(account_id, None)
        return self.advance_events()

    # -- the oracle -----------------------------------------------------------

    def oracle_deals(self, *, after_seq: int = 0, snapshot_seq: int | None = None) -> dict[str, str]:
        """Ground truth: deal id to canonical payload hash, read from the log.

        Deliberately bypasses paging, cursors and faults. An oracle computed
        through the path under test cannot detect that path dropping records.
        """
        bound = self._committed_seq if snapshot_seq is None else snapshot_seq
        return {
            event.payload["deal_id"]: DealRecord.from_payload(event.payload).payload_hash
            for event in self._deals
            if after_seq < event.source_seq <= bound
        }

    def oracle_accounts(self) -> dict[str, str]:
        return {
            account_id: AccountRecord.from_payload(payload).payload_hash
            for account_id, payload in self._accounts.items()
        }

    def oracle_positions(self) -> dict[str, str]:
        return {
            position_id: PositionRecord.from_payload(payload).payload_hash
            for position_id, payload in self._positions.items()
        }

    def oracle_deal_money(self, deal_id: str) -> Decimal:
        for event in self._deals:
            if event.payload["deal_id"] == deal_id:
                return Decimal(event.payload["commission"])
        raise KeyError(deal_id)

    # -- fault injection ------------------------------------------------------

    def inject(self, *faults: Fault) -> None:
        self.faults.extend(faults)

    def clear_faults(self) -> None:
        """Disarm every fault.

        Called between cases by the harness: a rule surviving into the next
        case makes that case unreproducible (WF3-248).
        """
        self.faults.clear()
        self.fault_firings.clear()

    def fired(self, mode: str) -> int:
        return self.fault_firings[mode]

    def assert_fired(self, mode: str) -> None:
        if not self.fault_firings[mode]:
            raise AssertionError(
                f"fault {mode!r} never fired; this run is NOT_CONCLUSIVE, not a pass"
            )

    def _maybe_fire(self, mode_filter: str, entity: Entity, page_index: int) -> Fault | None:
        """Find and consume a fault applicable at this point."""
        for fault in self.faults:
            if fault.mode == mode_filter and fault.matches(entity, page_index):
                fault.times -= 1
                self.fault_firings[fault.mode] += 1
                return fault
        return None

    def _fault_for(self, entity: Entity, page_index: int) -> Fault | None:
        for fault in self.faults:
            if fault.mode == "meta":
                continue
            if fault.matches(entity, page_index):
                fault.times -= 1
                self.fault_firings[fault.mode] += 1
                return fault
        return None

    # -- paged reads ----------------------------------------------------------

    def iter_deals(self, source_id: str, after_seq: int) -> AsyncIterator[RawPage]:
        """Deals above ``after_seq``, exclusive, as of a stable snapshot."""
        snapshot_seq = self._committed_seq
        items = [
            dict(event.payload)
            for event in self._deals
            if after_seq < event.source_seq <= snapshot_seq
        ]
        return self._paginate(Entity.DEALS, items, snapshot_seq)

    def iter_accounts(self, source_id: str) -> AsyncIterator[RawPage]:
        snapshot_seq = self._committed_seq
        items = [dict(payload) for payload in self._accounts.values()]
        return self._paginate(Entity.ACCOUNTS, items, snapshot_seq)

    def iter_positions(self, source_id: str) -> AsyncIterator[RawPage]:
        snapshot_seq = self._committed_seq
        items = [dict(payload) for payload in self._positions.values()]
        return self._paginate(Entity.POSITIONS, items, snapshot_seq)

    async def _paginate(
        self, entity: Entity, items: list[dict[str, Any]], snapshot_seq: int
    ) -> AsyncIterator[RawPage]:
        """Serve ``items`` as pages, applying any fault armed for each index.

        One page is produced per iteration and nothing is prepared ahead, so a
        consumer that stops early leaves no work in flight. That mirrors the
        no-prefetch rule the port requires (design 2.2).
        """
        chunks = [
            items[offset : offset + self.page_size]
            for offset in range(0, len(items), self.page_size)
        ] or [[]]

        index = 0
        request_cursor: str | None = None
        while index < len(chunks):
            chunk = [dict(item) for item in chunks[index]]
            is_last = index == len(chunks) - 1
            next_cursor = None if is_last else self._cursor(entity, snapshot_seq, index + 1)
            page = RawPage(
                source_id=self.source_id,
                entity=entity,
                run_id=self.run_id,
                generation=self.generation,
                snapshot_seq=snapshot_seq,
                page_index=index,
                items=tuple(chunk),
                cursor_present=True,
                next_cursor=next_cursor,
                filter_scope=FILTER_UNFILTERED,
                request_cursor=request_cursor,
            )
            fault = self._fault_for(entity, index)
            if fault is not None:
                self._raise_transport_fault(fault)
                page = self._apply_content_fault(fault, page, chunks, index)

            self.pages_served[str(entity)] += 1
            yield page
            if page.next_cursor is None:
                return
            request_cursor = page.next_cursor
            index += 1

    def _cursor(self, entity: Entity, snapshot_seq: int, index: int) -> str:
        return f"cur:{entity}:{self.run_id}:{self.generation}:{snapshot_seq}:{index}"

    def _raise_transport_fault(self, fault: Fault) -> None:
        if fault.mode == "timeout":
            raise UpstreamTimeoutError("connector timed out", page_index=fault.at_page)
        if fault.mode == "snapshot_expired":
            raise SnapshotExpiredError("cursor expired", page_index=fault.at_page)
        if fault.mode == "overloaded":
            raise OverloadedError(
                "connector is overloaded",
                retry_after_seconds=fault.payload if fault.payload else 1.0,
            )
        if fault.mode == "unavailable":
            raise UpstreamUnavailableError("connector unavailable")
        if fault.mode == "unauthenticated":
            raise AuthenticationError("connector rejected credentials")
        if fault.mode == "forbidden":
            raise AuthorizationError("connector denied access")
        if fault.mode == "invalid_json":
            raise SchemaError("connector returned an undecodable body")
        if fault.mode == "epoch_changed_error":
            # The connector noticed the reset itself and reported it as a typed
            # error, rather than us inferring it from page provenance.
            raise SourceEpochChangedError(
                "connector reports the source epoch changed",
                page_index=fault.at_page,
            )

    def _apply_content_fault(
        self,
        fault: Fault,
        page: RawPage,
        chunks: list[list[dict[str, Any]]],
        index: int,
    ) -> RawPage:
        """Return ``page`` corrupted according to ``fault.mode``.

        These are the faults a validating client is expected to *catch*, with
        two deliberate exceptions. ``false_terminal`` and ``drop_record``
        produce a page chain that validates perfectly while omitting data: no
        client-side rule can detect them, and only the oracle can. They exist
        to keep that limitation visible and tested (design 6.6, WF3-141).
        """
        changes: dict[str, Any] = {}
        items = [dict(item) for item in page.items]

        if fault.mode == "mixed_snapshot":
            changes["snapshot_seq"] = page.snapshot_seq + 1
        elif fault.mode == "epoch_change":
            changes["run_id"] = fault.payload or "run_9999"
            changes["generation"] = page.generation + 1
        elif fault.mode == "cursor_cycle":
            changes["next_cursor"] = page.request_cursor or self._cursor(
                page.entity, page.snapshot_seq, index
            )
        elif fault.mode == "drop_cursor_field":
            changes["cursor_present"] = False
            changes["next_cursor"] = None
        elif fault.mode == "oversized_page":
            padding = (self.page_size + 1) - len(items)
            template = items[0] if items else {}
            items = items + [dict(template) for _ in range(max(padding, 1))]
        elif fault.mode == "false_terminal":
            # Claims the snapshot ended while later pages still exist.
            changes["next_cursor"] = None
        elif fault.mode == "drop_record" and items:
            items.pop(0)
        elif fault.mode == "duplicate_identical" and items:
            items.append(dict(items[0]))
        elif fault.mode == "duplicate_conflict" and items:
            conflicting = dict(items[0])
            conflicting["commission"] = fault.payload or "-99.99"
            items.append(conflicting)
        elif fault.mode == "seq_backwards" and len(items) >= 2:
            items[0], items[1] = items[1], items[0]
        elif fault.mode == "seq_above_snapshot" and items:
            items[-1] = dict(items[-1]) | {"source_seq": page.snapshot_seq + 10}
        elif fault.mode == "float_money" and items:
            items[0] = dict(items[0]) | {"commission": 0.1}
        elif fault.mode == "nonfinite_money" and items:
            items[0] = dict(items[0]) | {"commission": "NaN"}
        elif fault.mode == "excess_scale" and items:
            items[0] = dict(items[0]) | {"commission": "-0.0000000000001"}
        elif fault.mode == "naive_timestamp" and items:
            items[0] = dict(items[0]) | {"executed_at": "2026-01-01T00:00:00"}
        elif fault.mode == "missing_field" and items:
            stripped = dict(items[0])
            stripped.pop(fault.payload or "commission", None)
            items[0] = stripped
        elif fault.mode == "unknown_side" and items:
            items[0] = dict(items[0]) | {"side": "LONG"}
        elif fault.mode == "blank_id" and items:
            key = "deal_id" if page.entity is Entity.DEALS else "account_id"
            items[0] = dict(items[0]) | {key: "   "}
        elif fault.mode == "bad_currency" and items:
            items[0] = dict(items[0]) | {"currency": "US"}
        elif fault.mode == "wrong_entity":
            changes["entity"] = (
                Entity.ACCOUNTS if page.entity is not Entity.ACCOUNTS else Entity.DEALS
            )
        elif fault.mode == "filter_scope":
            changes["filter_scope"] = fault.payload or "status=OPEN"

        changes["items"] = tuple(items)
        return _replace_page(page, changes)


def _replace_page(page: RawPage, changes: dict[str, Any]) -> RawPage:
    fields = {
        "source_id": page.source_id,
        "entity": page.entity,
        "run_id": page.run_id,
        "generation": page.generation,
        "snapshot_seq": page.snapshot_seq,
        "page_index": page.page_index,
        "items": page.items,
        "cursor_present": page.cursor_present,
        "next_cursor": page.next_cursor,
        "filter_scope": page.filter_scope,
        "request_cursor": page.request_cursor,
    }
    fields.update(changes)
    return RawPage(**fields)
