"""The Project 03 boundary (SYSTEM_DESIGN section 2.2).

Project 04 depends on Project 03 for two things and nothing else:

* **Read**: what has P03 actually published? Which scan, at which snapshot,
  with which Deal watermark, under which lease version.
* **Repair**: please run a full re-sync or a bounded backfill, under these
  preconditions, identified by this intent id.

Everything in these protocols is *additive* to P03 and must be contract-tested
on P03's side before it can be relied upon (section 2.2 "migration policy").
Project 04 never calls private internals and never writes P03's tables, so
these two narrow ports are the whole of the relationship.

Two deliberate design points:

* ``sync_status`` is used as a **pre-check** only. The authoritative read of
  the watermark and published scan happens inside the target-capture
  transaction (section 7.3), because a value fetched over a service call
  before the capture says nothing about the cut the capture actually saw.
  Section 7.2's danger note -- "W must not be read from an in-memory P03
  worker" -- is why.
* ``request_*`` returns a receipt, and a missing response is not a failure.
  A lost ACK leaves the outcome genuinely unknown, so the caller must query
  ``repair_intent_status`` with the same ``intent_id`` rather than retry blind
  (section 7.5, INV04-09).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from ..dto import RepairReceipt, SyncStatus


class P03ReadPort(Protocol):
    """Read-only view of what Project 03 has published."""

    def sync_status(self, source_id: str, epoch_id: str, entity: str) -> SyncStatus:
        """Current published scan, snapshot, Deal watermark, lease and epoch state."""
        ...

    def source_capability_profile(self, source_id: str) -> Mapping[str, Any]:
        """Whether this source is a simulator or a real SDK, and what it supports.

        Section 2.3 turns this into the set of classifications the audit is
        permitted to make, so a source that cannot promise stable snapshots
        yields diagnostics only and never a repair.
        """
        ...


class P03RepairPort(Protocol):
    """Fenced, idempotent orchestration requests (section 7.5)."""

    def request_full_resync(
        self,
        *,
        intent_id: str,
        source_id: str,
        entity: str,
        expected_epoch_id: str,
        expected_revision: str | None,
        expected_lease_version: int | None,
        approved_plan_hash: str,
        parameters: Mapping[str, Any],
    ) -> RepairReceipt:
        """Ask P03 to capture and atomically publish a fresh full snapshot.

        P03 must dedup on ``intent_id`` and re-verify the preconditions at its
        own commit boundary; a caller-side check is not a guard, because the
        world can change between the check and the commit (INV04-19).
        """
        ...

    def request_deal_backfill(
        self,
        *,
        intent_id: str,
        source_id: str,
        expected_epoch_id: str,
        expected_watermark: int | None,
        approved_plan_hash: str,
        parameters: Mapping[str, Any],
    ) -> RepairReceipt:
        """Ask P03 to replay a bounded Deal segment with full validation.

        Deal history is immutable, so this may only ever *add* missing rows;
        it must never rewrite a deal that already exists (section 8.1).
        """
        ...

    def repair_intent_status(self, intent_id: str) -> RepairReceipt | None:
        """The authoritative receipt, or None if P03 never saw the intent.

        ``None`` is the signal that a safe retry of the *same* intent id is
        permitted. Any receipt at all means the request landed and must be
        resolved by reading its outcome (section 7.6).
        """
        ...
