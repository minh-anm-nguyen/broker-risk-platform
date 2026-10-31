"""Durable audit of financially sensitive reads.

The contract (design doc sections 8.1 and 15):

* A ``READY`` financial payload is only sent after a durable receipt exists.
* If the commit outcome is ambiguous, the service looks the receipt up by its
  immutable ``audit_id``. If it cannot prove the commit inside the budget, it
  fails closed.
* Records are append-only and carry hashes and coarse metadata, never raw
  account identifiers, tokens or money values.

Two backends ship here. ``InMemoryAuditStore`` is for fast tests.
``JsonlAuditStore`` appends and fsyncs, so a crash-point test can inspect what
actually reached the disk. The PostgreSQL DDL for the same shape lives in
``migrations/versions/0001_risk_api_audit.sql``; wiring it needs a driver this
build does not depend on.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, Protocol, Sequence

from ..faults import FaultPoint, FaultRegistry

__all__ = [
    "AuditDecision",
    "AuditRecord",
    "AuditReceipt",
    "AuditStore",
    "AuditUnavailable",
    "AuditCommitAmbiguous",
    "InMemoryAuditStore",
    "JsonlAuditStore",
]

AuditDecision = Literal["ALLOW", "DENY", "BLOCK", "ERROR"]


class AuditUnavailable(RuntimeError):
    """The audit sink could not accept a record."""


class AuditCommitAmbiguous(RuntimeError):
    """The write may or may not have committed; the caller must prove it."""

    def __init__(self, audit_id: str) -> None:
        super().__init__(f"commit outcome unknown for audit {audit_id}")
        self.audit_id = audit_id


@dataclass(frozen=True, slots=True)
class AuditRecord:
    """One access record. Every identifier here is a hash or a version string."""

    audit_id: str
    request_id: str
    principal_scope_hash: str
    endpoint_id: str
    decision: AuditDecision
    result_status: str | None = None
    denial_family: str | None = None
    input_digest: str | None = None
    output_digest: str | None = None
    data_cut_digest: str | None = None
    risk_policy_version: str | None = None
    recon_revision: str | None = None
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    )
    evidence: tuple[str, ...] = ()

    @staticmethod
    def new_id() -> str:
        return str(uuid.uuid4())

    def as_row(self) -> dict[str, object]:
        row = asdict(self)
        row["evidence"] = list(self.evidence)
        return row


@dataclass(frozen=True, slots=True)
class AuditReceipt:
    """Proof the record is durable."""

    audit_id: str
    committed: bool
    created_at: str


class AuditStore(Protocol):
    async def append(self, record: AuditRecord) -> AuditReceipt: ...

    async def find(self, audit_id: str) -> AuditRecord | None: ...

    async def health(self) -> bool: ...


class InMemoryAuditStore:
    """Append-only in-process store.

    ``fail_commit``/``lose_ack`` let the audit-outage tests drive the two
    distinct failure shapes: nothing was written, and something may have been.
    """

    def __init__(self, *, faults: FaultRegistry | None = None) -> None:
        self._records: dict[str, AuditRecord] = {}
        self._order: list[str] = []
        self._faults = faults or FaultRegistry()
        self._lock = asyncio.Lock()
        self.appends = 0
        self.failures = 0

    async def append(self, record: AuditRecord) -> AuditReceipt:
        if self._faults.active(FaultPoint.AUDIT_SINK_DOWN) is not None:
            self.failures += 1
            raise AuditUnavailable("audit sink is unavailable")
        if self._faults.active(FaultPoint.AUDIT_FAIL_BEFORE_COMMIT) is not None:
            self.failures += 1
            raise AuditUnavailable("audit insert failed before commit")

        lose_ack = self._faults.active(FaultPoint.AUDIT_LOSE_ACK) is not None
        async with self._lock:
            if record.audit_id in self._records:
                # Append-only: a repeated id is an idempotent no-op, never an update.
                existing = self._records[record.audit_id]
                return AuditReceipt(existing.audit_id, True, existing.created_at)
            self._records[record.audit_id] = record
            self._order.append(record.audit_id)
            self.appends += 1

        if self._faults.active(FaultPoint.AUDIT_FAIL_AFTER_COMMIT) is not None:
            raise AuditCommitAmbiguous(record.audit_id)
        if lose_ack:
            # The row is durable but the acknowledgement never arrived.
            raise AuditCommitAmbiguous(record.audit_id)
        return AuditReceipt(record.audit_id, True, record.created_at)

    async def find(self, audit_id: str) -> AuditRecord | None:
        if self._faults.active(FaultPoint.AUDIT_SINK_DOWN) is not None:
            raise AuditUnavailable("audit sink is unavailable")
        return self._records.get(audit_id)

    async def health(self) -> bool:
        return self._faults.active(FaultPoint.AUDIT_SINK_DOWN) is None

    # ------------------------------------------------------------ inspection

    def all_records(self) -> tuple[AuditRecord, ...]:
        return tuple(self._records[audit_id] for audit_id in self._order)

    def records_for_request(self, request_id: str) -> tuple[AuditRecord, ...]:
        return tuple(row for row in self.all_records() if row.request_id == request_id)

    def decisions(self) -> tuple[str, ...]:
        return tuple(row.decision for row in self.all_records())


class JsonlAuditStore:
    """Append-only file store with an explicit fsync.

    Durability here is a real property a crash test can check: after a kill,
    whatever is in the file is what the service may claim it audited.
    """

    def __init__(self, path: str | Path, *, faults: FaultRegistry | None = None) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._faults = faults or FaultRegistry()
        self._lock = asyncio.Lock()
        self.appends = 0
        self.failures = 0

    async def append(self, record: AuditRecord) -> AuditReceipt:
        if self._faults.active(FaultPoint.AUDIT_SINK_DOWN) is not None:
            self.failures += 1
            raise AuditUnavailable("audit sink is unavailable")
        if self._faults.active(FaultPoint.AUDIT_FAIL_BEFORE_COMMIT) is not None:
            self.failures += 1
            raise AuditUnavailable("audit insert failed before commit")

        lose_ack = self._faults.active(FaultPoint.AUDIT_LOSE_ACK) is not None
        line = json.dumps(record.as_row(), sort_keys=True, separators=(",", ":")) + "\n"
        async with self._lock:
            existing = await self.find(record.audit_id)
            if existing is not None:
                return AuditReceipt(existing.audit_id, True, existing.created_at)
            await asyncio.to_thread(self._append_and_sync, line)
            self.appends += 1

        if self._faults.active(FaultPoint.AUDIT_FAIL_AFTER_COMMIT) is not None:
            raise AuditCommitAmbiguous(record.audit_id)
        if lose_ack:
            raise AuditCommitAmbiguous(record.audit_id)
        return AuditReceipt(record.audit_id, True, record.created_at)

    def _append_and_sync(self, line: str) -> None:
        # A crash mid-write can leave a partial final line. Starting a fresh
        # line keeps that damage to the one torn record.
        needs_newline = False
        if self._path.exists() and self._path.stat().st_size:
            with open(self._path, "rb") as probe:
                probe.seek(-1, os.SEEK_END)
                needs_newline = probe.read(1) != b"\n"
        with open(self._path, "a", encoding="utf-8") as handle:
            if needs_newline:
                handle.write("\n")
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())

    async def find(self, audit_id: str) -> AuditRecord | None:
        if self._faults.active(FaultPoint.AUDIT_SINK_DOWN) is not None:
            raise AuditUnavailable("audit sink is unavailable")
        if not self._path.exists():
            return None
        rows = await asyncio.to_thread(self._read_rows)
        for row in rows:
            if row.get("audit_id") == audit_id:
                return _record_from_row(row)
        return None

    def _read_rows(self) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        with open(self._path, encoding="utf-8") as handle:
            for raw in handle:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    rows.append(json.loads(raw))
                except json.JSONDecodeError:
                    # A torn final line is what a crash mid-write looks like.
                    continue
        return rows

    async def health(self) -> bool:
        if self._faults.active(FaultPoint.AUDIT_SINK_DOWN) is not None:
            return False
        return self._path.parent.exists()

    def all_records(self) -> tuple[AuditRecord, ...]:
        if not self._path.exists():
            return ()
        return tuple(_record_from_row(row) for row in self._read_rows())


def _record_from_row(row: dict[str, object]) -> AuditRecord:
    evidence = row.get("evidence") or ()
    if isinstance(evidence, Sequence) and not isinstance(evidence, str):
        evidence_tuple = tuple(str(item) for item in evidence)
    else:  # pragma: no cover - defensive
        evidence_tuple = ()
    return AuditRecord(
        audit_id=str(row["audit_id"]),
        request_id=str(row["request_id"]),
        principal_scope_hash=str(row["principal_scope_hash"]),
        endpoint_id=str(row["endpoint_id"]),
        decision=str(row["decision"]),  # type: ignore[arg-type]
        result_status=row.get("result_status"),  # type: ignore[arg-type]
        denial_family=row.get("denial_family"),  # type: ignore[arg-type]
        input_digest=row.get("input_digest"),  # type: ignore[arg-type]
        output_digest=row.get("output_digest"),  # type: ignore[arg-type]
        data_cut_digest=row.get("data_cut_digest"),  # type: ignore[arg-type]
        risk_policy_version=row.get("risk_policy_version"),  # type: ignore[arg-type]
        recon_revision=row.get("recon_revision"),  # type: ignore[arg-type]
        created_at=str(row.get("created_at", "")),
        evidence=evidence_tuple,
    )
