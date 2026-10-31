"""Audit receipts: durable, idempotent, and free of monetary values.

Two independent requirements meet here.

*No unaudited READY.* If the policy says audit is mandatory, a result that
could not be recorded is not publishable. The number may well be right, but it
would be unreproducible the moment the inputs move on, and "trust me, it was
correct yesterday" is not a financial control (G05-11, WF5-280).

*Ambiguous COMMIT is survivable.* The window between the database accepting an
INSERT and the client learning about it is unavoidable. An idempotency key
derived from the input digest closes it: a retry either finds the existing
receipt and returns it, or inserts the first one. What it never does is write a
second receipt for the same calculation (K05-15, WF5-328).

The receipt holds digests, versions, statuses and reason codes. It does not
hold amounts: the audit trail proves *which* inputs produced a result, and the
result itself is re-derivable from them.
"""

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from risk.errors import AuditError, ReasonCode
from risk.manifest import digest
from risk.status import GlobalStatus

__all__ = [
    "AuditReceipt",
    "AuditSink",
    "InMemoryAuditSink",
    "SqliteAuditSink",
    "make_idempotency_key",
    "make_calculation_id",
]


def make_idempotency_key(
    *,
    input_digest: str,
    risk_policy_version: str,
    requested_components: tuple[str, ...],
    strict_composite: bool,
) -> str:
    """Derive the key that makes a retry resolve to one receipt.

    Everything that can change the answer is in the key, and nothing that
    cannot. Notably absent: wall-clock time and a request id. Including either
    would make every retry look like a new calculation, which is exactly the
    duplicate the key exists to prevent.
    """
    return digest(
        {
            "input_digest": input_digest,
            "risk_policy_version": risk_policy_version,
            "requested_components": sorted(requested_components),
            "strict_composite": strict_composite,
        }
    )


def make_calculation_id(idempotency_key: str) -> str:
    """Deterministic calculation id derived from the idempotency key.

    Deterministic so that two processes computing the same report under the
    same inputs agree on the identifier instead of racing to create two
    (WF5-214).
    """
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"urn:risk:calculation:{idempotency_key}"))


@dataclass(frozen=True)
class AuditReceipt:
    """A durable record that one calculation happened, and on what."""

    calculation_id: str
    idempotency_key: str
    epoch_id: str
    account_scope_hash: str
    risk_policy_version: str
    symbol_catalog_version: str
    cut_manifest: dict[str, object]
    input_digest: str
    output_digest: str | None
    status: GlobalStatus
    reason_codes: tuple[str, ...]
    duration_ms: int
    created_at: datetime
    replayed: bool = False

    def as_payload(self) -> dict[str, object]:
        return {
            "calculation_id": self.calculation_id,
            "idempotency_key": self.idempotency_key,
            "epoch_id": self.epoch_id,
            "account_scope_hash": self.account_scope_hash,
            "risk_policy_version": self.risk_policy_version,
            "symbol_catalog_version": self.symbol_catalog_version,
            "input_digest": self.input_digest,
            "output_digest": self.output_digest,
            "status": str(self.status),
            "reason_codes": list(self.reason_codes),
            "duration_ms": self.duration_ms,
            "created_at": self.created_at.astimezone(UTC).isoformat(),
            "replayed": self.replayed,
        }


class AuditSink(Protocol):
    """Where receipts go. Must be durable before a READY is returned."""

    async def record(self, receipt: AuditReceipt) -> AuditReceipt: ...

    async def find(self, idempotency_key: str) -> AuditReceipt | None: ...


class InMemoryAuditSink:
    """Audit sink for unit tests and the pure-library entry point.

    ``fail_next`` and ``fail_always`` exist so that the fail-closed path is
    exercised by the test suite rather than asserted in prose.
    """

    def __init__(self) -> None:
        self._by_key: dict[str, AuditReceipt] = {}
        self.fail_next = 0
        self.fail_always = False
        self.writes = 0

    async def record(self, receipt: AuditReceipt) -> AuditReceipt:
        if self.fail_always or self.fail_next > 0:
            if self.fail_next > 0:
                self.fail_next -= 1
            raise AuditError(ReasonCode.AUDIT_PERSISTENCE_FAILED, "audit sink is unavailable")
        existing = self._by_key.get(receipt.idempotency_key)
        if existing is not None:
            # Idempotent: the first receipt wins and is returned as a replay.
            return AuditReceipt(**{**existing.__dict__, "replayed": True})
        self._by_key[receipt.idempotency_key] = receipt
        self.writes += 1
        return receipt

    async def find(self, idempotency_key: str) -> AuditReceipt | None:
        return self._by_key.get(idempotency_key)

    def __len__(self) -> int:
        return len(self._by_key)


class SqliteAuditSink:
    """Audit sink backed by the harness ``risk_calculation_audit`` table.

    Separate connection from the read path on purpose. The read snapshot is
    rolled back to prove nothing was written through it, so the audit write
    cannot share it (section 6.3).
    """

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._conn = connection
        self._conn.row_factory = sqlite3.Row

    async def record(self, receipt: AuditReceipt) -> AuditReceipt:
        import json

        existing = await self.find(receipt.idempotency_key)
        if existing is not None:
            return AuditReceipt(**{**existing.__dict__, "replayed": True})
        try:
            self._conn.execute(
                """
                INSERT INTO risk_calculation_audit (
                    calculation_id, idempotency_key, epoch_id, account_scope_hash,
                    risk_policy_version, symbol_catalog_version, cut_manifest,
                    input_digest, output_digest, status, reason_codes,
                    duration_ms, created_at
                ) VALUES (
                    :calculation_id, :idempotency_key, :epoch_id, :account_scope_hash,
                    :risk_policy_version, :symbol_catalog_version, :cut_manifest,
                    :input_digest, :output_digest, :status, :reason_codes,
                    :duration_ms, :created_at
                )
                """,
                {
                    "calculation_id": receipt.calculation_id,
                    "idempotency_key": receipt.idempotency_key,
                    "epoch_id": receipt.epoch_id,
                    "account_scope_hash": receipt.account_scope_hash,
                    "risk_policy_version": receipt.risk_policy_version,
                    "symbol_catalog_version": receipt.symbol_catalog_version,
                    "cut_manifest": json.dumps(receipt.cut_manifest, sort_keys=True),
                    "input_digest": receipt.input_digest,
                    "output_digest": receipt.output_digest,
                    "status": str(receipt.status),
                    "reason_codes": json.dumps(list(receipt.reason_codes)),
                    "duration_ms": receipt.duration_ms,
                    "created_at": receipt.created_at.astimezone(UTC).isoformat(),
                },
            )
            self._conn.commit()
        except sqlite3.IntegrityError:
            # Lost the insert race: the winner's receipt is the canonical one.
            self._conn.rollback()
            winner = await self.find(receipt.idempotency_key)
            if winner is None:  # pragma: no cover - would mean a different constraint
                raise AuditError(
                    ReasonCode.AUDIT_PERSISTENCE_FAILED, "audit insert conflicted but no row found"
                ) from None
            return AuditReceipt(**{**winner.__dict__, "replayed": True})
        except sqlite3.Error as exc:
            self._conn.rollback()
            raise AuditError(ReasonCode.AUDIT_PERSISTENCE_FAILED, "audit insert failed") from exc
        return receipt

    async def find(self, idempotency_key: str) -> AuditReceipt | None:
        import json

        row = self._conn.execute(
            "SELECT * FROM risk_calculation_audit WHERE idempotency_key = :key",
            {"key": idempotency_key},
        ).fetchone()
        if row is None:
            return None
        return AuditReceipt(
            calculation_id=row["calculation_id"],
            idempotency_key=row["idempotency_key"],
            epoch_id=row["epoch_id"],
            account_scope_hash=row["account_scope_hash"],
            risk_policy_version=row["risk_policy_version"],
            symbol_catalog_version=row["symbol_catalog_version"],
            cut_manifest=json.loads(row["cut_manifest"]),
            input_digest=row["input_digest"],
            output_digest=row["output_digest"],
            status=GlobalStatus(row["status"]),
            reason_codes=tuple(json.loads(row["reason_codes"])),
            duration_ms=0 if row["duration_ms"] is None else int(row["duration_ms"]),
            created_at=datetime.fromisoformat(row["created_at"]),
        )


async def record_with_retry(
    sink: AuditSink, receipt: AuditReceipt, *, attempts: int
) -> AuditReceipt:
    """Persist a receipt, retrying a transient sink failure a bounded number of times.

    Bounded, because an unbounded retry turns an audit outage into a hung
    request. When the attempts are gone the error propagates and the caller
    gets ``AUDIT_PERSISTENCE_FAILED`` rather than a result (WF5-329).
    """
    if attempts < 1:
        raise AuditError(ReasonCode.AUDIT_PERSISTENCE_FAILED, "audit retry budget must be at least 1")
    last: Exception | None = None
    for _ in range(attempts):
        try:
            return await sink.record(receipt)
        except AuditError as exc:
            last = exc
    assert last is not None
    raise last


def new_receipt(
    *,
    idempotency_key: str,
    epoch_id: str,
    account_scope_hash: str,
    risk_policy_version: str,
    symbol_catalog_version: str,
    cut_manifest: dict[str, Any],
    input_digest: str,
    output_digest: str | None,
    status: GlobalStatus,
    reason_codes: tuple[str, ...],
    duration_ms: int,
    now: datetime | None = None,
) -> AuditReceipt:
    """Build a receipt with a deterministic calculation id."""
    return AuditReceipt(
        calculation_id=make_calculation_id(idempotency_key),
        idempotency_key=idempotency_key,
        epoch_id=epoch_id,
        account_scope_hash=account_scope_hash,
        risk_policy_version=risk_policy_version,
        symbol_catalog_version=symbol_catalog_version,
        cut_manifest=cut_manifest,
        input_digest=input_digest,
        output_digest=output_digest,
        status=status,
        reason_codes=reason_codes,
        duration_ms=duration_ms,
        created_at=now or datetime.now(UTC),
    )
