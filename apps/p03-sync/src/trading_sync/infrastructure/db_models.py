"""PostgreSQL schema, as SQLAlchemy Core tables.

Follows the reference DDL in design 5.3. A few deliberate choices repeat often
enough to state once:

``TEXT`` for every id
    Account, position and deal ids keep the source's exact string. A numeric
    column would lose leading zeros and merge ``'0001'`` with ``'1'``
    (AS-03, WF3-034).

``NUMERIC(38,12)`` for every money or quantity column
    Exact decimal storage. The validator refuses anything that would not fit,
    so the database never has to round (WF3-088, WF3-089).

``epoch_id`` in every primary key
    Deal ids are unique only within a source epoch. The composite key is what
    makes a simulator reset, or two sources that happen to share an id, two
    independent rows rather than one overwritten one (INV-14, WF3-015,
    WF3-182).

No foreign key from ``deals.position_id``
    A historical deal legitimately references a position that is no longer in
    the current open set. A constraint here would reject real history
    (ADR-014, WF3-077).
"""

from __future__ import annotations

from sqlalchemy import (
    CHAR,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Numeric,
    PrimaryKeyConstraint,
    Table,
    Text,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB

__all__ = [
    "ENTITY_VALUES",
    "EPOCH_STATUS_VALUES",
    "MONEY",
    "SCAN_STATUS_VALUES",
    "account_staging",
    "accounts_current",
    "deals",
    "metadata",
    "position_staging",
    "positions_current",
    "quarantine_items",
    "source_epochs",
    "sync_audit",
    "sync_checkpoints",
    "sync_page_receipts",
    "sync_runs",
    "sync_sources",
]

metadata = MetaData()

#: One alias so a precision change is a single edit rather than thirty.
MONEY = Numeric(38, 12, asdecimal=True)

ENTITY_VALUES = ("accounts", "positions", "deals")
EPOCH_STATUS_VALUES = ("DISCOVERED", "ACTIVE", "DEGRADED", "BLOCKED", "ARCHIVED")
SCAN_STATUS_VALUES = (
    "CREATED",
    "FETCHING",
    "STAGED",
    "VALIDATING",
    "PUBLISHING",
    "PUBLISHED",
    "ABORTED",
    "QUARANTINED",
    "BLOCKED_EPOCH",
)


def _in_list(column: str, values: tuple[str, ...]) -> str:
    rendered = ", ".join(f"'{value}'" for value in values)
    return f"{column} IN ({rendered})"


sync_sources = Table(
    "sync_sources",
    metadata,
    Column("source_id", Text, primary_key=True),
    Column("source_kind", Text, nullable=False),
    Column("enabled", Boolean, nullable=False, server_default="true"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint(
        _in_list("source_kind", ("simulator", "manager_sdk")), name="ck_source_kind"
    ),
)

source_epochs = Table(
    "source_epochs",
    metadata,
    Column("epoch_id", Uuid, primary_key=True),
    Column("source_id", Text, ForeignKey("sync_sources.source_id"), nullable=False),
    Column("run_id", Text, nullable=False),
    Column("generation", BigInteger, nullable=False),
    Column("status", Text, nullable=False),
    Column("activated_at", DateTime(timezone=True)),
    Column("blocked_reason", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint("generation >= 0", name="ck_epoch_generation_nonneg"),
    CheckConstraint(_in_list("status", EPOCH_STATUS_VALUES), name="ck_epoch_status"),
    UniqueConstraint("source_id", "run_id", "generation", name="uq_source_epoch"),
)

sync_checkpoints = Table(
    "sync_checkpoints",
    metadata,
    Column("epoch_id", Uuid, ForeignKey("source_epochs.epoch_id"), nullable=False),
    Column("entity", Text, nullable=False),
    Column("filter_hash", Text, nullable=False),
    Column("watermark", BigInteger, nullable=False, server_default="0"),
    Column("published_scan_id", Uuid),
    Column("lease_token", Uuid),
    Column("lease_version", BigInteger, nullable=False, server_default="0"),
    Column("lease_until", DateTime(timezone=True)),
    Column("last_success_at", DateTime(timezone=True)),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    PrimaryKeyConstraint("epoch_id", "entity", "filter_hash", name="pk_sync_checkpoints"),
    CheckConstraint("watermark >= 0", name="ck_checkpoint_watermark_nonneg"),
    CheckConstraint("lease_version >= 0", name="ck_checkpoint_lease_version_nonneg"),
    CheckConstraint(_in_list("entity", ENTITY_VALUES), name="ck_checkpoint_entity"),
)

sync_runs = Table(
    "sync_runs",
    metadata,
    Column("scan_id", Uuid, primary_key=True),
    Column("epoch_id", Uuid, ForeignKey("source_epochs.epoch_id"), nullable=False),
    Column("entity", Text, nullable=False),
    Column("filter_hash", Text, nullable=False),
    Column("start_watermark", BigInteger),
    Column("snapshot_seq", BigInteger),
    Column("status", Text, nullable=False),
    Column("page_count", BigInteger, nullable=False, server_default="0"),
    Column("record_count", BigInteger, nullable=False, server_default="0"),
    Column("duplicate_count", BigInteger, nullable=False, server_default="0"),
    Column("owner_lease_version", BigInteger, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("ended_at", DateTime(timezone=True)),
    Column("error_code", Text),
    CheckConstraint("snapshot_seq IS NULL OR snapshot_seq >= 0", name="ck_run_snapshot_seq"),
    CheckConstraint("page_count >= 0", name="ck_run_page_count"),
    CheckConstraint("record_count >= 0", name="ck_run_record_count"),
    CheckConstraint(_in_list("entity", ENTITY_VALUES), name="ck_run_entity"),
    CheckConstraint(_in_list("status", SCAN_STATUS_VALUES), name="ck_run_status"),
    Index("idx_sync_runs_epoch_entity_time", "epoch_id", "entity", "started_at"),
)

sync_page_receipts = Table(
    "sync_page_receipts",
    metadata,
    Column("scan_id", Uuid, ForeignKey("sync_runs.scan_id"), nullable=False),
    Column("page_index", Integer, nullable=False),
    Column("item_count", Integer, nullable=False),
    Column("page_hash", CHAR(64), nullable=False),
    Column("is_terminal", Boolean, nullable=False),
    Column("committed_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    PrimaryKeyConstraint("scan_id", "page_index", name="pk_sync_page_receipts"),
    CheckConstraint("page_index >= 0", name="ck_receipt_page_index"),
    CheckConstraint("item_count BETWEEN 0 AND 500", name="ck_receipt_item_count"),
)

account_staging = Table(
    "account_staging",
    metadata,
    Column("scan_id", Uuid, ForeignKey("sync_runs.scan_id"), nullable=False),
    Column("account_id", Text, nullable=False),
    Column("currency", Text, nullable=False),
    Column("balance", MONEY, nullable=False),
    Column("credit", MONEY, nullable=False),
    Column("payload_hash", CHAR(64), nullable=False),
    PrimaryKeyConstraint("scan_id", "account_id", name="pk_account_staging"),
)

position_staging = Table(
    "position_staging",
    metadata,
    Column("scan_id", Uuid, ForeignKey("sync_runs.scan_id"), nullable=False),
    Column("position_id", Text, nullable=False),
    Column("account_id", Text, nullable=False),
    Column("symbol", Text, nullable=False),
    Column("side", Text, nullable=False),
    Column("volume_lots", MONEY, nullable=False),
    Column("open_price", MONEY, nullable=False),
    Column("source_status", Text, nullable=False),
    Column("payload_hash", CHAR(64), nullable=False),
    PrimaryKeyConstraint("scan_id", "position_id", name="pk_position_staging"),
    CheckConstraint(_in_list("side", ("BUY", "SELL")), name="ck_position_staging_side"),
)

accounts_current = Table(
    "accounts_current",
    metadata,
    Column("epoch_id", Uuid, ForeignKey("source_epochs.epoch_id"), nullable=False),
    Column("account_id", Text, nullable=False),
    Column("currency", Text, nullable=False),
    Column("balance", MONEY, nullable=False),
    Column("credit", MONEY, nullable=False),
    Column("presence", Text, nullable=False),
    Column("published_scan_id", Uuid, ForeignKey("sync_runs.scan_id"), nullable=False),
    Column("payload_hash", CHAR(64), nullable=False),
    Column("snapshot_seq", BigInteger, nullable=False),
    Column("published_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    PrimaryKeyConstraint("epoch_id", "account_id", name="pk_accounts_current"),
    CheckConstraint(_in_list("presence", ("PRESENT", "ABSENT")), name="ck_accounts_presence"),
)

positions_current = Table(
    "positions_current",
    metadata,
    Column("epoch_id", Uuid, ForeignKey("source_epochs.epoch_id"), nullable=False),
    Column("position_id", Text, nullable=False),
    Column("account_id", Text, nullable=False),
    Column("symbol", Text, nullable=False),
    Column("side", Text, nullable=False),
    Column("volume_lots", MONEY, nullable=False),
    Column("open_price", MONEY, nullable=False),
    Column("source_status", Text, nullable=False),
    Column("presence", Text, nullable=False),
    Column("published_scan_id", Uuid, ForeignKey("sync_runs.scan_id"), nullable=False),
    Column("payload_hash", CHAR(64), nullable=False),
    Column("snapshot_seq", BigInteger, nullable=False),
    Column("published_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    PrimaryKeyConstraint("epoch_id", "position_id", name="pk_positions_current"),
    CheckConstraint(_in_list("side", ("BUY", "SELL")), name="ck_positions_side"),
    CheckConstraint(_in_list("presence", ("PRESENT", "ABSENT")), name="ck_positions_presence"),
    Index("idx_positions_epoch_account", "epoch_id", "account_id"),
)

deals = Table(
    "deals",
    metadata,
    Column("epoch_id", Uuid, ForeignKey("source_epochs.epoch_id"), nullable=False),
    Column("deal_id", Text, nullable=False),
    Column("source_seq", BigInteger, nullable=False),
    Column("account_id", Text, nullable=False),
    # Nullable and unconstrained by design; see the module docstring.
    Column("position_id", Text),
    Column("symbol", Text, nullable=False),
    Column("entry_kind", Text, nullable=False),
    Column("side", Text, nullable=False),
    Column("volume_lots", MONEY, nullable=False),
    Column("price", MONEY, nullable=False),
    Column("realized_pnl_gross", MONEY, nullable=False),
    Column("commission", MONEY, nullable=False),
    Column("swap", MONEY, nullable=False),
    Column("executed_at", DateTime(timezone=True), nullable=False),
    Column("payload_hash", CHAR(64), nullable=False),
    Column("first_scan_id", Uuid, ForeignKey("sync_runs.scan_id"), nullable=False),
    Column("inserted_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    PrimaryKeyConstraint("epoch_id", "deal_id", name="pk_deals"),
    CheckConstraint("source_seq > 0", name="ck_deals_source_seq_positive"),
    CheckConstraint(_in_list("side", ("BUY", "SELL")), name="ck_deals_side"),
    Index("idx_deals_epoch_seq", "epoch_id", "source_seq", "deal_id"),
    Index("idx_deals_epoch_account_time", "epoch_id", "account_id", "executed_at"),
)

quarantine_items = Table(
    "quarantine_items",
    metadata,
    Column("quarantine_id", Uuid, primary_key=True),
    Column("epoch_id", Uuid, ForeignKey("source_epochs.epoch_id"), nullable=False),
    Column("scan_id", Uuid, ForeignKey("sync_runs.scan_id")),
    Column("entity", Text, nullable=False),
    # A hash, never the raw key: an incident record must not become a second
    # copy of the data it is about (design 11.6, WF3-190).
    Column("source_key_hash", Text),
    Column("reason_code", Text, nullable=False),
    Column("safe_details", JSONB, nullable=False, server_default="{}"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Index("idx_quarantine_epoch_time", "epoch_id", "created_at"),
)

sync_audit = Table(
    "sync_audit",
    metadata,
    Column("audit_id", Uuid, primary_key=True),
    Column("epoch_id", Uuid, ForeignKey("source_epochs.epoch_id"), nullable=False),
    Column("scan_id", Uuid, ForeignKey("sync_runs.scan_id")),
    Column("event_type", Text, nullable=False),
    Column("reason_code", Text),
    Column("safe_details", JSONB, nullable=False, server_default="{}"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Index("idx_sync_audit_epoch_time", "epoch_id", "created_at"),
)
