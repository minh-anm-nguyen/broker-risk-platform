"""add the composite unique constraint Project 07's outbox foreign key needs

Revision ID: 0002_epoch_identity
Revises: 0001_initial
Create Date: 2026-10-31

``source_epochs`` already guarantees this: ``epoch_id`` is the primary key, so
``(source_id, epoch_id)`` cannot repeat. PostgreSQL does not infer that. A
composite foreign key may only reference columns covered by an explicit unique
constraint, and Project 07's ``event_outbox`` declares

    FOREIGN KEY (source_id, epoch_id) REFERENCES source_epochs (source_id, epoch_id)

so that an event cannot claim the wrong source for an epoch - a guarantee in
the schema rather than in application code.

Without this constraint, applying P07's DDL to this database fails outright
with ``InvalidForeignKeyError``, which means the two projects cannot share one
database and the outbox row cannot sit in the same transaction as the Deal.
Found by running ``tests/integration_pg/test_outbox_atomicity.py`` against a
real PostgreSQL instance; no amount of reading either schema had revealed it
(contract gap CG-11).
"""

from __future__ import annotations

from typing import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_epoch_identity"
down_revision: str | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_CONSTRAINT = "uq_source_epoch_identity"


def _exists(bind) -> bool:
    return bool(
        bind.execute(
            sa.text(
                "SELECT 1 FROM pg_constraint WHERE conname = :name "
                "AND conrelid = 'source_epochs'::regclass"
            ),
            {"name": _CONSTRAINT},
        ).scalar()
    )


def upgrade() -> None:
    """Add the constraint only if it is not already there.

    Revision 0001 builds the schema with ``metadata.create_all``, so a database
    created after this constraint joined ``db_models`` already has it, while one
    migrated earlier does not. An unconditional CREATE fails on the first kind
    with DuplicateTableError - which is how this was found.
    """
    bind = op.get_bind()
    if _exists(bind):
        return
    op.create_unique_constraint(_CONSTRAINT, "source_epochs", ["source_id", "epoch_id"])


def downgrade() -> None:
    # Dropping this breaks Project 07's event_outbox if it is present, so the
    # downgrade is only safe on a database that never had the outbox applied.
    bind = op.get_bind()
    if _exists(bind):
        op.drop_constraint(_CONSTRAINT, "source_epochs", type_="unique")
