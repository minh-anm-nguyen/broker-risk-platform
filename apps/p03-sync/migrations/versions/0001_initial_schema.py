"""initial sync schema with immutable deal enforcement

Revision ID: 0001_initial
Revises:
Create Date: 2026-10-31

Tables are created from the shared ``db_models.metadata`` rather than being
re-declared here. The duplication the usual hand-written style produces is not
free: the two copies drift, and a constraint silently missing from the
migration is exactly the failure WF3-245 describes. Keeping one declaration
means the schema this migration builds is the schema the application believes
in, and ``tests/integration_pg/test_migrations.py`` asserts the live database
really carries every required unique constraint, check and index.

What metadata cannot express is written out explicitly below:

* a trigger that rejects UPDATE and DELETE on ``deals``. Immutability is the
  invariant most expensive to lose and the easiest to lose by accident -- one
  stray ``ON CONFLICT DO UPDATE`` would rewrite a commission. Enforcing it in
  the database means application code cannot violate it even by mistake
  (INV-05, WF3-101, WF3-104);
* least-privilege grants, so the application role holds DML and only the
  migration owner holds DDL (design 11.1, WF3-184).
"""

from __future__ import annotations

from typing import Sequence

import sqlalchemy as sa
from alembic import op

from trading_sync.infrastructure.db_models import metadata

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_IMMUTABLE_DEAL_FUNCTION = """
CREATE OR REPLACE FUNCTION deals_reject_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION
        'deals rows are immutable (attempted % on epoch_id=%, deal_id=%)',
        TG_OP, OLD.epoch_id, OLD.deal_id
        USING ERRCODE = 'restrict_violation';
END;
$$ LANGUAGE plpgsql;
"""

_IMMUTABLE_DEAL_TRIGGER = """
CREATE TRIGGER trg_deals_immutable
BEFORE UPDATE OR DELETE ON deals
FOR EACH ROW EXECUTE FUNCTION deals_reject_mutation();
"""


def upgrade() -> None:
    bind = op.get_bind()
    metadata.create_all(bind=bind, checkfirst=False)

    op.execute(_IMMUTABLE_DEAL_FUNCTION)
    op.execute(_IMMUTABLE_DEAL_TRIGGER)

    _grant_least_privilege(bind)


def downgrade() -> None:
    bind = op.get_bind()
    op.execute("DROP TRIGGER IF EXISTS trg_deals_immutable ON deals")
    op.execute("DROP FUNCTION IF EXISTS deals_reject_mutation()")
    metadata.drop_all(bind=bind, checkfirst=True)


def _grant_least_privilege(bind: sa.engine.Connection) -> None:
    """Grant DML to the application role, if that role exists.

    Skipped silently when ``sync_app`` is absent so a fresh developer database
    does not need role setup first. The integration suite asserts the split
    wherever the role does exist, which is where it matters.
    """
    exists = bind.execute(
        sa.text("SELECT 1 FROM pg_roles WHERE rolname = :role"), {"role": "sync_app"}
    ).scalar()
    if not exists:
        return
    for table in sorted(metadata.tables):
        # No DELETE: retention sweeps run as a separate, audited task, and the
        # ingestion role has no reason to be able to remove history.
        op.execute(f"GRANT SELECT, INSERT, UPDATE ON {table} TO sync_app")
    op.execute("GRANT USAGE ON SCHEMA public TO sync_app")
