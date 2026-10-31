"""Read Project 03's real target, not the harness (contract gap CG-13).

Needs a PostgreSQL database carrying Project 03's migrated schema and some
synced data. Without ``RECON_TARGET_PG_DSN`` every case here reports
BLOCKED_ENVIRONMENT, which by the project's own rule is not a pass.

These cases are deliberately thin on assertions about *values* and firm about
*shape*. The point of the module under test is that P04 can speak to the real
schema at all - the harness it used instead calls two of these columns by
different names, which is exactly the kind of divergence no component test
could see.
"""

from __future__ import annotations

import os
from decimal import Decimal

import pytest

from reconciliation.db.pg_target import PgTargetError, PostgresTargetReader

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def target_dsn() -> str:
    dsn = os.environ.get("RECON_TARGET_PG_DSN", "").strip()
    if not dsn:
        pytest.skip(
            "RECON_TARGET_PG_DSN is not set: the real-target cases are "
            "BLOCKED_ENVIRONMENT, not passing"
        )
    return dsn


@pytest.fixture(scope="module")
def reader(target_dsn: str) -> PostgresTargetReader:
    return PostgresTargetReader(target_dsn)


@pytest.fixture(scope="module")
def checkpoint(reader: PostgresTargetReader) -> tuple[str, str]:
    """An epoch and filter that actually has a published deals checkpoint."""
    with reader.capture() as connection:
        row = connection.execute(
            "SELECT epoch_id, filter_hash FROM sync_checkpoints"
            " WHERE entity = 'deals' AND published_scan_id IS NOT NULL LIMIT 1"
        ).fetchone()
    if row is None:
        pytest.skip(
            "the target has no published deals checkpoint, so there is no cut "
            "to capture: BLOCKED_ENVIRONMENT, not a pass"
        )
    return str(row[0]), str(row[1])


def test_the_cut_is_readable_and_names_its_scan(reader, checkpoint) -> None:
    """A cut is only usable when the checkpoint and its scan both exist.

    Reading rows against a checkpoint with no terminal scan would compare the
    source to a target that is mid-publication, and report a discrepancy whose
    cause is timing (design decision 4).
    """
    epoch_id, filter_hash = checkpoint
    with reader.capture() as connection:
        cut = reader.read_cut(
            connection, epoch_id=epoch_id, entity="deals", filter_hash=filter_hash
        )
    assert cut.publishable
    assert cut.epoch_status
    assert isinstance(cut.watermark, int)
    assert cut.published_scan_id
    assert cut.scan_status


def test_deal_rows_arrive_under_the_keys_p04_already_hashes(reader, checkpoint) -> None:
    """``entry_kind`` is aliased to ``entry`` at this boundary.

    Not cosmetic: P04's canonical digests are computed over these keys, so
    passing the real column name through would change every hash it has ever
    recorded. The alias is what lets a real target be compared against
    evidence captured from the harness.
    """
    epoch_id, filter_hash = checkpoint
    with reader.capture() as connection:
        cut = reader.read_cut(
            connection, epoch_id=epoch_id, entity="deals", filter_hash=filter_hash
        )
        rows = list(
            reader.iter_rows(
                connection, entity="deals", epoch_id=epoch_id, max_seq=cut.watermark
            )
        )
    assert rows, "the target reported a published cut but returned no deals"
    assert set(rows[0]) == {
        "deal_id",
        "source_seq",
        "account_id",
        "position_id",
        "symbol",
        "entry",
        "side",
        "volume_lots",
        "price",
        "realized_pnl_gross",
        "commission",
        "swap",
        "executed_at",
    }


def test_money_arrives_as_decimal_not_float(reader, checkpoint) -> None:
    """The driver must hand back exact values.

    If any of these were floats the amount would already be wrong and no
    reconciliation downstream could recover it.
    """
    epoch_id, filter_hash = checkpoint
    with reader.capture() as connection:
        cut = reader.read_cut(
            connection, epoch_id=epoch_id, entity="deals", filter_hash=filter_hash
        )
        row = next(
            reader.iter_rows(
                connection, entity="deals", epoch_id=epoch_id, max_seq=cut.watermark
            )
        )
    for field in ("volume_lots", "price", "realized_pnl_gross", "commission", "swap"):
        assert isinstance(row[field], Decimal), f"{field} came back as {type(row[field])}"


def test_positions_expose_source_status_under_its_harness_name(reader, checkpoint) -> None:
    epoch_id, _ = checkpoint
    with reader.capture() as connection:
        rows = list(reader.iter_rows(connection, entity="positions", epoch_id=epoch_id))
    if not rows:
        pytest.skip("no positions published for this epoch: nothing to assert")
    assert "status" in rows[0]
    assert "source_status" not in rows[0]


def test_an_unbounded_deal_read_is_refused(reader, checkpoint) -> None:
    """Without a watermark the read would include rows past the cut.

    A concurrent scan's rows would then look like a surplus in the target,
    which is a race reported as a data discrepancy.
    """
    epoch_id, _ = checkpoint
    with reader.capture() as connection:
        with pytest.raises(PgTargetError, match="max_seq"):
            list(reader.iter_rows(connection, entity="deals", epoch_id=epoch_id))


def test_an_unknown_entity_is_refused(reader, checkpoint) -> None:
    epoch_id, _ = checkpoint
    with reader.capture() as connection:
        with pytest.raises(PgTargetError, match="projection"):
            list(reader.iter_rows(connection, entity="quotes", epoch_id=epoch_id))


def test_a_missing_epoch_is_an_error_not_an_empty_cut(reader) -> None:
    """An epoch that does not exist must not read as "nothing to reconcile".

    Returning an empty cut would let a typo in an epoch id be reported as a
    clean reconciliation.
    """
    with reader.capture() as connection:
        with pytest.raises(PgTargetError, match="does not exist"):
            reader.read_cut(
                connection,
                epoch_id="00000000-0000-0000-0000-000000000000",
                entity="deals",
                filter_hash="UNFILTERED",
            )


def test_the_capture_cannot_write(reader, checkpoint) -> None:
    """READ ONLY on the transaction, roles aside.

    P04 owns no row in this schema. The GRANT is the real control, but a
    transaction that refuses writes means a bug in this package cannot mutate
    upstream facts even where the role would allow it. The statement below is
    a no-op by construction, so the only thing it can demonstrate is the
    refusal.
    """
    epoch_id, _ = checkpoint
    with reader.capture() as connection:
        with pytest.raises(Exception):
            connection.execute(
                "UPDATE deals SET commission = commission WHERE epoch_id = %s",
                (epoch_id,),
            )
