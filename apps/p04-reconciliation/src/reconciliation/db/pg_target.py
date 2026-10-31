"""Read Project 03's real PostgreSQL target (contract gap CG-13).

Until this existed, Project 04's view of Project 03 was a SQLite harness, and
``reconctl source check`` said so out loud: ``p03_implementation: "contract
harness (Project 03 not yet implemented)"``. P04 had therefore never
reconciled the data it is supposed to reconcile. Its 268 tests all exercise the
harness, so a divergence between the harness and P03's real schema would not
have been detected - and there are three such divergences, listed below.

Deliberately a separate module beside :mod:`engine`, not a replacement for it.
The harness path is what P04's existing suite runs against and it stays
untouched; this is the opt-in path for a real target. Rewriting the capture
logic to be dialect-generic would have meant changing every one of those tests
at the same time as introducing a new database, which is two risks at once on
the one component whose job is to be trustworthy about discrepancies.

Three things PostgreSQL does differently, and none of them is a translation
of SQLite:

* **The cut.** SQLite pins a read snapshot with ``BEGIN DEFERRED``; PostgreSQL
  uses ``REPEATABLE READ READ ONLY``, which is a real MVCC snapshot and the
  stronger guarantee. A concurrent Project 03 publish cannot be observed
  half-applied through it.
* **The read-only guarantee.** SQLite enforces it structurally with an
  authorizer returning ``SQLITE_DENY``. PostgreSQL has roles, so the GRANT is
  the control - the same conclusion Project 05 reached. ``READ ONLY`` on the
  transaction is defence in depth, not the mechanism.
* **The vocabulary.** P03's real columns are ``entry_kind`` and
  ``source_status``; the harness calls them ``entry`` and ``status``. They are
  aliased back at this boundary, which is what an adapter is for. The aliasing
  is not cosmetic: P04's canonical digests are computed over these keys, so
  renaming them downstream would change every hash it has ever recorded.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

__all__ = ["PgTargetError", "PostgresTargetReader"]


class PgTargetError(RuntimeError):
    """The target could not be read, so no verdict is possible.

    Separate from "the target disagrees with the source". A capture that could
    not complete must become ``INCONCLUSIVE``, never ``MATCHED`` and never a
    partial manifest (INV04-14).
    """


#: Row projections against Project 03's real schema. ``entry_kind`` and
#: ``source_status`` are aliased to the names P04's digest code already uses.
_ROW_SQL: Mapping[str, str] = {
    "accounts": (
        "SELECT account_id, currency, balance, credit"
        "  FROM accounts_current"
        " WHERE epoch_id = %s"
        " ORDER BY account_id"
    ),
    "positions": (
        "SELECT position_id, account_id, symbol, side, volume_lots, open_price,"
        "       source_status AS status"
        "  FROM positions_current"
        " WHERE epoch_id = %s"
        " ORDER BY position_id"
    ),
    "deals": (
        "SELECT deal_id, source_seq, account_id, position_id, symbol,"
        "       entry_kind AS entry, side, volume_lots, price,"
        "       realized_pnl_gross, commission, swap, executed_at"
        "  FROM deals"
        " WHERE epoch_id = %s AND source_seq <= %s"
        " ORDER BY source_seq"
    ),
}


@dataclass(frozen=True, slots=True)
class TargetCut:
    """What the target said about itself inside one snapshot."""

    epoch_id: str
    epoch_status: str
    entity: str
    watermark: int | None
    published_scan_id: str | None
    lease_version: int | None
    scan_status: str | None
    scan_snapshot_seq: int | None
    scan_record_count: int | None

    @property
    def publishable(self) -> bool:
        """Whether this cut may be compared against a source at all.

        A missing checkpoint or a scan that is not terminal means the target is
        mid-publication. Comparing against it would report a discrepancy whose
        cause is timing rather than data - exactly the conclusion design
        decision 4 forbids.
        """
        return (
            self.watermark is not None
            and self.published_scan_id is not None
            and self.scan_status is not None
        )


class PostgresTargetReader:
    """Read-only reader over Project 03's PostgreSQL schema."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise PgTargetError(
                "psycopg is not installed; install the postgres extra to read a "
                "real Project 03 target"
            ) from exc
        self._psycopg = psycopg

    @contextmanager
    def capture(self) -> Iterator[Any]:
        """One proven snapshot of the target.

        ``REPEATABLE READ READ ONLY``: every read inside the block sees one
        database revision, so accounts, positions and deals cannot be captured
        from three different instants. ``READ ONLY`` is belt to the role's
        braces - P04 owns no row in this schema and must not be able to write
        one even by accident (design decision 3).

        Rolled back rather than committed on the way out. A read transaction
        has nothing to commit, and rolling back is what proves nothing was
        written.
        """
        try:
            connection = self._psycopg.connect(self._dsn, autocommit=False)
        except self._psycopg.Error as exc:
            raise PgTargetError(f"could not connect to the target: {exc}") from exc
        try:
            connection.execute(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )
            yield connection
        except self._psycopg.Error as exc:
            raise PgTargetError(f"target capture failed: {exc}") from exc
        finally:
            try:
                connection.rollback()
            finally:
                connection.close()

    def read_cut(
        self, connection: Any, *, epoch_id: str, entity: str, filter_hash: str
    ) -> TargetCut:
        """Read the epoch, the checkpoint and the published scan in one snapshot.

        All three inside the caller's capture, deliberately. Fetching the
        checkpoint over a separate connection and then reading rows would
        compare rows against metadata that had already moved - the failure
        :mod:`target_capture` opens its docstring with.
        """
        epoch = connection.execute(
            "SELECT status FROM source_epochs WHERE epoch_id = %s", (epoch_id,)
        ).fetchone()
        if epoch is None:
            raise PgTargetError(f"epoch {epoch_id} does not exist in the target")

        checkpoint = connection.execute(
            "SELECT watermark, published_scan_id, lease_version"
            "  FROM sync_checkpoints"
            " WHERE epoch_id = %s AND entity = %s AND filter_hash = %s",
            (epoch_id, entity, filter_hash),
        ).fetchone()

        scan = None
        if checkpoint is not None and checkpoint[1] is not None:
            scan = connection.execute(
                "SELECT status, snapshot_seq, record_count FROM sync_runs"
                " WHERE scan_id = %s",
                (checkpoint[1],),
            ).fetchone()

        return TargetCut(
            epoch_id=epoch_id,
            epoch_status=str(epoch[0]),
            entity=entity,
            watermark=None if checkpoint is None else checkpoint[0],
            published_scan_id=None if checkpoint is None else checkpoint[1],
            lease_version=None if checkpoint is None else checkpoint[2],
            scan_status=None if scan is None else str(scan[0]),
            scan_snapshot_seq=None if scan is None else scan[1],
            scan_record_count=None if scan is None else scan[2],
        )

    def iter_rows(
        self, connection: Any, *, entity: str, epoch_id: str, max_seq: int | None = None
    ) -> Iterator[dict[str, Any]]:
        """Stream one entity's rows as dicts keyed the way P04 expects.

        ``max_seq`` bounds the Deal read to the watermark. Without it a scan
        running concurrently would contribute rows the source cut does not
        contain, and the comparison would report a surplus that is really a
        race.
        """
        if entity not in _ROW_SQL:
            raise PgTargetError(f"no projection for entity {entity!r}")
        if entity == "deals":
            if max_seq is None:
                raise PgTargetError(
                    "a deals capture needs an explicit max_seq; reading to the "
                    "end of the table would include rows beyond the cut"
                )
            params: tuple[Any, ...] = (epoch_id, max_seq)
        else:
            params = (epoch_id,)

        cursor = connection.execute(_ROW_SQL[entity], params)
        columns = [description.name for description in cursor.description]
        for row in cursor:
            yield dict(zip(columns, row, strict=True))
