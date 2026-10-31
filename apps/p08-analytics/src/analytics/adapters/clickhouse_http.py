"""ClickHouse over the HTTP interface.

**Coverage status:** this adapter is *not* exercised by the offline test
suite - it needs a real server. Section 13.1 requires a pinned server version
and re-run compatibility tests before implementation is trusted, so
:class:`HttpClickHouse` refuses to start on the design's placeholder version
and the release gate (G2) stays open until the integration run against a real
ClickHouse is recorded. Treat the behaviour here as reviewed, not verified.

The one genuinely delicate part is :meth:`insert_raw`. A client-side timeout
does **not** prove the server did nothing: the INSERT may already be
committed, and a retry that assumed failure would double-count. So a timeout
and a dropped connection both raise :class:`AmbiguousWriteError`, never a
plain retriable error, and the caller withholds the Kafka offset
(section 3.2, ADR08-09).
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable, Sequence

from ..canonical import format_timestamp, parse_timestamp
from ..errors import AmbiguousWriteError, ConfigurationError, PoisonError, Reason, RetriableError
from ..money import format_money, parse_money
from ..ports.clickhouse import (
    DailyRollupRow,
    DealFactRow,
    DealHashGroup,
    EventIdGroup,
    InsertResult,
    RawObservation,
    StorageStats,
)
from ..settings import VERSION_PLACEHOLDER

RAW_TABLE = "broker_events_raw"
FACTS_TABLE = "analytics_deals_build"
DAILY_TABLE = "analytics_daily_build"


class HttpClickHouse:
    """Implements :class:`~analytics.ports.clickhouse.ClickHousePort` over HTTP."""

    def __init__(
        self,
        *,
        url: str,
        database: str,
        max_query_duration_ms: int = 5_000,
        version_pin: str = VERSION_PLACEHOLDER,
        username: str = "default",
        password: str = "",
        client: Any | None = None,
    ) -> None:
        if version_pin == VERSION_PLACEHOLDER:
            raise ConfigurationError(
                "ch_server_version_pin must name the ClickHouse version this "
                "deployment was tested against (section 13.1)"
            )
        self._url = url.rstrip("/")
        self._database = database
        self._timeout_s = max_query_duration_ms / 1000
        self._version_pin = version_pin
        self._auth = (username, password)
        self._client = client or self._default_client()

    def _default_client(self):
        try:
            import httpx
        except ImportError as exc:  # pragma: no cover - driver optional
            raise ConfigurationError(
                "httpx is required for the ClickHouse HTTP adapter; "
                "install the 'infra' extra"
            ) from exc
        return httpx.Client(timeout=self._timeout_s, auth=self._auth)

    # -- transport ---------------------------------------------------------

    def _execute(self, sql: str, *, body: str | None = None, read_only: bool = True) -> str:
        params = {
            "database": self._database,
            "max_execution_time": str(int(self._timeout_s)),
            "query": sql,
        }
        if read_only:
            # Defence in depth behind the role grants: a read path must not be
            # able to mutate even if the SQL is wrong (section 8.2).
            params["readonly"] = "1"
        try:
            response = self._client.post(self._url, params=params, content=body or "")
        except Exception as exc:
            name = type(exc).__name__
            if read_only:
                raise RetriableError(
                    Reason.BACKEND_UNAVAILABLE, f"clickhouse request failed: {name}"
                ) from exc
            # A write whose outcome is unknown. This is the case that must
            # never be downgraded to "it failed, retry freely".
            raise AmbiguousWriteError(
                f"clickhouse write outcome unknown after {name}", table=sql[:40]
            ) from exc

        status = getattr(response, "status_code", 500)
        text = getattr(response, "text", "")
        if status == 200:
            return text
        if status in (400, 403, 404) and not read_only:
            # The server understood and refused: structurally bad rows. Nothing
            # was written, so this is poison, not a retry (P08-H-01).
            raise PoisonError(
                Reason.INSERT_REJECTED, f"clickhouse refused the batch: {text[:300]}"
            )
        if status in (507, 509) or "disk" in text.lower() and "full" in text.lower():
            raise RetriableError(Reason.STORAGE_FULL, f"clickhouse storage error: {text[:200]}")
        if read_only:
            raise RetriableError(
                Reason.BACKEND_UNAVAILABLE, f"clickhouse error {status}: {text[:200]}"
            )
        raise AmbiguousWriteError(f"clickhouse returned {status}: {text[:200]}")

    def _insert(self, table: str, rows: Sequence[dict[str, Any]]) -> InsertResult:
        if not rows:
            return InsertResult(rows_written=0, table=table, write_marker=self._write_marker())
        body = "\n".join(json.dumps(row, sort_keys=True, default=str) for row in rows)
        self._execute(
            f"INSERT INTO {self._database}.{table} FORMAT JSONEachRow",
            body=body,
            read_only=False,
        )
        return InsertResult(
            rows_written=len(rows), table=table, write_marker=self._write_marker()
        )

    def _write_marker(self) -> int:
        """A monotonic stand-in for "how far writes have progressed".

        Uses the server's own row count rather than a local counter, so a
        restarted process does not reset it and claim a reader is ahead of
        where it is.
        """
        text = self._execute(
            f"SELECT count() FROM {self._database}.{RAW_TABLE} "
            f"UNION ALL SELECT count() FROM {self._database}.{FACTS_TABLE} FORMAT JSON"
        )
        payload = json.loads(text)
        return sum(int(row["count()"]) for row in payload.get("data", []))

    def _select(self, sql: str) -> list[dict[str, Any]]:
        text = self._execute(sql + " FORMAT JSON")
        return json.loads(text).get("data", [])

    # -- inserts -----------------------------------------------------------

    def insert_raw(self, rows: Sequence[RawObservation], *, deadline_ms: int = 5_000) -> InsertResult:
        del deadline_ms
        return self._insert(RAW_TABLE, [_raw_to_json(row) for row in rows])

    def insert_deal_facts(
        self, rows: Sequence[DealFactRow], *, deadline_ms: int = 30_000
    ) -> InsertResult:
        del deadline_ms
        return self._insert(FACTS_TABLE, [_fact_to_json(row) for row in rows])

    def insert_daily_rollups(
        self, rows: Sequence[DailyRollupRow], *, deadline_ms: int = 30_000
    ) -> InsertResult:
        del deadline_ms
        return self._insert(DAILY_TABLE, [_daily_to_json(row) for row in rows])

    # -- reads -------------------------------------------------------------

    def scan_raw_deals(
        self,
        *,
        tenant_id: str,
        source_id: str,
        epoch_id: str,
        max_source_seq: int,
        max_write_marker: int,
    ) -> Iterable[RawObservation]:
        del max_write_marker  # server-side pinning uses the seq watermark
        rows = self._select(
            f"""
            SELECT * FROM {self._database}.{RAW_TABLE}
            WHERE tenant_id = {_lit(tenant_id)} AND source_id = {_lit(source_id)}
              AND epoch_id = {_lit(epoch_id)} AND event_type = 'DealRecorded'
              AND source_seq <= {int(max_source_seq)}
            ORDER BY deal_id, event_id, kafka_partition, kafka_offset
            """
        )
        return [_json_to_raw(row) for row in rows]

    def event_id_conflicts(
        self, *, tenant_id: str, source_id: str, epoch_id: str, max_write_marker: int
    ) -> Sequence[EventIdGroup]:
        del max_write_marker
        rows = self._select(
            f"""
            SELECT event_id, count() AS physical_observations,
                   uniqExact(payload_sha256) AS distinct_hashes
            FROM {self._database}.{RAW_TABLE}
            WHERE tenant_id = {_lit(tenant_id)} AND source_id = {_lit(source_id)}
              AND epoch_id = {_lit(epoch_id)} AND event_type = 'DealRecorded'
            GROUP BY event_id ORDER BY event_id
            """
        )
        return [
            EventIdGroup(
                event_id=row["event_id"],
                physical_observations=int(row["physical_observations"]),
                distinct_hashes=int(row["distinct_hashes"]),
            )
            for row in rows
        ]

    def deal_hash_conflicts(
        self, *, tenant_id: str, source_id: str, epoch_id: str, max_write_marker: int
    ) -> Sequence[DealHashGroup]:
        del max_write_marker
        rows = self._select(
            f"""
            SELECT deal_id, count() AS physical_observations,
                   uniqExact(deal_payload_hash) AS distinct_deal_hashes
            FROM {self._database}.{RAW_TABLE}
            WHERE tenant_id = {_lit(tenant_id)} AND source_id = {_lit(source_id)}
              AND epoch_id = {_lit(epoch_id)} AND event_type = 'DealRecorded'
            GROUP BY deal_id ORDER BY deal_id
            """
        )
        return [
            DealHashGroup(
                deal_id=row["deal_id"],
                physical_observations=int(row["physical_observations"]),
                distinct_deal_hashes=int(row["distinct_deal_hashes"]),
            )
            for row in rows
        ]

    def count_deal_facts(self, *, build_id: str, tenant_id: str) -> tuple[int, int]:
        rows = self._select(
            f"""
            SELECT count() AS physical_rows, uniqExact(deal_id) AS unique_deals
            FROM {self._database}.{FACTS_TABLE}
            WHERE build_id = {_lit(build_id)} AND tenant_id = {_lit(tenant_id)}
            """
        )
        if not rows:  # pragma: no cover - aggregate always returns a row
            return (0, 0)
        return int(rows[0]["physical_rows"]), int(rows[0]["unique_deals"])

    def read_deal_facts(
        self, *, build_id: str, tenant_id: str, source_id: str, epoch_id: str
    ) -> Iterable[DealFactRow]:
        rows = self._select(
            f"""
            SELECT * FROM {self._database}.{FACTS_TABLE}
            WHERE build_id = {_lit(build_id)} AND tenant_id = {_lit(tenant_id)}
              AND source_id = {_lit(source_id)} AND epoch_id = {_lit(epoch_id)}
            ORDER BY account_id, executed_at, deal_id
            """
        )
        return [_json_to_fact(row) for row in rows]

    def read_daily_rollups(
        self,
        *,
        build_id: str,
        tenant_id: str,
        source_id: str,
        epoch_id: str,
        from_day: date | None = None,
        to_day: date | None = None,
        accounts: Sequence[str] | None = None,
    ) -> Iterable[DailyRollupRow]:
        clauses = [
            f"build_id = {_lit(build_id)}",
            f"tenant_id = {_lit(tenant_id)}",
            f"source_id = {_lit(source_id)}",
            f"epoch_id = {_lit(epoch_id)}",
        ]
        if from_day:
            clauses.append(f"trade_day >= {_lit(from_day.isoformat())}")
        if to_day:
            clauses.append(f"trade_day <= {_lit(to_day.isoformat())}")
        if accounts:
            joined = ",".join(_lit(a) for a in sorted(accounts))
            clauses.append(f"account_id IN ({joined})")
        rows = self._select(
            f"SELECT * FROM {self._database}.{DAILY_TABLE} WHERE "
            + " AND ".join(clauses)
            + " ORDER BY trade_day, account_id, symbol, money_currency"
        )
        return [_json_to_daily(row) for row in rows]

    def delete_build(self, *, build_id: str) -> int:
        physical, _unique = self.count_deal_facts(build_id=build_id, tenant_id="")
        for table in (FACTS_TABLE, DAILY_TABLE):
            self._execute(
                f"ALTER TABLE {self._database}.{table} DELETE WHERE build_id = {_lit(build_id)}",
                read_only=False,
            )
        return physical

    def storage_stats(self) -> StorageStats:
        rows = self._select(
            f"""
            SELECT table, count() AS active_parts, sum(rows) AS physical_rows,
                   sum(bytes_on_disk) AS bytes
            FROM system.parts
            WHERE active AND database = {_lit(self._database)}
            GROUP BY table
            """
        )
        return StorageStats(
            table_rows={row["table"]: int(row["physical_rows"]) for row in rows},
            active_parts={row["table"]: int(row["active_parts"]) for row in rows},
            bytes_on_disk=sum(int(row["bytes"]) for row in rows),
        )

    def visible_write_marker(self, *, reader: str = "default") -> int:
        del reader
        return self._write_marker()


def _lit(value: str) -> str:
    """Quote a string literal.

    Parameter binding over the HTTP interface is awkward, so values are
    escaped here and every caller passes identifiers that came from validated
    settings or the control plane - never from a client (section 8.2).
    """
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _raw_to_json(row: RawObservation) -> dict[str, Any]:
    return {
        "tenant_id": row.tenant_id,
        "source_id": row.source_id,
        "epoch_id": row.epoch_id,
        "event_id": row.event_id,
        "dedupe_key": row.dedupe_key,
        "kafka_record_key": row.kafka_record_key,
        "event_type": row.event_type,
        "schema_version": row.schema_version,
        "payload_sha256": row.payload_sha256,
        "deal_payload_hash": row.deal_payload_hash or "",
        "deal_id": row.deal_id or "",
        "account_id": row.account_id,
        "source_seq": row.source_seq,
        "aggregate_version": row.aggregate_version,
        "event_time": format_timestamp(row.event_time),
        "executed_at": format_timestamp(row.executed_at) if row.executed_at else None,
        "kafka_topic": row.kafka_topic,
        "kafka_partition": row.kafka_partition,
        "kafka_offset": row.kafka_offset,
        "payload_json": row.payload_json,
    }


def _fact_to_json(row: DealFactRow) -> dict[str, Any]:
    return {
        "build_id": row.build_id,
        "tenant_id": row.tenant_id,
        "source_id": row.source_id,
        "epoch_id": row.epoch_id,
        "deal_id": row.deal_id,
        "account_id": row.account_id,
        "symbol": row.symbol,
        "side": row.side,
        "entry_kind": row.entry_kind,
        "source_seq": row.source_seq,
        "executed_at": format_timestamp(row.executed_at),
        "money_currency": row.money_currency,
        # Decimals cross the wire as strings; a JSON float here would be the
        # money defect section 7.2 warns about.
        "realized_pnl_gross": format_money(row.realized_pnl_gross),
        "commission": format_money(row.commission),
        "swap": format_money(row.swap),
        "volume_lots": format_money(row.volume_lots),
        "fee_coverage": row.fee_coverage,
        "deal_payload_hash": row.deal_payload_hash,
        "event_id": row.event_id,
    }


def _daily_to_json(row: DailyRollupRow) -> dict[str, Any]:
    return {
        "build_id": row.build_id,
        "tenant_id": row.tenant_id,
        "source_id": row.source_id,
        "epoch_id": row.epoch_id,
        "trade_day": row.trade_day.isoformat(),
        "account_id": row.account_id,
        "symbol": row.symbol,
        "money_currency": row.money_currency,
        "deal_count": row.deal_count,
        "realized_pnl_gross": format_money(row.realized_pnl_gross),
        "commission": format_money(row.commission),
        "swap": format_money(row.swap),
        "volume_lots": format_money(row.volume_lots),
    }


def _json_to_raw(row: dict[str, Any]) -> RawObservation:
    return RawObservation(
        tenant_id=row["tenant_id"],
        source_id=row["source_id"],
        epoch_id=row["epoch_id"],
        event_id=row["event_id"],
        dedupe_key=row["dedupe_key"],
        kafka_record_key=row["kafka_record_key"],
        event_type=row["event_type"],
        schema_version=int(row["schema_version"]),
        payload_sha256=row["payload_sha256"],
        deal_payload_hash=row.get("deal_payload_hash") or None,
        deal_id=row.get("deal_id") or None,
        account_id=row["account_id"],
        source_seq=int(row["source_seq"]),
        aggregate_version=int(row["aggregate_version"]),
        event_time=_ts(row["event_time"]),
        executed_at=_ts(row["executed_at"]) if row.get("executed_at") else None,
        kafka_topic=row["kafka_topic"],
        kafka_partition=int(row["kafka_partition"]),
        kafka_offset=int(row["kafka_offset"]),
        payload_json=row["payload_json"],
        ingested_at=_ts(row["ingested_at"]) if row.get("ingested_at") else None,
    )


def _json_to_fact(row: dict[str, Any]) -> DealFactRow:
    return DealFactRow(
        build_id=row["build_id"],
        tenant_id=row["tenant_id"],
        source_id=row["source_id"],
        epoch_id=row["epoch_id"],
        deal_id=row["deal_id"],
        account_id=row["account_id"],
        symbol=row["symbol"],
        side=row["side"],
        entry_kind=row["entry_kind"],
        source_seq=int(row["source_seq"]),
        executed_at=_ts(row["executed_at"]),
        money_currency=row["money_currency"],
        realized_pnl_gross=parse_money(row["realized_pnl_gross"], "realized_pnl_gross"),
        commission=parse_money(row["commission"], "commission"),
        swap=parse_money(row["swap"], "swap"),
        volume_lots=parse_money(row["volume_lots"], "volume_lots", scale=8),
        fee_coverage=row["fee_coverage"],
        deal_payload_hash=row["deal_payload_hash"],
        event_id=row["event_id"],
    )


def _json_to_daily(row: dict[str, Any]) -> DailyRollupRow:
    return DailyRollupRow(
        build_id=row["build_id"],
        tenant_id=row["tenant_id"],
        source_id=row["source_id"],
        epoch_id=row["epoch_id"],
        trade_day=date.fromisoformat(row["trade_day"]),
        account_id=row["account_id"],
        symbol=row["symbol"],
        money_currency=row["money_currency"],
        deal_count=int(row["deal_count"]),
        realized_pnl_gross=parse_money(row["realized_pnl_gross"], "realized_pnl_gross"),
        commission=parse_money(row["commission"], "commission"),
        swap=parse_money(row["swap"], "swap"),
        volume_lots=parse_money(row["volume_lots"], "volume_lots", scale=8),
    )


def _ts(value: str) -> datetime:
    text = str(value)
    if text.endswith("Z") or "+" in text:
        return parse_timestamp(text)
    # ClickHouse DateTime64 renders without an offset; the column is declared
    # UTC, so the offset is supplied rather than guessed.
    return parse_timestamp(text.replace(" ", "T") + "Z")
