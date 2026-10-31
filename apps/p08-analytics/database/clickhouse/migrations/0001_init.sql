-- Project 08 - ClickHouse schema. Design section 5.2.
--
-- Status: this DDL is the production counterpart of the in-process model the
-- test suite exercises. It has NOT been applied to a real server in this
-- repository, so release gate G2 stays open until a migration run against a
-- pinned ClickHouse version is recorded (section 13.1).
--
-- Three choices here are load-bearing and should not be "optimised" without
-- re-reading the design:
--
--   1. Plain MergeTree for raw, not ReplacingMergeTree. Duplicates must stay
--      visible as evidence; a background replacement would hide conflicting
--      observations of one event without an audit (ADR08-02, section 3.5).
--   2. Raw partitions by ingestion month, curated tables by event month. A
--      replayed 2023 Deal therefore lands in a 2026 raw partition on purpose,
--      while its daily report row belongs to its own 2023 day (INV08-14).
--   3. ORDER BY is a sparse index, never a uniqueness constraint. Nothing in
--      P08 may rely on it to deduplicate (section 5.1).

CREATE DATABASE IF NOT EXISTS broker_analytics;

-- ---------------------------------------------------------------------------
-- Bronze: append-only raw observations, including legal Kafka replay
-- duplicates. Never SUM() this table (ADR08-03).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS broker_analytics.broker_events_raw
(
  tenant_id          String,
  source_id          String,
  epoch_id           UUID,
  event_id           UUID,
  dedupe_key         String,
  kafka_record_key   String,
  event_type         LowCardinality(String),
  schema_version     UInt16,
  -- Validated as 64 lowercase hex characters *before* insert. FixedString
  -- alone accepts any 64 bytes and proves nothing about hashing.
  payload_sha256     FixedString(64),
  -- Mandatory for DealRecorded; the empty sentinel is used for markers,
  -- which are routed to the control log rather than aggregated.
  deal_payload_hash  String,
  deal_id            String,
  account_id         String,
  source_seq         UInt64,
  aggregate_version  UInt64,
  event_time         DateTime64(6, 'UTC'),
  executed_at        Nullable(DateTime64(6, 'UTC')),
  ingested_at        DateTime64(6, 'UTC') DEFAULT now64(6),
  kafka_topic        String,
  kafka_partition    UInt32,
  kafka_offset       UInt64,
  -- Canonical JSON, so an auditor can recompute payload_sha256 from the row.
  payload_json       String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (tenant_id, source_id, epoch_id, event_type, ingested_at, event_id)
SETTINGS index_granularity = 8192;

-- Conflict scans group by event_id and by deal_id within a scope, which the
-- sort key above does not serve. These skip indexes keep those scans from
-- degrading into full reads as the table grows.
ALTER TABLE broker_analytics.broker_events_raw
  ADD INDEX IF NOT EXISTS idx_event_id event_id TYPE bloom_filter(0.01) GRANULARITY 4;

ALTER TABLE broker_analytics.broker_events_raw
  ADD INDEX IF NOT EXISTS idx_deal_id deal_id TYPE bloom_filter(0.01) GRANULARITY 4;

ALTER TABLE broker_analytics.broker_events_raw
  ADD INDEX IF NOT EXISTS idx_source_seq source_seq TYPE minmax GRANULARITY 4;

-- ---------------------------------------------------------------------------
-- Silver: one immutable row per Deal per build. Written only by a builder
-- holding an active lease, and never rewritten under the same build_id
-- (P08-S-10).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS broker_analytics.analytics_deals_build
(
  build_id           UUID,
  tenant_id          String,
  source_id          String,
  epoch_id           UUID,
  deal_id            String,
  account_id         String,
  symbol             String,
  side               LowCardinality(String),
  entry_kind         LowCardinality(String),
  source_seq         UInt64,
  executed_at        DateTime64(6, 'UTC'),
  money_currency     LowCardinality(String),
  -- Decimal(38,12) is a chosen bound, not a law of nature: the application
  -- checks precision, scale and sum overflow explicitly rather than waiting
  -- for the server to truncate (section 5.2).
  realized_pnl_gross Decimal(38, 12),
  commission         Decimal(38, 12),
  swap               Decimal(38, 12),
  volume_lots        Decimal(38, 12),
  fee_coverage       LowCardinality(String),
  deal_payload_hash  FixedString(64),
  event_id           UUID
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(executed_at)
ORDER BY (tenant_id, source_id, epoch_id, build_id, account_id, executed_at, deal_id);

-- ---------------------------------------------------------------------------
-- Gold: rollups derived from the validated unique facts of exactly one build.
-- money_currency is part of the sort key because it is part of the grouping:
-- there is no row in which two currencies could be added (INV08-07).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS broker_analytics.analytics_daily_build
(
  build_id           UUID,
  tenant_id          String,
  source_id          String,
  epoch_id           UUID,
  trade_day          Date,
  account_id         String,
  symbol             String,
  money_currency     LowCardinality(String),
  deal_count         UInt64,
  realized_pnl_gross Decimal(38, 12),
  commission         Decimal(38, 12),
  swap               Decimal(38, 12),
  volume_lots        Decimal(38, 12)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trade_day)
ORDER BY (tenant_id, source_id, epoch_id, build_id, trade_day, account_id, symbol, money_currency);

-- ---------------------------------------------------------------------------
-- Lightweight control log for snapshot markers. Deliberately separate from
-- the Deal raw table: a marker says P03 published a snapshot and carries no
-- holdings, so it must not be reachable by anything that aggregates money
-- (INV08-16).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS broker_analytics.broker_markers_log
(
  tenant_id        String,
  source_id        String,
  epoch_id         UUID,
  event_id         UUID,
  event_type       LowCardinality(String),
  aggregate_id     String,
  snapshot_seq     UInt64,
  event_time       DateTime64(6, 'UTC'),
  ingested_at      DateTime64(6, 'UTC') DEFAULT now64(6),
  kafka_topic      String,
  kafka_partition  UInt32,
  kafka_offset     UInt64,
  payload_json     String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (tenant_id, source_id, epoch_id, event_time, event_id);

-- ---------------------------------------------------------------------------
-- Roles. The reader has SELECT on the curated tables only; it cannot read
-- raw payloads and cannot write anything (section 8.2). The builder and the
-- operator are separate principals, so a read path cannot mutate even if its
-- SQL is wrong.
-- ---------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS p08_reader;
GRANT SELECT ON broker_analytics.analytics_deals_build TO p08_reader;
GRANT SELECT ON broker_analytics.analytics_daily_build TO p08_reader;

CREATE ROLE IF NOT EXISTS p08_ingestor;
GRANT INSERT ON broker_analytics.broker_events_raw TO p08_ingestor;
GRANT INSERT ON broker_analytics.broker_markers_log TO p08_ingestor;
GRANT SELECT ON broker_analytics.broker_events_raw TO p08_ingestor;

CREATE ROLE IF NOT EXISTS p08_builder;
GRANT SELECT ON broker_analytics.broker_events_raw TO p08_builder;
GRANT INSERT, SELECT ON broker_analytics.analytics_deals_build TO p08_builder;
GRANT INSERT, SELECT ON broker_analytics.analytics_daily_build TO p08_builder;

-- Deleting build rows is garbage collection, which is an audited operator
-- action with its own protections (INV08-19). It is not granted to the
-- builder that creates them.
CREATE ROLE IF NOT EXISTS p08_operator;
GRANT SELECT, INSERT, ALTER DELETE, OPTIMIZE
  ON broker_analytics.analytics_deals_build TO p08_operator;
GRANT SELECT, INSERT, ALTER DELETE, OPTIMIZE
  ON broker_analytics.analytics_daily_build TO p08_operator;
GRANT SELECT, OPTIMIZE ON broker_analytics.broker_events_raw TO p08_operator;

-- NOTE: no TTL clause is declared on any table. Raw observations are the
-- evidence behind every published report, and an automatic expiry would
-- quietly remove the ability to reproduce one (INV08-19, P08-AC). Retention
-- is a governed operator decision with an archive, not a DDL default.
