# Local PostgreSQL for the integrated test run

What the suites need, why each piece exists, and how it was set up here. Without
this, 123 of Project 03's cases, one of Project 05's and one of Project 07's
report `BLOCKED_ENVIRONMENT` — which by the project's own rule is not a pass.

## What is required

A PostgreSQL server reachable on `127.0.0.1:55432`, with `fsync` on. A
durability test against a server that does not fsync proves nothing about
durability, so this is not a detail to trade for speed.

Verified against **PostgreSQL 17.6** (x86_64-windows). The components declare no
minimum beyond what their SQL uses; P07's DDL needs `pgcrypto`.

## Databases

Three, because the ownership models must not share one. Keeping them apart is
not tidiness — mixing them is contract gap CG-07, and it happened during this
very setup: Project 07's parity test created its stand-in copies of Project 03's
tables inside the database meant for the real schema.

| Database | Owner | Holds |
|---|---|---|
| `trading_sync_test` | P03 | Scratch database for P03's own suite. Created and torn down per run. |
| `broker_platform` | P07 | P07's outbox tables **plus** its deliberate stand-in for P03's. Never point the real P03 at this one. |
| `broker_risk` | P03 | The real P03 schema, from `alembic upgrade head`, plus P07's outbox tables. What P05 reads, what P04's PostgreSQL target reader reads, and where the outbox row shares the Deal's transaction. |

## Roles

| Role | Grants | Why |
|---|---|---|
| `sync_app` | `SELECT, INSERT, UPDATE` — **no DELETE** | P03's ingestion role. Deals are immutable history; an ingestion role has no reason to be able to remove it. P03's revision 0001 grants this if the role exists. |
| `risk_reader` | `SELECT` only | P05's role. Design decision 2 says P05 only reads upstream facts; this makes the database enforce it rather than trusting the code. P05's `test_postgres_role_cannot_mutate_upstream_tables` fails if you connect it as a superuser, and it is right to. |

## Setup

```sql
CREATE DATABASE broker_risk;
CREATE DATABASE broker_platform;
CREATE DATABASE trading_sync_test;
CREATE ROLE sync_app LOGIN;
CREATE ROLE risk_reader LOGIN;
```

```bash
# The real P03 schema, into its own database.
cd apps/p03-sync
SYNC_DATABASE_URL="postgresql+asyncpg://postgres@127.0.0.1:55432/broker_risk" \
  python -m alembic upgrade head
```

```sql
-- In broker_risk, after the migration.
GRANT USAGE ON SCHEMA public TO risk_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO risk_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO risk_reader;

GRANT USAGE ON SCHEMA public TO sync_app;
-- SELECT, INSERT, UPDATE per table. Not DELETE.
```

## Environment

```bash
export SYNC_TEST_DATABASE_URL="postgresql+asyncpg://postgres@127.0.0.1:55432/trading_sync_test"
export OUTBOX_PG_DSN="postgresql://postgres@127.0.0.1:55432/broker_platform"
export RISK_PG_DSN="postgresql://risk_reader@127.0.0.1:55432/broker_risk"
export RECON_TARGET_PG_DSN="postgresql://postgres@127.0.0.1:55432/broker_risk"
```

Do **not** also export `SYNC_DATABASE_URL` when running P03's suite:
`test_missing_database_url_fails_loudly_when_used` asserts the loud failure that
happens when it is absent, and setting it makes that case fail for the wrong
reason.

## Drivers

```bash
pip install "asyncpg>=0.29" "psycopg[binary]>=3.1"
```

`asyncpg` is P03's driver; `psycopg` is P05's and P07's. Both are needed for a
full run.

**Windows only:** psycopg's async path refuses the default `ProactorEventLoop`
and says so explicitly. P05's `tests/conftest.py` switches to
`WindowsSelectorEventLoopPolicy`. Without it every `RISK_PG_DSN` case fails with
"could not connect to PostgreSQL" while the code and the server are both fine —
the most misleading shape a failure can take.

## What this still does not give you

Kafka and ClickHouse. P07's slow suite uses its own `EmbeddedKafka` and P08's
runs in-process, so both pass without them — but neither has been exercised
against a real broker or a real ClickHouse server. The `P07 -> Kafka -> P08` leg
of the chain is therefore tested, not deployed. `compose.test.yml` is the
intended topology for that and has never been brought up.
