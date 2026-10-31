# Project 03 — Trading Data Synchronization

Ingests MT5 **account**, **position** and **deal** data from the Project 02
connector into PostgreSQL, and owns every durable decision about what has been
synced. Implements
[`docs/Project_03_Trading_Data_Synchronization_System_Design.md`](../docs/Project_03_Trading_Data_Synchronization_System_Design.md).

The design's own framing is worth keeping in view: the system either **syncs
correctly with evidence**, or it **stops in a controlled way** with incomplete
state. It never silently drops data, publishes a half-finished snapshot, mixes
two source epochs, or lets a buffer grow without bound.

> **Status.** Runs end to end against a real PostgreSQL and a fake Project 01
> source. The P0 release gate is **not** satisfied: 166 of 169 P0 what-if cases
> pass, and the three outstanding ones cannot be verified in this environment.
> See [Residual risk](#residual-risk). Numbers under
> [Measurements](#measurements) are measurements on one machine, not validated
> SLOs.

---

## Why it is shaped this way

Two source behaviours drive the whole design.

**Deals are immutable and carry an event sequence**, so they can be replayed
incrementally: read everything above the durable watermark, insert
idempotently, and move the watermark only once the entire snapshot is proven
read. The last clause is the point — committing 199 of 200 pages and then
advancing the watermark would skip a page permanently, and nothing downstream
could ever notice.

**Accounts and positions have neither property.** The source returns current
state with no change feed and no deletions, so there is no way to learn
"account 7 was removed" from a stream of updates. The only evidence is its
absence from a listing known to be complete. Hence: read the whole snapshot
into staging, then promote it in **one transaction** that both upserts what was
present and marks everything else absent.

Those two strategies are why the code has two services rather than one
generalised path.

---

## Layering

```
src/trading_sync/
├── domain/           pure rules: identifiers, money, time, hashing, page validation
├── application/      orchestration: epoch guard, lease, deal replay, snapshot publish
├── infrastructure/   PostgreSQL sink, HTTP source adapter, metrics, logging, retention
├── status/           read-only health and freshness surface
└── cli.py            operational commands
```

`domain` performs no I/O, reads no clock and touches no database, so the money
and identity vectors can be exercised exhaustively without a server. Its
guiding rule is **reject, do not repair**: every coercion it could perform —
rounding a thirteenth decimal place, reading a naive timestamp as server-local,
turning an unknown side into `BUY` — would silently change a number or a fact
somebody is accountable for.

---

## The guarantees, and where they live

| Guarantee | Enforced by | Verified by |
|---|---|---|
| No deal below the watermark is ever skipped (INV-01/02) | terminal checkpoint transaction asserts a gapless receipt chain, one terminal page, a live lease, an ACTIVE epoch and a compare-and-set on the old watermark | `tests/integration_pg/test_checkpoint.py`, `tests/crash/` |
| A stored deal's money never changes (INV-05) | `ON CONFLICT DO NOTHING` plus a content-hash comparison under `FOR SHARE`; **and** a PostgreSQL trigger rejecting `UPDATE`/`DELETE` on `deals` | `test_deal_sink.py`, `test_schema.py` |
| A reader sees the old snapshot or the new one, never a mixture (INV-07) | staging, then one promotion transaction | `test_snapshot.py`, `tests/crash/test_snapshot_crash.py` |
| Absent never means closed (INV-08) | `presence` is separate from `source_status`, and only an unfiltered scan may publish | `test_snapshot.py`, `test_compound_faults.py` |
| Two source epochs never mix (INV-14) | `epoch_id` in every primary key; a reset fences the old epoch and requires explicit approval | `test_epoch_and_cross_entity.py` |
| A fenced worker cannot publish (INV-09) | lease token **and** monotonic version re-read under a row lock, judged by the **database** clock | `test_leases.py` |
| Memory and connections stay bounded (INV-15) | one page in flight, no prefetch, bounded pool, response byte cap enforced *while reading* | `test_admission_and_lifecycle.py`, `tests/performance/` |

### What this layer cannot prove

A source that silently omits a deal and then reports a terminal page produces a
page chain that validates perfectly: consistent provenance, monotonic
sequence, gapless receipts, one terminal page. **No client-side rule can detect
it.** Completeness is not derivable from the metadata a source supplies.

That limit is pinned down by two tests rather than left as a footnote —
`test_validation.py::test_validation_cannot_detect_a_silently_omitted_record`
and `test_full_sync.py::test_a_source_that_claims_a_false_terminal_is_caught_only_by_the_oracle`.
Catching it needs an independent oracle, which is why the harness never uses
the fault-injected endpoint as its own expectation, and why Project 04's
reconciliation exists.

---

## Running it

### 1. A PostgreSQL to work against

The P0 gates for transactions, crash windows and lease fencing cannot be met by
mocks, so the suite needs a real server. `scripts/pg_local.sh` provisions a
throwaway cluster with durability **on**:

```bash
PG_BIN=/path/to/postgresql/bin PG_DATA=$PWD/.pgdata PG_PORT=55432 \
  scripts/pg_local.sh start
```

It creates `trading_sync` and `trading_sync_test`, plus the two roles the
design requires: `sync_migrator` holding DDL and `sync_app` holding DML only —
no `CREATE`, and no row removal.

### 2. Migrate

```bash
export SYNC_DATABASE_URL="postgresql+asyncpg://postgres@localhost:55432/trading_sync"
PYTHONPATH=src python -m alembic upgrade head
```

### 3. Sync

```bash
PYTHONPATH=src python -m trading_sync.cli enroll
PYTHONPATH=src python -m trading_sync.cli run --entity deals
PYTHONPATH=src python -m trading_sync.cli status
```

There is deliberately **no command that sets a watermark**. A watermark with no
receipt chain behind it is an unverifiable claim, and the publication guard
would refuse it anyway.

---

## Testing

```bash
export SYNC_TEST_DATABASE_URL="postgresql+asyncpg://postgres@localhost:55432/trading_sync_test"
PYTHONPATH="src;." python -m pytest -q            # everything
PYTHONPATH="src;." python -m pytest -m unit -q    # no database needed
make test-p0                                      # the gate suites
```

Without `SYNC_TEST_DATABASE_URL` the database-backed suites **skip**, and their
cases are reported `BLOCKED_ENVIRONMENT` — never `PASS`. A green run on a
machine with no database must not look like a satisfied gate.

| Layer | Marker | Count | What it establishes |
|---|---|---:|---|
| Pure unit | `unit` | 222 | Money exactness, time, identifiers, hashing, page-chain rules, retry policy, redaction, transport mapping |
| SQL integration | `integration` | 77 | Real transactions, constraints, lease fencing, publication atomicity, induced DB faults |
| Crash windows | `crash` | 9 | Kill points around every commit boundary |
| End-to-end | `e2e` | 29 | Full pipeline against the fake source, checked against its oracle |
| Property | `property` | 7 | Hypothesis searches over the pure layers |
| Model-based | `property` + `e2e` | 5 seeds | Random action sequences, invariants asserted after *every* transition |
| Performance | `performance` | 4 | Throughput, latency distribution, memory and pool bounds |

**350 passing, 1 skipped** (the soak, see below).

### Evidence artifacts

```bash
python scripts/whatif_report.py --require-p0
```

`artifacts/what_if_results.json` records each case the suite exercised;
`artifacts/whatif_coverage.json` merges that with the design's matrix. The case
list is **parsed from the design document**, so the report cannot drift from
its own source of truth, and a case no test covers appears as `NOT_TESTED` and
counts against the gate rather than vanishing.

The harness's own honesty is tested too (`tests/unit/test_harness_integrity.py`):
an injector that never fired fails its case, a barrier that was never crossed
fails its case, faults and barriers cannot leak between cases, and a marker
naming a case the design does not define is surfaced instead of being absorbed
as coverage.

### Measurements

From `artifacts/benchmark_deal_ingestion.json`, on one 8-vCPU Windows laptop
with `fsync=on` and `synchronous_commit=on`:

| Metric | Value |
|---|---|
| 10,000 deals, 500/page | 5.2 s (~1,940 deals/s) |
| Page commit p50 / p95 | 193 ms / 326 ms |
| Resident memory growth | 2.4 MiB |

The memory figure is the substantive one: flat growth across 20 pages shows the
pipeline streams rather than buffers, which is what keeps the design's 1M-deal
case from becoming an OOM. **These are measurements, not validated SLOs** — the
design's targets are explicitly provisional, and a single machine cannot
validate them.

---

## Residual risk

Three P0 cases are **not** verified here, so the release gate is not satisfied:

| Case | Why not | What it would take |
|---|---|---|
| WF3-112 — disk full during a deal batch | A real out-of-space condition cannot be induced safely on a shared disk. The SQLSTATE mapping (`53100`) and the containment path are tested; the physical condition is not. | A dedicated small filesystem or a loopback volume filled on purpose |
| WF3-160 — host power loss after COMMIT ack | Cannot be produced in software. The design itself defers this to "test hardware assumptions". | A host with controllable power and a durable volume |
| WF3-210 — 60-minute soak | The test exists and runs, but refuses to certify a run shorter than the design's hour. | `SYNC_SOAK_SECONDS=3600 pytest -m slow` |

Known limits beyond the matrix:

* **No cross-entity consistency.** Accounts, positions and deals each have
  their own cut, and `/sync/status` reports `cross_entity_consistent: false`
  unconditionally. The flag exists so a consumer must acknowledge that rather
  than assume otherwise. Project 04 owns reconciliation.
* **Simulator semantics only.** `run_id`, `generation`, `source_seq` and stable
  snapshot pagination are Project 01 behaviours. A real MT5 Manager SDK is not
  assumed to provide them; the capability gate refuses watermark-based
  incremental sync for a source that does not declare the guarantee, rather
  than inventing one.
* **Amendments to settled history are invisible here.** Once a deal is below
  the watermark it is never re-read, because `after_seq` is exclusive. A source
  correcting old history is Project 04's problem, not this layer's.
* **The oracle is a fake source.** It reproduces the simulator's contract
  faithfully enough to test this service, but passing against it proves
  correctness *within that contract*, not against a real broker.

---

## Handoff to Projects 04–06

Downstream consumers read `accounts_current`, `positions_current`, `deals`,
plus `sync_runs`, `sync_audit` and `quarantine_items` for provenance. Per
entity they get `(source_id, epoch_id, watermark/snapshot_seq, published_at,
presence)`.

Three things not to misread:

* `source_seq` is an **event sequence**, not a timestamp and not a deal count.
  The distance between it and the source's committed sequence includes non-deal
  events, so it is not a count of missing deals.
* `presence = ABSENT` means *not in our last complete snapshot*. It does not
  mean closed.
* A published snapshot is current **as of its `snapshot_seq`**, not as of now.
