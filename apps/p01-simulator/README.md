# MT5 Broker Simulator

**Project 01 of 09** in the *Broker Risk Reporting and MT5 Integration* learning
roadmap. A deterministic, stateful synthetic broker data source with
reproducible fault injection, built to be the test double that Project 02 (MT5
Integration Connector) and everything downstream is tested against.

Implements [`docs/Project_01_MT5_Broker_Simulator_System_Design.md`](../docs/Project_01_MT5_Broker_Simulator_System_Design.md).
Section references throughout this file point there.

---

## What this is not

**This is not the MT5 Manager API.** The REST surface is a *simulated test
contract*. `run_id`, `generation`, `snapshot_seq` and `source_seq` are
semantics this simulator defines for testing purposes and carry no claim that a
real MT5 Manager SDK exposes anything equivalent — Project 02's adapter is what
hides the difference. `GET /v1/meta` says so in its own payload
(`wire_compatible_with_mt5_manager_api: false`).

All data is synthetic. There is no trading, no order entry, no real account,
and no MT5 credential anywhere in this repository.

The design document's SLO figures are **targets to measure**, not results that
have been achieved. Where this README states a number, it states the
environment it came from.

---

## Quick start

```bash
cd mt5-broker-simulator
make install                 # or: pip install -e '.[test]'

make migrate                 # create the schema
make generate SCENARIO=scenarios/paging_10k.json
make verify                  # run the invariant suite; non-zero exit on failure
make serve                   # http://127.0.0.1:8001  (docs at /docs)
```

Loopback runs derive development tokens automatically. Any non-loopback bind
requires `SIM_DATA_TOKEN`, `SIM_ADMIN_TOKEN` and `SIM_CURSOR_SIGNING_KEY`, and
startup fails closed without them. Copy [`.env.example`](.env.example) and edit.

```bash
docker compose up --build -d   # needs the three SIM_* secrets exported
```

---

## The two planes

A **data plane** (`GET /v1/*`) that can only read, and an **admin plane**
(`/admin/v1/*`) that can create runs, reset generations and arm faults. They
use different tokens, and a data-plane token at an admin route gets `403`.

That separation is the point: the data-plane token is what you hand to the
system under test, and nothing the system under test can do should be able to
reset its own data source — a run that could would invalidate every result
produced with it.

### Data plane

| Route | Purpose |
|---|---|
| `GET /v1/meta` | Capabilities, active run, `snapshot_seq`, virtual clock |
| `GET /v1/accounts` | Accounts at a snapshot |
| `GET /v1/accounts/{id}` | One account, optionally `?as_of_seq=` in the past |
| `GET /v1/positions` | Positions, with nullable derived `unrealized_pnl` |
| `GET /v1/deals` | Deals; filter by account, symbol, time range or `after_seq` |
| `GET /v1/marks` | Simulated quotes, with a `stale` flag |
| `GET /health/live` | Process liveness. No query, no lock, never faulted |
| `GET /health/ready` | Readiness for *consistent* reads |

### Admin plane

`POST /admin/v1/runs`, `GET /runs/current`, `POST /runs/{id}/{reset,stop,advance,events}`,
`GET /jobs/{id}`, `POST /jobs/{id}/cancel`, `/faults` (`POST`/`GET`/`DELETE`),
`POST /faults/arm`, `GET /diagnostics/invariants`, `GET /metrics`.

Every operation that creates state requires an `Idempotency-Key`. Those are
exactly the calls whose response can be lost, and a retry without a key would
create a second run or a second event.

---

## Conventions a consumer can rely on

- **Money, prices and volumes are JSON strings**, never JSON numbers. A JSON
  number is a double in most clients, and `0.1` is not `0.1` after a round
  trip. On input a float is *rejected*, not coerced.
- **A missing valuation is `null`, never `0`.** No quote in the snapshot means
  unknown. `GET /v1/accounts/{id}` withholds `equity` and `unrealized_pnl`
  entirely and says `data_quality: "MARK_MISSING"` rather than publish a
  partial total as if it were complete.
- **`after_seq` is exclusive**; time ranges are `[from_time, to_time)`, so
  adjacent windows tile without double-counting a boundary deal.
- **Every list response carries `run_id`, `generation` and `snapshot_seq`**, so
  a consumer can detect that it is reading a different run than it started
  with.
- **`next_cursor` is opaque and HMAC-signed.** `null` means this *snapshot* has
  no further pages — not that no more data will ever exist.
- **Timestamps are RFC 3339 UTC.** Business time comes from the scenario's
  virtual clock, not the host clock.
- **Errors use one envelope** with a stable `code`. `400 INVALID_CURSOR` means
  "fix your request"; `410 SNAPSHOT_EXPIRED` / `RUN_REPLACED` mean "start a new
  sync". A client that confuses them either retries forever or resyncs for
  nothing.

The one deliberate exception: the `malformed_json` fault does not follow the
error contract. That is what it is for.

---

## How correctness is held together

**Versioned projections, not current-state tables.** Accounts, positions and
marks are stored as half-open version intervals `[valid_from_seq,
valid_to_seq)`. A reader at `snapshot_seq = S` selects the version valid at
`S`. Filtering a current-state table by `updated_seq <= S` looks equivalent and
is not: once a row is updated after `S`, its pre-`S` value is gone and a
multi-page scan silently loses it.

**One writer, one transaction.** The event log, every projection version, the
deal and the idempotency record all commit together. `seq` is allocated as
`committed_seq + 1` *inside* that transaction, so a rollback consumes no number
and the sequence stays contiguous. Statements per trading event: 8.

**Keyset pagination, never OFFSET.** `LIMIT 100 OFFSET 900000` makes SQLite
walk 900k rows to discard them; page 1 is instant and page 9000 times out.

**Determinism is counter-based.** Every generated value is derived from
`SHA-256(version | seed | spec_digest | purpose | index)`. A single seeded RNG
would not do: its output depends on the order and number of calls, so adding
one draw changes every dataset while the seed stays the same.

**Faults mutate a copy of the response, never the store.** The canonical digest
is identical before and after every response-layer fault, which is what keeps
the simulator usable as an oracle for the system being tested.

### Architecture decisions

| ADR | Decision | Why |
|---|---|---|
| 001 | REST contract instead of the real Manager SDK | No broker credential or Windows SDK needed; automatable |
| 002 | SQLite WAL + versioned projections | Little infrastructure, atomic state, real as-of reads |
| 003 | Single writer, one active run | Contiguous `seq`, deterministic state, one transaction boundary |
| 004 | Synthetic data + virtual clock + content digest | Reproduce bugs exactly; never touch sensitive data |
| 005 | Decimal as JSON strings | No binary float anywhere near an accounting value |
| 006 | Faults outside canonical truth | Test a client's failure handling without corrupting the oracle |
| 007 | Snapshot cursors with a TTL | No missing or duplicated record during a concurrent scan |

---

## Invariants

`INV-01..INV-16` are implemented in
[`src/simulator/domain/invariants.py`](src/simulator/domain/invariants.py) and
runnable three ways: `make verify`, `GET /admin/v1/diagnostics/invariants`, and
`scripts/collect_evidence.py`.

Eleven are **storage-scope** — provable from the database, so the suite checks
them. Five are **harness-scope**: they need an HTTP client, a second process, a
replay or load. Those report `NOT_APPLICABLE` *and name the test that proves
them*. They are never reported as passing.

```
INV-01 seq contiguous 1..committed_seq          storage
INV-02 deal_id unique within a run              storage
INV-03 deals reference a real account/position  storage  (also a DB foreign key)
INV-04 no two versions valid at one seq         storage
INV-05 OPEN volume > 0, CLOSED volume = 0       storage
INV-06 closed volume never exceeds opened       storage
INV-07 balance recomputes from the deal ledger  storage  (to the cent)
INV-08 unrealized uses an in-snapshot mark      storage
INV-09 same seed -> same canonical digest       harness  tests/integration/test_determinism.py
INV-10 pages stay within the snapshot           harness  tests/integration/test_pagination_snapshot.py
INV-11 a full scan returns each record once     harness  tests/integration/test_pagination_snapshot.py
INV-12 faults never change the store            harness  tests/integration/test_fault_injection.py
INV-13 one idempotency key, one effect          storage
INV-14 nothing beyond the committed watermark   storage
INV-15 one active run, replaced runs marked     storage
INV-16 queue/memory/concurrency stay bounded    harness  tests/performance/test_baseline_benchmark.py
```

Two properties of the suite worth knowing:

- **It is snapshot-consistent.** Every check is bounded by one `committed_seq`
  read at the start, so it can run while generation is in flight without
  raising false alarms. `INV-14` deliberately reads its own watermark *after*
  `MAX(seq)`, so a concurrent commit can only make it pass, never fail falsely.
- **It is tested against deliberate violations.** `tests/integration/test_invariant_sweep.py`
  tampers with the database behind the service's back — deletes an event, clones
  a deal, overlaps two versions, shifts a balance by one cent — and asserts each
  check catches it. An invariant that cannot fail has never been shown to check
  anything.

`scope=fast` runs only the aggregate checks and reports the rest as `SKIPPED`,
and a fast sweep never reports `storage_scope_ok`. Release gates use
`scope=full`.

---

## Fault injection

Response-layer faults are declared through `POST /admin/v1/faults`:
`http_error`, `delay_before_response`, `drop_one_item`, `duplicate_one_item`,
`shuffle_items`, `omit_field`, `replace_field_value`, `malformed_json`,
`wrong_content_type`, `truncate_response`, `stale_snapshot`.

Triggers are `always`, `nth_matching_request` (so "request 3 fails" means
exactly request 3) or `probability` — and probability is deterministic, derived
from `(seed, rule_id, match_index)`, so a failing chaos run replays exactly.

Guardrails that are not optional:

- `max_activations` and `ttl_seconds` are mandatory, and `delay_ms` is capped.
  A mistyped rule cannot wedge the service.
- Health and the control plane can never be faulted — declaring such a rule is
  refused at configuration time, and a broad rule still cannot reach them.
- At most one body-mutating action applies per request. Composing two would
  produce a payload no test described.
- An activation is counted only once the fault was actually **applied**.
  Counting at selection time would report a fault as injected when the handler
  errored and the mutation never ran — and a chaos result whose activation
  count is a guess proves nothing.

**Not configurable here, on purpose:** TCP reset, half-open connections, DNS
failure and MTU changes need a network proxy; a middleware cannot produce them,
and a rule claiming to would make a test that proves nothing. Disk full,
read-only mounts and `SIGKILL` are driven by named failpoints
([`persistence/failpoints.py`](src/simulator/persistence/failpoints.py)) and the
chaos suite.

---

## Tests

```bash
make unit-smoke        # every commit, blocks merge
make contract          # every commit
make p0-integration    # every PR, blocks merge
make property          # every PR
make chaos             # nightly / pre-release
make baseline-benchmark
make test-all          # everything
```

| Suite | What it proves |
|---|---|
| `tests/unit` | Golden Decimal vector from section 3.4 to the cent; every rejection rule; cursor signing and tamper detection; PRNG call-order independence; fail-fast configuration |
| `tests/contract` | Response envelope, decimals as strings *on the wire*, error codes, page-size limits, plane separation, injection resistance, log redaction |
| `tests/integration` | INV-10/11 against an independent oracle; a writer committing mid-scan; idempotent replay; A/B determinism across two databases; frozen golden digests per scenario; every fault mode with the digest unchanged; reset and generation isolation; mutation testing of the invariant suite itself |
| `tests/property` | The engine against a reference model written from the spec, plus a stateful machine asserting agreement after every single step; metamorphic relations (page size, replay, rejection, read order) |
| `tests/chaos` | `kill -9` on each side of `COMMIT` via named failpoints in a child process; simulated ENOSPC and read-only storage; a held exclusive lock; a corrupt database *copy*; restart over a persistent volume |
| `tests/performance` | Bootstrap throughput, read latency distribution, overload and recovery — each writing a manifest with its environment |

Crash tests run the child with `writer_batch_size=1`, so "the failpoint at
sequence N" means exactly one event rather than whichever batch contained it.
The child dies via `os._exit` — no unwinding, no atexit, no SQLite cleanup —
because a graceful shutdown would prove nothing about WAL recovery.

---

## Measured results

Measured on the development host during implementation:
**Windows 11, Python 3.14.4, SQLite 3.50.4, 8 logical CPUs, local SSD.**

This is **not** the section 9.1 reference environment (Linux x86-64, 4 vCPU,
8 GiB, SSD, Docker limits applied), so these are measurements, not a verdict on
the SLOs. `scripts/run_benchmark.sh` records the environment with every run and
`artifacts/<run_id>/scorecard.json` states which case applied.

| Metric | Target (§9.2) | Measured | Status |
|---|---|---|---|
| Bootstrap 100k deals | ≤ 300 s | **181.5 s** measured (141,771 events, 551 deals/s) | target met, off-reference host |
| Peak RSS during generation | ≤ 1.5 GiB | **75 MiB** measured | target met |
| Restart to ready | ≤ 30 s | **< 1 s** measured | target met |
| Invariant sweep (`scope=full`) | not specified | **10.5 s** measured at 40k deals | recorded |
| Canonical digest | not specified | **16 s** measured at 40k deals | recorded |
| Read p95 / throughput | ≤ 300 ms / ≥ 100 rps | not measured on a reference host | **NOT RUN** |

The sweep and digest rows are measured at 40k deals, not 100k. The 100k numbers
quoted during development predate the fixes below and are not comparable, and
an extrapolation is not a measurement — run
`scripts/run_benchmark.sh --profile baseline-100k` to record them.

Two findings from benchmarking are worth recording because they were real
defects, not tuning:

1. The invariant sweep originally used a self-join to detect overlapping
   version intervals. A 100k-deal run holds ~40k mark versions across three
   symbols — ~270M pairs — so the check did not run slowly, it hung. Replaced
   with an adjacent-pair `LEAD` comparison, which is sufficient for ordered
   half-open intervals and `O(n log n)`.
2. The canonical digest parsed and re-serialized each stored event payload.
   Folding the already-canonical stored string in directly was 23× faster *and*
   more faithful, since it hashes the bytes that were actually committed.

### Not tested

Stated explicitly rather than left as an absence:

- **Connector end-to-end resilience** — `NOT TESTED`. This project proves that
  faults fire as configured and that its own truth survives them. Whether a
  consumer recovers is Project 02's question (section 8).
- **Power-loss durability** — `NOT TESTED`. `kill -9` is not a power cut
  (WF-087).
- **Real MT5 Manager SDK parity** — out of scope by design.
- **Multi-process / HA** — unsupported, and startup refuses a multi-worker
  configuration rather than hoping SQLite locking is enough.
- **The container image** — `NOT BUILD-VERIFIED`. Docker was not available on
  the development host, so [`Dockerfile`](Dockerfile) and
  [`docker-compose.yml`](docker-compose.yml) are written but never built or
  run. Treat the first `docker compose up` as an unverified step. Everything
  else in this README was executed.
- **Reference-host SLO numbers** — the latency and throughput targets need the
  section 9.1 host. Run `scripts/run_benchmark.sh --profile baseline-100k
  --enforce-slo` there to turn them into assertions.

---

## Evidence bundle

```bash
python scripts/collect_evidence.py
```

Writes `artifacts/<run_id>/` with `manifest.json` (git SHA, interpreter, SQLite
version, host, run and seed), `scenario.json`, `fault_rules.json`,
`invariant_results.json`, `record_counts.json`, `what_if_results.json`,
`benchmarks.json`, `canonical_state.sha256` and `scorecard.json`.

The collector never invents a `PASS`. A skipped invariant is reported skipped, a
harness-scope invariant is reported as proven-elsewhere with the test name, and
a benchmark lacking its environment manifest is reported `INVALID_RESULT`
rather than published.

### What-if coverage

`what_if_results.json` maps all 127 cases from section 7 by **mechanically
scanning the test suite for `WF-nnn` references** — not from a hand-maintained
list that could claim a test covers something it never mentions. Current state:

| Status | Count | Meaning |
|---|---|---|
| `REFERENCED_BY_TEST` | 113 | A test names this case and lists the file |
| `NOT_TESTED` | 14 | A recorded decision, each with its reason |
| `NOT_MAPPED` | 0 | Nobody decided — treated as untested |

All **77 P0 cases** are referenced by at least one test, which is what gate G1
requires. The 14 `NOT_TESTED` cases are all P1 or P2 and need something this
project does not have: a network proxy (WF-070/071), a multi-runtime CI matrix
(WF-010), a constrained container (WF-125), a specific filesystem (WF-127), or
simply the long stress and soak runs that have not been performed
(WF-094/095/097/098/100).

A reference means a test *names* the case, not that the case is exhaustively
covered — and 127 cases are a structured inventory, not a proof of fault
tolerance. That is what the property-based, stateful, metamorphic and mutation
tests are for.

---

## Reproducibility

A dataset is a function of `seed + scenario spec + generator version` and
nothing else — not wall-clock time, not how long generation took, not how many
reads arrived while it ran, and not which process did it.

```bash
python -m simulator.cli digest    # canonical content digest of the active run
```

The digest covers event order, type, payload and virtual time; every deal; and
the account, position and mark versions at the snapshot. It deliberately
excludes `run_id`, `generation`, wall-clock `created_at` and job ids — those
differ between two legitimate replays of the same seed, so including any of
them would make the digest never match and prove nothing.

Current generator version: **1.0**; canonical encoding version **2**. A seed
only reproduces a dataset within one of each.

Six scenarios have their digests **frozen** in
[`tests/integration/test_golden_fixtures.py`](tests/integration/test_golden_fixtures.py),
recorded after the first fully green suite as section 10.2 requires. A/B
determinism proves a seed reproduces itself within one build; a frozen digest
proves the dataset has not changed since it was reviewed — without one, a
refactor that silently alters generated data passes every other test, because
both sides of every comparison move together.

If a golden test fails it is not flaky. Either a generation algorithm changed
(bump `GENERATOR_VERSION`) or the canonical encoding changed (bump
`CANONICAL_VERSION`); re-freezing without deciding which defeats the point. The
suite also asserts that every shipped scenario is either frozen or explicitly
accounted for, so a fixture cannot be added without deciding how it is
verified.

---

## Layout

```
src/simulator/
├── main.py            app factory, middleware stack, lifespan
├── settings.py        typed configuration with fail-fast validation
├── canonical.py       canonical JSON, timestamps, content digests
├── cli.py             migrate / serve / generate / verify / digest
├── api/               data plane, admin plane, health, schemas, error contract
├── domain/            entities, Decimal accounting, event engine, invariants
├── generation/        virtual clock, counter-based PRNG, generator, coordinator
├── persistence/       schema, writer, read repository, cursors, failpoints
├── faults/            rule model, matcher, response middleware
├── security/          bearer auth and plane separation, rate limit, admission
└── observability/     Prometheus metrics, structured logging with redaction
```

---

## Handoff to Project 02

Before this is used as a test double, the release gates in section 13 apply.
Where they stand:

- **G0 — data correctness.** Storage-scope invariants pass on every generated
  dataset; the section 3.4 golden vector matches to the cent; A/B replay of one
  seed produces an identical digest; idempotent retry and both crash boundaries
  add no duplicate; paging a changing snapshot loses and repeats nothing.
- **G1 — fault containment and recovery.** Every response fault mode fires as
  configured with the canonical digest unchanged; storage errors, a held lock,
  a corrupt copy and `kill -9` on both sides of commit all behave as contracted;
  overload is refused in a named, bounded way and recovers without repair.
- **G2 — performance and handoff.** The OpenAPI contract, scenario fixtures and
  evidence tooling are in place, and bootstrap, memory and restart targets are
  met on this host. The read latency and throughput targets still need the
  section 9.1 reference environment, and connector end-to-end resilience stays
  `NOT TESTED` until Project 02 exists.

What Project 02 should build against: `GET /v1/meta` for capability
negotiation, the signed opaque cursor for stable multi-page snapshots,
`after_seq` for incremental Deal sync, and `run_id`/`generation` to detect a
reset. The simulator will tell you when a snapshot is gone (`410`) rather than
quietly serving something else — treat that as the signal to start a new sync.
