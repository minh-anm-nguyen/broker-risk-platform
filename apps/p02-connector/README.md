# Project 02 — MT5 Integration Connector

An async, read-only port over a **blocking** broker driver. Implements
`docs/Project_02_MT5_Integration_Connector_System_Design.md`.

The connector reads accounts, positions and deals from a source that speaks
the Project 01 `v1` contract, hands validated pages to a consumer, and gets
out of the way. What it is actually for is everything it refuses to do:

- A blocking call never runs on the event loop.
- A caller's deadline expiring does **not** cancel the native call and does
  **not** free the session slot.
- A result that arrives after its session was replaced is discarded, never
  delivered.
- A page is emitted only when its provenance matches the scan that asked for
  it, and never partially.
- No financial value is rounded, re-signed, defaulted or coerced from a float.
- No checkpoint is ever advanced here. That belongs to the sync service.

---

## Quick start

```bash
cd mt5-connector
make PY=../.venv/Scripts/python.exe install     # or: pip install -e ".[test]"
cp .env.example .env                            # then set a real token

# the per-commit suite
make PY=../.venv/Scripts/python.exe smoke

# the full evidence run, including the chaos and benchmark lanes
make PY=../.venv/Scripts/python.exe whatif
make PY=../.venv/Scripts/python.exe scorecard
```

Against a running source:

```bash
export CONNECTOR_SOURCE_ID=broker-sim-01
export CONNECTOR_BASE_URL=http://127.0.0.1:8000
export SIMULATOR_DATA_TOKEN=...            # a data-plane token

python -m mt5_connector.cli probe          # handshake, print the capabilities
python -m mt5_connector.cli scan --entity deals
python -m mt5_connector.cli serve          # health + metrics on :8080
```

---

## Using it as a library

```python
from mt5_connector.client import Connector
from mt5_connector.config import load_settings
from mt5_connector.ports import Entity, ReadPlan
from mt5_connector.scan import Scanner

connector = Connector(load_settings())
await connector.connect()

scanner = Scanner(connector)

async def persist(page):
    # Your transaction. Awaited before the next page is fetched, so a slow
    # sink slows the producer instead of filling memory.
    ...

result = await scanner.run(ReadPlan(entity=Entity.DEALS, limit=100, after_seq=watermark),
                           sink=persist)

if result.complete:
    candidate = result.checkpoint_candidate   # commit this *after* your commit
```

`result.checkpoint_candidate` is `None` for every outcome except `COMPLETE`.
There is no method anywhere in this package that advances a watermark — see
[ADR-207](docs/adr/README.md#adr-207).

---

## How it works

```
caller
  │  read_page(plan, deadline_ms)
  ▼
admission ──► breaker ──► session ──► bounded mailbox ──► worker thread ──► driver
  │             │           │              │                    │             │
  │             │           │              │                    │        blocking I/O
  │             │           │              │                    ▼
  │             │           │              │              completion
  │             │           │              ▼                    │
  │             │           │       queue-wait drop             ▼
  │             │           ▼                              epoch fence
  │             ▼      FSM + epochs                             │
  ▼        fast-fail                                            ▼
fast-fail                                              validate → Page
```

| Module | Owns | Will not |
|---|---|---|
| [`ports.py`](src/mt5_connector/ports.py) | the async interface, `ReadPlan`, `Page`, `CheckpointCandidate` | expose a write or trade operation |
| [`config.py`](src/mt5_connector/config.py) | validated settings, the URL allowlist | clamp a limit, or accept a caller's host |
| [`admission.py`](src/mt5_connector/admission.py) | bounded admission, scan slots | park a caller instead of refusing it |
| [`actor.py`](src/mt5_connector/actor.py) | the mailbox, the worker, the single active call | let a timeout release the session slot |
| [`supervisor.py`](src/mt5_connector/supervisor.py) | the FSM, epochs, fencing, the watchdog | claim it stopped a hung thread |
| [`retry.py`](src/mt5_connector/retry.py) | attempts, full jitter, the breaker | retry invalid data or a 401 |
| [`validation.py`](src/mt5_connector/validation.py) | status mapping, envelope, provenance, ordering | repair a payload |
| [`dto.py`](src/mt5_connector/dto.py) / [`scalars.py`](src/mt5_connector/scalars.py) | strict decoding | coerce a float into money |
| [`scan.py`](src/mt5_connector/scan.py) | snapshot continuity, bounded memory, outcomes | claim a guarantee it stopped checking |
| [`telemetry.py`](src/mt5_connector/telemetry.py) | metrics, redaction, the loop monitor | put a business id in a metric label |

### The two things worth understanding

**A timeout is not a cancellation.** When a caller's deadline expires on a
dispatched call, the caller gets `DEADLINE_EXCEEDED` and the session stays
busy. `active_calls` is set and cleared *only by the worker thread*, when the
driver call actually enters and actually returns. That is the whole difference
between this and `wait_for(to_thread(...))`, and it is why
`details.session_slot_released` appears on the error.

**Thread mode cannot stop a hung call.** When a call runs past
`hung_call_hard_limit_ms` the session is quarantined, readiness goes false,
and no further call is dispatched on it. Recovering automatically requires
`worker_mode=process`, where killing the child closes the pipe the worker
thread is blocked on. The package does not pretend otherwise; see
[the runbook](docs/incident_runbook.md) §0 and
[ADR-204](docs/adr/README.md#adr-204).

---

## Worker modes

| | `thread` (default) | `process` |
|---|---|---|
| Blocking call off the event loop | yes | yes |
| One active call per session | yes | yes |
| Recovers from a hung call | **no** — quarantines and reports | yes — kill, reap, new epoch |
| Crash in the driver | can take the service down | takes the worker down only |
| Required for the native SDK driver | — | yes (enforced by config) |

---

## Error contract

Every failure is a `ConnectorError` subclass carrying `code`, `category`,
`retryable`, `resync_required`, `safe_action`, and the correlation fields
(`request_id`, `attempt`, `worker_epoch`, `source_generation`). The codes a
consumer must handle are published at `GET /v1/contract` and in
`mt5_connector.errors.ERROR_CODES`.

The distinctions that drive behaviour:

| Code | Retry? | Re-sync? | Means |
|---|---|---|---|
| `TRANSPORT_ERROR`, `UPSTREAM_UNAVAILABLE`, `UPSTREAM_TIMEOUT`, `RATE_LIMITED` | yes, in budget | no | the exchange did not complete |
| `AUTH_FAILED`, `PERMISSION_DENIED`, `CONFIGURATION_ERROR`, `INCOMPATIBLE_SDK` | **no** | no | fix the configuration; the source is disabled |
| `SOURCE_CONTRACT_ERROR` | **no** | no | the payload is unusable; quarantine it |
| `DATA_INTEGRITY_ERROR` | **no** | yes | the data contradicts itself |
| `SNAPSHOT_EXPIRED` | **no** | yes (this scan) | the cursor's snapshot is gone; restart the scan |
| `SOURCE_EPOCH_CHANGED` | **no** | yes (the source) | a different dataset; renegotiate identity |
| `DEADLINE_EXCEEDED`, `OVERLOADED`, `CIRCUIT_OPEN`, `CONNECTOR_CLOSED` | per code | no | the connector refused locally |

`asyncio.CancelledError` is never translated into any of these. It is the
caller's lifecycle signal and propagates untouched.

---

## Testing

| Lane | What it demonstrates | Command |
|---|---|---|
| `tests/unit` | deadlines, retry, FSM, DTO validation, the actor's occupancy model | `make unit` |
| `tests/contract` | the Project 01 `v1` read contract over real TCP | `make contract` |
| `tests/integration` | IT-01 … IT-10: isolation, timeouts, reset, poison data, overload, checkpoint boundary | `make integration` |
| `tests/chaos` | IT-07: a hung call, a killed worker process, a fenced stale result | `make chaos` |
| `tests/property` | generated invariants over money, time, the FSM and scan bookkeeping | `make property` |
| `tests/performance` | B0–B3 and the large-scan oracle | `make performance` |

The suite runs against `tests/fakesim`, a **contract double** of the Project
01 data plane, served over a loopback socket so the synchronous driver does
real blocking socket I/O. That is evidence about the contract; it is not
evidence that the two services have been run together, and the scorecard says
so rather than blurring the two.

### Evidence, not a green tick

```bash
make whatif      # → artifacts/<run>/what_if_results.json + manifest.json
make scorecard   # → artifacts/<run>/scorecard.md
make bench PROFILE=B1 FAULT_PROFILE=none
```

Every one of the design's 216 what-if cases gets one of five statuses, and
three of them are not passes:

- `PASS` — a test cited the case and it passed.
- `FAIL` — a citing test failed.
- `NOT_CONCLUSIVE` — a test ran and could not decide (a host too noisy to
  measure a 25 ms target, for instance).
- `NOT_SUPPORTED` — this build or environment cannot test it, **with the
  reason stated**: an authorised Manager SDK, a network-layer proxy, an
  8-hour soak.
- `NOT_TESTED` — nothing cites it. Reported, never inferred away.

Nothing is filled in by hand, and a fault that never activated cannot produce
a pass.

---

## What this build does not do

- **It does not talk to a real MT5 Manager API.** The native driver refuses to
  construct on an unqualified host and raises on every read, listing what
  would have to be verified first — thread safety, cancellation semantics,
  units, sign conventions, enum translations, whether any stable snapshot or
  incremental sequence exists at all. Returning synthesised data instead would
  be the worst available outcome.
- **It does not own a checkpoint, a database or a watermark.**
- **It does not compute equity, exposure or P&L.** Deriving a figure means
  owning its correctness.
- **It does not claim an atomic view across entities.** Each entity is its own
  snapshot; a broker-wide cut is reconciliation's job.
- **It does not inject its own faults.** Faults belong to the harness driving
  the source's control plane with its own credential.

---

## Layout

```
mt5-connector/
├── src/mt5_connector/        # the package
│   ├── drivers/              # blocking drivers: sync HTTP, fake, native stub
│   └── windows_worker/       # process isolation: IPC, launcher, kill/reap
├── tests/
│   ├── fakesim/              # the Project 01 contract double + fault control
│   └── unit contract integration chaos property performance
├── scripts/                  # what-if runner, scorecard, benchmark
├── scenarios/                # reproducible fault scenarios
├── docs/adr/                 # ADR-201 … ADR-212
└── artifacts/<run_id>/       # evidence: results, manifest, scorecard
```

See [`docs/sdk_mapping.md`](docs/sdk_mapping.md) for the native-SDK mapping
that does not exist yet and what it would take, and
[`docs/incident_runbook.md`](docs/incident_runbook.md) for operations.
