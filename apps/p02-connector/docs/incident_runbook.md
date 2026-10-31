# Incident runbook — Project 02 MT5 Integration Connector

Scope: the read-only connector. It owns no durable state, so **no incident
here can be resolved by editing data** — and none should be. If the data is
wrong, the remedy is a re-scan plus Project 04 reconciliation, not a fix
applied by this service.

Read §0 before the first page of any incident.

---

## 0. The one thing to know first

**Thread mode cannot stop a hung driver call.** There is no safe way to kill a
Python thread, so when a call runs past `hung_call_hard_limit_ms` the
connector does the only honest thing: it marks the session `QUARANTINED`,
refuses further work on it, keeps `/health/live` at 200 and drops
`/health/ready` to 503.

What that means operationally:

| Worker mode | A hung call is… | Automatic recovery? |
|---|---|---|
| `thread` (default) | detected, quarantined, reported | **No.** Restart the process, or run `process` mode. |
| `process` | detected, the child is killed and reaped, a new epoch is started | Yes, verified by `tests/chaos/test_process_worker.py`. |

Restarting *the connector* during a thread-mode hang works because it
restarts the process that owns the thread. Restarting *the session* does not,
and the connector will not claim it did. Anyone who expects automatic recovery
from a hard hang must be running `worker_mode=process`.

With `allow_thread_abandon=true` the supervisor will start a fresh session and
leave the old thread running. That is a **leak, not a recovery**: it is
counted in `connector_abandoned_workers`, and readiness stays false while any
abandoned worker is alive.

---

## 1. Triage in one minute

```bash
curl -s localhost:8080/health/live     # process. 200 unless draining.
curl -s localhost:8080/health/ready    # session. 503 carries `reasons`.
curl -s localhost:8080/metrics | grep -E 'connector_(readiness|sdk_active_calls|queue_depth|circuit_state|inflight_oldest_age|abandoned_workers)'
```

`/health/ready` is the diagnosis. Its `reasons` array maps directly to a
section below.

| `reasons` entry | Meaning | Section |
|---|---|---|
| `session_state_disabled` | A credential or configuration failure. No upstream traffic is being sent. | §2 |
| `session_state_quarantined`, `session_state_suspect` | A driver call is stuck. | §3 |
| `session_state_resync_required` | The source identity changed. | §4 |
| `session_state_backoff`, `no_session` | Cannot reach or handshake the source. | §5 |
| `circuit_open` | Repeated transport failures. | §5 |
| `abandoned_worker_present` | A thread was abandoned and is still running. | §3 |
| `admission_closed`, `draining` | Shutting down. | §7 |
| `capabilities_unknown` | Handshake has not completed. | §5 |

---

## 2. `AUTH_FAILED` / `PERMISSION_DENIED` — source disabled

**Symptom.** Readiness false with `session_state_disabled`;
`health.disabled_error_code` is `AUTH_FAILED` or `PERMISSION_DENIED`.

**What the connector is doing.** Nothing upstream. The error is stored and
replayed to every caller without a request, which is deliberate: 100 callers
against a bad credential must not become 300 auth attempts.

**Do.**
1. Check the credential in the secret store, not in logs — the token is never
   logged, by design.
2. Verify the source role is read-only and can reach the requested entity.
3. After fixing it, the source needs an explicit reconfiguration. There is no
   retry path out of `DISABLED`; restart the process, or call
   `supervisor.reconfigured()` if you are driving it as a library.

**Do not** raise `max_attempts_per_read` or add a retry loop. The state exists
to prevent exactly that.

**Recovery check.** `/health/ready` 200, `health.worker_epoch` incremented, and
no credential string anywhere in the logs.

---

## 3. Stuck driver call — `SUSPECT` → `QUARANTINED`

**Symptom.** `connector_inflight_oldest_age_seconds` climbing past
`hung_call_hard_limit_ms`; `connector_sdk_active_calls` stuck at 1; callers
receive `CIRCUIT_OPEN` with `details.reason = "session_quarantined"`.

**What the connector is doing.** Refusing to dispatch a second call on that
session, and saying so. The slot stays occupied until the call actually
returns — a caller's timeout does **not** free it.

**Do.**
1. Read `details.can_force_recover` on the error, or
   `health.actor.active_age_s`.
2. **`worker_mode=process`:** the watchdog has already killed and reaped the
   child and started a new epoch. Confirm a new `pid` and
   `worker_epoch` in the logs, then confirm readiness returned.
3. **`worker_mode=thread`:** restart the connector process. Nothing else
   releases the thread. If the source is suspected, capture
   `connector_sdk_call_duration_seconds` first — a hang that reproduces is a
   source bug worth reporting.
4. If `connector_abandoned_workers > 0`, the process is holding threads it
   cannot reclaim. Restart it; do not wait for the count to fall unless the
   call is expected to return.

**Recovery check.** `connector_sdk_active_calls` back to 0,
`connector_abandoned_workers` 0, readiness 200. Any completion from the old
epoch must appear in `connector_stale_completions_total`, never as a served
page.

---

## 4. `RESYNC_REQUIRED` / `SOURCE_EPOCH_CHANGED` — the source changed

**Symptom.** Readiness false with `session_state_resync_required`;
`connector_snapshot_restart_total` incremented; scans returning
`outcome = "resync_required"` and `checkpoint_candidate = null`.

**What the connector is doing.** Refusing to serve reads until a consumer
acknowledges, because the dataset behind the same configuration may be a
different dataset.

**Do.**
1. Compare `health.run_id` / `health.generation` against the source's.
2. Tell the consumer (Project 03) to abandon its in-flight scan. **Do not let
   it commit a checkpoint** — a scan that ended this way has no candidate to
   commit, by type.
3. Acknowledge the resync (`acknowledge_resync()`), which forces a fresh
   handshake and a new epoch.
4. Run a full initial scan and hand the result to Project 04 reconciliation.

**Do not** reuse a cursor from before the change. A cursor is bound to a run
and a generation, and the source answers 410 for one that is not.

**Recovery check.** A complete scan whose `checkpoint_candidate.run_id` and
`.generation` match the live source, and whose id set matches the oracle.

---

## 5. Source unreachable — `BACKOFF`, `CIRCUIT_OPEN`

**Symptom.** `connector_circuit_state` 2 (open) or 1 (half-open);
`connector_reconnect_total` climbing; callers receive `UPSTREAM_UNAVAILABLE`
or `CIRCUIT_OPEN`.

**What the connector is doing.** Bounded retries with full-jitter backoff, one
handshake at a time regardless of caller count, and exactly one probe while
half-open.

**Do.**
1. Confirm the source is actually down — `/health/live` on the source itself.
2. Check `connector_reconnect_total` by `reason` for the error class.
3. Leave the breaker alone. The cooldown is what stops a recovering source
   from being knocked over by the callers that were waiting.

**Recovery check.** `connector_circuit_state` 0, readiness 200, and a
successful page read.

---

## 6. Queue saturation — `OVERLOADED`

**Symptom.** `connector_queue_depth` at `max_queue_pending`;
`OVERLOADED` errors; `connector_requests_total{outcome="error"}` rising with
`error_class="OVERLOADED"`.

**What the connector is doing.** Refusing immediately rather than queueing.
This is designed behaviour, not degradation: with one serial worker, arrival
rate above service rate must be refused somewhere, and refusing at admission
costs nothing.

**Do.**
1. Look at `connector_sdk_call_duration_seconds` first. Saturation is usually
   a *slow source*, not too many callers.
2. Reduce caller concurrency, or page size, before raising
   `max_queue_pending` — a bigger queue converts fast failures into slow
   ones.
3. If one caller is responsible, set `max_pending_per_client` so it cannot
   starve the sync service.

**Do not** raise the queue to "absorb" a permanently slow source. Memory is
the only thing that absorbs.

---

## 7. Shutdown

`SIGTERM` closes admission immediately, then drains for
`drain_timeout_ms`.

* `close()` returning `true` — everything was reclaimed.
* `close()` returning `false` — a call was still running. The process is
  holding a thread it could not stop, and the log says so. Let the orchestrator
  kill the process; the consumer re-scans from its durable watermark.

A container restart mid-scan is safe by construction: the connector holds no
checkpoint, so the consumer simply re-reads from the watermark it last
committed and the sink's unique key absorbs the overlap.

---

## 8. Invalid financial data — `SOURCE_CONTRACT_ERROR`, `DATA_INTEGRITY_ERROR`

**Symptom.** `connector_validation_error_total` by `field`; a page refused
with a field name and a reason but no value.

**What the connector is doing.** Refusing the whole page and **not retrying**.
A `NaN` commission will be `NaN` on the second read; retrying converts one
source bug into a retry storm while still producing nothing usable.

**Do.**
1. Read `details.field` and `details.reason`. The value is deliberately
   absent — it is financial data and this error is logged.
2. Reproduce against the source directly with an operator credential to
   capture the payload in a controlled place.
3. Treat it as a source defect. Do not add a coercion, a rounding rule or a
   default to make it pass.

**Recovery check.** A golden-fixture scan passing after the source is fixed,
with `connector_validation_error_total` flat.

---

## 9. What never appears in an incident here

* A connector-side data fix. There is no write path.
* A checkpoint moved by this service. It holds none (ADR-207).
* A credential in a log line, a metric label or an error envelope.
* A claim that a hung thread was recovered. See §0.
