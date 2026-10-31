# Operator runbooks (SYSTEM_DESIGN section 15)

Written for: an on-call engineer who has a reason code and needs the next
action, not an explanation of the architecture.

Every runbook starts from a **reason code** the service emitted, because every
non-`MATCHED` outcome carries one. If you have a symptom but no reason code:

```bash
reconctl run show --run-id <RUN-UUID>        # status, reason_code, receipt chain
reconctl run evidence --run-id <RUN-UUID>   # findings and their confidence
```

## Three rules that override anything below

1. **Never hand-edit `deals`, `accounts_current`, `positions_current` or
   `sync_checkpoints` to "fix" a finding.** The service cannot do it — its
   database role is denied those writes — and neither should you. If a value
   is wrong, the fix is a Project 03 re-derivation from the source, or a
   documented correction protocol under a new epoch.
2. **`INCONCLUSIVE` is not an incident by itself.** It means the service
   refused to guess. Find out *why* before doing anything.
3. **`COMMITTED` is not `VERIFIED`.** Until `reconctl run verify` returns
   `verified: true`, the repair is not done, whatever Project 03's receipt
   says.

---

## RB04-01 — Source disconnected during an audit

**Reason codes:** `SOURCE_TRANSPORT_FAILED`, `SNAPSHOT_EXPIRED`,
`SOURCE_PAGE_MISSING`, `SOURCE_INCOMPLETE`

**What happened.** The source stopped answering, or the snapshot the scan was
pinned to expired, before a terminal page arrived. The partial manifest is
retained as evidence and is *not* eligible for comparison.

1. `reconctl run show --run-id <RUN>` → confirm `source_complete: false` and
   `receipt_chain.valid: false`. If `source_complete` were `true` here, that
   is a far more serious bug than the disconnect — escalate immediately.
2. Is the source reachable now? `reconctl source check`
   - **No** → wait. No repair may be dispatched from this run. Project 03
     keeps its last durable watermark and published scans; serving slightly
     stale data during a disconnect is correct behaviour.
   - **Yes** → continue.
3. Did the source identity change? Compare `run_id`/`generation` from
   `source check` against the run's pinned values in `run show`.
   - **Changed** → this is RB04-02, not this runbook.
   - **Same** → start a **new** audit. Do not try to resume the old one past
     its deadline; a fresh scan on a fresh snapshot is cheaper than reasoning
     about a half-expired cursor.
4. If expiry recurs on every attempt → RB04-05.

**Do not:** mark the run matched, reuse the partial manifest, or infer that
target rows absent from a partial scan are extra.

---

## RB04-02 — Source generation reset

**Reason codes:** `EPOCH_MISMATCH`, `SOURCE_IDENTITY_CHANGED`

**What happened.** The source regenerated its dataset. Its `(run_id,
generation)` no longer matches the epoch Project 03 is synced against, so
nothing captured under the old epoch can be compared with what the source says
now. The same deal id existing in both epochs is **not** a collision.

1. Confirm the change: `reconctl source check` versus `reconctl run show`.
2. Stop any in-flight repair for the old epoch. Approved plans for it will be
   refused as `STALE`/`EPOCH_MISMATCH` at dispatch, but do not rely on that —
   cancel them so nobody re-approves one.
3. Quarantine the old epoch for forensics. It stays readable; it just stops
   being a comparison target.
4. Have Project 03 bootstrap the **new** epoch and complete a full sync:
   terminal Deal watermark, then full Account and Position publications.
5. `reconctl source bootstrap`, then audit the new epoch only.
6. Keep the old epoch's evidence until the incident is closed. It is the only
   record of what the previous dataset looked like.

**Do not:** copy deal ids, balances or watermarks across the epoch boundary.
No documented migration contract proves identity stability across a
regeneration.

---

## RB04-03 — Deal conflict in amount, commission or swap

**Finding:** `IMMUTABLE_DEAL_CONFLICT` (severity `CRITICAL`)

**What happened.** The same `(epoch_id, deal_id)` has different canonical
content at the source and in the target, at a cut where the comparison was
valid. Deal history is immutable within an epoch, so one side is wrong and
**the service cannot tell which.**

This is the one finding with no automatic path forward. Any plan built from it
contains `MANUAL_INVESTIGATION` and nothing else. That is correct: a heuristic
that picked a side would be the service inventing financial data.

1. `reconctl run evidence --run-id <RUN>` → record `item_key`, `source_hash`,
   `target_hash`, `evidence_hash`. Attach these to the incident.
2. Establish which side changed, using evidence outside both systems:
   - the source's own event log or golden seed for the epoch;
   - Project 03's page receipts for the scan that wrote the row;
   - `reconctl audit verify` for any unexpected mutation of the audit trail.
3. If **the target is wrong**: this needs a correction protocol, not a repair.
   Options are a documented backfill under review (P1) or a re-bootstrap into
   a new epoch. Both need sign-off; neither is `UPDATE deals`.
4. If **the source is wrong**: the audit has done its job. Raise it with the
   source owner; do not change the target to match a bad source.
5. If the conflict count is large, treat it as one systemic incident rather
   than N conflicts, and check whether a canonicalisation or schema version
   changed on either side. `INCOMPARABLE_SCHEMA` would normally have caught
   that — if it did not, that is a second bug.

**Do not:** `ON CONFLICT DO UPDATE` on money, delete the target row, or
"re-sync to fix it". A full re-sync does not repair immutable history and may
hide the conflict.

---

## RB04-04 — Project 03 commit ACK lost, plan status `UNKNOWN`

**Reason code:** `REPAIR_UNKNOWN`

**What happened.** Project 03 may or may not have committed the repair; the
response was lost. **Both obvious reactions are wrong:** retrying risks a
double execution, and treating it as failed loses a committed change.

1. Ask Project 03 about the **same** intent id — never a new one:
   ```bash
   reconctl intent status --intent-id <INTENT-ID>
   ```
   The intent id is deterministic (`SHA256(plan_id + order + type +
   parameters_hash)`), so it is always recoverable from the plan.
2. Read the result:
   - **`found: false`** → Project 03 never saw it. Re-dispatching the *same*
     intent id is safe.
   - **`status: COMMITTED`** → the work is done. Go to verification; dispatch
     nothing.
   - **`ACCEPTED` / `RUNNING`** → still in flight. Wait and re-query. Do not
     dispatch.
   - **`FAILED` / `REJECTED`** → nothing was applied. Read `reason_code`; if
     it is a staleness rejection, build a new plan from fresh evidence rather
     than re-approving the old one.
3. Check `execution_count`: `0` for a rejection, `1` for a completed intent.
   **Above 1 is a serious idempotency defect** — escalate instead of
   continuing.
4. Once `COMMITTED`: `reconctl run verify --plan-id <PLAN>`.

---

## RB04-05 — Repeated cursor expiry on a large scan

**Reason code:** `SNAPSHOT_EXPIRED`, recurring

**What happened.** The scan cannot finish inside the source's snapshot TTL.
This is a throughput budget problem, not a data problem: TTL divided by
per-page latency caps how many pages one audit can read.

1. Measure, do not guess. From `run show` take the receipt count reached
   before expiry; from the metrics take page latency. Pages-to-TTL is the
   budget.
2. Reduce time per page first:
   - run the audit off-peak, away from Project 03's sync load;
   - make sure the audit is not competing with itself
     (`RECON_MAX_CONCURRENT_RUNS`).
3. Reduce pages needed: raise `RECON_SOURCE_MAX_PAGE_SIZE` toward the source's
   declared maximum (500). Fewer, larger pages is the cheapest win.
4. If it still cannot fit, **do not lower the completeness bar.** The correct
   answers are the P1 segmented-manifest design (incremental audit over
   bounded ranges) or a negotiated longer TTL from the source owner.
5. Record the measured ceiling. A full strict audit that cannot complete is a
   capability limit the consumers of this data need to know about.

**Do not:** resume an expired cursor, stitch two snapshots together, or accept
a partial manifest because it is "nearly all of it".

---

## RB04-06 — Database disk full or failover

**Reason codes:** `TARGET_DB_ERROR`, `TARGET_CAPTURE_CONFLICT`

**What happened.** Storage refused the capture, or the target moved underneath
a pinned read snapshot. Either way no manifest was committed.

1. Confirm nothing partial survived: `run show` must report
   `target_complete: false`. Captures are single transactions, so a partial
   manifest should be impossible — if you see one, that is the incident.
2. For a disk or IO failure, restore capacity first. Audits are the lowest
   priority consumer here; Project 03's ability to keep syncing matters more.
3. Reclaim audit space if needed: `reconctl gc`. It releases only manifests
   past retention for runs with **no** non-terminal plan, so it cannot delete
   the evidence behind an in-flight repair.
4. After a restore or failover, **before auditing anything**, confirm the
   restored database is not behind the source. A restored target whose
   watermark is ahead of its actual rows — or behind reality — will produce
   confident nonsense. Verify Project 03's checkpoints against its page
   receipts first.
5. `TARGET_CAPTURE_CONFLICT` alone, with healthy storage, is usually just a
   busy publisher. Retry the audit; it is designed to lose that race safely.

---

## RB04-07 — Post-repair verification failed

**Reason codes:** `VERIFICATION_FAILED`, or the verification run's own code

**What happened.** Project 03 reported `COMMITTED`, but the independent
re-audit did not come back `MATCHED`, so the plan is `FAILED`. **Nothing is
rolled back** — undoing a committed sync blindly is more dangerous than the
mismatch.

1. Look at the verification run, not the original. Its id is in the verify
   output, and its `verifies_plan_id` links it to the plan:
   `reconctl run show --run-id <VERIFICATION-RUN>`.
2. Why did it not match?
   - **`DIFFERENT_CUT`** → the source moved between the repair and the check.
     Not a failed repair. Re-audit at an aligned cut; ask Project 03 to
     republish if needed.
   - **`TARGET_NOT_CAUGHT_UP`** → the watermark has not reached the source
     snapshot yet. Wait, then re-verify.
   - **Same findings as before** → the repair genuinely did not work. Do not
     re-dispatch the same plan; build a new one from the new evidence, and
     check whether Project 03's re-sync actually read the source (its scan
     record and terminal receipt).
   - **Different findings than before** → the repair changed something
     unexpected. Treat as an incident and capture both runs' evidence hashes
     before anything else.
3. If verification keeps failing on the same entity, stop repairing and
   investigate Project 03's publication path. Repeated failed repairs against
   a broken sync is how a small inconsistency becomes a large one.

**Do not:** mark the plan verified manually, re-dispatch on the stale
approval, or revert Project 03's committed state without a documented plan.

---

## Appendix — reason code to runbook

| Reason code | Runbook |
|---|---|
| `SOURCE_TRANSPORT_FAILED`, `SOURCE_PAYLOAD_INVALID` | RB04-01 |
| `SNAPSHOT_EXPIRED` once / recurring | RB04-01 / RB04-05 |
| `SOURCE_PAGE_MISSING`, `SOURCE_INCOMPLETE`, `SOURCE_CURSOR_CYCLE` | RB04-01 |
| `SOURCE_DUPLICATE_KEY`, `SOURCE_ORDER_VIOLATED` | RB04-01, then raise with the source owner |
| `EPOCH_MISMATCH`, `SOURCE_IDENTITY_CHANGED` | RB04-02 |
| `IMMUTABLE_DEAL_CONFLICT`, `EXTRA_TARGET_DEAL` | RB04-03 |
| `REPAIR_UNKNOWN` | RB04-04 |
| `TARGET_DB_ERROR`, `TARGET_CAPTURE_CONFLICT` | RB04-06 |
| `VERIFICATION_FAILED` | RB04-07 |
| `DIFFERENT_CUT` | re-align: republish or re-audit. Never a corruption report |
| `TARGET_NOT_CAUGHT_UP` | wait for Project 03. Never a missing-data report |
| `MASS_DRIFT_ESCALATION`, `LARGE_SOURCE_COUNT_DROP` | human review before any action |
| `EVIDENCE_STALE`, `PLAN_STALE`, `LEASE_LOST` | discard the plan; build a new one from fresh evidence |
| `APPROVAL_*`, `PLAN_DIGEST_CHANGED`, `FORBIDDEN_ACTION` | the gate worked. Fix the request, not the gate |
| `INCOMPARABLE_SCHEMA` | a schema or canonicalisation version differs; resolve the mapping before auditing |
| `UNSUPPORTED_CAPABILITY` | the source cannot support a strict compare; diagnostics only |
| `P03_INVARIANT_BROKEN` | stop Project 03's sync and raise an incident |
| `ADMISSION_REJECTED`, `SCAN_QUOTA_EXCEEDED`, `RUN_DEADLINE_EXCEEDED` | capacity, not correctness. See RB04-05 |
