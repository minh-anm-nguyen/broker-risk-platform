# Project 06 — Risk Reporting REST API

Written for: engineers working on this repository and reviewers assessing it.

A read-only HTTP surface over the Project 05 risk engine. It parses, authorizes,
budgets, captures a data cut, calls the engine, verifies what came back, audits
the access and serializes the result. It contains no financial formula of its
own.

Implements `docs/Project_06_Risk_Reporting_REST_API_System_Design.md`.

## What works today, and what does not

**Implemented and tested**

- All six P0 endpoints plus health and internal metrics.
- Decimal-string money end to end, with a guard that refuses to serialize a
  float and a test that inspects raw JSON tokens rather than parsed values.
- Signed-token authentication (HS256, local issuer), role and object-level
  authorization, masked 404s, aggregate permission, no trust in any
  client-supplied tenant header.
- Cut capture from a single consistent read, plus a final validity check and
  one bounded recapture before release.
- Strict composite by default; `DEGRADED` only on explicit opt-in, and never
  with a composite total.
- An output contract guard that turns an unverifiable engine result into
  `502 INVALID_RISK_RESULT` with no money in the body.
- Signed, scan-fenced, scope-bound keyset cursors.
- Admission control: per-tenant token buckets (`429`) separate from capacity
  ceilings (`503`), with bounded queueing and deadline sub-budgets.
- CPU isolation in a bounded worker pool, a late-result fence, and an
  event-loop heartbeat.
- Durable append-only audit with a fail-closed gate; in-memory and fsynced
  JSONL backends.
- Metrics with an enforced label policy, and logs with enforced redaction.

**Not implemented, and reported as such rather than as passing**

- The PostgreSQL audit backend. The DDL is committed in
  `migrations/versions/0001_risk_api_audit.sql`; wiring it needs a driver this
  build does not depend on.
- Real Project 02/03/04 services. Their read-side contract is defined and
  version-checked at startup, but the data comes from an in-memory reference
  implementation (`repositories/read_views.py`). The compose file says so too.
- Every P1/P2 capability: OIDC/JWKS, the response cache, async report jobs,
  downloadable reports, multi-currency conversion, multi-source policy.
  `tests/test_scenario_manifest.py` lists the what-if domains these gate out
  and records them as `NOT_APPLICABLE_BY_GATE`.

**No performance claim.** The load tests assert saturation behaviour and
correctness under pressure on a loopback in-process harness. They publish no
SLO. The 120 ms figure in the design provenance note is a CV reference, not a
result from this code.

## Layout

```
apps/risk-reporting-api/
├── src/risk_api/
│   ├── api/            routers, dependencies, error rendering
│   ├── contracts/      request/response schemas, money, problem taxonomy
│   ├── authn/          token verification, local test issuer
│   ├── authz/          scope policy, typed principal context
│   ├── app/            cut capture, engine port, quality gate, pagination
│   ├── repositories/   read-only projection views, audit store
│   ├── admission/      quotas, budgets, admission control
│   ├── execution/      CPU worker pool, late-result fence
│   ├── observability/  metrics, redacting logs
│   └── faults.py       test-only injection registry
├── migrations/versions/
├── fixtures/           golden expected values, derived by hand
├── scripts/
└── tests/{unit,contracts,integration,adversarial,chaos,performance,property}

packages/risk-engine/    Project 05 reference engine: pure Decimal, no web imports
```

## Running it

```bash
make install            # editable installs of the engine and this app
make serve              # http://127.0.0.1:8060
make token              # a signed development bearer header
```

```bash
TOKEN=$(python scripts/mint_dev_token.py)
curl -s "http://127.0.0.1:8060/api/v1/accounts/00042/risk?source_id=sim-broker-a" \
  -H "Authorization: Bearer $TOKEN" | python -m json.tool
```

The development key material only works when `RISK_API_ENVIRONMENT` is `local`
or `test`. A staging or production instance that does not supply its own keys
refuses to start, and fault injection cannot be enabled there at all. See
`.env.example`.

## Endpoints

| Method | Path | Notes |
|---|---|---|
| GET | `/api/v1/accounts/{account_id}/risk` | Exposure, realized and unrealized for one authorized account |
| GET | `/api/v1/accounts/{account_id}/pnl` | Realized components over an explicit half-open UTC window |
| GET | `/api/v1/positions` | Current listing, keyset paged and fenced to one published scan |
| GET | `/api/v1/exposure` | Per-symbol units for the authorized scope |
| GET | `/api/v1/reports/daily` | Realized deal activity for one UTC calendar day |
| POST | `/api/v1/risk-calculations` | Typed read-only compute request |
| GET | `/health/live` | Event loop only; never touches the projection store |
| GET | `/health/ready` | Bounded dependency checks, coarse reasons |
| GET | `/internal/metrics` | Ops role only; internal listener in a deployment |

Errors are `application/problem+json` (RFC 9457) with a stable `code`. The whole
mapping is data in `contracts/problem_details.py` and asserted in
`tests/unit/test_problem_taxonomy.py`.

## Things worth knowing before changing this code

**A 200 is not the correctness oracle.** `app/report_quality.py` refuses to
serialize a result missing its cut, currency, digests or component arithmetic.
Deleting one of those checks should break a P0 test; if it does not, the suite
has a gap.

**Partial is opt-in, always.** `allow_partial=true` is the only way to receive
a `DEGRADED` report, and such a report never carries `composite_total`.
Flipping `strict_composite_default` is a financial policy change, not a
convenience setting.

**Absence is not zero.** A missing fee is `fee_coverage: NOT_AVAILABLE` and the
field is named `net_excluding_fee_usd`, never `net_usd`. An incomplete deal
watermark is `409 DATA_NOT_READY`, never `0.00`.

**Quote proof and cut compatibility are different questions.** A quote snapshot
can be trustworthy enough to value a position (`quote_cut_proven`) while still
not sitting on the same logical cut as the deals. The second case yields
`consistency: INDEPENDENT`, reason `INCOMPATIBLE_CUT`, and no composite total.
Coupling them hides a real class of wrong money.

**The realized window is anchored to the cut, not to the clock.** A risk
summary derives its window from the pinned publication instant, so the same cut
always produces the same digests. Using wall-clock now there breaks
reproducibility, which an earlier revision of this code did.

**A timeout does not stop CPU work.** `execution/cpu_worker.py` abandons the
wait, keeps accounting for the thread, and fences the late result so it cannot
be returned to a different request.

**Project 06 never writes upstream.** `tests/integration/test_authz_matrix_and_isolation.py`
checksums every projection table before and after each endpoint, and
`scripts/verify_invariants.py` fails on a mutating statement outside the audit
store.

## Tests

```bash
make smoke        # unit + contract, the per-commit gate
make test         # everything, including the slow markers
make invariants   # static boundary checks
make openapi-check
```

| Layer | What it holds |
|---|---|
| `tests/unit` | Decimal policy, problem taxonomy, token verification, scope policy, cursor codec, quotas and deadlines, CPU isolation, metrics and log policy, the output guard, request parsing, settings guards, and the reference engine against hand-derived values |
| `tests/contracts` | Routing, negotiation, authentication over HTTP, every endpoint's semantics and limits at `L`/`L+1`, header policy, health, metrics access, OpenAPI |
| `tests/integration` | Strict vs partial, engine contract violations, cut races with deterministic barriers, the audit gate including a lost commit acknowledgement, the authorization matrix, and session/pool isolation |
| `tests/adversarial` | Hostile paths, bodies, headers and cursors; probing; quota pressure |
| `tests/chaos` | Kill points around the audit commit against a real fsynced file, plus compound campaigns |
| `tests/performance` | Saturation shape, event-loop responsiveness, paging cost, resource stability. Prints measurements with `-s`; claims no SLO |
| `tests/property` | Hypothesis properties (idempotent reads, scope monotonicity, page partition, exposure arithmetic) and a state machine over the release decision |

Races use deterministic barriers from `faults.py`, not sleeps: a test arms a
barrier at a named pipeline point, the request blocks there, the test mutates
upstream state, then releases it. `tests/test_scenario_manifest.py` writes
`artifacts/scenario_manifest.json`, mapping design scenarios to tests with the
evidence each one recorded, and stating plainly which domains are not covered.

On a slow machine the full suite takes several minutes; pytest startup here is
dominated by process and filesystem overhead rather than by the tests.

## Boundaries

This service does not connect to the MT5 SDK (P02), write synchronized data
(P03), perform repair (P04), define financial formulas (P05), publish events
(P07) or store analytics (P08). It does not promise that one PostgreSQL
snapshot equals a globally aligned broker and quote cut: every payload carries
the cut it is actually valid for, and refuses to pretend otherwise.
