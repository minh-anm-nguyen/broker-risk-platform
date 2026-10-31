# Architecture Decision Records — Project 02

Twelve decisions, each with what it costs and what would make us revisit it.
They are cited by id from the code, so a reader who finds `ADR-202` in a
docstring can come here and learn why that line exists.

| ADR | Decision |
|---|---|
| [201](#adr-201) | An async port over a blocking driver |
| [202](#adr-202) | One session, one worker, one active call |
| [203](#adr-203) | Bounded admission that fails fast |
| [204](#adr-204) | Thread for the MVP, process for hard hangs |
| [205](#adr-205) | Retry by error class, never on invalid data |
| [206](#adr-206) | Worker epoch and source generation as fences |
| [207](#adr-207) | The connector owns no durable checkpoint |
| [208](#adr-208) | The simulator snapshot contract is explicit |
| [209](#adr-209) | Read-only; no trading surface |
| [210](#adr-210) | Strict `Decimal` and explicit timezones |
| [211](#adr-211) | An unverified capability is refused, not assumed |
| [212](#adr-212) | Test control plane separate from the data plane |

---

## ADR-201

**An async port over a blocking driver.** `MT5Client` is async and mentions
nothing about HTTP, threads or a vendor. The driver beneath it is ordinary
synchronous code.

**Why.** A consumer must not change when the source behind it does. It also
makes the hard case the default case: the driver is written as if it could
block for minutes, because the deployment it stands in for can.

**Cost.** Two layers where one would do, and a hand-off between them that has
to be exactly right. An async HTTP client would be simpler and would prove
nothing about isolating a call that cannot yield.

**Revisit when.** A real driver contract is proved and its concurrency
properties are documented.

---

## ADR-202

**One session, one worker, one active call.** `session_workers` is validated
to 1 and the configuration is refused otherwise. A single worker thread
consumes the mailbox, so serialisation is structural rather than checked.

**Why.** No vendor evidence says an MT5 Manager session is safe to drive
concurrently. Assuming it is turns a correctness question into a tuning knob,
and the failure mode is corrupted financial data rather than an error.

**Cost.** Throughput is capped at roughly `1 / mean_call_seconds` pages per
second per source. With 0.2 s calls that is about 5 pages/s — a real ceiling,
not a theoretical one.

**Revisit when.** The vendor documents thread-safe concurrent calls *and* a
concurrency test passes against a real session.

---

## ADR-203

**Bounded admission that fails fast.** At capacity a caller is refused
immediately with `OVERLOADED` rather than parked on a future.

**Why.** Little's Law: once arrival rate exceeds service rate, a queue must
grow to its cap. Letting callers wait spends memory to deliver failures
slowly. 100 callers against one serial worker would mean 100 retained
coroutines, 100 envelopes and 100 timeouts that all fire later.

**Cost.** Callers see refusals under load and must handle them. A client that
treats `OVERLOADED` as fatal will look fragile.

**Revisit when.** Benchmarks justify a different cap — not a different policy.

---

## ADR-204

**Thread for the MVP, process for hard hangs.** The default worker is a
dedicated thread. A call that runs past `hung_call_hard_limit_ms` quarantines
the session, and that is where thread mode stops.

**Why.** There is no safe way to stop a running Python thread. A design that
claimed automatic recovery in thread mode would be claiming something the
language does not offer. Process isolation makes recovery real and specific:
the worker thread is blocked reading a pipe, and killing the child closes it,
so the read returns and the thread unblocks.

**Cost.** Two modes to test and reason about. Process mode pays IPC
serialisation on every page and needs a spawnable interpreter.
`allow_thread_abandon` exists for tests and leaks a thread when used — which
readiness reports rather than hides.

**Revisit when.** Shipping against a real native SDK, where process mode is
mandatory and enforced by configuration.

---

## ADR-205

**Retry by error class, never on invalid data.** Only transport-family errors
are retried, inside the caller's absolute deadline, with full-jitter backoff.
A malformed payload, a duplicate id or a bad credential is read exactly once.

**Why.** A `NaN` commission will be `NaN` on the second read. Retrying it
turns one source bug into N requests during an incident and still produces
nothing usable. A 401 retried by 100 callers becomes 300 auth attempts.

**Cost.** A genuinely transient data error — if such a thing exists — needs a
new scan rather than a retry.

**Revisit when.** Reconciliation policy defines a class of data error that is
safe to re-read.

---

## ADR-206

**Worker epoch and source generation as fences.** Every session incarnation
gets an epoch; every page carries the source's run and generation. A
completion is checked against both before anything looks at its payload.

**Why.** The two answer different questions. The epoch catches "this result
belongs to a session we have replaced". The generation catches "this result
belongs to a dataset the source has replaced". Conflating them makes a stale
result from the same generation look fresh.

**Cost.** Four checks on a path that is already hot, and an epoch counter that
must only ever be minted in one place.

**Revisit when.** Never, without an equivalent mechanism in place first.

---

## ADR-207

**The connector owns no durable checkpoint.** It reports a
`CheckpointCandidate` and has no method that commits one.

**Why.** Having fetched a page is not having persisted it. A connector that
advanced a watermark on a successful read would skip data on any crash
between the read and the sink's commit — and would do it silently. Keeping the
watermark where the transaction is makes "the connector marked data as synced
before it was persisted" an error this interface does not permit.

**Cost.** The consumer must own the commit boundary and be idempotent. A
re-read after a crash re-delivers the overlap, which the sink's unique key
absorbs.

**Revisit when.** The sync service's transaction boundary changes — and then
the candidate's shape changes, not its ownership.

---

## ADR-208

**The simulator snapshot contract is explicit.** Snapshot continuity, cursor
opacity and the exclusive `after_seq` filter are negotiated from
`GET /v1/meta` and relied on only when declared.

**Why.** It makes a lossless multi-page scan testable in a lab against a real
oracle. Hard-coding the same assumptions would make them invisible, and they
would then be applied to a source that does not offer them.

**Cost.** A mapping layer, and a capability set that must be kept honest.

**Revisit when.** A real source is characterised. Its capabilities will
differ, and the negotiated flags are where that shows up.

---

## ADR-209

**Read-only; no trading surface.** The port has four methods, the driver
protocol has six, and none of them writes. The upstream path table contains
four `GET` routes and nothing else.

**Why.** Least privilege, and a bound on the worst case. A read-only connector
with a bug loses data; a connector with an order path and a bug loses money.

**Cost.** Any future order entry is a separate project with its own
authorisation — which is the intent.

**Revisit when.** Never in this project.

---

## ADR-210

**Strict `Decimal` and explicit timezones.** Money, prices and volumes cross
the boundary as decimal strings; a JSON float is refused, not coerced. A
timestamp without an offset is refused, not assumed to be UTC.

**Why.** `1.10000` through an IEEE 754 double is not `1.10000`, and
`Decimal(1.1)` preserves the error rather than removing it. Rounding would
import the source's representation error and hide it. "No offset" is an
unknown offset; guessing one shifts an execution time by hours.

**Cost.** Pages are refused that a lenient parser would accept. That is the
feature.

**Revisit when.** Multi-currency domain work needs a conversion policy — and
then conversion is explicit and owned elsewhere.

---

## ADR-211

**An unverified capability is refused, not assumed.** Every capability flag
defaults to `False`. An absent field means unknown, and unknown is treated as
unsupported.

**Why.** "Nothing said otherwise" and "the source supports it" must not
collapse into the same value. A connector that assumes a snapshot cursor
exists will report a complete, gap-free scan assembled from pages that were
never from one snapshot.

**Cost.** A source that supports something without declaring it is used more
conservatively than it needs to be.

**Revisit when.** A capability is verified by a conformance test, at which
point it is declared rather than inferred.

---

## ADR-212

**Test control plane separate from the data plane.** Faults are injected by
the harness through the *source's* control plane with its own credential. The
connector holds a data-plane token and has no code path to a control route.

**Why.** It keeps chaos away from anything real, and it means a compromised or
buggy connector cannot reset a run or arm a fault. The separation is
structural: `Settings.url_for` composes only from a fixed table of `GET`
paths, so there is nothing to misuse.

**Cost.** The test harness is a separate component with its own credential
handling.

**Revisit when.** Never; the production boundary is the point.
