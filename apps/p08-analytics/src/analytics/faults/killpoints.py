"""The 28 named injection sites of section 12, plus the plan that drives them.

Why named constants instead of monkeypatching: a kill point is part of the
design's vocabulary. ``KP08-05`` means "after the ClickHouse ACK, before the
Kafka offset commit", and that sentence appears in the design, the test name
and the evidence bundle. Patching an internal function instead would couple
every chaos test to the current call graph, and renaming a helper would
quietly stop injecting the fault while the test kept passing.

The plan is consulted at the site, so the production code paths carry the
check. That is a real cost - a handful of ``plan.fire(...)`` calls in
otherwise clean logic - bought deliberately, because the alternative is chaos
tests that cannot say where they struck.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Callable, Iterable, Mapping


class CrashError(BaseException):
    """Simulated process death.

    Inherits :class:`BaseException`, not :class:`Exception`, so that the
    ``except Exception`` handlers which legitimately exist throughout the
    pipeline cannot swallow a crash. A kill point that got caught and turned
    into a tidy retry would test the opposite of what it claims.
    """

    def __init__(self, site: str) -> None:
        self.site = site
        super().__init__(f"simulated crash at {site}")


class KillPoint(StrEnum):
    """Section 12's inventory. The value is the site name used in evidence."""

    # Ingest path
    AFTER_POLL_BEFORE_VALIDATE = "KP08-01"
    AFTER_VALIDATE_BEFORE_INSERT = "KP08-02"
    INSERT_SENT_NOT_ACKED = "KP08-03"
    INSERT_PERSISTED_RESPONSE_DROPPED = "KP08-04"
    AFTER_INSERT_ACK_BEFORE_OFFSET_COMMIT = "KP08-05"
    DURING_OFFSET_COMMIT = "KP08-06"
    AFTER_OFFSET_ACK_BEFORE_LOCAL_RECEIPT = "KP08-07"
    AFTER_POISON_DETECT_BEFORE_RECEIPT = "KP08-08"
    AFTER_QUARANTINE_COMMIT_BEFORE_OFFSET_ACK = "KP08-09"
    REBALANCE_DURING_INSERT = "KP08-10"

    # Build path
    BUILD_REQUESTED_BEFORE_LEASE = "KP08-11"
    AFTER_LEASE_BEFORE_MANIFEST = "KP08-12"
    AFTER_MANIFEST_BEFORE_RAW_SCAN = "KP08-13"
    MIDWAY_CONFLICT_SCAN = "KP08-14"
    AFTER_CONFLICT_SCAN_BEFORE_FACTS = "KP08-15"
    AFTER_10PCT_FACTS = "KP08-16"
    AFTER_90PCT_FACTS = "KP08-17"
    AFTER_FACTS_BEFORE_IDENTITY_PROOF = "KP08-18"
    AFTER_FACTS_VERIFIED_BEFORE_ROLLUP = "KP08-19"
    AFTER_PARTIAL_ROLLUP = "KP08-20"
    AFTER_ROLLUP_BEFORE_MONEY_ORACLE = "KP08-21"
    AFTER_ORACLES_BEFORE_EVIDENCE = "KP08-22"
    AFTER_EVIDENCE_BEFORE_VERIFIED = "KP08-23"
    AFTER_VERIFIED_BEFORE_POINTER_TX = "KP08-24"
    POINTER_COMMIT_ACK_LOST = "KP08-25"
    AFTER_PUBLISH_BEFORE_CACHE_INVALIDATION = "KP08-26"

    # Read / maintenance path
    CH_NODE_KILLED_DURING_READ = "KP08-27"
    GC_DELETING_WHILE_BUILD_ACQUIRED = "KP08-28"


ALL_KILL_POINTS: tuple[KillPoint, ...] = tuple(KillPoint)


#: Adapter-level fault sites. Unlike the kill points these do not stop the
#: process; they make an external system misbehave in a specific way.
class FaultSite(StrEnum):
    CH_INSERT_AMBIGUOUS = "ch.insert.ambiguous"
    CH_INSERT_REJECT = "ch.insert.reject"
    CH_INSERT_PARTIAL = "ch.insert.partial"
    CH_DISK_FULL = "ch.disk.full"
    CH_QUERY_TIMEOUT = "ch.query.timeout"
    CH_UNAVAILABLE = "ch.unavailable"
    CH_READER_LAG = "ch.reader.lag"
    KAFKA_COMMIT_FAIL = "kafka.commit.fail"
    KAFKA_COMMIT_ACK_LOST = "kafka.commit.ack_lost"
    KAFKA_UNAVAILABLE = "kafka.unavailable"
    PG_COMMIT_ACK_LOST = "pg.commit.ack_lost"
    PG_UNAVAILABLE = "pg.unavailable"
    AUDIT_UNAVAILABLE = "audit.unavailable"


@dataclass(slots=True)
class FaultAction:
    """One scheduled fault at one site."""

    site: str
    #: Called with the site's context; may raise. ``None`` means "crash here".
    action: Callable[[Mapping[str, Any]], None] | None = None
    #: How many times this action applies before it retires. ``None`` = always.
    times: int | None = 1
    #: Optional predicate narrowing the action to a specific context, e.g.
    #: only the batch containing a given offset.
    when: Callable[[Mapping[str, Any]], bool] | None = None
    fired: int = 0

    def matches(self, context: Mapping[str, Any]) -> bool:
        if self.times is not None and self.fired >= self.times:
            return False
        if self.when is not None and not self.when(context):
            return False
        return True

    def run(self, context: Mapping[str, Any]) -> None:
        self.fired += 1
        if self.action is None:
            raise CrashError(self.site)
        self.action(context)


class FaultPlan:
    """Deterministic schedule of faults, with an audit of what actually fired.

    No randomness and no wall-clock timing. A scenario either names the site
    it wants to break or it does not break, which is what makes a chaos suite
    rerunnable in CI (section 11, PASS condition (i)).
    """

    __slots__ = ("_actions", "_visits", "_fired", "_timeline")

    def __init__(self) -> None:
        self._actions: dict[str, list[FaultAction]] = {}
        self._visits: Counter[str] = Counter()
        self._fired: Counter[str] = Counter()
        self._timeline: list[dict[str, Any]] = []

    # -- scheduling --------------------------------------------------------

    def crash_at(
        self,
        site: str | KillPoint,
        *,
        times: int | None = 1,
        when: Callable[[Mapping[str, Any]], bool] | None = None,
    ) -> "FaultPlan":
        return self.add(FaultAction(site=str(site), action=None, times=times, when=when))

    def raise_at(
        self,
        site: str | KillPoint,
        error: BaseException | Callable[[Mapping[str, Any]], BaseException],
        *,
        times: int | None = 1,
        when: Callable[[Mapping[str, Any]], bool] | None = None,
    ) -> "FaultPlan":
        def action(context: Mapping[str, Any]) -> None:
            raise error(context) if callable(error) else error

        return self.add(FaultAction(site=str(site), action=action, times=times, when=when))

    def call_at(
        self,
        site: str | KillPoint,
        callback: Callable[[Mapping[str, Any]], None],
        *,
        times: int | None = 1,
        when: Callable[[Mapping[str, Any]], bool] | None = None,
    ) -> "FaultPlan":
        return self.add(FaultAction(site=str(site), action=callback, times=times, when=when))

    def add(self, action: FaultAction) -> "FaultPlan":
        self._actions.setdefault(action.site, []).append(action)
        return self

    # -- firing ------------------------------------------------------------

    def fire(self, site: str | KillPoint, **context: Any) -> None:
        """Consult the plan at ``site``. Raises if a fault is scheduled here."""
        key = str(site)
        self._visits[key] += 1
        actions = self._actions.get(key)
        if not actions:
            return
        for action in actions:
            if action.matches(context):
                self._fired[key] += 1
                self._timeline.append(
                    {"site": key, "visit": self._visits[key], "context": _redact(context)}
                )
                action.run(context)
                return

    def armed(self, site: str | KillPoint) -> bool:
        """Whether a fault is still pending at ``site``.

        Lets a caller skip expensive work it is about to lose anyway, without
        changing whether the fault fires.
        """
        return any(a.times is None or a.fired < a.times for a in self._actions.get(str(site), ()))

    # -- audit -------------------------------------------------------------

    @property
    def visits(self) -> Mapping[str, int]:
        return dict(self._visits)

    @property
    def fired(self) -> Mapping[str, int]:
        return dict(self._fired)

    def timeline(self) -> list[dict[str, Any]]:
        """``fault_timeline.jsonl`` content for the evidence bundle."""
        return list(self._timeline)

    def assert_fired(self, *sites: str | KillPoint) -> None:
        """Fail unless every named site actually delivered a fault.

        This is the guard that keeps section 11's ``NOT_RUN`` from being
        mislabelled ``PASS``.
        """
        missing = [str(s) for s in sites if self._fired.get(str(s), 0) == 0]
        if missing:
            raise AssertionError(
                "fault was scheduled but never fired at: "
                + ", ".join(missing)
                + f" (sites visited: {sorted(self._visits)})"
            )

    def pending(self) -> list[str]:
        """Scheduled faults that never fired; a scenario-design smell."""
        return sorted(
            site
            for site, actions in self._actions.items()
            if any(a.times is not None and a.fired < a.times for a in actions)
        )


class NullFaultPlan(FaultPlan):
    """A plan that can never be armed, for production wiring.

    Subclassing rather than threading ``plan is None`` checks through the call
    sites: the happy path then has exactly one shape, so a test exercises the
    same code the production deployment runs.
    """

    __slots__ = ()

    def add(self, action: FaultAction) -> "FaultPlan":  # pragma: no cover - defensive
        raise RuntimeError("NullFaultPlan cannot schedule faults")

    def fire(self, site: str | KillPoint, **context: Any) -> None:
        return None


def _redact(context: Mapping[str, Any]) -> dict[str, Any]:
    """Keep the timeline readable and free of payloads.

    Section 8.3 forbids high-cardinality identifiers in metrics; the same
    discipline applies to artefacts that get attached to tickets.
    """
    out: dict[str, Any] = {}
    for key, value in context.items():
        if key in ("payload", "payload_json", "rows", "records"):
            out[key] = f"<{_safe_len(value)} items>"
        elif isinstance(value, (str, int, bool)) or value is None:
            out[key] = value
        else:
            out[key] = repr(value)[:120]
    return out


def _safe_len(value: Any) -> int:
    try:
        return len(value)
    except TypeError:  # pragma: no cover - defensive
        return -1


def kill_point_catalog() -> list[dict[str, str]]:
    """The 28 sites with their design IDs, for the coverage report."""
    return [{"id": str(point), "site": point.name} for point in ALL_KILL_POINTS]
