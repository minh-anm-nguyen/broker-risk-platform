"""Fault rule selection.

Evaluation order is fixed by ``(priority, rule_id)``, never by insertion order
or dict iteration. A test that injects two rules must get the same outcome on
every run, and "whichever matched first" is not a specification (section 6.3).

At most one *terminal* action applies per request, plus at most one delay.
Composing two body mutations would produce a payload the test author never
described, so the second match is recorded as a match and skipped.

Probability is deterministic: it comes from
``SHA-256(seed | rule_id | match_index)``, so a chaos run that fails can be
replayed exactly instead of being re-rolled.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ..generation.deterministic_prng import DeterministicRandom
from ..observability.logging import get_logger
from ..observability.metrics import Metrics, NullMetrics
from ..settings import Settings
from .rules import (
    TERMINAL_MODES,
    FaultAction,
    FaultMode,
    FaultRule,
    TriggerKind,
)

logger = get_logger("fault_engine")


@dataclass(frozen=True, slots=True)
class RequestContext:
    method: str
    path: str
    account_id: str | None = None
    symbol: str | None = None


@dataclass(frozen=True, slots=True)
class FaultDecision:
    """What the middleware should do to this request."""

    delay_ms: int = 0
    delay_rule_id: str | None = None
    action: FaultAction | None = None
    rule_id: str | None = None

    @property
    def is_noop(self) -> bool:
        return self.delay_ms == 0 and self.action is None


class FaultEngine:
    """Holds the live rule set and decides what applies to a request.

    Rules live in memory and are the authority for matching: the middleware
    runs on every data-plane request, and a storage round trip there would put
    the fault engine in the read path's latency budget (WF-099). Durability is
    best effort via a persistence callback - fault rules are test
    instrumentation, and ``sim_fault_activations_total`` plus the structured
    log are the audit trail that matters (WF-112).
    """

    def __init__(
        self,
        settings: Settings,
        *,
        metrics: Metrics | NullMetrics | None = None,
    ) -> None:
        self._settings = settings
        self._metrics = metrics or NullMetrics()
        self._lock = threading.Lock()
        self._rules: dict[str, FaultRule] = {}
        self._ordered: tuple[FaultRule, ...] = ()
        self._seed = 0
        #: Master switch, so an operator can neutralize every rule in one call
        #: without having to delete them first (section 6.3).
        self._armed = True

    # -- configuration -----------------------------------------------------

    @property
    def armed(self) -> bool:
        return self._armed

    def set_armed(self, value: bool) -> None:
        with self._lock:
            self._armed = value

    def set_seed(self, seed: int) -> None:
        """Bind probabilistic activation to the active run's seed."""
        with self._lock:
            self._seed = seed

    def load(self, rules: list[FaultRule]) -> None:
        with self._lock:
            self._rules = {rule.rule_id: rule for rule in rules}
            self._reorder()

    def add(self, rule: FaultRule) -> None:
        with self._lock:
            if len(self._rules) >= self._settings.max_fault_rules and rule.rule_id not in self._rules:
                from ..domain.models import ValidationError

                raise ValidationError(
                    "the configured fault rule limit is already reached",
                    details={"max_fault_rules": self._settings.max_fault_rules},
                )
            self._rules[rule.rule_id] = rule
            self._reorder()

    def remove(self, rule_id: str) -> bool:
        with self._lock:
            existed = self._rules.pop(rule_id, None) is not None
            if existed:
                self._reorder()
            return existed

    def clear(self, *, run_id: str | None = None) -> int:
        with self._lock:
            if run_id is None:
                removed = len(self._rules)
                self._rules.clear()
            else:
                stale = [
                    key for key, rule in self._rules.items() if rule.run_id == run_id
                ]
                for key in stale:
                    del self._rules[key]
                removed = len(stale)
            self._reorder()
            return removed

    def get(self, rule_id: str) -> FaultRule | None:
        return self._rules.get(rule_id)

    def snapshot(self) -> tuple[FaultRule, ...]:
        """Immutable view of the rule set.

        Taken once at the start of a request so a concurrent rule change cannot
        alter the decision halfway through (WF-057).
        """
        return self._ordered

    def _reorder(self) -> None:
        self._ordered = tuple(
            sorted(self._rules.values(), key=lambda rule: (rule.priority, rule.rule_id))
        )

    def collect_expired(self) -> int:
        now = datetime.now(UTC)
        with self._lock:
            expired = [
                key
                for key, rule in self._rules.items()
                if rule.expired(now) or rule.exhausted
            ]
            for key in expired:
                del self._rules[key]
            if expired:
                self._reorder()
            return len(expired)

    # -- selection ---------------------------------------------------------

    def select(self, context: RequestContext) -> FaultDecision:
        """Choose what should happen to this request.

        Matching is counted here, because a request whose scope matched did
        match regardless of what follows - that is what keeps
        ``nth_matching_request`` meaningful. *Activation* is not counted here;
        the caller calls :meth:`commit` once the fault has actually been
        applied. Counting activations at selection time would report a fault as
        injected even when the handler errored and the mutation never ran, and
        a chaos result with an invented activation count proves nothing
        (WF-112, WF-119).
        """
        if not self._armed:
            return FaultDecision()
        rules = self.snapshot()
        if not rules:
            return FaultDecision()

        now = datetime.now(UTC)
        delay_ms = 0
        delay_rule_id: str | None = None
        action: FaultAction | None = None
        action_rule_id: str | None = None

        for rule in rules:
            if not rule.enabled or rule.exhausted or rule.expired(now):
                continue
            if not self._scope_matches(rule, context):
                continue

            with self._lock:
                rule.match_count += 1
                match_index = rule.match_count
            self._metrics.fault_matches.labels(rule_id=rule.rule_id).inc()

            is_delay = rule.action.mode is FaultMode.DELAY
            if (is_delay and delay_rule_id is not None) or (not is_delay and action is not None):
                # Already have one of this kind. The match was recorded above;
                # composing two body mutations would produce a payload no test
                # described, so this rule does not fire.
                continue
            if not self._trigger_fires(rule, match_index):
                continue

            if is_delay:
                delay_ms = int(rule.action.parameters.get("delay_ms", 0))
                delay_rule_id = rule.rule_id
            else:
                action = rule.action
                action_rule_id = rule.rule_id

        return FaultDecision(
            delay_ms=min(delay_ms, self._settings.max_fault_delay_ms),
            delay_rule_id=delay_rule_id,
            action=action,
            rule_id=action_rule_id,
        )

    def commit(self, rule_id: str) -> bool:
        """Record that a selected fault was actually applied."""
        with self._lock:
            rule = self._rules.get(rule_id)
            if rule is None:
                return False
            rule.activations += 1
            activations = rule.activations
            mode = rule.action.mode.value
            maximum = rule.trigger.max_activations
        self._metrics.fault_activations.labels(rule_id=rule_id, mode=mode).inc()
        logger.warning(
            "fault rule activated",
            extra={
                "rule_id": rule_id,
                "mode": mode,
                "action": mode,
                "activated": True,
                "activations": activations,
                "max_activations": maximum,
            },
        )
        return True

    @staticmethod
    def _scope_matches(rule: FaultRule, context: RequestContext) -> bool:
        scope = rule.scope
        if scope.method is not None and scope.method != context.method:
            return False
        if scope.path is not None and not context.path.startswith(scope.path):
            return False
        if scope.account_id is not None and scope.account_id != context.account_id:
            return False
        if scope.symbol is not None and scope.symbol != context.symbol:
            return False
        return True

    def _trigger_fires(self, rule: FaultRule, match_index: int) -> bool:
        trigger = rule.trigger
        if trigger.kind is TriggerKind.ALWAYS:
            return True
        if trigger.kind is TriggerKind.NTH_MATCHING_REQUEST:
            # Exactly the n-th matching request, so "request #3 fails" means
            # requests 1, 2 and 4 are untouched (WF-060).
            return match_index == trigger.n
        random = DeterministicRandom(self._seed, rule.rule_id)
        return random.probability_hit("fault", match_index, trigger.probability)

    # -- reporting ---------------------------------------------------------

    def list_payload(self) -> list[dict[str, Any]]:
        return [rule.to_payload() for rule in self.snapshot()]

    def activation_totals(self) -> dict[str, int]:
        return {rule.rule_id: rule.activations for rule in self.snapshot()}
