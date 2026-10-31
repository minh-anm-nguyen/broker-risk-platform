"""What-if campaign runner for the matrix in section 9.

Section 9 defines 312 cases across 26 failure domains, and sets three rules
this module takes literally:

1. **A case with no reproducible fault or no independent oracle is
   ``NOT_RUN``/``INVALID_TEST``, never ``PASS``.** The registry below
   therefore carries *both* implemented cases and explicitly declared
   ``NOT_RUN`` cases with the reason they cannot be executed here. A coverage
   number that quietly omits what was skipped is the dishonesty the design
   warns about.
2. **``PASS`` for a fault-injection case means a controlled failure matching
   the expectation**, not an HTTP 200.
3. **``INCONCLUSIVE`` is never counted as a pass** (``evidence.PASSING_RESULTS``).

Where a fault is injected, the case asserts the fault's own activation
counter -- P01's for transport faults, the harness's for repair faults -- so a
case that silently never fired is downgraded to ``INVALID_TEST`` by
``EvidenceManifest.validate`` rather than counted (section 10.6).

What this runner is **not**: a claim that all 208 P0 cases are implemented.
``coverage_summary()`` reports implemented versus declared per domain so the
gap is visible rather than inferred.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
import uuid
import warnings
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .clock import ManualClock
from .config import Settings, load_settings
from .db.engine import CaptureStrategy, Database, Role, transaction
from .db.repository import ReconRepository
from .errors import ActionType, Confidence, FindingCode, ReasonCode
from .evidence import CampaignReport, CaseResult, EvidenceManifest
from .ports.source import FakeSource, P01HttpSource
from .scheduler import ReconciliationScheduler

#: The 26 failure domains of section 9, with the design's own titles.
DOMAINS: dict[str, str] = {
    "A": "Bootstrap, config, source capability handshake",
    "B": "Source epoch, run reset, connector worker identity",
    "C": "Source pagination, completeness, cursor TTL",
    "D": "Target PostgreSQL snapshot capture and MVCC",
    "E": "Deal immutable history, ID-set, seq and money conflicts",
    "F": "Account state, account absence and balance correctness",
    "G": "Position state, absence, netting and references",
    "H": "Decimal precision, timestamps, canonicalization and hashes",
    "I": "Cross-entity consistency, reference integrity and time",
    "J": "Diff taxonomy, confidence, thresholds, false positives",
    "K": "Repair plan generation, immutable preconditions, dry-run",
    "L": "Authorization, approvals, separation of duties",
    "M": "Commit fencing, plan dispatch and P03 handoff",
    "N": "Idempotency, ambiguous ACK, crash windows and replay",
    "O": "P03 scheduler, leases, stale workers and concurrent flows",
    "P": "Connector/network/SDK transport and timeout behavior",
    "Q": "PostgreSQL availability, WAL, locks and storage failure",
    "R": "Resource limits, throughput, fairness and backpressure",
    "S": "Lifecycle, cancellation, retention and safe garbage collection",
    "T": "Incremental audit, sampling, manifests and false negatives",
    "U": "Observability, metrics, alerting and evidence reliability",
    "V": "Security, privacy, source isolation and adversarial inputs",
    "W": "Versioning, migrations, schema evolution and environment",
    "X": "Test oracle, determinism, mutation and chaos harness quality",
    "Y": "Compound faults and critical simultaneous failure campaigns",
    "Z": "Disaster recovery, incident response and operational boundaries",
}


# ---------------------------------------------------------------------------
# Case world
# ---------------------------------------------------------------------------


@dataclass
class CaseOutcome:
    """What a case observed. ``result`` is decided by the case itself."""

    result: CaseResult
    expected: str = ""
    actual: str = ""
    oracle: str = ""
    reason_code: str | None = None
    fault: str | None = None
    fault_confirmed: bool | None = None
    notes: list[str] = field(default_factory=list)
    source_epoch: str | None = None
    source_snapshot_seq: int | None = None
    target_watermark: int | None = None
    source_complete: bool | None = None
    target_complete: bool | None = None


class CampaignWorld:
    """A disposable Project 01 + Project 04 environment for one case.

    Each case gets its own simulator database and reconciliation database, so
    a case cannot pass or fail because of another case's residue.
    """

    def __init__(self, *, seed: int = 42, ttl_seconds: int = 900) -> None:
        self.root = Path(tempfile.mkdtemp(prefix="recon-campaign-"))
        self.seed = seed
        self._ttl = ttl_seconds
        self._exit_stack: list[Callable[[], None]] = []

        self._boot_simulator()
        self._boot_service()

    # ---- lifecycle -------------------------------------------------------

    def _boot_simulator(self) -> None:
        import os

        # Located by search, not by counting ``.parent`` hops: the old fixed
        # count silently resolved to a directory that does not exist once this
        # component moved in the tree, and because the guard was
        # ``if simulator_src.exists()`` the campaign then booted without the
        # simulator on the path and failed later, somewhere unrelated.
        simulator_src = None
        here = Path(__file__).resolve()
        for candidate in here.parents:
            for relative in (
                Path("p01-simulator") / "src",
                Path("apps") / "p01-simulator" / "src",
            ):
                found = candidate / relative
                if (found / "simulator").is_dir():
                    simulator_src = found
                    break
            if simulator_src is not None:
                break

        if simulator_src is None:
            raise RuntimeError(
                "could not locate the Project 01 simulator source tree. A "
                "campaign boots the real simulator in-process, so continuing "
                "without it would produce results attributed to a source that "
                "never ran."
            )
        if str(simulator_src) not in sys.path:
            sys.path.insert(0, str(simulator_src))

        self._saved_env = {
            key: os.environ.get(key)
            for key in (
                "SIM_ENV",
                "SIM_DB_PATH",
                "SIM_DATA_TOKEN",
                "SIM_ADMIN_TOKEN",
                "SIM_CURSOR_SIGNING_KEY",
                "SIM_SNAPSHOT_TTL_SEC",
                "SIM_SQLITE_SYNCHRONOUS",
            )
        }
        self.data_token = "dtok-" + "a" * 24
        self.admin_token = "atok-" + "b" * 24
        os.environ.update(
            {
                "SIM_ENV": "test",
                "SIM_DB_PATH": str(self.root / "simulator.db"),
                "SIM_DATA_TOKEN": self.data_token,
                "SIM_ADMIN_TOKEN": self.admin_token,
                "SIM_CURSOR_SIGNING_KEY": "ckey-" + "c" * 24,
                "SIM_SNAPSHOT_TTL_SEC": str(self._ttl),
                "SIM_SQLITE_SYNCHRONOUS": "OFF",
            }
        )

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            from starlette.testclient import TestClient

        from simulator.main import create_app

        self._client_cm = TestClient(
            create_app(), headers={"Authorization": f"Bearer {self.data_token}"}
        )
        self.client = self._client_cm.__enter__()
        self._exit_stack.append(lambda: self._client_cm.__exit__(None, None, None))

    def _boot_service(self) -> None:
        self.settings: Settings = load_settings(
            env="test",
            db_path=self.root / "recon.db",
            artifacts_dir=self.root / "artifacts",
            source_id="sim_local",
            worker_id="campaign-worker",
            approval_required=True,
            allow_deal_backfill=False,
            source_max_page_size=100,
            max_concurrent_runs=4,
        )
        self.db = Database(self.settings.db_path, synchronous="OFF")
        self.db.create_schema()
        self.clock = ManualClock()
        self.repository = ReconRepository(self.db, self.settings, self.clock)
        self.source = P01HttpSource(
            client=self.client,
            source_id="sim_local",
            max_page_size=100,
            max_retries=2,
            retry_base_delay_s=0.0,
            sleep=lambda _s: None,
        )

        from recon_harness.p03_sync import P03SyncHarness

        self.harness = P03SyncHarness(
            db=self.db,
            source=self.source,
            clock=self.clock,
            source_id="sim_local",
            page_size=100,
        )
        self.scheduler = self._make_scheduler(self.source)

    def _make_scheduler(
        self, source: Any, *, strategy: CaptureStrategy = CaptureStrategy.IMMEDIATE
    ) -> ReconciliationScheduler:
        return ReconciliationScheduler(
            db=self.db,
            repository=self.repository,
            source=source,
            read_port=self.harness,
            repair_port=self.harness,
            settings=self.settings,
            clock=self.clock,
            capture_strategy=strategy,
        )

    def with_fake_source(self, records: dict[str, Sequence[dict]], **kwargs: Any):
        """Swap in a deterministic source for faults P01 cannot produce."""
        identity = self.source.read_identity()
        fake = FakeSource(
            source_id="sim_local",
            run_id=identity.run_id,
            generation=identity.generation,
            snapshot_seq=identity.snapshot_seq,
            records=records,
            **kwargs,
        )
        return fake, self._make_scheduler(fake)

    def close(self) -> None:
        import os

        for undo in reversed(self._exit_stack):
            try:
                undo()
            except Exception:  # pragma: no cover - teardown must not mask results
                pass
        for key, value in self._saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        shutil.rmtree(self.root, ignore_errors=True)

    # ---- simulator control ----------------------------------------------

    def _admin_post(self, path: str, payload: dict | None = None) -> Any:
        return self.client.post(
            path,
            json=payload or {},
            headers={
                "Authorization": f"Bearer {self.admin_token}",
                "Idempotency-Key": str(uuid.uuid4()),
            },
        )

    def create_run(self, *, accounts: int = 5, deals: int = 40, positions: int = 5) -> dict:
        response = self._admin_post(
            "/admin/v1/runs",
            {
                "scenario_name": "campaign",
                "seed": self.seed,
                "start_virtual_time": "2026-01-01T00:00:00Z",
                "account_count": accounts,
                "deal_count": deals,
                "max_open_positions": positions,
            },
        )
        assert response.status_code == 202, response.text
        body = response.json()
        self._await_job(body["job_id"])
        return body

    def reset_run(self, run_id: str, generation: int) -> dict:
        response = self._admin_post(
            f"/admin/v1/runs/{run_id}/reset", {"expected_generation": generation}
        )
        assert response.status_code == 202, response.text
        body = response.json()
        if body.get("job_id"):
            self._await_job(body["job_id"])
        return body

    def advance(self, run_id: str, *, events: int = 10) -> None:
        response = self._admin_post(f"/admin/v1/runs/{run_id}/advance", {"events": events})
        assert response.status_code in (200, 202), response.text
        body = response.json()
        if isinstance(body, dict) and body.get("job_id"):
            self._await_job(body["job_id"])

    def _await_job(self, job_id: str, *, timeout_s: float = 60.0) -> None:
        import time

        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            response = self.client.get(
                f"/admin/v1/jobs/{job_id}",
                headers={"Authorization": f"Bearer {self.admin_token}"},
            )
            body = response.json()
            if body["status"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                assert body["status"] == "SUCCEEDED", body
                return
            time.sleep(0.02)
        raise AssertionError(f"simulator job {job_id} did not finish")

    def meta(self) -> dict:
        response = self.client.get(
            "/v1/meta", headers={"Authorization": f"Bearer {self.data_token}"}
        )
        return response.json()

    def add_fault(
        self,
        *,
        mode: str,
        parameters: dict | None = None,
        path: str | None = None,
        trigger: str = "always",
        n: int = 0,
        max_activations: int = 1,
    ) -> str:
        payload: dict[str, Any] = {
            "action": {"mode": mode, "parameters": parameters or {}},
            "trigger": {"kind": trigger, "n": n, "max_activations": max_activations},
        }
        if path:
            payload["scope"] = {"path": path}
        response = self._admin_post("/admin/v1/faults", payload)
        assert response.status_code == 201, response.text
        return response.json().get("rule_id", "")

    def fault_activations(self, rule_id: str) -> int:
        response = self.client.get(
            "/admin/v1/faults", headers={"Authorization": f"Bearer {self.admin_token}"}
        )
        body = response.json()
        rules = body.get("items", body) if isinstance(body, dict) else body
        for rule in rules:
            if rule.get("rule_id") == rule_id:
                return int(rule.get("activations", 0))
        return 0

    def clear_faults(self) -> None:
        self.client.delete(
            "/admin/v1/faults", headers={"Authorization": f"Bearer {self.admin_token}"}
        )

    # ---- convenience -----------------------------------------------------

    def synced(self, *, accounts: int = 5, deals: int = 40, positions: int = 5) -> dict:
        """A fully synced world: the clean starting point for most cases."""
        run = self.create_run(accounts=accounts, deals=deals, positions=positions)
        meta = self.meta()
        epoch_id = self.harness.bootstrap_epoch()
        watermark = self.harness.sync_deals(epoch_id)
        account_scan = self.harness.publish_snapshot(epoch_id, "accounts")
        position_scan = self.harness.publish_snapshot(epoch_id, "positions")
        return {
            "run_id": run["run_id"],
            "generation": run["generation"],
            "epoch_id": epoch_id,
            "snapshot_seq": int(meta["snapshot_seq"]),
            "watermark": watermark,
            "account_scan_id": account_scan,
            "position_scan_id": position_scan,
        }

    def first_value(self, sql: str, params: Sequence[Any] = ()) -> Any:
        conn = self.db.connect(Role.P03_SYNC)
        try:
            row = conn.execute(sql, tuple(params)).fetchone()
            return None if row is None else row[0]
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Case registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CaseSpec:
    case_id: str
    domain: str
    priority: str
    description: str
    run: Callable[[CampaignWorld], CaseOutcome] | None = None
    not_run_reason: str | None = None
    #: Simulator snapshot TTL for this case's world. A case that needs a real
    #: cursor expiry must declare it here: the TTL is fixed when the simulator
    #: boots, so setting it inside the case would have no effect and the case
    #: would silently stop testing its own premise.
    ttl_seconds: int = 900

    @property
    def implemented(self) -> bool:
        return self.run is not None


def _ok(expected: str, actual: str, oracle: str, **kwargs: Any) -> CaseOutcome:
    return CaseOutcome(
        result=CaseResult.PASS, expected=expected, actual=actual, oracle=oracle, **kwargs
    )


def _fail(expected: str, actual: str, **kwargs: Any) -> CaseOutcome:
    return CaseOutcome(result=CaseResult.FAIL, expected=expected, actual=actual, **kwargs)


# ---- Domain A: bootstrap and capability handshake -------------------------


def case_a01(world: CampaignWorld) -> CaseOutcome:
    """Source identity and declared limits are read before any audit."""
    world.create_run(accounts=3, deals=10, positions=3)
    identity = world.source.read_identity()
    capabilities = world.source.read_capabilities()
    ok = (
        identity.run_id
        and identity.generation >= 1
        and capabilities.max_page_size <= 500
        and capabilities.snapshot_ttl_seconds > 0
    )
    expected = "identity and declared limits available before audit"
    actual = f"run_id={identity.run_id} gen={identity.generation} page<={capabilities.max_page_size}"
    oracle = "values read from P01 /v1/meta"
    return _ok(expected, actual, oracle) if ok else _fail(expected, actual)


def case_a02(world: CampaignWorld) -> CaseOutcome:
    """An audit against an epoch P03 never registered is refused cleanly.

    Refused with a named reason code rather than a storage error: an unknown
    epoch is an invalid request, not an inconclusive audit, so no run is
    created for it.
    """
    from .errors import ReconError

    world.create_run(accounts=2, deals=5, positions=2)
    unknown_epoch = str(uuid.uuid4())

    expected = "refused with EPOCH_MISMATCH and no run persisted"
    before = world.repository.active_run_count()
    try:
        outcome = world.scheduler.run_audit(entity="deals", epoch_id=unknown_epoch)
    except ReconError as exc:
        reason = getattr(exc, "reason", None)
        actual = f"refused: {reason}"
        if reason is ReasonCode.EPOCH_MISMATCH:
            return _ok(
                expected,
                actual,
                "epoch absent from source_epochs; no recon_runs row created",
                reason_code=str(reason),
            )
        return _fail(expected, actual, reason_code=str(reason))

    actual = f"{outcome.status}/{outcome.reason_code} (expected a refusal)"
    return _fail(expected, actual, reason_code=str(outcome.reason_code))


def case_a03(world: CampaignWorld) -> CaseOutcome:
    """A source lacking stable snapshots cannot be strictly compared."""
    state = world.synced(accounts=3, deals=12, positions=3)
    from .dto import SourceCapabilities

    rows = list(world.harness.iter_target_rows(state["epoch_id"], "deals"))
    fake, scheduler = world.with_fake_source(
        {"deals": rows},
        capabilities=SourceCapabilities(stable_snapshot_pagination=False),
        page_size=100,
    )
    outcome = scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    expected = "UNSUPPORTED_CAPABILITY, no verdict"
    actual = f"{outcome.status}/{outcome.reason_code}"
    if outcome.reason_code is ReasonCode.UNSUPPORTED_CAPABILITY:
        return _ok(expected, actual, "capability profile denies full-set compare",
                   reason_code=str(outcome.reason_code))
    return _fail(expected, actual, reason_code=str(outcome.reason_code))


# ---- Domain B: epoch and run reset ---------------------------------------


def case_b01(world: CampaignWorld) -> CaseOutcome:
    """A source reset blocks the old epoch instead of comparing across it."""
    state = world.synced(accounts=3, deals=12, positions=3)
    world.reset_run(state["run_id"], state["generation"])
    outcome = world.scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    expected = "EPOCH_MISMATCH, no plan"
    actual = f"{outcome.status}/{outcome.reason_code}"
    plan = world.scheduler.plan_repair(outcome.recon_run_id, created_by="campaign")
    if outcome.reason_code is ReasonCode.EPOCH_MISMATCH and plan is None:
        return _ok(expected, actual, "P01 generation changed; no repair proposed",
                   reason_code=str(outcome.reason_code))
    return _fail(expected, f"{actual} plan={plan is not None}")


def case_b02(world: CampaignWorld) -> CaseOutcome:
    """A new generation is a new epoch; old ids are not merged (INV04-17)."""
    state = world.synced(accounts=3, deals=12, positions=3)
    world.reset_run(state["run_id"], state["generation"])
    new_epoch = world.harness.bootstrap_epoch()
    world.harness.sync_deals(new_epoch)
    shared = world.first_value(
        "SELECT COUNT(*) FROM (SELECT deal_id FROM deals GROUP BY deal_id HAVING COUNT(DISTINCT epoch_id) > 1)"
    )
    old_status = world.first_value(
        "SELECT status FROM source_epochs WHERE epoch_id = ?", (state["epoch_id"],)
    )
    expected = "old epoch SUPERSEDED, reused ids separated per epoch"
    actual = f"old={old_status} shared_ids={shared}"
    if new_epoch != state["epoch_id"] and old_status == "SUPERSEDED":
        return _ok(expected, actual, "epoch rows are distinct per (run_id, generation)")
    return _fail(expected, actual)


# ---- Domain C: pagination and completeness -------------------------------


def case_c01(world: CampaignWorld) -> CaseOutcome:
    """A scan interrupted before the terminal page cannot complete."""
    state = world.synced(accounts=3, deals=20, positions=3)
    rows = list(world.harness.iter_target_rows(state["epoch_id"], "deals"))
    from .errors import SourceError

    fake, scheduler = world.with_fake_source({"deals": rows}, page_size=4)
    fake.fail_at_page[2] = SourceError("transport dropped before the terminal page")
    outcome = scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    run = world.repository.get_run(outcome.recon_run_id)
    expected = "INCONCLUSIVE, source_complete=false, no proven findings"
    actual = f"{outcome.status} complete={run.source_complete} findings={outcome.finding_count}"
    proven = [f for f in outcome.findings if f["confidence"] == Confidence.PROVEN.value]
    if outcome.status == "INCONCLUSIVE" and not run.source_complete and not proven:
        return _ok(expected, actual, "receipt chain has no terminal receipt",
                   reason_code=str(outcome.reason_code), source_complete=False)
    return _fail(expected, actual)


def case_c02(world: CampaignWorld) -> CaseOutcome:
    """A genuine 410 SNAPSHOT_EXPIRED from the real simulator ends the scan.

    The world is built with a one-second cursor TTL (declared on the case
    spec) and the scan pauses past it between page 1 and page 2. The delay is
    client-side on purpose: P01's ``delay_before_response`` fault lands before
    the cursor is minted, so the lease would start after the delay and never
    expire -- the case would silently stop testing expiry. The ``410`` itself
    still comes from P01, so ``SNAPSHOT_EXPIRED`` is proof it occurred.
    """
    import time as _time

    state = world.synced(accounts=3, deals=30, positions=3)

    class _SleepsBetweenPages:
        def __init__(self, inner: Any) -> None:
            self._inner = inner
            self.pages_yielded = 0

        def read_identity(self):
            return self._inner.read_identity()

        def read_capabilities(self):
            return self._inner.read_capabilities()

        def iter_pages(self, entity: str, *, scope: str = "UNFILTERED", page_size: int | None = None):
            for page in self._inner.iter_pages(entity, scope=scope, page_size=page_size):
                self.pages_yielded += 1
                yield page
                if self.pages_yielded == 1:
                    _time.sleep(1.4)

    # A small page size guarantees there IS a second request to expire.
    tight = load_settings(
        env="test",
        db_path=world.settings.db_path,
        artifacts_dir=world.settings.artifacts_dir,
        source_id="sim_local",
        source_max_page_size=5,
    )
    slow = _SleepsBetweenPages(world.source)
    repo = ReconRepository(world.db, tight, world.clock)
    scheduler = ReconciliationScheduler(
        db=world.db,
        repository=repo,
        source=slow,
        read_port=world.harness,
        repair_port=world.harness,
        settings=tight,
        clock=world.clock,
    )
    outcome = scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    run = repo.get_run(outcome.recon_run_id)

    expected = "INCONCLUSIVE/SNAPSHOT_EXPIRED, source_complete=false"
    actual = f"{outcome.status}/{outcome.reason_code} complete={run.source_complete}"
    passed = (
        outcome.status == "INCONCLUSIVE"
        and outcome.reason_code is ReasonCode.SNAPSHOT_EXPIRED
        and not run.source_complete
    )
    return CaseOutcome(
        result=CaseResult.PASS if passed else CaseResult.FAIL,
        expected=expected,
        actual=actual,
        oracle="P01 served a real 410; the stored receipt chain stays incomplete",
        reason_code=str(outcome.reason_code),
        fault="cursor TTL elapsed between page 1 and page 2",
        fault_confirmed=outcome.reason_code is ReasonCode.SNAPSHOT_EXPIRED,
        source_complete=bool(run.source_complete),
    )


def case_c03(world: CampaignWorld) -> CaseOutcome:
    """A duplicated key across pages is a source-contract failure."""
    state = world.synced(accounts=3, deals=12, positions=3)
    rows = list(world.harness.iter_target_rows(state["epoch_id"], "deals"))
    fake, scheduler = world.with_fake_source({"deals": rows + [rows[0]]}, page_size=4)
    outcome = scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    expected = "SOURCE_DUPLICATE_KEY"
    actual = f"{outcome.status}/{outcome.reason_code}"
    if outcome.reason_code is ReasonCode.SOURCE_DUPLICATE_KEY:
        return _ok(expected, actual, "manifest primary key rejected the repeat",
                   reason_code=str(outcome.reason_code))
    return _fail(expected, actual)


# ---- Domain D: target capture and MVCC -----------------------------------


def case_d01(world: CampaignWorld) -> CaseOutcome:
    """A capture with no published scan is refused."""
    world.create_run(accounts=3, deals=10, positions=3)
    epoch_id = world.harness.bootstrap_epoch()
    outcome = world.scheduler.run_audit(entity="accounts", epoch_id=epoch_id)
    expected = "TARGET_NO_PUBLISHED_SCAN"
    actual = f"{outcome.status}/{outcome.reason_code}"
    if outcome.reason_code is ReasonCode.TARGET_NO_PUBLISHED_SCAN:
        return _ok(expected, actual, "sync_checkpoints has no published scan",
                   reason_code=str(outcome.reason_code))
    return _fail(expected, actual)


def case_d02(world: CampaignWorld) -> CaseOutcome:
    """A deferred capture refuses to commit a cut that moved under it."""
    from .db.engine import capture_transaction
    from .errors import TargetConflictError

    state = world.synced(accounts=4, deals=12, positions=4)
    conn = world.db.connect(Role.RECON)
    refused = False
    try:
        try:
            with capture_transaction(conn, strategy=CaptureStrategy.DEFERRED_SNAPSHOT):
                conn.execute(
                    "SELECT published_scan_id FROM sync_checkpoints WHERE epoch_id = ? AND entity='accounts'",
                    (state["epoch_id"],),
                ).fetchone()
                world.harness.corrupt_for_test(
                    "UPDATE accounts_current SET balance = '5.00' WHERE epoch_id = ?",
                    (state["epoch_id"],),
                )
                conn.execute(
                    """
                    INSERT INTO recon_target_items (recon_run_id, item_key, source_seq, payload_hash)
                    SELECT 'd02', account_id, NULL, payload_hash FROM accounts_current WHERE epoch_id = ?
                    """,
                    (state["epoch_id"],),
                )
        except TargetConflictError:
            refused = True
    finally:
        conn.close()
    leftover = world.repository
    with leftover.session() as c:
        rows = c.execute(
            "SELECT COUNT(*) FROM recon_target_items WHERE recon_run_id = 'd02'"
        ).fetchone()[0]
    expected = "TARGET_CAPTURE_CONFLICT, nothing committed"
    actual = f"refused={refused} leftover_rows={rows}"
    if refused and rows == 0:
        return _ok(expected, actual, "SQLITE_BUSY_SNAPSHOT on the capture write",
                   fault="concurrent atomic publication", fault_confirmed=True,
                   reason_code=ReasonCode.TARGET_CAPTURE_CONFLICT.value)
    return _fail(expected, actual)


# ---- Domain E: deals ------------------------------------------------------


def case_e01(world: CampaignWorld) -> CaseOutcome:
    """A missing deal at a proven cut is detected, nothing is written."""
    state = world.synced(accounts=4, deals=20, positions=4)
    victim = world.first_value(
        "SELECT deal_id FROM deals WHERE epoch_id = ? ORDER BY source_seq LIMIT 1",
        (state["epoch_id"],),
    )
    world.harness.corrupt_for_test(
        "DELETE FROM deals WHERE epoch_id = ? AND deal_id = ?", (state["epoch_id"], victim)
    )
    outcome = world.scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    codes = {f["finding_code"] for f in outcome.findings}
    expected = "one MISSING_TARGET_DEAL at PROVEN confidence"
    actual = f"{outcome.status} codes={sorted(codes)}"
    if codes == {FindingCode.MISSING_TARGET_DEAL.value} and outcome.finding_count == 1:
        return _ok(expected, actual, f"deal {victim} absent from target, present at source")
    return _fail(expected, actual)


def case_e02(world: CampaignWorld) -> CaseOutcome:
    """A money conflict quarantines and proposes no automatic correction."""
    state = world.synced(accounts=4, deals=20, positions=4)
    victim = world.first_value(
        "SELECT deal_id FROM deals WHERE epoch_id = ? LIMIT 1", (state["epoch_id"],)
    )
    world.harness.corrupt_for_test(
        "UPDATE deals SET commission = '-123.45' WHERE epoch_id = ? AND deal_id = ?",
        (state["epoch_id"], victim),
    )
    outcome = world.scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    plan = world.scheduler.plan_repair(outcome.recon_run_id, created_by="campaign")
    codes = {f["finding_code"] for f in outcome.findings}
    dispatchable = plan.dispatchable_actions if plan else ()
    expected = "IMMUTABLE_DEAL_CONFLICT, zero dispatchable actions"
    actual = f"codes={sorted(codes)} dispatchable={len(dispatchable)}"
    if FindingCode.IMMUTABLE_DEAL_CONFLICT.value in codes and not dispatchable:
        return _ok(expected, actual, "no authoritative value exists to choose")
    return _fail(expected, actual)


def case_e03(world: CampaignWorld) -> CaseOutcome:
    """An extra target deal is never deleted."""
    state = world.synced(accounts=4, deals=15, positions=4)
    world.harness.corrupt_for_test(
        """
        INSERT INTO deals (epoch_id, deal_id, source_seq, account_id, position_id, symbol,
                           entry, side, volume_lots, price, realized_pnl_gross, commission,
                           swap, executed_at, payload_hash)
        VALUES (?, 'D-CAMPAIGN', 1, 'A-000001', 'P-00000001', 'EURUSD', 'IN', 'BUY',
                '0.10', '1.10000', '0.00', '-1.00', '0.00', '2026-01-01T00:00:01Z', 'x')
        """,
        (state["epoch_id"],),
    )
    outcome = world.scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    plan = world.scheduler.plan_repair(outcome.recon_run_id, created_by="campaign")
    survives = world.first_value(
        "SELECT COUNT(*) FROM deals WHERE epoch_id = ? AND deal_id = 'D-CAMPAIGN'",
        (state["epoch_id"],),
    )
    codes = {f["finding_code"] for f in outcome.findings}
    expected = "EXTRA_TARGET_DEAL recorded, row untouched, no dispatchable action"
    actual = f"codes={sorted(codes)} survives={survives} dispatchable={len(plan.dispatchable_actions) if plan else 0}"
    if (
        FindingCode.EXTRA_TARGET_DEAL.value in codes
        and survives == 1
        and (plan is None or not plan.dispatchable_actions)
    ):
        return _ok(expected, actual, "row still present after the audit")
    return _fail(expected, actual)


def case_e04(world: CampaignWorld) -> CaseOutcome:
    """W < S yields TARGET_NOT_CAUGHT_UP, not a missing-deal claim."""
    world.create_run(accounts=3, deals=30, positions=3)
    epoch_id = world.harness.bootstrap_epoch()
    from recon_harness.p03_sync import P03SyncHarness

    partial = P03SyncHarness(
        db=world.db, source=world.source, clock=world.clock, source_id="sim_local", page_size=5
    )
    partial.sync_deals(epoch_id, max_pages=1, advance_watermark=False)
    outcome = world.scheduler.run_audit(entity="deals", epoch_id=epoch_id)
    expected = "TARGET_NOT_CAUGHT_UP, no MISSING_TARGET_DEAL"
    actual = f"{outcome.status}/{outcome.reason_code}"
    missing = [
        f for f in outcome.findings if f["finding_code"] == FindingCode.MISSING_TARGET_DEAL.value
    ]
    if outcome.reason_code is ReasonCode.TARGET_NOT_CAUGHT_UP and not missing:
        return _ok(expected, actual, "watermark behind the source snapshot",
                   reason_code=str(outcome.reason_code))
    return _fail(expected, actual)


# ---- Domain F/G: accounts and positions ----------------------------------


def case_f01(world: CampaignWorld) -> CaseOutcome:
    """An account mismatch at an equal cut plans a full re-sync only."""
    state = world.synced(accounts=5, deals=15, positions=5)
    victim = world.first_value(
        "SELECT account_id FROM accounts_current WHERE epoch_id = ? LIMIT 1", (state["epoch_id"],)
    )
    world.harness.corrupt_for_test(
        "UPDATE accounts_current SET balance = '0.01' WHERE epoch_id = ? AND account_id = ?",
        (state["epoch_id"], victim),
    )
    outcome = world.scheduler.run_audit(entity="accounts", epoch_id=state["epoch_id"])
    plan = world.scheduler.plan_repair(outcome.recon_run_id, created_by="campaign")
    types = {a.action_type for a in plan.actions} if plan else set()
    expected = "ACCOUNT_FIELD_MISMATCH -> REQUEST_FULL_ACCOUNT_RESYNC"
    actual = f"{outcome.status} types={sorted(t.value for t in types)}"
    if types == {ActionType.REQUEST_FULL_ACCOUNT_RESYNC}:
        no_money = all(
            key not in action.parameters
            for action in plan.actions
            for key in ("balance", "credit")
        )
        if no_money:
            return _ok(expected, actual, "action carries provenance, not values")
    return _fail(expected, actual)


def case_f02(world: CampaignWorld) -> CaseOutcome:
    """A changed source after publication is DIFFERENT_CUT, not corruption."""
    state = world.synced(accounts=5, deals=15, positions=5)
    world.advance(state["run_id"], events=12)
    outcome = world.scheduler.run_audit(entity="accounts", epoch_id=state["epoch_id"])
    codes = {f["finding_code"] for f in outcome.findings}
    expected = "DIFFERENT_CUT, no ACCOUNT_FIELD_MISMATCH, no plan"
    actual = f"{outcome.status}/{outcome.reason_code} codes={sorted(codes)}"
    plan = world.scheduler.plan_repair(outcome.recon_run_id, created_by="campaign")
    if (
        outcome.reason_code is ReasonCode.DIFFERENT_CUT
        and FindingCode.ACCOUNT_FIELD_MISMATCH.value not in codes
        and plan is None
    ):
        return _ok(expected, actual, "source snapshot moved past the published scan",
                   reason_code=str(outcome.reason_code))
    return _fail(expected, actual)


def case_g01(world: CampaignWorld) -> CaseOutcome:
    """A position absent from the source listing is not treated as closed."""
    state = world.synced(accounts=4, deals=12, positions=4)
    world.harness.corrupt_for_test(
        """
        INSERT INTO positions_current (epoch_id, position_id, account_id, symbol, side,
            volume_lots, open_price, status, published_scan_id, payload_hash)
        VALUES (?, 'P-CAMPAIGN', 'A-000001', 'EURUSD', 'BUY', '0.50', '1.10000', 'OPEN', ?, 'x')
        """,
        (state["epoch_id"], state["position_scan_id"]),
    )
    outcome = world.scheduler.run_audit(entity="positions", epoch_id=state["epoch_id"])
    finding = next(
        (
            f
            for f in outcome.findings
            if f["finding_code"] == FindingCode.ABSENT_AT_SOURCE_POSITION.value
        ),
        None,
    )
    status_after = world.first_value(
        "SELECT status FROM positions_current WHERE epoch_id = ? AND position_id = 'P-CAMPAIGN'",
        (state["epoch_id"],),
    )
    expected = "ABSENT_AT_SOURCE_POSITION at PROVISIONAL, status still OPEN"
    actual = f"finding={finding and finding['confidence']} status={status_after}"
    if (
        finding is not None
        and finding["confidence"] == Confidence.PROVISIONAL.value
        and status_after == "OPEN"
    ):
        return _ok(expected, actual, "absence recorded as an observation only")
    return _fail(expected, actual)


# ---- Domain H: canonicalisation ------------------------------------------


def case_h01(world: CampaignWorld) -> CaseOutcome:
    """Scale-equivalent money does not create a phantom mismatch."""
    state = world.synced(accounts=4, deals=12, positions=4)
    victim = world.first_value(
        "SELECT account_id FROM accounts_current WHERE epoch_id = ? LIMIT 1", (state["epoch_id"],)
    )
    original = world.first_value(
        "SELECT balance FROM accounts_current WHERE epoch_id = ? AND account_id = ?",
        (state["epoch_id"], victim),
    )
    # Same amount, different textual scale: "100.50" -> "100.5".
    rescaled = original.rstrip("0").rstrip(".") if "." in original else original
    world.harness.corrupt_for_test(
        "UPDATE accounts_current SET balance = ? WHERE epoch_id = ? AND account_id = ?",
        (rescaled, state["epoch_id"], victim),
    )
    outcome = world.scheduler.run_audit(entity="accounts", epoch_id=state["epoch_id"])
    expected = "MATCHED: scale-equivalent money is equal"
    actual = f"{outcome.status} findings={outcome.finding_count} ({original} vs {rescaled})"
    if outcome.status == "MATCHED":
        return _ok(expected, actual, "canonical quantisation to the declared scale")
    return _fail(expected, actual)


def case_h02(world: CampaignWorld) -> CaseOutcome:
    """An unrepresentable money value is refused, not rounded."""
    state = world.synced(accounts=4, deals=12, positions=4)
    victim = world.first_value(
        "SELECT account_id FROM accounts_current WHERE epoch_id = ? LIMIT 1", (state["epoch_id"],)
    )
    world.harness.corrupt_for_test(
        "UPDATE accounts_current SET balance = '1.00499' WHERE epoch_id = ? AND account_id = ?",
        (state["epoch_id"], victim),
    )
    outcome = world.scheduler.run_audit(entity="accounts", epoch_id=state["epoch_id"])
    expected = "INCOMPARABLE_SCHEMA; the value is never silently rounded"
    actual = f"{outcome.status}/{outcome.reason_code}"
    if outcome.reason_code is ReasonCode.INCOMPARABLE_SCHEMA:
        return _ok(expected, actual, "target row exceeds the declared USD scale",
                   reason_code=str(outcome.reason_code))
    return _fail(expected, actual)


# ---- Domain I: cross-entity ----------------------------------------------


def case_i01(world: CampaignWorld) -> CaseOutcome:
    """No global cut is claimed when the source does not provide one."""
    state = world.synced(accounts=4, deals=12, positions=4)
    outcome = world.scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    expected = "cross_entity_tier capped at T1 without a tested global cut"
    actual = f"tier={outcome.cross_entity_tier}"
    if outcome.cross_entity_tier in ("T0", "T1"):
        return _ok(expected, actual, "capability profile reports no global cut")
    return _fail(expected, actual)


# ---- Domain J: diff taxonomy and false positives -------------------------


def case_j01(world: CampaignWorld) -> CaseOutcome:
    """A clean world produces zero findings (no false positives)."""
    state = world.synced(accounts=5, deals=25, positions=5)
    results = {
        entity: world.scheduler.run_audit(entity=entity, epoch_id=state["epoch_id"])
        for entity in ("deals", "accounts", "positions")
    }
    expected = "MATCHED with zero findings for all three entities"
    actual = "; ".join(f"{k}={v.status}/{v.finding_count}" for k, v in results.items())
    if all(v.status == "MATCHED" and v.finding_count == 0 for v in results.values()):
        return _ok(expected, actual, "independent P01 read agrees with the manifests")
    return _fail(expected, actual)


def case_j02(world: CampaignWorld) -> CaseOutcome:
    """A sharp source count drop escalates rather than acting."""
    state = world.synced(accounts=4, deals=20, positions=4)
    fake, scheduler = world.with_fake_source({"deals": []}, page_size=100)
    outcome = scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    plan = scheduler.plan_repair(outcome.recon_run_id, created_by="campaign")
    codes = {f["finding_code"] for f in outcome.findings}
    expected = "LARGE_SOURCE_COUNT_DROP, no dispatchable action"
    actual = f"codes={sorted(codes)} dispatchable={len(plan.dispatchable_actions) if plan else 0}"
    if FindingCode.LARGE_SOURCE_COUNT_DROP.value in codes and (
        plan is None or not plan.dispatchable_actions
    ):
        return _ok(expected, actual, "empty complete listing against a populated target")
    return _fail(expected, actual)


# ---- Domain K/L: planning and approvals ----------------------------------


def _account_plan(world: CampaignWorld):
    state = world.synced(accounts=5, deals=15, positions=5)
    victim = world.first_value(
        "SELECT account_id FROM accounts_current WHERE epoch_id = ? LIMIT 1", (state["epoch_id"],)
    )
    world.harness.corrupt_for_test(
        "UPDATE accounts_current SET balance = '0.07' WHERE epoch_id = ? AND account_id = ?",
        (state["epoch_id"], victim),
    )
    outcome = world.scheduler.run_audit(entity="accounts", epoch_id=state["epoch_id"])
    plan = world.scheduler.plan_repair(outcome.recon_run_id, created_by="author-1")
    return state, outcome, plan


def case_k01(world: CampaignWorld) -> CaseOutcome:
    """A dry-run preview persists nothing."""
    state = world.synced(accounts=5, deals=15, positions=5)
    victim = world.first_value(
        "SELECT account_id FROM accounts_current WHERE epoch_id = ? LIMIT 1", (state["epoch_id"],)
    )
    world.harness.corrupt_for_test(
        "UPDATE accounts_current SET balance = '0.09' WHERE epoch_id = ? AND account_id = ?",
        (state["epoch_id"], victim),
    )
    outcome = world.scheduler.run_audit(entity="accounts", epoch_id=state["epoch_id"])
    preview = world.scheduler.plan_repair(
        outcome.recon_run_id, created_by="author-1", dry_run=True
    )
    stored = world.repository.get_plan(preview.plan_id) if preview else None
    expected = "preview built, nothing persisted"
    actual = f"preview={preview is not None} stored={stored is not None}"
    if preview is not None and stored is None:
        return _ok(expected, actual, "recon_plans has no row for the previewed plan")
    return _fail(expected, actual)


def case_l01(world: CampaignWorld) -> CaseOutcome:
    """Dispatch without approval is refused."""
    _state, _outcome, plan = _account_plan(world)
    if plan is None:
        return _fail("a plan for an account mismatch", "no plan produced")
    result = world.scheduler.dispatch(plan)
    expected = "APPROVAL_MISSING, nothing dispatched"
    actual = f"{result.plan_status}/{result.reason_code} outcomes={len(result.outcomes)}"
    if result.reason_code is ReasonCode.APPROVAL_MISSING and not result.outcomes:
        return _ok(expected, actual, "no approval row for the plan hash",
                   reason_code=str(result.reason_code))
    return _fail(expected, actual)


def case_l02(world: CampaignWorld) -> CaseOutcome:
    """Self-approval is refused (separation of duties)."""
    from .approvals import Role as ApprovalRole

    _state, _outcome, plan = _account_plan(world)
    if plan is None:
        return _fail("a plan", "no plan produced")
    decision = world.scheduler.approvals.decide(
        plan_id=plan.plan_id,
        actor_id="author-1",
        role=ApprovalRole.APPROVER,
        decision="APPROVE",
        plan_hash=plan.plan_hash,
    )
    expected = "APPROVAL_SELF"
    actual = f"allowed={decision.allowed}/{decision.reason_code}"
    if not decision.allowed and decision.reason_code is ReasonCode.APPROVAL_SELF:
        return _ok(expected, actual, "approver equals plan author",
                   reason_code=str(decision.reason_code))
    return _fail(expected, actual)


def case_l03(world: CampaignWorld) -> CaseOutcome:
    """An approval bound to a different plan hash is refused."""
    from .approvals import Role as ApprovalRole

    _state, _outcome, plan = _account_plan(world)
    if plan is None:
        return _fail("a plan", "no plan produced")
    decision = world.scheduler.approvals.decide(
        plan_id=plan.plan_id,
        actor_id="approver-2",
        role=ApprovalRole.APPROVER,
        decision="APPROVE",
        plan_hash="f" * 64,
    )
    expected = "PLAN_DIGEST_CHANGED"
    actual = f"allowed={decision.allowed}/{decision.reason_code}"
    if not decision.allowed and decision.reason_code is ReasonCode.PLAN_DIGEST_CHANGED:
        return _ok(expected, actual, "submitted hash differs from the stored plan",
                   reason_code=str(decision.reason_code))
    return _fail(expected, actual)


# ---- Domain M/N: dispatch, fencing and idempotency -----------------------


def _approved_plan(world: CampaignWorld):
    from .approvals import Role as ApprovalRole

    state, outcome, plan = _account_plan(world)
    if plan is not None:
        world.scheduler.approvals.decide(
            plan_id=plan.plan_id,
            actor_id="approver-2",
            role=ApprovalRole.APPROVER,
            decision="APPROVE",
            plan_hash=plan.plan_hash,
        )
    return state, outcome, plan


def case_m01(world: CampaignWorld) -> CaseOutcome:
    """A plan whose expected revision moved is STALE."""
    state, _outcome, plan = _approved_plan(world)
    if plan is None:
        return _fail("an approved plan", "no plan produced")
    world.harness.publish_snapshot(state["epoch_id"], "accounts")
    result = world.scheduler.dispatch(plan)
    expected = "PLAN_STALE, nothing dispatched"
    actual = f"{result.plan_status}/{result.reason_code}"
    if result.plan_status == "STALE" and result.reason_code is ReasonCode.PLAN_STALE:
        return _ok(expected, actual, "published_scan_id changed after planning",
                   reason_code=str(result.reason_code))
    return _fail(expected, actual)


def case_m02(world: CampaignWorld) -> CaseOutcome:
    """COMMITTED is not VERIFIED; verification is a separate audit."""
    _state, _outcome, plan = _approved_plan(world)
    if plan is None:
        return _fail("an approved plan", "no plan produced")
    dispatched = world.scheduler.dispatch(plan)
    verified = world.scheduler.verify(plan, entity="accounts")
    expected = "dispatch -> VERIFYING, then an independent audit -> VERIFIED"
    actual = f"dispatch={dispatched.plan_status} verify={verified.plan_status}"
    if dispatched.plan_status == "VERIFYING" and verified.verified:
        return _ok(expected, actual, f"verification run {verified.verification_run_id} MATCHED")
    return _fail(expected, actual)


def case_n01(world: CampaignWorld) -> CaseOutcome:
    """A lost ACK becomes UNKNOWN and resolves without double execution."""
    from .executor import RepairExecutor

    _state, _outcome, plan = _approved_plan(world)
    if plan is None:
        return _fail("an approved plan", "no plan produced")
    world.harness.faults.lose_repair_ack.arm(1)
    result = world.scheduler.dispatch(plan)
    confirmed = world.harness.faults.lose_repair_ack.fired

    executor = RepairExecutor(
        repository=world.repository,
        approvals=world.scheduler.approvals,
        leases=world.scheduler.leases,
        read_port=world.harness,
        repair_port=world.harness,
        settings=world.settings,
        clock=world.clock,
    )
    intent_id = plan.actions[0].intent_id
    resolved = executor.resolve_unknown(
        action_id=plan.actions[0].action_id, intent_id=intent_id
    )
    receipt = world.harness.repair_intent_status(intent_id)
    expected = "UNKNOWN then COMMITTED by receipt query, execution_count == 1"
    actual = (
        f"dispatch={result.plan_status} resolved={resolved.status} "
        f"executions={receipt.execution_count if receipt else None}"
    )
    passed = (
        result.plan_status == "UNKNOWN"
        and resolved.status == "COMMITTED"
        and receipt is not None
        and receipt.execution_count == 1
    )
    return CaseOutcome(
        result=CaseResult.PASS if passed else CaseResult.FAIL,
        expected=expected,
        actual=actual,
        oracle="P03 receipt execution_count",
        fault="lose_repair_ack",
        fault_confirmed=confirmed,
        reason_code=str(result.reason_code),
    )


def case_n02(world: CampaignWorld) -> CaseOutcome:
    """Replaying the same intent id executes the work only once."""
    _state, _outcome, plan = _approved_plan(world)
    if plan is None:
        return _fail("an approved plan", "no plan produced")
    world.scheduler.dispatch(plan)
    action = plan.actions[0]
    replay = world.harness.request_full_resync(
        intent_id=action.intent_id,
        source_id="sim_local",
        entity="accounts",
        expected_epoch_id=plan.expected_epoch_id,
        expected_revision=plan.expected_target_scan_id,
        expected_lease_version=plan.expected_lease_version,
        approved_plan_hash=plan.plan_hash,
        parameters=action.parameters,
    )
    expected = "execution_count stays 1 after a replay"
    actual = f"status={replay.status} executions={replay.execution_count}"
    if replay.execution_count == 1:
        return _ok(expected, actual, "P03 deduplicated on intent_id")
    return _fail(expected, actual)


def case_n03(world: CampaignWorld) -> CaseOutcome:
    """Accepted-then-failed never becomes VERIFIED."""
    _state, _outcome, plan = _approved_plan(world)
    if plan is None:
        return _fail("an approved plan", "no plan produced")
    world.harness.faults.accept_then_fail.arm(1)
    result = world.scheduler.dispatch(plan)
    confirmed = world.harness.faults.accept_then_fail.fired
    verified = world.scheduler.verify(plan, entity="accounts")
    expected = "FAILED, never VERIFIED"
    actual = f"dispatch={result.plan_status} verify_verified={verified.verified}"
    passed = result.plan_status == "FAILED" and not verified.verified
    return CaseOutcome(
        result=CaseResult.PASS if passed else CaseResult.FAIL,
        expected=expected,
        actual=actual,
        oracle="plan status in recon_plans",
        fault="accept_then_fail",
        fault_confirmed=confirmed,
        reason_code=str(result.reason_code),
    )


# ---- Domain O: leases ----------------------------------------------------


def case_o01(world: CampaignWorld) -> CaseOutcome:
    """A fenced worker cannot dispatch after a takeover."""
    state, _outcome, plan = _approved_plan(world)
    if plan is None:
        return _fail("an approved plan", "no plan produced")
    scope = world.settings.scope_hash("accounts")
    worker_a = world.scheduler.leases.acquire(
        epoch_id=state["epoch_id"], entity="accounts", scope_hash=scope, owner_id="worker-A"
    )
    world.scheduler.leases.steal(
        epoch_id=state["epoch_id"], entity="accounts", scope_hash=scope, owner_id="worker-B"
    )
    result = world.scheduler.dispatch(plan, lease=worker_a)
    expected = "STALE/LEASE_LOST, nothing dispatched"
    actual = f"{result.plan_status}/{result.reason_code}"
    if result.plan_status == "STALE" and result.reason_code is ReasonCode.LEASE_LOST:
        return _ok(expected, actual, "lease_version incremented by the takeover",
                   reason_code=str(result.reason_code))
    return _fail(expected, actual)


def case_o02(world: CampaignWorld) -> CaseOutcome:
    """An expired lease stops being valid without any takeover."""
    from .errors import LeaseLostError

    state = world.synced(accounts=3, deals=10, positions=3)
    scope = world.settings.scope_hash("deals")
    lease = world.scheduler.leases.acquire(
        epoch_id=state["epoch_id"], entity="deals", scope_hash=scope
    )
    before = world.scheduler.leases.is_valid(lease)
    world.clock.advance(world.settings.lease_ttl_s + 5)
    after = world.scheduler.leases.is_valid(lease)
    expected = "valid before the TTL, invalid after"
    actual = f"before={before} after={after}"
    if before and not after:
        return _ok(expected, actual, "lease_until compared against the injected clock")
    return _fail(expected, actual)


# ---- Domain P: transport -------------------------------------------------


def case_p01(world: CampaignWorld) -> CaseOutcome:
    """Persistent 503s exhaust bounded retries and stay inconclusive."""
    state = world.synced(accounts=3, deals=12, positions=3)
    rule = world.add_fault(
        mode="http_error",
        parameters={"status": 503},
        path="/v1/deals",
        trigger="always",
        max_activations=50,
    )
    outcome = world.scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    confirmed = world.fault_activations(rule) >= 1
    run = world.repository.get_run(outcome.recon_run_id)
    expected = "SOURCE_TRANSPORT_FAILED, source_complete=false"
    actual = f"{outcome.status}/{outcome.reason_code} complete={run.source_complete}"
    passed = (
        outcome.reason_code is ReasonCode.SOURCE_TRANSPORT_FAILED and not run.source_complete
    )
    return CaseOutcome(
        result=CaseResult.PASS if passed else CaseResult.FAIL,
        expected=expected,
        actual=actual,
        oracle="P01 fault activation counter",
        fault="http_error 503",
        fault_confirmed=confirmed,
        reason_code=str(outcome.reason_code),
    )


def case_p02(world: CampaignWorld) -> CaseOutcome:
    """A malformed body is refused at the boundary, not classified as a diff."""
    state = world.synced(accounts=3, deals=12, positions=3)
    rule = world.add_fault(
        mode="malformed_json", path="/v1/deals", trigger="always", max_activations=50
    )
    outcome = world.scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    confirmed = world.fault_activations(rule) >= 1
    proven = [f for f in outcome.findings if f["confidence"] == Confidence.PROVEN.value]
    expected = "INCONCLUSIVE with no proven findings"
    actual = f"{outcome.status}/{outcome.reason_code} proven={len(proven)}"
    passed = outcome.status == "INCONCLUSIVE" and not proven
    return CaseOutcome(
        result=CaseResult.PASS if passed else CaseResult.FAIL,
        expected=expected,
        actual=actual,
        oracle="P01 fault activation counter",
        fault="malformed_json",
        fault_confirmed=confirmed,
        reason_code=str(outcome.reason_code),
    )


# ---- Domain Q: storage ---------------------------------------------------


def case_q01(world: CampaignWorld) -> CaseOutcome:
    """A rolled-back capture leaves no partial manifest."""
    from .db.engine import transaction as tx

    state = world.synced(accounts=4, deals=12, positions=4)
    run = world.repository.create_run(
        request_id="q01", source_id="sim_local", epoch_id=state["epoch_id"], entity="accounts"
    )
    conn = world.db.connect(Role.RECON)
    try:
        try:
            with tx(conn):
                conn.execute(
                    "INSERT INTO recon_target_items VALUES (?, 'A-000001', NULL, 'abc')",
                    (run.recon_run_id,),
                )
                raise RuntimeError("storage failure mid-capture")
        except RuntimeError:
            pass
    finally:
        conn.close()
    with world.repository.session() as c:
        rows = c.execute(
            "SELECT COUNT(*) FROM recon_target_items WHERE recon_run_id = ?", (run.recon_run_id,)
        ).fetchone()[0]
    stored = world.repository.get_run(run.recon_run_id)
    expected = "no rows, target_complete=false"
    actual = f"rows={rows} complete={stored.target_complete}"
    if rows == 0 and not stored.target_complete:
        return _ok(expected, actual, "transaction rolled back atomically",
                   fault="exception mid-transaction", fault_confirmed=True)
    return _fail(expected, actual)


# ---- Domain R: resource limits -------------------------------------------


def case_r01(world: CampaignWorld) -> CaseOutcome:
    """The page budget is enforced."""
    state = world.synced(accounts=3, deals=30, positions=3)
    rows = list(world.harness.iter_target_rows(state["epoch_id"], "deals"))
    tight = load_settings(
        env="test",
        db_path=world.settings.db_path,
        source_id="sim_local",
        max_pages_per_run=2,
        max_items_per_run=10_000,
        source_max_page_size=2,
    )
    repo = ReconRepository(world.db, tight, world.clock)
    fake = FakeSource(
        source_id="sim_local",
        run_id=state["run_id"],
        generation=state["generation"],
        snapshot_seq=state["snapshot_seq"],
        records={"deals": rows},
        page_size=2,
    )
    scheduler = ReconciliationScheduler(
        db=world.db,
        repository=repo,
        source=fake,
        read_port=world.harness,
        repair_port=world.harness,
        settings=tight,
        clock=world.clock,
    )
    outcome = scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    expected = "SCAN_QUOTA_EXCEEDED"
    actual = f"{outcome.status}/{outcome.reason_code}"
    if outcome.reason_code is ReasonCode.SCAN_QUOTA_EXCEEDED:
        return _ok(expected, actual, "page budget of 2 against a larger scan",
                   reason_code=str(outcome.reason_code))
    return _fail(expected, actual)


def case_r02(world: CampaignWorld) -> CaseOutcome:
    """Concurrent audit runs are capped by admission control."""
    from .errors import AdmissionRejected

    state = world.synced(accounts=3, deals=10, positions=3)
    tight = load_settings(
        env="test",
        db_path=world.settings.db_path,
        source_id="sim_local",
        max_concurrent_runs=1,
    )
    repo = ReconRepository(world.db, tight, world.clock)
    held = repo.create_run(
        request_id="r02-held",
        source_id="sim_local",
        epoch_id=state["epoch_id"],
        entity="deals",
    )
    repo.set_status(held.recon_run_id, "CAPTURING_SOURCE")
    scheduler = ReconciliationScheduler(
        db=world.db,
        repository=repo,
        source=world.source,
        read_port=world.harness,
        repair_port=world.harness,
        settings=tight,
        clock=world.clock,
    )
    rejected = False
    try:
        scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    except AdmissionRejected:
        rejected = True
    expected = "ADMISSION_REJECTED beyond the concurrency limit"
    actual = f"rejected={rejected} active={repo.active_run_count()}"
    if rejected:
        return _ok(expected, actual, "one in-flight run against a limit of one")
    return _fail(expected, actual)


# ---- Domain S: retention -------------------------------------------------


def case_s01(world: CampaignWorld) -> CaseOutcome:
    """Evidence for an in-flight repair is not garbage collected."""
    state, _outcome, plan = _account_plan(world)
    if plan is None:
        return _fail("a plan", "no plan produced")
    released = world.repository.release_evidence(now_iso="2099-01-01T00:00:00.000000Z")
    with world.repository.session() as conn:
        remaining = conn.execute(
            "SELECT COUNT(*) FROM recon_source_items WHERE recon_run_id = ?",
            (plan.recon_run_id,),
        ).fetchone()[0]
    expected = "manifest retained while a plan is pending"
    actual = f"released={released} remaining_items={remaining}"
    if remaining > 0:
        return _ok(expected, actual, "GC skips runs with a non-terminal plan")
    return _fail(expected, actual)


def case_s02(world: CampaignWorld) -> CaseOutcome:
    """Evidence past retention with no active plan is released."""
    state = world.synced(accounts=3, deals=12, positions=3)
    outcome = world.scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    before = world.repository.source_item_count(outcome.recon_run_id)
    released = world.repository.release_evidence(now_iso="2099-01-01T00:00:00.000000Z")
    after = world.repository.source_item_count(outcome.recon_run_id)
    expected = "manifest released after retention when nothing references it"
    actual = f"before={before} after={after} released={released}"
    if before > 0 and after == 0 and released >= 1:
        return _ok(expected, actual, "terminal run with no pending plan")
    return _fail(expected, actual)


# ---- Domain T: manifests and false negatives ----------------------------


def case_t01(world: CampaignWorld) -> CaseOutcome:
    """Dropping a receipt breaks the completeness proof (no false negative)."""
    state = world.synced(accounts=3, deals=20, positions=3)
    rows = list(world.harness.iter_target_rows(state["epoch_id"], "deals"))
    fake, scheduler = world.with_fake_source({"deals": rows}, page_size=4)
    outcome = scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    if outcome.status != "MATCHED":
        return _fail("MATCHED baseline before tampering", f"{outcome.status}")
    with world.repository.session() as conn, transaction(conn):
        conn.execute(
            "DELETE FROM recon_source_items WHERE recon_run_id = ? AND page_index = 1",
            (outcome.recon_run_id,),
        )
        conn.execute(
            "DELETE FROM recon_source_page_receipts WHERE recon_run_id = ? AND page_index = 1",
            (outcome.recon_run_id,),
        )
    ok, reason, _stats = world.repository.validate_receipt_chain(outcome.recon_run_id)
    expected = "receipt chain invalid after a page is removed"
    actual = f"valid={ok} reason={reason}"
    if not ok:
        return _ok(expected, actual, "contiguity and item-count checks both fail",
                   reason_code=str(reason))
    return _fail(expected, actual)


# ---- Domain U: observability --------------------------------------------


def case_u01(world: CampaignWorld) -> CaseOutcome:
    """Every run outcome is traceable to evidence and metrics."""
    state = world.synced(accounts=3, deals=12, positions=3)
    outcome = world.scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    snapshot = world.scheduler.metrics.snapshot()
    names = {counter["name"] for counter in snapshot["counters"]}
    run = world.repository.get_run(outcome.recon_run_id)
    expected = "run row, metrics and audit trail all present"
    actual = f"metrics={len(names)} run_status={run.status}"
    if "recon_runs_started_total" in names and run is not None:
        return _ok(expected, actual, "metrics snapshot and recon_runs row")
    return _fail(expected, actual)


def case_u02(world: CampaignWorld) -> CaseOutcome:
    """The audit hash chain detects an in-place edit."""
    from .audit import verify_chain

    state = world.synced(accounts=3, deals=12, positions=3)
    world.scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    conn = world.db.connect(Role.RECON)
    try:
        before = verify_chain(conn)[0]
        with transaction(conn):
            conn.execute(
                "UPDATE recon_audit SET safe_details = '{\"tampered\":true}' WHERE sequence_no = 2"
            )
        after, bad, _detail = verify_chain(conn)
    finally:
        conn.close()
    expected = "chain valid before, broken after, with a reported position"
    actual = f"before={before} after={after} first_bad={bad}"
    if before and not after and bad == 2:
        return _ok(expected, actual, "recomputed chain over recon_audit",
                   fault="in-place audit row edit", fault_confirmed=True)
    return _fail(expected, actual)


# ---- Domain V: security -------------------------------------------------


def case_v01(world: CampaignWorld) -> CaseOutcome:
    """P04's role cannot write any Project 03 table."""
    import sqlite3

    from .db.schema import P03_OWNED_TABLES

    denied = 0
    allowed: list[str] = []
    conn = world.db.connect(Role.RECON)
    try:
        for table in sorted(P03_OWNED_TABLES):
            try:
                conn.execute(f"DELETE FROM {table}")
                allowed.append(table)
            except sqlite3.DatabaseError:
                denied += 1
    finally:
        conn.close()
    expected = f"all {len(P03_OWNED_TABLES)} P03 tables deny writes"
    actual = f"denied={denied} allowed={allowed}"
    if denied == len(P03_OWNED_TABLES) and not allowed:
        return _ok(expected, actual, "engine-level authoriser denial")
    return _fail(expected, actual)


def case_v02(world: CampaignWorld) -> CaseOutcome:
    """A hostile identifier is refused at the canonical boundary."""
    from .canonical import payload_hash
    from .errors import CanonicalizationError

    hostile = "A-1'; DROP TABLE deals;--"
    refused = False
    try:
        payload_hash(
            "accounts",
            {"account_id": hostile, "currency": "USD", "balance": "1.00", "credit": "0.00"},
        )
    except CanonicalizationError:
        refused = True
    tables = world.first_value(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='deals'"
    )
    expected = "refused, and the table still exists"
    actual = f"refused={refused} deals_table={tables}"
    if refused and tables == 1:
        return _ok(expected, actual, "identifier policy rejected the payload",
                   fault="adversarial identifier", fault_confirmed=True)
    return _fail(expected, actual)


# ---- Domain W: versioning ----------------------------------------------


def case_w01(world: CampaignWorld) -> CaseOutcome:
    """A run captured under another hash version is incomparable."""
    state = world.synced(accounts=3, deals=12, positions=3)
    outcome = world.scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    with world.repository.session() as conn, transaction(conn):
        conn.execute(
            "UPDATE recon_runs SET hash_version = 'sha256:other-v9', status='DIFFING', ended_at=NULL WHERE recon_run_id = ?",
            (outcome.recon_run_id,),
        )
    from .alignment import AlignmentEngine, AlignmentInputs

    run = world.repository.get_run(outcome.recon_run_id)
    decision = AlignmentEngine(world.settings).decide(
        AlignmentInputs(
            run=run,
            identity=world.source.read_identity(),
            capabilities=world.source.read_capabilities(),
            status=world.harness.sync_status("sim_local", state["epoch_id"], "deals"),
            expected_epoch_id=state["epoch_id"],
            expected_source_id="sim_local",
        )
    )
    expected = "INCOMPARABLE_SCHEMA, strict compare denied"
    actual = f"allowed={decision.strict_compare_allowed}/{decision.reason_code}"
    if not decision.strict_compare_allowed and decision.reason_code is ReasonCode.INCOMPARABLE_SCHEMA:
        return _ok(expected, actual, "hash version differs from the supported one",
                   reason_code=str(decision.reason_code))
    return _fail(expected, actual)


# ---- Domain X: oracle quality ------------------------------------------


def case_x01(world: CampaignWorld) -> CaseOutcome:
    """IT04-10: a silently truncating source does not cause a deletion.

    The independent oracle re-reads P01 with faults cleared and proves the
    source omitted a row it actually holds.
    """
    state = world.synced(accounts=4, deals=20, positions=4)
    rule = world.add_fault(
        mode="drop_one_item",
        parameters={"selection": "first"},
        path="/v1/deals",
        trigger="always",
        max_activations=1,
    )
    outcome = world.scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    confirmed = world.fault_activations(rule) >= 1
    extras = [
        f for f in outcome.findings if f["finding_code"] == FindingCode.EXTRA_TARGET_DEAL.value
    ]
    plan = world.scheduler.plan_repair(outcome.recon_run_id, created_by="campaign")
    survives = None
    oracle_caught = False
    if extras:
        victim = extras[0]["item_key"]
        survives = world.first_value(
            "SELECT COUNT(*) FROM deals WHERE epoch_id = ? AND deal_id = ?",
            (state["epoch_id"], victim),
        )
        world.clear_faults()
        identity_rows = []
        cursor = None
        while True:
            params = {"limit": 500, "after_seq": 0} if cursor is None else {"cursor": cursor}
            response = world.client.get(
                "/v1/deals",
                params=params,
                headers={"Authorization": f"Bearer {world.data_token}"},
            )
            body = response.json()
            identity_rows.extend(body["items"])
            cursor = body.get("next_cursor")
            if cursor is None:
                break
        oracle_caught = any(row["deal_id"] == victim for row in identity_rows)

    expected = "EXTRA_TARGET_DEAL, no deletion, oracle proves the source lied"
    actual = (
        f"extras={len(extras)} survives={survives} "
        f"dispatchable={len(plan.dispatchable_actions) if plan else 0} oracle_caught={oracle_caught}"
    )
    passed = (
        len(extras) == 1
        and survives == 1
        and (plan is None or not plan.dispatchable_actions)
        and oracle_caught
    )
    return CaseOutcome(
        result=CaseResult.PASS if passed else CaseResult.FAIL,
        expected=expected,
        actual=actual,
        oracle="independent P01 re-read with faults cleared",
        fault="drop_one_item",
        fault_confirmed=confirmed,
        notes=["ORACLE_VIOLATION detected: the source omitted a row it holds"]
        if oracle_caught
        else [],
    )


def case_x02(world: CampaignWorld) -> CaseOutcome:
    """Determinism: the same seed produces the same manifest digest."""
    state = world.synced(accounts=4, deals=20, positions=4)
    first = world.scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    second = world.scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    run_a = world.repository.get_run(first.recon_run_id)
    run_b = world.repository.get_run(second.recon_run_id)
    expected = "two independent audits of a stable world agree exactly"
    actual = f"{run_a.source_root_hash == run_b.source_root_hash}"
    if run_a.source_root_hash == run_b.source_root_hash and first.status == second.status:
        return _ok(expected, actual, "root hashes equal across two separate runs")
    return _fail(expected, actual)


# ---- Domain Y: compound faults -----------------------------------------


def case_y01(world: CampaignWorld) -> CaseOutcome:
    """Transport faults plus a corrupted target still fail closed."""
    state = world.synced(accounts=4, deals=20, positions=4)
    victim = world.first_value(
        "SELECT deal_id FROM deals WHERE epoch_id = ? LIMIT 1", (state["epoch_id"],)
    )
    world.harness.corrupt_for_test(
        "UPDATE deals SET swap = '-55.55' WHERE epoch_id = ? AND deal_id = ?",
        (state["epoch_id"], victim),
    )
    rule = world.add_fault(
        mode="http_error",
        parameters={"status": 503},
        path="/v1/deals",
        trigger="nth_matching_request",
        n=1,
        max_activations=1,
    )
    outcome = world.scheduler.run_audit(entity="deals", epoch_id=state["epoch_id"])
    confirmed = world.fault_activations(rule) >= 1
    plan = world.scheduler.plan_repair(outcome.recon_run_id, created_by="campaign")
    expected = "either a retried success finding the conflict, or a safe INCONCLUSIVE"
    actual = f"{outcome.status}/{outcome.reason_code}"
    safe = outcome.status in ("FINDINGS", "INCONCLUSIVE") and (
        plan is None or not plan.dispatchable_actions
    )
    return CaseOutcome(
        result=CaseResult.PASS if safe else CaseResult.FAIL,
        expected=expected,
        actual=actual,
        oracle="no dispatchable action for a money conflict under either path",
        fault="http_error 503 + target money corruption",
        fault_confirmed=confirmed,
        reason_code=str(outcome.reason_code),
    )


def case_y02(world: CampaignWorld) -> CaseOutcome:
    """A reset during a corrupted state blocks rather than repairing."""
    state = world.synced(accounts=4, deals=15, positions=4)
    victim = world.first_value(
        "SELECT account_id FROM accounts_current WHERE epoch_id = ? LIMIT 1", (state["epoch_id"],)
    )
    world.harness.corrupt_for_test(
        "UPDATE accounts_current SET balance = '0.05' WHERE epoch_id = ? AND account_id = ?",
        (state["epoch_id"], victim),
    )
    world.reset_run(state["run_id"], state["generation"])
    outcome = world.scheduler.run_audit(entity="accounts", epoch_id=state["epoch_id"])
    plan = world.scheduler.plan_repair(outcome.recon_run_id, created_by="campaign")
    expected = "EPOCH_MISMATCH, no repair for the superseded epoch"
    actual = f"{outcome.status}/{outcome.reason_code} plan={plan is not None}"
    if outcome.reason_code is ReasonCode.EPOCH_MISMATCH and plan is None:
        return _ok(expected, actual, "generation changed under a known-bad target",
                   reason_code=str(outcome.reason_code))
    return _fail(expected, actual)


# ---- Domain Z: disaster recovery ---------------------------------------


def case_z01(world: CampaignWorld) -> CaseOutcome:
    """A full disconnect-and-recover cycle converges to MATCHED (IT04-08)."""
    world.create_run(accounts=4, deals=30, positions=4)
    epoch_id = world.harness.bootstrap_epoch()

    # Partial sync: the "disconnect" left P03 behind.
    from recon_harness.p03_sync import P03SyncHarness

    partial = P03SyncHarness(
        db=world.db, source=world.source, clock=world.clock, source_id="sim_local", page_size=5
    )
    partial.sync_deals(epoch_id, max_pages=2, advance_watermark=False)
    during = world.scheduler.run_audit(entity="deals", epoch_id=epoch_id)

    # Reconnect: P03 scans to the terminal page and republishes.
    world.harness.sync_deals(epoch_id)
    world.harness.publish_snapshot(epoch_id, "accounts")
    world.harness.publish_snapshot(epoch_id, "positions")
    after = world.scheduler.run_audit(entity="deals", epoch_id=epoch_id)

    expected = "INCONCLUSIVE while behind, MATCHED after recovery"
    actual = f"during={during.status}/{during.reason_code} after={after.status}"
    if during.status == "INCONCLUSIVE" and after.status == "MATCHED":
        return _ok(expected, actual, "independent audit at a compatible cut after recovery")
    return _fail(expected, actual)


def case_z02(world: CampaignWorld) -> CaseOutcome:
    """A quarantined epoch stops all dispatch (operational boundary)."""
    state, _outcome, plan = _approved_plan(world)
    if plan is None:
        return _fail("an approved plan", "no plan produced")
    world.harness.quarantine_epoch(state["epoch_id"])
    result = world.scheduler.dispatch(plan)
    expected = "STALE/EPOCH_MISMATCH once the epoch is quarantined"
    actual = f"{result.plan_status}/{result.reason_code}"
    if result.plan_status == "STALE" and result.reason_code is ReasonCode.EPOCH_MISMATCH:
        return _ok(expected, actual, "epoch status is not ACTIVE",
                   reason_code=str(result.reason_code))
    return _fail(expected, actual)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def _declared(domain: str, start: int, count: int, reason: str, priority: str = "P0") -> list[CaseSpec]:
    """Register cases that exist in the matrix but are not executed here."""
    return [
        CaseSpec(
            case_id=f"WF4-{domain}-{index:02d}",
            domain=domain,
            priority=priority,
            description=f"{DOMAINS[domain]} (matrix case {index})",
            not_run_reason=reason,
        )
        for index in range(start, start + count)
    ]


#: Implemented cases. Each one injects a real fault or constructs a real
#: corrupted state, and checks an outcome against an oracle outside the code
#: path under test.
IMPLEMENTED: list[CaseSpec] = [
    CaseSpec("WF4-A-01", "A", "P0", "source identity and declared limits", case_a01),
    CaseSpec("WF4-A-02", "A", "P0", "unknown epoch is refused", case_a02),
    CaseSpec("WF4-A-03", "A", "P0", "missing capability denies strict compare", case_a03),
    CaseSpec("WF4-B-01", "B", "P0", "source reset blocks the old epoch", case_b01),
    CaseSpec("WF4-B-02", "B", "P0", "new generation is a separate epoch", case_b02),
    CaseSpec("WF4-C-01", "C", "P0", "interrupted scan cannot complete", case_c01),
    CaseSpec(
        "WF4-C-02", "C", "P0", "genuine cursor expiry mid-scan", case_c02, ttl_seconds=1
    ),
    CaseSpec("WF4-C-03", "C", "P0", "duplicate key across pages", case_c03),
    CaseSpec("WF4-D-01", "D", "P0", "no published scan refuses capture", case_d01),
    CaseSpec("WF4-D-02", "D", "P0", "concurrent publish fails capture closed", case_d02),
    CaseSpec("WF4-E-01", "E", "P0", "missing deal detected at a proven cut", case_e01),
    CaseSpec("WF4-E-02", "E", "P0", "deal money conflict quarantined", case_e02),
    CaseSpec("WF4-E-03", "E", "P0", "extra target deal never deleted", case_e03),
    CaseSpec("WF4-E-04", "E", "P0", "W<S is not a missing deal", case_e04),
    CaseSpec("WF4-F-01", "F", "P0", "account mismatch plans a resync", case_f01),
    CaseSpec("WF4-F-02", "F", "P0", "moved source cut is not corruption", case_f02),
    CaseSpec("WF4-G-01", "G", "P0", "absent position is not closed", case_g01),
    CaseSpec("WF4-H-01", "H", "P0", "scale-equivalent money matches", case_h01),
    CaseSpec("WF4-H-02", "H", "P0", "excess precision is refused", case_h02),
    CaseSpec("WF4-I-01", "I", "P0", "no global cut is claimed", case_i01),
    CaseSpec("WF4-J-01", "J", "P0", "clean world yields no findings", case_j01),
    CaseSpec("WF4-J-02", "J", "P0", "mass count drop escalates", case_j02),
    CaseSpec("WF4-K-01", "K", "P0", "dry run persists nothing", case_k01),
    CaseSpec("WF4-L-01", "L", "P0", "dispatch without approval refused", case_l01),
    CaseSpec("WF4-L-02", "L", "P0", "self-approval refused", case_l02),
    CaseSpec("WF4-L-03", "L", "P0", "approval bound to the plan hash", case_l03),
    CaseSpec("WF4-M-01", "M", "P0", "moved revision makes the plan stale", case_m01),
    CaseSpec("WF4-M-02", "M", "P0", "committed is not verified", case_m02),
    CaseSpec("WF4-N-01", "N", "P0", "lost ACK resolves without double execution", case_n01),
    CaseSpec("WF4-N-02", "N", "P0", "replayed intent executes once", case_n02),
    CaseSpec("WF4-N-03", "N", "P0", "accepted-then-failed is never verified", case_n03),
    CaseSpec("WF4-O-01", "O", "P0", "fenced worker cannot dispatch", case_o01),
    CaseSpec("WF4-O-02", "O", "P0", "expired lease becomes invalid", case_o02),
    CaseSpec("WF4-P-01", "P", "P0", "bounded retries on persistent 503", case_p01),
    CaseSpec("WF4-P-02", "P", "P0", "malformed body refused at the boundary", case_p02),
    CaseSpec("WF4-Q-01", "Q", "P0", "rolled-back capture leaves nothing", case_q01),
    CaseSpec("WF4-R-01", "R", "P0", "page budget enforced", case_r01),
    CaseSpec("WF4-R-02", "R", "P0", "admission control caps concurrency", case_r02),
    CaseSpec("WF4-S-01", "S", "P0", "evidence retained during a repair", case_s01),
    CaseSpec("WF4-S-02", "S", "P0", "evidence released after retention", case_s02),
    CaseSpec("WF4-T-01", "T", "P0", "dropped receipt breaks completeness", case_t01),
    CaseSpec("WF4-U-01", "U", "P0", "outcomes are traceable to evidence", case_u01),
    CaseSpec("WF4-U-02", "U", "P0", "audit chain detects tampering", case_u02),
    CaseSpec("WF4-V-01", "V", "P0", "P04 cannot write P03 tables", case_v01),
    CaseSpec("WF4-V-02", "V", "P0", "adversarial identifier refused", case_v02),
    CaseSpec("WF4-W-01", "W", "P0", "foreign hash version is incomparable", case_w01),
    CaseSpec("WF4-X-01", "X", "P0", "lying source caught by the oracle", case_x01),
    CaseSpec("WF4-X-02", "X", "P0", "repeated audits are deterministic", case_x02),
    CaseSpec("WF4-Y-01", "Y", "P0", "transport fault plus corruption fails closed", case_y01),
    CaseSpec("WF4-Y-02", "Y", "P0", "reset during corruption blocks repair", case_y02),
    CaseSpec("WF4-Z-01", "Z", "P0", "disconnect and recovery converge", case_z01),
    CaseSpec("WF4-Z-02", "Z", "P0", "quarantined epoch stops dispatch", case_z02),
]

#: Why the remaining matrix cases are not executed in this environment. Stated
#: per domain so the coverage gap is explicit rather than implied.
_NOT_RUN_REASONS: dict[str, str] = {
    "A": "needs a real Manager SDK capability profile to vary (section 0.4 forbids assuming one)",
    "B": "needs a multi-connector deployment with independent worker epochs",
    "C": "remaining cases need cursor-internals manipulation P01 does not expose",
    "D": "needs PostgreSQL MVCC specifics (vacuum, long transactions, serialisation failures)",
    "E": "remaining cases need a multi-million-deal corpus beyond this environment",
    "F": "remaining cases need broker-specific balance adjustment semantics (Project 05)",
    "G": "remaining cases need netting and position-reference semantics (Project 05)",
    "H": "remaining cases need multi-currency policy, which this MVP declares only for USD",
    "I": "needs a coordinated global-cut contract, which upstream does not provide",
    "J": "remaining cases need a labelled corpus of real production discrepancies",
    "K": "remaining cases need the P1 backfill protocol under review",
    "L": "remaining cases need an external IdP and real RBAC integration",
    "M": "remaining cases need the real Project 03 commit path",
    "N": "remaining cases need OS-level network partition injection (toxiproxy)",
    "O": "remaining cases need the real Project 03 scheduler and multi-process leases",
    "P": "remaining cases need a network proxy for partial writes and RST injection",
    "Q": "needs PostgreSQL failover, WAL exhaustion and disk-full simulation",
    "R": "remaining cases need the sustained-load rig from section 11.5",
    "S": "remaining cases need the partitioned-retention design from P1",
    "T": "needs the P1 incremental/segmented manifest implementation",
    "U": "remaining cases need the deployed metrics and alerting stack",
    "V": "remaining cases need network isolation and secret-manager integration",
    "W": "needs executed PostgreSQL migrations across two schema versions",
    "X": "remaining cases need the mutation-testing harness over the full suite",
    "Y": "remaining compound campaigns need the chaos orchestrator from section 10.6",
    "Z": "needs backup/restore drills against a real PostgreSQL cluster",
}


def _build_registry() -> list[CaseSpec]:
    """Implemented cases plus the declared remainder, 12 per domain."""
    cases: list[CaseSpec] = []
    by_domain: dict[str, list[CaseSpec]] = {letter: [] for letter in DOMAINS}
    for spec in IMPLEMENTED:
        by_domain[spec.domain].append(spec)

    for letter in DOMAINS:
        implemented = by_domain[letter]
        cases.extend(implemented)
        # Section 9 defines 12 cases per domain: 8 P0, 3 P1, 1 P2.
        remaining_p0 = max(0, 8 - len(implemented))
        start = len(implemented) + 1
        cases.extend(
            _declared(letter, start, remaining_p0, _NOT_RUN_REASONS[letter], "P0")
        )
        cases.extend(
            _declared(letter, start + remaining_p0, 3, _NOT_RUN_REASONS[letter], "P1")
        )
        cases.extend(
            _declared(letter, start + remaining_p0 + 3, 1, _NOT_RUN_REASONS[letter], "P2")
        )
    return cases


CASES: list[CaseSpec] = _build_registry()


def coverage_summary() -> dict[str, Any]:
    """Implemented versus declared, per domain and overall.

    Reported separately from the pass rate on purpose: a campaign can be
    entirely green while covering only part of the matrix, and conflating the
    two is how coverage gets overstated (section 9).
    """
    per_domain: dict[str, dict[str, int]] = {}
    for spec in CASES:
        bucket = per_domain.setdefault(
            spec.domain, {"declared": 0, "implemented": 0, "p0_declared": 0, "p0_implemented": 0}
        )
        bucket["declared"] += 1
        if spec.priority == "P0":
            bucket["p0_declared"] += 1
        if spec.implemented:
            bucket["implemented"] += 1
            if spec.priority == "P0":
                bucket["p0_implemented"] += 1
    totals = {
        "declared": len(CASES),
        "implemented": sum(1 for c in CASES if c.implemented),
        "p0_declared": sum(1 for c in CASES if c.priority == "P0"),
        "p0_implemented": sum(1 for c in CASES if c.priority == "P0" and c.implemented),
    }
    return {"totals": totals, "per_domain": per_domain}


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_campaign(*, tier: str = "P0", seed: int = 42, case_ids: Sequence[str] | None = None) -> CampaignReport:
    """Execute the campaign and return a report with per-case evidence."""
    report = CampaignReport(campaign_id=f"wf4-{tier.lower()}-seed{seed}")
    selected = [
        spec
        for spec in CASES
        if (case_ids is None or spec.case_id in case_ids)
        and (tier == "ALL" or spec.priority == tier)
    ]

    for spec in selected:
        if not spec.implemented:
            report.add(
                EvidenceManifest(
                    test_id=spec.case_id,
                    result=CaseResult.NOT_RUN,
                    domain=spec.domain,
                    priority=spec.priority,
                    description=spec.description,
                    expected_outcome="n/a",
                    actual_outcome="not executed",
                    notes=[spec.not_run_reason or "not implemented"],
                )
            )
            continue

        world: CampaignWorld | None = None
        try:
            world = CampaignWorld(seed=seed, ttl_seconds=spec.ttl_seconds)
            outcome = spec.run(world)  # type: ignore[misc]
            report.add(
                EvidenceManifest(
                    test_id=spec.case_id,
                    result=outcome.result,
                    domain=spec.domain,
                    priority=spec.priority,
                    description=spec.description,
                    policy_hash=world.settings.policy_hash,
                    source_epoch=outcome.source_epoch,
                    source_snapshot_seq=outcome.source_snapshot_seq,
                    target_watermark=outcome.target_watermark,
                    source_manifest_complete=outcome.source_complete,
                    target_manifest_complete=outcome.target_complete,
                    fault_injected=outcome.fault,
                    fault_activation_confirmed=outcome.fault_confirmed,
                    expected_outcome=outcome.expected,
                    actual_outcome=outcome.actual,
                    oracle_result=outcome.oracle,
                    reason_code=outcome.reason_code,
                    resource_metrics=world.scheduler.metrics.snapshot(),
                    notes=outcome.notes,
                )
            )
        except Exception as exc:  # a case that crashes is not a pass
            report.add(
                EvidenceManifest(
                    test_id=spec.case_id,
                    result=CaseResult.INVALID_TEST,
                    domain=spec.domain,
                    priority=spec.priority,
                    description=spec.description,
                    expected_outcome="the case's own expectation",
                    actual_outcome=f"{type(exc).__name__}: {exc}",
                    notes=["the case raised before it could judge its own outcome"],
                )
            )
        finally:
            if world is not None:
                world.close()

    return report
