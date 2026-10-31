"""Pydantic request and response models.

The one rule that governs every model here: **a money, price or volume value
crosses this boundary as a JSON string, never as a JSON number** (ADR-005).
A JSON number is an IEEE 754 double in most clients, and ``0.1`` is not
``0.1`` once it has been through one. The serializer below emits plain
notation with no exponent and no negative zero, so two equal amounts are also
equal as strings - which is what every golden fixture and every digest
depends on.

On input, a float is *rejected* rather than coerced. Accepting ``1.1`` as a
price would silently import the client's rounding error into stored data, and
the resulting discrepancy would surface much later as a cent that does not
reconcile.

Every list response carries ``run_id``, ``generation`` and ``snapshot_seq`` so
a consumer can detect that it is reading a different run than the one it
started with, rather than quietly merging two generations (WF-038, INV-15).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, PlainSerializer

from .. import GENERATOR_VERSION
from ..domain import accounting as acc
from ..domain.models import DEFAULT_SYMBOLS


def _parse_decimal_field(value: Any) -> Decimal:
    """Accept a decimal string or integer; reject float, NaN and Infinity."""
    return acc.parse_decimal(value, field="decimal")


#: Decimal on the wire: parsed strictly, serialized as a canonical string.
DecimalStr = Annotated[
    Decimal,
    BeforeValidator(_parse_decimal_field),
    PlainSerializer(acc.format_decimal, return_type=str, when_used="always"),
]


class StrictModel(BaseModel):
    """Rejects unknown fields.

    Silently ignoring an unexpected field would let a caller believe an option
    took effect when it was discarded.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)


# ---------------------------------------------------------------------------
# Data plane responses
# ---------------------------------------------------------------------------


class CapabilitiesOut(BaseModel):
    """What this build actually supports.

    Published explicitly so Project 02 negotiates against a declared contract
    instead of probing for behaviour - and so the fields that are *simulator
    semantics* rather than MT5 semantics are named as such.
    """

    api_version: str
    generator_version: str
    accounting_policy_version: str
    symbol_catalog_version: str
    supported_symbols: list[str]
    account_currency: str
    default_page_size: int
    max_page_size: int
    snapshot_ttl_seconds: int
    after_seq_is_exclusive: bool = True
    cursor_is_opaque_and_signed: bool = True
    time_range_convention: str = "[from_time, to_time)"
    #: Named plainly: these are the simulator's own semantics and carry no
    #: claim that a real MT5 Manager API exposes anything equivalent.
    simulator_specific_fields: list[str] = Field(
        default_factory=lambda: ["run_id", "generation", "snapshot_seq", "source_seq"]
    )
    wire_compatible_with_mt5_manager_api: bool = False


class MetaOut(BaseModel):
    run_id: str | None
    generation: int | None
    scenario_name: str | None
    seed: int | None
    spec_hash: str | None
    status: str | None
    snapshot_seq: int
    virtual_time: str | None
    start_virtual_time: str | None
    account_count: int
    deal_count: int
    open_position_count: int
    capabilities: CapabilitiesOut


class AccountOut(BaseModel):
    account_id: str
    currency: str
    balance: DecimalStr
    credit: DecimalStr


class AccountDetailOut(AccountOut):
    """Single-account view with derived figures.

    ``unrealized_pnl`` and ``equity`` are ``null`` when any open position of
    this account has no mark inside the snapshot. They are not zero: a missing
    quote is unknown, and reporting it as zero would be a confidently wrong
    number (section 3.4). ``data_quality`` says which case applies.
    """

    as_of_seq: int
    unrealized_pnl: DecimalStr | None
    equity: DecimalStr | None
    open_position_count: int
    data_quality: Literal["COMPLETE", "MARK_MISSING"]


class PositionOut(BaseModel):
    position_id: str
    account_id: str
    symbol: str
    side: str
    volume_lots: DecimalStr
    open_price: DecimalStr
    status: str
    #: Null when no mark exists for this symbol inside the snapshot.
    unrealized_pnl: DecimalStr | None = None
    mark_market_time: str | None = None
    mark_stale: bool | None = None


class DealOut(BaseModel):
    deal_id: str
    source_seq: int
    account_id: str
    position_id: str
    symbol: str
    entry: str
    side: str
    volume_lots: DecimalStr
    price: DecimalStr
    realized_pnl_gross: DecimalStr
    commission: DecimalStr
    swap: DecimalStr
    executed_at: str


class MarkOut(BaseModel):
    symbol: str
    bid: DecimalStr
    ask: DecimalStr
    market_time: str
    stale: bool


class PageEnvelope(BaseModel):
    """Common envelope for every list response."""

    run_id: str
    generation: int
    snapshot_seq: int
    #: ``null`` means this *snapshot* has no further pages. It does not promise
    #: that no more data will ever exist (section 5.1).
    next_cursor: str | None


class AccountsPage(PageEnvelope):
    items: list[AccountOut]


class PositionsPage(PageEnvelope):
    items: list[PositionOut]


class DealsPage(PageEnvelope):
    items: list[DealOut]


class MarksPage(PageEnvelope):
    items: list[MarkOut]


class ErrorBody(BaseModel):
    code: str
    message: str
    retryable: bool
    request_id: str | None = None
    run_id: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class ErrorEnvelope(BaseModel):
    error: ErrorBody


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


class LivenessOut(BaseModel):
    status: Literal["alive"]
    version: str


class ReadinessOut(BaseModel):
    """Readiness is not liveness and neither is correctness.

    ``ready`` false with ``recovery_required`` true means the process is fine
    but the data is not trustworthy, which is precisely the state section 4.3
    forbids reporting as healthy.
    """

    ready: bool
    reasons: list[str]
    run_id: str | None
    generation: int | None
    run_status: str | None
    committed_seq: int
    schema_version: int
    writer_running: bool
    writer_queue_depth: int
    writer_queue_capacity: int
    active_snapshots: int
    requests_in_flight: int
    recovery_required: bool
    faults_armed: bool
    active_fault_rules: int


# ---------------------------------------------------------------------------
# Admin requests
# ---------------------------------------------------------------------------


class CreateRunRequest(StrictModel):
    scenario_name: str = Field(min_length=1, max_length=128)
    seed: int = Field(ge=0, le=2**63 - 1)
    start_virtual_time: str
    account_count: int = Field(ge=0)
    deal_count: int = Field(ge=0)
    max_open_positions: int = Field(default=0, ge=0)
    symbols: list[str] = Field(default_factory=lambda: list(DEFAULT_SYMBOLS), max_length=32)
    initial_balance_usd: DecimalStr = Field(default=Decimal("10000.00"))
    generator_version: str = Field(default=GENERATOR_VERSION, max_length=32)
    execution_mode: Literal["bulk_generate"] = "bulk_generate"
    fault_profile: str = Field(default="none", max_length=64)


class ResetRequest(StrictModel):
    #: Guards against a stale operator resetting a run they are no longer
    #: looking at (WF-105). Optional, but omitting it is an explicit choice.
    expected_generation: int | None = Field(default=None, ge=1)
    spec: CreateRunRequest | None = None


class AdvanceRequest(StrictModel):
    events: int = Field(default=0, ge=0)
    clock_steps: int = Field(default=0, ge=0)


class InjectEventRequest(StrictModel):
    """One structured domain event, for golden and crash-boundary tests.

    This is explicitly **not** an order-entry API: there is no OMS, no order
    state machine and no routing. It applies a single already-decided domain
    event so a test can construct an exact state (section 5.2).
    """

    kind: Literal[
        "CREATE_ACCOUNT",
        "ADJUST_ACCOUNT",
        "UPDATE_MARK",
        "OPEN_POSITION",
        "INCREASE_POSITION",
        "CLOSE_POSITION",
    ]
    account_id: str | None = Field(default=None, max_length=64)
    currency: str | None = Field(default=None, max_length=8)
    balance: DecimalStr | None = None
    credit: DecimalStr | None = None
    amount: DecimalStr | None = None
    reason: str | None = Field(default=None, max_length=256)
    symbol: str | None = Field(default=None, max_length=32)
    bid: DecimalStr | None = None
    ask: DecimalStr | None = None
    side: Literal["BUY", "SELL"] | None = None
    volume_lots: DecimalStr | None = None
    price: DecimalStr | None = None
    commission: DecimalStr | None = Field(default=Decimal("0.00"))
    swap: DecimalStr | None = Field(default=Decimal("0.00"))


class FaultScopeIn(StrictModel):
    method: str | None = Field(default=None, max_length=8)
    path: str | None = Field(default=None, max_length=256)
    account_id: str | None = Field(default=None, max_length=64)
    symbol: str | None = Field(default=None, max_length=32)


class FaultTriggerIn(StrictModel):
    kind: Literal["always", "nth_matching_request", "probability"] = "always"
    n: int = Field(default=0, ge=0, le=1_000_000)
    probability: DecimalStr = Field(default=Decimal("0"))
    max_activations: int = Field(default=1, ge=1, le=1_000_000)
    ttl_seconds: int = Field(default=600, ge=1, le=86_400)


class FaultActionIn(StrictModel):
    mode: str = Field(max_length=64)
    parameters: dict[str, Any] = Field(default_factory=dict)


class FaultRuleRequest(StrictModel):
    rule_id: str | None = Field(default=None, max_length=64)
    enabled: bool = True
    layer: Literal["response"] = "response"
    priority: int = Field(default=100, ge=0, le=10_000)
    scope: FaultScopeIn = Field(default_factory=FaultScopeIn)
    trigger: FaultTriggerIn = Field(default_factory=FaultTriggerIn)
    action: FaultActionIn


class ArmFaultsRequest(StrictModel):
    armed: bool


# ---------------------------------------------------------------------------
# Admin responses
# ---------------------------------------------------------------------------


class JobOut(BaseModel):
    job_id: str
    run_id: str
    kind: str
    status: str
    #: What ``processed_count`` counts: ``deals`` for a generate job,
    #: ``events`` for an advance job. Stated rather than inferred.
    unit: str
    target_count: int
    processed_count: int
    error_code: str | None
    error_message: str | None
    cancel_requested: bool
    created_at: str
    started_at: str | None
    finished_at: str | None
    status_url: str


class RunAcceptedOut(BaseModel):
    job_id: str | None
    run_id: str
    generation: int
    status: str
    status_url: str | None = None
    previous_run_id: str | None = None
    previous_generation: int | None = None
    replayed: bool = False


class EventAppliedOut(BaseModel):
    run_id: str
    generation: int
    seq: int
    event_id: str
    event_type: str
    virtual_time: str
    deal_id: str | None
    replayed: bool


class InvariantOut(BaseModel):
    invariant_id: str
    title: str
    scope: str
    status: str
    detail: str
    covered_by: str
    observed: dict[str, Any]


class InvariantReportOut(BaseModel):
    run_id: str
    generation: int
    committed_seq: int
    #: ``fast`` or ``full``. A ``fast`` report has not checked the money.
    scope: str
    #: Empty string on a fast sweep: a digest that was never computed must not
    #: be mistaken for one that was.
    canonical_digest: str
    record_counts: dict[str, int]
    summary: dict[str, Any]
    results: list[InvariantOut]
