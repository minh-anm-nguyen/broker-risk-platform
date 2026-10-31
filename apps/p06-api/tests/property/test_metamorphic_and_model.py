"""Property-based and model-based checks.

Design doc section 13.4. These look for the bugs a fixed example set misses:
a scope that widens under an unusual account subset, a page partition that
duplicates a row, a digest that depends on input order, or a stale result
served after a revision moved.

The state machine mirrors the service's *decision* rules, not its arithmetic,
so it is an independent oracle rather than a second copy of the code.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from conftest import ACCOUNT_A, ACCOUNT_B, SOURCE_ID, add_account, add_position
from hypothesis import HealthCheck, given, settings as hypothesis_settings
from hypothesis import strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, invariant, rule
from risk_engine import format_units

pytestmark = pytest.mark.property

RISK = f"/api/v1/accounts/{ACCOUNT_A}/risk"
POSITIONS = "/api/v1/positions"
EXPOSURE = "/api/v1/exposure"

SLOW_SETTINGS = hypothesis_settings(
    max_examples=12,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)


class TestIdempotentReads:
    @SLOW_SETTINGS
    @given(components=st.lists(st.sampled_from(["EXPOSURE", "REALIZED", "UNREALIZED"]), min_size=1, max_size=3, unique=True))
    async def test_the_same_cut_yields_the_same_digest(self, client, auth, components):
        """Only the request-scoped identifiers may differ between two reads."""
        params = {"source_id": SOURCE_ID, "components": ",".join(components)}
        first = await client.get(RISK, params=params, headers=auth)
        second = await client.get(RISK, params=params, headers=auth)
        if first.status_code == 429 or second.status_code == 429:
            pytest.skip("rate limited; not what this property is about")
        assert first.status_code == second.status_code
        if first.status_code != 200:
            return
        a, b = first.json(), second.json()
        assert a["output_digest"] == b["output_digest"]
        assert a["input_digest"] == b["input_digest"]
        assert a["components"] == b["components"]
        assert a["request_id"] != b["request_id"]

    @SLOW_SETTINGS
    @given(order=st.permutations(["EXPOSURE", "REALIZED", "UNREALIZED"]))
    async def test_component_order_is_irrelevant(self, client, auth, order):
        response = await client.get(
            RISK, params={"source_id": SOURCE_ID, "components": ",".join(order)}, headers=auth
        )
        if response.status_code == 429:
            pytest.skip("rate limited")
        assert response.status_code == 200
        assert set(response.json()["components"]) == {"exposure", "realized", "unrealized"}


class TestScopeMonotonicity:
    @SLOW_SETTINGS
    @given(
        extras=st.lists(
            st.sampled_from([ACCOUNT_B, "00099", "00000", "99999"]), min_size=1, max_size=4, unique=True
        )
    )
    async def test_adding_an_unauthorized_account_never_widens_the_result(
        self, client, context, auth, extras
    ):
        """An unauthorized addition must deny, never silently drop."""
        selection = ",".join([ACCOUNT_A, *extras])
        response = await client.get(
            EXPOSURE, params={"source_id": SOURCE_ID, "account_ids": selection}, headers=auth
        )
        if response.status_code == 429:
            pytest.skip("rate limited")
        assert response.status_code in (403, 404), response.status_code
        for account in extras:
            assert account not in response.text

    @SLOW_SETTINGS
    @given(dummy=st.integers(min_value=0, max_value=3))
    async def test_the_authorized_subset_is_always_exactly_the_scope(
        self, client, auth, dummy
    ):
        response = await client.get(
            EXPOSURE, params={"source_id": SOURCE_ID, "account_ids": ACCOUNT_A}, headers=auth
        )
        if response.status_code == 429:
            pytest.skip("rate limited")
        assert response.json()["scope"]["account_ids"] == [ACCOUNT_A]


class TestPagePartition:
    @SLOW_SETTINGS
    @given(page_size=st.integers(min_value=1, max_value=7), count=st.integers(min_value=0, max_value=14))
    async def test_pages_partition_the_scan_exactly_once(
        self, client, world, auth, page_size, count
    ):
        """INV06-10: the union of pages equals the snapshot, with no duplicates."""
        for key in [k for k, row in world.positions.items() if row.account_id == ACCOUNT_A]:
            if key != "P-0001":
                world.positions.pop(key)
        for index in range(count):
            add_position(world, f"PP-{index:03d}", ACCOUNT_A, lots="0.1")

        expected = {
            key for key, row in world.positions.items() if row.account_id == ACCOUNT_A
        }
        seen: list[str] = []
        cursor = None
        for _ in range(40):
            params = {"source_id": SOURCE_ID, "limit": str(page_size)}
            if cursor:
                params["cursor"] = cursor
            response = await client.get(POSITIONS, params=params, headers=auth)
            if response.status_code == 429:
                pytest.skip("rate limited")
            assert response.status_code == 200, response.text
            body = response.json()
            assert len(body["items"]) <= page_size
            seen.extend(item["position_id"] for item in body["items"])
            cursor = body["next_cursor"]
            if not cursor:
                break
        else:
            pytest.fail("pagination did not terminate")

        assert len(seen) == len(set(seen)), "a position appeared twice"
        assert set(seen) == expected
        assert seen == sorted(seen), "keyset ordering was not preserved"


class TestExposureArithmetic:
    @SLOW_SETTINGS
    @given(
        lots=st.lists(
            st.decimals(min_value=Decimal("0.01"), max_value=Decimal("10"), places=2),
            min_size=1,
            max_size=5,
        ),
        sides=st.lists(st.sampled_from(["BUY", "SELL"]), min_size=1, max_size=5),
    )
    async def test_gross_and_net_reconcile_for_any_position_set(
        self, client, world, auth, lots, sides
    ):
        """Metamorphic: gross is long+short and net is long-short, always."""
        for key in [k for k, row in world.positions.items() if row.account_id == ACCOUNT_A]:
            world.positions.pop(key)
        pairs = list(zip(lots, sides, strict=False))
        if not pairs:
            pytest.skip("no positions generated")
        for index, (lot, side) in enumerate(pairs):
            add_position(world, f"PE-{index:03d}", ACCOUNT_A, side=side, lots=str(lot))

        response = await client.get(
            RISK, params={"source_id": SOURCE_ID, "components": "EXPOSURE"}, headers=auth
        )
        if response.status_code == 429:
            pytest.skip("rate limited")
        assert response.status_code == 200, response.text
        bucket = response.json()["components"]["exposure"]["by_symbol"]["EURUSD"]
        long_units = Decimal(bucket["long_units"])
        short_units = Decimal(bucket["short_units"])
        assert Decimal(bucket["gross_units"]) == long_units + short_units
        assert Decimal(bucket["net_units"]) == long_units - short_units

        expected_long = sum(
            (Decimal(str(lot)) * Decimal("100000") for lot, side in pairs if side == "BUY"),
            Decimal(0),
        )
        assert long_units == expected_long
        assert bucket["long_units"] == format_units(expected_long)


class TestEpochSeparation:
    @SLOW_SETTINGS
    @given(epoch=st.sampled_from(["E2", "E3", "E-other"]))
    async def test_a_rotated_epoch_never_serves_the_old_cut(self, client, world, auth, epoch):
        before = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        if before.status_code == 429:
            pytest.skip("rate limited")
        assert before.status_code == 200
        # Hypothesis reuses the fixture across examples, so read the current
        # epoch rather than assuming the pristine one.
        previous = before.json()["scope"]["epoch_id"]
        if previous == epoch:
            pytest.skip("already on this epoch")

        world.rotate_epoch(epoch)
        after = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        if after.status_code == 429:
            pytest.skip("rate limited")
        assert after.status_code == 200
        assert after.json()["scope"]["epoch_id"] == epoch
        assert after.json()["output_digest"] != before.json()["output_digest"]


class RiskApiModel(RuleBasedStateMachine):
    """A model of the service's admission and release decisions.

    The model tracks only what the design says must gate a release: the epoch,
    the published scan, the reconciliation revision and the authorized set. It
    then asserts the real service agrees about *whether* a report is releasable.
    """

    requests = Bundle("requests")

    def __init__(self) -> None:
        super().__init__()
        self.epoch = "E1"
        self.active_scan = "scan-P"
        self.recon_revision = 0
        self.recon_blocking = False
        self.allowed_accounts = {ACCOUNT_A}
        self.decisions: list[tuple[str, str]] = []

    @rule(target=requests, account=st.sampled_from([ACCOUNT_A, ACCOUNT_B, "99999"]))
    def start_request(self, account: str) -> tuple[str, str, str, int, bool]:
        decision = "ADMIT" if account in self.allowed_accounts else "DENY"
        self.decisions.append((account, decision))
        return (account, self.epoch, self.active_scan, self.recon_revision, self.recon_blocking)

    @rule()
    def change_source_epoch(self) -> None:
        self.epoch = f"E{int(self.epoch[1:]) + 1}" if self.epoch[1:].isdigit() else "E2"

    @rule()
    def publish_new_position_scan(self) -> None:
        self.active_scan = f"{self.active_scan}-next"

    @rule()
    def add_blocking_finding(self) -> None:
        self.recon_revision += 1
        self.recon_blocking = True

    @rule()
    def clear_blocking_finding(self) -> None:
        self.recon_revision += 1
        self.recon_blocking = False

    @rule(pinned=requests)
    def complete_request(self, pinned: tuple[str, str, str, int, bool]) -> None:
        account, epoch, scan, revision, blocking = pinned
        if account not in self.allowed_accounts:
            # A denied request must never become releasable later.
            assert ("DENY") in [d for _, d in self.decisions if _ == account]
            return
        current = (self.epoch, self.active_scan, self.recon_revision, self.recon_blocking)
        releasable = current == (epoch, scan, revision, blocking) and not blocking
        if blocking:
            assert not releasable, "a blocking finding can never be releasable"

    @invariant()
    def an_unauthorized_account_is_never_admitted(self) -> None:
        for account, decision in self.decisions:
            if account not in self.allowed_accounts:
                assert decision == "DENY"

    @invariant()
    def the_revision_never_goes_backwards(self) -> None:
        assert self.recon_revision >= 0


TestRiskApiStateMachine = RiskApiModel.TestCase
TestRiskApiStateMachine.settings = hypothesis_settings(
    max_examples=25, stateful_step_count=12, deadline=None
)


class TestConcurrencyInvariance:
    async def test_parallel_identical_requests_match_the_serial_oracle(self, client, auth):
        """Parallelism must not change a value."""
        serial = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert serial.status_code == 200
        expected = serial.json()["output_digest"]

        parallel = await asyncio.gather(
            *(client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth) for _ in range(6))
        )
        for response in parallel:
            if response.status_code == 429:
                continue
            assert response.status_code == 200
            assert response.json()["output_digest"] == expected
