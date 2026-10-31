"""The control plane's transactional guards. Sections 5.3, 6.5.

What-if domains S, T, AI. These are the guarantees that decide whether a
reader can trust a number, so each guard gets a test that tries to defeat it:
a second lease, an expired lease, a stale revision, a missing evidence row, a
revoked trust record, an illegal transition.
"""

from __future__ import annotations

import pytest

from analytics.adapters.control_sqlite import SqliteControlPlane
from analytics.clock import ManualClock
from analytics.errors import (
    AnalyticsError,
    BuildState,
    CompletenessError,
    FencedError,
    IntegrityError,
    Reason,
)
from analytics.ports.control import BuildRecord, QuarantineRecord

pytestmark = pytest.mark.component

TENANT, SOURCE, EPOCH, FAMILY = "t", "s", "e", "daily_deal_v1"
CUT, POLICY = "cut-1", "pol-1"
CLAIM_KEY = (TENANT, SOURCE, EPOCH, FAMILY, CUT, POLICY)


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def control(clock) -> SqliteControlPlane:
    plane = SqliteControlPlane(clock=clock)
    yield plane
    plane.close()


def make_build(control, *, build_id="b1", attempt_no=1, revision="recon-1", cut=CUT) -> BuildRecord:
    return control.create_build(
        BuildRecord(
            build_id=build_id,
            tenant_id=TENANT,
            source_id=SOURCE,
            epoch_id=EPOCH,
            report_family=FAMILY,
            cut_id=cut,
            policy_version=POLICY,
            recon_revision=revision,
            source_manifest_sha256="f" * 64,
            state=BuildState.REQUESTED,
            attempt_no=attempt_no,
        )
    )


def claim(control, *, build_id="b1", token="L1", seconds=300, cut=CUT):
    return control.acquire_claim(
        tenant_id=TENANT,
        source_id=SOURCE,
        epoch_id=EPOCH,
        report_family=FAMILY,
        cut_id=cut,
        policy_version=POLICY,
        build_id=build_id,
        lease_token=token,
        lease_seconds=seconds,
    )


def drive_to_verified(control, *, build_id="b1", token="L1", revision="recon-1"):
    """Walk a build to VERIFIED with evidence and trust in place."""
    for before, after in (
        (BuildState.REQUESTED, BuildState.BUILDING),
        (BuildState.BUILDING, BuildState.VALIDATING),
    ):
        control.transition_build(
            build_id=build_id, expected_state=before, new_state=after, lease_token=token
        )
    control.put_evidence(build_id=build_id, evidence_kind="decimal_oracle", evidence_json={"ok": True})
    control.transition_build(
        build_id=build_id,
        expected_state=BuildState.VALIDATING,
        new_state=BuildState.VERIFIED,
        lease_token=token,
        evidence_sha256="d" * 64,
    )
    control.put_trust(cut_key=CLAIM_KEY, revision=revision, verified=True, revoked=False)


class TestBuildRecords:
    def test_create_and_read_back(self, control):
        record = make_build(control)
        assert record.state is BuildState.REQUESTED
        assert control.get_build("b1") == record

    def test_build_id_cannot_be_reused(self, control):
        """P08-S-10: a retry must mint a new immutable id."""
        make_build(control)
        with pytest.raises(IntegrityError) as excinfo:
            make_build(control)
        assert excinfo.value.reason is Reason.BUILD_ID_REUSED

    def test_attempt_numbers_increment_per_logical_build(self, control):
        assert control.next_attempt_no(
            tenant_id=TENANT, source_id=SOURCE, epoch_id=EPOCH,
            report_family=FAMILY, cut_id=CUT, policy_version=POLICY,
        ) == 1
        make_build(control, build_id="b1", attempt_no=1)
        assert control.next_attempt_no(
            tenant_id=TENANT, source_id=SOURCE, epoch_id=EPOCH,
            report_family=FAMILY, cut_id=CUT, policy_version=POLICY,
        ) == 2

    def test_duplicate_attempt_number_is_refused(self, control):
        make_build(control, build_id="b1", attempt_no=1)
        with pytest.raises(Exception):
            make_build(control, build_id="b2", attempt_no=1)

    def test_list_filters_by_scope_and_state(self, control):
        make_build(control, build_id="b1", attempt_no=1)
        make_build(control, build_id="b2", attempt_no=2)
        assert len(control.list_builds(tenant_id=TENANT)) == 2
        assert control.list_builds(tenant_id="other") == []
        assert len(control.list_builds(tenant_id=TENANT, states=[BuildState.REQUESTED])) == 2
        assert control.list_builds(tenant_id=TENANT, states=[BuildState.PUBLISHED]) == []


class TestTransitions:
    def test_legal_transition_succeeds(self, control):
        make_build(control)
        claim(control)
        updated = control.transition_build(
            build_id="b1",
            expected_state=BuildState.REQUESTED,
            new_state=BuildState.BUILDING,
            lease_token="L1",
        )
        assert updated.state is BuildState.BUILDING

    @pytest.mark.parametrize(
        "before,after",
        [
            (BuildState.REQUESTED, BuildState.VERIFIED),
            (BuildState.REQUESTED, BuildState.PUBLISHED),
            (BuildState.BUILDING, BuildState.PUBLISHED),
            (BuildState.FAILED, BuildState.VERIFIED),
            (BuildState.QUARANTINED, BuildState.VERIFIED),
        ],
    )
    def test_illegal_transition_is_refused(self, control, before, after):
        make_build(control)
        with pytest.raises(AnalyticsError) as excinfo:
            control.transition_build(
                build_id="b1", expected_state=before, new_state=after, lease_token=None
            )
        assert excinfo.value.reason is Reason.BUILD_STATE_INVALID

    def test_wrong_expected_state_is_refused(self, control):
        make_build(control)
        with pytest.raises(AnalyticsError) as excinfo:
            control.transition_build(
                build_id="b1",
                expected_state=BuildState.BUILDING,
                new_state=BuildState.VALIDATING,
                lease_token=None,
            )
        assert excinfo.value.reason is Reason.BUILD_STATE_INVALID

    def test_a_foreign_lease_cannot_move_state(self, control):
        """INV08-11: losing the lease stops the worker, not just the publish."""
        make_build(control)
        claim(control, token="L1")
        with pytest.raises(FencedError) as excinfo:
            control.transition_build(
                build_id="b1",
                expected_state=BuildState.REQUESTED,
                new_state=BuildState.BUILDING,
                lease_token="L2",
            )
        assert excinfo.value.reason is Reason.LEASE_HELD_BY_OTHER

    def test_unknown_build_is_refused(self, control):
        with pytest.raises(AnalyticsError) as excinfo:
            control.transition_build(
                build_id="missing",
                expected_state=BuildState.REQUESTED,
                new_state=BuildState.BUILDING,
                lease_token=None,
            )
        assert excinfo.value.reason is Reason.BUILD_NOT_FOUND


class TestLeases:
    def test_second_live_lease_is_refused(self, control):
        make_build(control, build_id="b1", attempt_no=1)
        make_build(control, build_id="b2", attempt_no=2)
        claim(control, build_id="b1", token="L1")
        with pytest.raises(FencedError) as excinfo:
            claim(control, build_id="b2", token="L2")
        assert excinfo.value.reason is Reason.LEASE_HELD_BY_OTHER

    def test_lease_can_be_taken_over_after_expiry(self, control, clock):
        """P08-S-02: a dead worker's lease must not block forever."""
        make_build(control, build_id="b1", attempt_no=1)
        make_build(control, build_id="b2", attempt_no=2)
        claim(control, build_id="b1", token="L1", seconds=60)
        clock.advance(seconds=61)
        taken = claim(control, build_id="b2", token="L2", seconds=60)
        assert taken.active_build_id == "b2"
        assert taken.lease_token == "L2"

    def test_expiry_uses_the_control_plane_clock(self, control, clock):
        """Section 5.3: a skewed worker clock must not void a live lease."""
        make_build(control)
        claim(control, token="L1", seconds=300)
        control.assert_lease_held(claim_key=CLAIM_KEY, lease_token="L1")
        clock.advance(seconds=301)
        with pytest.raises(FencedError) as excinfo:
            control.assert_lease_held(claim_key=CLAIM_KEY, lease_token="L1")
        assert excinfo.value.reason is Reason.LEASE_LOST

    def test_wrong_token_is_not_held(self, control):
        make_build(control)
        claim(control, token="L1")
        with pytest.raises(FencedError):
            control.assert_lease_held(claim_key=CLAIM_KEY, lease_token="L2")

    def test_renew_extends_a_live_lease(self, control, clock):
        make_build(control)
        claim(control, token="L1", seconds=60)
        clock.advance(seconds=30)
        control.renew_claim(claim_key=CLAIM_KEY, lease_token="L1", lease_seconds=300)
        clock.advance(seconds=100)
        control.assert_lease_held(claim_key=CLAIM_KEY, lease_token="L1")

    def test_renew_cannot_resurrect_an_expired_lease(self, control, clock):
        """P08-S-08: another owner may already have replaced it."""
        make_build(control)
        claim(control, token="L1", seconds=60)
        clock.advance(seconds=61)
        with pytest.raises(FencedError) as excinfo:
            control.renew_claim(claim_key=CLAIM_KEY, lease_token="L1", lease_seconds=60)
        assert excinfo.value.reason is Reason.LEASE_LOST

    def test_release_frees_the_claim(self, control):
        make_build(control, build_id="b1", attempt_no=1)
        make_build(control, build_id="b2", attempt_no=2)
        claim(control, build_id="b1", token="L1")
        control.release_claim(claim_key=CLAIM_KEY, lease_token="L1")
        assert claim(control, build_id="b2", token="L2").lease_token == "L2"

    def test_lease_version_increases_monotonically(self, control):
        make_build(control)
        first = claim(control, token="L1").lease_version
        second = control.renew_claim(
            claim_key=CLAIM_KEY, lease_token="L1", lease_seconds=60
        ).lease_version
        assert second > first


class TestPublication:
    def test_first_publication_sets_revision_one(self, control):
        make_build(control)
        claim(control)
        drive_to_verified(control)
        pointer = control.publish_pointer(
            build_id="b1", lease_token="L1", expected_revision=None, actor="test"
        )
        assert pointer.publication_revision == 1
        assert pointer.current_build_id == "b1"
        assert control.get_build("b1").state is BuildState.PUBLISHED

    def test_unverified_build_cannot_publish(self, control):
        make_build(control)
        claim(control)
        with pytest.raises(CompletenessError) as excinfo:
            control.publish_pointer(
                build_id="b1", lease_token="L1", expected_revision=None, actor="test"
            )
        assert excinfo.value.reason is Reason.BUILD_NOT_VERIFIED

    def test_missing_evidence_blocks_publication(self, control):
        """KP08-22: no publish without durable evidence."""
        make_build(control)
        claim(control)
        for before, after in (
            (BuildState.REQUESTED, BuildState.BUILDING),
            (BuildState.BUILDING, BuildState.VALIDATING),
            (BuildState.VALIDATING, BuildState.VERIFIED),
        ):
            control.transition_build(
                build_id="b1", expected_state=before, new_state=after, lease_token="L1"
            )
        control.put_trust(cut_key=CLAIM_KEY, revision="recon-1", verified=True, revoked=False)
        with pytest.raises(CompletenessError) as excinfo:
            control.publish_pointer(
                build_id="b1", lease_token="L1", expected_revision=None, actor="test"
            )
        assert excinfo.value.reason is Reason.EVIDENCE_MISSING

    def test_missing_trust_blocks_publication(self, control):
        make_build(control)
        claim(control)
        for before, after in (
            (BuildState.REQUESTED, BuildState.BUILDING),
            (BuildState.BUILDING, BuildState.VALIDATING),
        ):
            control.transition_build(
                build_id="b1", expected_state=before, new_state=after, lease_token="L1"
            )
        control.put_evidence(build_id="b1", evidence_kind="o", evidence_json={})
        control.transition_build(
            build_id="b1",
            expected_state=BuildState.VALIDATING,
            new_state=BuildState.VERIFIED,
            lease_token="L1",
            evidence_sha256="d" * 64,
        )
        with pytest.raises(CompletenessError) as excinfo:
            control.publish_pointer(
                build_id="b1", lease_token="L1", expected_revision=None, actor="test"
            )
        assert excinfo.value.reason is Reason.RECONCILIATION_NOT_VERIFIED

    def test_revoked_trust_blocks_publication(self, control):
        """Section 6.7: P04 may revoke between the build and the flip."""
        make_build(control)
        claim(control)
        drive_to_verified(control)
        control.put_trust(cut_key=CLAIM_KEY, revision="recon-1", verified=True, revoked=True)
        with pytest.raises(CompletenessError) as excinfo:
            control.publish_pointer(
                build_id="b1", lease_token="L1", expected_revision=None, actor="test"
            )
        assert excinfo.value.reason is Reason.RECONCILIATION_REVOKED

    def test_trust_revision_drift_blocks_publication(self, control):
        make_build(control, revision="recon-1")
        claim(control)
        drive_to_verified(control, revision="recon-1")
        control.put_trust(cut_key=CLAIM_KEY, revision="recon-2", verified=True, revoked=False)
        with pytest.raises(CompletenessError) as excinfo:
            control.publish_pointer(
                build_id="b1", lease_token="L1", expected_revision=None, actor="test"
            )
        assert excinfo.value.reason is Reason.RECONCILIATION_REVOKED

    def test_expired_lease_cannot_publish(self, control, clock):
        make_build(control)
        claim(control, seconds=60)
        drive_to_verified(control)
        clock.advance(seconds=61)
        with pytest.raises(FencedError) as excinfo:
            control.publish_pointer(
                build_id="b1", lease_token="L1", expected_revision=None, actor="test"
            )
        assert excinfo.value.reason is Reason.LEASE_LOST

    def test_wrong_lease_token_cannot_publish(self, control):
        make_build(control)
        claim(control, token="L1")
        drive_to_verified(control, token="L1")
        with pytest.raises(FencedError):
            control.publish_pointer(
                build_id="b1", lease_token="L-WRONG", expected_revision=None, actor="test"
            )

    def test_stale_revision_cas_is_refused(self, control):
        """P08-T-04: a stale worker retrying after someone else published."""
        make_build(control, build_id="b1", attempt_no=1)
        claim(control, build_id="b1", token="L1")
        drive_to_verified(control, build_id="b1", token="L1")
        control.publish_pointer(
            build_id="b1", lease_token="L1", expected_revision=None, actor="test"
        )
        control.release_claim(claim_key=CLAIM_KEY, lease_token="L1")

        make_build(control, build_id="b2", attempt_no=2)
        claim(control, build_id="b2", token="L2")
        drive_to_verified(control, build_id="b2", token="L2")
        with pytest.raises(FencedError) as excinfo:
            control.publish_pointer(
                build_id="b2", lease_token="L2", expected_revision=None, actor="test"
            )
        assert excinfo.value.reason is Reason.PUBLICATION_REVISION_STALE

    def test_second_publication_supersedes_the_first(self, control):
        make_build(control, build_id="b1", attempt_no=1)
        claim(control, build_id="b1", token="L1")
        drive_to_verified(control, build_id="b1", token="L1")
        control.publish_pointer(
            build_id="b1", lease_token="L1", expected_revision=None, actor="test"
        )
        control.release_claim(claim_key=CLAIM_KEY, lease_token="L1")

        make_build(control, build_id="b2", attempt_no=2)
        claim(control, build_id="b2", token="L2")
        drive_to_verified(control, build_id="b2", token="L2")
        pointer = control.publish_pointer(
            build_id="b2", lease_token="L2", expected_revision=1, actor="test"
        )
        assert pointer.current_build_id == "b2"
        assert pointer.publication_revision == 2
        assert control.get_build("b1").state is BuildState.SUPERSEDED
        assert control.get_build("b2").state is BuildState.PUBLISHED

    def test_history_records_every_move(self, control):
        make_build(control)
        claim(control)
        drive_to_verified(control)
        control.publish_pointer(
            build_id="b1", lease_token="L1", expected_revision=None, actor="builder-7"
        )
        history = control.publication_history(
            tenant_id=TENANT, source_id=SOURCE, epoch_id=EPOCH, report_family=FAMILY
        )
        assert [entry.action for entry in history] == ["PUBLISH"]
        assert history[0].actor == "builder-7"
        assert history[0].previous_build_id is None


class TestRollback:
    def _publish_two(self, control):
        make_build(control, build_id="b1", attempt_no=1)
        claim(control, build_id="b1", token="L1")
        drive_to_verified(control, build_id="b1", token="L1")
        control.publish_pointer(
            build_id="b1", lease_token="L1", expected_revision=None, actor="test"
        )
        control.release_claim(claim_key=CLAIM_KEY, lease_token="L1")
        make_build(control, build_id="b2", attempt_no=2)
        claim(control, build_id="b2", token="L2")
        drive_to_verified(control, build_id="b2", token="L2")
        control.publish_pointer(
            build_id="b2", lease_token="L2", expected_revision=1, actor="test"
        )

    def test_rollback_restores_the_previous_build(self, control):
        """INV08-18: history is not rewritten; the revision moves forward."""
        self._publish_two(control)
        pointer = control.rollback_pointer(
            tenant_id=TENANT,
            source_id=SOURCE,
            epoch_id=EPOCH,
            report_family=FAMILY,
            target_build_id="b1",
            expected_revision=2,
            actor="oncall",
            note="completeness error found in b2",
        )
        assert pointer.current_build_id == "b1"
        assert pointer.publication_revision == 3
        assert control.get_build("b2").state is BuildState.SUPERSEDED
        history = control.publication_history(
            tenant_id=TENANT, source_id=SOURCE, epoch_id=EPOCH, report_family=FAMILY
        )
        assert [entry.action for entry in history] == ["PUBLISH", "PUBLISH", "ROLLBACK"]

    def test_rollback_with_a_stale_revision_is_refused(self, control):
        self._publish_two(control)
        with pytest.raises(FencedError) as excinfo:
            control.rollback_pointer(
                tenant_id=TENANT,
                source_id=SOURCE,
                epoch_id=EPOCH,
                report_family=FAMILY,
                target_build_id="b1",
                expected_revision=1,
                actor="oncall",
            )
        assert excinfo.value.reason is Reason.PUBLICATION_REVISION_STALE

    def test_cannot_roll_back_to_an_unverified_build(self, control):
        self._publish_two(control)
        make_build(control, build_id="b3", attempt_no=3)
        with pytest.raises(CompletenessError) as excinfo:
            control.rollback_pointer(
                tenant_id=TENANT,
                source_id=SOURCE,
                epoch_id=EPOCH,
                report_family=FAMILY,
                target_build_id="b3",
                expected_revision=2,
                actor="oncall",
            )
        assert excinfo.value.reason is Reason.BUILD_NOT_VERIFIED


class TestQuarantine:
    def _receipt(self, *, offset=10, receipt_id="qr-1") -> QuarantineRecord:
        return QuarantineRecord(
            receipt_id=receipt_id,
            tenant_id=TENANT,
            source_id=SOURCE,
            epoch_id=EPOCH,
            topic="broker.deals.v1",
            partition=0,
            offset=offset,
            event_id=None,
            reason="FIELD_MISSING",
            detail={"message": "no event_id"},
            payload_sha256=None,
            recorded_at=ManualClock().now(),
        )

    def test_receipt_is_idempotent_per_coordinate(self, control):
        """P08-F-09: replayed poison must not multiply receipts."""
        first = control.record_quarantine(self._receipt())
        second = control.record_quarantine(self._receipt(receipt_id="qr-other"))
        assert second.receipt_id == first.receipt_id
        assert len(control.list_quarantine()) == 1

    def test_unresolved_filter(self, control):
        control.record_quarantine(self._receipt())
        assert len(control.list_quarantine(unresolved_only=True)) == 1
        control.resolve_quarantine(receipt_id="qr-1", note="bad producer build, reprocessed")
        assert control.list_quarantine(unresolved_only=True) == []
        assert len(control.list_quarantine(unresolved_only=False)) == 1

    def test_scope_filter(self, control):
        control.record_quarantine(self._receipt())
        assert len(control.list_quarantine(tenant_id=TENANT)) == 1
        assert control.list_quarantine(tenant_id="other") == []

    def test_resolving_an_unknown_receipt_is_refused(self, control):
        with pytest.raises(AnalyticsError):
            control.resolve_quarantine(receipt_id="nope", note="x")


class TestEvidenceAndPins:
    def test_evidence_is_hashed_and_upserted(self, control):
        make_build(control)
        first = control.put_evidence(
            build_id="b1", evidence_kind="decimal_oracle", evidence_json={"a": 1}
        )
        second = control.put_evidence(
            build_id="b1", evidence_kind="decimal_oracle", evidence_json={"a": 2}
        )
        assert first.evidence_sha256 != second.evidence_sha256
        records = control.get_evidence(build_id="b1")
        assert len(records) == 1
        assert records[0].evidence_json == {"a": 2}

    def test_reader_pins_are_counted(self, control):
        make_build(control)
        assert control.active_reader_pins(build_id="b1") == 0
        control.register_reader_pin(build_id="b1", pin_id="p1")
        control.register_reader_pin(build_id="b1", pin_id="p2")
        assert control.active_reader_pins(build_id="b1") == 2
        control.release_reader_pin(pin_id="p1")
        assert control.active_reader_pins(build_id="b1") == 1

    def test_trust_round_trips(self, control):
        control.put_trust(
            cut_key=CLAIM_KEY, revision="r1", verified=False, revoked=True, findings=["gap"]
        )
        trust = control.get_trust(cut_key=CLAIM_KEY)
        assert trust == {
            "revision": "r1",
            "verified": False,
            "revoked": True,
            "findings": ["gap"],
            "updated_at": trust["updated_at"],
        }
