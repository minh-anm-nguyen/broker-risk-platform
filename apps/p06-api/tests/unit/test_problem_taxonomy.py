"""The public error taxonomy is a contract, so it is asserted as data.

WF06-Q. Design doc section 4.7 lists a status for every condition. If someone
edits the catalog, these tests fail rather than clients discovering the change
in production.
"""

from __future__ import annotations

import pytest
from risk_api.contracts.problem_details import (
    ENGINE_REASON_TO_PUBLIC_CODE,
    PROBLEM_CATALOG,
    ApiProblem,
    problem_body,
    public_code_for_reasons,
)
from risk_engine import ERROR_CODES

pytestmark = pytest.mark.unit

# The mapping documented in design doc section 4.7, condition -> status.
EXPECTED_STATUSES = {
    "UNAUTHENTICATED": 401,
    "ACCESS_DENIED": 403,
    "RESOURCE_NOT_FOUND": 404,
    "METHOD_NOT_ALLOWED": 405,
    "NOT_ACCEPTABLE": 406,
    "REQUEST_TOO_LARGE": 413,
    "URI_TOO_LONG": 414,
    "UNSUPPORTED_MEDIA_TYPE": 415,
    "INVALID_REQUEST": 422,
    "INVALID_PAYLOAD": 422,
    "INVALID_TIME_WINDOW": 422,
    "LIMIT_EXCEEDED": 422,
    "ACCOUNT_SCOPE_TOO_LARGE": 422,
    "HISTORICAL_WINDOW_TOO_LARGE": 422,
    "RESULT_TOO_LARGE": 422,
    "UNSUPPORTED_CALC_MODE": 422,
    "COMPOSITE_UNPROVEN": 409,
    "DATA_NOT_READY": 409,
    "EPOCH_CHANGED": 409,
    "CUT_CHANGED": 409,
    "SCAN_CHANGED": 409,
    "CURSOR_EXPIRED": 409,
    "RATE_LIMITED": 429,
    "OVERLOADED": 503,
    "DB_POOL_BUSY": 503,
    "DB_UNAVAILABLE": 503,
    "AUDIT_UNAVAILABLE": 503,
    "INVALID_RISK_RESULT": 502,
    "DEADLINE_EXCEEDED": 504,
    "INTERNAL_ERROR": 500,
}


@pytest.mark.parametrize(("code", "status"), sorted(EXPECTED_STATUSES.items()))
def test_catalog_matches_the_documented_status(code: str, status: int):
    assert PROBLEM_CATALOG[code].status == status


def test_every_catalog_entry_has_a_distinct_slug():
    slugs = [spec.slug for spec in PROBLEM_CATALOG.values()]
    assert len(slugs) == len(set(slugs))


def test_unknown_problem_code_cannot_be_raised():
    """A typo in a code must fail loudly, not emit a 500 with a blank body."""
    with pytest.raises(KeyError):
        ApiProblem("NO_SUCH_CODE", "x")


def test_retryable_flag_matches_the_retry_advice():
    assert PROBLEM_CATALOG["RATE_LIMITED"].retryable is True
    assert PROBLEM_CATALOG["OVERLOADED"].retryable is True
    assert PROBLEM_CATALOG["DEADLINE_EXCEEDED"].retryable is True
    # A client retrying these without a data change just wastes capacity.
    assert PROBLEM_CATALOG["COMPOSITE_UNPROVEN"].retryable is False
    assert PROBLEM_CATALOG["ACCESS_DENIED"].retryable is False
    assert PROBLEM_CATALOG["INVALID_RISK_RESULT"].retryable is False


def test_problem_body_has_the_rfc_9457_members():
    problem = ApiProblem("COMPOSITE_UNPROVEN", "cannot prove", reason_codes=("INCOMPATIBLE_CUT",))
    body = problem_body(problem, "req-1")
    assert set(body) == {
        "type",
        "title",
        "status",
        "detail",
        "instance",
        "code",
        "request_id",
        "retryable",
        "reason_codes",
    }
    assert body["type"].startswith("https://")
    assert body["instance"] == "urn:request:req-1"
    assert body["status"] == 409
    assert body["reason_codes"] == ["INCOMPATIBLE_CUT"]


def test_every_engine_error_code_has_a_public_mapping():
    """A new engine reason must not fall through to a blanket 500."""
    unmapped = sorted(set(ERROR_CODES) - set(ENGINE_REASON_TO_PUBLIC_CODE))
    assert unmapped == [], f"engine codes with no public mapping: {unmapped}"


def test_mapped_public_codes_all_exist_in_the_catalog():
    missing = sorted(set(ENGINE_REASON_TO_PUBLIC_CODE.values()) - set(PROBLEM_CATALOG))
    assert missing == []


@pytest.mark.parametrize(
    ("reasons", "expected"),
    [
        (("INCOMPATIBLE_CUT",), "COMPOSITE_UNPROVEN"),
        (("QUOTE_CUT_UNPROVEN",), "COMPOSITE_UNPROVEN"),
        (("RECON_BLOCKING",), "DATA_NOT_READY"),
        (("P03_UNPUBLISHED",), "DATA_NOT_READY"),
        (("DEAL_WATERMARK_UNPROVEN",), "DATA_NOT_READY"),
        (("EPOCH_CHANGED",), "EPOCH_CHANGED"),
        (("UNSUPPORTED_CALC_MODE",), "UNSUPPORTED_CALC_MODE"),
        (("FX_RATE_MISSING",), "UNSUPPORTED_CALC_MODE"),
        (("UNAUTHORIZED_SCOPE",), "RESOURCE_NOT_FOUND"),
        ((), "COMPOSITE_UNPROVEN"),
    ],
)
def test_reason_to_public_code(reasons: tuple[str, ...], expected: str):
    assert public_code_for_reasons(reasons) == expected


def test_the_most_specific_blocker_wins():
    """A client told only "composite unproven" cannot act on a blocking finding."""
    assert public_code_for_reasons(("INCOMPATIBLE_CUT", "RECON_BLOCKING")) == "DATA_NOT_READY"
    assert public_code_for_reasons(("FEE_COVERAGE_UNAVAILABLE", "QUOTE_CUT_UNPROVEN")) == (
        "COMPOSITE_UNPROVEN"
    )


def test_detail_text_is_whatever_the_service_wrote():
    problem = ApiProblem("INVALID_REQUEST", "components must not contain empty members")
    body = problem_body(problem, "req-2")
    assert body["detail"] == "components must not contain empty members"


def test_missing_detail_falls_back_to_the_title():
    body = problem_body(ApiProblem("OVERLOADED"), "req-3")
    assert body["detail"] == PROBLEM_CATALOG["OVERLOADED"].title
