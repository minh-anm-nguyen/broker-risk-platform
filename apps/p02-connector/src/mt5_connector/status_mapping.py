"""HTTP status to typed error: the mapping a consumer branches on.

Its own module because two very different callers need it and neither may
reach the other. The driver has to classify the status of its *handshake*
response - a 401 there must disable the source, not look like a malformed
page - and the validation layer has to classify the status of every page.
Leaving the mapping inside the validation layer meant the driver grew a
second, cruder one, and the two disagreed on exactly the case that matters:
an auth failure misclassified as a contract error never disables a source, so
100 callers become 300 upstream auth attempts (WF2-B-01, WF2-H-06).

The HTTP status decides the family; the source's own ``code`` decides between
members of it.
"""

from __future__ import annotations

from typing import Any

from .drivers.base import DriverResponse
from .errors import (
    AuthenticationError,
    ConnectorError,
    PermissionDeniedError,
    RateLimitedError,
    SnapshotExpiredError,
    SourceContractError,
    SourceEpochChangedError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)
from .retry import parse_retry_after

__all__ = ["map_error_response", "source_error_code"]


def source_error_code(payload: Any) -> tuple[str | None, str | None, bool]:
    """Pull ``code``, ``message`` and ``retryable`` out of an error envelope.

    Returns ``(None, None, False)`` for anything that is not shaped like the
    documented envelope, so a proxy's HTML or a bare string cannot be mistaken
    for a source-declared code.
    """
    if not isinstance(payload, dict):
        return None, None, False
    error = payload.get("error")
    if not isinstance(error, dict):
        return None, None, False
    code = error.get("code")
    message = error.get("message")
    retryable = error.get("retryable")
    return (
        code if isinstance(code, str) else None,
        message if isinstance(message, str) else None,
        retryable is True,
    )


def map_error_response(
    response: DriverResponse,
    *,
    source_id: str,
    operation: str,
    request_id: str | None = None,
    retry_after_cap_s: float,
) -> ConnectorError:
    """Classify a non-success response into a typed error.

    The HTTP status decides the family; the source's own ``code`` decides
    between members of it. The 410 pair is the case that matters most:
    ``SNAPSHOT_EXPIRED`` means "your cursor's snapshot is gone, start again
    from the durable checkpoint", while ``RUN_REPLACED`` means "the dataset
    itself was replaced, reconcile identity". A consumer that treats them the
    same either re-syncs when it did not need to or carries on across a
    source reset (section 6.2, WF2-D-04, WF2-J-03).
    """
    status = response.status
    code, message, _ = source_error_code(response.payload)
    detail: dict[str, Any] = {"source_code": code or "<absent>", "path": response.path_name}
    text = message or "the source returned an error"

    common = {
        "source_id": source_id,
        "operation": operation,
        "request_id": request_id,
        "upstream_status": status,
        "latency_ms": response.upstream_latency_ms,
    }

    if status == 401:
        return AuthenticationError(
            "the source rejected the data-plane credential", details=detail, **common
        )
    if status == 403:
        return PermissionDeniedError(
            "the data-plane credential is not permitted on this resource; a connector "
            "holding an admin token or addressing a control plane also lands here",
            details=detail,
            **common,
        )
    if status == 404:
        return SourceContractError(
            "the configured data-plane path does not exist on this source",
            details=detail,
            **common,
        )
    if status == 410:
        if code == "RUN_REPLACED":
            return SourceEpochChangedError(
                "the source replaced the run this scan was reading", details=detail, **common
            )
        return SnapshotExpiredError(
            "the snapshot this cursor referred to has expired", details=detail, **common
        )
    if status == 429:
        retry_after = parse_retry_after(response.retry_after, cap_s=retry_after_cap_s)
        error = RateLimitedError(
            "the source is rate limiting this connector",
            details={
                **detail,
                "retry_after_header_present": response.retry_after is not None,
            },
            **common,
        )
        # ``None`` means the header was absent or unusable, and the retry
        # controller then falls back to its own backoff rather than treating
        # it as zero (WF2-D-03).
        error.retry_after_s = retry_after
        return error
    if status == 504:
        return UpstreamTimeoutError(
            "the source reported an upstream timeout", details=detail, **common
        )
    if status in (500, 502, 503):
        return UpstreamUnavailableError(
            "the source is unavailable", details=detail, **common
        )
    if status in (400, 409, 413, 422):
        return SourceContractError(
            "the source refused the request as malformed; the connector built it, so "
            "this is a contract defect rather than something to retry",
            details=detail,
            **common,
        )
    if status == 204:
        # A list endpoint answering "no content" is not an empty page: an
        # empty page has an envelope with provenance. Treating 204 as "no
        # data" would let a scan complete and a checkpoint advance over data
        # that was never read (WF2-D-10).
        return SourceContractError(
            "the source returned 204 where a page envelope was required; this is not "
            "an empty dataset and must not advance a checkpoint",
            details=detail,
            **common,
        )
    if 200 <= status < 300:
        return SourceContractError(
            "unexpected success status for a page request", details=detail, **common
        )
    return SourceContractError(
        "the source returned an unclassified status", details=detail, **common
    )
