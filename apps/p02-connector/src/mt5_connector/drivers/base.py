"""The blocking driver contract, and the body decoding every driver shares.

A driver's job stops at "I obtained bytes and turned them into Python
objects". It does not interpret them: provenance, ordering and DTO
construction happen on the async side where the scan context lives
(:mod:`mt5_connector.validation`). Keeping the split there means the
expensive, dangerous work - socket reads, size limits, JSON parsing - happens
off the event loop, while the decisions that need scan state happen where that
state is single-threaded.

The decoding rules below are the ones that must run **before** a payload is
trusted, and in this order:

1. Content type, then declared length, then actual bytes read. Checking the
   declared ``Content-Length`` saves reading a 20 MiB body, but it is a claim,
   not a fact, so the streaming read is capped too (WF2-D-07, WF2-M-04).
2. Bracket depth, scanned over the raw bytes before parsing. ``json.loads``
   has no depth limit and 10,000 levels of nesting is a ``RecursionError`` -
   or worse, a C-level stack overflow (WF2-D-08).
3. Parse with duplicate keys and non-finite literals refused. ``json.loads``
   accepts ``NaN``, ``Infinity`` and a repeated key (last one wins) by
   default; all three are ambiguous payloads and none may become a DTO
   (WF2-D-09, WF2-L-02).

A ``DriverError`` is deliberately absent. Transport failures raise the
connector's own typed errors directly, because the classification - connect
refused versus read timeout versus TLS failure - is knowable only here, and
re-deriving it from a generic wrapper one layer up loses information.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Final, Protocol

from ..errors import SourceContractError

__all__ = [
    "DriverIdentity",
    "DriverResponse",
    "BlockingDriverProtocol",
    "guard_json_depth",
    "decode_json_body",
    "ACCEPTED_CONTENT_TYPES",
]

#: The only content types a data-plane response may carry. An HTML error page
#: from a proxy arrives with HTTP 200 and ``text/html``, and refusing it on
#: type is cheaper and more honest than discovering it has no ``items`` key
#: (WF2-D-06).
ACCEPTED_CONTENT_TYPES: Final[frozenset[str]] = frozenset(
    {"application/json", "application/problem+json"}
)

#: Literals ``json.loads`` accepts that this contract does not.
_NON_FINITE: Final[frozenset[str]] = frozenset({"NaN", "Infinity", "-Infinity"})


@dataclass(frozen=True, slots=True)
class DriverIdentity:
    """What the driver learned about the source when it opened the session.

    ``run_id`` and ``generation`` are ``None`` for any source that does not
    publish them. The driver reports absence; it never substitutes a value,
    because a fabricated identity would make epoch fencing silently useless
    (WF2-R-07).
    """

    source_kind: str
    contract_version: str | None
    run_id: str | None = None
    generation: int | None = None
    #: The capability document exactly as received, for the capability mapper
    #: and for evidence files.
    raw_meta: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DriverResponse:
    """One completed exchange, whatever its status.

    A 4xx or 5xx is a *result*, not an exception: the status and the body both
    carry information the error mapper needs, and raising here would force
    that mapper to reconstruct them. Only a failure to complete the exchange
    at all raises.
    """

    status: int
    payload: Any
    byte_count: int
    content_type: str | None
    retry_after: str | None = None
    #: Which allowlisted path produced this. A name from
    #: :data:`mt5_connector.config.DATA_PLANE_PATHS`, never a full URL, so a
    #: query string with a token in it cannot reach a log through this field.
    path_name: str = ""
    #: Server-side elapsed time where the transport reports it. Advisory.
    upstream_latency_ms: float | None = None


class BlockingDriverProtocol(Protocol):
    """The synchronous interface a worker owns.

    Every method may block for an unbounded time. The caller never assumes
    otherwise: the actor dispatches these on a worker it owns and treats the
    session slot as occupied until the method actually returns
    (section 5.1).

    These names are the connector's own. They are not Manager SDK method
    names, and a real driver must map them from the vendor's documented API
    rather than assume a correspondence (section 16.4).
    """

    def open(self) -> DriverIdentity:
        """Establish the session and read source identity. Blocking."""
        ...

    def fetch_meta(self) -> DriverResponse: ...

    def fetch_accounts(self, *, cursor: str | None, limit: int) -> DriverResponse: ...

    def fetch_positions(
        self,
        *,
        cursor: str | None,
        limit: int,
        account_id: str | None = None,
        symbol: str | None = None,
    ) -> DriverResponse: ...

    def fetch_deals(
        self,
        *,
        cursor: str | None,
        limit: int,
        after_seq: int | None = None,
        account_id: str | None = None,
        symbol: str | None = None,
    ) -> DriverResponse: ...

    def close(self) -> None:
        """Release the session. Must be safe to call twice (WF2-E-09)."""
        ...


def guard_json_depth(body: bytes, *, max_depth: int, path_name: str = "") -> None:
    """Refuse a body nested deeper than ``max_depth``, before parsing it.

    A byte scan rather than a parse: the point is to decide *without* building
    the object graph that would exhaust the stack. String contents are skipped
    so a bracket inside a symbol name is not counted, and an escape is
    consumed with its next byte so an escaped quote does not end the string.
    """
    depth = 0
    in_string = False
    escaped = False
    for byte in body:
        if in_string:
            if escaped:
                escaped = False
            elif byte == 0x5C:  # backslash
                escaped = True
            elif byte == 0x22:  # quote
                in_string = False
            continue
        if byte == 0x22:
            in_string = True
        elif byte in (0x7B, 0x5B):  # { [
            depth += 1
            if depth > max_depth:
                raise SourceContractError(
                    "response JSON is nested beyond the permitted depth and was "
                    "refused before parsing",
                    details={
                        "max_json_depth": max_depth,
                        "path": path_name,
                        "reason": "nesting_depth",
                    },
                )
        elif byte in (0x7D, 0x5D):  # } ]
            depth -= 1


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    seen: set[str] = set()
    for key, _ in pairs:
        if key in seen:
            # "Last one wins" is a silent choice between two different
            # documents. Which one was meant is unknowable, so neither is used.
            raise SourceContractError(
                "response JSON contains a duplicate object key, which makes the "
                "payload ambiguous",
                details={"field": key, "reason": "duplicate_key"},
            )
        seen.add(key)
    return dict(pairs)


def _reject_non_finite(literal: str) -> Any:
    raise SourceContractError(
        "response JSON contains a non-finite numeric literal",
        details={"literal": literal if literal in _NON_FINITE else "unknown", "reason": "non_finite"},
    )


def decode_json_body(
    body: bytes,
    *,
    content_type: str | None,
    max_bytes: int,
    max_depth: int,
    status: int,
    path_name: str = "",
) -> Any:
    """Turn response bytes into Python objects, or refuse them.

    Ordered so that each check is cheaper than the one it protects.
    """
    if len(body) > max_bytes:
        raise SourceContractError(
            "response body exceeds the permitted size and was refused before parsing",
            upstream_status=status,
            details={"max_response_bytes": max_bytes, "observed_bytes": len(body), "path": path_name},
        )

    base_type = (content_type or "").split(";", 1)[0].strip().lower()
    if base_type not in ACCEPTED_CONTENT_TYPES:
        raise SourceContractError(
            "response content type is not JSON",
            upstream_status=status,
            details={
                "observed_content_type": base_type or "<absent>",
                "accepted": sorted(ACCEPTED_CONTENT_TYPES),
                "path": path_name,
            },
        )

    if not body.strip():
        raise SourceContractError(
            "response body is empty where a JSON document was required",
            upstream_status=status,
            details={"path": path_name, "reason": "empty_body"},
        )

    guard_json_depth(body, max_depth=max_depth, path_name=path_name)

    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise SourceContractError(
            "response body is not valid UTF-8",
            upstream_status=status,
            details={"path": path_name, "reason": "encoding"},
        ) from exc

    try:
        return json.loads(
            text,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_non_finite,
        )
    except SourceContractError:
        raise
    except json.JSONDecodeError as exc:
        # A truncated body lands here. The position is useful for diagnosis;
        # the content around it is not included, because it may be a financial
        # record (INV-15).
        raise SourceContractError(
            "response body is not well-formed JSON",
            upstream_status=status,
            details={
                "path": path_name,
                "reason": "malformed_json",
                "error_position": exc.pos,
                "observed_bytes": len(body),
            },
        ) from exc
