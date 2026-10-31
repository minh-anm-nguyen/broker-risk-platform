"""The source boundary: reading independent evidence from P01 via P02.

Design rules enforced here (SYSTEM_DESIGN sections 2.1, 3.2, 7.4):

* **One snapshot per scan.** ``iter_pages`` pins ``(run_id, generation,
  snapshot_seq)`` from page 1 and validates every later page against it. A
  source that changes any of the three mid-scan ends the scan; it does not
  quietly contribute rows to a manifest that claims a single cut (section 2.1
  invariant).
* **Cursors are opaque and must not be edited.** The scan follows
  ``next_cursor`` verbatim and never re-derives one by combining a cursor with
  a fresh ``after_seq`` or filter, which section 2.1 forbids outright.
* **A repeated cursor is a cycle, not progress.** Digests of every cursor seen
  are kept so a source that loops a page forever is stopped by
  ``SOURCE_CURSOR_CYCLE`` instead of filling the manifest until a quota
  trips.
* **Retries are bounded and only for transport.** A 429/503 is retried with
  jittered backoff; a 410 is *never* retried, because the snapshot it refers
  to no longer exists and resuming the cursor would stitch two cuts together
  (section 2.1 "no self-resume of an expired cursor").
* **``next_cursor=null`` ends this snapshot, not the world.** The terminal flag
  means "this scan saw every row at S", never "no further events will exist".

The retry/timeout/DTO-validation duties here are the ones Project 02 owns in
the full architecture. Until P02 exists, this adapter performs them and is the
place a real Manager-SDK driver would slot in behind the same Protocol.
"""

from __future__ import annotations

import hashlib
import random
import time
from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Protocol

import httpx

from ..dto import SourceCapabilities, SourceIdentity, SourcePage
from ..errors import (
    DataIntegrityError,
    ReasonCode,
    SnapshotExpiredError,
    SourceEpochChangedError,
    SourceError,
)

#: P01 path per entity (section 2.1).
_ENTITY_PATHS: Mapping[str, str] = {
    "accounts": "/v1/accounts",
    "positions": "/v1/positions",
    "deals": "/v1/deals",
}

#: Statuses worth a bounded retry: the request may succeed unchanged.
_RETRYABLE_STATUS: frozenset[int] = frozenset({429, 500, 502, 503, 504})


def cursor_digest(cursor: str | None) -> str | None:
    """Short digest of an opaque cursor, for receipts and cycle detection.

    The cursor itself is signed and may encode internal state, so only its
    digest is persisted (section 12.2 secret handling).
    """
    if cursor is None:
        return None
    return hashlib.sha256(cursor.encode("utf-8")).hexdigest()


class SourcePort(Protocol):
    """What reconciliation needs from a source. Drivers implement this."""

    def read_identity(self) -> SourceIdentity: ...

    def read_capabilities(self) -> SourceCapabilities: ...

    def iter_pages(
        self, entity: str, *, scope: str = "UNFILTERED", page_size: int | None = None
    ) -> Iterator[SourcePage]: ...


class P01HttpSource:
    """Driver for the Project 01 simulator's HTTP data plane.

    ``client`` is injected so tests can bind an in-process ASGI transport to
    the real P01 application, which keeps the contract tests honest: they
    exercise P01's actual pagination, cursor signing and fault injection
    rather than a mock of it.
    """

    def __init__(
        self,
        *,
        client: httpx.Client,
        source_id: str,
        max_page_size: int = 500,
        max_retries: int = 3,
        retry_base_delay_s: float = 0.05,
        sleep=time.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self._client = client
        self._source_id = source_id
        self._max_page_size = max_page_size
        self._max_retries = max_retries
        self._retry_base_delay_s = retry_base_delay_s
        self._sleep = sleep
        self._rng = rng or random.Random(0)

    # ---- transport -------------------------------------------------------

    def _get(self, path: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                response = self._client.get(path, params=dict(params or {}))
            except httpx.HTTPError as exc:
                last_error = exc
                if attempt >= self._max_retries:
                    raise SourceError(
                        "source transport failed after bounded retries",
                        reason=ReasonCode.SOURCE_TRANSPORT_FAILED,
                        path=path,
                        attempts=attempt + 1,
                        detail=str(exc),
                    ) from exc
                self._backoff(attempt)
                continue

            if response.status_code == 410:
                # Not retryable by contract: the snapshot is gone, so the only
                # correct move is to abandon this manifest (section 2.1).
                raise SnapshotExpiredError(
                    "source snapshot expired mid-scan",
                    path=path,
                    status=410,
                    body=_safe_body(response),
                )
            if response.status_code == 409:
                raise SourceEpochChangedError(
                    "source reports the run was replaced",
                    path=path,
                    status=409,
                    body=_safe_body(response),
                )
            if response.status_code in _RETRYABLE_STATUS:
                last_error = SourceError(
                    "source returned a retryable status",
                    reason=ReasonCode.SOURCE_TRANSPORT_FAILED,
                    status=response.status_code,
                )
                if attempt >= self._max_retries:
                    raise SourceError(
                        "source kept returning a retryable status",
                        reason=ReasonCode.SOURCE_TRANSPORT_FAILED,
                        path=path,
                        status=response.status_code,
                        attempts=attempt + 1,
                    )
                self._backoff(attempt)
                continue
            if response.status_code >= 400:
                raise SourceError(
                    "source rejected the request",
                    reason=ReasonCode.SOURCE_TRANSPORT_FAILED,
                    path=path,
                    status=response.status_code,
                    body=_safe_body(response),
                )

            content_type = response.headers.get("content-type", "")
            if "application/json" not in content_type:
                raise DataIntegrityError(
                    "source returned a non-JSON content type",
                    path=path,
                    content_type=content_type,
                )
            try:
                payload = response.json()
            except ValueError as exc:
                raise DataIntegrityError(
                    "source returned a malformed JSON body", path=path, detail=str(exc)
                ) from exc
            if not isinstance(payload, dict):
                raise DataIntegrityError("source response was not a JSON object", path=path)
            return payload

        raise SourceError(  # pragma: no cover - loop always returns or raises
            "source transport exhausted", reason=ReasonCode.SOURCE_TRANSPORT_FAILED, detail=str(last_error)
        )

    def _backoff(self, attempt: int) -> None:
        delay = self._retry_base_delay_s * (2**attempt)
        self._sleep(delay * (0.5 + self._rng.random() / 2))

    # ---- identity --------------------------------------------------------

    def _meta(self) -> dict[str, Any]:
        return self._get("/v1/meta")

    def read_identity(self) -> SourceIdentity:
        payload = self._meta()
        run_id = payload.get("run_id")
        generation = payload.get("generation")
        if not isinstance(run_id, str) or not isinstance(generation, int):
            raise SourceError(
                "source has no active run identity to audit",
                reason=ReasonCode.UNSUPPORTED_CAPABILITY,
                run_id=run_id,
                generation=generation,
            )
        return SourceIdentity(
            source_id=self._source_id,
            run_id=run_id,
            generation=generation,
            snapshot_seq=int(payload.get("snapshot_seq") or 0),
            account_count=int(payload.get("account_count") or 0),
            deal_count=int(payload.get("deal_count") or 0),
            open_position_count=int(payload.get("open_position_count") or 0),
        )

    def read_capabilities(self) -> SourceCapabilities:
        return SourceCapabilities.from_meta(self._meta())

    # ---- paging ----------------------------------------------------------

    def iter_pages(
        self, entity: str, *, scope: str = "UNFILTERED", page_size: int | None = None
    ) -> Iterator[SourcePage]:
        if scope != "UNFILTERED":
            raise DataIntegrityError(
                "only an unfiltered scan can support a full-set comparison",
                reason=ReasonCode.SOURCE_SCAN_FILTERED,
                scope=scope,
            )
        try:
            path = _ENTITY_PATHS[entity]
        except KeyError:
            raise SourceError(
                "unsupported entity", reason=ReasonCode.UNSUPPORTED_CAPABILITY, entity=entity
            ) from None

        limit = min(page_size or self._max_page_size, self._max_page_size)
        pinned: SourcePage | None = None
        cursor: str | None = None
        seen_cursors: set[str] = set()
        index = 0

        while True:
            if cursor is None:
                # A deals scan starts at after_seq=0, which is exclusive, so
                # it includes every deal from sequence 1 (section 2.1).
                params: dict[str, Any] = {"limit": limit}
                if entity == "deals":
                    params["after_seq"] = 0
            else:
                params = {"cursor": cursor}

            payload = self._get(path, params)
            page = _parse_page(index, payload, entity=entity)
            page.validate_envelope(pinned, max_page_size=self._max_page_size)
            if pinned is None:
                pinned = page
            yield page

            if page.is_terminal:
                return
            next_cursor = page.next_cursor
            assert next_cursor is not None
            digest = cursor_digest(next_cursor)
            assert digest is not None
            if digest in seen_cursors:
                raise DataIntegrityError(
                    "source re-issued a cursor already seen in this scan",
                    reason=ReasonCode.SOURCE_CURSOR_CYCLE,
                    page_index=index,
                )
            seen_cursors.add(digest)
            cursor = next_cursor
            index += 1


def _safe_body(response: httpx.Response) -> str:
    """A bounded, non-secret excerpt for diagnostics."""
    try:
        return response.text[:256]
    except Exception:  # pragma: no cover - defensive
        return "<unreadable>"


def _parse_page(index: int, payload: Mapping[str, Any], *, entity: str) -> SourcePage:
    """Validate the page envelope before any row is trusted."""
    for field in ("run_id", "generation", "snapshot_seq"):
        if field not in payload or payload[field] is None:
            raise DataIntegrityError(
                "page envelope is missing mandatory provenance",
                page_index=index,
                field=field,
                entity=entity,
            )
    items = payload.get("items")
    if not isinstance(items, list):
        raise DataIntegrityError("page items is not a list", page_index=index, entity=entity)
    for item in items:
        if not isinstance(item, dict):
            raise DataIntegrityError(
                "page item is not an object", page_index=index, entity=entity
            )
    next_cursor = payload.get("next_cursor")
    if next_cursor is not None and not isinstance(next_cursor, str):
        raise DataIntegrityError("next_cursor is not a string", page_index=index, entity=entity)
    try:
        generation = int(payload["generation"])
        snapshot_seq = int(payload["snapshot_seq"])
    except (TypeError, ValueError) as exc:
        raise DataIntegrityError(
            "page envelope provenance is not numeric", page_index=index, detail=str(exc)
        ) from exc
    return SourcePage(
        page_index=index,
        run_id=str(payload["run_id"]),
        generation=generation,
        snapshot_seq=snapshot_seq,
        items=items,
        next_cursor=next_cursor,
    )


class FakeSource:
    """Programmable in-memory source for fixtures and property tests.

    Exists so a test can construct states P01 cannot be driven into easily --
    a source that truncates while claiming terminal (IT04-10), one that
    changes generation between two pages (F-07), or one that returns an item
    whose money field is a float. Fixtures that *can* be built on real P01
    use ``P01HttpSource`` instead, because a fake agreeing with a fake proves
    nothing (section 10.1).
    """

    def __init__(
        self,
        *,
        source_id: str = "sim_local",
        run_id: str = "run_0001",
        generation: int = 1,
        snapshot_seq: int = 100,
        records: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
        capabilities: SourceCapabilities | None = None,
        page_size: int = 500,
    ) -> None:
        self.source_id = source_id
        self.run_id = run_id
        self.generation = generation
        self.snapshot_seq = snapshot_seq
        self.records: dict[str, list[dict[str, Any]]] = {
            entity: [dict(row) for row in rows]
            for entity, rows in (records or {}).items()
        }
        self._capabilities = capabilities or SourceCapabilities()
        self.page_size = page_size
        #: Hooks a test installs to misbehave at a chosen page index.
        self.fail_at_page: dict[int, Exception] = {}
        self.mutate_page = None
        self.claim_terminal_at: int | None = None
        self.pages_served = 0

    def read_identity(self) -> SourceIdentity:
        return SourceIdentity(
            source_id=self.source_id,
            run_id=self.run_id,
            generation=self.generation,
            snapshot_seq=self.snapshot_seq,
            account_count=len(self.records.get("accounts", [])),
            deal_count=len(self.records.get("deals", [])),
            open_position_count=len(self.records.get("positions", [])),
        )

    def read_capabilities(self) -> SourceCapabilities:
        return self._capabilities

    def iter_pages(
        self, entity: str, *, scope: str = "UNFILTERED", page_size: int | None = None
    ) -> Iterator[SourcePage]:
        if scope != "UNFILTERED":
            raise DataIntegrityError(
                "only an unfiltered scan can support a full-set comparison",
                reason=ReasonCode.SOURCE_SCAN_FILTERED,
                scope=scope,
            )
        rows = self.records.get(entity, [])
        # ``self.page_size`` is this source's own maximum, mirroring P01's
        # ``max_page_size``: a caller may ask for less but never for more.
        size = min(page_size or self.page_size, self.page_size)
        chunks = [rows[i : i + size] for i in range(0, len(rows), size)] or [[]]
        pinned: SourcePage | None = None
        for index, chunk in enumerate(chunks):
            if index in self.fail_at_page:
                raise self.fail_at_page[index]
            terminal = index == len(chunks) - 1
            if self.claim_terminal_at is not None and index == self.claim_terminal_at:
                terminal = True
            page = SourcePage(
                page_index=index,
                run_id=self.run_id,
                generation=self.generation,
                snapshot_seq=self.snapshot_seq,
                items=list(chunk),
                next_cursor=None if terminal else f"cursor-{index}",
            )
            if self.mutate_page is not None:
                page = self.mutate_page(index, page)
            page.validate_envelope(pinned, max_page_size=self.page_size)
            if pinned is None:
                pinned = page
            self.pages_served += 1
            yield page
            if page.is_terminal:
                return
