"""HTTP adapter for the Project 02 connector.

Implements :class:`~trading_sync.application.ports.SyncSource` over the
endpoints Project 01 exposes through the connector. Three details carry real
weight.

**Field presence, not field value.** ``next_cursor`` is read with a
*membership* test. A body whose ``next_cursor`` is explicitly ``null`` is a
terminal page; a body missing the key is a schema error. Collapsing the two --
which ``body.get("next_cursor")`` would do -- lets a truncated response end a
scan and skip everything after it (WF3-044).

**The byte cap is enforced while reading.** The response is streamed and
abandoned as soon as it exceeds the cap, so an unexpected 50 MB body never
becomes resident memory. Checking afterwards would be checking too late
(WF3-147).

**No prefetch.** Each page is fetched when the consumer asks for the next one,
so the sink's commit paces the source. The generator form is what enforces
this; a background fetch loop would break the backpressure bound (WF3-146).
"""

from __future__ import annotations

import json
from typing import Any, AsyncIterator, Mapping

import httpx

from trading_sync.config import Settings
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity, RawPage, SourceIdentity, SourceKind
from trading_sync.errors import (
    AuthenticationError,
    AuthorizationError,
    OverloadedError,
    ResponseTooLargeError,
    SchemaError,
    SnapshotExpiredError,
    SourceContractError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)
from trading_sync.infrastructure.metrics import SyncMetrics

__all__ = ["HttpConnectorSource", "SUPPORTED_SCHEMA_VERSIONS"]

#: Page and meta bodies may carry a schema version. An unrecognised value is
#: refused rather than guessed at: a mapper applied to a schema it does not
#: know could read a renamed field as missing, or a repurposed one as its old
#: meaning (WF3-003).
SUPPORTED_SCHEMA_VERSIONS = frozenset({"1", "v1", "1.0"})

_ENTITY_PATHS = {
    Entity.ACCOUNTS: "/v1/accounts",
    Entity.POSITIONS: "/v1/positions",
    Entity.DEALS: "/v1/deals",
}


class HttpConnectorSource:
    """Reads pages from the connector, one page per consumer request."""

    def __init__(
        self,
        settings: Settings,
        *,
        client: httpx.AsyncClient | None = None,
        metrics: SyncMetrics | None = None,
    ) -> None:
        self._settings = settings
        self._metrics = metrics
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=settings.source.connector_endpoint,
            timeout=settings.source.request_timeout_seconds,
            headers=self._headers(settings),
        )

    @staticmethod
    def _headers(settings: Settings) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        token = settings.source.api_token
        if token is not None:
            headers["Authorization"] = f"Bearer {token.get_secret_value()}"
        return headers

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> HttpConnectorSource:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    # -- meta -----------------------------------------------------------------

    async def read_meta(self, source_id: str) -> SourceIdentity:
        body = await self._get_json("/v1/meta", params={})
        for field in ("run_id", "generation"):
            if field not in body:
                # Synthesising an identity would let two different simulator
                # runs share a watermark (WF3-001, WF3-002).
                raise SourceContractError(
                    "source meta is missing a required identity field", field=field
                )
        run_id = body["run_id"]
        generation = body["generation"]
        if not isinstance(run_id, str) or not run_id.strip():
            raise SourceContractError("source meta run_id is unusable")
        if isinstance(generation, bool) or not isinstance(generation, int):
            raise SourceContractError("source meta generation is not an integer")
        kind_value = body.get("source_kind", str(SourceKind.SIMULATOR))
        try:
            source_kind = SourceKind(kind_value)
        except ValueError:
            raise SourceContractError(
                "unknown source kind", source_kind=str(kind_value)
            ) from None
        committed = body.get("committed_seq", body.get("snapshot_seq", 0))
        if isinstance(committed, bool) or not isinstance(committed, int):
            raise SourceContractError("source meta committed_seq is not an integer")
        return SourceIdentity(
            source_id=source_id,
            source_kind=source_kind,
            run_id=run_id,
            generation=generation,
            committed_seq=committed,
            supports_incremental_deals=bool(
                body.get("supports_incremental_deals", source_kind is SourceKind.SIMULATOR)
            ),
        )

    # -- paged reads ----------------------------------------------------------

    def iter_accounts(self, source_id: str) -> AsyncIterator[RawPage]:
        return self._iter_pages(source_id, Entity.ACCOUNTS, {})

    def iter_positions(self, source_id: str) -> AsyncIterator[RawPage]:
        return self._iter_pages(source_id, Entity.POSITIONS, {})

    def iter_deals(self, source_id: str, after_seq: int) -> AsyncIterator[RawPage]:
        return self._iter_pages(source_id, Entity.DEALS, {"after_seq": after_seq})

    async def _iter_pages(
        self, source_id: str, entity: Entity, first_params: Mapping[str, Any]
    ) -> AsyncIterator[RawPage]:
        path = _ENTITY_PATHS[entity]
        params: dict[str, Any] = {
            "limit": self._settings.source.page_size,
            **first_params,
        }
        index = 0
        request_cursor: str | None = None
        while True:
            body = await self._get_json(path, params=params)
            page = self._to_raw_page(
                source_id, entity, body, index=index, request_cursor=request_cursor
            )
            yield page
            if page.next_cursor is None:
                return
            # Only now is the next request issued, so the consumer's commit
            # paces the source rather than the other way round.
            request_cursor = page.next_cursor
            params = {"cursor": request_cursor, "limit": self._settings.source.page_size}
            index += 1

    def _to_raw_page(
        self,
        source_id: str,
        entity: Entity,
        body: Mapping[str, Any],
        *,
        index: int,
        request_cursor: str | None,
    ) -> RawPage:
        for field in ("run_id", "generation", "snapshot_seq", "items"):
            if field not in body:
                raise SchemaError("page is missing a required field", field=field)
        items = body["items"]
        if not isinstance(items, list):
            raise SchemaError("page items is not an array", field="items")
        cursor_present = "next_cursor" in body
        next_cursor = body.get("next_cursor") if cursor_present else None
        if next_cursor is not None and not isinstance(next_cursor, str):
            raise SchemaError("next_cursor is not a string", field="next_cursor")
        return RawPage(
            source_id=source_id,
            entity=entity,
            run_id=str(body["run_id"]),
            generation=int(body["generation"]),
            snapshot_seq=int(body["snapshot_seq"]),
            page_index=index,
            items=tuple(items),
            cursor_present=cursor_present,
            next_cursor=next_cursor,
            filter_scope=str(body.get("filter_scope", FILTER_UNFILTERED)),
            request_cursor=request_cursor,
        )

    # -- transport ------------------------------------------------------------

    async def _get_json(self, path: str, *, params: Mapping[str, Any]) -> Mapping[str, Any]:
        try:
            async with self._client.stream("GET", path, params=dict(params)) as response:
                self._raise_for_status(response)
                payload = await self._read_capped(response)
        except httpx.TimeoutException as exc:
            self._count_error("UPSTREAM_TIMEOUT")
            raise UpstreamTimeoutError("connector request timed out", path=path) from exc
        except httpx.TransportError as exc:
            self._count_error("UPSTREAM_UNAVAILABLE")
            raise UpstreamUnavailableError(
                "connector transport failure", path=path
            ) from exc
        try:
            body = json.loads(payload)
        except ValueError as exc:
            self._count_error("SCHEMA_ERROR")
            raise SchemaError("connector response is not valid JSON", path=path) from exc
        if not isinstance(body, dict):
            raise SchemaError("connector response is not an object", path=path)
        self._require_known_schema(body)
        return body

    def _require_known_schema(self, body: Mapping[str, Any]) -> None:
        """Refuse a payload whose declared schema version we do not know.

        Absent is fine -- the pinned contract has no version field today, and
        requiring one would break against the current connector. A *present but
        unknown* version is the dangerous case, because it means the shape may
        have changed in ways the mapper cannot see.
        """
        declared = body.get("schema_version")
        if declared is None:
            return
        if str(declared) not in SUPPORTED_SCHEMA_VERSIONS:
            self._count_error("SCHEMA_ERROR")
            raise SchemaError(
                "connector declared an unsupported schema version",
                schema_version=str(declared),
                supported=sorted(SUPPORTED_SCHEMA_VERSIONS),
            )

    async def _read_capped(self, response: httpx.Response) -> bytes:
        """Read the body, abandoning it the moment it exceeds the cap."""
        cap = self._settings.source.max_response_bytes
        chunks: list[bytes] = []
        total = 0
        async for chunk in response.aiter_bytes():
            total += len(chunk)
            if total > cap:
                self._count_error("RESPONSE_TOO_LARGE")
                raise ResponseTooLargeError(
                    "connector response exceeded the byte cap",
                    max_bytes=cap,
                    read_bytes=total,
                )
            chunks.append(chunk)
        return b"".join(chunks)

    def _raise_for_status(self, response: httpx.Response) -> None:
        status = response.status_code
        if status < 400:
            return
        self._count_error(f"HTTP_{status}")
        if status == 401:
            raise AuthenticationError("connector rejected credentials")
        if status == 403:
            raise AuthorizationError("connector denied access")
        if status == 410:
            # The snapshot or cursor aged out. Recoverable, but only as a fresh
            # scan from the durable watermark (WF3-037).
            raise SnapshotExpiredError("connector reports the snapshot expired")
        if status == 429:
            raise OverloadedError(
                "connector is rate limiting",
                retry_after_seconds=_retry_after(response.headers.get("Retry-After")),
            )
        if status >= 500:
            raise UpstreamUnavailableError("connector returned a server error", status=status)
        raise SchemaError("connector returned an unexpected status", status=status)

    def _count_error(self, code: str) -> None:
        if self._metrics is not None:
            self._metrics.upstream_errors_total.labels(error_code=code).inc()


def _retry_after(value: str | None) -> float | None:
    """Parse ``Retry-After`` seconds, ignoring anything unparseable.

    The value is only a hint; the retry policy clamps it, so a hostile or
    nonsensical header cannot park a worker (WF3-136).
    """
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        return None
    return seconds if seconds > 0 else None
