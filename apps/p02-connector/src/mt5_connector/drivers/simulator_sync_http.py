"""Blocking HTTP driver for the Project 01 data plane.

Synchronous **on purpose**. An async client here would keep the event loop
responsive for free, and the thing this connector has to demonstrate is that a
call which *cannot* yield - a native SDK call, in the deployment this stands
in for - is contained anyway. So this driver blocks the thread it runs on, and
the actor is what keeps that off the event loop (design section 3.2).

Safety properties implemented here rather than configured:

* **Redirects are never followed** (WF2-D-11). A 302 to another host would
  resend the ``Authorization`` header to whoever answered. A 3xx is a contract
  error.
* **The address is re-checked after resolution** (WF2-C-11, WF2-N-08). An
  allowlisted hostname that resolves to ``169.254.169.254`` is a DNS rebinding
  attempt, and the allowlist on the *name* does not catch it.
* **The body is capped while streaming** (WF2-M-04). ``Content-Length`` is
  consulted first because it saves reading the body at all, but it is the
  sender's claim; the read stops at the cap regardless of what was declared.
* **Nothing credential-bearing is ever put in a URL or an exception.** The
  token travels in a header, request logging records a path *name*, and
  transport exceptions are replaced with typed errors whose details hold no
  URL (WF2-B-06, INV-15).
"""

from __future__ import annotations

import ipaddress
import socket
import time
from typing import Any, Final
from urllib.parse import urlsplit

import httpx

from ..config import Settings
from ..errors import (
    ConfigurationError,
    SourceContractError,
    TransportError,
    UpstreamTimeoutError,
)
from .base import DriverIdentity, DriverResponse, decode_json_body

__all__ = ["SimulatorSyncHttpDriver"]

#: Address ranges a data source must never resolve to. Cloud instance
#: metadata lives at 169.254.169.254 and is the classic SSRF target; the
#: others are ranges a broker endpoint has no business being in.
_FORBIDDEN_NETWORKS: Final[tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]] = (
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("::ffff:169.254.0.0/112"),
    ipaddress.ip_network("fe80::/10"),
)

#: TLS contexts, cached by verification mode.
#:
#: Building one loads and parses the whole CA bundle, which measured several
#: seconds on a Windows host. A new ``httpx.Client`` per reconnect would pay
#: that every time, so a flapping source would spend most of its reconnect
#: budget constructing identical contexts - and the budget exists to bound
#: *network* time, not local setup. The context is immutable once built and is
#: only read by the client, so sharing it across sessions is safe.
_SSL_CONTEXTS: dict[bool, Any] = {}


def _ssl_context(verify: bool) -> Any:
    context = _SSL_CONTEXTS.get(verify)
    if context is None:
        context = httpx.create_ssl_context(verify=verify)
        _SSL_CONTEXTS[verify] = context
    return context


def warm_tls_context(verify: bool = True) -> None:
    """Build the TLS context now, outside any request budget.

    Called during process start-up. Without it the first connect pays the
    whole CA-bundle load, and a connect budget sized for *network* latency
    then expires on local initialisation - which looks exactly like an
    unreachable source and is not one.
    """
    _ssl_context(verify)


class SimulatorSyncHttpDriver:
    """Owns one persistent HTTP session. Single-threaded by contract.

    The actor guarantees one call at a time, so nothing here needs a lock.
    That is an invariant of the caller, not an accident: ``httpx.Client`` is
    not safe to drive concurrently from several threads, and this driver is
    written assuming it never is (ADR-202).
    """

    source_kind = "simulator"

    def __init__(self, settings: Settings, *, token: str, transport: Any | None = None) -> None:
        self._settings = settings
        self._token = token
        self._client: httpx.Client | None = None
        self._transport = transport
        self._closed = False
        #: Which thread opened the session. Checked on every call so a driver
        #: used from the wrong thread fails loudly instead of corrupting a
        #: connection pool (WF2-E-06).
        self._owner_thread: int | None = None
        self.calls_made = 0

    # -- lifecycle ----------------------------------------------------------

    def open(self) -> DriverIdentity:
        import threading

        self._owner_thread = threading.get_ident()
        self._assert_address_allowed()
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
            "User-Agent": "mt5-connector/1.0",
        }
        timeout = httpx.Timeout(
            connect=self._settings.connect_timeout_ms / 1000.0,
            read=self._settings.upstream_read_timeout_ms / 1000.0,
            write=self._settings.upstream_read_timeout_ms / 1000.0,
            pool=self._settings.connect_timeout_ms / 1000.0,
        )
        self._client = httpx.Client(
            headers=headers,
            timeout=timeout,
            # A redirect would resend the bearer token to whatever host the
            # source named. There is no scenario where that is wanted.
            follow_redirects=False,
            verify=_ssl_context(self._settings.tls_verify),
            transport=self._transport,
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
        )
        response = self.fetch_meta()
        if not 200 <= response.status < 300:
            # Classified through the same mapper the page path uses. A 401
            # here must surface as an authentication failure so the supervisor
            # disables the source; reporting it as a generic contract error
            # would leave it retrying a credential that will never work
            # (WF2-B-01, WF2-H-06).
            from ..status_mapping import map_error_response

            raise map_error_response(
                response,
                source_id=self._settings.source_id,
                operation="connect",
                retry_after_cap_s=self._settings.retry_after_cap_ms / 1000.0,
            )
        payload = response.payload
        if not isinstance(payload, dict):
            raise SourceContractError(
                "the capability document is not a JSON object",
                upstream_status=response.status,
                details={"path": "meta"},
            )
        run_id = payload.get("run_id")
        generation = payload.get("generation")
        capabilities = payload.get("capabilities")
        contract_version = None
        if isinstance(capabilities, dict):
            value = capabilities.get("api_version")
            contract_version = value if isinstance(value, str) else None
        return DriverIdentity(
            source_kind=self.source_kind,
            contract_version=contract_version,
            run_id=run_id if isinstance(run_id, str) else None,
            generation=generation if isinstance(generation, int) and not isinstance(generation, bool) else None,
            raw_meta=payload,
        )

    def close(self) -> None:
        self._closed = True
        client, self._client = self._client, None
        if client is not None:
            # Explicit close rather than relying on finalisation: a session
            # left to the garbage collector keeps a socket and a file
            # descriptor, and a reconnect loop then leaks one per cycle
            # (WF2-M-07).
            client.close()

    # -- reads --------------------------------------------------------------

    def fetch_meta(self) -> DriverResponse:
        return self._get("meta", {})

    def fetch_accounts(self, *, cursor: str | None, limit: int) -> DriverResponse:
        return self._get("accounts", self._page_params(cursor, limit))

    def fetch_positions(
        self,
        *,
        cursor: str | None,
        limit: int,
        account_id: str | None = None,
        symbol: str | None = None,
    ) -> DriverResponse:
        params = self._page_params(cursor, limit)
        if cursor is None:
            self._add_optional(params, "account_id", account_id)
            self._add_optional(params, "symbol", symbol)
        return self._get("positions", params)

    def fetch_deals(
        self,
        *,
        cursor: str | None,
        limit: int,
        after_seq: int | None = None,
        account_id: str | None = None,
        symbol: str | None = None,
    ) -> DriverResponse:
        params = self._page_params(cursor, limit)
        if cursor is None:
            if after_seq is not None:
                params["after_seq"] = str(after_seq)
            self._add_optional(params, "account_id", account_id)
            self._add_optional(params, "symbol", symbol)
        return self._get("deals", params)

    @staticmethod
    def _page_params(cursor: str | None, limit: int) -> dict[str, str]:
        if cursor is not None:
            # A cursor travels alone. The source pinned the filters into it,
            # and sending a filter alongside is a different scan - which the
            # source answers with 400 rather than guessing (WF2-I-05).
            return {"cursor": cursor}
        return {"limit": str(limit)}

    @staticmethod
    def _add_optional(params: dict[str, str], name: str, value: str | None) -> None:
        if value is not None:
            params[name] = value

    # -- transport ----------------------------------------------------------

    def _get(self, path_name: str, params: dict[str, str]) -> DriverResponse:
        import threading

        if self._closed or self._client is None:
            raise TransportError(
                "the driver session is closed",
                details={"path": path_name, "reason": "session_closed"},
            )
        if self._owner_thread is not None and threading.get_ident() != self._owner_thread:
            # The session belongs to the worker that opened it. A call from
            # elsewhere means the actor's ownership rule has been broken, and
            # continuing would make the resulting corruption look like a
            # source bug.
            raise ConfigurationError(
                "the driver session was called from a thread that does not own it",
                details={"path": path_name, "reason": "thread_ownership"},
            )

        url = self._settings.url_for(path_name)
        started = time.monotonic()
        self.calls_made += 1
        try:
            with self._client.stream("GET", url, params=params) as response:
                if 300 <= response.status_code < 400:
                    raise SourceContractError(
                        "the source answered with a redirect, which is not followed "
                        "because it would resend the credential to another host",
                        upstream_status=response.status_code,
                        details={"path": path_name, "reason": "redirect"},
                    )
                declared = response.headers.get("content-length")
                if declared is not None:
                    try:
                        if int(declared) > self._settings.max_response_bytes:
                            # Refused here, before a single body byte is read.
                            raise SourceContractError(
                                "the source declared a response larger than the "
                                "permitted size; the body was not read",
                                upstream_status=response.status_code,
                                details={
                                    "path": path_name,
                                    "declared_bytes": int(declared),
                                    "max_response_bytes": self._settings.max_response_bytes,
                                },
                            )
                    except ValueError as exc:
                        raise SourceContractError(
                            "the source sent a non-numeric Content-Length",
                            upstream_status=response.status_code,
                            details={"path": path_name, "reason": "bad_content_length"},
                        ) from exc

                body = self._read_capped(response, path_name)
                latency_ms = (time.monotonic() - started) * 1000.0
                status = response.status_code
                content_type = response.headers.get("content-type")
                retry_after = response.headers.get("retry-after")
        except SourceContractError:
            raise
        except httpx.TimeoutException as exc:
            raise UpstreamTimeoutError(
                "the upstream read did not complete inside the per-call timeout",
                details={
                    "path": path_name,
                    "read_timeout_ms": self._settings.upstream_read_timeout_ms,
                    "transport_error": type(exc).__name__,
                },
            ) from exc
        except httpx.TransportError as exc:
            # One typed error for every way the exchange failed to complete.
            # The exception *type* is recorded; its string is not, because
            # httpx puts the full URL in some of them (WF2-B-06).
            raise TransportError(
                "the upstream exchange did not complete",
                details={"path": path_name, "transport_error": type(exc).__name__},
            ) from exc

        payload: Any = None
        if _expects_body(status):
            payload = decode_json_body(
                body,
                content_type=content_type,
                max_bytes=self._settings.max_response_bytes,
                max_depth=self._settings.max_json_depth,
                status=status,
                path_name=path_name,
            )
        return DriverResponse(
            status=status,
            payload=payload,
            byte_count=len(body),
            content_type=content_type,
            retry_after=retry_after,
            path_name=path_name,
            upstream_latency_ms=latency_ms,
        )

    def _read_capped(self, response: httpx.Response, path_name: str) -> bytes:
        """Read the body, stopping at the cap.

        Chunk by chunk, so a sender that lies about ``Content-Length`` or omits
        it entirely still cannot make this allocate without bound - including
        the drip-feed case where every chunk is tiny but the stream never ends
        (WF2-C-09, WF2-N-09).
        """
        cap = self._settings.max_response_bytes
        chunks: list[bytes] = []
        total = 0
        for chunk in response.iter_bytes():
            total += len(chunk)
            if total > cap:
                raise SourceContractError(
                    "the response body exceeded the permitted size while streaming "
                    "and the read was abandoned",
                    upstream_status=response.status_code,
                    details={
                        "path": path_name,
                        "max_response_bytes": cap,
                        "observed_bytes": total,
                    },
                )
            chunks.append(chunk)
        return b"".join(chunks)

    # -- address policy -----------------------------------------------------

    def _assert_address_allowed(self) -> None:
        parts = urlsplit(self._settings.base_url)
        host = parts.hostname or ""
        port = parts.port or (443 if parts.scheme == "https" else 80)
        try:
            infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
        except socket.gaierror as exc:
            raise TransportError(
                "the source hostname could not be resolved",
                details={"reason": "dns_failure", "host_configured": True},
            ) from exc
        if not infos:
            raise TransportError(
                "the source hostname resolved to no addresses",
                details={"reason": "dns_empty"},
            )
        for info in infos:
            address = info[4][0]
            try:
                parsed = ipaddress.ip_address(address)
            except ValueError:  # pragma: no cover - getaddrinfo returns literals
                continue
            if parsed.is_loopback:
                continue
            for network in _FORBIDDEN_NETWORKS:
                if parsed.version == network.version and parsed in network:
                    # The name passed the allowlist; the address did not. This
                    # is the rebinding case, so it fails closed.
                    raise ConfigurationError(
                        "the source hostname resolves to an address range that is "
                        "never a valid broker endpoint",
                        details={
                            "reason": "address_not_permitted",
                            "network": str(network),
                        },
                    )


def _expects_body(status: int) -> bool:
    """Whether a body must be present and parseable for this status.

    204 and 304 carry no body by definition. Every other status on this API -
    including every error - carries the JSON error envelope, so a 503 with an
    HTML body is a contract violation worth reporting as one.
    """
    return status not in (204, 304)
