"""Configuration, validated before anything is allocated (section 17.1).

Every check in here runs before a thread exists, a socket is opened or a
driver is constructed. That ordering is the requirement, not an
implementation detail: WF2-A-01 through WF2-A-07 all assert that a bad
configuration is refused *without* a worker having been created or a request
having been sent upstream, and the only way to guarantee that is to make
validation a precondition of building the object everything else needs.

Three policies worth naming:

* **No clamping.** ``page_size=100000`` is an error, not a request for 500. A
  clamped limit lets a caller believe it received a whole page when it
  received a fifth of one (WF2-M-05).
* **URLs are built, never accepted.** :meth:`Settings.url_for` composes a
  request from a fixed base and a fixed path constant. There is no code path
  that lets a caller supply a host, which is what closes WF2-N-01 by
  construction rather than by filter.
* **The base URL may not point at a control plane.** A connector holding a
  data-plane token has no business addressing ``/admin``; refusing the
  configuration outright is cheaper than auditing every call site
  (WF2-A-05).
"""

from __future__ import annotations

import ipaddress
import os
import re
from dataclasses import dataclass, field
from typing import Any, Final, Literal
from urllib.parse import urlsplit

from .errors import ConfigurationError

__all__ = [
    "DATA_PLANE_PATHS",
    "WorkerMode",
    "Settings",
    "load_settings",
]

#: Every upstream path this connector is allowed to request. A path not in
#: here cannot be reached, which keeps the data plane read-only even if a
#: future caller passes something unexpected (design section 9.1).
DATA_PLANE_PATHS: Final[dict[str, str]] = {
    "meta": "/v1/meta",
    "accounts": "/v1/accounts",
    "positions": "/v1/positions",
    "deals": "/v1/deals",
}

#: Hard ceiling on a page, independent of configuration. The source publishes
#: its own maximum and the effective cap is the lower of the two.
ABSOLUTE_MAX_PAGE_SIZE: Final[int] = 500

#: Hard ceiling on the admission queue. A configuration asking for more is
#: refused rather than honoured: an unbounded queue is the failure INV-03
#: exists to prevent, and "very large" is unbounded in practice.
ABSOLUTE_MAX_QUEUE: Final[int] = 1024

_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")
_SAFE_ENV_NAME = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")


class WorkerMode:
    THREAD = "thread"
    PROCESS = "process"
    ALL = ("thread", "process")


@dataclass(frozen=True, slots=True)
class Settings:
    """A validated, immutable connector configuration.

    Build one with :func:`load_settings` or by calling the constructor
    directly in a test. Either way :meth:`__post_init__` runs, so there is no
    such thing as an unvalidated ``Settings``.
    """

    source_id: str
    base_url: str
    source_kind: Literal["simulator", "manager_sdk", "fake"] = "simulator"
    driver: Literal[
        "simulator_sync_http", "fake_blocking", "manager_sdk_windows"
    ] = "simulator_sync_http"

    #: Name of the environment variable holding the data-plane bearer token.
    #: The token itself is never a field on this object, so a ``repr`` of the
    #: settings can be logged safely (INV-15).
    token_env: str = "SIMULATOR_DATA_TOKEN"

    worker_mode: str = WorkerMode.THREAD
    session_workers: int = 1
    max_queue_pending: int = 32
    max_concurrent_scans: int = 2

    default_page_size: int = 100
    max_page_size: int = ABSOLUTE_MAX_PAGE_SIZE
    max_response_bytes: int = 1_048_576
    max_json_depth: int = 64
    max_pages_per_scan: int = 2_500
    max_scan_tracked_keys: int = 200_000

    page_total_deadline_ms: int = 3_000
    queue_wait_cap_ms: int = 500
    upstream_read_timeout_ms: int = 1_500
    connect_timeout_ms: int = 500
    minimum_attempt_budget_ms: int = 50

    max_attempts_per_read: int = 3
    max_connect_attempts: int = 3
    retry_base_backoff_ms: int = 100
    retry_max_backoff_ms: int = 1_000
    retry_after_cap_ms: int = 2_000

    breaker_failure_threshold: int = 5
    breaker_cooldown_ms: int = 10_000

    hung_call_hard_limit_ms: int = 10_000
    #: Thread mode cannot kill a hung native call. With this ``False`` the
    #: session stays QUARANTINED and says so, which is the honest state
    #: (section 5.4). Setting it ``True`` lets the supervisor abandon the
    #: thread and start a new epoch - useful to exercise the fence in tests,
    #: but it leaks a thread and readiness reports that too.
    allow_thread_abandon: bool = False
    drain_timeout_ms: int = 5_000

    tls_verify: bool = True
    #: Plain HTTP is permitted only for a loopback address. A cleartext
    #: connection to anything else is refused rather than warned about
    #: (WF2-N-04).
    allow_plaintext_loopback: bool = True
    outbound_allowlist: tuple[str, ...] = ()

    heartbeat_interval_ms: int = 50
    #: The connector never injects its own faults. Faults belong to the test
    #: harness driving the *source* control plane, with its own credential
    #: (section 9.3).
    enable_fault_injection_in_connector: bool = False

    extra: dict[str, Any] = field(default_factory=dict)

    # -- validation ---------------------------------------------------------

    def __post_init__(self) -> None:
        self._check_identity()
        self._check_url()
        self._check_limits()
        self._check_time_budgets()
        self._check_retry()
        self._check_worker()

    def _check_identity(self) -> None:
        if not self.source_id or not self.source_id.strip():
            raise ConfigurationError(
                "source_id is required: the connector will not start without a stable "
                "logical identity for the source it reads",
                details={"field": "source_id"},
            )
        if not _SAFE_NAME.match(self.source_id):
            raise ConfigurationError(
                "source_id must be 1-64 characters of letters, digits, dot, colon, "
                "underscore or hyphen; it appears in metric labels and log fields",
                details={"field": "source_id"},
            )
        if not _SAFE_ENV_NAME.match(self.token_env):
            raise ConfigurationError(
                "token_env must be an uppercase environment variable name",
                details={"field": "token_env"},
            )
        if self.source_kind == "manager_sdk" and self.driver != "manager_sdk_windows":
            raise ConfigurationError(
                "source_kind 'manager_sdk' requires the manager_sdk_windows driver",
                details={"field": "driver"},
            )

    def _check_url(self) -> None:
        parts = urlsplit(self.base_url)
        if parts.scheme not in {"http", "https"}:
            raise ConfigurationError(
                "base_url must use the http or https scheme; a URL without a supported "
                "scheme is rejected before any network access is attempted",
                details={"field": "base_url", "supported": ["http", "https"]},
            )
        if not parts.hostname:
            raise ConfigurationError(
                "base_url must contain a host", details={"field": "base_url"}
            )
        if parts.query or parts.fragment:
            raise ConfigurationError(
                "base_url must not carry a query string or fragment; request parameters "
                "are composed by the connector, not configured",
                details={"field": "base_url"},
            )
        path = parts.path.rstrip("/")
        if path:
            # A base URL with a path would let "/admin/v1" be configured as the
            # root and every data-plane call land on the control plane.
            raise ConfigurationError(
                "base_url must be scheme://host[:port] with no path; the connector "
                "appends only the fixed data-plane paths it is allowed to request",
                details={
                    "field": "base_url",
                    "allowed_paths": sorted(DATA_PLANE_PATHS.values()),
                },
            )
        if parts.scheme == "http" and not self._is_loopback(parts.hostname):
            if not self.allow_plaintext_loopback:
                raise ConfigurationError(
                    "plaintext http is not permitted", details={"field": "base_url"}
                )
            raise ConfigurationError(
                "plaintext http is permitted only for a loopback source; a cleartext "
                "connection to a remote host would expose the bearer token",
                details={"field": "base_url", "host_is_loopback": False},
            )
        if parts.scheme == "https" and not self.tls_verify:
            # Disabling verification turns TLS into obfuscation. There is no
            # configuration that makes this acceptable against a real source.
            raise ConfigurationError(
                "tls_verify must not be disabled for an https source; use a custom CA "
                "bundle instead of turning verification off",
                details={"field": "tls_verify"},
            )
        if self.outbound_allowlist and parts.hostname not in self.outbound_allowlist:
            raise ConfigurationError(
                "base_url host is not in outbound_allowlist",
                details={"field": "base_url", "allowlist_size": len(self.outbound_allowlist)},
            )

    @staticmethod
    def _is_loopback(hostname: str) -> bool:
        if hostname in {"localhost", "localhost.localdomain"}:
            return True
        try:
            return ipaddress.ip_address(hostname).is_loopback
        except ValueError:
            return False

    def _check_limits(self) -> None:
        if not 1 <= self.max_page_size <= ABSOLUTE_MAX_PAGE_SIZE:
            raise ConfigurationError(
                f"max_page_size must be between 1 and {ABSOLUTE_MAX_PAGE_SIZE}",
                details={"field": "max_page_size", "maximum": ABSOLUTE_MAX_PAGE_SIZE},
            )
        if not 1 <= self.default_page_size <= self.max_page_size:
            raise ConfigurationError(
                "default_page_size must be between 1 and max_page_size; a page size of 0 "
                "or one above the cap is refused rather than adjusted",
                details={
                    "field": "default_page_size",
                    "minimum": 1,
                    "maximum": self.max_page_size,
                },
            )
        if not 1 <= self.max_queue_pending <= ABSOLUTE_MAX_QUEUE:
            raise ConfigurationError(
                f"max_queue_pending must be between 1 and {ABSOLUTE_MAX_QUEUE}; an "
                "unbounded or negative queue is the failure INV-03 exists to prevent",
                details={"field": "max_queue_pending", "maximum": ABSOLUTE_MAX_QUEUE},
            )
        if not 1 <= self.max_concurrent_scans <= 64:
            raise ConfigurationError(
                "max_concurrent_scans must be between 1 and 64",
                details={"field": "max_concurrent_scans"},
            )
        if not 1024 <= self.max_response_bytes <= 64 * 1_048_576:
            raise ConfigurationError(
                "max_response_bytes must be between 1 KiB and 64 MiB",
                details={"field": "max_response_bytes"},
            )
        if not 4 <= self.max_json_depth <= 256:
            raise ConfigurationError(
                "max_json_depth must be between 4 and 256",
                details={"field": "max_json_depth"},
            )
        if not 1 <= self.max_pages_per_scan <= 1_000_000:
            raise ConfigurationError(
                "max_pages_per_scan must be between 1 and 1000000",
                details={"field": "max_pages_per_scan"},
            )
        if not 0 <= self.max_scan_tracked_keys <= 10_000_000:
            raise ConfigurationError(
                "max_scan_tracked_keys must be between 0 and 10000000",
                details={"field": "max_scan_tracked_keys"},
            )

    def _check_time_budgets(self) -> None:
        for name, value, lo, hi in (
            ("page_total_deadline_ms", self.page_total_deadline_ms, 1, 600_000),
            ("queue_wait_cap_ms", self.queue_wait_cap_ms, 0, 600_000),
            ("upstream_read_timeout_ms", self.upstream_read_timeout_ms, 1, 600_000),
            ("connect_timeout_ms", self.connect_timeout_ms, 1, 600_000),
            ("minimum_attempt_budget_ms", self.minimum_attempt_budget_ms, 0, 600_000),
            ("hung_call_hard_limit_ms", self.hung_call_hard_limit_ms, 1, 3_600_000),
            ("drain_timeout_ms", self.drain_timeout_ms, 0, 600_000),
            ("heartbeat_interval_ms", self.heartbeat_interval_ms, 1, 10_000),
        ):
            if not lo <= value <= hi:
                raise ConfigurationError(
                    f"{name} must be between {lo} and {hi}", details={"field": name}
                )
        if self.queue_wait_cap_ms >= self.page_total_deadline_ms:
            # A queue-wait cap at or above the total deadline means a request
            # could burn its entire budget waiting and still be dispatched,
            # which is the behaviour WF2-H-01 rejects.
            raise ConfigurationError(
                "queue_wait_cap_ms must be below page_total_deadline_ms so a request "
                "cannot spend its whole budget queueing and still be dispatched",
                details={
                    "field": "queue_wait_cap_ms",
                    "page_total_deadline_ms": self.page_total_deadline_ms,
                },
            )

    def _check_retry(self) -> None:
        if not 1 <= self.max_attempts_per_read <= 10:
            raise ConfigurationError(
                "max_attempts_per_read must be between 1 and 10 (the first try counts)",
                details={"field": "max_attempts_per_read"},
            )
        if not 1 <= self.max_connect_attempts <= 10:
            raise ConfigurationError(
                "max_connect_attempts must be between 1 and 10",
                details={"field": "max_connect_attempts"},
            )
        if self.retry_base_backoff_ms < 0 or self.retry_max_backoff_ms < 0:
            raise ConfigurationError(
                "retry backoff must not be negative", details={"field": "retry_base_backoff_ms"}
            )
        if self.retry_base_backoff_ms > self.retry_max_backoff_ms:
            raise ConfigurationError(
                "retry_base_backoff_ms must not exceed retry_max_backoff_ms",
                details={"field": "retry_base_backoff_ms"},
            )
        if not 0 <= self.retry_after_cap_ms <= 600_000:
            raise ConfigurationError(
                "retry_after_cap_ms must be between 0 and 600000",
                details={"field": "retry_after_cap_ms"},
            )
        if not 1 <= self.breaker_failure_threshold <= 1_000:
            raise ConfigurationError(
                "breaker_failure_threshold must be between 1 and 1000",
                details={"field": "breaker_failure_threshold"},
            )
        if not 0 <= self.breaker_cooldown_ms <= 3_600_000:
            raise ConfigurationError(
                "breaker_cooldown_ms must be between 0 and 3600000",
                details={"field": "breaker_cooldown_ms"},
            )

    def _check_worker(self) -> None:
        if self.worker_mode not in WorkerMode.ALL:
            raise ConfigurationError(
                f"worker_mode must be one of {WorkerMode.ALL}",
                details={"field": "worker_mode"},
            )
        if self.session_workers != 1:
            # One active call per session until a vendor proves concurrent
            # calls are safe. Making this configurable to 2 would be a
            # correctness decision dressed up as a tuning knob (ADR-202).
            raise ConfigurationError(
                "session_workers must be 1: a session is not assumed thread-safe and "
                "this build will not run two calls against one concurrently",
                details={"field": "session_workers", "see": "ADR-202"},
            )
        if self.enable_fault_injection_in_connector:
            raise ConfigurationError(
                "the connector does not inject faults; drive the source control plane "
                "from the test harness with its own credential instead",
                details={"field": "enable_fault_injection_in_connector", "see": "section 9.3"},
            )
        if self.driver == "manager_sdk_windows" and self.worker_mode != WorkerMode.PROCESS:
            raise ConfigurationError(
                "the native Manager SDK driver requires worker_mode=process: a hung "
                "native call cannot be stopped inside a thread",
                details={"field": "worker_mode", "see": "ADR-204"},
            )

    # -- derived ------------------------------------------------------------

    @property
    def process_isolation(self) -> bool:
        return self.worker_mode == WorkerMode.PROCESS

    def url_for(self, name: str) -> str:
        """Compose an allowed upstream URL.

        ``name`` is a key of :data:`DATA_PLANE_PATHS`, not a path and never a
        URL. A caller cannot reach a host or a route that is not in this
        table, which is how SSRF is prevented here - by having no mechanism
        for it rather than by filtering one (WF2-N-01, WF2-A-05).
        """
        try:
            path = DATA_PLANE_PATHS[name]
        except KeyError as exc:
            raise ConfigurationError(
                "requested upstream path is not on the data plane allowlist",
                details={"requested": name, "allowed": sorted(DATA_PLANE_PATHS)},
            ) from exc
        return f"{self.base_url.rstrip('/')}{path}"

    def resolve_limit(self, limit: int | None) -> int:
        """Validate a caller's page size against the cap. Never clamps."""
        if limit is None:
            return self.default_page_size
        if not 1 <= limit <= self.max_page_size:
            raise ConfigurationError(
                "requested page size is outside the permitted range; it is refused "
                "rather than clamped so a caller cannot believe it received a whole page",
                details={"field": "limit", "minimum": 1, "maximum": self.max_page_size},
            )
        return limit

    def token(self, environ: dict[str, str] | None = None) -> str:
        """Read the bearer token from the environment.

        Absent is an error, not an empty string: continuing without a
        credential means sending unauthenticated reads upstream and
        discovering the problem as a 401 storm (WF2-A-07).
        """
        env = os.environ if environ is None else environ
        value = env.get(self.token_env, "")
        if not value or not value.strip():
            raise ConfigurationError(
                f"the data-plane token is missing: environment variable {self.token_env} "
                "is unset or blank, and the connector will not issue unauthenticated reads",
                details={"token_env": self.token_env},
            )
        token = value.strip()
        if len(token) > 512:
            raise ConfigurationError(
                "the data-plane token exceeds 512 characters",
                details={"token_env": self.token_env},
            )
        return token

    def redacted(self) -> dict[str, Any]:
        """A loggable view. Contains no credential by construction."""
        return {
            "source_id": self.source_id,
            "source_kind": self.source_kind,
            "driver": self.driver,
            "base_url": self.base_url,
            "token_env": self.token_env,
            "worker_mode": self.worker_mode,
            "max_queue_pending": self.max_queue_pending,
            "default_page_size": self.default_page_size,
            "max_page_size": self.max_page_size,
            "max_response_bytes": self.max_response_bytes,
            "page_total_deadline_ms": self.page_total_deadline_ms,
            "queue_wait_cap_ms": self.queue_wait_cap_ms,
            "upstream_read_timeout_ms": self.upstream_read_timeout_ms,
            "max_attempts_per_read": self.max_attempts_per_read,
            "hung_call_hard_limit_ms": self.hung_call_hard_limit_ms,
            "allow_thread_abandon": self.allow_thread_abandon,
            "tls_verify": self.tls_verify,
        }


_FIELD_TYPES: Final[dict[str, type]] = {
    "session_workers": int,
    "max_queue_pending": int,
    "max_concurrent_scans": int,
    "default_page_size": int,
    "max_page_size": int,
    "max_response_bytes": int,
    "max_json_depth": int,
    "max_pages_per_scan": int,
    "max_scan_tracked_keys": int,
    "page_total_deadline_ms": int,
    "queue_wait_cap_ms": int,
    "upstream_read_timeout_ms": int,
    "connect_timeout_ms": int,
    "minimum_attempt_budget_ms": int,
    "max_attempts_per_read": int,
    "max_connect_attempts": int,
    "retry_base_backoff_ms": int,
    "retry_max_backoff_ms": int,
    "retry_after_cap_ms": int,
    "breaker_failure_threshold": int,
    "breaker_cooldown_ms": int,
    "hung_call_hard_limit_ms": int,
    "drain_timeout_ms": int,
    "heartbeat_interval_ms": int,
    "allow_thread_abandon": bool,
    "tls_verify": bool,
    "allow_plaintext_loopback": bool,
    "enable_fault_injection_in_connector": bool,
}

_TRUE = frozenset({"1", "true", "yes", "on"})
_FALSE = frozenset({"0", "false", "no", "off"})


def load_settings(environ: dict[str, str] | None = None, **overrides: Any) -> Settings:
    """Build :class:`Settings` from ``CONNECTOR_*`` environment variables.

    Values are read once and the resulting object is frozen, so a file or
    variable changed afterwards cannot produce two different configurations
    inside one run (WF2-A-11). Keyword overrides win over the environment,
    which is what tests use.
    """
    env = dict(os.environ if environ is None else environ)
    values: dict[str, Any] = {}
    prefix = "CONNECTOR_"
    for key, raw in env.items():
        if not key.startswith(prefix):
            continue
        name = key[len(prefix) :].lower()
        if name == "outbound_allowlist":
            values[name] = tuple(part.strip() for part in raw.split(",") if part.strip())
            continue
        expected = _FIELD_TYPES.get(name)
        if expected is int:
            try:
                values[name] = int(raw)
            except ValueError as exc:
                raise ConfigurationError(
                    f"{key} must be an integer", details={"field": name}
                ) from exc
        elif expected is bool:
            lowered = raw.strip().lower()
            if lowered in _TRUE:
                values[name] = True
            elif lowered in _FALSE:
                values[name] = False
            else:
                raise ConfigurationError(
                    f"{key} must be a boolean", details={"field": name}
                )
        else:
            values[name] = raw
    values.update(overrides)

    known = set(Settings.__dataclass_fields__)
    unknown = sorted(set(values) - known)
    if unknown:
        # An ignored setting is worse than a rejected one: the operator
        # believes an option took effect when it was discarded.
        raise ConfigurationError(
            "unrecognised connector settings", details={"fields": unknown}
        )
    if "source_id" not in values:
        raise ConfigurationError(
            "source_id is required: set CONNECTOR_SOURCE_ID",
            details={"field": "source_id"},
        )
    if "base_url" not in values:
        raise ConfigurationError(
            "base_url is required: set CONNECTOR_BASE_URL",
            details={"field": "base_url"},
        )
    return Settings(**values)
