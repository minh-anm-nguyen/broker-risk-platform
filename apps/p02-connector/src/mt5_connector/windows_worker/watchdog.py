"""Process-isolated driver: the only mode that can recover from a hard hang.

:class:`ProcessDriverProxy` implements the ordinary
:class:`~mt5_connector.drivers.base.BlockingDriverProtocol`, so the actor, the
supervisor and the validation layer are unchanged by it. What changes is what
happens when a call never returns.

In thread mode the worker thread is blocked inside the driver and nothing can
retrieve it. Here the worker thread is blocked reading a **pipe**, and
:meth:`ProcessDriverProxy.emergency_stop` kills the child that owns the other
end. The pipe closes, the read returns EOF, the proxy raises, and the worker
thread unblocks and exits normally. So the recovery is real and the mechanism
is specific: it is the *process* that is killed, never the thread
(ADR-204, IT-07, WF2-F-06, WF2-R-04).

Fencing is doubled here because an IPC buffer can hold a message across a
restart. Each child is given an epoch, every frame carries it, and a frame
whose epoch is not current is discarded by both sides. A result that was
already in the pipe when the worker was replaced can therefore never be
answered as the new worker's (WF2-R-09, WF2-O-06).
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from ..drivers.base import DriverIdentity, DriverResponse
from ..errors import (
    ConnectorError,
    IncompatibleSdkError,
    TransportError,
)
from .protocol import PROTOCOL_VERSION, Frame, FrameError, read_frame, write_frame

__all__ = ["ProcessWorkerSpec", "ProcessDriverProxy", "WorkerTerminated"]

#: Errors reconstructed from a worker's failure frame, by code.
_ERROR_CLASSES: dict[str, type[ConnectorError]] = {}


def _error_from_payload(payload: dict[str, Any]) -> ConnectorError:
    """Rebuild a typed error from a worker's failure frame.

    Looked up by code against the real taxonomy so the classification the
    worker made survives the IPC boundary. An unknown code becomes a
    transport error rather than something more specific, because inventing a
    classification here would be worse than admitting ignorance.
    """
    if not _ERROR_CLASSES:
        from .. import errors as error_module

        for name in dir(error_module):
            candidate = getattr(error_module, name)
            if (
                isinstance(candidate, type)
                and issubclass(candidate, ConnectorError)
                and candidate is not ConnectorError
            ):
                _ERROR_CLASSES[candidate.code] = candidate
    code = str(payload.get("code") or "TRANSPORT_ERROR")
    cls = _ERROR_CLASSES.get(code, TransportError)
    error = cls(
        str(payload.get("message") or "the worker reported a failure"),
        upstream_status=payload.get("upstream_status"),
        details=dict(payload.get("details") or {}),
    )
    retry_after = payload.get("retry_after_s")
    if isinstance(retry_after, (int, float)):
        error.retry_after_s = float(retry_after)
    return error


class WorkerTerminated(TransportError):
    """The worker process went away while a call was outstanding.

    A transport error on purpose: for a read-only GET this is ambiguous in
    exactly the way a timeout is, and the retry controller treats it the same.
    """

    code = "TRANSPORT_ERROR"


@dataclass(slots=True)
class ProcessWorkerSpec:
    """How to build the child and the driver inside it."""

    driver: str
    settings_fields: dict[str, Any] = field(default_factory=dict)
    token: str = ""
    #: Only honoured by the fake driver, for chaos tests.
    script: list[dict[str, Any]] = field(default_factory=list)
    python_executable: str = field(default_factory=lambda: sys.executable)
    startup_timeout_s: float = 10.0
    call_timeout_s: float | None = None
    env: dict[str, str] | None = None

    def open_payload(self) -> dict[str, Any]:
        return {
            "driver": self.driver,
            "settings": self.settings_fields,
            "token": self.token,
            "script": self.script,
        }


class ProcessDriverProxy:
    """A driver whose calls happen in a child process.

    Not thread-safe, and does not need to be: the actor guarantees one call at
    a time, which is also what makes a single request/response pipe sufficient.
    """

    source_kind = "simulator"

    def __init__(self, spec: ProcessWorkerSpec, *, epoch: int) -> None:
        self._spec = spec
        self._epoch = epoch
        self._process: subprocess.Popen[bytes] | None = None
        self._lock = threading.Lock()
        self._closed = False
        self._terminated_reason: str | None = None
        self._sequence = 0
        self.pid: int | None = None
        self.calls_made = 0
        self.kills_requested = 0
        self.stale_frames_discarded = 0
        self._stderr_snapshot = ""

    # -- lifecycle ----------------------------------------------------------

    def open(self) -> DriverIdentity:
        # ``-s`` drops user site-packages. ``-I`` is deliberately **not** used:
        # isolated mode also implies ``-E``, which discards ``PYTHONPATH`` - so
        # the child could not import this package from a source checkout. The
        # protection ``-I`` would have given against a module planted in the
        # working directory is obtained instead by running the child *in* the
        # package root, which is a directory this project owns.
        command = [
            self._spec.python_executable,
            "-s",
            "-m",
            "mt5_connector.windows_worker.launcher",
        ]
        env = dict(os.environ if self._spec.env is None else self._spec.env)
        existing = env.get("PYTHONPATH", "")
        package_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        env["PYTHONPATH"] = (
            package_root if not existing else f"{package_root}{os.pathsep}{existing}"
        )
        try:
            process = subprocess.Popen(  # noqa: S603 - fixed argv, no shell
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                cwd=package_root,
                bufsize=0,
            )
        except OSError as exc:
            raise TransportError(
                "the worker process could not be started",
                details={"reason": "spawn_failed", "os_error": type(exc).__name__},
            ) from exc
        self._process = process
        self.pid = process.pid

        assert process.stdin is not None and process.stdout is not None
        try:
            write_frame(
                process.stdin,
                Frame(
                    kind="open",
                    worker_epoch=self._epoch,
                    request_id=self._next_request_id("open"),
                    payload=self._spec.open_payload(),
                ),
            )
            frame = self._read_frame_with_deadline(self._spec.startup_timeout_s)
        except (FrameError, OSError, BrokenPipeError) as exc:
            self._terminate("handshake failed")
            raise TransportError(
                "the worker process did not complete its handshake",
                details={"reason": "handshake_failed", "protocol": PROTOCOL_VERSION},
            ) from exc

        if frame.kind == "open_failed":
            error = _error_from_payload(frame.payload)
            self._terminate("driver open failed")
            raise error
        if frame.kind != "opened":
            self._terminate("unexpected handshake frame")
            raise TransportError(
                "the worker process sent an unexpected handshake frame",
                details={"reason": "handshake_protocol", "frame_kind": frame.kind},
            )
        payload = frame.payload
        return DriverIdentity(
            source_kind=str(payload.get("source_kind") or self.source_kind),
            contract_version=payload.get("contract_version"),
            run_id=payload.get("run_id"),
            generation=payload.get("generation"),
            raw_meta=dict(payload.get("raw_meta") or {}),
        )

    def close(self) -> None:
        """Ask the worker to exit, then make sure it did. Idempotent."""
        if self._closed:
            return
        self._closed = True
        process = self._process
        if process is None:
            return
        try:
            if process.stdin is not None and not process.stdin.closed:
                write_frame(
                    process.stdin,
                    Frame(
                        kind="close",
                        worker_epoch=self._epoch,
                        request_id=self._next_request_id("close"),
                        payload={},
                    ),
                )
        except (OSError, FrameError, ValueError):
            pass
        self._terminate("close requested", graceful=True)

    def emergency_stop(self) -> None:
        """Kill the worker now. Unblocks whatever is reading its pipe.

        This is the mechanism behind "recovering from a hung call". It is
        called from the supervisor's task while the actor's worker thread is
        blocked on the pipe read, which is safe because killing a subprocess
        does not touch that thread - it just makes its read return.
        """
        self.kills_requested += 1
        self._terminated_reason = "emergency_stop"
        self._terminate("emergency stop")

    def _terminate(self, reason: str, *, graceful: bool = False) -> None:
        process, self._process = self._process, None
        if process is None:
            return
        self._terminated_reason = self._terminated_reason or reason
        try:
            if graceful:
                try:
                    process.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    process.kill()
            else:
                process.kill()
            # Reaped, not merely signalled. An unreaped child is a zombie and
            # a leaked process slot (WF2-F-06).
            process.wait(timeout=5.0)
        except (subprocess.TimeoutExpired, OSError):
            pass
        finally:
            # Snapshotted after the wait and before the close. The worker is
            # gone by now so this cannot block, and without it the one message
            # explaining a failed handshake is discarded precisely when it is
            # needed.
            if process.stderr is not None:
                try:
                    self._stderr_snapshot = (process.stderr.read(8192) or b"").decode(
                        "utf-8", errors="replace"
                    )
                except (OSError, ValueError):
                    pass
            for stream in (process.stdin, process.stdout, process.stderr):
                try:
                    if stream is not None:
                        stream.close()
                except OSError:
                    pass

    @property
    def alive(self) -> bool:
        process = self._process
        return process is not None and process.poll() is None

    @property
    def exit_code(self) -> int | None:
        process = self._process
        return None if process is None else process.poll()

    # -- calls --------------------------------------------------------------

    def fetch_meta(self) -> DriverResponse:
        return self._call("fetch_meta", {})

    def fetch_accounts(self, *, cursor: str | None, limit: int) -> DriverResponse:
        return self._call("fetch_accounts", {"cursor": cursor, "limit": limit})

    def fetch_positions(
        self,
        *,
        cursor: str | None,
        limit: int,
        account_id: str | None = None,
        symbol: str | None = None,
    ) -> DriverResponse:
        return self._call(
            "fetch_positions",
            {"cursor": cursor, "limit": limit, "account_id": account_id, "symbol": symbol},
        )

    def fetch_deals(
        self,
        *,
        cursor: str | None,
        limit: int,
        after_seq: int | None = None,
        account_id: str | None = None,
        symbol: str | None = None,
    ) -> DriverResponse:
        return self._call(
            "fetch_deals",
            {
                "cursor": cursor,
                "limit": limit,
                "after_seq": after_seq,
                "account_id": account_id,
                "symbol": symbol,
            },
        )

    def _next_request_id(self, prefix: str) -> str:
        self._sequence += 1
        return f"{prefix}-{self._epoch}-{self._sequence}"

    def _call(self, operation: str, params: dict[str, Any]) -> DriverResponse:
        process = self._process
        if self._closed or process is None:
            raise WorkerTerminated(
                "the worker process is not running",
                details={
                    "reason": self._terminated_reason or "not_started",
                    "operation": operation,
                },
            )
        assert process.stdin is not None
        request_id = self._next_request_id(operation)
        self.calls_made += 1
        try:
            write_frame(
                process.stdin,
                Frame(
                    kind="call",
                    worker_epoch=self._epoch,
                    request_id=request_id,
                    payload={"operation": operation, "params": params},
                ),
            )
        except (OSError, BrokenPipeError, ValueError) as exc:
            raise WorkerTerminated(
                "the worker process closed its input before the request was sent",
                details={"reason": "broken_pipe", "operation": operation},
            ) from exc

        while True:
            try:
                frame = self._read_frame_with_deadline(self._spec.call_timeout_s)
            except FrameError as exc:
                # This is the path a kill takes: the pipe closed under a
                # blocking read, so the worker thread unblocks here.
                raise WorkerTerminated(
                    "the worker process stopped responding",
                    details={
                        "reason": self._terminated_reason or "worker_exited",
                        "operation": operation,
                        "exit_code": self.exit_code,
                    },
                ) from exc
            if frame.worker_epoch != self._epoch or frame.request_id != request_id:
                # A frame left over from a previous incarnation or request.
                # Discarded and counted; never matched to this call.
                self.stale_frames_discarded += 1
                continue
            if frame.kind == "result":
                payload = frame.payload
                return DriverResponse(
                    status=int(payload.get("status", 0)),
                    payload=payload.get("payload"),
                    byte_count=int(payload.get("byte_count", 0)),
                    content_type=payload.get("content_type"),
                    retry_after=payload.get("retry_after"),
                    path_name=str(payload.get("path_name") or ""),
                    upstream_latency_ms=payload.get("upstream_latency_ms"),
                )
            if frame.kind in ("failed", "rejected"):
                raise _error_from_payload(frame.payload)
            raise TransportError(
                "the worker sent a frame this gateway does not understand",
                details={"frame_kind": frame.kind, "operation": operation},
            )

    def _read_frame_with_deadline(self, timeout_s: float | None) -> Frame:
        process = self._process
        if process is None or process.stdout is None:
            raise FrameError("the worker stdout is not available")
        if timeout_s is None:
            # The ordinary path: block. A hang here is exactly what
            # ``emergency_stop`` resolves, and a local timeout would only hide
            # it while the child kept running.
            return read_frame(process.stdout)
        deadline = time.monotonic() + timeout_s
        while True:
            if process.poll() is not None:
                raise FrameError("the worker exited before answering")
            try:
                return read_frame(process.stdout)
            except FrameError:
                if time.monotonic() >= deadline:
                    raise
                raise

    def stderr_tail(self, *, limit: int = 4096) -> str:
        """Whatever the worker wrote to stderr. Diagnostics only.

        Reads the live stream when the worker is still up, and falls back to
        the snapshot taken at termination - otherwise the one message that
        explains a failed handshake is unavailable precisely because the
        handshake failed.
        """
        process = self._process
        if process is not None and process.stderr is not None:
            try:
                data = process.stderr.read(limit) or b""
            except (OSError, ValueError):
                data = b""
            if data:
                return data.decode("utf-8", errors="replace")
        return self._stderr_snapshot[:limit]


def require_process_mode_supported() -> None:
    """Raise unless this interpreter can host a worker process."""
    if not sys.executable:  # pragma: no cover - embedded interpreters
        raise IncompatibleSdkError(
            "process isolation needs a usable Python executable",
            details={"reason": "no_executable"},
        )
