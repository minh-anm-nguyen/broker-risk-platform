"""The worker process entry point.

Run as ``python -m mt5_connector.windows_worker.launcher``. It reads request
frames from stdin, performs **blocking** driver calls, and writes result
frames to stdout. That is all it does, and the narrowness is the point: this
process is the only thing that touches the session, so it is also the only
thing that has to be killed when a call will not return (section 3.3).

Design notes that matter when debugging it:

* **stdout is the protocol; stderr is for humans.** Anything written to
  stdout that is not a frame corrupts the stream, so logging goes to stderr.
  The first thing this process does is rebind ``sys.stdout`` away from the
  pipe for exactly that reason.
* **Nothing native crosses the boundary.** Only JSON-serialisable values are
  written back. A handle, a pointer or a vendor object is meaningless in
  another process and serialising one would be a use-after-free waiting to
  happen (section 3.3).
* **A hang here is survivable.** If a call never returns, this process stops
  answering and the parent kills it. That is the whole reason the worker is a
  process: a thread in the same state could not be reclaimed at all
  (ADR-204).
"""

from __future__ import annotations

import os
import sys
import traceback
from typing import Any

from ..config import Settings
from ..errors import ConnectorError
from .protocol import Frame, FrameError, read_frame, write_frame

__all__ = ["main", "serve"]


def _log(message: str, **fields: Any) -> None:
    parts = " ".join(f"{key}={value}" for key, value in fields.items())
    print(f"[worker pid={os.getpid()}] {message} {parts}", file=sys.stderr, flush=True)


def _build_driver(spec: dict[str, Any]) -> Any:
    """Construct the driver this worker owns, from the handshake payload."""
    kind = spec.get("driver")
    settings_fields = spec.get("settings") or {}
    token = spec.get("token") or ""
    if kind == "fake_blocking":
        from ..drivers.fake_blocking import FakeBlockingDriver, ScriptedOutcome

        driver = FakeBlockingDriver()
        for outcome in spec.get("script") or []:
            driver.script(
                ScriptedOutcome(
                    delay_s=float(outcome.get("delay_s", 0.0)),
                    block=bool(outcome.get("block", False)),
                    block_timeout_s=outcome.get("block_timeout_s"),
                )
            )
        return driver
    if kind == "simulator_sync_http":
        from ..drivers.simulator_sync_http import SimulatorSyncHttpDriver

        return SimulatorSyncHttpDriver(Settings(**settings_fields), token=token)
    if kind == "manager_sdk_windows":
        from ..drivers.manager_sdk_windows import ManagerSdkWindowsDriver

        return ManagerSdkWindowsDriver(Settings(**settings_fields))
    raise ConnectorError(f"unknown driver kind {kind!r}")


def _response_payload(response: Any) -> dict[str, Any]:
    return {
        "status": response.status,
        "payload": response.payload,
        "byte_count": response.byte_count,
        "content_type": response.content_type,
        "retry_after": response.retry_after,
        "path_name": response.path_name,
        "upstream_latency_ms": response.upstream_latency_ms,
    }


def _error_payload(exc: BaseException) -> dict[str, Any]:
    if isinstance(exc, ConnectorError):
        return {
            "code": exc.code,
            "message": exc.message,
            "details": exc.details,
            "upstream_status": exc.upstream_status,
            "retry_after_s": exc.retry_after_s,
        }
    # An unexpected exception is reduced to its type. The message may carry a
    # URL or a native string, and this payload is logged by the parent.
    return {
        "code": "TRANSPORT_ERROR",
        "message": "the worker driver raised an unexpected error",
        "details": {"driver_error": type(exc).__name__},
        "upstream_status": None,
        "retry_after_s": None,
    }


def serve(stdin: Any, stdout: Any) -> int:
    """The frame loop. Returns a process exit code."""
    epoch = 0
    driver: Any = None
    try:
        handshake = read_frame(stdin)
    except FrameError as exc:
        _log("handshake failed", reason=str(exc))
        return 2
    if handshake.kind != "open":
        _log("first frame was not an open", kind=handshake.kind)
        return 2
    epoch = handshake.worker_epoch
    try:
        driver = _build_driver(handshake.payload)
        identity = driver.open()
    except BaseException as exc:  # noqa: BLE001 - reported, then the worker exits
        write_frame(
            stdout,
            Frame(
                kind="open_failed",
                worker_epoch=epoch,
                request_id=handshake.request_id,
                payload=_error_payload(exc),
            ),
        )
        _log("driver open failed", error=type(exc).__name__)
        return 3
    write_frame(
        stdout,
        Frame(
            kind="opened",
            worker_epoch=epoch,
            request_id=handshake.request_id,
            payload={
                "pid": os.getpid(),
                "source_kind": identity.source_kind,
                "contract_version": identity.contract_version,
                "run_id": identity.run_id,
                "generation": identity.generation,
                "raw_meta": identity.raw_meta,
            },
        ),
    )
    _log("ready", epoch=epoch)

    while True:
        try:
            frame = read_frame(stdin)
        except FrameError:
            # The parent closed the pipe or died. Exit quietly; this is the
            # normal shutdown path as well as the parent-crashed path.
            break
        if frame.kind == "close":
            break
        if frame.worker_epoch != epoch:
            write_frame(
                stdout,
                Frame(
                    kind="rejected",
                    worker_epoch=epoch,
                    request_id=frame.request_id,
                    payload={
                        "code": "TRANSPORT_ERROR",
                        "message": "the request named a different worker epoch",
                        "details": {"frame_epoch": frame.worker_epoch, "worker_epoch": epoch},
                    },
                ),
            )
            continue
        if frame.kind != "call":
            continue

        operation = str(frame.payload.get("operation", ""))
        params = frame.payload.get("params") or {}
        try:
            response = _dispatch(driver, operation, params)
        except BaseException as exc:  # noqa: BLE001
            write_frame(
                stdout,
                Frame(
                    kind="failed",
                    worker_epoch=epoch,
                    request_id=frame.request_id,
                    payload=_error_payload(exc),
                ),
            )
            if not isinstance(exc, (ConnectorError, Exception)):  # pragma: no cover
                raise
            continue
        write_frame(
            stdout,
            Frame(
                kind="result",
                worker_epoch=epoch,
                request_id=frame.request_id,
                payload=_response_payload(response),
            ),
        )

    try:
        if driver is not None:
            driver.close()
    except Exception:  # noqa: BLE001 - shutdown must not fail loudly
        _log("driver close failed", trace=traceback.format_exc(limit=1).strip())
    _log("exiting", epoch=epoch)
    return 0


def _dispatch(driver: Any, operation: str, params: dict[str, Any]) -> Any:
    if operation == "fetch_meta":
        return driver.fetch_meta()
    if operation == "fetch_accounts":
        return driver.fetch_accounts(cursor=params.get("cursor"), limit=params["limit"])
    if operation == "fetch_positions":
        return driver.fetch_positions(
            cursor=params.get("cursor"),
            limit=params["limit"],
            account_id=params.get("account_id"),
            symbol=params.get("symbol"),
        )
    if operation == "fetch_deals":
        return driver.fetch_deals(
            cursor=params.get("cursor"),
            limit=params["limit"],
            after_seq=params.get("after_seq"),
            account_id=params.get("account_id"),
            symbol=params.get("symbol"),
        )
    raise ConnectorError(f"unknown worker operation {operation!r}")


def main() -> int:
    # The protocol owns stdout. Rebinding ``sys.stdout`` to stderr means a
    # stray ``print`` anywhere in the driver stack cannot corrupt a frame.
    pipe_out = sys.stdout.buffer
    pipe_in = sys.stdin.buffer
    sys.stdout = sys.stderr
    return serve(pipe_in, pipe_out)


if __name__ == "__main__":  # pragma: no cover - process entry point
    raise SystemExit(main())
