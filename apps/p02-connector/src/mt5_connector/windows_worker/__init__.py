"""Process-isolated worker: gateway side, child side, and the IPC between.

Thread mode is enough to prove that a blocking call stays off the event loop.
It is *not* enough to recover from a call that never returns, because a Python
thread cannot be stopped. This package is the answer to that: the session
lives in a child process, so recovery is a kill and a reap rather than a wish
(ADR-204).

The name follows the design document, where the native MT5 Manager SDK is a
Windows deployment. Nothing here is Windows-specific - the same isolation is
what the hard-hang chaos tests use on any platform.
"""

from __future__ import annotations

from .protocol import PROTOCOL_VERSION, Frame, FrameError, read_frame, write_frame
from .watchdog import ProcessDriverProxy, ProcessWorkerSpec, WorkerTerminated

__all__ = [
    "PROTOCOL_VERSION",
    "Frame",
    "FrameError",
    "read_frame",
    "write_frame",
    "ProcessDriverProxy",
    "ProcessWorkerSpec",
    "WorkerTerminated",
]
