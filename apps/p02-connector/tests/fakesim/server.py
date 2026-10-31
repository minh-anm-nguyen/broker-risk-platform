"""Run the contract double on a real loopback socket in a background thread.

A real socket, not an in-process ASGI transport. The connector's driver is
synchronous and must perform genuine blocking socket I/O - swapping in an
in-memory transport would remove the very behaviour under test, and the suite
would then prove that an async client keeps an event loop responsive, which
nobody doubted (design section 3.2).

The server runs on its own event loop in its own thread, so it keeps answering
while the test's loop is busy - including while a worker thread is parked
inside a blocking read.
"""

from __future__ import annotations

import asyncio
import socket
import threading
import time
from dataclasses import dataclass

import uvicorn
from fastapi import FastAPI

from .app import FakeSimulator, create_fake_simulator

__all__ = ["FakeSimulatorServer", "free_port"]


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@dataclass(slots=True)
class FakeSimulatorServer:
    """A started server plus the mutable state a test drives it through."""

    app: FastAPI
    sim: FakeSimulator
    host: str
    port: int
    _server: uvicorn.Server
    _thread: threading.Thread

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def stop(self, *, timeout_s: float = 5.0) -> None:
        self._server.should_exit = True
        self._thread.join(timeout_s)

    @classmethod
    def start(
        cls,
        sim: FakeSimulator | None = None,
        *,
        host: str = "127.0.0.1",
        port: int | None = None,
        startup_timeout_s: float = 10.0,
    ) -> "FakeSimulatorServer":
        app, state = create_fake_simulator(sim)
        resolved_port = port or free_port()
        config = uvicorn.Config(
            app,
            host=host,
            port=resolved_port,
            log_level="warning",
            access_log=False,
            # One worker loop in one thread; the test drives concurrency from
            # the client side.
            loop="asyncio",
            lifespan="on",
        )
        server = uvicorn.Server(config)
        # uvicorn installs signal handlers by default, which only works on the
        # main thread. This server runs on a worker thread.
        server.install_signal_handlers = lambda: None  # type: ignore[method-assign]

        def _run() -> None:
            asyncio.run(server.serve())

        thread = threading.Thread(target=_run, name=f"fakesim-{resolved_port}", daemon=True)
        thread.start()

        deadline = time.monotonic() + startup_timeout_s
        while time.monotonic() < deadline:
            if getattr(server, "started", False):
                break
            time.sleep(0.01)
        else:  # pragma: no cover - only on a badly loaded machine
            server.should_exit = True
            thread.join(2.0)
            raise RuntimeError("the fake simulator did not start inside its timeout")

        return cls(
            app=app,
            sim=state,
            host=host,
            port=resolved_port,
            _server=server,
            _thread=thread,
        )
