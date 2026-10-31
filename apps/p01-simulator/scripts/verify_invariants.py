#!/usr/bin/env python
"""Run the invariant suite against a database and exit non-zero on failure.

A thin wrapper over ``simulator verify`` so CI can call a path rather than
depend on the package being installed. The real logic lives in
:mod:`simulator.domain.invariants` - there is deliberately no second
implementation here, because a checker that disagrees with the one the service
uses is worse than no checker.

Exit codes: ``0`` sound, ``1`` a violation or an incomplete sweep, ``2`` no run
to check.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from simulator.cli import main as cli_main  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run",
        default=None,
        help="run id to check; defaults to the active run",
    )
    parser.add_argument(
        "--integrity",
        action="store_true",
        help="also run PRAGMA integrity_check (reads the whole database)",
    )
    arguments = parser.parse_args(argv)

    forwarded = ["verify"]
    if arguments.run:
        forwarded += ["--run", arguments.run]
    if arguments.integrity:
        forwarded.append("--integrity")
    return cli_main(forwarded)


if __name__ == "__main__":
    raise SystemExit(main())
