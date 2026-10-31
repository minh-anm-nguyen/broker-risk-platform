#!/usr/bin/env python
"""Run the suite by layer and emit the test-result contract from section 14.5.

Design section 14.5 pins a result shape and section 19 pins the statuses:
``PASS``, ``FAIL``, ``INCONCLUSIVE``, ``NOT_RUN``, mutually exclusive.

The rule this script exists to enforce is the one in section 14.2: a catalogue
of cases is not a reliability result. So a layer that collected no tests is
reported as ``NOT_RUN`` rather than omitted, a layer with skips is annotated
because a skipped case is not PASS evidence for its scenario, and the summary
is a release verdict rather than a percentage. What was *not* assessed is listed
explicitly, so a green run cannot be read as covering the performance or
three-broker gates.

Usage::

    python scripts/collect_evidence.py
    python scripts/collect_evidence.py --layers unit contract
    python scripts/collect_evidence.py --include-slow --out evidence/full
"""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


@dataclass(frozen=True, slots=True)
class Layer:
    """A test layer, the marker that selects it and the gate it feeds.

    ``priority`` drives the verdict: a P0 layer that is anything other than an
    executed PASS blocks release (design section 19), while a P1 layer failing
    is reported and does not.
    """

    name: str
    marker: str
    gate: str
    priority: str


LAYERS: tuple[Layer, ...] = (
    Layer("unit", "unit", "G07-1", "P0"),
    Layer("contract", "contract", "G07-1", "P0"),
    Layer("integration", "integration", "G07-2", "P0"),
    Layer("chaos", "chaos", "G07-4", "P0"),
    Layer("property", "property", "G07-4", "P0"),
    Layer("mutation", "mutation", "G07-4", "P0"),
    Layer("performance", "performance", "G07-5", "P1"),
)

BY_NAME = {layer.name: layer for layer in LAYERS}

NOT_ASSESSED = (
    "G07-5 performance targets: harness figures only, no network or replication",
    "G07-6 disaster recovery: no backup or restore drill",
    "G07-7 production-like: needs the HA-LIKE 3-broker lab",
    "the full 320-case P0 matrix from design section 13.2",
)


@dataclass
class LayerResult:
    layer: str
    marker: str
    gate: str
    priority: str
    status: str = "NOT_RUN"
    duration_seconds: float = 0.0
    evidence_paths: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def environment_manifest() -> dict[str, object]:
    """Record what this ran on.

    Section 16.3 asks for exact hardware and pinned versions. This is the
    honest subset obtainable from inside the process; an ``HA-LIKE`` run must
    add broker image digests and host specifications.
    """
    memory: int | None = None
    cpus: int | None = None
    try:
        import psutil

        memory = int(psutil.virtual_memory().total)
        cpus = psutil.cpu_count(logical=True)
    except ModuleNotFoundError:
        pass
    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "machine": platform.machine(),
        "logical_cpus": cpus,
        "total_memory_bytes": memory,
        "broker": "in-process model (outbox.broker.embedded)",
        "store": "sqlite",
        "caveat": (
            "no network, no replication, no broker fsync. Durability and throughput "
            "claims require the HA-LIKE lab from design section 16.1."
        ),
    }


def run_layer(layer: Layer, out_dir: Path, *, include_slow: bool) -> LayerResult:
    result = LayerResult(
        layer=layer.name, marker=layer.marker, gate=layer.gate, priority=layer.priority
    )
    selector = layer.marker if include_slow else f"{layer.marker} and not slow"
    log_path = out_dir / f"{layer.name}.log"

    started = time.perf_counter()
    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "-m", selector, "-q", "--no-header"],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    result.duration_seconds = round(time.perf_counter() - started, 2)

    output = completed.stdout + completed.stderr
    log_path.write_text(output, encoding="utf-8")
    result.evidence_paths.append(str(log_path.relative_to(REPO)))

    match completed.returncode:
        case 0:
            result.status = "PASS"
        case 5:
            # pytest's "no tests collected". A layer with no tests has not passed.
            result.status = "NOT_RUN"
            result.notes.append("no tests matched this marker")
        case code:
            result.status = "FAIL"
            result.notes.append(f"pytest exited {code}")

    if result.status == "PASS" and "skipped" in output:
        result.notes.append(
            "some cases were skipped; a skipped case is not PASS evidence for its scenario"
        )
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--layers", nargs="*", default=list(BY_NAME), choices=list(BY_NAME))
    parser.add_argument("--out", default="evidence")
    parser.add_argument("--include-slow", action="store_true", help="run the slow layers too")
    args = parser.parse_args(argv)

    out_dir = REPO / args.out
    out_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for name in args.layers:
        result = run_layer(BY_NAME[name], out_dir, include_slow=args.include_slow)
        results.append(result)
        suffix = f"  ({'; '.join(result.notes)})" if result.notes else ""
        print(f"{name:12s} {result.status:9s} {result.duration_seconds:7.2f}s{suffix}")

    blocking = [r for r in results if r.priority == "P0" and r.status != "PASS"]
    report = {
        "release_verdict": "BLOCKED" if blocking else "P0_LAYERS_PASS",
        "blocking_layers": [r.layer for r in blocking],
        "not_assessed": list(NOT_ASSESSED),
        "environment": environment_manifest(),
        "layers": [asdict(r) for r in results],
        "slow_layers_included": args.include_slow,
        "run_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    report_path = out_dir / "evidence.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

    print(f"\nverdict: {report['release_verdict']}")
    if blocking:
        print(f"blocking: {', '.join(report['blocking_layers'])}")
    print(f"not assessed: {len(NOT_ASSESSED)} item(s) listed in {report_path}")
    return 1 if blocking else 0


if __name__ == "__main__":
    sys.exit(main())
