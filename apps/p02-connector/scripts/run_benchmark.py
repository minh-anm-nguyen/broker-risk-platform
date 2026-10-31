"""Run a benchmark profile and write a report that says what it measured.

The discipline here is section 11.4's, and the field that matters most is
``fault_profile``. A run with a fault active and a baseline run are different
measurements, and reporting the first as the second is exactly what WF2-P-11
describes. So the profile is recorded in the report, and an unnamed fault is
refused rather than published.

Everything else exists so a number can be interpreted later: the machine, the
interpreter, the build, the dataset size and the raw ``BENCH`` lines the tests
emitted.

Usage::

    python scripts/run_benchmark.py --profile B1
    python scripts/run_benchmark.py --profile LARGE --deals 100000
    python scripts/run_benchmark.py --profile B1 --fault-profile slow_upstream
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

#: Profile name to the test class that runs it (section 11.3).
PROFILES: dict[str, str] = {
    "B0": "TestB0Contract",
    "B1": "TestB1Normal",
    "B2": "TestB2ConcurrentScans",
    "B3": "TestB3Burst",
    "LARGE": "TestLargeScan",
    "ALL": "",
}

#: ``none`` means a baseline. Anything else must *name* the fault that was
#: active, because a number measured under a fault is not a baseline number.
KNOWN_FAULT_PROFILES = ("none", "slow_upstream", "http_errors", "poison_data", "mixed")


def git_sha() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return result.stdout.strip() or "unknown"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="ALL", choices=sorted(PROFILES))
    parser.add_argument(
        "--fault-profile",
        default="none",
        help=(
            "the fault that was active during this run. 'none' means a baseline; "
            "a run under a fault reported as a baseline is the failure WF2-P-11 "
            "describes"
        ),
    )
    parser.add_argument("--deals", type=int, default=None, help="dataset size for LARGE")
    parser.add_argument("--out", type=Path, default=ROOT / "artifacts")
    parser.add_argument("--label", default=None, help="a note carried into the report")
    args = parser.parse_args(argv)

    if args.fault_profile not in KNOWN_FAULT_PROFILES:
        raise SystemExit(
            f"--fault-profile must be one of {KNOWN_FAULT_PROFILES}; an unnamed "
            "fault makes the resulting numbers uninterpretable"
        )

    run_id = time.strftime("bench-%Y%m%dT%H%M%SZ", time.gmtime())
    out_dir = args.out / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    selector = PROFILES[args.profile]
    target = "tests/performance/test_baseline.py"
    if selector:
        target = f"{target}::{selector}"

    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [str(ROOT / "src"), env.get("PYTHONPATH", "")]
    ).rstrip(os.pathsep)
    if args.deals is not None:
        env["CONNECTOR_BENCH_DEALS"] = str(args.deals)

    command = [
        sys.executable,
        "-m",
        "pytest",
        target,
        "-q",
        "-s",
        "-p",
        "no:cacheprovider",
    ]
    started = time.monotonic()
    result = subprocess.run(  # noqa: S603 - fixed argv, no shell
        command, cwd=ROOT, env=env, capture_output=True, text=True
    )
    elapsed = time.monotonic() - started

    measurements: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        if not line.startswith("BENCH "):
            continue
        try:
            measurements.append(json.loads(line[len("BENCH ") :]))
        except json.JSONDecodeError:  # pragma: no cover - malformed emission
            continue

    report = {
        "benchmark_run_id": run_id,
        "git_sha": git_sha(),
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        # The field WF2-P-11 turns on. Its presence is what lets a reader tell
        # a baseline from a chaos run.
        "fault_profile": args.fault_profile,
        "is_baseline": args.fault_profile == "none",
        "profile": args.profile,
        "label": args.label,
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "dataset_deals": args.deals,
        "wall_clock_s": round(elapsed, 3),
        "pytest_exit_code": result.returncode,
        "measurements": measurements,
        "source": (
            "Project 01 contract double (tests/fakesim) over loopback TCP; not the "
            "Project 01 service"
        ),
        "caveat": (
            "these are measurements on the machine named above, not service-level "
            "objectives (design section 0.3)"
        ),
    }
    report_path = out_dir / "benchmark.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    (out_dir / "pytest.log").write_text(
        result.stdout + "\n" + result.stderr, encoding="utf-8"
    )

    print(f"benchmark report: {report_path}")
    print(f"  profile={args.profile} fault_profile={args.fault_profile}")
    for measurement in measurements:
        print(f"  {json.dumps(measurement)}")
    if result.returncode != 0:
        print(result.stdout[-4000:], file=sys.stderr)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
