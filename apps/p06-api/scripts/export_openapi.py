#!/usr/bin/env python
"""Export the OpenAPI document so the contract is reviewable in a diff.

    python scripts/export_openapi.py [--check]

``--check`` fails if the committed document is stale, which is what CI runs: a
schema change should show up as a reviewed diff, not as a surprise to a client.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT.parents[1] / "packages" / "risk-engine" / "src"))

TARGET = ROOT / "openapi" / "risk-reporting-v1.json"


def build_document() -> dict:
    from risk_api.lifespan import build_context
    from risk_api.main import create_app
    from risk_api.settings import Settings

    settings = Settings(environment="test")
    app = create_app(context=build_context(settings=settings), settings=settings)
    return app.openapi()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail if the committed file is stale")
    args = parser.parse_args()

    document = json.dumps(build_document(), indent=2, sort_keys=True) + "\n"

    if args.check:
        if not TARGET.exists():
            sys.stdout.write(f"missing {TARGET.relative_to(ROOT)}; run without --check\n")
            return 1
        if TARGET.read_text(encoding="utf-8") != document:
            sys.stdout.write(
                f"{TARGET.relative_to(ROOT)} is stale; run scripts/export_openapi.py\n"
            )
            return 1
        sys.stdout.write("openapi document is current\n")
        return 0

    TARGET.parent.mkdir(parents=True, exist_ok=True)
    TARGET.write_text(document, encoding="utf-8")
    sys.stdout.write(f"wrote {TARGET.relative_to(ROOT)}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
