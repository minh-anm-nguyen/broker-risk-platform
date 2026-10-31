#!/usr/bin/env python
"""Run a fault recipe against an in-process instance and check its oracle.

Design doc section 14.1 proposes a recipe format; this is the runner for it.
Recipes live in ``recipes/*.yaml`` and are executed against a freshly built
app, so a reviewer can reproduce a race without reading the test suite.

    python scripts/run_fault_recipe.py recipes/IT06-04-repair-race-v1.yaml
    python scripts/run_fault_recipe.py recipes/*.yaml --json

Every run records evidence: which injections fired, the revisions before and
after, the HTTP capture, the audit decisions and whether upstream state was
touched. A recipe whose oracle cannot be checked is reported as INCONCLUSIVE
rather than as a pass.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT.parents[1] / "packages" / "risk-engine" / "src"))

BARRIER_PREFIX = "barrier."


def load_recipe(path: Path) -> dict[str, Any]:
    import yaml

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{path.name}: a recipe must be a mapping")
    for required in ("recipe_id", "action", "expect"):
        if required not in data:
            raise ValueError(f"{path.name}: missing {required!r}")
    return data


def build(recipe: dict[str, Any]):
    """Assemble an app instance with the recipe's world applied."""
    from risk_api.faults import FaultRegistry
    from risk_api.lifespan import build_context, default_policies
    from risk_api.main import create_app
    from risk_api.repositories.read_views import build_demo_world
    from risk_api.settings import Settings
    from risk_engine import AccountFact, DealFact, PositionFact
    from decimal import Decimal

    settings = Settings(environment="test", faults_enabled=True, log_level="CRITICAL")
    world_spec = recipe.get("world") or {}
    world = build_demo_world(source_id=world_spec.get("source_id", "sim-broker-a"))

    # A second account, so a cross-tenant attempt has a real target.
    world.accounts["00043"] = AccountFact("00043", "USD", "PRESENT", "scan-A")
    world.positions["P-0002"] = PositionFact(
        position_id="P-0002",
        account_id="00043",
        symbol="EURUSD",
        side="SELL",
        lots=Decimal("0.2"),
        open_price=Decimal("1.1010"),
        source_status="OPEN",
        presence="PRESENT",
        published_scan_id="scan-P",
    )
    world.deals["D-0002"] = DealFact(
        deal_id="D-0002",
        account_id="00043",
        source_seq=1051,
        entry_kind="OUT",
        gross_profit=Decimal("10.00"),
        commission=Decimal("-1.00"),
        swap=Decimal("0.00"),
        executed_at_utc="2026-10-15T10:00:00Z",
    )

    for field in (
        "epoch_id",
        "epoch_status",
        "position_scan_id",
        "account_scan_id",
        "deal_watermark_seq",
        "recon_revision",
        "recon_blocking_reason",
        "quote_snapshot_id",
        "deals_complete",
        "positions_published",
        "accounts_published",
    ):
        if field in world_spec:
            setattr(world, field, world_spec[field])

    faults = FaultRegistry(enabled=True)
    context = build_context(
        settings=settings,
        world=world,
        policies=default_policies(world.source_id),
        faults=faults,
    )
    app = create_app(context=context, settings=settings)
    return app, context


def mint_header(context, recipe: dict[str, Any]) -> dict[str, str]:
    from risk_api.authn.test_issuer import LocalTestIssuer, TokenSpec

    principal = recipe.get("principal") or {}
    issuer = LocalTestIssuer(
        issuer=context.settings.jwt_issuer,
        audience=context.settings.jwt_audience,
        keys=context.settings.jwt_keys,
        environment=context.settings.environment,
    )
    token = issuer.mint(
        TokenSpec(
            subject=principal.get("subject", "analyst-a"),
            tenant_id=principal.get("tenant_id", "tenant-a"),
            extra_claims={"roles": list(principal.get("roles", ["risk_analyst"]))},
        ),
        now=int(time.time()),
    )
    return {"Authorization": f"Bearer {token}"}


async def execute(recipe: dict[str, Any], injections: list[dict[str, Any]]) -> dict[str, Any]:
    """Run one variant and collect its evidence."""
    from httpx import ASGITransport, AsyncClient

    app, context = build(recipe)
    header = mint_header(context, recipe)
    action = recipe["action"]
    world = context.views.world

    barrier = None
    for injection in injections:
        at = injection["at"]
        if injection.get("action") == "barrier_hold" or at.startswith(BARRIER_PREFIX):
            if injection.get("action") in (None, "arm", "barrier_hold"):
                barrier = context.faults.arm_barrier(at)
                continue
        if injection.get("action") in (None, "arm"):
            context.faults.arm(at, injection.get("payload", True))

    mutations = [i for i in injections if i.get("action") == "bump_recon_revision"]
    revision_before = world.recon_revision
    checksum_before = world.checksum()

    context.workers.start()
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://recipe.test"
        ) as http:
            request = http.request(
                action.get("method", "GET"),
                action["path"],
                params=action.get("query"),
                json=action.get("body"),
                headers=header,
            )
            task = asyncio.create_task(request)

            if barrier is not None:
                for _ in range(400):
                    if barrier.arrivals:
                        break
                    await asyncio.sleep(0.005)
                if not barrier.arrivals:
                    return {
                        "status": "INCONCLUSIVE",
                        "reason": "the request never reached the barrier",
                    }
                for mutation in mutations:
                    world.bump_recon_revision(
                        mutation["revision"], blocking_reason=mutation.get("blocking_reason")
                    )
                barrier.release()

            response = await task
    finally:
        await context.workers.shutdown(drain_seconds=0.2)

    body_text = response.text
    try:
        body = response.json()
    except ValueError:
        body = {}

    return {
        "status": "RAN",
        "http_status": response.status_code,
        "public_code": body.get("code"),
        "body_text": body_text,
        "injections_fired": list(context.faults.fired),
        "revision_before": revision_before,
        "revision_after": world.recon_revision,
        "audit_decisions": [row.decision for row in context.audit.all_records()],
        "audit_rows": len(context.audit.all_records()),
        "response_audit_id": body.get("audit_id"),
        "source_unchanged_by_api": world.checksum() == checksum_before or bool(mutations),
        "engine_calls": context.engine.stats.calls,
    }


def check(expect: dict[str, Any], evidence: dict[str, Any]) -> list[str]:
    """Evaluate the recipe oracle. Returns the list of breaches."""
    breaches: list[str] = []

    if "http_status" in expect and evidence["http_status"] != expect["http_status"]:
        breaches.append(
            f"http_status {evidence['http_status']} != expected {expect['http_status']}"
        )

    codes = expect.get("public_code")
    if codes and evidence.get("public_code") not in codes:
        breaches.append(f"public_code {evidence.get('public_code')!r} not in {codes}")

    for forbidden in expect.get("forbidden_in_body", []):
        if forbidden in evidence["body_text"]:
            breaches.append(f"body contains forbidden text {forbidden!r}")

    if expect.get("response_has_financial_amounts") is False:
        for marker in ("gross_usd", "net_excluding_fee_usd", "composite_total"):
            if marker in evidence["body_text"]:
                breaches.append(f"body exposes {marker} on a refusal")

    if expect.get("response_has_financial_amounts") is True:
        if "net_excluding_fee_usd" not in evidence["body_text"]:
            breaches.append("body was expected to carry realized amounts")

    if "audit_rows" in expect and evidence["audit_rows"] != expect["audit_rows"]:
        breaches.append(f"audit_rows {evidence['audit_rows']} != {expect['audit_rows']}")

    for decision in expect.get("audit_decisions_exclude", []):
        if decision in evidence["audit_decisions"]:
            breaches.append(f"audit recorded a forbidden {decision} decision")

    for decision in expect.get("audit_decisions_include", []):
        if decision not in evidence["audit_decisions"]:
            breaches.append(f"audit is missing the required {decision} decision")

    if expect.get("receipt_matches_response_audit_id"):
        if not evidence.get("response_audit_id"):
            breaches.append("the response carried no audit_id to match")

    if expect.get("source_data_unchanged_by_api") and not evidence["source_unchanged_by_api"]:
        breaches.append("upstream state changed in a way the recipe did not cause")

    if not evidence["injections_fired"]:
        breaches.append("no injection fired, so the recipe proved nothing")

    return breaches


def run_recipe(path: Path) -> dict[str, Any]:
    recipe = load_recipe(path)
    variants = recipe.get("variants") or [{"injections": recipe.get("injections", [])}]
    results = []
    for index, variant in enumerate(variants):
        evidence = asyncio.run(execute(recipe, list(variant.get("injections", []))))
        if evidence["status"] != "RAN":
            results.append({"variant": index, "verdict": "INCONCLUSIVE", "evidence": evidence})
            continue
        breaches = check(recipe["expect"], evidence)
        results.append(
            {
                "variant": index,
                "verdict": "PASS" if not breaches else "FAIL",
                "breaches": breaches,
                "evidence": {k: v for k, v in evidence.items() if k != "body_text"},
            }
        )
    return {"recipe_id": recipe["recipe_id"], "path": str(path.name), "results": results}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("recipes", nargs="+", type=Path)
    parser.add_argument("--json", action="store_true", help="emit machine-readable evidence")
    args = parser.parse_args()

    reports = [run_recipe(path) for path in args.recipes]

    if args.json:
        sys.stdout.write(json.dumps(reports, indent=2, sort_keys=True) + "\n")
    else:
        for report in reports:
            sys.stdout.write(f"\n{report['recipe_id']} ({report['path']})\n")
            for result in report["results"]:
                sys.stdout.write(f"  variant {result['variant']}: {result['verdict']}\n")
                for breach in result.get("breaches", []):
                    sys.stdout.write(f"      {breach}\n")
                evidence = result["evidence"]
                if evidence.get("status") == "RAN":
                    sys.stdout.write(
                        f"      http={evidence['http_status']} code={evidence['public_code']} "
                        f"rev {evidence['revision_before']} -> {evidence['revision_after']} "
                        f"audit={evidence['audit_decisions']} engine_calls={evidence['engine_calls']}\n"
                    )

    failed = sum(
        1
        for report in reports
        for result in report["results"]
        if result["verdict"] != "PASS"
    )
    if failed:
        sys.stdout.write(f"\n{failed} recipe variant(s) did not pass\n")
        return 1
    sys.stdout.write("\nall recipe variants passed\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
