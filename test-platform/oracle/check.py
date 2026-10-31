#!/usr/bin/env python
"""Run the independent oracle against a live platform.

Reads the source over HTTP, reads the target out of PostgreSQL, and compares
them without importing a single component package. Everything it does is a
``SELECT`` or a ``GET``; it cannot change what it is checking.

What it establishes, in this order, because a later check means little if an
earlier one failed:

1. **Deal identity.** The exact set of deal ids the source lists is the exact
   set in the database. Not the same number of them.
2. **Account and position identity.** Same comparison, same reason.
3. **Deal money.** Realized P&L, commission and swap, each summed from the
   source payloads with this package's own arithmetic and compared by value to
   what was persisted.
4. **Outbox coverage.** Every persisted deal has a ``DealRecorded`` event and
   every event has a deal. This is the invariant that makes the Project 03 to
   Project 07 seam real rather than asserted.

Exit codes: ``0`` every check passed, ``1`` a check failed, ``2`` the oracle
could not run. The third is distinct on purpose - an oracle that could not
reach the source has proven nothing, and reporting that as a pass is exactly
the failure P09 section 19.3 exists to prevent.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from oracle.identity import compare_identity_sets  # noqa: E402
from oracle.money import compare_money  # noqa: E402

PAGE_LIMIT = 500


def _fetch_all(base: str, path: str, token: str, *, timeout: float) -> list[dict[str, Any]]:
    """Follow the source's cursor to the end and return every item.

    Pagination is followed rather than assumed away. Reading one page and
    comparing it would be comparing a prefix, which passes happily while
    everything after the first page is missing.
    """
    items: list[dict[str, Any]] = []
    cursor: str | None = None
    seen: set[str] = set()
    while True:
        query = f"?limit={PAGE_LIMIT}" + (f"&cursor={cursor}" if cursor else "")
        request = urllib.request.Request(base + path + query)
        request.add_header("Authorization", f"Bearer {token}")
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = json.loads(response.read())
        items.extend(body.get("items", []))
        cursor = body.get("next_cursor")
        if cursor is None:
            return items
        if cursor in seen:
            # A source that hands back a cursor it already gave would loop
            # here forever. Refusing beats hanging a release gate.
            raise RuntimeError(f"source repeated a cursor on {path}; pagination is looping")
        seen.add(cursor)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Independent oracle (T09-07, T09-08).")
    parser.add_argument("--source-url", default="http://127.0.0.1:8001")
    parser.add_argument("--source-token", default="dev-data-token")
    parser.add_argument("--dsn", default="postgresql://postgres@127.0.0.1:55432/broker_risk")
    parser.add_argument("--timeout", type=float, default=20.0)
    args = parser.parse_args(argv)

    try:
        import psycopg
    except ImportError:
        sys.stderr.write("psycopg is required: pip install 'psycopg[binary]'\n")
        return 2

    try:
        meta_request = urllib.request.Request(args.source_url + "/v1/meta")
        meta_request.add_header("Authorization", f"Bearer {args.source_token}")
        with urllib.request.urlopen(meta_request, timeout=args.timeout) as response:
            meta = json.loads(response.read())
        source_deals = _fetch_all(
            args.source_url, "/v1/deals", args.source_token, timeout=args.timeout
        )
        source_accounts = _fetch_all(
            args.source_url, "/v1/accounts", args.source_token, timeout=args.timeout
        )
        source_positions = _fetch_all(
            args.source_url, "/v1/positions", args.source_token, timeout=args.timeout
        )
    except (urllib.error.URLError, OSError, RuntimeError, ValueError) as exc:
        sys.stderr.write(f"oracle could not read the source: {exc}\n")
        return 2

    try:
        with psycopg.connect(args.dsn, autocommit=True) as connection:
            target_deals = connection.execute(
                "SELECT deal_id, realized_pnl_gross, commission, swap FROM deals"
            ).fetchall()
            target_accounts = {
                r[0] for r in connection.execute("SELECT account_id FROM accounts_current")
            }
            target_positions = {
                r[0] for r in connection.execute("SELECT position_id FROM positions_current")
            }
            event_deal_ids = {
                r[0]
                for r in connection.execute(
                    "SELECT payload->>'deal_id' FROM event_outbox "
                    "WHERE event_type = 'DealRecorded'"
                )
            }
    except Exception as exc:  # noqa: BLE001 - any failure here means no verdict
        sys.stderr.write(f"oracle could not read the target: {exc}\n")
        return 2

    print(
        f"source : {args.source_url}  run={meta.get('run_id')} "
        f"generation={meta.get('generation')} snapshot_seq={meta.get('snapshot_seq')}"
    )
    print(f"target : {args.dsn.rsplit('/', 1)[-1]}")
    print()

    source_deal_ids = {str(d["deal_id"]) for d in source_deals}
    target_deal_ids = {str(r[0]) for r in target_deals}

    verdicts: list[Any] = [
        compare_identity_sets("deal identity", source_deal_ids, target_deal_ids),
        compare_identity_sets(
            "account identity",
            {str(a["account_id"]) for a in source_accounts},
            {str(a) for a in target_accounts},
        ),
        compare_identity_sets(
            "position identity",
            {str(p["position_id"]) for p in source_positions},
            {str(p) for p in target_positions},
        ),
    ]

    # Money is compared only over the deals both sides agree exist. Summing
    # over differing sets would report a monetary difference whose real cause
    # is a missing record, and send somebody hunting a rounding bug that is not
    # there. The identity verdicts above are what report the missing record.
    agreed = sorted(source_deal_ids & target_deal_ids)
    source_by_id = {str(d["deal_id"]): d for d in source_deals}
    target_by_id = {str(r[0]): r for r in target_deals}
    for index, field in enumerate(("realized_pnl_gross", "commission", "swap"), start=1):
        verdicts.append(
            compare_money(
                f"deal {field}",
                [source_by_id[i][field] for i in agreed],
                [target_by_id[i][index] for i in agreed],
            )
        )

    verdicts.append(
        compare_identity_sets(
            "outbox coverage (deal <-> DealRecorded)", target_deal_ids, event_deal_ids
        )
    )

    failed = 0
    for verdict in verdicts:
        print(("  PASS  " if verdict.ok else "  FAIL  ") + verdict.describe())
        failed += 0 if verdict.ok else 1

    print()
    if failed:
        print(f"ORACLE FAILED: {failed} of {len(verdicts)} checks did not hold.")
        return 1
    print(f"ORACLE PASSED: {len(verdicts)} checks, all set-based or exact-decimal.")
    print("Scope: this proves source agreement for the entities above. It says")
    print("nothing about Kafka, ClickHouse or any served report.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
