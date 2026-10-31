#!/usr/bin/env python
"""Mint a signed development token for manual exploration.

    python scripts/mint_dev_token.py --subject analyst-a

Refuses to run unless the configured environment is local or test, and the
issuer itself refuses too. There is no path here that produces a credential a
non-local instance would accept.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT.parents[1] / "packages" / "risk-engine" / "src"))


def main() -> int:
    from risk_api.authn.test_issuer import LocalTestIssuer, TokenSpec
    from risk_api.lifespan import default_policies
    from risk_api.settings import Settings

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subject", default="analyst-a", help="policy subject to impersonate")
    parser.add_argument("--expires-in", type=int, default=900, help="token lifetime in seconds")
    parser.add_argument("--roles", default=None, help="comma-separated role override")
    parser.add_argument("--header", action="store_true", help="print a full Authorization header")
    args = parser.parse_args()

    settings = Settings()
    if settings.environment not in ("local", "test"):
        sys.stderr.write(
            f"refusing to mint a token for environment {settings.environment!r}\n"
        )
        return 2

    policies = default_policies()
    policy = policies.get(args.subject)
    if policy is None and args.roles is None:
        sys.stderr.write(
            f"unknown subject {args.subject!r}; known: {', '.join(sorted(policies))}\n"
            "pass --roles to mint a token for a subject with no bundled policy\n"
        )
        return 2

    roles = tuple(args.roles.split(",")) if args.roles else (policy.roles if policy else ())
    tenant = policy.tenant_id if policy else "tenant-a"

    issuer = LocalTestIssuer(
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        keys=settings.jwt_keys,
        environment=settings.environment,
    )
    token = issuer.mint(
        TokenSpec(
            subject=args.subject,
            tenant_id=tenant,
            expires_in=args.expires_in,
            token_id=f"dev-{args.subject}",
            extra_claims={"roles": list(roles)},
        ),
        now=int(time.time()),
    )
    sys.stdout.write(f"Authorization: Bearer {token}\n" if args.header else f"{token}\n")
    sys.stderr.write(
        f"subject={args.subject} tenant={tenant} roles={','.join(roles) or '(none)'} "
        f"expires_in={args.expires_in}s\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
