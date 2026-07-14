#!/usr/bin/env python3
"""Seed investors and initial balances into each bank's PostgreSQL database.

Each bank has its OWN postgres instance, so there is one URL per bank.
By default the URLs target the Docker network hostnames (pg_bank_N), which
means the script should run INSIDE the compose network:

    docker compose run --rm bank_0 python scripts/seed_db.py

To run from the host (or with a different topology), override the template:

    DB_URL_TEMPLATE="postgresql://exchange:exchange@localhost:5432/exchange_{bank_id}" \
        python scripts/seed_db.py

Notes:
- Run it AFTER the banks are up once, so the schema already exists.
- Seeding is optional: the system auto-creates investors with an initial
  cash balance the first time they appear in a committed order.
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import asyncpg

BANKS = [f"bank_{i}" for i in range(6)]
INVESTORS_PER_BANK = 5
INITIAL_CASH = 100_000.00

DEFAULT_URL_TEMPLATE = (
    "postgresql://exchange:exchange@pg_{bank_id}:5432/exchange_{bank_id}"
)


def _url_for(bank_id: str) -> str:
    # Legacy override: DB_BASE_URL is suffixed with _<bank_id>
    base = os.environ.get("DB_BASE_URL")
    if base:
        return f"{base}_{bank_id}"
    template = os.environ.get("DB_URL_TEMPLATE", DEFAULT_URL_TEMPLATE)
    return template.format(bank_id=bank_id)


async def seed_bank(db_url: str, bank_id: str) -> bool:
    try:
        conn = await asyncpg.connect(db_url, timeout=10)
    except Exception as exc:
        print(f"[{bank_id}] SKIPPED — could not connect ({exc})")
        return False
    now = datetime.now(timezone.utc)
    try:
        for j in range(INVESTORS_PER_BANK):
            investor_id = f"{bank_id}_inv_{j}"
            await conn.execute(
                """
                INSERT INTO investors (investor_id, bank_id, cash_balance, cash_reserved, created_at)
                VALUES ($1, $2, $3, 0, $4)
                ON CONFLICT (investor_id) DO NOTHING
                """,
                investor_id, bank_id, INITIAL_CASH, now,
            )
        print(f"[{bank_id}] seeded {INVESTORS_PER_BANK} investors")
        return True
    except Exception as exc:
        print(f"[{bank_id}] FAILED — {exc} (start the bank once so the schema exists)")
        return False
    finally:
        await conn.close()


async def main() -> None:
    results = await asyncio.gather(
        *[seed_bank(_url_for(bank_id), bank_id) for bank_id in BANKS]
    )
    ok = sum(1 for r in results if r)
    print(f"done: {ok}/{len(BANKS)} banks seeded")


if __name__ == "__main__":
    asyncio.run(main())
