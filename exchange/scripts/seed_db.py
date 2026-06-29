#!/usr/bin/env python3
"""Seed investors and initial balances into each bank's PostgreSQL database."""
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


async def seed_bank(db_url: str, bank_id: str) -> None:
    conn = await asyncpg.connect(db_url)
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
        print(f"seeded {INVESTORS_PER_BANK} investors for {bank_id}")
    finally:
        await conn.close()


async def main() -> None:
    base_url = os.environ.get(
        "DB_BASE_URL", "postgresql://exchange:exchange@localhost/exchange"
    )
    tasks = [
        seed_bank(f"{base_url}_{bank_id}", bank_id)
        for bank_id in BANKS
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
