"""
Reset the mini (8888-88-88) dataset: drop all tables, recreate, and reseed.

Usage:
    python scripts/reset_entries_mini_db.py
"""

from __future__ import annotations

import asyncio
import sys

sys.path.insert(0, ".")

from sqlalchemy import text

from core.db.engine import get_async_session

SUFFIX = "88888888"
TABLES = [
    f"news_clusters_{SUFFIX}",
    f"feed_entries_{SUFFIX}",
    f"flash_point_{SUFFIX}",
    f"processing_runs",  # clear run records too
]


async def reset():
    session_factory = get_async_session()

    async with session_factory() as session:
        # 1. Drop tables
        print("1. Dropping mini-db tables...")
        for table in TABLES:
            if table == "processing_runs":
                # Only delete runs targeting mini data, not the whole table
                await session.execute(
                    text("DELETE FROM processing_runs WHERE target_date = :td"),
                    {"td": SUFFIX},
                )
                print(f"   ✓ Cleaned processing_runs for {SUFFIX}")
            else:
                await session.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE'))
                print(f"   ✓ Dropped {table}")
        await session.commit()

    # 2. Reseed
    print("\n2. Reseeding...")
    from scripts.seed_mini_data import _main

    await _main(f"8888-88-88", drop=False)


if __name__ == "__main__":
    asyncio.run(reset())
