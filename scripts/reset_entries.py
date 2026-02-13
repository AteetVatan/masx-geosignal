"""Reset enrichment columns so the pipeline can be re-tested.

Usage:
    python scripts/reset_entries.py
"""
import asyncio

from sqlalchemy import text

from core.db.engine import get_async_engine


async def reset() -> None:
    engine = get_async_engine()

    async with engine.begin() as conn:
        # Find the latest feed_entries table
        r = await conn.execute(text(
            "SELECT tablename FROM pg_tables "
            "WHERE tablename LIKE 'feed_entries_%' "
            "ORDER BY tablename DESC LIMIT 1"
        ))
        table = r.scalar_one()
        print(f"Target table: {table}")

        # 1. Reset enrichment columns
        r1 = await conn.execute(text(
            f'UPDATE "{table}" '
            "SET content = NULL, title_en = NULL, hostname = NULL, "
            "summary = NULL, entities = NULL, geo_entities = NULL, images = NULL"
        ))
        print(f"Reset {r1.rowcount} feed_entries rows")

        # 2. Delete job records
        r2 = await conn.execute(text("DELETE FROM feed_entry_jobs"))
        print(f"Deleted {r2.rowcount} feed_entry_jobs rows")

        # 3. Delete processing runs
        r3 = await conn.execute(text("DELETE FROM processing_runs"))
        print(f"Deleted {r3.rowcount} processing_runs rows")

    await engine.dispose()
    print("Done — ready for re-test.")


if __name__ == "__main__":
    asyncio.run(reset())
