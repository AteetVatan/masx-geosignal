"""
Seed a MINIMAL flash_point + feed_entries dataset for quick testing.

Creates 2 flashpoints with 2 live-URL entries each (4 total).
All URLs are verified-live Al Jazeera articles as of Feb 2026.

Usage:
    python scripts/seed_mini_data.py                # uses today's date
    python scripts/seed_mini_data.py --date 2026-02-13
    python scripts/seed_mini_data.py --drop          # drop existing + recreate
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from datetime import UTC, date, datetime

import click
from sqlalchemy import text

# ── Ensure project root is on sys.path ────────────────
sys.path.insert(0, ".")

from core.db.engine import get_async_session
from core.db.table_resolver import make_table_name

# ───────────────────────────────────────────────────────
#  Mini Dataset — 2 flashpoints, 2 entries each
# ───────────────────────────────────────────────────────

FLASHPOINTS = [
    {
        "title": "Myanmar Civil War",
        "description": (
            "Ongoing civil war following the February 2021 military coup. "
            "Resistance forces including the People's Defense Force and ethnic "
            "armed organizations are fighting the military junta across the "
            "country. The junta held controversial elections in late 2025 and "
            "early 2026, widely condemned as illegitimate."
        ),
        "entities": [
            "Myanmar",
            "Tatmadaw",
            "National Unity Government",
            "People's Defense Force",
            "Aung San Suu Kyi",
        ],
        "domains": [
            "aljazeera.com",
            "bbc.com",
            "reuters.com",
            "irrawaddy.com",
        ],
    },
    {
        "title": "Ethiopia-Tigray Conflict",
        "description": (
            "Renewed clashes between Ethiopian government forces and Tigrayan "
            "fighters in early 2026, threatening the fragile 2022 Pretoria "
            "peace agreement. Drone strikes, flight suspensions, and fears of "
            "a return to full-scale conflict in northern Ethiopia."
        ),
        "entities": [
            "Ethiopia",
            "Tigray",
            "TPLF",
            "Abiy Ahmed",
            "Eritrea",
            "African Union",
        ],
        "domains": [
            "aljazeera.com",
            "bbc.com",
            "reuters.com",
            "reliefweb.int",
        ],
    },
]

FEED_ENTRIES_BY_FP = {
    "Myanmar Civil War": [
        {
            "url": "https://www.aljazeera.com/news/2026/1/31/myanmar-election-delivers-walkover-win-for-military-backed-political-party",
            "title": "Myanmar election delivers walkover win for military-backed political party",
            "description": "Myanmar's military rulers say polls were free and fair as UN reports 170 killed during election period.",
            "domain": "aljazeera.com",
            "language": "en",
            "sourcecountry": "Qatar",
            "seendate": "8888-88-88",
        },
        {
            "url": "https://www.aljazeera.com/news/2026/1/11/myanmars-military-holds-second-phase-of-elections-amid-civil-war",
            "title": "Myanmar's military holds second phase of elections amid civil war",
            "description": "Polls have opened in 100 townships across the country, with the military claiming 52 percent turnout in the first round.",
            "domain": "aljazeera.com",
            "language": "en",
            "sourcecountry": "Qatar",
            "seendate": "8888-88-88",
        },
    ],
    "Ethiopia-Tigray Conflict": [
        {
            "url": "https://www.aljazeera.com/news/2026/1/31/drone-strikes-in-ethiopias-tigray-kill-one-amid-fears-of-renewed-conflict",
            "title": "Drone strikes in Ethiopia's Tigray kill one amid fears of renewed conflict",
            "description": "The attack comes amid fears of a return to conflict following clashes between government troops and Tigrayan forces.",
            "domain": "aljazeera.com",
            "language": "en",
            "sourcecountry": "Qatar",
            "seendate": "8888-88-88",
        },
        {
            "url": "https://www.aljazeera.com/news/2026/1/29/clashes-between-government-troops-and-tigrayan-forces-erupt-in-ethiopia",
            "title": "Clashes between government troops and Tigrayan forces erupt in Ethiopia",
            "description": "'Deteriorating' situation causes suspension of flights, security and diplomatic sources say.",
            "domain": "aljazeera.com",
            "language": "en",
            "sourcecountry": "Qatar",
            "seendate": "8888-88-88",
        },
    ],
}


# ───────────────────────────────────────────────────────
#  Table Creation + Seeding (reuses same schema as seed_debug_data.py)
# ───────────────────────────────────────────────────────


async def create_tables(session, fp_table: str, fe_table: str, drop: bool = False):
    """Create flash_point and feed_entries tables."""
    if drop:
        await session.execute(text(f'DROP TABLE IF EXISTS "{fe_table}" CASCADE'))
        await session.execute(text(f'DROP TABLE IF EXISTS "{fp_table}" CASCADE'))

    await session.execute(
        text(f"""
        CREATE TABLE IF NOT EXISTS "{fp_table}" (
            id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            title       TEXT NOT NULL,
            description TEXT NOT NULL,
            entities    JSONB,
            domains     JSONB,
            run_id      TEXT,
            created_at  TIMESTAMPTZ DEFAULT NOW(),
            updated_at  TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    )

    await session.execute(
        text(f"""
        CREATE TABLE IF NOT EXISTS "{fe_table}" (
            id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            flashpoint_id       UUID REFERENCES "{fp_table}"(id) ON DELETE CASCADE,
            url                 TEXT,
            title               TEXT,
            seendate            TEXT,
            domain              TEXT,
            language            TEXT,
            sourcecountry       TEXT,
            description         TEXT,
            image               TEXT,
            title_en            TEXT,
            images              TEXT[] DEFAULT '{{}}'::TEXT[],
            hostname            TEXT,
            content             TEXT,
            compressed_content  TEXT,
            summary             TEXT,
            entities            JSONB,
            geo_entities        JSONB,
            created_at          TIMESTAMPTZ DEFAULT NOW(),
            updated_at          TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    )
    await session.commit()
    print(f"  ✓ Tables: {fp_table}, {fe_table}")


async def seed_data(session, fp_table: str, fe_table: str) -> int:
    """Insert flashpoints + feed entries. Returns entry count."""
    import json

    now = datetime.now(UTC)
    fp_map: dict[str, uuid.UUID] = {}
    total = 0

    # Flashpoints
    for fp in FLASHPOINTS:
        fp_id = uuid.uuid4()
        fp_map[fp["title"]] = fp_id
        await session.execute(
            text(f"""
                INSERT INTO "{fp_table}" (id, title, description, entities, domains, run_id, created_at, updated_at)
                VALUES (:id, :title, :description, CAST(:entities AS jsonb), CAST(:domains AS jsonb), :run_id, :created_at, :updated_at)
            """),
            {
                "id": fp_id,
                "title": fp["title"],
                "description": fp["description"],
                "entities": json.dumps(fp["entities"]),
                "domains": json.dumps(fp["domains"]),
                "run_id": "mini-seed-001",
                "created_at": now,
                "updated_at": now,
            },
        )
    print(f"  ✓ {len(FLASHPOINTS)} flashpoints inserted")

    # Feed entries
    for fp_title, entries in FEED_ENTRIES_BY_FP.items():
        fp_id = fp_map[fp_title]
        for entry in entries:
            await session.execute(
                text(f"""
                    INSERT INTO "{fe_table}" (
                        flashpoint_id, url, title, seendate, domain,
                        language, sourcecountry, description
                    )
                    VALUES (
                        :flashpoint_id, :url, :title, :seendate, :domain,
                        :language, :sourcecountry, :description
                    )
                """),
                {
                    "flashpoint_id": fp_id,
                    "url": entry["url"],
                    "title": entry["title"],
                    "seendate": entry["seendate"],
                    "domain": entry["domain"],
                    "language": entry["language"],
                    "sourcecountry": entry["sourcecountry"],
                    "description": entry["description"],
                },
            )
            total += 1

    await session.commit()
    print(f"  ✓ {total} feed entries inserted (content NULL — ready for pipeline)")
    return total


# ───────────────────────────────────────────────────────
#  CLI
# ───────────────────────────────────────────────────────


@click.command()
@click.option(
    "--date", "target_date", default=None, help="Date suffix (YYYY-MM-DD). Default: today"
)
@click.option("--drop", is_flag=True, help="Drop existing tables before creating")
def cli(target_date: str | None, drop: bool):
    """Seed a mini 2-flashpoint, 4-entry dataset for quick pipeline testing."""
    asyncio.run(_main(target_date, drop))


async def _main(target_date_str: str | None, drop: bool):
    if target_date_str:
        # Accept raw date strings (e.g. 8888-88-88) — build table suffix directly
        suffix = target_date_str.replace("-", "")
        fp_table = f"flash_point_{suffix}"
        fe_table = f"feed_entries_{suffix}"
        target_label = target_date_str
    else:
        target = date.today()
        fp_table = make_table_name("flash_point", target)
        fe_table = make_table_name("feed_entries", target)
        target_label = str(target)

    print("╔══════════════════════════════════════════════╗")
    print("║  MASX-GSGI Mini Data Seeder                 ║")
    print("╠══════════════════════════════════════════════╣")
    print(f"║  Date:    {target_label:<33}║")
    print(f"║  FP tbl:  {fp_table:<33}║")
    print(f"║  FE tbl:  {fe_table:<33}║")
    print(f"║  Drop:    {drop!s:<33}║")
    print(f"║  Entries: 4 (2 flashpoints × 2)             ║")
    print("╚══════════════════════════════════════════════╝")
    print()

    session_factory = get_async_session()

    async with session_factory() as session:
        # Check existing data
        try:
            result = await session.execute(text(f'SELECT COUNT(*) FROM "{fp_table}"'))
            existing = result.scalar()
            if existing and existing > 0 and not drop:
                print(f"  ⚠ Tables already have data ({existing} flashpoints). Use --drop to reseed.")
                return
        except Exception:
            await session.rollback()  # table doesn't exist yet — that's fine

        # Create + seed
        print("1. Creating tables...")
        await create_tables(session, fp_table, fe_table, drop=drop)

        print("\n2. Seeding data...")
        await seed_data(session, fp_table, fe_table)

    print("\n✓ Done! Run pipeline with:")
    print(f"  python -m apps.orchestrator.main --date {target_label}")


if __name__ == "__main__":
    cli()
