"""
Date-partitioned table resolver for Supabase.

The Supabase database uses date-partitioned tables named like:
  - feed_entries_YYYYMMDD
  - flash_point_YYYYMMDD
  - news_clusters_YYYYMMDD

This module resolves logical table names (e.g. "feed_entries") to the
actual physical tables in the database, using date suffixes.

The pipeline can target either:
  - A specific date's tables (for historical reprocessing)
  - The "latest" tables (for daily pipeline runs)
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

# Date pattern in table names: YYYYMMDD
_DATE_SUFFIX_RE = re.compile(r"_(\d{8})$")


def make_table_name(base_name: str, target_date: date) -> str:
    """Build a date-partitioned table name.

    >>> make_table_name("feed_entries", date(2025, 11, 3))
    'feed_entries_20251103'
    """
    return f"{base_name}_{target_date.strftime('%Y%m%d')}"


def extract_date_from_table(table_name: str) -> date | None:
    """Extract the date suffix from a partitioned table name.

    >>> extract_date_from_table("feed_entries_20251103")
    datetime.date(2025, 11, 3)
    """
    m = _DATE_SUFFIX_RE.search(table_name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d").date()
    except ValueError:
        return None


async def list_table_dates(
    session: AsyncSession,
    base_name: str,
) -> list[date]:
    """List all available dates for a given table base name.

    Returns dates in descending order (most recent first).
    """
    result = await session.execute(
        text(
            "SELECT tablename FROM pg_tables "
            "WHERE schemaname = 'public' "
            "AND tablename LIKE :pattern "
            "AND tablename NOT LIKE '%duplicate%' "
            "ORDER BY tablename DESC"
        ),
        {"pattern": f"{base_name}_%"},
    )
    dates = []
    for (table_name,) in result:
        d = extract_date_from_table(table_name)
        if d is not None:
            dates.append(d)
    return dates


async def get_latest_table_date(
    session: AsyncSession,
    base_name: str,
) -> date | None:
    """Get the most recent date suffix for a table base name.

    Returns None if no partitioned tables exist.
    """
    dates = await list_table_dates(session, base_name)
    return dates[0] if dates else None


async def resolve_tables(
    session: AsyncSession,
    target_date: date | None = None,
) -> dict[str, str]:
    """Resolve logical table names to physical date-partitioned names.

    If target_date is None, uses the latest available date from
    feed_entries tables.

    Returns a dict like:
        {
            "feed_entries": "feed_entries_20251103",
            "flash_point": "flash_point_20251103",
            "news_clusters": "news_clusters_20251103",
        }

    feed_entries and flash_point MUST exist. news_clusters is the output
    table and may not exist yet (use ensure_output_table to create it).

    Raises ValueError if required input tables don't exist.
    """
    if target_date is None:
        target_date = await get_latest_table_date(session, "feed_entries")
        if target_date is None:
            raise ValueError("No feed_entries tables found in the database")

    tables = {
        "feed_entries": make_table_name("feed_entries", target_date),
        "flash_point": make_table_name("flash_point", target_date),
        "news_clusters": make_table_name("news_clusters", target_date),
    }

    # Only verify INPUT tables exist (feed_entries and flash_point).
    # news_clusters is an OUTPUT table that may need to be created.
    required_tables = ["feed_entries", "flash_point"]
    for logical_name in required_tables:
        physical_name = tables[logical_name]
        result = await session.execute(
            text(
                "SELECT EXISTS ("
                "  SELECT 1 FROM pg_tables "
                "  WHERE schemaname = 'public' AND tablename = :name"
                ")"
            ),
            {"name": physical_name},
        )
        exists = result.scalar()
        if not exists:
            raise ValueError(
                f"Table '{physical_name}' does not exist for date {target_date} "
                f"(logical: {logical_name})"
            )

    return tables


async def ensure_output_table(
    session: AsyncSession,
    target_date: date | None = None,
) -> str:
    """Ensure the news_clusters output table exists for the target date.

    If the table doesn't exist, creates it using the schema from the
    latest existing news_clusters table.

    Returns the physical table name.
    """
    if target_date is None:
        target_date = date.today()

    table_name = make_table_name("news_clusters", target_date)

    # Check if it already exists
    result = await session.execute(
        text(
            "SELECT EXISTS ("
            "  SELECT 1 FROM pg_tables "
            "  WHERE schemaname = 'public' AND tablename = :name"
            ")"
        ),
        {"name": table_name},
    )
    if result.scalar():
        return table_name

    # Create it based on the canonical schema (matches news_clusters_YYYYMMDD spec)
    await session.execute(
        text(f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                id BIGSERIAL PRIMARY KEY,
                flashpoint_id uuid NOT NULL,
                cluster_id integer NOT NULL,
                summary text NOT NULL,
                article_count integer NOT NULL,
                top_domains jsonb DEFAULT '[]'::jsonb,
                languages jsonb DEFAULT '[]'::jsonb,
                urls jsonb DEFAULT '[]'::jsonb,
                images jsonb DEFAULT '[]'::jsonb,
                created_at timestamptz DEFAULT CURRENT_TIMESTAMP
            )
        """)
    )
    await session.commit()

    return table_name


class TableContext:
    """Holds the resolved table names for a pipeline run.

    This is the single source of truth for which physical tables
    a pipeline run should read from and write to.
    """

    def __init__(
        self,
        feed_entries: str,
        flash_point: str,
        news_clusters: str,
        target_date: date,
    ) -> None:
        self.feed_entries = feed_entries
        self.flash_point = flash_point
        self.news_clusters = news_clusters
        self.target_date = target_date

    @classmethod
    async def create(
        cls,
        session: AsyncSession,
        target_date: date | None = None,
    ) -> TableContext:
        """Create a TableContext by resolving date-partitioned tables.

        Args:
            session: Async database session.
            target_date: Specific date to target. If None, uses the
                        latest available date from feed_entries tables.
        """
        tables = await resolve_tables(session, target_date)

        # Resolve the actual target_date from the feed_entries table name
        resolved_date = extract_date_from_table(tables["feed_entries"])
        if resolved_date is None:
            resolved_date = target_date or date.today()

        return cls(
            feed_entries=tables["feed_entries"],
            flash_point=tables["flash_point"],
            news_clusters=tables["news_clusters"],
            target_date=resolved_date,
        )

    def __repr__(self) -> str:
        return (
            f"TableContext(date={self.target_date}, "
            f"feed={self.feed_entries}, fp={self.flash_point}, "
            f"nc={self.news_clusters})"
        )
