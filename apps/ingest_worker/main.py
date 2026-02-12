"""
Ingest Worker CLI — standalone entry point for the ingest stage.

Can be run as a Railway cron job independently of the orchestrator.
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from datetime import date, datetime, timezone

import click
import structlog

from core.config import get_settings
from core.config.logging import setup_logging

logger = structlog.get_logger(__name__)


async def run_ingest(
    run_id: str | None = None,
    target_date: date | None = None,
) -> None:
    """Execute ingestion stage."""
    settings = get_settings()
    setup_logging(settings.log_level, settings.log_format)

    if not run_id:
        run_id = f"ingest_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    from core.db.engine import get_async_session
    from core.db.repositories import FeedEntryRepo
    from core.db.table_resolver import TableContext

    session_factory = get_async_session()

    async with session_factory() as session:
        # Resolve tables
        table_ctx = await TableContext.create(session, target_date)
        logger.info("tables_resolved", table_ctx=repr(table_ctx))

        entry_repo = FeedEntryRepo(session)
        entries = await entry_repo.get_unprocessed(table_ctx, run_id, limit=10000)

        if not entries:
            logger.info("no_entries_to_ingest")
            return

        from apps.ingest_worker.service import IngestService

        svc = IngestService(session, run_id, settings, table_ctx)
        await svc.process_batch(entries)
        await session.commit()


@click.command()
@click.option("--run-id", default=None, help="Existing run ID to continue")
@click.option("--date", "target_date_str", default=None, help="Target date (YYYY-MM-DD)")
def cli(run_id: str | None, target_date_str: str | None) -> None:
    """Run the ingestion worker."""
    target_date = date.fromisoformat(target_date_str) if target_date_str else None
    try:
        asyncio.run(run_ingest(run_id, target_date))
    except Exception:
        logger.exception("ingest_worker_crashed")
        sys.exit(1)


if __name__ == "__main__":
    cli()
