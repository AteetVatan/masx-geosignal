"""
Pipeline Trigger API — FastAPI application.

Endpoints:
    GET  /health                     Unauthenticated liveness check
    POST /pipeline/run               Trigger a pipeline run (API key required)
    GET  /pipeline/runs?date=...     List runs for a date (API key required)
    GET  /pipeline/runs/{run_id}     Get single run status (API key required)
"""

from __future__ import annotations

import asyncio
import secrets
import sys
from contextlib import asynccontextmanager
from typing import Any

import click
import structlog
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.security import APIKeyHeader

from core.config import get_settings
from core.db.engine import get_async_session
from core.db.repositories import ProcessingRunRepo

from .schemas import (
    HealthResponse,
    RunStatusResponse,
    TriggerRequest,
    TriggerResponse,
)

logger = structlog.get_logger(__name__)

_API_KEY_HEADER = APIKeyHeader(name="X-Api-Key", auto_error=False)


# ── Auth dependency ──────────────────────────────────────────


async def verify_api_key(api_key: str | None = Depends(_API_KEY_HEADER)) -> str:
    """Validate API key using constant-time comparison (timing-attack safe)."""
    settings = get_settings()
    expected = settings.pipeline_api_key.get_secret_value()

    if not expected:
        raise HTTPException(
            status_code=500,
            detail="PIPELINE_API_KEY not configured on server",
        )

    if api_key is None or not secrets.compare_digest(api_key, expected):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

    return api_key


# ── Lifespan ─────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001
    """Validate critical settings on startup."""
    settings = get_settings()
    if settings.is_production and not settings.pipeline_api_key.get_secret_value():
        logger.critical("PIPELINE_API_KEY must be set in production")
        sys.exit(1)
    logger.info("api_started", port=settings.port)
    yield
    logger.info("api_shutdown")


# ── App ──────────────────────────────────────────────────────

app = FastAPI(
    title="GSGI Pipeline Trigger API",
    version="0.1.0",
    lifespan=lifespan,
)


# ── Helpers ──────────────────────────────────────────────────


def _run_to_response(run: Any) -> RunStatusResponse:
    """Convert a ProcessingRun ORM object to a response schema."""
    return RunStatusResponse(
        run_id=run.run_id,
        status=run.status.value if hasattr(run.status, "value") else str(run.status),
        pipeline_tier=run.pipeline_tier,
        target_date=run.target_date,
        started_at=run.started_at,
        completed_at=run.completed_at,
        error_message=run.error_message,
        metrics=run.metrics,
        created_at=run.created_at,
    )


# ── Endpoints ────────────────────────────────────────────────


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Unauthenticated health check."""
    return HealthResponse()


@app.post(
    "/pipeline/run",
    response_model=TriggerResponse,
    status_code=202,
    dependencies=[Depends(verify_api_key)],
)
async def trigger_pipeline(body: TriggerRequest) -> TriggerResponse:
    """Trigger a pipeline run as a subprocess."""
    settings = get_settings()
    session_factory = get_async_session()

    async with session_factory() as session:
        run_repo = ProcessingRunRepo(session)

        # Auto-recover stale runs before checking
        stale_count = await run_repo.mark_stale_runs_failed()
        if stale_count:
            await session.commit()

        # Reject if pipeline is already running
        if await run_repo.has_active_run():
            raise HTTPException(
                status_code=409,
                detail="A pipeline run is already in progress",
            )

    # Build subprocess command
    cmd = [sys.executable, "-m", "apps.orchestrator.main", "--date", str(body.target_date)]

    tier = body.tier or settings.pipeline_tier
    cmd.extend(["--tier", tier.value])

    logger.info(
        "pipeline_trigger",
        target_date=str(body.target_date),
        tier=tier.value,
        cmd=cmd,
    )

    # Spawn child process (non-blocking)
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        logger.info("pipeline_subprocess_started", pid=process.pid)
    except Exception as exc:
        logger.exception("pipeline_spawn_failed", error=str(exc))
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start pipeline: {exc}",
        ) from exc

    return TriggerResponse(
        target_date=str(body.target_date),
        message=f"Pipeline triggered for {body.target_date} (tier {tier.value})",
    )


@app.get(
    "/pipeline/runs",
    response_model=list[RunStatusResponse],
    dependencies=[Depends(verify_api_key)],
)
async def list_runs(
    date: str = Query(..., description="Target date (YYYY-MM-DD)"),
) -> list[RunStatusResponse]:
    """List pipeline runs for a given target date."""
    session_factory = get_async_session()

    async with session_factory() as session:
        run_repo = ProcessingRunRepo(session)
        runs = await run_repo.get_runs_by_date(date)

    if not runs:
        raise HTTPException(status_code=404, detail=f"No runs found for date {date}")

    return [_run_to_response(r) for r in runs]


@app.get(
    "/pipeline/runs/{run_id}",
    response_model=RunStatusResponse,
    dependencies=[Depends(verify_api_key)],
)
async def get_run(run_id: str) -> RunStatusResponse:
    """Get status of a specific pipeline run."""
    session_factory = get_async_session()

    async with session_factory() as session:
        run_repo = ProcessingRunRepo(session)
        run = await run_repo.get_run_by_id(run_id)

    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    return _run_to_response(run)


# ── CLI ──────────────────────────────────────────────────────


@click.command()
@click.option("--host", default="0.0.0.0", help="Bind host")
@click.option("--port", default=None, type=int, help="Bind port (defaults to PORT env)")
def cli(host: str, port: int | None) -> None:
    """Start the Pipeline Trigger API server."""
    settings = get_settings()
    port = port or settings.port
    uvicorn.run(
        "apps.api.main:app",
        host=host,
        port=port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    cli()
