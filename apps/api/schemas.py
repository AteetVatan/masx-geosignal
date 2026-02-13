"""Pydantic schemas for the Pipeline Trigger API."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field

from core.config.settings import PipelineTier


class TriggerRequest(BaseModel):
    """Request body for POST /pipeline/run."""

    target_date: date = Field(
        description="Target date (YYYY-MM-DD) for the feed tables to process",
    )
    tier: PipelineTier | None = Field(
        default=None,
        description="Pipeline tier override (A/B/C). Uses env default if omitted",
    )


class TriggerResponse(BaseModel):
    """Response body for POST /pipeline/run."""

    status: str = "accepted"
    target_date: str
    message: str = "Pipeline triggered successfully"


class RunStatusResponse(BaseModel):
    """Single pipeline run status."""

    run_id: str
    status: str
    pipeline_tier: str
    target_date: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_message: str | None = None
    metrics: dict[str, Any] | None = None
    created_at: datetime | None = None


class HealthResponse(BaseModel):
    """Response body for GET /health."""

    status: str = "ok"
    version: str = "0.1.0"
