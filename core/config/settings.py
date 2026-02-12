"""
Pydantic Settings — single source of truth for all configuration.

Reads from environment variables (and .env file in dev).
Every pipeline component imports `get_settings()` to resolve its config.
"""

from __future__ import annotations

import enum
from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineTier(str, enum.Enum):
    """Cost/quality tiers for the pipeline."""

    A = "A"  # Cheapest: fetch + extract + dedupe + metadata
    B = "B"  # Balanced: + embeddings + clustering + local summarization
    C = "C"  # Best quality: + Batch LLM summaries + translation + premium pass


class Settings(BaseSettings):
    """Application-wide settings loaded from env vars."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Database ──────────────────────────────────────
    database_url: str = Field(
        ...,
        description="Async Postgres DSN (postgresql+asyncpg://...)",
    )
    database_url_sync: str = Field(
        default="",
        description="Sync Postgres DSN for Alembic migrations",
    )

    # ── Supabase ──────────────────────────────────────
    supabase_url: str = Field(default="", description="Supabase project URL")
    supabase_anon_key: str = Field(default="", description="Supabase anon/public key")
    supabase_service_role_key: SecretStr = Field(
        default=SecretStr(""), description="Supabase service role key"
    )
    supabase_db_password: SecretStr = Field(
        default=SecretStr(""), description="Supabase database password"
    )

    # ── OpenAI ────────────────────────────────────────
    openai_api_key: SecretStr = Field(default=SecretStr(""))
    openai_model: str = "gpt-4o-mini"
    openai_batch_enabled: bool = True

    # ── Pipeline tier ─────────────────────────────────
    pipeline_tier: PipelineTier = PipelineTier.A

    # ── Concurrency ──────────────────────────────────
    max_concurrent_fetches: int = 50
    per_domain_concurrency: int = 3
    fetch_timeout_seconds: int = 30
    request_delay_seconds: float = 0.25

    # ── Embedding ────────────────────────────────────
    embedding_model: str = "all-MiniLM-L6-v2"
    embedding_dimension: int = 384

    # ── Clustering ───────────────────────────────────
    cluster_knn_k: int = 10
    cluster_cosine_threshold: float = 0.65

    # ── Extraction ───────────────────────────────────
    min_content_length: int = 200
    playwright_enabled: bool = False
    extraction_timeout_seconds: int = 20

    # ── Dedupe ───────────────────────────────────────
    minhash_num_perm: int = 128
    minhash_threshold: float = 0.8

    # ── Scoring ──────────────────────────────────────
    premium_llm_top_pct: float = 0.10  # Top 10% clusters get premium pass

    # ── Logging ──────────────────────────────────────
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_format: Literal["json", "console"] = "json"

    # ── Railway ──────────────────────────────────────
    railway_environment: str = "development"
    port: int = 8080

    @property
    def is_production(self) -> bool:
        return self.railway_environment == "production"

    @property
    def tier_has_embeddings(self) -> bool:
        return self.pipeline_tier in (PipelineTier.B, PipelineTier.C)

    @property
    def tier_has_clustering(self) -> bool:
        return self.pipeline_tier in (PipelineTier.B, PipelineTier.C)

    @property
    def tier_has_llm(self) -> bool:
        return self.pipeline_tier == PipelineTier.C


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached singleton settings instance."""
    return Settings()  # type: ignore[call-arg]
