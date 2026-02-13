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


class PipelineTier(enum.StrEnum):
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

    # ── LLM (provider-agnostic, OpenAI-compatible API) ─
    llm_provider: str = Field(
        default="together",
        description="LLM provider name: together, openai, mistral, groq, deepseek",
    )
    llm_api_key: SecretStr = Field(
        default=SecretStr(""),
        description="API key for the LLM provider",
    )
    llm_base_url: str = Field(
        default="https://api.together.xyz/v1",
        description="Base URL for the OpenAI-compatible API endpoint",
    )
    llm_model: str = Field(
        default="meta-llama/Llama-3.2-3B-Instruct-Turbo",
        description="Model identifier for the LLM provider",
    )
    llm_batch_enabled: bool = Field(
        default=False,
        description="Use Batch API (only supported by OpenAI)",
    )
    llm_rpm_limit: int = Field(
        default=600,
        description="Max LLM requests per minute (Together AI free tier = 600)",
    )
    llm_summarize_batch_size: int = Field(
        default=20,
        description="Number of clusters to summarize concurrently per batch",
    )
    # ── Fallback LLM (used when primary fails after retries) ─
    llm_provider_fallback: str = Field(
        default="mistral",
        description="Fallback LLM provider used when primary fails after retries",
    )
    llm_fallback_api_key: SecretStr = Field(
        default=SecretStr(""),
        description="API key for fallback LLM provider",
    )
    llm_fallback_base_url: str = Field(
        default="https://api.mistral.ai/v1",
        description="Base URL for fallback LLM provider",
    )
    llm_fallback_model: str = Field(
        default="mistral-small-latest",
        description="Model identifier for fallback LLM provider",
    )

    # Legacy fallback — if LLM_API_KEY is empty, try OPENAI_API_KEY
    openai_api_key: SecretStr = Field(default=SecretStr(""))

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

    # ── ML model paths ───────────────────────────────
    iptc_model_dir: str = "models/iptc-classifier"
    ner_model: str = "Davlan/distilbert-base-multilingual-cased-ner-hrl"
    local_summarizer_model: str = "sshleifer/distilbart-cnn-12-6"
    local_summarizer_onnx_dir: str = "models/distilbart-cnn-onnx"
    local_summarizer_workers: int = 2  # each loads ~2.4 GB ONNX; keep low
    fasttext_model_dir: str = "models"

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

    # ── API Trigger ──────────────────────────────────
    pipeline_api_key: SecretStr = Field(
        default=SecretStr(""),
        description="API key for the pipeline trigger endpoint",
    )

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

    @property
    def resolved_llm_api_key(self) -> str:
        """Return the LLM API key, falling back to OPENAI_API_KEY."""
        key = self.llm_api_key.get_secret_value()
        if key:
            return key
        return self.openai_api_key.get_secret_value()

    @property
    def resolved_fallback_api_key(self) -> str:
        """Return the fallback LLM API key."""
        return self.llm_fallback_api_key.get_secret_value()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached singleton settings instance."""
    return Settings()  # type: ignore[call-arg, unused-ignore]
