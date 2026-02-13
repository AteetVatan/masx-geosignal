"""
Tests for configuration and settings.

Covers:
- Tier properties
- Default values
- Environment variable loading
"""

from __future__ import annotations

from core.config.settings import PipelineTier, Settings


class TestPipelineTier:
    """Test tier-based feature flags."""

    def test_tier_a_no_embeddings(self) -> None:
        s = Settings(database_url="postgresql+asyncpg://x", pipeline_tier=PipelineTier.A)
        assert not s.tier_has_embeddings
        assert not s.tier_has_clustering
        assert not s.tier_has_llm

    def test_tier_b_has_embeddings_and_clustering(self) -> None:
        s = Settings(database_url="postgresql+asyncpg://x", pipeline_tier=PipelineTier.B)
        assert s.tier_has_embeddings
        assert s.tier_has_clustering
        assert not s.tier_has_llm

    def test_tier_c_has_everything(self) -> None:
        s = Settings(database_url="postgresql+asyncpg://x", pipeline_tier=PipelineTier.C)
        assert s.tier_has_embeddings
        assert s.tier_has_clustering
        assert s.tier_has_llm


class TestDefaults:
    """Test settings defaults."""

    def test_default_embedding_model(self) -> None:
        s = Settings(database_url="postgresql+asyncpg://x")
        assert s.embedding_model == "all-MiniLM-L6-v2"
        assert s.embedding_dimension == 384

    def test_default_concurrency(self) -> None:
        s = Settings(database_url="postgresql+asyncpg://x")
        assert s.max_concurrent_fetches == 50
        assert s.per_domain_concurrency == 3

    def test_default_tier_is_a(self) -> None:
        s = Settings(database_url="postgresql+asyncpg://x")
        assert s.pipeline_tier == PipelineTier.A

    def test_is_production(self) -> None:
        s = Settings(database_url="postgresql+asyncpg://x", railway_environment="production")
        assert s.is_production

    def test_not_production_by_default(self) -> None:
        s = Settings(database_url="postgresql+asyncpg://x")
        assert not s.is_production
