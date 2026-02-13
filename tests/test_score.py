"""
Tests for the hotspot scoring module.

Covers:
- Score range [0, 1]
- Component weights
- Recency decay behavior
- Topic weight application
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from core.pipeline.score import compute_hotspot_score


class TestHotspotScoring:
    """Test hotspot score computation."""

    def test_score_in_valid_range(self) -> None:
        score = compute_hotspot_score(
            article_count=10,
            unique_domains=5,
            max_recency=datetime.now(UTC),
            primary_topic="conflict, war and peace",
        )
        assert 0.0 <= score.score <= 1.0

    def test_higher_article_count_higher_score(self) -> None:
        score_low = compute_hotspot_score(
            article_count=2,
            unique_domains=2,
            max_recency=datetime.now(UTC),
        )
        score_high = compute_hotspot_score(
            article_count=50,
            unique_domains=2,
            max_recency=datetime.now(UTC),
        )
        assert score_high.score > score_low.score

    def test_recency_decay(self) -> None:
        now = datetime.now(UTC)

        score_recent = compute_hotspot_score(
            article_count=10,
            unique_domains=3,
            max_recency=now,
            now=now,
        )
        score_old = compute_hotspot_score(
            article_count=10,
            unique_domains=3,
            max_recency=now - timedelta(hours=48),
            now=now,
        )
        assert score_recent.score > score_old.score
        assert score_recent.components["recency"] > score_old.components["recency"]

    def test_conflict_topic_scores_higher(self) -> None:
        now = datetime.now(UTC)

        score_conflict = compute_hotspot_score(
            article_count=10,
            unique_domains=3,
            max_recency=now,
            primary_topic="conflict, war and peace",
            now=now,
        )
        score_sport = compute_hotspot_score(
            article_count=10,
            unique_domains=3,
            max_recency=now,
            primary_topic="sport",
            now=now,
        )
        assert score_conflict.score > score_sport.score

    def test_diversity_component(self) -> None:
        now = datetime.now(UTC)

        score_diverse = compute_hotspot_score(
            article_count=10,
            unique_domains=15,
            max_recency=now,
            now=now,
        )
        score_single = compute_hotspot_score(
            article_count=10,
            unique_domains=1,
            max_recency=now,
            now=now,
        )
        assert score_diverse.components["diversity"] > score_single.components["diversity"]

    def test_no_recency(self) -> None:
        score = compute_hotspot_score(
            article_count=10,
            unique_domains=3,
            max_recency=None,
        )
        assert score.components["recency"] == 0.0

    def test_components_present(self) -> None:
        score = compute_hotspot_score(
            article_count=10,
            unique_domains=3,
            max_recency=datetime.now(UTC),
        )
        assert "volume" in score.components
        assert "recency" in score.components
        assert "diversity" in score.components
        assert "topic_weight" in score.components

    def test_zero_articles(self) -> None:
        score = compute_hotspot_score(
            article_count=0,
            unique_domains=0,
            max_recency=None,
        )
        assert score.score >= 0.0
