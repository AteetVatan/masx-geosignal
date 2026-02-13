"""
Hotspot scoring module.

Computes a "hotspot score" for each cluster based on:
  - article_count (volume signal)
  - recency (how recent the articles are)
  - source_diversity (number of unique domains)
  - topic_weight (from IPTC top-level category)
  - entity_risk_signals (optional — placeholder)

Score is normalized to [0, 1] range.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime

import structlog

logger = structlog.get_logger(__name__)

# IPTC topic weights — conflict/disaster topics score higher
TOPIC_WEIGHTS: dict[str, float] = {
    "conflict, war and peace": 1.0,
    "disaster, accident and emergency incident": 0.9,
    "crime, law and justice": 0.8,
    "politics": 0.7,
    "society": 0.6,
    "health": 0.6,
    "environmental issue": 0.5,
    "economy, business and finance": 0.5,
    "human interest": 0.4,
    "education": 0.3,
    "religion": 0.3,
    "science and technology": 0.3,
    "labour": 0.3,
    "arts, culture, entertainment and media": 0.2,
    "lifestyle and leisure": 0.2,
    "sport": 0.1,
    "weather": 0.3,
}


@dataclass
class HotspotScore:
    """Computed score for a cluster."""

    cluster_id: int
    score: float
    components: dict[str, float]  # Breakdown of score components
    is_top_hotspot: bool = False


def compute_hotspot_score(
    article_count: int,
    unique_domains: int,
    max_recency: datetime | None,
    primary_topic: str = "unclassified",
    max_article_count: int = 100,  # for normalization
    now: datetime | None = None,
) -> HotspotScore:
    """
    Compute a hotspot score in [0, 1].

    Components (weighted sum):
    - volume:    30% — log-scaled article count
    - recency:   25% — exponential decay based on age
    - diversity: 25% — unique source domains
    - topic:     20% — IPTC topic weight
    """
    if now is None:
        now = datetime.now(UTC)

    # 1. Volume score (log-scaled, capped)
    volume = min(math.log2(article_count + 1) / math.log2(max_article_count + 1), 1.0)

    # 2. Recency score (exponential decay, half-life = 12 hours)
    if max_recency:
        if max_recency.tzinfo is None:
            max_recency = max_recency.replace(tzinfo=UTC)
        age_hours = (now - max_recency).total_seconds() / 3600
        recency = math.exp(-0.693 * age_hours / 12)  # half-life = 12h
    else:
        recency = 0.0

    # 3. Source diversity (log-scaled)
    diversity = min(math.log2(unique_domains + 1) / math.log2(20), 1.0)

    # 4. Topic weight
    topic_weight = TOPIC_WEIGHTS.get(primary_topic.lower(), 0.3)

    # Weighted sum
    score = 0.30 * volume + 0.25 * recency + 0.25 * diversity + 0.20 * topic_weight

    return HotspotScore(
        cluster_id=0,  # Set by caller
        score=round(score, 4),
        components={
            "volume": round(volume, 4),
            "recency": round(recency, 4),
            "diversity": round(diversity, 4),
            "topic_weight": round(topic_weight, 4),
        },
    )
