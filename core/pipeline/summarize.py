"""
Summarization module — OpenAI Batch API for cluster summaries.

Design:
- Summarize per cluster, NOT per article (cost-efficient)
- Only translate summaries when needed, not full content
- Use Batch API (50% cheaper, completes within 24h)
- Fallback: simple extractive summary for Tier A/B

LLM usage tiers:
- Tier A: extractive summary only (no LLM)
- Tier B: extractive + optional local summarizer
- Tier C: OpenAI Batch API + premium pass for top clusters
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class ClusterSummaryInput:
    """Input for summarizing a cluster of articles."""

    flashpoint_id: uuid.UUID
    cluster_id: int
    cluster_uuid: uuid.UUID
    articles: list[dict]  # [{title, content, language, url, domain}]


@dataclass
class ClusterSummaryResult:
    """Output of cluster summarization."""

    flashpoint_id: uuid.UUID
    cluster_id: int
    summary: str
    article_count: int
    top_domains: list[str]
    languages: list[str]
    urls: list[str]
    images: list[str]


# ── Extractive Summary (Tier A/B — no LLM) ───────────


def extractive_summary(articles: list[dict], max_sentences: int = 5) -> str:
    """
    Simple extractive summary: pick lead sentences from top articles.

    Articles should be sorted by relevance/recency.
    """
    sentences: list[str] = []

    for article in articles[:10]:  # Consider top 10 articles
        content = article.get("content", "") or article.get("description", "") or ""
        title = article.get("title_en") or article.get("title") or ""

        # Get first 2 sentences from each article
        article_sentences = re.split(r"(?<=[.!?])\s+", content.strip())
        for sent in article_sentences[:2]:
            sent = sent.strip()
            if len(sent) > 30 and sent not in sentences:
                sentences.append(sent)

        if len(sentences) >= max_sentences:
            break

    if not sentences and articles:
        # Fallback to titles
        for article in articles[:5]:
            title = article.get("title_en") or article.get("title") or ""
            if title:
                sentences.append(title)

    return " ".join(sentences[:max_sentences])


def aggregate_cluster_metadata(articles: list[dict]) -> dict:
    """Aggregate metadata across cluster articles."""
    domains: list[str] = []
    languages: list[str] = []
    urls: list[str] = []
    images: list[str] = []

    domain_counts: dict[str, int] = {}
    lang_set: set[str] = set()

    for article in articles:
        domain = article.get("domain") or article.get("hostname") or ""
        if domain:
            domain_counts[domain] = domain_counts.get(domain, 0) + 1

        lang = article.get("language", "")
        if lang:
            lang_set.add(lang)

        url = article.get("url", "")
        if url:
            urls.append(url)

        image = article.get("image", "")
        if image:
            images.append(image)

        # Also include images array
        for img in article.get("images", []) or []:
            if img and img not in images:
                images.append(img)

    # Top domains sorted by count
    top_domains = sorted(domain_counts.keys(), key=lambda d: domain_counts[d], reverse=True)

    return {
        "top_domains": top_domains[:10],
        "languages": sorted(lang_set),
        "urls": urls[:50],  # Cap at 50
        "images": images[:20],  # Cap at 20
    }


def summarize_cluster_local(cluster_input: ClusterSummaryInput) -> ClusterSummaryResult:
    """
    Summarize a cluster without LLM (Tier A/B).

    Uses extractive summary + metadata aggregation.
    """
    articles = cluster_input.articles
    summary = extractive_summary(articles)
    metadata = aggregate_cluster_metadata(articles)

    return ClusterSummaryResult(
        flashpoint_id=cluster_input.flashpoint_id,
        cluster_id=cluster_input.cluster_id,
        summary=summary,
        article_count=len(articles),
        top_domains=metadata["top_domains"],
        languages=metadata["languages"],
        urls=metadata["urls"],
        images=metadata["images"],
    )


# ── OpenAI Batch API (Tier C) ─────────────────────────


SUMMARIZE_SYSTEM_PROMPT = """You are a news intelligence analyst. Summarize the following cluster of news articles about the same event/topic into a single, concise, factual summary in English.

Requirements:
- Write 3-5 sentences maximum
- Include key facts: who, what, where, when
- If articles are in different languages, synthesize the information
- Be objective and factual
- Output ONLY the summary text, no headers or labels"""


def build_batch_request(
    cluster_input: ClusterSummaryInput,
) -> dict:
    """
    Build a single OpenAI Batch API request for a cluster.

    Returns a JSONL-compatible dict with custom_id.
    """
    # Combine article texts (truncated for token limits)
    combined_text_parts: list[str] = []
    for i, article in enumerate(cluster_input.articles[:15], 1):
        title = article.get("title_en") or article.get("title") or "Untitled"
        content = (article.get("content") or article.get("description") or "")[:1000]
        lang = article.get("language", "unknown")
        combined_text_parts.append(
            f"Article {i} [{lang}]: {title}\n{content}"
        )

    user_content = "\n\n---\n\n".join(combined_text_parts)

    custom_id = f"cluster_{cluster_input.flashpoint_id}_{cluster_input.cluster_id}"

    return {
        "custom_id": custom_id,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": SUMMARIZE_SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            "max_tokens": 500,
            "temperature": 0.3,
        },
    }


def write_batch_file(
    requests: list[dict],
    output_path: str,
) -> str:
    """Write batch requests to JSONL file for upload."""
    with open(output_path, "w", encoding="utf-8") as f:
        for req in requests:
            f.write(json.dumps(req) + "\n")

    logger.info("batch_file_written", path=output_path, count=len(requests))
    return output_path


def parse_batch_results(results_jsonl: str) -> dict[str, str]:
    """
    Parse OpenAI Batch API results JSONL into a mapping.

    Returns: {custom_id: summary_text}
    """
    summaries: dict[str, str] = {}

    for line in results_jsonl.strip().split("\n"):
        if not line.strip():
            continue
        result = json.loads(line)
        custom_id = result.get("custom_id", "")
        response = result.get("response", {})
        body = response.get("body", {})
        choices = body.get("choices", [])

        if choices:
            summary = choices[0].get("message", {}).get("content", "")
            summaries[custom_id] = summary.strip()
        else:
            error = result.get("error", {})
            logger.warning(
                "batch_result_no_choices",
                custom_id=custom_id,
                error=error,
            )

    logger.info("batch_results_parsed", count=len(summaries))
    return summaries
