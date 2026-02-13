"""
Summarization module — LLM-powered cluster summaries.

Design:
- Summarize per cluster, NOT per article (cost-efficient)
- Only translate summaries when needed, not full content
- Provider-agnostic: works with Together AI, OpenAI, Mistral, Groq, etc.
- Fallback: simple extractive summary for Tier A/B

LLM usage tiers:
- Tier A: extractive summary only (no LLM)
- Tier B: extractive + optional local summarizer
- Tier C: LLM API summaries (Together AI default) + premium pass
"""

from __future__ import annotations

import asyncio
import json
import re
import threading
import time
from collections import deque
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, Any

import structlog
from tenacity import RetryError, retry, stop_after_attempt, wait_fixed

from core.config.settings import get_settings

if TYPE_CHECKING:
    import uuid

logger = structlog.get_logger(__name__)


@dataclass
class ClusterSummaryInput:
    """Input for summarizing a cluster of articles."""

    flashpoint_id: uuid.UUID
    cluster_id: int
    cluster_uuid: uuid.UUID
    articles: list[dict[str, Any]]  # [{title, content, language, url, domain}]


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


def extractive_summary(articles: list[dict[str, Any]], max_sentences: int = 5) -> str:
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


def aggregate_cluster_metadata(articles: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate metadata across cluster articles."""
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


# ── LLM Rate Limiter ──────────────────────────────────


class _LLMRateLimiter:
    """Thread-safe sliding-window rate limiter (RPM)."""

    def __init__(self, rpm: int = 600) -> None:
        self._rpm = rpm
        self._window: deque[float] = deque()
        self._lock = threading.Lock()

    def wait(self) -> None:
        """Block until a request slot is available within the RPM window."""
        while True:
            with self._lock:
                now = time.monotonic()
                # Evict timestamps older than 60 s
                while self._window and self._window[0] <= now - 60:
                    self._window.popleft()
                if len(self._window) < self._rpm:
                    self._window.append(now)
                    return
                # Calculate how long to sleep until the oldest entry expires
                sleep_for = self._window[0] - (now - 60)
            time.sleep(max(sleep_for, 0.01))


_rate_limiter: _LLMRateLimiter | None = None
_rate_limiter_lock = threading.Lock()


def _get_rate_limiter() -> _LLMRateLimiter:
    """Return (or create) the singleton rate limiter from settings."""
    global _rate_limiter  # noqa: PLW0603
    if _rate_limiter is None:
        with _rate_limiter_lock:
            if _rate_limiter is None:
                settings = get_settings()
                _rate_limiter = _LLMRateLimiter(rpm=settings.llm_rpm_limit)
                logger.info("llm_rate_limiter_created", rpm=settings.llm_rpm_limit)
    return _rate_limiter


# ── LLM Summarization (Tier C) ────────────────────────


SUMMARIZE_SYSTEM_PROMPT = """You are a news intelligence analyst. Summarize the following cluster of news articles about the same event/topic into a single, comprehensive, factual summary in English.

Input format: TOML (articles table array).

Requirements:
- Include all key facts: who, what, where, when, why
- If articles are in different languages, synthesize the information
- Be objective and factual
- Cover the full scope of the event — do not omit important details
- Capture the essential information

Output (STRICT) - Return JSON ONLY:
{"summary": "<your summary here>"}
- Never output articles
Return JSON only, no extra text."""


def _estimate_max_tokens(messages: list[dict[str, str]]) -> int:
    """Estimate a dynamic max_tokens budget based on input size.

    Heuristic: ~1 token ≈ 4 characters.  We allow the output to be
    roughly 30 % of the input size, clamped to [150, 4096].
    """
    total_chars = sum(len(m.get("content", "")) for m in messages)
    estimated_input_tokens = total_chars // 4
    budget = max(150, min(estimated_input_tokens * 30 // 100, 4096))
    return budget


@lru_cache(maxsize=1)
def get_llm_client() -> Any:
    """
    Create and cache an OpenAI-compatible client for the configured LLM provider.

    Works with Together AI, OpenAI, Mistral, Groq, DeepSeek, etc.
    All use the OpenAI Python SDK with a custom base_url.

    Cached to reuse the underlying httpx connection pool across calls.
    """
    import openai

    settings = get_settings()
    api_key = settings.resolved_llm_api_key

    if not api_key:
        raise ValueError("No LLM API key configured. Set LLM_API_KEY or OPENAI_API_KEY in .env")

    return openai.OpenAI(
        api_key=api_key,
        base_url=settings.llm_base_url,
    )


@lru_cache(maxsize=1)
def get_fallback_llm_client() -> Any:
    """
    Create and cache an OpenAI-compatible client for the *fallback* LLM provider.

    Used when the primary provider fails after retries.
    Defaults to Mistral (cheapest: $0.10 / $0.30 per 1M tokens).

    Cached to reuse the underlying httpx connection pool across calls.
    """
    import openai

    settings = get_settings()
    api_key = settings.resolved_fallback_api_key

    if not api_key:
        raise ValueError(
            "No fallback LLM API key configured. "
            "Set LLM_FALLBACK_API_KEY in .env to enable automatic failover."
        )

    return openai.OpenAI(
        api_key=api_key,
        base_url=settings.llm_fallback_base_url,
    )


def _build_messages(cluster_input: ClusterSummaryInput) -> list[dict[str, str]]:
    """Build chat messages with pre-summarised article data in TOML.

    Two-stage approach:
      1. Each article is locally summarised by DistilBART (no data loss).
      2. Summaries are serialized as TOML and sent to the LLM.
    """
    from core.pipeline.local_summarizer import presummarize_articles
    from core.pipeline.toml_serde import dict_to_toml

    raw_articles = list(cluster_input.articles[:15])
    presummaries = presummarize_articles(raw_articles)

    articles_for_toml: list[dict[str, Any]] = []
    for i, article in enumerate(presummaries, 1):
        articles_for_toml.append({
            "id": i,
            "lang": article.get("language", "unknown"),
            "title": article.get("title_en") or article.get("title") or "Untitled",
            "content": article.get("content", ""),
        })

    user_content = dict_to_toml({"articles": articles_for_toml})

    return [
        {"role": "system", "content": SUMMARIZE_SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]


def _collect_summaries(obj: Any) -> list[str]:
    """Recursively collect all 'summary' string values from nested structure."""
    out: list[str] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and k.lower() == "summary" and isinstance(v, str):
                out.append(v)
            else:
                out.extend(_collect_summaries(v))
    elif isinstance(obj, list):
        for x in obj:
            out.extend(_collect_summaries(x))
    return out


def _clean_noise(s: str) -> str:
    """Strip markdown fences, structural chars, and other LLM noise."""
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = s.replace("\\'", "'")
    s = re.sub(r"```+", " ", s)
    s = re.sub(r"(?mi)^\s*(\[\[.*?\]\]|\[.*?\])\s*$", " ", s)  # TOML/YAML headers
    s = re.sub(r"[{}\[\]<>`=\\/]", " ", s)
    s = s.replace('"""', " ").replace("'''", " ")
    s = " ".join(s.split())
    return s.strip()


@lru_cache(maxsize=1)
def _get_sentence_segmenter():
    """Cached pysbd Segmenter — avoids re-loading language rules per call."""
    import pysbd
    return pysbd.Segmenter(language="en", clean=True)


def _extract_sentences_best_effort(
    raw: str, *, min_words: int = 3,
) -> str | None:
    """Extract summary sentences via best-effort JSON, then sentence segmentation.

    Pipeline:
      1) Try loads_best_effort to extract summary fields (or list items).
      2) If that fails, use the raw text.
      3) Sentence split: blingfire → pysbd.

    Returns joined sentences or ``None``.
    """
    from blingfire import text_to_sentences

    from core.pipeline.json_parse import loads_best_effort

    if not raw or not raw.strip():
        return None

    candidates: list[str] = []

    # 1) best-effort JSON parse (orjson → rapidjson → pyjson5 → json_repair)
    try:
        obj = loads_best_effort(raw)
        # If it's a list of strings, treat each item as candidate text
        if isinstance(obj, list) and all(isinstance(x, str) for x in obj):
            candidates.extend(obj)
        else:
            candidates.extend(_collect_summaries(obj))
    except Exception:
        pass

    # 2) fallback: if no candidates, use the raw text
    if not candidates:
        candidates = [raw]

    # Clean + segment
    seg = _get_sentence_segmenter()
    out: list[str] = []
    seen: set[str] = set()

    for c in candidates:
        c = _clean_noise(c)
        if not c:
            continue

        coarse = [x.strip() for x in text_to_sentences(c).splitlines() if x.strip()] or [c]
        for chunk in coarse:
            for sent in seg.segment(chunk):
                sent = sent.strip()
                if len(sent.split()) < min_words:
                    continue
                if sent not in seen:
                    seen.add(sent)
                    out.append(sent)

    return " ".join(out) if out else None


def _parse_llm_response(raw: str) -> str:
    """Parse LLM response — best-effort JSON first, sentence extraction fallback.

    The LLM is instructed to return JSON ``{"summary": "..."}``, but its
    output can be malformed.  We try ``loads_best_effort`` (orjson → rapidjson
    → pyjson5 → json_repair) first, then fall back to sentence segmentation.
    """
    from core.pipeline.json_parse import loads_best_effort

    # 1. Try best-effort JSON (expected output format)
    try:
        parsed = loads_best_effort(raw)
        if isinstance(parsed, list) and parsed:
            parsed = parsed[0]
        if isinstance(parsed, dict):
            summary = parsed.get("summary", "")
            if summary:
                return str(summary).strip()
    except Exception:
        pass

    # 2. Fallback: best-effort JSON + sentence extraction
    extracted = _extract_sentences_best_effort(raw)
    if extracted:
        return extracted.strip()

    # 3. Graceful fallback: treat the raw text as the summary
    logger.debug("llm_response_unparseable_using_raw", raw_preview=raw[:100])
    return raw.strip()


def _call_llm(client: Any, model: str, messages: list[dict[str, str]]) -> str:
    """Make a single LLM chat completion call, parsing JSON response."""
    _get_rate_limiter().wait()
    max_tokens = _estimate_max_tokens(messages)

    response = client.chat.completions.create(
        model=model,
        messages=messages,
        max_tokens=max_tokens,
        temperature=0,
    )

    content = response.choices[0].message.content or ""
    return _parse_llm_response(content)


def summarize_cluster_llm(cluster_input: ClusterSummaryInput) -> str:
    """
    Summarize a cluster using the configured LLM provider (real-time).

    Strategy:
      1. Try the primary provider up to 2 times (1 initial + 1 retry)
         with a 2-second wait between attempts.
      2. If the primary is exhausted, try the fallback provider once
         (defaults to Mistral — cheapest available).
      3. If the fallback also fails, raise so the caller can handle it
         (e.g. fall back to local extractive summary).
    """
    settings = get_settings()
    messages = _build_messages(cluster_input)

    # ── Primary: 2 retries with 1 s delay ─────────────────────
    @retry(
        stop=stop_after_attempt(2),
        wait=wait_fixed(2),
        reraise=True,
    )
    def _call_primary() -> str:
        client = get_llm_client()
        return _call_llm(client, settings.llm_model, messages)

    try:
        return _call_primary()
    except RetryError:
        logger.warning(
            "primary_llm_exhausted_trying_fallback",
            provider=settings.llm_provider,
            model=settings.llm_model,
            cluster_id=cluster_input.cluster_id,
        )
    except Exception:
        logger.warning(
            "primary_llm_exhausted_trying_fallback",
            provider=settings.llm_provider,
            model=settings.llm_model,
            cluster_id=cluster_input.cluster_id,
            exc_info=True,
        )

    # ── Fallback: single attempt ──────────────────────────────
    try:
        fallback_client = get_fallback_llm_client()
        result = _call_llm(fallback_client, settings.llm_fallback_model, messages)
        logger.info(
            "fallback_llm_succeeded",
            provider=settings.llm_provider_fallback,
            model=settings.llm_fallback_model,
            cluster_id=cluster_input.cluster_id,
        )
        return result
    except Exception:
        logger.error(
            "fallback_llm_also_failed",
            provider=settings.llm_provider_fallback,
            model=settings.llm_fallback_model,
            cluster_id=cluster_input.cluster_id,
            exc_info=True,
        )
        raise


async def summarize_cluster_llm_async(cluster_input: ClusterSummaryInput) -> str:
    """
    Async wrapper around summarize_cluster_llm.

    Runs the sync LLM call in a thread so the rate limiter blocks
    the thread (not the event loop) when the RPM cap is hit.
    """
    return await asyncio.to_thread(summarize_cluster_llm, cluster_input)


async def summarize_batch_llm(
    inputs: list[ClusterSummaryInput],
    batch_size: int = 20,
) -> list[str]:
    """
    Summarize multiple clusters concurrently in batches.

    Processes *batch_size* clusters at a time via asyncio.gather.
    The per-call rate limiter automatically sleeps in each thread
    when the RPM window is full.

    Returns one summary string per input (falls back to local
    extractive summary on per-cluster LLM failure).
    """
    results: list[str] = []

    for start in range(0, len(inputs), batch_size):
        batch = inputs[start : start + batch_size]
        logger.info(
            "llm_batch_summarizing",
            batch_start=start,
            batch_size=len(batch),
            total=len(inputs),
        )

        async def _safe_call(ci: ClusterSummaryInput) -> str:
            try:
                return await summarize_cluster_llm_async(ci)
            except Exception:
                logger.warning(
                    "llm_call_failed_fallback_local",
                    cluster_id=ci.cluster_id,
                    flashpoint_id=str(ci.flashpoint_id),
                    exc_info=True,
                )
                return summarize_cluster_local(ci).summary

        batch_results = await asyncio.gather(*[_safe_call(ci) for ci in batch])
        results.extend(batch_results)

    return results


def build_batch_request(
    cluster_input: ClusterSummaryInput,
) -> dict[str, Any]:
    """
    Build a single Batch API request for a cluster (OpenAI only).

    Returns a JSONL-compatible dict with custom_id.
    """
    settings = get_settings()
    messages = _build_messages(cluster_input)
    custom_id = f"cluster_{cluster_input.flashpoint_id}_{cluster_input.cluster_id}"

    max_tokens = _estimate_max_tokens(messages)

    return {
        "custom_id": custom_id,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": settings.llm_model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": 0,
        },
    }


def write_batch_file(
    requests: list[dict[str, Any]],
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
