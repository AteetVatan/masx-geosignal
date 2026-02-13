"""Local extractive/abstractive pre-summarisation using DistilBART.

Stage 1 of two-stage summarisation: each article is compressed locally
before being sent to the LLM cluster summariser in Stage 2.

Model: sshleifer/distilbart-cnn-12-6  (CPU, lazy-loaded, cached)
  - ONNX Runtime if exported (2× faster), falls back to vanilla PyTorch
Config: num_beams=1 (fast), do_sample=False, max_length=150

Chunking: BlingFire sentence segmentation + tokenizer-based packing
with 80-token overlap between chunks for context preservation.

Parallelism: ProcessPoolExecutor (default 8 workers) for CPU-bound
inference across multiple cores.
"""

from __future__ import annotations

import os
import time
from concurrent.futures import ProcessPoolExecutor
from functools import lru_cache
from pathlib import Path
from typing import Any

import structlog

from core.config import get_settings

logger = structlog.get_logger(__name__)

_MIN_CONTENT_CHARS = 1000  # skip articles already concise enough (~250 words)
_CHUNK_TOKENS = 900        # leave headroom for special tokens (model limit 1024)
_OVERLAP_TOKENS = 80       # overlap between successive chunks

# Module-level pool (created lazily, shared across calls)
_pool: ProcessPoolExecutor | None = None


def _get_pool() -> ProcessPoolExecutor:
    """Lazy-init the worker pool (one per process lifetime)."""
    global _pool
    if _pool is None:
        workers = get_settings().local_summarizer_workers
        _pool = ProcessPoolExecutor(
            max_workers=workers,
            initializer=_worker_init,
        )
        logger.info("summarizer_pool_created", workers=workers)
    return _pool


def shutdown_pool() -> None:
    """Shut down the worker pool and free worker-process memory.

    Each worker holds a full DistilBART model (~2.4 GB).
    Call this after local pre-summarisation is complete so the
    memory is released while clustering/LLM summarisation runs.
    """
    global _pool
    if _pool is not None:
        _pool.shutdown(wait=True)
        _pool = None
        logger.info("summarizer_pool_shut_down")


# ── Per-worker state (loaded once per child process) ─────────

_worker_pipeline: Any = None
_worker_tokenizer: Any = None


def _worker_init() -> None:
    """Initialise model + tokenizer inside each worker process."""
    global _worker_pipeline, _worker_tokenizer
    settings = get_settings()
    model_name = settings.local_summarizer_model
    onnx_dir = Path(settings.local_summarizer_onnx_dir)

    # Try ONNX first (2× faster on CPU)
    if onnx_dir.exists() and (onnx_dir / "config.json").exists():
        try:
            from optimum.onnxruntime import ORTModelForSeq2SeqLM
            from transformers import AutoTokenizer, pipeline

            # The export creates separate files; tell optimum the exact names
            load_kwargs: dict[str, str] = {}
            if (onnx_dir / "decoder_model.onnx").exists():
                load_kwargs["decoder_file_name"] = "decoder_model.onnx"
            if (onnx_dir / "decoder_with_past_model.onnx").exists():
                load_kwargs["decoder_with_past_file_name"] = (
                    "decoder_with_past_model.onnx"
                )

            ort_model = ORTModelForSeq2SeqLM.from_pretrained(
                str(onnx_dir), **load_kwargs,
            )
            _worker_tokenizer = AutoTokenizer.from_pretrained(str(onnx_dir))
            _worker_pipeline = pipeline(
                "summarization",
                model=ort_model,
                tokenizer=_worker_tokenizer,
                device=-1,
            )
            logger.info("worker_loaded_onnx", pid=os.getpid())
            return
        except Exception as exc:
            logger.warning("onnx_load_failed_falling_back", error=str(exc))

    # Vanilla PyTorch fallback
    try:
        from transformers import AutoTokenizer, pipeline

        _worker_pipeline = pipeline(
            "summarization",
            model=model_name,
            device=-1,
        )
        _worker_tokenizer = AutoTokenizer.from_pretrained(model_name, use_fast=True)
        logger.info("worker_loaded_pytorch", pid=os.getpid(), model=model_name)
    except Exception as exc:
        logger.error("worker_model_load_failed", error=str(exc), pid=os.getpid())


# ── Tokenizer (main process, for chunking before dispatch) ───


@lru_cache(maxsize=1)
def _get_tokenizer() -> Any:
    """Lazy-load the fast tokenizer (main process, for chunking)."""
    from transformers import AutoTokenizer

    settings = get_settings()
    onnx_dir = Path(settings.local_summarizer_onnx_dir)

    # Use ONNX tokenizer if available
    if onnx_dir.exists() and (onnx_dir / "tokenizer.json").exists():
        return AutoTokenizer.from_pretrained(str(onnx_dir), use_fast=True)

    return AutoTokenizer.from_pretrained(settings.local_summarizer_model, use_fast=True)


def _tlen(text: str) -> int:
    """Count tokens in text (no special tokens — for packing only)."""
    tokenizer = _get_tokenizer()
    return len(tokenizer.encode(text, add_special_tokens=False))


# ── Chunking (BlingFire + tokenizer) ─────────────────────────


def chunk_text(
    text: str,
    chunk_tokens: int = _CHUNK_TOKENS,
    overlap_tokens: int = _OVERLAP_TOKENS,
) -> list[str]:
    """Split text into token-counted chunks with overlap.

    Uses BlingFire for fast C++ sentence segmentation, then packs
    sentences greedily into chunks of ≤ ``chunk_tokens`` with
    ``overlap_tokens`` of trailing context carried to the next chunk.
    """
    from blingfire import text_to_sentences

    tokenizer = _get_tokenizer()

    # 1) Fast sentence split
    sents = [s.strip() for s in text_to_sentences(text).split("\n") if s.strip()]
    if not sents:
        return [text] if text.strip() else []

    chunks: list[str] = []
    cur_sents: list[str] = []
    cur_len = 0

    i = 0
    while i < len(sents):
        sent = sents[i]
        sl = _tlen(sent)

        # If a single sentence exceeds chunk_tokens, hard-split by tokens
        if sl > chunk_tokens:
            ids = tokenizer.encode(sent, add_special_tokens=False)
            for j in range(0, len(ids), chunk_tokens - overlap_tokens):
                piece = tokenizer.decode(
                    ids[j : j + chunk_tokens], skip_special_tokens=True
                )
                if piece.strip():
                    chunks.append(piece.strip())
            i += 1
            continue

        # Normal packing: try to fit this sentence
        if cur_len + sl <= chunk_tokens:
            cur_sents.append(sent)
            cur_len += sl
            i += 1
        else:
            # Flush current chunk
            chunk = " ".join(cur_sents).strip()
            if chunk:
                chunks.append(chunk)

            # Build overlap by walking backward over sentences
            overlap: list[str] = []
            olen = 0
            for s in reversed(cur_sents):
                olen += _tlen(s)
                overlap.append(s)
                if olen >= overlap_tokens:
                    break
            cur_sents = list(reversed(overlap))
            cur_len = sum(_tlen(s) for s in cur_sents)

    # Flush last chunk
    last = " ".join(cur_sents).strip()
    if last:
        chunks.append(last)

    return chunks or [text.strip()]


# ── Single-article summarisation (runs in worker process) ────


def _summarize_text(text: str) -> str:
    """Summarise one article's text — called inside worker process."""
    global _worker_pipeline

    if _worker_pipeline is None:
        # Worker model failed to load — simple truncation fallback
        return text[:2000]

    chunks = chunk_text(text)
    summaries: list[str] = []

    for chunk in chunks:
        try:
            result = _worker_pipeline(
                chunk,
                #max_length=150,
                #min_length=30,
                num_beams=1,
                do_sample=False,
            )
            summaries.append(result[0]["summary_text"])
        except Exception:
            logger.debug("chunk_summarize_failed_truncating", chunk_len=len(chunk))
            summaries.append(chunk[:500])

    return " ".join(summaries).strip()


def summarize_article(text: str) -> str:
    """Summarise a single article (main-process entry point).

    For short texts (< 1000 chars) returns as-is.
    For longer texts, delegates to worker process.
    """
    if len(text) < _MIN_CONTENT_CHARS:
        return text
    return _summarize_text(text)


# ── Batch pre-summarisation with ProcessPoolExecutor ─────────


def presummarize_articles(
    articles: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Pre-summarise article content using local DistilBART.

    Uses ProcessPoolExecutor for CPU-parallel inference.
    Returns a new list with ``content`` replaced by its local summary.
    Short articles (< 1000 chars) are kept as-is.
    """
    # Separate articles that need summarisation vs pass-through
    _t0 = time.perf_counter()
    to_summarize: list[tuple[int, str]] = []
    results: dict[int, str] = {}

    for idx, article in enumerate(articles):
        raw = article.get("content") or article.get("description") or ""
        if len(raw) < _MIN_CONTENT_CHARS:
            results[idx] = raw
        else:
            to_summarize.append((idx, raw))

    # Dispatch to worker pool
    if to_summarize:
        pool = _get_pool()
        texts = [text for _, text in to_summarize]
        indices = [idx for idx, _ in to_summarize]

        summaries = list(pool.map(_summarize_text, texts))
        for idx, summary in zip(indices, summaries):
            results[idx] = summary

    # Rebuild article list with summaries
    out: list[dict[str, Any]] = []
    for idx, article in enumerate(articles):
        out.append({**article, "content": results[idx]})
    logger.info(
        "presummarize_done",
        total=len(articles),
        summarized=len(to_summarize),
        elapsed_s=round(time.perf_counter() - _t0, 2),
    )
    return out
