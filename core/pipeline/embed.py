"""
Embedding module — sentence-transformers for local CPU embeddings.

Default model: all-MiniLM-L6-v2 (384 dimensions, fast, good quality).
Batched encoding for throughput.
"""

from __future__ import annotations

import time
from functools import lru_cache

import structlog

logger = structlog.get_logger(__name__)


@lru_cache(maxsize=1)
def _get_model(model_name: str = "all-MiniLM-L6-v2"):  # type: ignore[no-untyped-def]
    """Lazy-load the sentence-transformer model."""
    from sentence_transformers import SentenceTransformer

    logger.info("loading_embedding_model", model=model_name)
    model = SentenceTransformer(model_name)
    logger.info("embedding_model_loaded", model=model_name)
    return model


def embed_texts(
    texts: list[str],
    model_name: str = "all-MiniLM-L6-v2",
    batch_size: int = 64,
    show_progress: bool = False,
) -> list[list[float]]:
    """
    Encode texts into embeddings using sentence-transformers.

    Args:
        texts: List of text strings to encode.
        model_name: HuggingFace model identifier.
        batch_size: Encoding batch size (tune for memory).
        show_progress: Whether to show a progress bar.

    Returns:
        List of embedding vectors (list[float]).
    """
    if not texts:
        return []

    model = _get_model(model_name)
    start = time.monotonic()

    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=show_progress,
        normalize_embeddings=True,  # L2-normalize for cosine similarity
        convert_to_numpy=True,
    )

    duration_ms = int((time.monotonic() - start) * 1000)
    logger.info(
        "embeddings_computed",
        count=len(texts),
        model=model_name,
        duration_ms=duration_ms,
        avg_ms=round(duration_ms / len(texts), 1) if texts else 0,
    )

    return [emb.tolist() for emb in embeddings]


def embed_single(
    text: str,
    model_name: str = "all-MiniLM-L6-v2",
) -> list[float]:
    """Embed a single text string."""
    results = embed_texts([text], model_name=model_name)
    return results[0]
