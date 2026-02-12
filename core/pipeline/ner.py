"""
Named Entity Recognition (NER) using multilingual transformer model.

Uses Davlan/distilbert-base-multilingual-cased-ner-hrl for:
- PERSON, ORG, LOC, GPE, EVENT, DATE, NORP, LAW, MONEY, QUANTITY

Output format matches the upstream schema:
{
  "LOC": [{"text": "Brazil", "score": 0.9999}, ...],
  "ORG": [...],
  "PERSON": [...],
  ...
  "meta": {
    "chars": 3539,
    "model": "Davlan/distilbert-base-multilingual-cased-ner-hrl",
    "score": 0.9642,
    "chunks": 1
  }
}
"""

from __future__ import annotations

import os
from collections import defaultdict
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

# Model to use
NER_MODEL = os.getenv(
    "NER_MODEL",
    "Davlan/distilbert-base-multilingual-cased-ner-hrl",
)

# Maximum text length per chunk (in chars)
MAX_CHUNK_CHARS = 4000

# Entity label mapping from HuggingFace NER tags → our schema
# The HRL model uses BIO tags: B-PER, I-PER, B-ORG, I-ORG, B-LOC, I-LOC, B-DATE, I-DATE
HF_LABEL_MAP = {
    "PER": "PERSON",
    "ORG": "ORG",
    "LOC": "LOC",
    "DATE": "DATE",
    "MISC": "EVENT",  # Map MISC to EVENT as best-effort
}

# All entity categories in our schema
ALL_CATEGORIES = [
    "GPE", "LAW", "LOC", "ORG", "DATE", "NORP",
    "EVENT", "MONEY", "PERSON", "QUANTITY",
]


@dataclass
class NERResult:
    """NER extraction result matching the upstream entities schema."""
    entities: dict[str, list[dict[str, Any]]]
    meta: dict[str, Any]


@lru_cache(maxsize=1)
def _get_ner_pipeline():
    """Load the NER pipeline (lazy, cached)."""
    try:
        from transformers import pipeline

        ner = pipeline(
            "ner",
            model=NER_MODEL,
            aggregation_strategy="simple",
            device=-1,  # CPU
        )
        logger.info("ner_model_loaded", model=NER_MODEL)
        return ner
    except Exception as exc:
        logger.error("ner_model_load_failed", error=str(exc))
        return None


def _chunk_text(text: str, max_chars: int = MAX_CHUNK_CHARS) -> list[str]:
    """Split text into chunks that fit within model limits."""
    if len(text) <= max_chars:
        return [text]

    chunks = []
    # Split on paragraph boundaries first, then sentence boundaries
    paragraphs = text.split("\n\n")
    current_chunk = ""

    for para in paragraphs:
        if len(current_chunk) + len(para) + 2 <= max_chars:
            current_chunk += ("\n\n" if current_chunk else "") + para
        else:
            if current_chunk:
                chunks.append(current_chunk)
            # If single paragraph exceeds max, split on sentences
            if len(para) > max_chars:
                sentences = para.replace(". ", ".\n").split("\n")
                current_chunk = ""
                for sent in sentences:
                    if len(current_chunk) + len(sent) + 1 <= max_chars:
                        current_chunk += (" " if current_chunk else "") + sent
                    else:
                        if current_chunk:
                            chunks.append(current_chunk)
                        current_chunk = sent[:max_chars]
            else:
                current_chunk = para

    if current_chunk:
        chunks.append(current_chunk)

    return chunks or [text[:max_chars]]


def _merge_entities(
    raw_entities: list[dict],
) -> dict[str, list[dict[str, Any]]]:
    """Merge raw HuggingFace NER output into our schema format.

    Deduplicates entities by text (case-insensitive) and keeps
    the highest score for each unique entity.
    """
    by_category: dict[str, dict[str, float]] = defaultdict(dict)

    for ent in raw_entities:
        label = ent.get("entity_group", "")
        mapped_label = HF_LABEL_MAP.get(label, label)
        if mapped_label not in ALL_CATEGORIES:
            continue

        text = ent.get("word", "").strip()
        if not text or len(text) < 2:
            continue

        # Clean up subword artifacts
        text = text.replace("##", "").strip()
        if not text:
            continue

        score = float(ent.get("score", 0.0))

        # Keep highest score for each entity text
        key = text.lower()
        existing = by_category[mapped_label]
        if key not in existing or existing[key] < score:
            existing[key] = score

    # Build output: sort by score descending, use original casing
    result: dict[str, list[dict[str, Any]]] = {}
    for cat in ALL_CATEGORIES:
        entries = by_category.get(cat, {})
        sorted_entries = sorted(entries.items(), key=lambda x: x[1], reverse=True)
        result[cat] = [
            {"text": text.title() if cat in ("PERSON", "GPE", "LOC") else text,
             "score": round(score, 4)}
            for text, score in sorted_entries[:20]  # Cap at 20 per category
        ]

    return result


def extract_entities(text: str) -> NERResult:
    """
    Extract named entities from text.

    Args:
        text: Article content (can be multilingual).

    Returns:
        NERResult with entities dict and meta info.
    """
    ner = _get_ner_pipeline()

    # Empty result template
    empty = {cat: [] for cat in ALL_CATEGORIES}

    if ner is None:
        return NERResult(
            entities=empty,
            meta={"chars": len(text), "model": NER_MODEL, "score": 0.0, "chunks": 0},
        )

    try:
        chunks = _chunk_text(text)
        all_raw_entities: list[dict] = []

        for chunk in chunks:
            raw = ner(chunk)
            all_raw_entities.extend(raw)

        merged = _merge_entities(all_raw_entities)

        # Calculate average confidence
        all_scores = []
        for entities in merged.values():
            for ent in entities:
                all_scores.append(ent["score"])
        avg_score = sum(all_scores) / len(all_scores) if all_scores else 0.0

        return NERResult(
            entities=merged,
            meta={
                "chars": len(text),
                "model": NER_MODEL,
                "score": round(avg_score, 4),
                "chunks": len(chunks),
            },
        )

    except Exception as exc:
        logger.exception("ner_extraction_failed", error=str(exc))
        return NERResult(
            entities=empty,
            meta={"chars": len(text), "model": NER_MODEL, "score": 0.0, "chunks": 0},
        )
