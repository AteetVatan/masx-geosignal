"""
IPTC Media Topic classification using ONNX multilingual classifier.

Uses the 17 top-level IPTC Media Topics as canonical domain taxonomy.
Runs on CPU via ONNX Runtime for cost efficiency.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import structlog

logger = structlog.get_logger(__name__)

# ── IPTC 17 Top-Level Categories ──────────────────────

IPTC_TOP_LEVEL = {
    "01000000": "arts, culture, entertainment and media",
    "02000000": "crime, law and justice",
    "03000000": "disaster, accident and emergency incident",
    "04000000": "economy, business and finance",
    "05000000": "education",
    "06000000": "environmental issue",
    "07000000": "health",
    "08000000": "human interest",
    "09000000": "labour",
    "10000000": "lifestyle and leisure",
    "11000000": "politics",
    "12000000": "religion",
    "13000000": "science and technology",
    "14000000": "society",
    "15000000": "sport",
    "16000000": "conflict, war and peace",
    "17000000": "weather",
}

# Reverse mapping for label lookups
IPTC_LABEL_TO_CODE = {v: k for k, v in IPTC_TOP_LEVEL.items()}


@dataclass
class TopicClassification:
    """Result of IPTC topic classification."""

    iptc_top_level: str  # Friendly name
    iptc_code: str  # IPTC code (e.g. "16000000")
    iptc_path: str  # Full path (may include subtopic)
    confidence: float


@lru_cache(maxsize=1)
def _get_classifier():  # type: ignore[no-untyped-def]
    """
    Load the ONNX multilingual IPTC classifier.

    Downloads from HuggingFace if not cached locally.
    """
    try:
        import onnxruntime as ort
        from tokenizers import Tokenizer

        model_dir = Path(os.getenv("IPTC_MODEL_DIR", "models/iptc-classifier"))

        if not model_dir.exists():
            logger.info("downloading_iptc_model")
            # Auto-download from HuggingFace hub
            from huggingface_hub import snapshot_download

            snapshot_download(
                repo_id="onnx-community/multilingual-IPTC-news-topic-classifier-ONNX",
                local_dir=str(model_dir),
            )
            logger.info("iptc_model_downloaded", path=str(model_dir))

        # Load ONNX session
        model_path = model_dir / "model.onnx"
        if not model_path.exists():
            # Try quantized
            model_path = model_dir / "model_quantized.onnx"

        session = ort.InferenceSession(
            str(model_path),
            providers=["CPUExecutionProvider"],
        )

        # Load tokenizer
        tokenizer_path = model_dir / "tokenizer.json"
        tokenizer = Tokenizer.from_file(str(tokenizer_path))

        # Load label mapping
        import json

        config_path = model_dir / "config.json"
        if config_path.exists():
            with open(config_path) as f:
                config = json.load(f)
            id2label = config.get("id2label", {})
        else:
            id2label = {}

        return session, tokenizer, id2label

    except Exception as exc:
        logger.error("iptc_classifier_load_failed", error=str(exc))
        return None


def classify_topic(
    text: str,
    top_k: int = 3,
) -> list[TopicClassification]:
    """
    Classify text into IPTC Media Topics.

    Args:
        text: Article text or title (first ~512 tokens used).
        top_k: Number of top predictions to return.

    Returns:
        List of TopicClassification sorted by confidence DESC.
    """
    classifier = _get_classifier()
    if classifier is None:
        # Fallback: return "unclassified"
        return [
            TopicClassification(
                iptc_top_level="unclassified",
                iptc_code="00000000",
                iptc_path="unclassified",
                confidence=0.0,
            )
        ]

    session, tokenizer, id2label = classifier

    try:
        import numpy as np

        # Tokenize (truncate to 512 tokens)
        encoded = tokenizer.encode(text[:2000])
        input_ids = np.array([encoded.ids[:512]], dtype=np.int64)
        attention_mask = np.array([encoded.attention_mask[:512]], dtype=np.int64)

        # Run inference
        outputs = session.run(
            None,
            {"input_ids": input_ids, "attention_mask": attention_mask},
        )

        logits = outputs[0][0]

        # Softmax
        exp_logits = np.exp(logits - np.max(logits))
        probs = exp_logits / exp_logits.sum()

        # Get top-k
        top_indices = np.argsort(probs)[-top_k:][::-1]

        results: list[TopicClassification] = []
        for idx in top_indices:
            label = id2label.get(str(idx), f"unknown_{idx}")
            # Map to IPTC top-level
            iptc_code = IPTC_LABEL_TO_CODE.get(label.lower(), "00000000")
            iptc_name = IPTC_TOP_LEVEL.get(iptc_code, label)

            results.append(
                TopicClassification(
                    iptc_top_level=iptc_name,
                    iptc_code=iptc_code,
                    iptc_path=iptc_name,
                    confidence=float(probs[idx]),
                )
            )

        return results

    except Exception as exc:
        logger.warning("topic_classification_failed", error=str(exc))
        return [
            TopicClassification(
                iptc_top_level="unclassified",
                iptc_code="00000000",
                iptc_path="unclassified",
                confidence=0.0,
            )
        ]
