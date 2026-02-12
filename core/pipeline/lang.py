"""
Language detection using fastText LID model.

Falls back to the existing `feed_entries.language` field if already set;
otherwise detects with fastText (176 languages, ISO 639-1 codes).
"""

from __future__ import annotations

import os
import re
from functools import lru_cache
from pathlib import Path

import structlog

logger = structlog.get_logger(__name__)

# Model path — downloaded once, cached locally
_MODEL_DIR = Path(os.getenv("FASTTEXT_MODEL_DIR", "models"))
_MODEL_PATH = _MODEL_DIR / "lid.176.ftz"
_DOWNLOAD_URL = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz"


@lru_cache(maxsize=1)
def _get_model():  # type: ignore[no-untyped-def]
    """Lazy-load the fastText LID model."""
    import fasttext

    if not _MODEL_PATH.exists():
        logger.info("downloading_fasttext_model", url=_DOWNLOAD_URL)
        _MODEL_DIR.mkdir(parents=True, exist_ok=True)
        import urllib.request

        urllib.request.urlretrieve(_DOWNLOAD_URL, str(_MODEL_PATH))
        logger.info("fasttext_model_downloaded", path=str(_MODEL_PATH))

    # Suppress fastText warnings about deprecated API
    model = fasttext.load_model(str(_MODEL_PATH))
    return model


def detect_language(text: str, existing_lang: str | None = None) -> str:
    """
    Detect language of text.

    Args:
        text: Article text (first ~500 chars used for speed).
        existing_lang: Language code already stored in DB.

    Returns:
        ISO 639-1 language code (e.g. "en", "fr", "ar").
    """
    # Trust existing if it looks valid
    if existing_lang and re.match(r"^[a-z]{2,3}$", existing_lang):
        return existing_lang.lower()

    if not text or len(text.strip()) < 20:
        return "und"  # undetermined

    try:
        model = _get_model()
        # Use first 500 chars, single line
        sample = text[:500].replace("\n", " ").strip()
        predictions = model.predict(sample, k=1)
        label = predictions[0][0]  # e.g. "__label__en"
        confidence = predictions[1][0]

        lang_code = label.replace("__label__", "")

        logger.debug(
            "language_detected",
            lang=lang_code,
            confidence=round(float(confidence), 4),
        )
        return lang_code

    except Exception as exc:
        logger.warning("language_detection_failed", error=str(exc))
        return existing_lang or "und"
