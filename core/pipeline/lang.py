"""
Language detection using fastText LID model.

Falls back to the existing `feed_entries.language` field if already set;
otherwise detects with fastText (176 languages, ISO 639-1 codes).
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

import structlog

from core.config import get_settings

logger = structlog.get_logger(__name__)

_DOWNLOAD_URL = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz"


@lru_cache(maxsize=1)
def _get_model():  # type: ignore[no-untyped-def]
    """Lazy-load the fastText LID model."""
    import fasttext

    model_dir = Path(get_settings().fasttext_model_dir)
    model_path = model_dir / "lid.176.ftz"

    if not model_path.exists():
        logger.info("downloading_fasttext_model", url=_DOWNLOAD_URL)
        model_dir.mkdir(parents=True, exist_ok=True)
        import urllib.request

        urllib.request.urlretrieve(_DOWNLOAD_URL, str(model_path))
        logger.info("fasttext_model_downloaded", path=str(model_path))

    model = fasttext.load_model(str(model_path))
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
        return str(lang_code)

    except Exception as exc:
        logger.warning("language_detection_failed", error=str(exc))
        return existing_lang or "und"
