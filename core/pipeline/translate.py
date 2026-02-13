"""
Title translation — translate article titles to English.

Strategy (by tier):
- Tier A: Simple heuristic — if language is already English, use title as-is.
          Otherwise, attempt local translation via argostranslate.
- Tier B: Same as Tier A.
- Tier C: Can use OpenAI for higher-quality translation.

The title_en field in feed_entries is used downstream by the summarizer
and by the news_clusters output for English-language summaries.
"""

from __future__ import annotations

from functools import lru_cache

import structlog

logger = structlog.get_logger(__name__)


def translate_title(
    title: str,
    source_lang: str | None = None,
    target_lang: str = "en",
) -> str:
    """
    Translate a title to English.

    Args:
        title: Original article title.
        source_lang: ISO 639-1 language code (e.g. "pt", "ar", "fr").
        target_lang: Target language code (default "en").

    Returns:
        English title. Returns original if already English or translation fails.
    """
    if not title or not title.strip():
        return title or ""

    # If already English, return as-is
    if source_lang and source_lang.lower() in ("en", "eng"):
        return title

    # Try local translation
    translated = _translate_local(title, source_lang, target_lang)
    if translated and translated != title:
        return translated

    # Fallback: return original title
    return title


@lru_cache(maxsize=1)
def _get_argos_available() -> bool:
    """Check if argostranslate is available."""
    try:
        import importlib.util

        return importlib.util.find_spec("argostranslate.translate") is not None
    except ImportError:
        logger.debug("argostranslate_not_available")
        return False


def _translate_local(
    text: str,
    source_lang: str | None,
    target_lang: str,
) -> str | None:
    """Attempt translation using argostranslate (offline)."""
    if not _get_argos_available():
        return None

    try:
        import argostranslate.translate

        # Get installed languages
        installed_languages = argostranslate.translate.get_installed_languages()

        # Find source and target language objects
        source = None
        target = None

        for lang in installed_languages:
            if source_lang and lang.code == source_lang:
                source = lang
            if lang.code == target_lang:
                target = lang

        if source is None or target is None:
            # Try to install the package
            _install_language_package(source_lang, target_lang)

            # Retry
            installed_languages = argostranslate.translate.get_installed_languages()
            for lang in installed_languages:
                if source_lang and lang.code == source_lang:
                    source = lang
                if lang.code == target_lang:
                    target = lang

        if source is None or target is None:
            return None

        # Find translation
        translation = source.get_translation(target)
        if translation is None:
            return None

        result = translation.translate(text)
        return result if result else None

    except Exception as exc:
        logger.debug("local_translation_failed", error=str(exc), lang=source_lang)
        return None


def _install_language_package(source_lang: str | None, target_lang: str) -> None:
    """Attempt to install an argostranslate language package."""
    if not source_lang:
        return

    try:
        import argostranslate.package

        argostranslate.package.update_package_index()
        available = argostranslate.package.get_available_packages()

        pkg = next(
            (p for p in available if p.from_code == source_lang and p.to_code == target_lang),
            None,
        )

        if pkg:
            logger.info(
                "installing_translation_package",
                from_lang=source_lang,
                to_lang=target_lang,
            )
            argostranslate.package.install_from_path(pkg.download())
    except Exception as exc:
        logger.debug("translation_package_install_failed", error=str(exc))


def extract_hostname(url: str | None) -> str | None:
    """Extract hostname from a URL.

    Also sets the hostname field in feed_entries which is
    expected to be filled by this project.
    """
    if not url:
        return None
    try:
        from urllib.parse import urlparse

        parsed = urlparse(url)
        return parsed.hostname
    except Exception:
        logger.debug("hostname_extraction_failed", url=url[:120])
        return None
