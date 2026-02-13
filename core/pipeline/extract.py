"""
Extraction ensemble — reliable article text extraction.

Strategy: "download first, extract later" with ordered fallback:
  1. Trafilatura (favor_recall)
  2. readability-lxml
  3. jusText boilerplate removal
  4. BoilerPy3 (SAX-based)
  5. Playwright browser rendering (only when JS/consent detected)

Each extractor returns (text, method_name) or raises ExtractionError.
The ensemble tries each in order until one yields text above the
min_content_length threshold.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field

import structlog

logger = structlog.get_logger(__name__)


class ExtractionError(Exception):
    """All extractors failed to produce usable content."""


@dataclass
class ExtractionResult:
    """Output of the extraction ensemble."""

    text: str
    method: str
    char_count: int
    duration_ms: int
    raw_html_size: int = 0
    warnings: list[str] = field(default_factory=list)


# ── Individual Extractors ─────────────────────────────


def _extract_trafilatura(html: str) -> str | None:
    """Primary extractor using Trafilatura with favor_recall."""
    try:
        import trafilatura

        result = trafilatura.extract(
            html,
            favor_recall=True,
            include_comments=False,
            include_tables=True,
            no_fallback=False,
            deduplicate=True,
        )
        return result
    except Exception as exc:
        logger.debug("trafilatura_failed", error=str(exc))
        return None


def _extract_readability(html: str) -> str | None:
    """Fallback #1: readability-lxml."""
    try:
        from readability import Document

        doc = Document(html)
        summary_html = doc.summary()

        # Strip remaining HTML tags
        from lxml import etree

        tree = etree.fromstring(summary_html, parser=etree.HTMLParser())
        text = etree.tostring(tree, method="text", encoding="unicode")
        return text.strip() if text else None
    except Exception as exc:
        logger.debug("readability_failed", error=str(exc))
        return None


def _extract_justext(html: str) -> str | None:
    """Fallback #2: jusText boilerplate removal."""
    try:
        import justext

        paragraphs = justext.justext(html, justext.get_stoplist("English"))
        good_paragraphs = [p.text for p in paragraphs if not p.is_boilerplate]
        text = "\n\n".join(good_paragraphs)
        return text.strip() if text else None
    except Exception as exc:
        logger.debug("justext_failed", error=str(exc))
        return None


def _extract_boilerpy3(html: str) -> str | None:
    """Fallback #3: BoilerPy3 (SAX-based extraction)."""
    try:
        from boilerpy3.extractors import ArticleExtractor

        extractor = ArticleExtractor()
        text = extractor.get_content(html)
        return text.strip() if text else None
    except Exception as exc:
        logger.debug("boilerpy3_failed", error=str(exc))
        return None


# ── Heuristics ────────────────────────────────────────


# Patterns that suggest JS rendering / consent / paywall is needed
JS_INDICATORS = [
    re.compile(r"<noscript[^>]*>.*?enable\s+javascript", re.IGNORECASE | re.DOTALL),
    re.compile(r"window\.__NUXT__", re.IGNORECASE),
    re.compile(r"<div[^>]*id=[\"']app[\"'][^>]*>\s*</div>", re.IGNORECASE),
    re.compile(r"react-root|__next", re.IGNORECASE),
]

CONSENT_INDICATORS = [
    re.compile(r"cookie[- ]?consent|cookie[- ]?banner|gdpr", re.IGNORECASE),
    re.compile(r"accept.*cookies|manage.*preferences", re.IGNORECASE),
]

PAYWALL_INDICATORS = [
    re.compile(r"subscribe\s+to\s+continue|paywall|premium\s+content", re.IGNORECASE),
    re.compile(r"sign\s+in\s+to\s+read|create.*account.*to.*continue", re.IGNORECASE),
]


def detect_failure_reason(html: str, extracted_text: str | None) -> str | None:
    """Detect why extraction might have failed or produced poor results."""
    if not html:
        return "no_text"

    if any(p.search(html) for p in PAYWALL_INDICATORS):
        return "paywall"

    if any(p.search(html) for p in CONSENT_INDICATORS):
        return "consent"

    if any(p.search(html) for p in JS_INDICATORS):
        # Check if the body is mostly empty (SPA shell)
        body_match = re.search(r"<body[^>]*>(.*?)</body>", html, re.DOTALL | re.IGNORECASE)
        if body_match:
            body_text = re.sub(r"<[^>]+>", "", body_match.group(1)).strip()
            if len(body_text) < 100:
                return "js_required"

    if extracted_text is None or len(extracted_text.strip()) == 0:
        return "no_text"

    return None


def needs_browser_rendering(html: str) -> bool:
    """Heuristic: does this page need a real browser?"""
    reason = detect_failure_reason(html, None)
    return reason in ("js_required", "consent")


# ── Ensemble Orchestrator ─────────────────────────────


_EXTRACTORS = [
    ("trafilatura", _extract_trafilatura),
    ("readability", _extract_readability),
    ("justext", _extract_justext),
    ("boilerpy3", _extract_boilerpy3),
]


def extract_article_text(
    html: str,
    min_length: int = 200,
    use_browser_fallback: bool = False,
) -> ExtractionResult:
    """
    Run the extraction ensemble on raw HTML.

    Tries each extractor in order.  If all fail and heuristics indicate
    JS/consent is needed AND browser fallback is enabled, raises
    ExtractionError with reason so the caller can dispatch Playwright.
    """
    start = time.monotonic()
    warnings: list[str] = []

    for method_name, extractor_fn in _EXTRACTORS:
        text = extractor_fn(html)
        if text and len(text.strip()) >= min_length:
            duration = int((time.monotonic() - start) * 1000)
            logger.info(
                "extraction_success",
                method=method_name,
                chars=len(text),
                duration_ms=duration,
            )
            return ExtractionResult(
                text=_sanitize_text(text),
                method=method_name,
                char_count=len(text),
                duration_ms=duration,
                raw_html_size=len(html),
            )
        if text:
            warnings.append(f"{method_name}: too short ({len(text)} chars)")
        else:
            warnings.append(f"{method_name}: returned None")

    # All extractors failed — detect reason
    failure_reason = detect_failure_reason(html, None)
    duration = int((time.monotonic() - start) * 1000)

    raise ExtractionError(
        f"All extractors failed. Reason: {failure_reason}. "
        f"Tried: {', '.join(w for w in warnings)}. "
        f"Browser needed: {needs_browser_rendering(html)}."
    )


def _sanitize_text(text: str) -> str:
    """Clean extracted text: normalize whitespace, strip control chars."""
    # Remove null bytes and other control characters
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)
    # Normalize whitespace (preserve paragraph breaks)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()
