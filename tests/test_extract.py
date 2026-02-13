"""
Tests for the extraction ensemble.

Covers:
- Trafilatura extraction on real-ish HTML
- Fallback behavior when extraction fails
- Heuristic detection (JS, consent, paywall)
- Text sanitization
- Ensemble orchestration ordering
"""

from __future__ import annotations

import pytest

from core.pipeline.extract import (
    ExtractionError,
    _extract_boilerpy3,
    _extract_justext,
    _extract_readability,
    _extract_trafilatura,
    _sanitize_text,
    detect_failure_reason,
    extract_article_text,
    needs_browser_rendering,
)


class TestIndividualExtractors:
    """Test each extractor independently."""

    def test_trafilatura_extracts_article(self, sample_html: str) -> None:
        result = _extract_trafilatura(sample_html)
        assert result is not None
        assert len(result) > 100
        assert "major event" in result.lower() or "capital city" in result.lower()

    def test_readability_extracts_article(self, sample_html: str) -> None:
        result = _extract_readability(sample_html)
        assert result is not None
        assert len(result) > 50

    def test_justext_extracts_article(self, sample_html: str) -> None:
        result = _extract_justext(sample_html)
        # jusText may or may not extract depending on stoplist match
        # Just verify it doesn't crash
        if result:
            assert len(result) > 0

    def test_boilerpy3_extracts_article(self, sample_html: str) -> None:
        result = _extract_boilerpy3(sample_html)
        if result:
            assert len(result) > 0

    def test_extractors_handle_empty_html(self) -> None:
        assert _extract_trafilatura("") is None
        assert _extract_readability("") is None

    def test_extractors_handle_garbage(self) -> None:
        garbage = "<html><body>x</body></html>"
        # Should not crash, may return None or short text
        _extract_trafilatura(garbage)
        _extract_readability(garbage)
        _extract_justext(garbage)
        _extract_boilerpy3(garbage)


class TestHeuristics:
    """Test failure reason detection."""

    def test_detects_js_required(self, sample_js_heavy_html: str) -> None:
        reason = detect_failure_reason(sample_js_heavy_html, None)
        assert reason in ("js_required", "consent", "no_text")

    def test_detects_paywall(self, sample_paywall_html: str) -> None:
        reason = detect_failure_reason(sample_paywall_html, None)
        assert reason == "paywall"

    def test_detects_no_text(self) -> None:
        reason = detect_failure_reason("", None)
        assert reason == "no_text"

    def test_no_issue_with_good_html(self, sample_html: str) -> None:
        # With extracted text present, should not flag issues
        reason = detect_failure_reason(sample_html, "Some valid text content here")
        assert reason is None

    def test_needs_browser_js_page(self, sample_js_heavy_html: str) -> None:
        assert needs_browser_rendering(sample_js_heavy_html) is True

    def test_no_browser_needed_for_good_html(self, sample_html: str) -> None:
        assert needs_browser_rendering(sample_html) is False


class TestEnsemble:
    """Test the full ensemble orchestration."""

    def test_ensemble_extracts_good_html(self, sample_html: str) -> None:
        result = extract_article_text(sample_html, min_length=50)
        assert result.text
        assert len(result.text) >= 50
        assert result.method in ("trafilatura", "readability", "justext", "boilerpy3")
        assert result.char_count > 0
        assert result.duration_ms >= 0

    def test_ensemble_fails_on_empty(self) -> None:
        with pytest.raises(ExtractionError):
            extract_article_text("", min_length=100)

    def test_ensemble_respects_min_length(self, sample_html: str) -> None:
        result = extract_article_text(sample_html, min_length=10)
        assert len(result.text) >= 10


class TestSanitization:
    """Test text sanitization."""

    def test_removes_null_bytes(self) -> None:
        text = "Hello\x00World\x01Test"
        clean = _sanitize_text(text)
        assert "\x00" not in clean
        assert "\x01" not in clean

    def test_normalizes_whitespace(self) -> None:
        text = "Hello   World\t\tTest"
        clean = _sanitize_text(text)
        assert "   " not in clean
        assert "\t" not in clean

    def test_preserves_paragraph_breaks(self) -> None:
        text = "Paragraph one.\n\nParagraph two."
        clean = _sanitize_text(text)
        assert "\n\n" in clean

    def test_collapses_excessive_newlines(self) -> None:
        text = "One.\n\n\n\n\nTwo."
        clean = _sanitize_text(text)
        # Should collapse to max 2 newlines
        assert "\n\n\n" not in clean
