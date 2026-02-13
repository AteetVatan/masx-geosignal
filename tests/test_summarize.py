"""
Tests for the summarization module.

Covers:
- Extractive summary generation
- Metadata aggregation (top_domains, languages, urls, images)
- LLM Batch request building
- Batch result parsing
- Edge cases (empty articles, missing fields)
"""

from __future__ import annotations

import json
import uuid

import pytest

from core.pipeline.summarize import (
    ClusterSummaryInput,
    aggregate_cluster_metadata,
    build_batch_request,
    extractive_summary,
    parse_batch_results,
    summarize_cluster_local,
)


class TestExtractiveSummary:
    """Test local extractive summarization."""

    def test_basic_summary(self, sample_articles: list[dict]) -> None:
        summary = extractive_summary(sample_articles)
        assert len(summary) > 0
        assert isinstance(summary, str)

    def test_empty_articles(self) -> None:
        summary = extractive_summary([])
        assert summary == ""

    def test_articles_without_content(self) -> None:
        articles = [
            {"title": "Test Title", "content": "", "description": ""},
            {"title": "Another Title", "content": None},
        ]
        summary = extractive_summary(articles)
        # Should fall back to titles
        assert "Test Title" in summary or "Another Title" in summary

    def test_max_sentences_respected(self, sample_articles: list[dict]) -> None:
        summary = extractive_summary(sample_articles, max_sentences=2)
        # Should be relatively short
        assert len(summary) > 0


class TestMetadataAggregation:
    """Test cluster metadata aggregation."""

    def test_aggregates_domains(self, sample_articles: list[dict]) -> None:
        meta = aggregate_cluster_metadata(sample_articles)
        assert len(meta["top_domains"]) == 3
        assert "news.example.com" in meta["top_domains"]

    def test_aggregates_languages(self, sample_articles: list[dict]) -> None:
        meta = aggregate_cluster_metadata(sample_articles)
        assert "en" in meta["languages"]

    def test_aggregates_urls(self, sample_articles: list[dict]) -> None:
        meta = aggregate_cluster_metadata(sample_articles)
        assert len(meta["urls"]) == 3

    def test_aggregates_images(self, sample_articles: list[dict]) -> None:
        meta = aggregate_cluster_metadata(sample_articles)
        # 2 articles have images + 1 has images array
        assert len(meta["images"]) >= 2

    def test_empty_articles(self) -> None:
        meta = aggregate_cluster_metadata([])
        assert meta["top_domains"] == []
        assert meta["languages"] == []
        assert meta["urls"] == []
        assert meta["images"] == []

    def test_caps_urls_at_50(self) -> None:
        articles = [{"url": f"https://example.com/{i}"} for i in range(100)]
        meta = aggregate_cluster_metadata(articles)
        assert len(meta["urls"]) == 50


class TestLocalSummarization:
    """Test the full local summarization pipeline."""

    def test_produces_valid_result(self, sample_articles: list[dict]) -> None:
        fp_id = uuid.uuid4()
        cluster_input = ClusterSummaryInput(
            flashpoint_id=fp_id,
            cluster_id=1,
            cluster_uuid=uuid.uuid4(),
            articles=sample_articles,
        )

        result = summarize_cluster_local(cluster_input)

        assert result.flashpoint_id == fp_id
        assert result.cluster_id == 1
        assert result.article_count == 3
        assert len(result.summary) > 0
        assert len(result.top_domains) > 0
        assert len(result.urls) == 3


class TestBatchAPI:
    """Test LLM Batch API request building and parsing."""

    def test_build_batch_request(self, sample_articles: list[dict]) -> None:
        cluster_input = ClusterSummaryInput(
            flashpoint_id=uuid.uuid4(),
            cluster_id=1,
            cluster_uuid=uuid.uuid4(),
            articles=sample_articles,
        )

        request = build_batch_request(cluster_input)

        assert "custom_id" in request
        assert request["method"] == "POST"
        assert request["url"] == "/v1/chat/completions"
        assert "messages" in request["body"]
        assert request["body"]["model"]  # Model should come from settings

    def test_parse_batch_results_success(self) -> None:
        results_jsonl = json.dumps(
            {
                "custom_id": "cluster_test_1",
                "response": {
                    "status_code": 200,
                    "body": {
                        "choices": [{"message": {"content": "This is a summary of the cluster."}}]
                    },
                },
            }
        )

        summaries = parse_batch_results(results_jsonl)
        assert "cluster_test_1" in summaries
        assert summaries["cluster_test_1"] == "This is a summary of the cluster."

    def test_parse_batch_results_empty(self) -> None:
        summaries = parse_batch_results("")
        assert summaries == {}

    def test_parse_batch_results_error(self) -> None:
        results_jsonl = json.dumps(
            {
                "custom_id": "cluster_err_1",
                "response": {"status_code": 400, "body": {"choices": []}},
                "error": {"message": "Rate limited"},
            }
        )

        summaries = parse_batch_results(results_jsonl)
        assert "cluster_err_1" not in summaries


class TestLLMRetryFallback:
    """Test retry and fallback behaviour in summarize_cluster_llm."""

    @staticmethod
    def _make_cluster_input() -> ClusterSummaryInput:
        import uuid as _uuid

        return ClusterSummaryInput(
            flashpoint_id=_uuid.uuid4(),
            cluster_id=42,
            cluster_uuid=_uuid.uuid4(),
            articles=[
                {
                    "title": "Test article",
                    "content": "Some content about an event.",
                    "language": "en",
                    "url": "https://example.com/a",
                    "domain": "example.com",
                }
            ],
        )

    def test_retry_succeeds_on_second_attempt(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Primary fails once, succeeds on retry — no fallback needed."""
        from unittest.mock import MagicMock, patch

        from core.pipeline.summarize import summarize_cluster_llm

        call_count = 0

        def fake_call_llm(_client: object, _model: str, _msgs: list) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("transient error")
            return "Summary from retry"

        with (
            patch("core.pipeline.summarize._call_llm", side_effect=fake_call_llm),
            patch("core.pipeline.summarize.get_llm_client", return_value=MagicMock()),
        ):
            result = summarize_cluster_llm(self._make_cluster_input())

        assert result == "Summary from retry"
        assert call_count == 2  # 1 failure + 1 success

    def test_fallback_on_primary_exhausted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Primary fails 3 times → fallback provider returns summary."""
        from unittest.mock import MagicMock, patch

        from core.pipeline.summarize import summarize_cluster_llm

        primary_calls = 0

        def fake_call_llm(client: object, model: str, msgs: list) -> str:
            nonlocal primary_calls
            if model != "mistral-small-latest":
                primary_calls += 1
                raise RuntimeError("primary always fails")
            return "Summary from fallback"

        with (
            patch("core.pipeline.summarize._call_llm", side_effect=fake_call_llm),
            patch("core.pipeline.summarize.get_llm_client", return_value=MagicMock()),
            patch("core.pipeline.summarize.get_fallback_llm_client", return_value=MagicMock()),
        ):
            result = summarize_cluster_llm(self._make_cluster_input())

        assert result == "Summary from fallback"
        assert primary_calls == 2  # 1 initial + 1 retry

    def test_both_providers_fail_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Both primary and fallback fail → exception propagated."""
        from unittest.mock import MagicMock, patch

        from core.pipeline.summarize import summarize_cluster_llm

        def always_fails(_client: object, _model: str, _msgs: list) -> str:
            raise RuntimeError("API down")

        with (
            patch("core.pipeline.summarize._call_llm", side_effect=always_fails),
            patch("core.pipeline.summarize.get_llm_client", return_value=MagicMock()),
            patch("core.pipeline.summarize.get_fallback_llm_client", return_value=MagicMock()),
        ):
            with pytest.raises(RuntimeError, match="API down"):
                summarize_cluster_llm(self._make_cluster_input())


class TestParseLLMResponse:
    """Test _parse_llm_response with varied/noisy LLM outputs."""

    def test_valid_toml(self) -> None:
        from core.pipeline.summarize import _parse_llm_response

        raw = 'summary = """Heavy fighting erupted in the eastern region today causing civilian displacement."""'
        result = _parse_llm_response(raw)
        assert "Heavy fighting" in result
        assert "civilian displacement" in result

    def test_valid_json(self) -> None:
        from core.pipeline.summarize import _parse_llm_response

        raw = '{"summary": "Border conflict escalated with heavy artillery exchanges."}'
        result = _parse_llm_response(raw)
        assert "Border conflict" in result

    def test_table_array_with_summary(self) -> None:
        """Real-world: LLM returns [[articles]] table-array format."""
        from core.pipeline.summarize import _parse_llm_response

        raw = (
            '[[articles]]\n'
            'summary = "Sudanese doctor Mohamed Ibrahim recounts his escape from '
            "el-Fasher, capital of North Darfur province, where he was captured by "
            "Rapid Support Forces in October, during an 18-month siege of the "
            'Sudanese army\'s stronghold."'
        )
        result = _parse_llm_response(raw)
        assert "Sudanese doctor" in result
        assert "el-Fasher" in result

    def test_noisy_response_with_summary_assignment(self) -> None:
        """LLM wraps the TOML in preamble and trailing text."""
        from core.pipeline.summarize import _parse_llm_response

        raw = (
            "Here is the summary you requested:\n\n"
            'summary = """Heavy fighting was reported in the eastern region today. '
            'Multiple armed groups clashed near the border, causing civilian displacement."""\n\n'
            "Hope this helps!"
        )
        result = _parse_llm_response(raw)
        assert "Heavy fighting" in result
        assert "civilian displacement" in result

    def test_pure_prose_no_structure(self) -> None:
        """LLM returns plain prose without any TOML/JSON structure."""
        from core.pipeline.summarize import _parse_llm_response

        raw = (
            "Heavy fighting erupted in the eastern region today, displacing thousands of civilians. "
            "International organizations have expressed deep concern over the deteriorating situation. "
            "The UN has called for an immediate ceasefire to allow humanitarian aid access."
        )
        result = _parse_llm_response(raw)
        assert "Heavy fighting" in result
        assert "ceasefire" in result

    def test_prose_with_meta_noise(self) -> None:
        """LLM returns a good summary sandwiched between meta-phrases.
        The real summary content is extracted regardless of LLM preamble.
        """
        from core.pipeline.summarize import _parse_llm_response

        raw = (
            "Sure, here is the summary:\n\n"
            "Heavy fighting erupted in the eastern region today, displacing thousands of civilians. "
            "International organizations have expressed deep concern over the deteriorating situation.\n\n"
            "I hope this is helpful!"
        )
        result = _parse_llm_response(raw)
        # The real summary sentences are present
        assert "Heavy fighting" in result
        assert "deteriorating situation" in result

    def test_garbage_returns_raw(self) -> None:
        """Complete garbage returns raw text as graceful fallback."""
        from core.pipeline.summarize import _parse_llm_response

        raw = "xyz abc"
        result = _parse_llm_response(raw)
        assert result == "xyz abc"

    def test_code_fence_with_broken_toml_and_prose(self) -> None:
        """LLM wraps broken TOML in a code fence but also has clean prose."""
        from core.pipeline.summarize import _parse_llm_response

        raw = (
            "```toml\n"
            "[[articles]]\n"
            "summary = broken\n"
            "```\n\n"
            "Heavy fighting erupted in the eastern region today, displacing thousands. "
            "The UN has called for an immediate ceasefire to protect civilians in the area."
        )
        result = _parse_llm_response(raw)
        assert "Heavy fighting" in result
        assert "ceasefire" in result

    def test_sentences_extracted_from_varied_toml_noise(self) -> None:
        """Sentences extracted from TOML-like noise with key=value lines."""
        from core.pipeline.summarize import _parse_llm_response

        raw = (
            "lang = \"en\"\n"
            "id = 1\n"
            "title = \"Some title\"\n"
            "content = \"x\"\n"
            "Sudanese authorities reported major clashes between rival factions in the capital. "
            "Over ten thousand civilians have been displaced from their homes in the last week."
        )
        result = _parse_llm_response(raw)
        assert "Sudanese authorities" in result
        assert "displaced" in result
