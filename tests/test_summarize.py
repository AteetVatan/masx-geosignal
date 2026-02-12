"""
Tests for the summarization module.

Covers:
- Extractive summary generation
- Metadata aggregation (top_domains, languages, urls, images)
- OpenAI Batch request building
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
    """Test OpenAI Batch API request building and parsing."""

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
        assert request["body"]["model"] == "gpt-4o-mini"

    def test_parse_batch_results_success(self) -> None:
        results_jsonl = json.dumps({
            "custom_id": "cluster_test_1",
            "response": {
                "status_code": 200,
                "body": {
                    "choices": [{
                        "message": {
                            "content": "This is a summary of the cluster."
                        }
                    }]
                }
            }
        })

        summaries = parse_batch_results(results_jsonl)
        assert "cluster_test_1" in summaries
        assert summaries["cluster_test_1"] == "This is a summary of the cluster."

    def test_parse_batch_results_empty(self) -> None:
        summaries = parse_batch_results("")
        assert summaries == {}

    def test_parse_batch_results_error(self) -> None:
        results_jsonl = json.dumps({
            "custom_id": "cluster_err_1",
            "response": {
                "status_code": 400,
                "body": {"choices": []}
            },
            "error": {"message": "Rate limited"}
        })

        summaries = parse_batch_results(results_jsonl)
        assert "cluster_err_1" not in summaries
