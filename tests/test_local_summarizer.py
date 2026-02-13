"""Tests for the local DistilBART pre-summariser."""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from unittest.mock import MagicMock, patch

import pytest

from core.pipeline.local_summarizer import (
    _MIN_CONTENT_CHARS,
    chunk_text,
    presummarize_articles,
    summarize_article,
)


class TestChunkText:
    """Test BlingFire + tokenizer chunking."""

    @patch("core.pipeline.local_summarizer._get_tokenizer")
    def test_short_text_single_chunk(self, mock_tok: MagicMock) -> None:
        mock_tokenizer = MagicMock()
        mock_tokenizer.encode.side_effect = lambda s, **kw: s.split()
        mock_tok.return_value = mock_tokenizer

        chunks = chunk_text("Hello world. This is short.")
        assert len(chunks) >= 1
        assert "Hello" in chunks[0]

    @patch("core.pipeline.local_summarizer._get_tokenizer")
    def test_respects_chunk_limit(self, mock_tok: MagicMock) -> None:
        mock_tokenizer = MagicMock()
        mock_tokenizer.encode.side_effect = lambda s, **kw: s.split()
        mock_tok.return_value = mock_tokenizer

        text = ". ".join(f"Word{i}" for i in range(30))
        chunks = chunk_text(text, chunk_tokens=10, overlap_tokens=2)
        assert len(chunks) > 1

    def test_empty_text(self) -> None:
        result = chunk_text("")
        assert result == [] or result == [""]


class TestSummarizeArticle:
    """Test single-article summarisation."""

    def test_short_text_returned_as_is(self) -> None:
        short = "Short article."
        assert summarize_article(short) == short

    @patch("core.pipeline.local_summarizer._summarize_text")
    def test_long_text_dispatches(self, mock_summarize: MagicMock) -> None:
        mock_summarize.return_value = "Summarized."
        long_text = "Important news. " * 200

        result = summarize_article(long_text)
        assert result == "Summarized."
        mock_summarize.assert_called_once()


class TestPresummarizeArticles:
    """Test batch pre-summarisation with pool dispatch."""

    @patch("core.pipeline.local_summarizer._get_pool")
    def test_dispatches_long_articles_to_pool(self, mock_pool: MagicMock) -> None:
        mock_executor = MagicMock()
        mock_executor.map.return_value = ["Compressed."]
        mock_pool.return_value = mock_executor

        articles = [
            {"title": "Test", "content": "Long content here. " * 200, "language": "en"},
        ]
        result = presummarize_articles(articles)
        assert len(result) == 1
        assert result[0]["content"] == "Compressed."
        assert result[0]["title"] == "Test"
        mock_executor.map.assert_called_once()

    def test_short_content_skips_pool(self) -> None:
        articles = [{"title": "Short", "content": "Brief.", "language": "en"}]
        with patch("core.pipeline.local_summarizer._get_pool") as mock_pool:
            result = presummarize_articles(articles)
            mock_pool.assert_not_called()
        assert result[0]["content"] == "Brief."

    def test_empty_content_unchanged(self) -> None:
        articles = [{"title": "Empty", "content": "", "language": "en"}]
        with patch("core.pipeline.local_summarizer._get_pool") as mock_pool:
            result = presummarize_articles(articles)
            mock_pool.assert_not_called()
        assert result[0]["content"] == ""

    @patch("core.pipeline.local_summarizer._get_pool")
    def test_mixed_short_and_long(self, mock_pool: MagicMock) -> None:
        """Short articles skip pool, long ones get dispatched."""
        mock_executor = MagicMock()
        mock_executor.map.return_value = ["Long summary."]
        mock_pool.return_value = mock_executor

        articles = [
            {"title": "Short", "content": "Brief.", "language": "en"},
            {"title": "Long", "content": "Big article. " * 300, "language": "en"},
        ]
        result = presummarize_articles(articles)
        assert result[0]["content"] == "Brief."
        assert result[1]["content"] == "Long summary."
        # Only 1 article was dispatched to pool
        texts_sent = mock_executor.map.call_args[0][1]
        assert len(list(texts_sent)) == 1
