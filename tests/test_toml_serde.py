"""Tests for the TOML serde module (token-efficient LLM serialization)."""

from __future__ import annotations

import pytest

from core.pipeline.toml_serde import dict_to_toml, toml_to_dict


class TestDictToToml:
    """Test dict → TOML serialization."""

    def test_basic_dict(self) -> None:
        result = dict_to_toml({"title": "Hello", "count": 3})
        assert 'title = "Hello"' in result
        assert "count = 3" in result

    def test_strips_none_values(self) -> None:
        result = dict_to_toml({"title": "Hello", "missing": None})
        assert "title" in result
        assert "missing" not in result

    def test_nested_dict(self) -> None:
        result = dict_to_toml({"article": {"title": "Test", "lang": "en"}})
        assert "title" in result
        assert "lang" in result

    def test_list_of_dicts(self) -> None:
        data = {
            "articles": [
                {"id": 1, "title": "First"},
                {"id": 2, "title": "Second"},
            ]
        }
        result = dict_to_toml(data)
        assert "First" in result
        assert "Second" in result
        # Verify it round-trips correctly
        recovered = toml_to_dict(result)
        assert len(recovered["articles"]) == 2

    def test_float_coercion(self) -> None:
        """Known float fields (like 'confidence') should stay as floats."""
        result = dict_to_toml({"confidence": 1})
        # Should be 1.0 not 1
        assert "1.0" in result

    def test_empty_dict(self) -> None:
        result = dict_to_toml({})
        assert result.strip() == ""


class TestTomlToDict:
    """Test TOML → dict deserialization."""

    def test_basic_toml(self) -> None:
        result = toml_to_dict('summary = "A test summary"')
        assert result["summary"] == "A test summary"

    def test_fenced_toml(self) -> None:
        """Should strip markdown ```toml fences."""
        text = '```toml\nsummary = "Fenced summary"\n```'
        result = toml_to_dict(text)
        assert result["summary"] == "Fenced summary"

    def test_generic_fenced(self) -> None:
        """Should strip generic ``` fences."""
        text = '```\nsummary = "Generic fence"\n```'
        result = toml_to_dict(text)
        assert result["summary"] == "Generic fence"

    def test_mixed_output(self) -> None:
        """Should find TOML in mixed LLM output."""
        text = 'Here is the result:\nsummary = "Found in mixed"'
        result = toml_to_dict(text)
        assert result["summary"] == "Found in mixed"

    def test_invalid_toml_raises(self) -> None:
        with pytest.raises(ValueError, match="Could not parse TOML"):
            toml_to_dict("this is not valid toml {{{")

    def test_nested_toml(self) -> None:
        toml_str = """
[article]
title = "Test"
lang = "en"
"""
        result = toml_to_dict(toml_str)
        assert result["article"]["title"] == "Test"


class TestRoundTrip:
    """Test dict → TOML → dict round-trip."""

    def test_simple_roundtrip(self) -> None:
        original = {"summary": "A great summary about events."}
        toml_str = dict_to_toml(original)
        recovered = toml_to_dict(toml_str)
        assert recovered == original

    def test_articles_roundtrip(self) -> None:
        original = {
            "articles": [
                {"id": 1, "lang": "en", "title": "First", "content": "Some content"},
                {"id": 2, "lang": "fr", "title": "Second", "content": "More content"},
            ]
        }
        toml_str = dict_to_toml(original)
        recovered = toml_to_dict(toml_str)
        assert len(recovered["articles"]) == 2
        assert recovered["articles"][0]["title"] == "First"
        assert recovered["articles"][1]["lang"] == "fr"
