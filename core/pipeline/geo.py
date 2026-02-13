"""
Geo-entity extraction — resolves LOC/GPE entities to country codes.

Takes NER output (LOC/GPE entities) and resolves them to structured
geo-entity records with ISO alpha-2/alpha-3 codes using pycountry.

Output format matches the upstream schema:
[
  {
    "name": "Brazil",
    "count": 23,
    "alpha2": "BR",
    "alpha3": "BRA",
    "avg_score": 0.8478
  },
  ...
]
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class GeoEntity:
    """A resolved geographic entity."""

    name: str
    count: int
    alpha2: str
    alpha3: str
    avg_score: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "count": self.count,
            "alpha2": self.alpha2,
            "alpha3": self.alpha3,
            "avg_score": round(self.avg_score, 4),
        }


# ── Country resolution cache ─────────────────────────

_COUNTRY_CACHE: dict[str, tuple[str, str, str] | None] = {}

# Common name variants that pycountry doesn't resolve
_NAME_OVERRIDES: dict[str, tuple[str, str, str]] = {
    "usa": ("United States", "US", "USA"),
    "u.s.": ("United States", "US", "USA"),
    "u. s.": ("United States", "US", "USA"),
    "u.s.a.": ("United States", "US", "USA"),
    "united states of america": ("United States", "US", "USA"),
    "united states": ("United States", "US", "USA"),
    "america": ("United States", "US", "USA"),
    "uk": ("United Kingdom", "GB", "GBR"),
    "u.k.": ("United Kingdom", "GB", "GBR"),
    "britain": ("United Kingdom", "GB", "GBR"),
    "great britain": ("United Kingdom", "GB", "GBR"),
    "england": ("United Kingdom", "GB", "GBR"),
    "russia": ("Russia", "RU", "RUS"),
    "south korea": ("South Korea", "KR", "KOR"),
    "north korea": ("North Korea", "KP", "PRK"),
    "iran": ("Iran", "IR", "IRN"),
    "syria": ("Syria", "SY", "SYR"),
    "palestine": ("Palestine", "PS", "PSE"),
    "taiwan": ("Taiwan", "TW", "TWN"),
    "czech republic": ("Czechia", "CZ", "CZE"),
    "ivory coast": ("Côte d'Ivoire", "CI", "CIV"),
    "congo": ("Congo", "CG", "COG"),
    "dr congo": ("DR Congo", "CD", "COD"),
    "drc": ("DR Congo", "CD", "COD"),
    "uae": ("United Arab Emirates", "AE", "ARE"),
}


def _resolve_country(name: str) -> tuple[str, str, str] | None:
    """Resolve a location name to (country_name, alpha2, alpha3).

    Returns None if the name cannot be resolved to a country.
    """
    key = name.lower().strip()

    if key in _COUNTRY_CACHE:
        return _COUNTRY_CACHE[key]

    # Check overrides first
    if key in _NAME_OVERRIDES:
        result = _NAME_OVERRIDES[key]
        _COUNTRY_CACHE[key] = result
        return result

    try:
        import pycountry

        # Try exact match
        country = pycountry.countries.get(name=name)
        if country:
            result = (country.name, country.alpha_2, country.alpha_3)
            _COUNTRY_CACHE[key] = result
            return result

        # Try common name
        country = pycountry.countries.get(common_name=name)
        if country:
            result = (country.common_name or country.name, country.alpha_2, country.alpha_3)
            _COUNTRY_CACHE[key] = result
            return result

        # Try fuzzy search
        results = pycountry.countries.search_fuzzy(name)
        if results:
            country = results[0]
            result = (country.name, country.alpha_2, country.alpha_3)  # type: ignore[attr-defined]
            _COUNTRY_CACHE[key] = result
            return result

    except (LookupError, Exception):
        pass

    _COUNTRY_CACHE[key] = None
    return None


def extract_geo_entities(
    ner_entities: dict[str, list[dict[str, Any]]],
    source_country: str | None = None,
) -> list[dict[str, Any]]:
    """
    Extract geo-entities from NER output.

    Takes the entities dict (with LOC, GPE categories) and resolves
    location names to countries with ISO codes.

    Args:
        ner_entities: Dict from NER extraction (keys: LOC, GPE, etc.)
        source_country: Source country from feed_entries (as fallback)

    Returns:
        List of geo-entity dicts matching upstream schema.
    """
    # Collect all location mentions with their scores
    location_mentions: list[tuple[str, float]] = []

    for category in ("LOC", "GPE"):
        for ent in ner_entities.get(category, []):
            text = ent.get("text", "")
            score = ent.get("score", 0.0)
            if text:
                location_mentions.append((text, score))

    # Resolve each mention to a country
    country_data: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"name": "", "alpha2": "", "alpha3": "", "scores": [], "count": 0}
    )

    for text, score in location_mentions:
        resolved = _resolve_country(text)
        if resolved:
            name, alpha2, alpha3 = resolved
            key = alpha3
            country_data[key]["name"] = name
            country_data[key]["alpha2"] = alpha2
            country_data[key]["alpha3"] = alpha3
            country_data[key]["scores"].append(score)
            country_data[key]["count"] += 1

    # Add source country if available and not already present
    if source_country:
        resolved = _resolve_country(source_country)
        if resolved:
            name, alpha2, alpha3 = resolved
            if alpha3 not in country_data:
                country_data[alpha3] = {
                    "name": name,
                    "alpha2": alpha2,
                    "alpha3": alpha3,
                    "scores": [0.5],  # Lower confidence for source-only
                    "count": 1,
                }

    # Build output
    result: list[dict[str, Any]] = []
    for data in country_data.values():
        scores = data["scores"]
        avg_score = sum(scores) / len(scores) if scores else 0.0
        result.append(
            GeoEntity(
                name=data["name"],
                count=data["count"],
                alpha2=data["alpha2"],
                alpha3=data["alpha3"],
                avg_score=avg_score,
            ).to_dict()
        )

    # Sort by count descending
    result.sort(key=lambda x: x["count"], reverse=True)

    return result
