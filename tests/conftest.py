"""
Shared test fixtures.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest


@pytest.fixture
def sample_html() -> str:
    """Sample HTML for extraction tests."""
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head><title>Test Article</title></head>
    <body>
        <article>
            <h1>Breaking: Major Event in Capital City</h1>
            <p class="byline">By John Reporter | January 15, 2026</p>
            <div class="article-body">
                <p>A major event occurred today in the capital city, drawing attention
                from international observers. The incident began early in the morning
                when authorities reported unusual activity near the government district.</p>

                <p>Officials from multiple agencies responded to the scene, establishing
                a security perimeter around the affected area. Witnesses described seeing
                a significant police presence and emergency vehicles throughout the day.</p>

                <p>The government issued a statement calling for calm and assuring the
                public that the situation was under control. International partners
                expressed concern and offered assistance.</p>

                <p>"We are monitoring the situation closely," said the spokesperson for
                the foreign ministry. "Our priority is the safety of all citizens and
                foreign nationals in the area."</p>

                <p>Local hospitals reported treating several individuals for minor injuries.
                No casualties have been confirmed at this time. Transportation services
                in the area were temporarily suspended but have since resumed normal
                operations.</p>
            </div>
        </article>
    </body>
    </html>
    """


@pytest.fixture
def sample_js_heavy_html() -> str:
    """HTML from a SPA that needs JS rendering."""
    return """
    <!DOCTYPE html>
    <html>
    <head><title>App</title></head>
    <body>
        <div id="app"></div>
        <script>window.__NUXT__={}</script>
        <noscript>Please enable JavaScript to view this page.</noscript>
    </body>
    </html>
    """


@pytest.fixture
def sample_paywall_html() -> str:
    """HTML with paywall indicators."""
    return """
    <!DOCTYPE html>
    <html>
    <body>
        <article>
            <h1>Exclusive Report</h1>
            <p>Subscribe to continue reading this premium content.</p>
            <div class="paywall">Sign in to read the full article.</div>
        </article>
    </body>
    </html>
    """


@pytest.fixture
def sample_articles() -> list[dict]:
    """Sample article data for cluster tests."""
    return [
        {
            "feed_entry_id": str(uuid.uuid4()),
            "title": "Fighting intensifies in region",
            "title_en": "Fighting intensifies in region",
            "content": "Heavy fighting was reported in the eastern region today. "
            "Multiple armed groups clashed near the border, causing civilian "
            "displacement. UN observers called for an immediate ceasefire.",
            "url": "https://news.example.com/article1",
            "domain": "news.example.com",
            "hostname": "news.example.com",
            "language": "en",
            "image": "https://img.example.com/1.jpg",
            "images": [],
            "description": "Fighting in eastern region",
        },
        {
            "feed_entry_id": str(uuid.uuid4()),
            "title": "Border conflict escalates",
            "title_en": "Border conflict escalates",
            "content": "The border conflict escalated today with reports of heavy "
            "artillery exchanges. International organizations expressed deep concern "
            "over the deteriorating security situation.",
            "url": "https://world.example.org/article2",
            "domain": "world.example.org",
            "hostname": "world.example.org",
            "language": "en",
            "image": "https://img.example.org/2.jpg",
            "images": [],
            "description": "Border conflict escalation",
        },
        {
            "feed_entry_id": str(uuid.uuid4()),
            "title": "Humanitarian crisis deepens",
            "title_en": "Humanitarian crisis deepens",
            "content": "The humanitarian crisis in the conflict zone continued to "
            "deepen as aid agencies reported difficulty accessing affected populations. "
            "Over 50,000 people have been displaced.",
            "url": "https://aid.example.net/article3",
            "domain": "aid.example.net",
            "hostname": "aid.example.net",
            "language": "en",
            "image": "",
            "images": ["https://img.example.net/3a.jpg", "https://img.example.net/3b.jpg"],
            "description": "Humanitarian crisis",
        },
    ]


@pytest.fixture
def sample_entry_id() -> uuid.UUID:
    """A fixed UUID for deterministic tests."""
    return uuid.UUID("12345678-1234-5678-1234-567812345678")


@pytest.fixture
def sample_flashpoint_id() -> uuid.UUID:
    """A fixed flashpoint UUID."""
    return uuid.UUID("abcdef01-2345-6789-abcd-ef0123456789")
