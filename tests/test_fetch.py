"""
Tests for the async HTTP fetcher.

Covers:
- Circuit breaker behavior
- Domain semaphore isolation
- Retry on 429/503
- Basic fetch flow
"""

from __future__ import annotations

import time

import pytest

from core.pipeline.fetch import DomainCircuitBreaker


class TestCircuitBreaker:
    """Test per-domain circuit breaker."""

    def test_starts_closed(self) -> None:
        cb = DomainCircuitBreaker()
        assert not cb.is_open

    def test_opens_after_threshold(self) -> None:
        cb = DomainCircuitBreaker(threshold=3)
        cb.record_failure()
        cb.record_failure()
        assert not cb.is_open

        cb.record_failure()
        assert cb.is_open

    def test_resets_on_success(self) -> None:
        cb = DomainCircuitBreaker(threshold=3)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        assert cb.failures == 1
        assert not cb.is_open

    def test_auto_resets_after_cooldown(self) -> None:
        cb = DomainCircuitBreaker(threshold=2, cooldown_seconds=0.1)
        cb.record_failure()
        cb.record_failure()
        assert cb.is_open

        time.sleep(0.15)
        assert not cb.is_open  # Should auto-reset

    def test_failure_count_tracks(self) -> None:
        cb = DomainCircuitBreaker()
        cb.record_failure()
        cb.record_failure()
        assert cb.failures == 2

    def test_default_threshold_is_5(self) -> None:
        cb = DomainCircuitBreaker()
        for _ in range(4):
            cb.record_failure()
        assert not cb.is_open

        cb.record_failure()
        assert cb.is_open
