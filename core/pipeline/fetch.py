"""
Async HTTP fetcher with per-domain concurrency, backoff, and circuit breaker.

Design:
- Global semaphore limits total concurrent connections
- Per-domain semaphores limit concurrency to any single host
- Tenacity retry for 429/503 with exponential backoff
- Per-domain circuit breaker: if too many failures, skip that domain
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = structlog.get_logger(__name__)


class FetchError(Exception):
    """HTTP fetch failed after retries."""


class DomainBlocked(Exception):
    """Domain circuit breaker is open."""


@dataclass
class FetchResult:
    """Outcome of a URL fetch."""

    url: str
    html: str
    status_code: int
    duration_ms: int
    content_type: str = ""
    final_url: str = ""


@dataclass
class DomainCircuitBreaker:
    """Simple circuit breaker per domain."""

    failures: int = 0
    last_failure: float = 0.0
    threshold: int = 5
    cooldown_seconds: float = 300.0  # 5 minutes

    @property
    def is_open(self) -> bool:
        if self.failures < self.threshold:
            return False
        # Auto-reset after cooldown
        if time.monotonic() - self.last_failure > self.cooldown_seconds:
            self.failures = 0
            return False
        return True

    def record_failure(self) -> None:
        self.failures += 1
        self.last_failure = time.monotonic()

    def record_success(self) -> None:
        self.failures = max(0, self.failures - 1)


class AsyncFetcher:
    """
    High-throughput async URL fetcher with domain-level controls.

    Usage:
        async with AsyncFetcher(max_concurrent=50, per_domain=3) as fetcher:
            result = await fetcher.fetch("https://example.com/article")
    """

    def __init__(
        self,
        max_concurrent: int = 50,
        per_domain: int = 3,
        timeout: int = 30,
        delay: float = 0.25,
    ) -> None:
        self._global_sem = asyncio.Semaphore(max_concurrent)
        self._domain_sems: dict[str, asyncio.Semaphore] = defaultdict(
            lambda: asyncio.Semaphore(per_domain)
        )
        self._circuit_breakers: dict[str, DomainCircuitBreaker] = defaultdict(
            DomainCircuitBreaker
        )
        self._timeout = timeout
        self._delay = delay
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> AsyncFetcher:
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._timeout, connect=10.0),
            follow_redirects=True,
            http2=True,
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20,
            ),
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (compatible; GSGI-Bot/1.0; "
                    "+https://github.com/masx-gsgi)"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
            },
        )
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._client:
            await self._client.aclose()

    def _get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        from urllib.parse import urlparse

        return urlparse(url).netloc

    async def fetch(self, url: str) -> FetchResult:
        """Fetch a URL with all protections."""
        domain = self._get_domain(url)
        cb = self._circuit_breakers[domain]

        if cb.is_open:
            raise DomainBlocked(f"Circuit breaker open for {domain}")

        async with self._global_sem:
            async with self._domain_sems[domain]:
                return await self._do_fetch(url, domain, cb)

    @retry(
        retry=retry_if_exception_type(httpx.HTTPStatusError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    async def _do_fetch(
        self, url: str, domain: str, cb: DomainCircuitBreaker
    ) -> FetchResult:
        """Execute the actual HTTP request with retry."""
        assert self._client is not None

        start = time.monotonic()
        try:
            response = await self._client.get(url)

            # Raise for 429/503 to trigger retry
            if response.status_code in (429, 503):
                retry_after = response.headers.get("Retry-After", "5")
                try:
                    wait_time = min(int(retry_after), 60)
                except ValueError:
                    wait_time = 5
                logger.warning(
                    "rate_limited",
                    url=url,
                    status=response.status_code,
                    retry_after=wait_time,
                )
                await asyncio.sleep(wait_time)
                response.raise_for_status()

            response.raise_for_status()
            cb.record_success()

            duration = int((time.monotonic() - start) * 1000)

            # Polite delay
            if self._delay > 0:
                await asyncio.sleep(self._delay)

            return FetchResult(
                url=url,
                html=response.text,
                status_code=response.status_code,
                duration_ms=duration,
                content_type=response.headers.get("content-type", ""),
                final_url=str(response.url),
            )

        except httpx.HTTPStatusError:
            cb.record_failure()
            raise
        except Exception as exc:
            cb.record_failure()
            duration = int((time.monotonic() - start) * 1000)
            raise FetchError(
                f"Failed to fetch {url}: {exc}"
            ) from exc

    def get_domain_stats(self) -> dict[str, dict]:
        """Return circuit breaker stats per domain."""
        return {
            domain: {
                "failures": cb.failures,
                "is_open": cb.is_open,
            }
            for domain, cb in self._circuit_breakers.items()
        }
