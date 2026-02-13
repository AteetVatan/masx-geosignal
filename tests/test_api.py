"""
Tests for the Pipeline Trigger API.

Uses FastAPI's TestClient (synchronous) with dependency overrides
so no real database or subprocess is needed.
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient


# ── Helpers ──────────────────────────────────────────────────

TEST_API_KEY = "test-key-12345"


@pytest.fixture(autouse=True)
def _patch_env(monkeypatch):
    """Ensure tests don't read the real .env / environment."""
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "development")
    monkeypatch.setenv("PIPELINE_API_KEY", TEST_API_KEY)
    # Clear cached settings so fresh Settings() is created each time
    from core.config.settings import get_settings

    get_settings.cache_clear()


@pytest.fixture()
def client(_patch_env):
    """TestClient with lifespan + real auth dependency."""
    from apps.api.main import app

    with TestClient(app, raise_server_exceptions=False) as c:
        yield c

    from core.config.settings import get_settings

    get_settings.cache_clear()


# ── Health ───────────────────────────────────────────────────


class TestHealth:
    def test_health_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "version" in data

    def test_health_no_auth_required(self, client):
        """Health endpoint should work without API key header."""
        resp = client.get("/health")
        assert resp.status_code == 200


# ── Auth ─────────────────────────────────────────────────────


class TestAuth:
    def test_missing_api_key(self, client):
        """Endpoints requiring auth should return 401 without key."""
        resp = client.post("/pipeline/run", json={"target_date": "2025-01-01"})
        assert resp.status_code == 401

    def test_wrong_api_key(self, client):
        """Endpoints requiring auth should return 401 with wrong key."""
        resp = client.post(
            "/pipeline/run",
            json={"target_date": "2025-01-01"},
            headers={"X-Api-Key": "wrong-key"},
        )
        assert resp.status_code == 401

    def test_correct_api_key_passes_auth(self, client):
        """Auth should pass with correct key (endpoint may fail on DB but not 401)."""
        with patch("apps.api.main.get_async_session") as mock_sf:
            mock_session = AsyncMock()
            mock_repo = AsyncMock()
            mock_repo.mark_stale_runs_failed.return_value = 0
            mock_repo.has_active_run.return_value = True  # Will return 409 instead of proceeding

            mock_sf.return_value.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_sf.return_value.return_value.__aexit__ = AsyncMock(return_value=None)

            with patch("apps.api.main.ProcessingRunRepo", return_value=mock_repo):
                resp = client.post(
                    "/pipeline/run",
                    json={"target_date": "2025-01-01"},
                    headers={"X-Api-Key": TEST_API_KEY},
                )

            # 409 means auth passed, hit the "already running" guard
            assert resp.status_code == 409


# ── Trigger ──────────────────────────────────────────────────


class TestTrigger:
    def test_trigger_success(self, client):
        """POST /pipeline/run should return 202 when no active run."""
        with (
            patch("apps.api.main.get_async_session") as mock_sf,
            patch("apps.api.main.asyncio.create_subprocess_exec") as mock_subprocess,
        ):
            mock_session = AsyncMock()
            mock_repo = AsyncMock()
            mock_repo.mark_stale_runs_failed.return_value = 0
            mock_repo.has_active_run.return_value = False

            mock_sf.return_value.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_sf.return_value.return_value.__aexit__ = AsyncMock(return_value=None)

            mock_proc = AsyncMock()
            mock_proc.pid = 12345
            mock_subprocess.return_value = mock_proc

            with patch("apps.api.main.ProcessingRunRepo", return_value=mock_repo):
                resp = client.post(
                    "/pipeline/run",
                    json={"target_date": "2025-11-03"},
                    headers={"X-Api-Key": TEST_API_KEY},
                )

        assert resp.status_code == 202
        data = resp.json()
        assert data["status"] == "accepted"
        assert data["target_date"] == "2025-11-03"

    def test_trigger_conflict(self, client):
        """POST /pipeline/run should return 409 when a run is active."""
        with patch("apps.api.main.get_async_session") as mock_sf:
            mock_session = AsyncMock()
            mock_repo = AsyncMock()
            mock_repo.mark_stale_runs_failed.return_value = 0
            mock_repo.has_active_run.return_value = True

            mock_sf.return_value.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_sf.return_value.return_value.__aexit__ = AsyncMock(return_value=None)

            with patch("apps.api.main.ProcessingRunRepo", return_value=mock_repo):
                resp = client.post(
                    "/pipeline/run",
                    json={"target_date": "2025-11-03"},
                    headers={"X-Api-Key": TEST_API_KEY},
                )

        assert resp.status_code == 409

    def test_trigger_with_tier_override(self, client):
        """POST /pipeline/run with explicit tier."""
        with (
            patch("apps.api.main.get_async_session") as mock_sf,
            patch("apps.api.main.asyncio.create_subprocess_exec") as mock_subprocess,
        ):
            mock_session = AsyncMock()
            mock_repo = AsyncMock()
            mock_repo.mark_stale_runs_failed.return_value = 0
            mock_repo.has_active_run.return_value = False

            mock_sf.return_value.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_sf.return_value.return_value.__aexit__ = AsyncMock(return_value=None)

            mock_proc = AsyncMock()
            mock_proc.pid = 12345
            mock_subprocess.return_value = mock_proc

            with patch("apps.api.main.ProcessingRunRepo", return_value=mock_repo):
                resp = client.post(
                    "/pipeline/run",
                    json={"target_date": "2025-11-03", "tier": "C"},
                    headers={"X-Api-Key": TEST_API_KEY},
                )

        assert resp.status_code == 202
        assert "tier C" in resp.json()["message"]


# ── Status ───────────────────────────────────────────────────


class TestStatus:
    def test_runs_by_date_not_found(self, client):
        """GET /pipeline/runs?date=... should return 404 when no runs."""
        with patch("apps.api.main.get_async_session") as mock_sf:
            mock_session = AsyncMock()
            mock_repo = AsyncMock()
            mock_repo.get_runs_by_date.return_value = []

            mock_sf.return_value.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_sf.return_value.return_value.__aexit__ = AsyncMock(return_value=None)

            with patch("apps.api.main.ProcessingRunRepo", return_value=mock_repo):
                resp = client.get(
                    "/pipeline/runs",
                    params={"date": "2025-01-01"},
                    headers={"X-Api-Key": TEST_API_KEY},
                )

        assert resp.status_code == 404

    def test_run_by_id_not_found(self, client):
        """GET /pipeline/runs/{run_id} should return 404 when not found."""
        with patch("apps.api.main.get_async_session") as mock_sf:
            mock_session = AsyncMock()
            mock_repo = AsyncMock()
            mock_repo.get_run_by_id.return_value = None

            mock_sf.return_value.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_sf.return_value.return_value.__aexit__ = AsyncMock(return_value=None)

            with patch("apps.api.main.ProcessingRunRepo", return_value=mock_repo):
                resp = client.get(
                    "/pipeline/runs/nonexistent-run",
                    headers={"X-Api-Key": TEST_API_KEY},
                )

        assert resp.status_code == 404

    def test_runs_by_date_returns_data(self, client):
        """GET /pipeline/runs?date=... should return run data."""
        mock_run = MagicMock()
        mock_run.run_id = "run_20251103_120000_abc12345"
        mock_run.status.value = "completed"
        mock_run.pipeline_tier = "A"
        mock_run.target_date = "2025-11-03"
        mock_run.started_at = None
        mock_run.completed_at = None
        mock_run.error_message = None
        mock_run.metrics = {"processed": 100}
        mock_run.created_at = None

        with patch("apps.api.main.get_async_session") as mock_sf:
            mock_session = AsyncMock()
            mock_repo = AsyncMock()
            mock_repo.get_runs_by_date.return_value = [mock_run]

            mock_sf.return_value.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_sf.return_value.return_value.__aexit__ = AsyncMock(return_value=None)

            with patch("apps.api.main.ProcessingRunRepo", return_value=mock_repo):
                resp = client.get(
                    "/pipeline/runs",
                    params={"date": "2025-11-03"},
                    headers={"X-Api-Key": TEST_API_KEY},
                )

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["run_id"] == "run_20251103_120000_abc12345"
        assert data[0]["status"] == "completed"


# ── SSRF ─────────────────────────────────────────────────────


class TestSSRF:
    """Test that the SSRF blocklist in AsyncFetcher works."""

    def test_blocks_private_ips(self):
        from core.pipeline.fetch import AsyncFetcher

        # Cloud metadata
        assert not AsyncFetcher._is_safe_url("http://169.254.169.254/latest/meta-data/")
        # Loopback
        assert not AsyncFetcher._is_safe_url("http://127.0.0.1/admin")
        # Private RFC1918
        assert not AsyncFetcher._is_safe_url("http://10.0.0.1/internal")
        assert not AsyncFetcher._is_safe_url("http://192.168.1.1/router")

    def test_allows_public_urls(self):
        from core.pipeline.fetch import AsyncFetcher

        assert AsyncFetcher._is_safe_url("https://www.example.com/article")
        assert AsyncFetcher._is_safe_url("https://bbc.co.uk/news")

    def test_blocks_non_http(self):
        from core.pipeline.fetch import AsyncFetcher

        assert not AsyncFetcher._is_safe_url("ftp://files.example.com/data")
        assert not AsyncFetcher._is_safe_url("file:///etc/passwd")
