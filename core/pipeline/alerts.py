"""
Alert dispatchers — webhook, email, Slack stubs.

These are intentionally thin stubs for Week 3 scope.
Actual implementation would integrate with Slack API, SendGrid, etc.
"""

from __future__ import annotations

from dataclasses import dataclass

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class AlertPayload:
    """Alert data for a hot cluster."""

    flashpoint_id: str
    flashpoint_title: str
    cluster_id: int
    summary: str
    article_count: int
    hotspot_score: float
    top_domains: list[str]


async def dispatch_webhook(payload: AlertPayload, webhook_url: str) -> bool:
    """Send alert via webhook POST."""
    try:
        import httpx

        async with httpx.AsyncClient() as client:
            response = await client.post(
                webhook_url,
                json={
                    "type": "hotspot_alert",
                    "flashpoint_id": payload.flashpoint_id,
                    "flashpoint_title": payload.flashpoint_title,
                    "cluster_id": payload.cluster_id,
                    "summary": payload.summary,
                    "article_count": payload.article_count,
                    "hotspot_score": payload.hotspot_score,
                    "top_domains": payload.top_domains,
                },
                timeout=10.0,
            )
            response.raise_for_status()
            logger.info("webhook_sent", url=webhook_url, status=response.status_code)
            return True
    except Exception as exc:
        logger.error("webhook_failed", url=webhook_url, error=str(exc))
        return False


async def dispatch_slack(payload: AlertPayload, webhook_url: str) -> bool:
    """Send alert to Slack via incoming webhook."""
    try:
        import httpx

        slack_message = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"🔥 Hotspot Alert: {payload.flashpoint_title}",
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Score:* {payload.hotspot_score:.2f}"},
                        {"type": "mrkdwn", "text": f"*Articles:* {payload.article_count}"},
                        {"type": "mrkdwn", "text": f"*Cluster:* #{payload.cluster_id}"},
                    ],
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Summary:*\n{payload.summary[:500]}",
                    },
                },
            ],
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(webhook_url, json=slack_message, timeout=10.0)
            response.raise_for_status()
            logger.info("slack_alert_sent")
            return True
    except Exception as exc:
        logger.error("slack_alert_failed", error=str(exc))
        return False


async def dispatch_email_stub(payload: AlertPayload, recipient: str) -> bool:
    """Stub for email alerts — log only."""
    logger.info(
        "email_alert_stub",
        recipient=recipient,
        flashpoint=payload.flashpoint_title,
        score=payload.hotspot_score,
    )
    return True
