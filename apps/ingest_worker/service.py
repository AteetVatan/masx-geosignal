"""
Ingest Worker Service — fetch + extract + enrich + dedupe + embed.

Processes a batch of feed_entries through the ingestion pipeline:
1. Fetch raw HTML
2. Extract article text (ensemble)
3. Translate title → title_en
4. Extract hostname from URL
5. Run NER → entities
6. Resolve geo-entities → geo_entities
7. Deduplicate (hash + MinHash)
8. Compress content
9. Write enrichment fields back to feed_entries
10. Compute embeddings (sentence-transformers) — Tier B/C only
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import time
import uuid
from typing import Any, Sequence

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config.settings import Settings
from core.db.models import JobStatus
from core.db.repositories import FeedEntryJobRepo, FeedEntryRepo, VectorRepo
from core.db.table_resolver import TableContext
from core.pipeline.dedupe import DeduplicationEngine
from core.pipeline.extract import ExtractionFailed, extract_article_text
from core.pipeline.fetch import AsyncFetcher, DomainBlocked, FetchError
from core.pipeline.lang import detect_language
from core.pipeline.translate import translate_title, extract_hostname

logger = structlog.get_logger(__name__)


class IngestService:
    """Orchestrates the ingestion pipeline for a batch of entries."""

    def __init__(
        self,
        session: AsyncSession,
        run_id: str,
        settings: Settings,
        table_ctx: TableContext,
    ) -> None:
        self.session = session
        self.run_id = run_id
        self.settings = settings
        self.table_ctx = table_ctx
        self.entry_repo = FeedEntryRepo(session)
        self.job_repo = FeedEntryJobRepo(session)
        self.vector_repo = VectorRepo(session)
        self.dedupe_engine = DeduplicationEngine(
            num_perm=settings.minhash_num_perm,
            threshold=settings.minhash_threshold,
        )
        self._processed = 0
        self._failed = 0
        self._deduped = 0

    async def process_batch(self, entries: Sequence[dict[str, Any]]) -> dict:
        """Process all entries with concurrency control.

        Args:
            entries: List of dicts from FeedEntryRepo.get_unprocessed().
                     Each dict has keys: id, flashpoint_id, url, title, content, etc.
        """
        logger.info("ingest_batch_start", total=len(entries))

        async with AsyncFetcher(
            max_concurrent=self.settings.max_concurrent_fetches,
            per_domain=self.settings.per_domain_concurrency,
            timeout=self.settings.fetch_timeout_seconds,
            delay=self.settings.request_delay_seconds,
        ) as fetcher:
            # Process in chunks to manage memory and commits
            chunk_size = 100
            for i in range(0, len(entries), chunk_size):
                chunk = entries[i : i + chunk_size]
                tasks = [
                    self._process_single(entry, fetcher)
                    for entry in chunk
                ]
                await asyncio.gather(*tasks, return_exceptions=True)

                # Commit after each chunk
                await self.session.commit()

                logger.info(
                    "ingest_chunk_done",
                    chunk=i // chunk_size + 1,
                    processed=self._processed,
                    failed=self._failed,
                    deduped=self._deduped,
                )

        # Run embeddings in batch if tier allows
        if self.settings.tier_has_embeddings:
            await self._batch_embed()

        stats = {
            "processed": self._processed,
            "failed": self._failed,
            "deduped": self._deduped,
        }

        logger.info("ingest_batch_complete", **stats)
        return stats

    async def _process_single(
        self, entry: dict[str, Any], fetcher: AsyncFetcher
    ) -> None:
        """Process a single feed entry through the full enrichment pipeline.

        Pipeline: fetch → extract → translate → hostname → NER → geo → dedupe → store
        """
        entry_id = entry["id"]
        entry_id_str = str(entry_id)

        structlog.contextvars.bind_contextvars(
            feed_entry_id=entry_id_str,
            flashpoint_id=str(entry.get("flashpoint_id")),
        )

        start = time.monotonic()

        try:
            # ── Step 1: Fetch + Extract ──────────────────────
            url = entry.get("url")
            if not url:
                await self.job_repo.mark_failed(
                    entry_id, self.run_id, "No URL", "no_text"
                )
                self._failed += 1
                return

            try:
                fetch_result = await fetcher.fetch(url)
                html = fetch_result.html
                fetch_ms = fetch_result.duration_ms
            except DomainBlocked:
                await self.job_repo.mark_failed(
                    entry_id, self.run_id, "Domain blocked (circuit breaker)", "blocked"
                )
                self._failed += 1
                return
            except FetchError as exc:
                await self.job_repo.mark_failed(
                    entry_id, self.run_id, str(exc), "http_error"
                )
                self._failed += 1
                return

            # ── Step 2: Extract article text ─────────────────
            try:
                result = extract_article_text(
                    html,
                    min_length=self.settings.min_content_length,
                )
                text_content = result.text
                method = result.method
            except ExtractionFailed as exc:
                reason = "no_text"
                if "js_required" in str(exc):
                    reason = "js_required"
                elif "consent" in str(exc):
                    reason = "consent"
                elif "paywall" in str(exc):
                    reason = "paywall"

                await self.job_repo.mark_failed(
                    entry_id, self.run_id, str(exc)[:500], reason
                )
                self._failed += 1
                return

            extract_ms = int((time.monotonic() - start) * 1000) - fetch_ms

            # ── Step 3: Detect language ──────────────────────
            detected_lang = detect_language(text_content, entry.get("language"))

            # ── Step 4: Translate title → title_en ───────────
            title = entry.get("title") or ""
            title_en = translate_title(title, source_lang=detected_lang)

            # ── Step 5: Extract hostname ─────────────────────
            hostname = extract_hostname(url)

            # ── Step 6: NER → entities ───────────────────────
            entities_data = None
            geo_entities_data = None
            try:
                from core.pipeline.ner import extract_entities
                ner_result = extract_entities(text_content)
                entities_data = {**ner_result.entities, "meta": ner_result.meta}

                # ── Step 7: Geo-entities ─────────────────────
                from core.pipeline.geo import extract_geo_entities
                geo_entities_data = extract_geo_entities(
                    ner_result.entities,
                    source_country=entry.get("sourcecountry"),
                )
            except Exception as exc:
                logger.warning("enrichment_partial_failure", error=str(exc), step="ner_geo")

            # ── Step 8: Deduplicate ──────────────────────────
            dedupe_result = self.dedupe_engine.check_and_register(entry_id_str, text_content)

            if dedupe_result.is_exact_duplicate or dedupe_result.is_near_duplicate:
                # Even for duplicates, store enrichment data so the entry
                # has content (marks it as processed)
                compressed = base64.b64encode(
                    gzip.compress(text_content.encode("utf-8"))
                ).decode("ascii")

                await self.entry_repo.update_enrichment(
                    self.table_ctx,
                    entry_id,
                    content=text_content,
                    compressed_content=compressed,
                    title_en=title_en,
                    hostname=hostname,
                    entities=entities_data,
                    geo_entities=geo_entities_data,
                )

                await self.job_repo.update_status(
                    entry_id,
                    self.run_id,
                    JobStatus.SKIPPED_DUPLICATE,
                    content_hash=dedupe_result.content_hash,
                    is_duplicate=True,
                    duplicate_of=uuid.UUID(dedupe_result.duplicate_of) if dedupe_result.duplicate_of else None,
                )
                self._deduped += 1
                return

            # ── Step 9: Compress + Store all enrichment ──────
            compressed = base64.b64encode(
                gzip.compress(text_content.encode("utf-8"))
            ).decode("ascii")

            # Collect images from HTML if we have any
            images = _extract_images_from_html(html, url)

            await self.entry_repo.update_enrichment(
                self.table_ctx,
                entry_id,
                content=text_content,
                compressed_content=compressed,
                title_en=title_en,
                hostname=hostname,
                summary=None,  # Filled later during summarization stage
                entities=entities_data,
                geo_entities=geo_entities_data,
                images=images if images else None,
            )

            await self.job_repo.update_status(
                entry_id,
                self.run_id,
                JobStatus.EXTRACTED,
                extraction_method=method,
                extraction_chars=len(text_content),
                content_hash=dedupe_result.content_hash,
                fetch_duration_ms=fetch_ms,
                extract_duration_ms=extract_ms,
            )

            self._processed += 1

        except Exception as exc:
            logger.exception("entry_processing_error", error=str(exc))
            try:
                await self.job_repo.mark_failed(
                    entry_id, self.run_id, str(exc)[:500], "unknown"
                )
            except Exception:
                pass
            self._failed += 1

    async def _batch_embed(self) -> None:
        """Compute embeddings for all extracted entries in this run."""
        from core.pipeline.embed import embed_texts

        logger.info("batch_embedding_start")

        # Get all extracted (non-duplicate) entries from the date-partitioned table
        feed_table = self.table_ctx.feed_entries

        result = await self.session.execute(
            text(f"""
                SELECT fe.id, fe.content, fe.title
                FROM "{feed_table}" fe
                JOIN feed_entry_jobs jej ON fe.id = jej.feed_entry_id
                WHERE jej.run_id = :run_id
                AND jej.status = :status
                AND jej.is_duplicate = false
            """),
            {"run_id": self.run_id, "status": JobStatus.EXTRACTED.value},
        )
        rows = result.fetchall()

        if not rows:
            logger.info("no_entries_to_embed")
            return

        # Prepare texts for embedding (title + content snippet)
        entry_ids: list[uuid.UUID] = []
        texts: list[str] = []
        for row in rows:
            entry_ids.append(row[0])
            title = row[2] or ""
            content = (row[1] or "")[:1000]  # First 1000 chars
            texts.append(f"{title}. {content}".strip())

        # Batch embed
        embeddings = embed_texts(
            texts,
            model_name=self.settings.embedding_model,
            batch_size=64,
        )

        # Store in pgvector
        for eid, emb in zip(entry_ids, embeddings):
            await self.vector_repo.upsert_embedding(
                eid, emb, self.settings.embedding_model
            )

        # Update job status
        for eid in entry_ids:
            await self.job_repo.update_status(
                eid, self.run_id, JobStatus.EMBEDDED
            )

        await self.session.commit()
        logger.info("batch_embedding_complete", count=len(entry_ids))


def _extract_images_from_html(html: str, base_url: str) -> list[str]:
    """Extract image URLs from HTML content.

    Returns a list of image URLs found in og:image, article images, etc.
    """
    import re
    from urllib.parse import urljoin

    images: list[str] = []
    seen: set[str] = set()

    # Open Graph image
    og_match = re.search(
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\'](.*?)["\']',
        html, re.IGNORECASE,
    )
    if og_match:
        img = og_match.group(1).strip()
        if img and img not in seen:
            images.append(img)
            seen.add(img)

    # Twitter card image
    tw_match = re.search(
        r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\'](.*?)["\']',
        html, re.IGNORECASE,
    )
    if tw_match:
        img = tw_match.group(1).strip()
        if img and img not in seen:
            images.append(img)
            seen.add(img)

    # Article body images (limit to first 5 unique)
    for img_match in re.finditer(r'<img[^>]+src=["\'](.*?)["\']', html, re.IGNORECASE):
        if len(images) >= 5:
            break
        img = img_match.group(1).strip()
        if not img or img in seen:
            continue
        # Skip tiny/tracking pixels
        if any(x in img.lower() for x in ["1x1", "pixel", "tracker", "beacon", "spacer"]):
            continue
        # Make absolute URL
        if img.startswith("//"):
            img = "https:" + img
        elif img.startswith("/"):
            img = urljoin(base_url, img)
        if img.startswith("http"):
            images.append(img)
            seen.add(img)

    return images
