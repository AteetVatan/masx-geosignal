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
8. Write enrichment fields back to feed_entries
9. Compute embeddings (sentence-transformers) — Tier B/C only
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from typing import TYPE_CHECKING, Any

import structlog
from sqlalchemy import text

from core.db.models import JobStatus
from core.db.repositories import FeedEntryJobRepo, FeedEntryRepo, VectorRepo
from core.pipeline.dedupe import DeduplicationEngine
from core.pipeline.extract import ExtractionError, extract_article_text
from core.pipeline.fetch import AsyncFetcher, DomainBlockedError, FetchError
from core.pipeline.lang import detect_language
from core.pipeline.translate import extract_hostname, translate_title

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

    from core.config.settings import Settings
    from core.db.table_resolver import TableContext

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

    async def process_batch(self, entries: Sequence[dict[str, Any]]) -> dict[str, int]:
        """Process all entries with concurrency control.

        Fetches are parallelized via asyncio.gather (the slow part).
        DB writes remain sequential since AsyncSession is not thread-safe.

        Args:
            entries: List of dicts from FeedEntryRepo.get_unprocessed().
                     Each dict has keys: id, flashpoint_id, url, title, content, etc.
        """
        # Split entries: those needing fetch vs those with content from a prior run
        needs_fetch = [e for e in entries if not e.get("has_content")]
        already_fetched = [e for e in entries if e.get("has_content")]

        logger.info(
            "ingest_batch_start",
            total=len(entries),
            needs_fetch=len(needs_fetch),
            resuming=len(already_fetched),
        )

        # ── Process entries that already have content (resume path) ──
        if already_fetched:
            _t_resume = time.perf_counter()
            # Batch-fetch heavy columns (content, entities, geo_entities)
            resume_ids = [e["id"] for e in already_fetched]
            content_rows = await self.entry_repo.get_entry_content_batch(
                self.table_ctx, resume_ids
            )
            content_map = {row["id"]: row for row in content_rows}

            chunk_size = 100
            for i in range(0, len(already_fetched), chunk_size):
                chunk = already_fetched[i : i + chunk_size]
                for entry in chunk:
                    try:
                        # Merge heavy columns into entry dict
                        heavy = content_map.get(entry["id"], {})
                        entry["content"] = heavy.get("content")
                        entry["entities"] = heavy.get("entities")
                        entry["geo_entities"] = heavy.get("geo_entities")
                        await self._process_fetched(
                            entry, {"already_has_content": True}
                        )
                    except Exception:
                        logger.exception(
                            "unhandled_entry_error",
                            entry_id=str(entry.get("id")),
                        )
                        self._failed += 1
                await self.session.commit()
                self.session.expunge_all()  # Free ORM identity map memory

            logger.debug(
                "resume_chunk_done",
                processed=self._processed,
                failed=self._failed,
                deduped=self._deduped,
            )
            logger.info(
                "ingest_resume_done",
                entries=len(already_fetched),
                elapsed_s=round(time.perf_counter() - _t_resume, 2),
            )

        # ── Process entries that need fetching (normal path) ──
        _t_fetch_all = time.perf_counter()
        if needs_fetch:
            async with AsyncFetcher(
                max_concurrent=self.settings.max_concurrent_fetches,
                per_domain=self.settings.per_domain_concurrency,
                timeout=self.settings.fetch_timeout_seconds,
                delay=self.settings.request_delay_seconds,
            ) as fetcher:
                chunk_size = 100
                for i in range(0, len(needs_fetch), chunk_size):
                    chunk = needs_fetch[i : i + chunk_size]

                    # ── Phase 1: Fetch all URLs in this chunk concurrently ──
                    _t_chunk_fetch = time.perf_counter()
                    fetch_tasks = [
                        self._safe_fetch(entry, fetcher) for entry in chunk
                    ]
                    fetch_results = await asyncio.gather(*fetch_tasks)

                    logger.debug("fetch_results", results=fetch_results)

                    # ── Phase 2: Process results sequentially (DB writes) ──
                    for entry, fetch_outcome in zip(chunk, fetch_results, strict=True):
                        try:
                            await self._process_fetched(entry, fetch_outcome)
                        except Exception:
                            logger.exception(
                                "unhandled_entry_error",
                                entry_id=str(entry.get("id")),
                            )
                            self._failed += 1

                    # Commit after each chunk
                    await self.session.commit()
                    self.session.expunge_all()  # Free ORM identity map memory

                    logger.debug(
                        "ingest_chunk_done",
                        chunk=i // chunk_size + 1,
                        processed=self._processed,
                        failed=self._failed,
                        deduped=self._deduped,
                    )

        if needs_fetch:
            logger.info(
                "ingest_fetch_done",
                entries=len(needs_fetch),
                processed=self._processed,
                failed=self._failed,
                deduped=self._deduped,
                elapsed_s=round(time.perf_counter() - _t_fetch_all, 2),
            )

        # Run embeddings in batch if tier allows
        if self.settings.tier_has_embeddings:
            _t_embed = time.perf_counter()
            await self._batch_embed()
            logger.info(
                "ingest_embed_done",
                elapsed_s=round(time.perf_counter() - _t_embed, 2),
            )

        stats = {
            "processed": self._processed,
            "failed": self._failed,
            "deduped": self._deduped,
        }

        logger.info("ingest_batch_complete", **stats)
        return stats

    async def _safe_fetch(
        self, entry: dict[str, Any], fetcher: AsyncFetcher
    ) -> dict[str, Any]:
        """Fetch a single URL, returning a result dict instead of raising.

        This wrapper lets us use asyncio.gather without losing error info.
        Returns a dict with either 'result' (FetchResult) or 'error' key.
        """
        url = entry.get("url")
        if not url:
            return {"error": "no_url"}

        try:
            result = await fetcher.fetch(url)
            return {"result": result}
        except DomainBlockedError:
            return {"error": "blocked"}
        except FetchError as exc:
            return {"error": "http_error", "detail": str(exc)}
        except Exception as exc:
            return {"error": "fetch_exception", "detail": str(exc)}

    async def _process_fetched(
        self, entry: dict[str, Any], fetch_outcome: dict[str, Any]
    ) -> None:
        """Process a pre-fetched entry through extract → enrich → dedupe → store.

        Called sequentially after all fetches in a chunk complete.
        Supports a resume path: if fetch_outcome has 'already_has_content',
        the entry already has content from a prior run and we skip fetch+extract.
        """
        entry_id = entry["id"]
        entry_id_str = str(entry_id)

        structlog.contextvars.bind_contextvars(
            feed_entry_id=entry_id_str,
            flashpoint_id=str(entry.get("flashpoint_id")),
        )

        start = time.monotonic()

        try:
            # ── Resume shortcut: content already exists from a prior run ──
            if fetch_outcome.get("already_has_content"):
                text_content = entry["content"]
                url = entry["url"]
                logger.debug("resuming_with_existing_content", chars=len(text_content))
                # Jump directly to dedupe + enrichment gap-fill (Step 8)
                return await self._enrich_and_store(
                    entry, text_content, url, html=None, fetch_ms=0,
                )

            # ── Handle fetch errors from Phase 1 ─────────────
            error = fetch_outcome.get("error")
            if error == "no_url":
                await self.job_repo.mark_failed(entry_id, self.run_id, "No URL", "no_text")
                self._failed += 1
                return
            if error == "blocked":
                await self.job_repo.mark_failed(
                    entry_id, self.run_id, "Domain blocked (circuit breaker)", "blocked"
                )
                self._failed += 1
                return
            if error in ("http_error", "fetch_exception"):
                detail = fetch_outcome.get("detail", error)
                await self.job_repo.mark_failed(
                    entry_id, self.run_id, str(detail)[:500], "http_error"
                )
                self._failed += 1
                return

            fetch_result = fetch_outcome["result"]
            html = fetch_result.html
            fetch_ms = fetch_result.duration_ms
            url = entry["url"]

            # ── Step 2: Extract article text ─────────────────
            try:
                result = extract_article_text(
                    html,
                    min_length=self.settings.min_content_length,
                )
                text_content = result.text
                method = result.method
            except ExtractionError as exc:
                reason = "no_text"
                if "js_required" in str(exc):
                    reason = "js_required"
                elif "consent" in str(exc):
                    reason = "consent"
                elif "paywall" in str(exc):
                    reason = "paywall"

                await self.job_repo.mark_failed(entry_id, self.run_id, str(exc)[:500], reason)
                self._failed += 1
                return

            extract_ms = int((time.monotonic() - start) * 1000)

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

            # ── Step 8+9: Dedupe + store enrichment ──────────
            images = _extract_images_from_html(html, url) if html else None
            await self._enrich_and_store(
                entry, text_content, url, html=html,
                fetch_ms=fetch_ms, method=method, extract_ms=extract_ms,
                title_en=title_en, hostname=hostname,
                entities_data=entities_data, geo_entities_data=geo_entities_data,
                images=images,
            )

        except Exception as exc:
            logger.exception("entry_processing_error", error=str(exc))
            with contextlib.suppress(Exception):
                await self.job_repo.mark_failed(entry_id, self.run_id, str(exc)[:500], "unknown")
            self._failed += 1

    async def _enrich_and_store(
        self,
        entry: dict[str, Any],
        text_content: str,
        url: str,
        *,
        html: str | None = None,
        fetch_ms: int = 0,
        method: str | None = None,
        extract_ms: int | None = None,
        title_en: str | None = None,
        hostname: str | None = None,
        entities_data: dict[str, Any] | None = None,
        geo_entities_data: list[dict[str, Any]] | None = None,
        images: list[str] | None = None,
    ) -> None:
        """Shared dedupe + enrichment storage used by both fresh and resume paths.

        For resumed entries (no pre-computed enrichment args), fills gaps
        using values already present in the entry dict from the DB.
        """
        entry_id = entry["id"]
        entry_id_str = str(entry_id)

        # ── Fill enrichment gaps from existing entry data ──
        if title_en is None:
            title_en = entry.get("title_en")
            if not title_en:
                detected_lang = detect_language(text_content, entry.get("language"))
                title = entry.get("title") or ""
                title_en = translate_title(title, source_lang=detected_lang)

        if hostname is None:
            hostname = entry.get("hostname") or extract_hostname(url)

        if entities_data is None and not entry.get("entities"):
            try:
                from core.pipeline.ner import extract_entities

                ner_result = extract_entities(text_content)
                entities_data = {**ner_result.entities, "meta": ner_result.meta}

                if geo_entities_data is None and not entry.get("geo_entities"):
                    from core.pipeline.geo import extract_geo_entities

                    geo_entities_data = extract_geo_entities(
                        ner_result.entities,
                        source_country=entry.get("sourcecountry"),
                    )
            except Exception as exc:
                logger.warning("enrichment_partial_failure", error=str(exc), step="ner_geo")
        elif entities_data is None:
            entities_data = entry.get("entities")
        if geo_entities_data is None:
            geo_entities_data = entry.get("geo_entities")

        # ── Deduplicate ──
        dedupe_result = self.dedupe_engine.check_and_register(entry_id_str, text_content)

        if dedupe_result.is_exact_duplicate or dedupe_result.is_near_duplicate:
            await self.entry_repo.update_enrichment(
                self.table_ctx,
                entry_id,
                content=text_content,
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
                duplicate_of=uuid.UUID(dedupe_result.duplicate_of)
                if dedupe_result.duplicate_of
                else None,
            )
            self._deduped += 1
            return

        # ── Store enrichment ──
        await self.entry_repo.update_enrichment(
            self.table_ctx,
            entry_id,
            content=text_content,
            title_en=title_en,
            hostname=hostname,
            summary=None,
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

        # Bulk store in pgvector (chunked to stay within param limits)
        embedding_pairs = list(zip(entry_ids, embeddings, strict=True))
        await self.vector_repo.bulk_upsert_embeddings(
            embedding_pairs, self.settings.embedding_model
        )

        # Bulk update job status
        await self.job_repo.bulk_update_status(
            entry_ids, self.run_id, JobStatus.EMBEDDED
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
        html,
        re.IGNORECASE,
    )
    if og_match:
        img = og_match.group(1).strip()
        if img and img not in seen:
            images.append(img)
            seen.add(img)

    # Twitter card image
    tw_match = re.search(
        r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\'](.*?)["\']',
        html,
        re.IGNORECASE,
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
