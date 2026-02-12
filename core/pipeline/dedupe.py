"""
Deduplication module — exact + near-duplicate detection.

Exact duplicates:  SHA-256 of normalized text.
Near duplicates:   MinHash LSH via datasketch.

The strategy is dedupe-first to avoid wasting compute on embeddings/clustering
for content we've already processed.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass

import structlog
from datasketch import MinHash, MinHashLSH

logger = structlog.get_logger(__name__)


@dataclass
class DedupeResult:
    """Output of deduplication check."""

    content_hash: str
    is_exact_duplicate: bool
    is_near_duplicate: bool
    duplicate_of: str | None = None  # feed_entry_id of the original
    similarity: float = 0.0


class DeduplicationEngine:
    """
    Two-level deduplication: exact hash + MinHash LSH.

    Usage:
        engine = DeduplicationEngine(threshold=0.8)
        # For each article:
        result = engine.check_and_register(entry_id, text)
        if result.is_exact_duplicate or result.is_near_duplicate:
            skip_processing(entry_id)
    """

    def __init__(
        self,
        num_perm: int = 128,
        threshold: float = 0.8,
    ) -> None:
        self._num_perm = num_perm
        self._lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
        self._hashes: dict[str, str] = {}  # content_hash → entry_id
        self._minhashes: dict[str, MinHash] = {}  # entry_id → MinHash

    def normalize_text(self, text: str) -> str:
        """Normalize text for consistent hashing."""
        # Lowercase
        text = text.lower()
        # Unicode normalize
        text = unicodedata.normalize("NFKD", text)
        # Remove extra whitespace
        text = re.sub(r"\s+", " ", text).strip()
        # Remove punctuation (keep alphanumeric + spaces)
        text = re.sub(r"[^\w\s]", "", text)
        return text

    def compute_content_hash(self, text: str) -> str:
        """SHA-256 of normalized text."""
        normalized = self.normalize_text(text)
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    def compute_minhash(self, text: str) -> MinHash:
        """Compute MinHash signature from text shingles."""
        normalized = self.normalize_text(text)
        # Use word-level 3-shingles
        words = normalized.split()
        shingles = set()
        for i in range(len(words) - 2):
            shingle = " ".join(words[i : i + 3])
            shingles.add(shingle)

        mh = MinHash(num_perm=self._num_perm)
        for shingle in shingles:
            mh.update(shingle.encode("utf-8"))
        return mh

    def check_and_register(
        self, entry_id: str, text: str
    ) -> DedupeResult:
        """
        Check if text is a duplicate, and if not, register it.

        Returns DedupeResult with duplicate status.
        """
        content_hash = self.compute_content_hash(text)

        # Check exact duplicate
        if content_hash in self._hashes:
            original_id = self._hashes[content_hash]
            logger.info(
                "exact_duplicate_found",
                entry_id=entry_id,
                duplicate_of=original_id,
            )
            return DedupeResult(
                content_hash=content_hash,
                is_exact_duplicate=True,
                is_near_duplicate=False,
                duplicate_of=original_id,
                similarity=1.0,
            )

        # Check near duplicate via LSH
        mh = self.compute_minhash(text)
        candidates = self._lsh.query(mh)

        if candidates:
            # Find most similar candidate
            best_sim = 0.0
            best_id = None
            for cand_id in candidates:
                if cand_id in self._minhashes:
                    sim = mh.jaccard(self._minhashes[cand_id])
                    if sim > best_sim:
                        best_sim = sim
                        best_id = cand_id

            if best_id and best_sim >= 0.8:
                logger.info(
                    "near_duplicate_found",
                    entry_id=entry_id,
                    duplicate_of=best_id,
                    similarity=round(best_sim, 4),
                )
                # Still register the hash (different text, same content)
                self._hashes[content_hash] = entry_id
                return DedupeResult(
                    content_hash=content_hash,
                    is_exact_duplicate=False,
                    is_near_duplicate=True,
                    duplicate_of=best_id,
                    similarity=best_sim,
                )

        # Not a duplicate — register
        self._hashes[content_hash] = entry_id
        self._minhashes[entry_id] = mh
        try:
            self._lsh.insert(entry_id, mh)
        except ValueError:
            # Already inserted (shouldn't happen but be safe)
            pass

        return DedupeResult(
            content_hash=content_hash,
            is_exact_duplicate=False,
            is_near_duplicate=False,
        )

    @property
    def stats(self) -> dict:
        return {
            "total_registered": len(self._hashes),
            "lsh_entries": len(self._minhashes),
        }
