"""
Clustering module — kNN graph + Union-Find connected components.

Clusters are formed **strictly per flashpoint_id**.
Deterministic: same inputs → same cluster assignments.

Algorithm:
1. Build kNN graph (k configurable) using cosine similarity
2. Connect edges above cosine threshold
3. Union-Find to find connected components → clusters
4. Rank clusters within each flashpoint by:
   a. article_count DESC
   b. recency (max created_at) DESC
5. Assign dense-rank cluster_id starting from 1
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from dataclasses import dataclass, field

import numpy as np
import structlog

logger = structlog.get_logger(__name__)


@dataclass
class ClusterAssignment:
    """A single entry's cluster assignment."""

    feed_entry_id: uuid.UUID
    cluster_uuid: uuid.UUID
    cluster_id: int  # dense-rank within flashpoint
    similarity: float  # avg similarity to cluster centroid


@dataclass
class Cluster:
    """A cluster of related articles."""

    cluster_uuid: uuid.UUID
    cluster_id: int
    members: list[uuid.UUID] = field(default_factory=list)
    article_count: int = 0


# ── Union-Find ────────────────────────────────────────


class UnionFind:
    """Weighted Union-Find with path compression."""

    def __init__(self, n: int) -> None:
        self.parent = list(range(n))
        self.rank = [0] * n

    def find(self, x: int) -> int:
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x: int, y: int) -> None:
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self.rank[rx] < self.rank[ry]:
            rx, ry = ry, rx
        self.parent[ry] = rx
        if self.rank[rx] == self.rank[ry]:
            self.rank[rx] += 1


def cluster_entries(
    entry_ids: list[uuid.UUID],
    embeddings: list[list[float]],
    k: int = 10,
    cosine_threshold: float = 0.65,
) -> list[ClusterAssignment]:
    """
    Cluster entries using kNN graph + Union-Find.

    Args:
        entry_ids: List of feed_entry UUIDs (same order as embeddings).
        embeddings: Corresponding embedding vectors.
        k: Number of nearest neighbors to consider.
        cosine_threshold: Minimum cosine similarity to form an edge.

    Returns:
        List of ClusterAssignment objects.
    """
    try:
        n = len(entry_ids)
        if n == 0:
            return []

        if n == 1:
            cid = uuid.uuid4()
            return [
                ClusterAssignment(
                    feed_entry_id=entry_ids[0],
                    cluster_uuid=cid,
                    cluster_id=1,
                    similarity=1.0,
                )
            ]

        # Convert to numpy for vectorized operations
        emb_matrix = np.array(embeddings, dtype=np.float32)

        # L2-normalize (should already be normalized, but ensure)
        norms = np.linalg.norm(emb_matrix, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        emb_matrix = emb_matrix / norms

        # Compute cosine similarity matrix
        sim_matrix = emb_matrix @ emb_matrix.T

        # Build kNN graph and Union-Find
        uf = UnionFind(n)
        actual_k = min(k, n - 1)

        for i in range(n):
            # Get top-k neighbors (exclude self)
            sims = sim_matrix[i].copy()
            sims[i] = -1  # exclude self
            top_k_indices = np.argsort(sims)[-actual_k:]

            for j in top_k_indices:
                if sims[j] >= cosine_threshold:
                    uf.union(i, j)

        # Group by connected component
        components: dict[int, list[int]] = defaultdict(list)
        for i in range(n):
            root = uf.find(i)
            components[root].append(i)

        # Sort clusters by size (DESC) for dense-rank assignment
        sorted_clusters = sorted(
            components.values(),
            key=lambda members: len(members),
            reverse=True,
        )

        # Assign cluster_id (dense rank 1-based)
        assignments: list[ClusterAssignment] = []
        for cluster_rank, member_indices in enumerate(sorted_clusters, start=1):
            cluster_uuid = uuid.uuid4()

            # Compute centroid for similarity scores
            cluster_embeddings = emb_matrix[member_indices]
            centroid = cluster_embeddings.mean(axis=0)
            centroid_norm = np.linalg.norm(centroid)
            if centroid_norm > 0:
                centroid = centroid / centroid_norm

            for idx in member_indices:
                sim = float(np.dot(emb_matrix[idx], centroid))
                assignments.append(
                    ClusterAssignment(
                        feed_entry_id=entry_ids[idx],
                        cluster_uuid=cluster_uuid,
                        cluster_id=cluster_rank,
                        similarity=sim,
                    )
                )

        logger.info(
            "clustering_complete",
            total_entries=n,
            num_clusters=len(sorted_clusters),
            largest_cluster=len(sorted_clusters[0]) if sorted_clusters else 0,
        )
    except Exception as e:
        logger.error(
            "clustering_failed",
            error=str(e),
            total_entries=n,
            num_clusters=len(sorted_clusters),
            largest_cluster=len(sorted_clusters[0]) if sorted_clusters else 0,
        )   

    return assignments
