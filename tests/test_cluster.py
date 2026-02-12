"""
Tests for the clustering module.

Covers:
- Deterministic clustering (same inputs → same clusters)
- Single-entry edge case
- Empty input edge case
- cluster_id dense-rank assignment (by size DESC)
- Union-Find correctness
- Cosine threshold behavior
"""

from __future__ import annotations

import uuid

import numpy as np
import pytest

from core.pipeline.cluster import ClusterAssignment, UnionFind, cluster_entries


@pytest.fixture
def similar_embeddings() -> tuple[list[uuid.UUID], list[list[float]]]:
    """Create 6 embeddings that form 2 natural clusters."""
    np.random.seed(42)  # Deterministic

    ids = [uuid.UUID(int=i) for i in range(6)]

    # Cluster A: 3 similar vectors near [1, 0, 0, ...]
    base_a = np.zeros(384)
    base_a[0] = 1.0
    emb_a = [
        (base_a + np.random.normal(0, 0.05, 384)).tolist(),
        (base_a + np.random.normal(0, 0.05, 384)).tolist(),
        (base_a + np.random.normal(0, 0.05, 384)).tolist(),
    ]

    # Cluster B: 3 similar vectors near [0, 1, 0, ...]
    base_b = np.zeros(384)
    base_b[1] = 1.0
    emb_b = [
        (base_b + np.random.normal(0, 0.05, 384)).tolist(),
        (base_b + np.random.normal(0, 0.05, 384)).tolist(),
        (base_b + np.random.normal(0, 0.05, 384)).tolist(),
    ]

    return ids, emb_a + emb_b


class TestUnionFind:
    """Test Union-Find data structure."""

    def test_basic_union(self) -> None:
        uf = UnionFind(5)
        uf.union(0, 1)
        assert uf.find(0) == uf.find(1)
        assert uf.find(0) != uf.find(2)

    def test_transitive_union(self) -> None:
        uf = UnionFind(5)
        uf.union(0, 1)
        uf.union(1, 2)
        assert uf.find(0) == uf.find(2)

    def test_no_union(self) -> None:
        uf = UnionFind(3)
        assert uf.find(0) != uf.find(1)
        assert uf.find(1) != uf.find(2)

    def test_self_union(self) -> None:
        uf = UnionFind(3)
        uf.union(0, 0)
        assert uf.find(0) == 0


class TestClustering:
    """Test the full clustering pipeline."""

    def test_empty_input(self) -> None:
        result = cluster_entries([], [])
        assert result == []

    def test_single_entry(self) -> None:
        entry_id = uuid.uuid4()
        embedding = np.random.randn(384).tolist()

        result = cluster_entries([entry_id], [embedding])
        assert len(result) == 1
        assert result[0].feed_entry_id == entry_id
        assert result[0].cluster_id == 1
        assert result[0].similarity == 1.0

    def test_two_clusters_formed(
        self, similar_embeddings: tuple[list[uuid.UUID], list[list[float]]]
    ) -> None:
        ids, embeddings = similar_embeddings
        result = cluster_entries(ids, embeddings, k=3, cosine_threshold=0.5)

        # Should form 2 clusters
        cluster_ids = set(a.cluster_id for a in result)
        assert len(cluster_ids) == 2

        # Each entry should be assigned
        assert len(result) == 6

    def test_deterministic_clustering(
        self, similar_embeddings: tuple[list[uuid.UUID], list[list[float]]]
    ) -> None:
        """Same inputs should always produce the same cluster assignments."""
        ids, embeddings = similar_embeddings

        result1 = cluster_entries(ids, embeddings, k=3, cosine_threshold=0.5)
        result2 = cluster_entries(ids, embeddings, k=3, cosine_threshold=0.5)

        # Same cluster_id assignments
        mapping1 = {a.feed_entry_id: a.cluster_id for a in result1}
        mapping2 = {a.feed_entry_id: a.cluster_id for a in result2}
        assert mapping1 == mapping2

    def test_cluster_id_dense_rank(
        self, similar_embeddings: tuple[list[uuid.UUID], list[list[float]]]
    ) -> None:
        """cluster_id should be dense-ranked: 1, 2, ... (no gaps)."""
        ids, embeddings = similar_embeddings
        result = cluster_entries(ids, embeddings, k=3, cosine_threshold=0.5)

        cluster_ids = sorted(set(a.cluster_id for a in result))
        expected = list(range(1, len(cluster_ids) + 1))
        assert cluster_ids == expected

    def test_high_threshold_many_clusters(self) -> None:
        """Very high threshold should produce mostly singletons."""
        np.random.seed(99)
        n = 10
        ids = [uuid.UUID(int=i) for i in range(n)]
        embeddings = [np.random.randn(384).tolist() for _ in range(n)]

        result = cluster_entries(ids, embeddings, k=5, cosine_threshold=0.99)

        # With random embeddings and threshold=0.99, most should be singletons
        cluster_ids = set(a.cluster_id for a in result)
        assert len(cluster_ids) >= 5  # Most should be their own cluster

    def test_low_threshold_fewer_clusters(
        self, similar_embeddings: tuple[list[uuid.UUID], list[list[float]]]
    ) -> None:
        """Very low threshold should merge more aggressively."""
        ids, embeddings = similar_embeddings
        result = cluster_entries(ids, embeddings, k=5, cosine_threshold=0.01)

        # With very low threshold, might merge everything
        cluster_ids = set(a.cluster_id for a in result)
        assert len(cluster_ids) <= 3  # Likely 1 or 2 clusters

    def test_similarity_scores_valid(
        self, similar_embeddings: tuple[list[uuid.UUID], list[list[float]]]
    ) -> None:
        """All similarity scores should be in [-1, 1]."""
        ids, embeddings = similar_embeddings
        result = cluster_entries(ids, embeddings, k=3, cosine_threshold=0.5)

        for a in result:
            assert -1.0 <= a.similarity <= 1.0 + 1e-6
