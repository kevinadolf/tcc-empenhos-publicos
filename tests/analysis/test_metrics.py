import pytest
from pyspark.sql import DataFrame

from src.analysis.metrics import (
    community_detection_louvain,
    entropy,
    project_bipartite,
    shannon_entropy_by_neighbor,
    weighted_degree_centrality,
)
from src.common.spark_graph import SparkGraph
from src.db.graph_builder import NODE_FORNECEDOR, NODE_ORGAO


def test_weighted_degree_centrality(sample_graph):
    centrality = weighted_degree_centrality(sample_graph, NODE_FORNECEDOR)
    assert len(centrality) == 2
    assert all(score > 0 for score in centrality.values())


def test_entropy_fornecedores(sample_graph):
    entropy_scores = shannon_entropy_by_neighbor(
        sample_graph,
        focus_type=NODE_FORNECEDOR,
        neighbor_type=NODE_ORGAO,
    )
    assert len(entropy_scores) == 2
    assert all(score >= 0 for score in entropy_scores.values())


def test_entropy_handles_zero_values():
    assert entropy([0, 0]) == 0.0
    assert entropy([]) == 0.0


def test_project_bipartite(sample_graph):
    projected = project_bipartite(sample_graph, NODE_ORGAO, NODE_FORNECEDOR)
    assert isinstance(projected, DataFrame)
    assert projected.count() >= 0


def test_community_detection_louvain_fallback(monkeypatch, sample_graph):
    def raise_error(_self):
        raise RuntimeError("GraphFrames not available")

    monkeypatch.setattr(SparkGraph, "as_graphframe", raise_error, raising=True)
    communities = community_detection_louvain(sample_graph)
    vertex_ids = {
        row["id"] for row in sample_graph.vertices.select("id").collect()
    }
    assert set(communities.keys()) == vertex_ids
    assert set(communities.values()) == {0}
