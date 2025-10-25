import builtins

import networkx as nx
import pytest

from src.analysis.metrics import (
    community_detection_louvain,
    entropy,
    project_bipartite,
    shannon_entropy_by_neighbor,
    weighted_degree_centrality,
)
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
    assert isinstance(projected, nx.Graph)
    assert projected.number_of_nodes() >= 0


def test_community_detection_louvain_fallback(monkeypatch):
    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "networkx.algorithms.community":
            raise ImportError
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    graph = nx.Graph()
    graph.add_edge("a", "b")
    communities = community_detection_louvain(graph)
    assert communities == {"a": 0, "b": 0}
