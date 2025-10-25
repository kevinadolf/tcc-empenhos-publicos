"""Graph metrics used to support anomaly detection."""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from typing import Dict, Iterable, Tuple

import networkx as nx


def weighted_degree_centrality(graph: nx.MultiDiGraph, node_type: str) -> Dict:
    """Compute weighted degree centrality for nodes of a given type."""

    centrality = defaultdict(float)
    for u, v, data in graph.edges(data=True):
        weight = data.get("valor", 1.0)
        if u[0] == node_type:
            centrality[u] += weight
        if v[0] == node_type:
            centrality[v] += weight
    return dict(centrality)


def entropy(values: Iterable[float]) -> float:
    total = float(sum(values))
    if total == 0:
        return 0.0
    probs = [value / total for value in values if value > 0]
    if not probs:
        return 0.0
    return -sum(p * math.log(p, 2) for p in probs)


def shannon_entropy_by_neighbor(graph: nx.MultiDiGraph, focus_type: str, neighbor_type: str) -> Dict:
    """Entropy of distribution of weights from focus nodes towards neighbor nodes."""

    distribution: Dict = {}
    for node in [n for n in graph.nodes if graph.nodes[n].get("type") == focus_type]:
        neighbor_weights = Counter()

        # Outgoing edges
        for _, neighbor, data in graph.edges(node, data=True):
            if graph.nodes[neighbor].get("type") == neighbor_type:
                neighbor_weights[neighbor] += data.get("valor", 1.0)

        # Incoming edges
        for source, _, data in graph.in_edges(node, data=True):
            if graph.nodes[source].get("type") == neighbor_type:
                neighbor_weights[source] += data.get("valor", 1.0)

        if not neighbor_weights:
            for source, _, data in graph.in_edges(node, data=True):
                for predecessor, _, data_prev in graph.in_edges(source, data=True):
                    if graph.nodes[predecessor].get("type") == neighbor_type:
                        neighbor_weights[predecessor] += data_prev.get("valor", data.get("valor", 1.0))

        distribution[node] = entropy(neighbor_weights.values())

    return distribution


def project_bipartite(graph: nx.MultiDiGraph, source_type: str, target_type: str) -> nx.Graph:
    """Project a heterogeneous graph into a bipartite representation."""

    projected = nx.Graph()
    for u, v, data in graph.edges(data=True):
        if graph.nodes[u]["type"] == source_type and graph.nodes[v]["type"] == target_type:
            projected.add_edge(u, v, weight=data.get("valor", 1.0))
    return projected


def community_detection_louvain(graph: nx.Graph) -> Dict:
    try:
        import networkx.algorithms.community as nx_comm

        communities = nx_comm.louvain_communities(graph, weight="weight", seed=42)
        assignment = {}
        for idx, community in enumerate(communities):
            for node in community:
                assignment[node] = idx
        return assignment
    except ImportError:
        return {node: 0 for node in graph.nodes}
