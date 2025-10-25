"""Anomaly detection helpers built on top of network metrics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

import networkx as nx

from src.analysis.metrics import (
    community_detection_louvain,
    shannon_entropy_by_neighbor,
    weighted_degree_centrality,
)
from src.db.graph_builder import NODE_FORNECEDOR, NODE_ORGAO


@dataclass
class AnomalyResult:
    score: float
    reason: str
    context: Dict


def top_weighted_centrality(
    graph: nx.MultiDiGraph,
    node_type: str,
    limit: int = 10,
) -> List[Tuple]:
    centrality = weighted_degree_centrality(graph, node_type)
    ranked = sorted(centrality.items(), key=lambda item: item[1], reverse=True)
    return ranked[:limit]


def detect_entropy_outliers(
    graph: nx.MultiDiGraph,
    focus_type: str,
    neighbor_type: str,
    threshold: float,
) -> List[AnomalyResult]:
    entropy_scores = shannon_entropy_by_neighbor(graph, focus_type, neighbor_type)
    anomalies: List[AnomalyResult] = []
    for node, score in entropy_scores.items():
        if score > threshold:
            anomalies.append(
                AnomalyResult(
                    score=score,
                    reason=f"Alta entropia de relação com {neighbor_type}",
                    context={"node": node},
                ),
            )
    return anomalies


def detect_isolated_communities(
    graph: nx.MultiDiGraph,
    source_type: str,
    target_type: str,
    min_size: int = 2,
) -> List[AnomalyResult]:
    projection = graph.to_undirected()
    communities = community_detection_louvain(projection)

    groups: Dict[int, List] = {}
    for node, group_id in communities.items():
        groups.setdefault(group_id, []).append(node)

    anomalies = []
    for group_id, nodes in groups.items():
        relevant = [n for n in nodes if n[0] in {source_type, target_type}]
        if len(relevant) >= min_size:
            subgraph = graph.subgraph(relevant)
            if nx.is_connected(subgraph.to_undirected()):
                continue
        anomalies.append(
            AnomalyResult(
                score=float(len(nodes)),
                reason="Comunidade potencialmente isolada",
                context={"community_id": group_id, "nodes": nodes},
            ),
        )
    return anomalies


def run_default_anomaly_suite(graph: nx.MultiDiGraph) -> Dict[str, List[AnomalyResult]]:
    return {
        "centralidade_fornecedores": [
            AnomalyResult(
                score=score,
                reason="Fornecedor com alta centralidade ponderada",
                context={"node": node},
            )
            for node, score in top_weighted_centrality(graph, NODE_FORNECEDOR)
        ],
        "centralidade_orgaos": [
            AnomalyResult(
                score=score,
                reason="Órgão com alta centralidade ponderada",
                context={"node": node},
            )
            for node, score in top_weighted_centrality(graph, NODE_ORGAO)
        ],
        "alta_entropia_fornecedores": detect_entropy_outliers(
            graph,
            focus_type=NODE_FORNECEDOR,
            neighbor_type=NODE_ORGAO,
            threshold=1.0,
        ),
        "comunidades_isoladas": detect_isolated_communities(
            graph,
            source_type=NODE_ORGAO,
            target_type=NODE_FORNECEDOR,
        ),
    }
