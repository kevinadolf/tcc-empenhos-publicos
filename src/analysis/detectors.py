"""Anomaly detection helpers built on top of Spark-based graph metrics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

from pyspark.sql import functions as F

from src.analysis.metrics import (
    community_detection_louvain,
    shannon_entropy_by_neighbor,
    weighted_degree_centrality,
)
from src.common.spark_graph import SparkGraph
from src.db.graph_builder import NODE_FORNECEDOR, NODE_ORGAO


@dataclass
class AnomalyResult:
    score: float
    reason: str
    context: Dict


def top_weighted_centrality(
    graph: SparkGraph,
    node_type: str,
    limit: int = 10,
) -> List[Tuple[str, float]]:
    centrality = weighted_degree_centrality(graph, node_type)
    ranked = sorted(centrality.items(), key=lambda item: item[1], reverse=True)
    return ranked[:limit]


def detect_entropy_outliers(
    graph: SparkGraph,
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
                    score=float(score),
                    reason=f"Alta entropia de relação com {neighbor_type}",
                    context={"node_id": node, "focus_type": focus_type},
                ),
            )
    return anomalies


def detect_isolated_communities(
    graph: SparkGraph,
    source_type: str,
    target_type: str,
    min_size: int = 2,
) -> List[AnomalyResult]:
    relevant_types = {source_type, target_type}
    vertices = graph.vertices.filter(F.col("node_type").isin(*relevant_types)).cache()

    if vertices.rdd.isEmpty():
        vertices.unpersist(False)
        return []

    edges = (
        graph.edges.alias("e")
        .join(vertices.select(F.col("id").alias("src_id")), F.col("e.src") == F.col("src_id"))
        .join(vertices.select(F.col("id").alias("dst_id")), F.col("e.dst") == F.col("dst_id"))
        .select("e.src", "e.dst", "e.edge_type", "e.valor")
    )

    if edges.rdd.isEmpty():
        vertices.unpersist(False)
        return []

    subgraph = SparkGraph(graph.spark, vertices, edges)
    components = community_detection_louvain(subgraph)
    if not components:
        vertices.unpersist(False)
        return []

    component_df = (
        graph.spark.createDataFrame(
            [{"id": node_id, "component": component_id} for node_id, component_id in components.items()],
        )
    )

    enriched = component_df.join(vertices, on="id", how="inner")
    grouped = (
        enriched.groupBy("component")
        .agg(
            F.count("*").alias("size"),
            F.collect_list("id").alias("nodes"),
            F.collect_set("node_type").alias("node_types"),
        )
        .collect()
    )
    vertices.unpersist(False)

    if len(grouped) <= 1:
        return []

    anomalies: List[AnomalyResult] = []
    for row in grouped:
        if row["size"] < min_size:
            continue
        anomalies.append(
            AnomalyResult(
                score=float(row["size"]),
                reason="Comunidade potencialmente isolada",
                context={
                    "community_id": int(row["component"]),
                    "nodes": row["nodes"],
                    "node_types": list(row["node_types"]),
                },
            ),
        )

    return anomalies


def run_default_anomaly_suite(graph: SparkGraph) -> Dict[str, List[AnomalyResult]]:
    return {
        "centralidade_fornecedores": [
            AnomalyResult(
                score=float(score),
                reason="Fornecedor com alta centralidade ponderada",
                context={"node_id": node},
            )
            for node, score in top_weighted_centrality(graph, NODE_FORNECEDOR)
        ],
        "centralidade_orgaos": [
            AnomalyResult(
                score=float(score),
                reason="Órgão com alta centralidade ponderada",
                context={"node_id": node},
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

